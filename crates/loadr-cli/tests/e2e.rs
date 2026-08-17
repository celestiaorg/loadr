//! End-to-end tests driving the real `loadr` binary against the gRPC server.

use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_loadr");

fn write_plan(dir: &std::path::Path, yaml: &str) -> std::path::PathBuf {
    let path = dir.join("test.yaml");
    std::fs::write(&path, yaml).expect("write plan");
    path
}

fn dylib_name(stem: &str) -> String {
    #[cfg(target_os = "windows")]
    return format!("{stem}.dll");
    #[cfg(target_os = "macos")]
    return format!("lib{stem}.dylib");
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    return format!("lib{stem}.so");
}

fn build_reference_feeder() -> std::path::PathBuf {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let status = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args(["build", "-p", "loadr-example-signed-tx-feeder"])
        .current_dir(root)
        .status()
        .expect("build reference feeder");
    assert!(status.success());
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target"));
    let path = target.join("debug").join(dylib_name("signed_tx_feeder"));
    assert!(path.is_file(), "missing feeder at {}", path.display());
    path
}

#[test]
fn version_commands_include_revision() {
    let expected = format!("loadr {}", loadr_core::build_info::VERSION_WITH_REVISION);
    for args in [&["--version"][..], &["version"][..]] {
        let output = Command::new(BIN).args(args).output().expect("run version");
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).lines().next(),
            Some(expected.as_str())
        );
    }
}

#[test]
fn validation_rejects_removed_protocols_and_bad_grpc_templates() {
    let dir = tempfile::tempdir().expect("tmp");
    let plan = write_plan(
        dir.path(),
        r#"
scenarios:
  invalid:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request: { url: "https://example.invalid" }
      - request:
          url: grpc://127.0.0.1:50051
          grpc:
            reflection: true
            service: loadr.test.Echo
            method: UnaryEcho
            message: { value: "${unterminated" }
"#,
    );
    let output = Command::new(BIN)
        .args(["validate", plan.to_str().expect("path")])
        .output()
        .expect("validate");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("grpc://") && stderr.contains("unterminated"),
        "{stderr}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_grpc_run_produces_exact_request_count() {
    let server = loadr_testserver::GrpcEchoServer::spawn()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmp");
    let plan = write_plan(
        dir.path(),
        &format!(
            r#"
name: e2e-grpc
scenarios:
  unary:
    executor: shared-iterations
    vus: 4
    iterations: 30
    flow:
      - request:
          url: grpc://{addr}
          grpc:
            reflection: true
            service: loadr.test.Echo
            method: UnaryEcho
            message: {{ message: "vu-${{vu}}" }}
          checks: [{{ type: status, equals: 0 }}]
thresholds:
  grpc_reqs: ["count==30"]
"#,
            addr = server.addr
        ),
    );
    let summary = dir.path().join("summary.json");
    let output = Command::new(BIN)
        .args([
            "run",
            "--quiet",
            "--summary-export",
            summary.to_str().expect("summary path"),
            plan.to_str().expect("plan path"),
        ])
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(summary).expect("summary")).expect("json");
    let requests = summary["metrics"]
        .as_array()
        .expect("metrics")
        .iter()
        .find(|metric| metric["metric"] == "grpc_reqs")
        .expect("grpc_reqs");
    assert_eq!(requests["agg"]["sum"], 30.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_feeder_supplies_grpc_payloads() {
    let server = loadr_testserver::GrpcEchoServer::spawn()
        .await
        .expect("server");
    let feeder = build_reference_feeder();
    let feeder_json = serde_json::to_string(feeder.to_str().expect("utf8 path")).expect("json");
    let dir = tempfile::tempdir().expect("tmp");
    let plan = write_plan(
        dir.path(),
        &format!(
            r#"
plugins:
  - name: tx-signer
    path: {feeder_json}
    config: {{ seed: 1 }}
data:
  signed:
    type: plugin
    source: tx-signer
    config: {{ chain_id: testnet }}
scenarios:
  submit:
    executor: shared-iterations
    vus: 2
    iterations: 8
    flow:
      - request:
          url: grpc://{addr}
          grpc:
            reflection: true
            service: loadr.test.Echo
            method: UnaryEcho
            message: {{ message: "${{data.signed.nonce}}", payload: "${{data.signed.tx_b64}}" }}
          checks: [{{ type: status, equals: 0 }}]
thresholds:
  grpc_reqs: ["count==8"]
"#,
            addr = server.addr
        ),
    );
    let output = Command::new(BIN)
        .args(["run", "--quiet", plan.to_str().expect("plan path")])
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The `blocking: true` bracket is a runtime behavior, so drive it through the
/// real binary against a real server rather than asserting it compiles. The
/// upstream version of this test used the `noop` protocol, which this build
/// does not have.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_feeder_completes_a_grpc_run() {
    let server = loadr_testserver::GrpcEchoServer::spawn()
        .await
        .expect("server");
    let feeder = build_reference_feeder();
    let feeder_json = serde_json::to_string(feeder.to_str().expect("utf8 path")).expect("json");
    let dir = tempfile::tempdir().expect("tmp");
    let plan = write_plan(
        dir.path(),
        &format!(
            r#"
plugins:
  - name: tx-signer
    path: {feeder_json}
    config: {{ seed: 1 }}
data:
  signed:
    type: plugin
    source: tx-signer
    blocking: true
    config: {{ chain_id: testnet }}
scenarios:
  submit:
    executor: shared-iterations
    vus: 2
    iterations: 8
    flow:
      - request:
          url: grpc://{addr}
          grpc:
            reflection: true
            service: loadr.test.Echo
            method: UnaryEcho
            message: {{ message: "${{data.signed.nonce}}", payload: "${{data.signed.tx_b64}}" }}
          checks: [{{ type: status, equals: 0 }}]
thresholds:
  grpc_reqs: ["count==8"]
"#,
            addr = server.addr
        ),
    );
    let output = Command::new(BIN)
        .args(["run", "--quiet", plan.to_str().expect("plan path")])
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
