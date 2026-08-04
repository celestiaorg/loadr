//! End-to-end tests driving the real `loadr` binary against in-repo servers.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_loadr");

#[test]
fn version_commands_include_revision() {
    let expected = format!("loadr {}", loadr_core::build_info::VERSION_WITH_REVISION);

    let short = Command::new(BIN)
        .arg("--version")
        .output()
        .expect("run loadr --version");
    assert!(short.status.success());
    assert_eq!(String::from_utf8_lossy(&short.stdout).trim(), expected);

    let detailed = Command::new(BIN)
        .arg("version")
        .output()
        .expect("run loadr version");
    assert!(detailed.status.success());
    assert_eq!(
        String::from_utf8_lossy(&detailed.stdout).lines().next(),
        Some(expected.as_str())
    );
}

fn write_test(dir: &std::path::Path, name: &str, yaml: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, yaml).expect("write test yaml");
    path
}

/// Platform-correct cdylib artifact filename for a `[lib] name = "<stem>"`.
fn dylib_name(stem: &str) -> String {
    #[cfg(target_os = "windows")]
    {
        format!("{stem}.dll")
    }
    #[cfg(target_os = "macos")]
    {
        format!("lib{stem}.dylib")
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        format!("lib{stem}.so")
    }
}

/// Build a native example plugin (workspace member) and return the path to
/// its dynamic-library artifact, for use as a `plugins: [{ path: ... }]`
/// reference in a test plan.
fn build_native_plugin(package: &str, lib_stem: &str) -> std::path::PathBuf {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root exists")
        .to_path_buf();
    let status = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args(["build", "-p", package])
        .current_dir(&root)
        .status()
        .unwrap_or_else(|e| panic!("cannot run cargo for {package}: {e}"));
    assert!(status.success(), "building {package} failed");
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target"));
    let path = target.join("debug").join(dylib_name(lib_stem));
    assert!(
        path.is_file(),
        "missing native artifact at {}",
        path.display()
    );
    path
}

#[test]
fn validate_reports_invalid_grpc_templates() {
    let dir = tempfile::tempdir().expect("tmp");
    let test = write_test(
        dir.path(),
        "invalid-grpc-template.yaml",
        r#"
scenarios:
  invalid:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request:
          protocol: grpc
          url: grpc://127.0.0.1:50051
          grpc:
            reflection: true
            service: loadr.test.Echo
            method: UnaryEcho
            message: { value: "${unterminated" }
"#,
    );
    let output = Command::new(BIN)
        .args(["validate", test.to_str().expect("path")])
        .output()
        .expect("validate");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "validation unexpectedly passed");
    assert!(
        stderr.contains("grpc.message.value") && stderr.contains("unterminated"),
        "unexpected validation error: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_run_produces_metrics_and_passes() {
    let server = loadr_testserver::HttpTestServer::spawn()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
name: e2e-standalone
defaults:
  http: {{ base_url: {base} }}
scenarios:
  hit:
    executor: shared-iterations
    vus: 4
    iterations: 30
    flow:
      - request:
          name: json
          url: /json
          extract:
            - {{ type: jsonpath, name: token, expression: "$.token" }}
          checks:
            - {{ type: status, equals: 200 }}
            - {{ type: jsonpath, name: has items, expression: "$.items[0].id", equals: 1 }}
      - request:
          name: echo token
          method: POST
          url: /echo
          body: "tok=${{token}}"
          checks:
            - {{ type: body_contains, value: tok-123 }}
thresholds:
  http_reqs: [ "count==60" ]
  checks: [ "rate>0.99" ]
  http_req_failed: [ "rate<0.01" ]
"#,
        base = server.base_url()
    );
    let test = write_test(dir.path(), "t.yaml", &yaml);
    let summary_path = dir.path().join("summary.json");
    let junit_path = dir.path().join("junit.xml");

    let output = Command::new(BIN)
        .args([
            "run",
            "--quiet",
            "--summary-export",
            summary_path.to_str().expect("path"),
            "--junit",
            junit_path.to_str().expect("path"),
            test.to_str().expect("path"),
        ])
        .output()
        .expect("run loadr");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected success.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("http_req_duration"),
        "summary missing metrics: {stdout}"
    );
    assert!(stdout.contains("✓"), "summary missing checks: {stdout}");

    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary_path).expect("summary file"))
            .expect("summary json");
    assert_eq!(summary["thresholds_passed"], serde_json::json!(true));
    let reqs = summary["metrics"]
        .as_array()
        .expect("metrics")
        .iter()
        .find(|m| m["metric"] == "http_reqs")
        .expect("http_reqs");
    assert_eq!(reqs["agg"]["sum"], serde_json::json!(60.0));

    // JUnit report: well-formed, names the suite, and reports the thresholds as
    // a green testsuite that any CI test panel can ingest.
    let junit = std::fs::read_to_string(&junit_path).expect("junit file");
    assert!(junit.starts_with("<?xml version=\"1.0\""), "junit: {junit}");
    assert!(junit.contains("<testsuites name=\"loadr: e2e-standalone\""));
    assert!(junit.contains("<testsuite name=\"thresholds\""));
    assert!(junit.contains("classname=\"threshold\""));
    assert!(junit.contains("</testsuites>"));
}

/// Open-model dispatcher (`constant-arrival-rate`): the schedule is met with
/// zero dropped iterations when workers keep up, and `--worker-threads`
/// bounds the runtime. Exercises the claim-budget worker wake path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arrival_rate_keeps_schedule_without_drops() {
    let server = loadr_testserver::HttpTestServer::spawn()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
name: e2e-arrival-rate
defaults:
  http: {{ base_url: {base} }}
scenarios:
  arrivals:
    executor: constant-arrival-rate
    rate: 200
    duration: 2s
    graceful_stop: 100ms
    pre_allocated_vus: 20
    max_vus: 60
    flow:
      - request:
          name: json
          url: /json
          checks:
            - {{ type: status, equals: 200 }}
"#,
        base = server.base_url()
    );
    let test = write_test(dir.path(), "arrivals.yaml", &yaml);
    let summary_path = dir.path().join("summary.json");

    let output = Command::new(BIN)
        .args([
            "run",
            "--quiet",
            "--worker-threads",
            "2",
            "--summary-export",
            summary_path.to_str().expect("path"),
            test.to_str().expect("path"),
        ])
        .output()
        .expect("run loadr");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected success.\nstdout: {stdout}\nstderr: {stderr}"
    );

    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary_path).expect("summary file"))
            .expect("summary json");
    let metric_sum = |name: &str| -> f64 {
        summary["metrics"]
            .as_array()
            .expect("metrics")
            .iter()
            .find(|m| m["metric"] == name)
            .and_then(|m| m["agg"]["sum"].as_f64())
            .unwrap_or(0.0)
    };
    let iterations = metric_sum("iterations");
    // 200/s over 2s = ~400; allow generous slack for CI clocks but require
    // that the dispatcher actually drove the schedule.
    assert!(
        (300.0..=460.0).contains(&iterations),
        "iterations off schedule: {iterations}\nstdout: {stdout}"
    );
    assert_eq!(
        metric_sum("dropped_iterations"),
        0.0,
        "dropped iterations with idle workers\nstdout: {stdout}"
    );
}

/// The configurable dispatch interval must not discard the interval ending at
/// the scenario deadline, even when one tick is longer than the scenario.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arrival_rate_flushes_final_dispatch_interval() {
    let server = loadr_testserver::HttpTestServer::spawn()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
name: e2e-arrival-rate-final-interval
defaults:
  http: {{ base_url: {base} }}
scenarios:
  arrivals:
    executor: constant-arrival-rate
    rate: 100
    duration: 250ms
    graceful_stop: 100ms
    pre_allocated_vus: 25
    max_vus: 25
    flow:
      - request: {{ url: /json }}
"#,
        base = server.base_url()
    );
    let test = write_test(dir.path(), "final-interval.yaml", &yaml);
    let summary_path = dir.path().join("summary.json");

    let output = Command::new(BIN)
        .env("LOADR_DISPATCH_TICK_US", "1000000")
        .args([
            "run",
            "--quiet",
            "--summary-export",
            summary_path.to_str().expect("path"),
            test.to_str().expect("path"),
        ])
        .output()
        .expect("run loadr");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected success.\nstdout: {stdout}\nstderr: {stderr}"
    );

    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary_path).expect("summary file"))
            .expect("summary json");
    let iterations = summary["metrics"]
        .as_array()
        .expect("metrics")
        .iter()
        .find(|m| m["metric"] == "iterations")
        .and_then(|m| m["agg"]["sum"].as_f64())
        .unwrap_or(0.0);
    assert!(
        (20.0..=25.0).contains(&iterations),
        "final interval was not flushed: {iterations}\nstdout: {stdout}"
    );
}

/// Saturated open model: with `max_vus` far below the schedule, unclaimed
/// arrivals must surface as dropped iterations — and none may vanish.
/// Completed plus dropped matches the scheduled arrivals within one tick's
/// batch (`ceil(rate*tick)+1`); the deadline flush publishes the final
/// partial interval, so there is no unpublished tail (the exact identity is
/// unit-tested on the dispatcher's gate). The dispatch tick is pinned on the
/// child process — `dispatch_tick()` is a process-wide `OnceLock`, so setting
/// it on the test process would not work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arrival_rate_saturation_conserves_scheduled_arrivals() {
    let dir = tempfile::tempdir().expect("tmp");
    // 200/s scheduled against max 2 VUs x 50ms iterations (~40/s capacity):
    // most arrivals must expire as drops. Think-time-only flow, no network.
    // graceful_stop must outlast one iteration so claimed in-flight work
    // finishes (an aborted claim is neither completed nor dropped).
    let yaml = r#"
name: e2e-arrival-saturation
scenarios:
  saturated:
    executor: constant-arrival-rate
    rate: 200
    duration: 2s
    pre_allocated_vus: 1
    max_vus: 2
    graceful_stop: 1s
    flow:
      - think_time: { type: constant, duration: 50ms }
"#;
    let test = write_test(dir.path(), "saturation.yaml", yaml);
    let summary_path = dir.path().join("summary.json");

    let output = Command::new(BIN)
        .env("LOADR_DISPATCH_TICK_US", "20000")
        .args([
            "run",
            "--quiet",
            "--worker-threads",
            "2",
            "--summary-export",
            summary_path.to_str().expect("path"),
            test.to_str().expect("path"),
        ])
        .output()
        .expect("run loadr");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected success.\nstdout: {stdout}\nstderr: {stderr}"
    );

    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary_path).expect("summary file"))
            .expect("summary json");
    let metric_sum = |name: &str| -> f64 {
        summary["metrics"]
            .as_array()
            .expect("metrics")
            .iter()
            .find(|m| m["metric"] == name)
            .and_then(|m| m["agg"]["sum"].as_f64())
            .unwrap_or(0.0)
    };
    let completed = metric_sum("iterations");
    let dropped = metric_sum("dropped_iterations");
    assert!(completed > 0.0, "no iterations completed\nstdout: {stdout}");
    assert!(
        dropped > 0.0,
        "saturated pool dropped nothing\nstdout: {stdout}"
    );
    // Conservation at 20ms tick: ceil(200*0.02)+1 = 5. The deadline flush
    // publishes the final partial interval, so no extra tail slack is needed.
    let scheduled = 200.0 * 2.0;
    assert!(
        ((completed + dropped) - scheduled).abs() <= 5.0,
        "conservation violated: completed={completed} dropped={dropped}\nstdout: {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn threshold_failure_exits_99() {
    let server = loadr_testserver::HttpTestServer::spawn()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
scenarios:
  s:
    executor: per-vu-iterations
    vus: 1
    iterations: 3
    flow: [ {{ request: {{ url: {base}/delay/30 }} }} ]
thresholds:
  http_req_duration: [ "max<1" ]
"#,
        base = server.base_url()
    );
    let test = write_test(dir.path(), "t.yaml", &yaml);
    let output = Command::new(BIN)
        .args(["run", "--quiet", test.to_str().expect("path")])
        .output()
        .expect("run loadr");
    assert_eq!(
        output.status.code(),
        Some(99),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_control_while_if_repeat() {
    // while/if conditions are JS — exercise them through the real engine.
    let server = loadr_testserver::HttpTestServer::spawn()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
defaults:
  http: {{ base_url: {base} }}
js: {{ script: "" }}
metrics:
  loops: {{ kind: counter }}
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - js: "session.vars.n = 0"
      - while:
          condition: "Number(session.vars.n) < 3"
          steps:
            - request: {{ name: loop, url: /json, checks: [ {{ type: status, equals: 200 }} ] }}
            - js: "session.vars.n = Number(session.vars.n) + 1; session.counterAdd('loops', 1)"
      - if:
          condition: "Number(session.vars.n) === 3"
          then:
            - request: {{ name: done, url: /headers, checks: [ {{ type: status, equals: 200 }} ] }}
          else:
            - request: {{ name: never, url: /status/500 }}
      - repeat:
          times: 2
          steps:
            - request: {{ name: twice, url: /json }}
thresholds:
  loops: [ "count==3" ]
  checks: [ "rate>0.99" ]
"#,
        base = server.base_url()
    );
    let test = dir.path().join("t.yaml");
    std::fs::write(&test, yaml).expect("write");
    let summary = dir.path().join("s.json");
    let output = Command::new(BIN)
        .args([
            "run",
            "--quiet",
            "--summary-export",
            summary.to_str().expect("p"),
            test.to_str().expect("p"),
        ])
        .output()
        .expect("run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "flow control run failed.\nstdout: {stdout}\nstderr: {stderr}"
    );
    let s: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary).expect("summary")).expect("json");
    // 1 iteration: 3 loop reqs + 1 done + 2 repeat = 6 http_reqs; loops==3.
    let reqs = s["metrics"]
        .as_array()
        .expect("metrics")
        .iter()
        .find(|m| m["metric"] == "http_reqs")
        .expect("http_reqs");
    assert_eq!(reqs["agg"]["sum"], serde_json::json!(6.0));
    assert_eq!(s["thresholds_passed"], serde_json::json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn javascript_lifecycle_run() {
    let server = loadr_testserver::HttpTestServer::spawn()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmp");
    std::fs::write(
        dir.path().join("script.js"),
        r#"
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const visits = new Counter('visits');

export function setup() {
  const res = http.get('/json');
  return { token: res.json().token };
}

export default function (data) {
  const res = http.get('/echo?from=js', { headers: { 'X-Token': data.token } });
  check(res, {
    'status 200': (r) => r.status === 200,
    'token forwarded': (r) => r.body.includes('tok-123'),
  });
  visits.add(1);
}
"#,
    )
    .expect("write script");
    let yaml = format!(
        r#"
defaults:
  http: {{ base_url: {base} }}
js: {{ file: script.js }}
scenarios:
  scripted:
    executor: per-vu-iterations
    vus: 2
    iterations: 5
    exec: default
thresholds:
  visits: [ "count==10" ]
  checks: [ "rate>0.99" ]
"#,
        base = server.base_url()
    );
    let test = write_test(dir.path(), "t.yaml", &yaml);
    let output = Command::new(BIN)
        .args(["run", "--quiet", test.to_str().expect("path")])
        .output()
        .expect("run loadr");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout: {stdout}\nstderr: {stderr}"
    );
    assert!(stdout.contains("visits"), "custom metric missing: {stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn report_renders_html() {
    let server = loadr_testserver::HttpTestServer::spawn()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
name: report-demo
scenarios:
  s:
    executor: per-vu-iterations
    vus: 1
    iterations: 2
    flow: [ {{ request: {{ url: {base}/json, checks: [ {{ type: status, equals: 200 }} ] }} }} ]
"#,
        base = server.base_url()
    );
    let test = write_test(dir.path(), "t.yaml", &yaml);
    let summary = dir.path().join("s.json");
    let html = dir.path().join("report.html");
    let run = Command::new(BIN)
        .args([
            "run",
            "--quiet",
            "--summary-export",
            summary.to_str().expect("p"),
            test.to_str().expect("p"),
        ])
        .output()
        .expect("run");
    assert!(run.status.success());
    let report = Command::new(BIN)
        .args([
            "report",
            summary.to_str().expect("p"),
            "-o",
            html.to_str().expect("p"),
        ])
        .output()
        .expect("report");
    assert!(report.status.success());
    let content = std::fs::read_to_string(&html).expect("html");
    assert!(content.contains("report-demo"));
    assert!(content.contains("PASSED"));
    assert!(content.contains("http_req_duration"));
}

#[test]
fn convert_jmx_output_validates() {
    let jmx = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../loadr-convert/tests/data");
    let sample = std::fs::read_dir(&jmx)
        .expect("convert test data")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().and_then(|e| e.to_str()) == Some("jmx"))
        .expect("a .jmx sample");
    let dir = tempfile::tempdir().expect("tmp");
    let out = dir.path().join("converted.yaml");
    let convert = Command::new(BIN)
        .args([
            "convert",
            sample.to_str().expect("p"),
            "-o",
            out.to_str().expect("p"),
        ])
        .output()
        .expect("convert");
    assert!(
        convert.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&convert.stderr)
    );
    let validate = Command::new(BIN)
        .args(["validate", "--no-check-files", out.to_str().expect("p")])
        .output()
        .expect("validate");
    assert!(
        validate.status.success(),
        "converted plan invalid: {}",
        String::from_utf8_lossy(&validate.stderr)
    );
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Read controller stderr until both listen addresses are known.
fn parse_controller_addrs(child: &mut Child) -> (String, String) {
    let stderr = child.stderr.take().expect("controller stderr");
    let mut reader = BufReader::new(stderr);
    let mut grpc = None;
    let mut ui = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut line = String::new();
    while (grpc.is_none() || ui.is_none()) && std::time::Instant::now() < deadline {
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        if let Some(rest) = line.split("controller listening on ").nth(1) {
            grpc = rest.split_whitespace().next().map(str::to_string);
        }
        if let Some(rest) = line.split("web UI at http://").nth(1) {
            ui = rest
                .trim()
                .trim_end_matches('/')
                .split('/')
                .next()
                .map(str::to_string);
        }
    }
    // Keep draining in the background so the child never blocks on stderr.
    std::thread::spawn(move || {
        let mut sink = String::new();
        while reader.read_line(&mut sink).unwrap_or(0) > 0 {
            sink.clear();
        }
    });
    (grpc.expect("grpc addr"), ui.expect("ui addr"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_controller_and_agents_via_binary() {
    let server = loadr_testserver::HttpTestServer::spawn()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmp");

    let mut controller = ChildGuard(
        Command::new(BIN)
            .args([
                "controller",
                "--bind",
                "127.0.0.1:0",
                "--ui-bind",
                "127.0.0.1:0",
                "--storage-dir",
                dir.path().join("ctrl").to_str().expect("p"),
            ])
            .stderr(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn controller"),
    );
    let (grpc_addr, ui_addr) = parse_controller_addrs(&mut controller.0);

    let mut agents = Vec::new();
    for i in 0..2 {
        agents.push(ChildGuard(
            Command::new(BIN)
                .args([
                    "agent",
                    "--join",
                    &grpc_addr,
                    "--name",
                    &format!("e2e-agent-{i}"),
                    "--work-dir",
                    dir.path().join(format!("agent{i}")).to_str().expect("p"),
                ])
                .stderr(Stdio::null())
                .stdout(Stdio::null())
                .spawn()
                .expect("spawn agent"),
        ));
    }

    // Wait for both agents to register.
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build_http::<http_body_util::Full<bytes::Bytes>>();
    let agents_url: hyper::Uri = format!("http://{ui_addr}/api/agents").parse().expect("uri");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let response = client.get(agents_url.clone()).await;
        if let Ok(response) = response {
            use http_body_util::BodyExt as _;
            let body = response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            let count = value.as_array().map(|a| a.len()).unwrap_or(0);
            if count >= 2 {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "agents never registered"
        );
    }

    // Submit a run through `loadr run --controller`.
    let yaml = format!(
        r#"
name: e2e-distributed
scenarios:
  fleet:
    executor: shared-iterations
    vus: 4
    iterations: 40
    graceful_stop: 2s
    flow:
      - request:
          url: {base}/json
          checks: [ {{ type: status, equals: 200 }} ]
thresholds:
  http_reqs: [ "count==40" ]
  checks: [ "rate>0.99" ]
"#,
        base = server.base_url()
    );
    let test = write_test(dir.path(), "dist.yaml", &yaml);
    let summary_path = dir.path().join("dist-summary.json");
    let run = Command::new(BIN)
        .args([
            "run",
            "--controller",
            &ui_addr,
            "--summary-export",
            summary_path.to_str().expect("p"),
            test.to_str().expect("p"),
        ])
        .output()
        .expect("remote run");
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success(),
        "distributed run failed.\nstdout: {stdout}\nstderr: {stderr}"
    );

    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary_path).expect("summary"))
            .expect("summary json");
    assert_eq!(summary["thresholds_passed"], serde_json::json!(true));
    let reqs = summary["metrics"]
        .as_array()
        .expect("metrics")
        .iter()
        .find(|m| m["metric"] == "http_reqs")
        .expect("http_reqs");
    assert_eq!(
        reqs["agg"]["sum"],
        serde_json::json!(40.0),
        "exact iteration split across 2 agents"
    );
    // Both agents contributed (instance tags in the snapshot series).
    let instances: std::collections::BTreeSet<&str> = summary["snapshot"]["series"]
        .as_array()
        .expect("series")
        .iter()
        .filter_map(|s| s["tags"]["instance"].as_str())
        .collect();
    assert!(
        instances.len() >= 2,
        "expected both agents in series tags, got {instances:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugin_data_source_signs_grpc_payload_end_to_end() {
    let server = loadr_testserver::GrpcEchoServer::spawn()
        .await
        .expect("grpc server");
    let plugin_path = build_native_plugin(
        "loadr-plugin-example-native-data-source",
        "native_data_source",
    );
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
name: e2e-plugin-data-source
plugins:
  - name: tx-signer
    path: {plugin_path}
    config: {{ seed: 1 }}
data:
  signed_tx:
    type: plugin
    source: tx-signer
    config: {{ chain_id: testnet-1 }}
scenarios:
  submit:
    executor: shared-iterations
    vus: 2
    iterations: 4
    flow:
      - request:
          name: submit tx
          protocol: grpc
          url: grpc://{addr}
          grpc:
            reflection: true
            service: loadr.test.Echo
            method: UnaryEcho
            message: {{ message: "sig", payload: "${{data.signed_tx.tx_b64}}" }}
          checks:
            - {{ type: status, equals: 0 }}
"#,
        plugin_path = plugin_path.display(),
        addr = server.addr,
    );
    let test = write_test(dir.path(), "t.yaml", &yaml);
    let summary_path = dir.path().join("summary.json");

    let output = Command::new(BIN)
        .args([
            "run",
            "--quiet",
            "--summary-export",
            summary_path.to_str().expect("path"),
            test.to_str().expect("path"),
        ])
        .output()
        .expect("run loadr");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected success.\nstdout: {stdout}\nstderr: {stderr}"
    );

    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary_path).expect("summary file"))
            .expect("summary json");
    let metric = |name: &str| {
        summary["metrics"]
            .as_array()
            .expect("metrics")
            .iter()
            .find(|m| m["metric"] == name)
            .unwrap_or_else(|| panic!("missing metric {name}: {summary}"))
    };
    let grpc_reqs = metric("grpc_reqs")["agg"]["sum"]
        .as_f64()
        .expect("grpc_reqs sum");
    assert!(grpc_reqs > 0.0, "expected grpc_reqs > 0: {summary}");
    let http_req_failed = metric("http_req_failed")["agg"]["rate"]
        .as_f64()
        .expect("http_req_failed rate");
    assert_eq!(
        http_req_failed, 0.0,
        "expected zero failed requests: {summary}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugin_data_source_runs_at_fixed_count_against_noop() {
    let plugin_path = build_native_plugin(
        "loadr-plugin-example-native-data-source",
        "native_data_source",
    );
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
name: e2e-plugin-data-source-noop
plugins:
  - name: tx-signer
    path: {plugin_path}
    config: {{ seed: 1 }}
data:
  signed_tx:
    type: plugin
    source: tx-signer
    config: {{ chain_id: throughput-test }}
scenarios:
  generate:
    executor: shared-iterations
    vus: 3
    iterations: 12
    flow:
      - request:
          name: generate signed payload
          protocol: noop
          url: noop://local
          method: POST
          body: "${{data.signed_tx.tx_b64}}"
thresholds:
  noop_reqs: [ "count==12" ]
  http_req_failed: [ "rate==0" ]
"#,
        plugin_path = plugin_path.display(),
    );
    let test = write_test(dir.path(), "t.yaml", &yaml);
    let summary_path = dir.path().join("summary.json");

    let output = Command::new(BIN)
        .args([
            "run",
            "--quiet",
            "--summary-export",
            summary_path.to_str().expect("path"),
            test.to_str().expect("path"),
        ])
        .output()
        .expect("run loadr");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected success.\nstdout: {stdout}\nstderr: {stderr}"
    );

    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary_path).expect("summary file"))
            .expect("summary json");
    let metric = |name: &str| {
        summary["metrics"]
            .as_array()
            .expect("metrics")
            .iter()
            .find(|m| m["metric"] == name)
            .unwrap_or_else(|| panic!("missing metric {name}: {summary}"))
    };
    assert_eq!(metric("noop_reqs")["agg"]["sum"], serde_json::json!(12.0));
    assert_eq!(
        metric("http_req_failed")["agg"]["rate"],
        serde_json::json!(0.0)
    );
    assert!(
        metric("data_sent")["agg"]["sum"]
            .as_f64()
            .expect("data_sent sum")
            > 0.0,
        "the generated feeder payload should be accounted: {summary}"
    );
}

/// The `blocking: true` twin of the fixed-count run: the bracketed fetch path
/// must produce byte-identical results — same counts, same thresholds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugin_data_source_with_blocking_flag_runs_at_fixed_count() {
    let plugin_path = build_native_plugin(
        "loadr-plugin-example-native-data-source",
        "native_data_source",
    );
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
name: e2e-plugin-data-source-blocking
plugins:
  - name: tx-signer
    path: {plugin_path}
    config: {{ seed: 1 }}
data:
  signed_tx:
    type: plugin
    source: tx-signer
    config: {{ chain_id: throughput-test }}
    blocking: true
scenarios:
  generate:
    executor: shared-iterations
    vus: 3
    iterations: 12
    flow:
      - request:
          name: generate signed payload
          protocol: noop
          url: noop://local
          method: POST
          body: "${{data.signed_tx.tx_b64}}"
thresholds:
  noop_reqs: [ "count==12" ]
  http_req_failed: [ "rate==0" ]
"#,
        plugin_path = plugin_path.display(),
    );
    let test = write_test(dir.path(), "t.yaml", &yaml);
    let summary_path = dir.path().join("summary.json");

    let output = Command::new(BIN)
        .args([
            "run",
            "--quiet",
            "--summary-export",
            summary_path.to_str().expect("path"),
            test.to_str().expect("path"),
        ])
        .output()
        .expect("run loadr");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected success.\nstdout: {stdout}\nstderr: {stderr}"
    );

    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary_path).expect("summary file"))
            .expect("summary json");
    let metric = |name: &str| {
        summary["metrics"]
            .as_array()
            .expect("metrics")
            .iter()
            .find(|m| m["metric"] == name)
            .unwrap_or_else(|| panic!("missing metric {name}: {summary}"))
    };
    assert_eq!(metric("noop_reqs")["agg"]["sum"], serde_json::json!(12.0));
    assert_eq!(
        metric("http_req_failed")["agg"]["rate"],
        serde_json::json!(0.0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugin_data_source_missing_capability_fails_before_vus_start() {
    // hmac-signer is a real `kind = service` plugin that does not implement
    // `data_source` -- referencing it from `data.*.source` must fail plan
    // setup (before any VU starts), not silently no-op.
    let plugin_path = build_native_plugin("loadr-plugin-hmac-signer", "loadr_plugin_hmac_signer");
    let dir = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        r#"
name: e2e-plugin-data-source-missing-capability
plugins:
  - name: hmac-signer
    path: {plugin_path}
data:
  signed_tx:
    type: plugin
    source: hmac-signer
scenarios:
  submit:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: "https://example.invalid/${{data.signed_tx.x}}"
"#,
        plugin_path = plugin_path.display(),
    );
    let test = write_test(dir.path(), "t.yaml", &yaml);

    let output = Command::new(BIN)
        .args(["run", "--quiet", test.to_str().expect("path")])
        .output()
        .expect("run loadr");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "expected non-zero exit; stderr: {stderr}"
    );
    assert!(
        stderr.contains("not loaded or does not provide the data_source capability"),
        "expected the capability-missing error, got: {stderr}"
    );
}
