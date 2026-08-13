//! End-to-end distributed execution tests: controller + agents in-process,
//! with a mock HTTP protocol handler injected through the factory seam.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use loadr_agent::agent::validate_data_file_path;
use loadr_agent::controller::scale_shares;
use loadr_agent::{
    Agent, AgentConfig, AgentTls, Controller, ControllerConfig, ControllerHandle, ControllerTls,
    RunnerDeps, SubmitOptions,
};
use loadr_core::{
    PreparedRequest, ProtocolError, ProtocolHandler, ProtocolRegistry, ProtocolResponse, Timings,
    VuContext,
};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test scaffolding
// ---------------------------------------------------------------------------

/// A mock "http" protocol handler: sleeps 1–5ms and returns 200 with timings.
struct MockHttpHandler {
    counter: AtomicU64,
}

#[async_trait::async_trait]
impl ProtocolHandler for MockHttpHandler {
    fn name(&self) -> &str {
        "http"
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let ms = 1 + (n % 5);
        tokio::time::sleep(Duration::from_millis(ms)).await;
        let d = ms as f64;
        Ok(ProtocolResponse {
            status: 200,
            status_text: "OK".to_string(),
            protocol_version: "HTTP/1.1".to_string(),
            timings: Timings {
                waiting_ms: d,
                duration_ms: d,
                ..Default::default()
            },
            bytes_sent: 100,
            bytes_received: 256,
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

/// A mock `data_source` plugin: counts `init`/`next_row` calls and emits rows
/// with a single `tick` column.
struct MockDataSource {
    inits: Arc<AtomicU64>,
    rows: Arc<AtomicU64>,
}

impl loadr_core::DataSourcePlugin for MockDataSource {
    fn name(&self) -> &str {
        "fake"
    }

    fn init(
        &mut self,
        source_configs: &indexmap::IndexMap<String, serde_json::Value>,
    ) -> Result<(), String> {
        assert!(
            source_configs.contains_key("gen"),
            "data.gen config delivered to the plugin"
        );
        self.inits.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn next_row(
        &self,
        _ctx: &loadr_core::PluginRowCtx<'_>,
    ) -> Result<loadr_core::PluginRowResult, String> {
        let n = self.rows.fetch_add(1, Ordering::Relaxed);
        let mut row = loadr_core::data::Row::new();
        row.insert("tick".to_string(), n.to_string());
        Ok(loadr_core::PluginRowResult::Row(row))
    }
}

fn mock_deps() -> RunnerDeps {
    RunnerDeps {
        protocols: Arc::new(|_defaults, _base_dir| {
            let mut registry = ProtocolRegistry::new();
            registry.register(Arc::new(MockHttpHandler {
                counter: AtomicU64::new(0),
            }));
            Ok(registry)
        }),
        script: None,
        data_sources: None,
    }
}

/// `mock_deps` plus a data-source factory that backs every declared plugin
/// with a [`MockDataSource`] reporting into the shared counters.
fn mock_deps_with_data_sources(inits: &Arc<AtomicU64>, rows: &Arc<AtomicU64>) -> RunnerDeps {
    let inits = Arc::clone(inits);
    let rows = Arc::clone(rows);
    RunnerDeps {
        data_sources: Some(Arc::new(move |plugin_refs, _base_dir| {
            let mut sources: HashMap<String, Box<dyn loadr_core::DataSourcePlugin>> =
                HashMap::new();
            for plugin_ref in plugin_refs {
                if !plugin_ref.enabled {
                    continue;
                }
                sources.insert(
                    plugin_ref.name.clone(),
                    Box::new(MockDataSource {
                        inits: Arc::clone(&inits),
                        rows: Arc::clone(&rows),
                    }),
                );
            }
            Ok(sources)
        })),
        ..mock_deps()
    }
}

fn localhost0() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

fn temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("loadr-agent-test-{tag}-{}", uuid::Uuid::new_v4()))
}

fn spawn_agent(
    controller_addr: String,
    name: &str,
    agent_id: Option<String>,
    tls: Option<AgentTls>,
) -> CancellationToken {
    spawn_agent_with_deps(controller_addr, name, agent_id, tls, mock_deps())
}

fn spawn_agent_with_deps(
    controller_addr: String,
    name: &str,
    agent_id: Option<String>,
    tls: Option<AgentTls>,
    deps: RunnerDeps,
) -> CancellationToken {
    let token = CancellationToken::new();
    let config = AgentConfig {
        controller_addr,
        agent_id,
        agent_name: name.to_string(),
        labels: HashMap::new(),
        tls,
        work_dir: temp_dir(name),
        deps,
    };
    let child = token.clone();
    tokio::spawn(async move {
        let _ = Agent::run(config, child).await;
    });
    token
}

async fn start_controller(liveness: Duration) -> ControllerHandle {
    Controller::start(ControllerConfig {
        bind: localhost0(),
        tls: None,
        agent_liveness: liveness,
    })
    .await
    .expect("controller start")
}

async fn wait_until<F: FnMut() -> bool>(mut cond: F, timeout: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !cond() {
        assert!(
            tokio::time::Instant::now() <= deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn run_state(handle: &ControllerHandle, run_id: &str) -> String {
    handle
        .runs()
        .into_iter()
        .find(|r| r.run_id == run_id)
        .map(|r| r.state)
        .unwrap_or_default()
}

fn is_terminal(state: &str) -> bool {
    matches!(state, "finished" | "aborted" | "failed")
}

fn quick_submit() -> SubmitOptions {
    SubmitOptions {
        start_barrier: Duration::from_millis(300),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// End-to-end: exact metric merging across 3 agents
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_iterations_split_and_merged_exactly() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let addr = format!("http://{}", handle.addr());
    let _a1 = spawn_agent(addr.clone(), "a1", None, None);
    let _a2 = spawn_agent(addr.clone(), "a2", None, None);
    let _a3 = spawn_agent(addr.clone(), "a3", None, None);
    wait_until(
        || handle.agents().iter().filter(|a| a.healthy).count() == 3,
        Duration::from_secs(10),
        "3 agents registered",
    )
    .await;

    let plan = r#"
name: dist-e2e
scenarios:
  s:
    executor: shared-iterations
    vus: 4
    iterations: 120
    flow:
      - request: { url: "http://mock.local/x" }
thresholds:
  http_reqs: ["count==120"]
"#;
    let run_id = handle
        .submit(plan.to_string(), quick_submit())
        .await
        .expect("submit");

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(30),
        "run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "finished");

    let summary = handle.run_summary(&run_id).expect("merged summary");

    // Exact totals: shared iterations split 40/40/40 across the 3 agents.
    let http_reqs = summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_reqs")
        .expect("http_reqs metric");
    assert_eq!(http_reqs.agg.sum, 120.0, "central http_reqs sum");
    let iterations = summary
        .metrics
        .iter()
        .find(|m| m.metric == "iterations")
        .expect("iterations metric");
    assert_eq!(iterations.agg.sum, 120.0, "central iterations sum");

    // Percentiles survive the histogram merge.
    let duration = summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_req_duration")
        .expect("http_req_duration metric");
    assert!(duration.agg.p95.is_some(), "p95 present after merge");
    assert_eq!(duration.agg.count, 120);

    // Every agent contributed: series are tagged instance=<agent_name>.
    let mut per_instance: HashMap<String, f64> = HashMap::new();
    for series in summary
        .snapshot
        .series
        .iter()
        .filter(|s| s.metric == "http_reqs")
    {
        let instance = series.tags.get("instance").cloned().unwrap_or_default();
        *per_instance.entry(instance).or_insert(0.0) += series.agg.sum;
    }
    let instances: HashSet<&str> = per_instance.keys().map(String::as_str).collect();
    assert_eq!(
        instances,
        HashSet::from(["a1", "a2", "a3"]),
        "all agents contributed tagged series"
    );
    for (instance, sum) in &per_instance {
        assert_eq!(*sum, 40.0, "agent {instance} share of shared iterations");
    }

    // Centrally evaluated thresholds pass on the merged totals.
    let thresholds = handle.run_thresholds(&run_id);
    assert_eq!(thresholds.len(), 1);
    assert!(thresholds[0].passed, "{:?}", thresholds[0]);
    assert_eq!(thresholds[0].observed, Some(120.0));
    assert!(summary.thresholds_passed);

    // Per-agent summaries were reported too.
    assert_eq!(handle.run_agent_summaries(&run_id).len(), 3);

    handle.shutdown();
}

// ---------------------------------------------------------------------------
// Open-model rate split: total arrival rate is preserved across agents
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constant_arrival_rate_splits_proportionally() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let addr = format!("http://{}", handle.addr());
    let _a1 = spawn_agent(addr.clone(), "r1", None, None);
    let _a2 = spawn_agent(addr.clone(), "r2", None, None);
    let _a3 = spawn_agent(addr.clone(), "r3", None, None);
    wait_until(
        || handle.agents().iter().filter(|a| a.healthy).count() == 3,
        Duration::from_secs(10),
        "3 agents registered",
    )
    .await;

    let plan = r#"
name: rate-e2e
scenarios:
  open:
    executor: constant-arrival-rate
    rate: 50
    duration: 2s
    pre_allocated_vus: 6
    max_vus: 12
    graceful_stop: 1s
    flow:
      - request: { url: "http://mock.local/r" }
"#;
    let run_id = handle
        .submit(plan.to_string(), quick_submit())
        .await
        .expect("submit");
    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(30),
        "rate run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "finished");

    let summary = handle.run_summary(&run_id).expect("summary");
    let iterations = summary
        .metrics
        .iter()
        .find(|m| m.metric == "iterations")
        .expect("iterations metric");
    let total = iterations.agg.sum;
    assert!(
        (80.0..=120.0).contains(&total),
        "50/s over 2s split across 3 agents should land near 100, got {total}"
    );

    handle.shutdown();
}

// ---------------------------------------------------------------------------
// Controls: stop a long run
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_run_terminates_long_run_quickly() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let addr = format!("http://{}", handle.addr());
    let _a1 = spawn_agent(addr.clone(), "c1", None, None);
    let _a2 = spawn_agent(addr.clone(), "c2", None, None);
    wait_until(
        || handle.agents().iter().filter(|a| a.healthy).count() == 2,
        Duration::from_secs(10),
        "2 agents registered",
    )
    .await;

    let plan = r#"
name: long-run
scenarios:
  long:
    executor: constant-vus
    vus: 2
    duration: 30s
    flow:
      - request: { url: "http://mock.local/long" }
"#;
    let run_id = handle
        .submit(plan.to_string(), quick_submit())
        .await
        .expect("submit");

    // Let it actually start producing load, then stop it.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    handle.stop_run(&run_id).await.expect("stop");

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(15),
        "stopped run to terminate",
    )
    .await;
    let state = run_state(&handle, &run_id);
    assert!(
        state == "aborted" || state == "finished",
        "unexpected state {state}"
    );
    let summary = handle.run_summary(&run_id).expect("summary after stop");
    let http_reqs = summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_reqs")
        .expect("http_reqs");
    assert!(http_reqs.agg.sum > 0.0, "run produced load before the stop");

    handle.shutdown();
}

// ---------------------------------------------------------------------------
// Agent loss with Continue policy
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_loss_continue_finishes_from_survivor() {
    let handle = start_controller(Duration::from_secs(3)).await;
    let addr = format!("http://{}", handle.addr());
    let _keeper = spawn_agent(addr.clone(), "keeper", None, None);
    let loser = spawn_agent(addr.clone(), "loser", None, None);
    wait_until(
        || handle.agents().iter().filter(|a| a.healthy).count() == 2,
        Duration::from_secs(10),
        "2 agents registered",
    )
    .await;

    let plan = r#"
name: loss-run
scenarios:
  steady:
    executor: constant-vus
    vus: 2
    duration: 8s
    flow:
      - request: { url: "http://mock.local/loss" }
"#;
    let run_id = handle
        .submit(plan.to_string(), quick_submit())
        .await
        .expect("submit");

    // Kill one agent mid-run.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    loser.cancel();

    wait_until(
        || {
            handle
                .agents()
                .iter()
                .any(|a| a.name == "loser" && !a.healthy)
        },
        Duration::from_secs(8),
        "lost agent marked unhealthy",
    )
    .await;

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(30),
        "run to finish from the surviving agent",
    )
    .await;
    assert_eq!(
        run_state(&handle, &run_id),
        "finished",
        "Continue policy keeps the run going"
    );
    let summary = handle.run_summary(&run_id).expect("summary");
    let http_reqs = summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_reqs")
        .expect("http_reqs");
    assert!(http_reqs.agg.sum > 0.0);

    handle.shutdown();
}

// ---------------------------------------------------------------------------
// Reconnection with a stable agent id
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_reconnects_and_re_registers() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let addr = format!("http://{}", handle.addr());

    let first = spawn_agent(
        addr.clone(),
        "phoenix",
        Some("agent-phoenix".to_string()),
        None,
    );
    wait_until(
        || {
            handle
                .agents()
                .iter()
                .any(|a| a.id == "agent-phoenix" && a.healthy)
        },
        Duration::from_secs(10),
        "initial registration",
    )
    .await;

    first.cancel();
    wait_until(
        || {
            handle
                .agents()
                .iter()
                .any(|a| a.id == "agent-phoenix" && !a.healthy)
        },
        Duration::from_secs(10),
        "disconnect detection",
    )
    .await;

    let _second = spawn_agent(addr, "phoenix", Some("agent-phoenix".to_string()), None);
    wait_until(
        || {
            handle
                .agents()
                .iter()
                .any(|a| a.id == "agent-phoenix" && a.healthy)
        },
        Duration::from_secs(10),
        "re-registration",
    )
    .await;
    assert_eq!(handle.agents().len(), 1, "same id replaces the old entry");

    handle.shutdown();
}

// ---------------------------------------------------------------------------
// mTLS smoke test + plaintext rejection
// ---------------------------------------------------------------------------

struct TestCerts {
    ca: PathBuf,
    server_cert: PathBuf,
    server_key: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
}

fn make_certs() -> TestCerts {
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "loadr-test-ca");
    let ca = rcgen::CertifiedIssuer::self_signed(ca_params, ca_key).expect("ca cert");

    let server_key = rcgen::KeyPair::generate().expect("server key");
    let server_params =
        rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("server params");
    let server_cert = server_params
        .signed_by(&server_key, &ca)
        .expect("server cert");

    let client_key = rcgen::KeyPair::generate().expect("client key");
    let client_params =
        rcgen::CertificateParams::new(vec!["loadr-test-agent".to_string()]).expect("client params");
    let client_cert = client_params
        .signed_by(&client_key, &ca)
        .expect("client cert");

    let dir = temp_dir("certs");
    std::fs::create_dir_all(&dir).expect("certs dir");
    let write = |name: &str, contents: String| -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, contents).expect("write pem");
        path
    };
    TestCerts {
        ca: write("ca.pem", ca.pem()),
        server_cert: write("server.pem", server_cert.pem()),
        server_key: write("server.key", server_key.serialize_pem()),
        client_cert: write("client.pem", client_cert.pem()),
        client_key: write("client.key", client_key.serialize_pem()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mtls_agent_registers_and_plaintext_agent_is_rejected() {
    let certs = make_certs();
    let handle = Controller::start(ControllerConfig {
        bind: localhost0(),
        tls: Some(ControllerTls {
            cert_pem: certs.server_cert.clone(),
            key_pem: certs.server_key.clone(),
            client_ca_pem: Some(certs.ca.clone()),
        }),
        agent_liveness: Duration::from_secs(6),
    })
    .await
    .expect("tls controller");

    let tls_addr = format!("https://{}", handle.addr());
    let _secure = spawn_agent(
        tls_addr,
        "secure-agent",
        None,
        Some(AgentTls {
            ca_pem: Some(certs.ca.clone()),
            cert_pem: Some(certs.client_cert.clone()),
            key_pem: Some(certs.client_key.clone()),
            domain: Some("localhost".to_string()),
        }),
    );
    wait_until(
        || {
            handle
                .agents()
                .iter()
                .any(|a| a.name == "secure-agent" && a.healthy)
        },
        Duration::from_secs(10),
        "mTLS registration",
    )
    .await;

    // A plaintext agent must never register against a TLS controller.
    let plain_addr = format!("http://{}", handle.addr());
    let plain = spawn_agent(plain_addr, "plain-agent", None, None);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        !handle.agents().iter().any(|a| a.name == "plain-agent"),
        "plaintext agent must not register against a TLS controller"
    );
    plain.cancel();

    handle.shutdown();
}

// ---------------------------------------------------------------------------
// Plugin-backed data sources feed distributed runs through the factory seam
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plugin_data_source_feeds_distributed_run() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let addr = format!("http://{}", handle.addr());

    let inits = Arc::new(AtomicU64::new(0));
    let rows = Arc::new(AtomicU64::new(0));
    let _a1 = spawn_agent_with_deps(
        addr.clone(),
        "d1",
        None,
        None,
        mock_deps_with_data_sources(&inits, &rows),
    );
    let _a2 = spawn_agent_with_deps(
        addr.clone(),
        "d2",
        None,
        None,
        mock_deps_with_data_sources(&inits, &rows),
    );
    wait_until(
        || handle.agents().iter().filter(|a| a.healthy).count() == 2,
        Duration::from_secs(10),
        "2 agents registered",
    )
    .await;

    let plan = r#"
name: dist-plugin-data
plugins:
  - name: fake
scenarios:
  s:
    executor: shared-iterations
    vus: 2
    iterations: 40
    flow:
      - request: { url: "http://mock.local/x?t=${data.gen.tick}" }
data:
  gen:
    type: plugin
    source: fake
    config: { flavor: "distributed" }
"#;
    let run_id = handle
        .submit(plan.to_string(), quick_submit())
        .await
        .expect("submit");

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(30),
        "run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "finished");

    let summary = handle.run_summary(&run_id).expect("merged summary");
    let http_reqs = summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_reqs")
        .expect("http_reqs metric");
    assert_eq!(http_reqs.agg.sum, 40.0, "central http_reqs sum");

    // Each agent built its data sources once (init per assignment) and every
    // iteration pulled exactly one row through the plugin.
    assert_eq!(inits.load(Ordering::Relaxed), 2, "one init per agent");
    assert_eq!(
        rows.load(Ordering::Relaxed),
        40,
        "one plugin row per iteration across the fleet"
    );

    handle.shutdown();
}

// ---------------------------------------------------------------------------
// Unit tests: scale share math and path traversal rejection
// ---------------------------------------------------------------------------

#[test]
fn scale_share_math_matches_partition_semantics() {
    assert_eq!(scale_shares(10, 3), vec![4, 3, 3]);
    assert_eq!(scale_shares(9, 3), vec![3, 3, 3]);
    assert_eq!(scale_shares(2, 3), vec![1, 1, 0]);
    assert_eq!(scale_shares(0, 3), vec![0, 0, 0]);
    assert_eq!(scale_shares(7, 1), vec![7]);
    assert_eq!(scale_shares(5, 5), vec![1, 1, 1, 1, 1]);
    let shares = scale_shares(1234, 7);
    assert_eq!(
        shares.iter().sum::<u64>(),
        1234,
        "shares always sum to total"
    );
}

#[test]
fn data_file_paths_reject_traversal() {
    assert!(validate_data_file_path("users.csv").is_ok());
    assert!(validate_data_file_path("data/users.csv").is_ok());
    assert!(validate_data_file_path("deep/nested/dir/file.json").is_ok());

    assert!(validate_data_file_path("").is_err());
    assert!(validate_data_file_path("/etc/passwd").is_err());
    assert!(validate_data_file_path("../escape.csv").is_err());
    assert!(validate_data_file_path("data/../../escape.csv").is_err());
    assert!(validate_data_file_path("./sneaky.csv").is_err());
    assert!(validate_data_file_path("\\windows\\style").is_err());
}
