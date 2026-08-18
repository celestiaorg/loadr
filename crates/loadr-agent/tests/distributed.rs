//! End-to-end distributed execution tests: controller + agents in-process,
//! with a mock HTTP protocol handler injected through the factory seam.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use loadr_agent::agent::validate_data_file_path;
use loadr_agent::controller::scale_shares;
use loadr_agent::{AgentTls, Controller, ControllerConfig, ControllerTls};

mod support;
use support::*;

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
    assert_eq!(
        run_state(&handle, &run_id),
        "pending",
        "run remains pending until an agent crosses the start barrier"
    );

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(30),
        "run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "finished");

    let summary = handle.run_summary(&run_id).expect("merged summary");
    let run_info = handle
        .runs()
        .into_iter()
        .find(|r| r.run_id == run_id)
        .expect("run info");
    assert_eq!(run_info.ended_ms, Some(summary.ended_ms));

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let summary_after_wait = handle.run_summary(&run_id).expect("stable summary");
    assert_eq!(summary_after_wait.ended_ms, summary.ended_ms);
    assert_eq!(summary_after_wait.duration_secs, summary.duration_secs);

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

    // The controller's Prometheus source view has trusted per-agent labels
    // and a separate exact fleet aggregate.
    let metrics_view = handle.run_metrics_view(&run_id).expect("metrics view");
    let detailed_agents: HashSet<&str> = metrics_view
        .detailed
        .series
        .iter()
        .filter(|series| series.metric == "http_reqs")
        .filter_map(|series| series.tags.get("loadr_agent").map(String::as_str))
        .collect();
    assert_eq!(detailed_agents, HashSet::from(["a1", "a2", "a3"]));
    assert!(metrics_view
        .detailed
        .series
        .iter()
        .filter(|series| series.metric == "http_reqs")
        .all(|series| series
            .tags
            .get("loadr_agent_id")
            .is_some_and(|id| !id.is_empty())));
    let fleet_reqs = metrics_view
        .fleet
        .iter()
        .find(|metric| metric.metric == "http_reqs")
        .expect("fleet requests");
    assert_eq!(fleet_reqs.agg.sum, 120.0);
    let fleet_duration = metrics_view
        .fleet
        .iter()
        .find(|metric| metric.metric == "http_req_duration")
        .expect("fleet duration");
    assert_eq!(fleet_duration.agg.p95, duration.agg.p95);
    let fleet_vus = metrics_view
        .fleet
        .iter()
        .find(|metric| metric.metric == "vus")
        .expect("fleet VUs");
    assert_eq!(fleet_vus.agg.last, Some(0.0));
    let webui = handle
        .run_aggregate_snapshot(&run_id)
        .expect("exact Web UI aggregates");
    let request_rollup = webui
        .series
        .iter()
        .find(|series| series.metric == "request_reqs" && series.tags.is_empty())
        .expect("canonical fleet request rollup");
    assert_eq!(request_rollup.agg.sum, 120.0);
    let agent_rollups: HashSet<&str> = webui
        .series
        .iter()
        .filter(|series| series.metric == "request_reqs")
        .filter_map(|series| series.tags.get("loadr_agent").map(String::as_str))
        .collect();
    assert_eq!(agent_rollups, HashSet::from(["a1", "a2", "a3"]));

    // Centrally evaluated thresholds pass on the merged totals.
    let thresholds = handle.run_thresholds(&run_id);
    assert_eq!(thresholds.len(), 1);
    assert!(thresholds[0].passed, "{:?}", thresholds[0]);
    assert_eq!(thresholds[0].observed, Some(120.0));
    assert!(summary.thresholds_passed);

    // Per-agent summaries were reported too.
    assert_eq!(handle.run_agent_summaries(&run_id).len(), 3);

    // Terminal timestamps, duration, and derived rates are immutable on reads.
    let ended_ms = handle
        .runs()
        .into_iter()
        .find(|run| run.run_id == run_id)
        .and_then(|run| run.ended_ms)
        .expect("terminal timestamp");
    assert_eq!(ended_ms, summary.ended_ms);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let reread = handle.run_summary(&run_id).expect("frozen summary");
    assert_eq!(reread.ended_ms, summary.ended_ms);
    assert_eq!(reread.duration_secs, summary.duration_secs);
    assert_eq!(
        reread
            .metrics
            .iter()
            .find(|metric| metric.metric == "request_reqs")
            .and_then(|metric| metric.agg.per_second),
        summary
            .metrics
            .iter()
            .find(|metric| metric.metric == "request_reqs")
            .and_then(|metric| metric.agg.per_second)
    );

    handle.shutdown();
}

// ---------------------------------------------------------------------------
// Live gauges: active VUs are summed across agents for UI snapshots
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constant_vus_live_snapshot_sums_agent_vus() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let addr = format!("http://{}", handle.addr());
    let _a1 = spawn_agent(addr.clone(), "v1", None, None);
    let _a2 = spawn_agent(addr.clone(), "v2", None, None);
    let _a3 = spawn_agent(addr.clone(), "v3", None, None);
    wait_until(
        || handle.agents().iter().filter(|a| a.healthy).count() == 3,
        Duration::from_secs(10),
        "3 agents registered",
    )
    .await;

    let plan = r#"
name: vus-e2e
scenarios:
  closed:
    executor: constant-vus
    vus: 6
    duration: 2s
    flow:
      - request: { url: "http://mock.local/vus" }
"#;
    let run_id = handle
        .submit(plan.to_string(), quick_submit())
        .await
        .expect("submit");
    let rx = handle.watch_run(&run_id).expect("snapshot receiver");
    wait_until(
        || {
            let snap = rx.borrow();
            let vus: f64 = snap
                .series
                .iter()
                .filter(|s| s.metric == "vus")
                .filter_map(|s| s.agg.last)
                .sum();
            let series = snap.series.iter().filter(|s| s.metric == "vus").count();
            series == 3 && vus >= 6.0
        },
        Duration::from_secs(10),
        "fleet active VUs in snapshot",
    )
    .await;

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(30),
        "run completion",
    )
    .await;
    let summary = handle.run_summary(&run_id).expect("summary");
    let vus_max = summary
        .metrics
        .iter()
        .find(|m| m.metric == "vus_max")
        .expect("vus_max metric");
    assert_eq!(vus_max.agg.last, Some(6.0));

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
    let scale_error = handle
        .scale(&run_id, "long", 3)
        .await
        .expect_err("constant-vus scenario is not externally controlled");
    assert!(scale_error
        .to_string()
        .contains("not externally controlled"));
    handle.pause_run(&run_id, true).await.expect("pause ack");
    assert_eq!(
        handle
            .run_operational_info(&run_id)
            .expect("operational info")
            .paused,
        Some(true)
    );
    handle.pause_run(&run_id, false).await.expect("resume ack");
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
    let operational = handle
        .run_operational_info(&run_id)
        .expect("operational info");
    assert_eq!(operational.lost.len(), 1);
    assert_eq!(operational.contributing.len(), 2);
    assert_eq!(
        operational.on_agent_loss,
        loadr_agent::OnAgentLoss::Continue
    );

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

    let initial = handle
        .agents()
        .into_iter()
        .find(|agent| agent.id == "agent-phoenix")
        .expect("registered agent");
    assert!(
        initial.peer_addr.is_some(),
        "controller captures peer socket"
    );
    assert_eq!(
        initial.version.as_deref(),
        Some(loadr_core::build_info::VERSION)
    );
    assert_eq!(
        initial.revision.as_deref(),
        Some(loadr_core::build_info::GIT_REVISION)
    );

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
    let agents = handle.agents();
    assert_eq!(agents.len(), 1, "same id replaces the old entry");
    assert!(agents[0].peer_addr.is_some());
    assert_eq!(
        agents[0].revision.as_deref(),
        Some(loadr_core::build_info::GIT_REVISION)
    );

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
// Plugin protocols: the full plan (incl. `plugins:`) reaches the factory
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plan_plugins_reach_agent_protocol_factory() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let addr = format!("http://{}", handle.addr());
    let _a1 = spawn_agent_with_deps(addr.clone(), "pl1", None, None, plugin_gated_deps());
    wait_until(
        || handle.agents().iter().filter(|a| a.healthy).count() == 1,
        Duration::from_secs(10),
        "agent registered",
    )
    .await;

    let plan = r#"
name: plugin-proto-e2e
plugins:
  - name: custom
scenarios:
  s:
    executor: shared-iterations
    vus: 2
    iterations: 40
    flow:
      - request: { url: "custom://svc/x", protocol: custom }
"#;
    let run_id = handle
        .submit(plan.to_string(), quick_submit())
        .await
        .expect("submit");
    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(30),
        "plugin run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "finished");

    let summary = handle.run_summary(&run_id).expect("summary");
    // The plugin-provided handler executed every request: the missing-handler
    // path emits no `custom_reqs` at all, only `http_req_failed=true`.
    let custom_reqs = summary
        .metrics
        .iter()
        .find(|m| m.metric == "custom_reqs")
        .expect("custom_reqs metric present");
    assert_eq!(
        custom_reqs.agg.sum, 40.0,
        "all requests hit the custom handler"
    );
    let failed = summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_req_failed")
        .expect("http_req_failed metric present");
    assert_eq!(failed.agg.sum, 0.0, "no `no handler registered` failures");

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
