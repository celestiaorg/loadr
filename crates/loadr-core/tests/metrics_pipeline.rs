//! Coverage for the two `Output::wants_samples` paths: the classic sample
//! bus (when some output consumes raw samples) and shard-local recording
//! (when none do — the default `loadr run` has zero outputs, so this is the
//! common case). Harness modeled on flow_control.rs.
//!
//! Also covers the setup()-failure early-abort path: `Engine::run` must
//! return an error rather than hang, in both modes (see engine.rs's
//! `SampleSource` cancellation wiring).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use loadr_core::{
    Engine, EngineOptions, PreparedRequest, ProtocolError, ProtocolHandler, ProtocolRegistry,
    ProtocolResponse, Sample, ScriptEngine, ScriptError, ScriptHost, VuContext, VuScript,
};

/// A protocol handler that records every request and returns 200 after a
/// tiny fixed delay (enough for the timeline to see more than one instant).
#[derive(Default)]
struct MockHandler {
    count: AtomicU64,
}

#[async_trait]
impl ProtocolHandler for MockHandler {
    fn name(&self) -> &str {
        "http"
    }
    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        tokio::time::sleep(Duration::from_millis(2)).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(ProtocolResponse {
            status: 200,
            protocol_version: "HTTP/1.1".into(),
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

fn registry() -> (ProtocolRegistry, Arc<MockHandler>) {
    let handler = Arc::new(MockHandler::default());
    let mut reg = ProtocolRegistry::new();
    reg.register(handler.clone());
    (reg, handler)
}

/// Deterministic, exact-count plan: 4 VUs share exactly 40 iterations, one
/// request + one check each. Used identically by both the shard-mode and
/// bus-mode tests so their totals can be compared directly.
const PLAN_YAML: &str = r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 4
    iterations: 40
    flow:
      - request:
          url: "http://x/ping"
          checks:
            - { type: status, equals: 200 }
"#;
const EXPECTED_ITERATIONS: f64 = 40.0;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_output_run_uses_shards_and_reports_exact_totals() {
    let (reg, handler) = registry();
    let loaded =
        loadr_config::load_str(PLAN_YAML, &loadr_config::LoadOptions::new()).expect("parse");
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: reg,
            // Zero outputs: EngineOptions::default() (no wants_samples==true
            // output configured) — this is the shard-mode path.
            snapshot_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("engine");

    let result = engine.run().await.expect("run");

    assert_eq!(
        handler.count.load(Ordering::Relaxed),
        EXPECTED_ITERATIONS as u64
    );

    let http_reqs = result
        .summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_reqs")
        .expect("http_reqs metric");
    assert_eq!(
        http_reqs.agg.sum, EXPECTED_ITERATIONS,
        "exact http_reqs total"
    );

    let iterations = result
        .summary
        .metrics
        .iter()
        .find(|m| m.metric == "iterations")
        .expect("iterations metric");
    assert_eq!(
        iterations.agg.sum, EXPECTED_ITERATIONS,
        "exact iterations total"
    );

    let checks_total: u64 = result
        .summary
        .checks
        .iter()
        .map(|c| c.passes + c.fails)
        .sum();
    assert_eq!(
        checks_total, EXPECTED_ITERATIONS as u64,
        "exact checks total"
    );
    assert_eq!(
        result.summary.checks.iter().map(|c| c.fails).sum::<u64>(),
        0,
        "every check should have passed"
    );

    // Drain fix: with no channel/backlog to drain, duration_secs stays close
    // to how long this (near-instant, 2ms-per-request) plan actually took —
    // a channel-drain-inflated measurement would blow well past this.
    assert!(
        result.summary.duration_secs < 2.0,
        "duration_secs {} looks drain-inflated for a ~20ms plan",
        result.summary.duration_secs
    );

    // The gauge task's root bus clone pins to shard 0, so vus/vus_max still
    // show up even though every VU is recording into its own shard.
    assert!(
        result.summary.metrics.iter().any(|m| m.metric == "vus"),
        "vus gauge should be present"
    );
    assert!(
        result.summary.metrics.iter().any(|m| m.metric == "vus_max"),
        "vus_max gauge should be present"
    );

    assert!(
        !result.summary.timeline.is_empty(),
        "timeline should not be empty even for a short shard-mode run"
    );
}

/// A test-only output that opts into raw samples (`wants_samples() == true`,
/// same as the default), forcing the classic bus path even though the plan
/// has no "real" bus-requiring output — so it can assert bus-mode-specific
/// behavior (real per-sample timestamps) against the exact same plan as the
/// shard-mode test above.
#[derive(Clone)]
struct CollectingOutput {
    samples: Arc<parking_lot::Mutex<Vec<Sample>>>,
}

#[async_trait]
impl loadr_core::Output for CollectingOutput {
    fn name(&self) -> &str {
        "collecting-test-output"
    }

    fn wants_samples(&self) -> bool {
        true
    }

    async fn on_samples(&mut self, samples: &[Sample]) {
        self.samples.lock().extend(samples.iter().cloned());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sample_consuming_output_keeps_bus_path() {
    let (reg, handler) = registry();
    let collected = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let output = CollectingOutput {
        samples: collected.clone(),
    };
    let loaded =
        loadr_config::load_str(PLAN_YAML, &loadr_config::LoadOptions::new()).expect("parse");
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: reg,
            outputs: vec![Box::new(output)],
            snapshot_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("engine");

    let result = engine.run().await.expect("run");

    assert_eq!(
        handler.count.load(Ordering::Relaxed),
        EXPECTED_ITERATIONS as u64
    );

    let samples = collected.lock();
    assert!(
        !samples.is_empty(),
        "a wants_samples output should receive raw samples over the bus"
    );
    assert!(
        samples.iter().any(|s| s.timestamp_ms > 0),
        "bus-mode samples should carry real wall-clock timestamps"
    );
    drop(samples);

    // Same plan as the shard-mode test: totals must match exactly.
    let http_reqs = result
        .summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_reqs")
        .expect("http_reqs metric");
    assert_eq!(http_reqs.agg.sum, EXPECTED_ITERATIONS);
    let iterations = result
        .summary
        .metrics
        .iter()
        .find(|m| m.metric == "iterations")
        .expect("iterations metric");
    assert_eq!(iterations.agg.sum, EXPECTED_ITERATIONS);
    let checks_total: u64 = result
        .summary
        .checks
        .iter()
        .map(|c| c.passes + c.fails)
        .sum();
    assert_eq!(checks_total, EXPECTED_ITERATIONS as u64);
}

// ---------------------------------------------------------------------------
// setup() failure: the early-abort path must return an error, not hang.
// ---------------------------------------------------------------------------

/// A `ScriptEngine` whose `setup()` always fails, in order to exercise
/// `Engine::run`'s setup-failure path (engine.rs's Err arm) — the run must
/// return an error rather than hang. `instantiate` is never expected to run:
/// `run()` returns before any VU script would be created.
struct FailingSetupScript;

impl ScriptEngine for FailingSetupScript {
    fn setup(&self, _host: &mut dyn ScriptHost) -> Result<serde_json::Value, ScriptError> {
        Err(ScriptError::Runtime("setup always fails (test)".into()))
    }

    fn teardown(
        &self,
        _host: &mut dyn ScriptHost,
        _setup_data: serde_json::Value,
    ) -> Result<(), ScriptError> {
        Ok(())
    }

    fn instantiate(&self) -> Result<Box<dyn VuScript>, ScriptError> {
        unreachable!("setup() fails before any VU script is instantiated")
    }

    fn has_function(&self, _name: &str) -> bool {
        false
    }
}

const NEVER_RUN_PLAN: &str = r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request: { url: "http://x/never" }
"#;

/// Shard mode (default: zero outputs). Without cancelling the shard-mode
/// `done` token on this path, `agg_task.await` would hang forever — nothing
/// else ever signals the shard loop to stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setup_failure_returns_error_not_hang_shard_mode() {
    let (reg, _handler) = registry();
    let loaded =
        loadr_config::load_str(NEVER_RUN_PLAN, &loadr_config::LoadOptions::new()).expect("parse");
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: reg,
            script: Some(Arc::new(FailingSetupScript)),
            ..Default::default()
        },
    )
    .expect("engine");

    let outcome = tokio::time::timeout(Duration::from_secs(10), engine.run()).await;
    match outcome {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("expected setup() failure to fail the run"),
        Err(_) => panic!(
            "run() hung instead of returning an error after setup() failed (shard mode) — \
             the shard `done` token was not cancelled on this path"
        ),
    }
}

/// Bus mode (a `wants_samples` output forces it). This is the pre-existing
/// path: without dropping both `bus` and the setup `vu` (which each hold a
/// live sender) before awaiting `agg_task`, `samples_rx.recv()` never sees
/// `None` and the aggregator task — and so `agg_task.await` — never
/// completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setup_failure_returns_error_not_hang_bus_mode() {
    let (reg, _handler) = registry();
    let output = CollectingOutput {
        samples: Arc::new(parking_lot::Mutex::new(Vec::new())),
    };
    let loaded =
        loadr_config::load_str(NEVER_RUN_PLAN, &loadr_config::LoadOptions::new()).expect("parse");
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: reg,
            script: Some(Arc::new(FailingSetupScript)),
            outputs: vec![Box::new(output)],
            ..Default::default()
        },
    )
    .expect("engine");

    let outcome = tokio::time::timeout(Duration::from_secs(10), engine.run()).await;
    match outcome {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("expected setup() failure to fail the run"),
        Err(_) => panic!(
            "run() hung instead of returning an error after setup() failed (bus mode) — \
             `bus`/`vu` sender clones were not dropped before awaiting agg_task"
        ),
    }
}
