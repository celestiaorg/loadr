//! Lifecycle and race coverage for the open-model arrival dispatcher's
//! claim-budget hand-off (executor.rs `run_arrival_rate` + `ArrivalGate`):
//! pool growth bounds, parked workers racing small budgets, pause/resume
//! backlog, deadline/soft-stop closure, cancellation, and the ramping
//! schedule sharing the same loop. Harness modeled on flow_control.rs.
//!
//! All tests assume the default 5ms dispatch tick — `dispatch_tick()` is a
//! process-wide `OnceLock`, so `LOADR_DISPATCH_TICK_US` must never be set
//! here (tick-specific coverage lives in the CLI e2e suite, which pins the
//! env var on the spawned child process).

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use loadr_core::{
    Engine, EngineOptions, PreparedRequest, ProtocolError, ProtocolHandler, ProtocolRegistry,
    ProtocolResponse, RunResult, VuContext,
};

/// Records iteration starts, concurrency, and the VUs that served them, then
/// answers 200 after a fixed delay (the "iteration duration").
struct TrackingHandler {
    delay: Duration,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
    vus: parking_lot::Mutex<HashSet<u64>>,
    starts: parking_lot::Mutex<Vec<Instant>>,
}

impl TrackingHandler {
    fn new(delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            delay,
            in_flight: AtomicU64::new(0),
            max_in_flight: AtomicU64::new(0),
            vus: parking_lot::Mutex::new(HashSet::new()),
            starts: parking_lot::Mutex::new(Vec::new()),
        })
    }

    fn start_spread(&self) -> Duration {
        let starts = self.starts.lock();
        match (starts.iter().min(), starts.iter().max()) {
            (Some(first), Some(last)) => *last - *first,
            _ => Duration::ZERO,
        }
    }
}

#[async_trait]
impl ProtocolHandler for TrackingHandler {
    fn name(&self) -> &str {
        "http"
    }
    async fn execute(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        self.starts.lock().push(Instant::now());
        self.vus.lock().insert(ctx.vu_id);
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(ProtocolResponse {
            status: 200,
            protocol_version: "HTTP/1.1".into(),
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

fn engine(yaml: &str, handler: Arc<TrackingHandler>) -> Engine {
    let mut protocols = ProtocolRegistry::new();
    protocols.register(handler);
    let loaded = loadr_config::load_str(yaml, &loadr_config::LoadOptions::new()).expect("parse");
    Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols,
            ..Default::default()
        },
    )
    .expect("engine")
}

/// Sum a metric across all tag sets in the end-of-run summary.
fn metric_sum(result: &RunResult, name: &str) -> f64 {
    result
        .summary
        .metrics
        .iter()
        .filter(|m| m.metric == name)
        .map(|m| m.agg.sum)
        .sum()
}

/// Watchdog: a hung dispatcher/worker join must fail the test, not the CI job.
async fn run_within(engine: Engine, limit: Duration) -> RunResult {
    tokio::time::timeout(limit, engine.run())
        .await
        .expect("run exceeded watchdog — dispatcher or worker join hung")
        .expect("run")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn growth_stays_within_max_vus() {
    let handler = TrackingHandler::new(Duration::from_millis(50));
    let result = run_within(
        engine(
            r#"
scenarios:
  s:
    executor: constant-arrival-rate
    rate: 400
    duration: 1s
    pre_allocated_vus: 1
    max_vus: 4
    graceful_stop: 1s
    flow:
      - request: { url: "http://x/ping" }
"#,
            handler.clone(),
        ),
        Duration::from_secs(15),
    )
    .await;

    let distinct_vus = handler.vus.lock().len() as u64;
    assert!(distinct_vus > 1, "pool never grew past pre_allocated");
    assert!(distinct_vus <= 4, "pool grew past max_vus: {distinct_vus}");
    let max_in_flight = handler.max_in_flight.load(Ordering::SeqCst);
    assert!(
        max_in_flight <= 4,
        "concurrency exceeded max_vus: {max_in_flight}"
    );
    assert!(
        metric_sum(&result, "dropped_iterations") > 0.0,
        "a 10x-oversubscribed pool must drop arrivals"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_workers_race_small_budgets_without_lost_wakes() {
    let handler = TrackingHandler::new(Duration::ZERO);
    let result = run_within(
        engine(
            r#"
scenarios:
  s:
    executor: constant-arrival-rate
    rate: 200
    duration: 1s
    pre_allocated_vus: 16
    max_vus: 16
    graceful_stop: 0s
    flow:
      - request: { url: "http://x/ping" }
"#,
            handler,
        ),
        Duration::from_secs(15),
    )
    .await;

    let iterations = metric_sum(&result, "iterations");
    let dropped = metric_sum(&result, "dropped_iterations");
    // A lost wake starves every later batch (drops pile up, iterations
    // crater); scheduling noise costs at most a stray batch.
    assert!(
        (150.0..=270.0).contains(&iterations),
        "iterations={iterations} dropped={dropped}"
    );
    assert!(dropped <= 2.0, "idle pool dropped arrivals: {dropped}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_publishes_no_backlog() {
    let handler = TrackingHandler::new(Duration::ZERO);
    let engine = engine(
        r#"
scenarios:
  s:
    executor: constant-arrival-rate
    rate: 200
    duration: 1500ms
    pre_allocated_vus: 8
    max_vus: 8
    graceful_stop: 0s
    flow:
      - request: { url: "http://x/ping" }
"#,
        handler,
    );
    let handle = engine.handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        handle.pause(true);
        tokio::time::sleep(Duration::from_millis(500)).await;
        handle.pause(false);
    });
    let result = run_within(engine, Duration::from_secs(15)).await;

    let iterations = metric_sum(&result, "iterations");
    let dropped = metric_sum(&result, "dropped_iterations");
    // ~1.0s of active schedule at 200/s. A 500ms paused-time backlog would
    // add ~100 on resume; drops beyond a stray batch mean pause leaked
    // published budget into expiry instead of workers.
    assert!(
        iterations + dropped <= 230.0,
        "paused time produced arrivals: iterations={iterations} dropped={dropped}"
    );
    assert!(iterations >= 100.0, "iterations={iterations}");
    assert!(dropped <= 8.0, "dropped={dropped}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn natural_deadline_starts_no_late_iterations() {
    let handler = TrackingHandler::new(Duration::from_millis(20));
    let began = Instant::now();
    run_within(
        engine(
            r#"
scenarios:
  s:
    executor: constant-arrival-rate
    rate: 200
    duration: 500ms
    pre_allocated_vus: 4
    max_vus: 4
    graceful_stop: 1s
    flow:
      - request: { url: "http://x/ping" }
"#,
            handler.clone(),
        ),
        Duration::from_secs(15),
    )
    .await;

    // Parked workers must exit on the close broadcast, not sit out the 1s
    // grace window; only in-flight iterations (≤20ms) may straddle closure.
    let elapsed = began.elapsed();
    assert!(
        elapsed < Duration::from_millis(1200),
        "run took {elapsed:?}"
    );
    let spread = handler.start_spread();
    assert!(
        spread <= Duration::from_millis(650),
        "iteration started after closure: spread {spread:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn soft_stop_starts_no_late_iterations() {
    let handler = TrackingHandler::new(Duration::from_millis(20));
    let engine = engine(
        r#"
scenarios:
  s:
    executor: constant-arrival-rate
    rate: 200
    duration: 10s
    pre_allocated_vus: 4
    max_vus: 4
    graceful_stop: 1s
    flow:
      - request: { url: "http://x/ping" }
"#,
        handler.clone(),
    );
    let handle = engine.handle();
    let began = Instant::now();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.stop("test stop");
    });
    run_within(engine, Duration::from_secs(15)).await;

    let elapsed = began.elapsed();
    assert!(
        elapsed < Duration::from_millis(1500),
        "run took {elapsed:?}"
    );
    let spread = handler.start_spread();
    assert!(
        spread <= Duration::from_millis(400),
        "iteration started after soft stop: spread {spread:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_releases_workers_parked_during_pause() {
    let handler = TrackingHandler::new(Duration::ZERO);
    let engine = engine(
        r#"
scenarios:
  s:
    executor: constant-arrival-rate
    rate: 100
    duration: 700ms
    pre_allocated_vus: 4
    max_vus: 4
    graceful_stop: 5s
    flow:
      - request: { url: "http://x/ping" }
"#,
        handler,
    );
    let handle = engine.handle();
    let began = Instant::now();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.pause(true); // never resumed: the run ends while paused
    });
    run_within(engine, Duration::from_secs(15)).await;
    // Workers idle in the paused branch must observe closure at the natural
    // deadline, not wait out the 5s grace window.
    let elapsed = began.elapsed();
    assert!(elapsed < Duration::from_secs(3), "run took {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_wakes_parked_workers() {
    let handler = TrackingHandler::new(Duration::ZERO);
    let engine = engine(
        r#"
scenarios:
  s:
    executor: constant-arrival-rate
    rate: 10
    duration: 30s
    pre_allocated_vus: 8
    max_vus: 8
    graceful_stop: 5s
    flow:
      - request: { url: "http://x/ping" }
"#,
        handler,
    );
    let handle = engine.handle();
    let began = Instant::now();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.kill("test kill");
    });
    // A kill may surface as an aborted result or an error; only promptness
    // matters here — parked workers must not pin the join for 30s.
    let _ = tokio::time::timeout(Duration::from_secs(15), engine.run())
        .await
        .expect("run exceeded watchdog — parked workers not woken");
    let elapsed = began.elapsed();
    assert!(elapsed < Duration::from_secs(2), "run took {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ramping_schedule_uses_the_same_loop() {
    let handler = TrackingHandler::new(Duration::ZERO);
    let result = run_within(
        engine(
            r#"
scenarios:
  s:
    executor: ramping-arrival-rate
    start_rate: 0
    stages:
      - { duration: 1s, target: 400 }
    pre_allocated_vus: 8
    max_vus: 16
    graceful_stop: 500ms
    flow:
      - request: { url: "http://x/ping" }
"#,
            handler,
        ),
        Duration::from_secs(15),
    )
    .await;

    let iterations = metric_sum(&result, "iterations");
    let dropped = metric_sum(&result, "dropped_iterations");
    // Integral of 0→400/s over 1s ≈ 200 arrivals.
    assert!(
        (140.0..=270.0).contains(&(iterations + dropped)),
        "iterations={iterations} dropped={dropped}"
    );
    assert!(dropped <= 8.0, "idle pool dropped arrivals: {dropped}");
}
