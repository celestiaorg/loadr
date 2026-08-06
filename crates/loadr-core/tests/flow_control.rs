//! Integration tests for the Locust/Gatling-inspired flow-control steps,
//! feeder strategies and throttling — driven through the real engine with a
//! mock protocol handler. (JS-condition while/if coverage lives in the CLI
//! e2e suite, which wires the real QuickJS engine.)

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use loadr_core::{
    Engine, EngineOptions, PreparedRequest, ProtocolError, ProtocolHandler, ProtocolRegistry,
    ProtocolResponse, VuContext,
};

/// A protocol handler that records every request URL and returns 200.
#[derive(Default)]
struct RecordingHandler {
    count: AtomicU64,
    urls: parking_lot::Mutex<Vec<String>>,
}

#[async_trait]
impl ProtocolHandler for RecordingHandler {
    fn name(&self) -> &str {
        "http"
    }
    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.urls.lock().push(request.url.clone());
        Ok(ProtocolResponse {
            status: 200,
            protocol_version: "HTTP/1.1".into(),
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

struct BlockingHandler {
    started: AtomicU64,
    completed: AtomicU64,
    released: AtomicBool,
    started_notify: tokio::sync::Notify,
    release_notify: tokio::sync::Notify,
}

impl BlockingHandler {
    fn new() -> Arc<Self> {
        Arc::new(BlockingHandler {
            started: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            released: AtomicBool::new(false),
            started_notify: tokio::sync::Notify::new(),
            release_notify: tokio::sync::Notify::new(),
        })
    }

    async fn wait_started(&self) {
        loop {
            let started = self.started_notify.notified();
            if self.started.load(Ordering::Acquire) > 0 {
                return;
            }
            started.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_waiters();
    }
}

#[async_trait]
impl ProtocolHandler for BlockingHandler {
    fn name(&self) -> &str {
        "http"
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        self.started.fetch_add(1, Ordering::Release);
        self.started_notify.notify_waiters();
        loop {
            let released = self.release_notify.notified();
            if self.released.load(Ordering::Acquire) {
                break;
            }
            released.await;
        }
        self.completed.fetch_add(1, Ordering::Relaxed);
        Ok(ProtocolResponse {
            status: 200,
            protocol_version: "HTTP/1.1".into(),
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

fn registry(handler: Arc<RecordingHandler>) -> ProtocolRegistry {
    let mut reg = ProtocolRegistry::new();
    reg.register(handler);
    reg.register_alias("https", "http");
    reg
}

fn engine_with_handler<H>(
    yaml: &str,
    handler: Arc<H>,
    snapshot_interval: Duration,
) -> loadr_core::Engine
where
    H: ProtocolHandler + 'static,
{
    let loaded = loadr_config::load_str(yaml, &loadr_config::LoadOptions::new()).expect("parse");
    let mut reg = ProtocolRegistry::new();
    reg.register(handler);
    Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: reg,
            snapshot_interval,
            ..Default::default()
        },
    )
    .expect("engine")
}

async fn run(yaml: &str, handler: Arc<RecordingHandler>) -> loadr_core::RunResult {
    let loaded = loadr_config::load_str(yaml, &loadr_config::LoadOptions::new()).expect("parse");
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: registry(handler),
            ..Default::default()
        },
    )
    .expect("engine");
    engine.run().await.expect("run")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreach_iterates_a_list() {
    let handler = Arc::new(RecordingHandler::default());
    run(
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - foreach:
          items: [ "a", "b", "c", "d" ]
          var: sku
          steps:
            - request: { url: "http://x/item/${sku}" }
"#,
        handler.clone(),
    )
    .await;
    let urls = handler.urls.lock();
    assert_eq!(urls.len(), 4);
    for sku in ["a", "b", "c", "d"] {
        assert!(
            urls.iter().any(|u| u.ends_with(&format!("/item/{sku}"))),
            "missing {sku}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_branches_on_value() {
    let handler = Arc::new(RecordingHandler::default());
    run(
        r#"
variables: { tier: gold }
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - switch:
          value: "${vars.tier}"
          cases:
            gold:   [ { request: { url: "http://x/gold" } } ]
            silver: [ { request: { url: "http://x/silver" } } ]
          default: [ { request: { url: "http://x/default" } } ]
"#,
        handler.clone(),
    )
    .await;
    let urls = handler.urls.lock();
    assert_eq!(urls.len(), 1);
    assert!(urls[0].ends_with("/gold"), "got {}", urls[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn during_loops_for_a_duration() {
    let handler = Arc::new(RecordingHandler::default());
    let start = std::time::Instant::now();
    run(
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - during:
          duration: 1s
          steps:
            - request: { url: "http://x/poll" }
            - think_time: { type: constant, duration: 100ms }
"#,
        handler.clone(),
    )
    .await;
    let elapsed = start.elapsed();
    let n = handler.count.load(Ordering::Relaxed);
    assert!(
        elapsed >= std::time::Duration::from_secs(1),
        "looped for {elapsed:?}"
    );
    // ~10 polls in 1s at 100ms pacing; allow slack.
    assert!((5..=15).contains(&n), "polls={n}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_runs_branches_concurrently() {
    // Each branch sleeps; concurrent execution finishes far faster than serial.
    #[derive(Default)]
    struct SlowHandler {
        count: AtomicU64,
    }
    #[async_trait]
    impl ProtocolHandler for SlowHandler {
        fn name(&self) -> &str {
            "http"
        }
        async fn execute(
            &self,
            _ctx: &mut VuContext,
            request: &PreparedRequest,
        ) -> Result<ProtocolResponse, ProtocolError> {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            self.count.fetch_add(1, Ordering::Relaxed);
            Ok(ProtocolResponse {
                status: 200,
                protocol_version: "HTTP/1.1".into(),
                url: request.url.clone(),
                ..Default::default()
            })
        }
    }
    let handler = Arc::new(SlowHandler::default());
    let mut reg = ProtocolRegistry::new();
    reg.register(handler.clone());
    let loaded = loadr_config::load_str(
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - parallel:
          branches:
            - [ { request: { url: "http://x/a" } } ]
            - [ { request: { url: "http://x/b" } } ]
            - [ { request: { url: "http://x/c" } } ]
"#,
        &loadr_config::LoadOptions::new(),
    )
    .expect("parse");
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: reg,
            ..Default::default()
        },
    )
    .expect("engine");
    let start = std::time::Instant::now();
    engine.run().await.expect("run");
    let elapsed = start.elapsed();
    assert_eq!(handler.count.load(Ordering::Relaxed), 3);
    // 3 × 200ms serial = 600ms; concurrent should be well under 450ms.
    assert!(
        elapsed < std::time::Duration::from_millis(450),
        "took {elapsed:?} (not concurrent)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rendezvous_releases_when_users_arrive() {
    // Two VUs must both reach the barrier before either proceeds.
    let handler = Arc::new(RecordingHandler::default());
    let start = std::time::Instant::now();
    run(
        r#"
scenarios:
  s:
    executor: per-vu-iterations
    vus: 2
    iterations: 1
    flow:
      - rendezvous: { name: gate, users: 2, timeout: 5s }
      - request: { url: "http://x/after-gate" }
"#,
        handler.clone(),
    )
    .await;
    // Both VUs passed the gate (no timeout) and made their request.
    assert_eq!(handler.count.load(Ordering::Relaxed), 2);
    assert!(
        start.elapsed() < std::time::Duration::from_secs(5),
        "barrier timed out"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeat_runs_steps_n_times() {
    let handler = Arc::new(RecordingHandler::default());
    run(
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 3
    flow:
      - repeat:
          times: 4
          steps:
            - request: { url: "http://x/hit" }
"#,
        handler.clone(),
    )
    .await;
    // 3 iterations × 4 repeats = 12 requests.
    assert_eq!(handler.count.load(Ordering::Relaxed), 12);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_weighted_favours_heavy_branch() {
    let handler = Arc::new(RecordingHandler::default());
    run(
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 4
    iterations: 400
    flow:
      - random:
          strategy: weighted
          choices:
            - { weight: 9, steps: [ { request: { url: "http://x/common" } } ] }
            - { weight: 1, steps: [ { request: { url: "http://x/rare" } } ] }
"#,
        handler.clone(),
    )
    .await;
    let urls = handler.urls.lock();
    let common = urls.iter().filter(|u| u.ends_with("/common")).count();
    let rare = urls.iter().filter(|u| u.ends_with("/rare")).count();
    assert_eq!(common + rare, 400);
    // ~90/10 split; allow generous slack but the heavy branch must dominate.
    assert!(common > rare * 3, "common={common} rare={rare}");
    assert!(rare > 0, "rare branch should still fire sometimes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_robin_alternates() {
    let handler = Arc::new(RecordingHandler::default());
    run(
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 6
    flow:
      - random:
          strategy: round_robin
          choices:
            - { steps: [ { request: { url: "http://x/a" } } ] }
            - { steps: [ { request: { url: "http://x/b" } } ] }
            - { steps: [ { request: { url: "http://x/c" } } ] }
"#,
        handler.clone(),
    )
    .await;
    let urls = handler.urls.lock();
    assert_eq!(urls.iter().filter(|u| u.ends_with("/a")).count(), 2);
    assert_eq!(urls.iter().filter(|u| u.ends_with("/b")).count(), 2);
    assert_eq!(urls.iter().filter(|u| u.ends_with("/c")).count(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn throttle_caps_request_rate() {
    let handler = Arc::new(RecordingHandler::default());
    let start = std::time::Instant::now();
    run(
        r#"
scenarios:
  s:
    executor: constant-vus
    vus: 10
    duration: 2s
    throttle: { requests_per_second: 20 }
    flow:
      - request: { url: "http://x/throttled" }
"#,
        handler.clone(),
    )
    .await;
    let elapsed = start.elapsed().as_secs_f64();
    let count = handler.count.load(Ordering::Relaxed);
    // At 20 rps for ~2s, expect roughly 40 requests — never wildly more.
    assert!(
        count <= 55,
        "throttle exceeded: {count} requests in {elapsed:.1}s"
    );
    assert!(
        count >= 20,
        "throttle too aggressive: only {count} requests"
    );
}

fn assert_cancelled_request_metrics(result: &loadr_core::RunResult) {
    let http_reqs = result
        .summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_reqs")
        .expect("http_reqs");
    assert_eq!(http_reqs.agg.sum, 1.0);

    let duration = result
        .summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_req_duration")
        .expect("http_req_duration");
    assert_eq!(duration.agg.count, 1);
    assert!(duration.agg.max.is_some());

    let failed = result
        .summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_req_failed")
        .expect("http_req_failed");
    assert_eq!(failed.agg.count, 1);
    assert_eq!(failed.agg.sum, 1.0);

    let cancelled = result
        .summary
        .snapshot
        .series
        .iter()
        .find(|s| {
            s.metric == "http_req_failed"
                && s.tags.get("error_kind").map(String::as_str) == Some("cancelled")
        })
        .expect("cancelled failure series");
    assert_eq!(cancelled.agg.count, 1);
    assert_eq!(cancelled.agg.sum, 1.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_kill_records_cancelled_in_flight_request_metrics() {
    let handler = BlockingHandler::new();
    let engine = engine_with_handler(
        r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 30s
    flow:
      - request: { url: "http://x/slow" }
"#,
        handler.clone(),
        Duration::from_millis(100),
    );
    let handle = engine.handle();
    let run = tokio::spawn(engine.run());

    handler.wait_started().await;
    handle.kill("test kill");

    let result = match tokio::time::timeout(Duration::from_secs(5), run).await {
        Ok(joined) => joined.expect("run task").expect("run"),
        Err(_) => {
            handler.release();
            panic!("run did not finish after hard kill");
        }
    };

    assert_eq!(handler.completed.load(Ordering::Relaxed), 0);
    assert_cancelled_request_metrics(&result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_stop_deadline_records_cancelled_in_flight_request_metrics() {
    let handler = BlockingHandler::new();
    let engine = engine_with_handler(
        r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 30s
    graceful_stop: 50ms
    flow:
      - request: { url: "http://x/slow" }
"#,
        handler.clone(),
        Duration::from_millis(100),
    );
    let handle = engine.handle();
    let run = tokio::spawn(engine.run());

    handler.wait_started().await;
    handle.stop("test stop");

    let result = match tokio::time::timeout(Duration::from_secs(5), run).await {
        Ok(joined) => joined.expect("run task").expect("run"),
        Err(_) => {
            handler.release();
            panic!("run did not finish after graceful stop deadline");
        }
    };

    assert_eq!(handler.completed.load(Ordering::Relaxed), 0);
    assert_cancelled_request_metrics(&result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_in_flight_gauge_tracks_active_requests() {
    let handler = BlockingHandler::new();
    let engine = engine_with_handler(
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request: { url: "http://x/slow" }
"#,
        handler.clone(),
        Duration::from_millis(50),
    );
    let handle = engine.handle();
    let mut snapshots = handle.watch_snapshots();
    let run = tokio::spawn(engine.run());

    handler.wait_started().await;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let snap = snapshots.borrow_and_update().clone();
        let max_in_flight = snap
            .series
            .iter()
            .filter(|s| s.metric == "requests_in_flight")
            .filter_map(|s| s.agg.max)
            .fold(0.0_f64, f64::max);
        if max_in_flight >= 1.0 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            handler.release();
            panic!("requests_in_flight never reached 1");
        }
        snapshots.changed().await.expect("snapshots");
    }

    handler.release();
    let result = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("run timeout")
        .expect("run task")
        .expect("run");

    assert_eq!(handler.completed.load(Ordering::Relaxed), 1);
    let gauge = result
        .summary
        .metrics
        .iter()
        .find(|m| m.metric == "requests_in_flight")
        .expect("requests_in_flight");
    assert!(gauge.agg.max.unwrap_or_default() >= 1.0);
    assert_eq!(gauge.agg.last, Some(0.0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn weighted_data_pick_random() {
    // `random` feeder never exhausts even with on_eof: stop.
    let handler = Arc::new(RecordingHandler::default());
    run(
        r#"
data:
  ids:
    type: inline
    pick: random
    on_eof: stop
    rows:
      - { id: a }
      - { id: b }
      - { id: c }
scenarios:
  s:
    executor: shared-iterations
    vus: 2
    iterations: 50
    flow:
      - request: { url: "http://x/item/${data.ids.id}" }
"#,
        handler.clone(),
    )
    .await;
    // 50 iterations all completed (no early stop from the feeder).
    assert_eq!(handler.count.load(Ordering::Relaxed), 50);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeline_is_captured_over_the_run() {
    use std::time::Duration;
    let handler = Arc::new(RecordingHandler::default());
    let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 2
    duration: 1500ms
    flow:
      - request: { url: "http://x/ping" }
      - think_time: { type: constant, duration: 50ms }
"#;
    let loaded = loadr_config::load_str(yaml, &loadr_config::LoadOptions::new()).expect("parse");
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: registry(handler.clone()),
            // Fast snapshots so a short run still yields several timeline points.
            snapshot_interval: Duration::from_millis(250),
            ..Default::default()
        },
    )
    .expect("engine");
    let result = engine.run().await.expect("run");

    let tl = &result.summary.timeline;
    assert!(
        tl.len() >= 3,
        "expected several timeline points, got {}",
        tl.len()
    );
    // Elapsed time is monotonically non-decreasing.
    for w in tl.windows(2) {
        assert!(w[1].elapsed_secs >= w[0].elapsed_secs);
    }
    // Active VUs were captured (constant-vus = 2).
    assert!(
        tl.iter().any(|p| p.active_vus >= 1.0),
        "active VUs never recorded"
    );
    // Throughput was observed at some point.
    assert!(tl.iter().any(|p| p.rps > 0.0), "no throughput recorded");
    // Latency percentiles present once requests have completed.
    assert!(
        tl.iter().any(|p| p.latency_p95.is_some()),
        "no latency percentiles recorded"
    );
}
