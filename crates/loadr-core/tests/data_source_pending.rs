//! Nonblocking source backpressure and release of abandoned leases.
use async_trait::async_trait;
use loadr_core::{
    DataSourcePlugin, Engine, EngineOptions, PluginRowCtx, PluginRowResult, PreparedRequest,
    ProtocolError, ProtocolHandler, ProtocolRegistry, ProtocolResponse, VuContext,
};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering::SeqCst},
    Arc,
};
use std::time::Duration;

#[derive(Default)]
struct Seen {
    polls: AtomicU64,
    results: AtomicU64,
    discards: AtomicU64,
    requests: AtomicU64,
}
struct Source {
    seen: Arc<Seen>,
    pending: u64,
}
impl DataSourcePlugin for Source {
    fn name(&self) -> &str {
        "feeder"
    }
    fn init(&mut self, _: &indexmap::IndexMap<String, serde_json::Value>) -> Result<(), String> {
        Ok(())
    }
    fn next_row(&self, _: &PluginRowCtx<'_>) -> Result<PluginRowResult, String> {
        let n = self.seen.polls.fetch_add(1, SeqCst);
        if n < self.pending {
            return Ok(PluginRowResult::Pending(Duration::from_secs(60)));
        }
        Ok(PluginRowResult::Row(
            [("lease".into(), n.to_string())].into_iter().collect(),
        ))
    }
    fn wants_results(&self) -> bool {
        true
    }
    fn on_result(&self, raw: String) {
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(value["row"]["lease"].is_string());
        self.seen.results.fetch_add(1, SeqCst);
    }
    fn on_discard(&self, raw: String) {
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(value["row"]["lease"].is_string());
        self.seen.discards.fetch_add(1, SeqCst);
    }
}
struct Handler {
    seen: Arc<Seen>,
    hang: bool,
}
#[async_trait]
impl ProtocolHandler for Handler {
    fn name(&self) -> &str {
        "http"
    }
    async fn execute(
        &self,
        _: &mut VuContext,
        _: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        self.seen.requests.fetch_add(1, SeqCst);
        if self.hang {
            std::future::pending::<()>().await;
        }
        Ok(ProtocolResponse {
            status: 202,
            ..Default::default()
        })
    }
}
async fn run(executor: &str, suffix: &str, pending: u64, hang: bool) -> Arc<Seen> {
    run_with_prefix(executor, suffix, pending, hang, "").await
}
async fn run_with_prefix(
    executor: &str,
    suffix: &str,
    pending: u64,
    hang: bool,
    prefix: &str,
) -> Arc<Seen> {
    let seen = Arc::new(Seen::default());
    let plan = format!(
        r#"
plugins:
  - name: feeder
    path: ./unused.so
data:
  tx:
    type: plugin
    source: feeder
scenarios:
  s:
    {executor}
    graceful_stop: 0s
    flow:
{prefix}      - request:
          url: "http://example.test/${{data.tx.lease}}{suffix}"
"#
    );
    let loaded = loadr_config::load_str(&plan, &loadr_config::LoadOptions::new()).unwrap();
    let mut protocols = ProtocolRegistry::new();
    protocols.register(Arc::new(Handler {
        seen: seen.clone(),
        hang,
    }));
    let mut data_sources: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
    data_sources.insert(
        "feeder".into(),
        Box::new(Source {
            seen: seen.clone(),
            pending,
        }),
    );
    let engine = Engine::new(
        loaded.plan,
        ".".into(),
        EngineOptions {
            protocols,
            data_sources,
            ..Default::default()
        },
    )
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), engine.run())
        .await
        .expect("no blocked worker")
        .unwrap();
    if pending > 0 {
        let failed = result
            .summary
            .metrics
            .iter()
            .find(|m| m.metric == "http_req_failed")
            .unwrap();
        assert_eq!(failed.agg.rate, Some(0.0), "pending is not an HTTP failure");
        if executor.contains("arrival-rate") {
            let dropped = result
                .summary
                .metrics
                .iter()
                .find(|m| m.metric == "dropped_iterations")
                .unwrap();
            assert!(dropped.agg.sum >= pending as f64);
        }
    }
    seen
}
#[tokio::test(flavor = "multi_thread")]
async fn closed_model_pending_does_not_consume_iteration_budget() {
    let seen = run(
        "executor: per-vu-iterations\n    vus: 1\n    iterations: 2",
        "",
        3,
        false,
    )
    .await;
    assert_eq!(seen.polls.load(SeqCst), 5);
    assert_eq!(seen.requests.load(SeqCst), 2);
    assert_eq!(seen.results.load(SeqCst), 2);
    assert_eq!(seen.discards.load(SeqCst), 0);
}
#[tokio::test(flavor = "multi_thread")]
async fn arrival_model_pending_releases_vu_for_subsequent_arrivals() {
    let seen = run("executor: constant-arrival-rate\n    rate: 100\n    duration: 200ms\n    pre_allocated_vus: 1\n    max_vus: 1", "", 2, false).await;
    assert!(
        seen.requests.load(SeqCst) > 2,
        "pending must not retire or sleep the only VU"
    );
    assert_eq!(seen.polls.load(SeqCst), seen.requests.load(SeqCst) + 2);
    assert_eq!(seen.discards.load(SeqCst), 0);
}
#[tokio::test(flavor = "multi_thread")]
async fn failed_preparation_returns_each_lease_once() {
    let seen = run(
        "executor: per-vu-iterations\n    vus: 1\n    iterations: 2",
        "${missing}",
        0,
        false,
    )
    .await;
    assert_eq!(seen.requests.load(SeqCst), 0);
    assert_eq!(seen.results.load(SeqCst), 0);
    assert_eq!(seen.discards.load(SeqCst), 2);
}
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_request_returns_its_lease_once() {
    let seen = run(
        "executor: constant-vus\n    vus: 1\n    duration: 50ms",
        "",
        0,
        true,
    )
    .await;
    assert_eq!(seen.requests.load(SeqCst), 1);
    assert_eq!(seen.results.load(SeqCst), 0);
    assert_eq!(seen.discards.load(SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn closed_model_pending_does_not_repeat_earlier_steps() {
    let seen = run_with_prefix(
        "executor: per-vu-iterations\n    vus: 1\n    iterations: 2",
        "",
        3,
        false,
        "      - request:\n          url: http://example.test/before\n",
    )
    .await;
    assert_eq!(seen.requests.load(SeqCst), 4);
    assert_eq!(seen.results.load(SeqCst), 2);
    assert_eq!(seen.polls.load(SeqCst), 5);
}
