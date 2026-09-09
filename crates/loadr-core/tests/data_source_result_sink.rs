//! Flow-level coverage for `data.<name>.on_result`: a plugin that provides a
//! result sink sees the outcome of every request that used one of its rows,
//! and sees it only for requests that actually completed.
//!
//! Driven through the real engine with a mock protocol handler and an
//! injected `DataSourcePlugin` (no dylib).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use loadr_core::{
    DataSourcePlugin, Engine, EngineOptions, PluginRowCtx, PluginRowResult, PreparedRequest,
    ProtocolError, ProtocolHandler, ProtocolRegistry, ProtocolResponse, VuContext,
};

/// Replies with the status the plan asked for, so a test can drive failures.
struct StatusHandler {
    status: i64,
}

#[async_trait]
impl ProtocolHandler for StatusHandler {
    fn name(&self) -> &str {
        "http"
    }
    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        Ok(ProtocolResponse {
            status: self.status,
            protocol_version: "HTTP/1.1".to_string(),
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

/// Hands out a counter and records every result payload it is given.
#[derive(Default)]
struct RecordingPlugin {
    counter: AtomicU64,
    sink: bool,
    results: Arc<parking_lot::Mutex<Vec<serde_json::Value>>>,
}

impl DataSourcePlugin for RecordingPlugin {
    fn name(&self) -> &str {
        "feeder"
    }

    fn init(
        &mut self,
        _source_configs: &indexmap::IndexMap<String, serde_json::Value>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn next_row(&self, _ctx: &PluginRowCtx<'_>) -> Result<PluginRowResult, String> {
        let n = self.counter.fetch_add(1, Ordering::SeqCst).to_string();
        let mut row = loadr_core::data::Row::new();
        row.insert("n".to_string(), n);
        Ok(PluginRowResult::Row(row))
    }

    fn has_result_sink(&self) -> bool {
        self.sink
    }

    fn on_result(&self, result_json: String) {
        self.results
            .lock()
            .push(serde_json::from_str(&result_json).expect("valid result JSON"));
    }
}

async fn run(yaml: &str, sink: bool, status: i64) -> Vec<serde_json::Value> {
    let loaded = loadr_config::load_str(yaml, &loadr_config::LoadOptions::new()).expect("parse");
    let mut protocols = ProtocolRegistry::new();
    protocols.register(Arc::new(StatusHandler { status }));
    let results = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut data_sources: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
    data_sources.insert(
        "feeder".to_string(),
        Box::new(RecordingPlugin {
            counter: AtomicU64::new(0),
            sink,
            results: results.clone(),
        }),
    );
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols,
            data_sources,
            ..Default::default()
        },
    )
    .expect("engine");
    engine.run().await.expect("run");
    let out = results.lock().clone();
    out
}

fn plan(on_result: bool) -> String {
    format!(
        r#"
plugins:
  - name: feeder
    path: ./libfeeder.so
data:
  rows:
    type: plugin
    source: feeder
    on_result: {on_result}
scenarios:
  s:
    executor: per-vu-iterations
    vus: 1
    iterations: 2
    flow:
      - request:
          name: submit
          url: "http://example.test/tx?n=${{data.rows.n}}"
"#
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn result_sink_receives_one_payload_per_request() {
    let results = run(&plan(true), true, 200).await;
    assert_eq!(results.len(), 2, "one result per request");

    let first = &results[0];
    assert_eq!(first["source"], "rows");
    assert_eq!(first["scenario"], "s");
    assert_eq!(first["request"], "submit");
    assert_eq!(first["row"]["n"], "0");
    assert_eq!(first["response"]["status"], 200);
    // Each request pulls a fresh row, and each result carries its own.
    assert_eq!(results[1]["row"]["n"], "1");
    assert_eq!(first["seq"], 0);
    assert_eq!(results[1]["seq"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn result_sink_sees_failed_requests_too() {
    let results = run(&plan(true), true, 500).await;
    assert_eq!(results.len(), 2);
    // The status is reported as-is; deciding what counts as success is the
    // plugin's job, not the engine's.
    assert_eq!(results[0]["response"]["status"], 500);
}

#[tokio::test(flavor = "multi_thread")]
async fn no_payloads_without_on_result() {
    let results = run(&plan(false), true, 200).await;
    assert!(results.is_empty(), "on_result is opt-in");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_payloads_when_plugin_has_no_sink() {
    let results = run(&plan(true), false, 200).await;
    assert!(
        results.is_empty(),
        "on_result is ignored when the plugin provides no sink"
    );
}
