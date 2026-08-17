//! Flow-level coverage for per-frame feeder rows: every entry of
//! `grpc.messages` (and every `stream_repeat` copy of it) must render against
//! a fresh plugin-backed row, while fields *within* one frame stay coherent.
//! Without this, a client-streaming call carrying N signed transactions sends
//! the same transaction N times and a replay-guarded node drops all but one.
//!
//! Driven through the real engine with a mock protocol handler (no real gRPC
//! server — `reflection: true` passes validation without dialing) and an
//! injected `DataSourcePlugin` (no dylib).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use loadr_core::{
    DataSourcePlugin, Engine, EngineOptions, PluginRowCtx, PluginRowResult, PreparedRequest,
    ProtocolError, ProtocolHandler, ProtocolRegistry, ProtocolResponse, VuContext,
};

/// Captures the rendered frame list of every gRPC request.
#[derive(Default)]
struct RecordingGrpcHandler {
    frames: parking_lot::Mutex<Vec<Vec<serde_json::Value>>>,
}

#[async_trait]
impl ProtocolHandler for RecordingGrpcHandler {
    fn name(&self) -> &str {
        "grpc"
    }
    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let grpc = request.options.grpc.as_ref().expect("grpc options");
        self.frames.lock().push(grpc.messages.as_ref().clone());
        Ok(ProtocolResponse {
            status: 0,
            protocol_version: "grpc".to_string(),
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

/// Emits `{"n": <counter>, "echo": <same counter>}` so a test can assert both
/// that frames differ from each other and that two fields inside one frame
/// came from the same row.
#[derive(Default)]
struct CountingPlugin {
    counter: AtomicU64,
}

impl DataSourcePlugin for CountingPlugin {
    fn name(&self) -> &str {
        "signer"
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
        row.insert("n".to_string(), n.clone());
        row.insert("echo".to_string(), n);
        Ok(PluginRowResult::Row(row))
    }
}

async fn captured_frames(yaml: &str) -> Vec<Vec<serde_json::Value>> {
    let loaded = loadr_config::load_str(yaml, &loadr_config::LoadOptions::new()).expect("parse");
    let handler = Arc::new(RecordingGrpcHandler::default());
    let mut protocols = ProtocolRegistry::new();
    protocols.register(handler.clone());
    let mut data_sources: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
    data_sources.insert("signer".to_string(), Box::new(CountingPlugin::default()));
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
    let frames = handler.frames.lock().clone();
    frames
}

fn plan(messages: &str, stream_repeat: &str) -> String {
    format!(
        r#"
plugins:
  - name: signer
    path: ./libsigner.so
data:
  signed:
    type: plugin
    source: signer
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: grpc://example.invalid:1
          grpc:
            reflection: true
            service: loadr.test.Echo
            method: ClientStreamEcho
            messages: {messages}
{stream_repeat}
"#
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_streamed_frame_gets_a_fresh_plugin_row() {
    let frames = captured_frames(&plan(
        r#"[ { tx: "${data.signed.n}" }, { tx: "${data.signed.n}" }, { tx: "${data.signed.n}" } ]"#,
        "",
    ))
    .await;
    assert_eq!(frames.len(), 1, "one request");
    let values: Vec<_> = frames[0].iter().map(|f| f["tx"].clone()).collect();
    assert_eq!(values.len(), 3);
    let unique: std::collections::HashSet<_> = values.iter().map(ToString::to_string).collect();
    assert_eq!(
        unique.len(),
        3,
        "every frame carries a distinct row: {values:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_repeat_expands_frames_each_with_a_fresh_row() {
    let frames = captured_frames(&plan(
        r#"[ { tx: "${data.signed.n}" } ]"#,
        "            stream_repeat: 8",
    ))
    .await;
    let values: Vec<_> = frames[0].iter().map(|f| f["tx"].clone()).collect();
    assert_eq!(values.len(), 8, "stream_repeat expands the frame list");
    let unique: std::collections::HashSet<_> = values.iter().map(ToString::to_string).collect();
    assert_eq!(unique.len(), 8, "no duplicates across repeats: {values:?}");
}

/// Two fields of one frame must come from the SAME row — otherwise a signed
/// payload and its nonce would disagree and the node would reject every
/// request with a confusing signature error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fields_within_one_frame_share_a_row() {
    let frames = captured_frames(&plan(
        r#"[ { tx: "${data.signed.n}", nonce: "${data.signed.echo}" },
                       { tx: "${data.signed.n}", nonce: "${data.signed.echo}" } ]"#,
        "",
    ))
    .await;
    let frame = &frames[0];
    assert_eq!(frame.len(), 2);
    for f in frame {
        assert_eq!(f["tx"], f["nonce"], "fields in one frame share a row: {f}");
    }
    assert_ne!(frame[0]["tx"], frame[1]["tx"], "frames differ");
}

/// Memory-backed sources keep per-iteration semantics: only plugin rows are
/// evicted per frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_backed_rows_are_identical_across_frames() {
    let yaml = r#"
plugins:
  - name: signer
    path: ./libsigner.so
data:
  signed:
    type: plugin
    source: signer
  users:
    type: inline
    rows: [ { user: alice } ]
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: grpc://example.invalid:1
          grpc:
            reflection: true
            service: loadr.test.Echo
            method: ClientStreamEcho
            messages: [ { u: "${data.users.user}" }, { u: "${data.users.user}" } ]
"#;
    let frames = captured_frames(yaml).await;
    assert_eq!(frames[0][0]["u"], frames[0][1]["u"]);
}
