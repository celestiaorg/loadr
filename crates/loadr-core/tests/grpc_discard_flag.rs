//! Flow-level coverage for the gRPC lazy-decode gate: `prepare` computes
//! `GrpcRequest.discard_response_body` from whether the plan's
//! `extract`/`assert`/`checks` read the response body, ANDed with whether a
//! script `afterRequest` hook is present. Driven through the real engine
//! with a mock protocol handler (no real gRPC server — `reflection: true`
//! passes config validation without dialing) and a mock script engine (no
//! real JS runtime).

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use loadr_core::{
    Engine, EngineOptions, PreparedRequest, ProtocolError, ProtocolHandler, ProtocolRegistry,
    ProtocolResponse, ScriptEngine, ScriptError, ScriptHost, VuContext, VuScript,
};

/// Records the `discard_response_body` flag `prepare` computed for each gRPC
/// request; returns a canned response so `extract`/`assert`/`checks` in the
/// plan have something to match against.
#[derive(Default)]
struct RecordingGrpcHandler {
    modes: parking_lot::Mutex<Vec<(bool, bool, usize)>>,
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
        self.modes.lock().push((
            grpc.discard_response_body,
            grpc.protobuf_only_response,
            grpc.protobuf_checks
                .as_ref()
                .map_or(0, |checks| checks.len()),
        ));
        let actual_code = grpc
            .message
            .as_ref()
            .and_then(|message| message.get("responseCode"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        Ok(ProtocolResponse {
            status: 0,
            protocol_version: "grpc".to_string(),
            url: request.url.clone(),
            body: bytes::Bytes::from_static(br#"{"ok":true}"#),
            grpc_protobuf_outcomes: grpc
                .protobuf_checks
                .as_deref()
                .into_iter()
                .flatten()
                .map(|check| loadr_core::protocol::GrpcProtobufFieldOutcome {
                    id: check.id,
                    pass: check
                        .equals
                        .as_ref()
                        .and_then(serde_json::Value::as_i64)
                        .is_none_or(|expected| expected == actual_code),
                    detail: None,
                    actual_code: Some(actual_code),
                    missing: false,
                })
                .collect(),
            ..Default::default()
        })
    }
}

/// Minimal `ScriptEngine`/`VuScript`: reports a fixed, configurable set of
/// "exported" functions and no-ops everything else — enough to drive
/// `prepare`'s runtime `afterRequest` gate without a real JS runtime.
#[derive(Clone, Default)]
struct MockScriptEngine {
    functions: HashSet<String>,
}

impl MockScriptEngine {
    fn with_functions(names: &[&str]) -> Self {
        MockScriptEngine {
            functions: names.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl ScriptEngine for MockScriptEngine {
    fn setup(&self, _host: &mut dyn ScriptHost) -> Result<serde_json::Value, ScriptError> {
        Ok(serde_json::Value::Null)
    }

    fn teardown(
        &self,
        _host: &mut dyn ScriptHost,
        _setup_data: serde_json::Value,
    ) -> Result<(), ScriptError> {
        Ok(())
    }

    fn instantiate(&self) -> Result<Box<dyn VuScript>, ScriptError> {
        Ok(Box::new(MockVuScript {
            functions: self.functions.clone(),
        }))
    }

    fn has_function(&self, name: &str) -> bool {
        self.functions.contains(name)
    }
}

struct MockVuScript {
    functions: HashSet<String>,
}

impl VuScript for MockVuScript {
    fn call_function(
        &mut self,
        _host: &mut dyn ScriptHost,
        _name: &str,
        _args: &[serde_json::Value],
    ) -> Result<serde_json::Value, ScriptError> {
        Ok(serde_json::Value::Null)
    }

    fn eval(
        &mut self,
        _host: &mut dyn ScriptHost,
        _code: &str,
    ) -> Result<serde_json::Value, ScriptError> {
        Ok(serde_json::Value::Null)
    }

    fn has_function(&self, name: &str) -> bool {
        self.functions.contains(name)
    }
}

/// Run one gRPC request through the real engine; return the
/// `discard_response_body` flag(s) `prepare` computed for it.
async fn run_recording(
    yaml: &str,
    script: Option<Arc<dyn ScriptEngine>>,
) -> (Vec<(bool, bool, usize)>, loadr_core::Summary) {
    let loaded = loadr_config::load_str(yaml, &loadr_config::LoadOptions::new()).expect("parse");
    let handler = Arc::new(RecordingGrpcHandler::default());
    let mut protocols = ProtocolRegistry::new();
    protocols.register(handler.clone());
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols,
            script,
            ..Default::default()
        },
    )
    .expect("engine");
    let summary = engine.run().await.expect("run").summary;
    let modes = handler.modes.lock().clone();
    (modes, summary)
}

async fn response_modes(
    yaml: &str,
    script: Option<Arc<dyn ScriptEngine>>,
) -> Vec<(bool, bool, usize)> {
    run_recording(yaml, script).await.0
}

async fn discard_flags(yaml: &str, script: Option<Arc<dyn ScriptEngine>>) -> Vec<bool> {
    response_modes(yaml, script)
        .await
        .into_iter()
        .map(|mode| mode.0)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_request_discards_body() {
    let flags = discard_flags(
        r#"
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
            method: UnaryEcho
            message: { message: "hi" }
"#,
        None,
    )
    .await;
    assert_eq!(flags, vec![true]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_assert_still_discards_body() {
    let flags = discard_flags(
        r#"
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
            method: UnaryEcho
            message: { message: "hi" }
          assert:
            - { type: status, equals: 0 }
"#,
        None,
    )
    .await;
    assert_eq!(flags, vec![true]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protobuf_field_check_decodes_without_json_materialization() {
    let modes = response_modes(
        r#"
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
            method: UnaryEcho
            message: { message: "hi" }
          checks:
            - { type: protobuf_field, name: admission_accepted, field: code, equals: 0 }
"#,
        None,
    )
    .await;
    assert_eq!(modes, vec![(false, true, 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protobuf_failure_group_reaches_final_summary() {
    let (_, summary) = run_recording(
        r#"
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
            method: UnaryEcho
            message: { responseCode: 18 }
          checks:
            - type: protobuf_field
              name: admission_accepted
              field: code
              equals: 0
              failure_groups: { 18: WrongShard, 20: PoolAtCapacity }
"#,
        None,
    )
    .await;
    let check = summary
        .checks
        .iter()
        .find(|check| check.name == "admission_accepted")
        .expect("check summary");
    assert_eq!(check.fails, 1);
    assert_eq!(check.failure_groups.len(), 1);
    assert_eq!(check.failure_groups[0].code, Some(18));
    assert_eq!(check.failure_groups[0].label, "WrongShard");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonpath_assert_forces_decode() {
    let flags = discard_flags(
        r#"
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
            method: UnaryEcho
            message: { message: "hi" }
          assert:
            - { type: jsonpath, expression: "$.ok", equals: true }
"#,
        None,
    )
    .await;
    assert_eq!(flags, vec![false]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_forces_decode() {
    let flags = discard_flags(
        r#"
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
            method: UnaryEcho
            message: { message: "hi" }
          extract:
            - { type: jsonpath, name: ok, expression: "$.ok" }
"#,
        None,
    )
    .await;
    assert_eq!(flags, vec![false]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_contains_check_forces_decode() {
    let flags = discard_flags(
        r#"
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
            method: UnaryEcho
            message: { message: "hi" }
          checks:
            - { type: body_contains, value: "ok" }
"#,
        None,
    )
    .await;
    assert_eq!(flags, vec![false]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn js_condition_forces_decode() {
    let flags = discard_flags(
        r#"
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
            method: UnaryEcho
            message: { message: "hi" }
          assert:
            - { type: js, expression: "response.status === 0" }
"#,
        None,
    )
    .await;
    assert_eq!(flags, vec![false]);
}

const BARE_REQUEST_YAML: &str = r#"
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
            method: UnaryEcho
            message: { message: "hi" }
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_request_hook_forces_decode() {
    let script: Arc<dyn ScriptEngine> =
        Arc::new(MockScriptEngine::with_functions(&["afterRequest"]));
    let flags = discard_flags(BARE_REQUEST_YAML, Some(script)).await;
    assert_eq!(flags, vec![false]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn script_without_after_request_still_discards_body() {
    let script: Arc<dyn ScriptEngine> = Arc::new(MockScriptEngine::with_functions(&["setup"]));
    let flags = discard_flags(BARE_REQUEST_YAML, Some(script)).await;
    assert_eq!(flags, vec![true]);
}
