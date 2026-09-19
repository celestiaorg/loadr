//! `body: { binary_base64: ... }` sends raw bytes: the template renders per
//! request, the base64 is decoded, and the body reaches the handler as the
//! decoded bytes with `Content-Type: application/octet-stream`. A body that is
//! not valid base64 fails the request instead of being sent as text.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use loadr_core::{
    Engine, EngineOptions, PreparedRequest, ProtocolError, ProtocolHandler, ProtocolRegistry,
    ProtocolResponse, VuContext,
};

/// Records every request body and content type.
#[derive(Default)]
struct RecordingHandler {
    bodies: parking_lot::Mutex<Vec<Vec<u8>>>,
    content_types: parking_lot::Mutex<Vec<String>>,
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
        self.bodies.lock().push(request.body.to_vec());
        self.content_types.lock().push(
            request
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
                .map(|(_, v)| v.clone())
                .unwrap_or_default(),
        );
        Ok(ProtocolResponse {
            status: 202,
            protocol_version: "HTTP/1.1".into(),
            url: request.url.clone(),
            body: Bytes::from_static(b"{}"),
            ..Default::default()
        })
    }
}

async fn run_with(yaml: &str) -> (loadr_core::RunResult, Arc<RecordingHandler>) {
    let handler = Arc::new(RecordingHandler::default());
    let mut registry = ProtocolRegistry::new();
    registry.register(handler.clone());
    registry.register_alias("https", "http");
    let loaded = loadr_config::load_str(yaml, &loadr_config::LoadOptions::new()).expect("parse");
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: registry,
            ..Default::default()
        },
    )
    .expect("engine");
    let result = engine.run().await.expect("run");
    (result, handler)
}

/// Every byte value, so a body that is not valid UTF-8 proves it was never text.
fn all_bytes() -> Vec<u8> {
    (0u8..=255).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_base64_body_arrives_as_raw_bytes() {
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode(all_bytes());
    let yaml = format!(
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: http://x/transactions
          method: POST
          body: {{ binary_base64: "{encoded}" }}
"#
    );
    let (_result, handler) = run_with(&yaml).await;

    let bodies = handler.bodies.lock();
    assert_eq!(
        bodies.as_slice(),
        &[all_bytes()],
        "body was not the decoded bytes"
    );
    assert_eq!(
        handler.content_types.lock().as_slice(),
        &["application/octet-stream".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_row_supplies_the_base64_per_request() {
    use base64::Engine as _;
    let rows: Vec<Vec<u8>> = vec![vec![0x00, 0xff, 0x01], vec![0x7f, 0x80]];
    let csv: String = rows
        .iter()
        .map(|row| base64::engine::general_purpose::STANDARD.encode(row))
        .collect::<Vec<_>>()
        .join("\n");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rows.csv");
    std::fs::write(&path, format!("body\n{csv}\n")).expect("write rows");

    let yaml = format!(
        r#"
data:
  tx:
    type: csv
    path: {path}
    mode: shared
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 2
    flow:
      - request:
          url: http://x/transactions
          method: POST
          body: {{ binary_base64: "${{data.tx.body}}" }}
"#,
        path = path.display()
    );
    let (_result, handler) = run_with(&yaml).await;

    let mut sent = handler.bodies.lock().clone();
    sent.sort();
    let mut expected = rows;
    expected.sort();
    assert_eq!(sent, expected, "each row must decode to its own bytes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_that_is_not_base64_fails_the_request() {
    let (result, handler) = run_with(
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: http://x/transactions
          method: POST
          body: { binary_base64: "not base64!" }
"#,
    )
    .await;
    assert!(
        handler.bodies.lock().is_empty(),
        "an undecodable body must never be sent"
    );
    let failed = result
        .summary
        .metrics
        .iter()
        .find(|m| m.metric == "http_req_failed")
        .map(|m| m.agg.rate.unwrap_or_default())
        .unwrap_or_default();
    assert!(failed > 0.0, "an undecodable body must fail the request");
}

#[test]
fn a_body_with_two_sources_is_refused() {
    let yaml = r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: http://x/transactions
          method: POST
          body: { binary_base64: "AAA=", json: { a: 1 } }
"#;
    let error = loadr_config::load_str(yaml, &loadr_config::LoadOptions::new())
        .expect_err("two body sources must not load");
    let loadr_config::ConfigError::Invalid(diagnostics) = &error else {
        panic!("expected a validation error, got {error}");
    };
    assert!(
        diagnostics
            .iter()
            .any(|d| d.message.contains("binary_base64")),
        "the error should name every body source: {diagnostics:?}"
    );
}
