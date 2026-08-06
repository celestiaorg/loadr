//! End-to-end coverage for extraction: the classic `extract:` forms and the
//! new fused check-chains, driven through the real engine with a mock handler
//! that returns a fixed JSON body and records every request URL. We assert on
//! the recorded URLs (proving an extracted value flowed into a later request)
//! and on the run summary's `checks` (proving inline chain validation is
//! recorded like a standalone check).

#[path = "support/grpc_fixture.rs"]
mod grpc_fixture;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use loadr_core::{
    Engine, EngineOptions, PreparedRequest, ProtocolError, ProtocolHandler, ProtocolRegistry,
    ProtocolResponse, VuContext,
};

/// Returns a fixed JSON body for every request and records the URLs hit.
struct JsonHandler {
    body: &'static str,
    urls: parking_lot::Mutex<Vec<String>>,
}

#[async_trait]
impl ProtocolHandler for JsonHandler {
    fn name(&self) -> &str {
        "grpc"
    }
    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        self.urls.lock().push(request.url.clone());
        Ok(ProtocolResponse {
            status: 200,
            protocol_version: "gRPC".into(),
            url: request.url.clone(),
            body: Bytes::from_static(self.body.as_bytes()),
            headers: vec![("X-Token".into(), "  raw-token  ".into())],
            ..Default::default()
        })
    }
}

fn registry(handler: Arc<JsonHandler>) -> ProtocolRegistry {
    let mut reg = ProtocolRegistry::new();
    reg.register(handler);
    reg.register_alias("grpcs", "grpc");
    reg
}

const CATALOG: &str = r#"{
  "items": [
    {"id": 1, "name": "alpha", "price": 9, "stock": 0},
    {"id": 2, "name": "beta",  "price": 19, "stock": 5},
    {"id": 3, "name": "gamma", "price": 5,  "stock": 2}
  ],
  "order": {"id": "ord-42", "status": "PENDING"}
}"#;

async fn run_with(body: &'static str, yaml: &str) -> (loadr_core::RunResult, Arc<JsonHandler>) {
    let handler = Arc::new(JsonHandler {
        body,
        urls: parking_lot::Mutex::new(Vec::new()),
    });
    let loaded = grpc_fixture::load(yaml);
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            protocols: registry(handler.clone()),
            ..Default::default()
        },
    )
    .expect("engine");
    let result = engine.run().await.expect("run");
    (result, handler)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_jsonpath_extract_still_works() {
    let (_result, handler) = run_with(
        CATALOG,
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: http://x/catalog
          extract:
            - { type: jsonpath, name: order_id, expression: "$.order.id" }
      - request:
          url: "http://x/orders/${order_id}"
"#,
    )
    .await;
    let urls = handler.urls.lock();
    assert!(
        urls.iter().any(|u| u.ends_with("/orders/ord-42")),
        "classic extract did not feed downstream request: {urls:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_jmespath_filter_and_transform() {
    // Cheapest in-stock item is "gamma" (price 5, stock 2); lowercase + a
    // prepend prove the transform pipeline runs.
    let (_result, handler) = run_with(
        CATALOG,
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: http://x/catalog
          extract:
            - chain: cheapest
              jmespath: "items[?stock > `0`] | sort_by(@, &price)[0].name"
              as: string
              transform: [uppercase]
      - request:
          url: "http://x/buy/${cheapest}"
"#,
    )
    .await;
    let urls = handler.urls.lock();
    assert!(
        urls.iter().any(|u| u.ends_with("/buy/GAMMA")),
        "chain jmespath/transform did not produce expected value: {urls:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_header_transform_pipeline() {
    let (_result, handler) = run_with(
        CATALOG,
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: http://x/login
          extract:
            - chain: auth
              header: X-Token
              transform: [trim, { prepend: "Bearer " }]
      - request:
          url: "http://x/me?h=${auth}"
"#,
    )
    .await;
    let urls = handler.urls.lock();
    assert!(
        urls.iter().any(|u| u.contains("Bearer raw-token")),
        "chain header transform failed: {urls:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_inline_check_records_to_checks_metric() {
    let (result, _handler) = run_with(
        CATALOG,
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: http://x/catalog
          extract:
            - chain: status
              jsonpath: "$.order.status"
              transform: [lowercase]
              check: { one_of: [pending, shipped, delivered] }
"#,
    )
    .await;
    let check = result
        .summary
        .checks
        .iter()
        .find(|c| c.name == "status")
        .expect("chain check should appear in summary checks");
    assert_eq!(check.passes, 1);
    assert_eq!(check.fails, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_check_failure_aborts_iteration() {
    // The chain's check fails (status is PENDING, not 'shipped') and asks to
    // abort the iteration, so the second request must never fire.
    let (result, handler) = run_with(
        CATALOG,
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: http://x/catalog
          extract:
            - chain: status
              jsonpath: "$.order.status"
              transform: [lowercase]
              check: { equals: shipped, on_failure: abort_iteration }
      - request:
          url: http://x/should-not-run
"#,
    )
    .await;
    let urls = handler.urls.lock();
    assert!(
        !urls.iter().any(|u| u.ends_with("/should-not-run")),
        "abort_iteration on a failed chain check did not stop the iteration: {urls:?}"
    );
    let check = result
        .summary
        .checks
        .iter()
        .find(|c| c.name == "status")
        .expect("failed chain check should still be recorded");
    assert_eq!(check.fails, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_and_chain_mix_in_one_request() {
    let (_result, handler) = run_with(
        CATALOG,
        r#"
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: http://x/catalog
          extract:
            - { type: jsonpath, name: order_id, expression: "$.order.id" }
            - chain: top_name
              jmespath: "items | sort_by(@, &price)[-1].name"
      - request:
          url: "http://x/orders/${order_id}/item/${top_name}"
"#,
    )
    .await;
    let urls = handler.urls.lock();
    assert!(
        urls.iter().any(|u| u.ends_with("/orders/ord-42/item/beta")),
        "mixed classic + chain extraction failed: {urls:?}"
    );
}
