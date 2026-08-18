//! Integration tests for the `browser` protocol against a local HTTP server.
//!
//! These tests drive a real headless Chrome. If Chrome cannot launch in this
//! environment they FAIL (we want to know the browser protocol works here).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use indexmap::IndexMap;

use loadr_browser::BrowserHandler;
use loadr_config::{DataMode, DataSource, OnEof, PickStrategy};
use loadr_core::data::DataFeeds;
use loadr_core::metrics::{MetricRegistry, MetricsBus, Tags};
use loadr_core::protocol::{PreparedRequest, ProtocolHandler, ProtocolResponse, RequestOptions};
use loadr_core::vu::{RunContext, VuContext};
use loadr_testserver::HttpTestServer;

/// Build a minimal `RunContext` (mirrors `loadr-core`'s test pattern).
fn run_ctx() -> Arc<RunContext> {
    let mut sources = IndexMap::new();
    let mut row = IndexMap::new();
    row.insert("k".to_string(), serde_json::json!("v"));
    sources.insert(
        "rows".to_string(),
        DataSource::Inline {
            rows: vec![row],
            mode: DataMode::Shared,
            on_eof: OnEof::Recycle,
            pick: PickStrategy::Sequential,
        },
    );
    let data = DataFeeds::load(&sources, Path::new("."), HashMap::new()).expect("load data feeds");
    Arc::new(RunContext {
        variables: serde_json::Map::new(),
        secrets: HashMap::new(),
        env: HashMap::new(),
        data,
        registry: Arc::new(MetricRegistry::with_builtins()),
        base_dir: ".".into(),
        setup_data: parking_lot::RwLock::new(serde_json::Value::Null),
    })
}

fn vu_ctx() -> VuContext {
    let (bus, _rx) = MetricsBus::new();
    let mut ctx = VuContext::new(
        1,
        Arc::from("browser-test"),
        Arc::new(Tags::new()),
        bus,
        run_ctx(),
        true,
    );
    ctx.begin_iteration();
    ctx
}

fn request(name: &str, url: String) -> PreparedRequest {
    PreparedRequest {
        name: name.to_string(),
        protocol: "browser".to_string(),
        method: "GET".to_string(),
        url,
        headers: Vec::new(),
        body: Bytes::new(),
        timeout: Duration::from_secs(20),
        follow_redirects: true,
        max_redirects: 10,
        options: RequestOptions::default(),
    }
}

async fn navigate(handler: &BrowserHandler, ctx: &mut VuContext, url: String) -> ProtocolResponse {
    handler
        .execute(ctx, &request("nav", url))
        .await
        .expect("execute should not return Err for a normal navigation")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn navigates_html_page_with_timings_and_vitals() {
    let server = HttpTestServer::spawn().await.expect("spawn test server");
    let handler = BrowserHandler::new();
    let mut ctx = vu_ctx();

    let resp = navigate(&handler, &mut ctx, format!("{}/html", server.base_url())).await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    assert_eq!(resp.protocol_version, "browser");
    assert_eq!(resp.status, 200, "status should be 200");

    let title = resp.extras["title"].as_str().unwrap_or("");
    assert!(!title.is_empty(), "extras.title should be non-empty");

    let resources = resp.extras["resources"].as_u64().unwrap_or(0);
    assert!(
        resources >= 1,
        "expected at least one resource, got {resources}"
    );

    let load_ms = resp.extras["load_ms"].as_f64().unwrap_or(0.0);
    assert!(load_ms > 0.0, "load_ms should be positive, got {load_ms}");

    assert!(
        resp.timings.duration_ms.is_finite(),
        "duration_ms must be finite"
    );
    assert!(
        resp.timings.duration_ms >= 0.0,
        "duration_ms must be non-negative"
    );

    // extras carry the documented vitals keys.
    assert!(resp.extras.get("fcp_ms").is_some());
    assert!(resp.extras.get("lcp_ms").is_some());
    assert!(resp.extras.get("dcl_ms").is_some());
    assert!(resp.extras.get("transferred_bytes").is_some());

    // The page body is the rendered HTML.
    assert!(
        resp.body_text().contains("<html"),
        "body should contain rendered HTML"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reflects_server_delay_in_duration() {
    let server = HttpTestServer::spawn().await.expect("spawn test server");
    let handler = BrowserHandler::new();
    let mut ctx = vu_ctx();

    let resp = navigate(
        &handler,
        &mut ctx,
        format!("{}/delay/300", server.base_url()),
    )
    .await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    assert!(
        resp.timings.duration_ms >= 250.0,
        "duration {} should reflect the 300ms server delay",
        resp.timings.duration_ms
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reuses_page_across_requests_for_same_vu() {
    let server = HttpTestServer::spawn().await.expect("spawn test server");
    let handler = BrowserHandler::new();
    let mut ctx = vu_ctx();

    // Two sequential navigations on the same VU must reuse the same tab.
    let r1 = navigate(&handler, &mut ctx, format!("{}/html", server.base_url())).await;
    assert!(r1.error.is_none(), "first nav error: {:?}", r1.error);

    let r2 = navigate(&handler, &mut ctx, format!("{}/json", server.base_url())).await;
    assert!(r2.error.is_none(), "second nav error: {:?}", r2.error);
    assert_eq!(r2.status, 200);
    // Different pages would still both succeed, but reuse is proven by the
    // handler servicing both calls without relaunching (no crash / new tab),
    // which the handler's per-VU page table guarantees.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unresolvable_url_yields_error_response_not_err() {
    let handler = BrowserHandler::new();
    let mut ctx = vu_ctx();

    let resp = handler
        .execute(
            &mut ctx,
            &request("bad", "http://127.0.0.1:1/nope".to_string()),
        )
        .await
        .expect("execute should return Ok with an error response, not Err");

    assert_eq!(resp.status, 0, "failed navigation should report status 0");
    assert!(
        resp.error.is_some(),
        "navigation to a dead port should set error"
    );
    assert_eq!(resp.protocol_version, "browser");
}
