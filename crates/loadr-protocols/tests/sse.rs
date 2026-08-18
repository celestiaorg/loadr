//! Integration tests for the SSE protocol handler against the in-process test
//! server.
//!
//! Redis is no longer a built-in protocol: it lives in the runtime-loadable
//! `loadr-plugin-redis` crate, whose RESP encode/decode unit tests and gated
//! real-server integration tests (`LOADR_TEST_REDIS_URL`) live there.

use std::path::Path;
use std::sync::Arc;

use loadr_config::HttpDefaults;
use loadr_core::data::DataFeeds;
use loadr_core::metrics::{MetricRegistry, MetricsBus, Tags};
use loadr_core::protocol::{PreparedRequest, ProtocolHandler, RequestOptions};
use loadr_core::vu::{RunContext, VuContext};
use loadr_protocols::SseHandler;
use loadr_testserver::SseTestServer;

fn vu() -> VuContext {
    let data = DataFeeds::load(&Default::default(), Path::new("."), Default::default())
        .expect("data feeds");
    let run = Arc::new(RunContext {
        variables: serde_json::Map::new(),
        secrets: Default::default(),
        env: Default::default(),
        data,
        registry: Arc::new(MetricRegistry::with_builtins()),
        base_dir: ".".into(),
        setup_data: parking_lot::RwLock::new(serde_json::Value::Null),
    });
    let (bus, _rx) = MetricsBus::new();
    VuContext::new(1, Arc::from("t"), Arc::new(Tags::new()), bus, run, true)
}

fn req(url: &str, protocol: &str) -> PreparedRequest {
    PreparedRequest {
        name: url.to_string(),
        protocol: protocol.to_string(),
        method: "GET".to_string(),
        url: url.to_string(),
        headers: Vec::new(),
        body: bytes::Bytes::new(),
        timeout: std::time::Duration::from_secs(10),
        follow_redirects: false,
        max_redirects: 0,
        options: RequestOptions::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_streams_events_with_timings() {
    let server = SseTestServer::spawn().await.expect("sse server");
    let handler = SseHandler::new(&HttpDefaults::default(), Path::new(".")).expect("sse handler");
    let mut vu = vu();

    // Use the `sse://` alias to exercise scheme normalisation.
    let url = server.url().replacen("http://", "sse://", 1);
    let mut request = req(&url, "sse");
    request.options.plugin = Some(serde_json::json!({ "events": 5 }));

    let response = handler.execute(&mut vu, &request).await.expect("execute");

    assert!(
        response.error.is_none(),
        "unexpected sse error: {:?}",
        response.error
    );
    assert_eq!(response.status, 200);
    assert_eq!(response.protocol_version, "sse");

    let events_received = response.extras["events_received"].as_u64().unwrap();
    assert!(
        events_received >= 1,
        "expected events, got {events_received}"
    );
    assert_eq!(events_received, 5, "expected all 5 events");

    let last = &response.extras["last_event"];
    assert_eq!(last["type"], "tick");
    assert!(last["data"].as_str().unwrap().contains("\"n\":4"));

    assert!(response.bytes_received > 0);
    assert!(response.timings.duration_ms.is_finite());
    assert!(response.timings.duration_ms >= 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_until_substring_stops_early() {
    let server = SseTestServer::spawn().await.expect("sse server");
    let handler = SseHandler::new(&HttpDefaults::default(), Path::new(".")).expect("sse handler");
    let mut vu = vu();

    let mut request = req(&server.url(), "sse");
    request.options.plugin = Some(serde_json::json!({ "until": "\"n\":1" }));

    let response = handler.execute(&mut vu, &request).await.expect("execute");
    assert!(response.error.is_none());
    // Stops on the event whose data contains the substring (n=1 → 2 events).
    let events = response.extras["events_received"].as_u64().unwrap();
    assert_eq!(events, 2, "should stop at the matching event");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_connection_failure_is_reported() {
    let handler = SseHandler::new(&HttpDefaults::default(), Path::new(".")).expect("sse handler");
    let mut vu = vu();
    let request = req("sse://127.0.0.1:1/events", "sse");

    let response = handler.execute(&mut vu, &request).await.expect("execute");
    assert_eq!(response.status, 0);
    assert!(response.error.is_some());
    assert_eq!(response.protocol_version, "sse");
}
