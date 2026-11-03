//! End-to-end tests: LocalBackend + a real loadr_core::Engine (with a mock
//! protocol handler) behind WebUi, exercised over real HTTP.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use loadr_core::protocol::{PreparedRequest, ProtocolHandler, ProtocolRegistry, Timings};
use loadr_core::vu::VuContext;
use loadr_core::{Engine, EngineOptions, ProtocolError, ProtocolResponse};
use loadr_plugin_webui::{
    AuthConfig, EngineLauncher, LocalBackend, WebUi, WebUiConfig, WebUiHandle,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Mock HTTP protocol: status 200, 1-3ms latency, realistic timings.
#[derive(Default)]
struct MockHttp {
    counter: AtomicU64,
}

#[async_trait::async_trait]
impl ProtocolHandler for MockHttp {
    fn name(&self) -> &str {
        "http"
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let ms = 1 + self.counter.fetch_add(1, Ordering::Relaxed) % 3;
        tokio::time::sleep(Duration::from_millis(ms)).await;
        Ok(ProtocolResponse {
            status: 200,
            status_text: "OK".to_string(),
            protocol_version: "HTTP/1.1".to_string(),
            body: Bytes::from_static(b"ok"),
            timings: Timings {
                waiting_ms: ms as f64,
                duration_ms: ms as f64,
                ..Default::default()
            },
            bytes_sent: 120,
            bytes_received: 240,
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

/// Launcher that builds a REAL engine wired to the mock protocol.
fn mock_launcher() -> EngineLauncher {
    Arc::new(|plan, base_dir, run_id| {
        let mut protocols = ProtocolRegistry::new();
        protocols.register(Arc::new(MockHttp::default()));
        let opts = EngineOptions {
            run_id: Some(run_id),
            protocols,
            ..Default::default()
        };
        let engine = Engine::new(plan, base_dir, opts).map_err(|e| e.to_string())?;
        let handle = engine.handle();
        let task = tokio::spawn(engine.run());
        Ok((handle, task))
    })
}

struct TestServer {
    addr: SocketAddr,
    _handle: WebUiHandle,
    _dir: tempfile::TempDir,
    client: Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
    auth: Option<String>,
}

impl TestServer {
    async fn start(auth: AuthConfig, send_auth: Option<&str>) -> TestServer {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = Arc::new(
            LocalBackend::new(dir.path().to_path_buf(), mock_launcher()).expect("backend"),
        );
        let handle = WebUi::serve(WebUiConfig {
            bind: "127.0.0.1:0".parse().expect("addr"),
            auth,
            backend,
        })
        .await
        .expect("serve");
        TestServer {
            addr: handle.addr,
            _handle: handle,
            _dir: dir,
            client: Client::builder(TokioExecutor::new()).build_http(),
            auth: send_auth.map(str::to_string),
        }
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> http::Request<Full<Bytes>> {
        let mut builder = http::Request::builder()
            .method(method)
            .uri(format!("http://{}{path}", self.addr));
        if let Some(auth) = &self.auth {
            builder = builder.header("authorization", auth);
        }
        match body {
            Some(json) => builder
                .header("content-type", "application/json")
                .body(Full::from(Bytes::from(json.to_string())))
                .expect("request"),
            None => builder.body(Full::default()).expect("request"),
        }
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> (http::StatusCode, serde_json::Value) {
        let res = self
            .client
            .request(self.request(method, path, body))
            .await
            .expect("http call");
        let status = res.status();
        let bytes = res.into_body().collect().await.expect("body").to_bytes();
        let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::String(
            String::from_utf8_lossy(&bytes).to_string(),
        ));
        (status, value)
    }

    async fn raw(&self, path: &str) -> (http::StatusCode, http::HeaderMap, Bytes) {
        let res = self
            .client
            .request(self.request("GET", path, None))
            .await
            .expect("http call");
        let status = res.status();
        let headers = res.headers().clone();
        let bytes = res.into_body().collect().await.expect("body").to_bytes();
        (status, headers, bytes)
    }

    /// Poll the run until it reaches a terminal state.
    async fn wait_finished(&self, run_id: &str, timeout: Duration) -> serde_json::Value {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let (status, detail) = self.call("GET", &format!("/api/runs/{run_id}"), None).await;
            assert_eq!(status, http::StatusCode::OK, "run detail: {detail}");
            let state = detail["run"]["state"].as_str().unwrap_or("").to_string();
            if matches!(
                state.as_str(),
                "finished" | "degraded" | "aborted" | "failed"
            ) {
                return detail;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "run {run_id} did not finish in time (state {state})"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

const TINY_PLAN: &str = r#"
name: tiny
scenarios:
  hit:
    executor: shared-iterations
    vus: 2
    iterations: 20
    flow:
      - request:
          url: http://mock.local/
"#;

const CONSTANT_PLAN: &str = r#"
name: steady
scenarios:
  steady:
    executor: constant-vus
    vus: 3
    duration: 3s
    flow:
      - request:
          url: http://mock.local/
"#;

const LONG_PLAN: &str = r#"
name: long
scenarios:
  long:
    executor: constant-vus
    vus: 2
    duration: 60s
    flow:
      - request:
          url: http://mock.local/
"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_basic_bearer_and_401() {
    let auth = AuthConfig {
        basic: Some(("admin".to_string(), "s3cret".to_string())),
        tokens: vec!["tok-123".to_string()],
    };

    // No credentials → 401 with WWW-Authenticate.
    let server = TestServer::start(auth.clone(), None).await;
    let (status, headers, _) = server.raw("/api/runs").await;
    assert_eq!(status, http::StatusCode::UNAUTHORIZED);
    assert!(headers.contains_key("www-authenticate"));

    // /healthz is open.
    let (status, _, body) = server.raw("/healthz").await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(&body[..], b"ok");

    // Build metadata is public so the pre-authentication UI can render it.
    let (status, version) = server.call("GET", "/api/version", None).await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(version["version"], loadr_core::build_info::VERSION);
    assert_eq!(version["revision"], loadr_core::build_info::GIT_REVISION);

    // Wrong basic → 401.
    use base64::Engine as _;
    let bad = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("admin:wrong")
    );
    let server_bad = TestServer::start(auth.clone(), Some(&bad)).await;
    let (status, _) = server_bad.call("GET", "/api/runs", None).await;
    assert_eq!(status, http::StatusCode::UNAUTHORIZED);

    // Valid basic works.
    let good = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("admin:s3cret")
    );
    let server_basic = TestServer::start(auth.clone(), Some(&good)).await;
    let (status, runs) = server_basic.call("GET", "/api/runs", None).await;
    assert_eq!(status, http::StatusCode::OK);
    assert!(runs.is_array());

    // Valid bearer works.
    let server_bearer = TestServer::start(auth.clone(), Some("Bearer tok-123")).await;
    let (status, _) = server_bearer.call("GET", "/api/overview", None).await;
    assert_eq!(status, http::StatusCode::OK);

    // Token via query string (for EventSource-style clients).
    let (status, _, _) = server.raw("/api/logs?token=tok-123").await;
    assert_eq!(status, http::StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_lifecycle_and_summary() {
    let server = TestServer::start(AuthConfig::default(), None).await;

    let (status, created) = server
        .call(
            "POST",
            "/api/runs",
            Some(serde_json::json!({ "yaml": TINY_PLAN })),
        )
        .await;
    assert_eq!(status, http::StatusCode::CREATED, "create: {created}");
    let run_id = created["run_id"].as_str().expect("run_id").to_string();

    let detail = server.wait_finished(&run_id, Duration::from_secs(15)).await;
    assert_eq!(detail["run"]["state"], "finished");
    assert!(detail["run"]["passed"].is_null());
    assert_eq!(detail["run"]["scenarios"][0], "hit");

    // A run without thresholds is finished, not presented as a gated pass.
    let (status, runs) = server.call("GET", "/api/runs", None).await;
    assert_eq!(status, http::StatusCode::OK);
    let run = runs
        .as_array()
        .and_then(|a| a.iter().find(|r| r["run_id"] == run_id.as_str()))
        .expect("run in list")
        .clone();
    assert_eq!(run["state"], "finished");
    assert!(run["passed"].is_null());
    assert_eq!(run["name"], "tiny");
    assert!(
        run["ended_ms"].as_u64().expect("ended_ms")
            >= run["started_ms"].as_u64().expect("started_ms")
    );

    // Summary: exactly 20 requests.
    let (status, summary) = server
        .call("GET", &format!("/api/runs/{run_id}/summary"), None)
        .await;
    assert_eq!(status, http::StatusCode::OK);
    let metrics = summary["metrics"].as_array().expect("metrics");
    let http_reqs = metrics
        .iter()
        .find(|m| m["metric"] == "http_reqs")
        .expect("http_reqs metric");
    assert_eq!(http_reqs["agg"]["sum"], 20.0);
    let request_reqs = metrics
        .iter()
        .find(|m| m["metric"] == "request_reqs")
        .expect("canonical request rollup");
    assert_eq!(request_reqs["agg"]["sum"], 20.0);
    assert_eq!(summary["final_metrics"]["attempts"], 20);
    assert_eq!(summary["final_metrics"]["successful_requests"], 20);
    assert_eq!(summary["final_metrics"]["failed_requests"], 0);
    assert_eq!(
        summary["final_metrics"]["metric_contract"]["rate_denominator"],
        "full observed duration"
    );

    // Snapshot endpoint serves the last-known snapshot for finished runs.
    let (status, snapshot) = server
        .call("GET", &format!("/api/runs/{run_id}/snapshot"), None)
        .await;
    assert_eq!(status, http::StatusCode::OK);
    assert!(snapshot["series"].as_array().is_some_and(|s| !s.is_empty()));

    // Finished Overview uses frozen full-run metrics, never a live interval.
    let (status, overview) = server.call("GET", "/api/overview", None).await;
    assert_eq!(status, http::StatusCode::OK);
    assert!(overview["metrics"].is_null());
    let failures = &overview["final_metrics"]["failures"];
    assert!(
        failures.is_object(),
        "failures breakdown present: {overview}"
    );
    assert!(failures["event_total"].is_number());
    assert!(failures["failed_requests"].is_number());
    assert!(failures["by_status"].is_array());
    assert!(failures["by_exception"].is_array());
    assert_eq!(
        overview["final_metrics"]["metric_contract"]["latency_percentile_quality"],
        "exact_merged_histogram"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validate_returns_located_diagnostics() {
    let server = TestServer::start(AuthConfig::default(), None).await;

    // Type error: vus must be a number.
    let broken = "name: broken\nscenarios:\n  s:\n    executor: constant-vus\n    vus: \"lots\"\n    duration: 1s\n    flow:\n      - request:\n          url: http://mock.local/\n";
    let (status, body) = server
        .call(
            "POST",
            "/api/validate",
            Some(serde_json::json!({ "yaml": broken })),
        )
        .await;
    assert_eq!(status, http::StatusCode::OK);
    let diags = body["diagnostics"].as_array().expect("diagnostics");
    assert!(!diags.is_empty());
    assert!(diags[0]["line"].as_u64().expect("line number") >= 1);
    assert_eq!(diags[0]["severity"], "error");

    // Valid plan → no errors.
    let (_, body) = server
        .call(
            "POST",
            "/api/validate",
            Some(serde_json::json!({ "yaml": TINY_PLAN })),
        )
        .await;
    let diags = body["diagnostics"].as_array().expect("diagnostics");
    assert!(diags.iter().all(|d| d["severity"] != "error"), "{diags:?}");

    // Starting a broken run returns 422 with diagnostics.
    let (status, body) = server
        .call(
            "POST",
            "/api/runs",
            Some(serde_json::json!({ "yaml": broken })),
        )
        .await;
    assert_eq!(status, http::StatusCode::UNPROCESSABLE_ENTITY);
    assert!(body["diagnostics"]
        .as_array()
        .is_some_and(|d| !d.is_empty()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tests_crud_round_trip() {
    let server = TestServer::start(AuthConfig::default(), None).await;

    let (status, body) = server
        .call(
            "PUT",
            "/api/tests/smoke-1",
            Some(serde_json::json!({ "yaml": TINY_PLAN })),
        )
        .await;
    assert_eq!(status, http::StatusCode::OK, "{body}");

    let (status, tests) = server.call("GET", "/api/tests", None).await;
    assert_eq!(status, http::StatusCode::OK);
    let tests = tests.as_array().expect("tests array");
    assert_eq!(tests.len(), 1);
    assert_eq!(tests[0]["name"], "smoke-1");
    assert!(tests[0]["yaml"]
        .as_str()
        .expect("yaml")
        .contains("shared-iterations"));
    assert!(tests[0]["updated_ms"].as_u64().expect("updated") > 0);

    // Update, then delete.
    let (status, _) = server
        .call(
            "PUT",
            "/api/tests/smoke-1",
            Some(serde_json::json!({ "yaml": CONSTANT_PLAN })),
        )
        .await;
    assert_eq!(status, http::StatusCode::OK);
    let (_, tests) = server.call("GET", "/api/tests", None).await;
    assert!(tests[0]["yaml"]
        .as_str()
        .expect("yaml")
        .contains("constant-vus"));

    let (status, _) = server.call("DELETE", "/api/tests/smoke-1", None).await;
    assert_eq!(status, http::StatusCode::NO_CONTENT);
    let (_, tests) = server.call("GET", "/api/tests", None).await;
    assert_eq!(tests.as_array().map(Vec::len), Some(0));

    // Deleting again → 404; path traversal names rejected.
    let (status, _) = server.call("DELETE", "/api/tests/smoke-1", None).await;
    assert_eq!(status, http::StatusCode::NOT_FOUND);
    let (status, _) = server
        .call(
            "PUT",
            "/api/tests/..%2Fevil",
            Some(serde_json::json!({ "yaml": "x" })),
        )
        .await;
    assert_eq!(status, http::StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_stream_emits_snapshots() {
    let server = TestServer::start(AuthConfig::default(), None).await;
    let (status, created) = server
        .call(
            "POST",
            "/api/runs",
            Some(serde_json::json!({ "yaml": CONSTANT_PLAN })),
        )
        .await;
    assert_eq!(status, http::StatusCode::CREATED, "{created}");
    let run_id = created["run_id"].as_str().expect("run_id").to_string();

    let res = server
        .client
        .request(server.request("GET", &format!("/api/runs/{run_id}/stream"), None))
        .await
        .expect("sse connect");
    assert_eq!(res.status(), http::StatusCode::OK);
    let content_type = res
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(content_type.contains("text/event-stream"), "{content_type}");

    // Read frames until we have multiple parsed snapshot events.
    let mut body = res.into_body();
    let mut buf = String::new();
    let mut snapshots: Vec<serde_json::Value> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while snapshots.len() < 3 && tokio::time::Instant::now() < deadline {
        let frame = tokio::time::timeout(Duration::from_secs(3), body.frame()).await;
        let Ok(Some(Ok(frame))) = frame else { break };
        if let Some(data) = frame.data_ref() {
            buf.push_str(&String::from_utf8_lossy(data));
        }
        while let Some(idx) = buf.find("\n\n") {
            let raw = buf[..idx].to_string();
            buf = buf[idx + 2..].to_string();
            let mut event = "message";
            let mut data = String::new();
            for line in raw.lines() {
                if let Some(rest) = line.strip_prefix("event:") {
                    event = rest.trim();
                } else if let Some(rest) = line.strip_prefix("data:") {
                    data.push_str(rest.trim_start());
                }
            }
            if event == "snapshot" && !data.is_empty() {
                let parsed: serde_json::Value = serde_json::from_str(&data).expect("snapshot json");
                snapshots.push(parsed);
            }
        }
    }
    assert!(
        snapshots.len() >= 2,
        "expected at least 2 snapshot events, got {}",
        snapshots.len()
    );
    for snap in &snapshots {
        assert!(
            snap["rps"].is_number() || snap["rps"].is_null(),
            "invalid rps compatibility field: {snap}"
        );
        assert!(snap["latency"].is_object());
        assert!(snap["state"].is_string());
    }
    // At least one mid-run snapshot should show one compatible request sample.
    assert!(
        snapshots.iter().any(|snapshot| {
            snapshot["attempts_per_second"].as_f64().unwrap_or(0.0) > 0.0
                && snapshot["successful_per_second"].is_number()
                && snapshot["failed_per_second"].is_number()
                && snapshot["request_error_rate"].is_number()
        }),
        "no snapshot showed traffic"
    );

    server.wait_finished(&run_id, Duration::from_secs(15)).await;
}

/// Poll `GET /api/runs/{id}` until `is_paused` reaches `expected`. The pause
/// command propagates to the run loop asynchronously, so a single read right
/// after the POST can race the not-yet-applied flag (flaky on slow CI runners).
async fn wait_is_paused(server: &TestServer, run_id: &str, expected: bool) {
    // 500 * 20ms = 10s. The async pause propagation can exceed 4s on slow CI
    // runners (observed flaking on macos-latest), so give it real headroom.
    for _ in 0..500 {
        let (_, detail) = server
            .call("GET", &format!("/api/runs/{run_id}"), None)
            .await;
        if detail["is_paused"] == serde_json::Value::Bool(expected) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("run {run_id} did not report is_paused={expected} within timeout");
}

/// Poll until the run reports the given lifecycle state (e.g. "running").
/// `POST /api/runs` returns before the run handle is registered, so a pause
/// issued immediately can race a not-yet-ready run and be silently dropped.
async fn wait_run_state(server: &TestServer, run_id: &str, state: &str) {
    // 500 * 20ms = 10s — matches wait_is_paused; slow CI runners need the room.
    for _ in 0..500 {
        let (_, detail) = server
            .call("GET", &format!("/api/runs/{run_id}"), None)
            .await;
        if detail["run"]["state"] == serde_json::Value::String(state.to_string()) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("run {run_id} did not reach state={state} within timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_endpoint_flips_is_paused() {
    let server = TestServer::start(AuthConfig::default(), None).await;
    let (status, created) = server
        .call(
            "POST",
            "/api/runs",
            Some(serde_json::json!({ "yaml": LONG_PLAN })),
        )
        .await;
    assert_eq!(status, http::StatusCode::CREATED, "{created}");
    let run_id = created["run_id"].as_str().expect("run_id").to_string();

    // Wait until the run loop is actually running before pausing, so the pause
    // can't race run-handle registration.
    wait_run_state(&server, &run_id, "running").await;

    let (status, body) = server
        .call(
            "POST",
            &format!("/api/runs/{run_id}/pause"),
            Some(serde_json::json!({ "paused": true })),
        )
        .await;
    assert_eq!(status, http::StatusCode::OK, "{body}");
    wait_is_paused(&server, &run_id, true).await;

    let (status, _) = server
        .call(
            "POST",
            &format!("/api/runs/{run_id}/pause"),
            Some(serde_json::json!({ "paused": false })),
        )
        .await;
    assert_eq!(status, http::StatusCode::OK);
    wait_is_paused(&server, &run_id, false).await;

    // Clean up: kill the long run.
    let (status, _) = server
        .call(
            "POST",
            &format!("/api/runs/{run_id}/stop"),
            Some(serde_json::json!({ "kill": true })),
        )
        .await;
    assert_eq!(status, http::StatusCode::OK);
    server.wait_finished(&run_id, Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_endpoint_ends_run_early() {
    let server = TestServer::start(AuthConfig::default(), None).await;
    let (status, created) = server
        .call(
            "POST",
            "/api/runs",
            Some(serde_json::json!({ "yaml": LONG_PLAN })),
        )
        .await;
    assert_eq!(status, http::StatusCode::CREATED, "{created}");
    let run_id = created["run_id"].as_str().expect("run_id").to_string();

    // Let it actually start iterating, then request a graceful stop.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started = std::time::Instant::now();
    let (status, _) = server
        .call("POST", &format!("/api/runs/{run_id}/stop"), None)
        .await;
    assert_eq!(status, http::StatusCode::OK);
    server.wait_finished(&run_id, Duration::from_secs(20)).await;
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "graceful stop took too long"
    );

    // Stopping a finished run is a client error.
    let (status, _) = server
        .call("POST", &format!("/api/runs/{run_id}/stop"), None)
        .await;
    assert_eq!(status, http::StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overview_shape() {
    let server = TestServer::start(AuthConfig::default(), None).await;

    let (status, capabilities) = server.call("GET", "/api/capabilities", None).await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(capabilities["mode"], "standalone");
    assert_eq!(capabilities["logs_available"], true);
    assert_eq!(capabilities["persistent_history"], true);

    // Empty backend: nulls and zero counts.
    let (status, overview) = server.call("GET", "/api/overview", None).await;
    assert_eq!(status, http::StatusCode::OK);
    assert!(overview["run"].is_null());
    assert_eq!(overview["total_runs"], 0);

    let (_, created) = server
        .call(
            "POST",
            "/api/runs",
            Some(serde_json::json!({ "yaml": TINY_PLAN, "name": "ov-test" })),
        )
        .await;
    let run_id = created["run_id"].as_str().expect("run_id").to_string();
    server.wait_finished(&run_id, Duration::from_secs(15)).await;

    let (status, overview) = server.call("GET", "/api/overview", None).await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(overview["run"]["run_id"], run_id.as_str());
    assert_eq!(overview["run"]["name"], "ov-test");
    assert_eq!(overview["total_runs"], 1);
    assert_eq!(overview["live_runs"], 0);
    assert!(overview["metrics"].is_null());
    let metrics = &overview["final_metrics"];
    assert_eq!(metrics["window"], "full_observed_run");
    assert_eq!(metrics["attempts"], 20);
    assert_eq!(metrics["successful_requests"], 20);
    assert_eq!(metrics["failed_requests"], 0);
    assert!(metrics["average_attempts_per_second"].is_number());
    assert!(metrics["latency"]["p95"].is_number());

    // Logs captured backend activity.
    let (_, logs) = server.call("GET", "/api/logs", None).await;
    let logs = logs.as_array().expect("logs");
    assert!(logs
        .iter()
        .any(|l| l["message"].as_str().unwrap_or("").contains("started")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_spa_served_with_content_types() {
    let server = TestServer::start(AuthConfig::default(), None).await;

    let (status, headers, body) = server.raw("/").await;
    assert_eq!(status, http::StatusCode::OK);
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("text/html"), "{ct}");
    let html = String::from_utf8_lossy(&body);
    assert!(html.contains("loadr"), "index.html must contain 'loadr'");
    assert!(html.contains("app.js"));
    assert!(html.contains("id=\"build-version\""));

    let (status, headers, body) = server.raw("/app.js").await;
    assert_eq!(status, http::StatusCode::OK);
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("javascript"), "{ct}");
    assert!(!body.is_empty());
    let app_js = String::from_utf8_lossy(&body);
    assert!(app_js.contains("/api/version"));
    assert!(app_js.contains("title: a.name"));
    assert!(app_js.contains("title: a.id"));
    assert!(app_js.contains("a.peer_addr"));
    assert!(app_js.contains("Response status"));
    assert!(app_js.contains("response_status"));
    assert!(app_js.contains("category,protocol,cause,count,share_pct,denominator_count"));
    assert!(app_js.contains("Request attempts/s"));
    assert!(app_js.contains("FINAL — full observed run"));
    assert!(app_js.contains("thresholds failed"));
    assert!(app_js.contains("failureEventTotal"));
    assert!(app_js.contains("No failures recorded yet"));
    assert!(!app_js.contains("Failed HTTP status"));
    assert!(!app_js.contains("failGroupCard('by_status', 'HTTP status')"));

    let (status, headers, _) = server.raw("/style.css").await;
    assert_eq!(status, http::StatusCode::OK);
    assert!(headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .contains("text/css"));

    // SPA fallback: unknown paths serve the shell.
    let (status, headers, body) = server.raw("/runs/deep/link").await;
    assert_eq!(status, http::StatusCode::OK);
    assert!(headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .contains("text/html"));
    assert!(String::from_utf8_lossy(&body).contains("loadr"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scale_rejects_non_external_scenarios() {
    let server = TestServer::start(AuthConfig::default(), None).await;
    let (_, created) = server
        .call(
            "POST",
            "/api/runs",
            Some(serde_json::json!({ "yaml": LONG_PLAN })),
        )
        .await;
    let run_id = created["run_id"].as_str().expect("run_id").to_string();

    let (status, body) = server
        .call(
            "POST",
            &format!("/api/runs/{run_id}/scale"),
            Some(serde_json::json!({ "scenario": "long", "vus": 10 })),
        )
        .await;
    assert_eq!(status, http::StatusCode::BAD_REQUEST);
    assert!(body["error"]
        .as_str()
        .unwrap_or("")
        .contains("not externally controlled"));

    let (_, detail) = server
        .call("GET", &format!("/api/runs/{run_id}"), None)
        .await;
    assert_eq!(
        detail["externally_controlled"].as_array().map(Vec::len),
        Some(0)
    );

    let (status, _) = server
        .call(
            "POST",
            &format!("/api/runs/{run_id}/stop"),
            Some(serde_json::json!({ "kill": true })),
        )
        .await;
    assert_eq!(status, http::StatusCode::OK);
    server.wait_finished(&run_id, Duration::from_secs(15)).await;
}
