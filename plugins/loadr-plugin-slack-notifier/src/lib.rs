//! `loadr-plugin-slack-notifier` — a native **output** plugin that posts a
//! single end-of-run summary to a [Slack incoming webhook].
//!
//! # How it plugs in
//!
//! loadr's native output ABI ([`FfiOutput`]) is the same `start` /
//! `on_samples` / `on_snapshot` / `finish` lifecycle used by the shipped
//! `native-output` example and the `datadog` plugin. Unlike a streaming
//! exporter this plugin **ignores the sample stream entirely**: `on_samples`
//! and `on_snapshot` are no-ops. It acts only in `finish`, which receives the
//! final run [`Summary`] as JSON — the same object the JSON output writes as
//! its `summary` record — renders a compact message (pass/fail verdict, p95
//! `http_req_duration`, `http_req_failed` error rate, and a line per
//! threshold), and POSTs it once to the configured webhook.
//!
//! [`Summary`]: https://docs.rs/loadr-core
//!
//! # Transport
//!
//! A Slack incoming webhook is a plain HTTPS/JSON POST, so the plugin sends the
//! message directly over the project's existing **hyper + hyper-rustls** stack
//! — no Slack SDK and no extra C dependency. `hyper-rustls` uses `ring` +
//! webpki roots (pure-Rust TLS, no system OpenSSL), so the cdylib
//! cross-compiles cleanly for every release target. A single Tokio runtime,
//! created once, drives the async POST; the host calls `finish` from an
//! ordinary (non-async) thread, so it `block_on`s the request.
//!
//! The actual send goes through a small [`WebhookSender`] seam so the
//! message-building and delivery logic is unit-tested without any network.
//!
//! # Failure handling
//!
//! A webhook error (non-2xx or transport failure) is logged and leaves the
//! internal `messages_sent` counter at `0`; it does **not** change the run's
//! exit code, which is still governed by `thresholds:`.
//!
//! [Slack incoming webhook]: https://api.slack.com/messaging/webhooks

use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::CONTENT_TYPE;
use hyper::Request;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use once_cell::sync::OnceCell;
use serde_json::Value;
use tokio::runtime::Runtime;

use loadr_plugin_api::abi::{
    FfiOutput, FfiOutputBox, FfiOutput_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "slack-notifier";

/// Per-request timeout for the webhook POST.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Delivery seam
// ---------------------------------------------------------------------------

/// Sends a JSON body to a webhook URL and returns the HTTP status. The seam
/// lets `finish` be exercised in tests with a mock that records the payload,
/// so no unit test touches the network.
trait WebhookSender: Send {
    fn post(&self, url: &str, body: &str) -> Result<u16, String>;
}

/// The hyper HTTPS client (pure-Rust TLS, cheaply cloneable, internally pooled).
type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// The single Tokio runtime the plugin uses to drive the async POST.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build slack-notifier plugin tokio runtime")
    })
}

fn build_client() -> HttpClient {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // also lets the same connector serve a plaintext proxy `webhook_url`.
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(tls)
}

/// POST one JSON body and return the HTTP status. The response body is drained
/// so the pooled connection can be reused.
async fn post_once(
    client: &HttpClient,
    url: &str,
    body: Bytes,
    timeout: Duration,
) -> Result<u16, String> {
    let request = Request::builder()
        .method("POST")
        .uri(url)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(body))
        .map_err(|e| format!("building request failed: {e}"))?;

    let send = client.request(request);
    let resp = tokio::time::timeout(timeout, send)
        .await
        .map_err(|_| format!("request to {url} timed out"))?
        .map_err(|e| format!("request to {url} failed: {e}"))?;
    let status = resp.status().as_u16();
    // Drain (and discard) the body to release the connection.
    let _ = resp.into_body().collect().await;
    Ok(status)
}

/// The production sender: posts over hyper + hyper-rustls, blocking on the
/// shared runtime.
struct HyperSender {
    client: HttpClient,
    timeout: Duration,
}

impl WebhookSender for HyperSender {
    fn post(&self, url: &str, body: &str) -> Result<u16, String> {
        let body = Bytes::from(body.to_owned());
        runtime().block_on(post_once(&self.client, url, body, self.timeout))
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Extract and validate the required `webhook_url`. A missing or empty value is
/// an error so the plan is rejected at `start`, not silently at the end.
fn webhook_from_config(cfg: &Value) -> Result<String, String> {
    cfg.get("webhook_url")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "slack-notifier output requires a non-empty `webhook_url`".to_string())
}

// ---------------------------------------------------------------------------
// Summary -> Slack message
// ---------------------------------------------------------------------------

/// Slack emoji shortcodes for the pass/fail verdict, rendered by Slack.
const PASS_MARK: &str = ":white_check_mark:";
const FAIL_MARK: &str = ":x:";

/// Finite numeric field lookup (a stray NaN/Inf is treated as absent).
fn num(obj: &Value, key: &str) -> Option<f64> {
    obj.get(key)
        .and_then(Value::as_f64)
        .filter(|f| f.is_finite())
}

/// The `agg` object of a named metric in the summary's `metrics` array.
fn metric_agg<'a>(summary: &'a Value, metric: &str) -> Option<&'a Value> {
    summary
        .get("metrics")
        .and_then(Value::as_array)?
        .iter()
        .find(|m| m.get("metric").and_then(Value::as_str) == Some(metric))
        .and_then(|m| m.get("agg"))
}

/// Render the human-readable message text (Slack `mrkdwn`). Never panics: a
/// missing/oddly-shaped summary simply yields fewer lines.
fn build_text(summary: &Value) -> String {
    let name = summary
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("loadr run");
    let passed = summary
        .get("thresholds_passed")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let (mark, verdict) = if passed {
        (PASS_MARK, "PASSED")
    } else {
        (FAIL_MARK, "FAILED")
    };
    let duration = summary
        .get("duration_secs")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);

    let mut lines = vec![
        format!("*{name}* — {mark} {verdict}"),
        format!("_duration_ {duration:.1}s"),
    ];

    if let Some(p95) = metric_agg(summary, "http_req_duration").and_then(|a| num(a, "p95")) {
        lines.push(format!("p95 `http_req_duration`: {p95:.2}ms"));
    }
    if let Some(rate) = metric_agg(summary, "http_req_failed").and_then(|a| num(a, "rate")) {
        lines.push(format!(
            "error rate `http_req_failed`: {:.2}%",
            rate * 100.0
        ));
    }

    if let Some(thresholds) = summary
        .get("thresholds")
        .and_then(Value::as_array)
        .filter(|t| !t.is_empty())
    {
        lines.push("*thresholds*".to_string());
        for t in thresholds {
            let metric = t.get("metric").and_then(Value::as_str).unwrap_or("?");
            let expr = t.get("expression").and_then(Value::as_str).unwrap_or("");
            let ok = t.get("passed").and_then(Value::as_bool).unwrap_or(false);
            let tmark = if ok { PASS_MARK } else { FAIL_MARK };
            match num(t, "observed") {
                Some(v) => lines.push(format!("{tmark} `{metric}` {expr} (observed {v:.2})")),
                None => lines.push(format!("{tmark} `{metric}` {expr}")),
            }
        }
    }

    lines.join("\n")
}

/// Build the JSON body Slack's incoming webhook expects: `{"text": "..."}`.
fn build_body(summary: &Value) -> String {
    serde_json::json!({ "text": build_text(summary) }).to_string()
}

// ---------------------------------------------------------------------------
// The output plugin
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SlackNotifier {
    webhook_url: String,
    sender: Option<Box<dyn WebhookSender>>,
    /// Messages accepted by the webhook (Slack `2xx`). Normally `1` after a
    /// clean run; stays `0` if the webhook rejects the request or is
    /// unreachable.
    messages_sent: u64,
}

impl SlackNotifier {
    /// Render and deliver the summary. A no-op without a configured sender
    /// (only reachable if `start` was skipped, e.g. in unit tests that build
    /// the struct directly with a mock).
    fn deliver(&mut self, summary: &Value) {
        let body = build_body(summary);
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        match sender.post(&self.webhook_url, &body) {
            Ok(status) if (200..300).contains(&status) => self.messages_sent += 1,
            Ok(status) => eprintln!("slack-notifier: webhook returned HTTP {status}"),
            Err(e) => eprintln!("slack-notifier: webhook post failed: {e}"),
        }
    }
}

impl FfiOutput for SlackNotifier {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<(), RString> {
        let cfg: Value = match serde_json::from_str(config_json.as_str()) {
            Ok(v) => v,
            Err(e) => return RErr(RString::from(format!("invalid config JSON: {e}"))),
        };
        match webhook_from_config(&cfg) {
            Ok(url) => {
                self.webhook_url = url;
                self.sender = Some(Box::new(HyperSender {
                    client: build_client(),
                    timeout: REQUEST_TIMEOUT,
                }));
                self.messages_sent = 0;
                ROk(())
            }
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn on_samples(&mut self, _samples_json: RString) {}

    fn on_snapshot(&mut self, _snapshot_json: RString) {}

    fn finish(&mut self, summary_json: RString) {
        let summary = serde_json::from_str(summary_json.as_str()).unwrap_or(Value::Null);
        self.deliver(&summary);
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "output",
            "description": "Posts a run summary and threshold verdict to a Slack incoming webhook",
        })
        .to_string(),
    )
}

extern "C" fn make_output() -> FfiOutputBox {
    FfiOutput_TO::from_value(
        SlackNotifier::default(),
        abi_stable::erased_types::TD_Opaque,
    )
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RSome(make_output),
        make_protocol: RNone,
        make_service: RNone,
        make_data_source: RNone,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    /// One recorded call: `(url, body)`.
    type Calls = Arc<Mutex<Vec<(String, String)>>>;

    /// A mock sender that records every call and returns a scripted outcome, so
    /// `finish`/`deliver` can be tested without any network.
    struct MockSender {
        calls: Calls,
        result: Result<u16, String>,
    }

    impl WebhookSender for MockSender {
        fn post(&self, url: &str, body: &str) -> Result<u16, String> {
            self.calls
                .lock()
                .unwrap()
                .push((url.to_string(), body.to_string()));
            self.result.clone()
        }
    }

    /// Build a notifier wired to a mock sender returning `result`. Not named
    /// `new` (it returns a tuple, and the wiring is a test fixture).
    fn notifier_with(result: Result<u16, String>) -> (SlackNotifier, Calls) {
        let calls: Calls = Arc::new(Mutex::new(Vec::new()));
        let notifier = SlackNotifier {
            webhook_url: "https://hooks.slack.com/services/T/B/secret".to_string(),
            sender: Some(Box::new(MockSender {
                calls: calls.clone(),
                result,
            })),
            messages_sent: 0,
        };
        (notifier, calls)
    }

    fn sample_summary() -> Value {
        json!({
            "name": "checkout-load",
            "run_id": "r-123",
            "duration_secs": 42.5,
            "thresholds_passed": false,
            "metrics": [
                {"metric": "http_req_duration", "kind": "trend",
                 "agg": {"count": 100, "p95": 231.5, "avg": 88.0}},
                {"metric": "http_req_failed", "kind": "rate",
                 "agg": {"count": 100, "rate": 0.02}}
            ],
            "thresholds": [
                {"metric": "http_req_duration", "expression": "p(95)<250",
                 "observed": 231.5, "passed": true, "abort_on_fail": false},
                {"metric": "http_req_failed", "expression": "rate<0.01",
                 "observed": 0.02, "passed": false, "abort_on_fail": false}
            ]
        })
    }

    // -- config validation --------------------------------------------------

    #[test]
    fn webhook_required_and_non_empty() {
        assert!(webhook_from_config(&json!({})).is_err());
        assert!(webhook_from_config(&json!({"webhook_url": ""})).is_err());
        assert_eq!(
            webhook_from_config(&json!({"webhook_url": "https://hooks.slack.com/x"})).unwrap(),
            "https://hooks.slack.com/x"
        );
    }

    #[test]
    fn start_rejects_missing_webhook() {
        let mut n = SlackNotifier::default();
        assert!(n.start(RString::from("{}")).is_err());
        assert!(n.sender.is_none());
    }

    #[test]
    fn start_rejects_invalid_json() {
        let mut n = SlackNotifier::default();
        assert!(n.start(RString::from("not json")).is_err());
    }

    // -- message rendering --------------------------------------------------

    #[test]
    fn text_contains_verdict_p95_error_rate_and_thresholds() {
        let text = build_text(&sample_summary());
        assert!(text.contains("*checkout-load*"));
        // thresholds_passed=false -> FAILED with the fail mark.
        assert!(text.contains(FAIL_MARK));
        assert!(text.contains("FAILED"));
        assert!(text.contains("_duration_ 42.5s"));
        // p95 of http_req_duration, formatted in ms.
        assert!(text.contains("p95 `http_req_duration`: 231.50ms"));
        // error rate of http_req_failed as a percentage.
        assert!(text.contains("error rate `http_req_failed`: 2.00%"));
        // one line per threshold, with observed values.
        assert!(text.contains("`http_req_duration` p(95)<250 (observed 231.50)"));
        assert!(text.contains("`http_req_failed` rate<0.01 (observed 0.02)"));
    }

    #[test]
    fn passing_run_renders_pass_mark() {
        let summary = json!({"name": "ok", "thresholds_passed": true, "duration_secs": 1.0});
        let text = build_text(&summary);
        assert!(text.contains(PASS_MARK));
        assert!(text.contains("PASSED"));
    }

    #[test]
    fn missing_thresholds_defaults_to_passed_with_no_threshold_section() {
        // No `thresholds_passed` and no `thresholds` array: verdict defaults to
        // pass (thresholds that all hold, or none) and no threshold lines.
        let text = build_text(&json!({"name": "bare"}));
        assert!(text.contains("PASSED"));
        assert!(!text.contains("*thresholds*"));
    }

    #[test]
    fn body_is_valid_slack_text_json() {
        let body = build_body(&sample_summary());
        let v: Value = serde_json::from_str(&body).unwrap();
        // Slack incoming webhooks want a top-level `text` string.
        let text = v.get("text").and_then(Value::as_str).unwrap();
        assert!(text.contains("checkout-load"));
    }

    #[test]
    fn non_finite_metric_values_are_dropped() {
        let summary = json!({
            "name": "n",
            "metrics": [
                {"metric": "http_req_duration", "agg": {"p95": null}}
            ]
        });
        let text = build_text(&summary);
        // A null p95 must not render a `null`ms line.
        assert!(!text.contains("p95 `http_req_duration`"));
    }

    // -- delivery via the seam (no network) ---------------------------------

    #[test]
    fn finish_posts_once_and_counts_on_success() {
        let (mut n, calls) = notifier_with(Ok(200));
        n.finish(RString::from(sample_summary().to_string()));
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "https://hooks.slack.com/services/T/B/secret");
        assert!(calls[0].1.contains("checkout-load"));
        assert_eq!(n.messages_sent, 1);
    }

    #[test]
    fn non_2xx_status_does_not_count() {
        let (mut n, calls) = notifier_with(Ok(404));
        n.finish(RString::from(sample_summary().to_string()));
        assert_eq!(calls.lock().unwrap().len(), 1, "the POST was attempted");
        assert_eq!(n.messages_sent, 0, "but a 404 is not a delivered message");
    }

    #[test]
    fn transport_error_does_not_count() {
        let (mut n, _calls) = notifier_with(Err("connection refused".to_string()));
        n.finish(RString::from(sample_summary().to_string()));
        assert_eq!(n.messages_sent, 0);
    }

    #[test]
    fn finish_without_sender_is_a_noop() {
        // A default notifier (no `start`) has no sender: `finish` must not panic.
        let mut n = SlackNotifier::default();
        n.finish(RString::from(sample_summary().to_string()));
        assert_eq!(n.messages_sent, 0);
    }

    #[test]
    fn malformed_summary_still_posts_a_message() {
        // `finish` on non-JSON falls back to Value::Null, which renders the
        // default verdict rather than panicking, and still delivers.
        let (mut n, calls) = notifier_with(Ok(200));
        n.finish(RString::from("not json at all"));
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(n.messages_sent, 1);
    }

    // -- lifecycle no-ops ---------------------------------------------------

    #[test]
    fn samples_and_snapshots_are_ignored() {
        let (mut n, calls) = notifier_with(Ok(200));
        n.on_samples(RString::from("[{\"metric\":\"x\"}]"));
        n.on_snapshot(RString::from("{\"series\":[]}"));
        assert!(calls.lock().unwrap().is_empty(), "no POST before finish");
        assert_eq!(n.messages_sent, 0);
    }

    #[test]
    fn name_and_info_declare_output_kind() {
        assert_eq!(SlackNotifier::default().name().as_str(), NAME);
        let info: Value = serde_json::from_str(plugin_info().as_str()).unwrap();
        assert_eq!(info["kind"], "output");
        assert_eq!(info["name"], NAME);
    }
}
