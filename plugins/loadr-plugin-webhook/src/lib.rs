//! `loadr-plugin-webhook` — a native **output** plugin and the simplest possible
//! custom sink: it serialises each one-second metric snapshot and the
//! end-of-run summary as JSON and **POSTs** them to a URL you configure.
//!
//! # How it plugs in
//!
//! loadr's native output ABI ([`FfiOutput`]) is the same `start` /
//! `on_snapshot` / `finish` lifecycle used by the shipped `native-output`
//! example: the host calls `start(config)` once, then hands the plugin a JSON
//! snapshot roughly once a second, then `finish(summary)` at the end. This
//! plugin wraps each payload in a small envelope and POSTs it.
//!
//! # What gets sent
//!
//! Every request is a single POST with a JSON body and an `event` field naming
//! the payload kind:
//!
//!   * **`snapshot`** — one per second (`on_snapshot`): the run's live metric
//!     snapshot, the same one-second rollup the `prometheus` and `json` outputs
//!     see.
//!   * **`summary`** — one at the end (`finish`): the full end-of-run summary.
//!
//! Each body carries the run's `run_id` (lifted to the top level when the
//! payload contains one) so a receiver aggregating several concurrent runs can
//! keep them separate:
//!
//! ```json
//! { "event": "snapshot", "run_id": "…", "snapshot": { … } }
//! ```
//!
//! # Transport
//!
//! The transport is plain HTTP(S)/JSON over the project's existing **hyper +
//! hyper-rustls** stack — no SDK, no client library, no message broker.
//! `hyper-rustls` uses `ring` + webpki roots (pure-Rust TLS, no system
//! OpenSSL), so the cdylib cross-compiles cleanly for every release target. A
//! single multi-thread Tokio runtime, created once, drives the async POSTs; the
//! host calls the plugin from an ordinary (non-async) thread, so each delivery
//! `block_on`s its request bounded by `timeout`.
//!
//! # Signing
//!
//! When `hmac_secret` is set, each request is signed: the plugin computes
//! `HMAC-SHA256(secret, body)` over the exact JSON bytes and sends it as the
//! `X-Loadr-Signature` header (hex). A receiver recomputes it over the raw body
//! and compares before trusting the payload.

use std::sync::OnceLock;
use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::{BodyExt, Full};
use hyper::header::CONTENT_TYPE;
use hyper::Request;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::Value;
use sha2::Sha256;
use tokio::runtime::Runtime;

use loadr_plugin_api::abi::{
    FfiOutput, FfiOutputBox, FfiOutput_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "webhook";

/// Default per-request timeout when `timeout` is absent or unparseable.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// The signature header written when `hmac_secret` is configured.
const SIGNATURE_HEADER: &str = "X-Loadr-Signature";

// ---------------------------------------------------------------------------
// Transport seam. Delivery goes through a `Sender` so the plugin's accounting
// and body/signing logic can be unit-tested without ever touching the network.
// ---------------------------------------------------------------------------

/// Delivers one already-built request body with the given extra headers,
/// returning the HTTP status of a completed request or `Err` on a transport
/// failure (connection error / timeout).
trait Sender: Send {
    fn send(&self, body: &[u8], headers: &[(String, String)]) -> Result<u16, String>;
}

/// The hyper HTTPS client (pure-Rust TLS, cheaply cloneable, internally pooled).
type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// The single Tokio runtime the plugin uses to drive async HTTP.
fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build webhook plugin tokio runtime")
    })
}

fn build_client() -> HttpClient {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // lets the same connector serve a plaintext `http://` endpoint too.
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(tls)
}

/// The real hyper-backed sender: POSTs to a fixed `url` with a per-request
/// `timeout`.
struct HttpSender {
    client: HttpClient,
    url: String,
    timeout: Duration,
}

impl Sender for HttpSender {
    fn send(&self, body: &[u8], headers: &[(String, String)]) -> Result<u16, String> {
        let bytes = Bytes::copy_from_slice(body);
        runtime().block_on(post(&self.client, &self.url, bytes, headers, self.timeout))
    }
}

/// POST one body and return the HTTP status. `Content-Type: application/json`
/// is always set; any caller-supplied `Content-Type` is ignored so the JSON
/// content type cannot be overridden by mistake. The response body is drained
/// so the pooled connection can be reused.
async fn post(
    client: &HttpClient,
    url: &str,
    body: Bytes,
    headers: &[(String, String)],
    timeout: Duration,
) -> Result<u16, String> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(url)
        .header(CONTENT_TYPE, "application/json");
    for (k, v) in headers {
        if k.eq_ignore_ascii_case("content-type") {
            continue;
        }
        builder = builder.header(k.as_str(), v.as_str());
    }
    let request = builder
        .body(Full::new(body))
        .map_err(|e| format!("building request failed: {e}"))?;

    let send = client.request(request);
    let resp = if timeout.is_zero() {
        send.await
            .map_err(|e| format!("request to {url} failed: {e}"))?
    } else {
        tokio::time::timeout(timeout, send)
            .await
            .map_err(|_| format!("request to {url} timed out after {}ms", timeout.as_millis()))?
            .map_err(|e| format!("request to {url} failed: {e}"))?
    };
    let status = resp.status().as_u16();
    // Drain (and discard) the response body to release the connection.
    let _ = resp.into_body().collect().await;
    Ok(status)
}

// ---------------------------------------------------------------------------
// HMAC-SHA256 signing (RustCrypto) + hex encoding.
// ---------------------------------------------------------------------------

type HmacSha256 = Hmac<Sha256>;

/// Compute the hex-encoded `HMAC-SHA256(secret, body)` signature.
fn hmac_sign(secret: &str, body: &[u8]) -> String {
    // HMAC accepts a key of any length, so `new_from_slice` never errors here.
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts a key of any length");
    mac.update(body);
    let tag = mac.finalize().into_bytes();
    hex_encode(&tag)
}

/// Lowercase hex encoding, without pulling in a dependency for two lines.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

// ---------------------------------------------------------------------------
// Config parsing.
// ---------------------------------------------------------------------------

/// The validated plugin configuration.
#[derive(Debug)]
struct Config {
    url: String,
    headers: Vec<(String, String)>,
    hmac_secret: Option<String>,
    timeout: Duration,
}

/// Reject anything that is not an `http(s)://` URL hyper can parse, so a typo is
/// caught at `start` rather than dropped silently every second during the run.
fn validate_url(url: &str) -> Result<(), String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(format!("webhook `url` must be http(s)://…, got `{url}`"));
    }
    url.parse::<hyper::Uri>()
        .map_err(|e| format!("invalid webhook url `{url}`: {e}"))?;
    Ok(())
}

/// Static headers accept a `{ "Header": "value" }` object; non-string values
/// are skipped.
fn headers_from_config(v: Option<&Value>) -> Vec<(String, String)> {
    match v {
        Some(Value::Object(obj)) => obj
            .iter()
            .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
        _ => Vec::new(),
    }
}

/// Parse a duration string such as `"5s"`, `"500ms"` or `"1m"`. A bare number
/// (no unit) is read as seconds.
fn parse_duration_str(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let value: f64 = num.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    let secs = match unit.trim() {
        "ms" => value / 1000.0,
        "s" | "" => value,
        "m" => value * 60.0,
        _ => return None,
    };
    Some(Duration::from_secs_f64(secs))
}

/// Resolve the per-request timeout: a duration string, a bare number of
/// seconds, or the 5s default.
fn timeout_from_config(v: Option<&Value>) -> Duration {
    match v {
        Some(Value::String(s)) => parse_duration_str(s).unwrap_or(DEFAULT_TIMEOUT),
        Some(Value::Number(n)) => n
            .as_f64()
            .filter(|f| f.is_finite() && *f > 0.0)
            .map(Duration::from_secs_f64)
            .unwrap_or(DEFAULT_TIMEOUT),
        _ => DEFAULT_TIMEOUT,
    }
}

impl Config {
    /// Build a validated config from the plugin config JSON value.
    fn from_config(cfg: &Value) -> Result<Config, String> {
        let url = cfg
            .get("url")
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "webhook output requires a non-empty `url`".to_string())?;
        validate_url(&url)?;

        let hmac_secret = cfg
            .get("hmac_secret")
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|s| !s.is_empty());

        Ok(Config {
            url,
            headers: headers_from_config(cfg.get("headers")),
            hmac_secret,
            timeout: timeout_from_config(cfg.get("timeout")),
        })
    }
}

// ---------------------------------------------------------------------------
// Envelope construction.
// ---------------------------------------------------------------------------

/// Wrap a payload in the `{ event, run_id?, <event>: payload }` envelope, with
/// `run_id` lifted from the payload to the top level when present.
fn build_body(event: &str, payload: &Value) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert("event".to_string(), Value::String(event.to_string()));
    if let Some(run_id) = payload.get("run_id").and_then(Value::as_str) {
        obj.insert("run_id".to_string(), Value::String(run_id.to_string()));
    }
    obj.insert(event.to_string(), payload.clone());
    Value::Object(obj).to_string()
}

// ---------------------------------------------------------------------------
// The output plugin.
// ---------------------------------------------------------------------------

struct Webhook {
    config: Config,
    sender: Option<Box<dyn Sender>>,
    /// Requests the endpoint accepted (a 2xx response).
    deliveries: u64,
    /// Requests that failed — a transport error, a timeout, or a non-2xx status.
    delivery_errors: u64,
}

impl Default for Webhook {
    fn default() -> Self {
        Webhook {
            config: Config {
                url: String::new(),
                headers: Vec::new(),
                hmac_secret: None,
                timeout: DEFAULT_TIMEOUT,
            },
            sender: None,
            deliveries: 0,
            delivery_errors: 0,
        }
    }
}

impl Webhook {
    /// Parse, sign and POST a `<event>` payload, updating the delivery
    /// counters. A malformed payload is skipped (never counted); a missing
    /// sender (only reachable if `start` was skipped, e.g. in unit tests) is a
    /// no-op.
    fn emit(&mut self, event: &str, json: &str) {
        let Ok(payload) = serde_json::from_str::<Value>(json) else {
            return;
        };
        let body = build_body(event, &payload);
        self.deliver(body.as_bytes());
    }

    /// Sign (when configured) and hand one already-built body to the sender,
    /// recording the outcome. Fire-and-forget: a failure is counted and the run
    /// continues.
    fn deliver(&mut self, body: &[u8]) {
        let mut headers = self.config.headers.clone();
        if let Some(secret) = self.config.hmac_secret.as_deref() {
            headers.push((SIGNATURE_HEADER.to_string(), hmac_sign(secret, body)));
        }
        // Borrow the sender only for the call; the counter mutation follows.
        let Some(result) = self.sender.as_ref().map(|s| s.send(body, &headers)) else {
            return; // no sender (start() was skipped, e.g. in unit tests)
        };
        match result {
            Ok(status) if (200..300).contains(&status) => self.deliveries += 1,
            Ok(status) => {
                self.delivery_errors += 1;
                eprintln!(
                    "webhook output: delivery to {} returned HTTP {status}",
                    self.config.url
                );
            }
            Err(e) => {
                self.delivery_errors += 1;
                eprintln!(
                    "webhook output: delivery to {} failed: {e}",
                    self.config.url
                );
            }
        }
    }
}

impl FfiOutput for Webhook {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<(), RString> {
        let cfg: Value = match serde_json::from_str(config_json.as_str()) {
            Ok(v) => v,
            Err(e) => return RErr(RString::from(format!("invalid config JSON: {e}"))),
        };
        match Config::from_config(&cfg) {
            Ok(config) => {
                let sender = HttpSender {
                    client: build_client(),
                    url: config.url.clone(),
                    timeout: config.timeout,
                };
                *self = Webhook {
                    config,
                    sender: Some(Box::new(sender)),
                    deliveries: 0,
                    delivery_errors: 0,
                };
                ROk(())
            }
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn on_samples(&mut self, _samples_json: RString) {}

    fn on_snapshot(&mut self, snapshot_json: RString) {
        self.emit("snapshot", snapshot_json.as_str());
    }

    fn finish(&mut self, summary_json: RString) {
        self.emit("summary", summary_json.as_str());
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "output",
            "description": "POSTs each snapshot and the summary as JSON to a configured HTTP endpoint",
        })
        .to_string(),
    )
}

extern "C" fn make_output() -> FfiOutputBox {
    FfiOutput_TO::from_value(Webhook::default(), abi_stable::erased_types::TD_Opaque)
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

// ---------------------------------------------------------------------------
// Tests — all offline; delivery is exercised through a scripted mock sender,
// never a real socket.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    /// One recorded outbound request.
    #[derive(Debug, Clone)]
    struct Sent {
        body: String,
        headers: Vec<(String, String)>,
    }

    /// A scripted in-memory sender. Each `send` returns the next queued result
    /// (defaulting to `Ok(200)` once the script is drained) and records the
    /// exact body + headers it was handed.
    #[derive(Default)]
    struct MockSender {
        results: Mutex<VecDeque<Result<u16, String>>>,
        sent: Mutex<Vec<Sent>>,
    }

    impl MockSender {
        fn scripted(results: Vec<Result<u16, String>>) -> Arc<MockSender> {
            Arc::new(MockSender {
                results: Mutex::new(results.into_iter().collect()),
                sent: Mutex::new(Vec::new()),
            })
        }

        fn sent(&self) -> Vec<Sent> {
            self.sent.lock().unwrap().clone()
        }
    }

    impl Sender for Arc<MockSender> {
        fn send(&self, body: &[u8], headers: &[(String, String)]) -> Result<u16, String> {
            self.sent.lock().unwrap().push(Sent {
                body: String::from_utf8_lossy(body).into_owned(),
                headers: headers.to_vec(),
            });
            self.results.lock().unwrap().pop_front().unwrap_or(Ok(200))
        }
    }

    /// A `Webhook` wired to a scripted mock sender. Named `harness` (not `new`)
    /// because it returns a tuple.
    fn harness(config: Config, results: Vec<Result<u16, String>>) -> (Webhook, Arc<MockSender>) {
        let mock = MockSender::scripted(results);
        let webhook = Webhook {
            config,
            sender: Some(Box::new(mock.clone())),
            deliveries: 0,
            delivery_errors: 0,
        };
        (webhook, mock)
    }

    fn config(url: &str, hmac_secret: Option<&str>) -> Config {
        Config {
            url: url.to_string(),
            headers: Vec::new(),
            hmac_secret: hmac_secret.map(str::to_string),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    // -- hex + HMAC ----------------------------------------------------------

    #[test]
    fn hex_encodes_bytes() {
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0x00, 0x0f, 0xff]), "000fff");
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn hmac_matches_known_vector() {
        // Canonical RFC-style vector: HMAC-SHA256("key", "The quick brown fox
        // jumps over the lazy dog").
        assert_eq!(
            hmac_sign("key", b"The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    // -- config parsing ------------------------------------------------------

    #[test]
    fn from_config_requires_url() {
        assert!(Config::from_config(&serde_json::json!({})).is_err());
        assert!(Config::from_config(&serde_json::json!({ "url": "" })).is_err());
        assert!(Config::from_config(&serde_json::json!({ "url": "https://h/x" })).is_ok());
    }

    #[test]
    fn from_config_rejects_bad_scheme() {
        let err = Config::from_config(&serde_json::json!({ "url": "ftp://h/x" })).unwrap_err();
        assert!(err.contains("http(s)"), "{err}");
        let err =
            Config::from_config(&serde_json::json!({ "url": "hooks.example.com" })).unwrap_err();
        assert!(err.contains("http(s)"), "{err}");
    }

    #[test]
    fn from_config_parses_all_fields() {
        let cfg = Config::from_config(&serde_json::json!({
            "url": "https://hooks.example.com/loadr",
            "headers": { "X-Source": "loadr", "Authorization": "Bearer t", "n": 1 },
            "hmac_secret": "shh",
            "timeout": "250ms"
        }))
        .unwrap();
        assert_eq!(cfg.url, "https://hooks.example.com/loadr");
        assert_eq!(cfg.hmac_secret.as_deref(), Some("shh"));
        assert_eq!(cfg.timeout, Duration::from_millis(250));
        // Both string headers are present; the numeric value is dropped.
        assert!(cfg
            .headers
            .contains(&("X-Source".to_string(), "loadr".to_string())));
        assert!(cfg
            .headers
            .contains(&("Authorization".to_string(), "Bearer t".to_string())));
        assert_eq!(cfg.headers.len(), 2);
    }

    #[test]
    fn empty_hmac_secret_is_treated_as_unset() {
        let cfg =
            Config::from_config(&serde_json::json!({ "url": "https://h/x", "hmac_secret": "" }))
                .unwrap();
        assert!(cfg.hmac_secret.is_none());
    }

    // -- duration parsing ----------------------------------------------------

    #[test]
    fn parses_duration_units() {
        assert_eq!(parse_duration_str("5s"), Some(Duration::from_secs(5)));
        assert_eq!(
            parse_duration_str("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_duration_str("1m"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration_str("2"), Some(Duration::from_secs(2)));
        assert_eq!(
            parse_duration_str("1.5s"),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(parse_duration_str(""), None);
        assert_eq!(parse_duration_str("soon"), None);
        assert_eq!(parse_duration_str("5h"), None);
    }

    #[test]
    fn timeout_defaults_and_overrides() {
        assert_eq!(timeout_from_config(None), DEFAULT_TIMEOUT);
        assert_eq!(
            timeout_from_config(Some(&serde_json::json!("2s"))),
            Duration::from_secs(2)
        );
        // A bare number is seconds.
        assert_eq!(
            timeout_from_config(Some(&serde_json::json!(3))),
            Duration::from_secs(3)
        );
        // Unparseable string falls back to the default rather than erroring.
        assert_eq!(
            timeout_from_config(Some(&serde_json::json!("nope"))),
            DEFAULT_TIMEOUT
        );
    }

    // -- envelope ------------------------------------------------------------

    #[test]
    fn body_wraps_payload_and_lifts_run_id() {
        let payload = serde_json::json!({ "run_id": "abc", "series": [] });
        let body: Value = serde_json::from_str(&build_body("snapshot", &payload)).unwrap();
        assert_eq!(body["event"], "snapshot");
        assert_eq!(body["run_id"], "abc");
        // The payload is nested under the event name.
        assert_eq!(body["snapshot"]["run_id"], "abc");
        assert!(body["snapshot"]["series"].is_array());
    }

    #[test]
    fn body_omits_run_id_when_absent() {
        let body: Value =
            serde_json::from_str(&build_body("summary", &serde_json::json!({ "x": 1 }))).unwrap();
        assert_eq!(body["event"], "summary");
        assert!(body.get("run_id").is_none());
    }

    // -- delivery accounting (no network) ------------------------------------

    #[test]
    fn successful_delivery_is_counted() {
        let (mut wh, mock) = harness(config("https://h/x", None), vec![Ok(200)]);
        wh.emit("snapshot", r#"{"run_id":"r1","series":[]}"#);
        assert_eq!(wh.deliveries, 1);
        assert_eq!(wh.delivery_errors, 0);

        let sent = mock.sent();
        assert_eq!(sent.len(), 1);
        let body: Value = serde_json::from_str(&sent[0].body).unwrap();
        assert_eq!(body["event"], "snapshot");
        assert_eq!(body["run_id"], "r1");
    }

    #[test]
    fn non_2xx_and_transport_errors_count_as_errors() {
        let (mut wh, _mock) = harness(
            config("https://h/x", None),
            vec![Ok(500), Ok(404), Err("connection refused".to_string())],
        );
        wh.emit("snapshot", "{}");
        wh.emit("snapshot", "{}");
        wh.emit("snapshot", "{}");
        assert_eq!(wh.deliveries, 0);
        assert_eq!(wh.delivery_errors, 3);
    }

    #[test]
    fn hmac_secret_signs_the_exact_body() {
        let (mut wh, mock) = harness(config("https://h/x", Some("shh")), vec![Ok(202)]);
        wh.emit("summary", r#"{"run_id":"r9"}"#);
        assert_eq!(wh.deliveries, 1);

        let sent = mock.sent();
        let sig = sent[0]
            .headers
            .iter()
            .find(|(k, _)| k == SIGNATURE_HEADER)
            .map(|(_, v)| v.clone())
            .expect("signature header present");
        // The signature is exactly HMAC-SHA256(secret, the bytes we sent).
        assert_eq!(sig, hmac_sign("shh", sent[0].body.as_bytes()));
    }

    #[test]
    fn no_signature_header_without_secret() {
        let (mut wh, mock) = harness(config("https://h/x", None), vec![Ok(200)]);
        wh.emit("snapshot", "{}");
        assert!(mock.sent()[0]
            .headers
            .iter()
            .all(|(k, _)| k != SIGNATURE_HEADER));
    }

    #[test]
    fn static_headers_are_forwarded() {
        let mut cfg = config("https://h/x", None);
        cfg.headers = vec![("X-Source".to_string(), "loadr".to_string())];
        let (mut wh, mock) = harness(cfg, vec![Ok(200)]);
        wh.emit("snapshot", "{}");
        assert!(mock.sent()[0]
            .headers
            .contains(&("X-Source".to_string(), "loadr".to_string())));
    }

    #[test]
    fn malformed_payload_is_skipped_not_counted() {
        let (mut wh, mock) = harness(config("https://h/x", None), vec![]);
        wh.emit("snapshot", "not json at all");
        assert_eq!(wh.deliveries, 0);
        assert_eq!(wh.delivery_errors, 0);
        assert!(mock.sent().is_empty(), "nothing should have been sent");
    }

    #[test]
    fn finish_emits_a_summary_event() {
        let (mut wh, mock) = harness(config("https://h/x", None), vec![Ok(200)]);
        wh.finish(RString::from(r#"{"run_id":"done"}"#));
        let body: Value = serde_json::from_str(&mock.sent()[0].body).unwrap();
        assert_eq!(body["event"], "summary");
        assert_eq!(body["run_id"], "done");
        assert_eq!(wh.deliveries, 1);
    }

    #[test]
    fn on_snapshot_without_sender_is_a_noop() {
        // A default Webhook has no sender (start() was skipped); it must not
        // panic and must not count anything.
        let mut wh = Webhook::default();
        wh.on_snapshot(RString::from(r#"{"series":[]}"#));
        wh.finish(RString::from("{}"));
        assert_eq!(wh.deliveries, 0);
        assert_eq!(wh.delivery_errors, 0);
    }

    #[test]
    fn info_declares_output_kind() {
        let info = plugin_info();
        let v: Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "output");
        assert_eq!(v["name"], "webhook");
    }
}
