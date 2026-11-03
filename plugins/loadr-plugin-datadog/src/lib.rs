//! `loadr-plugin-datadog` — a native **output** plugin that batches loadr's
//! per-second metric snapshots into the [Datadog v2 series HTTP API].
//!
//! # How it plugs in
//!
//! loadr's native output ABI ([`FfiOutput`]) is the same `start` /
//! `on_snapshot` / `finish` lifecycle used by the shipped `native-output`
//! example: the host calls `start(config)` once, then hands the plugin a JSON
//! [`Snapshot`] roughly once a second, then `finish(summary)` at the end. This
//! plugin turns each snapshot's series into Datadog series points, buffers
//! them, and POSTs them in batches to `https://api.<site>/api/v2/series`
//! authenticated with a `DD-API-KEY` header.
//!
//! [`Snapshot`]: https://docs.rs/loadr-core
//!
//! # Transport
//!
//! The intake is plain HTTPS/JSON, so the plugin posts directly over the
//! project's existing **hyper + hyper-rustls** stack — no Datadog SDK, no trace
//! agent, no `dd-trace`. `hyper-rustls` uses `ring` + webpki roots (pure-Rust
//! TLS, no system OpenSSL), so the cdylib cross-compiles cleanly for every
//! release target. A single multi-thread Tokio runtime, created once, drives
//! the async POSTs; the host calls the plugin from an ordinary (non-async)
//! thread, so each flush `block_on`s its batch.
//!
//! # Mapping
//!
//! Each [`SeriesSnapshot`] maps to one or more Datadog series (v2 intake
//! `type`: `1` = count, `3` = gauge):
//!
//!   * **counter** → one `count` series carrying the increase over the interval
//!     (`interval_sum`), with the snapshot `interval` attached.
//!   * **gauge**   → one `gauge` series of the last value.
//!   * **rate**    → one `gauge` series of the pass fraction.
//!   * **trend**   → one `gauge` series per present aggregation
//!     (`<metric>.avg`, `.p95`, `.p99`, …).
//!
//! Series tags (`{key: value}`) become `key:value` strings, merged with any
//! global `tags` from the plugin config.
//!
//! [Datadog v2 series HTTP API]: https://docs.datadoghq.com/api/latest/metrics/#submit-metrics

use std::time::{Duration, Instant};

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
use serde::Serialize;
use serde_json::Value;
use tokio::runtime::Runtime;

use loadr_plugin_api::abi::{
    FfiOutput, FfiOutputBox, FfiOutput_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "datadog";

/// The Datadog v2 metric-intake `type` for a counter increase.
const DD_COUNT: u8 = 1;
/// The Datadog v2 metric-intake `type` for a gauge.
const DD_GAUGE: u8 = 3;

/// Trend aggregations projected as individual gauge series, mapping the
/// snapshot `agg` field to the metric-name suffix Datadog sees.
const TREND_AGGS: &[(&str, &str)] = &[
    ("avg", "avg"),
    ("min", "min"),
    ("max", "max"),
    ("med", "p50"),
    ("p90", "p90"),
    ("p95", "p95"),
    ("p99", "p99"),
    ("p999", "p999"),
];

/// The hyper HTTPS client (pure-Rust TLS, cheaply cloneable, internally pooled).
type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// The single Tokio runtime the plugin uses to drive async HTTP.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build datadog plugin tokio runtime")
    })
}

fn build_client() -> HttpClient {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // lets the same connector serve a plaintext proxy `url` override too.
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(tls)
}

// ---------------------------------------------------------------------------
// Payload types
// ---------------------------------------------------------------------------

/// One `{timestamp, value}` point (Datadog wants Unix seconds).
#[derive(Debug, Clone, PartialEq, Serialize)]
struct DdPoint {
    timestamp: i64,
    value: f64,
}

/// One Datadog v2 series entry.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct DdSeries {
    metric: String,
    #[serde(rename = "type")]
    typ: u8,
    points: Vec<DdPoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval: Option<i64>,
}

impl DdSeries {
    fn count(
        metric: String,
        ts: i64,
        value: f64,
        tags: Vec<String>,
        interval: Option<i64>,
    ) -> Self {
        DdSeries {
            metric,
            typ: DD_COUNT,
            points: vec![DdPoint {
                timestamp: ts,
                value,
            }],
            tags,
            interval,
        }
    }

    fn gauge(metric: String, ts: i64, value: f64, tags: Vec<String>) -> Self {
        DdSeries {
            metric,
            typ: DD_GAUGE,
            points: vec![DdPoint {
                timestamp: ts,
                value,
            }],
            tags,
            interval: None,
        }
    }
}

/// The request body Datadog expects: `{ "series": [...] }`.
#[derive(Serialize)]
struct Payload<'a> {
    series: &'a [DdSeries],
}

/// Serialise a batch of series to the v2 intake request body.
fn build_payload(series: &[DdSeries]) -> String {
    serde_json::to_string(&Payload { series }).unwrap_or_else(|_| "{\"series\":[]}".to_string())
}

// ---------------------------------------------------------------------------
// Snapshot -> Datadog series mapping
// ---------------------------------------------------------------------------

fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Finite numeric field lookup — non-finite values are rejected so a stray
/// NaN/Inf never produces a `null` in the JSON body (which Datadog would 400).
fn num(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(Value::as_f64).filter(|f| f.is_finite())
}

/// Turn one snapshot (as JSON) into a flat list of Datadog series. Never
/// panics: a missing/oddly-shaped snapshot simply yields no series.
fn series_from_snapshot(snapshot: &Value, prefix: &str, global_tags: &[String]) -> Vec<DdSeries> {
    let ts = snapshot
        .get("timestamp_ms")
        .and_then(Value::as_u64)
        .map(|ms| (ms / 1000) as i64)
        .unwrap_or_else(now_unix_secs);
    let interval = snapshot
        .get("interval_secs")
        .and_then(Value::as_f64)
        .filter(|v| v.is_finite() && *v > 0.0)
        .map(|v| v.round() as i64);

    let Some(entries) = snapshot.get("series").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for entry in entries {
        let Some(name) = entry.get("metric").and_then(Value::as_str) else {
            continue;
        };
        let kind = entry.get("kind").and_then(Value::as_str).unwrap_or("gauge");

        let mut tags = global_tags.to_vec();
        if let Some(obj) = entry.get("tags").and_then(Value::as_object) {
            for (k, v) in obj {
                if let Some(vs) = v.as_str() {
                    tags.push(format!("{k}:{vs}"));
                }
            }
        }

        let agg = entry.get("agg");
        let full = format!("{prefix}{name}");
        match kind {
            "counter" => {
                if let Some(v) = num(entry, "interval_sum") {
                    out.push(DdSeries::count(full, ts, v, tags, interval));
                }
            }
            "rate" => {
                let v = agg.and_then(|a| num(a, "rate")).unwrap_or(0.0);
                out.push(DdSeries::gauge(full, ts, v, tags));
            }
            "trend" => {
                for (key, suffix) in TREND_AGGS {
                    if let Some(v) = agg.and_then(|a| num(a, key)) {
                        out.push(DdSeries::gauge(
                            format!("{full}.{suffix}"),
                            ts,
                            v,
                            tags.clone(),
                        ));
                    }
                }
            }
            // "gauge" and any unknown kind: report the last value as a gauge.
            _ => {
                let v = agg.and_then(|a| num(a, "last")).unwrap_or(0.0);
                out.push(DdSeries::gauge(full, ts, v, tags));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Flush cadence + HTTP send with retry/backoff
// ---------------------------------------------------------------------------

/// Whether an accumulated buffer should be flushed now: non-empty, and either
/// full or older than the flush interval.
fn should_flush(len: usize, batch_size: usize, elapsed: Duration, interval: Duration) -> bool {
    len > 0 && (len >= batch_size || elapsed >= interval)
}

/// Whether an HTTP status warrants a retry (server-side 5xx only; 4xx is a
/// permanent client error and is surfaced immediately).
fn should_retry(status: u16) -> bool {
    (500..600).contains(&status)
}

/// Exponential backoff: `base * 2^attempt`, capped at 30s.
fn backoff_delay(attempt: u32, base: Duration) -> Duration {
    let factor = 1u32.checked_shl(attempt.min(6)).unwrap_or(64);
    base.saturating_mul(factor).min(Duration::from_secs(30))
}

/// Drive `send` with retry/backoff. `send` returns the HTTP status on a
/// completed request or `Err` on a transport failure. 2xx succeeds; 5xx and
/// transport errors are retried up to `max_retries` times with growing
/// backoff; 4xx fails immediately.
async fn send_with_retry<F, Fut>(
    mut send: F,
    max_retries: u32,
    base: Duration,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<u16, String>>,
{
    let mut attempt = 0u32;
    loop {
        match send().await {
            Ok(status) if (200..300).contains(&status) => return Ok(()),
            Ok(status) if should_retry(status) && attempt < max_retries => {
                tokio::time::sleep(backoff_delay(attempt, base)).await;
                attempt += 1;
            }
            Ok(status) => return Err(format!("datadog returned HTTP {status}")),
            Err(_) if attempt < max_retries => {
                tokio::time::sleep(backoff_delay(attempt, base)).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// POST one series batch and return the HTTP status. The body is drained so
/// the pooled connection can be reused.
async fn post_series(
    client: &HttpClient,
    endpoint: &str,
    api_key: &str,
    body: Bytes,
    timeout_ms: u64,
) -> Result<u16, String> {
    let request = Request::builder()
        .method("POST")
        .uri(endpoint)
        .header(CONTENT_TYPE, "application/json")
        .header("DD-API-KEY", api_key)
        .body(Full::new(body))
        .map_err(|e| format!("building request failed: {e}"))?;

    let send = client.request(request);
    let resp = if timeout_ms == 0 {
        send.await
            .map_err(|e| format!("request to {endpoint} failed: {e}"))?
    } else {
        tokio::time::timeout(Duration::from_millis(timeout_ms), send)
            .await
            .map_err(|_| format!("request to {endpoint} timed out after {timeout_ms}ms"))?
            .map_err(|e| format!("request to {endpoint} failed: {e}"))?
    };
    let status = resp.status().as_u16();
    // Drain (and discard) the response body to release the connection.
    let _ = resp.into_body().collect().await;
    Ok(status)
}

// ---------------------------------------------------------------------------
// The output plugin
// ---------------------------------------------------------------------------

struct Datadog {
    endpoint: String,
    api_key: String,
    prefix: String,
    global_tags: Vec<String>,
    batch_size: usize,
    flush_interval: Duration,
    timeout_ms: u64,
    max_retries: u32,
    base_backoff: Duration,
    buffer: Vec<DdSeries>,
    last_flush: Instant,
    client: Option<HttpClient>,
    flushes: u64,
    sent_series: u64,
}

impl Default for Datadog {
    fn default() -> Self {
        Datadog {
            endpoint: "https://api.datadoghq.com/api/v2/series".to_string(),
            api_key: String::new(),
            prefix: "loadr.".to_string(),
            global_tags: Vec::new(),
            batch_size: 1000,
            flush_interval: Duration::from_secs(15),
            timeout_ms: 10_000,
            max_retries: 3,
            base_backoff: Duration::from_millis(500),
            buffer: Vec::new(),
            last_flush: Instant::now(),
            client: None,
            flushes: 0,
            sent_series: 0,
        }
    }
}

/// Resolve the intake endpoint: an explicit `url` wins, else
/// `https://api.<site>/api/v2/series`.
fn endpoint_from_config(cfg: &Value) -> String {
    if let Some(u) = cfg
        .get("url")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        return u.to_string();
    }
    let site = cfg
        .get("site")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("datadoghq.com");
    format!("https://api.{site}/api/v2/series")
}

/// Global tags accept either a `["k:v", ...]` array or a `{k: v}` object.
fn tags_from_config(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|t| t.as_str().map(str::to_string))
            .collect(),
        Some(Value::Object(obj)) => obj
            .iter()
            .filter_map(|(k, val)| val.as_str().map(|s| format!("{k}:{s}")))
            .collect(),
        _ => Vec::new(),
    }
}

impl Datadog {
    /// Build a configured (client-less) instance from the plugin config JSON.
    fn from_config(cfg: &Value) -> Result<Datadog, String> {
        let api_key = cfg
            .get("api_key")
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "datadog output requires a non-empty `api_key`".to_string())?;

        Ok(Datadog {
            endpoint: endpoint_from_config(cfg),
            api_key,
            prefix: cfg
                .get("prefix")
                .and_then(Value::as_str)
                .unwrap_or("loadr.")
                .to_string(),
            global_tags: tags_from_config(cfg.get("tags")),
            batch_size: cfg
                .get("batch_size")
                .and_then(Value::as_u64)
                .map(|n| n.max(1) as usize)
                .unwrap_or(1000),
            flush_interval: Duration::from_secs(
                cfg.get("flush_interval_secs")
                    .and_then(Value::as_u64)
                    .unwrap_or(15)
                    .max(1),
            ),
            timeout_ms: cfg
                .get("timeout_ms")
                .and_then(Value::as_u64)
                .unwrap_or(10_000),
            max_retries: cfg.get("max_retries").and_then(Value::as_u64).unwrap_or(3) as u32,
            base_backoff: Duration::from_millis(500),
            buffer: Vec::new(),
            last_flush: Instant::now(),
            client: None,
            flushes: 0,
            sent_series: 0,
        })
    }

    /// Append a snapshot's series to the buffer (no I/O). Returns how many
    /// series were added.
    fn ingest(&mut self, snapshot_json: &str) -> usize {
        let Ok(value) = serde_json::from_str::<Value>(snapshot_json) else {
            return 0;
        };
        let mut series = series_from_snapshot(&value, &self.prefix, &self.global_tags);
        let added = series.len();
        self.buffer.append(&mut series);
        added
    }

    /// Flush the buffered series to Datadog. A no-op on an empty buffer, so it
    /// never panics and never posts an empty batch. Buffered series are
    /// consumed regardless of the send outcome so a persistent failure cannot
    /// grow the buffer without bound.
    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.buffer);
        self.flushes += 1;
        self.sent_series += batch.len() as u64;
        self.last_flush = Instant::now();

        // Without a client (only reachable if `start` was skipped, e.g. in
        // unit tests) there is nothing to post to.
        let Some(client) = self.client.clone() else {
            return;
        };
        let body = Bytes::from(build_payload(&batch));
        let endpoint = self.endpoint.clone();
        let api_key = self.api_key.clone();
        let timeout_ms = self.timeout_ms;
        let max_retries = self.max_retries;
        let base = self.base_backoff;

        let result = runtime().block_on(send_with_retry(
            || post_series(&client, &endpoint, &api_key, body.clone(), timeout_ms),
            max_retries,
            base,
        ));
        if let Err(e) = result {
            eprintln!(
                "datadog output: flush of {} series failed: {e}",
                batch.len()
            );
        }
    }
}

impl FfiOutput for Datadog {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<(), RString> {
        let cfg: Value = match serde_json::from_str(config_json.as_str()) {
            Ok(v) => v,
            Err(e) => return RErr(RString::from(format!("invalid config JSON: {e}"))),
        };
        match Datadog::from_config(&cfg) {
            Ok(mut dd) => {
                dd.client = Some(build_client());
                *self = dd;
                ROk(())
            }
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn on_samples(&mut self, _samples_json: RString) {}

    fn on_snapshot(&mut self, snapshot_json: RString) {
        self.ingest(snapshot_json.as_str());
        if should_flush(
            self.buffer.len(),
            self.batch_size,
            self.last_flush.elapsed(),
            self.flush_interval,
        ) {
            self.flush();
        }
    }

    fn finish(&mut self, _summary_json: RString) {
        // Drain whatever is left; a no-op on an empty buffer.
        self.flush();
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "output",
            "description": "Batches snapshot series into the Datadog v2 series HTTP API keyed by an API key",
        })
        .to_string(),
    )
}

extern "C" fn make_output() -> FfiOutputBox {
    FfiOutput_TO::from_value(Datadog::default(), abi_stable::erased_types::TD_Opaque)
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
    #![allow(clippy::field_reassign_with_default)]
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn find<'a>(series: &'a [DdSeries], metric: &str) -> Option<&'a DdSeries> {
        series.iter().find(|s| s.metric == metric)
    }

    // -- snapshot -> Datadog series mapping ---------------------------------

    #[test]
    fn snapshot_series_map_to_datadog_payloads() {
        let snap = json!({
            "timestamp_ms": 1_000_000u64, // -> 1000 unix seconds
            "elapsed_secs": 5.0,
            "interval_secs": 1.0,
            "series": [
                {
                    "metric": "http_reqs",
                    "kind": "counter",
                    "tags": {"method": "GET"},
                    "interval_count": 10,
                    "interval_sum": 10.0,
                    "agg": {"count": 100, "sum": 100.0}
                },
                {"metric": "vus", "kind": "gauge", "tags": {}, "agg": {"last": 25.0}},
                {"metric": "checks", "kind": "rate", "tags": {}, "agg": {"rate": 0.99}},
                {
                    "metric": "http_req_duration",
                    "kind": "trend",
                    "tags": {"method": "GET"},
                    "agg": {"avg": 12.0, "p95": 30.0, "max": 50.0}
                }
            ]
        });

        let series = series_from_snapshot(&snap, "loadr.", &["env:test".to_string()]);

        // counter -> count series with the interval increase + interval attached.
        let c = find(&series, "loadr.http_reqs").expect("counter series");
        assert_eq!(c.typ, DD_COUNT);
        assert_eq!(
            c.points,
            vec![DdPoint {
                timestamp: 1000,
                value: 10.0
            }]
        );
        assert_eq!(c.interval, Some(1));
        assert!(c.tags.contains(&"env:test".to_string()));
        assert!(c.tags.contains(&"method:GET".to_string()));

        // gauge -> last value.
        let g = find(&series, "loadr.vus").expect("gauge series");
        assert_eq!(g.typ, DD_GAUGE);
        assert_eq!(g.points[0].value, 25.0);
        assert_eq!(g.interval, None);

        // rate -> pass fraction as a gauge.
        let r = find(&series, "loadr.checks").expect("rate series");
        assert_eq!(r.typ, DD_GAUGE);
        assert_eq!(r.points[0].value, 0.99);

        // trend -> one gauge per present aggregation.
        assert_eq!(
            find(&series, "loadr.http_req_duration.avg").unwrap().points[0].value,
            12.0
        );
        assert_eq!(
            find(&series, "loadr.http_req_duration.p95").unwrap().points[0].value,
            30.0
        );
        assert_eq!(
            find(&series, "loadr.http_req_duration.max").unwrap().points[0].value,
            50.0
        );
        // No p99 was present, so no p99 series.
        assert!(find(&series, "loadr.http_req_duration.p99").is_none());

        // 1 counter + 1 gauge + 1 rate + 3 trend = 6 series.
        assert_eq!(series.len(), 6);
    }

    #[test]
    fn payload_serialises_type_and_omits_empty_fields() {
        let s = vec![
            DdSeries::gauge("loadr.vus".into(), 1000, 3.0, vec![]),
            DdSeries::count(
                "loadr.http_reqs".into(),
                1000,
                7.0,
                vec!["env:p".into()],
                Some(1),
            ),
        ];
        let body: Value = serde_json::from_str(&build_payload(&s)).unwrap();
        let arr = body["series"].as_array().unwrap();
        // Gauge: type 3, no tags/interval keys emitted.
        assert_eq!(arr[0]["type"], 3);
        assert_eq!(arr[0]["metric"], "loadr.vus");
        assert!(arr[0].get("tags").is_none());
        assert!(arr[0].get("interval").is_none());
        // Count: type 1, tags + interval present.
        assert_eq!(arr[1]["type"], 1);
        assert_eq!(arr[1]["interval"], 1);
        assert_eq!(arr[1]["tags"][0], "env:p");
        assert_eq!(arr[1]["points"][0]["timestamp"], 1000);
        assert_eq!(arr[1]["points"][0]["value"], 7.0);
    }

    #[test]
    fn non_finite_values_are_dropped() {
        // A trend whose agg values are non-numeric/absent yields no series
        // rather than a `null`-valued point Datadog would reject.
        let snap = json!({
            "timestamp_ms": 2000u64,
            "series": [
                {"metric": "t", "kind": "trend", "agg": {"avg": null}}
            ]
        });
        let series = series_from_snapshot(&snap, "loadr.", &[]);
        assert!(series.is_empty());
    }

    // -- config parsing -----------------------------------------------------

    #[test]
    fn from_config_requires_api_key() {
        assert!(Datadog::from_config(&json!({})).is_err());
        assert!(Datadog::from_config(&json!({"api_key": ""})).is_err());
        assert!(Datadog::from_config(&json!({"api_key": "abc"})).is_ok());
    }

    #[test]
    fn endpoint_defaults_and_overrides() {
        assert_eq!(
            endpoint_from_config(&json!({"api_key": "k"})),
            "https://api.datadoghq.com/api/v2/series"
        );
        assert_eq!(
            endpoint_from_config(&json!({"site": "datadoghq.eu"})),
            "https://api.datadoghq.eu/api/v2/series"
        );
        assert_eq!(
            endpoint_from_config(&json!({"url": "http://proxy.internal/intake"})),
            "http://proxy.internal/intake"
        );
    }

    #[test]
    fn tags_accept_array_or_object() {
        assert_eq!(
            tags_from_config(Some(&json!(["env:prod", "team:sre"]))),
            vec!["env:prod".to_string(), "team:sre".to_string()]
        );
        // Object form: BTreeMap ordering is deterministic.
        assert_eq!(
            tags_from_config(Some(&json!({"env": "prod", "team": "sre"}))),
            vec!["env:prod".to_string(), "team:sre".to_string()]
        );
        assert!(tags_from_config(None).is_empty());
    }

    // -- batching / flush cadence -------------------------------------------

    #[test]
    fn should_flush_cadence() {
        let big = Duration::from_secs(15);
        let none = Duration::from_secs(0);
        // Empty buffer never flushes, even when the interval has elapsed.
        assert!(!should_flush(0, 10, none, big));
        assert!(!should_flush(0, 10, Duration::from_secs(60), big));
        // Full buffer flushes immediately.
        assert!(should_flush(10, 10, none, big));
        assert!(should_flush(11, 10, none, big));
        // Time-based flush before the batch is full.
        assert!(should_flush(3, 10, Duration::from_secs(20), big));
        // Under size and under interval: hold.
        assert!(!should_flush(3, 10, Duration::from_secs(1), big));
    }

    #[test]
    fn ingest_accumulates_without_flushing() {
        let mut dd = Datadog::default(); // client None, large batch/interval
        let snap = json!({
            "timestamp_ms": 1000u64,
            "interval_secs": 1.0,
            "series": [
                {"metric": "vus", "kind": "gauge", "agg": {"last": 1.0}},
                {"metric": "http_reqs", "kind": "counter", "interval_sum": 2.0}
            ]
        })
        .to_string();

        assert_eq!(dd.ingest(&snap), 2);
        assert_eq!(dd.ingest(&snap), 2);
        assert_eq!(dd.buffer.len(), 4);
        assert_eq!(dd.flushes, 0);
    }

    #[test]
    fn on_snapshot_flushes_when_batch_full() {
        let mut dd = Datadog::default();
        dd.batch_size = 1; // flush after every snapshot (client None -> just clears)
        let snap = json!({
            "timestamp_ms": 1000u64,
            "series": [{"metric": "vus", "kind": "gauge", "agg": {"last": 1.0}}]
        })
        .to_string();

        dd.on_snapshot(RString::from(snap));
        assert_eq!(dd.buffer.len(), 0, "buffer flushed and cleared");
        assert_eq!(dd.flushes, 1);
        assert_eq!(dd.sent_series, 1);
    }

    // -- HTTP retry / backoff (no network) ----------------------------------

    #[test]
    fn retries_on_5xx_then_succeeds() {
        let calls = AtomicUsize::new(0);
        let script = [503u16, 500, 200];
        let res = runtime().block_on(send_with_retry(
            || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let status = script[n];
                async move { Ok::<u16, String>(status) }
            },
            5,
            Duration::from_millis(1),
        ));
        assert!(res.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn does_not_retry_on_4xx() {
        let calls = AtomicUsize::new(0);
        let res = runtime().block_on(send_with_retry(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok::<u16, String>(403) }
            },
            5,
            Duration::from_millis(1),
        ));
        assert!(res.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn retries_are_bounded_by_max_retries() {
        let calls = AtomicUsize::new(0);
        // Always 503, budget of 2 retries => 1 initial + 2 retries = 3 attempts.
        let res = runtime().block_on(send_with_retry(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok::<u16, String>(503) }
            },
            2,
            Duration::from_millis(1),
        ));
        assert!(res.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn transport_errors_are_retried_then_surfaced() {
        let calls = AtomicUsize::new(0);
        let res = runtime().block_on(send_with_retry(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Err::<u16, String>("connection refused".to_string()) }
            },
            1,
            Duration::from_millis(1),
        ));
        assert_eq!(res.as_ref().unwrap_err(), "connection refused");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn backoff_grows_and_caps() {
        let base = Duration::from_millis(10);
        assert_eq!(backoff_delay(0, base), Duration::from_millis(10));
        assert_eq!(backoff_delay(1, base), Duration::from_millis(20));
        assert_eq!(backoff_delay(2, base), Duration::from_millis(40));
        assert!(backoff_delay(1, base) > backoff_delay(0, base));
        assert!(backoff_delay(2, base) > backoff_delay(1, base));
        // Never exceeds the 30s cap.
        assert!(backoff_delay(30, Duration::from_secs(5)) <= Duration::from_secs(30));
    }

    #[test]
    fn should_retry_classifies_status() {
        assert!(should_retry(500));
        assert!(should_retry(503));
        assert!(!should_retry(200));
        assert!(!should_retry(429));
        assert!(!should_retry(404));
    }

    // -- robustness: empty / malformed snapshots ----------------------------

    #[test]
    fn no_panic_on_empty_or_malformed_snapshots() {
        let mut dd = Datadog::default();
        dd.on_snapshot(RString::from("{}"));
        dd.on_snapshot(RString::from("{\"series\":[]}"));
        dd.on_snapshot(RString::from("not json at all"));
        // A series entry missing its metric name is skipped, not fatal.
        dd.on_snapshot(RString::from("{\"series\":[{\"kind\":\"gauge\"}]}"));
        assert_eq!(dd.buffer.len(), 0);
        assert_eq!(dd.flushes, 0);
        // finish on an empty buffer is a no-op (no network, no panic).
        dd.finish(RString::from("{}"));
        assert_eq!(dd.flushes, 0);
    }

    #[test]
    fn empty_snapshot_produces_no_series() {
        assert!(series_from_snapshot(&json!({}), "loadr.", &[]).is_empty());
        assert!(series_from_snapshot(&json!({"series": []}), "loadr.", &[]).is_empty());
    }

    #[test]
    fn info_declares_output_kind() {
        let info = plugin_info();
        let v: Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "output");
        assert_eq!(v["name"], "datadog");
    }
}
