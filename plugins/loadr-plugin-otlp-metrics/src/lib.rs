//! `loadr-plugin-otlp-metrics` — a native **output** plugin that exports loadr's
//! per-second metric snapshots as [OpenTelemetry] **OTLP metrics** over
//! **OTLP/HTTP** to any OpenTelemetry collector.
//!
//! # How it plugs in
//!
//! loadr's native output ABI ([`FfiOutput`]) is the same `start` /
//! `on_snapshot` / `finish` lifecycle used by the shipped `native-output`
//! example: the host calls `start(config)` once, then hands the plugin a JSON
//! snapshot roughly once a second, then `finish(summary)` at the end. This
//! plugin converts each snapshot's series into an OTLP
//! `ExportMetricsServiceRequest` and POSTs it to the collector's `/v1/metrics`
//! endpoint.
//!
//! # Transport & encoding
//!
//! OTLP/HTTP is plain HTTP(S). The body is either binary protobuf
//! (`Content-Type: application/x-protobuf`, the default) or the equivalent OTLP
//! JSON (`Content-Type: application/json`). The protobuf body is hand-encoded on
//! the wire here — no `prost`, no `protox`, no `protoc`, no OpenTelemetry SDK —
//! keeping the build pure-Rust and dependency-light. The request ships over the
//! project's existing **hyper + hyper-rustls** stack (`ring` + webpki roots), so
//! the cdylib cross-compiles cleanly for every release target. A single
//! multi-thread Tokio runtime, created once, drives the async POSTs; the host
//! calls the plugin from an ordinary thread, so each export `block_on`s.
//!
//! # Mapping
//!
//! Each snapshot series maps to one or more OTLP metrics, with every data point
//! carrying the series' tags (and the run's `run_id`, when known) as attributes:
//!
//!   * **counter** → a monotonic cumulative `Sum` data point.
//!   * **gauge**   → a `Gauge` data point of the last value.
//!   * **rate**    → a `Gauge` data point of the pass fraction.
//!   * **trend**   → one `Gauge` data point per present quantile
//!     (`<metric>.avg`, `.p95`, `.p99`, …).
//!
//! [OpenTelemetry]: https://opentelemetry.io/

use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{HeaderName, HeaderValue, CONTENT_TYPE};
use hyper::Request;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use once_cell::sync::OnceCell;
use serde_json::{json, Value};
use tokio::runtime::Runtime;

use loadr_plugin_api::abi::{
    FfiOutput, FfiOutputBox, FfiOutput_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "otlp-metrics";

/// The OTLP instrumentation-scope name attached to every exported metric.
const SCOPE_NAME: &str = "loadr";

/// OTLP `AggregationTemporality::CUMULATIVE`.
const CUMULATIVE: u64 = 2;

/// Trend aggregations projected as individual gauge data points, mapping the
/// snapshot `agg` field to the metric-name suffix the backend sees.
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

// ---------------------------------------------------------------------------
// Protobuf wire encoder (minimal, OTLP subset only)
// ---------------------------------------------------------------------------

/// Hand-rolled protobuf primitives, sufficient for the OTLP metrics message.
/// Only the wire types the OTLP subset needs are implemented:
/// `0` (varint), `1` (64-bit), `2` (length-delimited).
mod pb {
    /// Append a base-128 varint.
    pub fn varint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                out.push(byte | 0x80);
            } else {
                out.push(byte);
                break;
            }
        }
    }

    /// Append a field tag: `(field_number << 3) | wire_type`.
    fn tag(out: &mut Vec<u8>, field: u32, wire: u8) {
        varint(out, ((field as u64) << 3) | wire as u64);
    }

    /// Append a length-delimited field (wire type 2).
    pub fn bytes_field(out: &mut Vec<u8>, field: u32, data: &[u8]) {
        tag(out, field, 2);
        varint(out, data.len() as u64);
        out.extend_from_slice(data);
    }

    /// Append a UTF-8 string field (wire type 2).
    pub fn string_field(out: &mut Vec<u8>, field: u32, s: &str) {
        bytes_field(out, field, s.as_bytes());
    }

    /// Append a varint-valued field (wire type 0).
    pub fn varint_field(out: &mut Vec<u8>, field: u32, v: u64) {
        tag(out, field, 0);
        varint(out, v);
    }

    /// Append a `bool` field (wire type 0).
    pub fn bool_field(out: &mut Vec<u8>, field: u32, v: bool) {
        varint_field(out, field, v as u64);
    }

    /// Append a `fixed64` field (wire type 1), little-endian.
    pub fn fixed64_field(out: &mut Vec<u8>, field: u32, v: u64) {
        tag(out, field, 1);
        out.extend_from_slice(&v.to_le_bytes());
    }

    /// Append a `double` field (wire type 1), IEEE-754 little-endian.
    pub fn double_field(out: &mut Vec<u8>, field: u32, v: f64) {
        fixed64_field(out, field, v.to_bits());
    }
}

/// Encode one OTLP `KeyValue { key, value: AnyValue { string_value } }`.
fn encode_key_value(key: &str, value: &str) -> Vec<u8> {
    let mut kv = Vec::new();
    pb::string_field(&mut kv, 1, key); // KeyValue.key
    let mut any = Vec::new();
    pb::string_field(&mut any, 1, value); // AnyValue.string_value
    pb::bytes_field(&mut kv, 2, &any); // KeyValue.value
    kv
}

/// Encode one OTLP `NumberDataPoint` (attributes + time + `as_double`).
fn encode_number_data_point(point: &DataPoint) -> Vec<u8> {
    let mut dp = Vec::new();
    for (k, v) in &point.attrs {
        let kv = encode_key_value(k, v);
        pb::bytes_field(&mut dp, 7, &kv); // NumberDataPoint.attributes
    }
    pb::fixed64_field(&mut dp, 3, point.time_nano); // NumberDataPoint.time_unix_nano
    pb::double_field(&mut dp, 4, point.value); // NumberDataPoint.as_double
    dp
}

/// Encode one OTLP `Metric` (Gauge or monotonic cumulative Sum).
fn encode_metric(metric: &MetricOut) -> Vec<u8> {
    let mut m = Vec::new();
    pb::string_field(&mut m, 1, &metric.name); // Metric.name
    if !metric.unit.is_empty() {
        pb::string_field(&mut m, 3, &metric.unit); // Metric.unit
    }
    let points: Vec<Vec<u8>> = metric.points.iter().map(encode_number_data_point).collect();
    match metric.kind {
        MetricKind::Gauge => {
            let mut gauge = Vec::new();
            for dp in &points {
                pb::bytes_field(&mut gauge, 1, dp); // Gauge.data_points
            }
            pb::bytes_field(&mut m, 5, &gauge); // Metric.gauge
        }
        MetricKind::Sum => {
            let mut sum = Vec::new();
            for dp in &points {
                pb::bytes_field(&mut sum, 1, dp); // Sum.data_points
            }
            pb::varint_field(&mut sum, 2, CUMULATIVE); // Sum.aggregation_temporality
            pb::bool_field(&mut sum, 3, true); // Sum.is_monotonic
            pb::bytes_field(&mut m, 7, &sum); // Metric.sum
        }
    }
    m
}

/// Encode a full OTLP `ExportMetricsServiceRequest` on the protobuf wire.
fn encode_request_protobuf(
    metrics: &[MetricOut],
    resource_attrs: &[(String, String)],
    scope_name: &str,
) -> Vec<u8> {
    // Resource { attributes }
    let mut resource = Vec::new();
    for (k, v) in resource_attrs {
        let kv = encode_key_value(k, v);
        pb::bytes_field(&mut resource, 1, &kv);
    }

    // ScopeMetrics { scope { name }, metrics[] }
    let mut scope_metrics = Vec::new();
    let mut scope = Vec::new();
    pb::string_field(&mut scope, 1, scope_name);
    pb::bytes_field(&mut scope_metrics, 1, &scope); // ScopeMetrics.scope
    for metric in metrics {
        let encoded = encode_metric(metric);
        pb::bytes_field(&mut scope_metrics, 2, &encoded); // ScopeMetrics.metrics
    }

    // ResourceMetrics { resource, scope_metrics[] }
    let mut resource_metrics = Vec::new();
    pb::bytes_field(&mut resource_metrics, 1, &resource);
    pb::bytes_field(&mut resource_metrics, 2, &scope_metrics);

    // ExportMetricsServiceRequest { resource_metrics[] }
    let mut request = Vec::new();
    pb::bytes_field(&mut request, 1, &resource_metrics);
    request
}

/// Encode a full OTLP `ExportMetricsServiceRequest` as OTLP/HTTP JSON.
fn encode_request_json(
    metrics: &[MetricOut],
    resource_attrs: &[(String, String)],
    scope_name: &str,
) -> Vec<u8> {
    let resource_kv: Vec<Value> = resource_attrs
        .iter()
        .map(|(k, v)| json!({ "key": k, "value": { "stringValue": v } }))
        .collect();
    let metric_json: Vec<Value> = metrics.iter().map(metric_to_json).collect();
    let body = json!({
        "resourceMetrics": [{
            "resource": { "attributes": resource_kv },
            "scopeMetrics": [{
                "scope": { "name": scope_name },
                "metrics": metric_json
            }]
        }]
    });
    serde_json::to_vec(&body).unwrap_or_default()
}

/// OTLP JSON for one metric.
fn metric_to_json(metric: &MetricOut) -> Value {
    let data_points: Vec<Value> = metric
        .points
        .iter()
        .map(|dp| {
            let attrs: Vec<Value> = dp
                .attrs
                .iter()
                .map(|(k, v)| json!({ "key": k, "value": { "stringValue": v } }))
                .collect();
            json!({
                "timeUnixNano": dp.time_nano.to_string(),
                "asDouble": dp.value,
                "attributes": attrs
            })
        })
        .collect();
    match metric.kind {
        MetricKind::Gauge => json!({
            "name": metric.name,
            "unit": metric.unit,
            "gauge": { "dataPoints": data_points }
        }),
        MetricKind::Sum => json!({
            "name": metric.name,
            "unit": metric.unit,
            "sum": {
                "dataPoints": data_points,
                "aggregationTemporality": CUMULATIVE,
                "isMonotonic": true
            }
        }),
    }
}

// ---------------------------------------------------------------------------
// Intermediate representation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricKind {
    Gauge,
    Sum,
}

#[derive(Debug, Clone, PartialEq)]
struct DataPoint {
    time_nano: u64,
    value: f64,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
struct MetricOut {
    name: String,
    unit: String,
    kind: MetricKind,
    points: Vec<DataPoint>,
}

/// Total data points across a metric slice.
fn count_data_points(metrics: &[MetricOut]) -> usize {
    metrics.iter().map(|m| m.points.len()).sum()
}

// ---------------------------------------------------------------------------
// Snapshot -> OTLP metric mapping
// ---------------------------------------------------------------------------

fn now_unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Finite numeric field lookup — non-finite values are rejected so a stray
/// NaN/Inf never produces an invalid data point.
fn num(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(Value::as_f64).filter(|f| f.is_finite())
}

/// The snapshot timestamp in Unix nanoseconds, defaulting to "now".
fn snapshot_time_nano(snap: &Value) -> u64 {
    snap.get("timestamp_ms")
        .and_then(Value::as_u64)
        .map(|ms| ms.saturating_mul(1_000_000))
        .unwrap_or_else(now_unix_nanos)
}

/// Data-point attributes for a series entry: the run's `run_id` (when known)
/// plus the series' own string tags.
fn point_attrs(entry: &Value, run_id: Option<&str>) -> Vec<(String, String)> {
    let mut attrs = Vec::new();
    if let Some(id) = run_id.filter(|s| !s.is_empty()) {
        attrs.push(("run_id".to_string(), id.to_string()));
    }
    if let Some(obj) = entry.get("tags").and_then(Value::as_object) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                attrs.push((k.clone(), s.to_string()));
            }
        }
    }
    attrs
}

/// Turn one snapshot (as JSON) into a flat list of OTLP metrics. Never panics:
/// a missing/oddly-shaped snapshot simply yields no metrics.
fn metrics_from_snapshot(snap: &Value, run_id: Option<&str>) -> Vec<MetricOut> {
    let time_nano = snapshot_time_nano(snap);
    let Some(entries) = snap.get("series").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for entry in entries {
        let Some(name) = entry.get("metric").and_then(Value::as_str) else {
            continue;
        };
        let kind = entry.get("kind").and_then(Value::as_str).unwrap_or("gauge");
        let unit = entry
            .get("unit")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let attrs = point_attrs(entry, run_id);
        let agg = entry.get("agg");

        let point = |value: f64| DataPoint {
            time_nano,
            value,
            attrs: attrs.clone(),
        };

        match kind {
            "counter" => {
                // Cumulative count, falling back to the interval increase.
                if let Some(v) = agg
                    .and_then(|a| num(a, "count"))
                    .or_else(|| num(entry, "interval_sum"))
                {
                    out.push(MetricOut {
                        name: name.to_string(),
                        unit,
                        kind: MetricKind::Sum,
                        points: vec![point(v)],
                    });
                }
            }
            "rate" => {
                let v = agg.and_then(|a| num(a, "rate")).unwrap_or(0.0);
                out.push(MetricOut {
                    name: name.to_string(),
                    unit,
                    kind: MetricKind::Gauge,
                    points: vec![point(v)],
                });
            }
            "trend" => {
                for (key, suffix) in TREND_AGGS {
                    if let Some(v) = agg.and_then(|a| num(a, key)) {
                        out.push(MetricOut {
                            name: format!("{name}.{suffix}"),
                            unit: unit.clone(),
                            kind: MetricKind::Gauge,
                            points: vec![point(v)],
                        });
                    }
                }
            }
            // "gauge" and any unknown kind: report the last value as a gauge.
            _ => {
                let v = agg.and_then(|a| num(a, "last")).unwrap_or(0.0);
                out.push(MetricOut {
                    name: name.to_string(),
                    unit,
                    kind: MetricKind::Gauge,
                    points: vec![point(v)],
                });
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Protobuf,
    Json,
}

impl Encoding {
    fn content_type(self) -> &'static str {
        match self {
            Encoding::Protobuf => "application/x-protobuf",
            Encoding::Json => "application/json",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    endpoint: String,
    encoding: Encoding,
    headers: Vec<(String, String)>,
    resource_attrs: Vec<(String, String)>,
    timeout_ms: u64,
    max_retries: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            endpoint: String::new(),
            encoding: Encoding::Protobuf,
            headers: Vec::new(),
            resource_attrs: vec![("service.name".to_string(), "loadr".to_string())],
            timeout_ms: 10_000,
            max_retries: 3,
        }
    }
}

/// Append `/v1/metrics` to a collector base URL unless it is already present.
fn build_endpoint(base: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    if trimmed.ends_with("/v1/metrics") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/metrics")
    }
}

/// Parse a JSON object map (`{k: v}`) into ordered string pairs, skipping any
/// non-string values.
fn string_pairs(v: Option<&Value>) -> Vec<(String, String)> {
    match v.and_then(Value::as_object) {
        Some(obj) => obj
            .iter()
            .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
        None => Vec::new(),
    }
}

impl Config {
    /// Build a validated config from the plugin config JSON.
    fn from_json(cfg: &Value) -> Result<Config, String> {
        let raw_endpoint = cfg
            .get("endpoint")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "otlp-metrics output requires a non-empty `endpoint`".to_string())?;
        if !(raw_endpoint.starts_with("http://") || raw_endpoint.starts_with("https://")) {
            return Err(format!(
                "otlp-metrics `endpoint` must be an http(s) URL, got `{raw_endpoint}`"
            ));
        }

        let encoding = match cfg.get("encoding").and_then(Value::as_str) {
            None => Encoding::Protobuf,
            Some(s) => match s.to_ascii_lowercase().as_str() {
                "protobuf" | "proto" | "pb" => Encoding::Protobuf,
                "json" => Encoding::Json,
                other => {
                    return Err(format!(
                        "otlp-metrics `encoding` must be `protobuf` or `json`, got `{other}`"
                    ))
                }
            },
        };

        let service_name = cfg
            .get("service_name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("loadr")
            .to_string();
        let mut resource_attrs = vec![("service.name".to_string(), service_name)];
        resource_attrs.extend(string_pairs(cfg.get("resource_attributes")));

        Ok(Config {
            endpoint: build_endpoint(raw_endpoint),
            encoding,
            headers: string_pairs(cfg.get("headers")),
            resource_attrs,
            timeout_ms: cfg
                .get("timeout_ms")
                .and_then(Value::as_u64)
                .unwrap_or(10_000),
            max_retries: cfg.get("max_retries").and_then(Value::as_u64).unwrap_or(3) as u32,
        })
    }
}

// ---------------------------------------------------------------------------
// HTTP transport seam
// ---------------------------------------------------------------------------

/// The seam every export goes through. The real implementation POSTs over
/// hyper; tests substitute a recording mock so no unit test touches the
/// network. `send` returns the HTTP status on a completed request, or `Err` on
/// a transport failure.
trait HttpSender: Send {
    fn send(&self, body: Bytes, content_type: &str) -> Result<u16, String>;
}

/// The hyper HTTPS client (pure-Rust TLS, cheaply cloneable, internally pooled).
type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// The single Tokio runtime the plugin uses to drive async HTTP.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build otlp-metrics plugin tokio runtime")
    })
}

fn build_client() -> HttpClient {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // lets the same connector serve a plaintext collector too.
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(tls)
}

/// Real OTLP/HTTP sender: one POST to the collector's `/v1/metrics` endpoint.
struct HyperSender {
    client: HttpClient,
    endpoint: String,
    headers: Vec<(HeaderName, HeaderValue)>,
    timeout_ms: u64,
}

impl HttpSender for HyperSender {
    fn send(&self, body: Bytes, content_type: &str) -> Result<u16, String> {
        runtime().block_on(async move {
            let mut builder = Request::builder()
                .method("POST")
                .uri(&self.endpoint)
                .header(CONTENT_TYPE, content_type);
            for (name, value) in &self.headers {
                builder = builder.header(name, value);
            }
            let request = builder
                .body(Full::new(body))
                .map_err(|e| format!("building request failed: {e}"))?;

            let send = self.client.request(request);
            let resp = if self.timeout_ms == 0 {
                send.await
                    .map_err(|e| format!("request to {} failed: {e}", self.endpoint))?
            } else {
                tokio::time::timeout(Duration::from_millis(self.timeout_ms), send)
                    .await
                    .map_err(|_| {
                        format!(
                            "request to {} timed out after {}ms",
                            self.endpoint, self.timeout_ms
                        )
                    })?
                    .map_err(|e| format!("request to {} failed: {e}", self.endpoint))?
            };
            let status = resp.status().as_u16();
            // Drain (and discard) the body so the pooled connection is reusable.
            let _ = resp.into_body().collect().await;
            Ok(status)
        })
    }
}

/// Build the real sender from a validated config, parsing headers up front so a
/// bad header fails `start` rather than silently dropping every export.
fn build_sender(config: &Config) -> Result<Box<dyn HttpSender + Send>, String> {
    let mut headers = Vec::with_capacity(config.headers.len());
    for (k, v) in &config.headers {
        let name = HeaderName::from_bytes(k.as_bytes())
            .map_err(|e| format!("invalid header name `{k}`: {e}"))?;
        let value =
            HeaderValue::from_str(v).map_err(|e| format!("invalid header value for `{k}`: {e}"))?;
        headers.push((name, value));
    }
    Ok(Box::new(HyperSender {
        client: build_client(),
        endpoint: config.endpoint.clone(),
        headers,
        timeout_ms: config.timeout_ms,
    }))
}

// ---------------------------------------------------------------------------
// Retry / backoff (no network)
// ---------------------------------------------------------------------------

/// Whether an HTTP status warrants a retry (server-side 5xx only; 4xx is a
/// permanent client error surfaced immediately).
fn should_retry(status: u16) -> bool {
    (500..600).contains(&status)
}

/// Exponential backoff: `base * 2^attempt`, capped at 30s.
fn backoff_delay(attempt: u32, base: Duration) -> Duration {
    let factor = 1u32.checked_shl(attempt.min(6)).unwrap_or(64);
    base.saturating_mul(factor).min(Duration::from_secs(30))
}

/// Drive `sender` with retry/backoff: 2xx succeeds; 5xx and transport errors
/// retry up to `max_retries` times with growing backoff; 4xx fails immediately.
fn export_with_retry(
    sender: &dyn HttpSender,
    body: Bytes,
    content_type: &str,
    max_retries: u32,
    base: Duration,
) -> Result<u16, String> {
    let mut attempt = 0u32;
    loop {
        match sender.send(body.clone(), content_type) {
            Ok(status) if (200..300).contains(&status) => return Ok(status),
            Ok(status) if should_retry(status) && attempt < max_retries => {
                std::thread::sleep(backoff_delay(attempt, base));
                attempt += 1;
            }
            Ok(status) => return Err(format!("collector returned HTTP {status}")),
            Err(_) if attempt < max_retries => {
                std::thread::sleep(backoff_delay(attempt, base));
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// The output plugin
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Otlp {
    config: Config,
    run_id: Option<String>,
    sender: Option<Box<dyn HttpSender + Send>>,
    base_backoff: Duration,
    datapoints_sent: u64,
    export_errors: u64,
}

impl Otlp {
    /// Encode a snapshot's metrics into the configured wire body.
    fn encode(&self, metrics: &[MetricOut]) -> Vec<u8> {
        match self.config.encoding {
            Encoding::Protobuf => {
                encode_request_protobuf(metrics, &self.config.resource_attrs, SCOPE_NAME)
            }
            Encoding::Json => encode_request_json(metrics, &self.config.resource_attrs, SCOPE_NAME),
        }
    }

    /// Export an already-encoded body, accounting for success/failure. A no-op
    /// without a sender (only reachable if `start` was skipped, e.g. a unit
    /// test exercising the mapping path).
    fn export(&mut self, body: Bytes, datapoints: usize) {
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        let content_type = self.config.encoding.content_type();
        match export_with_retry(
            sender.as_ref(),
            body,
            content_type,
            self.config.max_retries,
            self.base_backoff,
        ) {
            Ok(_) => self.datapoints_sent += datapoints as u64,
            Err(e) => {
                self.export_errors += 1;
                eprintln!("otlp-metrics: export of {datapoints} data points failed: {e}");
            }
        }
    }
}

impl FfiOutput for Otlp {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<(), RString> {
        let cfg: Value = match serde_json::from_str(config_json.as_str()) {
            Ok(v) => v,
            Err(e) => return RErr(RString::from(format!("invalid config JSON: {e}"))),
        };
        let config = match Config::from_json(&cfg) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(e)),
        };
        let sender = match build_sender(&config) {
            Ok(s) => s,
            Err(e) => return RErr(RString::from(e)),
        };
        self.config = config;
        self.sender = Some(sender);
        self.base_backoff = Duration::from_millis(500);
        self.run_id = None;
        self.datapoints_sent = 0;
        self.export_errors = 0;
        ROk(())
    }

    fn on_samples(&mut self, _samples_json: RString) {}

    fn on_snapshot(&mut self, snapshot_json: RString) {
        let Ok(value) = serde_json::from_str::<Value>(snapshot_json.as_str()) else {
            return;
        };
        if let Some(id) = value
            .get("run_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            self.run_id = Some(id.to_string());
        }
        let metrics = metrics_from_snapshot(&value, self.run_id.as_deref());
        let datapoints = count_data_points(&metrics);
        if datapoints == 0 {
            return;
        }
        let body = Bytes::from(self.encode(&metrics));
        self.export(body, datapoints);
    }

    fn finish(&mut self, summary_json: RString) {
        if let Ok(value) = serde_json::from_str::<Value>(summary_json.as_str()) {
            if self.run_id.is_none() {
                if let Some(id) = value
                    .get("run_id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    self.run_id = Some(id.to_string());
                }
            }
        }
        eprintln!(
            "otlp-metrics: exported {} data points ({} export errors)",
            self.datapoints_sent, self.export_errors
        );
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "output",
            "description": "Encodes snapshot series as OTLP metrics (protobuf/JSON over OTLP/HTTP) and posts them to an OpenTelemetry collector",
        })
        .to_string(),
    )
}

extern "C" fn make_output() -> FfiOutputBox {
    FfiOutput_TO::from_value(Otlp::default(), abi_stable::erased_types::TD_Opaque)
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
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    // -- test transport seam -------------------------------------------------

    /// A recording, scriptable sender; no network. Shared via `Arc` so a test
    /// can inspect it after handing a clone to the plugin.
    #[derive(Clone, Default)]
    struct MockSender {
        calls: Arc<Mutex<Vec<(Bytes, String)>>>,
        statuses: Arc<Mutex<VecDeque<Result<u16, String>>>>,
    }

    impl MockSender {
        fn with_statuses(statuses: &[Result<u16, String>]) -> Self {
            MockSender {
                calls: Arc::new(Mutex::new(Vec::new())),
                statuses: Arc::new(Mutex::new(statuses.iter().cloned().collect())),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }

        fn last_content_type(&self) -> Option<String> {
            self.calls.lock().unwrap().last().map(|(_, ct)| ct.clone())
        }

        fn last_body(&self) -> Option<Bytes> {
            self.calls.lock().unwrap().last().map(|(b, _)| b.clone())
        }
    }

    impl HttpSender for MockSender {
        fn send(&self, body: Bytes, content_type: &str) -> Result<u16, String> {
            self.calls
                .lock()
                .unwrap()
                .push((body, content_type.to_string()));
            self.statuses.lock().unwrap().pop_front().unwrap_or(Ok(200))
        }
    }

    // -- helpers ------------------------------------------------------------

    /// Parse a config that is expected to be valid.
    fn ok_cfg(v: Value) -> Config {
        Config::from_json(&v).expect("valid config")
    }

    /// Parse a config that is expected to be rejected, returning the message.
    fn err_cfg(v: Value) -> String {
        Config::from_json(&v).unwrap_err()
    }

    /// Build a plugin instance wired to a mock sender (no `start`, no network).
    fn plugin_with(config: Config, sender: MockSender) -> (Otlp, MockSender) {
        let otlp = Otlp {
            config,
            run_id: None,
            sender: Some(Box::new(sender.clone())),
            base_backoff: Duration::from_millis(0),
            datapoints_sent: 0,
            export_errors: 0,
        };
        (otlp, sender)
    }

    fn snapshot() -> String {
        json!({
            "timestamp_ms": 1_000u64, // -> 1_000_000_000 unix nanos
            "interval_secs": 1.0,
            "run_id": "run-123",
            "series": [
                {
                    "metric": "http_reqs",
                    "kind": "counter",
                    "tags": {"method": "GET"},
                    "interval_sum": 10.0,
                    "agg": {"count": 100.0}
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
        })
        .to_string()
    }

    fn find<'a>(metrics: &'a [MetricOut], name: &str) -> Option<&'a MetricOut> {
        metrics.iter().find(|m| m.name == name)
    }

    // -- snapshot -> OTLP mapping -------------------------------------------

    #[test]
    fn snapshot_maps_each_kind() {
        let snap: Value = serde_json::from_str(&snapshot()).unwrap();
        let metrics = metrics_from_snapshot(&snap, Some("run-123"));

        // counter -> monotonic Sum with the cumulative count.
        let c = find(&metrics, "http_reqs").expect("counter metric");
        assert_eq!(c.kind, MetricKind::Sum);
        assert_eq!(c.points[0].value, 100.0);
        assert_eq!(c.points[0].time_nano, 1_000_000_000);
        let run = ("run_id".to_string(), "run-123".to_string());
        let method = ("method".to_string(), "GET".to_string());
        assert!(c.points[0].attrs.contains(&run));
        assert!(c.points[0].attrs.contains(&method));

        // gauge -> last value.
        let g = find(&metrics, "vus").expect("gauge metric");
        assert_eq!(g.kind, MetricKind::Gauge);
        assert_eq!(g.points[0].value, 25.0);

        // rate -> pass fraction as a gauge.
        let r = find(&metrics, "checks").expect("rate metric");
        assert_eq!(r.kind, MetricKind::Gauge);
        assert_eq!(r.points[0].value, 0.99);

        // trend -> one gauge per present quantile; absent p99 is not emitted.
        let avg = find(&metrics, "http_req_duration.avg").expect("avg");
        let p95 = find(&metrics, "http_req_duration.p95").expect("p95");
        let max = find(&metrics, "http_req_duration.max").expect("max");
        assert_eq!(avg.points[0].value, 12.0);
        assert_eq!(p95.points[0].value, 30.0);
        assert_eq!(max.points[0].value, 50.0);
        assert!(find(&metrics, "http_req_duration.p99").is_none());

        // 1 counter + 1 gauge + 1 rate + 3 trend = 6 metrics.
        assert_eq!(metrics.len(), 6);
        assert_eq!(count_data_points(&metrics), 6);
    }

    #[test]
    fn counter_falls_back_to_interval_sum() {
        let snap = json!({
            "timestamp_ms": 1_000u64,
            "series": [{"metric": "bytes", "kind": "counter", "interval_sum": 7.0}]
        });
        let metrics = metrics_from_snapshot(&snap, None);
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].kind, MetricKind::Sum);
        assert_eq!(metrics[0].points[0].value, 7.0);
        // No run_id known -> no run_id attribute.
        assert!(metrics[0].points[0].attrs.is_empty());
    }

    #[test]
    fn empty_or_malformed_snapshot_yields_no_metrics() {
        assert!(metrics_from_snapshot(&json!({}), None).is_empty());
        assert!(metrics_from_snapshot(&json!({"series": []}), None).is_empty());
        // A series entry missing its metric name is skipped, not fatal.
        let snap = json!({"series": [{"kind": "gauge"}]});
        assert!(metrics_from_snapshot(&snap, None).is_empty());
    }

    #[test]
    fn non_finite_trend_values_are_dropped() {
        let snap = json!({
            "timestamp_ms": 2_000u64,
            "series": [{"metric": "t", "kind": "trend", "agg": {"avg": null}}]
        });
        assert!(metrics_from_snapshot(&snap, None).is_empty());
    }

    // -- config parsing -----------------------------------------------------

    #[test]
    fn from_json_requires_valid_endpoint() {
        assert!(Config::from_json(&json!({})).is_err());
        assert!(Config::from_json(&json!({"endpoint": ""})).is_err());
        // Non-http(s) scheme is rejected (e.g. a gRPC-style host:port).
        assert!(Config::from_json(&json!({"endpoint": "collector:4317"})).is_err());
        assert!(Config::from_json(&json!({"endpoint": "http://c:4318"})).is_ok());
    }

    #[test]
    fn from_json_appends_metrics_path_once() {
        let a = ok_cfg(json!({"endpoint": "http://collector:4318"}));
        assert_eq!(a.endpoint, "http://collector:4318/v1/metrics");
        // Trailing slash and an already-present path are both handled.
        let b = ok_cfg(json!({"endpoint": "http://collector:4318/"}));
        assert_eq!(b.endpoint, "http://collector:4318/v1/metrics");
        let c = ok_cfg(json!({"endpoint": "http://collector:4318/v1/metrics"}));
        assert_eq!(c.endpoint, "http://collector:4318/v1/metrics");
    }

    #[test]
    fn from_json_encoding_and_error() {
        let proto = ok_cfg(json!({"endpoint": "http://c"}));
        assert_eq!(proto.encoding, Encoding::Protobuf);
        // Case-insensitive `json`.
        let js = ok_cfg(json!({"endpoint": "http://c", "encoding": "JSON"}));
        assert_eq!(js.encoding, Encoding::Json);
        let err = err_cfg(json!({"endpoint": "http://c", "encoding": "grpc"}));
        assert!(err.contains("encoding"));
    }

    #[test]
    fn from_json_resource_attrs_and_service_name() {
        let c = ok_cfg(json!({
            "endpoint": "http://c:4318",
            "service_name": "checkout",
            "resource_attributes": {"deployment.environment": "staging"}
        }));
        let svc = ("service.name".to_string(), "checkout".to_string());
        let env = ("deployment.environment".to_string(), "staging".to_string());
        assert!(c.resource_attrs.contains(&svc));
        assert!(c.resource_attrs.contains(&env));

        // Default service.name when unset.
        let d = ok_cfg(json!({"endpoint": "http://c:4318"}));
        let default_svc = ("service.name".to_string(), "loadr".to_string());
        assert_eq!(d.resource_attrs, vec![default_svc]);
    }

    // -- encoders -----------------------------------------------------------

    #[test]
    fn varint_encodes_known_values() {
        let mut out = Vec::new();
        pb::varint(&mut out, 0);
        assert_eq!(out, vec![0x00]);
        out.clear();
        pb::varint(&mut out, 1);
        assert_eq!(out, vec![0x01]);
        out.clear();
        pb::varint(&mut out, 150);
        assert_eq!(out, vec![0x96, 0x01]);
        out.clear();
        pb::varint(&mut out, 300);
        assert_eq!(out, vec![0xac, 0x02]);
    }

    #[test]
    fn key_value_wire_bytes_are_stable() {
        // KeyValue{ key:"k"(f1,len1), value: AnyValue{ string_value:"v" }(f2,len4) }
        let kv = encode_key_value("k", "v");
        assert_eq!(kv, vec![0x0a, 0x01, b'k', 0x12, 0x03, 0x0a, 0x01, b'v']);
    }

    #[test]
    fn protobuf_request_is_non_empty_and_length_prefixed() {
        // A single small gauge keeps the encoded ResourceMetrics under 128
        // bytes, so its length fits in a single varint byte.
        let snap = json!({
            "timestamp_ms": 1_000u64,
            "series": [{"metric": "v", "kind": "gauge", "agg": {"last": 1.0}}]
        });
        let metrics = metrics_from_snapshot(&snap, None);
        let attrs = vec![("service.name".to_string(), "loadr".to_string())];
        let body = encode_request_protobuf(&metrics, &attrs, SCOPE_NAME);
        assert!(!body.is_empty());
        // Top-level field 1 (resource_metrics), wire type 2 (length-delimited).
        assert_eq!(body[0], (1 << 3) | 2);
        // The declared varint length matches the remaining bytes (one
        // ResourceMetrics). The length is a protobuf varint, not a single byte.
        let mut len = 0usize;
        let mut shift = 0u32;
        let mut i = 1;
        loop {
            let b = body[i];
            len |= ((b & 0x7f) as usize) << shift;
            i += 1;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        assert_eq!(len, body.len() - i);
    }

    #[test]
    fn json_request_shapes_otlp_document() {
        let snap: Value = serde_json::from_str(&snapshot()).unwrap();
        let metrics = metrics_from_snapshot(&snap, Some("run-123"));
        let attrs = vec![("service.name".to_string(), "loadr".to_string())];
        let body = encode_request_json(&metrics, &attrs, SCOPE_NAME);
        let doc: Value = serde_json::from_slice(&body).unwrap();

        let scope_metrics = &doc["resourceMetrics"][0]["scopeMetrics"][0];
        assert_eq!(scope_metrics["scope"]["name"], "loadr");
        let res_attrs = &doc["resourceMetrics"][0]["resource"]["attributes"];
        assert_eq!(res_attrs[0]["key"], "service.name");

        let out_metrics = scope_metrics["metrics"].as_array().unwrap();
        // The counter is a monotonic cumulative Sum in the JSON body.
        let counter = out_metrics
            .iter()
            .find(|m| m["name"] == "http_reqs")
            .unwrap();
        let sum = &counter["sum"];
        assert_eq!(sum["aggregationTemporality"], CUMULATIVE);
        assert_eq!(sum["isMonotonic"], true);
        assert_eq!(sum["dataPoints"][0]["asDouble"], 100.0);
        // timeUnixNano is a string per the OTLP JSON mapping.
        assert_eq!(sum["dataPoints"][0]["timeUnixNano"], "1000000000");
    }

    // -- export path (mock sender, no network) ------------------------------

    #[test]
    fn on_snapshot_exports_and_counts_datapoints() {
        let mut config = Config::default();
        config.endpoint = "http://c:4318/v1/metrics".to_string();
        config.encoding = Encoding::Json;
        let (mut otlp, mock) = plugin_with(config, MockSender::with_statuses(&[Ok(200)]));

        otlp.on_snapshot(RString::from(snapshot()));

        assert_eq!(mock.call_count(), 1);
        assert_eq!(otlp.datapoints_sent, 6);
        assert_eq!(otlp.export_errors, 0);
        assert_eq!(
            mock.last_content_type().as_deref(),
            Some("application/json")
        );
        // The exported body is valid OTLP JSON.
        let doc: Value = serde_json::from_slice(&mock.last_body().unwrap()).unwrap();
        let metrics = &doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
        assert!(metrics.as_array().is_some());
    }

    #[test]
    fn protobuf_export_sets_protobuf_content_type() {
        let mut config = Config::default();
        config.endpoint = "http://c:4318/v1/metrics".to_string();
        let (mut otlp, mock) = plugin_with(config, MockSender::with_statuses(&[Ok(200)]));

        otlp.on_snapshot(RString::from(snapshot()));

        let ct = mock.last_content_type();
        assert_eq!(ct.as_deref(), Some("application/x-protobuf"));
    }

    #[test]
    fn export_error_increments_error_counter() {
        let mut config = Config::default();
        config.endpoint = "http://c:4318/v1/metrics".to_string();
        config.max_retries = 0;
        let (mut otlp, _mock) = plugin_with(config, MockSender::with_statuses(&[Ok(500)]));

        otlp.on_snapshot(RString::from(snapshot()));

        assert_eq!(otlp.datapoints_sent, 0);
        assert_eq!(otlp.export_errors, 1);
    }

    #[test]
    fn empty_snapshot_does_not_export() {
        let mut config = Config::default();
        config.endpoint = "http://c:4318/v1/metrics".to_string();
        let (mut otlp, mock) = plugin_with(config, MockSender::with_statuses(&[Ok(200)]));

        otlp.on_snapshot(RString::from("{}"));
        otlp.on_snapshot(RString::from("{\"series\":[]}"));
        otlp.on_snapshot(RString::from("not json at all"));

        assert_eq!(mock.call_count(), 0);
        assert_eq!(otlp.datapoints_sent, 0);
        assert_eq!(otlp.export_errors, 0);
    }

    // -- retry / backoff ----------------------------------------------------

    #[test]
    fn export_retries_5xx_then_succeeds() {
        let mock = MockSender::with_statuses(&[Ok(503), Ok(500), Ok(200)]);
        let body = Bytes::from_static(b"x");
        let res = export_with_retry(&mock, body, "application/x-protobuf", 5, Duration::ZERO);
        assert!(res.is_ok());
        assert_eq!(mock.call_count(), 3);
    }

    #[test]
    fn export_does_not_retry_4xx() {
        let mock = MockSender::with_statuses(&[Ok(403)]);
        let body = Bytes::from_static(b"x");
        let res = export_with_retry(&mock, body, "application/x-protobuf", 5, Duration::ZERO);
        assert!(res.is_err());
        assert_eq!(mock.call_count(), 1);
    }

    #[test]
    fn export_retries_are_bounded() {
        let mock = MockSender::with_statuses(&[Ok(503), Ok(503), Ok(503), Ok(503)]);
        let body = Bytes::from_static(b"x");
        // 1 initial + 2 retries = 3 attempts.
        let res = export_with_retry(&mock, body, "application/x-protobuf", 2, Duration::ZERO);
        assert!(res.is_err());
        assert_eq!(mock.call_count(), 3);
    }

    #[test]
    fn export_retries_transport_errors_then_surfaces() {
        let refused = || Err::<u16, String>("connection refused".to_string());
        let mock = MockSender::with_statuses(&[refused(), refused()]);
        let body = Bytes::from_static(b"x");
        let res = export_with_retry(&mock, body, "application/x-protobuf", 1, Duration::ZERO);
        assert_eq!(res.as_ref().unwrap_err(), "connection refused");
        assert_eq!(mock.call_count(), 2);
    }

    #[test]
    fn should_retry_and_backoff_behaviour() {
        assert!(should_retry(500));
        assert!(should_retry(503));
        assert!(!should_retry(200));
        assert!(!should_retry(404));

        let base = Duration::from_millis(10);
        assert_eq!(backoff_delay(0, base), Duration::from_millis(10));
        assert_eq!(backoff_delay(1, base), Duration::from_millis(20));
        assert_eq!(backoff_delay(2, base), Duration::from_millis(40));
        assert!(backoff_delay(30, Duration::from_secs(5)) <= Duration::from_secs(30));
    }

    // -- info ---------------------------------------------------------------

    #[test]
    fn info_declares_output_kind() {
        let info = plugin_info();
        let v: Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "output");
        assert_eq!(v["name"], "otlp-metrics");
    }
}
