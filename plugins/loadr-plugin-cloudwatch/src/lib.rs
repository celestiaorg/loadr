//! `loadr-plugin-cloudwatch` — a native **output** plugin that streams loadr's
//! per-second metric snapshots into [Amazon CloudWatch] via the
//! `PutMetricData` API.
//!
//! # How it plugs in
//!
//! loadr's native output ABI ([`FfiOutput`]) is the same `start` /
//! `on_snapshot` / `finish` lifecycle used by the shipped `native-output`
//! example: the host calls `start(config)` once, hands the plugin a JSON
//! `Snapshot` roughly once a second during the run, then `finish(summary)` at
//! the end. This plugin converts each snapshot's series into CloudWatch metric
//! data, signs a `PutMetricData` request with SigV4 and POSTs it straight to
//! the regional monitoring endpoint. There is **no per-second buffering** — each
//! snapshot is a live submission — and a flush failure is counted and logged
//! without stalling or failing the run.
//!
//! # Transport
//!
//! CloudWatch `PutMetricData` is the plain AWS **Query API** over HTTPS
//! (`application/x-www-form-urlencoded` body, service `monitoring`), so the
//! request is sent directly over the project's existing **hyper + hyper-rustls**
//! stack and signed by a hand-rolled, pure-Rust **SigV4** signer (`sha2` +
//! `hmac`). No AWS SDK, no CloudWatch agent, no StatsD hop, and no OpenSSL/C
//! dependency, so the cdylib cross-compiles cleanly for every release target.
//!
//! # Mapping
//!
//! Each snapshot series maps to one or more CloudWatch metric data points:
//!
//!   * **counter** → one `Count` metric carrying the interval increase
//!     (`interval_sum`);
//!   * **gauge**   → one plain (`None`-unit) metric of the last value;
//!   * **rate**    → one plain metric of the pass fraction;
//!   * **trend**   → one plain metric per present aggregation
//!     (`<metric>.avg`, `<metric>.p95`, …).
//!
//! Series tags (`{key: value}`) become CloudWatch dimensions, merged with the
//! global `dimensions` from the plugin config and the run's `run_id`.
//!
//! [Amazon CloudWatch]: https://aws.amazon.com/cloudwatch/

use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac as _};
use http_body_util::{BodyExt as _, Full};
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use once_cell::sync::OnceCell;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::runtime::Runtime;
use url::Url;

use loadr_plugin_api::abi::{
    FfiOutput, FfiOutputBox, FfiOutput_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "cloudwatch";
/// The AWS service the SigV4 signature is scoped to.
const SERVICE: &str = "monitoring";
/// The CloudWatch Query API version `PutMetricData` targets.
const API_VERSION: &str = "2010-08-01";
/// Wall-clock cap for a single `PutMetricData` POST.
const POST_TIMEOUT_MS: u64 = 10_000;
/// CloudWatch's hard cap on metric data points per `PutMetricData` call.
const MAX_BATCH: usize = 1000;
/// CloudWatch's hard cap on dimensions per metric.
const MAX_DIMENSIONS: usize = 30;

/// Trend aggregations projected as individual metrics, mapping the snapshot
/// `agg` field to the metric-name suffix CloudWatch sees. Mirrors how the
/// `prometheus` and `datadog` outputs shape trends.
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
// Crypto helpers (pure-Rust; no C deps).
// ---------------------------------------------------------------------------

/// Lowercase hex encoding.
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Hex-encoded SHA-256 of `data`.
fn hex_sha256(data: &[u8]) -> String {
    hex_lower(Sha256::digest(data).as_slice())
}

/// HMAC-SHA256(`key`, `data`) as raw bytes.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    // HMAC accepts a key of any length, so `new_from_slice` never errors here.
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// AWS Signature Version 4.
// ---------------------------------------------------------------------------

/// Everything needed to produce a SigV4 `Authorization` header for one request.
struct SigV4Params<'a> {
    method: &'a str,
    /// Already-encoded, `/`-preserving canonical path.
    canonical_uri: &'a str,
    /// Already-encoded, sorted canonical query string (may be empty).
    canonical_query: &'a str,
    /// Headers to sign: lowercase name + value. Order-insensitive (sorted here).
    headers: Vec<(String, String)>,
    /// Hex SHA-256 of the request payload.
    payload_hash: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    region: &'a str,
    service: &'a str,
    /// `YYYYMMDDTHHMMSSZ`.
    amz_date: &'a str,
    /// `YYYYMMDD`.
    date_stamp: &'a str,
}

/// The signing outputs. Intermediate strings are exposed for testing against
/// the published AWS Signature Version 4 vectors.
#[allow(dead_code)] // canonical_request/string_to_sign asserted in tests only
struct Signed {
    authorization: String,
    canonical_request: String,
    string_to_sign: String,
    signature: String,
}

/// Compute the SigV4 signature and `Authorization` header for `p`.
fn sign_request(p: &SigV4Params) -> Signed {
    let mut headers = p.headers.clone();
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = headers
        .iter()
        .map(|(k, v)| format!("{k}:{}\n", v.trim()))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{uri}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload}",
        method = p.method,
        uri = p.canonical_uri,
        query = p.canonical_query,
        payload = p.payload_hash,
    );

    let scope = format!(
        "{date}/{region}/{service}/aws4_request",
        date = p.date_stamp,
        region = p.region,
        service = p.service,
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hash}",
        amz_date = p.amz_date,
        hash = hex_sha256(canonical_request.as_bytes()),
    );

    let k_date = hmac_sha256(
        format!("AWS4{}", p.secret_key).as_bytes(),
        p.date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, p.region.as_bytes());
    let k_service = hmac_sha256(&k_region, p.service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex_lower(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        key = p.access_key,
    );

    Signed {
        authorization,
        canonical_request,
        string_to_sign,
        signature,
    }
}

/// AWS URI-encode `input`: unreserved characters pass through, everything else
/// is `%`-escaped (uppercase hex). `/` is preserved unless `encode_slash`.
fn uri_encode(input: &str, encode_slash: bool) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// Current UTC `(amz_date, date_stamp)` for the request signature.
fn timestamps() -> (String, String) {
    use time::macros::format_description;
    let now = time::OffsetDateTime::now_utc();
    let amz_date = now
        .format(&format_description!(
            "[year][month][day]T[hour][minute][second]Z"
        ))
        .unwrap_or_default();
    let date_stamp = now
        .format(&format_description!("[year][month][day]"))
        .unwrap_or_default();
    (amz_date, date_stamp)
}

/// ISO-8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`) for a Unix-millisecond instant, the
/// form CloudWatch accepts for a metric `Timestamp`.
fn iso8601_from_ms(ms: u64) -> String {
    use time::macros::format_description;
    let secs = (ms / 1000) as i64;
    time::OffsetDateTime::from_unix_timestamp(secs)
        .ok()
        .and_then(|dt| {
            dt.format(&format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second]Z"
            ))
            .ok()
        })
        .unwrap_or_default()
}

/// Current wall-clock time in Unix milliseconds.
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Config + credentials.
// ---------------------------------------------------------------------------

/// The resolved output configuration.
#[derive(Debug, Clone)]
struct Config {
    namespace: String,
    region: String,
    /// Explicit endpoint override (e.g. a VPC endpoint or LocalStack).
    endpoint: Option<String>,
    /// Global dimensions attached to every metric.
    dimensions: Vec<(String, String)>,
    batch_size: usize,
}

/// Parse `dimensions` as a `{key: value}` object into ordered pairs. Non-string
/// values are skipped so a stray number never produces a malformed dimension.
fn dimensions_from_config(v: Option<&Value>) -> Vec<(String, String)> {
    match v {
        Some(Value::Object(obj)) => obj
            .iter()
            .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
        _ => Vec::new(),
    }
}

impl Config {
    /// Parse and validate the config JSON handed to `start`. `env_region` is the
    /// `AWS_REGION` fallback used when the config omits `region`; it is passed in
    /// (rather than read here) so this stays a pure function for testing.
    fn from_json(cfg: &Value, env_region: Option<String>) -> Result<Config, String> {
        let region = cfg
            .get("region")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(env_region)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "cloudwatch output requires a `region` (config `region` or env AWS_REGION)"
                    .to_string()
            })?;
        let namespace = cfg
            .get("namespace")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("loadr")
            .to_string();
        let endpoint = cfg
            .get("endpoint")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let batch_size = cfg
            .get("batch_size")
            .and_then(Value::as_u64)
            .map(|n| (n as usize).clamp(1, MAX_BATCH))
            .unwrap_or(MAX_BATCH);
        Ok(Config {
            namespace,
            region,
            endpoint,
            dimensions: dimensions_from_config(cfg.get("dimensions")),
            batch_size,
        })
    }
}

/// Resolved AWS credentials.
#[derive(Debug, Clone)]
struct Creds {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

/// Resolve credentials from the standard AWS environment variables. Empty
/// values are treated as absent (so an unset `AWS_SESSION_TOKEN` does not sign a
/// bogus security-token header).
fn resolve_credentials() -> Result<Creds, String> {
    fn env(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|s| !s.is_empty())
    }
    let access_key = env("AWS_ACCESS_KEY_ID")
        .ok_or_else(|| "missing AWS credentials (env AWS_ACCESS_KEY_ID)".to_string())?;
    let secret_key = env("AWS_SECRET_ACCESS_KEY")
        .ok_or_else(|| "missing AWS credentials (env AWS_SECRET_ACCESS_KEY)".to_string())?;
    let session_token = env("AWS_SESSION_TOKEN");
    Ok(Creds {
        access_key,
        secret_key,
        session_token,
    })
}

// ---------------------------------------------------------------------------
// Endpoint resolution.
// ---------------------------------------------------------------------------

/// A resolved `PutMetricData` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Endpoint {
    /// Full request URL.
    url: String,
    /// `Host` header value (host[:port]) as signed.
    host: String,
}

/// Resolve the monitoring endpoint: an explicit `endpoint` override wins, else
/// `https://monitoring.<region>.amazonaws.com/`.
fn resolve_endpoint(config: &Config) -> Result<Endpoint, String> {
    if let Some(endpoint) = &config.endpoint {
        let eu =
            Url::parse(endpoint).map_err(|e| format!("invalid `endpoint` `{endpoint}`: {e}"))?;
        let ehost_only = eu
            .host_str()
            .ok_or_else(|| format!("endpoint `{endpoint}` has no host"))?;
        let host = match eu.port() {
            Some(p) => format!("{ehost_only}:{p}"),
            None => ehost_only.to_string(),
        };
        let url = format!("{}://{host}/", eu.scheme());
        return Ok(Endpoint { url, host });
    }
    let host = format!("monitoring.{}.amazonaws.com", config.region);
    let url = format!("https://{host}/");
    Ok(Endpoint { url, host })
}

// ---------------------------------------------------------------------------
// Snapshot -> CloudWatch metric data mapping.
// ---------------------------------------------------------------------------

/// One CloudWatch metric data point.
#[derive(Debug, Clone, PartialEq)]
struct MetricDatum {
    name: String,
    value: f64,
    /// CloudWatch unit (`Count` for counters, `None` for everything else).
    unit: &'static str,
    /// ISO-8601 UTC timestamp.
    timestamp: String,
    dimensions: Vec<(String, String)>,
}

/// Finite numeric field lookup — non-finite values are rejected so a stray
/// NaN/Inf never produces a metric CloudWatch would reject.
fn num(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(Value::as_f64).filter(|f| f.is_finite())
}

/// Turn one snapshot (as JSON) into a flat list of CloudWatch metric data.
/// `base_dims` (config dimensions + `run_id`) are attached to every metric,
/// merged with each series' own tags and capped at CloudWatch's dimension
/// limit. Never panics: a missing/oddly-shaped snapshot yields no metrics.
fn data_from_snapshot(snapshot: &Value, base_dims: &[(String, String)]) -> Vec<MetricDatum> {
    let timestamp = match snapshot.get("timestamp_ms").and_then(Value::as_u64) {
        Some(ms) => iso8601_from_ms(ms),
        None => iso8601_from_ms(now_unix_ms()),
    };

    let Some(entries) = snapshot.get("series").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for entry in entries {
        let Some(name) = entry.get("metric").and_then(Value::as_str) else {
            continue;
        };
        let kind = entry.get("kind").and_then(Value::as_str).unwrap_or("gauge");

        let mut dims = base_dims.to_vec();
        if let Some(obj) = entry.get("tags").and_then(Value::as_object) {
            for (k, v) in obj {
                if let Some(vs) = v.as_str() {
                    dims.push((k.clone(), vs.to_string()));
                }
            }
        }
        dims.truncate(MAX_DIMENSIONS);

        let agg = entry.get("agg");
        let push = |out: &mut Vec<MetricDatum>, name: String, value: f64, unit: &'static str| {
            out.push(MetricDatum {
                name,
                value,
                unit,
                timestamp: timestamp.clone(),
                dimensions: dims.clone(),
            });
        };

        match kind {
            "counter" => {
                if let Some(v) = num(entry, "interval_sum") {
                    push(&mut out, name.to_string(), v, "Count");
                }
            }
            "rate" => {
                let v = agg.and_then(|a| num(a, "rate")).unwrap_or(0.0);
                push(&mut out, name.to_string(), v, "None");
            }
            "trend" => {
                for (key, suffix) in TREND_AGGS {
                    if let Some(v) = agg.and_then(|a| num(a, key)) {
                        push(&mut out, format!("{name}.{suffix}"), v, "None");
                    }
                }
            }
            // "gauge" and any unknown kind: report the last value.
            _ => {
                let v = agg.and_then(|a| num(a, "last")).unwrap_or(0.0);
                push(&mut out, name.to_string(), v, "None");
            }
        }
    }
    out
}

/// The `run_id` dimension for a snapshot, if it carries one.
fn run_id_dimension(snapshot: &Value) -> Option<(String, String)> {
    snapshot
        .get("run_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| ("run_id".to_string(), s.to_string()))
}

// ---------------------------------------------------------------------------
// PutMetricData request body (AWS Query form encoding).
// ---------------------------------------------------------------------------

/// Format a metric value for the Query body. Whole numbers render without a
/// trailing `.0` (`10`, not `10.0`), matching how CloudWatch echoes values.
fn format_value(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        (v as i64).to_string()
    } else {
        v.to_string()
    }
}

/// Build the `application/x-www-form-urlencoded` `PutMetricData` body for one
/// batch of metric data under `namespace`.
fn build_body(namespace: &str, data: &[MetricDatum]) -> String {
    let mut params: Vec<(String, String)> = vec![
        ("Action".to_string(), "PutMetricData".to_string()),
        ("Namespace".to_string(), namespace.to_string()),
        ("Version".to_string(), API_VERSION.to_string()),
    ];
    for (i, d) in data.iter().enumerate() {
        let m = i + 1;
        params.push((format!("MetricData.member.{m}.MetricName"), d.name.clone()));
        params.push((
            format!("MetricData.member.{m}.Timestamp"),
            d.timestamp.clone(),
        ));
        params.push((format!("MetricData.member.{m}.Unit"), d.unit.to_string()));
        params.push((
            format!("MetricData.member.{m}.Value"),
            format_value(d.value),
        ));
        for (j, (dn, dv)) in d.dimensions.iter().enumerate() {
            let n = j + 1;
            params.push((
                format!("MetricData.member.{m}.Dimensions.member.{n}.Name"),
                dn.clone(),
            ));
            params.push((
                format!("MetricData.member.{m}.Dimensions.member.{n}.Value"),
                dv.clone(),
            ));
        }
    }
    params
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
        .collect::<Vec<_>>()
        .join("&")
}

// ---------------------------------------------------------------------------
// Send outcome classification.
// ---------------------------------------------------------------------------

/// What one `PutMetricData` exchange amounted to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Sent,
    Throttled,
    Failed,
}

/// Classify a completed HTTP exchange. CloudWatch signals rate limiting either
/// as `429` or as a `400` carrying a `Throttling` error code in the body.
fn classify(status: u16, body: &str) -> Outcome {
    if (200..300).contains(&status) {
        Outcome::Sent
    } else if status == 429 || (status == 400 && body.contains("Throttling")) {
        Outcome::Throttled
    } else {
        Outcome::Failed
    }
}

// ---------------------------------------------------------------------------
// Send transport — a seam so `on_snapshot` can be unit-tested without a socket.
// ---------------------------------------------------------------------------

/// Performs one signed POST. `Ok((status, body))` is a completed HTTP exchange
/// (any status, body captured for throttle detection); `Err` is a transport
/// failure.
trait Sender: Send {
    fn send(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<(u16, String), String>;
}

/// The real hyper + hyper-rustls sender.
struct HyperSender;

impl Sender for HyperSender {
    fn send(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<(u16, String), String> {
        runtime().block_on(http_post(url, headers, body, POST_TIMEOUT_MS))
    }
}

/// The single Tokio runtime the plugin uses to drive the per-snapshot POSTs.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build cloudwatch plugin tokio runtime")
    })
}

/// POST `body` to `url` with the given headers, returning `(status, body)`.
async fn http_post(
    url: &str,
    headers: &[(String, String)],
    body: Vec<u8>,
    timeout_ms: u64,
) -> Result<(u16, String), String> {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // also allows a plaintext endpoint override (e.g. a local LocalStack).
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build(tls);

    let mut builder = Request::builder().method("POST").uri(url);
    for (k, v) in headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    let request = builder
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| format!("building request failed: {e}"))?;

    let send = client.request(request);
    let resp = if timeout_ms == 0 {
        send.await
            .map_err(|e| format!("request to {url} failed: {e}"))?
    } else {
        tokio::time::timeout(Duration::from_millis(timeout_ms), send)
            .await
            .map_err(|_| format!("request to {url} timed out after {timeout_ms}ms"))?
            .map_err(|e| format!("request to {url} failed: {e}"))?
    };
    let status = resp.status().as_u16();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("reading response body failed: {e}"))?
        .to_bytes();
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

// ---------------------------------------------------------------------------
// The output plugin.
// ---------------------------------------------------------------------------

struct CloudWatch {
    config: Option<Config>,
    creds: Option<Creds>,
    sender: Box<dyn Sender>,
    /// Metric data points accepted by `PutMetricData` (`cloudwatch_metrics_sent`).
    metrics_sent: u64,
    /// Requests rejected with `Throttling`/`429` (`cloudwatch_throttles`).
    throttles: u64,
    /// Requests that failed for any other reason.
    failures: u64,
}

impl Default for CloudWatch {
    fn default() -> Self {
        CloudWatch {
            config: None,
            creds: None,
            sender: Box::new(HyperSender),
            metrics_sent: 0,
            throttles: 0,
            failures: 0,
        }
    }
}

impl CloudWatch {
    /// Sign and POST one metric-data batch. Updates the health counters from the
    /// outcome. Pure of the network otherwise, so it is exercised in tests
    /// through a mock [`Sender`].
    fn put_batch(
        &mut self,
        config: &Config,
        creds: &Creds,
        endpoint: &Endpoint,
        chunk: &[MetricDatum],
    ) {
        let body = build_body(&config.namespace, chunk);
        let (amz_date, date_stamp) = timestamps();
        let payload_hash = hex_sha256(body.as_bytes());

        let mut headers = vec![
            (
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            ),
            ("host".to_string(), endpoint.host.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(token) = &creds.session_token {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }

        let signed = sign_request(&SigV4Params {
            method: "POST",
            canonical_uri: "/",
            canonical_query: "",
            headers: headers.clone(),
            payload_hash: &payload_hash,
            access_key: &creds.access_key,
            secret_key: &creds.secret_key,
            region: &config.region,
            service: SERVICE,
            amz_date: &amz_date,
            date_stamp: &date_stamp,
        });

        let mut request_headers = headers;
        request_headers.push(("authorization".to_string(), signed.authorization));

        match self
            .sender
            .send(&endpoint.url, &request_headers, body.into_bytes())
        {
            Ok((status, resp_body)) => match classify(status, &resp_body) {
                Outcome::Sent => self.metrics_sent += chunk.len() as u64,
                Outcome::Throttled => {
                    self.throttles += 1;
                    eprintln!(
                        "cloudwatch output: PutMetricData throttled ({} metrics dropped)",
                        chunk.len()
                    );
                }
                Outcome::Failed => {
                    self.failures += 1;
                    eprintln!("cloudwatch output: PutMetricData returned HTTP {status}");
                }
            },
            Err(e) => {
                self.failures += 1;
                eprintln!("cloudwatch output: PutMetricData request failed: {e}");
            }
        }
    }

    /// Publish one snapshot's metrics, splitting them across `PutMetricData`
    /// calls that respect the batch limit. Returns an error only for a
    /// programmer-level problem (called before a successful `start`); per-request
    /// send failures are counted and logged, never propagated, so a transient
    /// CloudWatch error does not fail the load test.
    fn publish(&mut self, snapshot: &Value) -> Result<(), String> {
        let config = self
            .config
            .clone()
            .ok_or_else(|| "cloudwatch: snapshot received before a successful start".to_string())?;
        let creds = self
            .creds
            .clone()
            .ok_or_else(|| "cloudwatch: credentials were not resolved".to_string())?;

        let mut base_dims = config.dimensions.clone();
        if let Some(run_id) = run_id_dimension(snapshot) {
            base_dims.push(run_id);
        }

        let data = data_from_snapshot(snapshot, &base_dims);
        if data.is_empty() {
            return Ok(());
        }

        let endpoint = resolve_endpoint(&config)?;
        for chunk in data.chunks(config.batch_size) {
            self.put_batch(&config, &creds, &endpoint, chunk);
        }
        Ok(())
    }
}

impl FfiOutput for CloudWatch {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<(), RString> {
        let cfg: Value = match serde_json::from_str(config_json.as_str()) {
            Ok(v) => v,
            Err(e) => return RErr(RString::from(format!("invalid config JSON: {e}"))),
        };
        // Validate the config first, so a bad target fails regardless of whatever
        // AWS credentials happen to be in the environment.
        let config = match Config::from_json(&cfg, std::env::var("AWS_REGION").ok()) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(e)),
        };
        let creds = match resolve_credentials() {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(e)),
        };
        self.config = Some(config);
        self.creds = Some(creds);
        ROk(())
    }

    fn on_samples(&mut self, _samples_json: RString) {}

    fn on_snapshot(&mut self, snapshot_json: RString) {
        let Ok(value) = serde_json::from_str::<Value>(snapshot_json.as_str()) else {
            return;
        };
        if let Err(e) = self.publish(&value) {
            eprintln!("cloudwatch output: {e}");
        }
    }

    fn finish(&mut self, _summary_json: RString) {
        // Snapshots are published live, so there is nothing buffered to flush.
        // Surface the run's export health so a persistent problem is visible.
        if self.throttles > 0 || self.failures > 0 {
            eprintln!(
                "cloudwatch output: run finished — {} metrics sent, {} throttled, {} failed",
                self.metrics_sent, self.throttles, self.failures
            );
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "output",
            "description": "Batches snapshot series into AWS CloudWatch PutMetricData over SigV4-signed HTTPS",
        })
        .to_string(),
    )
}

extern "C" fn make_output() -> FfiOutputBox {
    FfiOutput_TO::from_value(CloudWatch::default(), abi_stable::erased_types::TD_Opaque)
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
// Tests — all offline. The SigV4 signer is checked against a published AWS
// vector and the send path is driven through a scripted mock sender, never a
// real socket.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    #![allow(clippy::field_reassign_with_default)]
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    // -- SigV4 against a published AWS vector --------------------------------

    /// The `get-vanilla` case from the awslabs `aws-sig-v4-test-suite`: a
    /// non-S3 service signature, exactly the algorithm CloudWatch uses.
    #[test]
    fn sigv4_matches_get_vanilla_vector() {
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let signed = sign_request(&SigV4Params {
            method: "GET",
            canonical_uri: "/",
            canonical_query: "",
            headers: vec![
                ("host".into(), "example.amazonaws.com".into()),
                ("x-amz-date".into(), "20150830T123600Z".into()),
            ],
            payload_hash: empty,
            access_key: "AKIDEXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
            service: "service",
            amz_date: "20150830T123600Z",
            date_stamp: "20150830",
        });

        let expected_cr = "GET\n\
             /\n\
             \n\
             host:example.amazonaws.com\n\
             x-amz-date:20150830T123600Z\n\
             \n\
             host;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(signed.canonical_request, expected_cr);
        assert_eq!(
            signed.signature,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn empty_payload_hash_is_the_known_constant() {
        assert_eq!(
            hex_sha256(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn uri_encode_rules() {
        assert_eq!(uri_encode("http_reqs", true), "http_reqs");
        assert_eq!(uri_encode("a b", true), "a%20b");
        assert_eq!(uri_encode("k~e.y-1_2", true), "k~e.y-1_2");
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(uri_encode("a/b", false), "a/b");
    }

    // -- config parsing -----------------------------------------------------

    #[test]
    fn config_requires_region_from_config_or_env() {
        assert!(Config::from_json(&json!({}), None).is_err());
        assert!(Config::from_json(&json!({}), Some(String::new())).is_err());
        let c = Config::from_json(&json!({}), Some("eu-west-2".to_string())).unwrap();
        assert_eq!(c.region, "eu-west-2");
        // Explicit config region wins over the env fallback.
        let c = Config::from_json(
            &json!({ "region": "us-east-1" }),
            Some("eu-west-2".to_string()),
        )
        .unwrap();
        assert_eq!(c.region, "us-east-1");
    }

    #[test]
    fn config_defaults_and_overrides() {
        let c = Config::from_json(&json!({ "region": "us-east-1" }), None).unwrap();
        assert_eq!(c.namespace, "loadr");
        assert_eq!(c.batch_size, MAX_BATCH);
        assert!(c.endpoint.is_none());
        assert!(c.dimensions.is_empty());

        let c = Config::from_json(
            &json!({
                "region": "eu-west-2",
                "namespace": "loadr/checkout",
                "batch_size": 50,
                "endpoint": "http://localhost:4566",
                "dimensions": { "env": "staging", "service": "checkout" },
            }),
            None,
        )
        .unwrap();
        assert_eq!(c.namespace, "loadr/checkout");
        assert_eq!(c.batch_size, 50);
        assert_eq!(c.endpoint.as_deref(), Some("http://localhost:4566"));
        // Object ordering is deterministic (serde_json preserves insertion here
        // via a BTreeMap-backed map only when `preserve_order` is off; assert as
        // a set to stay robust).
        assert!(c
            .dimensions
            .contains(&("env".to_string(), "staging".to_string())));
        assert!(c
            .dimensions
            .contains(&("service".to_string(), "checkout".to_string())));
    }

    #[test]
    fn batch_size_is_clamped_to_the_cloudwatch_limit() {
        let c = Config::from_json(&json!({ "region": "r", "batch_size": 0 }), None).unwrap();
        assert_eq!(c.batch_size, 1);
        let c = Config::from_json(&json!({ "region": "r", "batch_size": 99999 }), None).unwrap();
        assert_eq!(c.batch_size, MAX_BATCH);
    }

    // -- endpoint resolution ------------------------------------------------

    fn config_with_region(region: &str) -> Config {
        Config {
            namespace: "loadr".to_string(),
            region: region.to_string(),
            endpoint: None,
            dimensions: Vec::new(),
            batch_size: MAX_BATCH,
        }
    }

    #[test]
    fn endpoint_defaults_to_regional_monitoring_host() {
        let e = resolve_endpoint(&config_with_region("eu-west-2")).unwrap();
        assert_eq!(e.host, "monitoring.eu-west-2.amazonaws.com");
        assert_eq!(e.url, "https://monitoring.eu-west-2.amazonaws.com/");
    }

    #[test]
    fn endpoint_override_is_used() {
        let mut config = config_with_region("us-east-1");
        config.endpoint = Some("http://localhost:4566".to_string());
        let e = resolve_endpoint(&config).unwrap();
        assert_eq!(e.host, "localhost:4566");
        assert_eq!(e.url, "http://localhost:4566/");
    }

    // -- snapshot -> metric data mapping ------------------------------------

    fn find<'a>(data: &'a [MetricDatum], name: &str) -> Option<&'a MetricDatum> {
        data.iter().find(|d| d.name == name)
    }

    #[test]
    fn snapshot_maps_to_metric_data() {
        let snap = json!({
            "timestamp_ms": 1_000_000u64, // -> 1970-01-01T00:16:40Z
            "series": [
                {
                    "metric": "http_reqs",
                    "kind": "counter",
                    "tags": { "method": "GET" },
                    "interval_sum": 10.0,
                    "agg": { "count": 100 }
                },
                { "metric": "vus", "kind": "gauge", "tags": {}, "agg": { "last": 25.0 } },
                { "metric": "checks", "kind": "rate", "tags": {}, "agg": { "rate": 0.99 } },
                {
                    "metric": "http_req_duration",
                    "kind": "trend",
                    "tags": { "method": "GET" },
                    "agg": { "avg": 12.0, "p95": 30.0, "max": 50.0 }
                }
            ]
        });
        let base = vec![("env".to_string(), "test".to_string())];
        let data = data_from_snapshot(&snap, &base);

        let c = find(&data, "http_reqs").expect("counter datum");
        assert_eq!(c.unit, "Count");
        assert_eq!(c.value, 10.0);
        assert_eq!(c.timestamp, "1970-01-01T00:16:40Z");
        assert!(c
            .dimensions
            .contains(&("env".to_string(), "test".to_string())));
        assert!(c
            .dimensions
            .contains(&("method".to_string(), "GET".to_string())));

        let g = find(&data, "vus").expect("gauge datum");
        assert_eq!(g.unit, "None");
        assert_eq!(g.value, 25.0);

        let r = find(&data, "checks").expect("rate datum");
        assert_eq!(r.value, 0.99);

        assert_eq!(find(&data, "http_req_duration.avg").unwrap().value, 12.0);
        assert_eq!(find(&data, "http_req_duration.p95").unwrap().value, 30.0);
        assert_eq!(find(&data, "http_req_duration.max").unwrap().value, 50.0);
        // No p99 was present, so no p99 metric.
        assert!(find(&data, "http_req_duration.p99").is_none());

        // 1 counter + 1 gauge + 1 rate + 3 trend = 6 metric data points.
        assert_eq!(data.len(), 6);
    }

    #[test]
    fn non_finite_trend_values_are_dropped() {
        let snap = json!({
            "timestamp_ms": 2000u64,
            "series": [ { "metric": "t", "kind": "trend", "agg": { "avg": null } } ]
        });
        assert!(data_from_snapshot(&snap, &[]).is_empty());
    }

    #[test]
    fn empty_or_malformed_snapshot_yields_no_data() {
        assert!(data_from_snapshot(&json!({}), &[]).is_empty());
        assert!(data_from_snapshot(&json!({ "series": [] }), &[]).is_empty());
        // A series entry missing its metric name is skipped, not fatal.
        assert!(data_from_snapshot(&json!({ "series": [ { "kind": "gauge" } ] }), &[]).is_empty());
    }

    #[test]
    fn dimensions_are_capped_at_the_cloudwatch_limit() {
        let mut tags = serde_json::Map::new();
        for i in 0..40 {
            tags.insert(format!("k{i}"), json!(format!("v{i}")));
        }
        let snap = json!({
            "timestamp_ms": 1000u64,
            "series": [ { "metric": "g", "kind": "gauge", "tags": tags, "agg": { "last": 1.0 } } ]
        });
        let data = data_from_snapshot(&snap, &[]);
        assert_eq!(data[0].dimensions.len(), MAX_DIMENSIONS);
    }

    #[test]
    fn run_id_dimension_is_extracted_when_present() {
        assert_eq!(
            run_id_dimension(&json!({ "run_id": "abc" })),
            Some(("run_id".to_string(), "abc".to_string()))
        );
        assert!(run_id_dimension(&json!({ "run_id": "" })).is_none());
        assert!(run_id_dimension(&json!({})).is_none());
    }

    // -- PutMetricData body -------------------------------------------------

    #[test]
    fn build_body_encodes_query_parameters() {
        let data = vec![MetricDatum {
            name: "http_reqs".to_string(),
            value: 10.0,
            unit: "Count",
            timestamp: "2020-01-01T00:00:00Z".to_string(),
            dimensions: vec![("env".to_string(), "stag ing".to_string())],
        }];
        let body = build_body("loadr", &data);
        assert!(body.contains("Action=PutMetricData"));
        assert!(body.contains("Version=2010-08-01"));
        assert!(body.contains("Namespace=loadr"));
        assert!(body.contains("MetricData.member.1.MetricName=http_reqs"));
        assert!(body.contains("MetricData.member.1.Unit=Count"));
        assert!(body.contains("MetricData.member.1.Value=10"));
        // ISO-8601 colons and the space in the dimension value are percent-encoded.
        assert!(body.contains("MetricData.member.1.Timestamp=2020-01-01T00%3A00%3A00Z"));
        assert!(body.contains("MetricData.member.1.Dimensions.member.1.Name=env"));
        assert!(body.contains("MetricData.member.1.Dimensions.member.1.Value=stag%20ing"));
    }

    #[test]
    fn format_value_renders_whole_numbers_without_a_fraction() {
        assert_eq!(format_value(10.0), "10");
        assert_eq!(format_value(0.99), "0.99");
        assert_eq!(format_value(12.5), "12.5");
    }

    // -- outcome classification ---------------------------------------------

    #[test]
    fn classify_maps_status_and_body() {
        assert_eq!(classify(200, ""), Outcome::Sent);
        assert_eq!(classify(204, ""), Outcome::Sent);
        assert_eq!(classify(429, ""), Outcome::Throttled);
        assert_eq!(
            classify(400, "<Error><Code>Throttling</Code></Error>"),
            Outcome::Throttled
        );
        assert_eq!(classify(400, "malformed input"), Outcome::Failed);
        assert_eq!(classify(403, ""), Outcome::Failed);
        assert_eq!(classify(500, ""), Outcome::Failed);
    }

    // -- publish path through a mock sender (no network) ---------------------

    /// A recorded POST.
    #[derive(Debug, Clone)]
    struct PostCall {
        url: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    /// A scripted sender: records every POST and returns a fixed `(status, body)`.
    struct MockSender {
        status: u16,
        body: String,
        calls: Arc<Mutex<Vec<PostCall>>>,
    }

    impl Sender for MockSender {
        fn send(
            &self,
            url: &str,
            headers: &[(String, String)],
            body: Vec<u8>,
        ) -> Result<(u16, String), String> {
            self.calls.lock().unwrap().push(PostCall {
                url: url.to_string(),
                headers: headers.to_vec(),
                body,
            });
            Ok((self.status, self.body.clone()))
        }
    }

    fn creds() -> Creds {
        Creds {
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        }
    }

    /// Build a plugin wired to a mock sender, returning it plus a handle to the
    /// recorded calls. (Not named `new` on purpose.)
    fn cloudwatch_with(
        config: Config,
        status: u16,
        body: &str,
    ) -> (CloudWatch, Arc<Mutex<Vec<PostCall>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender = MockSender {
            status,
            body: body.to_string(),
            calls: calls.clone(),
        };
        let cw = CloudWatch {
            config: Some(config),
            creds: Some(creds()),
            sender: Box::new(sender),
            metrics_sent: 0,
            throttles: 0,
            failures: 0,
        };
        (cw, calls)
    }

    fn one_gauge_snapshot() -> Value {
        json!({
            "timestamp_ms": 1000u64,
            "run_id": "run-xyz",
            "series": [ { "metric": "vus", "kind": "gauge", "agg": { "last": 3.0 } } ]
        })
    }

    #[test]
    fn publish_signs_and_posts() {
        let (mut cw, calls) = cloudwatch_with(config_with_region("eu-west-2"), 200, "");
        cw.publish(&one_gauge_snapshot()).unwrap();

        assert_eq!(cw.metrics_sent, 1);
        assert_eq!(cw.throttles, 0);
        assert_eq!(cw.failures, 0);

        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        let call = &recorded[0];
        assert_eq!(call.url, "https://monitoring.eu-west-2.amazonaws.com/");

        let header = |name: &str| {
            call.headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(
            header("content-type").as_deref(),
            Some("application/x-www-form-urlencoded")
        );
        assert_eq!(
            header("host").as_deref(),
            Some("monitoring.eu-west-2.amazonaws.com")
        );
        assert!(header("x-amz-date").is_some());
        let auth = header("authorization").expect("authorization header");
        assert!(
            auth.contains("/eu-west-2/monitoring/aws4_request"),
            "{auth}"
        );
        assert!(
            auth.contains("SignedHeaders=content-type;host;x-amz-date"),
            "{auth}"
        );

        // The signed body carries the run_id dimension and the gauge value.
        let body = String::from_utf8(call.body.clone()).unwrap();
        assert!(body.contains("MetricData.member.1.MetricName=vus"));
        assert!(body.contains("Dimensions.member.1.Name=run_id"));
        assert!(body.contains("Dimensions.member.1.Value=run-xyz"));
    }

    #[test]
    fn publish_splits_batches_over_the_limit() {
        let mut config = config_with_region("us-east-1");
        config.batch_size = 1;
        let (mut cw, calls) = cloudwatch_with(config, 200, "");
        let snap = json!({
            "timestamp_ms": 1000u64,
            "series": [
                { "metric": "a", "kind": "gauge", "agg": { "last": 1.0 } },
                { "metric": "b", "kind": "gauge", "agg": { "last": 2.0 } }
            ]
        });
        cw.publish(&snap).unwrap();
        // Two data points, batch_size 1 => two PutMetricData calls.
        assert_eq!(calls.lock().unwrap().len(), 2);
        assert_eq!(cw.metrics_sent, 2);
    }

    #[test]
    fn throttle_is_counted_and_not_a_failure() {
        let (mut cw, _calls) = cloudwatch_with(config_with_region("us-east-1"), 429, "");
        cw.publish(&one_gauge_snapshot()).unwrap();
        assert_eq!(cw.throttles, 1);
        assert_eq!(cw.metrics_sent, 0);
        assert_eq!(cw.failures, 0);

        let (mut cw, _calls) = cloudwatch_with(
            config_with_region("us-east-1"),
            400,
            "<Error><Code>Throttling</Code></Error>",
        );
        cw.publish(&one_gauge_snapshot()).unwrap();
        assert_eq!(cw.throttles, 1);
    }

    #[test]
    fn server_error_is_counted_as_a_failure() {
        let (mut cw, _calls) = cloudwatch_with(config_with_region("us-east-1"), 500, "");
        cw.publish(&one_gauge_snapshot()).unwrap();
        assert_eq!(cw.failures, 1);
        assert_eq!(cw.metrics_sent, 0);
    }

    #[test]
    fn session_token_is_signed_when_present() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut cw = CloudWatch {
            config: Some(config_with_region("us-east-1")),
            creds: Some(Creds {
                access_key: "AKIA".into(),
                secret_key: "secret".into(),
                session_token: Some("TOKEN123".into()),
            }),
            sender: Box::new(MockSender {
                status: 200,
                body: String::new(),
                calls: calls.clone(),
            }),
            metrics_sent: 0,
            throttles: 0,
            failures: 0,
        };
        cw.publish(&one_gauge_snapshot()).unwrap();
        let recorded = calls.lock().unwrap();
        let call = &recorded[0];
        assert!(call
            .headers
            .iter()
            .any(|(k, v)| k == "x-amz-security-token" && v == "TOKEN123"));
        let auth = call
            .headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .map(|(_, v)| v.clone())
            .unwrap();
        assert!(auth.contains("x-amz-security-token"), "{auth}");
    }

    #[test]
    fn empty_snapshot_makes_no_request() {
        let (mut cw, calls) = cloudwatch_with(config_with_region("us-east-1"), 200, "");
        cw.publish(&json!({ "series": [] })).unwrap();
        assert!(calls.lock().unwrap().is_empty());
        assert_eq!(cw.metrics_sent, 0);
    }

    #[test]
    fn publish_without_start_errors() {
        let mut cw = CloudWatch::default();
        let err = cw.publish(&one_gauge_snapshot()).unwrap_err();
        assert!(err.contains("start"), "{err}");
    }

    // -- output lifecycle ----------------------------------------------------

    #[test]
    fn start_rejects_invalid_config_before_touching_the_network() {
        let mut out = CloudWatch::default();
        assert!(matches!(out.start(RString::from("not json")), RErr(_)));
        assert!(out.config.is_none());
    }

    #[test]
    fn on_snapshot_is_robust_to_bad_input() {
        // No config resolved: publish errors, on_snapshot swallows it (no panic).
        let mut out = CloudWatch::default();
        out.on_snapshot(RString::from("not json at all"));
        out.on_snapshot(RString::from("{\"series\":[]}"));
        out.on_snapshot(RString::from(one_gauge_snapshot().to_string()));
        out.on_samples(RString::from("[]"));
        out.finish(RString::from("{}"));
        assert_eq!(out.metrics_sent, 0);
    }

    #[test]
    fn info_declares_output_kind() {
        let out = CloudWatch::default();
        assert_eq!(out.name().as_str(), NAME);
        let v: Value = serde_json::from_str(plugin_info().as_str()).unwrap();
        assert_eq!(v["kind"], "output");
        assert_eq!(v["name"], "cloudwatch");
    }
}
