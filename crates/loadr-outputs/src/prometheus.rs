//! Prometheus output: a scrape endpoint (text exposition format) and/or
//! remote-write push (snappy-compressed protobuf), both rendered from the
//! latest snapshot.
//!
//! Naming (metric and label names sanitized to `[a-zA-Z0-9_:]` / `[a-zA-Z0-9_]`):
//! - counter `m` → `loadr_m_total` = sum
//! - gauge `m` → `loadr_m` = last value
//! - rate `m` → `loadr_m_rate` (pass fraction) plus `loadr_m_passes_total` and
//!   `loadr_m_count_total`
//! - trend `m` → summary `loadr_m_milliseconds{quantile="0.5|0.9|0.95|0.99"}`
//!   plus `loadr_m_milliseconds_sum` / `loadr_m_milliseconds_count`

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderName, HeaderValue, Uri};
use http_body_util::Full;
use loadr_core::aggregate::{SeriesSnapshot, Snapshot};
use loadr_core::error::EngineError;
use loadr_core::metrics::{MetricKind, Tags};
use loadr_core::output::Output;
use loadr_core::summary::Summary;
use parking_lot::RwLock;
use prost::Message as _;

use crate::http_client;
use crate::proto::prometheus as prompb;

type Shared = Arc<RwLock<Option<Snapshot>>>;

/// Prometheus exporter: scrape endpoint and/or remote-write push.
pub struct PrometheusOutput {
    listen: Option<String>,
    remote_write_url: Option<String>,
    interval: Duration,
    final_scrape_grace: Duration,
    latest: Shared,
    bound_addr: Option<SocketAddr>,
    server_task: Option<tokio::task::JoinHandle<()>>,
    push_task: Option<tokio::task::JoinHandle<()>>,
}

impl PrometheusOutput {
    /// Create a Prometheus output. At least one of `listen` (scrape endpoint
    /// address, e.g. `127.0.0.1:9091`) or `remote_write_url` must be set;
    /// `interval` is the remote-write push period.
    pub fn new(
        listen: Option<String>,
        remote_write_url: Option<String>,
        interval: Duration,
    ) -> Self {
        PrometheusOutput {
            listen,
            remote_write_url,
            interval,
            final_scrape_grace: Duration::ZERO,
            latest: Arc::new(RwLock::new(None)),
            bound_addr: None,
            server_task: None,
            push_task: None,
        }
    }

    /// The address the scrape endpoint actually bound to (available after
    /// `start`; useful with `listen: 127.0.0.1:0`).
    pub fn bound_addr(&self) -> Option<SocketAddr> {
        self.bound_addr
    }

    /// Configure how long a standalone scrape listener remains available
    /// after the final snapshot is published, delaying `finish` by as much.
    /// Disabled (zero) by default: set it when a Prometheus server scrapes
    /// short-lived runs and must observe the terminal snapshot.
    pub fn with_final_scrape_grace(mut self, grace: Duration) -> Self {
        self.final_scrape_grace = grace;
        self
    }

    fn abort_server_task(&mut self) {
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
    }

    fn abort_push_task(&mut self) {
        if let Some(task) = self.push_task.take() {
            task.abort();
        }
    }

    fn abort_tasks(&mut self) {
        self.abort_server_task();
        self.abort_push_task();
    }
}

impl Drop for PrometheusOutput {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

#[async_trait]
impl Output for PrometheusOutput {
    fn name(&self) -> &str {
        "prometheus"
    }

    fn wants_samples(&self) -> bool {
        false
    }

    async fn start(&mut self) -> Result<(), EngineError> {
        if let Some(listen) = &self.listen {
            let listener = tokio::net::TcpListener::bind(listen).await.map_err(|err| {
                EngineError::Config(format!("prometheus listen `{listen}`: {err}"))
            })?;
            self.bound_addr = listener.local_addr().ok();
            let latest = self.latest.clone();
            self.server_task = Some(tokio::spawn(serve_scrape(listener, latest)));
        }
        if let Some(url) = &self.remote_write_url {
            let uri: Uri = url.parse().map_err(|err| {
                EngineError::Config(format!("prometheus remote_write_url `{url}`: {err}"))
            })?;
            let latest = self.latest.clone();
            let interval = self.interval;
            self.push_task = Some(tokio::spawn(async move {
                let client = http_client::client();
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    push_once(&client, &uri, &latest).await;
                }
            }));
        }
        Ok(())
    }

    async fn on_snapshot(&mut self, snapshot: &Snapshot) {
        *self.latest.write() = Some(snapshot.clone());
    }

    async fn finish(&mut self, summary: &Summary) {
        *self.latest.write() = Some(summary.snapshot.clone());
        self.abort_push_task();
        // One final push so short runs still export data.
        if let Some(url) = &self.remote_write_url {
            if let Ok(uri) = url.parse::<Uri>() {
                let client = http_client::client();
                push_once(&client, &uri, &self.latest).await;
            }
        }
        // A listen-mode exporter belongs to this process, so when a grace is
        // configured leave the final snapshot scrapeable before shutting the
        // listener down.
        if self.server_task.is_some() && !self.final_scrape_grace.is_zero() {
            tracing::info!(
                grace = ?self.final_scrape_grace,
                "keeping prometheus /metrics scrapeable (final_scrape_grace)"
            );
            tokio::time::sleep(self.final_scrape_grace).await;
        }
        self.abort_server_task();
    }
}

async fn serve_scrape(listener: tokio::net::TcpListener, latest: Shared) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(err) => {
                tracing::warn!(error = %err, "prometheus scrape accept failed");
                continue;
            }
        };
        let latest = latest.clone();
        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            let svc =
                hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                    let latest = latest.clone();
                    async move {
                        let response = if req.method() == http::Method::GET
                            && req.uri().path() == "/metrics"
                        {
                            let body = match &*latest.read() {
                                Some(snapshot) => render_exposition(snapshot),
                                None => String::new(),
                            };
                            http::Response::builder()
                                .status(http::StatusCode::OK)
                                .header(
                                    http::header::CONTENT_TYPE,
                                    "text/plain; version=0.0.4; charset=utf-8",
                                )
                                .body(Full::new(Bytes::from(body)))
                        } else {
                            http::Response::builder()
                                .status(http::StatusCode::NOT_FOUND)
                                .body(Full::new(Bytes::from("not found")))
                        };
                        response.map_err(|err| {
                            // Static response construction cannot fail in practice.
                            std::io::Error::other(err.to_string())
                        })
                    }
                });
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!(error = %err, "prometheus scrape connection error");
            }
        });
    }
}

async fn push_once(client: &http_client::HttpClient, uri: &Uri, latest: &Shared) {
    let snapshot = latest.read().clone();
    let Some(snapshot) = snapshot else {
        return;
    };
    let request = build_write_request(&snapshot);
    if request.timeseries.is_empty() {
        return;
    }
    let encoded = request.encode_to_vec();
    let compressed = match snap::raw::Encoder::new().compress_vec(&encoded) {
        Ok(c) => c,
        Err(err) => {
            tracing::warn!(error = %err, "prometheus remote-write snappy compression failed");
            return;
        }
    };
    let headers = [
        (
            http::header::CONTENT_ENCODING,
            HeaderValue::from_static("snappy"),
        ),
        (
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        ),
        (
            HeaderName::from_static("x-prometheus-remote-write-version"),
            HeaderValue::from_static("0.1.0"),
        ),
    ];
    match http_client::post(client, uri, &headers, compressed).await {
        Ok(status) if status.is_success() => {}
        Ok(status) => {
            tracing::warn!(%uri, %status, "prometheus remote-write rejected");
        }
        Err(err) => {
            tracing::warn!(%uri, error = %err, "prometheus remote-write failed");
        }
    }
}

/// One Prometheus sample derived from a series snapshot.
struct PromPoint {
    /// Metric family name (for `# TYPE` lines).
    family: String,
    /// Sample name (`family`, or `family_sum`/`family_count` for summaries).
    name: String,
    prom_type: &'static str,
    labels: Vec<(String, String)>,
    value: f64,
}

/// Sanitize a metric name to `[a-zA-Z0-9_:]`.
fn sanitize_metric(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Sanitize a label name to `[a-zA-Z_][a-zA-Z0-9_]*`.
fn sanitize_label(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

fn label_pairs(tags: &Tags) -> Vec<(String, String)> {
    tags.iter()
        .map(|(k, v)| (sanitize_label(k), v.clone()))
        .collect()
}

/// Expand one series snapshot into Prometheus points (shared by the scrape
/// renderer and remote-write).
fn prom_points(series: &SeriesSnapshot) -> Vec<PromPoint> {
    let base = format!("loadr_{}", sanitize_metric(&series.metric));
    let labels = label_pairs(&series.tags);
    let mut points = Vec::new();
    let mut push = |family: &str,
                    name: String,
                    prom_type: &'static str,
                    labels: Vec<(String, String)>,
                    value: f64| {
        points.push(PromPoint {
            family: family.to_string(),
            name,
            prom_type,
            labels,
            value,
        });
    };
    match series.kind {
        MetricKind::Counter => {
            let family = format!("{base}_total");
            push(&family, family.clone(), "counter", labels, series.agg.sum);
        }
        MetricKind::Gauge => {
            push(
                &base,
                base.clone(),
                "gauge",
                labels,
                series.agg.last.unwrap_or(0.0),
            );
        }
        MetricKind::Rate => {
            let rate = format!("{base}_rate");
            push(
                &rate,
                rate.clone(),
                "gauge",
                labels.clone(),
                series.agg.rate.unwrap_or(0.0),
            );
            let passes = format!("{base}_passes_total");
            push(
                &passes,
                passes.clone(),
                "counter",
                labels.clone(),
                series.agg.sum,
            );
            let count = format!("{base}_count_total");
            push(
                &count,
                count.clone(),
                "counter",
                labels,
                series.agg.count as f64,
            );
        }
        MetricKind::Trend => {
            let family = format!("{base}_milliseconds");
            for (q, v) in [
                ("0.5", series.agg.med),
                ("0.9", series.agg.p90),
                ("0.95", series.agg.p95),
                ("0.99", series.agg.p99),
            ] {
                if let Some(v) = v {
                    let mut ql = labels.clone();
                    ql.push(("quantile".to_string(), q.to_string()));
                    push(&family, family.clone(), "summary", ql, v);
                }
            }
            push(
                &family,
                format!("{family}_sum"),
                "summary",
                labels.clone(),
                series.agg.sum,
            );
            push(
                &family,
                format!("{family}_count"),
                "summary",
                labels,
                series.agg.count as f64,
            );
        }
    }
    points
}

fn fmt_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() { "+Inf" } else { "-Inf" }.to_string()
    } else {
        format!("{v}")
    }
}

fn escape_label_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Render the latest snapshot in the Prometheus text exposition format.
pub(crate) fn render_exposition(snapshot: &Snapshot) -> String {
    let mut out = String::new();
    let mut seen_families: HashSet<String> = HashSet::new();
    for series in &snapshot.series {
        for point in prom_points(series) {
            if seen_families.insert(point.family.clone()) {
                out.push_str("# TYPE ");
                out.push_str(&point.family);
                out.push(' ');
                out.push_str(point.prom_type);
                out.push('\n');
            }
            out.push_str(&point.name);
            if !point.labels.is_empty() {
                out.push('{');
                for (i, (k, v)) in point.labels.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(k);
                    out.push_str("=\"");
                    out.push_str(&escape_label_value(v));
                    out.push('"');
                }
                out.push('}');
            }
            out.push(' ');
            out.push_str(&fmt_value(point.value));
            out.push('\n');
        }
    }
    out
}

/// Build a remote-write `WriteRequest` from the latest snapshot.
pub(crate) fn build_write_request(snapshot: &Snapshot) -> prompb::WriteRequest {
    let timestamp = snapshot.timestamp_ms as i64;
    let mut timeseries = Vec::new();
    for series in &snapshot.series {
        for point in prom_points(series) {
            if !point.value.is_finite() {
                continue;
            }
            let mut labels: BTreeMap<String, String> = point.labels.into_iter().collect();
            labels.insert("__name__".to_string(), point.name);
            timeseries.push(prompb::TimeSeries {
                labels: labels
                    .into_iter()
                    .map(|(name, value)| prompb::Label { name, value })
                    .collect(),
                samples: vec![prompb::Sample {
                    value: point.value,
                    timestamp,
                }],
            });
        }
    }
    prompb::WriteRequest { timeseries }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{fixture_snapshot, fixture_summary, spawn_capture_server};
    use http_body_util::BodyExt as _;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scrape_endpoint_serves_exposition() {
        let mut out = PrometheusOutput::new(
            Some("127.0.0.1:0".to_string()),
            None,
            Duration::from_secs(5),
        );
        out.start().await.expect("start");
        let addr = out.bound_addr().expect("bound addr");
        out.on_snapshot(&fixture_snapshot()).await;

        let client = http_client::client();
        let uri: Uri = format!("http://{addr}/metrics").parse().expect("uri");
        let request = http::Request::builder()
            .uri(uri)
            .body(Full::new(Bytes::new()))
            .expect("request");
        let response = client.request(request).await.expect("scrape");
        assert_eq!(response.status(), http::StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).expect("utf8");

        assert!(
            text.contains("# TYPE loadr_http_reqs_total counter"),
            "{text}"
        );
        assert!(
            text.contains(r#"loadr_http_reqs_total{method="GET",status="200"} 5"#),
            "{text}"
        );
        assert!(
            text.contains("# TYPE loadr_http_req_duration_milliseconds summary"),
            "{text}"
        );
        assert!(text.contains(r#"quantile="0.95""#), "{text}");
        assert!(
            text.contains("loadr_http_req_duration_milliseconds_count"),
            "{text}"
        );
        assert!(text.contains("loadr_checks_rate"), "{text}");
        assert!(text.contains("loadr_checks_passes_total"), "{text}");
        assert!(text.contains("loadr_vus 7"), "{text}");

        out.finish(&fixture_summary()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn final_snapshot_remains_scrapeable_during_grace() {
        let grace = Duration::from_millis(120);
        let mut out = PrometheusOutput::new(
            Some("127.0.0.1:0".to_string()),
            None,
            Duration::from_secs(5),
        )
        .with_final_scrape_grace(grace);
        out.start().await.expect("start");
        let addr = out.bound_addr().expect("bound addr");
        out.on_snapshot(&fixture_snapshot()).await;

        let started = tokio::time::Instant::now();
        let finish = tokio::spawn(async move {
            out.finish(&fixture_summary()).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = http_client::client();
        let request = http::Request::builder()
            .uri(format!("http://{addr}/metrics"))
            .body(Full::new(Bytes::new()))
            .expect("request");
        let response = client.request(request).await.expect("final scrape");
        assert_eq!(response.status(), http::StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("loadr_vus"));

        finish.await.expect("finish task");
        assert!(started.elapsed() >= grace);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_write_pushes_snappy_protobuf() {
        let (addr, mut rx) = spawn_capture_server().await;
        let mut out = PrometheusOutput::new(
            None,
            Some(format!("http://{addr}/api/v1/write")),
            Duration::from_millis(50),
        );
        out.start().await.expect("start");
        out.on_snapshot(&fixture_snapshot()).await;

        let captured = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("push within timeout")
            .expect("captured request");
        assert_eq!(captured.method, "POST");
        assert_eq!(captured.path_and_query, "/api/v1/write");
        assert_eq!(
            captured
                .headers
                .get("content-encoding")
                .map(|v| v.as_bytes()),
            Some(b"snappy".as_ref())
        );
        assert_eq!(
            captured.headers.get("content-type").map(|v| v.as_bytes()),
            Some(b"application/x-protobuf".as_ref())
        );
        assert_eq!(
            captured
                .headers
                .get("x-prometheus-remote-write-version")
                .map(|v| v.as_bytes()),
            Some(b"0.1.0".as_ref())
        );

        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&captured.body)
            .expect("snappy decompress");
        let request = prompb::WriteRequest::decode(decompressed.as_slice()).expect("decode");
        assert!(!request.timeseries.is_empty());
        let names: Vec<&str> = request
            .timeseries
            .iter()
            .filter_map(|ts| {
                ts.labels
                    .iter()
                    .find(|l| l.name == "__name__")
                    .map(|l| l.value.as_str())
            })
            .collect();
        assert!(names.contains(&"loadr_http_reqs_total"), "{names:?}");
        assert!(
            names.contains(&"loadr_http_req_duration_milliseconds"),
            "{names:?}"
        );
        let counter = request
            .timeseries
            .iter()
            .find(|ts| {
                ts.labels
                    .iter()
                    .any(|l| l.name == "__name__" && l.value == "loadr_http_reqs_total")
            })
            .expect("counter series");
        assert_eq!(counter.samples.len(), 1);
        assert!((counter.samples[0].value - 5.0).abs() < 1e-9);
        assert!(counter
            .labels
            .iter()
            .any(|l| l.name == "method" && l.value == "GET"));
        // Labels sorted by name.
        let mut sorted = counter.labels.clone();
        sorted.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(counter.labels, sorted);

        out.finish(&fixture_summary()).await;
    }

    #[test]
    fn sanitizes_names_and_labels() {
        assert_eq!(sanitize_metric("http.req-duration"), "http_req_duration");
        assert_eq!(sanitize_metric("a:b"), "a:b");
        assert_eq!(sanitize_label("9bad.key"), "_9bad_key");
        assert_eq!(escape_label_value("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}
