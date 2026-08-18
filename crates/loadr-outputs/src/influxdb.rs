//! InfluxDB output: line protocol pushed over HTTP every interval.
//!
//! Endpoint: v2 `{url}/api/v2/write?bucket={database}&org={organization}` with
//! `Authorization: Token <token>` when a token is configured, otherwise the v1
//! `{url}/write?db={database}` endpoint.
//!
//! Lines (`loadr_<metric>,tag=val field=value timestamp_ns`):
//! - trend → fields `avg,min,max,med,p90,p95,p99`
//! - counter → `sum` plus the interval `rate` (events/sec over the snapshot window)
//! - rate → `rate` (pass fraction)
//! - gauge → `value` (last)

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{HeaderName, HeaderValue, Uri};
use loadr_core::aggregate::Snapshot;
use loadr_core::error::EngineError;
use loadr_core::metrics::MetricKind;
use loadr_core::output::Output;
use loadr_core::summary::Summary;
use parking_lot::RwLock;

use crate::http_client;

type Shared = Arc<RwLock<Option<Snapshot>>>;

#[derive(Clone)]
struct Target {
    uri: Uri,
    headers: Vec<(HeaderName, HeaderValue)>,
}

/// Pushes snapshot aggregates to InfluxDB as line protocol.
pub struct InfluxdbOutput {
    url: String,
    database: String,
    token: Option<String>,
    organization: Option<String>,
    interval: Duration,
    latest: Shared,
    target: Option<Target>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl InfluxdbOutput {
    /// Create an InfluxDB output pushing to `url` every `interval`.
    pub fn new(
        url: String,
        database: String,
        token: Option<String>,
        organization: Option<String>,
        interval: Duration,
    ) -> Self {
        InfluxdbOutput {
            url,
            database,
            token,
            organization,
            interval,
            latest: Arc::new(RwLock::new(None)),
            target: None,
            task: None,
        }
    }

    fn build_target(&self) -> Result<Target, EngineError> {
        let base = self.url.trim_end_matches('/');
        let mut headers = Vec::new();
        let url = match &self.token {
            Some(token) => {
                let org = self.organization.as_deref().unwrap_or("loadr");
                let value = HeaderValue::from_str(&format!("Token {token}")).map_err(|err| {
                    EngineError::Config(format!("influxdb token is not a valid header: {err}"))
                })?;
                headers.push((http::header::AUTHORIZATION, value));
                format!(
                    "{base}/api/v2/write?bucket={}&org={}",
                    query_encode(&self.database),
                    query_encode(org)
                )
            }
            None => format!("{base}/write?db={}", query_encode(&self.database)),
        };
        let uri: Uri = url
            .parse()
            .map_err(|err| EngineError::Config(format!("influxdb url `{url}`: {err}")))?;
        headers.push((
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        ));
        Ok(Target { uri, headers })
    }
}

impl Drop for InfluxdbOutput {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl Output for InfluxdbOutput {
    fn name(&self) -> &str {
        "influxdb"
    }

    fn wants_samples(&self) -> bool {
        false
    }

    async fn start(&mut self) -> Result<(), EngineError> {
        let target = self.build_target()?;
        self.target = Some(target.clone());
        let latest = self.latest.clone();
        let interval = self.interval;
        self.task = Some(tokio::spawn(async move {
            let client = http_client::client();
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                push_once(&client, &target, &latest).await;
            }
        }));
        Ok(())
    }

    async fn on_snapshot(&mut self, snapshot: &Snapshot) {
        *self.latest.write() = Some(snapshot.clone());
    }

    async fn finish(&mut self, summary: &Summary) {
        *self.latest.write() = Some(summary.snapshot.clone());
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // One final push so short runs still export data.
        if let Some(target) = &self.target {
            let client = http_client::client();
            push_once(&client, target, &self.latest).await;
        }
    }
}

async fn push_once(client: &http_client::HttpClient, target: &Target, latest: &Shared) {
    let snapshot = latest.read().clone();
    let Some(snapshot) = snapshot else {
        return;
    };
    let body = render_lines(&snapshot);
    if body.is_empty() {
        return;
    }
    match http_client::post(client, &target.uri, &target.headers, body.into_bytes()).await {
        Ok(status) if status.is_success() => {}
        Ok(status) => {
            tracing::warn!(uri = %target.uri, %status, "influxdb write rejected");
        }
        Err(err) => {
            tracing::warn!(uri = %target.uri, error = %err, "influxdb write failed");
        }
    }
}

/// Percent-encode a query parameter value (conservative allowlist).
fn query_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// Escape a measurement name (commas and spaces).
fn escape_measurement(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        match c {
            ',' => out.push_str("\\,"),
            ' ' => out.push_str("\\ "),
            other => out.push(other),
        }
    }
    out
}

/// Escape a tag key or value (commas, spaces and equals signs).
fn escape_tag(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            ',' => out.push_str("\\,"),
            ' ' => out.push_str("\\ "),
            '=' => out.push_str("\\="),
            other => out.push(other),
        }
    }
    out
}

/// Render one snapshot as InfluxDB line protocol.
pub(crate) fn render_lines(snapshot: &Snapshot) -> String {
    let timestamp_ns = (snapshot.timestamp_ms as u128) * 1_000_000;
    let mut out = String::new();
    for series in &snapshot.series {
        let mut fields: Vec<(&str, f64)> = Vec::new();
        match series.kind {
            MetricKind::Trend => {
                for (name, value) in [
                    ("avg", series.agg.avg),
                    ("min", series.agg.min),
                    ("max", series.agg.max),
                    ("med", series.agg.med),
                    ("p90", series.agg.p90),
                    ("p95", series.agg.p95),
                    ("p99", series.agg.p99),
                ] {
                    if let Some(v) = value {
                        fields.push((name, v));
                    }
                }
            }
            MetricKind::Counter => {
                fields.push(("sum", series.agg.sum));
                if snapshot.interval_secs > 0.0 {
                    fields.push(("rate", series.interval_sum / snapshot.interval_secs));
                }
            }
            MetricKind::Rate => {
                if let Some(rate) = series.agg.rate {
                    fields.push(("rate", rate));
                }
            }
            MetricKind::Gauge => {
                if let Some(last) = series.agg.last {
                    fields.push(("value", last));
                }
            }
        }
        fields.retain(|(_, v)| v.is_finite());
        if fields.is_empty() {
            continue;
        }
        out.push_str("loadr_");
        out.push_str(&escape_measurement(&series.metric));
        for (k, v) in &series.tags {
            out.push(',');
            out.push_str(&escape_tag(k));
            out.push('=');
            out.push_str(&escape_tag(v));
        }
        out.push(' ');
        for (i, (name, value)) in fields.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(name);
            out.push('=');
            out.push_str(&format!("{value}"));
        }
        out.push(' ');
        out.push_str(&timestamp_ns.to_string());
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{fixture_snapshot, fixture_summary, spawn_capture_server};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pushes_v2_line_protocol_with_auth() {
        let (addr, mut rx) = spawn_capture_server().await;
        let mut out = InfluxdbOutput::new(
            format!("http://{addr}"),
            "loadtest".to_string(),
            Some("secret-token".to_string()),
            Some("acme".to_string()),
            Duration::from_millis(50),
        );
        out.start().await.expect("start");
        out.on_snapshot(&fixture_snapshot()).await;

        let captured = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("push within timeout")
            .expect("captured request");
        assert_eq!(captured.method, "POST");
        assert_eq!(
            captured.path_and_query,
            "/api/v2/write?bucket=loadtest&org=acme"
        );
        assert_eq!(
            captured.headers.get("authorization").map(|v| v.as_bytes()),
            Some(b"Token secret-token".as_ref())
        );
        let body = String::from_utf8(captured.body).expect("utf8 body");
        let counter_line = body
            .lines()
            .find(|l| l.starts_with("loadr_http_reqs,"))
            .expect("counter line");
        assert!(counter_line.contains("method=GET"), "{counter_line}");
        assert!(counter_line.contains("sum=5"), "{counter_line}");
        let trend_line = body
            .lines()
            .find(|l| l.starts_with("loadr_http_req_duration,"))
            .expect("trend line");
        for field in ["avg=", "min=", "max=", "med=", "p90=", "p95=", "p99="] {
            assert!(trend_line.contains(field), "{trend_line}");
        }
        let gauge_line = body
            .lines()
            .find(|l| l.starts_with("loadr_vus "))
            .expect("gauge line");
        assert!(gauge_line.contains("value=7"), "{gauge_line}");
        let rate_line = body
            .lines()
            .find(|l| l.starts_with("loadr_checks,"))
            .expect("rate line");
        assert!(rate_line.contains("rate=0.75"), "{rate_line}");
        // Nanosecond timestamp at the end of each line.
        let ts: u128 = counter_line
            .rsplit(' ')
            .next()
            .expect("timestamp")
            .parse()
            .expect("numeric timestamp");
        assert!(ts > 1_000_000_000_000_000_000);

        out.finish(&fixture_summary()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uses_v1_endpoint_without_token() {
        let (addr, mut rx) = spawn_capture_server().await;
        let mut out = InfluxdbOutput::new(
            format!("http://{addr}/"),
            "k6 db".to_string(),
            None,
            None,
            Duration::from_millis(50),
        );
        out.start().await.expect("start");
        out.on_snapshot(&fixture_snapshot()).await;

        let captured = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("push within timeout")
            .expect("captured request");
        assert_eq!(captured.path_and_query, "/write?db=k6%20db");
        assert!(captured.headers.get("authorization").is_none());

        out.finish(&fixture_summary()).await;
    }

    #[test]
    fn escaping() {
        assert_eq!(escape_measurement("a b,c"), "a\\ b\\,c");
        assert_eq!(escape_tag("a=b c"), "a\\=b\\ c");
        assert_eq!(query_encode("k6 db"), "k6%20db");
    }
}
