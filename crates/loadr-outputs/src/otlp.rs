//! OTLP metrics export over gRPC or HTTP (binary protobuf), pushed every
//! interval from the latest snapshot.
//!
//! Mapping:
//! - counter → `Sum` (monotonic, cumulative, double) of the running total
//! - gauge → `Gauge` of the last value
//! - rate → `Gauge` of the pass fraction
//! - trend → `Summary` data point with quantiles 0.5/0.9/0.95/0.99 (milliseconds)
//!
//! Resource attributes: `service.name = "loadr"`. Sample tags become data
//! point attributes. Custom `headers` are applied as gRPC metadata / HTTP
//! headers.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{HeaderName, HeaderValue, Uri};
use indexmap::IndexMap;
use loadr_config::OtlpProtocol;
use loadr_core::aggregate::Snapshot;
use loadr_core::error::EngineError;
use loadr_core::metrics::{MetricKind, Tags};
use loadr_core::output::Output;
use loadr_core::summary::Summary;
use parking_lot::RwLock;
use prost::Message as _;

use crate::http_client;
use crate::proto::opentelemetry::proto::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use crate::proto::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
use crate::proto::opentelemetry::proto::common::v1::{
    any_value, AnyValue, InstrumentationScope, KeyValue,
};
use crate::proto::opentelemetry::proto::metrics::v1::{
    metric, number_data_point, summary_data_point, AggregationTemporality, Gauge, Metric,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary as OtlpSummary, SummaryDataPoint,
};
use crate::proto::opentelemetry::proto::resource::v1::Resource;

type Shared = Arc<RwLock<Option<Snapshot>>>;
type GrpcClient = MetricsServiceClient<tonic::transport::Channel>;
type GrpcHeaders = Vec<(
    tonic::metadata::MetadataKey<tonic::metadata::Ascii>,
    tonic::metadata::AsciiMetadataValue,
)>;

/// OTLP metrics exporter (gRPC or HTTP/protobuf).
pub struct OtlpOutput {
    endpoint: String,
    protocol: OtlpProtocol,
    headers: IndexMap<String, String>,
    interval: Duration,
    latest: Shared,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl OtlpOutput {
    /// Create an OTLP output exporting to `endpoint` every `interval`.
    pub fn new(
        endpoint: String,
        protocol: OtlpProtocol,
        headers: IndexMap<String, String>,
        interval: Duration,
    ) -> Self {
        OtlpOutput {
            endpoint,
            protocol,
            headers,
            interval,
            latest: Arc::new(RwLock::new(None)),
            task: None,
        }
    }

    /// Endpoint with an `http://` scheme prepended when missing (gRPC).
    fn grpc_endpoint(&self) -> String {
        if self.endpoint.contains("://") {
            self.endpoint.clone()
        } else {
            format!("http://{}", self.endpoint)
        }
    }

    fn http_uri(&self) -> Result<Uri, EngineError> {
        let endpoint = if self.endpoint.contains("://") {
            self.endpoint.clone()
        } else {
            format!("http://{}", self.endpoint)
        };
        let url = format!("{}/v1/metrics", endpoint.trim_end_matches('/'));
        url.parse()
            .map_err(|err| EngineError::Config(format!("otlp endpoint `{url}`: {err}")))
    }

    fn grpc_headers(&self) -> GrpcHeaders {
        let mut out = GrpcHeaders::new();
        for (key, value) in &self.headers {
            let parsed_key =
                tonic::metadata::MetadataKey::from_bytes(key.to_ascii_lowercase().as_bytes());
            let parsed_value = tonic::metadata::AsciiMetadataValue::try_from(value.as_str());
            match (parsed_key, parsed_value) {
                (Ok(k), Ok(v)) => out.push((k, v)),
                _ => tracing::warn!(header = %key, "otlp: skipping invalid grpc metadata header"),
            }
        }
        out
    }

    fn http_headers(&self) -> Vec<(HeaderName, HeaderValue)> {
        let mut out = vec![(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        )];
        for (key, value) in &self.headers {
            match (
                HeaderName::from_bytes(key.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                (Ok(k), Ok(v)) => out.push((k, v)),
                _ => tracing::warn!(header = %key, "otlp: skipping invalid http header"),
            }
        }
        out
    }
}

impl Drop for OtlpOutput {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl Output for OtlpOutput {
    fn name(&self) -> &str {
        "otlp"
    }

    fn wants_samples(&self) -> bool {
        false
    }

    async fn start(&mut self) -> Result<(), EngineError> {
        let latest = self.latest.clone();
        let interval = self.interval;
        match self.protocol {
            OtlpProtocol::Grpc => {
                let endpoint = self.grpc_endpoint();
                // Validate the endpoint up front.
                tonic::transport::Endpoint::from_shared(endpoint.clone()).map_err(|err| {
                    EngineError::Config(format!("otlp endpoint `{endpoint}`: {err}"))
                })?;
                let headers = self.grpc_headers();
                self.task = Some(tokio::spawn(async move {
                    let mut client: Option<GrpcClient> = None;
                    let mut tick = tokio::time::interval(interval);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        let snapshot = latest.read().clone();
                        let Some(snapshot) = snapshot else { continue };
                        export_grpc_once(&endpoint, &mut client, &headers, &snapshot).await;
                    }
                }));
            }
            OtlpProtocol::Http => {
                let uri = self.http_uri()?;
                let headers = self.http_headers();
                self.task = Some(tokio::spawn(async move {
                    let client = http_client::client();
                    let mut tick = tokio::time::interval(interval);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        let snapshot = latest.read().clone();
                        let Some(snapshot) = snapshot else { continue };
                        export_http_once(&client, &uri, &headers, &snapshot).await;
                    }
                }));
            }
        }
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
        // One final export so short runs still ship data.
        let snapshot = self.latest.read().clone();
        let Some(snapshot) = snapshot else { return };
        match self.protocol {
            OtlpProtocol::Grpc => {
                let mut client = None;
                export_grpc_once(
                    &self.grpc_endpoint(),
                    &mut client,
                    &self.grpc_headers(),
                    &snapshot,
                )
                .await;
            }
            OtlpProtocol::Http => {
                if let Ok(uri) = self.http_uri() {
                    let client = http_client::client();
                    export_http_once(&client, &uri, &self.http_headers(), &snapshot).await;
                }
            }
        }
    }
}

async fn export_grpc_once(
    endpoint: &str,
    client: &mut Option<GrpcClient>,
    headers: &GrpcHeaders,
    snapshot: &Snapshot,
) {
    if client.is_none() {
        let connected = async {
            let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())?
                .connect_timeout(Duration::from_secs(5))
                .connect()
                .await?;
            Ok::<_, tonic::transport::Error>(MetricsServiceClient::new(channel))
        }
        .await;
        match connected {
            Ok(c) => *client = Some(c),
            Err(err) => {
                tracing::warn!(%endpoint, error = %err, "otlp grpc connect failed");
                return;
            }
        }
    }
    let Some(active) = client.as_mut() else {
        return;
    };
    let mut request = tonic::Request::new(build_export_request(snapshot));
    for (key, value) in headers {
        request.metadata_mut().insert(key.clone(), value.clone());
    }
    if let Err(status) = active.export(request).await {
        tracing::warn!(%endpoint, %status, "otlp grpc export failed");
        *client = None;
    }
}

async fn export_http_once(
    client: &http_client::HttpClient,
    uri: &Uri,
    headers: &[(HeaderName, HeaderValue)],
    snapshot: &Snapshot,
) {
    let body = build_export_request(snapshot).encode_to_vec();
    match http_client::post(client, uri, headers, body).await {
        Ok(status) if status.is_success() => {}
        Ok(status) => {
            tracing::warn!(%uri, %status, "otlp http export rejected");
        }
        Err(err) => {
            tracing::warn!(%uri, error = %err, "otlp http export failed");
        }
    }
}

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn tags_to_attrs(tags: &Tags) -> Vec<KeyValue> {
    tags.iter().map(|(k, v)| string_attr(k, v)).collect()
}

fn same_shape(a: &Option<metric::Data>, b: &Option<metric::Data>) -> bool {
    matches!(
        (a, b),
        (Some(metric::Data::Sum(_)), Some(metric::Data::Sum(_)))
            | (Some(metric::Data::Gauge(_)), Some(metric::Data::Gauge(_)))
            | (
                Some(metric::Data::Summary(_)),
                Some(metric::Data::Summary(_))
            )
    )
}

/// Build the export request from a snapshot. Adjacent series of the same
/// metric (different tag sets) are merged into one `Metric` with multiple
/// data points.
pub(crate) fn build_export_request(snapshot: &Snapshot) -> ExportMetricsServiceRequest {
    let time_unix_nano = snapshot.timestamp_ms.saturating_mul(1_000_000);
    let start_ms = snapshot.timestamp_ms as f64 - snapshot.elapsed_secs * 1000.0;
    let start_time_unix_nano = (start_ms.max(0.0) as u64).saturating_mul(1_000_000);

    let mut metrics: Vec<Metric> = Vec::new();
    for series in &snapshot.series {
        let attributes = tags_to_attrs(&series.tags);
        let number_point = |value: f64| NumberDataPoint {
            start_time_unix_nano,
            time_unix_nano,
            value: Some(number_data_point::Value::AsDouble(value)),
            attributes: attributes.clone(),
            flags: 0,
        };
        let (unit, data) = match series.kind {
            MetricKind::Counter => (
                "",
                metric::Data::Sum(Sum {
                    data_points: vec![number_point(series.agg.sum)],
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    is_monotonic: true,
                }),
            ),
            MetricKind::Gauge => (
                "",
                metric::Data::Gauge(Gauge {
                    data_points: vec![number_point(series.agg.last.unwrap_or(0.0))],
                }),
            ),
            MetricKind::Rate => (
                "",
                metric::Data::Gauge(Gauge {
                    data_points: vec![number_point(series.agg.rate.unwrap_or(0.0))],
                }),
            ),
            MetricKind::Trend => {
                let quantile_values = [
                    (0.5, series.agg.med),
                    (0.9, series.agg.p90),
                    (0.95, series.agg.p95),
                    (0.99, series.agg.p99),
                ]
                .into_iter()
                .filter_map(|(quantile, value)| {
                    value.map(|value| summary_data_point::ValueAtQuantile { quantile, value })
                })
                .collect();
                (
                    "ms",
                    metric::Data::Summary(OtlpSummary {
                        data_points: vec![SummaryDataPoint {
                            start_time_unix_nano,
                            time_unix_nano,
                            count: series.agg.count,
                            sum: series.agg.sum,
                            quantile_values,
                            attributes: attributes.clone(),
                            flags: 0,
                        }],
                    }),
                )
            }
        };
        let next = Metric {
            name: series.metric.clone(),
            description: String::new(),
            unit: unit.to_string(),
            data: Some(data),
        };
        match metrics.last_mut() {
            Some(last) if last.name == next.name && same_shape(&last.data, &next.data) => {
                match (&mut last.data, next.data) {
                    (Some(metric::Data::Sum(a)), Some(metric::Data::Sum(b))) => {
                        a.data_points.extend(b.data_points);
                    }
                    (Some(metric::Data::Gauge(a)), Some(metric::Data::Gauge(b))) => {
                        a.data_points.extend(b.data_points);
                    }
                    (Some(metric::Data::Summary(a)), Some(metric::Data::Summary(b))) => {
                        a.data_points.extend(b.data_points);
                    }
                    _ => {}
                }
            }
            _ => metrics.push(next),
        }
    }

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_attr("service.name", "loadr")],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "loadr".to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                }),
                metrics,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::proto::collector::metrics::v1::metrics_service_server::{
        MetricsService, MetricsServiceServer,
    };
    use crate::proto::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceResponse;
    use crate::test_support::{fixture_snapshot, fixture_summary, spawn_capture_server};
    use tokio::sync::mpsc;

    fn metric_names(request: &ExportMetricsServiceRequest) -> Vec<String> {
        request
            .resource_metrics
            .iter()
            .flat_map(|rm| &rm.scope_metrics)
            .flat_map(|sm| &sm.metrics)
            .map(|m| m.name.clone())
            .collect()
    }

    fn find_metric<'a>(request: &'a ExportMetricsServiceRequest, name: &str) -> &'a Metric {
        request
            .resource_metrics
            .iter()
            .flat_map(|rm| &rm.scope_metrics)
            .flat_map(|sm| &sm.metrics)
            .find(|m| m.name == name)
            .unwrap_or_else(|| panic!("metric {name} not found"))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_export_posts_protobuf() {
        let (addr, mut rx) = spawn_capture_server().await;
        let mut headers = IndexMap::new();
        headers.insert("x-api-key".to_string(), "secret".to_string());
        let mut out = OtlpOutput::new(
            format!("http://{addr}"),
            OtlpProtocol::Http,
            headers,
            Duration::from_millis(50),
        );
        out.start().await.expect("start");
        out.on_snapshot(&fixture_snapshot()).await;

        let captured = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("export within timeout")
            .expect("captured request");
        assert_eq!(captured.method, "POST");
        assert_eq!(captured.path_and_query, "/v1/metrics");
        assert_eq!(
            captured.headers.get("content-type").map(|v| v.as_bytes()),
            Some(b"application/x-protobuf".as_ref())
        );
        assert_eq!(
            captured.headers.get("x-api-key").map(|v| v.as_bytes()),
            Some(b"secret".as_ref())
        );

        let request =
            ExportMetricsServiceRequest::decode(captured.body.as_slice()).expect("decode");
        let names = metric_names(&request);
        assert!(names.contains(&"http_reqs".to_string()), "{names:?}");
        assert!(
            names.contains(&"http_req_duration".to_string()),
            "{names:?}"
        );
        assert!(names.contains(&"vus".to_string()), "{names:?}");

        let sum = find_metric(&request, "http_reqs");
        let Some(metric::Data::Sum(sum)) = &sum.data else {
            panic!("http_reqs should be a Sum");
        };
        assert!(sum.is_monotonic);
        assert_eq!(
            sum.aggregation_temporality,
            AggregationTemporality::Cumulative as i32
        );
        assert_eq!(
            sum.data_points[0].value,
            Some(number_data_point::Value::AsDouble(5.0))
        );
        assert!(sum.data_points[0]
            .attributes
            .iter()
            .any(|kv| kv.key == "method"));

        let trend = find_metric(&request, "http_req_duration");
        let Some(metric::Data::Summary(summary)) = &trend.data else {
            panic!("http_req_duration should be a Summary");
        };
        assert_eq!(trend.unit, "ms");
        let dp = &summary.data_points[0];
        assert_eq!(dp.count, 5);
        let quantiles: Vec<f64> = dp.quantile_values.iter().map(|q| q.quantile).collect();
        assert_eq!(quantiles, vec![0.5, 0.9, 0.95, 0.99]);

        let resource = &request.resource_metrics[0].resource;
        assert!(resource
            .as_ref()
            .expect("resource")
            .attributes
            .iter()
            .any(|kv| kv.key == "service.name"));

        out.finish(&fixture_summary()).await;
    }

    struct CaptureSvc {
        tx: mpsc::UnboundedSender<(tonic::metadata::MetadataMap, ExportMetricsServiceRequest)>,
    }

    #[tonic::async_trait]
    impl MetricsService for CaptureSvc {
        async fn export(
            &self,
            request: tonic::Request<ExportMetricsServiceRequest>,
        ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
            let metadata = request.metadata().clone();
            let _ = self.tx.send((metadata, request.into_inner()));
            Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_export_reaches_collector() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let _ = tonic::transport::Server::builder()
                .add_service(MetricsServiceServer::new(CaptureSvc { tx }))
                .serve_with_incoming(incoming)
                .await;
        });

        let mut headers = IndexMap::new();
        headers.insert("X-Api-Key".to_string(), "secret".to_string());
        let mut out = OtlpOutput::new(
            format!("http://{addr}"),
            OtlpProtocol::Grpc,
            headers,
            Duration::from_millis(50),
        );
        out.start().await.expect("start");
        out.on_snapshot(&fixture_snapshot()).await;

        let (metadata, request) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("export within timeout")
            .expect("captured request");
        assert_eq!(
            metadata.get("x-api-key").map(|v| v.as_bytes()),
            Some(b"secret".as_ref())
        );
        let names = metric_names(&request);
        assert!(names.contains(&"http_reqs".to_string()), "{names:?}");
        assert!(names.contains(&"checks".to_string()), "{names:?}");
        let rate = find_metric(&request, "checks");
        let Some(metric::Data::Gauge(gauge)) = &rate.data else {
            panic!("rate should map to a Gauge");
        };
        assert_eq!(
            gauge.data_points[0].value,
            Some(number_data_point::Value::AsDouble(0.75))
        );

        out.finish(&fixture_summary()).await;
    }
}
