//! Shared fixtures and a capturing HTTP server for output tests.

use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::{BodyExt as _, Full};
use loadr_core::aggregate::{Aggregator, Snapshot};
use loadr_core::metrics::{now_millis, MetricKind, Sample, Tags};
use loadr_core::summary::Summary;
use tokio::sync::mpsc;

pub(crate) fn sample(metric: &str, kind: MetricKind, value: f64, tags: &[(&str, &str)]) -> Sample {
    let tags: Tags = tags
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    Sample {
        metric: Arc::from(metric),
        kind,
        value,
        tags: Arc::new(tags),
        timestamp_ms: now_millis(),
    }
}

/// One of each metric kind, with tags.
pub(crate) fn fixture_samples() -> Vec<Sample> {
    let mut samples = Vec::new();
    let tags: &[(&str, &str)] = &[("method", "GET"), ("status", "200")];
    for _ in 0..5 {
        samples.push(sample("http_reqs", MetricKind::Counter, 1.0, tags));
    }
    for v in [10.0, 11.0, 12.0, 13.0, 14.0] {
        samples.push(sample("http_req_duration", MetricKind::Trend, v, tags));
    }
    for v in [1.0, 1.0, 0.0, 1.0] {
        samples.push(sample("checks", MetricKind::Rate, v, &[("check", "ok")]));
    }
    samples.push(sample("vus", MetricKind::Gauge, 7.0, &[]));
    samples
}

/// A realistic snapshot built by running the fixtures through an aggregator.
pub(crate) fn fixture_snapshot() -> Snapshot {
    let mut agg = Aggregator::new();
    for s in fixture_samples() {
        agg.record(&s);
    }
    agg.snapshot()
}

pub(crate) fn fixture_summary() -> Summary {
    let mut agg = Aggregator::new();
    for s in fixture_samples() {
        agg.record(&s);
    }
    Summary::build(
        Some("demo".into()),
        "run-1".into(),
        now_millis(),
        vec!["default".into()],
        &mut agg,
        Vec::new(),
        None,
        Vec::new(),
    )
}

/// A captured HTTP request.
pub(crate) struct Captured {
    pub method: String,
    pub path_and_query: String,
    pub headers: http::HeaderMap,
    pub body: Vec<u8>,
}

/// Spawn a tiny HTTP server on 127.0.0.1:0 that captures every request and
/// replies 200 with an empty body.
pub(crate) async fn spawn_capture_server() -> (SocketAddr, mpsc::UnboundedReceiver<Captured>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind capture server");
    let addr = listener.local_addr().expect("local addr");
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let svc =
                    hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let tx = tx.clone();
                        async move {
                            let (parts, body) = req.into_parts();
                            let body = body
                                .collect()
                                .await
                                .map(|b| b.to_bytes().to_vec())
                                .unwrap_or_default();
                            let _ = tx.send(Captured {
                                method: parts.method.to_string(),
                                path_and_query: parts
                                    .uri
                                    .path_and_query()
                                    .map(|p| p.to_string())
                                    .unwrap_or_default(),
                                headers: parts.headers,
                                body,
                            });
                            Ok::<_, std::convert::Infallible>(http::Response::new(Full::new(
                                bytes::Bytes::new(),
                            )))
                        }
                    });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    (addr, rx)
}
