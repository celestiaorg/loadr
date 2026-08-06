//! Metric primitives: kinds, samples, the metric registry and the sample bus.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Sorted tag set attached to samples.
pub type Tags = BTreeMap<String, String>;

/// Whether a metric is a primary request counter for cross-protocol rollups.
/// GraphQL also emits the underlying HTTP transport metric, so including
/// `graphql_reqs` would count one operation twice.
pub fn is_request_counter_metric(metric: &str) -> bool {
    metric.ends_with("_reqs") && !matches!(metric, "graphql_reqs" | "request_reqs")
}

/// Whether a metric is a primary request-duration trend for cross-protocol
/// rollups. See [`is_request_counter_metric`] for the GraphQL exclusion.
pub fn is_request_duration_metric(metric: &str) -> bool {
    metric.ends_with("_req_duration")
        && !matches!(metric, "graphql_req_duration" | "request_duration")
}

/// The four metric kinds, matching k6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    /// Monotonically accumulating sum.
    Counter,
    /// Last value (also tracks min/max).
    Gauge,
    /// Fraction of non-zero samples.
    Rate,
    /// Distribution (HDR histogram): percentiles, avg, min, max.
    Trend,
}

impl MetricKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Rate => "rate",
            MetricKind::Trend => "trend",
        }
    }
}

impl From<loadr_config::MetricKindSpec> for MetricKind {
    fn from(spec: loadr_config::MetricKindSpec) -> Self {
        match spec {
            loadr_config::MetricKindSpec::Counter => MetricKind::Counter,
            loadr_config::MetricKindSpec::Gauge => MetricKind::Gauge,
            loadr_config::MetricKindSpec::Rate => MetricKind::Rate,
            loadr_config::MetricKindSpec::Trend => MetricKind::Trend,
        }
    }
}

/// Definition of a metric.
#[derive(Debug, Clone)]
pub struct MetricDef {
    pub name: Arc<str>,
    pub kind: MetricKind,
    /// Trend values are durations in milliseconds.
    pub time: bool,
    pub description: Option<String>,
}

/// Built-in metrics (name, kind, is-time).
pub const BUILTIN_METRIC_DEFS: &[(&str, MetricKind, bool)] = &[
    ("http_reqs", MetricKind::Counter, false),
    ("http_req_duration", MetricKind::Trend, true),
    ("http_req_blocked", MetricKind::Trend, true),
    ("http_req_connecting", MetricKind::Trend, true),
    ("http_req_tls_handshaking", MetricKind::Trend, true),
    ("http_req_sending", MetricKind::Trend, true),
    ("http_req_waiting", MetricKind::Trend, true),
    ("http_req_receiving", MetricKind::Trend, true),
    ("http_req_failed", MetricKind::Rate, false),
    ("iterations", MetricKind::Counter, false),
    ("iteration_duration", MetricKind::Trend, true),
    ("dropped_iterations", MetricKind::Counter, false),
    ("vus", MetricKind::Gauge, false),
    ("vus_max", MetricKind::Gauge, false),
    ("requests_in_flight", MetricKind::Gauge, false),
    ("checks", MetricKind::Rate, false),
    // Script (JS) exceptions raised in hooks, exec functions, and js steps.
    // Tagged with `exception` (a normalised message) and `scenario`.
    ("vu_exceptions", MetricKind::Counter, false),
    // Chaos faults injected by a scenario's `faults:` block.
    // Tagged with `kind` (`latency` or `drop`) and `scenario`.
    ("faults_injected", MetricKind::Counter, false),
    ("data_sent", MetricKind::Counter, false),
    ("data_received", MetricKind::Counter, false),
    ("ws_connecting", MetricKind::Trend, true),
    ("ws_session_duration", MetricKind::Trend, true),
    ("ws_msgs_sent", MetricKind::Counter, false),
    ("ws_msgs_received", MetricKind::Counter, false),
    ("grpc_reqs", MetricKind::Counter, false),
    ("grpc_req_duration", MetricKind::Trend, true),
    ("tcp_reqs", MetricKind::Counter, false),
    ("tcp_req_duration", MetricKind::Trend, true),
    ("udp_reqs", MetricKind::Counter, false),
    ("udp_req_duration", MetricKind::Trend, true),
    ("noop_reqs", MetricKind::Counter, false),
    ("noop_req_duration", MetricKind::Trend, true),
    ("graphql_reqs", MetricKind::Counter, false),
    ("graphql_req_duration", MetricKind::Trend, true),
];

/// Gauges whose fleet value is the sum of per-agent values (total running
/// VUs), not last-writer-wins. Distributed merging in
/// `Aggregator::aggregate_selector` consults this.
pub fn is_additive_gauge(metric: &str) -> bool {
    matches!(metric, "vus" | "vus_max")
}

/// Additive gauges that describe *currently held* capacity and therefore must
/// be zeroed when their reporter stops (agent lost or terminal, engine
/// shutdown) so fleet sums don't go stale. Keep this, [`is_additive_gauge`]
/// and the engine's terminal zero (`engine.rs`) in sync when adding a live
/// gauge.
pub const LIVE_GAUGES: &[&str] = &["vus"];

/// Registry of known metrics: built-ins, YAML custom metrics, and metrics
/// created at runtime from JS.
#[derive(Debug, Default)]
pub struct MetricRegistry {
    defs: RwLock<HashMap<Arc<str>, MetricDef>>,
}

impl MetricRegistry {
    pub fn with_builtins() -> Self {
        let reg = MetricRegistry::default();
        {
            let mut defs = reg.defs.write();
            for (name, kind, time) in BUILTIN_METRIC_DEFS {
                let name: Arc<str> = Arc::from(*name);
                defs.insert(
                    name.clone(),
                    MetricDef {
                        name,
                        kind: *kind,
                        time: *time,
                        description: None,
                    },
                );
            }
        }
        reg
    }

    /// Register a metric; returns an error when re-registering with a different kind.
    pub fn register(
        &self,
        name: &str,
        kind: MetricKind,
        time: bool,
        description: Option<String>,
    ) -> Result<Arc<str>, String> {
        let mut defs = self.defs.write();
        if let Some(existing) = defs.get(name) {
            if existing.kind != kind {
                return Err(format!(
                    "metric `{name}` already registered as {}, cannot redefine as {}",
                    existing.kind.as_str(),
                    kind.as_str()
                ));
            }
            return Ok(existing.name.clone());
        }
        let arc: Arc<str> = Arc::from(name);
        defs.insert(
            arc.clone(),
            MetricDef {
                name: arc.clone(),
                kind,
                time,
                description,
            },
        );
        Ok(arc)
    }

    pub fn get(&self, name: &str) -> Option<MetricDef> {
        self.defs.read().get(name).cloned()
    }

    pub fn all(&self) -> Vec<MetricDef> {
        self.defs.read().values().cloned().collect()
    }
}

/// Milliseconds since the UNIX epoch.
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One metric sample.
#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    pub metric: Arc<str>,
    pub kind: MetricKind,
    pub value: f64,
    pub tags: Arc<Tags>,
    /// Milliseconds since the UNIX epoch.
    pub timestamp_ms: u64,
}

/// Where a `MetricsBus` delivers samples.
#[derive(Clone)]
enum Sink {
    /// The classic per-run channel, drained by the aggregator task.
    Tx(tokio::sync::mpsc::UnboundedSender<Sample>),
    /// Straight into a shard-local aggregator — no channel, no per-sample
    /// clock read, no drain backlog. Chosen once at startup (see
    /// `Output::wants_samples`) when nothing needs raw samples.
    Shard {
        shards: Arc<crate::aggregate::MetricShards>,
        idx: usize,
    },
}

/// Cloneable fan-in handle that VUs use to emit samples.
#[derive(Clone)]
pub struct MetricsBus {
    sink: Sink,
    requests_in_flight: Arc<AtomicU64>,
}

impl std::fmt::Debug for MetricsBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.sink {
            Sink::Tx(_) => f.write_str("MetricsBus::Tx"),
            Sink::Shard { idx, .. } => f
                .debug_struct("MetricsBus::Shard")
                .field("idx", idx)
                .finish(),
        }
    }
}

impl MetricsBus {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<Sample>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            MetricsBus {
                sink: Sink::Tx(tx),
                requests_in_flight: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    /// Build a bus that records straight into `shards` instead of a channel
    /// (see `MetricShards`). Returns the root handle, pinned to shard 0 —
    /// per-VU handles come from `for_vu`.
    pub fn sharded(shards: Arc<crate::aggregate::MetricShards>) -> Self {
        MetricsBus {
            sink: Sink::Shard { shards, idx: 0 },
            requests_in_flight: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A handle pinned to VU `vu_id`'s shard (a plain clone in channel mode).
    /// Applied once, in `VuContext::new`, so every emit site — VUs, the JS
    /// host, plugin protocols — is covered with no call-site changes. The
    /// in-flight counter is shared with the parent handle either way.
    pub fn for_vu(&self, vu_id: u64) -> Self {
        let sink = match &self.sink {
            Sink::Tx(tx) => Sink::Tx(tx.clone()),
            Sink::Shard { shards, .. } => Sink::Shard {
                shards: shards.clone(),
                idx: (vu_id % shards.len() as u64) as usize,
            },
        };
        MetricsBus {
            sink,
            requests_in_flight: self.requests_in_flight.clone(),
        }
    }

    pub fn emit(&self, sample: Sample) {
        match &self.sink {
            // The receiver only closes at the very end of a run; late
            // samples from draining VUs are intentionally dropped.
            Sink::Tx(tx) => {
                let _ = tx.send(sample);
            }
            Sink::Shard { shards, idx } => shards.record(*idx, &sample),
        }
    }

    pub fn emit_value(&self, metric: &Arc<str>, kind: MetricKind, value: f64, tags: &Arc<Tags>) {
        // Shard mode skips the clock read entirely: JsonOutput/CsvOutput are
        // the only readers of `timestamp_ms`, and both force bus mode via
        // `wants_samples`.
        let timestamp_ms = match &self.sink {
            Sink::Tx(_) => now_millis(),
            Sink::Shard { .. } => 0,
        };
        self.emit(Sample {
            metric: metric.clone(),
            kind,
            value,
            tags: tags.clone(),
            timestamp_ms,
        });
    }

    pub fn counter(&self, metric: &Arc<str>, value: f64, tags: &Arc<Tags>) {
        self.emit_value(metric, MetricKind::Counter, value, tags);
    }

    pub fn gauge(&self, metric: &Arc<str>, value: f64, tags: &Arc<Tags>) {
        self.emit_value(metric, MetricKind::Gauge, value, tags);
    }

    pub fn rate(&self, metric: &Arc<str>, pass: bool, tags: &Arc<Tags>) {
        self.emit_value(metric, MetricKind::Rate, if pass { 1.0 } else { 0.0 }, tags);
    }

    pub fn trend(&self, metric: &Arc<str>, value: f64, tags: &Arc<Tags>) {
        self.emit_value(metric, MetricKind::Trend, value, tags);
    }

    pub(crate) fn begin_request(&self) {
        self.requests_in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn end_request(&self) {
        self.requests_in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn requests_in_flight(&self) -> u64 {
        self.requests_in_flight.load(Ordering::Relaxed)
    }
}

/// Interned built-in metric names, resolved once per engine.
#[derive(Debug, Clone)]
pub struct BuiltinMetrics {
    pub http_reqs: Arc<str>,
    pub http_req_duration: Arc<str>,
    pub http_req_blocked: Arc<str>,
    pub http_req_connecting: Arc<str>,
    pub http_req_tls_handshaking: Arc<str>,
    pub http_req_sending: Arc<str>,
    pub http_req_waiting: Arc<str>,
    pub http_req_receiving: Arc<str>,
    pub http_req_failed: Arc<str>,
    pub iterations: Arc<str>,
    pub iteration_duration: Arc<str>,
    pub dropped_iterations: Arc<str>,
    pub vus: Arc<str>,
    pub vus_max: Arc<str>,
    pub checks: Arc<str>,
    pub vu_exceptions: Arc<str>,
    pub faults_injected: Arc<str>,
    pub data_sent: Arc<str>,
    pub data_received: Arc<str>,
    pub grpc_reqs: Arc<str>,
    pub grpc_req_duration: Arc<str>,
}

impl BuiltinMetrics {
    pub fn resolve(registry: &MetricRegistry) -> Self {
        let name = |n: &str| {
            registry
                .get(n)
                .map(|d| d.name)
                .unwrap_or_else(|| Arc::from(n))
        };
        BuiltinMetrics {
            http_reqs: name("http_reqs"),
            http_req_duration: name("http_req_duration"),
            http_req_blocked: name("http_req_blocked"),
            http_req_connecting: name("http_req_connecting"),
            http_req_tls_handshaking: name("http_req_tls_handshaking"),
            http_req_sending: name("http_req_sending"),
            http_req_waiting: name("http_req_waiting"),
            http_req_receiving: name("http_req_receiving"),
            http_req_failed: name("http_req_failed"),
            iterations: name("iterations"),
            iteration_duration: name("iteration_duration"),
            dropped_iterations: name("dropped_iterations"),
            vus: name("vus"),
            vus_max: name("vus_max"),
            checks: name("checks"),
            vu_exceptions: name("vu_exceptions"),
            faults_injected: name("faults_injected"),
            data_sent: name("data_sent"),
            data_received: name("data_received"),
            grpc_reqs: name("grpc_reqs"),
            grpc_req_duration: name("grpc_req_duration"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_builtins() {
        let reg = MetricRegistry::with_builtins();
        let def = reg.get("http_req_duration").expect("builtin");
        assert_eq!(def.kind, MetricKind::Trend);
        assert!(def.time);
        assert_eq!(reg.get("checks").map(|d| d.kind), Some(MetricKind::Rate));
        assert_eq!(
            reg.get("requests_in_flight").map(|d| d.kind),
            Some(MetricKind::Gauge)
        );
    }

    #[test]
    fn register_custom_and_conflict() {
        let reg = MetricRegistry::with_builtins();
        reg.register("my_counter", MetricKind::Counter, false, None)
            .expect("register");
        // Same kind is idempotent.
        reg.register("my_counter", MetricKind::Counter, false, None)
            .expect("idempotent");
        // Different kind is an error.
        assert!(reg
            .register("my_counter", MetricKind::Trend, false, None)
            .is_err());
    }

    #[tokio::test]
    async fn bus_delivers_samples() {
        let (bus, mut rx) = MetricsBus::new();
        let metric: Arc<str> = Arc::from("checks");
        let tags = Arc::new(Tags::new());
        bus.rate(&metric, true, &tags);
        bus.counter(&metric, 2.0, &tags);
        let s1 = rx.recv().await.expect("sample");
        assert_eq!(s1.value, 1.0);
        assert_eq!(s1.kind, MetricKind::Rate);
        let s2 = rx.recv().await.expect("sample");
        assert_eq!(s2.value, 2.0);
    }

    fn shard_idx(bus: &MetricsBus) -> usize {
        match &bus.sink {
            Sink::Shard { idx, .. } => *idx,
            Sink::Tx(_) => panic!("expected a shard sink"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sharded_bus_records_and_drains_exactly() {
        let shards = Arc::new(crate::aggregate::MetricShards::new(4));
        let root = MetricsBus::sharded(shards.clone());

        // Same-shard pinning: VU k and VU k+4 land on the same shard (idx %
        // len), and the root handle itself is shard 0.
        assert_eq!(shard_idx(&root), 0);
        for k in 0..4u64 {
            let a = root.for_vu(k);
            let b = root.for_vu(k + 4);
            assert_eq!(shard_idx(&a), k as usize, "vu {k} pins to shard {k}");
            assert_eq!(
                shard_idx(&b),
                k as usize,
                "vu {} shares vu {k}'s shard",
                k + 4
            );
        }

        // Concurrent emits from every VU, each pinned to its own bus handle.
        let metric: Arc<str> = Arc::from("http_reqs");
        let tags = Arc::new(Tags::new());
        const PER_VU: usize = 200;
        let mut handles = Vec::new();
        for vu_id in 0..8u64 {
            let vu_bus = root.for_vu(vu_id);
            let metric = metric.clone();
            let tags = tags.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..PER_VU {
                    vu_bus.counter(&metric, 1.0, &tags);
                }
            }));
        }
        for h in handles {
            h.await.expect("vu task");
        }

        let mut target = crate::aggregate::Aggregator::new();
        shards.drain_into(&mut target);
        let total = target.snapshot().find("http_reqs").expect("series").agg.sum;
        assert_eq!(total, (8 * PER_VU) as f64);

        // A second drain sees nothing new: exactly-once delivery.
        let mut target2 = crate::aggregate::Aggregator::new();
        shards.drain_into(&mut target2);
        assert!(target2.snapshot().find("http_reqs").is_none());
    }
}
