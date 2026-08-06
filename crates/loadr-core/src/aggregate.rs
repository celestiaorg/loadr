//! The aggregator: turns the sample stream into live snapshots, threshold
//! inputs, end-of-run summaries, and mergeable deltas for distributed mode.
//!
//! Trends use HDR histograms (3 significant figures, auto-resize). Distributed
//! aggregation merges histograms — percentiles are computed only after merging.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::serialization::{Deserializer as HdrDeserializer, Serializer as _, V2Serializer};
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::metrics::{is_additive_gauge, now_millis, MetricKind, Sample, Tags};

/// Trend values are stored ×1000 in the histogram (3 decimal places).
const TREND_SCALE: f64 = 1000.0;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesKey {
    metric: Arc<str>,
    tags: Arc<Tags>,
}

#[derive(Debug)]
enum SeriesData {
    Counter {
        total: f64,
        flushed: f64,
    },
    Gauge {
        last: f64,
        min: f64,
        max: f64,
        seq: u64,
    },
    Rate {
        passes: u64,
        total: u64,
        flushed_passes: u64,
        flushed_total: u64,
    },
    Trend {
        hist: Histogram<u64>,
        /// Delta histogram since the last `take_delta` (distributed mode).
        delta_hist: Histogram<u64>,
        sum: f64,
        count: u64,
        min: f64,
        max: f64,
    },
}

#[derive(Debug)]
struct Series {
    kind: MetricKind,
    data: SeriesData,
    /// Counter total at the last snapshot, for interval rates.
    snap_counter: f64,
    snap_count: u64,
}

fn new_histogram() -> Histogram<u64> {
    // 3 significant figures, auto-resizing. `new` only fails for sigfig > 5.
    let mut h = Histogram::<u64>::new(3).unwrap_or_else(|_| Histogram::<u64>::new(2).expect("hdr"));
    h.auto(true);
    h
}

impl Series {
    fn new(kind: MetricKind) -> Self {
        let data = match kind {
            MetricKind::Counter => SeriesData::Counter {
                total: 0.0,
                flushed: 0.0,
            },
            MetricKind::Gauge => SeriesData::Gauge {
                last: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
                seq: 0,
            },
            MetricKind::Rate => SeriesData::Rate {
                passes: 0,
                total: 0,
                flushed_passes: 0,
                flushed_total: 0,
            },
            MetricKind::Trend => SeriesData::Trend {
                hist: new_histogram(),
                delta_hist: new_histogram(),
                sum: 0.0,
                count: 0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
            },
        };
        Series {
            kind,
            data,
            snap_counter: 0.0,
            snap_count: 0,
        }
    }

    fn record(&mut self, value: f64, seq: u64) {
        match &mut self.data {
            SeriesData::Counter { total, .. } => *total += value,
            SeriesData::Gauge {
                last,
                min,
                max,
                seq: gseq,
            } => {
                *last = value;
                *min = min.min(value);
                *max = max.max(value);
                *gseq = seq;
            }
            SeriesData::Rate { passes, total, .. } => {
                *total += 1;
                if value != 0.0 {
                    *passes += 1;
                }
            }
            SeriesData::Trend {
                hist,
                delta_hist,
                sum,
                count,
                min,
                max,
            } => {
                let scaled = (value * TREND_SCALE).round().max(0.0) as u64;
                // auto-resize histograms only fail on overflow of u64 range
                let _ = hist.record(scaled);
                let _ = delta_hist.record(scaled);
                *sum += value;
                *count += 1;
                *min = min.min(value);
                *max = max.max(value);
            }
        }
    }
}

/// Aggregated values for one series (or a merged selector view).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AggValues {
    pub count: u64,
    pub sum: f64,
    pub avg: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub med: Option<f64>,
    pub p90: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub p999: Option<f64>,
    /// Rate metrics: pass fraction in [0,1].
    pub rate: Option<f64>,
    /// Gauges: last value.
    pub last: Option<f64>,
    /// Counters and rate-of-events: per-second rate over the whole run.
    pub per_second: Option<f64>,
}

impl AggValues {
    /// Look up the aggregation a threshold expression asks for.
    pub fn value_for(&self, agg: &loadr_config::Agg, kind: MetricKind) -> Option<f64> {
        use loadr_config::Agg;
        match agg {
            Agg::Avg => self.avg,
            Agg::Min => self.min,
            Agg::Max => self.max,
            Agg::Med => self.med,
            Agg::Percentile(p) => match *p {
                p if (p - 90.0).abs() < 1e-9 => self.p90,
                p if (p - 95.0).abs() < 1e-9 => self.p95,
                p if (p - 99.0).abs() < 1e-9 => self.p99,
                p if (p - 99.9).abs() < 1e-9 => self.p999,
                // Other percentiles are resolved during evaluation against the
                // merged histogram (see Aggregator::aggregate_selector_percentile).
                _ => None,
            },
            // SLO expressions resolve to the equivalent percentile lookup
            // (slo(99.9%) -> p99.9; slo(50%) -> median). thresholds::evaluate
            // resolves before calling here; this arm keeps the match total.
            Agg::Slo(n) => match *n {
                n if (n - 50.0).abs() < 1e-9 => self.med,
                n if (n - 90.0).abs() < 1e-9 => self.p90,
                n if (n - 95.0).abs() < 1e-9 => self.p95,
                n if (n - 99.0).abs() < 1e-9 => self.p99,
                n if (n - 99.9).abs() < 1e-9 => self.p999,
                _ => None,
            },
            Agg::Rate => match kind {
                MetricKind::Rate => self.rate,
                _ => self.per_second,
            },
            Agg::Count => Some(if kind == MetricKind::Counter {
                self.sum
            } else {
                self.count as f64
            }),
            Agg::Value => self.last,
        }
    }
}

/// Snapshot of one series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesSnapshot {
    pub metric: String,
    pub kind: MetricKind,
    pub tags: Tags,
    pub agg: AggValues,
    /// Events recorded since the previous snapshot (for live RPS displays).
    pub interval_count: u64,
    /// Sum recorded since the previous snapshot. For counters this is the
    /// counter increase; for rates it is the number of passing samples.
    pub interval_sum: f64,
}

/// A point-in-time view of every series.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Snapshot {
    pub timestamp_ms: u64,
    pub elapsed_secs: f64,
    /// Seconds since the previous snapshot (interval_* values cover this window).
    pub interval_secs: f64,
    pub series: Vec<SeriesSnapshot>,
}

impl Snapshot {
    /// Find a series by metric name with no tags or any tags (first match).
    pub fn find(&self, metric: &str) -> Option<&SeriesSnapshot> {
        self.series.iter().find(|s| s.metric == metric)
    }

    /// Sum interval counts across all series of a metric (e.g. live RPS).
    pub fn interval_count(&self, metric: &str) -> u64 {
        self.series
            .iter()
            .filter(|s| s.metric == metric)
            .map(|s| s.interval_count)
            .sum()
    }

    /// Sum interval counts across *every* request-counter series — any metric
    /// whose name ends in `_reqs` (`http_reqs`, `grpc_reqs`, and plugin
    /// families like `mongo_reqs` / `goecho_reqs`). Used for live and timeline
    /// throughput so non-HTTP protocols aren't reported as zero RPS.
    pub fn interval_request_count(&self) -> u64 {
        self.series
            .iter()
            .filter(|s| crate::metrics::is_request_counter_metric(&s.metric))
            .map(|s| s.interval_count)
            .sum()
    }
}

/// Serializable, mergeable delta for distributed aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesDelta {
    pub metric: String,
    pub kind: MetricKind,
    pub tags: Tags,
    pub data: SeriesDeltaData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesDeltaData {
    Counter {
        delta: f64,
    },
    Gauge {
        last: f64,
        min: f64,
        max: f64,
    },
    Rate {
        passes: u64,
        total: u64,
    },
    /// HDR histogram, V2 encoding, base64; plus float min/max/sum for exactness.
    Trend {
        hdr_b64: String,
        sum: f64,
        count: u64,
        min: f64,
        max: f64,
    },
}

/// A batch of series deltas (one flush interval from one agent).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsDelta {
    pub series: Vec<SeriesDelta>,
}

/// The aggregator.
pub struct Aggregator {
    series: HashMap<SeriesKey, Series>,
    start: Instant,
    last_snapshot: Instant,
    seq: u64,
}

impl Default for Aggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregator {
    pub fn new() -> Self {
        Aggregator {
            series: HashMap::new(),
            start: Instant::now(),
            last_snapshot: Instant::now(),
            seq: 0,
        }
    }

    pub fn record(&mut self, sample: &Sample) {
        self.seq += 1;
        let key = SeriesKey {
            metric: sample.metric.clone(),
            tags: sample.tags.clone(),
        };
        let seq = self.seq;
        let series = self
            .series
            .entry(key)
            .or_insert_with(|| Series::new(sample.kind));
        series.record(sample.value, seq);
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    fn agg_values(series: &Series, elapsed_secs: f64) -> AggValues {
        let mut out = AggValues::default();
        match &series.data {
            SeriesData::Counter { total, .. } => {
                out.sum = *total;
                out.count = *total as u64;
                if elapsed_secs > 0.0 {
                    out.per_second = Some(*total / elapsed_secs);
                }
            }
            SeriesData::Gauge { last, min, max, .. } => {
                out.last = Some(*last);
                out.min = min.is_finite().then_some(*min);
                out.max = max.is_finite().then_some(*max);
                out.count = 1;
            }
            SeriesData::Rate { passes, total, .. } => {
                out.count = *total;
                out.sum = *passes as f64;
                if *total > 0 {
                    out.rate = Some(*passes as f64 / *total as f64);
                }
                if elapsed_secs > 0.0 {
                    out.per_second = Some(*total as f64 / elapsed_secs);
                }
            }
            SeriesData::Trend {
                hist,
                sum,
                count,
                min,
                max,
                ..
            } => {
                out.count = *count;
                out.sum = *sum;
                if *count > 0 {
                    out.avg = Some(*sum / *count as f64);
                    out.min = Some(*min);
                    out.max = Some(*max);
                    out.med = Some(hist.value_at_quantile(0.5) as f64 / TREND_SCALE);
                    out.p90 = Some(hist.value_at_quantile(0.90) as f64 / TREND_SCALE);
                    out.p95 = Some(hist.value_at_quantile(0.95) as f64 / TREND_SCALE);
                    out.p99 = Some(hist.value_at_quantile(0.99) as f64 / TREND_SCALE);
                    out.p999 = Some(hist.value_at_quantile(0.999) as f64 / TREND_SCALE);
                }
                if elapsed_secs > 0.0 {
                    out.per_second = Some(*count as f64 / elapsed_secs);
                }
            }
        }
        out
    }

    fn additive_gauge_values(series: &[&Series]) -> AggValues {
        let mut out = AggValues {
            count: 1,
            ..Default::default()
        };
        let mut last = 0.0;
        let mut min = 0.0;
        let mut max = 0.0;
        let mut has_value = false;
        let mut has_min = false;
        let mut has_max = false;

        for s in series {
            if let SeriesData::Gauge {
                last: l,
                min: m,
                max: x,
                ..
            } = &s.data
            {
                last += *l;
                has_value = true;
                if m.is_finite() {
                    min += *m;
                    has_min = true;
                }
                if x.is_finite() {
                    max += *x;
                    has_max = true;
                }
            }
        }

        out.last = has_value.then_some(last);
        out.min = has_min.then_some(min);
        out.max = has_max.then_some(max);
        out
    }

    /// Produce a snapshot and roll the interval window.
    pub fn snapshot(&mut self) -> Snapshot {
        self.build_snapshot(true)
    }

    /// Produce a cumulative snapshot without advancing the live interval
    /// window. This is intended for secondary projections such as a
    /// controller-owned Prometheus endpoint.
    pub fn cumulative_snapshot(&mut self) -> Snapshot {
        self.build_snapshot(false)
    }

    fn build_snapshot(&mut self, roll_interval: bool) -> Snapshot {
        let elapsed = self.start.elapsed().as_secs_f64();
        let interval = if roll_interval {
            let interval = self.last_snapshot.elapsed().as_secs_f64();
            self.last_snapshot = Instant::now();
            interval
        } else {
            0.0
        };
        let mut out = Vec::with_capacity(self.series.len());
        for (key, series) in self.series.iter_mut() {
            let agg = Self::agg_values(series, elapsed);
            let (interval_count, interval_sum) = if roll_interval {
                match &series.data {
                    SeriesData::Counter { total, .. } => {
                        let d = *total - series.snap_counter;
                        series.snap_counter = *total;
                        (d.max(0.0) as u64, d)
                    }
                    SeriesData::Rate { passes, total, .. } => {
                        let d = total.saturating_sub(series.snap_count);
                        let pass_delta = (*passes as f64 - series.snap_counter).max(0.0);
                        series.snap_count = *total;
                        series.snap_counter = *passes as f64;
                        (d, pass_delta)
                    }
                    SeriesData::Trend { count, .. } => {
                        let d = count.saturating_sub(series.snap_count);
                        series.snap_count = *count;
                        (d, d as f64)
                    }
                    SeriesData::Gauge { .. } => (0, 0.0),
                }
            } else {
                (0, 0.0)
            };
            out.push(SeriesSnapshot {
                metric: key.metric.to_string(),
                kind: series.kind,
                tags: (*key.tags).clone(),
                agg,
                interval_count,
                interval_sum,
            });
        }
        out.sort_by(|a, b| a.metric.cmp(&b.metric).then_with(|| a.tags.cmp(&b.tags)));
        Snapshot {
            timestamp_ms: now_millis(),
            elapsed_secs: elapsed,
            interval_secs: interval,
            series: out,
        }
    }

    /// Metric names currently held by this aggregator, in stable order.
    pub fn metric_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .series
            .keys()
            .map(|key| key.metric.to_string())
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// Merge every series of `metric` whose tags include all of `tag_filter`.
    pub fn aggregate_selector(
        &self,
        metric: &str,
        tag_filter: &[(String, String)],
    ) -> Option<(MetricKind, AggValues)> {
        let elapsed = self.start.elapsed().as_secs_f64();
        let mut matched: Vec<&Series> = Vec::new();
        for (key, series) in &self.series {
            if &*key.metric == metric
                && tag_filter
                    .iter()
                    .all(|(k, v)| key.tags.get(k).map(|tv| tv == v).unwrap_or(false))
            {
                matched.push(series);
            }
        }
        Self::merged_agg(metric, &matched, elapsed)
    }

    /// One-pass all-tags rollup: every metric merged across all of its
    /// series, in stable (sorted) metric order. Equivalent to calling
    /// [`aggregate_selector`](Self::aggregate_selector) with no filter for
    /// every metric name, but scans the series map once instead of once per
    /// metric.
    pub fn aggregate_all(&self) -> Vec<(String, MetricKind, AggValues)> {
        let elapsed = self.start.elapsed().as_secs_f64();
        let mut groups: BTreeMap<&str, Vec<&Series>> = BTreeMap::new();
        for (key, series) in &self.series {
            groups.entry(&key.metric).or_default().push(series);
        }
        groups
            .into_iter()
            .filter_map(|(metric, matched)| {
                Self::merged_agg(metric, &matched, elapsed)
                    .map(|(kind, agg)| (metric.to_string(), kind, agg))
            })
            .collect()
    }

    /// Merge already-matched series of one metric into a single aggregate.
    fn merged_agg(
        metric: &str,
        matched: &[&Series],
        elapsed: f64,
    ) -> Option<(MetricKind, AggValues)> {
        let first = matched.first()?;
        let kind = first.kind;
        if kind == MetricKind::Gauge && is_additive_gauge(metric) {
            return Some((kind, Self::additive_gauge_values(matched)));
        }
        if matched.len() == 1 {
            return Some((kind, Self::agg_values(first, elapsed)));
        }
        let mut merged = Series::new(kind);
        for s in matched {
            merge_series(metric, &mut merged, s);
        }
        Some((kind, Self::agg_values(&merged, elapsed)))
    }

    /// Exact cumulative aggregates for the whole run and selected tag
    /// groupings. Histograms are merged before percentiles are extracted.
    ///
    /// The snapshot includes canonical `request_reqs` and `request_duration`
    /// rollups across primary protocol families. GraphQL's secondary metrics
    /// are excluded because the underlying HTTP request is already present.
    pub fn aggregate_snapshot(&self, groupings: &[&[&str]]) -> Snapshot {
        use std::collections::BTreeSet;

        let elapsed = self.start.elapsed().as_secs_f64();
        let mut selectors: BTreeSet<(String, Tags)> = BTreeSet::new();
        let mut request_filters: BTreeSet<Tags> = BTreeSet::new();
        request_filters.insert(Tags::new());

        for key in self.series.keys() {
            if !matches!(&*key.metric, "request_reqs" | "request_duration") {
                selectors.insert((key.metric.to_string(), Tags::new()));
            }
            let request_metric = crate::metrics::is_request_counter_metric(&key.metric)
                || crate::metrics::is_request_duration_metric(&key.metric);
            for grouping in groupings {
                let Some(tags) = retained_tags(&key.tags, grouping) else {
                    continue;
                };
                if !matches!(&*key.metric, "request_reqs" | "request_duration") {
                    selectors.insert((key.metric.to_string(), tags.clone()));
                }
                if request_metric {
                    request_filters.insert(tags);
                }
            }
        }

        let mut out = Vec::with_capacity(selectors.len() + request_filters.len() * 2);
        for (metric, tags) in selectors {
            let filter: Vec<(String, String)> = tags
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            if let Some((kind, agg)) = self.aggregate_selector(&metric, &filter) {
                out.push(SeriesSnapshot {
                    metric,
                    kind,
                    tags,
                    agg,
                    interval_count: 0,
                    interval_sum: 0.0,
                });
            }
        }

        for tags in request_filters {
            if let Some((kind, agg)) = self.aggregate_matching(&tags, |metric| {
                crate::metrics::is_request_counter_metric(metric)
            }) {
                out.push(SeriesSnapshot {
                    metric: "request_reqs".to_string(),
                    kind,
                    tags: tags.clone(),
                    agg,
                    interval_count: 0,
                    interval_sum: 0.0,
                });
            }
            if let Some((kind, agg)) = self.aggregate_matching(&tags, |metric| {
                crate::metrics::is_request_duration_metric(metric)
            }) {
                out.push(SeriesSnapshot {
                    metric: "request_duration".to_string(),
                    kind,
                    tags,
                    agg,
                    interval_count: 0,
                    interval_sum: 0.0,
                });
            }
        }
        out.sort_by(|a, b| a.metric.cmp(&b.metric).then_with(|| a.tags.cmp(&b.tags)));
        Snapshot {
            timestamp_ms: now_millis(),
            elapsed_secs: elapsed,
            interval_secs: 0.0,
            series: out,
        }
    }

    fn aggregate_matching<F>(
        &self,
        tag_filter: &Tags,
        matches_metric: F,
    ) -> Option<(MetricKind, AggValues)>
    where
        F: Fn(&str) -> bool,
    {
        let elapsed = self.start.elapsed().as_secs_f64();
        let matched: Vec<&Series> = self
            .series
            .iter()
            .filter(|(key, _)| {
                matches_metric(&key.metric)
                    && tag_filter
                        .iter()
                        .all(|(k, v)| key.tags.get(k).is_some_and(|actual| actual == v))
            })
            .map(|(_, series)| series)
            .collect();
        let kind = matched.first()?.kind;
        let mut merged = Series::new(kind);
        for series in matched {
            merge_series("request_rollup", &mut merged, series);
        }
        Some((kind, Self::agg_values(&merged, elapsed)))
    }

    /// Exact percentile for an arbitrary p over a (merged) selector.
    pub fn aggregate_selector_percentile(
        &self,
        metric: &str,
        tag_filter: &[(String, String)],
        percentile: f64,
    ) -> Option<f64> {
        let mut merged: Option<Histogram<u64>> = None;
        let mut any = false;
        for (key, series) in &self.series {
            if &*key.metric == metric
                && tag_filter
                    .iter()
                    .all(|(k, v)| key.tags.get(k).map(|tv| tv == v).unwrap_or(false))
            {
                if let SeriesData::Trend { hist, .. } = &series.data {
                    any = true;
                    match &mut merged {
                        Some(m) => {
                            let _ = m.add(hist);
                        }
                        None => merged = Some(hist.clone()),
                    }
                }
            }
        }
        if !any {
            return None;
        }
        merged.map(|h| h.value_at_quantile(percentile / 100.0) as f64 / TREND_SCALE)
    }

    /// Drain a delta of everything recorded since the previous `take_delta`.
    pub fn take_delta(&mut self) -> MetricsDelta {
        let mut out = Vec::new();
        for (key, series) in self.series.iter_mut() {
            let data = match &mut series.data {
                SeriesData::Counter { total, flushed } => {
                    let delta = *total - *flushed;
                    if delta == 0.0 {
                        continue;
                    }
                    *flushed = *total;
                    SeriesDeltaData::Counter { delta }
                }
                SeriesData::Gauge { last, min, max, .. } => SeriesDeltaData::Gauge {
                    last: *last,
                    min: *min,
                    max: *max,
                },
                SeriesData::Rate {
                    passes,
                    total,
                    flushed_passes,
                    flushed_total,
                } => {
                    let dp = *passes - *flushed_passes;
                    let dt = *total - *flushed_total;
                    if dt == 0 {
                        continue;
                    }
                    *flushed_passes = *passes;
                    *flushed_total = *total;
                    SeriesDeltaData::Rate {
                        passes: dp,
                        total: dt,
                    }
                }
                SeriesData::Trend {
                    delta_hist,
                    sum,
                    count: _,
                    min,
                    max,
                    ..
                } => {
                    if delta_hist.is_empty() {
                        continue;
                    }
                    let mut buf = Vec::new();
                    let mut ser = V2Serializer::new();
                    if ser.serialize(delta_hist, &mut buf).is_err() {
                        continue;
                    }
                    let taken_count = delta_hist.len();
                    *delta_hist = new_histogram();
                    use base64::Engine as _;
                    SeriesDeltaData::Trend {
                        hdr_b64: base64::engine::general_purpose::STANDARD.encode(&buf),
                        // sum/min/max here describe the cumulative series; count is the delta.
                        sum: *sum,
                        count: taken_count,
                        min: *min,
                        max: *max,
                    }
                }
            };
            out.push(SeriesDelta {
                metric: key.metric.to_string(),
                kind: series.kind,
                tags: (*key.tags).clone(),
                data,
            });
        }
        MetricsDelta { series: out }
    }

    /// Undo a `take_delta` that could not be sent on (e.g. a congested
    /// uplink), so the next `take_delta` retransmits it along with anything
    /// recorded since. Restores delta-tracking state only (`flushed`,
    /// `flushed_*`, `delta_hist`) — never the cumulative state that feeds
    /// snapshots, thresholds and the summary, so this is safe to call on the
    /// same aggregator that produced `delta`.
    ///
    /// Gauges have nothing to restore: `take_delta` always re-emits the
    /// current value rather than draining it.
    pub fn restore_delta(&mut self, delta: &MetricsDelta) {
        for sd in &delta.series {
            let key = SeriesKey {
                metric: Arc::from(sd.metric.as_str()),
                tags: Arc::new(sd.tags.clone()),
            };
            // The series was created by the very `record`s this delta was
            // drained from, so it must already exist.
            let Some(series) = self.series.get_mut(&key) else {
                continue;
            };
            match (&mut series.data, &sd.data) {
                (SeriesData::Counter { flushed, .. }, SeriesDeltaData::Counter { delta }) => {
                    *flushed -= delta;
                }
                (
                    SeriesData::Rate {
                        flushed_passes,
                        flushed_total,
                        ..
                    },
                    SeriesDeltaData::Rate { passes, total },
                ) => {
                    *flushed_passes -= passes;
                    *flushed_total -= total;
                }
                (SeriesData::Trend { delta_hist, .. }, SeriesDeltaData::Trend { hdr_b64, .. }) => {
                    use base64::Engine as _;
                    if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(hdr_b64) {
                        let mut deser = HdrDeserializer::new();
                        if let Ok(h) = deser.deserialize::<u64, _>(&mut bytes.as_slice()) {
                            let _ = delta_hist.add(&h);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Override live gauge values matching a trusted tag. Used by the
    /// controller to prevent terminal or lost agents from remaining active.
    pub fn set_gauge_by_tag(&mut self, metric: &str, tag: &str, value: &str, gauge_value: f64) {
        self.seq += 1;
        let seq = self.seq;
        for (key, series) in &mut self.series {
            if &*key.metric != metric || key.tags.get(tag).map(String::as_str) != Some(value) {
                continue;
            }
            if let SeriesData::Gauge {
                last,
                min,
                max,
                seq: gauge_seq,
            } = &mut series.data
            {
                *last = gauge_value;
                *min = min.min(gauge_value);
                *max = max.max(gauge_value);
                *gauge_seq = seq;
            }
        }
    }

    /// Merge a delta from another aggregator (an agent) into this one.
    pub fn merge_delta(&mut self, delta: &MetricsDelta) {
        self.seq += 1;
        let seq = self.seq;
        for sd in &delta.series {
            let key = SeriesKey {
                metric: Arc::from(sd.metric.as_str()),
                tags: Arc::new(sd.tags.clone()),
            };
            let series = self
                .series
                .entry(key)
                .or_insert_with(|| Series::new(sd.kind));
            match (&mut series.data, &sd.data) {
                (SeriesData::Counter { total, .. }, SeriesDeltaData::Counter { delta }) => {
                    *total += delta;
                }
                (
                    SeriesData::Gauge {
                        last,
                        min,
                        max,
                        seq: gseq,
                    },
                    SeriesDeltaData::Gauge {
                        last: l2,
                        min: m2,
                        max: x2,
                    },
                ) => {
                    *last = *l2;
                    *gseq = seq;
                    *min = min.min(*m2);
                    *max = max.max(*x2);
                }
                (
                    SeriesData::Rate { passes, total, .. },
                    SeriesDeltaData::Rate {
                        passes: p2,
                        total: t2,
                    },
                ) => {
                    *passes += p2;
                    *total += t2;
                }
                (
                    SeriesData::Trend {
                        hist,
                        delta_hist,
                        sum,
                        count,
                        min,
                        max,
                    },
                    SeriesDeltaData::Trend {
                        hdr_b64,
                        min: m2,
                        max: x2,
                        ..
                    },
                ) => {
                    use base64::Engine as _;
                    if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(hdr_b64) {
                        let mut deser = HdrDeserializer::new();
                        if let Ok(h) = deser.deserialize::<u64, _>(&mut bytes.as_slice()) {
                            // Reconstruct sum approximately from the histogram so
                            // multi-agent averages stay consistent with percentiles.
                            let added: u64 = h.len();
                            let mean = h.mean() / TREND_SCALE;
                            let _ = hist.add(&h);
                            // Also feed delta_hist so a *second* hop (this
                            // aggregator's own take_delta, e.g. mid-tier
                            // federation) still carries the trend onward —
                            // otherwise it looks empty and gets dropped.
                            let _ = delta_hist.add(&h);
                            *count += added;
                            *sum += mean * added as f64;
                            *min = min.min(*m2);
                            *max = max.max(*x2);
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// Shard-local aggregators for lock-light concurrent recording when no
/// output consumes raw samples (`MetricsBus`'s shard mode). Each shard is an
/// independent `Aggregator`; VUs are pinned to one shard each (see
/// `MetricsBus::for_vu`), so only ~num_cores VUs ever contend on the same
/// shard's lock at once. `drain_into` folds every shard's delta into one
/// target aggregator via the same take_delta/merge_delta federation used for
/// distributed agents — run in-process instead of over the wire.
pub struct MetricShards {
    shards: Vec<Mutex<Aggregator>>,
}

impl MetricShards {
    pub fn new(n: usize) -> Self {
        MetricShards {
            shards: (0..n.max(1))
                .map(|_| Mutex::new(Aggregator::new()))
                .collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    /// Record straight into shard `idx`'s aggregator (`idx % len()`).
    pub fn record(&self, idx: usize, sample: &Sample) {
        self.shards[idx % self.shards.len()].lock().record(sample);
    }

    /// Drain every shard's delta into `target` — one `take_delta` +
    /// `merge_delta` per shard.
    pub fn drain_into(&self, target: &mut Aggregator) {
        for shard in &self.shards {
            let delta = shard.lock().take_delta();
            target.merge_delta(&delta);
        }
    }
}

fn retained_tags(tags: &Tags, grouping: &[&str]) -> Option<Tags> {
    let mut retained = Tags::new();
    for tag in grouping {
        retained.insert((*tag).to_string(), tags.get(*tag)?.clone());
    }
    Some(retained)
}

fn merge_series(metric: &str, merged: &mut Series, source: &Series) {
    match (&mut merged.data, &source.data) {
        (SeriesData::Counter { total, .. }, SeriesData::Counter { total: other, .. }) => {
            *total += other;
        }
        (
            SeriesData::Gauge {
                last,
                min,
                max,
                seq,
            },
            SeriesData::Gauge {
                last: other_last,
                min: other_min,
                max: other_max,
                seq: other_seq,
            },
        ) => {
            if is_additive_gauge(metric) {
                *last += *other_last;
                *seq = (*seq).max(*other_seq);
            } else if *other_seq >= *seq {
                *last = *other_last;
                *seq = *other_seq;
            }
            *min = min.min(*other_min);
            *max = max.max(*other_max);
        }
        (
            SeriesData::Rate { passes, total, .. },
            SeriesData::Rate {
                passes: other_passes,
                total: other_total,
                ..
            },
        ) => {
            *passes += other_passes;
            *total += other_total;
        }
        (
            SeriesData::Trend {
                hist,
                sum,
                count,
                min,
                max,
                ..
            },
            SeriesData::Trend {
                hist: other_hist,
                sum: other_sum,
                count: other_count,
                min: other_min,
                max: other_max,
                ..
            },
        ) => {
            let _ = hist.add(other_hist);
            *sum += other_sum;
            *count += other_count;
            *min = min.min(*other_min);
            *max = max.max(*other_max);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(metric: &str, kind: MetricKind, value: f64, tags: &[(&str, &str)]) -> Sample {
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

    #[test]
    fn trend_percentiles() {
        let mut agg = Aggregator::new();
        for i in 1..=1000 {
            agg.record(&sample("d", MetricKind::Trend, i as f64, &[]));
        }
        let snap = agg.snapshot();
        let s = snap.find("d").expect("series");
        let a = &s.agg;
        assert_eq!(a.count, 1000);
        assert!((a.avg.unwrap() - 500.5).abs() < 0.01);
        assert!((a.med.unwrap() - 500.0).abs() / 500.0 < 0.01);
        assert!((a.p95.unwrap() - 950.0).abs() / 950.0 < 0.01);
        assert!((a.p99.unwrap() - 990.0).abs() / 990.0 < 0.01);
        assert_eq!(a.min, Some(1.0));
        assert_eq!(a.max, Some(1000.0));
    }

    #[test]
    fn counter_and_rate() {
        let mut agg = Aggregator::new();
        agg.record(&sample("reqs", MetricKind::Counter, 1.0, &[]));
        agg.record(&sample("reqs", MetricKind::Counter, 2.0, &[]));
        agg.record(&sample("ok", MetricKind::Rate, 1.0, &[]));
        agg.record(&sample("ok", MetricKind::Rate, 0.0, &[]));
        agg.record(&sample("ok", MetricKind::Rate, 1.0, &[]));
        let snap = agg.snapshot();
        assert_eq!(snap.find("reqs").unwrap().agg.sum, 3.0);
        let rate = snap.find("ok").unwrap().agg.rate.unwrap();
        assert!((rate - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn interval_counts_roll() {
        let mut agg = Aggregator::new();
        agg.record(&sample("reqs", MetricKind::Counter, 5.0, &[]));
        let s1 = agg.snapshot();
        assert_eq!(s1.find("reqs").unwrap().interval_sum, 5.0);
        agg.record(&sample("reqs", MetricKind::Counter, 2.0, &[]));
        let s2 = agg.snapshot();
        assert_eq!(s2.find("reqs").unwrap().interval_sum, 2.0);
        assert_eq!(s2.find("reqs").unwrap().agg.sum, 7.0);
    }

    #[test]
    fn rate_interval_sum_tracks_passing_samples() {
        let mut agg = Aggregator::new();
        agg.record(&sample("failed", MetricKind::Rate, 1.0, &[]));
        agg.record(&sample("failed", MetricKind::Rate, 0.0, &[]));
        agg.record(&sample("failed", MetricKind::Rate, 1.0, &[]));
        let first = agg.snapshot();
        let rate = first.find("failed").unwrap();
        assert_eq!(rate.interval_count, 3);
        assert_eq!(rate.interval_sum, 2.0);

        agg.record(&sample("failed", MetricKind::Rate, 0.0, &[]));
        let second = agg.snapshot();
        let rate = second.find("failed").unwrap();
        assert_eq!(rate.interval_count, 1);
        assert_eq!(rate.interval_sum, 0.0);
    }

    #[test]
    fn selector_merges_tagged_series() {
        let mut agg = Aggregator::new();
        for i in 1..=100 {
            agg.record(&sample(
                "dur",
                MetricKind::Trend,
                i as f64,
                &[("scenario", "a")],
            ));
            agg.record(&sample(
                "dur",
                MetricKind::Trend,
                (i + 100) as f64,
                &[("scenario", "b")],
            ));
        }
        let (_, all) = agg.aggregate_selector("dur", &[]).expect("merged");
        assert_eq!(all.count, 200);
        assert!((all.avg.unwrap() - 100.5).abs() < 0.01);
        let (_, only_a) = agg
            .aggregate_selector("dur", &[("scenario".into(), "a".into())])
            .expect("a");
        assert_eq!(only_a.count, 100);
        assert!((only_a.avg.unwrap() - 50.5).abs() < 0.01);
        // Arbitrary percentile via histogram merge.
        let p42 = agg.aggregate_selector_percentile("dur", &[], 42.0).unwrap();
        assert!((p42 - 84.0).abs() / 84.0 < 0.02, "p42={p42}");
    }

    #[test]
    fn selector_sums_vu_gauges_across_tagged_series() {
        let mut agg = Aggregator::new();
        for instance in ["a", "b", "c"] {
            agg.record(&sample(
                "vus",
                MetricKind::Gauge,
                685.0,
                &[("instance", instance)],
            ));
            agg.record(&sample(
                "vus",
                MetricKind::Gauge,
                3333.0,
                &[("instance", instance)],
            ));
            agg.record(&sample(
                "vus_max",
                MetricKind::Gauge,
                3333.0,
                &[("instance", instance)],
            ));
        }

        let (_, vus) = agg.aggregate_selector("vus", &[]).expect("merged vus");
        assert_eq!(vus.last, Some(9999.0));
        assert_eq!(vus.min, Some(2055.0));
        assert_eq!(vus.max, Some(9999.0));

        let (_, vus_max) = agg
            .aggregate_selector("vus_max", &[])
            .expect("merged vus_max");
        assert_eq!(vus_max.last, Some(9999.0));
        assert_eq!(vus_max.min, Some(9999.0));
        assert_eq!(vus_max.max, Some(9999.0));
    }

    #[test]
    fn selector_keeps_generic_gauge_latest_wins() {
        let mut agg = Aggregator::new();
        agg.record(&sample(
            "queue_depth",
            MetricKind::Gauge,
            10.0,
            &[("instance", "a")],
        ));
        agg.record(&sample(
            "queue_depth",
            MetricKind::Gauge,
            20.0,
            &[("instance", "b")],
        ));

        let (_, gauge) = agg
            .aggregate_selector("queue_depth", &[])
            .expect("merged generic gauge");
        assert_eq!(gauge.last, Some(20.0));
        assert_eq!(gauge.min, Some(10.0));
        assert_eq!(gauge.max, Some(20.0));
    }

    /// The critical distributed-mode property: merged percentiles equal the
    /// percentiles of the union, not the average of per-agent percentiles.
    #[test]
    fn delta_merge_preserves_distribution() {
        // Agent 1 sees fast responses, agent 2 sees slow ones.
        let mut agent1 = Aggregator::new();
        let mut agent2 = Aggregator::new();
        for i in 1..=1000 {
            agent1.record(&sample("d", MetricKind::Trend, i as f64, &[]));
            agent2.record(&sample("d", MetricKind::Trend, (i + 1000) as f64, &[]));
        }
        let mut controller = Aggregator::new();
        controller.merge_delta(&agent1.take_delta());
        controller.merge_delta(&agent2.take_delta());
        let snap = controller.snapshot();
        let a = &snap.find("d").unwrap().agg;
        assert_eq!(a.count, 2000);
        // True p99 of 1..=2000 is 1980; averaging per-agent p99s would give ~1485.
        let p99 = a.p99.unwrap();
        assert!((p99 - 1980.0).abs() / 1980.0 < 0.01, "p99={p99}");
        assert_eq!(a.max, Some(2000.0));
        assert_eq!(a.min, Some(1.0));
    }

    #[test]
    fn delta_merge_sums_vu_gauges_across_agents() {
        let mut controller = Aggregator::new();
        for instance in ["a", "b", "c"] {
            let mut agent = Aggregator::new();
            agent.record(&sample(
                "vus",
                MetricKind::Gauge,
                3333.0,
                &[("instance", instance)],
            ));
            agent.record(&sample(
                "vus_max",
                MetricKind::Gauge,
                3333.0,
                &[("instance", instance)],
            ));
            controller.merge_delta(&agent.take_delta());
        }

        let (_, vus) = controller
            .aggregate_selector("vus", &[])
            .expect("controller vus");
        assert_eq!(vus.last, Some(9999.0));
        let (_, vus_max) = controller
            .aggregate_selector("vus_max", &[])
            .expect("controller vus_max");
        assert_eq!(vus_max.last, Some(9999.0));
    }

    #[test]
    fn take_delta_drains() {
        let mut agg = Aggregator::new();
        agg.record(&sample("c", MetricKind::Counter, 5.0, &[]));
        let d1 = agg.take_delta();
        assert_eq!(d1.series.len(), 1);
        let d2 = agg.take_delta();
        assert!(d2.series.is_empty(), "second delta should be empty");
        agg.record(&sample("c", MetricKind::Counter, 1.0, &[]));
        let d3 = agg.take_delta();
        match &d3.series[0].data {
            SeriesDeltaData::Counter { delta } => assert_eq!(*delta, 1.0),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rate_delta_merge() {
        let mut a1 = Aggregator::new();
        for i in 0..10 {
            a1.record(&sample(
                "checks",
                MetricKind::Rate,
                if i < 9 { 1.0 } else { 0.0 },
                &[],
            ));
        }
        let mut ctrl = Aggregator::new();
        ctrl.merge_delta(&a1.take_delta());
        let (_, agg) = ctrl.aggregate_selector("checks", &[]).unwrap();
        assert!((agg.rate.unwrap() - 0.9).abs() < 1e-9);
    }

    /// Draining a delta (for the distributed uplink) must never perturb the
    /// cumulative state that feeds snapshots, thresholds, and the summary.
    #[test]
    fn take_delta_preserves_cumulative_state() {
        let mut agg = Aggregator::new();
        agg.record(&sample("c", MetricKind::Counter, 5.0, &[]));
        agg.record(&sample("g", MetricKind::Gauge, 3.0, &[]));
        agg.record(&sample("r", MetricKind::Rate, 1.0, &[]));
        agg.record(&sample("r", MetricKind::Rate, 0.0, &[]));
        for i in 1..=50 {
            agg.record(&sample("t", MetricKind::Trend, i as f64, &[]));
        }

        let before = agg.snapshot();
        let drained = agg.take_delta();
        assert!(!drained.series.is_empty(), "drain should carry data");
        let after = agg.snapshot();

        for metric in ["c", "g", "r", "t"] {
            let b = &before.find(metric).expect("before").agg;
            let a = &after.find(metric).expect("after").agg;
            assert_eq!(a.count, b.count, "{metric} count perturbed by take_delta");
            assert_eq!(a.sum, b.sum, "{metric} sum perturbed by take_delta");
            assert_eq!(a.avg, b.avg, "{metric} avg perturbed by take_delta");
            assert_eq!(a.min, b.min, "{metric} min perturbed by take_delta");
            assert_eq!(a.max, b.max, "{metric} max perturbed by take_delta");
            assert_eq!(a.p99, b.p99, "{metric} p99 perturbed by take_delta");
            assert_eq!(a.rate, b.rate, "{metric} rate perturbed by take_delta");
            assert_eq!(a.last, b.last, "{metric} last perturbed by take_delta");
        }
    }

    /// Chained federation (shard → mid → top, e.g. a distributed agent's own
    /// shard aggregators feeding its per-agent aggregator, which then feeds
    /// the controller) must preserve trend statistics through *two* hops of
    /// take_delta/merge_delta, not just one. This fails against the old
    /// `merge_delta`, which feeds `hist` but never `delta_hist`: `mid`'s own
    /// `take_delta()` then finds `delta_hist` empty and silently drops the
    /// series, so `top` never sees it at all.
    #[test]
    fn chained_delta_federation_preserves_trends() {
        let mut shard = Aggregator::new();
        for i in 1..=1000 {
            shard.record(&sample("d", MetricKind::Trend, i as f64, &[]));
        }

        let mut mid = Aggregator::new();
        mid.merge_delta(&shard.take_delta());

        let mut top = Aggregator::new();
        top.merge_delta(&mid.take_delta());

        let snap = top.snapshot();
        let a = &snap
            .find("d")
            .expect("trend series must survive two federation hops")
            .agg;
        assert_eq!(a.count, 1000);
        let p99 = a.p99.unwrap();
        assert!((p99 - 990.0).abs() / 990.0 < 0.01, "p99={p99}");
        assert_eq!(a.min, Some(1.0));
        assert_eq!(a.max, Some(1000.0));
    }

    /// `restore_delta` undoes exactly what `take_delta` drained (the correct
    /// fold-back for a congested uplink): a delta taken after a restore must
    /// carry both the restored data and everything recorded since. This
    /// would fail for trends under the old (buggy) `merge_delta` fold-back,
    /// which feeds `hist` but never `delta_hist` — the restored trend values
    /// would never reappear in a later `take_delta`.
    #[test]
    fn restore_delta_retries_unsent() {
        let mut agg = Aggregator::new();
        agg.record(&sample("c", MetricKind::Counter, 5.0, &[]));
        agg.record(&sample("g", MetricKind::Gauge, 3.0, &[]));
        agg.record(&sample("r", MetricKind::Rate, 1.0, &[]));
        agg.record(&sample("r", MetricKind::Rate, 0.0, &[]));
        agg.record(&sample("t", MetricKind::Trend, 10.0, &[]));
        agg.record(&sample("t", MetricKind::Trend, 20.0, &[]));

        let d1 = agg.take_delta();
        agg.restore_delta(&d1);

        agg.record(&sample("c", MetricKind::Counter, 2.0, &[]));
        agg.record(&sample("g", MetricKind::Gauge, 9.0, &[]));
        agg.record(&sample("r", MetricKind::Rate, 1.0, &[]));
        agg.record(&sample("t", MetricKind::Trend, 30.0, &[]));

        let d2 = agg.take_delta();

        // The delta of just the extra records, from an independent aggregator.
        let mut extra = Aggregator::new();
        extra.record(&sample("c", MetricKind::Counter, 2.0, &[]));
        extra.record(&sample("g", MetricKind::Gauge, 9.0, &[]));
        extra.record(&sample("r", MetricKind::Rate, 1.0, &[]));
        extra.record(&sample("t", MetricKind::Trend, 30.0, &[]));
        let extra_delta = extra.take_delta();

        let mut via_d2 = Aggregator::new();
        via_d2.merge_delta(&d2);

        let mut via_d1_plus_extra = Aggregator::new();
        via_d1_plus_extra.merge_delta(&d1);
        via_d1_plus_extra.merge_delta(&extra_delta);

        let snap_d2 = via_d2.snapshot();
        let snap_combo = via_d1_plus_extra.snapshot();
        for metric in ["c", "g", "r", "t"] {
            let x = &snap_d2.find(metric).expect("d2").agg;
            let y = &snap_combo.find(metric).expect("combo").agg;
            assert_eq!(x.count, y.count, "{metric} count: d2 vs d1+extra");
            assert_eq!(x.sum, y.sum, "{metric} sum: d2 vs d1+extra");
            assert_eq!(x.min, y.min, "{metric} min: d2 vs d1+extra");
            assert_eq!(x.max, y.max, "{metric} max: d2 vs d1+extra");
            assert_eq!(x.last, y.last, "{metric} last: d2 vs d1+extra");
            assert_eq!(x.rate, y.rate, "{metric} rate: d2 vs d1+extra");
            assert_eq!(x.p99, y.p99, "{metric} p99: d2 vs d1+extra");
        }

        // The source agg's own cumulative state must not be perturbed by the
        // take -> restore -> take round trip (no double counting).
        let src = agg.snapshot();
        assert_eq!(src.find("c").unwrap().agg.sum, 7.0);
        assert_eq!(src.find("g").unwrap().agg.last, Some(9.0));
        let r = &src.find("r").unwrap().agg;
        assert_eq!(r.count, 3);
        assert!((r.rate.unwrap() - 2.0 / 3.0).abs() < 1e-9);
        let t = &src.find("t").unwrap().agg;
        assert_eq!(t.count, 3);
        assert_eq!(t.sum, 60.0);
        assert_eq!(t.min, Some(10.0));
        assert_eq!(t.max, Some(30.0));
    }

    #[test]
    fn additive_fleet_gauges_sum_tagged_agents() {
        let mut agg = Aggregator::new();
        agg.record(&sample(
            "vus",
            MetricKind::Gauge,
            3.0,
            &[("loadr_agent_id", "a")],
        ));
        agg.record(&sample(
            "vus",
            MetricKind::Gauge,
            5.0,
            &[("loadr_agent_id", "b")],
        ));
        let (_, fleet) = agg.aggregate_selector("vus", &[]).expect("fleet VUs");
        assert_eq!(fleet.last, Some(8.0));
    }

    #[test]
    fn cumulative_snapshot_does_not_roll_interval_window() {
        let mut agg = Aggregator::new();
        agg.record(&sample("reqs", MetricKind::Counter, 5.0, &[]));
        let cumulative = agg.cumulative_snapshot();
        assert_eq!(cumulative.find("reqs").unwrap().agg.sum, 5.0);
        assert_eq!(cumulative.find("reqs").unwrap().interval_sum, 0.0);
        let live = agg.snapshot();
        assert_eq!(live.find("reqs").unwrap().interval_sum, 5.0);
    }

    #[test]
    fn aggregate_all_matches_per_metric_selectors() {
        let mut agg = Aggregator::new();
        for i in 1..=100 {
            agg.record(&sample(
                "dur",
                MetricKind::Trend,
                i as f64,
                &[("agent", "a")],
            ));
            agg.record(&sample(
                "dur",
                MetricKind::Trend,
                (i + 100) as f64,
                &[("agent", "b")],
            ));
        }
        agg.record(&sample("reqs", MetricKind::Counter, 3.0, &[("agent", "a")]));
        agg.record(&sample("reqs", MetricKind::Counter, 4.0, &[("agent", "b")]));
        agg.record(&sample("vus", MetricKind::Gauge, 2.0, &[("agent", "a")]));
        agg.record(&sample("vus", MetricKind::Gauge, 5.0, &[("agent", "b")]));

        let all = agg.aggregate_all();
        let names: Vec<&str> = all.iter().map(|(m, _, _)| m.as_str()).collect();
        assert_eq!(names, ["dur", "reqs", "vus"], "sorted metric order");
        for (metric, kind, values) in &all {
            let (sel_kind, sel) = agg.aggregate_selector(metric, &[]).expect("selector");
            assert_eq!(*kind, sel_kind);
            assert_eq!(values.count, sel.count);
            assert_eq!(values.sum, sel.sum);
            assert_eq!(values.last, sel.last);
            assert_eq!(values.p99, sel.p99);
        }
        // vus is additive across agents.
        let vus = &all.iter().find(|(m, _, _)| m == "vus").unwrap().2;
        assert_eq!(vus.last, Some(7.0));
    }

    #[test]
    fn controller_can_zero_one_agents_live_gauge() {
        let mut agg = Aggregator::new();
        agg.record(&sample(
            "vus",
            MetricKind::Gauge,
            3.0,
            &[("loadr_agent_id", "a")],
        ));
        agg.record(&sample(
            "vus",
            MetricKind::Gauge,
            5.0,
            &[("loadr_agent_id", "b")],
        ));
        agg.set_gauge_by_tag("vus", "loadr_agent_id", "a", 0.0);
        let (_, fleet) = agg.aggregate_selector("vus", &[]).expect("fleet VUs");
        assert_eq!(fleet.last, Some(5.0));
    }
}
