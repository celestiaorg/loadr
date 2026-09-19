//! `observe` — pull system metrics in for load↔system correlation.
//!
//! The inverse of the push-based outputs: for the run window, query the
//! configured sources (Prometheus first), normalize the response into a simple
//! time-series form, and overlay it on the run timeline. A source that is
//! unreachable or returns garbage is logged and skipped — it never fails the
//! load test.
//!
//! Collection is *post-run* (one range query per source over
//! `[started_ms, ended_ms]`); live streaming into the engine snapshot loop is a
//! later phase. The local `system` source is the one exception: `/proc` can't
//! be range-queried retroactively, so it is sampled in the background during
//! the run ([`start_samplers`] / [`stop_samplers`]) and drained into the same
//! series shape at run end.

use crate::http_client;
use http::{HeaderName, HeaderValue, Uri};
use loadr_config::ObserveConfig;
use loadr_core::{AggValues, MetricKind, Summary, ThresholdStatus};

/// A normalized external metric series: time-ordered `(unix_ms, value)` points.
#[derive(Debug, Clone)]
pub struct ObservedSeries {
    /// Canonical metric name (e.g. `system_cpu`).
    pub name: String,
    /// Unit hint for axis formatting (`ratio`, `bytes`, …); empty if unknown.
    pub unit: String,
    /// Samples, ascending by timestamp.
    pub points: Vec<(i64, f64)>,
}

/// Pick a sensible range-query step (seconds) from the run's timeline cadence.
pub fn step_for(timeline: &[loadr_core::summary::TimelinePoint]) -> u64 {
    if timeline.len() >= 2 {
        let gap = (timeline[1].elapsed_secs - timeline[0].elapsed_secs).round();
        (gap as i64).clamp(1, 3600) as u64
    } else {
        1
    }
}

/// Collect every configured source over `[start_ms, end_ms]` at `step_secs`
/// resolution. Per-source failures are logged and skipped.
pub async fn collect(
    configs: &[ObserveConfig],
    start_ms: i64,
    end_ms: i64,
    step_secs: u64,
) -> Vec<ObservedSeries> {
    let client = http_client::client();
    let mut out = Vec::new();
    for cfg in configs {
        match cfg {
            ObserveConfig::Prometheus {
                name,
                source,
                query,
                as_name,
                unit,
                token,
            } => {
                let label = as_name
                    .clone()
                    .or_else(|| name.clone())
                    .unwrap_or_else(|| sanitize(query));
                match prometheus_range(
                    &client,
                    source,
                    query,
                    token.as_deref(),
                    start_ms,
                    end_ms,
                    step_secs,
                )
                .await
                {
                    Ok(series) => {
                        // One PromQL expr can return several label sets; suffix
                        // all but the first so names stay unique.
                        for (i, points) in series.into_iter().enumerate() {
                            let name = if i == 0 {
                                label.clone()
                            } else {
                                format!("{label}_{i}")
                            };
                            out.push(ObservedSeries {
                                name,
                                unit: unit.clone().unwrap_or_default(),
                                points,
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "observe: prometheus source '{}' failed: {e}",
                            name.as_deref().unwrap_or(query)
                        );
                    }
                }
            }
            // `system` sources are background samplers, not range queries —
            // started via [`start_samplers`] and drained via [`stop_samplers`].
            ObserveConfig::System { .. } => {}
        }
    }
    out
}

/// Resample each series onto the run timeline (nearest sample per point) and
/// write the values into each `TimelinePoint::external`.
pub fn attach(summary: &mut Summary, series: &[ObservedSeries]) {
    if series.is_empty() || summary.timeline.is_empty() {
        return;
    }
    let start_ms = summary.started_ms as i64;
    for s in series {
        if s.points.is_empty() {
            continue;
        }
        // Tolerance: don't fill across gaps wider than ~2 sample spacings.
        let spacing = if s.points.len() >= 2 {
            (s.points[s.points.len() - 1].0 - s.points[0].0) / (s.points.len() as i64 - 1)
        } else {
            5_000
        };
        let tol = (spacing * 2).max(5_000);
        for p in &mut summary.timeline {
            let abs = start_ms + (p.elapsed_secs * 1000.0) as i64;
            if let Some(v) = nearest(&s.points, abs, tol) {
                p.external.insert(s.name.clone(), v);
            }
        }
    }
}

/// Nearest sample value to `target_ms` within `tol_ms`, else `None`.
fn nearest(points: &[(i64, f64)], target_ms: i64, tol_ms: i64) -> Option<f64> {
    let mut best: Option<(i64, f64)> = None;
    for &(ts, v) in points {
        let d = (ts - target_ms).abs();
        if d <= tol_ms && best.is_none_or(|(bd, _)| d < bd) {
            best = Some((d, v));
        }
    }
    best.map(|(_, v)| v)
}

/// Run a Prometheus range query and return one `Vec<(unix_ms, value)>` per
/// returned series.
async fn prometheus_range(
    client: &http_client::HttpClient,
    source: &str,
    query: &str,
    token: Option<&str>,
    start_ms: i64,
    end_ms: i64,
    step_secs: u64,
) -> Result<Vec<Vec<(i64, f64)>>, String> {
    let base = source.trim_end_matches('/');
    let url = format!(
        "{base}/api/v1/query_range?query={q}&start={start}&end={end}&step={step}",
        q = percent_encode(query),
        start = start_ms / 1000,
        end = end_ms / 1000,
        step = step_secs.max(1),
    );
    let uri: Uri = url.parse().map_err(|e| format!("bad url {url}: {e}"))?;

    let mut headers: Vec<(HeaderName, HeaderValue)> = Vec::new();
    if let Some(tok) = token {
        if let Ok(v) = HeaderValue::from_str(&format!("Bearer {tok}")) {
            headers.push((http::header::AUTHORIZATION, v));
        }
    }

    let (status, body) = http_client::get(client, &uri, &headers).await?;
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    let json: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| format!("invalid JSON: {e}"))?;
    Ok(parse_matrix(&json))
}

/// Parse a Prometheus `query_range` matrix response into per-series points.
/// Tolerant: anything unexpected yields an empty result rather than erroring.
fn parse_matrix(json: &serde_json::Value) -> Vec<Vec<(i64, f64)>> {
    let result = match json.get("data").and_then(|d| d.get("result")) {
        Some(serde_json::Value::Array(a)) => a,
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    for entry in result {
        let Some(values) = entry.get("values").and_then(|v| v.as_array()) else {
            continue;
        };
        let mut points = Vec::with_capacity(values.len());
        for pair in values {
            let Some(arr) = pair.as_array() else { continue };
            if arr.len() != 2 {
                continue;
            }
            let ts = arr[0].as_f64();
            let val = arr[1].as_str().and_then(|s| s.parse::<f64>().ok());
            if let (Some(ts), Some(val)) = (ts, val) {
                if val.is_finite() {
                    points.push(((ts * 1000.0) as i64, val));
                }
            }
        }
        if !points.is_empty() {
            out.push(points);
        }
    }
    out
}

/// Minimal percent-encoding for a URL query component (RFC 3986 unreserved set).
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Fall back name from a PromQL query: keep it short and identifier-ish.
fn sanitize(query: &str) -> String {
    let s: String = query
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    let s = s.trim_matches('_');
    s.chars().take(40).collect()
}

/// Evaluate plan thresholds that target an observed (`observe:`) metric, against
/// the collected series — *post-run*. This lets a run be gated on the target's
/// health (`system_cpu: ["max<0.9"]`) using the ordinary threshold syntax.
///
/// Only thresholds whose metric name matches a collected series are handled
/// here; everything else is a load metric the engine already evaluated. Returns
/// one [`ThresholdStatus`] per matching expression so the caller can fold them
/// into the summary (replacing the engine's no-sample placeholders) and
/// recompute pass/fail.
///
/// Note: this is end-of-run gating, not live `abort_on_fail` — system metrics
/// aren't in the engine's live aggregator yet (a later, streaming phase).
pub fn evaluate_thresholds(
    thresholds: &indexmap::IndexMap<String, loadr_config::ThresholdList>,
    series: &[ObservedSeries],
) -> Vec<ThresholdStatus> {
    let mut out = Vec::new();
    for (key, list) in thresholds {
        let Ok(sel) = loadr_config::MetricSelector::parse(key) else {
            continue;
        };
        // Observed series carry no tags in this phase.
        if !sel.tags.is_empty() {
            continue;
        }
        let Some(s) = series.iter().find(|s| s.name == sel.metric) else {
            continue; // a load metric — the engine handled it
        };
        let Some(agg) = agg_values(s) else { continue };
        for entry in list.entries() {
            let Ok(expr) = loadr_config::ThresholdExpr::parse(entry.expression()) else {
                continue;
            };
            // Treat observed series as gauges (last/min/max/avg/percentiles).
            let observed = agg.value_for(&expr.agg, MetricKind::Gauge);
            let passed = observed.is_none_or(|v| expr.op.eval(v, expr.bound));
            out.push(ThresholdStatus {
                metric: sel.to_string(),
                expression: entry.expression().to_string(),
                observed,
                passed,
                abort_on_fail: entry.abort_on_fail(),
            });
        }
    }
    out
}

/// Build an [`AggValues`] (gauge-style) from a series' values for threshold eval.
fn agg_values(s: &ObservedSeries) -> Option<AggValues> {
    if s.points.is_empty() {
        return None;
    }
    let mut vals: Vec<f64> = s.points.iter().map(|(_, v)| *v).collect();
    let count = vals.len() as u64;
    let sum: f64 = vals.iter().sum();
    let avg = sum / count as f64;
    let last = s.points.last().map(|(_, v)| *v); // series is time-ordered
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pct = |p: f64| -> f64 {
        let rank = ((p / 100.0) * vals.len() as f64).ceil() as usize;
        vals[rank.saturating_sub(1).min(vals.len() - 1)]
    };
    Some(AggValues {
        count,
        sum,
        avg: Some(avg),
        min: vals.first().copied(),
        max: vals.last().copied(),
        med: Some(pct(50.0)),
        p90: Some(pct(90.0)),
        p95: Some(pct(95.0)),
        p99: Some(pct(99.0)),
        p999: Some(pct(99.9)),
        rate: None,
        last,
        per_second: Some(avg), // best-effort for `rate`-style series
    })
}

// ---------------------------------------------------------------------------
// `type: system` — background sampler for the local host's CPU / memory /
// disk / network. Unlike the pull sources above there is no backend to query
// after the run, so one task per source samples `/proc` every interval into a
// bounded in-memory ring, drained into ordinary [`ObservedSeries`] at run end.
// ---------------------------------------------------------------------------

/// Handles to the background samplers spawned for `type: system` observe
/// sources. Created by [`start_samplers`] at run start; the tasks are stopped
/// and their rings drained by [`stop_samplers`] at run end.
#[derive(Debug, Default)]
pub struct SystemSamplerHandles {
    samplers: Vec<system::Sampler>,
}

/// Start one background sampler per `type: system` observe source. Must run
/// at run start (local metrics can't be collected retroactively) and be paired
/// with [`stop_samplers`] at run end. Non-`system` sources are ignored.
#[cfg(target_os = "linux")]
pub fn start_samplers(configs: &[ObserveConfig]) -> SystemSamplerHandles {
    let mut samplers = Vec::new();
    for cfg in configs {
        let ObserveConfig::System {
            metrics,
            interval,
            as_prefix,
        } = cfg
        else {
            continue;
        };
        let enabled = system::enabled_metrics(metrics);
        if enabled.is_empty() {
            tracing::warn!("observe: system source has no known metrics; skipping");
            continue;
        }
        let prefix = as_prefix.as_deref().unwrap_or("system");
        // Default 1s; floor at 100ms — finer sampling only amplifies noise.
        let every = interval
            .map_or(std::time::Duration::from_secs(1), |d| d.as_duration())
            .max(std::time::Duration::from_millis(100));
        samplers.push(system::spawn(prefix, &enabled, every));
    }
    SystemSamplerHandles { samplers }
}

/// Non-Linux: the `system` source reads `/proc`, so it is unsupported here —
/// log a warning and start nothing (unsupported platforms yield empty series,
/// never errors).
#[cfg(not(target_os = "linux"))]
pub fn start_samplers(configs: &[ObserveConfig]) -> SystemSamplerHandles {
    if configs
        .iter()
        .any(|c| matches!(c, ObserveConfig::System { .. }))
    {
        tracing::warn!("observe: 'system' source is Linux-only; no local series collected");
    }
    SystemSamplerHandles::default()
}

/// Stop every sampler and drain its ring into [`ObservedSeries`] — the same
/// shape [`collect`] produces, so [`attach`] and [`evaluate_thresholds`] apply
/// to system series unchanged. Series that never received a sample are dropped.
pub fn stop_samplers(handles: SystemSamplerHandles) -> Vec<ObservedSeries> {
    let mut out = Vec::new();
    for s in handles.samplers {
        s.task.abort();
        for rs in s.ring.lock().iter() {
            if rs.points.is_empty() {
                continue;
            }
            out.push(ObservedSeries {
                name: rs.name.clone(),
                unit: rs.unit.clone(),
                points: rs.points.iter().copied().collect(),
            });
        }
    }
    out
}

/// `system`-source internals: pure `/proc` parsers plus the sampling task.
/// Everything except `spawn`/`sample_loop` is platform-independent (`&str` in,
/// values out) so the unit tests run on any host.
#[cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]
mod system {
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::sync::Arc;

    /// Cap on samples kept per series (~24h at the default 1s interval).
    pub(super) const RING_CAP: usize = 86_400;

    /// One running sampler task plus the ring it fills.
    #[derive(Debug)]
    pub(super) struct Sampler {
        pub(super) task: tokio::task::JoinHandle<()>,
        pub(super) ring: SharedRing,
    }

    /// Ring buffers shared between the sampler task and the drain.
    pub(super) type SharedRing = Arc<Mutex<Vec<RingSeries>>>;

    /// A bounded, time-ordered sample buffer for one series.
    #[derive(Debug)]
    pub(super) struct RingSeries {
        pub(super) metric: Metric,
        pub(super) name: String,
        pub(super) unit: String,
        pub(super) points: VecDeque<(i64, f64)>,
    }

    impl RingSeries {
        /// Append a sample, dropping the oldest once [`RING_CAP`] is reached.
        pub(super) fn push(&mut self, ts_ms: i64, value: f64) {
            while self.points.len() >= RING_CAP {
                self.points.pop_front();
            }
            self.points.push_back((ts_ms, value));
        }
    }

    /// The four local metrics a `system` source can sample.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Metric {
        Cpu,
        Memory,
        DiskIo,
        Network,
    }

    impl Metric {
        /// Series-name suffix under the configured prefix.
        fn suffix(self) -> &'static str {
            match self {
                Metric::Cpu => "cpu",
                Metric::Memory => "memory",
                Metric::DiskIo => "disk_io",
                Metric::Network => "network",
            }
        }

        /// Unit hint: `ratio` for 0..1 fractions, `bytes` for byte rates.
        fn unit(self) -> &'static str {
            match self {
                Metric::Cpu | Metric::Memory => "ratio",
                Metric::DiskIo | Metric::Network => "bytes",
            }
        }
    }

    /// Resolve the plan's `metrics:` list. Empty means all four; unknown names
    /// are logged and ignored; duplicates collapse.
    pub(super) fn enabled_metrics(metrics: &[String]) -> Vec<Metric> {
        if metrics.is_empty() {
            return vec![Metric::Cpu, Metric::Memory, Metric::DiskIo, Metric::Network];
        }
        let mut out = Vec::new();
        for m in metrics {
            let metric = match m.as_str() {
                "cpu" => Metric::Cpu,
                "memory" => Metric::Memory,
                "disk" => Metric::DiskIo,
                "network" => Metric::Network,
                other => {
                    tracing::warn!(
                        "observe: unknown system metric '{other}'; want cpu|memory|disk|network"
                    );
                    continue;
                }
            };
            if !out.contains(&metric) {
                out.push(metric);
            }
        }
        out
    }

    /// Build the (empty) ring series for the enabled metrics under `prefix`.
    pub(super) fn ring_for(prefix: &str, metrics: &[Metric]) -> Vec<RingSeries> {
        metrics
            .iter()
            .map(|&m| RingSeries {
                metric: m,
                name: format!("{prefix}_{}", m.suffix()),
                unit: m.unit().to_string(),
                points: VecDeque::new(),
            })
            .collect()
    }

    /// Spawn the background sampling task for one `system` source.
    #[cfg(target_os = "linux")]
    pub(super) fn spawn(prefix: &str, metrics: &[Metric], every: std::time::Duration) -> Sampler {
        let ring: SharedRing = Arc::new(Mutex::new(ring_for(prefix, metrics)));
        let task_ring = Arc::clone(&ring);
        let task = tokio::spawn(sample_loop(task_ring, every));
        Sampler { task, ring }
    }

    /// Sample `/proc` every `every`, pushing one point per enabled series,
    /// until the task is aborted by [`super::stop_samplers`].
    #[cfg(target_os = "linux")]
    async fn sample_loop(ring: SharedRing, every: std::time::Duration) {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // the first tick fires immediately — it's the baseline
        let mut prev = read_snapshot();
        loop {
            tick.tick().await;
            let cur = read_snapshot();
            push_values(&ring, cur.at_ms, &interval_values(&prev, &cur));
            prev = cur;
        }
    }

    /// Append one interval's values to whichever series are enabled.
    pub(super) fn push_values(ring: &SharedRing, at_ms: i64, vals: &IntervalValues) {
        let mut ring = ring.lock();
        for rs in ring.iter_mut() {
            if let Some(v) = vals.value(rs.metric) {
                rs.push(at_ms, v);
            }
        }
    }

    /// One pass over `/proc`; a file that can't be read or parsed just yields
    /// `None` for its metric.
    #[cfg(target_os = "linux")]
    fn read_snapshot() -> ProcSnapshot {
        let read = |path: &str| std::fs::read_to_string(path).ok();
        ProcSnapshot {
            at_ms: loadr_core::metrics::now_millis() as i64,
            cpu: read("/proc/stat").as_deref().and_then(parse_proc_stat),
            mem_ratio: read("/proc/meminfo").as_deref().and_then(parse_meminfo),
            disk_sectors: read("/proc/diskstats").as_deref().and_then(parse_diskstats),
            net_bytes: read("/proc/net/dev").as_deref().and_then(parse_net_dev),
        }
    }

    /// Raw counter readings from one pass over `/proc`, taken at `at_ms`.
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub(super) struct ProcSnapshot {
        pub(super) at_ms: i64,
        pub(super) cpu: Option<CpuTimes>,
        pub(super) mem_ratio: Option<f64>,
        pub(super) disk_sectors: Option<u64>,
        pub(super) net_bytes: Option<u64>,
    }

    /// Cumulative jiffies from the aggregate `cpu` line of `/proc/stat`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct CpuTimes {
        pub(super) busy: u64,
        pub(super) total: u64,
    }

    /// Per-interval metric values; `None` where a reading was unavailable.
    #[derive(Debug, Clone, Copy, Default, PartialEq)]
    pub(super) struct IntervalValues {
        pub(super) cpu: Option<f64>,
        pub(super) memory: Option<f64>,
        pub(super) disk_io: Option<f64>,
        pub(super) network: Option<f64>,
    }

    impl IntervalValues {
        fn value(&self, m: Metric) -> Option<f64> {
            match m {
                Metric::Cpu => self.cpu,
                Metric::Memory => self.memory,
                Metric::DiskIo => self.disk_io,
                Metric::Network => self.network,
            }
        }
    }

    /// Derive the interval `prev → cur`'s values. Delta-based metrics (cpu,
    /// disk, network) go `None` on a counter reset (value moving backwards) or
    /// a zero-length interval; memory is instantaneous.
    pub(super) fn interval_values(prev: &ProcSnapshot, cur: &ProcSnapshot) -> IntervalValues {
        let dt = (cur.at_ms - prev.at_ms) as f64 / 1000.0;
        if dt <= 0.0 {
            return IntervalValues {
                memory: cur.mem_ratio,
                ..Default::default()
            };
        }
        let cpu = match (prev.cpu, cur.cpu) {
            (Some(p), Some(c)) if c.total > p.total && c.busy >= p.busy => {
                Some(((c.busy - p.busy) as f64 / (c.total - p.total) as f64).clamp(0.0, 1.0))
            }
            _ => None,
        };
        let rate = |p: Option<u64>, c: Option<u64>, scale: f64| match (p, c) {
            (Some(p), Some(c)) if c >= p => Some((c - p) as f64 * scale / dt),
            _ => None,
        };
        IntervalValues {
            cpu,
            memory: cur.mem_ratio,
            // /proc/diskstats counts 512-byte sectors regardless of hardware.
            disk_io: rate(prev.disk_sectors, cur.disk_sectors, 512.0),
            network: rate(prev.net_bytes, cur.net_bytes, 1.0),
        }
    }

    /// Parse the aggregate `cpu` line of `/proc/stat` into busy/total jiffies.
    /// Busy = total − (idle + iowait), over the first eight time fields.
    pub(super) fn parse_proc_stat(s: &str) -> Option<CpuTimes> {
        let line = s.lines().find(|l| l.starts_with("cpu "))?;
        let fields = line
            .split_whitespace()
            .skip(1)
            .map(|t| t.parse::<u64>().ok())
            .collect::<Option<Vec<u64>>>()?;
        // user nice system idle iowait irq softirq steal [guest guest_nice]
        if fields.len() < 5 {
            return None;
        }
        let total: u64 = fields.iter().take(8).sum();
        let idle = fields[3] + fields[4];
        Some(CpuTimes {
            busy: total.saturating_sub(idle),
            total,
        })
    }

    /// Parse `/proc/meminfo` into a used/total ratio (`MemAvailable`-based).
    pub(super) fn parse_meminfo(s: &str) -> Option<f64> {
        let mut total = None;
        let mut available = None;
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                total = first_u64(rest);
            } else if let Some(rest) = line.strip_prefix("MemAvailable:") {
                available = first_u64(rest);
            }
        }
        let (total, available) = (total?, available?);
        if total == 0 {
            return None;
        }
        Some(total.saturating_sub(available) as f64 / total as f64)
    }

    /// First whitespace-separated integer in `s`.
    fn first_u64(s: &str) -> Option<u64> {
        s.split_whitespace().next()?.parse().ok()
    }

    /// Parse `/proc/diskstats` into total sectors read+written across physical
    /// devices. Partitions and virtual devices (`loop*`, `ram*`, `zram*`,
    /// `dm-*`, `md*`) are skipped so I/O isn't double-counted.
    pub(super) fn parse_diskstats(s: &str) -> Option<u64> {
        let mut devices: Vec<String> = Vec::new();
        let mut sectors: u64 = 0;
        let mut saw_any = false;
        for line in s.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 10 {
                continue;
            }
            let name = f[2];
            if ["loop", "ram", "zram", "dm-", "md"]
                .iter()
                .any(|p| name.starts_with(p))
            {
                continue;
            }
            if devices.iter().any(|d| is_partition_of(name, d)) {
                continue;
            }
            // f[5] = sectors read, f[9] = sectors written.
            let (Ok(read), Ok(written)) = (f[5].parse::<u64>(), f[9].parse::<u64>()) else {
                continue;
            };
            devices.push(name.to_string());
            sectors += read + written;
            saw_any = true;
        }
        saw_any.then_some(sectors)
    }

    /// True if `name` names a partition of `device`: the device name followed
    /// by digits (`sda1`) or `p` + digits (`nvme0n1p1`). `sdab` is *not* a
    /// partition of `sda`.
    pub(super) fn is_partition_of(name: &str, device: &str) -> bool {
        let Some(rest) = name.strip_prefix(device) else {
            return false;
        };
        if rest.is_empty() {
            return false;
        }
        let rest = rest.strip_prefix('p').unwrap_or(rest);
        !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit())
    }

    /// Parse `/proc/net/dev` into total rx+tx bytes across non-loopback
    /// interfaces. Header lines fail the numeric parse and fall through.
    pub(super) fn parse_net_dev(s: &str) -> Option<u64> {
        let mut bytes: u64 = 0;
        let mut saw_any = false;
        for line in s.lines() {
            let Some((iface, rest)) = line.split_once(':') else {
                continue;
            };
            if iface.trim() == "lo" {
                continue;
            }
            let f: Vec<&str> = rest.split_whitespace().collect();
            if f.len() < 9 {
                continue;
            }
            // f[0] = rx bytes, f[8] = tx bytes.
            let (Ok(rx), Ok(tx)) = (f[0].parse::<u64>(), f[8].parse::<u64>()) else {
                continue;
            };
            bytes += rx + tx;
            saw_any = true;
        }
        saw_any.then_some(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_matrix_response() {
        let json: serde_json::Value = serde_json::from_str(
            r#"{"status":"success","data":{"resultType":"matrix","result":[
                {"metric":{"instance":"api-1"},"values":[[1700000000,"0.12"],[1700000001,"0.40"],[1700000002,"NaN"]]}
            ]}}"#,
        )
        .unwrap();
        let series = parse_matrix(&json);
        assert_eq!(series.len(), 1);
        // NaN dropped, two finite points kept, ms-scaled.
        assert_eq!(
            series[0],
            vec![(1_700_000_000_000, 0.12), (1_700_000_001_000, 0.40)]
        );
    }

    #[test]
    fn empty_or_garbage_yields_no_series() {
        assert!(parse_matrix(&serde_json::json!({})).is_empty());
        assert!(parse_matrix(&serde_json::json!({"data":{"result":"nope"}})).is_empty());
    }

    #[test]
    fn nearest_respects_tolerance() {
        let pts = vec![(1000, 1.0), (2000, 2.0), (3000, 3.0)];
        assert_eq!(nearest(&pts, 2100, 500), Some(2.0)); // closest is 2000
        assert_eq!(nearest(&pts, 9000, 500), None); // beyond tolerance
    }

    #[test]
    fn attach_resamples_onto_timeline() {
        let mut summary = Summary {
            name: None,
            run_id: "r".into(),
            started_ms: 1_700_000_000_000,
            ended_ms: 1_700_000_003_000,
            duration_secs: 3.0,
            scenarios: vec![],
            metrics: vec![],
            checks: vec![],
            thresholds: vec![],
            thresholds_passed: true,
            aborted: None,
            snapshot: Default::default(),
            timeline: vec![tp(0.0), tp(1.0), tp(2.0)],
            jobs: vec![],
        };
        let series = vec![ObservedSeries {
            name: "system_cpu".into(),
            unit: "ratio".into(),
            points: vec![
                (1_700_000_000_000, 0.10),
                (1_700_000_001_000, 0.55),
                (1_700_000_002_000, 0.90),
            ],
        }];
        attach(&mut summary, &series);
        assert_eq!(summary.timeline[0].external.get("system_cpu"), Some(&0.10));
        assert_eq!(summary.timeline[1].external.get("system_cpu"), Some(&0.55));
        assert_eq!(summary.timeline[2].external.get("system_cpu"), Some(&0.90));
    }

    #[test]
    fn evaluates_observe_thresholds_post_run() {
        let series = vec![ObservedSeries {
            name: "system_cpu".into(),
            unit: "ratio".into(),
            points: vec![(0, 0.20), (1000, 0.60), (2000, 0.97)],
        }];
        let mut th: indexmap::IndexMap<String, loadr_config::ThresholdList> =
            indexmap::IndexMap::new();
        // max(0.97) < 0.98 passes; max < 0.90 fails.
        th.insert(
            "system_cpu".into(),
            loadr_config::ThresholdList::Single("max<0.98".into()),
        );
        th.insert(
            "http_req_duration".into(), // a load metric: must be ignored here
            loadr_config::ThresholdList::Single("p(95)<400".into()),
        );
        let out = evaluate_thresholds(&th, &series);
        assert_eq!(out.len(), 1, "only the observe metric is handled: {out:?}");
        assert_eq!(out[0].metric, "system_cpu");
        assert!(out[0].passed);

        let mut th2: indexmap::IndexMap<String, loadr_config::ThresholdList> =
            indexmap::IndexMap::new();
        th2.insert(
            "system_cpu".into(),
            loadr_config::ThresholdList::Single("max<0.90".into()),
        );
        let out2 = evaluate_thresholds(&th2, &series);
        assert!(!out2[0].passed, "max 0.97 should breach max<0.90");
    }

    // ---- `type: system` sampler internals ----

    #[test]
    fn parses_the_aggregate_proc_stat_cpu_line() {
        let stat = concat!(
            "cpu  4705 150 1120 16250 520 0 175 10 0 0\n",
            "cpu0 2352 75 560 8125 260 0 87 5 0 0\n",
            "intr 114930548 113199788 3 0 5\n",
        );
        let t = system::parse_proc_stat(stat).unwrap();
        // total = user..steal (first 8 fields); busy = total − (idle + iowait).
        assert_eq!(t.total, 4705 + 150 + 1120 + 16250 + 520 + 175 + 10);
        assert_eq!(t.busy, t.total - (16250 + 520));
        assert!(system::parse_proc_stat("btime 1700000000\n").is_none());
    }

    #[test]
    fn parses_meminfo_into_a_used_ratio() {
        let mem = concat!(
            "MemTotal:       16384 kB\n",
            "MemFree:         1024 kB\n",
            "MemAvailable:    4096 kB\n",
            "Buffers:          512 kB\n",
        );
        // used/total = (16384 − 4096) / 16384.
        assert_eq!(system::parse_meminfo(mem), Some(0.75));
        // No MemAvailable (ancient kernel): unavailable, not a guess.
        assert!(system::parse_meminfo("MemTotal: 16384 kB\n").is_none());
    }

    #[test]
    fn parses_diskstats_skipping_partitions_and_virtual_devices() {
        let disk = concat!(
            "   7       0 loop0 10 0 80 0 0 0 0 0 0 0 0\n",
            " 259       0 nvme0n1 1000 0 8000 0 500 0 4000 0 0 0 0\n",
            " 259       1 nvme0n1p1 900 0 7000 0 400 0 3500 0 0 0 0\n",
            "   8       0 sda 100 0 800 0 50 0 400 0 0 0 0\n",
        );
        // loop0 and the nvme partition are skipped; whole disks summed.
        assert_eq!(system::parse_diskstats(disk), Some(8000 + 4000 + 800 + 400));
        assert!(system::parse_diskstats("garbage\n").is_none());
    }

    #[test]
    fn partition_detection_is_name_aware() {
        assert!(system::is_partition_of("sda1", "sda"));
        assert!(system::is_partition_of("nvme0n1p2", "nvme0n1"));
        assert!(!system::is_partition_of("sdab", "sda")); // 28th disk, not a partition
        assert!(!system::is_partition_of("sda", "sda"));
    }

    #[test]
    fn parses_net_dev_skipping_loopback_and_headers() {
        let net = concat!(
            "Inter-|   Receive                                             |  Transmit\n",
            " face |bytes    packets errs drop fifo frame compressed multicast|bytes\n",
            "    lo: 999999 10 0 0 0 0 0 0 999999 10 0 0 0 0 0 0\n",
            "  eth0: 5000 50 0 0 0 0 0 0 2500 25 0 0 0 0 0 0\n",
            "wlan0:100 1 0 0 0 0 0 0 50 1 0 0 0 0 0 0\n", // no space after ':'
        );
        assert_eq!(system::parse_net_dev(net), Some(5000 + 2500 + 100 + 50));
        assert!(system::parse_net_dev("").is_none());
    }

    #[test]
    fn interval_values_use_deltas_and_guard_counter_resets() {
        let prev = snap(
            1_000,
            cpu_times(100, 1000),
            Some(0.40),
            Some(1_000),
            Some(10_000),
        );
        let cur = snap(
            3_000,
            cpu_times(150, 1100),
            Some(0.42),
            Some(3_000),
            Some(30_000),
        );
        let v = system::interval_values(&prev, &cur);
        assert_eq!(v.cpu, Some(0.5)); // 50 busy / 100 total jiffies
        assert_eq!(v.memory, Some(0.42)); // instantaneous, from `cur`
        assert_eq!(v.disk_io, Some(512_000.0)); // 2000 sectors × 512 B / 2 s
        assert_eq!(v.network, Some(10_000.0)); // 20000 B / 2 s

        // Counter resets must not produce garbage rates.
        let reset = snap(5_000, cpu_times(150, 1100), None, Some(100), Some(5));
        let v2 = system::interval_values(&cur, &reset);
        assert_eq!(v2.cpu, None); // cpu total didn't advance
        assert_eq!(v2.disk_io, None);
        assert_eq!(v2.network, None);

        // A zero-length interval yields only the instantaneous metric.
        let v3 = system::interval_values(&cur, &cur);
        assert_eq!(
            v3,
            system::IntervalValues {
                memory: Some(0.42),
                ..Default::default()
            }
        );
    }

    #[test]
    fn ring_drops_the_oldest_sample_beyond_the_cap() {
        let mut rings = system::ring_for("system", &[system::Metric::Cpu]);
        assert_eq!(rings[0].name, "system_cpu");
        assert_eq!(rings[0].unit, "ratio");
        for i in 0..(system::RING_CAP + 10) {
            rings[0].push(i as i64, i as f64);
        }
        assert_eq!(rings[0].points.len(), system::RING_CAP);
        assert_eq!(rings[0].points.front().copied(), Some((10, 10.0)));
    }

    #[tokio::test]
    async fn system_drain_matches_the_collect_series_shape() {
        // Unknown names are dropped, duplicates collapse, order is fixed.
        let enabled = system::enabled_metrics(&[
            "cpu".to_string(),
            "network".to_string(),
            "cpu".to_string(),
            "bogus".to_string(),
        ]);
        assert_eq!(enabled, vec![system::Metric::Cpu, system::Metric::Network]);
        assert_eq!(system::enabled_metrics(&[]).len(), 4); // empty = all four

        let ring: system::SharedRing =
            std::sync::Arc::new(parking_lot::Mutex::new(system::ring_for("sys", &enabled)));
        system::push_values(
            &ring,
            1_000,
            &system::IntervalValues {
                cpu: Some(0.5),
                memory: Some(0.9), // not enabled — must not surface
                network: Some(1_000.0),
                ..Default::default()
            },
        );
        system::push_values(
            &ring,
            2_000,
            &system::IntervalValues {
                cpu: Some(0.7),
                network: Some(2_000.0),
                ..Default::default()
            },
        );

        let handles = SystemSamplerHandles {
            samplers: vec![system::Sampler {
                task: tokio::spawn(async {}),
                ring,
            }],
        };
        let series = stop_samplers(handles);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].name, "sys_cpu");
        assert_eq!(series[0].unit, "ratio");
        assert_eq!(series[0].points, vec![(1_000, 0.5), (2_000, 0.7)]);
        assert_eq!(series[1].name, "sys_network");
        assert_eq!(series[1].unit, "bytes");
        assert_eq!(series[1].points, vec![(1_000, 1_000.0), (2_000, 2_000.0)]);
    }

    #[tokio::test]
    async fn system_series_resample_and_gate_like_prometheus_ones() {
        let rings = system::ring_for("system", &[system::Metric::Cpu]);
        let ring: system::SharedRing = std::sync::Arc::new(parking_lot::Mutex::new(rings));
        for (ts, v) in [
            (1_700_000_000_000_i64, 0.10),
            (1_700_000_001_000, 0.55),
            (1_700_000_002_000, 0.90),
        ] {
            system::push_values(
                &ring,
                ts,
                &system::IntervalValues {
                    cpu: Some(v),
                    ..Default::default()
                },
            );
        }
        let handles = SystemSamplerHandles {
            samplers: vec![system::Sampler {
                task: tokio::spawn(async {}),
                ring,
            }],
        };
        let series = stop_samplers(handles);

        // Same resampling behaviour as a prometheus-sourced series.
        let mut summary = summary_with(vec![tp(0.0), tp(1.0), tp(2.0)]);
        attach(&mut summary, &series);
        assert_eq!(summary.timeline[0].external.get("system_cpu"), Some(&0.10));
        assert_eq!(summary.timeline[1].external.get("system_cpu"), Some(&0.55));
        assert_eq!(summary.timeline[2].external.get("system_cpu"), Some(&0.90));

        // And the observe-threshold path evaluates them unchanged.
        let mut th: indexmap::IndexMap<String, loadr_config::ThresholdList> =
            indexmap::IndexMap::new();
        th.insert(
            "system_cpu".into(),
            loadr_config::ThresholdList::Single("max<0.95".into()),
        );
        let out = evaluate_thresholds(&th, &series);
        assert_eq!(out.len(), 1);
        assert!(out[0].passed, "max 0.90 is under 0.95: {out:?}");
    }

    fn cpu_times(busy: u64, total: u64) -> Option<system::CpuTimes> {
        Some(system::CpuTimes { busy, total })
    }

    fn snap(
        at_ms: i64,
        cpu: Option<system::CpuTimes>,
        mem_ratio: Option<f64>,
        disk_sectors: Option<u64>,
        net_bytes: Option<u64>,
    ) -> system::ProcSnapshot {
        system::ProcSnapshot {
            at_ms,
            cpu,
            mem_ratio,
            disk_sectors,
            net_bytes,
        }
    }

    fn summary_with(timeline: Vec<loadr_core::summary::TimelinePoint>) -> Summary {
        Summary {
            name: None,
            run_id: "r".into(),
            started_ms: 1_700_000_000_000,
            ended_ms: 1_700_000_003_000,
            duration_secs: 3.0,
            scenarios: vec![],
            metrics: vec![],
            checks: vec![],
            thresholds: vec![],
            thresholds_passed: true,
            aborted: None,
            snapshot: Default::default(),
            timeline,
            jobs: vec![],
        }
    }

    fn tp(elapsed: f64) -> loadr_core::summary::TimelinePoint {
        loadr_core::summary::TimelinePoint {
            elapsed_secs: elapsed,
            rps: 0.0,
            iterations_ps: 0.0,
            active_vus: 0.0,
            error_rate: 0.0,
            latency_avg: None,
            latency_p50: None,
            latency_p95: None,
            latency_p99: None,
            external: Default::default(),
        }
    }
}
