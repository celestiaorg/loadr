//! Builders for the trimmed live payloads sent to the SPA: the per-run SSE
//! "snapshot" event and the aggregate overview document.
//!
//! Decision-facing cumulative percentiles come from exact aggregate snapshots:
//! histograms are merged by the engine/controller before this module sees them.

use std::collections::BTreeSet;

use loadr_core::aggregate::AggValues;
use loadr_core::{Snapshot, Summary, ThresholdStatus};
use serde_json::{json, Value};

use crate::{now_ms, RunControlView, RunInfo, UiBackend};

const LIVE_STATES: [&str; 3] = ["pending", "running", "stopping"];

/// The trimmed once-per-second payload for live dashboards.
pub(crate) fn live_payload(
    snap: &Snapshot,
    exact: Option<&Snapshot>,
    thresholds: &[ThresholdStatus],
    run: &RunInfo,
    control: &RunControlView,
) -> Value {
    let latency = |pick: fn(&AggValues) -> Option<f64>| -> Value {
        exact
            .and_then(|aggregates| aggregate_value(aggregates, "request_duration", None, pick))
            .map(|v| json!(v))
            .unwrap_or(Value::Null)
    };
    let (check_passes, check_fails) = check_counts(snap);
    let request_interval = request_interval(snap, None);
    let attempts_per_second =
        request_interval.map(|counts| counts.attempts as f64 / snap.interval_secs);
    let successful_per_second =
        request_interval.map(|counts| counts.successful() as f64 / snap.interval_secs);
    let failed_per_second =
        request_interval.map(|counts| counts.failed as f64 / snap.interval_secs);
    let request_error_rate = request_interval.and_then(RequestInterval::error_rate);
    let latency_quality = if exact.is_some() {
        "exact_merged_histogram"
    } else {
        "unavailable"
    };
    let latency_average_quality = if exact.is_none() {
        "unavailable"
    } else if run.agents.is_empty() {
        "exact"
    } else {
        "histogram_reconstructed_approximate"
    };
    let interval_started_ms = snap
        .timestamp_ms
        .saturating_sub((snap.interval_secs.max(0.0) * 1000.0) as u64);

    json!({
        "run_id": run.run_id,
        "generated_ms": now_ms(),
        "ts": snap.timestamp_ms,
        "elapsed": snap.elapsed_secs,
        "interval_secs": snap.interval_secs,
        "interval_started_ms": interval_started_ms,
        "interval_ended_ms": snap.timestamp_ms,
        "state": run.state,
        "paused": control.is_paused,
        "complete": run.complete,
        "assigned_agents": run.agents,
        "contributing_agents": run.contributing_agents,
        "lost_agents": run.lost_agents,
        "rps": attempts_per_second,
        "attempts_per_second": attempts_per_second,
        "successful_per_second": successful_per_second,
        "failed_per_second": failed_per_second,
        "request_error_rate": request_error_rate,
        "request_attempts_interval": request_interval.map(|counts| counts.attempts),
        "successful_requests_interval": request_interval.map(RequestInterval::successful),
        "failed_requests_interval": request_interval.map(|counts| counts.failed),
        "iterations_ps": interval_rps(snap, "iterations", None),
        "error_rate": request_error_rate,
        "active_vus": gauge_sum(snap, "vus"),
        "max_vus": gauge_sum(snap, "vus_max"),
        "latency": {
            "avg": latency(|a| a.avg),
            "p50": latency(|a| a.med),
            "p90": latency(|a| a.p90),
            "p95": latency(|a| a.p95),
            "p99": latency(|a| a.p99),
        },
        "per_scenario": per_scenario(snap, exact),
        "per_agent": exact.map(per_agent).unwrap_or_default(),
        "thresholds": thresholds,
        "checks": { "passes": check_passes, "fails": check_fails },
        "data_sent_ps": interval_bytes_per_sec(snap, "data_sent"),
        "data_received_ps": interval_bytes_per_sec(snap, "data_received"),
        "request_reqs_total": exact
            .and_then(|aggregates| aggregate_value(aggregates, "request_reqs", None, |a| Some(a.sum)))
            .unwrap_or_else(|| request_counter_total(snap)),
        "metric_contract": {
            "request_rate_window": "interval",
            "request_rate_population": "all request attempts across protocols",
            "request_failure_definition": "canonical http_req_failed sample",
            "rps_window": "interval",
            "error_rate_window": "interval",
            "latency_window": "run_to_date",
            "latency_quality": latency_quality,
            "latency_average_quality": latency_average_quality,
            "latency_population": "all request attempts including failed requests",
        },
        "failures": failures_breakdown(snap),
    })
}

#[derive(Clone, Copy)]
struct RequestInterval {
    attempts: u64,
    failed: u64,
}

impl RequestInterval {
    fn successful(self) -> u64 {
        self.attempts.saturating_sub(self.failed)
    }

    fn error_rate(self) -> Option<f64> {
        (self.attempts > 0).then_some(self.failed as f64 / self.attempts as f64)
    }
}

/// Return one compatible interval population. Missing or mismatched canonical
/// request-failure samples make the whole success/failure split unavailable.
fn request_interval(snap: &Snapshot, scenario: Option<&str>) -> Option<RequestInterval> {
    if snap.interval_secs <= 0.0 {
        return None;
    }
    let request_series: Vec<_> = snap
        .series
        .iter()
        .filter(|series| loadr_core::metrics::is_request_counter_metric(&series.metric))
        .filter(|series| match scenario {
            Some(name) => series.tags.get("scenario").map(String::as_str) == Some(name),
            None => true,
        })
        .collect();
    let failure_series: Vec<_> = snap
        .series
        .iter()
        .filter(|series| series_matches(series, "http_req_failed", scenario))
        .collect();
    if request_series.is_empty() || failure_series.is_empty() {
        return None;
    }
    let attempts = request_series
        .iter()
        .map(|series| series.interval_count)
        .sum::<u64>();
    let failure_samples = failure_series
        .iter()
        .map(|series| series.interval_count)
        .sum::<u64>();
    let failed = failure_series
        .iter()
        .map(|series| series.interval_sum)
        .sum::<f64>()
        .round()
        .max(0.0) as u64;
    (attempts == failure_samples && failed <= attempts)
        .then_some(RequestInterval { attempts, failed })
}

/// A single failure-cause bucket: a label, its count, and share of all failures
/// in that category.
fn bucket(key: String, count: u64, category_total: u64) -> Value {
    let share = if category_total > 0 {
        count as f64 / category_total as f64
    } else {
        0.0
    };
    json!({
        "key": key,
        "count": count,
        "share": share,
        "denominator": category_total,
    })
}

/// Sort buckets descending by count, cap to `limit`, folding the rest into an
/// "other" row so the panel never grows unbounded under high-cardinality tags.
fn top_buckets(mut counts: Vec<(String, u64)>, limit: usize) -> Vec<Value> {
    counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let total: u64 = counts.iter().map(|(_, c)| c).sum();
    let mut out = Vec::new();
    if counts.len() > limit {
        let (head, tail) = counts.split_at(limit.saturating_sub(1));
        for (k, c) in head {
            out.push(bucket(k.clone(), *c, total));
        }
        let other: u64 = tail.iter().map(|(_, c)| c).sum();
        if other > 0 {
            out.push(bucket("other".to_string(), other, total));
        }
    } else {
        for (k, c) in &counts {
            out.push(bucket(k.clone(), *c, total));
        }
    }
    out
}

fn response_status_name(protocol: &str, status: &str) -> Option<&'static str> {
    let code: u16 = status.parse().ok()?;
    match protocol {
        "http" | "graphql" | "sse" | "browser" | "ws" => {
            http::StatusCode::from_u16(code).ok()?.canonical_reason()
        }
        "grpc" => match code {
            0 => Some("OK"),
            1 => Some("CANCELLED"),
            2 => Some("UNKNOWN"),
            3 => Some("INVALID_ARGUMENT"),
            4 => Some("DEADLINE_EXCEEDED"),
            5 => Some("NOT_FOUND"),
            6 => Some("ALREADY_EXISTS"),
            7 => Some("PERMISSION_DENIED"),
            8 => Some("RESOURCE_EXHAUSTED"),
            9 => Some("FAILED_PRECONDITION"),
            10 => Some("ABORTED"),
            11 => Some("OUT_OF_RANGE"),
            12 => Some("UNIMPLEMENTED"),
            13 => Some("INTERNAL"),
            14 => Some("UNAVAILABLE"),
            15 => Some("DATA_LOSS"),
            16 => Some("UNAUTHENTICATED"),
            _ => None,
        },
        _ => None,
    }
}

fn status_bucket(protocol: &str, status: &str, count: u64, category_total: u64) -> Value {
    let share = if category_total > 0 {
        count as f64 / category_total as f64
    } else {
        0.0
    };
    json!({
        "key": status,
        "protocol": if protocol.is_empty() { None } else { Some(protocol) },
        "status": status,
        "status_name": response_status_name(protocol, status),
        "count": count,
        "share": share,
        "denominator": category_total,
    })
}

/// Sort response statuses by count and keep protocols separate when their
/// numeric status spaces overlap (for example HTTP and gRPC).
fn top_status_buckets(mut counts: Vec<((String, String), u64)>, limit: usize) -> Vec<Value> {
    counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let total: u64 = counts.iter().map(|(_, count)| count).sum();
    let mut out = Vec::new();
    if counts.len() > limit {
        let (head, tail) = counts.split_at(limit.saturating_sub(1));
        for ((protocol, status), count) in head {
            out.push(status_bucket(protocol, status, *count, total));
        }
        let other: u64 = tail.iter().map(|(_, count)| count).sum();
        if other > 0 {
            out.push(bucket("other".to_string(), other, total));
        }
    } else {
        for ((protocol, status), count) in &counts {
            out.push(status_bucket(protocol, status, *count, total));
        }
    }
    out
}

/// Group failed requests, failed checks, and script exceptions by cause.
///
/// Sources, all from data the engine already tracks:
/// - Failed requests: the `http_req_failed` rate, which every protocol records.
///   Samples carrying an `error_kind`
///   (transport failures) or `error` (prepare/protocol/extraction) tag are
///   bucketed by that kind. Remaining failures are bucketed by `proto` and
///   non-zero `status`, with missing/zero statuses reported as `unknown`.
/// - Failed checks: the failing fraction of each `checks` series, by `check` tag.
/// - Script exceptions: the `vu_exceptions` counter, by `exception` tag.
pub(crate) fn failures_breakdown(snap: &Snapshot) -> Value {
    use std::collections::BTreeMap;
    const LIMIT: usize = 12;

    let mut by_status: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut by_error_kind: BTreeMap<String, u64> = BTreeMap::new();
    for s in snap.series.iter().filter(|s| s.metric == "http_req_failed") {
        let fails = s.agg.sum.max(0.0) as u64;
        if fails == 0 {
            continue;
        }
        if let Some(kind) = s.tags.get("error_kind").or_else(|| s.tags.get("error")) {
            *by_error_kind.entry(kind.clone()).or_default() += fails;
            continue;
        }
        let protocol = s.tags.get("proto").cloned().unwrap_or_default();
        let status = s
            .tags
            .get("status")
            .filter(|status| status.parse::<i64>().is_ok_and(|code| code > 0))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        *by_status.entry((protocol, status)).or_default() += fails;
    }

    // Failed checks: count = total evaluations, sum = passes, so fails = count - sum.
    let mut by_check: BTreeMap<String, u64> = BTreeMap::new();
    for s in snap.series.iter().filter(|s| s.metric == "checks") {
        let Some(name) = s.tags.get("check") else {
            continue;
        };
        let fails = s.agg.count.saturating_sub(s.agg.sum.max(0.0) as u64);
        if fails > 0 {
            *by_check.entry(name.clone()).or_default() += fails;
        }
    }

    // Script exceptions from the vu_exceptions counter's `exception` tag.
    let mut by_exception: BTreeMap<String, u64> = BTreeMap::new();
    for s in snap.series.iter().filter(|s| s.metric == "vu_exceptions") {
        let key = s
            .tags
            .get("exception")
            .cloned()
            .unwrap_or_else(|| "exception".to_string());
        *by_exception.entry(key).or_default() += s.agg.sum.max(0.0) as u64;
    }

    let sum_counts = |m: &BTreeMap<String, u64>| -> u64 { m.values().sum() };
    let status_total: u64 = by_status.values().sum();
    let error_total = sum_counts(&by_error_kind);
    let check_total = sum_counts(&by_check);
    let exception_total = sum_counts(&by_exception);

    let failed_requests = snap
        .series
        .iter()
        .filter(|s| s.metric == "http_req_failed")
        .map(|s| s.agg.sum.max(0.0) as u64)
        .sum::<u64>();
    json!({
        "event_total": status_total + error_total + check_total + exception_total,
        "failed_requests": failed_requests,
        "failed_checks": check_total,
        "exceptions": exception_total,
        "category_totals": {
            "by_status": status_total,
            "by_error_kind": error_total,
            "by_check": check_total,
            "by_exception": exception_total,
        },
        "share_contract": "each percentage uses its category total; categories may overlap",
        "by_status": top_status_buckets(by_status.into_iter().collect(), LIMIT),
        "by_error_kind": top_buckets(by_error_kind.into_iter().collect(), LIMIT),
        "by_check": top_buckets(by_check.into_iter().collect(), LIMIT),
        "by_exception": top_buckets(by_exception.into_iter().collect(), LIMIT),
    })
}

/// Decision-facing frozen values for a completed run.
pub(crate) fn final_payload(summary: &Summary, run: &RunInfo) -> Value {
    let metric = |name: &str| {
        summary
            .metrics
            .iter()
            .find(|metric| metric.metric == name)
            .map(|metric| &metric.agg)
    };
    let attempts = metric("request_reqs").map(|agg| agg.sum.max(0.0).round() as u64);
    let failure_metric = metric("http_req_failed");
    let compatible = attempts
        .zip(failure_metric)
        .and_then(|(attempts, failures)| {
            let failed = failures.sum.max(0.0).round() as u64;
            (failures.count == attempts && failed <= attempts)
                .then_some(RequestInterval { attempts, failed })
        });
    let duration = summary.duration_secs;
    let rate = |count: Option<u64>| {
        count.and_then(|count| (duration > 0.0).then_some(count as f64 / duration))
    };
    let latency = metric("request_duration");
    let average_quality = if latency.is_none() {
        "unavailable"
    } else if run.agents.is_empty() {
        "exact"
    } else {
        "histogram_reconstructed_approximate"
    };

    json!({
        "window": "full_observed_run",
        "started_ms": summary.started_ms,
        "ended_ms": summary.ended_ms,
        "duration_secs": duration,
        "attempts": attempts,
        "successful_requests": compatible.map(RequestInterval::successful),
        "failed_requests": compatible.map(|counts| counts.failed),
        "average_attempts_per_second": rate(attempts),
        "average_successful_per_second": rate(compatible.map(RequestInterval::successful)),
        "average_failed_per_second": rate(compatible.map(|counts| counts.failed)),
        "request_error_rate": compatible.and_then(RequestInterval::error_rate),
        "latency": {
            "avg": latency.and_then(|agg| agg.avg),
            "p95": latency.and_then(|agg| agg.p95),
        },
        "metric_contract": {
            "rate_denominator": "full observed duration",
            "request_failure_definition": "canonical http_req_failed sample",
            "latency_window": "run_to_date",
            "latency_population": "all request attempts including failed requests",
            "latency_percentile_quality": if latency.is_some() {
                "exact_merged_histogram"
            } else {
                "unavailable"
            },
            "latency_average_quality": average_quality,
        },
        "complete": run.complete,
        "failures": failures_breakdown(&summary.snapshot),
    })
}

/// The aggregate overview: the most relevant run (live preferred, else most
/// recent) plus fleet counters.
pub(crate) fn overview_json(backend: &dyn UiBackend) -> Value {
    let runs = backend.runs();
    let live_runs = runs
        .iter()
        .filter(|r| LIVE_STATES.contains(&r.state.as_str()))
        .count();
    let target = runs
        .iter()
        .find(|r| LIVE_STATES.contains(&r.state.as_str()))
        .or_else(|| runs.first());

    let (run, metrics, final_metrics) = match target {
        Some(r) => {
            if LIVE_STATES.contains(&r.state.as_str()) {
                let thresholds = backend.run_thresholds(&r.run_id);
                let exact = backend.run_aggregate_snapshot(&r.run_id);
                let control = backend.run_control_state(&r.run_id);
                let metrics = backend
                    .run_snapshot(&r.run_id)
                    .map(|snapshot| {
                        live_payload(&snapshot, exact.as_deref(), &thresholds, r, &control)
                    })
                    .unwrap_or(Value::Null);
                (
                    serde_json::to_value(r).unwrap_or(Value::Null),
                    metrics,
                    Value::Null,
                )
            } else {
                let final_metrics = backend
                    .run_summary(&r.run_id)
                    .map(|summary| final_payload(&summary, r))
                    .unwrap_or(Value::Null);
                (
                    serde_json::to_value(r).unwrap_or(Value::Null),
                    Value::Null,
                    final_metrics,
                )
            }
        }
        None => (Value::Null, Value::Null, Value::Null),
    };

    json!({
        "run": run,
        "metrics": metrics,
        "final_metrics": final_metrics,
        "live_runs": live_runs,
        "total_runs": runs.len(),
        "agents": backend.agents().len(),
    })
}

fn series_matches(s: &loadr_core::SeriesSnapshot, metric: &str, scenario: Option<&str>) -> bool {
    if s.metric != metric {
        return false;
    }
    scenario_matches(s, scenario)
}

fn scenario_matches(s: &loadr_core::SeriesSnapshot, scenario: Option<&str>) -> bool {
    match scenario {
        Some(name) => s.tags.get("scenario").map(String::as_str) == Some(name),
        None => true,
    }
}

/// Events recorded since the previous snapshot, per second.
fn interval_rps(snap: &Snapshot, metric: &str, scenario: Option<&str>) -> Option<f64> {
    if snap.interval_secs <= 0.0 {
        return None;
    }
    let count: u64 = snap
        .series
        .iter()
        .filter(|s| series_matches(s, metric, scenario))
        .map(|s| s.interval_count)
        .sum();
    Some(count as f64 / snap.interval_secs)
}

fn aggregate_value<F>(
    aggregates: &Snapshot,
    metric: &str,
    scenario: Option<&str>,
    pick: F,
) -> Option<f64>
where
    F: Fn(&AggValues) -> Option<f64>,
{
    aggregates
        .series
        .iter()
        .find(|series| {
            series.metric == metric
                && match scenario {
                    Some(name) => {
                        series.tags.len() == 1
                            && series.tags.get("scenario").map(String::as_str) == Some(name)
                    }
                    None => series.tags.is_empty(),
                }
        })
        .and_then(|series| pick(&series.agg))
}

/// Sum of gauge `last` values across series of a metric.
fn gauge_sum(snap: &Snapshot, metric: &str) -> f64 {
    snap.series
        .iter()
        .filter(|s| s.metric == metric)
        .filter_map(|s| s.agg.last)
        .sum()
}

fn request_counter_total(snap: &Snapshot) -> f64 {
    snap.series
        .iter()
        .filter(|s| loadr_core::metrics::is_request_counter_metric(&s.metric))
        .map(|s| s.agg.sum)
        .sum()
}

fn interval_bytes_per_sec(snap: &Snapshot, metric: &str) -> Option<f64> {
    if snap.interval_secs <= 0.0 {
        return None;
    }
    let sum: f64 = snap
        .series
        .iter()
        .filter(|s| s.metric == metric)
        .map(|s| s.interval_sum)
        .sum();
    Some((sum / snap.interval_secs).max(0.0))
}

fn check_counts(snap: &Snapshot) -> (u64, u64) {
    let mut passes = 0_u64;
    let mut total = 0_u64;
    for s in snap.series.iter().filter(|s| s.metric == "checks") {
        passes += s.agg.sum.max(0.0) as u64;
        total += s.agg.count;
    }
    (passes, total.saturating_sub(passes))
}

fn per_scenario(snap: &Snapshot, exact: Option<&Snapshot>) -> Vec<Value> {
    let mut names: BTreeSet<&str> = snap
        .series
        .iter()
        .filter_map(|s| s.tags.get("scenario").map(String::as_str))
        .collect();
    names.remove("setup");
    names.remove("teardown");
    names
        .into_iter()
        .map(|name| {
            let interval = request_interval(snap, Some(name));
            json!({
                "scenario": name,
                "rps": interval.map(|counts| counts.attempts as f64 / snap.interval_secs),
                "attempts_per_second": interval.map(|counts| counts.attempts as f64 / snap.interval_secs),
                "successful_per_second": interval.map(|counts| counts.successful() as f64 / snap.interval_secs),
                "failed_per_second": interval.map(|counts| counts.failed as f64 / snap.interval_secs),
                "iterations_ps": interval_rps(snap, "iterations", Some(name)),
                "p95": exact
                    .and_then(|aggregates| aggregate_value(aggregates, "request_duration", Some(name), |a| a.p95)),
                "avg": exact
                    .and_then(|aggregates| aggregate_value(aggregates, "request_duration", Some(name), |a| a.avg)),
                "error_rate": interval.and_then(RequestInterval::error_rate),
            })
        })
        .collect()
}

fn per_agent(aggregates: &Snapshot) -> Vec<Value> {
    let mut agents: BTreeSet<(String, String)> = BTreeSet::new();
    for series in &aggregates.series {
        if let (Some(name), Some(id)) = (
            series.tags.get("loadr_agent"),
            series.tags.get("loadr_agent_id"),
        ) {
            agents.insert((id.clone(), name.clone()));
        }
    }
    agents
        .into_iter()
        .map(|(id, name)| {
            let find = |metric: &str| {
                aggregates.series.iter().find(|series| {
                    series.metric == metric
                        && series.tags.get("loadr_agent_id") == Some(&id)
                        && series.tags.get("loadr_agent") == Some(&name)
                })
            };
            json!({
                "id": id,
                "name": name,
                "requests": find("request_reqs").map(|s| s.agg.sum).unwrap_or(0.0),
                "latency_avg": find("request_duration").and_then(|s| s.agg.avg),
                "latency_p95": find("request_duration").and_then(|s| s.agg.p95),
                "error_rate": find("http_req_failed").and_then(|s| s.agg.rate),
                "active_vus": find("vus").and_then(|s| s.agg.last).unwrap_or(0.0),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadr_core::aggregate::Aggregator;
    use loadr_core::metrics::{now_millis, MetricKind, Sample, Tags};
    use std::sync::Arc;

    fn run_info(state: &str) -> RunInfo {
        RunInfo {
            run_id: "run-test".to_string(),
            name: Some("test".to_string()),
            state: state.to_string(),
            passed: None,
            started_ms: now_ms(),
            ended_ms: None,
            observed_ms: now_ms(),
            scenarios: vec!["browse".to_string()],
            agents: Vec::new(),
            contributing_agents: Vec::new(),
            lost_agents: Vec::new(),
            complete: None,
            on_agent_loss: None,
        }
    }

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
    fn live_payload_shape() {
        let mut agg = Aggregator::new();
        for i in 0..50 {
            agg.record(&sample(
                "http_reqs",
                MetricKind::Counter,
                1.0,
                &[("scenario", "browse")],
            ));
            agg.record(&sample(
                "http_req_duration",
                MetricKind::Trend,
                10.0 + i as f64,
                &[("scenario", "browse")],
            ));
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                if i % 10 == 0 { 1.0 } else { 0.0 },
                &[("scenario", "browse")],
            ));
        }
        agg.record(&sample("vus", MetricKind::Gauge, 7.0, &[]));
        let exact = agg.aggregate_snapshot(&[&["scenario"]]);
        let mut snap = agg.snapshot();
        snap.interval_secs = 1.0;
        let payload = live_payload(
            &snap,
            Some(&exact),
            &[],
            &run_info("running"),
            &RunControlView::default(),
        );
        assert_eq!(payload["state"], "running");
        assert!(payload["rps"].as_f64().expect("rps") > 0.0);
        assert_eq!(payload["attempts_per_second"], 50.0);
        assert_eq!(payload["successful_per_second"], 45.0);
        assert_eq!(payload["failed_per_second"], 5.0);
        assert_eq!(payload["active_vus"], 7.0);
        let err = payload["error_rate"].as_f64().expect("error rate");
        assert!((err - 0.1).abs() < 1e-9);
        assert!(payload["latency"]["p95"].as_f64().expect("p95") > 10.0);
        let scenarios = payload["per_scenario"].as_array().expect("scenarios");
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0]["scenario"], "browse");
        assert_eq!(
            payload["metric_contract"]["latency_quality"],
            "exact_merged_histogram"
        );
    }

    #[test]
    fn distributed_percentile_uses_merged_histogram_and_shows_agent_contributions() {
        let mut agg = Aggregator::new();
        for value in 1..=1000 {
            agg.record(&sample(
                "http_req_duration",
                MetricKind::Trend,
                value as f64,
                &[("loadr_agent", "a"), ("loadr_agent_id", "id-a")],
            ));
            agg.record(&sample(
                "http_reqs",
                MetricKind::Counter,
                1.0,
                &[("loadr_agent", "a"), ("loadr_agent_id", "id-a")],
            ));
        }
        for value in 1001..=2000 {
            agg.record(&sample(
                "http_req_duration",
                MetricKind::Trend,
                value as f64,
                &[("loadr_agent", "b"), ("loadr_agent_id", "id-b")],
            ));
            agg.record(&sample(
                "http_reqs",
                MetricKind::Counter,
                1.0,
                &[("loadr_agent", "b"), ("loadr_agent_id", "id-b")],
            ));
        }
        let exact = agg.aggregate_snapshot(&[&["loadr_agent", "loadr_agent_id"]]);
        let snap = agg.snapshot();
        let payload = live_payload(
            &snap,
            Some(&exact),
            &[],
            &run_info("running"),
            &RunControlView::default(),
        );
        let p99 = payload["latency"]["p99"].as_f64().expect("p99");
        assert!((1970.0..=1990.0).contains(&p99), "true fleet p99: {p99}");
        assert_eq!(payload["per_agent"].as_array().map(Vec::len), Some(2));
        assert_eq!(payload["request_reqs_total"], 2000.0);
    }

    #[test]
    fn graphql_transport_is_counted_once_in_request_rollup() {
        let mut agg = Aggregator::new();
        agg.record(&sample("http_reqs", MetricKind::Counter, 1.0, &[]));
        agg.record(&sample("graphql_reqs", MetricKind::Counter, 1.0, &[]));
        agg.record(&sample("http_req_duration", MetricKind::Trend, 20.0, &[]));
        agg.record(&sample(
            "graphql_req_duration",
            MetricKind::Trend,
            20.0,
            &[],
        ));
        let exact = agg.aggregate_snapshot(&[]);
        let snap = agg.snapshot();
        let payload = live_payload(
            &snap,
            Some(&exact),
            &[],
            &run_info("running"),
            &RunControlView::default(),
        );
        assert_eq!(payload["request_reqs_total"], 1.0);
        assert_eq!(payload["latency"]["avg"], 20.0);
    }

    #[test]
    fn live_payload_counts_grpc_request_family() {
        let mut agg = Aggregator::new();
        for i in 0..20 {
            agg.record(&sample(
                "grpc_reqs",
                MetricKind::Counter,
                1.0,
                &[("scenario", "rpc")],
            ));
            agg.record(&sample(
                "grpc_req_duration",
                MetricKind::Trend,
                5.0 + i as f64,
                &[("scenario", "rpc")],
            ));
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                0.0,
                &[("scenario", "rpc")],
            ));
        }
        let exact = agg.aggregate_snapshot(&[&["scenario"]]);
        let snap = agg.snapshot();
        let payload = live_payload(
            &snap,
            Some(&exact),
            &[],
            &run_info("running"),
            &RunControlView::default(),
        );

        assert!(payload["rps"].as_f64().expect("rps") > 0.0);
        assert_eq!(payload["request_reqs_total"], 20.0);
        assert!(payload["latency"]["p95"].as_f64().expect("p95") > 0.0);
        let scenarios = payload["per_scenario"].as_array().expect("scenarios");
        assert_eq!(scenarios[0]["scenario"], "rpc");
        assert!(scenarios[0]["rps"].as_f64().expect("scenario rps") > 0.0);
        assert!(scenarios[0]["p95"].as_f64().expect("scenario p95") > 0.0);
    }

    #[test]
    fn live_payload_counts_plugin_request_family() {
        let mut agg = Aggregator::new();
        for i in 0..10 {
            agg.record(&sample(
                "mongo_reqs",
                MetricKind::Counter,
                1.0,
                &[("scenario", "transfers")],
            ));
            agg.record(&sample(
                "mongo_req_duration",
                MetricKind::Trend,
                1.0 + i as f64,
                &[("scenario", "transfers")],
            ));
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                0.0,
                &[("scenario", "transfers")],
            ));
        }
        let exact = agg.aggregate_snapshot(&[&["scenario"]]);
        let snap = agg.snapshot();
        let payload = live_payload(
            &snap,
            Some(&exact),
            &[],
            &run_info("running"),
            &RunControlView::default(),
        );

        assert!(payload["rps"].as_f64().expect("rps") > 0.0);
        assert_eq!(payload["request_reqs_total"], 10.0);
        assert!(payload["latency"]["p95"].as_f64().expect("p95") > 0.0);
        let scenarios = payload["per_scenario"].as_array().expect("scenarios");
        assert_eq!(scenarios[0]["scenario"], "transfers");
        assert!(scenarios[0]["rps"].as_f64().expect("scenario rps") > 0.0);
        assert!(scenarios[0]["p95"].as_f64().expect("scenario p95") > 0.0);
    }

    #[test]
    fn live_payload_sums_mixed_request_families() {
        let mut agg = Aggregator::new();
        for _ in 0..7 {
            agg.record(&sample(
                "http_reqs",
                MetricKind::Counter,
                1.0,
                &[("scenario", "mixed")],
            ));
            agg.record(&sample(
                "http_req_duration",
                MetricKind::Trend,
                10.0,
                &[("scenario", "mixed")],
            ));
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                0.0,
                &[("scenario", "mixed")],
            ));
        }
        for _ in 0..3 {
            agg.record(&sample(
                "grpc_reqs",
                MetricKind::Counter,
                1.0,
                &[("scenario", "mixed")],
            ));
            agg.record(&sample(
                "grpc_req_duration",
                MetricKind::Trend,
                30.0,
                &[("scenario", "mixed")],
            ));
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                0.0,
                &[("scenario", "mixed")],
            ));
        }
        let exact = agg.aggregate_snapshot(&[&["scenario"]]);
        let snap = agg.snapshot();
        let payload = live_payload(
            &snap,
            Some(&exact),
            &[],
            &run_info("running"),
            &RunControlView::default(),
        );

        assert_eq!(payload["request_reqs_total"], 10.0);
        assert!(payload["rps"].as_f64().expect("rps") > 0.0);
        assert_eq!(payload["latency"]["avg"], 16.0);
        let scenarios = payload["per_scenario"].as_array().expect("scenarios");
        assert_eq!(scenarios[0]["avg"], 16.0);
    }

    #[test]
    fn live_payload_sums_distributed_vus() {
        let mut agg = Aggregator::new();
        for instance in ["a", "b", "c"] {
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

        let snap = agg.snapshot();
        let payload = live_payload(
            &snap,
            None,
            &[],
            &run_info("running"),
            &RunControlView::default(),
        );
        assert_eq!(payload["active_vus"], 9999.0);
        assert_eq!(payload["max_vus"], 9999.0);
    }

    /// Build a snapshot exercising every failure source, then assert the
    /// breakdown groups by cause with correct counts and shares.
    #[test]
    fn failures_breakdown_groups_by_cause() {
        let mut agg = Aggregator::new();
        // 10 OK 200s + 3 failing 500s + 2 failing 404s.
        for _ in 0..10 {
            agg.record(&sample(
                "http_reqs",
                MetricKind::Counter,
                1.0,
                &[("status", "200")],
            ));
        }
        for _ in 0..3 {
            agg.record(&sample(
                "http_reqs",
                MetricKind::Counter,
                1.0,
                &[("status", "500")],
            ));
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                1.0,
                &[("status", "500"), ("proto", "http")],
            ));
        }
        for _ in 0..2 {
            agg.record(&sample(
                "http_reqs",
                MetricKind::Counter,
                1.0,
                &[("status", "404")],
            ));
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                1.0,
                &[("status", "404"), ("proto", "http")],
            ));
        }
        // 4 transport timeouts via http_req_failed with an error_kind tag.
        for _ in 0..4 {
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                1.0,
                &[("error_kind", "timeout")],
            ));
        }
        // A check "status is 200": 7 pass, 5 fail.
        for i in 0..12 {
            agg.record(&sample(
                "checks",
                MetricKind::Rate,
                if i < 7 { 1.0 } else { 0.0 },
                &[("check", "status is 200")],
            ));
        }
        // 6 script exceptions of the same normalised message.
        for _ in 0..6 {
            agg.record(&sample(
                "vu_exceptions",
                MetricKind::Counter,
                1.0,
                &[("exception", "TypeError: x is undefined")],
            ));
        }
        let snap = agg.snapshot();
        let f = failures_breakdown(&snap);

        // 5 status (3+2) + 4 error_kind + 5 check + 6 exception = 20.
        assert_eq!(f["event_total"], 20);
        assert_eq!(f["failed_requests"], 9);
        assert_eq!(f["failed_checks"], 5);
        assert_eq!(f["exceptions"], 6);

        let by_status = f["by_status"].as_array().expect("by_status");
        assert_eq!(by_status.len(), 2);
        // Highest count first: 500 with 3.
        assert_eq!(by_status[0]["key"], "500");
        assert_eq!(by_status[0]["protocol"], "http");
        assert_eq!(by_status[0]["status"], "500");
        assert_eq!(by_status[0]["status_name"], "Internal Server Error");
        assert_eq!(by_status[0]["count"], 3);
        let share = by_status[0]["share"].as_f64().expect("share");
        assert!((share - 3.0 / 5.0).abs() < 1e-9);
        assert_eq!(by_status[0]["denominator"], 5);

        let by_kind = f["by_error_kind"].as_array().expect("by_error_kind");
        assert_eq!(by_kind.len(), 1);
        assert_eq!(by_kind[0]["key"], "timeout");
        assert_eq!(by_kind[0]["count"], 4);

        let by_check = f["by_check"].as_array().expect("by_check");
        assert_eq!(by_check.len(), 1);
        assert_eq!(by_check[0]["key"], "status is 200");
        assert_eq!(by_check[0]["count"], 5);

        let by_exc = f["by_exception"].as_array().expect("by_exception");
        assert_eq!(by_exc.len(), 1);
        assert_eq!(by_exc[0]["count"], 6);
    }

    /// gRPC failures carry small numeric codes (1..16) and no error tag; they
    /// must land in by_status with a readable name, not vanish (issue: the
    /// CSV/Report buttons stayed disabled on gRPC runs with failures).
    #[test]
    fn failures_breakdown_grpc_status_failures() {
        let mut agg = Aggregator::new();
        for _ in 0..5 {
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                0.0,
                &[("status", "0"), ("proto", "grpc")],
            ));
        }
        for _ in 0..3 {
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                1.0,
                &[("status", "14"), ("proto", "grpc")],
            ));
        }
        for _ in 0..2 {
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                1.0,
                &[("status", "7"), ("proto", "grpc")],
            ));
        }
        let snap = agg.snapshot();
        let f = failures_breakdown(&snap);
        assert_eq!(f["event_total"], 5);
        assert_eq!(f["failed_requests"], 5);
        let by_status = f["by_status"].as_array().expect("by_status");
        assert_eq!(by_status.len(), 2);
        assert_eq!(by_status[0]["key"], "14");
        assert_eq!(by_status[0]["protocol"], "grpc");
        assert_eq!(by_status[0]["status"], "14");
        assert_eq!(by_status[0]["status_name"], "UNAVAILABLE");
        assert_eq!(by_status[0]["count"], 3);
        assert_eq!(by_status[1]["key"], "7");
        assert_eq!(by_status[1]["status_name"], "PERMISSION_DENIED");
        assert_eq!(by_status[1]["count"], 2);
    }

    #[test]
    fn failures_breakdown_keeps_protocol_status_spaces_distinct() {
        let mut agg = Aggregator::new();
        for _ in 0..2 {
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                1.0,
                &[("status", "14"), ("proto", "grpc")],
            ));
        }
        agg.record(&sample(
            "http_req_failed",
            MetricKind::Rate,
            1.0,
            &[("status", "16"), ("proto", "grpc")],
        ));
        agg.record(&sample(
            "http_req_failed",
            MetricKind::Rate,
            1.0,
            &[("status", "14"), ("proto", "risotto")],
        ));
        agg.record(&sample(
            "http_req_failed",
            MetricKind::Rate,
            1.0,
            &[("status", "599"), ("proto", "http")],
        ));

        let f = failures_breakdown(&agg.snapshot());
        assert_eq!(f["event_total"], 5);
        assert_eq!(f["failed_requests"], 5);
        let statuses = f["by_status"].as_array().expect("by_status");
        assert_eq!(statuses.len(), 4);

        let find = |protocol: &str, status: &str| {
            statuses
                .iter()
                .find(|row| row["protocol"] == protocol && row["status"] == status)
                .expect("protocol/status bucket")
        };
        let unavailable = find("grpc", "14");
        assert_eq!(unavailable["key"], "14");
        assert_eq!(unavailable["status_name"], "UNAVAILABLE");
        assert_eq!(unavailable["count"], 2);
        assert!((unavailable["share"].as_f64().expect("share") - 0.4).abs() < 1e-9);

        assert_eq!(find("grpc", "16")["status_name"], "UNAUTHENTICATED");
        assert!(find("risotto", "14")["status_name"].is_null());
        assert!(find("http", "599")["status_name"].is_null());
    }

    #[test]
    fn failures_breakdown_prefers_error_kind_and_keeps_unknown_failures() {
        let mut agg = Aggregator::new();
        agg.record(&sample(
            "http_req_failed",
            MetricKind::Rate,
            1.0,
            &[
                ("status", "500"),
                ("proto", "http"),
                ("error_kind", "connection_reset"),
            ],
        ));
        agg.record(&sample(
            "http_req_failed",
            MetricKind::Rate,
            1.0,
            &[("status", "0"), ("proto", "grpc")],
        ));

        let f = failures_breakdown(&agg.snapshot());
        assert_eq!(f["event_total"], 2);
        assert_eq!(f["failed_requests"], 2);

        let statuses = f["by_status"].as_array().expect("by_status");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0]["protocol"], "grpc");
        assert_eq!(statuses[0]["status"], "unknown");
        assert!(statuses[0]["status_name"].is_null());

        let errors = f["by_error_kind"].as_array().expect("by_error_kind");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["key"], "connection_reset");
        assert_eq!(errors[0]["count"], 1);
    }

    #[test]
    fn failures_breakdown_empty_when_all_ok() {
        let mut agg = Aggregator::new();
        for _ in 0..5 {
            agg.record(&sample(
                "http_reqs",
                MetricKind::Counter,
                1.0,
                &[("status", "200")],
            ));
        }
        let snap = agg.snapshot();
        let f = failures_breakdown(&snap);
        assert_eq!(f["event_total"], 0);
        assert!(f["by_status"].as_array().expect("arr").is_empty());
    }

    #[test]
    fn failures_breakdown_caps_high_cardinality() {
        let mut agg = Aggregator::new();
        // 20 distinct failing statuses -> capped to 12 with an "other" row.
        for code in 400..420 {
            agg.record(&sample(
                "http_req_failed",
                MetricKind::Rate,
                1.0,
                &[("status", &code.to_string())],
            ));
        }
        let snap = agg.snapshot();
        let f = failures_breakdown(&snap);
        let by_status = f["by_status"].as_array().expect("by_status");
        assert_eq!(by_status.len(), 12);
        assert_eq!(by_status.last().unwrap()["key"], "other");
        assert!(by_status[0]["protocol"].is_null());
        assert!(by_status[0]["status"].is_string());
        // All 20 failures still accounted for across the 12 rows.
        let summed: u64 = by_status.iter().map(|b| b["count"].as_u64().unwrap()).sum();
        assert_eq!(summed, 20);
    }

    #[test]
    fn incompatible_request_interval_is_unavailable() {
        let mut agg = Aggregator::new();
        for index in 0..10 {
            agg.record(&sample("http_reqs", MetricKind::Counter, 1.0, &[]));
            if index < 9 {
                agg.record(&sample("http_req_failed", MetricKind::Rate, 0.0, &[]));
            }
        }
        let exact = agg.aggregate_snapshot(&[]);
        let mut snap = agg.snapshot();
        snap.interval_secs = 1.0;
        let payload = live_payload(
            &snap,
            Some(&exact),
            &[],
            &run_info("running"),
            &RunControlView::default(),
        );

        assert!(payload["attempts_per_second"].is_null());
        assert!(payload["successful_per_second"].is_null());
        assert!(payload["failed_per_second"].is_null());
        assert!(payload["request_error_rate"].is_null());
    }
}
