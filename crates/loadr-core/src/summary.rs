//! End-of-run summary: structured data plus console/JSON renderers.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::aggregate::{AggValues, Aggregator, Snapshot};
use crate::metrics::MetricKind;
use crate::thresholds::ThresholdStatus;

/// Per-check pass/fail counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckSummary {
    pub name: String,
    pub passes: u64,
    pub fails: u64,
    /// Bounded semantic failure breakdown. Omitted for all existing checks
    /// and for protobuf checks without an explicit `failure_groups` mapping.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failure_groups: Vec<CheckFailureGroupSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckFailureGroupSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<i64>,
    pub label: String,
    pub count: u64,
}

#[derive(Default)]
struct CheckAccumulator {
    passes: u64,
    fails: u64,
    failure_groups: BTreeMap<(Option<i64>, String), u64>,
}

/// One metric in the summary, merged across all tag combinations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSummary {
    pub metric: String,
    pub kind: MetricKind,
    /// Trend values are milliseconds.
    pub agg: AggValues,
}

/// A single time bucket in the run timeline, derived from one live snapshot.
///
/// One point is emitted per snapshot interval. All values describe the run at
/// `elapsed_secs`; throughput and `error_rate` cover the interval window, while
/// latency percentiles are exact run-to-date aggregates and `active_vus` is
/// point-in-time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimelinePoint {
    /// Seconds since the run started.
    pub elapsed_secs: f64,
    /// Successful + failed requests per second over the interval.
    pub rps: f64,
    /// Completed iterations per second over the interval.
    pub iterations_ps: f64,
    /// Active virtual users at this instant.
    pub active_vus: f64,
    /// Fraction of requests that failed over the interval, in `[0, 1]`.
    pub error_rate: f64,
    /// `http_req_duration` average in milliseconds (null if no samples yet).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub latency_avg: Option<f64>,
    /// `http_req_duration` p50 in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub latency_p50: Option<f64>,
    /// `http_req_duration` p95 in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub latency_p95: Option<f64>,
    /// `http_req_duration` p99 in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub latency_p99: Option<f64>,
    /// External system metrics (from `observe:`) sampled at this point, keyed by
    /// canonical metric name. Empty unless load↔system correlation is configured.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub external: BTreeMap<String, f64>,
}

impl TimelinePoint {
    /// Derive a timeline point from a live snapshot.
    pub fn from_snapshot(snap: &crate::aggregate::Snapshot) -> TimelinePoint {
        Self::from_snapshots(snap, None)
    }

    /// Derive a timeline point using an exact aggregate snapshot for
    /// cross-tag and cross-protocol latency rollups.
    pub fn from_snapshots(
        snap: &crate::aggregate::Snapshot,
        exact: Option<&crate::aggregate::Snapshot>,
    ) -> TimelinePoint {
        let interval = if snap.interval_secs > 0.0 {
            snap.interval_secs
        } else {
            1.0
        };

        // Throughput over the interval (summed across all tag sets).
        let interval_count = |metric: &str| -> f64 {
            snap.series
                .iter()
                .filter(|s| s.metric == metric)
                .map(|s| s.interval_count)
                .sum::<u64>() as f64
        };

        // Failed fraction over this snapshot interval.
        let error_rate = {
            let (passes, total) = snap
                .series
                .iter()
                .filter(|s| s.metric == "http_req_failed")
                .fold((0.0_f64, 0_u64), |(p, t), s| {
                    (p + s.interval_sum, t + s.interval_count)
                });
            if total > 0 {
                passes / total as f64
            } else {
                0.0
            }
        };

        // Exact run-to-date request latency when the aggregator projection is
        // available. The weighted fallback is retained for old callers.
        let latency = |pick: fn(&AggValues) -> Option<f64>| -> Option<f64> {
            if let Some(value) = exact
                .and_then(|snapshot| {
                    snapshot.series.iter().find(|series| {
                        series.metric == "request_duration" && series.tags.is_empty()
                    })
                })
                .and_then(|series| pick(&series.agg))
            {
                return Some(value);
            }
            let mut acc = 0.0_f64;
            let mut total = 0_u64;
            for s in snap
                .series
                .iter()
                .filter(|s| crate::metrics::is_request_duration_metric(&s.metric))
            {
                if s.agg.count == 0 {
                    continue;
                }
                if let Some(v) = pick(&s.agg) {
                    acc += v * s.agg.count as f64;
                    total += s.agg.count;
                }
            }
            (total > 0).then_some(acc / total as f64)
        };

        let active_vus = snap
            .series
            .iter()
            .filter(|s| s.metric == "vus")
            .filter_map(|s| s.agg.last)
            .sum();

        TimelinePoint {
            elapsed_secs: snap.elapsed_secs,
            rps: snap.interval_request_count() as f64 / interval,
            iterations_ps: interval_count("iterations") / interval,
            active_vus,
            error_rate,
            latency_avg: latency(|a| a.avg),
            latency_p50: latency(|a| a.med),
            latency_p95: latency(|a| a.p95),
            latency_p99: latency(|a| a.p99),
            external: BTreeMap::new(),
        }
    }
}

/// The end-of-run summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub name: Option<String>,
    pub run_id: String,
    pub started_ms: u64,
    pub ended_ms: u64,
    pub duration_secs: f64,
    pub scenarios: Vec<String>,
    pub metrics: Vec<MetricSummary>,
    pub checks: Vec<CheckSummary>,
    pub thresholds: Vec<ThresholdStatus>,
    /// All thresholds passed (true when there are none).
    pub thresholds_passed: bool,
    /// Set when the run was aborted (reason).
    pub aborted: Option<String>,
    /// The final full snapshot (per-tag series detail).
    pub snapshot: Snapshot,
    /// Per-interval time series for charting (throughput, latency percentiles,
    /// active VUs, error rate). One point per snapshot interval. Empty for
    /// summaries produced before timeline capture existed.
    #[serde(default)]
    pub timeline: Vec<TimelinePoint>,
}

impl Summary {
    /// Build the summary from the final aggregator state.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        name: Option<String>,
        run_id: String,
        started_ms: u64,
        scenarios: Vec<String>,
        agg: &mut Aggregator,
        thresholds: Vec<ThresholdStatus>,
        aborted: Option<String>,
        timeline: Vec<TimelinePoint>,
    ) -> Summary {
        let snapshot = agg.snapshot();
        // Merge each metric across all tag sets.
        let metric_names: Vec<(String, MetricKind)> = {
            let mut seen = BTreeMap::new();
            for s in &snapshot.series {
                seen.entry(s.metric.clone()).or_insert(s.kind);
            }
            seen.into_iter().collect()
        };
        let mut metrics: Vec<MetricSummary> = metric_names
            .iter()
            .filter_map(|(m, _)| {
                agg.aggregate_selector(m, &[])
                    .map(|(kind, values)| MetricSummary {
                        metric: m.clone(),
                        kind,
                        agg: values,
                    })
            })
            .collect();
        let request_rollups = agg.aggregate_snapshot(&[]);
        metrics.retain(|metric| {
            !matches!(metric.metric.as_str(), "request_reqs" | "request_duration")
        });
        for metric in request_rollups.series.iter().filter(|series| {
            series.tags.is_empty()
                && matches!(series.metric.as_str(), "request_reqs" | "request_duration")
        }) {
            metrics.push(MetricSummary {
                metric: metric.metric.clone(),
                kind: metric.kind,
                agg: metric.agg.clone(),
            });
        }

        // Derived successful-throughput counter, sorted next to `request_reqs`.
        // `http_req_failed` is recorded by every protocol as a Rate where a "true"
        // sample means the request failed, so its `count` is the total request
        // count and `sum` the failure count. The difference is the number of
        // requests that got a non-error protocol response (transport ok and gRPC
        // status 0 / HTTP < 400) — this is NOT the count of application-accepted
        // requests. It gives users success-rate throughput without hand math.
        let request_reqs_ok = metrics
            .iter()
            .find(|m| m.metric == "http_req_failed")
            .filter(|m| m.agg.count > 0)
            .map(|m| {
                let ok = m.agg.count - m.agg.sum as u64;
                let per_second = if snapshot.elapsed_secs > 0.0 {
                    Some(ok as f64 / snapshot.elapsed_secs)
                } else {
                    None
                };
                MetricSummary {
                    metric: "request_reqs_ok".to_string(),
                    kind: MetricKind::Counter,
                    agg: AggValues {
                        count: ok,
                        sum: ok as f64,
                        per_second,
                        ..Default::default()
                    },
                }
            });
        if let Some(series) = request_reqs_ok {
            metrics.push(series);
        }

        metrics.sort_by(|a, b| a.metric.cmp(&b.metric));

        // Check summaries from `checks` series tagged with `check`.
        let mut checks: BTreeMap<String, CheckAccumulator> = BTreeMap::new();
        for s in snapshot.series.iter().filter(|s| s.metric == "checks") {
            let name = s
                .tags
                .get("check")
                .cloned()
                .unwrap_or_else(|| "unnamed".to_string());
            let entry = checks.entry(name).or_default();
            let passes = s.agg.sum as u64;
            entry.passes += passes;
            let fails = s.agg.count - passes;
            entry.fails += fails;
            if fails > 0 {
                if let Some(label) = s.tags.get("failure_group") {
                    let code = s
                        .tags
                        .get("failure_code")
                        .and_then(|value| value.parse::<i64>().ok());
                    *entry
                        .failure_groups
                        .entry((code, label.clone()))
                        .or_default() += fails;
                }
            }
        }
        let checks: Vec<CheckSummary> = checks
            .into_iter()
            .map(|(name, check)| CheckSummary {
                name,
                passes: check.passes,
                fails: check.fails,
                failure_groups: check
                    .failure_groups
                    .into_iter()
                    .map(|((code, label), count)| CheckFailureGroupSummary { code, label, count })
                    .collect(),
            })
            .collect();

        let thresholds_passed = thresholds.iter().all(|t| t.passed);
        let ended_ms = crate::metrics::now_millis();
        Summary {
            name,
            run_id,
            started_ms,
            ended_ms,
            duration_secs: snapshot.elapsed_secs,
            scenarios,
            metrics,
            checks,
            thresholds,
            thresholds_passed,
            aborted,
            snapshot,
            timeline,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// Console summary rendering (plain text; the CLI adds color).
    pub fn render_console(&self) -> String {
        let mut out = String::new();
        let title = self.name.as_deref().unwrap_or("loadr test");
        out.push_str(&format!(
            "\n  {} — {} scenario(s), {:.1}s\n\n",
            title,
            self.scenarios.len(),
            self.duration_secs
        ));
        if let Some(reason) = &self.aborted {
            out.push_str(&format!("  ! run aborted: {reason}\n\n"));
        }

        // Checks first.
        if !self.checks.is_empty() {
            let total_pass: u64 = self.checks.iter().map(|c| c.passes).sum();
            let total_fail: u64 = self.checks.iter().map(|c| c.fails).sum();
            let pct = if total_pass + total_fail > 0 {
                100.0 * total_pass as f64 / (total_pass + total_fail) as f64
            } else {
                100.0
            };
            out.push_str(&format!(
                "  checks{} {:>6.2}% — ✓ {} ✗ {}\n",
                dots("checks"),
                pct,
                total_pass,
                total_fail
            ));
            for c in &self.checks {
                let mark = if c.fails == 0 { "✓" } else { "✗" };
                out.push_str(&format!(
                    "    {mark} {} ({} / {})\n",
                    c.name,
                    c.passes,
                    c.passes + c.fails
                ));
                for group in &c.failure_groups {
                    let code = group
                        .code
                        .map(|code| format!(" [code {code}]"))
                        .unwrap_or_default();
                    out.push_str(&format!(
                        "        {}{}: {}\n",
                        group.label, code, group.count
                    ));
                }
            }
            out.push('\n');
        }

        for m in &self.metrics {
            if m.metric == "checks" {
                continue;
            }
            let line = match m.kind {
                MetricKind::Trend => format!(
                    "avg={} min={} med={} max={} p(90)={} p(95)={} p(99)={}",
                    fmt_ms(m.agg.avg),
                    fmt_ms(m.agg.min),
                    fmt_ms(m.agg.med),
                    fmt_ms(m.agg.max),
                    fmt_ms(m.agg.p90),
                    fmt_ms(m.agg.p95),
                    fmt_ms(m.agg.p99),
                ),
                MetricKind::Counter => format!(
                    "{} ({}/s)",
                    fmt_num(m.agg.sum),
                    fmt_num(m.agg.per_second.unwrap_or(0.0))
                ),
                // Failure-polarity rate (e.g. `http_req_failed`): a "true" sample
                // means the request FAILED, so `sum` is the failure count and the
                // rate is the failure rate. Put ✓ on the good (non-failed) outcome
                // and ✗ on failures, and label the percentage, so the glyphs keep
                // the ✓=good meaning they carry on the `checks` line.
                MetricKind::Rate if m.metric.ends_with("_failed") => {
                    let failed = m.agg.sum as u64;
                    let ok = m.agg.count.saturating_sub(failed);
                    format!(
                        "{:.2}% failed — ✓ {} ✗ {}",
                        m.agg.rate.unwrap_or(0.0) * 100.0,
                        ok,
                        failed
                    )
                }
                MetricKind::Rate => format!(
                    "{:.2}% — ✓ {} ✗ {}",
                    m.agg.rate.unwrap_or(0.0) * 100.0,
                    m.agg.sum as u64,
                    m.agg.count - m.agg.sum as u64
                ),
                MetricKind::Gauge => format!(
                    "value={} min={} max={}",
                    fmt_num(m.agg.last.unwrap_or(0.0)),
                    fmt_num(m.agg.min.unwrap_or(0.0)),
                    fmt_num(m.agg.max.unwrap_or(0.0))
                ),
            };
            out.push_str(&format!("  {}{} {}\n", m.metric, dots(&m.metric), line));
        }

        if !self.thresholds.is_empty() {
            out.push_str("\n  thresholds:\n");
            for t in &self.thresholds {
                let mark = if t.passed { "✓" } else { "✗" };
                let observed = t
                    .observed
                    .map(|v| format!("{v:.2}"))
                    .unwrap_or_else(|| "no samples".to_string());
                out.push_str(&format!(
                    "    {mark} {}: {} (observed: {})\n",
                    t.metric, t.expression, observed
                ));
            }
        }
        out.push('\n');
        out
    }

    /// Render the run as a JUnit XML report.
    ///
    /// Each threshold and each check becomes a `<testcase>`, grouped into a
    /// `thresholds` and a `checks` `<testsuite>`. A failed threshold, a check
    /// with any failures, and an aborted run each emit a `<failure>`. This is the
    /// shape every CI test reporter (GitHub Actions, GitLab, Jenkins, Bamboo)
    /// understands, so a loadr run drops straight into a pipeline's test panel.
    pub fn render_junit(&self) -> String {
        let suite_name = self.name.as_deref().unwrap_or("loadr test");
        let time = format!("{:.3}", self.duration_secs);

        // Thresholds suite: one testcase per threshold.
        let mut threshold_cases = String::new();
        let mut threshold_failures = 0u64;
        for t in &self.thresholds {
            let observed = t
                .observed
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "no samples".to_string());
            let name = format!("{}: {}", t.metric, t.expression);
            if t.passed {
                threshold_cases.push_str(&format!(
                    "    <testcase name=\"{}\" classname=\"threshold\"/>\n",
                    xml_escape(&name)
                ));
            } else {
                threshold_failures += 1;
                let msg = format!("threshold {} failed (observed: {})", t.expression, observed);
                threshold_cases.push_str(&format!(
                    "    <testcase name=\"{}\" classname=\"threshold\">\n      <failure message=\"{}\"/>\n    </testcase>\n",
                    xml_escape(&name),
                    xml_escape(&msg)
                ));
            }
        }

        // Checks suite: one testcase per named check.
        let mut check_cases = String::new();
        let mut check_failures = 0u64;
        for c in &self.checks {
            let total = c.passes + c.fails;
            if c.fails == 0 {
                check_cases.push_str(&format!(
                    "    <testcase name=\"{}\" classname=\"check\"/>\n",
                    xml_escape(&c.name)
                ));
            } else {
                check_failures += 1;
                let groups = if c.failure_groups.is_empty() {
                    String::new()
                } else {
                    let values = c
                        .failure_groups
                        .iter()
                        .map(|group| match group.code {
                            Some(code) => {
                                format!("{} (code {}): {}", group.label, code, group.count)
                            }
                            None => format!("{}: {}", group.label, group.count),
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("; failure groups: {values}")
                };
                let msg = format!("{} of {} checks failed{}", c.fails, total, groups);
                check_cases.push_str(&format!(
                    "    <testcase name=\"{}\" classname=\"check\">\n      <failure message=\"{}\"/>\n    </testcase>\n",
                    xml_escape(&c.name),
                    xml_escape(&msg)
                ));
            }
        }

        // A run-level testcase that fails when the run was aborted, so a crash or
        // abort_on_fail never reports as an all-green suite.
        let (run_case, run_failures) = match &self.aborted {
            Some(reason) => (
                format!(
                    "    <testcase name=\"run completed\" classname=\"run\">\n      <failure message=\"run aborted: {}\"/>\n    </testcase>\n",
                    xml_escape(reason)
                ),
                1u64,
            ),
            None => (
                "    <testcase name=\"run completed\" classname=\"run\"/>\n".to_string(),
                0u64,
            ),
        };

        let threshold_total = self.thresholds.len() as u64;
        let check_total = self.checks.len() as u64;
        let total_tests = threshold_total + check_total + 1;
        let total_failures = threshold_failures + check_failures + run_failures;

        let mut out = String::new();
        out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        out.push_str(&format!(
            "<testsuites name=\"loadr: {}\" tests=\"{}\" failures=\"{}\" time=\"{}\">\n",
            xml_escape(suite_name),
            total_tests,
            total_failures,
            time
        ));
        out.push_str(&format!(
            "  <testsuite name=\"thresholds\" tests=\"{}\" failures=\"{}\" time=\"{}\">\n{}  </testsuite>\n",
            threshold_total, threshold_failures, time, threshold_cases
        ));
        out.push_str(&format!(
            "  <testsuite name=\"checks\" tests=\"{}\" failures=\"{}\" time=\"{}\">\n{}  </testsuite>\n",
            check_total, check_failures, time, check_cases
        ));
        out.push_str(&format!(
            "  <testsuite name=\"run\" tests=\"1\" failures=\"{}\" time=\"{}\">\n{}  </testsuite>\n",
            run_failures, time, run_case
        ));
        out.push_str("</testsuites>\n");
        out
    }
}

/// Escape the five XML special characters for safe attribute/text content.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

fn dots(name: &str) -> String {
    let width = 30usize.saturating_sub(name.len());
    format!("{}:", ".".repeat(width.max(2)))
}

fn fmt_ms(v: Option<f64>) -> String {
    match v {
        None => "-".to_string(),
        Some(ms) if ms >= 1000.0 => format!("{:.2}s", ms / 1000.0),
        Some(ms) if ms >= 1.0 => format!("{ms:.2}ms"),
        Some(ms) => format!("{:.0}µs", ms * 1000.0),
    }
}

fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v:.2}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{now_millis, Sample, Tags};
    use std::sync::Arc;

    fn build_summary() -> Summary {
        let mut agg = Aggregator::new();
        let mut tags = Tags::new();
        tags.insert("check".into(), "status is 200".into());
        let tags = Arc::new(tags);
        for i in 0..10 {
            agg.record(&Sample {
                metric: Arc::from("checks"),
                kind: MetricKind::Rate,
                value: if i < 9 { 1.0 } else { 0.0 },
                tags: tags.clone(),
                timestamp_ms: now_millis(),
            });
            agg.record(&Sample {
                metric: Arc::from("http_req_duration"),
                kind: MetricKind::Trend,
                value: 10.0 + i as f64,
                tags: Arc::new(Tags::new()),
                timestamp_ms: now_millis(),
            });
        }
        Summary::build(
            Some("demo".into()),
            "run-1".into(),
            now_millis(),
            vec!["default".into()],
            &mut agg,
            vec![ThresholdStatus {
                metric: "http_req_duration".into(),
                expression: "p(95)<400".into(),
                observed: Some(18.0),
                passed: true,
                abort_on_fail: false,
            }],
            None,
            Vec::new(),
        )
    }

    #[test]
    fn builds_checks_and_metrics() {
        let s = build_summary();
        assert_eq!(s.checks.len(), 1);
        assert_eq!(s.checks[0].passes, 9);
        assert_eq!(s.checks[0].fails, 1);
        assert!(s.thresholds_passed);
        assert!(s
            .metrics
            .iter()
            .any(|m| m.metric == "http_req_duration" && m.agg.count == 10));
    }

    #[test]
    fn distributed_merge_preserves_bounded_failure_groups() {
        fn record_failures(agg: &mut Aggregator, code: i64, label: &str, count: usize) {
            let mut tags = Tags::new();
            tags.insert("check".into(), "admission_accepted".into());
            tags.insert("failure_code".into(), code.to_string());
            tags.insert("failure_group".into(), label.to_string());
            let tags = Arc::new(tags);
            for _ in 0..count {
                agg.record(&Sample {
                    metric: Arc::from("checks"),
                    kind: MetricKind::Rate,
                    value: 0.0,
                    tags: tags.clone(),
                    timestamp_ms: now_millis(),
                });
            }
        }

        let mut agent_a = Aggregator::new();
        let mut agent_b = Aggregator::new();
        record_failures(&mut agent_a, 18, "WrongShard", 2);
        record_failures(&mut agent_b, 18, "WrongShard", 3);
        record_failures(&mut agent_b, 20, "PoolAtCapacity", 4);
        let mut controller = Aggregator::new();
        controller.merge_delta(&agent_a.take_delta());
        controller.merge_delta(&agent_b.take_delta());

        let summary = Summary::build(
            Some("distributed".into()),
            "run-groups".into(),
            now_millis(),
            vec!["default".into()],
            &mut controller,
            Vec::new(),
            None,
            Vec::new(),
        );
        let check = &summary.checks[0];
        assert_eq!(check.fails, 9);
        assert_eq!(check.failure_groups.len(), 2);
        assert!(check.failure_groups.iter().any(|group| {
            group.code == Some(18) && group.label == "WrongShard" && group.count == 5
        }));
        assert!(check.failure_groups.iter().any(|group| {
            group.code == Some(20) && group.label == "PoolAtCapacity" && group.count == 4
        }));
        assert!(summary.render_console().contains("WrongShard [code 18]: 5"));
        assert!(summary.render_junit().contains("WrongShard (code 18): 5"));
    }

    #[test]
    fn console_render_contains_key_lines() {
        let s = build_summary();
        let text = s.render_console();
        assert!(text.contains("checks"));
        assert!(text.contains("✓ 9 ✗ 1"));
        assert!(text.contains("http_req_duration"));
        assert!(text.contains("p(95)<400"));
        assert!(text.contains("✓ http_req_duration"));
    }

    /// Build a summary from `n` requests of which `failed` reported a failure via
    /// the `http_req_failed` rate (as every protocol records it).
    fn build_request_summary(n: usize, failed: usize) -> Summary {
        let mut agg = Aggregator::new();
        let tags = Arc::new(Tags::new());
        for i in 0..n {
            agg.record(&Sample {
                metric: Arc::from("http_req_failed"),
                kind: MetricKind::Rate,
                value: if i < failed { 1.0 } else { 0.0 },
                tags: tags.clone(),
                timestamp_ms: now_millis(),
            });
            agg.record(&Sample {
                metric: Arc::from("grpc_reqs"),
                kind: MetricKind::Counter,
                value: 1.0,
                tags: tags.clone(),
                timestamp_ms: now_millis(),
            });
        }
        Summary::build(
            Some("grpc".into()),
            "run-req".into(),
            now_millis(),
            vec!["default".into()],
            &mut agg,
            Vec::new(),
            None,
            Vec::new(),
        )
    }

    #[test]
    fn http_req_failed_marks_ok_with_check_and_failed_with_cross() {
        // 8 of 10 requests failed -> 80% failure rate.
        let s = build_request_summary(10, 8);
        let text = s.render_console();
        // ✓ counts the 2 successful requests, ✗ the 8 failures, and the
        // percentage is labelled as the failure rate.
        assert!(
            text.contains("80.00% failed — ✓ 2 ✗ 8"),
            "unexpected http_req_failed line:\n{text}"
        );
    }

    #[test]
    fn request_reqs_ok_counts_non_failed_requests() {
        let s = build_request_summary(10, 8);
        let ok = s
            .metrics
            .iter()
            .find(|m| m.metric == "request_reqs_ok")
            .expect("request_reqs_ok should be synthesized when requests exist");
        assert_eq!(ok.kind, MetricKind::Counter);
        assert_eq!(ok.agg.count, 2);
        assert_eq!(ok.agg.sum as u64, 2);
        // request_reqs_ok == total requests - failed requests.
        let failed = s
            .metrics
            .iter()
            .find(|m| m.metric == "http_req_failed")
            .expect("http_req_failed present");
        assert_eq!(ok.agg.count, failed.agg.count - failed.agg.sum as u64);
    }

    #[test]
    fn request_reqs_ok_absent_without_requests() {
        // The demo summary records checks and durations but no http_req_failed.
        let s = build_summary();
        assert!(s.metrics.iter().all(|m| m.metric != "request_reqs_ok"));
    }

    #[test]
    fn junit_render_passing_threshold_and_run() {
        let s = build_summary();
        let xml = s.render_junit();
        assert!(xml.starts_with("<?xml version=\"1.0\""));
        assert!(xml.contains("<testsuites name=\"loadr: demo\""));
        // 1 threshold + 1 check + 1 run testcase; the check has 1 failing sample.
        assert!(xml.contains("tests=\"3\" failures=\"1\""));
        // Passing threshold and completed run are self-closing (no failure).
        assert!(xml.contains(
            "<testcase name=\"http_req_duration: p(95)&lt;400\" classname=\"threshold\"/>"
        ));
        assert!(xml.contains("<testcase name=\"run completed\" classname=\"run\"/>"));
        // The check with one failed sample reports a failure.
        assert!(xml.contains("<testcase name=\"status is 200\" classname=\"check\">"));
        assert!(xml.contains("1 of 10 checks failed"));
    }

    #[test]
    fn junit_render_marks_failures_and_escapes() {
        let mut agg = Aggregator::new();
        let mut tags = Tags::new();
        tags.insert("check".into(), "body has <tag> & \"quote\"".into());
        let tags = Arc::new(tags);
        for i in 0..10 {
            agg.record(&Sample {
                metric: Arc::from("checks"),
                kind: MetricKind::Rate,
                value: if i < 6 { 1.0 } else { 0.0 },
                tags: tags.clone(),
                timestamp_ms: now_millis(),
            });
        }
        let s = Summary::build(
            Some("esc & <test>".into()),
            "run-2".into(),
            now_millis(),
            vec!["default".into()],
            &mut agg,
            vec![ThresholdStatus {
                metric: "http_req_failed".into(),
                expression: "rate<0.01".into(),
                observed: Some(0.4),
                passed: false,
                abort_on_fail: false,
            }],
            Some("threshold rate<0.01 crossed".into()),
            Vec::new(),
        );
        let xml = s.render_junit();
        // 1 threshold + 1 check + 1 run, all three failing.
        assert!(xml.contains("tests=\"3\" failures=\"3\""));
        assert!(xml.contains("<testsuites name=\"loadr: esc &amp; &lt;test&gt;\""));
        assert!(xml.contains("body has &lt;tag&gt; &amp; &quot;quote&quot;"));
        assert!(xml.contains("4 of 10 checks failed"));
        assert!(xml.contains("threshold rate&lt;0.01 failed (observed: 0.40)"));
        assert!(xml.contains("run aborted: threshold rate&lt;0.01 crossed"));
    }

    #[test]
    fn json_round_trip() {
        let s = build_summary();
        let json = s.to_json();
        assert!(json["checks"][0].get("failure_groups").is_none());
        let back: Summary = serde_json::from_value(json).expect("round trip");
        assert_eq!(back.run_id, "run-1");
        assert_eq!(back.checks.len(), 1);
    }

    #[test]
    fn timeline_point_from_snapshot() {
        let mut agg = Aggregator::new();
        for i in 0..20 {
            agg.record(&Sample {
                metric: Arc::from("http_reqs"),
                kind: MetricKind::Counter,
                value: 1.0,
                tags: Arc::new(Tags::new()),
                timestamp_ms: now_millis(),
            });
            agg.record(&Sample {
                metric: Arc::from("http_req_duration"),
                kind: MetricKind::Trend,
                value: 10.0 + i as f64,
                tags: Arc::new(Tags::new()),
                timestamp_ms: now_millis(),
            });
            agg.record(&Sample {
                metric: Arc::from("http_req_failed"),
                kind: MetricKind::Rate,
                value: if i % 4 == 0 { 1.0 } else { 0.0 },
                tags: Arc::new(Tags::new()),
                timestamp_ms: now_millis(),
            });
        }
        agg.record(&Sample {
            metric: Arc::from("vus"),
            kind: MetricKind::Gauge,
            value: 5.0,
            tags: Arc::new(Tags::new()),
            timestamp_ms: now_millis(),
        });
        let snap = agg.snapshot();
        let p = TimelinePoint::from_snapshot(&snap);
        assert!(p.rps > 0.0, "rps should be positive");
        assert_eq!(p.active_vus, 5.0);
        // 5 of 20 requests failed → 25%.
        assert!((p.error_rate - 0.25).abs() < 1e-9, "err={}", p.error_rate);
        assert!(p.latency_p95.unwrap() >= p.latency_p50.unwrap());
        assert!(p.latency_avg.unwrap() > 0.0);
    }

    #[test]
    fn timeline_counts_plugin_protocol_requests() {
        // A plugin-only run emits `<family>_reqs` / `<family>_req_duration`,
        // never `http_reqs`. The timeline must still report real throughput and
        // latency. Regression: live/report RPS used to be hardcoded to
        // http_reqs, so plugin runs showed 0 RPS while saturated.
        let mut agg = Aggregator::new();
        for i in 0..15 {
            agg.record(&Sample {
                metric: Arc::from("mongo_reqs"),
                kind: MetricKind::Counter,
                value: 1.0,
                tags: Arc::new(Tags::new()),
                timestamp_ms: now_millis(),
            });
            agg.record(&Sample {
                metric: Arc::from("mongo_req_duration"),
                kind: MetricKind::Trend,
                value: 5.0 + i as f64,
                tags: Arc::new(Tags::new()),
                timestamp_ms: now_millis(),
            });
        }
        let snap = agg.snapshot();
        assert_eq!(
            snap.interval_request_count(),
            15,
            "rollup should count plugin _reqs families"
        );
        let p = TimelinePoint::from_snapshot(&snap);
        assert!(p.rps > 0.0, "plugin rps should be positive, got {}", p.rps);
        assert!(
            p.latency_p95.unwrap() > 0.0,
            "plugin latency should be reported"
        );
    }

    #[test]
    fn timeline_survives_round_trip() {
        let mut s = build_summary();
        s.timeline = vec![TimelinePoint {
            elapsed_secs: 1.0,
            rps: 10.0,
            iterations_ps: 4.0,
            active_vus: 3.0,
            error_rate: 0.1,
            latency_avg: Some(12.0),
            latency_p50: Some(10.0),
            latency_p95: Some(40.0),
            latency_p99: None,
            external: BTreeMap::new(),
        }];
        let back: Summary = serde_json::from_value(s.to_json()).expect("round trip");
        assert_eq!(back.timeline.len(), 1);
        assert_eq!(back.timeline[0].rps, 10.0);
        assert_eq!(back.timeline[0].latency_p99, None);
    }
}
