//! `loadr compare` — diff two summary JSON exports and gate CI on regressions.
//!
//! Loads a baseline and a current `loadr run --summary-export` file, computes
//! per-metric deltas with direction-aware regression detection (latency up,
//! throughput down, error rate up, checks pass-rate down), and renders the
//! result as a terminal table, GitHub-flavoured markdown, or JSON. With
//! `--assert` the process exits with the threshold-failure code (99) when any
//! regression exceeds its tolerance.

use std::path::{Path, PathBuf};

use clap::Args;
use loadr_core::summary::{CheckSummary, MetricSummary};
use loadr_core::{MetricKind, Summary};
use owo_colors::OwoColorize;
use serde::Serialize;

/// Default relative tolerance applied to the default-gated fields when no
/// `--max-regression` spec matches them.
const DEFAULT_TOLERANCE_PCT: f64 = 5.0;

/// Fields gated by default; all other fields only gate when an explicit
/// `--max-regression` spec targets them.
const DEFAULT_GATED: [&str; 4] = ["p95", "p99", "error_rate", "pass_rate"];

/// Terminal table headers (plain ASCII; markdown gets the pretty ones).
const HEADERS: [&str; 7] = [
    "metric", "field", "baseline", "current", "delta", "delta %", "",
];

/// Markdown table headers.
const MD_HEADERS: [&str; 7] = [
    "Metric", "Field", "Baseline", "Current", "Δ", "Δ%", "Verdict",
];

#[derive(Args)]
pub struct CompareArgs {
    /// Baseline summary JSON (`loadr run --summary-export`)
    pub baseline: PathBuf,
    /// Current summary JSON to compare against the baseline
    pub current: PathBuf,
    /// Write the comparison as JSON
    #[arg(long, value_name = "PATH")]
    pub output: Option<PathBuf>,
    /// Write a GitHub-flavoured markdown table (for PR comments)
    #[arg(long, value_name = "PATH")]
    pub markdown: Option<PathBuf>,
    /// Regression tolerance, `field=limit` or `metric.field=limit`
    /// (e.g. `p95=10%`, `error_rate=0.5`, `http_req_duration.p95=25`).
    /// Absolute limits use the field's display unit: milliseconds for latency,
    /// percentage points for rates. Repeatable.
    #[arg(long, value_name = "SPEC", value_parser = parse_tolerance)]
    pub max_regression: Vec<Tolerance>,
    /// Exit 99 when any regression exceeds its tolerance
    #[arg(long)]
    pub assert: bool,
}

/// A parsed `--max-regression` spec.
#[derive(Debug, Clone, PartialEq)]
pub struct Tolerance {
    /// Metric scope (`http_req_duration.p95=10%`); `None` applies the spec to
    /// every metric exposing the field.
    pub metric: Option<String>,
    /// Canonical field name (`p50`/`p95`/`p99`/`avg`/`per_second`/`count`/
    /// `error_rate`/`pass_rate`).
    pub field: String,
    pub limit: Limit,
}

/// How much worsening a tolerance allows.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Limit {
    /// Relative to the baseline value (`10%` → 10.0).
    Percent(f64),
    /// Absolute, in the field's display unit (ms, percentage points, …).
    Absolute(f64),
}

/// The full diff between two summaries.
#[derive(Debug, Clone, Serialize)]
pub struct Comparison {
    pub rows: Vec<MetricDelta>,
}

impl Comparison {
    /// Rows that worsened beyond their tolerance.
    pub fn regression_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|r| r.regression == Some(true))
            .count()
    }
}

/// One compared field of one metric.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MetricDelta {
    pub metric: String,
    pub field: String,
    pub baseline: f64,
    pub current: f64,
    pub delta_abs: f64,
    /// Relative change in percent; `None` when the baseline is zero.
    pub delta_pct: Option<f64>,
    /// `Some(true)` when the value worsened beyond its tolerance, `Some(false)`
    /// when a gate applies and holds, `None` when the field has no worse
    /// direction or no gate targets it.
    pub regression: Option<bool>,
}

/// Which way a field gets worse.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Direction {
    LowerIsBetter,
    HigherIsBetter,
    Neutral,
}

pub fn execute(args: CompareArgs) -> anyhow::Result<i32> {
    let baseline = load_summary(&args.baseline)?;
    let current = load_summary(&args.current)?;
    let comparison = diff(&baseline, &current, &args.max_regression);
    if comparison.rows.is_empty() {
        anyhow::bail!(
            "{} and {} share no comparable metrics",
            args.baseline.display(),
            args.current.display()
        );
    }
    print!("{}", render_terminal(&comparison));

    if let Some(path) = &args.output {
        std::fs::write(path, serde_json::to_string_pretty(&comparison)?)?;
        eprintln!(
            "{} comparison JSON written to {}",
            "✓".green(),
            path.display()
        );
    }
    if let Some(path) = &args.markdown {
        std::fs::write(path, render_markdown(&comparison))?;
        eprintln!("{} markdown written to {}", "✓".green(), path.display());
    }

    let regressions = comparison.regression_count();
    if regressions > 0 {
        eprintln!("{} {regressions} regression(s) beyond tolerance", "✗".red());
    } else {
        eprintln!("{} no regressions beyond tolerance", "✓".green());
    }
    Ok(if args.assert && regressions > 0 {
        loadr_core::EXIT_THRESHOLD_FAILED
    } else {
        0
    })
}

fn load_summary(path: &Path) -> anyhow::Result<Summary> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("{} is not a loadr summary export: {e}", path.display()))
}

/// Parse one `--max-regression` spec (`p95=10%`, `rps=-5%`,
/// `http_req_duration.p95=25`). The sign of the limit is ignored — a tolerance
/// is always a magnitude of allowed worsening.
fn parse_tolerance(spec: &str) -> Result<Tolerance, String> {
    let (target, limit) = spec
        .split_once('=')
        .ok_or_else(|| format!("expected `field=limit` (e.g. `p95=10%`), got `{spec}`"))?;
    let target = target.trim();
    if target.is_empty() {
        return Err(format!("missing field in `{spec}`"));
    }
    let (metric, field) = match target.rsplit_once('.') {
        Some((metric, field)) => (Some(metric.to_string()), field),
        None => (None, target),
    };
    if field.is_empty() {
        return Err(format!("missing field in `{spec}`"));
    }
    Ok(Tolerance {
        metric,
        field: normalize_field(field),
        limit: parse_limit(limit.trim())?,
    })
}

/// Parse the limit side of a tolerance spec (`10%` or `25`).
fn parse_limit(text: &str) -> Result<Limit, String> {
    if let Some(pct) = text.strip_suffix('%') {
        match pct.trim().parse() {
            Ok(v) => Ok(Limit::Percent(v)),
            Err(e) => Err(format!("bad percentage `{text}`: {e}")),
        }
    } else {
        match text.parse() {
            Ok(v) => Ok(Limit::Absolute(v)),
            Err(e) => Err(format!("bad number `{text}`: {e}")),
        }
    }
}

/// Map field aliases to the canonical names used in [`MetricDelta::field`].
fn normalize_field(field: &str) -> String {
    match field.to_ascii_lowercase().as_str() {
        "med" | "p50" => "p50".to_string(),
        "rps" | "per_second" => "per_second".to_string(),
        "checks" | "pass_rate" => "pass_rate".to_string(),
        other => other.to_string(),
    }
}

/// Diff two summaries. Pure: compares metrics present in both, plus a
/// synthetic checks pass-rate row and a thresholds-passed row.
pub(crate) fn diff(baseline: &Summary, current: &Summary, tolerances: &[Tolerance]) -> Comparison {
    let mut rows = Vec::new();
    for b in &baseline.metrics {
        // The `checks` rate metric is covered by the pass-rate row below.
        if b.metric == "checks" {
            continue;
        }
        let Some(c) = current.metrics.iter().find(|m| m.metric == b.metric) else {
            continue;
        };
        if c.kind != b.kind {
            continue;
        }
        let current_fields = fields_of(c);
        for (field, bv) in fields_of(b) {
            let Some(&(_, cv)) = current_fields.iter().find(|(f, _)| *f == field) else {
                continue;
            };
            rows.push(make_delta(&b.metric, b.kind, field, bv, cv, tolerances));
        }
    }

    // Checks pass-rate (percent), merged across all named checks.
    if let (Some(b), Some(c)) = (pass_rate(&baseline.checks), pass_rate(&current.checks)) {
        rows.push(make_delta(
            "checks",
            MetricKind::Rate,
            "pass_rate",
            b,
            c,
            tolerances,
        ));
    }

    // Thresholds verdict: a newly failing gate is always a regression.
    if !baseline.thresholds.is_empty() || !current.thresholds.is_empty() {
        let b = f64::from(u8::from(baseline.thresholds_passed));
        let c = f64::from(u8::from(current.thresholds_passed));
        rows.push(MetricDelta {
            metric: "thresholds".to_string(),
            field: "passed".to_string(),
            baseline: b,
            current: c,
            delta_abs: c - b,
            delta_pct: None,
            regression: Some(b > c),
        });
    }

    Comparison { rows }
}

/// The comparable fields for one metric, by kind. Rate values are scaled to
/// percent so absolute tolerances read as percentage points.
fn fields_of(m: &MetricSummary) -> Vec<(&'static str, f64)> {
    let a = &m.agg;
    match m.kind {
        MetricKind::Trend => [
            ("avg", a.avg),
            ("p50", a.med),
            ("p95", a.p95),
            ("p99", a.p99),
        ]
        .into_iter()
        .filter_map(|(field, v)| v.map(|v| (field, v)))
        .collect(),
        MetricKind::Counter => {
            let mut out = vec![("count", a.sum)];
            if let Some(ps) = a.per_second {
                out.push(("per_second", ps));
            }
            out
        }
        MetricKind::Rate => a
            .rate
            .map(|r| vec![(rate_field(&m.metric), r * 100.0)])
            .unwrap_or_default(),
        // Gauges (vus, …) describe configuration, not performance.
        MetricKind::Gauge => Vec::new(),
    }
}

/// Field name for a rate metric: failure rates get the gated `error_rate`.
fn rate_field(metric: &str) -> &'static str {
    if metric.contains("failed") || metric.contains("error") {
        "error_rate"
    } else {
        "rate"
    }
}

/// Merged pass percentage across all checks (`None` when there are none).
fn pass_rate(checks: &[CheckSummary]) -> Option<f64> {
    let (passes, total) = checks.iter().fold((0u64, 0u64), |(p, t), c| {
        (p + c.passes, t + c.passes + c.fails)
    });
    (total > 0).then(|| 100.0 * passes as f64 / total as f64)
}

fn make_delta(
    metric: &str,
    kind: MetricKind,
    field: &'static str,
    baseline: f64,
    current: f64,
    tolerances: &[Tolerance],
) -> MetricDelta {
    let delta_abs = current - baseline;
    let delta_pct = (baseline != 0.0).then(|| delta_abs / baseline * 100.0);
    let regression = regression_for(
        direction(kind, metric, field),
        baseline,
        current,
        field,
        find_tolerance(tolerances, metric, field),
    );
    MetricDelta {
        metric: metric.to_string(),
        field: field.to_string(),
        baseline,
        current,
        delta_abs,
        delta_pct,
        regression,
    }
}

/// Which way this metric/field combination gets worse.
fn direction(kind: MetricKind, metric: &str, field: &str) -> Direction {
    match kind {
        MetricKind::Trend => Direction::LowerIsBetter,
        MetricKind::Counter if is_throughput(metric) => Direction::HigherIsBetter,
        MetricKind::Counter | MetricKind::Gauge => Direction::Neutral,
        MetricKind::Rate if field == "error_rate" => Direction::LowerIsBetter,
        MetricKind::Rate if field == "pass_rate" => Direction::HigherIsBetter,
        MetricKind::Rate => Direction::Neutral,
    }
}

/// Request/iteration counters where a drop means lost throughput. Matches the
/// `<family>_reqs` convention plugin protocols follow too.
fn is_throughput(metric: &str) -> bool {
    metric == "iterations" || metric.ends_with("_reqs")
}

/// Most specific matching tolerance: metric-scoped beats field-wide.
fn find_tolerance<'a>(
    tolerances: &'a [Tolerance],
    metric: &str,
    field: &str,
) -> Option<&'a Tolerance> {
    let scoped = tolerances
        .iter()
        .find(|t| t.metric.as_deref() == Some(metric) && t.field == field);
    scoped.or_else(|| {
        tolerances
            .iter()
            .find(|t| t.metric.is_none() && t.field == field)
    })
}

/// Gate verdict for one row. Explicitly toleranced fields always gate; the
/// [`DEFAULT_GATED`] fields gate at [`DEFAULT_TOLERANCE_PCT`]; everything else
/// is informational (`None`).
fn regression_for(
    dir: Direction,
    baseline: f64,
    current: f64,
    field: &str,
    tolerance: Option<&Tolerance>,
) -> Option<bool> {
    let worsening = match dir {
        Direction::LowerIsBetter => current - baseline,
        Direction::HigherIsBetter => baseline - current,
        Direction::Neutral => return None,
    };
    let limit = match tolerance {
        Some(t) => t.limit,
        None if DEFAULT_GATED.contains(&field) => Limit::Percent(DEFAULT_TOLERANCE_PCT),
        None => return None,
    };
    Some(exceeds(worsening, baseline, limit))
}

/// True when `worsening` is beyond what `limit` allows (with float slack).
fn exceeds(worsening: f64, baseline: f64, limit: Limit) -> bool {
    let allowance = match limit {
        Limit::Percent(p) => baseline.abs() * (p.abs() / 100.0),
        Limit::Absolute(a) => a.abs(),
    };
    worsening > allowance + 1e-9
}

// ---------------------------------------------------------------------------
// Rendering (shared with `loadr sweep`).

/// Align rows into a two-space-separated text table. Returns the header line,
/// a dash separator, then one line per row.
pub(crate) fn render_text_table(headers: &[&str], rows: &[Vec<String>]) -> Vec<String> {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate().take(widths.len()) {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let format_line = |cells: &[&str]| -> String {
        let mut line = String::from("  ");
        for (i, width) in widths.iter().enumerate() {
            let cell = cells.get(i).copied().unwrap_or("");
            let pad = width.saturating_sub(cell.chars().count());
            line.push_str(cell);
            line.push_str(&" ".repeat(pad + 2));
        }
        line.trim_end().to_string()
    };
    let mut lines = Vec::with_capacity(rows.len() + 2);
    lines.push(format_line(headers));
    let dashes: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    lines.push(format_line(
        &dashes.iter().map(String::as_str).collect::<Vec<_>>(),
    ));
    for row in rows {
        lines.push(format_line(
            &row.iter().map(String::as_str).collect::<Vec<_>>(),
        ));
    }
    lines
}

/// Render a GitHub-flavoured markdown table (cells get `|` escaped).
pub(crate) fn render_markdown_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut out = String::new();
    out.push_str(&format!("| {} |\n", headers.join(" | ")));
    out.push_str(&format!("|{}|\n", vec![" --- "; headers.len()].join("|")));
    for row in rows {
        let cells: Vec<String> = row.iter().map(|c| c.replace('|', "\\|")).collect();
        out.push_str(&format!("| {} |\n", cells.join(" | ")));
    }
    out
}

/// Milliseconds (`1.20s` / `12.34ms` / `800µs`).
pub(crate) fn fmt_latency(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else if ms >= 1.0 {
        format!("{ms:.2}ms")
    } else {
        format!("{:.0}µs", ms * 1000.0)
    }
}

/// A value that is already in percent.
pub(crate) fn fmt_pct(pct: f64) -> String {
    format!("{pct:.2}%")
}

/// Events per second.
pub(crate) fn fmt_per_second(rate: f64) -> String {
    format!("{rate:.1}/s")
}

/// Plain number: integer when whole.
pub(crate) fn fmt_count(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v:.2}")
    }
}

/// Format one value in its field's display unit.
fn fmt_value(field: &str, v: f64) -> String {
    match field {
        "avg" | "p50" | "p95" | "p99" => fmt_latency(v),
        "error_rate" | "rate" | "pass_rate" => fmt_pct(v),
        "per_second" => fmt_per_second(v),
        "passed" => {
            let text = if v >= 1.0 { "pass" } else { "fail" };
            text.to_string()
        }
        _ => fmt_count(v),
    }
}

fn fmt_delta(d: &MetricDelta) -> String {
    if d.field == "passed" {
        return "-".to_string();
    }
    let sign = if d.delta_abs < 0.0 { "-" } else { "+" };
    format!("{sign}{}", fmt_value(&d.field, d.delta_abs.abs()))
}

fn fmt_delta_pct(d: &MetricDelta) -> String {
    if d.field == "passed" {
        return "-".to_string();
    }
    match d.delta_pct {
        None => "-".to_string(),
        Some(pct) => {
            let arrow = if d.delta_abs > 0.0 {
                "▲ "
            } else if d.delta_abs < 0.0 {
                "▼ "
            } else {
                ""
            };
            format!("{arrow}{:.1}%", pct.abs())
        }
    }
}

fn terminal_cells(d: &MetricDelta) -> Vec<String> {
    let mark = match d.regression {
        Some(true) => "✗",
        Some(false) => "✓",
        None => "",
    };
    vec![
        d.metric.clone(),
        d.field.clone(),
        fmt_value(&d.field, d.baseline),
        fmt_value(&d.field, d.current),
        fmt_delta(d),
        fmt_delta_pct(d),
        mark.to_string(),
    ]
}

fn render_terminal(cmp: &Comparison) -> String {
    let rows: Vec<Vec<String>> = cmp.rows.iter().map(terminal_cells).collect();
    let lines = render_text_table(&HEADERS, &rows);
    let mut out = String::from("\n");
    for (i, line) in lines.iter().enumerate() {
        // Lines 0 and 1 are the header and separator.
        let row = i.checked_sub(2).and_then(|r| cmp.rows.get(r));
        match row.and_then(|r| r.regression) {
            Some(true) => out.push_str(&line.red().to_string()),
            Some(false) => out.push_str(&line.green().to_string()),
            None => out.push_str(line),
        }
        out.push('\n');
    }
    out.push('\n');
    out
}

fn markdown_cells(d: &MetricDelta) -> Vec<String> {
    let bold = d.regression == Some(true);
    let emph = |s: String| if bold { format!("**{s}**") } else { s };
    let verdict = match d.regression {
        Some(true) => "**regression**".to_string(),
        Some(false) => "ok".to_string(),
        None => String::new(),
    };
    vec![
        format!("`{}`", d.metric),
        d.field.clone(),
        fmt_value(&d.field, d.baseline),
        fmt_value(&d.field, d.current),
        emph(fmt_delta(d)),
        emph(fmt_delta_pct(d)),
        verdict,
    ]
}

/// GitHub-flavoured markdown suitable for a PR comment.
pub(crate) fn render_markdown(cmp: &Comparison) -> String {
    let rows: Vec<Vec<String>> = cmp.rows.iter().map(markdown_cells).collect();
    let mut out = String::from("## Load test comparison\n\n");
    out.push_str(&render_markdown_table(&MD_HEADERS, &rows));
    out.push('\n');
    let regressions = cmp.regression_count();
    if regressions > 0 {
        out.push_str(&format!(
            "**{regressions} regression(s) beyond tolerance.**\n"
        ));
    } else {
        out.push_str("No regressions beyond tolerance.\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadr_core::{AggValues, Snapshot, ThresholdStatus};

    fn summary(metrics: Vec<MetricSummary>, checks: Vec<CheckSummary>) -> Summary {
        Summary {
            name: None,
            run_id: "r".to_string(),
            started_ms: 0,
            ended_ms: 0,
            duration_secs: 10.0,
            scenarios: Vec::new(),
            metrics,
            checks,
            thresholds: Vec::new(),
            thresholds_passed: true,
            aborted: None,
            snapshot: Snapshot::default(),
            timeline: Vec::new(),
        }
    }

    fn trend(name: &str, avg: f64, p50: f64, p95: f64, p99: f64) -> MetricSummary {
        MetricSummary {
            metric: name.to_string(),
            kind: MetricKind::Trend,
            agg: AggValues {
                count: 100,
                sum: avg * 100.0,
                avg: Some(avg),
                min: Some(p50 / 2.0),
                max: Some(p99 * 2.0),
                med: Some(p50),
                p90: Some(p95),
                p95: Some(p95),
                p99: Some(p99),
                ..Default::default()
            },
        }
    }

    fn counter(name: &str, sum: f64, per_second: f64) -> MetricSummary {
        MetricSummary {
            metric: name.to_string(),
            kind: MetricKind::Counter,
            agg: AggValues {
                count: sum as u64,
                sum,
                per_second: Some(per_second),
                ..Default::default()
            },
        }
    }

    fn rate(name: &str, fraction: f64) -> MetricSummary {
        MetricSummary {
            metric: name.to_string(),
            kind: MetricKind::Rate,
            agg: AggValues {
                count: 100,
                sum: fraction * 100.0,
                rate: Some(fraction),
                ..Default::default()
            },
        }
    }

    fn row<'a>(cmp: &'a Comparison, metric: &str, field: &str) -> &'a MetricDelta {
        cmp.rows
            .iter()
            .find(|r| r.metric == metric && r.field == field)
            .unwrap_or_else(|| panic!("no row for {metric}.{field}"))
    }

    fn tol(spec: &str) -> Tolerance {
        parse_tolerance(spec).expect("valid tolerance")
    }

    #[test]
    fn tolerance_parses_percent_and_absolute() {
        let t = tol("p95=10%");
        assert_eq!(t.metric, None);
        assert_eq!(t.field, "p95");
        assert_eq!(t.limit, Limit::Percent(10.0));
        let t = tol("error_rate=0.5");
        assert_eq!(t.metric, None);
        assert_eq!(t.field, "error_rate");
        assert_eq!(t.limit, Limit::Absolute(0.5));
    }

    #[test]
    fn tolerance_parses_scope_and_aliases() {
        let t = tol("http_req_duration.p95=25");
        assert_eq!(t.metric.as_deref(), Some("http_req_duration"));
        assert_eq!(t.field, "p95");
        assert_eq!(t.limit, Limit::Absolute(25.0));
        // Aliases normalize; the sign is kept but treated as a magnitude.
        assert_eq!(tol("rps=-5%").field, "per_second");
        assert_eq!(tol("rps=-5%").limit, Limit::Percent(-5.0));
        assert_eq!(tol("med=10%").field, "p50");
        assert_eq!(tol("checks=1%").field, "pass_rate");
    }

    #[test]
    fn tolerance_rejects_bad_specs() {
        assert!(parse_tolerance("p95").unwrap_err().contains("field=limit"));
        assert!(parse_tolerance("=10%").unwrap_err().contains("missing"));
        assert!(parse_tolerance("p95=fast")
            .unwrap_err()
            .contains("bad number"));
        assert!(parse_tolerance("p95=x%")
            .unwrap_err()
            .contains("bad percentage"));
    }

    #[test]
    fn diff_only_covers_metrics_present_in_both() {
        let b = summary(
            vec![
                trend("http_req_duration", 50.0, 40.0, 100.0, 150.0),
                trend("grpc_req_duration", 5.0, 4.0, 10.0, 15.0),
            ],
            Vec::new(),
        );
        let c = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 100.0, 150.0)],
            Vec::new(),
        );
        let cmp = diff(&b, &c, &[]);
        assert!(cmp.rows.iter().all(|r| r.metric == "http_req_duration"));
        assert_eq!(cmp.rows.len(), 4); // avg, p50, p95, p99
    }

    #[test]
    fn latency_regression_gates_by_default() {
        let b = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 100.0, 150.0)],
            Vec::new(),
        );
        let c = summary(
            vec![trend("http_req_duration", 60.0, 41.0, 120.0, 151.0)],
            Vec::new(),
        );
        let cmp = diff(&b, &c, &[]);
        // p95 +20% beyond the 5% default → regression.
        let p95 = row(&cmp, "http_req_duration", "p95");
        assert_eq!(p95.regression, Some(true));
        assert!((p95.delta_abs - 20.0).abs() < 1e-9);
        assert!((p95.delta_pct.unwrap() - 20.0).abs() < 1e-9);
        // p99 +0.7% is within the default → gated but passing.
        assert_eq!(
            row(&cmp, "http_req_duration", "p99").regression,
            Some(false)
        );
        // avg is informational without an explicit tolerance.
        assert_eq!(row(&cmp, "http_req_duration", "avg").regression, None);
    }

    #[test]
    fn improvement_is_never_a_regression() {
        let b = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 100.0, 150.0)],
            Vec::new(),
        );
        let c = summary(
            vec![trend("http_req_duration", 40.0, 30.0, 80.0, 120.0)],
            Vec::new(),
        );
        let cmp = diff(&b, &c, &[]);
        assert_eq!(
            row(&cmp, "http_req_duration", "p95").regression,
            Some(false)
        );
        assert!(row(&cmp, "http_req_duration", "p95").delta_abs < 0.0);
    }

    #[test]
    fn explicit_tolerance_overrides_the_default() {
        let b = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 100.0, 150.0)],
            Vec::new(),
        );
        let c = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 120.0, 150.0)],
            Vec::new(),
        );
        // +20% is fine under a 30% tolerance …
        let cmp = diff(&b, &c, &[tol("p95=30%")]);
        assert_eq!(
            row(&cmp, "http_req_duration", "p95").regression,
            Some(false)
        );
        // … and fine under a 25ms absolute one, but not a 10ms one.
        let cmp = diff(&b, &c, &[tol("p95=25")]);
        assert_eq!(
            row(&cmp, "http_req_duration", "p95").regression,
            Some(false)
        );
        let cmp = diff(&b, &c, &[tol("p95=10")]);
        assert_eq!(row(&cmp, "http_req_duration", "p95").regression, Some(true));
    }

    #[test]
    fn metric_scoped_tolerance_beats_field_wide() {
        let b = summary(
            vec![
                trend("http_req_duration", 50.0, 40.0, 100.0, 150.0),
                trend("grpc_req_duration", 5.0, 4.0, 10.0, 15.0),
            ],
            Vec::new(),
        );
        let c = summary(
            vec![
                trend("http_req_duration", 50.0, 40.0, 120.0, 150.0),
                trend("grpc_req_duration", 5.0, 4.0, 12.0, 15.0),
            ],
            Vec::new(),
        );
        let tols = [tol("p95=5%"), tol("http_req_duration.p95=50%")];
        let cmp = diff(&b, &c, &tols);
        assert_eq!(
            row(&cmp, "http_req_duration", "p95").regression,
            Some(false)
        );
        assert_eq!(row(&cmp, "grpc_req_duration", "p95").regression, Some(true));
    }

    #[test]
    fn throughput_drop_gates_only_with_a_tolerance() {
        let b = summary(vec![counter("http_reqs", 10000.0, 1000.0)], Vec::new());
        let c = summary(vec![counter("http_reqs", 9000.0, 900.0)], Vec::new());
        // Informational without a spec.
        let cmp = diff(&b, &c, &[]);
        assert_eq!(row(&cmp, "http_reqs", "per_second").regression, None);
        assert_eq!(row(&cmp, "http_reqs", "count").regression, None);
        // A -10% drop breaches `rps=-5%` (sign is a magnitude) …
        let cmp = diff(&b, &c, &[tol("rps=-5%")]);
        assert_eq!(row(&cmp, "http_reqs", "per_second").regression, Some(true));
        // … but not `rps=15%`.
        let cmp = diff(&b, &c, &[tol("rps=15%")]);
        assert_eq!(row(&cmp, "http_reqs", "per_second").regression, Some(false));
    }

    #[test]
    fn error_rate_increase_is_a_regression() {
        let b = summary(vec![rate("http_req_failed", 0.01)], Vec::new());
        let c = summary(vec![rate("http_req_failed", 0.02)], Vec::new());
        // 1pp → 2pp is way beyond 5% relative of 1pp.
        let cmp = diff(&b, &c, &[]);
        let r = row(&cmp, "http_req_failed", "error_rate");
        assert_eq!(r.regression, Some(true));
        assert!((r.baseline - 1.0).abs() < 1e-9, "rates render as percent");
        // An absolute 1.5pp allowance covers the increase.
        let cmp = diff(&b, &c, &[tol("error_rate=1.5")]);
        assert_eq!(
            row(&cmp, "http_req_failed", "error_rate").regression,
            Some(false)
        );
    }

    #[test]
    fn checks_pass_rate_row_gates_on_drops() {
        let checks_b = vec![CheckSummary {
            name: "status is 200".to_string(),
            passes: 100,
            fails: 0,
            failure_groups: Vec::new(),
        }];
        let checks_c = vec![CheckSummary {
            name: "status is 200".to_string(),
            passes: 90,
            fails: 10,
            failure_groups: Vec::new(),
        }];
        let b = summary(Vec::new(), checks_b);
        let c = summary(Vec::new(), checks_c);
        let cmp = diff(&b, &c, &[]);
        let r = row(&cmp, "checks", "pass_rate");
        assert_eq!(r.regression, Some(true));
        assert!((r.baseline - 100.0).abs() < 1e-9);
        assert!((r.current - 90.0).abs() < 1e-9);
        // The other direction is an improvement.
        let cmp = diff(&c, &b, &[]);
        assert_eq!(row(&cmp, "checks", "pass_rate").regression, Some(false));
        // No checks in one side → no row.
        let cmp = diff(&b, &summary(Vec::new(), Vec::new()), &[]);
        assert!(cmp.rows.is_empty());
    }

    #[test]
    fn thresholds_row_flags_new_failures() {
        let status = ThresholdStatus {
            metric: "http_req_duration".to_string(),
            expression: "p(95)<400".to_string(),
            observed: Some(500.0),
            passed: false,
            abort_on_fail: false,
        };
        let mut b = summary(Vec::new(), Vec::new());
        b.thresholds = vec![ThresholdStatus {
            passed: true,
            ..status.clone()
        }];
        let mut c = summary(Vec::new(), Vec::new());
        c.thresholds = vec![status];
        c.thresholds_passed = false;
        let cmp = diff(&b, &c, &[]);
        assert_eq!(row(&cmp, "thresholds", "passed").regression, Some(true));
        // Recovering is not a regression.
        let cmp = diff(&c, &b, &[]);
        assert_eq!(row(&cmp, "thresholds", "passed").regression, Some(false));
    }

    #[test]
    fn zero_baseline_has_no_relative_delta() {
        let b = summary(vec![counter("data_sent", 0.0, 0.0)], Vec::new());
        let c = summary(vec![counter("data_sent", 10.0, 1.0)], Vec::new());
        let cmp = diff(&b, &c, &[]);
        let r = row(&cmp, "data_sent", "count");
        assert_eq!(r.delta_pct, None);
        assert_eq!(fmt_delta_pct(r), "-");
        // data_sent is not a throughput counter → neutral.
        assert_eq!(r.regression, None);
    }

    #[test]
    fn markdown_bolds_regressions_and_uses_arrows() {
        let b = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 100.0, 150.0)],
            Vec::new(),
        );
        let c = summary(
            vec![trend("http_req_duration", 45.0, 40.0, 130.0, 150.0)],
            Vec::new(),
        );
        let md = render_markdown(&diff(&b, &c, &[]));
        assert!(md.contains("| Metric | Field | Baseline | Current |"));
        assert!(md.contains("`http_req_duration`"));
        // p95 +30% regressed: bold cells with an up arrow.
        assert!(md.contains("**+30.00ms**"));
        assert!(md.contains("**▲ 30.0%**"));
        assert!(md.contains("**regression**"));
        // avg improved: down arrow, unbolded.
        assert!(md.contains("▼ 10.0%"));
        assert!(md.contains("**1 regression(s) beyond tolerance.**"));
    }

    #[test]
    fn markdown_table_escapes_pipes() {
        let md = render_markdown_table(&["a", "b"], &[vec!["x|y".to_string(), "z".to_string()]]);
        assert!(md.contains("x\\|y"));
        assert!(md.starts_with("| a | b |\n| --- | --- |\n"));
    }

    #[test]
    fn text_table_aligns_columns() {
        let lines = render_text_table(
            &["metric", "value"],
            &[
                vec!["http_req_duration".to_string(), "1".to_string()],
                vec!["x".to_string(), "22".to_string()],
            ],
        );
        assert_eq!(lines.len(), 4);
        // Every value cell starts at the same column.
        let col = lines[0].find("value").expect("header column");
        assert_eq!(lines[2].find('1'), Some(col));
        assert_eq!(lines[3].find("22"), Some(col));
    }

    #[test]
    fn terminal_render_marks_regressions() {
        let b = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 100.0, 150.0)],
            Vec::new(),
        );
        let c = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 130.0, 150.0)],
            Vec::new(),
        );
        let text = render_terminal(&diff(&b, &c, &[]));
        assert!(text.contains("http_req_duration"));
        assert!(text.contains('✗'));
        assert!(text.contains("100.00ms"));
        assert!(text.contains("130.00ms"));
    }

    #[test]
    fn comparison_serializes_to_json() {
        let b = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 100.0, 150.0)],
            Vec::new(),
        );
        let c = summary(
            vec![trend("http_req_duration", 50.0, 40.0, 120.0, 150.0)],
            Vec::new(),
        );
        let json = serde_json::to_value(diff(&b, &c, &[])).expect("serializes");
        let rows = json["rows"].as_array().expect("rows array");
        assert_eq!(rows.len(), 4);
        let p95 = rows.iter().find(|r| r["field"] == "p95").expect("p95 row");
        assert_eq!(p95["metric"], "http_req_duration");
        assert_eq!(p95["regression"], true);
        assert_eq!(p95["delta_abs"], 20.0);
    }

    #[test]
    fn value_formatting_follows_field_units() {
        assert_eq!(fmt_value("p95", 1500.0), "1.50s");
        assert_eq!(fmt_value("p50", 12.345), "12.35ms");
        assert_eq!(fmt_value("avg", 0.5), "500µs");
        assert_eq!(fmt_value("error_rate", 1.5), "1.50%");
        assert_eq!(fmt_value("per_second", 123.45), "123.5/s");
        assert_eq!(fmt_value("count", 1000.0), "1000");
        assert_eq!(fmt_value("passed", 1.0), "pass");
        assert_eq!(fmt_value("passed", 0.0), "fail");
    }
}
