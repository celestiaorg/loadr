//! `loadr sweep` — run one plan across a parameter matrix and tabulate the
//! results side by side.
//!
//! Each `--var name=v1,v2,…` axis multiplies into a cartesian matrix; every
//! combination is executed sequentially by re-invoking the current `loadr`
//! binary (`run --summary-export …`) with the variable exported as
//! `LOADR_SWEEP_<NAME>` and, for `vus`/`duration`, applied as the matching CLI
//! override. The collected summaries render as a combo × latency/error/RPS
//! matrix on the terminal and optionally as GitHub-flavoured markdown.

use std::path::{Path, PathBuf};

use clap::Args;
use loadr_core::summary::MetricSummary;
use loadr_core::{MetricKind, Summary};
use owo_colors::OwoColorize;

use crate::commands::compare::{
    fmt_latency, fmt_pct, fmt_per_second, render_markdown_table, render_text_table,
};

/// Columns of the result matrix (rows are combos).
const MATRIX_HEADERS: [&str; 6] = ["combo", "p50", "p95", "p99", "error rate", "rps"];

#[derive(Args)]
pub struct SweepArgs {
    /// Test definition file
    pub plan: PathBuf,
    /// Sweep axis, `name=v1,v2,...` (e.g. `vus=10,50,100`). Repeatable;
    /// multiple axes form a cartesian product.
    #[arg(long = "var", value_name = "NAME=V1,V2,..", value_parser = parse_var)]
    pub vars: Vec<SweepVar>,
    /// Directory for the per-combo summary exports (default: `loadr-sweep/`)
    #[arg(long, value_name = "DIR")]
    pub out_dir: Option<PathBuf>,
    /// Write the result matrix as a GitHub-flavoured markdown table
    #[arg(long, value_name = "PATH")]
    pub markdown: Option<PathBuf>,
    /// Duration override for combos that don't sweep `duration`, e.g. `30s`
    #[arg(long)]
    pub duration: Option<String>,
    /// Treat this swept axis as an input size and report the fitted complexity
    /// exponent k (response time ≈ size^k). Values on the axis must be numeric.
    /// Pair with a `${payload:…}` body to catch super-linear parsers.
    #[arg(long, value_name = "AXIS")]
    pub complexity: Option<String>,
    /// Fail (exit 99) if the fitted complexity exponent exceeds this bound —
    /// e.g. `1.5` flags worse-than-quasilinear scaling. Implies `--complexity`.
    #[arg(long, value_name = "K")]
    pub max_exponent: Option<f64>,
}

/// One `--var` axis: a variable name and the values to sweep over.
#[derive(Debug, Clone, PartialEq)]
pub struct SweepVar {
    pub name: String,
    pub values: Vec<String>,
}

/// A combo's parsed summary and the exit code of the process that ran it.
pub(crate) type RunOutcome = (Summary, i32);

/// Seam for executing one sweep combination — the real implementation spawns
/// the current `loadr` binary; tests substitute a fake returning canned
/// summaries (repo convention: no subprocesses in unit tests).
pub(crate) trait ComboRunner {
    /// Run the plan with `combo` applied, exporting the summary to `export`.
    fn run(&mut self, combo: &[(String, String)], export: &Path) -> anyhow::Result<RunOutcome>;
}

/// One combination's outcome.
#[derive(Debug)]
pub(crate) struct SweepResult {
    pub label: String,
    pub export: PathBuf,
    pub exit_code: i32,
    /// `None` when the combo failed before producing a summary.
    pub summary: Option<Summary>,
}

pub fn execute(args: SweepArgs) -> anyhow::Result<i32> {
    if args.vars.is_empty() {
        anyhow::bail!("at least one --var is required (e.g. --var vus=10,50,100)");
    }
    let combos = expand_matrix(&args.vars);
    let out_dir = args.out_dir.unwrap_or_else(|| PathBuf::from("loadr-sweep"));
    std::fs::create_dir_all(&out_dir)?;
    let axes: Vec<&str> = args.vars.iter().map(|v| v.name.as_str()).collect();
    eprintln!(
        "{} sweeping {} combination(s) of {}",
        "→".cyan(),
        combos.len(),
        axes.join(" × ")
    );

    let mut runner = SubprocessRunner {
        plan: args.plan,
        duration: args.duration,
    };
    let results = run_sweep(&mut runner, &combos, &out_dir);

    println!();
    for line in render_text_table(&MATRIX_HEADERS, &matrix_rows(&results)) {
        println!("{line}");
    }
    println!();

    if let Some(path) = &args.markdown {
        std::fs::write(path, render_markdown(&results))?;
        eprintln!(
            "{} markdown matrix written to {}",
            "✓".green(),
            path.display()
        );
    }

    let failed: Vec<&SweepResult> = results.iter().filter(|r| r.exit_code != 0).collect();
    if !failed.is_empty() {
        for r in &failed {
            eprintln!(
                "{} {} failed (exit {}) — {}",
                "✗".red(),
                r.label,
                r.exit_code,
                r.export.display()
            );
        }
        eprintln!(
            "{} {} of {} combo(s) failed",
            "✗".red(),
            failed.len(),
            results.len()
        );
        return Ok(loadr_core::EXIT_THRESHOLD_FAILED);
    }

    // Complexity analysis (opt-in via --complexity / --max-exponent).
    let axis = match (&args.complexity, args.max_exponent) {
        (Some(a), _) => Some(a.clone()),
        (None, Some(_)) => {
            anyhow::bail!("--max-exponent requires --complexity <AXIS> naming the size axis");
        }
        (None, None) => None,
    };
    if let Some(axis) = axis {
        if !args.vars.iter().any(|v| v.name == axis) {
            anyhow::bail!("--complexity axis `{axis}` is not one of the swept --var axes");
        }
        println!();
        let fits = analyze_complexity(&combos, &results, &axis);
        let worst = report_complexity(&fits, &axis);
        if let (Some(bound), Some(k)) = (args.max_exponent, worst) {
            if k > bound {
                eprintln!(
                    "{} fitted exponent O(n^{k:.2}) exceeds the --max-exponent {bound:.2} bound",
                    "✗".red(),
                );
                return Ok(loadr_core::EXIT_THRESHOLD_FAILED);
            }
            eprintln!(
                "{} O(n^{k:.2}) within the --max-exponent {bound:.2} bound",
                "✓".green(),
            );
        }
    }
    Ok(0)
}

/// One group of measurements sharing every axis except the size axis, with the
/// complexity exponent fitted across its (size, latency) points.
#[derive(Debug)]
pub(crate) struct ComplexityFit {
    /// The fixed non-size axes for this group (empty when size is the only axis).
    pub group: String,
    /// (size, latency-ms) points, ascending by size.
    pub points: Vec<(f64, f64)>,
    /// Fitted exponent k in latency ≈ c·size^k (log-log least squares).
    pub exponent: Option<f64>,
}

/// Fit the complexity exponent of `latency ≈ c · size^k` via least-squares on
/// log(size) vs log(latency). Needs ≥2 points with ≥2 distinct positive sizes.
pub(crate) fn fit_exponent(points: &[(f64, f64)]) -> Option<f64> {
    let pts: Vec<(f64, f64)> = points
        .iter()
        .filter(|(x, y)| *x > 0.0 && *y > 0.0)
        .map(|(x, y)| (x.ln(), y.ln()))
        .collect();
    if pts.len() < 2 {
        return None;
    }
    let n = pts.len() as f64;
    let sx: f64 = pts.iter().map(|p| p.0).sum();
    let sy: f64 = pts.iter().map(|p| p.1).sum();
    let sxx: f64 = pts.iter().map(|p| p.0 * p.0).sum();
    let sxy: f64 = pts.iter().map(|p| p.0 * p.1).sum();
    let denom = n * sxx - sx * sx;
    if denom.abs() < f64::EPSILON {
        return None; // all sizes equal
    }
    Some((n * sxy - sx * sy) / denom)
}

/// Human verdict for a fitted exponent.
pub(crate) fn classify_exponent(k: f64) -> &'static str {
    match k {
        k if k < 0.5 => "flat / sub-linear",
        k if k < 1.2 => "≈ linear",
        k if k < 1.6 => "super-linear",
        k if k < 2.4 => "≈ quadratic ⚠ DoS risk",
        _ => "super-quadratic ⚠⚠ DoS",
    }
}

/// Build one [`ComplexityFit`] per group of combos that share all axes but
/// `size_axis`. Latency is the p95 of `http_req_duration`.
pub(crate) fn analyze_complexity(
    combos: &[Vec<(String, String)>],
    results: &[SweepResult],
    size_axis: &str,
) -> Vec<ComplexityFit> {
    use std::collections::BTreeMap;
    let mut groups: BTreeMap<String, Vec<(f64, f64)>> = BTreeMap::new();
    for (combo, res) in combos.iter().zip(results) {
        let Some(size) = combo
            .iter()
            .find(|(n, _)| n == size_axis)
            .and_then(|(_, v)| v.parse::<f64>().ok())
        else {
            continue;
        };
        let Some(lat) = res
            .summary
            .as_ref()
            .and_then(|s| find_metric(s, MetricKind::Trend, "http_req_duration", "_req_duration"))
            .and_then(|m| m.agg.p95)
        else {
            continue;
        };
        let key = combo
            .iter()
            .filter(|(n, _)| n != size_axis)
            .map(|(n, v)| format!("{n}={v}"))
            .collect::<Vec<_>>()
            .join(" ");
        groups.entry(key).or_default().push((size, lat));
    }
    groups
        .into_iter()
        .map(|(group, mut points)| {
            points.sort_by(|a, b| a.0.total_cmp(&b.0));
            let exponent = fit_exponent(&points);
            ComplexityFit {
                group,
                points,
                exponent,
            }
        })
        .collect()
}

/// Print the complexity report and return the worst fitted exponent seen.
fn report_complexity(fits: &[ComplexityFit], axis: &str) -> Option<f64> {
    println!("{}", format!("complexity (response time vs {axis})").bold());
    let mut worst: Option<f64> = None;
    for fit in fits {
        let scope = if fit.group.is_empty() {
            String::new()
        } else {
            format!(" [{}]", fit.group)
        };
        let trail = fit
            .points
            .iter()
            .map(|(s, l)| format!("{}→{}", fmt_size(*s), fmt_latency(*l)))
            .collect::<Vec<_>>()
            .join("  ");
        match fit.exponent {
            Some(k) => {
                worst = Some(worst.map_or(k, |w| w.max(k)));
                let verdict = classify_exponent(k);
                let line = format!("  O(n^{k:.2}){scope}  {verdict}");
                if k >= 1.6 {
                    println!("{}", line.yellow());
                } else {
                    println!("{}", line.green());
                }
                println!("    {}", trail.dimmed());
            }
            None => println!("  (not enough distinct points to fit){scope}"),
        }
    }
    worst
}

/// Compact size formatting for the report (1000 → 1.0k, 1e6 → 1.0M).
fn fmt_size(n: f64) -> String {
    if n >= 1e6 {
        format!("{:.1}M", n / 1e6)
    } else if n >= 1e3 {
        format!("{:.1}k", n / 1e3)
    } else {
        format!("{n:.0}")
    }
}

/// Parse one `--var` spec (`vus=10,50,100`).
fn parse_var(spec: &str) -> Result<SweepVar, String> {
    let (name, values) = spec
        .split_once('=')
        .ok_or_else(|| format!("expected `name=v1,v2,..` (e.g. `vus=10,50`), got `{spec}`"))?;
    let name = name.trim();
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!("bad variable name `{name}` in `{spec}`"));
    }
    let values: Vec<String> = values.split(',').map(|v| v.trim().to_string()).collect();
    if values.iter().any(|v| v.is_empty()) {
        return Err(format!("empty value in `{spec}`"));
    }
    Ok(SweepVar {
        name: name.to_string(),
        values,
    })
}

/// Cartesian product of all axes, preserving `--var` order. No axes yields a
/// single empty combination (callers reject that upfront).
pub(crate) fn expand_matrix(vars: &[SweepVar]) -> Vec<Vec<(String, String)>> {
    let mut combos: Vec<Vec<(String, String)>> = vec![Vec::new()];
    for var in vars {
        let mut next = Vec::with_capacity(combos.len() * var.values.len());
        for combo in &combos {
            for value in &var.values {
                let mut widened = combo.clone();
                widened.push((var.name.clone(), value.clone()));
                next.push(widened);
            }
        }
        combos = next;
    }
    combos
}

/// Human label for a combo (`vus=10 duration=30s`).
pub(crate) fn combo_label(combo: &[(String, String)]) -> String {
    let parts: Vec<String> = combo.iter().map(|(n, v)| format!("{n}={v}")).collect();
    parts.join(" ")
}

/// Filename-safe slug for a combo (`vus-10_duration-30s`).
pub(crate) fn combo_slug(combo: &[(String, String)]) -> String {
    let sanitize = |s: &str| s.chars().map(slug_char).collect::<String>();
    let parts: Vec<String> = combo
        .iter()
        .map(|(n, v)| format!("{}-{}", sanitize(n), sanitize(v)))
        .collect();
    parts.join("_")
}

fn slug_char(c: char) -> char {
    if c.is_ascii_alphanumeric() || c == '.' {
        c
    } else {
        '-'
    }
}

/// Environment variable a sweep value is exported under.
fn sweep_env_name(var: &str) -> String {
    format!("LOADR_SWEEP_{}", var.to_ascii_uppercase())
}

/// Run every combination sequentially, streaming a one-line status per combo.
/// A failing combo never aborts the sweep — it lands in the matrix as `-`.
pub(crate) fn run_sweep(
    runner: &mut dyn ComboRunner,
    combos: &[Vec<(String, String)>],
    out_dir: &Path,
) -> Vec<SweepResult> {
    let total = combos.len();
    let mut results = Vec::with_capacity(total);
    for (i, combo) in combos.iter().enumerate() {
        let label = combo_label(combo);
        let export = out_dir.join(format!("sweep-{}.json", combo_slug(combo)));
        eprintln!("{} [{}/{total}] {label}", "→".cyan(), i + 1);
        let (summary, exit_code) = match runner.run(combo, &export) {
            Ok((summary, code)) => {
                if code == 0 {
                    eprintln!("  {} exit {code} — {}", "✓".green(), export.display());
                } else {
                    eprintln!("  {} exit {code} — {}", "✗".red(), export.display());
                }
                (Some(summary), code)
            }
            Err(e) => {
                eprintln!("  {} {label}: {e:#}", "✗".red());
                (None, -1)
            }
        };
        results.push(SweepResult {
            label,
            export,
            exit_code,
            summary,
        });
    }
    results
}

/// Spawns the current `loadr` binary for one combo.
struct SubprocessRunner {
    plan: PathBuf,
    duration: Option<String>,
}

impl ComboRunner for SubprocessRunner {
    fn run(&mut self, combo: &[(String, String)], export: &Path) -> anyhow::Result<RunOutcome> {
        let exe = std::env::current_exe()?;
        let mut cmd = std::process::Command::new(exe);
        cmd.arg("run")
            .arg(&self.plan)
            .arg("--quiet")
            .arg("--summary-export")
            .arg(export)
            .stdout(std::process::Stdio::null());
        let mut sweeps_duration = false;
        for (name, value) in combo {
            cmd.env(sweep_env_name(name), value);
            // vus/duration map onto `loadr run`'s load overrides.
            match name.as_str() {
                "vus" => {
                    cmd.arg("--vus").arg(value);
                }
                "duration" => {
                    cmd.arg("--duration").arg(value);
                    sweeps_duration = true;
                }
                _ => {}
            }
        }
        if let Some(duration) = &self.duration {
            if !sweeps_duration {
                cmd.arg("--duration").arg(duration);
            }
        }
        let status = cmd.status()?;
        let code = status.code().unwrap_or(-1);
        let raw = std::fs::read_to_string(export).map_err(|e| {
            anyhow::anyhow!("combo produced no summary at {}: {e}", export.display())
        })?;
        let summary = serde_json::from_str(&raw).map_err(|e| {
            anyhow::anyhow!("{} is not a loadr summary export: {e}", export.display())
        })?;
        Ok((summary, code))
    }
}

/// One matrix cell, `-` when the value is missing.
fn cell(v: Option<f64>, fmt: impl Fn(f64) -> String) -> String {
    v.map(fmt).unwrap_or_else(|| "-".to_string())
}

/// Build the matrix rows (combo, p50, p95, p99, error rate, rps). Falls back
/// from the `http_` metrics to any protocol family following the
/// `<family>_req*` convention, so plugin-protocol sweeps tabulate too.
pub(crate) fn matrix_rows(results: &[SweepResult]) -> Vec<Vec<String>> {
    results
        .iter()
        .map(|r| {
            let Some(s) = &r.summary else {
                let mut row = vec![r.label.clone()];
                row.resize(MATRIX_HEADERS.len(), "-".to_string());
                return row;
            };
            let trend = find_metric(s, MetricKind::Trend, "http_req_duration", "_req_duration");
            let error = find_metric(s, MetricKind::Rate, "http_req_failed", "_req_failed")
                .and_then(|m| m.agg.rate)
                .map(|rate| rate * 100.0);
            let rps = find_metric(s, MetricKind::Counter, "http_reqs", "_reqs")
                .and_then(|m| m.agg.per_second);
            vec![
                r.label.clone(),
                cell(trend.and_then(|t| t.agg.med), fmt_latency),
                cell(trend.and_then(|t| t.agg.p95), fmt_latency),
                cell(trend.and_then(|t| t.agg.p99), fmt_latency),
                cell(error, fmt_pct),
                cell(rps, fmt_per_second),
            ]
        })
        .collect()
}

/// Exact metric name first, then the `<family>` suffix convention.
fn find_metric<'a>(
    summary: &'a Summary,
    kind: MetricKind,
    exact: &str,
    suffix: &str,
) -> Option<&'a MetricSummary> {
    let metrics = &summary.metrics;
    if let Some(m) = metrics.iter().find(|m| m.metric == exact) {
        return Some(m);
    }
    metrics
        .iter()
        .find(|m| m.kind == kind && m.metric.ends_with(suffix))
}

/// GitHub-flavoured markdown matrix suitable for a PR comment.
pub(crate) fn render_markdown(results: &[SweepResult]) -> String {
    let mut out = String::from("## loadr sweep\n\n");
    out.push_str(&render_markdown_table(
        &MATRIX_HEADERS,
        &matrix_rows(results),
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadr_core::{AggValues, Snapshot};

    fn var(name: &str, values: &[&str]) -> SweepVar {
        SweepVar {
            name: name.to_string(),
            values: values.iter().map(|v| v.to_string()).collect(),
        }
    }

    fn combo(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    fn summary_with(p50: f64, p95: f64, p99: f64, error_rate: f64, rps: f64) -> Summary {
        let trend = MetricSummary {
            metric: "http_req_duration".to_string(),
            kind: MetricKind::Trend,
            agg: AggValues {
                count: 100,
                sum: p50 * 100.0,
                avg: Some(p50),
                med: Some(p50),
                p95: Some(p95),
                p99: Some(p99),
                ..Default::default()
            },
        };
        let failed = MetricSummary {
            metric: "http_req_failed".to_string(),
            kind: MetricKind::Rate,
            agg: AggValues {
                count: 100,
                sum: error_rate * 100.0,
                rate: Some(error_rate),
                ..Default::default()
            },
        };
        let reqs = MetricSummary {
            metric: "http_reqs".to_string(),
            kind: MetricKind::Counter,
            agg: AggValues {
                count: 1000,
                sum: 1000.0,
                per_second: Some(rps),
                ..Default::default()
            },
        };
        Summary {
            name: None,
            run_id: "r".to_string(),
            started_ms: 0,
            ended_ms: 0,
            duration_secs: 10.0,
            scenarios: Vec::new(),
            metrics: vec![trend, failed, reqs],
            checks: Vec::new(),
            thresholds: Vec::new(),
            thresholds_passed: true,
            aborted: None,
            snapshot: Snapshot::default(),
            timeline: Vec::new(),
            jobs: Vec::new(),
        }
    }

    /// Fake runner: pops canned outcomes and records the combos it saw.
    struct FakeRunner {
        outcomes: Vec<anyhow::Result<RunOutcome>>,
        seen: Vec<Vec<(String, String)>>,
    }

    impl ComboRunner for FakeRunner {
        fn run(
            &mut self,
            combo: &[(String, String)],
            _export: &Path,
        ) -> anyhow::Result<RunOutcome> {
            self.seen.push(combo.to_vec());
            self.outcomes.remove(0)
        }
    }

    #[test]
    fn var_parses_name_and_values() {
        let v = parse_var("vus=10,50,100").expect("valid var");
        assert_eq!(v, var("vus", &["10", "50", "100"]));
        // Whitespace around values is trimmed.
        assert_eq!(
            parse_var("rate= 50 ,100").expect("valid"),
            var("rate", &["50", "100"])
        );
    }

    #[test]
    fn var_rejects_bad_specs() {
        assert!(parse_var("vus").unwrap_err().contains("name=v1,v2"));
        assert!(parse_var("=10").unwrap_err().contains("bad variable name"));
        assert!(parse_var("v us=10")
            .unwrap_err()
            .contains("bad variable name"));
        assert!(parse_var("vus=10,,50").unwrap_err().contains("empty value"));
    }

    #[test]
    fn matrix_expands_the_cartesian_product() {
        let combos = expand_matrix(&[var("vus", &["10", "50"]), var("rate", &["1", "2", "3"])]);
        assert_eq!(combos.len(), 6);
        assert_eq!(combos[0], combo(&[("vus", "10"), ("rate", "1")]));
        assert_eq!(combos[1], combo(&[("vus", "10"), ("rate", "2")]));
        assert_eq!(combos[5], combo(&[("vus", "50"), ("rate", "3")]));
    }

    #[test]
    fn matrix_of_one_axis_is_its_values() {
        let combos = expand_matrix(&[var("vus", &["10", "50", "100"])]);
        assert_eq!(combos.len(), 3);
        assert_eq!(combos[2], combo(&[("vus", "100")]));
        // Degenerate case, rejected by `execute` before it gets here.
        assert_eq!(expand_matrix(&[]), vec![Vec::new()]);
    }

    #[test]
    fn labels_slugs_and_env_names() {
        let c = combo(&[("vus", "10"), ("duration", "1m30s")]);
        assert_eq!(combo_label(&c), "vus=10 duration=1m30s");
        assert_eq!(combo_slug(&c), "vus-10_duration-1m30s");
        // Slugs stay filename-safe.
        assert_eq!(combo_slug(&combo(&[("rate", "50/s")])), "rate-50-s");
        assert_eq!(sweep_env_name("vus"), "LOADR_SWEEP_VUS");
    }

    #[test]
    fn run_sweep_drives_the_runner_in_order() {
        let combos = expand_matrix(&[var("vus", &["10", "50"])]);
        let mut runner = FakeRunner {
            outcomes: vec![
                Ok((summary_with(10.0, 20.0, 30.0, 0.0, 100.0), 0)),
                Ok((summary_with(15.0, 40.0, 80.0, 0.01, 90.0), 99)),
            ],
            seen: Vec::new(),
        };
        let results = run_sweep(&mut runner, &combos, Path::new("out"));
        assert_eq!(runner.seen, combos);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].label, "vus=10");
        assert_eq!(results[0].exit_code, 0);
        assert_eq!(
            results[0].export,
            Path::new("out").join("sweep-vus-10.json")
        );
        assert_eq!(results[1].exit_code, 99);
        assert!(results[1].summary.is_some());
    }

    #[test]
    fn run_sweep_keeps_going_past_failures() {
        let combos = expand_matrix(&[var("vus", &["10", "50"])]);
        let mut runner = FakeRunner {
            outcomes: vec![
                Err(anyhow::anyhow!("boom")),
                Ok((summary_with(10.0, 20.0, 30.0, 0.0, 100.0), 0)),
            ],
            seen: Vec::new(),
        };
        let results = run_sweep(&mut runner, &combos, Path::new("out"));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].exit_code, -1);
        assert!(results[0].summary.is_none());
        assert_eq!(results[1].exit_code, 0);
        assert!(results[1].summary.is_some());
    }

    #[test]
    fn fit_exponent_recovers_known_orders() {
        let quad: Vec<(f64, f64)> = [1.0, 2.0, 4.0, 8.0].iter().map(|&x| (x, x * x)).collect();
        assert!((fit_exponent(&quad).unwrap() - 2.0).abs() < 1e-9);
        let lin: Vec<(f64, f64)> = [1.0, 10.0, 100.0].iter().map(|&x| (x, 3.0 * x)).collect();
        assert!((fit_exponent(&lin).unwrap() - 1.0).abs() < 1e-9);
        assert!(fit_exponent(&[(5.0, 1.0), (5.0, 2.0)]).is_none());
        assert!(fit_exponent(&[(4.0, 1.0)]).is_none());
    }

    #[test]
    fn classify_exponent_labels_the_bands() {
        assert_eq!(classify_exponent(1.0), "≈ linear");
        assert_eq!(classify_exponent(1.5), "super-linear");
        assert!(classify_exponent(2.1).contains("quadratic"));
        assert!(classify_exponent(3.0).contains("super-quadratic"));
    }

    #[test]
    fn analyze_complexity_groups_and_fits() {
        let combos = vec![
            vec![("depth".to_string(), "1000".to_string())],
            vec![("depth".to_string(), "2000".to_string())],
        ];
        let results = vec![
            SweepResult {
                label: "depth=1000".into(),
                export: PathBuf::from("a"),
                exit_code: 0,
                summary: Some(summary_with(0.0, 100.0, 0.0, 0.0, 0.0)),
            },
            SweepResult {
                label: "depth=2000".into(),
                export: PathBuf::from("b"),
                exit_code: 0,
                summary: Some(summary_with(0.0, 400.0, 0.0, 0.0, 0.0)),
            },
        ];
        let fits = analyze_complexity(&combos, &results, "depth");
        assert_eq!(fits.len(), 1);
        assert!((fits[0].exponent.unwrap() - 2.0).abs() < 1e-6);
    }

    #[test]
    fn matrix_rows_format_cells() {
        let results = vec![
            SweepResult {
                label: "vus=10".to_string(),
                export: PathBuf::from("a.json"),
                exit_code: 0,
                summary: Some(summary_with(20.0, 45.5, 90.0, 0.01, 250.0)),
            },
            SweepResult {
                label: "vus=50".to_string(),
                export: PathBuf::from("b.json"),
                exit_code: -1,
                summary: None,
            },
        ];
        let rows = matrix_rows(&results);
        assert_eq!(
            rows[0],
            ["vus=10", "20.00ms", "45.50ms", "90.00ms", "1.00%", "250.0/s"]
        );
        assert_eq!(rows[1], ["vus=50", "-", "-", "-", "-", "-"]);
    }

    #[test]
    fn matrix_rows_fall_back_to_plugin_metric_families() {
        let mut summary = summary_with(20.0, 45.0, 90.0, 0.0, 100.0);
        for m in &mut summary.metrics {
            m.metric = m.metric.replace("http_", "mongo_");
        }
        let results = vec![SweepResult {
            label: "vus=10".to_string(),
            export: PathBuf::from("a.json"),
            exit_code: 0,
            summary: Some(summary),
        }];
        let rows = matrix_rows(&results);
        assert_eq!(rows[0][2], "45.00ms");
        assert_eq!(rows[0][5], "100.0/s");
    }

    #[test]
    fn markdown_matrix_renders_a_table() {
        let results = vec![SweepResult {
            label: "vus=10".to_string(),
            export: PathBuf::from("a.json"),
            exit_code: 0,
            summary: Some(summary_with(20.0, 45.0, 90.0, 0.01, 250.0)),
        }];
        let md = render_markdown(&results);
        assert!(md.starts_with("## loadr sweep\n\n"));
        assert!(md.contains("| combo | p50 | p95 | p99 | error rate | rps |"));
        assert!(md.contains("| vus=10 | 20.00ms | 45.00ms | 90.00ms | 1.00% | 250.0/s |"));
    }
}
