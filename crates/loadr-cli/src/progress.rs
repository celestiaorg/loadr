//! Live single-line console progress during a run.

use std::io::Write as _;

use loadr_core::{RunHandle, RunStatus};

/// Render a progress line once per second until the run finishes.
pub async fn show_progress(handle: RunHandle) {
    let mut snapshots = handle.watch_snapshots();
    let mut status = handle.watch_status();
    let started = std::time::Instant::now();
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            r = status.changed() => {
                if r.is_err() || matches!(*status.borrow(), RunStatus::Finished { .. }) {
                    break;
                }
            }
        }
        if matches!(*status.borrow(), RunStatus::Finished { .. }) {
            break;
        }
        let snap = snapshots.borrow_and_update().clone();
        let elapsed = started.elapsed().as_secs();
        let interval = snap.interval_secs.max(0.001);
        // Roll up across every protocol's request counter (http_reqs,
        // grpc_reqs, and plugin families like mongo_reqs) so plugin-only runs
        // don't show 0 RPS.
        let rps = snap.interval_request_count() as f64 / interval;
        let vus = snap
            .series
            .iter()
            .find(|s| s.metric == "vus")
            .and_then(|s| s.agg.last)
            .unwrap_or(0.0);
        // Highest p95 across every protocol's request-duration trend
        // (http_req_duration, grpc_req_duration, plugin <family>_req_duration).
        let p95 = snap
            .series
            .iter()
            .filter(|s| loadr_core::metrics::is_request_duration_metric(&s.metric))
            .filter_map(|s| s.agg.p95)
            .fold(f64::NAN, f64::max);
        let failed: u64 = snap
            .series
            .iter()
            .filter(|s| s.metric == "http_req_failed")
            .map(|s| s.agg.sum as u64)
            .sum();
        let paused = if handle.is_paused() { " [paused]" } else { "" };
        let p95_str = if p95.is_nan() {
            "-".to_string()
        } else {
            format!("{p95:.0}ms")
        };
        let clock = format!(
            "{:02}:{:02}:{:02}",
            elapsed / 3600,
            (elapsed / 60) % 60,
            elapsed % 60
        );
        let jobs = handle.job_statuses();
        let jobs_str = jobs
            .iter()
            .map(job_progress)
            .collect::<Vec<_>>()
            .join("  |  ");
        if !jobs.is_empty() && vus == 0.0 {
            // A jobs-only run has no request stats worth a column.
            eprint!("\r  running {clock}  {jobs_str}{paused}   ");
        } else {
            let jobs_str = if jobs.is_empty() {
                String::new()
            } else {
                format!("  |  {jobs_str}")
            };
            eprint!(
                "\r  running {clock}  vus {vus:>4}  rps {rps:>7.1}  p95 {p95_str:>8}  failed {failed}{jobs_str}{paused}   "
            );
        }
        let _ = std::io::stderr().flush();
    }
    eprintln!();
}

/// `gen 73.4% 7.34M/10.00M rows 410.2k/s eta 7s`, trimmed to what is known.
fn job_progress(job: &loadr_core::JobStatus) -> String {
    let unit = job.unit.as_deref().unwrap_or("");
    let mut out = job.name.clone();
    if let Some(fraction) = job.fraction() {
        out.push_str(&format!(" {:.1}%", fraction * 100.0));
    }
    match job.total {
        Some(total) => out.push_str(&format!(" {}/{}", compact(job.done), compact(total))),
        None => out.push_str(&format!(" {}", compact(job.done))),
    }
    if !unit.is_empty() {
        out.push_str(&format!(" {unit}"));
    }
    match job.state {
        loadr_core::JobState::Running => {
            if let Some(rate) = job.rate {
                out.push_str(&format!(" {}/s", compact(rate)));
            }
            if let Some(eta) = job.eta_secs {
                out.push_str(&format!(" eta {}s", eta.round() as u64));
            }
        }
        state => out.push_str(&format!(" [{}]", format!("{state:?}").to_lowercase())),
    }
    out
}

fn compact(v: f64) -> String {
    let abs = v.abs();
    if abs >= 1e9 {
        format!("{:.2}B", v / 1e9)
    } else if abs >= 1e6 {
        format!("{:.2}M", v / 1e6)
    } else if abs >= 1e4 {
        format!("{:.1}k", v / 1e3)
    } else {
        format!("{v:.0}")
    }
}
