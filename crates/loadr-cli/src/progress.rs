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
        eprint!(
            "\r  running {:02}:{:02}:{:02}  vus {vus:>4}  rps {rps:>7.1}  p95 {p95_str:>8}  failed {failed}{paused}   ",
            elapsed / 3600,
            (elapsed / 60) % 60,
            elapsed % 60
        );
        let _ = std::io::stderr().flush();
    }
    eprintln!();
}
