//! Jobs: finite work a plugin runs on its own threads. The host starts a job,
//! polls its progress once per snapshot interval and stops it — it never
//! schedules the work itself, so the plugin is free to tune its own
//! parallelism. A run whose plan has no scenarios lasts until every job is
//! done.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::metrics::{MetricsBus, Tags};

/// A unit of finite work driven by the host.
///
/// Calls come from one blocking thread, never concurrently. `start` must
/// return promptly and do the work elsewhere; `progress` must be cheap.
pub trait Job: Send {
    fn name(&self) -> &str;

    /// Begin the work (typically by spawning the plugin's own threads).
    /// `placement` says which share of a distributed run this instance is:
    /// every agent runs the job, so each must do only its own part.
    fn start(&mut self, placement: JobPlacement) -> Result<(), String>;

    /// Where the work stands. An `Err` fails the job.
    fn progress(&mut self) -> Result<JobProgress, String>;

    /// Cancel the work if still running, wait for it and flush. Called once,
    /// whether the job finished, failed or the run was stopped.
    fn stop(&mut self);
}

/// Where a job instance sits in a run: agent `agent_index` (0-based) of
/// `agent_count`. A standalone run is agent 0 of 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPlacement {
    pub agent_index: u64,
    pub agent_count: u64,
}

impl Default for JobPlacement {
    fn default() -> Self {
        JobPlacement {
            agent_index: 0,
            agent_count: 1,
        }
    }
}

/// What a plugin reports about its own work.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct JobProgress {
    pub state: ReportedState,
    /// Units completed so far.
    #[serde(default)]
    pub done: f64,
    /// Units in total, when known up front. Enables percentage and ETA.
    #[serde(default)]
    pub total: Option<f64>,
    /// What a unit is (`rows`, `files`, ...), for display.
    #[serde(default)]
    pub unit: Option<String>,
    /// Why the job failed, when `state` is `failed`.
    #[serde(default)]
    pub error: Option<String>,
    /// Free-form numeric values, emitted as `job_<key>` gauges.
    #[serde(default)]
    pub metrics: BTreeMap<String, f64>,
}

/// The states a plugin may report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportedState {
    #[default]
    Running,
    Finished,
    Failed,
}

/// Lifecycle of a job as the host sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Pending,
    Running,
    Finished,
    Failed,
    /// Stopped by the user (or a run abort) before it finished.
    Stopped,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Finished | JobState::Failed | JobState::Stopped
        )
    }
}

/// A job's status for live consumers (web UI, console) and the summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobStatus {
    pub name: String,
    pub state: JobState,
    pub done: f64,
    pub total: Option<f64>,
    pub unit: Option<String>,
    /// Units per second over the last poll interval; the whole-run average
    /// once the job is done.
    pub rate: Option<f64>,
    /// Seconds left at the average rate so far; needs `total`.
    pub eta_secs: Option<f64>,
    pub elapsed_secs: f64,
    pub error: Option<String>,
    #[serde(default)]
    pub metrics: BTreeMap<String, f64>,
}

impl JobStatus {
    fn pending(name: &str) -> Self {
        JobStatus {
            name: name.to_string(),
            state: JobState::Pending,
            done: 0.0,
            total: None,
            unit: None,
            rate: None,
            eta_secs: None,
            elapsed_secs: 0.0,
            error: None,
            metrics: BTreeMap::new(),
        }
    }

    /// Completed fraction in `0..=1`, when the total is known.
    pub fn fraction(&self) -> Option<f64> {
        self.total
            .filter(|t| *t > 0.0)
            .map(|t| (self.done / t).clamp(0.0, 1.0))
    }
}

/// Everything the job driver needs from the engine.
pub(crate) struct JobDriver {
    pub bus: MetricsBus,
    pub tags: Arc<Tags>,
    pub stop: CancellationToken,
    pub abort_tx: mpsc::UnboundedSender<String>,
    pub status_tx: watch::Sender<Arc<Vec<JobStatus>>>,
    pub interval: Duration,
    pub placement: JobPlacement,
}

/// How often the driver checks for a stop request between polls.
const STOP_CHECK: Duration = Duration::from_millis(50);

struct Slot {
    job: Box<dyn Job>,
    status: JobStatus,
    started: Instant,
    last_poll: Instant,
    last_done: f64,
    tags: Arc<Tags>,
}

impl JobDriver {
    /// Start every job and poll it until all are terminal or the run stops.
    /// Blocking: run it under `spawn_blocking`.
    pub(crate) fn run(self, jobs: Vec<Box<dyn Job>>) {
        let mut slots: Vec<Slot> = jobs
            .into_iter()
            .map(|job| {
                let mut tags = (*self.tags).clone();
                tags.insert("job".to_string(), job.name().to_string());
                let now = Instant::now();
                Slot {
                    status: JobStatus::pending(job.name()),
                    job,
                    started: now,
                    last_poll: now,
                    last_done: 0.0,
                    tags: Arc::new(tags),
                }
            })
            .collect();
        self.publish(&slots);

        for slot in &mut slots {
            slot.started = Instant::now();
            slot.last_poll = slot.started;
            match slot.job.start(self.placement) {
                Ok(()) => slot.status.state = JobState::Running,
                Err(e) => {
                    // Nothing ran, but the plugin may hold resources.
                    slot.job.stop();
                    self.fail(slot, format!("failed to start: {e}"));
                }
            }
        }
        self.publish(&slots);

        let mut next_poll = Instant::now() + self.interval;
        loop {
            if slots.iter().all(|s| s.status.state.is_terminal()) {
                break;
            }
            if self.stop.is_cancelled() {
                for slot in slots.iter_mut().filter(|s| !s.status.state.is_terminal()) {
                    slot.job.stop();
                    self.poll(slot);
                    if !slot.status.state.is_terminal() {
                        slot.status.state = JobState::Stopped;
                    }
                }
                self.emit_all(&slots);
                self.publish(&slots);
                break;
            }
            let now = Instant::now();
            if now < next_poll {
                std::thread::sleep(STOP_CHECK.min(next_poll - now));
                continue;
            }
            next_poll = now + self.interval;
            for slot in slots.iter_mut().filter(|s| !s.status.state.is_terminal()) {
                self.poll(slot);
                if slot.status.state.is_terminal() {
                    // Join the plugin's threads and flush, then take the
                    // final numbers.
                    let state = slot.status.state;
                    slot.job.stop();
                    self.poll(slot);
                    if slot.status.state != JobState::Failed {
                        slot.status.state = state;
                    }
                }
            }
            self.emit_all(&slots);
            self.publish(&slots);
        }

        // The last poll's interval rate is near zero once the work has
        // stopped; a finished job reports its average instead.
        for slot in &mut slots {
            let status = &mut slot.status;
            status.rate = (status.elapsed_secs > 0.0).then(|| status.done / status.elapsed_secs);
        }
        self.publish(&slots);
    }

    /// Refresh a slot from the plugin, failing it on a bad report.
    fn poll(&self, slot: &mut Slot) {
        let now = Instant::now();
        let progress = match slot.job.progress() {
            Ok(p) => p,
            Err(e) => {
                self.fail(slot, format!("bad progress report: {e}"));
                return;
            }
        };
        let status = &mut slot.status;
        let dt = now.duration_since(slot.last_poll).as_secs_f64();
        if dt > 0.0 {
            status.rate = Some(((progress.done - slot.last_done) / dt).max(0.0));
        }
        slot.last_poll = now;
        slot.last_done = progress.done;
        status.elapsed_secs = now.duration_since(slot.started).as_secs_f64();
        status.done = progress.done;
        status.total = progress.total;
        status.unit = progress.unit;
        status.metrics = progress.metrics;
        status.eta_secs = match status.total {
            Some(total) if status.done > 0.0 && status.elapsed_secs > 0.0 => {
                let average = status.done / status.elapsed_secs;
                Some(((total - status.done) / average).max(0.0))
            }
            _ => None,
        };
        match progress.state {
            ReportedState::Running => {}
            ReportedState::Finished => {
                status.state = JobState::Finished;
                status.eta_secs = Some(0.0);
            }
            ReportedState::Failed => {
                let error = progress
                    .error
                    .unwrap_or_else(|| "failed without an error message".to_string());
                self.fail(slot, error);
            }
        }
    }

    /// Mark a job failed and abort the run: a half-done job is not a result.
    fn fail(&self, slot: &mut Slot, error: String) {
        if slot.status.state == JobState::Failed {
            return;
        }
        tracing::error!(job = %slot.status.name, error = %error, "job failed");
        let _ = self
            .abort_tx
            .send(format!("job `{}` failed: {error}", slot.status.name));
        slot.status.state = JobState::Failed;
        slot.status.error = Some(error);
        slot.status.eta_secs = None;
    }

    fn emit_all(&self, slots: &[Slot]) {
        for slot in slots {
            let status = &slot.status;
            self.bus
                .gauge(&Arc::from("job_done"), status.done, &slot.tags);
            if let Some(fraction) = status.fraction() {
                self.bus
                    .gauge(&Arc::from("job_progress"), fraction, &slot.tags);
            }
            for (key, value) in &status.metrics {
                let name: Arc<str> = Arc::from(format!("job_{}", metric_key(key)));
                self.bus.gauge(&name, *value, &slot.tags);
            }
        }
    }

    fn publish(&self, slots: &[Slot]) {
        let statuses = slots.iter().map(|s| s.status.clone()).collect();
        let _ = self.status_tx.send(Arc::new(statuses));
    }
}

/// A plugin-chosen key as a metric-name fragment: `[a-z0-9_]` only.
fn metric_key(key: &str) -> String {
    key.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// One status per job for a distributed run, from each agent's latest report.
/// Counts, rates and metrics add up across agents; a failure anywhere fails
/// the job, and it is finished only once every agent finished it.
pub fn merge_job_statuses<'a>(
    reports: impl IntoIterator<Item = &'a [JobStatus]>,
) -> Vec<JobStatus> {
    let mut merged: Vec<(JobStatus, Vec<JobState>)> = Vec::new();
    for report in reports {
        for status in report {
            match merged.iter_mut().find(|(job, _)| job.name == status.name) {
                Some((job, states)) => {
                    job.done += status.done;
                    job.total = job.total.zip(status.total).map(|(a, b)| a + b);
                    job.rate = sum_options(job.rate, status.rate);
                    job.eta_secs = max_options(job.eta_secs, status.eta_secs);
                    job.elapsed_secs = job.elapsed_secs.max(status.elapsed_secs);
                    job.error = job.error.take().or_else(|| status.error.clone());
                    for (key, value) in &status.metrics {
                        *job.metrics.entry(key.clone()).or_insert(0.0) += value;
                    }
                    states.push(status.state);
                }
                None => merged.push((status.clone(), vec![status.state])),
            }
        }
    }
    merged
        .into_iter()
        .map(|(mut job, states)| {
            job.state = fleet_state(&states);
            if job.state.is_terminal() {
                job.eta_secs = None;
            }
            job
        })
        .collect()
}

fn fleet_state(states: &[JobState]) -> JobState {
    let any = |state| states.contains(&state);
    if any(JobState::Failed) {
        JobState::Failed
    } else if any(JobState::Running)
        || (any(JobState::Pending) && states.iter().any(|s| s.is_terminal()))
    {
        JobState::Running
    } else if any(JobState::Pending) {
        JobState::Pending
    } else if any(JobState::Stopped) {
        JobState::Stopped
    } else {
        JobState::Finished
    }
}

fn sum_options(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a + b),
        (a, b) => a.or(b),
    }
}

fn max_options(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_parses_minimal_and_full_reports() {
        let p: JobProgress = serde_json::from_str(r#"{"state":"running","done":5}"#).unwrap();
        assert_eq!(p.state, ReportedState::Running);
        assert_eq!(p.done, 5.0);
        assert_eq!(p.total, None);

        let p: JobProgress = serde_json::from_str(
            r#"{"state":"failed","done":1,"total":10,"unit":"rows",
                "error":"disk full","metrics":{"bytes":3}}"#,
        )
        .unwrap();
        assert_eq!(p.state, ReportedState::Failed);
        assert_eq!(p.error.as_deref(), Some("disk full"));
        assert_eq!(p.metrics["bytes"], 3.0);
    }

    #[test]
    fn metric_keys_are_sanitized() {
        assert_eq!(metric_key("Bytes Written/s"), "bytes_written_s");
    }

    #[test]
    fn fraction_needs_a_positive_total() {
        let mut s = JobStatus::pending("gen");
        s.done = 5.0;
        assert_eq!(s.fraction(), None);
        s.total = Some(0.0);
        assert_eq!(s.fraction(), None);
        s.total = Some(20.0);
        assert_eq!(s.fraction(), Some(0.25));
    }

    fn agent_status(state: JobState, done: f64, total: f64, rate: f64) -> JobStatus {
        JobStatus {
            state,
            done,
            total: Some(total),
            unit: Some("rows".into()),
            rate: Some(rate),
            eta_secs: Some((total - done) / rate),
            elapsed_secs: done / rate,
            metrics: BTreeMap::from([("bytes".to_string(), done * 10.0)]),
            ..JobStatus::pending("gen")
        }
    }

    #[test]
    fn agents_add_up_to_one_status_per_job() {
        let a = [agent_status(JobState::Running, 40.0, 100.0, 10.0)];
        let b = [agent_status(JobState::Finished, 100.0, 100.0, 20.0)];
        let merged = merge_job_statuses([&a[..], &b[..]]);
        assert_eq!(merged.len(), 1);
        let job = &merged[0];
        assert_eq!(
            (job.state, job.done, job.total),
            (JobState::Running, 140.0, Some(200.0))
        );
        assert_eq!(job.rate, Some(30.0));
        assert_eq!(job.eta_secs, Some(6.0), "the slowest agent decides");
        assert_eq!(job.elapsed_secs, 5.0);
        assert_eq!(job.metrics["bytes"], 1400.0);
    }

    #[test]
    fn the_fleet_state_waits_for_every_agent_and_any_failure_wins() {
        let merged = |states: &[JobState]| {
            let reports: Vec<Vec<JobStatus>> = states
                .iter()
                .map(|&state| vec![agent_status(state, 1.0, 1.0, 1.0)])
                .collect();
            merge_job_statuses(reports.iter().map(Vec::as_slice))[0].state
        };
        use JobState::*;
        assert_eq!(merged(&[Finished, Finished]), Finished);
        assert_eq!(merged(&[Finished, Running]), Running);
        assert_eq!(merged(&[Pending, Finished]), Running);
        assert_eq!(merged(&[Pending, Pending]), Pending);
        assert_eq!(merged(&[Finished, Stopped]), Stopped);
        assert_eq!(merged(&[Running, Failed]), Failed);
        let finished = [agent_status(Finished, 1.0, 1.0, 1.0)];
        assert_eq!(merge_job_statuses([&finished[..]])[0].eta_secs, None);
    }
}
