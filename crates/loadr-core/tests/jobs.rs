//! Jobs driven through the real engine: a plan with no scenarios runs until
//! its jobs finish, a failed job aborts the run, and a stop request stops a
//! job that would otherwise run forever.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use loadr_core::{Engine, EngineOptions, Job, JobProgress, JobState, ReportedState};

/// Counts to `total` on its own thread, one unit per millisecond.
struct Counter {
    total: u64,
    fail_at: Option<u64>,
    done: Arc<AtomicU64>,
    cancel: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
    stops: Arc<AtomicU64>,
}

impl Counter {
    fn new(total: u64) -> Self {
        Counter {
            total,
            fail_at: None,
            done: Arc::new(AtomicU64::new(0)),
            cancel: Arc::new(AtomicBool::new(false)),
            worker: None,
            stops: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Job for Counter {
    fn name(&self) -> &str {
        "counter"
    }

    fn start(&mut self) -> Result<(), String> {
        let (done, cancel, total) = (self.done.clone(), self.cancel.clone(), self.total);
        self.worker = Some(std::thread::spawn(move || {
            while !cancel.load(Ordering::SeqCst) && done.load(Ordering::SeqCst) < total {
                std::thread::sleep(Duration::from_millis(1));
                done.fetch_add(1, Ordering::SeqCst);
            }
        }));
        Ok(())
    }

    fn progress(&mut self) -> Result<JobProgress, String> {
        let done = self.done.load(Ordering::SeqCst);
        let state = match self.fail_at {
            Some(at) if done >= at => ReportedState::Failed,
            _ if done >= self.total => ReportedState::Finished,
            _ => ReportedState::Running,
        };
        Ok(JobProgress {
            state,
            done: done as f64,
            total: Some(self.total as f64),
            unit: Some("units".to_string()),
            error: (state == ReportedState::Failed).then(|| "boom".to_string()),
            metrics: [("Last Value".to_string(), done as f64)].into(),
        })
    }

    fn stop(&mut self) {
        self.stops.fetch_add(1, Ordering::SeqCst);
        self.cancel.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

const PLAN: &str = r#"
plugins:
  - name: counter
"#;

fn engine(job: Counter) -> Engine {
    let loaded = loadr_config::load_str(PLAN, &loadr_config::LoadOptions::new()).expect("parse");
    Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            jobs: vec![Box::new(job)],
            snapshot_interval: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .expect("engine")
}

#[tokio::test(flavor = "multi_thread")]
async fn plan_without_scenarios_runs_until_the_job_finishes() {
    let job = Counter::new(100);
    let stops = job.stops.clone();
    let result = engine(job).run().await.expect("run");

    assert!(result.aborted.is_none(), "{:?}", result.aborted);
    assert_eq!(stops.load(Ordering::SeqCst), 1, "stop runs exactly once");
    let status = &result.summary.jobs[0];
    assert_eq!(status.state, JobState::Finished);
    assert_eq!(status.done, 100.0);
    assert_eq!(status.fraction(), Some(1.0));
    assert_eq!(status.unit.as_deref(), Some("units"));

    let metric = |name: &str| result.summary.metrics.iter().any(|m| m.metric == name);
    assert!(metric("job_done"));
    assert!(metric("job_progress"));
    assert!(metric("job_last_value"), "custom metric keys are sanitized");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_job_aborts_the_run() {
    let mut job = Counter::new(1_000_000);
    job.fail_at = Some(20);
    let result = engine(job).run().await.expect("run");

    let status = &result.summary.jobs[0];
    assert_eq!(status.state, JobState::Failed);
    assert_eq!(status.error.as_deref(), Some("boom"));
    let aborted = result.aborted.expect("aborted");
    assert!(aborted.contains("job `counter` failed: boom"), "{aborted}");
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_the_run_stops_the_job() {
    let job = Counter::new(u64::MAX);
    let stops = job.stops.clone();
    let engine = engine(job);
    let handle = engine.handle();
    let run = tokio::spawn(engine.run());

    tokio::time::sleep(Duration::from_millis(200)).await;
    let live = handle.job_statuses();
    assert_eq!(live[0].state, JobState::Running);
    handle.stop("user");

    let result = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("run ends promptly after stop")
        .expect("join")
        .expect("run");
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    let status = &result.summary.jobs[0];
    assert_eq!(status.state, JobState::Stopped);
    assert!(status.done > 0.0);
    assert_eq!(result.aborted.as_deref(), Some("user"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_that_fails_to_start_aborts_the_run() {
    struct Broken;
    impl Job for Broken {
        fn name(&self) -> &str {
            "broken"
        }
        fn start(&mut self) -> Result<(), String> {
            Err("no disk".to_string())
        }
        fn progress(&mut self) -> Result<JobProgress, String> {
            unreachable!("never polled after a failed start")
        }
        fn stop(&mut self) {}
    }
    let loaded = loadr_config::load_str(PLAN, &loadr_config::LoadOptions::new()).expect("parse");
    let engine = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions {
            jobs: vec![Box::new(Broken)],
            ..Default::default()
        },
    )
    .expect("engine");
    let result = engine.run().await.expect("run");
    assert_eq!(result.summary.jobs[0].state, JobState::Failed);
    assert!(result
        .aborted
        .expect("aborted")
        .contains("failed to start: no disk"));
}

#[test]
fn a_plan_with_neither_scenarios_nor_jobs_is_rejected() {
    let loaded = loadr_config::load_str(PLAN, &loadr_config::LoadOptions::new()).expect("parse");
    let err = Engine::new(
        loaded.plan,
        std::path::PathBuf::from("."),
        EngineOptions::default(),
    )
    .err()
    .expect("rejected");
    assert!(err.to_string().contains("no scenarios or jobs"), "{err}");
}
