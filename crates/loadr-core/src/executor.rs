//! The seven executors. Closed models drive iterations from VU loops; open
//! models drive a precise arrival clock that starts iterations on schedule.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use loadr_config::ExecutorSpec;
use tokio::sync::{mpsc, watch, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;

use crate::flow::{FlowRunner, IterationOutcome};
use crate::metrics::{BuiltinMetrics, MetricsBus};
use crate::script::ScriptEngine;
use crate::vu::{RunContext, VuContext};

/// Scenario-level run parameters.
#[derive(Clone)]
pub struct ScenarioRunSpec {
    pub name: Arc<str>,
    pub spec: ExecutorSpec,
    pub start_time: Duration,
    pub graceful_stop: Duration,
    pub graceful_ramp_down: Duration,
}

/// Shared executor environment for one scenario.
#[derive(Clone)]
pub struct ExecEnv {
    pub runner: Arc<FlowRunner>,
    pub run_ctx: Arc<RunContext>,
    pub metrics: MetricsBus,
    pub builtins: Arc<BuiltinMetrics>,
    pub script: Option<Arc<dyn ScriptEngine>>,
    /// Run-level: stop starting new iterations.
    pub soft_stop: CancellationToken,
    /// Run-level: cancel everything now.
    pub hard_stop: CancellationToken,
    pub pause: watch::Receiver<bool>,
    /// Global VU id allocator.
    pub vu_ids: Arc<AtomicU64>,
    /// Active VU count for this scenario (drives the `vus` gauge).
    pub active_vus: Arc<AtomicU64>,
    /// Report an `abort_test` condition.
    pub abort_tx: mpsc::UnboundedSender<String>,
    /// Target VUs for `externally-controlled` scenarios.
    pub external_target: Option<watch::Receiver<u64>>,
}

struct VuWorker {
    ctx: VuContext,
    script: Option<Box<dyn crate::script::VuScript>>,
    /// Runner kept so the per-VU `on_stop` hook can fire when the worker drops.
    runner: Arc<FlowRunner>,
}

impl Drop for VuWorker {
    fn drop(&mut self) {
        // Fire the Locust-style `on_stop` hook once, when the VU retires.
        if self.runner.program.on_stop.is_none() || self.ctx.iteration == 0 {
            return;
        }
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runner.run_on_stop(&mut self.ctx, &mut self.script);
        }
    }
}

impl ExecEnv {
    fn new_worker(&self) -> VuWorker {
        let vu_id = self.vu_ids.fetch_add(1, Ordering::Relaxed) + 1;
        let ctx = VuContext::new(
            vu_id,
            self.runner.program.name.clone(),
            self.runner.program.tags.clone(),
            self.metrics.clone(),
            self.run_ctx.clone(),
            self.runner.program.cookies_auto,
        );
        let script = match &self.script {
            Some(engine) => match tokio::task::block_in_place(|| engine.instantiate()) {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::error!(error = %e, "failed to instantiate VU script runtime");
                    None
                }
            },
            None => None,
        };
        VuWorker {
            ctx,
            script,
            runner: self.runner.clone(),
        }
    }

    /// Wait while paused; returns false when stopped.
    async fn wait_unpaused(&self, cancel: &CancellationToken) -> bool {
        let mut pause = self.pause.clone();
        loop {
            if cancel.is_cancelled() {
                return false;
            }
            if !*pause.borrow() {
                return true;
            }
            tokio::select! {
                _ = cancel.cancelled() => return false,
                r = pause.changed() => {
                    if r.is_err() {
                        return true;
                    }
                }
            }
        }
    }

    /// Run one iteration, handling outcome plumbing. Returns false when the VU
    /// should stop iterating.
    async fn run_one(&self, worker: &mut VuWorker, scenario_cancel: &CancellationToken) -> bool {
        let outcome = tokio::select! {
            biased;
            _ = scenario_cancel.cancelled() => return false,
            o = self.runner.run_iteration(&mut worker.ctx, &mut worker.script) => o,
        };
        match outcome {
            IterationOutcome::Completed => true,
            IterationOutcome::StopVu => false,
            IterationOutcome::AbortScenario => {
                scenario_cancel.cancel();
                false
            }
            IterationOutcome::AbortTest(reason) => {
                let _ = self.abort_tx.send(reason);
                false
            }
        }
    }
}

/// Drive one scenario to completion (including graceful stop).
pub async fn run_scenario(spec: ScenarioRunSpec, env: ExecEnv) {
    if !spec.start_time.is_zero() {
        tokio::select! {
            _ = tokio::time::sleep(spec.start_time) => {}
            _ = env.hard_stop.cancelled() => return,
        }
    }
    tracing::info!(scenario = %spec.name, executor = ?executor_name(&spec.spec), "scenario starting");

    // Scenario-local cancellation: triggered by run-level stops, scenario
    // aborts, and the graceful-stop deadline.
    let scenario_cancel = CancellationToken::new();
    {
        let sc = scenario_cancel.clone();
        let hard = env.hard_stop.clone();
        tokio::spawn(async move {
            hard.cancelled().await;
            sc.cancel();
        });
    }

    match spec.spec.clone() {
        ExecutorSpec::ConstantVus { vus, duration } => {
            run_constant_vus(&spec, &env, &scenario_cancel, vus, duration).await;
        }
        ExecutorSpec::RampingVus { start_vus, stages } => {
            run_ramping_vus(&spec, &env, &scenario_cancel, start_vus, stages).await;
        }
        ExecutorSpec::PerVuIterations {
            vus,
            iterations,
            max_duration,
        } => {
            run_iterations(
                &spec,
                &env,
                &scenario_cancel,
                vus,
                IterationBudget::PerVu(iterations),
                max_duration,
            )
            .await;
        }
        ExecutorSpec::SharedIterations {
            vus,
            iterations,
            max_duration,
        } => {
            run_iterations(
                &spec,
                &env,
                &scenario_cancel,
                vus,
                IterationBudget::Shared(Arc::new(AtomicU64::new(iterations))),
                max_duration,
            )
            .await;
        }
        ExecutorSpec::ConstantArrivalRate {
            rate,
            duration,
            pre_allocated_vus,
            max_vus,
        } => {
            run_arrival_rate(
                &spec,
                &env,
                &scenario_cancel,
                RateSchedule::Constant { rate, duration },
                pre_allocated_vus,
                max_vus,
            )
            .await;
        }
        ExecutorSpec::RampingArrivalRate {
            start_rate,
            stages,
            pre_allocated_vus,
            max_vus,
        } => {
            run_arrival_rate(
                &spec,
                &env,
                &scenario_cancel,
                RateSchedule::Ramping { start_rate, stages },
                pre_allocated_vus,
                max_vus,
            )
            .await;
        }
        ExecutorSpec::ExternallyControlled { max_vus, duration } => {
            run_externally_controlled(&spec, &env, &scenario_cancel, max_vus, duration).await;
        }
    }
    tracing::info!(scenario = %spec.name, "scenario finished");
}

fn executor_name(spec: &ExecutorSpec) -> &'static str {
    match spec {
        ExecutorSpec::ConstantVus { .. } => "constant-vus",
        ExecutorSpec::RampingVus { .. } => "ramping-vus",
        ExecutorSpec::ConstantArrivalRate { .. } => "constant-arrival-rate",
        ExecutorSpec::RampingArrivalRate { .. } => "ramping-arrival-rate",
        ExecutorSpec::PerVuIterations { .. } => "per-vu-iterations",
        ExecutorSpec::SharedIterations { .. } => "shared-iterations",
        ExecutorSpec::ExternallyControlled { .. } => "externally-controlled",
    }
}

/// Arm the graceful-stop timer once the soft deadline passes: in-flight
/// iterations get `graceful` to finish, then the scenario is cancelled.
fn arm_graceful_stop(
    scenario_cancel: &CancellationToken,
    soft_stop: &CancellationToken,
    soft_deadline: Option<Instant>,
    graceful: Duration,
) {
    let sc = scenario_cancel.clone();
    let soft = soft_stop.clone();
    tokio::spawn(async move {
        match soft_deadline {
            Some(deadline) => {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline.into()) => {}
                    _ = soft.cancelled() => {}
                    _ = sc.cancelled() => return,
                }
            }
            None => {
                tokio::select! {
                    _ = soft.cancelled() => {}
                    _ = sc.cancelled() => return,
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(graceful) => sc.cancel(),
            _ = sc.cancelled() => {}
        }
    });
}

async fn run_constant_vus(
    spec: &ScenarioRunSpec,
    env: &ExecEnv,
    scenario_cancel: &CancellationToken,
    vus: u64,
    duration: Duration,
) {
    let soft_deadline = Instant::now() + duration;
    arm_graceful_stop(
        scenario_cancel,
        &env.soft_stop,
        Some(soft_deadline),
        spec.graceful_stop,
    );
    let mut handles = Vec::with_capacity(vus as usize);
    for _ in 0..vus {
        let env = env.clone();
        let cancel = scenario_cancel.clone();
        handles.push(tokio::spawn(async move {
            let mut worker = env.new_worker();
            env.active_vus.fetch_add(1, Ordering::Relaxed);
            while Instant::now() < soft_deadline
                && !cancel.is_cancelled()
                && !env.soft_stop.is_cancelled()
            {
                if !env.wait_unpaused(&cancel).await {
                    break;
                }
                if !env.run_one(&mut worker, &cancel).await {
                    break;
                }
            }
            env.active_vus.fetch_sub(1, Ordering::Relaxed);
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    scenario_cancel.cancel();
}

async fn run_ramping_vus(
    spec: &ScenarioRunSpec,
    env: &ExecEnv,
    scenario_cancel: &CancellationToken,
    start_vus: u64,
    stages: Vec<(Duration, u64)>,
) {
    let total: Duration = stages.iter().map(|(d, _)| *d).sum();
    let soft_deadline = Instant::now() + total;
    arm_graceful_stop(
        scenario_cancel,
        &env.soft_stop,
        Some(soft_deadline),
        spec.graceful_stop,
    );
    let peak = stages
        .iter()
        .map(|(_, t)| *t)
        .chain(std::iter::once(start_vus))
        .max()
        .unwrap_or(0);
    let (allowed_tx, allowed_rx) = watch::channel(start_vus);

    // Ramp controller: piecewise-linear interpolation, 100 ms resolution.
    {
        let cancel = scenario_cancel.clone();
        let stages = stages.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let mut ticker = tokio::time::interval(Duration::from_millis(100));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                let t = started.elapsed();
                let target = vus_at(start_vus, &stages, t);
                let _ = allowed_tx.send(target);
                if t >= total {
                    return;
                }
            }
        });
    }

    run_allocated_pool(
        env,
        scenario_cancel,
        peak,
        allowed_rx,
        Some(soft_deadline),
        spec.graceful_ramp_down,
    )
    .await;
    scenario_cancel.cancel();
}

/// Linear interpolation of the VU target across stages at time `t`.
fn vus_at(start_vus: u64, stages: &[(Duration, u64)], t: Duration) -> u64 {
    let mut from = start_vus as f64;
    let mut offset = Duration::ZERO;
    for (len, target) in stages {
        let to = *target as f64;
        if t < offset + *len {
            let progress = (t - offset).as_secs_f64() / len.as_secs_f64().max(1e-9);
            return (from + (to - from) * progress).round() as u64;
        }
        from = to;
        offset += *len;
    }
    stages.last().map(|(_, t)| *t).unwrap_or(start_vus)
}

/// A pool of `peak` parked VU tasks where VU *i* runs while `allowed > i`.
/// Used by ramping-vus and externally-controlled.
async fn run_allocated_pool(
    env: &ExecEnv,
    scenario_cancel: &CancellationToken,
    peak: u64,
    allowed_rx: watch::Receiver<u64>,
    soft_deadline: Option<Instant>,
    ramp_down_grace: Duration,
) {
    let mut handles = Vec::with_capacity(peak as usize);
    for i in 0..peak {
        let env = env.clone();
        let cancel = scenario_cancel.clone();
        let mut allowed = allowed_rx.clone();
        handles.push(tokio::spawn(async move {
            let mut worker: Option<VuWorker> = None;
            let mut active = false;
            loop {
                if cancel.is_cancelled() || env.soft_stop.is_cancelled() {
                    break;
                }
                if let Some(deadline) = soft_deadline {
                    if Instant::now() >= deadline {
                        break;
                    }
                }
                if *allowed.borrow() <= i {
                    if active {
                        env.active_vus.fetch_sub(1, Ordering::Relaxed);
                        active = false;
                    }
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        r = allowed.changed() => { if r.is_err() { break; } }
                    }
                    continue;
                }
                if !env.wait_unpaused(&cancel).await {
                    break;
                }
                if !active {
                    env.active_vus.fetch_add(1, Ordering::Relaxed);
                    active = true;
                }
                let w = worker.get_or_insert_with(|| env.new_worker());
                // Race the iteration against deallocation + ramp-down grace.
                let dealloc = {
                    let mut rx = allowed.clone();
                    async move {
                        loop {
                            if *rx.borrow() <= i {
                                tokio::time::sleep(ramp_down_grace).await;
                                if *rx.borrow() <= i {
                                    return;
                                }
                            }
                            if rx.changed().await.is_err() {
                                std::future::pending::<()>().await;
                            }
                        }
                    }
                };
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    keep_going = env.run_one(w, &cancel) => {
                        if !keep_going { break; }
                    }
                    _ = dealloc => { /* iteration interrupted by ramp-down */ }
                }
            }
            if active {
                env.active_vus.fetch_sub(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

enum IterationBudget {
    PerVu(u64),
    Shared(Arc<AtomicU64>),
}

async fn run_iterations(
    spec: &ScenarioRunSpec,
    env: &ExecEnv,
    scenario_cancel: &CancellationToken,
    vus: u64,
    budget: IterationBudget,
    max_duration: Duration,
) {
    let soft_deadline = Instant::now() + max_duration;
    arm_graceful_stop(
        scenario_cancel,
        &env.soft_stop,
        Some(soft_deadline),
        spec.graceful_stop,
    );
    let shared = match &budget {
        IterationBudget::Shared(c) => Some(c.clone()),
        IterationBudget::PerVu(_) => None,
    };
    let per_vu = match &budget {
        IterationBudget::PerVu(n) => *n,
        IterationBudget::Shared(_) => 0,
    };
    let mut handles = Vec::with_capacity(vus as usize);
    for _ in 0..vus {
        let env = env.clone();
        let cancel = scenario_cancel.clone();
        let shared = shared.clone();
        handles.push(tokio::spawn(async move {
            let mut worker = env.new_worker();
            env.active_vus.fetch_add(1, Ordering::Relaxed);
            let mut done = 0u64;
            loop {
                if cancel.is_cancelled()
                    || env.soft_stop.is_cancelled()
                    || Instant::now() >= soft_deadline
                {
                    break;
                }
                match &shared {
                    Some(counter) => {
                        // Claim one shared iteration.
                        let mut remaining = counter.load(Ordering::Relaxed);
                        loop {
                            if remaining == 0 {
                                break;
                            }
                            match counter.compare_exchange_weak(
                                remaining,
                                remaining - 1,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            ) {
                                Ok(_) => break,
                                Err(actual) => remaining = actual,
                            }
                        }
                        if remaining == 0 {
                            break;
                        }
                    }
                    None => {
                        if done >= per_vu {
                            break;
                        }
                    }
                }
                if !env.wait_unpaused(&cancel).await {
                    break;
                }
                let keep_going = env.run_one(&mut worker, &cancel).await;
                done += 1;
                if !keep_going {
                    break;
                }
            }
            env.active_vus.fetch_sub(1, Ordering::Relaxed);
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    scenario_cancel.cancel();
}

enum RateSchedule {
    Constant {
        rate: f64,
        duration: Duration,
    },
    Ramping {
        start_rate: f64,
        stages: Vec<(Duration, f64)>,
    },
}

impl RateSchedule {
    fn total_duration(&self) -> Duration {
        match self {
            RateSchedule::Constant { duration, .. } => *duration,
            RateSchedule::Ramping { stages, .. } => stages.iter().map(|(d, _)| *d).sum(),
        }
    }

    /// Rate (iterations/second) at elapsed time `t`.
    fn rate_at(&self, t: Duration) -> f64 {
        match self {
            RateSchedule::Constant { rate, .. } => *rate,
            RateSchedule::Ramping { start_rate, stages } => {
                let mut from = *start_rate;
                let mut offset = Duration::ZERO;
                for (len, target) in stages {
                    if t < offset + *len {
                        let progress = (t - offset).as_secs_f64() / len.as_secs_f64().max(1e-9);
                        return from + (target - from) * progress;
                    }
                    from = *target;
                    offset += *len;
                }
                stages.last().map(|(_, r)| *r).unwrap_or(*start_rate)
            }
        }
    }
}

/// Arrival dispatch tick. Arrivals due within one tick are released as a
/// single burst, so the tick bounds both scheduling precision and burst size
/// (e.g. at 150k/s a 5ms tick wakes ~750 workers back-to-back). It also
/// bounds how long a published arrival stays claimable before it is recorded
/// as dropped (see [`ArrivalGate`]). Tunable via `LOADR_DISPATCH_TICK_US`;
/// 1000 is recommended above ~50k/s per process.
fn dispatch_tick() -> Duration {
    static TICK: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *TICK.get_or_init(|| {
        let us = std::env::var("LOADR_DISPATCH_TICK_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5000)
            .clamp(250, 1_000_000);
        Duration::from_micros(us)
    })
}

/// Shared hand-off state between the arrival dispatcher and its workers.
///
/// Published arrivals are semaphore permits: `publish` adds one permit per
/// whole arrival due this tick, and tokio's fair semaphore hands permits to
/// FIFO-parked workers first — waking exactly as many workers as there are
/// arrivals, never the whole pool (the broadcast over-waking measured in
/// goals/perf-round-two/dispatcher-ab/results.md). A permit already assigned
/// to a parked worker counts as claimed: it escapes tick expiry and runs
/// when that worker is polled — except across closure, where the worker
/// observes `Closed` instead and the permit is returned for the post-join
/// sweep. Conservation: every permit is consumed exactly once, by a worker's
/// `forget` (runs) or a dispatcher sweep (dropped).
struct ArrivalGate {
    /// Permits = published-but-unclaimed arrivals. Closing the semaphore
    /// linearizes shutdown: every `Claimed` happens strictly before
    /// `close()` (tokio checks the closed bit before assigned permits).
    budget: Semaphore,
    /// Workers currently parked in `claim_parked` (sizes pool growth).
    parked: AtomicU64,
    /// Cancelled at closure so waiters not blocked on the semaphore — the
    /// paused branch — still observe shutdown promptly instead of sleeping
    /// out the grace period.
    closed: CancellationToken,
}

enum Claim {
    Claimed,
    Empty,
    Closed,
}

struct ParkGuard<'a>(&'a ArrivalGate);

impl Drop for ParkGuard<'_> {
    fn drop(&mut self) {
        self.0.parked.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ArrivalGate {
    fn new() -> Self {
        Self {
            budget: Semaphore::new(0),
            parked: AtomicU64::new(0),
            closed: CancellationToken::new(),
        }
    }

    /// Claim one published arrival without waiting.
    fn try_claim(&self) -> Claim {
        match self.budget.try_acquire() {
            Ok(permit) => {
                permit.forget();
                Claim::Claimed
            }
            Err(TryAcquireError::NoPermits) => Claim::Empty,
            Err(TryAcquireError::Closed) => Claim::Closed,
        }
    }

    /// Park until an arrival is assigned to this worker (fair FIFO order)
    /// or dispatch closes. `true` = claimed exactly one arrival.
    async fn claim_parked(&self) -> bool {
        match self.budget.acquire().await {
            Ok(permit) => {
                permit.forget();
                true
            }
            Err(_) => false, // closed
        }
    }

    /// Count this worker as parked for the dispatcher's growth sizing until
    /// the guard drops.
    fn park(&self) -> ParkGuard<'_> {
        self.parked.fetch_add(1, Ordering::SeqCst);
        ParkGuard(self)
    }

    /// Publish a batch of whole arrivals. Permits go to parked workers
    /// first (waking exactly the assignees); leftovers pool up for running
    /// workers' `try_claim`. A fresh batch is claimable until `now + tick`;
    /// while an earlier batch's expiry is still pending (burst ticks), new
    /// arrivals share it rather than extending older work indefinitely.
    fn publish(&self, due: u64, expiry: &mut Option<Instant>, now: Instant, tick: Duration) {
        self.budget.add_permits(due as usize);
        expiry.get_or_insert(now + tick);
    }

    /// Expire the active batch once wall time reaches its deadline,
    /// returning the unclaimed remainder. Driven only by the dispatcher's
    /// tick — never by ticker-event count (`Burst` delivers several events
    /// back-to-back after a stall) and never from claims.
    fn expire_due(&self, expiry: &mut Option<Instant>, now: Instant) -> u64 {
        match *expiry {
            Some(at) if now >= at => {
                *expiry = None;
                self.take_unclaimed()
            }
            _ => 0,
        }
    }

    /// Sweep all unassigned permits in one CAS (permits already assigned to
    /// parked workers are claimed and not sweepable — see type docs).
    fn take_unclaimed(&self) -> u64 {
        self.budget.forget_permits(Semaphore::MAX_PERMITS) as u64
    }

    /// Close dispatch: no claim can start afterwards. Returns the pool
    /// remainder for drop accounting; the caller must sweep once more after
    /// joining workers to collect permits surrendered by parked workers'
    /// cancelled acquires.
    fn close(&self) -> u64 {
        self.budget.close();
        self.closed.cancel();
        self.take_unclaimed()
    }
}

/// Open-model executors: workers claim scheduled arrivals as semaphore
/// permits from a shared tick-bounded budget (see [`ArrivalGate`]); the
/// dispatcher publishes each tick's batch — waking exactly the workers that
/// receive one — grows the pool to `max_vus`, and records arrivals nobody
/// claimed within one dispatch interval as dropped iterations.
async fn run_arrival_rate(
    spec: &ScenarioRunSpec,
    env: &ExecEnv,
    scenario_cancel: &CancellationToken,
    schedule: RateSchedule,
    pre_allocated: u64,
    max_vus: u64,
) {
    let total = schedule.total_duration();
    let soft_deadline = Instant::now() + total;
    arm_graceful_stop(
        scenario_cancel,
        &env.soft_stop,
        Some(soft_deadline),
        spec.graceful_stop,
    );

    let gate = Arc::new(ArrivalGate::new());
    let mut worker_handles = Vec::new();
    // Workers claim arrivals themselves; newly spawned workers take the same
    // claim path (no implicit first iteration). A claim flows unconditionally
    // into the iteration: any check between claim and run would leak budget.
    let spawn_worker = |env: ExecEnv, cancel: CancellationToken, gate: Arc<ArrivalGate>| {
        tokio::spawn(async move {
            let mut worker = env.new_worker();
            env.active_vus.fetch_add(1, Ordering::Relaxed);
            'live: loop {
                // Re-check pause before every claim. A permit assigned just
                // as pause flips runs one extra iteration — bounded by one
                // tick's batch, same class as the pre-claim race of the
                // broadcast design. The closed arm keeps a run that ends
                // while paused from sleeping out the grace period.
                if *env.pause.borrow() {
                    tokio::select! {
                        _ = gate.closed.cancelled() => break,
                        ok = env.wait_unpaused(&cancel) => {
                            if !ok {
                                break;
                            }
                        }
                    }
                    continue;
                }
                match gate.try_claim() {
                    Claim::Claimed => {
                        if !env.run_one(&mut worker, &cancel).await {
                            break 'live;
                        }
                    }
                    Claim::Closed => break,
                    Claim::Empty => {
                        let parked = gate.park();
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            got = gate.claim_parked() => {
                                drop(parked);
                                if !got {
                                    break; // closed while parked
                                }
                                if !env.run_one(&mut worker, &cancel).await {
                                    break 'live;
                                }
                            }
                        }
                    }
                }
            }
            env.active_vus.fetch_sub(1, Ordering::Relaxed);
        })
    };

    for _ in 0..pre_allocated {
        worker_handles.push(spawn_worker(
            env.clone(),
            scenario_cancel.clone(),
            gate.clone(),
        ));
    }
    let mut allocated = pre_allocated;

    let dropped_metric = &env.builtins.dropped_iterations;
    let scenario_tags = env.runner.program.tags.clone();
    let record_dropped = |n: u64| {
        if n > 0 {
            env.metrics
                .counter(dropped_metric, n as f64, &scenario_tags);
        }
    };

    let started = Instant::now();
    let mut emitted: f64 = 0.0; // fractional iterations owed
    let tick = dispatch_tick();
    let mut ticker = tokio::time::interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let deadline = tokio::time::sleep_until(soft_deadline.into());
    tokio::pin!(deadline);
    let mut last = Instant::now();
    // Wall-clock deadline of the active batch; enforced on dispatcher wakes.
    let mut expiry: Option<Instant> = None;
    // Distinguishes reaching the schedule's natural end from stop/cancel:
    // only a natural end drains the final flushed batch before closing.
    let mut natural_end = false;

    loop {
        let deadline_elapsed = tokio::select! {
            _ = scenario_cancel.cancelled() => break,
            _ = env.soft_stop.cancelled() => break,
            _ = ticker.tick() => false,
            _ = &mut deadline => true,
        };
        // A configurable tick may be as long as (or longer than) the whole
        // schedule. Credit and publish the interval ending at the deadline
        // before exiting, with the deadline itself as a wake-up so the flush
        // is not late; every path below breaks once the deadline elapsed, so
        // the completed sleep is never polled again.
        let now = Instant::now().min(soft_deadline);
        let deadline_elapsed = deadline_elapsed || now >= soft_deadline;
        record_dropped(gate.expire_due(&mut expiry, now));
        if *env.pause.borrow() {
            // Paused ticks publish nothing and expire what was already
            // published, so resume starts with no claimable backlog.
            // Workers stay parked until the next productive broadcast.
            if expiry.take().is_some() {
                record_dropped(gate.take_unclaimed());
            }
            last = now;
            if deadline_elapsed {
                natural_end = true;
                break;
            }
            continue;
        }
        let dt = (now - last).as_secs_f64();
        last = now;
        emitted += schedule.rate_at(started.elapsed()) * dt;
        let due = emitted as u64;
        if due == 0 {
            if deadline_elapsed {
                natural_end = true;
                break;
            }
            continue;
        }
        emitted -= due as f64;
        // Grow for arrivals the parked workers can't absorb, up to max_vus.
        // Workers waiting out a pause are not counted, so the first tick
        // after a resume may over-spawn slightly — capped by max_vus.
        let parked = gate.parked.load(Ordering::SeqCst);
        let grow = due
            .saturating_sub(parked)
            .min(max_vus.saturating_sub(allocated));
        for _ in 0..grow {
            worker_handles.push(spawn_worker(
                env.clone(),
                scenario_cancel.clone(),
                gate.clone(),
            ));
        }
        allocated += grow;
        gate.publish(due, &mut expiry, now, tick);
        if deadline_elapsed {
            natural_end = true;
            break;
        }
    }
    // A natural end flushes the final partial interval above, but closing in
    // the same poll would drop all of it: closure fails the acquires of
    // permits tokio has already assigned to parked workers (the closed bit
    // wins — see ArrivalGate), before those workers ever run. Let the active
    // batch live out its claim window instead: ticks keep driving expiry, so
    // this waits at most one dispatch interval, and stop/cancel (including
    // the graceful-stop watchdog) still exits immediately.
    if natural_end {
        while expiry.is_some() {
            tokio::select! {
                _ = scenario_cancel.cancelled() => break,
                _ = env.soft_stop.cancelled() => break,
                _ = ticker.tick() => {}
            }
            if *env.pause.borrow() {
                if expiry.take().is_some() {
                    record_dropped(gate.take_unclaimed());
                }
                break;
            }
            record_dropped(gate.expire_due(&mut expiry, Instant::now()));
        }
    }
    // Deadline, soft stop, or cancellation: unclaimed budget is dropped, and
    // closing releases parked and paused workers immediately (idle workers
    // must not wait out the grace period) while in-flight iterations finish
    // under the graceful window armed above.
    record_dropped(gate.close());
    for h in worker_handles {
        let _ = h.await;
    }
    // Permits that were assigned to parked workers at closure return to the
    // pool only as their acquire futures drop during the join; sweep them so
    // published = completed + dropped stays exact.
    record_dropped(gate.take_unclaimed());
    scenario_cancel.cancel();
}

async fn run_externally_controlled(
    spec: &ScenarioRunSpec,
    env: &ExecEnv,
    scenario_cancel: &CancellationToken,
    max_vus: u64,
    duration: Option<Duration>,
) {
    let soft_deadline = duration.map(|d| Instant::now() + d);
    arm_graceful_stop(
        scenario_cancel,
        &env.soft_stop,
        soft_deadline,
        spec.graceful_stop,
    );
    let allowed_rx = match &env.external_target {
        Some(rx) => rx.clone(),
        None => {
            // No external control connected: run at 0 VUs until stopped.
            let (_tx, rx) = watch::channel(0u64);
            rx
        }
    };
    run_allocated_pool(
        env,
        scenario_cancel,
        max_vus,
        allowed_rx,
        soft_deadline,
        spec.graceful_ramp_down,
    )
    .await;
    scenario_cancel.cancel();
}

/// Split an executor spec across `count` instances for distributed execution.
/// VU counts and shared iteration budgets split with remainder going to the
/// lowest indices; rates split fractionally so the global rate is exact.
pub fn partition_spec(spec: &ExecutorSpec, index: u64, count: u64) -> ExecutorSpec {
    assert!(count > 0 && index < count, "invalid partition");
    let split = |n: u64| -> u64 { n / count + u64::from(index < n % count) };
    let split_min1 = |n: u64| -> u64 { split(n).max(u64::from(n > 0)) };
    let frac = |r: f64| -> f64 { r / count as f64 };
    match spec {
        ExecutorSpec::ConstantVus { vus, duration } => ExecutorSpec::ConstantVus {
            vus: split(*vus),
            duration: *duration,
        },
        ExecutorSpec::RampingVus { start_vus, stages } => ExecutorSpec::RampingVus {
            start_vus: split(*start_vus),
            stages: stages.iter().map(|(d, t)| (*d, split(*t))).collect(),
        },
        ExecutorSpec::ConstantArrivalRate {
            rate,
            duration,
            pre_allocated_vus,
            max_vus,
        } => ExecutorSpec::ConstantArrivalRate {
            rate: frac(*rate),
            duration: *duration,
            pre_allocated_vus: split_min1(*pre_allocated_vus),
            max_vus: split_min1(*max_vus),
        },
        ExecutorSpec::RampingArrivalRate {
            start_rate,
            stages,
            pre_allocated_vus,
            max_vus,
        } => ExecutorSpec::RampingArrivalRate {
            start_rate: frac(*start_rate),
            stages: stages.iter().map(|(d, r)| (*d, frac(*r))).collect(),
            pre_allocated_vus: split_min1(*pre_allocated_vus),
            max_vus: split_min1(*max_vus),
        },
        ExecutorSpec::PerVuIterations {
            vus,
            iterations,
            max_duration,
        } => ExecutorSpec::PerVuIterations {
            vus: split(*vus),
            iterations: *iterations,
            max_duration: *max_duration,
        },
        ExecutorSpec::SharedIterations {
            vus,
            iterations,
            max_duration,
        } => ExecutorSpec::SharedIterations {
            vus: split_min1(*vus),
            iterations: split(*iterations),
            max_duration: *max_duration,
        },
        ExecutorSpec::ExternallyControlled { max_vus, duration } => {
            ExecutorSpec::ExternallyControlled {
                max_vus: split_min1(*max_vus),
                duration: *duration,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vus_interpolation() {
        let stages = vec![
            (Duration::from_secs(10), 10u64),
            (Duration::from_secs(10), 10u64),
            (Duration::from_secs(10), 0u64),
        ];
        assert_eq!(vus_at(0, &stages, Duration::ZERO), 0);
        assert_eq!(vus_at(0, &stages, Duration::from_secs(5)), 5);
        assert_eq!(vus_at(0, &stages, Duration::from_secs(10)), 10);
        assert_eq!(vus_at(0, &stages, Duration::from_secs(15)), 10);
        assert_eq!(vus_at(0, &stages, Duration::from_secs(25)), 5);
        assert_eq!(vus_at(0, &stages, Duration::from_secs(40)), 0);
    }

    #[test]
    fn rate_interpolation() {
        let schedule = RateSchedule::Ramping {
            start_rate: 0.0,
            stages: vec![
                (Duration::from_secs(10), 100.0),
                (Duration::from_secs(10), 100.0),
            ],
        };
        assert!((schedule.rate_at(Duration::from_secs(5)) - 50.0).abs() < 1e-9);
        assert!((schedule.rate_at(Duration::from_secs(15)) - 100.0).abs() < 1e-9);
        assert!((schedule.rate_at(Duration::from_secs(30)) - 100.0).abs() < 1e-9);
    }

    #[test]
    fn partition_splits_vus_with_remainder() {
        let spec = ExecutorSpec::ConstantVus {
            vus: 10,
            duration: Duration::from_secs(60),
        };
        let parts: Vec<u64> = (0..3)
            .map(|i| match partition_spec(&spec, i, 3) {
                ExecutorSpec::ConstantVus { vus, .. } => vus,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(parts, vec![4, 3, 3]);
        assert_eq!(parts.iter().sum::<u64>(), 10);
    }

    #[test]
    fn partition_splits_rate_exactly() {
        let spec = ExecutorSpec::ConstantArrivalRate {
            rate: 100.0,
            duration: Duration::from_secs(60),
            pre_allocated_vus: 10,
            max_vus: 20,
        };
        let total: f64 = (0..4)
            .map(|i| match partition_spec(&spec, i, 4) {
                ExecutorSpec::ConstantArrivalRate { rate, .. } => rate,
                _ => unreachable!(),
            })
            .sum();
        assert!((total - 100.0).abs() < 1e-9);
    }

    #[test]
    fn partition_shared_iterations_sum() {
        let spec = ExecutorSpec::SharedIterations {
            vus: 5,
            iterations: 101,
            max_duration: Duration::from_secs(600),
        };
        let total: u64 = (0..4)
            .map(|i| match partition_spec(&spec, i, 4) {
                ExecutorSpec::SharedIterations { iterations, .. } => iterations,
                _ => unreachable!(),
            })
            .sum();
        assert_eq!(total, 101);
    }

    #[test]
    fn partition_never_zeroes_arrival_workers() {
        let spec = ExecutorSpec::ConstantArrivalRate {
            rate: 10.0,
            duration: Duration::from_secs(10),
            pre_allocated_vus: 2,
            max_vus: 2,
        };
        for i in 0..5 {
            match partition_spec(&spec, i, 5) {
                ExecutorSpec::ConstantArrivalRate {
                    pre_allocated_vus,
                    max_vus,
                    ..
                } => {
                    assert!(pre_allocated_vus >= 1);
                    assert!(max_vus >= 1);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn gate_claims_are_exact_and_never_underflow() {
        let gate = ArrivalGate::new();
        assert!(matches!(gate.try_claim(), Claim::Empty));
        let mut expiry = None;
        gate.publish(3, &mut expiry, Instant::now(), Duration::from_millis(5));
        for _ in 0..3 {
            assert!(matches!(gate.try_claim(), Claim::Claimed));
        }
        assert!(matches!(gate.try_claim(), Claim::Empty));
    }

    #[test]
    fn gate_concurrent_claims_match_budget_exactly() {
        let gate = Arc::new(ArrivalGate::new());
        let mut expiry = None;
        gate.publish(1000, &mut expiry, Instant::now(), Duration::from_millis(5));
        let claimed = Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (gate, claimed) = (gate.clone(), claimed.clone());
                std::thread::spawn(move || {
                    while matches!(gate.try_claim(), Claim::Claimed) {
                        claimed.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(claimed.load(Ordering::SeqCst), 1000);
        assert!(matches!(gate.try_claim(), Claim::Empty));
    }

    #[test]
    fn gate_expiry_accounting_is_exact() {
        let gate = ArrivalGate::new();
        let tick = Duration::from_millis(5);
        let t0 = Instant::now();
        let mut expiry = None;
        assert_eq!(gate.expire_due(&mut expiry, t0), 0); // nothing active

        gate.publish(3, &mut expiry, t0, tick);
        assert_eq!(expiry, Some(t0 + tick));
        gate.publish(2, &mut expiry, t0 + tick / 2, tick); // burst batch
        assert_eq!(expiry, Some(t0 + tick)); // shares, never extends

        assert!(matches!(gate.try_claim(), Claim::Claimed));
        assert_eq!(
            gate.expire_due(&mut expiry, t0 + tick - Duration::from_micros(1)),
            0
        );
        // 5 published = 1 claimed + 4 dropped.
        assert_eq!(gate.expire_due(&mut expiry, t0 + tick), 4);
        assert_eq!(expiry, None);
        assert!(matches!(gate.try_claim(), Claim::Empty));

        gate.publish(1, &mut expiry, t0 + tick * 2, tick); // fresh window afterwards
        assert_eq!(expiry, Some(t0 + tick * 3));
    }

    #[test]
    fn gate_close_returns_unclaimed_and_blocks_claims() {
        let gate = ArrivalGate::new();
        let mut expiry = None;
        gate.publish(5, &mut expiry, Instant::now(), Duration::from_millis(5));
        assert!(matches!(gate.try_claim(), Claim::Claimed));
        assert!(matches!(gate.try_claim(), Claim::Claimed));
        assert_eq!(gate.close(), 3);
        assert!(matches!(gate.try_claim(), Claim::Closed)); // Closed, not Empty
        assert_eq!(gate.take_unclaimed(), 0);
    }

    /// Drives the real worker loop shape (hot try_claim, park, select with a
    /// cancel arm) against publishes that feed tokio's assign-on-add path:
    /// every published permit must be claimed or swept — none may vanish into
    /// a waiter node. Odd reps cancel before close to race the Acquire-drop
    /// permit-return path; the post-join sweep is what keeps the identity
    /// exact (assigned permits return to the pool only as workers' futures
    /// drop during join).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn gate_assign_race_loses_no_permits() {
        for rep in 0..200u32 {
            let gate = Arc::new(ArrivalGate::new());
            let cancel = CancellationToken::new();
            let claimed = Arc::new(AtomicU64::new(0));
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    let (gate, cancel, claimed) = (gate.clone(), cancel.clone(), claimed.clone());
                    tokio::spawn(async move {
                        loop {
                            match gate.try_claim() {
                                Claim::Claimed => {
                                    claimed.fetch_add(1, Ordering::SeqCst);
                                    tokio::task::yield_now().await;
                                }
                                Claim::Closed => break,
                                Claim::Empty => {
                                    let parked = gate.park();
                                    tokio::select! {
                                        _ = cancel.cancelled() => break,
                                        got = gate.claim_parked() => {
                                            drop(parked);
                                            if !got {
                                                break; // closed while parked
                                            }
                                            claimed.fetch_add(1, Ordering::SeqCst);
                                        }
                                    }
                                }
                            }
                        }
                    })
                })
                .collect();
            // Wait until all four are parked so publishes exercise the
            // assign-to-waiter path, not just the pool. `parked` rises just
            // before the acquire's first poll, so add one scheduler beat.
            while gate.parked.load(Ordering::SeqCst) < 4 {
                tokio::task::yield_now().await;
            }
            tokio::task::yield_now().await;
            let mut expiry = None;
            let mut published = 0u64;
            for i in 0..50u64 {
                let due = 1 + (i % 3); // < worker count: batches feed waiters
                gate.publish(due, &mut expiry, Instant::now(), Duration::from_secs(3600));
                published += due;
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(Duration::from_millis(2)).await; // let claimers drain
            if rep % 2 == 1 {
                cancel.cancel(); // race the cancel-arm permit-return path
            }
            let mut dropped = gate.close();
            let joined = tokio::time::timeout(Duration::from_secs(5), async {
                for w in workers {
                    w.await.unwrap();
                }
            })
            .await;
            assert!(joined.is_ok(), "a worker slept through close");
            dropped += gate.take_unclaimed(); // post-join sweep — load-bearing
            assert_eq!(claimed.load(Ordering::SeqCst) + dropped, published);
            assert_eq!(gate.parked.load(Ordering::SeqCst), 0);
            assert!(matches!(gate.try_claim(), Claim::Closed));
        }
    }
}
