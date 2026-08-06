//! The run orchestrator: wires config, scenarios, metrics, thresholds,
//! outputs and the script engine into one run with live control.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use loadr_config::{Template, TestPlan};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::aggregate::{Aggregator, MetricShards, Snapshot};
use crate::error::EngineError;
use crate::executor::{partition_spec, run_scenario, ExecEnv, ScenarioRunSpec};
use crate::flow::{FlowRunner, ScenarioProgram};
use crate::metrics::{BuiltinMetrics, MetricRegistry, MetricsBus, Sample, Tags};
use crate::output::Output;
use crate::protocol::ProtocolRegistry;
use crate::script::ScriptEngine;
use crate::summary::{Summary, TimelinePoint};
use crate::thresholds::{compile_thresholds, evaluate_all, CompiledThreshold, ThresholdStatus};
use crate::vu::{RunContext, VuContext};

/// Run status for live consumers.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Stopping,
    Finished { passed: bool },
}

/// Options for constructing an [`Engine`].
pub struct EngineOptions {
    pub run_id: Option<String>,
    /// Stable identity of this load-generating process within a run.
    pub instance_id: Option<String>,
    pub protocols: ProtocolRegistry,
    pub script: Option<Arc<dyn ScriptEngine>>,
    pub outputs: Vec<Box<dyn Output>>,
    /// Distributed partition: (index, count). None = the whole load.
    pub partition: Option<(u64, u64)>,
    /// Extra tags on every sample (e.g. `instance` in distributed mode).
    pub extra_tags: Tags,
    pub snapshot_interval: Duration,
    /// Loaded `data_source`-capable plugins, keyed by the `plugins:` name
    /// that `data.<name>.source` refers to.
    pub data_sources: HashMap<String, Box<dyn crate::data::DataSourcePlugin>>,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            run_id: None,
            instance_id: None,
            protocols: ProtocolRegistry::new(),
            script: None,
            outputs: Vec::new(),
            partition: None,
            extra_tags: Tags::new(),
            snapshot_interval: Duration::from_secs(1),
            data_sources: HashMap::new(),
        }
    }
}

/// The result of a completed run.
#[derive(Debug)]
pub struct RunResult {
    pub summary: Summary,
    /// All thresholds passed.
    pub passed: bool,
    pub aborted: Option<String>,
    /// Text returned by the JS `handleSummary(data)` hook, if exported (k6).
    pub custom_summary: Option<String>,
}

/// Cloneable live handle: snapshots, status, stop/pause/scale.
#[derive(Clone)]
pub struct RunHandle {
    pub run_id: Arc<str>,
    snapshots: watch::Receiver<Arc<Snapshot>>,
    aggregates: watch::Receiver<Arc<Snapshot>>,
    thresholds: watch::Receiver<Arc<Vec<ThresholdStatus>>>,
    status: watch::Receiver<RunStatus>,
    soft_stop: CancellationToken,
    hard_stop: CancellationToken,
    pause: Arc<watch::Sender<bool>>,
    stop_reason: Arc<parking_lot::Mutex<Option<String>>>,
    scale: Arc<parking_lot::Mutex<HashMap<String, watch::Sender<u64>>>>,
}

impl RunHandle {
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshots.borrow().clone()
    }

    pub fn watch_snapshots(&self) -> watch::Receiver<Arc<Snapshot>> {
        self.snapshots.clone()
    }

    /// Exact cumulative rollups for the whole run and each scenario. Unlike a
    /// regular snapshot, tagged histograms are merged before percentiles are
    /// calculated.
    pub fn aggregate_snapshot(&self) -> Arc<Snapshot> {
        self.aggregates.borrow().clone()
    }

    pub fn threshold_statuses(&self) -> Arc<Vec<ThresholdStatus>> {
        self.thresholds.borrow().clone()
    }

    pub fn status(&self) -> RunStatus {
        self.status.borrow().clone()
    }

    pub fn watch_status(&self) -> watch::Receiver<RunStatus> {
        self.status.clone()
    }

    /// Graceful stop: no new iterations; in-flight ones get their grace period.
    pub fn stop(&self, reason: impl Into<String>) {
        *self.stop_reason.lock() = Some(reason.into());
        self.soft_stop.cancel();
    }

    /// Immediate abort.
    pub fn kill(&self, reason: impl Into<String>) {
        *self.stop_reason.lock() = Some(reason.into());
        self.soft_stop.cancel();
        self.hard_stop.cancel();
    }

    pub fn pause(&self, paused: bool) {
        let _ = self.pause.send(paused);
    }

    pub fn is_paused(&self) -> bool {
        *self.pause.borrow()
    }

    /// Scale an `externally-controlled` scenario to `vus`.
    pub fn scale(&self, scenario: &str, vus: u64) -> Result<(), String> {
        let map = self.scale.lock();
        match map.get(scenario) {
            Some(tx) => {
                let _ = tx.send(vus);
                Ok(())
            }
            None => Err(format!(
                "scenario `{scenario}` is not externally controlled (available: {})",
                map.keys().cloned().collect::<Vec<_>>().join(", ")
            )),
        }
    }

    pub fn externally_controlled_scenarios(&self) -> Vec<String> {
        self.scale.lock().keys().cloned().collect()
    }
}

struct PreparedScenario {
    run_spec: ScenarioRunSpec,
    program: Arc<ScenarioProgram>,
}

/// The engine for one test run.
pub struct Engine {
    plan_name: Option<String>,
    run_id: Arc<str>,
    scenarios: Vec<PreparedScenario>,
    thresholds: Vec<CompiledThreshold>,
    run_ctx: Arc<RunContext>,
    protocols: Arc<ProtocolRegistry>,
    script: Option<Arc<dyn ScriptEngine>>,
    outputs: Vec<Box<dyn Output>>,
    builtins: Arc<BuiltinMetrics>,
    gauge_tags: Arc<Tags>,
    snapshot_interval: Duration,
    http_defaults: Arc<loadr_config::HttpDefaults>,
    // Handle plumbing.
    handle: RunHandle,
    snapshots_tx: watch::Sender<Arc<Snapshot>>,
    aggregates_tx: watch::Sender<Arc<Snapshot>>,
    thresholds_tx: watch::Sender<Arc<Vec<ThresholdStatus>>>,
    status_tx: watch::Sender<RunStatus>,
    external_targets: HashMap<String, watch::Receiver<u64>>,
}

impl Engine {
    pub fn new(
        plan: TestPlan,
        base_dir: PathBuf,
        opts: EngineOptions,
    ) -> Result<Engine, EngineError> {
        let mut plan = plan;
        // Extra tags become defaults so every program embeds them.
        for (k, v) in &opts.extra_tags {
            plan.defaults.tags.insert(k.clone(), v.clone());
        }

        let run_id: Arc<str> = Arc::from(
            opts.run_id
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        );
        let instance_id: Arc<str> = Arc::from(
            opts.instance_id
                .clone()
                .unwrap_or_else(|| "local".to_string()),
        );

        let env: HashMap<String, String> = std::env::vars().collect();

        // Static interpolation of variables (env-only references).
        let mut variables = serde_json::Map::new();
        for (name, value) in &plan.variables {
            let resolved = resolve_static_value(value, &env)
                .map_err(|e| EngineError::Config(format!("variable `{name}`: {e}")))?;
            variables.insert(name.clone(), resolved);
        }

        // Secrets.
        let mut secrets = HashMap::new();
        for (name, source) in &plan.secrets {
            let value = if let Some(var) = &source.env {
                env.get(var).cloned().ok_or_else(|| {
                    EngineError::Config(format!(
                        "secret `{name}`: environment variable `{var}` is not set"
                    ))
                })?
            } else if let Some(file) = &source.file {
                let resolved = if file.is_absolute() {
                    file.clone()
                } else {
                    base_dir.join(file)
                };
                std::fs::read_to_string(&resolved)
                    .map_err(|e| EngineError::Io {
                        path: resolved.display().to_string(),
                        source: e,
                    })?
                    .trim()
                    .to_string()
            } else {
                return Err(EngineError::Config(format!(
                    "secret `{name}` has no `env` or `file` source"
                )));
            };
            secrets.insert(name.clone(), value);
        }

        // Metrics registry: builtins + YAML custom metrics.
        let registry = Arc::new(MetricRegistry::with_builtins());
        for (name, def) in &plan.metrics {
            registry
                .register(name, def.kind.into(), def.time, def.description.clone())
                .map_err(EngineError::Config)?;
        }
        let builtins = Arc::new(BuiltinMetrics::resolve(&registry));

        // Data feeds.
        let mut data = crate::data::DataFeeds::load(&plan.data, &base_dir, opts.data_sources)?;
        data.set_execution_identity(
            Arc::clone(&run_id),
            instance_id,
            opts.partition.unwrap_or((0, 1)),
        );
        let gauge_tags = Arc::new(plan.defaults.tags.clone());

        let run_ctx = Arc::new(RunContext {
            variables,
            secrets,
            env,
            data,
            registry,
            base_dir: base_dir.clone(),
            setup_data: parking_lot::RwLock::new(serde_json::Value::Null),
        });

        // Thresholds.
        let thresholds = compile_thresholds(&plan.thresholds).map_err(EngineError::Config)?;

        // Scenarios.
        let mut scenarios = Vec::new();
        let mut external_targets = HashMap::new();
        let scale_map: HashMap<String, watch::Sender<u64>> = HashMap::new();
        let scale = Arc::new(parking_lot::Mutex::new(scale_map));
        for (name, scenario) in &plan.scenarios {
            let mut spec = scenario
                .executor_spec()
                .map_err(|e| EngineError::Config(format!("scenario `{name}`: {e}")))?;
            if let Some((index, count)) = opts.partition {
                spec = partition_spec(&spec, index, count);
            }
            if let loadr_config::ExecutorSpec::ExternallyControlled { .. } = &spec {
                let (tx, rx) = watch::channel(0u64);
                scale.lock().insert(name.clone(), tx);
                external_targets.insert(name.clone(), rx);
            }
            let program = Arc::new(ScenarioProgram::compile(&plan, name, scenario, &base_dir)?);
            scenarios.push(PreparedScenario {
                run_spec: ScenarioRunSpec {
                    name: Arc::from(name.as_str()),
                    spec,
                    start_time: scenario
                        .start_time
                        .map(|d| d.as_duration())
                        .unwrap_or(Duration::ZERO),
                    graceful_stop: scenario
                        .graceful_stop
                        .map(|d| d.as_duration())
                        .unwrap_or(Duration::from_secs(30)),
                    graceful_ramp_down: scenario
                        .graceful_ramp_down
                        .map(|d| d.as_duration())
                        .unwrap_or(Duration::from_secs(30)),
                },
                program,
            });
        }
        if scenarios.is_empty() {
            return Err(EngineError::Config(
                "test has no scenarios to run".to_string(),
            ));
        }

        let (snapshots_tx, snapshots_rx) = watch::channel(Arc::new(Snapshot::default()));
        let (aggregates_tx, aggregates_rx) = watch::channel(Arc::new(Snapshot::default()));
        let (thresholds_tx, thresholds_rx) = watch::channel(Arc::new(Vec::new()));
        let (status_tx, status_rx) = watch::channel(RunStatus::Pending);
        let (pause_tx, _pause_rx) = watch::channel(false);
        let handle = RunHandle {
            run_id: run_id.clone(),
            snapshots: snapshots_rx,
            aggregates: aggregates_rx,
            thresholds: thresholds_rx,
            status: status_rx,
            soft_stop: CancellationToken::new(),
            hard_stop: CancellationToken::new(),
            pause: Arc::new(pause_tx),
            stop_reason: Arc::new(parking_lot::Mutex::new(None)),
            scale,
        };

        Ok(Engine {
            plan_name: plan.name.clone(),
            run_id,
            scenarios,
            thresholds,
            run_ctx,
            protocols: Arc::new(opts.protocols),
            script: opts.script,
            outputs: opts.outputs,
            builtins,
            gauge_tags,
            snapshot_interval: opts.snapshot_interval,
            http_defaults: Arc::new(plan.defaults.http.clone()),
            handle,
            snapshots_tx,
            aggregates_tx,
            thresholds_tx,
            status_tx,
            external_targets,
        })
    }

    pub fn handle(&self) -> RunHandle {
        self.handle.clone()
    }

    /// Run the test to completion.
    pub async fn run(mut self) -> Result<RunResult, EngineError> {
        let started_ms = crate::metrics::now_millis();
        // Any output that consumes raw samples pins the whole run to the
        // classic bus/channel path; otherwise VUs record straight into shard
        // aggregators — no channel, no per-sample clock read, no end-of-run
        // drain backlog (see `Output::wants_samples`, `MetricShards`).
        let wants_samples = self.outputs.iter().any(|o| o.wants_samples());
        let (bus, sample_source, shard_done) = if wants_samples {
            let (bus, samples_rx) = MetricsBus::new();
            (bus, SampleSource::Bus(samples_rx), None)
        } else {
            let shard_count = std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1)
                .min(16);
            let shards = Arc::new(MetricShards::new(shard_count));
            let bus = MetricsBus::sharded(shards.clone());
            let done = CancellationToken::new();
            (
                bus,
                SampleSource::Shards {
                    shards,
                    done: done.clone(),
                },
                Some(done),
            )
        };
        let _ = self.status_tx.send(RunStatus::Running);

        // Abort channel: threshold failures, abort_test assertions.
        let (abort_tx, mut abort_rx) = mpsc::unbounded_channel::<String>();

        // Start outputs.
        let mut outputs = std::mem::take(&mut self.outputs);
        for output in &mut outputs {
            output.start().await?;
        }

        // Aggregator task.
        let agg_task = {
            let snapshots_tx = self.snapshots_tx.clone();
            let aggregates_tx = self.aggregates_tx.clone();
            let thresholds_tx = self.thresholds_tx.clone();
            let thresholds = std::mem::take(&mut self.thresholds);
            let abort_tx = abort_tx.clone();
            let interval = self.snapshot_interval;
            tokio::spawn(aggregator_loop(
                sample_source,
                outputs,
                thresholds,
                (snapshots_tx, aggregates_tx),
                thresholds_tx,
                abort_tx,
                interval,
            ))
        };

        // A FlowRunner for setup/teardown host calls (plan-level defaults).
        let lifecycle_program = Arc::new(ScenarioProgram {
            name: Arc::from("setup"),
            steps: Vec::new(),
            exec: None,
            on_start: None,
            on_stop: None,
            think_time: None,
            pacing: None,
            throttle: None,
            faults: None,
            barriers: parking_lot::Mutex::new(std::collections::HashMap::new()),
            tags: Arc::new({
                let mut t = Tags::new();
                t.insert("scenario".into(), "setup".into());
                t
            }),
            http: self.http_defaults.clone(),
            cookies_auto: self.http_defaults.cookies,
        });
        let lifecycle_runner = Arc::new(FlowRunner {
            program: lifecycle_program,
            protocols: self.protocols.clone(),
            builtins: self.builtins.clone(),
        });

        // setup()
        if let Some(script) = &self.script {
            let mut vu = VuContext::new(
                0,
                Arc::from("setup"),
                lifecycle_runner.program.tags.clone(),
                bus.clone(),
                self.run_ctx.clone(),
                self.http_defaults.cookies,
            );
            let setup_result =
                crate::flow::with_host(&lifecycle_runner, &mut vu, |host| script.setup(host));
            match setup_result {
                Ok(data) => {
                    *self.run_ctx.setup_data.write() = data;
                }
                Err(e) => {
                    self.handle.kill(format!("setup() failed: {e}"));
                    // Otherwise `agg_task` never sees end-of-input: in shard
                    // mode nothing ever cancels `done`; in bus mode `vu` and
                    // `bus` both still hold a live sender, so `recv()` would
                    // never return `None`. Either way `agg_task.await` below
                    // would hang forever instead of letting this early abort
                    // return an error.
                    if let Some(done) = &shard_done {
                        done.cancel();
                    }
                    drop(vu);
                    drop(bus);
                    let _ = agg_task.await;
                    return Err(EngineError::Script(format!("setup() failed: {e}")));
                }
            }
        }

        // VU gauge reporter.
        let active_counters: Vec<Arc<AtomicU64>> = self
            .scenarios
            .iter()
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect();
        let requests_in_flight_metric = self
            .run_ctx
            .registry
            .get("requests_in_flight")
            .map(|d| d.name)
            .unwrap_or_else(|| Arc::from("requests_in_flight"));
        let gauge_task = {
            let bus = bus.clone();
            let builtins = self.builtins.clone();
            let requests_in_flight_metric = requests_in_flight_metric.clone();
            let tags = self.gauge_tags.clone();
            let counters = active_counters.clone();
            let stop = self.handle.hard_stop.clone();
            let done = CancellationToken::new();
            let done_child = done.clone();
            let task = tokio::spawn(async move {
                let mut max_seen = 0u64;
                let mut ticker = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = stop.cancelled() => break,
                        _ = done_child.cancelled() => break,
                    }
                    let active: u64 = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
                    max_seen = max_seen.max(active);
                    bus.gauge(&builtins.vus, active as f64, &tags);
                    bus.gauge(&builtins.vus_max, max_seen as f64, &tags);
                    bus.gauge(
                        &requests_in_flight_metric,
                        bus.requests_in_flight() as f64,
                        &tags,
                    );
                }
            });
            (task, done)
        };

        // Spawn scenarios.
        let vu_ids = Arc::new(AtomicU64::new(0));
        let mut scenario_tasks = Vec::new();
        let scenario_names: Vec<String> = self
            .scenarios
            .iter()
            .map(|s| s.run_spec.name.to_string())
            .collect();
        for (i, prepared) in self.scenarios.drain(..).enumerate() {
            let runner = Arc::new(FlowRunner {
                program: prepared.program.clone(),
                protocols: self.protocols.clone(),
                builtins: self.builtins.clone(),
            });
            let env = ExecEnv {
                runner,
                run_ctx: self.run_ctx.clone(),
                metrics: bus.clone(),
                builtins: self.builtins.clone(),
                script: self.script.clone(),
                soft_stop: self.handle.soft_stop.clone(),
                hard_stop: self.handle.hard_stop.clone(),
                pause: self.handle.pause.subscribe(),
                vu_ids: vu_ids.clone(),
                active_vus: active_counters[i].clone(),
                abort_tx: abort_tx.clone(),
                external_target: self
                    .external_targets
                    .get(prepared.run_spec.name.as_ref())
                    .cloned(),
            };
            scenario_tasks.push(tokio::spawn(run_scenario(prepared.run_spec, env)));
        }
        drop(abort_tx);

        // Wait for completion or abort.
        let mut aborted: Option<String> = None;
        let all_done = futures::future::join_all(scenario_tasks);
        tokio::pin!(all_done);
        tokio::select! {
            _ = &mut all_done => {}
            reason = abort_rx.recv() => {
                if let Some(reason) = reason {
                    tracing::warn!(reason = %reason, "aborting run");
                    aborted = Some(reason.clone());
                    *self.handle.stop_reason.lock() = Some(reason);
                    let _ = self.status_tx.send(RunStatus::Stopping);
                    self.handle.soft_stop.cancel();
                    // Safety net: hard-cancel if graceful stop hangs.
                    let hard = self.handle.hard_stop.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(35)).await;
                        hard.cancel();
                    });
                    (&mut all_done).await;
                }
            }
            _ = self.handle.soft_stop.cancelled() => {
                let _ = self.status_tx.send(RunStatus::Stopping);
                let hard = self.handle.hard_stop.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(35)).await;
                    hard.cancel();
                });
                (&mut all_done).await;
            }
        }
        if aborted.is_none() {
            aborted = self.handle.stop_reason.lock().clone();
        }

        // teardown()
        if let Some(script) = &self.script {
            let mut vu = VuContext::new(
                0,
                Arc::from("teardown"),
                lifecycle_runner.program.tags.clone(),
                bus.clone(),
                self.run_ctx.clone(),
                self.http_defaults.cookies,
            );
            let setup_data = self.run_ctx.setup_data.read().clone();
            let result = crate::flow::with_host(&lifecycle_runner, &mut vu, |host| {
                script.teardown(host, setup_data)
            });
            if let Err(e) = result {
                tracing::error!(error = %e, "teardown() failed");
            }
        }

        // Stop gauge reporter, close the bus (or shard loop), finish aggregation.
        gauge_task.1.cancel();
        let _ = gauge_task.0.await;
        // Publish explicit terminal live-gauge values before closing the bus,
        // so scrape-based outputs do not retain stale non-zero values.
        bus.gauge(&self.builtins.vus, 0.0, &self.gauge_tags);
        bus.gauge(
            &requests_in_flight_metric,
            bus.requests_in_flight() as f64,
            &Arc::new(Tags::new()),
        );
        if let Some(done) = &shard_done {
            done.cancel();
        }
        drop(bus);
        let (mut aggregator, mut outputs, threshold_statuses, timeline) = agg_task
            .await
            .map_err(|e| EngineError::Other(format!("aggregator task panicked: {e}")))?;

        let summary = Summary::build(
            self.plan_name.clone(),
            self.run_id.to_string(),
            started_ms,
            scenario_names,
            &mut aggregator,
            threshold_statuses,
            aborted.clone(),
            timeline,
        );
        for output in &mut outputs {
            output.finish(&summary).await;
        }

        // handleSummary(data): let JS produce a custom end-of-run report (k6).
        let mut custom_summary = None;
        if let Some(script) = &self.script {
            if script.has_function("handleSummary") {
                match script.instantiate() {
                    Ok(mut vu_script) => {
                        // Detached bus: the summary VU's samples are discarded. Keep the
                        // receiver alive so sends don't fail, but never read from it.
                        let (summary_bus, _summary_rx) = MetricsBus::new();
                        let mut vu = VuContext::new(
                            0,
                            Arc::from("summary"),
                            lifecycle_runner.program.tags.clone(),
                            summary_bus,
                            self.run_ctx.clone(),
                            self.http_defaults.cookies,
                        );
                        let data = summary.to_json();
                        let result = crate::flow::with_host(&lifecycle_runner, &mut vu, |host| {
                            vu_script.call_function(host, "handleSummary", &[data])
                        });
                        match result {
                            Ok(serde_json::Value::String(s)) => custom_summary = Some(s),
                            Ok(serde_json::Value::Null) => {}
                            Ok(other) => {
                                custom_summary =
                                    Some(serde_json::to_string_pretty(&other).unwrap_or_default())
                            }
                            Err(e) => tracing::warn!(error = %e, "handleSummary() failed"),
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "could not run handleSummary()"),
                }
            }
        }

        let passed = summary.thresholds_passed && aborted.is_none();
        let _ = self.status_tx.send(RunStatus::Finished { passed });
        Ok(RunResult {
            passed: summary.thresholds_passed,
            summary,
            aborted,
            custom_summary,
        })
    }
}

/// Where the aggregator gets its samples from — chosen once at startup by
/// `Engine::run` based on `Output::wants_samples`. No hybrid mode.
enum SampleSource {
    /// The classic per-run channel, drained here and batched to outputs.
    Bus(mpsc::UnboundedReceiver<Sample>),
    /// Shard-local aggregators (see `MetricShards`), drained once per tick
    /// instead of read sample-by-sample. `done` ends the loop: nothing else
    /// signals completion since there's no channel to close.
    Shards {
        shards: Arc<MetricShards>,
        done: CancellationToken,
    },
}

/// The aggregator loop: drains samples (or shard deltas), snapshots once per
/// interval, feeds outputs, and evaluates thresholds continuously.
async fn aggregator_loop(
    source: SampleSource,
    mut outputs: Vec<Box<dyn Output>>,
    thresholds: Vec<CompiledThreshold>,
    snapshot_txs: (watch::Sender<Arc<Snapshot>>, watch::Sender<Arc<Snapshot>>),
    thresholds_tx: watch::Sender<Arc<Vec<ThresholdStatus>>>,
    abort_tx: mpsc::UnboundedSender<String>,
    interval: Duration,
) -> (
    Aggregator,
    Vec<Box<dyn Output>>,
    Vec<ThresholdStatus>,
    Vec<TimelinePoint>,
) {
    let (snapshots_tx, aggregates_tx) = snapshot_txs;
    let mut agg = Aggregator::new();
    let mut timeline: Vec<TimelinePoint> = Vec::new();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut abort_sent = false;
    // Only take/dispatch deltas when some output (e.g. the distributed
    // agent's uplink) actually consumes them — otherwise this is wasted
    // HDR-serialization work every tick.
    let wants_delta = outputs.iter().any(|o| o.wants_delta());

    match source {
        SampleSource::Bus(mut samples_rx) => {
            let mut batch: Vec<Sample> = Vec::with_capacity(1024);
            loop {
                tokio::select! {
                    maybe = samples_rx.recv() => {
                        match maybe {
                            Some(sample) => {
                                agg.record(&sample);
                                batch.push(sample);
                                // Opportunistically drain without yielding.
                                while batch.len() < 8192 {
                                    match samples_rx.try_recv() {
                                        Ok(s) => {
                                            agg.record(&s);
                                            batch.push(s);
                                        }
                                        Err(_) => break,
                                    }
                                }
                                if batch.len() >= 8192 {
                                    for output in &mut outputs {
                                        output.on_samples(&batch).await;
                                    }
                                    batch.clear();
                                }
                            }
                            None => break,
                        }
                    }
                    _ = ticker.tick() => {
                        if !batch.is_empty() {
                            for output in &mut outputs {
                                output.on_samples(&batch).await;
                            }
                            batch.clear();
                        }
                        tick(
                            &mut agg, &mut outputs, &thresholds, &snapshots_tx, &aggregates_tx,
                            &thresholds_tx, &abort_tx, &mut timeline, wants_delta, &mut abort_sent,
                        )
                        .await;
                    }
                }
            }
            if !batch.is_empty() {
                for output in &mut outputs {
                    output.on_samples(&batch).await;
                }
            }
        }
        SampleSource::Shards { shards, done } => {
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        shards.drain_into(&mut agg);
                        tick(
                            &mut agg, &mut outputs, &thresholds, &snapshots_tx, &aggregates_tx,
                            &thresholds_tx, &abort_tx, &mut timeline, wants_delta, &mut abort_sent,
                        )
                        .await;
                    }
                    _ = done.cancelled() => break,
                }
            }
            // Fold in anything recorded since the last tick.
            shards.drain_into(&mut agg);
        }
    }

    let last_statuses = finalize(
        &mut agg,
        &mut outputs,
        &thresholds,
        &snapshots_tx,
        &aggregates_tx,
        &thresholds_tx,
        &mut timeline,
        wants_delta,
    )
    .await;
    (agg, outputs, last_statuses, timeline)
}

/// Snapshot, dispatch to outputs, evaluate thresholds — the work every
/// snapshot tick does regardless of how samples got into `agg` (bus batches
/// or shard drains).
#[allow(clippy::too_many_arguments)]
async fn tick(
    agg: &mut Aggregator,
    outputs: &mut [Box<dyn Output>],
    thresholds: &[CompiledThreshold],
    snapshots_tx: &watch::Sender<Arc<Snapshot>>,
    aggregates_tx: &watch::Sender<Arc<Snapshot>>,
    thresholds_tx: &watch::Sender<Arc<Vec<ThresholdStatus>>>,
    abort_tx: &mpsc::UnboundedSender<String>,
    timeline: &mut Vec<TimelinePoint>,
    wants_delta: bool,
    abort_sent: &mut bool,
) {
    let aggregates = Arc::new(agg.aggregate_snapshot(&[&["scenario"]]));
    let snapshot = Arc::new(agg.snapshot());
    timeline.push(TimelinePoint::from_snapshots(&snapshot, Some(&aggregates)));
    for output in outputs.iter_mut() {
        output.on_snapshot(&snapshot).await;
    }
    let _ = snapshots_tx.send(snapshot);
    let _ = aggregates_tx.send(aggregates);
    if wants_delta {
        dispatch_delta(agg, outputs, false).await;
    }
    let (statuses, abort) = evaluate_all(thresholds, agg, agg.elapsed());
    let _ = thresholds_tx.send(Arc::new(statuses));
    if abort && !*abort_sent {
        *abort_sent = true;
        let _ = abort_tx.send("threshold crossed (abort_on_fail)".to_string());
    }
}

/// End-of-run: final threshold evaluation, final snapshot (with a trailing
/// timeline point for the residual window so short runs still produce a
/// timeline), and final delta flush.
#[allow(clippy::too_many_arguments)]
async fn finalize(
    agg: &mut Aggregator,
    outputs: &mut [Box<dyn Output>],
    thresholds: &[CompiledThreshold],
    snapshots_tx: &watch::Sender<Arc<Snapshot>>,
    aggregates_tx: &watch::Sender<Arc<Snapshot>>,
    thresholds_tx: &watch::Sender<Arc<Vec<ThresholdStatus>>>,
    timeline: &mut Vec<TimelinePoint>,
    wants_delta: bool,
) -> Vec<ThresholdStatus> {
    let (statuses, _) = evaluate_all(thresholds, agg, agg.elapsed());
    let _ = thresholds_tx.send(Arc::new(statuses.clone()));
    let final_aggregates = Arc::new(agg.aggregate_snapshot(&[&["scenario"]]));
    let final_snapshot = Arc::new(agg.snapshot());
    if final_snapshot.interval_secs > 0.0
        && final_snapshot
            .series
            .iter()
            .any(|s| s.interval_count > 0 || s.metric == "vus")
    {
        timeline.push(TimelinePoint::from_snapshots(
            &final_snapshot,
            Some(&final_aggregates),
        ));
    }
    let _ = snapshots_tx.send(final_snapshot);
    let _ = aggregates_tx.send(final_aggregates);
    if wants_delta {
        // Final flush: the run is ending, so block until it's accepted
        // (matches the pre-existing blocking final-flush behavior) rather
        // than restoring on a rejection nobody will retry.
        dispatch_delta(agg, outputs, true).await;
    }
    statuses
}

/// Drain a delta and hand it to every output that opted in via
/// `wants_delta`. If any of them rejects it (e.g. a congested channel),
/// restore it into the aggregator so the next `take_delta` retries it
/// together with anything recorded meanwhile.
/// The drained delta is shared by every `wants_delta` output and restored
/// if ANY refuses — with more than one delta consumer an accepting output
/// would see that data again next tick, so this path assumes a single
/// consumer (the agent uplink).
async fn dispatch_delta(agg: &mut Aggregator, outputs: &mut [Box<dyn Output>], last: bool) {
    let delta = agg.take_delta();
    if delta.series.is_empty() {
        return;
    }
    let mut all_accepted = true;
    for output in outputs.iter_mut() {
        if output.wants_delta() && !output.on_delta(&delta, last).await {
            all_accepted = false;
        }
    }
    if !all_accepted {
        agg.restore_delta(&delta);
    }
}

fn resolve_static_value(
    value: &serde_json::Value,
    env: &HashMap<String, String>,
) -> Result<serde_json::Value, String> {
    Ok(match value {
        serde_json::Value::String(s) => {
            let tpl = Template::parse(s).map_err(|e| e.to_string())?;
            if tpl.is_literal() {
                value.clone()
            } else {
                let rendered = tpl
                    .render(|expr| {
                        expr.strip_prefix("env.")
                            .and_then(|name| env.get(name).cloned())
                            // Non-env expressions resolve at runtime; keep as-is.
                            .or(Some(format!("${{{expr}}}")))
                    })
                    .map_err(|e| e.to_string())?;
                serde_json::Value::String(rendered)
            }
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|v| resolve_static_value(v, env))
                .collect::<Result<_, _>>()?,
        ),
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                out.insert(k.clone(), resolve_static_value(v, env)?);
            }
            serde_json::Value::Object(out)
        }
        other => other.clone(),
    })
}
