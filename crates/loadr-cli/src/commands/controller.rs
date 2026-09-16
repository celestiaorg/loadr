//! `loadr controller` — the distributed-mode control plane: coordination gRPC
//! server + management web UI + optional Prometheus endpoint.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Args;
use loadr_core::{AggValues, MetricKind, SeriesSnapshot, Snapshot, Tags};
use owo_colors::OwoColorize;

#[derive(Args)]
pub struct ControllerArgs {
    /// Coordination gRPC bind address
    #[arg(long, default_value = "127.0.0.1:7625")]
    pub bind: std::net::SocketAddr,
    /// Web UI / REST API bind address
    #[arg(long, default_value = "127.0.0.1:6464")]
    pub ui_bind: std::net::SocketAddr,
    /// Web UI basic-auth username
    #[arg(long)]
    pub ui_user: Option<String>,
    /// Web UI basic-auth password
    #[arg(long, env = "LOADR_UI_PASSWORD", hide_env_values = true)]
    pub ui_password: Option<String>,
    /// Web UI bearer token (repeatable)
    #[arg(long)]
    pub ui_token: Vec<String>,
    /// Serve a Prometheus scrape endpoint with fleet-wide metrics
    #[arg(long)]
    pub prometheus_listen: Option<String>,
    /// TLS certificate for the coordination listener (PEM)
    #[arg(long, requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    /// TLS private key (PEM)
    #[arg(long, requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    /// Require agent client certificates signed by this CA (mTLS)
    #[arg(long, requires = "tls_cert")]
    pub tls_client_ca: Option<PathBuf>,
    /// Directory for the test library and run history
    #[arg(long, default_value_os_t = default_storage_dir())]
    pub storage_dir: PathBuf,
    /// Seconds without traffic before an agent is considered lost
    #[arg(long, default_value = "6")]
    pub agent_liveness: u64,
}

fn default_storage_dir() -> PathBuf {
    dirs_home().join(".loadr").join("controller")
}

pub fn dirs_home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn execute(args: ControllerArgs) -> anyhow::Result<i32> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let tls = match (&args.tls_cert, &args.tls_key) {
            (Some(cert), Some(key)) => Some(loadr_agent::ControllerTls {
                cert_pem: cert.clone(),
                key_pem: key.clone(),
                client_ca_pem: args.tls_client_ca.clone(),
            }),
            _ => None,
        };
        let controller = loadr_agent::Controller::start(loadr_agent::ControllerConfig {
            bind: args.bind,
            tls,
            agent_liveness: std::time::Duration::from_secs(args.agent_liveness),
        })
        .await?;
        eprintln!(
            "{} controller listening on {} (agents join with: loadr agent --join {})",
            "→".cyan(),
            controller.addr(),
            controller.addr()
        );

        std::fs::create_dir_all(&args.storage_dir)?;
        let history = load_controller_history(&args.storage_dir.join("history"));
        let backend = Arc::new(ControllerBackend {
            controller: controller.clone(),
            storage_dir: args.storage_dir.clone(),
            history: parking_lot::Mutex::new(history),
        });
        // Persist terminal summaries even when no browser is polling /api/runs.
        let history_backend = backend.clone();
        let history_task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                let runs = history_backend.current_runs();
                history_backend.sync_history(&runs);
            }
        });

        let mut auth = loadr_plugin_webui::AuthConfig::default();
        if let (Some(user), Some(pass)) = (&args.ui_user, &args.ui_password) {
            auth.basic = Some((user.clone(), pass.clone()));
        }
        auth.tokens = args.ui_token.clone();
        let ui = loadr_plugin_webui::WebUi::serve(loadr_plugin_webui::WebUiConfig {
            bind: args.ui_bind,
            auth,
            backend: backend.clone(),
        })
        .await?;
        eprintln!("{} web UI at http://{}/", "→".cyan(), ui.addr);

        // Optional fleet-wide Prometheus endpoint fed from run snapshots.
        let mut prom_task = None;
        if let Some(listen) = &args.prometheus_listen {
            let mut output = loadr_outputs::PrometheusOutput::new(
                Some(listen.clone()),
                None,
                std::time::Duration::from_secs(5),
            );
            use loadr_core::Output as _;
            output
                .start()
                .await
                .map_err(|e| anyhow::anyhow!("prometheus endpoint: {e}"))?;
            if let Some(addr) = output.bound_addr() {
                eprintln!("{} prometheus metrics at http://{addr}/metrics", "→".cyan());
            }
            let controller = controller.clone();
            prom_task = Some(tokio::spawn(async move {
                let mut output = output;
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
                // Terminal runs return a cached view Arc, so an unchanged
                // pointer set means the exposition is already up to date and
                // the idle rebuild can be skipped entirely.
                let mut last_views: Vec<usize> = Vec::new();
                loop {
                    ticker.tick().await;
                    let runs = controller.runs();
                    let views: Vec<_> = select_prometheus_runs(&runs)
                        .into_iter()
                        .filter_map(|run| controller.run_metrics_view(&run.run_id))
                        .collect();
                    let ptrs: Vec<usize> = views
                        .iter()
                        .map(|view| Arc::as_ptr(view) as usize)
                        .collect();
                    if ptrs == last_views {
                        continue;
                    }
                    last_views = ptrs;
                    let snapshot = prometheus_snapshot(&views);
                    output.on_snapshot(&snapshot).await;
                }
            }));
        }

        eprintln!("  Ctrl-C to shut down");
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("\nshutting down...");
        if let Some(t) = prom_task {
            t.abort();
        }
        history_task.abort();
        ui.shutdown().await;
        controller.shutdown();
        Ok(0)
    })
}

fn select_prometheus_runs(
    runs: &[loadr_agent::RunSummaryInfo],
) -> Vec<&loadr_agent::RunSummaryInfo> {
    let is_active = |run: &loadr_agent::RunSummaryInfo| {
        matches!(run.state.as_str(), "pending" | "running" | "stopping")
    };
    let mut selected: Vec<_> = runs.iter().filter(|run| is_active(run)).collect();
    // Keep the newest completed run exported alongside active ones (`runs()`
    // is newest-first): its final counter values and terminal vus=0 must stay
    // scrapeable while the next run is still pending, not just while idle.
    // Series carry loadr_run_id, so concurrent runs don't collide.
    if let Some(completed) = runs.iter().find(|run| !is_active(run)) {
        selected.push(completed);
    }
    selected
}

fn prometheus_snapshot(views: &[Arc<loadr_agent::RunMetricsView>]) -> Snapshot {
    let mut snapshot = Snapshot {
        timestamp_ms: views
            .iter()
            .map(|view| view.detailed.timestamp_ms)
            .max()
            .unwrap_or_default(),
        elapsed_secs: views
            .iter()
            .map(|view| view.detailed.elapsed_secs)
            .fold(0.0, f64::max),
        ..Snapshot::default()
    };

    for view in views {
        let view = view.as_ref();
        let run_tags = run_tags(view);
        for series in &view.detailed.series {
            let mut series = series.clone();
            // The agent-injected `instance` tag (the agent name, kept for
            // legacy per-agent thresholds) duplicates the trusted
            // `loadr_agent` label and would collide with Prometheus' scrape
            // target label — drop only that value. A user-supplied `instance`
            // tag must survive, or series differing only in it would collapse
            // into duplicate labelsets and poison the whole exposition.
            if series.tags.get("instance") == series.tags.get("loadr_agent") {
                series.tags.remove("instance");
            }
            add_run_tags(&mut series.tags, view);
            snapshot.series.push(series);
        }
        for fleet in &view.fleet {
            snapshot.series.push(SeriesSnapshot {
                metric: format!("fleet_{}", fleet.metric),
                kind: fleet.kind,
                tags: run_tags.clone(),
                agg: fleet.agg.clone(),
                interval_count: 0,
                interval_sum: 0.0,
            });
        }
        let mut info_tags = run_tags.clone();
        info_tags.insert("state".to_string(), view.state.clone());
        snapshot
            .series
            .push(gauge_series("fleet_run_info", info_tags, 1.0));
        snapshot.series.push(gauge_series(
            "fleet_run_started_timestamp_seconds",
            run_tags,
            view.started_ms as f64 / 1000.0,
        ));
    }
    snapshot
        .series
        .sort_by(|a, b| a.metric.cmp(&b.metric).then_with(|| a.tags.cmp(&b.tags)));
    snapshot
}

fn run_tags(view: &loadr_agent::RunMetricsView) -> Tags {
    let mut tags = Tags::new();
    add_run_tags(&mut tags, view);
    tags
}

fn add_run_tags(tags: &mut Tags, view: &loadr_agent::RunMetricsView) {
    tags.insert("loadr_run_id".to_string(), view.run_id.clone());
    tags.insert(
        "loadr_run_name".to_string(),
        view.name.clone().unwrap_or_default(),
    );
}

fn gauge_series(metric: &str, tags: Tags, value: f64) -> SeriesSnapshot {
    SeriesSnapshot {
        metric: metric.to_string(),
        kind: MetricKind::Gauge,
        tags,
        agg: AggValues {
            count: 1,
            sum: value,
            min: Some(value),
            max: Some(value),
            last: Some(value),
            ..AggValues::default()
        },
        interval_count: 0,
        interval_sum: 0.0,
    }
}

/// `UiBackend` over a distributed `ControllerHandle`.
struct ControllerBackend {
    controller: loadr_agent::ControllerHandle,
    storage_dir: PathBuf,
    history: parking_lot::Mutex<HashMap<String, ControllerHistoryRecord>>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ControllerHistoryRecord {
    info: loadr_plugin_webui::RunInfo,
    summary: loadr_core::Summary,
    #[serde(default)]
    aggregate: Option<loadr_core::Snapshot>,
}

fn run_is_complete(info: &loadr_agent::RunOperationalInfo) -> bool {
    info.lost.is_empty()
        && info
            .assigned
            .iter()
            .all(|agent| info.contributing.contains(agent))
}

fn ui_run_state(state: &str, complete: bool) -> String {
    if !complete && state == "finished" {
        "degraded".to_string()
    } else {
        state.to_string()
    }
}

impl ControllerBackend {
    fn tests_dir(&self) -> PathBuf {
        self.storage_dir.join("tests")
    }

    /// Gather files a plan references, reading from the controller's disk
    /// (tests dir first, then storage dir) so agents receive them.
    fn collect_files(
        &self,
        plan: &loadr_config::TestPlan,
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        let mut wanted: Vec<String> = Vec::new();
        for source in plan.data.values() {
            if let loadr_config::DataSource::Csv { path, .. } = source {
                wanted.push(path.display().to_string());
            }
        }
        if let Some(js) = &plan.js {
            if let Some(file) = &js.file {
                wanted.push(file.display().to_string());
            }
        }
        for scenario in plan.scenarios.values() {
            collect_step_files(&scenario.flow, &mut wanted);
        }
        wanted.sort();
        wanted.dedup();
        let mut out = Vec::new();
        let mut missing = Vec::new();
        for rel in wanted {
            let rel_path = Path::new(&rel);
            if rel_path.is_absolute()
                || rel_path
                    .components()
                    .any(|component| !matches!(component, std::path::Component::Normal(_)))
            {
                return Err(format!(
                    "referenced file path must stay within controller storage: {rel}"
                ));
            }
            let mut found = false;
            for base in [self.tests_dir(), self.storage_dir.clone()] {
                let candidate = base.join(&rel);
                if let Ok(content) = std::fs::read(&candidate) {
                    out.push((rel.clone(), content));
                    found = true;
                    break;
                }
            }
            if !found {
                missing.push(rel);
            }
        }
        if missing.is_empty() {
            Ok(out)
        } else {
            Err(format!(
                "referenced file(s) not found in {} or {}: {}",
                self.tests_dir().display(),
                self.storage_dir.display(),
                missing.join(", ")
            ))
        }
    }

    fn current_runs(&self) -> Vec<loadr_plugin_webui::RunInfo> {
        self.controller
            .runs()
            .into_iter()
            .map(|run| {
                let summary = self.controller.run_summary(&run.run_id);
                let operational = self.controller.run_operational_info(&run.run_id);
                let lost_agents = operational
                    .as_ref()
                    .map(|info| info.lost.clone())
                    .unwrap_or_default();
                let complete = operational.as_ref().is_some_and(run_is_complete);
                let state = ui_run_state(&run.state, complete);
                loadr_plugin_webui::RunInfo {
                    run_id: run.run_id.clone(),
                    name: run.name,
                    state,
                    passed: summary.as_ref().and_then(|summary| {
                        if !complete {
                            Some(false)
                        } else if summary.thresholds.is_empty()
                            || summary
                                .thresholds
                                .iter()
                                .any(|threshold| threshold.observed.is_none())
                        {
                            None
                        } else {
                            Some(summary.thresholds_passed)
                        }
                    }),
                    started_ms: run.started_ms,
                    ended_ms: run.ended_ms,
                    observed_ms: loadr_core::metrics::now_millis(),
                    scenarios: operational
                        .as_ref()
                        .map(|info| info.scenarios.clone())
                        .or_else(|| summary.as_ref().map(|summary| summary.scenarios.clone()))
                        .unwrap_or_default(),
                    agents: run.agents,
                    contributing_agents: operational
                        .as_ref()
                        .map(|info| info.contributing.clone())
                        .unwrap_or_default(),
                    lost_agents,
                    complete: Some(complete),
                    on_agent_loss: operational.as_ref().map(|info| match info.on_agent_loss {
                        loadr_agent::OnAgentLoss::Continue => "continue".to_string(),
                        loadr_agent::OnAgentLoss::Abort => "abort".to_string(),
                    }),
                }
            })
            .collect()
    }

    fn sync_history(&self, runs: &[loadr_plugin_webui::RunInfo]) {
        let history_dir = self.storage_dir.join("history");
        let _ = std::fs::create_dir_all(&history_dir);
        let mut history = self.history.lock();
        for info in runs.iter().filter(|run| {
            matches!(
                run.state.as_str(),
                "finished" | "degraded" | "aborted" | "failed"
            )
        }) {
            if history.contains_key(&info.run_id) {
                continue;
            }
            let Some(summary) = self.controller.run_summary(&info.run_id) else {
                continue;
            };
            let record = ControllerHistoryRecord {
                info: info.clone(),
                summary,
                aggregate: self.controller.run_aggregate_snapshot(&info.run_id),
            };
            if persist_controller_history(&history_dir, &record).is_ok() {
                history.insert(info.run_id.clone(), record);
            }
        }
    }
}

fn load_controller_history(dir: &Path) -> HashMap<String, ControllerHistoryRecord> {
    let mut history = HashMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return history;
    };
    for entry in entries.flatten() {
        if entry
            .path()
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("json")
        {
            continue;
        }
        let Ok(bytes) = std::fs::read(entry.path()) else {
            continue;
        };
        let Ok(record) = serde_json::from_slice::<ControllerHistoryRecord>(&bytes) else {
            continue;
        };
        history.insert(record.info.run_id.clone(), record);
    }
    history
}

fn persist_controller_history(dir: &Path, record: &ControllerHistoryRecord) -> Result<(), String> {
    let path = dir.join(format!("{}.json", record.info.run_id));
    let temp = dir.join(format!("{}.json.tmp", record.info.run_id));
    let bytes = serde_json::to_vec_pretty(record).map_err(|error| error.to_string())?;
    std::fs::write(&temp, bytes).map_err(|error| error.to_string())?;
    std::fs::rename(&temp, &path).map_err(|error| error.to_string())
}

fn collect_step_files(steps: &[loadr_config::Step], wanted: &mut Vec<String>) {
    for step in steps {
        match step {
            loadr_config::Step::Request(r) => {
                if let Some(loadr_config::Body::Spec(spec)) = &r.body {
                    if let Some(f) = &spec.file {
                        wanted.push(f.display().to_string());
                    }
                }
                if let Some(grpc) = &r.grpc {
                    for f in &grpc.proto_files {
                        wanted.push(f.display().to_string());
                    }
                }
            }
            loadr_config::Step::Group(g) => collect_step_files(&g.steps, wanted),
            _ => {}
        }
    }
}

#[async_trait::async_trait]
impl loadr_plugin_webui::UiBackend for ControllerBackend {
    async fn start_test(
        &self,
        name: Option<String>,
        yaml: String,
        env: Option<String>,
    ) -> Result<String, String> {
        let loaded = loadr_config::load_str(
            &yaml,
            &loadr_config::LoadOptions {
                env: env.clone(),
                check_files: false,
                deny_errors: true,
            },
        )
        .map_err(|e| e.to_string())?;
        let files = self.collect_files(&loaded.plan)?;
        self.controller
            .submit(
                yaml,
                loadr_agent::SubmitOptions {
                    env,
                    name,
                    files,
                    agent_filter: None,
                    on_agent_loss: Default::default(),
                    start_barrier: std::time::Duration::from_secs(2),
                    preparation_timeout: std::time::Duration::from_secs(120),
                },
            )
            .await
            .map_err(|e| e.to_string())
    }

    fn validate_references(&self, yaml: &str, env: Option<&str>) -> Result<(), String> {
        let loaded = loadr_config::load_str(
            yaml,
            &loadr_config::LoadOptions {
                env: env.map(str::to_string),
                check_files: false,
                deny_errors: true,
            },
        )
        .map_err(|error| error.to_string())?;
        self.collect_files(&loaded.plan).map(|_| ())
    }

    fn runs(&self) -> Vec<loadr_plugin_webui::RunInfo> {
        let mut runs = self.current_runs();
        self.sync_history(&runs);
        let current: HashSet<String> = runs.iter().map(|run| run.run_id.clone()).collect();
        runs.extend(
            self.history
                .lock()
                .values()
                .filter(|record| !current.contains(&record.info.run_id))
                .map(|record| record.info.clone()),
        );
        runs.sort_by(|a, b| {
            b.started_ms
                .cmp(&a.started_ms)
                .then_with(|| a.run_id.cmp(&b.run_id))
        });
        runs
    }

    fn run_handle(&self, _run_id: &str) -> Option<loadr_core::RunHandle> {
        None // distributed runs stream via backend polling
    }

    fn run_snapshot(&self, run_id: &str) -> Option<Arc<loadr_core::Snapshot>> {
        self.controller
            .watch_run(run_id)
            .map(|rx| rx.borrow().clone())
            .or_else(|| {
                self.controller
                    .run_summary(run_id)
                    .map(|s| Arc::new(s.snapshot))
            })
            .or_else(|| {
                self.history
                    .lock()
                    .get(run_id)
                    .map(|record| Arc::new(record.summary.snapshot.clone()))
            })
    }

    fn run_aggregate_snapshot(&self, run_id: &str) -> Option<Arc<loadr_core::Snapshot>> {
        self.controller
            .run_aggregate_snapshot(run_id)
            .map(Arc::new)
            .or_else(|| {
                self.history
                    .lock()
                    .get(run_id)
                    .and_then(|record| record.aggregate.clone())
                    .map(Arc::new)
            })
    }

    fn run_thresholds(&self, run_id: &str) -> Vec<loadr_core::ThresholdStatus> {
        let current = self.controller.run_thresholds(run_id);
        if current.is_empty() {
            self.history
                .lock()
                .get(run_id)
                .map(|record| record.summary.thresholds.clone())
                .unwrap_or_default()
        } else {
            current
        }
    }

    fn run_summary(&self, run_id: &str) -> Option<loadr_core::Summary> {
        self.controller.run_summary(run_id).or_else(|| {
            self.history
                .lock()
                .get(run_id)
                .map(|record| record.summary.clone())
        })
    }

    fn run_control_state(&self, run_id: &str) -> loadr_plugin_webui::RunControlView {
        self.controller
            .run_operational_info(run_id)
            .map(|info| loadr_plugin_webui::RunControlView {
                externally_controlled: info.externally_controlled,
                is_paused: info.paused,
                agent_confirmed: true,
            })
            .unwrap_or_default()
    }

    async fn stop_run(&self, run_id: &str, kill: bool) -> Result<(), String> {
        if kill {
            self.controller
                .kill_run(run_id)
                .await
                .map_err(|e| e.to_string())
        } else {
            self.controller
                .stop_run(run_id)
                .await
                .map_err(|e| e.to_string())
        }
    }

    async fn pause_run(&self, run_id: &str, paused: bool) -> Result<(), String> {
        self.controller
            .pause_run(run_id, paused)
            .await
            .map_err(|e| e.to_string())
    }

    async fn scale_run(&self, run_id: &str, scenario: &str, vus: u64) -> Result<(), String> {
        self.controller
            .scale(run_id, scenario, vus)
            .await
            .map_err(|e| e.to_string())
    }

    fn agents(&self) -> Vec<loadr_plugin_webui::AgentView> {
        self.controller
            .agents()
            .into_iter()
            .map(|a| loadr_plugin_webui::AgentView {
                // The controller's value is already a monotonic age. Preserve
                // it so the UI does not depend on the browser's wall clock.
                last_heartbeat_age_ms: a.last_heartbeat_ms,
                id: a.id,
                name: a.name,
                healthy: a.healthy,
                active_vus: a.active_vus,
                cores: a.cores,
                peer_addr: a.peer_addr.map(|addr| addr.to_string()),
                version: a.version,
                revision: a.revision,
                // The controller reports `last_heartbeat_ms` as a delta (ms since
                // the last beat); the UI expects an absolute epoch timestamp.
                last_heartbeat_ms: loadr_core::metrics::now_millis()
                    .saturating_sub(a.last_heartbeat_ms),
                labels: a.labels.into_iter().collect(),
            })
            .collect()
    }

    fn tests(&self) -> Vec<loadr_plugin_webui::StoredTest> {
        let dir = self.tests_dir();
        let mut out = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("yaml") {
                    if let Ok(yaml) = std::fs::read_to_string(&path) {
                        let updated_ms = entry
                            .metadata()
                            .and_then(|m| m.modified())
                            .ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        out.push(loadr_plugin_webui::StoredTest {
                            name: path
                                .file_stem()
                                .map(|s| s.to_string_lossy().to_string())
                                .unwrap_or_default(),
                            yaml,
                            updated_ms,
                        });
                    }
                }
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn save_test(&self, name: String, yaml: String) -> Result<(), String> {
        if name.is_empty() || name.contains(['/', '\\']) || name.contains("..") {
            return Err("invalid test name".into());
        }
        let dir = self.tests_dir();
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        std::fs::write(dir.join(format!("{name}.yaml")), yaml).map_err(|e| e.to_string())
    }

    fn delete_test(&self, name: &str) -> Result<(), String> {
        if name.contains(['/', '\\']) || name.contains("..") {
            return Err("invalid test name".into());
        }
        std::fs::remove_file(self.tests_dir().join(format!("{name}.yaml")))
            .map_err(|e| e.to_string())
    }

    fn recent_logs(&self) -> Vec<loadr_plugin_webui::LogLine> {
        Vec::new()
    }

    fn capabilities(&self) -> loadr_plugin_webui::UiCapabilities {
        loadr_plugin_webui::UiCapabilities {
            mode: "distributed".to_string(),
            can_start_runs: true,
            can_edit_tests: true,
            logs_available: false,
            persistent_history: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Small HTTP client helpers shared with `loadr run --controller`.
// ---------------------------------------------------------------------------

pub type HttpClient = hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    http_body_util::Full<bytes::Bytes>,
>;

pub fn http_client() -> HttpClient {
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build_http()
}

pub async fn http_json(
    client: &HttpClient,
    method: http::Method,
    url: &str,
    body: Option<&serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let mut builder = http::Request::builder().method(method).uri(url);
    if body.is_some() {
        builder = builder.header(http::header::CONTENT_TYPE, "application/json");
    }
    let request = builder.body(http_body_util::Full::new(bytes::Bytes::from(
        body.map(serde_json::to_vec)
            .transpose()?
            .unwrap_or_default(),
    )))?;
    let response = client.request(request).await?;
    let status = response.status();
    use http_body_util::BodyExt as _;
    let bytes = response.into_body().collect().await?.to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| serde_json::json!({"raw": String::from_utf8_lossy(&bytes)}));
    if !status.is_success() {
        anyhow::bail!("{url} returned {status}: {value}");
    }
    Ok(value)
}

#[cfg(test)]
mod prometheus_tests {
    use super::*;

    fn counter_series(tags: &[(&str, &str)], sum: f64) -> SeriesSnapshot {
        SeriesSnapshot {
            metric: "http_reqs".into(),
            kind: MetricKind::Counter,
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            agg: AggValues {
                count: sum as u64,
                sum,
                ..AggValues::default()
            },
            interval_count: 0,
            interval_sum: 0.0,
        }
    }

    fn view_with(detailed: Snapshot) -> Arc<loadr_agent::RunMetricsView> {
        Arc::new(loadr_agent::RunMetricsView {
            run_id: "run-1".into(),
            name: Some("checkout".into()),
            state: "running".into(),
            started_ms: 1_700_000_000_000,
            detailed,
            fleet: vec![loadr_agent::FleetMetric {
                metric: "http_reqs".into(),
                kind: MetricKind::Counter,
                agg: AggValues {
                    count: 12,
                    sum: 12.0,
                    ..AggValues::default()
                },
            }],
        })
    }

    #[test]
    fn projection_separates_detailed_and_fleet_series() {
        // `instance` here is the agent-injected value (== agent name).
        let detailed = Snapshot {
            timestamp_ms: 1_700_000_000_000,
            elapsed_secs: 12.0,
            interval_secs: 0.0,
            series: vec![counter_series(
                &[
                    ("instance", "worker-a"),
                    ("loadr_agent", "worker-a"),
                    ("loadr_agent_id", "agent-a-id"),
                ],
                7.0,
            )],
        };
        let views = vec![view_with(detailed)];

        let projected = prometheus_snapshot(&views);
        let detailed = projected
            .series
            .iter()
            .find(|series| series.metric == "http_reqs")
            .expect("detailed series");
        assert!(!detailed.tags.contains_key("instance"));
        assert_eq!(detailed.tags["loadr_agent"], "worker-a");
        assert_eq!(detailed.tags["loadr_run_id"], "run-1");

        let fleet = projected
            .series
            .iter()
            .find(|series| series.metric == "fleet_http_reqs")
            .expect("fleet series");
        assert_eq!(fleet.agg.sum, 12.0);
        assert!(!fleet.tags.contains_key("loadr_agent"));
        assert_eq!(fleet.tags["loadr_run_name"], "checkout");

        let info = projected
            .series
            .iter()
            .find(|series| series.metric == "fleet_run_info")
            .expect("run info");
        assert_eq!(info.tags["state"], "running");
        assert_eq!(info.agg.last, Some(1.0));
    }

    #[test]
    fn user_instance_tags_survive_and_stay_distinct() {
        // Two series that differ only in a user-supplied `instance` tag must
        // not collapse into duplicate labelsets when the agent-injected value
        // is stripped.
        let detailed = Snapshot {
            timestamp_ms: 1_700_000_000_000,
            elapsed_secs: 12.0,
            interval_secs: 0.0,
            series: vec![
                counter_series(
                    &[
                        ("instance", "api-1"),
                        ("loadr_agent", "worker-a"),
                        ("loadr_agent_id", "agent-a-id"),
                    ],
                    3.0,
                ),
                counter_series(
                    &[
                        ("instance", "api-2"),
                        ("loadr_agent", "worker-a"),
                        ("loadr_agent_id", "agent-a-id"),
                    ],
                    4.0,
                ),
            ],
        };
        let views = vec![view_with(detailed)];

        let projected = prometheus_snapshot(&views);
        let reqs: Vec<_> = projected
            .series
            .iter()
            .filter(|series| series.metric == "http_reqs")
            .collect();
        assert_eq!(reqs.len(), 2);
        let instances: Vec<&str> = reqs
            .iter()
            .filter_map(|series| series.tags.get("instance"))
            .map(String::as_str)
            .collect();
        assert_eq!(instances, ["api-1", "api-2"]);
        assert_ne!(reqs[0].tags, reqs[1].tags, "labelsets must stay distinct");
    }

    #[test]
    fn selects_active_runs_plus_newest_completed() {
        let run = |id: &str, state: &str, started_ms: u64| loadr_agent::RunSummaryInfo {
            run_id: id.into(),
            name: None,
            state: state.into(),
            started_ms,
            ended_ms: None,
            agents: Vec::new(),
        };
        let runs = vec![
            run("stopping", "stopping", 4),
            run("active-b", "pending", 3),
            run("active-a", "running", 2),
            run("old", "finished", 1),
        ];
        let selected: Vec<_> = select_prometheus_runs(&runs)
            .into_iter()
            .map(|run| run.run_id.as_str())
            .collect();
        // The newest completed run stays exported while others are active so
        // its final counters remain scrapeable.
        assert_eq!(selected, ["stopping", "active-b", "active-a", "old"]);

        let completed = vec![run("new", "failed", 2), run("old", "finished", 1)];
        let selected = select_prometheus_runs(&completed);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].run_id, "new");

        let active_only = vec![run("solo", "running", 1)];
        let selected = select_prometheus_runs(&active_only);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].run_id, "solo");
    }

    #[test]
    fn controller_history_round_trips() {
        let temp = tempfile::tempdir().expect("temporary history directory");
        let info = loadr_plugin_webui::RunInfo {
            run_id: "run-history".into(),
            name: Some("checkout".into()),
            state: "degraded".into(),
            passed: Some(false),
            started_ms: 10,
            ended_ms: Some(20),
            observed_ms: 20,
            scenarios: vec!["browse".into()],
            agents: vec!["agent-a".into(), "agent-b".into()],
            contributing_agents: vec!["agent-a".into()],
            lost_agents: vec!["agent-b".into()],
            complete: Some(false),
            on_agent_loss: Some("continue".into()),
        };
        let summary = loadr_core::Summary {
            name: info.name.clone(),
            run_id: info.run_id.clone(),
            started_ms: 10,
            ended_ms: 20,
            duration_secs: 0.01,
            scenarios: info.scenarios.clone(),
            metrics: Vec::new(),
            checks: Vec::new(),
            thresholds: Vec::new(),
            thresholds_passed: true,
            aborted: None,
            snapshot: Snapshot::default(),
            timeline: Vec::new(),
        };
        let record = ControllerHistoryRecord {
            info,
            summary,
            aggregate: Some(Snapshot::default()),
        };

        persist_controller_history(temp.path(), &record).expect("persist history");
        let loaded = load_controller_history(temp.path());
        let loaded = loaded.get("run-history").expect("loaded history");
        assert_eq!(loaded.info.state, "degraded");
        assert_eq!(loaded.info.complete, Some(false));
        assert_eq!(loaded.info.lost_agents, ["agent-b"]);
        assert_eq!(loaded.summary.run_id, "run-history");
        assert!(loaded.aggregate.is_some());
    }

    #[test]
    fn fleet_completeness_requires_every_agent_and_preserves_stronger_states() {
        let mut info = loadr_agent::RunOperationalInfo {
            scenarios: Vec::new(),
            externally_controlled: Vec::new(),
            assigned: vec!["agent-a".into(), "agent-b".into()],
            contributing: vec!["agent-a".into()],
            completed: Vec::new(),
            lost: Vec::new(),
            on_agent_loss: loadr_agent::OnAgentLoss::Continue,
            paused: Some(false),
        };
        assert!(!run_is_complete(&info));
        assert_eq!(ui_run_state("finished", false), "degraded");
        assert_eq!(ui_run_state("failed", false), "failed");
        assert_eq!(ui_run_state("aborted", false), "aborted");

        info.contributing.push("agent-b".into());
        assert!(run_is_complete(&info));
        info.lost.push("agent-b".into());
        assert!(!run_is_complete(&info));
    }
}
