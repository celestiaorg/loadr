//! [`LocalBackend`]: the in-process reference implementation of
//! [`UiBackend`](crate::UiBackend) used by the CLI in standalone mode.
//!
//! It keeps a run registry, a yaml-file test library, an in-memory log ring
//! buffer (fed by [`WebUiLogLayer`], a `tracing-subscriber` layer), and
//! persists finished-run summaries as JSON files under `history/` so restarts
//! keep history. Runs are launched through an injected [`EngineLauncher`]
//! closure so this crate never decides which protocols or script engines are
//! wired in.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use loadr_config::{ConfigError, LoadOptions, TestPlan};
use loadr_core::engine::RunStatus;
use loadr_core::{EngineError, RunHandle, RunResult, Snapshot, Summary, ThresholdStatus};
use parking_lot::{Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::{now_ms, AgentView, LogLine, RunInfo, StoredTest, UiBackend};

/// What an [`EngineLauncher`] returns: the live handle plus the run task.
pub type LauncherResult = Result<(RunHandle, JoinHandle<Result<RunResult, EngineError>>), String>;

/// Builds and spawns an engine for a validated plan.
///
/// Injected at construction so the embedder controls protocols, script
/// engines and outputs: the CLI passes a closure that wires the real HTTP/WS/
/// gRPC handlers and the JS runtime; tests pass one with mocks.
pub type EngineLauncher = Arc<dyn Fn(TestPlan, PathBuf, String) -> LauncherResult + Send + Sync>;

const LOG_CAPACITY: usize = 1000;
const TEST_EXTENSIONS: [&str; 2] = ["yaml", "yml"];

type LogRing = Arc<Mutex<VecDeque<LogLine>>>;

struct RunEntry {
    info: RunInfo,
    handle: Option<RunHandle>,
    thresholds: Vec<ThresholdStatus>,
    last_snapshot: Option<Arc<Snapshot>>,
    last_aggregate: Option<Arc<Snapshot>>,
    summary: Option<Summary>,
}

/// Persisted per-run history record (one JSON file per finished run).
#[derive(serde::Serialize, serde::Deserialize)]
struct HistoryRecord {
    info: RunInfo,
    summary: Option<Summary>,
    #[serde(default)]
    aggregate: Option<Snapshot>,
}

/// In-process [`UiBackend`] for standalone mode.
pub struct LocalBackend {
    storage_dir: PathBuf,
    launcher: EngineLauncher,
    runs: Arc<RwLock<HashMap<String, RunEntry>>>,
    logs: LogRing,
}

impl LocalBackend {
    /// Create a backend rooted at `storage_dir` (tests live in `tests/`,
    /// finished-run summaries in `history/`). Existing history is loaded.
    pub fn new(storage_dir: PathBuf, launcher: EngineLauncher) -> Result<Self, String> {
        let tests_dir = storage_dir.join("tests");
        let history_dir = storage_dir.join("history");
        std::fs::create_dir_all(&tests_dir)
            .map_err(|e| format!("cannot create {}: {e}", tests_dir.display()))?;
        std::fs::create_dir_all(&history_dir)
            .map_err(|e| format!("cannot create {}: {e}", history_dir.display()))?;

        let mut runs = HashMap::new();
        for record in load_history(&history_dir) {
            let last_snapshot = record
                .summary
                .as_ref()
                .map(|s| Arc::new(s.snapshot.clone()));
            let thresholds = record
                .summary
                .as_ref()
                .map(|s| s.thresholds.clone())
                .unwrap_or_default();
            runs.insert(
                record.info.run_id.clone(),
                RunEntry {
                    info: record.info,
                    handle: None,
                    thresholds,
                    last_snapshot,
                    last_aggregate: record.aggregate.map(Arc::new),
                    summary: record.summary,
                },
            );
        }

        Ok(LocalBackend {
            storage_dir,
            launcher,
            runs: Arc::new(RwLock::new(runs)),
            logs: Arc::new(Mutex::new(VecDeque::with_capacity(LOG_CAPACITY))),
        })
    }

    /// Push one line into the log ring buffer (shown on the Logs page).
    pub fn push_log(&self, level: impl Into<String>, message: impl Into<String>) {
        push_log_line(
            &self.logs,
            LogLine {
                ts_ms: now_ms(),
                level: level.into(),
                message: message.into(),
            },
        );
    }

    /// A `tracing-subscriber` layer that feeds this backend's log buffer.
    ///
    /// Attach it to the embedder's subscriber so backend logs show up on the
    /// Logs page: `tracing_subscriber::registry().with(backend.log_layer())`.
    pub fn log_layer(&self) -> WebUiLogLayer {
        WebUiLogLayer {
            ring: self.logs.clone(),
        }
    }

    fn tests_dir(&self) -> PathBuf {
        self.storage_dir.join("tests")
    }

    fn history_dir(&self) -> PathBuf {
        self.storage_dir.join("history")
    }
}

#[async_trait::async_trait]
impl UiBackend for LocalBackend {
    async fn start_test(
        &self,
        name: Option<String>,
        yaml: String,
        env: Option<String>,
    ) -> Result<String, String> {
        let opts = LoadOptions {
            env,
            check_files: false,
            deny_errors: true,
        };
        let loaded = loadr_config::load_str(&yaml, &opts).map_err(config_error_to_string)?;
        let run_id = uuid::Uuid::new_v4().to_string();
        let scenarios: Vec<String> = loaded.plan.scenarios.keys().cloned().collect();
        let display_name = name.or_else(|| loaded.plan.name.clone());

        let (handle, task) =
            (self.launcher)(loaded.plan, self.storage_dir.clone(), run_id.clone())?;

        let info = RunInfo {
            run_id: run_id.clone(),
            name: display_name,
            state: "running".to_string(),
            passed: None,
            started_ms: now_ms(),
            ended_ms: None,
            observed_ms: now_ms(),
            scenarios,
            agents: Vec::new(),
            contributing_agents: Vec::new(),
            lost_agents: Vec::new(),
            complete: None,
            on_agent_loss: None,
        };
        self.runs.write().insert(
            run_id.clone(),
            RunEntry {
                info,
                handle: Some(handle),
                thresholds: Vec::new(),
                last_snapshot: None,
                last_aggregate: None,
                summary: None,
            },
        );
        self.push_log("info", format!("run {run_id} started"));

        // Watcher: capture the final summary, persist history.
        let runs = self.runs.clone();
        let logs = self.logs.clone();
        let history_dir = self.history_dir();
        let id = run_id.clone();
        tokio::spawn(async move {
            let outcome = task.await;
            let record = {
                let mut map = runs.write();
                let Some(entry) = map.get_mut(&id) else {
                    return;
                };
                entry.last_aggregate = entry
                    .handle
                    .as_ref()
                    .map(|handle| handle.aggregate_snapshot());
                entry.handle = None;
                entry.info.ended_ms = Some(now_ms());
                match outcome {
                    Ok(Ok(result)) => {
                        let passed = result.passed && result.aborted.is_none();
                        entry.info.state = if result.aborted.is_some() {
                            "aborted".to_string()
                        } else {
                            "finished".to_string()
                        };
                        entry.info.passed = Some(passed);
                        entry.info.ended_ms = Some(result.summary.ended_ms);
                        entry.thresholds = result.summary.thresholds.clone();
                        entry.last_snapshot = Some(Arc::new(result.summary.snapshot.clone()));
                        entry.summary = Some(result.summary);
                        let outcome = if passed {
                            "passed"
                        } else {
                            "failed thresholds"
                        };
                        push_log_line(
                            &logs,
                            LogLine {
                                ts_ms: now_ms(),
                                level: "info".to_string(),
                                message: format!("run {id} finished: {outcome}"),
                            },
                        );
                    }
                    Ok(Err(e)) => {
                        entry.info.state = "failed".to_string();
                        entry.info.passed = Some(false);
                        push_log_line(
                            &logs,
                            LogLine {
                                ts_ms: now_ms(),
                                level: "error".to_string(),
                                message: format!("run {id} failed: {e}"),
                            },
                        );
                    }
                    Err(e) => {
                        entry.info.state = "failed".to_string();
                        entry.info.passed = Some(false);
                        push_log_line(
                            &logs,
                            LogLine {
                                ts_ms: now_ms(),
                                level: "error".to_string(),
                                message: format!("run {id} task panicked: {e}"),
                            },
                        );
                    }
                }
                HistoryRecord {
                    info: entry.info.clone(),
                    summary: entry.summary.clone(),
                    aggregate: entry.last_aggregate.as_deref().cloned(),
                }
            };
            if let Err(e) = persist_history(&history_dir, &record) {
                push_log_line(
                    &logs,
                    LogLine {
                        ts_ms: now_ms(),
                        level: "warn".to_string(),
                        message: format!("could not persist history for run {id}: {e}"),
                    },
                );
            }
        });

        Ok(run_id)
    }

    fn runs(&self) -> Vec<RunInfo> {
        let map = self.runs.read();
        let mut out: Vec<RunInfo> = map
            .values()
            .map(|entry| {
                let mut info = entry.info.clone();
                if let Some(handle) = &entry.handle {
                    let status = handle.status();
                    info.state = status_string(&status).to_string();
                    if let RunStatus::Finished { passed } = status {
                        info.passed = Some(passed);
                    }
                }
                let thresholds = match &entry.handle {
                    Some(handle) => handle.threshold_statuses().as_ref().clone(),
                    None => entry.thresholds.clone(),
                };
                if info.state == "finished"
                    && (thresholds.is_empty()
                        || thresholds
                            .iter()
                            .any(|threshold| threshold.observed.is_none()))
                {
                    info.passed = None;
                }
                info.observed_ms = now_ms();
                info
            })
            .collect();
        out.sort_by(|a, b| {
            b.started_ms
                .cmp(&a.started_ms)
                .then_with(|| a.run_id.cmp(&b.run_id))
        });
        out
    }

    fn run_handle(&self, run_id: &str) -> Option<RunHandle> {
        self.runs.read().get(run_id).and_then(|e| e.handle.clone())
    }

    fn run_snapshot(&self, run_id: &str) -> Option<Arc<Snapshot>> {
        let map = self.runs.read();
        let entry = map.get(run_id)?;
        match &entry.handle {
            Some(h) => Some(h.snapshot()),
            None => entry.last_snapshot.clone(),
        }
    }

    fn run_aggregate_snapshot(&self, run_id: &str) -> Option<Arc<Snapshot>> {
        let map = self.runs.read();
        let entry = map.get(run_id)?;
        match &entry.handle {
            Some(handle) => Some(handle.aggregate_snapshot()),
            None => entry.last_aggregate.clone(),
        }
    }

    fn run_thresholds(&self, run_id: &str) -> Vec<ThresholdStatus> {
        let map = self.runs.read();
        let Some(entry) = map.get(run_id) else {
            return Vec::new();
        };
        match &entry.handle {
            Some(h) => h.threshold_statuses().as_ref().clone(),
            None => entry.thresholds.clone(),
        }
    }

    fn run_summary(&self, run_id: &str) -> Option<Summary> {
        self.runs.read().get(run_id).and_then(|e| e.summary.clone())
    }

    async fn stop_run(&self, run_id: &str, kill: bool) -> Result<(), String> {
        let handle = self
            .run_handle(run_id)
            .ok_or_else(|| format!("run `{run_id}` is not live"))?;
        if kill {
            handle.kill("killed via web UI");
            self.push_log("warn", format!("run {run_id} killed via web UI"));
        } else {
            handle.stop("stopped via web UI");
            self.push_log("info", format!("run {run_id} stop requested via web UI"));
        }
        Ok(())
    }

    async fn pause_run(&self, run_id: &str, paused: bool) -> Result<(), String> {
        let handle = self
            .run_handle(run_id)
            .ok_or_else(|| format!("run `{run_id}` is not live"))?;
        handle.pause(paused);
        let verb = if paused { "paused" } else { "resumed" };
        self.push_log("info", format!("run {run_id} {verb} via web UI"));
        Ok(())
    }

    async fn scale_run(&self, run_id: &str, scenario: &str, vus: u64) -> Result<(), String> {
        let handle = self
            .run_handle(run_id)
            .ok_or_else(|| format!("run `{run_id}` is not live"))?;
        handle.scale(scenario, vus)?;
        self.push_log(
            "info",
            format!("run {run_id}: scenario `{scenario}` scaled to {vus} VUs"),
        );
        Ok(())
    }

    fn agents(&self) -> Vec<AgentView> {
        Vec::new()
    }

    fn tests(&self) -> Vec<StoredTest> {
        let dir = self.tests_dir();
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            let is_yaml = path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| TEST_EXTENSIONS.contains(&e))
                .unwrap_or(false);
            if !is_yaml {
                continue;
            }
            let Some(name) = path.file_stem().map(|s| s.to_string_lossy().to_string()) else {
                continue;
            };
            let Ok(yaml) = std::fs::read_to_string(&path) else {
                continue;
            };
            let updated_ms = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            out.push(StoredTest {
                name,
                yaml,
                updated_ms,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn save_test(&self, name: String, yaml: String) -> Result<(), String> {
        validate_test_name(&name)?;
        let path = self.tests_dir().join(format!("{name}.yaml"));
        std::fs::write(&path, yaml).map_err(|e| format!("cannot save test `{name}`: {e}"))?;
        self.push_log("info", format!("test `{name}` saved"));
        Ok(())
    }

    fn delete_test(&self, name: &str) -> Result<(), String> {
        validate_test_name(name)?;
        let mut deleted = false;
        for ext in TEST_EXTENSIONS {
            let path = self.tests_dir().join(format!("{name}.{ext}"));
            if path.is_file() {
                std::fs::remove_file(&path)
                    .map_err(|e| format!("cannot delete test `{name}`: {e}"))?;
                deleted = true;
            }
        }
        if deleted {
            self.push_log("info", format!("test `{name}` deleted"));
            Ok(())
        } else {
            Err(format!("test `{name}` does not exist"))
        }
    }

    fn recent_logs(&self) -> Vec<LogLine> {
        self.logs.lock().iter().cloned().collect()
    }
}

pub(crate) fn status_string(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Pending => "pending",
        RunStatus::Running => "running",
        RunStatus::Stopping => "stopping",
        RunStatus::Finished { .. } => "finished",
    }
}

fn config_error_to_string(err: ConfigError) -> String {
    match err {
        ConfigError::Invalid(diags) => diags
            .iter()
            .map(|d| d.to_string())
            .collect::<Vec<_>>()
            .join("; "),
        other => other.to_string(),
    }
}

fn push_log_line(ring: &LogRing, line: LogLine) {
    let mut ring = ring.lock();
    if ring.len() >= LOG_CAPACITY {
        ring.pop_front();
    }
    ring.push_back(line);
}

fn load_history(dir: &Path) -> Vec<HistoryRecord> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        if let Ok(record) = serde_json::from_str::<HistoryRecord>(&text) {
            out.push(record);
        }
    }
    out
}

fn persist_history(dir: &Path, record: &HistoryRecord) -> Result<(), String> {
    let path = dir.join(format!("{}.json", record.info.run_id));
    let temp = dir.join(format!("{}.json.tmp", record.info.run_id));
    let json = serde_json::to_string(record).map_err(|e| e.to_string())?;
    std::fs::write(&temp, json).map_err(|e| e.to_string())?;
    std::fs::rename(&temp, &path).map_err(|e| e.to_string())
}

fn validate_test_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 128 {
        return Err("test name must be between 1 and 128 characters".to_string());
    }
    if name.starts_with('.') {
        return Err("test name cannot start with a dot".to_string());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '.' | '_' | '-'))
    {
        return Err(
            "test name may only contain letters, digits, spaces, dots, underscores and dashes"
                .to_string(),
        );
    }
    Ok(())
}

/// A `tracing-subscriber` [`Layer`](tracing_subscriber::Layer) that copies
/// every event into a [`LocalBackend`] log ring buffer for the Logs page.
#[derive(Clone)]
pub struct WebUiLogLayer {
    ring: LogRing,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WebUiLogLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let meta = event.metadata();
        let mut message = visitor.message.unwrap_or_default();
        if !visitor.extra.is_empty() {
            if !message.is_empty() {
                message.push(' ');
            }
            message.push_str(&visitor.extra.join(" "));
        }
        push_log_line(
            &self.ring,
            LogLine {
                ts_ms: now_ms(),
                level: meta.level().to_string().to_lowercase(),
                message: format!("{}: {message}", meta.target()),
            },
        );
    }
}

#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    extra: Vec<String>,
}

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        } else {
            self.extra.push(format!("{}={:?}", field.name(), value));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.extra.push(format!("{}={value}", field.name()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noop_launcher() -> EngineLauncher {
        Arc::new(|_plan, _dir, _id| Err("not used".to_string()))
    }

    #[test]
    fn test_name_validation() {
        assert!(validate_test_name("smoke-test_1.v2").is_ok());
        assert!(validate_test_name("").is_err());
        assert!(validate_test_name(".hidden").is_err());
        assert!(validate_test_name("../escape").is_err());
        assert!(validate_test_name("a/b").is_err());
    }

    #[tokio::test]
    async fn tests_crud_and_logs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend =
            LocalBackend::new(dir.path().to_path_buf(), noop_launcher()).expect("backend");
        backend
            .save_test("demo".to_string(), "name: demo\n".to_string())
            .expect("save");
        let tests = backend.tests();
        assert_eq!(tests.len(), 1);
        assert_eq!(tests[0].name, "demo");
        assert!(tests[0].yaml.contains("name: demo"));
        backend.delete_test("demo").expect("delete");
        assert!(backend.tests().is_empty());
        assert!(backend.delete_test("demo").is_err());

        backend.push_log("info", "hello");
        let logs = backend.recent_logs();
        assert!(logs.iter().any(|l| l.message.contains("hello")));
    }

    #[test]
    fn log_layer_captures_events() {
        use tracing_subscriber::layer::SubscriberExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let backend =
            LocalBackend::new(dir.path().to_path_buf(), noop_launcher()).expect("backend");
        let subscriber = tracing_subscriber::registry().with(backend.log_layer());
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(code = 7, "something happened");
        });
        let logs = backend.recent_logs();
        let line = logs.last().expect("one log line");
        assert_eq!(line.level, "warn");
        assert!(line.message.contains("something happened"));
        assert!(line.message.contains("code=7"));
    }

    #[test]
    fn exact_aggregate_survives_history_reload() {
        let dir = tempfile::tempdir().expect("tempdir");
        let history = dir.path().join("history");
        std::fs::create_dir_all(&history).expect("history directory");
        let record = HistoryRecord {
            info: RunInfo {
                run_id: "persisted".into(),
                name: None,
                state: "finished".into(),
                passed: None,
                started_ms: 1,
                ended_ms: Some(2),
                observed_ms: 2,
                scenarios: Vec::new(),
                agents: Vec::new(),
                contributing_agents: Vec::new(),
                lost_agents: Vec::new(),
                complete: None,
                on_agent_loss: None,
            },
            summary: None,
            aggregate: Some(Snapshot {
                timestamp_ms: 42,
                ..Snapshot::default()
            }),
        };
        persist_history(&history, &record).expect("persist history");

        let backend =
            LocalBackend::new(dir.path().to_path_buf(), noop_launcher()).expect("reload backend");
        let aggregate = backend
            .run_aggregate_snapshot("persisted")
            .expect("persisted exact aggregate");
        assert_eq!(aggregate.timestamp_ms, 42);
    }
}
