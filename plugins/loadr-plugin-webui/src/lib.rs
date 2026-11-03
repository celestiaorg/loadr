#![recursion_limit = "256"]

//! # loadr-plugin-webui
//!
//! The embedded management web UI for [loadr](https://loadr.io) — live
//! dashboards, run control (stop/kill/pause/scale), a YAML test library with
//! validation, agent status and backend logs, served from a single binary.
//!
//! The UI is decoupled from how runs are launched through the [`UiBackend`]
//! trait: the CLI implements it in standalone mode, the controller implements
//! it in distributed mode. A complete reference implementation,
//! [`LocalBackend`], lives in this crate and drives real
//! [`loadr_core::Engine`] runs through an injected [`EngineLauncher`].
//!
//! ```no_run
//! # async fn demo(backend: std::sync::Arc<dyn loadr_plugin_webui::UiBackend>) {
//! use loadr_plugin_webui::{AuthConfig, WebUi, WebUiConfig};
//!
//! let handle = WebUi::serve(WebUiConfig {
//!     bind: "127.0.0.1:6464".parse().expect("addr"),
//!     auth: AuthConfig::default(),
//!     backend,
//! })
//! .await
//! .expect("serve");
//! println!("web UI listening on http://{}", handle.addr);
//! # }
//! ```

pub mod backend;
pub mod server;

mod api;
mod payload;
mod stream;

use std::collections::BTreeMap;
use std::sync::Arc;

pub use backend::{EngineLauncher, LauncherResult, LocalBackend, WebUiLogLayer};
pub use server::{AuthConfig, WebUi, WebUiConfig, WebUiError, WebUiHandle};

/// One run as the UI sees it (live or historical).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunInfo {
    pub run_id: String,
    pub name: Option<String>,
    /// `pending` | `running` | `stopping` | `finished` | `degraded` |
    /// `aborted` | `failed`.
    pub state: String,
    /// Threshold outcome. `None` while live and when no threshold produced a
    /// decision (for example, no thresholds or no samples).
    pub passed: Option<bool>,
    pub started_ms: u64,
    pub ended_ms: Option<u64>,
    /// Backend wall-clock time when this view was produced. Live duration
    /// calculations use this instead of the browser clock.
    #[serde(default)]
    pub observed_ms: u64,
    pub scenarios: Vec<String>,
    /// Agent ids participating in the run (empty in standalone mode).
    pub agents: Vec<String>,
    /// Agent ids that have supplied metric data for this run.
    #[serde(default)]
    pub contributing_agents: Vec<String>,
    /// Assigned agents declared lost while the run was active.
    #[serde(default)]
    pub lost_agents: Vec<String>,
    /// Whether all assigned agents contributed without being lost. `None` in
    /// standalone mode, where fleet completeness is not applicable.
    #[serde(default)]
    pub complete: Option<bool>,
    /// Distributed loss policy (`continue` or `abort`).
    #[serde(default)]
    pub on_agent_loss: Option<String>,
}

/// Live control state as observed by the backend.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RunControlView {
    pub externally_controlled: Vec<String>,
    /// `None` means the backend cannot observe the applied pause state.
    pub is_paused: Option<bool>,
    /// Whether a successful control response confirms all target agents
    /// applied the command, rather than merely accepting the request.
    pub agent_confirmed: bool,
}

/// Backend capabilities used to avoid advertising unavailable production
/// features as empty or broken pages.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UiCapabilities {
    pub mode: String,
    pub can_start_runs: bool,
    pub can_edit_tests: bool,
    pub logs_available: bool,
    pub persistent_history: bool,
}

impl Default for UiCapabilities {
    fn default() -> Self {
        Self {
            mode: "standalone".to_string(),
            can_start_runs: true,
            can_edit_tests: true,
            logs_available: true,
            persistent_history: true,
        }
    }
}

/// One load-generation agent (distributed mode).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AgentView {
    pub id: String,
    pub name: String,
    pub healthy: bool,
    pub active_vus: u64,
    pub cores: u32,
    /// Direct peer socket observed by the controller for the current session.
    pub peer_addr: Option<String>,
    pub version: Option<String>,
    pub revision: Option<String>,
    /// Absolute wall-clock time (ms since the UNIX epoch) of the last heartbeat,
    /// retained for API compatibility and timestamp displays.
    pub last_heartbeat_ms: u64,
    /// Backend-observed age of the last heartbeat. This is authoritative for
    /// health/age displays because browser and controller clocks may differ.
    #[serde(default)]
    pub last_heartbeat_age_ms: u64,
    pub labels: BTreeMap<String, String>,
}

/// A saved test definition in the test library.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredTest {
    pub name: String,
    pub yaml: String,
    pub updated_ms: u64,
}

/// One backend log line for the Logs page.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogLine {
    pub ts_ms: u64,
    /// `trace` | `debug` | `info` | `warn` | `error`.
    pub level: String,
    pub message: String,
}

/// Everything the web UI needs from whatever is orchestrating runs.
///
/// Implemented by the CLI (standalone) and the controller (distributed);
/// [`LocalBackend`] is the in-process reference implementation.
#[async_trait::async_trait]
pub trait UiBackend: Send + Sync + 'static {
    /// Validate and start a test from YAML; returns the new run id.
    async fn start_test(
        &self,
        name: Option<String>,
        yaml: String,
        env: Option<String>,
    ) -> Result<String, String>;

    /// Backend-specific validation such as resolving referenced files from the
    /// controller's storage directory. Syntax/schema validation is handled by
    /// the Web UI API before this hook.
    fn validate_references(&self, _yaml: &str, _env: Option<&str>) -> Result<(), String> {
        Ok(())
    }

    /// All known runs, newest first.
    fn runs(&self) -> Vec<RunInfo>;

    /// Live control handle for a run (None once it finished).
    fn run_handle(&self, run_id: &str) -> Option<loadr_core::RunHandle>;

    /// Latest snapshot: live for running runs, last-known for finished ones.
    fn run_snapshot(&self, run_id: &str) -> Option<Arc<loadr_core::Snapshot>>;

    /// Exact cumulative rollups. Histogram series must be merged before
    /// percentiles are extracted; callers must not average snapshot
    /// percentiles as a substitute.
    fn run_aggregate_snapshot(&self, run_id: &str) -> Option<Arc<loadr_core::Snapshot>> {
        self.run_handle(run_id)
            .map(|handle| handle.aggregate_snapshot())
    }

    /// Current threshold statuses for a run.
    fn run_thresholds(&self, run_id: &str) -> Vec<loadr_core::ThresholdStatus>;

    /// End-of-run summary (None while the run is still live).
    fn run_summary(&self, run_id: &str) -> Option<loadr_core::Summary>;

    fn run_control_state(&self, run_id: &str) -> RunControlView {
        match self.run_handle(run_id) {
            Some(handle) => RunControlView {
                externally_controlled: handle.externally_controlled_scenarios(),
                is_paused: Some(handle.is_paused()),
                agent_confirmed: true,
            },
            None => RunControlView::default(),
        }
    }

    /// Stop a run: graceful by default, immediate when `kill` is set.
    async fn stop_run(&self, run_id: &str, kill: bool) -> Result<(), String>;

    /// Pause or resume a run.
    async fn pause_run(&self, run_id: &str, paused: bool) -> Result<(), String>;

    /// Scale an externally-controlled scenario to `vus`.
    async fn scale_run(&self, run_id: &str, scenario: &str, vus: u64) -> Result<(), String>;

    /// Connected agents (empty in standalone mode).
    fn agents(&self) -> Vec<AgentView>;

    /// The saved test library.
    fn tests(&self) -> Vec<StoredTest>;

    fn save_test(&self, name: String, yaml: String) -> Result<(), String>;

    fn delete_test(&self, name: &str) -> Result<(), String>;

    /// Recent backend log lines, oldest first.
    fn recent_logs(&self) -> Vec<LogLine>;

    fn capabilities(&self) -> UiCapabilities {
        UiCapabilities::default()
    }
}

/// Milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod agent_view_tests {
    use super::*;

    #[test]
    fn agent_debug_metadata_has_stable_json_fields() {
        let view = AgentView {
            id: "agent-a".to_string(),
            name: "worker-a".to_string(),
            healthy: true,
            active_vus: 4,
            cores: 8,
            peer_addr: Some("10.0.0.4:49152".to_string()),
            version: Some("1.29.0".to_string()),
            revision: Some("0123456789ab".to_string()),
            last_heartbeat_ms: 123,
            last_heartbeat_age_ms: 0,
            labels: BTreeMap::new(),
        };

        let value = serde_json::to_value(view).expect("serialize agent");
        assert_eq!(value["peer_addr"], "10.0.0.4:49152");
        assert_eq!(value["version"], "1.29.0");
        assert_eq!(value["revision"], "0123456789ab");
    }

    #[test]
    fn missing_debug_metadata_deserializes_as_unavailable() {
        let view: AgentView = serde_json::from_value(serde_json::json!({
            "id": "agent-a",
            "name": "worker-a",
            "healthy": false,
            "active_vus": 0,
            "cores": 8,
            "last_heartbeat_ms": 123,
            "labels": {}
        }))
        .expect("deserialize old agent payload");

        assert_eq!(view.peer_addr, None);
        assert_eq!(view.version, None);
        assert_eq!(view.revision, None);
    }
}
