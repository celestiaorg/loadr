//! REST API handlers under `/api`.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use loadr_config::{ConfigError, Diagnostic, LoadOptions};
use serde_json::{json, Value};

use crate::payload::overview_json;
use crate::server::AppState;

/// API error → JSON response.
pub(crate) enum ApiError {
    NotFound(String),
    Bad(String),
    Unprocessable(Vec<Diagnostic>),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::NotFound(msg) => {
                (StatusCode::NOT_FOUND, Json(json!({ "error": msg }))).into_response()
            }
            ApiError::Bad(msg) => {
                (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))).into_response()
            }
            ApiError::Unprocessable(diags) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "error": "validation failed", "diagnostics": diags })),
            )
                .into_response(),
        }
    }
}

/// Public build metadata used by the embedded UI footer.
pub(crate) async fn version() -> Json<Value> {
    Json(json!({
        "version": loadr_core::build_info::VERSION,
        "revision": loadr_core::build_info::GIT_REVISION,
    }))
}

pub(crate) async fn overview(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(overview_json(state.backend.as_ref()))
}

pub(crate) async fn capabilities(
    State(state): State<Arc<AppState>>,
) -> Json<crate::UiCapabilities> {
    Json(state.backend.capabilities())
}

pub(crate) async fn list_runs(State(state): State<Arc<AppState>>) -> Json<Vec<crate::RunInfo>> {
    Json(state.backend.runs())
}

#[derive(serde::Deserialize)]
pub(crate) struct StartRunBody {
    pub name: Option<String>,
    pub yaml: String,
    pub env: Option<String>,
}

pub(crate) async fn start_run(
    State(state): State<Arc<AppState>>,
    Json(body): Json<StartRunBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if body.yaml.trim().is_empty() {
        return Err(ApiError::Bad("`yaml` is required".to_string()));
    }
    // Validate first so the caller gets structured diagnostics, not a string.
    let opts = LoadOptions {
        env: body.env.clone(),
        check_files: false,
        deny_errors: true,
    };
    if let Err(e) = loadr_config::load_str(&body.yaml, &opts) {
        return Err(ApiError::Unprocessable(config_error_diagnostics(e)));
    }
    if let Err(message) = state
        .backend
        .validate_references(&body.yaml, body.env.as_deref())
    {
        return Err(ApiError::Unprocessable(vec![Diagnostic::error(
            "files", message,
        )]));
    }
    let run_id = state
        .backend
        .start_test(body.name, body.yaml, body.env)
        .await
        .map_err(ApiError::Bad)?;
    Ok((StatusCode::CREATED, Json(json!({ "run_id": run_id }))))
}

pub(crate) async fn run_detail(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let info = state
        .backend
        .runs()
        .into_iter()
        .find(|r| r.run_id == id)
        .ok_or_else(|| ApiError::NotFound(format!("run `{id}` not found")))?;
    let thresholds = state.backend.run_thresholds(&id);
    let control = state.backend.run_control_state(&id);
    Ok(Json(json!({
        "run": info,
        "thresholds": thresholds,
        "externally_controlled": control.externally_controlled,
        "is_paused": control.is_paused,
        "control_agent_confirmed": control.agent_confirmed,
    })))
}

pub(crate) async fn run_snapshot(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Arc<loadr_core::Snapshot>>, ApiError> {
    state
        .backend
        .run_snapshot(&id)
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("no snapshot for run `{id}`")))
}

pub(crate) async fn run_summary(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let summary = state
        .backend
        .run_summary(&id)
        .ok_or_else(|| ApiError::NotFound(format!("run `{id}` has no summary (still live?)")))?;
    let run = state
        .backend
        .runs()
        .into_iter()
        .find(|run| run.run_id == id)
        .ok_or_else(|| ApiError::NotFound(format!("run `{id}` not found")))?;
    let mut payload = serde_json::to_value(&summary)
        .map_err(|error| ApiError::Bad(format!("cannot serialize run summary: {error}")))?;
    payload["final_metrics"] = crate::payload::final_payload(&summary, &run);
    Ok(Json(payload))
}

#[derive(serde::Deserialize, Default)]
pub(crate) struct StopBody {
    #[serde(default)]
    pub kill: bool,
}

pub(crate) async fn stop_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    body: Option<Json<StopBody>>,
) -> Result<Json<Value>, ApiError> {
    let kill = body.map(|Json(b)| b.kill).unwrap_or(false);
    let confirmed = state.backend.run_control_state(&id).agent_confirmed;
    state
        .backend
        .stop_run(&id, kill)
        .await
        .map_err(ApiError::Bad)?;
    Ok(Json(
        json!({ "requested": true, "confirmed": confirmed, "kill": kill }),
    ))
}

#[derive(serde::Deserialize)]
pub(crate) struct PauseBody {
    pub paused: bool,
}

pub(crate) async fn pause_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<PauseBody>,
) -> Result<Json<Value>, ApiError> {
    let confirmed = state.backend.run_control_state(&id).agent_confirmed;
    state
        .backend
        .pause_run(&id, body.paused)
        .await
        .map_err(ApiError::Bad)?;
    Ok(Json(
        json!({ "paused": body.paused, "confirmed": confirmed }),
    ))
}

#[derive(serde::Deserialize)]
pub(crate) struct ScaleBody {
    pub scenario: String,
    pub vus: u64,
}

pub(crate) async fn scale_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<ScaleBody>,
) -> Result<Json<Value>, ApiError> {
    let confirmed = state.backend.run_control_state(&id).agent_confirmed;
    state
        .backend
        .scale_run(&id, &body.scenario, body.vus)
        .await
        .map_err(ApiError::Bad)?;
    Ok(Json(
        json!({ "scenario": body.scenario, "vus": body.vus, "confirmed": confirmed }),
    ))
}

pub(crate) async fn agents(State(state): State<Arc<AppState>>) -> Json<Vec<crate::AgentView>> {
    Json(state.backend.agents())
}

pub(crate) async fn list_tests(State(state): State<Arc<AppState>>) -> Json<Vec<crate::StoredTest>> {
    Json(state.backend.tests())
}

#[derive(serde::Deserialize)]
pub(crate) struct PutTestBody {
    pub yaml: String,
}

pub(crate) async fn put_test(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(body): Json<PutTestBody>,
) -> Result<Json<Value>, ApiError> {
    state
        .backend
        .save_test(name.clone(), body.yaml)
        .map_err(ApiError::Bad)?;
    Ok(Json(json!({ "saved": true, "name": name })))
}

pub(crate) async fn delete_test(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .backend
        .delete_test(&name)
        .map_err(ApiError::NotFound)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(serde::Deserialize)]
pub(crate) struct ValidateBody {
    pub yaml: String,
    pub env: Option<String>,
}

pub(crate) async fn validate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ValidateBody>,
) -> Json<Value> {
    let opts = LoadOptions {
        env: body.env.clone(),
        check_files: false,
        deny_errors: false,
    };
    let mut diagnostics = match loadr_config::load_str(&body.yaml, &opts) {
        Ok(loaded) => loaded.diagnostics,
        Err(e) => config_error_diagnostics(e),
    };
    if !diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == loadr_config::Severity::Error)
    {
        if let Err(message) = state
            .backend
            .validate_references(&body.yaml, body.env.as_deref())
        {
            diagnostics.push(Diagnostic::error("files", message));
        }
    }
    Json(json!({ "diagnostics": diagnostics }))
}

pub(crate) async fn logs(State(state): State<Arc<AppState>>) -> Json<Vec<crate::LogLine>> {
    Json(state.backend.recent_logs())
}

/// Flatten every [`ConfigError`] variant into located diagnostics.
fn config_error_diagnostics(err: ConfigError) -> Vec<Diagnostic> {
    match err {
        ConfigError::Invalid(diags) => diags,
        ConfigError::Deserialize(diag) => vec![diag],
        ConfigError::Syntax(msg) => vec![syntax_diagnostic(&msg)],
        ConfigError::UnknownEnv {
            requested,
            available,
        } => vec![Diagnostic::error(
            "env",
            if available.is_empty() {
                format!("unknown environment `{requested}`; no environments are defined")
            } else {
                format!(
                    "unknown environment `{requested}`; available: {}",
                    available.join(", ")
                )
            },
        )],
        other => vec![Diagnostic::error("", other.to_string())],
    }
}

/// Pull "line X column Y" out of a serde_yaml message for editor jumping.
fn syntax_diagnostic(msg: &str) -> Diagnostic {
    let mut diag = Diagnostic::error("", format!("YAML syntax error: {msg}"));
    if let (Some(line), col) = (
        find_number_after(msg, "line "),
        find_number_after(msg, "column "),
    ) {
        diag = diag.with_position(line, col.unwrap_or(1));
    }
    diag
}

fn find_number_after(text: &str, marker: &str) -> Option<usize> {
    let idx = text.find(marker)? + marker.len();
    let digits: String = text[idx..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syntax_diagnostic_extracts_position() {
        let d = syntax_diagnostic("mapping values are not allowed at line 3 column 7");
        assert_eq!(d.line, Some(3));
        assert_eq!(d.column, Some(7));
        assert!(d.message.contains("YAML syntax error"));
    }

    #[test]
    fn config_errors_become_diagnostics() {
        let err = loadr_config::load_str("scenariosss:\n  s: {}\n", &LoadOptions::new())
            .expect_err("should fail");
        let diags = config_error_diagnostics(err);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("unknown field"));
        assert!(diags[0].line.is_some());
    }
}
