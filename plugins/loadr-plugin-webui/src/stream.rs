//! Server-sent event streams: per-run live metrics and the global overview.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::response::sse::{Event, Sse};
use loadr_core::engine::RunStatus;
use loadr_core::RunHandle;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::api::ApiError;
use crate::backend::status_string;
use crate::payload::{live_payload, overview_json};
use crate::server::AppState;
use crate::UiBackend;

type EventStream = Sse<ReceiverStream<Result<Event, Infallible>>>;

fn sse_event(name: &str, payload: &Value) -> Result<Event, Infallible> {
    Ok(Event::default().event(name).data(payload.to_string()))
}

fn status_payload(state: &str, passed: Option<bool>) -> Value {
    json!({ "state": state, "passed": passed })
}

/// `GET /api/runs/{id}/stream` — one trimmed "snapshot" event per second plus
/// "status" events on state changes. Ends shortly after the run finishes.
pub(crate) async fn run_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<EventStream, ApiError> {
    let info = state
        .backend
        .runs()
        .into_iter()
        .find(|r| r.run_id == id)
        .ok_or_else(|| ApiError::NotFound(format!("run `{id}` not found")))?;

    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(32);
    match state.backend.run_handle(&id) {
        Some(handle) => {
            tokio::spawn(live_run_stream(handle, state.backend.clone(), id, tx));
        }
        None if matches!(info.state.as_str(), "pending" | "running" | "stopping") => {
            // No local handle (distributed run): poll the backend snapshot.
            let backend = state.backend.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    ticker.tick().await;
                    let Some(run) = backend.runs().into_iter().find(|r| r.run_id == id) else {
                        break;
                    };
                    if let Some(snap) = backend.run_snapshot(&id) {
                        let exact = backend.run_aggregate_snapshot(&id);
                        let control = backend.run_control_state(&id);
                        let payload = live_payload(
                            &snap,
                            exact.as_deref(),
                            &backend.run_thresholds(&id),
                            &run,
                            &control,
                        );
                        if tx.send(sse_event("snapshot", &payload)).await.is_err() {
                            break;
                        }
                    }
                    if !matches!(run.state.as_str(), "pending" | "running" | "stopping") {
                        let _ = tx
                            .send(sse_event("status", &status_payload(&run.state, run.passed)))
                            .await;
                        break;
                    }
                }
            });
        }
        None => {
            // Finished run: replay the last-known state once, then end.
            if let Some(snap) = state.backend.run_snapshot(&id) {
                let exact = state.backend.run_aggregate_snapshot(&id);
                let control = state.backend.run_control_state(&id);
                let payload = live_payload(
                    &snap,
                    exact.as_deref(),
                    &state.backend.run_thresholds(&id),
                    &info,
                    &control,
                );
                let _ = tx.try_send(sse_event("snapshot", &payload));
            }
            let _ = tx.try_send(sse_event(
                "status",
                &status_payload(&info.state, info.passed),
            ));
        }
    }
    Ok(Sse::new(ReceiverStream::new(rx)))
}

async fn live_run_stream(
    handle: RunHandle,
    backend: Arc<dyn UiBackend>,
    id: String,
    tx: mpsc::Sender<Result<Event, Infallible>>,
) {
    let mut snapshots = handle.watch_snapshots();
    let mut status = handle.watch_status();

    let send_snapshot = |tx: &mpsc::Sender<Result<Event, Infallible>>,
                         handle: &RunHandle,
                         snap: Arc<loadr_core::Snapshot>| {
        let Some(run) = backend.runs().into_iter().find(|run| run.run_id == id) else {
            return false;
        };
        let exact = backend.run_aggregate_snapshot(&id);
        let control = backend.run_control_state(&id);
        let payload = live_payload(
            &snap,
            exact.as_deref(),
            &handle.threshold_statuses(),
            &run,
            &control,
        );
        tx.try_send(sse_event("snapshot", &payload)).is_ok()
    };

    // Initial state so the dashboard renders immediately.
    let initial = handle.snapshot();
    if !send_snapshot(&tx, &handle, initial) {
        return;
    }
    let current = handle.status();
    let passed = match &current {
        RunStatus::Finished { passed } => Some(*passed),
        _ => None,
    };
    if tx
        .send(sse_event(
            "status",
            &status_payload(status_string(&current), passed),
        ))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            changed = snapshots.changed() => {
                if changed.is_err() {
                    break;
                }
                let snap = snapshots.borrow_and_update().clone();
                let Some(run) = backend.runs().into_iter().find(|run| run.run_id == id) else {
                    break;
                };
                let exact = backend.run_aggregate_snapshot(&id);
                let control = backend.run_control_state(&id);
                let payload = live_payload(
                    &snap,
                    exact.as_deref(),
                    &handle.threshold_statuses(),
                    &run,
                    &control,
                );
                if tx.send(sse_event("snapshot", &payload)).await.is_err() {
                    return;
                }
            }
            changed = status.changed() => {
                if changed.is_err() {
                    break;
                }
                let new_status = status.borrow_and_update().clone();
                let default_passed = match &new_status {
                    RunStatus::Finished { passed } => Some(*passed),
                    _ => None,
                };
                let finished = matches!(new_status, RunStatus::Finished { .. });
                if finished {
                    // Wait briefly for the final snapshot the engine emits.
                    let _ = tokio::time::timeout(
                        Duration::from_millis(300),
                        snapshots.changed(),
                    )
                    .await;
                    let snap = snapshots.borrow_and_update().clone();
                    let _ = send_snapshot(&tx, &handle, snap);
                }
                let reported = finished
                    .then(|| backend.runs().into_iter().find(|run| run.run_id == id))
                    .flatten();
                let (reported_state, reported_passed) = match reported {
                    Some(run) => (run.state, run.passed),
                    None => (status_string(&new_status).to_string(), default_passed),
                };
                if tx
                    .send(sse_event(
                        "status",
                        &status_payload(&reported_state, reported_passed),
                    ))
                    .await
                    .is_err()
                    || finished
                {
                    return;
                }
            }
            _ = tx.closed() => return,
        }
    }

    // Watch channels closed (engine task ended): flush the final state.
    let snap = handle.snapshot();
    let _ = send_snapshot(&tx, &handle, snap);
    let final_status = handle.status();
    let passed = match &final_status {
        RunStatus::Finished { passed } => Some(*passed),
        _ => None,
    };
    let _ = tx
        .send(sse_event(
            "status",
            &status_payload(status_string(&final_status), passed),
        ))
        .await;
}

/// `GET /api/stream` — one "overview" event per second across all runs.
pub(crate) async fn overview_stream(State(state): State<Arc<AppState>>) -> EventStream {
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(8);
    let backend: Arc<dyn UiBackend> = state.backend.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let payload = overview_json(backend.as_ref());
            if tx.send(sse_event("overview", &payload)).await.is_err() {
                return;
            }
        }
    });
    Sse::new(ReceiverStream::new(rx))
}
