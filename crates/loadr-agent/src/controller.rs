//! The coordination controller: accepts agent sessions, assigns partitioned
//! runs behind a synchronized start barrier, merges metric deltas into one
//! central aggregator and evaluates thresholds over the whole fleet.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::Stream;
use loadr_core::thresholds::{compile_thresholds, evaluate_all, CompiledThreshold};
use loadr_core::{
    AggValues, Aggregator, MetricKind, MetricsDelta, Snapshot, Summary, ThresholdStatus,
    TimelinePoint,
};
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch, Notify};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use crate::error::AgentError;
use crate::pb;
use crate::pb::agent_message::Msg as AgentMsg;
use crate::pb::controller_message::Msg as CtrlMsg;
use crate::pb::coordination_server::{Coordination, CoordinationServer};
use crate::{now_unix_ms, PROTOCOL_VERSION};

/// TLS settings for the controller listener.
#[derive(Debug, Clone)]
pub struct ControllerTls {
    pub cert_pem: PathBuf,
    pub key_pem: PathBuf,
    /// When set, agents must present a client certificate signed by this CA (mTLS).
    pub client_ca_pem: Option<PathBuf>,
}

/// Controller configuration.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub bind: SocketAddr,
    pub tls: Option<ControllerTls>,
    /// An agent with no traffic for this long is considered lost (default 6s).
    pub agent_liveness: Duration,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        ControllerConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            tls: None,
            agent_liveness: Duration::from_secs(90),
        }
    }
}

/// What to do with an in-flight run when one of its agents is lost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OnAgentLoss {
    /// Keep going with the remaining agents (default).
    #[default]
    Continue,
    /// Stop the run on the remaining agents and mark it failed.
    Abort,
}

/// Options for [`ControllerHandle::submit`].
#[derive(Clone)]
pub struct SubmitOptions {
    /// Environment override (`env.<name>` block in the plan).
    pub env: Option<String>,
    /// Run name override (defaults to the plan name).
    pub name: Option<String>,
    /// Data files shipped to every agent, as (relative path, content).
    pub files: Vec<(String, Vec<u8>)>,
    /// Only assign to agents whose labels contain all of these.
    pub agent_filter: Option<HashMap<String, String>>,
    pub on_agent_loss: OnAgentLoss,
    /// Synchronized start barrier delay (default 2s).
    pub start_barrier: Duration,
}

impl Default for SubmitOptions {
    fn default() -> Self {
        SubmitOptions {
            env: None,
            name: None,
            files: Vec::new(),
            agent_filter: None,
            on_agent_loss: OnAgentLoss::default(),
            start_barrier: Duration::from_secs(2),
        }
    }
}

/// Live agent info for CLIs and web UIs.
#[derive(Debug, Clone)]
pub struct AgentInfo {
    pub id: String,
    pub name: String,
    pub labels: HashMap<String, String>,
    pub cores: u32,
    /// Direct peer socket observed by the controller for the current session.
    pub peer_addr: Option<SocketAddr>,
    pub version: Option<String>,
    pub revision: Option<String>,
    pub connected_secs: u64,
    /// Milliseconds since the last heartbeat/traffic.
    pub last_heartbeat_ms: u64,
    pub active_vus: u64,
    pub healthy: bool,
}

/// Run listing entry.
#[derive(Debug, Clone)]
pub struct RunSummaryInfo {
    pub run_id: String,
    pub name: Option<String>,
    /// pending | running | stopping | finished | aborted | failed
    pub state: String,
    pub started_ms: u64,
    pub ended_ms: Option<u64>,
    pub agents: Vec<String>,
}

/// Decision-relevant lifecycle and completeness state for one distributed run.
#[derive(Debug, Clone)]
pub struct RunOperationalInfo {
    pub scenarios: Vec<String>,
    pub externally_controlled: Vec<String>,
    pub assigned: Vec<String>,
    pub contributing: Vec<String>,
    pub completed: Vec<String>,
    pub lost: Vec<String>,
    pub on_agent_loss: OnAgentLoss,
    /// Applied pause state. `None` means a partially acknowledged command may
    /// have left agents in different states.
    pub paused: Option<bool>,
}

/// One exact all-tags metric aggregate for a controller run.
#[derive(Debug, Clone)]
pub struct FleetMetric {
    pub metric: String,
    pub kind: MetricKind,
    pub agg: AggValues,
}

/// Prometheus-ready source data for one run. `detailed` retains individual
/// agent/tag series; `fleet` contains exact centrally merged aggregates.
#[derive(Debug, Clone)]
pub struct RunMetricsView {
    pub run_id: String,
    pub name: Option<String>,
    pub state: String,
    pub started_ms: u64,
    pub detailed: Snapshot,
    pub fleet: Vec<FleetMetric>,
}

/// Split `total` VUs across `agents`, remainder to the lowest indices —
/// matching `loadr_core::partition_spec` share math.
pub fn scale_shares(total: u64, agents: u64) -> Vec<u64> {
    (0..agents)
        .map(|i| total / agents + u64::from(i < total % agents))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunState {
    Pending,
    Running,
    Stopping,
    Finished,
    Aborted,
    Failed,
}

impl RunState {
    fn is_terminal(self) -> bool {
        matches!(
            self,
            RunState::Finished | RunState::Aborted | RunState::Failed
        )
    }

    fn as_str(self) -> &'static str {
        match self {
            RunState::Pending => "pending",
            RunState::Running => "running",
            RunState::Stopping => "stopping",
            RunState::Finished => "finished",
            RunState::Aborted => "aborted",
            RunState::Failed => "failed",
        }
    }
}

type AgentSender = mpsc::Sender<Result<pb::ControllerMessage, Status>>;

struct AgentEntry {
    name: String,
    labels: HashMap<String, String>,
    cores: u32,
    peer_addr: Option<SocketAddr>,
    version: Option<String>,
    revision: Option<String>,
    /// Identifies the agent *process*. A restart mints a new one, which is how
    /// the controller knows the uplink sequence restarted at 1 rather than
    /// continuing across a reconnect.
    incarnation: String,
    connected_at: Instant,
    last_heartbeat: Instant,
    active_vus: u64,
    connected: bool,
    session: u64,
    /// Highest uplink sequence applied for `incarnation`. Cumulative: every
    /// sequence at or below it has been applied exactly once.
    last_seq: u64,
    sender: AgentSender,
}

struct PendingControl {
    expected: HashSet<String>,
    responses: HashMap<String, Result<(), String>>,
    action: String,
    scenario: String,
    notify: Arc<Notify>,
}

/// What to do with one inbound uplink message, decided under a single `agents`
/// lock so the session fence and the deduplication cursor cannot disagree.
enum Admit {
    /// Live session, not yet applied.
    Apply,
    /// Live session, but already applied under this incarnation. The agent
    /// replayed it because an acknowledgement was lost; it has been
    /// acknowledged again.
    Duplicate,
    /// A newer registration superseded this stream. Nothing was touched.
    Stale,
}

struct ControllerRun {
    run_id: String,
    name: Option<String>,
    #[allow(dead_code)]
    plan_yaml: String,
    scenarios: Vec<String>,
    externally_controlled: Vec<String>,
    thresholds: Vec<CompiledThreshold>,
    on_agent_loss: OnAgentLoss,
    /// Agent ids in partition order.
    assigned: Vec<String>,
    state: Mutex<RunState>,
    started_ms: u64,
    finished_ms: Mutex<Option<u64>>,
    agg: Mutex<Aggregator>,
    /// agent_id → terminal event kind (finished/aborted/failed).
    done: Mutex<HashMap<String, String>>,
    /// Agents currently treated as lost for run-completion accounting.
    lost: Mutex<HashSet<String>>,
    /// Monotonic record used to qualify result completeness even after an
    /// agent reconnects and resumes.
    lost_ever: Mutex<HashSet<String>>,
    contributing: Mutex<HashSet<String>>,
    paused: Mutex<Option<bool>>,
    pending_controls: Mutex<HashMap<u64, PendingControl>>,
    /// Per-agent summaries as reported.
    summaries: Mutex<Vec<Summary>>,
    /// Frozen fleet summary, created exactly once at terminal transition.
    merged_summary: Mutex<Option<Summary>>,
    threshold_statuses: Mutex<Vec<ThresholdStatus>>,
    abort_reason: Mutex<Option<String>>,
    snapshot_tx: watch::Sender<Arc<Snapshot>>,
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
    /// Per-interval time series for the HTML report, sampled once a second
    /// from the centrally merged snapshot.
    timeline: Mutex<Vec<TimelinePoint>>,
    /// Prometheus projection cached once the run is terminal — a finished
    /// run's data no longer changes, so exporters stop paying the rebuild.
    prom_view: Mutex<Option<Arc<RunMetricsView>>>,
}

struct Inner {
    controller_id: String,
    liveness: Duration,
    agents: Mutex<HashMap<String, AgentEntry>>,
    runs: Mutex<HashMap<String, Arc<ControllerRun>>>,
    session_counter: AtomicU64,
    control_counter: AtomicU64,
}

fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

impl Inner {
    fn register_agent(
        &self,
        reg: &pb::Register,
        peer_addr: Option<SocketAddr>,
        sender: AgentSender,
        session: u64,
    ) {
        let superseded = {
            let mut agents = self.agents.lock();
            // Only a reconnect of the *same* process continues its uplink
            // sequence. A restarted agent begins again at 1, so its cursor has
            // to reset here — before its first message is admitted — or every
            // message it sends looks like a duplicate and gets acknowledged
            // against a cursor it never earned.
            let last_seq = agents
                .get(&reg.agent_id)
                .filter(|prev| prev.incarnation == reg.incarnation)
                .map_or(0, |prev| prev.last_seq);
            agents.insert(
                reg.agent_id.clone(),
                AgentEntry {
                    name: reg.agent_name.clone(),
                    labels: reg.labels.clone(),
                    cores: reg.cpu_cores,
                    peer_addr,
                    version: non_empty(&reg.loadr_version),
                    revision: non_empty(&reg.build_revision),
                    incarnation: reg.incarnation.clone(),
                    connected_at: Instant::now(),
                    last_heartbeat: Instant::now(),
                    active_vus: 0,
                    connected: true,
                    session,
                    last_seq,
                    sender,
                },
            )
        };
        if let Some(previous) = superseded {
            if previous.connected && previous.incarnation != reg.incarnation {
                tracing::warn!(
                    agent = %reg.agent_id,
                    "a second agent process registered with this id; check for a duplicated agent id"
                );
            }
            // Close the stream this session replaced instead of leaving it to
            // time out: the peer learns immediately, and with a reason.
            let _ = previous.sender.try_send(Err(Status::aborted(
                "session superseded by a newer registration",
            )));
        }
        if !reg.resume_run_id.is_empty() {
            self.resume_run(&reg.agent_id, &reg.resume_run_id);
        }
    }

    /// A reconnecting agent claims an in-flight run. Clear its lost marker only
    /// when the claim is credible: the run must exist, still be live, and
    /// actually have this agent in its partition.
    fn resume_run(&self, agent_id: &str, run_id: &str) {
        let Some(run) = self.runs.lock().get(run_id).cloned() else {
            tracing::debug!(agent = %agent_id, %run_id, "resume claim for an unknown run ignored");
            return;
        };
        if !run.assigned.iter().any(|id| id == agent_id) {
            tracing::warn!(
                agent = %agent_id, %run_id,
                "resume claim for a run this agent was never assigned"
            );
            return;
        }
        if run.state.lock().is_terminal() || run.done.lock().contains_key(agent_id) {
            tracing::debug!(agent = %agent_id, %run_id, "resume claim for a completed run ignored");
            return;
        }
        // The agent came back within the grace window: let its run count again.
        if run.lost.lock().remove(agent_id) {
            tracing::info!(agent = %agent_id, %run_id, "agent resumed an in-flight run");
        } else {
            tracing::info!(agent = %agent_id, %run_id, "agent reconnected with a run in flight");
        }
    }

    fn mark_disconnected(&self, agent_id: &str, session: u64) {
        let mut agents = self.agents.lock();
        if let Some(entry) = agents.get_mut(agent_id) {
            if entry.session == session {
                entry.connected = false;
            }
        }
    }

    /// Fence one inbound message against the live session, move the
    /// deduplication cursor and acknowledge it — all under one `agents` lock.
    ///
    /// A superseded stream is refused outright: it must not refresh liveness
    /// (that would mask a real loss), must not move the cursor, and must not be
    /// acknowledged. The agent's replay window is shared across its sessions, so
    /// acknowledging here would retire messages this controller explicitly
    /// declined to apply — losing them for good.
    fn admit(&self, agent_id: &str, session: u64, seq: u64) -> Admit {
        let mut agents = self.agents.lock();
        let Some(entry) = agents.get_mut(agent_id) else {
            return Admit::Stale;
        };
        if entry.session != session {
            return Admit::Stale;
        }
        // Any traffic from the live session is proof of life.
        entry.last_heartbeat = Instant::now();

        if seq == 0 {
            // Ephemeral (a heartbeat): the agent never buffered it, so there is
            // nothing to deduplicate. Re-advertise the cursor anyway — it is the
            // only traffic an idle agent sends, so it is what releases a window
            // whose acknowledgement went missing.
            let cursor = entry.last_seq;
            if cursor > 0 {
                send_uplink_ack(entry, cursor);
            }
            return Admit::Apply;
        }

        if seq <= entry.last_seq {
            // Our acknowledgement was lost, not the message. Re-acknowledge and
            // drop the payload: metric merging is additive, so applying twice
            // would inflate the run. Not acknowledging at all would make the
            // agent replay it forever.
            let cursor = entry.last_seq;
            send_uplink_ack(entry, cursor);
            return Admit::Duplicate;
        }
        if seq > entry.last_seq + 1 {
            // Replaying from the oldest unacknowledged message makes this
            // impossible; accept it rather than wedge the agent behind a
            // sequence it can no longer produce.
            tracing::warn!(
                agent = %agent_id,
                expected = entry.last_seq + 1,
                got = seq,
                "gap in the agent uplink sequence"
            );
        }
        // The cursor is the commit point, and it advances under the same lock
        // that validated the session. Deferring it to after the payload is
        // applied would let a registration land in between and have its freshly
        // reset cursor overwritten — silently discarding the new process's first
        // messages as duplicates.
        entry.last_seq = seq;
        send_uplink_ack(entry, seq);
        Admit::Apply
    }

    /// Apply one uplink message after fencing and deduplication. Returns `false`
    /// once a newer registration has superseded `session`, so the caller stops
    /// pumping a stream whose traffic will never be applied again.
    fn handle_agent_message(&self, agent_id: &str, session: u64, msg: pb::AgentMessage) -> bool {
        match self.admit(agent_id, session, msg.seq) {
            Admit::Apply => {
                self.apply_agent_message(agent_id, msg);
                true
            }
            Admit::Duplicate => true,
            Admit::Stale => {
                tracing::debug!(
                    agent = %agent_id,
                    session,
                    seq = msg.seq,
                    "dropped traffic from a superseded session"
                );
                false
            }
        }
    }

    /// The payload half.
    ///
    /// Every arm here is *total*: a message either takes effect or is
    /// permanently inapplicable (an unknown run, an undecodable delta). Nothing
    /// asks to be retried, which is what lets [`Inner::admit`] commit the cursor
    /// and acknowledge up front.
    fn apply_agent_message(&self, agent_id: &str, msg: pb::AgentMessage) {
        match msg.msg {
            Some(AgentMsg::Heartbeat(hb)) => {
                if let Some(entry) = self.agents.lock().get_mut(agent_id) {
                    entry.active_vus = hb.active_vus;
                }
            }
            Some(AgentMsg::Metrics(batch)) => {
                let run = self.runs.lock().get(&batch.run_id).cloned();
                let Some(run) = run else { return };
                match serde_json::from_slice::<MetricsDelta>(&batch.delta_json) {
                    Ok(mut delta) => {
                        // Identity labels are supplied by the registered
                        // controller session, never trusted from sample tags.
                        // Looked up before the state lock: no state → agents
                        // ordering edge.
                        let agent_name = self
                            .agents
                            .lock()
                            .get(agent_id)
                            .map(|entry| entry.name.clone())
                            .unwrap_or_else(|| agent_id.to_string());
                        add_agent_identity(&mut delta, &agent_name, agent_id);
                        // Apply under the run state lock: finalize_run holds
                        // it while freezing the merged summary, so a delta
                        // either fully applies (contributing + merge as one)
                        // before the freeze, or is dropped after it. Without
                        // this, the freeze could land between the two writes
                        // and publish "every agent contributed" beside an
                        // empty final summary.
                        let state = run.state.lock();
                        if state.is_terminal() {
                            tracing::debug!(
                                run_id = %batch.run_id,
                                agent = %agent_id,
                                "delta after the terminal freeze dropped"
                            );
                            return;
                        }
                        run.contributing.lock().insert(agent_id.to_string());
                        run.agg.lock().merge_delta(&delta);
                        // A delta can race the liveness sweep (or land after the
                        // agent's terminal event on a resumed stream). The sweep
                        // only zeroes *newly* lost agents, so re-zero here or the
                        // stale batch resurrects the agent's live gauges forever.
                        let lost = run.lost.lock().contains(agent_id);
                        let done = run.done.lock().contains_key(agent_id);
                        if lost || done {
                            self.zero_agent_live_gauges(&run, agent_id);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(run_id = %batch.run_id, error = %e, "bad metrics delta");
                    }
                }
            }
            Some(AgentMsg::Event(ev)) => self.handle_run_event(agent_id, ev),
            Some(AgentMsg::ControlAck(ack)) => {
                let run = self.runs.lock().get(&ack.run_id).cloned();
                let Some(run) = run else { return };
                let mut pending = run.pending_controls.lock();
                let Some(command) = pending.get_mut(&ack.command_id) else {
                    return;
                };
                if command.expected.contains(agent_id)
                    && ack.action == command.action
                    && ack.scenario == command.scenario
                {
                    let result = if ack.applied {
                        Ok(())
                    } else {
                        Err(if ack.detail.is_empty() {
                            "agent rejected command".to_string()
                        } else {
                            ack.detail
                        })
                    };
                    command.responses.insert(agent_id.to_string(), result);
                    command.notify.notify_one();
                }
            }
            Some(AgentMsg::Register(_)) | None => {}
        }
    }

    fn handle_run_event(&self, agent_id: &str, ev: pb::RunEvent) {
        let run = self.runs.lock().get(&ev.run_id).cloned();
        let Some(run) = run else {
            tracing::debug!(run_id = %ev.run_id, "event for unknown run ignored");
            return;
        };
        match ev.kind.as_str() {
            "started" => {
                let mut state = run.state.lock();
                if *state == RunState::Pending {
                    *state = RunState::Running;
                }
                tracing::info!(run_id = %ev.run_id, agent = %agent_id, "agent started run");
            }
            "finished" | "aborted" | "failed" => {
                if !ev.summary_json.is_empty() {
                    if let Ok(summary) = serde_json::from_slice::<Summary>(&ev.summary_json) {
                        run.summaries.lock().push(summary);
                    }
                }
                if (ev.kind == "aborted" || ev.kind == "failed") && !ev.detail.is_empty() {
                    let mut reason = run.abort_reason.lock();
                    if reason.is_none() {
                        *reason = Some(ev.detail.clone());
                    }
                }
                if ev.kind == "failed" {
                    tracing::warn!(
                        run_id = %ev.run_id,
                        agent = %agent_id,
                        detail = %ev.detail,
                        "agent run failed"
                    );
                }
                self.zero_agent_live_gauges(&run, agent_id);
                run.done.lock().insert(agent_id.to_string(), ev.kind);
                self.check_completion(&run);
            }
            other => tracing::debug!(kind = other, "unknown run event kind"),
        }
    }

    /// Finish the run once every assigned agent has either reported a
    /// terminal event or been declared lost.
    fn check_completion(&self, run: &Arc<ControllerRun>) {
        if run.state.lock().is_terminal() {
            return;
        }
        let (all_done, any_failed, any_aborted) = {
            let done = run.done.lock();
            let lost = run.lost.lock();
            let all_done = run
                .assigned
                .iter()
                .all(|a| done.contains_key(a) || lost.contains(a));
            let any_failed = done.values().any(|k| k == "failed") || done.is_empty();
            let any_aborted = done.values().any(|k| k == "aborted");
            (all_done, any_failed, any_aborted)
        };
        if !all_done {
            return;
        }
        let final_state = if any_failed {
            RunState::Failed
        } else if any_aborted {
            RunState::Aborted
        } else {
            RunState::Finished
        };
        self.finalize_run(run, final_state);
    }

    fn finalize_run(&self, run: &Arc<ControllerRun>, final_state: RunState) {
        let mut state = run.state.lock();
        if state.is_terminal() {
            return;
        }
        let finished_ms = now_unix_ms();
        *run.finished_ms.lock() = Some(finished_ms);
        let mut agg = run.agg.lock();
        let (statuses, _) = evaluate_all(&run.thresholds, &agg, agg.elapsed());
        let aggregates = agg.aggregate_snapshot(&[&["scenario"]]);
        let snapshot = Arc::new(agg.snapshot());
        *run.threshold_statuses.lock() = statuses;
        if snapshot.interval_secs > 0.0
            && snapshot
                .series
                .iter()
                .any(|s| s.interval_count > 0 || s.metric == "vus")
        {
            run.timeline
                .lock()
                .push(TimelinePoint::from_snapshots(&snapshot, Some(&aggregates)));
        }
        let thresholds = run.threshold_statuses.lock().clone();
        let aborted = run.abort_reason.lock().clone();
        let timeline = run.timeline.lock().clone();
        let mut summary = Summary::build(
            run.name.clone(),
            run.run_id.clone(),
            run.started_ms,
            run.scenarios.clone(),
            &mut agg,
            thresholds,
            aborted,
            timeline,
        );
        summary.ended_ms = finished_ms;
        summary.duration_secs = finished_ms.saturating_sub(run.started_ms) as f64 / 1000.0;
        summary.snapshot = (*snapshot).clone();
        // Publish the frozen summary before exposing the terminal state so
        // state and summary remain one atomic observer-facing transition.
        *run.merged_summary.lock() = Some(summary);
        drop(agg);
        *state = final_state;
        drop(state);
        let _ = run.snapshot_tx.send(snapshot);
        tracing::info!(run_id = %run.run_id, state = final_state.as_str(), "run completed");
    }

    fn zero_agent_live_gauges(&self, run: &ControllerRun, agent_id: &str) {
        let mut agg = run.agg.lock();
        for metric in loadr_core::metrics::LIVE_GAUGES {
            agg.set_gauge_by_tag(metric, "loadr_agent_id", agent_id, 0.0);
        }
    }

    /// Roll and publish exactly one controller-owned fleet interval.
    fn publish_live_snapshot(&self, run: &Arc<ControllerRun>) {
        let state = run.state.lock();
        if state.is_terminal() {
            return;
        }
        let (snapshot, aggregates) = {
            let mut agg = run.agg.lock();
            let (statuses, _) = evaluate_all(&run.thresholds, &agg, agg.elapsed());
            *run.threshold_statuses.lock() = statuses;
            let aggregates = agg.aggregate_snapshot(&[&["scenario"]]);
            (Arc::new(agg.snapshot()), aggregates)
        };
        if snapshot.interval_secs > 0.0
            && snapshot
                .series
                .iter()
                .any(|series| series.interval_count > 0 || series.metric == "vus")
        {
            run.timeline
                .lock()
                .push(TimelinePoint::from_snapshots(&snapshot, Some(&aggregates)));
        }
        let _ = run.snapshot_tx.send(snapshot);
    }

    /// Senders for the run's assigned agents that are still connected, in
    /// partition order.
    fn run_senders(&self, run: &ControllerRun) -> Vec<(String, AgentSender)> {
        let done = run.done.lock();
        let lost = run.lost.lock();
        let agents = self.agents.lock();
        run.assigned
            .iter()
            .filter(|id| !done.contains_key(*id) && !lost.contains(*id))
            .filter_map(|id| {
                agents
                    .get(id)
                    .filter(|e| e.connected)
                    .map(|e| (id.clone(), e.sender.clone()))
            })
            .collect()
    }

    /// Liveness sweep: declare agents lost and apply each run's loss policy.
    async fn sweep(&self) {
        let lost_ids: Vec<String> = self
            .agents
            .lock()
            .iter()
            .filter(|(_, e)| e.last_heartbeat.elapsed() > self.liveness)
            .map(|(id, _)| id.clone())
            .collect();
        if lost_ids.is_empty() {
            return;
        }
        let runs: Vec<Arc<ControllerRun>> = self.runs.lock().values().cloned().collect();
        for run in runs {
            if run.state.lock().is_terminal() {
                continue;
            }
            let newly: Vec<String> = lost_ids
                .iter()
                .filter(|id| {
                    run.assigned.contains(id)
                        && !run.lost.lock().contains(*id)
                        && !run.done.lock().contains_key(*id)
                })
                .cloned()
                .collect();
            if newly.is_empty() {
                continue;
            }
            for id in &newly {
                tracing::warn!(run_id = %run.run_id, agent = %id, "agent lost during run");
                run.lost.lock().insert(id.clone());
                run.lost_ever.lock().insert(id.clone());
                self.zero_agent_live_gauges(&run, id);
            }
            match run.on_agent_loss {
                OnAgentLoss::Continue => self.check_completion(&run),
                OnAgentLoss::Abort => {
                    {
                        let mut reason = run.abort_reason.lock();
                        if reason.is_none() {
                            *reason = Some(format!("agent(s) lost: {}", newly.join(", ")));
                        }
                    }
                    let targets = self.run_senders(&run);
                    for (_, sender) in targets {
                        let _ = sender
                            .send(Ok(control_message(&run.run_id, "stop", "", 0, 0)))
                            .await;
                    }
                    self.finalize_run(&run, RunState::Failed);
                }
            }
        }
    }
}

fn add_agent_identity(delta: &mut MetricsDelta, agent_name: &str, agent_id: &str) {
    for series in &mut delta.series {
        series
            .tags
            .insert("loadr_agent".to_string(), agent_name.to_string());
        series
            .tags
            .insert("loadr_agent_id".to_string(), agent_id.to_string());
    }
}

/// Cumulative acknowledgement: every uplink sequence at or below `seq` has been
/// applied.
///
/// Sent non-blocking so a congested downlink can never stall the inbound pump —
/// a dropped acknowledgement is harmless because the next one carries the same
/// or a higher cursor.
fn send_uplink_ack(entry: &AgentEntry, seq: u64) {
    let ack = pb::ControllerMessage {
        msg: Some(CtrlMsg::UplinkAck(pb::UplinkAck { seq })),
    };
    let _ = entry.sender.try_send(Ok(ack));
}

fn control_message(
    run_id: &str,
    action: &str,
    scenario: &str,
    value: u64,
    command_id: u64,
) -> pb::ControllerMessage {
    pb::ControllerMessage {
        msg: Some(CtrlMsg::Control(pb::Control {
            run_id: run_id.to_string(),
            action: action.to_string(),
            scenario: scenario.to_string(),
            value,
            command_id,
        })),
    }
}

struct CoordinationService {
    inner: Arc<Inner>,
}

type SessionStream = Pin<Box<dyn Stream<Item = Result<pb::ControllerMessage, Status>> + Send>>;

#[tonic::async_trait]
impl Coordination for CoordinationService {
    type SessionStream = SessionStream;

    async fn session(
        &self,
        request: Request<Streaming<pb::AgentMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let peer_addr = request.remote_addr();
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("stream closed before Register"))?;
        let reg = match first.msg {
            Some(AgentMsg::Register(r)) => r,
            _ => return Err(Status::invalid_argument("first message must be Register")),
        };
        if reg.protocol_version != PROTOCOL_VERSION {
            return Err(Status::failed_precondition(format!(
                "protocol version mismatch: controller speaks {PROTOCOL_VERSION}, agent speaks {}",
                reg.protocol_version
            )));
        }
        if reg.agent_id.is_empty() {
            return Err(Status::invalid_argument("agent_id is required"));
        }

        let (tx, rx) = mpsc::channel::<Result<pb::ControllerMessage, Status>>(128);
        let ack = pb::ControllerMessage {
            msg: Some(CtrlMsg::Registered(pb::Registered {
                controller_id: self.inner.controller_id.clone(),
                protocol_version: PROTOCOL_VERSION,
                message: format!("welcome {}", reg.agent_name),
            })),
        };
        tx.send(Ok(ack))
            .await
            .map_err(|_| Status::unavailable("session closed"))?;

        let session = self.inner.session_counter.fetch_add(1, Ordering::Relaxed) + 1;
        self.inner.register_agent(&reg, peer_addr, tx, session);
        tracing::info!(agent = %reg.agent_id, name = %reg.agent_name, "agent registered");

        let inner = self.inner.clone();
        let agent_id = reg.agent_id;
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(msg)) => {
                        if !inner.handle_agent_message(&agent_id, session, msg) {
                            // Superseded, and permanently so: the session
                            // counter only moves forward. Drop `inbound` so the
                            // transport tears this stream down rather than
                            // trickling messages nobody will ever apply.
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(status) => {
                        tracing::debug!(agent = %agent_id, error = %status, "agent stream ended");
                        break;
                    }
                }
            }
            inner.mark_disconnected(&agent_id, session);
            tracing::info!(agent = %agent_id, "agent disconnected");
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// The coordination controller. [`Controller::start`] binds the listener and
/// returns a [`ControllerHandle`] for submitting and managing runs.
pub struct Controller;

impl Controller {
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, AgentError> {
        let listener = tokio::net::TcpListener::bind(config.bind)
            .await
            .map_err(|e| AgentError::Transport(format!("bind {}: {e}", config.bind)))?;
        let addr = listener
            .local_addr()
            .map_err(|e| AgentError::Transport(e.to_string()))?;
        let inner = Arc::new(Inner {
            controller_id: uuid::Uuid::new_v4().to_string(),
            liveness: config.agent_liveness,
            agents: Mutex::new(HashMap::new()),
            runs: Mutex::new(HashMap::new()),
            session_counter: AtomicU64::new(0),
            control_counter: AtomicU64::new(0),
        });
        let shutdown = CancellationToken::new();

        let mut server = Server::builder();
        if let Some(tls) = &config.tls {
            server = server
                .tls_config(server_tls(tls)?)
                .map_err(|e| AgentError::Tls(e.to_string()))?;
        }
        let router = server.add_service(CoordinationServer::new(CoordinationService {
            inner: inner.clone(),
        }));
        let serve_token = shutdown.clone();
        tokio::spawn(async move {
            let result = router
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    serve_token.cancelled(),
                )
                .await;
            if let Err(e) = result {
                tracing::error!(error = %e, "coordination server failed");
            }
        });
        tokio::spawn(sweeper(inner.clone(), shutdown.clone()));
        tracing::info!(%addr, "controller listening");
        Ok(ControllerHandle {
            inner,
            addr,
            shutdown,
        })
    }
}

fn server_tls(tls: &ControllerTls) -> Result<ServerTlsConfig, AgentError> {
    let read = |path: &std::path::Path| -> Result<Vec<u8>, AgentError> {
        std::fs::read(path).map_err(|e| AgentError::Io {
            path: path.display().to_string(),
            source: e,
        })
    };
    let mut cfg = ServerTlsConfig::new().identity(Identity::from_pem(
        read(&tls.cert_pem)?,
        read(&tls.key_pem)?,
    ));
    if let Some(ca) = &tls.client_ca_pem {
        cfg = cfg
            .client_ca_root(Certificate::from_pem(read(ca)?))
            .client_auth_optional(false);
    }
    Ok(cfg)
}

async fn sweeper(inner: Arc<Inner>, shutdown: CancellationToken) {
    let tick = (inner.liveness / 4).max(Duration::from_millis(200));
    let mut ticker = tokio::time::interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.cancelled() => return,
        }
        inner.sweep().await;
    }
}

/// Per-run task: evaluate thresholds centrally once per second and keep the
/// snapshot watch fresh even when no batches arrive.
fn spawn_run_ticker(inner: Arc<Inner>, run: Arc<ControllerRun>, shutdown: CancellationToken) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.cancelled() => return,
            }
            inner.publish_live_snapshot(&run);
            if run.state.lock().is_terminal() {
                return;
            }
        }
    });
}

/// Cloneable handle to a running controller, used by the CLI and web UI.
#[derive(Clone)]
pub struct ControllerHandle {
    inner: Arc<Inner>,
    addr: SocketAddr,
    shutdown: CancellationToken,
}

impl ControllerHandle {
    /// The bound listener address (useful with port 0).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Validate a plan, partition it across all matching connected agents and
    /// start it behind a synchronized barrier. Returns the run id.
    pub async fn submit(
        &self,
        plan_yaml: String,
        opts: SubmitOptions,
    ) -> Result<String, AgentError> {
        let load_opts = loadr_config::LoadOptions {
            env: opts.env.clone(),
            check_files: false,
            deny_errors: true,
        };
        let loaded = loadr_config::load_str(&plan_yaml, &load_opts)
            .map_err(|e| AgentError::Config(e.to_string()))?;
        for (path, _) in &opts.files {
            crate::agent::validate_data_file_path(path)?;
        }
        let thresholds = compile_thresholds(&loaded.plan.thresholds).map_err(AgentError::Config)?;
        let scenarios: Vec<String> = loaded.plan.scenarios.keys().cloned().collect();
        let externally_controlled: Vec<String> = loaded
            .plan
            .scenarios
            .iter()
            .filter(|(_, scenario)| {
                matches!(
                    scenario.executor_spec(),
                    Ok(loadr_config::ExecutorSpec::ExternallyControlled { .. })
                )
            })
            .map(|(name, _)| name.clone())
            .collect();
        let name = opts.name.clone().or_else(|| loaded.plan.name.clone());

        // Pick agents: connected, fresh, matching the label filter.
        let mut selected: Vec<(String, AgentSender)> = {
            let agents = self.inner.agents.lock();
            agents
                .iter()
                .filter(|(_, e)| e.connected && e.last_heartbeat.elapsed() <= self.inner.liveness)
                .filter(|(_, e)| match &opts.agent_filter {
                    Some(filter) => filter.iter().all(|(k, v)| e.labels.get(k) == Some(v)),
                    None => true,
                })
                .map(|(id, e)| (id.clone(), e.sender.clone()))
                .collect()
        };
        if selected.is_empty() {
            return Err(AgentError::NoAgents);
        }
        selected.sort_by(|a, b| a.0.cmp(&b.0));

        let run_id = uuid::Uuid::new_v4().to_string();
        let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(Snapshot::default()));
        let run = Arc::new(ControllerRun {
            run_id: run_id.clone(),
            name,
            plan_yaml: plan_yaml.clone(),
            scenarios,
            externally_controlled,
            thresholds,
            on_agent_loss: opts.on_agent_loss,
            assigned: selected.iter().map(|(id, _)| id.clone()).collect(),
            state: Mutex::new(RunState::Pending),
            started_ms: now_unix_ms(),
            finished_ms: Mutex::new(None),
            agg: Mutex::new(Aggregator::new()),
            done: Mutex::new(HashMap::new()),
            lost: Mutex::new(HashSet::new()),
            lost_ever: Mutex::new(HashSet::new()),
            contributing: Mutex::new(HashSet::new()),
            paused: Mutex::new(Some(false)),
            pending_controls: Mutex::new(HashMap::new()),
            summaries: Mutex::new(Vec::new()),
            merged_summary: Mutex::new(None),
            threshold_statuses: Mutex::new(Vec::new()),
            abort_reason: Mutex::new(None),
            snapshot_tx,
            snapshot_rx,
            timeline: Mutex::new(Vec::new()),
            prom_view: Mutex::new(None),
        });
        self.inner.runs.lock().insert(run_id.clone(), run.clone());

        let count = selected.len() as u64;
        let files: Vec<pb::DataFile> = opts
            .files
            .iter()
            .map(|(path, content)| pb::DataFile {
                relative_path: path.clone(),
                content: content.clone(),
            })
            .collect();
        for (index, (agent_id, sender)) in selected.iter().enumerate() {
            let assignment = pb::ControllerMessage {
                msg: Some(CtrlMsg::Assignment(pb::Assignment {
                    run_id: run_id.clone(),
                    plan_yaml: plan_yaml.clone().into_bytes(),
                    partition_index: index as u64,
                    partition_count: count,
                    files: files.clone(),
                    env: opts.env.clone().unwrap_or_default(),
                })),
            };
            if sender.send(Ok(assignment)).await.is_err() {
                tracing::warn!(agent = %agent_id, run_id = %run_id, "assignment send failed");
            }
        }

        // Synchronized start barrier.
        let start_unix_ms = now_unix_ms() as i64 + opts.start_barrier.as_millis() as i64;
        for (agent_id, sender) in &selected {
            let start = pb::ControllerMessage {
                msg: Some(CtrlMsg::Start(pb::Start {
                    run_id: run_id.clone(),
                    start_unix_ms,
                })),
            };
            if sender.send(Ok(start)).await.is_err() {
                tracing::warn!(agent = %agent_id, run_id = %run_id, "start send failed");
            }
        }
        spawn_run_ticker(self.inner.clone(), run, self.shutdown.clone());
        Ok(run_id)
    }

    /// Graceful stop on every assigned agent.
    pub async fn stop_run(&self, run_id: &str) -> Result<(), AgentError> {
        self.control(run_id, "stop", "", None).await
    }

    /// Immediate abort on every assigned agent.
    pub async fn kill_run(&self, run_id: &str) -> Result<(), AgentError> {
        self.control(run_id, "kill", "", None).await
    }

    /// Pause or resume on every assigned agent.
    pub async fn pause_run(&self, run_id: &str, paused: bool) -> Result<(), AgentError> {
        let action = if paused { "pause" } else { "resume" };
        self.control(run_id, action, "", None).await
    }

    /// Scale an externally-controlled scenario to `vus_total` across the
    /// run's surviving agents (remainder to the lowest partition indices).
    pub async fn scale(
        &self,
        run_id: &str,
        scenario: &str,
        vus_total: u64,
    ) -> Result<(), AgentError> {
        self.control(run_id, "scale", scenario, Some(vus_total))
            .await
    }

    async fn control(
        &self,
        run_id: &str,
        action: &str,
        scenario: &str,
        vus_total: Option<u64>,
    ) -> Result<(), AgentError> {
        let run = self
            .inner
            .runs
            .lock()
            .get(run_id)
            .cloned()
            .ok_or_else(|| AgentError::UnknownRun(run_id.to_string()))?;
        let targets = self.inner.run_senders(&run);
        if targets.is_empty() {
            return Err(AgentError::NoAgents);
        }
        if action == "scale"
            && !run
                .externally_controlled
                .iter()
                .any(|name| name == scenario)
        {
            return Err(AgentError::Control(format!(
                "scenario `{scenario}` is not externally controlled (available: {})",
                run.externally_controlled.join(", ")
            )));
        }
        let command_id = self.inner.control_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let notify = Arc::new(Notify::new());
        run.pending_controls.lock().insert(
            command_id,
            PendingControl {
                expected: targets.iter().map(|(id, _)| id.clone()).collect(),
                responses: HashMap::new(),
                action: action.to_string(),
                scenario: scenario.to_string(),
                notify: notify.clone(),
            },
        );
        let shares = vus_total.map(|total| scale_shares(total, targets.len() as u64));
        for (index, (agent_id, sender)) in targets.iter().enumerate() {
            let value = shares
                .as_ref()
                .and_then(|s| s.get(index))
                .copied()
                .unwrap_or(0);
            if sender
                .send(Ok(control_message(
                    run_id, action, scenario, value, command_id,
                )))
                .await
                .is_err()
            {
                if let Some(pending) = run.pending_controls.lock().get_mut(&command_id) {
                    pending.responses.insert(
                        agent_id.clone(),
                        Err("could not deliver command".to_string()),
                    );
                }
            }
        }

        let wait_for_acks = async {
            loop {
                let complete = run
                    .pending_controls
                    .lock()
                    .get(&command_id)
                    .is_none_or(|pending| pending.responses.len() >= pending.expected.len());
                if complete {
                    break;
                }
                notify.notified().await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(3), wait_for_acks)
            .await
            .is_err()
        {
            let pending = run.pending_controls.lock().remove(&command_id);
            let missing = pending
                .map(|pending| {
                    pending
                        .expected
                        .iter()
                        .filter(|agent| !pending.responses.contains_key(*agent))
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if action == "pause" || action == "resume" {
                *run.paused.lock() = None;
            }
            return Err(AgentError::Control(format!(
                "timed out waiting for agent acknowledgement{}",
                if missing.is_empty() {
                    String::new()
                } else {
                    format!(": {}", missing.join(", "))
                }
            )));
        }
        let responses = run
            .pending_controls
            .lock()
            .remove(&command_id)
            .map(|pending| pending.responses)
            .unwrap_or_default();
        let failures: Vec<String> = responses
            .into_iter()
            .filter_map(|(agent, result)| result.err().map(|error| format!("{agent}: {error}")))
            .collect();
        if !failures.is_empty() {
            if action == "pause" || action == "resume" {
                *run.paused.lock() = None;
            }
            return Err(AgentError::Control(failures.join("; ")));
        }
        if action == "pause" || action == "resume" {
            *run.paused.lock() = Some(action == "pause");
        } else if action == "stop" || action == "kill" {
            let mut state = run.state.lock();
            if !state.is_terminal() {
                *state = RunState::Stopping;
            }
        }
        Ok(())
    }

    /// Known agents (including recently disconnected ones).
    pub fn agents(&self) -> Vec<AgentInfo> {
        let liveness = self.inner.liveness;
        self.inner
            .agents
            .lock()
            .iter()
            .map(|(id, e)| AgentInfo {
                id: id.clone(),
                name: e.name.clone(),
                labels: e.labels.clone(),
                cores: e.cores,
                peer_addr: e.peer_addr,
                version: e.version.clone(),
                revision: e.revision.clone(),
                connected_secs: e.connected_at.elapsed().as_secs(),
                last_heartbeat_ms: e.last_heartbeat.elapsed().as_millis() as u64,
                active_vus: e.active_vus,
                healthy: e.connected && e.last_heartbeat.elapsed() <= liveness,
            })
            .collect()
    }

    /// All known runs, newest first.
    pub fn runs(&self) -> Vec<RunSummaryInfo> {
        let mut out: Vec<RunSummaryInfo> = self
            .inner
            .runs
            .lock()
            .values()
            .map(|r| RunSummaryInfo {
                run_id: r.run_id.clone(),
                name: r.name.clone(),
                state: r.state.lock().as_str().to_string(),
                started_ms: r.started_ms,
                ended_ms: *r.finished_ms.lock(),
                agents: r.assigned.clone(),
            })
            .collect();
        out.sort_by(|a, b| {
            b.started_ms
                .cmp(&a.started_ms)
                .then_with(|| a.run_id.cmp(&b.run_id))
        });
        out
    }

    /// Live merged snapshots for a run (recomputed centrally, ≥250ms apart).
    pub fn watch_run(&self, run_id: &str) -> Option<watch::Receiver<Arc<Snapshot>>> {
        self.inner
            .runs
            .lock()
            .get(run_id)
            .map(|r| r.snapshot_rx.clone())
    }

    /// Cumulative detailed series plus exact all-tags fleet aggregates for a
    /// run, read under a single aggregator lock. Once the run is terminal the
    /// view is computed once and the cached `Arc` is returned from then on,
    /// so callers can cheaply detect "nothing changed" by pointer equality.
    pub fn run_metrics_view(&self, run_id: &str) -> Option<Arc<RunMetricsView>> {
        let run = self.inner.runs.lock().get(run_id).cloned()?;
        let state = *run.state.lock();
        if state.is_terminal() {
            if let Some(view) = run.prom_view.lock().clone() {
                return Some(view);
            }
        }
        let (detailed, fleet) = {
            let mut agg = run.agg.lock();
            let detailed = agg.cumulative_snapshot();
            let fleet = agg
                .aggregate_all()
                .into_iter()
                .map(|(metric, kind, values)| FleetMetric {
                    metric,
                    kind,
                    agg: values,
                })
                .collect();
            (detailed, fleet)
        };
        let view = Arc::new(RunMetricsView {
            run_id: run.run_id.clone(),
            name: run.name.clone(),
            state: state.as_str().to_string(),
            started_ms: run.started_ms,
            detailed,
            fleet,
        });
        if state.is_terminal() {
            *run.prom_view.lock() = Some(view.clone());
        }
        Some(view)
    }

    /// Exact cumulative aggregates for fleet, scenario, and agent views.
    pub fn run_aggregate_snapshot(&self, run_id: &str) -> Option<Snapshot> {
        let run = self.inner.runs.lock().get(run_id).cloned()?;
        let snapshot = run
            .agg
            .lock()
            .aggregate_snapshot(&[&["scenario"], &["loadr_agent", "loadr_agent_id"]]);
        Some(snapshot)
    }

    pub fn run_operational_info(&self, run_id: &str) -> Option<RunOperationalInfo> {
        let run = self.inner.runs.lock().get(run_id).cloned()?;
        let mut contributing: Vec<String> = run.contributing.lock().iter().cloned().collect();
        let mut completed: Vec<String> = run.done.lock().keys().cloned().collect();
        let mut lost: Vec<String> = run.lost_ever.lock().iter().cloned().collect();
        contributing.sort();
        completed.sort();
        lost.sort();
        let paused = *run.paused.lock();
        Some(RunOperationalInfo {
            scenarios: run.scenarios.clone(),
            externally_controlled: run.externally_controlled.clone(),
            assigned: run.assigned.clone(),
            contributing,
            completed,
            lost,
            on_agent_loss: run.on_agent_loss,
            paused,
        })
    }

    /// Centrally evaluated threshold statuses for a run.
    pub fn run_thresholds(&self, run_id: &str) -> Vec<ThresholdStatus> {
        self.inner
            .runs
            .lock()
            .get(run_id)
            .map(|r| r.threshold_statuses.lock().clone())
            .unwrap_or_default()
    }

    /// Per-agent summaries reported so far for a run.
    pub fn run_agent_summaries(&self, run_id: &str) -> Vec<Summary> {
        self.inner
            .runs
            .lock()
            .get(run_id)
            .map(|r| r.summaries.lock().clone())
            .unwrap_or_default()
    }

    /// The merged end-of-run summary, built from the central aggregator once
    /// the run reached a terminal state.
    pub fn run_summary(&self, run_id: &str) -> Option<Summary> {
        let run = self.inner.runs.lock().get(run_id).cloned()?;
        if !run.state.lock().is_terminal() {
            return None;
        }
        let summary = run.merged_summary.lock().clone();
        summary
    }

    /// Stop the listener and all background tasks.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod test_support {
    use super::*;

    pub(super) fn test_inner() -> Inner {
        Inner {
            controller_id: "controller-test".to_string(),
            liveness: Duration::from_secs(6),
            agents: Mutex::new(HashMap::new()),
            runs: Mutex::new(HashMap::new()),
            session_counter: AtomicU64::new(0),
            control_counter: AtomicU64::new(0),
        }
    }

    pub(super) fn test_run(run_id: &str, agent_ids: &[&str]) -> Arc<ControllerRun> {
        let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(Snapshot::default()));
        Arc::new(ControllerRun {
            run_id: run_id.to_string(),
            name: None,
            plan_yaml: String::new(),
            scenarios: Vec::new(),
            externally_controlled: Vec::new(),
            thresholds: Vec::new(),
            on_agent_loss: OnAgentLoss::Continue,
            assigned: agent_ids.iter().map(|agent| (*agent).to_string()).collect(),
            state: Mutex::new(RunState::Running),
            started_ms: 0,
            finished_ms: Mutex::new(None),
            agg: Mutex::new(Aggregator::new()),
            done: Mutex::new(HashMap::new()),
            lost: Mutex::new(HashSet::new()),
            lost_ever: Mutex::new(HashSet::new()),
            contributing: Mutex::new(HashSet::new()),
            paused: Mutex::new(Some(false)),
            pending_controls: Mutex::new(HashMap::new()),
            summaries: Mutex::new(Vec::new()),
            merged_summary: Mutex::new(None),
            threshold_statuses: Mutex::new(Vec::new()),
            abort_reason: Mutex::new(None),
            snapshot_tx,
            snapshot_rx,
            timeline: Mutex::new(Vec::new()),
            prom_view: Mutex::new(None),
        })
    }

    pub(super) type Downlink = mpsc::Receiver<Result<pb::ControllerMessage, Status>>;

    /// Register `agent_id` on `session` and hand back its downlink, both to
    /// assert on acknowledgements and to keep the channel open.
    pub(super) fn register(
        inner: &Inner,
        agent_id: &str,
        incarnation: &str,
        session: u64,
    ) -> Downlink {
        let (tx, rx) = mpsc::channel(16);
        inner.register_agent(&registration(agent_id, incarnation), None, tx, session);
        rx
    }

    pub(super) fn registration(agent_id: &str, incarnation: &str) -> pb::Register {
        pb::Register {
            agent_id: agent_id.to_string(),
            agent_name: agent_id.to_string(),
            protocol_version: PROTOCOL_VERSION,
            loadr_version: String::new(),
            cpu_cores: 1,
            labels: HashMap::new(),
            resume_run_id: String::new(),
            build_revision: String::new(),
            incarnation: incarnation.to_string(),
        }
    }

    pub(super) fn heartbeat(active_vus: u64) -> pb::AgentMessage {
        pb::AgentMessage {
            seq: 0,
            msg: Some(AgentMsg::Heartbeat(pb::Heartbeat {
                active_vus,
                cpu_load: 0.0,
                run_id: String::new(),
                run_state: "running".to_string(),
            })),
        }
    }

    /// A metrics batch carrying one `http_reqs` increment of `count`.
    pub(super) fn metrics(run_id: &str, seq: u64, count: u64) -> pb::AgentMessage {
        let mut agg = Aggregator::new();
        for _ in 0..count {
            agg.record(&loadr_core::Sample {
                metric: Arc::from("http_reqs"),
                kind: loadr_core::MetricKind::Counter,
                value: 1.0,
                tags: Arc::new(loadr_core::Tags::new()),
                timestamp_ms: now_unix_ms(),
            });
        }
        metrics_batch(run_id, seq, &agg.take_delta())
    }

    /// A metrics batch carrying `delta` verbatim, for tests that need a
    /// particular series rather than a counter increment.
    pub(super) fn metrics_batch(run_id: &str, seq: u64, delta: &MetricsDelta) -> pb::AgentMessage {
        pb::AgentMessage {
            seq,
            msg: Some(AgentMsg::Metrics(pb::MetricsBatch {
                run_id: run_id.to_string(),
                delta_json: serde_json::to_vec(delta).expect("delta json"),
            })),
        }
    }

    pub(super) fn run_event(run_id: &str, seq: u64, kind: &str) -> pb::AgentMessage {
        pb::AgentMessage {
            seq,
            msg: Some(AgentMsg::Event(pb::RunEvent {
                run_id: run_id.to_string(),
                kind: kind.to_string(),
                detail: String::new(),
                summary_json: Vec::new(),
            })),
        }
    }

    /// Every acknowledged sequence currently queued on a downlink, in order.
    pub(super) fn drain_acks(rx: &mut Downlink) -> Vec<u64> {
        let mut seqs = Vec::new();
        while let Ok(Ok(cm)) = rx.try_recv() {
            if let Some(CtrlMsg::UplinkAck(ack)) = cm.msg {
                seqs.push(ack.seq);
            }
        }
        seqs
    }

    pub(super) fn http_reqs(run: &ControllerRun) -> f64 {
        run.agg
            .lock()
            .snapshot()
            .series
            .iter()
            .filter(|s| s.metric == "http_reqs")
            .map(|s| s.agg.sum)
            .sum()
    }
}

#[cfg(test)]
mod metrics_tests {
    use super::test_support::*;
    use super::*;
    use loadr_core::aggregate::{SeriesDelta, SeriesDeltaData};
    use loadr_core::Tags;

    #[test]
    fn controller_overwrites_spoofed_agent_identity() {
        let mut tags = Tags::new();
        tags.insert("loadr_agent".into(), "spoofed".into());
        tags.insert("loadr_agent_id".into(), "spoofed-id".into());
        let mut delta = MetricsDelta {
            series: vec![SeriesDelta {
                metric: "http_reqs".into(),
                kind: MetricKind::Counter,
                tags,
                data: SeriesDeltaData::Counter { delta: 1.0 },
            }],
        };
        add_agent_identity(&mut delta, "worker-a", "agent-a-id");
        assert_eq!(delta.series[0].tags["loadr_agent"], "worker-a");
        assert_eq!(delta.series[0].tags["loadr_agent_id"], "agent-a-id");
    }

    #[test]
    fn late_delta_from_lost_agent_cannot_resurrect_live_gauges() {
        let inner = test_inner();
        let _downlink = register(&inner, "agent-a", "proc-1", 1);
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".into(), run.clone());

        let delta = MetricsDelta {
            series: vec![SeriesDelta {
                metric: "vus".into(),
                kind: MetricKind::Gauge,
                tags: Tags::new(),
                data: SeriesDeltaData::Gauge {
                    last: 50.0,
                    min: 0.0,
                    max: 50.0,
                },
            }],
        };
        let fleet_vus = || {
            let (_, values) = run
                .agg
                .lock()
                .aggregate_selector("vus", &[])
                .expect("vus series");
            values.last
        };

        inner.handle_agent_message("agent-a", 1, metrics_batch("run-1", 1, &delta));
        assert_eq!(fleet_vus(), Some(50.0));

        // The liveness sweep declares the agent lost and zeroes its gauges…
        run.lost.lock().insert("agent-a".into());
        inner.zero_agent_live_gauges(&run, "agent-a");
        assert_eq!(fleet_vus(), Some(0.0));

        // …then an in-flight delta lands. It must not resurrect the gauge. A
        // fresh sequence, since a replay of seq 1 would be deduplicated and
        // would prove nothing about the zeroing.
        inner.handle_agent_message("agent-a", 1, metrics_batch("run-1", 2, &delta));
        assert_eq!(fleet_vus(), Some(0.0));
    }

    /// The terminal freeze and a delta's contributing-insert + merge must not
    /// interleave. A sweep-driven finalization used to be able to freeze the
    /// summary between the two writes: the UI then showed "every agent
    /// contributed" beside an empty (all-dash) final table.
    #[test]
    fn post_terminal_delta_cannot_claim_contribution() {
        let (sender, _keepalive) = mpsc::channel(8);
        let inner = Inner {
            controller_id: "ctrl".into(),
            liveness: Duration::from_secs(6),
            agents: Mutex::new(HashMap::new()),
            runs: Mutex::new(HashMap::new()),
            session_counter: AtomicU64::new(0),
            control_counter: AtomicU64::new(0),
        };
        inner.agents.lock().insert(
            "agent-a".into(),
            AgentEntry {
                name: "worker-a".into(),
                labels: HashMap::new(),
                cores: 1,
                connected_at: Instant::now(),
                last_heartbeat: Instant::now(),
                active_vus: 0,
                connected: true,
                session: 1,
                sender,
            },
        );
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".into(), run.clone());

        // The sweep-abort shape: the run finalizes while the survivors' first
        // flush is still in flight.
        inner.finalize_run(&run, RunState::Failed);

        let delta = MetricsDelta {
            series: vec![SeriesDelta {
                metric: "http_reqs".into(),
                kind: MetricKind::Counter,
                tags: Tags::new(),
                data: SeriesDeltaData::Counter { delta: 40.0 },
            }],
        };
        let batch = pb::AgentMessage {
            msg: Some(AgentMsg::Metrics(pb::MetricsBatch {
                run_id: "run-1".into(),
                delta_json: serde_json::to_vec(&delta).expect("delta json"),
            })),
        };
        inner.handle_agent_message("agent-a", batch);

        assert!(
            run.contributing.lock().is_empty(),
            "a post-freeze delta must not mark its agent as contributing"
        );
        let frozen = run
            .merged_summary
            .lock()
            .clone()
            .expect("finalization froze a summary");
        assert!(
            frozen.metrics.iter().all(|m| m.metric != "http_reqs"),
            "the frozen summary must not gain data after the freeze"
        );
    }

    #[test]
    fn controller_tick_publishes_one_complete_fleet_interval() {
        let inner = test_inner();
        let mut downlinks = Vec::new();
        for agent_id in ["agent-a", "agent-b"] {
            downlinks.push(register(&inner, agent_id, "proc-1", 1));
        }
        let run = test_run("run-1", &["agent-a", "agent-b"]);
        inner.runs.lock().insert("run-1".into(), run.clone());
        let delta = MetricsDelta {
            series: vec![
                SeriesDelta {
                    metric: "http_reqs".into(),
                    kind: MetricKind::Counter,
                    tags: Tags::new(),
                    data: SeriesDeltaData::Counter { delta: 26.0 },
                },
                SeriesDelta {
                    metric: "http_req_failed".into(),
                    kind: MetricKind::Rate,
                    tags: Tags::new(),
                    data: SeriesDeltaData::Rate {
                        passes: 8,
                        total: 26,
                    },
                },
            ],
        };

        inner.handle_agent_message("agent-a", 1, metrics_batch("run-1", 1, &delta));
        inner.handle_agent_message("agent-b", 1, metrics_batch("run-1", 1, &delta));
        assert!(
            run.snapshot_rx.borrow().series.is_empty(),
            "agent arrivals must not roll a partial fleet interval"
        );

        inner.publish_live_snapshot(&run);
        let snapshot = run.snapshot_rx.borrow().clone();
        assert_eq!(snapshot.interval_request_count(), 52);
        assert_eq!(snapshot.interval_count("http_req_failed"), 52);
        let failed = snapshot
            .series
            .iter()
            .filter(|series| series.metric == "http_req_failed")
            .map(|series| series.interval_sum)
            .sum::<f64>();
        assert_eq!(failed, 16.0);
    }

    #[test]
    fn terminal_summary_is_frozen_at_finalization() {
        let inner = Arc::new(test_inner());
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".into(), run.clone());
        run.agg.lock().merge_delta(&MetricsDelta {
            series: vec![SeriesDelta {
                metric: "http_reqs".into(),
                kind: MetricKind::Counter,
                tags: Tags::new(),
                data: SeriesDeltaData::Counter { delta: 10.0 },
            }],
        });
        inner.finalize_run(&run, RunState::Finished);
        let first = run.merged_summary.lock().clone().expect("summary");
        std::thread::sleep(Duration::from_millis(5));
        let second = run.merged_summary.lock().clone().expect("summary");

        assert_eq!(first.ended_ms, second.ended_ms);
        assert_eq!(first.duration_secs, second.duration_secs);
        assert_eq!(
            first.metrics[0].agg.per_second,
            second.metrics[0].agg.per_second
        );
    }

    #[test]
    fn first_started_event_ends_pending_state() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        *run.state.lock() = RunState::Pending;
        inner.runs.lock().insert("run-1".into(), run.clone());

        inner.handle_run_event(
            "agent-a",
            pb::RunEvent {
                run_id: "run-1".into(),
                kind: "started".into(),
                detail: String::new(),
                summary_json: Vec::new(),
            },
        );

        assert_eq!(*run.state.lock(), RunState::Running);
    }
}

#[cfg(test)]
mod registration_tests {
    use super::test_support::*;
    use super::*;

    /// A registration carrying the build metadata the UI surfaces.
    fn with_build_metadata(agent_id: &str, version: &str, revision: &str) -> pb::Register {
        pb::Register {
            loadr_version: version.to_string(),
            build_revision: revision.to_string(),
            cpu_cores: 8,
            ..registration(agent_id, "proc-1")
        }
    }

    fn sender() -> AgentSender {
        mpsc::channel(1).0
    }

    #[test]
    fn registration_accepts_missing_debug_metadata() {
        let inner = test_inner();
        inner.register_agent(&with_build_metadata("agent-a", "", ""), None, sender(), 1);

        let agents = inner.agents.lock();
        let agent = agents.get("agent-a").expect("registered agent");
        assert_eq!(agent.peer_addr, None);
        assert_eq!(agent.version, None);
        assert_eq!(agent.revision, None);
    }

    #[test]
    fn same_id_replaces_peer_and_build_metadata() {
        let inner = test_inner();
        let first_peer = "10.0.0.4:41000".parse().expect("first peer");
        let second_peer = "10.0.0.4:42000".parse().expect("second peer");

        inner.register_agent(
            &with_build_metadata("agent-a", "1.28.0", "aaaaaaaaaaaa"),
            Some(first_peer),
            sender(),
            1,
        );
        inner.register_agent(
            &with_build_metadata("agent-a", "1.29.0", "bbbbbbbbbbbb"),
            Some(second_peer),
            sender(),
            2,
        );

        let agents = inner.agents.lock();
        assert_eq!(agents.len(), 1);
        let agent = agents.get("agent-a").expect("registered agent");
        assert_eq!(agent.peer_addr, Some(second_peer));
        assert_eq!(agent.version.as_deref(), Some("1.29.0"));
        assert_eq!(agent.revision.as_deref(), Some("bbbbbbbbbbbb"));
    }

    #[test]
    fn reconnect_of_the_same_agent_process_keeps_the_uplink_cursor() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());

        let _first = register(&inner, "agent-a", "proc-1", 1);
        assert!(inner.handle_agent_message("agent-a", 1, metrics("run-1", 1, 4)));

        // Same process, new stream: the cursor survives, so the message it is
        // about to replay is still recognized as a duplicate.
        let _second = register(&inner, "agent-a", "proc-1", 2);
        assert!(inner.handle_agent_message("agent-a", 2, metrics("run-1", 1, 4)));
        assert_eq!(
            http_reqs(&run),
            4.0,
            "a replay across a reconnect of the same process must not be merged twice"
        );
    }

    #[test]
    fn restarted_agent_process_resets_the_uplink_cursor() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());

        let _first = register(&inner, "agent-a", "proc-1", 1);
        for seq in 1..=3 {
            assert!(inner.handle_agent_message("agent-a", 1, metrics("run-1", seq, 1)));
        }

        // A restarted process starts again at sequence 1. Without the
        // incarnation reset every one of its messages would look like a
        // duplicate and be discarded — wholesale silent loss.
        let _second = register(&inner, "agent-a", "proc-2", 2);
        assert!(inner.handle_agent_message("agent-a", 2, metrics("run-1", 1, 5)));
        assert_eq!(
            http_reqs(&run),
            8.0,
            "a restarted agent's fresh sequence space must be accepted"
        );
    }

    #[test]
    fn superseded_session_receives_an_abort_status() {
        let inner = test_inner();
        let mut first = register(&inner, "agent-a", "proc-1", 1);
        let _second = register(&inner, "agent-a", "proc-1", 2);

        let status = first
            .try_recv()
            .expect("the superseded downlink carries an item")
            .expect_err("the item terminates the stream");
        assert_eq!(status.code(), tonic::Code::Aborted);
    }

    #[test]
    fn same_id_replaces_the_registered_entry() {
        let inner = test_inner();
        let _first = register(&inner, "agent-a", "proc-1", 1);
        let _second = register(&inner, "agent-a", "proc-2", 2);

        let agents = inner.agents.lock();
        assert_eq!(agents.len(), 1);
        let entry = agents.get("agent-a").expect("the agent is registered");
        assert_eq!(entry.session, 2);
        assert_eq!(entry.incarnation, "proc-2");
    }

    #[test]
    fn resume_claim_from_an_assigned_agent_clears_the_lost_marker() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        run.lost.lock().insert("agent-a".to_string());
        inner.runs.lock().insert("run-1".to_string(), run.clone());

        let mut reg = registration("agent-a", "proc-1");
        reg.resume_run_id = "run-1".to_string();
        inner.register_agent(&reg, None, sender(), 1);

        assert!(run.lost.lock().is_empty(), "the agent is counted again");
    }

    #[test]
    fn resume_claim_for_a_run_this_agent_was_never_assigned_is_refused() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-b"]);
        run.lost.lock().insert("agent-b".to_string());
        inner.runs.lock().insert("run-1".to_string(), run.clone());

        let mut reg = registration("agent-a", "proc-1");
        reg.resume_run_id = "run-1".to_string();
        inner.register_agent(&reg, None, sender(), 1);

        assert_eq!(
            run.lost.lock().len(),
            1,
            "an unassigned agent's claim must not touch the run's loss accounting"
        );
    }

    #[test]
    fn resume_claim_for_a_terminal_run_is_refused() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        run.lost.lock().insert("agent-a".to_string());
        *run.state.lock() = RunState::Finished;
        inner.runs.lock().insert("run-1".to_string(), run.clone());

        let mut reg = registration("agent-a", "proc-1");
        reg.resume_run_id = "run-1".to_string();
        inner.register_agent(&reg, None, sender(), 1);

        assert_eq!(
            run.lost.lock().len(),
            1,
            "a finished run's completeness record is already settled"
        );
    }
}

#[cfg(test)]
mod session_fencing_tests {
    use super::test_support::*;
    use super::*;

    /// Register two sessions for one agent id. Returns the stale downlink and
    /// the live one.
    fn two_sessions(inner: &Inner, agent_id: &str) -> (Downlink, Downlink) {
        let stale = register(inner, agent_id, "proc-1", 1);
        let live = register(inner, agent_id, "proc-1", 2);
        (stale, live)
    }

    #[test]
    fn stale_session_heartbeat_cannot_suppress_loss_detection() {
        let inner = test_inner();
        let (_stale, _live) = two_sessions(&inner, "agent-a");
        // Backdate the live entry so the liveness sweep is already about to
        // declare it lost.
        inner
            .agents
            .lock()
            .get_mut("agent-a")
            .expect("registered")
            .last_heartbeat = Instant::now() - Duration::from_secs(30);

        assert!(!inner.handle_agent_message("agent-a", 1, heartbeat(7)));

        let agents = inner.agents.lock();
        let entry = agents.get("agent-a").expect("registered");
        assert!(
            entry.last_heartbeat.elapsed() > inner.liveness,
            "a superseded stream must not vouch for the live session's liveness"
        );
        assert_eq!(entry.active_vus, 0, "nor report its VU count");
    }

    #[test]
    fn stale_session_metrics_are_not_merged() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());
        let (_stale, _live) = two_sessions(&inner, "agent-a");

        assert!(!inner.handle_agent_message("agent-a", 1, metrics("run-1", 1, 9)));
        assert_eq!(http_reqs(&run), 0.0);

        assert!(inner.handle_agent_message("agent-a", 2, metrics("run-1", 1, 9)));
        assert_eq!(http_reqs(&run), 9.0, "the live session is still served");
    }

    #[test]
    fn stale_session_terminal_event_cannot_finalize_a_run() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());
        let (_stale, _live) = two_sessions(&inner, "agent-a");

        assert!(!inner.handle_agent_message("agent-a", 1, run_event("run-1", 1, "finished")));
        assert!(run.done.lock().is_empty());
        assert!(
            !run.state.lock().is_terminal(),
            "a superseded stream must not complete the run"
        );
    }

    #[test]
    fn stale_session_message_is_never_acknowledged() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());
        let (mut stale, mut live) = two_sessions(&inner, "agent-a");

        assert!(!inner.handle_agent_message("agent-a", 1, metrics("run-1", 1, 3)));
        assert!(
            drain_acks(&mut stale).is_empty() && drain_acks(&mut live).is_empty(),
            "the agent's replay window is shared across its sessions, so acknowledging \
             a message the controller declined to apply would lose it for good"
        );
    }

    #[test]
    fn stale_session_traffic_stops_the_inbound_pump() {
        let inner = test_inner();
        let (_stale, _live) = two_sessions(&inner, "agent-a");
        assert!(
            !inner.handle_agent_message("agent-a", 1, heartbeat(0)),
            "a superseded session is permanently superseded, so its pump should stop"
        );
        assert!(inner.handle_agent_message("agent-a", 2, heartbeat(0)));
    }

    #[test]
    fn replayed_metric_delta_merges_exactly_once() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());
        let _live = register(&inner, "agent-a", "proc-1", 1);

        for _ in 0..3 {
            assert!(inner.handle_agent_message("agent-a", 1, metrics("run-1", 1, 6)));
        }
        assert_eq!(
            http_reqs(&run),
            6.0,
            "metric merging is additive, so a replay must be discarded"
        );
    }

    #[test]
    fn replayed_terminal_event_records_one_agent_summary() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a", "agent-b"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());
        let _live = register(&inner, "agent-a", "proc-1", 1);

        let mut finished = run_event("run-1", 1, "finished");
        let summary = {
            let mut agg = Aggregator::new();
            Summary::build(
                None,
                "run-1".to_string(),
                now_unix_ms(),
                vec!["default".to_string()],
                &mut agg,
                Vec::new(),
                None,
                Vec::new(),
            )
        };
        if let Some(AgentMsg::Event(ev)) = finished.msg.as_mut() {
            ev.summary_json = serde_json::to_vec(&summary).expect("summary json");
        }

        assert!(inner.handle_agent_message("agent-a", 1, finished.clone()));
        assert!(inner.handle_agent_message("agent-a", 1, finished));
        assert_eq!(
            run.summaries.lock().len(),
            1,
            "a double-counted agent summary would inflate the fleet report"
        );
        assert_eq!(run.done.lock().len(), 1);
    }

    #[test]
    fn duplicate_uplink_message_is_acknowledged_again() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());
        let mut live = register(&inner, "agent-a", "proc-1", 1);

        assert!(inner.handle_agent_message("agent-a", 1, metrics("run-1", 1, 1)));
        assert_eq!(drain_acks(&mut live), vec![1]);

        assert!(inner.handle_agent_message("agent-a", 1, metrics("run-1", 1, 1)));
        assert_eq!(
            drain_acks(&mut live),
            vec![1],
            "an unacknowledged duplicate would be replayed forever and wedge the window"
        );
    }

    #[test]
    fn uplink_acknowledgement_carries_the_controller_cursor() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());
        let mut live = register(&inner, "agent-a", "proc-1", 1);

        for seq in 1..=3 {
            assert!(inner.handle_agent_message("agent-a", 1, metrics("run-1", seq, 1)));
        }
        assert_eq!(drain_acks(&mut live), vec![1, 2, 3]);

        assert!(inner.handle_agent_message("agent-a", 1, metrics("run-1", 2, 1)));
        assert_eq!(
            drain_acks(&mut live),
            vec![3],
            "a replay is answered with the cursor, not the replayed sequence, so one \
             round trip retires the whole acknowledged prefix"
        );
    }

    #[test]
    fn sequence_gap_advances_the_cursor_rather_than_wedging_the_agent() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());
        let mut live = register(&inner, "agent-a", "proc-1", 1);

        assert!(inner.handle_agent_message("agent-a", 1, metrics("run-1", 7, 2)));
        assert_eq!(http_reqs(&run), 2.0, "a gap is accepted, not refused");
        assert_eq!(drain_acks(&mut live), vec![7]);
    }

    #[test]
    fn idle_heartbeat_readvertises_the_uplink_cursor() {
        let inner = test_inner();
        let run = test_run("run-1", &["agent-a"]);
        inner.runs.lock().insert("run-1".to_string(), run.clone());
        let mut live = register(&inner, "agent-a", "proc-1", 1);

        assert!(inner.handle_agent_message("agent-a", 1, metrics("run-1", 1, 1)));
        assert_eq!(drain_acks(&mut live), vec![1]);

        // The agent's acknowledgement went missing. An idle agent sends nothing
        // but heartbeats, so that is what has to unpin its window.
        assert!(inner.handle_agent_message("agent-a", 1, heartbeat(0)));
        assert_eq!(drain_acks(&mut live), vec![1]);
    }
}
