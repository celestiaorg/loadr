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
use loadr_core::{Aggregator, MetricsDelta, Snapshot, Summary, ThresholdStatus, TimelinePoint};
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
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
            agent_liveness: Duration::from_secs(6),
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
    /// pending | running | finished | aborted | failed
    pub state: String,
    pub started_ms: u64,
    pub agents: Vec<String>,
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
    lost: Mutex<HashSet<String>>,
    /// Per-agent summaries as reported.
    summaries: Mutex<Vec<Summary>>,
    threshold_statuses: Mutex<Vec<ThresholdStatus>>,
    abort_reason: Mutex<Option<String>>,
    snapshot_tx: watch::Sender<Arc<Snapshot>>,
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
    last_recompute: Mutex<Instant>,
    /// Per-interval time series for the HTML report, sampled ~once a second
    /// from the centrally merged snapshot.
    timeline: Mutex<Vec<TimelinePoint>>,
    last_timeline: Mutex<Instant>,
}

struct Inner {
    controller_id: String,
    liveness: Duration,
    agents: Mutex<HashMap<String, AgentEntry>>,
    runs: Mutex<HashMap<String, Arc<ControllerRun>>>,
    session_counter: AtomicU64,
}

impl Inner {
    fn register_agent(&self, reg: &pb::Register, sender: AgentSender, session: u64) {
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
                    Ok(delta) => {
                        run.agg.lock().merge_delta(&delta);
                        self.maybe_recompute(&run);
                    }
                    Err(e) => {
                        tracing::warn!(run_id = %batch.run_id, error = %e, "bad metrics delta");
                    }
                }
            }
            Some(AgentMsg::Event(ev)) => self.handle_run_event(agent_id, ev),
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
        let became_terminal = {
            let mut state = run.state.lock();
            if state.is_terminal() {
                false
            } else {
                *state = final_state;
                true
            }
        };
        if !became_terminal {
            return;
        }
        *run.finished_ms.lock() = Some(now_unix_ms());
        let mut agg = run.agg.lock();
        let (statuses, _) = evaluate_all(&run.thresholds, &agg, agg.elapsed());
        let snapshot = Arc::new(agg.snapshot());
        drop(agg);
        *run.threshold_statuses.lock() = statuses;
        if snapshot.interval_secs > 0.0
            && snapshot
                .series
                .iter()
                .any(|s| s.interval_count > 0 || s.metric == "vus")
        {
            run.timeline
                .lock()
                .push(TimelinePoint::from_snapshot(&snapshot));
        }
        let _ = run.snapshot_tx.send(snapshot);
        tracing::info!(run_id = %run.run_id, state = final_state.as_str(), "run completed");
    }

    /// Recompute the watch snapshot from the central aggregator, throttled to
    /// at most one recompute per 250ms.
    fn maybe_recompute(&self, run: &Arc<ControllerRun>) {
        {
            let mut last = run.last_recompute.lock();
            if last.elapsed() < Duration::from_millis(250) {
                return;
            }
            *last = Instant::now();
        }
        let snapshot = Arc::new(run.agg.lock().snapshot());
        // Sample the timeline ~once a second (the recompute itself is throttled
        // to 250ms, which is too fine-grained for charting).
        {
            let mut last = run.last_timeline.lock();
            if last.elapsed() >= Duration::from_millis(950) {
                *last = Instant::now();
                run.timeline
                    .lock()
                    .push(TimelinePoint::from_snapshot(&snapshot));
            }
        }
        let _ = run.snapshot_tx.send(snapshot);
    }

    /// Senders for the run's assigned agents that are still connected, in
    /// partition order.
    fn run_senders(&self, run: &ControllerRun) -> Vec<(String, AgentSender)> {
        let agents = self.agents.lock();
        run.assigned
            .iter()
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
                            .send(Ok(control_message(&run.run_id, "stop", "", 0)))
                            .await;
                    }
                    self.finalize_run(&run, RunState::Failed);
                }
            }
        }
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
) -> pb::ControllerMessage {
    pb::ControllerMessage {
        msg: Some(CtrlMsg::Control(pb::Control {
            run_id: run_id.to_string(),
            action: action.to_string(),
            scenario: scenario.to_string(),
            value,
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
        self.inner.register_agent(&reg, tx, session);
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
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.cancelled() => return,
            }
            if run.state.lock().is_terminal() {
                return;
            }
            {
                let agg = run.agg.lock();
                let (statuses, _) = evaluate_all(&run.thresholds, &agg, agg.elapsed());
                drop(agg);
                *run.threshold_statuses.lock() = statuses;
            }
            inner.maybe_recompute(&run);
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
            thresholds,
            on_agent_loss: opts.on_agent_loss,
            assigned: selected.iter().map(|(id, _)| id.clone()).collect(),
            state: Mutex::new(RunState::Pending),
            started_ms: now_unix_ms(),
            finished_ms: Mutex::new(None),
            agg: Mutex::new(Aggregator::new()),
            done: Mutex::new(HashMap::new()),
            lost: Mutex::new(HashSet::new()),
            summaries: Mutex::new(Vec::new()),
            threshold_statuses: Mutex::new(Vec::new()),
            abort_reason: Mutex::new(None),
            snapshot_tx,
            snapshot_rx,
            last_recompute: Mutex::new(Instant::now()),
            timeline: Mutex::new(Vec::new()),
            last_timeline: Mutex::new(Instant::now()),
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
        *run.state.lock() = RunState::Running;
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
        let shares = vus_total.map(|total| scale_shares(total, targets.len() as u64));
        for (index, (_, sender)) in targets.iter().enumerate() {
            let value = shares
                .as_ref()
                .and_then(|s| s.get(index))
                .copied()
                .unwrap_or(0);
            let _ = sender
                .send(Ok(control_message(run_id, action, scenario, value)))
                .await;
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
                connected_secs: e.connected_at.elapsed().as_secs(),
                last_heartbeat_ms: e.last_heartbeat.elapsed().as_millis() as u64,
                active_vus: e.active_vus,
                healthy: e.connected && e.last_heartbeat.elapsed() <= liveness,
            })
            .collect()
    }

    /// All known runs, oldest first.
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
                agents: r.assigned.clone(),
            })
            .collect();
        out.sort_by(|a, b| {
            a.started_ms
                .cmp(&b.started_ms)
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
        let thresholds = run.threshold_statuses.lock().clone();
        let aborted = run.abort_reason.lock().clone();
        let timeline = run.timeline.lock().clone();
        let mut agg = run.agg.lock();
        Some(Summary::build(
            run.name.clone(),
            run.run_id.clone(),
            run.started_ms,
            run.scenarios.clone(),
            &mut agg,
            thresholds,
            aborted,
            timeline,
        ))
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
            controller_id: "controller-1".to_string(),
            liveness: Duration::from_secs(5),
            agents: Mutex::new(HashMap::new()),
            runs: Mutex::new(HashMap::new()),
            session_counter: AtomicU64::new(0),
        }
    }

    pub(super) fn test_run(run_id: &str, agent_ids: &[&str]) -> Arc<ControllerRun> {
        let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(Snapshot::default()));
        Arc::new(ControllerRun {
            run_id: run_id.to_string(),
            name: None,
            plan_yaml: String::new(),
            scenarios: vec!["default".to_string()],
            thresholds: Vec::new(),
            on_agent_loss: OnAgentLoss::Continue,
            assigned: agent_ids.iter().map(|a| a.to_string()).collect(),
            state: Mutex::new(RunState::Running),
            started_ms: now_unix_ms(),
            finished_ms: Mutex::new(None),
            agg: Mutex::new(Aggregator::new()),
            done: Mutex::new(HashMap::new()),
            lost: Mutex::new(HashSet::new()),
            summaries: Mutex::new(Vec::new()),
            threshold_statuses: Mutex::new(Vec::new()),
            abort_reason: Mutex::new(None),
            snapshot_tx,
            snapshot_rx,
            last_recompute: Mutex::new(Instant::now()),
            timeline: Mutex::new(Vec::new()),
            last_timeline: Mutex::new(Instant::now()),
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
        inner.register_agent(&registration(agent_id, incarnation), tx, session);
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
        pb::AgentMessage {
            seq,
            msg: Some(AgentMsg::Metrics(pb::MetricsBatch {
                run_id: run_id.to_string(),
                delta_json: serde_json::to_vec(&agg.take_delta()).expect("delta json"),
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
mod registration_tests {
    use super::test_support::*;
    use super::*;

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
        inner.register_agent(&reg, mpsc::channel(4).0, 1);

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
        inner.register_agent(&reg, mpsc::channel(4).0, 1);

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
        inner.register_agent(&reg, mpsc::channel(4).0, 1);

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
