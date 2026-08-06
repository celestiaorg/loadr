//! Shared scaffolding for the distributed integration tests.
//!
//! Each integration test file compiles its own copy of this module, so items
//! only one of them uses would otherwise warn.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use loadr_agent::pb;
use loadr_agent::pb::coordination_client::CoordinationClient;
use loadr_agent::{
    Agent, AgentConfig, AgentTls, Controller, ControllerConfig, ControllerHandle, RunnerDeps,
    SubmitOptions, PROTOCOL_VERSION,
};
use loadr_core::{
    PreparedRequest, ProtocolError, ProtocolHandler, ProtocolRegistry, ProtocolResponse, Timings,
    VuContext,
};
use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Mock protocol + agent/controller harness
// ---------------------------------------------------------------------------

/// A mock gRPC protocol handler: sleeps 1–5ms and returns OK with timings.
pub struct MockGrpcHandler {
    counter: AtomicU64,
}

#[async_trait::async_trait]
impl ProtocolHandler for MockGrpcHandler {
    fn name(&self) -> &str {
        "grpc"
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let ms = 1 + (n % 5);
        tokio::time::sleep(Duration::from_millis(ms)).await;
        let d = ms as f64;
        Ok(ProtocolResponse {
            status: 0,
            status_text: "OK".to_string(),
            protocol_version: "gRPC".to_string(),
            timings: Timings {
                waiting_ms: d,
                duration_ms: d,
                ..Default::default()
            },
            bytes_sent: 100,
            bytes_received: 256,
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

/// A mock `data_source` plugin: counts `init`/`next_row` calls and emits rows
/// with a single `tick` column.
pub struct MockDataSource {
    inits: Arc<AtomicU64>,
    rows: Arc<AtomicU64>,
}

impl loadr_core::DataSourcePlugin for MockDataSource {
    fn name(&self) -> &str {
        "fake"
    }

    fn init(
        &mut self,
        source_configs: &indexmap::IndexMap<String, serde_json::Value>,
    ) -> Result<(), String> {
        assert!(
            source_configs.contains_key("gen"),
            "data.gen config delivered to the plugin"
        );
        self.inits.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn next_row(
        &self,
        _ctx: &loadr_core::PluginRowCtx<'_>,
    ) -> Result<loadr_core::PluginRowResult, String> {
        let n = self.rows.fetch_add(1, Ordering::Relaxed);
        let mut row = indexmap::IndexMap::new();
        row.insert("tick".to_string(), n.to_string());
        Ok(loadr_core::PluginRowResult::Row(row))
    }
}

pub fn mock_deps() -> RunnerDeps {
    RunnerDeps {
        protocols: Arc::new(|_plan, _base_dir| {
            let mut registry = ProtocolRegistry::new();
            registry.register(Arc::new(MockGrpcHandler {
                counter: AtomicU64::new(0),
            }));
            Ok(registry)
        }),
        script: None,
        data_sources: None,
    }
}

/// `mock_deps` plus a data-source factory that backs every declared plugin
/// with a [`MockDataSource`] reporting into the shared counters.
pub fn mock_deps_with_data_sources(inits: &Arc<AtomicU64>, rows: &Arc<AtomicU64>) -> RunnerDeps {
    let inits = Arc::clone(inits);
    let rows = Arc::clone(rows);
    RunnerDeps {
        data_sources: Some(Arc::new(move |plugin_refs, _base_dir| {
            let mut sources: HashMap<String, Box<dyn loadr_core::DataSourcePlugin>> =
                HashMap::new();
            for plugin_ref in plugin_refs {
                if !plugin_ref.enabled {
                    continue;
                }
                sources.insert(
                    plugin_ref.name.clone(),
                    Box::new(MockDataSource {
                        inits: Arc::clone(&inits),
                        rows: Arc::clone(&rows),
                    }),
                );
            }
            Ok(sources)
        })),
        ..mock_deps()
    }
}

pub fn localhost0() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

pub fn temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("loadr-agent-test-{tag}-{}", uuid::Uuid::new_v4()))
}

pub fn spawn_agent_with_deps(
    controller_addr: String,
    name: &str,
    agent_id: Option<String>,
    tls: Option<AgentTls>,
    deps: RunnerDeps,
) -> CancellationToken {
    let token = CancellationToken::new();
    let config = AgentConfig {
        controller_addr,
        agent_id,
        agent_name: name.to_string(),
        labels: HashMap::new(),
        tls,
        work_dir: temp_dir(name),
        deps,
    };
    let child = token.clone();
    tokio::spawn(async move {
        let _ = Agent::run(config, child).await;
    });
    token
}

pub fn spawn_agent(
    controller_addr: String,
    name: &str,
    agent_id: Option<String>,
    tls: Option<AgentTls>,
) -> CancellationToken {
    spawn_agent_with_deps(controller_addr, name, agent_id, tls, mock_deps())
}

pub async fn start_controller(liveness: Duration) -> ControllerHandle {
    Controller::start(ControllerConfig {
        bind: localhost0(),
        tls: None,
        agent_liveness: liveness,
    })
    .await
    .expect("controller start")
}

pub async fn wait_until<F: FnMut() -> bool>(mut cond: F, timeout: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !cond() {
        assert!(
            tokio::time::Instant::now() <= deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub fn run_state(handle: &ControllerHandle, run_id: &str) -> String {
    handle
        .runs()
        .into_iter()
        .find(|r| r.run_id == run_id)
        .map(|r| r.state)
        .unwrap_or_default()
}

pub fn is_terminal(state: &str) -> bool {
    matches!(state, "finished" | "aborted" | "failed")
}

pub fn quick_submit() -> SubmitOptions {
    SubmitOptions {
        start_barrier: Duration::from_millis(300),
        ..Default::default()
    }
}

/// The merged fleet total for `metric` in a terminal run's summary.
pub fn summary_metric_sum(handle: &ControllerHandle, run_id: &str, metric: &str) -> f64 {
    handle
        .run_summary(run_id)
        .expect("the run is terminal, so a merged summary exists")
        .metrics
        .iter()
        .find(|m| m.metric == metric)
        .unwrap_or_else(|| panic!("{metric} in the merged summary"))
        .agg
        .sum
}

// ---------------------------------------------------------------------------
// Fault injection: a TCP relay that can be cut without killing the agent
// ---------------------------------------------------------------------------

/// A TCP relay whose live connections can be severed on demand.
///
/// This is the only way to break an agent's stream *without* killing the agent
/// process: a restart mints a fresh uplink incarnation, which resets the
/// controller's deduplication cursor and so never exercises replay.
pub struct SeverableProxy {
    addr: SocketAddr,
    /// Cancelled and replaced to cut every connection currently relayed.
    live: Arc<Mutex<CancellationToken>>,
    /// Connections accepted so far, so a test can assert that the reconnects it
    /// meant to provoke actually happened.
    accepted: Arc<AtomicU64>,
}

impl SeverableProxy {
    pub async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind(localhost0()).await.expect("proxy bind");
        let addr = listener.local_addr().expect("proxy addr");
        let live = Arc::new(Mutex::new(CancellationToken::new()));
        let accepted = Arc::new(AtomicU64::new(0));
        let accepting = live.clone();
        let counter = accepted.clone();
        tokio::spawn(async move {
            while let Ok((mut downstream, _)) = listener.accept().await {
                let cut = accepting.lock().clone();
                counter.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let Ok(mut upstream) = TcpStream::connect(upstream).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream) => {}
                        // Dropping both halves closes the sockets, which is what
                        // the peers observe as a broken stream.
                        _ = cut.cancelled() => {}
                    }
                });
            }
        });
        SeverableProxy {
            addr,
            live,
            accepted,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// How many connections have been relayed, including the current one.
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }

    /// Cut every connection currently relayed. The listener keeps accepting, so
    /// a reconnect through the same address succeeds immediately.
    pub fn sever(&self) {
        let mut live = self.live.lock();
        live.cancel();
        *live = CancellationToken::new();
    }
}

// ---------------------------------------------------------------------------
// A hand-driven agent session, for asserting on the wire protocol itself
// ---------------------------------------------------------------------------

/// An agent session driven message by message over real gRPC.
///
/// Lets a test choose sequence numbers and incarnations directly, which the
/// production agent deliberately never allows — and needs no test hooks in it.
pub struct FakeAgent {
    uplink: mpsc::Sender<pb::AgentMessage>,
    inbound: tonic::Streaming<pb::ControllerMessage>,
}

impl FakeAgent {
    pub async fn connect(controller: SocketAddr, agent_id: &str, incarnation: &str) -> Self {
        let mut client = CoordinationClient::connect(format!("http://{controller}"))
            .await
            .expect("connect to the controller");
        let (uplink, rx) = mpsc::channel(8);
        uplink
            .send(pb::AgentMessage {
                seq: 0,
                msg: Some(pb::agent_message::Msg::Register(pb::Register {
                    agent_id: agent_id.to_string(),
                    agent_name: agent_id.to_string(),
                    protocol_version: PROTOCOL_VERSION,
                    loadr_version: "fake".to_string(),
                    cpu_cores: 1,
                    labels: HashMap::new(),
                    resume_run_id: String::new(),
                    build_revision: "fake".to_string(),
                    incarnation: incarnation.to_string(),
                })),
            })
            .await
            .expect("queue register");
        let inbound = client
            .session(ReceiverStream::new(rx))
            .await
            .expect("open session")
            .into_inner();
        let mut agent = FakeAgent { uplink, inbound };
        // The controller answers registration before anything else.
        let welcome = agent.next().await.expect("a Registered reply");
        assert!(
            matches!(
                welcome,
                Ok(pb::ControllerMessage {
                    msg: Some(pb::controller_message::Msg::Registered(_))
                })
            ),
            "expected Registered, got {welcome:?}"
        );
        agent
    }

    pub async fn send(&self, msg: pb::AgentMessage) {
        self.uplink.send(msg).await.expect("send to the controller");
    }

    /// The next controller message, or `None` if none arrives within a second.
    pub async fn next(&mut self) -> Option<Result<pb::ControllerMessage, tonic::Status>> {
        match tokio::time::timeout(Duration::from_secs(1), self.inbound.message()).await {
            Ok(Ok(Some(msg))) => Some(Ok(msg)),
            Ok(Ok(None)) => None,
            Ok(Err(status)) => Some(Err(status)),
            Err(_elapsed) => None,
        }
    }

    /// Every uplink acknowledgement the controller sends within a second,
    /// skipping the assignment and start traffic a submitted run produces.
    pub async fn acks(&mut self) -> Vec<u64> {
        let mut seqs = Vec::new();
        while let Some(Ok(cm)) = self.next().await {
            if let Some(pb::controller_message::Msg::UplinkAck(ack)) = cm.msg {
                seqs.push(ack.seq);
            }
        }
        seqs
    }

    /// Wait for the stream to end and report the terminating status, if any.
    pub async fn wait_for_end(&mut self) -> Option<tonic::Status> {
        while let Some(item) = self.next().await {
            match item {
                Ok(_) => continue,
                Err(status) => return Some(status),
            }
        }
        None
    }
}

/// A metrics batch carrying `count` `http_reqs` increments at sequence `seq`.
pub fn metrics_batch(run_id: &str, seq: u64, count: u64) -> pb::AgentMessage {
    let mut agg = loadr_core::Aggregator::new();
    for _ in 0..count {
        agg.record(&loadr_core::Sample {
            metric: Arc::from("http_reqs"),
            kind: loadr_core::MetricKind::Counter,
            value: 1.0,
            tags: Arc::new(loadr_core::Tags::new()),
            timestamp_ms: 0,
        });
    }
    pb::AgentMessage {
        seq,
        msg: Some(pb::agent_message::Msg::Metrics(pb::MetricsBatch {
            run_id: run_id.to_string(),
            delta_json: serde_json::to_vec(&agg.take_delta()).expect("delta json"),
        })),
    }
}

pub fn run_event(run_id: &str, seq: u64, kind: &str) -> pb::AgentMessage {
    pb::AgentMessage {
        seq,
        msg: Some(pb::agent_message::Msg::Event(pb::RunEvent {
            run_id: run_id.to_string(),
            kind: kind.to_string(),
            detail: String::new(),
            summary_json: Vec::new(),
        })),
    }
}

/// A minimal plan the controller can compile and assign. The fake agent ignores
/// the assignment; the plan only has to exist so a run does.
pub const MINIMAL_PLAN: &str = r#"
name: fake-agent-run
scenarios:
  s:
    executor: shared-iterations
    vus: 1
    iterations: 1
    flow:
      - request:
          url: grpc://mock.local
          grpc:
            reflection: true
            service: loadr.test.Mock
            method: Call
            message: {}
"#;
