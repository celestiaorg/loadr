//! Uplink durability and session fencing across reconnects.
//!
//! Two levers are used to break a session without breaking the agent:
//!
//! - [`SeverableProxy`] cuts a real agent's TCP connection mid-run, so the same
//!   agent *process* reconnects and replays its unacknowledged window. Killing
//!   the agent instead would mint a new incarnation and never exercise replay.
//! - [`FakeAgent`] drives the wire protocol by hand, so a test can choose
//!   sequence numbers and incarnations and open two sessions for one agent id.

use std::time::Duration;

use loadr_agent::pb;

mod support;
use support::*;

// ---------------------------------------------------------------------------
// A real agent whose stream is cut repeatedly mid-run
// ---------------------------------------------------------------------------

/// The headline guarantee, end to end: a broken stream costs latency, not data.
///
/// This covers the whole path — replay, deduplication and completion — under
/// real reconnects of a real agent, and either failure mode moves the exact
/// count (loss low, double-counting high). It is not, however, a reliable
/// detector of *loss* on its own: the pre-fix window between handing a message
/// to the wire and it reaching the socket is microseconds wide, so a cut rarely
/// lands inside it. `uplink::tests::unacked_messages_replay_after_a_severed_session`
/// is what pins that down deterministically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_survive_a_severed_session_without_double_counting() {
    // Generous liveness: the reconnect gap is ~500 ms of backoff plus jitter,
    // and being declared lost would fail the run for an unrelated reason.
    let handle = start_controller(Duration::from_secs(30)).await;
    let proxy = SeverableProxy::start(handle.addr()).await;
    let _agent = spawn_agent(
        format!("http://{}", proxy.addr()),
        "a1",
        Some("agent-severed".to_string()),
        None,
    );
    wait_until(
        || handle.agents().iter().filter(|a| a.healthy).count() == 1,
        Duration::from_secs(10),
        "the agent to register through the proxy",
    )
    .await;

    // Long enough for several snapshot ticks (500 ms each) to be in flight when
    // the stream is cut.
    let plan = r#"
name: severed-session
scenarios:
  s:
    executor: shared-iterations
    vus: 2
    iterations: 1200
    flow:
      - request: { url: "http://mock.local/x" }
thresholds:
  http_reqs: ["count==1200"]
"#;
    let run_id = handle
        .submit(plan.to_string(), quick_submit())
        .await
        .expect("submit");

    let mut severed = 0;
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if is_terminal(&run_state(&handle, &run_id)) {
            break;
        }
        proxy.sever();
        severed += 1;
    }
    assert!(
        severed >= 2,
        "the run finished before the stream could be cut twice, so this asserts nothing"
    );

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(60),
        "run completion",
    )
    .await;
    assert_eq!(
        run_state(&handle, &run_id),
        "finished",
        "the run must complete despite the severed sessions"
    );
    // One connection for the initial registration plus one per reconnect. The
    // final cut may land after the run already completed, so only the earlier
    // ones are guaranteed to have been followed by a reconnect.
    assert!(
        proxy.accepted() >= severed,
        "the agent must have reconnected mid-run: {} connections for {severed} cuts",
        proxy.accepted()
    );

    assert_eq!(
        summary_metric_sum(&handle, &run_id, "http_reqs"),
        1200.0,
        "every delta is delivered exactly once across the reconnects"
    );
    assert_eq!(
        summary_metric_sum(&handle, &run_id, "iterations"),
        1200.0,
        "iteration accounting survives the reconnects too"
    );
    let thresholds = handle.run_thresholds(&run_id);
    assert!(thresholds[0].passed, "{:?}", thresholds[0]);

    handle.shutdown();
}

// ---------------------------------------------------------------------------
// Fenced sessions and deduplication, asserted on the wire
// ---------------------------------------------------------------------------

/// Proves the superseded stream is actively terminated with `Aborted`, over the
/// real transport — the behaviour a tonic upgrade could silently change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_concurrent_sessions_for_one_agent_id_abort_the_older_stream() {
    let handle = start_controller(Duration::from_secs(30)).await;
    let mut first = FakeAgent::connect(handle.addr(), "agent-twin", "proc-1").await;
    let _second = FakeAgent::connect(handle.addr(), "agent-twin", "proc-2").await;

    let status = first
        .wait_for_end()
        .await
        .expect("the superseded stream is terminated with a status");
    assert_eq!(
        status.code(),
        tonic::Code::Aborted,
        "the peer should learn immediately, and with a reason: {status}"
    );

    handle.shutdown();
}

/// The same agent process reconnects and replays an already-applied delta. The
/// controller must acknowledge it again and merge it only once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replaying_over_a_new_session_merges_a_delta_only_once() {
    let handle = start_controller(Duration::from_secs(30)).await;
    let mut agent = FakeAgent::connect(handle.addr(), "agent-replay", "proc-1").await;
    let run_id = handle
        .submit(MINIMAL_PLAN.to_string(), quick_submit())
        .await
        .expect("submit");

    agent.send(metrics_batch(&run_id, 1, 40)).await;
    assert!(
        agent.acks().await.contains(&1),
        "the first delta is acknowledged"
    );
    drop(agent);

    // Same incarnation: the controller carries its cursor across the reconnect,
    // so the replay is recognized as a duplicate.
    let mut resumed = FakeAgent::connect(handle.addr(), "agent-replay", "proc-1").await;
    resumed.send(metrics_batch(&run_id, 1, 40)).await;
    assert!(
        resumed.acks().await.contains(&1),
        "a duplicate must still be acknowledged, or the agent replays it forever"
    );

    resumed.send(run_event(&run_id, 2, "finished")).await;
    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(10),
        "run completion",
    )
    .await;
    assert_eq!(
        summary_metric_sum(&handle, &run_id, "http_reqs"),
        40.0,
        "metric merging is additive, so the replay must not be merged twice"
    );

    handle.shutdown();
}

/// A restarted agent process reuses its stable agent id but starts its uplink
/// sequence again at 1. Without the incarnation reset the controller would
/// discard every one of those messages as a duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restarting_with_a_new_incarnation_is_not_treated_as_a_duplicate() {
    let handle = start_controller(Duration::from_secs(30)).await;
    let mut agent = FakeAgent::connect(handle.addr(), "agent-reborn", "proc-1").await;
    let run_id = handle
        .submit(MINIMAL_PLAN.to_string(), quick_submit())
        .await
        .expect("submit");

    for seq in 1..=3 {
        agent.send(metrics_batch(&run_id, seq, 5)).await;
    }
    assert!(
        agent.acks().await.contains(&3),
        "the cursor reached sequence 3"
    );
    drop(agent);

    let reborn = FakeAgent::connect(handle.addr(), "agent-reborn", "proc-2").await;
    reborn.send(metrics_batch(&run_id, 1, 7)).await;
    reborn.send(run_event(&run_id, 2, "finished")).await;
    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(10),
        "run completion",
    )
    .await;
    assert_eq!(
        summary_metric_sum(&handle, &run_id, "http_reqs"),
        22.0,
        "the restarted process's fresh sequence space must be accepted, not deduplicated away"
    );

    handle.shutdown();
}

/// An acknowledgement is only ever sent on the session that earned it. The
/// agent's replay window is shared across its sessions, so acknowledging a
/// superseded stream would retire messages the controller refused to apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_superseded_session_is_neither_applied_nor_acknowledged() {
    let handle = start_controller(Duration::from_secs(30)).await;
    let mut stale = FakeAgent::connect(handle.addr(), "agent-fenced", "proc-1").await;
    let run_id = handle
        .submit(MINIMAL_PLAN.to_string(), quick_submit())
        .await
        .expect("submit");
    let mut live = FakeAgent::connect(handle.addr(), "agent-fenced", "proc-1").await;

    stale.send(metrics_batch(&run_id, 1, 99)).await;
    let stale_traffic = stale.wait_for_end().await;
    assert_eq!(
        stale_traffic.map(|s| s.code()),
        Some(tonic::Code::Aborted),
        "the stale stream is terminated rather than served"
    );

    live.send(metrics_batch(&run_id, 1, 4)).await;
    assert!(
        agent_acked(&mut live, 1).await,
        "the live session is served"
    );
    live.send(run_event(&run_id, 2, "finished")).await;
    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(10),
        "run completion",
    )
    .await;
    assert_eq!(
        summary_metric_sum(&handle, &run_id, "http_reqs"),
        4.0,
        "only the live session's delta is merged"
    );

    handle.shutdown();
}

async fn agent_acked(agent: &mut FakeAgent, seq: u64) -> bool {
    agent.acks().await.contains(&seq)
}

/// Registration still rejects an agent speaking a different protocol version,
/// which is what stops a peer that expects acknowledgements from talking to one
/// that never sends them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mismatched_protocol_version_is_rejected_at_registration() {
    use loadr_agent::pb::coordination_client::CoordinationClient;
    use tokio_stream::wrappers::ReceiverStream;

    let handle = start_controller(Duration::from_secs(30)).await;
    let mut client = CoordinationClient::connect(format!("http://{}", handle.addr()))
        .await
        .expect("connect");
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(pb::AgentMessage {
        seq: 0,
        msg: Some(pb::agent_message::Msg::Register(pb::Register {
            agent_id: "agent-old".to_string(),
            agent_name: "agent-old".to_string(),
            protocol_version: loadr_agent::PROTOCOL_VERSION - 1,
            loadr_version: "old".to_string(),
            cpu_cores: 1,
            labels: Default::default(),
            resume_run_id: String::new(),
            incarnation: String::new(),
        })),
    })
    .await
    .expect("queue register");

    let status = client
        .session(ReceiverStream::new(rx))
        .await
        .expect_err("an out-of-date agent must be refused");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(handle.agents().is_empty(), "and never registered");

    handle.shutdown();
}
