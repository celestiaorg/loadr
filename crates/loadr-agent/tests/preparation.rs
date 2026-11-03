//! Preparation-barrier integration: the readiness handshake, agent
//! reservation, and restart/reconnect recovery around the pre-start boundary.

mod support;

use std::collections::HashMap;
use std::time::Duration;

use loadr_agent::{AgentError, SubmitOptions};
use support::*;

const TEN_ITERATIONS: &str = r#"
name: prep-e2e
scenarios:
  s:
    executor: shared-iterations
    vus: 2
    iterations: 10
    flow:
      - request: { url: "http://mock.local/x" }
"#;

const LONG_RUN: &str = r#"
name: prep-long
scenarios:
  s:
    executor: constant-vus
    vus: 2
    duration: 30s
    flow:
      - request: { url: "http://mock.local/x" }
"#;

fn abort_reason(handle: &loadr_agent::ControllerHandle, run_id: &str) -> String {
    handle
        .run_summary(run_id)
        .expect("the run is terminal, so a summary exists")
        .aborted
        .expect("a failed run records why")
}

/// Setup outlasting the liveness window must not get the agent declared
/// lost: heartbeats keep flowing while preparation runs off the session loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_preparation_outlives_liveness_without_false_loss() {
    let handle = start_controller(Duration::from_secs(2)).await;
    let addr = format!("http://{}", handle.addr());
    let _agent = spawn_agent_with_deps(
        addr,
        "slow-prep",
        None,
        None,
        slow_deps(Duration::from_secs(5)),
    );
    wait_until(
        || handle.agents().iter().any(|a| a.healthy),
        Duration::from_secs(10),
        "agent registration",
    )
    .await;

    let run_id = handle
        .submit(TEN_ITERATIONS.to_string(), quick_submit())
        .await
        .expect("submit");

    // Preparation spans two and a half liveness windows. Before the readiness
    // barrier this reliably produced "agent lost during run": setup blocked
    // the session loop and suppressed every heartbeat.
    for _ in 0..8 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            handle.agents().iter().all(|a| a.healthy),
            "the agent must stay healthy during slow preparation"
        );
        assert_ne!(
            run_state(&handle, &run_id),
            "failed",
            "slow preparation must not fail the run"
        );
    }

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(30),
        "run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "finished");
    assert_eq!(summary_metric_sum(&handle, &run_id, "http_reqs"), 10.0);

    handle.shutdown();
}

/// One agent cannot prepare: the run fails as a preparation failure (not an
/// execution failure), the healthy agent is disarmed and both reservations
/// are released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preparation_failure_stops_ready_agents_and_fails_the_run() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let addr = format!("http://{}", handle.addr());
    let _good = spawn_labeled_agent_with_deps(
        addr.clone(),
        "prep-good",
        None,
        None,
        mock_deps(),
        HashMap::from([("group".to_string(), "good".to_string())]),
    );
    let _bad = spawn_agent_with_deps(addr, "prep-bad", None, None, failing_deps("boom"));
    wait_until(
        || handle.agents().iter().filter(|a| a.healthy).count() == 2,
        Duration::from_secs(10),
        "2 agents registered",
    )
    .await;

    let run_id = handle
        .submit(TEN_ITERATIONS.to_string(), quick_submit())
        .await
        .expect("submit");
    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(10),
        "preparation failure settles the run",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "failed");
    let reason = abort_reason(&handle, &run_id);
    assert!(
        reason.contains("preparation failed") && reason.contains("boom"),
        "a distinct preparation failure with the agent's detail, got: {reason}"
    );

    wait_until(
        || handle.agents().iter().all(|a| a.active_run.is_none()),
        Duration::from_secs(5),
        "reservations released",
    )
    .await;

    // The healthy agent was disarmed, not left waiting for a Start that will
    // never come: it can run alone immediately.
    let run2 = handle
        .submit(
            TEN_ITERATIONS.to_string(),
            SubmitOptions {
                agent_filter: Some(HashMap::from([("group".to_string(), "good".to_string())])),
                ..quick_submit()
            },
        )
        .await
        .expect("the disarmed agent is assignable");
    wait_until(
        || is_terminal(&run_state(&handle, &run2)),
        Duration::from_secs(15),
        "second run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run2), "finished");
    assert_eq!(summary_metric_sum(&handle, &run2, "http_reqs"), 10.0);

    handle.shutdown();
}

/// A second submission while every matching agent is reserved is refused
/// before any run is created — instead of assigning agents that would
/// busy-reject and fail the new run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submissions_do_not_share_agents() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let addr = format!("http://{}", handle.addr());
    let _agent = spawn_agent(addr, "busy-agent", None, None);
    wait_until(
        || handle.agents().iter().any(|a| a.healthy),
        Duration::from_secs(10),
        "agent registration",
    )
    .await;

    let run_id = handle
        .submit(LONG_RUN.to_string(), quick_submit())
        .await
        .expect("submit");
    let second = handle
        .submit(TEN_ITERATIONS.to_string(), quick_submit())
        .await;
    assert!(
        matches!(second, Err(AgentError::AgentsBusy)),
        "the second submission must be refused up front, got {second:?}"
    );

    handle.stop_run(&run_id).await.expect("stop");
    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(15),
        "first run settles",
    )
    .await;

    let run2 = handle
        .submit(TEN_ITERATIONS.to_string(), quick_submit())
        .await
        .expect("the agent is free again");
    wait_until(
        || is_terminal(&run_state(&handle, &run2)),
        Duration::from_secs(15),
        "second run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run2), "finished");

    handle.shutdown();
}

/// Restarting an agent process (same id, fresh incarnation) terminates its
/// participation promptly — via the registration path, not the liveness
/// sweep — and frees the agent for clean reassignment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_restart_terminates_run_and_allows_reassignment() {
    // Liveness generous enough that only the restart path can settle the run.
    let handle = start_controller(Duration::from_secs(30)).await;
    let addr = format!("http://{}", handle.addr());
    let first = spawn_agent(addr.clone(), "phoenix", Some("phoenix".to_string()), None);
    wait_until(
        || handle.agents().iter().any(|a| a.healthy),
        Duration::from_secs(10),
        "agent registration",
    )
    .await;

    let run_id = handle
        .submit(LONG_RUN.to_string(), quick_submit())
        .await
        .expect("submit");
    wait_until(
        || run_state(&handle, &run_id) == "running",
        Duration::from_secs(10),
        "run start",
    )
    .await;

    first.cancel();
    let _second = spawn_agent(addr, "phoenix", Some("phoenix".to_string()), None);

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(15),
        "the restart settles the run",
    )
    .await;

    let run2 = handle
        .submit(TEN_ITERATIONS.to_string(), quick_submit())
        .await
        .expect("the restarted agent is assignable");
    wait_until(
        || is_terminal(&run_state(&handle, &run2)),
        Duration::from_secs(15),
        "second run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run2), "finished");
    assert_eq!(summary_metric_sum(&handle, &run2, "http_reqs"), 10.0);

    handle.shutdown();
}

/// Restarting an agent mid-preparation records a distinct preparation
/// failure, and the replacement process is cleanly assignable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_restart_during_preparation_fails_the_run_cleanly() {
    let handle = start_controller(Duration::from_secs(30)).await;
    let addr = format!("http://{}", handle.addr());
    let first = spawn_agent_with_deps(
        addr.clone(),
        "phoenix-prep",
        Some("phoenix-prep".to_string()),
        None,
        slow_deps(Duration::from_secs(4)),
    );
    wait_until(
        || handle.agents().iter().any(|a| a.healthy),
        Duration::from_secs(10),
        "agent registration",
    )
    .await;

    let run_id = handle
        .submit(TEN_ITERATIONS.to_string(), quick_submit())
        .await
        .expect("submit");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(run_state(&handle, &run_id), "pending");

    first.cancel();
    let _second = spawn_agent(addr, "phoenix-prep", Some("phoenix-prep".to_string()), None);

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(15),
        "the restart fails preparation",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "failed");
    let reason = abort_reason(&handle, &run_id);
    assert!(
        reason.contains("restarted during preparation"),
        "got: {reason}"
    );

    let run2 = handle
        .submit(TEN_ITERATIONS.to_string(), quick_submit())
        .await
        .expect("reassignment");
    wait_until(
        || is_terminal(&run_state(&handle, &run2)),
        Duration::from_secs(15),
        "second run completion",
    )
    .await;
    assert_eq!(run_state(&handle, &run2), "finished");

    handle.shutdown();
}

/// A session cut mid-preparation: the agent reconnects and resumes, its
/// readiness replays over the durable uplink, and the run completes exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_during_preparation_recovers() {
    let handle = start_controller(Duration::from_secs(6)).await;
    let proxy = SeverableProxy::start(handle.addr()).await;
    let _agent = spawn_agent_with_deps(
        format!("http://{}", proxy.addr()),
        "prep-sever",
        None,
        None,
        slow_deps(Duration::from_secs(2)),
    );
    wait_until(
        || handle.agents().iter().any(|a| a.healthy),
        Duration::from_secs(10),
        "agent registration",
    )
    .await;

    let run_id = handle
        .submit(TEN_ITERATIONS.to_string(), quick_submit())
        .await
        .expect("submit");
    tokio::time::sleep(Duration::from_millis(500)).await;
    proxy.sever();

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(30),
        "run completion across the reconnect",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "finished");
    assert_eq!(summary_metric_sum(&handle, &run_id, "http_reqs"), 10.0);
    assert!(
        proxy.accepted() >= 2,
        "the cut must have forced a reconnect"
    );

    handle.shutdown();
}

/// The protocol-level barrier: no Start frame reaches any agent until every
/// assigned agent has reported readiness, and the broadcast carries one
/// timestamp for the whole fleet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_is_broadcast_only_after_every_agent_is_ready() {
    let handle = start_controller(Duration::from_secs(30)).await;
    let mut a = FakeAgent::connect(handle.addr(), "fake-a", "proc-a").await;
    let mut b = FakeAgent::connect(handle.addr(), "fake-b", "proc-b").await;

    let run_id = handle
        .submit(MINIMAL_PLAN.to_string(), quick_submit())
        .await
        .expect("submit");

    let assignment_a = a.next_assignment().await.expect("a's assignment");
    let assignment_b = b.next_assignment().await.expect("b's assignment");
    assert_eq!(
        assignment_a.partition_index, 0,
        "partitions follow sorted agent ids"
    );
    assert_eq!(assignment_b.partition_index, 1);

    a.send(assignment_ready(&run_id, 1, 0)).await;
    assert!(
        b.next_start().await.is_none(),
        "no Start while an agent is still preparing"
    );
    assert_eq!(run_state(&handle, &run_id), "pending");

    b.send(assignment_ready(&run_id, 1, 1)).await;
    let start_a = a.next_start().await.expect("a's Start");
    let start_b = b.next_start().await.expect("b's Start");
    assert_eq!(
        start_a.start_unix_ms, start_b.start_unix_ms,
        "one synchronized timestamp for the fleet"
    );
    assert_eq!(start_a.run_id, run_id);
    wait_until(
        || run_state(&handle, &run_id) == "running",
        Duration::from_secs(5),
        "start commit",
    )
    .await;

    handle.shutdown();
}

/// An agent that never becomes ready fails the run at the preparation
/// timeout, with the laggard named; the ready agents are told to stand down
/// and the fleet is admissible again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_timeout_fails_the_run_and_releases_agents() {
    let handle = start_controller(Duration::from_secs(30)).await;
    let mut agent = FakeAgent::connect(handle.addr(), "fake-slow", "proc-1").await;

    let run_id = handle
        .submit(
            MINIMAL_PLAN.to_string(),
            SubmitOptions {
                preparation_timeout: Duration::from_millis(500),
                ..quick_submit()
            },
        )
        .await
        .expect("submit");
    let _assignment = agent.next_assignment().await.expect("assignment");

    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(10),
        "preparation timeout",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "failed");
    let reason = abort_reason(&handle, &run_id);
    assert!(
        reason.contains("preparation timed out") && reason.contains("fake-slow"),
        "the laggard is named, got: {reason}"
    );

    let control = agent
        .next_control()
        .await
        .expect("the laggard is told to stand down");
    assert_eq!(
        (control.run_id.as_str(), control.action.as_str()),
        (run_id.as_str(), "stop")
    );

    let run2 = handle
        .submit(MINIMAL_PLAN.to_string(), quick_submit())
        .await
        .expect("the reservation was released");
    assert_ne!(run2, run_id);

    handle.shutdown();
}

/// An agent that goes silent before Start is declared lost by the liveness
/// sweep, which fails preparation immediately — far sooner than the
/// preparation timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn losing_an_agent_before_start_fails_preparation() {
    let handle = start_controller(Duration::from_secs(2)).await;
    let mut agent = FakeAgent::connect(handle.addr(), "fake-lost", "proc-1").await;
    let run_id = handle
        .submit(MINIMAL_PLAN.to_string(), quick_submit())
        .await
        .expect("submit");
    let _assignment = agent.next_assignment().await.expect("assignment");

    // The fake agent sends nothing further — no heartbeats, no readiness.
    // The default 120s preparation timeout cannot be what fails the run
    // within this test's window; the 2s liveness sweep is.
    wait_until(
        || is_terminal(&run_state(&handle, &run_id)),
        Duration::from_secs(10),
        "loss during preparation",
    )
    .await;
    assert_eq!(run_state(&handle, &run_id), "failed");
    let reason = abort_reason(&handle, &run_id);
    assert!(
        reason.contains("lost during preparation") && reason.contains("fake-lost"),
        "got: {reason}"
    );

    handle.shutdown();
}

/// Ready, then gone: the barrier fires while the agent is away, and its
/// resume claim replays Start with the same stored timestamp the rest of the
/// fleet received.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_after_ready_replays_the_stored_start() {
    let handle = start_controller(Duration::from_secs(30)).await;
    let mut a = FakeAgent::connect(handle.addr(), "fake-ra", "proc-a").await;
    let mut b = FakeAgent::connect(handle.addr(), "fake-rb", "proc-b").await;
    let run_id = handle
        .submit(MINIMAL_PLAN.to_string(), quick_submit())
        .await
        .expect("submit");
    let _ = a.next_assignment().await.expect("a assigned");
    let _ = b.next_assignment().await.expect("b assigned");

    a.send(assignment_ready(&run_id, 1, 0)).await;
    assert!(
        a.acks().await.contains(&1),
        "readiness is recorded before the drop"
    );
    drop(a);

    b.send(assignment_ready(&run_id, 1, 1)).await;
    let start_b = b
        .next_start()
        .await
        .expect("the barrier fires on the live agent");

    let mut resumed =
        FakeAgent::connect_with_resume(handle.addr(), "fake-ra", "proc-a", &run_id).await;
    let start_a = resumed
        .next_start()
        .await
        .expect("the stored Start is replayed on resume");
    assert_eq!(
        start_a.start_unix_ms, start_b.start_unix_ms,
        "the reconnecting agent keeps the fleet's clock"
    );

    handle.shutdown();
}

/// Assigned but never ready when the stream breaks: the resume claim replays
/// the Assignment, Start stays gated on readiness, and the handshake then
/// completes normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_before_readiness_replays_the_assignment() {
    let handle = start_controller(Duration::from_secs(30)).await;
    let mut a = FakeAgent::connect(handle.addr(), "fake-pre", "proc-1").await;
    let run_id = handle
        .submit(MINIMAL_PLAN.to_string(), quick_submit())
        .await
        .expect("submit");
    let assignment = a.next_assignment().await.expect("first delivery");
    drop(a);

    let mut resumed =
        FakeAgent::connect_with_resume(handle.addr(), "fake-pre", "proc-1", &run_id).await;
    let replayed = resumed.next_assignment().await.expect("assignment replay");
    assert_eq!(replayed.run_id, assignment.run_id);
    assert_eq!(replayed.partition_index, 0);
    assert!(
        resumed.next_start().await.is_none(),
        "Start is still gated on readiness"
    );

    resumed.send(assignment_ready(&run_id, 1, 0)).await;
    let started = resumed.next_start().await.expect("Start after readiness");
    assert_eq!(started.run_id, run_id);

    handle.shutdown();
}
