# Controller & agents

## The coordination protocol

Controller and agents speak `loadr.coordination.v1` — a single bidirectional
gRPC stream per agent:

```text
agent ──▶ Register{agent_id, incarnation, name, protocol_version, cores, labels}
      ◀── Registered{controller_id}
      ◀── Assignment{run_id, plan_yaml, partition i/n, data files}
      ◀── Start{run_id, start_unix_ms}          # synchronized barrier
      ──▶ MetricsBatch{run_id, delta}    seq=n  # every second
      ──▶ Heartbeat{active_vus, run_state}      # every 2 seconds, unsequenced
      ◀── UplinkAck{seq}                        # cumulative receipt
      ◀── Control{stop|kill|pause|resume|scale}
      ──▶ RunEvent{started|finished|failed, summary}   seq=n
```

The protocol is versioned; an agent with an incompatible
`protocol_version` is rejected at registration.

## Delivery guarantees

A reconnect must not cost data — an exact iteration count has to stay exact even
if the stream breaks mid-run. Uplink delivery is therefore **at-least-once on
the wire, effectively-once at the controller**:

- Every metric batch and run event carries a per-agent `seq`, starting at 1. The
  agent keeps it in a bounded window until the controller acknowledges it, and a
  new session replays the window from the oldest unacknowledged message. A
  message is never removed just because it was handed to the wire.
- `UplinkAck` is cumulative: `seq = n` retires everything through `n`. The
  controller acknowledges duplicates too — an unacknowledged message would be
  replayed forever and eventually fill the window.
- The controller discards any `seq` at or below its own cursor before applying
  it. Metric merging is additive, so a replayed batch would otherwise
  double-count.
- `Register.incarnation` identifies the agent *process*. `agent_id` is stable
  across restarts by design, so a restart is what tells the controller to reset
  its cursor rather than treat the agent's fresh `seq = 1` as a duplicate.
- Heartbeats are unsequenced (`seq = 0`): they describe the moment they were
  built, so a replayed one would report state that no longer holds. They are
  skipped rather than queued when the stream is congested.
- When the window fills — a long disconnect — the agent folds metric deltas back
  into its own aggregator and retries them coalesced. Resolution degrades;
  totals do not.

Registration also **fences** the stream it replaces: the older session is closed
with `ABORTED`, and any traffic still arriving on it is ignored rather than
allowed to refresh liveness, merge metrics or complete the run.

One gap remains: shutting an agent down (Ctrl-C) still drops whatever is
unacknowledged, because the run is killed after the uplink is already gone.

## TLS / mTLS

```bash
loadr controller --bind 0.0.0.0:7625 \
  --tls-cert server.pem --tls-key server-key.pem \
  --tls-client-ca clients-ca.pem          # require client certs (mTLS)

loadr agent --join ctrl:7625 \
  --tls-ca ca.pem \
  --tls-cert agent.pem --tls-key agent-key.pem
```

Without flags the channel is plaintext — fine on a private network, not on
the internet.

## Failure handling

- **Heartbeats** every 2 s; an agent silent past the liveness window
  (default 6 s) is marked unhealthy.
- **Reconnection**: agents reconnect with jittered exponential backoff and
  re-register, resuming their identity.
- **Agent loss during a run** is policy-driven per submission:
  - `continue` (default) — remaining agents keep their share; the lost
    agent's portion of the load simply stops (the summary notes the
    reduced fleet).
  - `abort` — the controller stops the run everywhere.

## Data files

CSV files, JS modules, proto files and body files referenced by the test are
shipped inside the assignment and materialized in the agent's working
directory. Paths are sanitized — anything containing `..` or absolute paths
is rejected.

## Operating notes

- Agents are stateless; scale them with your orchestrator
  (`kubectl scale deploy/loadr-agent --replicas=20`).
- One controller handles many sequential/concurrent runs; each run records
  its agent set at submission time.
- The web UI on the controller shows the fleet (health, VUs, labels,
  last heartbeat) and every run's live metrics.
