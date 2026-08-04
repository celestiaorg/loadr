# Distributed testing overview

One machine tops out. loadr's distributed mode runs **one test across a fleet
of agents** with a single point of control and — crucially — *correct*
aggregate statistics.

```text
                    ┌──────────────────────────────┐
   loadr run ─────▶ │          controller          │ ◀───── web UI / API
   --controller     │  partitioning · aggregation  │
                    │  thresholds · run lifecycle  │
                    └──────┬───────┬───────┬───────┘
                     gRPC (mTLS)   │       │
                    ┌──────┴─┐ ┌───┴────┐ ┌┴───────┐
                    │ agent-1│ │ agent-2│ │ agent-3│   loadr agent --join ...
                    └────────┘ └────────┘ └────────┘
```

- The **controller** accepts agents, distributes test definitions and data
  files, partitions load, coordinates a synchronized start, aggregates
  metrics centrally and evaluates thresholds fleet-wide.
- **Agents** are dumb muscle: they receive an assignment, run their share
  with the ordinary engine, and stream metric deltas back every second.

## Quick start

```bash
# 1. control plane (also serves the web UI)
loadr controller --bind 0.0.0.0:7625 --ui-bind 0.0.0.0:6464

# 2. on each load generator
loadr agent --join controller-host:7625 --name agent-$(hostname)

# 3. submit a test (to the controller's API/UI port)
loadr run --controller controller-host:6464 test.yaml
```

Or the batteries-included stack (controller + 3 agents + Prometheus +
Grafana):

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Kubernetes manifests and a Helm chart live in
`deploy/` —
`helm install loadr deploy/helm/loadr --set agents.replicas=10`.

## What gets partitioned

| Executor | Split across N agents |
|---|---|
| `constant-vus`, `ramping-vus` | VU counts (remainder to the lowest indices) |
| `constant-arrival-rate`, `ramping-arrival-rate` | rates divided exactly (N×rate/N = rate) |
| `shared-iterations` | the iteration pool |
| `per-vu-iterations` | VUs split; iterations-per-VU unchanged |
| `externally-controlled` | scale commands split like VU counts |

Stage *timings* are identical everywhere — only magnitudes scale — so global
ramps are exact. Every agent first acknowledges readiness (files
materialized, engine built); the controller then sets a start timestamp a
barrier delay ahead (default 2 s), so the whole fleet starts on the same
clock no matter how long any agent's setup took.
