# Demo: 2,000,000 requests/second in AWS

One `loadr run` command drives a spot fleet of agents to a sustained
**2M req/s** against a horizontally-scaled demo API, with the controller UI
showing fleet-wide throughput and *correctly merged* HDR percentiles live.

```text
 laptop ── loadr run --controller ──▶ controller (c7g.2xlarge, UI :6464)
                                          │ gRPC :7625 — exact rate partitioning
                          ┌───────────────┼───────────────┐
                       agent-0 … agent-15 (c7gn.4xlarge, SPOT)
                          │  each agent hits EVERY target
                          ▼  (12 scenarios, one per target IP — no LB)
                       target-0 … target-11 (c7g.4xlarge, SPOT, tiny Go API :8080)
```

**Everything runs on spot** (one-time requests). Ballpark: ~29 instances /
~460 vCPUs ≈ **$25–30/h on-demand, typically $8–14/h on spot** — check
`aws ec2 describe-spot-price-history` for today's numbers. The stack lives
for a couple of hours and `terraform destroy` is the punchline.

## Design decisions

- **No load balancer.** An NLB at 2M req/s is its own scaling event and muddies
  the story if it lags the ramp. Instead `render-plans.sh` writes one scenario
  per target (absolute URL to its private IP); the controller partitions each
  scenario's rate across all agents, so load spreads evenly by construction.
- **Single AZ + cluster placement group.** Flat latency, no cross-AZ charges.
- **Spot everywhere.** Agent interruption mid-run is survivable — loadr's
  default agent-loss policy (`continue`) keeps the run going on the remaining
  fleet and notes it in the summary. That resilience is itself demo-able.
- **No SSH.** Shell access via SSM Session Manager only. gRPC between
  controller and agents is plaintext but never leaves the SG-guarded VPC;
  only the UI port (6464) is exposed, and only to `admin_cidr`.
- **PPS, not bandwidth, is the AWS ceiling** — hence network-optimized c7gn
  agents. At 2M req/s the fleet moves ≥4M packets/s each way before ACKs.

## Prerequisites

- terraform ≥ 1.5, jq, aws-vault; a `loadr` binary on your laptop
  (`cargo build --release -p loadr-cli` or the GitHub release).
- **Spot vCPU quota**: full fleet ≈ 460 vCPUs. Check *"All Standard
  (A, C, D, H, I, M, R, T, Z) Spot Instance Requests"* ≥ 512 in Service
  Quotas, or the apply will fail.
- **c7gn availability** in your region — swap `agent_instance_type` for
  `c6in.4xlarge` (x86; the userdata picks the right binary automatically) or
  `c7g.4xlarge` if needed.

## Runbook

### Phase 1 — calibrate (one agent, ~$2)

Never present an unmeasured ceiling. Bring the stack up with a single agent
and find what one instance actually sustains:

```bash
cd demos/2m-rps/terraform
aws-vault exec $AWS_PROFILE -- terraform init
aws-vault exec $AWS_PROFILE -- terraform apply \
  -var admin_cidr="$(curl -s https://checkip.amazonaws.com)/32" \
  -var agent_count=1

cd .. && ./scripts/render-plans.sh
loadr run --controller "$(terraform -chdir=terraform output -raw submit_endpoint)" plans/calibrate.yaml
```

Open the UI (`terraform output controller_ui_url`), watch the staircase, and
note the rate where `dropped_iterations` first climbs — that's the per-agent
ceiling. While it runs, check *which* wall you're hitting from an SSM session
on the agent: `ethtool -S ens5 | grep exceeded` (ENA PPS/bandwidth allowance)
vs `mpstat 1` (CPU). PPS-bound → bigger network-optimized instances;
CPU-bound → more or bigger instances, either works.

Size the fleet with 30% headroom:

> agents = ceil(2,000,000 / (ceiling × 0.7)) — e.g. ceiling 180k/s → 16 agents.

Measured anchor (2026-07-17): this staircase run co-located with one target on
a 6-core dev box sustained **~27k/s open-model** (drops from ~30k, p99 130ms)
— the methodology finds the knee cleanly, but generator and target were
sharing cores, so do NOT extrapolate fleet size from it. The default
`agent_count = 16` assumes ~180k/s per dedicated c7gn.4xlarge; if Phase 1
measures nearer 100k/s, the fleet needs ~29 agents (or 8xlarge agents) —
that's exactly why this phase exists.

### Phase 2 — the 2M run

```bash
cd terraform
aws-vault exec $AWS_PROFILE -- terraform apply \
  -var admin_cidr="$(curl -s https://checkip.amazonaws.com)/32" \
  -var agent_count=16          # from your calibration

cd .. && ./scripts/render-plans.sh   # re-render in case target IPs changed
loadr run --controller "$(terraform -chdir=terraform output -raw submit_endpoint)" plans/demo-2m.yaml
```

The ladder ramps 250k → 500k → 1M → 2M with 90-second holds, then a 5-minute
hold at 2M — 14 minutes total, sized to fit a 15-minute demo slot. Narrate from the UI: the synchronized start
barrier, exact rate partitioning (N×rate/N ≡ rate), and the fleet-wide p99
merged from per-agent HDR histograms — not averaged.

Sanity checks while it runs:

```bash
# per-target request counters (from any fleet instance via SSM)
curl -s http://<target-private-ip>:8080/stats
# spot interruptions, if any
aws-vault exec $AWS_PROFILE -- aws ec2 describe-instances \
  --filters Name=tag:Demo,Values=2m-rps Name=instance-state-name,Values=terminated
```

### Phase 3 — teardown (not optional)

```bash
cd terraform
aws-vault exec $AWS_PROFILE -- terraform destroy \
  -var admin_cidr="0.0.0.0/32" -auto-approve
```

State is local (`terraform.tfstate` — gitignored), so destroy from the same
checkout you applied from.

## Failure modes & rehearsal notes

| Risk | Mitigation |
|------|------------|
| Spot capacity for 29 instances in one placement group | `-var enable_placement_group=false`, or split instance types |
| Agent interrupted mid-demo | default `continue` policy; the dip in the throughput graph is a talking point, not a failure |
| Controller/target interrupted | re-run; consider on-demand for just the controller on demo day (edit `compute.tf`) |
| Targets saturate before agents | `/stats` shows per-target load; raise `target_count` and re-render |
| Numbers lower than hoped | drop `TOTAL_RPS=1000000 ./scripts/render-plans.sh` — a *sustained, honest* 1M with merged p99s beats a flaky 2M |

Rehearse the full ladder at least once before an audience. The plans are
generated — don't hand-edit `plans/*.yaml`; change the renderer or its inputs.
