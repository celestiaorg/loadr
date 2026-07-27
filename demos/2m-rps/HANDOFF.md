# Handoff: 2M req/s loadr demo — session context

Written 2026-07-19 to continue work started 2026-07-17 in a different Claude
session (that one was rooted in `~/development/tripwire`; start the new one in
`~/development/loadr.io`). Delete this file once the demo is proven.

## Mission

Prove a **2,000,000 req/s** distributed loadr demo in AWS: spot-only fleet,
one `loadr run` command, controller UI showing fleet-wide throughput and
correctly-merged HDR p99s. The ramp ladder must fit a **15-minute demo slot**
(current ladder: 14 min — 250k → 500k → 1M → 2M, 90s holds, 5-min hold at 2M).

## What exists (all under `demos/2m-rps/`, UNTRACKED — nothing committed yet)

- `terraform/` — VPC (10.42.0.0/16, single AZ, public subnet), optional
  cluster placement group, SSM-only IAM (no SSH), and three **spot** fleets:
  controller (`c7g.2xlarge`, UI :6464 / gRPC :7625), agents
  (`c7gn.4xlarge` default), targets (`c7g.4xlarge`, Go API :8080).
  Userdata installs the loadr release binary from GitHub (arch auto-detected
  via `uname -m`, so x86 fallback types just work), applies kernel tuning,
  runs systemd units. `terraform validate` + `fmt -check` pass. State is
  LOCAL (`terraform/terraform.tfstate`, gitignored) — destroy from the same
  checkout you applied from.
- `target/main.go` — tiny dependency-free Go API (`/json`, `/healthz`,
  `/stats` request counter), compiled on-instance by userdata. (The benchmark
  harness's target app is NOT in the repo, so this is self-contained.)
- `scripts/render-plans.sh` — generates `plans/demo-2m.yaml` +
  `plans/calibrate.yaml` from `terraform output -json target_private_ips`.
  Env knobs: `TOTAL_RPS` (default 2,000,000), `CAL_MAX` (calibration top
  rate, default 300,000). Plans are generated — never hand-edit.
- `README.md` — full runbook (calibrate → scale → destroy), failure modes.

**Sharding design (the key trick):** no load balancer. One scenario per
target private IP (absolute URLs — supported per `docs/src/yaml/requests.md`);
the controller partitions each scenario's rate across ALL agents, so spread
is even by construction and there's no NLB to pre-warm or blame. The
calibrate plan is also sharded across all targets so a single target's
ceiling is never mistaken for the agent's.

## Verified so far

- Both plans pass `loadr validate` (rendered with stubbed IPs, then deleted).
- CLI flags in userdata match the real binary: `controller --bind --ui-bind`,
  `agent --join --name`, `run --controller`.
- Release assets exist and download (HTTP 200):
  `https://github.com/levantar-ai/loadr/releases/latest/download/loadr-{aarch64,x86_64}-unknown-linux-gnu.tar.gz`
- Release binary built at `target/release/loadr` (needed locally to submit runs).
- **Local calibration (2026-07-17):** 6-core dev box, generator + target
  co-located: sustained **~27k/s open-model**, drops from ~30k, p99 130ms.
  Proves the staircase + `dropped_iterations` knee-finding methodology.
  Do NOT size the fleet from it (shared cores). Naive extrapolation says a
  c7gn.4xlarge might be anywhere 100k–250k/s → fleet is 8–29 agents. That
  spread is what cloud calibration resolves.

## Where it stopped

Blocked on **aws-vault MFA** — Claude cannot type the token. The user must
pre-auth in *their* terminal (the `!` prefix runs it in-session):

```
! aws-vault exec lev:andy.rea -- aws sts get-caller-identity
```

(Profile `lev:andy.rea` confirmed in `aws-vault list`. A 60s timeout on this
command from Claude means it's sitting on the MFA prompt — stop it and ask
the user, don't retry.)

## Next steps (in order)

1. User pre-auths (above).
2. Check `c7gn.8xlarge` exists in eu-west-2:
   `aws ec2 describe-instance-type-offerings --location-type region --filters Name=instance-type,Values=c7gn.8xlarge` —
   fallback `c6in.8xlarge` (x86, userdata copes). The user explicitly asked
   for a LARGER agent type for calibration (32 vCPU class).
3. Apply the calibration stack (~$2–3/h, all spot):
   ```
   cd demos/2m-rps/terraform
   aws-vault exec lev:andy.rea -- terraform init
   aws-vault exec lev:andy.rea -- terraform apply \
     -var admin_cidr="$(curl -s https://checkip.amazonaws.com)/32" \
     -var agent_count=1 -var target_count=4 \
     -var agent_instance_type=c7gn.8xlarge
   ```
4. Wait for boot (binary download + Go build ≈ 2 min; agent appears in the
   fleet view at `terraform output controller_ui_url`).
5. Render + run the 9-min staircase from the laptop:
   ```
   cd .. && CAL_MAX=500000 ./scripts/render-plans.sh
   ../../target/release/loadr run \
     --controller "$(terraform -chdir=terraform output -raw submit_endpoint)" \
     plans/calibrate.yaml
   ```
6. Read the knee (rate where `dropped_iterations` starts). Via SSM on the
   agent, check WHICH wall: `ethtool -S ens5 | grep exceeded` (ENA PPS) vs
   `mpstat 1` (CPU). PPS-bound → bigger/network-optimized; CPU-bound → either.
7. Size: `agents = ceil(2,000,000 / (ceiling × 0.7))`. Also sanity-check
   targets aren't the knee (`curl http://<target-ip>:8080/stats` via SSM).
8. Scale (`terraform apply -var agent_count=N -var target_count=12 ...` same
   admin_cidr/instance-type vars), re-render plans (IPs change!), run
   `plans/demo-2m.yaml`, capture the money screenshot.
9. **`terraform destroy`** — non-optional; spot or not it's ~460 vCPUs at
   full fleet.

## Gotchas / constraints

- **Spot vCPU quota**: full fleet ≈ 460 vCPUs ("All Standard Spot Instance
  Requests" ≥ 512 needed). Calibration stack is only ~72 vCPUs. Check before
  step 8, not after a half-failed apply.
- Placement-group spot capacity can fail → `-var enable_placement_group=false`.
- `admin_cidr` must be the CURRENT public IP each apply (remote-control
  sessions may move networks).
- If numbers disappoint, a sustained honest 1M (`TOTAL_RPS=1000000`) beats a
  flaky 2M — renderer supports it directly.
- Repo remotes: `origin` = levantar-ai/loadr (releases live here),
  `personal` = reaandrew/loadr.io. Conventional commits; CI releases off main
  (`release.yml` paths-ignore does NOT exclude `demos/`, so a
  `feat:`/`fix:`-typed commit touching only demos/ still cuts a CLI release —
  use `docs:` or `chore:` when committing this directory).
- User's standing preference: squash-merge PRs once CI is green.
