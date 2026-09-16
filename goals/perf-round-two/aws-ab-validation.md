# Goal - AWS A/B validation of round-one + raw transport

Everything measurable has now landed on risotto together: dispatcher fix,
gRPC call cache, lazy decode, metrics delta/sharding, and the raw h2
transport. The disposition's stated next step is measurement, not more code:
an A/B campaign against the round-one baselines, server-side judged.

Paste this whole block into a fresh coding-agent session:

```text
/goal Run the AWS A/B campaign validating the perf stack on risotto and produce a go/no-go on the raw-transport default

CONTEXT
- Build under test: loadr @ risotto a12b541 or later (contains all round-one
  work + raw transport + shard metrics).
- Tooling: the `loadr-aws-loadtest` project skill in the x402-risotto repo
  (.claude/skills/) — fleet map, deploy/rebuild flow, server-side judging
  conventions, samplers, analysis heuristics. Prior baselines live in the
  disposition table (docs/notes/loadr-perf-analysis-disposition.md) and in
  ~/loadr-plans/newrev-*-summary.json on the controller host.
- Round-one reference numbers (2x c8g.16xlarge agents -> nginx gRPC mock,
  server-side verified): old binary 64 workers 290k; 16 workers 388k;
  + dispatcher/cache fixes 494k total (247k/agent) at 2.56ms median;
  + 1ms tick same ceiling at 1.28ms median; h2load reference ~2.2M/host.
  Target from docs/loadr-s3-tower-bypass.md: 300k+/process with raw.
- Verification plan from that note: identical rate ladders `transport: raw`
  vs `channel`; expect the loadr-internal latency gap (loadr-measured vs
  `ss -ti` rtt) to collapse; connection count at the mock == pool size; zero
  failures; parity on all four call shapes before considering a default flip.

IMPLEMENTATION (measurement matrix, server-side judged throughout)
1. Rebuild/deploy risotto binaries to the fleet per the skill.
2. Ladder A/B: transport channel vs raw at fixed rate ladders to each
   config's ceiling; record loadr-side latency percentiles, server-side rate,
   ss -ti rtt on pooled connections, per-request CPU.
3. Knob sweeps at the winning transport: LOADR_DISPATCH_TICK_US 5000 vs 1000;
   --worker-threads 16 vs 64; 1 process vs 3 colocated (the round-one
   workaround — the goal is to show a single process now behaves comparably).
4. Metrics-pipeline validation on the same runs: timeline smoothness (no
   drain tail spike), summary duration_secs vs wall clock (no drain
   inflation), exact totals vs server-side counts, and controller CPU in
   distributed mode (delta path, no double aggregation).
5. Lazy-decode spot check: a status-only plan vs a jsonpath-assert plan at
   the same rate — expect measurably lower client CPU for the former.

OUT OF SCOPE
- Code changes (file findings as issues/goals instead); flipping the
  transport default (that is the OUTPUT of this goal, decided by humans on
  the numbers).

DELIVERABLE
- An updated results table in docs/notes/ (same format as the disposition's),
  raw summaries retained on the controller, and an explicit
  go/no-go recommendation on `transport: raw` as default plus any knob
  default changes, each backed by a number.

QUALITY BAR
Server-side verified rates only (never trust client-side alone — per the
skill's judging conventions); every claim traceable to a stored summary
file; identical hardware/ladders across compared configs.

DONE when: the results table exists with channel-vs-raw ladders, knob sweeps,
metrics-pipeline validation, and a numbers-backed default recommendation.
```
