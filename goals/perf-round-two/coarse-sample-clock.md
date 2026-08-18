# Goal - measure (then maybe implement) a coarse clock for bus-mode samples

In bus mode (any sample-consuming output: json/csv), every sample stamps
`SystemTime::now()` — 11 clock reads per HTTP request. Shard mode already
skips the clock entirely, and only the json and csv outputs ever read
per-sample timestamps. On Linux `clock_gettime` is a vDSO call (~20-25ns), so
this may not be worth code at all — measure first, implement only on
evidence.

Paste this whole block into a fresh coding-agent session:

```text
/goal Measure per-sample clock cost in bus mode and, only if it pays, replace it with a coarse cached timestamp

CONTEXT
- Base branch: risotto.
- `MetricsBus::emit_value` (crates/loadr-core/src/metrics.rs, Sink::Tx arm)
  calls `now_millis()` per sample; the Shard arm already stamps 0 because
  aggregation never reads timestamps.
- Per-sample timestamp consumers, verified: crates/loadr-outputs/src/json.rs:81
  and crates/loadr-outputs/src/csv_out.rs:71 only (statsd does not read it;
  snapshots carry their own timestamps).
- This was the disposition's lowest-priority deferred item; honesty about
  the win is part of the goal.

IMPLEMENTATION
1. Measure: a small release-mode micro-measurement (a #[ignore]d test or a
   one-off bin under scratch, NOT committed as a bench harness) of
   emit_value cost with and without the now_millis call at realistic sample
   shapes. Report ns/sample and the projected share at 250k req/s x 11
   samples.
2. Decision gate: if the clock is <~5% of emit cost, close this goal as
   wont-fix with the numbers in the PR/issue text — deleting the idea is a
   valid outcome.
3. If it pays: add a coarse clock — an AtomicU64 of unix-millis owned by a
   tokio interval task (suggest 10ms period) started by the engine only when
   bus mode is active, read Relaxed in emit_value. Fallback to now_millis()
   when the task isn't running (detached buses, tests, handleSummary bus).
   Document the json/csv timestamp quantization (<=10ms) in the two output
   docs and keep `Snapshot` timestamps exact.

OUT OF SCOPE
- Shard mode (already clockless); Instant-based rewrites; changing Sample's
  shape or the Output trait.

TESTS
- If implemented: bus-mode engine test asserting samples carry monotonically
  non-decreasing, non-zero timestamps within the run window (extend
  crates/loadr-core/tests/metrics_pipeline.rs's
  sample_consuming_output_keeps_bus_path); coarse-clock unit test (tick
  updates value; fallback path stamps non-zero).

QUALITY BAR
Release-mode numbers for the decision either way; focused tests if code
lands; no unrelated refactors; conventional commit, no Claude-Session
trailer. Run cargo fmt --all and cargo clippy --workspace --all-targets --
-D warnings, then cargo test -p loadr-core -p loadr-outputs --locked
(workspace suite before the PR: --exclude loadr-browser locally).

DONE when: either the goal is closed with measured numbers showing the
syscall is immaterial, or the coarse clock lands with bus-mode tests green
and the quantization documented.
```
