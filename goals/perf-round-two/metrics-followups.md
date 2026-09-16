# Goal - metrics pipeline follow-ups (regression test + panic-safe cleanup)

Two non-blocking items from the metrics-stack final review: the
drain-inflation fix has no regression test that would actually catch its
return, and shard-mode cleanup relies on explicit cancellation calls that a
panic could skip, leaking the aggregator task in long-lived agent processes.

Paste this whole block into a fresh coding-agent session:

```text
/goal Add a bus-vs-shard duration regression test and make shard-mode aggregator cleanup panic-safe

CONTEXT
- Base branch: risotto. Files: crates/loadr-core/src/engine.rs (Engine::run —
  SampleSource construction, `shard_done` CancellationToken, end-of-run
  cleanup and the setup-Err arm both cancel it explicitly),
  crates/loadr-core/tests/metrics_pipeline.rs (harness with RecordingHandler,
  no_output_run_uses_shards_and_reports_exact_totals,
  sample_consuming_output_keeps_bus_path, setup-failure tests).
- Review findings being addressed:
  1. The existing duration assertion (`duration_secs < 2.0` on a ~20ms plan)
     is ~100x loose and would not fail if bus-mode drain-inflation returned;
     a meaningful check compares the SAME plan under bus mode vs shard mode.
  2. `shard_done.cancel()` is called explicitly on the normal and setup-Err
     paths, but a panic unwinding through setup()/teardown() would skip both;
     bus mode self-heals via Drop closing the channel, shard mode leaks the
     aggregator task (live ticker + growing timeline Vec) for the process
     lifetime — meaningful for a long-lived `loadr agent` handling many runs.
     (Today loadr-js converts script panics to Results, so this is latent
     defense-in-depth, not a reproducible bug.)

IMPLEMENTATION
1. Head-to-head duration test in metrics_pipeline.rs: one plan, generous
   sample volume (enough iterations that a drain backlog would be visible,
   but CI-safe — aim <5s total), run twice: (a) no outputs (shard mode),
   (b) with a wants_samples()=true collector output (bus mode). Assert both
   summaries' totals identical AND
   `shard.duration_secs <= bus.duration_secs + epsilon` with epsilon generous
   enough to be non-flaky (e.g. 1.0s) — the point is catching a systematic
   multi-second drain regression, not micro-timing. If wall-clock comparison
   proves flaky in CI, fall back to asserting shard-mode
   `duration_secs <= plan duration + 1.0s` with sample volume high enough
   that unfixed drain-inflation would exceed it — document which variant
   landed and why.
2. Panic-safe cleanup: wrap `shard_done` cancellation in an RAII guard armed
   when the token is created and disarmed/fired on every exit path (a ~10
   line Drop struct in engine.rs; no new dependency — do not add scopeguard).
   The explicit cancel() calls can then be removed or kept as no-ops —
   prefer removing to keep one mechanism. Also confirm by inspection that
   `vu`/`bus` drops on the panic path are covered by normal unwinding (they
   are locals) and note it in the guard's comment.
3. Optional, if trivial while in the file: assert in the bus-mode half of the
   new test that collected samples carry non-zero timestamps (pins the
   bus/shard timestamp split; shard-mode timestamps are unobservable by
   design).

OUT OF SCOPE
- Changing drain semantics, shard count heuristics, or Output trait surface;
  the sanctioned trend-memory/avg-approximation trade-offs (documented,
  accepted).

TESTS
Item 1 IS a test; item 2 gets one: a plan whose setup panics (if a panicking
ScriptEngine mock is feasible in the harness) or, failing that, a unit test
of the guard's Drop behavior + inspection note that all engine exit paths are
covered.

QUALITY BAR
Non-flaky by construction (generous epsilons, bounded volumes); no unrelated
refactors; conventional commit, no Claude-Session trailer. Run cargo fmt
--all and cargo clippy --workspace --all-targets -- -D warnings, then cargo
test -p loadr-core --locked (workspace suite before the PR: --exclude
loadr-browser locally).

DONE when: the head-to-head (or documented fallback) duration test exists and
passes repeatedly, and shard_done cancellation is structurally panic-safe
with its covering test green.
```
