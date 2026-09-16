# Goal - arrival dispatcher without a wake per iteration

The open-model dispatcher performs one idle-queue probe and, when a worker is
available, one targeted `Notify` wake per scheduled iteration on a single task.
That makes it a plausible remaining single-task choke point now that metrics
recording is sharded, but the existing measurements do not isolate its cost.
This goal moves arrival claims to workers and batches the dispatcher's steady
state work by tick. The replacement adds a contended atomic and broadcast wakes,
so it must earn the change in a local release-mode A/B before landing.

Paste this whole block into a fresh coding-agent session:

```text
/goal Replace the arrival dispatcher's per-iteration idle-queue hand-off with a tick-bounded shared claim budget, preserving open-model accounting and proving the result with a local release-mode A/B

CONTEXT
- Base branch: origin/nikolai/perf-dispatcher-port at d28814f. File:
  crates/loadr-core/src/executor.rs — `run_arrival_rate` (:630),
  `dispatch_tick` (:615, env LOADR_DISPATCH_TICK_US, default 5000us,
  clamped 250..=1_000_000).
- This goal intentionally differs from the other perf-round-two goals: d28814f
  introduced the current dispatcher, --worker-threads, and the zero-drop e2e
  as one commit on its then-main parent. Current main still has the old oneshot
  dispatcher; risotto is the aggregation branch. executor.rs is identical on
  d28814f and risotto, so the references and behavior below hold on both.
- Current design: workers park on a per-worker `Arc<Notify>` and send it through
  `mpsc::unbounded_channel::<Arc<Notify>>()` every idle cycle. For every whole
  arrival owed, the dispatcher probes that queue, wakes one idle worker, spawns
  when none is idle and `allocated < max_vus`, or records one dropped iteration.
  At 150k scheduled arrivals/s this is 150k queue probes/s and up to 150k wakes/s
  on one task.
- Evidence is suggestive, not causal: docs/loadr-s3-tower-bypass.md ("Related")
  lists the dispatcher among reasons multiple processes beat one runtime, but
  it also identifies transport wakes and shared runtime state. Treat the
  dispatcher as a measured hypothesis, not "the last bottleneck."
- This change deliberately refines drop timing. Instead of deciding entirely
  inside the dispatcher's synchronous per-arrival loop, a published arrival may
  be claimed until one wall-clock dispatch interval after publication. Any
  unclaimed budget then expires and is recorded as dropped; it must never roll
  into later ticks as an unbounded backlog.

IMPLEMENTATION
- Keep the diff inside `run_arrival_rate` plus small private helpers. Use one
  shared `Arc<AtomicU64>` claim budget, one `Arc<AtomicU64>` parked-worker count,
  one shared `Arc<AtomicBool>` open/closed flag, and one `Arc<Notify>`. Remove
  the idle-worker mpsc channel and per-worker Notifys. Do not add a ring in this
  first version.
- Implement `try_claim` with `fetch_update` or a compare-exchange loop that
  decrements only when the observed budget is positive. Do not use a load
  followed by unchecked `fetch_sub`: concurrent claimers can underflow it.
  SeqCst everywhere is acceptable; document anything weaker. Do not rely on
  notification alone to make the counter state correct.
- Worker protocol, before every iteration:
  1. If dispatch is open, the run is not paused, and `try_claim` succeeds, run
     exactly one iteration and loop back to the same checks. A claim won before
     dispatcher closure may finish during the existing graceful-stop window.
  2. Otherwise create a `Notified` future from the shared `Notify` first, then
     increment the parked count, and re-check open/pause/budget: exit on closed,
     take the claim path on unpaused positive budget, otherwise await it (racing
     scenario cancellation, as parked workers do today). The creation order is
     required because `notify_waiters()` stores no permit; Tokio guarantees
     that a `Notified` created before the broadcast observes it even if it has
     not yet been polled. A broadcast before creation is covered by the final
     budget re-check.
  3. Increment the parked count once per park and decrement it exactly once at
     the single point the parked state is left — re-check exit, notification,
     or cancellation — before any claim attempt, so the dispatcher's next
     parked snapshot is not knowingly stale. Spurious wakes are expected.
- Dispatcher protocol on each active tick:
  1. Track the current budget's expiry as a local `Instant`. If wall time has
     reached it, atomically `swap(0)` the unclaimed budget and record that value
     as one batched `dropped_iterations` metric update. Do not expire merely
     because another ticker event arrived: `MissedTickBehavior::Burst` can yield
     several events back-to-back after a stall.
  2. Preserve the existing fractional schedule calculation, but turn whole
     arrivals into a batch (`floor` and subtract) rather than a per-arrival
     `while` loop. A budget is active while a dispatcher-local expiry from an
     earlier batch has not yet been enforced by step 1; if none is active, give
     the new batch an expiry of `now + dispatch_tick()`, otherwise (e.g. burst
     events) new arrivals share the existing expiry rather than extending older
     work indefinitely. Expiry is enforced only by step 1 on the tick;
     `try_claim` never checks the clock.
  3. Snapshot parked workers and grow by exactly
     `min(due.saturating_sub(parked), max_vus.saturating_sub(allocated))`. Queue
     those spawns, add the whole due batch to the shared budget, then call
     `notify_waiters()` at most once when the batch is non-zero. Newly spawned
     workers use the same claim path; they are not granted an implicit first
     iteration.
  4. Broadcast intentionally may wake more than `min(due, parked)`. This removes
     explicit targeted wakes but is not O(1) internally: Tokio traverses and
     wakes registered waiters, and all claimers contend on one cache line. The
     required low-rate/high-idle benchmark decides whether that trade is viable.
- Pause keeps the schedule clock behavior already present: expire any published
  unclaimed budget, keep pinning `last = now` on every paused tick as the
  current loop does, publish no paused-time arrivals, and leave workers parked.
  Running workers re-check pause before their next claim; resume is driven by
  the next productive tick's broadcast.
- On natural deadline, soft stop, or scenario cancellation, atomically take and
  record all unclaimed budget, close dispatch so no later claim can succeed, and
  broadcast once so parked workers exit. Linearize closure at `budget.swap(0)`:
  a claim that wins the race before that swap is in flight; a claim that loses
  sees zero. Store the closed flag before waking waiters, and never publish more
  budget after the swap. Join workers as today so in-flight iterations may
  finish until the existing graceful cancellation fires. Do not make idle
  workers wait out the grace period merely because they are parked.
- Preserve `max_vus`, `LOADR_DISPATCH_TICK_US`, pause/resume, soft/hard stop,
  natural deadline, graceful in-flight completion, and both constant- and
  ramping-arrival-rate sharing `RateSchedule`. Batch metric emission; do not
  reintroduce O(arrivals) dispatcher work to count drops.

OUT OF SCOPE
- Multiple dispatchers, per-core budgets, or a sharded Notify ring. If the
  broadcast or atomic A/B is poor, report that evidence and redesign separately.
- Changing the tick default, CLI/env surface, metrics pipeline, gRPC handler,
  public types, or `ExecEnv::run_one`.

CORRECTNESS TESTS
- Keep `arrival_rate_keeps_schedule_without_drops` green. It is a functional
  smoke test at 200/s, not evidence for the 150k/s performance claim.
- Add a saturation test with a deliberately small `max_vus` and slow iterations:
  constant rate, natural deadline, no pause, and `graceful_stop` long enough for
  claimed in-flight iterations to finish (an aborted claim is neither completed
  nor dropped and breaks the identity). Assert completion, positive drops, and
  conservation: completed plus dropped iterations must match scheduled arrivals
  within `ceil(rate * tick) + 1`, widened by one tick's arrivals in wall-clock
  tests because a stalled final tick enlarges the unpublished tail; the exact
  identity belongs in the private helper test below. A test that checks only
  `drops > 0` is insufficient.
- Add focused cases for: spawn growth from `pre_allocated_vus` up to but never
  beyond `max_vus`; many parked workers repeatedly racing small budgets without
  a lost wake or hang; pause/resume publishing no paused-time backlog; natural
  deadline and soft stop starting no post-closure claims; cancellation waking
  parked workers; and a ramping schedule using the same loop.
- Bound concurrency tests with timeouts and repeat the lost-wake case enough to
  exercise both sides of the registration/re-check race. Prefer a small private
  helper test for exact budget/expiry accounting where wall-clock e2e assertions
  would be flaky.
- Tick-specific e2es must set `LOADR_DISPATCH_TICK_US` on the spawned loadr child
  process. `dispatch_tick()` is cached in a process-wide `OnceLock`, so do not
  mutate the parent test process environment and expect independent tick values.
- Set `graceful_stop: 0s` in focused e2es not testing grace behavior or
  conservation; the existing 2s arrival test otherwise spends the default 30s
  grace waiting.

LOCAL PERFORMANCE VALIDATION (required; no AWS rig)
- Build two release binaries with identical current-stable toolchain, lockfile,
  features, and flags: exact base d28814f and the candidate. Keep the artifacts
  at distinct paths and record both revisions and `rustc -Vv`.
- Use a no-network plan whose only flow step is constant think time. Run both a
  zero-duration step (dispatcher/task ceiling) and a 1ms step (workers remain in
  flight across ticks), with no sample-consuming output.
- Matrix: `LOADR_DISPATCH_TICK_US` 1000 and 5000; `--worker-threads` 2 and 16;
  a low-rate/high-preallocated case that exposes broadcast over-waking; and
  fixed 50k, 150k, and 250k arrivals/s ladders where the host can sustain them.
  Size preallocated/max VUs identically for each pair.
- Pin each pair to the same physical cores. Alternate base/candidate order for
  at least five paired runs after warm-up. Capture achieved iterations/s,
  dropped iterations, wall time, and `perf stat` task-clock, cycles,
  instructions, context switches, CPU migrations, and cache misses. Report raw
  runs plus median and dispersion; do not select only favorable cases.
- There is no fixed percentage gate. Present the paired evidence for reviewer
  judgment. If results are neutral, noisy, or regressive, do not call the change
  good; retain the measurements and recommend close-or-redesign. This local A/B
  validates the isolated dispatcher mechanism, not the final Graviton/AWS
  ceiling, which remains part of the later aws-ab-validation goal.

QUALITY BAR
Focused correctness tests and local release A/B as above; no unrelated
refactors; conventional commit, no Claude-Session trailer. Run cargo fmt --all
and cargo clippy --workspace --all-targets -- -D warnings, then cargo test -p
loadr-core -p loadr-cli --locked (workspace suite before the PR: --workspace
--locked --exclude loadr-browser). Use a current stable toolchain capable of
building the locked dependencies.

DONE when: code inspection finds no per-arrival dispatcher loop, idle channel,
or targeted wake; all accounting/lifecycle/race tests pass; max_vus and both
arrival schedules are preserved; and the paired local A/B table is attached
with an evidence-backed recommendation rather than an assumed performance win.
```
