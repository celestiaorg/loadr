# Goal - data-source FFI calls that cannot stall the runtime

Plugin-backed data rows are fetched by a synchronous FFI call that runs inline
on a Tokio worker thread: async `run_request` calls sync `prepare`, and the
whole render → resolve → `next_row` chain happens without `spawn_blocking` or
`block_in_place`. A feeder that signs, hashes, or does I/O occupies a runtime
worker for the whole call; with more concurrently-preparing VUs than worker
threads, timers, the arrival dispatcher, and unrelated VUs all stall — the
load shape itself degrades, not just throughput. The sibling protocol-side fix
(`4ae8db1`, `origin/nikolai/fix-blocking-native-plugin-call`, merged to
`risotto`) proved and fixed exactly this hazard for protocol plugins but does
not touch the data-source adapter.

Bracketing is deliberately opt-in per source: `block_in_place` demotes the
current worker and migrates its run queue, so unconditionally paying it on a
microsecond in-memory feeder could cost more than it saves. The default path
must stay byte-identical.

Paste this whole block into a fresh coding-agent session:

```text
/goal Bracket plugin data-source fetches with block_in_place behind an opt-in per-source flag so a slow feeder cannot stall the Tokio runtime

CONTEXT
- Base branch: origin/nikolai/grpc-feeder-native-plugin at bb0a6cb — the home
  of the data_source capability. crates/loadr-core/src/data.rs and the
  NativeDataSourceAdapter block of crates/loadr-plugin-api/src/native.rs are
  byte-identical on risotto, so this merges cleanly there too (native.rs line
  numbers below are bb0a6cb's; risotto's file has drifted around the impl).
- The call chain is synchronous end-to-end with no offload: async
  FlowRunner::run_request invokes sync prepare without await
  (crates/loadr-core/src/flow.rs:1017) → render_template (flow.rs:1936) →
  VuContext::resolve_expr (crates/loadr-core/src/vu.rs:172) → data_row
  (vu.rs:150) → DataFeeds::next_row (crates/loadr-core/src/data.rs:342,
  plugin arm :355-368) → the sync trait method DataSourcePlugin::next_row
  (data.rs:58) → NativeDataSourceAdapter::next_row
  (crates/loadr-plugin-api/src/native.rs:456) → the abi_stable FFI call
  (native.rs:468).
- Both binaries run multi-thread runtimes (crates/loadr-cli/src/commands/
  run.rs:59, agent.rs:42), so block_in_place is available on the hot path.
- Plugin rows are fetched per request: on-demand rows are evicted in
  begin_request (vu.rs:138-145). One slow fetch therefore delays exactly one
  request — but does so while pinning a runtime worker. This is
  code-inspection evidence; the sibling regression test
  v1_protocol_call_does_not_block_core_runtime_timer (added by 4ae8db1,
  risotto-only) demonstrates the runtime-liveness hazard for the protocol
  twin. The load-shape impact of the data-source variant has not been
  measured — the A/B below decides the recommendation, it is not a formality.
- Why the protocol fix does not transfer: NativeProtocolAdapter::execute is
  async, so 4ae8db1 could wrap the FFI in tokio::task::spawn_blocking and
  await it. The data-source call sits under sync prepare; spawn_blocking is
  unawaitable there, and making prepare async is a flow-wide refactor. The
  JS host API faces the same constraint and brackets its synchronous callouts
  with tokio::task::block_in_place (flow.rs:1774, flow.rs:1915) — follow that
  precedent.
- Real feeders motivate the flag: the example tx-signer performs an Ed25519
  signature per row (plugins/examples/native-data-source/src/lib.rs:162),
  tens of microseconds of CPU; an I/O-backed feeder (DB, vault) would be
  milliseconds. A constant-generator feeder is sub-microsecond and must not
  pay for them.

IMPLEMENTATION
- Add `blocking: bool` (serde default false, schemars-documented) to the
  plugin data-source config variant DataSource::Plugin in
  crates/loadr-config/src/plan.rs (schema variant :1954, conversion :2067).
- Thread the flag through DataFeeds::load (data.rs:131) into the per-source
  plugin entry consulted by the DataFeeds::next_row plugin arm (data.rs:355).
- In that arm, when the source is marked blocking, run plugin.next_row(&ctx)
  inside tokio::task::block_in_place; otherwise call it inline exactly as
  today. Keep the bracket in loadr-core, not the adapter: loadr-plugin-api
  stays ABI-untouched and every DataSourcePlugin implementation (native now,
  anything later) is covered.
- Guard the bracket: block_in_place panics on a current_thread runtime and
  outside a runtime. Decide via tokio::runtime::Handle::try_current() +
  .runtime_flavor() (workspace tokio is 1.52) in a small private helper that
  returns bracket-or-inline; fall back to the inline call when the flavor is
  not MultiThread. The helper is the unit-test surface for the decision.
- init stays inline: it runs once per run during engine construction
  (DataFeeds::load), not on the request path.
- Document the flag on the data_source capability page (added by 38a1493)
  with one sentence on when to set it (CPU-heavy or I/O-backed feeders) and
  one on when not to (cheap in-memory generation).

OUT OF SCOPE
- Async prefetch/pipelining of plugin rows (fetch row N+1 on a blocking pool
  while request N is in flight). That is the real throughput fix for slow
  feeders, but it needs compile-time knowledge of which sources a request
  uses — design it separately once grpc-template-precompile lands.
- Making prepare or DataSourcePlugin async; any plugin ABI change; WASM data
  sources (none exist).
- The protocol-adapter path (already fixed by 4ae8db1 on risotto).

CORRECTNESS TESTS
- Data-source twin of v1_protocol_call_does_not_block_core_runtime_timer, in
  loadr-core where a mock DataSourcePlugin already exists (data.rs:515):
  build a `multi_thread` runtime with exactly 1 worker, arm a concurrent
  ~50 ms timer, then start one blocking-flagged source whose `next_row`
  signals entry and sleeps ~300 ms. Assert from elapsed wall time that the
  timer completes while the fetch is still in flight and that the fetched
  row is correct. Synchronize timer arming and plugin entry with channels or
  barriers so task-start order cannot make the test pass accidentally. The
  unchanged inline implementation must delay the timer by roughly the full
  fetch; `block_in_place` lets Tokio install a replacement worker.
- current_thread runtime test: a blocking-flagged source neither panics nor
  deadlocks (the flavor guard falls back to inline).
- Unit-test the bracket-or-inline helper directly for: multi-thread → bracket,
  current-thread → inline, no runtime → inline.
- Config coverage: YAML round-trip of blocking true/false/absent next to the
  existing DataSource::Plugin serde tests (plan.rs:2402); `loadr validate`
  accepts the flag.
- E2e in the shape of plugin_data_source_runs_at_fixed_count_against_noop
  (crates/loadr-cli/tests/e2e.rs:671) with blocking: true — same count and
  threshold assertions must hold.

LOCAL PERFORMANCE VALIDATION (required; no AWS rig)
- Two release binaries, identical toolchain/lockfile/flags (cargo +1.93.0 on
  this machine — default stable is too old): exact base bb0a6cb and the
  candidate. Record revisions and rustc -Vv.
- Plans: protocol `noop` + a plugin data source in the e2e.rs:671 shape,
  scaled up (constant-arrival-rate ladder the host sustains, plus one
  closed-model constant-vus case). Feeders: the tx-signer example as-is
  (fast CPU), and a dev-only mock source with a config-tunable sleep
  (test-support code, not the shipped example) at 1 ms and 5 ms.
- Three comparisons, ≥5 paired alternating runs each after warm-up:
  (a) flag off, base vs candidate — must be statistically indistinguishable
  (the default path is byte-identical; this is the no-regression gate);
  (b) fast feeder, flag on vs off on the candidate — quantifies the
  block_in_place tax so the docs can honestly say when NOT to opt in;
  (c) slow feeder at --worker-threads 2, flag on vs off — expect off to show
  late timers / dropped iterations / sagging achieved rate and on to hold
  the schedule. Capture achieved iterations/s, dropped_iterations, wall
  time, and perf stat context switches + CPU migrations.
- Report raw runs plus median and dispersion. If (c) shows no benefit,
  present that and recommend against the flag rather than landing it anyway.

QUALITY BAR
Focused correctness tests and the local release A/B as above; no unrelated
refactors; conventional commit, no Claude-Session trailer. Run cargo fmt --all
and cargo clippy --workspace --all-targets -- -D warnings, then cargo test -p
loadr-core -p loadr-config -p loadr-cli --locked (workspace suite before the
PR: --workspace --locked --exclude loadr-browser). Use a current stable
toolchain capable of building the locked dependencies.

DONE when: the flag parses, validates, and is documented; a blocking-flagged
300 ms feeder cannot delay a concurrent timer on a saturated runtime; the
default path is provably unchanged (test + A/B gate (a)); and the paired A/B
table is attached with an evidence-backed recommendation, including the
measured cost of opting in on a fast feeder.
```
