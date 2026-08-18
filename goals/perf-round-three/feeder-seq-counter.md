# Goal - lock-free plugin sequence counters

Every plugin row fetch takes a `parking_lot::Mutex` and heap-allocates a
`String` just to increment a per-source sequence number:
`VuFeedState::next_plugin_seq` locks `plugin_sequences` and calls
`sequences.entry(source.to_string())` on every call, hit or miss. The mutex
exists so parallel branches of one iteration share the counter (the `e168afb`
fix — before it, forked branches restarted at zero and produced duplicate
sequence values). Contention is therefore bounded to a VU's own parallel
branches, but the lock/unlock and the allocation are unconditional, per fetch,
on every VU. This goal keeps the sharing semantics exactly and makes the
steady-state fetch lock-free and allocation-free.

Paste this whole block into a fresh coding-agent session:

```text
/goal Make plugin sequence counters lock- and alloc-free per fetch while preserving cross-branch sharing

CONTEXT
- Base branch: origin/nikolai/grpc-feeder-native-plugin at bb0a6cb.
  crates/loadr-core/src/data.rs is byte-identical on risotto, so this merges
  cleanly there.
- Current design (crates/loadr-core/src/data.rs):
  - VuFeedState.plugin_sequences: Arc<parking_lot::Mutex<HashMap<String, u64>>>
    (data.rs:90).
  - next_plugin_seq (data.rs:106-112): lock, entry(source.to_string())
    — a String allocation on every call, even when the key exists —
    increment, return previous.
  - Called from the DataFeeds::next_row plugin arm on every fetch
    (data.rs:356). On-demand rows are refetched per request, so this is
    per-request cadence per source.
  - fork_for_parallel (data.rs:98-103) gives each parallel branch fresh
    cursors/shuffles but Arc-clones plugin_sequences — that sharing is the
    e168afb guarantee: two branches in the same iteration must never observe
    the same (vu, source, seq).
  - Each VU owns an independent VuFeedState (fresh Arc), so there is no
    cross-VU contention to preserve or worry about.
- Semantics to keep byte-for-byte: sequences start at 0, increment by 1 per
  fetch, are per (VU, source), never reset within a run, and are shared
  across parallel branches of the same VU.
- Evidence status: code inspection. The cost is one parking_lot lock/unlock
  plus one small heap allocation per fetch — real but small; the shared A/B
  below is the arbiter.

IMPLEMENTATION
- Replace the map values with shared atomic slots and add a per-branch local
  cache:
  - shared: Arc<parking_lot::Mutex<HashMap<String, Arc<AtomicU64>>>> —
    consulted only the first time a branch touches a source;
  - local: HashMap<String, Arc<AtomicU64>> (unsynchronized, per
    VuFeedState) — every later fetch is local.get(source) (borrowed &str
    lookup, no allocation) + fetch_add(1, Ordering::Relaxed).
- Relaxed is sufficient and must be justified in a comment: the counter is
  the only shared datum, uniqueness needs the atomicity of fetch_add, and no
  other memory is published through it. Do not silently use SeqCst.
- Slot creation: on local miss, lock the shared map once,
  entry(source.to_string()).or_insert_with(Arc::default), clone the Arc into
  the local cache. The String allocation and lock now happen once per
  (branch, source), not per fetch.
- fork_for_parallel: clone the shared map Arc, start with an empty local
  cache (mirrors today's shape at data.rs:98-103).
- Keep the whole change inside VuFeedState (data.rs:86-113) — the call site
  (data.rs:356) and DataSourcePlugin API do not change.
- If feeder-row-marshalling's interned Arc<str> source names land first,
  key both maps by Arc<str> for free; do not couple the two changes.

OUT OF SCOPE
- The FFI marshalling and clock/key allocations around the fetch (sibling
  goal feeder-row-marshalling).
- The blocking-FFI hazard (sibling goal feeder-ffi-offload).
- CSV/file feed cursors and shuffles (untouched).
- Any persistence or cross-VU sequence semantics (none exist today).

CORRECTNESS TESTS
- The e168afb regression stays green:
  parallel_plugin_data_source_sequences_are_unique_per_vu_source
  (crates/loadr-core/tests/flow_control.rs:331).
- New unit tests on VuFeedState directly:
  - one state, one source: sequences are 0,1,2,… in order;
  - fork two branches, interleave next_plugin_seq calls from both (spawn two
    threads doing N fetches each): the 2N observed values are exactly
    {0,…,2N-1} with no duplicates;
  - two sources advance independently;
  - a branch created after the parent has advanced continues from the shared
    value, never from 0.
- Bound the threaded test with a timeout and enough iterations to exercise
  the local-miss → shared-map race on both sides.

LOCAL PERFORMANCE VALIDATION (bundled)
- Do not build a separate harness: this change rides the
  feeder-row-marshalling A/B (same no-op protocol + plugin source plans,
  same paired-run protocol). State explicitly which binaries contain which
  of the two changes. If this lands alone, run that goal's harness with just
  this diff; the marshalling-dominated trivial-source case is the sensitive
  one.

QUALITY BAR
Focused correctness tests as above; no unrelated refactors; conventional
commit, no Claude-Session trailer. Run cargo fmt --all and cargo clippy
--workspace --all-targets -- -D warnings, then cargo test -p loadr-core
--locked (workspace suite before the PR: --workspace --locked --exclude
loadr-browser). Use a current stable toolchain capable of building the locked
dependencies.

DONE when: code inspection finds no mutex acquisition and no allocation on
the steady-state next_plugin_seq path (first-touch-per-branch excepted); the
ordering choice is justified in a comment; all listed tests pass including
the cross-branch uniqueness race test; and the bundled A/B reports which
binaries carried the change.
```
