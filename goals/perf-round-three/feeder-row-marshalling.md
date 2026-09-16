# Goal - cheaper feeder row marshalling

Every plugin row fetch performs two full JSON encode/decode round-trips plus a
row rebuild: the host serializes the fetch context to a `String`, the plugin
parses it and serializes its response, and the host parses that response into
a `serde_json` map only to immediately rebuild it into a `Row`, cloning every
key and re-stringifying every value, then heap-allocates an `Arc` around the
result. The ABI design note says JSON-in-`RString` marshalling cost is
"irrelevant at plugin-boundary call rates", but the same file admits
`next_row` "is on the request hot path and is called concurrently from VU
threads" — and on-demand rows are refetched for every request. This goal keeps
the JSON-in-`RString` ABI exactly as is and removes the avoidable work around
it: the intermediate value tree, the per-field clones, and the per-fetch
incidentals (a `SystemTime` read, repeated `String` key allocations).

The wins here are allocation-count reductions measured in fractions of a
microsecond each; they matter because they multiply by rows/s. Treat the A/B
as the arbiter — if the end-to-end gain is not visible on the no-op harness,
report that honestly.

Paste this whole block into a fresh coding-agent session:

```text
/goal Cut per-fetch allocations in the data-source marshalling path without changing the JSON-in-RString ABI

CONTEXT
- Base branch: origin/nikolai/grpc-feeder-native-plugin at bb0a6cb.
  crates/loadr-core/src/data.rs, crates/loadr-core/src/vu.rs, and the
  data-source portion of crates/loadr-plugin-api/src/native.rs are
  byte-identical on risotto, so the change merges cleanly there (native.rs
  line numbers are bb0a6cb's).
- Host-side fetch path, per (request, source) because on-demand rows are
  evicted every request (vu.rs:138-145):
  1. DataFeeds::next_row plugin arm (data.rs:355-368) builds a PluginRowCtx
     including ts_ms: crate::metrics::now_millis() — a SystemTime::now()
     read per fetch (data.rs:364) — and wraps the returned row in
     Arc::new(row) (data.rs:367).
  2. NativeDataSourceAdapter::next_row (native.rs:456) copies the ctx into
     FfiRowCtx (native.rs:392, all-borrowed fields), serializes it with
     serde_json::to_string (native.rs:466), moves the String into an RString
     for the FFI call (native.rs:468), parses the reply into FfiRowResponse
     { row: Option<serde_json map>, exhausted } (native.rs:405, :472), then
     rebuilds Row = IndexMap<String, String> by cloning every key and
     json_to_string-ing every value (native.rs:480-483;
     loadr_core::vu::json_to_string at vu.rs:210).
  3. VuContext::data_row inserts the row under a freshly allocated key:
     current_rows.insert(source.to_string(), row.clone()) (vu.rs:164) — and
     because plugin rows are evicted per request, this key allocation is per
     fetch, not per iteration. begin_request itself allocates
     current_request = Some(name.to_string()) and does a full retain scan of
     current_rows per request (vu.rs:142-144).
- Contradiction worth preserving in the commit message: the abi.rs design
  note (crates/loadr-plugin-api/src/abi.rs:4-9) assumed per-flush/per-request
  cadence is cheap; abi.rs:71-72 admits next_row is the request hot path.
  The ABI stays; the surrounding copies go.
- Evidence status: code inspection. No profile isolates these allocations
  yet; the required A/B below is the evidence.

IMPLEMENTATION
- Deserialize straight into the final row shape. Give FfiRowResponse (or a
  new sibling used by the adapter) a custom Deserialize whose row field
  builds IndexMap<String, String> directly with a map visitor: string values
  deserialize as owned Strings with no re-allocation, numbers/bools/null
  stringify during visitation, and nested arrays/objects (rare) may fall back
  to deserializing a serde_json::Value for that leaf and reusing
  json_to_string so output text is identical to today for every input.
  Kills: the intermediate map, the per-key clone, and the per-value
  json_to_string re-walk at native.rs:480-483.
- Hoist the clock read: compute ts_ms once in `VuContext::begin_request`,
  retain it as request state, and pass it through `RowIdentity` into every
  plugin fetch instead of calling now_millis() in `DataFeeds::next_row`
  (data.rs:364). Multiple sources fetched by one request then share the same
  millisecond timestamp — document that in the PluginRowCtx field docs; it
  was never guaranteed to be a per-call clock.
- Stop re-allocating map keys per fetch: intern source names once in
  DataFeeds::load (data.rs:131) as Arc<str> and key current_rows by Arc<str>
  (HashMap<Arc<str>, Arc<Row>> — lookups still take &str via Borrow<str>).
  data_row's insert then clones a refcount, not a String (vu.rs:164).
- Trim begin_request (vu.rs:138-145): keep the has_on_demand early-out,
  reuse the current_request String buffer (clear + push_str) instead of a
  fresh to_string per request, and only retain-scan when the previous
  request actually inserted an on-demand row (a small bool or count avoids
  scanning a map that holds only per-iteration CSV rows).
- The ctx serialization String (native.rs:466) stays: RString::from(String)
  takes ownership of the buffer, and FfiRowCtx is a handful of scalar
  fields — measure before micro-optimizing further.
- Behavior must be observably identical: same Row contents for every JSON
  value shape, same error strings for malformed replies, same
  exhausted/missing-row handling (native.rs:474-479).

OUT OF SCOPE
- Any ABI or wire-format change (binary encoding, borrowed FFI types) and
  any batching API (next_rows(n) as an abi_stable suffix method is a
  separate, future goal).
- Plugin-side costs (the example plugin's own parse/serialize).
- The sequence-counter lock/alloc (sibling goal feeder-seq-counter).
- The blocking-FFI hazard (sibling goal feeder-ffi-offload).

CORRECTNESS TESTS
- Adapter round-trip parity in loadr-plugin-api: for row values covering
  strings (incl. unicode + embedded quotes), integers, floats, booleans,
  null, nested arrays and objects, assert the produced Row equals the
  base implementation's output exactly (port the old rebuild loop into the
  test as the oracle). Cover exhausted: true, a missing row+exhausted reply
  (error), and malformed JSON (error text unchanged).
- loadr-core: existing tests stay green, in particular
  plugin_next_row_is_callable_concurrently (data.rs:915),
  has_on_demand_and_is_on_demand_flags (data.rs:951),
  data_row_stable_within_iteration (vu.rs:294), and
  parallel_plugin_data_source_sequences_are_unique_per_vu_source
  (crates/loadr-core/tests/flow_control.rs:331).
- New timestamp regression: fetch plugin source A, whose mock `next_row`
  records its ctx and then sleeps long enough to cross a millisecond boundary
  (for example 20 ms), followed by plugin source B in the same request. Assert
  both observe the same ts_ms. On the base implementation B receives a later
  per-fetch timestamp, so equality proves request-level hoisting rather than
  succeeding because two clock reads happened within one tick. A controllable
  test clock that returns distinct values on successive reads is an acceptable
  deterministic alternative.
- New retain-scan regression: load a mixed plan with at least one CSV source
  and one plugin source so `has_on_demand` is true. Begin a request that fetches
  only CSV data, begin the next request, and assert via the new bookkeeping
  (not timing) that no retain scan occurred. Also fetch a plugin row once and
  assert the following request does scan and evict that row while retaining
  the CSV row; this covers both sides of the bookkeeping transition.
- E2e plugin_data_source_runs_at_fixed_count_against_noop
  (crates/loadr-cli/tests/e2e.rs:671) unchanged and green.

LOCAL PERFORMANCE VALIDATION (required; shared harness with feeder-seq-counter)
- Two release binaries (cargo +1.93.0), base bb0a6cb vs candidate (stack the
  feeder-seq-counter change on top if both are in flight and A/B the pair —
  state exactly what each binary contains).
- Plan: protocol noop + tx-signer source in the e2e.rs:671 shape, scaled to a
  closed-model iterations ladder and one constant-arrival-rate case; no
  sample-consuming output. The signer's Ed25519 dominates per-row cost, so
  also run a dev-only trivial source (constant row) where marshalling is the
  whole cost — that is the sensitive configuration.
- ≥5 paired alternating runs per configuration after warm-up; capture rows/s
  (== iterations/s here), wall time, and perf stat task-clock, instructions,
  cache misses. Report raw + median + dispersion; call out a null result as
  a null result.

QUALITY BAR
Focused correctness tests and the local release A/B as above; no unrelated
refactors; conventional commit, no Claude-Session trailer. Run cargo fmt --all
and cargo clippy --workspace --all-targets -- -D warnings, then cargo test -p
loadr-core -p loadr-plugin-api -p loadr-cli --locked (workspace suite before
the PR: --workspace --locked --exclude loadr-browser; run
`rustup target add wasm32-wasip2` once for the plugin-api tests). Use a
current stable toolchain capable of building the locked dependencies.

DONE when: code inspection finds no intermediate serde_json map, no per-key
clone, no per-fetch SystemTime read, and no per-fetch String key allocation
on the plugin row path; the parity corpus and all listed existing tests pass;
and the paired A/B table (including the marshalling-dominated trivial-source
case) is attached with an honest conclusion.
```
