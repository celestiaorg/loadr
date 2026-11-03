# Goals - perf round three

Findings from the 2026-07 exploration of the request-generation path —
load shape → plugin data feeder → vanilla gRPC request preparation (stopping
before submission) — cross-checked against `risotto`. Two conclusions drove
this set: the plugin data-source ("feeder") hot path is unfixed everywhere
(`4ae8db1` offloaded only the *protocol* adapter's FFI; `data.rs` and the
data-source adapter are byte-identical between the feature branch and
`risotto`), and the round-one/two gRPC caches cover *static* content only —
a message with one `${data.*}` substitution re-parses templates per string
leaf, rebuilds the JSON tree, deep-clones it into `DynamicMessage`, and
traverses it twice for encoding, on every call, even on `risotto`.

Each goal is independent, sized for its own branch/PR, and written for a
fresh agent with no prior context. Unlike round two, goals do **not** assume
`risotto` as the base: each names its base explicitly, following the
convention that perf changes base on `main` or the owning feature branch —
feeder goals on `origin/nikolai/grpc-feeder-native-plugin` at `bb0a6cb` (the
touched files are byte-identical on `risotto`, so they merge cleanly there),
gRPC goals on `origin/nikolai/perf-grpc-call-cache-memoization` at `1c3d683`
(the request-prep lineage; `risotto` has drifted since — raw transport and
lazy decode — so expect a mechanical, symbol-anchored rebase). Once the
underlying branches land on `main`, every goal applies there directly.

| Goal | Outcome |
|---|---|
| [feeder-ffi-offload](feeder-ffi-offload.md) | Opt-in `blocking` flag brackets data-source FFI with `block_in_place`; a slow feeder can no longer stall the runtime |
| [feeder-row-marshalling](feeder-row-marshalling.md) | No intermediate value tree, per-field clones, per-fetch clock read, or per-fetch key allocation in the plugin row path |
| [feeder-seq-counter](feeder-seq-counter.md) | Plugin sequence counters are lock- and alloc-free per fetch, still shared across parallel branches |
| [grpc-template-precompile](grpc-template-precompile.md) | gRPC message/metadata templates parse once at compile time; zero `Template::parse` on the per-call render path |
| [grpc-encode-once](grpc-encode-once.md) | Rendered gRPC messages: no deep clone into `DynamicMessage`, exactly one encode, `bytes_sent` from the buffer |

Deliberately not goal-worthy (noted for completeness): the example
tx-signer's cross-VU `generated: AtomicU64` uses `SeqCst` under `limit`
(example code; make it `Relaxed` if it ever shows up in a profile), and the
closed-model per-iteration `watch::Receiver` clones in
`ExecEnv::wait_unpaused` / the ramping pool select (noise-level until an A/B
says otherwise). The remaining open dispatcher work stays in
[perf-round-two/dispatcher-idle-ring](../perf-round-two/dispatcher-idle-ring.md).

Shared quality bar (embedded in every goal): focused regression tests for the
change itself; perf claims measured in release mode with paired A/B runs;
`cargo fmt --all`;
`cargo clippy --workspace --all-targets -- -D warnings`; workspace tests green
(locally: `--exclude loadr-browser` — its tests need Chrome — and
`rustup target add wasm32-wasip2` once for the plugin-api tests); conventional
commits, no `Claude-Session:` trailers; never combined with unrelated
refactors. Note: pushes to `risotto` do not trigger CI — run the gates
locally.
