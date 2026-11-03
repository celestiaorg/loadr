# Goals - perf round two

Follow-ups left open after the 2026-07 request-hot-path work (dispatcher fix,
gRPC call cache + lazy decode, metrics delta/sharding, raw h2 transport — all
merged into `risotto` at `a12b541`). Each goal is independent, sized for its
own branch/PR, and written for a fresh agent with no prior context. Goals
assume `risotto` as the base branch, except `dispatcher-idle-ring`, which is
intentionally based directly on `origin/nikolai/perf-dispatcher-port` at
`d28814f` to keep that rewrite independent. Once the five `nikolai/perf-*` PRs
land on `main`, everything except `flow-emitter-dead-code` applies there too.

| Goal | Outcome |
|---|---|
| [dispatcher-idle-ring](dispatcher-idle-ring.md) | Measure and, only if it wins, replace the arrival dispatcher's per-iteration idle-queue hand-off |
| [interned-tag-sets](interned-tag-sets.md) | No per-request `BTreeMap`/`Arc<Tags>` allocation in metric emission |
| [grpc-call-cache-memoization](grpc-call-cache-memoization.md) | gRPC URL parse and metadata parse move into the per-VU call cache |
| [coarse-sample-clock](coarse-sample-clock.md) | Measure, and only if it pays: cached coarse timestamps for bus-mode samples |
| [grpc-test-gaps](grpc-test-gaps.md) | Close the test-coverage gaps flagged by the round-one reviews (pool distribution, literal detection, discard ordering, e2e metric names) |
| [metrics-followups](metrics-followups.md) | Bus-vs-shard duration regression test + panic-safe shard cleanup |
| [flow-emitter-dead-code](flow-emitter-dead-code.md) | Remove the orphaned `FlowRunner` emitter (risotto fails `clippy -D warnings` today) + two small inherited cleanups |
| [aws-ab-validation](aws-ab-validation.md) | AWS A/B campaign validating raw transport, shard metrics, and the new knobs against round-one baselines |

Shared quality bar (embedded in every goal): focused regression tests for the
change itself; perf claims measured in release mode; `cargo fmt --all`;
`cargo clippy --workspace --all-targets -- -D warnings`; workspace tests green
(locally: `--exclude loadr-browser` — its tests need Chrome — and
`rustup target add wasm32-wasip2` once for the plugin-api tests); conventional
commits, no `Claude-Session:` trailers; never combined with unrelated
refactors. Note: pushes to `risotto` do not trigger CI — run the gates
locally.
