# Goal - remove the orphaned FlowRunner emitter and two inherited cleanups

risotto currently fails `cargo clippy --workspace -- -D warnings`: the
`RequestMetricEmitter` refactor left the old `FlowRunner` metric-emission
methods in place with zero callers. Invisible day-to-day because risotto
pushes don't run CI — but it will block the next PR that touches loadr-core.
Two more one-liners from the round-one reviews ride along.

Paste this whole block into a fresh coding-agent session:

```text
/goal Delete the dead FlowRunner metric emitter and apply two small inherited cleanups in flow.rs/grpc.rs

CONTEXT
- Base branch: risotto (this goal is risotto-specific — main does not have
  the emitter refactor).
- Dead code: crates/loadr-core/src/flow.rs — `FlowRunner::emit_request_metrics`
  (:1291) and `FlowRunner::emit_named` (:1442) have no callers anywhere (the
  live path is `RequestMetricEmitter::emit_request_metrics` at :1872 with its
  own `emit_named` at :1982, called via `self.emitter.…`). `cargo check -p
  loadr-core` prints the dead_code warning; clippy -D warnings fails on it.
- Cleanup 2 (review finding): in the LIVE emitter's generic `other =>` arm,
  the inner family match still lists `"grpc"` and its comment still claims
  grpc shares the generic path — unreachable since the dedicated `"grpc" =>`
  arm (interned metrics) intercepts first. Drop `"grpc"` from that pattern
  and fix the comment (tcp/udp stay).
- Cleanup 3 (review finding): crates/loadr-protocols/src/grpc.rs `execute`
  builds the same `raw: Vec<&serde_json::Value>` expression in both the
  literal and non-literal outbound branches — hoist it above the match
  (verbatim inherited duplication; behavior identical).

IMPLEMENTATION
- Delete the two dead FlowRunner methods wholesale; remove any imports that
  become unused. Do NOT touch the RequestMetricEmitter versions.
- Apply cleanups 2 and 3 exactly as scoped — no other refactoring in either
  file.
- One commit, e.g. `chore(core): remove emitter refactor leftovers`; mention
  the clippy -D warnings restoration in the body.

OUT OF SCOPE
- Any behavior change (metric names/values byte-identical); the HostContext
  JS-bridge emission path; enabling CI on risotto pushes (worth raising
  separately with the humans, not doing here).

TESTS
- No new tests: existing metric-emission tests (flow.rs mod tests, engine
  tests, e2e summary assertions) pin behavior. The gate that matters:
  clippy -D warnings goes from red to green on the branch.

QUALITY BAR
No unrelated refactors; conventional commit, no Claude-Session trailer. Run
cargo fmt --all and cargo clippy --workspace --all-targets -- -D warnings
(must pass — that is the point), then cargo test -p loadr-core -p
loadr-protocols --locked (workspace suite before the PR: --exclude
loadr-browser locally).

DONE when: cargo clippy --workspace --all-targets -- -D warnings exits 0 on
the branch, grpc no longer appears in the generic family arm, the duplicated
raw extraction is hoisted, and all existing tests pass unchanged.
```
