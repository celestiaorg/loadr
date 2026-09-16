# Goal - close the gRPC test gaps from the round-one reviews

The round-one code reviews approved the gRPC hot-path and lazy-decode work
but punted five coverage gaps as follow-ups. None indicates a suspected bug —
each mechanism was verified by inspection — but all five would let a future
regression through silently.

Paste this whole block into a fresh coding-agent session:

```text
/goal Add the five gRPC regression tests punted by the round-one reviews

CONTEXT
- Base branch: risotto. Test homes: crates/loadr-protocols/tests/integration.rs
  (in-process GrpcEchoServer from testsupport/loadr-testserver, vu()/request
  helpers, transport-matrix helpers), crates/loadr-core/tests/grpc_discard_flag.rs
  (engine-level RecordingGrpcHandler harness), crates/loadr-config/src/validate.rs
  (unit tests in-module), crates/loadr-cli/tests/e2e.rs (black-box binary runs;
  already spins up a GrpcEchoServer in one test).
- The five gaps, from the review reports:
  1. Pool distribution: grpc_unary_pooled_channels uses ONE VU, so round-robin
     across VUs and the pool's double-checked-lock hit path are unexercised.
  2. Compile-time literal detection: `json_is_literal` wiring
     (loadr-core/src/flow.rs, compile_request -> grpc_literal_message/_messages)
     is only tested via hand-set flags, never through YAML.
  3. Discard ordering: grpc_unary_discard_skips_body tests decode->discard
     only; discard->decode would catch a future regression to conditional
     codec-flag assignment.
  4. validate.rs rejects `channel_pool_size: 0` with no test.
  5. No end-to-end assertion that a real `loadr run` emits
     grpc_reqs/grpc_req_duration into the summary.

IMPLEMENTATION
1. Two-VU pool test: pool size 2, two VUs, several calls each; observability —
   simplest is asserting on the server side: GrpcEchoServer can count distinct
   client connections (add a connection counter to the test server if absent;
   testsupport/loadr-testserver/src/grpc.rs) == pool size, proving both pooled
   channels carried traffic and VUs shared them.
2. YAML literal detection: engine-level test beside grpc_discard_flag.rs's
   harness — plan A with a fully literal message, plan B with `${vu}` in one
   leaf; RecordingGrpcHandler captures `message_literal` per request; assert
   true/false respectively (and messages Arc-stable across iterations for A if
   cheaply observable).
3. Extend grpc_unary_discard_skips_body: same VU/method, order discard=true
   FIRST then false; assert the decode call still returns a parseable body
   (cache entry not poisoned in either direction).
4. validate.rs unit test: `channel_pool_size: 0` -> the existing rejection
   error; `1` accepted (mirror the sibling grpc validation tests' style).
5. e2e: extend the existing gRPC e2e case (or add one) asserting the exported
   summary JSON contains grpc_reqs with the expected count and a
   grpc_req_duration entry (follow standalone_run_produces_metrics_and_passes'
   summary-export pattern).

OUT OF SCOPE
- New product code beyond a minimal test-server connection counter; fixing
  anything the new tests reveal (file/report separately if red).

TESTS
This goal IS tests. All five must fail meaningfully if their mechanism
regresses (e.g. #3 fails if codec.discard were assigned conditionally; #2
fails if Template::is_literal wiring breaks).

QUALITY BAR
No unrelated refactors; conventional commit, no Claude-Session trailer. Run
cargo fmt --all and cargo clippy --workspace --all-targets -- -D warnings,
then cargo test -p loadr-protocols -p loadr-core -p loadr-config -p loadr-cli
--locked (workspace suite before the PR: --exclude loadr-browser locally).

DONE when: all five tests exist, pass, and each demonstrably fails when its
guarded mechanism is inverted (spot-check by temporary mutation, noted in the
PR description).
```
