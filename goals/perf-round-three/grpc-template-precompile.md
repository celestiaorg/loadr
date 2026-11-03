# Goal - precompiled gRPC message and metadata templates

HTTP requests pre-parse their url, headers, and body into `Template`s once at
compile time; gRPC requests do not. The compiled request keeps the raw JSON
message, and every call re-discovers its structure: `render_json` calls
`Template::parse` on every string leaf — each parse copies the whole source
string and allocates a parts vector — and rebuilds the full JSON tree as a
fresh owned `Value`; metadata values go through `render_str`, which parses
again. The per-VU call cache (`71fcbe9`) added a compile-time fast path, but
only for messages with no substitutions at all: one `${data.*}` anywhere and
the entire message takes the parse-per-leaf path on every call — which is
exactly the data-feeder case this round targets. This goal moves all gRPC
template parsing to compile time, mirroring the HTTP path, and stops
re-cloning the static `GrpcRequest` scalars per call while at it.

Paste this whole block into a fresh coding-agent session:

```text
/goal Parse gRPC message and metadata templates once at compile time and render per call without re-parsing, matching the HTTP path

CONTEXT
- Base branch: origin/nikolai/perf-grpc-call-cache-memoization at 1c3d683 —
  the tip of the gRPC request-prep lineage (contains the per-VU call cache +
  literal encoded-body cache 71fcbe9, interned metric names + cache-identity
  fix a09309c, and URL/metadata memoization 1c3d683). All line numbers below
  are 1c3d683's. risotto contains this lineage but has drifted since (raw
  transport, lazy decode landed after) — anchor by symbol when rebasing;
  the render path is conceptually unchanged there.
- HTTP precedent (crates/loadr-core/src/flow.rs): CompiledRequest holds
  url: Template (:178) and headers: Vec<(String, Template)> (:179), parsed
  once in compile_request (:366) via the helper at :363. gRPC gets no such
  treatment: CompiledRequest carries the raw config JSON plus only
  grpc_literal_message / grpc_literal_messages (:194-195), computed at
  :455-465 by json_is_literal (:2044) — which itself Template::parses every
  leaf once at compile time and throws the parse away.
- Per-call cost today, prepare's grpc arm (flow.rs:1637-1675): a non-literal
  message runs render_json (:2055), which Template::parses every string leaf
  (:2063; Template::parse copies the full source string,
  crates/loadr-config/src/template.rs:45 and :100) and rebuilds the whole
  tree; every metadata value runs render_str (:2030, parse at :2036); and
  the GrpcRequest construction clones proto_files/proto_includes
  Vec<PathBuf>s and service/method Strings (:1663-1667) on every call.
- Single-expression splice semantics that MUST be preserved exactly
  (render_json :2063-2072): a string leaf whose template is a single
  expression renders and then serde_json::from_str-parses the result,
  splicing numbers/objects/arrays into the tree, falling back to a string;
  multi-part templates always render to a string; `$${` stays an escape
  (template.rs parser). Literal leaves keep their original value.
- Downstream identity contract (crates/loadr-protocols/src/grpc.rs): the
  per-VU CachedCall compares service/method/proto paths in matches()
  (:228, :234), and protocol.rs's GrpcRequest (crates/loadr-core/src/
  protocol.rs:80) already ships message as Option<Arc<Value>> +
  message_literal so the handler can cache encoded bytes by Arc identity.
  Rendered (non-literal) messages must keep producing a fresh Arc per call —
  the encoded-body cache keys on Arc pointer identity and must not see two
  different messages behind one pointer.
- Evidence status: code inspection; the parse-per-leaf cost scales with
  message size × call rate. The A/B below is the arbiter.

IMPLEMENTATION
- Add a compiled JSON-template tree in loadr-core (flow.rs, next to
  Template): enum CompiledJson { Lit(Arc<serde_json::Value>), Str(Template),
  Splice(Template), Array(Vec<CompiledJson>), Object(Vec<(String,
  CompiledJson)>) }.
  - Compile: string leaf → Template::parse once; is_literal → fold to Lit;
    single-expression template → Splice; otherwise Str. Arrays/objects whose
    children are all Lit collapse into one Lit holding the original subtree
    (Arc-shared, zero per-call work). Non-string scalars → Lit.
  - Render: walk the tree building an owned Value; Lit clones out of the Arc
    only where a parent is dynamic (a fully-Lit root is the literal case
    below); Splice reproduces the from_str-or-string fallback; Str renders
    via render_template. No Template::parse anywhere at render time.
- CompiledRequest: replace the raw grpc message JSON usage with
  grpc_message: Option<CompiledJson>, grpc_messages: Vec<CompiledJson>, and
  grpc_metadata: Vec<(String, Template)>, all built in compile_request.
  grpc_literal_message/grpc_literal_messages (:194-195) and json_is_literal
  (:2044) are subsumed: "root is Lit" is the literal condition; keep the
  existing GrpcRequest.message_literal semantics and the same Arcs handed
  out every call for stable encoded-body cache identity.
- prepare's grpc arm: render from the compiled tree; metadata values render
  without parsing. Behavior parity includes errors: a template that fails to
  parse must now fail at compile_request (engine build) instead of at
  request time — that is an intentional, strictly-better change; surface it
  with the same error text prefix compile_request uses for url/header
  templates (:363).
- Stop the per-call static clones (:1663-1667): make GrpcRequest carry
  proto_files/proto_includes as Arc<Vec<PathBuf>> and service/method as
  Arc<str> (protocol.rs:80), cloned refcounts per call. Update the grpc.rs
  consumers mechanically (matches() comparisons :228/:234 compare through
  the Arc; pool_from_protos and method lookup take &[PathBuf]/&str as now).
  risotto note: its GrpcRequest gained extra fields (transport, discard flag)
  — the Arc-ing rebases mechanically.
- Keep render_json/render_str for their remaining callers (JS vars, plugin
  options, body JSON at :2055 callers other than grpc) — do not refactor
  them in this change beyond what the grpc arm needs.

OUT OF SCOPE
- Encoding/DynamicMessage changes and bytes_sent (sibling goal
  grpc-encode-once; neither blocks the other). `metadata_literal` covers only
  gRPC metadata, not ordinary request headers or `beforeRequest` overrides,
  so it must not by itself bypass the handler's merged-metadata cache match.
- HTTP/WS/socket/plugin render paths and any render-semantics change.
- The reflection path, channel pooling, transports, response handling.

CORRECTNESS TESTS
- Render parity: port the base render_json into the test module as an oracle
  and assert new-vs-old equality over a matrix: fully-literal message;
  nested mixed static/dynamic objects and arrays; single-expression splice
  producing number, bool, object, array, and invalid-JSON-falls-back-to-
  string; multi-part string templates; `$${` escapes; unicode; metadata
  with literal and templated values. Drive dynamic values through a real
  VuContext with vars and a plugin/CSV data source so ${data.*} is covered.
- Literal-path identity: for a literal message, the Arc handed to the
  handler is pointer-identical across calls (encoded-body cache contract);
  for a templated message, consecutive calls yield distinct Arcs.
- Compile-time failure: an invalid template in message or metadata fails
  engine build with the compile_request error shape, and `loadr validate`
  reports it.
- Existing suites stay green, in particular the literal-cache and call-cache
  tests in crates/loadr-protocols/tests/integration.rs and the loadr-core
  flow tests from the 71fcbe9/1c3d683 lineage.

LOCAL PERFORMANCE VALIDATION (required; shared harness with grpc-encode-once)
- Two release binaries (cargo +1.93.0), base 1c3d683 vs candidate.
- Server: the in-repo loadr-testserver gRPC echo
  (testsupport/loadr-testserver, grpc.rs), loopback, TLS off.
- Plans: unary echo with (a) a small templated message (~6 leaves, 2
  dynamic), (b) a large templated message (~100 leaves, a handful dynamic —
  the parse-per-leaf worst case), (c) the same messages fully literal as a
  no-regression guard on the fast path. Closed-model ladder the host
  sustains; no sample-consuming output.
- ≥5 paired alternating runs per configuration after warm-up; capture
  iterations/s, wall time, perf stat instructions + task-clock, and one
  flamegraph per side for case (b) — Template::parse should vanish from the
  render stack. Report raw + median + dispersion.

QUALITY BAR
Focused correctness tests and the local release A/B as above; no unrelated
refactors; conventional commit, no Claude-Session trailer. Run cargo fmt --all
and cargo clippy --workspace --all-targets -- -D warnings, then cargo test -p
loadr-core -p loadr-config -p loadr-protocols --locked (workspace suite
before the PR: --workspace --locked --exclude loadr-browser). Use a current
stable toolchain capable of building the locked dependencies.

DONE when: code inspection finds no Template::parse on the per-call gRPC
render path and no per-call Vec<PathBuf>/String clone of the static
GrpcRequest scalars; the parity matrix (including splice fallbacks and
escapes) passes against the ported oracle; literal-path Arc identity is
proven by test; invalid templates fail at build time; and the paired A/B
table plus the case-(b) flamegraphs are attached.
```
