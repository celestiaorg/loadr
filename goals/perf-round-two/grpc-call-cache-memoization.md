# Goal - memoize gRPC URL and metadata parsing in the per-VU call cache

The per-VU `CachedCall` already caches descriptors, path, codec, and client,
but every request still re-parses the URL (`url::Url::parse`) and re-parses
every metadata pair (`MetadataKey::from_bytes` + value parse). Both are
invariant whenever the rendered strings are unchanged — the common case.

Paste this whole block into a fresh coding-agent session:

```text
/goal Move gRPC URL parsing and metadata parsing into the per-VU call cache, keyed on the rendered strings

CONTEXT
- Base branch: risotto. File: crates/loadr-protocols/src/grpc.rs.
- Per-request today, inside `execute`: `self.endpoint_uri(&request.url)?`
  (:680; parser at :413, returns (endpoint String, tls bool)) and
  `build_metadata(grpc, &request.headers)?` (:880; builder at :980 —
  MetadataKey::from_bytes + value.parse() per pair).
- `CachedCall` (~:197) with `CachedCallIdentity` (~:209) and `matches()`
  already keys on endpoint/service/method/proto identity/pool size/transport.
  The cache is per-VU (in `GrpcChannels` inside ctx extensions), so no locks.
- Templated urls/metadata are rendered in `prepare` (loadr-core/src/flow.rs)
  BEFORE reaching the handler — the handler sees plain strings that may
  differ per iteration when templated, and are byte-identical otherwise.
- tonic consumes the `MetadataMap` per call (`*request.metadata_mut() =
  metadata`), so a cached map must be cloned per call — a clone of a built
  MetadataMap is still much cheaper than re-parsing keys/values.

IMPLEMENTATION
- URL: `CachedCallIdentity` already effectively pins the endpoint; make the
  identity compare the RAW url string (store `url: String`) so a templated
  url that changes forces a rebuild, and stop calling `endpoint_uri` on cache
  hits (store the parsed (endpoint, tls) in `CachedCall`). Verify `matches()`
  call sites pass the raw url.
- Metadata: store the source pairs (`Vec<(String, String)>` as rendered) +
  the built `MetadataMap` in `CachedCall`. On each request compare the
  incoming pairs against the stored ones (cheap Vec equality; typical size
  0-3); on equality clone the cached map, else rebuild and replace. Note the
  gRPC request also merges `request.headers` — the comparison must cover the
  exact same inputs `build_metadata` consumes.
- Keep the rebuild path identical to `build_metadata` today (same errors).
- Cache identity semantics must not change otherwise: same entry count, no
  behavior change for templated plans beyond fewer parses.

OUT OF SCOPE
- Interning across VUs; changing prepare/rendering; caching for non-gRPC
  protocols; touching `ready()`/transport logic.

TESTS
- Extend crates/loadr-protocols/tests/integration.rs (GrpcEchoServer,
  existing vu()/grpc_request() helpers):
  - alternation test: one VU, two requests to the same method with DIFFERENT
    metadata pairs alternating — both succeed; note the echo server does NOT
    echo metadata back, so assert correctness at the unit level: factor the
    memo compare/rebuild into a testable fn and unit-test hit (ptr-stable or
    equality) vs miss (rebuilt) with alternating inputs, plus the
    integration-level smoke that alternating metadata still round-trips.
  - templated-url safety: two urls to the same method (spawn two echo
    servers) on one VU -> two cache entries / correct routing (extends the
    existing includes-alternation test pattern).
- Existing cache tests (proto_includes discrimination, pooled channels,
  discard tests) stay green.

QUALITY BAR
Focused regression tests as above; no unrelated refactors; conventional
commit, no Claude-Session trailer. Run cargo fmt --all and cargo clippy
--workspace --all-targets -- -D warnings, then cargo test -p loadr-protocols
--locked (workspace suite before the PR: --exclude loadr-browser locally).

DONE when: a repeated identical gRPC request performs no url::Url::parse and
no MetadataKey::from_bytes after its first iteration (code inspection + the
unit tests), with the alternation tests proving templated inputs still work.
```
