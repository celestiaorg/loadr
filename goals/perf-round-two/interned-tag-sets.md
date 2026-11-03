# Goal - interned per-request tag sets

Every request builds a fresh `BTreeMap` clone of the VU's base tags plus
name/method/status/proto extras and wraps it in a new `Arc` — one allocation
and tree build per request, at every request rate. Tag-set cardinality per VU
is tiny (a handful of request names x status codes), so a per-VU intern cache
removes the allocation entirely.

Paste this whole block into a fresh coding-agent session:

```text
/goal Intern per-request metric tag sets so the hot path stops allocating a BTreeMap per request

CONTEXT
- Base branch: risotto.
- `VuContext::sample_tags` (crates/loadr-core/src/vu.rs:117): fast path
  returns `self.base_tags.clone()` when no extras/group; slow path — taken by
  EVERY HTTP/gRPC request because request metrics pass
  (name, method, status, proto) extras — clones `base_tags` (:64) into a new
  `BTreeMap`, inserts extras, wraps in a fresh `Arc<Tags>`.
- Call sites feeding it per request: `RequestMetricEmitter::from_vu` /
  `emit_request_metrics` (crates/loadr-core/src/flow.rs:1846/:1872) which also
  does `response.status.to_string()` per request; the JS host bridge shares
  the same path. ~11 samples per request already share the one Arc — the
  remaining cost is the one build per request.
- `Tags = BTreeMap<String, String>` (crates/loadr-core/src/metrics.rs:11);
  Aggregator series keys are `{metric: Arc<str>, tags: Arc<Tags>}` and use
  Eq/Hash on the map contents, so interning also makes series-key hashing
  cheaper via pointer-stable Arcs (do NOT rely on pointer equality for
  correctness anywhere).

IMPLEMENTATION
- Add a small per-VU intern map (plain HashMap in VuContext or the emitter,
  no locks — VuContext is single-task): key derived from group + extras
  pairs, value `Arc<Tags>`. On hit return the cached Arc; on miss build once
  and insert. Key construction must not allocate on the hit path — e.g. hash
  the (&str) pairs with the default hasher into a u64 key and verify equality
  against the stored pairs to guard collisions, or use a
  `Vec<(String,String)>`-keyed map only on miss. Keep it simple; measure
  nothing fancier than needed.
- Status string: avoid `to_string()` per request — a static table for
  common codes (gRPC 0..=16, HTTP 100..=599) or itoa into a reusable buffer
  feeding the intern key.
- Unbounded per-VU cache is acceptable: cardinality is bounded by plan
  request names x observed statuses x methods. State the bound in a comment.
- `error_kind` tag (transport-error classification) joins the key when
  present — rare path, fine to always miss or key on it too.

OUT OF SCOPE
- Global (cross-VU) interning, locks, or changing `Tags`' type.
- Aggregator/output changes; sample struct changes.

TESTS
- Parity unit test in loadr-core: emitting the same request twice returns
  Arc-identical tags (ptr_eq) and map contents equal to the un-interned
  construction; different status/name produce different sets.
- Collision-guard test if a hashed key is used (two crafted keys, same
  bucket, distinct maps).
- Existing metric tests (flow.rs mod tests, engine tests, e2e
  standalone_run_produces_metrics_and_passes) stay green — they pin
  tag-visible behavior.

QUALITY BAR
Focused regression tests as above; no unrelated refactors; conventional
commit, no Claude-Session trailer. Run cargo fmt --all and cargo clippy
--workspace --all-targets -- -D warnings, then cargo test -p loadr-core
--locked (workspace suite before the PR: --exclude loadr-browser locally).

DONE when: a repeated identical request performs zero Tags allocations after
the first iteration (assert via the ptr_eq parity test) and all existing
metric tests pass unchanged.
```
