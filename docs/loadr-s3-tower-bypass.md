# Proposal: bypass tonic `Channel`'s internal queue — drive hyper h2 directly

**Status:** implemented 2026-07-13 as `transport: raw` on branch
`nikolai/grpc-raw-transport` (stacked on `nikolai/grpc-vu-shared-pool`, i.e.
directly on main — not on `risotto-grpc-perf-changes` as originally planned;
the call-cache commits are orthogonal and stay on risotto). Default remains
`channel` pending the AWS A/B. Notable deltas from this proposal: reconnect
uses a singleflight dial gate with a fixed 500 ms fail-fast cooldown (no
jittered backoff), reflection also runs on the raw transport, and raw gains
working `insecure_skip_verify`. **Effort:** days, not hours.

## Why (measured evidence)

The vanilla-gRPC ceiling is per loadr-agent *process* and is wake-chain bound,
not CPU/network bound. AWS rig, 2× c8g.16xlarge agents → nginx gRPC mock
(all rates server-side verified):

| Config | Ceiling | Median latency |
|---|---|---|
| old binary, 64 tokio workers | 290k | 11.7ms |
| old binary, 16 workers (taskset) | 388k | 3.9ms |
| + dispatcher/cache fixes (`4de59bb`,`8a9dd47`), 16 workers | 494k (247k/agent) | 2.56ms |
| + 1ms dispatch tick | 494k | **1.28ms** |
| h2load on the same hosts (reference) | ~2.2M/host | sub-ms |

Wire rtt on the pooled connections is 0.4–2.4ms while loadr measures 1.3–12ms —
the remaining gap is internal queueing/wakeups. Everything cacheable per-request
is now cached; **the biggest remaining structural cost is the transport hop.**

## What tonic `Channel` costs per request

`tonic::transport::Channel` is `tower::buffer::Buffer`: a bounded mpsc queue
**plus a dedicated worker task** owning the real hyper/h2 client. With a
64-channel pool that's 64 queues + 64 worker tasks. Each request:

1. VU task `ready()` → reserve a queue slot (semaphore + waker registration)
2. VU task `call()` → push request + response-oneshot into the mpsc → **wake #1: buffer worker**
3. Buffer worker drives hyper → h2 enqueues frames → **wake #2: h2 conn task**
4. Response arrives → **wake #3 via the oneshot → wake #4: VU resumes**

Plus: every request crosses a `Box<dyn Service>` (dynamic dispatch), and all
~47 VUs sharing a channel serialize through that channel's single worker task.

## The proposal

Drive `hyper::client::conn::http2::SendRequest` directly from the VU task —
**loadr's own HTTP handler already does exactly this** (`crates/loadr-protocols/src/http.rs:364-372`:
raw handshake + spawned conn driver, no tower). `tonic::client::Grpc<T>` only
requires `T: GrpcService<Body>` (blanket impl over `tower::Service`), so a small
`RawChannel: Clone + tower::Service<http::Request<Body>>` wrapping a shared,
reconnectable `SendRequest` slots in via `Grpc::with_origin`. The
`DynamicCodec`, the per-VU caches, and all four call shapes carry over untouched.

Per request this leaves ~2 wakeups (conn task on send, VU on response), zero
intermediate queues, 64 fewer tasks, no dyn dispatch.

Sketch (full detail in the original plan): new
`crates/loadr-protocols/src/grpc_transport.rs`; `RawChannelPool` mirroring the
existing `ChannelPool`; h2 builder parity with today's pooled endpoints (4MiB
stream / 8MiB conn windows, 30s keepalive); config `transport: channel | raw`
on `GrpcOptions` + `LOADR_GRPC_TRANSPORT` env for fleet A/B; **default stays
`channel`** until parity is proven.

## Pros

- Removes the last 2–3 per-request task hops — attacks the measured binding
  constraint directly. Realistic target: 300k+/process (headroom exists:
  h2load proves the raw-h2 floor on these hosts is ~2.2M/host).
- Deletes 64 buffer-worker tasks and their queues; less scheduler load helps
  everything else too.
- Explicit, tunable backpressure (in-flight semaphore per connection) instead of
  an opaque 1024-slot buffer.
- Reuses in-tree dial/TLS code from the HTTP handler; gains a working
  `insecure_skip_verify` for grpcs (the tonic path warns-and-ignores it today).

## Cons / risks

- **tower::buffer silently provided two things that must be hand-rolled:**
  1. *Backpressure*: without the buffer, a slow server means unbounded h2
     pending-stream growth → explicit per-conn in-flight gate (Semaphore,
     default ~512, `LOADR_GRPC_MAX_STREAMS_PER_CONN`), released on
     response-body completion.
  2. *Reconnect*: `connect_lazy` re-dial is Channel's job today → singleflight
     reconnect with jittered backoff; failures must map to
     `Status::unavailable("connection failed: …")` so `error_kind`
     classification and dashboards stay stable.
- `Grpc` clients are `&mut`-per-call: raw clients must stay per-VU (the per-VU
  call cache already enforces this pattern); sharing one instance across tasks
  is a correctness bug.
- Streaming shapes, TLS/ALPN (`h2`), and reconnect need dedicated tests
  (parametrize the existing 4-shape integration tests over both transports;
  kill/respawn echo-server test; `spawn_tls` variant; tiny
  `max_concurrent_streams` server test).
- Days of work + an A/B measurement cycle before flipping any default; two
  transports to maintain until `channel` can be retired.

## Verification plan

AWS rig A/B at identical rate ladders (`transport: raw` vs `channel`),
server-side judged; expect the client-internal latency gap (loadr-measured vs
`ss -ti` rtt) to collapse and per-request CPU to drop. Confirm connection count
at the mock equals pool size, zero failures, and parity on all four call shapes
before considering a default flip.

## Related: why do N processes beat one big tokio runtime today?

(Q from the 2026-07-11 session; same root cause S3 attacks.)

1. **Single-task choke points cap at one core each** no matter how many workers
   exist: one arrival dispatcher must wake once per arrival; one metrics
   aggregator consumes ~5 samples/request (≈1M+ channel sends/s at 200k req/s).
   N processes = N dispatchers + N aggregators.
2. **Cross-task wakes are expensive on a wide, mostly-idle runtime**: waking a
   parked worker is a futex syscall, and the woken task usually lands on a
   different core with cold caches. Evidence: merely shrinking 64→16 workers
   gave +27–34%; 3 colocated processes gave the best per-process rate of all.
3. **Shared runtime state** — one timer wheel (a timeout armed per request),
   one I/O driver, one global injector queue — is touched by all workers,
   bouncing cachelines across the Graviton's core clusters.

Processes are the workaround; S3 (fewer hops) + the metrics sharding stage
(per-worker aggregators, delta-merged — same pattern the agent uplink already
uses) are what let a *single* process behave like the share-nothing h2load
model. Until then: `LOADR_DISPATCH_TICK_US=1000 loadr agent --join <ctrl>
--worker-threads 16`, N processes per host.
