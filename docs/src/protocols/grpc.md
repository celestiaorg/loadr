# gRPC

loadr calls gRPC services **dynamically** — no code generation, no `protoc`
binary. Describe the service either with `.proto` files (compiled in-process
by protox) or via **server reflection**.

```yaml
- request:
    name: say hello
    url: grpc://greeter.example.com:50051       # grpcs:// for TLS
    grpc:
      proto_files: [ protos/helloworld.proto ]  # relative to the test file
      proto_includes: [ protos/ ]               # import search paths
      service: helloworld.Greeter
      method: SayHello
      message: { name: "vu-${vu}" }             # request message as JSON
      metadata: { x-api-key: "${secrets.key}" }
    assert:
      - { type: status, equals: 0 }             # gRPC code: 0 = OK
      - { type: jsonpath, expression: "$.message", exists: true }
```

With reflection instead of files:

```yaml
grpc:
  reflection: true
  service: helloworld.Greeter
  method: SayHello
  message: { name: "world" }
```

## Protobuf field checks

Use `protobuf_field` when an application-level result is carried in the
response message even though the gRPC transport status is OK:

```yaml
checks:
  - type: status
    name: grpc_transport_ok
    equals: 0
  - type: protobuf_field
    name: admission_accepted
    field: code
    equals: 0
    failure_groups:
      18: WrongShard
      20: PoolAtCapacity
      21: MempoolByteLimitExceeded
```

`field` is the exact top-level protobuf field name, not its ProtoJSON name.
Singular scalar and enum fields are supported; nested paths, messages, maps,
and repeated fields are rejected. `equals` is checked using the field's
descriptor type. Enum expectations may be their numeric value or declared
name; byte expectations use base64.

Presence follows protobuf rather than JSON semantics:

- An omitted proto3 implicit-presence scalar is present with its type's
  default value. Thus an omitted `uint32 code` satisfies `equals: 0` and
  `exists: true`.
- An omitted `optional uint32 owner_hint` is absent and satisfies
  `exists: false`. An explicitly encoded optional zero is present.

The response is decoded to a `DynamicMessage` for these checks, but it is not
converted to JSON unless an extractor, JSON/body check, or `afterRequest` hook
also needs the JSON body. Existing JSONPath behavior and its default-omitting
ProtoJSON representation are unchanged.

`failure_groups` is optional and valid only under `checks`. It bounds metric
cardinality: mapped failures receive `failure_code` and `failure_group` tags;
unmapped numeric values collapse to `failure_group=other` without a raw code.
Missing optional fields use `missing`, and transport/parse/empty-stream cases
use `no_response`. The final console, JSON, HTML, and JUnit summaries include
the grouped counts. Without `failure_groups`, no extra metric series or
summary fields are added.

## Streaming

All four shapes are supported. Streaming requests provide `messages`
(a list) instead of `message`:

```yaml
grpc:
  reflection: true
  service: helloworld.Greeter
  method: LotsOfReplies          # server streaming: responses collected
  message: { name: "stream" }
---
grpc:
  service: pkg.Ingest
  method: Push                   # client streaming
  messages: [ { v: 1 }, { v: 2 }, { v: 3 } ]
```

`stream_repeat: N` sends the `messages` list N times, one frame per copy —
useful when frames should differ only by generated data:

```yaml
grpc:
  service: mempool.Submitter
  method: SubmitStream
  stream_repeat: 32
  messages: [ { tx: "${data.signed.tx_b64}" } ]   # 32 frames, 32 distinct rows
```

Every frame re-fetches plugin-backed (`type: plugin`) data sources, so each
copy carries a fresh row; fields *within* one frame always come from the same
row. CSV, JSON, and inline sources keep their per-iteration value across all
frames. Building more frames than a non-client-streaming method accepts is an
error rather than a silent truncation.

The response body is the (last) response message rendered as JSON, so
`jsonpath` extraction/assertions work naturally. `extras.message_count` holds
the number of streamed responses.

`protobuf_field` also evaluates the last response message. An empty successful
stream has no response and therefore fails the check (`no_response` when
grouping is configured).

**Automatic skip**: when nothing needs the response body — no `extract`, no
`assert`/`checks` beyond `status`/`duration`/`header` (jsonpath, body/size
checks, `js`, ... all count as reading it), and no script `afterRequest`
hook — loadr skips building it entirely: no `DynamicMessage`, no JSON
conversion. This is automatic, with no config knob; the common status-only
load-generation case gets the full speedup for free. `body` is empty and
`message_count` still reflects every frame received; stream draining, status
and timings are unaffected.

With protobuf-only checks, loadr builds the `DynamicMessage` required for
descriptor-aware evaluation but still skips JSON serialization. Resolved
field descriptors and typed expected values are cached with the existing
per-VU call state.

## Semantics & metrics

- `status` is the gRPC status code (0 = OK); non-zero marks the request
  failed. `status_text` carries the code name and message.
- Metrics: `grpc_reqs`, `grpc_req_duration`, plus `data_sent`/`data_received`.
- By default, channels are pooled **per VU** per endpoint; proto descriptor
  pools are compiled once and cached process-wide.
- One connection per VU stops scaling past a couple thousand VUs against a
  single endpoint. Set `channel_pool_size` to instead share a fixed pool of
  HTTP/2 channels across **all** VUs (round-robined; each multiplexes many
  concurrent streams):

  ```yaml
  grpc:
    channel_pool_size: 8   # 8 shared connections instead of one per VU
  ```
- With `transport: channel`, each pooled channel queues outbound calls in a
  bounded `tower::buffer`; its depth defaults to 4096 and is configurable via
  the `LOADR_GRPC_BUFFER_SIZE` environment variable. It only applies to
  `channel_pool_size` channels, not the default per-VU ones — raise it if
  many VUs share a pooled channel and calls start stalling in `ready()`
  before the connection itself is the bottleneck.
- `grpcs://` uses the standard TLS config (custom CAs, mTLS).

### Shared-pool sizing

Size a shared pool from the number of RPCs that can be in flight at once
(Little's Law), independently for each agent and endpoint:

```text
in_flight = target_RPC/s_per_agent × p99_latency_seconds
effective_streams_per_connection = min(client_limit, server_MAX_CONCURRENT_STREAMS)
pool_size ≥ ceil(in_flight × headroom / effective_streams_per_connection)
```

Use a headroom factor of 1.25–1.5 for latency variation and bursts. With the
raw transport, the client limit defaults to 512; use a lower value if the
server advertises a lower HTTP/2 `MAX_CONCURRENT_STREAMS`. The channel
transport has no equivalent loadr semaphore, so size against the server limit
and confirm the result with a pool-size sweep.

The target is **gRPC calls per second**, not executor iterations or business
transactions per second. If one transaction makes multiple RPCs, include all
of them in the target RPC rate. In distributed runs, apply the calculation to
the rate handled by one agent, because every agent owns its own pool.

For example, one agent targeting 500,000 RPC/s at 200 ms p99 needs 100,000
concurrent streams. With raw's 512-stream default, the absolute minimum is
`ceil(100,000 / 512) = 196` connections. At 1.5× headroom it is
`ceil(100,000 × 1.5 / 512) = 293`; round up to a practical pool size such as
320:

```yaml
grpc:
  transport: raw
  channel_pool_size: 320
```

By comparison, a 64-connection raw pool permits at most 32,768 concurrent
streams. At 500,000 RPC/s it reaches that stream-cap ceiling at about 65.5 ms;
at 200 ms the corresponding ceiling is about 163,840 RPC/s. These are
transport-capacity upper bounds: CPU, network, server capacity, flow control,
and response size may produce a lower ceiling.

## Transport

`transport` selects the client stack driving the calls (default: `channel`):

```yaml
grpc:
  transport: raw           # channel (default) | raw
  channel_pool_size: 8     # works with either transport
```

- `channel` — tonic's `Channel`: every request goes through a tower::buffer
  queue owned by a per-channel worker task.
- `raw` — drives hyper HTTP/2 directly from the VU task: no intermediate
  queue, no per-channel worker, fewer wakeups per request. An experimental
  performance path for high-rate runs; measure with an A/B before relying
  on it.

Raw specifics:

- In-flight streams per connection are capped (default 512,
  `LOADR_GRPC_MAX_STREAMS_PER_CONN`) so a slow server cannot grow pending
  streams without bound.
- After a failed dial, calls fail fast with `Unavailable` for 500 ms before
  the next dial attempt. Connection failures surface exactly like the
  channel transport's: status 14, `connection failed: ...`.
- `grpcs://` with `transport: raw` honors `insecure_skip_verify` and TLS
  `min_version`/`max_version` (the channel transport warns and ignores
  them).
- The raw transport sends no `user-agent` header (tonic sends
  `tonic/<version>`).

`LOADR_GRPC_TRANSPORT=raw|channel` overrides `transport` for every request
in the process — handy for whole-fleet A/B runs without editing plans.
