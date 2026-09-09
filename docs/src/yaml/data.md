# Data parameterization

Feed iterations from CSV files or inline rows. A row is consumed **once per
iteration per source** (the first reference fetches it; later references in
the same iteration see the same row).

```yaml
data:
  users:
    type: csv
    path: data/users.csv     # relative to the test file
    mode: shared             # shared | per_vu
    on_eof: recycle          # recycle | stop
    delimiter: ","           # default ,
    has_header: true         # default true; otherwise columns are col0, col1, ...
  fixtures:
    type: inline
    rows:
      - { sku: W-1, qty: 1 }
      - { sku: W-2, qty: 3 }

scenarios:
  buy:
    executor: per-vu-iterations
    vus: 5
    iterations: 100
    flow:
      - request:
          method: POST
          url: /cart
          body: { form: { user: "${data.users.username}", sku: "${data.fixtures.sku}" } }
```

## Modes

- **`shared`** — one cursor for the whole run; VUs pull the next row
  atomically. Rows are spread across VUs (each row used once per lap).
- **`per_vu`** — every VU iterates the full data set from the top
  independently.

## End of data

- **`recycle`** — wrap to the first row (default).
- **`stop`** — the VU that hits EOF stops iterating (JMeter's
  "stop thread on EOF"). With shared mode this winds the test down as the
  data runs out — handy for "process each row exactly once" jobs.

From JS, fetch the current row with `session.data('users')` →
`{username: "...", password: "..."}`.

## Plugin-backed (on-demand) sources

`type: plugin` generates a row per **request**, on demand, from a native
plugin that provides the `data_source` capability — instead of loading rows
from a file up front. Use it when a value can't be pre-generated, e.g. a
time-sensitive, cryptographically signed payload that must land inside a
protobuf `bytes` field:

```yaml
plugins:
  - name: tx-signer
    path: ./target/release/libtx_signer.so
    config: { seed: 42 }

data:
  signed_tx:
    type: plugin
    source: tx-signer      # a plugins: entry providing data_source
    config:
      chain_id: testnet-1

scenarios:
  submit:
    executor: constant-vus
    vus: 100
    duration: 5m
    flow:
      - request:
          name: submit tx
          protocol: grpc
          url: grpc://node:50051
          grpc:
            proto_files: [submit.proto]
            service: mempool.Submitter
            method: Submit
            message:
              tx: "${data.signed_tx.tx_b64}"   # bytes field <- base64 string
          checks:
            - { type: status, equals: 0 }
```

The config surface is exactly `{ type: plugin, source: <plugin>, config:
<object>, blocking: <bool>, on_result: <bool> }`. **`mode`, `on_eof` and `pick` do not apply**
and are ignored if present — those describe iterating over a stored set of
rows, which doesn't exist here; a plugin generates every row fresh, per call.

**`blocking: true`** fetches this source's rows under
`tokio::task::block_in_place`, so a CPU-heavy (signing, hashing) or
I/O-backed (database, vault) feeder cannot stall the runtime's worker
threads — without it, enough concurrently-preparing VUs on a slow feeder
delay timers and unrelated VUs, degrading the load shape itself. Leave it
off (the default) for cheap in-memory generation: the bracket has a fixed
per-call cost that a microsecond feeder should not pay.

**`on_result: true`** hands the full result of every request that used a row
from this source back to the plugin, so a feeder can react to what the server
said. The plugin must provide the `result_sink` capability; if it doesn't, the
flag is ignored with a warning. Off by default — the response is serialised
per request, which a plugin that ignores it shouldn't pay for.

The plugin receives one JSON payload per row the request used:

```json
{"source": "nonces", "vu": 7, "iteration": 3, "seq": 41,
 "scenario": "submit", "request": "submit tx",
 "row": {"account": "acct-12", "nonce": "17"},
 "response": {"status": 200, "status_text": "OK", "body": "...",
              "headers": {...}, "duration_ms": 12.4, "error": null,
              "url": "...", "protocol": "HTTP/1.1"}}
```

The `row` is echoed back, so a plugin usually needs no pending-request
bookkeeping of its own. `seq` identifies the row uniquely with `vu` and
`source`. A streaming request that pulled N frames' worth of rows produces N
payloads.

Feedback must not be able to break a load test, so `on_result` returns nothing
and a request cancelled mid-flight (a graceful stop) reports nothing at all — a
plugin has to tolerate a row whose result never arrives.

Two costs worth knowing. The response body is serialised into the payload, so
a source that reports results on requests with large responses pays that per
request; and a gRPC streaming request that pulled N frames' worth of rows sends
N payloads, each carrying the same response.

`blocking: true` covers `on_result` as well as `next_row`. Leave it off for a
sink that just updates memory; turn it on if the sink does I/O, so a slow call
can't stall the other VUs sharing its runtime thread. Either way the call runs
inside the VU's own iteration, so its latency counts against that VU.

See `plugins/examples/native-nonce-feeder` for a worked example: per-account
blockchain nonces held in sharded maps, advanced only when the submission
succeeded.

**Freshness is per-request, not per-iteration.** CSV/JSON/inline sources
cache one row per iteration (all references in the same iteration see the
same row). Plugin-backed sources instead cache one row per **request
preparation**: every `${data.<name>.*}` field rendered while preparing a
single request sees the same generated row, but the next request in the same
iteration — or a retried request — gets a fresh one. This matters for a flow
that sends two signed submissions per iteration: they must not reuse the
same signature.

Within a gRPC streaming request, freshness is **per frame**: each entry of
`messages` (and each `stream_repeat` copy of it) pulls its own row, so a list
of length L with `stream_repeat: N` consumes `N × L` rows and every frame
carries a distinct payload. Fields inside one frame still share a row, so a
signature and its nonce always agree. Derive uniqueness from `seq` — `ts_ms`
is millisecond-granular and frames of one request share it. Those `N × L` rows
are consumed all-or-nothing: if the source reports exhaustion partway, the
request is abandoned and the rows already generated are discarded, so size any
generator limit as a multiple of `N × L`.

**Exhaustion retires the VU**, the same as `on_eof: stop` for a finite CSV.
**Plugin errors count as failed requests** (tagged `error:prepare` on
`http_req_failed`) and the run continues — a transient signing failure does
not abort the whole test.

**Distributed runs:** the plugin must be installed locally on every agent;
native plugin binaries are never shipped from the controller to agents. An
assignment referencing a plugin without the `data_source` capability (or not
loaded at all) fails cleanly before the synchronized start barrier.

See [Native data-source plugins](../plugins/developing.md#native-data-source-plugins)
for how to write one.

To measure the generator's maximum throughput without involving gRPC or another
backend, render a plugin value into a request handled by the built-in
[`noop` protocol](../protocols/noop.md). The `noop_reqs` per-second result then
covers feeder generation, interpolation, and the normal engine metric path.
