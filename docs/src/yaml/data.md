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
<object>, blocking: <bool> }`. **`mode`, `on_eof` and `pick` do not apply**
and are ignored if present — those describe iterating over a stored set of
rows, which doesn't exist here; a plugin generates every row fresh, per call.

**`blocking: true`** fetches this source's rows under
`tokio::task::block_in_place`, so a CPU-heavy (signing, hashing) or
I/O-backed (database, vault) feeder cannot stall the runtime's worker
threads — without it, enough concurrently-preparing VUs on a slow feeder
delay timers and unrelated VUs, degrading the load shape itself. Leave it
off (the default) for cheap in-memory generation: the bracket has a fixed
per-call cost that a microsecond feeder should not pay.

**Freshness is per-request, not per-iteration.** CSV/JSON/inline sources
cache one row per iteration (all references in the same iteration see the
same row). Plugin-backed sources instead cache one row per **request
preparation**: every `${data.<name>.*}` field rendered while preparing a
single request sees the same generated row, but the next request in the same
iteration — or a retried request — gets a fresh one. This matters for a flow
that sends two signed submissions per iteration: they must not reuse the
same signature.

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
