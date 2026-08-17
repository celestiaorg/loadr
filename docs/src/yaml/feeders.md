# Feeder plugins and throttling

Memory-backed data sources support CSV, JSON arrays, and inline rows. Native
feeders cover values that must be generated immediately before a request, such
as signed or timestamped transactions.

```yaml
plugins:
  - name: tx-signer
    path: ../target/release/libsigned_tx_feeder.so
    config: { seed: 7 }

data:
  signed:
    type: plugin
    source: tx-signer
    config: { chain_id: testnet }
```

Use generated fields like ordinary data:

```yaml
message: { transaction: "${data.signed.tx_b64}" }
```

Each call receives run, agent instance, partition, VU, iteration, per-source
sequence, scenario, request, and timestamp identity. `next_row` may be called
concurrently and should avoid global locks on its hot path.

`blocking: true` on the data source fetches its rows under
`tokio::task::block_in_place`, so a CPU-heavy (signing, hashing) or I/O-backed
(database, vault) feeder cannot stall the runtime's worker threads — without
it, enough concurrently-preparing VUs on a slow feeder delay timers and
unrelated VUs, degrading the load shape itself. It is off by default: the
bracket has a fixed per-call cost a microsecond feeder should not pay, and it
applies only inside a multi-thread runtime (the bracket panics on a
current-thread one, so that case falls back to the plain inline call).

A row is fetched once per request, and once per frame of a streaming request —
so a `messages` list of length L with `stream_repeat: N` pulls `N × L` rows and
every frame carries a distinct payload. Derive uniqueness from `seq`: `ts_ms`
is millisecond-granular and frames of one request share it. Those `N × L` rows
are consumed all-or-nothing; if the source reports `exhausted` partway, the
whole request is abandoned and the already-generated rows are discarded, so
size any generator limit as a multiple of `N × L`.

Build the reference feeder with:

```bash
cargo build -p loadr-example-signed-tx-feeder --release
```

Agents load feeders locally. Point each agent at the same installation root
with `loadr agent --plugins-dir /opt/loadr/feeders ...`.

`throttle: { requests_per_second: 200 }` remains available per scenario and
caps aggregate request starts independently of the executor model.
