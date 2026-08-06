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

Build the reference feeder with:

```bash
cargo build -p loadr-example-signed-tx-feeder --release
```

Agents load feeders locally. Point each agent at the same installation root
with `loadr agent --plugins-dir /opt/loadr/feeders ...`.

`throttle: { requests_per_second: 200 }` remains available per scenario and
caps aggregate request starts independently of the executor model.
