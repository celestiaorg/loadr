# loadr

loadr is a focused distributed gRPC load generator written in Rust. It keeps
the seven executor models, dynamic protobuf/reflection support, all four gRPC
call shapes, exact distributed histogram merging, Prometheus export, a live web
UI, and native on-demand feeder plugins.

```yaml
name: grpc-smoke
scenarios:
  unary:
    executor: constant-arrival-rate
    rate: 200
    duration: 2m
    pre_allocated_vus: 30
    max_vus: 100
    flow:
      - request:
          name: say-hello
          url: grpc://greeter.example.com:50051
          grpc:
            proto_files: [protos/helloworld.proto]
            service: helloworld.Greeter
            method: SayHello
            message: { name: "vu-${vu}" }
          checks: [{ type: status, equals: 0 }]

thresholds:
  grpc_req_duration: ["p(95)<150"]
outputs:
  - { type: prometheus, listen: 127.0.0.1:9091 }
```

Run locally:

```bash
cargo run -p loadr-cli -- run examples/10-grpc.yaml
```

Run as a fleet:

```bash
loadr controller --bind 0.0.0.0:7625
loadr agent --join controller-host:7625 --name agent-1
loadr run --controller controller-host:6464 examples/15-distributed.yaml
```

The controller partitions VUs and arrival rates, starts agents on a common
barrier, merges HDR histograms centrally, and exposes the web UI and a
Prometheus view. Agents resolve feeder libraries locally with `--plugins-dir`.

## Workspace

```text
crates/loadr-config       YAML model, schema, validation
crates/loadr-core         executors, VUs, flows, metrics, thresholds, feeders
crates/loadr-grpc         dynamic gRPC client, reflection, raw/tonic transports
crates/loadr-agent        controller/agent coordination and metric merging
crates/loadr-feeder-api   stable feeder-only native ABI and local registry
crates/loadr-prometheus   scrape endpoint and remote-write output
crates/loadr-webui        embedded management UI
crates/loadr-cli          loadr binary
testsupport/loadr-testserver  four-shape gRPC echo server
plugins/examples/signed-tx-feeder  reference ABI-v2 feeder
```

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

See [ARCHITECTURE.md](ARCHITECTURE.md) and the focused [docs](docs/src/SUMMARY.md).

## License

[Elastic License 2.0](LICENSE).
