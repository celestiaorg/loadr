# Architecture

loadr has one workload path: YAML plans compile into executor schedules and
gRPC requests. Virtual users render request templates, fetch memory/file rows
or invoke a native feeder, and execute a dynamic gRPC call. Samples flow into
sharded HDR-backed aggregation, thresholds, Prometheus, the web UI, or a
controller delta stream.

```text
YAML -> config/validation -> scenario programs -> seven executors
                                      |                |
                      CSV/JSON/inline/feeder       virtual users
                                      |                |
                                      +----> dynamic gRPC
                                                |
                                      metrics + HDR histograms
                                                |
                         local Prometheus/UI or controller merge
```

## Crate boundaries

- `loadr-config` owns the serializable test-plan model and validation.
- `loadr-core` owns scheduling, VU state, data sources, metric aggregation,
  thresholds, and the protocol-handler seam.
- `loadr-grpc` owns protobuf compilation/reflection and unary, client-stream,
  server-stream, and bidi execution. It supports tonic channels and a raw
  hyper HTTP/2 transport.
- `loadr-agent` owns the versioned bidirectional coordination stream,
  synchronized starts, partitioning, reconnect/replay, and controller merge.
- `loadr-feeder-api` is a deliberately small ABI-v2 boundary. A feeder is a
  local native library with `init` and concurrent `next_row` calls.
- `loadr-prometheus` owns scrape and remote-write projections.
- `loadr-webui` and `loadr-cli` are presentation and process wiring.

## Distributed identity

Every feeder call receives `run_id`, stable `instance_id`, partition index and
count, VU, iteration, per-source sequence, scenario, request, and timestamp.
This lets generators create globally unique payloads without a lock or a
controller round trip on the request hot path.

Agents send mergeable metric deltas through a replayable, acknowledged uplink.
The controller attaches trusted agent identity, merges histograms rather than
averaging percentiles, sums additive gauges, and fences late/stale sessions.

## Compatibility

Feeder ABI version 2 is intentionally incompatible with the former general
plugin ABI. Plugins are installed from local directories only; there is no
remote index or runtime protocol/output plugin surface.
