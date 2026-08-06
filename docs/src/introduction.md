# loadr

loadr is a distributed gRPC load generator. Test plans are declarative YAML,
the runtime supports all seven executor models, and gRPC requests are built at
runtime from protobuf files or server reflection. Unary, client-streaming,
server-streaming, and bidirectional-streaming methods share the same request
model.

Data can come from CSV, JSON, inline rows, or native feeder plugins invoked on
demand. Results are aggregated with HDR histograms, merged exactly by the
controller, exposed through Prometheus, and available in the embedded web UI.
