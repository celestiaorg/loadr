# Test-plan overview

```yaml
name: my-test
description: optional text

env: {}
variables: {}
secrets: {}
data: {}
metrics: {}
plugins: []

scenarios: {}   # required workloads
thresholds: {}
outputs: []     # Prometheus scrape/remote-write
```

Unknown keys are rejected. Durations accept values such as `300ms`, `30s`,
`1m30s`, and `1h`. Requests use `grpc://` or `grpcs://` endpoints and describe
their protobuf service/method under `grpc:`.

A minimal complete test is available at `examples/10-grpc.yaml`. Generate the
full machine-readable model with `loadr schema`.
