# No-op

The built-in `noop` protocol accepts a prepared request and immediately reports
success without opening a socket or calling a protocol plugin. It is intended
for self-testing loadr's request hot path and benchmarking on-demand data-source
plugins without a backend becoming the bottleneck.

```yaml
- request:
    name: generate payload
    protocol: noop
    url: noop://local
    method: POST
    body: "${data.signed_tx.tx_b64}"
```

`noop://` URLs infer the protocol automatically, so `protocol: noop` is
optional. The response has status `200` (`OK`), an empty body, no headers, no
error, and zero protocol duration. The rendered request body length is counted
in `data_sent`; no bytes are counted in `data_received`.

Metrics are `noop_reqs`, `noop_req_duration`, and the shared
`http_req_failed` rate. Use the `noop_reqs` per-second value as the throughput
result. `noop_req_duration` is always zero because request preparation, including
data-source generation, happens before the protocol handler runs.

For a complete native feeder benchmark, see
`examples/51-plugin-data-source-throughput.yaml`. It intentionally has no
per-request checks or external target, so its hot path consists of feeder row
generation, request interpolation, normal metric emission, and the no-op
response.
