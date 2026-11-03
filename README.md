<div align="center">

# loadr

**A modern load testing platform in a single Rust binary — everything k6 does, everything JMeter does, with a cleaner developer experience.**

[![CI](https://github.com/levantar-ai/loadr/actions/workflows/ci.yml/badge.svg)](https://github.com/levantar-ai/loadr/actions/workflows/ci.yml)
[![Release](https://github.com/levantar-ai/loadr/actions/workflows/release.yml/badge.svg)](https://github.com/levantar-ai/loadr/releases/latest)
[![Docs](https://img.shields.io/badge/docs-loadr.io-dc2626)](https://loadr.io/docs/)
[![License](https://img.shields.io/badge/license-Elastic--2.0-07070a)](LICENSE)

*Declarative YAML tests · embedded JavaScript · HTTP, gRPC, Redis, SQL & more · WASM & native plugins · distributed agents · live web UI*

</div>

---

```yaml
# test.yaml — readable by humans, validated by machines
name: checkout-under-load
defaults:
  http: { base_url: https://shop.example.com }

scenarios:
  shoppers:
    executor: ramping-vus
    stages: [ { duration: 2m, target: 100 }, { duration: 5m, target: 100 } ]
    flow:
      - request:
          url: /
          extract: [ { type: css, name: csrf, expression: "input[name=csrf]", attribute: value } ]
      - think_time: { type: uniform, min: 1s, max: 3s }
      - request:
          method: POST
          url: /cart
          body: { form: { sku: W-1, csrf: "${csrf}" } }
          checks: [ { type: status, equals: 201 } ]

thresholds:
  http_req_duration: [ "p(95)<400" ]
  http_req_failed: [ { threshold: "rate<0.01", abort_on_fail: true } ]
```

```console
$ loadr run test.yaml

  checkout-under-load — 1 scenario(s), 420.0s

  checks.....................: 99.82% — ✓ 41873 ✗ 75
  http_req_duration..........: avg=87.21ms min=12.04ms med=74.55ms max=1.20s p(90)=142ms p(95)=189ms p(99)=311ms
  http_reqs..................: 83896 (199/s)

  thresholds:
    ✓ http_req_duration: p(95)<400 (observed: 189.21)
    ✓ http_req_failed: rate<0.01 (observed: 0.00)
```

## Why loadr?

| | **k6** | **JMeter** | **loadr** |
|---|---|---|---|
| Test format | JavaScript | XML (GUI) | **YAML + JS, JSON-Schema-validated** |
| Open & closed load models | ✅ | partial | ✅ all 7 k6 executors |
| Protocols built in | HTTP, WS, gRPC | many | **HTTP/1.1+2, WS, SSE, gRPC (+reflection), GraphQL, TCP, UDP, no-op self-test** — plus Redis, SQL (PostgreSQL/MySQL) and MongoDB as installable native plugins |
| JMeter-style assertions/extractors/timers | ❌ | ✅ | ✅ JSONPath, XPath, CSS, regex, boundary; constant/uniform/gaussian/throughput timers |
| Plugins | Go/xk6 rebuild | jars | **WASM (sandboxed) + native, no rebuild** |
| Distributed | paid cloud | manual | **built-in controller/agents, correct HDR percentile merging** |
| Management UI | paid cloud | ❌ | **built-in web UI (RabbitMQ-style)** |
| Migration | — | — | `loadr convert` imports `.jmx` and k6 scripts |

## Install

```bash
# From a release binary (Linux/macOS/Windows builds at the releases page)
curl -sSL https://github.com/levantar-ai/loadr/releases/latest/download/loadr-x86_64-unknown-linux-gnu.tar.gz | tar xz

# From source
cargo install --git https://github.com/levantar-ai/loadr loadr-cli
```

Every release ships SHA256 checksums and SLSA build provenance — verify a
download with `gh attestation verify <archive> --repo levantar-ai/loadr`.

## Quickstart

```bash
loadr validate examples/01-quickstart.yaml     # friendly errors with line numbers
loadr run examples/01-quickstart.yaml          # run it
loadr run -e staging examples/13-environments.yaml   # environment overrides
loadr report results.json -o report.html       # standalone HTML report
loadr convert legacy-plan.jmx -o converted.yaml      # escape JMeter
loadr convert k6-script.js -o converted.yaml         # or migrate from k6
```

### The web UI

```bash
loadr run --ui test.yaml          # standalone run with a live dashboard
# or run the full management plane:
loadr controller --ui-bind 0.0.0.0:6464
```

Live RPS/latency/error charts, test editor with validation, run history,
threshold status, agent fleet view, start/stop/pause/scale from the browser.
Dark mode included.

### Distributed load

```bash
loadr controller --bind 0.0.0.0:7625 &            # coordination plane
loadr agent --join ctrl-host:7625 --name agent-1  # on each load generator
loadr run --controller ctrl-host:6464 test.yaml   # submit via the controller API
```

VU counts and arrival rates are partitioned across agents; HDR histograms are
merged centrally so percentiles are **exact**, never averaged. Or bring up the
whole stack — controller, 3 agents, Prometheus, Grafana dashboard — with one
command:

```bash
docker compose -f deploy/docker-compose.yml up --build
```

### Embedded JavaScript

```js
import http from 'loadr/http';
import { check, sleep } from 'loadr';

export function setup() { return { token: 'abc' }; }

export default function (data) {
  const res = http.get('/items', { headers: { Authorization: `Bearer ${data.token}` } });
  check(res, { 'status 200': (r) => r.status === 200 });
  sleep(1);
}
```

Reference it from YAML (`js: { file: ./script.js }`), call exported functions
per scenario (`exec: myFunction`), hook every request
(`beforeRequest`/`afterRequest`), or inline one-liners
(`${js: Math.random()}`). Sandboxed per VU with memory & time limits.

### Plugins

```bash
loadr plugin list
loadr plugin install ./my-extractor    # a .wasm component or native library
```

Five plugin types — protocol, output, extractor, assertion, service — over two
mechanisms: **WASM components** (WIT-defined, sandboxed, portable) and
**native libraries** (abi_stable, for performance-critical work). The web UI
itself is a service plugin. Examples for every type live in
[`plugins/examples/`](plugins/examples/).

## Documentation

The full book lives at **[loadr.io/docs](https://loadr.io/docs/)**:
getting started, the complete YAML reference (with JSON Schema for your
editor: `loadr schema > loadr.schema.json`), JS API docs, plugin development,
distributed testing, and k6/JMeter migration guides. Architecture and design
decisions: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Repository layout

```
crates/loadr-core        engine: executors, VUs, metrics (HDR), thresholds
crates/loadr-config      YAML schema, validation, ${...} interpolation
crates/loadr-js          embedded QuickJS runtime + loadr scripting stdlib
crates/loadr-protocols   HTTP/1.1+2, WebSocket, SSE, gRPC, GraphQL, TCP, UDP, no-op
crates/loadr-outputs     JSONL, CSV, Prometheus, InfluxDB, OTLP, StatsD
crates/loadr-plugin-api  WASM (WIT) + native (abi_stable) plugin SDK
crates/loadr-agent       distributed controller/agent coordination (gRPC)
crates/loadr-convert     .jmx and k6 script importers
crates/loadr-cli         the `loadr` binary
plugins/loadr-plugin-webui   the management web UI
examples/                15 runnable test definitions
deploy/                  Dockerfile, compose, k8s, Helm, Grafana dashboard
```

## Development

```bash
cargo test --workspace                              # full test suite
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p loadr-cli -- run examples/01-quickstart.yaml
mdbook serve docs                                   # docs at localhost:3000
```

## License

[Elastic License 2.0](LICENSE) — source-available. You may use, copy, modify and
redistribute loadr, **but you may not offer it to third parties as a hosted or
managed service** that exposes a substantial set of its functionality.
Copyright © 2026 Levantar / Andrew Rea.
