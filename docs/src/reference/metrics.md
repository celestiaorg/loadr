# Built-in metrics

Kinds: **Counter** (sum), **Gauge** (last/min/max), **Rate** (pass fraction),
**Trend** (HDR histogram: avg/min/med/max + any percentile).

## Core

| Metric | Kind | Meaning |
|---|---|---|
| `iterations` | Counter | completed iterations |
| `iteration_duration` | Trend | full iteration time (ms) |
| `dropped_iterations` | Counter | arrival-rate starts skipped (no free VU at `max_vus`) |
| `vus` | Gauge | active virtual users |
| `vus_max` | Gauge | peak VUs |
| `requests_in_flight` | Gauge | active protocol requests |
| `checks` | Rate | check pass rate (tag `check` = name) |
| `vu_exceptions` | Counter | uncaught JS exceptions in hooks/`exec`/`js` steps (tags `exception` = normalised message, `site`) |
| `data_sent` / `data_received` | Counter | bytes on the wire |

## HTTP (and GraphQL)

| Metric | Kind |
|---|---|
| `http_reqs` | Counter |
| `http_req_duration` | Trend (sending + waiting + receiving) |
| `http_req_blocked` | Trend (connection acquisition) |
| `http_req_connecting` | Trend (TCP) |
| `http_req_tls_handshaking` | Trend |
| `http_req_sending` / `http_req_waiting` / `http_req_receiving` | Trend |
| `http_req_failed` | Rate (transport error or status ≥ 400; transport failures carry an `error_kind` tag) |

## Other protocols

| Metric | Kind |
|---|---|
| `ws_connecting`, `ws_session_duration` | Trend |
| `ws_msgs_sent`, `ws_msgs_received` | Counter |
| `grpc_reqs` / `grpc_req_duration` | Counter / Trend |
| `graphql_reqs` / `graphql_req_duration` | Counter / Trend |
| `tcp_reqs` / `tcp_req_duration` | Counter / Trend |
| `udp_reqs` / `udp_req_duration` | Counter / Trend |

## Standard tags

`scenario`, `name` (request name), `method`, `status`, `proto`, `group`
(`::outer::inner`), `check` (on `checks` samples), `error_kind` (on
`http_req_failed` transport failures), `exception` / `site` (on
`vu_exceptions`), `instance` (agent name in distributed runs), plus everything
from `defaults.tags`, scenario `tags:` and request `tags:`.

## Custom metrics

Declare in YAML for threshold validation, or create ad hoc from JS:

```yaml
metrics:
  carts_created: { kind: counter }
  render_time: { kind: trend, time: true }
```

```js
new Counter('carts_created').add(1);
session.trendAdd('render_time', 16.6);
```
