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
| `data_sent` / `data_received` | Counter | request/response payload bytes reported by the protocol |

## HTTP (and GraphQL)

| Metric | Kind |
|---|---|
| `http_reqs` | Counter |
| `http_req_duration` | Trend (sending + waiting + receiving) |
| `http_req_blocked` | Trend (connection acquisition) |
| `http_req_connecting` | Trend (TCP) |
| `http_req_tls_handshaking` | Trend |
| `http_req_sending` / `http_req_waiting` / `http_req_receiving` | Trend |
| `http_req_failed` | Rate (transport error or status ≥ 400; transport failures carry an `error_kind` tag). Recorded for **every** protocol, not just HTTP — for gRPC a sample is a failure when the transport errors or the status is non-zero. |

## Other protocols

| Metric | Kind |
|---|---|
| `ws_connecting`, `ws_session_duration` | Trend |
| `ws_msgs_sent`, `ws_msgs_received` | Counter |
| `grpc_reqs` / `grpc_req_duration` | Counter / Trend |
| `graphql_reqs` / `graphql_req_duration` | Counter / Trend |
| `tcp_reqs` / `tcp_req_duration` | Counter / Trend |
| `udp_reqs` / `udp_req_duration` | Counter / Trend |
| `noop_reqs` / `noop_req_duration` | Counter / Trend (duration is always zero) |

## Request roll-ups & throughput

The end-of-run summary adds two synthetic roll-ups over the per-protocol request
metrics, plus one derived success counter:

| Metric | Kind | Meaning |
|---|---|---|
| `request_reqs` | Counter | sum of every `*_reqs` counter — **total requests attempted** across all protocols |
| `request_duration` | Trend | merged `*_req_duration` across all protocols |
| `request_reqs_ok` | Counter | requests that got a non-error response: `request_reqs` minus `http_req_failed` |

`request_reqs`, the per-family `*_reqs` counters, and `iterations` all count
**attempts, not successes** — a failed or no-response request still increments
them, so their per-second value (`.../s`) is *attempted* throughput. For
success-rate throughput use `request_reqs_ok` (its `.../s`). Note that "ok"
here means the request received a valid protocol response (transport ok, gRPC
status 0 / HTTP < 400); it does **not** mean the request was accepted by
application-level `check`s — use your `check` pass counts for that.

In the console summary, `http_req_failed` renders as the failure rate with the
glyphs on the good/bad outcomes:

```
http_req_failed...: 86.57% failed — ✓ <ok> ✗ <failed>
```

where `✓` is the count of requests that succeeded and `✗` the count that failed
(the opposite polarity from `checks`, where `✓` marks passing checks).

## Standard tags

`scenario`, `name` (request name), `method`, `status`, `proto`, `group`
(`::outer::inner`), `check` (on `checks` samples), `error_kind` (on
`http_req_failed` transport failures), `exception` / `site` (on
`vu_exceptions`), `instance` (legacy agent name used by distributed
thresholds), plus everything from `defaults.tags`, scenario `tags:` and request
`tags:`. Controller-exported Prometheus series additionally have
`loadr_agent`, `loadr_agent_id`, `loadr_run_id`, and `loadr_run_name`; the
agent-injected `instance` value (the agent name) is omitted there because it
duplicates `loadr_agent` and would collide with Prometheus' scrape-target
label. An `instance` tag you set yourself is preserved.

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
