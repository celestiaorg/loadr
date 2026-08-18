# The management UI

A built-in, RabbitMQ-style management interface — shipped as a first-party
**service plugin**, statically linked into the default binary.

```bash
loadr run --ui test.yaml                  # standalone: dashboard for this run
loadr controller --ui-bind 0.0.0.0:6464   # distributed: manage the whole fleet
```

Default address `127.0.0.1:6464` (loopback unless you bind otherwise —
deliberate security default).

## Pages

- **Overview** — live stat cards (interval RPS, active VUs, interval error
  rate, and exact run-to-date p95) and streaming charts, per-scenario
  table, threshold pass/fail pills, live check rates, and a **failure
  breakdown** panel (see below). Distributed runs include exact per-agent
  request, VU, error, and latency contributions. Updates once per second over
  SSE and visibly marks a disconnected or stale stream.
- **Runs** — every run with state and outcome; a run page with live charts,
  the threshold table, scenario breakdown, and controls: **Stop** (graceful),
  **Kill**, **Pause/Resume**, and a VU dial for `externally-controlled`
  scenarios. Distributed controls return success only after every targeted
  agent acknowledges applying the command. Finished runs render the full
  summary (metric table, checks, thresholds).
- **Tests** — a test library: upload/edit YAML in the browser with
  line-numbered editing and one-click **Validate** (including the selected
  environment and controller-side referenced files), then **Run**.
- **Agents** — the fleet: health, active VUs, cores, labels, last heartbeat.
  Per-run contribution is shown on the live run dashboard.
- **Logs** — live tail when the embedding backend provides log capture. The
  stock single-run and controller CLI backends currently report this
  capability as unavailable instead of showing a misleading empty log.

Controller run summaries and exact UI rollups are persisted under
`<storage-dir>/history` and are available again after a controller restart. A
run is complete only when every assigned agent contributes metrics and none is
lost. An otherwise-finished incomplete run is marked **degraded**; an aborted
or failed run keeps that stronger state. In every case, incomplete runs list
the missing/lost agents and cannot appear as passed; threshold values still
describe only the data that was received.

Dark mode is the default (there's a toggle; it remembers). No CDNs, no
trackers — the entire SPA is embedded in the binary.

## Failure breakdown

When a test produces failures, the **Failure breakdown** panel on the Overview
and live Run dashboards groups them by *cause* so you can see *why* requests
failed, not just *how many*. Four groups are shown, each row carrying its count
and share of the group, with a bar for quick scanning:

- **Response status** — failed responses grouped by protocol and status code.
  Known HTTP and gRPC statuses include their canonical names, such as
  `HTTP · Internal Server Error (500)` and `gRPC · UNAVAILABLE (14)`.
- **Transport / error** — connection-level failures grouped by a coarse kind
  (`timeout`, `dns`, `tls`, `connection_refused`, `connection_reset`,
  `connection`, `transport`) plus prepare/protocol/extraction errors.
- **Failed checks** — each [check](yaml/assertions-checks.md) that failed,
  by name, with the number of failing evaluations.
- **Script exceptions** — uncaught exceptions from JS hooks, `exec`
  functions, and `js` steps, grouped by a normalised message (volatile detail
  such as numbers and quoted strings is collapsed so the same logical error
  groups together).

High-cardinality groups are capped to the top causes with the remainder folded
into an **other** row.

The panel reports **failure events**, not unique failed requests: an HTTP 500
may also fail a check, so category totals can overlap. The separately displayed
failed-request count comes from `http_req_failed` and is not calculated by
adding the breakdown categories.

### Downloading the breakdown

Two buttons in the panel header export the current breakdown entirely in the
browser — no server round-trip:

- **↓ CSV** — a `category,protocol,cause,count,share_pct` file
  (`loadr-failures-<timestamp>.csv`) ready for spreadsheets or further
  analysis.
- **↓ Report** — a self-contained HTML report
  (`loadr-failures-<timestamp>.html`) you can archive or share.

The breakdown is also available programmatically as the `failures` object on
the live metrics payload (see the `/api/overview` and `/api/runs/:id/stream`
responses). Status buckets retain the compatibility `key` and additionally
carry `protocol`, `status`, and an optional `status_name`.

## Authentication

```bash
loadr controller --ui-user admin --ui-password s3cret      # HTTP Basic
loadr controller --ui-token "$(openssl rand -hex 24)"      # bearer token(s)
```

Both may be active at once; SSE connections accept
`?token=`. Without any auth flags the UI is open — bind it to loopback or put
it behind your proxy.

## API

Everything the UI does is a JSON API you can script against:

```text
GET  /api/overview                 GET  /api/runs            POST /api/runs
GET  /api/capabilities
GET  /api/runs/:id                 GET  /api/runs/:id/summary
GET  /api/runs/:id/stream (SSE)    POST /api/runs/:id/stop|pause|scale
GET  /api/agents                   GET/PUT/DELETE /api/tests[/:name]
POST /api/validate                 GET  /api/logs            GET /healthz
```
