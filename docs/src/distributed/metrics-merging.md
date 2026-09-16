# Metric aggregation

## The percentile trap

Most homegrown distributed setups report per-node percentiles and average
them. **That number is wrong** — often wildly. If agent A's p99 is 100 ms and
agent B's p99 is 1000 ms, the fleet's true p99 is *not* 550 ms; it depends on
the full shape of both distributions.

loadr never averages percentiles:

1. Every agent records trend metrics into **HDR histograms** (3 significant
   figures, auto-resizing).
2. Each second, the agent serializes a *delta* histogram (HDR V2 encoding)
   and streams it to the controller.
3. The controller **merges histograms** — a lossless operation — into a
   central aggregator per (metric, tag set).
4. Percentiles, thresholds, the live UI and the final summary are computed
   from the merged histograms only.

Counters and rates merge as exact sums (`passes`/`total`). Fleet-capacity
gauges (`vus` and `vus_max`) add the current value from each agent; other
gauges keep the most recent value plus min/max envelopes.

This is verified by tests: two in-process agents record disjoint latency
ranges (1–1000 ms and 1001–2000 ms); the merged p99 must equal the true p99
of the union (~1980 ms), where naive averaging would claim ~1485 ms.

## Tags & per-agent visibility

Request and scenario samples carry the legacy `instance: <agent-name>` tag,
so you can continue to threshold per instance:

```yaml
thresholds:
  "http_req_duration{instance:agent-1}": [ "p(95)<500" ]
```

The controller's Prometheus endpoint uses trusted labels derived from the
agent session:

- `loadr_agent` — agent name
- `loadr_agent_id` — stable agent identifier
- `loadr_run_id` and `loadr_run_name` — run identity

Detailed series retain their usual `loadr_*` names. The controller also emits
exact all-tags rollups as `loadr_fleet_*`; for example fleet TPS is:

```promql
rate(loadr_fleet_http_reqs_total{loadr_run_id="$run"}[30s])
```

Per-agent TPS remains available from detailed series:

```promql
sum by (loadr_agent) (
  rate(loadr_http_reqs_total{loadr_run_id="$run"}[30s])
)
```

The live Web UI uses the same centrally merged histograms for fleet, scenario,
and per-agent latency rows. It never reconstructs a fleet percentile from
numeric agent percentiles. The run page also shows assigned, contributing, and
lost-agent completeness so partial-fleet data cannot look like a normal pass.

Fleet trend quantiles come from the centrally merged HDR histogram, rather
than `max` or an average of agent quantiles. Run discovery is exposed through
`loadr_fleet_run_info` and
`loadr_fleet_run_started_timestamp_seconds`. The endpoint publishes every
pending/running run concurrently, plus the newest completed run — so a
finished run's final counters stay scrapeable while the next run is starting,
not just while the fleet idles.

## Threshold evaluation

Thresholds run **centrally** against the merged data — `abort_on_fail`
decisions consider fleet-wide reality, then fan `stop` commands out to every
agent. Local evaluation on agents is disabled in distributed runs to avoid
split-brain aborts.
