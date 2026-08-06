# Prometheus output

Prometheus is the only streaming output. A run may expose a scrape endpoint,
push remote-write batches, or do both:

```yaml
outputs:
  - type: prometheus
    listen: 127.0.0.1:9091
    remote_write_url: http://prometheus:9090/api/v1/write
    interval: 5s
    final_scrape_grace: 10s
```

`final_scrape_grace` keeps a standalone endpoint alive briefly after the run
so Prometheus can collect the terminal `vus=0` snapshot. The CLI shorthand is:

```bash
loadr run --output prometheus=127.0.0.1:9091 test.yaml
```

Controller exports attach trusted run and agent labels and add exact
fleet-level aggregates. The dashboard under `deploy/grafana/` targets these
names.
