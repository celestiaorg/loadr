# Summary

[Introduction](introduction.md)

# Getting started

- [Installation](getting-started/installation.md)
- [Your first test](getting-started/first-test.md)
- [The CLI](getting-started/cli.md)

# Writing tests (YAML reference)

- [Test definition overview](yaml/overview.md)
- [Scenarios & executors](yaml/scenarios-executors.md)
- [Requests](yaml/requests.md)
- [Flow control (loops, branches)](yaml/flow-control.md)
- [Extraction & correlation](yaml/extraction.md)
- [Assertions & checks](yaml/assertions-checks.md)
- [Thresholds](yaml/thresholds.md)
- [Data parameterization](yaml/data.md)
- [Feeders & throttling](yaml/feeders.md)
- [Variables, secrets & interpolation](yaml/variables.md)
- [Think time & pacing](yaml/timers.md)
- [Outputs](yaml/outputs.md)
- [Environments](yaml/environments.md)

# JavaScript

- [Embedded JavaScript overview](js/overview.md)
- [Lifecycle hooks](js/lifecycle.md)
- [JS API reference](js/api.md)

# Protocols

- [HTTP](protocols/http.md)
- [WebSocket](protocols/websocket.md)
- [Server-Sent Events](protocols/sse.md)
- [gRPC](protocols/grpc.md)
- [GraphQL](protocols/graphql.md)
- [Browser](protocols/browser.md)
- [TCP & UDP](protocols/sockets.md)
- [No-op self-test](protocols/noop.md)

# Distributed testing

- [Overview](distributed/overview.md)
- [Controller & agents](distributed/controller-agents.md)
- [Metric aggregation](distributed/metrics-merging.md)

# Web UI

- [The management UI](webui.md)

# Desktop app

- [loadr Desktop](desktop.md)

# Plugins

- [Plugin system overview](plugins/overview.md)
- [Installing plugins](plugins/installing.md)
- [WASM plugins](plugins/wasm.md)
- [Native plugins](plugins/native.md)
- [Writing a plugin in another language (C ABI)](plugins/c-abi.md)
- [MongoDB plugin](plugins/mongo.md)
- [PostgreSQL plugin](plugins/postgres.md)
- [MySQL plugin](plugins/mysql.md)
- [Redis plugin](plugins/redis.md)
- [Apache Kafka plugin](plugins/kafka.md)
- [Elasticsearch plugin](plugins/elasticsearch.md)
- [RabbitMQ plugin](plugins/rabbitmq.md)
- [NATS plugin](plugins/nats.md)
- [MQTT plugin](plugins/mqtt.md)
- [Cassandra / ScyllaDB plugin](plugins/cassandra.md)
- [Redis data loader](plugins/redis-loader.md)
- [SQL feeder](plugins/sql-feeder.md)
- [S3 dataset feeder](plugins/s3-dataset.md)
- [Synthetic data generator](plugins/faker-gen.md)
- [Datadog output](plugins/datadog.md)
- [Slack notifier](plugins/slack-notifier.md)
- [Webhook output](plugins/webhook.md)
- [S3 archive output](plugins/s3-archive.md)
- [JUnit report](plugins/junit-report.md)
- [CloudWatch collector](plugins/cloudwatch.md)
- [OTLP metrics collector](plugins/otlp-metrics.md)
- [Kubernetes metrics collector](plugins/k8s-metrics.md)
- [JWT decode extractor](plugins/jwt-decode.md)
- [XPath extractor](plugins/xpath.md)
- [CSS selector extractor](plugins/css-select.md)
- [Protobuf decode extractor](plugins/protobuf-decode.md)
- [JSON Schema assertion](plugins/json-schema.md)
- [OpenAPI contract assertion](plugins/openapi-contract.md)
- [Response signature assertion](plugins/response-signature.md)
- [OAuth2 token minter](plugins/oauth2-minter.md)
- [AWS SigV4 signer](plugins/aws-sigv4.md)
- [HMAC signer](plugins/hmac-signer.md)
- [Vault secret fetcher](plugins/vault-fetch.md)
- [DB seeder](plugins/db-seeder.md)
- [Testcontainers fixture](plugins/testcontainers.md)
- [Data cleanup](plugins/data-cleanup.md)
- [Developing a plugin](plugins/developing.md)
- [Publishing a plugin](plugins/publishing.md)

# Migration

- [Migrating from k6](migration/from-k6.md)
- [Migrating from JMeter](migration/from-jmeter.md)
- [Recording a browser session (HAR)](migration/from-har.md)
- [Replaying access logs](migration/from-access-logs.md)

# Reporting

- [HTML reports & time-series charts](reporting.md)

# Observability

- [System metrics (observe)](observe/system.md)

# Guides

- [Recording a session (loadr record)](guides/record.md)
- [Generating from a contract (loadr gen)](guides/gen.md)
- [Explaining a run (loadr explain)](guides/explain.md)
- [Run history & regression detection (loadr history)](guides/history.md)
- [Comparing runs (loadr compare)](guides/compare.md)
- [Parameter sweeps (loadr sweep)](guides/sweep.md)
- [Payload generation & complexity testing](guides/payload.md)
- [Chaos testing (fault injection)](guides/chaos.md)

# Continuous integration

- [GitHub Actions & JUnit reports](ci/github-actions.md)

# Reference

- [Built-in metrics](reference/metrics.md)
- [Exit codes](reference/exit-codes.md)
- [JSON Schema & editor setup](reference/json-schema.md)

# About

- [Credits & influences](credits.md)
