# SQL feeder plugin

> **Status:** planned — not yet in the signed [plugin index](installing.md). This
> page documents the intended service-to-file shape; the config keys may still
> change before the first release.

`loadr-plugin-sql-feeder` is a **service** plugin in the *data sources &
feeders* role. Instead of driving a target, it prepares a feeder fixture: when
the service starts it opens one connection, runs a single `SELECT` via
[`sqlx`](https://github.com/launchbadge/sqlx), and writes the result set as a
JSON array of row objects. A normal `type: json` data source then hands those
rows to VUs through the usual feeder interpolation. The plugin does **not**
provide the `data_source` capability, so do not configure it as
`data.<name>.type: plugin`.

Reach for it when the data that drives a run already lives in a table — real
user IDs, order numbers, API keys, tenant slugs — and you would otherwise export
it to a CSV first. The service does that export for you, at startup, against the
live schema. The database is touched **once**; it is not on the request hot path.

Like the [PostgreSQL](postgres.md) and [MySQL](mysql.md) protocol plugins, it is
near-pure Rust: `sqlx` built with **rustls** for TLS (no OpenSSL, no `libpq`),
gating only the driver features for the backends it serves — modelled on those
drivers, with the same connection-string handling and per-backend feature
gating.

The service ABI it uses is documented in
[Native plugins](native.md#the-interface).

## Install

`sql-feeder` will ship in the signed [plugin index](installing.md), so once
released you install it by name — no build toolchain required:

```bash
loadr plugin install sql-feeder
loadr plugin info sql-feeder
```

This resolves `sql-feeder` in the index, picks the artifact for your host
target, checks it against the plugin ABI your `loadr` build provides, downloads
it, verifies its sha256 and unpacks it into your plugins directory
(`~/.loadr/plugins/sql-feeder/`, or `$LOADR_PLUGINS_DIR`).

The installed manifest declares a service plugin:

```toml
[plugin]
name = "sql-feeder"
kind = "service"
type = "native"
entry = "libloadr_plugin_sql_feeder.so"   # .dylib on macOS, .dll on Windows
```

To run straight from a build tree instead, point the plan's `plugins:` entry at
the built artifact rather than resolving it by name:

```bash
cargo build -p loadr-plugin-sql-feeder --release
```

```yaml
plugins:
  - { name: sql-feeder, path: target/release/libloadr_plugin_sql_feeder.so }
```

## Use it in a test

List the plugin under `plugins:` with the connection `url`, `query`, and
`output` file. The service writes that file as JSON; the plan's `data:` block
reads it with the built-in `type: json` feeder. Each row column the query
returns binds a value the VUs reference through the usual
`${data.<name>.<column>}` interpolation. So a `select id, email from users`
exposes `${data.users.id}` and `${data.users.email}`:

```yaml
plugins:
  - name: sql-feeder          # or add: path: target/release/libloadr_plugin_sql_feeder.so
    config:
      url: postgres://loadr:loadr@db.example.com:5432/loadr
      query: select id, email from users
      output: data/users-from-db.json

data:
  users:
    type: json
    path: data/users-from-db.json
    mode: shared
    pick: sequential

scenarios:
  signup_replay:
    executor: constant-vus
    vus: 25
    duration: 2m
    flow:
      - request:
          name: fetch profile
          method: GET
          url: "https://api.example.com/users/${data.users.id}"
          headers: { X-User-Email: "${data.users.email}" }
          checks:
            - { type: status, equals: 200 }
```

The query runs **once**, when the service starts; the request loop only reads
from the generated JSON feeder file, so no per-VU database traffic happens
during the test.

## Config reference

The plugin export is set through the `plugins[].config` block:

| Key      | Required | Default | Meaning |
|----------|----------|---------|---------|
| `url`    | yes      | —       | Connection URI, e.g. `postgres://…` / `mysql://…`; passed straight to `sqlx` (any URL it accepts, including `?sslmode=require` for TLS). |
| `query`  | yes      | —       | The `SELECT` to run once at startup; its column names become the feeder's field names. |
| `output` | yes      | —       | JSON file path to write; point a `data.<name>.type: json` feeder at the same path. |

`${...}` interpolation works in `url` and `query`, so an environment variable or
`--env` value can supply the DSN (`url: "${env.DATABASE_URL}"`) without
hard-coding credentials in the plan.

Only row-producing statements make sense here: the query must return a result
set, and an empty `query` is rejected. The generated file uses the same JSON
array-of-objects shape as any local `type: json` feeder.

The normal JSON feeder controls apply to the `data.<name>` block:
`mode`, `pick`, and `on_eof` behave exactly as they do for a local JSON file.
See [Data parameterization](../yaml/data.md).

## Metrics

Because the query runs once at startup rather than per request, the feeder does
not emit a per-request metric family. A startup failure — an unreachable
database, a bad DSN, a query error, or a file write error — fails before VUs
begin rather than surfacing as a request failure, so there is no
`sql_feeder_reqs` / `_req_duration` family.

## Notes

- **Fetched once, then written.** The whole result set is read at startup,
  written to `output`, and the connection is closed before the load phase
  begins. The size of the set is bounded by available memory while the file is
  generated — scope the query with a `WHERE`/`LIMIT` rather than selecting an
  unbounded table.
- **Feeder, not a target.** This plugin *sources data*; it does not send load to
  the database. To put a database itself under test, use the
  [PostgreSQL](postgres.md) or [MySQL](mysql.md) protocol plugin, which run one
  query per request on the hot path.
- **Near-pure Rust.** `sqlx` is built with the **rustls** TLS backend and only
  the driver feature for the backends it serves, mirroring the postgres/mysql
  plugins — no OpenSSL or client-library system dependency, so the artifact is
  self-contained across platforms and installs by name with no build toolchain.
- **Synchronous ABI.** Like the other native plugins, it owns a single Tokio
  runtime and `block_on`s the async `sqlx` fetch at startup, because the plugin
  ABI is synchronous.
