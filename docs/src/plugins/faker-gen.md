# faker-gen plugin

> **Status:** planned — this plugin is not in the signed index yet. The shape
> below describes the intended service/feeder contract; treat it as a design
> note until it ships.

`loadr-plugin-faker-gen` is a **service plugin** (`kind = "service"`, role:
*data sources & feeders*). It starts a small in-process generator that produces
fake data rows — emails, UUIDs, names, numbers — for feeders to hand out to VUs.
It is **pure Rust** built on the `fake` and `rand` crates: no network calls, no
external generator process, no data file to ship. Every row is manufactured on
demand from a schema you declare.

The generator is **seeded**, so a run is reproducible: the same `seed` and the
same `schema` yield the same sequence of rows on every run and every machine.
Omit the seed and each run draws fresh random data instead. Rows are pulled by
VUs exactly the way a CSV or JSON feeder is consumed — through
`${data.<name>.<field>}` interpolation — so you get feeder-style data without
authoring or maintaining a fixture file.

The service lifecycle it uses is the native `FfiService` contract
(`start(config_json) → stop()`) documented in
[Native plugins](native.md#the-interface).

## Install

Once published, `faker-gen` will resolve from the signed
[plugin index](installing.md) by name — no build toolchain required:

```bash
loadr plugin install faker-gen
loadr plugin info faker-gen
```

This picks the artifact for your host target, checks it against the plugin ABI
your `loadr` build provides, verifies its sha256 and unpacks it into your
plugins directory (`~/.loadr/plugins/faker-gen/`, or `$LOADR_PLUGINS_DIR`).

The installed manifest declares a native service plugin:

```toml
[plugin]
name = "faker-gen"
version = "0.1.0"
kind = "service"
type = "native"
entry = "libloadr_plugin_faker_gen.so"
description = "Seeded fake-data generator that feeds VUs like a CSV feeder"
```

To run straight from a build tree instead, point the plan's `plugins:` entry at
the built artifact (`path: target/release/libloadr_plugin_faker_gen.so`) rather
than resolving it by name.

## Use it in a test

List the plugin under `plugins:` with its generator config, then reference it as
a `data:` feeder. The service starts once at the beginning of the run and every
VU pulls rows from it; a field is read with `${data.<feeder>.<field>}`, the same
syntax a CSV feeder uses.

```yaml
plugins:
  - name: faker-gen                    # or: { name: faker-gen, path: target/release/libloadr_plugin_faker_gen.so }
    config:
      schema:                          # field name -> generator kind
        email: email
        id: uuid
      seed: 42                         # fixed seed -> reproducible rows

data:
  users:
    type: plugin                       # feeder backed by a service plugin
    source: faker-gen                  # the plugin that produces rows

scenarios:
  signups:
    executor: constant-vus
    vus: 50
    duration: 1m
    flow:
      - request:
          name: register
          method: POST
          url: https://api.example.com/users
          body:
            json:
              id: "${data.users.id}"
              email: "${data.users.email}"
          checks:
            - { type: status, equals: 201 }
```

Because the generator is seeded, `data.users.id` and `data.users.email` resolve
to the same sequence on every run — handy for correlating a failure to an exact
row, or for keeping a run diffable in CI.

## Config reference

Config is the JSON object under the plugin's `config:` key. It is handed to the
service's `start()` verbatim at run start.

| Key      | Type   | Default          | Meaning |
|----------|--------|------------------|---------|
| `schema` | object | *(required)*     | Map of output field name → generator kind. Each key becomes a feeder field readable as `${data.<feeder>.<key>}`. |
| `seed`   | number | *(random)*       | Seed for the `rand` PRNG. Fixed seed ⇒ deterministic, reproducible rows. Omit it for fresh random data each run. |

### Generator kinds

The value of each `schema` entry names a `fake`-crate generator:

| Kind        | Example output                         |
|-------------|----------------------------------------|
| `email`     | `harold.reilly@example.com`            |
| `uuid`      | `9f1c8e2a-...` (v4)                     |
| `name`      | `Ada Lovelace`                         |
| `username`  | `ada_l`                                |
| `word`      | `lorem`                                |
| `int`       | a random integer                       |
| `bool`      | `true` / `false`                       |

Unknown kinds are rejected when the service starts, so a typo in `schema`
fails the run loudly rather than emitting empty fields.

## Metrics

The generator emits one counter for the rows it hands out:

| Metric                 | Kind    | Meaning |
|------------------------|---------|---------|
| `faker_rows_generated` | counter | One increment per row pulled by a VU |

That makes it easy to confirm the feeder is actually driving load and to gate on
it in `thresholds`:

```yaml
thresholds:
  faker_rows_generated: [ "count>0" ]
```

## Notes

- **Deterministic by seed.** With a fixed `seed` the row sequence is stable
  across runs and machines — reproducible fixtures with no file to commit. Drop
  the seed for non-repeating random data.
- **No external dependency.** Everything is generated in-process with the
  `fake` and `rand` crates; there is no generator server to run and no fixture
  to ship, unlike a CSV/JSON feeder that reads a `path:`.
- **Feeder semantics.** Rows are read through the same `${data.<name>.<field>}`
  interpolation as any other feeder, but `mode`/`pick`/`on_eof` do not apply —
  those describe iterating a stored row set, and this feeder mints a fresh row
  on every request instead (see
  [Plugin-backed sources](../yaml/data.md#plugin-backed-on-demand-sources)).
- **In-process service.** Native service plugins run in-process with full
  privileges (see [Native plugins](native.md#safety-notes)); the generator does
  no I/O beyond producing rows.
</content>
</invoke>
