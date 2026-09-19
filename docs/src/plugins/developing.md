# Developing a plugin

A practical walkthrough — we'll build, test and ship the `uppercase-extractor`
WASM plugin (the same one in `plugins/examples/wasm-extractor`).

## 1. Scaffold

```bash
cargo new --lib uppercase-extractor && cd uppercase-extractor
mkdir wit && cp <loadr repo>/crates/loadr-plugin-api/wit/loadr.wit wit/
```

```toml
[package]
name = "uppercase-extractor"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.58"
serde_json = "1"
```

## 2. Implement

```rust
wit_bindgen::generate!({ path: "wit", world: "loadr-plugin" });

struct Plugin;

impl exports::loadr::plugin::meta::Guest for Plugin {
    fn describe() -> exports::loadr::plugin::meta::Info {
        exports::loadr::plugin::meta::Info {
            name: "uppercase-extractor".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            kind: "extractor".into(),
            description: "boundary extractor that upper-cases the match".into(),
        }
    }
}

impl exports::loadr::plugin::extractor::Guest for Plugin {
    fn extract(body: Vec<u8>, _headers: Vec<(String, String)>, config: String) -> Option<String> {
        let cfg: serde_json::Value = serde_json::from_str(&config).ok()?;
        let (left, right) = (cfg["left"].as_str()?, cfg["right"].as_str()?);
        let text = String::from_utf8_lossy(&body);
        let start = text.find(left)? + left.len();
        let end = text[start..].find(right)? + start;
        Some(text[start..end].to_uppercase())
    }
}

export!(Plugin);
```

## 3. Build & package

```bash
rustup target add wasm32-wasip2
cargo build --release --target wasm32-wasip2

mkdir dist
cp target/wasm32-wasip2/release/uppercase_extractor.wasm dist/
cat > dist/plugin.toml <<'EOF'
[plugin]
name = "uppercase-extractor"
version = "0.1.0"
kind = "extractor"
type = "wasm"
entry = "uppercase_extractor.wasm"
description = "Boundary extractor that upper-cases the match"
EOF
```

## 4. Install & use

```bash
loadr plugin install ./dist
loadr plugin info uppercase-extractor
```

```yaml
plugins: [ { name: uppercase-extractor, config: { left: "token=", right: ";" } } ]
```

## 5. Publish to the index

A locally-installed directory is enough for development, but to make your
plugin installable by name (`loadr plugin install <name>`) it has to appear in
the **plugin index** — the catalogue described in
[Installing plugins](installing.md).

For each supported host target, package the `plugin.toml` plus the built
dynamic library into an archive (`.tar.gz` on Linux/macOS, `.zip` on Windows),
name it `<name>-<target>.<ext>`, and add an entry to `plugins/index.json`:

```json
{
  "schema": 1,
  "plugins": {
    "myproto": {
      "kind": "protocol",
      "description": "…",
      "latest": "0.1.0",
      "versions": {
        "0.1.0": {
          "min_loadr_abi": "1.0",
          "artifacts": {
            "x86_64-unknown-linux-gnu": {
              "url": "https://…/myproto-x86_64-unknown-linux-gnu.tar.gz",
              "sha256": "<sha256 of the archive>",
              "entry": "libloadr_plugin_myproto.so"
            }
          }
        }
      }
    }
  }
}
```

The release CI fills in the real `url`/`sha256` per target; bump
`min_loadr_abi` to the host ABI your build requires (the
`LOADR_PLUGIN_ABI_VERSION` you compiled against). The `entry` is the
per-platform artifact filename (`libloadr_plugin_<name>.so` /
`.dylib` / `loadr_plugin_<name>.dll`) and must match the `entry` inside the
archive's `plugin.toml`.

Until the index goes live you can hand a tester an archive directly:

```bash
loadr plugin install ./myproto-x86_64-unknown-linux-gnu.tar.gz --allow-untrusted
```

## Testing tips

- Drive the component directly in a Rust test with
  `loadr_plugin_api::WasmExtractor::load(path)` — exactly what loadr's own
  test suite does for the examples.
- For native plugins: build with `cargo build`, then
  `NativePlugin::load("target/debug/libmy_plugin.so")` in a test.
- Keep configs JSON-serializable and document them in your README; loadr
  passes the `config:` value through verbatim.

## Versioning rules

- WASM: the WIT package version (`loadr:plugin@0.1.0`) is the contract.
- Native: `abi_stable` layout checking is the contract; additionally the
  root module carries `abi_version` — bump on breaking changes and loadr
  will refuse mismatches with a clean message.

## Native protocol plugins

A **protocol plugin** adds a new load-test target (a database, a queue, a
bespoke wire protocol). It must be a *native* plugin — WASM plugins can only be
extractors/assertions. `loadr-plugin-mongo` is the reference implementation; see
[the MongoDB plugin](mongo.md) for an end-to-end example.

### The ABI

A protocol plugin implements the synchronous `FfiProtocol` trait and exports it
via `make_protocol`:

```rust
use loadr_plugin_api::abi::{FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION};
use loadr_plugin_api::{FfiRequest, FfiResponse};
use abi_stable::std_types::{RString, ROption::{RNone, RSome}};

struct MyProto;

impl FfiProtocol for MyProto {
    fn name(&self) -> RString { RString::from("myproto") }
    fn execute(&self, request_json: RString) -> RString {
        // parse FfiRequest JSON, run the op, return FfiResponse JSON.
        // MUST NOT panic — report failures via the response `error` field.
    }
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(MyProto, abi_stable::erased_types::TD_Opaque)
}

extern "C" fn plugin_info() -> RString { /* PluginInfo JSON, incl. "schemes" */ }

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RNone,
        make_protocol: RSome(make_protocol),
        make_service: RNone,
    }
}
```

Key facts that shape the design:

- `execute` is **synchronous**, takes `&self`, and runs on **one shared
  instance** (`Send + Sync`) created once via `make_protocol()`. There is no
  per-VU context across the FFI boundary.
- A plugin that drives an async client (most do) must therefore **own its async
  machinery**: create its own Tokio runtime inside the cdylib and `block_on`,
  and keep an **internal connection pool** keyed by the connection target
  (e.g. `OnceCell<Mutex<HashMap<String, Client>>>`), reused across every call
  and VU. Do not connect per request.
- Build the crate as `crate-type = ["cdylib"]`, `publish = false`, a member of
  the workspace under `plugins/`.

### Request / response JSON

The host serializes a `loadr_plugin_api::FfiRequest` to JSON and hands it to
`execute`; the plugin returns a `FfiResponse` as JSON:

Under native ABI v1, the host caches the serialized plugin-level `config`, so
it does not clone and traverse that invariant value again for every request.
The config remains part of each request payload, however, so ABI-v1 plugins
still parse the request JSON, including `config`, on every `execute` call.

```jsonc
// FfiRequest (host -> plugin)
{
  "name": "find users",          // metric `name` tag
  "method": "POST",
  "url": "mongodb://h:27017/db",  // the connection target / URL
  "headers": [["k", "v"]],
  "body_b64": "",                 // base64 request body
  "timeout_ms": 30000,
  "options": { ... },             // the request's `plugin:` block, ${...}-interpolated
  "config": { ... }               // merged plugin config (manifest [config] + PluginRef.config)
}

// FfiResponse (plugin -> host)
{
  "status": 1,                    // your convention; non-failed by default
  "status_text": "OK",
  "headers": [],
  "body_b64": "",
  "duration_ms": 1.7,
  "error": null,                  // Some(msg) => request is marked failed
  "extras": { "docs": 3 }         // free-form; the host can read fields out (see below)
}
```

The host already interpolates `${...}` in the request's `plugin:` block before
the plugin sees it, so `options` arrives fully rendered.

### Declaring the URL scheme(s) — routing contract

A runtime-loaded plugin cannot edit core, so it **declares the URL scheme(s) it
serves** and the host wires up routing automatically. Declare schemes in two
places (the manifest wins; `info()` is the fallback when a plugin is loaded by
bare path):

```toml
# plugin.toml
[plugin]
name = "myproto"
kind = "protocol"
type = "native"
entry = "libmyproto.so"
schemes = ["myproto", "myp"]      # URL schemes this plugin claims
```

```rust
// plugin_info() JSON
{ "name": "myproto", "kind": "protocol", "schemes": ["myproto", "myp"], ... }
```

When the host loads the plugin it registers those schemes with a process-global
scheme router (`loadr_core::protocol::register_plugin_schemes`). After that,
`ProtocolRegistry::infer` resolves a URL like `myproto://host/...` to the
handler whose `name()` is `myproto`. **Built-in schemes always win** over plugin
aliases, and an explicit `protocol: myproto` in YAML also resolves (it must
match the plugin handler's `name()`, which the validator accepts because it is
listed under `plugins:`).

So a test can target the plugin either way:

```yaml
plugins: [ { name: myproto } ]
flow:
  - request: { url: "myproto://host/...", plugin: { ... } }   # routed by scheme
  - request: { url: "host/...", protocol: myproto, plugin: { ... } }  # routed by name
```

### Metrics

The host derives a metric **family** from the handler `name()` for plugin
protocols, emitting `<name>_reqs` (counter), `<name>_req_duration` (trend), and
— when the response includes `extras.docs` — `<name>_docs` (counter). A response
with a non-null `error` increments `http_req_failed`. So `loadr-plugin-mongo`
(name `mongo`) produces `mongo_reqs` / `mongo_req_duration` / `mongo_docs`
without any core changes per plugin.

### Testing

- Unit-test the `execute`/`handle` logic by building `FfiRequest` JSON and
  asserting on the `FfiResponse` — no host needed.
- Integration-test against a real backend behind an env-var gate (e.g.
  `LOADR_TEST_MONGO_URL`) so CI skips it when the service is absent; bring the
  service up via `examples/harness/docker-compose.yml`.
- End-to-end, load the built artifact with
  `loadr_plugin_api::NativePlugin::load("target/debug/libmyproto.so")`.

## Native data-source plugins

A **data-source plugin** generates `data.<name>` rows on demand instead of
reading them from a CSV/JSON file — see [Data parameterization](../yaml/data.md#plugin-backed-on-demand-sources)
for the `type: plugin` YAML surface. It's the right shape when a row must be
computed fresh at request time (a signed, time-sensitive payload) rather than
loaded once from a fixture. `plugins/examples/native-data-source` (`tx-signer`)
is the reference implementation: it signs a small payload with Ed25519 on
every call.

### The ABI

A data-source plugin implements `FfiDataSource` and exports it via
`make_data_source`:

```rust
use loadr_plugin_api::abi::{
    FfiDataSource, FfiDataSourceBox, FfiDataSource_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use abi_stable::std_types::{RString, RResult::{ROk, RErr}, ROption::{RNone, RSome}};

#[derive(Default)]
struct MySource { /* signing key, per-source config, ... */ }

impl FfiDataSource for MySource {
    fn name(&self) -> RString { RString::from("my-source") }

    /// Called once before VUs start.
    fn init(&mut self, init_json: RString) -> RResult<(), RString> {
        // parse {"plugin_config": ..., "sources": {"<data name>": <config>, ...},
        //        "vus": ..., "vu_offset": ...}
        ROk(())
    }

    /// Called concurrently from VU worker threads, once per request.
    fn next_row(&self, ctx_json: RString) -> RResult<RString, RString> {
        // parse {"source","vu","iteration","seq","scenario","request"?,"ts_ms"}
        // return {"row": {"col": "value", ...}} or {"exhausted": true}
        ROk(RString::from(r#"{"row":{"col":"value"}}"#))
    }
}

extern "C" fn make_data_source() -> FfiDataSourceBox {
    FfiDataSource_TO::from_value(MySource::default(), abi_stable::erased_types::TD_Opaque)
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RNone,
        make_protocol: RNone,
        make_service: RNone,
        make_data_source: RSome(make_data_source),
    }
}
```

Key facts that shape the design:

- `next_row` is on the **request hot path** and is called **concurrently**
  from VU worker threads (`FfiDataSource: Send + Sync`) — every request
  preparation that references a `type: plugin` source calls it once. Keep it
  fast: do CPU work inline (an Ed25519 signature at ~25–50µs is fine), but
  never block on network/disk I/O here.
- `init` is your one-time setup hook, called once before any VU starts. Load
  keys and static configuration here — not per `next_row` call.
- A `kind = "service"` plugin can provide `make_service`, `make_data_source`,
  or both. A plugin that's data-source-only (like `tx-signer`) sets
  `make_service: RNone`; the host only errors if a service-kind plugin
  provides **neither** capability.
- The manifest may declare `capabilities = ["data_source"]` under
  `[plugin]` — informational only. The host's authoritative check is
  whether `make_data_source` is present in the loaded module.

### Reacting to request results

A data source can also see what the server did with the rows it produced.
Override `FfiDataSource::on_result`, and return `true` from `wants_results` so
the host calls it:

```rust
impl FfiDataSource for MySource {
    // name / init / next_row as above

    /// Called concurrently, after the response.
    fn on_result(&self, result_json: RString) {
        // {"source","vu","iteration","seq","scenario","request"?,
        //  "row": {...}, "response": {"status","body","headers",...}}
    }

    fn wants_results(&self) -> bool {
        true
    }
}
```

The host asks `wants_results` once, after `init`, and only then records rows
and serialises responses for this plugin — so a data source that doesn't
override it pays nothing per request. Return `true` only when you override
`on_result`.

Both methods have default bodies (`false` and a no-op), so a data source that
doesn't care about results needs no change. They sit at the end of the trait
for a reason: a plugin compiled before they existed has no vtable slots for
them, and abi_stable runs the defaults instead. (abi_stable's layout check
rejects a library declaring fewer methods than the host, so the loader skips
that check when missing trailing fields are the *only* difference. Any other
layout mismatch still fails the load.)

The row is echoed back in the payload, so a source usually needs no
pending-request bookkeeping — read what you generated straight off
`result_json`. State that `next_row` and `on_result` share lives on the data
source itself; `plugins/examples/native-nonce-feeder` shows the pattern with
sharded per-account nonces that only advance when the submission succeeded.

`on_result` runs before the `afterRequest` hook, so a row the hook pulls is never
misreported as belonging to the finished request. The payload's `request` is
the request's name as `next_row` saw it (unrendered, e.g. `submit ${vars.kind}`),
so the two can be matched.

Only declarative `request:` steps report results. A row a JS step pulls and
sends with `http.*` gets no `on_result`, and is dropped when the iteration
ends.

Results are best-effort by design: `on_result` returns nothing and a request
cancelled mid-flight reports nothing at all. A plugin must tolerate a row whose
result never arrives, and must not panic — it runs on a VU worker thread.

### Init / row JSON contracts

```jsonc
// init_json (host -> plugin, once before VUs start)
{
  "plugin_config": { "seed": 42 },              // merged [config] + PluginRef.config
  "sources": { "signed_tx": { "chain_id": "testnet-1" } }, // one entry per data.<name> backed by this plugin
  "vus": 250,                                   // most VU ids this instance allocates
  "vu_offset": 500                              // sum of "vus" over the agents before this one
}

// ctx_json (host -> plugin, per next_row call)
{
  "source": "signed_tx", "vu": 3, "iteration": 0, "seq": 5,
  "scenario": "submit", "request": "submit tx", "ts_ms": 1700000000000
}

// row response (plugin -> host)
{ "row": { "tx_b64": "...", "nonce": "3:5" } }
// or, when the generator is exhausted (retires the VU, like `on_eof: stop`):
{ "exhausted": true }
```

`seq` is a monotonic counter per (VU, source) — combine it with `vu` for
lock-free uniqueness across VUs with no shared state on your side. `request`
is the name of the request currently being prepared, or absent when the row
is fetched outside request preparation (e.g. from a JS step). Row values
cross as JSON scalars; strings map straight through, and a `bytes` protobuf
field expects base64 (`prost-reflect` decodes it automatically).

`vu` is local to one instance: in a distributed run every agent numbers its
VUs from 1. Add `vu_offset` from `init_json` to get an id that is unique across
the whole fleet, in `vu_offset + 1 ..= vu_offset + vus`. Every agent computes
the same split from the same plan, so the ranges tile without overlap. `vus`
counts the most VUs this agent can start (peak for ramping executors, the
larger of `pre_allocated_vus` and `max_vus` for arrival-rate ones), not the
number running right now.

A host that predates these fields omits both. If your plugin needs a
fleet-unique id to be correct, require `vu_offset` so init fails on such a
host, as `native-nonce-feeder` does: defaulting it to 0 would put every agent's
VUs on the same ids without any error.

### Testing

- Unit-test `init`/`next_row` by building the JSON payloads above and
  asserting on the response — no host needed.
- Load the built artifact with `loadr_plugin_api::NativePlugin::load(...)`
  and drive it through `make_data_source(config)` — see
  `crates/loadr-plugin-api/tests/native_plugins.rs` for the reference tests
  (init/next_row roundtrip, signature verification, exhaustion, concurrent
  calls from several threads).
- End-to-end, reference the built artifact from a plan's `plugins:` entry
  and a `data.<name>: { type: plugin, source: ... }` block, then run it
  through the real `loadr` binary.

## Native job plugins

A **job** is finite work a plugin runs on its own threads: generating a data
file, seeding a store, exporting a dataset. loadr doesn't schedule the work.
It starts the job, polls its progress once per snapshot interval for the
console and the web UI, and stops it. Threads, batching and I/O are entirely
up to the plugin. `plugins/examples/native-file-gen` is the reference
implementation.

A plan whose only workload is jobs needs no `scenarios:`. The run lasts until
every job is done:

```yaml
plugins:
  - name: file-gen
    config: { path: out.jsonl, rows: 10000000, threads: 8 }
```

### The ABI

A job is a `kind = "service"` plugin whose `FfiService` overrides two
defaulted methods:

```rust
impl FfiService for MyJob {
    fn name(&self) -> RString { RString::from("my-job") }

    /// Spawn the work and return promptly. `config_json` is the merged
    /// manifest `[config]` + plan `plugins:` config.
    fn start(&mut self, config_json: RString) -> RResult<RString, RString> { /* ... */ }

    /// Cancel if still running, join the threads, flush. Called once, on
    /// finish, failure, or a user stop.
    fn stop(&mut self) { /* ... */ }

    fn is_job(&self) -> bool { true }

    /// Polled about once a second from one thread. Read atomics; don't block.
    fn progress(&self) -> RString { /* JSON below */ }
}
```

`progress` returns:

```jsonc
{
  "state": "running",          // "running" | "finished" | "failed"
  "done": 7340000,             // units completed so far
  "total": 10000000,           // optional: enables % and ETA
  "unit": "rows",              // optional, for display
  "error": null,               // why, when "failed"
  "metrics": {                 // optional numbers, each a `job_<key>` gauge
    "bytes_written": 1.2e9
  }
}
```

- `finished` ends the job. The host calls `stop`, then polls once more for
  the final numbers.
- `failed` (or a failed `start`, or progress JSON that doesn't parse) aborts
  the run with `job \`<name>\` failed: <error>`.
- A graceful stop (Ctrl-C, or **Stop** in the web UI) calls `stop` while the
  job is still running. The job ends as `stopped`.
- The host emits `job_done`, `job_progress` (0–1, with a `total`) and
  `job_<key>` for each `metrics` entry, all tagged `job=<name>`. Thresholds
  and outputs see them like any other gauge.

The web UI shows each job as a progress card (bar, rate, ETA and metrics)
plus a throughput chart. The request panels are hidden when the run only
drives jobs. The end-of-run summary (console, `--summary-export`, web UI)
records each job's final state.

Jobs run in `loadr run` only. Distributed agents ignore them: splitting a
plugin's own work across agents isn't plumbed yet.

### Testing

- Drive `start` / `progress` / `stop` directly in unit tests. The job doesn't
  need a host. See the tests in `native-file-gen`.
- End-to-end, reference the built artifact from a plan with no `scenarios:`
  and run it through the real `loadr` binary. Add `--ui` to watch it.
