# Native plugins

Native plugins are dynamic libraries (`.so`/`.dylib`/`.dll`) using
[`abi_stable`](https://docs.rs/abi_stable) for a **checked, versioned ABI**:
at load time the library's type layouts are validated against the host's, so
mismatched versions fail with a clear error instead of undefined behaviour.

Data crosses the boundary as JSON strings — a deliberate trade: marshalling
cost is negligible at plugin-call frequency, and it keeps the ABI surface
tiny and forward-compatible.

## The interface

`loadr-plugin-api` exposes `#[sabi_trait]` object types:

```rust
#[sabi_trait]
pub trait FfiOutput {
    fn name(&self) -> RString;
    fn start(&mut self, config_json: RString) -> RResult<(), RString>;
    fn on_samples(&mut self, samples_json: RString);
    fn on_snapshot(&mut self, snapshot_json: RString);
    fn finish(&mut self, summary_json: RString);
}

#[sabi_trait]
pub trait FfiProtocol {
    fn name(&self) -> RString;
    /// request JSON -> response JSON ({status, headers, body_base64, duration_ms, ...})
    fn execute(&self, request_json: RString) -> RString;
}

#[sabi_trait]
pub trait FfiService {
    fn name(&self) -> RString;
    fn start(&mut self, config_json: RString) -> RResult<RString, RString>;
    fn stop(&mut self);
}

#[sabi_trait]
pub trait FfiDataSource {
    fn name(&self) -> RString;
    fn init(&mut self, init_json: RString) -> RResult<(), RString>;
    /// row context JSON -> `{"row": {...}}` or `{"exhausted": true}`; called
    /// concurrently from VU threads, once per request.
    fn next_row(&self, ctx_json: RString) -> RResult<RString, RString>;
}
```

A plugin exports one **root module** advertising what it provides:

```rust
use loadr_plugin_api::export_loadr_plugin;

export_loadr_plugin! {
    info: my_info_fn,
    output: make_my_output,      // any subset of output / protocol / service
}
```

## Building

```toml
# Cargo.toml
[lib]
crate-type = ["cdylib"]
[dependencies]
loadr-plugin-api = "0.1"
abi_stable = "0.11"
```

```bash
cargo build --release
# package target/release/libmy_plugin.so with a plugin.toml (type = "native")
```

The shipped examples are the best reference:

- `plugins/examples/native-output` —
  an output plugin writing snapshot digests to a file;
- `plugins/examples/native-protocol` —
  an `echo-proto` protocol handler, including how `request.options.plugin`
  config reaches your `execute`;
- `plugins/examples/native-data-source` —
  `tx-signer`, an on-demand data source generating Ed25519-signed rows; see
  [Native data-source plugins](developing.md#native-data-source-plugins) for
  the full contract.

## Safety notes

Native plugins run **in-process with full privileges** — treat them like any
dependency you compile in. Prefer WASM for anything that doesn't strictly
need native capability. loadr refuses to load a plugin whose abi_stable
layout check fails, and `loadr plugin info` shows what a library exports
before you enable it.
