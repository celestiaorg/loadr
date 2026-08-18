//! Loader and adapters for native dynamic-library plugins.
//!
//! [`NativePlugin::load`] uses `abi_stable`'s library header machinery, which
//! validates the plugin's type layout against the host's before any call is
//! made, then checks our own [`LOADR_PLUGIN_ABI_VERSION`]. Loaded libraries
//! are intentionally never unloaded (abi_stable leaks them), so handed-out
//! trait objects stay valid for the process lifetime.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use abi_stable::library::lib_header_from_path;
use abi_stable::std_types::{ROption, RResult, RString};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use loadr_core::data::{DataSourcePlugin, PluginRowCtx, PluginRowResult, Row};
use loadr_core::error::{EngineError, ProtocolError};
use loadr_core::metrics::Sample;
use loadr_core::{
    Output, PreparedRequest, ProtocolHandler, ProtocolResponse, Snapshot, Summary, VuContext,
};

use crate::abi::{
    FfiDataSourceBox, FfiOutputBox, FfiProtocolBox, FfiServiceBox, PluginModRef,
    LOADR_PLUGIN_ABI_VERSION,
};
use crate::error::PluginError;
use crate::traits::ServicePlugin;
use crate::PluginInfo;

/// JSON request payload handed to [`crate::abi::FfiProtocol::execute`].
#[derive(Debug, Serialize, Deserialize)]
pub struct FfiRequest {
    pub name: String,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    /// Request body, base64 encoded.
    pub body_b64: String,
    pub timeout_ms: u64,
    /// `options.plugin` from the prepared request, passed through verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<serde_json::Value>,
    /// Plugin-level configuration (manifest defaults + per-run overrides).
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub config: serde_json::Value,
}

/// JSON response payload returned by [`crate::abi::FfiProtocol::execute`].
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FfiResponse {
    #[serde(default)]
    pub status: i64,
    #[serde(default)]
    pub status_text: String,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// Response body, base64 encoded.
    #[serde(default)]
    pub body_b64: String,
    #[serde(default)]
    pub duration_ms: f64,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub extras: serde_json::Value,
}

/// A loaded native plugin library.
pub struct NativePlugin {
    module: PluginModRef,
    info: PluginInfo,
    path: PathBuf,
}

impl std::fmt::Debug for NativePlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativePlugin")
            .field("info", &self.info)
            .field("path", &self.path)
            .finish()
    }
}

impl NativePlugin {
    /// Load a plugin dynamic library and validate its ABI.
    pub fn load(path: &Path) -> Result<NativePlugin, PluginError> {
        // Deliberately NOT `RootModule::load_from_file`: that caches the root
        // module in a per-*type* static, so a second, different plugin
        // library would silently resolve to the first one loaded. The header
        // path validates version + layout the same way, per library.
        let header = lib_header_from_path(path).map_err(|e| PluginError::Load {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;
        let module: PluginModRef =
            header
                .init_root_module::<PluginModRef>()
                .map_err(|e| PluginError::Load {
                    path: path.display().to_string(),
                    message: e.to_string(),
                })?;
        let version = module.abi_version();
        if version != LOADR_PLUGIN_ABI_VERSION {
            return Err(PluginError::AbiVersion {
                host: LOADR_PLUGIN_ABI_VERSION,
                plugin: version,
            });
        }
        let info_json = module.info()();
        let info: PluginInfo =
            serde_json::from_str(info_json.as_str()).map_err(|e| PluginError::Load {
                path: path.display().to_string(),
                message: format!("invalid plugin info JSON: {e}"),
            })?;
        tracing::debug!(name = %info.name, kind = %info.kind, path = %path.display(), "loaded native plugin");
        Ok(NativePlugin {
            module,
            info,
            path: path.to_path_buf(),
        })
    }

    pub fn info(&self) -> &PluginInfo {
        &self.info
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Instantiate the plugin's output, wrapped as a `loadr_core::Output`.
    pub fn make_output(
        &self,
        config: serde_json::Value,
    ) -> Result<NativeOutputAdapter, PluginError> {
        match self.module.make_output() {
            ROption::RSome(ctor) => Ok(NativeOutputAdapter::new(ctor(), config)),
            ROption::RNone => Err(self.missing("output")),
        }
    }

    /// Instantiate the plugin's protocol handler.
    pub fn make_protocol(
        &self,
        config: serde_json::Value,
    ) -> Result<NativeProtocolAdapter, PluginError> {
        match self.module.make_protocol() {
            ROption::RSome(ctor) => NativeProtocolAdapter::new(ctor(), config),
            ROption::RNone => Err(self.missing("protocol")),
        }
    }

    /// Instantiate the plugin's service.
    pub fn make_service(&self) -> Result<NativeServiceAdapter, PluginError> {
        match self.module.make_service() {
            ROption::RSome(ctor) => Ok(NativeServiceAdapter::new(ctor())),
            ROption::RNone => Err(self.missing("service")),
        }
    }

    /// Instantiate the plugin's service, if it provides one. Unlike
    /// [`NativePlugin::make_service`], `None` (not an error) means the
    /// plugin simply doesn't implement the service lifecycle — used
    /// alongside [`NativePlugin::make_data_source`] so a service-kind
    /// plugin can be data-source-only.
    pub fn maybe_service(&self) -> Option<NativeServiceAdapter> {
        match self.module.make_service() {
            ROption::RSome(ctor) => Some(NativeServiceAdapter::new(ctor())),
            ROption::RNone => None,
        }
    }

    /// Instantiate the plugin's `data_source` capability, if it provides one.
    pub fn make_data_source(&self, config: serde_json::Value) -> Option<NativeDataSourceAdapter> {
        match self.module.make_data_source() {
            ROption::RSome(ctor) => Some(NativeDataSourceAdapter::new(ctor(), config)),
            ROption::RNone => None,
        }
    }

    fn missing(&self, expected: &str) -> PluginError {
        PluginError::KindMismatch {
            name: self.info.name.clone(),
            expected: expected.to_string(),
            actual: self.info.kind.clone(),
        }
    }
}

/// Bridges an FFI output plugin to `loadr_core::Output`.
pub struct NativeOutputAdapter {
    name: String,
    config: serde_json::Value,
    inner: FfiOutputBox,
}

impl std::fmt::Debug for NativeOutputAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeOutputAdapter")
            .field("name", &self.name)
            .finish()
    }
}

impl NativeOutputAdapter {
    fn new(inner: FfiOutputBox, config: serde_json::Value) -> Self {
        let name = inner.name().into_string();
        NativeOutputAdapter {
            name,
            config,
            inner,
        }
    }
}

#[async_trait]
impl Output for NativeOutputAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self) -> Result<(), EngineError> {
        match self.inner.start(RString::from(self.config.to_string())) {
            RResult::ROk(()) => Ok(()),
            RResult::RErr(e) => Err(EngineError::Other(format!(
                "plugin output `{}` failed to start: {e}",
                self.name
            ))),
        }
    }

    async fn on_samples(&mut self, samples: &[Sample]) {
        match serde_json::to_string(samples) {
            Ok(json) => self.inner.on_samples(RString::from(json)),
            Err(e) => tracing::warn!(output = %self.name, "cannot serialize samples: {e}"),
        }
    }

    async fn on_snapshot(&mut self, snapshot: &Snapshot) {
        match serde_json::to_string(snapshot) {
            Ok(json) => self.inner.on_snapshot(RString::from(json)),
            Err(e) => tracing::warn!(output = %self.name, "cannot serialize snapshot: {e}"),
        }
    }

    async fn finish(&mut self, summary: &Summary) {
        match serde_json::to_string(summary) {
            Ok(json) => self.inner.finish(RString::from(json)),
            Err(e) => tracing::warn!(output = %self.name, "cannot serialize summary: {e}"),
        }
    }
}

/// Bridges an FFI protocol plugin to `loadr_core::ProtocolHandler`.
pub struct NativeProtocolAdapter {
    name: String,
    config: CachedProtocolConfig,
    inner: Arc<FfiProtocolBox>,
}

impl std::fmt::Debug for NativeProtocolAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeProtocolAdapter")
            .field("name", &self.name)
            .finish()
    }
}

impl NativeProtocolAdapter {
    fn new(inner: FfiProtocolBox, config: serde_json::Value) -> Result<Self, PluginError> {
        let name = inner.name().into_string();
        let config = cache_protocol_config(config)?;
        Ok(NativeProtocolAdapter {
            name,
            config,
            inner: Arc::new(inner),
        })
    }
}

pub(crate) type CachedProtocolConfig = Arc<RawValue>;

pub(crate) fn cache_protocol_config(
    config: serde_json::Value,
) -> Result<CachedProtocolConfig, PluginError> {
    let config = serde_json::value::to_raw_value(&config)
        .map_err(|e| PluginError::Other(format!("cannot encode native plugin config: {e}")))?;
    Ok(Arc::from(config))
}

/// Host-only ABI-v1 wire representation. Unlike the public [`FfiRequest`],
/// this borrows dynamic request values and embeds the cached config through
/// [`RawValue`] so serde does not traverse it again.
#[derive(Serialize)]
struct WireFfiRequest<'a> {
    name: &'a str,
    method: &'a str,
    url: &'a str,
    headers: &'a [(String, String)],
    body_b64: &'a str,
    timeout_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<&'a RawValue>,
}

impl<'a> WireFfiRequest<'a> {
    fn new(request: &'a OwnedFfiRequest, body_b64: &'a str, config: &'a RawValue) -> Self {
        WireFfiRequest {
            name: &request.name,
            method: &request.method,
            url: &request.url,
            headers: &request.headers,
            body_b64,
            timeout_ms: request.timeout_ms,
            options: request.options.as_ref(),
            config: (config.get() != "null").then_some(config),
        }
    }
}

pub(crate) fn ffi_request_to_string(
    request: &OwnedFfiRequest,
    config: &RawValue,
) -> Result<String, serde_json::Error> {
    let body_b64 = base64::engine::general_purpose::STANDARD.encode(&request.body);
    serde_json::to_string(&WireFfiRequest::new(request, &body_b64, config))
}

pub(crate) fn ffi_request_to_vec(
    request: &OwnedFfiRequest,
    config: &RawValue,
) -> Result<Vec<u8>, serde_json::Error> {
    let body_b64 = base64::engine::general_purpose::STANDARD.encode(&request.body);
    serde_json::to_vec(&WireFfiRequest::new(request, &body_b64, config))
}

pub(crate) struct OwnedFfiRequest {
    name: String,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Bytes,
    timeout_ms: u64,
    options: Option<serde_json::Value>,
}

impl OwnedFfiRequest {
    pub(crate) fn from_prepared(request: &PreparedRequest) -> Self {
        OwnedFfiRequest {
            name: request.name.clone(),
            method: request.method.clone(),
            url: request.url.clone(),
            headers: request.headers.clone(),
            body: request.body.clone(),
            timeout_ms: request.timeout.as_millis() as u64,
            options: request.options.plugin.clone(),
        }
    }

    pub(crate) fn into_response_metadata(self) -> (String, u64) {
        let bytes_sent = self.body.len() as u64;
        (self.url, bytes_sent)
    }
}

fn call_v1_protocol(
    plugin_name: &str,
    inner: &FfiProtocolBox,
    request: OwnedFfiRequest,
    config: CachedProtocolConfig,
) -> Result<ProtocolResponse, ProtocolError> {
    let request_json = ffi_request_to_string(&request, &config)
        .map_err(|e| ProtocolError::InvalidRequest(format!("cannot encode request: {e}")))?;
    let (url, bytes_sent) = request.into_response_metadata();
    let response_json = inner.execute(RString::from(request_json));
    let ffi = serde_json::from_str(response_json.as_str()).map_err(|e| {
        ProtocolError::Transport(format!(
            "plugin `{plugin_name}` returned invalid response JSON: {e}"
        ))
    })?;
    response_from_ffi(plugin_name, ffi, url, bytes_sent)
}

pub(crate) fn response_from_ffi(
    plugin_name: &str,
    ffi: FfiResponse,
    url: String,
    bytes_sent: u64,
) -> Result<ProtocolResponse, ProtocolError> {
    let body = base64::engine::general_purpose::STANDARD
        .decode(&ffi.body_b64)
        .map_err(|e| {
            ProtocolError::Transport(format!(
                "plugin `{plugin_name}` returned invalid body base64: {e}"
            ))
        })?;
    let mut response = ProtocolResponse {
        status: ffi.status,
        status_text: ffi.status_text,
        headers: ffi.headers,
        bytes_sent,
        bytes_received: body.len() as u64,
        body: Bytes::from(body),
        protocol_version: plugin_name.to_string(),
        error: ffi.error,
        url,
        extras: ffi.extras,
        ..Default::default()
    };
    response.timings.duration_ms = ffi.duration_ms;
    response.timings.waiting_ms = ffi.duration_ms;
    Ok(response)
}

#[async_trait]
impl ProtocolHandler for NativeProtocolAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let owned = OwnedFfiRequest::from_prepared(request);
        let plugin_name = self.name.clone();
        let inner = Arc::clone(&self.inner);
        let config = Arc::clone(&self.config);
        let call_plugin_name = plugin_name.clone();
        tokio::task::spawn_blocking(move || {
            call_v1_protocol(&call_plugin_name, &inner, owned, config)
        })
        .await
        .map_err(|e| {
            ProtocolError::Transport(format!("plugin `{plugin_name}` blocking task failed: {e}"))
        })?
    }
}

/// Bridges an FFI service plugin to the [`ServicePlugin`] trait.
pub struct NativeServiceAdapter {
    name: String,
    inner: FfiServiceBox,
}

impl std::fmt::Debug for NativeServiceAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeServiceAdapter")
            .field("name", &self.name)
            .finish()
    }
}

impl NativeServiceAdapter {
    fn new(inner: FfiServiceBox) -> Self {
        let name = inner.name().into_string();
        NativeServiceAdapter { name, inner }
    }
}

impl ServicePlugin for NativeServiceAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn start(&mut self, config: &serde_json::Value) -> Result<String, PluginError> {
        match self.inner.start(RString::from(config.to_string())) {
            RResult::ROk(addr) => Ok(addr.into_string()),
            RResult::RErr(e) => Err(PluginError::Call(format!(
                "service `{}` failed to start: {e}",
                self.name
            ))),
        }
    }

    fn stop(&mut self) {
        self.inner.stop();
    }
}

/// JSON payload handed to [`crate::abi::FfiDataSource::init`].
#[derive(Serialize)]
struct FfiDataSourceInit<'a> {
    plugin_config: &'a serde_json::Value,
    sources: &'a IndexMap<String, serde_json::Value>,
}

/// JSON payload handed to [`crate::abi::FfiDataSource::next_row`].
#[derive(Serialize)]
struct FfiRowCtx<'a> {
    source: &'a str,
    vu: u64,
    iteration: u64,
    seq: u64,
    scenario: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    request: Option<&'a str>,
    ts_ms: u64,
}

/// JSON response from [`crate::abi::FfiDataSource::next_row`].
#[derive(Default, Deserialize)]
struct FfiRowResponse {
    #[serde(default)]
    row: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    exhausted: bool,
}

/// Bridges an FFI data-source plugin to `loadr_core::data::DataSourcePlugin`.
pub struct NativeDataSourceAdapter {
    name: String,
    config: serde_json::Value,
    inner: FfiDataSourceBox,
}

impl std::fmt::Debug for NativeDataSourceAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeDataSourceAdapter")
            .field("name", &self.name)
            .finish()
    }
}

impl NativeDataSourceAdapter {
    fn new(inner: FfiDataSourceBox, config: serde_json::Value) -> Self {
        let name = inner.name().into_string();
        NativeDataSourceAdapter {
            name,
            config,
            inner,
        }
    }
}

impl DataSourcePlugin for NativeDataSourceAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn init(&mut self, source_configs: &IndexMap<String, serde_json::Value>) -> Result<(), String> {
        let payload = FfiDataSourceInit {
            plugin_config: &self.config,
            sources: source_configs,
        };
        let json =
            serde_json::to_string(&payload).map_err(|e| format!("cannot encode init: {e}"))?;
        match self.inner.init(RString::from(json)) {
            RResult::ROk(()) => Ok(()),
            RResult::RErr(e) => Err(e.into_string()),
        }
    }

    fn next_row(&self, ctx: &PluginRowCtx<'_>) -> Result<PluginRowResult, String> {
        let ffi_ctx = FfiRowCtx {
            source: ctx.source,
            vu: ctx.vu,
            iteration: ctx.iteration,
            seq: ctx.seq,
            scenario: ctx.scenario,
            request: ctx.request,
            ts_ms: ctx.ts_ms,
        };
        let json = serde_json::to_string(&ffi_ctx)
            .map_err(|e| format!("cannot encode row context: {e}"))?;
        let response_json = match self.inner.next_row(RString::from(json)) {
            RResult::ROk(s) => s,
            RResult::RErr(e) => return Err(e.into_string()),
        };
        let response: FfiRowResponse = serde_json::from_str(response_json.as_str())
            .map_err(|e| format!("invalid row JSON: {e}"))?;
        if response.exhausted {
            return Ok(PluginRowResult::Exhausted);
        }
        let row_obj = response
            .row
            .ok_or_else(|| "plugin returned neither `row` nor `exhausted`".to_string())?;
        let row: Row = row_obj
            .iter()
            .map(|(k, v)| (k.clone(), loadr_core::vu::json_to_string(v)))
            .collect();
        Ok(PluginRowResult::Row(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::hint::black_box;
    use std::time::{Duration, Instant};

    use loadr_core::metrics::{MetricRegistry, MetricsBus, Tags};
    use loadr_core::vu::RunContext;
    use loadr_core::RequestOptions;

    use crate::abi::{FfiProtocol, FfiProtocol_TO};

    struct SlowProtocol;

    impl FfiProtocol for SlowProtocol {
        fn name(&self) -> RString {
            RString::from("slow")
        }

        fn execute(&self, _request_json: RString) -> RString {
            std::thread::sleep(Duration::from_millis(300));
            RString::from(r#"{"status":200,"status_text":"OK","body_b64":"","duration_ms":300}"#)
        }
    }

    fn minimal_vu() -> VuContext {
        let (bus, _rx) = MetricsBus::new();
        let run = Arc::new(RunContext {
            variables: serde_json::Map::new(),
            secrets: HashMap::new(),
            env: HashMap::new(),
            data: Default::default(),
            registry: Arc::new(MetricRegistry::with_builtins()),
            base_dir: ".".into(),
            setup_data: parking_lot::RwLock::new(serde_json::Value::Null),
        });
        VuContext::new(1, Arc::from("test"), Arc::new(Tags::new()), bus, run, true)
    }

    fn request() -> PreparedRequest {
        PreparedRequest {
            name: "slow".into(),
            protocol: "slow".into(),
            method: "GET".into(),
            url: "slow://local".into(),
            headers: Vec::new(),
            body: Bytes::new(),
            timeout: Duration::from_secs(5),
            follow_redirects: false,
            max_redirects: 0,
            options: RequestOptions::default(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v1_protocol_call_does_not_block_core_runtime_timer() {
        let inner = FfiProtocol_TO::from_value(SlowProtocol, abi_stable::erased_types::TD_Opaque);
        let adapter = NativeProtocolAdapter::new(inner, serde_json::Value::Null)
            .expect("construct native protocol adapter");
        let mut vu = minimal_vu();
        let request = request();
        let started = std::time::Instant::now();
        let execute = adapter.execute(&mut vu, &request);
        tokio::pin!(execute);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(25)) => {
                assert!(
                    started.elapsed() < Duration::from_millis(200),
                    "blocking plugin call stalled the core runtime for {:?}",
                    started.elapsed(),
                );
            }
            result = &mut execute => {
                panic!("slow plugin finished before timer fired: {result:?}");
            }
        }

        let response = execute.await.expect("slow plugin response");
        assert_eq!(response.status, 200);
    }

    fn prepared_request(options: Option<serde_json::Value>) -> PreparedRequest {
        PreparedRequest {
            name: "request \"name\"\n".into(),
            protocol: "fixture".into(),
            method: "M\\ETHOD".into(),
            url: "fixture://host/path?value=\"quoted\"&unicode=\u{2603}".into(),
            headers: vec![
                ("x-quote".into(), "\"\\\n\t".into()),
                ("x-unicode".into(), "caf\u{e9}".into()),
            ],
            body: Bytes::from_static(b"\0ffi body\xff"),
            timeout: Duration::from_millis(1_234),
            follow_redirects: false,
            max_redirects: 0,
            options: RequestOptions {
                plugin: options,
                ..Default::default()
            },
        }
    }

    fn public_ffi_request(request: OwnedFfiRequest, config: serde_json::Value) -> FfiRequest {
        FfiRequest {
            name: request.name,
            method: request.method,
            url: request.url,
            headers: request.headers,
            body_b64: base64::engine::general_purpose::STANDARD.encode(&request.body),
            timeout_ms: request.timeout_ms,
            options: request.options,
            config,
        }
    }

    #[test]
    fn cached_wire_request_matches_public_ffi_request_exactly() {
        let cases = [
            ("null", serde_json::Value::Null),
            ("empty object", serde_json::json!({})),
            (
                "nested object",
                serde_json::json!({
                    "outer": {
                        "array": [1, true, null, "quote \" slash \\ line\n"],
                        "object": {"key": "value"}
                    }
                }),
            ),
            (
                "array",
                serde_json::json!([1, "two", false, {"nested": null}]),
            ),
            (
                "escaped string",
                serde_json::json!("quotes \" backslash \\ newline\n tab\t control\u{0001}"),
            ),
            ("boolean", serde_json::json!(true)),
            ("integer", serde_json::json!(9_007_199_254_740_991_i64)),
            ("number", serde_json::json!(-12.5)),
        ];

        for (label, config) in cases {
            let request = prepared_request(Some(serde_json::json!({
                "dynamic": "value \"with\" escapes\\and\nlines"
            })));
            let public =
                public_ffi_request(OwnedFfiRequest::from_prepared(&request), config.clone());
            let expected_string = serde_json::to_string(&public).expect("serialize public request");
            let expected_value = serde_json::to_value(&public).expect("value of public request");
            let cached = cache_protocol_config(config.clone()).expect("cache config");
            let owned = OwnedFfiRequest::from_prepared(&request);

            let actual_string =
                ffi_request_to_string(&owned, &cached).expect("serialize cached request");
            let actual_vec = ffi_request_to_vec(&owned, &cached).expect("serialize cached bytes");
            let actual_value: serde_json::Value =
                serde_json::from_str(&actual_string).expect("parse cached request");

            assert_eq!(
                actual_string, expected_string,
                "string wire mismatch: {label}"
            );
            assert_eq!(
                actual_vec,
                expected_string.as_bytes(),
                "byte wire mismatch: {label}"
            );
            assert_eq!(actual_value, expected_value, "JSON value mismatch: {label}");

            let actual_config = actual_value
                .as_object()
                .expect("wire request object")
                .get("config");
            if config.is_null() {
                assert!(actual_config.is_none(), "null config must be omitted");
            } else {
                assert_eq!(
                    actual_config,
                    Some(&config),
                    "config JSON type changed: {label}"
                );
            }
        }
    }

    #[test]
    fn cached_wire_request_preserves_omission_and_scalar_types() {
        let request = prepared_request(None);
        let owned = OwnedFfiRequest::from_prepared(&request);
        let null = cache_protocol_config(serde_json::Value::Null).expect("cache null");
        let value: serde_json::Value = serde_json::from_str(
            &ffi_request_to_string(&owned, &null).expect("serialize null config"),
        )
        .expect("parse request");
        let object = value.as_object().expect("wire request object");
        assert!(!object.contains_key("options"));
        assert!(!object.contains_key("config"));

        for config in [
            serde_json::json!("scalar \"string\""),
            serde_json::json!(false),
            serde_json::json!(42),
        ] {
            let cached = cache_protocol_config(config.clone()).expect("cache scalar");
            let value: serde_json::Value = serde_json::from_str(
                &ffi_request_to_string(&owned, &cached).expect("serialize scalar config"),
            )
            .expect("parse request");
            assert_eq!(value.get("config"), Some(&config));
            assert_eq!(
                std::mem::discriminant(value.get("config").expect("embedded config")),
                std::mem::discriminant(&config),
                "cached scalar must retain its original JSON type",
            );
        }
    }

    fn config_with_serialized_size(bytes: usize) -> serde_json::Value {
        const EMPTY_OBJECT_BYTES: usize = 2;
        const OBJECT_OVERHEAD_BYTES: usize = r#"{"payload":""}"#.len();
        if bytes == EMPTY_OBJECT_BYTES {
            return serde_json::json!({});
        }
        assert!(bytes >= OBJECT_OVERHEAD_BYTES);
        serde_json::json!({"payload": "x".repeat(bytes - OBJECT_OVERHEAD_BYTES)})
    }

    /// Run with:
    /// `cargo test -p loadr-plugin-api --release cached_config_marshalling_benchmark -- --ignored --nocapture`
    #[test]
    #[ignore = "manual release-mode marshalling benchmark"]
    fn cached_config_marshalling_benchmark() {
        let request = prepared_request(Some(serde_json::json!({"dynamic": true})));
        let cases = [
            ("empty", 2, 100_000),
            ("1 KiB", 1_024, 20_000),
            ("64 KiB", 64 * 1_024, 1_000),
        ];

        for (label, config_bytes, iterations) in cases {
            let config = config_with_serialized_size(config_bytes);
            assert_eq!(
                serde_json::to_string(&config)
                    .expect("serialize benchmark config")
                    .len(),
                config_bytes,
            );
            let cached = cache_protocol_config(config.clone()).expect("cache benchmark config");

            let legacy_started = Instant::now();
            for _ in 0..iterations {
                let owned = OwnedFfiRequest::from_prepared(&request);
                let ffi_request = public_ffi_request(owned, config.clone());
                black_box(serde_json::to_string(&ffi_request).expect("legacy serialization"));
            }
            let legacy_elapsed = legacy_started.elapsed();

            let cached_started = Instant::now();
            for _ in 0..iterations {
                let owned = OwnedFfiRequest::from_prepared(&request);
                let config = Arc::clone(&cached);
                black_box(ffi_request_to_string(&owned, &config).expect("cached serialization"));
            }
            let cached_elapsed = cached_started.elapsed();

            let legacy_ns = legacy_elapsed.as_nanos() as f64 / iterations as f64;
            let cached_ns = cached_elapsed.as_nanos() as f64 / iterations as f64;
            println!(
                "{label}: config_bytes={config_bytes} iterations={iterations} \
                 legacy_ns_per_op={legacy_ns:.1} cached_ns_per_op={cached_ns:.1} \
                 speedup={:.2}x",
                legacy_ns / cached_ns,
            );
        }
    }
}
