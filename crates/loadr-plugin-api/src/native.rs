//! Loader and adapters for native dynamic-library plugins.
//!
//! [`NativePlugin::load`] uses `abi_stable`'s library header machinery, which
//! validates the plugin's type layout against the host's before any call is
//! made, then checks our own [`LOADR_PLUGIN_ABI_VERSION`]. Loaded libraries
//! are intentionally never unloaded (abi_stable leaks them), so handed-out
//! trait objects stay valid for the process lifetime.

use std::path::{Path, PathBuf};

use abi_stable::library::lib_header_from_path;
use abi_stable::std_types::{ROption, RResult, RStr, RString};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use loadr_core::data::{DataSourcePlugin, PluginRowCtx, PluginRowResult, Row};
use loadr_core::error::{EngineError, ProtocolError};
use loadr_core::metrics::Sample;
use loadr_core::{
    Output, PreparedRequest, ProtocolHandler, ProtocolResponse, Snapshot, Summary, VuContext,
};

use crate::abi::{
    FfiDataSourceBox, FfiOutputBox, FfiProtocolBox, FfiRowCtx, FfiServiceBox, FfiValue,
    PluginModRef, LOADR_PLUGIN_ABI_VERSION,
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
            ROption::RSome(ctor) => Ok(NativeProtocolAdapter::new(ctor(), config)),
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
    config: serde_json::Value,
    inner: FfiProtocolBox,
}

impl std::fmt::Debug for NativeProtocolAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeProtocolAdapter")
            .field("name", &self.name)
            .finish()
    }
}

impl NativeProtocolAdapter {
    fn new(inner: FfiProtocolBox, config: serde_json::Value) -> Self {
        let name = inner.name().into_string();
        NativeProtocolAdapter {
            name,
            config,
            inner,
        }
    }
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
        let ffi_request = FfiRequest {
            name: request.name.clone(),
            method: request.method.clone(),
            url: request.url.clone(),
            headers: request.headers.clone(),
            body_b64: base64::engine::general_purpose::STANDARD.encode(&request.body),
            timeout_ms: request.timeout.as_millis() as u64,
            options: request.options.plugin.clone(),
            config: self.config.clone(),
        };
        let request_json = serde_json::to_string(&ffi_request)
            .map_err(|e| ProtocolError::InvalidRequest(format!("cannot encode request: {e}")))?;
        let bytes_sent = request.body.len() as u64;
        let response_json = self.inner.execute(RString::from(request_json));
        let ffi: FfiResponse = serde_json::from_str(response_json.as_str()).map_err(|e| {
            ProtocolError::Transport(format!(
                "plugin `{}` returned invalid response JSON: {e}",
                self.name
            ))
        })?;
        let body = base64::engine::general_purpose::STANDARD
            .decode(&ffi.body_b64)
            .map_err(|e| {
                ProtocolError::Transport(format!(
                    "plugin `{}` returned invalid body base64: {e}",
                    self.name
                ))
            })?;
        let mut response = ProtocolResponse {
            status: ffi.status,
            status_text: ffi.status_text,
            headers: ffi.headers,
            bytes_sent,
            bytes_received: body.len() as u64,
            body: Bytes::from(body),
            protocol_version: self.name.clone(),
            error: ffi.error,
            url: request.url.clone(),
            extras: ffi.extras,
            ..Default::default()
        };
        response.timings.duration_ms = ffi.duration_ms;
        response.timings.waiting_ms = ffi.duration_ms;
        Ok(response)
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
            source: RStr::from(ctx.source),
            vu: ctx.vu,
            iteration: ctx.iteration,
            seq: ctx.seq,
            scenario: RStr::from(ctx.scenario),
            request: ctx.request.map(RStr::from).into(),
            ts_ms: ctx.ts_ms,
        };
        match self.inner.next_row(ffi_ctx) {
            RResult::ROk(ROption::RSome(fields)) => {
                let mut row = Row::new();
                for field in fields {
                    let name = field.name.into_string();
                    match field.value {
                        FfiValue::String(value) => {
                            row.insert(name, value.into_string());
                        }
                        FfiValue::Bytes(value) => {
                            row.insert_bytes(name, Bytes::from_owner(value));
                        }
                    }
                }
                Ok(PluginRowResult::Row(row))
            }
            RResult::ROk(ROption::RNone) => Ok(PluginRowResult::Exhausted),
            RResult::RErr(error) => Err(error.into_string()),
        }
    }
}
