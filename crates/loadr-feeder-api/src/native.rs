//! Native feeder loader and core adapter.

use std::path::{Path, PathBuf};

use abi_stable::library::lib_header_from_path;
use abi_stable::std_types::{RResult, RString};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use loadr_core::data::{DataSourcePlugin, PluginRowCtx, PluginRowResult, Row};

use crate::abi::{FeederModRef, FfiDataSourceBox, LOADR_FEEDER_ABI_VERSION};
use crate::{FeederInfo, PluginError};

/// A loaded native feeder library.
pub struct NativeFeeder {
    module: FeederModRef,
    info: FeederInfo,
    path: PathBuf,
}

impl std::fmt::Debug for NativeFeeder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeFeeder")
            .field("info", &self.info)
            .field("path", &self.path)
            .finish()
    }
}

impl NativeFeeder {
    pub fn load(path: &Path) -> Result<Self, PluginError> {
        let header = lib_header_from_path(path).map_err(|error| PluginError::Load {
            path: path.display().to_string(),
            message: error.to_string(),
        })?;
        let module: FeederModRef =
            header
                .init_root_module::<FeederModRef>()
                .map_err(|error| PluginError::Load {
                    path: path.display().to_string(),
                    message: error.to_string(),
                })?;
        let plugin_version = module.abi_version();
        if plugin_version != LOADR_FEEDER_ABI_VERSION {
            return Err(PluginError::AbiVersion {
                host: LOADR_FEEDER_ABI_VERSION,
                plugin: plugin_version,
            });
        }
        let encoded = module.info()();
        let info = serde_json::from_str(encoded.as_str()).map_err(|error| PluginError::Load {
            path: path.display().to_string(),
            message: format!("invalid feeder info JSON: {error}"),
        })?;
        tracing::debug!(path = %path.display(), "loaded native feeder");
        Ok(Self {
            module,
            info,
            path: path.to_path_buf(),
        })
    }

    pub fn info(&self) -> &FeederInfo {
        &self.info
    }

    pub fn make_data_source(&self, config: serde_json::Value) -> NativeDataSourceAdapter {
        NativeDataSourceAdapter::new(self.module.make_data_source()(), config)
    }
}

#[derive(Serialize)]
struct FfiDataSourceInit<'a> {
    plugin_config: &'a serde_json::Value,
    sources: &'a IndexMap<String, serde_json::Value>,
}

#[derive(Serialize)]
struct FfiRowCtx<'a> {
    run_id: &'a str,
    instance_id: &'a str,
    partition_index: u64,
    partition_count: u64,
    source: &'a str,
    vu: u64,
    iteration: u64,
    seq: u64,
    scenario: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    request: Option<&'a str>,
    ts_ms: u64,
}

#[derive(Default, Deserialize)]
struct FfiRowResponse {
    #[serde(default)]
    row: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    exhausted: bool,
}

/// Bridges the FFI trait to the engine's data-source trait.
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
        Self {
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
        let encoded = serde_json::to_string(&payload)
            .map_err(|error| format!("cannot encode init: {error}"))?;
        match self.inner.init(RString::from(encoded)) {
            RResult::ROk(()) => Ok(()),
            RResult::RErr(error) => Err(error.into_string()),
        }
    }

    fn next_row(&self, context: &PluginRowCtx<'_>) -> Result<PluginRowResult, String> {
        let encoded = serde_json::to_string(&FfiRowCtx {
            run_id: context.run_id,
            instance_id: context.instance_id,
            partition_index: context.partition_index,
            partition_count: context.partition_count,
            source: context.source,
            vu: context.vu,
            iteration: context.iteration,
            seq: context.seq,
            scenario: context.scenario,
            request: context.request,
            ts_ms: context.ts_ms,
        })
        .map_err(|error| format!("cannot encode row context: {error}"))?;
        let response = match self.inner.next_row(RString::from(encoded)) {
            RResult::ROk(response) => response,
            RResult::RErr(error) => return Err(error.into_string()),
        };
        let response: FfiRowResponse = serde_json::from_str(response.as_str())
            .map_err(|error| format!("invalid row JSON: {error}"))?;
        if response.exhausted {
            return Ok(PluginRowResult::Exhausted);
        }
        let values = response
            .row
            .ok_or_else(|| "feeder returned neither `row` nor `exhausted`".to_string())?;
        let row: Row = values
            .iter()
            .map(|(name, value)| (name.clone(), loadr_core::vu::json_to_string(value)))
            .collect();
        Ok(PluginRowResult::Row(row))
    }
}
