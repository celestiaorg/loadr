//! Stable native feeder ABI and local feeder registry for loadr.

pub mod abi;
pub mod error;
pub mod install;
pub mod manifest;
pub mod native;
pub mod registry;

pub use abi_stable;

pub use error::PluginError;
pub use install::remove;
pub use manifest::{merge_config, PluginManifest};
pub use native::{NativeDataSourceAdapter, NativeFeeder};
pub use registry::{default_plugins_dir, PluginRegistry, DISABLED_MARKER};

/// Identity reported by a native feeder library.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FeederInfo {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
}
