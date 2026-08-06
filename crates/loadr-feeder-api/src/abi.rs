//! Stable Rust-to-Rust ABI for native on-demand feeder plugins.

#![allow(non_local_definitions)]

use abi_stable::{
    declare_root_module_statics,
    library::RootModule,
    package_version_strings, sabi_trait,
    sabi_types::VersionStrings,
    std_types::{RBox, RResult, RString},
    StableAbi,
};

/// Voyager's deliberately breaking, feeder-only ABI.
pub const LOADR_FEEDER_ABI_VERSION: u32 = 2;

/// An on-demand data source used by `data.<name>.type: plugin`.
#[sabi_trait]
pub trait FfiDataSource: Send + Sync {
    fn name(&self) -> RString;

    /// Called once before VUs start.
    ///
    /// The JSON payload is
    /// `{"plugin_config": ..., "sources": {"<data name>": ...}}`.
    fn init(&mut self, init_json: RString) -> RResult<(), RString>;

    /// Generate one row for the current request.
    ///
    /// Returns `{"row": {...}}` or `{"exhausted": true}`.
    fn next_row(&self, context_json: RString) -> RResult<RString, RString>;
}

pub type FfiDataSourceBox = FfiDataSource_TO<'static, RBox<()>>;

/// Root module exported by every voyager feeder library.
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = FeederModRef)))]
#[sabi(missing_field(panic))]
pub struct FeederMod {
    pub abi_version: u32,
    /// JSON-encoded [`crate::FeederInfo`].
    pub info: extern "C" fn() -> RString,
    #[sabi(last_prefix_field)]
    pub make_data_source: extern "C" fn() -> FfiDataSourceBox,
}

impl RootModule for FeederModRef {
    declare_root_module_statics! {FeederModRef}
    const BASE_NAME: &'static str = "loadr_feeder";
    const NAME: &'static str = "loadr_feeder";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();
}

/// Export a native feeder root module.
#[macro_export]
macro_rules! export_loadr_feeder {
    ($module:expr) => {
        #[$crate::abi_stable::export_root_module]
        pub fn loadr_feeder_root_module() -> $crate::abi::FeederModRef {
            use $crate::abi_stable::prefix_type::PrefixTypeTrait;
            let module: $crate::abi::FeederMod = $module;
            module.leak_into_prefix()
        }
    };
}
