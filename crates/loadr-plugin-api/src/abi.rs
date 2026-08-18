//! Stable FFI surface for native dynamic-library plugins, built on
//! [`abi_stable`].
//!
//! Design note: all rich data (samples, snapshots, summaries, requests,
//! responses, configs) crosses the FFI boundary as JSON in [`RString`]s. This
//! keeps the ABI tiny and forward-compatible — adding a field to a payload is
//! never an ABI break. The marshalling cost is irrelevant at plugin-boundary
//! call rates (per flush batch / per snapshot / per request) compared to the
//! cost of an unstable `repr(Rust)` boundary.

// `#[sabi_trait]` expands to impls inside functions (abi_stable 0.11 predates
// the `non_local_definitions` lint); harmless here.
#![allow(non_local_definitions)]

use abi_stable::{
    declare_root_module_statics,
    library::RootModule,
    package_version_strings, sabi_trait,
    sabi_types::VersionStrings,
    std_types::{RBox, ROption, RResult, RString},
    StableAbi,
};

/// Bumped whenever the FFI surface changes incompatibly. Checked on load.
pub const LOADR_PLUGIN_ABI_VERSION: u32 = 1;

/// A metrics output plugin. JSON payloads mirror `loadr_core` types:
/// `on_samples` receives `Vec<Sample>`, `on_snapshot` a `Snapshot`,
/// `finish` a `Summary`.
#[sabi_trait]
pub trait FfiOutput: Send {
    fn name(&self) -> RString;

    /// Called once before the run with the plugin configuration (JSON object).
    fn start(&mut self, config_json: RString) -> RResult<(), RString>;

    /// Called per flush batch with a JSON array of samples.
    fn on_samples(&mut self, samples_json: RString);

    /// Called roughly once per second with a JSON snapshot.
    fn on_snapshot(&mut self, snapshot_json: RString);

    /// Called once at the end of the run with the JSON summary.
    fn finish(&mut self, summary_json: RString);
}

/// A protocol plugin. `execute` receives a JSON-encoded request (see
/// `loadr_plugin_api::native::FfiRequest`) and returns a JSON-encoded
/// response (`loadr_plugin_api::native::FfiResponse`).
#[sabi_trait]
pub trait FfiProtocol: Send + Sync {
    fn name(&self) -> RString;

    /// Execute one request. Must not panic; report failures via the
    /// response `error` field.
    fn execute(&self, request_json: RString) -> RString;
}

/// A background service plugin with an explicit lifecycle.
#[sabi_trait]
pub trait FfiService: Send {
    fn name(&self) -> RString;

    /// Start the service; returns a plugin-defined string (e.g. bound addr).
    fn start(&mut self, config_json: RString) -> RResult<RString, RString>;

    /// Stop the service (idempotent).
    fn stop(&mut self);
}

/// An on-demand data source (`data.<name>.type: plugin`). `next_row` is on
/// the request hot path and is called concurrently from VU threads.
#[sabi_trait]
pub trait FfiDataSource: Send + Sync {
    fn name(&self) -> RString;

    /// Called once before VUs start. `init_json`:
    /// `{"plugin_config": <merged [config] + PluginRef.config>,
    ///   "sources": {"<data name>": <data.<name>.config>, ...}}`
    fn init(&mut self, init_json: RString) -> RResult<(), RString>;

    /// `ctx_json`: `{"source","vu","iteration","seq","scenario","request"?,"ts_ms"}`.
    /// Returns `{"row": {"col": <scalar>, ...}}` or `{"exhausted": true}`.
    fn next_row(&self, ctx_json: RString) -> RResult<RString, RString>;
}

/// Boxed trait objects as they cross the FFI boundary.
pub type FfiOutputBox = FfiOutput_TO<'static, RBox<()>>;
pub type FfiProtocolBox = FfiProtocol_TO<'static, RBox<()>>;
pub type FfiServiceBox = FfiService_TO<'static, RBox<()>>;
pub type FfiDataSourceBox = FfiDataSource_TO<'static, RBox<()>>;

/// The root module every native loadr plugin exports.
///
/// A plugin provides at least one constructor; the host inspects `info()`
/// (a JSON-encoded [`crate::PluginInfo`]) to know what it is looking at.
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = PluginModRef)))]
#[sabi(missing_field(panic))]
pub struct PluginMod {
    /// Must equal [`LOADR_PLUGIN_ABI_VERSION`].
    pub abi_version: u32,
    /// JSON-encoded [`crate::PluginInfo`].
    pub info: extern "C" fn() -> RString,
    pub make_output: ROption<extern "C" fn() -> FfiOutputBox>,
    pub make_protocol: ROption<extern "C" fn() -> FfiProtocolBox>,
    #[sabi(last_prefix_field)]
    pub make_service: ROption<extern "C" fn() -> FfiServiceBox>,
    /// Suffix field: plugins compiled before it existed still load; the
    /// accessor then returns `RNone`. Not an ABI break — version stays 1.
    #[sabi(missing_field(default))]
    pub make_data_source: ROption<extern "C" fn() -> FfiDataSourceBox>,
}

impl RootModule for PluginModRef {
    declare_root_module_statics! {PluginModRef}
    const BASE_NAME: &'static str = "loadr_plugin";
    const NAME: &'static str = "loadr_plugin";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();
}

/// Export a native loadr plugin's root module.
///
/// ```ignore
/// use abi_stable::std_types::{ROption::RNone, ROption::RSome, RString};
/// use loadr_plugin_api::abi::{PluginMod, LOADR_PLUGIN_ABI_VERSION};
///
/// extern "C" fn info() -> RString { /* PluginInfo as JSON */ }
/// extern "C" fn make_output() -> loadr_plugin_api::abi::FfiOutputBox { /* ... */ }
///
/// loadr_plugin_api::export_loadr_plugin! {
///     PluginMod {
///         abi_version: LOADR_PLUGIN_ABI_VERSION,
///         info,
///         make_output: RSome(make_output),
///         make_protocol: RNone,
///         make_service: RNone,
///         make_data_source: RNone,
///     }
/// }
/// ```
#[macro_export]
macro_rules! export_loadr_plugin {
    ($module:expr) => {
        #[$crate::abi_stable::export_root_module]
        pub fn loadr_plugin_root_module() -> $crate::abi::PluginModRef {
            use $crate::abi_stable::prefix_type::PrefixTypeTrait;
            let module: $crate::abi::PluginMod = $module;
            module.leak_into_prefix()
        }
    };
}
