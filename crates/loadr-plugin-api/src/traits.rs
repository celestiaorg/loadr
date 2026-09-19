//! Core-facing plugin traits. The engine talks to plugins exclusively through
//! these object-safe traits; the WASM and native machinery lives behind them.

use loadr_core::ProtocolResponse;

use crate::error::PluginError;

/// A plugin-provided value extractor (e.g. boundary/regex/jsonpath variants).
///
/// `config` is the plugin-specific configuration object (manifest defaults
/// merged with the per-use overrides). Errors are strings so plugin failures
/// surface as extraction misses with a reason, never as engine crashes.
pub trait PluginExtractor: Send + Sync {
    fn name(&self) -> &str;

    /// Extract a value from a response. `Ok(None)` means "no match".
    fn extract(
        &self,
        response: &ProtocolResponse,
        config: &serde_json::Value,
    ) -> Result<Option<String>, String>;
}

/// A plugin-provided assertion/check over a response.
pub trait PluginAssertion: Send + Sync {
    fn name(&self) -> &str;

    /// Check a response: `(pass, detail)`.
    fn check(
        &self,
        response: &ProtocolResponse,
        config: &serde_json::Value,
    ) -> Result<(bool, String), String>;
}

/// A plugin-provided background service (e.g. a sidecar listener) with an
/// explicit start/stop lifecycle.
pub trait ServicePlugin: Send {
    fn name(&self) -> &str;

    /// Start the service. Returns a plugin-defined string (e.g. a bound
    /// address) on success.
    fn start(&mut self, config: &serde_json::Value) -> Result<String, PluginError>;

    /// Stop the service. Must be idempotent.
    fn stop(&mut self);

    /// Whether the service is a job (see `FfiService::is_job`). Default: no.
    fn is_job(&self) -> bool {
        false
    }

    /// A job's progress JSON (see `FfiService::progress`).
    fn progress(&self) -> String {
        String::new()
    }
}
