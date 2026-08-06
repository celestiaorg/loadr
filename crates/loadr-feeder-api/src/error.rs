//! Feeder discovery, loading, and call errors.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PluginError {
    #[error("feeder i/o error on {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid feeder manifest at {path}: {message}")]
    Manifest { path: String, message: String },
    #[error("failed to load feeder from {path}: {message}")]
    Load { path: String, message: String },
    #[error("feeder ABI version mismatch: host expects {host}, feeder was built against {plugin}")]
    AbiVersion { host: u32, plugin: u32 },
    #[error("feeder `{name}` not found in {dir}")]
    NotFound { name: String, dir: String },
    #[error("feeder call failed: {0}")]
    Call(String),
    #[error("{0}")]
    Other(String),
}

impl PluginError {
    pub(crate) fn io(path: &std::path::Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.display().to_string(),
            source,
        }
    }
}
