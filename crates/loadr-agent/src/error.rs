//! Error type shared by the agent and the controller.

use thiserror::Error;

/// Errors from distributed coordination.
#[derive(Debug, Error)]
pub enum AgentError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("tls error: {0}")]
    Tls(String),
    #[error("i/o error on {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("security violation: {0}")]
    Security(String),
    #[error("no connected agents match the request")]
    NoAgents,
    #[error("unknown run `{0}`")]
    UnknownRun(String),
    #[error("engine error: {0}")]
    Engine(String),
    #[error("control command failed: {0}")]
    Control(String),
}
