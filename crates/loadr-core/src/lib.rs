//! # loadr-core
//!
//! The load generation engine behind [loadr](https://loadr.io): scenario
//! scheduling, all seven executors, virtual users, the metrics engine
//! (HDR-histogram trends with distributed merging), thresholds, checks,
//! extraction, data feeds, cookie jars, and the protocol/script/output
//! abstractions the other crates plug into.

pub mod aggregate;
pub mod build_info;
pub mod conditions;
pub mod cookies;
pub mod data;
pub mod engine;
pub mod error;
pub mod executor;
pub mod extract;
pub mod flow;
pub mod metrics;
pub mod output;
pub mod pacing;
pub mod protocol;
pub mod script;
pub mod summary;
pub mod thresholds;
pub mod vu;

pub use aggregate::{AggValues, Aggregator, MetricsDelta, SeriesSnapshot, Snapshot};
pub use data::{DataSourcePlugin, PluginRowCtx, PluginRowResult, RowIdentity};
pub use engine::{Engine, EngineOptions, RunHandle, RunResult, RunStatus};
pub use error::{EngineError, ProtocolError, ScriptError};
pub use executor::partition_spec;
pub use flow::{FlowRunner, IterationOutcome, ScenarioProgram};
pub use metrics::{MetricKind, MetricRegistry, MetricsBus, Sample, Tags};
pub use output::{ChannelOutput, Output};
pub use protocol::{
    PreparedRequest, ProtocolHandler, ProtocolRegistry, ProtocolResponse, RequestOptions, Timings,
};
pub use script::{
    HostHttpRequest, HostHttpResponse, ScriptEngine, ScriptHost, ScriptLogLevel, VuScript,
};
pub use summary::{Summary, TimelinePoint};
pub use thresholds::ThresholdStatus;
pub use vu::{RunContext, VuContext};

/// Exit code when thresholds fail.
pub const EXIT_THRESHOLD_FAILED: i32 = 99;
