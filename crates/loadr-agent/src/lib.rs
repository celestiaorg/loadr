//! # loadr-agent
//!
//! Distributed execution for [loadr](https://loadr.io): a gRPC coordination
//! protocol, the load [`Agent`] that executes partitioned test plans, and the
//! [`Controller`] that assigns work, merges metric deltas centrally and
//! evaluates thresholds over the whole fleet.
//!
//! The agent never links protocol or script implementations directly; they are
//! injected through [`RunnerDeps`] factories so build order stays decoupled.

pub mod agent;
pub mod controller;
mod error;
mod uplink;

/// Generated protobuf/tonic code for `loadr.coordination.v1`.
pub mod pb {
    #![allow(clippy::all, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/loadr.coordination.v1.rs"));
}

/// The compiled `FileDescriptorSet` for the coordination proto, usable for
/// dynamic codecs or gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/coordination_descriptor.bin"));

/// Coordination protocol version. Registration with a different version is
/// rejected by the controller.
///
/// Version 3 introduced acknowledged uplink delivery (`AgentMessage.seq`,
/// `Register.incarnation`, `UplinkAck`). The new fields are wire-compatible, so
/// nothing but this check stops an agent that expects acknowledgements from
/// connecting to a controller that never sends them — where it would fill its
/// replay window and silently stop shipping metrics. Hence the hard rejection.
/// Version 2 is reserved for control-command acknowledgements, developed in
/// parallel and not yet upstream.
pub const PROTOCOL_VERSION: u32 = 3;

pub use agent::{Agent, AgentConfig, AgentTls, ProtocolFactory, RunnerDeps, ScriptFactory};
pub use controller::{
    AgentInfo, Controller, ControllerConfig, ControllerHandle, ControllerTls, OnAgentLoss,
    RunSummaryInfo, SubmitOptions,
};
pub use error::AgentError;

/// Milliseconds since the Unix epoch.
pub(crate) fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
