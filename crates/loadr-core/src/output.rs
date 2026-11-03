//! Output/exporter abstraction. Built-in outputs live in `loadr-outputs`;
//! plugin outputs implement the same trait via the plugin bridge.

use async_trait::async_trait;

use crate::aggregate::{MetricsDelta, Snapshot};
use crate::error::EngineError;
use crate::metrics::Sample;
use crate::summary::Summary;

/// A metrics consumer. Methods are called from the aggregator task:
/// `on_samples` per flush batch (~100 ms), `on_snapshot` once per second,
/// `finish` once at the end of the run. Opt-in delta path via `wants_delta`/`on_delta`
/// for drained [`MetricsDelta`]s (e.g. the distributed agent's uplink to its controller).
#[async_trait]
pub trait Output: Send {
    fn name(&self) -> &str;

    async fn start(&mut self) -> Result<(), EngineError> {
        Ok(())
    }

    async fn on_samples(&mut self, _samples: &[Sample]) {}

    /// Whether this output needs raw samples via `on_samples`. Conservative
    /// default: `true` (covers `NativeOutputAdapter`/plugin FFI, whose real
    /// needs the engine can't see). When every configured output overrides
    /// this to `false`, the engine records straight into shard aggregators
    /// instead of running the sample channel + aggregator batching (see
    /// `MetricShards`) — no channel, no per-sample clock read, no
    /// end-of-run drain backlog. Snapshot-only outputs (metrics backends,
    /// the live-UI channel, the distributed uplink) should override this.
    fn wants_samples(&self) -> bool {
        true
    }

    async fn on_snapshot(&mut self, _snapshot: &Snapshot) {}

    /// Opt in to receiving drained [`MetricsDelta`]s via `on_delta` (e.g. the
    /// distributed agent's uplink to its controller). Most outputs don't
    /// need this — snapshots and raw samples already cover them.
    fn wants_delta(&self) -> bool {
        false
    }

    /// A delta drained from the aggregator: once per snapshot tick, plus a
    /// final call with `last = true` at the end of the run. Return `false`
    /// if the delta could not be accepted (e.g. a congested channel) so the
    /// aggregator can restore it and retry on the next tick.
    async fn on_delta(&mut self, _delta: &MetricsDelta, _last: bool) -> bool {
        true
    }

    async fn finish(&mut self, _summary: &Summary) {}
}

/// An output that fans samples out to a tokio channel — used to bridge
/// metrics into the web UI and the agent's controller stream.
pub struct ChannelOutput {
    name: String,
    snapshots: tokio::sync::watch::Sender<std::sync::Arc<Snapshot>>,
}

impl ChannelOutput {
    pub fn new(
        name: impl Into<String>,
    ) -> (Self, tokio::sync::watch::Receiver<std::sync::Arc<Snapshot>>) {
        let (tx, rx) = tokio::sync::watch::channel(std::sync::Arc::new(Snapshot::default()));
        (
            ChannelOutput {
                name: name.into(),
                snapshots: tx,
            },
            rx,
        )
    }
}

#[async_trait]
impl Output for ChannelOutput {
    fn name(&self) -> &str {
        &self.name
    }

    fn wants_samples(&self) -> bool {
        false
    }

    async fn on_snapshot(&mut self, snapshot: &Snapshot) {
        let _ = self.snapshots.send(std::sync::Arc::new(snapshot.clone()));
    }
}
