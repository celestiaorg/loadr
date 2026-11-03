//! The agent's acknowledged uplink window.
//!
//! Producers append sequence-stamped messages to a bounded window. A
//! per-session writer streams the window onto the wire and the controller
//! acknowledges cumulatively. **Nothing leaves the window until it is
//! acknowledged**, so a session that dies mid-flight replays its unacknowledged
//! tail over the next one instead of dropping it.
//!
//! Delivery is therefore at-least-once, never exactly-once: after a reconnect
//! the controller can see a message it already applied. Metric merging is
//! additive, so the controller deduplicates per agent incarnation — see
//! `UplinkAck` in `coordination.proto` for the two obligations that entails.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use prost::Message as _;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::pb;

/// At the agent's 500 ms snapshot interval this holds minutes of metric deltas
/// — an order of magnitude past the 15 s reconnect-backoff ceiling. Sized in
/// bytes rather than messages because a `MetricsBatch` for a wide plan is
/// orders of magnitude larger than a run event.
const DEFAULT_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Backstop so a flood of tiny messages cannot grow the deque without bound.
const DEFAULT_MAX_MESSAGES: usize = 4096;

struct Entry {
    msg: pb::AgentMessage,
    /// The size admission reserved, so [`Uplink::ack`] frees exactly that much.
    bytes: usize,
}

struct State {
    next_seq: u64,
    /// Unacknowledged messages in sequence order; the front is the oldest.
    pending: VecDeque<Entry>,
    /// How many front entries the *current* session's writer already took.
    cursor: usize,
    bytes: usize,
    /// Bumped by [`Uplink::rewind`]. A writer holding an older epoch has been
    /// superseded and must not touch the cursor.
    epoch: u64,
    closed: bool,
}

impl State {
    /// The budget is a floor rather than a ceiling: admission stops once the
    /// window has reached it, so the window can hold at most one message beyond
    /// `max_bytes`. That keeps one predicate for both admission and
    /// [`Uplink::is_full`], and it means an empty window never refuses — a
    /// single oversized delta still gets out instead of parking its producer
    /// forever.
    fn is_full(&self, max_bytes: usize, max_messages: usize) -> bool {
        self.bytes >= max_bytes || self.pending.len() >= max_messages
    }
}

/// Why [`Uplink::push`] refused.
// The size spread between the variants is the point, for the same reason
// `push` allows `result_large_err`: carrying the refused message by value is
// what lets a parked producer retry it without cloning a payload orders of
// magnitude bigger than the enum.
#[allow(clippy::large_enum_variant)]
enum Rejected {
    /// The window is full. The message comes back so a parked producer can
    /// retry it without cloning.
    Full(pb::AgentMessage),
    /// The agent is shutting down; nothing more will be delivered.
    Closed,
}

/// An ordered, bounded window of uplink messages awaiting acknowledgement.
pub(crate) struct Uplink {
    state: Mutex<State>,
    /// "There is unsent work", or "your epoch is over". Exactly one writer
    /// consumes this, and `notify_one` stores a permit when nobody is waiting,
    /// so a plain check-then-wait loop cannot lose a wakeup.
    ready: Notify,
    /// "The window has room." Several producers may park here, so this is woken
    /// with `notify_waiters`, which only reaches waiters that already exist —
    /// callers must register before they check.
    room: Notify,
    max_bytes: usize,
    max_messages: usize,
    incarnation: String,
}

impl Uplink {
    pub(crate) fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_BYTES, DEFAULT_MAX_MESSAGES)
    }

    pub(crate) fn with_limits(max_bytes: usize, max_messages: usize) -> Self {
        Uplink {
            state: Mutex::new(State {
                next_seq: 0,
                pending: VecDeque::new(),
                cursor: 0,
                bytes: 0,
                epoch: 0,
                closed: false,
            }),
            ready: Notify::new(),
            room: Notify::new(),
            max_bytes,
            max_messages,
            incarnation: uuid::Uuid::new_v4().to_string(),
        }
    }

    /// Identifies this agent process's sequence space. The controller resets its
    /// deduplication cursor when it changes, which is what lets a restarted
    /// agent start again at sequence 1 without its messages looking like
    /// duplicates.
    pub(crate) fn incarnation(&self) -> &str {
        &self.incarnation
    }

    /// Append without waiting. `false` when the window is full or closed —
    /// leaving the sequence space untouched, so a caller that retries later
    /// produces the next sequence number rather than a gap.
    pub(crate) fn try_enqueue(&self, msg: pb::AgentMessage) -> bool {
        self.push(msg).is_ok()
    }

    /// Append, waiting for room. `false` only once the uplink is closed, at
    /// which point nothing can be delivered anyway.
    pub(crate) async fn enqueue(&self, mut msg: pb::AgentMessage) -> bool {
        loop {
            // Register before the capacity check: `room` is woken with
            // `notify_waiters`, which skips waiters that do not exist yet, so
            // checking first would let an acknowledgement slip through the gap
            // and park this producer until the one after it.
            let room = self.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            match self.push(msg) {
                Ok(_) => return true,
                Err(Rejected::Closed) => return false,
                Err(Rejected::Full(returned)) => msg = returned,
            }
            room.await;
        }
    }

    /// Whether the window would refuse another message. Lets producers bail out
    /// before serializing one: through a long disconnect that work would be
    /// repeated every tick over a payload that only grows.
    ///
    /// Exactly the predicate [`Uplink::push`] admits on, so a `false` here is a
    /// promise that a `try_enqueue` racing nothing else will be accepted.
    pub(crate) fn is_full(&self) -> bool {
        let state = self.state.lock();
        state.is_full(self.max_bytes, self.max_messages)
    }

    // The large `Err` is the point: handing the refused message back by move is
    // what lets a parked producer retry it without cloning a payload three
    // orders of magnitude bigger than the enum.
    #[allow(clippy::result_large_err)]
    fn push(&self, mut msg: pb::AgentMessage) -> Result<u64, Rejected> {
        // Measured before the lock and reused for the accounting so `ack` frees
        // exactly what admission reserved. The sequence varint stamped below
        // adds a couple of untracked bytes — noise against the budget.
        let bytes = msg.encoded_len();
        let mut state = self.state.lock();
        if state.closed {
            return Err(Rejected::Closed);
        }
        if state.is_full(self.max_bytes, self.max_messages) {
            return Err(Rejected::Full(msg));
        }
        state.next_seq += 1;
        let seq = state.next_seq;
        msg.seq = seq;
        state.bytes += bytes;
        state.pending.push_back(Entry { msg, bytes });
        drop(state);
        self.ready.notify_one();
        Ok(seq)
    }

    /// Retire every message through `seq`. Cumulative and idempotent: a
    /// repeated or older acknowledgement changes nothing.
    pub(crate) fn ack(&self, seq: u64) {
        let mut state = self.state.lock();
        if seq > state.next_seq {
            // The controller acknowledged something this process never sent, so
            // it is describing a different incarnation or is malformed.
            // Retiring on that basis would discard messages nobody has read.
            tracing::warn!(
                seq,
                queued = state.next_seq,
                "ignoring an uplink acknowledgement for a sequence never sent"
            );
            return;
        }
        let mut freed = 0;
        while state.pending.front().is_some_and(|e| e.msg.seq <= seq) {
            let entry = state
                .pending
                .pop_front()
                .expect("the front was just matched");
            state.bytes = state.bytes.saturating_sub(entry.bytes);
            freed += 1;
        }
        if freed == 0 {
            return;
        }
        // The writer had already streamed those entries; the cursor now indexes
        // the same logical position in a shorter deque.
        state.cursor = state.cursor.saturating_sub(freed);
        drop(state);
        self.room.notify_waiters();
    }

    /// Start a new session's view of the window: replay from the oldest
    /// unacknowledged message. Returns the epoch the new writer must present.
    fn rewind(&self) -> u64 {
        let mut state = self.state.lock();
        state.epoch += 1;
        state.cursor = 0;
        let epoch = state.epoch;
        drop(state);
        // Unconditional, so this also wakes a superseded writer to notice that
        // its epoch is over and exit.
        self.ready.notify_one();
        epoch
    }

    /// The next message this session has not written yet, or `None` once the
    /// session is superseded or the uplink is closed and fully written.
    ///
    /// Returns a clone: the window keeps the original, which is exactly what
    /// makes aborting a writer mid-send lossless. There is no `await` between
    /// taking the lock and advancing the cursor, so dropping this future either
    /// yields the message or touches nothing.
    async fn next_unsent(&self, epoch: u64) -> Option<pb::AgentMessage> {
        loop {
            {
                let mut state = self.state.lock();
                if state.epoch != epoch {
                    break;
                }
                if let Some(entry) = state.pending.get(state.cursor) {
                    let msg = entry.msg.clone();
                    state.cursor += 1;
                    return Some(msg);
                }
                if state.closed {
                    break;
                }
            }
            self.ready.notified().await;
        }
        // Hand the permit on: this writer is leaving without doing the work a
        // notification may have been meant for.
        self.ready.notify_one();
        None
    }

    /// Stop accepting messages and release every parked producer. Whatever is
    /// already in the window stays put; the current writer drains what it can.
    pub(crate) fn close(&self) {
        self.state.lock().closed = true;
        self.ready.notify_one();
        self.room.notify_waiters();
    }

    #[cfg(test)]
    fn pending_seqs(&self) -> Vec<u64> {
        self.state
            .lock()
            .pending
            .iter()
            .map(|e| e.msg.seq)
            .collect()
    }
}

/// One session's uplink writer.
///
/// It streams the unacknowledged window onto the session's outbound channel
/// from its own task, so a stalled wire can never stop the session loop from
/// reading acknowledgements or sending heartbeats — which would deadlock, since
/// acknowledgements are the only thing that frees window room.
pub(crate) struct SessionWriter {
    handle: Option<JoinHandle<()>>,
}

impl SessionWriter {
    /// Spawn a writer for a freshly opened session, replaying from the oldest
    /// unacknowledged message.
    ///
    /// Call this only once the stream is open and `Register` is queued:
    /// registration has to be the first frame on the wire.
    pub(crate) fn spawn(uplink: Arc<Uplink>, tx: mpsc::Sender<pb::AgentMessage>) -> Self {
        let epoch = uplink.rewind();
        let handle = tokio::spawn(async move {
            while let Some(msg) = uplink.next_unsent(epoch).await {
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
        });
        SessionWriter {
            handle: Some(handle),
        }
    }

    /// Stop the writer and wait for the cancellation to be observed. `abort`
    /// alone only *requests* it, and a straggler that kept running would go on
    /// writing to a stream the next session has already replaced.
    pub(crate) async fn stop(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for SessionWriter {
    /// Covers any path that returns without calling [`SessionWriter::stop`].
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::pb::agent_message::Msg as AgentMsg;

    fn event(detail: &str) -> pb::AgentMessage {
        pb::AgentMessage {
            seq: 0,
            msg: Some(AgentMsg::Event(pb::RunEvent {
                run_id: "run-1".to_string(),
                kind: "started".to_string(),
                detail: detail.to_string(),
                summary_json: Vec::new(),
            })),
        }
    }

    /// A message whose encoded size is dominated by `bytes` of padding.
    fn padded(bytes: usize) -> pb::AgentMessage {
        event(&"x".repeat(bytes))
    }

    #[test]
    fn seq_numbers_increase_monotonically_and_are_never_reused() {
        let uplink = Uplink::with_limits(64 * 1024, 16);
        for i in 0..3 {
            assert!(uplink.try_enqueue(event(&format!("e{i}"))));
        }
        assert_eq!(uplink.pending_seqs(), vec![1, 2, 3]);
        uplink.ack(3);
        assert!(uplink.try_enqueue(event("next")));
        assert_eq!(
            uplink.pending_seqs(),
            vec![4],
            "sequence numbers continue past acknowledged messages"
        );
    }

    #[test]
    fn a_full_window_rejects_without_consuming_a_sequence_number() {
        let uplink = Uplink::with_limits(64 * 1024, 2);
        assert!(uplink.try_enqueue(event("one")));
        assert!(uplink.try_enqueue(event("two")));
        assert!(!uplink.try_enqueue(event("refused")));
        uplink.ack(2);
        assert!(uplink.try_enqueue(event("three")));
        assert_eq!(
            uplink.pending_seqs(),
            vec![3],
            "a refused message must not burn a sequence number: the caller retries \
             its payload as a fresh message, and a gap would look like loss"
        );
    }

    #[tokio::test]
    async fn unacked_messages_replay_after_a_severed_session() {
        let uplink = Arc::new(Uplink::with_limits(64 * 1024, 16));
        for i in 0..4 {
            assert!(uplink.try_enqueue(event(&format!("e{i}"))));
        }

        // A one-slot channel, so the writer stalls with most of the window
        // unwritten — the state a mid-stream disconnect leaves behind.
        let (tx, mut rx) = mpsc::channel(1);
        let writer = SessionWriter::spawn(uplink.clone(), tx);
        assert_eq!(rx.recv().await.expect("first message").seq, 1);
        uplink.ack(1);
        drop(rx);
        writer.stop().await;

        // The next session replays from the oldest unacknowledged message: 1 is
        // retired, 2..=4 come back in order with nothing skipped.
        let (tx, mut rx) = mpsc::channel(8);
        let writer = SessionWriter::spawn(uplink.clone(), tx);
        for expected in 2..=4 {
            assert_eq!(
                rx.recv().await.expect("replayed message").seq,
                expected,
                "the replay must be ordered and complete"
            );
        }
        writer.stop().await;
    }

    #[tokio::test]
    async fn an_acked_prefix_never_replays() {
        let uplink = Arc::new(Uplink::with_limits(64 * 1024, 16));
        for i in 0..3 {
            assert!(uplink.try_enqueue(event(&format!("e{i}"))));
        }
        let (tx, mut rx) = mpsc::channel(8);
        let writer = SessionWriter::spawn(uplink.clone(), tx);
        for expected in 1..=3 {
            assert_eq!(rx.recv().await.expect("sent message").seq, expected);
        }
        uplink.ack(3);
        writer.stop().await;
        drop(rx);

        let (tx, mut rx) = mpsc::channel(8);
        let writer = SessionWriter::spawn(uplink.clone(), tx);
        assert!(
            timeout(Duration::from_millis(50), rx.recv()).await.is_err(),
            "a fully acknowledged window has nothing to replay"
        );
        writer.stop().await;
    }

    #[test]
    fn a_cumulative_ack_clears_every_message_through_that_sequence() {
        let uplink = Uplink::with_limits(64 * 1024, 16);
        for i in 0..5 {
            assert!(uplink.try_enqueue(event(&format!("e{i}"))));
        }
        uplink.ack(3);
        assert_eq!(uplink.pending_seqs(), vec![4, 5]);
        uplink.ack(2);
        assert_eq!(
            uplink.pending_seqs(),
            vec![4, 5],
            "an older acknowledgement changes nothing"
        );
    }

    #[test]
    fn an_ack_for_a_sequence_never_sent_is_ignored() {
        let uplink = Uplink::with_limits(64 * 1024, 16);
        assert!(uplink.try_enqueue(event("one")));
        uplink.ack(99);
        assert_eq!(
            uplink.pending_seqs(),
            vec![1],
            "an acknowledgement this process never earned must not retire anything"
        );
        uplink.ack(1);
        assert!(uplink.pending_seqs().is_empty());
    }

    #[tokio::test]
    async fn acking_makes_room_for_a_blocked_enqueue() {
        let uplink = Arc::new(Uplink::with_limits(64 * 1024, 1));
        assert!(uplink.try_enqueue(event("first")));

        let mut waiting = tokio::spawn({
            let uplink = uplink.clone();
            async move { uplink.enqueue(event("second")).await }
        });
        assert!(
            timeout(Duration::from_millis(50), &mut waiting)
                .await
                .is_err(),
            "a full window parks the producer"
        );

        uplink.ack(1);
        assert!(
            timeout(Duration::from_millis(500), waiting)
                .await
                .expect("the producer is released")
                .expect("the task did not panic"),
            "the message is accepted once the window drains"
        );
        assert_eq!(uplink.pending_seqs(), vec![2]);
    }

    #[tokio::test]
    async fn a_blocked_enqueue_is_released_by_close_and_reports_failure() {
        let uplink = Arc::new(Uplink::with_limits(64 * 1024, 1));
        assert!(uplink.try_enqueue(event("first")));

        let mut waiting = tokio::spawn({
            let uplink = uplink.clone();
            async move { uplink.enqueue(event("second")).await }
        });
        assert!(timeout(Duration::from_millis(50), &mut waiting)
            .await
            .is_err());

        uplink.close();
        assert!(
            !timeout(Duration::from_millis(500), waiting)
                .await
                .expect("the producer is released")
                .expect("the task did not panic"),
            "a closed uplink reports the message as undeliverable rather than hanging"
        );
    }

    #[tokio::test]
    async fn a_superseded_writer_cannot_advance_the_new_sessions_cursor() {
        let uplink = Uplink::with_limits(64 * 1024, 16);
        assert!(uplink.try_enqueue(event("only")));

        let superseded = uplink.rewind();
        let live = uplink.rewind();

        assert!(
            uplink.next_unsent(superseded).await.is_none(),
            "a superseded epoch is served nothing, so a straggling writer cannot \
             consume the live session's cursor"
        );
        assert_eq!(
            uplink.next_unsent(live).await.map(|m| m.seq),
            Some(1),
            "the live session still sees the message from the start of the window"
        );
    }

    #[test]
    fn the_window_is_bounded_by_bytes_before_it_is_bounded_by_count() {
        let uplink = Uplink::with_limits(4096, 64);
        let mut admitted = 0;
        while uplink.try_enqueue(padded(1024)) {
            admitted += 1;
            assert!(
                admitted < 64,
                "the count backstop must not be what stops us"
            );
        }
        assert!(
            (2..8).contains(&admitted),
            "a 4 KiB budget holds a handful of 1 KiB messages, not the 64-message cap; \
             admitted {admitted}"
        );
        assert!(uplink.is_full());
    }

    #[test]
    fn an_oversized_message_is_admitted_into_an_empty_window() {
        let uplink = Uplink::with_limits(512, 64);
        assert!(
            uplink.try_enqueue(padded(4096)),
            "an empty window never refuses, so one huge delta still gets out"
        );
        assert!(uplink.is_full());
        assert!(!uplink.try_enqueue(event("small")));
        uplink.ack(1);
        assert!(
            uplink.try_enqueue(event("small")),
            "room again once the oversized message is acknowledged"
        );
    }

    #[tokio::test]
    async fn next_unsent_returns_none_once_closed_and_fully_written() {
        let uplink = Uplink::with_limits(64 * 1024, 16);
        assert!(uplink.try_enqueue(event("one")));
        let epoch = uplink.rewind();
        assert_eq!(uplink.next_unsent(epoch).await.map(|m| m.seq), Some(1));
        uplink.close();
        assert!(uplink.next_unsent(epoch).await.is_none());
    }
}
