//! Transport owners of connections, raw frames, and unfunded output.
//!
//! [`ResourceEnvelope`] derives every byte bound of this module from the
//! transport's own limits. [`NodeResources`] holds the node-wide budgets, and
//! each connection and stream takes a narrower view of them.
//!
//! - A [`FrameGate`] admits one frame's bytes before the frame's memory exists.
//!   The reader asks before allocating a payload, and a sender asks before
//!   queueing a frame that has no other owner. Each stream keeps one floor slot
//!   outside the shared budgets, so another stream's retained frames can delay
//!   this stream's frames but never block its first one. That keeps a service
//!   able to drain the stream it waits on.
//! - A [`FrameGrant`] travels with its frame. A raw grant stays with the
//!   application's receiver through decoding, until its next receive. An output
//!   grant stays with the queued frame until its transport write ends. Dropping
//!   either, including through cancellation, returns the bytes.
//! - An [`OwnerGuard`] counts one handshake or stream in one [`OwnerPhase`], so
//!   a reader of [`OwnerLedger`] sees setup, installed, and retiring owners
//!   together across reconnects.
//!
//! Waiting here is local backpressure. It never faults a peer. A paused reader
//! stops granting QUIC credit, and the peer memory budget already funds that
//! unspent credit. Reading a frame moves its bytes out of QUIC's buffers, so a
//! raw grant and transport credit never charge the same bytes.

use std::sync::Arc;
use std::time::Duration;

use zakura_rt::{
    sync::counter::{AtomicUsize, Ordering},
    time::Instant,
};

use crate::zakura::{
    regulation::{sizing, OutputByteBudget, OutputGrant, SlotBudget, SlotPermit},
    FRAME_HEADER_BYTES,
};

#[cfg(test)]
mod tests;

/// The node's transport byte and worker bounds, derived from its transport limits.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResourceEnvelope {
    /// Raw inbound frame bytes that one connection may retain.
    pub(crate) connection_raw_bytes: u64,
    /// Raw inbound frame bytes that all connections may retain together.
    pub(crate) node_raw_bytes: u64,
    /// Queued frame bytes without another owner that one connection may retain.
    pub(crate) connection_output_bytes: u64,
    /// Queued frame bytes without another owner that the node may retain.
    pub(crate) node_output_bytes: u64,
    /// Request-stream executions that may run at once across the node.
    pub(crate) request_workers: usize,
}

impl ResourceEnvelope {
    /// Bounds for a node with these QUIC windows and connection cap.
    ///
    /// Each byte budget is at least twice its QUIC window, as `regulation::sizing`
    /// requires, so the transport stays the byte limit for a healthy peer.
    pub(crate) fn new(receive_window: u64, send_window: u64, max_connections: usize) -> Self {
        let connection_raw_bytes = receive_window.saturating_mul(2).max(1);
        let connection_output_bytes = send_window.saturating_mul(2).max(1);
        Self {
            connection_raw_bytes,
            node_raw_bytes: sizing::node_output_bytes().max(connection_raw_bytes),
            connection_output_bytes,
            node_output_bytes: sizing::node_output_bytes().max(connection_output_bytes),
            request_workers: max_connections.saturating_mul(2).max(1),
        }
    }

    /// The most frame bytes these bounds allow this module's owners to retain:
    /// the node budgets, plus each stream's raw and output floor frame.
    ///
    /// QUIC's own buffers come on top; the peer memory budget funds those.
    pub(crate) fn retained_bytes_bound(
        &self,
        max_connections: usize,
        max_open_streams: u16,
        max_frame_bytes: u32,
    ) -> u128 {
        let floor_frames = 2 * max_connections as u128 * u128::from(max_open_streams);
        u128::from(self.node_raw_bytes)
            + u128::from(self.node_output_bytes)
            + floor_frames * u128::from(max_frame_bytes)
    }
}

/// The node-wide owners of transport resources.
#[derive(Clone, Debug)]
pub(crate) struct NodeResources {
    envelope: ResourceEnvelope,
    raw: OutputByteBudget,
    output: OutputByteBudget,
    request_workers: SlotBudget,
    owners: OwnerLedger,
}

impl NodeResources {
    /// Empty budgets of the sizes in `envelope`.
    pub(crate) fn new(envelope: ResourceEnvelope) -> Self {
        Self {
            envelope,
            raw: byte_budget(envelope.node_raw_bytes),
            output: byte_budget(envelope.node_output_bytes),
            request_workers: SlotBudget::new(envelope.request_workers)
                .expect("the envelope keeps at least one worker within a semaphore's range"),
            owners: OwnerLedger::default(),
        }
    }

    /// The count of this node's handshakes and streams, by phase.
    pub(crate) fn owners(&self) -> &OwnerLedger {
        &self.owners
    }

    /// Fresh per-connection budgets within the node's budgets.
    pub(crate) fn connection(&self) -> ConnectionResources {
        ConnectionResources {
            node: self.clone(),
            raw: byte_budget(self.envelope.connection_raw_bytes),
            output: byte_budget(self.envelope.connection_output_bytes),
        }
    }
}

/// A byte budget whose size comes from a validated envelope.
fn byte_budget(bytes: u64) -> OutputByteBudget {
    OutputByteBudget::new(bytes.min(zakura_rt::sync::Semaphore::MAX_PERMITS as u64))
        .expect("the envelope keeps each byte budget positive and the minimum bounds it")
}

/// One connection's budgets, which share the node's budgets.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionResources {
    node: NodeResources,
    raw: OutputByteBudget,
    output: OutputByteBudget,
}

impl ConnectionResources {
    /// The owners of one stream that entered setup. Each has its own floor.
    pub(crate) fn stream(&self) -> StreamResources {
        StreamResources {
            raw: FrameGate::new(self.raw.clone(), self.node.raw.clone()),
            output: FrameGate::new(self.output.clone(), self.node.output.clone()),
            request_workers: self.node.request_workers.clone(),
            owner: self.node.owners.enter(OwnerPhase::Setup),
        }
    }
}

/// One stream's admission gates and its place in the owner count.
#[derive(Debug)]
pub(crate) struct StreamResources {
    /// Admits raw inbound frames before the reader allocates their payloads.
    pub(crate) raw: FrameGate,
    /// Admits queued outbound frames that have no other owner.
    pub(crate) output: FrameGate,
    /// Bounds request-stream executions across the node.
    pub(crate) request_workers: SlotBudget,
    /// Counts this stream from setup until its reader has ended.
    pub(crate) owner: OwnerGuard,
}

#[cfg(test)]
impl ConnectionResources {
    /// Owners that never bind, for tests of other behavior.
    pub(crate) fn unconstrained() -> Self {
        NodeResources::new(ResourceEnvelope::new(1 << 40, 1 << 40, 1 << 16)).connection()
    }
}

#[cfg(test)]
impl StreamResources {
    /// Owners that never bind, for tests of other behavior.
    pub(crate) fn unconstrained() -> Self {
        ConnectionResources::unconstrained().stream()
    }
}

/// Admits one stream's frames against its connection and the node.
#[derive(Clone, Debug)]
pub(crate) struct FrameGate {
    connection: OutputByteBudget,
    node: OutputByteBudget,
    floor: SlotBudget,
}

impl FrameGate {
    /// A gate with a fresh floor slot over these shared budgets.
    pub(crate) fn new(connection: OutputByteBudget, node: OutputByteBudget) -> Self {
        Self {
            connection,
            node,
            floor: SlotBudget::new(1).expect("one slot is a valid capacity"),
        }
    }

    /// Admit a frame of `bytes` without waiting: from the shared budgets if
    /// they have room, otherwise from this stream's floor slot.
    pub(crate) fn try_admit(&self, bytes: u64) -> Option<FrameGrant> {
        let shared = self.try_shared(bytes).map(FrameCharge::shared);
        shared
            .or_else(|| self.floor.try_reserve().map(FrameCharge::floor))
            .map(|charge| FrameGrant {
                _charge: charge,
                waited: Duration::ZERO,
            })
    }

    /// Wait for a frame of `bytes` until the shared budgets have room or the
    /// floor slot returns. Cancelling the wait takes nothing.
    pub(crate) async fn admit(&self, bytes: u64) -> FrameGrant {
        if let Some(grant) = self.try_admit(bytes) {
            return grant;
        }
        let started = Instant::now();
        let charge = zakura_rt::select! {
            biased;
            floor = self.floor.reserve() => FrameCharge::floor(floor),
            shared = self.shared(bytes) => FrameCharge::shared(shared),
        };
        FrameGrant {
            _charge: charge,
            waited: started.elapsed(),
        }
    }

    fn try_shared(&self, bytes: u64) -> Option<(OutputGrant, OutputGrant)> {
        let connection = self.connection.try_grant(self.connection.clamp(bytes))?;
        let node = self.node.try_grant(self.node.clamp(bytes))?;
        Some((connection, node))
    }

    async fn shared(&self, bytes: u64) -> (OutputGrant, OutputGrant) {
        let connection = self.connection.grant(self.connection.clamp(bytes)).await;
        let node = self.node.grant(self.node.clamp(bytes)).await;
        (connection, node)
    }
}

/// The encoded size of a frame with `payload_len` payload bytes.
pub(crate) fn frame_wire_bytes(payload_len: usize) -> u64 {
    // Widening usize to u64 is lossless on supported targets.
    (payload_len as u64).saturating_add(FRAME_HEADER_BYTES as u64)
}

/// The admission of one frame. Dropping it returns the frame's capacity.
#[derive(Debug)]
#[must_use = "dropping a frame grant releases its capacity"]
pub(crate) struct FrameGrant {
    _charge: FrameCharge,
    waited: Duration,
}

impl FrameGrant {
    /// How long admission waited.
    pub(crate) fn waited(&self) -> Duration {
        self.waited
    }
}

#[derive(Debug)]
enum FrameCharge {
    /// Field order is drop order: the node's bytes return first, because
    /// admission takes the connection's bytes first.
    Shared {
        _node: OutputGrant,
        _connection: OutputGrant,
    },
    Floor {
        _slot: SlotPermit,
    },
}

impl FrameCharge {
    fn floor(slot: SlotPermit) -> Self {
        Self::Floor { _slot: slot }
    }

    fn shared((connection, node): (OutputGrant, OutputGrant)) -> Self {
        Self::Shared {
            _node: node,
            _connection: connection,
        }
    }
}

/// The lifecycle phase of one handshake or stream owner.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum OwnerPhase {
    /// A control handshake is running.
    Handshake = 0,
    /// A stream holds its permit before its worker starts.
    Setup = 1,
    /// A stream's worker runs.
    Installed = 2,
    /// A stream's worker stopped and waits for its reader's buffers to drop.
    Retiring = 3,
}

const OWNER_PHASES: usize = 4;

impl OwnerPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Handshake => "handshake",
            Self::Setup => "setup",
            Self::Installed => "installed",
            Self::Retiring => "retiring",
        }
    }
}

/// Counts of this node's owners, read one phase at a time.
///
/// An owner moves between phases one count at a time, so a read during a move
/// can differ by that one owner.
#[cfg(test)]
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct OwnerCounts {
    pub(crate) handshake: usize,
    pub(crate) setup: usize,
    pub(crate) installed: usize,
    pub(crate) retiring: usize,
}

#[cfg(test)]
impl OwnerCounts {
    /// Every owner, whichever phase it is in.
    pub(crate) fn total(&self) -> usize {
        self.handshake + self.setup + self.installed + self.retiring
    }
}

/// The node's count of handshake and stream owners by phase.
#[derive(Clone, Debug, Default)]
pub(crate) struct OwnerLedger(Arc<[AtomicUsize; OWNER_PHASES]>);

impl OwnerLedger {
    /// Count a new owner in `phase` until the returned guard drops.
    pub(crate) fn enter(&self, phase: OwnerPhase) -> OwnerGuard {
        self.add(phase);
        OwnerGuard {
            ledger: self.clone(),
            phase: AtomicUsize::new(phase as usize),
        }
    }

    /// The current counts.
    #[cfg(test)]
    pub(crate) fn counts(&self) -> OwnerCounts {
        let count = |phase: OwnerPhase| self.0[phase as usize].load(Ordering::Relaxed);
        OwnerCounts {
            handshake: count(OwnerPhase::Handshake),
            setup: count(OwnerPhase::Setup),
            installed: count(OwnerPhase::Installed),
            retiring: count(OwnerPhase::Retiring),
        }
    }

    fn add(&self, phase: OwnerPhase) {
        let count = self.0[phase as usize].fetch_add(1, Ordering::Relaxed) + 1;
        self.report(phase, count);
    }

    fn remove(&self, phase: OwnerPhase) {
        let count = self.0[phase as usize].fetch_sub(1, Ordering::Relaxed) - 1;
        self.report(phase, count);
    }

    fn report(&self, phase: OwnerPhase, count: usize) {
        // Owner counts stay far below 2^53, so f64 represents them exactly.
        metrics::gauge!("zakura.p2p.owners", "phase" => phase.label()).set(count as f64);
    }
}

/// One owner's count in the [`OwnerLedger`].
#[derive(Debug)]
pub(crate) struct OwnerGuard {
    ledger: OwnerLedger,
    /// The current [`OwnerPhase`] as its discriminant. The owner's context is
    /// shared with its reader, so moves take a shared reference.
    phase: AtomicUsize,
}

impl OwnerGuard {
    /// Move this owner to `phase`.
    pub(crate) fn advance(&self, phase: OwnerPhase) {
        let previous = self.phase.swap(phase as usize, Ordering::Relaxed);
        self.ledger.add(phase);
        self.ledger.remove(owner_phase(previous));
    }
}

impl Drop for OwnerGuard {
    fn drop(&mut self) {
        self.ledger
            .remove(owner_phase(self.phase.load(Ordering::Relaxed)));
    }
}

fn owner_phase(discriminant: usize) -> OwnerPhase {
    match discriminant {
        0 => OwnerPhase::Handshake,
        1 => OwnerPhase::Setup,
        2 => OwnerPhase::Installed,
        _ => OwnerPhase::Retiring,
    }
}
