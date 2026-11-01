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
//! - A [`CompatResponseGrant`] funds one outbound compatibility exchange from a
//!   partition of its own. The requester holds it from before it opens the stream
//!   until its decoder finishes, so a response's frames and reassembly copies never
//!   compete with native traffic for raw frame bytes.
//!
//! Waiting here is local backpressure. It never faults a peer, and it never
//! comes from another connection's traffic: the node budgets hold every
//! connection's budget at once. A paused reader stops granting QUIC credit, and
//! the peer memory budget already funds that unspent credit. Reading a frame
//! moves its bytes out of QUIC's buffers, so a raw grant and transport credit
//! never charge the same bytes, and [`ResourceEnvelope`] adds the two.
//!
//! A request stream funds its response from the output budgets before its service
//! runs, so no materialized result waits unfunded. Each execution also runs on a
//! stream that holds one of its connection's `max_open_streams` permits, and a
//! connection keeps its slot until its stream workers settle.

use std::sync::Arc;
use std::time::Duration;

use tracing::debug;
use zakura_rt::{
    sync::counter::{AtomicUsize, Ordering},
    time::Instant,
};

use crate::zakura::{
    regulation::{OutputByteBudget, OutputGrant, SlotBudget, SlotPermit},
    FRAME_HEADER_BYTES,
};
use zakura_chain::serialization::MAX_PROTOCOL_MESSAGE_LEN;

/// The most response frame bytes one compatibility exchange may retain.
///
/// The responder caps a response at the same value, so an honest peer never
/// exceeds it.
pub(crate) const COMPAT_RESPONSE_MAX_AGGREGATE_BYTES: usize = 8 * MAX_PROTOCOL_MESSAGE_LEN;

/// The most bytes one compatibility exchange may charge: its frames, plus the
/// validator's and the decoder's reassembly copies of one object.
pub(crate) const COMPAT_EXCHANGE_MAX_BYTES: u64 =
    (COMPAT_RESPONSE_MAX_AGGREGATE_BYTES + 2 * MAX_PROTOCOL_MESSAGE_LEN) as u64;

/// Worst-case exchanges the compatibility partition funds at once. A connection
/// runs one exchange at a time. A one-object exchange at the protocol's message limit
/// charges three message lengths, so this partition funds about 50 of those at once.
const COMPAT_RESPONSE_EXCHANGES: u64 = 16;

#[cfg(test)]
mod tests;

/// The transport limits that [`ResourceEnvelope`] derives from.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransportLimits {
    /// The QUIC receive window of one connection.
    pub(crate) receive_window: u32,
    /// The QUIC send window of one connection.
    pub(crate) send_window: u64,
    /// Connections the node admits.
    pub(crate) max_connections: usize,
    /// QUIC connections the node holds beyond the admitted ones.
    pub(crate) max_pending_handshakes: usize,
    /// Streams one connection may hold open.
    pub(crate) max_open_streams: u16,
    /// The largest frame a stream carries.
    pub(crate) max_frame_bytes: u32,
}

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
    /// Request-stream executions that open streams allow at once across the node.
    pub(crate) request_executions: usize,
    /// Outbound compatibility exchange bytes that the node may retain.
    pub(crate) compat_response_bytes: u64,
    /// The frames each stream's raw and output floor slots may retain, in bytes.
    pub(crate) floor_frame_bytes: u128,
    /// Unspent transport credit the peer memory budget funds, in bytes.
    pub(crate) transport_credit_bytes: u128,
}

impl ResourceEnvelope {
    /// Bounds for a node with these transport limits.
    ///
    /// Each connection budget is twice its QUIC window, as `regulation::sizing`
    /// requires, so the transport stays one connection's byte limit. Each node
    /// budget holds every admitted connection's budget at once, so a busy peer
    /// cannot pause another one. A node budget can only bind on frames that a
    /// retiring service keeps after its connection released its slot.
    pub(crate) fn new(limits: TransportLimits) -> Self {
        let connections = limits.max_connections.max(1);
        // Widening usize, u16, and u32 to u64 or u128 is lossless on supported targets.
        let node_bytes =
            |connection_bytes: u64| connection_bytes.saturating_mul(connections as u64);
        let connection_raw_bytes = u64::from(limits.receive_window).saturating_mul(2).max(1);
        let connection_output_bytes = limits.send_window.saturating_mul(2).max(1);
        let streams = connections as u128 * u128::from(limits.max_open_streams);
        let live_connections = connections.saturating_add(limits.max_pending_handshakes) as u128;
        Self {
            connection_raw_bytes,
            node_raw_bytes: node_bytes(connection_raw_bytes),
            connection_output_bytes,
            node_output_bytes: node_bytes(connection_output_bytes),
            request_executions: connections.saturating_mul(usize::from(limits.max_open_streams)),
            compat_response_bytes: COMPAT_RESPONSE_EXCHANGES * COMPAT_EXCHANGE_MAX_BYTES,
            floor_frame_bytes: 2 * streams * u128::from(limits.max_frame_bytes),
            transport_credit_bytes: live_connections
                * u128::from(zakura_quic::connection_memory_bytes(
                    limits.receive_window,
                    limits.send_window,
                )),
        }
    }

    /// The most frame bytes these bounds allow this module's owners to retain:
    /// the node budgets, the compatibility partition, and each stream's raw and
    /// output floor frame.
    pub(crate) fn retained_frame_bytes(&self) -> u128 {
        u128::from(self.node_raw_bytes)
            + u128::from(self.node_output_bytes)
            + u128::from(self.compat_response_bytes)
            + self.floor_frame_bytes
    }

    /// The most transport memory these bounds allow: retained frames, plus the
    /// bytes QUIC buffers under unspent credit.
    ///
    /// The two parts have separate funders. The peer memory budget funds only
    /// the credit, and no frame budget charges credit, so a byte that a reader
    /// moves out of QUIC's buffers spends a frame grant and returns its credit.
    /// Both parts can be full at once: returned credit lets the peer send again.
    pub(crate) fn transport_memory_bytes(&self) -> u128 {
        self.retained_frame_bytes() + self.transport_credit_bytes
    }
}

/// The node-wide owners of transport resources.
#[derive(Clone, Debug)]
pub(crate) struct NodeResources {
    envelope: ResourceEnvelope,
    raw: OutputByteBudget,
    output: OutputByteBudget,
    compat_responses: OutputByteBudget,
    owners: OwnerLedger,
}

impl NodeResources {
    /// Empty budgets of the sizes in `envelope`.
    pub(crate) fn new(envelope: ResourceEnvelope) -> Self {
        Self {
            envelope,
            raw: byte_budget(envelope.node_raw_bytes),
            output: byte_budget(envelope.node_output_bytes),
            compat_responses: byte_budget(envelope.compat_response_bytes),
            owners: OwnerLedger::default(),
        }
    }

    /// The count of this node's handshakes and streams, by phase.
    pub(crate) fn owners(&self) -> &OwnerLedger {
        &self.owners
    }

    /// What this node's owners hold now, in constant time.
    pub(crate) fn usage(&self) -> NodeUsage {
        NodeUsage {
            owners: self.owners.counts(),
            raw_bytes: self.raw.granted(),
            output_bytes: self.output.granted(),
            compat_response_bytes: self.compat_responses.granted(),
        }
    }

    /// Publish [`Self::usage`] under fixed metric labels, and count a breach of
    /// `bound`, so reconnect churn that outlives its admission slot stays visible.
    ///
    /// This only observes. It never refuses an owner, because a stuck worker that
    /// outlived its drain timeout already holds what it holds.
    pub(crate) fn report_usage(&self, bound: OwnerBound) -> NodeUsage {
        let usage = self.usage();
        for (budget, bytes) in usage.budget_bytes() {
            // A byte budget's size stays far below 2^53, so f64 represents it exactly.
            metrics::gauge!("zakura.p2p.resources.held_bytes", "budget" => budget)
                .set(bytes as f64);
        }
        if !bound.admits(&usage.owners) {
            metrics::counter!("zakura.p2p.owners.over_bound").increment(1);
            debug!(?usage, ?bound, "Zakura owners exceed their admission bound");
        }
        usage
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
            owner: self.node.owners.enter(OwnerPhase::Setup),
        }
    }

    /// Fund one outbound compatibility exchange of at most `bytes` from the node's
    /// partition. Cancelling the wait takes nothing.
    ///
    /// The exchange counts as an installed stream until its grant drops. A response
    /// cannot wait for bytes that it already holds, because this single grant covers
    /// the whole response before the stream opens.
    pub(crate) async fn admit_compat_response(&self, bytes: u64) -> CompatResponseGrant {
        let budget = &self.node.compat_responses;
        let bytes = budget.grant(budget.clamp(bytes)).await;
        CompatResponseGrant {
            _bytes: bytes,
            _owner: self.node.owners.enter(OwnerPhase::Installed),
        }
    }

    /// The node's compatibility partition.
    #[cfg(test)]
    pub(crate) fn compat_responses(&self) -> &OutputByteBudget {
        &self.node.compat_responses
    }
}

/// The funding of one outbound compatibility exchange. Dropping it returns the
/// exchange's bytes and ends its owner count.
#[derive(Debug)]
#[must_use = "dropping a compatibility grant releases its capacity"]
pub(crate) struct CompatResponseGrant {
    _bytes: OutputGrant,
    _owner: OwnerGuard,
}

/// One stream's admission gates and its place in the owner count.
#[derive(Debug)]
pub(crate) struct StreamResources {
    /// Admits raw inbound frames before the reader allocates their payloads.
    pub(crate) raw: FrameGate,
    /// Admits queued outbound frames that have no other owner.
    pub(crate) output: FrameGate,
    /// Counts this stream from setup until its reader has ended.
    pub(crate) owner: OwnerGuard,
}

#[cfg(test)]
impl NodeResources {
    /// Take every free node-wide raw and output byte until the returned grants drop.
    pub(crate) fn hold_node_budgets(&self) -> (Option<OutputGrant>, Option<OutputGrant>) {
        (self.raw.hold_free(), self.output.hold_free())
    }
}

#[cfg(test)]
impl TransportLimits {
    /// Limits with these windows and connections, and no effect from the rest.
    pub(crate) fn with_windows(window: u32, max_connections: usize) -> Self {
        Self {
            receive_window: window,
            send_window: u64::from(window),
            max_connections,
            max_pending_handshakes: 0,
            max_open_streams: 1,
            max_frame_bytes: 1,
        }
    }
}

#[cfg(test)]
impl ConnectionResources {
    /// Owners that never bind, for tests of other behavior.
    pub(crate) fn unconstrained() -> Self {
        NodeResources::new(ResourceEnvelope::new(TransportLimits::with_windows(
            u32::MAX,
            1 << 16,
        )))
        .connection()
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
                charge,
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
            charge,
            waited: started.elapsed(),
        }
    }

    /// Wait to fund a result of at most `frames` frames and `bytes` bytes.
    /// Cancelling the wait takes nothing.
    ///
    /// A one-frame result may use the floor slot, because the floor funds one frame.
    /// A larger result waits for the shared budgets, so a saturated node cannot hide
    /// a whole multi-frame response behind one floor slot.
    pub(crate) async fn admit_result(&self, frames: usize, bytes: u64) -> FrameGrant {
        if frames <= 1 {
            return self.admit(bytes).await;
        }
        let started = Instant::now();
        let charge = FrameCharge::shared(self.shared(bytes).await);
        FrameGrant {
            charge,
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
    charge: FrameCharge,
    waited: Duration,
}

impl FrameGrant {
    /// How long admission waited.
    pub(crate) fn waited(&self) -> Duration {
        self.waited
    }

    /// Return every shared byte above `bytes`, which wakes the budgets' waiters.
    /// A floor slot funds one frame of any size, so it stays as it is.
    pub(crate) fn shrink_to(&mut self, bytes: u64) {
        if let FrameCharge::Shared { node, connection } = &mut self.charge {
            node.shrink_to(bytes);
            connection.shrink_to(bytes);
        }
    }
}

#[derive(Debug)]
enum FrameCharge {
    /// Field order is drop order: the node's bytes return first, because
    /// admission takes the connection's bytes first.
    Shared {
        node: OutputGrant,
        connection: OutputGrant,
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
        Self::Shared { node, connection }
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
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct OwnerCounts {
    pub(crate) handshake: usize,
    pub(crate) setup: usize,
    pub(crate) installed: usize,
    pub(crate) retiring: usize,
}

impl OwnerCounts {
    /// Every owner, whichever phase it is in.
    #[cfg(test)]
    pub(crate) fn total(&self) -> usize {
        self.handshake + self.setup + self.installed + self.retiring
    }

    /// Every stream owner, whichever generation or phase it is in.
    pub(crate) fn streams(&self) -> usize {
        self.setup + self.installed + self.retiring
    }
}

/// The most owners that this node's admission limits allow at once.
///
/// A stream owner holds its stream permit through retirement, and a connection
/// returns its admission slot only after its stream owners settle. So setup,
/// installed, and retiring owners of every generation share one bound.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct OwnerBound {
    /// Control handshakes, which each hold a pending-handshake permit.
    pub(crate) handshakes: usize,
    /// Stream owners: each connection's stream permits, plus its one outbound
    /// compatibility exchange.
    pub(crate) streams: usize,
}

impl OwnerBound {
    /// The bound for these admission limits.
    pub(crate) fn new(
        max_pending_handshakes: usize,
        max_connections: usize,
        max_open_streams: u16,
    ) -> Self {
        Self {
            handshakes: max_pending_handshakes,
            streams: max_connections.saturating_mul(usize::from(max_open_streams) + 1),
        }
    }

    /// Whether `counts` fit this bound.
    pub(crate) fn admits(&self, counts: &OwnerCounts) -> bool {
        counts.handshake <= self.handshakes && counts.streams() <= self.streams
    }
}

/// What this node's transport owners hold at one moment.
///
/// Each field reads its own counter, so a read during churn can mix moments.
/// A floor frame holds a floor slot instead of bytes, and QUIC's buffers belong
/// to the peer memory budget, so neither appears here.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodeUsage {
    pub(crate) owners: OwnerCounts,
    pub(crate) raw_bytes: u64,
    pub(crate) output_bytes: u64,
    pub(crate) compat_response_bytes: u64,
}

impl NodeUsage {
    /// Each byte budget's fixed metric label and its held bytes.
    pub(crate) fn budget_bytes(&self) -> [(&'static str, u64); 3] {
        [
            ("raw", self.raw_bytes),
            ("output", self.output_bytes),
            ("compat_response", self.compat_response_bytes),
        ]
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
