//! Transport owners of connections, raw frames, and unfunded output.
//!
//! [`ReactorShares`] gives each stream kind, the reactor that owns it, a
//! receive share and an output share on every connection. [`NodeResources`]
//! turns them into gates once, and each connection builds its own gates for
//! the kinds it negotiated.
//!
//! - A [`FrameGate`] admits one frame's bytes before the frame's memory exists.
//!   The reader asks before allocating a payload, and a sender asks before
//!   queueing a frame that has no other owner. Every stream of one kind on one
//!   connection shares that kind's gates, so a kind's retained frames can
//!   delay only that kind's frames on that connection.
//! - A [`FrameGrant`] travels with its frame. A raw grant stays with the
//!   application's receiver through decoding, until its next receive. It also
//!   holds the frame payload's QUIC receive credit (zakura-quic API-10), so the
//!   peer can't send those bytes again until the grant drops. An output grant
//!   stays with the queued frame until its transport write ends. Dropping
//!   either, including through cancellation, returns the bytes.
//! - An [`OwnerGuard`] counts one handshake or stream in one [`OwnerPhase`], so
//!   a reader of [`OwnerLedger`] sees setup, installed, and retiring owners
//!   together across reconnects.
//! - A [`CompatResponseGrant`] funds one outbound compatibility exchange from
//!   its connection's exchange gate. The requester holds it from before it
//!   opens the stream until its decoder finishes, so a response's frames and
//!   reassembly copies never wait on the gates of the connection's stream kinds.
//!   The grant can outlive its connection, but not its requester's client slot.
//!
//! One connection's QUIC receive window is the handshake share plus the
//! receive share of every kind it negotiated. Unread QUIC bytes and held raw
//! frames together stay within that window, and the raw gates keep at least the
//! handshake share free for unread bytes, so a conformant peer keeps the
//! credit it had before this node held any frame.
//!
//! Waiting here is local backpressure. It never faults a peer, and it never
//! comes from another connection's traffic: no gate spans connections.
//!
//! A request stream funds its response from its kind's output gate before its
//! service runs, so no materialized result waits unfunded. Each execution also runs on a
//! stream that holds one of its connection's `max_open_streams` permits. A
//! connection keeps its slot until its stream workers settle or teardown's bounded
//! drain ends. A worker that outlasts the drain stays in the [`OwnerLedger`] until
//! its task ends.

use std::sync::Arc;
use std::time::Duration;

use zakura_quic::{CreditHold, QuicConfig, RecvStream};
use zakura_rt::{
    sync::counter::{AtomicUsize, Ordering},
    time::Instant,
};

use crate::zakura::{
    regulation::{OutputByteBudget, OutputGrant},
    ReactorBudget, Stream, FRAME_HEADER_BYTES,
};
use zakura_chain::serialization::MAX_PROTOCOL_MESSAGE_LEN;

/// The most response frame bytes one compatibility exchange may retain, on either
/// side.
///
/// A request can name up to `MAX_TX_INV_IN_SENT_MESSAGE` hashes. Without this cap,
/// `encode_response` would retain a whole multi-frame response before its first write,
/// and a hostile responder could fill a requester with tens of GiB of accepted frames
/// before any decode. The local service emits far less, because `zakurad` caps
/// `getdata` at about 1 MiB, so an honest peer never exceeds it.
pub(crate) const COMPAT_RESPONSE_MAX_AGGREGATE_BYTES: usize = 8 * MAX_PROTOCOL_MESSAGE_LEN;

/// The most bytes one compatibility exchange may charge: its frames, plus the
/// validator's and the decoder's reassembly copies of one object.
pub(crate) const COMPAT_EXCHANGE_MAX_BYTES: u64 =
    (COMPAT_RESPONSE_MAX_AGGREGATE_BYTES + 2 * MAX_PROTOCOL_MESSAGE_LEN) as u64;

/// The outbound compatibility exchanges one connection may hold: one exchange
/// at [`COMPAT_EXCHANGE_MAX_BYTES`].
///
/// A connection's serving loop runs one exchange at a time, so one exchange is
/// on the wire while earlier ones finish decoding. A one-object exchange at the
/// protocol's message limit charges three message lengths, so a connection can
/// decode two of those while it fetches a third. A request writes its one frame
/// straight to its stream, so the exchange queues no output.
///
/// A grant can outlive its connection while its requester decodes, so a
/// replacement connection's gate runs beside it. The node bound therefore comes
/// from the compatibility client's concurrent requests, which each hold at most
/// one grant, not from the connection count.
pub const COMPAT_EXCHANGE_BUDGET: ReactorBudget = ReactorBudget {
    receive: COMPAT_EXCHANGE_MAX_BYTES,
    output: 0,
};

#[cfg(test)]
mod tests;

/// The output share a stream kind gets on each connection unless
/// [`ReactorShares`] names it: 64 MiB, the whole connection's output budget
/// before per-kind gates.
pub const DEFAULT_REACTOR_OUTPUT_BYTES: u64 = 64 * 1024 * 1024;

/// Each reactor's receive and output share on one connection, keyed by the
/// stream kind the reactor owns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactorShares {
    /// The QUIC receive window of a connection before it negotiates its
    /// streams. Every connection keeps it, for the control stream.
    pub control_receive_bytes: u64,
    /// The shares of the stream kinds this list names.
    pub kinds: Vec<(u16, ReactorBudget)>,
    /// The share of every stream kind that `kinds` does not name.
    pub other: ReactorBudget,
}

impl ReactorShares {
    /// Shares that keep `quic`'s windows: the handshake window is the
    /// configured connection window, and each kind may hold one stream window
    /// of received frames and [`DEFAULT_REACTOR_OUTPUT_BYTES`] of output.
    pub fn from_quic(quic: &QuicConfig) -> Self {
        Self {
            control_receive_bytes: u64::from(quic.receive_window_bytes),
            kinds: Vec::new(),
            other: ReactorBudget {
                receive: u64::from(quic.stream_receive_window_bytes),
                output: DEFAULT_REACTOR_OUTPUT_BYTES,
            },
        }
    }

    /// The share of stream `kind`.
    pub fn share(&self, kind: u16) -> ReactorBudget {
        self.kinds
            .iter()
            .find(|(named, _)| *named == kind)
            .map_or(self.other, |(_, share)| *share)
    }

    /// The QUIC receive window of a connection that negotiated `kinds`: the
    /// handshake window plus each kind's receive share.
    pub fn receive_window(&self, kinds: impl IntoIterator<Item = u16>) -> u64 {
        kinds
            .into_iter()
            .fold(self.control_receive_bytes, |window, kind| {
                window.saturating_add(self.share(kind).receive)
            })
    }

    /// The QUIC stream receive window: the largest receive share, so one
    /// stream can carry its whole kind's share.
    pub fn stream_receive_window(&self) -> u64 {
        self.kinds
            .iter()
            .map(|(_, share)| share.receive)
            .fold(self.other.receive, u64::max)
    }
}

/// The transport limits that [`ResourceEnvelope`] derives from.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransportLimits {
    /// The QUIC receive window of a connection that hasn't negotiated.
    pub(crate) control_receive_bytes: u64,
    /// The shares of one connection that negotiated every registered kind.
    pub(crate) connection_shares: ReactorBudget,
    /// The outbound compatibility exchange bytes the node may hold:
    /// [`COMPAT_EXCHANGE_BUDGET`]'s receive share per concurrent request of its
    /// compatibility client on a node that registers the legacy request kind, and
    /// zero otherwise.
    pub(crate) compat_exchange_bytes: u64,
    /// The QUIC send window of one connection.
    pub(crate) send_window: u64,
    /// Connections the node admits.
    pub(crate) max_connections: usize,
    /// QUIC connections the node holds beyond the admitted ones.
    pub(crate) max_pending_handshakes: usize,
}

/// The node's transport byte bounds, derived from its transport limits.
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
    /// Outbound compatibility exchange bytes that the node may retain.
    pub(crate) compat_response_bytes: u64,
    /// The QUIC windows of every live connection, in bytes.
    pub(crate) transport_credit_bytes: u128,
}

impl ResourceEnvelope {
    /// Bounds for a node with these transport limits.
    ///
    /// A connection's budgets are the sums of its kinds' shares, and the node's
    /// are the connections' sums, except the compatibility exchange bytes, which
    /// the limits give for the whole node. Admitted connections hold the full
    /// window, and pending handshakes hold the handshake window.
    pub(crate) fn new(limits: TransportLimits) -> Self {
        let connections = limits.max_connections.max(1);
        // Widening usize to u64 or u128 is lossless on supported targets.
        let node_bytes =
            |connection_bytes: u64| connection_bytes.saturating_mul(connections as u64);
        let shares = limits.connection_shares;
        let window = |receive: u64| u128::from(receive) + u128::from(limits.send_window);
        Self {
            connection_raw_bytes: shares.receive,
            node_raw_bytes: node_bytes(shares.receive),
            connection_output_bytes: shares.output,
            node_output_bytes: node_bytes(shares.output),
            compat_response_bytes: limits.compat_exchange_bytes,
            transport_credit_bytes: connections as u128
                * window(limits.control_receive_bytes.saturating_add(shares.receive))
                + limits.max_pending_handshakes as u128 * window(limits.control_receive_bytes),
        }
    }

    /// The most frame bytes these bounds allow this module's owners to retain:
    /// the kinds' gates and the compatibility exchange gates.
    pub(crate) fn retained_frame_bytes(&self) -> u128 {
        u128::from(self.node_raw_bytes)
            + u128::from(self.node_output_bytes)
            + u128::from(self.compat_response_bytes)
    }

    /// The most transport memory these bounds allow: retained frames, plus the
    /// bytes QUIC buffers under the windows.
    ///
    /// A held raw frame's bytes also sit inside its connection's receive
    /// window, so the sum counts them twice. The second count funds a payload
    /// buffer that the reader allocates before its bytes arrive.
    pub(crate) fn transport_memory_bytes(&self) -> u128 {
        self.retained_frame_bytes() + self.transport_credit_bytes
    }
}

/// The node-wide owners of transport resources.
#[derive(Clone, Debug)]
pub(crate) struct NodeResources {
    shares: Arc<ReactorShares>,
    /// Each registered kind, the capability bits of its streams, and its
    /// largest frame.
    kinds: Arc<[(u16, u64, u64)]>,
    owners: OwnerLedger,
}

impl NodeResources {
    /// Owners for `streams` under `shares`, where no frame exceeds
    /// `max_frame_bytes`.
    ///
    /// # Panics
    ///
    /// If a kind's receive or output share can't hold its largest frame. A
    /// frame above its gate takes the whole gate, so it would be charged less
    /// than it holds.
    pub(crate) fn new(
        shares: ReactorShares,
        streams: impl IntoIterator<Item = Stream>,
        max_frame_bytes: u32,
    ) -> Self {
        let mut kinds: Vec<(u16, u64, u64)> = Vec::new();
        for stream in streams {
            let frame = u64::from(stream.frame_cap.min(max_frame_bytes));
            match kinds.iter_mut().find(|(kind, ..)| *kind == stream.kind) {
                Some((_, capabilities, largest)) => {
                    *capabilities |= stream.capability;
                    *largest = (*largest).max(frame);
                }
                None => kinds.push((stream.kind, stream.capability, frame)),
            }
        }
        for (kind, _, largest) in &kinds {
            let ReactorBudget { receive, output } = shares.share(*kind);
            assert!(
                receive >= *largest,
                "stream kind {kind}'s receive share of {receive} bytes must hold its \
                 largest frame of {largest} bytes"
            );
            assert!(
                output >= *largest,
                "stream kind {kind}'s output share of {output} bytes must hold its \
                 largest frame of {largest} bytes"
            );
        }
        Self {
            shares: Arc::new(shares),
            kinds: kinds.into(),
            owners: OwnerLedger::default(),
        }
    }

    /// The count of this node's handshakes and streams, by phase.
    pub(crate) fn owners(&self) -> &OwnerLedger {
        &self.owners
    }

    /// Fresh gates for a connection that negotiated `capabilities`, one pair
    /// per kind it may carry.
    pub(crate) fn connection(&self, capabilities: u64) -> ConnectionResources {
        let gates: Vec<_> = self
            .kinds
            .iter()
            .filter(|(_, kind_capabilities, _)| capabilities & kind_capabilities != 0)
            .map(|(kind, ..)| (*kind, KindGates::new(self.shares.share(*kind))))
            .collect();
        ConnectionResources {
            owners: self.owners.clone(),
            receive_window: self
                .shares
                .receive_window(gates.iter().map(|(kind, _)| *kind)),
            gates: gates.into(),
            compat_exchanges: FrameGate::new(COMPAT_EXCHANGE_BUDGET.receive),
        }
    }
}

/// One kind's gates on one connection.
#[derive(Clone, Debug)]
struct KindGates {
    raw: FrameGate,
    output: FrameGate,
}

impl KindGates {
    fn new(share: ReactorBudget) -> Self {
        Self {
            raw: FrameGate::new(share.receive),
            output: FrameGate::new(share.output),
        }
    }
}

/// A byte budget whose size comes from a reactor share.
fn byte_budget(bytes: u64) -> OutputByteBudget {
    OutputByteBudget::new(bytes.clamp(1, zakura_rt::sync::Semaphore::MAX_PERMITS as u64))
        .expect("the clamp keeps each byte budget within the semaphore's range")
}

/// One connection's gates, by stream kind.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionResources {
    owners: OwnerLedger,
    gates: Arc<[(u16, KindGates)]>,
    receive_window: u64,
    /// Funds this connection's outbound compatibility exchanges.
    compat_exchanges: FrameGate,
}

impl ConnectionResources {
    /// The QUIC receive window this connection's kinds need: the handshake
    /// window plus each kind's receive share.
    pub(crate) fn receive_window(&self) -> u64 {
        self.receive_window
    }

    /// Count one stream that entered setup, before its kind is known.
    pub(crate) fn owner(&self) -> OwnerGuard {
        self.owners.enter(OwnerPhase::Setup)
    }

    /// The gates of a stream of `kind` that `owner` counts, or `None` if this
    /// connection didn't negotiate `kind`.
    pub(crate) fn stream(&self, kind: u16, owner: OwnerGuard) -> Option<StreamResources> {
        let (_, gates) = self.gates.iter().find(|(gated, _)| *gated == kind)?;
        Some(StreamResources {
            raw: gates.raw.clone(),
            output: gates.output.clone(),
            owner,
        })
    }

    /// Fund one outbound compatibility exchange of at most `bytes` from this
    /// connection's exchange gate. Cancelling the wait takes nothing.
    ///
    /// The exchange counts as an installed stream until its grant drops. A response
    /// cannot wait for bytes that it already holds, because this single grant covers
    /// the whole response before the stream opens. An exchange above the gate takes
    /// the whole gate, so it waits for the connection's earlier exchanges to finish
    /// decoding.
    pub(crate) async fn admit_compat_response(&self, bytes: u64) -> CompatResponseGrant {
        CompatResponseGrant {
            _bytes: self.compat_exchanges.admit(bytes).await,
            _owner: self.owners.enter(OwnerPhase::Installed),
        }
    }

    /// The budget of this connection's compatibility exchange gate.
    #[cfg(test)]
    pub(crate) fn compat_exchanges(&self) -> &OutputByteBudget {
        &self.compat_exchanges.budget
    }
}

/// The funding of one outbound compatibility exchange. Dropping it returns the
/// exchange's bytes and ends its owner count.
///
/// The connection's exchange ends when its requester gives up, and the requester
/// drops the grant once it decodes, so each grant lives within one request of the
/// compatibility client, even after its connection closes.
#[derive(Debug)]
#[must_use = "dropping a compatibility grant releases its capacity"]
pub(crate) struct CompatResponseGrant {
    _bytes: FrameGrant,
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
impl ConnectionResources {
    /// Gates that never bind for streams of `kinds`, for tests of other
    /// behavior.
    pub(crate) fn unconstrained(kinds: &[u16]) -> Self {
        let share = ReactorBudget {
            receive: 1 << 30,
            output: 1 << 30,
        };
        Self {
            owners: OwnerLedger::default(),
            gates: kinds
                .iter()
                .map(|kind| (*kind, KindGates::new(share)))
                .collect(),
            receive_window: u64::MAX,
            compat_exchanges: FrameGate::new(COMPAT_EXCHANGE_BUDGET.receive),
        }
    }
}

#[cfg(test)]
impl FrameGate {
    /// A gate over `budget`, for tests that read the budget.
    pub(crate) fn over(budget: OutputByteBudget) -> Self {
        Self { budget }
    }
}

#[cfg(test)]
impl StreamResources {
    /// Owners that never bind, for tests of other behavior.
    pub(crate) fn unconstrained() -> Self {
        let connection = ConnectionResources::unconstrained(&[0]);
        connection
            .stream(0, connection.owner())
            .expect("the test connection gates kind 0")
    }
}

/// Admits frames of one kind on one connection against that kind's share.
#[derive(Clone, Debug)]
pub(crate) struct FrameGate {
    budget: OutputByteBudget,
}

impl FrameGate {
    /// A gate of `bytes`.
    fn new(bytes: u64) -> Self {
        Self {
            budget: byte_budget(bytes),
        }
    }

    /// The bytes this gate can grant at once.
    pub(crate) fn capacity(&self) -> u64 {
        self.budget.capacity()
    }

    /// Admit a frame of `bytes` without waiting, if the gate has room.
    pub(crate) fn try_admit(&self, bytes: u64) -> Option<FrameGrant> {
        let charge = self.budget.clamp(bytes);
        self.budget
            .try_grant(charge)
            .map(|grant| FrameGrant::new(grant, charge, Duration::ZERO))
    }

    /// Wait for room for a frame of `bytes`. Cancelling the wait takes nothing.
    ///
    /// A frame above the gate takes the whole gate, so it waits for every
    /// other frame of its kind instead of waiting forever.
    pub(crate) async fn admit(&self, bytes: u64) -> FrameGrant {
        if let Some(grant) = self.try_admit(bytes) {
            return grant;
        }
        let started = Instant::now();
        let charge = self.budget.clamp(bytes);
        let grant = self.budget.grant(charge).await;
        FrameGrant::new(grant, charge, started.elapsed())
    }
}

/// The encoded size of a frame with `payload_len` payload bytes.
pub(crate) fn frame_wire_bytes(payload_len: usize) -> u64 {
    // Widening usize to u64 is lossless on supported targets.
    (payload_len as u64).saturating_add(FRAME_HEADER_BYTES as u64)
}

/// The admission of one frame. Dropping it returns the frame's capacity and
/// any QUIC credit it holds.
#[derive(Debug)]
#[must_use = "dropping a frame grant releases its capacity"]
pub(crate) struct FrameGrant {
    /// Field order is drop order: the credit returns first.
    credit: Option<CreditHold>,
    granted: OutputGrant,
    charge: u32,
    waited: Duration,
}

impl FrameGrant {
    fn new(charge: OutputGrant, bytes: u32, waited: Duration) -> Self {
        Self {
            credit: None,
            granted: charge,
            charge: bytes,
            waited,
        }
    }

    /// How long admission waited.
    pub(crate) fn waited(&self) -> Duration {
        self.waited
    }

    /// Return every byte above `bytes` to the gate, which wakes its waiters.
    /// A smaller grant stays as it is.
    pub(crate) fn shrink_to(&mut self, bytes: u64) {
        self.granted.shrink_to(bytes);
        self.charge = self.charge.min(u32::try_from(bytes).unwrap_or(u32::MAX));
    }

    /// Keep the QUIC receive credit of the payload that `recv` reads next
    /// from the peer until this grant drops.
    ///
    /// The hold is at most this grant's charge, so a connection's holds never
    /// exceed its kinds' receive shares, and the handshake share stays free.
    pub(crate) fn hold_credit(&mut self, recv: &mut RecvStream, payload_len: usize) {
        // Widening usize to u64 is lossless on supported targets.
        let bytes = (payload_len as u64).min(u64::from(self.charge));
        self.credit = Some(recv.hold_credit(bytes));
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
