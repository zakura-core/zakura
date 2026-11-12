//! sim-9.3: cross-family properties of the QUIC connection every service
//! shares with one peer (properties.md, Cross-family).
//!
//! They read the `load` rows a sampling transport tap logs on the node and
//! on each peer ([`TransportFact::Load`]), and the frames both ends read
//! and write. Each bound comes from the node's and the peers' production
//! limits ([`WireBounds`]).
//!
//! A run logs one row per frame, and a saturating run moves thousands of
//! frames, so the checks share one fold of the log ([`ConnectionFold`]),
//! which each check advances to its step, instead of folding the whole log
//! at every step.

use std::{cell::RefCell, collections::BTreeMap};

use zakura_network::zakura::ZakuraLocalLimits;

use crate::{
    observe::{Entry, TransportFact},
    property::{
        CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, TriggerHistory, Verdict,
    },
    Observation, ObservationLog,
};

/// While one service on a connection holds received frames, the peer never
/// blocks on the connection's receive credit.
pub const CREDIT_ISOLATED: &str = "session/credit-isolated";

/// While one service's writes fill a peer's send path, every other
/// service's frame on the connection reaches the node within one send
/// window of the other services' bytes.
pub const SEND_ISOLATED: &str = "session/send-isolated";

/// Each service's frame gates on a connection grant no more than that
/// service's shares, and free no more than their capacities. The check
/// reads the gates' own grants and free bytes, so a frame a service keeps
/// after its grant ends is out of its reach. A grant released twice leaves
/// its gate's free bytes above the capacity, which the check reads.
pub const CONNECTION_SHARES_BOUND: &str = "session/connection-shares-bound";

/// The production limits the properties judge against: the node's, and the
/// peers' QUIC send window.
#[derive(Clone, Debug)]
pub struct WireBounds {
    /// The node's limits, as `ZakuraLocalLimits::from_config` derives them
    /// from its config.
    pub node: ZakuraLocalLimits,
    /// The QUIC send window every peer runs with: the most unacknowledged
    /// stream bytes it keeps in flight and buffered on one connection.
    pub peer_send_window: u64,
}

/// What the properties read from a harness's view.
pub trait WireView {
    /// The bounds and the shared fold, or `None` for a run whose taps don't
    /// sample loads.
    fn wire(&self) -> Option<&WireState>;
}

/// The bounds and the fold a run's checks share.
#[derive(Debug)]
pub struct WireState {
    /// The bounds.
    pub bounds: WireBounds,
    fold: RefCell<ConnectionFold>,
}

impl WireState {
    /// A fresh fold under `bounds`.
    pub fn new(bounds: WireBounds) -> Self {
        WireState {
            bounds,
            fold: RefCell::default(),
        }
    }

    /// The fold advanced through `log`, and its findings at `step`.
    fn findings(&self, log: &ObservationLog, step: u64) -> StepFindings {
        let mut fold = self.fold.borrow_mut();
        fold.advance(&self.bounds, log);
        fold.at(step)
    }
}

/// The properties a wire harness with sampling taps reaches:
/// [`CREDIT_ISOLATED`] and [`CONNECTION_SHARES_BOUND`], both active, and
/// [`SEND_ISOLATED`], a draft until a planted bypass fails it.
pub fn wire_properties<S>() -> Vec<Property<S>>
where
    S: HasView,
    S::View: WireView,
{
    vec![
        Property {
            id: PropertyId::new(CREDIT_ISOLATED),
            claim: "while a service on a connection holds received frames, the peer never sends \
                    DATA_BLOCKED on it: the node's connection-level receive credit covers every \
                    service's share, so one service's held frames never stall another's",
            kind: Kind::Safety,
            // Every peer of a wire harness serves honestly, so its sends
            // stay within the credit each service grants.
            assumes: &[],
            // The taps' rows, and the view's bounds and fold.
            needs: &[Source::View, Source::Datagrams],
            status: Status::Active,
            check: check_credit_isolated::<S>,
            witness: Some(TriggerHistory::View(credit_held_past_handshake_share::<S>)),
        },
        Property {
            id: PropertyId::new(SEND_ISOLATED),
            claim: "a frame a peer writes on a connection reaches the node before the node reads \
                    more of other services' frame bytes on that connection than the peer's QUIC \
                    send window, plus one frame in progress on each other stream, plus each \
                    other stream's turn while QUIC sends the frame's own stream up to its end, \
                    unless the node's own gate for the frame's service holds it back",
            kind: Kind::Safety,
            assumes: &[],
            // The taps' rows, and the view's bounds and fold.
            needs: &[Source::View, Source::Datagrams],
            // A draft until a planted bypass fails it. Each service's worker
            // hands QUIC one frame at a time, and the saturated scenario's
            // mirrors never queue a second body on a block-sync stream ahead
            // of another service's frame. Raising block sync's send priority
            // and its stream's write-ahead from 64 KiB to 4 MiB together
            // keeps the bound: the longest wait is one body, 500,001 bytes
            // against a bound of 565,578.
            status: Status::Draft,
            check: check_send_isolated::<S>,
            witness: Some(TriggerHistory::View(written_under_saturation::<S>)),
        },
        Property {
            id: PropertyId::new(CONNECTION_SHARES_BOUND),
            claim: "on every connection, each service's raw inbound frame gate grants no more \
                    than its receive share and its output frame gate no more than its output \
                    share, as the node's reactor shares declare them, and neither gate frees \
                    more bytes than its capacity",
            kind: Kind::Safety,
            assumes: &[],
            // The taps' rows, and the view's bounds and fold.
            needs: &[Source::View, Source::Datagrams],
            status: Status::Active,
            check: check_connection_shares_bound::<S>,
            witness: Some(TriggerHistory::View(shares_bound_triggered::<S>)),
        },
    ]
}

/// One property's findings at one step.
#[derive(Clone, Debug, Default)]
struct Findings {
    /// Whether the step judged anything.
    judged: bool,
    /// Each violation, as expected and actual text.
    violations: Vec<(String, String)>,
    /// The first evidence the step found missing.
    missing: Option<String>,
}

/// Every property's findings at one step.
#[derive(Clone, Debug, Default)]
struct StepFindings {
    credit: Findings,
    send: Findings,
    shares: Findings,
}

/// One frame a peer started writing that the node hasn't read yet.
#[derive(Clone, Debug)]
struct Pending {
    kind: u16,
    message_type: u16,
    /// Whether the peer's send path was saturated when the write started.
    saturated: bool,
    /// Other services' frame payload bytes the node read since.
    others: u64,
    /// The largest of those frames on each node stream they came on.
    largest: BTreeMap<u64, u64>,
    /// This stream's frame payload bytes the node read since, up to and including
    /// this frame: what QUIC sends of this stream before the frame ends.
    own: u64,
    /// The most the node's gate for `kind` held since.
    held: u64,
}

/// A peer connection: the peer's name and its registration ID there.
type PeerConn = (String, u64);

/// The log folded so far.
#[derive(Debug, Default)]
struct ConnectionFold {
    /// The run and the entries folded.
    run: u64,
    folded: usize,
    /// The step whose findings `findings` holds.
    step: u64,
    findings: StepFindings,
    /// Each node connection's peer connection, joined by shared ID.
    node_shared: BTreeMap<String, u64>,
    peer_shared: BTreeMap<String, PeerConn>,
    peer_of: BTreeMap<u64, PeerConn>,
    /// Each node stream's kind, from the node's session rows.
    node_kinds: BTreeMap<(u64, u64), u16>,
    /// Each peer stream's kind, from the peer's admissions.
    peer_kinds: BTreeMap<(PeerConn, u64), u16>,
    /// Each node connection's `DATA_BLOCKED` count at its last load.
    data_blocked: BTreeMap<u64, u64>,
    /// Each node connection's kind that held more than the handshake share
    /// at its last load.
    holding: BTreeMap<u64, u16>,
    /// Each node connection's raw bytes held per kind at its last load.
    held: BTreeMap<(u64, u16), u64>,
    /// Each node connection's raw and output gates' free bytes per kind at
    /// its last load.
    free: BTreeMap<(u64, u16), (u64, u64)>,
    /// Each peer connection's saturation at its last load.
    saturated: BTreeMap<PeerConn, bool>,
    /// Each peer connection's pending writes, by stream and sequence.
    pending: BTreeMap<PeerConn, BTreeMap<(u64, u64), Pending>>,
    /// Whether a frame of one service was read while another held more
    /// than the handshake share on its connection.
    held_past_handshake: bool,
    /// Whether a service held more received frame bytes than another
    /// service's receive share on its connection.
    held_past_another_share: bool,
    /// Whether a gate's free bytes rose between two loads of its
    /// connection: a grant returned bytes.
    grant_returned: bool,
    /// Whether a write that started under saturation reached the node
    /// after other services' frames.
    written_under_saturation: bool,
}

impl ConnectionFold {
    /// Folds `log`'s entries past the last fold. A log of another run, or
    /// a shorter one, starts a fresh fold.
    fn advance(&mut self, bounds: &WireBounds, log: &ObservationLog) {
        let entries = log.entries();
        if self.run != log.run() || entries.len() < self.folded {
            *self = ConnectionFold {
                run: log.run(),
                ..ConnectionFold::default()
            };
        }
        for entry in &entries[self.folded..] {
            self.fold(bounds, entry);
        }
        self.folded = entries.len();
    }

    /// The findings at `step`, or none if the fold holds a later step's.
    fn at(&self, step: u64) -> StepFindings {
        if self.step == step {
            self.findings.clone()
        } else {
            StepFindings::default()
        }
    }

    fn fold(&mut self, bounds: &WireBounds, entry: &Entry) {
        if entry.step != self.step {
            self.step = entry.step;
            self.findings = StepFindings::default();
        }
        match &entry.obs {
            Observation::Session {
                connection,
                quic_stream,
                kind,
                ..
            } => {
                self.node_kinds.insert((*connection, *quic_stream), *kind);
            }
            Observation::Transport {
                instance: 0,
                connection: Some(connection),
                event,
                ..
            } => self.node(bounds, *connection, event),
            Observation::PeerTransport {
                peer,
                connection: Some(connection),
                event,
                ..
            } => self.peer((peer.clone(), *connection), event),
            _ => {}
        }
    }

    /// Folds one of the node's rows on `connection`.
    fn node(&mut self, bounds: &WireBounds, connection: u64, event: &TransportFact) {
        match event {
            TransportFact::SharedId { id } => {
                self.node_shared.insert(id.clone(), connection);
                if let Some(peer) = self.peer_shared.get(id) {
                    self.peer_of.insert(connection, peer.clone());
                }
            }
            TransportFact::Load {
                data_blocked,
                kinds,
                ..
            } => {
                self.credit(connection, *data_blocked);
                self.shares(bounds, connection, kinds);
            }
            TransportFact::HeaderRead {
                quic_stream,
                seq,
                message_type,
                payload_len,
                ..
            } => self.header_read(connection, *quic_stream, *seq, *message_type, *payload_len),
            TransportFact::FrameRead { quic_stream, seq } => {
                self.frame_read(bounds, connection, *quic_stream, *seq);
            }
            _ => {}
        }
    }

    /// Folds one of a peer's rows on `connection`.
    fn peer(&mut self, connection: PeerConn, event: &TransportFact) {
        match event {
            TransportFact::SharedId { id } => {
                if let Some(node) = self.node_shared.get(id) {
                    self.peer_of.insert(*node, connection.clone());
                }
                self.peer_shared.insert(id.clone(), connection);
            }
            TransportFact::Admitted {
                quic_stream, kind, ..
            } => {
                self.peer_kinds.insert((connection, *quic_stream), *kind);
            }
            TransportFact::Load {
                send_blocked_streams,
                ..
            } => {
                self.saturated.insert(connection, *send_blocked_streams > 0);
            }
            TransportFact::FrameWriteStarted {
                quic_stream,
                seq,
                message_type,
            } => {
                let Some(kind) = self.peer_kinds.get(&(connection.clone(), *quic_stream)) else {
                    return;
                };
                let pending = Pending {
                    kind: *kind,
                    message_type: *message_type,
                    saturated: self.saturated.get(&connection).copied().unwrap_or(false),
                    others: 0,
                    largest: BTreeMap::new(),
                    own: 0,
                    held: 0,
                };
                self.pending
                    .entry(connection)
                    .or_default()
                    .insert((*quic_stream, *seq), pending);
            }
            _ => {}
        }
    }

    /// [`CREDIT_ISOLATED`]: a load whose `DATA_BLOCKED` count grew.
    fn credit(&mut self, connection: u64, data_blocked: u64) {
        let last = self
            .data_blocked
            .insert(connection, data_blocked)
            .unwrap_or(0);
        self.findings.credit.judged = true;
        if data_blocked > last {
            self.findings.credit.violations.push((
                "the peer never blocks on the node's connection-level receive credit".to_owned(),
                format!(
                    "the peer sent {} DATA_BLOCKED on connection {connection}, {data_blocked} in all",
                    data_blocked - last
                ),
            ));
        }
    }

    /// [`CONNECTION_SHARES_BOUND`] on one load's kinds, and what the
    /// trigger histories of it and [`CREDIT_ISOLATED`] read.
    fn shares(&mut self, bounds: &WireBounds, connection: u64, kinds: &[crate::observe::KindLoad]) {
        let shares = bounds.node.reactor_shares();
        let handshake = u64::from(bounds.node.quic.receive_window_bytes);
        self.holding.remove(&connection);
        self.findings.shares.judged = true;
        for load in kinds {
            let share = shares.share(load.kind);
            self.held.insert((connection, load.kind), load.raw_held);
            let free = (
                load.raw_capacity
                    .saturating_sub(load.raw_held)
                    .saturating_add(load.raw_excess),
                load.output_capacity
                    .saturating_sub(load.output_held)
                    .saturating_add(load.output_excess),
            );
            if let Some(last) = self.free.insert((connection, load.kind), free) {
                self.grant_returned |= free.0 > last.0 || free.1 > last.1;
            }
            self.held_past_another_share |= kinds.iter().any(|other| {
                other.kind != load.kind && load.raw_held > shares.share(other.kind).receive
            });
            if load.raw_held > handshake {
                self.holding.insert(connection, load.kind);
            }
            for (what, held, bound, excess, capacity) in [
                (
                    "raw inbound gate",
                    load.raw_held,
                    share.receive,
                    load.raw_excess,
                    load.raw_capacity,
                ),
                (
                    "output gate",
                    load.output_held,
                    share.output,
                    load.output_excess,
                    load.output_capacity,
                ),
            ] {
                if held > bound {
                    self.findings.shares.violations.push((
                        format!("each service's {what} on a connection grants within its share"),
                        format!(
                            "kind {}'s {what} granted {held} bytes on connection {connection}, \
                             above its share of {bound}",
                            load.kind
                        ),
                    ));
                }
                // Only a grant released more than once frees bytes past the
                // capacity, and the held bytes then read as too few.
                if excess > 0 {
                    self.findings.shares.violations.push((
                        format!("each service's {what} on a connection frees within its capacity"),
                        format!(
                            "kind {}'s {what} freed {excess} bytes above its capacity of \
                             {capacity} on connection {connection}",
                            load.kind
                        ),
                    ));
                }
            }
        }
        if let Some(peer) = self.peer_of.get(&connection) {
            for pending in self
                .pending
                .get_mut(peer)
                .into_iter()
                .flat_map(|p| p.values_mut())
            {
                let held = self
                    .held
                    .get(&(connection, pending.kind))
                    .copied()
                    .unwrap_or(0);
                pending.held = pending.held.max(held);
            }
        }
    }

    /// Counts a frame of `kind` the node starts reading against every
    /// pending write of another kind on the connection.
    fn header_read(
        &mut self,
        connection: u64,
        quic_stream: u64,
        seq: u64,
        message_type: u16,
        payload_len: u32,
    ) {
        let Some(peer) = self.peer_of.get(&connection) else {
            return;
        };
        let Some(pending) = self.pending.get_mut(peer) else {
            return;
        };
        if let Some(write) = pending.get(&(quic_stream, seq)) {
            if write.message_type != message_type {
                self.findings.send.missing.get_or_insert_with(|| {
                    format!(
                        "connection {connection}'s stream {quic_stream} frame {seq} reads as \
                         message type {message_type}, but the peer wrote type {}",
                        write.message_type
                    )
                });
            }
        }
        let Some(kind) = self.node_kinds.get(&(connection, quic_stream)).copied() else {
            return;
        };
        let bytes = u64::from(payload_len);
        for ((stream, frame), write) in pending.iter_mut() {
            if *stream == quic_stream && *frame >= seq {
                write.own = write.own.saturating_add(bytes);
            } else if write.kind != kind {
                write.others = write.others.saturating_add(bytes);
                let largest = write.largest.entry(quic_stream).or_default();
                *largest = (*largest).max(bytes);
            }
        }
    }

    /// Settles the pending write the node finished reading, and notes a
    /// read of one kind while another holds past the handshake share.
    fn frame_read(&mut self, bounds: &WireBounds, connection: u64, quic_stream: u64, seq: u64) {
        let kind = self.node_kinds.get(&(connection, quic_stream)).copied();
        if let (Some(holder), Some(kind)) = (self.holding.get(&connection), kind) {
            self.held_past_handshake |= *holder != kind;
        }
        let Some(peer) = self.peer_of.get(&connection).cloned() else {
            return;
        };
        let Some(write) = self
            .pending
            .get_mut(&peer)
            .and_then(|pending| pending.remove(&(quic_stream, seq)))
        else {
            return;
        };
        let share = bounds.node.reactor_shares().share(write.kind).receive;
        // The node's own gate held the frame back: its kind's held frames
        // left no room for one more frame.
        if write
            .held
            .saturating_add(u64::from(bounds.node.max_frame_bytes))
            > share
            && write.held > 0
        {
            return;
        }
        self.written_under_saturation |= write.saturated && write.others > 0;
        // What was in flight when the write started, then one frame in
        // progress on each other stream, then the other streams' turns
        // while QUIC sends this stream's bytes up to the frame's end.
        // Widening usize to u64 is lossless on supported targets.
        let streams = write.largest.len() as u64;
        let bound = bounds
            .peer_send_window
            .saturating_add(write.largest.values().sum::<u64>())
            .saturating_add(write.own.saturating_mul(streams));
        self.findings.send.judged = true;
        if write.others > bound {
            self.findings.send.violations.push((
                format!(
                    "a peer's frame reaches the node within {bound} bytes of other services' \
                     frames on its connection"
                ),
                format!(
                    "{} wrote frame {seq} of kind {} on stream {quic_stream}, and the node read \
                     {} bytes of other services' frames on {streams} streams of connection \
                     {connection} before it",
                    peer.0, write.kind, write.others
                ),
            ));
        }
    }
}

/// `findings` as a verdict.
fn verdict<S: HasView>(cx: &mut CheckCtx<'_, S>, findings: Findings) -> Verdict {
    if let Some(detail) = findings.missing {
        cx.missing_evidence(detail);
        return Verdict::NotApplicable;
    }
    let mut violations = findings.violations.into_iter();
    match violations.next() {
        Some((expected, actual)) => {
            for (expected, actual) in violations {
                cx.record_violation(expected, actual);
            }
            cx.violated(expected, actual)
        }
        None if findings.judged => Verdict::Holds,
        None => Verdict::NotApplicable,
    }
}

/// The view's findings at the check's step, or `None` for a run that
/// samples no loads.
fn step_findings<S>(cx: &CheckCtx<'_, S>) -> Option<StepFindings>
where
    S: HasView,
    S::View: WireView,
{
    let view = cx.view();
    let wire = view.wire()?;
    Some(wire.findings(cx.log, cx.step))
}

/// Checks [`CREDIT_ISOLATED`] at each step with a node `load` row. A row
/// whose `DATA_BLOCKED` count grew violates it.
fn check_credit_isolated<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: WireView,
{
    match step_findings(cx) {
        Some(findings) => verdict(cx, findings.credit),
        None => Verdict::NotApplicable,
    }
}

/// Checks [`SEND_ISOLATED`] at each step where the node finishes reading
/// a frame a sampled peer wrote.
fn check_send_isolated<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: WireView,
{
    match step_findings(cx) {
        Some(findings) => verdict(cx, findings.send),
        None => Verdict::NotApplicable,
    }
}

/// Checks [`CONNECTION_SHARES_BOUND`] at each step with a node `load` row.
fn check_connection_shares_bound<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: WireView,
{
    match step_findings(cx) {
        Some(findings) => verdict(cx, findings.shares),
        None => Verdict::NotApplicable,
    }
}

/// [`CREDIT_ISOLATED`]'s trigger history: the node read a frame of one service while
/// another service on the connection held more received frame bytes than
/// the handshake share, so without that service's own share the held
/// frames would have taken the whole window.
fn credit_held_past_handshake_share<S>(view: &S::View, log: &ObservationLog) -> bool
where
    S: HasView,
    S::View: WireView,
{
    view.wire().is_some_and(|wire| {
        let mut fold = wire.fold.borrow_mut();
        fold.advance(&wire.bounds, log);
        fold.held_past_handshake
    })
}

/// [`CONNECTION_SHARES_BOUND`]'s trigger history: a service on a
/// connection held more received frame bytes than another service's
/// receive share there, so a gate the two shared would have let one take
/// the other's share; or a gate's free bytes rose between two loads, so a
/// grant returned bytes and a second return would free them twice.
fn shares_bound_triggered<S>(view: &S::View, log: &ObservationLog) -> bool
where
    S: HasView,
    S::View: WireView,
{
    view.wire().is_some_and(|wire| {
        let mut fold = wire.fold.borrow_mut();
        fold.advance(&wire.bounds, log);
        fold.held_past_another_share || fold.grant_returned
    })
}

/// [`SEND_ISOLATED`]'s trigger history: a frame a peer started writing
/// while its connection had a write waiting on flow control or the send
/// window reached the node after frames of other services.
fn written_under_saturation<S>(view: &S::View, log: &ObservationLog) -> bool
where
    S: HasView,
    S::View: WireView,
{
    view.wire().is_some_and(|wire| {
        let mut fold = wire.fold.borrow_mut();
        fold.advance(&wire.bounds, log);
        fold.written_under_saturation
    })
}
