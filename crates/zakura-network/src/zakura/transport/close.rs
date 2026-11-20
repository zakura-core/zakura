//! Why the node closed a connection.
//!
//! Every close records one [`CloseOutcome`]. A fault names the close-spec
//! row the peer broke, on the stream kind and version it broke it on. Any
//! other close is neutral: it assigns the peer no fault. The wire reset and
//! close codes don't carry the outcome.

use std::fmt;

/// One row of a close spec, by its catalogue ID, such as `bsync-close-3`.
///
/// Only the constants below construct a row, so a reader matches a row by
/// equality and never parses text.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CloseRow(&'static str);

impl CloseRow {
    /// The row's catalogue ID.
    pub const fn id(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for CloseRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// The connection close spec's rows that name a fault.
impl CloseRow {
    /// A persistent stream's prelude carries a request ID.
    pub const CONN_CLOSE_14: Self = Self("conn-close-14");
    /// A request stream has no request ID.
    pub const CONN_CLOSE_15: Self = Self("conn-close-15");
    /// The prelude's frame cap has no room for a payload.
    pub const CONN_CLOSE_16: Self = Self("conn-close-16");
    /// A session's streams don't name one valid session.
    pub const CONN_CLOSE_17: Self = Self("conn-close-17");
    /// A frame breaks its stream's message cap, its byte rate, or ends before
    /// its declared length.
    pub const CONN_CLOSE_18: Self = Self("conn-close-18");
    /// A request exceeds the message cap.
    pub const CONN_CLOSE_22: Self = Self("conn-close-22");
    /// The service rejects a request as a protocol error.
    pub const CONN_CLOSE_23: Self = Self("conn-close-23");
    /// The service exits with a protocol error.
    pub const CONN_CLOSE_24: Self = Self("conn-close-24");
}

/// The regulation close spec's rows: the generic rules every tabled stream
/// shares.
impl CloseRow {
    /// A frame longer than its row allows.
    pub const REG_CLOSE_1: Self = Self("reg-close-1");
    /// A message type with no row on this reader.
    pub const REG_CLOSE_2: Self = Self("reg-close-2");
    /// Nonzero flags.
    pub const REG_CLOSE_3: Self = Self("reg-close-3");
    /// A payload below its row's minimum.
    pub const REG_CLOSE_4: Self = Self("reg-close-4");
    /// More open requests than twice the advertised limit.
    pub const REG_CLOSE_5: Self = Self("reg-close-5");
    /// A response with no live reservation.
    pub const REG_CLOSE_6: Self = Self("reg-close-6");
    /// A response past its reservation's frame or byte budget.
    pub const REG_CLOSE_7: Self = Self("reg-close-7");
    /// Queued responses past what the live reservations owe.
    pub const REG_CLOSE_8: Self = Self("reg-close-8");
    /// A response to another request, or an ending claimed as a part.
    pub const REG_CLOSE_9: Self = Self("reg-close-9");
    /// A subscription message for no live subscription.
    pub const REG_CLOSE_10: Self = Self("reg-close-10");
    /// A page above its credit, or with no object.
    pub const REG_CLOSE_11: Self = Self("reg-close-11");
    /// An update out of sequence.
    pub const REG_CLOSE_12: Self = Self("reg-close-12");
    /// An acknowledgement of no sent page.
    pub const REG_CLOSE_13: Self = Self("reg-close-13");
    /// An `Open` or `Grant` that adds no credit.
    pub const REG_CLOSE_14: Self = Self("reg-close-14");
    /// A grant past the row's credit window.
    pub const REG_CLOSE_15: Self = Self("reg-close-15");
    /// A crossed grant past the ended subscription's window.
    pub const REG_CLOSE_16: Self = Self("reg-close-16");
    /// An update after `Close`.
    pub const REG_CLOSE_17: Self = Self("reg-close-17");
    /// An `Open` that reuses a live or tombstoned key.
    pub const REG_CLOSE_18: Self = Self("reg-close-18");
    /// An `Open` past twice `max_live` slots.
    pub const REG_CLOSE_19: Self = Self("reg-close-19");
}

/// The discovery close spec's rows that name a fault.
impl CloseRow {
    /// A `Hello` body that fails decode.
    pub const DISCO_CLOSE_5: Self = Self("disco-close-5");
    /// A `Hello` authored by another node than the connection's.
    pub const DISCO_CLOSE_6: Self = Self("disco-close-6");
    /// A `Hello` with an invalid signature, network, chain or protocol.
    pub const DISCO_CLOSE_7: Self = Self("disco-close-7");
    /// A `GetPeers` that fails decode.
    pub const DISCO_CLOSE_8: Self = Self("disco-close-8");
    /// A `Peers` answer that fails decode.
    pub const DISCO_CLOSE_12: Self = Self("disco-close-12");
    /// A `Peers` answer with more records than its query's limit.
    pub const DISCO_CLOSE_13: Self = Self("disco-close-13");
    /// A `GetServices` or `Services` that fails decode.
    pub const DISCO_CLOSE_14: Self = Self("disco-close-14");
    /// A `Services` answer with a summary its query didn't ask for.
    pub const DISCO_CLOSE_15: Self = Self("disco-close-15");
    /// A `Services` answer authored by another node than the peer.
    pub const DISCO_CLOSE_16: Self = Self("disco-close-16");
    /// A relayed record with an invalid signature, network, chain or body.
    pub const DISCO_CLOSE_17: Self = Self("disco-close-17");
}

/// The header-sync close spec's rows that name a fault.
impl CloseRow {
    /// A `Status` that fails decode.
    pub const HSYNC_CLOSE_3: Self = Self("hsync-close-3");
    /// A `SubscribeHeaders` that fails decode.
    pub const HSYNC_CLOSE_4: Self = Self("hsync-close-4");
    /// A `Headers` page that fails decode.
    pub const HSYNC_CLOSE_5: Self = Self("hsync-close-5");
    /// A tree-aux record with the wrong height or defaults.
    pub const HSYNC_CLOSE_6: Self = Self("hsync-close-6");
    /// A `HeadersOutcome` that fails decode.
    pub const HSYNC_CLOSE_7: Self = Self("hsync-close-7");
    /// A page above the latest `Status` caps.
    pub const HSYNC_CLOSE_10: Self = Self("hsync-close-10");
    /// A message that names another initial target.
    pub const HSYNC_CLOSE_11: Self = Self("hsync-close-11");
    /// A page whose schema differs from its subscription's.
    pub const HSYNC_CLOSE_12: Self = Self("hsync-close-12");
    /// A first page that extends no sent locator, or not the target's.
    pub const HSYNC_CLOSE_13: Self = Self("hsync-close-13");
    /// A later page that doesn't extend the receive cursor.
    pub const HSYNC_CLOSE_14: Self = Self("hsync-close-14");
    /// Headers that don't chain from their parent.
    pub const HSYNC_CLOSE_15: Self = Self("hsync-close-15");
    /// A page that passes the target.
    pub const HSYNC_CLOSE_16: Self = Self("hsync-close-16");
    /// A path that passes a higher sent locator.
    pub const HSYNC_CLOSE_17: Self = Self("hsync-close-17");
    /// A page that reaches the target at another height than advertised.
    pub const HSYNC_CLOSE_18: Self = Self("hsync-close-18");
    /// A target marker that disagrees with the page's last header.
    pub const HSYNC_CLOSE_19: Self = Self("hsync-close-19");
    /// A second target marker.
    pub const HSYNC_CLOSE_20: Self = Self("hsync-close-20");
    /// An outcome outside its window.
    pub const HSYNC_CLOSE_21: Self = Self("hsync-close-21");
    /// A `Grant` or `Close` that changes its subscription's fields.
    pub const HSYNC_CLOSE_31: Self = Self("hsync-close-31");
    /// An `Open` for an unadvertised schema.
    pub const HSYNC_CLOSE_32: Self = Self("hsync-close-32");
    /// A header the peer served fails validation.
    pub const HSYNC_CLOSE_33: Self = Self("hsync-close-33");
}

/// The block-sync close spec's rows that name a fault.
impl CloseRow {
    /// A message type the reader's role doesn't admit.
    pub const BSYNC_CLOSE_2: Self = Self("bsync-close-2");
    /// Nonzero flags.
    pub const BSYNC_CLOSE_3: Self = Self("bsync-close-3");
    /// A payload outside its row's length range.
    pub const BSYNC_CLOSE_4: Self = Self("bsync-close-4");
    /// A payload whose first byte differs from the frame type.
    pub const BSYNC_CLOSE_5: Self = Self("bsync-close-5");
    /// A `Status` field out of range.
    pub const BSYNC_CLOSE_6: Self = Self("bsync-close-6");
    /// A `GetBlocks` count or range out of bounds.
    pub const BSYNC_CLOSE_7: Self = Self("bsync-close-7");
    /// A `GetBlocks` that overlaps a live served range.
    pub const BSYNC_CLOSE_9: Self = Self("bsync-close-9");
    /// An ending count out of bounds.
    pub const BSYNC_CLOSE_11: Self = Self("bsync-close-11");
    /// An ending that matches no live range or its received prefix.
    pub const BSYNC_CLOSE_12: Self = Self("bsync-close-12");
    /// A `Block` header that doesn't parse.
    pub const BSYNC_CLOSE_13: Self = Self("bsync-close-13");
    /// A body past its range's remaining body bytes.
    pub const BSYNC_CLOSE_14: Self = Self("bsync-close-14");
    /// A `Block` that fails bounded decode.
    pub const BSYNC_CLOSE_15: Self = Self("bsync-close-15");
    /// A coinbase height other than the authorized height.
    pub const BSYNC_CLOSE_16: Self = Self("bsync-close-16");
    /// Transactions that don't match the header's merkle root.
    pub const BSYNC_CLOSE_17: Self = Self("bsync-close-17");
    /// A body the verifier proved tampered.
    pub const BSYNC_CLOSE_18: Self = Self("bsync-close-18");
    /// A `Block` whose header matches no live range.
    pub const BSYNC_CLOSE_19: Self = Self("bsync-close-19");
}

/// The `tx-relay` close table's rows that name a fault, from
/// `docs/specs/peer-message-regulation.md`.
impl CloseRow {
    /// A payload that doesn't decode as its message.
    pub const RELAY_CLOSE_3: Self = Self("relay-close-3");
    /// A second `Hello`.
    pub const RELAY_CLOSE_4: Self = Self("relay-close-4");
    /// A `Hello` outside the table's limits.
    pub const RELAY_CLOSE_5: Self = Self("relay-close-5");
    /// A `GetTxs` before the peer's `Hello`.
    pub const RELAY_CLOSE_6: Self = Self("relay-close-6");
    /// An answer to no outstanding request.
    pub const RELAY_CLOSE_9: Self = Self("relay-close-9");
    /// A `Tx` of another size than announced.
    pub const RELAY_CLOSE_10: Self = Self("relay-close-10");
    /// A `Tx` that doesn't hash to the requested ID.
    pub const RELAY_CLOSE_11: Self = Self("relay-close-11");
    /// A `Tx` body that doesn't decode.
    pub const RELAY_CLOSE_12: Self = Self("relay-close-12");
}

/// Why a close assigns the peer no fault.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NeutralCause {
    /// This node ended the connection for its own reasons: shutdown, a
    /// retired session, a finished exchange or a local bug.
    LocalCancellation,
    /// A local limit refused the connection or its work.
    ResourceRefusal,
    /// The negotiated limits or layout can't carry this node's services.
    IncompatibleConfiguration,
    /// The transport failed.
    IoFailure,
    /// A deadline passed.
    Timeout,
    /// The peer stalled again after a stall already retired its session.
    StallPolicy,
}

/// The outcome a connection closed with.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CloseOutcome {
    /// The peer broke `row` on a stream of `kind` at `version`.
    Fault {
        /// The stream kind.
        kind: u16,
        /// The stream kind's negotiated version.
        version: u16,
        /// The close-spec row.
        row: CloseRow,
    },
    /// The close assigns the peer no fault.
    Neutral {
        /// Why.
        cause: NeutralCause,
        /// The close site, for traces.
        detail: &'static str,
    },
}

impl CloseOutcome {
    /// The close of an ordered stream whose message bucket ran out: a local
    /// rate limit, so neutral.
    pub const ORDERED_RATE_LIMITED: Self =
        Self::neutral(NeutralCause::ResourceRefusal, "ordered_rate_limited");

    /// The close after a service task panicked: a local bug, so neutral.
    pub const SERVICE_PANIC: Self = Self::neutral(NeutralCause::LocalCancellation, "service_panic");

    /// A fault: the peer broke `row` on a stream of `kind` at `version`.
    pub const fn fault(kind: u16, version: u16, row: CloseRow) -> Self {
        Self::Fault { kind, version, row }
    }

    /// A neutral close for `cause`, at the close site `detail`.
    pub const fn neutral(cause: NeutralCause, detail: &'static str) -> Self {
        Self::Neutral { cause, detail }
    }

    /// A neutral close after a repeat stall, at the close site `detail`.
    pub const fn stall_policy(detail: &'static str) -> Self {
        Self::neutral(NeutralCause::StallPolicy, detail)
    }

    /// True if the close assigns the peer a fault.
    pub const fn is_fault(self) -> bool {
        matches!(self, Self::Fault { .. })
    }

    /// The broken row, for a fault.
    pub const fn row(self) -> Option<CloseRow> {
        match self {
            Self::Fault { row, .. } => Some(row),
            Self::Neutral { .. } => None,
        }
    }

    /// The trace label: a fault's row ID, or a neutral close's site.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Fault { row, .. } => row.id(),
            Self::Neutral { detail, .. } => detail,
        }
    }
}

impl fmt::Display for CloseOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fault { kind, version, row } => {
                write!(f, "fault {row} on stream {kind} v{version}")
            }
            Self::Neutral { cause, detail } => write!(f, "neutral {cause:?}: {detail}"),
        }
    }
}
