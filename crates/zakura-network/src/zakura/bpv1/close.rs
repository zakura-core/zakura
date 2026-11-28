//! Versioned close-cause enum for BPv1 (decision-5, bpv1-61).
//!
//! Every protocol-violation close in BPv1 cites exactly one active close row
//! together with the `(stream_kind, stream_version)` pair it occurred on. Retired rows (6, 7, 12, 17, 19) are deliberately
//! absent so they cannot be emitted.
//!
//! # Close properties
//!
//! Every row holds these properties. `close_tests` checks each row against
//! the production machines.
//!
//! - **Sound**: a conformant peer never triggers the row. Each row's test
//!   also runs the nearest conformant sequence and checks that it stays open.
//! - **Complete**: every violation that the row names closes. A violation
//!   that this node cannot prove from its own state does not close.
//! - **Attributed**: the close names every session that sent the violating
//!   message, and only those sessions.
//! - **Local faults stay local**: a full local budget, a failed local port,
//!   or a stale session drops or rejects without a close.
//! - **Neutral closes stay neutral**: a repeat liveness stall or a stalled
//!   control write closes without a row and records no fault.

use super::wire::BPV1_STREAM_VERSION;

/// Active BPv1 protocol-violation close rows.
///
/// Each discriminant is the row's `N` in `bpv1-close-N`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum Bpv1CloseRow {
    /// `bpv1-close-1`: A `Have` flags byte with reserved bits (decision-24),
    /// or a message type on the wrong BPv1 stream. Transport regulation
    /// closes a frame over its row cap, an unknown type, or nonzero frame
    /// flags before BPv1 reads it.
    Row1FrameHeader = 1,
    /// `bpv1-close-2`: Field out of range, truncation, or trailing bytes,
    /// including a zero `want_id`, an empty `Want` bitmap, and a `HeaderMeta`
    /// header that does not parse.
    Row2Decode = 2,
    /// `bpv1-close-3`: `HeaderMeta` whose header headerchain classifies as peer-invalid,
    /// or whose proof of work fails while its parent is unknown.
    Row3InvalidHeader = 3,
    /// `bpv1-close-4`: `HeaderMeta` layout mismatch or failed coinbase
    /// commitment. It closes every session whose copy equals the failing
    /// envelope, and no session whose copy differs.
    Row4BadMetaOrCommitment = 4,
    /// `bpv1-close-5`: Second `HeaderMeta` for one block on one connection
    /// while this node keeps the block pending, admitted, or retired.
    Row5DuplicateHeaderMeta = 5,
    /// `bpv1-close-8`: `Want` stripe or requested bit out of range for an
    /// admitted block. An empty bitmap closes with row 2.
    Row8BadWantRange = 8,
    /// `bpv1-close-9`: `Want` reused live id, overlapping live set, or > 2×
    /// `MAX_WANTS_INFLIGHT`. A `Want` stays live until this node writes its
    /// `WantEnd`.
    Row9WantReservationViolation = 9,
    /// `bpv1-close-10`: `StripeDone` stripe out of range or repeated.
    Row10BadStripeDone = 10,
    /// `bpv1-close-11`: Repeated `BlockDone`.
    Row11DuplicateBlockDone = 11,
    /// `bpv1-close-13`: `Part` index out of range or failed Merkle proof.
    Row13BadPartIndexOrProof = 13,
    /// `bpv1-close-14`: `Part` unwanted or repeated from the same peer.
    Row14UnwantedOrDuplicatePart = 14,
    /// `bpv1-close-15`: late `Part` or `Have` counter exhausted, for a retired
    /// block or one whose `HeaderMeta` this session dropped.
    Row15RetiredCounterExhausted = 15,
    /// `bpv1-close-16`: `Have` index out of range, a repeated index, or a
    /// bitmap whose length is not `ceil(stripes * n / 8)` (decision-33). A
    /// set padding bit is an index out of range.
    Row16BadHaveIndex = 16,
    /// `bpv1-close-18`: `WantEnd` with no live `Want`, `sent` above popcount, or `sent` below attributed parts.
    Row18BadWantEnd = 18,
    /// `bpv1-close-20`: Push flags past the connection's bound in one stripe
    /// (`n` from `proposer_node`, `n − k` otherwise), relay flags from a
    /// connection that is not `proposer_node`, or relay flags on more than
    /// `n − k` indices of one stripe.
    Row20BadPushFlags = 20,
    /// `bpv1-close-21`: `Part` or `Have` for a block whose `HeaderMeta`
    /// crossed the connection in neither direction (bpv1-14).
    Row21DataBeforeHeaderMeta = 21,
}

/// Complete versioned close cause attached to a BPv1 disconnect (decision-5).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Bpv1CloseCause {
    /// Stream kind on which the violation was observed.
    pub stream_kind: u16,
    /// Stream version negotiated for that stream.
    pub stream_version: u16,
    /// The cited `bpv1-close-N` row.
    pub row: Bpv1CloseRow,
}

impl Bpv1CloseRow {
    /// Stable metric and close-cause label (`bpv1-close-N`).
    pub const fn label(self) -> &'static str {
        match self {
            Self::Row1FrameHeader => "bpv1-close-1",
            Self::Row2Decode => "bpv1-close-2",
            Self::Row3InvalidHeader => "bpv1-close-3",
            Self::Row4BadMetaOrCommitment => "bpv1-close-4",
            Self::Row5DuplicateHeaderMeta => "bpv1-close-5",
            Self::Row8BadWantRange => "bpv1-close-8",
            Self::Row9WantReservationViolation => "bpv1-close-9",
            Self::Row10BadStripeDone => "bpv1-close-10",
            Self::Row11DuplicateBlockDone => "bpv1-close-11",
            Self::Row13BadPartIndexOrProof => "bpv1-close-13",
            Self::Row14UnwantedOrDuplicatePart => "bpv1-close-14",
            Self::Row15RetiredCounterExhausted => "bpv1-close-15",
            Self::Row16BadHaveIndex => "bpv1-close-16",
            Self::Row18BadWantEnd => "bpv1-close-18",
            Self::Row20BadPushFlags => "bpv1-close-20",
            Self::Row21DataBeforeHeaderMeta => "bpv1-close-21",
        }
    }

    /// Builds the [`Bpv1CloseCause`] for this row on `stream_kind` at
    /// [`BPV1_STREAM_VERSION`]. Rows 1 and 2 fire on either stream, so the
    /// caller names the stream it read.
    pub const fn cause(self, stream_kind: u16) -> Bpv1CloseCause {
        Bpv1CloseCause {
            stream_kind,
            stream_version: BPV1_STREAM_VERSION,
            row: self,
        }
    }
}
