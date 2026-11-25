//! Versioned close-cause enum for BPv1 (decision-5, bpv1-61).
//!
//! Every protocol-violation close in BPv1 cites exactly one active close row
//! together with the `(stream_kind, stream_version)` pair it occurred on. Retired rows (6, 7, 12, 17, 19) are deliberately
//! absent so they cannot be emitted.

use super::wire::BPV1_STREAM_VERSION;

/// Active BPv1 protocol-violation close rows.
///
/// Each discriminant is the row's `N` in `bpv1-close-N`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum Bpv1CloseRow {
    /// `bpv1-close-1`: Frame over row cap, unknown discriminator, or nonzero flags.
    Row1FrameHeader = 1,
    /// `bpv1-close-2`: Field out of range, truncation, or trailing bytes.
    Row2Decode = 2,
    /// `bpv1-close-3`: `HeaderMeta` whose header headerchain classifies as peer-invalid.
    Row3InvalidHeader = 3,
    /// `bpv1-close-4`: `HeaderMeta` layout mismatch or failed coinbase commitment.
    Row4BadMetaOrCommitment = 4,
    /// `bpv1-close-5`: Second `HeaderMeta` for one block on one connection.
    Row5DuplicateHeaderMeta = 5,
    /// `bpv1-close-8`: `Want` stripe or requested bit out of range, or empty bitmap.
    Row8BadWantRange = 8,
    /// `bpv1-close-9`: `Want` reused live id, overlapping live set, or > 2× `MAX_WANTS_INFLIGHT`.
    Row9WantReservationViolation = 9,
    /// `bpv1-close-10`: `StripeDone` stripe out of range or repeated.
    Row10BadStripeDone = 10,
    /// `bpv1-close-11`: Repeated `BlockDone`.
    Row11DuplicateBlockDone = 11,
    /// `bpv1-close-13`: `Part` index out of range or failed Merkle proof.
    Row13BadPartIndexOrProof = 13,
    /// `bpv1-close-14`: `Part` unwanted or repeated from the same peer.
    Row14UnwantedOrDuplicatePart = 14,
    /// `bpv1-close-15`: Retired block late `Part` or `Have` counter exhausted.
    Row15RetiredCounterExhausted = 15,
    /// `bpv1-close-16`: `Have` index out of range or repeated index.
    Row16BadHaveIndex = 16,
    /// `bpv1-close-18`: `WantEnd` with no live `Want`, `sent` above popcount, or `sent` below attributed parts.
    Row18BadWantEnd = 18,
    /// `bpv1-close-20`: Push flags past the connection's bound in one stripe
    /// (`n` from the block's proposer, `n − k` otherwise), relay flags from a
    /// connection that is not the proposer, or relay flags on more than
    /// `n − k` indices of one stripe. The proposer is `proposer_node`, or the
    /// node whose ID starts with the miner mesh output's gateway ID
    /// (decision-34).
    Row20BadPushFlags = 20,
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
