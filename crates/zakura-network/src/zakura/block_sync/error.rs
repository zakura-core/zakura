use super::*;
use crate::zakura::wire_codec::WireError;

/// Structured wire and stateless-validation errors for stream 6.
#[derive(Debug, Error)]
pub enum BlockSyncWireError {
    /// A decoded request or response block count exceeded its contract.
    #[error("Zakura block-sync block count {actual} exceeds cap {max}")]
    BlockCountLimit {
        /// Actual count.
        actual: u32,
        /// Maximum allowed count.
        max: u32,
    },

    /// A `GetBlocks`, `BlocksDone`, or `RangeUnavailable` count was zero.
    #[error("Zakura block-sync count must be non-zero")]
    ZeroBlockCount,

    /// A `GetBlocks` range extended above the maximum supported height.
    #[error(
        "Zakura block-sync range from {start:?} with count {count} exceeds the maximum height"
    )]
    BlockRangeOverflow {
        /// First requested height.
        start: block::Height,
        /// Requested block count.
        count: u32,
    },

    /// A decoded block body exceeded the consensus block-size limit.
    #[error("Zakura block-sync block length {actual} exceeds consensus cap {max}")]
    OversizedBlock {
        /// Actual serialized block length.
        actual: usize,
        /// Maximum allowed serialized block length.
        max: usize,
    },

    /// Frame and payload message types disagreed.
    #[error("Zakura block-sync frame type {frame} disagrees with payload type {payload}")]
    MismatchedFrameMessageType {
        /// Outer frame message type.
        frame: u16,
        /// Inner payload message type.
        payload: u8,
    },

    /// A structural wire error shared with every typed codec.
    #[error(transparent)]
    Wire(#[from] WireError),
}
