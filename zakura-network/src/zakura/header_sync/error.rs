use super::*;

/// Errors that prevent the header-sync reactor from starting.
#[derive(Debug, Error)]
pub enum HeaderSyncStartError {
    /// The configured anchor is neither genesis nor a hash-matching checkpoint.
    #[error("invalid Zakura header-sync anchor at height {anchor:?}")]
    InvalidAnchor {
        /// Rejected anchor.
        anchor: (block::Height, block::Hash),
    },

    /// The configured anchor is ahead of the durable verified body/history-tree base.
    #[error(
        "Zakura header-sync anchor height {anchor_height:?} is above the verified block tip \
         {verified_block_tip:?}"
    )]
    AnchorAboveVerifiedBlockTip {
        /// Rejected anchor height.
        anchor_height: block::Height,
        /// Durable verified body/history-tree base.
        verified_block_tip: block::Height,
    },

    /// Only one anchor field was configured.
    #[error("Zakura header-sync anchor_height and anchor_hash must be configured together")]
    IncompleteAnchor,
}
