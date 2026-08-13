use thiserror::Error;
use zakura_chain::block;

/// Checked inferred-height or advisory peer-height failure.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderHeightError {
    /// The known parent is already at the maximum supported height.
    #[error("child height after parent {0:?} exceeds the supported range")]
    Overflow(block::Height),
    /// An advisory peer height disagreed with the locally inferred height.
    #[error("peer header height {peer:?} does not match inferred height {inferred:?}")]
    PeerMismatch {
        /// Height inferred from the known parent.
        inferred: block::Height,
        /// Untrusted peer-provided height.
        peer: block::Height,
    },
}

/// Infer a child height from its known parent and optionally compare an advisory peer height.
pub fn infer_height(
    parent_height: block::Height,
    peer_height: Option<block::Height>,
) -> Result<block::Height, HeaderHeightError> {
    let inferred = parent_height
        .0
        .checked_add(1)
        .map(block::Height)
        .filter(|height| *height <= block::Height::MAX)
        .ok_or(HeaderHeightError::Overflow(parent_height))?;
    if let Some(peer) = peer_height {
        if peer != inferred {
            return Err(HeaderHeightError::PeerMismatch { inferred, peer });
        }
    }
    Ok(inferred)
}
