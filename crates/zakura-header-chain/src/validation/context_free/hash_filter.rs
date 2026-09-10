use thiserror::Error;
use zakura_chain::{block, work::difficulty::ExpandedDifficulty};

/// Header hash failed its already-expanded target threshold.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("header hash {hash:?} exceeds difficulty target {target:?}")]
pub struct HashFilterError {
    /// Raw internal header hash.
    pub hash: block::Hash,
    /// Expanded target threshold.
    pub target: ExpandedDifficulty,
}

/// Validate the little-endian header-hash difficulty filter.
pub fn validate_hash_filter(
    hash: block::Hash,
    target: ExpandedDifficulty,
) -> Result<(), HashFilterError> {
    if hash > target {
        return Err(HashFilterError { hash, target });
    }
    Ok(())
}
