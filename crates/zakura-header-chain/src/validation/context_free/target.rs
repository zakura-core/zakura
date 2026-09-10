use thiserror::Error;
use zakura_chain::{
    block,
    parameters::Network,
    work::difficulty::{ExpandedDifficulty, ParameterDifficulty as _},
};

/// Context-free compact-target domain or proof-of-work-limit failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CompactTargetError {
    /// Compact encoding represents a negative, zero, overflowed, or otherwise invalid target.
    #[error("invalid compact difficulty target")]
    Invalid,
    /// Expanded target is easier than this network's proof-of-work limit.
    #[error("difficulty target {target:?} exceeds network limit {limit:?}")]
    EasierThanLimit {
        /// Expanded candidate target.
        target: ExpandedDifficulty,
        /// Easiest target accepted on this network.
        limit: ExpandedDifficulty,
    },
}

/// Expand and validate a header's compact target against the network proof-of-work limit.
pub fn validate_compact_target(
    header: &block::Header,
    network: &Network,
) -> Result<ExpandedDifficulty, CompactTargetError> {
    let target = header
        .difficulty_threshold
        .to_expanded()
        .ok_or(CompactTargetError::Invalid)?;
    let limit = network.target_difficulty_limit();
    if target > limit {
        return Err(CompactTargetError::EasierThanLimit { target, limit });
    }
    Ok(target)
}
