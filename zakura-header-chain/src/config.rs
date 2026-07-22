//! Versioned engine resource limits.

use std::num::{NonZeroU32, NonZeroUsize};

use zakura_chain::parameters::{constants::MAX_BLOCK_REORG_HEIGHT, MAX_NON_FINALIZED_CHAIN_FORKS};

/// Exact v1 maximum number of retained non-finalized header nodes.
pub const MAX_NON_FINALIZED_NODES_V1: usize = 65_536;
/// Exact v1 maximum number of staged unknown targets across all peers.
pub const MAX_STAGED_TARGETS_V1: usize = 16;
/// Exact v1 maximum number of candidate tips.
pub const MAX_CANDIDATE_TIPS_V1: usize = MAX_NON_FINALIZED_CHAIN_FORKS;

/// Immutable resource bounds for one header-chain engine version.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct EngineLimits {
    /// Irreversible local finality depth.
    pub local_finality_depth: NonZeroU32,
    /// Maximum retained eligible and ineligible candidate tips.
    pub max_candidate_tips: NonZeroUsize,
    /// Maximum retained non-finalized DAG nodes.
    pub max_non_finalized_nodes: NonZeroUsize,
    /// Maximum staged unknown targets across all peers.
    pub max_staged_targets: NonZeroUsize,
}

impl EngineLimits {
    /// Return the exact limits frozen by specification version 1.3.
    pub fn v1() -> Self {
        Self {
            local_finality_depth: NonZeroU32::new(MAX_BLOCK_REORG_HEIGHT)
                .expect("the v1 local finality depth is nonzero"),
            max_candidate_tips: NonZeroUsize::new(MAX_CANDIDATE_TIPS_V1)
                .expect("the v1 candidate-tip limit is nonzero"),
            max_non_finalized_nodes: NonZeroUsize::new(MAX_NON_FINALIZED_NODES_V1)
                .expect("the v1 node limit is nonzero"),
            max_staged_targets: NonZeroUsize::new(MAX_STAGED_TARGETS_V1)
                .expect("the v1 staged-target limit is nonzero"),
        }
    }
}

impl Default for EngineLimits {
    fn default() -> Self {
        Self::v1()
    }
}

const _: () = assert!(MAX_BLOCK_REORG_HEIGHT == 1_000);
const _: () = assert!(MAX_CANDIDATE_TIPS_V1 == 10);
const _: () = assert!(MAX_NON_FINALIZED_NODES_V1 == 65_536);
const _: () = assert!(MAX_STAGED_TARGETS_V1 == 16);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_limits_v1_match_the_frozen_specification() {
        let limits = EngineLimits::v1();
        assert_eq!(limits.local_finality_depth.get(), 1_000);
        assert_eq!(limits.max_candidate_tips.get(), 10);
        assert_eq!(limits.max_non_finalized_nodes.get(), 65_536);
        assert_eq!(limits.max_staged_targets.get(), 16);
    }
}
