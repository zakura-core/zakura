//! Shared synchronous observable-header validation primitives.

mod contextual;
mod prepare;

pub use contextual::{
    validate_contextual_difficulty_and_time, AdjustedDifficulty, ContextualValidationError,
    BLOCK_MAX_TIME_SINCE_MEDIAN, POW_ADJUSTMENT_BLOCK_SPAN, POW_MEDIAN_BLOCK_SPAN,
};
pub use prepare::{prepare_headers, HeaderBatchInput, HeaderFailure, HeaderRule, HeaderRules};

use chrono::{DateTime, Utc};
use thiserror::Error;
use zakura_chain::{
    block::{self, Commitment, CommitmentError},
    parameters::{Network, NetworkKind},
    work::{
        difficulty::{ExpandedDifficulty, ParameterDifficulty as _},
        equihash,
    },
};

/// Invalid canonical encoding or signed-version semantics for an in-memory header.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid block-header version {version}: {reason}")]
pub struct HeaderEncodingError {
    /// Rejected unsigned storage representation.
    pub version: u32,
    /// Stable reason shared with canonical serialization.
    pub reason: &'static str,
}

/// Validate the signed version rule, then compute the canonical full-header hash.
pub fn validate_encoding_version_hash(
    header: &block::Header,
) -> Result<block::Hash, HeaderEncodingError> {
    block::validate_header_version(header.version).map_err(|reason| HeaderEncodingError {
        version: header.version,
        reason,
    })?;
    Ok(header.hash())
}

/// Parent-link failure at one exact zero-based header offset.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("header at offset {offset} names parent {actual:?}, expected {expected:?}")]
pub struct HeaderLinkError {
    /// Zero-based offset of the failing header.
    pub offset: usize,
    /// Expected parent hash.
    pub expected: block::Hash,
    /// Actual parent hash.
    pub actual: block::Hash,
}

/// Validate the first parent link and every internal link in a header run.
pub fn validate_link(
    parent_hash: block::Hash,
    headers: &[block::Header],
) -> Result<(), HeaderLinkError> {
    let mut expected = parent_hash;
    for (offset, header) in headers.iter().enumerate() {
        if header.previous_block_hash != expected {
            return Err(HeaderLinkError {
                offset,
                expected,
                actual: header.previous_block_hash,
            });
        }
        expected = header.hash();
    }
    Ok(())
}

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

/// Parse and validate the height- and network-specific commitment field structure.
pub fn validate_commitment_structure(
    header: &block::Header,
    network: &Network,
    height: block::Height,
) -> Result<Commitment, CommitmentError> {
    header.commitment(network, height)
}

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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PowPolicyKind {
    Validate,
    AuthenticatedCustomWaiver,
}

/// Network-bound proof-of-work policy; callers cannot construct a production waiver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PowPolicy {
    network: Network,
    kind: PowPolicyKind,
}

/// Invalid attempt to derive a proof-of-work waiver.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum PowPolicyError {
    /// Mainnet and the default public Testnet can never receive a waiver.
    #[error("proof-of-work cannot be disabled for production network {0:?}")]
    ProductionNetwork(NetworkKind),
    /// The authenticated custom configuration does not disable proof of work.
    #[error("custom network configuration does not authenticate a proof-of-work waiver")]
    PowNotDisabled,
}

impl PowPolicy {
    /// Construct a policy that always verifies proof of work for the bound network.
    pub fn validating(network: &Network) -> Self {
        Self {
            network: network.clone(),
            kind: PowPolicyKind::Validate,
        }
    }

    /// Derive the only policy permitted by an authenticated network configuration.
    pub fn for_network(network: &Network) -> Result<Self, PowPolicyError> {
        if network.disable_pow() {
            Self::authenticated_custom_waiver(network)
        } else {
            Ok(Self::validating(network))
        }
    }

    /// Attempt to construct a waiver from an authenticated custom-network configuration.
    pub fn authenticated_custom_waiver(network: &Network) -> Result<Self, PowPolicyError> {
        if matches!(network.kind(), NetworkKind::Mainnet)
            || (matches!(network.kind(), NetworkKind::Testnet) && network.is_default_testnet())
        {
            return Err(PowPolicyError::ProductionNetwork(network.kind()));
        }
        if !network.disable_pow() {
            return Err(PowPolicyError::PowNotDisabled);
        }
        Ok(Self {
            network: network.clone(),
            kind: PowPolicyKind::AuthenticatedCustomWaiver,
        })
    }

    /// Validate solution shape, network parameters, and proof unless this exact custom network
    /// has an authenticated disabled-PoW configuration.
    pub fn validate_solution(&self, header: &block::Header) -> Result<(), equihash::Error> {
        match self.kind {
            PowPolicyKind::Validate => header.solution.check(header, &self.network),
            PowPolicyKind::AuthenticatedCustomWaiver => {
                header.solution.validate_shape(&self.network)
            }
        }
    }

    /// Return true when this exact authenticated custom configuration waives Equihash.
    pub fn is_authenticated_custom_waiver(&self) -> bool {
        self.kind == PowPolicyKind::AuthenticatedCustomWaiver
    }
}

/// Apply the shared local two-hour future-time rule at an explicit injected clock value.
pub fn validate_future_time(
    header: &block::Header,
    now: DateTime<Utc>,
    height: block::Height,
    hash: block::Hash,
) -> Result<(), block::BlockTimeError> {
    header.time_is_valid_at(now, &height, &hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{
        block::genesis::regtest_genesis_block,
        parameters::testnet::{Parameters, RegtestParameters},
        work::difficulty::U256,
    };

    #[test]
    fn canonical_version_hash_link_and_height_boundaries() {
        let header = *regtest_genesis_block().header;
        let expected_hash = header.hash();
        assert_eq!(
            validate_encoding_version_hash(&header),
            Ok(expected_hash),
            "the shared validator hashes the complete canonical header"
        );

        let mut historical_non_four = header;
        historical_non_four.version = 5;
        assert!(validate_encoding_version_hash(&historical_non_four).is_ok());
        let mut too_old = header;
        too_old.version = 3;
        assert!(matches!(
            validate_encoding_version_hash(&too_old),
            Err(HeaderEncodingError { version: 3, .. })
        ));
        let mut high_bit = header;
        high_bit.version = 1 << 31;
        assert!(validate_encoding_version_hash(&high_bit).is_err());

        let mut child = header;
        child.previous_block_hash = expected_hash;
        assert_eq!(
            validate_link(header.previous_block_hash, &[header, child]),
            Ok(())
        );
        child.previous_block_hash = block::Hash([9; 32]);
        assert!(matches!(
            validate_link(header.previous_block_hash, &[header, child]),
            Err(HeaderLinkError { offset: 1, .. })
        ));
        assert_eq!(
            infer_height(block::Height(7), Some(block::Height(8))),
            Ok(block::Height(8))
        );
        assert!(matches!(
            infer_height(block::Height(7), Some(block::Height(9))),
            Err(HeaderHeightError::PeerMismatch { .. })
        ));
        assert_eq!(
            infer_height(block::Height::MAX, None),
            Err(HeaderHeightError::Overflow(block::Height::MAX))
        );
    }

    #[test]
    fn pow_policy_waiver_is_derived_only_from_custom_network_identity() {
        assert_eq!(
            PowPolicy::authenticated_custom_waiver(&Network::Mainnet),
            Err(PowPolicyError::ProductionNetwork(NetworkKind::Mainnet))
        );
        assert!(!PowPolicy::for_network(&Network::Mainnet)
            .expect("mainnet always validates proof of work")
            .is_authenticated_custom_waiver());
        let testnet = Network::new_default_testnet();
        assert_eq!(
            PowPolicy::authenticated_custom_waiver(&testnet),
            Err(PowPolicyError::ProductionNetwork(NetworkKind::Testnet))
        );
        assert!(!PowPolicy::for_network(&testnet)
            .expect("default testnet always validates proof of work")
            .is_authenticated_custom_waiver());
        let regtest = Network::new_regtest(RegtestParameters::default());
        let regtest_policy =
            PowPolicy::for_network(&regtest).expect("regtest is an authenticated custom network");
        assert!(regtest_policy.is_authenticated_custom_waiver());
        assert!(validate_compact_target(&regtest_genesis_block().header, &regtest).is_ok());
        let mut wrong_shape = *regtest_genesis_block().header;
        wrong_shape.solution = equihash::Solution::for_proposal();
        assert!(matches!(
            regtest_policy.validate_solution(&wrong_shape),
            Err(equihash::Error::InvalidSolutionSize { .. })
        ));

        let pow_disabled_custom = Parameters::build()
            .with_network_name("PowDisabledCustom")
            .expect("the custom network name is valid")
            .with_disable_pow(true)
            .to_network()
            .expect("the test custom-network parameters are valid");
        let custom_policy = PowPolicy::for_network(&pow_disabled_custom)
            .expect("configured custom networks may authenticate a PoW waiver");
        assert!(custom_policy.is_authenticated_custom_waiver());
        let proposal_header = block::Header {
            solution: equihash::Solution::for_proposal(),
            ..*regtest_genesis_block().header
        };
        assert!(custom_policy.validate_solution(&proposal_header).is_ok());
    }

    #[test]
    fn hash_filter_accepts_equality_and_rejects_one_above() {
        let target = ExpandedDifficulty::from(U256::from(42));
        let mut equal_bytes = [0; 32];
        equal_bytes[0] = 42;
        assert_eq!(
            validate_hash_filter(block::Hash(equal_bytes), target),
            Ok(())
        );

        let mut above_bytes = equal_bytes;
        above_bytes[0] = 43;
        assert_eq!(
            validate_hash_filter(block::Hash(above_bytes), target),
            Err(HashFilterError {
                hash: block::Hash(above_bytes),
                target,
            })
        );
    }

    #[test]
    fn future_time_accepts_two_hour_equality_and_rejects_one_second_above() {
        let mut header = *regtest_genesis_block().header;
        let now = header.time;
        let height = block::Height(1);
        let hash = header.hash();
        header.time = now + chrono::Duration::hours(2);
        assert!(validate_future_time(&header, now, height, hash).is_ok());
        header.time += chrono::Duration::seconds(1);
        assert!(validate_future_time(&header, now, height, hash).is_err());
    }
}
