use chrono::{Duration, Timelike};
use rayon::prelude::*;
use zakura_chain::parameters::Network;

use super::super::{
    infer_height, validate_commitment_structure, validate_compact_target,
    validate_encoding_version_hash, validate_hash_filter, PowPolicy, PowPolicyError,
};
use super::{failure::invalid, HeaderBatchInput, HeaderFailure, HeaderRule};
use crate::{
    Clock, EngineConfig, HeaderValidationState, PreparedHeader, PreparedHeaderBatch,
    ValidationLease,
};

/// Immutable authenticated rules used by the pure preparation pipeline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderRules {
    network: Network,
    pow_policy: PowPolicy,
    trust_anchor_digest: [u8; 32],
}

impl HeaderRules {
    /// Derive rules only from the validated engine configuration.
    pub fn from_engine_config(config: &EngineConfig) -> Result<Self, PowPolicyError> {
        Ok(Self {
            network: config.network.clone(),
            pow_policy: PowPolicy::for_network(&config.network)?,
            trust_anchor_digest: config.trust_anchor_digest(),
        })
    }

    /// Bind authenticated network parameters to a state-issued validation lease.
    /// The state transition independently rechecks the lease's anchor digest before mutation.
    pub fn for_validation_lease(lease: &ValidationLease) -> Result<Self, PowPolicyError> {
        let network = lease.network.clone();
        Ok(Self {
            pow_policy: PowPolicy::for_network(&network)?,
            network,
            trust_anchor_digest: lease.trust_anchor_digest,
        })
    }

    /// Return the authenticated network parameters bound into these rules.
    pub fn network(&self) -> &Network {
        &self.network
    }

    /// Return the authenticated trust-anchor identity sealed into preparation receipts.
    pub const fn trust_anchor_digest(&self) -> [u8; 32] {
        self.trust_anchor_digest
    }
}

/// This function validates and seals a complete batch without reading retained
/// graph state.
///
/// The function establishes these context-free properties:
///
/// - The batch contains at least one header and stays within the per-transition
///   header limit.
/// - Each header uses a supported encoded version.
/// - The function computes each header hash locally.
/// - Checked arithmetic infers each height from the supplied parent height.
/// - Each height uses the required commitment interpretation.
/// - Each compact target uses canonical encoding and stays within the network
///   limit.
/// - Each hash satisfies its target unless authenticated custom policy waives
///   proof of work.
/// - Each Equihash solution satisfies the authenticated proof-of-work policy.
/// - The function accepts each current timestamp or assigns an exact deadline to
///   each future timestamp.
/// - The function converts each compact target to exact per-block work.
///
/// A successful call returns a nonempty [`PreparedHeaderBatch`]. Each entry
/// contains the canonical header, its computed hash, inferred height, exact work,
/// and local validation state. The receipt binds the result to the supplied
/// parent, network policy, and trust-anchor digest.
///
/// The transition planner checks the retained parent and batch linkage. The
/// planner recomputes each hash, height, and work value. The planner checks
/// contextual difficulty and median time against retained ancestry. The planner
/// also applies finality, checkpoint, settled-pin, target-completion, and
/// auxiliary-provenance rules.
pub fn prepare_headers(
    input: HeaderBatchInput<'_>,
    parent_frontier: crate::Frontier,
    rules: &HeaderRules,
    clock: &dyn Clock,
) -> Result<PreparedHeaderBatch, HeaderFailure> {
    // Batch nonempty and within the per-transition header limit.
    if input.headers.is_empty() {
        return Err(HeaderFailure::Empty);
    }
    if input.headers.len() > crate::MAX_HEADERS_PER_TRANSITION_V1 {
        return Err(HeaderFailure::Oversized {
            actual: input.headers.len(),
            maximum: crate::MAX_HEADERS_PER_TRANSITION_V1,
        });
    }

    // Supported encoded version and locally computed header hash.
    let hash_results: Vec<_> = input
        .headers
        .par_iter()
        .enumerate()
        .map(|(offset, header)| {
            validate_encoding_version_hash(header)
                .map_err(|error| invalid(offset, HeaderRule::EncodingVersionHash, error))
        })
        .collect();
    let hashes: Vec<_> = hash_results.into_iter().collect::<Result<_, _>>()?;

    // Local-clock bound for future-timestamp classification.
    let now = clock.now();
    let future_limit = now
        .checked_add_signed(Duration::hours(2))
        .ok_or(HeaderFailure::ClockRange)?;

    // Checked height inference from the supplied parent height.
    let mut parent_height = parent_frontier.height;
    let heights: Vec<_> = (0..input.headers.len())
        .map(|offset| {
            let height = infer_height(parent_height, None)
                .map_err(|error| invalid(offset, HeaderRule::InferredHeight, error))?;
            parent_height = height;
            Ok(height)
        })
        .collect::<Result<_, HeaderFailure>>()?;

    let context_free: Vec<_> = input
        .headers
        .par_iter()
        .zip(&hashes)
        .zip(&heights)
        .enumerate()
        .map(|(offset, ((header, hash), height))| {
            // Height-dependent commitment interpretation.
            validate_commitment_structure(header, &rules.network, *height)
                .map_err(|error| invalid(offset, HeaderRule::CommitmentStructure, error))?;

            // Compact target canonical encoding and network limit.
            let target = validate_compact_target(header, &rules.network)
                .map_err(|error| invalid(offset, HeaderRule::CompactTarget, error))?;

            // Hash-to-target filter, unless authenticated custom policy waives PoW.
            if !rules.pow_policy.is_authenticated_custom_waiver() {
                validate_hash_filter(*hash, target)
                    .map_err(|error| invalid(offset, HeaderRule::HashToTarget, error))?;
            }

            // Equihash solution under the authenticated proof-of-work policy.
            rules
                .pow_policy
                .validate_solution(header)
                .map_err(|error| invalid(offset, HeaderRule::Equihash, error))?;

            // Accept current timestamps; assign an exact deadline to future ones.
            let canonical_header_time = header
                .time
                .with_nanosecond(0)
                .ok_or(HeaderFailure::ClockRange)?;
            let validation = if canonical_header_time > future_limit {
                HeaderValidationState::DeferredUntil(
                    canonical_header_time
                        .checked_sub_signed(Duration::hours(2))
                        .ok_or(HeaderFailure::ClockRange)?,
                )
            } else {
                HeaderValidationState::Valid
            };

            // Exact per-block work from the compact target.
            let block_work = header
                .difficulty_threshold
                .to_work()
                .ok_or_else(|| invalid(offset, HeaderRule::Work, "invalid compact target"))?;

            Ok(PreparedHeader {
                header: header.clone(),
                hash: *hash,
                height: *height,
                block_work,
                validation,
            })
        })
        .collect();
    let mut prepared = Vec::with_capacity(input.headers.len());
    for prepared_header in context_free {
        prepared.push(prepared_header?);
    }

    // Seal the batch to the parent, network policy, and trust-anchor digest.
    let evidence = PreparedHeaderBatch::context_free_evidence(
        parent_frontier,
        rules.trust_anchor_digest,
        &prepared,
    );
    PreparedHeaderBatch::new(
        prepared,
        parent_frontier,
        rules.network.clone(),
        rules.trust_anchor_digest,
        evidence,
    )
    .map_err(|error| invalid(0, HeaderRule::ValidationLease, error))
}
