//! Sealed validation of complete observable-header batches.

use std::sync::Arc;

use chrono::{Duration, Timelike};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{block, parameters::Network};

use super::{
    infer_height, validate_commitment_structure, validate_compact_target,
    validate_contextual_difficulty_and_time, validate_encoding_version_hash, validate_hash_filter,
    AdjustedDifficulty, PowPolicy, PowPolicyError, POW_ADJUSTMENT_BLOCK_SPAN,
};
use crate::{
    Clock, EngineConfig, EvidenceId, HeaderContextFact, HeaderValidationState, PreparedHeader,
    PreparedHeaderBatch, RuleId, ValidationLease,
};

/// Ordered, nonempty canonical headers to validate against one exact parent lease.
#[derive(Copy, Clone, Debug)]
pub struct HeaderBatchInput<'a> {
    /// Headers in exact parent-first wire order.
    pub headers: &'a [Arc<block::Header>],
}

impl<'a> HeaderBatchInput<'a> {
    /// Construct an input over one complete target response assembled by the requester.
    pub const fn new(headers: &'a [Arc<block::Header>]) -> Self {
        Self { headers }
    }
}

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

/// Stable preparation stage used for peer attribution and conformance diagnostics.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderRule {
    /// Canonical signed version and full-header hash.
    EncodingVersionHash,
    /// Exact parent and internal linkage.
    ParentLink,
    /// Checked local height inference.
    InferredHeight,
    /// Height-dependent commitment interpretation.
    CommitmentStructure,
    /// Compact target domain and network limit.
    CompactTarget,
    /// Header hash at or below its target.
    HashToTarget,
    /// Network-bound Equihash shape and proof.
    Equihash,
    /// Branch-local target adjustment and median-time rules.
    ContextualDifficultyAndTime,
    /// Local-clock future-header classification.
    LocalFutureTime,
    /// Exact durable validation lease and trust-anchor identity.
    ValidationLease,
    /// Exact per-block work calculation.
    Work,
}

impl HeaderRule {
    /// Return every normative rule implemented by this validation stage.
    pub const fn rule_ids(self) -> &'static [RuleId] {
        const ENCODING_VERSION_HASH: &[RuleId] = &[RuleId::new("LC-VAL-02")];
        const PARENT_LINK: &[RuleId] = &[RuleId::new("LC-VAL-03")];
        const INFERRED_HEIGHT: &[RuleId] = &[RuleId::new("LC-HEIGHT-01")];
        const COMMITMENT_STRUCTURE: &[RuleId] =
            &[RuleId::new("LC-COMMIT-01"), RuleId::new("LC-COMMIT-02")];
        const TARGET: &[RuleId] = &[RuleId::new("LC-VAL-05")];
        const EQUIHASH: &[RuleId] = &[RuleId::new("LC-VAL-04")];
        const CONTEXTUAL_DIFFICULTY_AND_TIME: &[RuleId] = &[
            RuleId::new("LC-VAL-06"),
            RuleId::new("LC-VAL-07"),
            RuleId::new("LC-TIME-01"),
        ];
        const LOCAL_FUTURE_TIME: &[RuleId] = &[RuleId::new("LC-VAL-08")];
        const VALIDATION_LEASE: &[RuleId] =
            &[RuleId::new("LC-ANCHOR-03"), RuleId::new("LC-VAL-11")];
        const WORK: &[RuleId] = &[RuleId::new("LC-VAL-10")];

        match self {
            Self::EncodingVersionHash => ENCODING_VERSION_HASH,
            Self::ParentLink => PARENT_LINK,
            Self::InferredHeight => INFERRED_HEIGHT,
            Self::CommitmentStructure => COMMITMENT_STRUCTURE,
            Self::CompactTarget | Self::HashToTarget => TARGET,
            Self::Equihash => EQUIHASH,
            Self::ContextualDifficultyAndTime => CONTEXTUAL_DIFFICULTY_AND_TIME,
            Self::LocalFutureTime => LOCAL_FUTURE_TIME,
            Self::ValidationLease => VALIDATION_LEASE,
            Self::Work => WORK,
        }
    }
}

/// Failure to prepare a batch.
/// A successful batch can represent only a local future-time deferral.
#[derive(Debug, Error)]
pub enum HeaderFailure {
    /// The caller supplied no headers for an insertion event.
    #[error("header batch is empty")]
    Empty,
    /// The batch exceeded the frozen per-transition header bound.
    #[error("header batch has {actual} entries, maximum {maximum}")]
    Oversized {
        /// Supplied header count.
        actual: usize,
        /// Frozen maximum header count.
        maximum: usize,
    },
    /// Durable state supplied an incoherent or stale validation lease.
    #[error("validation lease is incoherent with the authenticated header rules")]
    InvalidLease,
    /// One deterministic observable-header rule failed.
    #[error("header at offset {offset} failed {rule:?}: {reason}")]
    Invalid {
        /// Zero-based header offset.
        offset: usize,
        /// Exact failed stage.
        rule: HeaderRule,
        /// Stable human-readable source description.
        reason: String,
    },
    /// A local time calculation exceeded the representable timestamp range.
    #[error("local future-time boundary is outside the representable timestamp range")]
    ClockRange,
}

fn invalid(offset: usize, rule: HeaderRule, error: impl std::fmt::Display) -> HeaderFailure {
    HeaderFailure::Invalid {
        offset,
        rule,
        reason: error.to_string(),
    }
}

/// Validate a complete batch without reading retained graph state.
pub fn prepare_context_free_headers(
    input: HeaderBatchInput<'_>,
    parent: crate::Frontier,
    rules: &HeaderRules,
    clock: &dyn Clock,
) -> Result<PreparedHeaderBatch, HeaderFailure> {
    prepare_headers_inner(input, parent, None, rules, clock)
}

/// Validate a complete batch without mutation and seal its results to `lease`.
///
/// The compatibility entry point retains contextual validation until production callers have
/// moved to [`prepare_context_free_headers`] and the engine performs the graph-dependent rules.
pub fn prepare_headers(
    input: HeaderBatchInput<'_>,
    lease: &ValidationLease,
    rules: &HeaderRules,
    clock: &dyn Clock,
) -> Result<PreparedHeaderBatch, HeaderFailure> {
    if !lease.is_coherent(&rules.network, rules.trust_anchor_digest) {
        return Err(HeaderFailure::InvalidLease);
    }
    prepare_headers_inner(input, lease.parent, Some(lease), rules, clock)
}

fn prepare_headers_inner(
    input: HeaderBatchInput<'_>,
    parent_frontier: crate::Frontier,
    contextual_lease: Option<&ValidationLease>,
    rules: &HeaderRules,
    clock: &dyn Clock,
) -> Result<PreparedHeaderBatch, HeaderFailure> {
    if input.headers.is_empty() {
        return Err(HeaderFailure::Empty);
    }
    if input.headers.len() > crate::MAX_HEADERS_PER_TRANSITION_V1 {
        return Err(HeaderFailure::Oversized {
            actual: input.headers.len(),
            maximum: crate::MAX_HEADERS_PER_TRANSITION_V1,
        });
    }

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
    if contextual_lease.is_some() {
        let mut expected_parent = parent_frontier.hash;
        for (offset, (header, hash)) in input.headers.iter().zip(&hashes).enumerate() {
            if header.previous_block_hash != expected_parent {
                let error = super::HeaderLinkError {
                    offset,
                    expected: expected_parent,
                    actual: header.previous_block_hash,
                };
                return Err(invalid(offset, HeaderRule::ParentLink, error));
            }
            expected_parent = *hash;
        }
    }

    let now = clock.now();
    let future_limit = now
        .checked_add_signed(Duration::hours(2))
        .ok_or(HeaderFailure::ClockRange)?;
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
            validate_commitment_structure(header, &rules.network, *height)
                .map_err(|error| invalid(offset, HeaderRule::CommitmentStructure, error))?;
            let target = validate_compact_target(header, &rules.network)
                .map_err(|error| invalid(offset, HeaderRule::CompactTarget, error))?;
            if !rules.pow_policy.is_authenticated_custom_waiver() {
                validate_hash_filter(*hash, target)
                    .map_err(|error| invalid(offset, HeaderRule::HashToTarget, error))?;
            }
            rules
                .pow_policy
                .validate_solution(header)
                .map_err(|error| invalid(offset, HeaderRule::Equihash, error))?;

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
    let mut parent = parent_frontier;
    let mut context = contextual_lease.map(|lease| lease.predecessors.clone());
    let mut prepared = Vec::with_capacity(input.headers.len());

    for (offset, prepared_header) in context_free.into_iter().enumerate() {
        let prepared_header = prepared_header?;
        let header = &prepared_header.header;
        if let Some(context) = context.as_ref() {
            let adjustment = AdjustedDifficulty::new_from_header_time(
                header.time,
                parent.height,
                &rules.network,
                context
                    .iter()
                    .map(|fact| (fact.header.difficulty_threshold, fact.header.time)),
            )
            .map_err(|error| {
                invalid(
                    offset,
                    HeaderRule::ContextualDifficultyAndTime,
                    error.to_string(),
                )
            })?;
            validate_contextual_difficulty_and_time(header.difficulty_threshold, adjustment)
                .map_err(|error| invalid(offset, HeaderRule::ContextualDifficultyAndTime, error))?;
        }
        parent = crate::Frontier::new(prepared_header.height, prepared_header.hash);
        if let Some(context) = context.as_mut() {
            context.insert(
                0,
                HeaderContextFact {
                    frontier: parent,
                    header: header.clone(),
                },
            );
            context.truncate(POW_ADJUSTMENT_BLOCK_SPAN);
        }
        prepared.push(prepared_header);
    }

    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-context-free-batch-v1");
    hasher.update(parent_frontier.height.0.to_le_bytes());
    hasher.update(parent_frontier.hash.0);
    hasher.update(rules.trust_anchor_digest);
    for header in &prepared {
        hasher.update(header.height.0.to_le_bytes());
        hasher.update(header.hash.0);
    }
    PreparedHeaderBatch::new(
        prepared,
        parent_frontier,
        rules.network.clone(),
        rules.trust_anchor_digest,
        EvidenceId::from_digest(hasher.finalize().into()),
    )
    .map_err(|error| invalid(0, HeaderRule::ValidationLease, error))
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use zakura_chain::{
        block::genesis::regtest_genesis_block,
        parameters::{testnet::RegtestParameters, Network},
    };

    use super::*;
    use crate::{CheckpointSet, EngineMode, Frontier, TrustedAnchor};

    #[derive(Copy, Clone)]
    struct FixedClock(DateTime<Utc>);

    impl Clock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    fn fixture() -> (HeaderRules, ValidationLease, Arc<block::Header>) {
        let anchor_header = regtest_genesis_block().header.clone();
        let anchor = Frontier::new(block::Height(0), anchor_header.hash());
        let network = Network::new_regtest(RegtestParameters::default());
        let config = EngineConfig::new(
            EngineMode::Integrated,
            network,
            TrustedAnchor {
                frontier: anchor,
                header: anchor_header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the regtest anchor and release manifest are coherent");
        let rules = HeaderRules::from_engine_config(&config)
            .expect("authenticated regtest parameters define their PoW policy");
        let lease = ValidationLease::new(
            anchor,
            vec![HeaderContextFact {
                frontier: anchor,
                header: anchor_header.clone(),
            }],
            config.network.clone(),
            config.trust_anchor_digest(),
        );
        (rules, lease, anchor_header)
    }

    fn child(parent: Frontier, template: &block::Header, seconds: i64) -> Arc<block::Header> {
        Arc::new(block::Header {
            previous_block_hash: parent.hash,
            time: template.time + Duration::seconds(seconds),
            nonce: [u8::try_from(seconds).unwrap_or(u8::MAX); 32].into(),
            ..*template
        })
    }

    #[test]
    fn complete_batch_is_sealed_to_lease_and_uses_internal_context() {
        let (rules, lease, anchor) = fixture();
        let first = child(lease.parent, &anchor, 1);
        let first_frontier = Frontier::new(block::Height(1), first.hash());
        let second = child(first_frontier, &first, 2);
        let headers = [first, second];

        let batch = prepare_headers(
            HeaderBatchInput::new(&headers),
            &lease,
            &rules,
            &FixedClock(anchor.time + Duration::hours(1)),
        )
        .expect("the continuous custom-network batch is valid");

        assert_eq!(batch.receipt().parent(), lease.parent());
        assert_eq!(
            batch.receipt().trust_anchor_digest(),
            lease.trust_anchor_digest()
        );
        assert_eq!(batch.headers().len(), 2);
        assert_eq!(batch.headers()[0].height, block::Height(1));
        assert_eq!(batch.headers()[1].height, block::Height(2));
        assert_eq!(
            batch.headers()[1].header.previous_block_hash,
            headers[0].hash()
        );
    }

    #[test]
    fn future_time_is_deferred_but_deterministic_failures_are_rejected() {
        let (rules, lease, anchor) = fixture();
        let future = child(lease.parent, &anchor, 3 * 60 * 60);
        let now = anchor.time;
        let batch = prepare_headers(
            HeaderBatchInput::new(std::slice::from_ref(&future)),
            &lease,
            &rules,
            &FixedClock(now),
        )
        .expect("local future time is admitted only as deferred");
        assert_eq!(
            batch.headers()[0].validation,
            HeaderValidationState::DeferredUntil(future.time - Duration::hours(2))
        );

        let mut disconnected = *future;
        disconnected.previous_block_hash = block::Hash([0x55; 32]);
        let disconnected = Arc::new(disconnected);
        assert!(matches!(
            prepare_headers(
                HeaderBatchInput::new(std::slice::from_ref(&disconnected)),
                &lease,
                &rules,
                &FixedClock(now),
            ),
            Err(HeaderFailure::Invalid {
                rule: HeaderRule::ParentLink,
                ..
            })
        ));
    }

    #[test]
    fn oversized_batch_is_rejected_before_header_validation() {
        let (rules, lease, anchor) = fixture();
        let header = child(lease.parent, &anchor, 1);
        let headers = vec![header; crate::MAX_HEADERS_PER_TRANSITION_V1 + 1];
        assert!(matches!(
            prepare_headers(
                HeaderBatchInput::new(&headers),
                &lease,
                &rules,
                &FixedClock(anchor.time),
            ),
            Err(HeaderFailure::Oversized {
                actual,
                maximum: crate::MAX_HEADERS_PER_TRANSITION_V1,
            }) if actual == crate::MAX_HEADERS_PER_TRANSITION_V1 + 1
        ));
    }

    #[test]
    fn context_free_receipt_excludes_parent_and_branch_context_claims() {
        let (rules, lease, anchor) = fixture();
        let mut disconnected = *child(lease.parent, &anchor, 0);
        disconnected.previous_block_hash = block::Hash([0x55; 32]);
        let disconnected = Arc::new(disconnected);

        let batch = prepare_context_free_headers(
            HeaderBatchInput::new(std::slice::from_ref(&disconnected)),
            lease.parent(),
            &rules,
            &FixedClock(anchor.time),
        )
        .expect("graph-independent preparation does not claim parent linkage or MTP");

        assert_eq!(batch.receipt().parent(), lease.parent());
        assert_eq!(
            batch.receipt().trust_anchor_digest(),
            rules.trust_anchor_digest()
        );
        assert_eq!(batch.headers()[0].hash, disconnected.hash());
        assert_eq!(batch.headers()[0].height, block::Height(1));
    }

    #[test]
    fn invalid_version_is_rejected_before_link_hashing() {
        let (rules, lease, anchor) = fixture();
        let mut invalid = *child(lease.parent, &anchor, 1);
        invalid.version = 3;
        let invalid = Arc::new(invalid);

        assert!(matches!(
            prepare_headers(
                HeaderBatchInput::new(std::slice::from_ref(&invalid)),
                &lease,
                &rules,
                &FixedClock(anchor.time),
            ),
            Err(HeaderFailure::Invalid {
                rule: HeaderRule::EncodingVersionHash,
                ..
            })
        ));
    }

    #[test]
    fn empty_and_mutated_leases_fail_before_header_validation() {
        let (rules, lease, anchor) = fixture();
        assert!(matches!(
            prepare_headers(
                HeaderBatchInput::new(&[]),
                &lease,
                &rules,
                &FixedClock(anchor.time),
            ),
            Err(HeaderFailure::Empty)
        ));

        let mut mutated = lease.clone();
        mutated.parent.height = block::Height(1);
        let header = child(lease.parent, &anchor, 1);
        assert!(matches!(
            prepare_headers(
                HeaderBatchInput::new(std::slice::from_ref(&header)),
                &mutated,
                &rules,
                &FixedClock(anchor.time),
            ),
            Err(HeaderFailure::InvalidLease)
        ));

        let high_parent = Frontier::new(block::Height(100), lease.parent.hash);
        let short_lease = ValidationLease::new(
            high_parent,
            vec![HeaderContextFact {
                frontier: high_parent,
                header: anchor.clone(),
            }],
            lease.network.clone(),
            lease.trust_anchor_digest,
        );
        let header = child(high_parent, &anchor, 1);
        assert!(matches!(
            prepare_headers(
                HeaderBatchInput::new(std::slice::from_ref(&header)),
                &short_lease,
                &rules,
                &FixedClock(anchor.time),
            ),
            Err(HeaderFailure::InvalidLease)
        ));
    }

    #[test]
    fn validation_stages_expose_their_exact_normative_rule_ids() {
        let cases: &[(HeaderRule, &[RuleId])] = &[
            (HeaderRule::EncodingVersionHash, &[RuleId::new("LC-VAL-02")]),
            (HeaderRule::ParentLink, &[RuleId::new("LC-VAL-03")]),
            (HeaderRule::InferredHeight, &[RuleId::new("LC-HEIGHT-01")]),
            (
                HeaderRule::CommitmentStructure,
                &[RuleId::new("LC-COMMIT-01"), RuleId::new("LC-COMMIT-02")],
            ),
            (HeaderRule::CompactTarget, &[RuleId::new("LC-VAL-05")]),
            (HeaderRule::HashToTarget, &[RuleId::new("LC-VAL-05")]),
            (HeaderRule::Equihash, &[RuleId::new("LC-VAL-04")]),
            (
                HeaderRule::ContextualDifficultyAndTime,
                &[
                    RuleId::new("LC-VAL-06"),
                    RuleId::new("LC-VAL-07"),
                    RuleId::new("LC-TIME-01"),
                ],
            ),
            (HeaderRule::LocalFutureTime, &[RuleId::new("LC-VAL-08")]),
            (
                HeaderRule::ValidationLease,
                &[RuleId::new("LC-ANCHOR-03"), RuleId::new("LC-VAL-11")],
            ),
            (HeaderRule::Work, &[RuleId::new("LC-VAL-10")]),
        ];

        for (stage, expected) in cases {
            assert_eq!(stage.rule_ids(), *expected, "{stage:?}");
        }
    }
}
