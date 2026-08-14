//! Admitted header records and eligibility state.

use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{block, work::difficulty::Work};

use super::{Frontier, WorkCoordinate};
use crate::{EvidenceId, OperatorInvalidationId, SourceId};

/// Stable full-state consensus rule identity attached to body evidence.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRuleId(Arc<str>);

impl BodyRuleId {
    /// Construct a stable body-rule identity.
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    /// Return the stable identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Observable validation state of an admitted header.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderValidationState {
    /// Every header rule, including the injected-clock rule, passed.
    Valid,
    /// The header passed deterministic rules.
    /// Local time does not admit the header yet.
    DeferredUntil(DateTime<Utc>),
}

/// An `EligibilityReason` excludes an admitted header from selection.
///
/// The graph admits the header before it attaches an eligibility reason. Each
/// eligibility reason excludes the header and its descendants from selection.
/// The graph retains the header and its header-validation state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EligibilityReason {
    /// Header conflicts with a compiled settled-upgrade pin.
    SettledUpgradeConflict {
        /// Conflicting height.
        height: block::Height,
        /// Required hash.
        expected: block::Hash,
    },
    /// Header conflicts with an authenticated local checkpoint.
    CheckpointConflict {
        /// Conflicting height.
        height: block::Height,
        /// Required hash.
        expected: block::Hash,
    },
    /// Header conflicts at or below the immutable finality anchor.
    FinalityConflict {
        /// Current exact finality anchor.
        finalized: Frontier,
    },
    /// A full-state verifier proved that the corresponding body failed consensus.
    ConsensusBodyInvalid {
        /// Exact authoritative evidence identity.
        evidence: EvidenceId,
        /// Exact failed consensus rule.
        rule: BodyRuleId,
    },
    /// One independently reversible operator invalidation.
    OperatorInvalid {
        /// Exact invalidation to remove on reconsideration.
        id: OperatorInvalidationId,
        /// Canonical digest binding this reason to its target and identity.
        reason_digest: [u8; 32],
        /// Authenticated action evidence that introduced this reason.
        evidence: EvidenceId,
    },
}

impl Ord for EligibilityReason {
    fn cmp(&self, other: &Self) -> Ordering {
        use EligibilityReason::*;

        let rank = |reason: &Self| match reason {
            SettledUpgradeConflict { .. } => 0,
            CheckpointConflict { .. } => 1,
            FinalityConflict { .. } => 2,
            ConsensusBodyInvalid { .. } => 3,
            OperatorInvalid { .. } => 4,
        };
        rank(self)
            .cmp(&rank(other))
            .then_with(|| match (self, other) {
                (
                    SettledUpgradeConflict {
                        height: left_height,
                        expected: left_hash,
                    },
                    SettledUpgradeConflict {
                        height: right_height,
                        expected: right_hash,
                    },
                )
                | (
                    CheckpointConflict {
                        height: left_height,
                        expected: left_hash,
                    },
                    CheckpointConflict {
                        height: right_height,
                        expected: right_hash,
                    },
                ) => left_height
                    .cmp(right_height)
                    .then_with(|| left_hash.0.cmp(&right_hash.0)),
                (FinalityConflict { finalized: left }, FinalityConflict { finalized: right }) => {
                    left.height
                        .cmp(&right.height)
                        .then_with(|| left.hash.0.cmp(&right.hash.0))
                }
                (
                    ConsensusBodyInvalid {
                        evidence: left_evidence,
                        rule: left_rule,
                    },
                    ConsensusBodyInvalid {
                        evidence: right_evidence,
                        rule: right_rule,
                    },
                ) => left_evidence
                    .cmp(right_evidence)
                    .then_with(|| left_rule.as_str().cmp(right_rule.as_str())),
                (
                    OperatorInvalid {
                        id: left,
                        reason_digest: left_digest,
                        evidence: left_evidence,
                    },
                    OperatorInvalid {
                        id: right,
                        reason_digest: right_digest,
                        evidence: right_evidence,
                    },
                ) => left
                    .cmp(right)
                    .then_with(|| left_digest.cmp(right_digest))
                    .then_with(|| left_evidence.cmp(right_evidence)),
                _ => Ordering::Equal,
            })
    }
}

impl PartialOrd for EligibilityReason {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl EligibilityReason {
    pub(crate) fn operator_invalid(
        target: block::Hash,
        id: OperatorInvalidationId,
        evidence: EvidenceId,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-operator-invalidation-v1");
        hasher.update(target.0);
        hasher.update(id.bytes());
        Self::OperatorInvalid {
            id,
            reason_digest: hasher.finalize().into(),
            evidence,
        }
    }

    /// Return true when resource retention may discard this permanently invalid subtree first.
    pub fn is_permanent(&self) -> bool {
        !matches!(self, Self::OperatorInvalid { .. })
    }
}

/// Direct and ancestry-derived selection eligibility.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EligibilityState {
    /// The graph attaches these independent eligibility reasons to this header.
    pub direct_reasons: BTreeSet<EligibilityReason>,
    /// Nearest ineligible ancestor, if any.
    pub inherited_from: Option<block::Hash>,
}

impl EligibilityState {
    /// Return true when neither this header nor any ancestor is ineligible.
    pub fn is_eligible(&self, validation: HeaderValidationState) -> bool {
        validation == HeaderValidationState::Valid
            && self.direct_reasons.is_empty()
            && self.inherited_from.is_none()
    }

    /// Return true when this header has at least one permanent direct reason.
    pub fn has_permanent_reason(&self) -> bool {
        self.direct_reasons
            .iter()
            .any(|reason| reason.is_permanent())
    }
}

/// Full-state knowledge about a header's corresponding block body.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum BodyValidationState {
    /// No body conclusion is known.
    #[default]
    Unknown,
    /// Applicable header/body commitments matched.
    CommitmentMatched,
    /// Full state accepted this body.
    Verified {
        /// Stable verification evidence.
        evidence: EvidenceId,
    },
    /// A commitment-matching body deterministically failed consensus.
    ConsensusInvalid {
        /// Stable verification evidence.
        evidence: EvidenceId,
        /// Exact failed body rule.
        rule: BodyRuleId,
    },
    /// Body acquisition is temporarily unavailable and does not affect selection.
    Unavailable(BodyUnavailableSummary),
}

/// Durable bounded summary of one body-unavailability retry episode.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct BodyUnavailableSummary {
    /// Authoritative start of the current retry episode.
    pub started_at: DateTime<Utc>,
    /// Failed delivery attempts in the current episode.
    pub attempts: u32,
    /// Currently known eligible suppliers.
    pub suppliers: u32,
    /// Stable digest of the complete eligible-supplier identity set.
    pub supplier_set_digest: [u8; 32],
    /// Whether the persistent unavailability alarm has fired.
    pub alarmed: bool,
    /// Earliest time the scheduler permits another repeated-supplier attempt or alarm probe.
    pub next_probe_at: DateTime<Utc>,
}

impl BodyUnavailableSummary {
    /// Return a stable digest of one complete sorted eligible-supplier set.
    pub fn supplier_set_digest(suppliers: &BTreeSet<SourceId>) -> [u8; 32] {
        let mut state = Sha256::new();
        state.update(b"zakura-body-supplier-set-v1");
        for supplier in suppliers {
            state.update(supplier.digest());
        }
        state.finalize().into()
    }
}

/// One retained header DAG node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderNode {
    /// Canonical decoded header.
    pub header: Arc<block::Header>,
    /// Locally computed canonical header hash.
    pub hash: block::Hash,
    /// Exact parent hash.
    pub parent_hash: block::Hash,
    /// Locally inferred height.
    pub height: block::Height,
    /// Exact per-block work.
    pub block_work: Work,
    pub(crate) work_coordinate: WorkCoordinate,
    /// Observable header-validation state.
    pub validation: HeaderValidationState,
    /// Direct and inherited eligibility state.
    pub eligibility: EligibilityState,
    /// Full-state body knowledge.
    pub body_validation_state: BodyValidationState,
    /// Hash-keyed auxiliary delivery evidence IDs.
    pub aux_delivery_ids: Vec<EvidenceId>,
}

impl HeaderNode {
    /// Return true when this node currently participates in fork choice.
    pub fn is_eligible(&self) -> bool {
        self.eligibility.is_eligible(self.validation)
            && !matches!(
                self.body_validation_state,
                BodyValidationState::ConsensusInvalid { .. }
            )
    }

    /// Return this node's checked cumulative work coordinate.
    pub const fn work_coordinate(&self) -> WorkCoordinate {
        self.work_coordinate
    }

    /// Reconstruct one node from audited durable fields.
    #[allow(clippy::too_many_arguments)]
    pub fn from_durable_parts(
        header: Arc<block::Header>,
        hash: block::Hash,
        parent_hash: block::Hash,
        height: block::Height,
        block_work: Work,
        work_coordinate: WorkCoordinate,
        validation: HeaderValidationState,
        eligibility: EligibilityState,
        body: BodyValidationState,
        aux_delivery_ids: Vec<EvidenceId>,
    ) -> Result<Self, DurableNodeError> {
        if header.hash() != hash {
            return Err(DurableNodeError::Hash);
        }
        if header.previous_block_hash != parent_hash {
            return Err(DurableNodeError::Parent);
        }
        if header.difficulty_threshold.to_work() != Some(block_work) {
            return Err(DurableNodeError::Work);
        }
        Ok(Self {
            header,
            hash,
            parent_hash,
            height,
            block_work,
            work_coordinate,
            validation,
            eligibility,
            body_validation_state: body,
            aux_delivery_ids,
        })
    }
}

/// A durable node row contradicted its canonical header fields.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum DurableNodeError {
    /// The stored hash did not match the canonical header.
    #[error("durable header hash mismatch")]
    Hash,
    /// The stored parent did not match the canonical header link.
    #[error("durable header parent mismatch")]
    Parent,
    /// The stored per-block work did not match the compact target.
    #[error("durable header work mismatch")]
    Work,
}
