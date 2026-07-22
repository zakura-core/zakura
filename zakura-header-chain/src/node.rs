//! Admitted header records and eligibility state.

use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};

use chrono::{DateTime, Utc};
use zakura_chain::{block, work::difficulty::Work};

use crate::{EvidenceId, Frontier, OperatorInvalidationId, WorkCoordinate};

/// Stable full-state consensus rule identity attached to body evidence.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRuleId(&'static str);

impl BodyRuleId {
    /// Construct a stable body-rule identity.
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    /// Return the stable identifier.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Observable validation state of an admitted header.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderValidationState {
    /// Every header rule, including the injected-clock rule, passed.
    Valid,
    /// Deterministic rules passed, but local time does not admit this header yet.
    DeferredUntil(DateTime<Utc>),
}

/// One durable reason that a header cannot participate in selection.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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
    /// A commitment-matching body deterministically failed consensus.
    ConsensusBodyInvalid {
        /// Stable verifier evidence.
        evidence: EvidenceId,
        /// Exact failed body rule.
        rule: BodyRuleId,
    },
    /// One independently reversible operator invalidation.
    OperatorInvalid {
        /// Exact invalidation to remove on reconsideration.
        id: OperatorInvalidationId,
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
                    .then_with(|| left_rule.cmp(right_rule)),
                (OperatorInvalid { id: left }, OperatorInvalid { id: right }) => left.cmp(right),
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
    /// Return true when resource retention may discard this permanently invalid subtree first.
    pub fn is_permanent(self) -> bool {
        !matches!(self, Self::OperatorInvalid { .. })
    }
}

/// Direct and ancestry-derived selection eligibility.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EligibilityState {
    /// Independent durable reasons attached directly to this header.
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
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
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
    /// Failed delivery attempts in the current episode.
    pub attempts: u32,
    /// Currently known eligible suppliers.
    pub suppliers: u32,
    /// Whether the persistent unavailability alarm has fired.
    pub alarmed: bool,
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
    pub body: BodyValidationState,
    /// Hash-keyed auxiliary delivery evidence IDs.
    pub aux_delivery_ids: Vec<EvidenceId>,
}

impl HeaderNode {
    /// Return true when this node currently participates in fork choice.
    pub fn is_eligible(&self) -> bool {
        self.eligibility.is_eligible(self.validation)
    }

    /// Return this node's checked cumulative work coordinate.
    pub(crate) const fn work_coordinate(&self) -> WorkCoordinate {
        self.work_coordinate
    }
}
