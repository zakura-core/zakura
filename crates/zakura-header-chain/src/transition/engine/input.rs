//! Engine-boundary transition inputs and the durable facts they may carry.

use crate::{
    AuxEvidence, BodyEvidence, BodySupplierDiscovered, FinalityRecord, Frontier,
    FullStateFinalized, InsertHeaders, MigratedPinRefutation, OperatorBodyRetry,
    OperatorInvalidate, OperatorReconsider, StateVersion, TransitionDomain, TransitionEvent,
    ValidationLease, VerifiedBlockAccepted, VerifiedChainChanged,
};

/// Durable predecessor leases used for contextual header validation.
///
/// # Adapter loading obligation
///
/// Before planning, the state adapter must load every [`ValidationLease`] needed
/// to reconstruct difficulty/time context for parents that are no longer retained
/// in the header graph. Leases must be coherent with the active network and trust
/// anchor and authorized via
/// [`crate::FullStateEvidenceAuthority::authorizes_validation_lease`]. Omitting
/// required leases fails planning as [`crate::TransitionFailure::MissingDurableFacts`],
/// not as store I/O.
#[derive(Clone, Debug, Default)]
pub struct HeaderValidationFacts {
    /// Exact predecessor leases available for missing retained parents.
    pub validation_leases: Vec<ValidationLease>,
}

/// Durable facts consumed by prepared header insertion, including finality rebase history.
///
/// # Adapter loading obligation
///
/// In addition to [`HeaderValidationFacts`], the adapter must supply the contiguous
/// append-only [`FinalityRecord`] chain from the work's stable finality anchor to
/// current finality whenever monotone finality may rebase the insertion. Missing
/// or non-contiguous rebase history fails as
/// [`crate::TransitionFailure::MissingDurableFacts`] or stale preparation—never as
/// a successful partial admit.
#[derive(Clone, Debug, Default)]
pub struct HeaderInsertionFacts {
    /// Predecessor leases for the original and rebased parents.
    pub validation: HeaderValidationFacts,
    /// Contiguous finality records from the work's stable anchor to current finality.
    pub finality_rebase_history: Vec<FinalityRecord>,
}

/// Engine-boundary package that binds one [`TransitionEvent`] to the durable
/// facts that event may consume.
///
/// The state write adapter builds this from a [`crate::TransitionRequest`]: it
/// authenticates the event, loads only the store rows that variant needs, and
/// hands the result to [`super::HeaderChainEngine::plan_transition`]. The planner
/// never reads the durable store itself.
///
/// Exhaustiveness is the contract. Each variant carries exactly its allowed
/// facts (for example validation leases and finality rebase history for header
/// insertion, or a preserved migration pin for pin refutation). Unrelated store
/// facts are unrepresentable.
///
/// Freshness is also variant-specific: most inputs are version-qualified via
/// `expected_version`, while [`Self::InsertHeaders`] and [`Self::AuxEvidence`]
/// omit it and rely on work ownership instead.
#[derive(Clone, Debug)]
pub enum TransitionInput {
    /// Prepared header admission with contextual leases and optional rebase history.
    InsertHeaders {
        /// Authenticated prepared insertion.
        event: Box<InsertHeaders>,
        /// Durable validation and rebase facts for this insertion.
        facts: HeaderInsertionFacts,
    },
    /// Full-state selected-path replacement with contextual header leases.
    VerifiedChainChanged {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated verified-path change.
        event: VerifiedChainChanged,
        /// Durable predecessor leases for missing path headers.
        facts: HeaderValidationFacts,
    },
    /// Full-state side-path acceptance with contextual header leases.
    VerifiedBlockAccepted {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated side-path acceptance.
        event: VerifiedBlockAccepted,
        /// Durable predecessor leases for missing path headers.
        facts: HeaderValidationFacts,
    },
    /// Body delivery or verification evidence.
    BodyEvidence {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated body evidence.
        event: BodyEvidence,
    },
    /// Newly eligible body-supplier discovery.
    BodySupplierDiscovered {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated supplier discovery.
        event: BodySupplierDiscovered,
    },
    /// Authenticated operator body retry.
    OperatorBodyRetry {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated retry.
        event: OperatorBodyRetry,
    },
    /// Reversible operator invalidation.
    OperatorInvalidate {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated invalidation.
        event: OperatorInvalidate,
    },
    /// Reason-scoped operator reconsideration.
    OperatorReconsider {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated reconsideration.
        event: OperatorReconsider,
    },
    /// Integrated full-state finality advancement.
    FullStateFinalized {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated finality evidence.
        event: FullStateFinalized,
    },
    /// Migrated headers-only pin refutation with the preserved durable pin fact.
    MigratedPinRefutation {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated refutation.
        event: MigratedPinRefutation,
        /// The exact preserved migration pin when durable history contains it.
        preserved_pin: Option<Frontier>,
    },
    /// Hash-scoped auxiliary evidence; freshness is owner-qualified.
    AuxEvidence {
        /// Authenticated auxiliary update.
        event: Box<AuxEvidence>,
    },
    /// Reevaluate all locally due future-time deferrals.
    ReevaluateDeferred {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
    },
}

impl TransitionInput {
    /// Return the submitted event domain.
    pub fn domain(&self) -> TransitionDomain {
        self.event().domain()
    }

    /// Return the typed event carried by this input.
    pub fn event(&self) -> TransitionEvent {
        match self {
            Self::InsertHeaders { event, .. } => TransitionEvent::InsertHeaders(event.clone()),
            Self::VerifiedChainChanged { event, .. } => {
                TransitionEvent::VerifiedChainChanged(event.clone())
            }
            Self::VerifiedBlockAccepted { event, .. } => {
                TransitionEvent::VerifiedBlockAccepted(event.clone())
            }
            Self::BodyEvidence { event, .. } => TransitionEvent::BodyEvidence(event.clone()),
            Self::BodySupplierDiscovered { event, .. } => {
                TransitionEvent::BodySupplierDiscovered(*event)
            }
            Self::OperatorBodyRetry { event, .. } => TransitionEvent::OperatorBodyRetry(*event),
            Self::OperatorInvalidate { event, .. } => TransitionEvent::OperatorInvalidate(*event),
            Self::OperatorReconsider { event, .. } => TransitionEvent::OperatorReconsider(*event),
            Self::FullStateFinalized { event, .. } => {
                TransitionEvent::FullStateFinalized(event.clone())
            }
            Self::MigratedPinRefutation { event, .. } => {
                TransitionEvent::MigratedPinRefutation(event.clone())
            }
            Self::AuxEvidence { event } => TransitionEvent::AuxEvidence(event.clone()),
            Self::ReevaluateDeferred { .. } => TransitionEvent::ReevaluateDeferred,
        }
    }

    /// Return the caller-observed durable version when the input is version-qualified.
    ///
    /// Owner-qualified insertion and auxiliary inputs return `None` because their
    /// freshness is enforced by work ownership rather than state version.
    pub const fn expected_version(&self) -> Option<StateVersion> {
        match self {
            Self::InsertHeaders { .. } | Self::AuxEvidence { .. } => None,
            Self::VerifiedChainChanged {
                expected_version, ..
            }
            | Self::VerifiedBlockAccepted {
                expected_version, ..
            }
            | Self::BodyEvidence {
                expected_version, ..
            }
            | Self::BodySupplierDiscovered {
                expected_version, ..
            }
            | Self::OperatorBodyRetry {
                expected_version, ..
            }
            | Self::OperatorInvalidate {
                expected_version, ..
            }
            | Self::OperatorReconsider {
                expected_version, ..
            }
            | Self::FullStateFinalized {
                expected_version, ..
            }
            | Self::MigratedPinRefutation {
                expected_version, ..
            }
            | Self::ReevaluateDeferred { expected_version } => Some(*expected_version),
        }
    }

    /// Return header-validation leases when this input carries them.
    pub fn header_validation_facts(&self) -> Option<&HeaderValidationFacts> {
        match self {
            Self::InsertHeaders { facts, .. } => Some(&facts.validation),
            Self::VerifiedChainChanged { facts, .. }
            | Self::VerifiedBlockAccepted { facts, .. } => Some(facts),
            _ => None,
        }
    }

    /// Return finality rebase history when this input is a header insertion.
    pub fn finality_rebase_history(&self) -> Option<&[FinalityRecord]> {
        match self {
            Self::InsertHeaders { facts, .. } => Some(&facts.finality_rebase_history),
            _ => None,
        }
    }

    /// Return the preserved migrated pin fact when this input is a pin refutation.
    pub const fn preserved_migrated_pin(&self) -> Option<Option<Frontier>> {
        match self {
            Self::MigratedPinRefutation { preserved_pin, .. } => Some(*preserved_pin),
            _ => None,
        }
    }
}
