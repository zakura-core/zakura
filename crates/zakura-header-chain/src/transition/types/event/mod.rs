//! Authenticated transition events and version-qualified requests.

mod auxiliary_evidence;
mod body;
mod finality;
mod header;
mod operator;
mod replay;
mod verified;

pub use auxiliary_evidence::AuxEvidence;
pub use body::{
    BodyCommitmentKind, BodyEvidence, BodyPayloadMismatch, BodySupplierDiscovered,
    BodyVerificationClass, BodyVerificationOutcome, ConsensusBodyInvalid, OperatorBodyRetry,
    TransientBodyFailure, TransientBodyFailureKind, VerifiedBodyEvidence,
};
pub use finality::{FullStateFinalized, MigratedPinRefutation};
pub use header::{InsertHeaders, TargetCompletion};
pub use operator::{OperatorInvalidate, OperatorReconsider};
pub use replay::{EventAdmission, TransitionDomain, TransitionFingerprint};
pub use verified::{
    VerifiedBlockAccepted, VerifiedChainChanged, VerifiedChangeCause, VerifiedHeaderRef,
};

use crate::{AuxAuthentication, BodyWorkOwner, EvidenceId, HeaderSyncWorkOwner, StateVersion};

use replay::hash_transition_payload;
use sha2::{Digest, Sha256};

/// Authenticated evidence that one state-service transition may apply.
///
/// The state write path builds a [`TransitionRequest`] around this value. Peers
/// and consensus never submit it directly: they produce higher-level work that
/// the adapter authenticates and classifies into one of these variants.
///
/// An event records only what happened (header admission, body evidence,
/// operator action, and so on). It does not carry durable store facts; the
/// adapter binds those separately into [`crate::TransitionInput`] before the
/// engine plans. Callers also never submit desired consequences—the planner
/// derives effects from the event and coherent state.
///
/// Domains that are replay-protected hash this payload into
/// [`TransitionFingerprint`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionEvent {
    /// Prepared header admission.
    InsertHeaders(Box<InsertHeaders>),
    /// Full-state selected path changed.
    VerifiedChainChanged(VerifiedChainChanged),
    /// Full state accepted a block without changing its selected path.
    VerifiedBlockAccepted(VerifiedBlockAccepted),
    /// Body delivery/verification evidence.
    BodyEvidence(BodyEvidence),
    /// A newly eligible supplier restarted body acquisition.
    BodySupplierDiscovered(BodySupplierDiscovered),
    /// An authenticated operator restarted body acquisition.
    OperatorBodyRetry(OperatorBodyRetry),
    /// Reversible operator invalidation.
    OperatorInvalidate(OperatorInvalidate),
    /// Reason-scoped operator reconsideration.
    OperatorReconsider(OperatorReconsider),
    /// Integrated full-state finality advancement.
    FullStateFinalized(FullStateFinalized),
    /// Integrated full state refuted an imported headers-only pin.
    MigratedPinRefutation(MigratedPinRefutation),
    /// Hash-scoped auxiliary evidence.
    AuxEvidence(Box<AuxEvidence>),
    /// Reevaluate all locally due future-time deferrals.
    ReevaluateDeferred,
}

impl TransitionEvent {
    /// Return the authority gate fixed for this event category.
    pub fn admission(&self) -> EventAdmission {
        match self {
            Self::VerifiedChainChanged(_)
            | Self::VerifiedBlockAccepted(_)
            | Self::BodyEvidence(_)
            | Self::BodySupplierDiscovered(_)
            | Self::FullStateFinalized(_)
            | Self::MigratedPinRefutation(_)
            | Self::AuxEvidence(_)
            | Self::OperatorInvalidate(_)
            | Self::OperatorReconsider(_) => EventAdmission::IntegratedFullState,
            Self::OperatorBodyRetry(_) => EventAdmission::RegisteredScheduler,
            Self::InsertHeaders(_) => EventAdmission::RegisteredHeaderCompletion,
            Self::ReevaluateDeferred => EventAdmission::AnyMode,
        }
    }

    /// Return this event's stable idempotency identity when it carries durable evidence.
    pub fn idempotency_key(&self) -> Option<EvidenceId> {
        match self {
            Self::InsertHeaders(event) => match event.completion {
                TargetCompletion::SelectedAuxiliaryRepair { .. } => {
                    event.aux.first().map(|delivery| delivery.delivery_id)
                }
                TargetCompletion::TargetComplete { .. } | TargetCompletion::TargetPrefix { .. } => {
                    Some(event.batch.evidence())
                }
            },
            Self::VerifiedChainChanged(event) => Some(event.full_state_transition_id),
            Self::VerifiedBlockAccepted(event) => Some(event.full_state_transition_id),
            Self::BodyEvidence(BodyEvidence::PayloadMismatch(event)) => Some(event.evidence),
            Self::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => Some(event.evidence),
            Self::BodyEvidence(BodyEvidence::Transient(event)) => Some(event.evidence),
            Self::BodyEvidence(BodyEvidence::Verified(event)) => Some(event.evidence),
            Self::BodySupplierDiscovered(event) => Some(event.evidence),
            Self::OperatorBodyRetry(event) => Some(event.evidence),
            Self::OperatorInvalidate(event) => Some(event.evidence),
            Self::OperatorReconsider(event) => Some(event.evidence),
            Self::FullStateFinalized(event) => Some(event.full_state_transition_id),
            Self::MigratedPinRefutation(event) => Some(event.full_state_transition_id),
            Self::AuxEvidence(event) => match event.authentication {
                AuxAuthentication::Unauthenticated => None,
                AuxAuthentication::Authenticated { evidence, .. }
                | AuxAuthentication::Rejected { evidence }
                | AuxAuthentication::Disputed { evidence } => Some(evidence),
            },
            Self::ReevaluateDeferred => None,
        }
    }

    /// Return the domain-separated canonical replay fingerprint, when replay protection applies.
    pub fn fingerprint(&self) -> Option<TransitionFingerprint> {
        let evidence = self.idempotency_key()?;
        let domain = match self {
            Self::InsertHeaders(_) => TransitionDomain::InsertHeaders,
            Self::VerifiedChainChanged(_) => TransitionDomain::VerifiedChainChanged,
            Self::VerifiedBlockAccepted(_) => TransitionDomain::VerifiedBlockAccepted,
            Self::BodyEvidence(BodyEvidence::PayloadMismatch(_)) => {
                TransitionDomain::BodyPayloadMismatch
            }
            Self::BodyEvidence(BodyEvidence::ConsensusInvalid(_)) => {
                TransitionDomain::ConsensusBodyInvalid
            }
            Self::BodyEvidence(BodyEvidence::Transient(_)) => {
                TransitionDomain::TransientBodyFailure
            }
            Self::BodyEvidence(BodyEvidence::Verified(_)) => TransitionDomain::VerifiedBody,
            Self::BodySupplierDiscovered(_) => TransitionDomain::BodySupplierDiscovered,
            Self::OperatorBodyRetry(_) => TransitionDomain::OperatorBodyRetry,
            Self::OperatorInvalidate(_) => TransitionDomain::OperatorInvalidate,
            Self::OperatorReconsider(_) => TransitionDomain::OperatorReconsider,
            Self::FullStateFinalized(_) => TransitionDomain::FullStateFinalized,
            Self::MigratedPinRefutation(_) => TransitionDomain::MigratedPinRefutation,
            Self::AuxEvidence(_) => TransitionDomain::AuxEvidence,
            Self::ReevaluateDeferred => return None,
        };
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-transition-payload-v1");
        hasher.update([domain.code()]);
        hash_transition_payload(&mut hasher, self);
        Some(TransitionFingerprint::from_parts(
            domain,
            evidence,
            hasher.finalize().into(),
        ))
    }

    /// Return explicit branch ownership for asynchronous network-originated events.
    pub fn header_sync_owner(&self) -> Option<HeaderSyncWorkOwner> {
        match self {
            Self::InsertHeaders(event) => Some(event.owner),
            _ => None,
        }
    }

    /// Return body authority for asynchronous body-originated evidence.
    pub fn body_owner(&self) -> Option<BodyWorkOwner> {
        match self {
            Self::AuxEvidence(event) => Some(event.owner),
            _ => None,
        }
    }

    /// Return the submitted event's domain, including non-replayed inputs.
    pub fn domain(&self) -> TransitionDomain {
        match self {
            Self::InsertHeaders(_) => TransitionDomain::InsertHeaders,
            Self::VerifiedChainChanged(_) => TransitionDomain::VerifiedChainChanged,
            Self::VerifiedBlockAccepted(_) => TransitionDomain::VerifiedBlockAccepted,
            Self::BodyEvidence(BodyEvidence::PayloadMismatch(_)) => {
                TransitionDomain::BodyPayloadMismatch
            }
            Self::BodyEvidence(BodyEvidence::ConsensusInvalid(_)) => {
                TransitionDomain::ConsensusBodyInvalid
            }
            Self::BodyEvidence(BodyEvidence::Transient(_)) => {
                TransitionDomain::TransientBodyFailure
            }
            Self::BodyEvidence(BodyEvidence::Verified(_)) => TransitionDomain::VerifiedBody,
            Self::BodySupplierDiscovered(_) => TransitionDomain::BodySupplierDiscovered,
            Self::OperatorBodyRetry(_) => TransitionDomain::OperatorBodyRetry,
            Self::OperatorInvalidate(_) => TransitionDomain::OperatorInvalidate,
            Self::OperatorReconsider(_) => TransitionDomain::OperatorReconsider,
            Self::FullStateFinalized(_) => TransitionDomain::FullStateFinalized,
            Self::MigratedPinRefutation(_) => TransitionDomain::MigratedPinRefutation,
            Self::AuxEvidence(_) => TransitionDomain::AuxEvidence,
            Self::ReevaluateDeferred => TransitionDomain::ReevaluateDeferred,
        }
    }
}

/// Version-qualified request assembled by the state write path.
///
/// [`Self::event`] is the authenticated evidence; [`Self::expected_version`] is
/// the caller's observed durable version. The adapter then binds event-specific
/// store facts into [`crate::TransitionInput`] for the engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionRequest {
    /// Exact durable version observed by the caller.
    pub expected_version: StateVersion,
    /// Authenticated evidence of what happened.
    ///
    /// Callers never submit desired consequences.
    pub event: TransitionEvent,
}
