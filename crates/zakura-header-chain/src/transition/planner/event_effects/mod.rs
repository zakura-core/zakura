//! Read-only context and exhaustive dispatch for event evidence application.

mod auxiliary_authentication;
mod body_availability;
mod deferred_time;
mod full_state_evidence;
mod header_admission;
pub(super) mod header_validation;
mod operator_policy;

use crate::{
    BodyEvidence, EngineSnapshot, Frontier, HeaderChainEngine, TransitionContext, TransitionEvent,
    TransitionFailure, TransitionInput,
};

use super::projected_state::ProjectedTransitionState;

/// Read-only inputs shared by every domain event handler.
pub(in crate::transition::planner) struct ApplyEventContext<'a> {
    pub(super) engine: &'a HeaderChainEngine,
    pub(super) input: &'a TransitionInput,
    pub(super) transition: &'a TransitionContext<'a>,
    pub(super) before: &'a EngineSnapshot,
    pub(super) old_selected: &'a [Frontier],
    pub(super) migrated_pin_refuted: Option<Frontier>,
}

/// Apply typed event evidence into the projected transition state.
pub(super) fn apply_event_evidence(
    projected: &mut ProjectedTransitionState<'_>,
    event: &TransitionEvent,
    event_context: &ApplyEventContext<'_>,
) -> Result<(), TransitionFailure> {
    match event {
        TransitionEvent::InsertHeaders(event) => {
            header_admission::apply(projected, event, event_context)
        }
        TransitionEvent::VerifiedChainChanged(event) => {
            full_state_evidence::apply_chain_change(projected, event, event_context)
        }
        TransitionEvent::VerifiedBlockAccepted(event) => {
            full_state_evidence::apply_block_acceptance(projected, event, event_context)
        }
        TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(event)) => {
            body_availability::apply_payload_mismatch(event)
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Transient(event)) => {
            body_availability::apply_transient(projected, event)
        }
        TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => {
            full_state_evidence::apply_consensus_invalid(projected, event)
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Verified(event)) => {
            full_state_evidence::apply_verified_body(projected, event)
        }
        TransitionEvent::BodySupplierDiscovered(event) => {
            body_availability::apply_supplier_discovery(projected, event, event_context)
        }
        TransitionEvent::OperatorBodyRetry(event) => {
            operator_policy::apply_body_retry(projected, event)
        }
        TransitionEvent::OperatorInvalidate(event) => {
            operator_policy::apply_invalidate(projected, event)
        }
        TransitionEvent::OperatorReconsider(event) => {
            operator_policy::apply_reconsider(projected, event)
        }
        TransitionEvent::FullStateFinalized(event) => {
            full_state_evidence::apply_finality_proof(projected, event)
        }
        TransitionEvent::MigratedPinRefutation(event) => {
            full_state_evidence::apply_migrated_pin_refutation(event, event_context)
        }
        TransitionEvent::AuxEvidence(event) => {
            auxiliary_authentication::apply(projected, event, event_context)
        }
        TransitionEvent::ReevaluateDeferred => deferred_time::apply(projected, event_context),
    }
}

/// Resolve whether durable facts authenticate a migrated-pin refutation.
pub(super) fn migrated_pin_refuted(
    input: &TransitionInput,
    event: &TransitionEvent,
) -> Result<Option<Frontier>, TransitionFailure> {
    let TransitionEvent::MigratedPinRefutation(event) = event else {
        return Ok(None);
    };
    let Some(preserved) = input.preserved_migrated_pin() else {
        return Err(
            crate::StoreError::Unavailable("migrated finality fact was not supplied").into(),
        );
    };
    Ok((preserved == Some(event.pin)).then_some(event.pin))
}
