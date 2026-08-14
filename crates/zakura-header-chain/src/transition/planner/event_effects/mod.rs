//! Read-only context and exhaustive dispatch for event evidence projection.

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
pub(in crate::transition::planner) struct EventProjectionContext<'a> {
    pub(super) engine: &'a HeaderChainEngine,
    pub(super) input: &'a TransitionInput,
    pub(super) transition: &'a TransitionContext<'a>,
    pub(super) snapshot_before_commit: &'a EngineSnapshot,
    pub(super) old_selected: &'a [Frontier],
    pub(super) migrated_pin_refuted: Option<Frontier>,
}

/// Project typed event evidence into the projected transition state.
pub(super) fn project_event_evidence(
    projected: &mut ProjectedTransitionState<'_>,
    event: &TransitionEvent,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    match event {
        TransitionEvent::InsertHeaders(event) => {
            header_admission::admit_prepared_headers(projected, event, event_context)
        }
        TransitionEvent::VerifiedChainChanged(event) => {
            full_state_evidence::project_verified_chain_change(projected, event, event_context)
        }
        TransitionEvent::VerifiedBlockAccepted(event) => {
            full_state_evidence::project_verified_block_acceptance(projected, event, event_context)
        }
        TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(event)) => {
            body_availability::admit_payload_mismatch(event)
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Transient(event)) => {
            body_availability::project_transient_body_failure(projected, event)
        }
        TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => {
            full_state_evidence::project_consensus_invalid_body(projected, event)
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Verified(event)) => {
            full_state_evidence::project_verified_body_evidence(projected, event)
        }
        TransitionEvent::BodySupplierDiscovered(event) => {
            body_availability::project_body_supplier_discovery(projected, event, event_context)
        }
        TransitionEvent::OperatorBodyRetry(event) => {
            operator_policy::project_operator_body_retry(projected, event)
        }
        TransitionEvent::OperatorInvalidate(event) => {
            operator_policy::project_operator_invalidation(projected, event)
        }
        TransitionEvent::OperatorReconsider(event) => {
            operator_policy::project_operator_reconsideration(projected, event)
        }
        TransitionEvent::FullStateFinalized(event) => {
            full_state_evidence::project_full_state_finality(projected, event)
        }
        TransitionEvent::MigratedPinRefutation(event) => {
            full_state_evidence::project_migrated_pin_refutation(event, event_context)
        }
        TransitionEvent::AuxEvidence(event) => {
            auxiliary_authentication::authenticate_auxiliary_deliveries(
                projected,
                event,
                event_context,
            )
        }
        TransitionEvent::ReevaluateDeferred => {
            deferred_time::reevaluate_elapsed_deferrals(projected, event_context)
        }
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
        return Err(TransitionFailure::MissingDurableFacts(
            "migrated finality fact was not supplied",
        ));
    };
    Ok((preserved == Some(event.pin)).then_some(event.pin))
}
