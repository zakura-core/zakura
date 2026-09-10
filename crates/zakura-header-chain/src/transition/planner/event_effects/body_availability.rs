//! Transient body availability and supplier-discovery evidence.

use super::super::{BodyViolation, InvalidTransitionEvidence, TransitionFailure};
use crate::graph::HeaderGraphView;
use crate::BodyValidationState;

use super::super::projected_state::ProjectedTransitionState;
use super::EventProjectionContext;

/// Payload mismatches are informational and must not mutate the header DAG.
pub(super) fn admit_payload_mismatch(
    _event: &crate::BodyPayloadMismatch,
) -> Result<(), TransitionFailure> {
    Ok(())
}

/// Record a transient body-unavailable episode without regressing verified bodies.
pub(super) fn project_transient_body_failure(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::TransientBodyFailure,
) -> Result<(), TransitionFailure> {
    if event.availability.attempts == 0
        || event.availability.suppliers == 0
        || event.availability.started_at > event.availability.next_probe_at
    {
        return Err(InvalidTransitionEvidence::Body(BodyViolation::InvalidTransientEpisode).into());
    }
    if matches!(
        projected
            .graph()
            .view_header_node(event.hash)
            .map(|node| &node.body_validation_state),
        Some(BodyValidationState::Verified { .. })
    ) {
        return Err(
            InvalidTransitionEvidence::Body(BodyViolation::RetryConflictsWithVerified).into(),
        );
    }
    projected.set_body_validation_state(
        event.hash,
        BodyValidationState::Unavailable(event.availability),
    )?;
    Ok(())
}

/// Expand the selected persistent body's supplier set for an already-due probe.
pub(super) fn project_body_supplier_discovery(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::BodySupplierDiscovered,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    let context = event_context.transition;
    let old = match projected
        .graph()
        .view_header_node(event.hash)
        .map(|node| &node.body_validation_state)
    {
        Some(BodyValidationState::Unavailable(summary))
            if event.hash == projected.graph().view_select_best_header_chain()?.0.hash
                && summary.alarmed =>
        {
            *summary
        }
        _ => {
            return Err(InvalidTransitionEvidence::Body(
                BodyViolation::SupplierRequiresPersistentAlarm,
            )
            .into());
        }
    };
    if event.availability.started_at != old.started_at
        || event.availability.attempts != old.attempts
        || event.availability.suppliers == 0
        || !event.availability.alarmed
        || event.availability.next_probe_at < event.availability.started_at
        || event.availability.next_probe_at > context.clock.now()
    {
        return Err(InvalidTransitionEvidence::Body(BodyViolation::SupplierEpisodeChanged).into());
    }
    let has_new_supplier = event.availability.suppliers > old.suppliers
        || (event.availability.suppliers == old.suppliers
            && event.availability.supplier_set_digest != old.supplier_set_digest);
    if !has_new_supplier {
        return Err(InvalidTransitionEvidence::Body(BodyViolation::NoNewSupplier).into());
    }
    projected.set_body_validation_state(
        event.hash,
        BodyValidationState::Unavailable(event.availability),
    )?;
    Ok(())
}
