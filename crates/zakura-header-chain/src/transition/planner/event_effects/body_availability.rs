//! Transient body availability and supplier-discovery evidence.

use crate::graph::HeaderGraphView;
use crate::{BodyValidationState, TransitionFailure};

use super::super::projected_state::ProjectedTransitionState;
use super::ApplyEventContext;

/// Payload mismatches are informational and must not mutate the header DAG.
pub(super) fn apply_payload_mismatch(
    _event: &crate::BodyPayloadMismatch,
) -> Result<(), TransitionFailure> {
    Ok(())
}

/// Record a transient body-unavailable episode without regressing verified bodies.
pub(super) fn apply_transient(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::TransientBodyFailure,
) -> Result<(), TransitionFailure> {
    if event.availability.attempts == 0
        || event.availability.suppliers == 0
        || event.availability.started_at > event.availability.next_probe_at
    {
        return Err(TransitionFailure::InvalidEvidence(
            "body retry evidence has an invalid episode summary",
        ));
    }
    if matches!(
        projected
            .graph()
            .view_header_node(event.hash)
            .map(|node| &node.body_validation_state),
        Some(BodyValidationState::Verified { .. })
    ) {
        return Err(TransitionFailure::InvalidEvidence(
            "body retry evidence cannot regress an already verified body",
        ));
    }
    projected.set_body_validation_state(
        event.hash,
        BodyValidationState::Unavailable(event.availability),
    )?;
    Ok(())
}

/// Expand the selected persistent body's supplier set for an already-due probe.
pub(super) fn apply_supplier_discovery(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::BodySupplierDiscovered,
    event_context: &ApplyEventContext<'_>,
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
            return Err(TransitionFailure::InvalidEvidence(
                "body supplier discovery requires the selected persistent alarm",
            ));
        }
    };
    if event.availability.started_at != old.started_at
        || event.availability.attempts != old.attempts
        || event.availability.suppliers == 0
        || !event.availability.alarmed
        || event.availability.next_probe_at < event.availability.started_at
        || event.availability.next_probe_at > context.clock.now()
    {
        return Err(TransitionFailure::InvalidEvidence(
            "body supplier discovery must preserve the persistent retry episode",
        ));
    }
    let has_new_supplier = event.availability.suppliers > old.suppliers
        || (event.availability.suppliers == old.suppliers
            && event.availability.supplier_set_digest != old.supplier_set_digest);
    if !has_new_supplier {
        return Err(TransitionFailure::InvalidEvidence(
            "body supplier discovery does not add an eligible supplier",
        ));
    }
    projected.set_body_validation_state(
        event.hash,
        BodyValidationState::Unavailable(event.availability),
    )?;
    Ok(())
}
