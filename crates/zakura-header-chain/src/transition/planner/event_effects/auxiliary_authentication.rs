//! Auxiliary delivery authentication and rejection.

use super::super::{AuxiliaryViolation, InvalidTransitionEvidence, TransitionFailure};
use crate::graph::HeaderGraphView;
use crate::Frontier;

use super::super::projected_state::ProjectedTransitionState;
use super::EventProjectionContext;

/// Authenticate or reject previously admitted auxiliary deliveries.
pub(super) fn authenticate_auxiliary_deliveries(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::AuxEvidence,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    let engine = event_context.engine;
    if event.deliveries.is_empty() || event.deliveries.len() > 2 {
        return Err(InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::DeliveryCount).into());
    }

    for (index, event_delivery) in event.deliveries.iter().enumerate() {
        if event.deliveries[..index].iter().any(|prior| {
            prior.header_hash == event_delivery.header_hash
                && prior.delivery_id == event_delivery.delivery_id
        }) {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::DuplicateDelivery,
            )
            .into());
        }
        let header = projected
            .graph()
            .view_header_node(event_delivery.header_hash)
            .ok_or(TransitionFailure::InvalidEvidence(
                InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::UnknownHeader),
            ))?;
        let header_frontier = Frontier::new(header.height, header.hash);
        if projected
            .graph()
            .view_header_ancestor(event.owner.branch.target_tip_hash, header.height)?
            != Some(header_frontier)
        {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::OutsideOwnedBranch,
            )
            .into());
        }
        let existing = engine
            .aux_deliveries(event_delivery.header_hash)
            .iter()
            .copied()
            .find(|delivery| delivery.delivery_id == event_delivery.delivery_id)
            .ok_or(TransitionFailure::InvalidEvidence(
                InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::UnknownDelivery),
            ))?;
        if existing != *event_delivery || !header.aux_delivery_ids.contains(&existing.delivery_id) {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::ProvenanceMismatch,
            )
            .into());
        }
        if existing.authentication == event.authentication {
            continue;
        }
        if !existing.authentication.can_refine_to(event.authentication) {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::NonRefiningAuthentication,
            )
            .into());
        }
        if let crate::AuxAuthentication::Authenticated { boundary_hash, .. } = event.authentication
        {
            let boundary = projected.graph().view_header_node(boundary_hash).ok_or(
                TransitionFailure::InvalidEvidence(InvalidTransitionEvidence::Auxiliary(
                    AuxiliaryViolation::UnknownBoundary,
                )),
            )?;
            let expected_height = header.height.next().map_err(|_| {
                InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::BoundaryHeightOverflow)
            })?;
            let boundary_frontier = Frontier::new(boundary.height, boundary.hash);
            if boundary.height != expected_height
                || boundary.parent_hash != header.hash
                || projected
                    .graph()
                    .view_header_ancestor(event.owner.branch.target_tip_hash, boundary.height)?
                    != Some(boundary_frontier)
            {
                return Err(InvalidTransitionEvidence::Auxiliary(
                    AuxiliaryViolation::InvalidBoundary,
                )
                .into());
            }
        } else if event.authentication == crate::AuxAuthentication::Unauthenticated {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::AuthenticationRemoval,
            )
            .into());
        }
        let mut delivery = existing;
        delivery.authentication = event.authentication;
        projected.update_aux_delivery(delivery);
    }
    if event.authentication == crate::AuxAuthentication::Unauthenticated {
        return Err(InvalidTransitionEvidence::Auxiliary(
            AuxiliaryViolation::AuthenticationRemoval,
        )
        .into());
    }
    Ok(())
}
