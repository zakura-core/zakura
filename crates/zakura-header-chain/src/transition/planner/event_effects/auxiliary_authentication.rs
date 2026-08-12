//! Auxiliary delivery authentication and rejection.

use crate::graph::HeaderGraphView;
use crate::{Frontier, TransitionFailure};

use super::super::projected_state::ProjectedTransitionState;
use super::ApplyEventContext;

/// Authenticate or reject previously admitted auxiliary deliveries.
pub(super) fn apply(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::AuxEvidence,
    event_context: &ApplyEventContext<'_>,
) -> Result<(), TransitionFailure> {
    let engine = event_context.engine;
    if event.deliveries.is_empty() || event.deliveries.len() > 2 {
        return Err(TransitionFailure::InvalidEvidence(
            "auxiliary evidence must name one or two exact deliveries",
        ));
    }

    for (index, event_delivery) in event.deliveries.iter().enumerate() {
        if event.deliveries[..index].iter().any(|prior| {
            prior.header_hash == event_delivery.header_hash
                && prior.delivery_id == event_delivery.delivery_id
        }) {
            return Err(TransitionFailure::InvalidEvidence(
                "auxiliary evidence names the same delivery more than once",
            ));
        }
        let header = projected
            .graph()
            .view_header_node(event_delivery.header_hash)
            .ok_or(TransitionFailure::InvalidEvidence(
                "auxiliary evidence references an unknown header",
            ))?;
        let header_frontier = Frontier::new(header.height, header.hash);
        if projected
            .graph()
            .view_header_ancestor(event.owner.branch.target_tip_hash, header.height)?
            != Some(header_frontier)
        {
            return Err(TransitionFailure::InvalidEvidence(
                "auxiliary evidence is outside its owned branch",
            ));
        }
        let existing = engine
            .aux_deliveries(event_delivery.header_hash)
            .iter()
            .copied()
            .find(|delivery| delivery.delivery_id == event_delivery.delivery_id)
            .ok_or(TransitionFailure::InvalidEvidence(
                "auxiliary evidence references an unknown delivery",
            ))?;
        if existing != *event_delivery || !header.aux_delivery_ids.contains(&existing.delivery_id) {
            return Err(TransitionFailure::InvalidEvidence(
                "auxiliary evidence changes delivery provenance",
            ));
        }
        if existing.authentication == event.authentication {
            continue;
        }
        if existing.authentication != crate::AuxAuthentication::Unauthenticated {
            return Err(TransitionFailure::InvalidEvidence(
                "an authenticated or rejected auxiliary delivery is immutable",
            ));
        }
        if let crate::AuxAuthentication::Authenticated { boundary_hash, .. } = event.authentication
        {
            let boundary = projected.graph().view_header_node(boundary_hash).ok_or(
                TransitionFailure::InvalidEvidence("auxiliary authentication boundary is unknown"),
            )?;
            let expected_height = header.height.next().map_err(|_| {
                TransitionFailure::InvalidEvidence(
                    "auxiliary authentication boundary height overflowed",
                )
            })?;
            let boundary_frontier = Frontier::new(boundary.height, boundary.hash);
            if boundary.height != expected_height
                || boundary.parent_hash != header.hash
                || projected
                    .graph()
                    .view_header_ancestor(event.owner.branch.target_tip_hash, boundary.height)?
                    != Some(boundary_frontier)
            {
                return Err(TransitionFailure::InvalidEvidence(
                    "auxiliary authentication is not the owned one-header-later boundary",
                ));
            }
        } else if event.authentication == crate::AuxAuthentication::Unauthenticated {
            return Err(TransitionFailure::InvalidEvidence(
                "auxiliary evidence cannot remove authentication",
            ));
        }
        let mut delivery = existing;
        delivery.authentication = event.authentication;
        projected.update_aux_delivery(delivery);
    }
    if event.authentication == crate::AuxAuthentication::Unauthenticated {
        return Err(TransitionFailure::InvalidEvidence(
            "auxiliary evidence cannot remove authentication",
        ));
    }
    Ok(())
}
