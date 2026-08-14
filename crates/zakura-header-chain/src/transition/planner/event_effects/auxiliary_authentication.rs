//! Auxiliary observation validation and outcome derivation.

use super::super::{AuxiliaryViolation, InvalidTransitionEvidence, TransitionFailure};
use crate::graph::HeaderGraphView;
use crate::{AuxOutcome, AuxOutcomeStatus, AuxVerificationKindV1, Frontier};

use super::super::projected_state::ProjectedTransitionState;
use super::EventProjectionContext;

/// Derive outcomes from one checked observation.
pub(super) fn authenticate_auxiliary_deliveries(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::AuxEvidence,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    let Some(observation) = event.observation() else {
        return Ok(());
    };
    let deliveries = observation.deliveries();
    let observation_id = observation.observation_id();
    let engine = event_context.engine;

    let mut stored = Vec::with_capacity(deliveries.len());
    for event_delivery in deliveries {
        let header = projected
            .graph()
            .view_header_node(event_delivery.header_hash)
            .ok_or(TransitionFailure::InvalidEvidence(
                InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::UnknownHeader),
            ))?;
        let header_frontier = Frontier::new(header.height, header.hash);
        if projected.graph().view_header_ancestor(
            observation.owner().authority.header.branch.target_tip_hash,
            header.height,
        )? != Some(header_frontier)
        {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::OutsideOwnedBranch,
            )
            .into());
        }
        let existing = engine
            .aux_delivery(event_delivery.delivery_id)
            .copied()
            .ok_or(TransitionFailure::InvalidEvidence(
                InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::UnknownDelivery),
            ))?;
        if !same_provenance(existing, *event_delivery)
            || existing.header_hash != header.hash
            || !header.aux_delivery_ids.contains(&existing.delivery_id)
        {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::ProvenanceMismatch,
            )
            .into());
        }
        stored.push(existing);
    }

    if stored
        .iter()
        .all(|delivery| delivery.outcome().contains_observation(observation_id))
    {
        return Ok(());
    }

    let Some(_witness) = observation.boundary_witness() else {
        return Ok(());
    };
    let Some((status, boundary_hash)) = derive_outcome_boundary(projected, observation)? else {
        return Ok(());
    };

    for existing in stored {
        if existing.outcome().contains_observation(observation_id) {
            continue;
        }
        if !existing.outcome().can_refine_to(status) {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::ImmutableAuthentication,
            )
            .into());
        }
        let outcome =
            AuxOutcome::derived(existing.outcome(), status, observation_id, boundary_hash);
        projected.update_aux_delivery(existing.with_outcome(outcome));
    }
    Ok(())
}

fn derive_outcome_boundary(
    projected: &ProjectedTransitionState<'_>,
    observation: &crate::AuxObservationV1,
) -> Result<Option<(AuxOutcomeStatus, zakura_chain::block::Hash)>, TransitionFailure> {
    let deliveries = observation.deliveries();
    let owned_tip = observation.owner().authority.header.branch.target_tip_hash;
    let (status, boundary) = match observation.verification().kind() {
        AuxVerificationKindV1::CurrentVerified => (
            AuxOutcomeStatus::Authenticated,
            direct_owned_successor(projected, deliveries[0].header_hash, owned_tip)?,
        ),
        AuxVerificationKindV1::CurrentFailed => (
            AuxOutcomeStatus::Rejected,
            direct_owned_successor(projected, deliveries[0].header_hash, owned_tip)?,
        ),
        AuxVerificationKindV1::SuccessorFailed => {
            (AuxOutcomeStatus::Rejected, Some(deliveries[0].header_hash))
        }
        AuxVerificationKindV1::AmbiguousFailed => {
            let successor =
                direct_owned_successor(projected, deliveries[0].header_hash, owned_tip)?;
            if successor != Some(deliveries[1].header_hash) {
                return Err(InvalidTransitionEvidence::Auxiliary(
                    AuxiliaryViolation::InvalidBoundary,
                )
                .into());
            }
            (AuxOutcomeStatus::Disputed, successor)
        }
    };
    Ok(boundary.map(|boundary| (status, boundary)))
}

fn direct_owned_successor(
    projected: &ProjectedTransitionState<'_>,
    delivery_hash: zakura_chain::block::Hash,
    owned_tip: zakura_chain::block::Hash,
) -> Result<Option<zakura_chain::block::Hash>, TransitionFailure> {
    let delivery = projected.graph().view_header_node(delivery_hash).ok_or(
        TransitionFailure::InvalidEvidence(InvalidTransitionEvidence::Auxiliary(
            AuxiliaryViolation::UnknownHeader,
        )),
    )?;
    let Some(height) = delivery.height.next().ok() else {
        return Ok(None);
    };
    Ok(projected
        .graph()
        .view_header_ancestor(owned_tip, height)?
        .filter(|frontier| {
            projected
                .graph()
                .view_header_node(frontier.hash)
                .is_some_and(|node| node.parent_hash == delivery_hash)
        })
        .map(|frontier| frontier.hash))
}

fn same_provenance(left: crate::AuxDelivery, right: crate::AuxDelivery) -> bool {
    left.delivery_id == right.delivery_id
        && left.header_hash == right.header_hash
        && left.source == right.source
        && left.owner == right.owner
        && left.body_size == right.body_size
        && left.tree_aux == right.tree_aux
}
