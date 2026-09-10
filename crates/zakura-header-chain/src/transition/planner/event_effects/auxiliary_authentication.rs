//! Auxiliary observation validation and outcome derivation.

use super::super::{AuxiliaryViolation, InvalidTransitionEvidence, TransitionFailure};
use crate::graph::HeaderGraphView;
use crate::{AuxOutcomeStatus, AuxVerificationKindV1, Frontier};

use super::super::projected_state::ProjectedTransitionState;
use super::EventProjectionContext;

struct DerivedAuxiliaryOutcome {
    status: AuxOutcomeStatus,
    boundary_hash: zakura_chain::block::Hash,
}

/// Derive outcomes from one checked observation.
pub(super) fn authenticate_auxiliary_deliveries(
    projected: &mut ProjectedTransitionState<'_>,
    auxiliary_evidence: &crate::AuxEvidence,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    let Some(observation) = auxiliary_evidence.observation() else {
        return Ok(());
    };
    let deliveries = observation.deliveries();
    let observation_id = observation.observation_id();
    let engine = event_context.engine;

    let mut retained_deliveries = Vec::with_capacity(deliveries.len());
    for event_delivery in deliveries {
        let header_node = projected
            .graph()
            .view_header_node(event_delivery.header_hash)
            .ok_or(TransitionFailure::InvalidEvidence(
                InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::UnknownHeader),
            ))?;
        let header_frontier = Frontier::new(header_node.height, header_node.hash);
        if projected.graph().view_header_ancestor(
            observation.owner().authority.header.branch.target_tip_hash,
            header_node.height,
        )? != Some(header_frontier)
        {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::OutsideOwnedBranch,
            )
            .into());
        }
        let retained_delivery = engine
            .aux_delivery(event_delivery.delivery_id)
            .copied()
            .ok_or(TransitionFailure::InvalidEvidence(
                InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::UnknownDelivery),
            ))?;
        if !has_same_delivery_provenance(retained_delivery, *event_delivery)
            || retained_delivery.header_hash != header_node.hash
            || !header_node
                .aux_delivery_ids
                .contains(&retained_delivery.delivery_id)
        {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::ProvenanceMismatch,
            )
            .into());
        }
        retained_deliveries.push(retained_delivery);
    }

    if retained_deliveries
        .iter()
        .all(|delivery| delivery.outcome().contains_observation(observation_id))
    {
        return Ok(());
    }

    if observation.boundary_witness().is_none() {
        return Ok(());
    }
    let Some(derived_outcome) = derive_auxiliary_outcome(projected, observation)? else {
        return Ok(());
    };

    for retained_delivery in retained_deliveries {
        if retained_delivery
            .outcome()
            .contains_observation(observation_id)
        {
            continue;
        }
        if !retained_delivery
            .outcome()
            .can_refine_to(derived_outcome.status)
        {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::NonRefiningAuthentication,
            )
            .into());
        }
        let refined_outcome = retained_delivery.outcome().refined_by_observation(
            derived_outcome.status,
            observation_id,
            derived_outcome.boundary_hash,
        );
        projected.update_aux_delivery(retained_delivery.with_outcome(refined_outcome));
    }
    Ok(())
}

/// Derive the outcome classification and exact owned-branch boundary.
fn derive_auxiliary_outcome(
    projected: &ProjectedTransitionState<'_>,
    observation: &crate::AuxObservationV1,
) -> Result<Option<DerivedAuxiliaryOutcome>, TransitionFailure> {
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
    Ok(boundary.map(|boundary_hash| DerivedAuxiliaryOutcome {
        status,
        boundary_hash,
    }))
}

/// Return the delivery's direct successor on the observation's owned branch.
fn direct_owned_successor(
    projected: &ProjectedTransitionState<'_>,
    delivery_header_hash: zakura_chain::block::Hash,
    owned_tip: zakura_chain::block::Hash,
) -> Result<Option<zakura_chain::block::Hash>, TransitionFailure> {
    let delivery = projected
        .graph()
        .view_header_node(delivery_header_hash)
        .ok_or(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::UnknownHeader),
        ))?;
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
                .is_some_and(|header_node| header_node.parent_hash == delivery_header_hash)
        })
        .map(|frontier| frontier.hash))
}

fn has_same_delivery_provenance(left: crate::AuxDelivery, right: crate::AuxDelivery) -> bool {
    left.delivery_id == right.delivery_id
        && left.header_hash == right.header_hash
        && left.source == right.source
        && left.owner == right.owner
        && left.body_size == right.body_size
        && left.tree_aux == right.tree_aux
}
