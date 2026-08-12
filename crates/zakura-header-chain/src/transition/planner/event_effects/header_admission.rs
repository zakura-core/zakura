//! Prepared header insertion and atomic auxiliary delivery admission.

use std::collections::{HashMap, HashSet};

use crate::graph::HeaderGraphView;
use crate::{
    BodyValidationState, Frontier, GraphError, HeaderValidationState, TargetCompletion,
    TransitionFailure,
};

use super::super::projected_state::ProjectedTransitionState;
use super::header_validation::{anchor_reasons, retained_header_context};
use super::ApplyEventContext;

/// Admit a prepared header batch and any accompanying unauthenticated auxiliary deliveries.
pub(super) fn apply(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::InsertHeaders,
    event_context: &ApplyEventContext<'_>,
) -> Result<(), TransitionFailure> {
    let engine = event_context.engine;
    let durable = event_context.durable;
    let context = event_context.transition;
    let receipt = event.batch.receipt();
    if receipt.parent().hash != event.parent_hash
        || receipt.trust_anchor_digest() != context.config.trust_anchor_digest()
    {
        return Err(TransitionFailure::StalePreparation);
    }
    let parent_node = projected
        .graph()
        .view_header_node(event.parent_hash)
        .ok_or(GraphError::UnknownParent {
            header: event.target_tip_hash,
            parent: event.parent_hash,
        })?;
    let parent_frontier = Frontier::new(parent_node.height, parent_node.hash);
    if receipt.parent() != parent_frontier
        || receipt.network() != &context.config.network
        || receipt.trust_anchor_digest() != context.config.trust_anchor_digest()
    {
        return Err(TransitionFailure::StalePreparation);
    }
    let common_ancestor = match event.completion {
        TargetCompletion::TargetComplete { common_ancestor }
        | TargetCompletion::TargetPrefix { common_ancestor }
        | TargetCompletion::SelectedAuxiliaryRepair {
            common_ancestor, ..
        } => common_ancestor,
    };
    if common_ancestor != parent_frontier {
        return Err(TransitionFailure::InvalidEvidence(
            "target completion ancestor does not match the retained parent",
        ));
    }
    let mut contextual =
        retained_header_context(projected.graph(), parent_frontier, durable, context)?;
    let mut parent = parent_frontier;
    for prepared in event.batch.headers() {
        if prepared.header.previous_block_hash != parent.hash
            || prepared.hash != prepared.header.hash()
            || prepared.height
                != parent
                    .height
                    .next()
                    .map_err(|_| GraphError::HeightOverflow {
                        parent: parent.hash,
                    })?
            || prepared.block_work
                != prepared.header.difficulty_threshold.to_work().ok_or(
                    TransitionFailure::InvalidEvidence("invalid prepared target"),
                )?
        {
            return Err(TransitionFailure::InvalidEvidence(
                "prepared header batch is inconsistent",
            ));
        }
        let adjustment = crate::AdjustedDifficulty::new_from_header_time(
            prepared.header.time,
            parent.height,
            &context.config.network,
            contextual.iter().copied(),
        )
        .map_err(|_| {
            TransitionFailure::InvalidEvidence(
                "prepared header has incomplete retained difficulty context",
            )
        })?;
        crate::validate_contextual_difficulty_and_time(
            prepared.header.difficulty_threshold,
            adjustment,
        )
        .map_err(|_| {
            TransitionFailure::InvalidEvidence(
                "prepared header failed retained contextual validation",
            )
        })?;
        let validation = match prepared.validation {
            HeaderValidationState::DeferredUntil(until) if until <= context.clock.now() => {
                HeaderValidationState::Valid
            }
            state => state,
        };
        let reasons = anchor_reasons(context, prepared.height, prepared.hash);
        parent = match projected.insert_header(
            prepared.header.clone(),
            validation,
            reasons,
            BodyValidationState::Unknown,
        )? {
            crate::InsertResult::Inserted(frontier)
            | crate::InsertResult::AlreadyPresent(frontier) => frontier,
        };
        contextual.insert(
            0,
            (prepared.header.difficulty_threshold, prepared.header.time),
        );
        contextual.truncate(crate::POW_ADJUSTMENT_BLOCK_SPAN);
    }
    if parent.hash != event.target_tip_hash {
        return Err(TransitionFailure::InvalidEvidence(
            "target completion does not end at the pursued hash",
        ));
    }
    match event.completion {
        TargetCompletion::SelectedAuxiliaryRepair {
            selected_target, ..
        } => {
            if event.owner.body_owner().is_none() {
                return Err(TransitionFailure::InvalidEvidence(
                    "selected auxiliary repair does not have body authority",
                ));
            }
            if event.batch.headers().len() != 1
                || event.aux.len() != 1
                || event.aux[0].tree_aux.is_none()
                || selected_target != parent
                || event.owner.header_authority().branch.target_tip_hash
                    != event_context.before.frontiers.header_best.hash
                || event_context
                    .old_selected
                    .iter()
                    .find(|frontier| frontier.height == selected_target.height)
                    .map(|frontier| frontier.hash)
                    != Some(selected_target.hash)
                || projected.graph().view_header_ancestor(
                    event.owner.header_authority().branch.target_tip_hash,
                    selected_target.height,
                )? != Some(selected_target)
            {
                return Err(TransitionFailure::InvalidEvidence(
                    "auxiliary repair is not one exact selected header",
                ));
            }
        }
        TargetCompletion::TargetComplete { .. } | TargetCompletion::TargetPrefix { .. } => {
            if event.owner.header_owner().is_none() {
                return Err(TransitionFailure::InvalidEvidence(
                    "ordinary header insertion does not have pure header authority",
                ));
            }
            if event.owner.header_authority().branch.target_tip_hash != event.target_tip_hash {
                return Err(TransitionFailure::InvalidEvidence(
                    "target completion does not end at the pursued hash",
                ));
            }
        }
    }
    let batch_headers: HashMap<_, _> = event
        .batch
        .headers()
        .iter()
        .map(|header| (header.hash, header.height))
        .collect();
    let mut delivery_ids = HashSet::new();
    for delivery in &event.aux {
        let expected_height = batch_headers.get(&delivery.header_hash).copied();
        if !delivery_ids.insert(delivery.delivery_id)
            || delivery.owner != event.owner
            || delivery.source != event.source
            || delivery.authentication != crate::AuxAuthentication::Unauthenticated
            || expected_height.is_none()
            || delivery
                .tree_aux
                .is_some_and(|aux| Some(aux.height) != expected_height)
        {
            return Err(TransitionFailure::InvalidEvidence(
                "auxiliary delivery does not match the admitted target",
            ));
        }
        let indexed_count = projected
            .graph()
            .view_header_node(delivery.header_hash)
            .expect("the auxiliary header was checked above")
            .aux_delivery_ids
            .iter()
            .filter(|delivery_id| **delivery_id == delivery.delivery_id)
            .count();
        let stored = engine.aux_delivery(delivery.delivery_id).copied();
        match (stored, indexed_count) {
            (Some(stored), 1) if stored == *delivery => continue,
            (None, 0) => {}
            _ => {
                return Err(TransitionFailure::InvalidEvidence(
                    "auxiliary delivery replay changes provenance or indexing",
                ));
            }
        }
        projected.record_aux_delivery(*delivery)?;
    }
    Ok(())
}
