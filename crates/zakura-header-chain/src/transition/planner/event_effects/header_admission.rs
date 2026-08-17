//! Prepared header insertion and atomic auxiliary delivery admission.

use super::super::{
    AuxiliaryViolation, HeaderPathKind, HeaderPathProblem, HeaderValidationCheck, HeaderViolation,
    InvalidTransitionEvidence, TransitionFailure,
};
use chrono::Duration;
use std::collections::{HashMap, HashSet};

use crate::graph::HeaderGraphView;
use crate::{BodyValidationState, Frontier, GraphError, HeaderValidationState, TargetCompletion};

use super::super::projected_state::ProjectedTransitionState;
use super::header_validation::{
    anchor_reasons, check_contextual_difficulty_and_time, retained_header_context,
};
use super::EventProjectionContext;

/// Admit a prepared header batch and any accompanying unauthenticated auxiliary deliveries.
pub(super) fn admit_prepared_headers(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::InsertHeaders,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    let engine = event_context.engine;
    let facts = event_context.input.header_validation_facts();
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
        || receipt.network() != context.config.network()
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
        return Err(
            InvalidTransitionEvidence::Header(crate::HeaderViolation::Path {
                kind: HeaderPathKind::Completion,
                problem: HeaderPathProblem::AncestorMismatch,
            })
            .into(),
        );
    }
    let mut contextual =
        retained_header_context(projected.graph(), parent_frontier, facts, context)?;
    let mut parent = parent_frontier;
    let admission_now = context.clock.now();
    let future_limit = admission_now.checked_add_signed(Duration::hours(2)).ok_or(
        InvalidTransitionEvidence::Header(HeaderViolation::Validation {
            source: crate::HeaderValidationSource::Prepared,
            check: HeaderValidationCheck::IdentityOrLocalTime,
        }),
    )?;
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
                    TransitionFailure::InvalidEvidence(InvalidTransitionEvidence::header_path(
                        HeaderPathKind::Completion,
                        HeaderPathProblem::InvalidPreparedTarget,
                    )),
                )?
        {
            return Err(InvalidTransitionEvidence::header_path(
                HeaderPathKind::Completion,
                HeaderPathProblem::BatchInconsistent,
            )
            .into());
        }
        check_contextual_difficulty_and_time(
            prepared.header.time,
            prepared.header.difficulty_threshold,
            parent.height,
            context.config.network(),
            contextual.iter().copied(),
            crate::HeaderValidationSource::Prepared,
        )?;
        let validation = match prepared.validation {
            HeaderValidationState::Valid if prepared.header.time > future_limit => {
                HeaderValidationState::DeferredUntil(
                    prepared
                        .header
                        .time
                        .checked_sub_signed(Duration::hours(2))
                        .ok_or(InvalidTransitionEvidence::Header(
                            HeaderViolation::Validation {
                                source: crate::HeaderValidationSource::Prepared,
                                check: HeaderValidationCheck::IdentityOrLocalTime,
                            },
                        ))?,
                )
            }
            HeaderValidationState::DeferredUntil(until) if until <= admission_now => {
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
        return Err(
            InvalidTransitionEvidence::Header(crate::HeaderViolation::Path {
                kind: HeaderPathKind::Completion,
                problem: HeaderPathProblem::TipMismatch,
            })
            .into(),
        );
    }
    match event.completion {
        TargetCompletion::SelectedAuxiliaryRepair {
            selected_target, ..
        } => {
            if event.owner.body_owner().is_none() {
                return Err(InvalidTransitionEvidence::Header(
                    HeaderViolation::RepairOwnerRoleMismatch,
                )
                .into());
            }
            if event.batch.headers().len() != 1
                || event.aux.len() != 1
                || event.aux[0].tree_aux.is_none()
                || selected_target != parent
                || event.owner.header_authority().branch.target_tip_hash
                    != event_context
                        .snapshot_before_commit
                        .frontiers
                        .header_best
                        .hash
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
                return Err(InvalidTransitionEvidence::Header(
                    HeaderViolation::AuxiliaryRepairShape,
                )
                .into());
            }
        }
        TargetCompletion::TargetComplete { .. } | TargetCompletion::TargetPrefix { .. } => {
            if event.owner.header_owner().is_none() {
                return Err(InvalidTransitionEvidence::Header(
                    HeaderViolation::OrdinaryOwnerRoleMismatch,
                )
                .into());
            }
            if event.owner.header_authority().branch.target_tip_hash != event.target_tip_hash {
                return Err(
                    InvalidTransitionEvidence::Header(crate::HeaderViolation::Path {
                        kind: HeaderPathKind::Completion,
                        problem: HeaderPathProblem::TipMismatch,
                    })
                    .into(),
                );
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
            || delivery.outcome().status() != crate::AuxOutcomeStatus::Unauthenticated
            || expected_height.is_none()
            || delivery
                .tree_aux
                .is_some_and(|aux| Some(aux.height) != expected_height)
        {
            return Err(InvalidTransitionEvidence::Auxiliary(
                AuxiliaryViolation::AdmittedTargetMismatch,
            )
            .into());
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
                return Err(InvalidTransitionEvidence::Auxiliary(
                    AuxiliaryViolation::ReplayConflict,
                )
                .into());
            }
        }
        projected.record_aux_delivery(*delivery)?;
    }
    Ok(())
}
