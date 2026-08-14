//! Authoritative full-state conclusions and finality evidence.

use crate::graph::HeaderGraphView;
use crate::{BodyValidationState, Frontier, GraphError, HeaderValidationState};

use super::super::projected_state::ProjectedTransitionState;
use super::super::{
    BodyViolation, FinalityViolation, HeaderPathKind, HeaderPathProblem, InvalidTransitionEvidence,
    TransitionFailure,
};
use super::header_validation::{anchor_reasons, validate_full_state_header};
use super::EventProjectionContext;

/// Project a verified winning-path change.
pub(super) fn project_verified_chain_change(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::VerifiedChainChanged,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    let facts = event_context.input.header_validation_facts();
    let context = event_context.transition;
    if projected.verified().last().copied() != Some(event.old_tip) {
        return Err(TransitionFailure::StalePreparation);
    }
    let mut parent = match event.cause {
        crate::VerifiedChangeCause::Grow | crate::VerifiedChangeCause::CheckpointFinalizedGrow => {
            event.old_tip
        }
        crate::VerifiedChangeCause::Reset => projected.graph().view_finalized_frontier(),
    };
    if matches!(event.cause, crate::VerifiedChangeCause::Reset) {
        projected.reset_verified(parent);
    }
    for header in &event.new_path {
        if header.header.hash() != header.hash
            || header.header.previous_block_hash != parent.hash
            || header.height
                != parent
                    .height
                    .next()
                    .map_err(|_| GraphError::HeightOverflow {
                        parent: parent.hash,
                    })?
        {
            return Err(
                InvalidTransitionEvidence::Header(crate::HeaderViolation::Path {
                    kind: HeaderPathKind::Verified,
                    problem: HeaderPathProblem::Discontinuous,
                })
                .into(),
            );
        }
        if projected.graph().view_header_node(header.hash).is_none() {
            validate_full_state_header(projected.graph(), parent, header, facts, context)?;
            projected.insert_header(
                header.header.clone(),
                HeaderValidationState::Valid,
                anchor_reasons(context, header.height, header.hash),
                BodyValidationState::Verified {
                    evidence: event.full_state_transition_id,
                },
            )?;
        } else {
            projected.set_body_validation_state(
                header.hash,
                BodyValidationState::Verified {
                    evidence: event.full_state_transition_id,
                },
            )?;
        }
        if projected
            .graph()
            .view_header_node(header.hash)
            .is_none_or(|node| !node.is_eligible())
        {
            return Err(
                InvalidTransitionEvidence::Header(crate::HeaderViolation::Path {
                    kind: HeaderPathKind::Verified,
                    problem: HeaderPathProblem::Ineligible,
                })
                .into(),
            );
        }
        parent = Frontier::new(header.height, header.hash);
        projected.push_verified(parent);
    }
    Ok(())
}

/// Accept a finalized-rooted side path without replacing the verified winner.
pub(super) fn project_verified_block_acceptance(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::VerifiedBlockAccepted,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    let facts = event_context.input.header_validation_facts();
    let context = event_context.transition;
    if event.path.is_empty() {
        return Err(
            InvalidTransitionEvidence::Header(crate::HeaderViolation::Path {
                kind: HeaderPathKind::AcceptedSide,
                problem: HeaderPathProblem::Empty,
            })
            .into(),
        );
    }
    let mut parent = projected.graph().view_finalized_frontier();
    let last_index = event.path.len().saturating_sub(1);
    for (index, header) in event.path.iter().enumerate() {
        if header.header.hash() != header.hash
            || header.header.previous_block_hash != parent.hash
            || header.height
                != parent
                    .height
                    .next()
                    .map_err(|_| GraphError::HeightOverflow {
                        parent: parent.hash,
                    })?
        {
            return Err(
                InvalidTransitionEvidence::Header(crate::HeaderViolation::Path {
                    kind: HeaderPathKind::AcceptedSide,
                    problem: HeaderPathProblem::Discontinuous,
                })
                .into(),
            );
        }
        if projected.graph().view_header_node(header.hash).is_none() {
            validate_full_state_header(projected.graph(), parent, header, facts, context)?;
            projected.insert_header(
                header.header.clone(),
                HeaderValidationState::Valid,
                anchor_reasons(context, header.height, header.hash),
                BodyValidationState::Verified {
                    evidence: event.full_state_transition_id,
                },
            )?;
        } else if index == last_index {
            projected.set_body_validation_state(
                header.hash,
                BodyValidationState::Verified {
                    evidence: event.full_state_transition_id,
                },
            )?;
        }
        parent = Frontier::new(header.height, header.hash);
    }
    Ok(())
}

/// Project permanent consensus-invalid body evidence.
pub(super) fn project_consensus_invalid_body(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::ConsensusBodyInvalid,
) -> Result<(), TransitionFailure> {
    if matches!(
        projected
            .graph()
            .view_header_node(event.hash)
            .map(|node| &node.body_validation_state),
        Some(BodyValidationState::Verified { .. })
    ) {
        return Err(InvalidTransitionEvidence::Body(
            BodyViolation::InvalidityConflictsWithVerified,
        )
        .into());
    }
    projected.set_body_validation_state(
        event.hash,
        BodyValidationState::ConsensusInvalid {
            evidence: event.evidence,
            rule: event.rule.clone(),
        },
    )?;
    Ok(())
}

/// Project exact full-state body acceptance.
pub(super) fn project_verified_body_evidence(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::VerifiedBodyEvidence,
) -> Result<(), TransitionFailure> {
    projected.set_body_validation_state(
        event.hash,
        BodyValidationState::Verified {
            evidence: event.evidence,
        },
    )?;
    Ok(())
}

/// Validate that a full-state finality proof matches the exact verified prefix.
pub(super) fn project_full_state_finality(
    projected: &ProjectedTransitionState<'_>,
    event: &crate::FullStateFinalized,
) -> Result<(), TransitionFailure> {
    let expected: Vec<_> = projected
        .verified()
        .iter()
        .take_while(|frontier| frontier.height <= event.new_finalized.height)
        .map(|frontier| frontier.hash)
        .collect();
    if event.verified_path_proof != expected {
        return Err(InvalidTransitionEvidence::Finality(FinalityViolation::ProofMismatch).into());
    }
    Ok(())
}

/// Validate migrated-pin refutation evidence against durable authentication.
pub(super) fn project_migrated_pin_refutation(
    event: &crate::MigratedPinRefutation,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    if event.invalid_header.height > event.pin.height
        || event_context.migrated_pin_refuted != Some(event.pin)
    {
        return Err(InvalidTransitionEvidence::Finality(
            FinalityViolation::ImportedPinRefutationMismatch,
        )
        .into());
    }
    Ok(())
}
