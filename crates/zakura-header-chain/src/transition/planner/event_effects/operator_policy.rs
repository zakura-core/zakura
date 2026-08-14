//! Operator-driven eligibility and body-retry policy.

use super::super::{
    BodyViolation, InvalidTransitionEvidence, OperatorViolation, TransitionFailure,
};
use crate::graph::HeaderGraphView;
use crate::{BodyValidationState, EligibilityReason};

use super::super::projected_state::ProjectedTransitionState;

/// Restart the selected persistent body alarm with a fresh episode.
pub(super) fn project_operator_body_retry(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::OperatorBodyRetry,
) -> Result<(), TransitionFailure> {
    if event.hash != projected.graph().view_select_best_header_chain()?.0.hash
        || event.availability.attempts != 0
        || event.availability.suppliers == 0
        || event.availability.alarmed
        || event.availability.started_at != event.availability.next_probe_at
    {
        return Err(
            InvalidTransitionEvidence::Body(BodyViolation::InvalidOperatorRetryEpisode).into(),
        );
    }
    if !matches!(
        projected.graph().view_header_node(event.hash).map(|node| &node.body_validation_state),
        Some(BodyValidationState::Unavailable(summary)) if summary.alarmed
    ) {
        return Err(InvalidTransitionEvidence::Body(
            BodyViolation::OperatorRetryRequiresPersistentAlarm,
        )
        .into());
    }
    projected.set_body_validation_state(
        event.hash,
        BodyValidationState::Unavailable(event.availability),
    )?;
    Ok(())
}

/// Add one reversible operator invalidation reason and dirty verified selection.
pub(super) fn project_operator_invalidation(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::OperatorInvalidate,
) -> Result<(), TransitionFailure> {
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest as _;
    hasher.update(b"zakura-operator-invalidation-v1");
    hasher.update(event.target.0);
    hasher.update(event.id.bytes());
    let expected_digest: [u8; 32] = hasher.finalize().into();
    if event.operator_reason_digest != expected_digest {
        return Err(InvalidTransitionEvidence::Operator(OperatorViolation::BindingMismatch).into());
    }
    if event.target == projected.graph().view_finalized_frontier().hash {
        return Err(
            InvalidTransitionEvidence::Operator(OperatorViolation::FinalizedAnchorTarget).into(),
        );
    }
    projected.add_operator_invalidation(
        event.target,
        EligibilityReason::operator_invalid(event.target, event.id, event.evidence),
    )?;
    Ok(())
}

/// Remove one reversible operator invalidation and dirty verified selection when needed.
pub(super) fn project_operator_reconsideration(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::OperatorReconsider,
) -> Result<(), TransitionFailure> {
    projected.remove_operator_invalidation(event.target, event.id, event.invalidation_evidence)?;
    Ok(())
}
