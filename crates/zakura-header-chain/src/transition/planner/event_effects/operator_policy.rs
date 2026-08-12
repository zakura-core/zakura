//! Operator-driven eligibility and body-retry policy.

use crate::graph::HeaderGraphView;
use crate::{BodyValidationState, EligibilityReason, TransitionFailure};

use super::super::projected_state::ProjectedTransitionState;

/// Restart the selected persistent body alarm with a fresh episode.
pub(super) fn apply_body_retry(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::OperatorBodyRetry,
) -> Result<(), TransitionFailure> {
    if event.hash != projected.graph().view_select_best_header_chain()?.0.hash
        || event.availability.attempts != 0
        || event.availability.suppliers == 0
        || event.availability.alarmed
        || event.availability.started_at != event.availability.next_probe_at
    {
        return Err(TransitionFailure::InvalidEvidence(
            "operator body retry has an invalid fresh episode",
        ));
    }
    if !matches!(
        projected.graph().view_header_node(event.hash).map(|node| &node.body_validation_state),
        Some(BodyValidationState::Unavailable(summary)) if summary.alarmed
    ) {
        return Err(TransitionFailure::InvalidEvidence(
            "operator body retry requires the selected persistent alarm",
        ));
    }
    projected.set_body_validation_state(
        event.hash,
        BodyValidationState::Unavailable(event.availability),
    )?;
    Ok(())
}

/// Add one reversible operator invalidation reason and dirty verified selection.
pub(super) fn apply_invalidate(
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
        return Err(TransitionFailure::InvalidEvidence(
            "operator invalidation identity is not bound to its target",
        ));
    }
    if event.target == projected.graph().view_finalized_frontier().hash {
        return Err(TransitionFailure::InvalidEvidence(
            "operator invalidation cannot target the finalized anchor",
        ));
    }
    projected.add_operator_invalidation(
        event.target,
        EligibilityReason::operator_invalid(event.target, event.id, event.evidence),
    )?;
    Ok(())
}

/// Remove one reversible operator invalidation and dirty verified selection when needed.
pub(super) fn apply_reconsider(
    projected: &mut ProjectedTransitionState<'_>,
    event: &crate::OperatorReconsider,
) -> Result<(), TransitionFailure> {
    projected.remove_operator_invalidation(event.target, event.id, event.invalidation_evidence)?;
    Ok(())
}
