//! Deferred local-time header reevaluation.

use crate::graph::HeaderGraphView;
use crate::{HeaderValidationState, TransitionFailure};

use super::super::projected_state::ProjectedTransitionState;
use super::EventProjectionContext;

/// Promote every due deferred header to valid using the authoritative clock.
pub(super) fn reevaluate_elapsed_deferrals(
    projected: &mut ProjectedTransitionState<'_>,
    event_context: &EventProjectionContext<'_>,
) -> Result<(), TransitionFailure> {
    let now = event_context.transition.clock.now();
    let due: Vec<_> = projected
        .graph()
        .view_header_nodes()
        .into_iter()
        .filter_map(|node| match node.validation {
            HeaderValidationState::DeferredUntil(until) if until <= now => Some(node.hash),
            _ => None,
        })
        .collect();
    for hash in due {
        projected.set_header_validation_state(hash, HeaderValidationState::Valid)?;
    }
    Ok(())
}
