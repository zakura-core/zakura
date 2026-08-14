//! Generation-increment leaf checks.

use crate::{Frontier, HeaderChainEngine, PlanCandidate};

use super::super::InvariantViolation;

pub(crate) fn verify_generations(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
    selected: &[Frontier],
    verified: &[Frontier],
) -> Result<(), InvariantViolation> {
    let old_selected = plan.snapshot_before_commit.frontiers.header_best;
    let old_verified = plan.snapshot_before_commit.frontiers.verified_best;
    let selected_changed = selected.last().copied() != Some(old_selected)
        || !plan.change_set.selected_projection.put.is_empty()
        || plan.change_set.selected_projection.remove_before.is_some()
        || plan.change_set.selected_projection.remove_from.is_some();
    let verified_changed = verified.last().copied() != Some(old_verified)
        || !plan.change_set.verified_projection.put.is_empty()
        || plan.change_set.verified_projection.remove_before.is_some()
        || plan.change_set.verified_projection.remove_from.is_some();
    let alarm_changed = plan.snapshot_before_commit.alarms != plan.change_set.metadata.alarms;
    let effects = !plan.change_set.put_nodes.is_empty()
        || !plan.change_set.delete_nodes.is_empty()
        || !plan.change_set.aux_changes.is_empty()
        || plan.change_set.finality_append.is_some()
        || selected_changed
        || verified_changed
        || alarm_changed;
    let expected_state = if effects {
        plan.snapshot_before_commit
            .state_version
            .checked_next()
            .ok()
    } else {
        Some(plan.snapshot_before_commit.state_version)
    };
    let header_validation_changed =
        plan.change_set
            .put_nodes
            .iter()
            .try_fold(false, |changed, node| {
                Ok::<_, InvariantViolation>(
                    changed
                        || engine_before_commit
                            .graph()
                            .header_node(node.hash)
                            .is_some_and(|old| old.validation != node.validation),
                )
            })?;
    let header_eligibility_changed = plan.change_set.put_nodes.iter().any(|node| {
        engine_before_commit
            .graph()
            .header_node(node.hash)
            .is_some_and(|old| old.is_eligible() != node.is_eligible())
    });
    let header_effect = selected_changed
        || !plan.change_set.index_changes.inserted.is_empty()
        || !plan.change_set.delete_nodes.is_empty()
        || header_validation_changed
        || header_eligibility_changed
        || !plan.change_set.eligibility_changes.is_empty()
        || plan.change_set.finality_append.is_some();
    let expected_header = if header_effect {
        plan.snapshot_before_commit
            .header_generation
            .checked_next()
            .ok()
    } else {
        Some(plan.snapshot_before_commit.header_generation)
    };
    let verified_effect = verified_changed || plan.change_set.finality_append.is_some();
    let expected_verified = if verified_effect {
        plan.snapshot_before_commit
            .verified_generation
            .checked_next()
            .ok()
    } else {
        Some(plan.snapshot_before_commit.verified_generation)
    };
    if Some(plan.change_set.metadata.state_version) != expected_state
        || Some(plan.change_set.metadata.header_generation) != expected_header
        || Some(plan.change_set.metadata.verified_generation) != expected_verified
    {
        return Err(InvariantViolation::Generation);
    }
    Ok(())
}
