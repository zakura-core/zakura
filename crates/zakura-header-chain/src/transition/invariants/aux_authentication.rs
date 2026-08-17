//! Incremental fast-path for auxiliary authentication transitions.

use crate::{AuxDelta, HeaderChainEngine, PlanCandidate, ProjectionDelta};

use super::checks::{verify_aux, verify_generations};
use super::{InvariantViolation, VerificationMode};
use zakura_chain::block;

pub(crate) fn is_incremental_aux_authentication(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
) -> bool {
    let metadata = &plan.change_set.metadata;
    let source_metadata = engine_before_commit.metadata();

    plan.effect().is_aux_authentication()
        && !plan.change_set.aux_changes.is_empty()
        && plan.change_set.aux_changes.len() <= 2
        && plan
            .change_set
            .aux_changes
            .iter()
            .all(|change| matches!(change, AuxDelta::Put(_)))
        && plan.change_set.put_nodes.is_empty()
        && plan.change_set.delete_nodes.is_empty()
        && plan.change_set.index_changes.inserted.is_empty()
        && plan.change_set.index_changes.deleted.is_empty()
        && plan.change_set.selected_projection == ProjectionDelta::default()
        && plan.change_set.verified_projection == ProjectionDelta::default()
        && plan.change_set.eligibility_changes.is_empty()
        && plan.change_set.finality_append.is_none()
        && plan.graph_delta().is_empty()
        && metadata.disk_format == source_metadata.disk_format
        && metadata.mode == source_metadata.mode
        && metadata.network_id == source_metadata.network_id
        && metadata.network_policy_digest == source_metadata.network_policy_digest
        && metadata.anchor_manifest_digest == source_metadata.anchor_manifest_digest
        && metadata.work_origin == source_metadata.work_origin
        && metadata.finality_epoch == source_metadata.finality_epoch
        && metadata.frontiers == source_metadata.frontiers
        && metadata.header_best_score == source_metadata.header_best_score
        && metadata.oldest_retained_height == source_metadata.oldest_retained_height
        && metadata.alarms == source_metadata.alarms
}

pub(crate) fn verify_incremental_aux_authentication(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
    mode: VerificationMode,
) -> Result<(), InvariantViolation> {
    if engine_before_commit.snapshot() != plan.snapshot_before_commit {
        return Err(InvariantViolation::SnapshotBeforeCommit);
    }
    for change in &plan.change_set.aux_changes {
        let AuxDelta::Put(delivery) = change else {
            return Err(InvariantViolation::Auxiliary(block::Hash([0; 32])));
        };
        let existing = engine_before_commit
            .aux_deliveries(delivery.header_hash)
            .iter()
            .find(|existing| existing.delivery_id == delivery.delivery_id)
            .ok_or(InvariantViolation::Auxiliary(delivery.header_hash))?;
        let expected = existing.with_outcome(delivery.outcome());
        if expected != **delivery
            || !existing
                .outcome()
                .can_refine_to(delivery.outcome().status())
        {
            return Err(InvariantViolation::Auxiliary(delivery.header_hash));
        }
    }
    verify_generations(
        engine_before_commit,
        plan,
        engine_before_commit.selected_projection(),
        engine_before_commit.verified_projection(),
    )?;
    verify_aux(
        engine_before_commit,
        engine_before_commit.graph(),
        plan,
        mode,
    )?;
    Ok(())
}
