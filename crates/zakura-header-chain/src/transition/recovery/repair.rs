//! Repair classification and recovery-plan assembly.

use std::collections::BTreeSet;

use crate::EngineConfig;

use super::contracts::{RecoveryFailure, RecoveryPlan, RecoveryRepair, StoreAuditRead};
use super::phases::{AuditedSource, ReconstructedDerivedViews};

/// Compare reconstructed state with durable caches and assemble one recovery plan.
pub(super) fn classify_and_plan<S: StoreAuditRead>(
    store: &S,
    audited: AuditedSource,
    derived: ReconstructedDerivedViews,
    config: &EngineConfig,
) -> Result<RecoveryPlan, RecoveryFailure> {
    let AuditedSource {
        snapshot_before_repair,
        mut metadata,
        trust_anchor_changed,
        ..
    } = audited;
    let ReconstructedDerivedViews {
        source_nodes,
        header_nodes,
        header_child_edges,
        selected_projection,
        verified_projection,
        deferred_entries,
        selected_tip,
        selected_score,
        oldest_retained_height,
        body_unavailable_alarm,
    } = derived;

    let mut repairs = BTreeSet::new();
    if trust_anchor_changed {
        repairs.insert(RecoveryRepair::TrustAnchorConfiguration);
    }
    compare_by_key(
        store.header_child_edges()?,
        &header_child_edges,
        |(parent, child)| (parent.0, child.0),
        RecoveryRepair::ChildIndex,
        &mut repairs,
    );
    compare_by_key(
        store.deferred_entries()?,
        &deferred_entries,
        |(until, hash)| (until.timestamp(), until.timestamp_subsec_nanos(), hash.0),
        RecoveryRepair::DeferredIndex,
        &mut repairs,
    );
    if store.selected_projection()? != selected_projection
        || metadata.frontiers.header_best != selected_tip
        || metadata.header_best_score != selected_score
    {
        repairs.insert(RecoveryRepair::SelectedProjection);
    }
    if store.verified_projection()? != verified_projection {
        repairs.insert(RecoveryRepair::VerifiedProjection);
    }
    if source_nodes != header_nodes {
        repairs.insert(RecoveryRepair::InheritedEligibility);
    }
    if metadata.oldest_retained_height != oldest_retained_height {
        repairs.insert(RecoveryRepair::RetentionMetadata);
    }
    if metadata.alarms.header_best_body_unavailable != body_unavailable_alarm {
        repairs.insert(RecoveryRepair::BodyAvailabilityAlarm);
    }

    if !repairs.is_empty() {
        metadata.state_version = metadata.state_version.checked_next()?;
        metadata.anchor_manifest_digest = config.trust_anchor_digest();
        if repairs.contains(&RecoveryRepair::SelectedProjection)
            || repairs.contains(&RecoveryRepair::InheritedEligibility)
        {
            metadata.header_generation = metadata.header_generation.checked_next()?;
        }
        if repairs.contains(&RecoveryRepair::VerifiedProjection) {
            metadata.verified_generation = metadata.verified_generation.checked_next()?;
        }
        metadata.frontiers.header_best = selected_tip;
        metadata.header_best_score = selected_score;
        metadata.oldest_retained_height = oldest_retained_height;
        metadata.alarms.header_best_body_unavailable = body_unavailable_alarm;
    }

    Ok(RecoveryPlan {
        snapshot_before_repair,
        metadata,
        header_nodes,
        header_child_edges,
        selected_projection,
        verified_projection,
        deferred_entries,
        repairs,
    })
}

fn compare_by_key<T, K: Ord, F: FnMut(&T) -> K>(
    mut actual: Vec<T>,
    expected: &[T],
    mut key: F,
    repair: RecoveryRepair,
    repairs: &mut BTreeSet<RecoveryRepair>,
) where
    T: Clone + Eq,
{
    let mut expected = expected.to_vec();
    actual.sort_by_key(&mut key);
    expected.sort_by_key(key);
    if actual != expected {
        repairs.insert(repair);
    }
}
