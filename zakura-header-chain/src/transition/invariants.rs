//! Bounded commit-time verification of every projected transition invariant.

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use zakura_chain::block;

use crate::{
    AuxDelta, BodyValidationState, EligibilityReason, EngineMode, FinalitySource, Frontier,
    HeaderNode, MemHeaderStore, ProjectionDelta, StoreRead, TransitionPlan,
};

/// Stable, category-specific projected-state invariant failures.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum InvariantViolation {
    /// 1. A row key, canonical header, and locally computed hash disagree.
    #[error("node hash invariant failed at {0:?}")]
    NodeHash(block::Hash),
    /// 2. A non-anchor node lacks one exact height-minus-one parent.
    #[error("parent invariant failed at {0:?}")]
    Parent(block::Hash),
    /// 3. Hash, parent/child, height, or planned indexes do not round-trip.
    #[error("index invariant failed at {0:?}")]
    Index(block::Hash),
    /// 4. A work coordinate has the wrong origin or parent-plus-block value.
    #[error("work invariant failed at {0:?}")]
    Work(block::Hash),
    /// 5. Cached inherited eligibility differs from exact ancestry.
    #[error("eligibility invariant failed at {0:?}")]
    Eligibility(block::Hash),
    /// 6. The selected projection is not a gapless finalized-to-tip path.
    #[error("selected projection invariant failed at {0:?}")]
    SelectedProjection(block::Hash),
    /// 7. `header_best` is not the maximum eligible score.
    #[error("selection invariant failed")]
    Selection,
    /// 8. The verified projection contradicts its mode or body evidence.
    #[error("verified projection invariant failed at {0:?}")]
    VerifiedProjection(block::Hash),
    /// 9. A retained path conflicts with an authenticated trust pin.
    #[error("trust-pin invariant failed at height {0:?}")]
    TrustPin(block::Height),
    /// 10. Finalized, selected, or verified protected state was evicted.
    #[error("protected-path invariant failed at {0:?}")]
    Protected(block::Hash),
    /// 11. The projected DAG exceeds a frozen resource limit.
    #[error("resource-limit invariant failed")]
    Limits,
    /// 12. State or frontier generation increments disagree with actual changes.
    #[error("generation invariant failed")]
    Generation,
    /// 13. Auxiliary evidence lacks a retained foreign key or provenance link.
    #[error("auxiliary invariant failed at {0:?}")]
    Auxiliary(block::Hash),
    /// The coherent source view changed or failed while checking the plan.
    #[error("source snapshot changed during invariant verification")]
    SourceSnapshot,
}

/// Verify the complete projected state before an adapter may commit `plan`.
pub fn verify_plan<S: StoreRead>(
    before: &S,
    plan: &TransitionPlan,
) -> Result<(), InvariantViolation> {
    let source = before
        .snapshot()
        .map_err(|_| InvariantViolation::SourceSnapshot)?;
    if source != plan.before {
        return Err(InvariantViolation::SourceSnapshot);
    }
    let source_metadata = before
        .metadata()
        .map_err(|_| InvariantViolation::SourceSnapshot)?;
    let graph = plan.projected();
    let metadata = &plan.change_set.metadata;
    if source_metadata.state_version != source.state_version
        || metadata.mode != source.mode
        || metadata.work_origin != source_metadata.work_origin
    {
        return Err(InvariantViolation::SourceSnapshot);
    }
    if metadata.frontiers.finalized != graph.finalized()
        || metadata.frontiers.finalized.height < source.frontiers.finalized.height
        || match plan.change_set.finality_append {
            Some(record) => {
                record.previous != source.frontiers.finalized
                    || record.current != metadata.frontiers.finalized
                    || record.epoch != metadata.finality_epoch
            }
            None => metadata.frontiers.finalized != source.frontiers.finalized,
        }
    {
        return Err(InvariantViolation::Protected(
            metadata.frontiers.finalized.hash,
        ));
    }
    if let Some(record) = plan.change_set.finality_append {
        let valid_source = match record.source {
            FinalitySource::FullState { .. } => metadata.mode == EngineMode::Integrated,
            FinalitySource::HeadersOnlyDepth { selected_tip } => {
                metadata.mode == EngineMode::HeadersOnly
                    && selected_tip
                        .height
                        .0
                        .saturating_sub(record.current.height.0)
                        == plan.limits.local_finality_depth.get()
                    && graph
                        .ancestor(selected_tip.hash, record.current.height)
                        .ok()
                        .flatten()
                        == Some(record.current)
            }
            FinalitySource::MigratedHeadersOnly => true,
        };
        if !valid_source {
            return Err(InvariantViolation::Protected(record.current.hash));
        }
    } else if metadata.finality_epoch != source_metadata.finality_epoch {
        return Err(InvariantViolation::Generation);
    }
    for node in graph.nodes() {
        verify_node(graph, node, metadata.work_origin.hash)?;
    }
    verify_indexes(before, plan)?;
    verify_candidates(plan)?;
    let selected = projected_path(before, &source, &plan.change_set.selected_projection, true)?;
    let verified = projected_path(before, &source, &plan.change_set.verified_projection, false)?;
    verify_projection(
        graph,
        &selected,
        metadata.frontiers.header_best,
        InvariantViolation::SelectedProjection,
    )?;
    let best = graph
        .select_header_best()
        .map_err(|_| InvariantViolation::Selection)?;
    if best.0 != metadata.frontiers.header_best || best.1 != metadata.header_best_score {
        return Err(InvariantViolation::Selection);
    }
    verify_verified(
        graph,
        metadata.mode,
        &verified,
        metadata.frontiers.verified_best,
    )?;
    verify_pins(graph, &plan.trust_pins, &selected, &verified)?;
    verify_protected(graph, plan)?;
    if graph.len().saturating_sub(1) > plan.limits.max_non_finalized_nodes.get()
        || graph.eligible_tips().len() > plan.limits.max_candidate_tips.get()
    {
        return Err(InvariantViolation::Limits);
    }
    verify_generations(plan, &selected, &verified)?;
    verify_aux(before, plan)?;
    Ok(())
}

fn verify_candidates(plan: &TransitionPlan) -> Result<(), InvariantViolation> {
    let mut expected: Vec<_> = plan
        .projected()
        .eligible_tips()
        .into_iter()
        .map(|tip| {
            plan.projected()
                .score(tip.hash)
                .map(|score| (score, tip.hash))
                .map_err(|_| InvariantViolation::Selection)
        })
        .collect::<Result<_, _>>()?;
    expected.sort_unstable_by_key(|(score, hash)| (*score, hash.0));
    if plan.change_set.candidate_tips != expected {
        return Err(InvariantViolation::Selection);
    }
    Ok(())
}

fn verify_node(
    graph: &MemHeaderStore,
    node: &HeaderNode,
    work_origin: block::Hash,
) -> Result<(), InvariantViolation> {
    if node.header.hash() != node.hash {
        return Err(InvariantViolation::NodeHash(node.hash));
    }
    if !graph.hashes_at_height(node.height).contains(&node.hash) {
        return Err(InvariantViolation::Index(node.hash));
    }
    if node.work_coordinate().origin_hash() != work_origin {
        return Err(InvariantViolation::Work(node.hash));
    }
    if node.hash == graph.finalized().hash {
        if node.eligibility.inherited_from.is_some() {
            return Err(InvariantViolation::Eligibility(node.hash));
        }
        return Ok(());
    }
    let parent = graph
        .node(node.parent_hash)
        .ok_or(InvariantViolation::Parent(node.hash))?;
    if parent.height.next().ok() != Some(node.height)
        || !graph.children(parent.hash).contains(&node.hash)
    {
        return Err(InvariantViolation::Parent(node.hash));
    }
    if parent.work_coordinate().checked_add(node.block_work).ok() != Some(node.work_coordinate()) {
        return Err(InvariantViolation::Work(node.hash));
    }
    if node.eligibility.inherited_from != (!parent.is_eligible()).then_some(parent.hash) {
        return Err(InvariantViolation::Eligibility(node.hash));
    }
    Ok(())
}

fn verify_indexes<S: StoreRead>(
    before: &S,
    plan: &TransitionPlan,
) -> Result<(), InvariantViolation> {
    let mut inserted = HashSet::new();
    for node in &plan.change_set.put_nodes {
        if before
            .node(node.hash)
            .map_err(|_| InvariantViolation::SourceSnapshot)?
            .is_none()
        {
            inserted.insert(Frontier::new(node.height, node.hash));
        }
    }
    let indexed: HashSet<_> = plan
        .change_set
        .index_changes
        .inserted
        .iter()
        .copied()
        .collect();
    if inserted != indexed {
        return Err(InvariantViolation::Index(
            inserted
                .symmetric_difference(&indexed)
                .next()
                .map_or(block::Hash([0; 32]), |frontier| frontier.hash),
        ));
    }
    let deleted: HashSet<_> = plan.change_set.delete_nodes.iter().copied().collect();
    let deindexed: HashSet<_> = plan
        .change_set
        .index_changes
        .deleted
        .iter()
        .copied()
        .collect();
    if deleted != deindexed {
        return Err(InvariantViolation::Index(
            deleted
                .symmetric_difference(&deindexed)
                .next()
                .copied()
                .unwrap_or(block::Hash([0; 32])),
        ));
    }
    Ok(())
}

fn projected_path<S: StoreRead>(
    before: &S,
    source: &crate::EngineSnapshot,
    delta: &ProjectionDelta,
    selected: bool,
) -> Result<Vec<Frontier>, InvariantViolation> {
    let tip = if selected {
        source.frontiers.header_best
    } else {
        source.frontiers.verified_best
    };
    let mut path = Vec::new();
    for raw_height in source.frontiers.finalized.height.0..=tip.height.0 {
        let height = block::Height(raw_height);
        let hash = if selected {
            before.selected_hash(height)
        } else {
            before.verified_hash(height)
        }
        .map_err(|_| InvariantViolation::SourceSnapshot)?
        .ok_or(InvariantViolation::SourceSnapshot)?;
        path.push(Frontier::new(height, hash));
    }
    if let Some(remove_from) = delta.remove_from {
        path.retain(|frontier| frontier.height < remove_from);
    }
    path.extend(delta.put.iter().copied());
    Ok(path)
}

fn verify_projection(
    graph: &MemHeaderStore,
    projection: &[Frontier],
    tip: Frontier,
    failure: fn(block::Hash) -> InvariantViolation,
) -> Result<(), InvariantViolation> {
    if projection.first().copied() != Some(graph.finalized())
        || projection.last().copied() != Some(tip)
    {
        return Err(failure(tip.hash));
    }
    for pair in projection.windows(2) {
        if pair[1].height.0 != pair[0].height.0 + 1
            || graph
                .node(pair[1].hash)
                .is_none_or(|node| node.parent_hash != pair[0].hash)
        {
            return Err(failure(pair[1].hash));
        }
    }
    Ok(())
}

fn verify_verified(
    graph: &MemHeaderStore,
    mode: EngineMode,
    projection: &[Frontier],
    tip: Frontier,
) -> Result<(), InvariantViolation> {
    verify_projection(
        graph,
        projection,
        tip,
        InvariantViolation::VerifiedProjection,
    )?;
    if mode == EngineMode::HeadersOnly && projection != [graph.finalized()] {
        return Err(InvariantViolation::VerifiedProjection(tip.hash));
    }
    if mode == EngineMode::Integrated {
        for frontier in projection.iter().skip(1) {
            if !matches!(
                graph.node(frontier.hash).map(|node| node.body.clone()),
                Some(BodyValidationState::Verified { .. })
            ) {
                return Err(InvariantViolation::VerifiedProjection(frontier.hash));
            }
        }
    }
    Ok(())
}

fn verify_pins(
    graph: &MemHeaderStore,
    pins: &[Frontier],
    selected: &[Frontier],
    verified: &[Frontier],
) -> Result<(), InvariantViolation> {
    for pin in pins {
        for projection in [selected, verified] {
            if let Some(frontier) = projection
                .iter()
                .find(|frontier| frontier.height == pin.height)
            {
                if frontier.hash != pin.hash {
                    return Err(InvariantViolation::TrustPin(pin.height));
                }
            }
        }
        for hash in graph.hashes_at_height(pin.height) {
            if hash == pin.hash {
                continue;
            }
            let node = graph
                .node(hash)
                .ok_or(InvariantViolation::TrustPin(pin.height))?;
            let has_reason = node.eligibility.direct_reasons.iter().any(|reason| {
                matches!(reason,
                    EligibilityReason::SettledUpgradeConflict { height, expected }
                    | EligibilityReason::CheckpointConflict { height, expected }
                    if *height == pin.height && *expected == pin.hash)
            });
            if !has_reason {
                return Err(InvariantViolation::TrustPin(pin.height));
            }
        }
    }
    Ok(())
}

fn verify_protected(
    graph: &MemHeaderStore,
    plan: &TransitionPlan,
) -> Result<(), InvariantViolation> {
    for frontier in [
        plan.change_set.metadata.frontiers.finalized,
        plan.change_set.metadata.frontiers.header_best,
        plan.change_set.metadata.frontiers.verified_best,
    ] {
        if graph.node(frontier.hash).is_none()
            || plan.change_set.delete_nodes.contains(&frontier.hash)
        {
            return Err(InvariantViolation::Protected(frontier.hash));
        }
    }
    Ok(())
}

fn verify_generations(
    plan: &TransitionPlan,
    selected: &[Frontier],
    verified: &[Frontier],
) -> Result<(), InvariantViolation> {
    let old_selected = plan.before.frontiers.header_best;
    let old_verified = plan.before.frontiers.verified_best;
    let selected_changed = selected.last().copied() != Some(old_selected)
        || !plan.change_set.selected_projection.put.is_empty()
        || plan.change_set.selected_projection.remove_from.is_some();
    let verified_changed = verified.last().copied() != Some(old_verified)
        || !plan.change_set.verified_projection.put.is_empty()
        || plan.change_set.verified_projection.remove_from.is_some();
    let effects = !plan.change_set.put_nodes.is_empty()
        || !plan.change_set.delete_nodes.is_empty()
        || !plan.change_set.aux_changes.is_empty()
        || plan.change_set.finality_append.is_some()
        || selected_changed
        || verified_changed;
    let expected_state = if effects {
        plan.before.state_version.checked_next().ok()
    } else {
        Some(plan.before.state_version)
    };
    let header_effect = selected_changed
        || !plan.change_set.eligibility_changes.is_empty()
        || plan.change_set.finality_append.is_some();
    let expected_header = if header_effect {
        plan.before.header_generation.checked_next().ok()
    } else {
        Some(plan.before.header_generation)
    };
    let verified_effect = verified_changed || plan.change_set.finality_append.is_some();
    let expected_verified = if verified_effect {
        plan.before.verified_generation.checked_next().ok()
    } else {
        Some(plan.before.verified_generation)
    };
    if Some(plan.change_set.metadata.state_version) != expected_state
        || Some(plan.change_set.metadata.header_generation) != expected_header
        || Some(plan.change_set.metadata.verified_generation) != expected_verified
    {
        return Err(InvariantViolation::Generation);
    }
    Ok(())
}

fn verify_aux<S: StoreRead>(before: &S, plan: &TransitionPlan) -> Result<(), InvariantViolation> {
    let deleted_ids: HashSet<_> = plan
        .change_set
        .aux_changes
        .iter()
        .filter_map(|change| match change {
            AuxDelta::Delete(id) => Some(*id),
            AuxDelta::Put(_) => None,
        })
        .collect();
    let puts: HashMap<_, _> = plan
        .change_set
        .aux_changes
        .iter()
        .filter_map(|change| match change {
            AuxDelta::Put(delivery) => Some((delivery.delivery_id, delivery.as_ref())),
            AuxDelta::Delete(_) => None,
        })
        .collect();
    for node in plan.projected.nodes() {
        let mut deliveries = before
            .aux_deliveries(node.hash)
            .map_err(|_| InvariantViolation::SourceSnapshot)?;
        deliveries.retain(|delivery| !deleted_ids.contains(&delivery.delivery_id));
        deliveries.extend(
            puts.values()
                .filter(|delivery| delivery.header_hash == node.hash)
                .map(|delivery| **delivery),
        );
        for delivery in deliveries {
            if delivery.header_hash != node.hash
                || !node.aux_delivery_ids.contains(&delivery.delivery_id)
            {
                return Err(InvariantViolation::Auxiliary(node.hash));
            }
        }
    }
    for delivery in puts.values() {
        if plan.projected.node(delivery.header_hash).is_none() {
            return Err(InvariantViolation::Auxiliary(delivery.header_hash));
        }
    }
    for hash in &plan.change_set.delete_nodes {
        for delivery in before
            .aux_deliveries(*hash)
            .map_err(|_| InvariantViolation::SourceSnapshot)?
        {
            if !deleted_ids.contains(&delivery.delivery_id) {
                return Err(InvariantViolation::Auxiliary(*hash));
            }
        }
    }
    Ok(())
}
