//! Bounded commit-time verification of every projected transition invariant.

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use zakura_chain::block;

use crate::graph::GraphOverlay;
use crate::graph::HeaderGraphView;
use crate::{
    AuxDelta, BodyValidationState, EligibilityReason, EngineMode, FinalitySource, Frontier,
    HeaderChainEngine, HeaderNode, ProjectionDelta, TransitionPlan,
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
pub(crate) fn verify_plan(
    before: &HeaderChainEngine,
    plan: &TransitionPlan,
) -> Result<(), InvariantViolation> {
    let source = before.snapshot();
    if source != plan.before {
        return Err(InvariantViolation::SourceSnapshot);
    }
    before
        .graph()
        .validate_delta(plan.graph_delta())
        .map_err(|_| InvariantViolation::Index(block::Hash([0; 32])))?;
    let source_metadata = before.metadata();
    let delta_graph = GraphOverlay::from_delta(before.graph(), plan.graph_delta());
    let delta_finalized = delta_graph.view_finalized();
    #[cfg(any(test, feature = "fuzz-impl"))]
    let graph = plan.projected_graph().clone();
    #[cfg(not(any(test, feature = "fuzz-impl")))]
    let graph = delta_graph;
    let metadata = &plan.change_set.metadata;
    if source_metadata.state_version != source.state_version
        || metadata.mode != source.mode
        || metadata.work_origin != source_metadata.work_origin
    {
        return Err(InvariantViolation::SourceSnapshot);
    }
    if metadata.frontiers.finalized != graph.view_finalized()
        || metadata.frontiers.finalized != delta_finalized
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
                        .view_ancestor(selected_tip.hash, record.current.height)
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
    #[cfg(any(test, feature = "fuzz-impl"))]
    let nodes = graph.view_nodes();
    #[cfg(not(any(test, feature = "fuzz-impl")))]
    let nodes = changed_boundary_nodes(&graph, plan);
    for node in nodes {
        verify_node(&graph, node, metadata.work_origin.hash)?;
    }
    verify_indexes(before, plan)?;
    let selected = projected_path(before, &source, &plan.change_set.selected_projection, true)?;
    let verified = projected_path(before, &source, &plan.change_set.verified_projection, false)?;
    verify_projection(
        &graph,
        &selected,
        metadata.frontiers.header_best,
        InvariantViolation::SelectedProjection,
    )?;
    let best = graph
        .view_select_header_best()
        .map_err(|_| InvariantViolation::Selection)?;
    if best.0 != metadata.frontiers.header_best || best.1 != metadata.header_best_score {
        return Err(InvariantViolation::Selection);
    }
    verify_verified(
        &graph,
        metadata.mode,
        &verified,
        metadata.frontiers.verified_best,
    )?;
    verify_pins(&graph, &plan.trust_pins, &selected, &verified)?;
    verify_protected(&graph, plan)?;
    if graph.view_node_count().saturating_sub(1) > plan.limits.max_non_finalized_nodes.get()
        || graph.view_eligible_tips().len() > plan.limits.max_candidate_tips.get()
    {
        return Err(InvariantViolation::Limits);
    }
    verify_generations(before, plan, &selected, &verified)?;
    verify_aux(before, &graph, plan)?;
    Ok(())
}

fn verify_node<G: HeaderGraphView>(
    graph: &G,
    node: &HeaderNode,
    work_origin: block::Hash,
) -> Result<(), InvariantViolation> {
    if node.header.hash() != node.hash {
        return Err(InvariantViolation::NodeHash(node.hash));
    }
    if !graph
        .view_hashes_at_height(node.height)
        .contains(&node.hash)
    {
        return Err(InvariantViolation::Index(node.hash));
    }
    if node.work_coordinate().origin_hash() != work_origin {
        return Err(InvariantViolation::Work(node.hash));
    }
    if node.hash == graph.view_finalized().hash {
        if node.eligibility.inherited_from.is_some() {
            return Err(InvariantViolation::Eligibility(node.hash));
        }
        return Ok(());
    }
    let parent = graph
        .view_node(node.parent_hash)
        .ok_or(InvariantViolation::Parent(node.hash))?;
    if parent.height.next().ok() != Some(node.height)
        || !graph.view_children(parent.hash).contains(&node.hash)
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

fn verify_indexes(
    before: &HeaderChainEngine,
    plan: &TransitionPlan,
) -> Result<(), InvariantViolation> {
    if plan.change_set.put_nodes != plan.graph_delta().put_nodes
        || plan.change_set.delete_nodes != plan.graph_delta().delete_nodes
    {
        return Err(InvariantViolation::Index(block::Hash([0; 32])));
    }
    let mut inserted = HashSet::new();
    for node in &plan.change_set.put_nodes {
        if before.graph().node(node.hash).is_none() {
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

fn projected_path(
    before: &HeaderChainEngine,
    source: &crate::EngineSnapshot,
    delta: &ProjectionDelta,
    selected: bool,
) -> Result<Vec<Frontier>, InvariantViolation> {
    let tip = if selected {
        source.frontiers.header_best
    } else {
        source.frontiers.verified_best
    };
    let mut path = if selected {
        before.selected_projection().to_vec()
    } else {
        before.verified_projection().to_vec()
    };
    if path.last().copied() != Some(tip)
        || path.first().copied() != Some(source.frontiers.finalized)
    {
        return Err(InvariantViolation::SourceSnapshot);
    }
    if let Some(remove_before) = delta.remove_before {
        path.retain(|frontier| frontier.height >= remove_before);
    }
    if let Some(remove_from) = delta.remove_from {
        path.retain(|frontier| frontier.height < remove_from);
    }
    path.extend(delta.put.iter().copied());
    Ok(path)
}

fn verify_projection<G: HeaderGraphView>(
    graph: &G,
    projection: &[Frontier],
    tip: Frontier,
    failure: fn(block::Hash) -> InvariantViolation,
) -> Result<(), InvariantViolation> {
    if projection.first().copied() != Some(graph.view_finalized())
        || projection.last().copied() != Some(tip)
    {
        return Err(failure(tip.hash));
    }
    for pair in projection.windows(2) {
        if pair[1].height.0 != pair[0].height.0 + 1
            || graph
                .view_node(pair[1].hash)
                .is_none_or(|node| node.parent_hash != pair[0].hash)
        {
            return Err(failure(pair[1].hash));
        }
    }
    Ok(())
}

fn verify_verified<G: HeaderGraphView>(
    graph: &G,
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
    if mode == EngineMode::HeadersOnly && projection != [graph.view_finalized()] {
        return Err(InvariantViolation::VerifiedProjection(tip.hash));
    }
    if mode == EngineMode::Integrated {
        for frontier in projection.iter().skip(1) {
            if !matches!(
                graph.view_node(frontier.hash).map(|node| node.body.clone()),
                Some(BodyValidationState::Verified { .. })
            ) {
                return Err(InvariantViolation::VerifiedProjection(frontier.hash));
            }
        }
    }
    Ok(())
}

fn verify_pins<G: HeaderGraphView>(
    graph: &G,
    pins: &[Frontier],
    selected: &[Frontier],
    verified: &[Frontier],
) -> Result<(), InvariantViolation> {
    for pin in pins {
        for projection in [selected, verified] {
            if let Ok(index) =
                projection.binary_search_by_key(&pin.height, |frontier| frontier.height)
            {
                let frontier = projection[index];
                if frontier.hash != pin.hash {
                    return Err(InvariantViolation::TrustPin(pin.height));
                }
            }
        }
        for hash in graph.view_hashes_at_height(pin.height) {
            if hash == pin.hash {
                continue;
            }
            let node = graph
                .view_node(hash)
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

fn verify_protected<G: HeaderGraphView>(
    graph: &G,
    plan: &TransitionPlan,
) -> Result<(), InvariantViolation> {
    for frontier in [
        plan.change_set.metadata.frontiers.finalized,
        plan.change_set.metadata.frontiers.header_best,
        plan.change_set.metadata.frontiers.verified_best,
    ] {
        if graph.view_node(frontier.hash).is_none()
            || plan.change_set.delete_nodes.contains(&frontier.hash)
        {
            return Err(InvariantViolation::Protected(frontier.hash));
        }
    }
    Ok(())
}

fn verify_generations(
    before: &HeaderChainEngine,
    plan: &TransitionPlan,
    selected: &[Frontier],
    verified: &[Frontier],
) -> Result<(), InvariantViolation> {
    let old_selected = plan.before.frontiers.header_best;
    let old_verified = plan.before.frontiers.verified_best;
    let selected_changed = selected.last().copied() != Some(old_selected)
        || !plan.change_set.selected_projection.put.is_empty()
        || plan.change_set.selected_projection.remove_before.is_some()
        || plan.change_set.selected_projection.remove_from.is_some();
    let verified_changed = verified.last().copied() != Some(old_verified)
        || !plan.change_set.verified_projection.put.is_empty()
        || plan.change_set.verified_projection.remove_before.is_some()
        || plan.change_set.verified_projection.remove_from.is_some();
    let alarm_changed = plan.before.alarms != plan.change_set.metadata.alarms;
    let effects = !plan.change_set.put_nodes.is_empty()
        || !plan.change_set.delete_nodes.is_empty()
        || !plan.change_set.aux_changes.is_empty()
        || plan.change_set.finality_append.is_some()
        || selected_changed
        || verified_changed
        || alarm_changed;
    let expected_state = if effects {
        plan.before.state_version.checked_next().ok()
    } else {
        Some(plan.before.state_version)
    };
    let header_validation_changed =
        plan.change_set
            .put_nodes
            .iter()
            .try_fold(false, |changed, node| {
                Ok::<_, InvariantViolation>(
                    changed
                        || before
                            .graph()
                            .node(node.hash)
                            .is_some_and(|old| old.validation != node.validation),
                )
            })?;
    let header_effect = selected_changed
        || !plan.change_set.index_changes.inserted.is_empty()
        || !plan.change_set.delete_nodes.is_empty()
        || header_validation_changed
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

fn verify_aux<G: HeaderGraphView>(
    before: &HeaderChainEngine,
    graph: &G,
    plan: &TransitionPlan,
) -> Result<(), InvariantViolation> {
    let mut put_ids = HashSet::new();
    for change in &plan.change_set.aux_changes {
        let AuxDelta::Put(delivery) = change else {
            continue;
        };
        if !put_ids.insert(delivery.delivery_id)
            || before
                .aux_delivery(delivery.delivery_id)
                .is_some_and(|existing| existing.header_hash != delivery.header_hash)
        {
            return Err(InvariantViolation::Auxiliary(delivery.header_hash));
        }
    }
    let deletes: Vec<_> = plan
        .change_set
        .aux_changes
        .iter()
        .filter_map(|change| match change {
            AuxDelta::Delete {
                header_hash,
                delivery_id,
            } => Some((*header_hash, *delivery_id)),
            AuxDelta::Put(_) => None,
        })
        .collect();
    for (header_hash, delivery_id) in &deletes {
        let exists = before
            .aux_deliveries(*header_hash)
            .iter()
            .any(|delivery| delivery.delivery_id == *delivery_id);
        if !exists {
            return Err(InvariantViolation::Auxiliary(*header_hash));
        }
    }
    let deleted_ids: HashSet<_> = deletes
        .into_iter()
        .map(|(_, delivery_id)| delivery_id)
        .collect();
    let puts: HashMap<_, _> = plan
        .change_set
        .aux_changes
        .iter()
        .filter_map(|change| match change {
            AuxDelta::Put(delivery) => Some((delivery.delivery_id, delivery.as_ref())),
            AuxDelta::Delete { .. } => None,
        })
        .collect();
    #[cfg(any(test, feature = "fuzz-impl"))]
    let nodes = graph.view_nodes();
    #[cfg(not(any(test, feature = "fuzz-impl")))]
    let nodes: Vec<_> = plan
        .change_set
        .put_nodes
        .iter()
        .filter_map(|changed| graph.view_node(changed.hash))
        .collect();
    for node in nodes {
        let mut deliveries = before.aux_deliveries(node.hash).to_vec();
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
        if graph.view_node(delivery.header_hash).is_none() {
            return Err(InvariantViolation::Auxiliary(delivery.header_hash));
        }
    }
    for hash in &plan.change_set.delete_nodes {
        for delivery in before.aux_deliveries(*hash) {
            if !deleted_ids.contains(&delivery.delivery_id) {
                return Err(InvariantViolation::Auxiliary(*hash));
            }
        }
    }
    Ok(())
}

#[cfg(not(any(test, feature = "fuzz-impl")))]
fn changed_boundary_nodes<'a, G: HeaderGraphView>(
    graph: &'a G,
    plan: &TransitionPlan,
) -> Vec<&'a HeaderNode> {
    let mut hashes = HashSet::from([
        plan.change_set.metadata.frontiers.finalized.hash,
        plan.change_set.metadata.frontiers.header_best.hash,
        plan.change_set.metadata.frontiers.verified_best.hash,
    ]);
    for node in &plan.graph_delta().put_nodes {
        hashes.insert(node.hash);
        hashes.insert(node.parent_hash);
        hashes.extend(graph.view_children(node.hash));
    }
    for (parent, child) in plan
        .graph_delta()
        .add_children
        .iter()
        .chain(&plan.graph_delta().remove_children)
    {
        hashes.insert(*parent);
        hashes.insert(*child);
    }
    hashes
        .into_iter()
        .filter_map(|hash| graph.view_node(hash))
        .collect()
}
