//! Generation policy and durable write-set derivation.

use std::{borrow::Cow, sync::Arc};

use crate::graph::{GraphDelta, HeaderGraphView};
use crate::{
    BodyValidationState, ChangeSet, EligibilityDelta, EngineLimits, EngineMetadata, EngineSnapshot,
    Frontier, FrontierSet, GraphError, HeaderChainEngine, IndexChanges, MemHeaderStore,
    ProjectionDelta, TransitionContext, TransitionDomain, TransitionEffect, TransitionEvent,
    TransitionFingerprint,
};

use super::admission::validate_authority;
use super::projected_state::SettledProjectedState;
use super::retention::RetentionPlan;
use super::{
    InvalidTransitionEvidence, PlanCandidate, PlannerCoherenceViolation, ProjectionKind,
    TransitionFailure,
};

/// Inputs required to derive the atomic write set from settled projections.
pub(super) struct DerivePlanInputs<'a> {
    pub(super) snapshot_before_commit: EngineSnapshot,
    pub(super) metadata: EngineMetadata,
    pub(super) base_graph: &'a MemHeaderStore,
    pub(super) projected: SettledProjectedState<'a>,
    pub(super) old_selected: &'a [Frontier],
    pub(super) old_verified: &'a [Frontier],
    pub(super) selected: Cow<'a, [Frontier]>,
    pub(super) finality_append: Option<crate::FinalityRecord>,
    pub(super) retention: RetentionPlan,
    pub(super) fingerprint: Option<TransitionFingerprint>,
    pub(super) domain: TransitionDomain,
    pub(super) effect: TransitionEffect,
    pub(super) trust_pins: Arc<[Frontier]>,
    pub(super) limits: EngineLimits,
}

/// Derive the atomic change set and private graph delta.
pub(super) fn derive_plan(
    inputs: DerivePlanInputs<'_>,
) -> Result<PlanCandidate, TransitionFailure> {
    let DerivePlanInputs {
        snapshot_before_commit,
        mut metadata,
        base_graph,
        projected,
        old_selected,
        old_verified,
        selected,
        finality_append,
        retention,
        fingerprint,
        domain,
        effect,
        trust_pins,
        limits,
    } = inputs;
    let (graph, graph_delta, verified, aux_changes) = projected.into_write_parts();
    let put_nodes = graph_delta.updated_header_nodes().to_vec();
    let delete_nodes = graph_delta.deleted_header_hashes().to_vec();
    let put_consensus_invalid_body_tombstones =
        graph_delta.new_consensus_invalid_body_tombstones().to_vec();
    let mut eligibility_changes: Vec<_> = put_nodes
        .iter()
        .filter_map(|node| {
            let old = base_graph.header_node(node.hash)?;
            (old.eligibility != node.eligibility).then(|| EligibilityDelta {
                hash: node.hash,
                before: old.eligibility.clone(),
                after: node.eligibility.clone(),
            })
        })
        .collect();
    eligibility_changes.sort_unstable_by_key(|delta| delta.hash.0);
    let selected_changed = selected.as_ref() != old_selected;
    let verified_changed = verified.as_ref() != old_verified;
    let header_topology_changed = !delete_nodes.is_empty()
        || put_nodes
            .iter()
            .any(|node| base_graph.header_node(node.hash).is_none());
    let header_validation_changed = put_nodes.iter().any(|node| {
        base_graph
            .header_node(node.hash)
            .is_some_and(|old| old.validation != node.validation)
    });
    let header_eligibility_changed = put_nodes.iter().any(|node| {
        base_graph
            .header_node(node.hash)
            .is_some_and(|old| old.is_eligible() != node.is_eligible())
    });
    let selected_tip = *selected.last().ok_or(TransitionFailure::InvalidEvidence(
        InvalidTransitionEvidence::Planner(PlannerCoherenceViolation::EmptyProjection(
            ProjectionKind::Selected,
        )),
    ))?;
    metadata.alarms.resource_stalled = retention.resource_stalled;
    let selected_tip_node = graph
        .view_header_node(selected_tip.hash)
        .ok_or(GraphError::UnknownHeaderNode(selected_tip.hash))?;
    metadata.alarms.header_best_body_unavailable = match &selected_tip_node.body_validation_state {
        BodyValidationState::Unavailable(summary) if summary.alarmed => Some(*summary),
        _ => None,
    };
    let alarm_changed = metadata.alarms != snapshot_before_commit.alarms;
    let changed = !put_nodes.is_empty()
        || !delete_nodes.is_empty()
        || !aux_changes.is_empty()
        || finality_append.is_some()
        || selected_changed
        || verified_changed
        || alarm_changed;
    if changed {
        metadata.state_version = metadata.state_version.checked_next()?;
        if selected_changed
            || header_topology_changed
            || header_validation_changed
            || header_eligibility_changed
            || !eligibility_changes.is_empty()
            || finality_append.is_some()
        {
            metadata.header_generation = metadata.header_generation.checked_next()?;
        }
        if verified_changed || finality_append.is_some() {
            metadata.verified_generation = metadata.verified_generation.checked_next()?;
        }
        if let Some(record) = finality_append {
            metadata.finality_epoch = record.epoch;
        }
        if let Some(fingerprint) = fingerprint {
            metadata.last_transition = Some(fingerprint);
        }
    }
    let verified_tip = *verified.last().ok_or(TransitionFailure::InvalidEvidence(
        InvalidTransitionEvidence::Planner(PlannerCoherenceViolation::EmptyProjection(
            ProjectionKind::Verified,
        )),
    ))?;
    metadata.frontiers = FrontierSet {
        finalized: graph.view_finalized_frontier(),
        header_best: selected_tip,
        verified_best: verified_tip,
    };
    metadata.header_best_score = graph.view_header_chain_score(selected_tip.hash)?;
    metadata.oldest_retained_height = if delete_nodes.is_empty() {
        put_nodes
            .iter()
            .map(|node| node.height)
            .min()
            .map_or(snapshot_before_commit.oldest_retained_height, |height| {
                height.min(snapshot_before_commit.oldest_retained_height)
            })
    } else {
        graph
            .view_header_nodes()
            .into_iter()
            .map(|node| node.height)
            .min()
            .unwrap_or(graph.view_finalized_frontier().height)
    };
    let inserted = put_nodes
        .iter()
        .filter(|node| base_graph.header_node(node.hash).is_none())
        .map(|node| Frontier::new(node.height, node.hash))
        .collect();
    let change_set = ChangeSet {
        put_nodes,
        delete_nodes: delete_nodes.clone(),
        put_consensus_invalid_body_tombstones,
        index_changes: IndexChanges {
            inserted,
            deleted: delete_nodes,
        },
        selected_projection: projection_delta(old_selected, &selected),
        verified_projection: projection_delta(old_verified, &verified),
        eligibility_changes,
        aux_changes,
        finality_append,
        metadata,
    };
    Ok(PlanCandidate {
        snapshot_before_commit,
        change_set,
        graph_delta,
        domain,
        effect,
        trust_pins,
        limits,
    })
}

/// Construct a zero-effect plan after re-checking authority.
pub(super) fn no_change(
    engine: &HeaderChainEngine,
    snapshot_before_commit: EngineSnapshot,
    metadata: EngineMetadata,
    event: TransitionEvent,
    context: &TransitionContext<'_>,
    domain: TransitionDomain,
    effect: TransitionEffect,
) -> Result<PlanCandidate, TransitionFailure> {
    validate_authority(&event, context)?;
    Ok(PlanCandidate {
        snapshot_before_commit,
        change_set: ChangeSet {
            put_nodes: Vec::new(),
            delete_nodes: Vec::new(),
            put_consensus_invalid_body_tombstones: Vec::new(),
            index_changes: IndexChanges::default(),
            selected_projection: ProjectionDelta::default(),
            verified_projection: ProjectionDelta::default(),
            eligibility_changes: Vec::new(),
            aux_changes: Vec::new(),
            finality_append: None,
            metadata,
        },
        graph_delta: GraphDelta::empty(engine.graph()),
        domain,
        effect,
        trust_pins: invariant_pins(context),
        limits: context.config.limits,
    })
}

/// Construct an alarm-only resource-stall plan that discards staged event effects.
pub(super) fn resource_stalled(
    engine: &HeaderChainEngine,
    snapshot_before_commit: EngineSnapshot,
    domain: TransitionDomain,
    context: &TransitionContext<'_>,
) -> Result<PlanCandidate, TransitionFailure> {
    let mut metadata = engine.metadata().clone();
    if !metadata.alarms.resource_stalled {
        metadata.alarms.resource_stalled = true;
        metadata.state_version = metadata.state_version.checked_next()?;
    }
    Ok(PlanCandidate {
        snapshot_before_commit,
        change_set: ChangeSet {
            put_nodes: Vec::new(),
            delete_nodes: Vec::new(),
            put_consensus_invalid_body_tombstones: Vec::new(),
            index_changes: IndexChanges::default(),
            selected_projection: ProjectionDelta::default(),
            verified_projection: ProjectionDelta::default(),
            eligibility_changes: Vec::new(),
            aux_changes: Vec::new(),
            finality_append: None,
            metadata,
        },
        graph_delta: GraphDelta::empty(engine.graph()),
        domain,
        effect: TransitionEffect::resource_stalled(),
        trust_pins: invariant_pins(context),
        limits: context.config.limits,
    })
}

pub(super) fn invariant_pins(context: &TransitionContext<'_>) -> Arc<[Frontier]> {
    context.config.trust_pins()
}

/// Compute a projection delta between consecutive retained frontiers.
pub(super) fn projection_delta(old: &[Frontier], new: &[Frontier]) -> ProjectionDelta {
    let remove_before = new.first().and_then(|first| {
        old.first()
            .is_some_and(|old_first| old_first.height < first.height)
            .then_some(first.height)
    });
    let old_start = remove_before
        .map(|height| old.partition_point(|frontier| frontier.height < height))
        .unwrap_or(0);
    let comparable_old = &old[old_start..];
    let common = comparable_old
        .iter()
        .zip(new)
        .take_while(|(left, right)| left == right)
        .count();
    ProjectionDelta {
        remove_before,
        remove_from: comparable_old
            .get(common)
            .or_else(|| new.get(common))
            .map(|frontier| frontier.height),
        put: new[common..].to_vec(),
    }
}
