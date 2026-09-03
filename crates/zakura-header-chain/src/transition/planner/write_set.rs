//! Generation policy and durable write-set derivation.

use std::{borrow::Cow, sync::Arc};

use crate::graph::{GraphDelta, HeaderGraphView};
use crate::{
    BodyValidationState, BodyWorkEffect, ChangeSet, EligibilityDelta, EngineLimits, EngineMetadata,
    EngineSnapshot, Frontier, FrontierSet, GraphError, HeaderChainEngine, IndexChanges,
    MemHeaderStore, ProjectionDelta, TransitionContext, TransitionDomain, TransitionEffect,
    TransitionEvent, TransitionFingerprint,
};

use super::admission::validate_authority;
use super::projected_state::SettledProjectedState;
use super::retention::RetentionPlan;
use super::settlement::FinalityLineage;
use super::{
    InvalidTransitionEvidence, PlanCandidate, PlannerCoherenceViolation, ProjectionKind,
    TransitionFailure,
};

/// Inputs required to derive the atomic write set from settled projections.
pub(super) struct DerivePlanInputs<'a> {
    pub(super) transition_source: crate::transition::engine::EngineTransitionSource,
    pub(super) snapshot_before_commit: EngineSnapshot,
    pub(super) metadata: EngineMetadata,
    pub(super) base_graph: &'a MemHeaderStore,
    pub(super) projected: SettledProjectedState<'a>,
    pub(super) old_selected: &'a [Frontier],
    pub(super) old_verified: &'a [Frontier],
    pub(super) selected: Cow<'a, [Frontier]>,
    pub(super) finality_append: Option<crate::FinalityRecord>,
    pub(super) finality_lineage: FinalityLineage,
    pub(super) finality_ancestry: crate::FinalityWitnessProof,
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
        transition_source,
        snapshot_before_commit,
        mut metadata,
        base_graph,
        projected,
        old_selected,
        old_verified,
        selected,
        finality_append,
        finality_lineage,
        finality_ancestry,
        retention,
        fingerprint,
        domain,
        mut effect,
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
    effect.body_work = body_work_effect(
        old_selected,
        &selected,
        old_verified,
        &verified,
        finality_append,
        finality_lineage,
    );
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
        finality_ancestry,
        metadata,
    };
    Ok(PlanCandidate {
        transition_source,
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
        transition_source: engine.transition_source(),
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
            finality_ancestry: crate::FinalityWitnessProof::default(),
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
        transition_source: engine.transition_source(),
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
            finality_ancestry: crate::FinalityWitnessProof::default(),
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

/// Classify whether final selected and verified projections retain prior body work.
pub(super) fn body_work_effect(
    old_selected: &[Frontier],
    new_selected: &[Frontier],
    old_verified: &[Frontier],
    new_verified: &[Frontier],
    finality_append: Option<crate::FinalityRecord>,
    finality_lineage: FinalityLineage,
) -> BodyWorkEffect {
    if projection_retains_path(
        old_selected,
        new_selected,
        finality_append,
        finality_lineage.continues_selected,
    ) && projection_retains_path(
        old_verified,
        new_verified,
        finality_append,
        finality_lineage.continues_verified,
    ) {
        BodyWorkEffect::Preserved
    } else {
        BodyWorkEffect::Invalidated
    }
}

fn projection_retains_path(
    old: &[Frontier],
    new: &[Frontier],
    finality_append: Option<crate::FinalityRecord>,
    finality_continues_old: bool,
) -> bool {
    let Some(record) = finality_append else {
        return new.starts_with(old);
    };
    let retained_old =
        &old[old.partition_point(|frontier| frontier.height < record.current.height)..];
    if retained_old.is_empty() {
        // The finality append trims away every prior entry, so `starts_with` compares
        // against an empty prefix and holds for any new path, including one on an
        // unrelated branch. Only the captured ancestry distinguishes a lineage the
        // finalized frontier continues from one it replaces.
        return finality_continues_old;
    }
    new.starts_with(retained_old)
}

#[cfg(test)]
mod body_work_epoch_tests {
    use zakura_chain::block;

    use super::*;

    fn path(markers: &[u8]) -> Vec<Frontier> {
        markers
            .iter()
            .enumerate()
            .map(|(height, marker)| {
                Frontier::new(
                    block::Height(u32::try_from(height).expect("the fixture height fits in u32")),
                    block::Hash([*marker; 32]),
                )
            })
            .collect()
    }

    fn frontier(height: u32, marker: u8) -> Frontier {
        Frontier::new(block::Height(height), block::Hash([marker; 32]))
    }

    fn finality(current: Frontier) -> crate::FinalityRecord {
        crate::FinalityRecord {
            previous: frontier(0, 1),
            current,
            source: crate::FinalitySource::MigratedHeadersOnly,
            epoch: crate::FinalityEpoch::new(1),
        }
    }

    /// The finalized frontier descends from both prior projection tips.
    fn continues() -> FinalityLineage {
        FinalityLineage {
            continues_selected: true,
            continues_verified: true,
        }
    }

    /// The finalized frontier descends from neither prior projection tip.
    fn replaces() -> FinalityLineage {
        FinalityLineage::default()
    }

    #[test]
    fn selected_extension_and_finalized_trim_preserve_body_work_epoch() {
        let old = path(&[1, 2, 3]);
        let extended = path(&[1, 2, 3, 4]);
        assert_eq!(
            body_work_effect(&old, &extended, &old, &old, None, replaces()),
            BodyWorkEffect::Preserved
        );
        // The trim retains a prefix of both projections, so that prefix carries the
        // continuation evidence and the classifier never consults the ancestry.
        assert_eq!(
            body_work_effect(
                &old,
                &extended[1..],
                &old,
                &old[1..],
                Some(finality(extended[1])),
                replaces(),
            ),
            BodyWorkEffect::Preserved
        );
    }

    #[test]
    fn side_branch_changes_preserve_body_work_epoch() {
        let selected = path(&[1, 2, 3]);
        assert_eq!(
            body_work_effect(&selected, &selected, &selected, &selected, None, replaces()),
            BodyWorkEffect::Preserved
        );
    }

    #[test]
    fn selected_replacement_and_retreat_advance_body_work_epoch() {
        let old = path(&[1, 2, 3]);
        for replacement in [
            path(&[1, 9, 8]),
            path(&[1, 9, 8, 7]),
            vec![Frontier::new(block::Height(8), block::Hash([8; 32]))],
            path(&[1, 2]),
        ] {
            assert_eq!(
                body_work_effect(&old, &replacement, &old, &old, None, replaces()),
                BodyWorkEffect::Invalidated
            );
        }
    }

    #[test]
    fn verified_growth_preserves_and_verified_reset_advances_body_work_epoch() {
        let selected = path(&[1, 2, 3, 4]);
        let old_verified = path(&[1, 2]);
        let grown_verified = path(&[1, 2, 3]);
        assert_eq!(
            body_work_effect(
                &selected,
                &selected,
                &old_verified,
                &grown_verified,
                None,
                replaces(),
            ),
            BodyWorkEffect::Preserved
        );
        assert_eq!(
            body_work_effect(
                &selected,
                &selected,
                &old_verified,
                &path(&[1, 9]),
                None,
                replaces(),
            ),
            BodyWorkEffect::Invalidated
        );
    }

    #[test]
    fn invalid_body_advances_epoch_only_when_selection_changes() {
        let selected = path(&[1, 2, 3]);
        assert_eq!(
            body_work_effect(
                &selected,
                &path(&[1, 8, 9]),
                &selected,
                &selected,
                None,
                replaces(),
            ),
            BodyWorkEffect::Invalidated
        );
        assert_eq!(
            body_work_effect(&selected, &selected, &selected, &selected, None, replaces()),
            BodyWorkEffect::Preserved
        );
    }

    #[test]
    fn authoritative_finality_retires_a_complete_old_projection_it_continues() {
        let old = vec![frontier(4, 4), frontier(5, 5)];
        let new = vec![frontier(8, 8), frontier(9, 9)];
        let record = finality(new[0]);

        assert_eq!(
            body_work_effect(&old, &new, &old, &new, Some(record), continues()),
            BodyWorkEffect::Preserved
        );
    }

    #[test]
    fn finality_above_the_old_tip_invalidates_a_replaced_lineage() {
        let old = vec![frontier(4, 4), frontier(5, 5)];
        let unrelated = vec![frontier(8, 0x88), frontier(9, 0x99)];
        let record = finality(unrelated[0]);

        // The trim empties both retained projections, so the paths alone cannot tell a
        // continuation from a replacement. Without ancestry back to the old tips the
        // classifier must retire the epoch.
        assert_eq!(
            body_work_effect(&old, &unrelated, &old, &unrelated, Some(record), replaces()),
            BodyWorkEffect::Invalidated
        );
    }

    #[test]
    fn finality_invalidates_when_it_continues_only_one_projection() {
        let old = vec![frontier(4, 4), frontier(5, 5)];
        let new = vec![frontier(8, 8), frontier(9, 9)];
        let record = finality(new[0]);

        for lineage in [
            FinalityLineage {
                continues_selected: true,
                continues_verified: false,
            },
            FinalityLineage {
                continues_selected: false,
                continues_verified: true,
            },
        ] {
            assert_eq!(
                body_work_effect(&old, &new, &old, &new, Some(record), lineage),
                BodyWorkEffect::Invalidated
            );
        }
    }

    #[test]
    fn authoritative_finality_preserves_the_retained_projection_prefix() {
        let old = vec![frontier(4, 4), frontier(5, 5), frontier(6, 6)];
        let new = vec![frontier(5, 5), frontier(6, 6), frontier(7, 7)];
        let record = finality(new[0]);

        assert_eq!(
            body_work_effect(&old, &new, &old, &new, Some(record), replaces()),
            BodyWorkEffect::Preserved
        );
    }

    #[test]
    fn finality_does_not_authorize_a_conflicting_reanchor() {
        let old = vec![frontier(4, 4), frontier(5, 5), frontier(6, 6)];
        let reanchored = vec![frontier(5, 0x55), frontier(6, 0x66)];
        let record = finality(reanchored[0]);

        assert_eq!(
            body_work_effect(
                &old,
                &reanchored,
                &old,
                &reanchored,
                Some(record),
                continues()
            ),
            BodyWorkEffect::Invalidated
        );
    }

    #[test]
    fn projection_retirement_without_finality_invalidates_body_work() {
        let old = vec![frontier(4, 4), frontier(5, 5), frontier(6, 6)];
        let arbitrary_suffix = &old[1..];

        assert_eq!(
            body_work_effect(
                &old,
                arbitrary_suffix,
                &old,
                arbitrary_suffix,
                None,
                continues()
            ),
            BodyWorkEffect::Invalidated
        );
    }
}
