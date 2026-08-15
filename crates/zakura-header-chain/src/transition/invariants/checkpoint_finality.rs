//! Incremental fast-path for checkpoint finality transitions.

use crate::graph::GraphOverlay;
use crate::graph::HeaderGraphView;
use crate::{
    AuxDelta, BodyValidationState, EngineMode, FinalitySource, Frontier, HeaderChainEngine,
    PlanCandidate, ProjectionDelta,
};

use super::checks::{
    verify_aux, verify_generations, verify_indexes, verify_pins, verify_protected,
};
#[cfg(any(test, feature = "fuzz-impl"))]
use super::materialize_projected_graph;
use super::{InvariantViolation, VerificationMode};

pub(crate) fn is_incremental_checkpoint_finality(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
) -> bool {
    let source = engine_before_commit.snapshot();
    let metadata = &plan.change_set.metadata;
    let Some(record) = plan.change_set.finality_append else {
        return false;
    };
    let finalized = record.current;

    plan.effect().is_checkpoint_finality()
        && source.mode == EngineMode::Integrated
        && matches!(record.source, FinalitySource::FullState { .. })
        && record.previous == source.frontiers.finalized
        && finalized.height > source.frontiers.finalized.height
        && metadata.frontiers.finalized == finalized
        && metadata.frontiers.verified_best == finalized
        && metadata.frontiers.header_best == source.frontiers.header_best
        && engine_before_commit.verified_projection() == [source.frontiers.finalized]
        && engine_before_commit
            .selected_projection()
            .binary_search_by_key(&finalized.height, |frontier| frontier.height)
            .ok()
            .is_some_and(|index| engine_before_commit.selected_projection()[index] == finalized)
        && plan.change_set.selected_projection
            == (ProjectionDelta {
                remove_before: Some(finalized.height),
                remove_from: None,
                put: Vec::new(),
            })
        && plan.change_set.verified_projection
            == (ProjectionDelta {
                remove_before: Some(finalized.height),
                remove_from: Some(finalized.height),
                put: vec![finalized],
            })
        && plan.change_set.put_nodes.len() == 1
        && plan.change_set.put_nodes[0].hash == finalized.hash
        && plan.change_set.eligibility_changes.is_empty()
        && plan
            .change_set
            .aux_changes
            .iter()
            .all(|change| matches!(change, AuxDelta::Delete { .. }))
        && plan.graph_delta().finalized_frontier() == Some(finalized)
}

pub(crate) fn verify_incremental_checkpoint_finality(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
    mode: VerificationMode,
) -> Result<(), InvariantViolation> {
    let source = engine_before_commit.snapshot();
    if source != plan.snapshot_before_commit {
        return Err(InvariantViolation::SnapshotBeforeCommit);
    }
    let delta_graph = GraphOverlay::from_delta(engine_before_commit.graph(), plan.graph_delta())
        .map_err(|error| super::graph_error_violation(error, plan))?;
    let delta_finalized = delta_graph.view_finalized_frontier();
    #[cfg(any(test, feature = "fuzz-impl"))]
    if mode == VerificationMode::Exhaustive {
        let projected_graph = materialize_projected_graph(engine_before_commit, plan)?;
        return verify_incremental_checkpoint_against_graph(
            engine_before_commit,
            plan,
            &source,
            &projected_graph,
            delta_finalized,
            mode,
        );
    }
    verify_incremental_checkpoint_against_graph(
        engine_before_commit,
        plan,
        &source,
        &delta_graph,
        delta_finalized,
        mode,
    )
}

pub(crate) fn verify_incremental_checkpoint_against_graph<G: HeaderGraphView>(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
    source: &crate::EngineSnapshot,
    graph: &G,
    delta_finalized: Frontier,
    mode: VerificationMode,
) -> Result<(), InvariantViolation> {
    let metadata = &plan.change_set.metadata;
    let record = plan
        .change_set
        .finality_append
        .expect("the incremental checkpoint shape requires finality evidence");
    let finalized = record.current;
    let source_metadata = engine_before_commit.metadata();

    if source_metadata.state_version != source.state_version
        || super::immutable_metadata_changed(source_metadata, metadata)
        || metadata.mode != source.mode
        || metadata.work_origin != source_metadata.work_origin
    {
        return Err(InvariantViolation::SnapshotBeforeCommit);
    }
    if metadata.frontiers.finalized != graph.view_finalized_frontier()
        || metadata.frontiers.finalized != delta_finalized
        || record.previous != source.frontiers.finalized
        || record.current != metadata.frontiers.finalized
        || record.epoch != metadata.finality_epoch
        || !matches!(record.source, FinalitySource::FullState { .. })
    {
        return Err(InvariantViolation::Protected(finalized.hash));
    }

    let changed = &plan.change_set.put_nodes[0];
    let previous = engine_before_commit
        .graph()
        .header_node(changed.hash)
        .ok_or(InvariantViolation::Protected(changed.hash))?;
    if changed.hash != previous.hash || changed.header != previous.header {
        return Err(InvariantViolation::NodeHash(changed.hash));
    }
    if changed.height != previous.height || changed.parent_hash != previous.parent_hash {
        return Err(InvariantViolation::Parent(changed.hash));
    }
    if changed.block_work != previous.block_work
        || changed.work_coordinate() != previous.work_coordinate()
    {
        return Err(InvariantViolation::Work(changed.hash));
    }
    if changed.validation != previous.validation
        || changed.eligibility.direct_reasons != previous.eligibility.direct_reasons
        || changed.eligibility.inherited_from.is_some()
    {
        return Err(InvariantViolation::Eligibility(changed.hash));
    }
    if changed.aux_delivery_ids != previous.aux_delivery_ids {
        return Err(InvariantViolation::Auxiliary(changed.hash));
    }
    if !matches!(
        changed.body_validation_state,
        BodyValidationState::Verified { .. }
    ) {
        return Err(InvariantViolation::VerifiedProjection(changed.hash));
    }
    let projected_changed = graph
        .view_header_node(changed.hash)
        .ok_or(InvariantViolation::Protected(changed.hash))?;
    if projected_changed != changed {
        return Err(InvariantViolation::Index(changed.hash));
    }

    verify_indexes(engine_before_commit, plan)?;
    let selected = engine_before_commit.selected_projection();
    let selected_start = selected
        .binary_search_by_key(&finalized.height, |frontier| frontier.height)
        .map_err(|_| InvariantViolation::SelectedProjection(finalized.hash))?;
    for hash in &plan.change_set.delete_nodes {
        let Some(node) = engine_before_commit.graph().header_node(*hash) else {
            return Err(InvariantViolation::Index(*hash));
        };
        if node.height >= finalized.height
            && selected
                .binary_search_by_key(&node.height, |frontier| frontier.height)
                .ok()
                .is_some_and(|index| selected[index].hash == *hash)
        {
            return Err(InvariantViolation::SelectedProjection(*hash));
        }
    }

    let best = graph
        .view_select_best_header_chain()
        .map_err(|_| InvariantViolation::Selection)?;
    if best.0 != metadata.frontiers.header_best || best.1 != metadata.header_best_score {
        return Err(InvariantViolation::Selection);
    }
    verify_pins(
        &plan.trust_pins,
        &selected[selected_start..],
        &[finalized],
        &plan.change_set.put_nodes,
    )?;
    verify_protected(graph, plan)?;
    if graph.view_header_node_count().saturating_sub(1) > plan.limits.max_non_finalized_nodes.get()
        || graph.view_eligible_header_tips().len() > plan.limits.max_candidate_tips.get()
    {
        return Err(InvariantViolation::Limits);
    }
    verify_generations(
        engine_before_commit,
        plan,
        &[metadata.frontiers.header_best],
        &[metadata.frontiers.verified_best],
    )?;
    verify_aux(engine_before_commit, graph, plan, mode)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use zakura_chain::work::difficulty::U256;

    use super::super::test_support::{checkpoint_fixture, hash, projected_graph};
    use super::*;
    use crate::{EvidenceId, HeaderValidationState, WorkCoordinate};

    fn verify_changed_node(
        fixture: &super::super::test_support::Fixture,
        plan: &PlanCandidate,
    ) -> Result<(), InvariantViolation> {
        let graph = projected_graph(&fixture.engine, plan);
        verify_incremental_checkpoint_against_graph(
            &fixture.engine,
            plan,
            &fixture.engine.snapshot(),
            &graph,
            fixture.child,
            VerificationMode::Production,
        )
    }

    #[test]
    fn valid_incremental_checkpoint_matches_production_and_exhaustive_verification() {
        let (fixture, plan) = checkpoint_fixture();
        assert!(is_incremental_checkpoint_finality(&fixture.engine, &plan));
        assert_eq!(
            verify_incremental_checkpoint_finality(
                &fixture.engine,
                &plan,
                VerificationMode::Production,
            ),
            Ok(())
        );
        assert_eq!(
            verify_incremental_checkpoint_finality(
                &fixture.engine,
                &plan,
                VerificationMode::Exhaustive,
            ),
            Ok(())
        );
    }

    #[test]
    fn incremental_checkpoint_classifies_each_changed_node_failure() {
        let (fixture, baseline) = checkpoint_fixture();

        let mut corrupt = baseline.clone();
        let mut other_header = *corrupt.change_set.put_nodes[0].header;
        other_header.nonce.0[0] ^= 1;
        corrupt.change_set.put_nodes[0].header = std::sync::Arc::new(other_header);
        assert_eq!(
            verify_changed_node(&fixture, &corrupt),
            Err(InvariantViolation::NodeHash(fixture.child.hash))
        );

        let mut corrupt = baseline.clone();
        corrupt.change_set.put_nodes[0].height = zakura_chain::block::Height(2);
        assert_eq!(
            verify_changed_node(&fixture, &corrupt),
            Err(InvariantViolation::Parent(fixture.child.hash))
        );

        let mut corrupt = baseline.clone();
        corrupt.change_set.put_nodes[0].work_coordinate =
            WorkCoordinate::new(hash(0x61), U256::zero());
        assert_eq!(
            verify_changed_node(&fixture, &corrupt),
            Err(InvariantViolation::Work(fixture.child.hash))
        );

        let mut corrupt = baseline.clone();
        corrupt.change_set.put_nodes[0].validation =
            HeaderValidationState::DeferredUntil(chrono::Utc::now() + chrono::Duration::seconds(1));
        assert_eq!(
            verify_changed_node(&fixture, &corrupt),
            Err(InvariantViolation::Eligibility(fixture.child.hash))
        );

        let mut corrupt = baseline.clone();
        corrupt.change_set.put_nodes[0]
            .aux_delivery_ids
            .push(EvidenceId::from_digest([0x62; 32]));
        assert_eq!(
            verify_changed_node(&fixture, &corrupt),
            Err(InvariantViolation::Auxiliary(fixture.child.hash))
        );

        let mut corrupt = baseline.clone();
        corrupt.change_set.put_nodes[0].body_validation_state = BodyValidationState::Unknown;
        assert_eq!(
            verify_changed_node(&fixture, &corrupt),
            Err(InvariantViolation::VerifiedProjection(fixture.child.hash))
        );

        let mut corrupt = baseline;
        corrupt.change_set.put_nodes[0].body_validation_state = BodyValidationState::Verified {
            evidence: EvidenceId::from_digest([0x63; 32]),
        };
        assert_eq!(
            verify_changed_node(&fixture, &corrupt),
            Err(InvariantViolation::Index(fixture.child.hash))
        );
    }

    #[test]
    fn incremental_checkpoint_rejects_immutable_metadata_drift() {
        let (fixture, mut plan) = checkpoint_fixture();
        plan.change_set.metadata.anchor_manifest_digest = [0x64; 32];
        assert_eq!(
            verify_incremental_checkpoint_finality(
                &fixture.engine,
                &plan,
                VerificationMode::Production,
            ),
            Err(InvariantViolation::SnapshotBeforeCommit)
        );
        assert_eq!(
            verify_incremental_checkpoint_finality(
                &fixture.engine,
                &plan,
                VerificationMode::Exhaustive,
            ),
            Err(InvariantViolation::SnapshotBeforeCommit)
        );
    }
}
