//! Bounded commit-time verification of every projected transition invariant.

mod aux_authentication;
mod checkpoint_finality;
mod checks;

#[cfg(any(test, not(feature = "fuzz-impl")))]
use std::collections::HashSet;

use thiserror::Error;
use zakura_chain::block;

use crate::graph::{GraphError, GraphOverlay, HeaderGraphView};
#[cfg(test)]
use crate::EngineTransition;
use crate::{EngineMode, FinalitySource, Frontier, HeaderChainEngine, HeaderNode, PlanCandidate};

use aux_authentication::verify_incremental_aux_authentication;
use checkpoint_finality::verify_incremental_checkpoint_finality;
use checks::{
    projected_path, verify_aux, verify_generations, verify_indexes, verify_node, verify_pins,
    verify_projection, verify_protected, verify_verified,
};

pub(crate) use aux_authentication::is_incremental_aux_authentication;
pub(crate) use checkpoint_finality::is_incremental_checkpoint_finality;

/// Stable, category-specific projected-state invariant failures.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum InvariantViolation {
    /// 1. The verifier found conflicting row-key, canonical-header, and computed hashes.
    #[error("node hash invariant failed at {0:?}")]
    NodeHash(block::Hash),
    /// 2. The verifier found a non-anchor node without an exact height-minus-one parent.
    #[error("parent invariant failed at {0:?}")]
    Parent(block::Hash),
    /// 3. The verifier could not round-trip a hash, parent/child, height, or planned index.
    #[error("index invariant failed at {0:?}")]
    Index(block::Hash),
    /// 4. The verifier found an incorrect work origin or parent-plus-block value.
    #[error("work invariant failed at {0:?}")]
    Work(block::Hash),
    /// 5. The verifier found cached inherited eligibility that differs from exact ancestry.
    #[error("eligibility invariant failed at {0:?}")]
    Eligibility(block::Hash),
    /// 6. The verifier found a gap in the finalized-to-tip selected projection.
    #[error("selected projection invariant failed at {0:?}")]
    SelectedProjection(block::Hash),
    /// 7. The verifier found an eligible score above `header_best`.
    #[error("selection invariant failed")]
    Selection,
    /// 8. The verifier found a verified projection that contradicts its mode or body evidence.
    #[error("verified projection invariant failed at {0:?}")]
    VerifiedProjection(block::Hash),
    /// 9. The verifier found a retained path that conflicts with an authenticated trust pin.
    #[error("trust-pin invariant failed at height {0:?}")]
    TrustPin(block::Height),
    /// 10. The transition evicted finalized, selected, or verified protected state.
    #[error("protected-path invariant failed at {0:?}")]
    Protected(block::Hash),
    /// 11. The projected DAG exceeds a frozen resource limit.
    ///
    /// Distinct from a verified resource stall and from
    /// [`crate::TransitionFailure::AuxiliaryLimitExceeded`]. See [`crate::ApplyResult`].
    #[error("resource-limit invariant failed")]
    Limits,
    /// 12. The verifier found generation increments that disagree with actual changes.
    #[error("generation invariant failed")]
    Generation,
    /// 13. The verifier found auxiliary evidence without a retained foreign key or provenance link.
    #[error("auxiliary invariant failed at {0:?}")]
    Auxiliary(block::Hash),
    /// The coherent snapshot before commit changed or failed during plan verification.
    #[error("snapshot before commit changed during invariant verification")]
    SnapshotBeforeCommit,
}

/// Selects which projected graph and node set the verifier inspects.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum VerificationMode {
    /// Materialized projected graph and every retained node (test/fuzz oracle).
    #[cfg(any(test, feature = "fuzz-impl"))]
    Exhaustive,
    /// Delta overlay and changed-boundary nodes (shipped production path).
    #[cfg(any(test, not(feature = "fuzz-impl")))]
    Production,
}

fn default_verification_mode() -> VerificationMode {
    #[cfg(any(test, feature = "fuzz-impl"))]
    {
        VerificationMode::Exhaustive
    }
    #[cfg(not(any(test, feature = "fuzz-impl")))]
    {
        VerificationMode::Production
    }
}

fn graph_error_violation(error: GraphError, plan: &PlanCandidate) -> InvariantViolation {
    let fallback = plan
        .graph_delta()
        .updated_header_nodes()
        .first()
        .map(|node| node.hash)
        .or_else(|| plan.graph_delta().deleted_header_hashes().first().copied())
        .or_else(|| {
            plan.graph_delta()
                .new_consensus_invalid_body_tombstones()
                .first()
                .map(|tombstone| tombstone.hash)
        })
        .unwrap_or(plan.change_set.metadata.frontiers.finalized.hash);
    let hash = match error {
        GraphError::StaleDelta { .. } => return InvariantViolation::SnapshotBeforeCommit,
        GraphError::AnchorHashMismatch { expected, .. } => expected,
        GraphError::UnknownParent { header, .. } | GraphError::InvalidHeaderNode { header, .. } => {
            header
        }
        GraphError::HeightOverflow { parent } => parent,
        GraphError::ConflictingDuplicate(hash)
        | GraphError::DuplicateHeaderNode(hash)
        | GraphError::UnknownHeaderNode(hash)
        | GraphError::IneligibleFinalizedFrontier(hash)
        | GraphError::HeaderNodeHasChildren(hash)
        | GraphError::PermanentBodyInvalidity(hash) => hash,
        GraphError::FinalizedFrontierNotDescendant { candidate, .. } => candidate,
        GraphError::RevisionExhausted
        | GraphError::InvalidAncestorHeight { .. }
        | GraphError::Work(_) => fallback,
    };
    InvariantViolation::Index(hash)
}

/// Independently check that `plan`'s projection obeys every transition invariant under `engine_before_commit`.
///
/// Pure gate between [`PlanCandidate`] and [`EngineTransition`]: no mutation; success is required
/// before `EngineTransition::from_verified`; failure is [`InvariantViolation`].
pub(crate) fn verify_candidate(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
) -> Result<(), InvariantViolation> {
    verify_plan_with_mode(engine_before_commit, plan, default_verification_mode())
}

fn verify_plan_with_mode(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
    mode: VerificationMode,
) -> Result<(), InvariantViolation> {
    if is_incremental_aux_authentication(engine_before_commit, plan) {
        return verify_incremental_aux_authentication(engine_before_commit, plan, mode);
    }
    if is_incremental_checkpoint_finality(engine_before_commit, plan) {
        return verify_incremental_checkpoint_finality(engine_before_commit, plan, mode);
    }

    verify_plan_exhaustive(engine_before_commit, plan, mode)
}

fn verify_plan_exhaustive(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
    mode: VerificationMode,
) -> Result<(), InvariantViolation> {
    let source = engine_before_commit.snapshot();
    if source != plan.snapshot_before_commit {
        return Err(InvariantViolation::SnapshotBeforeCommit);
    }
    engine_before_commit
        .graph()
        .validate_delta(plan.graph_delta())
        .map_err(|error| graph_error_violation(error, plan))?;
    let source_metadata = engine_before_commit.metadata();
    let delta_graph = GraphOverlay::from_delta(engine_before_commit.graph(), plan.graph_delta())
        .map_err(|error| graph_error_violation(error, plan))?;
    let delta_finalized = delta_graph.view_finalized_frontier();
    #[cfg(any(test, feature = "fuzz-impl"))]
    if mode == VerificationMode::Exhaustive {
        let projected_graph = materialize_projected_graph(engine_before_commit, plan)?;
        return verify_plan_against_graph(
            engine_before_commit,
            plan,
            &source,
            source_metadata,
            &projected_graph,
            delta_finalized,
            mode,
        );
    }
    verify_plan_against_graph(
        engine_before_commit,
        plan,
        &source,
        source_metadata,
        &delta_graph,
        delta_finalized,
        mode,
    )
}

fn verify_plan_against_graph<G: HeaderGraphView>(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
    source: &crate::EngineSnapshot,
    source_metadata: &crate::EngineMetadata,
    graph: &G,
    delta_finalized: Frontier,
    mode: VerificationMode,
) -> Result<(), InvariantViolation> {
    let metadata = &plan.change_set.metadata;
    let expected_work_origin = if plan.graph_delta().rebases_work_coordinates() {
        source.frontiers.finalized
    } else {
        source_metadata.work_origin
    };
    if source_metadata.state_version != source.state_version
        || metadata.mode != source.mode
        || metadata.work_origin != expected_work_origin
        || metadata.headers_only_migration_epoch != source_metadata.headers_only_migration_epoch
    {
        return Err(InvariantViolation::SnapshotBeforeCommit);
    }
    if metadata.frontiers.finalized != graph.view_finalized_frontier()
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
                        .view_header_ancestor(selected_tip.hash, record.current.height)
                        .ok()
                        .flatten()
                        == Some(record.current)
            }
            FinalitySource::MigratedHeadersOnly => false,
        };
        if !valid_source {
            return Err(InvariantViolation::Protected(record.current.hash));
        }
    } else if metadata.finality_epoch != source_metadata.finality_epoch {
        return Err(InvariantViolation::Generation);
    }
    for node in verification_nodes(engine_before_commit, graph, plan, mode) {
        verify_node(graph, node, metadata.work_origin.hash)?;
    }
    verify_indexes(engine_before_commit, plan)?;
    let selected = projected_path(
        engine_before_commit,
        source,
        &plan.change_set.selected_projection,
        true,
    )?;
    let verified = projected_path(
        engine_before_commit,
        source,
        &plan.change_set.verified_projection,
        false,
    )?;
    verify_projection(
        graph,
        &selected,
        metadata.frontiers.header_best,
        InvariantViolation::SelectedProjection,
    )?;
    let best = graph
        .view_select_best_header_chain()
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
    verify_pins(
        &plan.trust_pins,
        &selected,
        &verified,
        &plan.change_set.put_nodes,
    )?;
    verify_protected(graph, plan)?;
    if graph.view_header_node_count().saturating_sub(1) > plan.limits.max_non_finalized_nodes.get()
        || graph.view_eligible_header_tips().len() > plan.limits.max_candidate_tips.get()
    {
        return Err(InvariantViolation::Limits);
    }
    verify_generations(engine_before_commit, plan, &selected, &verified)?;
    verify_aux(engine_before_commit, graph, plan, mode)?;
    Ok(())
}

fn verification_nodes<'a, G: HeaderGraphView>(
    _engine_before_commit: &HeaderChainEngine,
    graph: &'a G,
    _plan: &PlanCandidate,
    mode: VerificationMode,
) -> Vec<&'a HeaderNode> {
    match mode {
        #[cfg(any(test, feature = "fuzz-impl"))]
        VerificationMode::Exhaustive => graph.view_header_nodes(),
        #[cfg(any(test, not(feature = "fuzz-impl")))]
        VerificationMode::Production => changed_boundary_nodes(_engine_before_commit, graph, _plan),
    }
}

#[cfg(any(test, feature = "fuzz-impl"))]
pub(crate) fn materialize_projected_graph(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
) -> Result<crate::graph::MemHeaderStore, InvariantViolation> {
    let mut graph = engine_before_commit.graph().clone();
    graph
        .apply_delta(plan.graph_delta())
        .map_err(|error| graph_error_violation(error, plan))?;
    Ok(graph)
}

#[cfg(any(test, not(feature = "fuzz-impl")))]
fn changed_boundary_nodes<'a, G: HeaderGraphView>(
    engine_before_commit: &HeaderChainEngine,
    graph: &'a G,
    plan: &PlanCandidate,
) -> Vec<&'a HeaderNode> {
    let mut hashes = HashSet::from([
        plan.change_set.metadata.frontiers.finalized.hash,
        plan.change_set.metadata.frontiers.header_best.hash,
        plan.change_set.metadata.frontiers.verified_best.hash,
    ]);
    for node in plan.graph_delta().updated_header_nodes() {
        hashes.insert(node.hash);
        hashes.insert(node.parent_hash);
        hashes.extend(graph.view_header_children(node.hash));
    }
    for hash in plan.graph_delta().deleted_header_hashes() {
        if let Some(node) = engine_before_commit.graph().header_node(*hash) {
            hashes.insert(node.parent_hash);
            hashes.extend(engine_before_commit.graph().header_children(*hash));
        }
    }
    hashes
        .into_iter()
        .filter_map(|hash| graph.view_header_node(hash))
        .collect()
}

/// Re-run verification against an already verified plan in tests and fuzzing.
#[cfg(test)]
pub(crate) fn verify_plan(
    engine_before_commit: &HeaderChainEngine,
    plan: &EngineTransition,
) -> Result<(), InvariantViolation> {
    verify_plan_with_mode(
        engine_before_commit,
        plan.candidate(),
        default_verification_mode(),
    )
}

/// Verify `plan` using the exact production overlay and boundary-node path.
#[cfg(test)]
pub(crate) fn verify_plan_production(
    engine_before_commit: &HeaderChainEngine,
    plan: &EngineTransition,
) -> Result<(), InvariantViolation> {
    verify_plan_with_mode(
        engine_before_commit,
        plan.candidate(),
        VerificationMode::Production,
    )
}
