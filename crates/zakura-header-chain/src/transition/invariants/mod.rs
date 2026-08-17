//! Bounded commit-time verification of every projected transition invariant.

mod aux_authentication;
mod checkpoint_finality;
mod checks;
#[cfg(test)]
mod test_support;

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
        GraphError::UnknownParent { header, .. }
        | GraphError::InvalidHeaderNode { header, .. }
        | GraphError::DirectEligibilityReasonLimit { header, .. } => header,
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

fn immutable_metadata_changed(
    source: &crate::EngineMetadata,
    projected: &crate::EngineMetadata,
) -> bool {
    projected.disk_format != source.disk_format
        || projected.network_id != source.network_id
        || projected.anchor_manifest_digest != source.anchor_manifest_digest
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
        || immutable_metadata_changed(source_metadata, metadata)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::{block::genesis::regtest_genesis_block, parameters::Network};

    use super::test_support::{candidate_with_delta, fixture, hash, no_change_candidate};
    use super::*;
    use crate::graph::GraphOverlay;
    use crate::{
        BodyRuleId, BodyValidationState, EligibilityReason, EngineMode, EvidenceId,
        HeaderNodeInvariant, WorkCoordinateError,
    };

    fn stage_tombstone_only(
        overlay: &mut GraphOverlay<'_>,
        parent_hash: block::Hash,
        nonce: u8,
    ) -> block::Hash {
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = parent_hash;
        header.nonce.0[0] = nonce;
        let header = Arc::new(header);
        let tombstone_hash = header.hash();
        overlay
            .insert(
                header,
                crate::HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the temporary tombstoned child is inserted");
        overlay
            .set_body_validation_state(
                tombstone_hash,
                BodyValidationState::ConsensusInvalid {
                    evidence: EvidenceId::from_digest([0x33; 32]),
                    rule: BodyRuleId::new("test.graph-error-fallback"),
                },
            )
            .expect("the temporary child accepts a permanent tombstone");
        overlay
            .remove_header_leaf(tombstone_hash)
            .expect("removing a new child leaves only its append-only tombstone");
        tombstone_hash
    }

    #[test]
    fn graph_errors_map_to_the_exact_subject() {
        let fixture = fixture(EngineMode::HeadersOnly);
        let plan = no_change_candidate(&fixture.engine);
        let expected = hash(0x11);
        let actual = hash(0x12);
        let header = hash(0x13);
        let parent = hash(0x14);
        let candidate = hash(0x15);
        let cases = [
            (
                GraphError::AnchorHashMismatch { expected, actual },
                InvariantViolation::Index(expected),
            ),
            (
                GraphError::UnknownParent { header, parent },
                InvariantViolation::Index(header),
            ),
            (
                GraphError::InvalidHeaderNode {
                    header,
                    invariant: HeaderNodeInvariant::CanonicalHeaderHash,
                },
                InvariantViolation::Index(header),
            ),
            (
                GraphError::HeightOverflow { parent },
                InvariantViolation::Index(parent),
            ),
            (
                GraphError::ConflictingDuplicate(hash(0x20)),
                InvariantViolation::Index(hash(0x20)),
            ),
            (
                GraphError::DuplicateHeaderNode(hash(0x21)),
                InvariantViolation::Index(hash(0x21)),
            ),
            (
                GraphError::UnknownHeaderNode(hash(0x22)),
                InvariantViolation::Index(hash(0x22)),
            ),
            (
                GraphError::IneligibleFinalizedFrontier(hash(0x23)),
                InvariantViolation::Index(hash(0x23)),
            ),
            (
                GraphError::HeaderNodeHasChildren(hash(0x24)),
                InvariantViolation::Index(hash(0x24)),
            ),
            (
                GraphError::PermanentBodyInvalidity(hash(0x25)),
                InvariantViolation::Index(hash(0x25)),
            ),
            (
                GraphError::FinalizedFrontierNotDescendant {
                    current: hash(0x26),
                    candidate,
                },
                InvariantViolation::Index(candidate),
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(graph_error_violation(error, &plan), expected);
        }
        assert_eq!(
            graph_error_violation(
                GraphError::StaleDelta {
                    current_revision: crate::GraphRevision::default(),
                    delta_base_revision: crate::GraphRevision::default(),
                },
                &plan,
            ),
            InvariantViolation::SnapshotBeforeCommit
        );
    }

    #[test]
    fn graph_error_fallback_prefers_updated_deleted_tombstone_then_finalized() {
        let fixture = fixture(EngineMode::HeadersOnly);
        let mut graph = fixture.engine.graph().clone();
        let mut sibling_header = *regtest_genesis_block().header;
        sibling_header.previous_block_hash = fixture.anchor.hash;
        sibling_header.nonce.0[0] = 0x30;
        let sibling = match graph
            .insert(
                Arc::new(sibling_header),
                crate::HeaderValidationState::Valid,
                [EligibilityReason::FinalityConflict {
                    finalized: fixture.anchor,
                }],
                BodyValidationState::Unknown,
            )
            .expect("the ineligible sibling is retained without changing fork choice")
        {
            crate::InsertResult::Inserted(frontier)
            | crate::InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let engine = HeaderChainEngine::from_audited_state(
            graph,
            fixture.engine.metadata().clone(),
            fixture.engine.selected_projection().to_vec(),
            fixture.engine.verified_projection().to_vec(),
            [],
        )
        .expect("the fallback fixture remains coherent with an ineligible sibling");

        let mut updated_overlay = GraphOverlay::new(engine.graph());
        updated_overlay
            .set_body_validation_state(
                fixture.child.hash,
                BodyValidationState::Verified {
                    evidence: EvidenceId::from_digest([0x31; 32]),
                },
            )
            .expect("the fixture child accepts a body-state update");
        updated_overlay
            .remove_header_leaf(sibling.hash)
            .expect("the ineligible sibling is a removable leaf");
        let tombstone_hash = stage_tombstone_only(&mut updated_overlay, fixture.child.hash, 0x32);
        let updated = candidate_with_delta(&engine, updated_overlay.delta());

        let mut deleted_overlay = GraphOverlay::new(engine.graph());
        deleted_overlay
            .remove_header_leaf(sibling.hash)
            .expect("the ineligible sibling is a removable leaf");
        stage_tombstone_only(&mut deleted_overlay, fixture.child.hash, 0x32);
        let deleted = candidate_with_delta(&engine, deleted_overlay.delta());

        let mut tombstone_overlay = GraphOverlay::new(engine.graph());
        stage_tombstone_only(&mut tombstone_overlay, fixture.child.hash, 0x32);
        let tombstone = candidate_with_delta(&engine, tombstone_overlay.delta());

        let fallback_error = || GraphError::Work(WorkCoordinateError::Overflow);
        assert_eq!(
            graph_error_violation(fallback_error(), &updated),
            InvariantViolation::Index(fixture.child.hash)
        );
        assert_eq!(
            graph_error_violation(fallback_error(), &deleted),
            InvariantViolation::Index(sibling.hash)
        );
        assert_eq!(
            graph_error_violation(fallback_error(), &tombstone),
            InvariantViolation::Index(tombstone_hash)
        );
        assert_eq!(
            graph_error_violation(fallback_error(), &no_change_candidate(&fixture.engine)),
            InvariantViolation::Index(fixture.anchor.hash)
        );
    }

    #[test]
    fn every_fallback_only_graph_error_uses_the_same_subject() {
        let fixture = fixture(EngineMode::HeadersOnly);
        let plan = no_change_candidate(&fixture.engine);
        for error in [
            GraphError::RevisionExhausted,
            GraphError::InvalidAncestorHeight {
                ancestor: block::Height(2),
                descendant: block::Height(1),
            },
            GraphError::Work(WorkCoordinateError::Underflow),
        ] {
            assert_eq!(
                graph_error_violation(error, &plan),
                InvariantViolation::Index(fixture.anchor.hash)
            );
        }
    }

    #[test]
    fn immutable_metadata_drift_fails_closed_in_both_verifiers() {
        let fixture = fixture(EngineMode::HeadersOnly);
        let mut cases = Vec::new();

        let mut disk_format = no_change_candidate(&fixture.engine);
        disk_format.change_set.metadata.disk_format =
            crate::HeaderChainDiskVersion(crate::HeaderChainDiskVersion::CURRENT.0 + 1);
        cases.push(disk_format);

        let mut network = no_change_candidate(&fixture.engine);
        network.change_set.metadata.network_id = Network::Mainnet.kind();
        cases.push(network);

        let mut manifest = no_change_candidate(&fixture.engine);
        manifest.change_set.metadata.anchor_manifest_digest = [0xff; 32];
        cases.push(manifest);

        for plan in cases {
            for mode in [VerificationMode::Production, VerificationMode::Exhaustive] {
                assert_eq!(
                    verify_plan_with_mode(&fixture.engine, &plan, mode),
                    Err(InvariantViolation::SnapshotBeforeCommit)
                );
            }
        }
    }

    #[test]
    fn production_and_exhaustive_paths_agree_on_valid_and_corrupt_candidates() {
        let fixture = fixture(EngineMode::HeadersOnly);
        let valid = no_change_candidate(&fixture.engine);
        assert_eq!(
            verify_plan_with_mode(&fixture.engine, &valid, VerificationMode::Production),
            verify_plan_with_mode(&fixture.engine, &valid, VerificationMode::Exhaustive)
        );
        assert_eq!(
            verify_plan_with_mode(&fixture.engine, &valid, VerificationMode::Exhaustive),
            Ok(())
        );

        let mut corrupt = valid;
        corrupt.change_set.metadata.anchor_manifest_digest = [0xfe; 32];
        assert_eq!(
            verify_plan_with_mode(&fixture.engine, &corrupt, VerificationMode::Production),
            verify_plan_with_mode(&fixture.engine, &corrupt, VerificationMode::Exhaustive)
        );
    }
}
