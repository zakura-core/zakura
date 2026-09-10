//! Deterministic reconstruction of derived indexes and projections.

use std::collections::HashMap;

use zakura_chain::block;

use crate::{
    BodyValidationState, EngineConfig, EngineMetadata, EngineMode, Frontier, HeaderNode,
    MemHeaderStore,
};

use super::contracts::{source_failure, AuditViolation, RecoveryFailure};
use super::phases::{AuditedSource, ReconstructedDerivedViews};

/// Reconstruct every derived view without changing authoritative node state.
pub(super) fn reconstruct_derived_views(
    audited: &AuditedSource,
    config: &EngineConfig,
) -> Result<ReconstructedDerivedViews, RecoveryFailure> {
    let finalized = audited.metadata.frontiers.finalized;
    let mut graph = MemHeaderStore::reconstruct(crate::HeaderGraphReconstruction::new(
        finalized,
        audited.source_header_nodes.clone(),
        audited.consensus_invalid_body_tombstones.clone(),
    ))
    .map_err(|_| RecoveryFailure::Source {
        violations: vec![AuditViolation::ProtectedPath(finalized.hash)],
    })?;
    graph
        .recompute_all_header_eligibility()
        .map_err(|_| RecoveryFailure::Source {
            violations: vec![AuditViolation::ProtectedPath(finalized.hash)],
        })?;
    let mut header_nodes: Vec<_> = graph.header_nodes().cloned().collect();
    header_nodes.sort_unstable_by_key(|header_node| (header_node.height, header_node.hash.0));
    let header_nodes_by_hash: HashMap<_, _> = header_nodes
        .iter()
        .map(|header_node| (header_node.hash, header_node.clone()))
        .collect();

    let mut header_child_edges: Vec<_> = header_nodes
        .iter()
        .filter(|header_node| header_node.hash != finalized.hash)
        .map(|header_node| (header_node.parent_hash, header_node.hash))
        .collect();
    header_child_edges.sort_unstable_by_key(|(parent, child)| (parent.0, child.0));
    let mut deferred_entries: Vec<_> = header_nodes
        .iter()
        .filter_map(|header_node| match header_node.validation {
            crate::HeaderValidationState::Valid => None,
            crate::HeaderValidationState::DeferredUntil(until) => Some((until, header_node.hash)),
        })
        .collect();
    deferred_entries.sort_unstable_by_key(|(until, hash)| (*until, hash.0));
    if graph.eligible_header_tips().len() > config.limits.max_candidate_tips.get() {
        return Err(source_failure(AuditViolation::Limits));
    }
    let (selected_tip, selected_score) = graph
        .select_best_header_chain()
        .map_err(|_| source_failure(AuditViolation::ProtectedPath(finalized.hash)))?;
    if config.mode == EngineMode::HeadersOnly
        && selected_tip.height.0.saturating_sub(finalized.height.0)
            > config.limits.local_finality_depth.get()
    {
        return Err(source_failure(AuditViolation::Finality));
    }
    let selected_projection = path_to(&header_nodes_by_hash, finalized, selected_tip)?;
    let verified_projection = verified_path(&header_nodes_by_hash, &audited.metadata)?;
    let oldest_retained_height = header_nodes
        .iter()
        .map(|header_node| header_node.height)
        .min()
        .unwrap_or(finalized.height);
    let body_unavailable_alarm = match &header_nodes_by_hash
        .get(&selected_tip.hash)
        .ok_or_else(|| source_failure(AuditViolation::ProtectedPath(selected_tip.hash)))?
        .body_validation_state
    {
        crate::BodyValidationState::Unavailable(summary) if summary.alarmed => Some(*summary),
        _ => None,
    };
    Ok(ReconstructedDerivedViews {
        source_nodes: audited.source_header_nodes.clone(),
        header_nodes,
        header_child_edges,
        selected_projection,
        verified_projection,
        deferred_entries,
        selected_tip,
        selected_score,
        oldest_retained_height,
        body_unavailable_alarm,
    })
}

fn verified_path(
    nodes: &HashMap<block::Hash, HeaderNode>,
    metadata: &EngineMetadata,
) -> Result<Vec<Frontier>, RecoveryFailure> {
    if metadata.mode == EngineMode::HeadersOnly {
        if metadata.frontiers.verified_best != metadata.frontiers.finalized {
            return Err(source_failure(AuditViolation::ProtectedPath(
                metadata.frontiers.verified_best.hash,
            )));
        }
        return Ok(vec![metadata.frontiers.finalized]);
    }
    let path = path_to(
        nodes,
        metadata.frontiers.finalized,
        metadata.frontiers.verified_best,
    )?;
    if path.iter().skip(1).any(|frontier| {
        nodes.get(&frontier.hash).is_none_or(|node| {
            !node.is_eligible()
                || !matches!(
                    node.body_validation_state,
                    BodyValidationState::Verified { .. }
                )
        })
    }) {
        return Err(source_failure(AuditViolation::ProtectedPath(
            metadata.frontiers.verified_best.hash,
        )));
    }
    Ok(path)
}

fn path_to(
    nodes: &HashMap<block::Hash, HeaderNode>,
    finalized: Frontier,
    tip: Frontier,
) -> Result<Vec<Frontier>, RecoveryFailure> {
    let mut current = tip;
    let mut path = Vec::new();
    loop {
        let node = nodes
            .get(&current.hash)
            .filter(|node| node.height == current.height)
            .ok_or_else(|| source_failure(AuditViolation::ProtectedPath(current.hash)))?;
        path.push(current);
        if current == finalized {
            break;
        }
        current = Frontier::new(
            current
                .height
                .previous()
                .map_err(|_| source_failure(AuditViolation::ProtectedPath(current.hash)))?,
            node.parent_hash,
        );
    }
    path.reverse();
    Ok(path)
}
