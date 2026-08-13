//! Selected and verified projection leaf checks.

use crate::graph::HeaderGraphView;
use crate::{BodyValidationState, EngineMode, Frontier, HeaderChainEngine, ProjectionDelta};
use zakura_chain::block;

use super::super::InvariantViolation;

pub(crate) fn projected_path(
    engine_before_commit: &HeaderChainEngine,
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
        engine_before_commit.selected_projection().to_vec()
    } else {
        engine_before_commit.verified_projection().to_vec()
    };
    if path.last().copied() != Some(tip)
        || path.first().copied() != Some(source.frontiers.finalized)
    {
        return Err(InvariantViolation::SnapshotBeforeCommit);
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

pub(crate) fn verify_projection<G: HeaderGraphView>(
    graph: &G,
    projection: &[Frontier],
    tip: Frontier,
    failure: fn(block::Hash) -> InvariantViolation,
) -> Result<(), InvariantViolation> {
    if projection.first().copied() != Some(graph.view_finalized_frontier())
        || projection.last().copied() != Some(tip)
    {
        return Err(failure(tip.hash));
    }
    for pair in projection.windows(2) {
        if pair[1].height.0 != pair[0].height.0 + 1
            || graph
                .view_header_node(pair[1].hash)
                .is_none_or(|node| node.parent_hash != pair[0].hash)
        {
            return Err(failure(pair[1].hash));
        }
    }
    Ok(())
}

pub(crate) fn verify_verified<G: HeaderGraphView>(
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
    if mode == EngineMode::HeadersOnly && projection != [graph.view_finalized_frontier()] {
        return Err(InvariantViolation::VerifiedProjection(tip.hash));
    }
    if mode == EngineMode::Integrated {
        for frontier in projection.iter().skip(1) {
            if !matches!(
                graph
                    .view_header_node(frontier.hash)
                    .map(|node| node.body_validation_state.clone()),
                Some(BodyValidationState::Verified { .. })
            ) {
                return Err(InvariantViolation::VerifiedProjection(frontier.hash));
            }
        }
    }
    Ok(())
}
