//! Write-set index coherence leaf checks.

use std::collections::HashSet;

use zakura_chain::block;

use crate::{Frontier, HeaderChainEngine, PlanCandidate};

use super::super::InvariantViolation;

pub(crate) fn verify_indexes(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
) -> Result<(), InvariantViolation> {
    if plan.change_set.put_nodes != plan.graph_delta().updated_header_nodes()
        || plan.change_set.delete_nodes != plan.graph_delta().deleted_header_hashes()
        || plan.change_set.put_consensus_invalid_body_tombstones
            != plan.graph_delta().new_consensus_invalid_body_tombstones()
    {
        return Err(InvariantViolation::Index(block::Hash([0; 32])));
    }
    let mut inserted = HashSet::new();
    for node in &plan.change_set.put_nodes {
        if engine_before_commit
            .graph()
            .header_node(node.hash)
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
