//! Write-set index coherence leaf checks.

use std::collections::HashSet;

use zakura_chain::block;

use crate::{Frontier, HeaderChainEngine, PlanCandidate};

use super::super::InvariantViolation;

fn first_mismatch_hash<T: PartialEq>(
    left: &[T],
    right: &[T],
    hash: impl Fn(&T) -> block::Hash,
) -> block::Hash {
    left.iter()
        .zip(right)
        .find_map(|(left, right)| (left != right).then(|| hash(left)))
        .or_else(|| left.get(right.len()).map(&hash))
        .or_else(|| right.get(left.len()).map(hash))
        .expect("unequal write-set sequences contain a mismatched entry")
}

pub(crate) fn verify_indexes(
    engine_before_commit: &HeaderChainEngine,
    plan: &PlanCandidate,
) -> Result<(), InvariantViolation> {
    if plan.change_set.put_nodes != plan.graph_delta().updated_header_nodes() {
        return Err(InvariantViolation::Index(first_mismatch_hash(
            &plan.change_set.put_nodes,
            plan.graph_delta().updated_header_nodes(),
            |node| node.hash,
        )));
    }
    if plan.change_set.delete_nodes != plan.graph_delta().deleted_header_hashes() {
        return Err(InvariantViolation::Index(first_mismatch_hash(
            &plan.change_set.delete_nodes,
            plan.graph_delta().deleted_header_hashes(),
            |hash| *hash,
        )));
    }
    if plan.change_set.put_consensus_invalid_body_tombstones
        != plan.graph_delta().new_consensus_invalid_body_tombstones()
    {
        return Err(InvariantViolation::Index(first_mismatch_hash(
            &plan.change_set.put_consensus_invalid_body_tombstones,
            plan.graph_delta().new_consensus_invalid_body_tombstones(),
            |tombstone| tombstone.hash,
        )));
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
                .min_by_key(|frontier| frontier.hash.0)
                .expect("unequal inserted-index sets contain a differing frontier")
                .hash,
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
                .min_by_key(|hash| hash.0)
                .copied()
                .expect("unequal deleted-index sets contain a differing hash"),
        ));
    }
    Ok(())
}
