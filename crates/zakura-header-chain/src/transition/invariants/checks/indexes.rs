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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::block::genesis::regtest_genesis_block;

    use super::super::super::test_support::{candidate_with_delta, fixture, hash};
    use super::*;
    use crate::graph::GraphOverlay;
    use crate::{BodyValidationState, EngineMode, EvidenceId, HeaderValidationState};

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Entry {
        value: u8,
        hash: block::Hash,
    }

    #[test]
    fn first_mismatch_hash_uses_left_replacement_then_the_first_extra_entry() {
        let left = [
            Entry {
                value: 1,
                hash: hash(0x11),
            },
            Entry {
                value: 2,
                hash: hash(0x12),
            },
        ];
        let replacement = [
            left[0].clone(),
            Entry {
                value: 3,
                hash: hash(0x13),
            },
        ];
        assert_eq!(
            first_mismatch_hash(&left, &replacement, |entry| entry.hash),
            left[1].hash
        );
        assert_eq!(
            first_mismatch_hash(&left, &left[..1], |entry| entry.hash),
            left[1].hash
        );
        assert_eq!(
            first_mismatch_hash(&left[..1], &left, |entry| entry.hash),
            left[1].hash
        );
    }

    #[test]
    fn write_set_sequence_mismatch_reports_the_first_left_hash() {
        let fixture = fixture(EngineMode::HeadersOnly);
        let mut overlay = GraphOverlay::new(fixture.engine.graph());
        overlay
            .set_body_validation_state(
                fixture.child.hash,
                BodyValidationState::Verified {
                    evidence: EvidenceId::from_digest([0x21; 32]),
                },
            )
            .expect("the fixture child accepts a body-state update");
        let mut plan = candidate_with_delta(&fixture.engine, overlay.delta());
        plan.change_set.put_nodes[0].body_validation_state = BodyValidationState::Unknown;

        assert_eq!(
            verify_indexes(&fixture.engine, &plan),
            Err(InvariantViolation::Index(fixture.child.hash))
        );
    }

    #[test]
    fn index_set_mismatches_choose_the_smallest_raw_hash() {
        let fixture = fixture(EngineMode::HeadersOnly);
        let mut inserted_overlay = GraphOverlay::new(fixture.engine.graph());
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = fixture.child.hash;
        header.nonce.0[0] = 0x31;
        inserted_overlay
            .insert(
                Arc::new(header),
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the fixture grandchild is inserted");
        let mut inserted = candidate_with_delta(&fixture.engine, inserted_overlay.delta());
        let smallest = hash(0);
        inserted.change_set.index_changes.inserted =
            vec![Frontier::new(block::Height(9), smallest)];
        assert_eq!(
            verify_indexes(&fixture.engine, &inserted),
            Err(InvariantViolation::Index(smallest))
        );

        let mut deleted_overlay = GraphOverlay::new(fixture.engine.graph());
        deleted_overlay
            .remove_header_leaf(fixture.child.hash)
            .expect("the fixture child is a removable leaf");
        let mut deleted = candidate_with_delta(&fixture.engine, deleted_overlay.delta());
        deleted.change_set.index_changes.deleted = vec![smallest];
        assert_eq!(
            verify_indexes(&fixture.engine, &deleted),
            Err(InvariantViolation::Index(smallest))
        );
    }
}
