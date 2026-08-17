//! Retention behavior and invariant coverage.

use std::{num::NonZeroUsize, sync::Arc};

use super::*;
use crate::{
    BodyValidationState, EligibilityReason, HeaderValidationState, InsertResult, MemHeaderStore,
};
use zakura_chain::block::genesis::regtest_genesis_block;

fn retention_graph() -> MemHeaderStore {
    let block = regtest_genesis_block();
    let hash = block.hash();
    let work = block
        .header
        .difficulty_threshold
        .to_work()
        .expect("valid work");
    MemHeaderStore::new(
        Frontier::new(block::Height(0), hash),
        block.header.clone(),
        work,
        work.as_u256(),
    )
    .expect("fixture anchor matches")
}

fn insert_header(
    store: &mut MemHeaderStore,
    parent: block::Hash,
    seed: u8,
    reasons: impl IntoIterator<Item = EligibilityReason>,
) -> Frontier {
    let mut header = *regtest_genesis_block().header;
    header.previous_block_hash = parent;
    header.nonce = [seed; 32].into();
    let header = Arc::new(header);
    match store
        .insert(
            header,
            HeaderValidationState::Valid,
            reasons,
            BodyValidationState::Unknown,
        )
        .expect("fixture parent is retained")
    {
        InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
    }
}

fn limits(tips: usize, nodes: usize) -> EngineLimits {
    EngineLimits {
        max_candidate_tips: NonZeroUsize::new(tips).expect("test limit is nonzero"),
        max_non_finalized_nodes: NonZeroUsize::new(nodes).expect("test limit is nonzero"),
        ..EngineLimits::v1()
    }
}

#[test]
fn candidate_tip_eviction_is_lowest_work_then_smallest_raw_hash() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let tips: Vec<_> = (1..=12)
        .map(|seed| insert_header(&mut store, anchor.hash, seed, []))
        .collect();
    let header_best = store
        .select_best_header_chain()
        .expect("graph is coherent")
        .0;
    let mut expected: Vec<_> = tips
        .iter()
        .copied()
        .filter(|tip| *tip != header_best)
        .collect();
    expected.sort_unstable_by_key(|tip| {
        store
            .header_chain_score(tip.hash)
            .expect("retained")
            .tip_hash
            .0
    });

    let plan = enforce_retention(&mut store, header_best, anchor, [], limits(10, 100))
        .expect("retention succeeds");
    assert!(store.header_node(expected[0].hash).is_none());
    assert!(store.header_node(expected[1].hash).is_none());
    assert_eq!(store.eligible_header_tips().len(), 10);
    assert!(store.header_node(header_best.hash).is_some());
    assert!(!plan.admission_refused);
    assert_eq!(plan.work.protected_path_visits, 2);
    assert_eq!(plan.work.candidate_nodes_scanned, 13);
    assert_eq!(plan.work.evicted_nodes, 2);
    assert_eq!(plan.work.graph_workspaces, 4);

    let reacquired_seed = (1..=12)
        .find(|seed| {
            let mut header = *regtest_genesis_block().header;
            header.previous_block_hash = anchor.hash;
            header.nonce = [*seed; 32].into();
            header.hash() == expected[0].hash
        })
        .expect("the evicted fixture tip has a seed");
    let reacquired = insert_header(&mut store, anchor.hash, reacquired_seed, []);
    assert_eq!(reacquired.hash, expected[0].hash);
    assert!(store
        .header_node(reacquired.hash)
        .expect("reacquired")
        .is_eligible());
}

#[test]
fn eligible_tip_with_deferred_descendant_is_evicted_without_stalling() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let tips: Vec<_> = (1..=12)
        .map(|seed| insert_header(&mut store, anchor.hash, seed, []))
        .collect();
    let header_best = store
        .select_best_header_chain()
        .expect("graph is coherent")
        .0;
    let victim = tips
        .iter()
        .copied()
        .filter(|tip| *tip != header_best)
        .min_by_key(|tip| store.header_chain_score(tip.hash).expect("retained"))
        .expect("one unprotected tip exists");
    let deferred = insert_header(&mut store, victim.hash, 13, []);
    store
        .set_header_validation_state(
            deferred.hash,
            HeaderValidationState::DeferredUntil(
                regtest_genesis_block().header.time + chrono::Duration::hours(3),
            ),
        )
        .expect("the deferred descendant is retained");

    assert!(store
        .eligible_header_tips()
        .iter()
        .any(|tip| tip.hash == victim.hash));
    assert!(!store
        .header_node(deferred.hash)
        .expect("the deferred child is retained")
        .is_eligible());

    let plan = enforce_retention(&mut store, header_best, anchor, [], limits(10, 100))
        .expect("retention evicts the ineligible descendant subtree");

    assert!(store.header_node(deferred.hash).is_none());
    assert!(store.header_node(victim.hash).is_none());
    assert_eq!(store.eligible_header_tips().len(), 10);
    assert!(!plan.admission_refused);
}

#[test]
fn permanent_subtrees_are_evicted_first_only_under_pressure() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let permanent = insert_header(
        &mut store,
        anchor.hash,
        1,
        [EligibilityReason::CheckpointConflict {
            height: block::Height(1),
            expected: block::Hash([9; 32]),
        }],
    );
    let selected = insert_header(&mut store, anchor.hash, 2, []);
    let spare = insert_header(&mut store, anchor.hash, 3, []);

    enforce_retention(&mut store, selected, anchor, [], limits(10, 2))
        .expect("permanent subtree frees capacity");
    assert!(store.header_node(permanent.hash).is_none());
    assert!(store.header_node(selected.hash).is_some());
    assert!(store.header_node(spare.hash).is_some());
}

#[test]
fn permanent_eviction_exposes_leaf_for_node_limit_eviction() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let parent = insert_header(&mut store, anchor.hash, 1, []);
    let permanent = insert_header(
        &mut store,
        parent.hash,
        2,
        [EligibilityReason::CheckpointConflict {
            height: block::Height(2),
            expected: block::Hash([9; 32]),
        }],
    );
    let selected = insert_header(&mut store, anchor.hash, 3, []);

    let plan = enforce_retention(&mut store, selected, anchor, [], limits(10, 1))
        .expect("permanent eviction exposes the parent as an eviction candidate");

    assert!(!plan.admission_refused);
    assert!(store.header_node(permanent.hash).is_none());
    assert!(store.header_node(parent.hash).is_none());
    assert!(store.header_node(selected.hash).is_some());
    assert_eq!(store.header_node_count(), 2);
    assert_eq!(plan.work.candidate_nodes_scanned, 7);
    assert_eq!(plan.work.evicted_nodes, 2);
}

#[test]
fn retention_below_both_limits_has_no_graph_work_or_effects() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let permanent = insert_header(
        &mut store,
        anchor.hash,
        1,
        [EligibilityReason::CheckpointConflict {
            height: block::Height(1),
            expected: block::Hash([9; 32]),
        }],
    );
    let selected = insert_header(&mut store, anchor.hash, 2, []);
    let before_header_node_count = store.header_node_count();
    let before_tips = store.eligible_header_tips();
    let before_permanent = store.header_node(permanent.hash).cloned();
    let before_selected = store.header_node(selected.hash).cloned();

    let plan = enforce_retention(&mut store, selected, anchor, [], limits(10, 10))
        .expect("a graph below both limits needs no eviction planning");

    assert_eq!(plan, RetentionPlan::default());
    assert_eq!(plan.work, RetentionWork::default());
    assert_eq!(store.header_node_count(), before_header_node_count);
    assert_eq!(store.eligible_header_tips(), before_tips);
    assert_eq!(store.header_node(permanent.hash), before_permanent.as_ref());
    assert_eq!(store.header_node(selected.hash), before_selected.as_ref());
}

#[test]
fn protected_paths_and_context_references_fail_closed_under_node_pressure() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let first = insert_header(&mut store, anchor.hash, 1, []);
    let selected = insert_header(&mut store, first.hash, 2, []);
    let unprotected = insert_header(
        &mut store,
        anchor.hash,
        3,
        [EligibilityReason::CheckpointConflict {
            height: block::Height(1),
            expected: block::Hash([9; 32]),
        }],
    );

    let plan = enforce_retention(&mut store, selected, anchor, [first.hash], limits(10, 1))
        .expect("retention returns a typed refusal");
    assert!(plan.admission_refused);
    assert!(plan.resource_stalled);
    assert!(store.header_node(selected.hash).is_some());
    assert!(store.header_node(unprotected.hash).is_some());
}

#[test]
fn verified_side_node_does_not_become_a_protection_root() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let verified_body = insert_header(&mut store, anchor.hash, 1, []);
    store
        .set_body_validation_state(
            verified_body.hash,
            BodyValidationState::Verified {
                evidence: crate::EvidenceId::from_digest([0x51; 32]),
            },
        )
        .expect("the full-state side branch becomes verified");
    let selected_parent = insert_header(&mut store, anchor.hash, 2, []);
    let selected = insert_header(&mut store, selected_parent.hash, 3, []);
    let _header_only = insert_header(&mut store, anchor.hash, 4, []);

    let plan = enforce_retention(&mut store, selected, selected, [], limits(1, 100))
        .expect("retention evicts both unprotected side branches");

    assert!(!plan.admission_refused);
    assert!(store.header_node(selected.hash).is_some());
    assert!(store.header_node(verified_body.hash).is_none());
    assert_eq!(store.eligible_header_tips().len(), 1);
}

#[test]
fn unselected_verified_body_paths_do_not_fill_retention_capacity() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let first = insert_header(&mut store, anchor.hash, 1, []);
    let verified_body = insert_header(&mut store, first.hash, 2, []);
    store
        .set_body_validation_state(
            verified_body.hash,
            BodyValidationState::Verified {
                evidence: crate::EvidenceId::from_digest([0x52; 32]),
            },
        )
        .expect("the full-state branch becomes verified");
    let selected = insert_header(&mut store, anchor.hash, 3, []);

    let plan = enforce_retention(&mut store, selected, selected, [], limits(1, 1))
        .expect("retention removes the unprotected verified side path");

    assert!(!plan.admission_refused);
    assert!(store.header_node(first.hash).is_none());
    assert!(store.header_node(verified_body.hash).is_none());
    assert!(store.header_node(selected.hash).is_some());
}

#[test]
fn authoritative_full_state_replacement_evicts_the_dropped_verified_branch() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let retained_tips = (1..=10)
        .map(|seed| insert_header(&mut store, anchor.hash, seed, []))
        .collect::<Vec<_>>();
    for tip in &retained_tips {
        store
            .set_body_validation_state(
                tip.hash,
                BodyValidationState::Verified {
                    evidence: crate::EvidenceId::from_digest(tip.hash.0),
                },
            )
            .expect("the full-state branch becomes verified");
    }

    let replacement = insert_header(&mut store, anchor.hash, 11, []);
    store
        .set_body_validation_state(
            replacement.hash,
            BodyValidationState::Verified {
                evidence: crate::EvidenceId::from_digest(replacement.hash.0),
            },
        )
        .expect("the replacement full-state branch becomes verified");
    let dropped = retained_tips[0];
    let staged_tips = retained_tips
        .iter()
        .copied()
        .skip(1)
        .map(|tip| tip.hash)
        .chain(std::iter::once(replacement.hash))
        .collect::<Vec<_>>();

    let plan = enforce_retention(
        &mut store,
        replacement,
        replacement,
        staged_tips,
        limits(10, 100),
    )
    .expect("the authoritative full-state fork set permits replacement");

    assert!(!plan.admission_refused);
    assert!(store.header_node(dropped.hash).is_none());
    assert!(store.header_node(replacement.hash).is_some());
    assert_eq!(store.eligible_header_tips().len(), 10);
}

#[test]
fn overlapping_protected_paths_stop_at_their_first_protected_ancestor() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let first = insert_header(&mut store, anchor.hash, 1, []);
    let selected = insert_header(&mut store, first.hash, 2, []);
    let branch = insert_header(&mut store, first.hash, 3, []);
    let mut protected_header_hashes = HashSet::new();

    assert_eq!(
        add_protected_header_path(&store, selected.hash, &mut protected_header_hashes)
            .expect("the selected path is coherent"),
        3
    );
    assert_eq!(
        add_protected_header_path(&store, branch.hash, &mut protected_header_hashes)
            .expect("the branch joins the selected path"),
        1
    );
    assert_eq!(
        add_protected_header_path(&store, first.hash, &mut protected_header_hashes)
            .expect("the context reference is already protected"),
        0
    );
    assert!(protected_header_hashes.contains(&anchor.hash));
    assert!(protected_header_hashes.contains(&first.hash));
    assert!(protected_header_hashes.contains(&selected.hash));
    assert!(protected_header_hashes.contains(&branch.hash));
}

#[test]
fn reference_pruned_before_retention_is_no_longer_protectable() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();

    let plan = enforce_retention(
        &mut store,
        anchor,
        anchor,
        [block::Hash([0x91; 32])],
        limits(10, 100),
    )
    .expect("a reference already pruned by finality is ignored");

    assert_eq!(plan, RetentionPlan::default());
}

#[test]
fn exact_v1_node_boundary_refuses_to_evict_the_selected_path() {
    let mut store = retention_graph();
    let anchor = store.finalized_frontier();
    let mut selected = anchor;
    for offset in 0..=crate::MAX_NON_FINALIZED_NODES_V1 {
        let seed = u8::try_from(offset % 251).expect("the reduced test nonce fits in u8");
        selected = insert_header(&mut store, selected.hash, seed, []);
    }
    assert_eq!(
        store.header_node_count() - 1,
        crate::MAX_NON_FINALIZED_NODES_V1 + 1
    );

    let plan = enforce_retention(&mut store, selected, anchor, [], EngineLimits::v1())
        .expect("the exact boundary produces a typed refusal");
    assert!(plan.admission_refused);
    assert!(plan.resource_stalled);
    assert!(store.header_node(selected.hash).is_some());
}
