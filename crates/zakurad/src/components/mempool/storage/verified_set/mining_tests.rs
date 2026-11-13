//! Mining-order scores, tie-breaking, and mutation coverage.

use super::*;
use crate::components::mempool::storage::fixtures::TxFactory;

fn insert(
    verified: &mut VerifiedSet,
    tx: VerifiedUnminedTx,
    spent: Vec<transparent::OutPoint>,
) -> transaction::Hash {
    let id = tx.transaction.id().mined_id();
    verified
        .insert(tx, spent, &mut PendingOutputs::default(), None)
        .unwrap();
    id
}

fn ids(verified: &VerifiedSet) -> Vec<transaction::Hash> {
    verified
        .mining_transactions()
        .map(|tx| tx.transaction.id().mined_id())
        .collect()
}

fn set_time(verified: &mut VerifiedSet, id: transaction::Hash, seconds: i64) {
    let affected = verified.dependency_group([id]);
    verified.remove_ordering_candidates(&affected);
    verified.transactions.get_mut(&id).unwrap().time =
        Some(chrono::DateTime::from_timestamp(seconds, 0).unwrap());
    verified.insert_ordering_candidates(&affected);
}

#[test]
fn mining_order_prefers_fee_per_cost_over_absolute_fee() {
    let mut factory = TxFactory::new();
    let mut verified = VerifiedSet::default();
    let large = factory.tx_with(30_000, 0, 25_000);
    let small = factory.tx(20_000);
    assert!(large.cost() > 2 * small.cost());
    let large = insert(&mut verified, large, Vec::new());
    let small = insert(&mut verified, small, Vec::new());
    let highest = insert(&mut verified, factory.tx(40_000), Vec::new());
    assert_eq!(ids(&verified), vec![highest, small, large]);
    verified.assert_dependency_groups_are_bounded();
}

#[test]
fn equal_mining_scores_prefer_older_entries_then_txid() {
    let mut factory = TxFactory::new();
    let mut verified = VerifiedSet::default();
    let a = insert(&mut verified, factory.tx(10_000), Vec::new());
    let oldest = insert(&mut verified, factory.tx(10_000), Vec::new());
    let b = insert(&mut verified, factory.tx(10_000), Vec::new());
    set_time(&mut verified, a, 200);
    set_time(&mut verified, oldest, 100);
    set_time(&mut verified, b, 200);
    let mut tied = [a, b];
    tied.sort();
    assert_eq!(ids(&verified), vec![oldest, tied[0], tied[1]]);
    verified.assert_dependency_groups_are_bounded();
}

#[test]
fn mining_uses_ancestors_and_rescores_children_when_the_parent_is_mined() {
    let mut factory = TxFactory::new();
    let mut verified = VerifiedSet::default();
    let parent = insert(&mut verified, factory.tx_with(10_000, 1, 0), Vec::new());
    let child = insert(
        &mut verified,
        factory.tx(90_000),
        vec![transparent::OutPoint::from_usize(parent, 0)],
    );
    let independent = insert(&mut verified, factory.tx(60_000), Vec::new());
    // The child pays nine zatoshis per cost on its own, but five with its parent.
    // A high-fee child does not raise its parent's mining score.
    assert_eq!(ids(&verified), vec![independent, child, parent]);
    verified.assert_dependency_groups_are_bounded();

    verified.clear_mined_dependencies(&HashSet::from([parent]));
    assert_eq!(ids(&verified), vec![child, independent, parent]);
    verified.remove_all([parent]);
    assert_eq!(ids(&verified), vec![child, independent]);
    verified.assert_dependency_groups_are_bounded();
}

#[test]
fn mining_packages_count_shared_ancestors_once() {
    let mut factory = TxFactory::new();
    let mut verified = VerifiedSet::default();
    let parent = insert(&mut verified, factory.tx_with(10_000, 2, 0), Vec::new());
    let child = insert(
        &mut verified,
        factory.tx_with(20_000, 1, 0),
        vec![transparent::OutPoint::from_usize(parent, 0)],
    );
    let grandchild = insert(
        &mut verified,
        factory.tx(90_000),
        vec![
            transparent::OutPoint::from_usize(parent, 1),
            transparent::OutPoint::from_usize(child, 0),
        ],
    );
    let (Reverse(score), _, _) = verified.mining_candidate(grandchild);
    assert_eq!(
        score,
        EvictionCost::new(120_000, 3 * MEMPOOL_TRANSACTION_COST_THRESHOLD)
    );
    let independent = insert(&mut verified, factory.tx(45_000), Vec::new());
    assert_eq!(ids(&verified), vec![independent, grandchild, child, parent]);
    verified.assert_dependency_groups_are_bounded();
}

#[test]
fn descendant_removal_and_clear_remove_mining_candidates() {
    let mut factory = TxFactory::new();
    let mut verified = VerifiedSet::default();
    let parent = insert(&mut verified, factory.tx_with(10_000, 1, 0), Vec::new());
    insert(
        &mut verified,
        factory.tx(20_000),
        vec![transparent::OutPoint::from_usize(parent, 0)],
    );
    let survivor = insert(&mut verified, factory.tx(30_000), Vec::new());
    assert_eq!(verified.remove(&parent).len(), 2);
    assert_eq!(ids(&verified), vec![survivor]);
    verified.assert_dependency_groups_are_bounded();
    verified.clear();
    assert!(ids(&verified).is_empty());
    verified.assert_dependency_groups_are_bounded();
}

#[test]
fn failed_insert_does_not_change_mining_order() {
    let mut factory = TxFactory::new();
    let mut verified = VerifiedSet::default();
    let tx = factory.tx(10_000);
    insert(&mut verified, tx.clone(), Vec::new());
    let before = verified.mining_order.clone();
    assert_eq!(
        verified.insert(tx, Vec::new(), &mut PendingOutputs::default(), None),
        Err(SameEffectsTipRejectionError::SpendConflict)
    );
    assert_eq!(verified.mining_order, before);
    verified.assert_dependency_groups_are_bounded();
}
