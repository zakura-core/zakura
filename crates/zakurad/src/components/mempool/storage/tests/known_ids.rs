//! Known-ID removal regressions and block-cleanup measurements.

use std::{collections::HashSet, hint::black_box, sync::Arc, time::Instant};

use zakura_chain::{
    block::Height,
    transaction::{Transaction, VerifiedUnminedTx, MEMPOOL_TRANSACTION_COST_THRESHOLD},
    transparent::OutPoint,
};

use super::super::{config::Config, fixtures::TxFactory, SameEffectsChainRejectionError, Storage};

fn with_expiry(mut tx: VerifiedUnminedTx, height: Height) -> VerifiedUnminedTx {
    let mut transaction = tx.transaction.transaction().as_ref().clone();
    *transaction.expiry_height_mut() = height;
    tx.transaction = transaction.into();
    tx
}

#[test]
fn expiry_tracks_boundaries_removal_and_clear() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let removed_early = with_expiry(factory.tx(10_000), Height(10));
    let due = with_expiry(factory.tx(10_000), Height(10));
    let future = with_expiry(factory.tx(10_000), Height(11));
    let no_expiry = with_expiry(factory.tx(10_000), Height(0));
    let early_id = removed_early.transaction.id();
    let due_id = due.transaction.id();
    let survivors = HashSet::from([future.transaction.id(), no_expiry.transaction.id()]);
    for tx in [removed_early, due, future, no_expiry] {
        storage.insert(tx, Vec::new(), None).unwrap();
    }
    assert!(storage.remove_expired_transactions(Height(9)).is_empty());
    assert_eq!(storage.remove_exact(&HashSet::from([early_id])), 1);
    assert_eq!(
        storage.remove_expired_transactions(Height(10)),
        HashSet::from([due_id]),
    );
    assert!(storage.remove_expired_transactions(Height(10)).is_empty());
    assert_eq!(storage.tx_ids().collect::<HashSet<_>>(), survivors);

    storage.clear();
    let last = with_expiry(factory.tx(10_000), Height::MAX);
    let last_id = last.transaction.id();
    storage.insert(last, Vec::new(), None).unwrap();
    assert_eq!(
        storage.remove_expired_transactions(Height::MAX),
        HashSet::from([last_id]),
    );
    assert_eq!(storage.transaction_count(), 0);
}

#[test]
fn expiry_removes_later_expiring_descendants_from_the_index() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = with_expiry(factory.tx_with(10_000, 1, 0), Height(10));
    let child = with_expiry(factory.tx(10_000), Height(20));
    let parent_id = parent.transaction.id();
    let child_id = child.transaction.id();
    storage.insert(parent, Vec::new(), None).unwrap();
    storage
        .insert(
            child,
            vec![OutPoint::from_usize(parent_id.mined_id(), 0)],
            None,
        )
        .unwrap();
    assert_eq!(
        storage.remove_expired_transactions(Height(10)),
        HashSet::from([parent_id, child_id]),
    );
    assert!(storage.remove_expired_transactions(Height(20)).is_empty());
    assert!(storage.rejection_error(&child_id).is_none());
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn expiry_removes_overlapping_roots_and_reports_each_transaction_once() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = with_expiry(factory.tx_with(10_000, 1, 0), Height(1));
    let child = with_expiry(factory.tx_with(10_000, 1, 0), Height(1));
    let survivor = with_expiry(factory.tx(10_000), Height(0));
    let parent_id = parent.transaction.id();
    let child_id = child.transaction.id();
    let survivor_id = survivor.transaction.id();
    let survivor_cost = survivor.cost();
    let output = OutPoint::from_usize(parent_id.mined_id(), 0);
    storage.insert(parent, Vec::new(), None).unwrap();
    storage.insert(child, vec![output], None).unwrap();
    storage.insert(survivor, Vec::new(), None).unwrap();

    assert_eq!(
        storage.remove_expired_transactions(Height(1)),
        HashSet::from([parent_id, child_id])
    );
    assert_eq!(
        storage.tx_ids().collect::<HashSet<_>>(),
        HashSet::from([survivor_id])
    );
    assert_eq!(storage.total_cost(), survivor_cost);
    assert!(storage.created_output(&output).is_none());
    for id in [parent_id, child_id] {
        assert_eq!(
            storage.rejection_error(&id),
            Some(SameEffectsChainRejectionError::Expired.into())
        );
    }
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn mined_parent_keeps_unmined_child_and_ignores_unknown_mined_ids() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx(10_000);
    let parent_id = parent.transaction.id();
    let child_id = child.transaction.id();
    let missing = factory.tx(10_000).transaction.id().mined_id();
    let block_transactions = vec![parent.transaction.transaction().clone()];
    storage.insert(parent, Vec::new(), None).unwrap();
    storage
        .insert(
            child,
            vec![OutPoint::from_usize(parent_id.mined_id(), 0)],
            None,
        )
        .unwrap();
    let mined = HashSet::from([parent_id.mined_id(), missing]);
    let removed = storage.reject_and_remove_same_effects(&mined, block_transactions);
    assert_eq!(removed.mined, HashSet::from([parent_id]));
    assert!(removed.invalidated.is_empty());
    assert_eq!(
        storage.tx_ids().collect::<HashSet<_>>(),
        HashSet::from([child_id])
    );
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn mining_parent_and_child_preserves_the_unmined_grandchild() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx_with(10_000, 1, 0);
    let grandchild = factory.tx(10_000);
    let parent_id = parent.transaction.id();
    let child_id = child.transaction.id();
    let grandchild_id = grandchild.transaction.id();
    let block = vec![
        parent.transaction.transaction().clone(),
        child.transaction.transaction().clone(),
    ];
    storage.insert(parent, Vec::new(), None).unwrap();
    storage
        .insert(
            child,
            vec![OutPoint::from_usize(parent_id.mined_id(), 0)],
            None,
        )
        .unwrap();
    storage
        .insert(
            grandchild,
            vec![OutPoint::from_usize(child_id.mined_id(), 0)],
            None,
        )
        .unwrap();
    let mined = HashSet::from([parent_id.mined_id(), child_id.mined_id()]);
    let removed = storage.reject_and_remove_same_effects(&mined, block);
    assert_eq!(removed.mined, HashSet::from([parent_id, child_id]));
    assert!(removed.invalidated.is_empty());
    assert_eq!(
        storage.tx_ids().collect::<HashSet<_>>(),
        HashSet::from([grandchild_id])
    );
    assert!(storage.transaction_dependencies().dependencies().is_empty());
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn batch_removal_deduplicates_overlapping_roots_and_keeps_unrelated_transactions() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx_with(10_000, 1, 0);
    let grandchild = factory.tx(10_000);
    let survivor = factory.tx(10_000);
    let parent_id = parent.transaction.id();
    let child_id = child.transaction.id();
    let grandchild_id = grandchild.transaction.id();
    let survivor_id = survivor.transaction.id();
    storage.insert(parent, Vec::new(), None).unwrap();
    storage
        .insert(
            child,
            vec![OutPoint::from_usize(parent_id.mined_id(), 0)],
            None,
        )
        .unwrap();
    storage
        .insert(
            grandchild,
            vec![OutPoint::from_usize(child_id.mined_id(), 0)],
            None,
        )
        .unwrap();
    storage.insert(survivor, Vec::new(), None).unwrap();
    let missing = factory.tx(10_000).transaction.id().mined_id();
    assert_eq!(
        storage.verified.remove_all([
            child_id.mined_id(),
            parent_id.mined_id(),
            missing,
            child_id.mined_id()
        ]),
        HashSet::from([parent_id, child_id, grandchild_id]),
    );
    assert_eq!(
        storage.tx_ids().collect::<HashSet<_>>(),
        HashSet::from([survivor_id])
    );
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn block_conflict_removes_descendants_and_caches_the_conflicting_root() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx(10_000);
    let parent_id = parent.transaction.id();
    let child_id = child.transaction.id();
    let conflict = with_expiry(parent.clone(), Height(2));
    let mined = HashSet::from([conflict.transaction.id().mined_id()]);
    storage.insert(parent, Vec::new(), None).unwrap();
    storage
        .insert(
            child,
            vec![OutPoint::from_usize(parent_id.mined_id(), 0)],
            None,
        )
        .unwrap();
    let removed = storage
        .reject_and_remove_same_effects(&mined, vec![conflict.transaction.transaction().clone()]);
    assert!(removed.mined.is_empty());
    assert_eq!(removed.invalidated, HashSet::from([parent_id, child_id]));
    assert_eq!(storage.total_cost(), 0);
    assert_eq!(
        storage.rejection_error(&parent_id),
        Some(SameEffectsChainRejectionError::DuplicateSpend.into())
    );
    assert!(storage.rejection_error(&child_id).is_none());
    storage.verified.assert_dependency_groups_are_bounded();
}

/// Measures existing block/expiry entry points; restoration is outside timing.
/// Run the identical harness on the control and candidate revisions.
#[test]
#[ignore = "manual performance measurement; run with --release --ignored --nocapture"]
#[allow(clippy::print_stdout, reason = "manual benchmark results")]
fn benchmark_mempool_known_id_cleanup() {
    const SAMPLES: usize = 21;
    let config = Config::default();
    let default_count = config.tx_cost_limit / MEMPOOL_TRANSACTION_COST_THRESHOLD;
    let mut factory = TxFactory::new();
    for count in [default_count / 8, default_count] {
        for removed_count in [0, 1, count / 8] {
            let mut storage = Storage::new(&config);
            let mut victims = Vec::new();
            for index in 0..count {
                let is_victim = index < removed_count;
                let tx = with_expiry(factory.tx(10_000), Height(u32::from(is_victim)));
                if is_victim {
                    victims.push(tx.clone());
                }
                storage.insert(tx, Vec::new(), None).unwrap();
            }
            for mode in ["mined", "conflict", "expiry"] {
                let block_transactions: Vec<Arc<Transaction>> = victims
                    .iter()
                    .map(|tx| {
                        if mode == "conflict" {
                            with_expiry(tx.clone(), Height(2))
                                .transaction
                                .transaction()
                                .clone()
                        } else {
                            tx.transaction.transaction().clone()
                        }
                    })
                    .collect();
                let mined = block_transactions
                    .iter()
                    .map(|tx| tx.unmined_id().mined_id())
                    .collect();
                let mut samples = Vec::with_capacity(SAMPLES);
                for _ in 0..SAMPLES {
                    let start = Instant::now();
                    let removed = if mode == "expiry" {
                        storage
                            .remove_expired_transactions(black_box(Height(1)))
                            .len()
                    } else {
                        storage
                            .reject_and_remove_same_effects(
                                black_box(&mined),
                                black_box(block_transactions.clone()),
                            )
                            .total_len()
                    };
                    samples.push(start.elapsed());
                    assert_eq!(u64::try_from(black_box(removed)).unwrap(), removed_count);
                    storage.chain_rejected_same_effects.clear();
                    for tx in &victims {
                        storage.insert(tx.clone(), Vec::new(), None).unwrap();
                    }
                    assert_eq!(u64::try_from(storage.transaction_count()).unwrap(), count);
                }
                samples.sort();
                println!(
                    "cleanup transactions={count} removed={removed_count} case={mode} \
                     median_ns={} min_ns={} max_ns={}",
                    samples[SAMPLES / 2].as_nanos(),
                    samples[0].as_nanos(),
                    samples[SAMPLES - 1].as_nanos(),
                );
            }
        }
    }
}
