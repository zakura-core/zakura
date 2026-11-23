//! What a block removes, in the order the pool core logs it, and what it
//! caches.

use std::collections::HashSet;

use zakura_chain::{
    block::Height,
    transaction::{self, VerifiedUnminedTx},
    transparent::{self, OutPoint},
};
use zakura_node_services::mempool::port::RemovalReason;

use super::{
    super::{
        config::Config, fixtures::TxFactory, BlockRemovals, EffectsRejection,
        SameEffectsChainRejectionError,
    },
    Storage,
};

/// `tx` expiring at `height`. A changed expiry is a changed mined id, and the
/// copy keeps `tx`'s nullifier, so the two conflict.
fn with_expiry(mut tx: VerifiedUnminedTx, height: Height) -> VerifiedUnminedTx {
    let mut transaction = tx.transaction.transaction().as_ref().clone();
    *transaction.expiry_height_mut() = height;
    tx.transaction = transaction.into();
    tx
}

/// Stores `tx`, which spends `spent` from the pool. Returns its mined id.
fn insert(
    storage: &mut Storage,
    tx: &VerifiedUnminedTx,
    spent: Vec<OutPoint>,
) -> transaction::Hash {
    storage
        .insert(
            tx.clone(),
            spent,
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .unwrap();
    tx.transaction.id().mined_id()
}

/// The first output of the transaction `id`.
fn output(id: transaction::Hash) -> OutPoint {
    OutPoint::from_usize(id, 0)
}

/// The chain-scoped rejection cached for `id`, if any.
fn cached(storage: &Storage, id: transaction::Hash) -> Option<SameEffectsChainRejectionError> {
    match storage.rejected_effects(&id, crate::Instant::now()) {
        Some(EffectsRejection::Chain(error)) => Some(error),
        _ => None,
    }
}

#[test]
fn mined_parent_keeps_unmined_child_and_ignores_unknown_mined_ids() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx(10_000);
    let parent_id = insert(&mut storage, &parent, Vec::new());
    insert(&mut storage, &child, vec![output(parent_id)]);
    let missing = factory.tx(10_000).transaction.id().mined_id();

    let removed = storage.connect_block(
        &[missing, parent_id],
        [parent.transaction.transaction().as_ref()],
        Height(1),
        crate::Instant::now(),
    );
    assert_eq!(
        removed,
        BlockRemovals {
            mined: vec![parent.transaction.id()],
            dropped: Vec::new(),
        }
    );
    assert_eq!(storage.pooled_ids(), vec![child.transaction.id()]);
    assert!(storage.transaction_dependencies().dependencies().is_empty());
    for id in [missing, parent_id] {
        assert_eq!(
            cached(&storage, id),
            Some(SameEffectsChainRejectionError::Mined)
        );
    }
    storage.verified.assert_dependency_groups_are_bounded();
}

/// A block removes what it mined in block order, then each conflicting
/// transaction oldest first with what depends on it, then each expired one
/// the same way. Only the roots are cached.
#[test]
fn a_block_reports_mined_then_conflicting_then_expired_in_log_order() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let first_mined = factory.tx(10_000);
    let second_mined = factory.tx(10_000);
    let conflicting = factory.tx_with(10_000, 1, 0);
    let conflicting_child = factory.tx(10_000);
    let expiring = with_expiry(factory.tx_with(10_000, 1, 0), Height(5));
    let expiring_child = factory.tx(10_000);
    let survivor = factory.tx(10_000);

    let first_mined_id = insert(&mut storage, &first_mined, Vec::new());
    let second_mined_id = insert(&mut storage, &second_mined, Vec::new());
    let expiring_id = insert(&mut storage, &expiring, Vec::new());
    let expiring_child_id = insert(&mut storage, &expiring_child, vec![output(expiring_id)]);
    let conflicting_id = insert(&mut storage, &conflicting, Vec::new());
    let conflicting_child_id = insert(
        &mut storage,
        &conflicting_child,
        vec![output(conflicting_id)],
    );
    insert(&mut storage, &survivor, Vec::new());

    let rival = with_expiry(conflicting.clone(), Height(1_000));
    let removed = storage.connect_block(
        &[
            second_mined_id,
            first_mined_id,
            rival.transaction.id().mined_id(),
        ],
        [
            second_mined.transaction.transaction().as_ref(),
            first_mined.transaction.transaction().as_ref(),
            rival.transaction.transaction().as_ref(),
        ],
        Height(5),
        crate::Instant::now(),
    );
    assert_eq!(
        removed,
        BlockRemovals {
            mined: vec![second_mined.transaction.id(), first_mined.transaction.id()],
            dropped: vec![
                (conflicting.transaction.id(), RemovalReason::Conflicted),
                (
                    conflicting_child.transaction.id(),
                    RemovalReason::Invalidated
                ),
                (expiring.transaction.id(), RemovalReason::Expired),
                (expiring_child.transaction.id(), RemovalReason::Invalidated),
            ],
        }
    );
    assert_eq!(storage.pooled_ids(), vec![survivor.transaction.id()]);
    assert_eq!(
        cached(&storage, conflicting_id),
        Some(SameEffectsChainRejectionError::DuplicateSpend)
    );
    assert_eq!(
        cached(&storage, expiring_id),
        Some(SameEffectsChainRejectionError::Expired)
    );
    for id in [conflicting_child_id, expiring_child_id] {
        assert_eq!(cached(&storage, id), None);
    }
    storage.verified.assert_dependency_groups_are_bounded();
}

/// A root that an older root's removal already took is logged as
/// invalidated, but still cached as a root.
#[test]
fn a_root_taken_with_an_older_root_is_logged_invalidated_and_cached_as_a_root() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx(10_000);
    let parent_id = insert(&mut storage, &parent, Vec::new());
    let child_id = insert(&mut storage, &child, vec![output(parent_id)]);

    // The block spends both nullifiers, child's first.
    let rivals = [
        with_expiry(child.clone(), Height(1_000)),
        with_expiry(parent.clone(), Height(1_000)),
    ];
    let removed = storage.connect_block(
        &[],
        rivals
            .iter()
            .map(|tx| tx.transaction.transaction().as_ref()),
        Height(1),
        crate::Instant::now(),
    );
    assert_eq!(
        removed.dropped,
        vec![
            (parent.transaction.id(), RemovalReason::Conflicted),
            (child.transaction.id(), RemovalReason::Invalidated),
        ]
    );
    for id in [parent_id, child_id] {
        assert_eq!(
            cached(&storage, id),
            Some(SameEffectsChainRejectionError::DuplicateSpend)
        );
    }

    let parent = with_expiry(factory.tx_with(10_000, 1, 0), Height(3));
    let child = with_expiry(factory.tx(10_000), Height(2));
    let parent_id = insert(&mut storage, &parent, Vec::new());
    let child_id = insert(&mut storage, &child, vec![output(parent_id)]);
    let removed = storage.connect_block(&[], [], Height(3), crate::Instant::now());
    assert_eq!(
        removed.dropped,
        vec![
            (parent.transaction.id(), RemovalReason::Expired),
            (child.transaction.id(), RemovalReason::Invalidated),
        ]
    );
    for id in [parent_id, child_id] {
        assert_eq!(
            cached(&storage, id),
            Some(SameEffectsChainRejectionError::Expired)
        );
    }
    storage.verified.assert_dependency_groups_are_bounded();
}

/// Children leave oldest first, and a child of two conflicting parents
/// leaves with the older one, before the younger parent.
#[test]
fn a_removal_walks_children_oldest_first() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 2, 0);
    let children: Vec<_> = (0..2).map(|_| factory.tx(10_000)).collect();
    let parent_id = insert(&mut storage, &parent, Vec::new());
    for (index, child) in children.iter().enumerate() {
        insert(
            &mut storage,
            child,
            vec![OutPoint::from_usize(parent_id, index)],
        );
    }
    let rival = with_expiry(parent.clone(), Height(1_000));
    let removed = storage.connect_block(
        &[],
        [rival.transaction.transaction().as_ref()],
        Height(1),
        crate::Instant::now(),
    );
    assert_eq!(
        removed.dropped,
        vec![
            (parent.transaction.id(), RemovalReason::Conflicted),
            (children[0].transaction.id(), RemovalReason::Invalidated),
            (children[1].transaction.id(), RemovalReason::Invalidated),
        ]
    );

    let parents: Vec<_> = (0..2).map(|_| factory.tx_with(10_000, 1, 0)).collect();
    let child = factory.tx(10_000);
    let parent_ids: Vec<_> = parents
        .iter()
        .map(|parent| insert(&mut storage, parent, Vec::new()))
        .collect();
    insert(
        &mut storage,
        &child,
        parent_ids.iter().map(|&id| output(id)).collect(),
    );
    // The block spends the younger parent's nullifier first.
    let rivals: Vec<_> = parents
        .iter()
        .rev()
        .map(|parent| with_expiry(parent.clone(), Height(1_000)))
        .collect();
    let removed = storage.connect_block(
        &[],
        rivals
            .iter()
            .map(|tx| tx.transaction.transaction().as_ref()),
        Height(2),
        crate::Instant::now(),
    );
    assert_eq!(
        removed.dropped,
        vec![
            (parents[0].transaction.id(), RemovalReason::Conflicted),
            (child.transaction.id(), RemovalReason::Invalidated),
            (parents[1].transaction.id(), RemovalReason::Conflicted),
        ]
    );
    storage.verified.assert_dependency_groups_are_bounded();
}

/// A child that spends a pooled parent's output leaves with it even if
/// its verifier found that output on a chain that had mined the parent, so
/// it named no pool input.
#[test]
fn a_spender_with_no_pool_input_leaves_with_its_parent() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 1, 0);
    let parent_id = insert(&mut storage, &parent, Vec::new());
    let mut child = factory.tx(10_000);
    let mut transaction = child.transaction.transaction().as_ref().clone();
    *transaction.inputs_mut() = vec![transparent::Input::PrevOut {
        outpoint: output(parent_id),
        unlock_script: transparent::Script::new(&[]),
        sequence: 0,
    }];
    child.transaction = transaction.into();
    insert(&mut storage, &child, Vec::new());

    let rival = with_expiry(parent.clone(), Height(1_000));
    let removed = storage.connect_block(
        &[],
        [rival.transaction.transaction().as_ref()],
        Height(1),
        crate::Instant::now(),
    );
    assert_eq!(
        removed.dropped,
        vec![
            (parent.transaction.id(), RemovalReason::Conflicted),
            (child.transaction.id(), RemovalReason::Invalidated),
        ]
    );
    assert_eq!(storage.transaction_count(), 0);
    storage.verified.assert_dependency_groups_are_bounded();
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
    for tx in [&removed_early, &due, &future, &no_expiry] {
        insert(&mut storage, tx, Vec::new());
    }
    let expire = |storage: &mut Storage, height| {
        storage
            .connect_block(&[], [], height, crate::Instant::now())
            .dropped
    };
    assert!(expire(&mut storage, Height(9)).is_empty());
    assert_eq!(storage.remove_exact(&HashSet::from([early_id])), 1);
    assert_eq!(
        expire(&mut storage, Height(10)),
        vec![(due_id, RemovalReason::Expired)]
    );
    assert!(expire(&mut storage, Height(10)).is_empty());
    assert_eq!(
        storage.pooled_ids(),
        vec![future.transaction.id(), no_expiry.transaction.id()]
    );

    storage.take_all();
    let last = with_expiry(factory.tx(10_000), Height::MAX);
    insert(&mut storage, &last, Vec::new());
    assert_eq!(
        expire(&mut storage, Height::MAX),
        vec![(last.transaction.id(), RemovalReason::Expired)]
    );
    assert_eq!(storage.transaction_count(), 0);
}

#[test]
fn mining_parent_and_child_preserves_the_unmined_grandchild() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx_with(10_000, 1, 0);
    let grandchild = factory.tx(10_000);
    let parent_id = insert(&mut storage, &parent, Vec::new());
    let child_id = insert(&mut storage, &child, vec![output(parent_id)]);
    insert(&mut storage, &grandchild, vec![output(child_id)]);

    let removed = storage.connect_block(
        &[parent_id, child_id],
        [
            parent.transaction.transaction().as_ref(),
            child.transaction.transaction().as_ref(),
        ],
        Height(1),
        crate::Instant::now(),
    );
    assert_eq!(
        removed,
        BlockRemovals {
            mined: vec![parent.transaction.id(), child.transaction.id()],
            dropped: Vec::new(),
        }
    );
    assert_eq!(storage.pooled_ids(), vec![grandchild.transaction.id()]);
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
    let parent_id = insert(&mut storage, &parent, Vec::new());
    let child_id = insert(&mut storage, &child, vec![output(parent_id)]);
    let grandchild_id = insert(&mut storage, &grandchild, vec![output(child_id)]);
    insert(&mut storage, &survivor, Vec::new());
    let missing = factory.tx(10_000).transaction.id().mined_id();

    let removed: HashSet<_> = storage
        .verified
        .remove_batch([child_id, parent_id, missing, child_id])
        .into_iter()
        .map(|tx| tx.transaction.id().mined_id())
        .collect();
    assert_eq!(removed, HashSet::from([parent_id, child_id, grandchild_id]));
    assert_eq!(storage.pooled_ids(), vec![survivor.transaction.id()]);
    storage.verified.assert_dependency_groups_are_bounded();
}

/// Everything leaves oldest first, and a later insertion still gets a
/// larger stamp than any before it.
#[test]
fn take_all_is_oldest_first_and_stamps_keep_growing() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let txs: Vec<_> = (0..3).map(|_| factory.tx(10_000)).collect();
    // Hashes are random, so storage's map order is not insertion order.
    for tx in txs.iter().rev() {
        insert(&mut storage, tx, Vec::new());
    }
    let before = storage.insertions();
    let taken: Vec<_> = storage
        .take_all()
        .into_iter()
        .map(|tx| tx.transaction.id())
        .collect();
    assert_eq!(
        taken,
        txs.iter()
            .rev()
            .map(|tx| tx.transaction.id())
            .collect::<Vec<_>>()
    );
    let id = insert(&mut storage, &txs[0], Vec::new());
    assert!(storage
        .added_at(&id)
        .is_some_and(|added_at| added_at > before));
}
