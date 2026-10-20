//! Tests for fee-based mempool eviction.

use std::{cmp::Reverse, sync::Arc, thread};

use proptest::{collection::vec, prelude::*};

use zakura_chain::{
    transaction::{
        self, zip317::MARGINAL_FEE, Transaction, VerifiedUnminedTx,
        MEMPOOL_TRANSACTION_COST_THRESHOLD,
    },
    transparent::{self, OutPoint},
};

use crate::components::mempool::storage::{
    eviction_cost::EvictionCost,
    fixtures::TxFactory,
    verified_set::{MAX_MEMPOOL_ANCESTORS, MAX_MEMPOOL_PACKAGE_TRANSACTIONS},
    *,
};

/// The ZIP-401 cost of every transaction that [`TxFactory`] builds without padding.
const COST: u64 = MEMPOOL_TRANSACTION_COST_THRESHOLD;

/// Returns a storage whose cost limit fits `count` transactions of cost [`COST`].
fn storage_for(count: u64) -> Storage {
    Storage::new(&config::Config {
        tx_cost_limit: count * COST,
        ..Default::default()
    })
}

/// Inserts `tx` into `storage` and returns its id, panicking if the insert fails.
fn insert(storage: &mut Storage, tx: &VerifiedUnminedTx, spent: Vec<OutPoint>) -> UnminedTxId {
    storage
        .insert(tx.clone(), spent, None)
        .expect("test transaction fits in the mempool")
}

/// Returns the ids of the transactions in `storage`.
fn ids(storage: &Storage) -> HashSet<UnminedTxId> {
    storage.tx_ids().collect()
}

/// Returns the first transparent outpoint that `tx` creates.
fn first_output(tx: &VerifiedUnminedTx) -> OutPoint {
    OutPoint::from_usize(tx.transaction.id().mined_id(), 0)
}

#[test]
fn full_mempool_evicts_lowest_fee_transaction() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);

    let a = insert(&mut storage, &factory.tx(20_000), vec![]);
    let b = insert(&mut storage, &factory.tx(10_000), vec![]);
    let c = insert(&mut storage, &factory.tx(30_000), vec![]);

    let newcomer = factory.tx(10_000 + MARGINAL_FEE);
    let (result, evicted) = storage.insert_with_evicted_ids(newcomer.clone(), vec![], None);

    assert_eq!(result, Ok(newcomer.transaction.id()));
    assert_eq!(evicted, [b].into());
    assert_eq!(ids(&storage), [a, c, newcomer.transaction.id()].into());
    assert_eq!(
        storage.rejection_error(&b),
        Some(ExactTipRejectionError::Evicted.into()),
    );
}

#[test]
fn full_mempool_rejects_fee_within_increment_without_changes() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);

    for fee in [20_000, 10_000, 30_000] {
        insert(&mut storage, &factory.tx(fee), vec![]);
    }
    let before = ids(&storage);
    let before_cost = storage.total_cost();

    let newcomer = factory.tx(10_000 + MARGINAL_FEE - 1);
    let (result, evicted) = storage.insert_with_evicted_ids(newcomer.clone(), vec![], None);

    assert_eq!(
        result,
        Err(ExactTipRejectionError::BelowEvictionCost.into())
    );
    assert!(evicted.is_empty());
    assert_eq!(ids(&storage), before);
    assert_eq!(storage.total_cost(), before_cost);
    assert!(before
        .iter()
        .all(|tx_id| storage.rejection_error(tx_id).is_none()));
    assert_eq!(
        storage.rejection_error(&newcomer.transaction.id()),
        Some(ExactTipRejectionError::BelowEvictionCost.into()),
    );
}

#[test]
fn transaction_that_needs_its_ancestors_evicted_is_rejected() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(2);

    let parent = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &parent, vec![]);
    let before = ids(&storage);

    // The newcomer only fits if it evicts its own parent.
    let newcomer = factory.tx_with(1_000_000, 0, 9_000);
    assert!(newcomer.cost() > COST && newcomer.cost() <= 2 * COST);
    let (result, evicted) =
        storage.insert_with_evicted_ids(newcomer, vec![first_output(&parent)], None);

    assert_eq!(
        result,
        Err(ExactTipRejectionError::BelowEvictionCost.into())
    );
    assert!(evicted.is_empty());
    assert_eq!(ids(&storage), before);
}

#[test]
fn high_fee_child_protects_low_fee_parent() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);

    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx(50_000);
    let other = factory.tx(20_000);
    let parent_id = insert(&mut storage, &parent, vec![]);
    let child_id = insert(&mut storage, &child, vec![first_output(&parent)]);
    let other_id = insert(&mut storage, &other, vec![]);

    // The parent package pays 60,000 over two costs, which beats the other transaction.
    let newcomer = factory.tx(20_000 + MARGINAL_FEE);
    let (result, evicted) = storage.insert_with_evicted_ids(newcomer.clone(), vec![], None);

    assert_eq!(result, Ok(newcomer.transaction.id()));
    assert_eq!(evicted, [other_id].into());
    assert!(ids(&storage).is_superset(&[parent_id, child_id].into()));
}

#[test]
fn low_fee_child_is_evicted_without_its_parent() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);

    let parent = factory.tx_with(50_000, 1, 0);
    let child = factory.tx(10_000);
    let other = factory.tx(20_000);
    let parent_id = insert(&mut storage, &parent, vec![]);
    let child_id = insert(&mut storage, &child, vec![first_output(&parent)]);
    let other_id = insert(&mut storage, &other, vec![]);

    let newcomer = factory.tx(10_000 + MARGINAL_FEE);
    let (result, evicted) = storage.insert_with_evicted_ids(newcomer.clone(), vec![], None);

    assert_eq!(result, Ok(newcomer.transaction.id()));
    assert_eq!(evicted, [child_id].into());
    assert!(ids(&storage).is_superset(&[parent_id, other_id].into()));
}

#[test]
fn evicting_a_parent_evicts_its_descendants() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);

    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx(10_000);
    let other = factory.tx(100_000);
    let parent_id = insert(&mut storage, &parent, vec![]);
    // Give the child a later insertion time.
    thread::sleep(Duration::from_millis(2));
    let child_id = insert(&mut storage, &child, vec![first_output(&parent)]);
    let other_id = insert(&mut storage, &other, vec![]);

    let newcomer = factory.tx(10_000 + MARGINAL_FEE);
    let (result, evicted) = storage.insert_with_evicted_ids(newcomer.clone(), vec![], None);

    // Both transactions pay the same rate, so the newest one goes first. Evicting the child
    // frees enough space.
    assert_eq!(result, Ok(newcomer.transaction.id()));
    assert_eq!(evicted, [child_id].into());
    assert!(ids(&storage).is_superset(&[parent_id, other_id].into()));

    // A larger newcomer must also evict the parent, which is now alone.
    let large = factory.tx_with(1_000_000, 0, 10_000);
    let (result, evicted) = storage.insert_with_evicted_ids(large.clone(), vec![], None);

    assert_eq!(result, Ok(large.transaction.id()));
    assert!(evicted.contains(&parent_id));
    assert!(storage.total_cost() <= 3 * COST);
}

/// Inserts a chain of `len` transactions that each spend the previous one, and returns the
/// last transaction.
fn insert_chain(
    storage: &mut Storage,
    factory: &mut TxFactory,
    len: usize,
    fee: impl Fn(usize) -> u64,
) -> VerifiedUnminedTx {
    let mut previous: Option<VerifiedUnminedTx> = None;
    for index in 0..len {
        let tx = factory.tx_with(fee(index), 1, 0);
        let spent = previous.iter().map(first_output).collect();
        insert(storage, &tx, spent);
        previous = Some(tx);
    }

    previous.expect("chain is not empty")
}

#[test]
fn three_transaction_chain_evicts_its_cheap_tail() {
    let _init_guard = zakura_test::init();
    const CHAIN_LEN: usize = MAX_MEMPOOL_ANCESTORS + 1;

    let mut factory = TxFactory::new();
    let mut storage = storage_for(CHAIN_LEN as u64);

    // The chain ends in a cheap tail.
    let tail = insert_chain(&mut storage, &mut factory, CHAIN_LEN, |index| {
        if index + 1 == CHAIN_LEN {
            10_000
        } else {
            20_000
        }
    });
    storage.verified.assert_dependency_groups_are_bounded();

    let newcomer = factory.tx(10_000 + MARGINAL_FEE);
    let (result, evicted) = storage.insert_with_evicted_ids(newcomer.clone(), vec![], None);

    assert_eq!(result, Ok(newcomer.transaction.id()));
    assert_eq!(evicted, [tail.transaction.id()].into());
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn transaction_with_too_many_ancestors_is_rejected() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(1_000);

    // The last transaction of the chain has the most ancestors that the mempool allows.
    let tail = insert_chain(
        &mut storage,
        &mut factory,
        MAX_MEMPOOL_ANCESTORS + 1,
        |_| 10_000,
    );
    let before = ids(&storage);

    let child = factory.tx(10_000);
    assert_eq!(
        storage.insert(child, vec![first_output(&tail)], None),
        Err(SameEffectsTipRejectionError::TooManyAncestors.into())
    );
    assert_eq!(ids(&storage), before);
}

#[test]
fn newcomer_never_evicts_its_ancestors() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);

    let parent = factory.tx_with(10_000, 1, 0);
    let parent_id = insert(&mut storage, &parent, vec![]);
    let other = insert(&mut storage, &factory.tx(20_000), vec![]);
    insert(&mut storage, &factory.tx(30_000), vec![]);

    // The child brings both itself and its parent above the victim plus increment.
    let newcomer = factory.tx(2 * (20_000 + MARGINAL_FEE) - 10_000);
    let (result, evicted) =
        storage.insert_with_evicted_ids(newcomer.clone(), vec![first_output(&parent)], None);

    assert_eq!(result, Ok(newcomer.transaction.id()));
    assert_eq!(evicted, [other].into());
    assert!(ids(&storage).contains(&parent_id));
}

#[test]
fn equal_eviction_costs_evict_the_newest_transaction() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);

    let older = insert(&mut storage, &factory.tx(10_000), vec![]);
    // Give the second transaction a later insertion time.
    thread::sleep(Duration::from_millis(2));
    let newer = insert(&mut storage, &factory.tx(10_000), vec![]);
    insert(&mut storage, &factory.tx(40_000), vec![]);

    let newcomer = factory.tx(10_000 + MARGINAL_FEE);
    let (result, evicted) = storage.insert_with_evicted_ids(newcomer, vec![], None);

    assert!(result.is_ok());
    assert_eq!(evicted, [newer].into());
    assert!(ids(&storage).contains(&older));
}

#[test]
fn rejections_clear_when_a_block_frees_space() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(2);

    let mined = factory.tx(20_000);
    let mined_id = insert(&mut storage, &mined, vec![]);
    let cheap = factory.tx(10_000);
    let cheap_id = insert(&mut storage, &cheap, vec![]);

    // The newcomer evicts the cheap transaction, and a second cheap transaction is rejected.
    insert(&mut storage, &factory.tx(10_000 + MARGINAL_FEE), vec![]);
    let rejected = factory.tx(10_000);
    assert_eq!(
        storage.insert(rejected.clone(), vec![], None),
        Err(ExactTipRejectionError::BelowEvictionCost.into())
    );
    assert!(storage.contains_rejected(&cheap_id));

    // A block mines one transaction, which frees space and clears the tip rejections.
    storage.reject_and_remove_same_effects(&[mined_id.mined_id()].into(), vec![]);
    storage.clear_tip_rejections();

    assert_eq!(storage.insert(cheap.clone(), vec![], None), Ok(cheap_id));

    // The mempool is full again, so the rejected transaction still needs to pay more than
    // the cheapest transaction.
    assert_eq!(
        storage.insert(rejected, vec![], None),
        Err(ExactTipRejectionError::BelowEvictionCost.into())
    );
}

/// Returns the outpoint at `index` that `tx` creates.
fn output(tx: &VerifiedUnminedTx, index: usize) -> OutPoint {
    OutPoint::from_usize(tx.transaction.id().mined_id(), index)
}

#[test]
fn third_child_is_rejected_without_changing_the_pool() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let parent = factory.tx_with(10_000, MAX_MEMPOOL_PACKAGE_TRANSACTIONS, 0);
    insert(&mut storage, &parent, vec![]);
    for index in 0..MAX_MEMPOOL_ANCESTORS {
        let child = factory.tx(10_000);
        insert(&mut storage, &child, vec![output(&parent, index)]);
    }
    let before = ids(&storage);
    let sibling = factory.tx(100_000);
    let (result, evicted) = storage.insert_with_evicted_ids(
        sibling,
        vec![output(&parent, MAX_MEMPOOL_ANCESTORS)],
        None,
    );
    assert_eq!(
        result,
        Err(SameEffectsTipRejectionError::TooManyPackageTransactions.into())
    );
    assert!(evicted.is_empty());
    assert_eq!(ids(&storage), before);
}

#[test]
fn joining_two_unconfirmed_parents_is_accepted() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let a = factory.tx_with(10_000, 1, 0);
    let b = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &a, vec![]);
    insert(&mut storage, &b, vec![]);
    let joined = factory.tx(100_000);
    insert(
        &mut storage,
        &joined,
        vec![first_output(&a), first_output(&b)],
    );
    assert_eq!(
        storage.transaction_count(),
        MAX_MEMPOOL_PACKAGE_TRANSACTIONS
    );
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn joining_groups_counts_existing_children_and_rejects_atomically() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let a = factory.tx_with(10_000, 2, 0);
    let b = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &a, vec![]);
    insert(&mut storage, &b, vec![]);
    let child = factory.tx(10_000);
    insert(&mut storage, &child, vec![first_output(&a)]);
    let before = ids(&storage);
    let joined = factory.tx(100_000);
    let (result, evicted) =
        storage.insert_with_evicted_ids(joined, vec![output(&a, 1), first_output(&b)], None);
    assert_eq!(
        result,
        Err(SameEffectsTipRejectionError::TooManyPackageTransactions.into())
    );
    assert!(evicted.is_empty());
    assert_eq!(ids(&storage), before);
}

#[test]
fn triangle_counts_shared_grandchild_once() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(4);
    let parent = factory.tx_with(10_000, 2, 0);
    let child = factory.tx_with(10_000, 1, 0);
    let grandchild = factory.tx(100_000);
    let parent_id = insert(&mut storage, &parent, vec![]);
    let child_id = insert(&mut storage, &child, vec![first_output(&parent)]);
    let grandchild_id = insert(
        &mut storage,
        &grandchild,
        vec![output(&parent, 1), first_output(&child)],
    );
    insert(&mut storage, &factory.tx(100_000), vec![]);
    storage.verified.assert_dependency_groups_are_bounded();
    // All three possible edges exist. The package pays 120,000 / 3 = 40,000,
    // despite the grandchild being reachable through two paths.
    let before = ids(&storage);
    let underpaying = factory.tx(40_000 + MARGINAL_FEE - 1);
    let (result, evicted) = storage.insert_with_evicted_ids(underpaying, vec![], None);
    assert_eq!(
        result,
        Err(ExactTipRejectionError::BelowEvictionCost.into())
    );
    assert!(evicted.is_empty());
    assert_eq!(ids(&storage), before);
    let newcomer = factory.tx(40_000 + MARGINAL_FEE);
    let (result, evicted) = storage.insert_with_evicted_ids(newcomer.clone(), vec![], None);
    assert_eq!(result, Ok(newcomer.transaction.id()));
    assert_eq!(evicted, [parent_id, child_id, grandchild_id].into());
}

#[test]
fn spending_multiple_outputs_of_one_parent_is_accepted() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let parent = factory.tx_with(10_000, 2, 0);
    insert(&mut storage, &parent, vec![]);
    let child = factory.tx(10_000);
    insert(
        &mut storage,
        &child,
        vec![output(&parent, 0), output(&parent, 1)],
    );
    assert_eq!(storage.transaction_count(), 2);
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn mining_a_parent_releases_capacity_for_another_grandchild() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let parent = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &parent, vec![]);
    let child = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &child, vec![first_output(&parent)]);
    let grandchild = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &grandchild, vec![first_output(&child)]);
    let mined = [parent.transaction.id().mined_id()].into();
    storage.clear_mined_dependencies(&mined);
    storage.reject_and_remove_same_effects(&mined, vec![]);
    let next = factory.tx(10_000);
    insert(&mut storage, &next, vec![first_output(&grandchild)]);
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn removing_a_grandchild_releases_group_capacity() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let parent = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &parent, vec![]);
    let child = factory.tx_with(10_000, 2, 0);
    insert(&mut storage, &child, vec![first_output(&parent)]);
    let grandchild = factory.tx(10_000);
    insert(&mut storage, &grandchild, vec![first_output(&child)]);
    assert_eq!(
        storage.remove_exact(&[grandchild.transaction.id()].into()),
        1
    );
    let next = factory.tx(10_000);
    insert(&mut storage, &next, vec![output(&child, 1)]);
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn higher_fee_conflicting_transaction_does_not_replace_or_evict() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let outpoint = OutPoint::from_usize(transaction::Hash([7; 32]), 0);
    // Neither final sequence numbers nor Bitcoin-style replacement signaling
    // permit replacing an existing spend in this mempool.
    for sequence in [u32::MAX, u32::MAX - 2] {
        let spending = |mut tx: VerifiedUnminedTx| {
            let mut transaction = Transaction::clone(tx.transaction.transaction());
            let Transaction::V6 { inputs, .. } = &mut transaction else {
                unreachable!("TxFactory builds V6 transactions");
            };
            inputs.push(transparent::Input::PrevOut {
                outpoint,
                unlock_script: transparent::Script::new(&[]),
                sequence,
            });
            tx.transaction = Arc::new(transaction).into();
            tx
        };
        let mut storage = storage_for(3);
        let original = spending(factory.tx_with(10_000, 1, 0));
        insert(&mut storage, &original, vec![]);
        let child = factory.tx_with(10_000, 1, 0);
        insert(&mut storage, &child, vec![first_output(&original)]);
        let grandchild = factory.tx(10_000);
        insert(&mut storage, &grandchild, vec![first_output(&child)]);
        let before = ids(&storage);
        let replacement = spending(factory.tx_with(1_000_000, 1, 0));
        let (result, evicted) = storage.insert_with_evicted_ids(replacement, vec![], None);
        assert_eq!(
            result,
            Err(SameEffectsTipRejectionError::SpendConflict.into())
        );
        assert!(evicted.is_empty());
        assert_eq!(ids(&storage), before);
    }
}

/// Supported ways to connect a newcomer to an existing two-member group.
#[derive(Clone, Copy)]
enum AdmissionShape {
    Chain,
    Branch,
    Join,
    Triangle,
}

/// Fills a pool with the two-member group and one independent eviction victim.
fn ancestor_admission_fixture(
    factory: &mut TxFactory,
    shape: AdmissionShape,
    capacity: u64,
) -> (Storage, Vec<OutPoint>, UnminedTxId, u64) {
    let mut storage = storage_for(capacity);
    let parent = factory.tx_with(COST, 2, 0);
    insert(&mut storage, &parent, vec![]);
    let child_fee = match shape {
        AdmissionShape::Branch => 10 * COST,
        _ => COST,
    };
    let child = factory.tx_with(child_fee, 1, 0);
    let spent = match shape {
        AdmissionShape::Join => vec![],
        _ => vec![first_output(&parent)],
    };
    insert(&mut storage, &child, spent);
    let victim = insert(&mut storage, &factory.tx(COST + 2 * MARGINAL_FEE), vec![]);
    let spent = match shape {
        AdmissionShape::Chain => vec![first_output(&child)],
        AdmissionShape::Branch => vec![output(&parent, 1)],
        AdmissionShape::Join => vec![first_output(&parent), first_output(&child)],
        AdmissionShape::Triangle => vec![output(&parent, 1), first_output(&child)],
    };
    let ancestor_count = match shape {
        AdmissionShape::Branch => 1,
        _ => 2,
    };
    let package_fee = (ancestor_count + 1) * (COST + 3 * MARGINAL_FEE);
    let required_fee = package_fee - ancestor_count * COST;
    (storage, spent, victim, required_fee)
}

#[test]
fn ancestor_package_rate_must_outbid_victims_for_every_group_shape() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    for shape in [
        AdmissionShape::Chain,
        AdmissionShape::Branch,
        AdmissionShape::Join,
        AdmissionShape::Triangle,
    ] {
        let (mut storage, spent, victim, required_fee) =
            ancestor_admission_fixture(&mut factory, shape, 3);
        let before = ids(&storage);
        let before_cost = storage.total_cost();
        // Both rejected children pass the old child-only admission check.
        for fee in [COST + 3 * MARGINAL_FEE, required_fee - 1] {
            let child = factory.tx(fee);
            assert!(VerifiedSet::eviction_cost(&child)
                .exceeds_by_increment(EvictionCost::new(COST + 2 * MARGINAL_FEE, COST)));
            let (result, evicted) = storage.insert_with_evicted_ids(child, spent.clone(), None);
            assert_eq!(
                result,
                Err(ExactTipRejectionError::BelowEvictionCost.into())
            );
            assert!(evicted.is_empty());
            assert_eq!(ids(&storage), before);
            assert_eq!(storage.total_cost(), before_cost);
            storage.verified.assert_dependency_groups_are_bounded();
        }
        let child = factory.tx(required_fee);
        let (result, evicted) = storage.insert_with_evicted_ids(child.clone(), spent, None);
        assert_eq!(result, Ok(child.transaction.id()));
        assert_eq!(evicted, [victim].into());
        storage.verified.assert_dependency_groups_are_bounded();
    }
}

#[test]
fn ancestor_package_pricing_does_not_apply_with_spare_capacity() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    for shape in [
        AdmissionShape::Chain,
        AdmissionShape::Branch,
        AdmissionShape::Join,
        AdmissionShape::Triangle,
    ] {
        let (mut storage, spent, _, _) = ancestor_admission_fixture(&mut factory, shape, 4);
        let before = ids(&storage);
        let child = factory.tx(COST);
        let (result, evicted) = storage.insert_with_evicted_ids(child.clone(), spent, None);
        assert_eq!(result, Ok(child.transaction.id()));
        assert!(evicted.is_empty());
        assert!(ids(&storage).is_superset(&before));
        storage.verified.assert_dependency_groups_are_bounded();
    }
}

#[test]
fn rich_ancestors_do_not_subsidize_an_underpaying_child() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);
    let tail = insert_chain(&mut storage, &mut factory, 2, |_| 10 * COST);
    let victim = insert(&mut storage, &factory.tx(COST), vec![]);
    let before = ids(&storage);
    let child = factory.tx(COST + MARGINAL_FEE - 1);
    let (result, evicted) = storage.insert_with_evicted_ids(child, vec![first_output(&tail)], None);
    assert_eq!(
        result,
        Err(ExactTipRejectionError::BelowEvictionCost.into())
    );
    assert!(evicted.is_empty());
    assert_eq!(ids(&storage), before);
    let child = factory.tx(COST + MARGINAL_FEE);
    let (result, evicted) =
        storage.insert_with_evicted_ids(child.clone(), vec![first_output(&tail)], None);
    assert_eq!(result, Ok(child.transaction.id()));
    assert_eq!(evicted, [victim].into());
}

#[test]
fn ancestor_package_must_outbid_every_victim_before_any_eviction() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);
    let parent = factory.tx_with(COST, 1, 0);
    insert(&mut storage, &parent, vec![]);
    let cheap = insert(&mut storage, &factory.tx(COST), vec![]);
    let expensive = insert(&mut storage, &factory.tx(2 * COST), vec![]);
    let probe = factory.tx_with(10 * COST, 0, 9_000);
    assert!(probe.cost() > COST && probe.cost() <= 2 * COST);
    let victim_rate = 2 * COST + MARGINAL_FEE;
    let own_fee = (victim_rate * probe.cost()).div_ceil(COST);
    let package_fee = (victim_rate * (probe.cost() + parent.cost())).div_ceil(COST);
    let required_fee = package_fee - u64::from(parent.miner_fee);
    let before = ids(&storage);
    for fee in [own_fee, required_fee - 1] {
        let child = factory.tx_with(fee, 0, 9_000);
        let (result, evicted) =
            storage.insert_with_evicted_ids(child, vec![first_output(&parent)], None);
        assert_eq!(
            result,
            Err(ExactTipRejectionError::BelowEvictionCost.into())
        );
        assert!(evicted.is_empty());
        assert_eq!(ids(&storage), before);
        storage.verified.assert_dependency_groups_are_bounded();
    }
    let child = factory.tx_with(required_fee, 0, 9_000);
    let (result, evicted) =
        storage.insert_with_evicted_ids(child.clone(), vec![first_output(&parent)], None);
    assert_eq!(result, Ok(child.transaction.id()));
    assert_eq!(evicted, [cheap, expensive].into());
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn rebuilding_an_evicted_chain_requires_a_higher_package_rate() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(3);
    let mut standalone_fee = COST + 2 * MARGINAL_FEE;
    insert(&mut storage, &factory.tx(standalone_fee), vec![]);
    for _ in 0..5 {
        let tail = insert_chain(&mut storage, &mut factory, 2, |_| COST);
        let package_rate = standalone_fee + MARGINAL_FEE;
        let required_fee = 3 * package_rate - 2 * COST;
        let insufficient = factory.tx(required_fee - 1);
        let before = ids(&storage);
        let (result, evicted) =
            storage.insert_with_evicted_ids(insufficient, vec![first_output(&tail)], None);
        assert_eq!(
            result,
            Err(ExactTipRejectionError::BelowEvictionCost.into())
        );
        assert!(evicted.is_empty());
        assert_eq!(ids(&storage), before);
        let child = factory.tx(required_fee);
        assert!(storage
            .insert(child, vec![first_output(&tail)], None)
            .is_ok());
        let old_fee = standalone_fee;
        standalone_fee = package_rate + MARGINAL_FEE;
        assert!(standalone_fee > old_fee);
        let (result, evicted) =
            storage.insert_with_evicted_ids(factory.tx(standalone_fee), vec![], None);
        assert!(result.is_ok());
        assert_eq!(evicted.len(), MAX_MEMPOOL_PACKAGE_TRANSACTIONS);
        assert_eq!(storage.transaction_count(), 1);
        storage.verified.assert_dependency_groups_are_bounded();
    }
}

/// Returns `tx` with `padding` more bytes of authorizing data.
///
/// The padded transaction has the same mined ID and fee, but a different witnessed ID and a
/// higher cost.
fn pad_authorizing_data(tx: &VerifiedUnminedTx, padding: usize) -> VerifiedUnminedTx {
    let mut transaction = Transaction::clone(tx.transaction.transaction());
    let Transaction::V6 {
        ironwood_shielded_data: Some(shielded_data),
        ..
    } = &mut transaction
    else {
        unreachable!("TxFactory builds V6 transactions with Ironwood data");
    };
    shielded_data
        .proof
        .0
        .extend(std::iter::repeat_n(0, padding));

    let mut padded = tx.clone();
    padded.transaction = Arc::new(transaction).into();
    padded
}

#[test]
fn rejecting_a_padded_variant_does_not_reject_the_original() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(2);

    insert(&mut storage, &factory.tx(10_000), vec![]);
    insert(&mut storage, &factory.tx(10_000), vec![]);

    let original = factory.tx(10_000 + MARGINAL_FEE);
    let padded = pad_authorizing_data(&original, 10_000);
    assert_eq!(
        padded.transaction.id().mined_id(),
        original.transaction.id().mined_id()
    );
    assert_ne!(padded.transaction.id(), original.transaction.id());
    assert!(padded.cost() > original.cost());

    // The padded variant pays less per unit of cost, so the full mempool rejects it.
    assert_eq!(
        storage.insert(padded, vec![], None),
        Err(ExactTipRejectionError::BelowEvictionCost.into())
    );
    assert_eq!(
        storage.insert(original.clone(), vec![], None),
        Ok(original.transaction.id())
    );
}

/// Returns the eviction victims or admission error for `incoming`, recomputing
/// every package from scratch for each victim.
fn reference_eviction(
    storage: &Storage,
    incoming: &VerifiedUnminedTx,
    parents: &[transaction::Hash],
) -> Result<HashSet<transaction::Hash>, MempoolError> {
    let transactions = storage.verified.transactions();
    let dependencies = storage.verified.transaction_dependencies().dependencies();
    let dependents = storage.verified.transaction_dependencies().dependents();

    let walk = |start: transaction::Hash,
                edges: &HashMap<transaction::Hash, HashSet<transaction::Hash>>,
                skip: &HashSet<transaction::Hash>| {
        let mut found = HashSet::new();
        let mut pending = vec![start];
        while let Some(tx_id) = pending.pop() {
            for &next in edges.get(&tx_id).into_iter().flatten() {
                if !skip.contains(&next) && found.insert(next) {
                    pending.push(next);
                }
            }
        }
        found
    };

    let mut unavailable = HashSet::new();
    for &parent in parents {
        unavailable.insert(parent);
        unavailable.extend(walk(parent, dependencies, &HashSet::new()));
    }

    if unavailable.len() > MAX_MEMPOOL_ANCESTORS {
        return Err(SameEffectsTipRejectionError::TooManyAncestors.into());
    }
    let mut connected = dependencies.clone();
    for (&parent, children) in dependents {
        connected.entry(parent).or_default().extend(children);
    }
    let mut group = HashSet::new();
    for &parent in parents {
        group.insert(parent);
        group.extend(walk(parent, &connected, &HashSet::new()));
    }
    if group.len() >= MAX_MEMPOOL_PACKAGE_TRANSACTIONS {
        return Err(SameEffectsTipRejectionError::TooManyPackageTransactions.into());
    }

    let needed = (storage.total_cost() + incoming.cost()).saturating_sub(storage.tx_cost_limit);
    let own_fee = u64::from(incoming.miner_fee);
    let own_cost = incoming.cost();
    let ancestor_fee: u64 = unavailable
        .iter()
        .map(|id| u64::from(transactions[id].miner_fee))
        .sum();
    let ancestor_cost: u64 = unavailable.iter().map(|id| transactions[id].cost()).sum();
    let mut evicted = HashSet::new();
    let mut freed = 0;

    while freed < needed {
        let (score, _, _, package) = transactions
            .iter()
            .filter(|(tx_id, _)| !unavailable.contains(*tx_id))
            .map(|(&tx_id, tx)| {
                let mut package = walk(tx_id, dependents, &unavailable);
                package.insert(tx_id);
                let combined = package
                    .iter()
                    .map(|member| VerifiedSet::eviction_cost(&transactions[member]))
                    .reduce(EvictionCost::combine)
                    .expect("the package contains its root");
                let score = VerifiedSet::eviction_cost(tx).max(combined);
                (score, Reverse(tx.time), tx_id, package)
            })
            .min_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)))
            .ok_or(ExactTipRejectionError::BelowEvictionCost)?;

        // Independently check both rates without production combine/min helpers.
        let outbids =
            |fee: u64, cost: u64| EvictionCost::new(fee, cost).exceeds_by_increment(score);
        if !outbids(own_fee, own_cost) || !outbids(own_fee + ancestor_fee, own_cost + ancestor_cost)
        {
            return Err(ExactTipRejectionError::BelowEvictionCost.into());
        }

        freed += package
            .iter()
            .map(|member| transactions[member].cost())
            .sum::<u64>();
        unavailable.extend(&package);
        evicted.extend(package);
    }

    Ok(evicted)
}

proptest! {
    /// After any sequence of inserts, the mempool stays under its cost limit, every eviction
    /// is paid for, and every rejection leaves the mempool unchanged.
    #[test]
    fn eviction_is_paid_for_and_rejection_is_atomic(
        txs in vec((1_000..100_000_u64, 0..20_000_usize), 1..24),
        limit_txs in 1..6_u64,
    ) {
        let _init_guard = zakura_test::init();
        let mut factory = TxFactory::new();
        let mut storage = storage_for(limit_txs);

        for (fee, padding) in txs {
            let tx = factory.tx_with(fee, 0, padding);
            let incoming = VerifiedSet::eviction_cost(&tx);
            let before: HashMap<_, _> = storage
                .transactions()
                .values()
                .map(|tx| (tx.transaction.id(), VerifiedSet::eviction_cost(tx)))
                .collect();

            let (result, evicted) = storage.insert_with_evicted_ids(tx, vec![], None);

            prop_assert!(storage.total_cost() <= limit_txs * COST);

            match result {
                Ok(_) => {
                    for evicted_id in &evicted {
                        prop_assert!(incoming.exceeds_by_increment(before[evicted_id]));
                    }
                }
                Err(error) => {
                    prop_assert_eq!(
                        error,
                        MempoolError::from(ExactTipRejectionError::BelowEvictionCost)
                    );
                    prop_assert!(evicted.is_empty());
                    prop_assert_eq!(ids(&storage), before.keys().copied().collect());
                }
            }
        }
    }

    /// Admission and eviction match a brute-force reference on random graphs.
    /// Dependency groups stay bounded after inserts, evictions, and mining.
    #[test]
    fn eviction_matches_reference_on_random_graphs(
        steps in vec(
            (
                0..8_u8,
                1_000..200_000_u64,
                0..20_000_usize,
                vec(any::<prop::sample::Index>(), 0..3),
            ),
            1..32,
        ),
        limit_txs in 2..10_u64,
    ) {
        let _init_guard = zakura_test::init();
        let mut factory = TxFactory::new();
        let mut storage = storage_for(limit_txs);

        for (kind, fee, padding, parent_picks) in steps {
            let mut in_pool: Vec<_> = storage.verified.transactions().keys().copied().collect();
            in_pool.sort();

            if kind == 0 && !in_pool.is_empty() {
                // Mine a transaction that has no mempool parents.
                let roots: Vec<_> = in_pool
                    .iter()
                    .filter(|tx_id| {
                        storage
                            .verified
                            .transaction_dependencies()
                            .direct_dependencies(tx_id)
                            .is_empty()
                    })
                    .copied()
                    .collect();
                let mined = roots[parent_picks.first().map_or(0, |pick| pick.index(roots.len()))];
                let mined: HashSet<_> = [mined].into();
                storage.clear_mined_dependencies(&mined);
                storage.reject_and_remove_same_effects(&mined, vec![]);
                storage.verified.assert_dependency_groups_are_bounded();
                continue;
            }

            let mut parents: Vec<_> = parent_picks
                .iter()
                .filter(|_| !in_pool.is_empty())
                .map(|pick| in_pool[pick.index(in_pool.len())])
                .collect();
            parents.sort();
            parents.dedup();
            let spent: Vec<_> = parents
                .iter()
                .map(|&parent| OutPoint::from_usize(parent, 0))
                .collect();

            let tx = factory.tx_with(fee, 1, padding);
            let expected = reference_eviction(&storage, &tx, &parents);
            let before = ids(&storage);

            let (result, evicted) = storage.insert_with_evicted_ids(tx.clone(), spent, None);
            storage.verified.assert_dependency_groups_are_bounded();
            prop_assert!(storage.total_cost() <= limit_txs * COST);

            match expected {
                Ok(expected) => {
                    prop_assert_eq!(result, Ok(tx.transaction.id()));
                    let evicted: HashSet<_> =
                        evicted.iter().map(UnminedTxId::mined_id).collect();
                    prop_assert_eq!(evicted, expected);
                }
                Err(error) => {
                    prop_assert_eq!(result, Err(error));
                    prop_assert!(evicted.is_empty());
                    prop_assert_eq!(ids(&storage), before);
                }
            }
        }
    }
}
