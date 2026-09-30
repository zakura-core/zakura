//! Tests for fee-based mempool eviction.

use std::{sync::Arc, thread};

use proptest::{
    collection::vec,
    prelude::*,
    strategy::{Strategy, ValueTree},
    test_runner::TestRunner,
};

use zakura_chain::{
    amount::Amount,
    at_least_one, ironwood,
    orchard::{self, tree},
    parameters::NetworkUpgrade,
    primitives::Halo2Proof,
    transaction::{
        zip317::MARGINAL_FEE, LockTime, Transaction, VerifiedUnminedTx,
        MEMPOOL_TRANSACTION_COST_THRESHOLD,
    },
    transparent::{self, OutPoint},
};

use crate::components::mempool::storage::{policy::p2pkh_lock_script, *};

/// The ZIP-401 cost of every transaction that [`TxFactory`] builds without padding.
const COST: u64 = MEMPOOL_TRANSACTION_COST_THRESHOLD;

/// Builds verified transactions with chosen fees, sizes, and transparent outputs.
struct TxFactory {
    runner: TestRunner,
}

impl TxFactory {
    fn new() -> Self {
        Self {
            runner: TestRunner::deterministic(),
        }
    }

    /// Returns a transaction with cost [`COST`] that pays `fee`.
    fn tx(&mut self, fee: u64) -> VerifiedUnminedTx {
        self.tx_with(fee, 0, 0)
    }

    /// Returns a transaction that pays `fee`, has `outputs` transparent outputs, and is
    /// `padding` bytes larger than the smallest transaction.
    ///
    /// Each transaction reveals a new Ironwood nullifier, so transactions never conflict.
    fn tx_with(&mut self, fee: u64, outputs: usize, padding: usize) -> VerifiedUnminedTx {
        let action = any::<ironwood::Action>()
            .new_tree(&mut self.runner)
            .expect("test action strategy creates a value")
            .current();

        let transaction = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: zakura_chain::block::Height(1_000_000),
            inputs: Vec::new(),
            outputs: (0..outputs)
                .map(|_| transparent::Output {
                    value: Amount::try_from(1_000_000).expect("valid test amount"),
                    lock_script: p2pkh_lock_script(&[0; 20]),
                })
                .collect(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: Some(ironwood::ShieldedData {
                flags: orchard::Flags::ENABLE_SPENDS,
                value_balance: Amount::zero(),
                shared_anchor: tree::Root::default(),
                proof: Halo2Proof(vec![0; 4992 + padding]),
                actions: at_least_one![ironwood::AuthorizedAction {
                    action,
                    spend_auth_sig: [0u8; 64].into(),
                }],
                binding_sig: [0u8; 64].into(),
            }),
        };

        // Construct with a fee that passes the ZIP-317 mempool checks, then set the test fee.
        let mut tx = VerifiedUnminedTx::new(
            Arc::new(transaction).into(),
            Amount::try_from(1_000_000).expect("valid test fee"),
            0,
            0,
            Arc::new(vec![]),
        )
        .expect("test transaction pays the conventional fee");
        tx.miner_fee = Amount::try_from(fee).expect("valid test fee");

        tx
    }
}

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
        Some(SameEffectsTipRejectionError::Evicted.into()),
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
        Err(SameEffectsTipRejectionError::BelowEvictionCost.into())
    );
    assert!(evicted.is_empty());
    assert_eq!(ids(&storage), before);
    assert_eq!(storage.total_cost(), before_cost);
    assert!(before
        .iter()
        .all(|tx_id| storage.rejection_error(tx_id).is_none()));
    assert_eq!(
        storage.rejection_error(&newcomer.transaction.id()),
        Some(SameEffectsTipRejectionError::BelowEvictionCost.into()),
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
        Err(SameEffectsTipRejectionError::BelowEvictionCost.into())
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

#[test]
fn long_chain_evicts_its_cheap_tail() {
    let _init_guard = zakura_test::init();
    const CHAIN_LEN: u64 = 2_000;

    let mut factory = TxFactory::new();
    let mut storage = storage_for(CHAIN_LEN);

    // Build a chain in which every transaction spends the previous one, ending in a cheap tail.
    let mut previous: Option<VerifiedUnminedTx> = None;
    let mut tail_id = None;
    for index in 0..CHAIN_LEN {
        let fee = if index + 1 == CHAIN_LEN {
            10_000
        } else {
            20_000
        };
        let tx = factory.tx_with(fee, 1, 0);
        let spent = previous.iter().map(first_output).collect();
        tail_id = Some(insert(&mut storage, &tx, spent));
        previous = Some(tx);
    }

    let newcomer = factory.tx(10_000 + MARGINAL_FEE);
    let (result, evicted) = storage.insert_with_evicted_ids(newcomer.clone(), vec![], None);

    assert_eq!(result, Ok(newcomer.transaction.id()));
    assert_eq!(evicted, [tail_id.expect("chain is not empty")].into());
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

    let newcomer = factory.tx(20_000 + MARGINAL_FEE);
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
        Err(SameEffectsTipRejectionError::BelowEvictionCost.into())
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
        Err(SameEffectsTipRejectionError::BelowEvictionCost.into())
    );
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
                        MempoolError::from(SameEffectsTipRejectionError::BelowEvictionCost)
                    );
                    prop_assert!(evicted.is_empty());
                    prop_assert_eq!(ids(&storage), before.keys().copied().collect());
                }
            }
        }
    }
}
