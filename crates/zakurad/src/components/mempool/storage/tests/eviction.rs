//! Tests for fee-based mempool eviction.

use std::{cmp::Reverse, sync::Arc, thread};

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
        self, zip317::MARGINAL_FEE, LockTime, Transaction, VerifiedUnminedTx,
        MEMPOOL_TRANSACTION_COST_THRESHOLD,
    },
    transparent::{self, OutPoint},
};

use crate::components::mempool::storage::{
    eviction_cost::EvictionCost,
    policy::p2pkh_lock_script,
    verified_set::{MAX_MEMPOOL_ANCESTORS, MAX_MEMPOOL_PACKAGE_TRANSACTIONS},
    *,
};

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
fn parent_child_pair_evicts_its_cheap_child() {
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
fn second_child_is_rejected_without_changing_the_pool() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let parent = factory.tx_with(10_000, 2, 0);
    insert(&mut storage, &parent, vec![]);
    let child = factory.tx(10_000);
    insert(&mut storage, &child, vec![first_output(&parent)]);
    let before = ids(&storage);
    let sibling = factory.tx(100_000);
    let (result, evicted) =
        storage.insert_with_evicted_ids(sibling, vec![output(&parent, 1)], None);
    assert_eq!(
        result,
        Err(SameEffectsTipRejectionError::TooManyPackageTransactions.into())
    );
    assert!(evicted.is_empty());
    assert_eq!(ids(&storage), before);
}

#[test]
fn joining_two_unconfirmed_parents_is_rejected() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let a = factory.tx_with(10_000, 1, 0);
    let b = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &a, vec![]);
    insert(&mut storage, &b, vec![]);
    let before = ids(&storage);
    let joined = factory.tx(100_000);
    let (result, evicted) =
        storage.insert_with_evicted_ids(joined, vec![first_output(&a), first_output(&b)], None);
    assert_eq!(
        result,
        Err(SameEffectsTipRejectionError::TooManyAncestors.into())
    );
    assert!(evicted.is_empty());
    assert_eq!(ids(&storage), before);
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
    assert_eq!(
        storage.transaction_count(),
        MAX_MEMPOOL_PACKAGE_TRANSACTIONS
    );
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn mining_a_parent_allows_its_child_to_become_a_parent() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let parent = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &parent, vec![]);
    let child = factory.tx_with(10_000, 1, 0);
    insert(&mut storage, &child, vec![first_output(&parent)]);
    let mined = [parent.transaction.id().mined_id()].into();
    storage.clear_mined_dependencies(&mined);
    storage.reject_and_remove_same_effects(&mined, vec![]);
    let next = factory.tx(10_000);
    insert(&mut storage, &next, vec![first_output(&child)]);
    storage.verified.assert_dependency_groups_are_bounded();
}

#[test]
fn removing_a_child_releases_its_parents_child_slot() {
    let _init_guard = zakura_test::init();
    let mut factory = TxFactory::new();
    let mut storage = storage_for(10);
    let parent = factory.tx_with(10_000, 2, 0);
    insert(&mut storage, &parent, vec![]);
    let child = factory.tx(10_000);
    insert(&mut storage, &child, vec![first_output(&parent)]);
    assert_eq!(storage.remove_exact(&[child.transaction.id()].into()), 1);
    let next = factory.tx(10_000);
    insert(&mut storage, &next, vec![output(&parent, 1)]);
    storage.verified.assert_dependency_groups_are_bounded();
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
    let incoming = VerifiedSet::eviction_cost(incoming);
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

        if !incoming.exceeds_by_increment(score) {
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
