//! Fixed test vectors for mempool storage.

#![allow(clippy::unwrap_in_result)]

use std::iter;

use eyre::Result;

use transparent::OutPoint;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::Network,
};

use zakura_chain::transparent;

use super::{ExactTipRejectionError, MempoolError, Storage};
use crate::storage::*;

/// Eviction memory time used for tests. Most tests won't care about this
/// so we use a large enough value that will never be reached in the tests.
const EVICTION_MEMORY_TIME: Duration = Duration::from_secs(60 * 60);

/// Transaction count used in some tests to derive the mempool test size.
const MEMPOOL_TX_COUNT: usize = 4;

fn ironwood_action() -> zakura_chain::ironwood::Action {
    use proptest::{
        prelude::any,
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };

    let mut runner = TestRunner::default();
    any::<zakura_chain::ironwood::Action>()
        .new_tree(&mut runner)
        .expect("test action strategy creates a value")
        .current()
}

fn ironwood_v6_tx(
    expiry_height: Height,
    action: zakura_chain::ironwood::Action,
) -> std::sync::Arc<zakura_chain::transaction::Transaction> {
    use zakura_chain::{
        at_least_one, ironwood,
        orchard::{self, tree},
        parameters::NetworkUpgrade,
        primitives::Halo2Proof,
        transaction::{LockTime, Transaction},
    };

    std::sync::Arc::new(Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height,
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: Some(ironwood::ShieldedData {
            flags: orchard::Flags::ENABLE_SPENDS,
            value_balance: Amount::zero(),
            shared_anchor: tree::Root::default(),
            proof: Halo2Proof(vec![0; 4992]),
            actions: at_least_one![ironwood::AuthorizedAction {
                action,
                spend_auth_sig: [0u8; 64].into(),
            }],
            binding_sig: [0u8; 64].into(),
        }),
    })
}

fn verified_ironwood_v6_tx(
    expiry_height: Height,
    action: zakura_chain::ironwood::Action,
) -> zakura_chain::transaction::VerifiedUnminedTx {
    let miner_fee = Amount::try_from(1_000_000).expect("valid test fee");

    zakura_chain::transaction::VerifiedUnminedTx::new(
        ironwood_v6_tx(expiry_height, action).into(),
        miner_fee,
        0,
        0,
        std::sync::Arc::new(vec![]),
    )
    .expect("test transaction has a valid mempool fee")
}

#[test]
fn oversized_policy_rejection_is_cached_by_exact_id() {
    let mut storage = Storage::new(&config::Config::default());
    let transaction = Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .find(|transaction| transaction.transaction.id().auth_digest().is_some())
        .expect("mainnet test vectors contain a witnessed transaction");
    let tx_id = transaction.transaction.id();
    let rejection = NonStandardTransactionError::TransactionTooLarge {
        actual_bytes: 250_001,
        max_bytes: 250_000,
    };

    storage.reject(
        tx_id,
        ExactTipRejectionError::FailedStandard(rejection.clone()).into(),
        crate::Instant::now(),
    );

    assert_eq!(
        storage.rejection(&tx_id, crate::Instant::now()),
        Some(MempoolError::StorageExactTip(
            ExactTipRejectionError::FailedStandard(rejection)
        ))
    );

    let mut different_authorizing_data = tx_id;
    let different_auth_digest = different_authorizing_data
        .auth_digest_mut()
        .expect("test selected a witnessed transaction");
    different_auth_digest.0[0] ^= 1;
    assert_eq!(different_authorizing_data.mined_id(), tx_id.mined_id());
    assert!(!storage
        .rejection(&different_authorizing_data, crate::Instant::now())
        .is_some());
}

#[test]
fn mempool_storage_crud_exact_mainnet() {
    let _init_guard = zakura_test::init();

    let network = Network::Mainnet;

    // Create an empty storage instance
    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: u64::MAX,
        eviction_memory_time: EVICTION_MEMORY_TIME,
        ..Default::default()
    });

    // Get one (1) unmined transaction
    let unmined_tx = network
        .unmined_transactions_in_blocks(..)
        .next_back()
        .expect("at least one unmined transaction");

    // Insert unmined tx into the mempool.
    let _ = storage.insert(
        unmined_tx.clone(),
        Vec::new(),
        None,
        crate::Instant::now(),
        chrono::Utc::now(),
    );

    // Check that it is in the mempool, and not rejected.
    assert!(storage.contains(&unmined_tx.transaction.id().mined_id()));

    // Remove tx
    let removal_count = storage.remove_exact(&iter::once(unmined_tx.transaction.id()).collect());

    // Check that it is /not/ in the mempool.
    assert_eq!(removal_count, 1);
    assert!(!storage.contains(&unmined_tx.transaction.id().mined_id()));
}

#[test]
fn mempool_storage_basic() -> Result<()> {
    let _init_guard = zakura_test::init();

    for network in Network::iter() {
        mempool_storage_basic_for_network(network)?;
    }

    Ok(())
}

fn mempool_storage_basic_for_network(network: Network) -> Result<()> {
    // Get transactions from the first 10 blocks of the Zcash blockchain
    let unmined_transactions: Vec<_> = network.unmined_transactions_in_blocks(..=10).collect();

    assert!(
        MEMPOOL_TX_COUNT < unmined_transactions.len(),
        "inconsistent MEMPOOL_TX_COUNT value for this test; decrease it"
    );

    // Use the sum of the costs of the first `MEMPOOL_TX_COUNT` transactions
    // as the cost limit
    let tx_cost_limit = unmined_transactions
        .iter()
        .take(MEMPOOL_TX_COUNT)
        .map(|tx| tx.cost())
        .sum();

    // Create an empty storage
    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit,
        ..Default::default()
    });

    // Insert them all to the storage
    let mut maybe_inserted_transactions = Vec::new();
    let mut some_rejected_transactions = Vec::new();
    for unmined_transaction in unmined_transactions.clone() {
        let result = storage.insert(
            unmined_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        );
        match result {
            Ok(_) => {
                // While the transaction was inserted here, it can be rejected later.
                maybe_inserted_transactions.push(unmined_transaction);
            }
            Err(_) => {
                // Other transactions can be evicted on a successful insert,
                // so not all rejected transactions will be added.
                some_rejected_transactions.push(unmined_transaction);
            }
        }
    }
    // We know the first MEMPOOL_TX_COUNT must have been inserted successfully.
    assert!(maybe_inserted_transactions.len() >= MEMPOOL_TX_COUNT);
    assert_eq!(
        some_rejected_transactions.len() + maybe_inserted_transactions.len(),
        unmined_transactions.len()
    );

    // Test if the actual number of inserted/rejected transactions is consistent.
    assert!(storage.verified.transaction_count() <= maybe_inserted_transactions.len());
    assert!(storage.rejected_count() >= some_rejected_transactions.len());

    // Test if rejected transactions were actually rejected.
    for tx in some_rejected_transactions.iter() {
        assert!(!storage.contains(&tx.transaction.id().mined_id()));
    }

    // Query all the ids we have for rejected, get back `total - MEMPOOL_SIZE`
    let all_ids: HashSet<UnminedTxId> = unmined_transactions
        .iter()
        .map(|tx| tx.transaction.id())
        .collect();

    // Convert response to a `HashSet`, because the order of the response doesn't matter.
    let all_rejected_ids: HashSet<UnminedTxId> = storage
        .rejected_transactions(all_ids, crate::Instant::now())
        .collect();

    let some_rejected_ids = some_rejected_transactions
        .iter()
        .map(|tx| tx.transaction.id())
        .collect::<HashSet<_>>();

    // Test if the rejected transactions we have are a subset of the actually
    // rejected transactions.
    assert!(some_rejected_ids.is_subset(&all_rejected_ids));

    Ok(())
}

#[test]
fn mempool_storage_crud_same_effects_mainnet() {
    let _init_guard = zakura_test::init();

    let network = Network::Mainnet;

    // Create an empty storage instance
    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: 160_000_000,
        eviction_memory_time: EVICTION_MEMORY_TIME,
        ..Default::default()
    });

    // Get one (1) unmined transaction
    let unmined_tx_1 = network
        .unmined_transactions_in_blocks(..)
        .next_back()
        .expect("at least one unmined transaction");

    // Insert unmined tx into the mempool.
    let _ = storage.insert(
        unmined_tx_1.clone(),
        Vec::new(),
        None,
        crate::Instant::now(),
        chrono::Utc::now(),
    );

    // Check that it is in the mempool, and not rejected.
    assert!(storage.contains(&unmined_tx_1.transaction.id().mined_id()));

    // Reject and remove mined tx
    let removal_count = storage
        .mine(
            &iter::once(unmined_tx_1.transaction.id().mined_id()).collect(),
            crate::Instant::now(),
        )
        .len();

    // Check that it is /not/ in the mempool as a verified transaction.
    assert_eq!(removal_count, 1);
    assert!(!storage.contains(&unmined_tx_1.transaction.id().mined_id()));

    // Check that it's rejection is cached in the chain_rejected_same_effects' `Mined` eviction list.
    assert_eq!(
        storage.rejection(&unmined_tx_1.transaction.id(), crate::Instant::now()),
        Some(SameEffectsChainRejectionError::Mined.into())
    );
    assert_eq!(
        storage.insert(
            unmined_tx_1,
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now()
        ),
        Err(SameEffectsChainRejectionError::Mined.into())
    );

    // Get a different unmined transaction
    let unmined_tx_2 = network
        .unmined_transactions_in_blocks(1..)
        .find(|tx| {
            tx.transaction
                .transaction()
                .spent_outpoints()
                .next()
                .is_some()
        })
        .expect("at least one unmined transaction with at least 1 spent outpoint");

    // Insert unmined tx into the mempool.
    assert_eq!(
        storage.insert(
            unmined_tx_2.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now()
        ),
        Ok(unmined_tx_2.transaction.id())
    );

    // Check that it is in the mempool, and not rejected.
    assert!(storage.contains(&unmined_tx_2.transaction.id().mined_id()));

    // The pool core finds that a block spent its inputs. Storage removes it and
    // caches it as a duplicate spend.
    let mined_id = unmined_tx_2.transaction.id().mined_id();
    assert!(storage.remove_one(&mined_id).is_some());
    storage.reject_effects_chain(
        mined_id,
        SameEffectsChainRejectionError::DuplicateSpend,
        crate::Instant::now(),
    );

    // Check that it is /not/ in the mempool as a verified transaction.
    assert!(!storage.contains(&unmined_tx_2.transaction.id().mined_id()));

    // Check that it's rejection is cached in the chain_rejected_same_effects' `SpendConflict` eviction list.
    assert_eq!(
        storage.rejection(&unmined_tx_2.transaction.id(), crate::Instant::now()),
        Some(SameEffectsChainRejectionError::DuplicateSpend.into())
    );
    assert_eq!(
        storage.insert(
            unmined_tx_2,
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now()
        ),
        Err(SameEffectsChainRejectionError::DuplicateSpend.into())
    );
}

#[test]
fn mempool_rejects_ironwood_conflict_on_insert() {
    let _init_guard = zakura_test::init();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: 160_000_000,
        eviction_memory_time: EVICTION_MEMORY_TIME,
        ..Default::default()
    });

    let action = ironwood_action();
    let first_mempool_tx = verified_ironwood_v6_tx(Height(1), action.clone());
    let second_mempool_tx = verified_ironwood_v6_tx(Height(2), action);

    let first_mempool_id = first_mempool_tx.transaction.id();
    let second_mempool_id = second_mempool_tx.transaction.id();

    assert_eq!(
        storage.insert(
            first_mempool_tx,
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now()
        ),
        Ok(first_mempool_id)
    );

    assert_eq!(
        storage.insert(
            second_mempool_tx,
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now()
        ),
        Err(SameEffectsTipRejectionError::SpendConflict.into())
    );

    assert!(storage.contains(&first_mempool_id.mined_id()));
    assert!(!storage.contains(&second_mempool_id.mined_id()));
    assert_eq!(
        storage.rejection(&second_mempool_id, crate::Instant::now()),
        Some(SameEffectsTipRejectionError::SpendConflict.into())
    );
}

/// Check that the transaction dependencies are updated when transactions with spent mempool outputs
/// are inserted into storage, and that the `Storage.remove()` method also removes any transactions
/// that directly or indirectly spend outputs of a removed transaction.
#[test]
fn mempool_removes_dependent_transactions() -> Result<()> {
    let network = Network::Mainnet;

    // Create an empty storage
    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: 160_000_000,
        eviction_memory_time: EVICTION_MEMORY_TIME,
        ..Default::default()
    });

    // Each transaction spends the previous one, so the chain stops at the ancestor limit.
    let unmined_txs_with_transparent_outputs = || {
        network
            .unmined_transactions_in_blocks(..)
            .filter(|tx| {
                // treat outputs < 100 zatoshis as "dust" for these tests, we want them out
                let dust_threshold: Amount<NonNegative> =
                    Amount::try_from(100u64).expect("valid amount");
                !tx.transaction.transaction().outputs().is_empty()
                    && tx
                        .transaction
                        .transaction()
                        .outputs()
                        .iter()
                        .all(|out| out.value >= dust_threshold)
            })
            .take(MAX_MEMPOOL_ANCESTORS + 1)
    };

    let mut fake_spent_outpoints: Vec<OutPoint> = Vec::new();
    let mut expected_transaction_dependencies = HashMap::new();
    let mut expected_transaction_dependents = HashMap::new();
    for unmined_tx in unmined_txs_with_transparent_outputs() {
        let tx_id = unmined_tx.transaction.id().mined_id();
        let num_outputs = unmined_tx.transaction.transaction().outputs().len();

        if let Some(&fake_spent_outpoint) = fake_spent_outpoints.first() {
            expected_transaction_dependencies
                .insert(tx_id, [fake_spent_outpoint.hash].into_iter().collect());
            expected_transaction_dependents
                .insert(fake_spent_outpoint.hash, [tx_id].into_iter().collect());
        }

        storage
            .insert(
                unmined_tx.clone(),
                fake_spent_outpoints,
                None,
                crate::Instant::now(),
                chrono::Utc::now(),
            )
            .expect("should insert transaction");

        // Add up to 5 of this transaction's outputs as fake spent outpoints for the next transaction
        fake_spent_outpoints = (0..num_outputs.min(5))
            .map(|i| OutPoint::from_usize(tx_id, i))
            .collect();
    }

    assert_eq!(
        storage.transaction_dependencies().dependencies().len(),
        unmined_txs_with_transparent_outputs()
            .count()
            .checked_sub(1)
            .expect("at least one unmined transaction with transparent outputs"),
        "should have an entry all inserted txns except the first one"
    );

    assert_eq!(
        storage.transaction_dependencies().dependencies(),
        &expected_transaction_dependencies,
        "should have expected transaction dependencies"
    );

    assert_eq!(
        storage.transaction_dependencies().dependents(),
        &expected_transaction_dependents,
        "should have expected transaction dependents"
    );

    // Remove the first transaction and check that everything in storage is emptied.
    let first_tx = unmined_txs_with_transparent_outputs()
        .next()
        .expect("at least one unmined transaction with transparent outputs");

    let expected_num_removed = storage.transaction_count();
    let num_removed = storage.remove_exact(&[first_tx.transaction.id()].into_iter().collect());

    assert_eq!(
        num_removed, expected_num_removed,
        "remove_exact should total storage transaction count"
    );

    assert!(
        storage.transaction_dependencies().dependencies().is_empty(),
        "tx deps should be empty"
    );

    assert_eq!(
        storage.transaction_count(),
        0,
        "verified set should be empty"
    );

    Ok(())
}

// ---- Policy function unit tests ----

use super::super::policy::{p2pk_lock_script, p2pkh_lock_script, p2sh_lock_script};

#[test]
fn standard_script_kind_classifies_p2pkh() {
    let _init_guard = zakura_test::init();
    let script = p2pkh_lock_script(&[0xaa; 20]);
    let kind = super::super::policy::standard_script_kind(&script);
    assert!(
        matches!(
            kind,
            Some(zcash_script::solver::ScriptKind::PubKeyHash { .. })
        ),
        "P2PKH script should be classified as PubKeyHash"
    );
}

#[test]
fn standard_script_kind_classifies_p2sh() {
    let _init_guard = zakura_test::init();
    let script = p2sh_lock_script(&[0xbb; 20]);
    let kind = super::super::policy::standard_script_kind(&script);
    assert!(
        matches!(
            kind,
            Some(zcash_script::solver::ScriptKind::ScriptHash { .. })
        ),
        "P2SH script should be classified as ScriptHash"
    );
}

#[test]
fn standard_script_kind_classifies_op_return() {
    let _init_guard = zakura_test::init();
    // OP_RETURN followed by data
    let script = transparent::Script::new(&[0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef]);
    let kind = super::super::policy::standard_script_kind(&script);
    assert!(
        matches!(
            kind,
            Some(zcash_script::solver::ScriptKind::NullData { .. })
        ),
        "OP_RETURN script should be classified as NullData"
    );
}

#[test]
fn standard_script_kind_classifies_p2pk() {
    let _init_guard = zakura_test::init();
    // Compressed pubkey starts with 0x02 or 0x03
    let mut pubkey = [0x02; 33];
    pubkey[1..].fill(0xaa);
    let script = p2pk_lock_script(&pubkey);
    let kind = super::super::policy::standard_script_kind(&script);
    assert!(
        matches!(kind, Some(zcash_script::solver::ScriptKind::PubKey { .. })),
        "P2PK script should be classified as PubKey"
    );
}

#[test]
fn standard_script_kind_rejects_nonstandard() {
    let _init_guard = zakura_test::init();
    // Random garbage bytes should not be a standard script
    let script = transparent::Script::new(&[0xff, 0xfe, 0xfd]);
    let kind = super::super::policy::standard_script_kind(&script);
    assert!(
        kind.is_none(),
        "non-standard script should be classified as None"
    );
}
