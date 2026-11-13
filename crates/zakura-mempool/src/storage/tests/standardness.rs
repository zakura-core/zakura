//! Today's standardness vectors, run against the core's own copy of the
//! pool rules. Today's pool runs the same cases in zakurad's
//! `mempool/tests/vector.rs`. The first transaction is used where today's
//! picks one at random, since the pool crate takes its random numbers from
//! `zakura_rt`.

#![allow(clippy::unwrap_in_result)]

use std::sync::Arc;

use eyre::Result;
use zakura_chain::{
    amount::Amount,
    block::Height,
    parameters::Network,
    transaction::{Transaction, VerifiedUnminedTx},
    transparent,
};

use super::{MempoolError, Storage};
use crate::storage::*;

/// Check that verified transactions are rejected if non-standard
#[test]
fn mempool_reject_non_standard() -> Result<()> {
    let network = Network::Mainnet;

    // pick a random transaction from the dummy Zcash blockchain
    let unmined_transactions = network.unmined_transactions_in_blocks(1..=10);
    let transactions = unmined_transactions.collect::<Vec<_>>();
    let mut last_transaction = transactions.first().expect("Missing transaction").clone();

    last_transaction.height = Some(Height(100_000));

    // Modify the transaction to make it non-standard.
    // This is done by replacing its outputs with a dust output.
    let mut tx = last_transaction.transaction.transaction().clone();
    let tx_mut = Arc::make_mut(&mut tx);
    *tx_mut.outputs_mut() = vec![transparent::Output {
        value: Amount::new(10), // this is below the dust threshold
        lock_script: p2pkh_script([0u8; 20]),
    }];
    last_transaction.transaction = tx.into();

    // Set cost limit to the cost of the transaction we will try to insert.
    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    // Insert the modified transaction into the mempool storage.
    // Expect insertion to fail for non-standard transaction.
    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for non-standard tx");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::IsDust)
    );

    Ok(())
}

/// Check that standard OP_RETURN outputs are accepted when datacarrier is enabled.
#[test]
fn mempool_accept_standard_op_return() -> Result<()> {
    let network = Network::Mainnet;

    let mut last_transaction = network
        .unmined_transactions_in_blocks(1..=10)
        .next()
        .expect("missing transaction");

    last_transaction.height = Some(Height(100_000));

    let mut tx = last_transaction.transaction.transaction().clone();
    let tx_mut = Arc::make_mut(&mut tx);
    *tx_mut.outputs_mut() = vec![transparent::Output {
        value: Amount::new(0),
        lock_script: op_return_script(&[0x01]),
    }];
    last_transaction.transaction = tx.into();

    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    storage.insert(
        last_transaction.clone(),
        Vec::new(),
        None,
        crate::Instant::now(),
        chrono::Utc::now(),
    )?;

    Ok(())
}

/// Check that oversized OP_RETURN scripts are rejected.
#[test]
fn mempool_reject_op_return_too_large() -> Result<()> {
    let network = Network::Mainnet;

    let mut last_transaction = network
        .unmined_transactions_in_blocks(1..=10)
        .next()
        .expect("missing transaction");

    last_transaction.height = Some(Height(100_000));

    let mut tx = last_transaction.transaction.transaction().clone();
    let tx_mut = Arc::make_mut(&mut tx);
    *tx_mut.outputs_mut() = vec![transparent::Output {
        value: Amount::new(0),
        lock_script: op_return_script(&[0x03]),
    }];
    last_transaction.transaction = tx.into();

    let cost_limit = last_transaction.cost();
    // Shrink the OP_RETURN size limit to trigger the oversized rejection path.
    let mempool_config = config::Config {
        tx_cost_limit: cost_limit,
        max_datacarrier_bytes: 2,
        ..Default::default()
    };

    let mut storage: Storage = Storage::new(&mempool_config);

    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for non-standard tx");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::DataCarrierTooLarge)
    );

    Ok(())
}

/// Check that multiple OP_RETURN outputs are rejected.
#[test]
fn mempool_reject_multi_op_return() -> Result<()> {
    let network = Network::Mainnet;

    let mut last_transaction = network
        .unmined_transactions_in_blocks(1..=10)
        .next()
        .expect("missing transaction");

    last_transaction.height = Some(Height(100_000));

    let mut tx = last_transaction.transaction.transaction().clone();
    let tx_mut = Arc::make_mut(&mut tx);
    *tx_mut.outputs_mut() = vec![
        transparent::Output {
            value: Amount::new(0),
            lock_script: op_return_script(&[0x04]),
        },
        transparent::Output {
            value: Amount::new(0),
            lock_script: op_return_script(&[0x05]),
        },
    ];
    last_transaction.transaction = tx.into();

    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for non-standard tx");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::MultiOpReturn)
    );

    Ok(())
}

/// Check that non-standard scriptPubKeys are rejected.
#[test]
fn mempool_reject_non_standard_scriptpubkey() -> Result<()> {
    let network = Network::Mainnet;

    let mut last_transaction = network
        .unmined_transactions_in_blocks(1..=10)
        .next()
        .expect("missing transaction");

    last_transaction.height = Some(Height(100_000));

    let mut tx = last_transaction.transaction.transaction().clone();
    let tx_mut = Arc::make_mut(&mut tx);
    *tx_mut.outputs_mut() = vec![transparent::Output {
        value: Amount::new(1000),
        lock_script: transparent::Script::new(&[0x00]),
    }];
    last_transaction.transaction = tx.into();

    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for non-standard tx");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::ScriptPubKeyNonStandard)
    );

    Ok(())
}

/// Check that bare multisig outputs are rejected.
#[test]
fn mempool_reject_bare_multisig() -> Result<()> {
    let network = Network::Mainnet;

    let mut last_transaction = network
        .unmined_transactions_in_blocks(1..=10)
        .next()
        .expect("missing transaction");

    last_transaction.height = Some(Height(100_000));

    let mut tx = last_transaction.transaction.transaction().clone();
    let tx_mut = Arc::make_mut(&mut tx);
    *tx_mut.outputs_mut() = vec![transparent::Output {
        value: Amount::new(1000),
        lock_script: multisig_script(1, 1),
    }];
    last_transaction.transaction = tx.into();

    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for non-standard tx");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::BareMultiSig)
    );

    Ok(())
}

/// Check that oversized bare multisig outputs are rejected as non-standard.
#[test]
fn mempool_reject_large_multisig() -> Result<()> {
    let network = Network::Mainnet;

    let mut last_transaction = network
        .unmined_transactions_in_blocks(1..=10)
        .next()
        .expect("missing transaction");

    last_transaction.height = Some(Height(100_000));

    let mut tx = last_transaction.transaction.transaction().clone();
    let tx_mut = Arc::make_mut(&mut tx);
    *tx_mut.outputs_mut() = vec![transparent::Output {
        value: Amount::new(1000),
        lock_script: multisig_script(1, 4),
    }];
    last_transaction.transaction = tx.into();

    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for non-standard tx");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::ScriptPubKeyNonStandard)
    );

    Ok(())
}

/// Check that oversized scriptSig inputs are rejected.
#[test]
fn mempool_reject_large_scriptsig() -> Result<()> {
    let network = Network::Mainnet;

    let mut last_transaction = pick_transaction_with_prevout(&network);

    last_transaction.height = Some(Height(100_000));

    let mut tx = last_transaction.transaction.transaction().clone();
    let tx_mut = Arc::make_mut(&mut tx);
    set_first_prevout_unlock_script(tx_mut, transparent::Script::new(&vec![0u8; 1651]));
    last_transaction.transaction = tx.into();

    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for non-standard tx");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::ScriptSigTooLarge)
    );

    Ok(())
}

/// Check that non-push-only scriptSig inputs are rejected.
#[test]
fn mempool_reject_non_push_only_scriptsig() -> Result<()> {
    let network = Network::Mainnet;

    let mut last_transaction = pick_transaction_with_prevout(&network);

    last_transaction.height = Some(Height(100_000));

    let mut tx = last_transaction.transaction.transaction().clone();
    let tx_mut = Arc::make_mut(&mut tx);
    set_first_prevout_unlock_script(tx_mut, transparent::Script::new(&[0xac]));
    last_transaction.transaction = tx.into();

    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for non-standard tx");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::ScriptSigNotPushOnly)
    );

    Ok(())
}

/// Check that transactions with too many sigops are rejected.
#[test]
fn mempool_reject_too_many_sigops() -> Result<()> {
    let network = Network::Mainnet;

    let mut last_transaction = network
        .unmined_transactions_in_blocks(1..=10)
        .next()
        .expect("missing transaction");

    last_transaction.height = Some(Height(100_000));

    // Set the legacy sigop count above the MAX_STANDARD_TX_SIGOPS limit of 4000.
    last_transaction.legacy_sigop_count = 4001;

    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for too many sigops");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::TooManySigops)
    );

    Ok(())
}

/// Check that transactions with non-standard inputs (non-standard spent output script)
/// are rejected.
#[test]
fn mempool_reject_non_standard_inputs() -> Result<()> {
    let network = Network::Mainnet;

    // Use a transaction that has at least one transparent PrevOut input.
    let mut last_transaction = pick_transaction_with_prevout(&network);

    last_transaction.height = Some(Height(100_000));

    // Provide a non-standard spent output script so are_inputs_standard() returns false.
    // Use a script that doesn't match any known template (OP_1 OP_2 OP_ADD).
    let non_standard_script = transparent::Script::new(&[0x51, 0x52, 0x93]);
    let non_standard_output = transparent::Output {
        value: 0u64.try_into().unwrap(),
        lock_script: non_standard_script,
    };
    // Provide one spent output per transparent input (including coinbase inputs in the count,
    // since are_inputs_standard expects spent_outputs.len() == tx.inputs().len()).
    let input_count = last_transaction.transaction.transaction().inputs().len();
    last_transaction.spent_outputs = std::sync::Arc::new(vec![non_standard_output; input_count]);

    let cost_limit = last_transaction.cost();

    let mut storage: Storage = Storage::new(&config::Config {
        tx_cost_limit: cost_limit,
        ..config::Config::default()
    });

    let insert_err = storage
        .insert(
            last_transaction.clone(),
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .expect_err("expected insert to fail for non-standard inputs");

    assert_eq!(
        insert_err,
        MempoolError::NonStandardTransaction(NonStandardTransactionError::NonStandardInputs)
    );

    Ok(())
}

fn op_return_script(data: &[u8]) -> transparent::Script {
    // Build a minimal OP_RETURN script using small pushdata (<= 75 bytes).
    assert!(data.len() <= 75, "test helper only supports small pushdata");

    let mut bytes = Vec::with_capacity(2 + data.len());
    bytes.push(0x6a);
    bytes.push(data.len() as u8);
    bytes.extend_from_slice(data);
    transparent::Script::new(&bytes)
}

fn multisig_script(required: u8, key_count: usize) -> transparent::Script {
    // Construct a bare multisig output: OP_M <pubkeys> OP_N OP_CHECKMULTISIG.
    assert!(required >= 1 && required <= key_count as u8);
    assert!(key_count <= 16);

    let mut bytes = Vec::new();
    bytes.push(op_n(required));

    for i in 0..key_count {
        bytes.push(33u8);
        let mut pubkey = vec![0u8; 33];
        pubkey[0] = 0x02;
        pubkey[1] = i as u8;
        bytes.extend_from_slice(&pubkey);
    }

    bytes.push(op_n(key_count as u8));
    bytes.push(0xae);

    transparent::Script::new(&bytes)
}

fn p2pkh_script(pubkey_hash: [u8; 20]) -> transparent::Script {
    let mut bytes = Vec::with_capacity(25);
    bytes.push(0x76);
    bytes.push(0xa9);
    bytes.push(20);
    bytes.extend_from_slice(&pubkey_hash);
    bytes.push(0x88);
    bytes.push(0xac);
    transparent::Script::new(&bytes)
}

fn op_n(n: u8) -> u8 {
    if n == 0 {
        0x00
    } else {
        0x50 + n
    }
}

fn set_first_prevout_unlock_script(tx: &mut Transaction, script: transparent::Script) {
    for input in tx.inputs_mut() {
        if let transparent::Input::PrevOut { unlock_script, .. } = input {
            *unlock_script = script;
            return;
        }
    }

    panic!("missing prevout input");
}

fn pick_transaction_with_prevout(network: &Network) -> VerifiedUnminedTx {
    network
        .unmined_transactions_in_blocks(..)
        .find(|transaction| {
            transaction
                .transaction
                .transaction()
                .inputs()
                .iter()
                .any(|input| matches!(input, transparent::Input::PrevOut { .. }))
        })
        .expect("missing non-coinbase transaction")
}
