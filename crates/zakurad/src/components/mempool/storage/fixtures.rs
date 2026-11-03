//! Synthetic verified transactions for storage tests and benchmarks.
//!
//! Proofs and signatures are placeholders. These fixtures exercise storage after
//! verification; they do not measure cryptographic verification.

use std::sync::Arc;

use proptest::{prelude::*, strategy::ValueTree, test_runner::TestRunner};

use zakura_chain::{
    amount::Amount,
    at_least_one, ironwood,
    orchard::{self, tree},
    parameters::NetworkUpgrade,
    primitives::Halo2Proof,
    transaction::{LockTime, Transaction, VerifiedUnminedTx},
    transparent,
};

use super::policy::p2pkh_lock_script;

/// Builds verified transactions with chosen fees, sizes, and transparent outputs.
pub(super) struct TxFactory {
    runner: TestRunner,
}

impl TxFactory {
    pub(super) fn new() -> Self {
        Self {
            runner: TestRunner::deterministic(),
        }
    }

    /// Returns a transaction that pays `fee` with cost
    /// [`zakura_chain::transaction::MEMPOOL_TRANSACTION_COST_THRESHOLD`].
    pub(super) fn tx(&mut self, fee: u64) -> VerifiedUnminedTx {
        self.tx_with(fee, 0, 0)
    }

    /// Returns a transaction that pays `fee`, has `outputs` transparent outputs,
    /// and contains `padding` extra proof bytes.
    ///
    /// Each transaction reveals a new Ironwood nullifier, so transactions never conflict.
    pub(super) fn tx_with(
        &mut self,
        fee: u64,
        outputs: usize,
        padding: usize,
    ) -> VerifiedUnminedTx {
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
