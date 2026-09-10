use std::pin::Pin;

use proptest::prelude::*;
use tower::ServiceExt;

use super::{
    downloads::TransactionDownloadVerifyError, error::MempoolError, queue_source_log_label,
    storage::Storage, transaction_cooldown_peer, transaction_error_peer_log_label, ActiveState,
    InboundTxDownloads, Mempool, Request,
};
use crate::{
    components::sync::{RecentSyncLengths, SyncStatus},
    BoxError,
};
use zakura_chain::{
    amount::{Amount, NonNegative},
    parameters::NetworkKind,
    transaction::{Transaction, UnminedTx, VerifiedUnminedTx},
    transparent::{self, Address},
};
use zakura_node_services::mempool::QueueSource;

mod prop;
mod vector;

#[test]
fn legacy_queue_source_log_labels_require_explicit_opt_in() {
    let source = QueueSource::LegacySocket("192.0.2.1:8233".parse().expect("valid test socket"));

    assert_eq!(queue_source_log_label(&source, false), "legacy:redacted");
    assert_eq!(
        queue_source_log_label(&source, true),
        "legacy:192.0.2.1:8233"
    );

    let source = QueueSource::Zakura(vec![7, 8, 9]);
    assert_eq!(queue_source_log_label(&source, false), "zakura:[7, 8, 9]");
    assert_eq!(queue_source_log_label(&source, true), "zakura:[7, 8, 9]");
}

#[test]
fn transaction_error_peer_log_labels_require_explicit_opt_in() {
    let error = TransactionDownloadVerifyError::Invalid {
        error: zakura_consensus::error::TransactionError::WrongVersion,
        advertiser_addr: Some("192.0.2.1:8233".parse().expect("valid test socket")),
    };

    assert_eq!(
        transaction_error_peer_log_label(&error, false).as_deref(),
        Some("legacy:redacted")
    );
    assert_eq!(
        transaction_error_peer_log_label(&error, true).as_deref(),
        Some("legacy:192.0.2.1:8233")
    );
}

#[test]
fn lock_time_and_maturity_failures_start_no_cooldown() {
    use std::{collections::HashMap, sync::Arc};

    use chrono::{TimeZone, Utc};
    use zakura_chain::{
        block::Height,
        parameters::{Network, NetworkUpgrade},
        transaction::{Hash, LockTime},
    };
    use zakura_consensus::{error::TransactionError, transaction::check};

    let peer = "192.0.2.1:8233".parse().expect("valid test socket");
    let invalid = |error| TransactionDownloadVerifyError::Invalid {
        error,
        advertiser_addr: Some(peer),
    };

    assert_eq!(
        transaction_cooldown_peer(&invalid(TransactionError::WrongVersion)),
        Some(peer)
    );

    // A spend of a coinbase output created at height 1, one block later.
    let outpoint = transparent::OutPoint::from_usize(Hash([0; 32]), 0);
    let spend = Arc::new(Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: vec![transparent::Input::PrevOut {
            outpoint,
            unlock_script: transparent::Script::new(&[]),
            sequence: 0,
        }],
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    });
    let coinbase_utxo = transparent::Utxo::new(
        transparent::Output::new(Amount::zero(), transparent::Script::new(&[])),
        Height(1),
        true,
    );
    let immature_spend = check::tx_transparent_coinbase_spends_maturity(
        &Network::Mainnet,
        spend,
        Height(2),
        Arc::new(HashMap::new()),
        &HashMap::from([(outpoint, coinbase_utxo)]),
    )
    .expect_err("the coinbase output is immature at height 2");
    assert_eq!(
        transaction_cooldown_peer(&invalid(immature_spend.clone())),
        None,
        "{immature_spend:?}"
    );

    // Checked against this node's tip, which can lag the peer's tip.
    let lock_times = [
        TransactionError::LockedUntilAfterBlockHeight(Height(100)),
        TransactionError::LockedUntilAfterBlockTime(
            Utc.timestamp_opt(1_700_000_000, 0)
                .single()
                .expect("valid test timestamp"),
        ),
    ];
    for error in lock_times {
        assert_ne!(error.mempool_misbehavior_score(), 0, "{error:?}");
        assert_eq!(
            transaction_cooldown_peer(&invalid(error.clone())),
            None,
            "{error:?}"
        );
    }

    // Unattributed failures start nothing.
    assert_eq!(
        transaction_cooldown_peer(&TransactionDownloadVerifyError::Invalid {
            error: TransactionError::WrongVersion,
            advertiser_addr: None,
        }),
        None
    );
}

impl Mempool {
    /// Get the storage field of the mempool for testing purposes.
    pub fn storage(&mut self) -> &mut Storage {
        match &mut self.active_state {
            ActiveState::Disabled => panic!("mempool must be enabled"),
            ActiveState::Enabled { storage, .. } => storage,
        }
    }

    /// Get the transaction downloader of the mempool for testing purposes.
    pub fn tx_downloads(&self) -> &Pin<Box<InboundTxDownloads>> {
        match &self.active_state {
            ActiveState::Disabled => panic!("mempool must be enabled"),
            ActiveState::Enabled { tx_downloads, .. } => tx_downloads,
        }
    }

    /// Enable the mempool by pretending the synchronization is close to the tip.
    ///
    /// Requires a chain tip action to enable the mempool before the future resolves.
    pub async fn enable(&mut self, recent_syncs: &mut RecentSyncLengths) {
        // Pretend we're close to tip
        SyncStatus::sync_close_to_tip(recent_syncs);
        // Make a dummy request to poll the mempool and make it enable itself
        self.dummy_call().await;
    }

    /// Pretend the synchronization is far from the tip and poll the mempool.
    async fn sync_far_from_tip(&mut self, recent_syncs: &mut RecentSyncLengths) {
        // Pretend we're far from the tip
        SyncStatus::sync_far_from_tip(recent_syncs);
        // Make a dummy request to poll the mempool.
        self.dummy_call().await;
    }

    /// Perform a dummy service call so that `poll_ready` is called.
    pub async fn dummy_call(&mut self) {
        self.oneshot(Request::CheckForVerifiedTransactions)
            .await
            .expect("unexpected failure when checking for verified transactions");
    }
}

/// Helper trait to extract the [`MempoolError`] from a [`BoxError`].
pub trait UnboxMempoolError {
    /// Extract and unbox the [`MempoolError`] stored inside `self`.
    ///
    /// # Panics
    ///
    /// If the `boxed_error` is not a boxed [`MempoolError`].
    fn unbox_mempool_error(self) -> MempoolError;
}

impl UnboxMempoolError for MempoolError {
    fn unbox_mempool_error(self) -> MempoolError {
        self
    }
}

impl UnboxMempoolError for BoxError {
    fn unbox_mempool_error(self) -> MempoolError {
        self.downcast::<MempoolError>()
            .expect("error is not an expected `MempoolError`")
            // TODO: use `Box::into_inner` when it becomes stabilized.
            .as_ref()
            .clone()
    }
}

impl<T, E> UnboxMempoolError for Result<T, E>
where
    E: UnboxMempoolError,
{
    fn unbox_mempool_error(self) -> MempoolError {
        match self {
            Ok(_) => panic!("expected a mempool error, but got a success instead"),
            Err(error) => error.unbox_mempool_error(),
        }
    }
}

/// Return a [`VerifiedUnminedTx`] strategy with outputs and inputs adjusted to pass standardness.
pub fn standard_verified_unmined_tx_strategy() -> BoxedStrategy<VerifiedUnminedTx> {
    any::<Transaction>()
        .prop_map(|mut transaction| {
            standardize_transaction(&mut transaction);

            let unmined_tx = UnminedTx::from(transaction);
            let miner_fee = unmined_tx.conventional_fee();

            VerifiedUnminedTx::new(unmined_tx, miner_fee, 0, 0, std::sync::Arc::new(vec![]))
                .expect("standardized transaction should pass ZIP-317 checks")
        })
        .boxed()
}

/// Mutate a transaction so its transparent inputs/outputs pass standardness checks.
pub fn standardize_transaction(transaction: &mut Transaction) {
    let lock_script = standard_lock_script();
    let output_value = Amount::<NonNegative>::try_from(10_000).expect("valid amount");

    for input in transaction.inputs_mut() {
        if let transparent::Input::PrevOut { unlock_script, .. } = input {
            *unlock_script = transparent::Script::new(&[]);
        }
    }

    for output in transaction.outputs_mut() {
        output.lock_script = lock_script.clone();
        output.value = output_value;
    }
}

fn standard_lock_script() -> transparent::Script {
    Address::from_pub_key_hash(NetworkKind::Mainnet, [0u8; 20]).script()
}
