//! Tracks transaction locations by their inputs and revealed nullifiers.

#[cfg(feature = "indexer")]
use std::sync::Arc;

use crossbeam_channel::{Receiver, TryRecvError};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use zakura_chain::block::Height;

use crate::service::finalized_state::ZakuraDb;

#[cfg(feature = "indexer")]
use crate::{
    service::{non_finalized_state::Chain, read},
    Spend,
};

#[cfg(all(test, not(feature = "indexer")))]
use crate::service::finalized_state::WriteDisk;

use super::{super::super::DiskWriteBatch, CancelFormatChange};

#[cfg(feature = "indexer")]
fn first_spend_is_indexed(
    transaction: &zakura_chain::transaction::Transaction,
    zakura_db: &ZakuraDb,
) -> Option<bool> {
    transaction
        .inputs()
        .iter()
        .filter_map(|input| Some(input.outpoint()?.into()))
        .chain(transaction.sprout_nullifiers().cloned().map(Spend::from))
        .chain(transaction.sapling_nullifiers().cloned().map(Spend::from))
        .chain(
            transaction
                .orchard_nullifiers()
                .cloned()
                .map(Spend::Orchard),
        )
        .chain(
            transaction
                .ironwood_nullifiers()
                .cloned()
                .map(Spend::Ironwood),
        )
        .next()
        .map(|spend| {
            read::spending_transaction_hash::<Arc<Chain>>(None, zakura_db, spend).is_some()
        })
}

#[cfg(all(test, not(feature = "indexer")))]
fn first_spend_is_indexed(
    transaction: &zakura_chain::transaction::Transaction,
    zakura_db: &ZakuraDb,
) -> Option<bool> {
    let has_transaction_hash = |location: Option<crate::TransactionLocation>| {
        location
            .and_then(|location| zakura_db.transaction_hash(location))
            .is_some()
    };

    if let Some(outpoint) = transaction
        .inputs()
        .iter()
        .find_map(|input| input.outpoint())
    {
        return Some(has_transaction_hash(zakura_db.spending_tx_loc(&outpoint)));
    }
    if let Some(nullifier) = transaction.sprout_nullifiers().next() {
        return Some(has_transaction_hash(
            zakura_db.sprout_revealing_tx_loc(nullifier),
        ));
    }
    if let Some(nullifier) = transaction.sapling_nullifiers().next() {
        return Some(has_transaction_hash(
            zakura_db.sapling_revealing_tx_loc(nullifier),
        ));
    }
    if let Some(nullifier) = transaction.orchard_nullifiers().next() {
        return Some(has_transaction_hash(
            zakura_db.orchard_revealing_tx_loc(nullifier),
        ));
    }
    if let Some(nullifier) = transaction.ironwood_nullifiers().next() {
        return Some(has_transaction_hash(
            zakura_db.ironwood_revealing_tx_loc(nullifier),
        ));
    }

    None
}

#[cfg(all(test, not(feature = "indexer")))]
fn prepare_nullifier_index_batch(
    batch: &mut DiskWriteBatch,
    zakura_db: &ZakuraDb,
    transaction: &zakura_chain::transaction::Transaction,
    transaction_location: crate::TransactionLocation,
) {
    let db = zakura_db.db();
    let sprout_nullifiers = db.cf_handle("sprout_nullifiers").unwrap();
    let sapling_nullifiers = db.cf_handle("sapling_nullifiers").unwrap();
    let orchard_nullifiers = db.cf_handle("orchard_nullifiers").unwrap();
    let ironwood_nullifiers = db.cf_handle("ironwood_nullifiers").unwrap();

    for nullifier in transaction.sprout_nullifiers() {
        batch.zs_insert(&sprout_nullifiers, nullifier, transaction_location);
    }
    for nullifier in transaction.sapling_nullifiers() {
        batch.zs_insert(&sapling_nullifiers, nullifier, transaction_location);
    }
    for nullifier in transaction.orchard_nullifiers() {
        batch.zs_insert(&orchard_nullifiers, nullifier, transaction_location);
    }
    for nullifier in transaction.ironwood_nullifiers() {
        batch.zs_insert(&ironwood_nullifiers, nullifier, transaction_location);
    }
}

/// Runs disk format upgrade for tracking transaction locations by their inputs and revealed nullifiers.
///
/// Returns `Ok` if the upgrade completed, and `Err` if it was cancelled.
#[allow(clippy::unwrap_in_result)]
#[instrument(skip(zakura_db, cancel_receiver))]
pub fn run(
    initial_finalized_tip_height: Height,
    zakura_db: &ZakuraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
        return Err(CancelFormatChange);
    }

    (0..=initial_finalized_tip_height.0)
        .into_par_iter()
        .try_for_each(|height| {
            let height = Height(height);
            let mut batch = DiskWriteBatch::new();
            let mut should_index_at_height = false;

            let transactions = zakura_db.transactions_by_location_range(
                crate::TransactionLocation::from_index(height, 1)
                    ..=crate::TransactionLocation::max_for_height(height),
            );

            for (tx_loc, tx) in transactions {
                if tx.is_coinbase() {
                    continue;
                }

                if !should_index_at_height {
                    if let Some(is_indexed) = first_spend_is_indexed(&tx, zakura_db) {
                        if is_indexed {
                            // Skip transactions in blocks with existing indexes
                            return Ok(());
                        } else {
                            should_index_at_height = true
                        }
                    } else {
                        continue;
                    };
                }

                for input in tx.inputs() {
                    if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
                        return Err(CancelFormatChange);
                    }

                    let spent_outpoint = input
                        .outpoint()
                        .expect("should filter out coinbase transactions");

                    let spent_output_location = zakura_db
                        .output_location(&spent_outpoint)
                        .expect("should have location for spent outpoint");

                    let _ = zakura_db
                        .tx_loc_by_spent_output_loc_cf()
                        .with_batch_for_writing(&mut batch)
                        .zs_insert(&spent_output_location, &tx_loc);
                }

                #[cfg(feature = "indexer")]
                batch.prepare_nullifier_batch(zakura_db, &tx, tx_loc);

                #[cfg(all(test, not(feature = "indexer")))]
                prepare_nullifier_index_batch(&mut batch, zakura_db, &tx, tx_loc);
            }

            if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
                return Err(CancelFormatChange);
            }

            zakura_db
                .write_batch(batch)
                .expect("unexpected database write failure");

            if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
                return Err(CancelFormatChange);
            }

            Ok(())
        })
}
