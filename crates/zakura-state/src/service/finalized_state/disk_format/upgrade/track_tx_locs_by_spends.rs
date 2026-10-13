//! Tracks transaction locations by their inputs and revealed nullifiers.

use std::sync::Arc;

use crossbeam_channel::{Receiver, TryRecvError};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use zakura_chain::block::Height;

use crate::{
    service::{finalized_state::ZakuraDb, non_finalized_state::Chain, read},
    Spend,
};

use super::{super::super::DiskWriteBatch, CancelFormatChange};

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

    // A database without transparent indexes never gets transparent spender entries, because
    // it never indexed the history they belong to. Shielded nullifier spenders, including
    // Ironwood, are still indexed.
    let skip_transparent_indexes = zakura_db.skips_transparent_indexes();

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
                    if let Some(spend) = tx
                        .inputs()
                        .iter()
                        .filter(|_| !skip_transparent_indexes)
                        .filter_map(|input| Some(input.outpoint()?.into()))
                        .chain(tx.sprout_nullifiers().cloned().map(Spend::from))
                        .chain(tx.sapling_nullifiers().cloned().map(Spend::from))
                        .chain(tx.orchard_nullifiers().cloned().map(Spend::Orchard))
                        .chain(tx.ironwood_nullifiers().cloned().map(Spend::Ironwood))
                        .next()
                    {
                        if read::spending_transaction_hash::<Arc<Chain>>(None, zakura_db, spend)
                            .expect("transparent spends are only checked when indexes are built")
                            .is_some()
                        {
                            // Skip transactions in blocks with existing indexes
                            return Ok(());
                        } else {
                            should_index_at_height = true
                        }
                    } else {
                        continue;
                    };
                }

                for input in tx.inputs().iter().filter(|_| !skip_transparent_indexes) {
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

                batch.prepare_nullifier_batch(zakura_db, &tx, tx_loc);
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
