//! Replay retained bodies at H to rebuild derived indexes, while consensus UTXOs stay fixed.
//!
//! Each step replays a window of blocks. It resolves every spend from the omitted
//! outputs that applying stored, and deletes each resolved row. One batch commits
//! the window's index updates, historical pools, deletions, and cursor. After the
//! terminal block, the final audit publishes completion.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use rayon::prelude::*;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height},
    block_info::BlockInfo,
    parameters::{spentness_hints::Commitment, Network},
    transparent,
    value_balance::ValueBalance,
};

use super::{record::OMITTED_OUTPUTS, Progress, SpentnessError};
use crate::service::{
    check::utxo::transparent_coinbase_spend,
    finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        disk_format::{
            transparent::{AddressBalanceLocation, AddressBalanceLocationUpdates},
            OutputLocation, TransactionLocation,
        },
        zakura_db::ZakuraDb,
    },
};

/// The most blocks that one rebuild step replays.
#[cfg(not(test))]
const MAX_WINDOW_BLOCKS: usize = 1_000;
/// Tests use small windows, so spends cross window boundaries.
#[cfg(test)]
const MAX_WINDOW_BLOCKS: usize = 4;
/// A rebuild step stops adding blocks once their serialized size reaches this limit.
const MAX_WINDOW_BYTES: u64 = 64 * 1024 * 1024;
/// Log rebuild progress each time the cursor crosses a multiple of this height.
const PROGRESS_LOG_INTERVAL: u32 = 10_000;

/// The replayed transparent pool and the omitted outputs that no replayed input has spent.
struct Cursor {
    transparent_value: Amount<NonNegative>,
    unspent_omitted: u64,
}

/// The transparent changes of one replayed block, in the shapes the index writers take.
#[derive(Default)]
struct BlockChanges {
    created: BTreeMap<OutputLocation, transparent::Utxo>,
    spent: HashMap<transparent::OutPoint, transparent::Utxo>,
    spent_by_location: BTreeMap<OutputLocation, transparent::Utxo>,
    #[cfg(feature = "indexer")]
    spent_locations: HashMap<transparent::OutPoint, OutputLocation>,
    created_value: Amount<NonNegative>,
    spent_value: Amount<NonNegative>,
    spends: u64,
}

impl ZakuraDb {
    /// Replay one window of blocks, or audit and publish completion after the terminal block.
    ///
    /// Returns `true` once construction is complete. `yield_control` runs between
    /// units of work, so the writer can serve header control messages.
    pub(crate) fn rebuild_spentness_step(
        &mut self,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<bool, SpentnessError> {
        yield_control()?;
        let started = Instant::now();
        let Some(Progress::Rebuilding {
            commitment,
            indexed_height,
            transparent_value,
            unspent_omitted,
        }) = self.spentness_progress()?
        else {
            return Ok(!self.spentness_incomplete());
        };
        // Replay copies every pool except transparent from applying, which leaves NSM at zero.
        if !commitment.precedes_nu7(&self.network()) {
            return Err(SpentnessError::ReachesNu7 {
                height: commitment.terminal_height,
            });
        }
        let cursor = Cursor {
            transparent_value: Amount::try_from(transparent_value)?,
            unspent_omitted,
        };
        let terminal = Height(commitment.terminal_height);
        // Genesis adds no transparent indexes or pool change. Startup bounds the
        // indexed height by the terminal height, so the next height cannot overflow.
        let first = indexed_height.map_or(Height(1), |indexed| Height(indexed + 1));
        if first > terminal {
            self.complete_rebuild(commitment, cursor, yield_control)?;
            metrics::histogram!("state.spentness.rebuild.audit_seconds")
                .record(started.elapsed().as_secs_f64());
            return Ok(true);
        }
        if indexed_height.is_none() {
            tracing::info!(
                height = terminal.0,
                "rebuilding spentness indexes before ordinary commits resume"
            );
        }

        let window = self.rebuild_window(first, terminal)?;
        let (last, _) = *window.last().expect("the window holds at least one block");
        let (mut batch, cursor) = self.replay_window(&window, cursor)?;
        batch.prepare_spentness_progress(
            self,
            Progress::Rebuilding {
                commitment,
                indexed_height: Some(last.0),
                transparent_value: cursor.transparent_value.into(),
                unspent_omitted: cursor.unspent_omitted,
            },
        )?;
        metrics::histogram!("state.spentness.rebuild.prepare_seconds")
            .record(started.elapsed().as_secs_f64());

        let started = Instant::now();
        self.write_batch(batch)?;
        metrics::histogram!("state.spentness.rebuild.commit_seconds")
            .record(started.elapsed().as_secs_f64());
        metrics::gauge!("state.spentness.rebuilt_height").set(f64::from(last.0));
        if last.0 / PROGRESS_LOG_INTERVAL > (first.0 - 1) / PROGRESS_LOG_INTERVAL {
            tracing::info!(height = last.0, "committed spentness index replay progress");
        }
        Ok(false)
    }

    /// The heights and saved accounting that the next step replays.
    fn rebuild_window(
        &self,
        first: Height,
        terminal: Height,
    ) -> Result<Vec<(Height, BlockInfo)>, SpentnessError> {
        let mut window = Vec::new();
        let mut bytes = 0;
        for height in (first.0..=terminal.0).map(Height).take(MAX_WINDOW_BLOCKS) {
            let saved = self
                .block_info_cf()
                .zs_get(&height)
                .ok_or(SpentnessError::Mismatch(
                    "historical block accounting is missing",
                ))?;
            bytes += u64::from(saved.size());
            window.push((height, saved));
            if bytes >= MAX_WINDOW_BYTES {
                break;
            }
        }
        Ok(window)
    }

    /// Prepare the window's rebuilt indexes, historical pools, and omitted-output deletions.
    fn replay_window(
        &self,
        window: &[(Height, BlockInfo)],
        mut cursor: Cursor,
    ) -> Result<(DiskWriteBatch, Cursor), SpentnessError> {
        let network = self.network();
        let blocks: Vec<Arc<Block>> = window
            .par_iter()
            .map(|(height, _)| {
                self.block((*height).into()).ok_or(SpentnessError::Mismatch(
                    "rebuild block is missing from retained history",
                ))
            })
            .collect::<Result<_, _>>()?;
        let inputs: Vec<(TransactionLocation, transparent::OutPoint)> = window
            .iter()
            .zip(&blocks)
            .flat_map(|((height, _), block)| {
                block
                    .transactions
                    .iter()
                    .enumerate()
                    .flat_map(move |(index, transaction)| {
                        let spending = TransactionLocation::from_usize(*height, index);
                        transaction
                            .inputs()
                            .iter()
                            .filter_map(transparent::Input::outpoint)
                            .map(move |outpoint| (spending, outpoint))
                    })
            })
            .collect();
        // Reads see the state before this window. The serial replay below rejects a
        // second spend of an output that this window already consumed.
        let resolved: Vec<(OutputLocation, transparent::Utxo)> = inputs
            .into_par_iter()
            .map(|(spending, outpoint)| self.resolve_spend(spending, outpoint))
            .collect::<Result<_, _>>()?;

        let mut resolved = resolved.into_iter();
        let mut consumed = HashSet::new();
        let mut batch = DiskWriteBatch::new();
        let mut changes = Vec::with_capacity(blocks.len());
        for ((height, saved), block) in window.iter().zip(&blocks) {
            let block_changes =
                replay_block(&network, block, *height, &mut resolved, &mut consumed)?;
            let transparent_value = (u64::from(cursor.transparent_value)
                + u64::from(block_changes.created_value))
            .checked_sub(u64::from(block_changes.spent_value))
            .ok_or(SpentnessError::Mismatch(
                "rebuilt transparent pool is negative",
            ))?;
            cursor.transparent_value = Amount::try_from(transparent_value)?;
            cursor.unspent_omitted = cursor
                .unspent_omitted
                .checked_sub(block_changes.spends)
                .ok_or(SpentnessError::Mismatch(
                    "replay spent more outputs than the artifact omits",
                ))?;
            // Applying recorded exact pools except transparent.
            let mut pool = *saved.value_pools();
            pool.set_transparent_value_balance(ValueBalance::from_transparent_amount(
                cursor.transparent_value,
            ));
            let _ = self
                .block_info_cf()
                .with_batch_for_writing(&mut batch)
                .zs_insert(height, &BlockInfo::new(pool, saved.size()));
            changes.push(block_changes);
        }

        let mut balances =
            AddressBalanceLocationUpdates::Insert(self.window_balances(&network, &changes));
        for (((height, _), block), block_changes) in window.iter().zip(&blocks).zip(&changes) {
            batch.prepare_transparent_index_entries(
                self,
                &network,
                block,
                *height,
                &block_changes.created,
                &block_changes.spent,
                &block_changes.spent_by_location,
                #[cfg(feature = "indexer")]
                &block_changes.spent_locations,
                &mut balances,
            );
        }
        batch.prepare_transparent_balances_batch(&self.db, balances);

        let omitted_outputs = self.omitted_outputs_cf();
        for location in consumed {
            batch.zs_delete(&omitted_outputs, location);
        }
        Ok((batch, cursor))
    }

    /// Resolve one spend from the omitted outputs. It must follow its output and spare genesis.
    fn resolve_spend(
        &self,
        spending: TransactionLocation,
        outpoint: transparent::OutPoint,
    ) -> Result<(OutputLocation, transparent::Utxo), SpentnessError> {
        let creating = self
            .transaction_location(outpoint.hash)
            .ok_or(SpentnessError::Mismatch(
                "spend has no retained creating transaction",
            ))?;
        if creating >= spending || creating.height.is_min() {
            return Err(SpentnessError::Mismatch(
                "spend precedes its output or spends genesis",
            ));
        }
        let location = OutputLocation::from_output_index(creating, outpoint.index);
        let output = self
            .db
            .zs_get(&self.omitted_outputs_cf(), &location)
            .ok_or(SpentnessError::Mismatch(
                "spend names an output that the artifact retains or an earlier block spent",
            ))?;
        let utxo = transparent::OrderedUtxo::new(
            output,
            location.height(),
            location.transaction_index().as_usize(),
        )
        .utxo;
        Ok((location, utxo))
    }

    /// The stored balances of the addresses that the window spends from or pays to.
    fn window_balances(
        &self,
        network: &Network,
        changes: &[BlockChanges],
    ) -> HashMap<transparent::Address, AddressBalanceLocation> {
        let addresses: HashSet<transparent::Address> = changes
            .iter()
            .flat_map(|block| {
                block
                    .spent_by_location
                    .values()
                    .chain(block.created.values())
            })
            .filter_map(|utxo| utxo.output.address(network))
            .collect();
        addresses
            .into_par_iter()
            .filter_map(|address| {
                let balance = self.address_balance_location(&address)?;
                Some((address, balance))
            })
            .collect()
    }

    /// Audit the rebuilt state at H, clear the omitted outputs, and complete.
    fn complete_rebuild(
        &self,
        commitment: Commitment,
        cursor: Cursor,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<(), SpentnessError> {
        // Each replayed spend consumed a distinct omitted output. When none remains,
        // the omitted outputs are exactly the spent outputs, so the survivors are
        // exactly the terminal UTXO set.
        if cursor.unspent_omitted != 0 {
            return Err(SpentnessError::Mismatch(
                "the artifact omits outputs that no retained input spends",
            ));
        }
        if cursor.transparent_value != self.finalized_value_pool().transparent_amount() {
            return Err(SpentnessError::Mismatch(
                "rebuilt transparent pool differs from the terminal survivors",
            ));
        }
        self.audit_address_indexes(yield_control)?;

        let terminal = Height(commitment.terminal_height);
        let mut batch = DiskWriteBatch::new();
        // Replay already deleted every row. The range tombstone lets compaction drop them.
        // The terminal height is a block height, so the next height fits in u32.
        batch.zs_delete_range(
            &self.omitted_outputs_cf(),
            OutputLocation::from_usize(Height(0), 0, 0),
            OutputLocation::from_usize(Height(terminal.0 + 1), 0, 0),
        );
        let progress = Progress::Complete {
            rollback_floor: terminal.0,
            commitment,
        };
        batch.prepare_spentness_progress(self, progress.clone())?;
        self.write_batch(batch)?;
        self.publish_spentness_status(&progress);
        tracing::info!(
            height = terminal.0,
            "spentness index rebuild verified and complete"
        );
        Ok(())
    }

    fn omitted_outputs_cf(&self) -> impl rocksdb::AsColumnFamilyRef + '_ {
        self.db
            .cf_handle(OMITTED_OUTPUTS)
            .expect("omitted-output column family is declared")
    }
}

/// Replay one block's transparent transfers from its resolved spends.
///
/// Rejects a second spend of any output in `consumed`, an invalid coinbase spend,
/// and a transaction whose outputs exceed its inputs.
#[allow(clippy::unwrap_in_result)]
fn replay_block(
    network: &Network,
    block: &Block,
    height: Height,
    resolved: &mut impl Iterator<Item = (OutputLocation, transparent::Utxo)>,
    consumed: &mut HashSet<OutputLocation>,
) -> Result<BlockChanges, SpentnessError> {
    let mut changes = BlockChanges::default();
    for (transaction_index, transaction) in block.transactions.iter().enumerate() {
        let restriction = transaction.coinbase_spend_restriction(network, height);
        for outpoint in transaction
            .inputs()
            .iter()
            .filter_map(transparent::Input::outpoint)
        {
            let (location, utxo) = resolved
                .next()
                .expect("the window resolved every input in block order");
            if !consumed.insert(location) {
                return Err(SpentnessError::Mismatch("rebuild found a duplicate spend"));
            }
            transparent_coinbase_spend(outpoint, restriction, &utxo)
                .map_err(|_| SpentnessError::Mismatch("rebuild found an invalid coinbase spend"))?;
            changes.spent_value = (changes.spent_value + utxo.output.value)?;
            // Each spend consumed a distinct output, so the count cannot overflow.
            changes.spends += 1;
            changes.spent.insert(outpoint, utxo.clone());
            changes.spent_by_location.insert(location, utxo);
            #[cfg(feature = "indexer")]
            changes.spent_locations.insert(outpoint, location);
        }
        if !transaction.is_coinbase() {
            transaction
                .value_balance(&changes.spent)?
                .remaining_transaction_value()?;
        }
        for (output_index, output) in transaction.outputs().iter().enumerate() {
            changes.created_value = (changes.created_value + output.value)?;
            changes.created.insert(
                OutputLocation::from_usize(height, transaction_index, output_index),
                transparent::Utxo::new(output.clone(), height, transaction.is_coinbase()),
            );
        }
    }
    Ok(changes)
}
