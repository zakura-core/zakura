//! Replay retained bodies at H to rebuild derived indexes, while consensus UTXOs stay fixed.
//!
//! Each step replays one block and commits its index updates with the cursor. The
//! replay restores address balances, received totals, first-receive locations,
//! address UTXOs, transaction indexes, and historical value pools. The last step
//! runs the final audit and publishes completion.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use zakura_chain::{
    amount::NonNegative,
    block::{Block, Height},
    block_info::BlockInfo,
    parameters::spentness_hints::Commitment,
    transaction::Transaction,
    transparent,
    value_balance::ValueBalance,
};

use super::{record::PoolAccounting, Progress, SpentnessError};
use crate::service::{
    check::utxo::transparent_coinbase_spend,
    finalized_state::{
        disk_db::DiskWriteBatch,
        disk_format::{
            transparent::AddressBalanceLocationUpdates, OutputLocation, TransactionLocation,
        },
        zakura_db::ZakuraDb,
    },
};

/// Creating transactions kept in memory. Each is bounded by the block size limit.
const REPLAY_CACHE_CAPACITY: usize = 64;
/// Log rebuild progress every this many blocks.
const PROGRESS_LOG_INTERVAL: u32 = 10_000;

/// Recently read creating transactions, cleared when full.
#[derive(Default)]
pub(crate) struct ReplayCache(HashMap<TransactionLocation, Arc<Transaction>>);

impl ReplayCache {
    fn transaction(
        &mut self,
        db: &ZakuraDb,
        location: TransactionLocation,
    ) -> Result<Arc<Transaction>, SpentnessError> {
        if let Some(transaction) = self.0.get(&location) {
            return Ok(transaction.clone());
        }
        metrics::counter!("state.spentness.rebuild.creating_transaction_reads").increment(1);
        let transaction = db
            .transactions_by_location_range(location..=location)
            .next()
            .map(|(_, transaction)| Arc::new(transaction))
            .ok_or(SpentnessError::Mismatch(
                "creating transaction is missing from retained history",
            ))?;
        if db.transaction_hash(location) != Some(transaction.hash()) {
            return Err(SpentnessError::Mismatch(
                "retained creating transaction hash differs from its index",
            ));
        }
        if self.0.len() == REPLAY_CACHE_CAPACITY {
            self.0.clear();
        }
        self.0.insert(location, transaction.clone());
        Ok(transaction)
    }

    /// Rebuild the unspent entry for `location` from its retained creating transaction.
    pub(super) fn utxo(
        &mut self,
        db: &ZakuraDb,
        location: OutputLocation,
    ) -> Result<transparent::Utxo, SpentnessError> {
        let transaction = self.transaction(db, location.transaction_location())?;
        let output = transaction
            .outputs()
            .get(location.output_index().as_usize())
            .ok_or(SpentnessError::Mismatch(
                "output index is outside its creating transaction",
            ))?;
        Ok(transparent::Utxo::new(
            output.clone(),
            location.height(),
            transaction.is_coinbase(),
        ))
    }
}

/// The transparent changes of one replayed block, in the shapes the index writers take.
#[derive(Default)]
struct BlockChanges {
    created: BTreeMap<OutputLocation, transparent::Utxo>,
    spent: HashMap<transparent::OutPoint, transparent::Utxo>,
    spent_by_location: BTreeMap<OutputLocation, transparent::Utxo>,
    spending_transactions: BTreeMap<OutputLocation, TransactionLocation>,
    #[cfg(feature = "indexer")]
    spent_locations: HashMap<transparent::OutPoint, OutputLocation>,
}

/// Compare every pool except transparent, which the rebuild replays separately.
fn shielded_pools(mut pool: ValueBalance<NonNegative>) -> ValueBalance<NonNegative> {
    pool.set_transparent_value_balance(ValueBalance::zero());
    pool
}

impl ZakuraDb {
    /// Replay one block, or audit and publish completion after the terminal block.
    ///
    /// Returns `true` once construction is complete. `yield_control` runs between
    /// units of work, so the writer can serve header control messages.
    pub(crate) fn rebuild_spentness_step(
        &mut self,
        cache: &mut ReplayCache,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<bool, SpentnessError> {
        yield_control()?;
        let started = Instant::now();
        let Some(Progress::Rebuilding {
            commitment,
            indexed_height,
            replay_accounting,
            survivor_value,
        }) = self.spentness_progress()?
        else {
            return Ok(!self.spentness_incomplete());
        };
        let replay_pool = ValueBalance::<NonNegative>::try_from(replay_accounting)?;

        let height = match indexed_height {
            Some(indexed) if indexed == commitment.terminal_height => {
                self.complete_rebuild(
                    commitment,
                    replay_pool,
                    survivor_value,
                    cache,
                    yield_control,
                )?;
                metrics::histogram!("state.spentness.rebuild.audit_seconds")
                    .record(started.elapsed().as_secs_f64());
                return Ok(true);
            }
            // The terminal height bounds `indexed`, so the next height cannot overflow.
            Some(indexed) => Height(indexed + 1),
            None => {
                tracing::info!(
                    height = commitment.terminal_height,
                    "rebuilding spentness indexes before ordinary commits resume"
                );
                Height(0)
            }
        };

        let (mut batch, pool) = self.rebuild_block(cache, height, replay_pool)?;
        batch.prepare_spentness_progress(
            self,
            Progress::Rebuilding {
                commitment,
                indexed_height: Some(height.0),
                replay_accounting: PoolAccounting::from(pool),
                survivor_value,
            },
        )?;
        metrics::histogram!("state.spentness.rebuild.prepare_seconds")
            .record(started.elapsed().as_secs_f64());

        let started = Instant::now();
        self.write_batch(batch)?;
        metrics::histogram!("state.spentness.rebuild.commit_seconds")
            .record(started.elapsed().as_secs_f64());
        metrics::gauge!("state.spentness.rebuilt_height").set(f64::from(height.0));
        if height.0.is_multiple_of(PROGRESS_LOG_INTERVAL) {
            tracing::info!(
                height = height.0,
                "committed spentness index replay progress"
            );
        }
        Ok(false)
    }

    /// Prepare one block's rebuilt indexes and historical pools.
    ///
    /// Returns the batch and the replayed pools after the block.
    fn rebuild_block(
        &self,
        cache: &mut ReplayCache,
        height: Height,
        replay_pool: ValueBalance<NonNegative>,
    ) -> Result<(DiskWriteBatch, ValueBalance<NonNegative>), SpentnessError> {
        let block = self.block(height.into()).ok_or(SpentnessError::Mismatch(
            "rebuild block is missing from retained history",
        ))?;
        let saved = self
            .block_info_cf()
            .zs_get(&height)
            .ok_or(SpentnessError::Mismatch(
                "historical block accounting is missing",
            ))?;

        let changes = self.replay_block(cache, height, &block)?;
        let mut batch = DiskWriteBatch::new();
        let mut pool = replay_pool;
        // Genesis outputs are unspendable, so genesis adds no transparent indexes or pool change.
        if !height.is_min() {
            pool = pool.add_chain_value_pool_change(
                block.chain_value_pool_change(&changes.spent, None)?,
            )?;
            // Applying recorded the exact deferred pool supplied by checkpoint verification.
            pool.set_deferred_amount(saved.value_pools().deferred_amount());
            self.prepare_rebuilt_indexes(&mut batch, &block, height, changes);
        }
        if shielded_pools(pool) != shielded_pools(*saved.value_pools()) {
            return Err(SpentnessError::Mismatch(
                "rebuilt shielded pools differ from checkpoint accounting",
            ));
        }
        let _ = self
            .block_info_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&height, &BlockInfo::new(pool, saved.size()));
        Ok((batch, pool))
    }

    fn prepare_rebuilt_indexes(
        &self,
        batch: &mut DiskWriteBatch,
        block: &Block,
        height: Height,
        changes: BlockChanges,
    ) {
        let network = self.network();
        let balances = changes
            .spent_by_location
            .values()
            .chain(changes.created.values())
            .filter_map(|utxo| utxo.output.address(&network))
            .filter_map(|address| {
                self.address_balance_location(&address)
                    .map(|balance| (address, balance))
            })
            .collect();
        batch.prepare_transparent_indexes_batch(
            self,
            &network,
            block,
            height,
            &changes.created,
            &changes.spent,
            &changes.spent_by_location,
            #[cfg(feature = "indexer")]
            &changes.spent_locations,
            AddressBalanceLocationUpdates::Insert(balances),
        );

        // Without the indexer, ordinary state keeps no spent-output index. The rebuild
        // writes one anyway so later blocks can detect duplicate spends. Completion
        // deletes it.
        #[cfg(not(feature = "indexer"))]
        for (location, spending) in changes.spending_transactions {
            let _ = self
                .tx_loc_by_spent_output_loc_cf()
                .with_batch_for_writing(batch)
                .zs_insert(&location, &spending);
        }
    }

    /// Audit the rebuilt state at H, drop the temporary spent-output index, and complete.
    fn complete_rebuild(
        &self,
        commitment: Commitment,
        replay_pool: ValueBalance<NonNegative>,
        survivor_value: u64,
        cache: &mut ReplayCache,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<(), SpentnessError> {
        if replay_pool != self.finalized_value_pool()
            || u64::from(replay_pool.transparent_amount()) != survivor_value
        {
            return Err(SpentnessError::Mismatch(
                "rebuilt terminal pools differ from the fixed consensus pools",
            ));
        }
        self.audit_rebuilt_state(&commitment, cache, yield_control)?;

        let terminal_height = commitment.terminal_height;
        let progress = Progress::Complete {
            rollback_floor: terminal_height,
            commitment,
        };
        let mut batch = DiskWriteBatch::new();
        #[cfg(not(feature = "indexer"))]
        {
            use crate::service::finalized_state::{
                disk_db::WriteDisk, zakura_db::transparent::TX_LOC_BY_SPENT_OUT_LOC,
            };
            let spent_outputs = self
                .db
                .cf_handle(TX_LOC_BY_SPENT_OUT_LOC)
                .expect("spending index column family is declared");
            // The terminal height is a block height, so the next height fits in u32.
            let end = Height(terminal_height + 1);
            batch.zs_delete_range(
                &spent_outputs,
                OutputLocation::from_usize(Height(0), 0, 0),
                OutputLocation::from_usize(end, 0, 0),
            );
        }
        batch.prepare_spentness_progress(self, progress.clone())?;
        self.write_batch(batch)?;
        self.publish_spentness_status(&progress);
        tracing::info!(
            height = terminal_height,
            "spentness index rebuild verified and complete"
        );
        Ok(())
    }

    fn replay_block(
        &self,
        cache: &mut ReplayCache,
        height: Height,
        block: &Block,
    ) -> Result<BlockChanges, SpentnessError> {
        let mut changes = BlockChanges::default();
        for (transaction_index, transaction) in block.transactions.iter().enumerate() {
            self.replay_transaction(cache, height, transaction_index, transaction, &mut changes)?;
        }
        Ok(changes)
    }

    /// Resolve each input from retained history, check the value balance, and record outputs.
    fn replay_transaction(
        &self,
        cache: &mut ReplayCache,
        height: Height,
        transaction_index: usize,
        transaction: &Transaction,
        changes: &mut BlockChanges,
    ) -> Result<(), SpentnessError> {
        let spending = TransactionLocation::from_usize(height, transaction_index);
        if self.transaction_hash(spending) != Some(transaction.hash()) {
            return Err(SpentnessError::Mismatch(
                "rebuild transaction hash differs from its retained index",
            ));
        }
        for outpoint in transaction
            .inputs()
            .iter()
            .filter_map(transparent::Input::outpoint)
        {
            self.replay_spend(cache, height, transaction, spending, outpoint, changes)?;
        }
        if !transaction.is_coinbase() {
            transaction
                .value_balance(&changes.spent)?
                .remaining_transaction_value()?;
        }
        for (output_index, output) in transaction.outputs().iter().enumerate() {
            changes.created.insert(
                OutputLocation::from_usize(height, transaction_index, output_index),
                transparent::Utxo::new(output.clone(), height, transaction.is_coinbase()),
            );
        }
        Ok(())
    }

    /// Resolve one spend. It must follow its output, spend it once, and respect coinbase rules.
    fn replay_spend(
        &self,
        cache: &mut ReplayCache,
        height: Height,
        transaction: &Transaction,
        spending: TransactionLocation,
        outpoint: transparent::OutPoint,
        changes: &mut BlockChanges,
    ) -> Result<(), SpentnessError> {
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
        let spent_earlier = self
            .tx_location_by_spent_output_location(&location)
            .is_some();
        let spent_in_block = changes
            .spending_transactions
            .insert(location, spending)
            .is_some();
        if spent_earlier || spent_in_block {
            return Err(SpentnessError::Mismatch("rebuild found a duplicate spend"));
        }

        let utxo = cache.utxo(self, location)?;
        transparent_coinbase_spend(
            outpoint,
            transaction.coinbase_spend_restriction(&self.network(), height),
            &utxo,
        )
        .map_err(|_| SpentnessError::Mismatch("rebuild found an invalid coinbase spend"))?;
        changes.spent.insert(outpoint, utxo.clone());
        changes.spent_by_location.insert(location, utxo);
        #[cfg(feature = "indexer")]
        changes.spent_locations.insert(outpoint, location);
        Ok(())
    }
}
