//! Insert artifact survivors while checkpoint blocks commit through H.
//!
//! Applying consumes every output bit, including genesis and non-address scripts.
//! It inserts terminal survivors without resolving or deleting spent input UTXOs,
//! retains raw transactions, and defers address indexes to the rebuild. The
//! transparent pool therefore counts only the survivors created so far.

use std::{collections::BTreeMap, time::Instant};

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Hash, Height},
    parallel::tree::NoteCommitmentTrees,
    parameters::spentness_hints::{Commitment, VerifiedArtifact},
    transparent,
    value_balance::ValueBalance,
};

use super::{outputs_in_order, record::PoolAccounting, Progress, SpentnessError};
use crate::{
    request::FinalizedBlock,
    service::finalized_state::{
        disk_db::DiskWriteBatch,
        disk_format::OutputLocation,
        vct::VctWriteData,
        zakura_db::{metrics::block_precommit_metrics, ZakuraDb},
    },
    CommitCheckpointVerifiedError,
};

/// The artifact position and survivor total at a block boundary.
struct Cursor {
    ordinal: u64,
    survivor_value: Amount<NonNegative>,
}

/// One block's survivors, its output count, and the cursor after it.
struct BlockSurvivors {
    utxos: BTreeMap<OutputLocation, transparent::Utxo>,
    outputs: u64,
    cursor: Cursor,
}

impl ZakuraDb {
    /// Commit one checkpoint block while applying, and advance the durable cursor.
    ///
    /// The terminal block moves the record to `Rebuilding` after checking the exact
    /// hash and output count. `commit` writes the batch; a failure marks the run failed
    /// because the batch outcome is uncertain until restart.
    pub(crate) fn write_spentness_block<C>(
        &mut self,
        finalized: FinalizedBlock,
        previous_trees: Option<NoteCommitmentTrees>,
        mut vct_data: VctWriteData,
        commit: C,
    ) -> Result<Hash, CommitCheckpointVerifiedError>
    where
        C: FnOnce(&mut Self, DiskWriteBatch) -> Result<(), CommitCheckpointVerifiedError>,
    {
        let run = self
            .spentness
            .applying
            .clone()
            .ok_or(SpentnessError::WriteOrder(
                "ordinary commits are blocked during index rebuilding",
            ))?;
        let commitment = run.artifact.commitment();
        if finalized.height.0 > commitment.terminal_height {
            return Err(SpentnessError::WriteOrder(
                "body commits cannot cross an incomplete spentness boundary",
            )
            .into());
        }
        let cursor = self.apply_cursor(commitment, finalized.height)?;

        let mut batch = DiskWriteBatch::new();
        let started = Instant::now();
        let survivors = block_survivors(&finalized, &run.artifact, cursor)?;
        batch.prepare_created_utxos(&self.db, &survivors.utxos);
        metrics::histogram!("state.spentness.transparent.prepare_seconds")
            .record(started.elapsed().as_secs_f64());

        let pool = self.applying_value_pool(&finalized, survivors.cursor.survivor_value)?;
        let progress = next_progress(&finalized, commitment, &survivors.cursor)?;
        batch.prepare_block_header_and_transaction_data_batch(self, &finalized, true, None)?;
        batch.prepare_shielded_transaction_batch(self, &finalized);
        vct_data.sync_below = Some(Height(commitment.terminal_height));
        batch.prepare_trees_batch(self, &finalized, previous_trees, vct_data)?;
        batch.prepare_value_pool_records(self, &finalized, pool);
        batch.prepare_spentness_progress(self, progress.clone())?;

        let started = Instant::now();
        commit(self, batch).map_err(|error| {
            self.fail_spentness();
            SpentnessError::Commit(Box::new(error))
        })?;
        self.publish_spentness_status(&progress);
        if matches!(progress, Progress::Rebuilding { .. }) {
            self.spentness.applying = None;
        }
        block_precommit_metrics(&finalized.block, finalized.hash, finalized.height);
        metrics::histogram!("state.spentness.commit_seconds")
            .record(started.elapsed().as_secs_f64());
        record_block_metrics(&survivors, finalized.height);
        Ok(finalized.hash)
    }

    /// The cursor for the next block: zero on an empty database, else the recorded cursor.
    fn apply_cursor(
        &self,
        commitment: &Commitment,
        height: Height,
    ) -> Result<Cursor, SpentnessError> {
        match self.spentness_progress()? {
            None if self.tip().is_none() && height.is_min() => Ok(Cursor {
                ordinal: 0,
                survivor_value: Amount::zero(),
            }),
            Some(Progress::Applying {
                commitment: recorded,
                next_ordinal,
                survivor_value,
                ..
            }) if &recorded == commitment => Ok(Cursor {
                ordinal: next_ordinal,
                survivor_value: Amount::try_from(survivor_value)?,
            }),
            _ => Err(SpentnessError::WriteOrder(
                "unexpected spentness write phase",
            )),
        }
    }

    /// The exact shielded and deferred pools, with transparent value from survivors only.
    fn applying_value_pool(
        &self,
        finalized: &FinalizedBlock,
        survivor_value: Amount<NonNegative>,
    ) -> Result<ValueBalance<NonNegative>, SpentnessError> {
        let mut pool = self.finalized_value_pool();
        if !finalized.height.is_min() {
            let change = finalized
                .block
                .shielded_chain_value_pool_change(finalized.deferred_pool_balance_change)?;
            pool = pool.add_chain_value_pool_change(change)?;
        }
        pool.set_transparent_value_balance(ValueBalance::from_transparent_amount(survivor_value));
        Ok(pool)
    }
}

/// Read one membership bit per output, and collect the outputs the artifact retains.
fn block_survivors(
    finalized: &FinalizedBlock,
    artifact: &VerifiedArtifact,
    mut cursor: Cursor,
) -> Result<BlockSurvivors, SpentnessError> {
    let first_ordinal = cursor.ordinal;
    let mut utxos = BTreeMap::new();
    for (location, transaction, output) in outputs_in_order(&finalized.block, finalized.height) {
        let retained = artifact.retains(cursor.ordinal)?;
        // `retains` accepted the ordinal, so it is below the output count and cannot overflow.
        cursor.ordinal += 1;
        if !retained {
            continue;
        }
        if finalized.height.is_min() {
            return Err(SpentnessError::Mismatch(
                "artifact retains a genesis output",
            ));
        }
        cursor.survivor_value = (cursor.survivor_value + output.value)?;
        let utxo =
            transparent::Utxo::new(output.clone(), finalized.height, transaction.is_coinbase());
        utxos.insert(location, utxo);
    }
    Ok(BlockSurvivors {
        utxos,
        outputs: cursor.ordinal - first_ordinal,
        cursor,
    })
}

/// The record to store with this block. The terminal block starts the rebuild.
fn next_progress(
    finalized: &FinalizedBlock,
    commitment: &Commitment,
    cursor: &Cursor,
) -> Result<Progress, SpentnessError> {
    if finalized.height.0 != commitment.terminal_height {
        return Ok(Progress::Applying {
            commitment: commitment.clone(),
            height: finalized.height.0,
            block_hash: finalized.hash.0,
            next_ordinal: cursor.ordinal,
            survivor_value: cursor.survivor_value.into(),
        });
    }
    if finalized.hash.0 != commitment.terminal_block_hash
        || cursor.ordinal != commitment.output_count
    {
        return Err(SpentnessError::Mismatch(
            "terminal hash or output count differs from the commitment",
        ));
    }
    Ok(Progress::Rebuilding {
        commitment: commitment.clone(),
        indexed_height: None,
        replay_accounting: PoolAccounting::default(),
        survivor_value: cursor.survivor_value.into(),
    })
}

fn record_block_metrics(survivors: &BlockSurvivors, height: Height) {
    let inserted =
        u64::try_from(survivors.utxos.len()).expect("usize fits in u64 on supported targets");
    metrics::counter!("state.spentness.bits").increment(survivors.outputs);
    metrics::counter!("state.spentness.utxo.inserts").increment(inserted);
    // Applying never reads or deletes UTXOs. Report zero so benchmarks can confirm it.
    metrics::counter!("state.spentness.utxo.reads").increment(0);
    metrics::counter!("state.spentness.utxo.deletes").increment(0);
    metrics::counter!("state.spentness.utxo.omitted").increment(survivors.outputs - inserted);
    metrics::gauge!("state.spentness.construction_height").set(f64::from(height.0));
}
