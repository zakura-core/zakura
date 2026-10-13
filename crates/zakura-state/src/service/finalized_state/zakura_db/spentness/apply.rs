//! Resolve each checkpoint block's spends and membership bits for the ordinary writer.
//!
//! Applying consumes every output bit, including genesis and non-address scripts.
//! The ordinary writer inserts survivors into the UTXO set and writes every index,
//! balance, and value pool. It writes no UTXO rows for omitted outputs, because
//! blocks spend them before H. Spends resolve from [`LiveOutputs`] in memory, or from
//! the omitted-output journal after an eviction or a restart.
//!
//! Only omitted outputs enter the map and the journal, so each resolved spend proves
//! that the artifact omits its output. At H, the resolved spends must equal the
//! omitted outputs. A valid chain never spends an output twice, so the omitted
//! outputs are then exactly the spent outputs, and the survivors are exactly the UTXO
//! set at H.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use rayon::prelude::*;
use zakura_chain::{
    block::{Hash, Height},
    parameters::spentness_hints::Commitment,
    transaction, transparent,
};

use super::{
    live::{encode_journal, find_in_journal, OmittedOutput},
    outputs_in_order,
    record::OMITTED_OUTPUTS,
    ApplyingRun, Progress, SpentnessError,
};
use crate::{
    request::FinalizedBlock,
    service::finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        disk_format::{OutputLocation, RawBytes, TransactionLocation},
        zakura_db::{ZakuraDb, PARALLEL_BLOCK_READ_THRESHOLD},
    },
};

/// A spend of an output that the artifact retains, or of an output that no earlier block created.
const UNRESOLVED_SPEND: SpentnessError =
    SpentnessError::Mismatch("a block spends an output that the artifact retains at H");

/// One block's spentness writes, prepared before the ordinary block batch.
pub(crate) struct HintedBlock {
    /// Created outputs that the artifact omits. They get no UTXO rows.
    pub(crate) omitted: HashSet<OutputLocation>,
    /// Every spent output, in block order.
    pub(crate) spent: Vec<(transparent::OutPoint, OutputLocation, transparent::Utxo)>,
    /// This block's omitted outputs that it does not spend itself.
    journal: Vec<u8>,
    progress: Progress,
    hits: u64,
    misses: u64,
}

/// The artifact position and spentness counts at a block boundary.
struct Cursor {
    ordinal: u64,
    omitted: u64,
    resolved: u64,
}

impl ZakuraDb {
    /// The applying run that must take a block at `height`, or `None` for ordinary state.
    pub(crate) fn hinted_run_for(
        &self,
        height: Height,
    ) -> Result<Option<Arc<ApplyingRun>>, SpentnessError> {
        if !self.spentness_incomplete() {
            return Ok(None);
        }
        let run = self
            .spentness
            .applying
            .clone()
            .ok_or(SpentnessError::WriterStopped)?;
        if height.0 > run.artifact.commitment().terminal_height {
            return Err(SpentnessError::WriteOrder(
                "body commits cannot cross an incomplete spentness boundary",
            ));
        }
        Ok(Some(run))
    }

    /// Read the membership bits of `finalized`'s outputs and resolve its spends.
    pub(crate) fn prepare_hinted_block(
        &self,
        run: &ApplyingRun,
        finalized: &FinalizedBlock,
        tx_hash_indexes: &HashMap<transaction::Hash, usize>,
    ) -> Result<HintedBlock, SpentnessError> {
        let commitment = run.artifact.commitment();
        let height = finalized.height;
        let network = self.network();
        let mut cursor = self.apply_cursor(commitment, height)?;

        let mut omitted = HashSet::new();
        for (location, _, _) in outputs_in_order(&finalized.block, height) {
            let retained = run.artifact.retains(cursor.ordinal)?;
            // `retains` accepted the ordinal, so it is below the output count and cannot overflow.
            cursor.ordinal += 1;
            if height.is_min() {
                // Genesis outputs are unspendable, so they are neither survivors nor omitted.
                if retained {
                    return Err(SpentnessError::Mismatch(
                        "artifact retains a genesis output",
                    ));
                }
            } else if !retained {
                omitted.insert(location);
            }
        }

        let mut live = run.live.lock().map_err(|_| {
            SpentnessError::Inconsistent("a writer panicked while holding the live-output map")
        })?;
        let mut spent = Vec::new();
        let mut misses = Vec::new();
        let mut spent_here = HashSet::new();
        for outpoint in finalized
            .block
            .transactions
            .iter()
            .flat_map(|transaction| transaction.inputs())
            .filter_map(transparent::Input::outpoint)
        {
            if let Some(tx_index) = tx_hash_indexes.get(&outpoint.hash) {
                let location = OutputLocation::from_outpoint(
                    TransactionLocation::from_usize(height, *tx_index),
                    &outpoint,
                );
                if !omitted.contains(&location) {
                    return Err(UNRESOLVED_SPEND);
                }
                let utxo = finalized
                    .new_outputs
                    .get(&outpoint)
                    .ok_or(UNRESOLVED_SPEND)?
                    .utxo
                    .clone();
                spent_here.insert(location);
                spent.push(Some((outpoint, location, utxo)));
            } else if let Some(output) = live.take(&outpoint) {
                spent.push(Some((outpoint, output.location, output.utxo())));
            } else {
                misses.push((spent.len(), outpoint));
                spent.push(None);
            }
        }

        let resolve = |(slot, outpoint): (usize, transparent::OutPoint)| {
            let output = self.journal_output(&outpoint)?;
            Ok::<_, SpentnessError>((slot, (outpoint, output.location, output.utxo())))
        };
        let resolved: Vec<_> = if misses.len() >= PARALLEL_BLOCK_READ_THRESHOLD {
            misses
                .par_iter()
                .copied()
                .map(resolve)
                .collect::<Result<_, _>>()?
        } else {
            misses
                .iter()
                .copied()
                .map(resolve)
                .collect::<Result<_, _>>()?
        };
        let misses = count(resolved.len());
        for (slot, spend) in resolved {
            spent[slot] = Some(spend);
        }
        // Every miss resolved above, so every slot is filled.
        let spent: Vec<_> = spent
            .into_iter()
            .map(|spend| spend.ok_or(UNRESOLVED_SPEND))
            .collect::<Result<_, _>>()?;
        let spends = count(spent.len());
        cursor.omitted += count(omitted.len());
        // Distinct spends of omitted outputs cannot outnumber them, and the count
        // check at H rejects any excess.
        cursor.resolved += spends;

        let mut journal = Vec::new();
        for (location, transaction, output) in outputs_in_order(&finalized.block, height) {
            if !omitted.contains(&location) || spent_here.contains(&location) {
                continue;
            }
            let output = OmittedOutput::new(location, output, transaction.is_coinbase(), &network);
            journal.push(output.clone());
            let outpoint = transparent::OutPoint::from_usize(
                finalized.transaction_hashes[location.transaction_index().as_usize()],
                location.output_index().as_usize(),
            );
            live.insert(outpoint, output);
        }
        let evicted = live.evict_if_full();
        metrics::counter!("state.spentness.live.evicted").increment(count(evicted));
        // The map holds far fewer than 2^52 entries, so the gauge value is exact.
        metrics::gauge!("state.spentness.live.entries").set(live.len() as f64);

        Ok(HintedBlock {
            omitted,
            spent,
            journal: if journal.is_empty() {
                Vec::new()
            } else {
                encode_journal(&journal)
            },
            progress: next_progress(finalized, commitment, &cursor)?,
            hits: spends - misses,
            misses,
        })
    }

    /// Find an omitted output in the journal of the block that created it.
    fn journal_output(
        &self,
        outpoint: &transparent::OutPoint,
    ) -> Result<OmittedOutput, SpentnessError> {
        let location = self
            .transaction_location(outpoint.hash)
            .ok_or(UNRESOLVED_SPEND)?;
        let journal = self
            .db
            .cf_handle(OMITTED_OUTPUTS)
            .expect("omitted-output column family is declared");
        let bytes: RawBytes = self
            .db
            .zs_get(&journal, &location.height)
            .ok_or(UNRESOLVED_SPEND)?;
        find_in_journal(
            bytes.raw_bytes(),
            location.height,
            location.index,
            outpoint.index,
            &self.network(),
        )?
        .ok_or(UNRESOLVED_SPEND)
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
                omitted: 0,
                resolved: 0,
            }),
            Some(Progress::Applying {
                commitment: recorded,
                next_ordinal,
                omitted_outputs,
                resolved_spends,
                ..
            }) if &recorded == commitment => Ok(Cursor {
                ordinal: next_ordinal,
                omitted: omitted_outputs,
                resolved: resolved_spends,
            }),
            _ => Err(SpentnessError::WriteOrder(
                "unexpected spentness write phase",
            )),
        }
    }

    /// Publish the committed phase, and release the artifact at H.
    pub(crate) fn finish_hinted_block(&mut self, hinted: &HintedBlock, height: Height) {
        self.publish_spentness_status(&hinted.progress);
        if matches!(hinted.progress, Progress::Complete { .. }) {
            self.spentness.applying = None;
            tracing::info!(
                height = height.0,
                "spentness construction verified and complete"
            );
        }
        metrics::counter!("state.spentness.spends.hits").increment(hinted.hits);
        metrics::counter!("state.spentness.spends.misses").increment(hinted.misses);
        metrics::counter!("state.spentness.utxo.omitted").increment(count(hinted.omitted.len()));
        metrics::gauge!("state.spentness.construction_height").set(f64::from(height.0));
    }
}

impl DiskWriteBatch {
    /// Store the block's journal records and the next progress record.
    ///
    /// The terminal block deletes the journal, because every omitted output is spent.
    pub(crate) fn prepare_hinted_block(
        &mut self,
        db: &ZakuraDb,
        hinted: &HintedBlock,
        height: Height,
    ) -> Result<(), SpentnessError> {
        let journal = db
            .db
            .cf_handle(OMITTED_OUTPUTS)
            .expect("omitted-output column family is declared");
        if matches!(hinted.progress, Progress::Complete { .. }) {
            // The terminal height is a block height, so the next height fits in u32.
            self.zs_delete_range(&journal, Height(0), Height(height.0 + 1));
        } else if !hinted.journal.is_empty() {
            self.zs_insert(
                &journal,
                height,
                RawBytes::new_raw_bytes(hinted.journal.clone()),
            );
        }
        self.prepare_spentness_progress(db, hinted.progress.clone())
    }
}

/// Convert a collection length to a counter value.
fn count(len: usize) -> u64 {
    // Supported targets have at most 64-bit pointers, so every length fits in u64.
    len as u64
}

/// The record to store with this block. The terminal block completes the run.
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
            omitted_outputs: cursor.omitted,
            resolved_spends: cursor.resolved,
        });
    }
    if Hash(commitment.terminal_block_hash) != finalized.hash
        || cursor.ordinal != commitment.output_count
    {
        return Err(SpentnessError::Mismatch(
            "terminal hash or output count differs from the commitment",
        ));
    }
    if cursor.resolved != cursor.omitted {
        return Err(SpentnessError::Mismatch(
            "the artifact omits outputs that no block spends before H",
        ));
    }
    Ok(Progress::Complete {
        commitment: commitment.clone(),
        rollback_floor: commitment.terminal_height,
    })
}
