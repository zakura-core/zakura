//! Shared block, header, and transaction reading code.
//!
//! In the functions in this module:
//!
//! The block write task commits blocks to the finalized state before updating
//! `chain` or `non_finalized_state` with a cached copy of the non-finalized chains
//! in `NonFinalizedState.chain_set`. Then the block commit task can
//! commit additional blocks to the finalized state after we've cloned the
//! `chain` or `non_finalized_state`.
//!
//! This means that some blocks can be in both:
//! - the cached [`Chain`] or [`NonFinalizedState`], and
//! - the shared finalized [`ZakuraDb`] reference.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};
use zakura_node_services::block_memory::{
    AdmissionFailure, BlockHandle, HeaderHandle, ReadMemoryExhausted, TransactionHandle,
};

use chrono::{DateTime, Utc};

use zakura_chain::{
    block::{self, Height},
    block_info::BlockInfo,
    serialization::ZcashSerialize as _,
    transaction::{self},
    transparent::{self, Utxo},
};

use crate::{
    response::{AnyTx, MinedTx, ParentInputs},
    service::{
        finalized_state::ZakuraDb,
        non_finalized_state::{Chain, NonFinalizedState},
        read::tip_height,
    },
    HashOrHeight,
};

#[cfg(feature = "indexer")]
use crate::request::Spend;

/// Returns the [`Block`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chains` or finalized `db`.
pub fn any_block<'a, C: AsRef<Chain> + 'a>(
    mut chains: impl Iterator<Item = &'a C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Result<Option<BlockHandle>, AdmissionFailure> {
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    if let Some(contextual) = chains.find_map(|c| c.as_ref().block(hash_or_height)) {
        return Ok(Some(contextual.block.clone()));
    }
    db.block(hash_or_height)
}

/// Returns the [`Block`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chain` or finalized `db`.
pub fn block<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Result<Option<BlockHandle>, AdmissionFailure>
where
    C: AsRef<Chain>,
{
    any_block(chain.iter(), db, hash_or_height)
}

/// Returns the [`Block`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chain` or finalized `db`.
pub fn block_and_size<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Result<Option<(BlockHandle, usize)>, AdmissionFailure>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    if let Some(contextual) = chain
        .as_ref()
        .and_then(|chain| chain.as_ref().block(hash_or_height))
    {
        let size = contextual.block.zcash_serialized_size();
        return Ok(Some((contextual.block.clone(), size)));
    }
    db.block_and_size(hash_or_height)
}

/// Returns the [`block::Header`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chain` or finalized `db`.
///
/// Finalized reads take no admission slot, so chain-context work cannot starve.
/// Use [`block_header_result`] for a header that leaves its read job.
pub fn block_header<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<HeaderHandle>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chain
        .as_ref()
        .and_then(|chain| chain.as_ref().block(hash_or_height))
        .map(|contextual| contextual.block.retained_header())
        .or_else(|| db.block_header(hash_or_height).map(Into::into))
}

/// Reads a header that leaves its read job. A finalized header retains admission
/// through its final holder. Capacity refusal is separate from missing data.
pub fn block_header_result<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Result<Option<HeaderHandle>, ReadMemoryExhausted>
where
    C: AsRef<Chain>,
{
    if let Some(contextual) = chain
        .as_ref()
        .and_then(|chain| chain.as_ref().block(hash_or_height))
    {
        return Ok(Some(contextual.block.retained_header()));
    }
    db.block_header_result(hash_or_height)
}

/// Returns the [`Transaction`] with [`transaction::Hash`], if it exists in the
/// non-finalized `chain` or finalized `db`.
fn transaction<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash: transaction::Hash,
) -> Result<Option<(TransactionHandle, Height, DateTime<Utc>)>, ReadMemoryExhausted>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since transactions are the same in the finalized and non-finalized state,
    // we check the most efficient alternative first. (`chain` is always in
    // memory, but `db` stores transactions on disk, with a memory cache.)
    if let Some(transaction) = chain.and_then(|chain| {
        chain
            .as_ref()
            .transaction(hash)
            .map(|(tx, height, time)| (tx.clone(), height, time))
    }) {
        return Ok(Some(transaction));
    }
    db.transaction(hash)
}

/// Returns a [`MinedTx`] for a [`Transaction`] with [`transaction::Hash`],
/// if one exists in the non-finalized `chain` or finalized `db`.
pub fn mined_transaction<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash: transaction::Hash,
) -> Result<Option<MinedTx>, ReadMemoryExhausted>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // It is ok to do this lookup in two different calls. Finalized state updates
    // can only add overlapping blocks, and hashes are unique.
    let chain = chain.as_ref();

    let Some((tx, height, time)) = transaction(chain, db, hash)? else {
        return Ok(None);
    };
    Ok(tip_height(chain, db).map(|tip| MinedTx::new(tx, height, 1 + tip.0 - height.0, time)))
}

/// Returns a [`AnyTx`] for a [`Transaction`] with [`transaction::Hash`],
/// if one exists in any chain in `chains` or finalized `db`.
/// The first chain in `chains` must be the best chain.
pub fn any_transaction<'a>(
    chains: impl Iterator<Item = &'a Arc<Chain>>,
    db: &ZakuraDb,
    hash: transaction::Hash,
) -> Result<Option<AnyTx>, ReadMemoryExhausted> {
    // # Correctness
    //
    // It is ok to do this lookup in multiple different calls. Finalized state updates
    // can only add overlapping blocks, and hashes are unique.
    //
    // Capture the best chain tip before searching, not inside the search closure.
    // The closure only runs when the tx is found in a non-finalized chain; if the tx
    // is only in the finalized DB, the closure never fires and best_chain would stay
    // None, causing tip_height to undercount confirmations by ~MAX_BLOCK_REORG_HEIGHT.
    // See <https://github.com/ZcashFoundation/zebra/issues/10470>.
    // peekable() reads the first element without consuming it, so the iterator can
    // still be used in find_map below.
    let mut chains = chains.peekable();
    let best_chain = chains.peek().copied();
    let in_memory = chains.enumerate().find_map(|(i, chain)| {
        chain
            .as_ref()
            .transaction(hash)
            .map(|(tx, height, time)| (tx.clone(), height, time, i == 0, Some(chain)))
    });
    let transaction = match in_memory {
        Some(transaction) => Some(transaction),
        None => db
            .transaction(hash)?
            .map(|(tx, height, time)| (tx, height, time, true, None)),
    };
    let Some((tx, height, time, in_best_chain, containing_chain)) = transaction else {
        return Ok(None);
    };

    if in_best_chain {
        Ok(tip_height(best_chain, db)
            .map(|tip| AnyTx::Mined(MinedTx::new(tx, height, 1 + tip.0 - height.0, time))))
    } else {
        Ok(containing_chain
            .and_then(|chain| chain.block(height.into()))
            .map(|block| AnyTx::Side((tx, block.hash))))
    }
}

/// Returns the [`transaction::Hash`]es for the block with `hash_or_height`,
/// if it exists in the non-finalized `chain` or finalized `db`.
///
/// The returned hashes are in block order.
///
/// Returns `None` if the block is not found.
pub fn transaction_hashes_for_block<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<Arc<[transaction::Hash]>>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chain
        .as_ref()
        .and_then(|chain| chain.as_ref().transaction_hashes_for_block(hash_or_height))
        .or_else(|| db.transaction_hashes_for_block(hash_or_height))
}

/// Returns the [`transaction::Hash`]es for the block with `hash_or_height`,
/// if it exists in any chain in `chains` or finalized `db`.
/// The first chain in `chains` must be the best chain.
///
/// The returned hashes are in block order.
///
/// Returns `None` if the block is not found.
pub fn transaction_hashes_for_any_block<'a>(
    chains: impl Iterator<Item = &'a Arc<Chain>>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<(Arc<[transaction::Hash]>, bool)> {
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (`chain` is always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chains
        .enumerate()
        .find_map(|(i, chain)| {
            chain
                .as_ref()
                .transaction_hashes_for_block(hash_or_height)
                .map(|hashes| (hashes.clone(), i == 0))
        })
        .or_else(|| {
            db.transaction_hashes_for_block(hash_or_height)
                .map(|hashes| (hashes, true))
        })
}

/// Returns the [`Utxo`] for [`transparent::OutPoint`], if it exists in the
/// non-finalized `chain` or finalized `db`.
///
/// Non-finalized UTXOs are returned regardless of whether they have been spent.
///
/// Finalized UTXOs are only returned if they are unspent in the finalized chain.
/// They may have been spent in the non-finalized chain,
/// but this function returns them without checking for non-finalized spends,
/// because we don't know which non-finalized chain will be committed to the finalized state.
pub fn utxo<C>(chain: Option<C>, db: &ZakuraDb, outpoint: transparent::OutPoint) -> Option<Utxo>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since UTXOs are the same in the finalized and non-finalized state,
    // we check the most efficient alternative first. (`chain` is always in
    // memory, but `db` stores transactions on disk, with a memory cache.)
    chain
        .and_then(|chain| chain.as_ref().created_utxo(&outpoint))
        .or_else(|| db.utxo(&outpoint).map(|utxo| utxo.utxo))
}

/// Returns the [`Utxo`] for [`transparent::OutPoint`], if it exists and is unspent in the
/// non-finalized `chain` or finalized `db`.
pub fn unspent_utxo<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    outpoint: transparent::OutPoint,
) -> Option<Utxo>
where
    C: AsRef<Chain>,
{
    match chain {
        Some(chain) if chain.as_ref().spent_utxos.contains_key(&outpoint) => None,
        chain => utxo(chain, db, outpoint),
    }
}

/// The smallest number of parent inputs read in parallel.
///
/// Measured on Mainnet replays: below this, Rayon's overhead outweighs overlapping the reads,
/// and at 32 a block with 29 transparent inputs was still about 1.3 ms slower than Zebra.
const MIN_PARALLEL_PARENT_INPUT_READS: usize = 8;

/// The smallest number of parent inputs read on [`PARENT_INPUT_READ_POOL`].
///
/// Measured on cold-cache Mainnet replays: from 128 inputs the deeper I/O queue made blocks 2-7%
/// faster, while sending every parallel read through the wide pool made smaller blocks 1-3%
/// slower.
const MIN_WIDE_PARENT_INPUT_READS: usize = 128;

/// A dedicated pool for reading the parent inputs of large blocks.
///
/// These reads mostly wait on disk when the UTXO set is not cached, so more threads than cores
/// keep more reads in flight than the shared pool can. `None` if the threads could not be
/// created, in which case large blocks read on the shared pool.
static PARENT_INPUT_READ_POOL: LazyLock<Option<rayon::ThreadPool>> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(64)
        .thread_name(|i| format!("parent-input-read-{i}"))
        .build()
        .inspect_err(|error| {
            tracing::warn!(
                ?error,
                "could not create the parent input read pool, using the shared pool"
            )
        })
        .ok()
});

/// The most locking-script bytes a parent input check keeps for the transaction checks.
///
/// The check keeps this map while the block's transaction checks run, on top of the copies those
/// checks already make, so it is kept small. A full 2 MB block of standard 25-byte scripts keeps
/// at most about 1.2 MB. A block whose inputs all exist but pass this limit, valid or not, gets
/// [`ParentInputs::Inconclusive`], so its transactions look their inputs up one by one, as they
/// did before the check returned outputs. A missing input still gets [`ParentInputs::Missing`].
const MAX_KEPT_PARENT_INPUT_BYTES: usize = 4 * 1024 * 1024;

/// The result of reading one input in [`read_parent_inputs`].
enum ParentInputRead {
    /// The output, kept for the transaction checks.
    Kept(Utxo),
    /// The output exists, but keeping it would pass the byte limit.
    Dropped,
    /// The output is not unspent at the parent.
    Missing,
    /// Not read, because an earlier input is already known to be missing.
    Skipped,
}

/// Reads `outpoints` with `lookup`, which returns an output if it is unspent at the parent.
///
/// Returns [`ParentInputs::Missing`] with the first missing input in `outpoints` order, or
/// [`ParentInputs::Unspent`] if every input was found and their locking scripts fit in
/// `max_kept_bytes`, or [`ParentInputs::Inconclusive`] otherwise. Repeated outpoints are read
/// once, and inputs after one already known to be missing are not read, so an invalid block can't
/// make this keep more than `max_kept_bytes` of scripts.
fn read_parent_inputs(
    outpoints: &[transparent::OutPoint],
    max_kept_bytes: usize,
    lookup: impl Fn(&transparent::OutPoint) -> Option<Utxo> + Sync,
) -> ParentInputs {
    let mut seen = HashSet::with_capacity(outpoints.len());
    let outpoints: Vec<transparent::OutPoint> = outpoints
        .iter()
        .copied()
        .filter(|outpoint| seen.insert(*outpoint))
        .collect();

    // The lowest index found missing so far. Only inputs after it are skipped, so every input
    // before the first missing one is still read and the reported input stays first in order.
    let first_missing = AtomicUsize::new(usize::MAX);
    let kept_bytes = AtomicUsize::new(0);
    let read = |(index, outpoint): (usize, &transparent::OutPoint)| {
        if index > first_missing.load(Ordering::Relaxed) {
            return ParentInputRead::Skipped;
        }
        let Some(utxo) = lookup(outpoint) else {
            first_missing.fetch_min(index, Ordering::Relaxed);
            return ParentInputRead::Missing;
        };
        let size = utxo.output.lock_script.as_raw_bytes().len();
        let fits = kept_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |kept| {
                kept.checked_add(size)
                    .filter(|&total| total <= max_kept_bytes)
            })
            .is_ok();
        if fits {
            ParentInputRead::Kept(utxo)
        } else {
            ParentInputRead::Dropped
        }
    };
    let reads: Vec<ParentInputRead> = if outpoints.len() >= MIN_PARALLEL_PARENT_INPUT_READS {
        use rayon::prelude::*;
        let read_all = || outpoints.par_iter().enumerate().map(read).collect();
        let wide_pool = (outpoints.len() >= MIN_WIDE_PARENT_INPUT_READS)
            .then(|| PARENT_INPUT_READ_POOL.as_ref())
            .flatten();
        match wide_pool {
            Some(pool) => pool.install(read_all),
            None => read_all(),
        }
    } else {
        outpoints.iter().enumerate().map(read).collect()
    };

    let mut utxos = HashMap::with_capacity(outpoints.len());
    let mut complete = true;
    for (outpoint, read) in outpoints.iter().zip(reads) {
        match read {
            ParentInputRead::Kept(utxo) => {
                utxos.insert(*outpoint, utxo);
            }
            ParentInputRead::Missing => return ParentInputs::Missing(*outpoint),
            // Skipped inputs only follow a missing one, which returns above.
            ParentInputRead::Dropped | ParentInputRead::Skipped => complete = false,
        }
    }
    if complete {
        ParentInputs::Unspent(Arc::new(utxos))
    } else {
        ParentInputs::Inconclusive
    }
}

/// Checks `outpoints` against the UTXO set at `parent`.
///
/// Returns [`ParentInputs::Unspent`] with every spent output, or [`ParentInputs::Missing`] with
/// the first missing input in `outpoints` order, as [`read_parent_inputs`] describes. Returns
/// either one only if `parent` is the finalized tip, or if `parent` is in a non-finalized chain
/// that continues the finalized tip. Finalization can remove an output that a block after
/// `parent` spends, so callers must discard the result if the finalized tip changes during this
/// read.
pub fn parent_inputs(
    non_finalized_state: &NonFinalizedState,
    db: &ZakuraDb,
    (finalized_height, finalized_hash): (Height, block::Hash),
    parent: block::Hash,
    outpoints: &[transparent::OutPoint],
) -> ParentInputs {
    let read_inputs = |chain: Option<&Arc<Chain>>| {
        read_parent_inputs(outpoints, MAX_KEPT_PARENT_INPUT_BYTES, |outpoint| {
            unspent_utxo(chain, db, *outpoint)
        })
    };

    // The database holds exactly the finalized tip's UTXO set.
    if parent == finalized_hash {
        return read_inputs(None);
    }
    // No chain can accept a child of a block below the finalized tip.
    if db.height(parent).is_some() {
        return ParentInputs::ParentUnavailable;
    }

    // A fork at `parent` reverts the outputs created and spent above it.
    let Some(chain) = non_finalized_state
        .find_chain(|chain| chain.non_finalized_tip_hash() == parent)
        .or_else(|| {
            non_finalized_state
                .chain_iter()
                .find_map(|chain| chain.fork(parent))
                .map(Arc::new)
        })
    else {
        return ParentInputs::ParentUnavailable;
    };

    // The database supplies the outputs below the chain, so the chain must continue the
    // finalized tip without a gap.
    let root_height = chain.non_finalized_root_height();
    let continues_finalized_tip = if finalized_height.next().ok() == Some(root_height) {
        chain
            .blocks
            .values()
            .next()
            .map(|root| root.block.header().previous_block_hash)
            == Some(finalized_hash)
    } else {
        root_height <= finalized_height
            && chain.hash_by_height(finalized_height) == Some(finalized_hash)
    };
    if !continues_finalized_tip {
        return ParentInputs::Inconclusive;
    }

    read_inputs(Some(&chain))
}

/// Returns the [`Hash`](transaction::Hash) of the transaction that spent an output at
/// the provided [`transparent::OutPoint`] or revealed the provided nullifier, if it exists
/// and is spent or revealed in the non-finalized `chain` or finalized `db` and its
/// spending transaction hash has been indexed.
///
/// # Errors
///
/// Returns [`TransparentIndexesUnavailable`](crate::TransparentIndexesUnavailable) for every
/// [`Spend::OutPoint`] if `db` skips transparent indexes. A non-finalized answer could only be
/// partial, so it is not returned either.
#[cfg(feature = "indexer")]
pub fn spending_transaction_hash<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    spend: Spend,
) -> Result<Option<transaction::Hash>, crate::TransparentIndexesUnavailable>
where
    C: AsRef<Chain>,
{
    if matches!(spend, Spend::OutPoint(_)) {
        db.check_transparent_indexes_available()?;
    }

    match chain.and_then(|chain| chain.as_ref().spending_transaction_hash(&spend)) {
        Some(hash) => Ok(Some(hash)),
        None => db.spending_transaction_hash(&spend),
    }
}

/// Returns the [`Utxo`] for [`transparent::OutPoint`], if it exists in any chain
/// in the `non_finalized_state`, or in the finalized `db`.
///
/// Non-finalized UTXOs are returned regardless of whether they have been spent.
///
/// Finalized UTXOs are only returned if they are unspent in the finalized chain.
/// They may have been spent in one or more non-finalized chains,
/// but this function returns them without checking for non-finalized spends,
/// because we don't know which non-finalized chain the request belongs to.
///
/// UTXO spends are checked once the block reaches the non-finalized state,
/// by [`check::utxo::transparent_spend()`](crate::service::check::utxo::transparent_spend).
pub fn any_utxo(
    non_finalized_state: NonFinalizedState,
    db: &ZakuraDb,
    outpoint: transparent::OutPoint,
) -> Option<Utxo> {
    // # Correctness
    //
    // Since UTXOs are the same in the finalized and non-finalized state,
    // we check the most efficient alternative first. (`non_finalized_state` is always in
    // memory, but `db` stores transactions on disk, with a memory cache.)
    non_finalized_state
        .any_utxo(&outpoint)
        .or_else(|| db.utxo(&outpoint).map(|utxo| utxo.utxo))
}

/// Returns the [`BlockInfo`] with [`block::Hash`] or
/// [`Height`], if it exists in the non-finalized `chain` or finalized `db`.
pub fn block_info<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<BlockInfo>
where
    C: AsRef<Chain>,
{
    any_block_info(chain.iter(), db, hash_or_height)
}

/// Returns the [`BlockInfo`] for `hash_or_height` in `non_finalized_state` or `db`.
///
/// A hash resolves on any non-finalized chain, because a block verifier looks up its
/// parent, which can be on a side chain. A height resolves only on the best chain, because
/// a taller side chain can contain heights the best chain doesn't have yet.
pub fn block_info_by_hash_or_best_chain_height(
    non_finalized_state: &NonFinalizedState,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<BlockInfo> {
    match hash_or_height {
        HashOrHeight::Hash(_) => {
            any_block_info(non_finalized_state.chain_iter(), db, hash_or_height)
        }
        HashOrHeight::Height(_) => block_info(non_finalized_state.best_chain(), db, hash_or_height),
    }
}

/// Returns the [`BlockInfo`] of the block with [`block::Hash`] or [`Height`], if it
/// exists in any of the non-finalized `chains` or in the finalized `db`.
///
/// Heights resolve on the first chain in `chains` that has them, so use
/// [`block_info_by_hash_or_best_chain_height`] to look up heights in the best chain.
pub fn any_block_info<'a, C: AsRef<Chain> + 'a>(
    mut chains: impl Iterator<Item = &'a C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<BlockInfo> {
    // # Correctness
    //
    // Since blocks are the same in the finalized and non-finalized state, we
    // check the most efficient alternative first. (the chains are always in memory,
    // but `db` stores blocks on disk, with a memory cache.)
    chains
        .find_map(|chain| chain.as_ref().block_info(hash_or_height))
        .or_else(|| db.block_info(hash_or_height))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use zakura_chain::{amount::Amount, transparent::Script};

    use super::*;

    /// Returns `count` distinct outpoints.
    fn outpoints(count: usize) -> Vec<transparent::OutPoint> {
        (0..count)
            .map(|index| transparent::OutPoint {
                hash: transaction::Hash([7; 32]),
                index: u32::try_from(index).expect("test counts fit in u32"),
            })
            .collect()
    }

    /// Returns an output with a `script_len`-byte locking script.
    fn utxo(script_len: usize) -> Utxo {
        Utxo::new(
            transparent::Output {
                value: Amount::zero(),
                lock_script: Script::new(&vec![0; script_len]),
            },
            Height(1),
            false,
        )
    }

    /// Both parallel paths return every output, and report the first missing input in order.
    #[test]
    fn parallel_reads_keep_outputs_and_input_order() {
        for count in [MIN_PARALLEL_PARENT_INPUT_READS, MIN_WIDE_PARENT_INPUT_READS] {
            let inputs = outpoints(count);

            let ParentInputs::Unspent(utxos) =
                read_parent_inputs(&inputs, usize::MAX, |_| Some(utxo(1)))
            else {
                panic!("every input exists, so {count} inputs are unspent");
            };
            assert_eq!(utxos.len(), count);

            let (first, second) = (inputs[count / 2], inputs[count - 1]);
            assert_eq!(
                read_parent_inputs(&inputs, usize::MAX, |outpoint| {
                    (*outpoint != first && *outpoint != second).then(|| utxo(1))
                }),
                ParentInputs::Missing(first),
                "{count} inputs"
            );
        }
    }

    /// Outputs past the byte limit make the check inconclusive, unless an input is missing.
    #[test]
    fn kept_output_bytes_are_limited() {
        let inputs = outpoints(MIN_WIDE_PARENT_INPUT_READS);
        let total = inputs.len() * 10;

        assert!(matches!(
            read_parent_inputs(&inputs, total, |_| Some(utxo(10))),
            ParentInputs::Unspent(_)
        ));
        assert_eq!(
            read_parent_inputs(&inputs, total - 1, |_| Some(utxo(10))),
            ParentInputs::Inconclusive
        );

        let missing = inputs[inputs.len() - 1];
        assert_eq!(
            read_parent_inputs(&inputs, 0, |outpoint| (*outpoint != missing)
                .then(|| utxo(10))),
            ParentInputs::Missing(missing)
        );
    }

    /// Repeated outpoints are read once, and inputs after a missing one are not read.
    #[test]
    fn repeated_and_later_inputs_are_not_read() {
        let lookups = AtomicUsize::new(0);
        let repeated = vec![outpoints(1)[0]; MIN_WIDE_PARENT_INPUT_READS];
        read_parent_inputs(&repeated, usize::MAX, |_| {
            lookups.fetch_add(1, Ordering::Relaxed);
            Some(utxo(1))
        });
        assert_eq!(lookups.load(Ordering::Relaxed), 1);

        let lookups = AtomicUsize::new(0);
        let inputs = outpoints(MIN_PARALLEL_PARENT_INPUT_READS - 1);
        assert_eq!(
            read_parent_inputs(&inputs, usize::MAX, |_| {
                lookups.fetch_add(1, Ordering::Relaxed);
                None
            }),
            ParentInputs::Missing(inputs[0])
        );
        assert_eq!(lookups.load(Ordering::Relaxed), 1);
    }
}
