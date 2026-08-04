//! Reading note commitment trees.
//!
//! In the functions in this module:
//!
//! The block write task commits blocks to the finalized state before updating
//! `chain` with a cached copy of the best non-finalized chain from
//! `NonFinalizedState.chain_set`. Then the block commit task can commit additional blocks to
//! the finalized state after we've cloned the `chain`.
//!
//! This means that some blocks can be in both:
//! - the cached [`Chain`], and
//! - the shared finalized [`ZakuraDb`] reference.

use std::{collections::BTreeMap, sync::Arc};

use zakura_chain::{
    block, ironwood, orchard, sapling,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex, TRACKED_SUBTREE_HEIGHT},
};

use crate::{
    error::{HistoricalSubtreeUnavailable, HistoricalTreeUnavailable},
    service::{finalized_state::ZakuraDb, non_finalized_state::Chain},
    HashOrHeight,
};

// Doc-only items
#[allow(unused_imports)]
use zakura_chain::subtree::NoteCommitmentSubtree;

/// Returns an error if the per-height note commitment tree for `hash_or_height` was never
/// written, because `db` was built by the verified-commitment-trees fast-sync path and
/// `hash_or_height` falls in the absent band `[U, H)`.
///
/// Read handlers call this only after a tree read came back empty, so a tree that is present
/// (below `U`, or at or above the handoff) is never rejected. Distinguishing the absent band
/// from an ordinary miss is what stops a client from reading the miss as the empty tree; see
/// [`HistoricalTreeUnavailable`].
pub fn check_historical_tree_available(
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Result<(), HistoricalTreeUnavailable> {
    match db.vct_synced_below() {
        Some(handoff) if db.vct_historical_tree_unavailable(hash_or_height) => {
            Err(HistoricalTreeUnavailable {
                hash_or_height,
                handoff,
            })
        }
        _ => Ok(()),
    }
}

/// Trims `subtrees` to the contiguous run beginning at `start_index`.
///
/// `z_getsubtreesbyindex` serves a continuous list: a client builds witnesses by walking indexes in
/// order, so anything past a gap is unusable and a missing `start_index` means there is nothing to
/// serve at all. Callers that merge two sources — the node's own rows and a published artifact —
/// use this to re-establish that contract over the union.
pub fn contiguous_subtrees_from<Node>(
    mut subtrees: BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    start_index: NoteCommitmentSubtreeIndex,
) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>> {
    if !subtrees.contains_key(&start_index) {
        return BTreeMap::new();
    }

    subtrees.retain(|index, _| *index >= start_index);

    let mut expected = start_index.0;
    let mut contiguous = BTreeMap::new();
    for (index, data) in subtrees {
        if index.0 != expected {
            break;
        }

        contiguous.insert(index, data);
        let Some(next) = expected.checked_add(1) else {
            break;
        };
        expected = next;
    }

    contiguous
}

/// Returns `true` if the subtree at `start_index` completed at or below the checkpoint handoff,
/// given `handoff_leaves`, the pool's note commitment count at the handoff height.
///
/// Subtrees complete at every multiple of `1 << TRACKED_SUBTREE_HEIGHT` leaves, so the leaf
/// count floor-divides to the number of subtrees completed by then, and index `i` is one of
/// them exactly when `i` is below that count.
pub(crate) fn subtree_completed_by_handoff(
    start_index: NoteCommitmentSubtreeIndex,
    handoff_leaves: u64,
) -> bool {
    u64::from(start_index) < (handoff_leaves >> TRACKED_SUBTREE_HEIGHT)
}

/// Returns an error if the served run stops short of a subtree that exists on this chain but
/// this node cannot supply.
///
/// `first_missing` is the lowest index the served run does not cover: `start_index` when nothing
/// was served, otherwise one past the end of the contiguous run. `handoff_leaves` reads the pool's
/// commitment count at the handoff, which bounds the indexes the fast path could have skipped:
/// anything at or above it completed after the handoff and is genuinely absent on any node, so a
/// client asking past the tip still gets today's empty list.
///
/// # Correctness
///
/// Checking only that `start_index` was served is not enough. `z_getsubtreesbyindex` returns one
/// contiguous run, so a gap anywhere in it truncates the response — and a single unbounded request
/// from index 0 would then return a short list with no error, which a client reads as "that is
/// every subtree on this chain". That is the same silent-truncation failure the typed errors exist
/// to remove, just arriving through a truncated artifact instead of an absent one.
fn check_historical_subtree_available(
    db: &ZakuraDb,
    pool: &'static str,
    first_missing: Option<NoteCommitmentSubtreeIndex>,
    handoff_leaves: impl FnOnce(&ZakuraDb, block::Height) -> Option<u64>,
) -> Result<(), HistoricalSubtreeUnavailable> {
    // No gap: either the run covered everything asked for, or it ran past the last possible index.
    let Some(first_missing) = first_missing else {
        return Ok(());
    };

    let Some(handoff) = db.vct_synced_below() else {
        return Ok(());
    };

    // Without the handoff leaf count there is no way to tell a skipped subtree from one that
    // never existed, so fail closed. Reporting the archive-mode error for a subtree that was
    // genuinely never completed is a visible, diagnosable wrong answer; serving a truncated list
    // is the silent one this whole path exists to remove. The tree at the handoff is outside the
    // absent band and at or below the tip, so this is not expected to happen — but "not expected"
    // is the wrong thing to lean on when the failure is silent.
    let Some(leaves) = handoff_leaves(db, handoff) else {
        tracing::error!(
            pool,
            ?handoff,
            "no note commitment tree at the checkpoint handoff, so completed subtrees cannot be \
             distinguished from absent ones; refusing to serve",
        );
        metrics::counter!("state.historical_tree.handoff_tree_missing").increment(1);

        return Err(HistoricalSubtreeUnavailable {
            pool,
            index: first_missing,
            handoff,
        });
    };

    if subtree_completed_by_handoff(first_missing, leaves) {
        Err(HistoricalSubtreeUnavailable {
            pool,
            index: first_missing,
            handoff,
        })
    } else {
        Ok(())
    }
}

/// Merges published subtree records into `stored`, keeping the node's own row wherever both carry
/// an index.
///
/// The node computed and verified its own rows; a published record is trusted only after a digest
/// the artifact carries itself, which is not a signature. A correct artifact holds only subtrees
/// completed at or below the handoff and so never overlaps what the node stores, which means an
/// overlap is precisely the corrupt-or-hostile case where precedence decides whether a wrong root
/// reaches a client.
pub fn merge_published_subtrees<Node>(
    stored: &mut BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    published: impl IntoIterator<Item = (NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>)>,
) {
    for (index, data) in published {
        stored.entry(index).or_insert(data);
    }
}

/// Returns the lowest index in `[start_index, end_index)` that `subtrees` does not serve, or
/// `None` when the run covers the whole request.
///
/// `subtrees` is contiguous from `start_index` by the time this runs, so the first index it fails
/// to cover is one past its last key.
pub(crate) fn first_missing_subtree_index<Node>(
    subtrees: &BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    start_index: NoteCommitmentSubtreeIndex,
    end_index: Option<NoteCommitmentSubtreeIndex>,
) -> Option<NoteCommitmentSubtreeIndex> {
    let first_missing = match subtrees.keys().next_back() {
        // `u16::MAX` is the last index that can exist, so a run reaching it has no successor to
        // be missing.
        Some(last) => NoteCommitmentSubtreeIndex(last.0.checked_add(1)?),
        None => start_index,
    };

    // An index the client did not ask for is not missing from its answer.
    match end_index {
        Some(end) if first_missing >= end => None,
        _ => Some(first_missing),
    }
}

/// Returns an error if the Sapling subtree list `subtrees` is missing `start_index` because it
/// completed inside the verified-commitment-trees absent band.
///
/// See [`check_historical_subtree_available`].
pub fn check_historical_sapling_subtrees_available(
    db: &ZakuraDb,
    start_index: NoteCommitmentSubtreeIndex,
    end_index: Option<NoteCommitmentSubtreeIndex>,
    subtrees: &BTreeMap<
        NoteCommitmentSubtreeIndex,
        NoteCommitmentSubtreeData<sapling_crypto::Node>,
    >,
) -> Result<(), HistoricalSubtreeUnavailable> {
    check_historical_subtree_available(
        db,
        "sapling",
        first_missing_subtree_index(subtrees, start_index, end_index),
        |db, handoff| db.sapling_tree_by_height(&handoff).map(|tree| tree.count()),
    )
}

/// Returns an error if the Orchard subtree list `subtrees` is missing `start_index` because it
/// completed inside the verified-commitment-trees absent band.
///
/// See [`check_historical_subtree_available`].
pub fn check_historical_orchard_subtrees_available(
    db: &ZakuraDb,
    start_index: NoteCommitmentSubtreeIndex,
    end_index: Option<NoteCommitmentSubtreeIndex>,
    subtrees: &BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<orchard::tree::Node>>,
) -> Result<(), HistoricalSubtreeUnavailable> {
    check_historical_subtree_available(
        db,
        "orchard",
        first_missing_subtree_index(subtrees, start_index, end_index),
        |db, handoff| db.orchard_tree_by_height(&handoff).map(|tree| tree.count()),
    )
}

/// Returns an error if the Ironwood subtree list `subtrees` is missing `start_index` because it
/// completed inside the verified-commitment-trees absent band.
///
/// See [`check_historical_subtree_available`].
pub fn check_historical_ironwood_subtrees_available(
    db: &ZakuraDb,
    start_index: NoteCommitmentSubtreeIndex,
    end_index: Option<NoteCommitmentSubtreeIndex>,
    subtrees: &BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<orchard::tree::Node>>,
) -> Result<(), HistoricalSubtreeUnavailable> {
    check_historical_subtree_available(
        db,
        "ironwood",
        first_missing_subtree_index(subtrees, start_index, end_index),
        |db, handoff| {
            db.ironwood_tree_by_height(&handoff)
                .map(|tree| tree.count())
        },
    )
}

/// Returns the Sapling
/// [`NoteCommitmentTree`](sapling::tree::NoteCommitmentTree) specified by a
/// hash or height, if it exists in the non-finalized `chain` or finalized `db`.
pub fn sapling_tree<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<Arc<sapling::tree::NoteCommitmentTree>>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since sapling treestates are the same in the finalized and non-finalized
    // state, we check the most efficient alternative first. (`chain` is always
    // in memory, but `db` stores blocks on disk, with a memory cache.)
    chain
        .and_then(|chain| chain.as_ref().sapling_tree(hash_or_height))
        .or_else(|| db.sapling_tree_by_hash_or_height(hash_or_height))
}

/// Returns a list of Sapling [`NoteCommitmentSubtree`]s with indexes in the provided range.
///
/// If there is no subtree at the first index in the range, the returned list is empty.
/// Otherwise, subtrees are continuous up to the finalized tip.
///
/// See [`subtrees`] for more details.
pub fn sapling_subtrees<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<sapling_crypto::Node>>
where
    C: AsRef<Chain>,
{
    subtrees(
        chain,
        range,
        |chain, range| chain.sapling_subtrees_in_range(range),
        |range| db.sapling_subtree_list_by_index_range(range),
    )
}

/// Returns the Orchard
/// [`NoteCommitmentTree`](orchard::tree::NoteCommitmentTree) specified by a
/// hash or height, if it exists in the non-finalized `chain` or finalized `db`.
pub fn orchard_tree<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<Arc<orchard::tree::NoteCommitmentTree>>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since orchard treestates are the same in the finalized and non-finalized
    // state, we check the most efficient alternative first. (`chain` is always
    // in memory, but `db` stores blocks on disk, with a memory cache.)
    chain
        .and_then(|chain| chain.as_ref().orchard_tree(hash_or_height))
        .or_else(|| db.orchard_tree_by_hash_or_height(hash_or_height))
}

/// Returns a list of Orchard [`NoteCommitmentSubtree`]s with indexes in the provided range.
///
/// If there is no subtree at the first index in the range, the returned list is empty.
/// Otherwise, subtrees are continuous up to the finalized tip.
///
/// See [`subtrees`] for more details.
pub fn orchard_subtrees<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<orchard::tree::Node>>
where
    C: AsRef<Chain>,
{
    subtrees(
        chain,
        range,
        |chain, range| chain.orchard_subtrees_in_range(range),
        |range| db.orchard_subtree_list_by_index_range(range),
    )
}

/// Returns the Ironwood
/// [`NoteCommitmentTree`](ironwood::tree::NoteCommitmentTree) specified by a
/// hash or height, if it exists in the non-finalized `chain` or finalized `db`.
pub fn ironwood_tree<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<Arc<ironwood::tree::NoteCommitmentTree>>
where
    C: AsRef<Chain>,
{
    chain
        .and_then(|chain| chain.as_ref().ironwood_tree(hash_or_height))
        .or_else(|| db.ironwood_tree_by_hash_or_height(hash_or_height))
}

/// Returns a list of Ironwood [`NoteCommitmentSubtree`]s with indexes in the
/// provided range.
///
/// If there is no subtree at the first index in the range, the returned list is
/// empty. Otherwise, subtrees are continuous up to the finalized tip.
///
/// See [`subtrees`] for more details.
pub fn ironwood_subtrees<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<ironwood::tree::Node>>
where
    C: AsRef<Chain>,
{
    subtrees(
        chain,
        range,
        |chain, range| chain.ironwood_subtrees_in_range(range),
        |range| db.ironwood_subtree_list_by_index_range(range),
    )
}

/// Returns a list of [`NoteCommitmentSubtree`]s in the provided range.
///
/// If there is no subtree at the first index in the range, the returned list is empty.
/// Otherwise, subtrees are continuous up to the finalized tip.
///
/// Accepts a `chain` from the non-finalized state, a `range` of subtree indexes to retrieve,
/// a `read_chain` function for retrieving the `range` of subtrees from `chain`, and
/// a `read_disk` function for retrieving the `range` from [`ZakuraDb`].
///
/// Returns a consistent set of subtrees for the supplied chain fork and database.
/// Avoids reading the database if the subtrees are present in memory.
///
/// # Correctness
///
/// APIs that return single subtrees can't be used for `read_chain` and `read_disk`, because they
/// can create an inconsistent list of subtrees after concurrent non-finalized and finalized updates.
fn subtrees<C, Range, Node, ChainSubtreeFn, DbSubtreeFn>(
    chain: Option<C>,
    range: Range,
    read_chain: ChainSubtreeFn,
    read_disk: DbSubtreeFn,
) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>
where
    C: AsRef<Chain>,
    Node: PartialEq,
    Range: std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
    ChainSubtreeFn: FnOnce(
        &Chain,
        Range,
    )
        -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    DbSubtreeFn:
        FnOnce(Range) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
{
    use std::ops::Bound::*;

    let Some(start_index) = (match range.start_bound().cloned() {
        Included(start_index) => Some(start_index),
        Excluded(start_index) => start_index.0.checked_add(1).map(Into::into),
        Unbounded => Some(0.into()),
    }) else {
        return BTreeMap::new();
    };

    // # Correctness
    //
    // After `chain` was cloned, the StateService can commit additional blocks to the finalized state `db`.
    // Usually, the subtrees of these blocks are consistent. But if the `chain` is a different fork to `db`,
    // then the trees can be inconsistent. In that case, if `chain` does not contain a subtree at the first
    // index in the provided range, we ignore all the trees in `chain` after the first inconsistent tree,
    // because we know they will be inconsistent as well. (It is cryptographically impossible for tree roots
    // to be equal once the leaves have diverged.)

    let results = match chain.map(|chain| read_chain(chain.as_ref(), range.clone())) {
        Some(chain_results) if chain_results.contains_key(&start_index) => return chain_results,
        Some(chain_results) => {
            let mut db_results = read_disk(range);

            // Check for inconsistent trees in the fork.
            for (chain_index, chain_subtree) in chain_results {
                // If there's no matching index, just update the list of trees.
                let Some(db_subtree) = db_results.get(&chain_index) else {
                    db_results.insert(chain_index, chain_subtree);
                    continue;
                };

                // We have an outdated chain fork, so skip this subtree and all remaining subtrees.
                if &chain_subtree != db_subtree {
                    break;
                }
                // Otherwise, the subtree is already in the list, so we don't need to add it.
            }

            db_results
        }
        None => read_disk(range),
    };

    // Check that we got the start subtree
    if results.contains_key(&start_index) {
        results
    } else {
        BTreeMap::new()
    }
}

/// Get the history tree of the provided chain.
pub fn history_tree<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Option<Arc<zakura_chain::history_tree::HistoryTree>>
where
    C: AsRef<Chain>,
{
    chain
        .and_then(|chain| chain.as_ref().history_tree(hash_or_height))
        .or_else(|| {
            let (tip_height, tip_hash) = db.tip()?;
            match hash_or_height {
                HashOrHeight::Height(height) if height == tip_height => Some(db.history_tree()),
                HashOrHeight::Hash(hash) if hash == tip_hash => Some(db.history_tree()),
                _ => None,
            }
        })
}
