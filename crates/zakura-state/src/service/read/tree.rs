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
    block, ironwood, orchard,
    parameters::NetworkUpgrade,
    sapling,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex, TRACKED_SUBTREE_HEIGHT},
};

use crate::{
    error::{
        HistoricalSubtreeUnavailable, HistoricalSubtreeUnavailableReason, HistoricalTreeUnavailable,
    },
    service::{
        finalized_state::{embedded_last_checkpoint_leaf_counts, ZakuraDb},
        non_finalized_state::Chain,
    },
    HashOrHeight,
};

// Doc-only items
#[allow(unused_imports)]
use zakura_chain::subtree::NoteCommitmentSubtree;

/// Returns `tree` if it exists, or the consensus-defined empty frontier before `pool_activation`.
///
/// Returns an error if the tree was never written because `db` was built by the
/// verified-commitment-trees fast-sync path and `hash_or_height` falls in the absent band `[U, H)`.
fn resolve_historical_tree<Tree: Default>(
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
    pool_activation: NetworkUpgrade,
    tree: Option<Tree>,
) -> Result<Option<Tree>, HistoricalTreeUnavailable> {
    if tree.is_some() {
        return Ok(tree);
    }

    // An empty frontier is only an answer for a block this node has. Both arms resolve through an
    // index column family (`height_by_hash` and `hash_by_height`), which pruning retains — it
    // removes only the `tx_by_loc` bodies. Keep it that way: a body-backed probe such as
    // `contains_body_at_height` would report every pruned block as absent, silently turning a
    // pre-activation empty frontier back into a "missing tree" error in pruned storage mode.
    let Some(height) = (match hash_or_height {
        HashOrHeight::Hash(hash) => db.height(hash),
        HashOrHeight::Height(height) => db.contains_height(height).then_some(height),
    }) else {
        return Ok(None);
    };

    if pool_activation
        .activation_height(&db.network())
        .is_none_or(|activation| height < activation)
    {
        return Ok(Some(Default::default()));
    }

    match db.vct_synced_below() {
        Some(last_checkpoint) if db.vct_tree_absent(height) => Err(HistoricalTreeUnavailable {
            hash_or_height,
            last_checkpoint,
        }),
        _ => Ok(None),
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

/// Merges published subtree records into `stored`, keeping the node's own row wherever both carry
/// an index.
///
/// The node computed and verified its own rows; a published record is trusted only after a digest
/// the artifact carries itself, which is not a signature. A correct artifact holds only subtrees
/// completed at or below the last checkpoint and so never overlaps what the node stores, which means an
/// overlap is precisely the corrupt-or-hostile case where precedence decides whether a wrong root
/// reaches a client.
///
/// `vct_applied_below` is the durable fast-sync marker: a newer artifact still contains those
/// skipped roots, but its extra suffix must not fill heights this node synced itself. Records that
/// complete above the verified tip stay in this union so availability can see the full skip-band
/// run; [`retain_subtrees_completed_at_or_below`] drops them before serving.
pub fn merge_published_subtrees<Node>(
    stored: &mut BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    published: impl IntoIterator<Item = (NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>)>,
    vct_applied_below: block::Height,
) {
    for (index, data) in published
        .into_iter()
        .filter(|(_, data)| data.end_height <= vct_applied_below)
    {
        stored.entry(index).or_insert(data);
    }
}

/// Drops subtree records completed above `verified_tip`.
///
/// Availability checks the skip-band union, which may include published records this node has not
/// reached yet. Serving must not return a root for a height this node has not verified; those
/// records are "not yet completed at this tip", the same as asking past the chain tip.
pub fn retain_subtrees_completed_at_or_below<Node>(
    subtrees: &mut BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    verified_tip: block::Height,
) {
    subtrees.retain(|_, data| data.end_height <= verified_tip);
}

/// Returns `true` if the subtree at `start_index` completed at or below the last checkpoint,
/// given `last_checkpoint_leaves`, the pool's note commitment count at the last checkpoint height.
///
/// Subtrees complete at every multiple of `1 << TRACKED_SUBTREE_HEIGHT` leaves, so the leaf
/// count floor-divides to the number of subtrees completed by then, and index `i` is one of
/// them exactly when `i` is below that count.
pub(crate) fn subtree_completed_by_last_checkpoint(
    start_index: NoteCommitmentSubtreeIndex,
    last_checkpoint_leaves: u64,
) -> bool {
    u64::from(start_index) < (last_checkpoint_leaves >> TRACKED_SUBTREE_HEIGHT)
}

/// Returns `true` while the finalized tip has not reached the last checkpoint.
pub(crate) fn is_syncing_below_last_checkpoint(
    finalized_tip: Option<block::Height>,
    last_checkpoint: block::Height,
) -> bool {
    finalized_tip.is_some_and(|tip| tip < last_checkpoint)
}

/// Returns an error if the served run stops short of a subtree that exists on this chain but
/// this node cannot supply.
///
/// `first_missing` is the lowest index the served run does not cover: `start_index` when nothing
/// was served, otherwise one past the end of the contiguous run. `last_checkpoint_leaves` reads
/// the pool's commitment count at the last checkpoint, which bounds the indexes the fast path
/// could have skipped: anything at or above it completed after the last checkpoint and is
/// genuinely absent on any node, so a client asking past the tip still gets today's empty list.
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
    authenticated_last_checkpoint_leaves: impl FnOnce(&ZakuraDb, block::Height) -> Option<u64>,
    last_checkpoint_leaves: impl FnOnce(&ZakuraDb, block::Height) -> Option<u64>,
) -> Result<(), HistoricalSubtreeUnavailable> {
    // No gap: either the run covered everything asked for, or it ran past the last possible index.
    let Some(first_missing) = first_missing else {
        return Ok(());
    };

    let Some(last_checkpoint) = db.vct_synced_below() else {
        return Ok(());
    };

    // The embedded final frontier is authenticated before fast sync begins, so its leaf count can
    // bound skipped subtrees even while the finalized tip is still below the last checkpoint.
    if let Some(leaves) = authenticated_last_checkpoint_leaves(db, last_checkpoint) {
        return if subtree_completed_by_last_checkpoint(first_missing, leaves) {
            Err(HistoricalSubtreeUnavailable {
                pool,
                index: first_missing,
                last_checkpoint,
                reason: HistoricalSubtreeUnavailableReason::NotStored,
            })
        } else {
            Ok(())
        };
    }

    // The durable last-checkpoint marker is written by the first fast-path commit, so it exists
    // throughout an ordinary fast sync even though the last checkpoint height is still above the
    // finalized tip. If no matching authenticated frontier is available, do not run
    // `last_checkpoint_leaves`: its backward search has no tip guard and can return a row below the
    // unreached last checkpoint.
    if is_syncing_below_last_checkpoint(db.finalized_tip_height(), last_checkpoint) {
        tracing::debug!(
            pool,
            ?last_checkpoint,
            "last checkpoint has not been reached; refusing to serve incomplete subtrees",
        );

        return Err(HistoricalSubtreeUnavailable {
            pool,
            index: first_missing,
            last_checkpoint,
            reason: HistoricalSubtreeUnavailableReason::Indeterminate,
        });
    }

    // Without the last checkpoint leaf count there is no way to tell a skipped subtree from one that
    // never existed, so fail closed. Reporting the archive-mode error for a subtree that was
    // genuinely never completed is a visible, diagnosable wrong answer; serving a truncated list
    // is the silent one this whole path exists to remove. Once the tip reaches the last checkpoint,
    // its tree is outside the absent band and must be present.
    let Some(leaves) = last_checkpoint_leaves(db, last_checkpoint) else {
        tracing::error!(
            pool,
            ?last_checkpoint,
            "no note commitment tree at the last checkpoint, so completed subtrees cannot be \
             distinguished from absent ones; refusing to serve",
        );
        metrics::counter!("state.historical_tree.last_checkpoint_tree_missing").increment(1);

        return Err(HistoricalSubtreeUnavailable {
            pool,
            index: first_missing,
            last_checkpoint,
            reason: HistoricalSubtreeUnavailableReason::NotStored,
        });
    };

    if subtree_completed_by_last_checkpoint(first_missing, leaves) {
        Err(HistoricalSubtreeUnavailable {
            pool,
            index: first_missing,
            last_checkpoint,
            reason: HistoricalSubtreeUnavailableReason::NotStored,
        })
    } else {
        Ok(())
    }
}

/// Returns the lowest index in `[start_index, end_index)` that `subtrees` does not serve, or
/// `None` when the run covers the whole request.
///
/// Scans the returned keys in order so an internal gap is detected even when later subtrees are
/// present.
pub(crate) fn first_missing_subtree_index<Node>(
    subtrees: &BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    start_index: NoteCommitmentSubtreeIndex,
    end_index: Option<NoteCommitmentSubtreeIndex>,
) -> Option<NoteCommitmentSubtreeIndex> {
    let mut first_missing = start_index;

    for index in subtrees.keys() {
        if *index != first_missing {
            break;
        }

        // `u16::MAX` is the last index that can exist, so a run reaching it has no successor to
        // be missing.
        first_missing = NoteCommitmentSubtreeIndex(first_missing.0.checked_add(1)?);
    }

    // An index the client did not ask for is not missing from its answer.
    match end_index {
        Some(end) if first_missing >= end => None,
        _ => Some(first_missing),
    }
}

/// Returns an error if the Sapling subtree list `subtrees` has a gap in the
/// verified-commitment-trees absent band.
///
/// See [`check_historical_subtree_available`].
pub(crate) fn check_historical_sapling_subtrees_available(
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
        |db, last_checkpoint| {
            embedded_last_checkpoint_leaf_counts(&db.network(), last_checkpoint)
                .map(|(sapling, _, _)| sapling)
        },
        |db, last_checkpoint| {
            // Sparse-tree dedup only omits the last checkpoint row when an older frontier is identical.
            db.latest_stored_sapling_tree(&last_checkpoint)
                .map(|tree| tree.count())
        },
    )
}

/// Returns an error if the Orchard subtree list `subtrees` has a gap in the
/// verified-commitment-trees absent band.
///
/// See [`check_historical_subtree_available`].
pub(crate) fn check_historical_orchard_subtrees_available(
    db: &ZakuraDb,
    start_index: NoteCommitmentSubtreeIndex,
    end_index: Option<NoteCommitmentSubtreeIndex>,
    subtrees: &BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<orchard::tree::Node>>,
) -> Result<(), HistoricalSubtreeUnavailable> {
    check_historical_subtree_available(
        db,
        "orchard",
        first_missing_subtree_index(subtrees, start_index, end_index),
        |db, last_checkpoint| {
            embedded_last_checkpoint_leaf_counts(&db.network(), last_checkpoint)
                .map(|(_, orchard, _)| orchard)
        },
        |db, last_checkpoint| {
            // Sparse-tree dedup only omits the last checkpoint row when an older frontier is identical.
            db.latest_stored_orchard_tree(&last_checkpoint)
                .map(|tree| tree.count())
        },
    )
}

/// Returns an error if the Ironwood subtree list `subtrees` has a gap in the
/// verified-commitment-trees absent band.
///
/// See [`check_historical_subtree_available`].
pub(crate) fn check_historical_ironwood_subtrees_available(
    db: &ZakuraDb,
    start_index: NoteCommitmentSubtreeIndex,
    end_index: Option<NoteCommitmentSubtreeIndex>,
    subtrees: &BTreeMap<
        NoteCommitmentSubtreeIndex,
        NoteCommitmentSubtreeData<ironwood::tree::Node>,
    >,
) -> Result<(), HistoricalSubtreeUnavailable> {
    check_historical_subtree_available(
        db,
        "ironwood",
        first_missing_subtree_index(subtrees, start_index, end_index),
        |db, last_checkpoint| {
            embedded_last_checkpoint_leaf_counts(&db.network(), last_checkpoint)
                .map(|(_, _, ironwood)| ironwood)
        },
        |db, last_checkpoint| {
            // Sparse-tree dedup only omits the last checkpoint row when an older frontier is identical.
            db.latest_stored_ironwood_tree(&last_checkpoint)
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
) -> Result<Option<Arc<sapling::tree::NoteCommitmentTree>>, HistoricalTreeUnavailable>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since sapling treestates are the same in the finalized and non-finalized
    // state, we check the most efficient alternative first. (`chain` is always
    // in memory, but `db` stores blocks on disk, with a memory cache.)
    let tree = chain
        .and_then(|chain| chain.as_ref().sapling_tree(hash_or_height))
        .or_else(|| db.sapling_tree_by_hash_or_height(hash_or_height));

    resolve_historical_tree(db, hash_or_height, NetworkUpgrade::Sapling, tree)
}

/// Returns a list of Sapling [`NoteCommitmentSubtree`]s with indexes in the provided range.
///
/// If there is no subtree at the first index in the range, the returned list is empty.
/// Otherwise, subtrees are continuous up to the finalized tip.
///
/// See [`subtrees_with_gaps`] for more details.
pub fn sapling_subtrees<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
) -> Result<
    BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<sapling_crypto::Node>>,
    HistoricalSubtreeUnavailable,
>
where
    C: AsRef<Chain>,
{
    let (start_index, end_index) = subtree_range_bounds(&range);
    let subtrees = sapling_subtrees_with_gaps(chain, db, range);
    let subtrees = subtrees_from_start(subtrees, start_index);

    if let Some(start_index) = start_index {
        check_historical_sapling_subtrees_available(db, start_index, end_index, &subtrees)?;
    }

    Ok(subtrees)
}

/// Returns the raw union of Sapling subtree rows from `chain` and `db`, including rows after gaps.
pub(crate) fn sapling_subtrees_with_gaps<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<sapling_crypto::Node>>
where
    C: AsRef<Chain>,
{
    subtrees_with_gaps(
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
) -> Result<Option<Arc<orchard::tree::NoteCommitmentTree>>, HistoricalTreeUnavailable>
where
    C: AsRef<Chain>,
{
    // # Correctness
    //
    // Since orchard treestates are the same in the finalized and non-finalized
    // state, we check the most efficient alternative first. (`chain` is always
    // in memory, but `db` stores blocks on disk, with a memory cache.)
    let tree = chain
        .and_then(|chain| chain.as_ref().orchard_tree(hash_or_height))
        .or_else(|| db.orchard_tree_by_hash_or_height(hash_or_height));

    resolve_historical_tree(db, hash_or_height, NetworkUpgrade::Nu5, tree)
}

/// Returns a list of Orchard [`NoteCommitmentSubtree`]s with indexes in the provided range.
///
/// If there is no subtree at the first index in the range, the returned list is empty.
/// Otherwise, subtrees are continuous up to the finalized tip.
///
/// See [`subtrees_with_gaps`] for more details.
pub fn orchard_subtrees<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
) -> Result<
    BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<orchard::tree::Node>>,
    HistoricalSubtreeUnavailable,
>
where
    C: AsRef<Chain>,
{
    let (start_index, end_index) = subtree_range_bounds(&range);
    let subtrees = orchard_subtrees_with_gaps(chain, db, range);
    let subtrees = subtrees_from_start(subtrees, start_index);

    if let Some(start_index) = start_index {
        check_historical_orchard_subtrees_available(db, start_index, end_index, &subtrees)?;
    }

    Ok(subtrees)
}

/// Returns the raw union of Orchard subtree rows from `chain` and `db`, including rows after gaps.
pub(crate) fn orchard_subtrees_with_gaps<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<orchard::tree::Node>>
where
    C: AsRef<Chain>,
{
    subtrees_with_gaps(
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
) -> Result<Option<Arc<ironwood::tree::NoteCommitmentTree>>, HistoricalTreeUnavailable>
where
    C: AsRef<Chain>,
{
    let tree = chain
        .and_then(|chain| chain.as_ref().ironwood_tree(hash_or_height))
        .or_else(|| db.ironwood_tree_by_hash_or_height(hash_or_height));

    resolve_historical_tree(db, hash_or_height, NetworkUpgrade::Nu6_3, tree)
}

/// Returns a list of Ironwood [`NoteCommitmentSubtree`]s with indexes in the
/// provided range.
///
/// If there is no subtree at the first index in the range, the returned list is
/// empty. Otherwise, subtrees are continuous up to the finalized tip.
///
/// See [`subtrees_with_gaps`] for more details.
pub fn ironwood_subtrees<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
) -> Result<
    BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<ironwood::tree::Node>>,
    HistoricalSubtreeUnavailable,
>
where
    C: AsRef<Chain>,
{
    let (start_index, end_index) = subtree_range_bounds(&range);
    let subtrees = ironwood_subtrees_with_gaps(chain, db, range);
    let subtrees = subtrees_from_start(subtrees, start_index);

    if let Some(start_index) = start_index {
        check_historical_ironwood_subtrees_available(db, start_index, end_index, &subtrees)?;
    }

    Ok(subtrees)
}

/// Returns the raw union of Ironwood subtree rows from `chain` and `db`, including rows after gaps.
pub(crate) fn ironwood_subtrees_with_gaps<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<ironwood::tree::Node>>
where
    C: AsRef<Chain>,
{
    subtrees_with_gaps(
        chain,
        range,
        |chain, range| chain.ironwood_subtrees_in_range(range),
        |range| db.ironwood_subtree_list_by_index_range(range),
    )
}

/// Returns the first requested subtree index and the exclusive end bound.
fn subtree_range_bounds(
    range: &impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex>,
) -> (
    Option<NoteCommitmentSubtreeIndex>,
    Option<NoteCommitmentSubtreeIndex>,
) {
    use std::ops::Bound::*;

    let start = match range.start_bound().cloned() {
        Included(start) => Some(start),
        Excluded(start) => start.0.checked_add(1).map(Into::into),
        Unbounded => Some(0.into()),
    };
    let end = match range.end_bound().cloned() {
        Included(end) => end.0.checked_add(1).map(Into::into),
        Excluded(end) => Some(end),
        Unbounded => None,
    };

    (start, end)
}

/// Drops all subtree rows unless the requested start row is present.
fn subtrees_from_start<Node>(
    subtrees: BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    start_index: Option<NoteCommitmentSubtreeIndex>,
) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>> {
    if start_index.is_some_and(|start_index| subtrees.contains_key(&start_index)) {
        subtrees
    } else {
        BTreeMap::new()
    }
}

/// Returns a consistent chain-plus-database view without dropping rows after a missing start.
///
/// APIs that return single subtrees can't be used here, because they can create an inconsistent
/// list after concurrent non-finalized and finalized updates.
fn subtrees_with_gaps<C, Range, Node, ChainSubtreeFn, DbSubtreeFn>(
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

    match chain.map(|chain| read_chain(chain.as_ref(), range.clone())) {
        Some(chain_results) if chain_results.contains_key(&start_index) => chain_results,
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
