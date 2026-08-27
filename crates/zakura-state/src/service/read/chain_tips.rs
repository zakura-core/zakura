//! Enumerating the tips of every chain the node currently tracks.
//!
//! This backs the `getchaintips` RPC method.
//!
//! # Cost
//!
//! zcashd answers `getchaintips` by scanning its entire block index twice under
//! `cs_main`, which costs seconds once the index holds millions of entries. Zakura
//! keeps only the non-finalized chains in memory, and that set is bounded by
//! [`MAX_NON_FINALIZED_CHAIN_FORKS`][1] and [`MAX_INVALIDATED_BLOCKS`][2]. This
//! function walks that set, adds a few point lookups in the finalized state, and
//! walks the selected header chain from the block tip down to its fork with the
//! best chain. The caller bounds that last walk to the headers at or below the block
//! tip, so headers-first sync does not widen it. The cost therefore follows the
//! number of tracked forks and the depth of a reorg, not the height of the chain,
//! and it never scans a block index.
//!
//! # Coverage
//!
//! zcashd remembers every stale tip it has ever seen, because its block index is
//! never pruned. Zakura drops a fork once it is below the finalized tip, so this
//! function reports the tips that are still live: the best chain, the non-finalized
//! forks, recently invalidated branches, and the selected header chain when some
//! block bodies are unavailable.
//!
//! [1]: crate::constants::MAX_NON_FINALIZED_CHAIN_FORKS
//! [2]: crate::constants::MAX_INVALIDATED_BLOCKS

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use zakura_chain::block::{self, Height};
use zakura_header_chain::Frontier;

use crate::{
    service::{
        finalized_state::ZakuraDb,
        non_finalized_state::{Chain, NonFinalizedState},
        read::find::tip,
    },
    ContextuallyVerifiedBlock,
};

/// The status of a chain tip.
///
/// These are the `status` values zcashd's `getchaintips` can return, restricted to
/// the ones Zakura can actually distinguish. zcashd's `valid-headers` and `unknown`
/// have no Zakura equivalent: every block in the non-finalized state is contextually
/// verified, so a tip is either fully valid, invalidated, or header-only.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum ChainTipStatus {
    /// The tip of the current best chain.
    Active,

    /// A fully validated tip that is not part of the best chain.
    ValidFork,

    /// The node selected this header tip, but not all block bodies for its branch
    /// are available.
    HeadersOnly,

    /// This branch was invalidated by `invalidateblock`.
    Invalid,
}

/// The part of the selected header chain that [`chain_tips`] needs.
#[derive(Copy, Clone, Debug)]
pub struct SelectedHeaders<'a> {
    /// The tip of the selected header chain.
    pub tip: Frontier,

    /// The selected headers at or below the best chain tip, in ascending height
    /// order.
    ///
    /// The selected header chain and the best chain agree at and below the finalized
    /// tip, so their fork is always in this range. Bounding the range keeps the fork
    /// search off the headers-first sync gap, which holds every header the node has
    /// validated but not yet filled in.
    pub overlap: &'a [Frontier],
}

/// The tip of one chain that this node tracks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainTipInfo {
    /// The height of this tip.
    pub height: Height,

    /// The block hash of this tip.
    pub hash: block::Hash,

    /// The number of blocks between this tip and the block it shares with the best
    /// chain. Zero for the best chain's own tip.
    pub branch_len: u32,

    /// The status of the chain ending at this tip.
    pub status: ChainTipStatus,
}

/// Returns the tip of every chain this node currently tracks, in descending height
/// order.
///
/// `selected_headers` is the selected header tip and its overlap with the best
/// chain, when headers-first sync is running. Pass `None` to omit header-only tips.
///
/// The best chain tip is always reported, and is always the only [`Active`] entry.
/// Returns an empty list if the node has no blocks at all.
///
/// [`Active`]: ChainTipStatus::Active
pub fn chain_tips(
    non_finalized_state: &NonFinalizedState,
    db: &ZakuraDb,
    selected_headers: Option<SelectedHeaders<'_>>,
) -> Vec<ChainTipInfo> {
    let best_chain = non_finalized_state.best_chain();

    // # Correctness
    //
    // The finalized tip can advance while we read the non-finalized chains, because
    // the block write task commits to the finalized state before it updates
    // `chain_set`. That only makes an already-reported fork slightly stale, which is
    // the same guarantee every other chain read in this module gives.
    let Some((best_height, best_hash)) = tip(best_chain, db) else {
        // No blocks in either state, so there are no tips to report.
        return Vec::new();
    };

    let mut tips = vec![ChainTipInfo {
        height: best_height,
        hash: best_hash,
        branch_len: 0,
        status: ChainTipStatus::Active,
    }];

    // Track the hashes we have already reported, so a tip is never listed twice.
    let mut seen: HashSet<block::Hash> = HashSet::from([best_hash]);

    // Index every block in every invalidated branch. `invalidated_parents` holds the
    // parent hash of each of those blocks: a block in that set has a known successor,
    // so it is not a tip, and zcashd would not report it. `invalidated_by_hash` walks
    // a branch back to the block it forked from, which is not always the parent of
    // its root: `invalidateblock` can be called more than once on the same branch, and
    // then one branch's parent is the tip of another.
    let invalidated_branches = non_finalized_state.invalidated_blocks();

    let mut invalidated_parents: HashSet<block::Hash> = HashSet::new();
    let mut invalidated_by_hash: HashMap<block::Hash, (Height, block::Hash)> = HashMap::new();

    for blocks in invalidated_branches.values() {
        for invalidated_block in blocks.iter() {
            let parent = invalidated_block.block.header.previous_block_hash;

            invalidated_parents.insert(parent);
            invalidated_by_hash.insert(invalidated_block.hash, (invalidated_block.height, parent));
        }
    }

    // Non-finalized forks. `chain_iter()` yields chains in descending work order, so
    // the best chain comes first and is already covered above.
    let chains: Vec<&Arc<Chain>> = non_finalized_state.chain_iter().collect();

    for (index, chain) in chains.iter().enumerate() {
        let hash = chain.non_finalized_tip_hash();
        if !seen.insert(hash) {
            continue;
        }

        // Skip a chain whose tip is an interior block of another tracked chain. That
        // block has a known successor, so it is not a tip, and zcashd would not
        // report it. Zakura keeps a shortened chain in `chain_set` after a reorg or
        // an `invalidateblock`, and its tip is often still inside a longer chain.
        let has_successor = chains.iter().enumerate().any(|(other_index, other)| {
            other_index != index
                && other.non_finalized_tip_hash() != hash
                && other.contains_block_hash(hash)
        });

        // A fork tip whose child was invalidated also has a known successor. The best
        // chain tip is exempt, because zcashd always reports the active tip, and it
        // is already in `tips` above.
        if has_successor || invalidated_parents.contains(&hash) {
            continue;
        }

        let height = chain.non_finalized_tip_height();
        let fork_height = fork_height(chain, best_chain);

        tips.push(ChainTipInfo {
            height,
            hash,
            branch_len: height.0.saturating_sub(fork_height.0),
            status: ChainTipStatus::ValidFork,
        });
    }

    // Invalidated branches.
    for blocks in invalidated_branches.values() {
        let (Some(root_block), Some(tip_block)) = (blocks.first(), blocks.last()) else {
            continue;
        };

        if !seen.insert(tip_block.hash) {
            continue;
        }

        // The tip of a branch that another branch was later split from is not a tip.
        if invalidated_parents.contains(&tip_block.hash) {
            continue;
        }

        let fork_height =
            invalidated_fork_height(root_block, &invalidated_by_hash, &chains, best_chain, db);

        tips.push(ChainTipInfo {
            height: tip_block.height,
            hash: tip_block.hash,
            branch_len: tip_block.height.0.saturating_sub(fork_height.0),
            status: ChainTipStatus::Invalid,
        });
    }

    // The selected header chain can have more work at the same or a lower height.
    // Use its ancestry and block availability instead of comparing tip heights.
    if let Some(SelectedHeaders {
        tip: header_tip,
        overlap,
    }) = selected_headers
    {
        let body_available = chains
            .iter()
            .any(|chain| chain.contains_block_hash(header_tip.hash))
            || db.height(header_tip.hash).is_some()
            || invalidated_branches
                .values()
                .any(|blocks| blocks.iter().any(|block| block.hash == header_tip.hash));

        // Look for the fork only when this tip is reported. When the body is
        // available, which is the steady state, the search below never runs.
        if !body_available {
            // `overlap` already stops at the best chain tip, so this reads the
            // database once per block between the fork and that tip, and never once
            // per header in the sync gap above it.
            let fork_height = overlap.iter().rev().find_map(|header| {
                (best_chain_hash_at_height(best_chain, db, header.height) == Some(header.hash))
                    .then_some(header.height)
            });

            // A header chain that shares no block with the best chain has no branch
            // length to report, so it is left out.
            if let Some(fork_height) = fork_height {
                if seen.insert(header_tip.hash) {
                    tips.push(ChainTipInfo {
                        height: header_tip.height,
                        hash: header_tip.hash,
                        branch_len: header_tip.height.0.saturating_sub(fork_height.0),
                        status: ChainTipStatus::HeadersOnly,
                    });
                }
            }
        }
    }

    // zcashd sorts tips by descending height. Ties are broken by hash so that
    // repeated calls on an unchanged state return an identical list.
    tips.sort_by(|a, b| {
        b.height
            .cmp(&a.height)
            .then_with(|| a.hash.0.cmp(&b.hash.0))
    });

    tips
}

/// Returns the height of the block that an invalidated branch forked from.
///
/// zcashd measures `branchlen` from the fork with the active chain, not from the
/// parent of the branch root. Those differ when `invalidateblock` was called more
/// than once on the same branch, because the root's parent is then invalidated too.
/// This walks back to the deepest invalidated ancestor, then on through the chain
/// that holds its parent, which can itself be a fork rather than the best chain.
///
/// # Limitations
///
/// The fork limit can evict the chain that holds the parent. The node then has no
/// record of the blocks between the branch and the best chain, so the fork is
/// measured from the deepest ancestor it still tracks and `branchlen` is short by
/// the length of the evicted part. zcashd never has to do this, because it never
/// drops a block from its index.
fn invalidated_fork_height(
    root_block: &ContextuallyVerifiedBlock,
    invalidated_by_hash: &HashMap<block::Hash, (Height, block::Hash)>,
    chains: &[&Arc<Chain>],
    best_chain: Option<&Arc<Chain>>,
    db: &ZakuraDb,
) -> Height {
    let mut root_height = root_block.height;
    let mut parent = root_block.block.header.previous_block_hash;

    // Each step moves to a distinct invalidated block, so the walk cannot take more
    // steps than there are invalidated blocks.
    for _ in 0..invalidated_by_hash.len() {
        let Some(&(height, next_parent)) = invalidated_by_hash.get(&parent) else {
            break;
        };

        root_height = height;
        parent = next_parent;
    }

    let parent_height = Height(root_height.0.saturating_sub(1));

    if best_chain_hash_at_height(best_chain, db, parent_height) == Some(parent) {
        return parent_height;
    }

    chains
        .iter()
        .find(|chain| chain.contains_block_hash(parent))
        .map(|chain| fork_height(chain, best_chain))
        // The parent is in no tracked chain, so the fork limit evicted it. Measure
        // from the parent, the deepest ancestor this node can still name.
        .unwrap_or(parent_height)
}

/// Returns the active block hash at `height`.
fn best_chain_hash_at_height(
    best_chain: Option<&Arc<Chain>>,
    db: &ZakuraDb,
    height: Height,
) -> Option<block::Hash> {
    best_chain
        .and_then(|chain| chain.hash_by_height(height))
        .or_else(|| db.hash(height))
}

/// Returns the height of the highest block that `chain` shares with `best_chain`.
///
/// If the two chains share no non-finalized block, they diverged at or below the
/// finalized tip, so the fork is the parent of `chain`'s non-finalized root.
fn fork_height(chain: &Arc<Chain>, best_chain: Option<&Arc<Chain>>) -> Height {
    let root_height = chain.non_finalized_root_height();
    let tip_height = chain.non_finalized_tip_height();

    if let Some(best_chain) = best_chain {
        // Non-finalized chains are bounded by the finalized tip, so this walks at
        // most the depth of the non-finalized state.
        for height in (root_height.0..=tip_height.0).rev() {
            let height = Height(height);

            if let Some(hash) = chain.hash_by_height(height) {
                if best_chain.contains_block_hash(hash) {
                    return height;
                }
            }
        }
    }

    Height(root_height.0.saturating_sub(1))
}
