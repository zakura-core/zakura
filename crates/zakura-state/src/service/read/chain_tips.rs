//! Enumerating the tips of every chain the node currently tracks.
//!
//! This backs the `getchaintips` RPC method.
//!
//! # Cost
//!
//! zcashd answers `getchaintips` by scanning its entire block index twice under
//! `cs_main`, which costs seconds once the index holds millions of entries. Zakura
//! keeps only the non-finalized chains in memory, and that set is bounded by
//! [`MAX_NON_FINALIZED_CHAIN_FORKS`][1] and [`MAX_INVALIDATED_BLOCKS`][2]. So this
//! function is bounded by the number of forks, not by the height of the chain, and
//! it never touches the finalized state's block index.
//!
//! # Coverage
//!
//! zcashd remembers every stale tip it has ever seen, because its block index is
//! never pruned. Zakura drops a fork once it is below the finalized tip, so this
//! function reports the tips that are still live: the best chain, the non-finalized
//! forks, recently invalidated branches, and the header chain when headers-first
//! sync is ahead of the block tip.
//!
//! [1]: crate::constants::MAX_NON_FINALIZED_CHAIN_FORKS
//! [2]: crate::constants::MAX_INVALIDATED_BLOCKS

use std::{collections::HashSet, sync::Arc};

use zakura_chain::block::{self, Height};

use crate::service::{
    finalized_state::ZakuraDb,
    non_finalized_state::{Chain, NonFinalizedState},
    read::find::tip,
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

    /// The header chain is ahead of the best chain, and the blocks for this tip are
    /// not available yet.
    HeadersOnly,

    /// This branch was invalidated, either by consensus or by `invalidateblock`.
    Invalid,
}

impl ChainTipStatus {
    /// Returns the zcashd-compatible name of this status.
    pub fn as_str(self) -> &'static str {
        match self {
            ChainTipStatus::Active => "active",
            ChainTipStatus::ValidFork => "valid-fork",
            ChainTipStatus::HeadersOnly => "headers-only",
            ChainTipStatus::Invalid => "invalid",
        }
    }
}

impl std::fmt::Display for ChainTipStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
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
/// `header_tip` is the tip of the header chain, when headers-first sync is running
/// and has selected one. Pass `None` to omit header-only tips; a header tip at or
/// below the best chain tip is ignored, because its blocks are already available.
///
/// The best chain tip is always reported, and is always the only [`Active`] entry.
/// Returns an empty list if the node has no blocks at all.
///
/// [`Active`]: ChainTipStatus::Active
pub fn chain_tips(
    non_finalized_state: &NonFinalizedState,
    db: &ZakuraDb,
    header_tip: Option<(Height, block::Hash)>,
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

        if has_successor {
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

    // Invalidated branches. The map is keyed by the height of the branch's root, so
    // the branch forked from its parent at `root_height - 1`.
    for (root_height, blocks) in non_finalized_state.invalidated_blocks() {
        let Some(tip_block) = blocks.last() else {
            continue;
        };

        if !seen.insert(tip_block.hash) {
            continue;
        }

        let fork_height = root_height.0.saturating_sub(1);

        tips.push(ChainTipInfo {
            height: tip_block.height,
            hash: tip_block.hash,
            branch_len: tip_block.height.0.saturating_sub(fork_height),
            status: ChainTipStatus::Invalid,
        });
    }

    // The header chain, when it is ahead of the blocks we have. Headers build on the
    // best chain, so the fork point is the best chain tip.
    if let Some((height, hash)) = header_tip {
        if height > best_height && seen.insert(hash) {
            tips.push(ChainTipInfo {
                height,
                hash,
                branch_len: height.0.saturating_sub(best_height.0),
                status: ChainTipStatus::HeadersOnly,
            });
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
