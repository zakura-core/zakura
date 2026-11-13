//! The best-chain changes the block writer publishes to the pool core.
//!
//! See [`zakura_node_services::mempool::port::ChainTransitionSender`] for the
//! rule. The writer begins a change before any new best-chain state becomes
//! readable and finishes it once the new tip is published. Readers read the
//! best chain from the published non-finalized state and the finalized
//! database, so those two writes are what each change brackets.

use std::sync::Arc;

use zakura_chain::{block, parameters::Network};
use zakura_node_services::mempool::port::{ChainTip, ChainTransition, ConnectedBlock, Generation};

use crate::{
    request::ContextuallyVerifiedBlock,
    service::{chain_tip::mempool_rules_change, non_finalized_state::Chain},
};

/// The tip a committed block is.
fn tip_of(block: &ContextuallyVerifiedBlock) -> ChainTip {
    ChainTip {
        hash: block.hash,
        height: block.height,
    }
}

/// The connected block for a committed block. It shares the block's
/// transaction handles.
fn connected_block(block: &ContextuallyVerifiedBlock) -> ConnectedBlock {
    ConnectedBlock {
        hash: block.hash,
        height: block.height,
        transactions: block.block.transactions().collect(),
        transaction_hashes: block.transaction_hashes.clone(),
    }
}

/// The transition from the published best chain `old_best` on top of the
/// finalized tip, whose tip was `before`, to `new_best`, whose tip is `after`.
///
/// `None` for either chain is an empty non-finalized state, whose best tip is
/// the finalized tip. The finalized tip only moves up along the old best chain
/// between two publications, so the blocks both chains share are the new
/// chain's blocks that the old chain holds, and the finalized blocks below
/// them.
///
/// Returns `None` when the chains do not match `before` and `after`, so the
/// caller cannot name the blocks that changed.
pub(super) fn non_finalized_transition(
    network: &Network,
    before: Option<ChainTip>,
    old_best: Option<&Chain>,
    new_best: Option<&Chain>,
    after: ChainTip,
    generation: Generation,
) -> Option<ChainTransition> {
    if let Some(old_best) = old_best {
        let (height, hash) = old_best.non_finalized_tip();
        if before != Some(ChainTip { hash, height }) {
            return None;
        }
    }

    // The fork point is the highest new block the old chain holds. Without
    // one, it is below the new chain's root: the finalized tip it builds on.
    let mut connected = Vec::new();
    let fork_height = match new_best {
        Some(new_best) => {
            if new_best.non_finalized_tip() != (after.height, after.hash) {
                return None;
            }
            let mut fork_height = None;
            for block in new_best.blocks.values().rev() {
                let shared =
                    old_best.is_some_and(|old_best| old_best.height_by_hash(block.hash).is_some());
                if shared {
                    fork_height = Some(block.height);
                    break;
                }
                connected.push(connected_block(block));
            }
            connected.reverse();
            match fork_height {
                Some(height) => height,
                None => new_best.non_finalized_root_height().previous().ok()?,
            }
        }
        None => after.height,
    };

    let disconnected: Vec<ChainTip> = old_best
        .map(|old_best| {
            old_best
                .blocks
                .range(block::Height(fork_height.0.saturating_add(1))..)
                .rev()
                .map(|(_, block)| tip_of(block))
                .collect()
        })
        .unwrap_or_default();

    // An old chain that was only the finalized tip cannot be above the fork,
    // because the finalized tip only moves up.
    if old_best.is_none() && before.is_some_and(|before| before.height > fork_height) {
        return None;
    }

    Some(ChainTransition {
        rules_change: mempool_rules_change(network, before.map(|tip| tip.height), after.height),
        before,
        disconnected,
        connected,
        after,
        generation,
    })
}

/// The transition for a checkpoint commit of `block` on top of `before`, or
/// `None` if `block` does not extend it.
pub(super) fn checkpoint_transition(
    network: &Network,
    before: Option<ChainTip>,
    block: ConnectedBlock,
    previous_block_hash: block::Hash,
    generation: Generation,
) -> Option<ChainTransition> {
    let extends = match before {
        Some(before) => before.hash == previous_block_hash,
        None => block.height == block::Height(0),
    };
    if !extends {
        return None;
    }
    let after = ChainTip {
        hash: block.hash,
        height: block.height,
    };
    Some(ChainTransition {
        rules_change: mempool_rules_change(network, before.map(|tip| tip.height), after.height),
        before,
        disconnected: Vec::new(),
        connected: vec![block],
        after,
        generation,
    })
}

/// The old best chain to diff against, taken only when a consumer will need
/// the transition. Taking it is an `Arc` clone.
pub(super) fn old_best_chain(
    subscribed: bool,
    published: &crate::service::non_finalized_state::NonFinalizedState,
) -> Option<Option<Arc<Chain>>> {
    subscribed.then(|| published.best_chain().cloned())
}
