//! Randomised property tests for the Zebra chain tip.

use std::{collections::HashSet, env, sync::Arc};

use futures::FutureExt;
use proptest::prelude::*;
use proptest_derive::Arbitrary;

use zakura_chain::{
    block::{self, Block},
    chain_tip::ChainTip,
    fmt::{DisplayToDebug, SummaryDebug},
    parameters::{Network, NetworkUpgrade},
};

use crate::service::chain_tip::{
    ChainTipBlock, ChainTipSender, TipAction, MAX_RECENT_TIP_ANCESTORS,
};

use TipChangeCheck::*;

/// The default number of proptest cases for these tests.
///
/// Currently, there are 24 different test case combinations,
/// and each test `Vec` has an average of 50 blocks.
const DEFAULT_BLOCK_VEC_PROPTEST_CASES: u32 = 8;

proptest! {
    #![proptest_config(
        proptest::test_runner::Config::with_cases(env::var("PROPTEST_CASES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_BLOCK_VEC_PROPTEST_CASES))
    )]

    /// Check that the best tip uses the non-finalized tip if available,
    /// or otherwise the finalized tip.
    #[test]
    fn best_tip_is_latest_non_finalized_then_latest_finalized(
        tip_updates in any::<SummaryDebug<Vec<(BlockUpdate, BlockConnection, TipChangeCheck)>>>(),
        network in any::<Network>(),
    ) {
        let (mut chain_tip_sender, latest_chain_tip, mut chain_tip_change) = ChainTipSender::new(None, &network);

        let mut latest_finalized_tip = None;
        let mut latest_non_finalized_tip = None;
        let mut seen_non_finalized_tip = false;

        let mut published_run: Vec<ChainTipBlock> = Vec::new();
        let mut last_block_hash = None;
        let mut chain_hashes = HashSet::new();

        for (mut update, connection, tip_change_check) in tip_updates {
            // prepare the update
            if connection.is_grow() {
                if let (Some(mut block), Some(last_block_hash)) = (update.block(), last_block_hash) {
                    let block_mut = Arc::make_mut(&mut block);
                    Arc::make_mut(&mut block_mut.header).previous_block_hash = last_block_hash;

                    *update.block_mut() = Some(block);
                }
            }

            let block = update.block();
            let chain_tip = block.clone().map(|block| ChainTipBlock::from(block.0));

            if let Some(chain_tip) = chain_tip.clone() {
                if chain_hashes.contains(&chain_tip.hash) {
                    // skip duplicate blocks - they are rejected by zakura-state
                    continue;
                }
                last_block_hash = Some(chain_tip.hash);
                chain_hashes.insert(chain_tip.hash);
            }

            // do the update
            if update.is_finalized() {
                chain_tip_sender.set_finalized_tip(chain_tip.clone());
                if let Some(block) = block {
                    latest_finalized_tip = Some((chain_tip.unwrap(), block));
                }
            } else {
                chain_tip_sender.set_best_non_finalized_tip(chain_tip.clone());
                if let Some(block) = block {
                    latest_non_finalized_tip = Some((chain_tip.unwrap(), block));
                    seen_non_finalized_tip = true;
                }
            }

            // check the results
            let expected_tip = if seen_non_finalized_tip {
                latest_non_finalized_tip.clone()
            } else {
                latest_finalized_tip.clone()
            };

            let chain_tip_height = expected_tip
                .as_ref()
                .map(|(chain_tip, _block)| chain_tip.height);
            let expected_height = expected_tip.as_ref().and_then(|(_chain_tip, block)| block.coinbase_height());
            prop_assert_eq!(latest_chain_tip.best_tip_height(), chain_tip_height);
            prop_assert_eq!(latest_chain_tip.best_tip_height(), expected_height);

            let chain_tip_hash = expected_tip
                .as_ref()
                .map(|(chain_tip, _block)| chain_tip.hash);
            let expected_hash = expected_tip.as_ref().map(|(_chain_tip, block)| block.hash());
            prop_assert_eq!(latest_chain_tip.best_tip_hash(), chain_tip_hash);
            prop_assert_eq!(latest_chain_tip.best_tip_hash(), expected_hash);

            let chain_tip_transaction_ids = expected_tip
                .as_ref()
                .map(|(chain_tip, _block)| chain_tip.transaction_hashes.clone())
                .unwrap_or_else(|| Arc::new([]));
            let expected_transaction_ids = expected_tip
                .as_ref()
                .iter()
                .flat_map(|(_chain_tip, block)| block.transactions.clone())
                .map(|transaction| transaction.hash())
                .collect();
            prop_assert_eq!(
                latest_chain_tip.best_tip_mined_transaction_ids(),
                chain_tip_transaction_ids
            );
            prop_assert_eq!(
                latest_chain_tip.best_tip_mined_transaction_ids(),
                expected_transaction_ids
            );

            // Track the trailing run of published tips that are each a child of the previous one,
            // which is the history the sender keeps for skipped blocks.
            if let Some((chain_tip, _block)) = expected_tip.as_ref() {
                if published_run.last().map(|tip: &ChainTipBlock| tip.hash) != Some(chain_tip.hash) {
                    if published_run.last().map(|tip| tip.hash) != Some(chain_tip.previous_block_hash) {
                        published_run.clear();
                    }
                    published_run.push(chain_tip.clone());
                }
            }

            let old_last_change_hash = chain_tip_change.last_change_hash;
            let expected_action = expected_tip_action(&network, old_last_change_hash, &published_run);

            match tip_change_check {
                WaitFor => {
                    // TODO: use `unconstrained` to avoid spurious cooperative multitasking waits
                    //       (needs a recent tokio version)
                    // See:
                    // https://github.com/ZcashFoundation/zebra/pull/2777#discussion_r712488817
                    // https://docs.rs/tokio/1.11.0/tokio/task/index.html#cooperative-scheduling
                    // https://tokio.rs/blog/2020-04-preemption
                    prop_assert_eq!(
                        chain_tip_change
                            .wait_for_tip_change()
                            .now_or_never()
                            .transpose()
                            .expect("watch sender is not dropped"),
                        expected_action,
                        "\n\
                         unexpected wait_for_tip_change TipAction\n\
                         old last_change_hash: {:?}\n\
                         new last_change_hash: {:?}",
                        old_last_change_hash,
                        chain_tip_change.last_change_hash
                    );
                }

                Last => {
                    prop_assert_eq!(
                        chain_tip_change.last_tip_change(),
                        expected_action,
                        "\n\
                         unexpected last_tip_change TipAction\n\
                         old last_change_hash: {:?}\n\
                         new last_change_hash: {:?}",
                        old_last_change_hash,
                        chain_tip_change.last_change_hash
                    );
                }

                Skip => {}
            }
        }
    }
}

/// Returns the action a [`ChainTipChange`] whose last change was `last_change_hash` should
/// report, given the trailing run of published tips that each extend the previous one.
///
/// The change grows through skipped blocks when the last change is one of the
/// `MAX_RECENT_TIP_ANCESTORS` tips before the current tip, every skipped block has transactions,
/// and no added block is just below an activation height. Otherwise it resets.
fn expected_tip_action(
    network: &Network,
    last_change_hash: Option<block::Hash>,
    published_run: &[ChainTipBlock],
) -> Option<TipAction> {
    let (new_tip, earlier) = published_run.split_last()?;

    if Some(new_tip.hash) == last_change_hash {
        // some updates don't do anything, so there's no new action
        return None;
    }

    let skipped_blocks = if Some(new_tip.previous_block_hash) == last_change_hash {
        Some(Vec::new())
    } else {
        let kept_start = earlier.len().saturating_sub(MAX_RECENT_TIP_ANCESTORS);
        earlier[kept_start..]
            .iter()
            .rposition(|tip| Some(tip.hash) == last_change_hash)
            .map(|index| earlier[kept_start + index + 1..].to_vec())
            .filter(|skipped| skipped.iter().all(|tip| !tip.transactions.is_empty()))
    };

    // Mirror `ChainTipChange::action`: the mempool verifies against the *next*
    // height, so a reset happens at `activation_height - 1`, and also at the
    // temporary-Orchard-disabling soft fork's activation height (see commit
    // 548672c5e). `next` always succeeds because the tip is far below `Height::MAX`.
    let reaches_activation = |tip: &ChainTipBlock| {
        let next_height = tip
            .height
            .next()
            .expect("chain tip height is far below Height::MAX");
        NetworkUpgrade::is_activation_height(network, next_height)
            || network.is_temporary_orchard_disabling_soft_fork_activation_height(next_height)
    };

    match skipped_blocks {
        Some(skipped)
            if !skipped
                .iter()
                .chain(std::iter::once(new_tip))
                .any(reaches_activation) =>
        {
            Some(TipAction::grow_through(skipped, new_tip.clone()))
        }
        _ => Some(TipAction::reset_with(new_tip.clone())),
    }
}

/// Block update test cases for [`ChainTipSender`]
#[derive(Arbitrary, Clone, Debug)]
enum BlockUpdate {
    Finalized(Option<DisplayToDebug<Arc<Block>>>),
    NonFinalized(Option<DisplayToDebug<Arc<Block>>>),
}

impl BlockUpdate {
    /// Returns the inner block, regardless of variant.
    pub fn block(&self) -> Option<DisplayToDebug<Arc<Block>>> {
        match self {
            BlockUpdate::Finalized(block) => block.clone(),
            BlockUpdate::NonFinalized(block) => block.clone(),
        }
    }

    /// Returns a mutable reference to the inner block, regardless of variant.
    pub fn block_mut(&mut self) -> &mut Option<DisplayToDebug<Arc<Block>>> {
        match self {
            BlockUpdate::Finalized(block) => block,
            BlockUpdate::NonFinalized(block) => block,
        }
    }

    /// Is it finalized?
    pub fn is_finalized(&self) -> bool {
        matches!(self, BlockUpdate::Finalized(_))
    }
}

/// Block update test case variants for [`ChainTipChange`]
#[derive(Arbitrary, Copy, Clone, Debug, Eq, PartialEq)]
enum BlockConnection {
    Reset,
    Grow,
}

impl BlockConnection {
    /// Is this a grow?
    pub fn is_grow(&self) -> bool {
        *self == BlockConnection::Grow
    }
}

/// Block update checks for [`ChainTipChange`]
#[derive(Arbitrary, Copy, Clone, Debug, Eq, PartialEq)]
enum TipChangeCheck {
    /// Check that `wait_for_tip_change` returns the correct result
    WaitFor,

    /// Check that `last_tip_change` returns the correct result
    Last,

    /// Don't check this case (the next check combines it with later changes)
    Skip,
}
