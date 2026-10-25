use std::{iter, sync::Arc};

use chrono::Utc;
use futures::FutureExt;

use zakura_chain::{
    block,
    chain_tip::{ChainTip, NoChainTip},
    parameters::Network::*,
};

use super::super::{ChainTipBlock, ChainTipSender, TipAction, MAX_RECENT_TIP_ANCESTORS};

fn tip(height: u32, hash_byte: u8, parent_byte: u8) -> ChainTipBlock {
    ChainTipBlock {
        hash: block::Hash([hash_byte; 32]),
        height: block::Height(height),
        time: Utc::now(),
        transactions: Vec::new(),
        transaction_hashes: Arc::from([]),
        previous_block_hash: block::Hash([parent_byte; 32]),
    }
}

#[test]
fn current_best_tip_is_initially_empty() {
    let (_chain_tip_sender, latest_chain_tip, _chain_tip_change) =
        ChainTipSender::new(None, &Mainnet);

    assert_eq!(latest_chain_tip.best_tip_height(), None);
    assert_eq!(latest_chain_tip.best_tip_hash(), None);
    assert_eq!(
        latest_chain_tip.best_tip_mined_transaction_ids(),
        iter::empty().collect()
    );
}

#[test]
fn empty_latest_chain_tip_is_empty() {
    let latest_chain_tip = NoChainTip;

    assert_eq!(latest_chain_tip.best_tip_height(), None);
    assert_eq!(latest_chain_tip.best_tip_hash(), None);
    assert_eq!(
        latest_chain_tip.best_tip_mined_transaction_ids(),
        iter::empty().collect()
    );
}

#[test]
fn chain_tip_change_is_initially_not_ready() {
    let (_chain_tip_sender, _latest_chain_tip, mut chain_tip_change) =
        ChainTipSender::new(None, &Mainnet);

    // TODO: use `tokio::task::unconstrained` to avoid spurious waits from tokio's cooperative multitasking
    //       (needs a recent tokio version)
    // See:
    // https://github.com/ZcashFoundation/zebra/pull/2777#discussion_r712488817
    // https://docs.rs/tokio/1.11.0/tokio/task/index.html#cooperative-scheduling
    // https://tokio.rs/blog/2020-04-preemption

    let first = chain_tip_change
        .wait_for_tip_change()
        .now_or_never()
        .transpose()
        .expect("watch sender is not dropped");

    assert_eq!(first, None);

    assert_eq!(chain_tip_change.last_tip_change(), None);

    // try again, just to be sure
    let first = chain_tip_change
        .wait_for_tip_change()
        .now_or_never()
        .transpose()
        .expect("watch sender is not dropped");

    assert_eq!(first, None);

    assert_eq!(chain_tip_change.last_tip_change(), None);

    // also test our manual `Clone` impl
    #[allow(clippy::redundant_clone)]
    let first_clone = chain_tip_change
        .clone()
        .wait_for_tip_change()
        .now_or_never()
        .transpose()
        .expect("watch sender is not dropped");

    assert_eq!(first_clone, None);

    assert_eq!(chain_tip_change.last_tip_change(), None);
}

#[test]
fn empty_non_finalized_state_returns_tip_publication_to_finalized() {
    let initial = tip(10, 10, 9);
    let non_finalized = tip(12, 12, 11);
    let finalized = tip(11, 11, 10);
    let (mut sender, latest, _) = ChainTipSender::new(initial, &Mainnet);

    sender.set_best_non_finalized_tip(non_finalized.clone());
    sender.set_finalized_tip(finalized.clone());
    assert_eq!(latest.best_tip_hash(), Some(non_finalized.hash));

    sender.clear_best_non_finalized_tip(finalized.clone());
    assert_eq!(latest.best_tip_height(), Some(finalized.height));
    assert_eq!(latest.best_tip_hash(), Some(finalized.hash));
}

#[test]
fn late_broadcast_completion_does_not_rewind_the_tip_cursor() {
    let (mut sender, _, mut changes) = ChainTipSender::new(None, &Mainnet);
    sender.set_best_non_finalized_tip(Some(tip(2, 2, 1)));
    changes.mark_last_change_hash(block::Hash([2; 32]));
    changes.mark_last_change_hash(block::Hash([1; 32]));
    assert_eq!(changes.last_tip_change(), None);
    sender.set_best_non_finalized_tip(Some(tip(3, 3, 2)));
    assert_eq!(
        changes
            .last_tip_change()
            .unwrap()
            .best_tip_hash_and_height(),
        (block::Hash([3; 32]), block::Height(3))
    );
}

/// Returns a tip block with one real transaction, so it looks like a block with known contents.
fn tip_with_transaction(height: u32, hash_byte: u8, parent_byte: u8) -> ChainTipBlock {
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("test vector block deserializes");

    ChainTipBlock {
        transactions: block.transactions.clone(),
        transaction_hashes: block.transactions.iter().map(|tx| tx.hash()).collect(),
        ..tip(height, hash_byte, parent_byte)
    }
}

/// Publishes `count` tips that extend `parent`, one height and hash byte at a time.
fn publish_children(
    sender: &mut ChainTipSender,
    parent: &ChainTipBlock,
    count: u8,
) -> Vec<ChainTipBlock> {
    let mut parent = parent.clone();
    let mut published = Vec::new();

    for _ in 0..count {
        let child =
            tip_with_transaction(parent.height.0 + 1, parent.hash.0[0] + 1, parent.hash.0[0]);
        sender.set_best_non_finalized_tip(child.clone());
        published.push(child.clone());
        parent = child;
    }

    published
}

#[test]
fn skipped_blocks_are_reported_as_one_grow_in_chain_order() {
    let last_seen = tip_with_transaction(100, 100, 99);
    let (mut sender, _, mut changes) = ChainTipSender::new(last_seen.clone(), &Mainnet);
    assert!(changes.last_tip_change().expect("initial tip").is_reset());

    let mut published = publish_children(&mut sender, &last_seen, 3);
    let new_tip = published.pop().expect("three tips were published");

    assert_eq!(
        changes.last_tip_change(),
        Some(TipAction::grow_through(published, new_tip)),
    );
    assert_eq!(changes.last_tip_change(), None);
}

#[test]
fn waiting_for_a_tip_change_also_grows_through_skipped_blocks() {
    let last_seen = tip_with_transaction(100, 100, 99);
    let (mut sender, _, mut changes) = ChainTipSender::new(last_seen.clone(), &Mainnet);
    assert!(changes.last_tip_change().expect("initial tip").is_reset());

    let mut published = publish_children(&mut sender, &last_seen, 2);
    let new_tip = published.pop().expect("two tips were published");

    let action = changes
        .wait_for_tip_change()
        .now_or_never()
        .expect("a tip change is pending")
        .expect("watch sender is not dropped");
    assert_eq!(action, TipAction::grow_through(published, new_tip));
}

#[test]
fn skipping_more_blocks_than_the_channel_keeps_resets() {
    let last_seen = tip_with_transaction(100, 100, 99);

    // The channel keeps the last seen tip plus the blocks after it, so the most it can
    // report is one fewer skipped block than it keeps.
    let kept = MAX_RECENT_TIP_ANCESTORS as u8;
    for (published_count, expect_grow) in [(kept, true), (kept + 1, false)] {
        let (mut sender, _, mut changes) = ChainTipSender::new(last_seen.clone(), &Mainnet);
        assert!(changes.last_tip_change().expect("initial tip").is_reset());

        let published = publish_children(&mut sender, &last_seen, published_count);
        let new_tip = published.last().expect("tips were published");

        let action = changes.last_tip_change().expect("the tip changed");
        assert_eq!(action.best_tip_hash(), new_tip.hash);
        assert_eq!(
            !action.is_reset(),
            expect_grow,
            "{published_count} published tips after the last seen tip",
        );
    }
}

#[test]
fn a_fork_between_polls_resets() {
    let last_seen = tip_with_transaction(100, 100, 99);
    let (mut sender, _, mut changes) = ChainTipSender::new(last_seen.clone(), &Mainnet);
    assert!(changes.last_tip_change().expect("initial tip").is_reset());

    publish_children(&mut sender, &last_seen, 2);

    // A tip at the next height whose parent is not the published tip at height 101.
    let fork_tip = tip_with_transaction(103, 203, 201);
    sender.set_best_non_finalized_tip(fork_tip.clone());

    assert_eq!(
        changes.last_tip_change(),
        Some(TipAction::reset_with(fork_tip)),
    );
}

#[test]
fn a_skipped_block_before_an_activation_height_resets() {
    let activation_height = zakura_chain::parameters::NetworkUpgrade::Nu5
        .activation_height(&Mainnet)
        .expect("NU5 activates on Mainnet");

    // The skipped block at `activation_height - 1` is where the mempool must be cleared.
    let last_seen = tip_with_transaction(activation_height.0 - 2, 100, 99);
    let (mut sender, _, mut changes) = ChainTipSender::new(last_seen.clone(), &Mainnet);
    assert!(changes.last_tip_change().expect("initial tip").is_reset());

    let published = publish_children(&mut sender, &last_seen, 2);
    let new_tip = published.last().expect("two tips were published");
    assert_eq!(new_tip.height, activation_height);

    assert_eq!(
        changes.last_tip_change(),
        Some(TipAction::reset_with(new_tip.clone())),
    );
}

#[test]
fn a_skipped_block_without_transactions_resets() {
    let last_seen = tip_with_transaction(100, 100, 99);
    let (mut sender, _, mut changes) = ChainTipSender::new(last_seen.clone(), &Mainnet);
    assert!(changes.last_tip_change().expect("initial tip").is_reset());

    // A block whose transactions were pruned cannot be applied to the mempool.
    sender.set_best_non_finalized_tip(tip(101, 101, 100));
    let new_tip = tip_with_transaction(102, 102, 101);
    sender.set_best_non_finalized_tip(new_tip.clone());

    assert_eq!(
        changes.last_tip_change(),
        Some(TipAction::reset_with(new_tip)),
    );
}
