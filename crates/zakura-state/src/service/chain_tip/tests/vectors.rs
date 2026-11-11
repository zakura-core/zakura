use std::{iter, sync::Arc};

use chrono::Utc;
use futures::FutureExt;

use zakura_chain::{
    block,
    chain_tip::{ChainTip, NoChainTip},
    parameters::Network::*,
};

use super::super::{ChainTipBlock, ChainTipSender};

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

/// Reading tip metadata borrows the transaction vector rather than allocating a copy.
#[test]
fn tip_metadata_reads_do_not_clone_block_transactions() {
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("the fixture is a valid block");
    let mut initial = tip(1, 1, 0);
    initial.transactions = block.transactions.iter().cloned().map(Into::into).collect();
    let transactions_ptr = initial.transactions.as_ptr();
    let expected_time = initial.time;
    let (sender, latest, mut changes) = ChainTipSender::new(Some(initial), &Mainnet);

    assert_eq!(latest.best_tip_height(), Some(block::Height(1)));
    assert_eq!(latest.best_tip_hash(), Some(block::Hash([1; 32])));
    assert_eq!(
        latest.best_tip_height_and_hash(),
        Some((block::Height(1), block::Hash([1; 32])))
    );
    assert_eq!(latest.best_tip_block_time(), Some(expected_time));
    assert_eq!(
        latest.best_tip_height_and_block_time(),
        Some((block::Height(1), expected_time))
    );
    latest.with_chain_tip_metadata(|tip| {
        assert_eq!(
            tip.transactions.as_ptr(),
            transactions_ptr,
            "metadata reads use the published vector without cloning it"
        );
    });
    // Once the initial tip is seen, repeated polling takes the scalar-only path.
    changes.last_tip_change();
    assert!(changes.last_tip_change().is_none());
    drop(sender);
}
