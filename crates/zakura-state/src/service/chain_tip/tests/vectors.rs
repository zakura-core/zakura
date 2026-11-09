use std::{iter, sync::Arc};

use chrono::Utc;
use futures::FutureExt;

use zakura_chain::{
    block,
    chain_tip::{ChainTip, NoChainTip},
    parameters::{testnet::RegtestParameters, Network, Network::*},
};

use super::super::{ChainTipBlock, ChainTipSender, TipAction};

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

/// The mempool must revalidate what it holds when the post-quantum rules begin to apply.
///
/// A script in their namespace can hide inside a pay-to-script-hash redeem script, which the
/// legacy rules accept and these rules refuse, so a spend the mempool took before the height
/// is invalid from it on with nothing about the transaction changed. Only a reset drops it;
/// growing the pool keeps it and still offers it to mining.
#[test]
fn post_quantum_activation_resets_the_chain_tip() {
    // The chain reaches the activation height by growing onto the block below it, so the tip
    // that triggers the reset is the one before: the mempool verifies against the next height.
    let activation = 12;
    #[allow(
        clippy::needless_update,
        reason = "test-only fields depend on feature unification"
    )]
    let network = Network::new_regtest(RegtestParameters {
        pq_activation_height: Some(block::Height(activation)),
        ..Default::default()
    });

    let (mut sender, _, mut changes) = ChainTipSender::new(None, &network);

    // An ordinary step well below the height grows the chain, so the pool keeps its contents.
    sender.set_best_non_finalized_tip(Some(tip(activation - 3, 9, 8)));
    assert!(matches!(
        changes.last_tip_change(),
        Some(TipAction::Reset { .. })
    ));
    sender.set_best_non_finalized_tip(Some(tip(activation - 2, 10, 9)));
    assert!(
        matches!(changes.last_tip_change(), Some(TipAction::Grow { .. })),
        "a step below the activation height grows the chain"
    );

    // The step whose next block is the first under the new rules resets it.
    sender.set_best_non_finalized_tip(Some(tip(activation - 1, 11, 10)));
    assert!(
        matches!(
            changes.last_tip_change(),
            Some(TipAction::Reset {
                height: block::Height(h),
                ..
            }) if h == activation - 1
        ),
        "the block before the activation height resets the chain"
    );

    // Once the rules apply, every later step is ordinary growth again.
    sender.set_best_non_finalized_tip(Some(tip(activation, 12, 11)));
    assert!(
        matches!(changes.last_tip_change(), Some(TipAction::Grow { .. })),
        "a step above the activation height grows the chain"
    );

    // A chain that never applies the rules never resets for them.
    let (mut sender, _, mut changes) =
        ChainTipSender::new(None, &Network::new_regtest(<_>::default()));
    sender.set_best_non_finalized_tip(Some(tip(activation - 2, 10, 9)));
    assert!(matches!(
        changes.last_tip_change(),
        Some(TipAction::Reset { .. })
    ));
    sender.set_best_non_finalized_tip(Some(tip(activation - 1, 11, 10)));
    assert!(
        matches!(changes.last_tip_change(), Some(TipAction::Grow { .. })),
        "without a configured height there is nothing to reset for"
    );
}
