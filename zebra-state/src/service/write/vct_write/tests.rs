use std::{sync::Arc, time::Duration, time::Instant};

use tokio::sync::{mpsc, oneshot};
use zebra_chain::{block::Height, serialization::ZcashDeserializeInto};

use super::{VctWriteManager, VCT_AWAIT_SUCCESSOR_WAIT, VCT_ROOT_RETRY_WAIT};
use crate::{
    request::CheckpointVerifiedBlock, service::queued_blocks::QueuedCheckpointVerified,
    tests::FakeChainHelper,
};

/// Builds a distinct [`QueuedCheckpointVerified`] with a discarded response channel, so
/// tests can tell blocks apart by hash without caring about the response side.
fn queued_block(seed: u128) -> QueuedCheckpointVerified {
    let genesis = zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Arc<zebra_chain::block::Block>>()
        .expect("genesis block deserializes");
    let block = genesis.make_fake_child().set_work(seed);
    let (rsp_tx, _rsp_rx) = oneshot::channel();
    (CheckpointVerifiedBlock::from(block), rsp_tx)
}

/// Builds a queued block that extends `parent`, so it passes the
/// successor-linkage check in [`VctWriteManager::fill_successor`].
fn queued_child_of(parent: &QueuedCheckpointVerified, seed: u128) -> QueuedCheckpointVerified {
    let block = parent.0.block.clone().make_fake_child().set_work(seed);
    let (rsp_tx, _rsp_rx) = oneshot::channel();
    (CheckpointVerifiedBlock::from(block), rsp_tx)
}

#[test]
fn take_ready_returns_none_when_empty() {
    let mut manager = VctWriteManager::default();
    assert!(manager.take_ready().is_none());
}

#[test]
fn take_ready_prefers_retry_over_lookahead() {
    let mut manager = VctWriteManager::default();
    let retry_block = queued_block(1);
    let retry_hash = retry_block.0.hash;
    let lookahead_block = queued_block(2);
    let lookahead_hash = lookahead_block.0.hash;

    manager.lookahead.push_back(lookahead_block);
    manager.retry = Some(retry_block);

    let first = manager.take_ready().expect("retry block is ready");
    assert_eq!(
        first.0.hash, retry_hash,
        "retry must be taken before lookahead"
    );

    let second = manager.take_ready().expect("lookahead block is ready");
    assert_eq!(second.0.hash, lookahead_hash);

    assert!(manager.take_ready().is_none());
}

#[test]
fn defer_makes_block_the_next_ready_block() {
    let mut manager = VctWriteManager::default();
    let block = queued_block(1);
    let hash = block.0.hash;

    manager.defer(block);

    let ready = manager.take_ready().expect("deferred block is ready");
    assert_eq!(ready.0.hash, hash);
}

#[test]
fn fill_successor_only_buffers_one_linking_block_at_a_time() {
    let mut manager = VctWriteManager::default();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let current = queued_block(1);
    let first = queued_child_of(&current, 2);
    let first_hash = first.0.hash;
    let second = queued_child_of(&first, 3);
    let second_hash = second.0.hash;
    tx.send(first).expect("channel is open");
    tx.send(second).expect("channel is open");

    // First fill: lookahead was empty, so it pulls exactly one (linking) block.
    manager.fill_successor(&mut rx, &current);
    assert_eq!(manager.lookahead.len(), 1);
    assert_eq!(manager.lookahead.front().unwrap().0.hash, first_hash);

    // Second fill: the front already links to `current`, so it's a no-op — the
    // second block stays buffered in the channel, not in the look-ahead.
    manager.fill_successor(&mut rx, &current);
    assert_eq!(manager.lookahead.len(), 1);
    assert_eq!(manager.lookahead.front().unwrap().0.hash, first_hash);

    // Once the successor commits it becomes the current block; draining the
    // look-ahead lets the next fill pull that block's own successor.
    let committed = manager.take_ready().expect("look-ahead block is ready");
    manager.fill_successor(&mut rx, &committed);
    assert_eq!(manager.lookahead.front().unwrap().0.hash, second_hash);
}

/// A buffered block that does not extend the block being committed is discarded:
/// it can't witness the commit, and — because a parked retry is always taken
/// before the look-ahead — it would otherwise never be popped, wedging the retry
/// loop against a bogus witness that spuriously evicts good roots.
#[test]
fn fill_successor_discards_non_successors_and_keeps_the_linking_block() {
    let mut manager = VctWriteManager::default();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let current = queued_block(1);
    // A sibling of `current` (another child of genesis): buffered, but not linking.
    let non_successor = queued_block(2);
    let successor = queued_child_of(&current, 3);
    let successor_hash = successor.0.hash;

    manager.lookahead.push_back(non_successor);
    tx.send(successor).expect("channel is open");

    manager.fill_successor(&mut rx, &current);

    assert_eq!(manager.lookahead.len(), 1);
    assert_eq!(
        manager.lookahead.front().unwrap().0.hash,
        successor_hash,
        "the non-linking block is dropped and the true successor takes its place"
    );
    let witness = manager
        .next_vct_block()
        .expect("successor is buffered")
        .block;
    assert_eq!(witness.header.previous_block_hash, current.0.hash);
}

/// With no linking block buffered anywhere, the look-ahead ends up empty, so the
/// write loop's await-successor deferral engages instead of a bogus witness.
#[test]
fn fill_successor_empties_the_lookahead_when_no_successor_exists() {
    let mut manager = VctWriteManager::default();
    let (_tx, mut rx) = mpsc::unbounded_channel();
    let current = queued_block(1);
    manager.lookahead.push_back(queued_block(2));

    manager.fill_successor(&mut rx, &current);

    assert!(manager.is_lookahead_empty());
    assert!(manager.next_vct_block().is_none());
}

#[test]
fn next_vct_block_reflects_the_lookahead_front() {
    let mut manager = VctWriteManager::default();
    assert!(manager.next_vct_block().is_none());

    let block = queued_block(1);
    let expected_block = block.0.block.clone();
    let expected_auth_data_root = block.0.auth_data_root;
    manager.lookahead.push_back(block);

    let next_vct_block = manager.next_vct_block().expect("look-ahead has a block");
    assert_eq!(next_vct_block.block, expected_block);
    assert_eq!(next_vct_block.auth_data_root, expected_auth_data_root);
}

#[test]
fn reset_clears_the_lookahead() {
    let mut manager = VctWriteManager::default();
    manager.lookahead.push_back(queued_block(1));
    manager.lookahead.push_back(queued_block(2));

    let network = zebra_chain::parameters::Network::Mainnet;
    let config = crate::Config::ephemeral();
    let mut finalized_state = crate::service::finalized_state::FinalizedState::new(
        &config,
        &network,
        #[cfg(feature = "elasticsearch")]
        false,
    )
    .expect("opening an ephemeral database should succeed");

    manager.reset(&mut finalized_state);

    assert!(manager.is_lookahead_empty());
}

#[test]
fn on_commit_success_is_a_no_op_without_a_stall() {
    let mut manager = VctWriteManager::default();
    // Must not panic, and must leave the (already-clear) stall state alone.
    manager.on_commit_success();
    assert!(manager.stall.is_none());
    assert!(!manager.stall_logged);
}

#[test]
fn on_commit_success_clears_an_escalated_stall() {
    let mut manager = VctWriteManager::default();
    let height = Height(1);

    // Force the stall past the warn threshold so it gets escalated (logged).
    manager.stall = Some((height, Instant::now() - Duration::from_secs(31)));
    manager.on_retryable_error(height, true, queued_block(1));
    assert!(manager.stall_logged, "the stall should have been escalated");

    manager.on_commit_success();

    assert!(manager.stall.is_none());
    assert!(!manager.stall_logged);
}

#[test]
fn on_retryable_error_keeps_the_same_stall_start_for_a_repeated_height() {
    let mut manager = VctWriteManager::default();
    let height = Height(5);

    manager.on_retryable_error(height, true, queued_block(1));
    let first_seen = manager.stall.expect("a stall is now tracked").1;

    manager.on_retryable_error(height, true, queued_block(2));
    let still_first_seen = manager.stall.expect("the stall is still tracked").1;

    assert_eq!(
        first_seen, still_first_seen,
        "retrying the same height must not reset the stall's start time"
    );
}

#[test]
fn on_retryable_error_resets_the_stall_for_a_different_height() {
    let mut manager = VctWriteManager::default();

    manager.on_retryable_error(Height(1), true, queued_block(1));
    manager.stall_logged = true; // simulate an already-escalated stall

    manager.on_retryable_error(Height(2), true, queued_block(2));

    assert_eq!(manager.stall.map(|(h, _)| h), Some(Height(2)));
    assert!(
        !manager.stall_logged,
        "a new height starts a fresh, unescalated stall"
    );
}

#[test]
fn on_retryable_error_escalates_past_the_warn_threshold() {
    let mut manager = VctWriteManager::default();
    let height = Height(7);

    // Below the threshold: not escalated yet.
    manager.on_retryable_error(height, true, queued_block(1));
    assert!(!manager.stall_logged);

    // Backdate the stall past the warn threshold and retry the same height.
    manager.stall = Some((height, Instant::now() - Duration::from_secs(31)));
    manager.on_retryable_error(height, true, queued_block(2));
    assert!(manager.stall_logged);
}

#[test]
fn on_retryable_error_parks_the_block_for_retry() {
    let mut manager = VctWriteManager::default();
    let block = queued_block(1);
    let hash = block.0.hash;

    manager.on_retryable_error(Height(1), true, block);

    let ready = manager
        .take_ready()
        .expect("the block was parked for retry");
    assert_eq!(ready.0.hash, hash);
}

#[test]
fn on_retryable_error_wait_depends_on_root_availability() {
    let mut manager = VctWriteManager::default();

    let missing_root_wait = manager.on_retryable_error(Height(1), true, queued_block(1));
    assert_eq!(missing_root_wait, VCT_ROOT_RETRY_WAIT);

    let successor_wait = manager.on_retryable_error(Height(2), false, queued_block(2));
    assert_eq!(successor_wait, VCT_AWAIT_SUCCESSOR_WAIT);
}
