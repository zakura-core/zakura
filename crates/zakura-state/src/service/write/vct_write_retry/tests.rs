use std::{sync::Arc, time::Duration, time::Instant};

use tokio::sync::{mpsc, oneshot};
use zakura_chain::{block::Height, serialization::ZcashDeserializeInto};

use super::{
    VctWriteRetryCause, VctWriteRetryManager, VCT_AWAIT_SUCCESSOR_WAIT, VCT_ROOT_RETRY_WAIT,
};
use crate::{
    request::CheckpointVerifiedBlock,
    service::{
        queued_blocks::QueuedCheckpointVerified,
        write::{VctRootRepairState, VctRootRepairStatus},
    },
    tests::FakeChainHelper,
};

const MISSING_ROOT: VctWriteRetryCause = VctWriteRetryCause::MissingRoot {
    replacement_required: false,
};
const REJECTED_ROOT: VctWriteRetryCause = VctWriteRetryCause::MissingRoot {
    replacement_required: true,
};

/// Builds a distinct [`QueuedCheckpointVerified`] with a discarded response channel, so
/// tests can tell blocks apart by hash without caring about the response side.
fn queued_block(seed: u128) -> QueuedCheckpointVerified {
    let genesis = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
        .expect("genesis block deserializes");
    let block = genesis.make_fake_child().set_work(seed);
    let (rsp_tx, _rsp_rx) = oneshot::channel();
    (CheckpointVerifiedBlock::from(block), rsp_tx)
}

#[test]
fn take_retry_returns_none_when_empty() {
    let mut manager = VctWriteRetryManager::default();
    assert!(manager.take_retryable_block().is_none());
}

#[test]
fn parked_retry_is_taken_before_successor_and_leaves_channel_untouched() {
    let mut manager = VctWriteRetryManager::default();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let retry_block = queued_block(1);
    let retry_hash = retry_block.0.hash;
    let successor = queued_block(2);
    let successor_hash = successor.0.hash;

    manager.retryable_block = Some(retry_block);
    tx.send(successor).expect("channel is open");

    let first = manager
        .take_retryable_block()
        .expect("retry block is ready");
    assert_eq!(
        first.0.hash, retry_hash,
        "the stalled current block must be taken before channel input"
    );
    assert_eq!(
        rx.try_recv()
            .expect("taking the retry leaves the successor queued")
            .0
            .hash,
        successor_hash
    );
}

#[test]
fn on_commit_success_is_a_no_op_without_a_stall() {
    let mut manager = VctWriteRetryManager::default();
    // A successful commit leaves an inactive stall unchanged.
    manager.on_commit_success();
    assert!(manager.root_stall.is_none());
    assert!(!manager.root_stall_reported);
}

#[test]
fn on_commit_success_clears_an_escalated_stall() {
    let mut manager = VctWriteRetryManager::default();
    let height = Height(1);

    // The backdated start time forces the manager to report the stall.
    manager.root_stall = Some((height, Instant::now() - Duration::from_secs(31)));
    manager.on_retryable_error(height, MISSING_ROOT, queued_block(1));
    assert!(manager.root_stall_reported, "the manager reports the stall");

    manager.on_commit_success();

    assert!(manager.root_stall.is_none());
    assert!(!manager.root_stall_reported);
}

#[test]
fn on_retryable_error_keeps_the_same_stall_start_for_a_repeated_height() {
    let mut manager = VctWriteRetryManager::default();
    let height = Height(5);

    manager.on_retryable_error(height, MISSING_ROOT, queued_block(1));
    let first_seen = manager.root_stall.expect("a stall is now tracked").1;

    manager.on_retryable_error(height, MISSING_ROOT, queued_block(2));
    let still_first_seen = manager.root_stall.expect("the stall is still tracked").1;

    assert_eq!(
        first_seen, still_first_seen,
        "retrying the same height must not reset the stall's start time"
    );
}

#[test]
fn on_retryable_error_resets_the_stall_for_a_different_height() {
    let mut manager = VctWriteRetryManager::default();

    manager.on_retryable_error(Height(1), MISSING_ROOT, queued_block(1));
    manager.root_stall_reported = true;

    manager.on_retryable_error(Height(2), MISSING_ROOT, queued_block(2));

    assert_eq!(
        manager.root_stall.map(|(height, _)| height),
        Some(Height(2))
    );
    assert!(
        !manager.root_stall_reported,
        "a new height starts an unreported stall"
    );
}

#[test]
fn on_retryable_error_escalates_past_the_warn_threshold() {
    let mut manager = VctWriteRetryManager::default();
    let height = Height(7);

    // The manager does not report a new stall at error level.
    manager.on_retryable_error(height, MISSING_ROOT, queued_block(1));
    assert!(!manager.root_stall_reported);

    // The backdated start time moves the stall past the warning threshold.
    manager.root_stall = Some((height, Instant::now() - Duration::from_secs(31)));
    manager.on_retryable_error(height, MISSING_ROOT, queued_block(2));
    assert!(manager.root_stall_reported);
}

#[test]
fn on_retryable_error_parks_the_block_for_retry() {
    let mut manager = VctWriteRetryManager::default();
    let block = queued_block(1);
    let hash = block.0.hash;

    manager.on_retryable_error(Height(1), MISSING_ROOT, block);

    let ready = manager
        .take_retryable_block()
        .expect("the block was parked for retry");
    assert_eq!(ready.0.hash, hash);
}

#[test]
fn on_retryable_error_wait_depends_on_root_availability() {
    let mut manager = VctWriteRetryManager::default();

    let missing_root_wait = manager.on_retryable_error(Height(1), MISSING_ROOT, queued_block(1));
    assert_eq!(missing_root_wait, VCT_ROOT_RETRY_WAIT);

    let successor_wait = manager.on_retryable_error(
        Height(2),
        VctWriteRetryCause::MissingSuccessor,
        queued_block(2),
    );
    assert_eq!(successor_wait, VCT_AWAIT_SUCCESSOR_WAIT);
}

#[test]
fn root_repair_signal_deduplicates_repeated_missing_root_polls() {
    let (tx, mut rx) = tokio::sync::watch::channel(VctRootRepairStatus::default());
    let mut manager = VctWriteRetryManager::new(tx);

    manager.on_retryable_error(Height(42), MISSING_ROOT, queued_block(1));
    let first = *rx.borrow_and_update();
    assert_eq!(
        first.state,
        VctRootRepairState::Unavailable { height: Height(42) }
    );
    assert_eq!(first.generation, 1);

    manager.on_retryable_error(Height(42), MISSING_ROOT, queued_block(2));
    assert!(
        !rx.has_changed().expect("watch channel remains open"),
        "polling the same absent root must not flood repair notifications"
    );
}

#[test]
fn root_repair_signal_advances_generation_after_rejected_replacement() {
    let (tx, mut rx) = tokio::sync::watch::channel(VctRootRepairStatus::default());
    let mut manager = VctWriteRetryManager::new(tx);

    manager.on_retryable_error(Height(42), MISSING_ROOT, queued_block(1));
    let first = *rx.borrow_and_update();
    assert_eq!(first.generation, 1);

    manager.on_retryable_error(Height(42), REJECTED_ROOT, queued_block(2));
    let second = *rx.borrow_and_update();
    assert_eq!(second.generation, 2);
    assert_eq!(second.state, first.state);
}

#[test]
fn a_hidden_higher_sweep_need_keeps_the_lower_committer_episode() {
    let (tx, mut rx) = tokio::sync::watch::channel(VctRootRepairStatus::default());
    let mut manager = VctWriteRetryManager::new(tx);
    let committer_height = Height(42);
    let sweep_height = Height(84);

    manager.request_committer_repair_for_test(committer_height);
    let committer_status = *rx.borrow_and_update();
    assert_eq!(
        committer_status.state,
        VctRootRepairState::Unavailable {
            height: committer_height
        }
    );

    manager.request_sweep_repair(sweep_height);
    assert!(
        !rx.has_changed().expect("watch channel remains open"),
        "a hidden higher need must not restart the committer repair episode"
    );

    manager.on_commit_success();
    let sweep = *rx.borrow_and_update();
    assert_eq!(
        sweep.state,
        VctRootRepairState::Unavailable {
            height: sweep_height
        }
    );
    assert_eq!(sweep.generation, committer_status.generation + 1);
}

#[test]
fn root_repair_signal_ignores_await_successor_and_clears_on_commit() {
    let (tx, mut rx) = tokio::sync::watch::channel(VctRootRepairStatus::default());
    let mut manager = VctWriteRetryManager::new(tx);

    manager.on_retryable_error(
        Height(42),
        VctWriteRetryCause::MissingSuccessor,
        queued_block(1),
    );
    assert!(
        !rx.has_changed().expect("watch channel remains open"),
        "await-successor stalls do not need root repair"
    );

    manager.on_retryable_error(Height(42), MISSING_ROOT, queued_block(2));
    assert!(matches!(
        rx.borrow_and_update().state,
        VctRootRepairState::Unavailable { .. }
    ));

    manager.on_commit_success();
    assert_eq!(rx.borrow_and_update().state, VctRootRepairState::Idle);
}

#[test]
fn reset_withdraws_published_root_repair_need() {
    let _init_guard = zakura_test::init();
    let (tx, mut rx) = tokio::sync::watch::channel(VctRootRepairStatus::default());
    let mut manager = VctWriteRetryManager::new(tx);
    let mut finalized_state = crate::service::finalized_state::FinalizedState::new(
        &crate::Config::ephemeral(),
        &zakura_chain::parameters::Network::Mainnet,
    )
    .expect("opening an ephemeral finalized state succeeds");

    manager.on_retryable_error(Height(42), MISSING_ROOT, queued_block(1));
    let published = *rx.borrow_and_update();
    assert_eq!(
        published.state,
        VctRootRepairState::Unavailable { height: Height(42) }
    );

    manager.reset(&mut finalized_state);
    let withdrawn = *rx.borrow_and_update();
    assert_eq!(
        withdrawn.state,
        VctRootRepairState::Idle,
        "a queue reset must withdraw the published repair need"
    );
    assert_eq!(
        withdrawn.generation, published.generation,
        "withdrawing a repair must not consume a new generation"
    );

    // A stall that persists across the reset re-publishes under a new generation.
    manager.on_retryable_error(Height(42), MISSING_ROOT, queued_block(2));
    let republished = *rx.borrow_and_update();
    assert_eq!(
        republished.state,
        VctRootRepairState::Unavailable { height: Height(42) }
    );
    assert_eq!(republished.generation, published.generation + 1);
}
