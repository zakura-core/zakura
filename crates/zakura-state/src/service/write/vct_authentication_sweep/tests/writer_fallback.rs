//! Exercise metadata availability through the production checkpoint write queue.

// Every height-to-usize cast below indexes the fixed fixture through TOP (24),
// so the values fit in usize on every supported target.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use zakura_node_services::sync_lifecycle::{
    HeaderRuntimeDetachedReason, HeaderRuntimeStatus, LifecycleEpoch,
};

use super::*;
use crate::service::{
    write::{
        BlockWriteNotice, BlockWriteSender, BlockWriteTaskExit, HeaderChainObservers,
        NonFinalizedWriteMessage, WriteBlockWorkerTask,
    },
    ChainTipSender,
};

type CommitResponse =
    oneshot::Receiver<Result<block::Hash, crate::error::CommitCheckpointVerifiedError>>;

struct RunningWriter {
    senders: BlockWriteSender,
    _invalid_resets: mpsc::UnboundedReceiver<block::Hash>,
    task: Option<Arc<std::thread::JoinHandle<BlockWriteTaskExit>>>,
    repair: watch::Receiver<VctRootRepairStatus>,
}

impl RunningWriter {
    /// Starts the production writer with an empty checkpoint queue.
    fn start(fixture: &Fixture) -> Self {
        Self::start_queued(fixture, &[]).0
    }

    /// Queues every block before the production loop starts, preventing an idle sweep.
    fn start_queued(fixture: &Fixture, blocks: &[Arc<Block>]) -> (Self, Vec<CommitResponse>) {
        let live = NonFinalizedState::new(&fixture.network);
        let (tip_sender, _, _) = ChainTipSender::new(None, &fixture.network);
        let (live_sender, _) = watch::channel(live.clone());
        let (snapshots, _) = watch::channel(None);
        let (views, _) = watch::channel(None);
        let (readers, _) = watch::channel(None);
        let (statuses, _) = watch::channel(HeaderRuntimeStatus::Detached {
            epoch: LifecycleEpoch::INITIAL,
            reason: HeaderRuntimeDetachedReason::AttachmentPending,
        });
        let (finalized, finalized_block_write_receiver) = mpsc::unbounded_channel();
        let (non_finalized, non_finalized_block_write_receiver) = mpsc::unbounded_channel();
        let (invalid_block_reset_sender, invalid_resets) = mpsc::unbounded_channel();
        let (non_finalized_write_update_sender, _) = mpsc::unbounded_channel();
        let (vct_root_repair_sender, repair) = watch::channel(VctRootRepairStatus::default());
        let (block_commit_sender, _) = watch::channel(BlockWriteNotice::default());
        let mut responses = Vec::new();
        for block in blocks {
            let (sender, receiver) = oneshot::channel();
            finalized
                .send((CheckpointVerifiedBlock::from(block.clone()), sender, 0))
                .expect("the initial checkpoint block queues");
            responses.push(receiver);
        }
        let task = WriteBlockWorkerTask {
            finalized_block_write_receiver,
            non_finalized_block_write_receiver,
            finalized_state: fixture.finalized_state.clone(),
            non_finalized_state: live,
            invalid_block_reset_sender,
            non_finalized_write_update_sender,
            chain_tip_sender: tip_sender,
            non_finalized_state_sender: live_sender,
            vct_root_repair_sender,
            block_commit_sender,
            backup_dir_path: None,
            header_chain: Some(HeaderChainWriter::new(
                fixture.writer.runtime.clone(),
                fixture.writer.config.clone(),
            )),
            attach_header_chain_at_handoff: false,
            header_chain_observers: HeaderChainObservers::new(snapshots, views, readers, statuses),
        };
        (
            Self {
                senders: BlockWriteSender {
                    finalized: Some(finalized),
                    non_finalized: Some(non_finalized),
                },
                _invalid_resets: invalid_resets,
                task: Some(Arc::new(std::thread::spawn(move || task.run()))),
                repair,
            },
            responses,
        )
    }

    /// Sends one checkpoint block to the running production loop.
    fn queue(&self, block: Arc<Block>) -> CommitResponse {
        let (sender, receiver) = oneshot::channel();
        self.senders
            .finalized
            .as_ref()
            .expect("checkpoint writes are enabled")
            .send((CheckpointVerifiedBlock::from(block), sender, 0))
            .expect("the writer accepts the checkpoint block");
        receiver
    }

    /// Requires the queued checkpoint block to commit within the fixture deadline.
    async fn commit(&self, block: Arc<Block>) {
        let expected = block.hash();
        let hash = tokio::time::timeout(Duration::from_secs(5), self.queue(block))
            .await
            .expect("available full trees or VCT metadata must allow the block to commit")
            .expect("the writer returns its response")
            .expect("the valid checkpoint block commits");
        assert_eq!(hash, expected);
    }
}

impl Drop for RunningWriter {
    fn drop(&mut self) {
        self.senders.finalized.take();
        self.senders.non_finalized.take();
        let task = Arc::into_inner(self.task.take().expect("the writer task exists"))
            .expect("the fixture owns the writer task");
        let result = task.join().expect("the writer does not panic");
        assert!(matches!(result, BlockWriteTaskExit::Completed));
    }
}

#[tokio::test]
async fn writer_recomputes_without_native_headers() {
    assert_writer_recomputes(None).await;
}

#[tokio::test]
async fn writer_recomputes_missing_roots_before_freezing() {
    assert_writer_recomputes(Some(Height(BODY_TIP + 1))).await;
}

#[tokio::test]
async fn writer_recomputes_missing_successor_before_freezing() {
    assert_writer_recomputes(Some(Height(BODY_TIP + 2))).await;
}

async fn assert_writer_recomputes(missing: Option<Height>) {
    let _init_guard = zakura_test::init();
    let mut fixture = Fixture::new();
    if missing.is_some() {
        fixture.insert_headers(missing, None);
    }
    let next = fixture.chain[BODY_TIP as usize + 1].clone();
    let mut reference = Fixture::new();
    reference
        .finalized_state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(next.clone()).into(),
            None,
            None,
            "ordinary recomputation reference",
        )
        .expect("current trees support ordinary recomputation");

    let writer = RunningWriter::start(&fixture);
    writer.commit(next.clone()).await;
    assert_eq!(fixture.finalized_state.vct_fast_count(), 0);
    assert_eq!(fixture.finalized_state.vct_fast_synced_below(), None);
    assert_eq!(
        fixture.finalized_state.db.history_tree().hash(),
        reference.finalized_state.db.history_tree().hash(),
    );
    assert_eq!(
        fixture.finalized_state.db.sapling_tree_for_tip(),
        reference.finalized_state.db.sapling_tree_for_tip(),
    );
    assert_eq!(
        fixture.finalized_state.db.orchard_tree_for_tip(),
        reference.finalized_state.db.orchard_tree_for_tip(),
    );
    assert_eq!(
        fixture
            .writer
            .runtime
            .publisher()
            .snapshot()
            .frontiers
            .finalized,
        Frontier::new(Height(BODY_TIP + 1), next.hash()),
    );
    // An availability fallback must not disable later fast commits.
    if let Some(missing) = missing {
        for height in BODY_TIP + 2..=missing.0 + 1 {
            writer.commit(fixture.chain[height as usize].clone()).await;
        }
        assert_eq!(fixture.finalized_state.vct_fast_count(), 1);
        assert_eq!(
            fixture.finalized_state.vct_fast_synced_below(),
            Some(Height(TOP + 1))
        );
    }
    drop(writer);
}

#[tokio::test]
async fn writer_waits_for_missing_successor_after_freezing() {
    let _init_guard = zakura_test::init();
    let mut fixture = Fixture::new();
    fixture.insert_headers(Some(Height(BODY_TIP + 3)), None);
    let writer = RunningWriter::start(&fixture);
    writer
        .commit(fixture.chain[BODY_TIP as usize + 1].clone())
        .await;
    assert_eq!(fixture.finalized_state.vct_fast_count(), 1);
    assert_eq!(
        fixture.finalized_state.vct_fast_synced_below(),
        Some(Height(TOP + 1))
    );
    let history = fixture.finalized_state.db.history_tree().hash();
    let mut response = writer.queue(fixture.chain[BODY_TIP as usize + 2].clone());
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut response)
            .await
            .is_err()
    );
    assert_eq!(
        fixture.finalized_state.db.finalized_tip_height(),
        Some(Height(BODY_TIP + 1))
    );
    assert_eq!(fixture.finalized_state.db.history_tree().hash(), history);
    drop(writer);
}

#[tokio::test]
async fn writer_keeps_bad_roots_pending_after_freezing() {
    let _init_guard = zakura_test::init();
    let mut fixture = Fixture::new();
    let bad = Height(BODY_TIP + 2);
    let successor = Height(BODY_TIP + 3);
    fixture.insert_headers(Some(successor), Some((bad, Corruption::SaplingRoot)));
    let writer = RunningWriter::start(&fixture);
    writer
        .commit(fixture.chain[BODY_TIP as usize + 1].clone())
        .await;
    assert_eq!(fixture.finalized_state.vct_fast_count(), 1);
    drop(writer);

    // The missing successor prevents early rejection of the bad roots. Deliver it
    // after the first fast commit so the writer encounters the failure while frozen.
    fixture.redeliver(successor, None, 0x79);
    let history = fixture.finalized_state.db.history_tree().hash();
    let writer = RunningWriter::start(&fixture);
    let mut response = writer.queue(fixture.chain[bad.0 as usize].clone());
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut response)
            .await
            .is_err()
    );
    assert!(fixture
        .authentications(bad)
        .contains(&TestAuxStatus::Disputed));
    assert_eq!(
        fixture.finalized_state.db.finalized_tip_height(),
        Some(Height(BODY_TIP + 1))
    );
    assert_eq!(fixture.finalized_state.db.history_tree().hash(), history);
    drop(writer);
}

/// A parked checkpoint must authenticate an available honest pair while more bodies are queued.
#[tokio::test]
async fn writer_recovers_crossed_disputes_without_an_idle_sweep() {
    assert_writer_recovers_crossed_disputes(false).await;
}

/// Recovery must not require metadata beyond the blocked commit's successor.
#[tokio::test]
async fn writer_recovers_crossed_disputes_without_following_metadata() {
    assert_writer_recovers_crossed_disputes(true).await;
}

/// Recovers through the production loop with no further input after the crossed repair.
async fn assert_writer_recovers_crossed_disputes(missing_following_metadata: bool) {
    let _init_guard = zakura_test::init();
    let mut fixture = Fixture::new();
    let blocked = Height(BODY_TIP + 2);
    let successor = Height(blocked.0 + 1);
    fixture.insert_headers(
        missing_following_metadata.then_some(Height(successor.0 + 1)),
        Some((successor, Corruption::AuthDataRoot)),
    );
    let last = if missing_following_metadata {
        blocked
    } else {
        successor
    };
    let blocks = &fixture.chain[BODY_TIP as usize + 1..=last.0 as usize];
    let (mut writer, responses) = RunningWriter::start_queued(&fixture, blocks);
    tokio::time::timeout(
        Duration::from_secs(5),
        writer
            .repair
            .wait_for(|status| status.state == VctRootRepairState::Unavailable { height: blocked }),
    )
    .await
    .expect("the parked block requests metadata repair")
    .expect("the writer keeps the repair channel open");
    assert!(fixture
        .authentications(blocked)
        .contains(&TestAuxStatus::Disputed));

    // The new supplier pairs a wrong predecessor with the honest successor. Both honest
    // payloads are now retained, but the newly admitted wrong pair ranks ahead of the dispute.
    let insert = fixture.redelivery(blocked, Some((blocked, Corruption::SaplingRoot)), 0x80);
    let authority = crate::HeaderChainBodyEvidenceAuthority::new_test();
    let (sender, receiver) = oneshot::channel();
    writer
        .senders
        .non_finalized
        .as_ref()
        .expect("the control channel is open")
        .send(NonFinalizedWriteMessage::ApplyHeaderChainInsert {
            prepared: authority.from_registered_header_attempt(insert),
            rsp_tx: sender,
        })
        .expect("the repair response reaches the production writer");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), receiver)
            .await
            .expect("the repair is processed")
            .expect("the writer answers the repair")
            .expect("the repair is coherent"),
        ApplyResult::Committed
    ));

    // No more peer input arrives. With H+2 metadata missing, recovery authenticates only H.
    // The committer must then reject the wrong H+1 row and retry H using the honest row.
    for (response, block) in responses.into_iter().zip(blocks) {
        let hash = tokio::time::timeout(Duration::from_secs(5), response)
            .await
            .expect("the parked writer finds the already retained honest pair")
            .expect("the writer answers every queued block")
            .expect("the valid checkpoint block commits");
        assert_eq!(hash, block.hash());
    }
    assert_eq!(
        fixture.finalized_state.vct_fast_count(),
        u64::from(last.0 - BODY_TIP)
    );
    if missing_following_metadata {
        assert!(fixture
            .authentications(successor)
            .contains(&TestAuxStatus::Rejected));
    }
    drop(writer);
}
