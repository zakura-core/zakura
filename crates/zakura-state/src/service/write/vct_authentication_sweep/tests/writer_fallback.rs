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
    write::{BlockWriteSender, BlockWriteTaskExit, HeaderChainObservers},
    ChainTipSender,
};

struct RunningWriter {
    senders: BlockWriteSender,
    _invalid_resets: mpsc::UnboundedReceiver<block::Hash>,
    task: Option<Arc<std::thread::JoinHandle<BlockWriteTaskExit>>>,
}

impl RunningWriter {
    fn start(fixture: &Fixture) -> Self {
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
        let (senders, invalid_resets, _, _, _, task) = BlockWriteSender::spawn_with_header_chain(
            fixture.finalized_state.clone(),
            live,
            tip_sender,
            live_sender,
            true,
            None,
            Some(HeaderChainWriter::new(
                fixture.writer.runtime.clone(),
                fixture.writer.config.clone(),
            )),
            false,
            HeaderChainObservers::new(snapshots, views, readers, statuses),
        );
        Self {
            senders,
            _invalid_resets: invalid_resets,
            task,
        }
    }

    fn queue(
        &self,
        block: Arc<Block>,
    ) -> oneshot::Receiver<Result<block::Hash, crate::error::CommitCheckpointVerifiedError>> {
        let (sender, receiver) = oneshot::channel();
        self.senders
            .finalized
            .as_ref()
            .expect("checkpoint writes are enabled")
            .send((CheckpointVerifiedBlock::from(block), sender))
            .expect("the writer accepts the checkpoint block");
        receiver
    }

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
