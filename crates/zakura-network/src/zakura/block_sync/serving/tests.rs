use super::*;
use crate::zakura::{
    block_sync::{
        peer_registry::{PeerRegistry, SessionAdmission},
        serving_regulation::GetBlocksServingRegulator,
    },
    framed_channel,
    transport::{worker_framed_channel, FramedWorkerRecv},
    FramedSend,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Condvar, Mutex,
};
use tokio::sync::Notify;
use tokio_util::task::AbortOnDropHandle;

#[derive(Debug)]
struct Storage {
    calls: AtomicUsize,
    changed: Notify,
    release: (Mutex<bool>, Condvar),
    fail: bool,
    available: usize,
}

#[derive(Debug)]
struct Source(Arc<Storage>);

impl Source {
    fn new(released: bool) -> Arc<Self> {
        Self::with_outcome(released, false, 2)
    }

    fn with_outcome(released: bool, fail: bool, available: usize) -> Arc<Self> {
        Arc::new(Self(Arc::new(Storage {
            calls: AtomicUsize::new(0),
            changed: Notify::new(),
            release: (Mutex::new(released), Condvar::new()),
            fail,
            available,
        })))
    }

    async fn wait_calls(&self, count: usize) {
        time::timeout(Duration::from_secs(2), async {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.0.calls.load(Ordering::Acquire) >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }

    fn release(&self) {
        *self.0.release.0.lock().unwrap() = true;
        self.0.release.1.notify_all();
    }
}

impl BlockRangeSource for Source {
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, crate::BoxError>> {
        let storage = self.0.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let (start, count, max_bytes, lease) = request.into_parts();
                assert!(lease.try_start());
                storage.calls.fetch_add(1, Ordering::AcqRel);
                storage.changed.notify_waiters();
                let (released, wait) = storage
                    .release
                    .1
                    .wait_timeout_while(
                        storage.release.0.lock().unwrap(),
                        Duration::from_secs(5),
                        |released| !*released,
                    )
                    .unwrap();
                assert!(
                    *released && !wait.timed_out(),
                    "the test must release its database job"
                );
                if storage.fail {
                    return Err(io::Error::other("test storage failure").into());
                }
                let vectors = [
                    &*zakura_test::vectors::BLOCK_MAINNET_1_BYTES,
                    &*zakura_test::vectors::BLOCK_MAINNET_2_BYTES,
                ];
                let mut blocks = Vec::new();
                let mut bytes = 0;
                for (index, encoded) in vectors.into_iter().enumerate() {
                    let height = block::Height(u32::try_from(index + 1).unwrap());
                    if lease.is_cancelled()
                        || blocks.len() >= usize::try_from(count).unwrap().min(storage.available)
                    {
                        break;
                    }
                    if height < start {
                        continue;
                    }
                    bytes += encoded.len();
                    if bytes > usize::try_from(max_bytes).unwrap() {
                        break;
                    }
                    blocks.push((
                        height,
                        Arc::new(block::Block::zcash_deserialize(encoded.as_slice()).unwrap()),
                        encoded.len(),
                    ));
                }
                Ok(BlockRangeReadResult::new(blocks, lease))
            })
            .await?
        })
    }
}

struct Fixture {
    session: BlockSyncPeerSession,
    requests: FramedSend,
    data: FramedWorkerRecv,
    regulator: GetBlocksServingRegulator,
    registry: Arc<PeerRegistry>,
    status: watch::Sender<BlockSyncStatus>,
    task: AbortOnDropHandle<Result<(), crate::zakura::SinkReject>>,
}

impl Fixture {
    fn new(source: Arc<dyn BlockRangeSource>) -> Self {
        Self::with_queue_depth(source, 1)
    }

    fn with_queue_depth(source: Arc<dyn BlockRangeSource>, depth: usize) -> Self {
        let config = ZakuraBlockSyncConfig {
            max_blocks_per_response: 2,
            ..ZakuraBlockSyncConfig::default()
        };
        let registry = Arc::new(PeerRegistry::new());
        let peer = ZakuraPeerId::new(vec![111; 32]).unwrap();
        let SessionAdmission::Fresh { generation } = registry.admit_session(
            &peer,
            ServicePeerDirection::Inbound,
            &config,
            1,
            Instant::now(),
        ) else {
            panic!("fresh fixture")
        };
        let regulator = GetBlocksServingRegulator::new(config.clone());
        let admission = regulator.session(peer.clone());
        let (send, data) = worker_framed_channel(depth);
        let session = BlockSyncPeerSession::for_test_with_session_id(
            peer,
            generation,
            send,
            CancellationToken::new(),
        );
        let (requests, recv) = framed_channel(1);
        let (status, status_rx) = watch::channel(BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(2),
            ..config.initial_status()
        });
        let task = AbortOnDropHandle::new(tokio::spawn(serve_requests(
            session.clone(),
            recv,
            admission,
            registry.clone(),
            status_rx,
            Some(source),
            crate::zakura::ZakuraTrace::noop(),
        )));
        Self {
            session,
            requests,
            data,
            regulator,
            registry,
            status,
            task,
        }
    }

    async fn request(&self) {
        self.requests
            .send(
                BlockSyncMessage::GetBlocks {
                    start_height: block::Height(1),
                    count: 2,
                }
                .encode_frame()
                .unwrap(),
            )
            .await
            .unwrap();
    }

    async fn next(&mut self) -> BlockSyncMessage {
        let queued = time::timeout(Duration::from_secs(2), self.data.recv())
            .await
            .unwrap()
            .unwrap();
        let (frame, _guard) = queued.into_parts();
        BlockSyncMessage::decode_frame(frame).unwrap()
    }

    async fn finish(self) {
        self.session.cancel_token().cancel();
        time::timeout(Duration::from_secs(2), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn request_before_status_waits_and_then_receives_the_complete_range() {
    let source = Source::new(true);
    let mut f = Fixture::new(source.clone());
    f.request().await;
    tokio::task::yield_now().await;
    assert_eq!(source.0.calls.load(Ordering::Acquire), 0);
    f.session.mark_status_received();
    assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
    assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
    assert!(matches!(
        f.next().await,
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(1),
            returned: 2
        }
    ));
    assert_eq!(source.0.calls.load(Ordering::Acquire), 1);
    f.finish().await;
}

#[tokio::test]
async fn storage_outcomes_preserve_the_response_prefix_and_ending_under_backpressure() {
    for depth in 1..=3 {
        for available in 0..=2 {
            for fail in [false, true] {
                let source = Source::with_outcome(true, fail, available);
                let mut f = Fixture::with_queue_depth(source.clone(), depth);
                let regulator = f.regulator.clone();
                f.session.mark_status_received();
                f.request().await;
                let returned = if fail { 0 } else { available };
                for height in 1..=returned {
                    let BlockSyncMessage::Block(block) = f.next().await else {
                        panic!("the response prefix must contain its available blocks");
                    };
                    assert_eq!(
                        block.coinbase_height(),
                        Some(block::Height(u32::try_from(height).unwrap()))
                    );
                }
                match f.next().await {
                    BlockSyncMessage::RangeUnavailable {
                        start_height,
                        count,
                    } => {
                        assert_eq!(returned, 0);
                        assert_eq!((start_height, count), (block::Height(1), 2));
                    }
                    BlockSyncMessage::BlocksDone {
                        start_height,
                        returned: count,
                    } => {
                        assert!(returned > 0);
                        assert_eq!(start_height, block::Height(1));
                        assert_eq!(usize::try_from(count).unwrap(), returned);
                    }
                    other => panic!("the response must end after its available prefix: {other:?}"),
                }
                assert_eq!(source.0.calls.load(Ordering::Acquire), 1);
                f.finish().await;
                assert_eq!(regulator.snapshot().node_active, 0);
                assert_eq!(regulator.snapshot().peer_active, 0);
            }
        }
    }
}

#[tokio::test]
async fn a_retained_response_frame_blocks_the_next_read_from_that_peer() {
    let source = Source::new(true);
    let mut f = Fixture::new(source.clone());
    f.session.mark_status_received();
    f.request().await;
    source.wait_calls(1).await;
    let held = time::timeout(Duration::from_secs(2), f.data.recv())
        .await
        .unwrap()
        .unwrap();
    f.request().await;
    assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
    assert!(matches!(
        f.next().await,
        BlockSyncMessage::BlocksDone { returned: 2, .. }
    ));
    tokio::task::yield_now().await;
    assert_eq!(source.0.calls.load(Ordering::Acquire), 1);
    assert_eq!(f.regulator.snapshot().node_active, 1);
    assert_eq!(f.regulator.snapshot().peer_active, 1);
    drop(held);
    source.wait_calls(2).await;
    f.finish().await;
}

#[tokio::test]
async fn aborting_the_serving_task_keeps_a_running_database_job_charged() {
    let source = Source::new(false);
    let mut f = Fixture::new(source.clone());
    f.session.mark_status_received();
    f.request().await;
    source.wait_calls(1).await;
    f.task.abort();
    assert!((&mut f.task).await.unwrap_err().is_cancelled());
    assert_eq!(f.regulator.snapshot().node_active, 1);
    assert_eq!(f.regulator.snapshot().peer_active, 1);
    let replacement = f.regulator.session(f.session.peer_id().clone());
    let request = super::super::serving_regulation::GetBlocksRequest {
        start_height: block::Height(1),
        count: 2,
    };
    let mut pending = Box::pin(replacement.admit_request(&request));
    assert!(futures::poll!(&mut pending).is_pending());
    assert_eq!(
        f.regulator.snapshot().node_active,
        1,
        "the replacement waits for its peer before acquiring node capacity"
    );
    source.release();
    let permit = time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap();
    assert_eq!(f.regulator.snapshot().node_active, 1);
    drop(permit);
    assert_eq!(f.regulator.snapshot().node_active, 0);
}

#[tokio::test(start_paused = true)]
async fn missing_status_expires_without_starting_storage() {
    let source = Source::new(true);
    let mut f = Fixture::new(source.clone());
    tokio::task::yield_now().await;
    time::advance(Duration::from_secs(11)).await;
    assert!((&mut f.task).await.unwrap().is_err());
    assert_eq!(source.0.calls.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn paired_version_is_selected_without_a_test_override() {
    use crate::zakura::Service;
    let service = BlockSyncService::new(ZakuraBlockSyncConfig::default());
    assert_eq!(service.streams().len(), 2);
    assert!(service.ordered_stream_pair(service.streams()[0]).is_some());
    assert_eq!(service.streams()[0].version, 3);
    assert_eq!(service.streams()[0].capability, 1 << 6);
}

#[tokio::test]
async fn a_slow_storage_read_keeps_ownership_without_a_query_timeout() {
    let source = Source::new(false);
    let mut f = Fixture::new(source.clone());
    f.session.mark_status_received();
    f.request().await;
    source.wait_calls(1).await;
    time::pause();
    time::advance(Duration::from_secs(9)).await;
    assert!(!f.task.is_finished());
    assert_eq!(f.regulator.snapshot().node_active, 1);
    time::resume();
    source.release();
    assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
    assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
    assert!(matches!(
        f.next().await,
        BlockSyncMessage::BlocksDone { returned: 2, .. }
    ));
    f.finish().await;
}

#[tokio::test]
async fn serving_does_not_read_above_the_committed_status_range() {
    let source = Source::new(true);
    let mut f = Fixture::new(source.clone());
    f.status
        .send_modify(|status| status.servable_high = block::Height(0));
    f.session.mark_status_received();
    f.request().await;
    assert!(matches!(
        f.next().await,
        BlockSyncMessage::RangeUnavailable {
            start_height: block::Height(1),
            count: 2
        }
    ));
    assert_eq!(source.0.calls.load(Ordering::Acquire), 0);
    f.finish().await;
}

#[tokio::test]
async fn local_count_and_byte_limits_end_after_the_permitted_prefix() {
    for byte_limit in [false, true] {
        let source = Source::new(true);
        let mut f = Fixture::new(source);
        f.status.send_modify(|status| {
            if byte_limit {
                status.max_response_bytes =
                    u32::try_from(zakura_test::vectors::BLOCK_MAINNET_1_BYTES.len()).unwrap();
            } else {
                status.max_blocks_per_response = 1;
            }
        });
        f.session.mark_status_received();
        f.request().await;
        assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
        assert!(matches!(
            f.next().await,
            BlockSyncMessage::BlocksDone { returned: 1, .. }
        ));
        f.finish().await;
    }
}

#[tokio::test]
async fn a_stale_session_cannot_read_after_waiting_for_admission() {
    let source = Source::new(true);
    let mut f = Fixture::new(source.clone());
    let held: Vec<_> = (0..64)
        .map(|id| {
            f.regulator
                .session(ZakuraPeerId::new(vec![id; 32]).unwrap())
                .admit_now(1)
                .unwrap()
                .commit()
        })
        .collect();
    f.session.mark_status_received();
    f.request().await;
    time::timeout(Duration::from_secs(2), async {
        while f.regulator.snapshot().peer_active < 65 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(source.0.calls.load(Ordering::Acquire), 0);
    f.registry
        .remove_session(f.session.peer_id(), f.session.session_id());
    drop(held);
    time::timeout(Duration::from_secs(2), &mut f.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(source.0.calls.load(Ordering::Acquire), 0);
    assert_eq!(f.regulator.snapshot().node_active, 0);
    assert_eq!(f.regulator.snapshot().peer_active, 0);
    assert!(time::timeout(Duration::from_millis(20), f.data.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn cancellation_during_storage_suppresses_old_output_and_retains_the_job() {
    let source = Source::new(false);
    let mut f = Fixture::new(source.clone());
    f.session.mark_status_received();
    f.request().await;
    source.wait_calls(1).await;
    f.session.cancel_token().cancel();
    time::timeout(Duration::from_secs(2), &mut f.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(f.regulator.snapshot().node_active, 1);
    source.release();
    time::timeout(Duration::from_secs(2), async {
        while f.regulator.snapshot().node_active > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.regulator.snapshot().peer_active, 0);
    assert!(time::timeout(Duration::from_millis(20), f.data.recv())
        .await
        .is_err());
}
