use super::*;
use std::collections::BTreeMap;

use crate::zakura::block_sync::reorder::RetainedBody;

use zakura_chain::serialization::ZcashSerialize;
use zakura_test::vectors::{BLOCK_MAINNET_2_BYTES, BLOCK_MAINNET_3_BYTES, BLOCK_MAINNET_5_BYTES};

/// The reorder, applying, and in-flight submission gauges, and the view
/// admission reads, equal sums over the bodies the sequencer holds through
/// out-of-order acceptance, drain, submission, detachment, completion, commit,
/// and reset. The gauges are read before the view is published, so each must
/// follow the sequencer on its own. Each retained body keeps its request's
/// admission estimate and its allocation's identity from reorder to applying.
/// Once the driver drops its submissions, no other holder keeps a body alive.
#[tokio::test]
async fn byte_gauges_and_view_follow_sequencer_owners() {
    let recorder = super::super::super::tests::gauge_recorder::GaugeRecorder::default();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let frontiers = genesis_frontiers();
    let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (_body_tx, body_rx) = mpsc::channel(1);
    let (_control_tx, control_rx) = mpsc::unbounded_channel();
    let (actions, mut actions_rx) = mpsc::channel(8);
    let (view_tx, view_rx) = watch::channel(initial_view(frontiers));
    let mut task = SequencerTask::new(
        Sequencer::new(block::Height(0), 2),
        ByteBudget::new(1),
        Arc::new(WorkQueue::new(block::Height(0))),
        Arc::new(PeerRegistry::new()),
        actions,
        ThroughputMeter::new(Instant::now()),
        frontiers,
        Some(super::super::test_work_scope()),
        crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
        Arc::new(crate::zakura::block_sync::BlockSyncClock),
        body_rx,
        control_rx,
        input_bytes.clone(),
        input_decoded_bytes.clone(),
        view_tx,
        Duration::from_secs(60),
        ZakuraTrace::noop(),
    );
    let check = |task: &mut SequencerTask, label: &str| {
        let owners = task.sequencer.ownership_snapshot();
        let reorder: u64 = owners.reorder.iter().map(|body| body.bytes).sum();
        let applying: u64 = owners.applying.iter().map(|body| body.bytes).sum();
        let submissions: u64 = owners.submissions.iter().map(|owner| owner.bytes).sum();
        assert_eq!(
            (
                recorder.bytes("sync.block.reorder.buffered_bytes"),
                recorder.bytes("sync.block.applying.buffered_bytes"),
                recorder.bytes("sync.block.submission.in_flight_bytes"),
            ),
            (reorder, applying, submissions),
            "gauges after {label}"
        );
        task.publish_view();
        let view = *view_rx.borrow();
        assert_eq!(
            view.reorder_buffered_bytes, reorder,
            "reorder bytes after {label}"
        );
        assert_eq!(
            view.applying_buffered_bytes, applying,
            "applying bytes after {label}"
        );
        assert_eq!(
            view.in_flight_submission_bytes, submissions,
            "submission bytes after {label}"
        );
        owners
    };

    // Heights 3 and 5 arrive before height 1, so they wait in the reorder
    // buffer. Height 4 never arrives, so height 5 stays there until the reset.
    let fixtures = [
        (3, BLOCK_MAINNET_3_BYTES.as_slice()),
        (5, BLOCK_MAINNET_5_BYTES.as_slice()),
        (1, BLOCK_MAINNET_1_BYTES.as_slice()),
        (2, BLOCK_MAINNET_2_BYTES.as_slice()),
    ];
    // Each request reserved a hint below the body's wire size.
    let estimate = |height: u32| 1_000 + u64::from(height);
    let mut bodies = Vec::new();
    let mut allocations = BTreeMap::new();
    for (index, (height, bytes)) in fixtures.into_iter().enumerate() {
        let height = block::Height(height);
        let block: Arc<block::Block> = Arc::new(bytes.zcash_deserialize_into().unwrap());
        let raw: Arc<[u8]> = block.zcash_serialize_to_vec().unwrap().into();
        let wire_bytes = raw.len() as u64;
        allocations.insert(height, Arc::as_ptr(&raw).cast::<u8>().addr());
        bodies.push((height, Arc::downgrade(&block), Arc::downgrade(&raw)));
        let mut body = SequencedBody::new_queued(
            super::super::test_work_owner(),
            zakura_header_chain::SourceId::from_digest([1; 32]),
            height,
            block.hash(),
            block.header.previous_block_hash,
            BufferedBlockBody::from_decoded_block(block, Some(raw)),
            wire_bytes,
            Some(estimate(height.0)),
            ZakuraPeerId::new(vec![1; 32]).unwrap(),
            Instant::now(),
            input_bytes.clone(),
            input_decoded_bytes.clone(),
        );
        body.leave_queue();
        task.handle_accept_body(body);
        if index == 1 {
            let owners = check(&mut task, "out-of-order acceptance");
            assert_eq!(owners.reorder.len(), 2);
            assert!(owners.applying.is_empty());
        }
    }
    // Height 2 drains heights 2 and 3 after height 1.
    let owners = check(&mut task, "drain");
    assert_eq!(
        owners.reorder,
        [RetainedBody {
            height: block::Height(5),
            owner: super::super::test_work_owner(),
            bytes: BLOCK_MAINNET_5_BYTES.len() as u64,
            admitted_estimate_bytes: Some(estimate(5)),
            allocation: allocations[&block::Height(5)],
            decoded_attributed_memory_bytes: 0,
        }]
    );
    let applying: Vec<_> = owners
        .applying
        .iter()
        .map(|body| (body.height, body.admitted_estimate_bytes, body.allocation))
        .collect();
    assert_eq!(
        applying,
        [1, 2, 3].map(|height| {
            let height = block::Height(height);
            (height, Some(estimate(height.0)), allocations[&height])
        }),
        "draining keeps each body's estimate and allocation"
    );
    let fixture_bytes: usize = fixtures
        .iter()
        .filter(|(height, _)| *height != 5)
        .map(|(_, bytes)| bytes.len())
        .sum();
    assert_eq!(
        recorder.bytes("sync.block.applying.buffered_bytes"),
        fixture_bytes as u64,
        "the applying gauge counts the fixtures' wire bytes"
    );

    // The two-body submission window submits heights 1 and 2.
    task.submit_pending_blocks().await;
    let submitted: Vec<_> = std::iter::from_fn(|| actions_rx.try_recv().ok())
        .filter_map(|action| match action {
            BlockSyncAction::SubmitBlock {
                owner,
                source,
                token,
                block,
            } => Some((owner, source, token, block)),
            _ => None,
        })
        .collect();
    assert_eq!(submitted.len(), 2);
    assert_eq!(check(&mut task, "submission").submissions.len(), 2);

    // Height 2 leaves `applying`, but the driver still holds its submission.
    task.sequencer.remove_applying(block::Height(2));
    let owners = check(&mut task, "detachment");
    assert!(owners
        .submissions
        .iter()
        .any(|owner| owner.height == block::Height(2) && owner.detached));

    let (owner, source, token, block) = &submitted[1];
    assert!(task.sequencer.finish_submission(
        *owner,
        *source,
        *token,
        block::Height(2),
        block.hash()
    ));
    check(&mut task, "detached completion");

    task.sequencer.advance_verified_tip(block::Height(1), true);
    check(&mut task, "commit");

    task.destructive_reset_to(block::Height(0), false);
    assert!(check(&mut task, "reset").reorder.is_empty());

    let (owner, source, token, block) = &submitted[0];
    task.sequencer
        .finish_submission(*owner, *source, *token, block::Height(1), block.hash());
    let owners = check(&mut task, "final completion");
    assert!(owners.applying.is_empty());
    assert!(owners.submissions.is_empty());

    // Once the driver drops its submissions, the sequencer holds no body.
    drop(submitted);
    for (height, block, raw) in &bodies {
        assert!(
            block.upgrade().is_none() && raw.upgrade().is_none(),
            "a holder keeps the height {} body alive",
            height.0
        );
    }
}

/// Dropping the task reports that it closed before it releases the bodies
/// left in its input, so those releases need no next holder. It then
/// reports an empty sequencer.
#[tokio::test]
async fn dropping_the_task_releases_its_queued_bodies_after_it_closes() {
    use super::super::block_sync_observe::{
        BlockEvent, BlockSyncObserver, HeldBody, Holder, HolderChange, ObserverSlot,
        SequencerOwnership,
    };

    #[derive(Debug, Default)]
    struct Log(std::sync::Mutex<Vec<BlockEvent>>);
    impl BlockSyncObserver for Log {
        fn observe(&self, event: BlockEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    let frontiers = genesis_frontiers();
    let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (body_tx, body_rx) = mpsc::channel(1);
    let (_control_tx, control_rx) = mpsc::unbounded_channel();
    let (actions, _actions_rx) = mpsc::channel(8);
    let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
    let log = Arc::new(Log::default());
    let task = SequencerTask::new(
        Sequencer::new(block::Height(0), 2),
        ByteBudget::new(1),
        Arc::new(WorkQueue::new(block::Height(0))),
        Arc::new(PeerRegistry::new()),
        actions,
        ThroughputMeter::new(Instant::now()),
        frontiers,
        Some(super::super::test_work_scope()),
        crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
        Arc::new(crate::zakura::block_sync::BlockSyncClock),
        body_rx,
        control_rx,
        input_bytes.clone(),
        input_decoded_bytes.clone(),
        view_tx,
        Duration::from_secs(60),
        ZakuraTrace::noop(),
    )
    .with_observer(ObserverSlot(Some(log.clone())));

    let block: Arc<block::Block> =
        Arc::new(BLOCK_MAINNET_2_BYTES.zcash_deserialize_into().unwrap());
    let raw: Arc<[u8]> = block.zcash_serialize_to_vec().unwrap().into();
    let allocation = Arc::as_ptr(&raw).cast::<u8>().addr();
    let bytes = raw.len() as u64;
    let decoded_attributed_memory_bytes = block.attributed_memory_size_bytes();
    body_tx
        .try_send(SequencedBody::new_queued(
            super::super::test_work_owner(),
            zakura_header_chain::SourceId::from_digest([1; 32]),
            block::Height(2),
            block.hash(),
            block.header.previous_block_hash,
            BufferedBlockBody::from_decoded_block(block, Some(raw)),
            bytes,
            None,
            ZakuraPeerId::new(vec![1; 32]).unwrap(),
            Instant::now(),
            input_bytes,
            input_decoded_bytes,
        ))
        .unwrap_or_else(|_| panic!("the input has room"));
    drop(task);

    let events = std::mem::take(&mut *log.0.lock().unwrap());
    assert_eq!(
        events.last(),
        Some(&BlockEvent::Sequencer(SequencerOwnership::default()))
    );
    assert_eq!(
        events[..2],
        [
            BlockEvent::SequencerClosed,
            BlockEvent::Holder {
                body: HeldBody {
                    height: block::Height(2),
                    owner: super::super::test_work_owner(),
                    bytes,
                },
                holder: Holder::Queued {
                    allocation,
                    decoded_attributed_memory_bytes,
                },
                change: HolderChange::Released,
            },
        ]
    );
}
