use super::*;
use std::collections::BTreeMap;

use crate::zakura::block_sync::reorder::RetainedBody;

use zakura_chain::serialization::ZcashSerialize;
use zakura_test::vectors::{BLOCK_MAINNET_2_BYTES, BLOCK_MAINNET_3_BYTES, BLOCK_MAINNET_5_BYTES};

/// The reorder, applying, and in-flight submission gauges, and the view
/// admission reads, equal sums over the bodies the sequencer holds through
/// out-of-order acceptance, drain, submission, detachment, completion, commit,
/// and reset. IO publishes the gauges at completed transition boundaries. Each retained body keeps its request's
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
    let budget = ByteBudget::new(1);
    let mut task = SequencerIO::new(
        Sequencer::new(block::Height(0), 2),
        Arc::new(WorkQueue::new(block::Height(0), budget)),
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
        ZakuraBlockSyncConfig::default()
            .decoded_block_pool(10_000)
            .expect("fixture decoded allowance fits"),
    );
    let check = |task: &mut SequencerIO, label: &str| {
        task.payloads.apply(&mut task.state.sequencer);
        let owners = task
            .state
            .sequencer
            .ownership_snapshot(&task.payloads.entries);
        assert!(
            owners.payloads_match_policy(),
            "runtime inventory after {label}"
        );
        let reorder: u64 = owners.reorder.iter().map(|body| body.bytes).sum();
        let applying: u64 = owners.applying.iter().map(|body| body.bytes).sum();
        let submissions: u64 = owners.submissions.iter().map(|owner| owner.bytes).sum();
        task.publish_view();
        assert_eq!(
            (
                recorder.bytes("sync.block.reorder.buffered_bytes"),
                recorder.bytes("sync.block.applying.buffered_bytes"),
                recorder.bytes("sync.block.submission.in_flight_bytes"),
            ),
            (reorder, applying, submissions),
            "gauges after {label}"
        );
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
            payload_id: 2,
            policy_decoded_bytes: 0,
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
    task.state.sequencer.remove_applying(block::Height(2));
    let owners = check(&mut task, "detachment");
    assert!(owners
        .submissions
        .iter()
        .any(|owner| owner.height == block::Height(2) && owner.detached));

    let (owner, source, token, block) = &submitted[1];
    assert!(task.state.sequencer.finish_submission(
        *owner,
        *source,
        *token,
        block::Height(2),
        block.hash()
    ));
    check(&mut task, "detached completion");

    task.state
        .sequencer
        .advance_verified_tip(block::Height(1), true);
    check(&mut task, "commit");

    let mut operations = Vec::new();
    task.state
        .destructive_reset_to(block::Height(0), false, &mut operations);
    task.execute(operations).await;
    assert!(check(&mut task, "reset").reorder.is_empty());

    let (owner, source, token, block) = &submitted[0];
    task.state
        .sequencer
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

    #[derive(Debug)]
    struct Log {
        events: std::sync::Mutex<Vec<BlockEvent>>,
        retained: std::sync::Weak<block::Block>,
    }
    impl BlockSyncObserver for Log {
        fn observe(&self, event: BlockEvent) {
            if matches!(&event, BlockEvent::Sequencer(owners) if *owners == SequencerOwnership::default())
            {
                assert!(
                    self.retained.upgrade().is_none(),
                    "empty teardown snapshot must follow actual payload release"
                );
            }
            self.events.lock().unwrap().push(event);
        }
    }

    let frontiers = genesis_frontiers();
    let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (body_tx, body_rx) = mpsc::channel(1);
    let (_control_tx, control_rx) = mpsc::unbounded_channel();
    let (actions, _actions_rx) = mpsc::channel(8);
    let (view_tx, _view_rx) = watch::channel(initial_view(frontiers));
    let retained = test_block();
    let log = Arc::new(Log {
        events: std::sync::Mutex::new(Vec::new()),
        retained: Arc::downgrade(&retained),
    });
    let budget = ByteBudget::new(1);
    let mut task = SequencerIO::new(
        Sequencer::new(block::Height(0), 2),
        Arc::new(WorkQueue::new(block::Height(0), budget)),
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
        ZakuraBlockSyncConfig::default()
            .decoded_block_pool(10_000)
            .expect("fixture decoded allowance fits"),
    )
    .with_observer(ObserverSlot(Some(log.clone())));

    let mut accepted = SequencedBody::new_queued(
        super::super::test_work_owner(),
        zakura_header_chain::SourceId::from_digest([1; 32]),
        block::Height(1),
        retained.hash(),
        retained.header.previous_block_hash,
        BufferedBlockBody::from_decoded_block(retained, None),
        1_000,
        None,
        ZakuraPeerId::new(vec![1; 32]).unwrap(),
        Instant::now(),
        input_bytes.clone(),
        input_decoded_bytes.clone(),
    );
    accepted.leave_queue();
    task.handle_accept_body(accepted);
    assert!(log.retained.upgrade().is_some());

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

    let events = std::mem::take(&mut *log.events.lock().unwrap());
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

fn runtime_payload(
    height: block::Height,
    block: Arc<block::Block>,
) -> (Sequencer, SequencerPayloads, std::sync::Weak<block::Block>) {
    let weak = Arc::downgrade(&block);
    let frame = BlockSyncMessage::Block(block.clone())
        .encode_frame()
        .unwrap();
    let (_, Some(raw)) = BlockSyncMessage::decode_frame_with_raw_block_payload(frame).unwrap()
    else {
        panic!("the block frame retains its raw payload");
    };
    let hash = block.hash();
    let previous = block.header.previous_block_hash;
    let bytes = u64::try_from(raw.len()).unwrap();
    let mut payloads = SequencerPayloads::default();
    let facts = payloads.insert(BufferedBlockBody::from_decoded_block(block, Some(raw)));
    let mut policy = Sequencer::new(block::Height(0), 1);
    assert!(matches!(
        policy.accept_buffered_body(
            super::super::test_work_owner(),
            zakura_header_chain::SourceId::from_digest([1; 32]),
            height,
            hash,
            previous,
            facts,
            bytes,
            None,
            ZakuraPeerId::new(vec![1; 32]).unwrap(),
        ),
        super::super::sequencer::AcceptOutcome::Buffered { .. }
    ));
    policy.drain_ready_into_applying();
    payloads.apply(&mut policy);
    (policy, payloads, weak)
}

#[test]
fn runtime_payload_demotion_drops_the_decoded_copy() {
    let block = Arc::new(
        BLOCK_MAINNET_2_BYTES
            .as_slice()
            .zcash_deserialize_into()
            .unwrap(),
    );
    let (policy, payloads, weak) = runtime_payload(block::Height(2), block);
    assert_eq!(policy.reorder_len(), 1);
    assert_eq!(payloads.retained_ids().count(), 1);
    assert!(
        weak.upgrade().is_none(),
        "raw retention must release the decoded payload"
    );
}

#[test]
fn runtime_payload_reset_preserves_driver_copy_but_policy_clones_retain_no_payload() {
    let (mut policy, mut payloads, weak) = runtime_payload(block::Height(1), test_block());
    let original = weak
        .upgrade()
        .expect("the runtime owner retains the input decode");
    let prepared = payloads
        .prepare(
            &mut policy,
            block::Height(1),
            &DecodedBlockPool::new(u64::MAX),
        )
        .unwrap()
        .unwrap();
    assert!(
        matches!(&prepared.block, BlockHandle::Shared(block) if Arc::ptr_eq(&original, block)),
        "submission reuses the handoff decode"
    );
    assert_eq!(prepared.newly_decoded_wire_bytes, None);
    drop(original);
    let policy_snapshot = policy.clone();
    policy.reset_to(block::Height(0), false);
    payloads.apply(&mut policy);
    assert!(
        weak.upgrade().is_some(),
        "the driver still holds its submitted copy"
    );
    drop(prepared);
    assert!(
        weak.upgrade().is_none(),
        "reset owner retained decoded payload after driver drop"
    );
    assert_eq!(payloads.retained_ids().count(), 0);
    assert_eq!(
        policy_snapshot.applying_len(),
        1,
        "the policy snapshot still holds only facts"
    );
}

#[test]
fn runtime_payload_inventory_tracks_reset_and_untracked_clone() {
    let (mut policy, mut payloads, _) = runtime_payload(block::Height(1), test_block());
    assert!(policy
        .ownership_snapshot(&payloads.entries)
        .payloads_match_policy());
    let retained = payloads.entries.values().next().unwrap().clone();
    let untracked = payloads.insert(retained);
    let snapshot = policy.ownership_snapshot(&payloads.entries);
    assert_eq!(snapshot.payloads.len(), 2);
    assert_eq!(snapshot.applying.len(), 1);
    assert!(
        !snapshot.payloads_match_policy(),
        "capture must see an untracked shared allocation"
    );
    payloads.release(untracked.id);
    assert!(policy
        .ownership_snapshot(&payloads.entries)
        .payloads_match_policy());
    policy.reset_to(block::Height(0), false);
    payloads.apply(&mut policy);
    assert!(
        policy
            .ownership_snapshot(&payloads.entries)
            .payloads_match_policy(),
        "capture must detect runtime retention after policy reset"
    );
}

#[test]
fn runtime_payload_inventory_compares_actual_size_with_policy_size() {
    let (mut policy, payloads, _) = runtime_payload(block::Height(1), test_block());
    let facts = policy.submission_body(block::Height(1)).unwrap();
    policy
        .prepare_submit(block::Height(1), facts.id, facts.decoded_bytes + 1)
        .unwrap();
    assert!(
        !policy
            .ownership_snapshot(&payloads.entries)
            .payloads_match_policy(),
        "capture must measure the actual payload independently of policy size"
    );
}

#[tokio::test]
async fn decoded_capacity_release_retries_pending_body_without_apply_completion() {
    use std::{future::Future, task::Poll};
    use zakura_node_services::block_memory::Admission;

    for (release_before_wait, reset_before_release) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let pool = DecodedBlockPool::for_block_count(std::num::NonZeroU64::new(1).unwrap());
        let blocker = pool
            .try_admit(
                Arc::try_unwrap(test_block()).unwrap(),
                Admission::WithinLimit,
            )
            .unwrap();
        let retained = BlockHandle::Owned(blocker).transaction(0).unwrap();
        let expected_hash = retained.hash();
        let mut task = retention_test_task(u64::MAX);
        task.decoded_pool = pool.clone();
        let (actions, mut actions_rx) = mpsc::channel(1);
        task.actions = actions;
        let (_body_tx, body_rx) = mpsc::channel(1);
        task.body_input_rx = body_rx;
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        task.control_input_rx = control_rx;
        let block = test_block();
        let raw = BlockSyncMessage::Block(block.clone())
            .encode()
            .unwrap()
            .into();
        let mut body = SequencedBody::new_queued(
            super::super::test_work_owner(),
            zakura_header_chain::SourceId::from_digest([1; 32]),
            block::Height(1),
            block.hash(),
            block.header.previous_block_hash,
            BufferedBlockBody::RawFramePayload(raw),
            BLOCK_MAINNET_1_BYTES.len() as u64,
            None,
            ZakuraPeerId::new(vec![1; 32]).unwrap(),
            Instant::now(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
        );
        body.leave_queue();
        task.handle_accept_body(body);
        task.submit_pending_blocks().await;
        assert!(task.decoded_capacity_blocked.is_some());
        assert!(actions_rx.try_recv().is_err());
        assert_eq!(pool.reserved_blocks(), Some(1));
        assert!(
            task.payloads
                .entries
                .values()
                .all(|body| !body.is_decoded()),
            "failed admission must leave the raw payload intact"
        );
        assert_eq!(
            retained.hash(),
            expected_hash,
            "the downstream payload remains readable"
        );
        if reset_before_release {
            control_tx
                .send(SequencerControlInput::FrontierReset {
                    frontiers: genesis_frontiers(),
                    preserve_active_successors: false,
                    peer_has_successor_after: false,
                    peer_outstanding_conflicts_at_tip: false,
                })
                .unwrap();
        }
        let mut retained = Some(retained);
        if release_before_wait {
            drop(retained.take());
        }
        let mut running = Box::pin(task.run());
        if !release_before_wait {
            std::future::poll_fn(|cx| {
                assert!(running.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert!(actions_rx.try_recv().is_err());
            drop(retained.take());
        }
        std::future::poll_fn(|cx| {
            assert!(running.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        if reset_before_release {
            assert!(
                actions_rx.try_recv().is_err(),
                "capacity release must not submit stale work"
            );
            assert_eq!(pool.reserved_blocks(), Some(0));
            assert_eq!(pool.reserved_bytes(), 0);
            continue;
        }
        let action = actions_rx
            .try_recv()
            .expect("last-holder release must make the pending submission ready");
        let BlockSyncAction::SubmitBlock {
            block: submitted, ..
        } = action
        else {
            panic!("capacity release must submit the pending body");
        };
        assert!(matches!(submitted, BlockHandle::Owned(_)));
        assert_eq!(submitted.hash(), block.hash());
        assert_eq!(pool.reserved_blocks(), Some(1));
        drop(running);
        assert_eq!(
            pool.reserved_blocks(),
            Some(1),
            "the driver still retains the allocation"
        );
        drop(submitted);
        assert_eq!(pool.reserved_blocks(), Some(0));
        assert_eq!(pool.reserved_bytes(), 0);
    }
}

/// The verified tip cannot wait on the byte bound: a commit-window body decodes
/// and submits while the bound is full. The driver's terminal handoff then frees
/// the share while downstream retention keeps the retained charge.
#[tokio::test]
async fn commit_window_body_submits_at_a_full_decoded_byte_bound() {
    use zakura_node_services::block_memory::Admission;

    let pool = DecodedBlockPool::for_pipeline(std::num::NonZeroU64::new(8).unwrap(), 1);
    let blocker = pool
        .try_admit(
            Arc::try_unwrap(test_block()).unwrap(),
            Admission::PipelineExempt,
        )
        .unwrap();
    let blocker_bytes = blocker.decoded_bytes();
    let mut task = retention_test_task(u64::MAX);
    task.decoded_pool = pool.clone();
    let (actions, mut actions_rx) = mpsc::channel(1);
    task.actions = actions;
    let block = test_block();
    let raw = BlockSyncMessage::Block(block.clone())
        .encode()
        .unwrap()
        .into();
    let mut body = SequencedBody::new_queued(
        super::super::test_work_owner(),
        zakura_header_chain::SourceId::from_digest([1; 32]),
        block::Height(1),
        block.hash(),
        block.header.previous_block_hash,
        BufferedBlockBody::RawFramePayload(raw),
        BLOCK_MAINNET_1_BYTES.len() as u64,
        None,
        ZakuraPeerId::new(vec![1; 32]).unwrap(),
        Instant::now(),
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
    );
    body.leave_queue();
    task.handle_accept_body(body);
    assert_eq!(
        task.state
            .sequencer()
            .applying_decoded_attributed_memory_bytes(),
        0
    );
    task.submit_pending_blocks().await;

    assert_eq!(task.decoded_capacity_blocked, None);
    let BlockSyncAction::SubmitBlock {
        block: submitted, ..
    } = actions_rx.try_recv().expect("the window body submits")
    else {
        panic!("the commit-window body must submit");
    };
    let decoded_bytes = submitted.view().attributed_memory_size_bytes();
    assert_eq!(
        task.state
            .sequencer()
            .applying_decoded_attributed_memory_bytes(),
        decoded_bytes,
        "the policy total follows the raw body's decode"
    );
    assert_eq!(pool.pipeline_bytes(), blocker_bytes + decoded_bytes);

    submitted.leave_pipeline();
    assert_eq!(pool.pipeline_bytes(), blocker_bytes);
    assert_eq!(pool.reserved_bytes(), blocker_bytes + decoded_bytes);
    drop(blocker);
    assert_eq!(pool.pipeline_bytes(), 0);
    assert_eq!(pool.reserved_bytes(), decoded_bytes);
}

/// Raw bodies cost the decoded pool nothing. A refused decode keeps them raw, an
/// admitted decode charges the measured size once, and reuse charges nothing more.
#[test]
fn decoded_byte_totals_follow_raw_to_decoded_changes() {
    use zakura_node_services::block_memory::Admission;

    let block = test_block();
    let bytes = block.attributed_memory_size_bytes();
    let pool = DecodedBlockPool::for_pipeline(std::num::NonZeroU64::new(8).unwrap(), bytes);
    let blocker = pool
        .try_admit(
            Arc::try_unwrap(test_block()).unwrap(),
            Admission::WithinLimit,
        )
        .unwrap();
    let raw: Arc<[u8]> = BlockSyncMessage::Block(block).encode().unwrap().into();
    let mut body = BufferedBlockBody::RawFramePayload(raw);

    assert_eq!(
        body.decoded_block(&pool, Admission::WithinLimit)
            .unwrap_err(),
        AdmissionFailure::Capacity
    );
    assert!(!body.is_decoded(), "refusal keeps the raw fallback");
    assert_eq!(body.decoded_attributed_memory_size_bytes(), 0);
    assert_eq!(pool.pipeline_bytes(), bytes);

    blocker.leave_pipeline();
    assert_eq!(pool.pipeline_bytes(), 0);
    let decoded = body.decoded_block(&pool, Admission::WithinLimit).unwrap();
    assert_eq!(body.decoded_attributed_memory_size_bytes(), bytes);
    assert_eq!(pool.pipeline_bytes(), bytes);
    let reused = body.decoded_block(&pool, Admission::WithinLimit).unwrap();
    assert_eq!(pool.pipeline_bytes(), bytes, "reuse does not decode again");
    assert_eq!(pool.reserved_bytes(), 2 * bytes);

    body.retain_for_backlog_in_place();
    assert_eq!(body.decoded_attributed_memory_size_bytes(), 0);
    assert_eq!(pool.pipeline_bytes(), bytes, "live handles keep the charge");
    drop((decoded, reused));
    assert_eq!(pool.pipeline_bytes(), 0);
    assert_eq!(pool.reserved_bytes(), bytes, "the backend holder remains");
    drop(blocker);
    assert_eq!(pool.reserved_bytes(), 0);
    assert!(!pool.is_closed());
}
