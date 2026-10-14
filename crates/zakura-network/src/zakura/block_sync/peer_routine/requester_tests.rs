use super::super::request::BlockSizeEstimate;
use super::*;
use crate::zakura::regulation::ReservationPool;
use zakura_chain::serialization::ZcashDeserializeInto;

/// The routine, its outbound frames, its pool, its connection and its reports to the reactor.
type RequesterRoutine = (
    PeerRoutine,
    FramedRecv,
    ReservationPool,
    CancellationToken,
    mpsc::Receiver<RoutineToReactor>,
);

/// [`requester_routine_with_pool`] with a private two-entry pool.
fn requester_routine(expected_hash: block::Hash) -> RequesterRoutine {
    requester_routine_with_pool(expected_hash, ReservationPool::new(2).unwrap())
}

/// Builds a live requester that can fetch height 1 with `expected_hash` from `pool`.
fn requester_routine_with_pool(
    expected_hash: block::Hash,
    pool: ReservationPool,
) -> RequesterRoutine {
    let connection = CancellationToken::new();
    let (mut routine, outbound, events) =
        super::tests::status_test_routine_with(pool.clone(), &connection);
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(10),
        max_blocks_per_response: 1,
        ..BlockSyncStatus::default()
    });
    routine.work.extend(
        super::super::test_work_scope(),
        [(
            block::Height(1),
            expected_hash,
            BlockSizeEstimate::Confirmed(1000),
        )],
    );
    (routine, outbound, pool, connection, events)
}

/// Encodes `message` and hands it to the routine as one inbound frame.
async fn try_deliver(
    routine: &mut PeerRoutine,
    message: BlockSyncMessage,
) -> Result<(), SinkReject> {
    let frame = message.encode_frame().expect("test messages are encodable");
    routine.handle_frame(&mut block_sync_guard(), frame).await
}

/// [`try_deliver`] for a frame the routine must accept.
async fn deliver(routine: &mut PeerRoutine, message: BlockSyncMessage) {
    try_deliver(routine, message)
        .await
        .expect("the routine accepts the frame");
}

/// Waits up to one second for the routine's next outbound frame.
async fn next_sent(outbound: &mut FramedRecv) -> crate::zakura::Frame {
    time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .expect("the routine sends promptly")
        .expect("the outbound channel stays open")
}

/// Parses a mainnet block test vector.
fn mainnet_block(bytes: &[u8]) -> Arc<block::Block> {
    Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
}

#[tokio::test]
async fn replacement_requests_from_cached_status_without_waiting_for_an_announcement() {
    let (mut routine, mut outbound, _events) = super::tests::status_test_routine();
    routine.session = routine
        .session
        .clone()
        .with_status_for_test(BlockSyncStatus {
            servable_high: block::Height(10),
            ..Default::default()
        });
    let (_input, recv) = crate::zakura::framed_channel(4);
    routine.recv = recv;
    let (_view, view_rx) = watch::channel(*routine.sequencer_view.borrow());
    routine.sequencer_view = view_rx;
    routine.work.extend(
        super::super::test_work_scope(),
        [(
            block::Height(1),
            block::Hash([1; 32]),
            BlockSizeEstimate::Confirmed(1000),
        )],
    );
    assert!(!routine.received_status);
    let mut running = Box::pin(routine.run());
    let request = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::select! {
            request = outbound.recv() => request.unwrap(),
            result = &mut running => panic!("routine ended before requesting: {result:?}"),
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        BlockSyncMessage::decode_frame(request).unwrap(),
        BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            ..
        }
    ));
}

#[tokio::test]
async fn written_timeout_preserves_authorization_and_blocks_overlap_until_ending() {
    let (mut routine, mut outbound, pool, connection, _) = requester_routine(block::Hash([1; 32]));
    routine.try_fill().await;
    let request = next_sent(&mut outbound).await;
    assert!(matches!(
        BlockSyncMessage::decode_frame(request).unwrap(),
        BlockSyncMessage::GetBlocks { .. }
    ));
    assert_eq!(pool.held(), 1);
    let deadline = routine.window.outstanding[0].deadline;
    assert!(routine.expire_due_timeouts(deadline));
    assert!(routine.window.outstanding.is_empty());
    assert_eq!(
        pool.held(),
        1,
        "written authorization survives scheduler timeout"
    );
    let grace = routine.config.effective_liveness_timeout();
    let retirement = routine.requester.retirement_deadline(grace).unwrap();
    routine.retry_avoid.clear();
    routine.window.note_block_progress(Instant::now(), grace);
    assert_eq!(routine.try_fill().await, Some(retirement));
    assert!(
        routine.window.outstanding.is_empty(),
        "the old range still authorizes a response"
    );
    assert!(routine.work.pending_contains(block::Height(1)));
    let before = routine.next_request_id.unwrap().get();
    let (_input, recv) = crate::zakura::framed_channel(4);
    routine.recv = recv;
    let (_view, view_rx) = watch::channel(*routine.sequencer_view.borrow());
    routine.sequencer_view = view_rx;
    let mut guard = block_sync_guard();
    let mut running = Box::pin(routine.run_inner(&mut guard));
    // Re-poll a parked routine without new work or an ending. The initial BBR
    // tick can trigger one extra fill, but there must be no take/return spin.
    for _ in 0..100 {
        assert!(futures::poll!(&mut running).is_pending());
    }
    drop(running);
    assert!(routine.next_request_id.unwrap().get() - before <= 2);

    let ending = BlockSyncMessage::RangeUnavailable {
        start_height: block::Height(1),
        count: 1,
    };
    deliver(&mut routine, ending.clone()).await;
    assert_eq!(pool.held(), 0);
    assert!(!connection.is_cancelled());
    assert!(routine.check_request_liveness(retirement).is_ok());
    assert!(
        try_deliver(&mut routine, ending).await.is_err(),
        "a second ending is unsolicited"
    );
}

#[tokio::test]
async fn unwritten_timeout_retracts_authorization_without_closing_connection() {
    let (mut routine, _outbound, pool, connection, _) = requester_routine(block::Hash([1; 32]));
    routine.try_fill().await;
    assert_eq!(pool.held(), 1);
    let deadline = routine.window.outstanding[0].deadline;
    assert!(routine.expire_due_timeouts(deadline));
    assert_eq!(pool.held(), 0);
    assert!(!connection.is_cancelled());
}

#[tokio::test]
async fn wrong_header_is_rejected_before_transaction_decoding_or_decode_capacity() {
    let (mut routine, mut outbound, pool, connection, _) = requester_routine(block::Hash([1; 32]));
    routine.config.max_blocks_per_response = 3;
    routine.max_blocks_per_response = 3;
    routine.work.extend(
        super::super::test_work_scope(),
        [
            (
                block::Height(2),
                block::Hash([2; 32]),
                BlockSizeEstimate::Confirmed(1000),
            ),
            (
                block::Height(3),
                block::Hash([3; 32]),
                BlockSizeEstimate::Confirmed(1000),
            ),
        ],
    );
    routine.try_fill().await;
    assert_eq!(routine.window.outstanding[0].request.count, 3);
    next_sent(&mut outbound).await;
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let mut payload = vec![MSG_BS_BLOCK];
    block.header.zcash_serialize(&mut payload).unwrap();
    // There is no transaction count. Header identity must reject this frame
    // before the full decoder or the closed sequencer channel is reached.
    let error = routine
        .handle_frame(
            &mut block_sync_guard(),
            crate::zakura::Frame {
                message_type: 3,
                flags: 0,
                payload,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, SinkReject::Local(_)));
    assert!(error.to_string().contains("next expected header"));
    assert!(connection.is_cancelled());
    assert_eq!(
        pool.held(),
        1,
        "retirement keeps authorization until cleanup"
    );
    let registry = routine.registry.clone();
    let peer = routine.peer.clone();
    let work = routine.work.clone();
    drop(routine);
    assert!(work.pending_contains(block::Height(1)));
    registry.remove(&peer);
    assert!(registry.is_body_retry_avoided(
        &peer,
        super::super::test_work_scope(),
        block::Hash([1; 32]),
        Instant::now()
    ));
    for byte in [2, 3] {
        assert!(work.pending_contains(block::Height(u32::from(byte))));
        assert!(registry.is_body_retry_avoided(
            &peer,
            super::super::test_work_scope(),
            block::Hash([byte; 32]),
            Instant::now()
        ));
    }
    assert!(!registry.is_body_retry_avoided(
        &peer,
        super::super::test_work_scope(),
        block::Hash([4; 32]),
        Instant::now()
    ));
    let other_peer = ZakuraPeerId::new(vec![255; 32]).unwrap();
    assert!(!registry.is_body_retry_avoided(
        &other_peer,
        super::super::test_work_scope(),
        block::Hash([2; 32]),
        Instant::now()
    ));
}

#[tokio::test]
async fn invalid_ending_keeps_the_written_exchange_fenced() {
    let (mut routine, mut outbound, pool, connection, _) = requester_routine(block::Hash([1; 32]));
    routine.try_fill().await;
    next_sent(&mut outbound).await;
    let ending = BlockSyncMessage::BlocksDone {
        start_height: block::Height(1),
        returned: 1,
    };
    assert!(try_deliver(&mut routine, ending).await.is_err());
    assert_eq!(pool.held(), 1);
    drop(routine);
    assert!(
        connection.is_cancelled(),
        "dropping an unanswered written exchange closes its connection"
    );
}

#[tokio::test]
async fn local_finality_or_competing_receipt_keeps_the_original_response_authorized() {
    for finalized in [true, false] {
        let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
        let (mut routine, mut outbound, pool, connection, _) = requester_routine(block.hash());
        let (body_sender, mut bodies) = mpsc::channel(1);
        routine.sequencer_input = body_sender;
        let (view_tx, view_rx) = watch::channel(*routine.sequencer_view.borrow());
        routine.sequencer_view = view_rx;
        routine.try_fill().await;
        next_sent(&mut outbound).await;
        if finalized {
            let released = routine.work.advance_floor(block::Height(1));
            routine.budget.release(released);
            view_tx.send_modify(|view| view.download_floor = block::Height(1));
            routine.gc_committed_outstanding();
        } else {
            // The shared work ledger records another peer's receipt before ours.
            let released = routine.work.claim_received(block::Height(1));
            routine.budget.release(released);
        }
        assert_eq!(pool.held(), 1);
        deliver(&mut routine, BlockSyncMessage::Block(block)).await;
        assert!(
            bodies.try_recv().is_err(),
            "already satisfied work is not delivered twice"
        );
        deliver(
            &mut routine,
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(1),
                returned: 1,
            },
        )
        .await;
        assert_eq!(pool.held(), 0);
        drop(routine);
        assert!(!connection.is_cancelled());
    }
}

#[tokio::test]
async fn an_authorized_block_after_local_retirement_is_not_a_peer_violation() {
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let (mut routine, mut outbound, pool, connection, mut events) = requester_routine(block.hash());
    let (body_sender, _bodies) = mpsc::channel(1);
    routine.sequencer_input = body_sender;
    routine.try_fill().await;
    next_sent(&mut outbound).await;
    // A local timeout drops the window entry but keeps the written reservation.
    let deadline = routine.window.outstanding[0].deadline;
    assert!(routine.expire_due_timeouts(deadline));
    assert!(routine.window.outstanding.is_empty());
    assert_eq!(pool.held(), 1);
    // A local reorg drops the height, and the peer's later Status contracts its range.
    let released = routine.work.reset_above(block::Height(0));
    routine.budget.release(released);
    routine.servable_low = block::Height(0);
    routine.servable_high = block::Height(0);
    while events.try_recv().is_ok() {}

    deliver(&mut routine, BlockSyncMessage::Block(block)).await;
    let reported = std::iter::from_fn(|| events.try_recv().ok())
        .any(|event| matches!(event, RoutineToReactor::Misbehavior { .. }));
    assert!(
        !reported,
        "the peer answered a request it was authorized to answer"
    );
    deliver(
        &mut routine,
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(1),
            returned: 1,
        },
    )
    .await;
    assert_eq!(pool.held(), 0);
    drop(routine);
    assert!(!connection.is_cancelled());
}

#[tokio::test]
async fn an_authorized_body_above_its_size_estimate_is_not_a_peer_violation() {
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1046401_BYTES);
    let height = block.coinbase_height().unwrap();
    let (mut routine, mut outbound, _pool, _connection, mut events) =
        requester_routine(block.hash());
    // Queue this 73 KB body behind the smallest estimate, far beyond tolerance.
    let released = routine.work.reset_above(block::Height(0));
    routine.budget.release(released);
    routine.work.extend(
        super::super::test_work_scope(),
        [(height, block.hash(), BlockSizeEstimate::Confirmed(500))],
    );
    routine.servable_high = height;
    let (body_sender, mut bodies) = mpsc::channel(1);
    routine.sequencer_input = body_sender;
    routine.try_fill().await;
    next_sent(&mut outbound).await;
    while events.try_recv().is_ok() {}

    deliver(&mut routine, BlockSyncMessage::Block(block)).await;
    assert!(bodies.try_recv().is_ok(), "the body is still sequenced");
    let reported = std::iter::from_fn(|| events.try_recv().ok())
        .any(|event| matches!(event, RoutineToReactor::Misbehavior { .. }));
    assert!(
        !reported,
        "a wrong local estimate is not the supplier's fault"
    );
}

#[tokio::test]
async fn a_partial_ending_requeues_only_the_missing_suffix() {
    let first = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let second = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_2_BYTES);
    let (mut routine, mut outbound, pool, connection, _) = requester_routine(first.hash());
    routine.config.max_blocks_per_response = 2;
    routine.max_blocks_per_response = 2;
    routine.work.extend(
        super::super::test_work_scope(),
        [(
            block::Height(2),
            second.hash(),
            BlockSizeEstimate::Confirmed(1000),
        )],
    );
    let (sender, mut bodies) = mpsc::channel(2);
    routine.sequencer_input = sender;
    routine.try_fill().await;
    let sent = next_sent(&mut outbound).await;
    assert!(matches!(
        BlockSyncMessage::decode_frame(sent).unwrap(),
        BlockSyncMessage::GetBlocks { count: 2, .. }
    ));
    deliver(&mut routine, BlockSyncMessage::Block(first)).await;
    assert!(bodies.try_recv().is_ok());
    deliver(
        &mut routine,
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(1),
            returned: 1,
        },
    )
    .await;
    assert!(!routine.work.pending_contains(block::Height(1)));
    assert!(routine.work.pending_contains(block::Height(2)));
    assert_eq!(pool.held(), 0);
    drop(routine);
    assert!(!connection.is_cancelled());
}

#[tokio::test]
async fn local_session_share_leaves_entries_for_another_requester() {
    use super::super::regulated::live_requester::SESSION_RESERVATIONS;

    let pool = ReservationPool::new(2 * SESSION_RESERVATIONS).unwrap();
    let (mut first, _outbound, _, first_connection, _) =
        requester_routine_with_pool(block::Hash([1; 32]), pool.clone());
    let requester = &first.requester;
    for index in 0..SESSION_RESERVATIONS {
        let height = u32::try_from(index + 100).unwrap();
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_le_bytes());
        let writer = requester
            .reserve(
                block::Height(height),
                &[block::Hash(hash)],
                100,
                pool.try_entry().unwrap(),
                requester.open().unwrap(),
            )
            .unwrap();
        assert!(writer.publish(|| {}));
        assert!(writer.try_start(|| true));
    }
    assert_eq!(pool.held(), SESSION_RESERVATIONS);
    first.try_fill().await;
    assert!(first.window.outstanding.is_empty());
    assert!(!first.cancel.is_cancelled());
    assert!(!first_connection.is_cancelled());

    let (mut second, mut outbound, _, second_connection, _) =
        requester_routine_with_pool(block::Hash([2; 32]), pool.clone());
    second.try_fill().await;
    let frame = next_sent(&mut outbound).await;
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame).unwrap(),
        BlockSyncMessage::GetBlocks { .. }
    ));
    assert_eq!(pool.held(), SESSION_RESERVATIONS + 1);
    assert!(!second_connection.is_cancelled());
}

#[tokio::test]
async fn abandoned_height_closes_locally_and_can_be_requested_on_replacement() {
    let hash = block::Hash([1; 32]);
    let (mut routine, mut outbound, pool, connection, _) = requester_routine(hash);
    let work = Arc::clone(&routine.work);
    routine.try_fill().await;
    next_sent(&mut outbound).await;
    assert!(routine.expire_due_timeouts(routine.window.outstanding[0].deadline));
    // Unrelated accepted blocks can disarm the old block-progress watchdog.
    routine
        .window
        .note_block_progress(Instant::now(), routine.config.effective_liveness_timeout());
    assert!(routine.window.block_liveness_deadline.is_none());
    routine.retry_avoid.clear();
    let deadline = routine
        .requester
        .retirement_deadline(routine.config.effective_liveness_timeout())
        .unwrap();
    assert!(routine
        .check_request_liveness(deadline - Duration::from_millis(1))
        .is_ok());
    assert!(
        routine.earliest_deadline_sleep(None).deadline().into_std()
            <= deadline + Duration::from_millis(1)
    );
    assert!(matches!(
        routine.handle_deadlines(deadline).await,
        Err(SinkReject::Local(_))
    ));
    assert!(connection.is_cancelled());
    assert_eq!(
        pool.held(),
        1,
        "closing does not release live authorization early"
    );
    drop(routine);
    assert_eq!(pool.held(), 0);

    let (mut replacement, mut outbound, ..) = requester_routine_with_pool(hash, pool);
    replacement.work = work;
    replacement.try_fill().await;
    let frame = next_sent(&mut outbound).await;
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame).unwrap(),
        BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1
        }
    ));
}

#[tokio::test]
async fn missing_endings_cannot_pin_the_pool_below_each_peers_limit() {
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let pool = ReservationPool::new(3).unwrap();
    let mut peers = Vec::new();
    for _ in 0..3 {
        let (mut routine, mut outbound, _, connection, _) =
            requester_routine_with_pool(block.hash(), pool.clone());
        let (bodies, mut received) = mpsc::channel(1);
        routine.sequencer_input = bodies;
        routine.try_fill().await;
        next_sent(&mut outbound).await;
        deliver(&mut routine, BlockSyncMessage::Block(block.clone())).await;
        assert!(received.try_recv().is_ok());
        assert!(!routine.requester.at_capacity());
        peers.push((routine, outbound, connection));
    }
    assert!(
        pool.try_entry().is_none(),
        "all peers retain an unended exchange"
    );
    for (routine, outbound, connection) in peers {
        let grace = routine.config.effective_liveness_timeout();
        let deadline = routine
            .requester
            .retirement_deadline(grace)
            .expect("the last block starts the missing-ending deadline");
        assert!(routine
            .check_request_liveness(deadline - Duration::from_nanos(1))
            .is_ok());
        assert!(matches!(
            routine.check_request_liveness(deadline),
            Err(SinkReject::Local(_))
        ));
        assert!(connection.is_cancelled());
        drop(routine);
        drop(outbound);
    }
    assert_eq!(
        pool.held(),
        0,
        "retired sessions return authorization capacity"
    );
    assert!(pool.try_entry().is_some(), "another peer can make progress");
}

#[tokio::test(start_paused = true)]
async fn buffered_ending_survives_a_long_local_decode_wait() {
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let (mut routine, mut outbound, pool, connection, _) = requester_routine(block.hash());
    let (bodies, mut received) = mpsc::channel(1);
    let held = bodies.clone().try_reserve_owned().unwrap();
    routine.sequencer_input = bodies;
    routine.try_fill().await;
    outbound.recv().await.unwrap();
    let (input, mut buffered) = crate::zakura::framed_channel(1);
    input
        .send(
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(1),
                returned: 1,
            }
            .encode_frame()
            .unwrap(),
        )
        .await
        .unwrap();
    let mut processing = Box::pin(deliver(&mut routine, BlockSyncMessage::Block(block)));
    assert!(futures::poll!(&mut processing).is_pending());
    time::advance(Duration::from_secs(40)).await;
    drop(held);
    processing.await;
    received.try_recv().unwrap();
    assert_eq!(
        pool.held(),
        1,
        "the ending still owns authorization after all bodies arrive"
    );
    assert!(routine
        .check_request_liveness(time::Instant::now().into_std())
        .is_ok());
    assert!(!connection.is_cancelled());
    routine
        .handle_frame(&mut block_sync_guard(), buffered.recv().await.unwrap())
        .await
        .unwrap();
    assert_eq!(pool.held(), 0);
    assert!(!connection.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn a_live_partially_returned_range_waits_without_retry_spin() {
    let (mut routine, mut outbound, _, connection, _) = requester_routine(block::Hash([1; 32]));
    routine.config.max_blocks_per_response = 2;
    routine.max_blocks_per_response = 2;
    routine.work.extend(
        super::super::test_work_scope(),
        [(
            block::Height(2),
            block::Hash([2; 32]),
            BlockSizeEstimate::Confirmed(1000),
        )],
    );
    routine.try_fill().await;
    outbound.recv().await.unwrap();
    assert_eq!(routine.window.outstanding.len(), 1);
    let owner = routine.window.outstanding[0].request.owner;
    // A central scheduler may return one height while this request still owns
    // the rest. Its wire authorization is live and has not begun retirement.
    let returned = routine
        .work
        .release_reserved_and_return_items_detailed_for_owner(owner, [block::Height(1)]);
    routine.budget.release(returned.released_bytes);
    assert_eq!(returned.returned_count, 1);
    assert_eq!(
        routine
            .requester
            .retirement_deadline(routine.config.effective_liveness_timeout()),
        None
    );
    // Keep the connection eligible for another request. Without prior progress,
    // the initial probe limit masks the retry loop before it touches the queue.
    routine.window.note_block_progress(
        time::Instant::now().into_std(),
        routine.config.effective_liveness_timeout(),
    );
    assert_eq!(routine.try_fill().await, None);
    assert!(routine.work.pending_contains(block::Height(1)));
    let before = routine.work.taken_items_for_test();
    let (_input, recv) = crate::zakura::framed_channel(4);
    routine.recv = recv;
    let (_view, view_rx) = watch::channel(*routine.sequencer_view.borrow());
    routine.sequencer_view = view_rx;
    let mut guard = block_sync_guard();
    let mut running = Box::pin(routine.run_inner(&mut guard));
    for _ in 0..100 {
        assert!(futures::poll!(&mut running).is_pending());
        // Let zero-duration sleeps actually fire. Pure synchronous polling cannot
        // distinguish a timer-driven spin from a legitimately parked future.
        time::advance(Duration::from_millis(1)).await;
    }
    drop(running);
    assert!(routine.work.taken_items_for_test() - before <= 2);
    assert_eq!(routine.window.outstanding.len(), 1);
    assert!(!connection.is_cancelled());
}

#[tokio::test]
async fn a_disappeared_avoidance_timer_still_retries_immediately() {
    let (routine, ..) = requester_routine(block::Hash([1; 32]));
    let now = Instant::now();
    assert_eq!(routine.retry_filter_wake_deadline(now, true), Some(now));
    assert_eq!(routine.retry_filter_wake_deadline(now, false), None);
}

/// Active time before a local pause is preserved by the real routine's deadline loop.
#[tokio::test(start_paused = true)]
async fn a_withheld_ending_retires_after_reading_resumes_from_a_local_pause() {
    time::advance(Duration::from_secs(7)).await;
    let first = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let second = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_2_BYTES);
    let (mut routine, mut outbound, pool, connection, _) = requester_routine(first.hash());
    routine.work.extend(
        super::super::test_work_scope(),
        [(
            block::Height(2),
            second.hash(),
            BlockSizeEstimate::Confirmed(2000),
        )],
    );
    let (bodies, mut received) = mpsc::channel(1);
    routine.sequencer_input = bodies;
    let (input, recv) = crate::zakura::framed_channel(4);
    routine.recv = recv;
    let (_view, view_rx) = watch::channel(*routine.sequencer_view.borrow());
    routine.sequencer_view = view_rx;
    let mut running = Box::pin(routine.run());
    assert!(futures::poll!(&mut running).is_pending());
    outbound.recv().await.unwrap();
    input
        .send(BlockSyncMessage::Block(first).encode_frame().unwrap())
        .await
        .unwrap();
    assert!(futures::poll!(&mut running).is_pending());
    let claimed_at = time::Instant::now();
    // The first range has its body, but no ending. Its clock runs for ten seconds.
    assert_eq!(received.len(), 1);
    outbound.recv().await.unwrap();
    time::advance(Duration::from_secs(10)).await;
    input
        .send(BlockSyncMessage::Block(second).encode_frame().unwrap())
        .await
        .unwrap();
    assert!(futures::poll!(&mut running).is_pending());
    // The second body's local decode slot is blocked behind the first body.
    time::advance(Duration::from_secs(40)).await;
    assert!(futures::poll!(&mut running).is_pending());
    received.try_recv().unwrap();
    assert!(futures::poll!(&mut running).is_pending());
    received.try_recv().unwrap();
    assert!(!connection.is_cancelled());
    time::advance(Duration::from_secs(21)).await;
    assert!(futures::poll!(&mut running).is_pending());
    assert!(!connection.is_cancelled());
    time::advance(Duration::from_secs(1)).await;
    assert_eq!(time::Instant::now() - claimed_at, Duration::from_secs(72));
    // Poll at this instant. Awaiting would let paused time advance to a later deadline.
    assert!(matches!(
        futures::poll!(&mut running),
        std::task::Poll::Ready(Err(SinkReject::Local(_)))
    ));
    assert!(connection.is_cancelled());
    assert_eq!(pool.held(), 0);
}

/// A peer that keeps the receive arm ready cannot postpone its own request timeouts.
#[tokio::test]
async fn a_status_flood_cannot_postpone_request_timeouts() {
    let (mut routine, mut outbound, _pool, _connection, _events) =
        requester_routine(block::Hash([1; 32]));
    routine.try_fill().await;
    next_sent(&mut outbound).await;
    assert_eq!(routine.window.outstanding.len(), 1);
    routine.window.outstanding[0].deadline = Instant::now() - Duration::from_secs(1);
    let (input, recv) = crate::zakura::framed_channel(1024);
    let status = BlockSyncMessage::Status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(10),
        max_blocks_per_response: 1,
        ..BlockSyncStatus::default()
    })
    .encode_frame()
    .unwrap();
    while input.try_send(status.clone()).is_ok() {}
    routine.recv = recv;
    let (_view, view_rx) = watch::channel(*routine.sequencer_view.borrow());
    routine.sequencer_view = view_rx;
    let mut guard = block_sync_guard();
    let mut running = Box::pin(routine.run_inner(&mut guard));
    // One poll handles frames until the cooperative budget runs out, while
    // frames are still queued.
    assert!(futures::poll!(&mut running).is_pending());
    drop(running);
    assert!(
        routine.window.outstanding.is_empty(),
        "the overdue request timed out while frames were ready"
    );
}
