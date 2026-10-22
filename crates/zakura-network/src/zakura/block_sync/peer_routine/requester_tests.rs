use super::super::regulated::live_requester::MAX_PENDING_ENDINGS;
use super::super::{request::BlockSizeEstimate, MSG_BS_BLOCK};
use super::*;
use crate::zakura::regulation::ReservationPool;
use crate::zakura::{CwndUnit, MAX_BS_BLOCKS_PER_REQUEST};
use tokio::time;
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
    let deadline = routine.window.outstanding.first().unwrap().deadline;
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
    let deadline = routine.window.outstanding.first().unwrap().deadline;
    assert!(routine.expire_due_timeouts(deadline));
    assert_eq!(pool.held(), 0);
    assert!(!connection.is_cancelled());
}

#[tokio::test]
async fn wrong_header_is_rejected_before_transaction_decoding_or_decode_capacity() {
    let (mut routine, mut outbound, pool, connection, _) = requester_routine(block::Hash([1; 32]));
    routine.try_fill().await;
    assert_eq!(routine.window.outstanding.first().unwrap().request.count, 1);
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
        block::Hash([1; 32]),
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
    let deadline = routine.window.outstanding.first().unwrap().deadline;
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

/// A body that keeps the expected header but not its transactions is junk that
/// can decode far larger than the real block. The routine rejects it before it
/// reaches the sequencer, including the duplicate-transaction Merkle malleation
/// (CVE-2012-2459) that keeps the root.
#[tokio::test]
async fn a_body_whose_transactions_miss_its_header_disconnects_before_sequencing() {
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1046401_BYTES);
    let height = block.coinbase_height().unwrap();
    assert_eq!(
        block.transactions.len() % 2,
        1,
        "an odd count lets the last repeat"
    );

    let mut truncated = (*block).clone();
    truncated.transactions.pop();
    let mut malleated = (*block).clone();
    malleated
        .transactions
        .push(malleated.transactions.last().unwrap().clone());
    assert_eq!(
        malleated
            .transactions
            .iter()
            .collect::<block::merkle::Root>(),
        block.header.merkle_root,
        "the repeated last transaction keeps the Merkle root"
    );

    for junk in [truncated, malleated] {
        let (mut routine, mut outbound, _pool, connection, mut events) =
            requester_routine(block.hash());
        let released = routine.work.reset_above(block::Height(0));
        routine.budget.release(released);
        routine.work.extend(
            super::super::test_work_scope(),
            [(height, block.hash(), BlockSizeEstimate::Confirmed(80_000))],
        );
        routine.servable_high = height;
        let (body_sender, mut bodies) = mpsc::channel(1);
        routine.sequencer_input = body_sender;
        routine.try_fill().await;
        next_sent(&mut outbound).await;
        while events.try_recv().is_ok() {}

        let error = try_deliver(&mut routine, BlockSyncMessage::Block(Arc::new(junk)))
            .await
            .unwrap_err();
        assert!(matches!(error, SinkReject::Protocol(_)), "{error}");
        assert!(
            bodies.try_recv().is_err(),
            "junk never reaches the sequencer"
        );
        let reported = std::iter::from_fn(|| events.try_recv().ok()).any(|event| {
            matches!(
                event,
                RoutineToReactor::Misbehavior {
                    reason: BlockSyncMisbehavior::InvalidBlock,
                    ..
                }
            )
        });
        assert!(reported);
        drop(routine);
        drop(connection);
    }
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
    assert!(routine.expire_due_timeouts(routine.window.outstanding.first().unwrap().deadline));
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
        assert!(!routine.requester.occupancy().at_capacity);
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
    let first = routine.window.outstanding.first_key().unwrap();
    routine
        .window
        .outstanding
        .set_deadline(first, Instant::now() - Duration::from_secs(1));
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

/// Another peer's body ends this routine's written request reservation. Then
/// `drop_height` removes that height from `in_flight`. The routine must withdraw
/// its claim, because the queue no longer tracks the height under its owner.
async fn assert_answered_elsewhere_claim_is_withdrawn(
    drop_height: impl FnOnce(
        &PeerRoutine,
        zakura_header_chain::BodyWorkOwner,
        block::Height,
        block::Hash,
    ) -> u64,
) {
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let height = block.coinbase_height().unwrap();
    let (mut routine, mut outbound, _pool, _connection, _events) = requester_routine(block.hash());
    routine.try_fill().await;
    next_sent(&mut outbound).await;
    let owner = routine.window.outstanding.first().unwrap().request.owner;
    let released = routine
        .work
        .release_answered_elsewhere(owner, height)
        .expect("the request still owns its reserved height");
    routine.budget.release(released);
    assert!(routine
        .registry
        .has_outstanding_request(height, block.hash()));

    let released = drop_height(&routine, owner, height, block.hash());
    routine.budget.release(released);
    routine.gc_obsolete_outstanding();

    assert!(routine.window.outstanding.is_empty());
    assert!(!routine
        .registry
        .has_outstanding_request(height, block.hash()));
}

#[tokio::test]
async fn a_reorg_retires_a_request_another_peer_already_answered() {
    assert_answered_elsewhere_claim_is_withdrawn(|routine, _, _, _| {
        routine.work.reset_above(block::Height(0))
    })
    .await;
}

#[tokio::test]
async fn a_retention_deferral_retires_a_request_another_peer_already_answered() {
    assert_answered_elsewhere_claim_is_withdrawn(|routine, owner, height, hash| {
        assert!(routine
            .work
            .defer_received_for_owner(owner, height, hash, 1, block::Height(0)));
        0
    })
    .await;
}

/// A peer that keeps the receive arm ready cannot postpone the deadline for an
/// ending it withholds after sending every body.
#[tokio::test(start_paused = true)]
async fn a_status_flood_cannot_postpone_a_withheld_ending() {
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let (mut routine, mut outbound, _pool, connection, _events) = requester_routine(block.hash());
    let (bodies, mut received) = mpsc::channel(1);
    routine.sequencer_input = bodies;
    routine.try_fill().await;
    next_sent(&mut outbound).await;
    deliver(&mut routine, BlockSyncMessage::Block(block)).await;
    received.try_recv().unwrap();
    let grace = routine.config.effective_liveness_timeout();
    let retirement = routine
        .requester
        .retirement_deadline(grace)
        .expect("the last block starts the missing-ending deadline");
    // Only the missing-ending deadline is due. The other timers cannot end the routine.
    if let Some(key) = routine.window.outstanding.first_key() {
        routine
            .window
            .outstanding
            .set_deadline(key, retirement + Duration::from_secs(3600));
    }
    routine.window.block_liveness_deadline = None;
    time::advance(retirement.saturating_duration_since(time::Instant::now().into_std())).await;
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
    assert!(matches!(
        futures::poll!(&mut running),
        std::task::Poll::Ready(Err(SinkReject::Local(_)))
    ));
    assert!(connection.is_cancelled());
}

/// A body is refused before it claims anything when no live range expects it,
/// or when it exceeds the response's advertised body bytes.
#[tokio::test]
async fn an_unsolicited_or_overrunning_block_disconnects_before_claim() {
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let body_len = u32::try_from(zakura_test::vectors::BLOCK_MAINNET_1_BYTES.len()).unwrap();

    let (mut routine, _outbound, pool, _connection, _events) = requester_routine(block.hash());
    let (bodies, mut received) = mpsc::channel(1);
    routine.sequencer_input = bodies;
    assert!(matches!(
        try_deliver(&mut routine, BlockSyncMessage::Block(block.clone())).await,
        Err(SinkReject::Protocol(_))
    ));
    assert!(received.try_recv().is_err());
    assert_eq!(pool.held(), 0);
    assert!(routine.work.pending_contains(block::Height(1)));

    // The exact limit is accepted. One byte less overruns it.
    for (limit, accepted) in [(body_len, true), (body_len - 1, false)] {
        let (mut routine, mut outbound, pool, _connection, _events) =
            requester_routine(block.hash());
        let (bodies, mut received) = mpsc::channel(1);
        routine.sequencer_input = bodies;
        routine.max_response_bytes = limit;
        routine.try_fill().await;
        next_sent(&mut outbound).await;
        let result = try_deliver(&mut routine, BlockSyncMessage::Block(block.clone())).await;
        assert_eq!(result.is_ok(), accepted, "{limit}-byte limit: {result:?}");
        assert_eq!(received.try_recv().is_ok(), accepted);
        if !accepted {
            assert!(matches!(result, Err(SinkReject::Protocol(_))));
            assert_eq!(
                routine.requester.unreceived_expected(16),
                [(block::Height(1), block.hash())],
                "the refused body claims nothing"
            );
            assert_eq!(pool.held(), 1);
        }
    }
}

/// The header claim happens before the local body decode slot. When the
/// sequencer is gone, the claim stands: the body is neither re-offered on this
/// connection nor accepted again. The reservation stays held, because it still
/// owes the ending, until the routine drops. The drop then frees the pool entry
/// and returns the height to pending, because the body never reached the sequencer.
#[tokio::test]
async fn a_local_failure_after_a_claim_keeps_the_reservation_consumed() {
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let (mut routine, mut outbound, pool, connection, _events) = requester_routine(block.hash());
    let (bodies, received) = mpsc::channel(1);
    drop(received);
    routine.sequencer_input = bodies;
    routine.try_fill().await;
    next_sent(&mut outbound).await;
    assert!(matches!(
        try_deliver(&mut routine, BlockSyncMessage::Block(block.clone())).await,
        Err(SinkReject::Local(_))
    ));
    assert!(routine.requester.unreceived_expected(16).is_empty());
    assert_eq!(pool.held(), 1, "the ending still owns the reservation");
    assert!(!connection.is_cancelled());
    // The claim advanced past this header, so a resend matches no next header.
    // That closes the connection without a peer fault.
    let resent = try_deliver(&mut routine, BlockSyncMessage::Block(block))
        .await
        .unwrap_err();
    assert!(matches!(resent, SinkReject::Local(_)));
    assert!(resent.to_string().contains("next expected header"));
    assert!(connection.is_cancelled());
    assert_eq!(pool.held(), 1);
    let work = routine.work.clone();
    drop(routine);
    assert_eq!(pool.held(), 0);
    assert!(work.pending_contains(block::Height(1)));
}

/// The header proves which hash arrived. Its coinbase must also name the
/// height that the claim assigned to it.
#[tokio::test]
async fn authorized_block_with_another_coinbase_height_is_a_protocol_violation() {
    // Height 1 expects block 2's header, so the claim succeeds for height 1.
    let block = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_2_BYTES);
    let (mut routine, mut outbound, _pool, _connection, _events) = requester_routine(block.hash());
    let (bodies, mut received) = mpsc::channel(1);
    routine.sequencer_input = bodies;
    routine.try_fill().await;
    next_sent(&mut outbound).await;
    let error = try_deliver(&mut routine, BlockSyncMessage::Block(block))
        .await
        .unwrap_err();
    assert!(matches!(error, SinkReject::Protocol(_)), "{error:?}");
    assert!(error.to_string().contains("coinbase height"));
    assert!(received.try_recv().is_err());
}

/// Our requester asks for one block per `GetBlocks`, whatever an old config or the
/// peer advertises, and keeps several of those requests outstanding at once.
#[tokio::test]
async fn requests_stay_single_block_for_any_advertisement() {
    for peer_max_blocks in [2, MAX_BS_BLOCKS_PER_REQUEST] {
        let (mut routine, mut outbound, _pool, _connection, _) =
            requester_routine_with_pool(block::Hash([1; 32]), ReservationPool::new(8).unwrap());
        // An old config may still carry a multi-block value. It is ignored.
        routine.config.max_blocks_per_response = Some(MAX_BS_BLOCKS_PER_REQUEST);
        assert_eq!(routine.config.bbr_cwnd_unit, CwndUnit::Bytes);
        routine.handle_status(BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(10),
            max_blocks_per_response: peer_max_blocks,
            ..BlockSyncStatus::default()
        });
        routine.work.extend(
            super::super::test_work_scope(),
            (2..=4).map(|height| {
                (
                    block::Height(height),
                    block::Hash([u8::try_from(height).unwrap(); 32]),
                    BlockSizeEstimate::Confirmed(1000),
                )
            }),
        );
        // Lift the cold-start probe limit so the fill is bounded only by the cwnd.
        routine
            .window
            .note_block_progress(Instant::now(), routine.config.effective_liveness_timeout());

        routine.try_fill().await;

        let mut requested = Vec::new();
        while let Ok(frame) = outbound.try_recv() {
            if let BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } = BlockSyncMessage::decode_frame(frame).unwrap()
            {
                requested.push((start_height, count));
            }
        }
        requested.sort();
        assert_eq!(
            requested,
            (1..=4)
                .map(|height| (block::Height(height), 1))
                .collect::<Vec<_>>(),
            "peer advertised {peer_max_blocks} blocks per response",
        );
        assert_eq!(routine.window.outstanding.len(), 4);
    }
}

/// Heights of synthesized work in the pending-ending tests. Well above
/// [`MAX_PENDING_ENDINGS`] so the bound, not the queue, stops a withholding peer.
const BULK_WORK: u32 = 2048;

/// A routine whose peer serves `1..=BULK_WORK` and advertises the protocol's
/// request ceiling, so only local limits bound its work. Also returns the
/// synthesized blocks, indexed by height minus one, and the sequencer input,
/// which holds every body without consuming it.
fn bulk_routine() -> (
    RequesterRoutine,
    Vec<Arc<block::Block>>,
    mpsc::Receiver<SequencedBody>,
) {
    let connection = CancellationToken::new();
    let pool = super::tests::test_request_pool();
    let (mut routine, outbound, events) =
        super::tests::status_test_routine_with(pool.clone(), &connection);
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(BULK_WORK),
        max_blocks_per_response: 1,
        max_inflight_requests: crate::zakura::block_sync::config::MAX_BS_INFLIGHT_REQUESTS,
        ..BlockSyncStatus::default()
    });
    let template = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let blocks: Vec<_> = (1..=BULK_WORK)
        .map(|height| super::super::tests::fake_block_at_height(&template, block::Height(height)))
        .collect();
    routine.work.extend(
        super::super::test_work_scope(),
        blocks.iter().map(|block| {
            let size = block.zcash_serialize_to_vec().unwrap().len();
            (
                block.coinbase_height().unwrap(),
                block.hash(),
                BlockSizeEstimate::Confirmed(u32::try_from(size).unwrap()),
            )
        }),
    );
    // Every body stays queued, so the input must hold all of them.
    let (bodies, received) = mpsc::channel(usize::try_from(BULK_WORK).unwrap());
    routine.sequencer_input = bodies;
    (
        (routine, outbound, pool, connection, events),
        blocks,
        received,
    )
}

/// Drains the routine's outbound frames and returns the heights it requested.
fn requested_heights(outbound: &mut FramedRecv) -> Vec<block::Height> {
    let mut heights = Vec::new();
    while let Ok(frame) = outbound.try_recv() {
        if let BlockSyncMessage::GetBlocks { start_height, .. } =
            BlockSyncMessage::decode_frame(frame).unwrap()
        {
            heights.push(start_height);
        }
    }
    heights
}

/// Plays a peer that sends every requested body, lowest request first, and
/// never an ending. Refills after each body, like the run loop. Stops once the
/// routine has nothing outstanding, and returns every height it requested.
async fn withhold_endings(
    routine: &mut PeerRoutine,
    outbound: &mut FramedRecv,
    blocks: &[Arc<block::Block>],
) -> Vec<block::Height> {
    let mut requested = Vec::new();
    let mut unanswered = std::collections::VecDeque::new();
    let mut unended = 0;
    loop {
        routine.try_fill().await;
        let new = requested_heights(outbound);
        assert!(
            new.is_empty() || unended < MAX_PENDING_ENDINGS,
            "{} new requests went to a peer withholding {unended} endings",
            new.len(),
        );
        requested.extend(&new);
        unanswered.extend(new);
        let Some(height) = unanswered.pop_front() else {
            return requested;
        };
        let index = usize::try_from(height.0 - 1).unwrap();
        deliver(routine, BlockSyncMessage::Block(blocks[index].clone())).await;
        unended += 1;
    }
}

/// A peer that returns every body but withholds every ending stops receiving
/// requests at [`MAX_PENDING_ENDINGS`], long before it holds its session share.
/// The missing-ending deadline still retires the gated connection.
#[tokio::test]
async fn withheld_endings_stop_new_requests_at_the_pending_ending_bound() {
    let ((mut routine, mut outbound, pool, connection, _events), blocks, _bodies) = bulk_routine();
    let requested = withhold_endings(&mut routine, &mut outbound, &blocks).await;

    assert!(requested.len() >= MAX_PENDING_ENDINGS);
    assert!(
        requested.len() < usize::try_from(BULK_WORK).unwrap(),
        "the bound, not the queue, stopped the peer",
    );
    assert_eq!(
        routine.requester.occupancy().pending_endings,
        requested.len()
    );
    assert_eq!(pool.held(), requested.len());
    assert!(routine.window.outstanding.is_empty());
    assert!(routine.work.pending_contains(block::Height(BULK_WORK)));
    routine.try_fill().await;
    assert!(requested_heights(&mut outbound).is_empty());

    // The gate never replaces the missing-ending deadline.
    let deadline = routine
        .requester
        .retirement_deadline(routine.config.effective_liveness_timeout())
        .expect("the first unended body starts the missing-ending deadline");
    assert!(routine
        .check_request_liveness(deadline - Duration::from_nanos(1))
        .is_ok());
    assert!(!connection.is_cancelled());
    assert!(matches!(
        routine.check_request_liveness(deadline),
        Err(SinkReject::Local(_))
    ));
    assert!(connection.is_cancelled());
    drop(routine);
    assert_eq!(pool.held(), 0, "retirement returns every reservation");
}

/// Endings that arrive late reopen the gate as soon as fewer than
/// [`MAX_PENDING_ENDINGS`] remain owed.
#[tokio::test]
async fn a_late_ending_below_the_bound_lets_the_peer_take_work_again() {
    let ((mut routine, mut outbound, pool, connection, _events), blocks, _bodies) = bulk_routine();
    let mut unended = withhold_endings(&mut routine, &mut outbound, &blocks).await;
    unended.sort();

    let mut requested = Vec::new();
    for start_height in unended {
        deliver(
            &mut routine,
            BlockSyncMessage::BlocksDone {
                start_height,
                returned: 1,
            },
        )
        .await;
        routine.try_fill().await;
        requested = requested_heights(&mut outbound);
        let pending = routine.requester.occupancy().pending_endings;
        if pending >= MAX_PENDING_ENDINGS {
            assert!(requested.is_empty(), "gated with {pending} endings owed");
        } else {
            assert_eq!(pending, MAX_PENDING_ENDINGS - 1);
            break;
        }
    }
    assert!(!requested.is_empty(), "the reopened gate issues new work");
    assert!(!connection.is_cancelled());
    assert_eq!(
        pool.held(),
        MAX_PENDING_ENDINGS - 1 + requested.len(),
        "only owed endings and the new requests hold reservations",
    );
}

/// An honest peer sends each ending right after its body, but ready-order
/// serving can interleave several responses. Bodies arriving in batches of
/// 12 (one per serving job), with their endings in reverse order, never reach
/// the bound, so the peer takes all the work.
#[tokio::test]
async fn interleaved_honest_endings_never_reach_the_pending_ending_bound() {
    const INTERLEAVED: usize = 12;
    let ((mut routine, mut outbound, pool, connection, _events), blocks, _bodies) = bulk_routine();
    let mut unanswered = std::collections::VecDeque::new();
    let mut requested = 0;
    let mut most_pending = 0;
    loop {
        routine.try_fill().await;
        let new = requested_heights(&mut outbound);
        requested += new.len();
        unanswered.extend(new);
        if unanswered.is_empty() {
            break;
        }
        let batch: Vec<_> = unanswered
            .drain(..INTERLEAVED.min(unanswered.len()))
            .collect();
        for height in &batch {
            let index = usize::try_from(height.0 - 1).unwrap();
            deliver(&mut routine, BlockSyncMessage::Block(blocks[index].clone())).await;
            most_pending = most_pending.max(routine.requester.occupancy().pending_endings);
            routine.try_fill().await;
            let new = requested_heights(&mut outbound);
            requested += new.len();
            unanswered.extend(new);
        }
        for start_height in batch.into_iter().rev() {
            deliver(
                &mut routine,
                BlockSyncMessage::BlocksDone {
                    start_height,
                    returned: 1,
                },
            )
            .await;
            routine.try_fill().await;
            let new = requested_heights(&mut outbound);
            requested += new.len();
            unanswered.extend(new);
        }
    }
    assert_eq!(requested, usize::try_from(BULK_WORK).unwrap());
    assert_eq!(most_pending, INTERLEAVED);
    assert_eq!(routine.requester.occupancy().pending_endings, 0);
    assert_eq!(pool.held(), 0);
    assert!(!connection.is_cancelled());
}

/// A second peer that shares `fast`'s registry and work, reserves from `pool`
/// and serves `1..=BULK_WORK`. It has no RTprop, which ranks below any
/// measured peer, and its download floor sits just below `floor`, so `floor`
/// is the height its floor arm contests.
fn slower_floor_peer(
    fast: &PeerRoutine,
    pool: ReservationPool,
    floor: block::Height,
) -> (PeerRoutine, FramedRecv) {
    let (mut slow, outbound, _events) =
        super::tests::status_test_routine_with(pool, &CancellationToken::new());
    slow.peer = ZakuraPeerId::new(vec![9; 32]).unwrap();
    slow.registry = Arc::clone(&fast.registry);
    slow.generation = slow
        .registry
        .admit_session(
            &slow.peer,
            slow.session.direction(),
            &slow.config,
            0,
            Instant::now(),
        )
        .generation();
    slow.work = Arc::clone(&fast.work);
    let mut view = *slow.sequencer_view.borrow();
    view.download_floor = block::Height(floor.0 - 1);
    // Fill only borrows the view, so its sender need not outlive this setup.
    slow.sequencer_view = watch::channel(view).1;
    slow.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(BULK_WORK),
        max_blocks_per_response: 1,
        max_inflight_requests: crate::zakura::block_sync::config::MAX_BS_INFLIGHT_REQUESTS,
        ..BlockSyncStatus::default()
    });
    (slow, outbound)
}

/// A fast peer gated on owed endings publishes no free slots, so a slower peer
/// takes the floor height instead of deferring it to a peer that cannot
/// request it. Once the endings arrive, the fast peer is the preferred floor
/// server again and deferred routines are woken to re-rank.
#[tokio::test]
async fn a_peer_gated_on_endings_does_not_hold_the_floor_from_a_slower_peer() {
    let ((mut fast, mut fast_outbound, pool, _fast_connection, _), blocks, _fast_bodies) =
        bulk_routine();
    let mut unended = withhold_endings(&mut fast, &mut fast_outbound, &blocks).await;
    assert!(
        fast.window.bbr_rtprop_ms(Instant::now()).is_some(),
        "the fast peer has a measured RTprop"
    );
    let registry = Arc::clone(&fast.registry);
    let floor = fast
        .work
        .first_pending_in_range(block::Height(1), block::Height(BULK_WORK))
        .expect("the gated peer left work behind");

    let (mut slow, mut slow_outbound) = slower_floor_peer(&fast, pool, floor);

    slow.try_fill().await;
    assert_eq!(
        requested_heights(&mut slow_outbound),
        [floor],
        "the slower peer takes the floor from the gated one",
    );

    // Endings down to the bound keep the fast peer gated, and the next one
    // reopens it. Only that change wakes routines that wait on the ranking.
    let ranking = registry.subscribe_floor_ranking().notified();
    tokio::pin!(ranking);
    ranking.as_mut().enable();
    unended.sort();
    let mut endings = unended.into_iter();
    while fast.requester.occupancy().pending_endings > MAX_PENDING_ENDINGS {
        let start_height = endings.next().unwrap();
        deliver(
            &mut fast,
            BlockSyncMessage::BlocksDone {
                start_height,
                returned: 1,
            },
        )
        .await;
    }
    assert!(!registry.floor_has_preferred_unsaturated_server(floor, &slow.peer, None, false));
    assert!(futures::poll!(ranking.as_mut()).is_pending());
    deliver(
        &mut fast,
        BlockSyncMessage::BlocksDone {
            start_height: endings.next().unwrap(),
            returned: 1,
        },
    )
    .await;
    assert!(registry.floor_has_preferred_unsaturated_server(floor, &slow.peer, None, false));
    assert!(futures::poll!(ranking.as_mut()).is_ready());
}

/// Two sessions that open every request their window allows and then withhold
/// every ending hold at most a quarter of the node pool together, so a third
/// session can still reserve.
#[tokio::test]
async fn warmed_sessions_withholding_endings_leave_the_pool_to_others() {
    use super::super::config::MAX_BS_INFLIGHT_REQUESTS;
    use super::super::regulated::live_requester::SESSION_RESERVATIONS;

    // Wider than the session share used to be, so only the share stops each window.
    const WINDOW: u32 = MAX_BS_INFLIGHT_REQUESTS / 2 + 64;
    let pool = super::tests::test_request_pool();
    let mut sessions = Vec::new();
    for _ in 0..2 {
        let mut probe = super::scaling_tests::probe_in(WINDOW, pool.clone());
        loop {
            probe.routine.try_fill().await;
            if requested_heights(&mut probe.outbound).is_empty() {
                break;
            }
        }
        // Answered bodies leave the window but keep their reservations.
        for block in probe.blocks.iter().take(MAX_PENDING_ENDINGS) {
            deliver(&mut probe.routine, BlockSyncMessage::Block(block.clone())).await;
        }
        probe.routine.try_fill().await;
        assert!(requested_heights(&mut probe.outbound).is_empty());
        assert_eq!(
            probe.routine.requester.occupancy().len,
            SESSION_RESERVATIONS
        );
        sessions.push(probe);
    }
    // The protocol ceiling fits usize on supported targets.
    let node_pool = MAX_BS_INFLIGHT_REQUESTS as usize;
    assert!(
        pool.held() <= node_pool / 4,
        "two sessions hold {} of {node_pool} entries",
        pool.held(),
    );

    let (mut third, mut outbound, ..) =
        requester_routine_with_pool(block::Hash([1; 32]), pool.clone());
    third.try_fill().await;
    assert!(matches!(
        BlockSyncMessage::decode_frame(next_sent(&mut outbound).await).unwrap(),
        BlockSyncMessage::GetBlocks { .. }
    ));
}

/// A `bulk_routine` status that advertises `max_inflight_requests`.
fn inflight_limit(max_inflight_requests: u32) -> BlockSyncStatus {
    BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(BULK_WORK),
        max_blocks_per_response: 1,
        max_inflight_requests,
        ..BlockSyncStatus::default()
    }
}

/// A measured fast peer that advertises 8 in-flight requests and holds one
/// exchange whose ending it withholds, and a slower peer from
/// [`slower_floor_peer`] that defers the returned floor height to it. The last
/// value keeps the fast peer's channels open.
async fn fast_peer_holding_one_exchange() -> (
    PeerRoutine,
    PeerRoutine,
    FramedRecv,
    block::Height,
    Box<dyn std::any::Any>,
) {
    let ((mut fast, mut fast_outbound, pool, connection, events), blocks, bodies) = bulk_routine();
    fast.handle_status(inflight_limit(8));
    // The cold-start probe sends one request. Its body arrives, its ending does not.
    fast.try_fill().await;
    let [first] = requested_heights(&mut fast_outbound)[..] else {
        panic!("the cold-start probe sends one request");
    };
    let index = usize::try_from(first.0 - 1).unwrap();
    deliver(&mut fast, BlockSyncMessage::Block(blocks[index].clone())).await;
    assert!(fast
        .window
        .bbr_rtprop_ms(time::Instant::now().into_std())
        .is_some());
    assert_eq!(fast.requester.occupancy().len, 1);
    let floor = fast
        .work
        .first_pending_in_range(block::Height(1), block::Height(BULK_WORK))
        .unwrap();
    let (mut slow, mut slow_outbound) = slower_floor_peer(&fast, pool, floor);

    // With room under its limit, the faster peer is the preferred floor server.
    slow.try_fill().await;
    assert!(requested_heights(&mut slow_outbound).is_empty());
    let keep = Box::new((fast_outbound, connection, events, bodies));
    (fast, slow, slow_outbound, floor, keep)
}

/// A status that lowers the advertised in-flight limit to what a fast peer
/// already holds stops its requester, so a slower peer takes the floor at
/// once instead of deferring until the next slot heartbeat. The status here
/// is rate-limited, which still applies the new limit. Time is paused so the
/// status always falls inside the rate window, however slow the runner.
#[tokio::test(start_paused = true)]
async fn a_lowered_inflight_limit_releases_the_floor_to_a_slower_peer() {
    let (mut fast, mut slow, mut slow_outbound, floor, _keep) =
        fast_peer_holding_one_exchange().await;
    let registry = Arc::clone(&fast.registry);
    let ranking = registry.subscribe_floor_ranking().notified();
    tokio::pin!(ranking);
    ranking.as_mut().enable();
    fast.handle_status(inflight_limit(1));
    assert!(fast.pending_status.is_some(), "the status was rate-limited");
    assert!(futures::poll!(ranking.as_mut()).is_ready());
    slow.try_fill().await;
    assert_eq!(
        requested_heights(&mut slow_outbound),
        [floor],
        "the slower peer takes the floor once the fast one refuses work",
    );
}

/// A peer that alternates its in-flight limit between 1 and 2 through
/// rate-limited statuses publishes its stop once. Reopening waits for a status
/// the rate limiter admits, so the flood wakes routines waiting on the floor
/// ranking once, not on every frame.
#[tokio::test(start_paused = true)]
async fn alternating_rate_limited_limits_publish_the_stop_once() {
    let (mut fast, slow, _slow_outbound, floor, _keep) = fast_peer_holding_one_exchange().await;
    let registry = Arc::clone(&fast.registry);
    let mut notifications = 0;
    for round in 0..16 {
        let ranking = registry.subscribe_floor_ranking().notified();
        tokio::pin!(ranking);
        ranking.as_mut().enable();
        fast.handle_status(inflight_limit(if round % 2 == 0 { 1 } else { 2 }));
        assert!(
            fast.pending_status.is_some(),
            "every status is rate-limited"
        );
        if futures::poll!(ranking.as_mut()).is_ready() {
            notifications += 1;
        }
    }
    assert_eq!(notifications, 1, "only the first stop is published");
    assert!(!registry.floor_has_preferred_unsaturated_server(floor, &slow.peer, None, false));

    // The next admitted status publishes the reopened requester.
    let ranking = registry.subscribe_floor_ranking().notified();
    tokio::pin!(ranking);
    ranking.as_mut().enable();
    time::advance(Duration::from_secs(1)).await;
    fast.flush_pending_status();
    assert!(fast.pending_status.is_none());
    assert!(futures::poll!(ranking.as_mut()).is_ready());
    assert!(registry.floor_has_preferred_unsaturated_server(floor, &slow.peer, None, false));
}
