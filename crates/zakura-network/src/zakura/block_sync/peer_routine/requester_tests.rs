use super::super::{regulated::live_requester::LiveRequester, request::BlockSizeEstimate};
use super::*;
use crate::zakura::{
    regulation::{ReservationPool, WriterFence},
    CloseCause,
};
use zakura_chain::serialization::ZcashDeserializeInto;

fn requester_routine(
    expected_hash: block::Hash,
) -> (PeerRoutine, FramedRecv, ReservationPool, CancellationToken) {
    requester_routine_with_pool(expected_hash, ReservationPool::new(2).unwrap())
}

fn requester_routine_with_pool(
    expected_hash: block::Hash,
    pool: ReservationPool,
) -> (PeerRoutine, FramedRecv, ReservationPool, CancellationToken) {
    let (mut routine, outbound, _events) = super::tests::status_test_routine();
    let connection = CancellationToken::new();
    let fence = WriterFence::new(connection.clone(), CloseCause::default());
    routine.requester = Some(LiveRequester::new(
        pool.clone(),
        fence,
        &routine.recv,
        routine.session.request_sender(),
    ));
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
    (routine, outbound, pool, connection)
}

#[tokio::test]
async fn written_timeout_preserves_authorization_and_blocks_overlap_until_ending() {
    let (mut routine, mut outbound, pool, connection) = requester_routine(block::Hash([1; 32]));
    routine.try_fill().await;
    let request = tokio::time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .unwrap()
        .unwrap();
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
    let retirement = routine
        .requester
        .as_ref()
        .unwrap()
        .retirement_deadline(grace)
        .unwrap();
    routine.retry_avoid.clear();
    routine.window.disarm_liveness_after_progress_if_idle();
    routine.try_fill().await;
    assert!(
        routine.window.outstanding.is_empty(),
        "the old range still authorizes a response"
    );
    let ending = BlockSyncMessage::RangeUnavailable {
        start_height: block::Height(1),
        count: 1,
    }
    .encode_frame()
    .unwrap();
    routine
        .handle_frame(&mut block_sync_guard(), ending.clone())
        .await
        .unwrap();
    assert_eq!(pool.held(), 0);
    assert!(!connection.is_cancelled());
    assert!(routine.check_request_liveness(retirement).is_ok());
    assert!(
        routine
            .handle_frame(&mut block_sync_guard(), ending)
            .await
            .is_err(),
        "a second ending is unsolicited"
    );
}

#[tokio::test]
async fn unwritten_timeout_retracts_authorization_without_closing_connection() {
    let (mut routine, _outbound, pool, connection) = requester_routine(block::Hash([1; 32]));
    routine.try_fill().await;
    assert_eq!(pool.held(), 1);
    let deadline = routine.window.outstanding[0].deadline;
    assert!(routine.expire_due_timeouts(deadline));
    assert_eq!(pool.held(), 0);
    assert!(!connection.is_cancelled());
}

#[tokio::test]
async fn wrong_header_is_rejected_before_transaction_decoding_or_decode_capacity() {
    let (mut routine, mut outbound, _pool, _connection) = requester_routine(block::Hash([1; 32]));
    routine.try_fill().await;
    tokio::time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .unwrap()
        .unwrap();
    let block: block::Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
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
    assert!(error.to_string().contains("next expected header"));
}

#[tokio::test]
async fn invalid_ending_keeps_the_written_exchange_fenced() {
    let (mut routine, mut outbound, pool, connection) = requester_routine(block::Hash([1; 32]));
    routine.try_fill().await;
    tokio::time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .unwrap()
        .unwrap();
    let ending = BlockSyncMessage::BlocksDone {
        start_height: block::Height(1),
        returned: 1,
    }
    .encode_frame()
    .unwrap();
    assert!(routine
        .handle_frame(&mut block_sync_guard(), ending)
        .await
        .is_err());
    assert_eq!(pool.held(), 1);
    drop(routine);
    assert!(
        connection.is_cancelled(),
        "dropping an unanswered written exchange closes its connection"
    );
}

#[tokio::test]
async fn authorized_block_and_exact_ending_complete_the_live_request() {
    let block: block::Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let (mut routine, mut outbound, pool, connection) = requester_routine(block.hash());
    let (body_sender, mut bodies) = mpsc::channel(1);
    routine.sequencer_input = body_sender;
    routine.try_fill().await;
    tokio::time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .unwrap()
        .unwrap();
    let response = BlockSyncMessage::Block(Arc::new(block))
        .encode_frame()
        .unwrap();
    routine
        .handle_frame(&mut block_sync_guard(), response)
        .await
        .unwrap();
    assert!(
        bodies.try_recv().is_ok(),
        "the authorized body reaches the existing sequencer"
    );
    assert_eq!(
        pool.held(),
        1,
        "the ending still owns authorization after all bodies arrive"
    );
    let ending = BlockSyncMessage::BlocksDone {
        start_height: block::Height(1),
        returned: 1,
    }
    .encode_frame()
    .unwrap();
    routine
        .handle_frame(&mut block_sync_guard(), ending)
        .await
        .unwrap();
    assert_eq!(pool.held(), 0);
    drop(routine);
    assert!(
        !connection.is_cancelled(),
        "the completed exchange leaves its connection reusable"
    );
}

#[tokio::test]
async fn local_session_share_leaves_entries_for_another_requester() {
    use super::super::regulated::live_requester::SESSION_RESERVATIONS;

    let pool = ReservationPool::new(2 * SESSION_RESERVATIONS).unwrap();
    let (mut first, _outbound, _, first_connection) =
        requester_routine_with_pool(block::Hash([1; 32]), pool.clone());
    let requester = first.requester.as_ref().unwrap();
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

    let (mut second, mut outbound, _, second_connection) =
        requester_routine_with_pool(block::Hash([2; 32]), pool.clone());
    second.try_fill().await;
    let frame = time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .unwrap()
        .unwrap();
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
    let (mut routine, mut outbound, pool, connection) = requester_routine(hash);
    let work = Arc::clone(&routine.work);
    routine.try_fill().await;
    time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(routine.expire_due_timeouts(routine.window.outstanding[0].deadline));
    // Unrelated accepted blocks can disarm the old block-progress watchdog.
    routine
        .window
        .note_block_progress(Instant::now(), routine.config.effective_liveness_timeout());
    assert!(routine.window.block_liveness_deadline.is_none());
    routine.retry_avoid.clear();
    let deadline = routine
        .requester
        .as_ref()
        .unwrap()
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

    let (mut replacement, mut outbound, _, _) = requester_routine_with_pool(hash, pool);
    replacement.work = work;
    replacement.try_fill().await;
    let frame = time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame).unwrap(),
        BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1
        }
    ));
}
