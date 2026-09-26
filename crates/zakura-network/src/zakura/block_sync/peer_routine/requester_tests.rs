use super::super::{regulated::live_requester::LiveRequester, request::BlockSizeEstimate};
use super::*;
use crate::zakura::{
    regulation::{ReservationPool, WriterFence},
    CloseCause,
};
use zakura_chain::serialization::ZcashDeserializeInto;

fn requester_routine() -> (PeerRoutine, FramedRecv, ReservationPool, CancellationToken) {
    let (mut routine, outbound, _events) = super::tests::status_test_routine();
    let pool = ReservationPool::new(2).unwrap();
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
            block::Hash([1; 32]),
            BlockSizeEstimate::Confirmed(1000),
        )],
    );
    (routine, outbound, pool, connection)
}

#[tokio::test]
async fn written_timeout_preserves_authorization_and_blocks_overlap_until_ending() {
    let (mut routine, mut outbound, pool, connection) = requester_routine();
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
    let (mut routine, _outbound, pool, connection) = requester_routine();
    routine.try_fill().await;
    assert_eq!(pool.held(), 1);
    let deadline = routine.window.outstanding[0].deadline;
    assert!(routine.expire_due_timeouts(deadline));
    assert_eq!(pool.held(), 0);
    assert!(!connection.is_cancelled());
}

#[tokio::test]
async fn wrong_header_is_rejected_before_transaction_decoding_or_decode_capacity() {
    let (mut routine, mut outbound, _pool, _connection) = requester_routine();
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
    assert!(format!("{error:?}").contains("next expected header"));
}

#[tokio::test]
async fn invalid_ending_keeps_the_written_exchange_fenced() {
    let (mut routine, mut outbound, pool, connection) = requester_routine();
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
