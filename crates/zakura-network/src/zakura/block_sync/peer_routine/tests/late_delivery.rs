use super::super::super::work_queue::RequestWrite;
use super::*;
use std::num::NonZeroU64;
use zakura_chain::serialization::ZcashDeserializeInto;

async fn unmatched_delivery_reliability(timed_out: bool, delivered_height: u32) -> (u64, u64) {
    let first: Arc<block::Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    let second: Arc<block::Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    let (mut routine, mut outbound, _events) = status_test_routine();
    let (input, mut received) = mpsc::channel(16);
    routine.sequencer_input = input;
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(1),
        ..BlockSyncStatus::default()
    });
    let scope = super::super::super::test_work_scope();
    routine.work.extend(
        scope,
        [
            (block::Height(1), first.hash(), BlockSizeEstimate::Unknown),
            (block::Height(2), second.hash(), BlockSizeEstimate::Unknown),
        ],
    );
    if timed_out {
        routine.try_fill().await;
        tokio::time::timeout(Duration::from_secs(1), outbound.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(routine.window.outstanding.len(), 1);
        let deadline = routine.window.outstanding[0].deadline;
        assert!(routine.expire_due_timeouts(deadline));
    } else {
        // An unrelated past failure must not be refunded by unsolicited work.
        routine.window.record_timeout(1);
    }
    routine.servable_high = block::Height(2);
    let before = routine.window.bbr_reliability_permille();
    let height = block::Height(delivered_height);
    let request = NonZeroU64::new(77).unwrap();
    let items = routine
        .work
        .take_for_request(height, height, 1, u64::MAX, 99, request);
    assert_eq!(items.len(), 1);
    let claim = RequestWrite::new(
        scope.bind(99, request),
        items,
        Arc::clone(&routine.work),
        routine.budget.clone(),
        CancellationToken::new(),
    );
    assert!(claim.publish(|| {}));
    let body = if delivered_height == 1 { first } else { second };
    routine.handle_body(body, Some(1024), None, None).await;
    let delivered = received
        .try_recv()
        .expect("the accepted body reaches the sequencer");
    assert_eq!(delivered.height, height);
    (before, routine.window.bbr_reliability_permille())
}

#[tokio::test(start_paused = true)]
async fn unmatched_body_without_own_timeout_does_not_refund_reliability() {
    let (before, after) = unmatched_delivery_reliability(false, 1).await;
    assert_eq!(after, before);
}

#[tokio::test(start_paused = true)]
async fn unmatched_body_for_another_height_does_not_refund_reliability() {
    let (before, after) = unmatched_delivery_reliability(true, 2).await;
    assert_eq!(after, before);
}

#[tokio::test(start_paused = true)]
async fn unmatched_body_for_own_timed_out_request_refunds_reliability() {
    let (before, after) = unmatched_delivery_reliability(true, 1).await;
    assert!(after > before);
}
