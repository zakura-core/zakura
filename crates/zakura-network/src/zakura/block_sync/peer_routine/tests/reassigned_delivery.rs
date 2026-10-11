use super::super::super::work_queue::RequestWrite;
use super::*;
use zakura_chain::serialization::ZcashDeserializeInto;

#[tokio::test(start_paused = true)]
async fn matched_late_body_settles_the_reassigned_active_owner() {
    let body: Arc<block::Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    let (mut routine, mut outbound, _events) = status_test_routine();
    let (tx, mut received) = mpsc::channel(16);
    routine.sequencer_input = tx;
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(1),
        ..BlockSyncStatus::default()
    });
    let scope = super::super::super::test_work_scope();
    routine.work.extend(
        scope,
        [(block::Height(1), body.hash(), BlockSizeEstimate::Unknown)],
    );
    routine.try_fill().await;
    outbound.recv().await.unwrap();
    let old_owner = routine.window.outstanding[0].request.owner;
    // The watchdog returns the reservation while the old routine still has its window.
    let returned = routine
        .work
        .release_reserved_and_return_items_detailed_for_owner(old_owner, [block::Height(1)]);
    routine.budget.release(returned.released_bytes);
    let request = std::num::NonZeroU64::new(77).unwrap();
    let new_owner = scope.bind(99, request);
    let items =
        routine
            .work
            .take_for_request(block::Height(1), block::Height(1), 1, u64::MAX, 99, request);
    assert_eq!(items.len(), 1);
    let claim = RequestWrite::new(
        new_owner,
        items,
        Arc::clone(&routine.work),
        routine.budget.clone(),
        CancellationToken::new(),
    );
    assert!(claim.publish(|| {}));
    routine.handle_body(body, Some(1024), None, None).await;
    let delivered = received
        .try_recv()
        .expect("a matching late body must settle the current active reservation");
    assert_eq!(delivered.owner, new_owner);
    assert_eq!(routine.work.reserved_bytes(), 0);
}

#[tokio::test(start_paused = true)]
async fn competing_receipt_of_a_written_request_does_not_charge_timeout() {
    let (mut routine, mut outbound, _events) = status_test_routine();
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(1),
        ..BlockSyncStatus::default()
    });
    routine.work.extend(
        super::super::super::test_work_scope(),
        [(
            block::Height(1),
            block::Hash([1; 32]),
            BlockSizeEstimate::Unknown,
        )],
    );
    routine.try_fill().await;
    outbound.recv().await.unwrap();
    let owner = routine.window.outstanding[0].request.owner;
    let deadline = routine.window.outstanding[0].deadline;
    let before = routine.window.bbr_reliability_permille();
    routine.budget.release(
        routine
            .work
            .receive_body_for_owner(owner, block::Height(1), 1024)
            .unwrap(),
    );
    // Ownership stays live for the sequencer, but the peer no longer owes a body.
    assert_eq!(routine.work.owner_for_height(block::Height(1)), Some(owner));
    routine.expire_due_timeouts(deadline);
    assert_eq!(
        routine.window.bbr_reliability_permille(),
        before,
        "displaced reservations must not charge peer failures"
    );
    assert!(routine.window.outstanding.is_empty());
}

#[tokio::test(start_paused = true)]
async fn current_reservation_wins_even_if_registry_cleanup_started() {
    let body: Arc<block::Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    let (mut routine, mut outbound, _events) = status_test_routine();
    let (tx, mut received) = mpsc::channel(16);
    routine.sequencer_input = tx;
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(1),
        ..BlockSyncStatus::default()
    });
    routine.work.extend(
        super::super::super::test_work_scope(),
        [(block::Height(1), body.hash(), BlockSizeEstimate::Unknown)],
    );
    routine.try_fill().await;
    outbound.recv().await.unwrap();
    let owner = routine.window.outstanding[0].request.owner;
    routine
        .registry
        .clear_outstanding(&routine.peer, routine.generation);
    assert!(routine.work.has_reserved_owner(owner, block::Height(1)));
    routine.handle_body(body, Some(1024), None, None).await;
    assert_eq!(
        received
            .try_recv()
            .expect("registry metadata cannot veto a still-active queue owner")
            .owner,
        owner
    );
    assert_eq!(routine.work.reserved_bytes(), 0);
}
#[tokio::test(start_paused = true)]
async fn matched_body_from_retired_epoch_cannot_settle_a_replacement_claim() {
    let body: Arc<block::Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    let (mut routine, mut outbound, _events) = status_test_routine();
    let (tx, mut received) = mpsc::channel(16);
    routine.sequencer_input = tx;
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(1),
        ..BlockSyncStatus::default()
    });
    let scope = super::super::super::test_work_scope();
    routine.work.extend(
        scope,
        [(block::Height(1), body.hash(), BlockSizeEstimate::Unknown)],
    );
    routine.try_fill().await;
    outbound.recv().await.unwrap();
    let old_owner = routine.window.outstanding[0].request.owner;
    // The watchdog returns the reservation while the old routine still has its window.
    let returned = routine
        .work
        .release_reserved_and_return_items_detailed_for_owner(old_owner, [block::Height(1)]);
    routine.budget.release(returned.released_bytes);
    let request = std::num::NonZeroU64::new(77).unwrap();
    let mut refreshed = scope;
    refreshed.body_work_epoch = scope.body_work_epoch.checked_next().unwrap();
    routine.work.refresh_authority(refreshed);
    let new_owner = refreshed.bind(old_owner.session_id, request);
    let items = routine.work.take_for_request(
        block::Height(1),
        block::Height(1),
        1,
        u64::MAX,
        old_owner.session_id,
        request,
    );
    assert_eq!(items.len(), 1);
    let claim = RequestWrite::new(
        new_owner,
        items,
        Arc::clone(&routine.work),
        routine.budget.clone(),
        CancellationToken::new(),
    );
    assert!(claim.publish(|| {}));
    routine.handle_body(body, Some(1024), None, None).await;
    assert!(
        received.try_recv().is_err(),
        "a retired epoch cannot adopt a replacement claim"
    );
    assert_eq!(
        routine.work.owner_for_height(block::Height(1)),
        Some(new_owner)
    );
    assert_eq!(routine.work.reserved_bytes(), block::MAX_BLOCK_BYTES);
}
