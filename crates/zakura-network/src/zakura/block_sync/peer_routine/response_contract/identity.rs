//! Bodies or endings that do not match this connection's original request:
//! unauthorized ones are rejected, fork answers are only discarded.

use super::*;

#[tokio::test]
async fn r01_published_request_accepts_an_immediate_response() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.assert_live(1, "R01");
}

#[tokio::test]
async fn r06_done_count_must_equal_consumed_prefix() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.rejects(f.done(3), "R06 returned=3 after one body").await;
}

#[tokio::test]
async fn r06_done_before_any_body_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.rejects(f.done(1), "R06 terminal before a body").await;
}

#[tokio::test]
async fn r06_done_cannot_close_a_different_start() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.rejects(
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(101),
            returned: 1,
        },
        "R06 wrong range identity",
    )
    .await;
}

#[tokio::test]
async fn r07_unavailable_count_must_equal_original_request() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.rejects(f.unavailable(2), "R07 wrong unavailable count")
        .await;
}

#[tokio::test]
async fn r07_unavailable_after_a_body_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.rejects(f.unavailable(3), "R07 unavailable after body")
        .await;
}

#[tokio::test]
async fn r07_unavailable_without_a_request_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.rejects(f.unavailable(3), "R07 no live request").await;
}

#[tokio::test]
async fn r07_legal_unavailable_requeues_only_still_needed_heights() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.view.send_modify(|view| {
        view.download_floor = block::Height(100);
        view.verified_tip = block::Height(100);
        view.finalized = block::Height(100);
    });
    f.deliver(f.unavailable(3)).await.unwrap();
    f.assert_no_peer_fault();
    assert!(!f.routine.work.pending_contains(block::Height(100)));
    assert!(f.routine.work.pending_contains(block::Height(101)));
    assert!(f.routine.work.pending_contains(block::Height(102)));
}

#[tokio::test]
async fn r08_last_body_does_not_consume_the_terminal() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    for index in 0..3 {
        f.body(index).await;
    }
    f.assert_live(3, "R08");
    f.deliver(f.done(3)).await.unwrap();
    assert!(f.routine.window.outstanding.is_empty());
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn r08_duplicate_terminal_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.deliver(f.unavailable(3)).await.unwrap();
    f.rejects(f.unavailable(3), "R08 duplicate unavailable")
        .await;
}

#[tokio::test]
async fn r08_different_second_terminal_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.deliver(f.done(1)).await.unwrap();
    f.rejects(f.unavailable(3), "R08 second terminal of another kind")
        .await;
}

#[tokio::test]
async fn r04_later_correct_hash_is_not_the_next_expected_part() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    // Block 101 arrives first: not the next part, so it spends a part and is dropped.
    f.discards(
        BlockSyncMessage::Block(f.blocks[1].clone()),
        1,
        "R04 out of order",
    )
    .await;
    // Block 100 now arrives at position two, whose expected hash is block 101's.
    f.discards(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        2,
        "R04 out of order",
    )
    .await;
    f.assert_live(0, "R04 discarded parts are not received parts");
}

#[tokio::test]
async fn r04_wrong_hash_inside_the_range_is_consumed_and_discarded() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    let mut wrong = (*f.blocks[0]).clone();
    Arc::make_mut(&mut wrong.header).nonce[0] ^= 1;
    f.discards(
        BlockSyncMessage::Block(Arc::new(wrong)),
        1,
        "R04 wrong expected hash is a fork answer, not misconduct",
    )
    .await;
    // The range ends by the peer's own count and every height returns for another peer.
    f.deliver(f.done(1)).await.unwrap();
    assert!(
        f.routine.window.outstanding.is_empty(),
        "R06 the ending retires the exchange"
    );
    assert_eq!(
        f.routine.work.pending_len(),
        3,
        "R04 discarded heights are still needed"
    );
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn r04_a_discarded_body_buys_write_grace_not_a_full_liveness_interval() {
    // A zero congestion window seals a useless peer and hands eviction to the
    // liveness timer. Renewing a full interval per discarded part would let a peer
    // spend credit banked before it was sealed to stay connected for hours.
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    let before = f
        .routine
        .window
        .block_liveness_deadline
        .expect("publishing a request arms the liveness deadline");

    let mut wrong = (*f.blocks[0]).clone();
    Arc::make_mut(&mut wrong.header).nonce[0] ^= 1;
    f.discards(
        BlockSyncMessage::Block(Arc::new(wrong)),
        1,
        "R04 a fork answer earns write grace",
    )
    .await;

    let after = f
        .routine
        .window
        .block_liveness_deadline
        .expect("the deadline stays armed");
    let granted = after.saturating_duration_since(std::time::Instant::now());
    assert!(
        granted <= f.routine.config.request_timeout,
        "R04 a discard grants one request timeout, got {granted:?}"
    );
    assert!(
        granted < f.routine.config.effective_liveness_timeout(),
        "R04 a discard must not renew the full liveness interval"
    );
    assert!(
        after >= before || granted > Duration::ZERO,
        "the deadline is live"
    );
}

#[tokio::test]
async fn r04_a_discarded_body_does_not_prove_this_peer_supplies_blocks() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    let stalled_requests = f.routine.window.requests_without_block_progress;
    let mut wrong = (*f.blocks[0]).clone();
    Arc::make_mut(&mut wrong.header).nonce[0] ^= 1;
    f.discards(
        BlockSyncMessage::Block(Arc::new(wrong)),
        1,
        "R04 a fork answer is responsive, not useful",
    )
    .await;
    assert!(
        !f.routine.window.has_block_progress(),
        "R04 a discarded body must not lift the unproven-peer request cap"
    );
    assert_eq!(
        f.routine.window.requests_without_block_progress, stalled_requests,
        "R04 a discarded body must not clear the no-progress request count"
    );
}

#[tokio::test]
async fn r05_duplicate_body_spends_a_part_without_a_second_delivery() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.discards(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        2,
        "R05 duplicate body",
    )
    .await;
    f.assert_live(1, "R05 the duplicate is not a second received part");
}

#[tokio::test]
async fn r12_a_body_after_the_last_part_has_no_started_response_and_disconnects() {
    let mut f = Fixture::new(100, 1);
    f.publish().await;
    f.body(0).await;
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R12 the only range is fully consumed, so no response is still started",
    )
    .await;
}

#[tokio::test]
async fn r05_body_after_terminal_has_no_authorization() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.deliver(f.done(1)).await.unwrap();
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R05 consumed exchange",
    )
    .await;
}

#[tokio::test]
async fn r03_needed_height_does_not_authorize_an_unsolicited_body() {
    let mut f = Fixture::new(100, 3);
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R03 needed is not authorized",
    )
    .await;
}

#[tokio::test]
async fn r03_servable_height_does_not_authorize_an_unsolicited_body() {
    let mut f = Fixture::new(100, 3);
    f.routine.work.reset_above(block::Height(99));
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R03 advertised is not authorized",
    )
    .await;
}

#[tokio::test]
async fn r03_another_peers_request_does_not_authorize_this_peer() {
    let mut a = Fixture::for_peer(100, 3, 1);
    a.publish().await;
    let mut b = Fixture::for_peer(100, 3, 2);
    b.routine.work = a.routine.work.clone();
    b.routine.registry = a.routine.registry.clone();
    b.rejects(
        BlockSyncMessage::Block(b.blocks[0].clone()),
        "R03 another peer owns the request",
    )
    .await;
}

#[tokio::test]
async fn r03_local_floor_does_not_authorize_an_unsolicited_body() {
    let mut f = Fixture::new(100, 3);
    f.view
        .send_modify(|view| view.download_floor = block::Height(102));
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R03 obsolete is not authorized",
    )
    .await;
}

#[tokio::test]
async fn r04_body_beyond_the_authorized_range_spends_a_part_and_is_dropped() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.routine.servable_high = block::Height(200);
    let extra = fake_blocks_in_range(103, 103).pop().unwrap();
    f.discards(
        BlockSyncMessage::Block(extra),
        1,
        "R04 body beyond range is a wrong answer at position one, not a fault",
    )
    .await;
    f.assert_live(0, "R04 a body outside the range is never a received part");
}

#[tokio::test]
async fn r05_separately_authorized_peers_can_deliver_the_same_block() {
    let mut a = Fixture::for_peer(100, 3, 1);
    let mut b = Fixture::for_peer(100, 3, 2);
    a.publish().await;
    b.publish().await;
    assert_eq!(a.blocks[0].hash(), b.blocks[0].hash());
    a.body(0).await;
    b.body(0).await;
    a.deliver(a.done(1)).await.unwrap();
    b.deliver(b.done(1)).await.unwrap();
    a.assert_no_peer_fault();
    b.assert_no_peer_fault();
}

#[tokio::test]
async fn r08_done_cannot_be_consumed_twice_after_a_full_response() {
    let mut f = Fixture::new(100, 1);
    f.publish().await;
    f.body(0).await;
    f.deliver(f.done(1)).await.unwrap();
    f.rejects(f.done(1), "R08 duplicate Done after complete body prefix")
        .await;
}

#[tokio::test]
async fn r07_unavailable_cannot_close_another_start() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.rejects(
        BlockSyncMessage::RangeUnavailable {
            start_height: block::Height(101),
            count: 3,
        },
        "R07 wrong request identity",
    )
    .await;
}

#[tokio::test]
async fn r05_duplicate_body_spends_the_same_part_after_local_finality_advances() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.view
        .send_modify(|view| view.download_floor = block::Height(100));
    f.discards(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        2,
        "R05 a consumed part below the local floor keeps its wire outcome",
    )
    .await;
    f.assert_live(1, "R05 the duplicate is not a second received part");
}
