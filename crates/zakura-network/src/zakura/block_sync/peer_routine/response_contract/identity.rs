//! Reject bodies or endings that do not match this connection's original request.

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
    f.rejects(
        BlockSyncMessage::Block(f.blocks[1].clone()),
        "R04 out of order",
    )
    .await;
}

#[tokio::test]
async fn r04_wrong_hash_must_disconnect_before_handler() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    let mut wrong = (*f.blocks[0]).clone();
    Arc::make_mut(&mut wrong.header).nonce[0] ^= 1;
    f.rejects(
        BlockSyncMessage::Block(Arc::new(wrong)),
        "R04 wrong expected hash",
    )
    .await;
}

#[tokio::test]
async fn r05_duplicate_body_cannot_consume_a_part_twice() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R05 duplicate body",
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
async fn r04_body_beyond_the_authorized_range_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.routine.servable_high = block::Height(200);
    let extra = fake_blocks_in_range(103, 103).pop().unwrap();
    f.rejects(BlockSyncMessage::Block(extra), "R04 body beyond range")
        .await;
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
async fn r05_duplicate_body_remains_invalid_after_local_finality_advances() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.view
        .send_modify(|view| view.download_floor = block::Height(100));
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R05 consumed part below local floor",
    )
    .await;
}
