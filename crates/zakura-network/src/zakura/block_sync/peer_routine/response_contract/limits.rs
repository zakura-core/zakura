//! Enforce actual response bytes before decoding or waiting for handler space.

use super::*;

async fn check_receiver_byte_cap(excess: bool, underestimated: bool) {
    let mut f = Fixture::new(100, 3);
    let actual: u32 = f
        .blocks
        .iter()
        .map(|b| u32::try_from(b.zcash_serialized_size()).unwrap())
        .sum();
    // Estimates affect local scheduling, not the peer's response contract. Small
    // estimates let the request reach the count boundary before the byte bound.
    f.routine.work = Arc::new(WorkQueue::new(block::Height(99)));
    f.routine.work.set_estimate_floor_for_tests(1);
    f.routine.work.extend(
        super::super::test_work_scope(),
        f.blocks.iter().map(|body| {
            (
                body.coinbase_height().unwrap(),
                body.hash(),
                BlockSizeEstimate::Advertised(1),
            )
        }),
    );
    if !underestimated {
        f.routine.config.size_deviation_tolerance = u32::MAX;
    }
    f.routine.max_response_bytes = actual - u32::from(excess);
    f.publish().await;
    for index in 0..2 {
        f.body(index).await;
    }
    if excess {
        f.rejects(
            BlockSyncMessage::Block(f.blocks[2].clone()),
            "R12 cumulative bodies exceed advertised bytes by one",
        )
        .await;
    } else {
        f.body(2).await;
        // Tags and the nine-byte terminal are excluded from the body-byte limit.
        f.deliver(f.done(3)).await.unwrap();
        f.assert_no_peer_fault();
    }
}

#[tokio::test]
async fn r12_cumulative_body_bytes_cannot_exceed_the_advertised_cap() {
    check_receiver_byte_cap(true, false).await;
}

#[tokio::test]
async fn r12_exact_body_byte_limit_excludes_tags_and_terminal() {
    check_receiver_byte_cap(false, false).await;
}

#[tokio::test]
async fn r12_internal_estimates_do_not_create_undeclared_peer_obligations() {
    check_receiver_byte_cap(false, true).await;
}

#[tokio::test]
async fn f04_invalid_body_identity_is_rejected_before_waiting_for_handler_capacity() {
    let mut f = Fixture::new(100, 3);
    let held: Vec<_> = (0..256)
        .map(|_| {
            f.routine
                .sequencer_input
                .clone()
                .try_reserve_owned()
                .unwrap()
        })
        .collect();
    let result = time::timeout(
        Duration::from_millis(100),
        f.routine.handle_frame(
            &mut f.guard,
            BlockSyncMessage::Block(f.blocks[0].clone())
                .encode_frame()
                .unwrap(),
        ),
    )
    .await;
    drop(held);
    assert!(
        matches!(result, Ok(Err(SinkReject::Protocol(_)))),
        "F04: an unauthorized response must not wait on downstream capacity: {result:?}"
    );
}

#[tokio::test]
async fn f04_absent_reservation_is_checked_before_allocating_a_decoded_body() {
    let mut f = Fixture::new(100, 3);
    let probe = zakura_test::execution::ExecutionProbe::new(false, false);
    f.routine.decode_probe = Some(probe.clone());
    let result = f
        .deliver(BlockSyncMessage::Block(f.blocks[0].clone()))
        .await;
    let allocation = probe.snapshot().largest_allocation;
    assert_eq!(
        allocation, 0,
        "F04 no reservation exists to fund a Block decode, result={result:?}"
    );
    assert!(matches!(result, Err(SinkReject::Protocol(_))));
    assert!(f.bodies.is_empty());
}

#[tokio::test]
async fn c06_local_handler_closure_is_not_peer_misconduct() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.bodies.close();
    let result = f
        .deliver(BlockSyncMessage::Block(f.blocks[0].clone()))
        .await;
    assert!(matches!(result, Err(SinkReject::Local(_))), "{result:?}");
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn authorized_body_records_real_decode_allocations() {
    let mut f = Fixture::new(100, 1);
    f.publish().await;
    let probe = zakura_test::execution::ExecutionProbe::new(false, false);
    f.routine.decode_probe = Some(probe.clone());
    f.body(0).await;
    assert!(probe.snapshot().largest_allocation > 0);
}
