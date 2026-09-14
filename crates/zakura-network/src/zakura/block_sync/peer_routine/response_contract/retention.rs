//! A legal response still needs room in the local block backlog.
//!
//! These bodies are above the checkpoint window, whose fixed exception lets
//! verification make progress even when the ordinary backlog is full.

use super::*;

fn underestimated(count: u32, estimate: u32) -> Fixture {
    let mut f = Fixture::new(1_000, count);
    f.view.send_modify(|view| {
        view.verified_tip = block::Height(0);
        view.finalized = block::Height(0);
    });
    f.routine.work = Arc::new(WorkQueue::new(block::Height(999)));
    f.routine.work.set_estimate_floor_for_tests(1);
    f.routine.work.extend(
        crate::zakura::block_sync::test_work_scope(),
        f.blocks.iter().map(|body| {
            (
                body.coinbase_height().unwrap(),
                body.hash(),
                BlockSizeEstimate::Advertised(estimate),
            )
        }),
    );
    f
}

async fn active_retention_boundary(count: u32, estimate: u32, backlog: u64, fits: bool) {
    let mut f = underestimated(count, estimate);
    let actual = u64::try_from(f.blocks[0].zcash_serialized_size()).unwrap();
    let other_reservations = u64::from(count - 1) * u64::from(estimate);
    // At receipt, the body's actual bytes replace its own estimate. All other
    // reservations and buffered bytes must still fit alongside it.
    f.routine.config.max_reorder_lookahead_bytes =
        backlog + other_reservations + actual - u64::from(!fits);
    f.view
        .send_modify(|view| view.reorder_buffered_bytes = backlog);
    f.publish().await;
    let owner = f.routine.window.outstanding[0].request.owner;
    let height = f.blocks[0].coinbase_height().unwrap();
    f.deliver(BlockSyncMessage::Block(f.blocks[0].clone()))
        .await
        .unwrap();
    assert_eq!(
        f.bodies.len(),
        usize::from(fits),
        "R12: actual bytes must fit after replacing only this body's estimate"
    );
    assert_eq!(f.routine.work.pending_contains(height), !fits);
    assert_eq!(
        f.routine.work.owner_for_height(height),
        fits.then_some(owner)
    );
    assert_eq!(f.routine.budget.reserved(), other_reservations);
    assert_eq!(f.routine.work.reserved_bytes(), other_reservations);
    f.assert_live(1, "R12: local retention does not undo response consumption");
    f.assert_no_peer_fault();
    f.deliver(f.done(1)).await.unwrap();
    f.assert_no_peer_fault();
    assert!(f.routine.window.outstanding.is_empty());
    assert_eq!(f.routine.budget.reserved(), 0);
    assert_eq!(f.routine.work.reserved_bytes(), 0);
    assert_eq!(f.routine.work.pending_contains(height), !fits);
}

#[tokio::test]
async fn r12_active_retention_exact_fit_and_one_byte_over() {
    for fits in [false, true] {
        active_retention_boundary(3, 1, 1_000, fits).await;
    }
}

#[tokio::test]
async fn r12_discarded_body_cannot_be_sent_twice() {
    let mut f = underestimated(1, 1);
    f.routine.config.max_reorder_lookahead_bytes = 1;
    f.publish().await;
    f.deliver(BlockSyncMessage::Block(f.blocks[0].clone()))
        .await
        .unwrap();
    assert!(f.bodies.is_empty());
    f.assert_no_peer_fault();
    f.assert_live(1, "R12: a discarded body still consumes response credit");
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R12: even a locally discarded body cannot be sent twice",
    )
    .await;
}

proptest! {
    #[test]
    fn r12_active_retention_accounts_for_other_work(
        count in 1u32..=8,
        estimate in 1u32..=32,
        backlog in 0u64..100_000,
        fits in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(active_retention_boundary(count, estimate, backlog, fits));
    }
}

#[tokio::test]
async fn r12_active_response_burst_stops_retaining_when_lookahead_is_full() {
    let mut f = underestimated(128, 1);
    let actual = u64::try_from(f.blocks[0].zcash_serialized_size()).unwrap();
    let cap = actual * 4;
    f.routine.config.max_reorder_lookahead_bytes = cap;
    f.publish().await;
    let mut retained_bytes = 0;
    let mut retained = Vec::new();
    for index in 0..f.blocks.len() {
        let body = f.blocks[index].clone();
        let height = body.coinbase_height().unwrap();
        let size = u64::try_from(body.zcash_serialized_size()).unwrap();
        f.deliver(BlockSyncMessage::Block(body)).await.unwrap();
        if let Ok(body) = f.bodies.try_recv() {
            retained_bytes += size;
            // Keep the real queued-body charge alive for the next receipt.
            retained.push(body);
        } else {
            assert!(f.routine.work.pending_contains(height));
        }
        assert!(
            retained_bytes <= cap,
            "R12: tiny hints cannot grow the backlog past its cap"
        );
        let remaining = u64::try_from(f.blocks.len() - index - 1).unwrap();
        assert_eq!(f.routine.budget.reserved(), remaining);
        assert_eq!(f.routine.work.reserved_bytes(), remaining);
    }
    assert!(
        !retained.is_empty(),
        "a useful body fits before the backlog fills"
    );
    assert!(retained.len() < f.blocks.len());
    f.assert_live(128, "R12: discarded bodies still count toward the ending");
    f.deliver(f.done(128)).await.unwrap();
    f.assert_no_peer_fault();
    assert_eq!(f.routine.budget.reserved(), 0);
}

#[tokio::test]
async fn r12_late_retention_refusal_preserves_pending_or_reassigned_work() {
    for reassigned in [false, true] {
        let mut a = underestimated(3, 1);
        a.publish().await;
        let deadline = a.routine.window.outstanding[0].deadline;
        a.routine
            .expire_due_timeouts(deadline + Duration::from_millis(1));
        let mut b = Fixture::for_peer(1_000, 3, 2);
        b.share_work_from(&a);
        if reassigned {
            b.publish().await;
        }
        let height = a.blocks[0].coinbase_height().unwrap();
        let owner = a.routine.work.owner_for_height(height);
        let reserved = a.routine.budget.reserved();
        a.routine.config.max_reorder_lookahead_bytes = 1;
        a.deliver(BlockSyncMessage::Block(a.blocks[0].clone()))
            .await
            .unwrap();
        assert!(a.bodies.is_empty());
        assert_eq!(a.routine.work.owner_for_height(height), owner);
        assert_eq!(a.routine.work.pending_contains(height), !reassigned);
        assert_eq!(a.routine.budget.reserved(), reserved);
        assert_eq!(a.routine.work.reserved_bytes(), reserved);
        a.assert_live(1, "R12: late refusal consumes only the original response");
        a.deliver(a.done(1)).await.unwrap();
        a.assert_no_peer_fault();
        if !reassigned {
            b.publish().await;
        }
        // Another owner can still finish the same work once it has room.
        for index in 0..3 {
            b.body(index).await;
        }
        b.deliver(b.done(3)).await.unwrap();
        assert_eq!(b.routine.budget.reserved(), 0);
    }
}

#[tokio::test]
async fn r12_late_retention_replaces_the_current_owners_estimate() {
    let mut a = underestimated(3, 1);
    a.publish().await;
    let deadline = a.routine.window.outstanding[0].deadline;
    a.routine
        .expire_due_timeouts(deadline + Duration::from_millis(1));
    // The same needed blocks are rediscovered with larger estimates before
    // another peer takes them. The old response still has its original hints.
    assert_eq!(a.routine.work.reset_above(block::Height(999)), 0);
    let estimate = 64;
    a.routine.work.extend(
        crate::zakura::block_sync::test_work_scope(),
        a.blocks.iter().map(|body| {
            (
                body.coinbase_height().unwrap(),
                body.hash(),
                BlockSizeEstimate::Advertised(estimate),
            )
        }),
    );
    let mut b = Fixture::for_peer(1_000, 3, 2);
    b.share_work_from(&a);
    b.publish().await;
    let owner = b.routine.window.outstanding[0].request.owner;
    let actual = u64::try_from(a.blocks[0].zcash_serialized_size()).unwrap();
    let other_reservations = 2 * u64::from(estimate);
    a.routine.config.max_reorder_lookahead_bytes = actual + other_reservations;
    a.body(0).await;
    assert_eq!(
        a.routine.work.owner_for_height(block::Height(1_000)),
        Some(owner)
    );
    assert_eq!(a.routine.budget.reserved(), other_reservations);
    a.deliver(a.done(1)).await.unwrap();
    b.deliver(BlockSyncMessage::Block(b.blocks[0].clone()))
        .await
        .unwrap();
    assert!(
        b.bodies.is_empty(),
        "the work is already received from the old peer"
    );
    for index in 1..3 {
        b.body(index).await;
    }
    b.deliver(b.done(3)).await.unwrap();
    b.assert_no_peer_fault();
    assert_eq!(b.routine.budget.reserved(), 0);
}
