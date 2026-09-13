//! Keep written response permissions until an ending or connection closure.

use super::*;

#[tokio::test]
async fn r09_deadline_does_not_revoke_a_written_request() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    let deadline = f.routine.window.outstanding[0].deadline;
    f.routine
        .expire_due_timeouts(deadline + Duration::from_millis(1));
    f.assert_live(0, "R09 local deadline");
    f.body(0).await;
    f.deliver(f.done(1)).await.unwrap();
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn r10_finality_does_not_consume_network_response_parts() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.view.send_modify(|view| {
        view.download_floor = block::Height(102);
        view.verified_tip = block::Height(102);
        view.finalized = block::Height(102);
    });
    f.routine.gc_committed_outstanding();
    f.assert_live(0, "R10 finality is not a peer response");
    f.deliver(f.unavailable(3)).await.unwrap();
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn r10_reorganization_keeps_the_original_response_identity() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.routine.work.reset_above(block::Height(99));
    f.view.send_modify(|view| view.reset_epoch += 1);
    f.routine.on_view_changed();
    f.assert_live(0, "R10 selected-chain reset");
}

#[tokio::test]
async fn r02_retry_cannot_send_an_overlapping_live_range() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    let deadline = f.routine.window.outstanding[0].deadline;
    f.routine
        .expire_due_timeouts(deadline + Duration::from_millis(1));
    f.routine.retry_avoid.clear();
    f.routine.try_fill().await;
    assert!(
        time::timeout(Duration::from_millis(20), f.outbound.recv())
            .await
            .is_err(),
        "R02: a local deadline cannot authorize a second overlapping wire request"
    );
}

#[tokio::test]
async fn r11_connection_drop_releases_only_unreceived_work() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    let work = f.routine.work.clone();
    let budget = f.routine.budget.clone();
    let registry = f.routine.registry.clone();
    let peer = f.routine.peer.clone();
    let session = f.routine.session.clone();
    drop(f);
    assert!(
        session.connection_is_closed_for_test(),
        "unfinished authorization ends only with the connection"
    );
    assert_eq!(budget.reserved(), 0);
    assert!(!work.pending_contains(block::Height(100)));
    assert!(work.pending_contains(block::Height(101)));
    assert!(work.pending_contains(block::Height(102)));
    assert!(!registry.peer_has_outstanding_height(&peer, block::Height(101)));
}

#[tokio::test]
async fn r09_reassigned_work_still_consumes_the_original_peers_response() {
    let mut a = Fixture::for_peer(100, 3, 1);
    a.publish().await;
    let deadline = a.routine.window.outstanding[0].deadline;
    a.routine
        .expire_due_timeouts(deadline + Duration::from_millis(1));
    let mut b = Fixture::for_peer(100, 3, 2);
    b.share_work_from(&a);
    b.publish().await;
    b.body(0).await;
    a.deliver(BlockSyncMessage::Block(a.blocks[0].clone()))
        .await
        .unwrap();
    a.assert_no_peer_fault();
    a.assert_live(1, "R09 original authorization after competing delivery");
    a.deliver(a.done(1)).await.unwrap();
    b.deliver(b.done(1)).await.unwrap();
}

#[tokio::test]
async fn r10_lost_local_interest_does_not_revoke_expected_hashes() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.routine.work.reset_above(block::Height(99));
    f.routine.gc_obsolete_outstanding();
    f.assert_live(0, "R10 changed local interest");
}

#[tokio::test]
async fn r11_terminals_pending_still_occupy_protocol_inflight_capacity() {
    let mut f = Fixture::new(100, 3);
    f.routine.window.max_inflight_requests = 1;
    f.publish().await;
    for index in 0..3 {
        f.body(index).await;
    }
    f.routine.work.extend(
        super::super::test_work_scope(),
        fake_blocks_in_range(103, 103).iter().map(|body| {
            (
                block::Height(103),
                body.hash(),
                BlockSizeEstimate::Confirmed(u32::try_from(body.zcash_serialized_size()).unwrap()),
            )
        }),
    );
    f.routine.servable_high = block::Height(103);
    f.routine.try_fill().await;
    assert!(
        time::timeout(Duration::from_millis(20), f.outbound.recv())
            .await
            .is_err(),
        "R11: the missing terminal occupies the only advertised protocol slot"
    );
}

#[derive(Clone, Debug)]
enum LocalChange {
    Deadline,
    Finality,
    Reset,
    LostInterest,
}

async fn legal_history(count: u32, prefix: usize, changes: Vec<LocalChange>) {
    let mut f = Fixture::new(100, count);
    f.publish().await;
    for index in 0..prefix {
        f.body(index).await;
    }
    f.assert_live(prefix, "R01/R08 model after each consumed prefix");
    for change in changes {
        match change {
            LocalChange::Deadline => {
                let deadline = f.routine.window.outstanding[0].deadline;
                f.routine
                    .expire_due_timeouts(deadline + Duration::from_millis(1));
            }
            LocalChange::Finality => {
                f.view.send_modify(|view| {
                    view.download_floor = block::Height(99 + count);
                    view.verified_tip = view.download_floor;
                    view.finalized = view.download_floor;
                });
                f.routine.gc_committed_outstanding();
            }
            LocalChange::Reset => {
                f.routine.work.reset_above(block::Height(99));
                f.view.send_modify(|view| view.reset_epoch += 1);
                f.routine.on_view_changed();
            }
            LocalChange::LostInterest => {
                f.routine.work.reset_above(block::Height(99));
                f.routine.gc_obsolete_outstanding();
            }
        }
        f.assert_live(prefix, &format!("R09/R10 model after {change:?}"));
        f.assert_no_peer_fault();
    }
    // The peer is still allowed to close its original exchange even when local
    // work changed. Consumed network parts are independent of verification need.
    let ending = if prefix == 0 {
        f.unavailable(count)
    } else {
        f.done(u32::try_from(prefix).unwrap())
    };
    f.deliver(ending).await.unwrap();
    assert!(f.routine.window.outstanding.is_empty());
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn r01_r08_mandatory_legal_terminal_and_maximal_count_histories() {
    for count in [1, 2, 128] {
        for prefix in [0, 1, usize::try_from(count).unwrap()] {
            legal_history(count, prefix, Vec::new()).await;
        }
    }
}

proptest! {
    #[test]
    fn r09_r10_generated_local_changes_preserve_live_authorization(
        count in 1u32..=128, prefix in 0usize..=128,
        changes in prop::collection::vec(prop_oneof![Just(LocalChange::Deadline), Just(LocalChange::Finality), Just(LocalChange::Reset), Just(LocalChange::LostInterest)], 1..9),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(legal_history(count, prefix.min(usize::try_from(count).unwrap()), changes));
    }
}

#[tokio::test]
async fn r11_connection_closure_cleans_each_publication_and_response_phase() {
    // Includes queued but unclaimed, written, partial body, and terminal pending.
    for phase in 0usize..=4 {
        let mut f = Fixture::new(100, 3);
        if phase == 0 {
            f.routine.try_fill().await;
        } else {
            f.publish().await;
        }
        for index in 0..phase.saturating_sub(1) {
            f.body(index).await;
        }
        let work = f.routine.work.clone();
        let budget = f.routine.budget.clone();
        let registry = f.routine.registry.clone();
        let peer = f.routine.peer.clone();
        drop(f);
        assert_eq!(budget.reserved(), 0, "R11 closure phase={phase}");
        for index in 0..3 {
            let height = block::Height(100 + u32::try_from(index).unwrap());
            assert!(!registry.peer_has_outstanding_height(&peer, height));
            assert_eq!(
                work.pending_contains(height),
                index >= phase.saturating_sub(1)
            );
        }
    }
}

#[tokio::test]
async fn r09_former_owner_body_uses_the_replacement_work_owner() {
    let mut a = Fixture::for_peer(100, 3, 1);
    a.publish().await;
    let old_owner = a.routine.window.outstanding[0].request.owner;
    a.routine
        .expire_due_timeouts(a.routine.window.outstanding[0].deadline);
    let mut b = Fixture::for_peer(100, 3, 2);
    b.share_work_from(&a);
    b.publish().await;
    let current_owner = b.routine.window.outstanding[0].request.owner;
    assert_ne!(old_owner, current_owner);
    a.deliver(BlockSyncMessage::Block(a.blocks[0].clone()))
        .await
        .unwrap();
    let received = a
        .bodies
        .try_recv()
        .expect("authorized useful late body reaches handling");
    assert_eq!(received.owner, current_owner);
    a.assert_live(1, "original connection consumes its own wire part");
    a.assert_no_peer_fault();
}

#[tokio::test]
async fn r11_queued_only_drop_skips_the_write_and_keeps_the_connection() {
    let mut f = Fixture::new(100, 3);
    f.routine.try_fill().await;
    let queued = f.outbound.recv().await.unwrap();
    let session = f.routine.session.clone();
    drop(f);
    assert!(!session.connection_is_closed_for_test());
    queued
        .write_with(|_| async {
            panic!("a skipped request must not reach the wire");
            #[allow(unreachable_code)]
            Ok::<_, ()>(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn r11_withheld_ending_closes_locally_on_first_and_repeated_stall() {
    for allow_park in [true, false] {
        let mut f = Fixture::new(100, 3);
        f.routine.allow_no_progress_park = allow_park;
        f.publish().await;
        for index in 0..3 {
            f.body(index).await;
        }
        let deadline = f.routine.window.block_liveness_deadline.unwrap();
        let result = f.routine.check_block_liveness(deadline);
        assert!(matches!(result, Err(SinkReject::Connection(_))));
        f.assert_live(3, "liveness is not terminal consumption");
        f.assert_no_peer_fault();
        let session = f.routine.session.clone();
        drop(f);
        assert!(session.connection_is_closed_for_test());
    }
}

#[tokio::test]
async fn status_availability_changes_preserve_existing_response_credit() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.deliver(BlockSyncMessage::Status(BlockSyncStatus {
        servable_low: block::Height(101),
        servable_high: block::Height(200),
        max_blocks_per_response: 128,
        max_inflight_requests: 8,
        ..f.routine.config.initial_status()
    }))
    .await
    .unwrap();
    assert!(!f.routine.session.connection_is_closed_for_test());
    f.assert_live(1, "servable changes do not revoke prior hashes");
    f.body(1).await;
    f.deliver(f.done(2)).await.unwrap();
}

#[tokio::test]
async fn status_numeric_changes_require_local_reconnect_without_a_peer_fault() {
    for (blocks, inflight, bytes) in [
        (3, 8, 1_048_576),
        (5, 8, 1_048_576),
        (4, 7, 1_048_576),
        (4, 9, 1_048_576),
        (4, 8, 1_048_575),
        (4, 8, 1_048_577),
    ] {
        let mut f = Fixture::with_initial_status(
            100,
            3,
            0x47,
            Some(BlockSyncStatus {
                servable_low: block::Height(100),
                servable_high: block::Height(102),
                max_blocks_per_response: 4,
                max_inflight_requests: 8,
                max_response_bytes: 1_048_576,
                ..BlockSyncStatus::default()
            }),
        );
        f.publish().await;
        f.body(0).await;
        f.deliver(BlockSyncMessage::Status(BlockSyncStatus {
            servable_low: block::Height(100),
            servable_high: block::Height(200),
            max_blocks_per_response: blocks,
            max_inflight_requests: inflight,
            max_response_bytes: bytes,
            ..f.routine.config.initial_status()
        }))
        .await
        .unwrap();
        assert!(f.routine.session.connection_is_closed_for_test());
        f.assert_no_peer_fault();
        f.assert_live(1, "numeric changes do not fabricate an ending");
    }
}
