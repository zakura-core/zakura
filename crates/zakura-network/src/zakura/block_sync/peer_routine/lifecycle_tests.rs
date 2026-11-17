//! Production-path witnesses for a peer routine's lifetime: receipt admission and
//! settlement, and a stale session's cleanup.
//!
//! Each case enters through the frame path or the routine's `run` and `Drop`, so
//! the permit, the receipt, and the funding transfer are the production ones.
//! `buffered_responses_are_considered_before_stream_failure` witnesses a response
//! buffered at close.

use std::sync::atomic::Ordering;

use super::super::{
    request::BlockSizeEstimate, sequencer_task::initial_view, tests::genesis_frontiers,
    BlockSyncMessage,
};
use super::scaling_tests::{deliver, probe, probe_in, Probe};
use super::tests::shared_floor_routine;
use super::*;
use crate::zakura::{regulation::ReservationPool, transport::ByteBudget};

fn one_request_probe() -> Probe {
    probe_in(1, ReservationPool::new(1).unwrap())
}

/// Forget the request covering `height`, so its body arrives unmatched.
fn forget_request(routine: &mut PeerRoutine, height: block::Height) {
    let key = routine
        .state
        .window
        .outstanding_key_for_height(height)
        .unwrap();
    routine.state.remove_request(key).unwrap();
}

/// The sequencer closes before a body gets its slot. The body fails locally,
/// never settles a receipt, and the routine's cleanup still returns the work.
#[tokio::test(flavor = "current_thread")]
async fn a_closed_sequencer_refuses_admission_without_settling() {
    let Probe {
        budget,
        mut routine,
        mut outbound,
        blocks,
        ..
    } = one_request_probe();
    routine.try_fill();
    outbound.recv().await.unwrap();
    let charge = budget.reserved();
    let (sender, receiver) = mpsc::channel(1);
    drop(receiver);
    routine.io.sequencer_input = sender;
    let frame = BlockSyncMessage::Block(blocks[0].clone())
        .encode_frame()
        .unwrap();

    let result = routine.handle_frame(&mut block_sync_guard(), frame).await;

    assert!(matches!(result, Err(SinkReject::Local(_))));
    assert_eq!(budget.reserved(), charge);
    let work = routine.io.work.clone();
    drop(routine);
    assert_eq!(budget.reserved(), 0);
    assert!(work.pending_contains(block::Height(1)));
}

/// A session replacement owns the height again before the old routine's `Drop`
/// runs. The exact owner keeps the old cleanup from returning, releasing, or
/// unpublishing the replacement's request.
#[tokio::test(flavor = "current_thread")]
async fn a_stale_session_cleanup_leaves_the_replacement_request() {
    let budget = ByteBudget::new(1_000_000);
    let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
    work.extend(
        super::super::test_work_scope(),
        [(
            block::Height(1),
            block::Hash([1; 32]),
            BlockSizeEstimate::Advertised(1_000),
        )],
    );
    let registry = Arc::new(PeerRegistry::new());
    let (_view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
    let (mut old, _old_outbound) = shared_floor_routine(7, &work, &registry, view_rx.clone());
    old.try_fill();
    let old_owner = work.owner_for_height(block::Height(1)).unwrap();
    // Another actor retires the old request while its routine still records it.
    work.release_reserved_and_return_items_detailed_for_owner(old_owner, [block::Height(1)]);
    assert_eq!(budget.reserved(), 0);

    let (mut replacement, _new_outbound) = shared_floor_routine(7, &work, &registry, view_rx);
    assert_ne!(old.io.generation, replacement.io.generation);
    let report = replacement.try_fill();
    assert_eq!(report.sent, 1);
    let owner = work.owner_for_height(block::Height(1)).unwrap();
    assert_ne!(owner, old_owner);
    let charge = budget.reserved();
    assert!(charge > 0);

    let (peer, generation) = (replacement.io.peer.clone(), replacement.io.generation);
    drop(old);

    assert_eq!(work.owner_for_height(block::Height(1)), Some(owner));
    assert!(work.in_flight_contains(block::Height(1)));
    assert_eq!(budget.reserved(), charge);
    assert_eq!(work.reserved_bytes(), charge);
    assert_eq!(registry.outstanding_snapshot(&peer, generation).len(), 1);

    drop(replacement);
    assert_eq!(budget.reserved(), 0);
    assert!(work.pending_contains(block::Height(1)));
    assert!(registry.outstanding_snapshot(&peer, generation).is_empty());
}

/// A reset wins the race against a published request's writer start. Transport
/// skips the frame and the routine's next collection retires it without charging
/// the peer.
#[tokio::test(flavor = "current_thread")]
async fn a_reset_before_writer_start_skips_the_published_request() {
    let Probe {
        budget,
        mut routine,
        mut outbound,
        ..
    } = one_request_probe();
    routine.try_fill();
    let streak = routine.state.window.requests_without_block_progress;
    assert_eq!(routine.state.window.outstanding.len(), 1);
    routine.io.work.reset_above(block::Height(0));
    assert_eq!(budget.reserved(), 0);

    // The transport's receive side reaches the frame, skips it, and has nothing
    // left to deliver.
    assert!(futures::poll!(Box::pin(outbound.recv())).is_pending());
    routine.gc_skipped_outstanding();

    assert!(routine.state.window.outstanding.is_empty());
    assert_eq!(routine.io.requester.occupancy().len, 0);
    assert!(routine.state.window.requests_without_block_progress < streak);
}

/// A reset clears an unproven peer's probe streak. The routine publishes its
/// slots after the streak clears, so the peer at its probe cap shows free
/// slots again.
#[tokio::test(flavor = "current_thread")]
async fn a_reset_that_clears_the_probe_cap_republishes_free_slots() {
    let budget = ByteBudget::new(1_000_000);
    let work = Arc::new(WorkQueue::new(block::Height(0), budget));
    work.extend(
        super::super::test_work_scope(),
        [(
            block::Height(1),
            block::Hash([1; 32]),
            BlockSizeEstimate::Advertised(1_000),
        )],
    );
    let registry = Arc::new(PeerRegistry::new());
    let (view_tx, view_rx) = watch::channel(initial_view(genesis_frontiers()));
    let (mut routine, _outbound) = shared_floor_routine(7, &work, &registry, view_rx);
    assert_eq!(routine.try_fill().sent, 1);
    assert!(
        !routine.state.published_free_slots,
        "the probe reached the cap"
    );

    view_tx.send_modify(|view| view.reset_epoch += 1);
    routine.on_view_changed();

    assert_eq!(routine.state.window.requests_without_block_progress, 0);
    assert!(
        routine.state.published_free_slots,
        "the reset cleared the cap, so the publication shows free slots"
    );
}

/// A late body proves progress and clears an unproven peer's probe streak. The
/// routine publishes its slots after the receipt, so the peer at its probe cap
/// shows free slots again.
#[tokio::test(start_paused = true)]
async fn a_late_body_that_clears_the_probe_cap_republishes_free_slots() {
    let Probe {
        budget: _,
        mut routine,
        mut outbound,
        blocks,
        mut bodies,
        inbound: _inbound,
        _events,
    } = probe(1);
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(1),
        max_blocks_per_response: 1,
        max_inflight_requests: 3,
        ..BlockSyncStatus::default()
    });
    assert_eq!(routine.try_fill().sent, 1);
    outbound.recv().await.unwrap();
    assert!(
        !routine.state.published_free_slots,
        "the probe reached the cap"
    );
    forget_request(&mut routine, block::Height(1));

    deliver(&mut routine, BlockSyncMessage::Block(blocks[0].clone())).await;

    assert!(
        bodies.try_recv().is_ok(),
        "the late body reached the sequencer"
    );
    assert_eq!(routine.state.window.requests_without_block_progress, 0);
    assert!(
        routine.state.published_free_slots,
        "the late body cleared the cap, so the publication shows free slots"
    );
}

/// How a body waiting for its sequencer slot stops waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitEnd {
    /// The sequencer frees a slot and the receipt finishes.
    Admitted,
    /// The routine's future is dropped.
    FutureDropped,
    /// The session's token cancels. The routine's own cleanup then returns the work.
    SessionCancelled,
}

/// Exercise the production receipt future while the sequencer queue is full. No
/// body reaches the sequencer until the slot opens. The request keeps its charge
/// until then, and the blocked flag and read pause end with the wait.
#[tokio::test(flavor = "current_thread")]
async fn receipt_waits_for_admission_before_settlement() {
    use WaitEnd::*;
    for (matched, end) in [
        (true, Admitted),
        (true, FutureDropped),
        (true, SessionCancelled),
        (false, Admitted),
        (false, FutureDropped),
    ] {
        let Probe {
            budget,
            mut routine,
            mut outbound,
            blocks,
            bodies: _bodies,
            inbound: _inbound,
            _events,
        } = probe(1);
        routine.try_fill();
        outbound.recv().await.unwrap();
        if !matched {
            forget_request(&mut routine, block::Height(1));
        }
        let work = routine.io.work.clone();
        let charge = work.reserved_bytes();
        assert!(charge > 0);
        let unrelated = budget.reserve_owned(17).unwrap();
        let (sender, mut receiver) = mpsc::channel(1);
        let slot = sender.clone().reserve_owned().await.unwrap();
        routine.io.sequencer_input = sender;
        let resident = routine.io.sequencer_input_bytes.clone();
        let blocked = routine.io.handoff_blocked.clone();
        let cancel = routine.io.cancel.clone();
        let frame = BlockSyncMessage::Block(blocks[0].clone())
            .encode_frame()
            .unwrap();
        let mut guard = block_sync_guard();
        let mut handoff = Box::pin(routine.handle_frame(&mut guard, frame));

        assert!(futures::poll!(&mut handoff).is_pending());
        assert!(blocked.load(Ordering::Relaxed));
        assert_eq!(
            work.reserved_bytes(),
            charge,
            "waiting for admission preserves the request reservation"
        );
        assert_eq!(
            budget.reserved(),
            charge + 17,
            "the request still owns the charge"
        );
        assert_eq!(resident.load(Ordering::Relaxed), 0);

        match end {
            Admitted => {
                drop(slot);
                tokio::time::timeout(Duration::from_secs(1), handoff)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(work.reserved_bytes(), 0);
                assert_eq!(budget.reserved(), 17);
                drop(
                    receiver
                        .try_recv()
                        .expect("completed handoff queued the body"),
                );
            }
            FutureDropped | SessionCancelled => {
                if end == SessionCancelled {
                    cancel.cancel();
                    assert!(matches!(
                        futures::poll!(&mut handoff),
                        std::task::Poll::Ready(Ok(()))
                    ));
                }
                drop(handoff);
                drop(slot);
                assert!(receiver.try_recv().is_err());
                assert_eq!(work.reserved_bytes(), charge);
                assert_eq!(budget.reserved(), charge + 17);
                if end == FutureDropped {
                    work.reset_above(block::Height(0));
                }
            }
        }
        assert!(!blocked.load(Ordering::Relaxed));
        assert_eq!(resident.load(Ordering::Relaxed), 0);
        drop(routine);
        assert_eq!(
            budget.reserved(),
            17,
            "settlement preserves unrelated charges, and cleanup cannot release twice"
        );
        if end == SessionCancelled {
            assert!(work.pending_contains(block::Height(1)));
        }
        drop(unrelated);
    }
}

#[tokio::test(start_paused = true)]
async fn synchronous_receipts_do_not_wake_floor_rankers() {
    for matched in [true, false] {
        let Probe {
            budget: _,
            mut routine,
            mut outbound,
            blocks,
            mut bodies,
            inbound: _inbound,
            _events,
        } = probe(3);
        routine.handle_status(BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(2),
            max_blocks_per_response: 1,
            max_inflight_requests: 3,
            ..BlockSyncStatus::default()
        });
        routine.try_fill();
        outbound.recv().await.unwrap();
        outbound.recv().await.unwrap();
        assert!(outbound.try_recv().is_err());
        // Prime the RTT with a real receipt. Paused time keeps the next receipt's
        // RTT unchanged, so neither slots nor RTT require a ranking notification.
        deliver(&mut routine, BlockSyncMessage::Block(blocks[0].clone())).await;
        bodies.try_recv().unwrap();
        assert!(routine.state.published_free_slots);
        if !matched {
            forget_request(&mut routine, block::Height(2));
        }
        let registry = routine.io.registry.clone();
        let ranked = registry.subscribe_floor_ranking().notified();
        tokio::pin!(ranked);
        assert!(futures::poll!(&mut ranked).is_pending());

        deliver(&mut routine, BlockSyncMessage::Block(blocks[1].clone())).await;

        assert!(bodies.try_recv().is_ok(), "the body reached the sequencer");
        assert!(
            futures::poll!(&mut ranked).is_pending(),
            "a synchronous receipt must not wake floor rankers: matched={matched}"
        );
    }
}

/// A receiver wake can race with cancellation before the sender settles funding.
#[tokio::test(flavor = "current_thread")]
async fn receipt_keeps_funding_until_the_production_send_completes() {
    use std::sync::atomic::AtomicU64;

    for matched in [true, false] {
        let Probe {
            budget,
            mut routine,
            mut outbound,
            blocks,
            bodies: _bodies,
            inbound: _inbound,
            _events,
        } = probe(1);
        routine.try_fill();
        outbound.recv().await.unwrap();
        if !matched {
            forget_request(&mut routine, block::Height(1));
        }
        let charge = routine.io.work.reserved_bytes();
        assert!(charge > 0);
        let unrelated = budget.reserve_owned(17).unwrap();
        let (sender, mut receiver) = mpsc::channel(1);
        routine.io.sequencer_input = sender;
        let resident = routine.io.sequencer_input_bytes.clone();
        let charge_at_send = Arc::new(AtomicU64::new(0));
        let resident_at_send = Arc::new(AtomicU64::new(0));
        let waker = crate::zakura::transport::callback_waker({
            let budget = budget.clone();
            let resident = resident.clone();
            let charge_at_send = charge_at_send.clone();
            let resident_at_send = resident_at_send.clone();
            let cancel = routine.io.cancel.clone();
            move || {
                charge_at_send.store(budget.reserved(), Ordering::SeqCst);
                resident_at_send.store(resident.load(Ordering::Relaxed), Ordering::SeqCst);
                cancel.cancel();
            }
        });
        let mut context = std::task::Context::from_waker(&waker);
        assert!(receiver.poll_recv(&mut context).is_pending());

        deliver(&mut routine, BlockSyncMessage::Block(blocks[0].clone())).await;

        assert!(
            routine.io.cancel.is_cancelled(),
            "the send woke the receiver"
        );
        assert_eq!(
            charge_at_send.load(Ordering::SeqCst),
            charge + 17,
            "the request charge survives until the send completes"
        );
        assert!(
            resident_at_send.load(Ordering::SeqCst) > 0,
            "the body is charged before the receiver can run"
        );
        assert_eq!(routine.io.work.reserved_bytes(), 0);
        assert_eq!(routine.io.budget.reserved(), 17);
        drop(
            receiver
                .try_recv()
                .expect("cancellation cannot interrupt the admitted send"),
        );
        assert_eq!(resident.load(Ordering::Relaxed), 0);
        drop(unrelated);
        assert_eq!(routine.io.budget.reserved(), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn accounting_failure_cancels_production_receipt() {
    for matched in [true, false] {
        let Probe {
            budget,
            mut routine,
            mut outbound,
            blocks,
            mut bodies,
            inbound: _inbound,
            _events,
        } = probe(1);
        routine.try_fill();
        outbound.recv().await.unwrap();
        if !matched {
            forget_request(&mut routine, block::Height(1));
        }
        let charge = routine.io.work.reserved_bytes();
        assert!(charge > 0);
        // Keep actual funding live while removing it from the queue's settlement pool.
        let retained = routine.io.work.retain_funding_for_test(charge);
        tokio::time::timeout(
            Duration::from_secs(1),
            deliver(&mut routine, BlockSyncMessage::Block(blocks[0].clone())),
        )
        .await
        .unwrap();
        assert!(
            routine.io.cancel.is_cancelled(),
            "receipt accounting failure did not cancel the routine"
        );
        assert!(budget.is_closed());
        assert!(
            bodies.try_recv().is_err(),
            "failed receipt published a body"
        );
        assert_eq!(routine.io.sequencer_input_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(budget.reserved(), charge);
        drop(routine);
        assert_eq!(budget.reserved(), charge);
        drop(retained);
        assert_eq!(budget.reserved(), 0);
        assert!(budget.is_closed());
    }
}
