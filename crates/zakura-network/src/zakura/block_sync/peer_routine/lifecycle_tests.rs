//! Production-path witnesses for a peer routine's lifetime: cancellation before
//! receipt admission, a response buffered at close, and a stale session's cleanup.
//!
//! Each case enters through the frame path or the routine's `run` and `Drop`, so
//! the permit, the receipt, and the funding transfer are the production ones.

use std::sync::atomic::Ordering;

use super::super::{
    request::BlockSizeEstimate, sequencer_task::initial_view, tests::genesis_frontiers,
    BlockSyncMessage,
};
use super::scaling_tests::{probe_in, Probe};
use super::tests::shared_floor_routine;
use super::*;
use crate::zakura::{
    regulation::ReservationPool,
    transport::{ByteBudget, OrderedStreamFailureCause},
};

fn one_request_probe() -> Probe {
    probe_in(1, ReservationPool::new(1).unwrap())
}

/// The session cancels while a body waits for its sequencer slot. No body
/// reaches the sequencer, the request keeps its charge until the routine's own
/// cleanup, and the blocked flag and read pause end with the wait.
#[tokio::test(flavor = "current_thread")]
async fn cancellation_before_admission_keeps_the_request_charge() {
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
    assert!(charge > 0);

    let (sender, mut receiver) = mpsc::channel(1);
    let slot = sender.clone().reserve_owned().await.unwrap();
    routine.io.sequencer_input = sender;
    let cancel = routine.io.cancel.clone();
    let blocked = routine.io.handoff_blocked.clone();
    let frame = BlockSyncMessage::Block(blocks[0].clone())
        .encode_frame()
        .unwrap();
    let mut guard = block_sync_guard();
    {
        let mut handoff = Box::pin(routine.handle_frame(&mut guard, frame));
        assert!(futures::poll!(&mut handoff).is_pending());
        assert!(blocked.load(Ordering::Relaxed));
        assert_eq!(budget.reserved(), charge);

        cancel.cancel();

        assert!(matches!(
            futures::poll!(&mut handoff),
            std::task::Poll::Ready(Ok(()))
        ));
    }
    assert!(!blocked.load(Ordering::Relaxed));
    assert_eq!(budget.reserved(), charge);
    assert_eq!(routine.io.work.reserved_bytes(), charge);
    drop(slot);
    assert!(receiver.try_recv().is_err());
    assert_eq!(routine.io.sequencer_input_bytes.load(Ordering::Relaxed), 0);

    let work = routine.io.work.clone();
    drop(routine);
    assert_eq!(budget.reserved(), 0);
    assert!(work.pending_contains(block::Height(1)));
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

/// A valid response sits in the inbound queue when the stream fails. The drain
/// on the way out sends the body through its production receipt: the sequencer
/// holds it, its funding settled once, and the routine's cleanup releases nothing
/// more.
#[tokio::test(flavor = "current_thread")]
async fn a_buffered_response_at_close_settles_through_the_real_receipt() {
    let Probe {
        budget,
        mut routine,
        mut outbound,
        blocks,
        inbound,
        mut bodies,
        ..
    } = one_request_probe();
    let cause = OrderedStreamFailureCause::default();
    routine.io.recv = {
        let recv = std::mem::replace(&mut routine.io.recv, crate::zakura::framed_channel(1).1);
        recv.with_failure_cause(cause.clone())
    };
    routine.try_fill();
    outbound.recv().await.unwrap();
    let work = routine.io.work.clone();
    let cancel = routine.io.cancel.clone();
    assert!(budget.reserved() > 0);
    inbound
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .unwrap(),
        )
        .await
        .unwrap();
    cause.record(OrderedStreamFailure::RemoteClose);
    cancel.cancel();

    let result = tokio::time::timeout(Duration::from_secs(5), routine.run())
        .await
        .expect("cancellation ends the run after its drain");

    assert!(result.is_ok(), "{result:?}");
    let body = bodies.try_recv().expect("the drain sequenced the body");
    assert_eq!(body.height, block::Height(1));
    assert_eq!(budget.reserved(), 0, "the receipt settled its funding");
    assert_eq!(work.reserved_bytes(), 0);
    assert!(work.in_flight_contains(block::Height(1)));
    assert!(!work.pending_contains(block::Height(1)));
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
