use super::*;
use crate::zakura::block_sync::block_sync_observe::{
    BlockEvent, BlockSyncObserver, ObserverSlot, RequestStep,
};
use crate::zakura::transport::ByteBudget;
use crate::zakura::{
    block_sync::{test_work_scope, BlockSizeEstimate},
    transport::worker_framed_channel,
    Frame, FramedSend,
};
use std::{num::NonZeroU64, time::Duration};

impl RequestWriteStatus {
    pub(in crate::zakura::block_sync) fn written_for_tests() -> Self {
        Self(Arc::new(AtomicU8::new(WRITTEN)), None)
    }
}

thread_local! {
    static BEFORE_RESET_HEIGHT_CHECK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static BEFORE_DROP_CLEANUP: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

pub(super) fn before_reset_height_check() {
    if let Some(hook) = BEFORE_RESET_HEIGHT_CHECK.with_borrow_mut(Option::take) {
        hook();
    }
}

pub(super) fn before_drop_cleanup() {
    if let Some(hook) = BEFORE_DROP_CLEANUP.with_borrow_mut(Option::take) {
        hook();
    }
}

#[test]
fn settlement_observes_write_status_while_last_owner_is_dropping() {
    for started in [false, true] {
        let mut f = Fixture::new();
        let claim = f.take(1);
        let claim = claim.publish(|_| {}).unwrap();
        if started {
            assert!(claim.try_start());
        }
        let weak = Arc::downgrade(&claim);
        let owner = claim.owner();
        let work = f.work.clone();

        // Arbitrate expiry after the last strong reference is gone, before Drop
        // acquires the work lock to return the request's remaining heights.
        BEFORE_DROP_CLEANUP.with_borrow_mut(|hook| {
            *hook = Some(Box::new(move || {
                assert!(weak.upgrade().is_none());
                let outcome = work.release_reserved_and_return_items_detailed_for_owner(
                    owner,
                    [block::Height(1)],
                );
                assert_eq!(outcome.request_was_unwritten, !started);
                assert_eq!(outcome.returned_count, 1);
                assert_eq!(outcome.released_bytes, 100);
            }));
        });
        drop(claim);
        assert_eq!(f.budget.reserved(), 0);
        assert_eq!(f.work.reserved_bytes(), 0);
        assert_eq!(f.work.pending_len(), 2);
        assert!(f.work.lock().request_writes.is_empty());
    }
}

#[tokio::test]
async fn funded_receipt_survives_reset_and_replacement() {
    let mut f = Fixture::new();
    let unrelated = f.budget.reserve_owned(17).unwrap();
    let claim = f.take(1);
    let owner = claim.owner();
    let claim = claim.publish(|_| {}).unwrap();
    assert!(claim.try_start());
    claim.written();
    drop(claim);
    assert!(f.work.lock().request_writes.is_empty());
    assert_eq!(f.budget.reserved(), 217, "funding outlives the writer");

    let observed_budget = f.budget.clone();
    let capacity = observed_budget.subscribe_capacity().notified();
    tokio::pin!(capacity);
    capacity.as_mut().enable();
    let receipt = f
        .work
        .begin_receipt_for_owner(owner, block::Height(1))
        .unwrap()
        .unwrap();
    assert_eq!(receipt.funding.bytes(), 100);
    assert_eq!(f.work.reserved_bytes(), 100);
    assert_eq!(
        f.budget.reserved(),
        217,
        "receipt transfers without releasing"
    );
    assert!(futures::poll!(&mut capacity).is_pending());

    f.reset();
    assert!(
        futures::poll!(&mut capacity).is_ready(),
        "reset wakes capacity waiters"
    );
    assert_eq!(f.work.reserved_bytes(), 0);
    assert_eq!(
        f.budget.reserved(),
        117,
        "reset releases only the unreceived height"
    );
    f.refill();
    let replacement = f.take(2);
    let replacement = replacement.publish(|_| {}).unwrap();
    assert_eq!(f.budget.reserved(), 317);
    f.expire(owner);
    assert_eq!(
        f.budget.reserved(),
        317,
        "the old owner cannot settle replacement work"
    );
    drop(receipt);
    assert_eq!(
        f.budget.reserved(),
        217,
        "receipt cancellation preserves replacement funding"
    );
    drop(replacement);
    assert_eq!(f.budget.reserved(), 17);
    drop(unrelated);
    assert_eq!(f.budget.reserved(), 0);
}

#[test]
fn owned_overdraft_rejects_counter_overflow_without_charging() {
    let budget = ByteBudget::new(u64::MAX);
    let full = budget.reserve_owned(u64::MAX).unwrap();
    assert!(budget.charge_owned(1).is_none());
    assert_eq!(budget.reserved(), u64::MAX);
    drop(full);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn reset_drops_unaffected_last_claim_after_unlocking() {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let reset = std::thread::spawn(move || {
        let mut f = Fixture::new();
        let claim = f.take(1);
        let claim = claim.publish(|_| {}).unwrap();
        // Release the writer's reference after reset upgrades the weak reference,
        // but before it rejects this claim at or below the new floor.
        BEFORE_RESET_HEIGHT_CHECK.with_borrow_mut(|hook| *hook = Some(Box::new(|| drop(claim))));
        f.work.reset_above(block::Height(2));
        assert_eq!(f.budget.reserved(), 0);
        assert_eq!(f.work.reserved_bytes(), 0);
        assert_eq!(f.work.pending_len(), 0);
        assert_eq!(f.work.in_flight_len(), 0);
        assert!(!f.cancel.is_cancelled());
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("dropping the last unaffected claim must not deadlock reset");
    reset.join().unwrap();
}

struct Fixture {
    work: Arc<WorkQueue>,
    budget: ByteBudget,
    cancel: CancellationToken,
}

impl Fixture {
    fn new() -> Self {
        Self::with_queue(WorkQueue::new(block::Height(0), ByteBudget::new(1000)))
    }

    /// A fixture whose queue reports each request step to the returned log.
    fn observed() -> (Self, StepLog) {
        let log = StepLog::default();
        let observer = ObserverSlot(Some(Arc::new(log.clone())));
        let fixture = Self::with_queue(
            WorkQueue::new(block::Height(0), ByteBudget::new(1000)).with_observer(observer),
        );
        (fixture, log)
    }

    fn with_queue(work: WorkQueue) -> Self {
        let fixture = Self {
            budget: work.budget(),
            work: Arc::new(work),
            cancel: CancellationToken::new(),
        };
        fixture.work.set_estimate_floor_for_tests(1);
        fixture.refill();
        fixture
    }

    fn refill(&self) {
        self.work.extend(
            test_work_scope(),
            [
                (
                    block::Height(1),
                    block::Hash([1; 32]),
                    BlockSizeEstimate::Confirmed(100),
                ),
                (
                    block::Height(2),
                    block::Hash([2; 32]),
                    BlockSizeEstimate::Confirmed(100),
                ),
            ],
        );
    }

    fn take(&mut self, id: u64) -> UnpublishedRequest {
        let taken = self.work.take_request(
            block::Height(1),
            block::Height(2),
            2,
            200,
            7,
            NonZeroU64::new(id).unwrap(),
        );
        assert_eq!(taken.items().len(), 2);
        taken.admit(block::Height(0), self.cancel.clone()).unwrap()
    }

    fn expire(&mut self, owner: BodyWorkOwner) {
        self.work
            .release_reserved_and_return_items_detailed_for_owner(
                owner,
                [block::Height(1), block::Height(2)],
            );
    }

    fn reset(&mut self) {
        self.work.reset_above(block::Height(0));
    }
}

#[test]
fn batched_ownership_filter_follows_publication_receipt_and_replacement() {
    let mut f = Fixture::new();
    let claim = f.take(1);
    let owner = claim.owner();
    let candidates = |owner| {
        vec![
            (block::Height(1), (owner, 11)),
            (block::Height(2), (owner, 22)),
            (block::Height(3), (owner, 33)),
        ]
    };
    let work = f.work.clone();
    let retained = |entries: Vec<(_, (BodyWorkOwner, u8))>| {
        let disowned = work.disowned(&entries, |(owner, _)| *owner);
        entries
            .into_iter()
            .filter(|entry| !disowned.contains(entry))
            .collect::<Vec<_>>()
    };

    assert!(retained(candidates(owner)).is_empty(), "unpublished work");
    let _claim = claim.publish(|_| {}).unwrap();
    assert_eq!(
        retained(candidates(owner)),
        vec![
            (block::Height(1), (owner, 11)),
            (block::Height(2), (owner, 22)),
        ],
    );

    drop(
        f.work
            .begin_receipt_for_owner(owner, block::Height(1))
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        retained(candidates(owner)),
        vec![(block::Height(1), (owner, 11))],
        "receipt retains its owner while the unsent height returns to pending",
    );

    f.reset();
    f.refill();
    assert!(retained(candidates(owner)).is_empty(), "reset work");
    let replacement = f.take(2);
    assert!(retained(candidates(replacement.owner())).is_empty());
    let replacement = replacement.publish(|_| {}).unwrap();
    let mut mixed = candidates(owner);
    mixed.insert(0, (block::Height(2), (replacement.owner(), 22)));
    assert_eq!(
        retained(mixed),
        vec![(block::Height(2), (replacement.owner(), 22))],
        "a later stale entry for the same height must not hide its current owner",
    );
}

fn publish(claim: UnpublishedRequest, sender: &FramedSend) -> Arc<RequestWrite> {
    let slot = sender.try_reserve_guarded().unwrap();
    claim
        .publish(|claim| {
            assert!(slot.send_request(
                Frame {
                    message_type: 2,
                    flags: 0,
                    payload: vec![0; 9]
                },
                claim.clone()
            ));
        })
        .unwrap()
}

#[tokio::test]
async fn expiry_before_writer_claim_skips_the_entire_frame() {
    let mut f = Fixture::new();
    let (sender, mut receiver) = worker_framed_channel(1);
    let claim = f.take(1);
    let claim = publish(claim, &sender);
    let status = claim.status();
    f.expire(claim.owner());
    drop(claim);
    receiver
        .recv()
        .await
        .unwrap()
        .write_with(|_| async {
            panic!("an expired unwritten request must never reach QUIC");
            #[allow(unreachable_code)]
            Ok::<_, ()>(())
        })
        .await
        .unwrap();
    assert!(status.was_skipped());
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.pending_len(), 2);
    assert!(!f.cancel.is_cancelled());
}

#[tokio::test]
async fn started_request_finishes_after_expiry_without_releasing_its_replacement() {
    let mut f = Fixture::new();
    let (sender, mut receiver) = worker_framed_channel(1);
    let claim = f.take(1);
    let owner = claim.owner();
    let claim = publish(claim, &sender);
    let status = claim.status();
    drop(claim);
    let queued = receiver.recv().await.unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(queued.write_with(|_| async {
        started_tx.send(()).unwrap();
        finish_rx.await.unwrap();
        Ok::<_, ()>(())
    }));
    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .unwrap()
        .unwrap();
    f.expire(owner);
    assert!(
        !status.was_skipped(),
        "an already-started request is still written"
    );
    assert_eq!(f.budget.reserved(), 0);
    let replacement = f.take(2);
    let replacement = publish(replacement, &sender);
    assert!(
        !f.cancel.is_cancelled(),
        "expiry alone lets the started frame finish"
    );
    finish_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(f.budget.reserved(), 200);
    assert_eq!(
        f.work.owner_for_height(block::Height(1)),
        Some(replacement.owner())
    );
    drop(receiver);
    drop(replacement);
    assert_eq!(f.budget.reserved(), 0);
}

#[tokio::test]
async fn aborting_a_partial_write_cancels_the_session_and_returns_work() {
    let mut f = Fixture::new();
    let (sender, mut receiver) = worker_framed_channel(1);
    let claim = f.take(1);
    let claim = publish(claim, &sender);
    drop(claim);
    let queued = receiver.recv().await.unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(queued.write_with(|_| async {
        started_tx.send(()).unwrap();
        std::future::pending::<Result<(), ()>>().await
    }));
    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .unwrap()
        .unwrap();
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    assert!(f.cancel.is_cancelled());
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.pending_len(), 2);
}

#[test]
fn reset_before_publication_cannot_return_a_replacement_take() {
    let mut f = Fixture::new();
    let old = f.take(1);
    f.reset();
    f.refill();
    let replacement = f.take(2);
    assert!(old
        .publish(|_| panic!("reset invalidated this take"))
        .is_none());
    assert_eq!(f.budget.reserved(), 200);
    assert_eq!(f.work.in_flight_len(), 2);
    let (sender, receiver) = worker_framed_channel(1);
    let replacement = publish(replacement, &sender);
    assert_eq!(
        f.work.owner_for_height(block::Height(1)),
        Some(replacement.owner())
    );
    drop(receiver);
    drop(replacement);
    assert_eq!(f.budget.reserved(), 0);
}

#[tokio::test]
async fn queued_expiry_wakes_waiters_after_commit_settles_every_byte() {
    let mut f = Fixture::new();
    let claim = f.take(1);
    let claim = claim.publish(|_| {}).unwrap();
    assert_eq!(f.budget.reserved(), 200);
    f.work.advance_floor(block::Height(2));
    assert_eq!(f.budget.reserved(), 0);

    let available = f.work.subscribe_available().notified();
    tokio::pin!(available);
    available.as_mut().enable();
    let outcome = f
        .work
        .release_reserved_and_return_items_detailed_for_owner(claim.owner(), []);
    assert!(outcome.request_was_unwritten);
    assert_eq!(outcome.released_bytes, 0);
    assert_eq!(outcome.returned_count, 0);
    tokio::time::timeout(Duration::from_secs(1), available)
        .await
        .expect("expiring the queued frame wakes waiters even without returned work or bytes");
    assert!(!claim.try_start());
    drop(claim);
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.reserved_bytes(), 0);
}

#[test]
fn reset_cancels_a_started_request_but_only_skips_a_queued_request() {
    for started in [false, true] {
        let mut f = Fixture::new();
        let (sender, receiver) = worker_framed_channel(1);
        let claim = f.take(1);
        let claim = publish(claim, &sender);
        if started {
            assert!(claim.try_start());
        }
        f.reset();
        assert_eq!(f.cancel.is_cancelled(), started);
        assert!(!claim.try_start());
        drop(receiver);
        drop(claim);
        assert_eq!(f.budget.reserved(), 0);
    }
}

#[test]
fn queue_failure_after_receipt_preserves_the_received_height() {
    let mut f = Fixture::new();
    let (sender, receiver) = worker_framed_channel(1);
    let claim = f.take(1);
    let claim = publish(claim, &sender);
    drop(
        f.work
            .begin_receipt_for_owner(claim.owner(), block::Height(1))
            .unwrap()
            .unwrap(),
    );
    drop(receiver);
    drop(claim);
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.in_flight_len(), 1);
    assert!(!f.work.pending_contains(block::Height(1)));
    assert!(f.work.pending_contains(block::Height(2)));
}

#[test]
fn receiver_closing_after_slot_reservation_settles_publication_once() {
    let mut f = Fixture::new();
    let (sender, receiver) = worker_framed_channel(1);
    let slot = sender.try_reserve_guarded().unwrap();
    let claim = f.take(1);
    drop(receiver);
    let mut delivered = true;
    let claim = claim
        .publish(|claim| {
            delivered = slot.send_request(
                Frame {
                    message_type: 2,
                    flags: 0,
                    payload: vec![],
                },
                claim.clone(),
            );
        })
        .unwrap();
    assert!(!delivered);
    claim.delivery_failed();
    drop(claim);
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.reserved_bytes(), 0);
    assert_eq!(f.work.pending_len(), 2);
}

#[test]
fn reset_cannot_interleave_outstanding_publication_and_enqueue() {
    let mut f = Fixture::new();
    let claim = f.take(1);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel();
    let (reset_tx, reset_rx) = std::sync::mpsc::channel();
    let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
    let work = f.work.clone();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let (sender, receiver) = worker_framed_channel(1);
            let slot = sender.try_reserve_guarded().unwrap();
            let claim = claim
                .publish(|claim| {
                    entered_tx.send(()).unwrap();
                    finish_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                    assert!(slot.send_request(
                        Frame {
                            message_type: 2,
                            flags: 0,
                            payload: vec![]
                        },
                        claim.clone()
                    ));
                })
                .unwrap();
            drop(receiver);
            drop(claim);
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        scope.spawn(move || {
            attempt_tx.send(()).unwrap();
            work.reset_above(block::Height(0));
            reset_tx.send(()).unwrap();
        });
        attempt_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(
            reset_rx.recv_timeout(Duration::from_millis(20)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        finish_tx.send(()).unwrap();
        reset_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    });
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.reserved_bytes(), 0);
}

#[tokio::test]
async fn partial_queued_expiry_returns_the_whole_request_before_queue_drain() {
    for heights in [
        vec![block::Height(1)],
        vec![block::Height(2)],
        vec![block::Height(1), block::Height(2)],
    ] {
        let mut f = Fixture::new();
        let (sender, mut receiver) = worker_framed_channel(2);
        let claim = f.take(1);
        let owner = claim.owner();
        let claim = publish(claim, &sender);
        let status = claim.status();
        drop(claim);
        let outcome = f
            .work
            .release_reserved_and_return_items_detailed_for_owner(owner, heights);

        assert!(status.was_skipped());
        assert_eq!(outcome.returned_count, 2);
        assert_eq!(outcome.min_height, Some(block::Height(1)));
        assert_eq!(outcome.max_height, Some(block::Height(2)));
        assert_eq!(outcome.released_bytes, 200);
        assert_eq!(f.work.pending_len(), 2);
        assert_eq!(f.work.in_flight_len(), 0);
        assert_eq!(f.budget.reserved(), 0);
        assert_eq!(f.work.reserved_bytes(), 0);

        let replacement = f.take(2);
        let replacement = publish(replacement, &sender);
        let stale = f
            .work
            .release_reserved_and_return_items_detailed_for_owner(owner, [block::Height(1)]);
        assert_eq!(stale.released_bytes, 0);
        assert_eq!(stale.missing_count, 2);
        receiver
            .recv()
            .await
            .unwrap()
            .write_with(|_| async {
                panic!("the expired request cannot reach the transport");
                #[allow(unreachable_code)]
                Ok::<(), ()>(())
            })
            .await
            .unwrap();
        for height in [block::Height(1), block::Height(2)] {
            assert_eq!(f.work.owner_for_height(height), Some(replacement.owner()));
        }
        assert_eq!(f.budget.reserved(), 200);
        assert_eq!(f.work.reserved_bytes(), 200);
        assert!(!f.cancel.is_cancelled());
        drop(receiver);
        drop(replacement);
        assert_eq!(f.budget.reserved(), 0);
    }
}

#[test]
fn partial_started_expiry_keeps_the_other_height_reserved() {
    let mut f = Fixture::new();
    let (sender, receiver) = worker_framed_channel(1);
    let claim = f.take(1);
    let claim = publish(claim, &sender);
    assert!(claim.try_start());
    let outcome = f
        .work
        .release_reserved_and_return_items_detailed_for_owner(claim.owner(), [block::Height(1)]);

    assert!(!claim.status().was_skipped());
    assert_eq!(outcome.returned_count, 1);
    assert_eq!(outcome.released_bytes, 100);
    assert_eq!(f.work.pending_len(), 1);
    assert_eq!(
        f.work.owner_for_height(block::Height(2)),
        Some(claim.owner())
    );
    assert_eq!(f.budget.reserved(), 100);
    assert_eq!(f.work.reserved_bytes(), 100);
    assert!(!f.cancel.is_cancelled());
    claim.written();
    f.expire(claim.owner());
    drop(receiver);
    drop(claim);
    assert!(!f.cancel.is_cancelled());
    assert_eq!(f.budget.reserved(), 0);
}

#[test]
fn receipt_retires_only_unwritten_requests_and_preserves_the_body() {
    for started in [false, true] {
        for claim_received in [false, true] {
            let mut f = Fixture::new();
            let (sender, receiver) = worker_framed_channel(1);
            let claim = f.take(1);
            let claim = publish(claim, &sender);
            if started {
                assert!(claim.try_start());
            }
            let before = f.budget.reserved();
            if claim_received {
                drop(
                    f.work
                        .claim_receipt_for_owner(claim.owner(), block::Height(1))
                        .unwrap(),
                );
            } else {
                drop(
                    f.work
                        .begin_receipt_for_owner(claim.owner(), block::Height(1))
                        .unwrap()
                        .unwrap(),
                );
            }
            let released = before - f.budget.reserved();
            assert_eq!(released, if started { 100 } else { 200 });
            assert_eq!(claim.status().was_skipped(), !started);
            assert_eq!(f.budget.reserved(), if started { 100 } else { 0 });
            assert_eq!(f.work.reserved_bytes(), f.budget.reserved());
            assert_eq!(
                f.work.owner_for_height(block::Height(1)),
                Some(claim.owner())
            );
            assert!(!f.work.pending_contains(block::Height(1)));
            assert_eq!(f.work.pending_contains(block::Height(2)), !started);

            let outcome = f.work.release_reserved_and_return_items_detailed_for_owner(
                claim.owner(),
                [block::Height(2)],
            );

            assert_eq!(outcome.returned_count, u64::from(started));
            assert_eq!(outcome.released_bytes, if started { 100 } else { 0 });
            if started {
                claim.written();
            }
            drop(receiver);
            drop(claim);
            assert!(!f.cancel.is_cancelled());
            assert_eq!(f.budget.reserved(), 0);
            assert_eq!(f.work.in_flight_len(), 1);
        }
    }
}

#[test]
fn queued_expiry_racing_reset_settles_each_reservation_once() {
    for _ in 0..8 {
        let mut f = Fixture::new();
        let (sender, receiver) = worker_framed_channel(1);
        let claim = f.take(1);
        let claim = publish(claim, &sender);
        let owner = claim.owner();
        let barrier = std::sync::Barrier::new(2);
        let work = f.work.clone();
        std::thread::scope(|scope| {
            let barrier = &barrier;

            scope.spawn(move || {
                barrier.wait();
                work.reset_above(block::Height(0));
            });
            barrier.wait();
            f.work
                .release_reserved_and_return_items_detailed_for_owner(owner, [block::Height(1)]);
        });
        assert_eq!(f.budget.reserved(), 0);
        assert_eq!(f.work.reserved_bytes(), 0);
        assert_eq!(f.work.pending_len(), 0);
        assert_eq!(f.work.in_flight_len(), 0);
        drop(receiver);
        drop(claim);
        assert_eq!(f.budget.reserved(), 0);
        assert!(!f.cancel.is_cancelled());
    }
}

#[test]
fn expiry_after_forward_reset_discards_committed_heights() {
    for received in [false, true] {
        let mut f = Fixture::new();
        let (sender, receiver) = worker_framed_channel(1);
        let claim = f.take(1);
        let claim = publish(claim, &sender);
        if received {
            // Receipt retires queued requests immediately. A written request can
            // retain both a received prefix and an unreceived suffix until reset.
            assert!(claim.try_start());
            claim.written();
            drop(
                f.work
                    .begin_receipt_for_owner(claim.owner(), block::Height(1))
                    .unwrap()
                    .unwrap(),
            );
        }
        // Reset retains heights at or below its new floor. Cleanup must discard
        // that prefix instead of offering already-committed work to another peer.
        f.work.reset_above(block::Height(2));
        let outcome = f.work.release_reserved_and_return_items_detailed_for_owner(
            claim.owner(),
            [block::Height(1), block::Height(2)],
        );
        assert_eq!(outcome.committed_count, 2);
        assert_eq!(outcome.returned_count, 0);
        assert_eq!(outcome.released_bytes, if received { 100 } else { 200 });

        assert_eq!(f.work.pending_len(), 0);
        assert_eq!(f.work.in_flight_len(), 0);
        assert_eq!(f.work.reserved_bytes(), 0);
        assert_eq!(f.budget.reserved(), 0);
        drop(receiver);
        drop(claim);
        assert_eq!(f.work.pending_len(), 0);
        assert_eq!(f.budget.reserved(), 0);
        assert!(!f.cancel.is_cancelled());
    }
}

#[tokio::test]
async fn request_write_cleanup_preserves_the_recorded_transport_failure() {
    use crate::zakura::transport::{OrderedStreamFailure, OrderedStreamFailureCause};

    for failure in [
        OrderedStreamFailure::RemoteClose,
        OrderedStreamFailure::WriteTimeout,
    ] {
        let mut f = Fixture::new();
        let (sender, mut receiver) = worker_framed_channel(1);
        let claim = f.take(1);
        let claim = publish(claim, &sender);
        drop(claim);
        let cause = OrderedStreamFailureCause::default();
        let result = receiver
            .recv()
            .await
            .unwrap()
            .write_with(|_| async {
                assert!(!f.cancel.is_cancelled());
                assert_eq!(f.budget.reserved(), 200);
                // The transport records the error while the real request owner is
                // still alive. Its destructor must retain that cause through cleanup.
                cause.record(failure);
                Err::<(), _>("write failed")
            })
            .await;
        assert!(result.is_err());
        assert!(f.cancel.is_cancelled());
        assert_eq!(cause.get(), Some(failure));
        assert_eq!(f.work.pending_len(), 2);
        assert_eq!(f.work.reserved_bytes(), 0);
        assert_eq!(f.budget.reserved(), 0);
    }
}

/// Both budget-ledger gauges equal sums over the work queue's owners after
/// each charge, transfer, and release path. No check calls a publication
/// helper, so each gauge must follow its ledger on its own.
#[test]
fn budget_ledger_gauges_follow_work_owners() {
    use crate::zakura::block_sync::{
        tests::gauge_recorder::GaugeRecorder, work_queue::WorkItemPlace,
    };

    let recorder = GaugeRecorder::default();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let mut f = Fixture::new();
    // Returns the work and budget gauges after checking them against the owners.
    let check = |f: &Fixture, label: &str| {
        let owners = f.work.ownership_snapshot();
        let reserved: u64 = owners.iter().map(|item| item.reserved_bytes).sum();
        let provisional: u64 = owners
            .iter()
            .filter(|item| item.place == WorkItemPlace::Provisional)
            .map(|item| item.estimated_bytes)
            .sum();
        let work_gauge = recorder.bytes("sync.block.work.reserved_bytes");
        let budget_gauge = recorder.bytes("sync.block.budget.reserved_bytes");
        assert_eq!(work_gauge, reserved, "work gauge after {label}");
        assert_eq!(
            budget_gauge,
            reserved + provisional,
            "budget gauge after {label}"
        );
        for floor in [block::Height(0), block::Height(1)] {
            let above: u64 = owners
                .iter()
                .filter(|item| item.place == WorkItemPlace::InFlight && item.height > floor)
                .map(|item| item.reserved_bytes)
                .sum();
            assert_eq!(
                f.work.reserved_above(floor).0,
                above,
                "reservations above {floor:?} after {label}"
            );
        }
        (work_gauge, budget_gauge)
    };

    assert_eq!(check(&f, "start"), (0, 0));

    // The budget funds a provisional take before the queue charges it.
    let claim = f.take(1);
    assert_eq!(check(&f, "provisional take"), (0, 200));
    drop(claim);
    assert_eq!(check(&f, "unpublished drop"), (0, 0));

    let claim = f.take(2);
    let claim = claim.publish(|_| {}).unwrap();
    assert_eq!(check(&f, "publication"), (200, 200));
    drop(
        f.work
            .begin_receipt_for_owner(claim.owner(), block::Height(1))
            .unwrap()
            .unwrap(),
    );
    assert!(check(&f, "receipt").0 < 200);
    f.reset();
    drop(claim);
    assert_eq!(check(&f, "reset after receipt"), (0, 0));

    f.refill();
    let claim = f.take(3);
    let claim = claim.publish(|_| {}).unwrap();
    f.expire(claim.owner());
    assert_eq!(check(&f, "timeout"), (0, 0));
    drop(claim);
    assert_eq!(check(&f, "drop after timeout"), (0, 0));

    let claim = f.take(4);
    let claim = claim.publish(|_| {}).unwrap();
    f.reset();
    assert_eq!(check(&f, "reset"), (0, 0));
    drop(claim);

    f.refill();
    let claim = f.take(5);
    let claim = claim.publish(|_| {}).unwrap();
    assert_eq!(check(&f, "last publication"), (200, 200));
    drop(claim);
    assert_eq!(check(&f, "published drop"), (0, 0));
}

/// Review P5a-2: a body that answers a competing request carries the charge
/// of the request its release settles. Another routine can retire the old
/// request and publish a replacement with a new estimate between a separate
/// charge read and the release, so the charge comes from the release.
#[test]
fn answered_elsewhere_returns_the_settled_requests_charge() {
    let height = block::Height(1);
    let hash = block::Hash([1; 32]);
    let budget = ByteBudget::new(10_000);
    let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
    work.extend(
        test_work_scope(),
        [(height, hash, BlockSizeEstimate::Confirmed(2_000))],
    );
    let cancel = CancellationToken::new();
    let take = |id, bytes| {
        let taken = work.take_request(height, height, 1, 10_000, 7, NonZeroU64::new(id).unwrap());
        assert_eq!(taken.items().len(), 1);
        let request = taken.admit(block::Height(0), cancel.clone()).unwrap();
        assert_eq!(request.bytes(), bytes);
        request.publish(|_| {}).unwrap()
    };
    let old = take(1, 2_000);
    assert_eq!(work.reserved_in_flight_charge(height), Some(2_000));
    work.release_reserved_and_return_items_detailed_for_owner(old.owner(), [height]);
    work.refresh_size_estimates(
        test_work_scope(),
        [(height, hash, BlockSizeEstimate::Confirmed(1_500))],
    );
    let replacement = take(2, 1_500);
    let ended = work
        .begin_competing_receipt(replacement.owner(), height)
        .unwrap()
        .expect("replacement owns the reserved height");
    assert_eq!(ended.charge, 1_500);
    assert_eq!(ended.funding.bytes(), 1_500);
    drop(ended);
    assert_eq!(budget.reserved(), 0);
}

/// The request steps a queue reported, in order.
#[derive(Clone, Debug, Default)]
struct StepLog(Arc<std::sync::Mutex<Vec<RequestStep>>>);

impl StepLog {
    /// Takes the steps reported since the last call.
    fn take(&self) -> Vec<RequestStep> {
        std::mem::take(&mut self.0.lock().unwrap())
    }
}

impl BlockSyncObserver for StepLog {
    fn observe(&self, event: BlockEvent) {
        if let BlockEvent::Request { step, .. } = event {
            self.0.lock().unwrap().push(step);
        }
    }
}

#[test]
fn each_write_outcome_reports_its_steps() {
    use RequestStep::*;
    use RequestWritePhase as Phase;

    // Transport writes the frame.
    let (mut f, log) = Fixture::observed();
    let claim = f.take(1);
    let claim = claim.publish(|_| {}).unwrap();
    assert!(claim.try_start());
    claim.written();
    drop(claim);
    assert_eq!(
        log.take(),
        [Published, Started, Written, Dropped(Phase::Written)]
    );

    // The queue lost its receiver during publication.
    let (mut f, log) = Fixture::observed();
    let (sender, receiver) = worker_framed_channel(1);
    let slot = sender.try_reserve_guarded().unwrap();
    let claim = f.take(1);
    drop(receiver);
    let claim = claim
        .publish(|claim| {
            assert!(!slot.send_request(
                Frame {
                    message_type: 2,
                    flags: 0,
                    payload: vec![],
                },
                claim.clone(),
            ));
        })
        .unwrap();
    claim.delivery_failed();
    drop(claim);
    assert_eq!(
        log.take(),
        [Published, DeliveryFailed, Expired, Dropped(Phase::Skipped)]
    );

    // A queued request expires, so transport skips it.
    let (mut f, log) = Fixture::observed();
    let claim = f.take(1);
    let claim = claim.publish(|_| {}).unwrap();
    f.expire(claim.owner());
    assert!(!claim.try_start());
    drop(claim);
    assert_eq!(log.take(), [Published, Expired, Dropped(Phase::Skipped)]);

    // A routine expires a queued request through the status it kept, as its
    // timeout and retirement paths do.
    let (mut f, log) = Fixture::observed();
    let claim = f.take(1);
    let claim = claim.publish(|_| {}).unwrap();
    let status = claim.status();
    assert!(status.expire_unwritten());
    assert!(!status.expire_unwritten(), "only the transition reports");
    drop(claim);
    assert_eq!(log.take(), [Published, Expired, Dropped(Phase::Skipped)]);

    // A reset retires a queued request and a started one. Only the queued
    // request expires.
    for started in [false, true] {
        let (mut f, log) = Fixture::observed();
        let claim = f.take(1);
        let claim = claim.publish(|_| {}).unwrap();
        if started {
            assert!(claim.try_start());
        }
        f.reset();
        drop(claim);
        let expected: &[RequestStep] = if started {
            &[Published, Started, Reset, Dropped(Phase::Started)]
        } else {
            &[Published, Reset, Expired, Dropped(Phase::Skipped)]
        };
        assert_eq!(log.take(), expected);
    }

    // A take that never publishes drops unpublished.
    let (mut f, log) = Fixture::observed();
    drop(f.take(1));
    assert_eq!(log.take(), [Dropped(Phase::Unpublished)]);
}

#[test]
fn dropping_unpolled_publication_returns_work_and_funding() {
    let mut f = Fixture::new();
    let unrelated = f.budget.reserve_owned(17).unwrap();
    let request = f.take(1);
    let publication = async move { request.publish(|_| panic!("future was never polled")) };
    assert_eq!(f.budget.reserved(), 217);
    drop(publication);
    assert_eq!(f.budget.reserved(), 17);
    assert_eq!(f.work.pending_len(), 2);
    assert_eq!(f.work.in_flight_len(), 0);
    assert!(f.work.lock().request_writes.is_empty());
    drop(unrelated);
}

#[test]
fn cancelled_publication_returns_work_and_funding() {
    let mut f = Fixture::new();
    let unrelated = f.budget.reserve_owned(17).unwrap();
    let request = f.take(1);
    f.cancel.cancel();
    assert!(request
        .publish(|_| panic!("cancelled request cannot publish"))
        .is_none());
    assert_eq!(f.budget.reserved(), 17);
    assert_eq!(f.work.pending_len(), 2);
    assert_eq!(f.work.in_flight_len(), 0);
    assert!(f.work.lock().request_writes.is_empty());
    drop(unrelated);
}

#[test]
fn publication_unwind_settles_after_unlocking() {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut f = Fixture::new();
        let unrelated = f.budget.reserve_owned(17).unwrap();
        let request = f.take(1);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            request.publish(|_| panic!("publication interrupted"));
        }));
        assert!(result.is_err());
        assert_eq!(f.budget.reserved(), 17);
        assert_eq!(f.work.pending_len(), 2);
        assert_eq!(f.work.in_flight_len(), 0);
        assert!(f.work.lock().request_writes.is_empty());
        drop(unrelated);
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("unwind releases the work lock before claim cleanup");
    worker.join().unwrap();
}

#[test]
fn dropping_unfunded_take_returns_work_without_charging() {
    let f = Fixture::new();
    let taken = f.work.take_request(
        block::Height(1),
        block::Height(2),
        2,
        200,
        7,
        NonZeroU64::new(1).unwrap(),
    );
    let admission = async move { taken.admit(block::Height(0), CancellationToken::new()) };
    assert_eq!(f.work.in_flight_len(), 2);
    assert_eq!(f.budget.reserved(), 0);
    drop(admission);
    assert_eq!(f.work.in_flight_len(), 0);
    assert_eq!(f.work.pending_len(), 2);
    assert_eq!(f.budget.reserved(), 0);
}

#[test]
fn filtered_take_derives_funding_and_priority_from_retained_items() {
    let f = Fixture::new();
    f.work.extend(
        test_work_scope(),
        [(
            block::Height(3),
            block::Hash([3; 32]),
            BlockSizeEstimate::Confirmed(300),
        )],
    );
    let mut taken = f.work.take_request(
        block::Height(1),
        block::Height(3),
        3,
        500,
        7,
        NonZeroU64::new(1).unwrap(),
    );
    assert_eq!(taken.items().len(), 3);
    taken.retain_range(1..2);
    assert!(f.work.pending_contains(block::Height(1)));
    assert!(f.work.pending_contains(block::Height(3)));
    let request = taken.admit(block::Height(0), f.cancel.clone()).unwrap();
    assert_eq!(request.items().len(), 1);
    assert_eq!(request.items()[0].0, block::Height(2));
    assert_eq!(request.priority(), RequestPriority::AboveFloor);
    assert_eq!(request.bytes(), 100);
    assert_eq!(f.budget.reserved(), 100);
    let claim = request.publish(|_| {}).unwrap();
    assert_eq!(f.work.reserved_bytes(), 100);
    drop(claim);
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.pending_len(), 3);
}

#[test]
fn full_budget_refuses_speculation_but_funds_only_the_owned_floor() {
    let f = Fixture::new();
    let unrelated = f.budget.reserve_owned(1000).unwrap();
    let above = f.work.take_request(
        block::Height(2),
        block::Height(2),
        1,
        100,
        7,
        NonZeroU64::new(1).unwrap(),
    );
    assert!(above.admit(block::Height(0), f.cancel.clone()).is_none());
    assert!(f.work.pending_contains(block::Height(2)));
    assert_eq!(f.budget.reserved(), 1000);
    let floor = f
        .work
        .take_request(
            block::Height(1),
            block::Height(1),
            1,
            1,
            7,
            NonZeroU64::new(2).unwrap(),
        )
        .admit(block::Height(0), f.cancel.clone())
        .unwrap();
    assert_eq!(floor.bytes(), 100);
    assert_eq!(floor.priority(), RequestPriority::Floor);
    assert_eq!(f.budget.reserved(), 1100);
    let duplicate = f.work.take_request(
        block::Height(1),
        block::Height(1),
        1,
        1,
        8,
        NonZeroU64::new(3).unwrap(),
    );
    assert!(duplicate
        .admit(block::Height(0), f.cancel.clone())
        .is_none());
    assert_eq!(f.budget.reserved(), 1100);
    drop(floor);
    assert_eq!(f.budget.reserved(), 1000);
    drop(unrelated);
}

#[test]
fn stale_take_cannot_fund_or_return_replacement_work() {
    let f = Fixture::new();
    let old = f.work.take_request(
        block::Height(1),
        block::Height(2),
        2,
        200,
        7,
        NonZeroU64::new(1).unwrap(),
    );
    f.work.reset_above(block::Height(0));
    f.refill();
    let replacement = f.work.take_request(
        block::Height(1),
        block::Height(2),
        2,
        200,
        7,
        NonZeroU64::new(2).unwrap(),
    );
    assert!(old.admit(block::Height(0), f.cancel.clone()).is_none());
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.in_flight_len(), 2);
    assert_eq!(f.work.pending_len(), 0);
    let request = replacement
        .admit(block::Height(0), f.cancel.clone())
        .unwrap();
    assert_eq!(f.budget.reserved(), 200);
    drop(request);
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.pending_len(), 2);
}

#[test]
fn capacity_release_wakes_after_unlocking_the_work_queue() {
    use std::{
        future::Future,
        task::{Context, Wake, Waker},
    };
    use zakura_rt::sync::atomic::{AtomicBool, AtomicUsize};

    struct CapacityWake {
        work: Arc<WorkQueue>,
        unlocked: AtomicBool,
        count: AtomicUsize,
    }
    impl Wake for CapacityWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.unlocked
                .store(self.work.inner.try_lock().is_ok(), Ordering::SeqCst);
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    for operation in 0..5 {
        let mut f = Fixture::new();
        let request = f.take(1);
        let owner = request.owner();
        let claim = request.publish(|_| {}).unwrap();
        assert!(claim.try_start());
        claim.written();
        drop(claim);
        let wake = Arc::new(CapacityWake {
            work: f.work.clone(),
            unlocked: AtomicBool::new(false),
            count: AtomicUsize::new(0),
        });
        let waker = Waker::from(wake.clone());
        let mut context = Context::from_waker(&waker);
        let mut notified = Box::pin(f.budget.subscribe_capacity().notified());
        assert!(notified.as_mut().poll(&mut context).is_pending());
        match operation {
            0 => {
                f.work.release_reserved_heights_for_owner(
                    owner,
                    [block::Height(1), block::Height(2)],
                );
            }
            1 => {
                f.work
                    .release_and_return_items([block::Height(1), block::Height(2)]);
            }
            2 => {
                f.work.release_reserved_and_return_items_detailed_for_owner(
                    owner,
                    [block::Height(1), block::Height(2)],
                );
            }
            3 => {
                f.work.advance_floor(block::Height(2));
            }
            4 => {
                f.work.reset_above(block::Height(0));
            }
            _ => unreachable!(),
        }
        assert_eq!(f.budget.reserved(), 0, "operation {operation}");
        assert_eq!(
            wake.count.load(Ordering::SeqCst),
            1,
            "operation {operation}"
        );
        assert!(
            wake.unlocked.load(Ordering::SeqCst),
            "operation {operation} woke under the work lock"
        );
    }
}

#[test]
fn funding_drift_closes_publication_and_survives_reset() {
    let mut f = Fixture::new();
    let request = f.take(1);
    assert!(
        f.work.audit_funding(),
        "unpublished funding may exceed the ledger"
    );
    {
        let mut inner = f.work.lock();
        // Inject an untracked owner without changing the logical ledger.
        inner
            .funding
            .merge(&mut f.budget.reserve_owned(1).unwrap())
            .unwrap();
    }
    assert!(!f.work.audit_funding());
    assert!(request
        .publish(|_| panic!("closed queue published"))
        .is_none());
    // Remove the injected owner so reset exercises a consistent drained queue.
    let funding = f.work.lock().funding.split(1).unwrap();
    drop(funding);
    f.work.reset_above(block::Height(0));
    assert_eq!(f.budget.reserved(), 0);
    assert!(!f.work.audit_funding(), "reset must preserve closure");
}

#[test]
fn funding_drift_refuses_normal_and_floor_admission() {
    for floor in [block::Height(0), block::Height(1)] {
        let f = Fixture::new();
        let taken = f.work.take_request(
            block::Height(1),
            block::Height(2),
            2,
            200,
            7,
            NonZeroU64::new(1).unwrap(),
        );
        f.work
            .lock()
            .funding
            .merge(&mut f.budget.reserve_owned(1).unwrap())
            .unwrap();
        assert!(!f.work.audit_funding());
        assert!(taken.admit(floor, f.cancel.clone()).is_none());
        assert_eq!(f.budget.reserved(), 1);
        assert_eq!(f.work.pending_len(), 2);
    }
}

#[test]
fn failed_receipt_transfer_preserves_funding_and_reports_error() {
    let mut f = Fixture::new();
    let claim = f.take(1).publish(|_| {}).unwrap();
    let retained = f.work.lock().funding.split(150).unwrap();
    let outcome = f
        .work
        .begin_receipt_for_owner(claim.owner(), block::Height(1));
    assert!(matches!(
        outcome,
        Err(crate::zakura::transport::ReservationError::Amount)
    ));
    assert!(f.work.lock().admission_closed);
    assert!(f.budget.is_closed());
    assert_eq!(f.work.lock().funding.bytes(), 50);
    assert_eq!(f.budget.reserved(), 200);
    assert!(!claim.try_start());
    drop(claim);
    f.work.reset_above(block::Height(0));
    assert_eq!(
        f.budget.reserved(),
        200,
        "reset retains quarantined funding"
    );
    let budget = f.budget.clone();
    drop(f);
    assert_eq!(budget.reserved(), 150);
    drop(retained);
    assert_eq!(budget.reserved(), 0);
    assert!(budget.is_closed());
}

#[test]
fn failed_settlement_retains_pool_without_poisoning_queue() {
    for operation in 0..3 {
        let mut f = Fixture::new();
        let claim = f.take(1).publish(|_| {}).unwrap();
        assert!(claim.try_start());
        claim.written();
        let retained = f.work.lock().funding.split(150).unwrap();
        match operation {
            0 => f.work.reset_above(block::Height(0)),
            1 => f.work.advance_floor(block::Height(2)),
            2 => f
                .work
                .release_reserved_heights_for_owner(claim.owner(), [block::Height(1)]),
            _ => unreachable!(),
        }
        assert!(f.work.lock().admission_closed);
        assert!(f.budget.is_closed());
        assert_eq!(f.work.lock().funding.bytes(), 50);
        assert_eq!(f.budget.reserved(), 200);
        drop(claim);
        drop(retained);
        assert_eq!(f.budget.reserved(), 50);
    }
}

#[test]
fn rejected_publication_releases_wrong_budget_after_unlock() {
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Wake, Waker},
    };
    struct Probe(Arc<WorkQueue>, AtomicBool);
    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            self.1
                .store(self.0.inner.try_lock().is_ok(), Ordering::SeqCst);
        }
    }
    let mut f = Fixture::new();
    let mut request = f.take(1);
    let other = ByteBudget::new(1_000);
    let original = std::mem::replace(&mut request.funding, other.reserve_owned(200).unwrap());
    let probe = Arc::new(Probe(f.work.clone(), AtomicBool::new(false)));
    let waker = Waker::from(probe.clone());
    let mut context = Context::from_waker(&waker);
    let mut released = Box::pin(other.subscribe_capacity().notified());
    assert!(released.as_mut().poll(&mut context).is_pending());
    assert!(request
        .publish(|_| panic!("wrong-budget request published"))
        .is_none());
    assert!(f.work.lock().admission_closed);
    assert!(f.budget.is_closed());
    assert!(other.is_closed());
    assert_eq!(other.reserved(), 0);
    assert!(
        probe.1.load(Ordering::SeqCst),
        "rejected funding woke under the queue lock"
    );
    assert_eq!(f.budget.reserved(), 200);
    assert_eq!(f.work.reserved_bytes(), 0);
    assert_eq!(f.work.pending_len(), 2);
    drop(original);
    assert_eq!(f.budget.reserved(), 0);
}

#[test]
fn publication_amount_mismatch_quarantines_funding() {
    let mut f = Fixture::new();
    let mut request = f.take(1);
    let retained = request.funding.split(1).unwrap();
    assert!(request
        .publish(|_| panic!("underfunded request published"))
        .is_none());
    assert!(f.work.lock().admission_closed);
    assert!(f.budget.is_closed());
    assert_eq!(f.work.lock().funding.bytes(), 199);
    assert_eq!(f.budget.reserved(), 200);
    f.work.reset_above(block::Height(0));
    assert_eq!(f.budget.reserved(), 200);
    drop(retained);
    assert_eq!(f.budget.reserved(), 199);
}

#[test]
fn closed_queue_never_starts_previously_published_request() {
    for close_budget in [false, true] {
        let mut f = Fixture::new();
        let claim = f.take(1).publish(|_| {}).unwrap();
        if close_budget {
            f.budget.close();
        } else {
            f.work
                .lock()
                .funding
                .merge(&mut f.budget.reserve_owned(1).unwrap())
                .unwrap();
            assert!(!f.work.audit_funding());
        }
        assert!(!claim.try_start(), "closed owner authorized a writer");
        drop(claim);
        assert_eq!(f.budget.reserved(), if close_budget { 0 } else { 1 });
    }
}

impl WorkQueue {
    pub(in crate::zakura::block_sync) fn retain_funding_for_test(
        &self,
        bytes: u64,
    ) -> crate::zakura::transport::ByteReservation {
        self.lock().funding.split(bytes).unwrap()
    }
}
