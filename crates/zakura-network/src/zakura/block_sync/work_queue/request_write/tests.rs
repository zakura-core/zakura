use super::*;
use crate::zakura::{
    block_sync::{test_work_scope, BlockSizeEstimate},
    transport::worker_framed_channel,
    Frame, FramedSend,
};
use std::{num::NonZeroU64, time::Duration};

struct Fixture {
    work: Arc<WorkQueue>,
    budget: ByteBudget,
    cancel: CancellationToken,
}

impl Fixture {
    fn new() -> Self {
        let fixture = Self {
            work: Arc::new(WorkQueue::new(block::Height(0))),
            budget: ByteBudget::new(1000),
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

    fn take(&mut self, id: u64) -> Arc<RequestWrite> {
        let items = self.work.take_for_request(
            block::Height(1),
            block::Height(2),
            2,
            200,
            7,
            NonZeroU64::new(id).unwrap(),
        );
        assert_eq!(items.len(), 2);
        assert!(self.budget.try_reserve(200));
        RequestWrite::new(
            items[0].1.owner.unwrap(),
            items,
            self.work.clone(),
            self.budget.clone(),
            self.cancel.clone(),
        )
    }

    fn expire(&mut self, owner: BodyWorkOwner) {
        let outcome = self
            .work
            .release_reserved_and_return_items_detailed_for_owner(
                owner,
                [block::Height(1), block::Height(2)],
            );
        self.budget.release(outcome.released_bytes);
    }

    fn reset(&mut self) {
        self.budget.release(self.work.reset_above(block::Height(0)));
    }
}

fn publish(claim: &Arc<RequestWrite>, sender: &FramedSend) {
    let slot = sender.try_reserve_guarded().unwrap();
    assert!(claim.publish(|| {
        assert!(slot.send_request(
            Frame {
                message_type: 2,
                flags: 0,
                payload: vec![0; 9]
            },
            claim.clone()
        ));
    }));
}

#[tokio::test]
async fn expiry_before_writer_claim_skips_the_entire_frame() {
    let mut f = Fixture::new();
    let (sender, mut receiver) = worker_framed_channel(1);
    let claim = f.take(1);
    publish(&claim, &sender);
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
    publish(&claim, &sender);
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
    assert_eq!(f.budget.reserved(), 0);
    let replacement = f.take(2);
    publish(&replacement, &sender);
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
    publish(&claim, &sender);
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
    assert!(!old.publish(|| panic!("reset invalidated this take")));
    drop(old);
    assert_eq!(f.budget.reserved(), 200);
    assert_eq!(f.work.in_flight_len(), 2);
    let (sender, receiver) = worker_framed_channel(1);
    publish(&replacement, &sender);
    assert_eq!(
        f.work.owner_for_height(block::Height(1)),
        Some(replacement.owner())
    );
    drop(receiver);
    drop(replacement);
    assert_eq!(f.budget.reserved(), 0);
}

#[test]
fn reset_cancels_a_started_request_but_only_skips_a_queued_request() {
    for started in [false, true] {
        let mut f = Fixture::new();
        let (sender, receiver) = worker_framed_channel(1);
        let claim = f.take(1);
        publish(&claim, &sender);
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
    publish(&claim, &sender);
    f.budget.release(
        f.work
            .release_active_reserved_height_for_owner(claim.owner(), block::Height(1))
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
    assert!(claim.publish(|| {
        delivered = slot.send_request(
            Frame {
                message_type: 2,
                flags: 0,
                payload: vec![],
            },
            claim.clone(),
        );
    }));
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
        let claim = claim.clone();
        scope.spawn(move || {
            let (sender, receiver) = worker_framed_channel(1);
            let slot = sender.try_reserve_guarded().unwrap();
            assert!(claim.publish(|| {
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
            }));
            drop(receiver);
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        scope.spawn(move || {
            attempt_tx.send(()).unwrap();
            reset_tx.send(work.reset_above(block::Height(0))).unwrap();
        });
        attempt_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(
            reset_rx.recv_timeout(Duration::from_millis(20)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        finish_tx.send(()).unwrap();
        f.budget
            .release(reset_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    });
    drop(claim);
    assert_eq!(f.budget.reserved(), 0);
    assert_eq!(f.work.reserved_bytes(), 0);
}
