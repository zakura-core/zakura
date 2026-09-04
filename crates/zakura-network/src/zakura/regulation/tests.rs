use std::time::Duration;

use super::*;
use crate::zakura::testkit::TestClock;

#[test]
fn rate_budget_rejects_configuration_that_cannot_refill() {
    assert_eq!(
        RateBudget::with_clock(0, 1, TestClock::new()).unwrap_err(),
        RateBudgetConfigError::ZeroCapacity,
    );
    assert_eq!(
        RateBudget::with_clock(1, 0, TestClock::new()).unwrap_err(),
        RateBudgetConfigError::ZeroRefill,
    );
}

#[test]
fn provisional_rate_reservation_returns_all_units_on_drop() {
    let budget = RateBudget::with_clock(100, 10, TestClock::new())
        .expect("the rate budget configuration is valid");
    let reservation = budget
        .try_reserve(80)
        .expect("the full budget covers eighty units");

    assert_eq!(budget.available(), 20);
    drop(reservation);
    assert_eq!(budget.available(), 100);
}

#[test]
fn committed_rate_reservation_keeps_consumed_units_spent() {
    let budget = RateBudget::with_clock(100, 10, TestClock::new())
        .expect("the rate budget configuration is valid");
    let reservation = budget
        .try_reserve(80)
        .expect("the full budget covers eighty units");
    let mut committed = reservation
        .commit(20)
        .expect("the initial spend fits the reservation");
    committed
        .spend(30)
        .expect("completed work fits the refundable remainder");

    committed.finish();

    assert_eq!(budget.available(), 50);
}

#[test]
fn rate_refill_preserves_fractional_time() {
    let clock = TestClock::new();
    let budget = RateBudget::with_clock(10, 3, clock.clone())
        .expect("the rate budget configuration is valid");
    budget
        .try_reserve(10)
        .expect("the full budget covers its capacity")
        .commit(10)
        .expect("the full reservation can be spent")
        .finish();

    clock.advance(Duration::from_millis(333));
    assert_eq!(budget.available(), 0);
    clock.advance(Duration::from_millis(1));
    assert_eq!(budget.available(), 1);
}

#[tokio::test(start_paused = true)]
async fn rate_wait_wakes_when_a_reservation_is_returned() {
    let budget = RateBudget::new(100, 1).expect("the rate budget configuration is valid");
    let reservation = budget
        .try_reserve(100)
        .expect("the full budget covers its capacity");
    let waiting_budget = budget.clone();
    let waiter = tokio::spawn(async move { waiting_budget.wait_for(50).await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    drop(reservation);

    tokio::task::yield_now().await;
    waiter
        .await
        .expect("the waiter task should not panic")
        .expect("the returned reservation fits the budget");
}

#[tokio::test(start_paused = true)]
async fn rate_wait_wakes_at_the_refill_deadline() {
    let budget = RateBudget::new(100, 10).expect("the rate budget configuration is valid");
    budget
        .try_reserve(100)
        .expect("the full budget covers its capacity")
        .commit(100)
        .expect("the full reservation can be spent")
        .finish();
    let waiting_budget = budget.clone();
    let waiter = tokio::spawn(async move { waiting_budget.wait_for(50).await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    waiter
        .await
        .expect("the waiter task should not panic")
        .expect("five seconds refill fifty units");
}

#[tokio::test]
async fn impossible_waits_return_capacity_errors() {
    let outstanding = OutstandingByteBudget::new(10);
    assert_eq!(
        outstanding.wait_for(11).await,
        Err(OutstandingCapacityError {
            requested: 11,
            capacity: 10,
        })
    );

    let rate = RateBudget::new(10, 1).expect("the rate budget configuration is valid");
    assert_eq!(
        rate.wait_for(11).await,
        Err(RateReservationError::ExceedsCapacity {
            requested: 11,
            capacity: 10,
        })
    );
}

#[tokio::test]
async fn outstanding_wait_does_not_miss_a_concurrent_release() {
    let budget = OutstandingByteBudget::new(10);
    let reservation = budget
        .try_reserve(10)
        .expect("the request fits the budget")
        .expect("the empty budget admits the reservation");
    let waiting_budget = budget.clone();
    let waiter = tokio::spawn(async move { waiting_budget.wait_for(1).await });
    tokio::task::yield_now().await;

    drop(reservation);

    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("the release must wake the waiter")
        .expect("the waiter task should not panic")
        .expect("one byte fits after release");
}

#[tokio::test(start_paused = true)]
async fn outstanding_budget_does_not_refill_with_time() {
    let budget = OutstandingByteBudget::new(10);
    let reservation = budget
        .try_reserve(10)
        .expect("the request fits the budget")
        .expect("the empty budget admits the reservation");

    tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;

    assert_eq!(budget.available(), 0);
    reservation.release();
    assert_eq!(budget.available(), 10);
}

#[test]
fn frame_lease_holds_peer_and_node_bytes() {
    let node = OutstandingByteBudget::new(100);
    let peer = OutstandingByteBudget::new(100);
    let mut node_reservation = node
        .try_reserve(100)
        .expect("the request fits the node budget")
        .expect("the node budget is empty");
    let mut peer_reservation = peer
        .try_reserve(100)
        .expect("the request fits the peer budget")
        .expect("the peer budget is empty");

    let lease = OutstandingByteReservation::transfer_to_frame(
        [&mut node_reservation, &mut peer_reservation],
        60,
    )
    .expect("both reservations cover the frame");
    assert_eq!(lease.accounted_bytes(), 60);

    drop(node_reservation);
    drop(peer_reservation);
    assert_eq!(node.reserved(), 60);
    assert_eq!(peer.reserved(), 60);

    drop(lease);
    assert_eq!(node.reserved(), 0);
    assert_eq!(peer.reserved(), 0);
}

#[test]
fn failed_multi_budget_transfer_changes_nothing() {
    let first = OutstandingByteBudget::new(100);
    let second = OutstandingByteBudget::new(50);
    let mut first_reservation = first
        .try_reserve(100)
        .expect("the request fits the first budget")
        .expect("the first budget is empty");
    let mut second_reservation = second
        .try_reserve(50)
        .expect("the request fits the second budget")
        .expect("the second budget is empty");

    assert!(OutstandingByteReservation::transfer_to_frame(
        [&mut first_reservation, &mut second_reservation],
        60,
    )
    .is_none());
    assert_eq!(first_reservation.remaining(), 100);
    assert_eq!(second_reservation.remaining(), 50);
    assert_eq!(first.reserved(), 100);
    assert_eq!(second.reserved(), 50);
}

#[test]
fn slot_permits_bound_and_release_owned_items() {
    let budget = SlotBudget::new(2).expect("two slots fit the semaphore");
    let first = budget.try_reserve().expect("the first slot is available");
    let second = budget.try_reserve().expect("the second slot is available");
    assert_eq!(budget.capacity(), 2);
    assert_eq!(budget.reserved(), 2);
    assert!(budget.try_reserve().is_none());

    drop(first);
    let replacement = budget
        .try_reserve()
        .expect("dropping an owner releases its slot");
    assert_eq!(budget.reserved(), 2);

    drop((second, replacement));
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn slot_budget_rejects_unusable_capacities() {
    assert!(SlotBudget::new(0).is_err());
    assert!(SlotBudget::new(tokio::sync::Semaphore::MAX_PERMITS + 1).is_err());
}

#[tokio::test]
async fn slot_waiters_receive_and_retain_owned_capacity() {
    use futures::poll;

    let budget = SlotBudget::new(1).expect("one slot fits the semaphore");
    let reservation = budget.try_reserve().expect("the slot is initially free");
    let first = budget.reserve();
    let second = budget.reserve();
    tokio::pin!(first, second);
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut second).is_pending());

    drop(reservation);
    let first_permit = tokio::time::timeout(Duration::from_secs(1), &mut first)
        .await
        .expect("the first waiter receives the released slot");
    assert_eq!(budget.reserved(), 1);
    assert!(budget.try_reserve().is_none());
    assert!(poll!(&mut second).is_pending());

    drop(first_permit);
    let second_permit = tokio::time::timeout(Duration::from_secs(1), &mut second)
        .await
        .expect("the second waiter receives the next released slot");
    assert_eq!(budget.reserved(), 1);
    drop(second_permit);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test]
async fn cancelled_slot_waiter_leaves_capacity_for_its_successor() {
    use futures::poll;

    let budget = SlotBudget::new(1).expect("one slot fits the semaphore");
    let owner = budget.try_reserve().expect("the slot is initially free");
    let mut cancelled = Box::pin(budget.reserve());
    let mut successor = Box::pin(budget.reserve());
    assert!(poll!(&mut cancelled).is_pending());
    assert!(poll!(&mut successor).is_pending());
    drop(cancelled);
    drop(owner);
    let permit = tokio::time::timeout(Duration::from_secs(1), successor)
        .await
        .expect("cancelling the first waiter does not strand the next one");
    assert_eq!(budget.reserved(), 1);
    drop(permit);
    assert_eq!(budget.reserved(), 0);
}
