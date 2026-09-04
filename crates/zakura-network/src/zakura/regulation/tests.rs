use std::time::Duration;

use proptest::prelude::*;

use super::*;
use crate::zakura::testkit::TestClock;

#[test]
fn provisional_rate_charge_refunds_on_drop() {
    let bucket = ByteRateBucket::with_clock(100, 10, TestClock::new());
    let charge = bucket
        .try_charge(80)
        .expect("the full bucket covers 80 bytes");

    assert_eq!(bucket.balance(), 20);
    drop(charge);
    assert_eq!(bucket.balance(), 100);
}

#[test]
fn committed_rate_charge_keeps_overhead_and_usage_spent() {
    let bucket = ByteRateBucket::with_clock(100, 10, TestClock::new());
    let charge = bucket
        .try_charge(80)
        .expect("the full bucket covers 80 bytes");
    let mut committed = charge
        .commit(20)
        .expect("the overhead is smaller than the original charge");
    committed
        .record_usage(30)
        .expect("queued bytes fit in the refundable response charge");

    committed.settle();

    assert_eq!(bucket.balance(), 50);
}

#[test]
fn rate_refill_preserves_fractional_time() {
    let clock = TestClock::new();
    let bucket = ByteRateBucket::with_clock(10, 3, clock.clone());
    let charge = bucket
        .try_charge(10)
        .expect("the full bucket covers its capacity");
    let committed = charge
        .commit(10)
        .expect("the complete charge can become non-refundable");
    committed.settle();

    clock.advance(Duration::from_millis(333));
    assert_eq!(bucket.balance(), 0);
    clock.advance(Duration::from_millis(1));
    assert_eq!(bucket.balance(), 1);
}

#[tokio::test(start_paused = true)]
async fn rate_wait_wakes_when_a_charge_refunds_before_refill() {
    let bucket = ByteRateBucket::new(100, 1);
    let charge = bucket
        .try_charge(100)
        .expect("the full bucket covers its capacity");
    let waiting_bucket = bucket.clone();
    let waiter = tokio::spawn(async move { waiting_bucket.wait_for(50).await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    drop(charge);

    tokio::task::yield_now().await;
    assert!(waiter.is_finished());
    waiter
        .await
        .expect("the waiter task should not panic")
        .expect("the refunded charge fits the bucket");
}

#[tokio::test(start_paused = true)]
async fn rate_wait_wakes_at_the_refill_deadline() {
    let bucket = ByteRateBucket::new(100, 10);
    let charge = bucket
        .try_charge(100)
        .expect("the full bucket covers its capacity");
    charge
        .commit(100)
        .expect("the complete charge can become non-refundable")
        .settle();
    let waiting_bucket = bucket.clone();
    let waiter = tokio::spawn(async move { waiting_bucket.wait_for(50).await });
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
        .expect("five seconds refill fifty bytes");
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

    let rate = ByteRateBucket::new(10, 1);
    assert_eq!(
        rate.wait_for(11).await,
        Err(RateChargeError::ExceedsCapacity {
            requested: 11,
            capacity: 10,
        })
    );
}

#[tokio::test]
async fn outstanding_wait_does_not_miss_a_concurrent_release() {
    let budget = OutstandingByteBudget::new(10);
    let reservation = budget
        .try_reserve_owned(10)
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
        .try_reserve_owned(10)
        .expect("the request fits the budget")
        .expect("the empty budget admits the reservation");

    tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;

    assert_eq!(budget.available(), 0);
    reservation.release_remaining();
    assert_eq!(budget.available(), 10);
}

#[test]
fn one_frame_lease_releases_every_charged_budget() {
    let node = OutstandingByteBudget::new(100);
    let peer = OutstandingByteBudget::new(100);
    let mut node_reservation = node
        .try_reserve_owned(100)
        .expect("the request fits the node budget")
        .expect("the node budget is empty");
    let mut peer_reservation = peer
        .try_reserve_owned(100)
        .expect("the request fits the peer budget")
        .expect("the peer budget is empty");

    let lease = OutstandingByteReservation::transfer_all(
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
        .try_reserve_owned(100)
        .expect("the request fits the first budget")
        .expect("the first budget is empty");
    let mut second_reservation = second
        .try_reserve_owned(50)
        .expect("the request fits the second budget")
        .expect("the second budget is empty");

    assert!(OutstandingByteReservation::transfer_all(
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
fn pending_input_permits_bound_and_release_retained_items() {
    let budget = PendingInputBudget::new(2).expect("two permits fit the semaphore");
    let first = budget.try_reserve().expect("the first slot is available");
    let second = budget.try_reserve().expect("the second slot is available");
    assert_eq!(budget.capacity(), 2);
    assert_eq!(budget.reserved(), 2);
    assert!(budget.try_reserve().is_none());

    drop(first);
    assert_eq!(budget.reserved(), 1);
    let replacement = budget
        .try_reserve()
        .expect("dropping an owner releases its slot");
    assert_eq!(budget.reserved(), 2);

    drop((second, replacement));
    assert_eq!(budget.reserved(), 0);
}

proptest! {
    #[test]
    fn arbitrary_pending_input_histories_never_exceed_capacity(
        capacity in 0_usize..=64,
        operations in prop::collection::vec((any::<bool>(), any::<u16>()), 1..256),
    ) {
        let budget = PendingInputBudget::new(capacity)
            .expect("the generated capacity is below the semaphore maximum");
        let mut permits = Vec::<PendingInputPermit>::new();

        for (reserve, selector) in operations {
            if reserve {
                if let Some(permit) = budget.try_reserve() {
                    permits.push(permit);
                }
            } else if !permits.is_empty() {
                let index = usize::from(selector) % permits.len();
                drop(permits.swap_remove(index));
            }

            prop_assert_eq!(budget.reserved(), permits.len());
            prop_assert!(budget.reserved() <= budget.capacity());
        }

        drop(permits);
        prop_assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn arbitrary_outstanding_ownership_histories_conserve_bytes(
        operations in prop::collection::vec((0_u8..4, 0_u16..1_024), 1..256),
    ) {
        let budget = OutstandingByteBudget::new(4_096);
        let mut reservations = Vec::<OutstandingByteReservation>::new();
        let mut leases = Vec::<FrameLease>::new();

        for (operation, value) in operations {
            match operation {
                0 => {
                    if let Ok(Some(reservation)) = budget.try_reserve_owned(u64::from(value)) {
                        reservations.push(reservation);
                    }
                }
                1 if !reservations.is_empty() => {
                    let index = usize::from(value) % reservations.len();
                    let bytes = u64::from(value).min(reservations[index].remaining());
                    if let Some(lease) = OutstandingByteReservation::transfer_all(
                        [&mut reservations[index]],
                        bytes,
                    ) {
                        leases.push(lease);
                    }
                }
                2 if !reservations.is_empty() => {
                    let index = usize::from(value) % reservations.len();
                    drop(reservations.swap_remove(index));
                }
                3 if !leases.is_empty() => {
                    let index = usize::from(value) % leases.len();
                    drop(leases.swap_remove(index));
                }
                _ => {}
            }

            let expected_reservations: u64 = reservations
                .iter()
                .map(OutstandingByteReservation::remaining)
                .sum();
            let expected_leases: u64 = leases.iter().map(FrameLease::accounted_bytes).sum();
            prop_assert_eq!(budget.reserved(), expected_reservations + expected_leases);
            prop_assert!(budget.reserved() <= budget.capacity());
        }

        drop(reservations);
        drop(leases);
        prop_assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn arbitrary_rate_ownership_histories_refund_exactly(
        operations in prop::collection::vec((0_u8..5, 0_u16..1_024), 1..256),
    ) {
        const CAPACITY: u64 = 4_096;

        let bucket = ByteRateBucket::with_clock(CAPACITY, 1_024, TestClock::new());
        let mut provisional = Vec::<(RateCharge<TestClock>, u64)>::new();
        let mut committed = Vec::<(CommittedRateCharge<TestClock>, u64)>::new();
        let mut permanently_spent = 0_u64;

        for (operation, value) in operations {
            match operation {
                0 => {
                    let bytes = u64::from(value);
                    if let Ok(charge) = bucket.try_charge(bytes) {
                        provisional.push((charge, bytes));
                    }
                }
                1 if !provisional.is_empty() => {
                    let index = usize::from(value) % provisional.len();
                    drop(provisional.swap_remove(index));
                }
                2 if !provisional.is_empty() => {
                    let index = usize::from(value) % provisional.len();
                    let (charge, charged) = provisional.swap_remove(index);
                    let overhead = u64::from(value) % charged.saturating_add(1);
                    let refundable = charged - overhead;
                    permanently_spent += overhead;
                    committed.push((
                        charge
                            .commit(overhead)
                            .expect("the generated overhead never exceeds the charge"),
                        refundable,
                    ));
                }
                3 if !committed.is_empty() => {
                    let index = usize::from(value) % committed.len();
                    let (charge, refundable) = &mut committed[index];
                    let used = u64::from(value) % refundable.saturating_add(1);
                    charge
                        .record_usage(used)
                        .expect("the generated usage never exceeds the refundable remainder");
                    *refundable -= used;
                    permanently_spent += used;
                }
                4 if !committed.is_empty() => {
                    let index = usize::from(value) % committed.len();
                    drop(committed.swap_remove(index));
                }
                _ => {}
            }

            let provisionally_held: u64 = provisional.iter().map(|(_, bytes)| bytes).sum();
            let committed_refundable: u64 = committed.iter().map(|(_, bytes)| bytes).sum();
            prop_assert_eq!(
                bucket.balance(),
                CAPACITY - permanently_spent - provisionally_held - committed_refundable,
            );
        }
    }

    #[test]
    fn arbitrary_rate_histories_keep_balances_bounded(
        operations in prop::collection::vec((0_u8..5, 0_u16..1_024), 1..256),
    ) {
        let clock = TestClock::new();
        let bucket = ByteRateBucket::with_clock(4_096, 1_024, clock.clone());
        let mut provisional = Vec::<RateCharge<TestClock>>::new();
        let mut committed = Vec::<CommittedRateCharge<TestClock>>::new();

        for (operation, value) in operations {
            match operation {
                0 => {
                    if let Ok(charge) = bucket.try_charge(u64::from(value)) {
                        provisional.push(charge);
                    }
                }
                1 if !provisional.is_empty() => {
                    let index = usize::from(value) % provisional.len();
                    drop(provisional.swap_remove(index));
                }
                2 if !provisional.is_empty() => {
                    let index = usize::from(value) % provisional.len();
                    let charge = provisional.swap_remove(index);
                    let overhead = u64::from(value).min(charge.charged());
                    committed.push(
                        charge
                            .commit(overhead)
                            .expect("the generated overhead is clamped to the charge"),
                    );
                }
                3 if !committed.is_empty() => {
                    let index = usize::from(value) % committed.len();
                    let bytes = u64::from(value).min(committed[index].refundable());
                    committed[index]
                        .record_usage(bytes)
                        .expect("the generated usage is clamped to the refundable remainder");
                }
                4 => clock.advance(Duration::from_millis(u64::from(value))),
                _ => {}
            }

            prop_assert!(bucket.balance() <= bucket.capacity());
        }

        drop(provisional);
        drop(committed);
        prop_assert!(bucket.balance() <= bucket.capacity());
    }
}
