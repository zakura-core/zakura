use std::time::Duration;

use super::*;

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
