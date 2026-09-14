//! A release must help the blocked allocation before the waiter returns.

use super::*;
use futures::poll;

#[tokio::test]
async fn a_release_before_the_first_poll_is_not_lost() {
    for local in [false, true] {
        let node = pool(2 * CONNECTION_SETUP_BYTES + 10, CONNECTION_SETUP_BYTES + 10);
        let connection = node.connection();
        let other = node.connection();
        let blocker = if local { &connection } else { &other };
        let held = blocker.try_reserve(10).unwrap();
        assert!(connection.try_reserve(10).is_none());
        let changed = connection.wait_for_capacity(10);
        tokio::pin!(changed);
        drop(held);
        assert!(poll!(&mut changed).is_ready());
        assert!(connection.try_reserve(10).is_some());
    }
}

#[tokio::test]
async fn a_small_release_does_not_finish_waiting_for_a_larger_allocation() {
    let node = pool(CONNECTION_SETUP_BYTES + 100, CONNECTION_SETUP_BYTES + 10);
    let connection = node.connection();
    let large = connection.try_reserve(8).unwrap();
    let small = connection.try_reserve(2).unwrap();
    let changed = connection.wait_for_capacity(8);
    tokio::pin!(changed);
    assert!(poll!(&mut changed).is_pending());
    drop(small);
    assert!(poll!(&mut changed).is_pending());
    drop(large);
    assert!(poll!(&mut changed).is_ready());
    assert!(connection.try_reserve(8).is_some());
}

#[tokio::test]
async fn a_connection_wait_switches_to_the_node_when_its_own_capacity_returns() {
    let node = pool(2 * CONNECTION_SETUP_BYTES + 15, CONNECTION_SETUP_BYTES + 10);
    let connection = node.connection();
    let other = node.connection();
    let local = connection.try_reserve(10).unwrap();
    let remote = other.try_reserve(5).unwrap();
    let changed = connection.wait_for_capacity(8);
    tokio::pin!(changed);
    assert!(poll!(&mut changed).is_pending());
    drop(local);
    // The connection now has ten free bytes, but another service leaves the
    // node with only five. The waiter must switch to node capacity.
    let competing_service = other.try_reserve(5).unwrap();
    assert!(poll!(&mut changed).is_pending());
    drop(competing_service);
    assert!(poll!(&mut changed).is_ready());
    assert!(connection.try_reserve(8).is_some());
    drop(remote);
}

#[tokio::test]
async fn a_node_wait_switches_to_the_connection_when_local_capacity_is_spent() {
    let node = pool(2 * CONNECTION_SETUP_BYTES + 12, CONNECTION_SETUP_BYTES + 10);
    let connection = node.connection();
    let other = node.connection();
    let remote = other.try_reserve(10).unwrap();
    let changed = connection.wait_for_capacity(9);
    tokio::pin!(changed);
    assert!(poll!(&mut changed).is_pending());
    let local = connection.try_reserve(2).unwrap();
    drop(remote);
    assert!(poll!(&mut changed).is_pending());
    drop(local);
    assert!(poll!(&mut changed).is_ready());
    assert!(connection.try_reserve(9).is_some());
}

#[tokio::test]
async fn checking_or_cancelling_a_capacity_wait_does_not_allocate() {
    use std::{future::Future, task::Context};
    use zakura_test::allocations::measure;

    let node = pool(CONNECTION_SETUP_BYTES + 10, CONNECTION_SETUP_BYTES + 10);
    let connection = node.connection();
    let held = connection.try_reserve(10).unwrap();
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    let (_, allocations) = measure(|| {
        let changed = connection.wait_for_capacity(10);
        tokio::pin!(changed);
        assert!(changed.as_mut().poll(&mut context).is_pending());
    });
    assert_eq!(allocations.requested_bytes, 0);
    drop(held);
    assert!(connection.try_reserve(10).is_some());
}
