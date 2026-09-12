use super::*;
use crate::zakura::{
    regulation::{ResponseAdmissionError, ResponseScope},
    CloseCause,
};
use tokio_util::sync::CancellationToken;

impl ResponseMemory {
    pub(crate) fn reserved_for_test(&self) -> u64 {
        self.node.reserved()
    }
}

impl ConnectionResponseMemory {
    pub(crate) fn reserved_for_test(&self) -> u64 {
        self.connection.reserved()
    }
}

#[test]
fn allocation_plans_are_admitted_as_one_reservation() {
    let node = ResponseMemory::new(4096, 4096);
    let connection = node.connection();
    let scope = ResponseScope::with_memory(
        CancellationToken::new(),
        CloseCause::new(),
        connection.clone(),
    );
    let empty = scope.authorize().unwrap();
    let fixed = node.reserved_for_test();
    drop(empty);
    assert_eq!(node.reserved_for_test(), 0);

    assert_eq!(
        scope.authorize_with_metadata(u64::MAX).unwrap_err(),
        ResponseAdmissionError::MemoryFull,
    );
    assert_eq!(node.reserved_for_test(), 0);
    let owner = scope.authorize_with_metadata(4096 - fixed).unwrap();
    let writer = owner.write_permission();
    assert_eq!(node.reserved_for_test(), 4096);
    assert_eq!(
        scope.authorize_with_metadata(1).unwrap_err(),
        ResponseAdmissionError::MemoryFull,
    );
    drop(owner);
    assert_eq!(node.reserved_for_test(), 4096);
    drop(writer);
    assert_eq!(node.reserved_for_test(), 0);
}

#[test]
fn both_limits_apply_across_messages_connections_and_clones() {
    let node = ResponseMemory::new(100, 70);
    let first = node.connection();
    let second = node.connection();
    let first_message = first.try_reserve(40).unwrap();
    let another_message = first.clone().try_reserve(30).unwrap();
    assert!(first.try_reserve(1).is_none());
    let other_connection = second.try_reserve(30).unwrap();
    assert!(second.try_reserve(1).is_none());
    assert_eq!(node.reserved_for_test(), 100);
    drop(first_message);
    assert_eq!(first.reserved_for_test(), 30);
    let replacement = second.try_reserve(40).unwrap();
    assert_eq!(node.reserved_for_test(), 100);
    drop((another_message, other_connection, replacement));
    assert_eq!(node.reserved_for_test(), 0);
    assert_eq!(first.reserved_for_test(), 0);
    assert_eq!(second.reserved_for_test(), 0);
}

#[test]
fn failed_reservations_and_counter_boundaries_do_not_leak_or_wrap() {
    let node = ResponseMemory::new(u64::MAX, u64::MAX);
    let first = node.connection();
    let other = node.connection();
    assert!(first.try_reserve(0).is_none());
    let all = first.try_reserve(u64::MAX).unwrap();
    assert!(other.try_reserve(1).is_none());
    assert_eq!(other.reserved_for_test(), 0);
    assert_eq!(first.reserved_for_test(), u64::MAX);
    drop(all);
    assert_eq!(node.reserved_for_test(), 0);
    assert!(other.try_reserve(1).is_some());
    assert_eq!(node.reserved_for_test(), 0);
}

#[test]
fn authorization_memory_outlives_endings_and_receiver_replacement() {
    for finish in [false, true] {
        let node = ResponseMemory::new(4096, 4096);
        let memory = node.connection();
        let connection = CancellationToken::new();
        let old = ResponseScope::with_memory(connection.clone(), CloseCause::new(), memory.clone());
        let mut owner = old.authorize().unwrap();
        let writer = owner.write_permission();
        let retained = node.reserved_for_test();
        assert!(retained > 0);
        let rest = memory.try_reserve(4096 - retained).unwrap();
        if finish {
            assert!(writer.publish(|| {}));
            assert!(writer.try_start(|| true));
            owner.finish();
        }
        drop(owner);
        assert!(old.retire());
        let new = ResponseScope::with_memory(connection.clone(), CloseCause::new(), memory.clone());
        assert_eq!(
            new.authorize().unwrap_err(),
            ResponseAdmissionError::MemoryFull
        );
        assert_eq!(node.reserved_for_test(), 4096);
        assert!(!connection.is_cancelled());
        drop(writer);
        assert_eq!(node.reserved_for_test(), 4096 - retained);
        let replacement = new.authorize().unwrap();
        assert_eq!(node.reserved_for_test(), 4096);
        drop((replacement, rest));
        assert_eq!(node.reserved_for_test(), 0);
    }
}

#[test]
fn connection_closure_keeps_started_writer_memory_charged() {
    let node = ResponseMemory::new(4096, 4096);
    let connection = CancellationToken::new();
    let scope =
        ResponseScope::with_memory(connection.clone(), CloseCause::new(), node.connection());
    let owner = scope.authorize().unwrap();
    let writer = owner.write_permission();
    assert!(writer.publish(|| {}));
    assert!(writer.try_start(|| true));
    drop(owner);
    assert!(connection.is_cancelled());
    let other_connection = node.connection();
    assert!(other_connection.try_reserve(4096).is_none());
    drop(writer);
    assert!(other_connection.try_reserve(4096).is_some());
}

#[tokio::test]
async fn another_connection_release_wakes_a_registered_waiter() {
    use futures::poll;
    let node = ResponseMemory::new(10, 10);
    let first = node.connection();
    let second = node.connection();
    let owner = first.try_reserve(10).unwrap();
    let changed = second.subscribe_capacity().notified();
    tokio::pin!(changed);
    changed.as_mut().enable();
    assert!(second.try_reserve(1).is_none());
    assert!(poll!(&mut changed).is_pending());
    drop(owner);
    assert!(poll!(&mut changed).is_ready());
    assert!(second.try_reserve(10).is_some());
}

#[test]
fn racing_connections_cannot_overcommit_the_node() {
    let node = ResponseMemory::new(16, 16);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(32));
    let threads: Vec<_> = (0..32)
        .map(|_| {
            let memory = node.connection();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                memory.try_reserve(1)
            })
        })
        .collect();
    let retained: Vec<_> = threads
        .into_iter()
        .filter_map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(retained.len(), 16);
    assert_eq!(node.reserved_for_test(), 16);
    drop(retained);
    assert_eq!(node.reserved_for_test(), 0);
}
