//! Check shared pool limits, setup charges, owner lifetimes and wakeups.

use super::*;
use crate::zakura::{
    regulation::{ExchangeOpenError, WriterFence},
    CloseCause,
};
use tokio_util::sync::CancellationToken;

mod allocations;
mod wakeups;

impl ResponseMemory {
    pub(crate) fn connection(&self) -> ConnectionResponseMemory {
        self.try_connection()
            .expect("the fixture funds connection setup")
    }

    pub(crate) fn node_setup_bytes_for_test() -> u64 {
        NODE_SETUP_BYTES
    }

    pub(crate) fn setup_bytes_for_test() -> u64 {
        CONNECTION_SETUP_BYTES
    }

    pub(crate) fn reserved_for_test(&self) -> u64 {
        self.node.reserved()
    }
}

impl ConnectionResponseMemory {
    pub(crate) fn reserved_for_test(&self) -> u64 {
        self.0.connection.reserved()
    }
}

fn pool(variable_bytes: u64, connection_bytes: u64) -> ResponseMemory {
    ResponseMemory::new(NODE_SETUP_BYTES + variable_bytes, connection_bytes)
}

#[test]
fn fixed_setup_is_funded_until_the_last_connection_owner_exits() {
    let node = pool(CONNECTION_SETUP_BYTES, CONNECTION_SETUP_BYTES);
    let connection = node.connection();
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + CONNECTION_SETUP_BYTES
    );
    assert_eq!(connection.reserved_for_test(), CONNECTION_SETUP_BYTES);
    assert!(node.try_connection().is_none());
    assert!(connection.try_reserve(1).is_none());
    let last = connection.clone();
    drop(connection);
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + CONNECTION_SETUP_BYTES
    );
    drop(last);
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
    assert!(node.try_connection().is_some());
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
    assert!(pool(CONNECTION_SETUP_BYTES, CONNECTION_SETUP_BYTES - 1)
        .try_connection()
        .is_none());
    assert!(pool(CONNECTION_SETUP_BYTES - 1, CONNECTION_SETUP_BYTES)
        .try_connection()
        .is_none());
}

#[test]
fn pool_setup_survives_the_endpoint_handle_until_connections_exit() {
    let node = ResponseMemory::default();
    let setup_owner = Arc::downgrade(&node._setup);
    let connection = node.connection();
    drop(node);
    assert!(setup_owner.upgrade().is_some());
    assert_eq!(
        connection.0.pool.reserved_for_test(),
        NODE_SETUP_BYTES + CONNECTION_SETUP_BYTES
    );
    drop(connection);
    assert!(setup_owner.upgrade().is_none());
}

#[test]
fn failed_scope_setup_preserves_the_existing_receiver() {
    let setup = CONNECTION_SETUP_BYTES + WriterFence::setup_bytes_for_test();
    let node = pool(setup + 4096, setup + 4096);
    let connection = node.connection();
    let cancel = CancellationToken::new();
    let cause = CloseCause::new();
    let scope = WriterFence::with_memory(cancel.clone(), cause.clone(), connection.clone());
    let rest = connection.try_reserve(4096).unwrap();
    assert_eq!(
        WriterFence::try_with_memory(&cancel, &cause, connection.clone()).unwrap_err(),
        ExchangeOpenError::MemoryFull,
    );
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES + setup + 4096);
    assert!(!cancel.is_cancelled());
    drop(rest);
    let mut owner = scope.try_open().unwrap();
    let writer = owner.writer();
    assert!(writer.publish(|| {}));
    assert!(writer.try_start(|| true));
    owner.end();
    drop((scope, owner, writer));
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + CONNECTION_SETUP_BYTES
    );
    drop(connection);
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
}

#[test]
fn jointly_admitted_storage_outlives_the_exchange_and_scope() {
    let node = ResponseMemory::default();
    let connection = node.connection();
    let baseline = node.reserved_for_test();
    let scope = WriterFence::with_memory(CancellationToken::new(), CloseCause::new(), connection);
    let (mut authorization, retained) = scope.try_open_with_retained_memory(128, 256).unwrap();
    let writer = authorization.writer();
    assert!(writer.publish(|| {}));
    assert!(writer.try_start(|| true));
    authorization.end();
    drop((authorization, writer, scope));
    assert_eq!(node.reserved_for_test(), baseline + 256);
    drop(retained);
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
}

#[test]
fn retained_growth_and_request_admission_are_atomic() {
    let setup = CONNECTION_SETUP_BYTES + WriterFence::setup_bytes_for_test();
    let node = pool(setup + 4096, setup + 4096);
    let connection = node.connection();
    let scope = WriterFence::with_memory(CancellationToken::new(), CloseCause::new(), connection);
    let before = node.reserved_for_test();
    assert!(matches!(
        scope.try_open_with_retained_memory(1, 4096),
        Err(ExchangeOpenError::MemoryFull)
    ));
    assert!(matches!(
        scope.try_open_with_retained_memory(1, u64::MAX),
        Err(ExchangeOpenError::MemoryFull)
    ));
    assert_eq!(node.reserved_for_test(), before);
    assert!(scope.try_open_with_retained_memory(1, 2048).is_ok());
    assert_eq!(node.reserved_for_test(), before);
}

#[test]
fn allocation_plans_are_admitted_as_one_reservation() {
    let setup = CONNECTION_SETUP_BYTES + WriterFence::setup_bytes_for_test();
    let limit = setup + 4096;
    let node = pool(limit, limit);
    let connection = node.connection();
    let scope = WriterFence::with_memory(
        CancellationToken::new(),
        CloseCause::new(),
        connection.clone(),
    );
    let empty = scope.try_open().unwrap();
    let fixed = node.reserved_for_test() - NODE_SETUP_BYTES - setup;
    drop(empty);
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES + setup);
    assert_eq!(
        scope.open_with_metadata(u64::MAX).unwrap_err(),
        ExchangeOpenError::MemoryFull
    );
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES + setup);
    let owner = scope.open_with_metadata(4096 - fixed).unwrap();
    let writer = owner.writer();
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES + limit);
    assert_eq!(
        scope.open_with_metadata(1).unwrap_err(),
        ExchangeOpenError::MemoryFull
    );
    drop(owner);
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES + limit);
    drop(writer);
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES + setup);
    drop(scope);
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + CONNECTION_SETUP_BYTES
    );
    drop(connection);
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
}

#[test]
fn both_limits_apply_across_messages_connections_and_clones() {
    let node = pool(
        2 * CONNECTION_SETUP_BYTES + 100,
        CONNECTION_SETUP_BYTES + 70,
    );
    let first = node.connection();
    let second = node.connection();
    let first_message = first.try_reserve(40).unwrap();
    let another_message = first.clone().try_reserve(30).unwrap();
    assert!(first.try_reserve(1).is_none());
    let other_connection = second.try_reserve(30).unwrap();
    assert!(second.try_reserve(1).is_none());
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + 2 * CONNECTION_SETUP_BYTES + 100
    );
    drop(first_message);
    assert_eq!(first.reserved_for_test(), CONNECTION_SETUP_BYTES + 30);
    let replacement = second.try_reserve(40).unwrap();
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + 2 * CONNECTION_SETUP_BYTES + 100
    );
    drop((another_message, other_connection, replacement));
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + 2 * CONNECTION_SETUP_BYTES
    );
    assert_eq!(first.reserved_for_test(), CONNECTION_SETUP_BYTES);
    assert_eq!(second.reserved_for_test(), CONNECTION_SETUP_BYTES);
    drop((first, second));
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
}

#[test]
fn failed_reservations_and_counter_boundaries_do_not_leak_or_wrap() {
    let node = ResponseMemory::new(u64::MAX, u64::MAX);
    let first = node.connection();
    let other = node.connection();
    assert!(first.try_reserve(0).is_none());
    let all = first
        .try_reserve(u64::MAX - NODE_SETUP_BYTES - 2 * CONNECTION_SETUP_BYTES)
        .unwrap();
    assert!(other.try_reserve(1).is_none());
    assert_eq!(node.reserved_for_test(), u64::MAX);
    assert_eq!(other.reserved_for_test(), CONNECTION_SETUP_BYTES);
    assert_eq!(
        first.reserved_for_test(),
        u64::MAX - NODE_SETUP_BYTES - CONNECTION_SETUP_BYTES
    );
    drop(all);
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + 2 * CONNECTION_SETUP_BYTES
    );
    assert!(other.try_reserve(1).is_some());
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + 2 * CONNECTION_SETUP_BYTES
    );
    drop((first, other));
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
}

#[test]
fn authorization_memory_outlives_endings_and_receiver_replacement() {
    for finish in [false, true] {
        let setup = CONNECTION_SETUP_BYTES + 2 * WriterFence::setup_bytes_for_test();
        let limit = setup + 4096;
        let node = pool(limit, limit);
        let memory = node.connection();
        let connection = CancellationToken::new();
        let old = WriterFence::with_memory(connection.clone(), CloseCause::new(), memory.clone());
        let new = WriterFence::with_memory(connection.clone(), CloseCause::new(), memory.clone());
        let mut owner = old.try_open().unwrap();
        let writer = owner.writer();
        let retained = node.reserved_for_test() - NODE_SETUP_BYTES - setup;
        assert!(retained > 0);
        let rest = memory.try_reserve(4096 - retained).unwrap();
        if finish {
            assert!(writer.publish(|| {}));
            assert!(writer.try_start(|| true));
            owner.end();
        }
        drop(owner);
        assert!(old.retire());
        assert_eq!(new.try_open().unwrap_err(), ExchangeOpenError::MemoryFull);
        assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES + limit);
        assert!(!connection.is_cancelled());
        drop(writer);
        assert_eq!(
            node.reserved_for_test(),
            NODE_SETUP_BYTES + limit - retained
        );
        let replacement = new.try_open().unwrap();
        assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES + limit);
        drop((replacement, rest));
        assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES + setup);
        drop((old, new, memory));
        assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
    }
}

#[test]
fn connection_closure_keeps_started_writer_memory_charged() {
    let setup = 2 * CONNECTION_SETUP_BYTES + WriterFence::setup_bytes_for_test();
    let node = pool(setup + 4096, setup + 4096);
    let connection = CancellationToken::new();
    let scope = WriterFence::with_memory(connection.clone(), CloseCause::new(), node.connection());
    let owner = scope.try_open().unwrap();
    let writer = owner.writer();
    let other_connection = node.connection();
    let charged = node.reserved_for_test() - NODE_SETUP_BYTES;
    let rest = other_connection
        .try_reserve(setup + 4096 - charged)
        .unwrap();
    assert!(writer.publish(|| {}));
    assert!(writer.try_start(|| true));
    drop((owner, scope));
    assert!(connection.is_cancelled());
    assert!(other_connection.try_reserve(1).is_none());
    drop(writer);
    assert!(other_connection
        .try_reserve(charged - CONNECTION_SETUP_BYTES)
        .is_some());
    drop((rest, other_connection));
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
}

#[tokio::test]
async fn another_connection_release_wakes_a_registered_waiter() {
    use futures::poll;
    let node = pool(2 * CONNECTION_SETUP_BYTES + 10, CONNECTION_SETUP_BYTES + 10);
    let first = node.connection();
    let second = node.connection();
    let owner = first.try_reserve(10).unwrap();
    let changed = second.wait_for_capacity(10);
    tokio::pin!(changed);
    assert!(second.try_reserve(1).is_none());
    assert!(poll!(&mut changed).is_pending());
    drop(owner);
    assert!(poll!(&mut changed).is_ready());
    assert!(second.try_reserve(10).is_some());
}

#[test]
fn racing_connections_cannot_overcommit_the_node() {
    let node = pool(
        32 * CONNECTION_SETUP_BYTES + 16,
        CONNECTION_SETUP_BYTES + 16,
    );
    let connections: Vec<_> = (0..32).map(|_| node.connection()).collect();
    let barrier = Arc::new(std::sync::Barrier::new(32));
    let threads: Vec<_> = connections
        .iter()
        .cloned()
        .map(|memory| {
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
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + 32 * CONNECTION_SETUP_BYTES + 16
    );
    drop(retained);
    assert_eq!(
        node.reserved_for_test(),
        NODE_SETUP_BYTES + 32 * CONNECTION_SETUP_BYTES
    );
    drop(connections);
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
}

// Helpers only for tests that compare the ported ownership contracts.
impl WriterFence {
    pub(crate) fn with_memory(
        connection: CancellationToken,
        cause: CloseCause,
        memory: ConnectionResponseMemory,
    ) -> Self {
        Self::try_with_memory(&connection, &cause, memory).expect("the fixture funds the fence")
    }
    fn open_with_metadata(&self, bytes: u64) -> Result<super::super::Exchange, ExchangeOpenError> {
        self.try_open_with_retained_memory(bytes, 0)
            .map(|(exchange, _)| exchange)
    }
}

#[test]
fn unfunded_fences_cannot_silently_admit_extra_metadata() {
    let fence = WriterFence::new(CancellationToken::new(), CloseCause::new());
    assert_eq!(
        fence.try_open_with_retained_memory(1, 0).unwrap_err(),
        ExchangeOpenError::Unfunded
    );
    assert!(fence.try_open().is_ok());
}

#[test]
fn funded_fence_refuses_retired_or_closed_connections_without_allocating() {
    for close in [false, true] {
        let node = ResponseMemory::default();
        let cancel = CancellationToken::new();
        let fence = WriterFence::with_memory(cancel.clone(), CloseCause::new(), node.connection());
        let before = node.reserved_for_test();
        if close {
            cancel.cancel();
        } else {
            fence.retire();
        }
        let (result, allocations) = zakura_test::allocations::measure(|| fence.try_open());
        assert_eq!(result.unwrap_err(), ExchangeOpenError::Retired);
        assert_eq!(allocations.requested_bytes, 0);
        assert_eq!(node.reserved_for_test(), before);
    }
}

#[tokio::test]
async fn transport_write_retains_funding_after_the_ending_and_receiver_drop() {
    use crate::zakura::{transport::worker_framed_channel, Frame};
    use futures::poll;

    let node = ResponseMemory::default();
    let memory = node.connection();
    let baseline = node.reserved_for_test();
    let cancel = CancellationToken::new();
    let fence = WriterFence::with_memory(cancel.clone(), CloseCause::new(), memory.clone());
    let mut exchange = fence.try_open().unwrap();
    let writer = exchange.writer();
    let funded = node.reserved_for_test();
    let (send, mut output) = worker_framed_channel(1);
    send.send_fenced(
        Frame {
            message_type: 1,
            flags: 0,
            payload: Vec::new(),
        },
        &writer,
    )
    .await
    .unwrap();
    drop(writer);
    let queued = output.recv().await.unwrap();
    let mut write = Box::pin(queued.write_with(|_| std::future::pending::<Result<(), ()>>()));
    assert!(poll!(&mut write).is_pending());
    // A response can arrive while the transport write is still completing.
    exchange.end();
    drop((exchange, fence));
    assert!(!cancel.is_cancelled());
    assert_eq!(node.reserved_for_test(), funded);
    drop(write);
    assert_eq!(node.reserved_for_test(), baseline);
    drop(memory);
    assert_eq!(node.reserved_for_test(), NODE_SETUP_BYTES);
}
