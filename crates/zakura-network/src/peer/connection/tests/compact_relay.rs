//! Tests for compact block relay negotiation and high-bandwidth source slots.

use std::sync::Arc;

use futures::{channel::mpsc, sink::SinkMapErr, StreamExt};
use tokio::sync::{watch, Semaphore};

use zakura_chain::serialization::SerializationError;
use zakura_test::mock_service::{MockService, PanicAssertion};

use crate::{
    peer::{connection::Connection, ClientRequest},
    protocol::external::Message,
    Request, Response,
};

/// A test connection with a mock inbound service and a channel peer writer.
type TestConnection = Connection<
    MockService<Request, Response, PanicAssertion>,
    SinkMapErr<mpsc::Sender<Message>, fn(mpsc::SendError) -> SerializationError>,
>;

/// A running test connection, and the channels that drive and observe it.
struct RunningConnection {
    /// The connection's peer message sender. The connection fails if it is dropped.
    _peer_tx: mpsc::Sender<Result<Message, SerializationError>>,
    /// The connection's client request sender. The connection shuts down if it is dropped.
    _client_tx: mpsc::Sender<ClientRequest>,
    /// The messages the connection writes to its peer.
    outbound: mpsc::Receiver<Message>,
    /// The peer set's high-bandwidth source selection for this connection.
    slots_tx: watch::Sender<Option<Arc<Semaphore>>>,
    /// The connection task.
    task: tokio::task::JoinHandle<()>,
}

impl RunningConnection {
    /// Returns the next message the connection writes to its peer.
    async fn next_written(&mut self) -> Message {
        self.outbound
            .next()
            .await
            .expect("the running connection keeps its peer writer open")
    }

    /// Asserts that the connection has written nothing it hasn't already returned.
    async fn assert_nothing_written(&mut self) {
        settle().await;
        assert!(
            self.outbound.try_recv().is_err(),
            "the connection wrote an unexpected message"
        );
    }
}

impl Drop for RunningConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Starts `connection` with `slots` as its initial high-bandwidth source selection.
fn run(
    mut connection: TestConnection,
    client_tx: mpsc::Sender<ClientRequest>,
    outbound: mpsc::Receiver<Message>,
    slots: Option<Arc<Semaphore>>,
) -> RunningConnection {
    let (slots_tx, slots_rx) = watch::channel(slots);
    connection.compact_receive_rx = slots_rx;
    let (peer_tx, peer_rx) = mpsc::channel(4);
    let task = tokio::spawn(connection.run(peer_rx));
    RunningConnection {
        _peer_tx: peer_tx,
        _client_tx: client_tx,
        outbound,
        slots_tx,
        task,
    }
}

/// Starts a connection that negotiated compact blocks, with `slots` as its initial
/// high-bandwidth source selection.
fn run_negotiated(slots: Option<Arc<Semaphore>>) -> RunningConnection {
    let (connection, client_tx, _inbound, outbound, _error_slot) =
        super::new_early_relay_test_connection::<PanicAssertion>();
    run(connection, client_tx, outbound, slots)
}

/// Returns a `sendcmpct` message for version 1 with `announce`.
fn sendcmpct(announce: bool) -> Message {
    Message::SendCompact {
        announce,
        version: 1,
    }
}

/// Lets spawned connection tasks run.
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

/// A negotiated connection that the peer set hasn't selected asks the peer not to announce
/// blocks with `cmpctblock`.
#[tokio::test]
async fn unselected_connection_sends_low_bandwidth_at_start() {
    let _init_guard = zakura_test::init();

    let mut connection = run_negotiated(None);

    assert_eq!(connection.next_written().await, sendcmpct(false));
    connection.assert_nothing_written().await;
}

/// A negotiated connection that the peer set selected before it started takes a slot and asks
/// the peer to announce blocks with `cmpctblock`.
#[tokio::test]
async fn selected_connection_sends_high_bandwidth_at_start() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(3));
    let mut connection = run_negotiated(Some(slots.clone()));

    assert_eq!(connection.next_written().await, sendcmpct(true));
    assert_eq!(
        slots.available_permits(),
        2,
        "the connection keeps its slot"
    );
    connection.assert_nothing_written().await;
}

/// A selected connection starts in low-bandwidth mode when every slot is taken.
#[tokio::test]
async fn selected_connection_without_free_slot_sends_low_bandwidth_at_start() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(1));
    let held = slots.clone().try_acquire_owned().expect("the slot is free");
    let mut connection = run_negotiated(Some(slots.clone()));

    assert_eq!(connection.next_written().await, sendcmpct(false));

    // The connection takes the slot as soon as it is free.
    drop(held);
    assert_eq!(connection.next_written().await, sendcmpct(true));
    assert_eq!(slots.available_permits(), 0);
}

/// Promotion takes a slot, then writes `sendcmpct(1, 1)`.
#[tokio::test]
async fn promotion_writes_high_bandwidth() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(3));
    let mut connection = run_negotiated(None);
    assert_eq!(connection.next_written().await, sendcmpct(false));

    connection.slots_tx.send_replace(Some(slots.clone()));

    assert_eq!(connection.next_written().await, sendcmpct(true));
    assert_eq!(
        slots.available_permits(),
        2,
        "the connection keeps its slot"
    );
    connection.assert_nothing_written().await;
}

/// Demotion writes `sendcmpct(0, 1)`, and releases the slot only once that write completes. The
/// next selected connection then takes the slot.
#[tokio::test]
async fn demotion_writes_low_bandwidth_then_releases_slot() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(1));
    let mut demoted = run_negotiated(Some(slots.clone()));
    let mut promoted = run_negotiated(None);
    assert_eq!(demoted.next_written().await, sendcmpct(true));
    assert_eq!(promoted.next_written().await, sendcmpct(false));

    // The test peer writer finishes a write only when the test reads it, so the demotion
    // write stays incomplete until `sendcmpct(0, 1)` is read.
    settle().await;
    demoted.slots_tx.send_replace(None);
    promoted.slots_tx.send_replace(Some(slots.clone()));
    settle().await;
    assert_eq!(
        slots.available_permits(),
        0,
        "the demoted connection keeps its slot until its write completes"
    );
    promoted.assert_nothing_written().await;

    assert_eq!(demoted.next_written().await, sendcmpct(false));
    assert_eq!(promoted.next_written().await, sendcmpct(true));
    assert_eq!(
        slots.available_permits(),
        0,
        "the promoted connection keeps its slot"
    );
    demoted.assert_nothing_written().await;
}

/// A connection without compact blocks writes no `sendcmpct`, even if it is offered slots.
#[tokio::test]
async fn non_negotiated_connection_sends_nothing() {
    let _init_guard = zakura_test::init();

    let slots = Arc::new(Semaphore::new(3));
    let (connection, client_tx, _inbound, outbound, _error_slot) =
        super::new_test_connection::<PanicAssertion>();
    let mut connection = run(connection, client_tx, outbound, Some(slots.clone()));

    connection.assert_nothing_written().await;
    connection.slots_tx.send_replace(None);
    connection.slots_tx.send_replace(Some(slots.clone()));
    connection.assert_nothing_written().await;
    assert_eq!(slots.available_permits(), 3);
}

/// The peer's `sendcmpct` turns `cmpctblock` announcements on for version 1 only. The connection
/// consumes the message without an inbound request or a penalty.
#[tokio::test]
async fn received_sendcmpct_sets_send_for_version_1_only() {
    let _init_guard = zakura_test::init();

    let (mut connection, _client_tx, mut inbound, _outbound, error_slot) =
        super::new_early_relay_test_connection::<PanicAssertion>();

    for (announce, version, send) in [
        (true, 1, true),
        (true, 2, false),
        (true, 1, true),
        (false, 1, false),
        (true, 0, false),
        (false, 2, false),
    ] {
        let unused = connection
            .handle_message_as_request(Message::SendCompact { announce, version })
            .await;
        assert_eq!(unused, None, "the connection consumes sendcmpct");
        assert_eq!(
            connection.compact.send, send,
            "announce={announce} version={version}"
        );
    }

    inbound.expect_no_requests().await;
    assert!(
        error_slot.try_get_error().is_none(),
        "sendcmpct is not misbehavior"
    );
}
