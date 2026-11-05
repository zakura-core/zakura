//! Fixed test vectors for peer connections.
//!
//! TODO: add tests for:
//!   - inbound message as request
//!   - inbound message, but not a request (or a response)

use std::{
    collections::HashSet,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use futures::{
    channel::{mpsc, oneshot},
    sink::SinkMapErr,
    FutureExt, SinkExt, StreamExt,
};
use indexmap::IndexSet;
use tower::load_shed::error::Overloaded;
use tracing::Span;

use zakura_chain::{block, serialization::SerializationError};
use zakura_node_services::block_memory::BlockHandle;
use zakura_test::mock_service::{MockService, PanicAssertion};

use crate::{
    constants::{MAX_OVERLOAD_DROP_PROBABILITY, MIN_OVERLOAD_DROP_PROBABILITY, REQUEST_TIMEOUT},
    peer::{
        connection::{overload_drop_connection_probability, Connection, State},
        ClientRequest, ErrorSlot,
    },
    peer_set::ActiveConnectionCounter,
    protocol::external::{InventoryHash, Message},
    types::Nonce,
    PeerError, PeerSource, Request, Response,
};

/// Test that the connection run loop works as a future
#[tokio::test]
async fn connection_run_loop_ok() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (connection, client_tx, mut inbound_service, mut peer_outbound_messages, shared_error_slot) =
        new_test_connection();

    let connection = connection.run(peer_rx);

    // The run loop will wait forever for a request from Zebra or the peer,
    // without any errors, channel closes, or bytes written.
    //
    // But the connection closes if we drop the future, so we avoid the drop by cloning it.
    let connection = connection.shared();
    let connection_guard = connection.clone();
    let result = connection.now_or_never();
    assert_eq!(result, None);

    let error = shared_error_slot.try_get_error();
    assert!(error.is_none(), "unexpected error: {error:?}");

    assert!(!client_tx.is_closed());
    assert!(!peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // We need to drop the future, because it holds a mutable reference to the bytes.
    std::mem::drop(connection_guard);
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that the connection run loop works as a spawned task
#[tokio::test]
async fn connection_run_loop_spawn_ok() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (connection, client_tx, mut inbound_service, mut peer_outbound_messages, shared_error_slot) =
        new_test_connection();

    // Spawn the connection run loop
    let mut connection_join_handle = tokio::spawn(connection.run(peer_rx));

    let error = shared_error_slot.try_get_error();
    assert!(error.is_none(), "unexpected error: {error:?}");

    assert!(!client_tx.is_closed());
    assert!(!peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // Make sure that the connection did not:
    // - panic, or
    // - return.
    //
    // This test doesn't cause any fatal errors,
    // so returning would be incorrect behaviour.
    let connection_result = futures::poll!(&mut connection_join_handle);
    assert!(
        matches!(connection_result, Poll::Pending),
        "unexpected run loop termination: {connection_result:?}",
    );

    // We need to abort the connection, because it holds a lock on the outbound channel.
    connection_join_handle.abort();
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that a peer writer which never closes cannot retain a failed connection.
#[tokio::test]
async fn connection_shutdown_times_out_stalled_peer_tx_close() {
    let _init_guard = zakura_test::init();

    tokio::time::pause();

    let mut active_connections = ActiveConnectionCounter::new_counter();
    let connection_tracker = active_connections.track_connection();
    let (connection, client_tx, shared_error_slot) =
        super::new_never_closing_test_connection::<crate::BoxError>(connection_tracker);

    // Closing the peer receiver fails the connection immediately.
    let (peer_tx, peer_rx) = mpsc::channel(1);
    drop(peer_tx);

    let mut connection_task = tokio::spawn(connection.run(peer_rx));
    tokio::task::yield_now().await;

    assert_eq!(
        shared_error_slot
            .try_get_error()
            .expect("peer stream closure must fail the connection")
            .inner_debug(),
        "ConnectionClosed",
    );
    assert!(
        client_tx.is_closed(),
        "failed connections must close client work before flushing the peer writer"
    );
    assert!(
        matches!(futures::poll!(&mut connection_task), Poll::Pending),
        "stalled close must wait for its timeout"
    );

    tokio::time::advance(REQUEST_TIMEOUT).await;

    assert!(
        connection_task.await.is_ok(),
        "connection must finish after the peer writer close timeout"
    );
    assert_eq!(
        active_connections.update_count(),
        0,
        "finished connections must release their connection tracker"
    );
}

/// Test that the connection run loop works as a spawned task with messages in and out
#[tokio::test]
async fn connection_run_loop_message_ok() {
    let _init_guard = zakura_test::init();

    tokio::time::pause();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (mut peer_tx, peer_rx) = mpsc::channel(1);

    let (
        connection,
        mut client_tx,
        mut inbound_service,
        mut peer_outbound_messages,
        shared_error_slot,
    ) = new_test_connection();

    // Spawn the connection run loop
    let mut connection_join_handle = tokio::spawn(connection.run(peer_rx));

    // Simulate a message send and receive
    let (request_tx, mut request_rx) = oneshot::channel();
    let request = ClientRequest {
        request: Request::Peers,
        tx: request_tx,
        inv_collector: None,
        transient_addr: None,
        span: Span::current(),
    };

    client_tx
        .try_send(request)
        .expect("internal request channel is valid");
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, Some(Message::GetAddr));

    peer_tx
        .try_send(Ok(Message::Addr(Vec::new())))
        .expect("peer inbound response channel is valid");

    // give the event loop time to run
    tokio::task::yield_now().await;
    let peer_response = request_rx.try_recv();
    assert_eq!(
        peer_response
            .expect("peer internal response channel is valid")
            .expect("response is present")
            .expect("response is a message (not an error)"),
        Response::Peers(Vec::new()),
    );

    let error = shared_error_slot.try_get_error();
    assert!(error.is_none(), "unexpected error: {error:?}");

    assert!(!client_tx.is_closed());
    assert!(!peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // Make sure that the connection did not:
    // - panic, or
    // - return.
    //
    // This test doesn't cause any fatal errors,
    // so returning would be incorrect behaviour.
    let connection_result = futures::poll!(&mut connection_join_handle);
    assert!(
        matches!(connection_result, Poll::Pending),
        "unexpected run loop termination: {connection_result:?}",
    );

    // We need to abort the connection, because it holds a lock on the outbound channel.
    connection_join_handle.abort();
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// An empty mempool reply releases the connection before the request timeout.
#[tokio::test(start_paused = true)]
async fn empty_mempool_reply_unblocks_queued_block_request() {
    let _init_guard = zakura_test::init();
    let (mut peer_tx, peer_rx) = mpsc::channel(1);
    let (connection, mut client_tx, _inbound_service, mut outbound, error_slot) =
        new_test_connection();
    let connection_task = tokio::spawn(connection.run(peer_rx));

    let (mempool_tx, mempool_rx) = oneshot::channel();
    client_tx
        .try_send(ClientRequest {
            request: Request::MempoolTransactionIds,
            tx: mempool_tx,
            inv_collector: None,
            transient_addr: None,
            span: Span::current(),
        })
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), outbound.next())
            .await
            .unwrap(),
        Some(Message::Mempool),
    );

    let hash = block::Hash([1; 32]);
    let (block_tx, block_rx) = oneshot::channel();
    client_tx
        .try_send(ClientRequest {
            request: Request::BlocksByHash(IndexSet::from([hash])),
            tx: block_tx,
            inv_collector: None,
            transient_addr: None,
            span: Span::current(),
        })
        .unwrap();
    assert!(outbound.next().now_or_never().is_none());

    peer_tx.send(Ok(Message::Inv(Vec::new()))).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        assert_eq!(
            mempool_rx.await.unwrap().unwrap(),
            Response::TransactionIds(Vec::new()),
        );
        assert_eq!(
            outbound.next().await,
            Some(Message::GetData(vec![InventoryHash::Block(hash)])),
        );
    })
    .await
    .expect("an empty mempool reply must release the queued block request without a timeout");

    assert!(error_slot.try_get_error().is_none());
    drop(block_rx);
    connection_task.abort();
    assert!(connection_task.await.unwrap_err().is_cancelled());
}

/// A late empty mempool reply must not finish a later block-locator request.
#[tokio::test(start_paused = true)]
async fn cancelled_mempool_reply_does_not_complete_find_blocks() {
    let _init_guard = zakura_test::init();
    let (mut peer_tx, peer_rx) = mpsc::channel(1);
    let (connection, mut client_tx, _inbound_service, mut outbound, error_slot) =
        new_test_connection();
    let connection_task = tokio::spawn(connection.run(peer_rx));

    let (mempool_tx, mempool_rx) = oneshot::channel();
    client_tx
        .try_send(ClientRequest {
            request: Request::MempoolTransactionIds,
            tx: mempool_tx,
            inv_collector: None,
            transient_addr: None,
            span: Span::current(),
        })
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), outbound.next())
            .await
            .unwrap(),
        Some(Message::Mempool),
    );
    drop(mempool_rx);

    let (find_tx, mut find_rx) = oneshot::channel();
    client_tx
        .send(ClientRequest {
            request: Request::FindBlocks {
                known_blocks: Vec::new(),
                stop: None,
            },
            tx: find_tx,
            inv_collector: None,
            transient_addr: None,
            span: Span::current(),
        })
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), outbound.next())
            .await
            .unwrap(),
        Some(Message::GetBlocks {
            known_blocks: Vec::new(),
            stop: None,
        }),
    );

    peer_tx.send(Ok(Message::Inv(Vec::new()))).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut find_rx)
            .await
            .is_err(),
        "the late empty mempool reply must leave the block-locator request pending",
    );

    let hashes = vec![block::Hash([1; 32]), block::Hash([2; 32])];
    peer_tx
        .send(Ok(Message::Inv(
            hashes.iter().copied().map(InventoryHash::Block).collect(),
        )))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), find_rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Response::BlockHashes(hashes),
    );

    assert!(error_slot.try_get_error().is_none());
    connection_task.abort();
    assert!(connection_task.await.unwrap_err().is_cancelled());
}

/// Test that the connection run loop fails correctly when dropped
#[tokio::test]
async fn connection_run_loop_future_drop() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (connection, client_tx, mut inbound_service, mut peer_outbound_messages, shared_error_slot) =
        new_test_connection();

    let connection = connection.run(peer_rx);

    // now_or_never implicitly drops the connection future.
    let result = connection.now_or_never();
    assert_eq!(result, None);

    let error = shared_error_slot.try_get_error();
    assert_eq!(
        error.expect("missing expected error").inner_debug(),
        "ConnectionDropped",
    );

    assert!(client_tx.is_closed());
    assert!(peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that the connection run loop fails correctly when the internal client closes the connection channel
#[tokio::test]
async fn connection_run_loop_client_close() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (
        connection,
        mut client_tx,
        mut inbound_service,
        mut peer_outbound_messages,
        shared_error_slot,
    ) = new_test_connection();

    let connection = connection.run(peer_rx);

    // Explicitly close the client channel.
    client_tx.close_channel();

    // If we drop the future, the connection will close anyway, so we avoid the drop by cloning it.
    let connection = connection.shared();
    let connection_guard = connection.clone();
    let result = connection.now_or_never();
    assert_eq!(result, Some(()));

    let error = shared_error_slot.try_get_error();
    assert_eq!(
        error.expect("missing expected error").inner_debug(),
        "ClientDropped",
    );

    assert!(client_tx.is_closed());
    assert!(peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // We need to drop the future, because it holds a mutable reference to the bytes.
    std::mem::drop(connection_guard);
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that the connection run loop fails correctly when the internal client drops the connection channel
#[tokio::test]
async fn connection_run_loop_client_drop() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (connection, client_tx, mut inbound_service, mut peer_outbound_messages, shared_error_slot) =
        new_test_connection();

    let connection = connection.run(peer_rx);

    // Drop the client channel.
    std::mem::drop(client_tx);

    // If we drop the future, the connection will close anyway, so we avoid the drop by cloning it.
    let connection = connection.shared();
    let connection_guard = connection.clone();
    let result = connection.now_or_never();
    assert_eq!(result, Some(()));

    let error = shared_error_slot.try_get_error();
    assert_eq!(
        error.expect("missing expected error").inner_debug(),
        "ClientDropped",
    );

    assert!(peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // We need to drop the future, because it holds a mutable reference to the bytes.
    std::mem::drop(connection_guard);
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that the connection run loop fails correctly when the peer channel is closed.
/// (We're not sure if tokio closes or drops the TcpStream when the TCP connection closes.)
#[tokio::test]
async fn connection_run_loop_inbound_close() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (mut peer_tx, peer_rx) = mpsc::channel(1);

    let (connection, client_tx, mut inbound_service, mut peer_outbound_messages, shared_error_slot) =
        new_test_connection();

    let connection = connection.run(peer_rx);

    // Explicitly close the inbound peer channel.
    peer_tx.close_channel();

    // If we drop the future, the connection will close anyway, so we avoid the drop by cloning it.
    let connection = connection.shared();
    let connection_guard = connection.clone();
    let result = connection.now_or_never();
    assert_eq!(result, Some(()));

    let error = shared_error_slot.try_get_error();
    assert_eq!(
        error.expect("missing expected error").inner_debug(),
        "ConnectionClosed",
    );

    assert!(client_tx.is_closed());
    assert!(peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // We need to drop the future, because it holds a mutable reference to the bytes.
    std::mem::drop(connection_guard);
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that an inbound parser error takes the normal peer failure path.
#[tokio::test]
async fn connection_run_loop_inbound_parser_error() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (mut peer_tx, peer_rx) = mpsc::channel(1);

    let (connection, client_tx, mut inbound_service, mut peer_outbound_messages, error_slot) =
        new_test_connection();

    peer_tx
        .try_send(Err(SerializationError::Parse("test peer parser panic")))
        .expect("test peer channel should accept a parser error");

    let connection = connection.run(peer_rx).shared();
    let connection_guard = connection.clone();
    assert_eq!(connection.now_or_never(), Some(()));

    let error = error_slot
        .try_get_error()
        .expect("peer parser error should be stored");
    assert_eq!(
        error.inner_debug(),
        "Serialization(Parse(\"test peer parser panic\"))"
    );
    assert!(client_tx.is_closed());
    assert!(peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    std::mem::drop(connection_guard);
    assert_eq!(peer_outbound_messages.next().await, None);
}

/// Test that the connection run loop fails correctly when the peer channel is dropped
/// (We're not sure if tokio closes or drops the TcpStream when the TCP connection closes.)
#[tokio::test]
async fn connection_run_loop_inbound_drop() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (connection, client_tx, mut inbound_service, mut peer_outbound_messages, shared_error_slot) =
        new_test_connection();

    let connection = connection.run(peer_rx);

    // Drop the inbound peer channel.
    std::mem::drop(peer_tx);

    // If we drop the future, the connection will close anyway, so we avoid the drop by cloning it.
    let connection = connection.shared();
    let connection_guard = connection.clone();
    let result = connection.now_or_never();
    assert_eq!(result, Some(()));

    let error = shared_error_slot.try_get_error();
    assert_eq!(
        error.expect("missing expected error").inner_debug(),
        "ConnectionClosed",
    );

    assert!(client_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // We need to drop the future, because it holds a mutable reference to the bytes.
    std::mem::drop(connection_guard);
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that the connection run loop fails correctly on internal connection errors.
#[tokio::test]
async fn connection_run_loop_failed() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (
        mut connection,
        client_tx,
        mut inbound_service,
        mut peer_outbound_messages,
        shared_error_slot,
    ) = new_test_connection();

    // Simulate an internal connection error.
    connection.state = State::Failed;
    shared_error_slot
        .try_update_error(PeerError::Overloaded.into())
        .expect("unexpected previous error in tests");

    let connection = connection.run(peer_rx);

    // If we drop the future, the connection will close anyway, so we avoid the drop by cloning it.
    let connection = connection.shared();
    let connection_guard = connection.clone();
    let result = connection.now_or_never();
    // Because the peer error mutex is a sync mutex,
    // the connection can't exit until it reaches the outer async loop.
    assert_eq!(result, Some(()));

    let error = shared_error_slot.try_get_error();
    assert_eq!(
        error.expect("missing expected error").inner_debug(),
        "Overloaded",
    );

    assert!(client_tx.is_closed());
    assert!(peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // We need to drop the future, because it holds a mutable reference to the bytes.
    std::mem::drop(connection_guard);
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that the connection run loop fails correctly when sending a message to a peer times out,
/// but we are not expecting a response message from the peer.
#[tokio::test]
async fn connection_run_loop_send_timeout_nil_response() {
    let _init_guard = zakura_test::init();

    tokio::time::pause();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (
        connection,
        mut client_tx,
        mut inbound_service,
        mut peer_outbound_messages,
        shared_error_slot,
    ) = new_test_connection();

    // Spawn the connection run loop
    let mut connection_join_handle = tokio::spawn(connection.run(peer_rx));

    // Simulate a message send timeout
    let (request_tx, mut request_rx) = oneshot::channel();
    let request = ClientRequest {
        request: Request::AdvertiseTransactionIds(HashSet::new(), None),
        tx: request_tx,
        inv_collector: None,
        transient_addr: None,
        span: Span::current(),
    };

    client_tx.try_send(request).expect("channel is valid");

    // Make the send timeout
    tokio::time::sleep(REQUEST_TIMEOUT + Duration::from_secs(1)).await;

    // Send timeouts close the connection
    let error = shared_error_slot.try_get_error();
    assert_eq!(
        error.expect("missing expected error").inner_debug(),
        "ConnectionSendTimeout",
    );

    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, Some(Message::Inv(Vec::new())));

    let peer_response = request_rx.try_recv();
    assert_eq!(
        peer_response
            .expect("peer internal response channel is valid")
            .expect("response is present")
            .expect_err("response is an error (not a message)")
            .inner_debug(),
        "ConnectionSendTimeout",
    );

    assert!(client_tx.is_closed());
    assert!(peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // Make sure that the connection finished, but did not panic.
    let connection_result = futures::poll!(&mut connection_join_handle);
    assert!(
        matches!(connection_result, Poll::Ready(Ok(()))),
        "expected run loop termination, but run loop continued: {connection_result:?}",
    );

    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that the connection run loop fails correctly when sending a message to a peer times out,
/// and we are expecting a response message from the peer.
#[tokio::test]
async fn connection_run_loop_send_timeout_expect_response() {
    let _init_guard = zakura_test::init();

    tokio::time::pause();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (
        connection,
        mut client_tx,
        mut inbound_service,
        mut peer_outbound_messages,
        shared_error_slot,
    ) = new_test_connection();

    // Spawn the connection run loop
    let mut connection_join_handle = tokio::spawn(connection.run(peer_rx));

    // Simulate a message send timeout
    let (request_tx, mut request_rx) = oneshot::channel();
    let request = ClientRequest {
        request: Request::Peers,
        tx: request_tx,
        inv_collector: None,
        transient_addr: None,
        span: Span::current(),
    };

    client_tx.try_send(request).expect("channel is valid");

    // Make the send timeout
    tokio::time::sleep(REQUEST_TIMEOUT + Duration::from_secs(1)).await;

    // Send timeouts close the connection
    let error = shared_error_slot.try_get_error();
    assert_eq!(
        error.expect("missing expected error").inner_debug(),
        "ConnectionSendTimeout",
    );

    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, Some(Message::GetAddr));

    let peer_response = request_rx.try_recv();
    assert_eq!(
        peer_response
            .expect("peer internal response channel is valid")
            .expect("response is present")
            .expect_err("response is an error (not a message)")
            .inner_debug(),
        "ConnectionSendTimeout",
    );

    assert!(client_tx.is_closed());
    assert!(peer_tx.is_closed());

    inbound_service.expect_no_requests().await;

    // Make sure that the connection finished, but did not panic.
    let connection_result = futures::poll!(&mut connection_join_handle);
    assert!(
        matches!(connection_result, Poll::Ready(Ok(()))),
        "expected run loop termination, but run loop continued: {connection_result:?}",
    );

    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Test that the connection run loop continues but returns an error to the client,
/// when a peer accepts a message, but does not send an expected response.
#[tokio::test]
async fn connection_run_loop_receive_timeout() {
    let _init_guard = zakura_test::init();

    tokio::time::pause();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (
        connection,
        mut client_tx,
        mut inbound_service,
        mut peer_outbound_messages,
        shared_error_slot,
    ) = new_test_connection();

    // Spawn the connection run loop
    let mut connection_join_handle = tokio::spawn(connection.run(peer_rx));

    // Simulate a message receive timeout
    let (request_tx, mut request_rx) = oneshot::channel();
    let request = ClientRequest {
        request: Request::Peers,
        tx: request_tx,
        inv_collector: None,
        transient_addr: None,
        span: Span::current(),
    };

    client_tx.try_send(request).expect("channel is valid");
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, Some(Message::GetAddr));

    // Make the receive timeout
    tokio::time::sleep(REQUEST_TIMEOUT + Duration::from_secs(1)).await;

    // Receive timeouts don't close the connection
    let error = shared_error_slot.try_get_error();
    assert!(error.is_none(), "unexpected error: {error:?}");

    assert!(!client_tx.is_closed());
    assert!(!peer_tx.is_closed());

    let peer_response = request_rx.try_recv();
    assert_eq!(
        peer_response
            .expect("peer internal response channel is valid")
            .expect("response is present")
            .expect_err("response is an error (not a message)")
            .inner_debug(),
        "ConnectionReceiveTimeout",
    );

    inbound_service.expect_no_requests().await;

    // Make sure that the connection did not:
    // - panic, or
    // - return.
    //
    // This test doesn't cause any fatal errors,
    // so returning would be incorrect behaviour.
    let connection_result = futures::poll!(&mut connection_join_handle);
    assert!(
        matches!(connection_result, Poll::Pending),
        "unexpected run loop termination: {connection_result:?}",
    );

    // We need to abort the connection, because it holds a lock on the outbound channel.
    connection_join_handle.abort();
    let outbound_message = peer_outbound_messages.next().await;
    assert_eq!(outbound_message, None);
}

/// Check basic properties of overload probabilities
#[test]
fn overload_probability_reduces_over_time() {
    let now = Instant::now();

    // Edge case: previous is in the future due to OS monotonic clock bugs
    let prev = now + Duration::from_secs(1);
    assert_eq!(
        overload_drop_connection_probability(now, Some(prev)),
        MAX_OVERLOAD_DROP_PROBABILITY,
        "if the overload time is in the future (OS bugs?), it should have maximum drop probability",
    );

    // Overload/DoS case/edge case: rapidly repeated overloads
    let prev = now;
    assert_eq!(
        overload_drop_connection_probability(now, Some(prev)),
        MAX_OVERLOAD_DROP_PROBABILITY,
        "if the overload times are the same, overloads should have maximum drop probability",
    );

    // Overload/DoS case: rapidly repeated overloads
    let prev = now - Duration::from_micros(1);
    let drop_probability = overload_drop_connection_probability(now, Some(prev));
    assert!(
        drop_probability <= MAX_OVERLOAD_DROP_PROBABILITY,
        "if the overloads are very close together, drops can optionally decrease: {drop_probability} <= {MAX_OVERLOAD_DROP_PROBABILITY}",
    );
    assert!(
        MAX_OVERLOAD_DROP_PROBABILITY - drop_probability < 0.001,
        "if the overloads are very close together, drops can only decrease slightly: {drop_probability}",
    );
    let last_probability = drop_probability;

    // Overload/DoS case: rapidly repeated overloads
    let prev = now - Duration::from_millis(1);
    let drop_probability = overload_drop_connection_probability(now, Some(prev));
    assert!(
        drop_probability < last_probability,
        "if the overloads decrease, drops should decrease: {drop_probability} < {last_probability}",
    );
    assert!(
        MAX_OVERLOAD_DROP_PROBABILITY - drop_probability < 0.001,
        "if the overloads are very close together, drops can only decrease slightly: {drop_probability}",
    );
    let last_probability = drop_probability;

    // Overload/DoS case: rapidly repeated overloads
    let prev = now - Duration::from_millis(10);
    let drop_probability = overload_drop_connection_probability(now, Some(prev));
    assert!(
        drop_probability < last_probability,
        "if the overloads decrease, drops should decrease: {drop_probability} < {last_probability}",
    );
    assert!(
        MAX_OVERLOAD_DROP_PROBABILITY - drop_probability < 0.001,
        "if the overloads are very close together, drops can only decrease slightly: {drop_probability}",
    );
    let last_probability = drop_probability;

    // Overload case: frequent overloads
    let prev = now - Duration::from_millis(100);
    let drop_probability = overload_drop_connection_probability(now, Some(prev));
    assert!(
        drop_probability < last_probability,
        "if the overloads decrease, drops should decrease: {drop_probability} < {last_probability}",
    );
    assert!(
        MAX_OVERLOAD_DROP_PROBABILITY - drop_probability < 0.01,
        "if the overloads are very close together, drops can only decrease slightly: {drop_probability}",
    );
    let last_probability = drop_probability;

    // Overload case: occasional but repeated overloads
    let prev = now - Duration::from_secs(1);
    let drop_probability = overload_drop_connection_probability(now, Some(prev));
    assert!(
        drop_probability < last_probability,
        "if the overloads decrease, drops should decrease: {drop_probability} < {last_probability}",
    );
    assert!(
        MAX_OVERLOAD_DROP_PROBABILITY - drop_probability > 0.4,
        "if the overloads are distant, drops should decrease a lot: {drop_probability}",
    );
    let last_probability = drop_probability;

    // Overload case: occasional overloads
    let prev = now - Duration::from_secs(5);
    let drop_probability = overload_drop_connection_probability(now, Some(prev));
    assert!(
        drop_probability < last_probability,
        "if the overloads decrease, drops should decrease: {drop_probability} < {last_probability}",
    );
    assert_eq!(
        drop_probability, MIN_OVERLOAD_DROP_PROBABILITY,
        "if overloads are far apart, drops should have minimum drop probability: {drop_probability}",
    );
    let _last_probability = drop_probability;

    // Base case: infrequent overloads
    let prev = now - Duration::from_secs(10);
    let drop_probability = overload_drop_connection_probability(now, Some(prev));
    assert_eq!(
        drop_probability, MIN_OVERLOAD_DROP_PROBABILITY,
        "if overloads are far apart, drops should have minimum drop probability: {drop_probability}",
    );

    // Base case: no previous overload
    let drop_probability = overload_drop_connection_probability(now, None);
    assert_eq!(
        drop_probability, MIN_OVERLOAD_DROP_PROBABILITY,
        "if there is no previous overload time, overloads should have minimum drop probability: {drop_probability}",
    );
}

/// Test that connections are randomly terminated in response to `Overloaded` errors.
///
/// TODO: do a similar test on the real service stack created in the `start` command.
#[tokio::test(flavor = "multi_thread")]
async fn connection_is_randomly_disconnected_on_overload() {
    let _init_guard = zakura_test::init();

    // The number of times we repeat the test
    const TEST_RUNS: usize = 220;
    // The expected number of tests before a test failure due to random chance.
    // Based on 10 tests per PR, 100 PR pushes per week, 50 weeks per year.
    const TESTS_BEFORE_FAILURE: f32 = 50_000.0;

    let test_runs = TEST_RUNS.try_into().expect("constant fits in i32");
    // The probability of random test failure is:
    // MIN_OVERLOAD_DROP_PROBABILITY^TEST_RUNS + MAX_OVERLOAD_DROP_PROBABILITY^TEST_RUNS
    assert!(
        1.0 / MIN_OVERLOAD_DROP_PROBABILITY.powi(test_runs) > TESTS_BEFORE_FAILURE,
        "not enough test runs: failures must be frequent enough to happen in almost all tests"
    );
    assert!(
        1.0 / MAX_OVERLOAD_DROP_PROBABILITY.powi(test_runs) > TESTS_BEFORE_FAILURE,
        "not enough test runs: successes must be frequent enough to happen in almost all tests"
    );

    let mut connection_continues = 0;
    let mut connection_closes = 0;

    for _ in 0..TEST_RUNS {
        // The real stream and sink are from a split TCP connection,
        // but that doesn't change how the state machine behaves.
        let (mut peer_tx, peer_rx) = mpsc::channel(1);

        let (
            connection,
            _client_tx,
            mut inbound_service,
            mut peer_outbound_messages,
            shared_error_slot,
        ) = new_test_connection();

        // The connection hasn't run so it must not have errors
        let error = shared_error_slot.try_get_error();
        assert!(
            error.is_none(),
            "unexpected error before starting the connection event loop: {error:?}",
        );

        // Start the connection run loop future in a spawned task
        let connection_handle = tokio::spawn(connection.run(peer_rx));
        tokio::time::sleep(Duration::from_millis(1)).await;

        // The connection hasn't received any messages, so it must not have errors
        let error = shared_error_slot.try_get_error();
        assert!(
            error.is_none(),
            "unexpected error before sending messages to the connection event loop: {error:?}",
        );

        // Simulate an overloaded connection error in response to an inbound request.
        let inbound_req = Message::GetAddr;
        peer_tx
            .send(Ok(inbound_req))
            .await
            .expect("send to channel always succeeds");
        tokio::time::sleep(Duration::from_millis(1)).await;

        // The connection hasn't got a response, so it must not have errors
        let error = shared_error_slot.try_get_error();
        assert!(
            error.is_none(),
            "unexpected error before sending responses to the connection event loop: {error:?}",
        );

        inbound_service
            .expect_request(Request::Peers)
            .await
            .respond_error(Overloaded::new().into());
        tokio::time::sleep(Duration::from_millis(1)).await;

        let outbound_result = peer_outbound_messages.try_recv();
        assert!(
            outbound_result.is_err(),
            "unexpected outbound message after Overloaded error:\n\
             {outbound_result:?}\n\
             note: Err(TryRecvError::Empty) means no messages, Err(TryRecvError::Closed) means the channel is closed"
        );

        let error = shared_error_slot.try_get_error();
        if error.is_some() {
            connection_closes += 1;
        } else {
            connection_continues += 1;
        }

        // We need to terminate the spawned task
        connection_handle.abort();
    }

    assert!(
        connection_closes > 0,
        "some overloaded connections must be closed at random"
    );
    assert!(
        connection_continues > 0,
        "some overloaded errors must be ignored at random"
    );
}

/// Test that operator-configured protected peers (block-gossip / zcashd-compat
/// sidecars) are never disconnected in response to `Overloaded` errors: their
/// requests are still shed, but the connection is never severed, even as
/// repeated overloads would drive an ordinary peer's drop probability toward
/// `MAX_OVERLOAD_DROP_PROBABILITY`.
#[tokio::test(flavor = "multi_thread")]
async fn protected_connection_is_never_disconnected_on_overload() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (mut peer_tx, peer_rx) = mpsc::channel(1);

    let (
        connection,
        _client_tx,
        mut inbound_service,
        mut peer_outbound_messages,
        shared_error_slot,
    ) = new_protected_test_connection();

    // Start the connection run loop future in a spawned task
    let connection_handle = tokio::spawn(connection.run(peer_rx));
    tokio::time::sleep(Duration::from_millis(1)).await;

    // Repeatedly overload the inbound service on the same connection. Each
    // overload refreshes `last_overload_time`, so for an ordinary peer the drop
    // probability climbs quadratically toward `MAX_OVERLOAD_DROP_PROBABILITY`.
    // A protected peer must survive every one of them.
    const OVERLOADS: usize = 50;
    for i in 0..OVERLOADS {
        // Simulate an overloaded connection error in response to an inbound request.
        peer_tx
            .send(Ok(Message::GetAddr))
            .await
            .expect("send to channel always succeeds");
        tokio::time::sleep(Duration::from_millis(1)).await;

        inbound_service
            .expect_request(Request::Peers)
            .await
            .respond_error(Overloaded::new().into());
        tokio::time::sleep(Duration::from_millis(1)).await;

        // The request is shed: no response is sent to the peer.
        let outbound_result = peer_outbound_messages.try_recv();
        assert!(
            outbound_result.is_err(),
            "overload {i}: protected peer's request must be shed, not answered:\n{outbound_result:?}"
        );

        // The connection is never severed for a protected peer.
        let error = shared_error_slot.try_get_error();
        assert!(
            error.is_none(),
            "overload {i}: protected peer must never be disconnected on overload, but got: {error:?}"
        );
    }

    // We need to terminate the spawned task
    connection_handle.abort();
}

/// A block `getdata` from a configured sidecar carries its classified
/// connection source into the inbound service.
#[tokio::test]
async fn protected_connection_attributes_inbound_block_request() {
    let _init_guard = zakura_test::init();
    let (mut peer_tx, peer_rx) = mpsc::channel(1);
    let (connection, _client_tx, mut inbound_service, _peer_messages, _error_slot) =
        new_protected_test_connection();
    let connection_handle = tokio::spawn(connection.run(peer_rx));
    let parent_hash = block::Hash([0x11; 32]);
    let child_hash = block::Hash([0x22; 32]);

    peer_tx
        .send(Ok(Message::GetData(vec![
            parent_hash.into(),
            child_hash.into(),
        ])))
        .await
        .expect("test peer channel is open");

    inbound_service
        .expect_request(Request::BlocksByHashFrom {
            hashes: IndexSet::from([parent_hash, child_hash]),
            source: PeerSource::LegacySocket(
                "127.0.0.1:4"
                    .parse()
                    .expect("test peer socket address is valid"),
            ),
        })
        .await
        .respond_error(Overloaded::new().into());

    connection_handle.abort();
}

#[tokio::test]
async fn connection_ping_pong_round_trip() {
    let _init_guard = zakura_test::init();

    // The real stream and sink are from a split TCP connection,
    // but that doesn't change how the state machine behaves.
    let (peer_tx, peer_rx) = mpsc::channel(1);

    let (
        connection,
        mut client_tx,
        _inbound_service,
        mut peer_outbound_messages,
        _shared_error_slot,
    ) = new_test_connection();

    let connection = tokio::spawn(connection.run(peer_rx));

    // === Client sends Ping request ===
    let (response_tx, response_rx) = oneshot::channel();
    let nonce = Nonce::default();

    client_tx
        .send(ClientRequest {
            request: Request::Ping(nonce),
            tx: response_tx,
            inv_collector: None,
            transient_addr: None,
            span: Span::none(),
        })
        .await
        .expect("send to connection should succeed");

    // === Peer receives Ping message ===
    let outbound_msg = peer_outbound_messages
        .next()
        .await
        .expect("expected outbound Ping message");

    let ping_nonce = match outbound_msg {
        Message::Ping(nonce) => nonce,
        msg => panic!("expected Ping message, but got: {msg:?}"),
    };

    assert_eq!(
        nonce, ping_nonce,
        "Ping nonce in request must match message sent to peer"
    );

    // === Peer sends matching Pong ===
    let pong_rtt = Duration::from_millis(42);
    tokio::time::sleep(pong_rtt).await;

    peer_tx
        .clone()
        .send(Ok(Message::Pong(ping_nonce)))
        .await
        .expect("sending Pong to connection should succeed");

    // === Client receives Pong response and verifies RTT ===
    match response_rx.await.expect("response channel must succeed") {
        Ok(Response::Pong(rtt)) => {
            assert!(
                rtt >= pong_rtt,
                "measured RTT {rtt:?} must be >= simulated RTT {pong_rtt:?}"
            );
        }
        Ok(resp) => panic!("unexpected response: {resp:?}"),
        Err(err) => panic!("unexpected error: {err:?}"),
    }

    drop(peer_tx);
    let _ = connection.await;
}

/// Creates a new [`Connection`] instance for unit tests.
fn new_test_connection() -> (
    Connection<
        MockService<Request, Response, PanicAssertion>,
        SinkMapErr<mpsc::Sender<Message>, fn(mpsc::SendError) -> SerializationError>,
    >,
    mpsc::Sender<ClientRequest>,
    MockService<Request, Response, PanicAssertion>,
    mpsc::Receiver<Message>,
    ErrorSlot,
) {
    super::new_test_connection()
}

/// Creates a new protected-peer [`Connection`] instance for unit tests
/// (exempt from the inbound-overload connection drop).
fn new_protected_test_connection() -> (
    Connection<
        MockService<Request, Response, PanicAssertion>,
        SinkMapErr<mpsc::Sender<Message>, fn(mpsc::SendError) -> SerializationError>,
    >,
    mpsc::Sender<ClientRequest>,
    MockService<Request, Response, PanicAssertion>,
    mpsc::Receiver<Message>,
    ErrorSlot,
) {
    super::new_protected_test_connection()
}

/// Inventory completion cannot release a gossip slot before the actual block write.
#[tokio::test(start_paused = true)]
async fn block_gossip_slot_tracks_connection_write() {
    use crate::{peer::block_gossip::BlockGossipPeer, InventoryResponse};
    use std::sync::Arc;
    use zakura_chain::serialization::ZcashDeserializeInto;

    let _init_guard = zakura_test::init();
    let (mut peer_tx, peer_rx) = mpsc::channel(1);
    let (mut connection, _client_tx, mut inbound, mut outbound, _error) = new_test_connection();
    let (block_advert_tx, block_advert_rx) =
        tokio::sync::mpsc::channel(crate::peer::block_gossip::BLOCK_ADVERT_QUEUE);
    connection.block_advert_rx = block_advert_rx;
    let gossip = BlockGossipPeer {
        sender: block_advert_tx,
        address: None,
        uploads: connection.block_uploads.clone(),
        registry: None,
        version: crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION,
        sidecar: false,
    };
    let connection_task = tokio::spawn(connection.run(peer_rx));
    let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let hash = block.hash();
    let slots = crate::peer::block_gossip::BlockUploadSlots::new(1);
    let advertisement = tokio::spawn(gossip.advertise(hash, slots.clone()));
    assert_eq!(
        outbound.next().await.unwrap(),
        Message::Inv(vec![hash.into()])
    );
    peer_tx
        .send(Ok(Message::GetData(vec![hash.into()])))
        .await
        .unwrap();
    inbound
        .expect_request(Request::BlocksByHash(IndexSet::from([hash])))
        .await
        .respond(Response::Blocks(vec![InventoryResponse::Available((
            block.clone().into(),
            None,
        ))]));
    tokio::task::yield_now().await;
    // The zero-capacity sink cannot flush its block until we read it below.
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert_eq!(slots.available_uploads(), 0);
    assert!(!advertisement.is_finished());
    assert_eq!(outbound.next().await.unwrap(), Message::Block(block.into()));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(749)).await;
    tokio::task::yield_now().await;
    assert_eq!(slots.available_uploads(), 0);
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(advertisement.await.unwrap());
    assert_eq!(slots.available_uploads(), 1);
    connection_task.abort();
}

/// A `headers` response keeps its state read charge until the wire send finishes.
#[tokio::test(start_paused = true)]
async fn tcp_headers_response_holds_read_admission_through_the_send() {
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
    use zakura_node_services::block_memory::{HeaderHandle, ReadMemoryBudget};

    let _init_guard = zakura_test::init();
    let (mut peer_tx, peer_rx) = mpsc::channel(1);
    let (connection, _client_tx, mut inbound, mut outbound, _error) = new_test_connection();
    let connection_task = tokio::spawn(connection.run(peer_rx));

    let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let pool = ReadMemoryBudget::new(std::num::NonZeroU64::MAX);
    let header = pool
        .try_reserve(
            zakura_node_services::block_memory::DecodedSize::decoded_size_bytes(&*block.header),
        )
        .unwrap()
        .retain(*block.header)
        .unwrap();

    let known_blocks = vec![block.hash()];
    peer_tx
        .send(Ok(Message::GetHeaders {
            known_blocks: known_blocks.clone(),
            stop: None,
        }))
        .await
        .unwrap();
    inbound
        .expect_request(Request::FindHeaders {
            known_blocks,
            stop: None,
        })
        .await
        .respond(Response::BlockHeaders(vec![HeaderHandle::from(header)]));

    tokio::task::yield_now().await;
    assert!(
        pool.retained_bytes() > 0,
        "the zero-capacity sink cannot flush until the test reads it"
    );
    let Message::Headers(sent) = outbound.next().await.unwrap() else {
        panic!("a headers response becomes a headers message")
    };
    assert_eq!(sent[0].header, block.header);
    // The connection task finishes its flush and drops the handles on a later turn.
    for _ in 0..16 {
        if pool.retained_bytes() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(pool.retained_bytes(), 0);
    connection_task.abort();
}

/// A block the inbound service leaves out of its response cannot hold a gossip slot.
#[tokio::test(start_paused = true)]
async fn truncated_block_request_releases_gossip_slot() {
    use crate::peer::block_gossip::BlockGossipPeer;

    let _init_guard = zakura_test::init();
    let (mut peer_tx, peer_rx) = mpsc::channel(1);
    let (mut connection, _client_tx, mut inbound, mut outbound, _error) = new_test_connection();
    let (block_advert_tx, block_advert_rx) =
        tokio::sync::mpsc::channel(crate::peer::block_gossip::BLOCK_ADVERT_QUEUE);
    connection.block_advert_rx = block_advert_rx;
    let gossip = BlockGossipPeer {
        sender: block_advert_tx,
        address: None,
        uploads: connection.block_uploads.clone(),
        registry: None,
        version: crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION,
        sidecar: false,
    };
    let connection_task = tokio::spawn(connection.run(peer_rx));
    let decoy = block::Hash([1; 32]);
    let hash = block::Hash([2; 32]);
    let slots = crate::peer::block_gossip::BlockUploadSlots::new(1);
    let advertisement = tokio::spawn(gossip.advertise(hash, slots.clone()));
    assert_eq!(
        outbound.next().await.unwrap(),
        Message::Inv(vec![hash.into()])
    );
    peer_tx
        .send(Ok(Message::GetData(vec![decoy.into(), hash.into()])))
        .await
        .unwrap();
    inbound
        .expect_request(Request::BlocksByHash(IndexSet::from([decoy, hash])))
        .await
        .respond(Response::Blocks(Vec::new()));
    assert!(!advertisement.await.unwrap());
    assert_eq!(slots.available_uploads(), 1);
    connection_task.abort();
}

/// A short bound for the tests below, whose connections use real time.
const RESOURCE_TEST_BOUND: Duration = Duration::from_secs(5);

/// Waits for the connection task to bring `budget`'s charge to `bytes`.
async fn budget_reaches(budget: &crate::inbound_resources::InboundBudget, bytes: u64) {
    while budget.used() != bytes {
        tokio::task::yield_now().await;
    }
}

fn mainnet_blocks() -> [zakura_chain::block::Block; 2] {
    use zakura_chain::serialization::ZcashDeserializeInto;

    [
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap(),
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    ]
}

/// Runs `test` under the bound that the tests below share.
async fn within_resource_bound(test: impl std::future::Future<Output = ()>) {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(RESOURCE_TEST_BOUND, test)
        .await
        .expect("the connection handles its messages");
}

/// A running connection whose inbound budget holds one byte, so any real payload is
/// above its capacity.
struct BudgetedConnection {
    peer_tx: mpsc::Sender<Result<Message, SerializationError>>,
    client_tx: mpsc::Sender<ClientRequest>,
    inbound: MockService<Request, Response, PanicAssertion>,
    outbound: mpsc::Receiver<Message>,
    error: ErrorSlot,
    budget: crate::inbound_resources::InboundBudget,
    task: tokio::task::JoinHandle<()>,
}

fn budgeted_connection() -> BudgetedConnection {
    let (peer_tx, peer_rx) = mpsc::channel(1);
    let (mut connection, client_tx, inbound, outbound, error) = new_test_connection();
    let budget = crate::inbound_resources::InboundBudget::new(1);
    connection.inbound_budget = budget.clone();
    BudgetedConnection {
        peer_tx,
        client_tx,
        inbound,
        outbound,
        error,
        budget,
        task: tokio::spawn(connection.run(peer_rx)),
    }
}

impl BudgetedConnection {
    /// Requests two blocks and returns them with the receiver of their response.
    async fn request_two_blocks(
        &mut self,
    ) -> (
        [BlockHandle; 2],
        oneshot::Receiver<Result<Response, crate::peer::SharedPeerError>>,
    ) {
        let blocks = mainnet_blocks().map(|block| BlockHandle::from(Arc::new(block)));
        let (tx, response) = oneshot::channel();
        self.client_tx
            .try_send(ClientRequest {
                request: Request::BlocksByHash(blocks.iter().map(|block| block.hash()).collect()),
                tx,
                inv_collector: None,
                transient_addr: None,
                span: Span::current(),
            })
            .expect("the connection accepts its first request");
        assert!(matches!(
            self.outbound.next().await,
            Some(Message::GetData(items)) if items.len() == 2
        ));
        (blocks, response)
    }
}

/// L.objects: a response to this node's own request charges every decoded block until
/// its handoff. It never refuses, even above the budget's capacity.
#[tokio::test]
async fn tcp_response_blocks_charge_until_handoff_without_refusing() {
    within_resource_bound(async {
        let mut tcp = budgeted_connection();
        let ([first, second], mut response) = tcp.request_two_blocks().await;
        let first_bytes = first.decoded_bytes();

        tcp.peer_tx.send(Ok(Message::Block(first))).await.unwrap();
        budget_reaches(&tcp.budget, first_bytes).await;
        assert!(matches!(response.try_recv(), Ok(None)));

        tcp.peer_tx.send(Ok(Message::Block(second))).await.unwrap();
        assert!(matches!(
            response.await.unwrap().unwrap(),
            Response::Blocks(blocks) if blocks.len() == 2
        ));
        budget_reaches(&tcp.budget, 0).await;
        tcp.task.abort();
    })
    .await;
}

/// A cancelled request ends the charge for the payloads it already received.
#[tokio::test]
async fn cancelling_a_tcp_request_ends_its_response_charge() {
    within_resource_bound(async {
        let mut tcp = budgeted_connection();
        let ([first, _second], response) = tcp.request_two_blocks().await;
        let first_bytes = first.decoded_bytes();

        tcp.peer_tx.send(Ok(Message::Block(first))).await.unwrap();
        budget_reaches(&tcp.budget, first_bytes).await;
        drop(response);
        budget_reaches(&tcp.budget, 0).await;
        tcp.task.abort();
    })
    .await;
}

/// L.objects: an unsolicited block has no retained owner, so it takes no charge.
/// An unsolicited transaction drops on a saturated budget without faulting the peer,
/// and otherwise holds its charge until its handler ends.
#[tokio::test]
async fn unsolicited_tcp_transactions_respect_the_budget() {
    use zakura_chain::transaction::UnminedTx;

    within_resource_bound(async {
        let mut tcp = budgeted_connection();
        let [block, _] = mainnet_blocks();
        let transaction = UnminedTx::from(block.transactions[0].clone());

        tcp.peer_tx
            .send(Ok(Message::Block(Arc::new(block).into())))
            .await
            .unwrap();
        let held = tcp.budget.try_charge(1).expect("the budget is idle");
        tcp.peer_tx
            .send(Ok(Message::Tx(transaction.clone())))
            .await
            .unwrap();

        // A ping proves the connection finished the refused transaction and stays open.
        tcp.peer_tx
            .send(Ok(Message::Ping(Nonce::default())))
            .await
            .unwrap();
        assert!(matches!(tcp.outbound.next().await, Some(Message::Pong(_))));
        tcp.inbound.expect_no_requests().await;
        assert!(tcp.error.try_get_error().is_none());
        drop(held);

        tcp.peer_tx
            .send(Ok(Message::Tx(transaction.clone())))
            .await
            .unwrap();
        let request = tcp
            .inbound
            .expect_request(Request::PushTransaction(transaction, None))
            .await;
        assert_eq!(
            tcp.budget.used(),
            1,
            "the oversized item clamps to the budget"
        );
        request.respond(Response::Nil);
        budget_reaches(&tcp.budget, 0).await;
        tcp.task.abort();
    })
    .await;
}

/// L.headers: a `headers` response charges only its new allocations. A read handle
/// keeps its own slot and is copied. A shared header has no slot and is not copied.
#[tokio::test]
async fn tcp_header_copies_charge_only_their_new_allocations() {
    use zakura_node_services::block_memory::{HeaderHandle, ReadMemoryBudget};

    within_resource_bound(async {
        let mut tcp = budgeted_connection();

        let [first, second] = mainnet_blocks();
        let shared_header = second.header.clone();
        let pool = ReadMemoryBudget::new(std::num::NonZeroU64::MAX);
        let read_header = pool
            .try_reserve(
                zakura_node_services::block_memory::DecodedSize::decoded_size_bytes(&*first.header),
            )
            .unwrap()
            .retain(*first.header)
            .unwrap();
        let headers = vec![
            HeaderHandle::from(read_header),
            HeaderHandle::from(shared_header.clone()),
        ];

        let (wire, copied_bytes) = crate::peer::connection::wire_headers(&headers);
        assert!(Arc::ptr_eq(&wire[1].header, &shared_header));
        assert_eq!(
            copied_bytes,
            crate::peer::connection::COPIED_HEADER_BYTES
                + crate::inbound_resources::vec_retained_bytes(&wire)
        );
        drop(wire);

        let known_blocks = vec![first.hash()];
        tcp.peer_tx
            .send(Ok(Message::GetHeaders {
                known_blocks: known_blocks.clone(),
                stop: None,
            }))
            .await
            .unwrap();
        tcp.inbound
            .expect_request(Request::FindHeaders {
                known_blocks,
                stop: None,
            })
            .await
            .respond(Response::BlockHeaders(headers));

        budget_reaches(&tcp.budget, copied_bytes).await;
        assert!(
            pool.retained_bytes() > 0,
            "the handle keeps its read charge"
        );

        let Some(Message::Headers(sent)) = tcp.outbound.next().await else {
            panic!("a headers response becomes a headers message")
        };
        assert_eq!(sent.len(), 2);
        drop(sent);
        budget_reaches(&tcp.budget, 0).await;
        while pool.retained_bytes() != 0 {
            tokio::task::yield_now().await;
        }
        tcp.task.abort();
    })
    .await;
}

/// C.1 to C.4, C.9, C.10, and L.mempool: a connection awaits each inbound service call
/// before it reads the next message, so each connection adds one call to the static
/// bound in `inbound_resources`.
#[tokio::test]
async fn a_tcp_connection_runs_one_inbound_call_at_a_time() {
    within_resource_bound(async {
        let mut tcp = budgeted_connection();
        tcp.peer_tx.send(Ok(Message::Mempool)).await.unwrap();
        let first = tcp
            .inbound
            .expect_request(Request::MempoolTransactionIds)
            .await;

        tcp.peer_tx.send(Ok(Message::Mempool)).await.unwrap();
        tcp.inbound.expect_no_requests().await;

        first.respond(Response::Nil);
        tcp.inbound
            .expect_request(Request::MempoolTransactionIds)
            .await
            .respond(Response::Nil);
        tcp.task.abort();
    })
    .await;
}

/// L.mempool: an empty snapshot sends nothing, and a larger snapshot than one message
/// allows sends exactly one message of the protocol's limit.
#[tokio::test]
async fn a_tcp_mempool_response_is_bounded() {
    use zakura_chain::transaction::{self, UnminedTxId};

    within_resource_bound(async {
        let mut tcp = budgeted_connection();

        tcp.peer_tx.send(Ok(Message::Mempool)).await.unwrap();
        tcp.inbound
            .expect_request(Request::MempoolTransactionIds)
            .await
            .respond(Response::Nil);
        tcp.peer_tx
            .send(Ok(Message::Ping(Nonce::default())))
            .await
            .unwrap();
        assert!(matches!(tcp.outbound.next().await, Some(Message::Pong(_))));

        let limit = usize::try_from(crate::MAX_TX_INV_IN_SENT_MESSAGE).unwrap();
        let ids = vec![UnminedTxId::Legacy(transaction::Hash([1; 32])); limit + 1];
        tcp.peer_tx.send(Ok(Message::Mempool)).await.unwrap();
        tcp.inbound
            .expect_request(Request::MempoolTransactionIds)
            .await
            .respond(Response::TransactionIds(ids));
        assert!(matches!(
            tcp.outbound.next().await,
            Some(Message::Inv(items)) if items.len() == limit
        ));
        tcp.task.abort();
    })
    .await;
}

/// Lets spawned connection tasks run without advancing the paused clock.
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

/// Returns a client request for `request`, and the receiver for its response.
fn client_request(
    request: Request,
) -> (
    ClientRequest,
    oneshot::Receiver<Result<Response, crate::SharedPeerError>>,
) {
    let (tx, rx) = oneshot::channel();
    let request = ClientRequest {
        request,
        tx,
        inv_collector: None,
        transient_addr: None,
        span: Span::none(),
    };
    (request, rx)
}

/// Returns a transaction and a block to use as peer responses.
fn response_vectors() -> (
    zakura_chain::transaction::UnminedTx,
    std::sync::Arc<block::Block>,
) {
    use zakura_chain::serialization::ZcashDeserializeInto;

    let block: std::sync::Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("test vector is a valid block");
    (block.transactions[0].clone().into(), block)
}

/// The request slot is busy exactly while a client request is outstanding, and responses,
/// `notfound`, unsolicited messages and late responses still reach the right handler.
#[tokio::test(start_paused = true)]
async fn request_slot_tracks_responses_and_notfound() {
    let _init_guard = zakura_test::init();

    let (mut peer_tx, peer_rx) = mpsc::channel(4);
    let (connection, mut client_tx, mut inbound, mut outbound, _error) = new_test_connection();
    let slot = connection.request_slot.clone();
    let connection_task = tokio::spawn(connection.run(peer_rx));
    let (tx, block) = response_vectors();
    let txid = tx.id();

    settle().await;
    assert!(slot.is_free(), "a new connection can write a request");

    // A transaction response holds the slot until it arrives.
    let (request, response) = client_request(Request::TransactionsById([txid].into()));
    client_tx.send(request).await.expect("connection is open");
    assert_eq!(
        outbound.next().await,
        Some(Message::GetData(vec![txid.into()]))
    );
    settle().await;
    assert!(!slot.is_free(), "awaiting a transaction response");

    // An unsolicited block advert goes to the inbound service and keeps the request outstanding.
    peer_tx
        .send(Ok(Message::Inv(vec![block.hash().into()])))
        .await
        .expect("connection is open");
    inbound
        .expect_request_that(
            |request| matches!(request, Request::AdvertiseBlock(advert, _) if advert.hash == block.hash()),
        )
        .await
        .respond(Response::Nil);
    settle().await;
    assert!(
        !slot.is_free(),
        "unsolicited messages do not end the request"
    );

    peer_tx
        .send(Ok(Message::Tx(tx.clone())))
        .await
        .expect("connection is open");
    let response = response
        .await
        .expect("connection responds")
        .expect("peer sent the transaction");
    assert!(matches!(response, Response::Transactions(ref txs) if txs.len() == 1));
    settle().await;
    assert!(slot.is_free(), "the response frees the slot");

    // A `notfound` for the outstanding transaction ends the request with an error.
    let (request, response) = client_request(Request::TransactionsById([txid].into()));
    client_tx.send(request).await.expect("connection is open");
    assert_eq!(
        outbound.next().await,
        Some(Message::GetData(vec![txid.into()]))
    );
    peer_tx
        .send(Ok(Message::NotFound(vec![txid.into()])))
        .await
        .expect("connection is open");
    let error = response
        .await
        .expect("connection responds")
        .expect_err("peer does not have the transaction");
    assert!(error.inner_debug().contains("NotFoundResponse"));
    settle().await;
    assert!(slot.is_free(), "a notfound frees the slot");

    // A late transaction response that arrives during a block request is handled as a pushed
    // transaction, and does not complete the block request.
    let (request, response) = client_request(Request::BlocksByHash([block.hash()].into()));
    client_tx.send(request).await.expect("connection is open");
    assert_eq!(
        outbound.next().await,
        Some(Message::GetData(vec![block.hash().into()]))
    );
    peer_tx
        .send(Ok(Message::Tx(tx.clone())))
        .await
        .expect("connection is open");
    inbound
        .expect_request_that(|request| matches!(request, Request::PushTransaction(..)))
        .await
        .respond(Response::Nil);
    settle().await;
    assert!(!slot.is_free(), "still awaiting the block");

    peer_tx
        .send(Ok(Message::Block(block.clone().into())))
        .await
        .expect("connection is open");
    let response = response
        .await
        .expect("connection responds")
        .expect("peer sent the block");
    assert!(matches!(response, Response::Blocks(ref blocks) if blocks.len() == 1));
    settle().await;
    assert!(slot.is_free(), "the block response frees the slot");

    connection_task.abort();
}

/// Cancellation and timeouts free the request slot, and a disconnect leaves it busy.
#[tokio::test(start_paused = true)]
async fn request_slot_tracks_cancellation_timeout_and_disconnect() {
    let _init_guard = zakura_test::init();

    let (mut peer_tx, peer_rx) = mpsc::channel(4);
    let (connection, mut client_tx, _inbound, mut outbound, error_slot) = new_test_connection();
    let slot = connection.request_slot.clone();
    let connection_task = tokio::spawn(connection.run(peer_rx));
    let (tx, _block) = response_vectors();
    let txid = tx.id();

    // Cancelling the request frees the slot.
    let (request, response) = client_request(Request::TransactionsById([txid].into()));
    client_tx.send(request).await.expect("connection is open");
    assert_eq!(
        outbound.next().await,
        Some(Message::GetData(vec![txid.into()]))
    );
    settle().await;
    assert!(!slot.is_free());
    drop(response);
    settle().await;
    assert!(slot.is_free(), "cancellation frees the slot");

    // A response timeout fails the request and frees the slot.
    let (request, response) = client_request(Request::TransactionsById([txid].into()));
    client_tx.send(request).await.expect("connection is open");
    assert_eq!(
        outbound.next().await,
        Some(Message::GetData(vec![txid.into()]))
    );
    tokio::time::advance(REQUEST_TIMEOUT + Duration::from_secs(1)).await;
    let error = response
        .await
        .expect("connection responds")
        .expect_err("request timed out");
    assert_eq!(error.inner_debug(), "ConnectionReceiveTimeout");
    settle().await;
    assert!(slot.is_free(), "a timeout frees the slot");

    // A disconnect fails the outstanding request and leaves the slot busy.
    let (request, response) = client_request(Request::TransactionsById([txid].into()));
    client_tx.send(request).await.expect("connection is open");
    assert_eq!(
        outbound.next().await,
        Some(Message::GetData(vec![txid.into()]))
    );
    peer_tx.close_channel();
    let error = response
        .await
        .expect("connection responds")
        .expect_err("connection closed");
    assert_eq!(error.inner_debug(), "ConnectionClosed");
    assert!(error_slot.try_get_error().is_some());
    assert!(!slot.is_free(), "a failed connection cannot write requests");

    let _ = connection_task.await;
}

/// A block request that spends half its timeout unanswered is recorded on the request slot, and a
/// delivered block clears the record. Early cancellations and other requests leave it unset.
#[tokio::test(start_paused = true)]
async fn request_slot_records_unanswered_block_requests() {
    let _init_guard = zakura_test::init();

    let (mut peer_tx, peer_rx) = mpsc::channel(4);
    let (connection, mut client_tx, _inbound, mut outbound, _error) = new_test_connection();
    let slot = connection.request_slot.clone();
    let connection_task = tokio::spawn(connection.run(peer_rx));
    let (tx, block) = response_vectors();
    let txid = tx.id();
    let half_timeout = crate::constants::REQUEST_TIMEOUT / 2;

    // A transaction request that times out says nothing about blocks.
    let (request, response) = client_request(Request::TransactionsById([txid].into()));
    client_tx.send(request).await.expect("connection is open");
    outbound.next().await.expect("request is written");
    settle().await;
    tokio::time::advance(crate::constants::REQUEST_TIMEOUT).await;
    assert!(response.await.expect("connection responds").is_err());
    settle().await;
    assert!(!slot.block_request_unanswered());

    // An early cancellation says nothing about the peer.
    let (request, response) = client_request(Request::BlocksByHash([block.hash()].into()));
    client_tx.send(request).await.expect("connection is open");
    outbound.next().await.expect("request is written");
    settle().await;
    tokio::time::advance(half_timeout - Duration::from_secs(1)).await;
    drop(response);
    settle().await;
    assert!(slot.is_free());
    assert!(!slot.block_request_unanswered());

    // A downloader's own timeout usually cancels just before the connection's timer fires.
    let (request, response) = client_request(Request::BlocksByHash([block.hash()].into()));
    client_tx.send(request).await.expect("connection is open");
    outbound.next().await.expect("request is written");
    settle().await;
    tokio::time::advance(half_timeout).await;
    drop(response);
    settle().await;
    assert!(slot.is_free());
    assert!(slot.block_request_unanswered());

    // A delivered block clears the record.
    let (request, response) = client_request(Request::BlocksByHash([block.hash()].into()));
    client_tx.send(request).await.expect("connection is open");
    outbound.next().await.expect("request is written");
    settle().await;
    peer_tx
        .send(Ok(Message::Block(block.clone().into())))
        .await
        .expect("connection is open");
    response
        .await
        .expect("connection responds")
        .expect("peer sent the block");
    settle().await;
    assert!(!slot.block_request_unanswered());

    // The connection's own timeout also records the request.
    let (request, response) = client_request(Request::BlocksByHash([block.hash()].into()));
    client_tx.send(request).await.expect("connection is open");
    outbound.next().await.expect("request is written");
    settle().await;
    tokio::time::advance(crate::constants::REQUEST_TIMEOUT).await;
    assert!(response.await.expect("connection responds").is_err());
    settle().await;
    assert!(slot.block_request_unanswered());

    connection_task.abort();
}

/// Returns a block advert for `hash`, and the receiver for its write result.
fn block_advert(
    hash: block::Hash,
) -> (
    crate::peer::block_gossip::BlockAdvert,
    tokio::sync::oneshot::Receiver<Result<(), crate::peer::block_gossip::BlockAdvertError>>,
) {
    let (result, delivery) = tokio::sync::oneshot::channel();
    (
        crate::peer::block_gossip::BlockAdvert { hash, result },
        delivery,
    )
}

/// Block adverts bypass pending responses without replacing their handlers or closing the client.
///
/// A peer at our tip never answers `getblocks`, so an advert queued behind it would otherwise
/// wait for the request to time out.
#[tokio::test]
async fn block_advert_while_awaiting_response() {
    let _init_guard = zakura_test::init();
    tokio::time::pause();

    tokio::time::timeout(Duration::from_secs(1), async {
        let (mut peer_tx, peer_rx) = mpsc::channel(1);
        let (
            mut connection,
            mut client_tx,
            mut inbound_service,
            mut peer_outbound_messages,
            shared_error_slot,
        ) = new_test_connection();
        let (block_advert_tx, block_advert_rx) =
            tokio::sync::mpsc::channel(crate::peer::block_gossip::BLOCK_ADVERT_QUEUE);
        connection.block_advert_rx = block_advert_rx;
        let connection_task = tokio::spawn(connection.run(peer_rx));

        // Exercise the idle path before leaving a getblocks request awaiting its response.
        let idle_hash = block::Hash([1; 32]);
        let (advert, idle_delivery) = block_advert(idle_hash);
        block_advert_tx.try_send(advert).unwrap();
        assert_eq!(
            peer_outbound_messages.next().await,
            Some(Message::Inv(vec![InventoryHash::Block(idle_hash)]))
        );
        idle_delivery.await.unwrap().unwrap();

        let (request_tx, mut request_rx) = oneshot::channel();
        client_tx
            .try_send(ClientRequest {
                request: Request::FindBlocks {
                    known_blocks: vec![idle_hash],
                    stop: None,
                },
                tx: request_tx,
                inv_collector: None,
                transient_addr: None,
                span: Span::current(),
            })
            .unwrap();
        assert!(matches!(
            peer_outbound_messages.next().await,
            Some(Message::GetBlocks { .. })
        ));

        let busy_hash = block::Hash([2; 32]);
        let (advert, busy_delivery) = block_advert(busy_hash);
        block_advert_tx.try_send(advert).unwrap();
        assert_eq!(
            peer_outbound_messages.next().await,
            Some(Message::Inv(vec![InventoryHash::Block(busy_hash)]))
        );
        busy_delivery.await.unwrap().unwrap();
        assert!(request_rx.try_recv().unwrap().is_none());

        // Closing only the advert channel must leave the original request alive.
        drop(block_advert_tx);
        let next_hash = block::Hash([3; 32]);
        peer_tx
            .try_send(Ok(Message::Inv(vec![InventoryHash::Block(next_hash)])))
            .unwrap();
        // A one-hash inv is also a block announcement, so it reaches the inbound service too.
        tokio::join!(
            async {
                inbound_service
                    .expect_request(Request::AdvertiseBlock(next_hash.into(), None))
                    .await
                    .respond(Response::Nil);
            },
            async {
                assert_eq!(
                    request_rx.await.unwrap().unwrap(),
                    Response::BlockHashes(vec![next_hash])
                );
            }
        );
        assert!(shared_error_slot.try_get_error().is_none());

        drop(client_tx);
        connection_task.await.unwrap();
    })
    .await
    .expect("adverts and the original response finish before the request timeout");
}

/// A block advert sent as an ordinary client request is answered with an error, not written as
/// an `inv`: only the advert queue writes adverts. The connection stays usable.
#[tokio::test]
async fn block_advert_as_ordinary_request_is_an_error() {
    let _init_guard = zakura_test::init();
    tokio::time::pause();

    tokio::time::timeout(Duration::from_secs(1), async {
        let (_peer_tx, peer_rx) = mpsc::channel(1);
        let (
            connection,
            mut client_tx,
            _inbound_service,
            mut peer_outbound_messages,
            shared_error_slot,
        ) = new_test_connection();
        let connection_task = tokio::spawn(connection.run(peer_rx));

        let (request_tx, request_rx) = oneshot::channel();
        client_tx
            .try_send(ClientRequest {
                request: Request::AdvertiseBlock(block::Hash([5; 32]).into(), None),
                tx: request_tx,
                inv_collector: None,
                transient_addr: None,
                span: Span::current(),
            })
            .unwrap();
        let error = request_rx
            .await
            .unwrap()
            .expect_err("an advert sent as an ordinary request fails");
        assert_eq!(error.inner_debug(), "AdvertRequiresDedicatedService");
        assert!(peer_outbound_messages.next().now_or_never().is_none());
        assert!(shared_error_slot.try_get_error().is_none());

        drop(client_tx);
        connection_task.await.unwrap();
    })
    .await
    .expect("the request is answered before the request timeout");
}

/// A pending GetBlocks response must not be displaced by a simultaneous unsolicited advert.
#[tokio::test]
async fn getblocks_response_precedes_ready_block_advert() {
    let _init_guard = zakura_test::init();
    tokio::time::pause();

    tokio::time::timeout(Duration::from_secs(1), async {
        let (mut peer_tx, peer_rx) = mpsc::channel(1);
        let (
            mut connection,
            client_tx,
            mut inbound_service,
            mut peer_outbound_messages,
            shared_error_slot,
        ) = new_test_connection();
        let (block_advert_tx, block_advert_rx) =
            tokio::sync::mpsc::channel(crate::peer::block_gossip::BLOCK_ADVERT_QUEUE);
        connection.block_advert_rx = block_advert_rx;
        let response_hash = block::Hash([3; 32]);
        let advert_hash = block::Hash([4; 32]);

        // Queue both inputs before starting the loop, making selection order deterministic.
        peer_tx
            .try_send(Ok(Message::GetBlocks {
                known_blocks: Vec::new(),
                stop: None,
            }))
            .unwrap();
        let (advert, _delivery) = block_advert(advert_hash);
        block_advert_tx.try_send(advert).unwrap();
        let connection_task = tokio::spawn(connection.run(peer_rx));

        tokio::join!(
            async {
                inbound_service
                    .expect_request(Request::FindBlocks {
                        known_blocks: Vec::new(),
                        stop: None,
                    })
                    .await
                    .respond(Response::BlockHashes(vec![response_hash]));
            },
            async {
                assert_eq!(
                    peer_outbound_messages.next().await,
                    Some(Message::Inv(vec![InventoryHash::Block(response_hash)]))
                );
                assert_eq!(
                    peer_outbound_messages.next().await,
                    Some(Message::Inv(vec![InventoryHash::Block(advert_hash)]))
                );
            }
        );
        assert!(shared_error_slot.try_get_error().is_none());

        drop(client_tx);
        connection_task.await.unwrap();
    })
    .await
    .expect("the response and advert are both delivered");
}

/// A peer that always has a message ready cannot keep a queued block advert from being written:
/// once its messages have kept the advert waiting for the priority delay, the advert goes before
/// the next one, and so do the adverts queued behind it. Until then, and again for a later
/// advert, its messages go first.
#[tokio::test(start_paused = true)]
async fn block_advert_is_written_during_a_run_of_peer_messages() {
    use crate::peer::connection::WAITING_ADVERT_PRIORITY_DELAY;

    let _init_guard = zakura_test::init();

    let (
        mut connection,
        client_tx,
        _inbound_service,
        mut peer_outbound_messages,
        shared_error_slot,
    ) = new_test_connection();
    let (block_advert_tx, block_advert_rx) =
        tokio::sync::mpsc::channel(crate::peer::block_gossip::BLOCK_ADVERT_QUEUE);
    connection.block_advert_rx = block_advert_rx;

    // The peer always has a ping ready, which the connection answers by itself.
    let pings = futures::stream::repeat_with(|| Ok(Message::Ping(Nonce::default())));
    let connection_task = tokio::spawn(connection.run(pings));

    for (round, advert_hash) in [block::Hash([5; 32]), block::Hash([6; 32])]
        .into_iter()
        .enumerate()
    {
        if round == 1 {
            // An advert the advertiser gave up on is skipped without delaying the one behind it.
            let (stale_advert, stale_delivery) = block_advert(block::Hash([7; 32]));
            drop(stale_delivery);
            block_advert_tx.try_send(stale_advert).unwrap();
        }
        let (advert, delivery) = block_advert(advert_hash);
        block_advert_tx.try_send(advert).unwrap();
        assert_advert_written_after_delay(&mut peer_outbound_messages, advert_hash).await;
        delivery.await.unwrap().unwrap();
    }

    // A burst of adverts queued within one delay is written in consecutive turns once the first
    // is due, so the last one is not kept waiting past the advertiser's budget.
    let burst = [
        block::Hash([8; 32]),
        block::Hash([9; 32]),
        block::Hash([10; 32]),
    ];
    let mut deliveries = Vec::new();
    for advert_hash in burst {
        let (advert, delivery) = block_advert(advert_hash);
        block_advert_tx.try_send(advert).unwrap();
        deliveries.push(delivery);
    }
    for _ in 0..PEER_MESSAGES_FIRST {
        assert!(
            matches!(peer_outbound_messages.next().await, Some(Message::Pong(_))),
            "peer messages go first until the burst has waited the priority delay",
        );
    }
    tokio::time::advance(WAITING_ADVERT_PRIORITY_DELAY).await;
    let mut pongs_after_delay = 0;
    let mut written = Vec::new();
    while written.len() < burst.len() {
        match peer_outbound_messages.next().await {
            Some(Message::Pong(_)) => pongs_after_delay += 1,
            Some(Message::Inv(inv)) => written.push(inv),
            other => panic!("unexpected message to the peer: {other:?}"),
        }
        assert!(
            pongs_after_delay <= 1,
            "no peer message goes between the adverts of a burst once the first is due",
        );
    }
    assert_eq!(
        written,
        burst.map(|hash| vec![InventoryHash::Block(hash)]).to_vec()
    );
    for delivery in deliveries {
        delivery.await.unwrap().unwrap();
    }
    assert!(shared_error_slot.try_get_error().is_none());

    connection_task.abort();
    drop(client_tx);
}

/// The same holds while a request of ours is pending, the busy loop's realistic case: a peer
/// that streams messages after our `getblocks` cannot keep a queued block advert from being
/// written, and our request stays pending.
#[tokio::test(start_paused = true)]
async fn block_advert_is_written_during_a_run_of_peer_messages_while_a_request_is_pending() {
    let _init_guard = zakura_test::init();

    let (
        mut connection,
        mut client_tx,
        _inbound_service,
        mut peer_outbound_messages,
        shared_error_slot,
    ) = new_test_connection();
    let (block_advert_tx, block_advert_rx) =
        tokio::sync::mpsc::channel(crate::peer::block_gossip::BLOCK_ADVERT_QUEUE);
    connection.block_advert_rx = block_advert_rx;

    // The peer is silent until our request is on the wire, then always has a ping ready.
    let (wake_peer, peer_woken) = oneshot::channel();
    let pings = futures::stream::once(peer_woken)
        .flat_map(|_| futures::stream::repeat_with(|| Ok(Message::Ping(Nonce::default()))));
    let connection_task = tokio::spawn(connection.run(pings));

    let mut response = send_find_blocks(&mut client_tx, &mut peer_outbound_messages).await;
    wake_peer.send(()).unwrap();

    let advert_hash = block::Hash([5; 32]);
    let (advert, delivery) = block_advert(advert_hash);
    block_advert_tx.try_send(advert).unwrap();
    assert_advert_written_after_delay(&mut peer_outbound_messages, advert_hash).await;
    delivery.await.unwrap().unwrap();

    assert!(
        matches!(response.try_recv(), Ok(None)),
        "the request is still pending after the advert is written",
    );
    assert!(shared_error_slot.try_get_error().is_none());

    connection_task.abort();
    drop(client_tx);
}

/// Peer messages answered while an advert waits, enough to show its messages go first.
const PEER_MESSAGES_FIRST: usize = 8;

/// Asserts that the peer's pings are answered first while the clock stands still, and that the
/// `inv` for `advert_hash` goes before the next pong once the clock has moved by the priority
/// delay.
async fn assert_advert_written_after_delay(
    peer_outbound_messages: &mut mpsc::Receiver<Message>,
    advert_hash: block::Hash,
) {
    use crate::peer::connection::WAITING_ADVERT_PRIORITY_DELAY;

    // The clock only moves when this test moves it, so the delay has not passed.
    for _ in 0..PEER_MESSAGES_FIRST {
        assert!(
            matches!(peer_outbound_messages.next().await, Some(Message::Pong(_))),
            "peer messages go first until the advert has waited the priority delay",
        );
    }

    tokio::time::advance(WAITING_ADVERT_PRIORITY_DELAY).await;
    // The connection may have answered one more ping before it saw the clock move.
    let mut pongs_after_delay = 0;
    loop {
        match peer_outbound_messages.next().await {
            Some(Message::Pong(_)) => pongs_after_delay += 1,
            Some(Message::Inv(inv)) => {
                assert_eq!(inv, vec![InventoryHash::Block(advert_hash)]);
                break;
            }
            other => panic!("unexpected message to the peer: {other:?}"),
        }
        assert!(
            pongs_after_delay <= 1,
            "the advert goes before the next peer message once the delay has passed",
        );
    }
}

/// Sends a `FindBlocks` request on `client_tx` and waits for its `getblocks` to reach the wire.
///
/// Returns the receiver for the request's response.
async fn send_find_blocks(
    client_tx: &mut mpsc::Sender<ClientRequest>,
    peer_outbound_messages: &mut mpsc::Receiver<Message>,
) -> oneshot::Receiver<Result<Response, crate::peer::SharedPeerError>> {
    let (request_tx, request_rx) = oneshot::channel();
    client_tx
        .try_send(ClientRequest {
            request: Request::FindBlocks {
                known_blocks: vec![block::Hash([1; 32])],
                stop: None,
            },
            tx: request_tx,
            inv_collector: None,
            transient_addr: None,
            span: Span::current(),
        })
        .unwrap();
    assert!(matches!(
        peer_outbound_messages.next().await,
        Some(Message::GetBlocks { .. })
    ));
    request_rx
}

/// A one-hash block `inv` that arrives while `getblocks` is pending answers the request and is
/// also handed to the inbound service as a block advert.
///
/// A peer announces a new block with a one-hash `inv`, which the connection cannot tell from a
/// reply. If it were only the reply, the gossip downloader would never see the block, and the
/// syncer would fetch it only after every request in its discovery round had finished.
#[tokio::test]
async fn single_block_inv_during_getblocks_is_also_gossiped() {
    let _init_guard = zakura_test::init();
    tokio::time::pause();

    tokio::time::timeout(Duration::from_secs(1), async {
        let (mut peer_tx, peer_rx) = mpsc::channel(1);
        let (
            connection,
            mut client_tx,
            mut inbound_service,
            mut peer_outbound_messages,
            shared_error_slot,
        ) = new_test_connection();
        let connection_task = tokio::spawn(connection.run(peer_rx));

        let request_rx = send_find_blocks(&mut client_tx, &mut peer_outbound_messages).await;

        let announced_hash = block::Hash([2; 32]);
        peer_tx
            .try_send(Ok(Message::Inv(vec![InventoryHash::Block(announced_hash)])))
            .unwrap();

        // The reply is delivered before the advert reaches the inbound service.
        assert_eq!(
            request_rx.await.unwrap().unwrap(),
            Response::BlockHashes(vec![announced_hash])
        );
        inbound_service
            .expect_request(Request::AdvertiseBlock(announced_hash.into(), None))
            .await
            .respond(Response::Nil);
        assert!(shared_error_slot.try_get_error().is_none());

        drop(client_tx);
        connection_task.await.unwrap();
    })
    .await
    .expect("the response and the block advert are both delivered");
}

/// The `getblocks` reply is not lost when the inbound service sheds the block advert that the
/// same one-hash `inv` produced, whether or not the overload policy then drops the connection.
#[tokio::test]
async fn single_block_inv_during_getblocks_survives_inbound_overload() {
    let _init_guard = zakura_test::init();
    tokio::time::pause();

    tokio::time::timeout(Duration::from_secs(1), async {
        let (mut peer_tx, peer_rx) = mpsc::channel(1);
        let (connection, mut client_tx, mut inbound_service, mut peer_outbound_messages, _) =
            new_test_connection();
        let connection_task = tokio::spawn(connection.run(peer_rx));

        let request_rx = send_find_blocks(&mut client_tx, &mut peer_outbound_messages).await;

        let announced_hash = block::Hash([2; 32]);
        peer_tx
            .try_send(Ok(Message::Inv(vec![InventoryHash::Block(announced_hash)])))
            .unwrap();

        assert_eq!(
            request_rx.await.unwrap().unwrap(),
            Response::BlockHashes(vec![announced_hash])
        );
        inbound_service
            .expect_request(Request::AdvertiseBlock(announced_hash.into(), None))
            .await
            .respond(Err(Overloaded::new()));

        drop(client_tx);
        connection_task.await.unwrap();
    })
    .await
    .expect("the reply is delivered although the advert was shed");
}

/// A block `inv` with several hashes is a `getblocks` reply only, never a block advert.
#[tokio::test]
async fn multi_block_inv_during_getblocks_is_only_the_reply() {
    let _init_guard = zakura_test::init();
    tokio::time::pause();

    tokio::time::timeout(Duration::from_secs(1), async {
        let (mut peer_tx, peer_rx) = mpsc::channel(1);
        let (
            connection,
            mut client_tx,
            mut inbound_service,
            mut peer_outbound_messages,
            shared_error_slot,
        ) = new_test_connection();
        let connection_task = tokio::spawn(connection.run(peer_rx));

        let request_rx = send_find_blocks(&mut client_tx, &mut peer_outbound_messages).await;

        let hashes = vec![block::Hash([2; 32]), block::Hash([3; 32])];
        peer_tx
            .try_send(Ok(Message::Inv(
                hashes.iter().copied().map(InventoryHash::Block).collect(),
            )))
            .unwrap();

        assert_eq!(
            request_rx.await.unwrap().unwrap(),
            Response::BlockHashes(hashes)
        );
        inbound_service.expect_no_requests().await;
        assert!(shared_error_slot.try_get_error().is_none());

        drop(client_tx);
        connection_task.await.unwrap();
    })
    .await
    .expect("the reply is delivered without a block advert");
}

/// The `getblocks` reply still arrives when the inbound service fails the block advert that the
/// same one-hash `inv` produced, and the connection stays open.
#[tokio::test]
async fn single_block_inv_during_getblocks_survives_inbound_error() {
    let _init_guard = zakura_test::init();
    tokio::time::pause();

    tokio::time::timeout(Duration::from_secs(1), async {
        let (mut peer_tx, peer_rx) = mpsc::channel(1);
        let (
            connection,
            mut client_tx,
            mut inbound_service,
            mut peer_outbound_messages,
            shared_error_slot,
        ) = new_test_connection();
        let connection_task = tokio::spawn(connection.run(peer_rx));

        let request_rx = send_find_blocks(&mut client_tx, &mut peer_outbound_messages).await;

        let announced_hash = block::Hash([2; 32]);
        peer_tx
            .try_send(Ok(Message::Inv(vec![InventoryHash::Block(announced_hash)])))
            .unwrap();

        tokio::join!(
            async {
                inbound_service
                    .expect_request(Request::AdvertiseBlock(announced_hash.into(), None))
                    .await
                    .respond(Err("synthetic inbound service error"));
            },
            async {
                assert_eq!(
                    request_rx.await.unwrap().unwrap(),
                    Response::BlockHashes(vec![announced_hash])
                );
            }
        );
        assert!(shared_error_slot.try_get_error().is_none());

        // The connection still serves requests after the failed advert.
        let request_rx = send_find_blocks(&mut client_tx, &mut peer_outbound_messages).await;
        let hashes = vec![block::Hash([3; 32]), block::Hash([4; 32])];
        peer_tx
            .try_send(Ok(Message::Inv(
                hashes.iter().copied().map(InventoryHash::Block).collect(),
            )))
            .unwrap();
        assert_eq!(
            request_rx.await.unwrap().unwrap(),
            Response::BlockHashes(hashes)
        );
        assert!(shared_error_slot.try_get_error().is_none());

        drop(client_tx);
        connection_task.await.unwrap();
    })
    .await
    .expect("the reply is delivered although the advert failed");
}

/// An unsolicited transaction completes a pending transaction batch before inbound processing,
/// even when the inbound service fails. Both partial and not-found replies keep the connection
/// usable for later requests.
#[tokio::test]
async fn transaction_reply_precedes_unsolicited_transaction_inbound_error() {
    use zakura_chain::{
        serialization::ZcashDeserializeInto,
        transaction::{self, UnminedTx, UnminedTxId},
    };

    let _init_guard = zakura_test::init();
    tokio::time::pause();

    let requested_block: block::Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let unrelated_block: block::Block = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let requested = UnminedTx::from(requested_block.transactions[0].clone());
    let unrelated = UnminedTx::from(unrelated_block.transactions[0].clone());
    let missing = UnminedTxId::from_legacy_id(transaction::Hash([9; 32]));
    assert_ne!(requested.id(), unrelated.id());

    tokio::time::timeout(Duration::from_secs(1), async {
        for received_requested in [false, true] {
            let (mut peer_tx, peer_rx) = mpsc::channel(1);
            let (
                connection,
                mut client_tx,
                mut inbound_service,
                mut peer_outbound_messages,
                shared_error_slot,
            ) = new_test_connection();
            let connection_task = tokio::spawn(connection.run(peer_rx));
            let (request_tx, request_rx) = oneshot::channel();
            client_tx
                .try_send(ClientRequest {
                    request: Request::TransactionsById(HashSet::from([requested.id(), missing])),
                    tx: request_tx,
                    inv_collector: None,
                    transient_addr: None,
                    span: Span::current(),
                })
                .unwrap();
            assert!(matches!(
                peer_outbound_messages.next().await,
                Some(Message::GetData(_))
            ));

            if received_requested {
                peer_tx
                    .send(Ok(Message::Tx(requested.clone())))
                    .await
                    .unwrap();
            }
            peer_tx
                .send(Ok(Message::Tx(unrelated.clone())))
                .await
                .unwrap();

            // Do not answer the inbound request until the transaction reply has arrived.
            let response = request_rx.await.unwrap();
            if received_requested {
                assert_eq!(
                    response.unwrap(),
                    Response::Transactions(vec![
                        crate::InventoryResponse::Available((requested.clone(), None)),
                        crate::InventoryResponse::Missing(missing),
                    ])
                );
            } else {
                assert_eq!(
                    response.unwrap_err().not_found_class(),
                    Some(crate::NotFoundClass::Response)
                );
            }
            inbound_service
                .expect_request(Request::PushTransaction(unrelated.clone(), None))
                .await
                .respond(Err("synthetic inbound service error"));

            let request_rx = send_find_blocks(&mut client_tx, &mut peer_outbound_messages).await;
            let hashes = vec![block::Hash([3; 32]), block::Hash([4; 32])];
            peer_tx
                .send(Ok(Message::Inv(
                    hashes.iter().copied().map(InventoryHash::Block).collect(),
                )))
                .await
                .unwrap();
            assert_eq!(
                request_rx.await.unwrap().unwrap(),
                Response::BlockHashes(hashes)
            );
            assert!(shared_error_slot.try_get_error().is_none());

            drop(client_tx);
            connection_task.await.unwrap();
        }
    })
    .await
    .expect("transaction replies precede inbound failures and later requests still succeed");
}

/// A connection rechecks bans while a request is awaiting its response, and does not write an
/// advert to a banned peer. The pending request and the connection are unaffected.
#[tokio::test]
async fn queued_block_advert_is_revoked_by_ban_while_awaiting_response() {
    queued_block_advert_is_revoked_by_ban(BanAddressForm::Same).await;
}

/// A peer connected through its IPv4-mapped IPv6 address, as on a dual-stack listener, is
/// still revoked by a ban recorded for its canonical IPv4 address.
#[tokio::test]
async fn queued_block_advert_is_revoked_by_canonical_ban_of_mapped_ipv6_peer() {
    queued_block_advert_is_revoked_by_ban(BanAddressForm::MappedPeerCanonicalBan).await;
}

/// How the banned address relates to the connection's peer address.
enum BanAddressForm {
    /// The ban holds the connection's own peer address.
    Same,
    /// The peer connected as `::ffff:A.B.C.D`, and the ban holds `A.B.C.D`.
    MappedPeerCanonicalBan,
}

async fn queued_block_advert_is_revoked_by_ban(form: BanAddressForm) {
    let _init_guard = zakura_test::init();
    tokio::time::pause();

    tokio::time::timeout(Duration::from_secs(1), async {
        let (_peer_tx, peer_rx) = mpsc::channel(1);
        // The protected test connection has a real peer address that a ban can match.
        let (
            mut connection,
            mut client_tx,
            _inbound_service,
            mut peer_outbound_messages,
            shared_error_slot,
        ) = new_protected_test_connection();
        let (block_advert_tx, block_advert_rx) =
            tokio::sync::mpsc::channel(crate::peer::block_gossip::BLOCK_ADVERT_QUEUE);
        connection.block_advert_rx = block_advert_rx;
        let banned_ip = match form {
            BanAddressForm::Same => connection
                .connection_info
                .connected_addr
                .get_transient_addr()
                .expect("the protected test connection has a peer address")
                .ip(),
            BanAddressForm::MappedPeerCanonicalBan => {
                let canonical = std::net::Ipv4Addr::new(203, 0, 113, 7);
                let mut info = (*connection.connection_info).clone();
                info.connected_addr = crate::peer::ConnectedAddr::new_inbound_direct(
                    std::net::SocketAddr::new(canonical.to_ipv6_mapped().into(), 8233).into(),
                );
                connection.connection_info = std::sync::Arc::new(info);
                canonical.into()
            }
        };
        connection.bans = crate::BannedIps::with_banned_ip(banned_ip);
        let connection_task = tokio::spawn(connection.run(peer_rx));

        let (request_tx, mut request_rx) = oneshot::channel();
        client_tx
            .try_send(ClientRequest {
                request: Request::FindBlocks {
                    known_blocks: vec![block::Hash([1; 32])],
                    stop: None,
                },
                tx: request_tx,
                inv_collector: None,
                transient_addr: None,
                span: Span::current(),
            })
            .unwrap();
        assert!(matches!(
            peer_outbound_messages.next().await,
            Some(Message::GetBlocks { .. })
        ));

        let (advert, delivery) = block_advert(block::Hash([2; 32]));
        block_advert_tx.try_send(advert).unwrap();
        assert!(matches!(
            delivery.await.unwrap(),
            Err(crate::peer::block_gossip::BlockAdvertError::AuthorizationRevoked)
        ));
        assert!(peer_outbound_messages.next().now_or_never().is_none());
        assert!(request_rx.try_recv().unwrap().is_none());
        assert!(shared_error_slot.try_get_error().is_none());

        connection_task.abort();
    })
    .await
    .expect("the revoked advert is answered before the request timeout");
}
