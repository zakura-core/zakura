//! Tests for peer connections

#![allow(clippy::unwrap_in_result)]

use std::{
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    panic,
    sync::Arc,
    task::{Context, Poll},
};

use chrono::Utc;
use futures::{channel::mpsc, sink::SinkMapErr, Sink, SinkExt};

use zakura_chain::{block::Height, serialization::SerializationError};
use zakura_test::mock_service::MockService;

use crate::{
    constants::CURRENT_NETWORK_PROTOCOL_VERSION,
    peer::{ClientRequest, ConnectedAddr, Connection, ConnectionInfo, ErrorSlot},
    peer_set::ActiveConnectionCounter,
    protocol::{
        external::{AddrInVersion, Message},
        types::{Nonce, PeerServices},
    },
    Request, Response, VersionMessage,
};

mod prop;
mod vectors;

/// Test that dropping a peer sender does not require a Tokio timer context.
#[test]
fn peer_tx_drop_does_not_require_tokio_timer() {
    let result = panic::catch_unwind(|| drop(super::peer_tx::PeerTx::from(NeverClosingSink)));

    assert!(result.is_ok(), "dropping a peer sender must not panic");
}

/// A sink that accepts messages but never finishes closing.
#[derive(Clone, Debug)]
struct NeverClosingSink;

impl Sink<Message> for NeverClosingSink {
    type Error = SerializationError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: std::pin::Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
}

/// Creates a new [`Connection`] instance for testing.
fn new_test_connection<A>() -> (
    Connection<
        MockService<Request, Response, A>,
        SinkMapErr<mpsc::Sender<Message>, fn(mpsc::SendError) -> SerializationError>,
    >,
    mpsc::Sender<ClientRequest>,
    MockService<Request, Response, A>,
    mpsc::Receiver<Message>,
    ErrorSlot,
) {
    new_test_connection_with_protection(false)
}

/// Creates a new [`Connection`] marked as an operator-configured protected peer
/// (exempt from the inbound-overload connection drop).
fn new_protected_test_connection<A>() -> (
    Connection<
        MockService<Request, Response, A>,
        SinkMapErr<mpsc::Sender<Message>, fn(mpsc::SendError) -> SerializationError>,
    >,
    mpsc::Sender<ClientRequest>,
    MockService<Request, Response, A>,
    mpsc::Receiver<Message>,
    ErrorSlot,
) {
    new_test_connection_with_protection(true)
}

/// Creates a new [`Connection`] instance for testing, setting whether it is an
/// operator-configured protected peer.
fn new_test_connection_with_protection<A>(
    is_protected_peer: bool,
) -> (
    Connection<
        MockService<Request, Response, A>,
        SinkMapErr<mpsc::Sender<Message>, fn(mpsc::SendError) -> SerializationError>,
    >,
    mpsc::Sender<ClientRequest>,
    MockService<Request, Response, A>,
    mpsc::Receiver<Message>,
    ErrorSlot,
) {
    let mock_inbound_service = MockService::build().finish();
    let (client_tx, client_rx) = mpsc::channel(0);
    let shared_error_slot = ErrorSlot::default();

    // Normally the network has more capacity than the sender's single implicit slot,
    // but the smaller capacity makes some tests easier.
    let (peer_tx, peer_rx) = mpsc::channel(0);

    let error_converter: fn(mpsc::SendError) -> SerializationError = |_| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "peer outbound message stream was closed",
        )
        .into()
    };
    let peer_tx = peer_tx.sink_map_err(error_converter);

    let fake_addr: SocketAddr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4).into();
    let fake_version = CURRENT_NETWORK_PROTOCOL_VERSION;
    let fake_services = PeerServices::default();

    let remote = VersionMessage {
        version: fake_version,
        services: fake_services,
        timestamp: Utc::now(),
        address_recv: AddrInVersion::new(fake_addr, fake_services),
        address_from: AddrInVersion::new(fake_addr, fake_services),
        nonce: Nonce::default(),
        user_agent: "connection test".to_string(),
        start_height: Height(0),
        relay: true,
    };

    let connected_addr = if is_protected_peer {
        ConnectedAddr::new_inbound_direct(fake_addr.into())
    } else {
        ConnectedAddr::Isolated
    };
    let connection_info = ConnectionInfo {
        connected_addr,
        local: remote.clone(),
        remote,
        negotiated_version: fake_version,
        is_protected_peer,
    };
    let addr_label = connection_info.connected_addr.get_transient_addr_label();

    let connection = Connection::new(
        mock_inbound_service.clone(),
        client_rx,
        shared_error_slot.clone(),
        peer_tx,
        ActiveConnectionCounter::new_counter().track_connection(),
        Arc::new(connection_info),
        addr_label,
        Vec::new(),
    );

    (
        connection,
        client_tx,
        mock_inbound_service,
        peer_rx,
        shared_error_slot,
    )
}

/// Creates a connection whose peer writer never finishes closing.
fn new_never_closing_test_connection<A>(
    connection_tracker: crate::peer_set::ConnectionTracker,
) -> (
    Connection<MockService<Request, Response, A>, NeverClosingSink>,
    mpsc::Sender<ClientRequest>,
    ErrorSlot,
) {
    let mock_inbound_service = MockService::build().finish();
    let (client_tx, client_rx) = mpsc::channel(0);
    let shared_error_slot = ErrorSlot::default();

    let fake_addr: SocketAddr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4).into();
    let fake_version = CURRENT_NETWORK_PROTOCOL_VERSION;
    let fake_services = PeerServices::default();
    let remote = VersionMessage {
        version: fake_version,
        services: fake_services,
        timestamp: Utc::now(),
        address_recv: AddrInVersion::new(fake_addr, fake_services),
        address_from: AddrInVersion::new(fake_addr, fake_services),
        nonce: Nonce::default(),
        user_agent: "connection test".to_string(),
        start_height: Height(0),
        relay: true,
    };
    let connection_info = ConnectionInfo {
        connected_addr: ConnectedAddr::Isolated,
        local: remote.clone(),
        remote,
        negotiated_version: fake_version,
        is_protected_peer: false,
    };
    let addr_label = connection_info.connected_addr.get_transient_addr_label();

    let connection = Connection::new(
        mock_inbound_service,
        client_rx,
        shared_error_slot.clone(),
        NeverClosingSink,
        connection_tracker,
        Arc::new(connection_info),
        addr_label,
        Vec::new(),
    );

    (connection, client_tx, shared_error_slot)
}
