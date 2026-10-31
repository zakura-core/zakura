//! A real [`Client`] driving a real [`Connection`] over in-memory peer channels.
//!
//! Unlike [`ClientTestHarness`](super::ClientTestHarness), which hands raw [`ClientRequest`]s to
//! the test, this keeps the connection state machine between the client and the remote peer. Tests
//! use it to observe exactly when a request is written to the peer.

use std::{io, sync::Arc};

use chrono::Utc;
use futures::{
    channel::{mpsc, oneshot},
    future, SinkExt,
};
use tokio::sync::broadcast;

use zakura_chain::{block::Height, serialization::SerializationError};
use zakura_test::mock_service::{MockService, PanicAssertion};

use crate::{
    constants::CURRENT_NETWORK_PROTOCOL_VERSION,
    peer::{Client, ConnectedAddr, Connection, ConnectionInfo, ErrorSlot},
    peer_set::ActiveConnectionCounter,
    protocol::{
        external::{AddrInVersion, Message},
        types::{Nonce, PeerServices},
    },
    BoxError, PeerSocketAddr, Request, Response, VersionMessage,
};

/// A [`Client`] whose requests are handled by a spawned [`Connection`] task.
pub(crate) struct ConnectedClient {
    /// The client half of the connection.
    pub client: Client,

    /// Delivers messages from the remote peer to the connection.
    pub peer_inbound: mpsc::Sender<Result<Message, SerializationError>>,

    /// Receives the messages the connection wrote to the remote peer.
    pub peer_outbound: mpsc::Receiver<Message>,

    /// The inbound service that answers the remote peer's requests and advertisements.
    pub inbound_service: MockService<Request, Response, PanicAssertion>,
}

impl ConnectedClient {
    /// Spawns a connection to a mocked remote peer at `addr`, and returns its client.
    ///
    /// Must be called inside a Tokio runtime.
    pub fn spawn(addr: PeerSocketAddr) -> Self {
        let inbound_service = MockService::build().for_unit_tests();
        let (server_tx, server_rx) = mpsc::channel(0);
        let error_slot = ErrorSlot::default();

        // Leave room for several writes so tests can inspect them after the fact.
        let (peer_tx, peer_outbound) = mpsc::channel(16);
        let error_converter: fn(mpsc::SendError) -> SerializationError = |_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "peer outbound message stream was closed",
            )
            .into()
        };
        let peer_tx = peer_tx.sink_map_err(error_converter);
        let (peer_inbound, peer_rx) = mpsc::channel(16);

        let services = PeerServices::NODE_NETWORK;
        let remote = VersionMessage {
            version: CURRENT_NETWORK_PROTOCOL_VERSION,
            services,
            timestamp: Utc::now(),
            address_recv: AddrInVersion::new(addr, services),
            address_from: AddrInVersion::new(addr, services),
            nonce: Nonce::default(),
            user_agent: "connected client test".to_string(),
            start_height: Height(0),
            relay: true,
        };
        let connection_info = Arc::new(ConnectionInfo {
            connected_addr: ConnectedAddr::new_outbound_direct(addr),
            local: remote.clone(),
            remote,
            negotiated_version: CURRENT_NETWORK_PROTOCOL_VERSION,
            is_protected_peer: false,
        });
        let addr_label = connection_info.connected_addr.get_transient_addr_label();

        let connection = Connection::new(
            inbound_service.clone(),
            server_rx,
            error_slot.clone(),
            peer_tx,
            ActiveConnectionCounter::new_counter().track_connection(),
            connection_info.clone(),
            addr_label,
            Vec::new(),
        );
        let block_uploads = connection.block_uploads.clone();
        let request_slot = connection.request_slot.clone();
        let connection_task = tokio::spawn(connection.run(peer_rx));

        // The client shuts down if the heartbeat task drops its shutdown receiver,
        // so the stand-in heartbeat task holds it until it is aborted.
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let heartbeat_task = tokio::spawn(async move {
            let _shutdown_rx = shutdown_rx;
            future::pending::<Result<(), BoxError>>().await
        });

        let client = Client {
            block_uploads,
            request_slot,
            peer_registry_updater: None,
            connection_info,
            shutdown_tx: Some(shutdown_tx),
            server_tx,
            inv_collector: broadcast::channel(16).0,
            error_slot,
            connection_task,
            heartbeat_task,
        };

        ConnectedClient {
            client,
            peer_inbound,
            peer_outbound,
            inbound_service,
        }
    }
}
