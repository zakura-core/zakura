//! A peer connection service wrapper type to handle load tracking and provide access to the
//! reported protocol version.

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    task::{Context, Poll},
};

use tower::{
    load::{Load, PeakEwma},
    Service,
};

use crate::{
    constants::{EWMA_DECAY_TIME_NANOS, EWMA_DEFAULT_RTT},
    peer::{Client, ConnectedAddr, ConnectionInfo},
    peer_registry::sanitize_subversion,
    protocol::external::{canonical_socket_addr, types::PeerServices, types::Version},
};

/// A client service wrapper that keeps track of its load.
///
/// It also keeps track of the peer's reported protocol version.
#[derive(Debug)]
pub struct LoadTrackedClient {
    /// Queues paced block inventory independently of ordinary request readiness.
    pub(crate) block_gossip: super::block_gossip::BlockGossipPeer,

    /// A service representing a connected peer, wrapped in a load tracker.
    service: PeakEwma<Client>,

    /// Reports whether the connection can write a request as soon as it is sent.
    ///
    /// A copy of the client's slot, because [`PeakEwma`] does not expose the inner client.
    request_slot: super::client::RequestSlot,

    /// The metadata for the connected peer `service`.
    connection_info: Arc<ConnectionInfo>,

    /// The connection's process-local number in the trace files.
    connection_id: u64,

    /// The sanitized user agent the peer advertised in its handshake, for peer traces.
    user_agent: Arc<str>,
}

/// Create a new [`LoadTrackedClient`] wrapping the provided `client` service.
impl From<Client> for LoadTrackedClient {
    fn from(client: Client) -> Self {
        let connection_info = client.connection_info.clone();
        let block_gossip = super::block_gossip::BlockGossipPeer {
            sender: client.block_advert_tx.clone(),
            address: connection_info.connected_addr.get_transient_addr(),
            uploads: client.block_uploads.clone(),
            announced: client.announced_blocks.clone(),
            registry: client.peer_registry_updater.clone(),
            version: connection_info.remote.version,
            sidecar: false,
        };

        let request_slot = client.request_slot.clone();
        let connection_id = client.connection_id;
        let user_agent = sanitize_subversion(&connection_info.remote.user_agent).into();

        let service = PeakEwma::new(
            client,
            EWMA_DEFAULT_RTT,
            EWMA_DECAY_TIME_NANOS,
            tower::load::CompleteOnResponse::default(),
        );

        LoadTrackedClient {
            block_gossip,
            service,
            request_slot,
            connection_info,
            connection_id,
            user_agent,
        }
    }
}

impl LoadTrackedClient {
    /// Retrieve the peer's reported protocol version.
    pub fn remote_version(&self) -> Version {
        self.connection_info.remote.version
    }

    /// Returns true if the peer may have full block history: its handshake lacks the
    /// [`PeerServices::NODE_PRUNED`] hint.
    ///
    /// Any other service bits are ignored, so peers that advertise no services, such as older
    /// Zakura pruned nodes, are treated as archive candidates too. The hint is self-reported.
    pub(crate) fn claims_full_block_history(&self) -> bool {
        !self
            .connection_info
            .remote
            .services
            .contains(PeerServices::NODE_PRUNED)
    }

    /// Returns true if the connection would write a request sent now, rather than leave it queued
    /// behind a response the connection is still awaiting.
    ///
    /// [`Service::poll_ready`] only reports space in the request channel. While the connection is
    /// awaiting a response to an earlier request, it leaves new requests in that channel until the
    /// response arrives, times out, or is cancelled.
    ///
    /// Only meaningful after `poll_ready` has returned ready, which means this client has no request
    /// of its own still queued. Requests that the heartbeat or block gossip tasks have queued, but
    /// the connection has not taken yet, are not visible here.
    ///
    /// A connection that is between two iterations of its event loop when this client sends a
    /// request can briefly report free after it takes that request.
    pub(crate) fn can_send_request_now(&self) -> bool {
        self.request_slot.is_free()
    }

    /// Returns true if a block request timed out or was cancelled after at least half its timeout,
    /// and the peer has not completed a successful block response since then.
    pub(crate) fn has_unanswered_block_request(&self) -> bool {
        self.request_slot.block_request_unanswered()
    }

    /// Returns true if block routing should prefer this advertiser over its busy advertisers.
    ///
    /// A connection can write a request now, and the peer did not leave its last block request
    /// unanswered. Otherwise routing only offers this advertiser through ordinary load balancing,
    /// so a peer that withholds blocks can't win every retry while other advertisers are busy.
    pub(crate) fn is_preferred_block_source(&self) -> bool {
        self.can_send_request_now() && !self.has_unanswered_block_request()
    }

    /// Retrieve the peer's self-reported chain height from its handshake.
    pub(crate) fn remote_start_height(&self) -> zakura_chain::block::Height {
        self.connection_info.remote.start_height
    }

    /// Retrieve the connection's process-local number, shared by every trace row about it.
    pub(crate) fn connection_id(&self) -> u64 {
        self.connection_id
    }

    /// Retrieve the sanitized user agent the peer advertised in its handshake.
    pub(crate) fn remote_user_agent(&self) -> Arc<str> {
        self.user_agent.clone()
    }

    /// Returns true if this peer connected directly to us from `ip`.
    pub fn is_inbound_direct_from_ip(&self, ip: &IpAddr) -> bool {
        let expected_ip = canonical_socket_addr(SocketAddr::new(*ip, 0)).ip();

        matches!(
            self.connection_info.connected_addr,
            ConnectedAddr::InboundDirect { addr }
                if canonical_socket_addr(addr.remove_socket_addr_privacy()).ip() == expected_ip
        )
    }
}

impl<Request> Service<Request> for LoadTrackedClient
where
    Client: Service<Request>,
{
    type Response = <Client as Service<Request>>::Response;
    type Error = <Client as Service<Request>>::Error;
    type Future = <PeakEwma<Client> as Service<Request>>::Future;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        self.service.call(request)
    }
}

impl Load for LoadTrackedClient {
    type Metric = <PeakEwma<Client> as Load>::Metric;

    fn load(&self) -> Self::Metric {
        self.service.load()
    }
}
