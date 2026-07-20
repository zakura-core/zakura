//! A peer connection service wrapper type to handle load tracking and provide access to the
//! reported protocol version.

use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use tower::{
    load::{Load, PeakEwma},
    Service,
};

use crate::{
    constants::{EWMA_DECAY_TIME_NANOS, EWMA_DEFAULT_RTT},
    peer::{Client, ConnectedAddr, ConnectionInfo},
    protocol::external::{canonical_socket_addr, types::Version},
};

static NEXT_PEER_TRACE_ID: AtomicU64 = AtomicU64::new(1);

/// A client service wrapper that keeps track of its load.
///
/// It also keeps track of the peer's reported protocol version.
#[derive(Debug)]
pub struct LoadTrackedClient {
    /// A service representing a connected peer, wrapped in a load tracker.
    service: PeakEwma<Client>,

    /// The metadata for the connected peer `service`.
    connection_info: Arc<ConnectionInfo>,

    /// A process-local identifier for correlating privacy-preserving peer traces.
    trace_id: u64,
}

/// Create a new [`LoadTrackedClient`] wrapping the provided `client` service.
impl From<Client> for LoadTrackedClient {
    fn from(client: Client) -> Self {
        let connection_info = client.connection_info.clone();

        let service = PeakEwma::new(
            client,
            EWMA_DEFAULT_RTT,
            EWMA_DECAY_TIME_NANOS,
            tower::load::CompleteOnResponse::default(),
        );

        LoadTrackedClient {
            service,
            connection_info,
            trace_id: NEXT_PEER_TRACE_ID
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
                    Some(id.saturating_add(1))
                })
                .expect("trace ID update succeeds because its closure always returns Some"),
        }
    }
}

impl LoadTrackedClient {
    /// Retrieve the peer's reported protocol version.
    pub fn remote_version(&self) -> Version {
        self.connection_info.remote.version
    }

    /// Retrieve the peer's self-reported chain height from its handshake.
    pub(crate) fn remote_start_height(&self) -> zakura_chain::block::Height {
        self.connection_info.remote.start_height
    }

    /// Retrieve a process-local identifier used to correlate peer trace events.
    pub(crate) fn trace_id(&self) -> u64 {
        self.trace_id
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
