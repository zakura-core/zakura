//! Creating isolated connections to specific peers.

use std::future::Future;

use tokio::io::{AsyncRead, AsyncWrite};
use tower::{util::Oneshot, Service};

use zakura_chain::{chain_tip::NoChainTip, parameters::Network};

use crate::{
    peer::{self, Client, ConnectedAddr, HandshakeRequest},
    peer_set::ActiveConnectionCounter,
    BoxError, Config, P2pStack, Request, Response,
};

#[cfg(test)]
mod tests;

/// Creates an isolated Zcash peer connection using the provided data stream.
/// This function is for testing purposes only.
///
/// The connection is completely isolated from all other node state and aims to
/// be minimally distinguishable from other clients. The connection pool
/// returned by [`init`](crate::init) should be used for requests that do not
/// require isolated state or an existing transport.
///
/// This function does not implement timeout behavior, so callers may want to
/// layer it with a timeout.
///
/// # Inputs
///
/// - `network`: the Zcash [`Network`] used for this connection.
/// - `data_stream`: an existing transport.
/// - `user_agent`: a valid BIP14 user-agent, such as the empty string.
///
/// # Additional Inputs
///
/// - `inbound_service`: a [`tower::Service`] that answers inbound requests from
///   the connected peer.
///
/// # Privacy
///
/// This function can make the isolated connection send different responses to peers,
/// which makes it stand out from other isolated connections from other peers.
pub fn connect_isolated_with_inbound<PeerTransport, InboundService>(
    network: &Network,
    data_stream: PeerTransport,
    user_agent: String,
    inbound_service: InboundService,
) -> impl Future<Output = Result<Client, BoxError>>
where
    PeerTransport: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    InboundService:
        Service<Request, Response = Response, Error = BoxError> + Clone + Send + 'static,
    InboundService::Future: Send,
{
    let config = Config {
        network: network.clone(),
        // Isolated connections are legacy-only: they must not advertise the Zakura P2P v2
        // service bit, or attempt an upgrade that would deanonymise the caller.
        p2p_stack: P2pStack::Legacy,
        ..Config::default()
    };

    let handshake = peer::Handshake::builder()
        .with_config(config)
        .with_inbound_service(inbound_service)
        .with_user_agent(user_agent)
        .with_latest_chain_tip(NoChainTip)
        .finish()
        .expect("provided mandatory builder parameters");

    // Don't send or track any metadata about the connection
    let connected_addr = ConnectedAddr::new_isolated();
    let connection_tracker = ActiveConnectionCounter::new_counter().track_connection();

    Oneshot::new(
        handshake,
        HandshakeRequest {
            data_stream,
            connected_addr,
            connection_tracker,
        },
    )
}
