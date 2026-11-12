//! Keeps connections open to the configured unconditional peers.

use std::{net::SocketAddr, time::Duration};

use futures::{future::BoxFuture, sink::SinkExt, stream::FuturesUnordered, StreamExt};
use rand_10::RngExt;
use tokio::time::{sleep, Instant, MissedTickBehavior};
use tower::{Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::diagnostic::task::WaitForPanics;
use zakura_node_services::task::erased;

use crate::{
    constants,
    peer::{self, address_is_valid_for_outbound_connections, OutboundConnectorRequest},
    peer_registry::PeerRegistry,
    peer_set::ActiveConnectionCounter,
    protocol::external::{canonical_ip, canonical_socket_addr},
    BoxError, Config, PeerSocketAddr,
};

use super::DiscoveredPeer;

/// The redial state of one dialed unconditional peer.
#[derive(Debug)]
struct RedialState {
    /// The address Zakura dials.
    addr: PeerSocketAddr,

    /// The earliest time of the next dial.
    next_dial: Instant,

    /// The number of dials since this peer last had a healthy connection.
    attempts: u32,

    /// Whether a dial to this peer is in flight.
    is_dialing: bool,

    /// When this peer was first seen connected, if it still is.
    connected_since: Option<Instant>,
}

/// Returns the addresses that Zakura dials to stay connected to its unconditional peers.
///
/// Leaves out this node's own listener, and addresses that are invalid for outbound
/// connections on the configured network.
pub(super) fn unconditional_peer_dial_addrs(
    config: &Config,
    listen_addr: SocketAddr,
) -> Vec<PeerSocketAddr> {
    let mut dial_addrs = config
        .unconditional_peers
        .dial_addrs(config.network.default_port());

    dial_addrs.retain(|&addr| {
        let addr_label = addr.addr_label(config.expose_peer_addresses);

        if is_own_listener(addr, listen_addr, config.external_addr) {
            debug!(
                peer = %addr_label,
                "not dialing an unconditional peer address that is this node's own listener"
            );
            return false;
        }

        if let Err(error) = address_is_valid_for_outbound_connections(addr, config.network.clone())
        {
            warn!(
                peer = %addr_label,
                ?error,
                "not dialing an unconditional peer address that is invalid for outbound connections"
            );
            return false;
        }

        true
    });

    dial_addrs
}

/// Returns whether `addr` is a listener address of this node that can be recognized
/// without connecting.
///
/// The handshake rejects every other self-connection by its nonce.
fn is_own_listener(
    addr: PeerSocketAddr,
    listen_addr: SocketAddr,
    external_addr: Option<SocketAddr>,
) -> bool {
    let listen_addr = canonical_socket_addr(listen_addr);

    if external_addr.map(canonical_socket_addr) == Some(*addr) {
        return true;
    }

    addr.port() == listen_addr.port()
        && (addr.ip() == listen_addr.ip()
            || (listen_addr.ip().is_unspecified() && addr.ip().is_loopback()))
}

/// Returns whether a dial to `addr` would add nothing to `connections`.
///
/// That is the case when Zakura holds an outbound connection to `addr`, an inbound
/// connection from its IP, or `max_connections_per_ip` connections to its IP. The peer
/// set drops any further connection to that IP.
fn is_connected(
    addr: PeerSocketAddr,
    connections: &[(PeerSocketAddr, bool)],
    max_connections_per_ip: usize,
) -> bool {
    let ip = canonical_ip(addr.ip());
    let mut connections_to_ip: usize = 0;

    for (connected_addr, is_inbound) in connections {
        if canonical_ip(connected_addr.ip()) != ip {
            continue;
        }
        if *is_inbound || *connected_addr == addr {
            return true;
        }
        connections_to_ip = connections_to_ip.saturating_add(1);
    }

    connections_to_ip >= max_connections_per_ip
}

/// Returns the delay before the dial that follows `attempts` dials without a healthy
/// connection.
fn redial_delay(attempts: u32) -> Duration {
    constants::MIN_UNCONDITIONAL_PEER_REDIAL_DELAY
        .checked_mul(2u32.saturating_pow(attempts))
        .unwrap_or(constants::MAX_UNCONDITIONAL_PEER_REDIAL_DELAY)
        .min(constants::MAX_UNCONDITIONAL_PEER_REDIAL_DELAY)
}

/// Returns `delay` plus a random extra of up to a quarter of it.
///
/// Two nodes that list each other dial at the same moment after their connection drops.
/// With one connection per IP, each node can keep the dial that the other one drops.
/// The random extra lets one dial land first on a later round.
fn with_jitter(delay: Duration) -> Duration {
    let max_jitter_millis = u64::try_from(delay.as_millis() / 4).unwrap_or(u64::MAX);

    delay.saturating_add(Duration::from_millis(
        rand_10::rng().random_range(0..=max_jitter_millis),
    ))
}

/// Dials each address in `dial_addrs`, and dials it again whenever no connection to that
/// peer is open, see [`is_connected`]. Sends each new connection to `peerset_tx`.
///
/// A dial that does not lead to a lasting connection doubles the peer's redial delay,
/// from [`constants::MIN_UNCONDITIONAL_PEER_REDIAL_DELAY`] up to
/// [`constants::MAX_UNCONDITIONAL_PEER_REDIAL_DELAY`]. A connection that lasts
/// [`constants::UNCONDITIONAL_PEER_HEALTHY_CONNECTION`] resets it, so the peer is dialed
/// as soon as that connection drops.
///
/// These dials use their own connection counter, so the outbound connection limit never
/// blocks them. [`Config::unconditional_peer_connection_limit`] adds their slots to the
/// total connection limit.
///
/// Returns an error when `peerset_tx` is closed.
#[instrument(skip_all)]
pub(super) async fn keep_unconditional_peers_connected<C>(
    config: Config,
    dial_addrs: Vec<PeerSocketAddr>,
    peer_registry: PeerRegistry,
    outbound_connector: C,
    mut peerset_tx: futures::channel::mpsc::Sender<DiscoveredPeer>,
) -> Result<(), BoxError>
where
    C: Service<
            OutboundConnectorRequest,
            Response = (PeerSocketAddr, peer::Client),
            Error = BoxError,
        > + Clone
        + Send
        + 'static,
    C::Future: Send + 'static,
{
    let expose_peer_addresses = config.expose_peer_addresses;

    if expose_peer_addresses {
        info!(
            ?dial_addrs,
            "keeping connections open to unconditional peers"
        );
    } else {
        info!(
            dial_addrs = dial_addrs.len(),
            "keeping connections open to unconditional peers"
        );
    }

    let mut active_connections = ActiveConnectionCounter::new_counter_with(
        dial_addrs.len(),
        "Unconditional Peer Connections",
    );

    let start = Instant::now();
    let mut peers: Vec<RedialState> = dial_addrs
        .into_iter()
        .map(|addr| RedialState {
            addr,
            next_dial: start,
            attempts: 0,
            is_dialing: false,
            connected_since: None,
        })
        .collect();

    // Each dial runs in its own task, so it makes progress while this task sleeps.
    let mut dials: FuturesUnordered<BoxFuture<'static, (usize, Result<DiscoveredPeer, BoxError>)>> =
        FuturesUnordered::new();

    let mut check_timer = tokio::time::interval(constants::UNCONDITIONAL_PEER_CHECK_INTERVAL);
    // A late check must not be followed by a burst of catch-up checks.
    check_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;

            // An empty pool yields `None`, which disables this branch until the next dial.
            Some((index, dial_result)) = dials.next() => {
                let peer: &mut RedialState = peers
                    .get_mut(index)
                    .expect("dial indexes come from the peer list, which never shrinks");
                peer.is_dialing = false;

                match dial_result {
                    Ok((addr, client)) => {
                        info!(
                            peer = %addr.addr_label(expose_peer_addresses),
                            "connected to an unconditional peer"
                        );

                        // The peer set channel has room for every connection slot,
                        // so this send doesn't block.
                        peerset_tx.send((addr, client)).await?;
                    }
                    Err(error) => {
                        metrics::counter!("peer.unconditional.dial.failed.total").increment(1);

                        // Only the first failure since the last healthy connection is
                        // worth an operator's attention: a peer without a listener
                        // fails every dial.
                        let addr_label = peer.addr.addr_label(expose_peer_addresses);
                        if peer.attempts <= 1 {
                            info!(
                                peer = %addr_label,
                                ?error,
                                "failed to connect to an unconditional peer, retrying with backoff"
                            );
                        } else {
                            debug!(
                                peer = %addr_label,
                                ?error,
                                attempts = peer.attempts,
                                "failed to connect to an unconditional peer"
                            );
                        }
                    }
                }
            }

            _ = check_timer.tick() => {
                // Drain the close notifications, so the counter's channel stays small.
                active_connections.update_count();

                let connections = peer_registry.connection_addrs();

                for (index, peer) in peers.iter_mut().enumerate() {
                    if peer.is_dialing {
                        continue;
                    }

                    let now = Instant::now();

                    if is_connected(peer.addr, &connections, config.max_connections_per_ip) {
                        let connected_since = *peer.connected_since.get_or_insert(now);
                        if now.saturating_duration_since(connected_since)
                            >= constants::UNCONDITIONAL_PEER_HEALTHY_CONNECTION
                        {
                            peer.attempts = 0;
                        }
                        continue;
                    }
                    peer.connected_since = None;

                    if now < peer.next_dial {
                        continue;
                    }

                    peer.is_dialing = true;
                    peer.next_dial = now + with_jitter(redial_delay(peer.attempts));
                    peer.attempts = peer.attempts.saturating_add(1);
                    metrics::counter!("peer.unconditional.dial.total").increment(1);

                    let request = OutboundConnectorRequest {
                        addr: peer.addr,
                        connection_tracker: active_connections.track_connection(),
                    };
                    let outbound_connector = outbound_connector.clone();

                    dials.push(
                        tokio::spawn(erased(
                            async move { (index, outbound_connector.oneshot(request).await) }
                                .in_current_span(),
                        ))
                        .wait_for_panics(),
                    );

                    // Security: start outbound connections at least this far apart,
                    // like the crawler does.
                    sleep(constants::MIN_OUTBOUND_PEER_CONNECTION_INTERVAL).await;
                }
            }
        }
    }
}
