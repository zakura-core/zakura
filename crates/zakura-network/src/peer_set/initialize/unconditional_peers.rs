//! Keeps connections open to the configured unconditional peers.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use futures::{future::BoxFuture, sink::SinkExt, stream::FuturesUnordered, FutureExt, StreamExt};
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
    protocol::external::{canonical_ip, canonical_peer_addr, canonical_socket_addr},
    unconditional_peer_set::ResolvedUnconditionalPeers,
    BannedIps, BoxError, Config, PeerSocketAddr,
};

use super::DiscoveredPeer;

/// A peer that Zakura dials to stay connected to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum DialTarget {
    /// A fixed address.
    Addr(PeerSocketAddr),

    /// A DNS name and port. The name is resolved again on every dial.
    Host(Arc<str>, u16),
}

impl DialTarget {
    /// Returns a label for this peer using the configured privacy policy.
    fn label(&self, expose_peer_addresses: bool) -> String {
        match self {
            DialTarget::Addr(addr) => addr.addr_label(expose_peer_addresses),
            DialTarget::Host(name, port) if expose_peer_addresses => format!("{name}:{port}"),
            DialTarget::Host(_name, port) => format!("redacted name:{port}"),
        }
    }
}

/// The redial state of one dialed unconditional peer.
#[derive(Debug)]
struct RedialState {
    /// The peer Zakura dials.
    target: DialTarget,

    /// The addresses this peer can be connected at: its configured address, or the
    /// addresses that its name resolved to at the last lookup that found any.
    addrs: Vec<PeerSocketAddr>,

    /// The number of dials to this peer, which picks the next of `addrs` to dial.
    dials: usize,

    /// The earliest time of the next attempt.
    next_attempt: Instant,

    /// The number of attempts since this peer last had a healthy connection.
    attempts: u32,

    /// Whether a lookup or a dial for this peer is in flight.
    is_attempting: bool,

    /// When this peer was first seen connected, if it still is.
    connected_since: Option<Instant>,
}

/// A finished step of an attempt to connect to the peer at `index`.
enum AttemptEvent {
    /// The lookup of the peer's name finished.
    Resolved {
        index: usize,
        result: Result<Vec<SocketAddr>, BoxError>,
    },

    /// The dial finished.
    Dialed {
        index: usize,
        result: Result<DiscoveredPeer, BoxError>,
    },
}

/// Returns the peers that Zakura dials to stay connected to its unconditional peers.
///
/// Leaves out the addresses that [`is_dialable`] rejects. A name's addresses are only
/// known, and checked, when it is resolved.
pub(super) fn unconditional_peer_dial_targets(
    config: &Config,
    listen_addr: SocketAddr,
) -> Vec<DialTarget> {
    let addrs = config
        .unconditional_peers
        .dial_addrs(config.network.default_port())
        .into_iter()
        .filter(|&addr| is_dialable(addr, config, listen_addr))
        .map(DialTarget::Addr);
    let hosts = config
        .unconditional_peers
        .dial_hosts()
        .into_iter()
        .map(|(name, port)| DialTarget::Host(name, port));

    addrs.chain(hosts).collect()
}

/// Returns whether Zakura can dial `addr`: it is not this node's own listener, and it is
/// valid for outbound connections on the configured network.
fn is_dialable(addr: PeerSocketAddr, config: &Config, listen_addr: SocketAddr) -> bool {
    let addr_label = addr.addr_label(config.expose_peer_addresses);

    if is_own_listener(addr, listen_addr, config.external_addr) {
        debug!(
            peer = %addr_label,
            "not dialing an unconditional peer address that is this node's own listener"
        );
        return false;
    }

    if let Err(error) = address_is_valid_for_outbound_connections(addr, config.network.clone()) {
        warn!(
            peer = %addr_label,
            ?error,
            "not dialing an unconditional peer address that is invalid for outbound connections"
        );
        return false;
    }

    true
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

/// Resolves a DNS name and port to its socket addresses, within
/// [`constants::DNS_LOOKUP_TIMEOUT`].
pub(super) async fn resolve_host(
    (name, port): (Arc<str>, u16),
) -> Result<Vec<SocketAddr>, BoxError> {
    let addrs = tokio::time::timeout(
        constants::DNS_LOOKUP_TIMEOUT,
        tokio::net::lookup_host((&*name, port)),
    )
    .await??;

    Ok(addrs.collect())
}

/// Returns the first [`constants::MAX_UNCONDITIONAL_PEER_RESOLVED_ADDRS`] distinct
/// addresses in `resolved` that Zakura can dial, see [`is_dialable`].
fn dialable_addrs(
    resolved: Vec<SocketAddr>,
    config: &Config,
    listen_addr: SocketAddr,
) -> Vec<PeerSocketAddr> {
    let mut addrs = Vec::new();

    for addr in resolved.into_iter().map(canonical_peer_addr) {
        if addrs.len() >= constants::MAX_UNCONDITIONAL_PEER_RESOLVED_ADDRS {
            break;
        }
        if !addrs.contains(&addr) && is_dialable(addr, config, listen_addr) {
            addrs.push(addr);
        }
    }

    addrs
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

/// Returns the delay before the attempt that follows `attempts` attempts without a
/// healthy connection.
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

/// Logs a failed attempt to connect to `peer`.
///
/// Only the first failure since the last healthy connection is worth an operator's
/// attention: a peer without a listener fails every dial.
fn log_failed_attempt(peer: &RedialState, error: &BoxError, expose_peer_addresses: bool) {
    metrics::counter!("peer.unconditional.dial.failed.total").increment(1);

    let peer_label = peer.target.label(expose_peer_addresses);
    if peer.attempts <= 1 {
        info!(
            peer = %peer_label,
            ?error,
            "failed to connect to an unconditional peer, retrying with backoff"
        );
    } else {
        debug!(
            peer = %peer_label,
            ?error,
            attempts = peer.attempts,
            "failed to connect to an unconditional peer"
        );
    }
}

/// Connects to each peer in `dial_targets`, and connects to it again whenever no
/// connection to it is open, see [`is_connected`]. Sends each new connection to
/// `peerset_tx`.
///
/// A peer with a DNS name is looked up with `resolver` at the start of every attempt, so
/// Zakura follows the peer when its name moves to a new address. The addresses it
/// resolves to are published to `resolved_peers`, which exempts them from scoring and
/// bans, and any ban on them is lifted from `bans`. They replace the addresses of the
/// last lookup. When a lookup fails, Zakura dials the addresses of the last one that
/// worked. Successive dials go through a name's addresses in turn.
///
/// An attempt that does not lead to a lasting connection doubles the peer's redial
/// delay, from [`constants::MIN_UNCONDITIONAL_PEER_REDIAL_DELAY`] up to
/// [`constants::MAX_UNCONDITIONAL_PEER_REDIAL_DELAY`]. A connection that lasts
/// [`constants::UNCONDITIONAL_PEER_HEALTHY_CONNECTION`] resets it, so the peer is dialed
/// as soon as that connection drops.
///
/// These dials use their own connection counter, so the outbound connection limit never
/// blocks them. [`Config::unconditional_peer_connection_limit`] adds their slots to the
/// total connection limit.
///
/// Returns an error when `peerset_tx` is closed.
#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub(super) async fn keep_unconditional_peers_connected<R, C>(
    config: Config,
    listen_addr: SocketAddr,
    dial_targets: Vec<DialTarget>,
    peer_registry: PeerRegistry,
    resolved_peers: ResolvedUnconditionalPeers,
    bans: BannedIps,
    resolver: R,
    outbound_connector: C,
    mut peerset_tx: futures::channel::mpsc::Sender<DiscoveredPeer>,
) -> Result<(), BoxError>
where
    R: Service<(Arc<str>, u16), Response = Vec<SocketAddr>, Error = BoxError>
        + Clone
        + Send
        + 'static,
    R::Future: Send + 'static,
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
        let dial_targets: Vec<_> = dial_targets
            .iter()
            .map(|target| target.label(true))
            .collect();
        info!(
            ?dial_targets,
            "keeping connections open to unconditional peers"
        );
    } else {
        info!(
            dial_targets = dial_targets.len(),
            "keeping connections open to unconditional peers"
        );
    }

    let mut active_connections = ActiveConnectionCounter::new_counter_with(
        dial_targets.len(),
        "Unconditional Peer Connections",
    );

    let start = Instant::now();
    let mut peers: Vec<RedialState> = dial_targets
        .into_iter()
        .map(|target| RedialState {
            addrs: match &target {
                DialTarget::Addr(addr) => vec![*addr],
                DialTarget::Host(..) => Vec::new(),
            },
            target,
            dials: 0,
            next_attempt: start,
            attempts: 0,
            is_attempting: false,
            connected_since: None,
        })
        .collect();

    // Each lookup and dial runs in its own task, so it makes progress while this task
    // sleeps.
    let mut events: FuturesUnordered<BoxFuture<'static, AttemptEvent>> = FuturesUnordered::new();

    // Starts a dial to `addr` for the peer at `index`.
    let mut start_dial = |index: usize, addr: PeerSocketAddr| {
        let request = OutboundConnectorRequest {
            addr,
            connection_tracker: active_connections.track_connection(),
        };
        let outbound_connector = outbound_connector.clone();

        // Drain the close notifications, so the counter's channel stays small.
        active_connections.update_count();

        tokio::spawn(erased(
            async move {
                AttemptEvent::Dialed {
                    index,
                    result: outbound_connector.oneshot(request).await,
                }
            }
            .in_current_span(),
        ))
        .wait_for_panics()
    };

    let mut check_timer = tokio::time::interval(constants::UNCONDITIONAL_PEER_CHECK_INTERVAL);
    // A late check must not be followed by a burst of catch-up checks.
    check_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;

            // An empty pool yields `None`, which disables this branch until the next
            // attempt.
            Some(event) = events.next() => match event {
                AttemptEvent::Resolved { index, result } => {
                    let peer: &mut RedialState = peers
                        .get_mut(index)
                        .expect("event indexes come from the peer list, which never shrinks");

                    let lookup = result.and_then(|resolved| {
                        let addrs = dialable_addrs(resolved, &config, listen_addr);
                        if addrs.is_empty() {
                            Err("the name resolved to no address that Zakura can dial".into())
                        } else {
                            Ok(addrs)
                        }
                    });

                    let lookup_error = match lookup {
                        Ok(addrs) if addrs != peer.addrs => {
                            debug!(
                                peer = %peer.target.label(expose_peer_addresses),
                                addrs = addrs.len(),
                                "an unconditional peer's name resolved to new addresses"
                            );
                            peer.addrs = addrs;

                            // Exempt the new addresses before any connection to them
                            // can be scored.
                            for addr in &peer.addrs {
                                bans.remove(addr.ip());
                            }
                            resolved_peers.publish(
                                peers
                                    .iter()
                                    .filter(|peer| matches!(peer.target, DialTarget::Host(..)))
                                    .flat_map(|peer| peer.addrs.iter().map(|addr| addr.ip())),
                            );
                            None
                        }
                        Ok(_unchanged_addrs) => None,
                        Err(error) => Some(error),
                    };

                    let peer: &mut RedialState = peers
                        .get_mut(index)
                        .expect("event indexes come from the peer list, which never shrinks");

                    // A failed lookup falls back to the addresses of the last one that
                    // worked.
                    if peer.addrs.is_empty() {
                        let error = lookup_error
                            .expect("a successful lookup leaves the peer with an address");
                        peer.is_attempting = false;
                        log_failed_attempt(peer, &error, expose_peer_addresses);
                        continue;
                    }

                    let connections = peer_registry.connection_addrs();
                    if peer
                        .addrs
                        .iter()
                        .any(|addr| is_connected(*addr, &connections, config.max_connections_per_ip))
                    {
                        peer.is_attempting = false;
                        continue;
                    }

                    let addr = *peer
                        .addrs
                        .get(peer.dials % peer.addrs.len())
                        .expect("the remainder is an index into a list that is not empty");
                    peer.dials = peer.dials.wrapping_add(1);
                    events.push(start_dial(index, addr));

                    // Security: start outbound connections at least this far apart,
                    // like the crawler does.
                    sleep(constants::MIN_OUTBOUND_PEER_CONNECTION_INTERVAL).await;
                }

                AttemptEvent::Dialed { index, result } => {
                    let peer: &mut RedialState = peers
                        .get_mut(index)
                        .expect("event indexes come from the peer list, which never shrinks");
                    peer.is_attempting = false;

                    match result {
                        Ok((addr, client)) => {
                            info!(
                                peer = %peer.target.label(expose_peer_addresses),
                                addr = %addr.addr_label(expose_peer_addresses),
                                "connected to an unconditional peer"
                            );

                            // The peer set channel has room for every connection slot,
                            // so this send doesn't block.
                            peerset_tx.send((addr, client)).await?;
                        }
                        Err(error) => log_failed_attempt(peer, &error, expose_peer_addresses),
                    }
                }
            },

            _ = check_timer.tick() => {
                let connections = peer_registry.connection_addrs();

                for (index, peer) in peers.iter_mut().enumerate() {
                    if peer.is_attempting {
                        continue;
                    }

                    let now = Instant::now();

                    if peer
                        .addrs
                        .iter()
                        .any(|addr| is_connected(*addr, &connections, config.max_connections_per_ip))
                    {
                        let connected_since = *peer.connected_since.get_or_insert(now);
                        if now.saturating_duration_since(connected_since)
                            >= constants::UNCONDITIONAL_PEER_HEALTHY_CONNECTION
                        {
                            peer.attempts = 0;
                        }
                        continue;
                    }
                    peer.connected_since = None;

                    if now < peer.next_attempt {
                        continue;
                    }

                    peer.is_attempting = true;
                    peer.next_attempt = now + with_jitter(redial_delay(peer.attempts));
                    peer.attempts = peer.attempts.saturating_add(1);
                    metrics::counter!("peer.unconditional.dial.total").increment(1);

                    match &peer.target {
                        DialTarget::Addr(addr) => {
                            events.push(start_dial(index, *addr));

                            // Security: start outbound connections at least this far
                            // apart, like the crawler does.
                            sleep(constants::MIN_OUTBOUND_PEER_CONNECTION_INTERVAL).await;
                        }
                        DialTarget::Host(name, port) => {
                            let lookup = resolver.clone().oneshot((name.clone(), *port));

                            events.push(
                                tokio::spawn(erased(
                                    async move {
                                        AttemptEvent::Resolved {
                                            index,
                                            result: lookup.await,
                                        }
                                    }
                                    .in_current_span(),
                                ))
                                .wait_for_panics()
                                .boxed(),
                            );
                        }
                    }
                }
            }
        }
    }
}
