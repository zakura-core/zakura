//! Keeps connections open to the configured unconditional peers.

use std::{collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration};

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
    BoxError, Config, PeerSocketAddr,
};

use super::DiscoveredPeer;

/// The addresses that a lookup of an unconditional peer's DNS name returned.
type Lookup = io::Result<Vec<SocketAddr>>;

/// The redial state of one dialed unconditional peer.
#[derive(Debug)]
struct RedialState {
    /// The peer's DNS name and port, if it is listed by name, with the name's index in
    /// [`UnconditionalPeers::dial_names`]. Zakura looks the name up before each dial.
    ///
    /// [`UnconditionalPeers::dial_names`]: crate::UnconditionalPeers::dial_names
    name: Option<(usize, Arc<str>, u16)>,

    /// The address Zakura dials. For a peer listed by name, this is the first address
    /// Zakura could dial in the name's last lookup that had one, and `None` until then.
    addr: Option<PeerSocketAddr>,

    /// The earliest time of the next attempt.
    next_attempt: Instant,

    /// The number of attempts since this peer last had a healthy connection.
    attempts: u32,

    /// Whether an attempt to connect to this peer is in flight.
    is_attempting: bool,

    /// Whether this peer has been attempted before. Every attempt but the first waits
    /// a random delay, see [`redial_jitter`].
    has_attempted: bool,

    /// When this peer was first seen connected, if it still is.
    connected_since: Option<Instant>,

    /// The address of this peer's dial in flight, if any. It counts against the per-IP
    /// limit before it finishes, see [`connections_with_dials_in_flight`].
    dialing: Option<PeerSocketAddr>,
}

/// A finished step of an attempt to connect to the peer at `index`.
enum AttemptEvent {
    /// The random delay before the dial is over, and so is the lookup of the peer's DNS
    /// name, if it is listed by name.
    Delayed {
        index: usize,
        lookup: Option<Lookup>,
    },

    /// The dial finished.
    Dialed {
        index: usize,
        result: Result<DiscoveredPeer, BoxError>,
    },
}

/// Returns the addresses that Zakura dials to stay connected to its unconditional peers.
///
/// Leaves out the addresses that [`is_dialable`] rejects.
pub(super) fn unconditional_peer_dial_addrs(
    config: &Config,
    listen_addr: SocketAddr,
) -> Vec<PeerSocketAddr> {
    config
        .unconditional_peers
        .dial_addrs(config.network.default_port())
        .into_iter()
        .filter(|&addr| is_dialable(addr, config, listen_addr))
        .collect()
}

/// Leaves the unconditional peers that [`is_dialable`] rejects, such as this node's own
/// listener, out of dialing, see [`UnconditionalPeers::leave_out_undialable`].
///
/// Zakura does this once its listener is bound, before anything uses the list, so the
/// startup dials, the crawler and the inbound listener keep no room for these peers.
///
/// [`UnconditionalPeers::leave_out_undialable`]: crate::UnconditionalPeers::leave_out_undialable
pub(super) fn leave_out_undialable_unconditional_peers(
    config: &mut Config,
    listen_addr: SocketAddr,
) {
    config.unconditional_peers = config
        .unconditional_peers
        .leave_out_undialable(config.network.default_port(), |addr| {
            is_dialable(addr, config, listen_addr)
        });
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

/// Returns the connections in `peer_registry`, and an outbound connection for each dial
/// in flight in `peers`, so a dial counts against `max_connections_per_ip` before it
/// finishes and listed addresses on one IP cannot start more dials than the limit.
fn connections_with_dials_in_flight(
    peer_registry: &PeerRegistry,
    peers: &[RedialState],
) -> Vec<(PeerSocketAddr, bool)> {
    let mut connections = peer_registry.connection_addrs();
    connections.extend(
        peers
            .iter()
            .filter_map(|peer| peer.dialing)
            .map(|addr| (addr, false)),
    );
    connections
}

/// Looks up the addresses of the DNS name `name`, with `port`, and gives up after
/// [`constants::DNS_LOOKUP_TIMEOUT`].
pub(super) fn lookup_host(name: Arc<str>, port: u16) -> BoxFuture<'static, Lookup> {
    async move {
        let addrs = tokio::time::timeout(
            constants::DNS_LOOKUP_TIMEOUT,
            tokio::net::lookup_host((&*name, port)),
        )
        .await??;

        Ok(addrs.collect())
    }
    .boxed()
}

/// Returns whether `addr` is a listener address of this node that can be recognized
/// without connecting.
///
/// A wildcard listener only counts loopback addresses of its own address family: an
/// IPv4 wildcard cannot accept IPv6, and whether an IPv6 wildcard accepts IPv4 depends
/// on the platform. The handshake rejects every other self-connection by its nonce.
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
            || (listen_addr.ip().is_unspecified()
                && addr.ip().is_loopback()
                && addr.is_ipv4() == listen_addr.is_ipv4()))
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

/// Returns a random delay of up to [`constants::MAX_UNCONDITIONAL_PEER_REDIAL_JITTER`].
///
/// Two nodes that list each other both redial when their connection drops. When the
/// two dials start within about half a round trip of each other, each node keeps its
/// own dial and drops the other's, and both connections close. Independent random
/// delays make that rare, and separate the two nodes on the next round when it happens.
fn redial_jitter() -> Duration {
    let max_jitter_millis =
        u64::try_from(constants::MAX_UNCONDITIONAL_PEER_REDIAL_JITTER.as_millis())
            .unwrap_or(u64::MAX);

    Duration::from_millis(rand_10::rng().random_range(0..=max_jitter_millis))
}

/// Warns about each IP that more of `dial_addrs` name than `max_connections_per_ip`
/// allows connections to.
///
/// [`is_connected`] counts such an address as connected once the IP is at its limit,
/// so Zakura never dials it. Without this line, nothing would tell the operator.
fn warn_about_addrs_over_the_ip_limit(dial_addrs: &[PeerSocketAddr], config: &Config) {
    let mut addrs_by_ip: HashMap<_, usize> = HashMap::new();
    for addr in dial_addrs {
        *addrs_by_ip.entry(canonical_ip(addr.ip())).or_default() += 1;
    }

    for addr in dial_addrs {
        if let Some(addrs) = addrs_by_ip.remove(&canonical_ip(addr.ip())) {
            if addrs > config.max_connections_per_ip {
                warn!(
                    peer = %addr.addr_label(config.expose_peer_addresses),
                    addrs,
                    max_connections_per_ip = config.max_connections_per_ip,
                    "unconditional peer addresses share an IP beyond max_connections_per_ip, \
                     so only that many of them are kept connected"
                );
            }
        }
    }
}

/// Dials each address in `dial_addrs` and each DNS name in the configured unconditional
/// peers, and dials it again whenever no connection to that peer is open, see
/// [`is_connected`]. Sends each new connection to `peerset_tx`.
///
/// A peer listed by name is looked up with `lookup` before each dial, so a peer that
/// comes back at a new address is found there. Zakura dials the first address in the
/// answer that [`is_dialable`], and records it with
/// [`UnconditionalPeers::set_resolved_ip`], so it is treated like a listed address. When
/// a lookup fails or has no such address, Zakura dials the last address it had. A name
/// is never looked up while its peer is connected.
///
/// An attempt that does not lead to a lasting connection doubles the peer's redial
/// delay, from [`constants::MIN_UNCONDITIONAL_PEER_REDIAL_DELAY`] up to
/// [`constants::MAX_UNCONDITIONAL_PEER_REDIAL_DELAY`]. A connection that lasts
/// [`constants::UNCONDITIONAL_PEER_HEALTHY_CONNECTION`] resets it, so the peer is
/// attempted at the next check after that connection drops.
///
/// Every attempt but a peer's first waits [`redial_jitter`], then dials only if the
/// peer is still not connected.
///
/// These dials use their own connection counter, so the outbound connection limit never
/// blocks them. [`Config::unconditional_peer_connection_limit`] adds their slots to the
/// total connection limit.
///
/// Returns an error when `peerset_tx` is closed.
///
/// [`UnconditionalPeers::set_resolved_ip`]: crate::UnconditionalPeers::set_resolved_ip
#[instrument(skip_all)]
pub(super) async fn keep_unconditional_peers_connected<C, L>(
    config: Config,
    dial_addrs: Vec<PeerSocketAddr>,
    listen_addr: SocketAddr,
    lookup: L,
    peer_registry: PeerRegistry,
    outbound_connector: C,
    mut peerset_tx: futures::channel::mpsc::Sender<DiscoveredPeer>,
) -> Result<(), BoxError>
where
    L: Fn(Arc<str>, u16) -> BoxFuture<'static, Lookup> + Send + 'static,
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
    let dial_names: Vec<_> = config.unconditional_peers.dial_names().collect();

    if expose_peer_addresses {
        // `PeerSocketAddr`'s `Debug` redacts the address, so log the labels.
        let dial_addrs: Vec<_> = dial_addrs
            .iter()
            .map(|addr| addr.addr_label(true))
            .collect();
        info!(
            ?dial_addrs,
            ?dial_names,
            "keeping connections open to unconditional peers"
        );
    } else {
        info!(
            dial_addrs = dial_addrs.len(),
            dial_names = dial_names.len(),
            "keeping connections open to unconditional peers"
        );
    }
    warn_about_addrs_over_the_ip_limit(&dial_addrs, &config);

    let start = Instant::now();
    let mut peers: Vec<RedialState> = dial_addrs
        .into_iter()
        .map(|addr| (None, Some(addr)))
        .chain(
            dial_names
                .into_iter()
                .enumerate()
                .map(|(name_index, (name, port))| (Some((name_index, name, port)), None)),
        )
        .map(|(name, addr)| RedialState {
            name,
            addr,
            next_attempt: start,
            attempts: 0,
            is_attempting: false,
            has_attempted: false,
            connected_since: None,
            dialing: None,
        })
        .collect();

    let mut active_connections =
        ActiveConnectionCounter::new_counter_with(peers.len(), "Unconditional Peer Connections");

    // Each dial runs in its own task, so it makes progress while this task sleeps.
    let mut events: FuturesUnordered<BoxFuture<'static, AttemptEvent>> = FuturesUnordered::new();

    let mut check_timer = tokio::time::interval(constants::UNCONDITIONAL_PEER_CHECK_INTERVAL);
    // A late check must not be followed by a burst of catch-up checks.
    check_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;

            // An empty pool yields `None`, which disables this branch until the next
            // attempt.
            Some(event) = events.next() => match event {
                AttemptEvent::Delayed { index, lookup } => {
                    let connections = connections_with_dials_in_flight(&peer_registry, &peers);
                    let peer: &mut RedialState = peers
                        .get_mut(index)
                        .expect("event indexes come from the peer list, which never shrinks");

                    if let (Some((name_index, name, _port)), Some(lookup)) = (&peer.name, lookup) {
                        let dial_addr = lookup.and_then(|addrs| {
                            addrs
                                .into_iter()
                                .map(canonical_peer_addr)
                                .find(|&addr| is_dialable(addr, &config, listen_addr))
                                .ok_or_else(|| io::Error::other("no address that Zakura can dial"))
                        });

                        match dial_addr {
                            Ok(addr) if peer.addr != Some(addr) => {
                                info!(
                                    peer = %addr.addr_label(expose_peer_addresses),
                                    "an unconditional peer's DNS name resolved to a new address"
                                );
                                peer.addr = Some(addr);
                                // Zakura dials the address below, so exempt it before a
                                // connection to it can be scored.
                                config
                                    .unconditional_peers
                                    .set_resolved_ip(*name_index, addr.ip());
                            }
                            Ok(_) => {}
                            // Keep the last address, if any: the peer may still be there.
                            // Like a failed dial, only the first failure is worth an
                            // operator's attention.
                            Err(error) => {
                                let name = expose_peer_addresses.then_some(name);
                                if peer.attempts <= 1 {
                                    info!(
                                        ?name,
                                        ?error,
                                        "failed to look up an unconditional peer's DNS name, \
                                         retrying with backoff"
                                    );
                                } else {
                                    debug!(
                                        ?name,
                                        ?error,
                                        "failed to look up an unconditional peer's DNS name"
                                    );
                                }
                            }
                        }
                    }

                    // The peer may have dialed in while this attempt waited, or a dial to
                    // another listed address may have filled its IP. A name that has
                    // never resolved has no address to dial.
                    let addr = peer.addr.filter(|&addr| {
                        !is_connected(addr, &connections, config.max_connections_per_ip)
                    });
                    let Some(addr) = addr else {
                        peer.is_attempting = false;
                        continue;
                    };

                    metrics::counter!("peer.unconditional.dial.total").increment(1);

                    // Drain the close notifications, so the counter's channel stays
                    // small.
                    active_connections.update_count();
                    let request = OutboundConnectorRequest {
                        addr,
                        connection_tracker: active_connections.track_connection(),
                    };
                    peer.dialing = Some(addr);
                    let outbound_connector = outbound_connector.clone();

                    events.push(
                        tokio::spawn(erased(
                            async move {
                                AttemptEvent::Dialed {
                                    index,
                                    result: outbound_connector.oneshot(request).await,
                                }
                            }
                            .in_current_span(),
                        ))
                        .wait_for_panics(),
                    );

                    // Security: start outbound connections at least this far apart,
                    // like the crawler does.
                    sleep(constants::MIN_OUTBOUND_PEER_CONNECTION_INTERVAL).await;
                }

                AttemptEvent::Dialed { index, result } => {
                    let peer: &mut RedialState = peers
                        .get_mut(index)
                        .expect("event indexes come from the peer list, which never shrinks");
                    peer.is_attempting = false;
                    let dialed_addr = peer
                        .dialing
                        .take()
                        .expect("every dial sets `dialing` before it starts");

                    match result {
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
                            let addr_label = dialed_addr.addr_label(expose_peer_addresses);

                            // A peer that moves to the v2 protocol closes this
                            // connection without a fault. Its v2 connection is in the
                            // registry, so the next check counts it as connected.
                            if error
                                .downcast_ref::<peer::HandshakeError>()
                                .is_some_and(peer::HandshakeError::is_neutral_disconnect)
                            {
                                debug!(
                                    peer = %addr_label,
                                    ?error,
                                    "an unconditional peer closed its legacy connection neutrally"
                                );
                                continue;
                            }

                            metrics::counter!("peer.unconditional.dial.failed.total").increment(1);

                            // Only the first failure since the last healthy connection
                            // is worth an operator's attention: a peer without a
                            // listener fails every dial.
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
            },

            _ = check_timer.tick() => {
                let connections = connections_with_dials_in_flight(&peer_registry, &peers);

                for (index, peer) in peers.iter_mut().enumerate() {
                    if peer.is_attempting {
                        continue;
                    }

                    let now = Instant::now();

                    if peer.addr.is_some_and(|addr| {
                        is_connected(addr, &connections, config.max_connections_per_ip)
                    }) {
                        let connected_since = *peer.connected_since.get_or_insert(now);
                        if now.saturating_duration_since(connected_since)
                            >= constants::UNCONDITIONAL_PEER_HEALTHY_CONNECTION
                        {
                            peer.attempts = 0;
                            // The delay of the attempt that connected no longer applies.
                            peer.next_attempt = now;
                        }
                        continue;
                    }
                    peer.connected_since = None;

                    if now < peer.next_attempt {
                        continue;
                    }

                    let delay = if peer.has_attempted {
                        redial_jitter()
                    } else {
                        Duration::ZERO
                    };

                    peer.is_attempting = true;
                    peer.has_attempted = true;
                    peer.next_attempt = now + redial_delay(peer.attempts);
                    peer.attempts = peer.attempts.saturating_add(1);

                    let lookup = peer
                        .name
                        .as_ref()
                        .map(|(_name_index, name, port)| lookup(name.clone(), *port));

                    events.push(
                        async move {
                            sleep(delay).await;
                            let lookup = match lookup {
                                Some(lookup) => Some(lookup.await),
                                None => None,
                            };
                            AttemptEvent::Delayed { index, lookup }
                        }
                        .boxed(),
                    );
                }
            }
        }
    }
}
