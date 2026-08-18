//! A peer set whose size is dynamically determined by resource constraints.
//!
//! The [`PeerSet`] implementation is adapted from the one in [tower::Balance][tower-balance].
//!
//! [tower-balance]: https://github.com/tower-rs/tower/tree/master/tower/src/balance

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    convert::Infallible,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::{
    future::{self, FutureExt},
    sink::SinkExt,
    stream::{FuturesUnordered, StreamExt},
    Future, TryFutureExt,
};
use rand::seq::SliceRandom;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
    time::{sleep, Instant},
};
use tokio_stream::wrappers::IntervalStream;
use tower::{
    buffer::Buffer, discover::Change, layer::Layer, util::BoxService, Service, ServiceExt,
};
use tracing_futures::Instrument;

use zakura_chain::{chain_tip::ChainTip, diagnostic::task::WaitForPanics};

use crate::{
    address_book_updater::{AddressBookUpdater, MIN_CHANNEL_SIZE},
    constants,
    meta_addr::{MetaAddr, MetaAddrChange},
    peer::{
        self, address_is_valid_for_inbound_listeners, HandshakeRequest, MinimumPeerVersion,
        OutboundConnectorRequest, PeerPreference,
    },
    peer_cache_updater::peer_cache_updater,
    peer_registry::PeerRegistry,
    peer_set::{set::MorePeers, ActiveConnectionCounter, CandidateSet, ConnectionTracker, PeerSet},
    protocol::external::{canonical_peer_addr, canonical_socket_addr, types::PeerServices},
    AddressBook, BannedIps, BoxError, Config, PeerSocketAddr, Request, Response,
};

#[cfg(test)]
mod tests;

mod recent_by_ip;

/// A successful outbound peer connection attempt or inbound connection handshake.
///
/// The [`Handshake`](peer::Handshake) service returns a [`Result`]. Only successful connections
/// should be sent on the channel. Errors should be logged or ignored.
///
/// We don't allow any errors in this type, because:
/// - The connection limits don't include failed connections
/// - tower::Discover interprets an error as stream termination
type DiscoveredPeer = (PeerSocketAddr, peer::Client);

/// How often accumulated peer misbehavior reports are sent to the address book.
///
/// Batching caps address book queue and mutex pressure at one flush per
/// interval, no matter how fast peers send invalid blocks or transactions.
/// Five seconds keeps ban-threshold score application prompt; do not remove the
/// batching to make bans immediate.
const MISBEHAVIOR_BATCH_INTERVAL: Duration = Duration::from_secs(5);

/// Batches peer misbehavior reports before forwarding them to the address book updater.
async fn batch_misbehavior_reports(
    mut misbehavior_rx: mpsc::Receiver<(PeerSocketAddr, u32)>,
    address_book_updater: mpsc::Sender<MetaAddrChange>,
) {
    let mut misbehaviors: HashMap<PeerSocketAddr, u32> = HashMap::new();
    // Coalesce repeated reports so invalid blocks or transactions cannot turn
    // every report into address book queue and mutex pressure.
    let mut flush_timer = IntervalStream::new(tokio::time::interval_at(
        Instant::now() + MISBEHAVIOR_BATCH_INTERVAL,
        MISBEHAVIOR_BATCH_INTERVAL,
    ));

    loop {
        tokio::select! {
            msg = misbehavior_rx.recv() => match msg {
                Some((peer_addr, score_increment)) => *misbehaviors
                    .entry(peer_addr)
                    .or_default()
                    += score_increment,
                None => break,
            },

            _ = flush_timer.next() => {
                for (addr, score_increment) in misbehaviors.drain() {
                    let _ = address_book_updater
                        .send(MetaAddr::new_misbehavior(addr, score_increment))
                        .await;
                }
            },
        };
    }

    tracing::warn!("exiting misbehavior update batch task");
}

/// Initialize a peer set, using a network `config`, `inbound_service`,
/// and `latest_chain_tip`.
///
/// The peer set abstracts away peer management to provide a
/// [`tower::Service`] representing "the network" that load-balances requests
/// over available peers.  The peer set automatically crawls the network to
/// find more peer addresses and opportunistically connects to new peers.
///
/// Each peer connection's message handling is isolated from other
/// connections, unlike in `zcashd`.  The peer connection first attempts to
/// interpret inbound messages as part of a response to a previously-issued
/// request.  Otherwise, inbound messages are interpreted as requests and sent
/// to the supplied `inbound_service`.
///
/// Wrapping the `inbound_service` in [`tower::load_shed`] middleware will
/// cause the peer set to shrink when the inbound service is unable to keep up
/// with the volume of inbound requests.
///
/// Use [`NoChainTip`][1] to explicitly provide no chain tip receiver.
///
/// In addition to returning a service for outbound requests, this method
/// returns a shared [`AddressBook`] updated with last-seen timestamps for
/// connected peers. The shared address book should be accessed using a
/// [blocking thread](https://docs.rs/tokio/1.15.0/tokio/task/index.html#blocking-and-yielding),
/// to avoid async task deadlocks.
///
/// # Panics
///
/// If `config.config.peerset_initial_target_size` is zero.
/// (zakura-network expects to be able to connect to at least one peer.)
///
/// [1]: zakura_chain::chain_tip::NoChainTip
pub async fn init<S, C>(
    config: Config,
    inbound_service: S,
    latest_chain_tip: C,
    user_agent: String,
    advertised_services: PeerServices,
) -> (
    Buffer<BoxService<Request, Response, BoxError>, Request>,
    Arc<std::sync::Mutex<AddressBook>>,
    mpsc::Sender<(PeerSocketAddr, u32)>,
)
where
    S: Service<Request, Response = Response, Error = BoxError> + Clone + Send + Sync + 'static,
    S::Future: Send + 'static,
    C: ChainTip + Clone + Send + Sync + 'static,
{
    let (peer_set, address_book, misbehavior_tx, _zakura_endpoint) = init_with_zakura_header_sync(
        config,
        inbound_service,
        latest_chain_tip,
        user_agent,
        advertised_services,
        Vec::new(),
        None,
    )
    .await;

    (peer_set, address_book, misbehavior_tx)
}

/// Initialize a peer set and optionally expose a real-driver Zakura header-sync endpoint.
pub async fn init_with_zakura_header_sync<S, C>(
    config: Config,
    inbound_service: S,
    latest_chain_tip: C,
    user_agent: String,
    advertised_services: PeerServices,
    block_gossip_peer_ips: Vec<IpAddr>,
    header_sync_driver_startup: Option<crate::zakura::ZakuraHeaderSyncDriverStartup>,
) -> (
    Buffer<BoxService<Request, Response, BoxError>, Request>,
    Arc<std::sync::Mutex<AddressBook>>,
    mpsc::Sender<(PeerSocketAddr, u32)>,
    Option<crate::zakura::ZakuraEndpoint>,
)
where
    S: Service<Request, Response = Response, Error = BoxError> + Clone + Send + Sync + 'static,
    S::Future: Send + 'static,
    C: ChainTip + Clone + Send + Sync + 'static,
{
    let (tcp_listener, listen_addr) = if config.legacy_p2p() {
        let (tcp_listener, listen_addr) = open_listener(&config.clone()).await;
        (Some(tcp_listener), listen_addr)
    } else {
        info!("legacy P2P disabled; not opening Zcash protocol listener");
        (None, config.listen_addr)
    };
    // Clone the inbound service for the Zakura legacy-gossip sink before the
    // handshake builder consumes the original below. The factory only runs when
    // `v2_p2p` is enabled; otherwise the endpoint is `None` and the clone drops.
    let inbound_for_zakura_sink = inbound_service.clone();
    let peer_registry = PeerRegistry::default();
    let zakura_endpoint = crate::zakura::spawn_zakura_endpoint_with_peer_registry(
        &config,
        move |supervisor, trace| {
            Arc::new(crate::zakura::LegacyGossipSink::spawn_with_trace(
                inbound_for_zakura_sink,
                supervisor,
                trace,
            )) as Arc<dyn crate::zakura::Service>
        },
        header_sync_driver_startup,
        peer_registry.clone(),
    )
    .await
    .expect("Zakura endpoint should start when P2P v2 is enabled");

    let (address_book, bans, address_book_updater, address_metrics, address_book_updater_guard) =
        AddressBookUpdater::spawn_with_peer_registry(
            &config,
            listen_addr,
            advertised_services,
            peer_registry.clone(),
        );

    let (misbehavior_tx, misbehavior_rx) = mpsc::channel(
        // Leave enough room for a misbehaviour update on every peer connection
        // before the channel is drained.
        config
            .peerset_total_connection_limit()
            .max(MIN_CHANNEL_SIZE),
    );

    tokio::spawn(
        batch_misbehavior_reports(misbehavior_rx, address_book_updater.clone()).in_current_span(),
    );

    // Create a broadcast channel for peer inventory advertisements.
    // If it reaches capacity, this channel drops older inventory advertisements.
    //
    // When Zebra is at the chain tip with an up-to-date mempool,
    // we expect to have at most 1 new transaction per connected peer,
    // and 1-2 new blocks across the entire network.
    // (The block syncer and mempool crawler handle bulk fetches of blocks and transactions.)
    let (inv_sender, inv_receiver) = broadcast::channel(config.peerset_total_connection_limit());

    // Construct services that handle inbound handshakes and perform outbound
    // handshakes. These use the same handshake service internally to detect
    // self-connection attempts. Both are decorated with a tower TimeoutLayer to
    // enforce timeouts as specified in the Config.

    // Inbound peers exempt from the inbound-overload connection drop: the
    // operator-configured block-gossip / zcashd-compat sidecars. Canonicalized
    // (IPv4-mapped IPv6 -> IPv4) so an inbound `::ffff:` address still matches,
    // exactly like the reserved-slot accounting in `accept_inbound_connections`.
    let protected_peer_ips: Arc<HashSet<IpAddr>> = Arc::new(
        block_gossip_peer_ips
            .iter()
            .map(|&ip| canonical_socket_addr(SocketAddr::new(ip, 0)).ip())
            .collect(),
    );

    let (listen_handshaker, outbound_connector) = {
        use tower::timeout::TimeoutLayer;
        let hs_timeout = TimeoutLayer::new(constants::HANDSHAKE_TIMEOUT);
        let zakura_handshake_connector = zakura_endpoint
            .as_ref()
            .map(|endpoint| endpoint.connector());

        let mut hs_builder = peer::Handshake::builder()
            .with_config(config.clone())
            .with_inbound_service(inbound_service)
            .with_inventory_collector(inv_sender)
            .with_address_book_updater(address_book_updater.clone())
            .with_advertised_services(advertised_services)
            .with_user_agent(user_agent)
            .with_latest_chain_tip(latest_chain_tip.clone())
            .with_peer_registry(peer_registry)
            .with_protected_peer_ips(protected_peer_ips)
            .want_transactions(true);

        if let Some(zakura_handshake_connector) = zakura_handshake_connector {
            hs_builder = hs_builder.with_zakura_handshake_connector(zakura_handshake_connector);
        }

        let hs = hs_builder
            .finish()
            .expect("configured all required parameters");
        (
            hs_timeout.layer(hs.clone()),
            hs_timeout.layer(peer::Connector::new(hs)),
        )
    };

    // Create an mpsc channel for peer changes,
    // based on the maximum number of inbound and outbound peers.
    //
    // The connection limit does not apply to errors,
    // so they need to be handled before sending to this channel.
    let (peerset_tx, peerset_rx) =
        futures::channel::mpsc::channel::<DiscoveredPeer>(config.peerset_total_connection_limit());

    let discovered_peers = peerset_rx.map(|(address, client)| {
        Result::<_, Infallible>::Ok(Change::Insert(address, client.into()))
    });

    // Create an mpsc channel for peerset demand signaling,
    // based on the maximum number of outbound peers.
    let (mut demand_tx, demand_rx) =
        futures::channel::mpsc::channel::<MorePeers>(config.peerset_outbound_connection_limit());

    // Create a oneshot to send background task JoinHandles to the peer set
    let (handle_tx, handle_rx) = tokio::sync::oneshot::channel();

    // Connect the rx end to a PeerSet, wrapping new peers in load instruments.
    let peer_set = PeerSet::new(
        &config,
        block_gossip_peer_ips.clone(),
        discovered_peers,
        demand_tx.clone(),
        handle_rx,
        inv_receiver,
        bans.clone(),
        address_metrics,
        MinimumPeerVersion::new(latest_chain_tip, &config.network),
        None,
    );
    let peer_set = Buffer::new(BoxService::new(peer_set), constants::PEERSET_BUFFER_SIZE);

    // Start the peer disk cache updater
    let peer_cache_updater_fut = peer_cache_updater(config.clone(), address_book.clone());
    let peer_cache_updater_guard = tokio::spawn(peer_cache_updater_fut.in_current_span());

    let mut task_handles = vec![address_book_updater_guard, peer_cache_updater_guard];

    if let Some(tcp_listener) = tcp_listener {
        // Connect peerset_tx to the 3 peer sources:
        //
        // 1. Incoming peer connections, via a listener.
        let listen_fut = accept_inbound_connections(
            config.clone(),
            tcp_listener,
            constants::MIN_INBOUND_PEER_CONNECTION_INTERVAL,
            listen_handshaker,
            peerset_tx.clone(),
            bans,
            block_gossip_peer_ips,
        );
        task_handles.push(tokio::spawn(listen_fut.in_current_span()));

        // 2. Initial peers, specified in the config and cached on disk.
        let initial_peers_fut = add_initial_peers(
            config.clone(),
            outbound_connector.clone(),
            peerset_tx.clone(),
            address_book_updater.clone(),
        );
        let initial_peers_join = tokio::spawn(initial_peers_fut.in_current_span());

        // 3. Outgoing peers we connect to in response to load.
        let mut candidates = CandidateSet::new(address_book.clone(), peer_set.clone());

        // Wait for the initial seed peer count
        let mut active_outbound_connections = initial_peers_join
            .wait_for_panics()
            .await
            .expect("unexpected error connecting to initial peers");
        let active_initial_peer_count = active_outbound_connections.update_count();

        // We need to await candidates.update() here,
        // because zcashd rate-limits `addr`/`addrv2` messages per connection,
        // and if we only have one initial peer,
        // we need to ensure that its `Response::Addr` is used by the crawler.
        //
        // TODO: this might not be needed after we added the Connection peer address cache,
        //       try removing it in a future release?
        info!(
            ?active_initial_peer_count,
            "sending initial request for peers"
        );
        let _ = candidates.update_initial(active_initial_peer_count).await;

        // Compute remaining connections to open.
        let demand_count = config
            .peerset_initial_target_size
            .saturating_sub(active_outbound_connections.update_count());

        for _ in 0..demand_count {
            let _ = demand_tx.try_send(MorePeers);
        }

        // Start the peer crawler
        let crawl_fut = crawl_and_dial(
            config.clone(),
            demand_tx,
            demand_rx,
            candidates,
            outbound_connector,
            peerset_tx,
            active_outbound_connections,
            address_book_updater,
            Arc::new(AtomicBool::new(false)),
        );
        task_handles.push(tokio::spawn(crawl_fut.in_current_span()));
    } else {
        task_handles.push(tokio::spawn(
            async move {
                let _peerset_tx = peerset_tx;
                let mut demand_rx = demand_rx;

                while let Some(MorePeers) = demand_rx.next().await {
                    debug!("ignoring legacy peer demand because legacy P2P is disabled");
                }

                future::pending::<Result<(), BoxError>>().await
            }
            .in_current_span(),
        ));
    }

    // Capture the supervisor before the endpoint is moved into the keep-alive
    // task, so we can back the dual-stack adapters with the same first-seen cache.
    let zakura_supervisor_and_trace = zakura_endpoint
        .as_ref()
        .map(|endpoint| (endpoint.supervisor(), endpoint.trace()));

    let returned_zakura_endpoint = zakura_endpoint.clone();
    if let Some(zakura_endpoint) = zakura_endpoint {
        task_handles.push(tokio::spawn(async move {
            let _zakura_endpoint = zakura_endpoint;
            future::pending::<Result<(), BoxError>>().await
        }));
    }

    handle_tx.send(task_handles).unwrap();

    // When the Zakura endpoint is active, wrap the legacy peer set so locally
    // originated gossip and inventory fetches also flow over Zakura. The internal
    // candidate set and crawler keep using the unwrapped legacy peer set above;
    // only the service handed to the syncer/mempool/inbound becomes dual-stack.
    let peer_set = match zakura_supervisor_and_trace {
        Some((supervisor, trace)) => {
            let dual_stack = crate::zakura::ZakuraDualStackService::new_with_trace(
                peer_set,
                supervisor,
                config.legacy_p2p(),
                trace,
                config.zakura.stream_open_rate_per_second,
            );
            Buffer::new(BoxService::new(dual_stack), constants::PEERSET_BUFFER_SIZE)
        }
        None => peer_set,
    };

    (
        peer_set,
        address_book,
        misbehavior_tx,
        returned_zakura_endpoint,
    )
}

/// Use the provided `outbound_connector` to connect to the configured DNS seeder and
/// disk cache initial peers, then send the resulting peer connections over `peerset_tx`.
///
/// Also sends every initial peer address to the `address_book_updater`.
#[instrument(skip(config, outbound_connector, peerset_tx, address_book_updater))]
async fn add_initial_peers<S>(
    config: Config,
    outbound_connector: S,
    mut peerset_tx: futures::channel::mpsc::Sender<DiscoveredPeer>,
    address_book_updater: tokio::sync::mpsc::Sender<MetaAddrChange>,
) -> Result<ActiveConnectionCounter, BoxError>
where
    S: Service<
            OutboundConnectorRequest,
            Response = (PeerSocketAddr, peer::Client),
            Error = BoxError,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    let initial_peers = limit_initial_peers(&config, address_book_updater).await;

    let mut handshake_success_total: usize = 0;
    let mut handshake_error_total: usize = 0;

    let mut active_outbound_connections = ActiveConnectionCounter::new_counter_with(
        config.peerset_outbound_connection_limit(),
        "Outbound Connections",
    );

    // TODO: update when we add Tor peers or other kinds of addresses.
    let ipv4_peer_count = initial_peers.iter().filter(|ip| ip.is_ipv4()).count();
    let ipv6_peer_count = initial_peers.iter().filter(|ip| ip.is_ipv6()).count();
    info!(
        ?ipv4_peer_count,
        ?ipv6_peer_count,
        "connecting to initial peer set"
    );

    // # Security
    //
    // Resists distributed denial of service attacks by making sure that
    // new peer connections are initiated at least `MIN_OUTBOUND_PEER_CONNECTION_INTERVAL` apart.
    //
    // # Correctness
    //
    // Each `FuturesUnordered` can hold one `Buffer` or `Batch` reservation for
    // an indefinite period. We can use `FuturesUnordered` without filling
    // the underlying network buffers, because we immediately drive this
    // single `FuturesUnordered` to completion, and handshakes have a short timeout.
    let mut handshakes: FuturesUnordered<_> = initial_peers
        .into_iter()
        .enumerate()
        .map(|(i, addr)| {
            let connection_tracker = active_outbound_connections.track_connection();
            let req = OutboundConnectorRequest {
                addr,
                connection_tracker,
            };
            let outbound_connector = outbound_connector.clone();

            // Spawn a new task to make the outbound connection.
            tokio::spawn(
                async move {
                    // Only spawn one outbound connector per
                    // `MIN_OUTBOUND_PEER_CONNECTION_INTERVAL`,
                    // by sleeping for the interval multiplied by the peer's index in the list.
                    sleep(
                        constants::MIN_OUTBOUND_PEER_CONNECTION_INTERVAL.saturating_mul(i as u32),
                    )
                    .await;

                    // As soon as we create the connector future,
                    // the handshake starts running as a spawned task.
                    outbound_connector
                        .oneshot(req)
                        .map_err(move |e| (addr, e))
                        .await
                }
                .in_current_span(),
            )
            .wait_for_panics()
        })
        .collect();

    while let Some(handshake_result) = handshakes.next().await {
        match handshake_result {
            Ok(change) => {
                handshake_success_total += 1;
                let peer_addr = change.0;
                debug!(
                    ?handshake_success_total,
                    ?handshake_error_total,
                    peer = %peer_addr.addr_label(config.expose_peer_addresses),
                    "an initial peer handshake succeeded"
                );

                // The connection limit makes sure this send doesn't block
                peerset_tx.send(change).await?;
            }
            Err((addr, ref e)) => {
                handshake_error_total += 1;
                let addr_label = addr.addr_label(config.expose_peer_addresses);

                // this is verbose, but it's better than just hanging with no output when there are errors
                let mut expected_error = false;
                if let Some(io_error) = e.downcast_ref::<tokio::io::Error>() {
                    // Some systems only have IPv4, or only have IPv6,
                    // so these errors are not particularly interesting.
                    if io_error.kind() == tokio::io::ErrorKind::AddrNotAvailable {
                        expected_error = true;
                    }
                }

                if expected_error {
                    debug!(
                        successes = ?handshake_success_total,
                        errors = ?handshake_error_total,
                        peer = %addr_label,
                        ?e,
                        "an initial peer connection failed"
                    );
                } else {
                    info!(
                        successes = ?handshake_success_total,
                        errors = ?handshake_error_total,
                        peer = %addr_label,
                        %e,
                        "an initial peer connection failed"
                    );
                }
            }
        }

        // Security: Let other tasks run after each connection is processed.
        //
        // Avoids remote peers starving other Zebra tasks using initial connection successes or errors.
        tokio::task::yield_now().await;
    }

    let outbound_connections = active_outbound_connections.update_count();
    info!(
        ?handshake_success_total,
        ?handshake_error_total,
        ?outbound_connections,
        "finished connecting to initial seed and disk cache peers"
    );

    Ok(active_outbound_connections)
}

/// Limit the number of `initial_peers` addresses entries to the configured
/// `peerset_initial_target_size`.
///
/// Returns randomly chosen entries from the provided set of addresses,
/// in a random order.
///
/// Also sends every initial peer to the `address_book_updater`.
async fn limit_initial_peers(
    config: &Config,
    address_book_updater: tokio::sync::mpsc::Sender<MetaAddrChange>,
) -> HashSet<PeerSocketAddr> {
    let all_peers: HashSet<PeerSocketAddr> = config.initial_peers().await;
    let mut preferred_peers: BTreeMap<PeerPreference, Vec<PeerSocketAddr>> = BTreeMap::new();

    let all_peers_count = all_peers.len();
    if all_peers_count > config.peerset_initial_target_size {
        info!(
            "limiting the initial peers list from {} to {}",
            all_peers_count, config.peerset_initial_target_size,
        );
    }

    // Filter out invalid initial peers, and prioritise valid peers for initial connections.
    // (This treats initial peers the same way we treat gossiped peers.)
    for peer_addr in all_peers {
        let preference = PeerPreference::new(peer_addr, config.network.clone());

        match preference {
            Ok(preference) => preferred_peers
                .entry(preference)
                .or_default()
                .push(peer_addr),
            Err(error) => info!(
                peer = %peer_addr.addr_label(config.expose_peer_addresses),
                ?error,
                "invalid initial peer from DNS seeder, configured IP address, or disk cache",
            ),
        }
    }

    // Send every initial peer to the address book, in preferred order.
    // (This treats initial peers the same way we treat gossiped peers.)
    //
    // # Security
    //
    // Initial peers are limited because:
    // - the number of initial peers is limited
    // - this code only runs once at startup
    for peer in preferred_peers.values().flatten() {
        let peer_addr = MetaAddr::new_initial_peer(*peer);
        // `send` only waits when the channel is full.
        // The address book updater runs in its own thread, so we will only wait for a short time.
        let _ = address_book_updater.send(peer_addr).await;
    }

    // Split out the `initial_peers` that will be shuffled and returned,
    // choosing preferred peers first.
    let mut initial_peers: HashSet<PeerSocketAddr> = HashSet::new();
    for better_peers in preferred_peers.values() {
        let mut better_peers = better_peers.clone();
        let (chosen_peers, _unused_peers) = better_peers.partial_shuffle(
            &mut rand::thread_rng(),
            config.peerset_initial_target_size - initial_peers.len(),
        );

        initial_peers.extend(chosen_peers.iter());

        if initial_peers.len() >= config.peerset_initial_target_size {
            break;
        }
    }

    initial_peers
}

/// Open a peer connection listener on `config.listen_addr`,
/// returning the opened [`TcpListener`], and the address it is bound to.
///
/// If the listener is configured to use an automatically chosen port (port `0`),
/// then the returned address will contain the actual port.
///
/// # Panics
///
/// If opening the listener fails.
#[instrument(skip(config), fields(addr = ?config.listen_addr))]
pub(crate) async fn open_listener(config: &Config) -> (TcpListener, SocketAddr) {
    // Warn if we're configured using the wrong network port.
    if let Err(wrong_addr) =
        address_is_valid_for_inbound_listeners(config.listen_addr, config.network.clone())
    {
        warn!(
            "We are configured with address {} on {:?}, but it could cause network issues. \
             The default port for {:?} is {}. Error: {wrong_addr:?}",
            config.listen_addr,
            config.network,
            config.network,
            config.network.default_port(),
        );
    }

    info!(
        "Trying to open Zcash protocol endpoint at {}...",
        config.listen_addr
    );
    let listener_result = TcpListener::bind(config.listen_addr).await;

    let listener = match listener_result {
        Ok(l) => l,
        Err(e) => panic!(
            "Opening Zcash network protocol listener {:?} failed: {e:?}. \
             Hint: Check if another zakurad or zcashd process is running. \
             Try changing the network listen_addr in the Zakura config.",
            config.listen_addr,
        ),
    };

    let local_addr = listener
        .local_addr()
        .expect("unexpected missing local addr for open listener");
    info!("Opened Zcash protocol endpoint at {}", local_addr);

    (listener, local_addr)
}

const MAX_INBOUND_ACCEPT_BURST: u32 = 16;
const MIN_INBOUND_ACCEPT_BURST_INTERVAL: Duration = Duration::from_millis(100);

/// Listens for peer connections on `addr`, then sets up each connection as a
/// Zcash peer.
///
/// Uses `handshaker` to perform a Zcash network protocol handshake, and sends
/// the [`peer::Client`] result over `peerset_tx`.
///
/// Limits the number of active inbound connections based on `config`,
/// and waits `min_inbound_peer_connection_interval` between connections.
///
/// A bounded accept burst drains connections that are already queued by the
/// kernel. Without it, the normal one-second handshake pacing can keep a full
/// listen backlog from ever reaching a configured local sidecar connection.
#[instrument(skip(config, listener, handshaker, peerset_tx), fields(listener_addr = ?listener.local_addr()))]
async fn accept_inbound_connections<S>(
    config: Config,
    listener: TcpListener,
    min_inbound_peer_connection_interval: Duration,
    handshaker: S,
    peerset_tx: futures::channel::mpsc::Sender<DiscoveredPeer>,
    bans: BannedIps,
    zcashd_compat_peer_ips: Vec<IpAddr>,
) -> Result<(), BoxError>
where
    S: Service<peer::HandshakeRequest<TcpStream>, Response = peer::Client, Error = BoxError>
        + Clone,
    S::Future: Send + 'static,
{
    let mut recent_inbound_connections =
        recent_by_ip::RecentByIp::new(None, Some(config.max_connections_per_ip));

    let mut active_inbound_connections = ActiveConnectionCounter::new_counter_with(
        config.peerset_inbound_connection_limit(),
        "Inbound Connections",
    );
    let mut active_zcashd_compat_connections =
        ActiveConnectionCounter::new_counter_with(1, "zcashd-compat Inbound Connection");
    let zcashd_compat_peer_ips: HashSet<_> = zcashd_compat_peer_ips
        .into_iter()
        .map(|ip| canonical_socket_addr(SocketAddr::new(ip, 0)).ip())
        .collect();
    let zcashd_compat_reserved_slots = usize::from(
        !zcashd_compat_peer_ips.is_empty() && config.peerset_inbound_connection_limit() > 0,
    );
    let public_inbound_connection_limit = config
        .peerset_inbound_connection_limit()
        .saturating_sub(zcashd_compat_reserved_slots);

    let mut handshakes: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>> =
        FuturesUnordered::new();
    // Keeping an unresolved future in the pool means the stream never terminates.
    handshakes.push(future::pending().boxed());

    let mut prefetched_inbound = None;
    let mut accept_burst_len = 0u32;

    loop {
        // Check for panics in finished tasks, before accepting new connections
        let inbound_result = if let Some(inbound_result) = prefetched_inbound.take() {
            inbound_result
        } else {
            tokio::select! {
                biased;
                next_handshake_res = handshakes.next() => match next_handshake_res {
                    // The task has already sent the peer change to the peer set.
                    Some(()) => continue,
                    None => unreachable!("handshakes never terminates, because it contains a future that never resolves"),
                },

                // This future must wait until new connections are available: it can't have a timeout.
                inbound_result = listener.accept() => inbound_result,
            }
        };

        if let Ok((tcp_stream, addr)) = inbound_result {
            // Canonicalize IPv4-mapped IPv6 (`::ffff:A.B.C.D`) from dual-stack sockets so the
            // ban check, the recent-inbound limiter, and the PeerSet key all match the
            // canonical form used by the ban map and address book.
            //
            // TODO(test): this async accept loop over a real `TcpListener` isn't unit-tested
            // directly; the equivalence of mapped and canonical forms is covered by the
            // peer-set tests in `peer_set::set::tests`.
            let addr: PeerSocketAddr = canonical_peer_addr(addr);
            let addr_label = addr.addr_label(config.expose_peer_addresses);

            if bans.contains(addr.ip()) {
                debug!(peer = %addr_label, "banned inbound connection attempt");
                std::mem::drop(tcp_stream);
                continue;
            }

            let active_public_inbound_connections = active_inbound_connections.update_count();
            let active_zcashd_compat_inbound_connections =
                active_zcashd_compat_connections.update_count();
            let active_total_inbound_connections =
                active_public_inbound_connections + active_zcashd_compat_inbound_connections;
            let canonical_ip = canonical_socket_addr(addr.remove_socket_addr_privacy()).ip();
            let is_zcashd_compat_peer = zcashd_compat_peer_ips.contains(&canonical_ip);
            let connection_tracker = if is_zcashd_compat_peer {
                if active_zcashd_compat_inbound_connections >= zcashd_compat_reserved_slots
                    || active_total_inbound_connections >= config.peerset_inbound_connection_limit()
                {
                    // Too many zcashd-compat or total open inbound connections already.
                    // Close the connection.
                    std::mem::drop(tcp_stream);
                    // Allow invalid connections to be cleared quickly,
                    // but still put a limit on our CPU and network usage from failed connections.
                    tokio::time::sleep(constants::MIN_INBOUND_PEER_FAILED_CONNECTION_INTERVAL)
                        .await;
                    continue;
                }

                active_zcashd_compat_connections.track_connection()
            } else if active_public_inbound_connections >= public_inbound_connection_limit
                || active_total_inbound_connections >= config.peerset_inbound_connection_limit()
                || recent_inbound_connections.is_past_limit_or_add(addr.ip())
            {
                // Too many open inbound connections or pending handshakes already.
                // Close the connection.
                std::mem::drop(tcp_stream);
                // Allow invalid connections to be cleared quickly,
                // but still put a limit on our CPU and network usage from failed connections.
                tokio::time::sleep(constants::MIN_INBOUND_PEER_FAILED_CONNECTION_INTERVAL).await;
                continue;
            } else {
                active_inbound_connections.track_connection()
            };
            debug!(
                inbound_connections = ?active_total_inbound_connections,
                ?is_zcashd_compat_peer,
                "handshaking on an open inbound peer connection"
            );

            let handshake_task = accept_inbound_handshake(
                addr,
                addr_label,
                handshaker.clone(),
                tcp_stream,
                connection_tracker,
                peerset_tx.clone(),
            )
            .await?
            .wait_for_panics();

            handshakes.push(handshake_task);

            // Poll once to detect and retain a connection already queued by the kernel.
            // Keeping that result lets us drain a bounded burst without racing or losing it.
            let accept_ready = if let Some(inbound_result) = listener.accept().now_or_never() {
                prefetched_inbound = Some(inbound_result);
                accept_burst_len += 1;
                true
            } else {
                accept_burst_len = 0;
                false
            };

            // Configured sidecars skip pacing so a drained backlog connection can
            // complete handshake promptly.
            if !is_zcashd_compat_peer {
                if accept_ready && accept_burst_len < MAX_INBOUND_ACCEPT_BURST {
                    tokio::time::sleep(MIN_INBOUND_ACCEPT_BURST_INTERVAL).await;
                } else {
                    accept_burst_len = 0;
                    // Rate-limit successful public handshakes when no backlog is
                    // visible, and after each bounded drain burst.
                    tokio::time::sleep(min_inbound_peer_connection_interval).await;
                }
            }
        } else {
            // Allow invalid connections to be cleared quickly,
            // but still put a limit on our CPU and network usage from failed connections.
            debug!(?inbound_result, "error accepting inbound connection");
            tokio::time::sleep(constants::MIN_INBOUND_PEER_FAILED_CONNECTION_INTERVAL).await;
        }

        // Security: Let other tasks run after each connection is processed.
        //
        // Avoids remote peers starving other Zebra tasks using inbound connection successes or
        // errors.
        //
        // Preventing a denial of service is important in this code, so we want to sleep *and* make
        // the next connection after other tasks have run. (Sleeps are not guaranteed to do that.)
        tokio::task::yield_now().await;
    }
}

/// Set up a new inbound connection as a Zcash peer.
///
/// Uses `handshaker` to perform a Zcash network protocol handshake, and sends
/// the [`peer::Client`] result over `peerset_tx`.
//
// TODO: when we support inbound proxies, distinguish between proxied listeners and
//       direct listeners in the span generated by this instrument macro
#[instrument(
    skip(addr, addr_label, handshaker, tcp_stream, connection_tracker, peerset_tx),
    fields(peer = %addr_label)
)]
async fn accept_inbound_handshake<S>(
    addr: PeerSocketAddr,
    addr_label: String,
    mut handshaker: S,
    tcp_stream: TcpStream,
    connection_tracker: ConnectionTracker,
    peerset_tx: futures::channel::mpsc::Sender<DiscoveredPeer>,
) -> Result<tokio::task::JoinHandle<()>, BoxError>
where
    S: Service<peer::HandshakeRequest<TcpStream>, Response = peer::Client, Error = BoxError>
        + Clone,
    S::Future: Send + 'static,
{
    let connected_addr = peer::ConnectedAddr::new_inbound_direct(addr);

    debug!("got incoming connection");

    // # Correctness
    //
    // Holding the drop guard returned by Span::enter across .await points will
    // result in incorrect traces if it yields.
    //
    // This await is okay because the handshaker's `poll_ready` method always returns Ready.
    handshaker.ready().await?;

    // Construct a handshake future but do not drive it yet....
    let handshake = handshaker.call(HandshakeRequest {
        data_stream: tcp_stream,
        connected_addr,
        connection_tracker,
    });
    // ... instead, spawn a new task to handle this connection
    let mut peerset_tx = peerset_tx.clone();

    let handshake_task = tokio::spawn(
        async move {
            let handshake_result = handshake.await;

            if let Ok(client) = handshake_result {
                // The connection limit makes sure this send doesn't block
                let _ = peerset_tx.send((addr, client)).await;
            } else {
                debug!(?handshake_result, "error handshaking with inbound peer");
            }
        }
        .in_current_span(),
    );

    Ok(handshake_task)
}

/// An action that the peer crawler can take.
enum CrawlerAction {
    /// Drop the demand signal because there are too many pending handshakes.
    DemandDrop,
    /// Initiate a handshake to the next candidate peer in response to demand.
    ///
    /// If there are no available candidates, crawl existing peers.
    ///
    /// `restore_replenishment_demand` is set when this action consumed a unit of
    /// timer replenishment demand. Failed dials that restore `MorePeers` must
    /// credit that unit so the retry does not permanently double-spend the budget.
    DemandHandshakeOrCrawl { restore_replenishment_demand: bool },
    /// Crawl existing peers for more peers in response to a timer `tick`.
    TimerCrawl { tick: Instant },
    /// Clear a finished successful handshake.
    HandshakeFinished,
    /// Clear a finished failed handshake.
    ///
    /// When `restore_replenishment_demand` is set, credits one unit of remaining
    /// replenishment demand. That flag is only set when this attempt both spent
    /// timer budget and successfully restored `MorePeers` to the demand channel,
    /// so a full channel cannot credit budget without a matching later decrement.
    HandshakeFailed { restore_replenishment_demand: bool },
    /// Clear a finished demand crawl (DemandHandshakeOrCrawl with no peers).
    DemandCrawlFinished,
    /// Clear a finished TimerCrawl.
    TimerCrawlFinished,
}

const OUTBOUND_PEER_REPLENISHMENT_TARGET_NUMERATOR: usize = 80;
const OUTBOUND_PEER_REPLENISHMENT_TARGET_DENOMINATOR: usize = 100;

/// Returns the number of outbound peers needed to reach 80% of the configured
/// peer set target size, bounded by the outbound connection limit.
///
/// The target is keyed to `peerset_initial_target_size`, the operator's stated
/// desired peer set size. Keying it to the outbound connection limit instead
/// makes every change to that safety ceiling silently move the steady-state
/// outbound peer count.
///
/// The connection limit still bounds the result, so lowering it below the
/// target does not leave the crawler dialing forever against the ceiling.
///
/// The target calculation avoids overflow by applying the ratio separately to
/// the quotient and remainder. [`usize::div_ceil`] rounds partial peers upward,
/// and [`usize::saturating_sub`] returns zero at or above the target.
fn outbound_peer_replenishment_demand(
    active_outbound_connections: usize,
    peerset_target_size: usize,
    outbound_connection_limit: usize,
) -> usize {
    let target_outbound_connections = (peerset_target_size
        / OUTBOUND_PEER_REPLENISHMENT_TARGET_DENOMINATOR)
        .saturating_mul(OUTBOUND_PEER_REPLENISHMENT_TARGET_NUMERATOR)
        .saturating_add(
            (peerset_target_size % OUTBOUND_PEER_REPLENISHMENT_TARGET_DENOMINATOR)
                .saturating_mul(OUTBOUND_PEER_REPLENISHMENT_TARGET_NUMERATOR)
                .div_ceil(OUTBOUND_PEER_REPLENISHMENT_TARGET_DENOMINATOR),
        )
        .min(outbound_connection_limit);

    target_outbound_connections.saturating_sub(active_outbound_connections)
}

/// Given a channel `demand_rx` that signals a need for new peers, try to find
/// and connect to new peers, and send the resulting `peer::Client`s through the
/// `peerset_tx` channel.
///
/// Crawl for new peers every `config.crawl_new_peer_interval`.
/// Also crawl whenever there is demand, but no new peers in `candidates`.
/// After a periodic crawl, refresh the connection demand needed to reach 80%
/// of the configured peer set target size.
///
/// If a handshake fails, restore the unused demand signal by sending it to
/// `demand_tx`. When that restore succeeds and the attempt consumed timer
/// budget, credit one replenishment unit so the restored signal does not
/// permanently double-spend the budget. A full demand channel does not credit,
/// because there will be no later channel-driven decrement.
///
/// The crawler terminates when `candidates.update()` or `peerset_tx` returns a
/// permanent internal error. Transient errors and individual peer errors should
/// be handled within the crawler.
///
/// Uses `active_outbound_connections` to limit the number of active outbound connections
/// across both the initial peers and crawler. The limit is based on `config`.
#[allow(clippy::too_many_arguments)]
#[instrument(
    skip(
        config,
        demand_tx,
        demand_rx,
        candidates,
        outbound_connector,
        peerset_tx,
        active_outbound_connections,
        address_book_updater,
        force_failed_dial_demand_restore_full,
    ),
    fields(
        new_peer_interval = ?config.crawl_new_peer_interval,
    )
)]
async fn crawl_and_dial<C, S>(
    config: Config,
    demand_tx: futures::channel::mpsc::Sender<MorePeers>,
    mut demand_rx: futures::channel::mpsc::Receiver<MorePeers>,
    candidates: CandidateSet<S>,
    outbound_connector: C,
    peerset_tx: futures::channel::mpsc::Sender<DiscoveredPeer>,
    mut active_outbound_connections: ActiveConnectionCounter,
    address_book_updater: tokio::sync::mpsc::Sender<MetaAddrChange>,
    // When true, failed dials report a full demand channel so tests can cover
    // unrestored-demand credit gating without racing the crawler select loop.
    force_failed_dial_demand_restore_full: Arc<AtomicBool>,
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
    S: Service<Request, Response = Response, Error = BoxError> + Send + Sync + 'static,
    S::Future: Send + 'static,
{
    use CrawlerAction::*;

    info!(
        crawl_new_peer_interval = ?config.crawl_new_peer_interval,
        outbound_connections = ?active_outbound_connections.update_count(),
        "starting the peer address crawler",
    );

    // # Concurrency
    //
    // Allow tasks using the candidate set to be spawned, so they can run concurrently.
    // Previously, Zebra has had deadlocks and long hangs caused by running dependent
    // candidate set futures in the same async task.
    let candidates = Arc::new(futures::lock::Mutex::new(candidates));

    // This contains both crawl and handshake tasks.
    let mut handshakes: FuturesUnordered<
        Pin<Box<dyn Future<Output = Result<CrawlerAction, BoxError>> + Send>>,
    > = FuturesUnordered::new();
    // <FuturesUnordered as Stream> returns None when empty.
    // Keeping an unresolved future in the pool means the stream never terminates.
    handshakes.push(future::pending().boxed());

    let mut crawl_timer = tokio::time::interval(config.crawl_new_peer_interval);
    // If the crawl is delayed, also delay all future crawls.
    // (Shorter intervals just add load, without any benefit.)
    crawl_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut crawl_timer = IntervalStream::new(crawl_timer).map(|tick| TimerCrawl { tick });

    // Periodic replenishment is best-effort. Every consumed demand action
    // decrements this counter, even if it crawls instead of dialing. The next
    // periodic crawl refreshes any remaining deficit.
    let remaining_replenishment_demand = AtomicUsize::new(0);

    // # Concurrency
    //
    // To avoid hangs and starvation, the crawler must spawn a separate task for each crawl
    // and handshake, so they can make progress independently (and avoid deadlocking each other).
    loop {
        metrics::gauge!("crawler.in_flight_handshakes").set(
            handshakes
                .len()
                .checked_sub(1)
                .expect("the pool always contains an unresolved future") as f64,
        );

        let crawler_action = tokio::select! {
            biased;
            // Check for completed handshakes first, because the rest of the app needs them.
            // Pending handshakes are limited by the connection limit.
            next_handshake_res = handshakes.next() => next_handshake_res.expect(
                "handshakes never terminates, because it contains a future that never resolves"
            ),
            // The timer is rate-limited
            next_timer = crawl_timer.next() => Ok(next_timer.expect("timers never terminate")),
            // External demand is potentially unlimited, so it goes after the
            // rate-limited timer in this biased select.
            next_demand = demand_rx.next() => next_demand
                .ok_or("demand stream closed, is Zakura shutting down?".into())
                // Placeholder flag; replaced below after checking the live counter.
                .map(|MorePeers| DemandHandshakeOrCrawl {
                    restore_replenishment_demand: false,
                }),
            // Existing channel demand gets priority over local replenishment.
            // Each action consumes one unit of replenishment demand below.
            _ = future::ready(()),
                if remaining_replenishment_demand.load(Ordering::Relaxed) > 0 =>
            {
                Ok(DemandHandshakeOrCrawl {
                    restore_replenishment_demand: false,
                })
            }
        };

        let crawler_action = match crawler_action {
            Ok(DemandHandshakeOrCrawl { .. }) => {
                // Only credit later if this action actually consumed a timer
                // replenishment unit. Channel demand with remaining == 0 must
                // not invent local budget on failure.
                let restore_replenishment_demand =
                    remaining_replenishment_demand.load(Ordering::Relaxed) > 0;
                // Rust 1.97 replaces this API with `try_update`.
                // Zakura supports Rust 1.91.
                #[allow(deprecated)]
                let _ = remaining_replenishment_demand.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |demand| Some(demand.saturating_sub(1)),
                );

                if active_outbound_connections.update_count()
                    >= config.peerset_outbound_connection_limit()
                {
                    remaining_replenishment_demand.store(0, Ordering::Relaxed);
                    Ok(DemandDrop)
                } else {
                    Ok(DemandHandshakeOrCrawl {
                        restore_replenishment_demand,
                    })
                }
            }
            crawler_action => crawler_action,
        };

        match crawler_action {
            // Dummy actions
            Ok(DemandDrop) => {
                // This is set to trace level because when the peer set is
                // congested it can generate a lot of demand signals.
                trace!("too many open connections or in-flight handshakes, dropping demand signal");
            }

            // Spawned tasks
            Ok(DemandHandshakeOrCrawl {
                restore_replenishment_demand,
            }) => {
                let candidates = candidates.clone();
                let outbound_connector = outbound_connector.clone();
                let peerset_tx = peerset_tx.clone();
                let address_book_updater = address_book_updater.clone();
                let demand_tx = demand_tx.clone();
                let expose_peer_addresses = config.expose_peer_addresses;
                let force_failed_dial_demand_restore_full =
                    force_failed_dial_demand_restore_full.clone();

                // Increment the connection count before we spawn the connection.
                let outbound_connection_tracker = active_outbound_connections.track_connection();
                let outbound_connections = active_outbound_connections.update_count();
                debug!(?outbound_connections, "opening an outbound peer connection");

                // Spawn each handshake or crawl into an independent task, so handshakes can
                // make progress while crawls are running.
                //
                // # Concurrency
                //
                // The peer crawler must be able to make progress even if some handshakes are
                // rate-limited. So the async mutex and next peer timeout are awaited inside the
                // spawned task.
                let handshake_or_crawl_handle = tokio::spawn(
                    async move {
                        // Try to get the next available peer for a handshake.
                        //
                        // candidates.next() has a short timeout, and briefly holds the address
                        // book lock, so it shouldn't hang.
                        //
                        // Hold the lock for as short a time as possible.
                        let candidate = { candidates.lock().await.next().await };

                        if let Some(candidate) = candidate {
                            // we don't need to spawn here, because there's nothing running concurrently
                            match dial(
                                candidate,
                                outbound_connector,
                                outbound_connection_tracker,
                                outbound_connections,
                                peerset_tx,
                                address_book_updater,
                                demand_tx,
                                expose_peer_addresses,
                                force_failed_dial_demand_restore_full,
                            )
                            .await?
                            {
                                DialOutcome::Connected => Ok(HandshakeFinished),
                                DialOutcome::Failed { demand_restored } => Ok(HandshakeFailed {
                                    restore_replenishment_demand: restore_replenishment_demand
                                        && demand_restored,
                                }),
                            }
                        } else {
                            // There weren't any peers, so try to get more peers.
                            debug!("demand for peers but no available candidates");

                            crawl(candidates, demand_tx).await?;

                            Ok(DemandCrawlFinished)
                        }
                    }
                    .in_current_span(),
                )
                .wait_for_panics();

                handshakes.push(handshake_or_crawl_handle);
            }
            Ok(TimerCrawl { tick }) => {
                let candidates = candidates.clone();
                let demand_tx = demand_tx.clone();

                let crawl_handle = tokio::spawn(
                    async move {
                        debug!(
                            ?tick,
                            "crawling for more peers in response to the crawl timer"
                        );

                        crawl(candidates, demand_tx).await?;

                        Ok(TimerCrawlFinished)
                    }
                    .in_current_span(),
                )
                .wait_for_panics();

                handshakes.push(crawl_handle);
            }

            // Completed spawned tasks
            Ok(HandshakeFinished) => {
                // Already logged in dial()
            }
            Ok(HandshakeFailed {
                restore_replenishment_demand,
            }) => {
                // Credit only when this attempt spent timer budget and dial()
                // successfully restored MorePeers. A full channel must not
                // credit, or remaining would rise without a matching decrement.
                if restore_replenishment_demand {
                    let _ = remaining_replenishment_demand.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(DemandCrawlFinished) => {
                // This is set to trace level because when the peerset is
                // congested it can generate a lot of demand signal very rapidly.
                trace!("demand-based crawl finished");
            }
            Ok(TimerCrawlFinished) => {
                let active_outbound_connections = active_outbound_connections.update_count();
                let peerset_target_size = config.peerset_initial_target_size;
                let outbound_connection_limit = config.peerset_outbound_connection_limit();
                let replenishment_demand = outbound_peer_replenishment_demand(
                    active_outbound_connections,
                    peerset_target_size,
                    outbound_connection_limit,
                );
                remaining_replenishment_demand.store(replenishment_demand, Ordering::Relaxed);

                // This is the only place the crawler's view of its own outbound
                // connections is visible, and a node that stops replenishing looks
                // exactly like a node that has enough peers from the outside. It is
                // logged at info because release builds enable the `max_level_info`
                // tracing feature, which compiles debug records out entirely: a
                // debug record here cannot be read on a deployed node, whatever the
                // configured filter says. One record per
                // `crawl_new_peer_interval` bounds the volume.
                info!(
                    active_outbound_connections,
                    peerset_target_size,
                    outbound_connection_limit,
                    remaining_replenishment_demand = replenishment_demand,
                    "timer-based crawl finished"
                );
            }

            // Fatal errors and shutdowns
            Err(error) => {
                info!(?error, "crawler task exiting due to an error");
                return Err(error);
            }
        }

        // Security: Let other tasks run after each crawler action is processed.
        //
        // Avoids remote peers starving other Zebra tasks using outbound connection errors.
        tokio::task::yield_now().await;
    }
}

/// Try to get more peers using `candidates`, then queue a connection attempt
/// using `demand_tx` if the crawl discovers peers.
#[instrument(skip(candidates, demand_tx))]
async fn crawl<S>(
    candidates: Arc<futures::lock::Mutex<CandidateSet<S>>>,
    mut demand_tx: futures::channel::mpsc::Sender<MorePeers>,
) -> Result<(), BoxError>
where
    S: Service<Request, Response = Response, Error = BoxError> + Send + Sync + 'static,
    S::Future: Send + 'static,
{
    // update() has timeouts, and briefly holds the address book
    // lock, so it shouldn't hang.
    // Try to get new peers, holding the lock for as short a time as possible.
    let result = {
        let result = candidates.lock().await.update().await;
        std::mem::drop(candidates);
        result
    };
    let more_peers = match result {
        Ok(more_peers) => more_peers,
        Err(e) => {
            info!(
                ?e,
                "candidate set returned an error, is Zakura shutting down?"
            );
            return Err(e);
        }
    };

    // Queue a connection attempt for discovered peers.
    //
    // # Security
    //
    // Update attempts are rate-limited by the candidate set, and we only try
    // peers if there was actually an update.
    if let Some(more_peers) = more_peers {
        if let Err(send_error) = demand_tx.try_send(more_peers) {
            if send_error.is_disconnected() {
                // Zakura's peer set is shutting down.
                return Err(send_error.into());
            }
        }
    }

    Ok(())
}

/// Outcome of an outbound dial for crawler replenishment bookkeeping.
enum DialOutcome {
    /// The handshake succeeded and the peer was sent to the peer set.
    Connected,
    /// The handshake failed.
    ///
    /// `demand_restored` is true only when `MorePeers` was successfully
    /// re-queued on the demand channel.
    Failed { demand_restored: bool },
}

/// Try to restore demand after a failed dial.
///
/// Returns whether `MorePeers` was queued. A full channel returns `Ok(false)`
/// so callers do not credit replenishment budget without a later decrement.
fn try_restore_demand_after_failed_dial(
    demand_tx: &mut futures::channel::mpsc::Sender<MorePeers>,
    force_failed_dial_demand_restore_full: &AtomicBool,
) -> Result<bool, BoxError> {
    // Tests can force a full-channel outcome without racing the crawler select.
    if force_failed_dial_demand_restore_full.load(Ordering::Relaxed) {
        return Ok(false);
    }

    match demand_tx.try_send(MorePeers) {
        Ok(()) => Ok(true),
        Err(send_error) if send_error.is_disconnected() => Err(send_error.into()),
        Err(_) => Ok(false),
    }
}

/// Try to connect to `candidate` using `outbound_connector`.
/// Uses `outbound_connection_tracker` to track the active connection count.
///
/// On success, sends peers to `peerset_tx`.
/// On failure, marks the peer as failed in the address book,
/// then re-adds demand to `demand_tx` when the channel has capacity.
#[allow(clippy::too_many_arguments)]
#[instrument(skip(
    candidate,
    outbound_connector,
    outbound_connection_tracker,
    outbound_connections,
    peerset_tx,
    address_book_updater,
    demand_tx,
    expose_peer_addresses,
    force_failed_dial_demand_restore_full,
), fields(peer = %candidate.addr.addr_label(expose_peer_addresses)))]
async fn dial<C>(
    candidate: MetaAddr,
    mut outbound_connector: C,
    outbound_connection_tracker: ConnectionTracker,
    outbound_connections: usize,
    mut peerset_tx: futures::channel::mpsc::Sender<DiscoveredPeer>,
    address_book_updater: tokio::sync::mpsc::Sender<MetaAddrChange>,
    mut demand_tx: futures::channel::mpsc::Sender<MorePeers>,
    expose_peer_addresses: bool,
    force_failed_dial_demand_restore_full: Arc<AtomicBool>,
) -> Result<DialOutcome, BoxError>
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
    // If Zebra only has a few connections, we log connection failures at info level,
    // so users can diagnose and fix the problem. This defines the threshold for info logs.
    const MAX_CONNECTIONS_FOR_INFO_LOG: usize = 5;

    // # Correctness
    //
    // To avoid hangs, the dialer must only await:
    // - functions that return immediately, or
    // - functions that have a reasonable timeout

    let addr_label = candidate.addr.addr_label(expose_peer_addresses);
    debug!(peer = %addr_label, "attempting outbound connection in response to demand");

    // the connector is always ready, so this can't hang
    let outbound_connector = outbound_connector.ready().await?;

    let req = OutboundConnectorRequest {
        addr: candidate.addr,
        connection_tracker: outbound_connection_tracker,
    };

    // the handshake has timeouts, so it shouldn't hang
    let handshake_result = outbound_connector.call(req).map(Into::into).await;

    match handshake_result {
        Ok((address, client)) => {
            debug!(peer = %addr_label, "successfully dialed new peer");

            // The connection limit makes sure this send doesn't block.
            peerset_tx.send((address, client)).await?;

            Ok(DialOutcome::Connected)
        }
        // The connection was never opened, or it failed the handshake and was dropped.
        Err(error) => {
            let neutral_disconnect = error
                .downcast_ref::<peer::HandshakeError>()
                .is_some_and(peer::HandshakeError::is_neutral_disconnect);

            // Silence verbose info logs in production, but keep logs if the number of connections is low.
            // Also silence them completely in tests.
            if outbound_connections <= MAX_CONNECTIONS_FOR_INFO_LOG && !cfg!(test) {
                info!(?error, peer = %addr_label, "failed to make outbound connection to peer");
            } else {
                debug!(?error, peer = %addr_label, "failed to make outbound connection to peer");
            }

            if !neutral_disconnect {
                report_failed(
                    address_book_updater.clone(),
                    candidate,
                    expose_peer_addresses,
                )
                .await;
            }

            // The demand signal that was taken out of the queue to attempt to connect to the
            // failed candidate never turned into a connection, so add it back when possible.
            //
            // # Security
            //
            // Handshake failures are rate-limited by peer attempt timeouts.
            let demand_restored = try_restore_demand_after_failed_dial(
                &mut demand_tx,
                &force_failed_dial_demand_restore_full,
            )?;

            Ok(DialOutcome::Failed { demand_restored })
        }
    }
}

/// Mark `addr` as a failed peer to `address_book_updater`.
#[instrument(
    skip(address_book_updater, addr, expose_peer_addresses),
    fields(peer = %addr.addr.addr_label(expose_peer_addresses))
)]
async fn report_failed(
    address_book_updater: tokio::sync::mpsc::Sender<MetaAddrChange>,
    addr: MetaAddr,
    expose_peer_addresses: bool,
) {
    // The connection info is the same as what's already in the address book.
    let addr = MetaAddr::new_errored(addr.addr, None);

    // Ignore send errors on Zebra shutdown.
    let _ = address_book_updater.send(addr).await;
}
