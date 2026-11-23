//! Abstractions that represent "the rest of the network".
//!
//! # Implementation
//!
//! The [`PeerSet`] implementation is adapted from the one in [tower::Balance][tower-balance].
//!
//! As described in Tower's documentation, it:
//!
//! > Distributes requests across inner services using the [Power of Two Choices][p2c].
//! >
//! > As described in the [Finagle Guide][finagle]:
//! >
//! > > The algorithm randomly picks two services from the set of ready endpoints and
//! > > selects the least loaded of the two. By repeatedly using this strategy, we can
//! > > expect a manageable upper bound on the maximum load of any server.
//! > >
//! > > The maximum load variance between any two servers is bound by `ln(ln(n))` where
//! > > `n` is the number of servers in the cluster.
//!
//! The Power of Two Choices should work well for many network requests, but not all of them.
//! Some requests should only be made to a subset of connected peers.
//! For example, a request for a particular inventory item
//! should be made to a peer that has recently advertised that inventory hash.
//! Other requests require broadcasts, such as transaction diffusion.
//!
//! Implementing this specialized routing logic inside the `PeerSet` -- so that
//! it continues to abstract away "the rest of the network" into one endpoint --
//! is not a problem, as the `PeerSet` can simply maintain more information on
//! its peers and route requests appropriately. However, there is a problem with
//! maintaining accurate backpressure information, because the `Service` trait
//! requires that service readiness is independent of the data in the request.
//!
//! For this reason, in the future, this code will probably be refactored to
//! address this backpressure mismatch. One possibility is to refactor the code
//! so that one entity holds and maintains the peer set and metadata on the
//! peers, and each "backpressure category" of request is assigned to different
//! `Service` impls with specialized `poll_ready()` implementations. Another
//! less-elegant solution (which might be useful as an intermediate step for the
//! inventory case) is to provide a way to borrow a particular backing service,
//! say by address.
//!
//! [finagle]: https://twitter.github.io/finagle/guide/Clients.html#power-of-two-choices-p2c-least-loaded
//! [p2c]: http://www.eecs.harvard.edu/~michaelm/postscripts/handbook2001.pdf
//! [tower-balance]: https://github.com/tower-rs/tower/tree/master/tower/src/balance
//!
//! # Behavior During Network Upgrades
//!
//! [ZIP-201] specifies peer behavior during network upgrades:
//!
//! > With scheduled network upgrades, at the activation height, nodes on each consensus branch
//! > should disconnect from nodes on other consensus branches and only accept new incoming
//! > connections from nodes on the same consensus branch.
//!
//! Zebra handles this with the help of [`MinimumPeerVersion`], which determines the minimum peer
//! protocol version to accept based on the current best chain tip height. The minimum version is
//! therefore automatically increased when the block height reaches a network upgrade's activation
//! height. The helper type is then used to:
//!
//! - cancel handshakes to outdated peers, in `handshake::negotiate_version`
//! - cancel requests to and disconnect from peers that have become outdated, in
//!   [`PeerSet::push_unready`]
//! - disconnect from peers that have just responded and became outdated, in
//!   [`PeerSet::poll_unready`]
//! - disconnect from idle peers that have become outdated, in
//!   [`PeerSet::disconnect_from_outdated_peers`]
//!
//! ## Network Coalescence
//!
//! [ZIP-201] also specifies how Zcashd behaves [leading up to a activation
//! height][1]. Since Zcashd limits the number of connections to at most eight
//! peers, it will gradually migrate its connections to up-to-date peers as it
//! approaches the activation height.
//!
//! The motivation for this behavior is to avoid an abrupt partitioning the network, which can lead
//! to isolated peers and increases the chance of an eclipse attack on some peers of the network.
//!
//! Zebra does not gradually migrate its peers as it approaches an activation height. This is
//! because Zebra by default can connect to up to 75 peers, as can be seen in [`Config::default`].
//! Since this is a lot larger than the 8 peers Zcashd connects to, an eclipse attack becomes a lot
//! more costly to execute, and the probability of an abrupt network partition that isolates peers
//! is lower.
//!
//! Even if a Zebra node is manually configured to connect to a smaller number
//! of peers, the [`AddressBook`][2] is configured to hold a large number of
//! peer addresses ([`MAX_ADDRS_IN_ADDRESS_BOOK`][3]). Since the address book
//! prioritizes addresses it trusts (like those that it has successfully
//! connected to before), the node should be able to recover and rejoin the
//! network by itself, as long as the address book is populated with enough
//! entries.
//!
//! [1]: https://zips.z.cash/zip-0201#network-coalescence
//! [2]: crate::AddressBook
//! [3]: crate::constants::MAX_ADDRS_IN_ADDRESS_BOOK
//! [ZIP-201]: https://zips.z.cash/zip-0201

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
    convert,
    fmt::Debug,
    marker::PhantomData,
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::{
    channel::{mpsc, oneshot},
    future::{FutureExt, TryFutureExt},
    prelude::*,
    stream::FuturesUnordered,
    task::noop_waker,
};
use itertools::Itertools;
use tokio::{
    sync::{broadcast, mpsc as tokio_mpsc, watch},
    task::JoinHandle,
    time::{sleep_until, Sleep},
};
use tower::{
    discover::{Change, Discover},
    load::Load,
    Service,
};

use zakura_chain::{block, chain_tip::ChainTip, parameters::Network};

use crate::{
    address_book::AddressMetrics,
    constants::MIN_PEER_SET_LOG_INTERVAL,
    peer::{
        block_gossip::{AdvertFanout, BlockGossipPeer},
        LoadTrackedClient, MinimumPeerVersion,
    },
    peer_registry::ESTIMATE_WINDOW,
    peer_set::{
        legacy_peer_trace::{
            BlockRequestRoute, BlockSourceChoice, LegacyPeerTrace, PeerTraceContext,
            SecondRequestChoice,
        },
        second_request::{
            arrival_duration, is_stalled, is_usable_block, median_u128, race_block_request,
            sample_rate_bps, second_request_reason, HeldSecondRequest, SecondRequestReason,
            SecondResponse,
        },
        stall_tracker::FindResponseStallTracker,
        unready_service::{Error as UnreadyError, UnreadyService},
        InventoryChange, InventoryRegistry,
    },
    protocol::{
        external::{canonical_ip, InventoryHash},
        internal::{Request, Response},
    },
    BannedIps, BoxError, Config, PeerError, PeerSocketAddr, PeerSource, SharedPeerError,
};

#[cfg(test)]
mod tests;

/// How long a transaction request waits for a busy peer that advertised it, before it is routed
/// to any ready peer as if no peer had advertised it.
///
/// Shorter than the dual stack's 3 s legacy attempt, so that the fallback still has time to run.
const TX_ADVERTISER_WAIT: Duration = Duration::from_secs(2);

/// The most transaction requests held for busy advertisers at once. Further requests are routed
/// without waiting.
const MAX_HELD_TX_REQUESTS: usize = 64;

/// A single-transaction request waiting for a busy peer that advertised the transaction.
struct HeldTxRequest {
    req: Request,
    hash: InventoryHash,
    /// When the request stops waiting for an advertiser.
    deadline: tokio::time::Instant,
    /// The caller's span, which routing runs in.
    span: tracing::Span,
    /// Receives the routed request's response future.
    reply: oneshot::Sender<futures::future::BoxFuture<'static, Result<Response, BoxError>>>,
}

/// A signal sent by the [`PeerSet`] when it has no ready peers, and gets a request from Zebra.
///
/// In response to this signal, the crawler tries to open more peer connections.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct MorePeers;

/// A signal sent by the [`PeerSet`] to cancel a [`Client`][1]'s current request
/// or response.
///
/// When it receives this signal, the [`Client`][1] stops processing and exits.
///
/// [1]: crate::peer::Client
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct CancelClientWork;

/// Classification of a `FindBlocks`/`FindHeaders` response, sent from a
/// response-wrapping future to [`PeerSet::poll_ready`] via an mpsc channel so
/// the stall tracker can be updated and the peer disconnected if needed.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum StallOutcome {
    Stall,
    Clear,
}

fn classify_find_response<E>(result: &Result<Response, E>) -> Option<StallOutcome> {
    match result {
        Ok(Response::BlockHashes(hashes)) if hashes.is_empty() => Some(StallOutcome::Stall),
        Ok(Response::BlockHashes(_)) => Some(StallOutcome::Clear),
        Ok(Response::BlockHeaders(headers)) if headers.is_empty() => Some(StallOutcome::Stall),
        Ok(Response::BlockHeaders(_)) => Some(StallOutcome::Clear),
        Ok(_) => None,
        Err(_) => Some(StallOutcome::Stall),
    }
}

/// Outcome of a routed single-block request, fed back into [`PeerSet`] without cross-thread
/// locks so transfer rates and announced-block misses stay single-owner.
enum BlockFeedback {
    Delivered {
        peer: PeerSocketAddr,
        connection_id: u64,
        idle_at_dispatch: bool,
        elapsed: Duration,
        block_bytes: u64,
        at: tokio::time::Instant,
    },
    Missed {
        peer: PeerSocketAddr,
        connection_id: u64,
        at: tokio::time::Instant,
    },
}

/// Fleet-wide median RTT and incoming block rate across currently measured ready peers.
#[derive(Clone, Copy, Debug, Default)]
struct ReadyMedians {
    rtt: Option<Duration>,
    rate_bps: Option<u64>,
}

/// A [`tower::Service`] that abstractly represents "the rest of the network".
///
/// # Security
///
/// The `Discover::Key` must be the transient remote address of each peer. This
/// address may only be valid for the duration of a single connection. (For
/// example, inbound connections have an ephemeral remote port, and proxy
/// connections have an ephemeral local or proxy port.)
///
/// Otherwise, malicious peers could interfere with other peers' `PeerSet` state.
pub struct PeerSet<D, C>
where
    D: Discover<Key = PeerSocketAddr, Service = LoadTrackedClient> + Unpin,
    D::Error: Into<BoxError>,
    C: ChainTip,
{
    // Peer Tracking: New Peers
    //
    /// Provides new and deleted peer [`Change`]s to the peer set,
    /// via the [`Discover`] trait implementation.
    discover: D,

    /// A channel that asks the peer crawler task to connect to more peers.
    demand_signal: mpsc::Sender<MorePeers>,

    /// A shared list of banned IP addresses.
    bans: BannedIps,

    /// Tracks peers returning empty `FindBlocks`/`FindHeaders` responses.
    /// Mutated only from [`Self::poll_ready`] via [`Self::stall_event_rx`].
    find_response_stalls: FindResponseStallTracker,

    /// Receives stall/clear events from tracked routing futures in
    /// [`Self::route_p2c`]. The channel keeps the tracker single-owner (no
    /// `Mutex`) and confines mutation to `poll_ready`, where the peer set can
    /// call [`Self::remove`] directly.
    stall_event_rx: tokio_mpsc::UnboundedReceiver<(PeerSocketAddr, StallOutcome)>,

    /// Producer clones handed to each tracked request's response wrapper.
    stall_event_tx: tokio_mpsc::UnboundedSender<(PeerSocketAddr, StallOutcome)>,

    // Peer Tracking: Ready Peers
    //
    /// Connected peers that are ready to receive requests from Zebra,
    /// or send requests to Zebra.
    ready_services: HashMap<D::Key, D::Service>,

    // Request Routing
    //
    /// Stores gossiped inventory hashes from connected peers.
    ///
    /// Used to route inventory requests to peers that are likely to have it.
    inventory_registry: InventoryRegistry,

    /// Senders for the current ready and busy connection generations.
    block_gossip_peers: HashMap<D::Key, BlockGossipPeer>,
    /// Publishes `block_gossip_peers` to block advertisers, which do not wait for a ready peer.
    block_gossip_peers_tx: watch::Sender<Vec<BlockGossipPeer>>,
    /// Whether `block_gossip_peers` changed since it was last published.
    block_gossip_peers_changed: bool,
    /// Selects each block broadcast's initial group and delay.
    block_gossip_fanout: AdvertFanout,

    /// Inbound peer IPs that must always receive block inventory broadcasts.
    block_gossip_peer_ips: HashSet<IpAddr>,

    /// Structured attribution for legacy peer discovery and block requests.
    legacy_peer_trace: LegacyPeerTrace,

    // Peer Tracking: Busy Peers
    //
    /// Connected peers that are handling a Zebra request,
    /// or Zebra is handling one of their requests.
    unready_services: FuturesUnordered<UnreadyService<D::Key, D::Service, Request>>,

    /// Channels used to cancel the request that an unready service is doing.
    cancel_handles: HashMap<D::Key, oneshot::Sender<CancelClientWork>>,

    /// Transaction requests waiting for a busy advertiser, oldest first. See
    /// [`Self::route_held_requests`].
    held_tx_requests: VecDeque<HeldTxRequest>,

    /// Single shared timer waking the owner at the earliest deadline across held block hedges
    /// and held transaction requests.
    held_timer: Option<Pin<Box<Sleep>>>,

    // Peer Validation
    //
    /// An endpoint to see the minimum peer protocol version in real time.
    ///
    /// The minimum version depends on the block height, and [`MinimumPeerVersion`] listens for
    /// height changes and determines the correct minimum version.
    minimum_peer_version: MinimumPeerVersion<C>,

    /// The configured limit for inbound and outbound connections.
    ///
    /// The peer set panics if this size is exceeded.
    /// If that happens, our connection limit code has a bug.
    peerset_total_connection_limit: usize,

    // Background Tasks
    //
    /// Channel for passing ownership of tokio JoinHandles from PeerSet's background tasks
    ///
    /// The join handles passed into the PeerSet are used populate the `guards` member
    handle_rx: tokio::sync::oneshot::Receiver<Vec<JoinHandle<Result<(), BoxError>>>>,

    /// Unordered set of handles to background tasks associated with the `PeerSet`
    ///
    /// These guards are checked for errors as part of `poll_ready` which lets
    /// the `PeerSet` propagate errors from background tasks back to the user
    guards: futures::stream::FuturesUnordered<JoinHandle<Result<(), BoxError>>>,

    // Metrics and Logging
    //
    /// Address book metrics watch channel.
    ///
    /// Used for logging diagnostics.
    address_metrics: watch::Receiver<AddressMetrics>,

    /// The last time we logged a message about the peer set size
    last_peer_log: Option<Instant>,

    /// The configured maximum number of peers that can be in the
    /// peer set per IP, defaults to [`crate::constants::DEFAULT_MAX_CONNS_PER_IP`]
    max_conns_per_ip: usize,

    /// Whether peer address labels in logs are unredacted.
    expose_peer_addresses: bool,

    /// Whether announced blocks are requested from the announcer expected to answer sooner,
    /// rather than the less loaded one, and also from a second announcer expected to deliver
    /// them sooner. See [`crate::config::BlockDownloadScoreAndHedgeConfig`].
    block_download_score_and_hedge: bool,

    /// Internal second block requests held while their primary requests are outstanding.
    held_second_requests: Vec<HeldSecondRequest>,

    /// Latest observed incoming block transfer rate (bytes/sec) per peer connection, valid for
    /// [`ESTIMATE_WINDOW`].
    peer_rates: HashMap<PeerSocketAddr, (u64, tokio::time::Instant, u64)>,

    /// Expiry instant until which an announcer that failed to deliver an announced block is
    /// passed over by scored routing, keyed by peer address and connection generation.
    announced_block_misses: HashMap<PeerSocketAddr, (u64, tokio::time::Instant)>,

    /// Decoded byte size of the most recently delivered block, used with per-peer transfer rates
    /// to estimate transfer time when legacy `inv` messages omit block size.
    last_block_bytes: u64,

    /// Receives delivery and miss outcomes from routed single-block request futures.
    block_feedback_rx: tokio_mpsc::UnboundedReceiver<BlockFeedback>,

    /// Producer clones handed to routed single-block request wrappers.
    block_feedback_tx: tokio_mpsc::UnboundedSender<BlockFeedback>,

    /// The network of this peer set.
    network: Network,
}

impl<D, C> Drop for PeerSet<D, C>
where
    D: Discover<Key = PeerSocketAddr, Service = LoadTrackedClient> + Unpin,
    D::Error: Into<BoxError>,
    C: ChainTip,
{
    fn drop(&mut self) {
        // We don't have access to the current task (if any), so we just drop everything we can.
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        self.shut_down_tasks_and_channels(&mut cx);
    }
}

impl<D, C> PeerSet<D, C>
where
    D: Discover<Key = PeerSocketAddr, Service = LoadTrackedClient> + Unpin,
    D::Error: Into<BoxError>,
    C: ChainTip,
{
    #[allow(clippy::too_many_arguments)]
    /// Construct a peerset which uses `discover` to manage peer connections.
    ///
    /// Arguments:
    /// - `config`: configures the peer set connection limit;
    /// - `block_gossip_peer_ips`: inbound peer IPs that must always receive block inventory broadcasts.
    /// - `discover`: handles peer connects and disconnects;
    /// - `demand_signal`: requests more peers when all peers are busy (unready);
    /// - `handle_rx`: receives background task handles,
    ///   monitors them to make sure they're still running,
    ///   and shuts down all the tasks as soon as one task exits;
    /// - `inv_stream`: receives inventory changes from peers,
    ///   allowing the peer set to direct inventory requests;
    /// - `bans`: a map of banned IP addresses that should be dropped;
    /// - `address_book`: when peer set is busy, it logs address book diagnostics.
    /// - `minimum_peer_version`: endpoint to see the minimum peer protocol version in real time.
    /// - `max_conns_per_ip`: configured maximum number of peers that can be in the
    ///   peer set per IP, defaults to the config value or to
    ///   [`crate::constants::DEFAULT_MAX_CONNS_PER_IP`].
    pub fn new(
        config: &Config,
        block_gossip_peer_ips: Vec<IpAddr>,
        discover: D,
        demand_signal: mpsc::Sender<MorePeers>,
        handle_rx: tokio::sync::oneshot::Receiver<Vec<JoinHandle<Result<(), BoxError>>>>,
        inv_stream: broadcast::Receiver<InventoryChange>,
        bans: BannedIps,
        address_metrics: watch::Receiver<AddressMetrics>,
        minimum_peer_version: MinimumPeerVersion<C>,
        max_conns_per_ip: Option<usize>,
    ) -> Self {
        let (stall_event_tx, stall_event_rx) = tokio_mpsc::unbounded_channel();
        let (block_feedback_tx, block_feedback_rx) = tokio_mpsc::unbounded_channel();
        Self {
            // New peers
            discover,
            demand_signal,
            // Banned peers
            bans,

            // Stall tracking
            find_response_stalls: FindResponseStallTracker::new(),
            stall_event_rx,
            stall_event_tx,

            // Ready peers
            ready_services: HashMap::new(),
            // Request Routing
            inventory_registry: InventoryRegistry::new(inv_stream, config.expose_peer_addresses),
            block_gossip_peers: HashMap::new(),
            block_gossip_peers_tx: watch::channel(Vec::new()).0,
            block_gossip_peers_changed: false,
            block_gossip_fanout: AdvertFanout::from_config(config),
            block_gossip_peer_ips: block_gossip_peer_ips.into_iter().collect(),
            legacy_peer_trace: LegacyPeerTrace::new(
                config.zakura.trace_dir.clone(),
                config.expose_peer_addresses,
            ),

            // Busy peers
            unready_services: FuturesUnordered::new(),
            cancel_handles: HashMap::new(),
            held_tx_requests: VecDeque::new(),
            held_timer: None,

            // Peer validation
            minimum_peer_version,
            peerset_total_connection_limit: config.peerset_total_connection_limit(),

            // Background tasks
            handle_rx,
            guards: futures::stream::FuturesUnordered::new(),

            // Metrics
            last_peer_log: None,
            address_metrics,

            max_conns_per_ip: max_conns_per_ip.unwrap_or(config.max_connections_per_ip),
            expose_peer_addresses: config.expose_peer_addresses,
            block_download_score_and_hedge: config.block_download_score_and_hedge.enabled,
            held_second_requests: Vec::new(),
            peer_rates: HashMap::new(),
            announced_block_misses: HashMap::new(),
            last_block_bytes: 0,
            block_feedback_rx,
            block_feedback_tx,

            network: config.network.clone(),
        }
    }

    /// Check background task handles to make sure they're still running.
    ///
    /// Never returns `Ok`.
    ///
    /// If any background task exits, shuts down all other background tasks,
    /// and returns an error. Otherwise, returns `Pending`, and registers a wakeup for
    /// receiving the background tasks, or the background tasks exiting.
    fn poll_background_errors(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        futures::ready!(self.receive_tasks_if_needed(cx))?;

        // Return Pending if all background tasks are still running.
        match futures::ready!(Pin::new(&mut self.guards).poll_next(cx)) {
            Some(res) => {
                info!(
                    background_tasks = %self.guards.len(),
                    "a peer set background task exited, shutting down other peer set tasks"
                );

                self.shut_down_tasks_and_channels(cx);

                // Flatten the join result and inner result, and return any errors.
                res.map_err(Into::into)
                    // TODO: replace with Result::flatten when it stabilises (#70142)
                    .and_then(convert::identity)?;

                // Turn Ok() task exits into errors.
                Poll::Ready(Err("a peer set background task exited".into()))
            }

            None => {
                self.shut_down_tasks_and_channels(cx);
                Poll::Ready(Err("all peer set background tasks have exited".into()))
            }
        }
    }

    /// Receive background tasks, if they've been sent on the channel, but not consumed yet.
    ///
    /// Returns a result representing the current task state, or `Poll::Pending` if the background
    /// tasks should be polled again to check their state.
    fn receive_tasks_if_needed(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        if self.guards.is_empty() {
            // Return Pending if the tasks have not been sent yet.
            let handles = futures::ready!(Pin::new(&mut self.handle_rx).poll(cx));

            match handles {
                // The tasks have been sent, but not consumed yet.
                Ok(handles) => {
                    // Currently, the peer set treats an empty background task set as an error.
                    //
                    // TODO: refactor `handle_rx` and `guards` into an enum
                    //       for the background task state: Waiting/Running/Shutdown.
                    assert!(
                        !handles.is_empty(),
                        "the peer set requires at least one background task"
                    );

                    self.guards.extend(handles);

                    Poll::Ready(Ok(()))
                }

                // The sender was dropped without sending the tasks.
                Err(_) => Poll::Ready(Err(
                    "sender did not send peer background tasks before it was dropped".into(),
                )),
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    /// Shut down:
    /// - services by dropping the service lists
    /// - background tasks via their join handles or cancel handles
    /// - channels by closing the channel
    fn shut_down_tasks_and_channels(&mut self, cx: &mut Context<'_>) {
        // Drop services and cancel their background tasks.
        self.ready_services = HashMap::new();
        self.block_gossip_peers.clear();
        self.block_gossip_peers_tx.send_replace(Vec::new());

        for (_peer_key, handle) in self.cancel_handles.drain() {
            let _ = handle.send(CancelClientWork);
        }
        self.unready_services = FuturesUnordered::new();

        // Dropping held requests fails their waiting callers.
        self.held_second_requests.clear();
        self.held_tx_requests.clear();
        self.held_timer = None;

        // Close the MorePeers channel for all senders,
        // so we don't add more peers to a shut down peer set.
        self.demand_signal.close_channel();

        // Shut down background tasks, ignoring pending polls.
        self.handle_rx.close();
        let _ = self.receive_tasks_if_needed(cx);
        for guard in self.guards.iter() {
            guard.abort();
        }
    }

    /// Checks for newly ready, disconnects from outdated peers, and polls ready peer errors.
    fn poll_peers(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        // Check for newly ready peers, including newly added peers (which are added as unready).
        // So it needs to run after `poll_discover()`. Registers a wakeup if there are any unready
        // peers.
        //
        // Each connected peer should become ready within a few minutes, or timeout, close the
        // connection, and release its connection slot.
        //
        // TODO: drop peers that overload us with inbound messages and never become ready (#7822)
        let _poll_pending_or_ready: Poll<Option<()>> = self.poll_unready(cx)?;

        // Cleanup

        // Only checks the versions of ready peers, so it needs to run after `poll_unready()`.
        self.disconnect_from_outdated_peers();

        // Check for failures in ready peers, removing newly errored or disconnected peers.
        // So it needs to run after `poll_unready()`.
        self.poll_ready_peer_errors(cx).map(Ok)
    }

    /// Check busy peer services for request completion or errors.
    ///
    /// Move newly ready services to the ready list if they are for peers with supported protocol
    /// versions, otherwise they are dropped. Also drop failed services.
    ///
    /// Never returns an error.
    ///
    /// Returns `Ok(Some(())` if at least one peer became ready, `Poll::Pending` if there are
    /// unready peers, but none became ready, and `Ok(None)` if the unready peers were empty.
    ///
    /// If there are any remaining unready peers, registers a wakeup for the next time one becomes
    /// ready. If there are no unready peers, doesn't register any wakeups. (Since wakeups come
    /// from peers, there needs to be at least one peer to register a wakeup.)
    fn poll_unready(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<()>, BoxError>> {
        let mut result = Poll::Pending;

        // # Correctness
        //
        // `poll_next()` must always be called, because `self.unready_services` could have been
        // empty before the call to `self.poll_ready()`.
        //
        // > When new futures are added, `poll_next` must be called in order to begin receiving
        // > wake-ups for new futures.
        //
        // <https://docs.rs/futures/latest/futures/stream/futures_unordered/struct.FuturesUnordered.html>
        //
        // Returns Pending if we've finished processing the unready service changes,
        // but there are still some unready services.
        loop {
            // No ready peers left, but there are some unready peers pending.
            let Poll::Ready(ready_peer) = Pin::new(&mut self.unready_services).poll_next(cx) else {
                break;
            };

            match ready_peer {
                // No unready peers in the list.
                None => {
                    // If we've finished processing the unready service changes, and there are no
                    // unready services left, it doesn't make sense to return Pending, because
                    // their stream is terminated. But when we add more unready peers and call
                    // `poll_next()`, its termination status will be reset, and it will receive
                    // wakeups again.
                    if result.is_pending() {
                        result = Poll::Ready(Ok(None));
                    }

                    break;
                }

                // Unready -> Ready
                Some(Ok((key, svc))) => {
                    trace!(
                        peer = %key.addr_label(self.expose_peer_addresses),
                        "service became ready"
                    );

                    if self.bans.contains(canonical_ip(key.ip())) {
                        warn!(
                            peer = %key.addr_label(self.expose_peer_addresses),
                            "service is banned, dropping service"
                        );
                        std::mem::drop(svc);
                        let cancel = self.cancel_handles.remove(&key);
                        debug_assert!(
                            cancel.is_some(),
                            "missing cancel handle for banned unready peer"
                        );
                        continue;
                    }

                    self.push_ready(true, key, svc);

                    // Return Ok if at least one peer became ready.
                    result = Poll::Ready(Ok(Some(())));
                }

                // Unready -> Canceled
                Some(Err((key, UnreadyError::Canceled))) => {
                    // A service be canceled because we've connected to the same service twice.
                    // In that case, there is a cancel handle for the peer address,
                    // but it belongs to the service for the newer connection.
                    trace!(
                        peer = %key.addr_label(self.expose_peer_addresses),
                        duplicate_connection = self.cancel_handles.contains_key(&key),
                        "service was canceled, dropping service"
                    );
                }
                Some(Err((key, UnreadyError::CancelHandleDropped(_)))) => {
                    // Similarly, services with dropped cancel handes can have duplicates.
                    trace!(
                        peer = %key.addr_label(self.expose_peer_addresses),
                        duplicate_connection = self.cancel_handles.contains_key(&key),
                        "cancel handle was dropped, dropping service"
                    );
                }

                // Unready -> Errored
                Some(Err((key, UnreadyError::Inner(error)))) => {
                    debug!(%error, "service failed while unready, dropping service");

                    let cancel = self.cancel_handles.remove(&key);
                    assert!(cancel.is_some(), "missing cancel handle");
                }
            }
        }

        result
    }

    /// Checks previously ready peer services for errors.
    ///
    /// The only way these peer `Client`s can become unready is when we send them a request,
    /// because the peer set has exclusive access to send requests to each peer. (If an inbound
    /// request is in progress, it will be handled, then our request will be sent by the connection
    /// task.)
    ///
    /// Returns `Poll::Ready` if there are some ready peers, and `Poll::Pending` if there are no
    /// ready peers. Registers a wakeup if any peer has failed due to a disconnection, hang, or protocol error.
    ///
    /// # Panics
    ///
    /// If any peers somehow became unready without being sent a request. This indicates a bug in the peer set, where requests
    /// are sent to peers without putting them in `unready_peers`.
    fn poll_ready_peer_errors(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut previous = HashMap::new();
        std::mem::swap(&mut previous, &mut self.ready_services);

        // TODO: consider only checking some peers each poll (for performance reasons),
        //       but make sure we eventually check all of them.
        for (key, mut svc) in previous.drain() {
            let Poll::Ready(peer_readiness) = Pin::new(&mut svc).poll_ready(cx) else {
                unreachable!(
                    "unexpected unready peer: peers must be put into the unready_peers list \
                     after sending them a request"
                );
            };

            match peer_readiness {
                // Still ready, add it back to the list.
                Ok(()) => {
                    if self.bans.contains(canonical_ip(key.ip())) {
                        debug!(
                            peer = %key.addr_label(self.expose_peer_addresses),
                            "service ip is banned, dropping service"
                        );
                        std::mem::drop(svc);
                        continue;
                    }

                    self.push_ready(false, key, svc)
                }

                // Ready -> Errored
                Err(error) => {
                    debug!(%error, "service failed while ready, dropping service");

                    // Ready services can just be dropped, they don't need any cleanup.
                    std::mem::drop(svc);
                }
            }
        }

        if self.ready_services.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    /// Returns the number of peer connections Zebra already has with
    /// the provided IP address
    ///
    /// # Performance
    ///
    /// This method is `O(connected peers)`, so it should not be called from a loop
    /// that is already iterating through the peer set.
    fn num_peers_with_ip(&self, ip: IpAddr) -> usize {
        let ip = canonical_ip(ip);
        self.ready_services
            .keys()
            .chain(self.cancel_handles.keys())
            .filter(|addr| canonical_ip(addr.ip()) == ip)
            .count()
    }

    /// Returns `true` if Zebra is already connected to the IP and port in `addr`.
    fn has_peer_with_addr(&self, addr: PeerSocketAddr) -> bool {
        self.ready_services.contains_key(&addr) || self.cancel_handles.contains_key(&addr)
    }

    /// Drops the ready service for `key` if its connection has already failed, and returns
    /// whether it did. A busy service for `key` is left alone.
    ///
    /// The dropped connection's stall count is cleared, so a reconnecting peer starts fresh, as it
    /// does after a disconnect.
    ///
    /// A ready service is never pending: it only becomes unready when the peer set sends it a
    /// request, and unready services are tracked separately.
    fn drop_ready_service_if_failed(&mut self, key: &D::Key, cx: &mut Context<'_>) -> bool {
        let Some(svc) = self.ready_services.get_mut(key) else {
            return false;
        };

        match svc.poll_ready(cx) {
            Poll::Ready(Err(error)) => {
                debug!(
                    %error,
                    peer = %key.addr_label(self.expose_peer_addresses),
                    "ready service failed before the peer reconnected, dropping service"
                );
                self.ready_services.remove(key);
                self.find_response_stalls.clear(*key);
                true
            }
            Poll::Ready(Ok(())) => false,
            Poll::Pending => unreachable!(
                "unexpected unready peer: peers must be put into the unready_peers list \
                 after sending them a request"
            ),
        }
    }

    /// Processes the entire list of newly inserted or removed services.
    ///
    /// Puts inserted services in the unready list.
    /// Drops removed services, after cancelling any pending requests.
    ///
    /// If the peer connector channel is closed, returns an error.
    ///
    /// Otherwise, returns `Ok` if it discovered at least one peer, or `Poll::Pending` if it didn't
    /// discover any peers. Always registers a wakeup for new peers, even when it returns `Ok`.
    fn poll_discover(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        // Return pending if there are no peers in the list.
        let mut result = Poll::Pending;

        loop {
            // If we've emptied the list, finish looping, otherwise process the new peer.
            let Poll::Ready(discovered) = Pin::new(&mut self.discover).poll_discover(cx) else {
                break;
            };

            // If the change channel has a permanent error, return that error.
            let change = discovered
                .ok_or("discovery stream closed")?
                .map_err(Into::into)?;

            // Otherwise we have successfully processed a peer.
            result = Poll::Ready(Ok(()));

            // Process each change.
            match change {
                Change::Remove(key) => {
                    trace!(
                        peer = %key.addr_label(self.expose_peer_addresses),
                        "got Change::Remove from Discover"
                    );
                    self.remove(&key);
                }
                Change::Insert(key, svc) => {
                    // We add peers as unready, so that we:
                    // - always do the same checks on every ready peer, and
                    // - check for any errors that happened right after the handshake
                    trace!(
                        peer = %key.addr_label(self.expose_peer_addresses),
                        "got Change::Insert from Discover"
                    );

                    // # Security
                    //
                    // Drop the new peer if we are already connected to it.
                    // Preferring old connections avoids connection thrashing.
                    //
                    // A ready connection that has already closed is not a live duplicate: drop it
                    // instead, so a peer that reconnects before the next health sweep keeps its
                    // place in the set.
                    if self.has_peer_with_addr(key) && !self.drop_ready_service_if_failed(&key, cx)
                    {
                        std::mem::drop(svc);
                        continue;
                    }

                    // # Security
                    //
                    // drop the new peer if there are already `max_conns_per_ip` peers with
                    // the same IP address in the peer set.
                    if self.num_peers_with_ip(key.ip()) >= self.max_conns_per_ip {
                        std::mem::drop(svc);
                        continue;
                    }

                    self.push_unready(key, svc);
                }
            }
        }

        result
    }

    /// Checks if the minimum peer version has changed, and disconnects from outdated peers.
    fn disconnect_from_outdated_peers(&mut self) {
        if let Some(minimum_version) = self.minimum_peer_version.changed() {
            // It is ok to drop ready services, they don't need anything cancelled.
            self.ready_services
                .retain(|_address, peer| peer.remote_version() >= minimum_version);
        }
    }

    /// Takes a ready service by key.
    fn take_ready_service(&mut self, key: &D::Key) -> Option<D::Service> {
        if let Some(svc) = self.ready_services.remove(key) {
            assert!(
                !self.cancel_handles.contains_key(key),
                "cancel handles are only used for unready service work"
            );

            Some(svc)
        } else {
            None
        }
    }

    /// Drains pending stall/clear events from tracked routing futures and
    /// disconnects peers that have exceeded the stall threshold. The peer's
    /// TCP connection is closed when its service is dropped; address book and
    /// ban list are untouched, so the peer is free to reconnect.
    fn drain_stall_events(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some((addr, outcome))) = self.stall_event_rx.poll_recv(cx) {
            // A late event from a connection that already left the set must not
            // start a count that a reconnect at the same address would inherit.
            if !self.has_peer_with_addr(addr) {
                continue;
            }
            match outcome {
                StallOutcome::Stall => {
                    if self.find_response_stalls.record_stall(addr) {
                        info!(
                            peer = %addr.addr_label(self.expose_peer_addresses),
                            "dropping stalled peer: exceeded FindBlocks/FindHeaders stall threshold",
                        );
                        self.remove(&addr);
                    }
                }
                StallOutcome::Clear => self.find_response_stalls.clear(addr),
            }
        }
    }

    /// Remove the service corresponding to `key` from the peer set.
    ///
    /// Drops the service, cancelling any pending request or response to that peer.
    /// If the peer does not exist, does nothing.
    fn remove(&mut self, key: &D::Key) {
        self.find_response_stalls.clear(*key);

        if let Some(ready_service) = self.take_ready_service(key) {
            // A ready service has no work to cancel, so just drop it.
            std::mem::drop(ready_service);
        } else if let Some(handle) = self.cancel_handles.remove(key) {
            // Cancel the work, implicitly dropping the cancel handle.
            // The service future returns a `Canceled` error,
            // making `poll_unready` drop the service.
            let _ = handle.send(CancelClientWork);
        }
    }

    /// Adds a ready service to the ready list if it's for a peer with a supported version.
    /// If `was_unready` is true, also removes the peer's cancel handle.
    ///
    /// If the service is for a connection to an outdated peer, the service is dropped.
    fn push_ready(&mut self, was_unready: bool, key: D::Key, svc: D::Service) {
        self.insert_block_gossip_peer(key, &svc);
        let cancel = self.cancel_handles.remove(&key);
        assert_eq!(
            cancel.is_some(),
            was_unready,
            "missing or unexpected cancel handle"
        );

        if svc.remote_version() >= self.minimum_peer_version.current() {
            self.ready_services.insert(key, svc);
        } else {
            std::mem::drop(svc);
        }
    }

    /// Adds a busy service to the unready list if it's for a peer with a supported version,
    /// and adds a cancel handle for the service's current request.
    ///
    /// If the service is for a connection to an outdated peer, the request is cancelled and the
    /// service is dropped.
    fn push_unready(&mut self, key: D::Key, svc: D::Service) {
        self.insert_block_gossip_peer(key, &svc);
        let peer_version = svc.remote_version();
        let (tx, rx) = oneshot::channel();

        self.unready_services.push(UnreadyService {
            key: Some(key),
            service: Some(svc),
            cancel: rx,
            _req: PhantomData,
        });

        if peer_version >= self.minimum_peer_version.current() {
            self.cancel_handles.insert(key, tx);
        } else {
            // Cancel any request made to the service because it is using an outdated protocol
            // version.
            let _ = tx.send(CancelClientWork);
        }
    }

    /// Performs P2C on `self.ready_services` to randomly select a less-loaded ready service.
    ///
    /// For a `FindBlocks` or `FindHeaders` request (`is_find_request`), skips zcashd-compat
    /// sidecar peers whose handshake advertised no more blocks than this node has. A sidecar
    /// follows only this node, so it can only extend this node's chain if it started ahead, for
    /// example from restored chain state. Otherwise zcashd leaves a `getblocks` with nothing newer
    /// unanswered (and any `getheaders` during its initial block download), so the request would
    /// hold the sidecar's only connection until it times out, and the block `inv`s the sidecar
    /// follows would queue behind it.
    ///
    /// For a request for block bodies, chooses among the remaining peers with
    /// [`Self::select_block_body_peer`]. For a `FindBlocks` request, chooses with
    /// [`Self::select_block_hashes_peer`].
    fn select_ready_p2c_peer(&self, req: &Request) -> Option<D::Key> {
        let is_find_request = matches!(
            req,
            Request::FindBlocks { .. } | Request::FindHeaders { .. }
        );
        let candidates = self
            .ready_services
            .iter()
            .filter(|(_, service)| {
                !(is_find_request && self.is_sidecar_without_new_blocks(service))
            })
            .map(|(key, _)| *key)
            .collect();
        match req {
            Request::BlocksByHash(_) | Request::BlocksByHashFrom { .. } => {
                self.select_block_body_peer(&candidates)
            }
            Request::FindBlocks { .. } => self.select_block_hashes_peer(&candidates),
            _ => self.select_p2c_peer_from_list(&candidates),
        }
    }

    /// Performs P2C on `candidates` for a `FindBlocks` request.
    ///
    /// A `getblocks` reply advertises blocks the peer can serve, so a pruned peer stops before
    /// the first body it has deleted. While this node is syncing its locator is usually below a
    /// pruned peer's retained range, and the peer sends nothing back: the request then holds
    /// until an unrelated block `inv` or the request timeout. So prefer peers that may hold old
    /// blocks, as for block bodies. `getheaders` is unaffected, as pruned peers keep every header.
    fn select_block_hashes_peer(&self, candidates: &HashSet<D::Key>) -> Option<D::Key> {
        if self.is_near_network_tip() {
            return self.select_p2c_peer_from_list(candidates);
        }
        self.select_p2c_peer_from_list(&self.prefer_old_block_holders(candidates))
    }

    /// Returns the `candidates` that may hold old blocks, see [`Self::may_hold_old_blocks`], or
    /// all of them if none does.
    fn prefer_old_block_holders<'a>(
        &self,
        candidates: &'a HashSet<D::Key>,
    ) -> Cow<'a, HashSet<D::Key>> {
        let archive_candidates: HashSet<D::Key> = candidates
            .iter()
            .filter(|key| {
                self.ready_services
                    .get(key)
                    .is_some_and(|service| self.may_hold_old_blocks(service))
            })
            .copied()
            .collect();

        if archive_candidates.is_empty() {
            Cow::Borrowed(candidates)
        } else {
            Cow::Owned(archive_candidates)
        }
    }

    /// Returns true if this node's best chain tip is at or near the estimated network tip, and
    /// false while it is still syncing or has no tip yet.
    fn is_near_network_tip(&self) -> bool {
        self.minimum_peer_version
            .chain_tip()
            .is_at_or_near_network_tip(&self.network)
    }

    /// Returns true if `service` is a zcashd sidecar whose handshake advertised no more blocks
    /// than this node has. A sidecar follows only this node, so it then has no block this node
    /// lacks, and zcashd leaves a request for such a block unanswered.
    fn is_sidecar_without_new_blocks(&self, service: &D::Service) -> bool {
        let tip_height = self
            .minimum_peer_version
            .chain_tip()
            .best_tip_height()
            .unwrap_or(block::Height(0));

        self.is_zcashd_compat_peer(service) && service.remote_start_height() <= tip_height
    }

    /// Returns true if `service` may hold old blocks that pruned peers have deleted: it does not
    /// advertise the pruned hint, and it is not a sidecar without new blocks.
    ///
    /// The hint is self-reported: a peer that omits it is asked like any archive peer.
    fn may_hold_old_blocks(&self, service: &D::Service) -> bool {
        service.claims_full_block_history() && !self.is_sidecar_without_new_blocks(service)
    }

    /// Returns true if this node is still syncing and every peer it would download old blocks
    /// from is busy: the peers that may hold old blocks and have not left a block request
    /// unanswered.
    ///
    /// Requests then wait for one of them, as they do when every peer is busy. Otherwise the
    /// ready pruned peers would take the block requests those peers have no room for, and answer
    /// `notfound`.
    fn awaits_archive_block_source(&self) -> bool {
        if self.is_near_network_tip() {
            return false;
        }

        let is_source = |service: &D::Service| {
            self.may_hold_old_blocks(service) && !service.has_unanswered_block_request()
        };

        if self.ready_services.values().any(is_source) {
            return false;
        }

        Pin::new(&self.unready_services)
            .iter_pin_ref()
            .filter_map(|unready| unready.get_ref().service.as_ref())
            .any(is_source)
    }

    /// Performs P2C on `candidates` for a request for block bodies.
    ///
    /// While syncing, requests can be for blocks a pruned peer has deleted. Prefer peers that may
    /// hold old blocks, see [`Self::may_hold_old_blocks`], unless no candidate is one: this node
    /// is connected to none, or the ones it is connected to do not have this block.
    /// [`Self::awaits_archive_block_source`] makes requests wait while they are only busy.
    ///
    /// First leave out peers that have left a block request unanswered, unless that would remove
    /// every candidate. Otherwise a silent archive peer can capture every retry even when a
    /// pruned peer still holds the requested blocks. A successful block response clears this
    /// penalty; merely being busy with another response does not incur it.
    ///
    /// Near the network tip this node asks for recent blocks, which pruned peers keep, so all
    /// candidates are treated alike.
    fn select_block_body_peer(&self, candidates: &HashSet<D::Key>) -> Option<D::Key> {
        if self.is_near_network_tip() {
            return self.select_p2c_peer_from_list(candidates);
        }

        let responsive_candidates: HashSet<D::Key> = candidates
            .iter()
            .filter(|key| {
                self.ready_services
                    .get(key)
                    .is_some_and(|service| !service.has_unanswered_block_request())
            })
            .copied()
            .collect();
        let candidates = if responsive_candidates.is_empty() {
            candidates
        } else {
            &responsive_candidates
        };

        self.select_p2c_peer_from_list(&self.prefer_old_block_holders(candidates))
    }

    /// Performs P2C on `ready_service_list` to randomly select a less-loaded ready service.
    #[allow(clippy::unwrap_in_result)]
    fn select_p2c_peer_from_list(&self, ready_service_list: &HashSet<D::Key>) -> Option<D::Key> {
        match ready_service_list.len() {
            0 => None,
            1 => Some(
                *ready_service_list
                    .iter()
                    .next()
                    .expect("just checked there is one service"),
            ),
            len => {
                // Choose 2 random peers, then return the least loaded of those 2 peers.
                let (a, b) = Self::sample_two(ready_service_list, len);

                let a_load = self.query_load(&a).expect("supplied services are ready");
                let b_load = self.query_load(&b).expect("supplied services are ready");

                let selected = if a_load <= b_load { a } else { b };

                trace!(
                    a.key = %a.addr_label(self.expose_peer_addresses),
                    a.load = ?a_load,
                    b.key = %b.addr_label(self.expose_peer_addresses),
                    b.load = ?b_load,
                    selected = %selected.addr_label(self.expose_peer_addresses),
                    ?len,
                    "selected service by p2c"
                );

                Some(selected)
            }
        }
    }

    /// Chooses two different random peers from `peers`, which has `len >= 2` entries.
    fn sample_two(peers: &HashSet<D::Key>, len: usize) -> (D::Key, D::Key) {
        let idxs = rand_10::seq::index::sample(&mut rand_10::rng(), len, 2);
        let nth = |index| {
            *peers
                .iter()
                .nth(index)
                .expect("sample returns valid indexes")
        };
        (nth(idxs.index(0)), nth(idxs.index(1)))
    }

    /// Returns true if `peer` is still live on the connection generation `connection_id`.
    fn is_current_peer_connection(&self, peer: &PeerSocketAddr, connection_id: u64) -> bool {
        if let Some(svc) = self.ready_services.get(peer) {
            svc.connection_id() == connection_id
        } else {
            self.cancel_handles.contains_key(peer)
        }
    }

    /// Drains block delivery and miss outcomes from completed single-block requests.
    fn drain_block_feedback(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some(event)) = self.block_feedback_rx.poll_recv(cx) {
            match event {
                BlockFeedback::Delivered {
                    peer,
                    connection_id,
                    idle_at_dispatch,
                    elapsed,
                    block_bytes,
                    at,
                } => {
                    if block_bytes > 0 {
                        self.last_block_bytes = block_bytes;
                    }
                    if !self.is_current_peer_connection(&peer, connection_id) {
                        continue;
                    }
                    if self
                        .announced_block_misses
                        .get(&peer)
                        .is_some_and(|(id, missed_at)| *id == connection_id && at >= *missed_at)
                    {
                        self.announced_block_misses.remove(&peer);
                    }
                    if idle_at_dispatch {
                        let rtt = self
                            .block_gossip_peers
                            .get(&peer)
                            .and_then(|gossip| gossip.min_rtt(at))
                            .unwrap_or(Duration::ZERO);
                        if let Some(rate_bps) = sample_rate_bps(block_bytes, elapsed, rtt) {
                            self.peer_rates.insert(peer, (connection_id, at, rate_bps));
                        }
                    }
                }
                BlockFeedback::Missed {
                    peer,
                    connection_id,
                    at,
                } => {
                    if self.is_current_peer_connection(&peer, connection_id)
                        && self
                            .announced_block_misses
                            .get(&peer)
                            .is_none_or(|(id, missed_at)| *id != connection_id || *missed_at <= at)
                    {
                        self.announced_block_misses
                            .insert(peer, (connection_id, at));
                    }
                }
            }
        }
    }

    /// Returns the miss cooldown expiry for `peer` if its current connection still has an active
    /// announced-block miss.
    fn miss_expiry(
        &self,
        peer: &PeerSocketAddr,
        connection_id: u64,
        now: tokio::time::Instant,
    ) -> Option<tokio::time::Instant> {
        self.announced_block_misses
            .get(peer)
            .filter(|(id, _)| *id == connection_id)
            .map(|(_, missed_at)| *missed_at + ESTIMATE_WINDOW)
            .filter(|expiry| *expiry > now)
    }

    /// Returns `(rate_bps, expiry)` for `peer`'s current connection if a fresh rate exists.
    fn peer_rate(
        &self,
        peer: &PeerSocketAddr,
        connection_id: u64,
        now: tokio::time::Instant,
    ) -> Option<(u64, tokio::time::Instant)> {
        self.peer_rates
            .get(peer)
            .filter(|(id, at, _)| {
                *id == connection_id && now.saturating_duration_since(*at) <= ESTIMATE_WINDOW
            })
            .map(|(_, at, rate)| (*rate, *at + ESTIMATE_WINDOW))
    }

    /// Computes fleet-wide median RTT and incoming transfer rate across measured ready peers.
    fn ready_medians(&self, now: tokio::time::Instant) -> ReadyMedians {
        let mut rtts: Vec<u128> = self
            .ready_services
            .values()
            .filter_map(|svc| svc.block_gossip.min_rtt(now).map(|d| d.as_nanos()))
            .collect();
        let mut rates: Vec<u128> = self
            .ready_services
            .iter()
            .filter_map(|(addr, svc)| {
                self.peer_rate(addr, svc.connection_id(), now)
                    .map(|(rate, _)| u128::from(rate))
            })
            .collect();
        ReadyMedians {
            rtt: median_u128(&mut rtts)
                .map(|ns| Duration::from_nanos(u64::try_from(ns).unwrap_or(u64::MAX))),
            rate_bps: median_u128(&mut rates).map(|r| u64::try_from(r).unwrap_or(u64::MAX)),
        }
    }

    /// Expected arrival duration for one block from `svc`, falling back to `medians` for missing
    /// terms. Returns `None` only when neither this peer nor any ready peer has an RTT sample.
    fn expected_arrival_for(
        &self,
        key: &PeerSocketAddr,
        svc: &LoadTrackedClient,
        now: tokio::time::Instant,
        medians: ReadyMedians,
    ) -> Option<Duration> {
        let rtt = svc.block_gossip.min_rtt(now).or(medians.rtt)?;
        let rate_bps = self
            .peer_rate(key, svc.connection_id(), now)
            .map(|(rate, _)| rate)
            .or(medians.rate_bps);
        Some(arrival_duration(rtt, self.last_block_bytes, rate_bps))
    }

    /// Draws two random peers from `peers` and returns the one expected to arrive sooner.
    fn select_p2c_peer_by_arrival(
        &self,
        peers: &HashSet<D::Key>,
        now: tokio::time::Instant,
        medians: ReadyMedians,
    ) -> Option<D::Key> {
        match peers.len() {
            0 => None,
            1 => peers.iter().next().copied(),
            len => {
                let (a, b) = Self::sample_two(peers, len);
                let a_est = self
                    .ready_services
                    .get(&a)
                    .and_then(|svc| self.expected_arrival_for(&a, svc, now, medians));
                let b_est = self
                    .ready_services
                    .get(&b)
                    .and_then(|svc| self.expected_arrival_for(&b, svc, now, medians));
                let selected = match (a_est, b_est) {
                    (Some(a_arr), Some(b_arr)) if b_arr < a_arr => b,
                    (None, Some(_)) => b,
                    _ => a,
                };
                Some(selected)
            }
        }
    }

    /// Chooses one of the ready block `advertisers`, and describes the choice for the trace.
    ///
    /// Among the advertisers that are preferred block sources, it draws two at random and takes
    /// the less loaded one, or, with `[network.block_download_score_and_hedge]` enabled, the one
    /// expected to arrive sooner. Falls back to load-based P2C over all `advertisers` if none is
    /// preferred.
    fn select_block_advertiser(
        &self,
        advertisers: &HashSet<D::Key>,
        now: tokio::time::Instant,
        medians: ReadyMedians,
    ) -> Option<(D::Key, BlockSourceChoice)> {
        let preferred_advertisers: HashSet<D::Key> = advertisers
            .iter()
            .filter(|key| {
                self.ready_services.get(*key).is_some_and(|svc| {
                    svc.is_preferred_block_source()
                        && !(self.block_download_score_and_hedge
                            && self.miss_expiry(key, svc.connection_id(), now).is_some())
                })
            })
            .copied()
            .collect();

        let peer = if self.block_download_score_and_hedge {
            self.select_p2c_peer_by_arrival(&preferred_advertisers, now, medians)
        } else {
            self.select_p2c_peer_from_list(&preferred_advertisers)
        }
        .or_else(|| self.select_busy_block_advertiser(advertisers))?;

        let score = self
            .ready_services
            .get(&peer)
            .and_then(|svc| self.expected_arrival_for(&peer, svc, now, medians));
        let best_score = preferred_advertisers
            .iter()
            .filter_map(|key| {
                self.ready_services
                    .get(key)
                    .and_then(|svc| self.expected_arrival_for(key, svc, now, medians))
            })
            .min();
        let choice = BlockSourceChoice {
            candidates: preferred_advertisers.len(),
            score,
            best_score,
        };
        if choice.candidates >= 2 {
            metrics::counter!("pool.route_inv.score.choices.count").increment(1);
            if choice.score.is_none() {
                metrics::counter!("pool.route_inv.score.unscored.count").increment(1);
            } else if choice.score == choice.best_score {
                metrics::counter!("pool.route_inv.score.chosen_best.count").increment(1);
            }
        }
        Some((peer, choice))
    }

    /// Performs P2C over all `advertisers`, for when none of them is a preferred block source.
    /// If the selected connection is busy, the block request waits behind its current response.
    fn select_busy_block_advertiser(&self, advertisers: &HashSet<D::Key>) -> Option<D::Key> {
        let peer = self.select_p2c_peer_from_list(advertisers);
        let selected_busy_peer = peer
            .and_then(|key| self.ready_services.get(&key))
            .is_some_and(|svc| !svc.can_send_request_now());
        if selected_busy_peer {
            metrics::counter!("pool.route_inv.advertiser.awaiting_response.count").increment(1);
        }
        peer
    }

    /// Wraps a routed single-block response future to trace its completion and report delivery or
    /// miss feedback back to the peer set.
    #[allow(clippy::too_many_arguments)]
    fn wrap_block_response(
        &self,
        fut: <LoadTrackedClient as Service<Request>>::Future,
        peer: PeerSocketAddr,
        connection_id: u64,
        trace_peer: PeerTraceContext,
        hash: block::Hash,
        route: BlockRequestRoute,
        idle_at_dispatch: bool,
        advertised: bool,
        started: tokio::time::Instant,
    ) -> SecondResponse {
        let request_id = self.legacy_peer_trace.next_request_id();
        let trace = self.legacy_peer_trace.clone();
        let feedback_tx = self.block_feedback_tx.clone();
        async move {
            let result = fut.await;
            let at = tokio::time::Instant::now();
            let elapsed = at.saturating_duration_since(started);
            trace.block_request_finish(request_id, trace_peer, hash, route, elapsed, &result);
            let mapped = result.map_err(Into::into);
            if is_usable_block(hash, &mapped) {
                if let Ok(Response::Blocks(blocks)) = &mapped {
                    if let Some((block, _)) = blocks[0].available() {
                        let _ = feedback_tx.send(BlockFeedback::Delivered {
                            peer,
                            connection_id,
                            idle_at_dispatch,
                            elapsed,
                            block_bytes: block.decoded_bytes(),
                            at,
                        });
                    }
                }
            } else if advertised {
                let _ = feedback_tx.send(BlockFeedback::Missed {
                    peer,
                    connection_id,
                    at,
                });
            }
            mapped
        }
        .boxed()
    }

    /// Randomly chooses `max_peers` ready services, ignoring service load.
    ///
    /// The chosen peers are unique, but their order is not fully random.
    fn select_random_ready_peers(&self, max_peers: usize) -> Vec<D::Key> {
        use rand_10::seq::IteratorRandom;

        self.ready_services
            .keys()
            .copied()
            .sample(&mut rand_10::rng(), max_peers)
    }

    /// Records the block gossip sender for the current ready or busy connection generation.
    ///
    /// Marks the published peer list stale only when this is a new peer or a new connection for
    /// the key, because this runs every time a peer moves between ready and busy.
    fn insert_block_gossip_peer(&mut self, key: D::Key, svc: &D::Service) {
        let mut peer = svc.block_gossip.clone();
        peer.sidecar = self.is_zcashd_compat_peer(svc);
        let unchanged = self.block_gossip_peers.get(&key).is_some_and(|known| {
            known.sender.same_channel(&peer.sender) && known.sidecar == peer.sidecar
        });
        self.block_gossip_peers.insert(key, peer);
        self.block_gossip_peers_changed |= !unchanged;
    }

    /// Returns true if `service` is a configured zcashd sidecar peer.
    fn is_zcashd_compat_peer(&self, service: &D::Service) -> bool {
        self.block_gossip_peer_ips
            .iter()
            .any(|ip| service.is_inbound_direct_from_ip(ip))
    }

    /// Accesses a ready endpoint by `key` and returns its current load.
    ///
    /// Returns `None` if the service is not in the ready service list.
    fn query_load(&self, key: &D::Key) -> Option<<D::Service as Load>::Metric> {
        let svc = self.ready_services.get(key);
        svc.map(|svc| svc.load())
    }

    /// Build privacy-preserving metadata for correlating requests to one legacy peer connection.
    fn legacy_peer_trace_context(
        &self,
        peer: PeerSocketAddr,
        svc: &D::Service,
    ) -> PeerTraceContext {
        PeerTraceContext {
            connection_id: svc.connection_id(),
            peer,
            user_agent: svc.remote_user_agent(),
            peer_start_height: svc.remote_start_height(),
            local_tip_height: self.minimum_peer_version.chain_tip().best_tip_height(),
        }
    }

    /// Routes a request using P2C load-balancing.
    fn route_p2c(&mut self, req: Request) -> <Self as tower::Service<Request>>::Future {
        let is_find_request = matches!(
            &req,
            Request::FindBlocks { .. } | Request::FindHeaders { .. }
        );

        if let Some(p2c_key) = self.select_ready_p2c_peer(&req) {
            tracing::trace!(
                peer = %p2c_key.addr_label(self.expose_peer_addresses),
                "routing based on p2c"
            );

            let mut svc = self
                .take_ready_service(&p2c_key)
                .expect("selected peer must be ready");

            let find_blocks_trace = match &req {
                Request::FindBlocks { known_blocks, stop } => Some((
                    self.legacy_peer_trace.next_request_id(),
                    self.legacy_peer_trace_context(p2c_key, &svc),
                    known_blocks.first().copied(),
                    *stop,
                    Instant::now(),
                )),
                _ => None,
            };

            let track_stalls =
                is_find_request && !self.is_zcashd_compat_peer(&svc) && !self.is_near_network_tip();

            let fut = svc.call(req);
            self.push_unready(p2c_key, svc);

            if track_stalls || find_blocks_trace.is_some() {
                let stall_tx = self.stall_event_tx.clone();
                let trace = self.legacy_peer_trace.clone();
                return async move {
                    let result = fut.await;
                    if track_stalls {
                        if let Some(outcome) = classify_find_response(&result) {
                            let _ = stall_tx.send((p2c_key, outcome));
                        }
                    }
                    if let Some((request_id, peer, locator_tip, stop, started)) = find_blocks_trace
                    {
                        trace.find_blocks_finish(
                            request_id,
                            peer,
                            locator_tip,
                            stop,
                            started.elapsed(),
                            &result,
                        );
                    }
                    result.map_err(Into::into)
                }
                .boxed();
            }

            return fut.map_err(Into::into).boxed();
        }

        async move {
            // Let other tasks run, so a retry request might get different ready peers.
            tokio::task::yield_now().await;

            // # Security
            //
            // Avoid routing requests to peers that are missing inventory.
            // If we kept trying doomed requests, peers that are missing our requested inventory
            // could take up a large amount of our bandwidth and retry limits.
            Err(SharedPeerError::from(PeerError::NoReadyPeers))
        }
        .map_err(Into::into)
        .boxed()
    }

    /// Tries to route a request to a ready peer that advertised that inventory,
    /// falling back to a ready peer that isn't missing the inventory.
    ///
    /// A transaction request whose advertisers are all busy waits for one of them instead, see
    /// [`Self::route_held_requests`]. Another peer usually doesn't have the transaction yet.
    ///
    /// If all ready peers are missing the inventory,
    /// returns a synthetic [`NotFoundRegistry`](PeerError::NotFoundRegistry) error.
    ///
    /// Uses P2C to route requests to the least loaded peer in each list.
    fn route_inv(
        &mut self,
        req: Request,
        hash: InventoryHash,
    ) -> <Self as tower::Service<Request>>::Future {
        let advertising_peer_list = self
            .inventory_registry
            .advertising_peers(hash)
            .filter(|&addr| self.ready_services.contains_key(addr))
            .copied()
            .collect();

        let now = tokio::time::Instant::now();
        let medians = if matches!(hash, InventoryHash::Block(_)) {
            self.ready_medians(now)
        } else {
            ReadyMedians::default()
        };

        // # Security
        //
        // Choose a random, less-loaded peer with the inventory.
        //
        // If we chose the first peer in HashMap order,
        // peers would be able to influence our choice by switching addresses.
        // But we need the choice to be random,
        // so that a peer can't provide all our inventory responses.
        let (peer, choice) = match hash {
            InventoryHash::Block(_) => self
                .select_block_advertiser(&advertising_peer_list, now, medians)
                .map_or((None, None), |(peer, choice)| (Some(peer), Some(choice))),
            _ => (self.select_p2c_peer_from_list(&advertising_peer_list), None),
        };

        if let Some(mut svc) = peer.and_then(|key| self.take_ready_service(&key)) {
            let peer = peer.expect("just checked peer is Some");
            tracing::trace!(
                ?hash,
                peer = %peer.addr_label(self.expose_peer_addresses),
                "routing to a peer which advertised inventory"
            );
            metrics::counter!("pool.route_inv.advertiser.count").increment(1);
            let idle_at_dispatch = svc.can_send_request_now();
            let connection_id = svc.connection_id();
            let trace_peer = self.legacy_peer_trace_context(peer, &svc);
            let fut = svc.call(req);
            self.push_unready(peer, svc);
            if let InventoryHash::Block(block_hash) = hash {
                let choice = choice.unwrap_or_default();
                let primary = self.wrap_block_response(
                    fut,
                    peer,
                    connection_id,
                    trace_peer,
                    block_hash,
                    BlockRequestRoute::Advertised(choice),
                    idle_at_dispatch,
                    true,
                    now,
                );
                return self.maybe_hedge_block(block_hash, peer, choice.score, now, primary);
            }
            return fut.map_err(Into::into).boxed();
        }

        if !matches!(hash, InventoryHash::Block(_))
            && self.held_tx_requests.len() < MAX_HELD_TX_REQUESTS
            && self.has_busy_advertiser(hash)
        {
            return self.hold_tx_request(req, hash);
        }

        self.route_inv_to_any_peer(req, hash)
    }

    /// Arms an internal event-driven second block request when scoring/hedging is enabled and the
    /// block has at least one advertiser.
    fn maybe_hedge_block(
        &mut self,
        hash: block::Hash,
        first_peer: PeerSocketAddr,
        first_score: Option<Duration>,
        dispatched_at: tokio::time::Instant,
        primary: SecondResponse,
    ) -> SecondResponse {
        if !self.block_download_score_and_hedge {
            return primary;
        }
        let Some(first_score) = first_score else {
            metrics::counter!("pool.route_inv.hedge.unscored_first.count").increment(1);
            return primary;
        };
        let (held, hedge_rx) = HeldSecondRequest::new(hash, first_peer, first_score, dispatched_at);
        self.held_second_requests.push(held);
        race_block_request(hash, primary, hedge_rx)
    }

    /// Routes an inventory request to a ready peer that isn't missing the inventory, without
    /// preferring a peer that advertised it.
    ///
    /// If all ready peers are missing the inventory, returns a synthetic
    /// [`NotFoundRegistry`](PeerError::NotFoundRegistry) error.
    fn route_inv_to_any_peer(
        &mut self,
        req: Request,
        hash: InventoryHash,
    ) -> <Self as tower::Service<Request>>::Future {
        let missing_peer_list: HashSet<PeerSocketAddr> = self
            .inventory_registry
            .missing_peers(hash)
            .copied()
            .collect();

        // No ready peer advertised this block, so its known source is the most likely to have it:
        // a child's advertiser has validated the child's parent. Advertisers go first, so this
        // can't take a request away from a peer that said it has the block.
        let block_source = match &req {
            Request::BlocksByHashFrom {
                source: PeerSource::LegacySocket(addr),
                ..
            } if self.ready_services.contains_key(addr) && !missing_peer_list.contains(addr) => {
                Some(*addr)
            }
            _ => None,
        };
        let maybe_peer_list = match block_source {
            Some(addr) => HashSet::from([addr]),
            None => self
                .ready_services
                .keys()
                .filter(|addr| !missing_peer_list.contains(addr))
                .copied()
                .collect(),
        };

        // Security: choose a random, less-loaded peer that might have the inventory.
        let peer = if matches!(hash, InventoryHash::Block(_)) {
            self.select_block_body_peer(&maybe_peer_list)
        } else {
            self.select_p2c_peer_from_list(&maybe_peer_list)
        };

        let now = tokio::time::Instant::now();
        let has_advertiser = matches!(hash, InventoryHash::Block(_))
            && self
                .inventory_registry
                .advertising_peers(hash)
                .next()
                .is_some();
        let first_score = peer.and_then(|key| {
            if !(has_advertiser && self.block_download_score_and_hedge) {
                return None;
            }
            let medians = self.ready_medians(now);
            let svc = self.ready_services.get(&key)?;
            self.expected_arrival_for(&key, svc, now, medians)
        });

        if let Some(mut svc) = peer.and_then(|key| self.take_ready_service(&key)) {
            let peer = peer.expect("just checked peer is Some");
            tracing::trace!(
                ?hash,
                peer = %peer.addr_label(self.expose_peer_addresses),
                "routing to a peer that might have inventory"
            );
            metrics::counter!("pool.route_inv.maybe.count").increment(1);
            let idle_at_dispatch = svc.can_send_request_now();
            let connection_id = svc.connection_id();
            let trace_peer = self.legacy_peer_trace_context(peer, &svc);
            let fut = svc.call(req);
            self.push_unready(peer, svc);
            if let InventoryHash::Block(block_hash) = hash {
                let primary = self.wrap_block_response(
                    fut,
                    peer,
                    connection_id,
                    trace_peer,
                    block_hash,
                    BlockRequestRoute::Maybe,
                    idle_at_dispatch,
                    false,
                    now,
                );
                if has_advertiser {
                    return self.maybe_hedge_block(block_hash, peer, first_score, now, primary);
                }
                return primary;
            }
            return fut.map_err(Into::into).boxed();
        }

        // Split the synthetic registry-miss by cause so a stall can be diagnosed (the two collapse
        // into the same `NotFoundRegistry` error otherwise). Incremented synchronously here, not in
        // the returned future, so the count reflects the routing decision even if it isn't polled.
        //
        // - `no_ready`: effectively no ready peer to try — peer-set saturation (speculative
        //   look-ahead keeps every peer busy), so the head-of-line block can't get on the wire.
        // - `all_missing`: ready peers exist, but every one is marked missing this hash — local
        //   inventory-marker staleness.
        if self.ready_services.is_empty() {
            metrics::counter!("pool.route_inv.notfound.no_ready.count").increment(1);
        } else {
            metrics::counter!("pool.route_inv.notfound.all_missing.count").increment(1);
        }

        tracing::debug!(
            ?hash,
            "all ready peers are missing inventory, failing request"
        );

        async move {
            // Let other tasks run, so a retry request might get different ready peers.
            tokio::task::yield_now().await;

            // # Security
            //
            // Avoid routing requests to peers that are missing inventory.
            // If we kept trying doomed requests, peers that are missing our requested inventory
            // could take up a large amount of our bandwidth and retry limits.
            Err(SharedPeerError::from(PeerError::NotFoundRegistry(vec![
                hash,
            ])))
        }
        .map_err(Into::into)
        .boxed()
    }

    /// Returns true if a peer that advertised `hash` is busy with a request.
    fn has_busy_advertiser(&self, hash: InventoryHash) -> bool {
        self.inventory_registry
            .advertising_peers(hash)
            .any(|addr| self.cancel_handles.contains_key(addr))
    }

    /// Returns true if a peer that advertised `hash` is ready.
    fn has_ready_advertiser(&self, hash: InventoryHash) -> bool {
        self.inventory_registry
            .advertising_peers(hash)
            .any(|addr| self.ready_services.contains_key(addr))
    }

    /// Queues a transaction request for a busy advertiser, and returns a future that waits for
    /// the routed request's response.
    fn hold_tx_request(
        &mut self,
        req: Request,
        hash: InventoryHash,
    ) -> <Self as tower::Service<Request>>::Future {
        let (reply, routed) = oneshot::channel();
        self.held_tx_requests.push_back(HeldTxRequest {
            req,
            hash,
            deadline: tokio::time::Instant::now() + TX_ADVERTISER_WAIT,
            span: tracing::Span::current(),
            reply,
        });

        async move {
            let response = routed
                .await
                .map_err(|_| "peer set shut down before routing a held request")?;
            response.await
        }
        .boxed()
    }

    /// Routes held block hedges and held transaction requests that can go now, sharing one timer
    /// across both queues. The peer set's owner calls this after routing each new request (so held
    /// work never takes a peer from a waiting primary request) and when idle.
    pub(super) fn route_held_requests(&mut self, cx: &mut Context<'_>) -> Result<(), BoxError> {
        self.drain_block_feedback(cx);
        if self.held_second_requests.is_empty() && self.held_tx_requests.is_empty() {
            self.held_timer = None;
            metrics::gauge!("pool.route_inv.hedge.held").set(0.0);
            return Ok(());
        }

        // Adverts that arrived while requests were held are only read here and while routing.
        let _poll_pending_or_ready: Poll<()> = self.inventory_registry.poll_inventory(cx)?;

        let mut routed = false;
        let mut next_deadline: Option<tokio::time::Instant> = None;

        // Held block hedges first: they are time-sensitive tip blocks, and primary requests
        // already ran before this method was entered.
        if !self.held_second_requests.is_empty() {
            // Register the single fleet-wide RTT waker before reading any RTT or median.
            if let Some(gossip) = self
                .block_gossip_peers
                .values()
                .find(|gossip| gossip.registry.is_some())
            {
                gossip.notify_when_rtt_changes(cx);
            }
            let considered_at = tokio::time::Instant::now();
            let medians = self.ready_medians(considered_at);
            for mut held in std::mem::take(&mut self.held_second_requests) {
                if !held.poll_open(cx, considered_at) {
                    continue;
                }
                let mut deadline = held.next_decision_after(considered_at);
                match self.choose_second_request(&held, considered_at, medians, cx, &mut deadline) {
                    Some((peer, choice)) => {
                        let response = held
                            .span
                            .in_scope(|| self.send_second_request(held.hash, peer, choice));
                        held.send(response);
                        routed = true;
                    }
                    None => {
                        next_deadline = Some(next_deadline.map_or(deadline, |at| at.min(deadline)));
                        self.held_second_requests.push(held);
                    }
                }
            }
        }
        metrics::gauge!("pool.route_inv.hedge.held").set(self.held_second_requests.len() as f64);

        // Held transaction requests waiting for busy advertisers.
        let now = tokio::time::Instant::now();
        for _ in 0..self.held_tx_requests.len() {
            let held = self
                .held_tx_requests
                .pop_front()
                .expect("the loop runs once per held request");
            if held.reply.is_canceled() {
                continue;
            }

            let to_advertiser = self.has_ready_advertiser(held.hash);
            if !to_advertiser && held.deadline > now && self.has_busy_advertiser(held.hash) {
                next_deadline =
                    Some(next_deadline.map_or(held.deadline, |at| at.min(held.deadline)));
                self.held_tx_requests.push_back(held);
                continue;
            }
            if !to_advertiser && self.ready_services.is_empty() {
                self.held_tx_requests.push_back(held);
                continue;
            }

            let response = held.span.in_scope(|| {
                if to_advertiser {
                    self.route_inv(held.req, held.hash)
                } else {
                    self.route_inv_to_any_peer(held.req, held.hash)
                }
            });
            let _ = held.reply.send(response);
            routed = true;
        }

        match (next_deadline, self.held_timer.as_mut()) {
            (Some(deadline), Some(timer)) => timer.as_mut().reset(deadline),
            (Some(deadline), None) => self.held_timer = Some(Box::pin(sleep_until(deadline))),
            (None, _) => self.held_timer = None,
        }
        let timer_expired = self
            .held_timer
            .as_mut()
            .is_some_and(|timer| timer.as_mut().poll(cx).is_ready());
        if routed || timer_expired {
            cx.waker().wake_by_ref();
        }

        Ok(())
    }

    /// Chooses the peer to send a second request for a gossiped block to, if another announcer
    /// should deliver it sooner than the first request or the first request has stalled.
    fn choose_second_request(
        &self,
        held: &HeldSecondRequest,
        considered_at: tokio::time::Instant,
        medians: ReadyMedians,
        cx: &Context<'_>,
        next_decision: &mut tokio::time::Instant,
    ) -> Option<(D::Key, SecondRequestChoice)> {
        let waited = considered_at.saturating_duration_since(held.dispatched_at);
        let stalled = is_stalled(held.first_score, waited);
        let mut best_measured: Option<(D::Key, Duration)> = None;
        let mut measured_count = 0usize;
        let mut fallback_unmeasured: Option<(D::Key, Option<Duration>)> = None;

        for addr in self
            .inventory_registry
            .advertising_peers(InventoryHash::from(held.hash))
            .filter(|addr| **addr != held.first_peer)
        {
            let Some(svc) = self.ready_services.get(addr) else {
                continue;
            };
            svc.notify_when_usable(cx);
            if !svc.is_preferred_block_source() {
                continue;
            }
            let conn_id = svc.connection_id();
            if let Some(expiry) = self.miss_expiry(addr, conn_id, considered_at) {
                *next_decision = (*next_decision).min(expiry);
                continue;
            }
            if let Some((rate, expiry)) = self.peer_rate(addr, conn_id, considered_at) {
                if medians.rate_bps.is_none_or(|median| rate < median) {
                    *next_decision = (*next_decision).min(expiry);
                }
            }
            if svc.block_gossip.min_rtt(considered_at).is_some() {
                if let Some(score) = self.expected_arrival_for(addr, svc, considered_at, medians) {
                    measured_count += 1;
                    if best_measured.is_none_or(|(_, best)| score < best) {
                        best_measured = Some((*addr, score));
                    }
                }
            } else if fallback_unmeasured.is_none() {
                let score = self.expected_arrival_for(addr, svc, considered_at, medians);
                fallback_unmeasured = Some((*addr, score));
            }
        }

        if let Some((peer, score)) = best_measured {
            let Some(reason) = second_request_reason(held.first_score, waited, score) else {
                metrics::counter!("pool.route_inv.hedge.not_yet.count").increment(1);
                return None;
            };
            return Some((
                peer,
                SecondRequestChoice {
                    first_request_waited: waited,
                    reason,
                    candidates: measured_count,
                    score: Some(score),
                    first_score: held.first_score,
                },
            ));
        }

        if stalled {
            if let Some((peer, score)) = fallback_unmeasured {
                return Some((
                    peer,
                    SecondRequestChoice {
                        first_request_waited: waited,
                        reason: SecondRequestReason::Stalled,
                        candidates: 1,
                        score,
                        first_score: held.first_score,
                    },
                ));
            }
        }

        metrics::counter!("pool.route_inv.hedge.no_candidate.count").increment(1);
        None
    }

    /// Sends the second request for `hash` to `peer` and returns its traced response future.
    fn send_second_request(
        &mut self,
        hash: block::Hash,
        peer: D::Key,
        choice: SecondRequestChoice,
    ) -> SecondResponse {
        let mut svc = self
            .take_ready_service(&peer)
            .expect("the peer was chosen from the ready set, which has not changed since");

        metrics::counter!("pool.route_inv.hedge.count", "reason" => choice.reason.label())
            .increment(1);
        let idle_at_dispatch = svc.can_send_request_now();
        let connection_id = svc.connection_id();
        let trace_peer = self.legacy_peer_trace_context(peer, &svc);
        let started = tokio::time::Instant::now();
        let fut = svc.call(Request::BlocksByHash(std::iter::once(hash).collect()));
        self.push_unready(peer, svc);

        self.wrap_block_response(
            fut,
            peer,
            connection_id,
            trace_peer,
            hash,
            BlockRequestRoute::Hedge(choice),
            idle_at_dispatch,
            true,
            started,
        )
    }

    /// Routes the same request to up to `max_peers` ready peers, ignoring return values.
    ///
    /// `max_peers` must be at least one, and at most the number of ready peers.
    fn route_multiple(
        &mut self,
        req: Request,
        max_peers: usize,
    ) -> <Self as tower::Service<Request>>::Future {
        assert!(
            max_peers > 0,
            "requests must be routed to at least one peer"
        );
        assert!(
            max_peers <= self.ready_services.len(),
            "requests can only be routed to ready peers"
        );

        let selected_peers = self.select_random_ready_peers(max_peers);
        self.send_multiple(req, selected_peers)
    }

    /// Sends the same request to the provided ready peers, ignoring return values.
    ///
    /// # Security
    ///
    /// Callers should choose peers randomly, ignoring load.
    /// This avoids favouring malicious peers, because peers can influence their own load.
    ///
    /// The order of peers isn't completely random,
    /// but peer request order is not security-sensitive.
    fn send_multiple(
        &mut self,
        req: Request,
        peers: Vec<D::Key>,
    ) -> <Self as tower::Service<Request>>::Future {
        let futs = FuturesUnordered::new();
        for key in peers {
            let mut svc = self
                .take_ready_service(&key)
                .expect("selected peers are ready");
            futs.push(svc.call(req.clone()).map_err(|_| ()));
            self.push_unready(key, svc);
        }

        async move {
            let results = futs.collect::<Vec<Result<_, _>>>().await;
            let succeeded = results.iter().any(Result::is_ok);
            tracing::debug!(
                ok.len = results.iter().filter(|r| r.is_ok()).count(),
                err.len = results.iter().filter(|r| r.is_err()).count(),
                "sent peer request to multiple peers"
            );
            if results.is_empty() || succeeded {
                Ok(Response::Nil)
            } else {
                Err(std::io::Error::other("every selected peer request failed").into())
            }
        }
        .boxed()
    }

    /// Broadcasts the same request to lots of ready peers, ignoring return values.
    fn route_broadcast(&mut self, req: Request) -> <Self as tower::Service<Request>>::Future {
        // Broadcasts ignore the response
        self.route_multiple(req, self.number_of_peers_to_broadcast())
    }

    /// Schedule legacy inventory by block size, including busy sidecars.
    fn route_block_broadcast(&mut self, req: Request) -> <Self as tower::Service<Request>>::Future {
        let Ok((block, push)) = self.block_gossip_fanout.advertised_block(req) else {
            unreachable!("block routing only accepts block advertisements");
        };
        self.prune_block_gossip_peers();
        // A minimum version increase cancels busy outdated peers only after their current work.
        let minimum_version = self.minimum_peer_version.current();
        let peers = self
            .block_gossip_peers
            .values()
            .filter(|peer| peer.version >= minimum_version)
            .cloned()
            .collect();
        crate::peer::block_gossip::broadcast_block(
            peers,
            block.hash,
            self.block_gossip_fanout.window(block.size),
            tokio::time::Instant::now() + self.block_gossip_fanout.delay(),
            push,
            block.pending_send,
        )
    }

    /// Processes connection events even when no ordinary request is waiting, and sends the held
    /// second block requests that now qualify.
    /// Registers wakeups but does not demand an idle peer.
    pub(super) fn maintain(&mut self, cx: &mut Context<'_>) -> Result<(), BoxError> {
        // Update service and peer statuses.
        //
        // # Correctness
        //
        // All of the futures that receive a context from this method can wake the peer set owner
        // task. If there are no ready peers, and no new peers, network requests will pause until:
        // - an unready peer becomes ready, or
        // - a new peer arrives.

        // Drain stall and block-feedback events first, so disconnects free up slots and fresh
        // rates/misses are visible before routing.
        self.drain_stall_events(cx);
        self.drain_block_feedback(cx);

        // Check for new peers, and register a task wakeup when the next new peers arrive. New peers
        // can be infrequent if our connection slots are full, or we're connected to all
        // available/useful peers.
        let _poll_pending_or_ready: Poll<()> = self.poll_discover(cx)?;

        // This task doesn't provide new peers or newly ready peers.
        let _poll_pending: Poll<()> = self.poll_background_errors(cx)?;

        let _ = self.poll_peers(cx)?;
        self.prune_block_gossip_peers();
        self.publish_block_gossip_peers();

        // Peers leave the set through many paths. Pruning here, instead of at each
        // path, keeps per-peer maps bounded by the live peers.
        let (ready_services, cancel_handles) = (&self.ready_services, &self.cancel_handles);
        let is_live = |addr: &PeerSocketAddr| {
            ready_services.contains_key(addr) || cancel_handles.contains_key(addr)
        };
        if !self.find_response_stalls.is_empty() {
            self.find_response_stalls.retain(is_live);
        }
        if !self.peer_rates.is_empty() {
            self.peer_rates.retain(|addr, _| is_live(addr));
        }
        if !self.announced_block_misses.is_empty() {
            self.announced_block_misses.retain(|addr, _| is_live(addr));
        }

        // These metrics should run last, to report the most up-to-date information.
        self.log_peer_set_size();
        self.update_metrics();

        Ok(())
    }

    /// Checks whether a request can be routed now. [`Self::maintain`] must have run in the same
    /// poll, so its wakeups are registered when this returns `Poll::Pending`.
    ///
    /// Refreshes the inventory registry here rather than in maintenance, because only request
    /// routing reads it, and asks the crawler for more peers when none is ready.
    ///
    /// A syncing node also waits while the peers it downloads old blocks from are all busy,
    /// although pruned peers may be ready: see [`Self::awaits_archive_block_source`].
    pub(super) fn poll_routing_readiness(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), BoxError>> {
        let _poll_pending_or_ready: Poll<()> = self.inventory_registry.poll_inventory(cx)?;

        if self.ready_services.is_empty() || self.awaits_archive_block_source() {
            // # Correctness
            //
            // If the channel is full, drop the demand signal rather than waiting. If we waited
            // here, the crawler could deadlock sending a request to fetch more peers, because it
            // also empties the channel.
            trace!("no ready services for requests, sending demand signal");
            let _ = self.demand_signal.try_send(MorePeers);

            // # Correctness
            //
            // The current task must be scheduled for wakeup every time we return `Poll::Pending`.
            //
            // As long as there are unready or new peers, this task will run, because:
            // - a busy peer that this node waits to download old blocks from is an unready peer.
            // - `poll_discover` schedules this task for wakeup when new peers arrive.
            // - if there are unready peers, `poll_unready` or `poll_ready_peers` schedule this
            //   task for wakeup when peer services become ready.
            //
            // To avoid peers blocking on a full peer status/error channel:
            // - `poll_background_errors` schedules this task for wakeup when the peer status
            //   update task exits.
            return Poll::Pending;
        }

        Poll::Ready(Ok(()))
    }

    /// Asks the crawler for more peers when no connection can carry a block advert.
    ///
    /// Block advertisers wait for membership changes instead of request readiness, so this is
    /// their only demand signal.
    pub(super) fn demand_peers_if_none_advertisable(&mut self) {
        if self.block_gossip_peers.is_empty() {
            trace!("no advertisable peers, sending demand signal");
            let _ = self.demand_signal.try_send(MorePeers);
        }
    }

    /// Authorization sources watched by the owning driver: the ban list, and a minimum peer
    /// version tracker that reports version changes rather than every chain tip change.
    pub(super) fn authorization(&self) -> (BannedIps, MinimumPeerVersion<C>)
    where
        C: Clone,
    {
        (self.bans.clone(), self.minimum_peer_version.clone())
    }

    /// Drops the block gossip peers of connections that left the peer set, and marks the
    /// published peer list stale if any were dropped.
    fn prune_block_gossip_peers(&mut self) {
        let before = self.block_gossip_peers.len();
        let minimum = self.minimum_peer_version.current();
        self.block_gossip_peers.retain(|key, peer| {
            (self.ready_services.contains_key(key) || self.cancel_handles.contains_key(key))
                && !peer.sender.is_closed()
                && !self.bans.contains(key.ip())
                && peer.version >= minimum
        });
        self.block_gossip_peers_changed |= self.block_gossip_peers.len() != before;
    }

    /// Publishes the block gossip peers to block advertisers, if they changed.
    fn publish_block_gossip_peers(&mut self) {
        if std::mem::take(&mut self.block_gossip_peers_changed) {
            self.block_gossip_peers_tx
                .send_replace(self.block_gossip_peers.values().cloned().collect());
        }
    }

    /// Returns a block advertiser that reaches this peer set's connected peers, including busy
    /// ones, without waiting for the peer set to have a ready peer.
    pub(crate) fn block_advertiser(&self) -> crate::peer::block_gossip::BlockAdvertiser<C>
    where
        C: Clone,
    {
        crate::peer::block_gossip::BlockAdvertiser {
            peers: self.block_gossip_peers_tx.subscribe(),
            fanout: self.block_gossip_fanout,
            minimum_peer_version: self.minimum_peer_version.clone(),
            bans: self.bans.clone(),
            refresh: None,
        }
    }

    /// Given a number of ready peers calculate to how many of them Zebra will
    /// actually send the request to. Return this number.
    pub(crate) fn number_of_peers_to_broadcast(&self) -> usize {
        if self.network.is_regtest() {
            // In regtest, we broadcast to all peers, so that we can test the
            // peer set with a small number of peers.
            self.ready_services.len()
        } else {
            // We are currently sending broadcast messages to a third of the total peers.
            const PEER_FRACTION_TO_BROADCAST: usize = 3;

            // Round up, so that if we have one ready peer, it gets the request.
            self.ready_services
                .len()
                .div_ceil(PEER_FRACTION_TO_BROADCAST)
        }
    }

    /// Returns the list of addresses in the peer set.
    fn peer_set_addresses(&self) -> Vec<PeerSocketAddr> {
        self.ready_services
            .keys()
            .chain(self.cancel_handles.keys())
            .cloned()
            .collect()
    }

    /// Logs the peer set size, and any potential connectivity issues.
    fn log_peer_set_size(&mut self) {
        let ready_services_len = self.ready_services.len();
        let unready_services_len = self.unready_services.len();
        trace!(ready_peers = ?ready_services_len, unready_peers = ?unready_services_len);

        let now = Instant::now();

        // These logs are designed to be human-readable in a terminal, at the
        // default Zebra log level. If you need to know the peer set size for
        // every request, use the trace-level logs, or the metrics exporter.
        if let Some(last_peer_log) = self.last_peer_log {
            // Avoid duplicate peer set logs
            if now.duration_since(last_peer_log) < MIN_PEER_SET_LOG_INTERVAL {
                return;
            }
        } else {
            // Suppress initial logs until the peer set has started up.
            // There can be multiple initial requests before the first peer is
            // ready.
            self.last_peer_log = Some(now);
            return;
        }

        self.last_peer_log = Some(now);

        // Log potential duplicate connections.
        let peers = self.peer_set_addresses();

        // Check for duplicates by address and port: these are unexpected and represent a bug.
        let duplicates: Vec<PeerSocketAddr> = peers.iter().duplicates().cloned().collect();

        let mut peer_counts = peers.iter().counts();
        peer_counts.retain(|peer, _count| duplicates.contains(peer));

        if !peer_counts.is_empty() {
            let duplicate_connections: usize = peer_counts.values().sum();

            warn!(
                ?duplicate_connections,
                duplicated_peers = ?peer_counts.len(),
                peers = ?peers.len(),
                "duplicate peer connections in peer set"
            );
        }

        // Check for duplicates by address: these can happen if there are multiple nodes
        // behind a NAT or on a single server.
        let peers: Vec<IpAddr> = peers.iter().map(|addr| addr.ip()).collect();
        let duplicates: Vec<IpAddr> = peers.iter().duplicates().cloned().collect();

        let mut peer_counts = peers.iter().counts();
        peer_counts.retain(|peer, _count| duplicates.contains(peer));

        if !peer_counts.is_empty() {
            let duplicate_connections: usize = peer_counts.values().sum();

            info!(
                ?duplicate_connections,
                duplicated_peers = ?peer_counts.len(),
                peers = ?peers.len(),
                "duplicate IP addresses in peer set"
            );
        }

        // Only log connectivity warnings if all our peers are busy (or there are no peers).
        if ready_services_len > 0 {
            return;
        }

        let address_metrics = *self.address_metrics.borrow();
        if unready_services_len == 0 {
            warn!(
                ?address_metrics,
                "network request with no peer connections. Hint: check your network connection"
            );
        } else {
            info!(?address_metrics, "network request with no ready peers: finding more peers, waiting for {} peers to answer requests",
                  unready_services_len);
        }
    }

    /// Updates the peer set metrics.
    ///
    /// # Panics
    ///
    /// If the peer set size exceeds the connection limit.
    fn update_metrics(&self) {
        let num_ready = self.ready_services.len();
        let num_unready = self.unready_services.len();
        let num_peers = num_ready + num_unready;
        metrics::gauge!("pool.num_ready").set(num_ready as f64);
        metrics::gauge!("pool.num_unready").set(num_unready as f64);
        metrics::gauge!("zcash.net.peers").set(num_peers as f64);

        // Security: make sure we haven't exceeded the connection limit
        if num_peers > self.peerset_total_connection_limit {
            let address_metrics = *self.address_metrics.borrow();
            panic!(
                "unexpectedly exceeded configured peer set connection limit: \n\
                 peers: {num_peers:?}, ready: {num_ready:?}, unready: {num_unready:?}, \n\
                 address_metrics: {address_metrics:?}",
            );
        }
    }
}

impl<D, C> Service<Request> for PeerSet<D, C>
where
    D: Discover<Key = PeerSocketAddr, Service = LoadTrackedClient> + Unpin,
    D::Error: Into<BoxError>,
    C: ChainTip,
{
    type Response = Response;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.maintain(cx)?;
        self.poll_routing_readiness(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let fut = match req {
            Request::DisconnectPeer(addr) => {
                self.remove(&addr);
                async { Ok(Response::Nil) }.boxed()
            }
            // Only do inventory-aware routing on individual items.
            Request::BlocksByHash(ref hashes) | Request::BlocksByHashFrom { ref hashes, .. }
                if hashes.len() == 1 =>
            {
                let hash = InventoryHash::from(*hashes.iter().next().unwrap());
                self.route_inv(req, hash)
            }
            Request::TransactionsById(ref hashes)
            | Request::TransactionsByIdFrom {
                ids: ref hashes, ..
            } if hashes.len() == 1 => {
                let hash = InventoryHash::from(*hashes.iter().next().unwrap());
                self.route_inv(req, hash)
            }

            // Broadcast advertisements to lots of peers
            Request::AdvertiseTransactionIds(_, _) => self.route_broadcast(req),
            Request::AdvertiseBlock(_, _)
            | Request::AdvertiseBlockToAll(_)
            | Request::AdvertiseMinedBlock(_) => self.route_block_broadcast(req),

            // Choose a random less-loaded peer for all other requests
            _ => self.route_p2c(req),
        };
        self.update_metrics();

        fut
    }
}
