//! The `AddressBook` manages information about what peers exist, when they were
//! seen, and what services they provide.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use chrono::Utc;
use tokio::sync::watch;
use tracing::Span;

use zakura_chain::{parameters::Network, serialization::DateTime32};

use crate::{
    constants::{self, ADDR_RESPONSE_LIMIT_DENOMINATOR, MAX_ADDRS_IN_MESSAGE},
    meta_addr::MetaAddrChange,
    peer_registry::PeerRegistry,
    protocol::external::{canonical_ip, canonical_peer_addr, canonical_socket_addr},
    types::{MetaAddr, PeerServices},
    unconditional_peer_set::UnconditionalPeerSet,
    AddressBookPeers, PeerAddrState, PeerSocketAddr,
};

#[cfg(test)]
mod tests;

/// Read-only access to currently banned peer IP addresses.
#[derive(Clone, Debug)]
pub struct BannedIps {
    inner: Arc<RwLock<BanList>>,
    changes: tokio::sync::watch::Sender<()>,
}

impl Default for BannedIps {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            changes: tokio::sync::watch::channel(()).0,
        }
    }
}

/// A bounded FIFO list of banned peer IP addresses.
#[derive(Debug, Default)]
struct BanList {
    /// Banned IP addresses for fast membership checks.
    ips: HashSet<IpAddr>,

    /// Banned IP addresses in insertion order for FIFO eviction.
    insertion_order: VecDeque<IpAddr>,
}

impl BanList {
    /// Inserts `ip`, evicting the oldest IP when the list exceeds its bound.
    fn insert(&mut self, ip: IpAddr) {
        let ip = canonical_ip(ip);

        if !self.ips.insert(ip) {
            return;
        }

        self.insertion_order.push_back(ip);

        if self.ips.len() > constants::MAX_BANNED_IPS {
            let oldest = self
                .insertion_order
                .pop_front()
                .expect("ban order has an entry for every banned IP");

            self.ips.remove(&oldest);
        }
    }
}

impl BannedIps {
    /// Watches authorization changes without polling the ban list.
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<()> {
        self.changes.subscribe()
    }

    /// Bans `ip` unless `is_exempt` returns true, and notifies readers after releasing
    /// the list lock. Returns false when `is_exempt` stopped the ban.
    ///
    /// `is_exempt` runs while the list is locked, and so does the exemption in
    /// [`exempt_and_lift`](Self::exempt_and_lift). So a racing ban either sees the
    /// exemption, or lands before it and is lifted.
    fn insert_unless(&self, ip: IpAddr, is_exempt: impl FnOnce() -> bool) -> bool {
        let changed = {
            let mut list = self
                .inner
                .write()
                .expect("ban list lock should not be poisoned");
            if is_exempt() {
                return false;
            }
            let changed = !list.ips.contains(&canonical_ip(ip));
            list.insert(ip);
            changed
        };
        if changed {
            self.changes.send_replace(());
        }

        true
    }

    /// Runs `exempt`, then lifts the bans on `ips`, while holding the list lock, and
    /// notifies readers after releasing it.
    ///
    /// No ban can land between the exemption and the lifted bans, see
    /// [`insert_unless`](Self::insert_unless).
    pub(crate) fn exempt_and_lift(
        &self,
        exempt: impl FnOnce(),
        ips: impl IntoIterator<Item = IpAddr>,
    ) {
        let changed = {
            let mut list = self
                .inner
                .write()
                .expect("ban list lock should not be poisoned");
            exempt();

            let mut changed = false;
            for ip in ips {
                let ip = canonical_ip(ip);
                if list.ips.remove(&ip) {
                    list.insertion_order.retain(|banned_ip| *banned_ip != ip);
                    changed = true;
                }
            }
            changed
        };
        if changed {
            self.changes.send_replace(());
        }
    }

    /// Lifts the ban on `ip`, if it has one.
    #[cfg(test)]
    pub(crate) fn remove(&self, ip: IpAddr) {
        self.exempt_and_lift(|| {}, [ip]);
    }

    /// Returns whether `ip` is currently banned.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = canonical_ip(ip);

        self.inner
            .read()
            .expect("ban list lock should not be poisoned")
            .ips
            .contains(&ip)
    }

    /// Bans `ip`, whatever is exempt.
    #[cfg(test)]
    fn insert(&self, ip: IpAddr) {
        self.insert_unless(ip, || false);
    }

    #[cfg(test)]
    pub(crate) fn with_banned_ip(ip: IpAddr) -> Self {
        let bans = Self::default();
        bans.insert(ip);
        bans
    }
}

/// The number of peers in each [`PeerAddrState`].
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct StateCounts {
    responded: usize,
    never_attempted_gossiped: usize,
    failed: usize,
    attempt_pending: usize,
}

impl StateCounts {
    fn get(&self, state: PeerAddrState) -> usize {
        match state {
            PeerAddrState::Responded => self.responded,
            PeerAddrState::NeverAttemptedGossiped => self.never_attempted_gossiped,
            PeerAddrState::Failed => self.failed,
            PeerAddrState::AttemptPending => self.attempt_pending,
        }
    }

    fn get_mut(&mut self, state: PeerAddrState) -> &mut usize {
        match state {
            PeerAddrState::Responded => &mut self.responded,
            PeerAddrState::NeverAttemptedGossiped => &mut self.never_attempted_gossiped,
            PeerAddrState::Failed => &mut self.failed,
            PeerAddrState::AttemptPending => &mut self.attempt_pending,
        }
    }

    fn add(&mut self, state: PeerAddrState) {
        *self.get_mut(state) += 1;
    }

    fn remove(&mut self, state: PeerAddrState) {
        let count = self.get_mut(state);
        *count = count
            .checked_sub(1)
            .expect("every indexed peer was counted when it was inserted");
    }
}

/// Peer addresses indexed for direct lookup, priority order, and IP removal.
#[derive(Clone, Debug, Default)]
struct AddressBookIndex {
    /// Peer metadata keyed by its canonical listener address.
    by_addr: HashMap<PeerSocketAddr, MetaAddr>,

    /// Peer metadata in connection-attempt order.
    by_priority: BTreeSet<MetaAddr>,

    /// Canonical listener addresses grouped by canonical IP.
    by_ip: HashMap<IpAddr, HashSet<PeerSocketAddr>>,

    /// The number of indexed peers in each connection state.
    state_counts: StateCounts,
}

impl AddressBookIndex {
    fn len(&self) -> usize {
        self.by_addr.len()
    }

    /// Returns the number of peers whose last connection state is `state`.
    fn state_count(&self, state: PeerAddrState) -> usize {
        self.state_counts.get(state)
    }

    /// Returns the peers in the `Responded` state, in connection-attempt order.
    ///
    /// [`MetaAddr`]'s order puts `Responded` peers first, so this visits only
    /// those peers rather than the whole index.
    fn responded_values(&self) -> impl Iterator<Item = &MetaAddr> {
        self.by_priority
            .iter()
            .take_while(|peer| peer.last_connection_state == PeerAddrState::Responded)
    }

    fn get(&self, addr: &PeerSocketAddr) -> Option<MetaAddr> {
        self.by_addr.get(addr).copied()
    }

    fn insert(&mut self, meta_addr: MetaAddr) {
        let addr = meta_addr.addr;
        let previous = self.by_addr.insert(addr, meta_addr);
        if let Some(previous) = previous {
            assert!(
                self.by_priority.remove(&previous),
                "replaced peer must exist in the priority index"
            );
            self.state_counts.remove(previous.last_connection_state);
        } else {
            assert!(
                self.by_ip.entry(addr.ip()).or_default().insert(addr),
                "new peer must not exist in the IP index"
            );
        }

        assert!(
            self.by_priority.insert(meta_addr),
            "new peer metadata must be unique in the priority index"
        );
        self.state_counts.add(meta_addr.last_connection_state);
    }

    fn remove(&mut self, addr: &PeerSocketAddr) -> Option<MetaAddr> {
        let removed = self.by_addr.remove(addr)?;

        assert!(
            self.by_priority.remove(&removed),
            "removed peer must exist in the priority index"
        );
        self.state_counts.remove(removed.last_connection_state);

        let remove_ip = {
            let Some(same_ip_addrs) = self.by_ip.get_mut(&addr.ip()) else {
                panic!("removed peer IP must exist in the IP index");
            };
            assert!(
                same_ip_addrs.remove(addr),
                "removed peer must exist in the IP index"
            );
            same_ip_addrs.is_empty()
        };
        if remove_ip {
            self.by_ip.remove(&addr.ip());
        }

        Some(removed)
    }

    fn remove_ip(&mut self, ip: IpAddr) {
        let ip = canonical_ip(ip);
        let Some(addrs) = self.by_ip.remove(&ip) else {
            return;
        };

        for addr in addrs {
            let removed = self
                .by_addr
                .remove(&addr)
                .expect("IP index peer must exist in the address index");
            assert!(
                self.by_priority.remove(&removed),
                "IP index peer must exist in the priority index"
            );
            self.state_counts.remove(removed.last_connection_state);
        }
    }

    fn ordered_values(&self) -> impl DoubleEndedIterator<Item = &MetaAddr> {
        self.by_priority.iter()
    }

    #[cfg(test)]
    fn assert_consistent(&self) {
        assert_eq!(self.by_addr.len(), self.by_priority.len());
        assert_eq!(
            self.by_addr.len(),
            self.by_ip.values().map(HashSet::len).sum::<usize>()
        );

        for (addr, meta_addr) in &self.by_addr {
            assert_eq!(*addr, meta_addr.addr);
            assert!(self.by_priority.contains(meta_addr));
            assert!(self
                .by_ip
                .get(&addr.ip())
                .is_some_and(|same_ip_addrs| same_ip_addrs.contains(addr)));
        }

        for meta_addr in self.ordered_values() {
            assert_eq!(self.by_addr.get(&meta_addr.addr), Some(meta_addr));
        }

        for (ip, addrs) in &self.by_ip {
            for addr in addrs {
                assert_eq!(&addr.ip(), ip);
                assert!(self.by_addr.contains_key(addr));
            }
        }

        let peers: Vec<_> = self.ordered_values().copied().collect();
        assert!(peers.windows(2).all(|pair| pair[0] <= pair[1]));

        let mut recounted = StateCounts::default();
        for peer in &peers {
            recounted.add(peer.last_connection_state);
        }
        assert_eq!(self.state_counts, recounted);
        assert_eq!(
            self.responded_values().count(),
            recounted.responded,
            "responded peers must sort before every other peer"
        );
    }
}

/// A database of peer listener addresses, their advertised services, and
/// information on when they were last seen.
///
/// # Security
///
/// Address book state must be based on outbound connections to peers.
///
/// If the address book is updated incorrectly:
/// - malicious peers can interfere with other peers' `AddressBook` state,
///   or
/// - Zebra can advertise unreachable addresses to its own peers.
///
/// ## Adding Addresses
///
/// The address book should only contain Zcash listener port addresses from peers
/// on the configured network. These addresses can come from:
/// - DNS seeders
/// - addresses gossiped by other peers
/// - the canonical address (`Version.address_from`) provided by each peer,
///   particularly peers on inbound connections.
///
/// The remote addresses of inbound connections must not be added to the address
/// book, because they contain ephemeral outbound ports, not listener ports.
///
/// Isolated connections must not add addresses or update the address book.
///
/// ## Updating Address State
///
/// Updates to address state must be based on outbound connections to peers.
///
/// Updates must not be based on:
/// - the remote addresses of inbound connections, or
/// - the canonical address of any connection.
#[derive(Debug)]
pub struct AddressBook {
    /// Peer listener addresses, suitable for outbound connections,
    /// in connection attempt order.
    ///
    /// Some peers in this list might have open outbound or inbound connections.
    ///
    /// Direct, priority-ordered, and per-IP indexes are updated together.
    peers: AddressBookIndex,

    /// The address with a last_connection_state of [`PeerAddrState::Responded`] and
    /// the most recent `last_response` time by IP.
    ///
    /// This is used to avoid initiating outbound connections past [`Config::max_connections_per_ip`](crate::config::Config), and
    /// currently only supports a `max_connections_per_ip` of 1, and must be `None` when used with a greater `max_connections_per_ip`.
    // TODO: Replace with `by_ip: HashMap<IpAddr, BTreeMap<DateTime32, MetaAddr>>` to support configured `max_connections_per_ip` greater than 1
    most_recent_by_ip: Option<HashMap<IpAddr, MetaAddr>>,

    /// A list of banned addresses, with the time they were banned.
    bans_by_ip: BannedIps,

    /// The peers that are never scored or banned.
    ///
    /// The address book drops misbehavior changes for these peers, and leaves the IPs
    /// that Zakura dials to stay connected to them out of
    /// [`reconnection_peers`](Self::reconnection_peers).
    unconditional_peers: UnconditionalPeerSet,

    /// The local listener address.
    local_listener: SocketAddr,

    /// The services advertised for our own [`local_listener`](Self::local_listener)
    /// address when it is gossiped to peers.
    ///
    /// This must match the services advertised during the handshake, so a node
    /// that does not advertise [`PeerServices::NODE_NETWORK`] (for example, a
    /// pruned node) does not gossip itself as a full node.
    local_listener_services: PeerServices,

    /// The configured Zcash network.
    network: Network,

    /// Active connection metadata, stored outside address discovery state.
    peer_registry: Option<PeerRegistry>,

    /// The maximum number of addresses in the address book.
    ///
    /// Always set to [`MAX_ADDRS_IN_ADDRESS_BOOK`](constants::MAX_ADDRS_IN_ADDRESS_BOOK),
    /// in release builds. Lower values are used during testing.
    addr_limit: usize,

    /// The span for operations on this address book.
    span: Span,

    /// Whether operational log fields expose legacy peer addresses.
    expose_peer_addresses: bool,

    /// A channel used to send the latest address book metrics.
    address_metrics_tx: watch::Sender<AddressMetrics>,

    /// Whether the address book changed since metrics were last published.
    address_metrics_dirty: bool,

    #[cfg(test)]
    address_metrics_update_count: usize,

    /// The last time we logged a message about the address metrics.
    last_address_log: Option<Instant>,
}

/// Metrics about the states of the addresses in an [`AddressBook`].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct AddressMetrics {
    /// The number of addresses in the `Responded` state.
    pub responded: usize,

    /// The number of addresses in the `NeverAttemptedGossiped` state.
    pub never_attempted_gossiped: usize,

    /// The number of addresses in the `Failed` state.
    pub failed: usize,

    /// The number of addresses in the `AttemptPending` state.
    pub attempt_pending: usize,

    /// The number of `Responded` addresses within the liveness limit.
    pub recently_live: usize,

    /// The number of `Responded` addresses outside the liveness limit.
    pub recently_stopped_responding: usize,

    /// The number of addresses in the address book, regardless of their states.
    pub num_addresses: usize,

    /// The maximum number of addresses in the address book.
    pub address_limit: usize,
}

#[allow(clippy::len_without_is_empty)]
impl AddressBook {
    /// Construct an [`AddressBook`] with the given `local_listener` on `network`.
    ///
    /// Uses the supplied [`tracing::Span`] for address book operations.
    pub fn new(
        local_listener: SocketAddr,
        network: &Network,
        max_connections_per_ip: usize,
        span: Span,
    ) -> AddressBook {
        let constructor_span = span.clone();
        let _guard = constructor_span.enter();

        let instant_now = Instant::now();
        let chrono_now = Utc::now();

        // The default value is correct for an empty address book,
        // and it gets replaced by `update_metrics` anyway.
        let (address_metrics_tx, _address_metrics_rx) = watch::channel(AddressMetrics::default());

        // Avoid initiating outbound handshakes when max_connections_per_ip is 1.
        let should_limit_outbound_conns_per_ip = max_connections_per_ip == 1;
        let mut new_book = AddressBook {
            peers: AddressBookIndex::default(),
            local_listener: canonical_socket_addr(local_listener),
            // Default to full-node services; callers that advertise different
            // services set them with `with_local_listener_services`.
            local_listener_services: PeerServices::NODE_NETWORK,
            network: network.clone(),
            peer_registry: None,
            addr_limit: constants::MAX_ADDRS_IN_ADDRESS_BOOK,
            span,
            expose_peer_addresses: false,
            address_metrics_tx,
            address_metrics_dirty: false,
            #[cfg(test)]
            address_metrics_update_count: 0,
            last_address_log: None,
            most_recent_by_ip: should_limit_outbound_conns_per_ip.then(HashMap::new),
            bans_by_ip: Default::default(),
            unconditional_peers: UnconditionalPeerSet::default(),
        };

        new_book.update_metrics(instant_now, chrono_now);
        new_book
    }

    /// Sets the services advertised for our own gossiped listener address.
    ///
    /// These must match the services advertised during the handshake.
    #[must_use]
    pub fn with_local_listener_services(mut self, services: PeerServices) -> Self {
        self.local_listener_services = services;
        self
    }

    /// Sets whether operational logs expose legacy peer addresses.
    #[must_use]
    pub(crate) fn with_expose_peer_addresses(mut self, expose_peer_addresses: bool) -> Self {
        self.expose_peer_addresses = expose_peer_addresses;
        self
    }

    /// Sets the peers that are never scored or banned.
    #[must_use]
    pub(crate) fn with_unconditional_peers(
        mut self,
        unconditional_peers: impl Into<UnconditionalPeerSet>,
    ) -> Self {
        self.unconditional_peers = unconditional_peers.into();
        self
    }

    /// Returns the peers that are never scored or banned. The returned set follows the
    /// addresses that unconditional peer names resolve to.
    pub fn unconditional_peers(&self) -> UnconditionalPeerSet {
        self.unconditional_peers.clone()
    }

    /// Attach the active peer registry used by local RPC diagnostics.
    #[must_use]
    pub(crate) fn with_peer_registry(mut self, peer_registry: PeerRegistry) -> Self {
        self.peer_registry = Some(peer_registry);
        self
    }

    /// Construct an [`AddressBook`] with the given `local_listener`, `network`,
    /// `addr_limit`, [`tracing::Span`], and addresses.
    ///
    /// `addr_limit` is enforced by this method, and by [`AddressBook::update`].
    ///
    /// If there are multiple [`MetaAddr`]s with the same address,
    /// an arbitrary address is inserted into the address book,
    /// and the rest are dropped.
    ///
    /// This constructor can be used to break address book invariants,
    /// so it should only be used in tests.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn new_with_addrs(
        local_listener: SocketAddr,
        network: &Network,
        max_connections_per_ip: usize,
        addr_limit: usize,
        span: Span,
        addrs: impl IntoIterator<Item = MetaAddr>,
    ) -> AddressBook {
        let constructor_span = span.clone();
        let _guard = constructor_span.enter();

        let instant_now = Instant::now();
        let chrono_now = Utc::now();

        // The maximum number of addresses should be always greater than 0
        assert!(addr_limit > 0);

        let mut new_book = AddressBook::new(local_listener, network, max_connections_per_ip, span);
        new_book.addr_limit = addr_limit;

        let addrs = addrs
            .into_iter()
            .map(|mut meta_addr| {
                meta_addr.addr = canonical_peer_addr(meta_addr.addr);
                meta_addr
            })
            .filter(|meta_addr| meta_addr.address_is_valid_for_outbound(network))
            .map(|meta_addr| (meta_addr.addr, meta_addr));

        for (socket_addr, meta_addr) in addrs {
            // overwrite any duplicate addresses
            new_book.peers.insert(meta_addr);
            // Add the address to `most_recent_by_ip` if it has responded
            if new_book.should_update_most_recent_by_ip(meta_addr) {
                new_book
                    .most_recent_by_ip
                    .as_mut()
                    .expect("should be some when should_update_most_recent_by_ip is true")
                    .insert(socket_addr.ip(), meta_addr);
            }
            // exit as soon as we get enough addresses
            if new_book.peers.len() >= addr_limit {
                break;
            }
        }

        new_book.update_metrics(instant_now, chrono_now);
        new_book
    }

    /// Return a watch channel for the address book metrics.
    ///
    /// The metrics in the watch channel are only updated when the address book updates,
    /// so they can be significantly outdated if Zebra is disconnected or hung.
    ///
    /// The current metrics value is marked as seen.
    /// So `Receiver::changed` will only return after the next address book update.
    pub fn address_metrics_watcher(&self) -> watch::Receiver<AddressMetrics> {
        self.address_metrics_tx.subscribe()
    }

    /// Set the local listener address. Only for use in tests.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn set_local_listener(&mut self, addr: SocketAddr) {
        self.local_listener = addr;
    }

    /// Get the local listener address.
    ///
    /// This address contains minimal state, but it is not sanitized.
    pub fn local_listener_meta_addr(&self, now: chrono::DateTime<Utc>) -> MetaAddr {
        let now: DateTime32 = now.try_into().expect("will succeed until 2038");

        MetaAddr::new_local_listener_change(self.local_listener, self.local_listener_services)
            .local_listener_into_new_meta_addr(now)
    }

    /// Get the local listener [`SocketAddr`].
    pub fn local_listener_socket_addr(&self) -> SocketAddr {
        self.local_listener
    }

    /// Get the active addresses in `self` in random order with sanitized timestamps,
    /// including our local listener address.
    ///
    /// Limited to the number of peer addresses Zebra should give out per `GetAddr` request.
    pub fn fresh_get_addr_response(&self) -> Vec<MetaAddr> {
        let now = Utc::now();
        let mut peers = self.sanitized(now);
        let address_limit = peers.len().div_ceil(ADDR_RESPONSE_LIMIT_DENOMINATOR);
        peers.truncate(MAX_ADDRS_IN_MESSAGE.min(address_limit));

        peers
    }

    /// Get the active addresses in `self` in random order with sanitized timestamps,
    /// including our local listener address.
    pub(crate) fn sanitized(&self, now: chrono::DateTime<Utc>) -> Vec<MetaAddr> {
        use rand_10::seq::SliceRandom;
        let _guard = self.span.enter();

        let mut peers = self.peers.clone();

        // Unconditionally add our local listener address to the advertised peers,
        // to replace any self-connection failures. The address book and change
        // constructors make sure that the SocketAddr is canonical.
        let local_listener = self.local_listener_meta_addr(now);
        peers.insert(local_listener);

        // Then sanitize and shuffle
        let mut peers: Vec<MetaAddr> = peers
            .ordered_values()
            .filter_map(|meta_addr| meta_addr.sanitize(&self.network))
            // # Security
            //
            // Remove peers that:
            //   - last responded more than three hours ago, or
            //   - haven't responded yet but were reported last seen more than three hours ago
            //
            // This prevents Zebra from gossiping nodes that are likely unreachable. Gossiping such
            // nodes impacts the network health, because connection attempts end up being wasted on
            // peers that are less likely to respond.
            .filter(|addr| addr.is_active_for_gossip(now))
            .collect();

        peers.shuffle(&mut rand_10::rng());

        peers
    }

    /// Get active outbound listener addresses in measured RTT order, followed by
    /// shuffled addresses without a measurement, excluding our local listener.
    pub fn cacheable(&self, now: chrono::DateTime<Utc>) -> Vec<MetaAddr> {
        let _guard = self.span.enter();

        let peers = self.peers.clone();

        // Get peers in preferred order, then keep the recently active ones
        let mut eligible: Vec<_> = peers
            .ordered_values()
            // # Security
            //
            // Remove peers that:
            //   - last responded more than three hours ago, or
            //   - haven't responded yet but were reported last seen more than three hours ago
            //
            // This prevents Zebra from caching nodes that are likely unreachable,
            // which improves startup time and reliability.
            .filter(|addr| addr.is_active_for_gossip(now))
            // An inbound peer's entry holds its ephemeral source port, not a listener.
            .filter(|addr| !addr.is_inbound())
            // Address-only persistence must not forget a known rejection.
            .filter(|addr| addr.last_known_info_is_valid_for_outbound(&self.network))
            .cloned()
            .collect();

        // Keep measured RTT preference and stable measured ties. Unmeasured peers
        // have no trusted ranking: gossip timestamps and address values must not
        // decide which ones survive cache truncation.
        eligible.sort_by_key(|peer| (peer.rtt().is_none(), peer.rtt()));
        let first_unmeasured = eligible.partition_point(|peer| peer.rtt().is_some());
        use rand_10::seq::SliceRandom;
        eligible[first_unmeasured..].shuffle(&mut rand_10::rng());
        eligible
    }

    /// Look up `addr` in the address book, and return its [`MetaAddr`].
    ///
    /// Converts `addr` to a canonical address before looking it up.
    pub fn get(&mut self, addr: PeerSocketAddr) -> Option<MetaAddr> {
        let addr = canonical_peer_addr(*addr);
        self.peers.get(&addr)
    }

    /// Returns true if `updated` needs to be applied to the recent outbound peer connection IP cache.
    ///
    /// Checks if there are no existing entries in the address book with this IP,
    /// or if `updated` has a more recent `last_response` requiring the outbound connector to wait
    /// longer before initiating handshakes with peers at this IP.
    ///
    /// This code only needs to check a single cache entry, rather than the entire address book,
    /// because other code maintains these invariants:
    /// - `last_response` times for an entry can only increase.
    /// - this is the only field checked by `has_connection_recently_responded()`
    ///
    /// See [`AddressBook::is_ready_for_connection_attempt_with_ip`] for more details.
    fn should_update_most_recent_by_ip(&self, updated: MetaAddr) -> bool {
        let Some(most_recent_by_ip) = self.most_recent_by_ip.as_ref() else {
            return false;
        };

        if let Some(previous) = most_recent_by_ip.get(&updated.addr.ip()) {
            updated.last_connection_state == PeerAddrState::Responded
                && updated.last_response() > previous.last_response()
        } else {
            updated.last_connection_state == PeerAddrState::Responded
        }
    }

    /// Returns true if `addr` is the latest entry for its IP, which is stored in `most_recent_by_ip`.
    /// The entry is checked for an exact match to the IP and port of `addr`.
    fn should_remove_most_recent_by_ip(&self, addr: PeerSocketAddr) -> bool {
        let Some(most_recent_by_ip) = self.most_recent_by_ip.as_ref() else {
            return false;
        };

        if let Some(previous) = most_recent_by_ip.get(&addr.ip()) {
            previous.addr == addr
        } else {
            false
        }
    }

    /// Apply `change` to the address book, returning the updated `MetaAddr`,
    /// if the change was valid.
    ///
    /// # Correctness
    ///
    /// All changes should go through `update`, so that the address book
    /// only contains valid outbound addresses.
    ///
    /// Change addresses must be canonical `PeerSocketAddr`s. This makes sure that
    /// each address book entry has a unique IP address.
    ///
    /// # Security
    ///
    /// This function must apply every attempted, responded, and failed change
    /// to the address book. This prevents rapid reconnections to the same peer.
    ///
    /// As an exception, this function can ignore all changes for specific
    /// [`PeerSocketAddr`]s. Ignored addresses will never be used to connect to
    /// peers.
    ///
    /// It also drops misbehavior changes for configured unconditional peers,
    /// which are never scored or banned but can still be connected.
    pub fn update(&mut self, change: MetaAddrChange) -> Option<MetaAddr> {
        let updated = self.update_inner(change);
        self.publish_metrics();
        updated
    }

    /// Apply `change` without immediately publishing address book metrics.
    #[allow(clippy::unwrap_in_result)]
    fn update_inner(&mut self, change: MetaAddrChange) -> Option<MetaAddr> {
        let addr_label = change.addr().addr_label(self.expose_peer_addresses);

        if self.bans_by_ip.contains(change.addr().ip()) {
            tracing::warn!(
                peer = %addr_label,
                ?change,
                "attempted to add a banned peer addr to address book"
            );
            return None;
        }

        if matches!(change, MetaAddrChange::UpdateMisbehavior { .. })
            && self.unconditional_peers.contains(change.addr().ip())
        {
            debug!(
                peer = %addr_label,
                score_increment = change.misbehavior_score(),
                "ignored misbehavior report for an unconditional peer"
            );
            metrics::counter!("address_book.misbehavior.unconditional_ignored.total").increment(1);
            return None;
        }

        let previous = self.get(change.addr());

        let _guard = self.span.enter();

        let instant_now = Instant::now();
        let chrono_now = Utc::now();

        let updated = change.apply_to_meta_addr(previous, instant_now, chrono_now);

        trace!(
            peer = %addr_label,
            ?change,
            ?updated,
            ?previous,
            total_peers = self.peers.len(),
            recent_peers = self.recently_live_peers(chrono_now).len(),
            "calculated updated address book entry",
        );

        if let Some(updated) = updated {
            if updated.misbehavior() >= constants::MAX_PEER_MISBEHAVIOR_SCORE {
                // Ban and skip outbound connections with excessively misbehaving peers.
                let banned_ip = updated.addr.ip();

                // A name lookup can exempt this address after the check above.
                let is_banned = self
                    .bans_by_ip
                    .insert_unless(banned_ip, || self.unconditional_peers.contains(banned_ip));
                if !is_banned {
                    debug!(
                        peer = %addr_label,
                        "ignored a ban for a peer that became unconditional"
                    );
                    metrics::counter!("address_book.misbehavior.unconditional_ignored.total")
                        .increment(1);
                    return None;
                }

                // `most_recent_by_ip` is only populated when
                // `max_connections_per_ip == 1`. The ban path runs for any
                // configured value, so we must guard the optional cache rather
                // than unwrap it.
                if let Some(most_recent_by_ip) = self.most_recent_by_ip.as_mut() {
                    most_recent_by_ip.remove(&banned_ip);
                }

                self.peers.remove_ip(banned_ip);
                self.address_metrics_dirty = true;

                warn!(
                    peer = %addr_label,
                    ?updated,
                    total_peers = self.peers.len(),
                    recent_peers = self.recently_live_peers(chrono_now).len(),
                    "banned ip and removed banned peer addresses from address book",
                );

                return None;
            }

            // Ignore invalid outbound addresses.
            // (Inbound connections can be monitored via Zebra's metrics.)
            if !updated.address_is_valid_for_outbound(&self.network) {
                return None;
            }

            // Ignore invalid outbound services and other info,
            // but only if the peer has never been attempted.
            //
            // Otherwise, if we got the info directly from the peer,
            // store it in the address book, so we know not to reconnect.
            if !updated.last_known_info_is_valid_for_outbound(&self.network)
                && updated.last_connection_state.is_never_attempted()
            {
                return None;
            }

            self.peers.insert(updated);
            self.address_metrics_dirty = true;

            // Add the address to `most_recent_by_ip` if it sent the most recent
            // response Zebra has received from this IP.
            if self.should_update_most_recent_by_ip(updated) {
                self.most_recent_by_ip
                    .as_mut()
                    .expect("should be some when should_update_most_recent_by_ip is true")
                    .insert(updated.addr.ip(), updated);
            }

            debug!(
                peer = %addr_label,
                ?change,
                ?updated,
                ?previous,
                total_peers = self.peers.len(),
                recent_peers = self.recently_live_peers(chrono_now).len(),
                "updated address book entry",
            );

            // Security: Limit the number of peers in the address book.
            //
            // We only delete outdated peers when we have too many peers.
            // If we deleted them as soon as they became too old,
            // then other peers could re-insert them into the address book.
            // And we would start connecting to those outdated peers again,
            // ignoring the age limit in [`MetaAddr::is_probably_reachable`].
            while self.peers.len() > self.addr_limit {
                let surplus_peer = self
                    .peers()
                    .next_back()
                    .expect("just checked there is at least one peer");

                self.peers.remove(&surplus_peer.addr);

                // Check if this surplus peer's addr matches that in `most_recent_by_ip`
                // for this the surplus peer's ip to remove it there as well.
                if self.should_remove_most_recent_by_ip(surplus_peer.addr) {
                    self.most_recent_by_ip
                        .as_mut()
                        .expect("should be some when should_remove_most_recent_by_ip is true")
                        .remove(&surplus_peer.addr.ip());
                }

                debug!(
                    surplus = ?surplus_peer,
                    ?updated,
                    total_peers = self.peers.len(),
                    recent_peers = self.recently_live_peers(chrono_now).len(),
                    "removed surplus address book entry",
                );
            }

            assert!(self.len() <= self.addr_limit);
        }

        updated
    }

    /// Return an iterator over all peers.
    ///
    /// Returns peers in reconnection attempt order, including recently connected peers.
    pub fn peers(&'_ self) -> impl DoubleEndedIterator<Item = MetaAddr> + '_ {
        let _guard = self.span.enter();
        self.peers.ordered_values().cloned()
    }

    /// Is this IP ready for a new outbound connection attempt?
    /// Checks if the outbound connection with the most recent response at this IP has recently responded.
    ///
    /// Note: last_response times may remain live for a long time if the local clock is changed to an earlier time.
    fn is_ready_for_connection_attempt_with_ip(
        &self,
        ip: &IpAddr,
        chrono_now: chrono::DateTime<Utc>,
    ) -> bool {
        let Some(most_recent_by_ip) = self.most_recent_by_ip.as_ref() else {
            // if we're not checking IPs, any connection is allowed
            return true;
        };
        let Some(same_ip_peer) = most_recent_by_ip.get(ip) else {
            // If there's no entry for this IP, any connection is allowed
            return true;
        };
        !same_ip_peer.has_connection_recently_responded(chrono_now)
    }

    /// Return an iterator over peers that are due for a reconnection attempt,
    /// in reconnection attempt order.
    ///
    /// Leaves out every address on the IP of a dialed unconditional peer, or of an address
    /// that a listed name resolves to. Zakura keeps that peer connected on a slot of its
    /// own, and a connection to another node on its IP could take the room under
    /// `max_connections_per_ip` that the peer needs.
    pub fn reconnection_peers(
        &'_ self,
        instant_now: Instant,
        chrono_now: chrono::DateTime<Utc>,
    ) -> impl DoubleEndedIterator<Item = MetaAddr> + '_ {
        let _guard = self.span.enter();

        // Skip live peers, and peers pending a reconnect attempt.
        // The peers are already stored in sorted order.
        self.peers
            .ordered_values()
            .filter(move |peer| {
                // Inbound entries stay in the book for liveness, bans, and per-IP dial
                // limits, but their ephemeral source ports are not listeners to dial.
                !peer.is_inbound()
                    && !self.unconditional_peers.is_kept_connected(peer.addr.ip())
                    && peer.is_ready_for_connection_attempt(instant_now, chrono_now, &self.network)
                    && self.is_ready_for_connection_attempt_with_ip(&peer.addr.ip(), chrono_now)
            })
            .cloned()
    }

    /// Return an iterator over all the peers in `state`,
    /// in reconnection attempt order, including recently connected peers.
    pub fn state_peers(
        &'_ self,
        state: PeerAddrState,
    ) -> impl DoubleEndedIterator<Item = MetaAddr> + '_ {
        let _guard = self.span.enter();

        self.peers
            .ordered_values()
            .filter(move |peer| peer.last_connection_state == state)
            .cloned()
    }

    /// Return an iterator over peers that might be connected,
    /// in reconnection attempt order.
    pub fn maybe_connected_peers(
        &'_ self,
        instant_now: Instant,
        chrono_now: chrono::DateTime<Utc>,
    ) -> impl DoubleEndedIterator<Item = MetaAddr> + '_ {
        let _guard = self.span.enter();

        self.peers
            .ordered_values()
            .filter(move |peer| {
                !peer.is_ready_for_connection_attempt(instant_now, chrono_now, &self.network)
            })
            .cloned()
    }

    /// Returns banned IP addresses.
    pub fn bans(&self) -> BannedIps {
        self.bans_by_ip.clone()
    }

    /// Clones this address book with an independent, empty ban list.
    #[cfg(feature = "internal-bench")]
    #[doc(hidden)]
    pub fn clone_with_fresh_bans_for_benchmark(&self) -> Self {
        let mut address_book = self.clone();
        address_book.bans_by_ip = BannedIps::default();
        address_book
    }

    /// Returns the number of entries in this address book.
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Returns metrics for the addresses in this address book.
    /// Only for use in tests.
    ///
    /// # Correctness
    ///
    /// Use [`AddressBook::address_metrics_watcher`] in production code,
    /// to avoid deadlocks.
    #[cfg(test)]
    pub fn address_metrics(&self, now: chrono::DateTime<Utc>) -> AddressMetrics {
        self.address_metrics_internal(now)
    }

    /// Returns metrics for the addresses in this address book.
    ///
    /// # Correctness
    ///
    /// External callers should use [`AddressBook::address_metrics_watcher`]
    /// in production code, to avoid deadlocks.
    /// (Using the watch channel receiver does not lock the address book mutex.)
    fn address_metrics_internal(&self, now: chrono::DateTime<Utc>) -> AddressMetrics {
        let responded = self.peers.state_count(PeerAddrState::Responded);
        let never_attempted_gossiped = self
            .peers
            .state_count(PeerAddrState::NeverAttemptedGossiped);
        let failed = self.peers.state_count(PeerAddrState::Failed);
        let attempt_pending = self.peers.state_count(PeerAddrState::AttemptPending);

        let recently_live = self.recently_live(now).count();
        let recently_stopped_responding = responded
            .checked_sub(recently_live)
            .expect("all recently live peers must have responded");

        let num_addresses = self.len();

        AddressMetrics {
            responded,
            never_attempted_gossiped,
            failed,
            attempt_pending,
            recently_live,
            recently_stopped_responding,
            num_addresses,
            address_limit: self.addr_limit,
        }
    }

    /// Returns the peers that have responded recently, in connection-attempt order.
    fn recently_live(&self, now: chrono::DateTime<Utc>) -> impl Iterator<Item = &MetaAddr> {
        self.peers
            .responded_values()
            .filter(move |peer| peer.was_recently_live(now))
    }

    /// Publish metrics if the address book changed since the previous update.
    fn publish_metrics(&mut self) {
        if !self.address_metrics_dirty {
            return;
        }

        self.address_metrics_dirty = false;
        self.update_metrics(Instant::now(), Utc::now());
    }

    /// Update the metrics for this address book.
    fn update_metrics(&mut self, instant_now: Instant, chrono_now: chrono::DateTime<Utc>) {
        let _guard = self.span.enter();

        #[cfg(test)]
        {
            self.address_metrics_update_count += 1;
        }

        let m = self.address_metrics_internal(chrono_now);

        // Ignore errors: we don't care if any receivers are listening.
        let _ = self.address_metrics_tx.send(m);

        // TODO: rename to address_book.[state_name]
        metrics::gauge!("candidate_set.responded").set(m.responded as f64);
        metrics::gauge!("candidate_set.gossiped").set(m.never_attempted_gossiped as f64);
        metrics::gauge!("candidate_set.failed").set(m.failed as f64);
        metrics::gauge!("candidate_set.pending").set(m.attempt_pending as f64);

        // TODO: rename to address_book.responded.recently_live
        metrics::gauge!("candidate_set.recently_live").set(m.recently_live as f64);
        // TODO: rename to address_book.responded.stopped_responding
        metrics::gauge!("candidate_set.disconnected").set(m.recently_stopped_responding as f64);

        std::mem::drop(_guard);
        self.log_metrics(&m, instant_now);
    }

    /// Log metrics for this address book
    fn log_metrics(&mut self, m: &AddressMetrics, now: Instant) {
        let _guard = self.span.enter();

        trace!(
            address_metrics = ?m,
        );

        if m.responded > 0 {
            return;
        }

        // These logs are designed to be human-readable in a terminal, at the
        // default Zebra log level. If you need to know address states for
        // every request, use the trace-level logs, or the metrics exporter.
        if let Some(last_address_log) = self.last_address_log {
            // Avoid duplicate address logs
            if now.saturating_duration_since(last_address_log).as_secs() < 60 {
                return;
            }
        } else {
            // Suppress initial logs until the peer set has started up.
            // There can be multiple address changes before the first peer has
            // responded.
            self.last_address_log = Some(now);
            return;
        }

        self.last_address_log = Some(now);
        // if all peers have failed
        if m.responded + m.attempt_pending + m.never_attempted_gossiped == 0 {
            warn!(
                address_metrics = ?m,
                "all peer addresses have failed. Hint: check your network connection"
            );
        } else {
            info!(
                address_metrics = ?m,
                "no active peer connections: trying gossiped addresses"
            );
        }
    }
}

impl AddressBookPeers for AddressBook {
    fn recently_live_peers(&self, now: chrono::DateTime<Utc>) -> Vec<MetaAddr> {
        let _guard = self.span.enter();

        self.recently_live(now).copied().collect()
    }

    fn connected_peers(&self) -> Option<Vec<crate::ConnectedPeer>> {
        self.peer_registry
            .as_ref()
            .map(PeerRegistry::connected_peers)
    }

    fn add_peer(&mut self, peer: PeerSocketAddr) -> bool {
        if self.get(peer).is_some() {
            // Peer already exists in the address book, so we don't need to add it again.
            return false;
        }
        self.update(MetaAddr::new_initial_peer(peer)).is_some()
    }
}

impl AddressBookPeers for Arc<Mutex<AddressBook>> {
    fn recently_live_peers(&self, now: chrono::DateTime<Utc>) -> Vec<MetaAddr> {
        self.lock()
            .expect("panic in a previous thread that was holding the mutex")
            .recently_live_peers(now)
    }

    fn connected_peers(&self) -> Option<Vec<crate::ConnectedPeer>> {
        self.lock()
            .expect("panic in a previous thread that was holding the mutex")
            .connected_peers()
    }

    fn add_peer(&mut self, peer: PeerSocketAddr) -> bool {
        self.lock()
            .expect("panic in a previous thread that was holding the mutex")
            .add_peer(peer)
    }
}

impl Extend<MetaAddrChange> for AddressBook {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = MetaAddrChange>,
    {
        // Publish once after the full batch instead of scanning the address
        // book after every accepted change.
        for change in iter.into_iter() {
            self.update_inner(change);
        }

        self.publish_metrics();
    }
}

impl Clone for AddressBook {
    /// Clone the addresses, address limit, local listener address, and span.
    ///
    /// Cloned address books have a separate metrics struct watch channel, and an empty last address log.
    ///
    /// All address books update the same prometheus metrics.
    fn clone(&self) -> AddressBook {
        // The existing metrics might be outdated, but we avoid calling `update_metrics`,
        // so we don't overwrite the prometheus metrics from the main address book.
        let (address_metrics_tx, _address_metrics_rx) =
            watch::channel(*self.address_metrics_tx.borrow());

        AddressBook {
            peers: self.peers.clone(),
            local_listener: self.local_listener,
            local_listener_services: self.local_listener_services,
            network: self.network.clone(),
            peer_registry: self.peer_registry.clone(),
            addr_limit: self.addr_limit,
            span: self.span.clone(),
            expose_peer_addresses: self.expose_peer_addresses,
            address_metrics_tx,
            address_metrics_dirty: false,
            #[cfg(test)]
            address_metrics_update_count: 0,
            last_address_log: None,
            most_recent_by_ip: self.most_recent_by_ip.clone(),
            bans_by_ip: self.bans_by_ip.clone(),
            unconditional_peers: self.unconditional_peers.clone(),
        }
    }
}
