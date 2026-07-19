//! Configuration for Zakura's network communication.

use std::{
    collections::HashSet,
    fmt,
    io::{self, ErrorKind},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use indexmap::IndexSet;
use iroh::SecretKey;
use rand::rngs::OsRng;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use tokio::fs;

use tracing::Span;
use zakura_chain::{
    common::atomic_write,
    parameters::{
        testnet::{
            self, ConfiguredActivationHeights, ConfiguredCheckpoints, ConfiguredFundingStreams,
            ConfiguredLockboxDisbursement, RegtestParameters,
        },
        Magic, Network, NetworkKind,
    },
    work::difficulty::U256,
};

use crate::{
    constants::{
        DEFAULT_CRAWL_NEW_PEER_INTERVAL, DEFAULT_MAX_CONNS_PER_IP,
        DEFAULT_PEERSET_INITIAL_TARGET_SIZE, DNS_LOOKUP_TIMEOUT, INBOUND_PEER_LIMIT_MULTIPLIER,
        MAX_PEER_DISK_CACHE_SIZE, OUTBOUND_PEER_LIMIT_MULTIPLIER,
    },
    protocol::external::{canonical_peer_addr, canonical_socket_addr},
    zakura::ZakuraConfig,
    BoxError, PeerSocketAddr,
};

mod cache_dir;

#[cfg(test)]
mod tests;

pub use cache_dir::CacheDir;

pub(crate) use cache_dir::{
    default_network_identity_dir, zakura_node_secret_key_file_path as zakura_secret_key_file_path,
};

/// A sensitive iroh secret-key override for Zakura P2P node identity.
#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(transparent)]
pub struct ZakuraNodeSecretKey(String);

impl ZakuraNodeSecretKey {
    /// Returns the secret-key override.
    ///
    /// Callers should only expose this value to iroh identity construction or
    /// controlled persistence paths.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ZakuraNodeSecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ZakuraNodeSecretKey([redacted])")
    }
}

impl serde::Serialize for ZakuraNodeSecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("[redacted]")
    }
}

/// An error that can occur while resolving the Zakura node secret key.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ZakuraSecretKeyError {
    /// The configured `zakura_node_secret_key` is not a valid iroh secret key.
    #[error("configured zakura_node_secret_key is not a valid iroh secret key")]
    InvalidConfigured,
}

/// The number of times Zakura will retry each initial peer's DNS resolution,
/// before checking if any other initial peers have returned addresses.
///
/// After doing this number of retries of a failed single peer, Zakura will
/// check if it has enough peer addresses from other seed peers. If it has
/// enough addresses, it won't retry this peer again.
///
/// If the number of retries is `0`, other peers are checked after every successful
/// or failed DNS attempt.
const MAX_SINGLE_SEED_PEER_DNS_RETRIES: usize = 0;

/// The peer-to-peer stack Zakura runs, selected by `network.p2p_stack` in `zakurad.toml`.
///
/// [`P2pStack::Default`] is a placeholder for the configured network's binary default; it is
/// turned into one of the three real stacks by [`P2pStack::resolve`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum P2pStack {
    /// Follow the configured network's binary default, which can change between releases.
    #[default]
    Default,

    /// The legacy TCP Zcash P2P stack only.
    Legacy,

    /// The experimental native Zakura P2P v2 stack only.
    Zakura,

    /// Both stacks: mutually capable peers are upgraded to the experimental Zakura P2P v2
    /// stack, and the legacy stack stays available for peers that can't upgrade.
    Dual,
}

impl P2pStack {
    /// Resolves [`P2pStack::Default`] to the binary default for `network`, and returns every
    /// other stack unchanged.
    ///
    /// Mainnet defaults to [`P2pStack::Legacy`] until Zakura P2P v2 is proven there. Every other
    /// network defaults to [`P2pStack::Dual`], so Zakura P2P v2 gets exercised while legacy
    /// peers stay reachable.
    pub fn resolve(self, network: &Network) -> P2pStack {
        match self {
            P2pStack::Default if matches!(network, Network::Mainnet) => P2pStack::Legacy,
            P2pStack::Default => P2pStack::Dual,
            resolved => resolved,
        }
    }

    /// Returns `true` if this stack runs the legacy TCP Zcash P2P listener, dialer, and crawler.
    ///
    /// [`P2pStack::Default`] runs neither stack: resolve it with [`P2pStack::resolve`] first.
    fn runs_legacy(self) -> bool {
        matches!(self, P2pStack::Legacy | P2pStack::Dual)
    }

    /// Returns `true` if this stack runs the native Zakura P2P v2 endpoint.
    ///
    /// [`P2pStack::Default`] runs neither stack: resolve it with [`P2pStack::resolve`] first.
    fn runs_zakura(self) -> bool {
        matches!(self, P2pStack::Zakura | P2pStack::Dual)
    }
}

/// Configuration for networking code.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, default, into = "DConfig")]
pub struct Config {
    /// The address on which this node should listen for connections.
    ///
    /// Can be `address:port` or just `address`. If there is no configured
    /// port, Zakura will use the default port for the configured `network`.
    ///
    /// `address` can be an IP address or a DNS name. DNS names are
    /// only resolved once, when Zakura starts up.
    ///
    /// By default, Zakura listens on `[::]` (all IPv6 and IPv4 addresses).
    /// This enables dual-stack support, accepting both IPv4 and IPv6 connections.
    ///
    /// If a specific listener address is configured, Zakura will advertise
    /// it to other nodes. But by default, Zakura uses an unspecified address
    /// ("\[::\]:port"), which is not advertised to other nodes.
    ///
    /// Zakura does not currently support:
    /// - [Advertising a different external IP address #1890](https://github.com/ZcashFoundation/zebra/issues/1890), or
    /// - [Auto-discovering its own external IP address #1893](https://github.com/ZcashFoundation/zebra/issues/1893).
    ///
    /// However, other Zakura instances compensate for unspecified or incorrect
    /// listener addresses by adding the external IP addresses of peers to
    /// their address books.
    pub listen_addr: SocketAddr,

    /// The external address of this node if any.
    ///
    /// Zakura binds to `listen_addr`, but this can be an internal address if the node
    /// is behind a firewall, load balancer or NAT. This field can be used to
    /// advertise a different address to peers making it possible to receive inbound
    /// connections and contribute to the P2P network from behind a firewall, load balancer, or NAT.
    pub external_addr: Option<SocketAddr>,

    /// The network to connect to.
    pub network: Network,

    /// A list of initial peers for the peerset when operating on
    /// mainnet.
    pub initial_mainnet_peers: IndexSet<String>,

    /// A list of initial peers for the peerset when operating on
    /// testnet.
    pub initial_testnet_peers: IndexSet<String>,

    /// An optional root directory for storing cached peer address data.
    ///
    /// # Configuration
    ///
    /// Set to:
    /// - `true` to read and write peer addresses to disk using the default cache path,
    /// - `false` to disable reading and writing peer addresses to disk,
    /// - `'/custom/cache/directory'` to read and write peer addresses to a custom directory.
    ///
    /// By default, all Zakura instances run by the same user will share a single peer cache.
    /// If you use a custom cache path, you might also want to change `state.cache_dir`.
    ///
    /// # Functionality
    ///
    /// The peer cache is a list of the addresses of some recently useful peers.
    ///
    /// For privacy reasons, the cache does *not* include any other information about peers,
    /// such as when they were connected to the node.
    ///
    /// Deleting or modifying the peer cache can impact your node's:
    /// - reliability: if DNS or the Zcash DNS seeders are unavailable or broken
    /// - security: if DNS is compromised with malicious peers
    ///
    /// If you delete it, Zakura will replace it with a fresh set of peers from the DNS seeders.
    ///
    /// # Defaults
    ///
    /// The default directory is platform dependent, based on
    /// [`dirs::cache_dir()`](https://docs.rs/dirs/3.0.1/dirs/fn.cache_dir.html):
    ///
    /// |Platform | Value                                           | Example                              |
    /// | ------- | ----------------------------------------------- | ------------------------------------ |
    /// | Linux   | `$XDG_CACHE_HOME/zakura` or `$HOME/.cache/zakura` | `/home/alice/.cache/zakura`           |
    /// | macOS   | `$HOME/Library/Caches/zakura`                    | `/Users/Alice/Library/Caches/zakura`  |
    /// | Windows | `{FOLDERID_LocalAppData}\zakura`                 | `C:\Users\Alice\AppData\Local\zakura` |
    /// | Other   | `std::env::current_dir()/cache/zakura`           | `/cache/zakura`                       |
    ///
    /// # Security
    ///
    /// If you are running Zakura with elevated permissions ("root"), create the
    /// directory for this file before running Zakura, and make sure the Zakura user
    /// account has exclusive access to that directory, and other users can't modify
    /// its parent directories.
    ///
    /// # Implementation Details
    ///
    /// Each network has a separate peer list, which is updated regularly from the current
    /// address book. These lists are stored in `network/mainnet.peers` and
    /// `network/testnet.peers` files, underneath the `cache_dir` path.
    ///
    /// Previous peer lists are automatically loaded at startup, and used to populate the
    /// initial peer set and address book.
    pub cache_dir: CacheDir,

    /// The directory for long-term network identity secrets.
    ///
    /// The auto-generated Zakura iroh identity key is stored under this
    /// directory as `<network>.zakura-iroh-secret-key`. Keep this directory
    /// outside state or cache snapshot paths, or snapshots can clone the node's
    /// long-term P2P identity.
    ///
    /// The default is `~/.zakura`.
    pub identity_dir: PathBuf,

    /// An optional persistent iroh secret key for Zakura P2P identity.
    ///
    /// This is reserved for Zakura endpoint construction. If unset, a future Zakura endpoint
    /// implementation will generate an ed25519 iroh [`SecretKey`] on first use
    /// and persist it under [`identity_dir`](Self::identity_dir), outside Zakura's
    /// cache and state directories by default.
    ///
    /// This value is not used by the legacy TCP peer set.
    pub zakura_node_secret_key: Option<ZakuraNodeSecretKey>,

    /// The peer-to-peer stack Zakura runs.
    ///
    /// | `zakura.toml` value | Stack |
    /// |---|---|
    /// | `"default"` | The configured network's binary default |
    /// | `"legacy"` | The legacy TCP Zcash P2P stack only |
    /// | `"zakura"` | The experimental native Zakura P2P v2 stack only |
    /// | `"dual"` | Both stacks, enabling experimental v2 with legacy fallback |
    ///
    /// Leave this at `"default"` so Zakura can change the per-network default during upgrades.
    /// See [`P2pStack::resolve`] for the current defaults, and [`legacy_p2p`](Self::legacy_p2p)
    /// and [`v2_p2p`](Self::v2_p2p) for the resolved stack.
    pub p2p_stack: P2pStack,

    /// Native Zakura endpoint, connection, and bootstrap settings.
    ///
    /// When [`v2_p2p`](Self::v2_p2p) is false, these settings are parsed but no iroh endpoint is
    /// started. The total intended connection budget is roughly
    /// `peerset_initial_target_size + zakura.max_connections`; tune both together.
    pub zakura: ZakuraConfig,

    /// The initial target size for the peer set.
    ///
    /// Also used to limit the number of inbound and outbound connections made by Zakura,
    /// and the size of the cached peer list.
    ///
    /// If you have a slow network connection, and Zakura is having trouble
    /// syncing, try reducing the peer set size. You can also reduce the peer
    /// set size to reduce Zakura's bandwidth usage.
    pub peerset_initial_target_size: usize,

    /// How frequently we attempt to crawl the network to discover new peer
    /// addresses.
    ///
    /// Zakura asks its connected peers for more peer addresses:
    /// - regularly, every time `crawl_new_peer_interval` elapses, and
    /// - if the peer set is busy, and there aren't any peer addresses for the
    ///   next connection attempt.
    #[serde(with = "humantime_serde")]
    pub crawl_new_peer_interval: Duration,

    /// The maximum number of legacy TCP peer connections Zakura will keep for a given IP address
    /// before it drops any additional legacy peer connections with that IP.
    ///
    /// The default and minimum value are 1.
    ///
    /// Zakura uses [`ZakuraConfig::max_connections_per_ip`] for native v2 admission.
    ///
    /// # Security
    ///
    /// Increasing this config above 1 reduces Zakura's network security.
    ///
    /// If this config is greater than 1, Zakura can initiate multiple outbound handshakes to the same
    /// IP address.
    ///
    /// This config does not currently limit the number of inbound connections that Zakura will accept
    /// from the same IP address.
    ///
    /// If Zakura makes multiple inbound or outbound connections to the same IP, they will be dropped
    /// after the handshake, but before adding them to the peer set. The total numbers of inbound and
    /// outbound connections are also limited to a multiple of `peerset_initial_target_size`.
    pub max_connections_per_ip: usize,

    /// Exposes legacy peer IP addresses in peer activity logs, structured trace files, and
    /// Prometheus metric labels. This includes connected peers and candidate or address book
    /// entries.
    ///
    /// Literal addresses supplied in the node configuration can appear in startup logs and
    /// `seed` labels regardless of this setting.
    /// If `trace_dir` is configured in `[network.zakura]`, legacy sync diagnostics can write
    /// unredacted addresses to `legacy_sync.jsonl`.
    ///
    /// # Security
    ///
    /// Enabling this setting reveals peer topology in logs and trace files, and can create
    /// high-cardinality metric series. Restrict access to logs, trace directories, the metrics
    /// endpoint, and downstream monitoring systems.
    pub expose_peer_addresses: bool,
}

impl Config {
    /// The maximum number of outbound connections that Zakura will open at the same time.
    /// When this limit is reached, Zakura stops opening outbound connections.
    ///
    /// # Security
    ///
    /// See the note at [`INBOUND_PEER_LIMIT_MULTIPLIER`].
    ///
    /// # Performance
    ///
    /// Zakura's peer set should be limited to a reasonable size,
    /// to avoid queueing too many in-flight block downloads.
    /// A large queue of in-flight block downloads can choke a
    /// constrained local network connection.
    ///
    /// We assume that Zakura nodes have at least 10 Mbps bandwidth.
    /// Therefore, a maximum-sized block can take up to 2 seconds to
    /// download. So the initial outbound peer set adds up to 100 seconds worth
    /// of blocks to the queue. If Zakura has reached its outbound peer limit,
    /// that adds an extra 200 seconds of queued blocks.
    ///
    /// But the peer set for slow nodes is typically much smaller, due to
    /// the handshake RTT timeout. And Zakura responds to inbound request
    /// overloads by dropping peer connections.
    pub fn peerset_outbound_connection_limit(&self) -> usize {
        self.peerset_initial_target_size * OUTBOUND_PEER_LIMIT_MULTIPLIER
    }

    /// The maximum number of inbound connections that Zakura will accept at the same time.
    /// When this limit is reached, Zakura drops new inbound connections,
    /// without handshaking on them.
    ///
    /// # Security
    ///
    /// See the note at [`INBOUND_PEER_LIMIT_MULTIPLIER`].
    pub fn peerset_inbound_connection_limit(&self) -> usize {
        self.peerset_initial_target_size * INBOUND_PEER_LIMIT_MULTIPLIER
    }

    /// The maximum number of inbound and outbound connections that Zakura will have
    /// at the same time.
    pub fn peerset_total_connection_limit(&self) -> usize {
        self.peerset_outbound_connection_limit() + self.peerset_inbound_connection_limit()
    }

    /// Returns the initial seed peer hostnames for the configured network.
    pub fn initial_peer_hostnames(&self) -> IndexSet<String> {
        match &self.network {
            Network::Mainnet => self.initial_mainnet_peers.clone(),
            Network::Testnet(_params) => self.initial_testnet_peers.clone(),
        }
    }

    /// Resolve initial seed peer IP addresses, based on the configured network,
    /// and load cached peers from disk, if available.
    ///
    /// # Panics
    ///
    /// If a configured address is an invalid [`SocketAddr`] or DNS name.
    pub async fn initial_peers(&self) -> HashSet<PeerSocketAddr> {
        // TODO: do DNS and disk in parallel if startup speed becomes important
        let dns_peers = Config::resolve_peers(
            &self.initial_peer_hostnames().iter().cloned().collect(),
            self.expose_peer_addresses,
        )
        .await;

        if self.network.is_regtest() {
            // Only return local peer addresses and skip loading the peer cache on Regtest.
            dns_peers
                .into_iter()
                .filter(PeerSocketAddr::is_localhost)
                .collect()
        } else {
            // Ignore disk errors because the cache is optional and the method already logs them.
            let disk_peers = self.load_peer_cache().await.unwrap_or_default();

            dns_peers.into_iter().chain(disk_peers).collect()
        }
    }

    /// Concurrently resolves `peers` into zero or more IP addresses, with a
    /// timeout of a few seconds on each DNS request.
    ///
    /// If DNS resolution fails or times out for all peers, continues retrying
    /// until at least one peer is found.
    async fn resolve_peers(
        peers: &HashSet<String>,
        expose_peer_addresses: bool,
    ) -> HashSet<PeerSocketAddr> {
        use futures::stream::StreamExt;

        if peers.is_empty() {
            warn!(
                "no initial peers in the network config. \
                 Hint: you must configure at least one peer IP or DNS seeder to run Zakura, \
                 give it some previously cached peer IP addresses on disk, \
                 or make sure Zakura's listener port gets inbound connections."
            );
            return HashSet::new();
        }

        loop {
            // We retry each peer individually, as well as retrying if there are
            // no peers in the combined list. DNS failures are correlated, so all
            // peers can fail DNS, leaving Zakura with a small list of custom IP
            // address peers. Individual retries avoid this issue.
            let peer_addresses = peers
                .iter()
                .map(|s| {
                    Config::resolve_host(s, MAX_SINGLE_SEED_PEER_DNS_RETRIES, expose_peer_addresses)
                })
                .collect::<futures::stream::FuturesUnordered<_>>()
                .concat()
                .await;

            if peer_addresses.is_empty() {
                tracing::info!(
                    ?peers,
                    ?peer_addresses,
                    "empty peer list after DNS resolution, retrying after {} seconds",
                    DNS_LOOKUP_TIMEOUT.as_secs(),
                );
                tokio::time::sleep(DNS_LOOKUP_TIMEOUT).await;
            } else {
                return peer_addresses;
            }
        }
    }

    /// Resolves `host` into zero or more IP addresses, retrying up to
    /// `max_retries` times.
    ///
    /// If DNS continues to fail, returns an empty list of addresses.
    ///
    /// # Panics
    ///
    /// If a configured address is an invalid [`SocketAddr`] or DNS name.
    async fn resolve_host(
        host: &str,
        max_retries: usize,
        expose_peer_addresses: bool,
    ) -> HashSet<PeerSocketAddr> {
        for retries in 0..=max_retries {
            if let Ok(addresses) = Config::resolve_host_once(host, expose_peer_addresses).await {
                return addresses;
            }

            if retries < max_retries {
                tracing::info!(
                    ?host,
                    previous_attempts = ?(retries + 1),
                    "Waiting {DNS_LOOKUP_TIMEOUT:?} to retry seed peer DNS resolution",
                );
                tokio::time::sleep(DNS_LOOKUP_TIMEOUT).await;
            } else {
                tracing::info!(
                    ?host,
                    attempts = ?(retries + 1),
                    "Seed peer DNS resolution failed, checking for addresses from other seed peers",
                );
            }
        }

        HashSet::new()
    }

    /// Resolves `host` into zero or more IP addresses.
    ///
    /// If `host` is a DNS name, performs DNS resolution with a timeout of a few seconds.
    /// If DNS resolution fails or times out, returns an error.
    ///
    /// # Panics
    ///
    /// If a configured address is an invalid [`SocketAddr`] or DNS name.
    async fn resolve_host_once(
        host: &str,
        expose_peer_addresses: bool,
    ) -> Result<HashSet<PeerSocketAddr>, BoxError> {
        let fut = tokio::net::lookup_host(host);
        let fut = tokio::time::timeout(DNS_LOOKUP_TIMEOUT, fut);

        match fut.await {
            Ok(Ok(ip_addrs)) => {
                let ip_addrs: Vec<PeerSocketAddr> = ip_addrs.map(canonical_peer_addr).collect();

                // This log is needed for user debugging, but it's annoying during tests.
                #[cfg(not(test))]
                info!(seed = ?host, remote_ip_count = ?ip_addrs.len(), "resolved seed peer IP addresses");
                #[cfg(test)]
                debug!(seed = ?host, remote_ip_count = ?ip_addrs.len(), "resolved seed peer IP addresses");

                for ip in &ip_addrs {
                    // Count each initial peer, recording the seed config and resolved IP address.
                    //
                    // If an IP is returned by multiple seeds,
                    // each duplicate adds 1 to the initial peer count.
                    // (But we only make one initial connection attempt to each IP.)
                    metrics::counter!(
                        "zcash.net.peers.initial",
                        "seed" => host.to_string(),
                        "remote_ip" => ip.addr_label(expose_peer_addresses)
                    )
                    .increment(1);
                }

                Ok(ip_addrs.into_iter().collect())
            }
            Ok(Err(e)) if e.kind() == ErrorKind::InvalidInput => {
                // TODO: add testnet/mainnet ports, like we do with the listener address
                panic!(
                    "Invalid peer IP address in Zakura config: addresses must have ports:\n\
                     resolving {host:?} returned {e:?}"
                );
            }
            Ok(Err(e)) => {
                tracing::info!(?host, ?e, "DNS error resolving peer IP addresses");
                Err(e.into())
            }
            Err(e) => {
                tracing::info!(?host, ?e, "DNS timeout resolving peer IP addresses");
                Err(e.into())
            }
        }
    }

    /// Returns the addresses in the peer list cache file, if available.
    pub async fn load_peer_cache(&self) -> io::Result<HashSet<PeerSocketAddr>> {
        let Some(peer_cache_file) = self.cache_dir.peer_cache_file_path(&self.network) else {
            return Ok(HashSet::new());
        };

        let peer_list = match fs::read_to_string(&peer_cache_file).await {
            Ok(peer_list) => peer_list,
            Err(peer_list_error) => {
                // We expect that the cache will be missing for new Zakura installs
                if peer_list_error.kind() == ErrorKind::NotFound {
                    return Ok(HashSet::new());
                } else {
                    info!(
                        ?peer_list_error,
                        "could not load cached peer list, using default seed peers"
                    );
                    return Err(peer_list_error);
                }
            }
        };

        // Skip and log addresses that don't parse, and automatically deduplicate using the HashSet.
        // (These issues shouldn't happen unless users modify the file.)
        let peer_list: HashSet<PeerSocketAddr> = peer_list
            .lines()
            .filter_map(|peer| {
                peer.parse()
                    .map_err(|peer_parse_error| {
                        info!(
                            ?peer_parse_error,
                            "invalid peer address in cached peer list, skipping"
                        );
                        peer_parse_error
                    })
                    .ok()
            })
            .collect();

        // This log is needed for user debugging, but it's annoying during tests.
        #[cfg(not(test))]
        info!(
            cached_ip_count = ?peer_list.len(),
            ?peer_cache_file,
            "loaded cached peer IP addresses"
        );
        #[cfg(test)]
        debug!(
            cached_ip_count = ?peer_list.len(),
            ?peer_cache_file,
            "loaded cached peer IP addresses"
        );

        for ip in &peer_list {
            // Count each initial peer, recording the cache file and loaded IP address.
            //
            // If an IP is returned by DNS seeders and the cache,
            // each duplicate adds 1 to the initial peer count.
            // (But we only make one initial connection attempt to each IP.)
            metrics::counter!(
                "zcash.net.peers.initial",
                "cache" => peer_cache_file.display().to_string(),
                "remote_ip" => ip.addr_label(self.expose_peer_addresses)
            )
            .increment(1);
        }

        Ok(peer_list)
    }

    /// Atomically writes a new `peer_list` to the peer list cache file, if configured.
    /// If the list is empty, keeps the previous cache file.
    ///
    /// Also creates the peer cache directory, if it doesn't already exist.
    ///
    /// Atomic writes avoid corrupting the cache if Zakura panics or crashes, or if multiple Zakura
    /// instances try to read and write the same cache file.
    pub async fn update_peer_cache(&self, peer_list: HashSet<PeerSocketAddr>) -> io::Result<()> {
        let Some(peer_cache_file) = self.cache_dir.peer_cache_file_path(&self.network) else {
            return Ok(());
        };

        if peer_list.is_empty() {
            info!(
                ?peer_cache_file,
                "cacheable peer list was empty, keeping previous cache"
            );
            return Ok(());
        }

        let selected_peers: Vec<PeerSocketAddr> = peer_list
            .iter()
            .take(MAX_PEER_DISK_CACHE_SIZE)
            .copied()
            .collect();

        // Turn IP addresses into unredacted strings so they remain reconnectable.
        let mut peer_list: Vec<String> = selected_peers
            .iter()
            .map(|peer| peer.remove_socket_addr_privacy().to_string())
            .collect();
        // # Privacy
        //
        // Sort to destroy any peer order, which could leak peer connection times.
        // (Currently the HashSet argument does this as well.)
        peer_list.sort();
        // Make a newline-separated list
        let peer_data = peer_list.join("\n");

        // Write the peer cache file atomically so the cache is not corrupted if Zakura shuts down
        // or crashes.
        let span = Span::current();
        let write_result = tokio::task::spawn_blocking(move || {
            span.in_scope(move || atomic_write(peer_cache_file, peer_data.as_bytes()))
        })
        .await
        .expect("could not write the peer cache file")?;

        match write_result {
            Ok(peer_cache_file) => {
                info!(
                    cached_ip_count = ?peer_list.len(),
                    ?peer_cache_file,
                    "updated cached peer IP addresses"
                );

                for ip in &selected_peers {
                    metrics::counter!(
                        "zcash.net.peers.cache",
                        "cache" => peer_cache_file.display().to_string(),
                        "remote_ip" => ip.addr_label(self.expose_peer_addresses)
                    )
                    .increment(1);
                }

                Ok(())
            }
            Err(error) => Err(error.error),
        }
    }

    /// Resolves the Zakura native iroh [`SecretKey`] for this node, persisting a
    /// freshly generated key on first use so the node keeps a stable
    /// [`NodeId`](iroh::NodeId) across restarts.
    ///
    /// Resolution order:
    /// 1. If [`zakura_node_secret_key`](Self::zakura_node_secret_key) is configured,
    ///    it is parsed and used verbatim. An unparsable value is a hard error.
    /// 2. Otherwise, the persisted key file under
    ///    [`identity_dir`](Self::identity_dir) is loaded; when it is missing or
    ///    unreadable a fresh key is generated and written atomically with
    ///    owner-only (`0o600`) permissions, so every later startup reuses the
    ///    same identity.
    /// 3. If the key cannot be persisted, the freshly generated identity is used
    ///    ephemerally for this run.
    ///
    /// # Security
    ///
    /// The persisted key file is the node's long-term private identity. It is
    /// written outside the cache and state directories and restricted to owner
    /// read/write on Unix.
    pub fn zakura_secret_key(&self) -> Result<SecretKey, ZakuraSecretKeyError> {
        if let Some(secret) = &self.zakura_node_secret_key {
            return SecretKey::from_str(secret.expose_secret())
                .map_err(|_| ZakuraSecretKeyError::InvalidConfigured);
        }

        let key_file = zakura_secret_key_file_path(&self.identity_dir, &self.network);

        Ok(load_or_generate_zakura_secret_key(&key_file))
    }

    /// Returns `true` if Zakura should run the legacy TCP Zcash P2P listener, initial peer
    /// dialing, and peer crawler on the configured network.
    pub fn legacy_p2p(&self) -> bool {
        self.p2p_stack.resolve(&self.network).runs_legacy()
    }

    /// Returns `true` if Zakura should run the native Zakura P2P v2 endpoint on the configured
    /// network, advertise the P2P v2 service bit during the legacy Zcash handshake, and route
    /// mutually capable peers to the Zakura upgrade hook.
    pub fn v2_p2p(&self) -> bool {
        self.p2p_stack.resolve(&self.network).runs_zakura()
    }

    /// Builds a network config for tests with [`p2p_stack`](Self::p2p_stack) pinned to
    /// `p2p_stack`, so the stack is never re-derived from the network's binary defaults.
    ///
    /// Override other fields with struct-update syntax, e.g.
    /// `Config { network, ..Config::for_test(P2pStack::Dual) }`.
    ///
    /// # Panics
    ///
    /// If `p2p_stack` is [`P2pStack::Default`]: a test that doesn't pin its stack silently
    /// changes behaviour when the per-network defaults change.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn for_test(p2p_stack: P2pStack) -> Config {
        assert_ne!(
            p2p_stack,
            P2pStack::Default,
            "tests must pin an explicit P2P stack",
        );

        Config {
            p2p_stack,
            ..Config::default()
        }
    }
}

/// Loads a persisted Zakura secret key from `key_file`, or generates, persists, and
/// returns a fresh key when the file is absent or cannot be parsed.
///
/// I/O failures are logged and downgraded to an ephemeral key for this run, so a
/// read-only or full cache directory never prevents the node from starting.
fn load_or_generate_zakura_secret_key(key_file: &Path) -> SecretKey {
    match std::fs::read_to_string(key_file) {
        Ok(contents) => match SecretKey::from_str(contents.trim()) {
            Ok(secret_key) => return secret_key,
            Err(_) => warn!(
                ?key_file,
                "ignoring unparsable Zakura node secret key file; regenerating a new identity"
            ),
        },
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => warn!(
            ?error,
            ?key_file,
            "could not read Zakura node secret key file; regenerating a new identity"
        ),
    }

    let secret_key = SecretKey::generate(OsRng);
    persist_zakura_secret_key(key_file, &secret_key);
    secret_key
}

/// Atomically writes `secret_key` to `key_file` as lowercase hex and restricts
/// the file to owner-only access. Persistence failures are logged but not fatal.
fn persist_zakura_secret_key(key_file: &Path, secret_key: &SecretKey) {
    let encoded = hex::encode(secret_key.to_bytes());

    match atomic_write(key_file.to_path_buf(), encoded.as_bytes()) {
        Ok(Ok(path)) => {
            if let Err(error) = restrict_secret_key_file_permissions(&path) {
                warn!(
                    ?error,
                    ?path,
                    "persisted Zakura node secret key but could not restrict its permissions"
                );
            } else {
                info!(?path, "persisted a new Zakura node secret key");
            }
        }
        Ok(Err(error)) => warn!(
            ?error,
            ?key_file,
            "could not persist Zakura node secret key; using an ephemeral identity this run"
        ),
        Err(error) => warn!(
            ?error,
            ?key_file,
            "could not persist Zakura node secret key; using an ephemeral identity this run"
        ),
    }
}

/// Restricts the persisted secret key file to owner read/write (`0o600`) on Unix.
#[cfg(unix)]
fn restrict_secret_key_file_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

/// File permissions are not restricted on non-Unix platforms.
#[cfg(not(unix))]
fn restrict_secret_key_file_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

impl Default for Config {
    fn default() -> Config {
        let mainnet_peers = [
            "dnsseed.str4d.xyz:8233",
            "dnsseed.z.cash:8233",
            "mainnet.seeder.shieldedinfra.net:8233",
            "mainnet.seeder.zfnd.org:8233",
        ]
        .iter()
        .map(|&s| String::from(s))
        .collect();

        let testnet_peers = [
            "dnsseed.testnet.z.cash:18233",
            "testnet.seeder.zfnd.org:18233",
        ]
        .iter()
        .map(|&s| String::from(s))
        .collect();

        Config {
            listen_addr: "[::]:8233"
                .parse()
                .expect("Hardcoded address should be parseable"),
            external_addr: None,
            network: Network::Mainnet,
            initial_mainnet_peers: mainnet_peers,
            initial_testnet_peers: testnet_peers,
            cache_dir: CacheDir::default(),
            identity_dir: default_network_identity_dir(),
            zakura_node_secret_key: None,
            p2p_stack: P2pStack::Default,
            zakura: ZakuraConfig::default(),
            crawl_new_peer_interval: DEFAULT_CRAWL_NEW_PEER_INTERVAL,

            // # Security
            //
            // The default peerset target size should be large enough to ensure
            // nodes have a reliable set of peers.
            //
            // But Zakura should only make a small number of initial outbound connections,
            // so that idle peers don't use too many connection slots.
            peerset_initial_target_size: DEFAULT_PEERSET_INITIAL_TARGET_SIZE,
            max_connections_per_ip: DEFAULT_MAX_CONNS_PER_IP,
            expose_peer_addresses: false,
        }
    }
}

/// Maps the deprecated `legacy_p2p` and `v2_p2p` booleans onto a [`P2pStack`], so configs
/// written before `p2p_stack` keep loading.
///
/// Setting `p2p_stack` alongside either deprecated field is rejected rather than resolved by
/// precedence, because the settings can contradict each other.
fn p2p_stack_from_config<'de, D>(
    p2p_stack: Option<P2pStack>,
    legacy_p2p: Option<bool>,
    v2_p2p: Option<bool>,
) -> Result<P2pStack, D::Error>
where
    D: Deserializer<'de>,
{
    match (p2p_stack, legacy_p2p, v2_p2p) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => Err(de::Error::custom(
            "network.p2p_stack can't be combined with the deprecated network.legacy_p2p or \
             network.v2_p2p settings; use network.p2p_stack on its own",
        )),

        (Some(p2p_stack), None, None) => Ok(p2p_stack),

        (None, None, None) => Ok(P2pStack::Default),

        (None, legacy_p2p, v2_p2p) => match (legacy_p2p.unwrap_or(true), v2_p2p.unwrap_or(true)) {
            (true, true) => Ok(P2pStack::Dual),
            (true, false) => Ok(P2pStack::Legacy),
            (false, true) => Ok(P2pStack::Zakura),
            (false, false) => Err(de::Error::custom(
                "network.legacy_p2p and network.v2_p2p are both false, which disables all \
                 peer-to-peer networking; set network.p2p_stack instead",
            )),
        },
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DTestnetParameters {
    network_name: Option<String>,
    network_magic: Option<[u8; 4]>,
    slow_start_interval: Option<u32>,
    target_difficulty_limit: Option<String>,
    disable_pow: Option<bool>,
    genesis_hash: Option<String>,
    activation_heights: Option<ConfiguredActivationHeights>,
    pre_nu6_funding_streams: Option<ConfiguredFundingStreams>,
    post_nu6_funding_streams: Option<ConfiguredFundingStreams>,
    funding_streams: Option<Vec<ConfiguredFundingStreams>>,
    pre_blossom_halving_interval: Option<u32>,
    lockbox_disbursements: Option<Vec<ConfiguredLockboxDisbursement>>,
    #[serde(default)]
    checkpoints: ConfiguredCheckpoints,
    /// If `true`, automatically repeats configured funding stream addresses to fill
    /// all required periods.
    extend_funding_stream_addresses_as_required: Option<bool>,
    /// Height at which the soft fork that temporarily disables Orchard actions activates.
    ///
    /// If unset, the default activation height for the network is used; the soft fork
    /// cannot be disabled via configuration.
    temporary_orchard_disabling_soft_fork_height: Option<u32>,
}

/// Network configuration used during deserialization.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum DNetwork {
    DefaultForKind(NetworkKind),
    ConfiguredRegtest {
        params: Box<DTestnetParameters>,

        #[serde(default, skip_serializing)]
        regtest: Option<bool>,
    },
    ConfiguredTestnet(Box<DTestnetParameters>),
}

impl Default for DNetwork {
    fn default() -> Self {
        DNetwork::DefaultForKind(NetworkKind::Mainnet)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct DConfig {
    listen_addr: String,
    external_addr: Option<String>,
    network: DNetwork,

    /// Legacy testnet parameters, kept for backwards compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    testnet_parameters: Option<DTestnetParameters>,

    initial_mainnet_peers: IndexSet<String>,
    initial_testnet_peers: IndexSet<String>,
    cache_dir: CacheDir,
    identity_dir: PathBuf,
    #[serde(default, skip_serializing)]
    zakura_node_secret_key: Option<ZakuraNodeSecretKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    p2p_stack: Option<P2pStack>,
    /// Deprecated, superseded by `p2p_stack`. Accepted so configs written for older releases
    /// keep loading, and never written back out.
    #[serde(default, skip_serializing)]
    legacy_p2p: Option<bool>,
    /// Deprecated, superseded by `p2p_stack`. See `legacy_p2p`.
    #[serde(default, alias = "enable_p2p_v2", skip_serializing)]
    v2_p2p: Option<bool>,
    zakura: ZakuraConfig,
    peerset_initial_target_size: usize,
    #[serde(alias = "new_peer_interval", with = "humantime_serde")]
    crawl_new_peer_interval: Duration,
    max_connections_per_ip: Option<usize>,
    expose_peer_addresses: bool,
}

impl Default for DConfig {
    fn default() -> Self {
        let config = Config::default();
        Self {
            listen_addr: "[::]".to_string(),
            external_addr: None,
            network: Default::default(),
            testnet_parameters: None,
            initial_mainnet_peers: config.initial_mainnet_peers,
            initial_testnet_peers: config.initial_testnet_peers,
            cache_dir: config.cache_dir,
            identity_dir: config.identity_dir,
            zakura_node_secret_key: config.zakura_node_secret_key,
            p2p_stack: Some(config.p2p_stack),
            legacy_p2p: None,
            v2_p2p: None,
            zakura: config.zakura,
            peerset_initial_target_size: config.peerset_initial_target_size,
            crawl_new_peer_interval: config.crawl_new_peer_interval,
            max_connections_per_ip: Some(config.max_connections_per_ip),
            expose_peer_addresses: config.expose_peer_addresses,
        }
    }
}

impl From<Arc<testnet::Parameters>> for DTestnetParameters {
    fn from(params: Arc<testnet::Parameters>) -> Self {
        Self {
            network_name: Some(params.network_name().to_string()),
            network_magic: Some(params.network_magic().0),
            slow_start_interval: Some(params.slow_start_interval().0),
            target_difficulty_limit: Some(params.target_difficulty_limit().to_string()),
            disable_pow: Some(params.disable_pow()),
            genesis_hash: Some(params.genesis_hash().to_string()),
            activation_heights: Some(params.activation_heights().into()),
            pre_nu6_funding_streams: None,
            post_nu6_funding_streams: None,
            funding_streams: Some(params.funding_streams().iter().map(Into::into).collect()),
            pre_blossom_halving_interval: Some(
                params
                    .pre_blossom_halving_interval()
                    .try_into()
                    .expect("should convert"),
            ),
            lockbox_disbursements: Some(
                params
                    .lockbox_disbursements()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            ),
            checkpoints: if params.checkpoints() == testnet::Parameters::default().checkpoints() {
                ConfiguredCheckpoints::Default(true)
            } else {
                params.checkpoints().into()
            },
            extend_funding_stream_addresses_as_required: None,
            temporary_orchard_disabling_soft_fork_height: params
                .temporary_orchard_disabling_soft_fork_height()
                .map(|height| height.0),
        }
    }
}

impl From<Config> for DConfig {
    fn from(
        Config {
            listen_addr,
            external_addr,
            network,
            initial_mainnet_peers,
            initial_testnet_peers,
            cache_dir,
            identity_dir,
            zakura_node_secret_key,
            p2p_stack,
            zakura,
            peerset_initial_target_size,
            crawl_new_peer_interval,
            max_connections_per_ip,
            expose_peer_addresses,
        }: Config,
    ) -> Self {
        let dnetwork = match network.kind() {
            NetworkKind::Testnet => match network
                .parameters()
                .filter(|params| !params.is_default_testnet())
                .map(Into::into)
            {
                Some(params) => DNetwork::ConfiguredTestnet(Box::new(params)),
                None => DNetwork::DefaultForKind(NetworkKind::Testnet),
            },

            NetworkKind::Regtest => match network.parameters().map(Into::into) {
                Some(params) => DNetwork::ConfiguredRegtest {
                    params: Box::new(params),
                    regtest: Some(true),
                },
                None => DNetwork::DefaultForKind(NetworkKind::Regtest),
            },

            other_kind => DNetwork::DefaultForKind(other_kind),
        };

        DConfig {
            listen_addr: listen_addr.to_string(),
            external_addr: external_addr.map(|addr| addr.to_string()),
            network: dnetwork,
            testnet_parameters: None,
            initial_mainnet_peers,
            initial_testnet_peers,
            cache_dir,
            identity_dir,
            zakura_node_secret_key,
            p2p_stack: Some(p2p_stack),
            legacy_p2p: None,
            v2_p2p: None,
            zakura,
            peerset_initial_target_size,
            crawl_new_peer_interval,
            max_connections_per_ip: Some(max_connections_per_ip),
            expose_peer_addresses,
        }
    }
}

impl<'de> Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let DConfig {
            listen_addr,
            external_addr,
            network: dnetwork,
            testnet_parameters,
            initial_mainnet_peers,
            initial_testnet_peers,
            cache_dir,
            identity_dir,
            zakura_node_secret_key,
            p2p_stack,
            legacy_p2p,
            v2_p2p,
            zakura,
            peerset_initial_target_size,
            crawl_new_peer_interval,
            max_connections_per_ip,
            expose_peer_addresses,
        } = DConfig::deserialize(deserializer)?;

        let p2p_stack = p2p_stack_from_config::<D>(p2p_stack, legacy_p2p, v2_p2p)?;

        let network = match (dnetwork, testnet_parameters) {
            (DNetwork::ConfiguredTestnet(params), _) => {
                build_configured_testnet::<D>(*params, &initial_testnet_peers)?
            }
            (DNetwork::ConfiguredRegtest { params, .. }, _) => {
                Network::new_regtest(build_regtest_params(*params))
            }
            (DNetwork::DefaultForKind(NetworkKind::Mainnet), _) => Network::Mainnet,
            (DNetwork::DefaultForKind(NetworkKind::Testnet), Some(params)) => {
                build_configured_testnet::<D>(params, &initial_testnet_peers)?
            }
            (DNetwork::DefaultForKind(NetworkKind::Testnet), None) => {
                Network::new_default_testnet()
            }
            (DNetwork::DefaultForKind(NetworkKind::Regtest), Some(params)) => {
                Network::new_regtest(build_regtest_params(params))
            }
            (DNetwork::DefaultForKind(NetworkKind::Regtest), None) => {
                Network::new_regtest(Default::default())
            }
        };

        let listen_addr = match listen_addr.parse::<SocketAddr>().or_else(|_| format!("{listen_addr}:{}", network.default_port()).parse()) {
            Ok(socket) => Ok(socket),
            Err(_) => match listen_addr.parse::<IpAddr>() {
                Ok(ip) => Ok(SocketAddr::new(ip, network.default_port())),
                Err(err) => Err(de::Error::custom(format!(
                    "{err}; Hint: addresses can be a IPv4, IPv6 (with brackets), or a DNS name, the port is optional"
                ))),
            },
        }?;

        let external_socket_addr = if let Some(address) = &external_addr {
            match address.parse::<SocketAddr>().or_else(|_| format!("{address}:{}", network.default_port()).parse()) {
                Ok(socket) => Ok(Some(socket)),
                Err(_) => match address.parse::<IpAddr>() {
                    Ok(ip) => Ok(Some(SocketAddr::new(ip, network.default_port()))),
                    Err(err) => Err(de::Error::custom(format!(
                        "{err}; Hint: addresses can be a IPv4, IPv6 (with brackets), or a DNS name, the port is optional"
                    ))),
                },
            }?
        } else {
            None
        };

        let [max_connections_per_ip, peerset_initial_target_size] = [
            ("max_connections_per_ip", max_connections_per_ip, DEFAULT_MAX_CONNS_PER_IP),
            // If we want Zakura to operate with no network,
            // we should implement a `zakurad` command that doesn't use `zakura-network`.
            ("peerset_initial_target_size", Some(peerset_initial_target_size), DEFAULT_PEERSET_INITIAL_TARGET_SIZE)
        ].map(|(field_name, non_zero_config_field, default_config_value)| {
            if non_zero_config_field == Some(0) {
                warn!(
                    ?field_name,
                    ?non_zero_config_field,
                    "{field_name} should be greater than 0, using default value of {default_config_value} instead"
                );
            }

            non_zero_config_field.filter(|config_value| config_value > &0).unwrap_or(default_config_value)
        });

        // Clamp too-small budgets rather than rejecting existing configurations.
        let mut zakura = zakura;
        zakura.apply_network_defaults(&network);
        let default_zakura_bootstrap_peers =
            ZakuraConfig::default_bootstrap_peers_for_network(&network);
        if zakura.bootstrap_peers.is_empty() {
            warn!(
                ?network,
                "no Zakura bootstrap peers configured; configure zakura.bootstrap_peers or make sure this node receives inbound Zakura connections"
            );
        } else if network.kind() != NetworkKind::Regtest
            && zakura.bootstrap_peers != default_zakura_bootstrap_peers
        {
            warn!(
                ?network,
                configured_zakura_bootstrap_peers = ?zakura.bootstrap_peers,
                ?default_zakura_bootstrap_peers,
                "configured Zakura bootstrap peers differ from the default peers for this network"
            );
        }
        if zakura_listens_on_loopback_with_non_loopback_bootstrap_peers(&zakura) {
            warn!(
                ?network,
                listen_addr = ?zakura.listen_addr,
                bootstrap_peers = ?zakura.bootstrap_peers,
                "configured Zakura listen_addr is loopback-only, but bootstrap peers use \
                 non-loopback addresses; native Zakura dials may fail with \
                 `Can't assign requested address`. Use 0.0.0.0:<port> or another routable \
                 interface address for public Zakura peers"
            );
        }
        zakura
            .block_sync
            .clamp_inflight_block_bytes_to_request_floor();
        zakura.block_sync.clamp_reorder_lookahead_to_floor();
        zakura.block_sync.validate().map_err(|error| {
            de::Error::custom(format!("invalid zakura.block_sync config: {error}"))
        })?;

        Ok(Config {
            listen_addr: canonical_socket_addr(listen_addr),
            external_addr: external_socket_addr,
            network,
            initial_mainnet_peers,
            initial_testnet_peers,
            cache_dir,
            identity_dir,
            zakura_node_secret_key,
            p2p_stack,
            zakura,
            peerset_initial_target_size,
            crawl_new_peer_interval,
            max_connections_per_ip,
            expose_peer_addresses,
        })
    }
}

fn zakura_listens_on_loopback_with_non_loopback_bootstrap_peers(zakura: &ZakuraConfig) -> bool {
    let Some(listen_addr) = zakura.listen_addr else {
        return false;
    };

    listen_addr.ip().is_loopback()
        && zakura
            .bootstrap_peers
            .iter()
            .filter_map(|peer| peer.rsplit_once('@'))
            .filter_map(|(_node_id, addr)| addr.parse::<SocketAddr>().ok())
            .any(|addr| !addr.ip().is_loopback())
}

/// Accepts an [`IndexSet`] of initial peers,
///
/// Returns true if any of them are the default Testnet or Mainnet initial peers.
fn contains_default_initial_peers(initial_peers: &IndexSet<String>) -> bool {
    let Config {
        initial_mainnet_peers: mut default_initial_peers,
        initial_testnet_peers: default_initial_testnet_peers,
        ..
    } = Config::default();
    default_initial_peers.extend(default_initial_testnet_peers);

    initial_peers
        .intersection(&default_initial_peers)
        .next()
        .is_some()
}

fn build_configured_testnet<'de, D>(
    params: DTestnetParameters,
    initial_testnet_peers: &IndexSet<String>,
) -> Result<Network, D::Error>
where
    D: Deserializer<'de>,
{
    let DTestnetParameters {
        network_name,
        network_magic,
        slow_start_interval,
        target_difficulty_limit,
        disable_pow,
        genesis_hash,
        activation_heights,
        pre_nu6_funding_streams,
        post_nu6_funding_streams,
        funding_streams,
        pre_blossom_halving_interval,
        lockbox_disbursements,
        checkpoints,
        extend_funding_stream_addresses_as_required,
        temporary_orchard_disabling_soft_fork_height,
    } = params;

    let mut params_builder = testnet::Parameters::build();

    if let Some(network_name) = network_name.clone() {
        params_builder = params_builder
            .with_network_name(network_name)
            .map_err(de::Error::custom)?
    }

    if let Some(network_magic) = network_magic {
        params_builder = params_builder
            .with_network_magic(Magic(network_magic))
            .map_err(de::Error::custom)?;
    }

    if let Some(genesis_hash) = genesis_hash {
        params_builder = params_builder
            .with_genesis_hash(genesis_hash)
            .map_err(de::Error::custom)?;
    }

    if let Some(slow_start_interval) = slow_start_interval {
        params_builder = params_builder
            .with_slow_start_interval(slow_start_interval.try_into().map_err(de::Error::custom)?);
    }

    if let Some(target_difficulty_limit) = target_difficulty_limit.clone() {
        params_builder = params_builder
            .with_target_difficulty_limit(
                target_difficulty_limit
                    .parse::<U256>()
                    .map_err(de::Error::custom)?,
            )
            .map_err(de::Error::custom)?;
    }

    if let Some(disable_pow) = disable_pow {
        params_builder = params_builder.with_disable_pow(disable_pow);
    }

    // Retain default Testnet activation heights unless there's an empty [testnet_parameters.activation_heights] section.
    if let Some(activation_heights) = activation_heights {
        params_builder = params_builder
            .with_activation_heights(activation_heights)
            .map_err(de::Error::custom)?
    }

    if let Some(halving_interval) = pre_blossom_halving_interval {
        params_builder = params_builder
            .with_halving_interval(halving_interval.into())
            .map_err(de::Error::custom)?
    }

    // Set configured funding streams after setting any parameters that affect the funding stream address period.
    let mut funding_streams_vec = funding_streams.unwrap_or_default();

    if let Some(funding_streams) = post_nu6_funding_streams {
        funding_streams_vec.insert(0, funding_streams);
    }

    if let Some(funding_streams) = pre_nu6_funding_streams {
        funding_streams_vec.insert(0, funding_streams);
    }

    if !funding_streams_vec.is_empty() {
        params_builder = params_builder.with_funding_streams(funding_streams_vec);
    }

    if let Some(lockbox_disbursements) = lockbox_disbursements {
        params_builder = params_builder.with_lockbox_disbursements(lockbox_disbursements);
    }

    params_builder = params_builder
        .with_checkpoints(checkpoints)
        .map_err(de::Error::custom)?;

    if let Some(true) = extend_funding_stream_addresses_as_required {
        params_builder = params_builder.extend_funding_streams();
    }

    // Retain the default soft-fork activation height unless one is configured.
    if let Some(height) = temporary_orchard_disabling_soft_fork_height {
        params_builder = params_builder.with_temporary_orchard_disabling_soft_fork_height(
            height.try_into().map_err(de::Error::custom)?,
        );
    }

    // Return an error if the initial testnet peers includes any of the default initial Mainnet or Testnet
    // peers and the configured network parameters are incompatible with the default public Testnet.
    if !params_builder.is_compatible_with_default_parameters()
        && contains_default_initial_peers(initial_testnet_peers)
    {
        return Err(de::Error::custom(
            "cannot use default initials peers with incompatible testnet",
        ));
    };

    // Return the default Testnet if no network name was configured and all parameters match the default Testnet
    if network_name.is_none() && params_builder == testnet::Parameters::build() {
        Ok(Network::new_default_testnet())
    } else {
        Ok(params_builder.to_network().map_err(de::Error::custom)?)
    }
}

fn build_regtest_params(params: DTestnetParameters) -> RegtestParameters {
    let DTestnetParameters {
        activation_heights,
        pre_nu6_funding_streams,
        post_nu6_funding_streams,
        funding_streams,
        lockbox_disbursements,
        checkpoints,
        extend_funding_stream_addresses_as_required,
        ..
    } = params;

    let mut funding_streams_vec = funding_streams.unwrap_or_default();

    if let Some(funding_streams) = post_nu6_funding_streams {
        funding_streams_vec.insert(0, funding_streams);
    }

    if let Some(funding_streams) = pre_nu6_funding_streams {
        funding_streams_vec.insert(0, funding_streams);
    }

    RegtestParameters {
        activation_heights: activation_heights.unwrap_or_default(),
        funding_streams: Some(funding_streams_vec),
        lockbox_disbursements,
        checkpoints: Some(checkpoints),
        extend_funding_stream_addresses_as_required,
    }
}
