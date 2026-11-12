//! Seed-derived connection opportunities after cache-first startup.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use futures::{channel::mpsc, FutureExt, StreamExt};
use indexmap::IndexSet;
use tokio_util::task::AbortOnDropHandle;
use tower::{service_fn, util::BoxCloneService};

use zakura_chain::serialization::DateTime32;

use crate::{
    address_book::AddressMetrics,
    address_book_updater::AddressBookUpdater,
    constants,
    meta_addr::{MetaAddr, MetaAddrChange},
    peer::{ClientTestHarness, OutboundConnectorRequest},
    peer_set::{set::MorePeers, ActiveConnectionCounter, ConnectionTracker},
    protocol::types::PeerServices,
    AddressBook, BoxError, CacheDir, Config, PeerSocketAddr, Request, Response,
};

use super::super::{
    crawl_and_dial, dial_initial_peers, CandidateSet, DiscoveredPeer, SeedBootstrap,
};

const TIMEOUT: Duration = Duration::from_secs(10);

/// A full healthy cache must not make a late seed depend on gossip or ordinary demand.
#[tokio::test]
async fn late_seed_connects_above_cached_target_without_gossip() {
    let _init_guard = zakura_test::init();
    for target in [2, 4] {
        let cache_dir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: CacheDir::custom_path(cache_dir.path()),
            identity_dir: crate::config::test_identity_dir(),
            peerset_initial_target_size: target,
            ..Config::default()
        };
        let cached: IndexSet<PeerSocketAddr> = (1..=target)
            .map(|i| format!("127.0.0.{i}:8233").parse().unwrap())
            .collect();
        config.update_peer_cache(cached.clone()).await.unwrap();
        let seed: PeerSocketAddr = "127.0.1.1:8233".parse().unwrap();
        let (book, _bans, book_tx, _metrics, updater) =
            AddressBookUpdater::spawn(&config, config.listen_addr, PeerServices::NODE_NETWORK);
        let _updater = AbortOnDropHandle::new(updater);
        let live_connections = Arc::new(Mutex::new(Vec::new()));
        let (attempt_tx, mut attempt_rx) = tokio::sync::mpsc::unbounded_channel();
        let connector = {
            let book = book.clone();
            let live_connections = live_connections.clone();
            service_fn(move |mut request: OutboundConnectorRequest| {
                attempt_tx.send(request.addr).unwrap();
                request.connection_tracker.mark_open();
                live_connections
                    .lock()
                    .unwrap()
                    .push(request.connection_tracker);
                book.lock()
                    .unwrap()
                    .update(MetaAddr::new_responded(request.addr, None))
                    .unwrap();
                async move {
                    let (client, _harness) = ClientTestHarness::build().finish();
                    Ok::<DiscoveredPeer, BoxError>((request.addr, client))
                }
            })
        };
        let (peerset_tx, mut peerset_rx) = mpsc::channel(target + 2);
        let (seed_tx, seed_rx) = tokio::sync::oneshot::channel();
        let startup = tokio::time::timeout(
            TIMEOUT,
            dial_initial_peers(
                config.clone(),
                async move { seed_rx.await.unwrap() }.boxed(),
                connector.clone(),
                peerset_tx.clone(),
                book_tx.clone(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        for _ in 0..target {
            let attempted = tokio::time::timeout(TIMEOUT, attempt_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(cached.contains(&attempted));
            let connected = tokio::time::timeout(TIMEOUT, peerset_rx.next())
                .await
                .unwrap()
                .unwrap();
            assert!(cached.contains(&connected.0));
        }
        let unavailable_gossip = service_fn(|_| async {
            Err::<Response, BoxError>("cached peers provide no usable gossip".into())
        });
        let candidates = CandidateSet::new(book, unavailable_gossip);
        let (demand_tx, demand_rx) = mpsc::channel(config.peerset_outbound_connection_limit());
        let seed_bootstrap = SeedBootstrap::new(
            &config,
            startup.pending_seed_peers,
            startup.resolved_seed_peers,
            startup.attempted_peers,
            startup.seed_peer_attempted,
        );
        let crawler = crawl_and_dial(
            config.clone(),
            demand_tx,
            demand_rx,
            candidates,
            connector,
            peerset_tx,
            startup.active_connections,
            book_tx,
            seed_bootstrap,
            Arc::new(AtomicBool::new(false)),
        );
        let _crawler = AbortOnDropHandle::new(tokio::spawn(crawler));
        seed_tx.send(vec![seed]).unwrap();
        let attempted = tokio::time::timeout(TIMEOUT, attempt_rx.recv())
            .await
            .expect("late seed must get a bounded connection opportunity without ordinary demand")
            .unwrap();
        assert_eq!(attempted, seed);
        let connected = tokio::time::timeout(TIMEOUT, peerset_rx.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(connected.0, seed);
        assert_eq!(live_connections.lock().unwrap().len(), target + 1);
        assert!(target < config.peerset_outbound_connection_limit());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), attempt_rx.recv())
                .await
                .is_err()
        );
    }
}

/// Keep the production tracker alive until success, failure, or cancellation is chosen.
struct TestConnection {
    tracker: Option<ConnectionTracker>,
    active: Arc<AtomicUsize>,
    dropped: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for TestConnection {
    fn drop(&mut self) {
        drop(self.tracker.take());
        self.active.fetch_sub(1, Ordering::SeqCst);
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

struct ControlledAttempt {
    addr: PeerSocketAddr,
    finish: tokio::sync::oneshot::Sender<bool>,
    dropped: tokio::sync::oneshot::Receiver<()>,
}

type TestConnector = BoxCloneService<OutboundConnectorRequest, DiscoveredPeer, BoxError>;

struct SeedTestNetwork {
    config: Config,
    book: Arc<Mutex<AddressBook>>,
    book_tx: tokio::sync::mpsc::Sender<MetaAddrChange>,
    address_metrics: tokio::sync::watch::Receiver<AddressMetrics>,
    connector: TestConnector,
    attempts: tokio::sync::mpsc::UnboundedReceiver<ControlledAttempt>,
    peerset_tx: mpsc::Sender<DiscoveredPeer>,
    peerset_rx: mpsc::Receiver<DiscoveredPeer>,
    live: Arc<Mutex<Vec<(PeerSocketAddr, TestConnection)>>>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    _updater: AbortOnDropHandle<Result<(), BoxError>>,
}

impl SeedTestNetwork {
    fn new(config: Config) -> Self {
        let (book, _bans, book_tx, address_metrics, updater) =
            AddressBookUpdater::spawn(&config, config.listen_addr, PeerServices::NODE_NETWORK);
        let live = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (attempt_tx, attempts) = tokio::sync::mpsc::unbounded_channel();
        let limit = config.peerset_outbound_connection_limit();
        let connector = {
            let book = book.clone();
            let live = live.clone();
            let active = active.clone();
            let peak = peak.clone();
            service_fn(move |request: OutboundConnectorRequest| {
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                assert!(
                    count <= limit,
                    "pending and open connections share the hard cap"
                );
                let (finish, result) = tokio::sync::oneshot::channel();
                let (dropped_tx, dropped) = tokio::sync::oneshot::channel();
                let mut connection = TestConnection {
                    tracker: Some(request.connection_tracker),
                    active: active.clone(),
                    dropped: Some(dropped_tx),
                };
                attempt_tx
                    .send(ControlledAttempt {
                        addr: request.addr,
                        finish,
                        dropped,
                    })
                    .unwrap();
                let book = book.clone();
                let live = live.clone();
                async move {
                    if !result.await.unwrap() {
                        return Err("controlled seed test handshake failure".into());
                    }
                    connection.tracker.as_mut().unwrap().mark_open();
                    book.lock()
                        .unwrap()
                        .update(MetaAddr::new_responded(request.addr, None))
                        .unwrap();
                    live.lock().unwrap().push((request.addr, connection));
                    let (client, _harness) = ClientTestHarness::build().finish();
                    Ok::<DiscoveredPeer, BoxError>((request.addr, client))
                }
            })
        };
        let (peerset_tx, peerset_rx) = mpsc::channel(32);
        Self {
            config,
            book,
            book_tx,
            address_metrics,
            connector: BoxCloneService::new(connector),
            attempts,
            peerset_tx,
            peerset_rx,
            live,
            active,
            peak,
            _updater: AbortOnDropHandle::new(updater),
        }
    }

    /// Model already-open startup peers with the same counter handed to the crawler.
    fn cached_connections(&self) -> (ActiveConnectionCounter, HashSet<PeerSocketAddr>) {
        let mut counter = ActiveConnectionCounter::new_counter_with(
            self.config.peerset_outbound_connection_limit(),
            "Seed Bootstrap Test Connections",
        );
        let mut admitted = HashSet::new();
        for index in 1..=self.config.peerset_initial_target_size {
            let addr = format!("127.0.2.{index}:8233").parse().unwrap();
            admitted.insert(addr);
            self.book
                .lock()
                .unwrap()
                .update(MetaAddr::new_responded(addr, None))
                .unwrap();
            let mut tracker = counter.track_connection();
            tracker.mark_open();
            self.active.fetch_add(1, Ordering::SeqCst);
            self.live.lock().unwrap().push((
                addr,
                TestConnection {
                    tracker: Some(tracker),
                    active: self.active.clone(),
                    dropped: None,
                },
            ));
        }
        self.peak.store(admitted.len(), Ordering::SeqCst);
        assert_eq!(counter.update_count(), admitted.len());
        (counter, admitted)
    }

    fn add_candidates(&self, addresses: &[PeerSocketAddr]) {
        for addr in addresses {
            self.book
                .lock()
                .unwrap()
                .update(MetaAddr::new_initial_peer(*addr))
                .unwrap();
        }
    }

    async fn wait_for_addresses(&mut self, addresses: &[PeerSocketAddr]) {
        drive_paused(async {
            loop {
                let all_present = {
                    let mut book = self.book.lock().unwrap();
                    addresses.iter().all(|addr| book.get(*addr).is_some())
                };
                if all_present {
                    return;
                }
                self.address_metrics.changed().await.unwrap();
            }
        })
        .await;
    }

    fn crawl(&self, counter: ActiveConnectionCounter, bootstrap: SeedBootstrap) -> TestCrawler {
        let candidates = CandidateSet::new(self.book.clone(), service_fn(no_gossip));
        let (demand_tx, demand_rx) = mpsc::channel(32);
        let task = tokio::spawn(crawl_and_dial(
            self.config.clone(),
            demand_tx.clone(),
            demand_rx,
            candidates,
            self.connector.clone(),
            self.peerset_tx.clone(),
            counter,
            self.book_tx.clone(),
            bootstrap,
            Arc::new(AtomicBool::new(false)),
        ));
        TestCrawler {
            demand_tx,
            task: AbortOnDropHandle::new(task),
        }
    }

    async fn next_attempt(&mut self) -> ControlledAttempt {
        drive_paused(self.attempts.recv())
            .await
            .expect("the connector stays alive until the test shuts it down")
    }

    async fn succeed(&mut self, attempt: ControlledAttempt) {
        attempt.finish.send(true).unwrap();
        let peer = drive_paused(self.peerset_rx.next()).await.unwrap();
        assert_eq!(peer.0, attempt.addr);
    }

    /// Probe for two dial intervals so an accidentally queued dial has time to run.
    async fn assert_no_attempt(&mut self) {
        assert!(drive_paused(tokio::time::timeout(
            constants::MIN_OUTBOUND_PEER_CONNECTION_INTERVAL * 2,
            self.attempts.recv(),
        ))
        .await
        .is_err());
    }

    fn disconnect(&self, addr: PeerSocketAddr) {
        let mut live = self.live.lock().unwrap();
        let index = live.iter().position(|(peer, _)| *peer == addr).unwrap();
        live.remove(index);
    }
}

struct TestCrawler {
    demand_tx: mpsc::Sender<MorePeers>,
    task: AbortOnDropHandle<Result<(), BoxError>>,
}

impl TestCrawler {
    fn flood_demand(&mut self) {
        for _ in 0..16 {
            self.demand_tx.try_send(MorePeers).unwrap();
        }
    }

    async fn shutdown(&mut self) {
        self.demand_tx.close_channel();
        let error = drive_paused(&mut self.task).await.unwrap().unwrap_err();
        assert!(error.to_string().contains("demand stream closed"));
    }
}

/// The persistent blocking address-book worker prevents automatic paused-time advancement.
/// Drive both async and blocking work explicitly, with virtual and wall-clock deadlines.
async fn drive_paused<T>(future: impl std::future::Future<Output = T>) -> T {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let wall_deadline = std::time::Instant::now() + TIMEOUT;
    tokio::pin!(future);
    loop {
        if let Some(result) = future.as_mut().now_or_never() {
            return result;
        }
        assert!(
            tokio::time::Instant::now() < deadline && std::time::Instant::now() < wall_deadline,
            "controlled peer operation exceeded the test deadline"
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::spawn_blocking(|| {}).await.unwrap();
        tokio::task::yield_now().await;
    }
}

async fn no_gossip(_: Request) -> Result<Response, BoxError> {
    Err("cached peers provide no usable gossip".into())
}

fn bounded_test_config() -> Config {
    Config {
        identity_dir: crate::config::test_identity_dir(),
        peerset_initial_target_size: 4,
        crawl_new_peer_interval: Duration::from_secs(3600),
        ..Config::default()
    }
}

fn seed_addresses() -> Vec<PeerSocketAddr> {
    (1..=6)
        .map(|index| format!("127.0.3.{index}:8233").parse().unwrap())
        .collect()
}

fn ordinary_addresses() -> Vec<PeerSocketAddr> {
    (1..=8)
        .map(|index| format!("127.0.4.{index}:8233").parse().unwrap())
        .collect()
}

/// Flooded ordinary demand leaves exactly one hard-cap slot for one seed success.
#[tokio::test(start_paused = true)]
async fn seed_reservation_shares_the_hard_cap_with_ordinary_demand() {
    let _init_guard = zakura_test::init();
    let mut network = SeedTestNetwork::new(bounded_test_config());
    assert_eq!(network.config.peerset_outbound_connection_limit(), 6);
    let (counter, admitted) = network.cached_connections();
    let ordinary = ordinary_addresses();
    network.add_candidates(&ordinary);
    let (seed_tx, seed_rx) = tokio::sync::oneshot::channel();
    let bootstrap = SeedBootstrap::new(
        &network.config,
        Some(async move { seed_rx.await.unwrap() }.boxed().fuse()),
        Vec::new(),
        admitted,
        false,
    );
    let mut crawler = network.crawl(counter, bootstrap);
    crawler.flood_demand();
    let attempt = network.next_attempt().await;
    assert!(ordinary.contains(&attempt.addr));
    network.succeed(attempt).await;
    network.assert_no_attempt().await;
    assert_eq!(network.active.load(Ordering::SeqCst), 5);

    let seeds = seed_addresses();
    seed_tx.send(seeds.clone()).unwrap();
    let attempt = network.next_attempt().await;
    assert_eq!(attempt.addr, seeds[0]);
    assert_eq!(network.active.load(Ordering::SeqCst), 6);
    crawler.flood_demand();
    network.assert_no_attempt().await;
    network.succeed(attempt).await;
    network.assert_no_attempt().await;
    assert_eq!(network.peak.load(Ordering::SeqCst), 6);

    // Free capacity without ordinary demand: the one-success seed opportunity is spent.
    network.disconnect(seeds[0]);
    network.assert_no_attempt().await;
    assert_eq!(network.active.load(Ordering::SeqCst), 5);
    crawler.shutdown().await;
}

/// DNS gets a short grace period, not an indefinite reduction in outbound capacity.
#[tokio::test(start_paused = true)]
async fn unresolved_seed_reservation_expires_and_late_seed_waits_for_capacity() {
    let _init_guard = zakura_test::init();
    let mut network = SeedTestNetwork::new(bounded_test_config());
    let (counter, admitted) = network.cached_connections();
    let ordinary = ordinary_addresses();
    network.add_candidates(&ordinary);
    let (seed_tx, seed_rx) = tokio::sync::oneshot::channel();
    let bootstrap = SeedBootstrap::new(
        &network.config,
        Some(async move { seed_rx.await.unwrap() }.boxed().fuse()),
        Vec::new(),
        admitted,
        false,
    );
    let reserve_until = bootstrap.reserve_until;
    let mut crawler = network.crawl(counter, bootstrap);
    crawler.flood_demand();
    let attempt = network.next_attempt().await;
    assert!(ordinary.contains(&attempt.addr));
    network.succeed(attempt).await;
    network.assert_no_attempt().await;
    assert!(tokio::time::Instant::now() < reserve_until);
    assert_eq!(network.active.load(Ordering::SeqCst), 5);

    tokio::time::advance(constants::DNS_LOOKUP_TIMEOUT).await;
    crawler.flood_demand();
    let attempt = network.next_attempt().await;
    assert!(ordinary.contains(&attempt.addr));
    let released_addr = attempt.addr;
    network.succeed(attempt).await;
    assert_eq!(network.active.load(Ordering::SeqCst), 6);

    let seed = seed_addresses()[0];
    seed_tx.send(vec![seed]).unwrap();
    network.assert_no_attempt().await;
    network.disconnect(released_addr);
    // Wake the crawler so it observes the tracker notification and gives seeds priority.
    crawler.demand_tx.try_send(MorePeers).unwrap();
    let attempt = network.next_attempt().await;
    assert_eq!(attempt.addr, seed);
    network.succeed(attempt).await;
    network.assert_no_attempt().await;
    assert_eq!(network.peak.load(Ordering::SeqCst), 6);
    crawler.shutdown().await;
}

/// Failed seed probes are serial, finite, and never manufacture ordinary demand.
#[tokio::test(start_paused = true)]
async fn seed_failures_spend_only_the_seed_attempt_budget() {
    let _init_guard = zakura_test::init();
    let mut network = SeedTestNetwork::new(bounded_test_config());
    let (counter, admitted) = network.cached_connections();
    network.add_candidates(&ordinary_addresses());
    let seeds = seed_addresses();
    // Rejected candidates must not consume the budget for actual connector calls.
    let resolved = std::iter::once("127.0.3.254:0".parse().unwrap())
        .chain(seeds.iter().copied())
        .collect();
    let bootstrap = SeedBootstrap::new(&network.config, None, resolved, admitted, false);
    let mut crawler = network.crawl(counter, bootstrap);
    for seed in seeds
        .iter()
        .take(network.config.peerset_initial_target_size / 2)
    {
        let attempt = network.next_attempt().await;
        assert_eq!(attempt.addr, *seed);
        network.assert_no_attempt().await;
        attempt.finish.send(false).unwrap();
        drive_paused(attempt.dropped).await.unwrap();
    }
    network.assert_no_attempt().await;
    assert_eq!(network.active.load(Ordering::SeqCst), 4);
    assert_eq!(network.peak.load(Ordering::SeqCst), 5);
    crawler.shutdown().await;
}

/// The crawler owns pending seed handshakes, including their connection reservations.
#[tokio::test(start_paused = true)]
async fn crawler_shutdown_cancels_seed_probe_and_drops_its_tracker() {
    let _init_guard = zakura_test::init();
    let mut network = SeedTestNetwork::new(bounded_test_config());
    let (counter, admitted) = network.cached_connections();
    let bootstrap = SeedBootstrap::new(&network.config, None, seed_addresses(), admitted, false);
    let mut crawler = network.crawl(counter, bootstrap);
    let mut attempt = network.next_attempt().await;
    assert_eq!(network.active.load(Ordering::SeqCst), 5);
    crawler.shutdown().await;
    drive_paused(attempt.finish.closed()).await;
    drive_paused(attempt.dropped).await.unwrap();
    assert_eq!(network.active.load(Ordering::SeqCst), 4);
    assert!(network.attempts.try_recv().is_err());
}

/// DNS resolved while the final startup handshake is pending must not be lost.
#[tokio::test]
async fn resolved_but_undialed_startup_seeds_reach_the_crawler() {
    let _init_guard = zakura_test::init();
    let cache = tempfile::tempdir().unwrap();
    let config = Config {
        cache_dir: CacheDir::custom_path(cache.path()),
        ..bounded_test_config()
    };
    let cached: IndexSet<_> = ordinary_addresses().into_iter().take(4).collect();
    config.update_peer_cache(cached.clone()).await.unwrap();
    tokio::time::pause();
    let mut network = SeedTestNetwork::new(config);
    let (seed_tx, seed_rx) = tokio::sync::oneshot::channel();
    let mut startup = AbortOnDropHandle::new(tokio::spawn(dial_initial_peers(
        network.config.clone(),
        async move { seed_rx.await.unwrap() }.boxed(),
        network.connector.clone(),
        network.peerset_tx.clone(),
        network.book_tx.clone(),
    )));
    let mut admitted = Vec::new();
    for _ in 0..4 {
        let attempt = network.next_attempt().await;
        assert!(cached.contains(&attempt.addr));
        admitted.push(attempt);
    }
    let final_handshake = admitted.pop().unwrap();
    for attempt in admitted {
        network.succeed(attempt).await;
    }
    let seeds = seed_addresses();
    seed_tx.send(seeds.clone()).unwrap();
    network.wait_for_addresses(&seeds).await;
    assert!((&mut startup).now_or_never().is_none());
    network.succeed(final_handshake).await;
    let startup = drive_paused(&mut startup).await.unwrap().unwrap();
    assert!(startup.pending_seed_peers.is_none());
    assert_eq!(startup.resolved_seed_peers, seeds);
    assert!(!startup.seed_peer_attempted);
    assert_eq!(
        startup.attempted_peers,
        cached.into_iter().collect::<HashSet<_>>()
    );
    let bootstrap = SeedBootstrap::new(
        &network.config,
        startup.pending_seed_peers,
        startup.resolved_seed_peers,
        startup.attempted_peers,
        startup.seed_peer_attempted,
    );
    let mut crawler = network.crawl(startup.active_connections, bootstrap);
    let attempt = network.next_attempt().await;
    assert_eq!(attempt.addr, seeds[0]);
    network.succeed(attempt).await;
    assert_eq!(network.active.load(Ordering::SeqCst), 5);
    crawler.shutdown().await;
}

/// Source bounds apply after removing startup IPs and duplicate seed IPs.
#[tokio::test]
async fn seed_source_limit_preserves_diversity_and_excludes_startup_ips() {
    let config = bounded_test_config();
    let cached: PeerSocketAddr = "127.0.5.1:8233".parse().unwrap();
    let first_seed: PeerSocketAddr = "127.0.5.2:8233".parse().unwrap();
    let mut peers = vec![cached, cached];
    for port in 8234..8238 {
        peers.push(format!("127.0.5.1:{port}").parse().unwrap());
    }
    peers.push(first_seed);
    for port in 8234..8238 {
        peers.push(format!("127.0.5.2:{port}").parse().unwrap());
    }
    peers.extend(seed_addresses());
    let mut bootstrap =
        SeedBootstrap::new(&config, None, Vec::new(), HashSet::from([cached]), false);
    bootstrap.accept_resolved(peers);
    let retained: Vec<_> = bootstrap.candidates.iter().copied().collect();
    let expected: Vec<_> = std::iter::once(first_seed)
        .chain(seed_addresses().into_iter().take(3))
        .collect();
    assert_eq!(retained, expected);
    assert_eq!(retained.len(), config.peerset_initial_target_size);
    assert_eq!(bootstrap.attempts_remaining, 2);
}

/// Seed selection keeps bans, peer state, validity, and per-IP limits authoritative.
#[tokio::test(start_paused = true)]
async fn seed_candidates_reject_banned_recent_invalid_and_inbound_peers() {
    let _init_guard = zakura_test::init();
    let config = bounded_test_config();
    let addresses: Vec<PeerSocketAddr> = (1..=7)
        .map(|index| format!("127.0.6.{index}:8233").parse().unwrap())
        .collect();
    let stale = constants::MIN_PEER_RECONNECTION_DELAY * 2;
    let old_instant = std::time::Instant::now() - stale;
    let old_time = chrono::Utc::now() - chrono::Duration::from_std(stale).unwrap();
    let inbound = MetaAddr::new_connected(addresses[0], &PeerServices::NODE_NETWORK, true)
        .apply_to_meta_addr(None, old_instant, old_time)
        .unwrap();
    assert!(inbound.is_ready_for_connection_attempt(
        std::time::Instant::now(),
        chrono::Utc::now(),
        &config.network,
    ));
    let invalid = MetaAddr::new_connected(addresses[1], &PeerServices::empty(), false)
        .apply_to_meta_addr(None, old_instant, old_time)
        .unwrap();
    let mut book = AddressBook::new_with_addrs(
        config.listen_addr,
        &config.network,
        config.max_connections_per_ip,
        16,
        tracing::Span::none(),
        [inbound, invalid],
    );
    book.update(MetaAddr::new_responded(addresses[2], None))
        .unwrap();
    let busy_ip: PeerSocketAddr = "127.0.6.4:9000".parse().unwrap();
    book.update(MetaAddr::new_responded(busy_ip, None)).unwrap();
    book.update(MetaAddr::new_initial_peer(addresses[4]))
        .unwrap();
    assert!(book
        .update(MetaAddr::new_misbehavior(
            addresses[4],
            constants::MAX_PEER_MISBEHAVIOR_SCORE,
        ))
        .is_none());
    assert!(book.bans().contains(addresses[4].ip()));
    let book = Arc::new(Mutex::new(book));
    let mut candidates = CandidateSet::new(book, service_fn(no_gossip));
    for addr in &addresses[..5] {
        assert!(
            tokio::time::timeout(TIMEOUT, candidates.next_seed(*addr, 1))
                .await
                .unwrap()
                .is_none()
        );
    }
    let invalid_socket = "127.0.6.8:0".parse().unwrap();
    assert!(
        tokio::time::timeout(TIMEOUT, candidates.next_seed(invalid_socket, 1))
            .await
            .unwrap()
            .is_none()
    );
    let eligible = addresses[5];
    let (claimed, claim) = tokio::time::timeout(TIMEOUT, candidates.next_seed(eligible, 1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.addr, eligible);
    drop(claim);
    assert!(
        tokio::time::timeout(TIMEOUT, candidates.next_seed(eligible, 1))
            .await
            .unwrap()
            .is_none()
    );
}

/// A full gossip book cannot erase the retained seed or its in-flight IP exclusion.
#[tokio::test(start_paused = true)]
async fn seed_selection_survives_gossip_pruning_and_blocks_same_ip_overlap() {
    let _init_guard = zakura_test::init();
    let config = bounded_test_config();
    let seed: PeerSocketAddr = "127.0.7.1:8233".parse().unwrap();
    let alias: PeerSocketAddr = "127.0.7.1:9000".parse().unwrap();
    let gossip =
        MetaAddr::new_gossiped_meta_addr(alias, PeerServices::NODE_NETWORK, DateTime32::now());
    let mut book = AddressBook::new_with_addrs(
        config.listen_addr,
        &config.network,
        config.max_connections_per_ip,
        1,
        tracing::Span::none(),
        [gossip],
    );
    book.update(MetaAddr::new_initial_peer(seed)).unwrap();
    assert!(
        book.get(seed).is_none(),
        "gossip already pruned the seed entry"
    );
    let book = Arc::new(Mutex::new(book));
    let mut candidates = CandidateSet::new(book.clone(), service_fn(no_gossip));
    let (candidate, claim) = tokio::time::timeout(TIMEOUT, candidates.next_seed(seed, 1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(candidate.addr, seed);
    assert!(book.lock().unwrap().get(seed).is_none());
    assert_eq!(book.lock().unwrap().len(), 1);
    assert!(tokio::time::timeout(TIMEOUT, candidates.next())
        .await
        .unwrap()
        .is_none());
    drop(claim);
    assert_eq!(
        tokio::time::timeout(TIMEOUT, candidates.next())
            .await
            .unwrap()
            .unwrap()
            .addr,
        alias,
    );
}

/// A pruned ordinary attempt also excludes a seed at the same IP until its guard drops.
#[tokio::test(start_paused = true)]
async fn ordinary_selection_claim_survives_pruning_until_seed_admission() {
    let _init_guard = zakura_test::init();
    let config = bounded_test_config();
    let ordinary: PeerSocketAddr = "127.0.8.1:9000".parse().unwrap();
    let seed: PeerSocketAddr = "127.0.8.1:8233".parse().unwrap();
    let replacement: PeerSocketAddr = "127.0.8.2:8233".parse().unwrap();
    let gossip =
        MetaAddr::new_gossiped_meta_addr(ordinary, PeerServices::NODE_NETWORK, DateTime32::now());
    let book = Arc::new(Mutex::new(AddressBook::new_with_addrs(
        config.listen_addr,
        &config.network,
        config.max_connections_per_ip,
        1,
        tracing::Span::none(),
        [gossip],
    )));
    let mut candidates = CandidateSet::new(book.clone(), service_fn(no_gossip));
    let (selected, ordinary_claim) = tokio::time::timeout(TIMEOUT, candidates.next_tracked())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.addr, ordinary);
    book.lock()
        .unwrap()
        .update(
            MetaAddr::new_gossiped_meta_addr(
                replacement,
                PeerServices::NODE_NETWORK,
                DateTime32::now(),
            )
            .new_gossiped_change()
            .unwrap(),
        )
        .unwrap();
    assert!(book.lock().unwrap().get(ordinary).is_none());
    assert!(tokio::time::timeout(TIMEOUT, candidates.next_seed(seed, 1))
        .await
        .unwrap()
        .is_none());
    drop(ordinary_claim);
    let (selected, _seed_claim) = tokio::time::timeout(TIMEOUT, candidates.next_seed(seed, 1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.addr, seed);
}

/// Late DNS still reaches ordinary discovery when startup already used a seed opportunity.
#[tokio::test(start_paused = true)]
async fn crawler_publishes_late_dns_with_seed_probe_disabled() {
    let _init_guard = zakura_test::init();
    let mut network = SeedTestNetwork::new(bounded_test_config());
    let (counter, admitted) = network.cached_connections();
    let (seed_tx, seed_rx) = tokio::sync::oneshot::channel();
    let bootstrap = SeedBootstrap::new(
        &network.config,
        Some(async move { seed_rx.await.unwrap() }.boxed().fuse()),
        Vec::new(),
        admitted,
        true,
    );
    let mut crawler = network.crawl(counter, bootstrap);
    let seeds = seed_addresses();
    seed_tx.send(seeds.clone()).unwrap();
    network.wait_for_addresses(&seeds).await;
    network.assert_no_attempt().await;
    crawler.shutdown().await;
}

/// Shutting down also drops unresolved DNS instead of leaving a background lookup owned nowhere.
#[tokio::test(start_paused = true)]
async fn crawler_shutdown_drops_unresolved_seed_lookup() {
    let _init_guard = zakura_test::init();
    let mut network = SeedTestNetwork::new(bounded_test_config());
    let (counter, admitted) = network.cached_connections();
    let (mut seed_tx, seed_rx) = tokio::sync::oneshot::channel();
    let bootstrap = SeedBootstrap::new(
        &network.config,
        Some(async move { seed_rx.await.unwrap() }.boxed().fuse()),
        Vec::new(),
        admitted,
        false,
    );
    let mut crawler = network.crawl(counter, bootstrap);
    crawler.shutdown().await;
    drive_paused(seed_tx.closed()).await;
    assert_eq!(network.active.load(Ordering::SeqCst), 4);
    assert!(network.attempts.try_recv().is_err());
}
