//! Event-driven startup dialing and cancellation regressions.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{channel::mpsc, StreamExt};
use tokio::{
    sync::{mpsc as tokio_mpsc, oneshot},
    task::JoinHandle,
    time::{timeout, Instant},
};
use tower::{service_fn, util::BoxCloneService};

use crate::{
    config::CacheDir,
    constants::INITIAL_PEER_DIAL_CONCURRENCY,
    meta_addr::MetaAddrChange,
    peer::{ClientTestHarness, OutboundConnectorRequest},
    peer_set::{ActiveConnectionCounter, ConnectionTracker},
    BoxError, Config, PeerSocketAddr,
};

use super::super::{add_initial_peers, DiscoveredPeer};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

struct Attempt {
    addr: PeerSocketAddr,
    started_at: Instant,
    finish: oneshot::Sender<bool>,
}

type TestConnector = BoxCloneService<OutboundConnectorRequest, DiscoveredPeer, BoxError>;

/// Hold each connector future until the test chooses success, failure, or cancellation.
fn controlled_connector() -> (
    TestConnector,
    tokio_mpsc::UnboundedReceiver<Attempt>,
    Arc<Mutex<Vec<ConnectionTracker>>>,
) {
    let (attempt_tx, attempt_rx) = tokio_mpsc::unbounded_channel();
    let live_connections = Arc::new(Mutex::new(Vec::new()));
    let live = live_connections.clone();
    let connector = service_fn(move |request: OutboundConnectorRequest| {
        let (finish, result) = oneshot::channel();
        attempt_tx
            .send(Attempt {
                addr: request.addr,
                started_at: Instant::now(),
                finish,
            })
            .unwrap();
        let live = live.clone();
        async move {
            let success = result.await.unwrap();
            if success {
                live.lock().unwrap().push(request.connection_tracker);
                let (client, _harness) = ClientTestHarness::build().finish();
                Ok((request.addr, client))
            } else {
                drop(request.connection_tracker);
                Err("controlled startup failure".into())
            }
        }
    });
    (
        BoxCloneService::new(connector),
        attempt_rx,
        live_connections,
    )
}

/// Persist ordered candidates without DNS or a live network.
fn cached_config(peers: &[PeerSocketAddr], target: usize) -> (Config, tempfile::TempDir) {
    let cache = tempfile::tempdir().unwrap();
    let config = Config {
        cache_dir: CacheDir::custom_path(cache.path()),
        identity_dir: crate::config::test_identity_dir(),
        initial_mainnet_peers: Default::default(),
        peerset_initial_target_size: target,
        ..Config::default()
    };
    let file = config
        .cache_dir
        .peer_cache_file_path(&config.network)
        .unwrap();
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(
        file,
        peers
            .iter()
            .map(|addr| addr.remove_socket_addr_privacy().to_string())
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .unwrap();
    (config, cache)
}

fn peers(count: u8) -> Vec<PeerSocketAddr> {
    (1..=count)
        .map(|ip| std::net::SocketAddr::from(([127, 0, 0, ip], 8233)).into())
        .collect()
}

fn spawn_initial(
    config: Config,
    connector: TestConnector,
) -> (
    JoinHandle<Result<ActiveConnectionCounter, BoxError>>,
    mpsc::Receiver<DiscoveredPeer>,
    tokio_mpsc::Receiver<MetaAddrChange>,
) {
    let (peer_tx, peer_rx) = mpsc::channel(256);
    let (book_tx, book_rx) = tokio_mpsc::channel(256);
    (
        tokio::spawn(add_initial_peers(
            config,
            connector,
            peer_tx,
            book_tx,
            futures::future::ready(Vec::new()),
        )),
        peer_rx,
        book_rx,
    )
}

async fn next_attempt(attempts: &mut tokio_mpsc::UnboundedReceiver<Attempt>) -> Attempt {
    timeout(TEST_TIMEOUT, attempts.recv())
        .await
        .unwrap()
        .unwrap()
}

/// Fifty simultaneous attempts refill on either outcome, without waiting for the other peers.
#[tokio::test]
async fn startup_window_refills_on_success_and_failure() {
    let _init_guard = zakura_test::init();
    let expected = peers(55);
    let (config, _cache) = cached_config(&expected, expected.len());
    assert_eq!(INITIAL_PEER_DIAL_CONCURRENCY, 50);
    let (connector, mut attempts, live) = controlled_connector();
    let (task, mut discovered, _book) = spawn_initial(config, connector);

    let first = next_attempt(&mut attempts).await;
    // Pause only after disk loading: filesystem I/O is not part of the pacing assertion.
    tokio::time::pause();
    let started_at = Instant::now();
    let mut pending = vec![first];
    for _ in 1..INITIAL_PEER_DIAL_CONCURRENCY {
        pending.push(next_attempt(&mut attempts).await);
    }
    assert_eq!(Instant::now(), started_at);
    let initial: std::collections::HashSet<_> = pending.iter().map(|a| a.addr).collect();
    assert_eq!(initial, expected[..50].iter().copied().collect());
    tokio::task::yield_now().await;
    assert!(
        attempts.try_recv().is_err(),
        "the fifty-first dial must wait"
    );

    pending.pop().unwrap().finish.send(false).unwrap();
    let after_error = next_attempt(&mut attempts).await;
    assert_eq!(after_error.addr, expected[50]);
    assert_eq!(after_error.started_at, started_at);
    pending.push(after_error);

    pending.pop().unwrap().finish.send(true).unwrap();
    let after_success = next_attempt(&mut attempts).await;
    assert_eq!(after_success.addr, expected[51]);
    assert_eq!(after_success.started_at, started_at);
    pending.push(after_success);
    assert!(timeout(TEST_TIMEOUT, discovered.next())
        .await
        .unwrap()
        .is_some());

    for attempt in pending {
        attempt.finish.send(false).unwrap();
    }
    let mut tail = std::collections::HashSet::new();
    for _ in &expected[52..] {
        let attempt = next_attempt(&mut attempts).await;
        tail.insert(attempt.addr);
        assert_eq!(attempt.started_at, started_at);
        attempt.finish.send(false).unwrap();
    }
    assert_eq!(tail, expected[52..].iter().copied().collect());
    let mut counter = timeout(TEST_TIMEOUT, task).await.unwrap().unwrap().unwrap();
    assert_eq!(
        counter.update_count(),
        1,
        "only the successful connection remains reserved"
    );
    live.lock().unwrap().clear();
    assert_eq!(counter.update_count(), 0);
    assert!(attempts.recv().await.is_none());
    assert_eq!(Instant::now(), started_at, "startup adds no pacing timer");
}

/// The window changes concurrency, not the selected total or per-IP budget.
#[tokio::test]
async fn startup_window_preserves_target_and_ip_limits() {
    let _init_guard = zakura_test::init();
    let expected = peers(5);
    let mut cached = vec![expected[0], "127.0.0.1:9000".parse().unwrap()];
    cached.extend_from_slice(&expected[1..]);
    let (config, _cache) = cached_config(&cached, 3);
    let (connector, mut attempts, _live) = controlled_connector();
    let (task, _discovered, mut book) = spawn_initial(config, connector);
    let mut dialed = std::collections::HashSet::new();
    for _ in 0..3 {
        let attempt = next_attempt(&mut attempts).await;
        dialed.insert(attempt.addr);
        attempt.finish.send(false).unwrap();
    }
    timeout(TEST_TIMEOUT, task).await.unwrap().unwrap().unwrap();
    assert_eq!(dialed, expected[..3].iter().copied().collect());
    assert!(attempts.recv().await.is_none());
    let mut addresses = Vec::new();
    while let Some(change) = book.recv().await {
        addresses.push(change.addr());
    }
    assert_eq!(addresses, cached, "even skipped candidates reach discovery");
}

/// Dropping startup aborts every admitted connector and never starts the queued suffix.
#[tokio::test]
async fn cancelling_startup_aborts_in_flight_dials() {
    let _init_guard = zakura_test::init();
    let count = INITIAL_PEER_DIAL_CONCURRENCY + 3;
    let (config, _cache) = cached_config(&peers(count.try_into().unwrap()), count);
    let (connector, mut attempts, _live) = controlled_connector();
    let (task, _discovered, _book) = spawn_initial(config, connector);
    let mut pending = Vec::new();
    for _ in 0..INITIAL_PEER_DIAL_CONCURRENCY {
        pending.push(next_attempt(&mut attempts).await);
    }
    assert!(attempts.try_recv().is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    for mut attempt in pending {
        timeout(TEST_TIMEOUT, attempt.finish.closed())
            .await
            .unwrap();
    }
    assert!(timeout(TEST_TIMEOUT, attempts.recv())
        .await
        .unwrap()
        .is_none());
}

/// A closed peer-set receiver cancels the remaining connectors on the error path too.
#[tokio::test]
async fn startup_delivery_failure_aborts_remaining_dials() {
    let _init_guard = zakura_test::init();
    let count = INITIAL_PEER_DIAL_CONCURRENCY + 3;
    let (config, _cache) = cached_config(&peers(count.try_into().unwrap()), count);
    let (connector, mut attempts, live) = controlled_connector();
    let (task, discovered, _book) = spawn_initial(config, connector);
    let mut pending = Vec::new();
    for _ in 0..INITIAL_PEER_DIAL_CONCURRENCY {
        pending.push(next_attempt(&mut attempts).await);
    }
    drop(discovered);
    pending.pop().unwrap().finish.send(true).unwrap();
    assert!(timeout(TEST_TIMEOUT, task).await.unwrap().unwrap().is_err());
    for mut attempt in pending {
        timeout(TEST_TIMEOUT, attempt.finish.closed())
            .await
            .unwrap();
    }
    live.lock().unwrap().clear();
    assert!(timeout(TEST_TIMEOUT, attempts.recv())
        .await
        .unwrap()
        .is_none());
}
