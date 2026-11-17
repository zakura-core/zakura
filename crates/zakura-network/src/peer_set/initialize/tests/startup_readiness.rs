//! Network readiness must not depend on completion of legacy bootstrap.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use indexmap::IndexSet;
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    sync::oneshot,
    time::{timeout, Instant},
};
use tokio_util::task::AbortOnDropHandle;
use tower::{service_fn, ServiceExt};
use zakura_chain::{block::Block, chain_tip::NoChainTip, serialization::ZcashDeserializeInto};

use crate::{
    config::{CacheDir, P2pStack},
    protocol::types::PeerServices,
    zakura::{testkit::ZakuraTestNode, LegacyGossipSink, ZakuraPeerId},
    BoxError, Config, InventoryResponse, PeerSource, Request, Response,
};

use super::super::init_with_zakura;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Accepts a real TCP connection but never answers its version handshake.
struct SilentSeed {
    addr: SocketAddr,
    connected: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
    closed: AbortOnDropHandle<Result<bool, BoxError>>,
}

impl SilentSeed {
    async fn bind() -> Result<Self, BoxError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (connected_tx, connected) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let closed = AbortOnDropHandle::new(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            // Reading the version header proves the dial has reached the real handshake.
            let mut header = [0; 24];
            socket.read_exact(&mut header).await?;
            let _ = connected_tx.send(());
            let disconnected = async {
                let mut buffer = [0; 4096];
                while socket.read(&mut buffer).await? != 0 {}
                Ok::<_, BoxError>(true)
            };
            tokio::select! {
                result = disconnected => result,
                _ = release_rx => Ok(false),
            }
        }));
        Ok(Self {
            addr,
            connected,
            release,
            closed,
        })
    }
}

fn config(seed: SocketAddr, stack: P2pStack) -> Config {
    let mut config = Config {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        cache_dir: CacheDir::disabled(),
        initial_mainnet_peers: [seed.to_string()].into_iter().collect(),
        peerset_initial_target_size: 1,
        ..Config::for_test(stack)
    };
    config.zakura.bootstrap_peers.clear();
    config
}

/// Startup returns with a silent legacy seed, and a real native connection serves a block.
#[tokio::test]
async fn native_block_download_does_not_wait_for_legacy_handshake() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    timeout(TEST_TIMEOUT, native_download_with_silent_seed(None)).await??;
    Ok(())
}

/// Legacy-only startup keeps its bootstrap wait, and cancelling startup closes its dials.
#[tokio::test]
async fn legacy_only_startup_keeps_its_bootstrap_wait_and_cancels_dials() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let mut seed = SilentSeed::bind().await?;
    let inbound = service_fn(|_| async { Ok::<_, BoxError>(Response::Nil) });
    let startup = AbortOnDropHandle::new(tokio::spawn(init_with_zakura(
        config(seed.addr, P2pStack::Legacy),
        inbound,
        NoChainTip,
        "/startup-test/".to_owned(),
        PeerServices::NODE_NETWORK,
        Vec::new(),
        None,
        Vec::new(),
    )));
    timeout(TEST_TIMEOUT, &mut seed.connected).await??;
    tokio::task::yield_now().await;
    assert!(
        !startup.is_finished(),
        "legacy-only startup still waits for bootstrap"
    );
    startup.abort();
    assert!(startup.await.unwrap_err().is_cancelled());
    assert!(
        timeout(Duration::from_secs(1), &mut seed.closed).await???,
        "cancelling startup closes the pending TCP connection before its handshake timeout"
    );
    Ok(())
}

/// A repeatable latency probe using the same production initialization and native fetch path.
#[tokio::test]
#[ignore = "controlled startup timing; run explicitly with output enabled"]
async fn startup_timing_with_two_second_legacy_seed() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    for _ in 0..3 {
        let (ready, fetched) = timeout(
            TEST_TIMEOUT,
            native_download_with_silent_seed(Some(Duration::from_secs(2))),
        )
        .await??;
        println!(
            "startup_probe ready_ms={:.3} block_fetched_ms={:.3}",
            ready.as_secs_f64() * 1000.0,
            fetched.as_secs_f64() * 1000.0
        );
    }
    Ok(())
}

/// Fetches a known block over native QUIC while the legacy seed withholds its handshake.
/// With `release_after`, it also runs on the old implementation for a timing comparison.
async fn native_download_with_silent_seed(
    release_after: Option<Duration>,
) -> Result<(Duration, Duration), BoxError> {
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let hash = block.hash();
    let serving = block.clone();
    let peer = ZakuraTestNode::builder(501)
        .service_from_supervisor(move |supervisor| {
            let serving = serving.clone();
            let inbound = service_fn(move |request| {
                let block = serving.clone();
                async move {
                    match request {
                        Request::BlocksByHash(hashes)
                            if hashes == IndexSet::from([block.hash()]) =>
                        {
                            Ok::<_, BoxError>(Response::Blocks(vec![InventoryResponse::Available(
                                (block.into(), None),
                            )]))
                        }
                        request => Err(format!("unexpected native request: {request:?}").into()),
                    }
                }
            });
            Arc::new(LegacyGossipSink::spawn(inbound, supervisor))
        })
        .spawn()
        .await?;
    let mut seed = SilentSeed::bind().await?;
    let inbound = service_fn(|_| async { Ok::<_, BoxError>(Response::Nil) });
    let started = Instant::now();
    let mut startup = AbortOnDropHandle::new(tokio::spawn(init_with_zakura(
        config(seed.addr, P2pStack::Dual),
        inbound,
        NoChainTip,
        "/startup-test/".to_owned(),
        PeerServices::NODE_NETWORK,
        Vec::new(),
        None,
        Vec::new(),
    )));
    timeout(TEST_TIMEOUT, &mut seed.connected).await??;
    // Keep the release sender alive in the regression so only client shutdown can close
    // the connection. The timing probe instead releases it after a fixed delay.
    let mut release = Some(seed.release);
    let release_task = release_after.map(|delay| {
        let release = release
            .take()
            .expect("the timing probe releases the seed once");
        AbortOnDropHandle::new(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = release.send(());
        }))
    });
    let (network, book, reporter, endpoint, adverts) = match release_after {
        Some(_) => timeout(TEST_TIMEOUT, &mut startup).await???,
        None => timeout(Duration::from_secs(1), &mut startup)
            .await
            .expect("native startup must not wait for the legacy seed handshake")??,
    };
    let ready = started.elapsed();
    let endpoint = endpoint.expect("the dual stack starts a native endpoint");
    peer.connect_native_to_addr(endpoint.node_addr().await, TEST_TIMEOUT)
        .await?;
    let peer_id = ZakuraPeerId::new(peer.node_addr().await.id.as_bytes().to_vec())?;
    let response = timeout(
        TEST_TIMEOUT,
        network.clone().oneshot(Request::BlocksByHashFrom {
            hashes: IndexSet::from([hash]),
            source: PeerSource::Zakura(peer_id),
        }),
    )
    .await??;
    assert!(
        matches!(response, Response::Blocks(ref bodies) if matches!(bodies.as_slice(), [InventoryResponse::Available((body, _))] if body.hash() == hash)),
        "the native peer returns the requested block"
    );
    let fetched = started.elapsed();
    if release_after.is_none() {
        assert!(
            !seed.closed.is_finished(),
            "the block arrives before legacy bootstrap finishes"
        );
    }
    endpoint.shutdown().await;
    drop((network, book, reporter, adverts));
    if release_after.is_none() {
        assert!(
            timeout(Duration::from_secs(1), &mut seed.closed).await???,
            "shutdown closes the pending legacy connection before its handshake timeout"
        );
    }
    peer.shutdown().await;
    drop(release_task);
    Ok((ready, fetched))
}
