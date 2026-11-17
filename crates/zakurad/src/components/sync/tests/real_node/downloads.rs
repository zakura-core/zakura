//! Attribute duplicate wire downloads to independent queues or the production hedge policy.
use super::*;
use crate::components::{
    inbound::downloads as inbound_downloads,
    sync::{self, downloads as sync_downloads, legacy_trace::LegacySyncTrace},
};
use futures::StreamExt;

#[derive(Clone, Debug)]
struct Attempt {
    hash: zakura_chain::block::Hash,
    owner: &'static str,
    connection: usize,
    number: usize,
    started: std::time::Instant,
    finished: Option<std::time::Instant>,
    outcome: &'static str,
}
type Attempts = Arc<Mutex<Vec<Attempt>>>;

struct Completion {
    attempts: Attempts,
    index: usize,
}
impl Drop for Completion {
    fn drop(&mut self) {
        let mut attempts = self.attempts.lock().unwrap();
        let attempt = &mut attempts[self.index];
        if attempt.finished.is_none() {
            attempt.finished = Some(std::time::Instant::now());
            attempt.outcome = "cancelled";
        }
    }
}

/// Only observes and routes calls to real TCP clients; it never supplies a block or verification result.
fn attributed(clients: Vec<(usize, Peers)>, owner: &'static str, attempts: Attempts) -> Peers {
    BoxCloneSyncService::new(tower::service_fn(move |request: Request| {
        let hash = match &request {
            Request::BlocksByHash(hashes) | Request::BlocksByHashFrom { hashes, .. } => {
                *hashes.iter().next().unwrap()
            }
            _ => panic!("download services request blocks"),
        };
        let mut records = attempts.lock().unwrap();
        let number = 1 + records
            .iter()
            .filter(|r| r.hash == hash && r.owner == owner)
            .count();
        let selected = (number - 1) % clients.len();
        let connection = clients[selected].0;
        let index = records.len();
        records.push(Attempt {
            hash,
            owner,
            connection,
            number,
            started: std::time::Instant::now(),
            finished: None,
            outcome: "pending",
        });
        drop(records);
        let attempts = attempts.clone();
        let request = Request::BlocksByHash([hash].into_iter().collect());
        let response = clients[selected].1.clone().oneshot(request);
        let completion = Completion {
            attempts: attempts.clone(),
            index,
        };
        async move {
            let _completion = completion;
            let result = response.await;
            let mut attempts = attempts.lock().unwrap();
            attempts[index].finished = Some(std::time::Instant::now());
            attempts[index].outcome = if result.is_ok() { "received" } else { "error" };
            result
        }
    }))
}

fn verifier(
    node: &Node,
) -> BoxCloneSyncService<VerifyRequest, zakura_chain::block::Hash, BoxError> {
    BoxCloneSyncService::new(node.verifier.clone())
}

fn sync_queue(
    node: &Node,
    network: Peers,
) -> sync_downloads::Downloads<
    Peers,
    BoxCloneSyncService<VerifyRequest, zakura_chain::block::Hash, BoxError>,
    zakura_state::LatestChainTip,
> {
    let state = node.state.clone();
    sync_downloads::Downloads::new(
        network,
        verifier(node),
        node.tip.clone(),
        sync_downloads::ParentWait(Arc::new(move |hash| {
            let response = state
                .clone()
                .oneshot(zakura_state::Request::Block(hash.into()))
                .boxed();
            async move { matches!(response.await, Ok(zakura_state::Response::Block(Some(_)))) }
                .boxed()
        })),
        watch::channel(false).0,
        8,
        node.network.clone(),
        Height(0),
        LegacySyncTrace::new(None, false),
    )
}

#[expect(
    clippy::print_stdout,
    reason = "the attribution fixture emits per-request wire accounting"
)]
fn report(
    scenario: &str,
    hash: zakura_chain::block::Hash,
    peers: &[WirePeer],
    attempts: &Attempts,
) {
    let records = attempts.lock().unwrap();
    let first = records
        .iter()
        .filter(|a| a.hash == hash)
        .map(|a| a.started)
        .min()
        .unwrap();
    for attempt in records.iter().filter(|a| a.hash == hash) {
        println!(
            "{}",
            serde_json::json!({
                "scenario": scenario, "receiver": "receiver-1", "process": std::process::id(), "hash": hash.to_string(), "owner": attempt.owner,
                "connection": attempt.connection, "attempt": attempt.number, "outcome": attempt.outcome,
                "hedge_of": (scenario == "intentional-slow-download-hedge" && attempt.number == 2).then_some(1),
                "started_ms": attempt.started.duration_since(first).as_secs_f64()*1000.0,
                "finished_ms": attempt.finished.map(|t| t.duration_since(first).as_secs_f64()*1000.0),
            })
        );
    }
    for (connection, peer) in peers.iter().enumerate() {
        let getdata = peer.count(hash, "getdata");
        let events = peer.events.lock().unwrap();
        println!(
            "{}",
            serde_json::json!({
                "scenario": scenario, "receiver": "receiver-1", "process": std::process::id(), "connection": connection,
                "hash": hash.to_string(), "getdata": getdata,
                "bodies": events.iter().filter(|e| e.hash == hash && e.kind == "block").count(),
                "body_wire_bytes": events.iter().filter(|e| e.hash == hash && e.kind == "block").map(|e| e.bytes).sum::<usize>(),
            })
        );
    }
}

async fn queue_case(connections: usize, owner: &str, delayed: bool, block: &Block) {
    let mut relay = Node::new().await;
    let mut receiver = Node::new().await;
    let mut peers = vec![WirePeer::connect(&relay).await];
    if connections == 2 {
        peers.push(WirePeer::connect(&relay).await);
    }
    assert_eq!(relay.submit(block).await, "accepted");
    peers[0].announced(block.hash()).await;
    receiver.gate.hold();
    relay.hold_body.send_replace(delayed);
    let attempts = Attempts::default();
    let mut gossip = inbound_downloads::Downloads::new(
        8,
        false,
        attributed(
            vec![(0, peers[0].client.clone())],
            "gossip",
            attempts.clone(),
        ),
        verifier(&receiver),
        receiver.state.clone(),
        receiver.tip.clone(),
        receiver.network.clone(),
    );
    let sync_client = peers[connections - 1].client.clone();
    let mut sync = Box::pin(sync_queue(
        &receiver,
        attributed(
            vec![(connections - 1, sync_client)],
            "sync",
            attempts.clone(),
        ),
    ));
    let has_gossip = owner != "sync";
    let has_sync = owner != "gossip";
    if has_gossip {
        assert!(matches!(
            gossip.download_and_verify(block.hash(), None),
            inbound_downloads::DownloadAction::AddedToQueue
        ));
        assert!(matches!(
            gossip.download_and_verify(block.hash(), None),
            inbound_downloads::DownloadAction::AlreadyQueued
        ));
    }
    if has_sync {
        sync.download_and_verify(block.hash()).await.unwrap();
        assert!(matches!(
            sync.download_and_verify(block.hash()).await,
            Err(sync_downloads::BlockDownloadVerifyError::DuplicateBlockQueuedForDownload { .. })
        ));
    }
    if delayed {
        assert_eq!(
            timeout(Duration::from_secs(10), relay.body_requests.recv())
                .await
                .unwrap(),
            Some(block.hash())
        );
        // Delay an actual inbound body response, rather than fabricating a successful download.
        tokio::time::sleep(Duration::from_millis(100)).await;
        relay.hold_body.send_replace(false);
    }
    let expected = usize::from(has_gossip) + usize::from(has_sync);
    for _ in 0..expected {
        assert_eq!(receiver.gate.entered().await, block.hash());
    }
    assert_eq!(
        peers
            .iter()
            .map(|p| p.count(block.hash(), "block"))
            .sum::<usize>(),
        expected
    );
    receiver.gate.release();
    if has_gossip {
        assert!(timeout(Duration::from_secs(20), gossip.next())
            .await
            .unwrap()
            .is_some());
    }
    if has_sync {
        assert!(timeout(Duration::from_secs(20), sync.next())
            .await
            .unwrap()
            .is_some());
    }
    assert_eq!(receiver.tip.best_tip_hash(), Some(block.hash()));
    assert!(!receiver.bans.is_banned(relay.p2p.ip()));
    let scenario = format!(
        "{owner}/{connections}-connections/{}",
        if delayed { "delayed" } else { "responsive" }
    );
    report(&scenario, block.hash(), &peers, &attempts);
    assert_eq!(attempts.lock().unwrap().len(), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn independent_sync_and_gossip_queues_duplicate_downloads() {
    let _init = zakura_test::init();
    let source = Node::new().await;
    let block = large_block(source.template().await);
    for connections in [1, 2] {
        for owner in ["gossip", "sync", "both"] {
            for delayed in [false, true] {
                queue_case(connections, owner, delayed, &block).await;
            }
        }
    }
}

async fn hedge_case(block: &Block) {
    let mut relay = Node::new().await;
    let receiver = Node::new().await;
    let peers = vec![
        WirePeer::connect(&relay).await,
        WirePeer::connect(&relay).await,
    ];
    assert_eq!(relay.submit(block).await, "accepted");
    let attempts = Attempts::default();
    let clients = peers
        .iter()
        .enumerate()
        .map(|(i, p)| (i, p.client.clone()))
        .collect();
    let network = attributed(clients, "sync", attempts.clone());
    let hedged = tower::hedge::Hedge::new(
        tower::ServiceBuilder::new()
            .concurrency_limit(8)
            .timeout(sync::BLOCK_DOWNLOAD_TIMEOUT)
            .service(network),
        sync_downloads::AlwaysHedge,
        sync::BLOCK_DOWNLOAD_HEDGE_MIN_DATA_POINTS,
        0.95,
        2 * sync::SYNC_RESTART_DELAY,
    );
    let hedged = BoxCloneSyncService::new(Buffer::new(hedged, 8));
    // Populate the real estimator through actual completed TCP downloads, then rotate its
    // production 90-second window. No synthetic latencies or successful verifier stand-ins.
    for _ in 0..sync::BLOCK_DOWNLOAD_HEDGE_MIN_DATA_POINTS {
        hedged
            .clone()
            .oneshot(Request::BlocksByHash(
                [relay.network.genesis_hash()].into_iter().collect(),
            ))
            .await
            .unwrap();
    }
    tokio::time::sleep(2 * sync::SYNC_RESTART_DELAY + Duration::from_millis(10)).await;
    while relay.body_requests.try_recv().is_ok() {}
    relay.hold_body.send_replace(true);
    let mut downloads = Box::pin(sync_queue(&receiver, hedged));
    downloads.download_and_verify(block.hash()).await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            timeout(Duration::from_secs(20), relay.body_requests.recv())
                .await
                .unwrap(),
            Some(block.hash())
        );
    }
    assert_eq!(
        attempts
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a.hash == block.hash())
            .count(),
        2
    );
    relay.hold_body.send_replace(false);
    assert!(timeout(Duration::from_secs(20), downloads.next())
        .await
        .unwrap()
        .unwrap()
        .is_ok());
    timeout(Duration::from_secs(20), async {
        while peers
            .iter()
            .map(|p| p.count(block.hash(), "block"))
            .sum::<usize>()
            != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(receiver.tip.best_tip_hash(), Some(block.hash()));
    assert!(!receiver.bans.is_banned(relay.p2p.ip()));
    report(
        "intentional-slow-download-hedge",
        block.hash(),
        &peers,
        &attempts,
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "warms and rotates the production 90-second hedge estimator over real TCP"]
async fn slow_download_hedges_can_transfer_two_bodies() {
    let _init = zakura_test::init();
    let source = Node::new().await;
    let block = large_block(source.template().await);
    hedge_case(&block).await;
}
