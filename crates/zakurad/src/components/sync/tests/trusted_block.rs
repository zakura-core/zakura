//! Trusted HTTP submissions through real state, consensus, and TCP peers.

#![allow(clippy::unwrap_in_result)]

mod native;
mod pow;
mod wire;

use futures::FutureExt;
use http_body_util::{BodyExt, Full};
use hyper::{body::Bytes, Request as HttpRequest, StatusCode};
use hyper_util::rt::TokioIo;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::AbortHandle,
    time::timeout,
};
use tower::{
    buffer::Buffer,
    util::{BoxCloneService, BoxCloneSyncService, BoxService},
    ServiceExt,
};

use zakura_chain::{
    block::{Block, Height},
    chain_tip::ChainTip,
    parameters::{testnet::ConfiguredActivationHeights, Network},
    serialization::ZcashSerialize,
    transparent::Address,
};
use zakura_consensus::{router::RouterError, Request as VerifyRequest};
use zakura_network::{types::PeerServices, P2pStack, Request, Response};
use zakura_rpc::{
    client::{BlockTemplateTimeSource, SubmitBlockResponse},
    methods::RpcImpl,
    proposal_block_from_template, EarlyMinedBlockChannel, PendingBlockRegistry,
};

use crate::{
    components::{
        inbound::{Inbound, InboundSetupData},
        mempool::{Config as MempoolConfig, Mempool},
        sync::{gossip::gossip_best_tip_block_hashes, RecentSyncLengths, SyncStatus},
    },
    BoxError,
};

type State = Buffer<
    zakura_state::Request,
    futures::future::BoxFuture<'static, Result<zakura_state::Response, BoxError>>,
>;
type Verifier = Buffer<
    VerifyRequest,
    futures::future::BoxFuture<'static, Result<zakura_chain::block::Hash, RouterError>>,
>;
type Peers = BoxCloneSyncService<Request, Response, BoxError>;

/// A relay request held before entering the real peer set.
struct HeldRelay {
    hash: zakura_chain::block::Hash,
    release: oneshot::Sender<()>,
    cancelled: oneshot::Receiver<()>,
}

/// Delays commit requests before passing them to the real consensus router.
struct ValidationGate {
    held: watch::Sender<bool>,
    entered: mpsc::UnboundedReceiver<zakura_chain::block::Hash>,
    finished: mpsc::UnboundedReceiver<(zakura_chain::block::Hash, bool)>,
    failures: mpsc::UnboundedReceiver<(zakura_chain::block::Hash, pow::ValidationFailure)>,
}

impl ValidationGate {
    fn hold(&self) {
        self.held.send_replace(true);
    }
    fn release(&self) {
        self.held.send_replace(false);
    }
    async fn entered(&mut self) -> zakura_chain::block::Hash {
        timeout(Duration::from_secs(20), self.entered.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

/// A node with the production inbound service, ban reporter, gossip, and verifier.
struct Node {
    network: Network,
    rpc: SocketAddr,
    auth: String,
    p2p: SocketAddr,
    peers: Peers,
    endpoint: Option<zakura_network::zakura::ZakuraEndpoint>,
    native_startup: Option<zakura_network::zakura::ZakuraHeaderSyncDriverStartup>,
    native_violations: mpsc::UnboundedReceiver<zakura_network::zakura::BlockSyncMisbehavior>,
    advertisements: mpsc::UnboundedReceiver<zakura_chain::block::Hash>,
    state: State,
    verifier: Verifier,
    tip: zakura_state::LatestChainTip,
    pending: PendingBlockRegistry,
    bans: zakura_network::MisbehaviorReporter,
    gate: ValidationGate,
    hold_relay: watch::Sender<bool>,
    held_relays: mpsc::UnboundedReceiver<HeldRelay>,
    hold_body: watch::Sender<bool>,
    body_requests: mpsc::UnboundedReceiver<zakura_chain::block::Hash>,
    hold_parent_lookup: watch::Sender<bool>,
    parent_lookups: mpsc::UnboundedReceiver<()>,
    tasks: Vec<AbortHandle>,
    _recent: RecentSyncLengths,
    _directory: tempfile::TempDir,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.gate.release();
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Node {
    async fn new(seed: Option<SocketAddr>) -> Self {
        Self::with_stack(seed, P2pStack::Legacy).await
    }

    async fn with_stack(seed: Option<SocketAddr>, stack: P2pStack) -> Self {
        let network = Network::new_regtest(
            ConfiguredActivationHeights {
                nu5: Some(1),
                ..Default::default()
            }
            .into(),
        );
        Self::with_network(
            seed,
            stack,
            network,
            zakura_chain::block::genesis::regtest_genesis_block(),
        )
        .await
    }

    async fn with_network(
        seed: Option<SocketAddr>,
        stack: P2pStack,
        network: Network,
        genesis: Arc<Block>,
    ) -> Self {
        assert_eq!(network.genesis_hash(), genesis.hash());
        let (
            zakura_state::InitializedState {
                state,
                read_state,
                latest_chain_tip: tip,
                chain_tip_change,
                block_announcements,
            },
            body_authority,
        ) = zakura_state::init_with_header_chain_body_evidence(
            zakura_state::Config {
                enable_zakura_header_seed_from_committed_blocks: true,
                ..zakura_state::Config::ephemeral()
            },
            &network,
            Height(0),
            0,
        )
        .await
        .unwrap();
        let state = Buffer::new(state, 16);
        state
            .clone()
            .oneshot(zakura_state::Request::CommitCheckpointVerifiedBlock(
                genesis.into(),
            ))
            .await
            .unwrap();
        let (verifier, tx_verifier, _background, _) = zakura_consensus::router::init_test(
            zakura_consensus::Config::default(),
            &network,
            state.clone(),
        )
        .await;
        let coordinator = crate::commands::start::zakura::SyncCoordinator::new();
        let native_startup = if stack == P2pStack::Dual {
            Some(
                crate::commands::start::zakura::zakura_header_sync_driver_startup(
                    state.clone(),
                    read_state.clone(),
                    body_authority.clone(),
                    &network,
                    &coordinator,
                )
                .await
                .unwrap(),
            )
        } else {
            None
        };
        let pending = PendingBlockRegistry::default();
        let (setup_tx, setup_rx) = oneshot::channel();
        let inbound = Buffer::new(
            BoxService::new(
                Inbound::new(8, false, None, vec![], setup_rx).with_pending_blocks(pending.clone()),
            ),
            16,
        );
        let (advertisement_tx, advertisements) = mpsc::unbounded_channel();
        let (hold_body, body_held) = watch::channel(false);
        let (body_requested, body_requests) = mpsc::unbounded_channel();
        let genesis_hash = network.genesis_hash();
        let inbound = tower::service_fn(move |request: Request| {
            if let Request::AdvertiseBlock(block, _) = &request {
                // Fixture bootstrap can advertise genesis before the submitted block.
                if block.hash != genesis_hash {
                    let _ = advertisement_tx.send(block.hash);
                }
            }
            let body = matches!(&request, Request::BlocksByHash(_));
            if let Request::BlocksByHash(hashes) = &request {
                for hash in hashes {
                    let _ = body_requested.send(*hash);
                }
            }
            let mut held = body_held.clone();
            let response = inbound.clone().oneshot(request).boxed();
            async move {
                if body {
                    while *held.borrow_and_update() {
                        held.changed().await.unwrap();
                    }
                }
                response.await
            }
            .boxed()
        });
        let directory = tempfile::tempdir().unwrap();
        let config = zakura_network::Config {
            network: network.clone(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            initial_mainnet_peers: Default::default(),
            initial_testnet_peers: seed.into_iter().map(|addr| addr.to_string()).collect(),
            cache_dir: zakura_network::CacheDir::disabled(),
            identity_dir: directory.path().join("identity"),
            p2p_stack: stack,
            zakura: zakura_network::zakura::ZakuraConfig {
                listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                bootstrap_peers: vec![],
                ..Default::default()
            },
            max_connections_per_ip: 8,
            ..Default::default()
        };
        let (peers, address_book, bans, endpoint) = zakura_network::init_with_zakura_header_sync(
            config,
            inbound,
            tip.clone(),
            "trusted submission test".into(),
            PeerServices::NODE_NETWORK,
            vec![],
            native_startup.clone(),
        )
        .await;
        let p2p = address_book.lock().unwrap().local_listener_socket_addr();
        let (sync, mut recent) = SyncStatus::new();
        SyncStatus::sync_close_to_tip(&mut recent);
        let (mut pool, _) = Mempool::new(
            &MempoolConfig::default(),
            false,
            peers.clone(),
            state.clone(),
            BoxCloneService::new(read_state.clone()),
            tx_verifier,
            sync.clone(),
            tip.clone(),
            chain_tip_change,
            bans.clone(),
            vec![],
            Default::default(),
        );
        pool.enable(&mut recent).await;
        let pool = Buffer::new(BoxService::new(pool), 16);
        let (held, _) = watch::channel(false);
        let (entered, entered_rx) = mpsc::unbounded_channel();
        let (finished, finished_rx) = mpsc::unbounded_channel();
        let (failures, failures_rx) = mpsc::unbounded_channel();
        let gate_sender = held.clone();
        let real_verifier: Verifier = verifier.clone();
        let gated = tower::service_fn(move |request: VerifyRequest| {
            let verifier = real_verifier.clone();
            let mut held = gate_sender.subscribe();
            let entered = entered.clone();
            let finished = finished.clone();
            let failures = failures.clone();
            let commit = matches!(
                request,
                VerifyRequest::Commit(_) | VerifyRequest::CommitMined { .. }
            );
            let hash = request.block().hash();
            let verification = verifier.oneshot(request).boxed();
            async move {
                if commit {
                    entered.send(hash).unwrap();
                    while *held.borrow_and_update() {
                        held.changed().await.unwrap();
                    }
                }
                let result = verification.await.map_err(|error| {
                    *error
                        .downcast::<RouterError>()
                        .expect("real router returns RouterError")
                });
                if commit {
                    if let Err(error) = &result {
                        let _ = failures.send((hash, pow::ValidationFailure::from(error)));
                    }
                    let _ = finished.send((hash, result.is_ok()));
                }
                result
            }
            .boxed()
        });
        let gated = Buffer::new(gated, 16);
        let mut native_tasks = vec![];
        let (violation_tx, native_violations) = mpsc::unbounded_channel();
        if let Some(endpoint) = &endpoint {
            let mut actions = endpoint.take_block_sync_actions().await.unwrap();
            let (driver_tx, driver_rx) = mpsc::channel(32);
            native_tasks.push(
                tokio::spawn(async move {
                    while let Some(action) = actions.recv().await {
                        if let zakura_network::zakura::BlockSyncAction::Misbehavior {
                            reason, ..
                        } = &action
                        {
                            let _ = violation_tx.send(reason.clone());
                        }
                        if driver_tx.send(action).await.is_err() {
                            break;
                        }
                    }
                })
                .abort_handle(),
            );
            let driver = tokio::spawn(crate::commands::start::zakura::drive_block_sync_actions(
                driver_rx,
                endpoint.supervisor(),
                Some(endpoint.clone()),
                endpoint.block_sync().unwrap(),
                tip.clone(),
                read_state.clone(),
                Some(BoxCloneService::new(state.clone())),
                Some(body_authority),
                gated.clone(),
                Height(0),
                1,
                1,
                1,
                endpoint.trace(),
                None,
                coordinator,
                std::future::pending(),
            ));
            native_tasks.push(driver.abort_handle());
        }
        assert!(setup_tx
            .send(InboundSetupData {
                address_book,
                block_download_peer_set: peers.clone(),
                block_verifier: gated.clone(),
                mempool: pool.clone(),
                state: state.clone(),
                latest_chain_tip: tip.clone(),
                network: network.clone(),
                misbehavior: bans.clone(),
            })
            .is_ok());
        let early = EarlyMinedBlockChannel::new();
        let (_logs, log_rx) = watch::channel(None);
        let (hold_parent_lookup, parent_held) = watch::channel(false);
        let (parent_lookup, parent_lookups) = mpsc::unbounded_channel();
        let rpc_read_state = read_state.clone();
        let rpc_read_state = tower::service_fn(move |request: zakura_state::ReadRequest| {
            let held = *parent_held.borrow()
                && matches!(&request, zakura_state::ReadRequest::BlockInfo(_));
            let parent_lookup = parent_lookup.clone();
            let read_state = rpc_read_state.clone();
            async move {
                if held {
                    let _ = parent_lookup.send(());
                    std::future::pending::<()>().await;
                }
                read_state.oneshot(request).await
            }
        });
        let (rpc, queue) = RpcImpl::new(
            network.clone(),
            zakura_rpc::config::mining::Config {
                miner_address: Some(Address::from_pub_key_hash(network.kind(), [7; 20]).into()),
                ..Default::default()
            },
            false,
            "test",
            "trusted submission test",
            pool,
            state.clone(),
            rpc_read_state,
            gated.clone(),
            sync.clone(),
            tip.clone(),
            zakura_network::address_book_peers::MockAddressBookPeers::default(),
            log_rx,
            Some(early.sender()),
        );
        let rpc = rpc.with_pending_blocks(pending.clone());
        let rpc_addr: SocketAddr = format!("127.0.0.1:{}", zakura_test::net::random_known_port())
            .parse()
            .unwrap();
        let server = zakura_rpc::server::RpcServer::start(
            rpc,
            zakura_rpc::config::rpc::Config {
                listen_addr: Some(rpc_addr),
                cookie_dir: directory.path().to_path_buf(),
                enable_cookie_auth: true,
                enable_trusted_block_submission: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let credentials = std::fs::read(directory.path().join(".cookie")).unwrap();
        let auth = basic_auth(&credentials);
        let (hold_relay, relay_held) = watch::channel(false);
        let (relay_tx, held_relays) = mpsc::unbounded_channel();
        let relay_peers = peers.clone();
        let relay_service = tower::service_fn(move |request: Request| {
            let held = *relay_held.borrow();
            let relay_tx = relay_tx.clone();
            let peers = relay_peers.clone();
            async move {
                if let Request::AdvertiseMinedBlock(block) = &request {
                    if held {
                        let (release, released) = oneshot::channel();
                        let (finished, cancelled) = oneshot::channel();
                        relay_tx
                            .send(HeldRelay {
                                hash: block.hash,
                                release,
                                cancelled,
                            })
                            .unwrap();
                        released.await?;
                        let result = peers.oneshot(request).await;
                        let _ = finished.send(());
                        return result;
                    }
                }
                peers.oneshot(request).await
            }
        });
        let gossip = tokio::spawn(gossip_best_tip_block_hashes(
            sync,
            tip.clone(),
            block_announcements,
            relay_service,
            read_state,
            Some(early.receiver()),
        ));
        let tasks = vec![
            _background.state_checkpoint_verify_handle.abort_handle(),
            queue.abort_handle(),
            server.abort_handle(),
            gossip.abort_handle(),
        ];
        Self {
            network,
            rpc: rpc_addr,
            auth,
            p2p,
            peers,
            endpoint,
            native_startup,
            native_violations,
            advertisements,
            state,
            verifier: gated,
            tip,
            pending,
            bans,
            gate: ValidationGate {
                held,
                entered: entered_rx,
                finished: finished_rx,
                failures: failures_rx,
            },
            hold_relay,
            held_relays,
            hold_body,
            body_requests,
            hold_parent_lookup,
            parent_lookups,
            tasks: tasks.into_iter().chain(native_tasks).collect(),
            _recent: recent,
            _directory: directory,
        }
    }

    async fn template(&self) -> Block {
        let (status, bytes) = http(
            self.rpc,
            &self.auth,
            "/",
            "application/json",
            br#"{"jsonrpc":"2.0","id":1,"method":"getblocktemplate","params":[]}"#.to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let response: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let template = serde_json::from_value(response["result"].clone())
            .unwrap_or_else(|_| panic!("getblocktemplate failed: {response}"));
        proposal_block_from_template(&template, BlockTemplateTimeSource::default(), &self.network)
            .unwrap()
    }

    async fn submit(&self, bytes: Vec<u8>, trusted: bool) -> String {
        submit(self.rpc, self.auth.clone(), bytes, trusted).await
    }

    async fn connected(&self) {
        timeout(Duration::from_secs(20), self.peers.clone().ready())
            .await
            .unwrap()
            .unwrap();
    }
}

/// Corrupts the body while preserving the header hash, exercising corrected-body recovery.
fn faulty_body(block: &Block) -> Block {
    let mut faulty = block.clone();
    Arc::make_mut(&mut faulty.transactions[0])
        .outputs_mut()
        .clear();
    assert_eq!(faulty.hash(), block.hash());
    assert_ne!(
        faulty
            .transactions
            .iter()
            .collect::<zakura_chain::block::merkle::Root>(),
        faulty.header.merkle_root
    );
    faulty
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_rejection_cancels_held_relay_and_withdraws_body_before_peer_delivery() {
    let _init = zakura_test::init();
    let mut relay = Node::new(None).await;
    let mut receiver = Node::new(Some(relay.p2p)).await;
    relay.connected().await;
    receiver.connected().await;
    let valid = relay.template().await;
    let invalid = faulty_body(&valid);
    relay.hold_relay.send_replace(true);
    relay.gate.hold();
    let submission = tokio::spawn(submit(
        relay.rpc,
        relay.auth.clone(),
        invalid.zcash_serialize_to_vec().unwrap(),
        true,
    ));
    assert_eq!(relay.gate.entered().await, invalid.hash());
    let held = timeout(Duration::from_secs(20), relay.held_relays.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.hash, invalid.hash());
    assert!(relay.pending.get(invalid.hash()).is_some());
    assert!(!submission.is_finished());
    relay.gate.release();
    assert_eq!(submission.await.unwrap(), "rejected");
    timeout(Duration::from_secs(20), held.cancelled)
        .await
        .unwrap()
        .expect_err("rejection drops the relay future before the peer set receives it");
    assert!(
        held.release.send(()).is_err(),
        "releasing the barrier cannot revive cancelled relay"
    );
    assert!(relay.pending.get(invalid.hash()).is_none());
    let observer = zakura_network::connect_isolated_with_inbound(
        &relay.network,
        tokio::net::TcpStream::connect(relay.p2p).await.unwrap(),
        "withdrawal observer".into(),
        tower::service_fn(|_| async { Ok::<_, BoxError>(Response::Nil) }),
    )
    .await
    .unwrap();
    let observer = Buffer::new(observer, 4);
    let missing = timeout(
        Duration::from_secs(20),
        observer.clone().oneshot(Request::BlocksByHash(
            [invalid.hash()].into_iter().collect(),
        )),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        missing
            .downcast_ref::<zakura_network::SharedPeerError>()
            .unwrap()
            .not_found_class(),
        Some(zakura_network::NotFoundClass::Response)
    );
    assert!(receiver.advertisements.try_recv().is_err());
    assert_eq!(receiver.tip.best_tip_height(), Some(Height(0)));
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
    assert!(!receiver.bans.is_banned(relay.p2p.ip()));
    timeout(
        Duration::from_secs(20),
        receiver.peers.clone().oneshot(Request::Peers),
    )
    .await
    .unwrap()
    .expect("withdrawing an unsent invalid block leaves the peer connected");

    // Withdrawal must not suppress a corrected submission with the same header hash.
    relay.hold_relay.send_replace(false);
    relay.gate.hold();
    let bytes = valid.zcash_serialize_to_vec().unwrap();
    let retry = tokio::spawn(submit(relay.rpc, relay.auth.clone(), bytes.clone(), true));
    assert_eq!(relay.gate.entered().await, valid.hash());
    assert_eq!(
        timeout(Duration::from_secs(20), receiver.advertisements.recv())
            .await
            .unwrap(),
        Some(valid.hash())
    );
    timeout(Duration::from_secs(20), async {
        while receiver.tip.best_tip_hash() != Some(valid.hash()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let zakura_state::Response::Block(Some(fetched)) = receiver
        .state
        .clone()
        .oneshot(zakura_state::Request::Block(valid.hash().into()))
        .await
        .unwrap()
    else {
        panic!("the corrected body is fetched and committed by the receiver")
    };
    assert_eq!(fetched.zcash_serialize_to_vec().unwrap(), bytes);
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
    assert!(!retry.is_finished());
    relay.gate.release();
    assert_eq!(retry.await.unwrap(), "accepted");
    assert_eq!(relay.tip.best_tip_hash(), Some(valid.hash()));
    assert!(relay.pending.get(valid.hash()).is_none());
    assert!(!receiver.bans.is_banned(relay.p2p.ip()));
}

#[tokio::test(flavor = "multi_thread")]
async fn faulty_trusted_block_bans_relay_and_closes_connections_then_valid_retry_commits() {
    let _init = zakura_test::init();
    let mut relay = Node::new(None).await;
    let receiver = Node::new(Some(relay.p2p)).await;
    relay.connected().await;
    receiver.connected().await;
    // This second real socket shares the relay's IP. The ban must close it as well.
    let observer = zakura_network::connect_isolated_with_inbound(
        &receiver.network,
        tokio::net::TcpStream::connect(receiver.p2p).await.unwrap(),
        "ban observer".into(),
        tower::service_fn(|_| async { Ok::<_, BoxError>(Response::Nil) }),
    )
    .await
    .unwrap();
    let observer = Buffer::new(observer, 4);
    observer.clone().oneshot(Request::Peers).await.unwrap();
    let valid = relay.template().await;
    let invalid = faulty_body(&valid);
    relay.gate.hold();
    let submission = tokio::spawn(submit(
        relay.rpc,
        relay.auth.clone(),
        invalid.zcash_serialize_to_vec().unwrap(),
        true,
    ));
    assert_eq!(relay.gate.entered().await, invalid.hash());
    timeout(Duration::from_secs(20), async {
        while !receiver.bans.is_banned(relay.p2p.ip()) {
            // Poll the production inbound service while its download completes.
            let _ = observer.clone().oneshot(Request::Peers).await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(receiver.tip.best_tip_height(), Some(Height(0)));
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
    assert!(!submission.is_finished());
    timeout(Duration::from_secs(10), async {
        loop {
            if observer.clone().oneshot(Request::Peers).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the IP ban closes the established peer socket");
    use tokio::io::AsyncReadExt;
    let mut refused = tokio::net::TcpStream::connect(receiver.p2p).await.unwrap();
    let mut byte = [0];
    let read = timeout(Duration::from_secs(5), refused.read(&mut byte))
        .await
        .unwrap();
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "a banned IP cannot start another handshake"
    );
    relay.gate.release();
    assert_eq!(submission.await.unwrap(), "rejected");
    assert!(relay.pending.get(invalid.hash()).is_none());
    let cleanup_peer = zakura_network::connect_isolated_with_inbound(
        &relay.network,
        tokio::net::TcpStream::connect(relay.p2p).await.unwrap(),
        "cleanup observer".into(),
        tower::service_fn(|_| async { Ok::<_, BoxError>(Response::Nil) }),
    )
    .await
    .unwrap();
    let cleanup_peer = Buffer::new(cleanup_peer, 4);
    let missing = cleanup_peer
        .clone()
        .oneshot(Request::BlocksByHash(
            [invalid.hash()].into_iter().collect(),
        ))
        .await
        .unwrap_err();
    assert_eq!(
        missing
            .downcast_ref::<zakura_network::SharedPeerError>()
            .unwrap()
            .not_found_class(),
        Some(zakura_network::NotFoundClass::Response)
    );
    // A separate receiver has no previous notfound routing record for this hash.
    let mut fresh_receiver = Node::new(Some(relay.p2p)).await;
    fresh_receiver.connected().await;
    relay.gate.hold();
    let retry = tokio::spawn(submit(
        relay.rpc,
        relay.auth.clone(),
        valid.zcash_serialize_to_vec().unwrap(),
        true,
    ));
    assert_eq!(relay.gate.entered().await, valid.hash());
    assert_eq!(
        timeout(
            Duration::from_secs(20),
            fresh_receiver.advertisements.recv()
        )
        .await
        .unwrap(),
        Some(valid.hash())
    );
    let progress_peer = zakura_network::connect_isolated_with_inbound(
        &fresh_receiver.network,
        tokio::net::TcpStream::connect(fresh_receiver.p2p)
            .await
            .unwrap(),
        "recovery observer".into(),
        tower::service_fn(|_| async { Ok::<_, BoxError>(Response::Nil) }),
    )
    .await
    .unwrap();
    let progress_peer = Buffer::new(progress_peer, 4);
    timeout(Duration::from_secs(20), async {
        while fresh_receiver.tip.best_tip_hash() != Some(valid.hash()) {
            progress_peer.clone().oneshot(Request::Peers).await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(
        !retry.is_finished(),
        "a later valid block still relays before local verification"
    );
    relay.gate.release();
    assert_eq!(retry.await.unwrap(), "accepted");
    assert_eq!(relay.tip.best_tip_hash(), Some(valid.hash()));
    assert!(relay.pending.get(valid.hash()).is_none());
    let child = relay.template().await;
    assert_eq!(child.header.previous_block_hash, valid.hash());
    let child_bytes = child.zcash_serialize_to_vec().unwrap();
    relay.gate.hold();
    let descendant = tokio::spawn(submit(
        relay.rpc,
        relay.auth.clone(),
        child_bytes.clone(),
        true,
    ));
    assert_eq!(relay.gate.entered().await, child.hash());
    timeout(Duration::from_secs(20), async {
        loop {
            let announced = fresh_receiver.advertisements.recv().await.unwrap();
            if announced == child.hash() {
                break;
            }
            // Committing the corrected parent can also publish its committed announcement.
            assert_eq!(announced, valid.hash());
        }
    })
    .await
    .expect("the descendant is announced while its local validation is held");
    timeout(Duration::from_secs(20), async {
        while fresh_receiver.tip.best_tip_hash() != Some(child.hash()) {
            progress_peer.clone().oneshot(Request::Peers).await.unwrap();
        }
    })
    .await
    .unwrap();
    let zakura_state::Response::Block(Some(fetched)) = fresh_receiver
        .state
        .clone()
        .oneshot(zakura_state::Request::Block(child.hash().into()))
        .await
        .unwrap()
    else {
        panic!("the receiver committed the descendant fetched before relay validation")
    };
    assert_eq!(fetched.zcash_serialize_to_vec().unwrap(), child_bytes);
    assert_eq!(relay.tip.best_tip_hash(), Some(valid.hash()));
    assert!(!descendant.is_finished());
    relay.gate.release();
    assert_eq!(descendant.await.unwrap(), "accepted");
    assert_eq!(relay.tip.best_tip_height(), Some(Height(2)));
    assert!(relay.pending.get(child.hash()).is_none());
    assert!(!fresh_receiver.bans.is_banned(relay.p2p.ip()));
    progress_peer.clone().oneshot(Request::Peers).await.unwrap();
}

/// The documented pool workflow forwards the original byte buffer only on acceptance.
async fn validate_then_relay(
    upstream: (SocketAddr, String),
    relay: (SocketAddr, String),
    bytes: Vec<u8>,
) -> String {
    let result = timeout(
        Duration::from_secs(8 * 60),
        submit(upstream.0, upstream.1, bytes.clone(), false),
    )
    .await
    .unwrap_or_else(|_| "inconclusive".to_owned());
    if result != "accepted" {
        return result;
    }
    submit(relay.0, relay.1, bytes, true).await
}

async fn recommended_sequence(separate_validator: bool, faulty: bool) {
    let _init = zakura_test::init();
    let builder = Node::new(None).await;
    let valid = builder.template().await;
    let block = if faulty { faulty_body(&valid) } else { valid };
    let bytes = block.zcash_serialize_to_vec().unwrap();
    let mut upstream = if separate_validator {
        Node::new(None).await
    } else {
        builder
    };
    let mut relay = Node::new(None).await;
    let mut receiver = Node::new(Some(relay.p2p)).await;
    relay.connected().await;
    receiver.connected().await;
    upstream.gate.hold();
    relay.gate.hold();
    let workflow = tokio::spawn(validate_then_relay(
        (upstream.rpc, upstream.auth.clone()),
        (relay.rpc, relay.auth.clone()),
        bytes.clone(),
    ));
    assert_eq!(upstream.gate.entered().await, block.hash());
    assert!(relay.gate.entered.try_recv().is_err());
    assert!(relay.pending.get(block.hash()).is_none());
    assert_eq!(receiver.tip.best_tip_height(), Some(Height(0)));
    upstream.gate.release();
    if faulty {
        assert_eq!(workflow.await.unwrap(), "rejected");
        assert!(relay.gate.entered.try_recv().is_err());
        assert!(relay.pending.get(block.hash()).is_none());
        assert_eq!(receiver.tip.best_tip_height(), Some(Height(0)));
        assert!(!receiver.bans.is_banned(relay.p2p.ip()));
        assert!(receiver.advertisements.try_recv().is_err());
        assert!(receiver.gate.entered.try_recv().is_err());
    } else {
        assert_eq!(relay.gate.entered().await, block.hash());
        assert_eq!(upstream.tip.best_tip_hash(), Some(block.hash()));
        timeout(Duration::from_secs(20), async {
            while receiver.tip.best_tip_hash() != Some(block.hash()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            relay
                .pending
                .get(block.hash())
                .unwrap()
                .zcash_serialize_to_vec()
                .unwrap(),
            bytes
        );
        assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
        assert!(!workflow.is_finished());
        relay.gate.release();
        assert_eq!(workflow.await.unwrap(), "accepted");
        assert!(relay.pending.get(block.hash()).is_none());
        assert!(!receiver.bans.is_banned(relay.p2p.ip()));
        assert_eq!(
            timeout(Duration::from_secs(20), receiver.advertisements.recv())
                .await
                .unwrap(),
            Some(block.hash())
        );
    }
    timeout(
        Duration::from_secs(20),
        receiver.peers.clone().oneshot(Request::Peers),
    )
    .await
    .unwrap()
    .expect("the downstream peer connection remains usable");
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_relay_after_gbt_validation() {
    recommended_sequence(false, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn faulty_trusted_block_over_v2_compatibility_is_rejected_without_legacy_ip_ban() {
    let _init = zakura_test::init();
    let mut relay = Node::with_stack(None, P2pStack::Dual).await;
    let mut receiver = Node::with_stack(None, P2pStack::Dual).await;
    let relay_endpoint = relay.endpoint.as_ref().unwrap();
    let receiver_endpoint = receiver.endpoint.as_ref().unwrap();
    let dial = relay_endpoint.spawn_native_dial(receiver_endpoint.node_addr().await);
    relay.connected().await;
    receiver.connected().await;
    let valid = relay.template().await;
    let invalid = faulty_body(&valid);
    relay.gate.hold();
    let submission = tokio::spawn(submit(
        relay.rpc,
        relay.auth.clone(),
        invalid.zcash_serialize_to_vec().unwrap(),
        true,
    ));
    relay.gate.entered().await;
    let peer_id = timeout(Duration::from_secs(20), async {
        loop {
            if let Some(id) = receiver_endpoint
                .supervisor()
                .registered_ids()
                .await
                .first()
            {
                break id.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Production ignores compatibility advertisements: native sync owns automatic
    // downloads. An explicit compatibility request still serves the pending body.
    let response = timeout(
        Duration::from_secs(20),
        receiver.peers.clone().oneshot(Request::BlocksByHashFrom {
            hashes: [invalid.hash()].into_iter().collect(),
            source: zakura_network::PeerSource::Zakura(peer_id),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let Response::Blocks(blocks) = response else {
        panic!("expected block response")
    };
    let zakura_network::InventoryResponse::Available((fetched, _)) = &blocks[0] else {
        panic!("pending body must be available")
    };
    assert_eq!(
        fetched.zcash_serialize_to_vec().unwrap(),
        invalid.zcash_serialize_to_vec().unwrap()
    );
    assert!(receiver
        .verifier
        .clone()
        .oneshot(VerifyRequest::Commit(fetched.block.clone()))
        .await
        .is_err());
    let verified = timeout(Duration::from_secs(20), receiver.gate.finished.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(verified, (invalid.hash(), false));
    assert!(!receiver.bans.is_banned(relay.p2p.ip()));
    assert_eq!(
        receiver_endpoint.supervisor().registered_ids().await.len(),
        1
    );
    assert_eq!(receiver.tip.best_tip_height(), Some(Height(0)));
    relay.gate.release();
    assert_eq!(submission.await.unwrap(), "rejected");
    relay_endpoint.shutdown().await;
    receiver_endpoint.shutdown().await;
    dial.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_relay_after_separate_validation() {
    recommended_sequence(true, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_relay_skipped_after_gbt_rejection() {
    recommended_sequence(false, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_relay_skipped_after_separate_validator_rejection() {
    recommended_sequence(true, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_http_rejects_malformed_bytes_and_preserves_validation_after_client_disconnect() {
    let _init = zakura_test::init();
    let mut relay = Node::new(None).await;
    let block = relay.template().await;
    let bytes = block.zcash_serialize_to_vec().unwrap();
    let mut trailing = bytes.clone();
    trailing.push(0);
    for body in [vec![0, 1, 2], trailing] {
        assert_eq!(
            http(
                relay.rpc,
                &relay.auth,
                "/submittrustedblock",
                "application/octet-stream",
                body
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert!(relay.pending.get(block.hash()).is_none());
    relay.gate.hold();
    let submission = tokio::spawn(submit(relay.rpc, relay.auth.clone(), bytes.clone(), true));
    relay.gate.entered().await;
    assert_eq!(
        relay
            .submit(faulty_body(&block).zcash_serialize_to_vec().unwrap(), true)
            .await,
        "duplicate-inconclusive"
    );
    assert_eq!(
        relay
            .pending
            .get(block.hash())
            .unwrap()
            .zcash_serialize_to_vec()
            .unwrap(),
        bytes,
        "a concurrent duplicate must not replace the owner's body"
    );
    submission.abort();
    relay.gate.release();
    timeout(Duration::from_secs(20), async {
        while relay.tip.best_tip_hash() != Some(block.hash())
            || relay.pending.get(block.hash()).is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(relay.submit(bytes, true).await, "duplicate");
    let response = relay
        .state
        .clone()
        .oneshot(zakura_state::Request::AnyChainBlock(block.hash().into()))
        .await
        .unwrap();
    assert!(matches!(response, zakura_state::Response::Block(Some(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_block_with_unknown_parent_is_not_relayed_early() {
    let _init = zakura_test::init();
    // An isolated miner builds a parent and child that the relay has not seen.
    let miner = Node::new(None).await;
    let parent = miner.template().await;
    assert_eq!(
        miner
            .submit(parent.zcash_serialize_to_vec().unwrap(), false)
            .await,
        "accepted"
    );
    let child = miner.template().await;
    assert_eq!(child.header.previous_block_hash, parent.hash());
    let child_bytes = child.zcash_serialize_to_vec().unwrap();

    let mut relay = Node::new(None).await;
    let mut receiver = Node::new(Some(relay.p2p)).await;
    relay.connected().await;
    receiver.connected().await;

    relay.gate.hold();
    let orphan = tokio::spawn(submit(
        relay.rpc,
        relay.auth.clone(),
        child_bytes.clone(),
        true,
    ));
    assert_eq!(relay.gate.entered().await, child.hash());
    // Early relay would reach the receiver within milliseconds while validation is held.
    assert!(
        timeout(Duration::from_secs(2), receiver.advertisements.recv())
            .await
            .is_err(),
        "a block whose parent is unknown is not announced before validation"
    );
    relay.gate.release();
    assert_eq!(orphan.await.unwrap(), "inconclusive");
    assert!(relay.pending.get(child.hash()).is_none());

    assert_eq!(
        relay
            .submit(parent.zcash_serialize_to_vec().unwrap(), true)
            .await,
        "accepted"
    );
    assert_eq!(relay.gate.entered().await, parent.hash());
    assert_eq!(
        timeout(Duration::from_secs(20), receiver.advertisements.recv())
            .await
            .unwrap(),
        Some(parent.hash())
    );
    // This fixture has no syncer, so the receiver must commit the parent before it can accept
    // the child.
    timeout(Duration::from_secs(20), async {
        while receiver.tip.best_tip_hash() != Some(parent.hash()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the receiver fetches the parent from the relay");

    // Once the parent is committed, the same child is relayed early again.
    relay.gate.hold();
    let retry = tokio::spawn(submit(relay.rpc, relay.auth.clone(), child_bytes, true));
    assert_eq!(relay.gate.entered().await, child.hash());
    // The parent can be announced twice: early, then after it commits.
    timeout(Duration::from_secs(20), async {
        loop {
            match receiver.advertisements.recv().await {
                Some(hash) if hash == child.hash() => break,
                Some(hash) => assert_eq!(hash, parent.hash()),
                None => panic!("the receiver's inbound service stopped"),
            }
        }
    })
    .await
    .expect("the child is announced while its validation is held");
    relay.gate.release();
    assert_eq!(retry.await.unwrap(), "accepted");
    timeout(Duration::from_secs(20), async {
        while receiver.tip.best_tip_hash() != Some(child.hash()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the receiver fetches the retried child from the relay");
}

#[tokio::test]
async fn stalled_parent_lookup_still_starts_validation_and_releases_submission_capacity() {
    let _init = zakura_test::init();
    let mut node = Node::new(None).await;
    let block = faulty_body(&node.template().await);
    let bytes = block.zcash_serialize_to_vec().unwrap();
    node.gate.hold();
    node.hold_relay.send_replace(true);
    node.hold_parent_lookup.send_replace(true);
    let submission = tokio::spawn(submit(node.rpc, node.auth.clone(), bytes.clone(), true));
    timeout(Duration::from_secs(2), node.parent_lookups.recv())
        .await
        .unwrap()
        .unwrap();

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    assert_eq!(
        timeout(Duration::from_secs(2), node.gate.entered.recv())
            .await
            .expect("a stalled optional parent lookup cannot prevent validation")
            .unwrap(),
        block.hash()
    );
    assert!(node.pending.get(block.hash()).is_some());
    assert!(node.held_relays.try_recv().is_err());

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(8 * 60 + 1)).await;
    tokio::time::resume();
    assert_eq!(submission.await.unwrap(), "inconclusive");
    assert!(node.pending.get(block.hash()).is_none());
    assert_eq!(node.tip.best_tip_height(), Some(Height(0)));

    node.hold_parent_lookup.send_replace(false);
    node.gate.release();
    node.hold_relay.send_replace(false);
    assert_eq!(node.submit(bytes, true).await, "rejected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_parent_lookup_does_not_prevent_valid_commit() {
    let _init = zakura_test::init();
    let node = Node::new(None).await;
    let block = node.template().await;
    node.hold_parent_lookup.send_replace(true);
    assert_eq!(
        timeout(
            Duration::from_secs(10),
            node.submit(block.zcash_serialize_to_vec().unwrap(), true),
        )
        .await
        .expect("a stalled parent lookup cannot prevent a valid block from committing"),
        "accepted"
    );
    assert!(node.pending.get(block.hash()).is_none());
    assert_eq!(node.tip.best_tip_hash(), Some(block.hash()));
}

#[tokio::test]
async fn trusted_validation_timeout_withdraws_body_and_allows_retry() {
    let _init = zakura_test::init();
    let mut node = Node::new(None).await;
    let valid = node.template().await;
    let invalid = faulty_body(&valid);
    node.gate.hold();
    let submission = tokio::spawn(submit(
        node.rpc,
        node.auth.clone(),
        invalid.zcash_serialize_to_vec().unwrap(),
        true,
    ));
    assert_eq!(node.gate.entered().await, invalid.hash());
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(8 * 60 + 1)).await;
    tokio::time::resume();
    assert_eq!(submission.await.unwrap(), "inconclusive");
    assert!(node.pending.get(invalid.hash()).is_none());
    assert_eq!(node.tip.best_tip_height(), Some(Height(0)));
    node.gate.release();
    assert_eq!(
        node.submit(invalid.zcash_serialize_to_vec().unwrap(), true)
            .await,
        "rejected"
    );
}

#[tokio::test]
async fn upstream_timeout_stops_trusted_fanout() {
    let _init = zakura_test::init();
    let mut upstream = Node::new(None).await;
    let mut relay = Node::new(None).await;
    let block = upstream.template().await;
    upstream.gate.hold();
    let workflow = tokio::spawn(validate_then_relay(
        (upstream.rpc, upstream.auth.clone()),
        (relay.rpc, relay.auth.clone()),
        block.zcash_serialize_to_vec().unwrap(),
    ));
    upstream.gate.entered().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(8 * 60 + 1)).await;
    tokio::time::resume();
    assert_eq!(workflow.await.unwrap(), "inconclusive");
    assert!(relay.gate.entered.try_recv().is_err());
    assert!(relay.pending.get(block.hash()).is_none());
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
    upstream.gate.release();
}

#[tokio::test(flavor = "multi_thread")]
async fn upstream_duplicate_does_not_authorize_the_submitted_body() {
    let _init = zakura_test::init();
    let upstream = Node::new(None).await;
    let mut relay = Node::new(None).await;
    let block = upstream.template().await;
    assert_eq!(
        upstream
            .submit(block.zcash_serialize_to_vec().unwrap(), false)
            .await,
        "accepted"
    );
    let result = validate_then_relay(
        (upstream.rpc, upstream.auth.clone()),
        (relay.rpc, relay.auth.clone()),
        faulty_body(&block).zcash_serialize_to_vec().unwrap(),
    )
    .await;
    assert_eq!(result, "duplicate");
    assert!(relay.gate.entered.try_recv().is_err());
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
}

#[tokio::test(flavor = "multi_thread")]
async fn full_pending_registry_rejects_before_verification_and_recovers() {
    let _init = zakura_test::init();
    let mut node = Node::new(None).await;
    let valid = node.template().await;
    let mut reservations = vec![];
    for nonce in 1..=16 {
        let mut block = valid.clone();
        Arc::make_mut(&mut block.header).nonce[0] = nonce;
        reservations.push(node.pending.insert(Arc::new(block)).unwrap());
    }
    let (status, _) = http(
        node.rpc,
        &node.auth,
        "/submittrustedblock",
        "application/octet-stream",
        valid.zcash_serialize_to_vec().unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(node.gate.entered.try_recv().is_err());
    drop(reservations);
    assert_eq!(
        node.submit(valid.zcash_serialize_to_vec().unwrap(), true)
            .await,
        "accepted"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_valid_competing_block_commits_under_normal_chain_rules() {
    let _init = zakura_test::init();
    let node = Node::new(None).await;
    let block = node.template().await;
    let mut competitor = block.clone();
    Arc::make_mut(&mut competitor.header).nonce[0] ^= 1;
    assert_eq!(
        node.submit(block.zcash_serialize_to_vec().unwrap(), false)
            .await,
        "accepted"
    );
    assert_eq!(
        node.submit(competitor.zcash_serialize_to_vec().unwrap(), true)
            .await,
        "accepted"
    );
    assert!(matches!(
        node.state
            .clone()
            .oneshot(zakura_state::Request::KnownBlock(competitor.hash()))
            .await
            .unwrap(),
        zakura_state::Response::KnownBlock(Some(
            zakura_state::KnownBlock::BestChain | zakura_state::KnownBlock::SideChain
        ))
    ));
    assert_eq!(node.tip.best_tip_height(), Some(Height(1)));
    assert!(!node.bans.is_banned(node.p2p.ip()));
}

fn basic_auth(bytes: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    format!("Basic {}", STANDARD.encode(bytes))
}

async fn http(
    address: SocketAddr,
    auth: &str,
    path: &str,
    content_type: &str,
    bytes: Vec<u8>,
) -> (StatusCode, Vec<u8>) {
    timeout(Duration::from_secs(9 * 60), async {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(connection));
        let response = client
            .send_request(
                HttpRequest::post(path)
                    .header("Host", address.to_string())
                    .header("Authorization", auth)
                    .header("Content-Type", content_type)
                    .body(Full::new(Bytes::from(bytes)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        task.abort();
        (status, body)
    })
    .await
    .unwrap()
}

async fn submit(address: SocketAddr, auth: String, bytes: Vec<u8>, trusted: bool) -> String {
    if trusted {
        let (status, response) = http(
            address,
            &auth,
            "/submittrustedblock",
            "application/octet-stream",
            bytes,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        String::from_utf8(response).unwrap()
    } else {
        let request = serde_json::json!({"jsonrpc":"2.0", "id":1, "method":"submitblock", "params":[hex::encode(bytes)]});
        let (status, response) = http(
            address,
            &auth,
            "/",
            "application/json",
            serde_json::to_vec(&request).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
        assert!(response.get("error").is_none(), "{response}");
        match serde_json::from_value::<SubmitBlockResponse>(response["result"].clone()).unwrap() {
            SubmitBlockResponse::Accepted => "accepted".into(),
            SubmitBlockResponse::ErrorResponse(error) => serde_json::to_value(error)
                .unwrap()
                .as_str()
                .unwrap()
                .into(),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_block_is_relayed_before_real_validation_and_commits_afterward() {
    let _init = zakura_test::init();
    let mut relay = Node::new(None).await;
    let mut receiver = Node::new(Some(relay.p2p)).await;
    relay.connected().await;
    receiver.connected().await;
    let block = relay.template().await;
    let bytes = block.zcash_serialize_to_vec().unwrap();
    relay.gate.hold();
    let submission = tokio::spawn(submit(relay.rpc, relay.auth.clone(), bytes.clone(), true));
    assert_eq!(relay.gate.entered().await, block.hash());
    assert_eq!(
        timeout(Duration::from_secs(20), receiver.advertisements.recv())
            .await
            .unwrap(),
        Some(block.hash())
    );
    timeout(Duration::from_secs(20), async {
        while receiver.tip.best_tip_hash() != Some(block.hash()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
    assert!(!submission.is_finished());
    let zakura_state::Response::Block(Some(fetched)) = receiver
        .state
        .clone()
        .oneshot(zakura_state::Request::Block(block.hash().into()))
        .await
        .unwrap()
    else {
        panic!("receiver committed the fetched block")
    };
    assert_eq!(fetched.zcash_serialize_to_vec().unwrap(), bytes);
    assert_eq!(
        relay.template().await.header.previous_block_hash,
        relay.network.genesis_hash(),
        "a pending trusted block cannot become a template parent"
    );
    assert_eq!(
        relay
            .pending
            .get(block.hash())
            .unwrap()
            .zcash_serialize_to_vec()
            .unwrap(),
        bytes
    );
    relay.gate.release();
    assert_eq!(submission.await.unwrap(), "accepted");
    assert_eq!(relay.tip.best_tip_hash(), Some(block.hash()));
    assert!(relay.pending.get(block.hash()).is_none());
    assert!(!receiver.bans.is_banned(relay.p2p.ip()));
    timeout(
        Duration::from_secs(20),
        receiver.peers.clone().oneshot(Request::Peers),
    )
    .await
    .unwrap()
    .expect("the downstream peer connection remains usable");
}
