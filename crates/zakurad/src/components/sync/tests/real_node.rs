//! Regtest nodes built from real state, consensus, inbound, gossip and TCP peers.
//!
//! The only test hooks are wrappers around production services: one holds block commits
//! before the real router sees them, the other holds a node's replies to block requests.
//! Neither supplies a block or a verification result.

#![allow(clippy::unwrap_in_result)]

mod downloads;

use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use bytes::BytesMut;
use futures::FutureExt;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
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
    serialization::{ZcashDeserialize, ZcashSerialize},
    transparent::Address,
};
use zakura_consensus::{router::RouterError, Request as VerifyRequest};
use zakura_network::{
    types::{InventoryHash, PeerServices},
    P2pStack, Request, Response,
};
use zakura_rpc::{
    client::{BlockTemplateTimeSource, GetBlockTemplateResponse, SubmitBlockResponse},
    methods::{RpcImpl, RpcServer},
    proposal_block_from_template, EarlyMinedBlockChannel,
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
type Rpc = RpcImpl<
    Buffer<
        zakura_node_services::mempool::Request,
        futures::future::BoxFuture<
            'static,
            Result<zakura_node_services::mempool::Response, BoxError>,
        >,
    >,
    State,
    zakura_state::ReadStateService,
    zakura_state::LatestChainTip,
    zakura_network::address_book_peers::MockAddressBookPeers,
    Verifier,
    SyncStatus,
>;

/// Holds block commits before passing them to the real consensus router.
struct ValidationGate {
    held: watch::Sender<bool>,
    entered: mpsc::UnboundedReceiver<zakura_chain::block::Hash>,
}

impl ValidationGate {
    fn hold(&self) {
        self.held.send_replace(true);
    }

    fn release(&self) {
        self.held.send_replace(false);
    }

    /// Waits until a commit for some block reaches the gate, and returns that block's hash.
    async fn entered(&mut self) -> zakura_chain::block::Hash {
        timeout(Duration::from_secs(20), self.entered.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

/// A Regtest node with the production inbound service, ban reporter, gossip and verifier.
struct Node {
    network: Network,
    p2p: SocketAddr,
    peers: Peers,
    rpc: Rpc,
    state: State,
    verifier: Verifier,
    tip: zakura_state::LatestChainTip,
    bans: zakura_network::MisbehaviorReporter,
    gate: ValidationGate,
    /// While true, this node delays its replies to block requests from peers.
    hold_body: watch::Sender<bool>,
    /// Every block hash peers requested from this node, in request order.
    body_requests: mpsc::UnboundedReceiver<zakura_chain::block::Hash>,
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
    async fn new() -> Self {
        let network = Network::new_regtest(
            ConfiguredActivationHeights {
                nu5: Some(1),
                ..Default::default()
            }
            .into(),
        );
        let genesis = zakura_chain::block::genesis::regtest_genesis_block();
        assert_eq!(network.genesis_hash(), genesis.hash());
        let (
            zakura_state::InitializedState {
                state,
                read_state,
                latest_chain_tip: tip,
                chain_tip_change,
                block_announcements,
            },
            _body_authority,
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
        let (verifier, tx_verifier, background, _) = zakura_consensus::router::init_test(
            zakura_consensus::Config::default(),
            &network,
            state.clone(),
        )
        .await;
        let (setup_tx, setup_rx) = oneshot::channel();
        let inbound = Buffer::new(
            BoxService::new(Inbound::new(8, false, None, vec![], setup_rx)),
            16,
        );
        let (hold_body, body_held) = watch::channel(false);
        let (body_requested, body_requests) = mpsc::unbounded_channel();
        let inbound = tower::service_fn(move |request: Request| {
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
            initial_testnet_peers: Default::default(),
            cache_dir: zakura_network::CacheDir::disabled(),
            identity_dir: directory.path().join("identity"),
            p2p_stack: P2pStack::Legacy,
            max_connections_per_ip: 8,
            ..Default::default()
        };
        let (peers, address_book, bans, _endpoint) = zakura_network::init_with_zakura_header_sync(
            config,
            inbound,
            tip.clone(),
            "real node test".into(),
            PeerServices::NODE_NETWORK,
            vec![],
            None,
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
        let gate_sender = held.clone();
        let real_verifier: Verifier = verifier.clone();
        let gated = tower::service_fn(move |request: VerifyRequest| {
            let verifier = real_verifier.clone();
            let mut held = gate_sender.subscribe();
            let entered = entered.clone();
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
                verification.await.map_err(|error| {
                    *error
                        .downcast::<RouterError>()
                        .expect("real router returns RouterError")
                })
            }
            .boxed()
        });
        let gated = Buffer::new(gated, 16);
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
        let (rpc, queue) = RpcImpl::new(
            network.clone(),
            zakura_rpc::config::mining::Config {
                miner_address: Some(Address::from_pub_key_hash(network.kind(), [7; 20]).into()),
                ..Default::default()
            },
            false,
            "test",
            "real node test",
            pool,
            state.clone(),
            read_state.clone(),
            gated.clone(),
            sync.clone(),
            tip.clone(),
            zakura_network::address_book_peers::MockAddressBookPeers::default(),
            log_rx,
            Some(early.sender()),
        );
        let gossip = tokio::spawn(gossip_best_tip_block_hashes(
            sync,
            tip.clone(),
            block_announcements,
            peers.clone(),
            read_state,
            Some(early.receiver()),
        ));
        let tasks = vec![
            background.state_checkpoint_verify_handle.abort_handle(),
            queue.abort_handle(),
            gossip.abort_handle(),
        ];
        Self {
            network,
            p2p,
            peers,
            rpc,
            state,
            verifier: gated,
            tip,
            bans,
            gate: ValidationGate {
                held,
                entered: entered_rx,
            },
            hold_body,
            body_requests,
            tasks,
            _recent: recent,
            _directory: directory,
        }
    }

    /// A block built from this node's `getblocktemplate` response.
    async fn template(&self) -> Block {
        let GetBlockTemplateResponse::TemplateMode(template) =
            self.rpc.get_block_template(None).await.unwrap()
        else {
            panic!("getblocktemplate without parameters answers in template mode")
        };
        proposal_block_from_template(&template, BlockTemplateTimeSource::default(), &self.network)
            .unwrap()
    }

    /// Submits a block through `submitblock`, returning "accepted" or the rejection reason.
    async fn submit(&self, block: &Block) -> String {
        let hex = serde_json::json!(hex::encode(block.zcash_serialize_to_vec().unwrap()));
        match self
            .rpc
            .submit_block(serde_json::from_value(hex).unwrap(), None)
            .await
            .unwrap()
        {
            SubmitBlockResponse::Accepted => "accepted".into(),
            SubmitBlockResponse::ErrorResponse(error) => serde_json::to_value(error)
                .unwrap()
                .as_str()
                .unwrap()
                .into(),
        }
    }

    async fn connected(&self) {
        timeout(Duration::from_secs(20), self.peers.clone().ready())
            .await
            .unwrap()
            .unwrap();
    }
}

/// A nearly full block with an unspendable coinbase output.
fn large_block(mut block: Block) -> Block {
    let mut script = vec![0; 1_990_000];
    script[0] = 0x6a; // OP_RETURN: padding is data in an output, never executed.
    Arc::make_mut(&mut block.transactions[0]).outputs_mut()[0].lock_script =
        zakura_chain::transparent::Script::new(&script);
    Arc::make_mut(&mut block.header).merkle_root = block.transactions.iter().collect();
    block
}

#[derive(Clone, Debug)]
struct WireEvent {
    kind: &'static str,
    hash: zakura_chain::block::Hash,
    bytes: usize,
}

/// Decodes copies of the actual socket bytes, leaving the production transport unchanged.
struct Capture {
    buffered: BytesMut,
    events: Arc<Mutex<Vec<WireEvent>>>,
}

impl Capture {
    fn new(events: Arc<Mutex<Vec<WireEvent>>>) -> Self {
        Self {
            buffered: BytesMut::new(),
            events,
        }
    }

    fn record(&mut self, bytes: &[u8]) {
        self.buffered.extend_from_slice(bytes);
        while self.buffered.len() >= 24 {
            let payload = usize::try_from(u32::from_le_bytes(
                self.buffered[16..20].try_into().unwrap(),
            ))
            .unwrap();
            assert!(
                payload <= 2_000_000,
                "fixture messages fit the legacy payload bound"
            );
            let bytes = 24 + payload;
            if self.buffered.len() < bytes {
                break;
            }
            let frame = self.buffered.split_to(bytes);
            let command = &frame[4..16];
            let mut body = &frame[24..];
            let (kind, hashes) = if command.starts_with(b"inv\0") {
                (
                    "inv",
                    block_hashes(Vec::<InventoryHash>::zcash_deserialize(&mut body).unwrap()),
                )
            } else if command.starts_with(b"getdata\0") {
                (
                    "getdata",
                    block_hashes(Vec::<InventoryHash>::zcash_deserialize(&mut body).unwrap()),
                )
            } else if command.starts_with(b"block\0") {
                let block = Block::zcash_deserialize_bounded(&mut body).unwrap();
                ("block", vec![block.hash()])
            } else {
                continue;
            };
            for hash in hashes {
                self.events
                    .lock()
                    .unwrap()
                    .push(WireEvent { kind, hash, bytes });
            }
        }
    }
}

fn block_hashes(items: Vec<InventoryHash>) -> Vec<zakura_chain::block::Hash> {
    items
        .into_iter()
        .filter_map(|item| match item {
            InventoryHash::Block(hash) => Some(hash),
            _ => None,
        })
        .collect()
}

struct ObservedSocket {
    socket: tokio::net::TcpStream,
    received: Capture,
    sent: Capture,
}

impl AsyncRead for ObservedSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.socket).poll_read(cx, buf);
        self.received.record(&buf.filled()[before..]);
        result
    }
}

impl AsyncWrite for ObservedSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.socket).poll_write(cx, buf);
        if let Poll::Ready(Ok(written)) = result {
            self.sent.record(&buf[..written]);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

/// A real legacy TCP connection to a node that records every inventory, request and block
/// message crossing it.
struct WirePeer {
    client: Peers,
    events: Arc<Mutex<Vec<WireEvent>>>,
    advertisements: mpsc::UnboundedReceiver<zakura_chain::block::Hash>,
}

impl WirePeer {
    async fn connect(relay: &Node) -> Self {
        let events = Arc::new(Mutex::new(Vec::new()));
        let socket = ObservedSocket {
            socket: tokio::net::TcpStream::connect(relay.p2p).await.unwrap(),
            received: Capture::new(events.clone()),
            sent: Capture::new(events.clone()),
        };
        let (advertised, advertisements) = mpsc::unbounded_channel();
        let client = zakura_network::connect_isolated_with_inbound(
            &relay.network,
            socket,
            "wire observer".into(),
            tower::service_fn(move |request| {
                if let Request::AdvertiseBlock(block, _) = request {
                    let _ = advertised.send(block.hash);
                }
                async { Ok::<_, BoxError>(Response::Nil) }
            }),
        )
        .await
        .unwrap();
        let client = BoxCloneSyncService::new(Buffer::new(client, 4));
        client
            .clone()
            .oneshot(Request::Ping(zakura_network::types::Nonce(123)))
            .await
            .unwrap();
        relay.connected().await;
        Self {
            client,
            events,
            advertisements,
        }
    }

    async fn announced(&mut self, hash: zakura_chain::block::Hash) {
        timeout(Duration::from_secs(20), async {
            while self.advertisements.recv().await.unwrap() != hash {}
        })
        .await
        .unwrap();
    }

    /// How many messages of `kind` ("inv", "getdata" or "block") for `hash` crossed this
    /// connection, in either direction.
    fn count(&self, hash: zakura_chain::block::Hash, kind: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.hash == hash && event.kind == kind)
            .count()
    }
}
