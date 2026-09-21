//! Subscription recovery against a gRPC server that can omit session metadata.

use std::{
    convert::Infallible,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{body::Body, codegen::http, transport::server::TcpIncoming, Response};
use tower::Service;
use zakura_chain::{chain_tip::ChainTip, serialization::ZcashDeserializeInto};

use super::*;

type BlockStream = ReceiverStream<Result<BlockAndHash, Status>>;
type Subscription = (
    NonFinalizedStateChangeRequest,
    oneshot::Sender<Response<BlockStream>>,
);

/// Exposes only the subscription RPC, allowing the test to emulate older servers.
#[derive(Clone)]
struct SubscriptionServer(mpsc::Sender<Subscription>);

impl tonic::server::NamedService for SubscriptionServer {
    const NAME: &'static str = "zebra.indexer.rpc.Indexer";
}

impl Service<http::Request<Body>> for SubscriptionServer {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::default();
            Ok(tonic::server::Grpc::new(codec)
                .server_streaming(service, request)
                .await)
        })
    }
}

impl tonic::server::ServerStreamingService<NonFinalizedStateChangeRequest> for SubscriptionServer {
    type Response = BlockAndHash;
    type ResponseStream = BlockStream;
    type Future = BoxFuture<'static, Result<Response<BlockStream>, Status>>;

    fn call(&mut self, request: tonic::Request<NonFinalizedStateChangeRequest>) -> Self::Future {
        let requests = self.0.clone();
        Box::pin(async move {
            let (sender, receiver) = oneshot::channel();
            requests
                .send((request.into_inner(), sender))
                .await
                .map_err(|_| Status::cancelled("test request receiver closed"))?;
            receiver
                .await
                .map_err(|_| Status::cancelled("test response sender closed"))
        })
    }
}

fn chain_fixture() -> (Network, Arc<Block>, Arc<Block>) {
    use zakura_chain::{
        parameters::{
            testnet::{ConfiguredActivationHeights, ConfiguredCheckpoints, Parameters},
            NetworkUpgrade,
        },
        transaction::{LockTime, Transaction},
    };

    let genesis: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let network = Parameters::build()
        .with_genesis_hash(genesis.hash())
        .unwrap()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![(
            Height(0),
            genesis.hash(),
        )]))
        .unwrap()
        .with_activation_heights(ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        })
        .unwrap()
        .clear_funding_streams()
        .with_slow_start_interval(Height::MIN)
        .with_disable_pow(true)
        .with_target_difficulty_limit(genesis.header.difficulty_threshold.to_expanded().unwrap())
        .unwrap()
        .to_network()
        .unwrap();
    let mut block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    block.transactions = vec![Arc::new(Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1),
        inputs: block.transactions[0].inputs().to_vec(),
        outputs: block.transactions[0].outputs().to_vec(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    })];
    let commitment = block::ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &block::CHAIN_HISTORY_ACTIVATION_RESERVED.into(),
        &block.auth_data_root(),
    );
    let header = Arc::make_mut(&mut block.header);
    header.merkle_root = block.transactions.iter().collect();
    header.commitment_bytes = <[u8; 32]>::from(commitment).into();
    (network, genesis, Arc::new(block))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unidentified_or_changed_sessions_resubscribe_before_waiting_for_blocks() {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(Duration::from_secs(10), async {
        for (previous_session, next_session) in [
            (Some("old-primary"), None),
            (Some("old-primary"), Some("new-primary")),
            (None, None),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let (requests, mut received) = mpsc::channel(1);
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(SubscriptionServer(requests))
                    .serve_with_incoming(TcpIncoming::from(listener))
                    .await
                    .unwrap();
            });
            let (network, genesis, block) = chain_fixture();
            let mut finalized =
                zakura_state::FinalizedState::new(&zakura_state::Config::ephemeral(), &network)
                    .unwrap();
            finalized
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(genesis).into(),
                    None,
                    None,
                    "subscription test",
                )
                .unwrap();
            let mut state = NonFinalizedState::new(&network);
            let mut prepared = SemanticallyVerifiedBlock::from(block.clone());
            prepared.receipt_order = Some(1);
            state.commit_new_chain(prepared, &finalized.db).unwrap();
            let (tip_sender, tip, mut tip_change) = ChainTipSender::new(None, &network);
            let (state_sender, mut state_receiver) = watch::channel(state.clone());
            let (started, _) = watch::channel(true);
            let mut syncer = TrustedChainSync {
                indexer_rpc_client: IndexerClient::connect(endpoint).await.unwrap(),
                db: finalized.db.clone(),
                non_finalized_state: state,
                chain_tip_sender: tip_sender,
                non_finalized_state_sender: state_sender,
                started_sync_sender: started,
                finalized_tip_updater: None,
                receipt_session: previous_session.map(str::to_owned),
            };
            let sync = tokio::spawn(async move { syncer.sync().await });

            let (request, response) = received.recv().await.unwrap();
            assert_eq!(
                request.chain_tip_hashes,
                vec![block.hash().bytes_in_display_order().to_vec()]
            );
            assert_eq!(request.receipt_session.as_deref(), previous_session);
            // Keep this stream open and idle. A legacy server skipped the known
            // chain, so waiting for a block here would stall until the timeout.
            let (_idle_sender, idle_receiver) = mpsc::channel(1);
            let mut reply = Response::new(ReceiverStream::new(idle_receiver));
            if let Some(session) = next_session {
                reply.metadata_mut().insert(
                    crate::indexer::RECEIPT_SESSION_HEADER,
                    session.parse().unwrap(),
                );
            }
            response.send(reply).unwrap();

            let (request, response) = received.recv().await.unwrap();
            assert!(request.chain_tip_hashes.is_empty());
            assert_eq!(request.receipt_session.as_deref(), next_session);
            let (blocks, receiver) = mpsc::channel(1);
            let mut reply = Response::new(ReceiverStream::new(receiver));
            if let Some(session) = next_session {
                reply.metadata_mut().insert(
                    crate::indexer::RECEIPT_SESSION_HEADER,
                    session.parse().unwrap(),
                );
            }
            response.send(reply).unwrap();
            let mut replacement = block.as_ref().clone();
            Arc::make_mut(&mut replacement.header).nonce.0[0] ^= 1;
            let replacement = Arc::new(replacement);
            blocks
                .send(Ok(BlockAndHash::new(
                    replacement.hash(),
                    replacement.clone(),
                )))
                .await
                .unwrap();
            if next_session.is_some() {
                blocks
                    .send(Ok(BlockAndHash {
                        chain_snapshot: Some(crate::indexer::NonFinalizedChainTips {
                            hashes: vec![replacement.hash().bytes_in_display_order().to_vec()],
                        }),
                        ..Default::default()
                    }))
                    .await
                    .unwrap();
            }
            state_receiver
                .wait_for(|state| {
                    state
                        .best_tip()
                        .is_some_and(|(_, hash)| hash == replacement.hash())
                })
                .await
                .unwrap();
            while tip.best_tip_hash() != Some(replacement.hash()) {
                tip_change.wait_for_tip_change().await.unwrap();
            }
            assert_eq!(tip.best_tip_hash(), Some(replacement.hash()));
            assert_eq!(
                state_receiver.borrow().best_tip().unwrap().1,
                replacement.hash()
            );
            assert_eq!(state_receiver.borrow().chain_iter().count(), 1);
            assert!(!state_receiver.borrow().any_chain_contains(&block.hash()));
            assert!(!sync.is_finished());
            sync.abort();
            server.abort();
        }
    })
    .await
    .expect("session changes must recover without waiting for a new block");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_legacy_session_clears_published_fork_and_tracks_finalized_tip() {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(Duration::from_secs(15), async {
        for previous_session in [Some("previous-primary"), None] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let (requests, mut received) = mpsc::channel(1);
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(SubscriptionServer(requests))
                    .serve_with_incoming(TcpIncoming::from(listener))
                    .await
                    .unwrap();
            });
            let (network, genesis, block) = chain_fixture();
            let mut finalized =
                zakura_state::FinalizedState::new(&zakura_state::Config::ephemeral(), &network)
                    .unwrap();
            finalized
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(genesis.clone()).into(),
                    None,
                    None,
                    "legacy reset test",
                )
                .unwrap();
            let mut state = NonFinalizedState::new(&network);
            state
                .commit_new_chain(block.clone().into(), &finalized.db)
                .unwrap();
            let (tip_sender, tip, mut tip_change) = ChainTipSender::new(None, &network);
            let (state_sender, mut state_receiver) = watch::channel(state.clone());
            let (started, _) = watch::channel(true);
            let mut syncer = TrustedChainSync {
                indexer_rpc_client: IndexerClient::connect(endpoint).await.unwrap(),
                db: finalized.db.clone(),
                non_finalized_state: state,
                chain_tip_sender: tip_sender,
                non_finalized_state_sender: state_sender,
                started_sync_sender: started,
                finalized_tip_updater: None,
                receipt_session: previous_session.map(str::to_owned),
            };
            syncer.update_channels();
            assert_eq!(tip.best_tip_hash(), Some(block.hash()));
            let sync = tokio::spawn(async move { syncer.sync().await });
            let (_, response) = received.recv().await.unwrap();
            let (_first_sender, first_stream) = mpsc::channel(1);
            response
                .send(Response::new(ReceiverStream::new(first_stream)))
                .unwrap();
            let (request, response) = received.recv().await.unwrap();
            assert!(request.chain_tip_hashes.is_empty());
            let (_idle_sender, idle_stream) = mpsc::channel(1);
            response
                .send(Response::new(ReceiverStream::new(idle_stream)))
                .unwrap();
            state_receiver
                .wait_for(|state| state.is_chain_set_empty())
                .await
                .unwrap();
            while tip.best_tip_hash() != Some(genesis.hash()) {
                tip_change.wait_for_tip_change().await.unwrap();
            }

            // The legacy stream remains empty as the shared finalized database advances.
            let mut next = block.as_ref().clone();
            Arc::make_mut(&mut next.header).nonce.0[0] ^= 1;
            let next = Arc::new(next);
            finalized
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(next.clone()).into(),
                    None,
                    None,
                    "legacy reset test",
                )
                .unwrap();
            while tip.best_tip_hash() != Some(next.hash()) {
                tip_change.wait_for_tip_change().await.unwrap();
            }
            assert!(state_receiver.borrow().is_chain_set_empty());
            assert!(!sync.is_finished());
            sync.abort();
            server.abort();
        }
    })
    .await
    .expect("an empty legacy primary must replace the published fork without a block message");
}

fn snapshot_response(
    receiver: mpsc::Receiver<Result<BlockAndHash, Status>>,
) -> Response<BlockStream> {
    let mut response = Response::new(ReceiverStream::new(receiver));
    response.metadata_mut().insert(
        crate::indexer::RECEIPT_SESSION_HEADER,
        "primary".parse().unwrap(),
    );
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_snapshots_preserve_published_state_until_a_complete_boundary() {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(Duration::from_secs(15), async {
        for change_session in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let (requests, mut received) = mpsc::channel(1);
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(SubscriptionServer(requests))
                    .serve_with_incoming(TcpIncoming::from(listener))
                    .await
                    .unwrap();
            });
            let (network, genesis, a) = chain_fixture();
            let mut b = a.as_ref().clone();
            Arc::make_mut(&mut b.header).nonce.0[0] ^= 1;
            let b = Arc::new(b);
            let mut finalized =
                zakura_state::FinalizedState::new(&zakura_state::Config::ephemeral(), &network)
                    .unwrap();
            finalized
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(genesis.clone()).into(),
                    None,
                    None,
                    "snapshot test",
                )
                .unwrap();
            let mut state = NonFinalizedState::new(&network);
            let mut prepared = SemanticallyVerifiedBlock::from(a.clone());
            prepared.receipt_order = Some(2);
            state.commit_new_chain(prepared, &finalized.db).unwrap();
            let (tip_sender, tip, mut tip_change) = ChainTipSender::new(None, &network);
            let (state_sender, mut state_receiver) = watch::channel(state.clone());
            let (started, _) = watch::channel(true);
            let mut syncer = TrustedChainSync {
                indexer_rpc_client: IndexerClient::connect(endpoint).await.unwrap(),
                db: finalized.db.clone(),
                non_finalized_state: state,
                chain_tip_sender: tip_sender,
                non_finalized_state_sender: state_sender,
                started_sync_sender: started,
                finalized_tip_updater: None,
                receipt_session: Some(
                    if change_session {
                        "old-primary"
                    } else {
                        "primary"
                    }
                    .into(),
                ),
            };
            syncer.update_channels();
            state_receiver.borrow_and_update();
            let sync = tokio::spawn(async move { syncer.sync().await });
            let (_, mut response) = received.recv().await.unwrap();
            let (_reset_sender, reset_stream) = mpsc::channel(1);
            if change_session {
                response.send(snapshot_response(reset_stream)).unwrap();
                let (request, next_response) = received.recv().await.unwrap();
                assert!(request.chain_tip_hashes.is_empty());
                response = next_response;
            }
            let expected_tips = if change_session {
                vec![]
            } else {
                vec![a.hash().bytes_in_display_order().to_vec()]
            };
            let expected_tip = if change_session {
                genesis.hash()
            } else {
                a.hash()
            };
            let (blocks, stream) = mpsc::channel(2);
            response.send(snapshot_response(stream)).unwrap();
            let mut message = BlockAndHash::new(b.hash(), b.clone());
            message.receipt_order = Some(1);
            blocks.send(Ok(message.clone())).await.unwrap();
            blocks
                .send(Err(Status::unavailable("interrupted snapshot")))
                .await
                .unwrap();

            // Receiving the next request proves the client consumed the preceding
            // block and stream error. None of that unfinished batch may be visible.
            let (request, response) = received.recv().await.unwrap();
            assert_eq!(request.chain_tip_hashes, expected_tips);
            assert_eq!(tip.best_tip_hash(), Some(expected_tip));
            assert_eq!(
                state_receiver.borrow().best_tip().map(|(_, hash)| hash),
                (!change_session).then_some(a.hash())
            );
            state_receiver.borrow_and_update();
            let (blocks, stream) = mpsc::channel(2);
            response.send(snapshot_response(stream)).unwrap();
            blocks.send(Ok(message)).await.unwrap();
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    state_receiver.wait_for(|state| state
                        .best_tip()
                        .is_some_and(|(_, hash)| hash == b.hash()))
                )
                .await
                .is_err(),
                "replayed blocks must wait for the snapshot boundary"
            );
            let snapshot = |hashes: Vec<block::Hash>| BlockAndHash {
                chain_snapshot: Some(crate::indexer::NonFinalizedChainTips {
                    hashes: hashes
                        .into_iter()
                        .map(|hash| hash.bytes_in_display_order().to_vec())
                        .collect(),
                }),
                ..Default::default()
            };
            blocks.send(Ok(snapshot(vec![b.hash()]))).await.unwrap();
            state_receiver
                .wait_for(|state| state.best_tip().is_some_and(|(_, hash)| hash == b.hash()))
                .await
                .unwrap();
            while tip.best_tip_hash() != Some(b.hash()) {
                tip_change.wait_for_tip_change().await.unwrap();
            }
            assert_eq!(state_receiver.borrow().chain_iter().count(), 1);
            blocks.send(Ok(snapshot(vec![]))).await.unwrap();
            state_receiver
                .wait_for(|state| state.is_chain_set_empty())
                .await
                .unwrap();
            while tip.best_tip_hash() != Some(genesis.hash()) {
                tip_change.wait_for_tip_change().await.unwrap();
            }
            sync.abort();
            server.abort();
        }
    })
    .await
    .expect("snapshot recovery must finish");
}
