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
async fn changed_sessions_resubscribe_before_waiting_for_blocks() {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(Duration::from_secs(10), async {
        for next_session in [None, Some("new-primary")] {
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
                receipt_session: Some("old-primary".into()),
            };
            let sync = tokio::spawn(async move { syncer.sync().await });

            let (request, response) = received.recv().await.unwrap();
            assert_eq!(
                request.chain_tip_hashes,
                vec![block.hash().bytes_in_display_order().to_vec()]
            );
            assert_eq!(request.receipt_session.as_deref(), Some("old-primary"));
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
            blocks
                .send(Ok(BlockAndHash::new(block.hash(), block.clone())))
                .await
                .unwrap();
            state_receiver.changed().await.unwrap();
            while tip.best_tip_hash() != Some(block.hash()) {
                tip_change.wait_for_tip_change().await.unwrap();
            }
            assert_eq!(tip.best_tip_hash(), Some(block.hash()));
            assert_eq!(state_receiver.borrow().best_tip().unwrap().1, block.hash());
            assert!(!sync.is_finished());
            sync.abort();
            server.abort();
        }
    })
    .await
    .expect("session changes must recover without waiting for a new block");
}
