//! A gossiped block that this node has already committed is not downloaded again.
//!
//! The downloader runs against a real state, so each test sees the answers a node with that
//! storage mode gives. The network service records every request it receives, and answers a
//! block request with the block the test supplies.
//!
//! A header without a body cannot be committed from outside the state crate, so that case uses
//! a state stand-in. The state's own tests check that it gives the stand-in's answer.

use std::time::Duration;

use futures::StreamExt as _;
use tokio::sync::mpsc;
use tower::{buffer::Buffer, service_fn, util::BoxCloneService};

use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_state::{Config as StateConfig, PruningConfig, StorageMode};
use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_GENESIS_BYTES};

use super::*;

type TestNetwork = BoxCloneService<zn::Request, zn::Response, BoxError>;
type TestVerifier = BoxCloneService<zakura_consensus::Request, block::Hash, BoxError>;
type TestState = BoxCloneService<zs::Request, zs::Response, BoxError>;

const DEADLINE: Duration = Duration::from_secs(10);

fn block(bytes: &[u8]) -> Arc<Block> {
    bytes.zcash_deserialize_into().expect("valid block vector")
}

fn announcer() -> zn::PeerSource {
    zn::PeerSource::LegacySocket(
        "203.0.113.7:8233"
            .parse()
            .expect("valid test socket address"),
    )
}

fn pruned_storage() -> StateConfig {
    StateConfig {
        storage_mode: StorageMode::Pruned(PruningConfig::default()),
        ..StateConfig::ephemeral()
    }
}

/// A downloader over a real state, with the requests and commits that leave it.
struct Node {
    downloads: Pin<Box<Downloads<TestNetwork, TestVerifier, TestState>>>,
    state: TestState,
    network_requests: mpsc::UnboundedReceiver<zn::Request>,
    commits: mpsc::UnboundedReceiver<block::Hash>,
}

impl Node {
    /// Starts a node whose finalized chain is `finalized`. Its peers answer any block request
    /// with `served`, and its verifier accepts every block.
    async fn new(config: StateConfig, finalized: &[Arc<Block>], served: Arc<Block>) -> Self {
        let zs::InitializedState {
            state,
            latest_chain_tip,
            ..
        } = zs::init(config, &Network::Mainnet, block::Height::MAX, 0)
            .await
            .expect("ephemeral state initialization succeeds");
        let state: TestState = BoxCloneService::new(Buffer::new(state, 10));
        for block in finalized {
            tokio::time::timeout(
                DEADLINE,
                state
                    .clone()
                    .oneshot(zs::Request::CommitCheckpointVerifiedBlock(
                        block.clone().into(),
                    )),
            )
            .await
            .expect("the block commits in time")
            .expect("the block extends the finalized chain");
        }

        Self::over(state, latest_chain_tip, served)
    }

    /// Starts a node over `state`, whose chain tip is `latest_chain_tip`.
    fn over(state: TestState, latest_chain_tip: zs::LatestChainTip, served: Arc<Block>) -> Self {
        let (requests, network_requests) = mpsc::unbounded_channel();
        let network = BoxCloneService::new(service_fn(move |request: zn::Request| {
            let served = served.clone();
            let sent = requests.send(request);
            async move {
                sent.map_err(|_| "the test dropped the network request receiver")?;
                Ok(zn::Response::Blocks(vec![
                    zn::InventoryResponse::Available((served.into(), None)),
                ]))
            }
        }));
        let (committed, commits) = mpsc::unbounded_channel();
        let verifier =
            BoxCloneService::new(service_fn(move |request: zakura_consensus::Request| {
                let committed = committed.clone();
                async move {
                    let zakura_consensus::Request::Commit(block) = request else {
                        return Err("unexpected verifier request".into());
                    };
                    let hash = block.hash();
                    committed
                        .send(hash)
                        .map_err(|_| "the test dropped the commit receiver")?;
                    Ok(hash)
                }
            }));

        let downloads = Box::pin(Downloads::new(
            crate::components::test_misbehavior::reporter(),
            MAX_INBOUND_CONCURRENCY,
            false,
            network,
            verifier,
            state.clone(),
            latest_chain_tip,
            Network::Mainnet,
        ));

        Self {
            downloads,
            state,
            network_requests,
            commits,
        }
    }

    /// Returns true if the state can still serve the body of `hash`.
    async fn stores_body(&self, hash: block::Hash) -> bool {
        let response = self
            .state
            .clone()
            .oneshot(zs::Request::Block(hash.into()))
            .await
            .expect("the state answers a block read");
        let zs::Response::Block(body) = response else {
            panic!("unexpected block read response: {response:?}");
        };
        body.is_some()
    }

    /// Announces `hash`, and returns how the download ended.
    async fn announce(&mut self, hash: block::Hash) -> Result<block::Hash, String> {
        assert_eq!(
            self.downloads
                .as_mut()
                .download_and_verify(hash, Some(announcer())),
            DownloadAction::AddedToQueue
        );
        self.next_outcome().await
    }

    /// Receives `block` unrequested from a peer, and returns how its verification ended.
    async fn receive_pushed(&mut self, block: Arc<Block>) -> Result<block::Hash, String> {
        assert_eq!(
            self.downloads
                .as_mut()
                .verify_pushed(block.into(), Some(announcer())),
            DownloadAction::AddedToQueue
        );
        self.next_outcome().await
    }

    async fn next_outcome(&mut self) -> Result<block::Hash, String> {
        tokio::time::timeout(DEADLINE, self.downloads.next())
            .await
            .expect("the block is handled in time")
            .expect("the downloads stream is open")
            .map_err(|(error, _)| error.to_string())
    }

    /// Asserts that the block was dropped as already held, before any request to a peer.
    fn assert_skipped_without_a_download(&mut self, outcome: Result<block::Hash, String>) {
        assert_eq!(
            outcome,
            Err("already present".to_string()),
            "a block the node has already committed is dropped as already present"
        );
        assert!(
            self.network_requests.try_recv().is_err(),
            "no request may reach a peer for a block the node has already committed"
        );
        assert!(
            self.commits.try_recv().is_err(),
            "a block the node has already committed is not verified again"
        );
    }

    /// Asserts that the announced block `hash` was requested from a peer once and verified.
    fn assert_downloaded_once(&mut self, hash: block::Hash, outcome: Result<block::Hash, String>) {
        assert_eq!(
            outcome,
            Ok(hash),
            "a block the node is missing is downloaded and verified"
        );
        let request = self
            .network_requests
            .try_recv()
            .expect("the missing block is requested from a peer");
        assert_eq!(
            request,
            zn::Request::block_by_hash(hash, Some(announcer())),
            "the request asks the announcer for the announced block"
        );
        assert!(
            self.network_requests.try_recv().is_err(),
            "the missing block is requested only once"
        );
        assert_eq!(
            self.commits.try_recv(),
            Ok(hash),
            "the downloaded block goes to the verifier"
        );
    }
}

/// The reported bug: a pruned node deleted the body of an old finalized block, and a peer that
/// is still syncing announces it.
#[tokio::test(flavor = "multi_thread")]
async fn pruned_node_does_not_download_a_finalized_block_whose_body_it_deleted() {
    let _init_guard = zakura_test::init();
    let genesis = block(&BLOCK_MAINNET_GENESIS_BYTES);
    let block1 = block(&BLOCK_MAINNET_1_BYTES);
    let mut node = Node::new(pruned_storage(), &[genesis, block1.clone()], block1.clone()).await;
    assert!(
        !node.stores_body(block1.hash()).await,
        "the pruned node no longer stores the body of the finalized block"
    );

    let outcome = node.announce(block1.hash()).await;

    node.assert_skipped_without_a_download(outcome);
}

/// A pushed block shares the check, so it is not verified again either.
#[tokio::test(flavor = "multi_thread")]
async fn pruned_node_drops_a_pushed_finalized_block_whose_body_it_deleted() {
    let _init_guard = zakura_test::init();
    let genesis = block(&BLOCK_MAINNET_GENESIS_BYTES);
    let block1 = block(&BLOCK_MAINNET_1_BYTES);
    let mut node = Node::new(pruned_storage(), &[genesis, block1.clone()], block1.clone()).await;

    let outcome = node.receive_pushed(block1).await;

    node.assert_skipped_without_a_download(outcome);
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_node_does_not_download_a_finalized_block_it_still_stores() {
    let _init_guard = zakura_test::init();
    let genesis = block(&BLOCK_MAINNET_GENESIS_BYTES);
    let block1 = block(&BLOCK_MAINNET_1_BYTES);
    let mut node = Node::new(
        StateConfig::ephemeral(),
        &[genesis, block1.clone()],
        block1.clone(),
    )
    .await;
    assert!(
        node.stores_body(block1.hash()).await,
        "the archive node stores the body of the finalized block"
    );

    let outcome = node.announce(block1.hash()).await;

    node.assert_skipped_without_a_download(outcome);
}

/// Skipping committed blocks must not stop a pruned node from fetching the next block.
#[tokio::test(flavor = "multi_thread")]
async fn pruned_node_downloads_a_block_it_has_never_seen() {
    let _init_guard = zakura_test::init();
    let genesis = block(&BLOCK_MAINNET_GENESIS_BYTES);
    let block1 = block(&BLOCK_MAINNET_1_BYTES);
    let mut node = Node::new(pruned_storage(), &[genesis], block1.clone()).await;

    let outcome = node.announce(block1.hash()).await;

    node.assert_downloaded_once(block1.hash(), outcome);
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_node_downloads_a_block_it_has_never_seen() {
    let _init_guard = zakura_test::init();
    let genesis = block(&BLOCK_MAINNET_GENESIS_BYTES);
    let block1 = block(&BLOCK_MAINNET_1_BYTES);
    let mut node = Node::new(StateConfig::ephemeral(), &[genesis], block1.clone()).await;

    let outcome = node.announce(block1.hash()).await;

    node.assert_downloaded_once(block1.hash(), outcome);
}

/// Header sync can hold a block's header before its body, and a commit that is still pending
/// can fail. The state reports neither as a committed block, so the body is still fetched.
#[tokio::test]
async fn node_downloads_a_block_it_knows_only_by_header_or_has_not_finished_committing() {
    let _init_guard = zakura_test::init();
    let block1 = block(&BLOCK_MAINNET_1_BYTES);
    let hash = block1.hash();

    for (case, answer) in [
        ("known only by header", None),
        (
            "sent to the state but not committed",
            Some(zs::KnownBlock::WriteChannel),
        ),
        (
            "queued in the state but not committed",
            Some(zs::KnownBlock::Queue),
        ),
    ] {
        let state = BoxCloneService::new(service_fn(move |request: zs::Request| {
            let answer = answer.clone();
            async move {
                match request {
                    zs::Request::KnownBlock(asked) if asked == hash => {
                        Ok(zs::Response::KnownBlock(answer))
                    }
                    request => Err(format!("unexpected state request: {request:?}").into()),
                }
            }
        }));
        let (_tip_sender, latest_chain_tip, _tip_change) =
            zs::ChainTipSender::new(None, &Network::Mainnet);
        let mut node = Node::over(state, latest_chain_tip, block1.clone());

        let outcome = node.announce(hash).await;

        assert_eq!(
            outcome,
            Ok(hash),
            "a block {case} is downloaded and verified"
        );
        node.assert_downloaded_once(hash, outcome);
    }
}
