//! Gossiped children that arrive before their parents.
//!
//! The harness's verifier holds each block until its parent has committed, like the real
//! verifier's parent and UTXO waits, so a child whose parent never arrives waits forever.

use std::{
    collections::{HashMap, HashSet},
    future,
    sync::Arc,
    time::Duration,
};

use indexmap::IndexSet;
use tokio::sync::{mpsc, oneshot, watch};
use tower::{service_fn, util::BoxCloneService};
use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_network::InventoryResponse::Available;

use super::*;

type TestNetwork = BoxCloneService<zn::Request, zn::Response, BoxError>;
type TestVerifier = BoxCloneService<zakura_consensus::Request, block::Hash, BoxError>;
type TestState = BoxCloneService<zs::Request, zs::Response, BoxError>;

/// How long the harness waits for something that should happen promptly.
///
/// Time is paused in these tests, so this only bounds a hang.
const EXPECTED_WAIT: Duration = Duration::from_secs(5);

/// How long the harness waits to show that something does not happen.
///
/// Time is paused, so the runtime skips straight to this deadline once every task is idle.
const ABSENT_WAIT: Duration = Duration::from_secs(30);

fn block_from(bytes: &[u8]) -> Arc<Block> {
    bytes
        .zcash_deserialize_into()
        .expect("test vector deserializes")
}

fn genesis() -> Arc<Block> {
    block_from(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES)
}

fn block_1() -> Arc<Block> {
    block_from(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES)
}

fn block_2() -> Arc<Block> {
    block_from(&zakura_test::vectors::BLOCK_MAINNET_2_BYTES)
}

fn legacy(last_octet: u8) -> zn::PeerSource {
    zn::PeerSource::LegacySocket(([10, 0, 0, last_octet], 8233).into())
}

fn request_from(hash: block::Hash, source: zn::PeerSource) -> zn::Request {
    zn::Request::BlocksByHashFrom {
        hashes: IndexSet::from([hash]),
        source,
    }
}

fn requested_hash(request: &zn::Request) -> block::Hash {
    match request {
        zn::Request::BlocksByHashFrom { hashes, .. } | zn::Request::BlocksByHash(hashes) => *hashes
            .iter()
            .next()
            .expect("gossip downloads request exactly one hash"),
        request => panic!("unexpected network request: {request:?}"),
    }
}

/// A gossip download stream with controllable peers and a parent-waiting verifier.
struct Harness {
    downloads: Downloads<TestNetwork, TestVerifier, TestState>,

    /// Every block request the network received, in order.
    requests: mpsc::UnboundedReceiver<zn::Request>,

    /// Every block the verifier committed, in order.
    commits: mpsc::UnboundedReceiver<block::Hash>,

    /// Requests for these hashes are answered only after the paired sender fires.
    gates: Arc<std::sync::Mutex<HashMap<block::Hash, oneshot::Receiver<()>>>>,

    _tip_sender: zs::ChainTipSender,
}

impl Harness {
    /// Builds a harness whose tip is the genesis block.
    ///
    /// The network serves `blocks`. Requests for any other hash never complete, like a parent
    /// that no peer can deliver.
    fn new(blocks: impl IntoIterator<Item = Arc<Block>>) -> Self {
        let genesis = genesis();
        let genesis_hash = genesis.hash();

        let blocks: Arc<HashMap<block::Hash, Arc<Block>>> = Arc::new(
            blocks
                .into_iter()
                .map(|block| (block.hash(), block))
                .collect(),
        );
        let gates: Arc<std::sync::Mutex<HashMap<block::Hash, oneshot::Receiver<()>>>> =
            Arc::default();

        let (requests_tx, requests) = mpsc::unbounded_channel();
        let network_gates = gates.clone();
        let network = BoxCloneService::new(service_fn(move |request: zn::Request| {
            let blocks = blocks.clone();
            let gates = network_gates.clone();
            let requests_tx = requests_tx.clone();

            async move {
                let hash = requested_hash(&request);
                requests_tx.send(request)?;

                let gate = gates
                    .lock()
                    .expect("gate lock is not poisoned")
                    .remove(&hash);
                if let Some(gate) = gate {
                    let _ = gate.await;
                }

                match blocks.get(&hash) {
                    Some(block) => Ok(zn::Response::Blocks(vec![Available((block.clone(), None))])),
                    None => future::pending().await,
                }
            }
        }));

        let committed = Arc::new(watch::Sender::new(HashSet::from([genesis_hash])));

        let (commits_tx, commits) = mpsc::unbounded_channel();
        let verifier_committed = committed.clone();
        let verifier =
            BoxCloneService::new(service_fn(move |request: zakura_consensus::Request| {
                let committed = verifier_committed.clone();
                let commits_tx = commits_tx.clone();

                async move {
                    let zakura_consensus::Request::Commit(block) = request else {
                        return Err("unexpected verifier request".into());
                    };
                    let hash = block.hash();
                    let parent = block.header.previous_block_hash;

                    committed
                        .subscribe()
                        .wait_for(|committed| committed.contains(&parent))
                        .await?;
                    committed.send_modify(|committed| {
                        committed.insert(hash);
                    });
                    commits_tx.send(hash)?;

                    Ok(hash)
                }
            }));

        let state = BoxCloneService::new(service_fn(move |request: zs::Request| {
            let known = committed.clone();

            async move {
                match request {
                    // Gossip always fetches the body, so the state never reports it present.
                    zs::Request::AnyChainBlock(_) => Ok(zs::Response::Block(None)),
                    zs::Request::KnownBlock(hash) => Ok(zs::Response::KnownBlock(
                        known
                            .borrow()
                            .contains(&hash)
                            .then_some(zs::KnownBlock::BestChain),
                    )),
                    request => Err(format!("unexpected state request: {request:?}").into()),
                }
            }
        }));

        let mut tip_block: zs::ChainTipBlock = zs::SemanticallyVerifiedBlock::from(genesis).into();
        tip_block.height = block::Height(0);
        let (mut tip_sender, latest_chain_tip, _tip_change) =
            zs::ChainTipSender::new(None, &Network::Mainnet);
        tip_sender.set_finalized_tip(tip_block);

        let downloads = Downloads::new(
            MAX_INBOUND_CONCURRENCY,
            false,
            network,
            verifier,
            state,
            latest_chain_tip,
            Network::Mainnet,
        );

        Self {
            downloads,
            requests,
            commits,
            gates,
            _tip_sender: tip_sender,
        }
    }

    /// Holds the network's answer for `hash` until the returned sender fires or is dropped.
    fn hold(&self, hash: block::Hash) -> oneshot::Sender<()> {
        let (release, gate) = oneshot::channel();
        self.gates
            .lock()
            .expect("gate lock is not poisoned")
            .insert(hash, gate);
        release
    }

    async fn next_request(&mut self) -> zn::Request {
        tokio::time::timeout(EXPECTED_WAIT, self.requests.recv())
            .await
            .expect("a block request is sent promptly")
            .expect("the network request channel is open")
    }

    async fn assert_no_request(&mut self, context: &str) {
        if let Ok(request) = tokio::time::timeout(ABSENT_WAIT, self.requests.recv()).await {
            panic!("{context}: unexpected block request {request:?}");
        }
    }

    async fn next_commit(&mut self) -> block::Hash {
        tokio::time::timeout(EXPECTED_WAIT, self.commits.recv())
            .await
            .expect("a block commits promptly")
            .expect("the verifier commit channel is open")
    }

    async fn assert_no_commit(&mut self, context: &str) {
        if let Ok(hash) = tokio::time::timeout(ABSENT_WAIT, self.commits.recv()).await {
            panic!("{context}: unexpected commit {hash:?}");
        }
    }
}

/// Reproducer: a gossiped child arrives, and its parent was never requested.
///
/// Today nothing requests the parent. The child reaches the verifier and waits there, holding
/// its global queue slot and its advertiser's only legacy slot, so even an `inv` for the parent
/// from the child's advertiser is refused.
#[tokio::test(start_paused = true)]
async fn child_before_unrequested_parent_never_requests_the_parent() {
    let parent = block_1();
    let child = block_2();
    let mut harness = Harness::new([parent.clone(), child.clone()]);

    assert_eq!(
        harness
            .downloads
            .download_and_verify(child.hash(), Some(legacy(2))),
        DownloadAction::AddedToQueue
    );
    assert_eq!(
        harness.next_request().await,
        request_from(child.hash(), legacy(2))
    );

    harness
        .assert_no_request("nothing requests the missing parent")
        .await;
    harness
        .assert_no_commit("the child waits for a parent that never arrives")
        .await;

    assert_eq!(
        harness.downloads.queue_len(),
        1,
        "the waiting child keeps its inbound queue slot"
    );
    assert_eq!(
        harness
            .downloads
            .download_and_verify(parent.hash(), Some(legacy(2))),
        DownloadAction::FullQueue,
        "the waiting child holds its advertiser's only legacy slot, so that peer's parent inv is refused"
    );
}

/// Reproducer: a gossiped child arrives while its parent is downloading from a slow source.
///
/// The child waits for the existing parent download, and commits as soon as the parent does.
/// No second request is made, so the child's latency is the slow source's latency.
#[tokio::test(start_paused = true)]
async fn child_before_slow_parent_waits_for_the_parent_download() {
    let parent = block_1();
    let child = block_2();
    let mut harness = Harness::new([parent.clone(), child.clone()]);
    let release_parent = harness.hold(parent.hash());

    assert_eq!(
        harness
            .downloads
            .download_and_verify(parent.hash(), Some(legacy(1))),
        DownloadAction::AddedToQueue
    );
    assert_eq!(
        harness
            .downloads
            .download_and_verify(child.hash(), Some(legacy(2))),
        DownloadAction::AddedToQueue
    );

    let requests = [harness.next_request().await, harness.next_request().await];
    for expected in [
        request_from(parent.hash(), legacy(1)),
        request_from(child.hash(), legacy(2)),
    ] {
        assert!(
            requests.contains(&expected),
            "{expected:?} was requested: {requests:?}"
        );
    }

    harness
        .assert_no_request("the existing parent download is the only parent request")
        .await;
    harness
        .assert_no_commit("the child waits for the slow parent")
        .await;

    release_parent
        .send(())
        .expect("the parent request is waiting for release");

    assert_eq!(harness.next_commit().await, parent.hash());
    assert_eq!(harness.next_commit().await, child.hash());
}
