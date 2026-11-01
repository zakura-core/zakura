//! Gossiped children that arrive before their parents.
//!
//! The harness's verifier holds each block until its parent has committed, like the real
//! verifier's parent and UTXO waits, so a child whose parent never arrives waits forever.
//!
//! Harnesses run on Regtest, which waives proof of work, because most tests rewrite block headers.

use std::{
    collections::{HashMap, HashSet},
    future,
    sync::Arc,
    time::Duration,
};

use futures::StreamExt as _;
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

fn block_3() -> Arc<Block> {
    block_from(&zakura_test::vectors::BLOCK_MAINNET_3_BYTES)
}

/// Returns `block` with its parent replaced by `parent`, keeping its coinbase height.
///
/// The block no longer has valid proof of work, so it needs a network that waives it.
fn with_parent(block: &Block, parent: block::Hash) -> Arc<Block> {
    let mut header = *block.header;
    header.previous_block_hash = parent;

    Arc::new(Block {
        header: Arc::new(header),
        transactions: block.transactions.clone(),
    })
}

/// Returns `block` with a different nonce, so it is a distinct sibling with the same parent.
fn sibling_of(block: &Block, nonce_byte: u8) -> Arc<Block> {
    let mut header = *block.header;
    header.nonce = [nonce_byte; 32].into();

    Arc::new(Block {
        header: Arc::new(header),
        transactions: block.transactions.clone(),
    })
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

    /// The next request for each of these hashes fails.
    failures: Arc<std::sync::Mutex<HashSet<block::Hash>>>,

    _tip_sender: zs::ChainTipSender,
}

impl Harness {
    /// Builds a harness whose tip is the genesis block.
    ///
    /// The network serves `blocks`. Requests for any other hash never complete, like a parent
    /// that no peer can deliver.
    fn new(blocks: impl IntoIterator<Item = Arc<Block>>) -> Self {
        Self::on_network(Network::new_regtest(Default::default()), blocks)
    }

    /// [`Self::new`], on `network`.
    fn on_network(chain_network: Network, blocks: impl IntoIterator<Item = Arc<Block>>) -> Self {
        Self::with_capacity(chain_network, blocks, MAX_INBOUND_CONCURRENCY)
    }

    fn with_capacity(
        chain_network: Network,
        blocks: impl IntoIterator<Item = Arc<Block>>,
        capacity: usize,
    ) -> Self {
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
        let failures: Arc<std::sync::Mutex<HashSet<block::Hash>>> = Arc::default();

        let (requests_tx, requests) = mpsc::unbounded_channel();
        let network_gates = gates.clone();
        let network_failures = failures.clone();
        let network = BoxCloneService::new(service_fn(move |request: zn::Request| {
            let blocks = blocks.clone();
            let gates = network_gates.clone();
            let failures = network_failures.clone();
            let requests_tx = requests_tx.clone();

            async move {
                let hash = requested_hash(&request);
                requests_tx.send(request)?;

                if failures
                    .lock()
                    .expect("failure lock is not poisoned")
                    .remove(&hash)
                {
                    return Err("the peer failed the request".into());
                }

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
                    zs::Request::CheckParentInputs { parent, .. } => Ok(
                        zs::Response::ParentInputs(if known.borrow().contains(&parent) {
                            zs::ParentInputs::Available
                        } else {
                            zs::ParentInputs::ParentUnavailable
                        }),
                    ),
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
            zs::ChainTipSender::new(None, &chain_network);
        tip_sender.set_finalized_tip(tip_block);

        let downloads = Downloads::new(
            capacity,
            false,
            network,
            verifier,
            state,
            latest_chain_tip,
            chain_network,
            block::Height(0),
        );

        Self {
            downloads,
            requests,
            commits,
            gates,
            failures,
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

fn unavailable(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

/// Saturated parent waits release an excess child's slot, then recover it from
/// parent progress without another advertisement while unrelated waits remain.
#[tokio::test(start_paused = true)]
async fn deferred_child_releases_its_slot_and_commits_after_its_parent_arrives() {
    let parent = block_2();
    let child = block_3();
    let orphans: Vec<_> = (0..3)
        .map(|index| with_parent(&block_3(), unavailable(100 + index)))
        .collect();
    let mut blocks = orphans.clone();
    blocks.extend([block_1(), parent.clone(), child.clone()]);
    let mut harness = Harness::with_capacity(Network::new_regtest(Default::default()), blocks, 4);
    for (index, orphan) in orphans.iter().enumerate() {
        harness.downloads.download_and_verify(
            orphan.hash(),
            Some(legacy(u8::try_from(index + 10).unwrap())),
        );
        assert_eq!(requested_hash(&harness.next_request().await), orphan.hash());
    }
    tokio::time::sleep(Duration::from_millis(1)).await;
    harness
        .downloads
        .download_and_verify(child.hash(), Some(legacy(20)));
    assert_eq!(requested_hash(&harness.next_request().await), child.hash());
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(harness.downloads.spawner.admission().queue_len(), 3);
    assert!(harness
        .downloads
        .spawner
        .parent_budget
        .is_deferred(&child.hash()));
    for (block, source) in [(block_1(), legacy(21)), (parent.clone(), legacy(22))] {
        assert_eq!(
            harness
                .downloads
                .download_and_verify(block.hash(), Some(source)),
            DownloadAction::AddedToQueue
        );
        assert_eq!(requested_hash(&harness.next_request().await), block.hash());
        assert_eq!(harness.next_commit().await, block.hash());
    }
    assert_eq!(requested_hash(&harness.next_request().await), child.hash());
    assert_eq!(harness.next_commit().await, child.hash());
    assert_eq!(harness.downloads.spawner.admission().queue_len(), 3);
}

impl Harness {
    /// Returns every block request sent until the downloads go idle.
    async fn drain_requests(&mut self) -> Vec<zn::Request> {
        let mut requests = Vec::new();
        while let Ok(Some(request)) = tokio::time::timeout(ABSENT_WAIT, self.requests.recv()).await
        {
            requests.push(request);
        }
        requests
    }

    fn parent_recoveries(&self) -> usize {
        self.downloads.spawner.admission().parent_recoveries.len()
    }
}

/// A gossiped child arrives, and its parent was never requested.
///
/// The child's advertiser has the parent, so the parent is requested from it as soon as the
/// downloaded child shows the parent is missing. The recovery is not charged to that
/// advertiser's legacy slot, which the waiting child holds. Both blocks then commit in order,
/// through the normal verifier path, and the recovery's bookkeeping is released.
#[tokio::test(start_paused = true)]
async fn child_before_unrequested_parent_requests_the_parent_from_the_childs_advertiser() {
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
    assert_eq!(
        harness.next_request().await,
        request_from(parent.hash(), legacy(2)),
        "the missing parent is requested from the child's advertiser"
    );

    assert_eq!(harness.next_commit().await, parent.hash());
    assert_eq!(harness.next_commit().await, child.hash());
    harness
        .assert_no_request("one recovery request per missing parent")
        .await;

    let mut results = HashSet::new();
    for _ in 0..2 {
        let hash = tokio::time::timeout(EXPECTED_WAIT, harness.downloads.next())
            .await
            .expect("a finished download is yielded")
            .expect("the downloads stream is open")
            .expect("the download succeeds");
        results.insert(hash);
    }
    assert_eq!(results, HashSet::from([parent.hash(), child.hash()]));
    assert_eq!(harness.downloads.spawner.admission().queue_len(), 0);
    assert_eq!(harness.parent_recoveries(), 0);
}

/// A gossiped child arrives while its parent is downloading from a slow source.
///
/// The child attaches to the existing parent download instead of starting a second one, and
/// commits as soon as the parent does. Hedging a slow source is a separate, measured policy.
#[tokio::test(start_paused = true)]
async fn child_before_slow_parent_attaches_to_the_parent_download() {
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
        .assert_no_request("the child attaches to the existing parent download")
        .await;
    harness
        .assert_no_commit("the child waits for the slow parent")
        .await;
    assert_eq!(harness.parent_recoveries(), 0);

    release_parent
        .send(())
        .expect("the parent request is waiting for release");

    assert_eq!(harness.next_commit().await, parent.hash());
    assert_eq!(harness.next_commit().await, child.hash());
}

/// Two children of the same missing parent share one parent download.
#[tokio::test(start_paused = true)]
async fn concurrent_children_share_one_parent_download() {
    let parent = block_1();
    let child = block_2();
    let sibling = sibling_of(&child, 7);
    assert_ne!(child.hash(), sibling.hash());
    let mut harness = Harness::new([parent.clone(), child.clone(), sibling.clone()]);
    let release_parent = harness.hold(parent.hash());

    for (block, source) in [(&child, legacy(2)), (&sibling, legacy(3))] {
        assert_eq!(
            harness
                .downloads
                .download_and_verify(block.hash(), Some(source)),
            DownloadAction::AddedToQueue
        );
    }

    let requests = harness.drain_requests().await;
    let parent_requests = requests
        .iter()
        .filter(|request| requested_hash(request) == parent.hash())
        .count();
    assert_eq!(
        requests.len(),
        3,
        "two children and one parent: {requests:?}"
    );
    assert_eq!(parent_requests, 1, "children share the parent download");
    assert_eq!(harness.parent_recoveries(), 1);

    release_parent
        .send(())
        .expect("the parent request is waiting for release");

    assert_eq!(harness.next_commit().await, parent.hash());
    let children = HashSet::from([harness.next_commit().await, harness.next_commit().await]);
    assert_eq!(children, HashSet::from([child.hash(), sibling.hash()]));
}

/// A parent that no peer delivers does not stop an unrelated block whose parent is known.
#[tokio::test(start_paused = true)]
async fn unavailable_parent_does_not_block_unrelated_ready_blocks() {
    let orphan = with_parent(&block_2(), unavailable(0x42));
    let ready = block_1();
    let mut harness = Harness::new([orphan.clone(), ready.clone()]);

    assert_eq!(
        harness
            .downloads
            .download_and_verify(orphan.hash(), Some(legacy(2))),
        DownloadAction::AddedToQueue
    );
    assert_eq!(
        harness.next_request().await,
        request_from(orphan.hash(), legacy(2))
    );
    assert_eq!(
        harness.next_request().await,
        request_from(unavailable(0x42), legacy(2))
    );

    assert_eq!(
        harness
            .downloads
            .download_and_verify(ready.hash(), Some(legacy(3))),
        DownloadAction::AddedToQueue
    );
    assert_eq!(
        harness.next_request().await,
        request_from(ready.hash(), legacy(3))
    );
    assert_eq!(harness.next_commit().await, ready.hash());

    harness
        .assert_no_commit("the orphan still waits for its unavailable parent")
        .await;
}

/// A recovered parent whose own parent is missing does not start another recovery.
#[tokio::test(start_paused = true)]
async fn recovered_parent_does_not_recover_its_own_parent() {
    let parent = with_parent(&block_1(), unavailable(0x77));
    let child = with_parent(&block_2(), parent.hash());
    let mut harness = Harness::new([parent.clone(), child.clone()]);

    assert_eq!(
        harness
            .downloads
            .download_and_verify(child.hash(), Some(legacy(2))),
        DownloadAction::AddedToQueue
    );

    let requests = harness.drain_requests().await;
    assert_eq!(
        requests,
        vec![
            request_from(child.hash(), legacy(2)),
            request_from(parent.hash(), legacy(2)),
        ],
        "recovery stops after one hop"
    );
}

/// A child more than one block beyond the tip's child leaves the gap to the syncer.
#[tokio::test(start_paused = true)]
async fn child_beyond_one_hop_from_the_tip_does_not_recover_its_parent() {
    // The tip is at height 0, so block 3 is missing two blocks.
    let child = block_3();
    let mut harness = Harness::new([block_2(), child.clone()]);

    assert_eq!(
        harness
            .downloads
            .download_and_verify(child.hash(), Some(legacy(2))),
        DownloadAction::AddedToQueue
    );

    assert_eq!(
        harness.drain_requests().await,
        vec![request_from(child.hash(), legacy(2))]
    );
}

/// One advertiser can have only one recovery in flight, however many orphans it sends.
#[tokio::test(start_paused = true)]
async fn one_parent_recovery_per_advertiser() {
    let peer = zn::PeerSource::Zakura(
        zn::zakura::ZakuraPeerId::new(vec![9; 32]).expect("test peer id is within bounds"),
    );
    let orphans = [
        with_parent(&block_2(), unavailable(0x51)),
        with_parent(&block_2(), unavailable(0x52)),
    ];
    let mut harness = Harness::new(orphans.clone());

    for orphan in &orphans {
        assert_eq!(
            harness
                .downloads
                .download_and_verify(orphan.hash(), Some(peer.clone())),
            DownloadAction::AddedToQueue
        );
    }

    let requests = harness.drain_requests().await;
    let recoveries = requests
        .iter()
        .filter(|request| [unavailable(0x51), unavailable(0x52)].contains(&requested_hash(request)))
        .count();
    assert_eq!(
        requests.len(),
        3,
        "two orphans and one recovery: {requests:?}"
    );
    assert_eq!(recoveries, 1);
    assert_eq!(harness.parent_recoveries(), 1);
}

/// A flood of orphans with unavailable parents, from many advertisers, starts at most
/// [`MAX_PARENT_RECOVERIES`] recoveries.
#[tokio::test(start_paused = true)]
async fn orphan_flood_with_unavailable_parents_is_bounded() {
    const ORPHANS: u8 = 40;

    let orphans: Vec<_> = (0..ORPHANS)
        .map(|index| with_parent(&block_2(), unavailable(100 + index)))
        .collect();
    let mut harness = Harness::new(orphans.clone());

    for (index, orphan) in (0..ORPHANS).zip(&orphans) {
        assert_eq!(
            harness
                .downloads
                .download_and_verify(orphan.hash(), Some(legacy(100 + index))),
            DownloadAction::AddedToQueue
        );
    }

    let requests = harness.drain_requests().await;
    let orphan_hashes: HashSet<_> = orphans.iter().map(|orphan| orphan.hash()).collect();
    let recoveries = requests
        .iter()
        .filter(|request| !orphan_hashes.contains(&requested_hash(request)))
        .count();

    assert_eq!(requests.len() - recoveries, usize::from(ORPHANS));
    assert_eq!(recoveries, MAX_PARENT_RECOVERIES);
    assert_eq!(harness.parent_recoveries(), MAX_PARENT_RECOVERIES);
    assert_eq!(
        harness.downloads.spawner.admission().queue_len(),
        usize::from(ORPHANS) + MAX_PARENT_RECOVERIES
    );
}

#[tokio::test(start_paused = true)]
async fn dropping_downloads_cancels_child_and_parent_recovery() {
    let child = block_2();
    let mut harness = Harness::new([child.clone()]);
    harness
        .downloads
        .download_and_verify(child.hash(), Some(legacy(2)));
    harness.next_request().await;
    harness.next_request().await;
    let Harness {
        downloads,
        mut requests,
        mut commits,
        ..
    } = harness;
    drop(downloads);
    tokio::time::timeout(EXPECTED_WAIT, async {
        assert!(requests.recv().await.is_none());
        assert!(commits.recv().await.is_none());
    })
    .await
    .expect("dropping downloads cancels every task and releases its services");
}

/// A child without valid proof of work starts no recovery and charges nothing to its advertiser,
/// so that advertiser's later child with valid proof of work recovers its own parent.
#[tokio::test(start_paused = true)]
async fn invalid_proof_of_work_child_does_not_start_a_recovery() {
    let peer = zn::PeerSource::Zakura(
        zn::zakura::ZakuraPeerId::new(vec![9; 32]).expect("test peer id is within bounds"),
    );
    let parent = block_1();
    let child = block_2();
    let invalid = with_parent(&child, unavailable(0x42));
    let mut harness = Harness::on_network(
        Network::Mainnet,
        [parent.clone(), child.clone(), invalid.clone()],
    );

    assert_eq!(
        harness
            .downloads
            .download_and_verify(invalid.hash(), Some(peer.clone())),
        DownloadAction::AddedToQueue
    );
    assert_eq!(
        harness.drain_requests().await,
        vec![request_from(invalid.hash(), peer.clone())],
        "the unavailable parent is not requested"
    );
    assert_eq!(harness.parent_recoveries(), 0);

    assert_eq!(
        harness
            .downloads
            .download_and_verify(child.hash(), Some(peer.clone())),
        DownloadAction::AddedToQueue
    );
    assert_eq!(
        harness.next_request().await,
        request_from(child.hash(), peer.clone())
    );
    assert_eq!(
        harness.next_request().await,
        request_from(parent.hash(), peer)
    );
}

/// A finished recovery holds nothing, even before the stream collects its result.
///
/// The stream is only polled on the next inbound request, so a failed parent download must not
/// look in flight to the next child that needs the same parent.
#[tokio::test(start_paused = true)]
async fn failed_recovery_does_not_suppress_a_later_recovery() {
    let parent = block_1();
    let child = block_2();
    let sibling = sibling_of(&child, 7);
    let mut harness = Harness::new([parent.clone(), child.clone(), sibling.clone()]);
    harness
        .failures
        .lock()
        .expect("failure lock is not poisoned")
        .insert(parent.hash());

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
    assert_eq!(
        harness.next_request().await,
        request_from(parent.hash(), legacy(2))
    );

    // The failed task finishes while the stream is idle.
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(harness.parent_recoveries(), 0);
    assert_eq!(harness.downloads.spawner.admission().queue_len(), 1);

    assert_eq!(
        harness
            .downloads
            .download_and_verify(sibling.hash(), Some(legacy(3))),
        DownloadAction::AddedToQueue
    );
    assert_eq!(
        harness.next_request().await,
        request_from(sibling.hash(), legacy(3))
    );
    assert_eq!(
        harness.next_request().await,
        request_from(parent.hash(), legacy(3)),
        "the later child requests the parent again"
    );

    assert_eq!(harness.next_commit().await, parent.hash());
    let children = HashSet::from([harness.next_commit().await, harness.next_commit().await]);
    assert_eq!(children, HashSet::from([child.hash(), sibling.hash()]));
}
