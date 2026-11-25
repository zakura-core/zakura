//! The BPv1 verifier adapter over the real block verifier router and state, during
//! catch-up (#2718).
//!
//! A miner node generates a post-NU7 Regtest chain. A second node starts at
//! genesis and receives that chain as a node behind the tip does: BPv1 submits a
//! tip block far above the verified tip, and block sync delivers the blocks below
//! it with the request the block-sync driver sends.
//!
//! A block whose parent is missing waits in one of two places. With ZIP 234
//! active, the router waits for the parent's block info before the state sees the
//! block. Without it, the state holds the block in its orphan queue. The tests
//! cover both.

use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc;
use tower::{buffer::Buffer, ServiceExt};
use zakura_chain::{
    amount::Amount,
    block::{self, genesis::regtest_genesis_block, Block, Height},
    chain_sync_status::mock::MockSyncStatus,
    chain_tip::ChainTip,
    parameters::{
        subsidy::is_zip234_active,
        testnet::{ConfiguredActivationHeights, ConfiguredLockboxDisbursement, RegtestParameters},
        Network,
    },
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transparent::Address,
};
use zakura_consensus::RouterError;
use zakura_header_chain::BodyVerificationClass;
use zakura_network::zakura::bpv1::{BlockVerifierPort, VerifierVerdict, WorkId};
use zakura_node_services::{
    block_memory::{Admission, DecodedBlockPool, OwnedBlock},
    mempool,
};
use zakura_rpc::methods::{RpcImpl, RpcServer};

use super::{RouterBlockVerifierAdapter, SyncCoordinator};
use crate::commands::start::zakura::block_verify_error_class;

/// The gap between the verified tip and the block BPv1 submits, as in #2718.
const GAP: usize = 20;

/// Bounds each test, so a wedged handoff fails instead of hanging.
const TEST_LIMIT: Duration = Duration::from_secs(30);

type Router =
    Buffer<zakura_consensus::Request, BoxFuture<'static, Result<block::Hash, RouterError>>>;
type State = Buffer<
    zakura_state::Request,
    BoxFuture<'static, Result<zakura_state::Response, zakura_state::BoxError>>,
>;

/// Regtest with NU7 active from height 1, and ZIP 234 too if `zip234` is set.
///
/// NU6.1 activates at height 1 too, and its block must pay a lockbox
/// disbursement, so the network configures one of zero value to a Regtest P2SH
/// address.
fn network(zip234: bool) -> Network {
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        },
        lockbox_disbursements: Some(vec![ConfiguredLockboxDisbursement {
            address: "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8".to_string(),
            amount: Amount::zero(),
        }]),
        test_nsm_reissuance_height: zip234.then_some(Height(1)),
        ..Default::default()
    });
    assert_eq!(
        is_zip234_active(
            &network,
            Height(u32::try_from(GAP).expect("the gap fits a height"))
        ),
        zip234,
        "the network activates ZIP 234 below the far block only if asked",
    );
    network
}

/// A node's real state and block verifier router, with taps on the router's state
/// requests.
struct Node {
    router: Router,
    state: State,
    tip: zakura_state::LatestChainTip,
    read_state: zakura_state::ReadStateService,
    /// The state commits the router requested, per block.
    commits: Arc<Mutex<HashMap<block::Hash, usize>>>,
    /// The parents whose block info the router waited for, in request order.
    parent_waits: mpsc::UnboundedReceiver<block::Hash>,
}

impl Node {
    /// A node whose verified tip is the Regtest genesis block.
    async fn at_genesis(network: &Network) -> Self {
        let zakura_state::InitializedState {
            state,
            read_state,
            latest_chain_tip: tip,
            ..
        } = zakura_state::init_test_services(network).await;
        state
            .clone()
            .oneshot(zakura_state::Request::CommitCheckpointVerifiedBlock(
                regtest_genesis_block().into(),
            ))
            .await
            .expect("the state commits genesis");

        let commits = Arc::new(Mutex::new(HashMap::new()));
        let (parent_wait, parent_waits) = mpsc::unbounded_channel();
        let tapped = tower::service_fn({
            let state = state.clone();
            let commits = commits.clone();
            move |request: zakura_state::Request| {
                match &request {
                    zakura_state::Request::CommitSemanticallyVerifiedBlock(block)
                    | zakura_state::Request::CommitSemanticallyVerifiedBlockWithAdmission {
                        block,
                        ..
                    } => {
                        *commits
                            .lock()
                            .expect("the commit tap is not poisoned")
                            .entry(block.hash)
                            .or_default() += 1;
                    }
                    zakura_state::Request::AwaitBlockInfo(parent) => {
                        let _ = parent_wait.send(*parent);
                    }
                    _ => {}
                }
                state.clone().oneshot(request)
            }
        });
        let (router, _transaction_verifier, _background, _max_checkpoint_height) =
            zakura_consensus::router::init_test(
                zakura_consensus::Config::default(),
                network,
                tapped,
            )
            .await;

        Node {
            router,
            state,
            tip,
            read_state,
            commits,
            parent_waits,
        }
    }

    /// Delivers `block` as native block sync does, and returns the class the
    /// block-sync driver gives a failure.
    fn deliver(
        &self,
        block: &Arc<Block>,
    ) -> BoxFuture<'static, Result<block::Hash, BodyVerificationClass>> {
        self.router
            .clone()
            .oneshot(zakura_consensus::Request::Commit(block.clone().into()))
            .map(|result| result.map_err(|error| block_verify_error_class(&error)))
            .boxed()
    }

    /// The next parent whose block info the router waits for.
    async fn next_parent_wait(&mut self) -> block::Hash {
        self.parent_waits
            .recv()
            .await
            .expect("the router keeps its state tap")
    }

    /// The number of state commits the router requested for `hash`.
    fn commits(&self, hash: block::Hash) -> usize {
        self.commits
            .lock()
            .expect("the commit tap is not poisoned")
            .get(&hash)
            .copied()
            .unwrap_or_default()
    }
}

/// Genesis and the `length` blocks a miner node generates on it.
async fn mined_chain(network: &Network, length: usize) -> Vec<Arc<Block>> {
    let miner = Node::at_genesis(network).await;
    let tip = miner.tip.clone();
    let mempool = tower::service_fn(move |request: mempool::Request| {
        let tip_hash = tip.best_tip_hash();
        async move {
            match request {
                mempool::Request::FullTransactions => Ok(mempool::Response::FullTransactions {
                    transactions: vec![],
                    transaction_dependencies: Default::default(),
                    last_seen_tip_hash: tip_hash.expect("the miner has a tip"),
                }),
                request => Err(format!("the miner's mempool has no answer to {request:?}").into()),
            }
        }
    });
    let mut sync_status = MockSyncStatus::default();
    sync_status.set_is_close_to_tip(true);
    let (_last_event, last_event) = tokio::sync::watch::channel(None);
    let (rpc, queue) = RpcImpl::new(
        network.clone(),
        zakura_rpc::config::mining::Config {
            miner_address: Some(Address::from_pub_key_hash(network.kind(), [7; 20]).into()),
            ..Default::default()
        },
        false,
        "test",
        "bpv1 catch-up test",
        mempool,
        miner.state.clone(),
        miner.read_state.clone(),
        miner.router.clone(),
        sync_status,
        miner.tip.clone(),
        zakura_network::address_book_peers::MockAddressBookPeers::default(),
        last_event,
        None,
    );
    rpc.generate(u32::try_from(length).expect("the test chain is short"))
        .await
        .expect("the miner generates the chain");
    queue.abort();

    let mut chain = Vec::with_capacity(length + 1);
    for height in 0..=length {
        let height = Height(u32::try_from(height).expect("the test chain is short"));
        let zakura_state::ReadResponse::Block(Some(block)) = miner
            .read_state
            .clone()
            .oneshot(zakura_state::ReadRequest::Block(height.into()))
            .await
            .expect("the miner's state answers a block query")
        else {
            panic!("the miner committed a block at {height:?}");
        };
        // The state answers with a block handle. Decode a plain block from its bytes.
        let bytes = block
            .zcash_serialize_to_vec()
            .expect("a committed block serializes");
        chain.push(Arc::new(
            bytes
                .zcash_deserialize_into()
                .expect("a serialized block deserializes"),
        ));
    }
    chain
}

/// `block` as BPv1 holds it after assembly: decoded from its bytes, so it shares
/// no transaction with block sync's copy.
fn owned(block: &Arc<Block>) -> OwnedBlock {
    let decoded: Block = block
        .zcash_serialize_to_vec()
        .expect("a mined block serializes")
        .zcash_deserialize_into()
        .expect("a serialized block deserializes");
    DecodedBlockPool::for_block_count(NonZeroU64::new(4).expect("4 is non-zero"))
        .try_admit(decoded, Admission::WithinLimit)
        .expect("an empty pool admits one unshared block")
}

/// BPv1 submits `H+20` while the verified tip is `H`, and block sync delivers
/// `H+1..H+19` (#2718). Block sync's own copy of `H+20` arrives while BPv1's copy
/// waits for its parent, and waits for it in turn (#2726). Once the gap commits,
/// `H+20` commits once, through BPv1, and block sync's copy is a duplicate.
async fn far_block_commits_once_after_block_sync_fills_the_gap(zip234: bool) {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(TEST_LIMIT, async {
        let network = network(zip234);
        let chain = mined_chain(&network, GAP).await;
        let mut node = Node::at_genesis(&network).await;
        let adapter = Arc::new(RouterBlockVerifierAdapter::new(
            node.router.clone(),
            SyncCoordinator::new(),
        ));
        let (gap, far) = (&chain[1..GAP], &chain[GAP]);

        let bpv1 = tokio::spawn({
            let adapter = adapter.clone();
            let block = owned(far);
            async move { adapter.submit(block, WorkId(1)).await }
        });
        if zip234 {
            assert_eq!(node.next_parent_wait().await, gap[GAP - 2].hash());
            assert_eq!(
                node.commits(far.hash()),
                0,
                "the state has not seen the submission, so it holds no place in the orphan queue",
            );
        } else {
            // The parent is missing, so the state holds the submission in its orphan queue,
            // and it is the only block there.
            while node.commits(far.hash()) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        let sync_copy = tokio::spawn(node.deliver(far));
        for block in gap {
            assert_eq!(
                node.deliver(block).await,
                Ok(block.hash()),
                "the state commits each gap block, and refuses none with QueueFull",
            );
        }

        assert_eq!(
            bpv1.await.expect("the submission task finishes"),
            VerifierVerdict::Valid
        );
        assert_eq!(
            sync_copy.await.expect("the delivery task finishes"),
            Err(BodyVerificationClass::Duplicate),
        );
        assert_eq!(node.commits(far.hash()), 1, "the router verifies H+20 once");
        assert_eq!(node.tip.best_tip_hash(), Some(far.hash()));
    })
    .await
    .expect("the handoff finishes");
}

/// With ZIP 234 active, the far block waits for its parent's block info, outside
/// the state's orphan queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_far_bpv1_block_waiting_for_parent_info_commits_once_after_block_sync_fills_the_gap() {
    far_block_commits_once_after_block_sync_fills_the_gap(true).await;
}

/// Without ZIP 234, the far block waits in the state's orphan queue, and takes
/// one place there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_far_bpv1_block_waiting_in_the_orphan_queue_commits_once_after_block_sync_fills_the_gap()
{
    far_block_commits_once_after_block_sync_fills_the_gap(false).await;
}

/// Block sync commits a block first. BPv1's later submission of it is a duplicate,
/// and the router does not verify it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bpv1_block_that_block_sync_committed_first_is_a_duplicate() {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(TEST_LIMIT, async {
        let network = network(true);
        let chain = mined_chain(&network, 1).await;
        let node = Node::at_genesis(&network).await;
        let adapter = RouterBlockVerifierAdapter::new(node.router.clone(), SyncCoordinator::new());
        let next = &chain[1];

        assert_eq!(node.deliver(next).await, Ok(next.hash()));
        assert_eq!(
            adapter.submit(owned(next), WorkId(1)).await,
            VerifierVerdict::Duplicate
        );
        assert_eq!(node.commits(next.hash()), 1);
    })
    .await
    .expect("the handoff finishes");
}
