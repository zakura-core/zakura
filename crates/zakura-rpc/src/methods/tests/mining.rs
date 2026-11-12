//! Regressions for parent-dependent mining RPC work and failures.

use std::{
    future::Future,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll},
};

use futures::{future::BoxFuture, FutureExt};
use zakura_chain::{
    block::Hash,
    block_info::BlockInfo,
    chain_sync_status::MockSyncStatus,
    chain_tip::mock::MockChainTip,
    parameters::testnet::{ConfiguredActivationHeights, RegtestParameters},
    serialization::{DateTime32, Duration32},
};
use zakura_network::address_book_peers::MockAddressBookPeers;
use zakura_node_services::BoxError;
use zakura_state::GetBlockTemplateChainInfo;
use zakura_test::mock_service::{MockService, PanicAssertion};
use zcash_address::{ToAddress, ZcashAddress};

use super::super::types::get_block_template::constants::MAX_FULL_FIRST_ANSWERS;
use super::super::*;
use super::utils::fake_history_tree;
use super::vectors::fake_verified_mempool_tx;
use crate::tests::test_rpc::{no_mempool_snapshots, TestRpcImpl};

type Mock<Req, Resp> = MockService<Req, Resp, PanicAssertion, BoxError>;
type Verifier = Mock<zakura_consensus::Request, Hash>;

fn network() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        },
        test_nsm_reissuance_height: Some(Height(3)),
        ..Default::default()
    })
}

fn rpc<M: MempoolService, R: ReadStateService>(
    network: Network,
    mempool: M,
    read: R,
    tip: MockChainTip,
) -> (TestRpcImpl, Verifier) {
    rpc_reading_mempool_snapshots(network, mempool, no_mempool_snapshots(), read, tip)
}

/// Like [`rpc`], for a mempool that publishes its snapshots on `mempool_snapshots`.
fn rpc_reading_mempool_snapshots<M: MempoolService, R: ReadStateService>(
    network: Network,
    mempool: M,
    mempool_snapshots: mempool::MempoolSnapshotReceiver,
    read: R,
    tip: MockChainTip,
) -> (TestRpcImpl, Verifier) {
    // Template construction can wait for the build slot and can exceed the mock
    // default of 300 ms on busy CI runners. Use the enclosing test deadline.
    let verifier: Verifier = MockService::build()
        .with_max_request_delay(Duration::from_secs(10))
        .for_unit_tests();
    let miner_address = ZcashAddress::from_transparent_p2pkh(network.network_type(), [0x7e; 20]);
    let mut sync_status = MockSyncStatus::default();
    sync_status.set_is_close_to_tip(true);
    let (_, rx) = watch::channel(None);
    let (rpc, queue) = TestRpcImpl::new_for_tests_with_mempool_snapshots(
        network,
        config::mining::Config {
            miner_address: Some(miner_address),
            internal_miner: true,
            optimistic_block_inventory: true,
            ..Default::default()
        },
        false,
        "test",
        "mining regression",
        mempool,
        mempool_snapshots,
        MockService::build().for_unit_tests(),
        read,
        verifier.clone(),
        sync_status,
        tip,
        MockAddressBookPeers::default(),
        rx,
        None,
    );
    queue.abort();
    (rpc, verifier)
}

/// A permanently stalled service distinguishes readiness waits from response waits.
#[derive(Clone)]
struct StalledRead {
    ready: bool,
}
impl Service<ReadRequest> for StalledRead {
    type Response = ReadResponse;
    type Error = BoxError;
    type Future = BoxFuture<'static, std::result::Result<ReadResponse, BoxError>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), BoxError>> {
        if self.ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn call(&mut self, _: ReadRequest) -> Self::Future {
        futures::future::pending().boxed()
    }
}

#[tokio::test(start_paused = true)]
async fn subsidy_timeout_covers_readiness_and_response() {
    for ready in [false, true] {
        let (tip, _) = MockChainTip::new();
        let (rpc, _) = rpc(
            network(),
            MockService::build().for_unit_tests(),
            StalledRead { ready },
            tip,
        );
        let request = rpc.get_block_subsidy(Some(3));
        tokio::pin!(request);
        assert!(futures::poll!(&mut request).is_pending());
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(futures::poll!(&mut request).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        let Poll::Ready(Err(error)) = futures::poll!(&mut request) else {
            panic!("the entire parent lookup must expire at its deadline");
        };
        assert_eq!(error.code(), -1);
        assert_eq!(
            error.message(),
            "timed out waiting for parent block information"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn subsidy_lookup_preserves_success_and_service_errors() {
    for fail in [false, true] {
        let read = tower::service_fn(move |request| async move {
            assert!(matches!(request, ReadRequest::BlockInfo(_)));
            tokio::time::sleep(Duration::from_secs(29)).await;
            if fail {
                Err::<ReadResponse, BoxError>("state read failed".into())
            } else {
                Ok(ReadResponse::BlockInfo(Some(BlockInfo::new(
                    Default::default(),
                    0,
                ))))
            }
        });
        let (tip, _) = MockChainTip::new();
        let (rpc, _) = rpc(network(), MockService::build().for_unit_tests(), read, tip);
        let result = rpc.get_block_subsidy(Some(3)).await;
        if fail {
            let error = result.unwrap_err();
            assert_eq!(error.code(), -1);
            assert_eq!(error.message(), "state read failed");
        } else {
            result.unwrap();
        }
    }
}

/// `getblocksubsidy` reports the ZIP 214 Revision 2 streams until ZIP 218 moves the third
/// halving, which is 4,656,000 on Testnet with NU7 at 4,386,000.
#[tokio::test]
async fn subsidy_reports_revision_2_streams_until_the_zip_218_third_halving() {
    let mut activation_heights: ConfiguredActivationHeights = Network::new_default_testnet()
        .parameters()
        .expect("Testnet has parameters")
        .activation_heights()
        .into();
    activation_heights.nu7 = Some(4_386_000);
    let network = zakura_chain::parameters::testnet::Parameters::build()
        .with_activation_heights(activation_heights)
        .expect("activation heights are valid")
        .to_network()
        .expect("configured network is valid");

    // A zero NSM value balance adds no reissuance to the block subsidy.
    let read = tower::service_fn(|request| async move {
        assert!(matches!(request, ReadRequest::BlockInfo(_)));
        Ok::<_, BoxError>(ReadResponse::BlockInfo(Some(BlockInfo::new(
            Default::default(),
            0,
        ))))
    });
    let (tip, _) = MockChainTip::new();
    let (rpc, _) = rpc(network, MockService::build().for_unit_tests(), read, tip);
    let zatoshis = |zec: Zec<zakura_chain::amount::NonNegative>| i64::from(zec);

    // 4,476,000 is the third halving before ZIP 218.
    for height in [4_476_000, 4_655_999] {
        let subsidy = rpc
            .get_block_subsidy(Some(height))
            .await
            .expect("the subsidy is available");
        assert_eq!(zatoshis(subsidy.total_block_subsidy()), 52_083_333);
        assert_eq!(zatoshis(subsidy.funding_streams_total()), 4_166_666);
        assert_eq!(zatoshis(subsidy.lockbox_total()), 6_249_999);
        assert_eq!(zatoshis(subsidy.miner()), 41_666_668);
    }

    let subsidy = rpc
        .get_block_subsidy(Some(4_656_000))
        .await
        .expect("the subsidy is available");
    assert_eq!(zatoshis(subsidy.total_block_subsidy()), 26_041_666);
    assert_eq!(zatoshis(subsidy.funding_streams_total()), 0);
    assert_eq!(zatoshis(subsidy.lockbox_total()), 0);
    assert_eq!(zatoshis(subsidy.miner()), 26_041_666);
}

#[tokio::test]
async fn subsidy_before_activation_does_not_read_parent() {
    let (tip, _) = MockChainTip::new();
    let (rpc, _) = rpc(
        network(),
        MockService::build().for_unit_tests(),
        StalledRead { ready: false },
        tip,
    );
    assert!(rpc
        .get_block_subsidy(Some(2))
        .now_or_never()
        .unwrap()
        .is_ok());
}

fn chain_info(height: u32, hash: u8, balance: i64) -> GetBlockTemplateChainInfo {
    let mut pools = ValueBalance::default();
    pools.set_nsm_value_balance_amount(balance.try_into().unwrap());
    GetBlockTemplateChainInfo {
        value_pools: pools,
        expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
        tip_height: Height(height),
        tip_hash: Hash([hash; 32]),
        cur_time: 1654008617.into(),
        min_time: 1654008606.into(),
        max_time: 1654008728.into(),
        chain_history_root: fake_history_tree(&Network::Mainnet).hash(),
    }
}

fn mining_rpc(
    info: watch::Receiver<GetBlockTemplateChainInfo>,
) -> (
    TestRpcImpl,
    zakura_chain::chain_tip::mock::MockChainTipSender,
    Verifier,
) {
    let (tip, sender) = MockChainTip::new();
    sender.send_best_tip_height(info.borrow().tip_height);
    sender.send_best_tip_hash(info.borrow().tip_hash);
    let mempool_info = info.clone();
    let mempool = tower::service_fn(move |_| {
        let hash = mempool_info.borrow().tip_hash;
        async move {
            Ok::<_, BoxError>(mempool::Response::FullTransactions {
                transactions: vec![].into(),
                transaction_dependencies: Default::default(),
                last_seen_tip_hash: hash,
            })
        }
    });
    let read = tower::service_fn(move |request| {
        let info = info.borrow().clone();
        async move {
            Ok::<_, BoxError>(match request {
                ReadRequest::ChainInfo => ReadResponse::ChainInfo(info),
                // The fallback path confirms a failed proposal against committed
                // state, which can be ahead of both watches.
                ReadRequest::Tip => ReadResponse::Tip(Some((info.tip_height, info.tip_hash))),
                other => unreachable!("unexpected read request: {other:?}"),
            })
        }
    });
    let (rpc, verifier) = rpc(network(), mempool, read, tip);
    (rpc, sender, verifier)
}

/// Returns a mining RPC whose state reads come from `info`, with its tip sender, a count of
/// chain info reads, and a flag that makes the next mempool fetch last one mempool poll interval.
fn counting_mining_rpc(
    info: watch::Receiver<GetBlockTemplateChainInfo>,
) -> (
    TestRpcImpl,
    zakura_chain::chain_tip::mock::MockChainTipSender,
    Arc<AtomicUsize>,
    Arc<AtomicBool>,
) {
    let (tip, sender) = MockChainTip::new();
    sender.send_best_tip_height(info.borrow().tip_height);
    sender.send_best_tip_hash(info.borrow().tip_hash);
    let read_count = Arc::new(AtomicUsize::new(0));
    let reads = read_count.clone();
    let read_info = info.clone();
    let read = tower::service_fn(move |request| {
        let info = read_info.borrow().clone();
        if matches!(request, ReadRequest::ChainInfo) {
            reads.fetch_add(1, Ordering::SeqCst);
        }
        async move {
            Ok::<_, BoxError>(match request {
                ReadRequest::ChainInfo => ReadResponse::ChainInfo(info),
                ReadRequest::Tip => ReadResponse::Tip(Some((info.tip_height, info.tip_hash))),
                other => unreachable!("unexpected read request: {other:?}"),
            })
        }
    });
    let slow_mempool = Arc::new(AtomicBool::new(false));
    let slow = slow_mempool.clone();
    let mempool = tower::service_fn(move |_| {
        let hash = info.borrow().tip_hash;
        let slow = slow.swap(false, Ordering::SeqCst);
        async move {
            if slow {
                tokio::time::sleep(Duration::from_secs(MEMPOOL_LONG_POLL_INTERVAL)).await;
            }
            Ok::<_, BoxError>(mempool::Response::FullTransactions {
                transactions: vec![].into(),
                transaction_dependencies: Default::default(),
                last_seen_tip_hash: hash,
            })
        }
    });
    let (rpc, _) = rpc(network(), mempool, read, tip);
    (rpc, sender, read_count, slow_mempool)
}

/// Returns a mining RPC whose mempool holds one transaction and has processed the tip `info`
/// names, with its tip sender, the sender its mempool publishes snapshots on, and a count of
/// the requests sent to the mempool.
///
/// The mempool service always answers for the tip `info` names, as the mempool does once a
/// request makes it process the new block. The caller publishes the snapshot for a tip to have
/// the mempool process that block without a request.
fn populated_mining_rpc(
    info: watch::Receiver<GetBlockTemplateChainInfo>,
) -> (
    TestRpcImpl,
    zakura_chain::chain_tip::mock::MockChainTipSender,
    mempool::MempoolSnapshotSender,
    Arc<AtomicUsize>,
) {
    let (rpc, tip, mempool_snapshots, request_count, _) =
        populated_mining_rpc_with(info, MempoolTip::FollowsChain);
    (rpc, tip, mempool_snapshots, request_count)
}

/// The block that the mempool in [`populated_mining_rpc_with`] answers requests for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MempoolTip {
    /// The tip the chain info names: a request makes the mempool process a new block.
    FollowsChain,
    /// The block of the last snapshot published, starting with the tip the chain info names
    /// at first: the mempool has not seen a later block until its snapshot is published.
    LastPublished,
}

/// Like [`populated_mining_rpc`], with the mempool answering for `mempool_tip`, and also
/// returning the block verifier.
fn populated_mining_rpc_with(
    info: watch::Receiver<GetBlockTemplateChainInfo>,
    mempool_tip: MempoolTip,
) -> (
    TestRpcImpl,
    zakura_chain::chain_tip::mock::MockChainTipSender,
    mempool::MempoolSnapshotSender,
    Arc<AtomicUsize>,
    Verifier,
) {
    let (tip, sender) = MockChainTip::new();
    sender.send_best_tip_height(info.borrow().tip_height);
    sender.send_best_tip_hash(info.borrow().tip_hash);
    let mempool_snapshots = mempool::MempoolSnapshotSender::new();
    let request_count = Arc::new(AtomicUsize::new(0));
    let requests = request_count.clone();
    let processed_tip = Arc::new(std::sync::Mutex::new(info.borrow().tip_hash));
    let published = mempool_snapshots.subscribe();
    let mempool_info = info.clone();
    let mempool = tower::service_fn(move |_| {
        requests.fetch_add(1, Ordering::SeqCst);
        let hash = match mempool_tip {
            MempoolTip::FollowsChain => mempool_info.borrow().tip_hash,
            MempoolTip::LastPublished => {
                let mut processed_tip = processed_tip.lock().unwrap();
                if let Some(snapshot) = published.latest() {
                    *processed_tip = snapshot.last_seen_tip_hash;
                }
                *processed_tip
            }
        };
        async move {
            Ok::<_, BoxError>(mempool::Response::FullTransactions {
                transactions: vec![fake_verified_mempool_tx()].into(),
                transaction_dependencies: Default::default(),
                last_seen_tip_hash: hash,
            })
        }
    });
    let read = tower::service_fn(move |request| {
        let info = info.borrow().clone();
        async move {
            Ok::<_, BoxError>(match request {
                ReadRequest::ChainInfo => ReadResponse::ChainInfo(info),
                other => unreachable!("unexpected read request: {other:?}"),
            })
        }
    });
    let (rpc, verifier) =
        rpc_reading_mempool_snapshots(network(), mempool, mempool_snapshots.subscribe(), read, tip);
    (rpc, sender, mempool_snapshots, request_count, verifier)
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("test work must complete")
}

/// A single blocking thread lets these tests hold construction at its scheduling
/// boundary without adding instrumentation to the RPC implementation.
fn mining_runtime(test: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(test);
}

struct BlockingPoolGate {
    release: Option<std::sync::mpsc::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl BlockingPoolGate {
    async fn new() -> Self {
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let task = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            // An OS deadline also bounds runtime shutdown if a test fails.
            released.recv_timeout(Duration::from_secs(10)).unwrap();
        });
        let gate = Self {
            release: Some(release),
            task: Some(task),
        };
        bounded(started).await.unwrap();
        gate
    }

    async fn release(mut self) {
        self.release.take().unwrap().send(()).unwrap();
        bounded(self.task.take().unwrap()).await.unwrap();
    }
}

impl Drop for BlockingPoolGate {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

/// Holds the one template build slot, so these tests can stop construction where it is
/// admitted without adding instrumentation to the RPC implementation.
struct TemplateBuildGate {
    release: Option<std::sync::mpsc::Sender<()>>,
    build: Option<JoinHandle<Result<()>>>,
}

impl TemplateBuildGate {
    async fn new(rpc: &TestRpcImpl) -> Self {
        let handler = rpc.gbt.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let build = tokio::spawn(async move {
            handler
                .run_template_build(TemplateBuildWork::Proof, move || {
                    entered.send(()).unwrap();
                    // An OS deadline also bounds runtime shutdown if a test fails.
                    released.recv_timeout(Duration::from_secs(10)).unwrap();
                })
                .await
        });
        let gate = Self {
            release: Some(release),
            build: Some(build),
        };
        bounded(started).await.unwrap();
        gate
    }

    async fn release(mut self) {
        self.release.take().unwrap().send(()).unwrap();
        bounded(self.build.take().unwrap()).await.unwrap().unwrap();
    }
}

impl Drop for TemplateBuildGate {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

/// A miner paid to a transparent address has no proof to create, so its template is built
/// where it was asked for, however busy the blocking threads are.
#[test]
fn proof_free_template_does_not_wait_for_a_blocking_thread() {
    mining_runtime(async {
        let (_, info) = watch::channel(chain_info(2, 1, 400_000_000));
        let (rpc, _, _) = mining_rpc(info);
        let gate = BlockingPoolGate::new().await;
        bounded(rpc.get_block_template(None)).await.unwrap();
        gate.release().await;
    });
}

/// Templates are still built one at a time when they are built in place.
#[test]
fn proof_free_template_waits_for_the_build_slot() {
    mining_runtime(async {
        let (_, info) = watch::channel(chain_info(2, 1, 400_000_000));
        let (rpc, _, _) = mining_rpc(info);
        let gate = TemplateBuildGate::new(&rpc).await;
        let request = rpc.get_block_template(None);
        tokio::pin!(request);
        assert!(
            futures::poll!(&mut request).is_pending(),
            "construction must wait for the build in progress"
        );
        gate.release().await;
        bounded(request).await.unwrap();
    });
}

#[test]
fn only_a_shielded_miner_reward_needs_a_proof_build() {
    use config::mining::{default_miner_address, MinerAddressType};
    use types::get_block_template::MinerParams;

    let net = network();
    let miner = |address_type| {
        MinerParams::new(
            &net,
            config::mining::Config {
                miner_address: Some(
                    default_miner_address(net.kind(), &address_type)
                        .parse()
                        .unwrap(),
                ),
                ..Default::default()
            },
        )
        .unwrap()
    };
    let transparent = miner(MinerAddressType::Transparent);
    let unified = miner(MinerAddressType::Unified);

    assert_eq!(
        TemplateBuildWork::for_coinbase(&net, Height(2), &transparent),
        TemplateBuildWork::ProofFree
    );
    assert_eq!(
        TemplateBuildWork::for_coinbase(&net, Height(2), &unified),
        TemplateBuildWork::Proof
    );
    // Before shielded rewards are payable, a unified address is paid at its transparent
    // receiver.
    assert_eq!(
        TemplateBuildWork::for_coinbase(&net, Height(0), &unified),
        TemplateBuildWork::ProofFree
    );
}

#[test]
fn precomputed_template_does_not_wait_for_proof_capacity() {
    mining_runtime(async {
        let chain = chain_info(1, 1, 0);
        let (_, info) = watch::channel(chain.clone());
        let (rpc, _, _) = mining_rpc(info);
        let params = rpc.gbt.miner_params().unwrap();
        let coinbase =
            TransactionTemplate::new_coinbase(&network(), Height(2), params, Amount::zero(), None)
                .unwrap();
        let expected_coinbase = coinbase.data.clone();
        let gate = BlockingPoolGate::new().await;
        let handler = rpc.gbt.clone();
        let mut proof = Box::pin(handler.run_template_build(TemplateBuildWork::Proof, || ()));
        assert!(futures::poll!(&mut proof).is_pending());

        let mut template = Box::pin(rpc.build_mining_template(
            Some(coinbase),
            params,
            &chain,
            template_id(&chain),
            vec![],
            Some(false),
        ));
        let Poll::Ready(Ok((template, _))) = futures::poll!(&mut template) else {
            panic!("a precomputed coinbase must not wait behind proof construction");
        };
        assert_eq!(template.coinbase_txn.data, expected_coinbase);
        assert_eq!(template.previous_block_hash, chain.tip_hash);
        gate.release().await;
        bounded(proof).await.unwrap();
    });
}

/// Every path that builds a template waits for the one build slot, and a caller that stops
/// waiting leaves the slot free for the next build.
#[test]
fn template_build_paths_wait_for_the_build_slot() {
    mining_runtime(async {
        for path in [
            "normal",
            "precompute",
            "tip change",
            "recovery",
            "shared restoration",
        ] {
            let old_height = if path == "precompute" { 0 } else { 2 };
            let chain = chain_info(old_height, 1, 0);
            let (info_tx, info) = watch::channel(chain.clone());
            let (rpc, tip, _) = mining_rpc(info);
            let initial = bounded(rpc.get_block_template(None))
                .await
                .unwrap()
                .try_into_template()
                .unwrap();
            let gate = TemplateBuildGate::new(&rpc).await;

            let mut request = match path {
                "normal" => rpc.get_block_template(None).boxed(),
                "shared restoration" => {
                    // Only validated work is restored once its registration is gone.
                    rpc.gbt.template_rejections.send_modify(|state| {
                        state.mark_prepared(chain.tip_hash, initial.work_id());
                    });
                    let shared = ReadyTemplate {
                        template: Arc::new(initial.clone()),
                        json: Arc::default(),
                    };
                    rpc.reuse_shared_template(shared, None)
                        .map(|result| {
                            result.map(|template| {
                                template.expect("shared work is still on the current parent")
                            })
                        })
                        .boxed()
                }
                "recovery" => {
                    rpc.gbt.template_rejections.send_modify(|state| {
                        state.reject(chain.tip_hash, "rejected-work");
                    });
                    rpc.finish_mining_template(initial, &chain, rpc.gbt.miner_params().unwrap())
                        .map(|result| {
                            result.map(|template| {
                                template.expect(
                                    "recovery parent is unchanged, so finish returns a template",
                                )
                            })
                        })
                        .boxed()
                }
                _ => rpc
                    .get_block_template(Some(GetBlockTemplateParameters {
                        long_poll_id: Some(initial.long_poll_id),
                        ..Default::default()
                    }))
                    .boxed(),
            };
            assert!(futures::poll!(&mut request).is_pending(), "{path}");
            if path == "tip change" {
                let next = chain_info(3, 2, 400_000_000);
                info_tx.send_replace(next.clone());
                tip.send_best_tip_height(next.tip_height);
                tip.send_best_tip_hash(next.tip_hash);
                assert!(futures::poll!(&mut request).is_pending());
            }
            // The build is waiting for the slot. Its caller disconnects.
            drop(request);
            gate.release().await;
            bounded(rpc.gbt.run_template_build(TemplateBuildWork::Proof, || ()))
                .await
                .unwrap();
        }
    });
}

#[test]
fn running_template_build_retains_capacity_when_cancelled() {
    mining_runtime(async {
        let (_, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, _, _) = mining_rpc(info);
        let handler = rpc.gbt.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let build = tokio::spawn(async move {
            handler
                .run_template_build(TemplateBuildWork::Proof, move || {
                    entered.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(10)).unwrap();
                })
                .await
        });
        bounded(started).await.unwrap();
        build.abort();
        assert!(bounded(build).await.unwrap_err().is_cancelled());

        // A cancelled waiter must be removed before any blocking job is spawned for it.
        let (ran, cancelled) = tokio::sync::oneshot::channel();
        let mut waiter = Box::pin(
            rpc.gbt
                .run_template_build(TemplateBuildWork::Proof, move || ran.send(())),
        );
        assert!(futures::poll!(&mut waiter).is_pending());
        drop(waiter);
        assert!(bounded(cancelled).await.is_err());

        tokio::time::pause();
        let mut contender = Box::pin(rpc.gbt.run_template_build(TemplateBuildWork::Proof, || ()));
        assert!(futures::poll!(&mut contender).is_pending());
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(futures::poll!(&mut contender).is_pending());
        tokio::time::advance(Duration::from_secs(2)).await;
        let Poll::Ready(Err(error)) = futures::poll!(&mut contender) else {
            panic!("a running build must retain capacity after its caller disconnects");
        };
        assert_eq!(error.code(), -1);
        assert!(error.message().contains("construction capacity"));
        release.send(()).unwrap();
        bounded(rpc.gbt.run_template_build(TemplateBuildWork::Proof, || ()))
            .await
            .unwrap();
    });
}

fn template_id(info: &GetBlockTemplateChainInfo) -> types::long_poll::LongPollId {
    types::long_poll::LongPollInput::new(info.tip_height, info.tip_hash, info.max_time, vec![])
        .generate_id()
}

#[tokio::test]
async fn template_worker_panic_is_an_rpc_error() {
    let (_, info) = watch::channel(chain_info(2, 1, 0));
    let (rpc, _, _) = mining_rpc(info);
    // The synchronous constructor requires a parent below Height::MAX. Exercise
    // its actual invariant panic rather than injecting a panic into production.
    let invalid_parent = chain_info(Height::MAX.0, 1, 0);
    let error = bounded(rpc.build_mining_template(
        None,
        rpc.gbt.miner_params().unwrap(),
        &invalid_parent,
        template_id(&invalid_parent),
        vec![],
        None,
    ))
    .await
    .unwrap_err();
    assert_eq!(error.code(), -1);
    assert!(error
        .message()
        .contains("chain tip must be below Height::MAX"));
    // A worker panic must also release construction capacity.
    bounded(rpc.get_block_template(None)).await.unwrap();
}

/// A template superseded during construction is rebuilt on the new tip, not returned
/// and not reported as an error. The tip change here is an equal-height reorg: same
/// height, different hash.
#[test]
fn tip_change_during_construction_rebuilds_on_new_parent() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, _) = mining_rpc(info);
        let gate = TemplateBuildGate::new(&rpc).await;
        let request = rpc.get_block_template(None);
        tokio::pin!(request);
        assert!(futures::poll!(&mut request).is_pending());

        let next = chain_info(2, 2, 0);
        info_tx.send_replace(next.clone());
        tip.send_best_tip_hash(next.tip_hash);
        gate.release().await;

        let template = bounded(request).await.unwrap().try_into_template().unwrap();
        assert_eq!(template.previous_block_hash, next.tip_hash);
        assert_eq!(template.height, 3);
        assert_eq!(
            rpc.gbt.template_rejections.borrow().parent,
            Some(next.tip_hash)
        );
    });
}

/// Pathological churn is bounded: after the configured number of rebuilds, the
/// RPC preserves the previous transient error instead of rebuilding forever.
#[tokio::test]
async fn template_rebuild_limit_returns_parent_changed_error() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    let initial = chain_info(2, 1, 0);
    let (info_tx, info) = watch::channel(initial.clone());
    let (tip, tip_sender) = MockChainTip::new();
    tip_sender.send_best_tip_height(initial.tip_height);
    tip_sender.send_best_tip_hash(initial.tip_hash);
    let tip_sender = Arc::new(tip_sender);
    let attempts = Arc::new(AtomicUsize::new(0));

    let mempool_info = info.clone();
    let mempool_info_tx = info_tx.clone();
    let mempool_tip_sender = tip_sender.clone();
    let mempool_attempts = attempts.clone();
    let mempool = tower::service_fn(move |_| {
        let current_tip = mempool_info.borrow().tip_hash;
        let attempt = mempool_attempts.fetch_add(1, Ordering::SeqCst);
        let next_hash =
            u8::try_from(attempt + 2).expect("test rebuild count fits in a block hash byte");
        let next = chain_info(2, next_hash, 0);
        mempool_info_tx.send_replace(next.clone());
        mempool_tip_sender.send_best_tip_hash(next.tip_hash);
        async move {
            Ok::<_, BoxError>(mempool::Response::FullTransactions {
                transactions: vec![].into(),
                transaction_dependencies: Default::default(),
                last_seen_tip_hash: current_tip,
            })
        }
    });
    let read_info = info.clone();
    let read = tower::service_fn(move |request| {
        assert!(matches!(request, ReadRequest::ChainInfo));
        let info = read_info.borrow().clone();
        async move { Ok::<_, BoxError>(ReadResponse::ChainInfo(info)) }
    });
    let (rpc, _) = rpc(network(), mempool, read, tip);

    let error = bounded(rpc.get_block_template(None)).await.unwrap_err();
    assert_eq!(error.code(), 0);
    assert_eq!(error.message(), "template parent changed; retry");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        MAX_TEMPLATE_REBUILDS + 1,
        "the initial build plus every permitted rebuild must be attempted"
    );
}

/// Two callers race across a tip change. The slow caller fetched chain info for the old
/// parent; the fast caller selects the new parent while the slow one is still building.
/// Both must return work on the new parent, and the slow caller must not clobber the
/// parent the fast caller selected.
#[test]
fn concurrent_caller_selecting_new_parent_makes_slow_caller_rebuild() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mut verifier) = mining_rpc(info);
        let gate = TemplateBuildGate::new(&rpc).await;

        let slow = rpc.get_block_template(None);
        tokio::pin!(slow);
        assert!(futures::poll!(&mut slow).is_pending());

        let next = chain_info(2, 2, 0);
        info_tx.send_replace(next.clone());
        tip.send_best_tip_hash(next.tip_hash);

        let fast = rpc.get_block_template(None);
        tokio::pin!(fast);
        assert!(futures::poll!(&mut fast).is_pending());
        assert_eq!(
            rpc.gbt.template_rejections.borrow().parent,
            Some(next.tip_hash),
            "the fast caller selects the new parent before building"
        );
        rpc.gbt.template_rejections.send_modify(|state| {
            assert!(state.reject(next.tip_hash, "rejected-work"));
        });

        gate.release().await;
        let (slow, fast, ()) = bounded(async {
            tokio::join!(slow, fast, async {
                for _ in 0..2 {
                    verifier
                        .expect_request_that(|req| {
                            matches!(req, zakura_consensus::Request::Prepare { .. })
                        })
                        .await
                        .respond(Hash([9; 32]));
                }
            })
        })
        .await;
        for response in [slow, fast] {
            let template = response.unwrap().try_into_template().unwrap();
            assert_eq!(template.previous_block_hash, next.tip_hash);
        }
        assert_eq!(
            rpc.gbt.template_rejections.borrow().parent,
            Some(next.tip_hash),
            "the slow caller's rebuild must not reset the parent"
        );
        assert!(
            rpc.gbt
                .template_rejections
                .borrow()
                .contains("rejected-work"),
            "the slow caller must not clear the new parent's rejection records"
        );
    });
}

/// A caller whose chain info is already behind the tip watch cannot select its stale
/// parent, even if the parent was selected earlier by someone else.
#[tokio::test]
async fn stale_parent_selection_is_refused() {
    let (_, info) = watch::channel(chain_info(2, 1, 0));
    let (rpc, tip, _) = mining_rpc(info);
    assert!(rpc
        .track_template_parent(Hash([1; 32]), &mut rpc.gbt.template_rejections.subscribe())
        .is_some());
    tip.send_best_tip_hash(Hash([2; 32]));
    assert!(!rpc
        .track_template_parent(Hash([1; 32]), &mut rpc.gbt.template_rejections.subscribe())
        .is_some());
    assert_eq!(
        rpc.gbt.template_rejections.borrow().parent,
        Some(Hash([1; 32])),
        "a refused selection leaves the state untouched"
    );
    assert!(rpc
        .track_template_parent(Hash([2; 32]), &mut rpc.gbt.template_rejections.subscribe())
        .is_some());
}

#[tokio::test]
async fn preactivation_template_accepts_negative_balance() {
    let (_, info) = watch::channel(chain_info(1, 1, -1));
    let (rpc, _, _) = mining_rpc(info);
    bounded(rpc.get_block_template(None)).await.unwrap();
}

fn assert_reward(template: &BlockTemplateResponse, balance: i64) {
    use zakura_chain::parameters::subsidy::halving_block_subsidy;
    let base = halving_block_subsidy(Height(template.height), &network()).unwrap();
    // Independent fixed NU7 payout oracle, including rounding up.
    let bonus = (i128::from(balance) * 1_375 + 9_999_999_999) / 10_000_000_000;
    let expected = i64::from(base)
        + if template.height >= 3 {
            i64::try_from(bonus).unwrap()
        } else {
            0
        };
    let coinbase: Transaction = template
        .coinbase_txn
        .data
        .as_ref()
        .zcash_deserialize_into()
        .unwrap();
    let paid: i64 = coinbase
        .outputs()
        .iter()
        .map(|output| i64::from(output.value()))
        .sum();
    assert_eq!(paid, expected);
}

#[test]
fn long_poll_builds_on_new_parent_without_blocking_runtime() {
    mining_runtime(async {
        for (old_height, new_height) in [(0, 1), (0, 2), (2, 3), (1, 2), (2, 5), (4, 2)] {
            let (info_tx, info) = watch::channel(chain_info(old_height, 1, 0));
            let (rpc, tip, _) = mining_rpc(info);
            let initial = bounded(rpc.get_block_template(None))
                .await
                .unwrap()
                .try_into_template()
                .unwrap();
            let request = rpc.get_block_template(Some(GetBlockTemplateParameters {
                long_poll_id: Some(initial.long_poll_id),
                ..Default::default()
            }));
            tokio::pin!(request);
            assert!(
                futures::poll!(&mut request).is_pending(),
                "matching long poll must wait"
            );
            let gate = TemplateBuildGate::new(&rpc).await;
            let next = chain_info(new_height, 2, 400_000_000);
            info_tx.send_replace(next.clone());
            tip.send_best_tip_height(next.tip_height);
            tip.send_best_tip_hash(next.tip_hash);
            let response = match futures::poll!(&mut request) {
                Poll::Ready(response) if (old_height, new_height) == (0, 1) => Some(response),
                Poll::Pending if (old_height, new_height) != (0, 1) => None,
                _ => panic!("only a matching precomputed coinbase can bypass the blocking pool"),
            };
            gate.release().await;
            let response = match response {
                Some(response) => response,
                None => bounded(request).await,
            };
            let template = response.unwrap().try_into_template().unwrap();
            assert_eq!(template.previous_block_hash, next.tip_hash);
            assert_eq!(template.height, new_height + 1);
            assert_reward(&template, 400_000_000);
        }
    });
}

/// Testnet long polling fetches fresh state when minimum difficulty becomes valid, before the
/// next mempool poll, and an already expired time range doesn't cause a refresh loop.
#[test]
fn long_poll_refreshes_testnet_difficulty_after_time_limit() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(300_000, 1, 0));
        let (rpc, _tip, read_count, _) = counting_mining_rpc(info);
        bounded(rpc.get_block_template(None)).await.unwrap();
        let gate = TemplateBuildGate::new(&rpc).await;
        tokio::time::pause();

        // A timestamp clamped at a past consensus ceiling must not cause a
        // one-second refresh loop after that ceiling has expired.
        let mut expired = chain_info(300_000, 1, 0);
        expired.cur_time = expired.max_time;
        expired.expected_difficulty = network().target_difficulty_limit().to_compact();
        info_tx.send_replace(expired.clone());
        {
            let request = rpc.get_block_template(Some(GetBlockTemplateParameters {
                long_poll_id: Some(template_id(&expired)),
                ..Default::default()
            }));
            tokio::pin!(request);
            assert!(futures::poll!(&mut request).is_pending());
            let reads = read_count.load(Ordering::SeqCst);
            tokio::time::advance(Duration::from_secs(2)).await;
            assert!(futures::poll!(&mut request).is_pending());
            assert_eq!(read_count.load(Ordering::SeqCst), reads);
        }

        // Start within one mempool polling interval of the difficulty timeout,
        // so a fresh state read proves the time-limit timer woke this request.
        let clock = DateTime32::now();
        let mut standard = chain_info(300_000, 1, 0);
        standard.cur_time = clock;
        standard.max_time = clock.checked_add(Duration32::from_seconds(3)).unwrap();
        let gap: Duration32 = NetworkUpgrade::minimum_difficulty_spacing_for_height(
            &network(),
            standard.tip_height.next().unwrap(),
        )
        .unwrap()
        .try_into()
        .unwrap();
        let spacing: Duration32 = NetworkUpgrade::target_spacing_for_height(
            &network(),
            standard.tip_height.next().unwrap(),
        )
        .try_into()
        .unwrap();
        let parent_time = standard.max_time.checked_sub(gap).unwrap();
        let median_time = parent_time
            .checked_sub(Duration32::from_seconds(5 * spacing.seconds()))
            .unwrap();
        standard.min_time = median_time
            .checked_add(Duration32::from_seconds(1))
            .unwrap();
        standard.expected_difficulty = (network().target_difficulty_limit() / 4_u32).to_compact();
        let old_id = template_id(&standard);
        info_tx.send_replace(standard.clone());
        let request = rpc.get_block_template(Some(GetBlockTemplateParameters {
            long_poll_id: Some(old_id),
            ..Default::default()
        }));
        tokio::pin!(request);
        assert!(futures::poll!(&mut request).is_pending());
        let reads_before_expiry = read_count.load(Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(futures::poll!(&mut request).is_pending());
        assert_eq!(read_count.load(Ordering::SeqCst), reads_before_expiry);

        let mut minimum = standard.clone();
        minimum.cur_time = standard
            .max_time
            .checked_add(Duration32::from_seconds(1))
            .unwrap();
        minimum.min_time = minimum.cur_time;
        minimum.max_time = median_time
            .checked_add(Duration32::from_minutes(90))
            .unwrap();
        minimum.expected_difficulty = network().target_difficulty_limit().to_compact();
        info_tx.send_replace(minimum.clone());
        tokio::time::advance(Duration::from_secs(3) + Duration::from_millis(1)).await;
        assert!(futures::poll!(&mut request).is_pending());
        assert!(
            read_count.load(Ordering::SeqCst) > reads_before_expiry,
            "the time-limit timer must fetch fresh state before the mempool timer"
        );
        tokio::time::resume();
        gate.release().await;
        let template = bounded(request).await.unwrap().try_into_template().unwrap();
        assert_eq!(template.previous_block_hash, standard.tip_hash);
        assert_eq!(template.cur_time, minimum.cur_time);
        assert!(template.cur_time > standard.max_time);
        assert_ne!(template.bits, standard.expected_difficulty);
        assert_eq!(template.bits, minimum.expected_difficulty);
        assert_ne!(template.long_poll_id, old_id);
        assert_eq!(template.submit_old, Some(false));
    });
}

/// A testnet refresh deadline that passes during a slow mempool fetch refreshes the template as
/// soon as the fetch returns, instead of waiting for the next mempool poll.
#[test]
fn long_poll_refresh_deadline_survives_slow_mempool_fetch() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(300_000, 1, 0));
        let (rpc, _tip, read_count, slow_mempool) = counting_mining_rpc(info);
        bounded(rpc.get_block_template(None)).await.unwrap();
        let gate = TemplateBuildGate::new(&rpc).await;
        tokio::time::pause();

        // Minimum difficulty becomes valid a few seconds from now, before the slow
        // mempool fetch below finishes.
        let clock = DateTime32::now();
        let mut standard = chain_info(300_000, 1, 0);
        standard.cur_time = clock;
        standard.max_time = clock.checked_add(Duration32::from_seconds(3)).unwrap();
        let old_id = template_id(&standard);
        info_tx.send_replace(standard.clone());
        let mut minimum = standard.clone();
        minimum.cur_time = standard
            .max_time
            .checked_add(Duration32::from_seconds(1))
            .unwrap();
        minimum.min_time = minimum.cur_time;
        minimum.max_time = minimum
            .cur_time
            .checked_add(Duration32::from_minutes(90))
            .unwrap();
        minimum.expected_difficulty = network().target_difficulty_limit().to_compact();

        slow_mempool.store(true, Ordering::SeqCst);
        let request = rpc.get_block_template(Some(GetBlockTemplateParameters {
            long_poll_id: Some(old_id),
            ..Default::default()
        }));
        tokio::pin!(request);
        assert!(futures::poll!(&mut request).is_pending());
        let reads_before_expiry = read_count.load(Ordering::SeqCst);
        info_tx.send_replace(minimum.clone());
        tokio::time::advance(
            Duration::from_secs(MEMPOOL_LONG_POLL_INTERVAL) + Duration::from_millis(1),
        )
        .await;
        assert!(futures::poll!(&mut request).is_pending());
        assert!(
            read_count.load(Ordering::SeqCst) > reads_before_expiry,
            "a deadline that passed during the mempool fetch must refresh right away"
        );

        tokio::time::resume();
        gate.release().await;
        let template = bounded(request).await.unwrap().try_into_template().unwrap();
        assert_eq!(template.cur_time, minimum.cur_time);
        assert_eq!(template.bits, minimum.expected_difficulty);
        assert_ne!(template.long_poll_id, old_id);
        assert_eq!(template.submit_old, Some(false));
    });
}

/// Mainnet starts its relative timeout after fetching the clock sample from state.
#[test]
fn long_poll_mainnet_deadline_survives_slow_state_fetch() {
    mining_runtime(async {
        let clock = DateTime32::now();
        let mut original = chain_info(3_000_000, 1, 0);
        original.cur_time = clock;
        original.max_time = clock.checked_add(Duration32::from_seconds(10)).unwrap();
        original.min_time = original
            .max_time
            .checked_sub(Duration32::from_seconds(90 * 60 - 1))
            .unwrap();
        let (info_tx, info) = watch::channel(original.clone());
        let (tip, sender) = MockChainTip::new();
        sender.send_best_tip_height(original.tip_height);
        sender.send_best_tip_hash(original.tip_hash);
        sender.send_estimated_distance_to_network_chain_tip(0);
        sender.send_best_tip_block_time(chrono::Utc::now());
        let read_count = Arc::new(AtomicUsize::new(0));
        let reads = read_count.clone();
        let slow_read = Arc::new(AtomicBool::new(false));
        let slow = slow_read.clone();
        let read = tower::service_fn(move |request| {
            assert!(matches!(request, ReadRequest::ChainInfo));
            reads.fetch_add(1, Ordering::SeqCst);
            let slow = slow.swap(false, Ordering::SeqCst);
            let info = info.clone();
            async move {
                if slow {
                    tokio::time::sleep(Duration::from_secs(6)).await;
                }
                // State samples the clock after its potentially slow database reads.
                Ok::<_, BoxError>(ReadResponse::ChainInfo(info.borrow().clone()))
            }
        });
        let mempool = tower::service_fn(move |_| async move {
            Ok::<_, BoxError>(mempool::Response::FullTransactions {
                transactions: vec![].into(),
                transaction_dependencies: Default::default(),
                last_seen_tip_hash: original.tip_hash,
            })
        });
        let (rpc, _) = rpc(Network::Mainnet, mempool, read, tip);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let gate = TemplateBuildGate::new(&rpc).await;
        tokio::time::pause();
        slow_read.store(true, Ordering::SeqCst);

        let request = rpc.get_block_template(Some(GetBlockTemplateParameters {
            long_poll_id: Some(initial.long_poll_id),
            ..Default::default()
        }));
        tokio::pin!(request);
        assert!(futures::poll!(&mut request).is_pending());
        let reads_before_deadline = read_count.load(Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(6) + Duration::from_millis(1)).await;
        let mut sampled = info_tx.borrow().clone();
        sampled.cur_time = clock.checked_add(Duration32::from_seconds(6)).unwrap();
        info_tx.send_replace(sampled.clone());
        assert!(futures::poll!(&mut request).is_pending());

        // Four seconds remain after the fetch. Starting that wait before the fetch
        // would already have expired it and forced a premature replacement.
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(futures::poll!(&mut request).is_pending());
        assert_eq!(read_count.load(Ordering::SeqCst), reads_before_deadline);

        sampled.cur_time = sampled.max_time;
        info_tx.send_replace(sampled);
        tokio::time::advance(Duration::from_secs(1) + Duration::from_millis(1)).await;
        assert!(futures::poll!(&mut request).is_pending());
        assert!(read_count.load(Ordering::SeqCst) > reads_before_deadline);
        tokio::time::resume();
        gate.release().await;
        let template = bounded(request).await.unwrap().try_into_template().unwrap();
        assert_eq!(template.long_poll_id, initial.long_poll_id);
        assert_eq!(template.cur_time, initial.max_time);
        assert_eq!(template.submit_old, Some(false));
    });
}

/// A rebuild must remember that the long-poll deadline already woke the request.
/// Otherwise, restoring the original tip and long-poll ID can make it wait forever
/// because a clamped current time disables the deadline timer.
#[test]
fn superseded_deadline_build_does_not_resume_long_polling() {
    mining_runtime(async {
        let mut original = chain_info(2, 1, 0);
        original.cur_time = 1654008727.into();
        let deadline = original
            .max_time
            .saturating_duration_since(original.cur_time)
            .to_std();
        let (info_tx, info) = watch::channel(original.clone());
        let (rpc, tip, _) = mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let gate = TemplateBuildGate::new(&rpc).await;
        tokio::time::pause();

        let request = rpc.get_block_template(Some(GetBlockTemplateParameters {
            long_poll_id: Some(initial.long_poll_id),
            ..Default::default()
        }));
        tokio::pin!(request);
        assert!(futures::poll!(&mut request).is_pending());
        tokio::time::advance(deadline + Duration::from_millis(1)).await;
        assert!(
            futures::poll!(&mut request).is_pending(),
            "the deadline build must wait for blocking-pool capacity"
        );

        let other = chain_info(2, 2, 0);
        info_tx.send_replace(other.clone());
        tip.send_best_tip_hash(other.tip_hash);
        assert!(rpc
            .track_template_parent(other.tip_hash, &mut rpc.gbt.template_rejections.subscribe())
            .is_some());

        let mut restored = original;
        restored.cur_time = restored.max_time;
        info_tx.send_replace(restored.clone());
        tip.send_best_tip_hash(restored.tip_hash);

        gate.release().await;
        tokio::time::resume();
        let template = bounded(request).await.unwrap().try_into_template().unwrap();
        assert_eq!(template.previous_block_hash, restored.tip_hash);
        assert_eq!(template.submit_old, Some(false));
    });
}

#[test]
fn simultaneous_long_polls_remain_responsive() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, _) = mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let mut requests: Vec<_> = (0..4)
            .map(|_| {
                Box::pin(rpc.get_block_template(Some(GetBlockTemplateParameters {
                    long_poll_id: Some(initial.long_poll_id),
                    ..Default::default()
                })))
            })
            .collect();
        for request in &mut requests {
            assert!(futures::poll!(request).is_pending());
        }
        let gate = TemplateBuildGate::new(&rpc).await;
        let next = chain_info(3, 2, 400_000_000);
        info_tx.send_replace(next.clone());
        tip.send_best_tip_height(next.tip_height);
        tip.send_best_tip_hash(next.tip_hash);
        for request in &mut requests {
            assert!(
                futures::poll!(request).is_pending(),
                "each awakened request must yield to the blocking pool"
            );
        }
        gate.release().await;
        let responses = bounded(futures::future::join_all(requests)).await;
        for response in responses {
            let template = response.unwrap().try_into_template().unwrap();
            assert_eq!(template.previous_block_hash, next.tip_hash);
            assert_reward(&template, 400_000_000);
        }
    });
}

/// Long polls answered together on a new tip must not each select from the mempool and queue
/// a full template build. Up to [`MAX_FULL_FIRST_ANSWERS`] of them build the full first
/// answer, and the rest answer coinbase-only.
///
/// Here the mempool has published its snapshot for the new block, so no long poll sends the
/// mempool a request.
#[test]
fn simultaneous_long_polls_build_a_bounded_number_of_full_first_answers() {
    answer_simultaneous_long_polls_on_a_new_tip(MempoolSnapshotOnNewTip::Published);
}

/// While the mempool's snapshot is still behind the new block, every long poll answers
/// coinbase-only at once, and none sends the mempool a request.
#[test]
fn simultaneous_long_polls_do_not_ask_a_mempool_that_is_behind() {
    answer_simultaneous_long_polls_on_a_new_tip(MempoolSnapshotOnNewTip::Behind);
}

/// Whether the mempool has published its snapshot for the new block when long polls ask for
/// their first answer on it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum MempoolSnapshotOnNewTip {
    /// The snapshot is for the new block.
    Published,
    /// No snapshot is for the new block, so every first answer is coinbase-only.
    Behind,
}

/// Answers more long polls together on a new tip than may build a full first answer, and
/// checks how many answer with the full template ([`MAX_FULL_FIRST_ANSWERS`] when the
/// mempool's snapshot is for the new block, none otherwise), that the rest answer
/// coinbase-only, and that they send the mempool no request.
fn answer_simultaneous_long_polls_on_a_new_tip(snapshot: MempoolSnapshotOnNewTip) {
    const LONG_POLLS: usize = 8;

    mining_runtime(async move {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, mempool_requests) = populated_mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let old_tip_long_poll = || {
            Box::pin(rpc.get_block_template(Some(GetBlockTemplateParameters {
                long_poll_id: Some(initial.long_poll_id),
                ..Default::default()
            })))
        };

        // A block lands. The mempool has published its transactions for it only in the
        // `Published` case.
        let next = chain_info(3, 2, 400_000_000);
        if snapshot == MempoolSnapshotOnNewTip::Published {
            mempool_snapshots.publish(Some(mempool::MempoolSnapshot {
                last_seen_tip_hash: next.tip_hash,
                transactions: vec![fake_verified_mempool_tx()].into(),
                transaction_dependencies: Default::default(),
            }));
        }
        info_tx.send_replace(next.clone());
        tip.send_best_tip_height(next.tip_height);
        tip.send_best_tip_hash(next.tip_hash);

        // No template build can start while the build slot is held, so every long poll
        // that starts a full first answer is still working on it when the others arrive.
        let gate = TemplateBuildGate::new(&rpc).await;
        let requests_before = mempool_requests.load(Ordering::SeqCst);
        let mut requests: Vec<_> = (0..LONG_POLLS).map(|_| old_tip_long_poll()).collect();
        for request in &mut requests {
            assert!(futures::poll!(request).is_pending());
        }
        assert_eq!(
            mempool_requests.load(Ordering::SeqCst) - requests_before,
            0,
            "a first answer on a new tip never asks the mempool"
        );

        gate.release().await;
        let templates: Vec<_> = bounded(futures::future::join_all(requests))
            .await
            .into_iter()
            .map(|response| response.unwrap().try_into_template().unwrap())
            .collect();
        for template in &templates {
            assert_eq!(template.previous_block_hash, next.tip_hash);
        }
        let full_answers = templates
            .iter()
            .filter(|template| !template.transactions.is_empty())
            .count();
        if snapshot == MempoolSnapshotOnNewTip::Behind {
            assert_eq!(full_answers, 0, "no list for the new block is published");
            return;
        }
        assert_eq!(
            full_answers, MAX_FULL_FIRST_ANSWERS,
            "the long polls over the limit answer coinbase-only"
        );

        // The finished long polls no longer count towards the limit.
        let later = bounded(old_tip_long_poll())
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(later.previous_block_hash, next.tip_hash);
        assert!(
            !later.transactions.is_empty(),
            "a long poll answered after the others finished gets the full template"
        );
    });
}

/// Returns a mining RPC with one mempool transaction, on a tip that has just moved to `next`,
/// and the ID of a long poll made before the move.
///
/// Every long poll sent with that ID is answered at once with the full template on `next`.
async fn rpc_with_long_poll_id_from_old_tip(
    next: &GetBlockTemplateChainInfo,
) -> (TestRpcImpl, types::long_poll::LongPollId) {
    let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
    let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
    let initial = bounded(rpc.get_block_template(None))
        .await
        .unwrap()
        .try_into_template()
        .unwrap();
    move_populated_tip(&info_tx, &tip, &mempool_snapshots, next);
    // The read service and the tip keep answering for `next` after their senders drop.
    (rpc, initial.long_poll_id)
}

/// Moves the tip to `next`, with the mempool's snapshot for it already published.
fn move_populated_tip(
    info: &watch::Sender<GetBlockTemplateChainInfo>,
    tip: &zakura_chain::chain_tip::mock::MockChainTipSender,
    mempool_snapshots: &mempool::MempoolSnapshotSender,
    next: &GetBlockTemplateChainInfo,
) {
    mempool_snapshots.publish(Some(mempool::MempoolSnapshot {
        last_seen_tip_hash: next.tip_hash,
        transactions: vec![fake_verified_mempool_tx()].into(),
        transaction_dependencies: Default::default(),
    }));
    info.send_replace(next.clone());
    tip.send_best_tip_height(next.tip_height);
    tip.send_best_tip_hash(next.tip_hash);
}

fn long_poll(
    rpc: &TestRpcImpl,
    long_poll_id: types::long_poll::LongPollId,
) -> BoxFuture<'_, Result<GetBlockTemplateResponse>> {
    rpc.get_block_template(Some(GetBlockTemplateParameters {
        long_poll_id: Some(long_poll_id),
        ..Default::default()
    }))
    .boxed()
}

/// Long polls that ask for the same work get one template: the first of them builds it, the
/// ones that arrive during the build wait for it, and a later one takes the finished template.
#[test]
fn long_polls_asking_for_the_same_work_share_one_build() {
    mining_runtime(async {
        let next = chain_info(3, 2, 400_000_000);
        let (rpc, old_tip_id) = rpc_with_long_poll_id_from_old_tip(&next).await;

        let gate = TemplateBuildGate::new(&rpc).await;
        let mut together: Vec<_> = (0..MAX_FULL_FIRST_ANSWERS)
            .map(|_| long_poll(&rpc, old_tip_id))
            .collect();
        for request in &mut together {
            assert!(futures::poll!(request).is_pending());
        }
        gate.release().await;

        let mut templates: Vec<_> = bounded(futures::future::join_all(together))
            .await
            .into_iter()
            .map(|response| response.unwrap().try_into_template().unwrap())
            .collect();
        templates.push(
            bounded(long_poll(&rpc, old_tip_id))
                .await
                .unwrap()
                .try_into_template()
                .unwrap(),
        );
        for template in &templates {
            assert_eq!(template.previous_block_hash, next.tip_hash);
            assert_eq!(template.transactions.len(), 1);
            assert_eq!(template.submit_old, Some(false));
            assert_eq!(
                template.work_id(),
                templates[0].work_id(),
                "every long poll must receive the one shared build"
            );
        }
    });
}

/// Serving shared work restores the block content needed by compact submissions after eviction.
#[test]
fn reused_shared_work_restores_evicted_compact_submission_content() {
    mining_runtime(async {
        let next = chain_info(3, 2, 400_000_000);
        let (rpc, old_tip_id) = rpc_with_long_poll_id_from_old_tip(&next).await;
        let shared = bounded(long_poll(&rpc, old_tip_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let block = Arc::new(proposal_block_from_template(&shared, None, &rpc.network).unwrap());
        let candidates = rpc.gbt.prepared_candidates();
        candidates.insert(
            block.clone(),
            shared.work_id(),
            zakura_consensus::PreparedCandidateSource::ServerTemplate,
            &rpc.network,
        );
        // Background validation registers passing work and marks it validated.
        rpc.gbt.template_rejections.send_modify(|state| {
            state.mark_prepared(next.tip_hash, shared.work_id());
        });
        assert!(candidates.resolve(shared.work_id(), *block.header).is_ok());

        // Fill the real bounded resolver with distinct candidates, evicting the shared answer.
        for index in 3..=40 {
            let mut other = (*block).clone();
            Arc::make_mut(&mut other.header).previous_block_hash = Hash([index; 32]);
            candidates.insert(
                Arc::new(other),
                &format!("other-work-{index}"),
                zakura_consensus::PreparedCandidateSource::ServerTemplate,
                &rpc.network,
            );
        }
        assert_eq!(
            candidates.resolve(shared.work_id(), *block.header),
            Err(ResolvePreparedCandidateError::StaleWork)
        );

        let reused = bounded(long_poll(&rpc, old_tip_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(reused.work_id(), shared.work_id());
        let restored = candidates
            .resolve(reused.work_id(), *block.header)
            .expect("freshly served shared work must reconstruct a compact submission");
        assert_eq!(restored.transactions, block.transactions);
        assert_eq!(restored.header, block.header);
    });
}

/// Shared work that has not been validated yet is served as it is, like the builder's own answer:
/// reusing it neither waits for the build slot nor registers it for compact submissions. Its
/// background validation registers it if it passes.
#[test]
fn reused_unvalidated_work_is_served_without_registering_it() {
    mining_runtime(async {
        let next = chain_info(3, 2, 400_000_000);
        let (rpc, old_tip_id) = rpc_with_long_poll_id_from_old_tip(&next).await;
        let shared = bounded(long_poll(&rpc, old_tip_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let header = *proposal_block_from_template(&shared, None, &rpc.network)
            .unwrap()
            .header;
        assert!(!rpc.mining_template_prepared(shared.work_id()));

        let gate = TemplateBuildGate::new(&rpc).await;
        let reused = bounded(long_poll(&rpc, old_tip_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        gate.release().await;

        assert_eq!(reused.work_id(), shared.work_id());
        assert_eq!(
            rpc.gbt
                .prepared_candidates()
                .resolve(shared.work_id(), header),
            Err(ResolvePreparedCandidateError::StaleWork)
        );
    });
}

/// A template taken from the shared store is written into its response from JSON that is
/// encoded once. The response must be the bytes that encoding the template afresh gives, for
/// both values of `submit_old`, and outside a response the template serializes as its fields.
#[test]
fn shared_template_answer_is_the_bytes_of_a_fresh_encoding() {
    use crate::server::jsonrpc::{Id, MethodResponse};

    mining_runtime(async {
        let next = chain_info(3, 2, 400_000_000);
        let (rpc, old_tip_id) = rpc_with_long_poll_id_from_old_tip(&next).await;
        // The same tip with no mempool transactions: only the mempool changed since.
        let same_tip_id =
            LongPollInput::new(next.tip_height, next.tip_hash, next.max_time, vec![]).generate_id();
        let built = bounded(long_poll(&rpc, old_tip_id)).await.unwrap();

        for (long_poll_id, submit_old) in [(old_tip_id, false), (same_tip_id, true)] {
            // Asked twice, so the second answer is written from JSON the first one encoded.
            for _ in 0..2 {
                let shared = bounded(long_poll(&rpc, long_poll_id)).await.unwrap();
                let mut afresh = shared.clone().try_into_template().unwrap();
                assert_eq!(afresh.submit_old, Some(submit_old));
                assert_eq!(
                    afresh.work_id(),
                    built.clone().try_into_template().unwrap().work_id()
                );
                afresh.shared_json = Default::default();
                let afresh = GetBlockTemplateResponse::from(afresh);

                let response = |template: &GetBlockTemplateResponse, limit| {
                    MethodResponse::response(Id::Number(1), &Ok(template), limit)
                };
                assert_eq!(
                    response(&shared, usize::MAX).as_json().get(),
                    response(&afresh, usize::MAX).as_json().get()
                );
                assert_eq!(
                    serde_json::to_value(&shared).unwrap(),
                    serde_json::to_value(&afresh).unwrap()
                );
                // The shared JSON counts toward the response size limit like any other.
                assert!(response(&shared, 200).is_error());
            }
        }
    });
}

/// A request without a long poll ID asks for a template now. It takes a stored template only
/// when that is the template it would build, and it never stores the one it builds.
#[test]
fn request_without_a_long_poll_id_takes_only_the_template_it_would_build() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let next = chain_info(3, 2, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        let plain = || async {
            bounded(rpc.get_block_template(None))
                .await
                .unwrap()
                .try_into_template()
                .unwrap()
        };

        // Nothing is stored yet, and this request's own build is not stored either.
        let own = plain().await;
        let shared = bounded(long_poll(&rpc, initial.long_poll_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(own.long_poll_id, shared.long_poll_id);
        assert_ne!(own.work_id(), shared.work_id());

        // The stored template is what a request in the same second would build.
        let taken = plain().await;
        assert_eq!(taken.work_id(), shared.work_id());
        assert_eq!(taken.submit_old, None);

        // A second later the same work has a newer time, so the request builds it.
        info_tx.send_modify(|info| info.cur_time = 1654008618.into());
        let later = plain().await;
        assert_eq!(later.long_poll_id, shared.long_poll_id);
        assert_eq!(later.cur_time, 1654008618.into());
        assert_ne!(later.work_id(), shared.work_id());
    });
}

/// The ID of the template on the tip `info` names, for the one transaction that
/// [`populated_mining_rpc`]'s mempool holds.
fn populated_template_id(info: &GetBlockTemplateChainInfo) -> types::long_poll::LongPollId {
    types::long_poll::LongPollInput::new(
        info.tip_height,
        info.tip_hash,
        info.max_time,
        [fake_verified_mempool_tx().transaction.id()],
    )
    .generate_id()
}

/// Returns the template stored for the tip `info` names, once the task that builds templates
/// ahead of requests has stored it and encoded its answers.
async fn template_built_ahead(
    rpc: &TestRpcImpl,
    info: &GetBlockTemplateChainInfo,
) -> BlockTemplateResponse {
    let id = populated_template_id(info);
    let stored = bounded(async {
        loop {
            if let Some(stored) = rpc.gbt.shared_template.ready(&id) {
                break stored;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    // The task encodes on the one blocking thread, so this job runs after its encoding.
    bounded(tokio::task::spawn_blocking(|| ())).await.unwrap();
    BlockTemplateResponse::clone(&stored.template)
}

/// Lets the tasks that are ready to run do so, for a test that then checks nothing happened.
async fn let_other_tasks_run() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

/// On each new tip the node builds the template without being asked. Requests that arrive
/// after it, with or without a long poll ID, are answered from that template: they complete
/// while the build slot is held, so they did not build.
#[test]
fn requests_after_a_new_tip_take_the_template_built_ahead() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        tokio::spawn(rpc.clone().run_template_builder());

        let mut old_tip_id = initial.long_poll_id;
        for next in [chain_info(3, 2, 400_000_000), chain_info(4, 3, 400_000_000)] {
            move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
            let built_ahead = template_built_ahead(&rpc, &next).await;
            assert_eq!(built_ahead.previous_block_hash, next.tip_hash);
            assert_eq!(built_ahead.transactions.len(), 1);

            let gate = TemplateBuildGate::new(&rpc).await;
            let plain = bounded(rpc.get_block_template(None))
                .await
                .unwrap()
                .try_into_template()
                .unwrap();
            let polled = bounded(long_poll(&rpc, old_tip_id))
                .await
                .unwrap()
                .try_into_template()
                .unwrap();
            gate.release().await;

            assert_eq!(plain.work_id(), built_ahead.work_id());
            assert_eq!(plain.submit_old, None);
            assert_eq!(polled.work_id(), built_ahead.work_id());
            assert_eq!(polled.submit_old, Some(false));
            old_tip_id = polled.long_poll_id;
        }
    });
}

/// A long poll that is waiting when the tip changes builds the template and stores it. The task
/// that builds ahead leaves the tip to it, so the stored template is the long poll's.
#[test]
fn waiting_long_poll_builds_the_template_for_its_tip() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let mut waiting = long_poll(&rpc, initial.long_poll_id);
        assert!(futures::poll!(&mut waiting).is_pending());

        let mut builder_rpc = rpc.clone();
        let read_state = builder_rpc.read_state.clone();
        let reads = Arc::new(AtomicUsize::new(0));
        let builder_reads = reads.clone();
        builder_rpc.read_state =
            tower::util::BoxCloneSyncService::new(tower::service_fn(move |request| {
                builder_reads.fetch_add(1, Ordering::SeqCst);
                read_state.clone().oneshot(request)
            }));
        // Keep the long poll from answering while the builder's backoff timer expires.
        // A competing builder would read state and claim this tip's shared template.
        let gate = TemplateBuildGate::new(&rpc).await;
        tokio::time::pause();
        let mut builder = Box::pin(builder_rpc.run_template_builder());
        assert!(futures::poll!(&mut builder).is_pending());
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        let next = chain_info(3, 2, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        tokio::time::advance(WAITING_LONG_POLL_CHECK_INTERVAL).await;
        assert!(futures::poll!(&mut builder).is_pending());
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "the background builder must not fetch state while a long poll waits"
        );
        assert!(rpc
            .gbt
            .shared_template
            .wait_for(next.tip_hash, &populated_template_id(&next))
            .now_or_never()
            .expect("no background build may hold the shared template")
            .is_none());

        tokio::time::resume();
        gate.release().await;
        let answered = bounded(waiting).await.unwrap().try_into_template().unwrap();
        let stored = template_built_ahead(&rpc, &next).await;

        assert_eq!(answered.previous_block_hash, next.tip_hash);
        assert_eq!(answered.transactions.len(), 1);
        assert_eq!(answered.work_id(), stored.work_id());
        assert!(!rpc.gbt.has_waiting_long_poll());
    });
}

/// A reorg cancels the background build's claim even while construction is waiting for
/// capacity. Once capacity returns, only work for the replacement parent is published.
#[test]
fn build_ahead_waiting_for_capacity_is_cancelled_on_a_reorg() {
    mining_runtime(async {
        let old = chain_info(3, 2, 400_000_000);
        let (info_tx, info) = watch::channel(old.clone());
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &old);
        let gate = TemplateBuildGate::new(&rpc).await;
        rpc.gbt.note_template_requested();
        let mut builder = Box::pin(rpc.clone().run_template_builder());

        assert!(futures::poll!(&mut builder).is_pending());
        let old_id = populated_template_id(&old);
        assert!(rpc
            .gbt
            .shared_template
            .wait_for(old.tip_hash, &old_id)
            .now_or_never()
            .is_none());

        // Replace the parent at the same height while its template owns the claim.
        let mut replacement = chain_info(3, 3, 400_000_000);
        // Repeated-byte hashes have the same long-poll checksum; give the replacement
        // a distinct checksum so looking up the old ID also distinguishes the two builds.
        replacement.tip_hash.0[0] ^= 1;
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &replacement);
        assert!(futures::poll!(&mut builder).is_pending());
        assert!(rpc
            .gbt
            .shared_template
            .wait_for(old.tip_hash, &old_id)
            .now_or_never()
            .expect("the reorg must release the old build's claim")
            .is_none());
        assert!(rpc
            .gbt
            .shared_template
            .wait_for(replacement.tip_hash, &populated_template_id(&replacement))
            .now_or_never()
            .is_none());

        let builder = tokio::spawn(builder);
        gate.release().await;
        let stored = template_built_ahead(&rpc, &replacement).await;
        assert_eq!(stored.previous_block_hash, replacement.tip_hash);
        assert!(rpc.gbt.shared_template.ready(&old_id).is_none());
        let answer = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(answer.previous_block_hash, replacement.tip_hash);
        assert_eq!(answer.work_id(), stored.work_id());
        builder.abort();
        assert!(builder.await.unwrap_err().is_cancelled());
    });
}

/// A rejection that arrives after the background build claims its template prevents
/// that work from being served, even if construction subsequently publishes it.
#[test]
fn build_ahead_rejected_while_waiting_for_capacity_is_not_served() {
    mining_runtime(async {
        let next = chain_info(3, 2, 400_000_000);
        let (info_tx, info) = watch::channel(next.clone());
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        let gate = TemplateBuildGate::new(&rpc).await;
        rpc.gbt.note_template_requested();
        let mut builder = Box::pin(rpc.clone().run_template_builder());
        assert!(futures::poll!(&mut builder).is_pending());
        let id = populated_template_id(&next);
        assert!(rpc
            .gbt
            .shared_template
            .wait_for(next.tip_hash, &id)
            .now_or_never()
            .is_none());

        rpc.gbt.template_rejections.send_modify(|state| {
            state.reject(next.tip_hash, "rejected-before-publication");
        });
        let revision = rpc.gbt.template_rejections.borrow().revision;
        assert_ne!(revision, id.revision);

        let builder = tokio::spawn(builder);
        gate.release().await;
        template_built_ahead(&rpc, &next).await;
        let shared = rpc.gbt.shared_template.ready(&id).unwrap();
        assert!(bounded(rpc.reuse_shared_template(shared, None))
            .await
            .unwrap()
            .is_none());
        let state = rpc.gbt.template_rejections.borrow();
        assert!(state.needs_fallback());
        assert_eq!(state.revision, revision);
        drop(state);
        builder.abort();
        assert!(builder.await.unwrap_err().is_cancelled());
    });
}

/// The task that builds ahead waits for the mempool to process the new block for as long as
/// that takes. A long poll that arrives meanwhile is not held for it: it is answered on the new
/// tip at once, and the template is built ahead once the mempool has processed the block.
#[test]
fn long_poll_is_not_held_while_the_build_ahead_waits_for_the_mempool() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _, _verifier) =
            populated_mining_rpc_with(info, MempoolTip::LastPublished);
        mempool_snapshots.publish(Some(mempool::MempoolSnapshot {
            last_seen_tip_hash: chain_info(2, 1, 0).tip_hash,
            transactions: vec![fake_verified_mempool_tx()].into(),
            transaction_dependencies: Default::default(),
        }));
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        tokio::spawn(rpc.clone().run_template_builder());

        // A block lands, and the mempool has not processed it yet.
        let next = chain_info(3, 2, 400_000_000);
        info_tx.send_replace(next.clone());
        tip.send_best_tip_height(next.tip_height);
        tip.send_best_tip_hash(next.tip_hash);
        let_other_tasks_run().await;
        let id = populated_template_id(&next);
        assert!(rpc.gbt.shared_template.ready(&id).is_none());

        let answered = bounded(long_poll(&rpc, initial.long_poll_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(answered.previous_block_hash, next.tip_hash);
        assert_eq!(answered.submit_old, Some(false));
        assert!(
            answered.transactions.is_empty(),
            "without the mempool's list the long poll answers coinbase-only"
        );

        // Once the mempool publishes its list, the template is built ahead.
        mempool_snapshots.publish(Some(mempool::MempoolSnapshot {
            last_seen_tip_hash: next.tip_hash,
            transactions: vec![fake_verified_mempool_tx()].into(),
            transaction_dependencies: Default::default(),
        }));
        let stored = template_built_ahead(&rpc, &next).await;
        assert_eq!(stored.previous_block_hash, next.tip_hash);
        assert_eq!(stored.transactions.len(), 1);
    });
}

/// Admitting a transaction withdraws the mempool's published list, and nothing publishes it
/// again until the next block or a request for it. The task that builds ahead then asks the
/// mempool, and a request after it takes the template it stored.
#[test]
fn build_ahead_asks_the_mempool_when_its_list_was_withdrawn() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        rpc.gbt.note_template_requested();
        tokio::spawn(rpc.clone().run_template_builder());

        // The mempool processes the new block, then admits a transaction.
        let next = chain_info(3, 2, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        mempool_snapshots.publish(None);

        let built_ahead = template_built_ahead(&rpc, &next).await;
        assert_eq!(built_ahead.previous_block_hash, next.tip_hash);
        assert_eq!(built_ahead.transactions.len(), 1);

        let plain = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(plain.work_id(), built_ahead.work_id());
    });
}

/// A node that has not been asked for a template builds none ahead, so a node with a miner
/// address that no miner uses does no mining work on each block. After the first request, the
/// next block's template is built ahead.
#[test]
fn nothing_is_built_ahead_until_a_template_is_requested() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, mempool_requests) = populated_mining_rpc(info);
        tokio::time::pause();
        tokio::spawn(rpc.clone().run_template_builder());

        let next = chain_info(3, 2, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        tokio::time::advance(WAITING_LONG_POLL_CHECK_INTERVAL * 3).await;
        let_other_tasks_run().await;
        assert!(rpc
            .gbt
            .shared_template
            .ready(&populated_template_id(&next))
            .is_none());
        assert_eq!(mempool_requests.load(Ordering::SeqCst), 0);
        tokio::time::resume();

        bounded(rpc.get_block_template(None)).await.unwrap();
        let after = chain_info(4, 3, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &after);
        let built_ahead = template_built_ahead(&rpc, &after).await;
        assert_eq!(built_ahead.previous_block_hash, after.tip_hash);
    });
}

/// Background validation keeps only the newest waiting template, so a newer one can replace a
/// shared template's own request for it. A long poll that takes the shared template asks again,
/// so the template it is given is validated. Taking it again while it is being validated does
/// not queue a second validation.
#[test]
fn a_shared_template_whose_validation_was_replaced_is_validated_when_taken() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _, mut verifier) =
            populated_mining_rpc_with(info, MempoolTip::FollowsChain);
        let is_prepare = |request: &zakura_consensus::Request| {
            matches!(request, zakura_consensus::Request::Prepare { .. })
        };

        // The first template's validation holds the one validation slot.
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let initial_validation = verifier.expect_request_that(is_prepare).await;

        // The first long poll on the new tip builds and stores the shared template. A newer
        // template then replaces its waiting validation.
        let next = chain_info(3, 2, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        let shared = bounded(long_poll(&rpc, initial.long_poll_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let mut newer = initial.clone();
        newer.work_id = "newer-work".to_string();
        assert!(rpc.gbt.queue_template_preparation(newer).is_none());

        let taken = bounded(long_poll(&rpc, initial.long_poll_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(taken.work_id(), shared.work_id());

        initial_validation.respond(Hash([9; 32]));
        let shared_validation = verifier
            .expect_request_that(|request| {
                is_prepare(request) && request.block().header().previous_block_hash == next.tip_hash
            })
            .await;

        let taken_again = bounded(long_poll(&rpc, initial.long_poll_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(taken_again.work_id(), shared.work_id());
        shared_validation.respond(Hash([9; 32]));
        let_other_tasks_run().await;
        assert!(
            verifier.try_next_request().now_or_never().is_none(),
            "the template being validated is not queued again"
        );
        assert!(rpc
            .gbt
            .template_rejections
            .borrow()
            .is_prepared(shared.work_id()));
    });
}

/// Nothing is built ahead on a tip whose templates are being recovered after a rejection: only
/// validated fallback work may be handed out there.
#[test]
fn nothing_is_built_ahead_while_a_tip_recovers_from_a_rejection() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        rpc.gbt.note_template_requested();
        tokio::spawn(rpc.clone().run_template_builder());

        let next = chain_info(3, 2, 400_000_000);
        rpc.gbt.template_rejections.send_modify(|state| {
            state.set_parent(next.tip_hash);
            state.reject(next.tip_hash, "rejected-work");
        });
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        let_other_tasks_run().await;

        let mut id = populated_template_id(&next);
        id.revision = rpc.gbt.template_rejections.borrow().revision;
        assert!(rpc.gbt.shared_template.ready(&id).is_none());
    });
}

/// The answers encoded ahead of requests are the bytes that encoding the template afresh gives,
/// for a request without a long poll ID and for a long poll from the old tip.
#[test]
fn answers_encoded_ahead_are_the_bytes_of_a_fresh_encoding() {
    use crate::server::jsonrpc::{Id, MethodResponse};

    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        tokio::spawn(rpc.clone().run_template_builder());
        let next = chain_info(3, 2, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        let built_ahead = template_built_ahead(&rpc, &next).await;

        let answers = [
            bounded(rpc.get_block_template(None)).await.unwrap(),
            bounded(long_poll(&rpc, initial.long_poll_id))
                .await
                .unwrap(),
        ];
        for (answer, submit_old) in answers.into_iter().zip([None, Some(false)]) {
            let mut afresh = answer.clone().try_into_template().unwrap();
            assert_eq!(afresh.work_id(), built_ahead.work_id());
            assert_eq!(afresh.submit_old, submit_old);
            afresh.shared_json = Default::default();
            let afresh = GetBlockTemplateResponse::from(afresh);

            let response = |template: &GetBlockTemplateResponse| {
                MethodResponse::response(Id::Number(1), &Ok(template), usize::MAX)
            };
            assert_eq!(
                response(&answer).as_json().get(),
                response(&afresh).as_json().get()
            );
        }
    });
}

/// `generate` randomizes the coinbase data on a clone of the handler. The clone must not share
/// templates built with the original miner parameters.
#[test]
fn randomized_miner_params_do_not_share_templates() {
    mining_runtime(async {
        let next = chain_info(3, 2, 400_000_000);
        let (rpc, old_tip_id) = rpc_with_long_poll_id_from_old_tip(&next).await;
        let shared = bounded(long_poll(&rpc, old_tip_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let again = bounded(long_poll(&rpc, old_tip_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(again.work_id(), shared.work_id());

        let mut randomized = rpc.clone();
        randomized.gbt.randomize_coinbase_data();
        let own = bounded(long_poll(&randomized, old_tip_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(own.previous_block_hash, next.tip_hash);
        assert_ne!(own.work_id(), shared.work_id());
        assert_ne!(own.coinbase_txn.data, shared.coinbase_txn.data);
    });
}

/// A build whose caller disconnects leaves the long polls waiting for it to build their own.
#[test]
fn abandoned_shared_build_falls_back_to_per_caller_build() {
    mining_runtime(async {
        let next = chain_info(3, 2, 400_000_000);
        let (rpc, old_tip_id) = rpc_with_long_poll_id_from_old_tip(&next).await;

        let gate = TemplateBuildGate::new(&rpc).await;
        let mut builder = long_poll(&rpc, old_tip_id);
        let mut waiter = long_poll(&rpc, old_tip_id);
        assert!(futures::poll!(&mut builder).is_pending());
        assert!(
            futures::poll!(&mut waiter).is_pending(),
            "a long poll waits for the shared build instead of building"
        );
        drop(builder);
        gate.release().await;

        let template = bounded(waiter).await.unwrap().try_into_template().unwrap();
        assert_eq!(template.previous_block_hash, next.tip_hash);
        assert_eq!(template.transactions.len(), 1);
    });
}

/// A template shared on one tip is not served once the tip has moved on.
#[test]
fn shared_template_is_not_served_on_a_newer_tip() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let next = chain_info(3, 2, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        let shared = bounded(long_poll(&rpc, initial.long_poll_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(shared.previous_block_hash, next.tip_hash);

        let newest = chain_info(4, 3, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &newest);
        let template = bounded(long_poll(&rpc, initial.long_poll_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(template.previous_block_hash, newest.tip_hash);
        assert_ne!(template.work_id(), shared.work_id());
    });
}

/// A long poll that waits for a shared build is not handed its template when the tip moves on
/// during the build, even though the template has the long poll ID it asked for: it answers on
/// the new tip instead.
#[test]
fn shared_template_finished_after_a_tip_change_is_not_served() {
    mining_runtime(async {
        let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
        let (rpc, tip, mempool_snapshots, _) = populated_mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let next = chain_info(3, 2, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &next);
        let shared = bounded(long_poll(&rpc, initial.long_poll_id))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();

        // Stand in for another long poll's build of the same work on `next`.
        let build = rpc
            .gbt
            .shared_template
            .claim(next.tip_hash, shared.long_poll_id)
            .expect("no other build is running");
        let mut waiter = long_poll(&rpc, initial.long_poll_id);
        assert!(
            futures::poll!(&mut waiter).is_pending(),
            "a long poll for the work being built waits for it"
        );

        let newest = chain_info(4, 3, 400_000_000);
        move_populated_tip(&info_tx, &tip, &mempool_snapshots, &newest);
        build.publish(shared.clone());

        let template = bounded(waiter).await.unwrap().try_into_template().unwrap();
        assert_eq!(template.previous_block_hash, newest.tip_hash);
        assert_ne!(template.work_id(), shared.work_id());
    });
}

#[tokio::test]
async fn long_poll_preserves_negative_balance_error() {
    let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
    let (rpc, tip, _) = mining_rpc(info);
    let initial = bounded(rpc.get_block_template(None))
        .await
        .unwrap()
        .try_into_template()
        .unwrap();
    let request = rpc.get_block_template(Some(GetBlockTemplateParameters {
        long_poll_id: Some(initial.long_poll_id),
        ..Default::default()
    }));
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    let next = chain_info(3, 2, -1);
    info_tx.send_replace(next.clone());
    tip.send_best_tip_height(next.tip_height);
    tip.send_best_tip_hash(next.tip_hash);
    let error = bounded(request).await.unwrap_err();
    assert_eq!(error.code(), -1);
    assert!(error.message().contains("NSM value balance is negative"));
}

#[test]
fn rejection_during_construction_rebuilds_template() {
    mining_runtime(async {
        let (_, info) = watch::channel(chain_info(2, 1, 400_000_000));
        // Recovery treats a closed tip channel as a stale parent, so keep the sender alive.
        let (rpc, _tip, mut verifier) = mining_rpc(info);
        let gate = TemplateBuildGate::new(&rpc).await;
        let request = rpc.get_block_template(None);
        tokio::pin!(request);
        assert!(futures::poll!(&mut request).is_pending());
        rpc.gbt.template_rejections.send_modify(|state| {
            assert!(state.reject(Hash([1; 32]), "rejected-work"));
        });
        let (response, (), ()) = bounded(async {
            tokio::join!(
                request,
                async {
                    verifier
                        .expect_request_that(|req| {
                            matches!(req, zakura_consensus::Request::Prepare { .. })
                        })
                        .await
                        .respond(Hash([9; 32]));
                },
                async {
                    // Reproduce construction taking longer than the default mock
                    // deadline before the verifier can receive its request.
                    tokio::time::sleep(zakura_test::mock_service::DEFAULT_MAX_REQUEST_DELAY * 2)
                        .await;
                    gate.release().await;
                }
            )
        })
        .await;
        let template = response.unwrap().try_into_template().unwrap();
        assert_eq!(template.long_poll_id.revision, 1);
        assert_eq!(template.submit_old, Some(false));
        assert_reward(&template, 400_000_000);
        assert!(rpc
            .gbt
            .template_rejections
            .borrow()
            .is_prepared(template.work_id()));
    });
}

#[test]
fn recovery_construction_yields_and_rebuilds_on_parent_change() {
    mining_runtime(async {
        for change_tip in [false, true] {
            let chain = chain_info(2, 1, 400_000_000);
            let (_, info) = watch::channel(chain.clone());
            let (rpc, tip, mut verifier) = mining_rpc(info);
            let params = rpc.gbt.miner_params().unwrap();
            let (template, _) = bounded(rpc.build_mining_template(
                None,
                params,
                &chain,
                template_id(&chain),
                vec![],
                None,
            ))
            .await
            .unwrap();
            rpc.gbt.template_rejections.send_modify(|state| {
                state.set_parent(chain.tip_hash);
                state.reject(chain.tip_hash, "rejected-work");
            });
            let gate = TemplateBuildGate::new(&rpc).await;
            let request = rpc.finish_mining_template(template, &chain, params);
            tokio::pin!(request);
            assert!(futures::poll!(&mut request).is_pending());
            // Recovery must wait for construction before asking the verifier.
            assert!(verifier.try_next_request().now_or_never().is_none());
            if change_tip {
                tip.send_best_tip_hash(Hash([2; 32]));
            }
            gate.release().await;
            let response = if change_tip {
                // Recovery stops at the tip change before it dispatches validation.
                let response = bounded(request).await;
                assert!(verifier.try_next_request().now_or_never().is_none());
                response
            } else {
                let (response, ()) = bounded(async {
                    tokio::join!(request, async {
                        verifier
                            .expect_request_that(|req| {
                                matches!(req, zakura_consensus::Request::Prepare { .. })
                            })
                            .await
                            .respond(Hash([9; 32]));
                    })
                })
                .await;
                response
            };
            if change_tip {
                assert!(
                    response
                        .expect("a superseded recovery is not an error")
                        .is_none(),
                    "a tip change during recovery must ask the caller to rebuild, \
                     not surface a transient error to the miner"
                );
            } else {
                let template = response
                    .unwrap()
                    .expect("parent is unchanged, so recovery returns a template")
                    .try_into_template()
                    .unwrap();
                assert_reward(&template, 400_000_000);
                assert!(rpc
                    .gbt
                    .template_rejections
                    .borrow()
                    .is_prepared(template.work_id()));
            }
        }
    });
}

#[test]
fn failed_recovery_validation_rebuilds_when_committed_state_moved_on() {
    mining_runtime(async {
        let chain = chain_info(2, 1, 400_000_000);
        let (info_tx, info) = watch::channel(chain.clone());
        let (rpc, _tip, mut verifier) = mining_rpc(info);
        let params = rpc.gbt.miner_params().unwrap();
        let (template, _) = bounded(rpc.build_mining_template(
            None,
            params,
            &chain,
            template_id(&chain),
            vec![],
            None,
        ))
        .await
        .unwrap();
        rpc.gbt.template_rejections.send_modify(|state| {
            state.set_parent(chain.tip_hash);
            state.reject(chain.tip_hash, "rejected-work");
        });

        // Committed state moves on while both watches still name the old parent, so
        // the context re-check passes and only the committed-tip read can tell that
        // the proposal failed because the tip moved rather than because it is bad.
        info_tx.send_modify(|current| current.tip_hash = Hash([7; 32]));

        let (response, ()) = bounded(async {
            tokio::join!(
                rpc.finish_mining_template(template, &chain, params),
                async {
                    verifier
                        .expect_request_that(|req| {
                            matches!(req, zakura_consensus::Request::Prepare { .. })
                        })
                        .await
                        .respond_error(
                            "proposal is not based on the current best chain tip".into(),
                        );
                }
            )
        })
        .await;

        assert!(
            response
                .expect("a stale proposal failure is not the miner's error to see")
                .is_none(),
            "when committed state has moved past the template's parent, the caller \
             must rebuild instead of receiving the verifier's own error"
        );
    });
}

#[tokio::test]
async fn recovery_preserves_negative_balance_error() {
    let (_, info) = watch::channel(chain_info(2, 1, 0));
    let (rpc, _, _) = mining_rpc(info);
    let template = bounded(rpc.get_block_template(None))
        .await
        .unwrap()
        .try_into_template()
        .unwrap();
    rpc.gbt.template_rejections.send_modify(|state| {
        state.reject(Hash([1; 32]), "rejected-work");
    });
    let error = bounded(rpc.finish_mining_template(
        template,
        &chain_info(2, 1, -1),
        rpc.gbt.miner_params().unwrap(),
    ))
    .await
    .unwrap_err();
    assert_eq!(error.code(), -1);
    assert!(error.message().contains("NSM value balance is negative"));
}

/// Real proof smoke coverage complements the deterministic scheduling tests.
/// Run explicitly: cargo test -p zakura-rpc --release shielded_template_rewards -- --ignored
#[tokio::test]
#[ignore = "generates a real shielded proof"]
async fn shielded_template_rewards() {
    use config::mining::{default_miner_address, MinerAddressType};
    use types::{get_block_template::MinerParams, long_poll::LongPollInput};
    use zakura_chain::parameters::subsidy::halving_block_subsidy;
    let _guard = zakura_test::init();
    let net = network();
    let (_, info) = watch::channel(chain_info(2, 1, 400_000_000));
    let (mut rpc, _, _) = mining_rpc(info);
    rpc.network = net.clone();
    let params = MinerParams::new(
        &net,
        config::mining::Config {
            miner_address: Some(
                default_miner_address(net.kind(), &MinerAddressType::Unified)
                    .parse()
                    .unwrap(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    let chain = chain_info(2, 1, 400_000_000);
    let id =
        LongPollInput::new(chain.tip_height, chain.tip_hash, chain.max_time, vec![]).generate_id();
    let (template, _) = tokio::time::timeout(
        Duration::from_secs(300),
        rpc.build_mining_template(None, &params, &chain, id, vec![], None),
    )
    .await
    .unwrap()
    .unwrap();
    let tx: Transaction = template
        .coinbase_txn
        .data
        .as_ref()
        .zcash_deserialize_into()
        .unwrap();
    let base = i64::from(halving_block_subsidy(Height(3), &net).unwrap());
    let expected = base + 55;
    let (sapling, orchard, ironwood) = (
        i64::from(tx.sapling_value_balance().sapling_amount()),
        i64::from(tx.orchard_value_balance().orchard_amount()),
        i64::from(tx.ironwood_value_balance().ironwood_amount()),
    );
    assert_eq!((sapling, orchard, ironwood), (0, 0, -expected));
}
