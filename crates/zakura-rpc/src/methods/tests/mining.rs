//! Regressions for parent-dependent mining RPC work and failures.

use std::{
    future::Future,
    task::{Context, Poll},
};

use futures::{future::BoxFuture, FutureExt};
use zakura_chain::{
    block::Hash,
    block_info::BlockInfo,
    chain_sync_status::MockSyncStatus,
    chain_tip::mock::MockChainTip,
    parameters::testnet::{ConfiguredActivationHeights, RegtestParameters},
};
use zakura_network::address_book_peers::MockAddressBookPeers;
use zakura_node_services::BoxError;
use zakura_state::GetBlockTemplateChainInfo;
use zakura_test::mock_service::{MockService, PanicAssertion};
use zcash_address::{ToAddress, ZcashAddress};
use zcash_protocol::consensus::NetworkType;

use super::super::*;
use super::utils::fake_history_tree;

type Mock<Req, Resp> = MockService<Req, Resp, PanicAssertion, BoxError>;
type Verifier = Mock<zakura_consensus::Request, Hash>;
type TestRpc<M, R> = RpcImpl<
    M,
    Mock<zakura_state::Request, zakura_state::Response>,
    R,
    MockChainTip,
    MockAddressBookPeers,
    Verifier,
    MockSyncStatus,
>;

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
) -> (TestRpc<M, R>, Verifier) {
    // Template construction runs on the blocking pool and can exceed the mock
    // default of 300 ms on busy CI runners. Use the enclosing test deadline.
    let verifier: Verifier = MockService::build()
        .with_max_request_delay(Duration::from_secs(10))
        .for_unit_tests();
    let (_, rx) = watch::channel(None);
    let (rpc, queue) = RpcImpl::new(
        network,
        config::mining::Config {
            miner_address: Some(ZcashAddress::from_transparent_p2pkh(
                NetworkType::Regtest,
                [0x7e; 20],
            )),
            internal_miner: true,
            optimistic_block_inventory: true,
            ..Default::default()
        },
        false,
        "test",
        "mining regression",
        mempool,
        MockService::build().for_unit_tests(),
        read,
        verifier.clone(),
        MockSyncStatus::default(),
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
    TestRpc<impl MempoolService, impl ReadStateService>,
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
                transactions: vec![],
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

#[test]
fn normal_template_construction_leaves_runtime_responsive() {
    mining_runtime(async {
        let (_, info) = watch::channel(chain_info(2, 1, 400_000_000));
        let (rpc, _, _) = mining_rpc(info);
        let gate = BlockingPoolGate::new().await;
        let request = rpc.get_block_template(None);
        tokio::pin!(request);
        assert!(
            futures::poll!(&mut request).is_pending(),
            "construction must yield to the blocking pool"
        );
        gate.release().await;
        bounded(request).await.unwrap();
    });
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
        let mut proof = Box::pin(handler.run_template_build(|| ()));
        assert!(futures::poll!(&mut proof).is_pending());

        let mut template = Box::pin(rpc.build_mining_template(
            Some(coinbase),
            params,
            &chain,
            template_id(&chain),
            vec![],
            Some(false),
        ));
        let Poll::Ready(Ok(template)) = futures::poll!(&mut template) else {
            panic!("a precomputed coinbase must not wait behind proof construction");
        };
        assert_eq!(template.coinbase_txn.data, expected_coinbase);
        assert_eq!(template.previous_block_hash, chain.tip_hash);
        gate.release().await;
        bounded(proof).await.unwrap();
    });
}

#[test]
fn template_build_paths_retain_capacity_when_cancelled() {
    mining_runtime(async {
        for path in ["normal", "precompute", "tip change", "recovery"] {
            let old_height = if path == "precompute" { 0 } else { 2 };
            let chain = chain_info(old_height, 1, 0);
            let (info_tx, info) = watch::channel(chain.clone());
            let (rpc, tip, _) = mining_rpc(info);
            let initial = bounded(rpc.get_block_template(None))
                .await
                .unwrap()
                .try_into_template()
                .unwrap();
            let gate = BlockingPoolGate::new().await;
            tokio::time::pause();

            let mut request = match path {
                "normal" => rpc.get_block_template(None).boxed(),
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
            // The build is queued behind the occupied blocking pool. Its caller disconnects.
            drop(request);
            let handler = rpc.gbt.clone();
            let mut contender = Box::pin(handler.run_template_build(|| ()));
            assert!(futures::poll!(&mut contender).is_pending());
            tokio::time::advance(Duration::from_secs(31)).await;
            let Poll::Ready(Err(error)) = futures::poll!(&mut contender) else {
                panic!("{path} must retain its slot until the queued job finishes");
            };
            assert_eq!(error.code(), -1);
            assert!(error.message().contains("construction capacity"));

            gate.release().await;
            bounded(handler.run_template_build(|| ())).await.unwrap();
            tokio::time::resume();
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
                .run_template_build(move || {
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
        let mut waiter = Box::pin(rpc.gbt.run_template_build(move || ran.send(())));
        assert!(futures::poll!(&mut waiter).is_pending());
        drop(waiter);
        assert!(bounded(cancelled).await.is_err());

        tokio::time::pause();
        let mut contender = Box::pin(rpc.gbt.run_template_build(|| ()));
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
        bounded(rpc.gbt.run_template_build(|| ())).await.unwrap();
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
        let gate = BlockingPoolGate::new().await;
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
                transactions: vec![],
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
        let gate = BlockingPoolGate::new().await;

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
    assert!(rpc.select_mining_template_parent(Hash([1; 32])));
    tip.send_best_tip_hash(Hash([2; 32]));
    assert!(!rpc.select_mining_template_parent(Hash([1; 32])));
    assert_eq!(
        rpc.gbt.template_rejections.borrow().parent,
        Some(Hash([1; 32])),
        "a refused selection leaves the state untouched"
    );
    assert!(rpc.select_mining_template_parent(Hash([2; 32])));
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
            let gate = BlockingPoolGate::new().await;
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
        let gate = BlockingPoolGate::new().await;
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
        assert!(rpc.select_mining_template_parent(other.tip_hash));

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
        let gate = BlockingPoolGate::new().await;
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
        let (rpc, _, mut verifier) = mining_rpc(info);
        let gate = BlockingPoolGate::new().await;
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
            let template = bounded(rpc.build_mining_template(
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
            let gate = BlockingPoolGate::new().await;
            let request = rpc.finish_mining_template(template, &chain, params);
            tokio::pin!(request);
            assert!(futures::poll!(&mut request).is_pending());
            // Recovery must wait for construction before asking the verifier.
            assert!(verifier.try_next_request().now_or_never().is_none());
            if change_tip {
                tip.send_best_tip_hash(Hash([2; 32]));
            }
            gate.release().await;
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
        let template = bounded(rpc.build_mining_template(
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
#[ignore = "generates three real shielded proofs"]
async fn shielded_template_rewards() {
    use config::mining::{default_miner_address, MinerAddressType};
    use types::{get_block_template::MinerParams, long_poll::LongPollInput};
    use zakura_chain::parameters::subsidy::halving_block_subsidy;
    let _guard = zakura_test::init();
    for (pool, address_type) in [
        ("sapling", MinerAddressType::Sapling),
        ("orchard", MinerAddressType::Unified),
        ("ironwood", MinerAddressType::Unified),
    ] {
        // Orchard rewards precede NU6.3; Sapling and Ironwood exercise NSM activation.
        let net = if pool == "orchard" {
            Network::new_regtest(RegtestParameters {
                activation_heights: ConfiguredActivationHeights {
                    nu5: Some(1),
                    nu6: Some(100),
                    nu6_1: Some(101),
                    nu6_2: Some(102),
                    nu6_3: Some(103),
                    nu7: Some(104),
                    ..Default::default()
                },
                test_nsm_reissuance_height: Some(Height(104)),
                ..Default::default()
            })
        } else {
            network()
        };
        let (_, info) = watch::channel(chain_info(2, 1, 400_000_000));
        let (mut rpc, _, _) = mining_rpc(info);
        rpc.network = net.clone();
        let params = MinerParams::new(
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
        .unwrap();
        let chain = chain_info(2, 1, 400_000_000);
        let id = LongPollInput::new(chain.tip_height, chain.tip_hash, chain.max_time, vec![])
            .generate_id();
        let template = tokio::time::timeout(
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
        let expected = base + if pool == "orchard" { 0 } else { 55 };
        let (sapling, orchard, ironwood) = (
            i64::from(tx.sapling_value_balance().sapling_amount()),
            i64::from(tx.orchard_value_balance().orchard_amount()),
            i64::from(tx.ironwood_value_balance().ironwood_amount()),
        );
        let balances = match pool {
            "sapling" => (-expected, 0, 0),
            "orchard" => (0, -expected, 0),
            _ => (0, 0, -expected),
        };
        assert_eq!((sapling, orchard, ironwood), balances, "{pool}");
    }
}
