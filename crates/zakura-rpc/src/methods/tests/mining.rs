//! Regressions for parent-dependent mining RPC work and failures.

use std::{
    future::Future,
    sync::Mutex,
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
        nsm_reissuance_height: Some(Height(3)),
        ..Default::default()
    })
}

fn rpc<M: MempoolService, R: ReadStateService>(
    network: Network,
    mempool: M,
    read: R,
    tip: MockChainTip,
) -> (TestRpc<M, R>, Verifier) {
    let verifier: Verifier = MockService::build().for_unit_tests();
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
        assert!(matches!(request, ReadRequest::ChainInfo));
        let info = info.borrow().clone();
        async move { Ok::<_, BoxError>(ReadResponse::ChainInfo(info)) }
    });
    let (rpc, verifier) = rpc(network(), mempool, read, tip);
    (rpc, sender, verifier)
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("test work must complete")
}

/// Blocks only the construction thread. The independent watchdog makes inline execution
/// fail without deadlocking the single-worker runtime or its shutdown.
fn construction_gate() -> (
    Arc<dyn Fn() + Send + Sync>,
    tokio::sync::oneshot::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = Mutex::new(Some(entered_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let hook = Arc::new(move || {
        if let Some(tx) = entered_tx.lock().unwrap().take() {
            let _ = tx.send(());
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .expect("async worker must remain free to release construction");
        }
    });
    (hook, entered_rx, release_tx)
}

#[tokio::test]
async fn normal_template_construction_leaves_runtime_responsive() {
    let (_, info) = watch::channel(chain_info(2, 1, 400_000_000));
    let (mut rpc, _, _) = mining_rpc(info);
    let (hook, entered, release) = construction_gate();
    rpc.template_build_hook = Some(hook);
    let request = tokio::spawn(async move { rpc.get_block_template(None).await });
    bounded(entered).await.unwrap();
    // This task can run before construction finishes, even on one Tokio worker.
    release.send(()).unwrap();
    bounded(request).await.unwrap().unwrap();
}

#[tokio::test]
async fn template_worker_panic_is_an_rpc_error() {
    let (_, info) = watch::channel(chain_info(2, 1, 0));
    let (mut rpc, _, _) = mining_rpc(info);
    rpc.template_build_hook = Some(Arc::new(|| panic!("test construction panic")));
    let error = bounded(rpc.get_block_template(None)).await.unwrap_err();
    assert_eq!(error.code(), -1);
    assert!(error.message().contains("test construction panic"));
}

#[tokio::test]
async fn tip_change_during_construction_does_not_publish_stale_work() {
    let (_, info) = watch::channel(chain_info(2, 1, 0));
    let (mut rpc, tip, _) = mining_rpc(info);
    let (hook, entered, release) = construction_gate();
    rpc.template_build_hook = Some(hook);
    let request = tokio::spawn(async move { rpc.get_block_template(None).await });
    bounded(entered).await.unwrap();
    tip.send_best_tip_hash(Hash([2; 32]));
    release.send(()).unwrap();
    let error = bounded(request).await.unwrap().unwrap_err();
    assert!(error.message().contains("parent changed"));
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

#[tokio::test]
async fn long_poll_builds_on_new_parent_without_blocking_runtime() {
    for (old_height, new_height) in [(0, 1), (0, 2), (2, 3), (1, 2), (2, 5), (4, 2)] {
        let (info_tx, info) = watch::channel(chain_info(old_height, 1, 0));
        let (mut rpc, tip, _) = mining_rpc(info);
        let initial = bounded(rpc.get_block_template(None))
            .await
            .unwrap()
            .try_into_template()
            .unwrap();
        let (hook, entered, release) = construction_gate();
        rpc.template_build_hook = Some(hook);
        let request = rpc.get_block_template(Some(GetBlockTemplateParameters {
            long_poll_id: Some(initial.long_poll_id),
            ..Default::default()
        }));
        tokio::pin!(request);
        assert!(
            futures::poll!(&mut request).is_pending(),
            "matching long poll must wait"
        );
        let next = chain_info(new_height, 2, 400_000_000);
        info_tx.send_replace(next.clone());
        tip.send_best_tip_height(next.tip_height);
        tip.send_best_tip_hash(next.tip_hash);
        let (response, ()) = bounded(async {
            tokio::join!(request, async {
                entered.await.unwrap();
                release.send(()).unwrap();
            })
        })
        .await;
        let template = response.unwrap().try_into_template().unwrap();
        assert_eq!(template.previous_block_hash, next.tip_hash);
        assert_eq!(template.height, new_height + 1);
        assert_reward(&template, 400_000_000);
    }
}

#[tokio::test]
async fn simultaneous_long_polls_remain_responsive() {
    let (info_tx, info) = watch::channel(chain_info(2, 1, 0));
    let (mut rpc, tip, _) = mining_rpc(info);
    let initial = bounded(rpc.get_block_template(None))
        .await
        .unwrap()
        .try_into_template()
        .unwrap();
    let (hook, entered, release) = construction_gate();
    rpc.template_build_hook = Some(hook);
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
    let next = chain_info(3, 2, 400_000_000);
    info_tx.send_replace(next.clone());
    tip.send_best_tip_height(next.tip_height);
    tip.send_best_tip_hash(next.tip_hash);
    let (responses, ()) = bounded(async {
        tokio::join!(futures::future::join_all(requests), async {
            entered.await.unwrap();
            release.send(()).unwrap();
        })
    })
    .await;
    for response in responses {
        let template = response.unwrap().try_into_template().unwrap();
        assert_eq!(template.previous_block_hash, next.tip_hash);
        assert_reward(&template, 400_000_000);
    }
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

#[tokio::test]
async fn rejection_during_construction_rebuilds_off_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (_, info) = watch::channel(chain_info(2, 1, 400_000_000));
    let (mut rpc, _, mut verifier) = mining_rpc(info);
    let (first_hook, first_entered, first_release) = construction_gate();
    let (recovery_hook, recovery_entered, recovery_release) = construction_gate();
    let builds = AtomicUsize::new(0);
    rpc.template_build_hook = Some(Arc::new(move || {
        if builds.fetch_add(1, Ordering::SeqCst) == 0 {
            first_hook();
        } else {
            recovery_hook();
        }
    }));
    let rejections = rpc.gbt.template_rejections.clone();
    let request = tokio::spawn(async move { rpc.get_block_template(None).await });
    bounded(first_entered).await.unwrap();
    rejections.send_modify(|state| {
        state.reject(Hash([1; 32]), "rejected-work");
    });
    first_release.send(()).unwrap();
    bounded(recovery_entered).await.unwrap();
    recovery_release.send(()).unwrap();
    let validation = verifier
        .expect_request_that(|req| matches!(req, zakura_consensus::Request::Prepare { .. }))
        .await;
    validation.respond(Hash([9; 32]));
    let template = bounded(request)
        .await
        .unwrap()
        .unwrap()
        .try_into_template()
        .unwrap();
    assert_eq!(template.long_poll_id.revision, 1);
    assert_eq!(template.submit_old, Some(false));
    assert_reward(&template, 400_000_000);
    assert!(rejections.borrow().is_prepared(template.work_id()));
}

#[tokio::test]
async fn recovery_rejects_tip_change_during_construction() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (_, info) = watch::channel(chain_info(2, 1, 0));
    let (mut rpc, tip, mut verifier) = mining_rpc(info);
    let (hook, entered, release) = construction_gate();
    let builds = AtomicUsize::new(0);
    rpc.template_build_hook = Some(Arc::new(move || {
        if builds.fetch_add(1, Ordering::SeqCst) == 1 {
            hook();
        }
    }));
    rpc.gbt.template_rejections.send_modify(|state| {
        state.set_parent(Hash([1; 32]));
        state.reject(Hash([1; 32]), "rejected-work");
    });
    let request = tokio::spawn(async move { rpc.get_block_template(None).await });
    bounded(entered).await.unwrap();
    tip.send_best_tip_hash(Hash([2; 32]));
    release.send(()).unwrap();
    verifier
        .expect_request_that(|req| matches!(req, zakura_consensus::Request::Prepare { .. }))
        .await
        .respond(Hash([9; 32]));
    let error = bounded(request).await.unwrap().unwrap_err();
    assert!(error.message().contains("changed during recovery"));
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
                nsm_reissuance_height: Some(Height(104)),
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
