//! Exercise RPC completion delays against the actual state writer and gossip loop.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::{sync::mpsc, time::Instant};
use tower::{buffer::Buffer, ServiceExt};

use zakura_chain::{
    block::{
        Block, ChainHistoryBlockTxAuthCommitmentHash, Height, CHAIN_HISTORY_ACTIVATION_RESERVED,
    },
    chain_sync_status::MockSyncStatus,
    parameters::{testnet::ConfiguredActivationHeights, Network},
    serialization::ZcashSerialize,
    transaction::{arbitrary::transaction_to_fake_v5, Transaction},
    transparent::Input,
};
use zakura_network::{address_book_peers::MockAddressBookPeers, Request, Response};
use zakura_rpc::{
    methods::{RpcImpl, RpcServer},
    EarlyMinedBlock, EarlyMinedBlockChannel, MinedBlockSubmission, PendingBlockSignal,
};
use zakura_test::mock_service::MockService;

use crate::components::sync::{gossip::gossip_best_tip_block_hashes, BlockGossipError, SyncStatus};

type Advertisement = (Instant, Request);

/// Drive gossip without advancing paused time while the real state's read worker runs.
/// Tokio's automatic clock advancement could otherwise mistake an OS-thread read for a stall.
async fn next_advertisement(
    mut gossip: Pin<&mut impl Future<Output = Result<(), BlockGossipError>>>,
    advertisements: &mut mpsc::UnboundedReceiver<Advertisement>,
) -> Advertisement {
    let started = std::time::Instant::now();
    loop {
        assert!(futures::poll!(gossip.as_mut()).is_pending());
        if let Ok(advertisement) = advertisements.try_recv() {
            return advertisement;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "gossip must reach the peer service without advancing the test clock"
        );
        tokio::task::yield_now().await;
    }
}

/// Give spawned broadcasts and real state reads time to run while the gossip clock is frozen.
/// A single poll followed by `try_recv` could miss a broadcast that was just spawned.
async fn assert_no_advertisement(
    mut gossip: Pin<&mut impl Future<Output = Result<(), BlockGossipError>>>,
    advertisements: &mut mpsc::UnboundedReceiver<Advertisement>,
) {
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_millis(100) {
        assert!(futures::poll!(gossip.as_mut()).is_pending());
        assert!(
            advertisements.try_recv().is_err(),
            "no additional advertisement is expected"
        );
        tokio::task::yield_now().await;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RelayOrdering {
    HeldRpc,
    EarlyAfterCommit,
    EarlyAfterAnotherLocalCommit,
    RetryAfterFailure,
    EarlyWaitingForReadiness,
    EarlyAlreadyDispatched,
}

/// A service that exposes entry into readiness and never becomes ready.
struct NeverReady(Arc<std::sync::atomic::AtomicBool>);

impl tower::Service<Request> for NeverReady {
    type Response = Response;
    type Error = crate::BoxError;
    type Future = std::future::Ready<Result<Response, crate::BoxError>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        std::task::Poll::Pending
    }

    fn call(&mut self, _: Request) -> Self::Future {
        panic!("an early event must not dispatch after commit while waiting for readiness")
    }
}

/// The adapter skips cryptographic verification, but uses real state admission, contextual
/// commitment and publication. RPC's verification response remains held throughout observation.
async fn delayed_completion(early_inventory: bool, ordering: RelayOrdering) {
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        }
        .into(),
    );
    let zakura_state::InitializedState {
        state,
        read_state,
        latest_chain_tip: latest_tip,
        block_announcements,
        ..
    } = zakura_state::init(zakura_state::Config::ephemeral(), &network, Height(0), 0)
        .await
        .unwrap();
    let state = Buffer::new(state, 1);
    let genesis = zakura_chain::block::genesis::regtest_genesis_block();
    tokio::time::timeout(
        Duration::from_secs(30),
        state
            .clone()
            .oneshot(zakura_state::Request::CommitCheckpointVerifiedBlock(
                genesis.clone().into(),
            )),
    )
    .await
    .unwrap()
    .unwrap();
    let candidate = child_of_genesis(&genesis, &network);

    // State's blocking handoff stays on the outer multi-thread runtime. Drive gossip on a
    // current-thread runtime so its clock can be frozen while the real writer finishes.
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(async move {
                let mining_events = EarlyMinedBlockChannel::new();
                let mining_sender = mining_events.sender();
                let delayed_sender = mining_sender.clone();
                let (advertise_tx, mut advertisements) = mpsc::unbounded_channel();
                let fail_next = Arc::new(std::sync::atomic::AtomicBool::new(
                    ordering == RelayOrdering::RetryAfterFailure,
                ));
                let peers = tower::service_fn(move |request| {
                    let fail = matches!(&request, Request::AdvertiseMinedBlock(_))
                        && fail_next.swap(false, std::sync::atomic::Ordering::SeqCst);
                    advertise_tx.send((Instant::now(), request)).unwrap();
                    async move {
                        if fail { Err::<Response, crate::BoxError>("injected broadcast failure".into()) }
                        else { Ok(Response::Nil) }
                    }
                });
                let (sync_status, mut recent_syncs) = SyncStatus::new();
                SyncStatus::sync_close_to_tip(&mut recent_syncs);
                let gossip = gossip_best_tip_block_hashes(
                    sync_status, latest_tip.clone(), block_announcements,
                    peers, read_state.clone(), Some(mining_events.receiver()),
                );
                tokio::pin!(gossip);
                let (_, initial) = next_advertisement(gossip.as_mut(), &mut advertisements).await;
                assert!(matches!(initial, Request::AdvertiseBlock(block, None) if block.hash == genesis.hash()));

                let mut verifier: MockService<_, _, _, crate::BoxError> = MockService::build().for_unit_tests();
                let mempool: MockService<_, _, _, crate::BoxError> = MockService::build().for_unit_tests();
                let (_logs, log_rx) = tokio::sync::watch::channel(None);
                let (rpc, queue) = RpcImpl::new(
                    network,
                    zakura_rpc::config::mining::Config {
                        optimistic_block_inventory: early_inventory, ..Default::default()
                    },
                    false, "test", "miner relay test", mempool,
                    zakura_node_services::mempool::MempoolSnapshotSender::new().subscribe(),
                    state.clone(), read_state,
                    verifier.clone(), MockSyncStatus::default(), latest_tip,
                    MockAddressBookPeers::default(), log_rx, Some(mining_sender),
                );
                let bytes = candidate.zcash_serialize_to_vec().unwrap();
                let submission = tokio::spawn(async move {
                    rpc.submit_block(
                        serde_json::from_value(serde_json::json!(hex::encode(bytes))).unwrap(), None,
                    ).await
                });
                let verification_response = verifier.expect_request_that(|request| {
                    matches!(request, zakura_consensus::Request::CommitMined { .. })
                }).await;
                let origin = verification_response.request().origin();
                let zakura_consensus::Request::CommitMined { block, admission, .. } =
                    verification_response.request() else { unreachable!("request checked above") };
                let admission = admission.clone();
                admission.authorize_optimistic_relay();
                let mut verified: zakura_state::SemanticallyVerifiedBlock = block.clone().into();
                verified.origin = origin;
                let mut release_early = None;
                let waiting_early = if ordering == RelayOrdering::EarlyWaitingForReadiness {
                    let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
                    let task = tokio::spawn(crate::components::sync::gossip::broadcast_early_with_timeout(
                        NeverReady(polled.clone()), Request::AdvertiseMinedBlock(
                            zakura_network::AdvertisedBlock::new(candidate.hash(), candidate.zcash_serialized_size()),
                        ), admission.clone(),
                    ));
                    tokio::time::timeout(Duration::from_secs(10), async {
                        while !polled.load(std::sync::atomic::Ordering::SeqCst) {
                            tokio::task::yield_now().await;
                        }
                    }).await.unwrap();
                    Some(task)
                } else if ordering == RelayOrdering::EarlyAlreadyDispatched {
                    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
                    release_early = Some(release_tx);
                    let mut started_tx = Some(started_tx);
                    let mut release_rx = Some(release_rx);
                    let peers = tower::service_fn(move |_| {
                        started_tx.take().unwrap().send(()).unwrap();
                        let release = release_rx.take().unwrap();
                        async move {
                            release.await.unwrap();
                            Ok::<_, crate::BoxError>(Response::Nil)
                        }
                    });
                    let task = tokio::spawn(crate::components::sync::gossip::broadcast_early_with_timeout(
                        peers, Request::AdvertiseMinedBlock(zakura_network::AdvertisedBlock::new(
                            candidate.hash(), candidate.zcash_serialized_size(),
                        )), admission.clone(),
                    ));
                    tokio::time::timeout(Duration::from_secs(10), started_rx).await.unwrap().unwrap();
                    Some(task)
                } else { None };
                let committed = tokio::time::timeout(Duration::from_secs(30), state.clone().oneshot(
                    zakura_state::Request::CommitSemanticallyVerifiedBlockWithAdmission {
                        block: verified, admission: admission.clone(),
                        requested_at: std::time::Instant::now(),
                    },
                )).await.unwrap().unwrap();
                let zakura_state::Response::Committed(committed) = committed else {
                    panic!("the writer returns a commit result");
                };
                assert!(admission.wait().await);
                assert!(admission.is_committed());
                if let Some(waiting) = waiting_early {
                    if let Some(release) = release_early {
                        assert!(!waiting.is_finished(), "commit preserves an already dispatched early broadcast");
                        release.send(()).unwrap();
                        assert!(tokio::time::timeout(Duration::from_secs(10), waiting).await.unwrap().unwrap());
                    } else {
                        assert!(!tokio::time::timeout(Duration::from_secs(10), waiting).await.unwrap().unwrap(),
                            "commit cancels early readiness without dispatch");
                    }
                }
                assert!(!admission.optimistic_relay_authorized());
                tokio::time::pause();
                let committed_at = Instant::now();
                let (first_at, first) = next_advertisement(gossip.as_mut(), &mut advertisements).await;
                assert_eq!(first_at, committed_at, "state publication needs no RPC coordination delay");
                assert!(matches!(first, Request::AdvertiseMinedBlock(block) if block.hash == committed));
                assert!(!submission.is_finished());
                if ordering == RelayOrdering::RetryAfterFailure {
                    let (_, retry) = next_advertisement(gossip.as_mut(), &mut advertisements).await;
                    assert!(matches!(retry, Request::AdvertiseMinedBlock(block) if block.hash == committed));
                }
                assert_no_advertisement(gossip.as_mut(), &mut advertisements).await;

                if ordering == RelayOrdering::EarlyAfterAnotherLocalCommit {
                    let mut sibling = (*candidate).clone();
                    Arc::make_mut(&mut sibling.header).nonce.0[0] ^= 1;
                    let sibling = Arc::new(sibling);
                    let sibling_hash = sibling.hash();
                    let size = sibling.zcash_serialized_size();
                    let mut verified: zakura_state::SemanticallyVerifiedBlock = sibling.into();
                    verified.origin = zakura_state::BlockOrigin::LocalSubmission { size };
                    tokio::time::resume();
                    state.clone().oneshot(zakura_state::Request::CommitSemanticallyVerifiedBlock(
                        verified,
                    )).await.unwrap();
                    tokio::time::pause();
                    let (_, other) = next_advertisement(gossip.as_mut(), &mut advertisements).await;
                    assert!(matches!(other, Request::AdvertiseMinedBlock(block) if block.hash == sibling_hash));
                    assert_no_advertisement(gossip.as_mut(), &mut advertisements).await;
                }

                if ordering != RelayOrdering::HeldRpc {
                    // Model an early event queued before commit but consumed only after its
                    // committed broadcast (and possibly a different block's) has finished.
                    delayed_sender.send(EarlyMinedBlock {
                        hash: committed, height: Height(1), size: candidate.zcash_serialized_size(),
                        submitted_at: std::time::Instant::now(), submission: MinedBlockSubmission::FullBlock,
                        pending: PendingBlockSignal::valid_for_tests(), admission,
                    }).unwrap();
                    assert_no_advertisement(gossip.as_mut(), &mut advertisements).await;
                }
                verification_response.respond(committed);
                tokio::time::resume();
                let response = tokio::time::timeout(Duration::from_secs(10), submission)
                    .await.unwrap().unwrap().unwrap();
                assert_eq!(serde_json::to_value(response).unwrap(), serde_json::Value::Null);
                assert_no_advertisement(gossip.as_mut(), &mut advertisements).await;
                queue.abort();
            });
    }).await.unwrap();
}

/// Build a height-1 coinbase block with the history and transaction commitments expected by
/// the real Regtest writer. Proof of work and semantic checks are outside this test's boundary.
fn child_of_genesis(genesis: &Arc<Block>, network: &Network) -> Arc<Block> {
    let mut coinbase = transaction_to_fake_v5(&genesis.transactions[0], network, Height(1));
    let Transaction::V5 { inputs, .. } = &mut coinbase else {
        unreachable!("the conversion produces a v5 transaction");
    };
    let Input::Coinbase { height, .. } = &mut inputs[0] else {
        unreachable!("the genesis transaction is a coinbase");
    };
    *height = Height(1);
    let mut child = (**genesis).clone();
    child.transactions = vec![Arc::new(coinbase)];
    let commitment: [u8; 32] = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &CHAIN_HISTORY_ACTIVATION_RESERVED.into(),
        &child.auth_data_root(),
    )
    .into();
    let header = Arc::make_mut(&mut child.header);
    header.previous_block_hash = genesis.hash();
    header.time += chrono::Duration::seconds(1);
    header.merkle_root = child.transactions.iter().collect();
    header.commitment_bytes = commitment.into();
    Arc::new(child)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_writer_early_inventory_does_not_wait_for_rpc_completion() {
    delayed_completion(true, RelayOrdering::HeldRpc).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_writer_without_early_inventory_does_not_wait_for_rpc_completion() {
    delayed_completion(false, RelayOrdering::HeldRpc).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_writer_suppresses_early_event_after_committed_broadcast() {
    delayed_completion(true, RelayOrdering::EarlyAfterCommit).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_writer_suppresses_old_early_event_after_another_local_commit() {
    delayed_completion(true, RelayOrdering::EarlyAfterAnotherLocalCommit).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_writer_retries_failed_committed_broadcast_without_stale_early_event() {
    delayed_completion(true, RelayOrdering::RetryAfterFailure).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_writer_cancels_early_broadcast_waiting_for_network_readiness() {
    delayed_completion(true, RelayOrdering::EarlyWaitingForReadiness).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_writer_keeps_an_already_dispatched_early_broadcast_running() {
    delayed_completion(true, RelayOrdering::EarlyAlreadyDispatched).await;
}
