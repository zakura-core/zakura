//! Exercise RPC completion delays against the actual state writer and gossip loop.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::{sync::mpsc, time::Instant};
use tower::{buffer::Buffer, ServiceExt};

use zakura_chain::{
    block::{
        Block, ChainHistoryBlockTxAuthCommitmentHash, Height, CHAIN_HISTORY_ACTIVATION_RESERVED,
    },
    chain_sync_status::MockSyncStatus,
    chain_tip::ChainTip,
    parameters::{testnet::ConfiguredActivationHeights, Network},
    serialization::ZcashSerialize,
    transaction::{arbitrary::transaction_to_fake_v5, Transaction},
    transparent::Input,
};
use zakura_network::{address_book_peers::MockAddressBookPeers, Request, Response};
use zakura_rpc::{
    methods::{RpcImpl, RpcServer},
    MinedSubmissions, SubmitBlockChannel,
};
use zakura_test::mock_service::MockService;

use crate::components::sync::{
    gossip::{gossip_best_tip_block_hashes, BLOCK_GOSSIP_TIMEOUT},
    BlockGossipError, SyncStatus,
};

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
            "advertised before release or timeout"
        );
        tokio::task::yield_now().await;
    }
}

/// The Regtest fixture enters at the semantic-verifier boundary. This adapter skips
/// cryptographic verification, but state performs the real contextual commit, admission and
/// tip publication. Only delivery of its successful result back to RPC is held behind a gate.
async fn delayed_completion(early_inventory: bool, hold_until_timeout: bool) {
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        }
        .into(),
    );
    let (state, read_state, latest_tip, tip_changes) =
        zakura_state::init(zakura_state::Config::ephemeral(), &network, Height(0), 0)
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

    // The real state service needs a multi-thread runtime for its blocking handoff. Keep its
    // buffer there, and drive RPC/gossip on a separate runtime whose clock can be paused.
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let submissions = MinedSubmissions::default();
                let mining_events = SubmitBlockChannel::new();
                let mining_sender = mining_events.sender();
                let (advertise_tx, mut advertisements) = mpsc::unbounded_channel();
                let peers = tower::service_fn(move |request| {
                    advertise_tx.send((Instant::now(), request)).unwrap();
                    async { Ok::<_, crate::BoxError>(Response::Nil) }
                });
                let (sync_status, mut recent_syncs) = SyncStatus::new();
                SyncStatus::sync_close_to_tip(&mut recent_syncs);
                let gossip = gossip_best_tip_block_hashes(
                    sync_status,
                    tip_changes,
                    peers,
                    read_state.clone(),
                    Some(mining_events.receiver()),
                    submissions.clone(),
                );
                tokio::pin!(gossip);
                let (_, initial) = next_advertisement(gossip.as_mut(), &mut advertisements).await;
                assert!(
                    matches!(initial, Request::AdvertiseBlock(block, None) if block.hash == genesis.hash())
                );

                let mut verifier: MockService<_, _, _, crate::BoxError> = MockService::build().for_unit_tests();
                let mempool: MockService<_, _, _, crate::BoxError> = MockService::build().for_unit_tests();
                let (_logs, log_rx) = tokio::sync::watch::channel(None);
                let (rpc, queue) = RpcImpl::new(
                    network,
                    zakura_rpc::config::mining::Config {
                        optimistic_block_inventory: early_inventory,
                        ..Default::default()
                    },
                    false,
                    "test",
                    "miner relay test",
                    mempool,
                    state.clone(),
                    read_state,
                    verifier.clone(),
                    MockSyncStatus::default(),
                    latest_tip.clone(),
                    MockAddressBookPeers::default(),
                    log_rx,
                    Some(mining_sender),
                );
                let rpc = rpc.with_mined_submissions(submissions.clone());
                let bytes = candidate.zcash_serialize_to_vec().unwrap();
                let submission = tokio::spawn(async move {
                    rpc.submit_block(
                        serde_json::from_value(serde_json::json!(hex::encode(bytes))).unwrap(),
                        None,
                    )
                    .await
                });
                let verification_response = verifier
                    .expect_request_that(|request| {
                        matches!(request, zakura_consensus::Request::CommitMined { .. })
                    })
                    .await;
                let zakura_consensus::Request::CommitMined { block, admission } =
                    verification_response.request()
                else {
                    unreachable!("the request was checked above");
                };
                // Stand in for consensus's authorization of this known fixture. State still
                // decides whether the real writer handoff permits early inventory.
                admission.authorize_optimistic_relay();
                let commit = state.oneshot(
                    zakura_state::Request::CommitSemanticallyVerifiedBlockWithAdmission {
                        block: block.clone().into(),
                        admission: admission.clone(),
                        requested_at: std::time::Instant::now(),
                    },
                );
                // Wait for actual publication before starting the controlled gossip observation.
                let committed = tokio::time::timeout(Duration::from_secs(30), commit)
                .await
                .unwrap()
                .unwrap();
                let zakura_state::Response::Committed(committed) = committed else {
                    panic!("the writer must return a commit result");
                };
                assert!(admission.is_admitted());
                assert!(admission.optimistic_relay_authorized());
                // Retain the responder: the writer has committed, but RPC cannot send its final event yet.
                let mut verification_response = Some(verification_response);
                assert_eq!(committed, candidate.hash());
                assert_eq!(latest_tip.best_tip_hash(), Some(committed));
                assert!(submissions.contains(&committed));
                assert!(!submission.is_finished());

                // The actual OS writer has completed. From here, advance only the coordination clock;
                // next_advertisement keeps real size reads runnable without automatic time jumps.
                tokio::time::pause();
                let committed_at = Instant::now();
                assert!(futures::poll!(gossip.as_mut()).is_pending());
                let deadline = submissions
                    .gossip_deadline(committed, BLOCK_GOSSIP_TIMEOUT)
                    .unwrap();
                assert_eq!(deadline - committed_at, Duration::from_secs(90));
                let (first_at, first) = if early_inventory {
                    next_advertisement(gossip.as_mut(), &mut advertisements).await
                } else {
                    assert_no_advertisement(gossip.as_mut(), &mut advertisements).await;
                    let wait = if hold_until_timeout {
                        deadline - Instant::now()
                    } else {
                        Duration::from_secs(1)
                    };
                    tokio::time::advance(wait - Duration::from_millis(1)).await;
                    assert_no_advertisement(gossip.as_mut(), &mut advertisements).await;
                    // Tokio rounds timers to milliseconds; cross the deadline by one tick.
                    tokio::time::advance(Duration::from_millis(if hold_until_timeout { 2 } else { 1 })).await;
                    if !hold_until_timeout {
                        verification_response.take().unwrap().respond(committed);
                    }
                    next_advertisement(gossip.as_mut(), &mut advertisements).await
                };
                let delay = first_at.saturating_duration_since(committed_at);
                let expected_delay = match (early_inventory, hold_until_timeout) {
                    (true, _) => Duration::ZERO,
                    (false, false) => Duration::from_secs(1),
                    (false, true) => deadline - committed_at + Duration::from_millis(1),
                };
                assert_eq!(delay, expected_delay);
                if !early_inventory && hold_until_timeout {
                    assert!(matches!(first, Request::AdvertiseBlock(block, None) if block.hash == committed));
                    assert!(submissions.contains(&committed));
                    assert!(!submission.is_finished());
                } else {
                    assert!(matches!(first, Request::AdvertiseMinedBlock(block) if block.hash == committed));
                }
                tracing::info!(
                    early_inventory,
                    held_until_timeout = hold_until_timeout,
                    first_advertisement_delay = ?delay,
                    "observed first block advertisement"
                );

                if let Some(response) = verification_response.take() {
                    response.respond(committed);
                }
                // Resume real time before waiting for task cleanup, so timeouts stay meaningful.
                tokio::time::resume();
                let response = tokio::time::timeout(Duration::from_secs(10), submission)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(response).unwrap(),
                    serde_json::Value::Null
                );
                assert!(!submissions.contains(&committed));
                queue.abort();
            });
    })
    .await
    .unwrap();
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
    delayed_completion(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_writer_without_early_inventory_waits_for_delayed_rpc_completion() {
    delayed_completion(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_writer_without_early_inventory_relays_after_coordination_timeout() {
    delayed_completion(false, true).await;
}
