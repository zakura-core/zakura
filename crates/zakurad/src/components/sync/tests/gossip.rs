//! Integration tests for block hash gossip.

#![allow(clippy::unwrap_in_result)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{task::JoinHandle, time::timeout};
use tower::{builder::ServiceBuilder, util::BoxService, Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::{
    block::{Block, Height},
    fmt::humantime_seconds,
    parameters::Network::Mainnet,
    serialization::ZcashDeserializeInto,
};
use zakura_network::{Request, Response};
use zakura_rpc::{MinedBlockEvent, PendingBlockSignal, SubmitBlockChannel};
use zakura_state::{
    ChainTipBlock, ChainTipSender, Config as StateConfig, CHAIN_TIP_UPDATE_WAIT_LIMIT,
};
use zakura_test::mock_service::{MockService, PanicAssertion};

use crate::components::sync::{
    self, BlockGossipError, SyncStatus, PEER_GOSSIP_DELAY, TIPS_RESPONSE_TIMEOUT,
};

const MAX_PEER_SET_REQUEST_DELAY: Duration = Duration::from_secs(30);

struct GossipTestSetup {
    peer_set: MockService<Request, Response, PanicAssertion>,
    submitblock_sender: tokio::sync::mpsc::UnboundedSender<MinedBlockEvent>,
    state_service: BoxService<zakura_state::Request, zakura_state::Response, crate::BoxError>,
    gossip_task_handle: JoinHandle<Result<(), BlockGossipError>>,
}

async fn setup_gossip_test() -> GossipTestSetup {
    let _init_guard = zakura_test::init();

    let network = Mainnet;
    let state_config = StateConfig::ephemeral();
    let (state, _read_only_state, _latest_chain_tip, mut chain_tip_change) =
        zakura_state::init(state_config, &network, Height::MAX, 0)
            .await
            .expect("ephemeral state initialization succeeds");

    let mut state_service = ServiceBuilder::new().buffer(1).service(state);

    let genesis_block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .unwrap();
    state_service
        .ready()
        .await
        .unwrap()
        .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
            genesis_block.into(),
        ))
        .await
        .unwrap();

    if let Err(timeout_error) = timeout(
        CHAIN_TIP_UPDATE_WAIT_LIMIT,
        chain_tip_change.wait_for_tip_change(),
    )
    .await
    .map(|change_result| change_result.expect("unexpected chain tip update failure"))
    {
        panic!(
            "timeout waiting for genesis chain tip change after {}: {timeout_error:?}",
            humantime_seconds(CHAIN_TIP_UPDATE_WAIT_LIMIT),
        );
    }

    let block_one: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    state_service
        .clone()
        .oneshot(zakura_state::Request::CommitCheckpointVerifiedBlock(
            block_one.clone().into(),
        ))
        .await
        .unwrap();

    let (sync_status, mut recent_syncs) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent_syncs);

    let mut peer_set = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();

    let submitblock_channel = SubmitBlockChannel::new();
    let submitblock_sender = submitblock_channel.sender();
    let gossip_task_handle = tokio::spawn(
        sync::gossip_best_tip_block_hashes(
            sync_status,
            chain_tip_change,
            peer_set.clone(),
            Some(submitblock_channel.receiver()),
        )
        .in_current_span(),
    );

    // The genesis block gossip is skipped because block 1 is committed before the task starts.
    peer_set
        .expect_request(Request::AdvertiseBlock(block_one.hash(), None))
        .await
        .respond(Response::Nil);

    GossipTestSetup {
        peer_set,
        submitblock_sender,
        state_service: BoxService::new(state_service),
        gossip_task_handle,
    }
}

/// Synthetic selected-tip notifications isolate the scheduler from async state commits.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn committed_tip_relay_is_prompt() {
    let _init_guard = zakura_test::init();
    let (mut tip_sender, _latest_tip, tip_change) = ChainTipSender::new(None, &Mainnet);
    let (sync_status, mut recent_syncs) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent_syncs);
    let mut peer_set = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();
    let _gossip_task_handle = tokio::spawn(sync::gossip_best_tip_block_hashes(
        sync_status,
        tip_change,
        peer_set.clone(),
        None,
    ));
    let tip = |byte, height, previous| ChainTipBlock {
        hash: zakura_chain::block::Hash([byte; 32]),
        height: Height(height),
        time: chrono::Utc::now(),
        transactions: Vec::new(),
        transaction_hashes: Arc::from([]),
        previous_block_hash: zakura_chain::block::Hash([previous; 32]),
    };
    tip_sender.set_finalized_tip(tip(1, 1, 0));
    peer_set
        .expect_request(Request::AdvertiseBlock(
            zakura_chain::block::Hash([1; 32]),
            None,
        ))
        .await
        .respond(Response::Nil);
    tokio::task::yield_now().await;

    let changed_at = tokio::time::Instant::now();
    tip_sender.set_finalized_tip(tip(2, 2, 1));
    peer_set
        .expect_request(Request::AdvertiseBlock(
            zakura_chain::block::Hash([2; 32]),
            None,
        ))
        .await
        .respond(Response::Nil);
    let elapsed = changed_at.elapsed();
    tracing::info!(
        "relay_probe scenario=back_to_back elapsed_ms={}",
        elapsed.as_millis()
    );

    assert!(
        elapsed < Duration::from_secs(1),
        "closely spaced tip must be prompt"
    );

    // A second selected-tip change at the same height exercises the timer while
    // it is active, as in a competing-branch selection.
    tokio::task::yield_now().await;
    let changed_at = tokio::time::Instant::now();
    tip_sender.set_finalized_tip(tip(3, 2, 1));
    peer_set
        .expect_request(Request::AdvertiseBlock(
            zakura_chain::block::Hash([3; 32]),
            None,
        ))
        .await
        .respond(Response::Nil);
    let same_height_elapsed = changed_at.elapsed();
    tracing::info!(
        "relay_probe scenario=same_height elapsed_ms={}",
        same_height_elapsed.as_millis()
    );
    assert!(same_height_elapsed < Duration::from_secs(1));

    // After a long idle, the loop has already consumed its delay in either arm.
    tokio::time::advance(Duration::from_secs(8)).await;
    let changed_at = tokio::time::Instant::now();
    tip_sender.set_finalized_tip(tip(4, 2, 1));
    peer_set
        .expect_request(Request::AdvertiseBlock(
            zakura_chain::block::Hash([4; 32]),
            None,
        ))
        .await
        .respond(Response::Nil);
    let idle_elapsed = changed_at.elapsed();
    tracing::info!(
        "relay_probe scenario=idle_same_height elapsed_ms={}",
        idle_elapsed.as_millis()
    );
    assert!(idle_elapsed < Duration::from_secs(1));
}

/// A stalled peer exposes how many relay futures can overlap under tip churn.
/// The synthetic tips model selected-tip notifications, not consensus validation.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn committed_tip_stalled_send_is_bounded() {
    let _init_guard = zakura_test::init();
    struct ActiveSend(Arc<AtomicUsize>);
    impl Drop for ActiveSend {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    let (mut tip_sender, _latest_tip, tip_change) = ChainTipSender::new(None, &Mainnet);
    let (sync_status, mut recent_syncs) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent_syncs);
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let sent = Arc::new(AtomicUsize::new(0));
    let (request_sender, mut request_receiver) = tokio::sync::mpsc::unbounded_channel();
    let service = tower::service_fn({
        let active = active.clone();
        let maximum = maximum.clone();
        let sent = sent.clone();
        let request_sender = request_sender.clone();
        move |_request: Request| {
            let active = active.clone();
            let maximum = maximum.clone();
            let sent = sent.clone();
            let request_sender = request_sender.clone();
            async move {
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(count, Ordering::SeqCst);
                sent.fetch_add(1, Ordering::SeqCst);
                let _ = request_sender.send(());
                let _guard = ActiveSend(active);
                std::future::pending::<()>().await;
                Ok::<Response, crate::BoxError>(Response::Nil)
            }
        }
    });
    let gossip_task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        sync_status,
        tip_change,
        service,
        None,
    ));
    let tip = |byte, height, previous| ChainTipBlock {
        hash: zakura_chain::block::Hash([byte; 32]),
        height: Height(height),
        time: chrono::Utc::now(),
        transactions: Vec::new(),
        transaction_hashes: Arc::from([]),
        previous_block_hash: zakura_chain::block::Hash([previous; 32]),
    };
    tip_sender.set_finalized_tip(tip(1, 1, 0));
    request_receiver
        .recv()
        .await
        .expect("initial relay request arrives");
    assert_eq!(
        sent.load(Ordering::SeqCst),
        1,
        "first blocked send must start"
    );
    assert_eq!(
        active.load(Ordering::SeqCst),
        1,
        "first send must be in flight"
    );
    for byte in 2..=21 {
        tip_sender.set_finalized_tip(tip(byte, u32::from(byte), byte - 1));
        tokio::time::advance(Duration::from_millis(110)).await;
        tokio::task::yield_now().await;
    }
    let peak = maximum.load(Ordering::SeqCst);
    let requests = sent.load(Ordering::SeqCst);
    tracing::info!("relay_burst_probe tips=20 requests={requests} max_in_flight={peak}");
    assert_eq!(peak, 1, "only one ordinary operation may be in flight");
    assert_eq!(
        requests, 1,
        "tip churn must coalesce while the sender is busy"
    );
    gossip_task.abort();
    assert!(gossip_task.await.unwrap_err().is_cancelled());
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "canceling gossip must drop the ordinary send"
    );
}

/// A failed ordinary send retries without another selected-tip notification.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn committed_tip_failure_retries_without_new_tip() {
    let _init_guard = zakura_test::init();
    let (mut tip_sender, _latest_tip, tip_change) = ChainTipSender::new(None, &Mainnet);
    let (sync_status, mut recent_syncs) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent_syncs);
    let calls = Arc::new(AtomicUsize::new(0));
    let (request_sender, mut request_receiver) = tokio::sync::mpsc::unbounded_channel();
    let service = tower::service_fn({
        let calls = calls.clone();
        move |_request: Request| {
            let calls = calls.clone();
            let request_sender = request_sender.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let _ = request_sender.send(());
                Err::<Response, crate::BoxError>(
                    std::io::Error::other("injected send failure").into(),
                )
            }
        }
    });
    let gossip_task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        sync_status,
        tip_change,
        service,
        None,
    ));
    tip_sender.set_finalized_tip(ChainTipBlock {
        hash: zakura_chain::block::Hash([1; 32]),
        height: Height(1),
        time: chrono::Utc::now(),
        transactions: Vec::new(),
        transaction_hashes: Arc::from([]),
        previous_block_hash: zakura_chain::block::Hash([0; 32]),
    });
    request_receiver
        .recv()
        .await
        .expect("first relay request arrives");
    tokio::time::sleep(Duration::from_secs(20)).await;
    tokio::task::yield_now().await;
    let count = calls.load(Ordering::SeqCst);
    tracing::info!("relay_failed_send_probe elapsed_after_failure_s=20 requests={count}");
    assert!(count >= 2, "unchanged tip must retry after failed delivery");
    assert!(count <= 21, "failures must not cause a busy retry loop");
    gossip_task.abort();
}

/// After a successful mined block broadcast, the gossip task marks the tip as seen and does not
/// send a duplicate committed-tip gossip for the same hash.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn mined_block_marks_tip_after_successful_broadcast() {
    let GossipTestSetup {
        mut peer_set,
        submitblock_sender,
        mut state_service,
        gossip_task_handle: _gossip_task_handle,
    } = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();

    state_service
        .ready()
        .await
        .unwrap()
        .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
            block_two.clone().into(),
        ))
        .await
        .unwrap();

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash: block_two.hash(),
            height: block_two.coinbase_height().unwrap(),
        })
        .expect("mined block notification should be accepted");

    peer_set
        .expect_request(Request::AdvertiseBlockToAll(block_two.hash()))
        .await
        .respond(Response::Nil);

    // Allow the spawned broadcast task to send the mark notification.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The committed tip gossip path should not advertise the same hash again.
    tokio::time::sleep(PEER_GOSSIP_DELAY).await;
    peer_set.expect_no_requests().await;
}

/// A successful mined-block broadcast still suppresses the committed-tip fallback for that hash
/// even when another mined-block notification is already queued.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn mined_block_mark_survives_pending_submit_queue() {
    let GossipTestSetup {
        mut peer_set,
        submitblock_sender,
        mut state_service,
        gossip_task_handle: _gossip_task_handle,
    } = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let height = block_two.coinbase_height().unwrap();
    let hash = block_two.hash();

    state_service
        .ready()
        .await
        .unwrap()
        .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
            block_two.clone().into(),
        ))
        .await
        .unwrap();

    // First mined notification — start AdvertiseBlockToAll but hold the response open.
    submitblock_sender
        .send(MinedBlockEvent::Committed { hash, height })
        .expect("mined block notification should be accepted");

    let first_broadcast = peer_set
        .expect_request(Request::AdvertiseBlockToAll(hash))
        .await;

    // Queue a second notification while the first broadcast is still in flight so the
    // submit-block channel is nonempty when the first mark arrives.
    submitblock_sender
        .send(MinedBlockEvent::Committed { hash, height })
        .expect("second mined block notification should be accepted");

    first_broadcast.respond(Response::Nil);

    // Second mined path also fires AdvertiseBlockToAll for the queued notification.
    peer_set
        .expect_request(Request::AdvertiseBlockToAll(hash))
        .await
        .respond(Response::Nil);

    // Allow spawned broadcast tasks to deliver marks.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Without unconditional marking, the first mark would be dropped while the queue was
    // nonempty and the committed-tip path could still AdvertiseBlock(hash).
    tokio::time::sleep(PEER_GOSSIP_DELAY).await;
    peer_set.expect_no_requests().await;
}

/// If a mined block broadcast times out, the committed tip gossip path should still advertise the
/// hash as a fallback.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn mined_block_broadcast_timeout_uses_committed_tip_fallback() {
    let GossipTestSetup {
        mut peer_set,
        submitblock_sender,
        mut state_service,
        gossip_task_handle: _gossip_task_handle,
    } = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();

    state_service
        .ready()
        .await
        .unwrap()
        .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
            block_two.clone().into(),
        ))
        .await
        .unwrap();

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash: block_two.hash(),
            height: block_two.coinbase_height().unwrap(),
        })
        .expect("mined block notification should be accepted");

    let slow_broadcast = peer_set
        .expect_request(Request::AdvertiseBlockToAll(block_two.hash()))
        .await;

    // Hold the mined block broadcast open past the gossip timeout so it fails without marking.
    tokio::time::sleep(TIPS_RESPONSE_TIMEOUT + Duration::from_secs(1)).await;
    drop(slow_broadcast);

    // The committed tip gossip path should advertise the same hash as a fallback.
    tokio::time::sleep(PEER_GOSSIP_DELAY).await;
    peer_set
        .expect_request(Request::AdvertiseBlock(block_two.hash(), None))
        .await
        .respond(Response::Nil);
}

/// An early broadcast advertises a hash whose body the node cannot serve yet, so it must not
/// suppress the committed-tip fallback.
///
/// A peer can follow the early inventory, exhaust `PENDING_BLOCK_WAIT` waiting for the body, and
/// give up. If the later committed broadcast then fails, the committed-tip gossip is the only
/// thing left that prompts that peer to ask again.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn early_broadcast_does_not_suppress_the_committed_tip_fallback() {
    let GossipTestSetup {
        mut peer_set,
        submitblock_sender,
        mut state_service,
        gossip_task_handle: _gossip_task_handle,
    } = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let hash = block_two.hash();
    let height = block_two.coinbase_height().unwrap();

    // The early broadcast succeeds, which is what would mark the tip as already gossiped.
    submitblock_sender
        .send(MinedBlockEvent::Early {
            hash,
            height,
            submitted_at: tokio::time::Instant::now().into_std(),
            pending: PendingBlockSignal::valid_for_tests(),
        })
        .expect("the early mined block notification is accepted");

    peer_set
        .expect_request(Request::AdvertiseBlockToAll(hash))
        .await
        .respond(Response::Nil);

    // Let the spawned early broadcast finish before the block commits.
    tokio::time::sleep(Duration::from_millis(200)).await;

    state_service
        .ready()
        .await
        .unwrap()
        .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
            block_two.clone().into(),
        ))
        .await
        .unwrap();

    submitblock_sender
        .send(MinedBlockEvent::Committed { hash, height })
        .expect("the committed mined block notification is accepted");

    // Hold the committed broadcast open past the gossip timeout so it fails without marking.
    let slow_broadcast = peer_set
        .expect_request(Request::AdvertiseBlockToAll(hash))
        .await;
    tokio::time::sleep(TIPS_RESPONSE_TIMEOUT + Duration::from_secs(1)).await;
    drop(slow_broadcast);

    // Nothing has advertised a body this node can serve, so the fallback must still run.
    tokio::time::sleep(PEER_GOSSIP_DELAY).await;
    peer_set
        .expect_request(Request::AdvertiseBlock(hash, None))
        .await
        .respond(Response::Nil);
}

fn selected_tip(byte: u8) -> ChainTipBlock {
    ChainTipBlock {
        hash: zakura_chain::block::Hash([byte; 32]),
        height: Height(2),
        time: chrono::Utc::now(),
        transactions: Vec::new(),
        transaction_hashes: Arc::from([]),
        previous_block_hash: zakura_chain::block::Hash([0; 32]),
    }
}

#[tokio::test(start_paused = true)]
async fn committed_tip_busy_send_coalesces_to_latest_after_obsolete_completion() {
    let (mut tips, _latest, changes) = ChainTipSender::new(None, &Mainnet);
    let (status, mut recent) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent);
    let mut peers = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();
    let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        status,
        changes,
        peers.clone(),
        None,
    ));
    tips.set_finalized_tip(selected_tip(1));
    let first = peers
        .expect_request(Request::AdvertiseBlock(selected_tip(1).hash, None))
        .await;
    for byte in 2..=21 {
        tips.set_finalized_tip(selected_tip(byte));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    first.respond(Response::Nil);
    peers
        .expect_request(Request::AdvertiseBlock(selected_tip(21).hash, None))
        .await
        .respond(Response::Nil);
    // Completing the old hash must not consume the newest tip; intermediate tips need no send.
    tokio::time::sleep(Duration::from_secs(2)).await;
    peers.expect_no_requests().await;
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn committed_tip_failed_send_recovers_then_stops_retrying() {
    let (mut tips, _latest, changes) = ChainTipSender::new(None, &Mainnet);
    let (status, mut recent) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent);
    let mut peers = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();
    let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        status,
        changes,
        peers.clone(),
        None,
    ));
    tips.set_finalized_tip(selected_tip(1));
    let request = Request::AdvertiseBlock(selected_tip(1).hash, None);
    peers
        .expect_request(request.clone())
        .await
        .respond_error(std::io::Error::other("injected failure").into());
    let failed_at = tokio::time::Instant::now();
    peers.expect_request(request).await.respond(Response::Nil);
    assert!(
        failed_at.elapsed() >= Duration::from_secs(1),
        "retry must back off"
    );
    assert!(
        failed_at.elapsed() < Duration::from_secs(2),
        "unchanged tip must retry promptly"
    );
    tokio::time::sleep(Duration::from_secs(20)).await;
    peers.expect_no_requests().await;
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn committed_tip_catchup_suppresses_and_coalesces_announcements() {
    let (mut tips, _latest, changes) = ChainTipSender::new(None, &Mainnet);
    let (status, mut recent) = SyncStatus::new();
    let mut peers = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();
    let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        status,
        changes,
        peers.clone(),
        None,
    ));
    tips.set_finalized_tip(selected_tip(1));
    tokio::time::sleep(Duration::from_secs(2)).await;
    tips.set_finalized_tip(selected_tip(2));
    peers.expect_no_requests().await;
    SyncStatus::sync_close_to_tip(&mut recent);
    peers
        .expect_request(Request::AdvertiseBlock(selected_tip(2).hash, None))
        .await
        .respond(Response::Nil);
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn committed_tip_closed_channels_terminate_without_spinning() {
    let (tips, latest, changes) = ChainTipSender::new(None, &Mainnet);
    let (status, _recent) = SyncStatus::new();
    let peers = MockService::build().for_unit_tests::<Request, Response, crate::BoxError>();
    let (mined_sender, mined_receiver) = tokio::sync::mpsc::unbounded_channel();
    drop(mined_sender);
    let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        status,
        changes,
        peers,
        Some(mined_receiver),
    ));
    drop(tips);
    drop(latest);
    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(BlockGossipError::TipChange(_))));
}

#[derive(Clone)]
struct ReadinessGate {
    opened: Arc<std::sync::atomic::AtomicBool>,
    waker: Arc<futures::task::AtomicWaker>,
    peer: MockService<Request, Response, PanicAssertion>,
}

impl Service<Request> for ReadinessGate {
    type Response = Response;
    type Error = crate::BoxError;
    type Future = <MockService<Request, Response, PanicAssertion> as Service<Request>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.waker.register(cx.waker());
        if self.opened.load(Ordering::SeqCst) {
            self.peer.poll_ready(cx)
        } else {
            std::task::Poll::Pending
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        self.peer.call(request)
    }
}

#[tokio::test(start_paused = true)]
async fn committed_tip_readiness_timeout_recovers_without_new_tip() {
    let (mut tips, _latest, changes) = ChainTipSender::new(None, &Mainnet);
    let (status, mut recent) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent);
    let mut peers = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();
    let gate = ReadinessGate {
        opened: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        waker: Arc::new(futures::task::AtomicWaker::new()),
        peer: peers.clone(),
    };
    let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        status,
        changes,
        gate.clone(),
        None,
    ));
    tips.set_finalized_tip(selected_tip(1));
    tokio::time::sleep(TIPS_RESPONSE_TIMEOUT + Duration::from_secs(2)).await;
    peers.expect_no_requests().await;
    gate.opened.store(true, Ordering::SeqCst);
    gate.waker.wake();
    timeout(
        Duration::from_secs(2),
        peers.expect_request(Request::AdvertiseBlock(selected_tip(1).hash, None)),
    )
    .await
    .unwrap()
    .respond(Response::Nil);
    tokio::time::sleep(Duration::from_secs(10)).await;
    peers.expect_no_requests().await;
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn committed_tip_state_shutdown_cancels_failed_retry() {
    let (mut tips, latest, changes) = ChainTipSender::new(None, &Mainnet);
    let (status, mut recent) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent);
    let mut peers = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();
    let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        status,
        changes,
        peers.clone(),
        None,
    ));
    tips.set_finalized_tip(selected_tip(1));
    peers
        .expect_request(Request::AdvertiseBlock(selected_tip(1).hash, None))
        .await
        .respond_error(std::io::Error::other("injected failure").into());
    drop(tips);
    drop(latest);
    let result = timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(BlockGossipError::TipChange(_))));
    peers.expect_no_requests().await;
}

#[tokio::test(start_paused = true)]
async fn committed_tip_state_shutdown_while_catching_up() {
    let (mut tips, latest, changes) = ChainTipSender::new(None, &Mainnet);
    let (status, _recent) = SyncStatus::new();
    let mut peers = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();
    let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        status,
        changes,
        peers.clone(),
        None,
    ));
    tips.set_finalized_tip(selected_tip(1));
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(tips);
    drop(latest);
    let result = timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(BlockGossipError::TipChange(_))));
    peers.expect_no_requests().await;
}

/// Mined success for a newly selected tip must discard an obsolete failed ordinary tip,
/// even when the mined completion and the tip watch notification are ready together.
#[tokio::test(start_paused = true)]
async fn mined_completion_for_new_tip_clears_obsolete_pending_retry() {
    for _ in 0..16 {
        let (mut tips, _latest, changes) = ChainTipSender::new(None, &Mainnet);
        let (status, mut recent) = SyncStatus::new();
        SyncStatus::sync_close_to_tip(&mut recent);
        let mut peers = MockService::build()
            .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
            .for_unit_tests();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
            status,
            changes,
            peers.clone(),
            Some(receiver),
        ));
        tips.set_finalized_tip(selected_tip(1));
        let old = peers
            .expect_request(Request::AdvertiseBlock(selected_tip(1).hash, None))
            .await;
        sender
            .send(MinedBlockEvent::Committed {
                hash: selected_tip(2).hash,
                height: Height(2),
            })
            .unwrap();
        let mined = peers
            .expect_request(Request::AdvertiseBlockToAll(selected_tip(2).hash))
            .await;
        tips.set_finalized_tip(selected_tip(2));
        mined.respond(Response::Nil);
        old.respond_error(std::io::Error::other("obsolete send failed").into());
        tokio::time::sleep(Duration::from_secs(10)).await;
        peers.expect_no_requests().await;
        task.abort();
    }
}

#[tokio::test(start_paused = true)]
async fn mined_notification_backlog_does_not_starve_ordinary_completion() {
    let (mut tips, _latest, changes) = ChainTipSender::new(None, &Mainnet);
    let (status, mut recent) = SyncStatus::new();
    SyncStatus::sync_close_to_tip(&mut recent);
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let premature_mined = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (finish_tx, finish_rx) = tokio::sync::watch::channel(false);
    let service = tower::service_fn({
        let completed = completed.clone();
        let premature_mined = premature_mined.clone();
        move |request| {
            let completed = completed.clone();
            let premature_mined = premature_mined.clone();
            let started_tx = started_tx.clone();
            let mut finish_rx = finish_rx.clone();
            async move {
                match request {
                    Request::AdvertiseBlock(..) => {
                        started_tx.send(()).unwrap();
                        finish_rx.wait_for(|done| *done).await.unwrap();
                        completed.store(true, Ordering::SeqCst);
                    }
                    Request::AdvertiseBlockToAll(..) => {
                        if !completed.load(Ordering::SeqCst) {
                            premature_mined.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    _ => unreachable!(),
                }
                Ok::<_, crate::BoxError>(Response::Nil)
            }
        }
    });
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        status,
        changes,
        service,
        Some(receiver),
    ));
    tips.set_finalized_tip(selected_tip(1));
    timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    for _ in 0..512 {
        sender
            .send(MinedBlockEvent::Committed {
                hash: selected_tip(2).hash,
                height: Height(2),
            })
            .unwrap();
    }
    finish_tx.send(true).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(completed.load(Ordering::SeqCst));
    assert_eq!(
        premature_mined.load(Ordering::SeqCst),
        0,
        "a ready ordinary completion must be polled ahead of the mined backlog"
    );
    task.abort();
}
