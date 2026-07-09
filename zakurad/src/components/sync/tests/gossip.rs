//! Integration tests for block hash gossip.

#![allow(clippy::unwrap_in_result)]

use std::{sync::Arc, time::Duration};

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
use zakura_rpc::SubmitBlockChannel;
use zakura_state::{Config as StateConfig, CHAIN_TIP_UPDATE_WAIT_LIMIT};
use zakura_test::mock_service::{MockService, PanicAssertion};

use crate::components::sync::{
    self, BlockGossipError, SyncStatus, PEER_GOSSIP_DELAY, TIPS_RESPONSE_TIMEOUT,
};

const MAX_PEER_SET_REQUEST_DELAY: Duration = Duration::from_secs(30);

struct GossipTestSetup {
    peer_set: MockService<Request, Response, PanicAssertion>,
    submitblock_sender: tokio::sync::mpsc::Sender<(zakura_chain::block::Hash, Height)>,
    state_service: BoxService<zakura_state::Request, zakura_state::Response, crate::BoxError>,
    gossip_task_handle: JoinHandle<Result<(), BlockGossipError>>,
}

async fn setup_gossip_test() -> GossipTestSetup {
    let _init_guard = zakura_test::init();

    let network = Mainnet;
    let state_config = StateConfig::ephemeral();
    let (state, _read_only_state, _latest_chain_tip, mut chain_tip_change) =
        zakura_state::init(state_config, &network, Height::MAX, 0).await;

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
    tokio::time::sleep(PEER_GOSSIP_DELAY).await;
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

/// After a successful mined block broadcast, the gossip task marks the tip as seen and does not
/// send a duplicate committed-tip gossip for the same hash.
#[tokio::test(flavor = "multi_thread")]
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
        .send((block_two.hash(), block_two.coinbase_height().unwrap()))
        .await
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
#[tokio::test(flavor = "multi_thread")]
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
        .send((hash, height))
        .await
        .expect("mined block notification should be accepted");

    let first_broadcast = peer_set
        .expect_request(Request::AdvertiseBlockToAll(hash))
        .await;

    // Queue a second notification while the first broadcast is still in flight so the
    // submit-block channel is nonempty when the first mark arrives.
    submitblock_sender
        .send((hash, height))
        .await
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
#[tokio::test(flavor = "multi_thread")]
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
        .send((block_two.hash(), block_two.coinbase_height().unwrap()))
        .await
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
