//! Integration tests for block hash gossip.

#![allow(clippy::unwrap_in_result)]

use std::{sync::Arc, time::Duration};

use tokio::{task::JoinHandle, time::timeout};
use tower::{builder::ServiceBuilder, util::BoxService, Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::{
    block::{self, Block, Height},
    fmt::humantime_seconds,
    parameters::Network::Mainnet,
    serialization::ZcashDeserializeInto,
};
use zakura_network::{Request, Response};
use zakura_rpc::{MinedBlockEvent, PendingBlockSignal, SubmitBlockChannel};
use zakura_state::{
    ChainTipChange, ChainTipSender, CheckpointVerifiedBlock, Config as StateConfig,
    CHAIN_TIP_UPDATE_WAIT_LIMIT,
};
use zakura_test::mock_service::{MockService, PanicAssertion};

use crate::components::sync::{self, gossip::BLOCK_GOSSIP_TIMEOUT, BlockGossipError, SyncStatus};

const MAX_PEER_SET_REQUEST_DELAY: Duration = Duration::from_secs(30);

struct GossipTestSetup {
    peer_set: MockService<Request, Response, PanicAssertion>,
    submitblock_sender: tokio::sync::mpsc::UnboundedSender<MinedBlockEvent>,
    gossip_task_handle: JoinHandle<Result<(), BlockGossipError>>,
}

// Paused time must not race the state's OS writer thread. Mocked gossip scenarios
// publish tips synchronously through the same channel the writer uses.
async fn setup_gossip_test() -> (ChainTipSender, GossipTestSetup) {
    let _init_guard = zakura_test::init();
    let block_one: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let tip = CheckpointVerifiedBlock::from(block_one.clone()).into();
    let (chain_tip_sender, _latest_chain_tip, chain_tip_change) =
        ChainTipSender::new(Some(tip), &Mainnet);
    let setup = start_gossip_test(chain_tip_change, block_one.hash()).await;
    (chain_tip_sender, setup)
}

async fn setup_state_gossip_test() -> (
    BoxService<zakura_state::Request, zakura_state::Response, crate::BoxError>,
    GossipTestSetup,
) {
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

    let setup = start_gossip_test(chain_tip_change, block_one.hash()).await;
    (BoxService::new(state_service), setup)
}

async fn start_gossip_test(
    chain_tip_change: ChainTipChange,
    initial_tip_hash: block::Hash,
) -> GossipTestSetup {
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

    // Block 1 is the initial tip when the task starts.
    peer_set
        .expect_request(Request::AdvertiseBlock(initial_tip_hash, None))
        .await
        .respond(Response::Nil);

    GossipTestSetup {
        peer_set,
        submitblock_sender,
        gossip_task_handle,
    }
}

/// After a successful mined block broadcast, the gossip task marks the tip as seen and does not
/// send a duplicate committed-tip gossip for the same hash.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn mined_block_marks_tip_after_successful_broadcast() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash: block_two.hash(),
            height: block_two.coinbase_height().unwrap(),
        })
        .expect("mined block notification should be accepted");

    peer_set
        .expect_request(Request::AdvertiseMinedBlock(block_two.hash()))
        .await
        .respond(Response::Nil);

    // The committed tip gossip path should not advertise the same hash again.
    peer_set.expect_no_requests().await;
}

/// A successful mined-block broadcast still suppresses the committed-tip fallback for that hash
/// even when another mined-block notification is already queued.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn mined_block_mark_survives_pending_submit_queue() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let height = block_two.coinbase_height().unwrap();
    let hash = block_two.hash();

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    // Hold the first mined broadcast response open.
    submitblock_sender
        .send(MinedBlockEvent::Committed { hash, height })
        .expect("mined block notification should be accepted");

    let first_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(hash))
        .await;

    // Queue a second notification while the first broadcast is still in flight so the
    // submit-block channel is nonempty when the first mark arrives.
    submitblock_sender
        .send(MinedBlockEvent::Committed { hash, height })
        .expect("second mined block notification should be accepted");

    first_broadcast.respond(Response::Nil);

    // The queued mined notification starts another broadcast.
    peer_set
        .expect_request(Request::AdvertiseMinedBlock(hash))
        .await
        .respond(Response::Nil);

    // Without unconditional marking, the first mark would be dropped while the queue was
    // nonempty and the committed-tip path could still AdvertiseBlock(hash).
    peer_set.expect_no_requests().await;
}

/// If a mined block broadcast times out, the committed tip gossip path should still advertise the
/// hash as a fallback.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn mined_block_broadcast_timeout_uses_committed_tip_fallback() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash: block_two.hash(),
            height: block_two.coinbase_height().unwrap(),
        })
        .expect("mined block notification should be accepted");

    let slow_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(block_two.hash()))
        .await;

    // Hold the mined block broadcast open past the gossip timeout so it fails without marking.
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT + Duration::from_secs(1)).await;
    drop(slow_broadcast);

    // The committed tip gossip path should advertise the same hash as a fallback.
    peer_set
        .expect_request(Request::AdvertiseMinedBlock(block_two.hash()))
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
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

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
        .expect_request(Request::AdvertiseMinedBlock(hash))
        .await
        .respond(Response::Nil);

    // Let the spawned early broadcast finish before the block commits.
    tokio::time::sleep(Duration::from_millis(200)).await;

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    submitblock_sender
        .send(MinedBlockEvent::Committed { hash, height })
        .expect("the committed mined block notification is accepted");

    // Hold the committed broadcast open past the gossip timeout so it fails without marking.
    let slow_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(hash))
        .await;
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT + Duration::from_secs(1)).await;
    drop(slow_broadcast);

    // Nothing has advertised a body this node can serve, so the fallback must still run.
    peer_set
        .expect_request(Request::AdvertiseMinedBlock(hash))
        .await
        .respond(Response::Nil);
}

/// Consecutive committed blocks are gossiped as soon as they commit, with no delay between them.
///
/// This test uses real time: with a paused clock, the state commit auto-advances time by minutes,
/// which would hide a gossip delay.
#[tokio::test(flavor = "current_thread")]
async fn consecutive_committed_blocks_are_gossiped_without_delay() {
    /// Well below the removed 7 second delay, and far above the expected latency.
    const MAX_GOSSIP_LATENCY: Duration = Duration::from_secs(3);

    let (
        mut state_service,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender: _submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_state_gossip_test().await;

    for block_bytes in [
        &*zakura_test::vectors::BLOCK_MAINNET_2_BYTES,
        &*zakura_test::vectors::BLOCK_MAINNET_3_BYTES,
    ] {
        let block: Arc<Block> = block_bytes.zcash_deserialize_into().unwrap();

        state_service
            .ready()
            .await
            .unwrap()
            .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
                block.clone().into(),
            ))
            .await
            .unwrap();
        let committed_at = tokio::time::Instant::now();

        peer_set
            .expect_request(Request::AdvertiseBlock(block.hash(), None))
            .await
            .respond(Response::Nil);

        assert!(
            committed_at.elapsed() < MAX_GOSSIP_LATENCY,
            "block gossip waited {:?} after the commit",
            committed_at.elapsed(),
        );
    }
}

/// While a mined block broadcast is in flight, the committed-tip path does not advertise the same
/// hash.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn in_flight_mined_block_broadcast_suppresses_committed_tip_gossip() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();

    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_two.clone()).into(),
    ));

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash: block_two.hash(),
            height: block_two.coinbase_height().unwrap(),
        })
        .expect("mined block notification should be accepted");

    let in_flight_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(block_two.hash()))
        .await;

    // Hold the broadcast open for less than its timeout, so it is still in flight when the
    // committed-tip path sees the tip change.
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT / 2).await;
    in_flight_broadcast.respond(Response::Nil);

    peer_set.expect_no_requests().await;
}

/// Checkpoint commits acknowledge before publishing the tip, unlike non-finalized commits.
/// A mined broadcast that is still in flight must suppress that later notification.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn in_flight_mined_block_broadcast_handles_delayed_tip_notification() {
    let (
        mut chain_tip_sender,
        GossipTestSetup {
            mut peer_set,
            submitblock_sender,
            gossip_task_handle: _gossip_task_handle,
        },
    ) = setup_gossip_test().await;

    let block_two: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let hash = block_two.hash();

    submitblock_sender
        .send(MinedBlockEvent::Committed {
            hash,
            height: block_two.coinbase_height().unwrap(),
        })
        .expect("mined block notification should be accepted");
    let in_flight_broadcast = peer_set
        .expect_request(Request::AdvertiseMinedBlock(hash))
        .await;

    // Publish the tip only after the mined notification has started its broadcast.
    chain_tip_sender.set_finalized_tip(Some(CheckpointVerifiedBlock::from(block_two).into()));
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT / 2).await;
    in_flight_broadcast.respond(Response::Nil);

    peer_set.expect_no_requests().await;

    // A dead gossip task would also send no duplicate. Require it to relay the next tip.
    let block_three: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_3_BYTES
        .zcash_deserialize_into()
        .unwrap();
    chain_tip_sender.set_finalized_tip(Some(
        CheckpointVerifiedBlock::from(block_three.clone()).into(),
    ));
    peer_set
        .expect_request(Request::AdvertiseBlock(block_three.hash(), None))
        .await
        .respond(Response::Nil);
}
