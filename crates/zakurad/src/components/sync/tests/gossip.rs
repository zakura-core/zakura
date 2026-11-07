//! Integration tests for committed and early block announcements.

#![allow(clippy::unwrap_in_result)]

use crate::components::sync::{self, gossip::BLOCK_GOSSIP_TIMEOUT, BlockGossipError, SyncStatus};
use std::{sync::Arc, time::Duration};
use tokio::{
    task::JoinHandle,
    time::{timeout, Instant},
};
use tower::{util::BoxCloneService, ServiceExt};
use zakura_chain::{
    block::{Block, Height},
    parameters::Network::Mainnet,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zakura_network::{AdvertisedBlock, Request, Response};
use zakura_rpc::{
    EarlyMinedBlock, EarlyMinedBlockChannel, MinedBlockSubmission, PendingBlockSignal,
};
use zakura_state::{
    BlockAnnouncements, BlockOrigin, ChainTipSender, CheckpointVerifiedBlock,
    CommittedBlockAnnouncement, Config as StateConfig, LatestChainTip,
};
use zakura_test::mock_service::{MockService, PanicAssertion};

const MAX_PEER_SET_REQUEST_DELAY: Duration = Duration::from_secs(30);
type ReadState =
    BoxCloneService<zakura_state::ReadRequest, zakura_state::ReadResponse, crate::BoxError>;

fn block(n: usize) -> Arc<Block> {
    let bytes = match n {
        0 => &*zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES,
        1 => &*zakura_test::vectors::BLOCK_MAINNET_1_BYTES,
        2 => &*zakura_test::vectors::BLOCK_MAINNET_2_BYTES,
        3 => &*zakura_test::vectors::BLOCK_MAINNET_3_BYTES,
        _ => unreachable!("fixture contains four blocks"),
    };
    bytes.zcash_deserialize_into().unwrap()
}

fn with_size(block: &Block) -> AdvertisedBlock {
    AdvertisedBlock::new(block.hash(), block.zcash_serialized_size())
}

fn without_size(block: &Block) -> AdvertisedBlock {
    AdvertisedBlock {
        size: None,
        ..with_size(block)
    }
}

fn read_state_without_sizes() -> ReadState {
    BoxCloneService::new(tower::service_fn(|request| async move {
        match request {
            zakura_state::ReadRequest::BlockInfo(_) => {
                Ok(zakura_state::ReadResponse::BlockInfo(None))
            }
            request => unreachable!("gossip only reads block sizes, not {request:?}"),
        }
    }))
}

struct GossipTestSetup {
    peer_set: MockService<Request, Response, PanicAssertion>,
    early_sender: tokio::sync::mpsc::UnboundedSender<EarlyMinedBlock>,
    task: JoinHandle<Result<(), BlockGossipError>>,
    recent_syncs: sync::RecentSyncLengths,
}

fn start(
    latest_tip: LatestChainTip,
    announcements: BlockAnnouncements,
    read_state: ReadState,
    close: bool,
) -> GossipTestSetup {
    let (sync_status, mut recent_syncs) = SyncStatus::new();
    if close {
        SyncStatus::sync_close_to_tip(&mut recent_syncs);
    }
    let peer_set = MockService::build()
        .with_max_request_delay(MAX_PEER_SET_REQUEST_DELAY)
        .for_unit_tests();
    let early = EarlyMinedBlockChannel::new();
    let early_sender = early.sender();
    let task = tokio::spawn(sync::gossip_best_tip_block_hashes(
        sync_status,
        latest_tip,
        announcements,
        peer_set.clone(),
        read_state,
        Some(early.receiver()),
    ));
    GossipTestSetup {
        peer_set,
        early_sender,
        task,
        recent_syncs,
    }
}

async fn setup() -> (ChainTipSender, GossipTestSetup) {
    let _init = zakura_test::init();
    let first = block(1);
    let (sender, latest, _) = ChainTipSender::new(
        Some(CheckpointVerifiedBlock::from(first.clone()).into()),
        &Mainnet,
    );
    let (sender, announcements) = sender.with_block_announcements();
    let mut gossip = start(latest, announcements, read_state_without_sizes(), true);
    gossip
        .peer_set
        .expect_request(Request::AdvertiseBlock(without_size(&first), None))
        .await
        .respond(Response::Nil);
    (sender, gossip)
}

fn local_commit(sender: &mut ChainTipSender, block: Arc<Block>) {
    let committed = CommittedBlockAnnouncement {
        hash: block.hash(),
        height: block.coinbase_height().unwrap(),
        origin: BlockOrigin::LocalSubmission {
            size: block.zcash_serialized_size(),
        },
    };
    sender.publish_finalized_commit(CheckpointVerifiedBlock::from(block).into(), committed);
}

#[tokio::test(start_paused = true)]
async fn local_commit_uses_mining_path_once_without_rpc_notification() {
    let (mut sender, mut gossip) = setup().await;
    let local = block(2);
    let committed_at = Instant::now();
    local_commit(&mut sender, local.clone());
    gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&local)))
        .await
        .respond(Response::Nil);
    assert_eq!(committed_at.elapsed(), Duration::ZERO);
    gossip.peer_set.expect_no_requests().await;
}

#[tokio::test(start_paused = true)]
async fn relayed_tip_change_is_gossiped_without_waiting() {
    let (mut sender, mut gossip) = setup().await;
    let peer = block(2);
    let committed_at = Instant::now();
    sender.set_finalized_tip(Some(CheckpointVerifiedBlock::from(peer.clone()).into()));
    gossip
        .peer_set
        .expect_request(Request::AdvertiseBlock(without_size(&peer), None))
        .await
        .respond(Response::Nil);
    assert_eq!(committed_at.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn ordinary_updates_coalesce_but_local_commits_are_all_broadcast() {
    let (mut sender, mut gossip) = setup().await;
    local_commit(&mut sender, block(2));
    local_commit(&mut sender, block(3));
    // Both publications happen before gossip runs. No ordinary relay is queued for either.
    for n in [2, 3] {
        gossip
            .peer_set
            .expect_request(Request::AdvertiseMinedBlock(with_size(&block(n))))
            .await
            .respond(Response::Nil);
    }
    gossip.peer_set.expect_no_requests().await;
}

#[tokio::test(start_paused = true)]
async fn local_non_tip_commit_is_announced_while_far_from_network_tip() {
    let _init = zakura_test::init();
    let best = block(3);
    let (sender, latest, _) = ChainTipSender::new(
        Some(CheckpointVerifiedBlock::from(best.clone()).into()),
        &Mainnet,
    );
    let (mut sender, announcements) = sender.with_block_announcements();
    let mut gossip = start(latest, announcements, read_state_without_sizes(), false);
    let local = block(2);
    sender.publish_non_finalized_commit(
        CheckpointVerifiedBlock::from(best).into(),
        CommittedBlockAnnouncement {
            hash: local.hash(),
            height: local.coinbase_height().unwrap(),
            origin: BlockOrigin::LocalSubmission {
                size: local.zcash_serialized_size(),
            },
        },
    );
    gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&local)))
        .await
        .respond(Response::Nil);
    gossip.peer_set.expect_no_requests().await;
}

#[tokio::test(start_paused = true)]
async fn in_flight_local_broadcast_does_not_delay_a_newer_peer_tip() {
    let (mut sender, mut gossip) = setup().await;
    local_commit(&mut sender, block(2));
    let slow = gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block(2))))
        .await;
    sender.set_finalized_tip(Some(CheckpointVerifiedBlock::from(block(3)).into()));
    gossip
        .peer_set
        .expect_request(Request::AdvertiseBlock(without_size(&block(3)), None))
        .await
        .respond(Response::Nil);
    slow.respond(Response::Nil);
    gossip.peer_set.expect_no_requests().await;
}

#[tokio::test(start_paused = true)]
async fn mined_block_broadcast_timeout_uses_committed_tip_fallback() {
    let (mut sender, mut gossip) = setup().await;
    local_commit(&mut sender, block(2));
    let slow = gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block(2))))
        .await;
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT + Duration::from_secs(1)).await;
    drop(slow);
    gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block(2))))
        .await
        .respond(Response::Nil);
}

#[tokio::test(start_paused = true)]
async fn early_broadcast_does_not_suppress_the_committed_tip_fallback() {
    let (mut sender, mut gossip) = setup().await;
    let local = block(2);
    gossip
        .early_sender
        .send(EarlyMinedBlock {
            admission: zakura_state::BlockAdmission::pending(),
            hash: local.hash(),
            height: local.coinbase_height().unwrap(),
            size: local.zcash_serialized_size(),
            submitted_at: std::time::Instant::now(),
            submission: MinedBlockSubmission::FullBlock,
            pending: PendingBlockSignal::valid_for_tests(),
        })
        .unwrap();
    gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&local)))
        .await
        .respond(Response::Nil);
    local_commit(&mut sender, local.clone());
    let slow = gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&local)))
        .await;
    tokio::time::sleep(BLOCK_GOSSIP_TIMEOUT + Duration::from_secs(1)).await;
    drop(slow);
    gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&local)))
        .await
        .respond(Response::Nil);
}

#[tokio::test(start_paused = true)]
async fn closed_early_channel_does_not_block_committed_announcements() {
    let (
        mut sender,
        GossipTestSetup {
            mut peer_set,
            early_sender,
            task: _task,
            recent_syncs: _recent_syncs,
        },
    ) = setup().await;
    drop(early_sender);
    local_commit(&mut sender, block(2));
    peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&block(2))))
        .await
        .respond(Response::Nil);
}

#[tokio::test(start_paused = true)]
async fn closing_state_publication_ends_gossip() {
    let (sender, gossip) = setup().await;
    drop(sender);
    assert!(matches!(
        gossip.task.await.unwrap(),
        Err(BlockGossipError::AnnouncementsClosed)
    ));
}

/// The real writer publishes the local announcement before its caller can resume. Holding
/// that caller after commit models an RPC lifecycle that never reaches its response handling.
#[tokio::test(flavor = "multi_thread")]
async fn real_writer_announces_while_commit_caller_is_suspended() {
    let _init = zakura_test::init();
    let zakura_state::InitializedState {
        state,
        read_state: read,
        latest_chain_tip: latest,
        block_announcements: announcements,
        ..
    } = zakura_state::init(StateConfig::ephemeral(), &Mainnet, Height::MAX, 0)
        .await
        .unwrap();
    let state = tower::util::BoxCloneSyncService::new(tower::buffer::Buffer::new(state, 1));
    state
        .clone()
        .oneshot(zakura_state::Request::CommitCheckpointVerifiedBlock(
            block(0).into(),
        ))
        .await
        .unwrap();
    let mut gossip = start(
        latest,
        announcements,
        BoxCloneService::new(read.clone()),
        true,
    );
    gossip
        .peer_set
        .expect_request(Request::AdvertiseBlock(with_size(&block(0)), None))
        .await
        .respond(Response::Nil);
    let local = block(1);
    let checkpoint =
        CheckpointVerifiedBlock::from(local.clone()).with_origin(BlockOrigin::LocalSubmission {
            size: local.zcash_serialized_size(),
        });
    let (committed, received) = tokio::sync::oneshot::channel();
    let caller_state = state.clone();
    let caller = tokio::spawn(async move {
        caller_state
            .oneshot(zakura_state::Request::CommitCheckpointVerifiedBlock(
                checkpoint,
            ))
            .await
            .unwrap();
        committed.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    timeout(Duration::from_secs(10), received)
        .await
        .unwrap()
        .unwrap();
    gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&local)))
        .await
        .respond(Response::Nil);
    let info = read
        .oneshot(zakura_state::ReadRequest::BlockInfo(local.hash().into()))
        .await
        .unwrap();
    assert!(matches!(
        info,
        zakura_state::ReadResponse::BlockInfo(Some(_))
    ));
    assert!(!caller.is_finished());
    caller.abort();
    gossip.peer_set.expect_no_requests().await;
}

#[tokio::test(start_paused = true)]
async fn ordinary_relay_pauses_when_sync_falls_behind_and_local_commits_continue() {
    let (mut sender, mut gossip) = setup().await;
    tokio::task::yield_now().await;
    for _ in 0..sync::RecentSyncLengths::MAX_RECENT_LENGTHS {
        gossip.recent_syncs.push_extend_tips_length(1_000);
    }
    sender.set_finalized_tip(zakura_state::ChainTipBlock::from(
        CheckpointVerifiedBlock::from(block(2)),
    ));
    gossip.peer_set.expect_no_requests().await;
    // A successful side-chain submission bypasses the ordinary relay gate.
    let local = block(3);
    sender.publish_finalized_commit(
        CheckpointVerifiedBlock::from(block(2)).into(),
        CommittedBlockAnnouncement {
            hash: local.hash(),
            height: Height(3),
            origin: BlockOrigin::LocalSubmission {
                size: local.zcash_serialized_size(),
            },
        },
    );
    gossip
        .peer_set
        .expect_request(Request::AdvertiseMinedBlock(with_size(&local)))
        .await
        .respond(Response::Nil);
    SyncStatus::sync_close_to_tip(&mut gossip.recent_syncs);
    gossip
        .peer_set
        .expect_request(Request::AdvertiseBlock(without_size(&block(2)), None))
        .await
        .respond(Response::Nil);
}
