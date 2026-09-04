//! Executable regulated-load contract for inbound `GetBlocks` serving.
//!
//! These tests use the same service, peer routine, reactor, driver events, and
//! framed queues as production. Small capacities make each ownership boundary
//! observable without turning the deterministic contract lane into a hardware
//! benchmark. Ignored tests at the bottom exercise the same rules over native
//! QUIC and report the measurements used to validate production limits.

use proptest::{prelude::*, test_runner::TestCaseError};
use std::{
    fmt::Display,
    future::Future,
    time::{Duration, Instant},
};

use futures::future::join_all;
use tokio::{runtime::Builder, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use zakura_chain::{block, parameters::Network};

use super::super::super::{
    config::{GET_BLOCKS_REQUEST_OVERHEAD_BYTES, GET_BLOCKS_TERMINAL_PAYLOAD_BYTES},
    events::RoutineToReactor,
    peer_routine::{admit_and_forward_get_blocks, ServingAdmissionOutcome},
    serving_regulation::{
        pending_input_capacity, pending_input_capacity_per_session, serving_cost,
        GetBlocksServingRegulator,
    },
    spawn_block_sync_reactor, test_block_apply_outcome, BlockApplyResult, BlockRangeRequestId,
    BlockSyncAction, BlockSyncEvent, BlockSyncFrontiers, BlockSyncHandle, BlockSyncMessage,
    BlockSyncMisbehavior, BlockSyncStartup, BlockSyncStatus, GetBlocksServingRegulationConfig,
    ZakuraBlockSyncConfig, MAX_BS_RESPONSE_BYTES, ZAKURA_CAP_BLOCK_SYNC, ZAKURA_STREAM_BLOCK_SYNC,
};
use crate::zakura::{
    testkit::{
        await_until, HostilePeer, MockApplyFrontier, SyntheticBlockCorpus, SyntheticBlockShape,
        SyntheticBlockSyncPeer, SyntheticBlockSyncPeers, ZakuraTestNode,
    },
    FullStateFrontiers, ServicePeerDirection, ServicePeerLimits, ZakuraLocalLimits, ZakuraPeerId,
};
use crate::{BoxError, Config};

use super::runner::{assert_contract_test_manifest, GeneratedTestConfig};

const TIP: block::Height = block::Height(16);
const WAIT_ATTEMPTS: usize = 512;
const TEST_BARRIER_TIMEOUT: Duration = Duration::from_secs(1);

const GB_RL_TEST_MANIFEST: &[(&str, &[&str])] = &[
    ("GB-RL-01", &["gb_rl_01_charge_matches_declared_formula"]),
    (
        "GB-RL-02",
        &["gb_rl_02_blocked_request_bounds_queue_and_is_admitted_once_after_release"],
    ),
    (
        "GB-RL-03",
        &["gb_rl_03_attempt_rolls_back_and_commit_keeps_overhead"],
    ),
    (
        "GB-RL-04",
        &["gb_rl_04_rejections_settle_once_and_account_their_terminal_frame"],
    ),
    (
        "GB-RL-05",
        &["gb_rl_05_peer_rate_backlog_and_ledger_are_isolated"],
    ),
    (
        "GB-RL-06",
        &["gb_rl_06_backlog_never_overshoots_and_draining_resumes_work"],
    ),
    (
        "GB-RL-07",
        &["gb_rl_07_stalled_outstanding_bytes_do_not_refill_with_time"],
    ),
    (
        "GB-RL-08",
        &["gb_rl_08_handoff_failures_hold_rollback_or_settle_exactly_once"],
    ),
    (
        "GB-RL-09",
        &["gb_rl_09_session_end_settles_permit_but_frame_leases_survive_until_drop"],
    ),
    (
        "GB-RL-10a",
        &["gb_rl_10a_generated_hostile_flood_stays_within_all_declared_bounds"],
    ),
    (
        "GB-RL-10b",
        &["gb_rl_10b_native_reading_flood_preserves_honest_tiny_and_full_service"],
    ),
    (
        "GB-RL-10c",
        &[
            "gb_rl_10c_native_stopped_readers_stay_bounded_reclaim_and_restore_service",
            "gb_rl_10c_quic_send_windows_fit_node_transport_envelope",
        ],
    ),
    (
        "GB-RL-11",
        &["gb_rl_11_pipelined_serving_requests_keep_same_stream_download_live"],
    ),
    (
        "GB-RL-12",
        &["gb_rl_12_supported_configuration_covers_largest_request"],
    ),
    (
        "GB-RL-13",
        &["gb_rl_13_under_budget_histories_match_pre_regulation_reference_model"],
    ),
    (
        "GB-RL-14",
        &["gb_rl_14_reconnect_retains_rate_bucket_and_bounds_inactive_cache"],
    ),
    (
        "GB-RL-15",
        &["gb_rl_15_stale_session_gate_rolls_back_regulation_ownership"],
    ),
    (
        "GB-RL-16",
        &["gb_rl_16_pending_requests_stay_within_session_and_node_bounds"],
    ),
];

#[test]
fn gb_rl_contract_manifest_names_every_requirement() {
    const EXPECTED_IDS: &[&str] = &[
        "GB-RL-01",
        "GB-RL-02",
        "GB-RL-03",
        "GB-RL-04",
        "GB-RL-05",
        "GB-RL-06",
        "GB-RL-07",
        "GB-RL-08",
        "GB-RL-09",
        "GB-RL-10a",
        "GB-RL-10b",
        "GB-RL-10c",
        "GB-RL-11",
        "GB-RL-12",
        "GB-RL-13",
        "GB-RL-14",
        "GB-RL-15",
        "GB-RL-16",
    ];
    assert_contract_test_manifest(EXPECTED_IDS, GB_RL_TEST_MANIFEST);
}

#[derive(Copy, Clone, Debug)]
struct ServingQuery {
    request_id: BlockRangeRequestId,
    start: block::Height,
    count: u32,
}

struct ServingHarness {
    handle: BlockSyncHandle,
    actions: mpsc::Receiver<BlockSyncAction>,
    peers: SyntheticBlockSyncPeers,
    reactor: JoinHandle<()>,
}

impl ServingHarness {
    fn new(config: ZakuraBlockSyncConfig, queue_depth: usize, max_connections: usize) -> Self {
        let tip = (TIP, block::Hash([0x71; 32]));
        let (_tip_tx, tip_rx) = tokio::sync::watch::channel(tip);
        let startup = BlockSyncStartup::new(
            BlockSyncFrontiers {
                finalized_height: tip.0,
                verified_block_tip: tip.0,
                verified_block_hash: tip.1,
            },
            tip,
            tip_rx,
            config.clone(),
        )
        .with_max_connections(max_connections);
        let (handle, actions, reactor) = spawn_block_sync_reactor(startup);
        let peers = SyntheticBlockSyncPeers::new(config, handle.clone(), queue_depth);
        Self {
            handle,
            actions,
            peers,
            reactor,
        }
    }

    async fn connect_ready(&self, peer_id: ZakuraPeerId, conn_id: u64) -> SyntheticBlockSyncPeer {
        let mut peer = self
            .peers
            .connect_peer(peer_id.clone(), conn_id, ServicePeerDirection::Outbound)
            .expect("the synthetic block-sync session connects");
        wait_until("reactor admits synthetic peer", || {
            self.handle.peer_snapshot().outbound_peers > 0
        })
        .await;
        expect_initial_status(&mut peer).await;
        peer.try_send(BlockSyncMessage::Status(serving_status()))
            .expect("the peer Status enters the real framed queue");
        let registry = &self
            .handle
            .routine_wiring
            .as_ref()
            .expect("a spawned reactor exposes routine wiring")
            .registry;
        wait_until("peer Status reaches the production registry", || {
            registry.has_received_status(&peer_id)
        })
        .await;
        drain_outbound_status(&mut peer).await;
        peer
    }
}

impl Drop for ServingHarness {
    fn drop(&mut self) {
        self.reactor.abort();
    }
}

fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("32-byte test peer id is valid")
}

fn serving_status() -> BlockSyncStatus {
    BlockSyncStatus {
        servable_low: block::Height::MIN,
        servable_high: TIP,
        tip_hash: block::Hash([0x71; 32]),
        max_blocks_per_response: 128,
        max_inflight_requests: 64,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
    }
}

fn regulated_config(response_bytes: u32) -> ZakuraBlockSyncConfig {
    let mut config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 1,
        max_inflight_requests: 64,
        max_response_bytes: response_bytes,
        ..ZakuraBlockSyncConfig::default()
    };
    let cost = serving_cost(&config, 1).expect("one-block serving arithmetic fits");
    config.get_blocks_serving_regulation = GetBlocksServingRegulationConfig {
        peer_rate_bytes_per_second: 1,
        peer_rate_capacity_bytes: cost.charge * 8,
        peer_backlog_bytes: cost.response_cap * 8,
        node_rate_bytes_per_second: 1,
        node_rate_capacity_bytes: cost.charge * 32,
        node_outstanding_bytes: cost.response_cap * 32,
    };
    assert!(config.validate().is_ok());
    config
}

async fn wait_until(label: &str, mut predicate: impl FnMut() -> bool) {
    for _ in 0..WAIT_ATTEMPTS {
        if predicate() {
            return;
        }
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for {label}");
}

async fn await_test_barrier<E>(
    label: &str,
    barrier: impl Future<Output = Result<(), E>>,
) -> Result<(), String>
where
    E: Display,
{
    match tokio::time::timeout(TEST_BARRIER_TIMEOUT, barrier).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(format!("{label} failed: {error}")),
        Err(_) => Err(format!("{label} timed out after {TEST_BARRIER_TIMEOUT:?}")),
    }
}

/// Order queued reactor and peer inputs without treating task yields as quiescence.
async fn synchronize_serving_inputs<'a>(
    handle: &BlockSyncHandle,
    peers: impl IntoIterator<Item = &'a SyntheticBlockSyncPeer>,
) -> Result<(), String> {
    await_test_barrier("reactor pre-frame barrier", handle.barrier_for_test()).await?;
    for peer in peers {
        let label = format!("peer routine barrier for {:?}", peer.peer_id());
        await_test_barrier(&label, peer.barrier_for_test()).await?;
    }
    await_test_barrier("reactor post-frame barrier", handle.barrier_for_test()).await
}

async fn drain_outbound_status(peer: &mut SyntheticBlockSyncPeer) {
    loop {
        let message = peer
            .recv_timeout(std::time::Duration::from_nanos(1))
            .await
            .expect("outbound block-sync frames remain decodable");
        match message {
            Some(BlockSyncMessage::Status(_)) => {}
            Some(message) => panic!("expected only startup Status, got {message:?}"),
            None => return,
        }
    }
}

async fn expect_initial_status(peer: &mut SyntheticBlockSyncPeer) {
    let message = peer
        .recv_timeout(std::time::Duration::from_secs(1))
        .await
        .expect("the startup block-sync frame remains decodable");
    assert!(
        matches!(message, Some(BlockSyncMessage::Status(_))),
        "an admitted block-sync session receives the local Status first"
    );
    drain_outbound_status(peer).await;
}

async fn next_contract_action(actions: &mut mpsc::Receiver<BlockSyncAction>) -> BlockSyncAction {
    for _ in 0..WAIT_ATTEMPTS {
        while let Ok(action) = actions.try_recv() {
            if !matches!(action, BlockSyncAction::QueryNeededBlocks { .. }) {
                return action;
            }
        }
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for a block-sync contract action");
}

fn query_from(action: BlockSyncAction, expected_peer: &ZakuraPeerId) -> ServingQuery {
    match action {
        BlockSyncAction::QueryBlocksByHeightRange {
            request_id,
            peer,
            start,
            count,
        } => {
            assert_eq!(&peer, expected_peer);
            ServingQuery {
                request_id,
                start,
                count,
            }
        }
        action => panic!("expected a serving state query, got {action:?}"),
    }
}

async fn finish_unavailable(handle: &BlockSyncHandle, peer: &ZakuraPeerId, query: ServingQuery) {
    handle
        .send(BlockSyncEvent::BlockRangeResponseFinished {
            request_id: query.request_id,
            peer: peer.clone(),
            start_height: query.start,
            requested_count: query.count,
            returned_count: 0,
        })
        .await
        .expect("the real reactor accepts the controlled driver result");
}

#[tokio::test(start_paused = true)]
async fn gb_rl_02_blocked_request_bounds_queue_and_is_admitted_once_after_release() {
    let mut config = regulated_config(512);
    config.max_inflight_requests = 1;
    let cost = serving_cost(&config, 1).expect("test request cost is valid");
    config.get_blocks_serving_regulation.peer_backlog_bytes = cost.response_cap;
    config.get_blocks_serving_regulation.node_outstanding_bytes = cost.response_cap;
    let mut harness = ServingHarness::new(config, 4, 1);
    let peer_id = peer(0x02);
    let mut remote = harness.connect_ready(peer_id.clone(), 2).await;

    remote
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1,
        })
        .expect("the first request queues");
    let first = query_from(next_contract_action(&mut harness.actions).await, &peer_id);

    remote
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(2),
            count: 1,
        })
        .expect("the blocked request queues");
    wait_until(
        "the blocked request is removed from the inbound queue",
        || remote.inbound_capacity() == 4,
    )
    .await;
    remote
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(3),
            count: 1,
        })
        .expect("one request queues behind the pending one");
    wait_until("the bounded serving queue holds one request", || {
        remote.inbound_capacity() == 4
    })
    .await;
    remote
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(4),
            count: 1,
        })
        .expect("an over-window request reaches the routine");
    wait_until("the routine drops the over-window request", || {
        remote.inbound_capacity() == 4
    })
    .await;
    assert_eq!(
        remote.inbound_capacity(),
        4,
        "the routine keeps decoding after its bounded serving queue fills"
    );
    assert!(
        harness.actions.try_recv().is_err(),
        "dropping an over-window request must not query or score the peer"
    );

    finish_unavailable(&harness.handle, &peer_id, first).await;
    wait_until("the first terminal frame owns its lease", || {
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding
            == GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
    })
    .await;
    assert!(harness.actions.try_recv().is_err());
    assert!(matches!(
        remote.recv().await.expect("terminal frame decodes"),
        Some(BlockSyncMessage::RangeUnavailable { .. })
    ));

    let second = query_from(next_contract_action(&mut harness.actions).await, &peer_id);
    assert_eq!(second.start, block::Height(2));
    finish_unavailable(&harness.handle, &peer_id, second).await;
    assert!(matches!(
        remote.recv().await.expect("terminal frame decodes"),
        Some(BlockSyncMessage::RangeUnavailable { .. })
    ));

    let third = query_from(next_contract_action(&mut harness.actions).await, &peer_id);
    assert_eq!(third.start, block::Height(3));
    finish_unavailable(&harness.handle, &peer_id, third).await;
    assert!(matches!(
        remote.recv().await.expect("terminal frame decodes"),
        Some(BlockSyncMessage::RangeUnavailable { .. })
    ));

    synchronize_serving_inputs(&harness.handle, [&remote])
        .await
        .expect("the serving path reaches its explicit observation barrier");
    wait_until("all accepted serving requests settle", || {
        let snapshot = harness.handle.serving_regulation_snapshot();
        snapshot.pending_inputs == 0 && snapshot.node_outstanding == 0
    })
    .await;
    assert!(
        harness.actions.try_recv().is_err(),
        "the dropped over-window request must not be admitted later"
    );
}

#[tokio::test(start_paused = true)]
async fn gb_rl_16_pending_requests_stay_within_session_and_node_bounds() {
    let mut config = regulated_config(512);
    config.max_inflight_requests = 2;
    let cost = serving_cost(&config, 1).expect("test request cost is valid");
    config.get_blocks_serving_regulation.node_outstanding_bytes = cost.response_cap;

    let max_connections = 2;
    let mut harness = ServingHarness::new(config, 16, max_connections);
    let peer_a = peer(0x16);
    let peer_b = peer(0x17);
    let remote_a = harness.connect_ready(peer_a.clone(), 16).await;
    let remote_b = harness.connect_ready(peer_b, 17).await;

    remote_a
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1,
        })
        .expect("the request that saturates response ownership queues");
    let _held_query = query_from(next_contract_action(&mut harness.actions).await, &peer_a);

    // Each session may retain one active admission plus two requests behind it.
    // A fourth request is decoded and dropped, proving the bound does not rely
    // on transport backpressure.
    for remote in [&remote_a, &remote_b] {
        for start in 2..=5 {
            remote
                .try_send(BlockSyncMessage::GetBlocks {
                    start_height: block::Height(start),
                    count: 1,
                })
                .expect("the bounded pending-request flood enters the peer queue");
        }
        remote
            .barrier_for_test()
            .await
            .expect("the peer routine processes every preceding request");
    }

    let snapshot = harness.handle.serving_regulation_snapshot();
    assert_eq!(snapshot.session_pending_input_capacity, 3);
    assert_eq!(snapshot.max_session_pending_inputs, 3);
    assert_eq!(snapshot.pending_input_capacity, 3 * max_connections);
    assert_eq!(snapshot.pending_inputs, snapshot.pending_input_capacity);
    assert_eq!(
        snapshot.aggregate_session_pending_inputs, snapshot.pending_inputs,
        "every node permit must remain attributable to a session"
    );
    assert!(
        harness.actions.try_recv().is_err(),
        "pending or excess requests must not reach the state driver"
    );

    remote_a.cancel();
    remote_b.cancel();
    wait_until(
        "session end releases pending requests and response ownership",
        || {
            let snapshot = harness.handle.serving_regulation_snapshot();
            snapshot.pending_inputs == 0 && snapshot.node_outstanding == 0
        },
    )
    .await;
    let released = harness.handle.serving_regulation_snapshot();
    assert_eq!(released.aggregate_session_pending_inputs, 0);
    assert_eq!(released.max_session_pending_inputs, 0);
}

#[tokio::test(start_paused = true)]
async fn gb_rl_04_rejections_settle_once_and_account_their_terminal_frame() {
    let config = regulated_config(512);
    let cost = serving_cost(&config, 1).expect("test request cost is valid");
    let mut harness = ServingHarness::new(config, 4, 2);

    let no_status_id = peer(0x40);
    let mut no_status = harness
        .peers
        .connect_peer(no_status_id.clone(), 40, ServicePeerDirection::Outbound)
        .expect("the no-Status peer connects");
    wait_until("the no-Status peer is admitted", || {
        harness.handle.peer_snapshot().outbound_peers == 1
    })
    .await;
    expect_initial_status(&mut no_status).await;
    let no_status_balance = harness
        .handle
        .serving_peer_rate_balance(&no_status_id)
        .expect("the admitted peer owns a rate bucket");
    no_status
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1,
        })
        .expect("the no-Status request queues");
    match next_contract_action(&mut harness.actions).await {
        BlockSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, no_status_id);
            assert_eq!(reason, BlockSyncMisbehavior::GetBlocksSpam);
        }
        action => panic!("expected GetBlocksSpam, got {action:?}"),
    }
    assert_eq!(
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding,
        0
    );
    assert_eq!(
        harness
            .handle
            .serving_peer_rate_balance(&no_status_id)
            .expect("the peer bucket is retained"),
        no_status_balance - GET_BLOCKS_REQUEST_OVERHEAD_BYTES
    );

    let above_tip_id = peer(0x41);
    let mut above_tip = harness.connect_ready(above_tip_id.clone(), 41).await;
    let above_tip_balance = harness
        .handle
        .serving_peer_rate_balance(&above_tip_id)
        .expect("the admitted peer owns a rate bucket");
    above_tip
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(TIP.0 + 1),
            count: 1,
        })
        .expect("the above-tip request queues");
    wait_until("the rejection queues a leased terminal", || {
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding
            == GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
    })
    .await;
    assert!(matches!(
        above_tip.recv().await.expect("terminal frame decodes"),
        Some(BlockSyncMessage::RangeUnavailable { .. })
    ));
    wait_until("the above-tip terminal frame releases its lease", || {
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding
            == 0
    })
    .await;
    let settled_balance = harness
        .handle
        .serving_peer_rate_balance(&above_tip_id)
        .expect("the peer bucket is retained");
    assert_eq!(
        settled_balance,
        above_tip_balance - GET_BLOCKS_REQUEST_OVERHEAD_BYTES - GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
    );
    assert_eq!(
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding,
        0
    );
    synchronize_serving_inputs(&harness.handle, [&above_tip])
        .await
        .expect("the rejection path reaches its explicit observation barrier");
    assert_eq!(
        harness.handle.serving_peer_rate_balance(&above_tip_id),
        Some(settled_balance),
        "settlement must not run twice"
    );
    assert!(cost.charge > GET_BLOCKS_REQUEST_OVERHEAD_BYTES);
}

#[tokio::test(start_paused = true)]
async fn gb_rl_05_peer_rate_backlog_and_ledger_are_isolated() {
    let mut config = regulated_config(512);
    let cost = serving_cost(&config, 1).expect("test request cost is valid");
    config.get_blocks_serving_regulation.peer_backlog_bytes = cost.response_cap;
    config.get_blocks_serving_regulation.node_outstanding_bytes = cost.response_cap * 2;
    let mut harness = ServingHarness::new(config, 8, 2);
    let peer_a = peer(0x51);
    let peer_b = peer(0x52);
    let a = harness.connect_ready(peer_a.clone(), 51).await;
    let b = harness.connect_ready(peer_b.clone(), 52).await;

    a.try_send(BlockSyncMessage::GetBlocks {
        start_height: block::Height(1),
        count: 1,
    })
    .expect("peer A's first request queues");
    let first_a = query_from(next_contract_action(&mut harness.actions).await, &peer_a);
    a.try_send(BlockSyncMessage::GetBlocks {
        start_height: block::Height(2),
        count: 1,
    })
    .expect("peer A's blocked request queues");
    b.try_send(BlockSyncMessage::GetBlocks {
        start_height: block::Height(3),
        count: 1,
    })
    .expect("peer B's request queues independently");

    let first_b = query_from(next_contract_action(&mut harness.actions).await, &peer_b);
    assert_eq!(first_b.start, block::Height(3));
    assert_eq!(
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding,
        cost.response_cap * 2
    );
    assert_eq!(
        harness
            .handle
            .serving_peer_rate_balance(&peer_b)
            .expect("peer B has its own rate bucket"),
        harness
            .handle
            .serving_peer_rate_balance(&peer_a)
            .expect("peer A has its own rate bucket")
    );
    finish_unavailable(&harness.handle, &peer_a, first_a).await;
}

#[tokio::test(start_paused = true)]
async fn gb_rl_06_backlog_never_overshoots_and_draining_resumes_work() {
    let corpus = SyntheticBlockCorpus::generate(
        1,
        0x6006,
        SyntheticBlockShape {
            target_block_bytes: Some(64 * 1024),
        },
    );
    let body = corpus
        .block_at(block::Height(1))
        .expect("the synthetic body exists");
    let body_size = corpus
        .size_at(block::Height(1))
        .expect("the synthetic body has a serialized size");
    let mut config = regulated_config(u32::try_from(body_size).expect("test body size fits u32"));
    let cost = serving_cost(&config, 1).expect("test request cost is valid");
    config.get_blocks_serving_regulation.peer_backlog_bytes = cost.response_cap;
    config.get_blocks_serving_regulation.node_outstanding_bytes = cost.response_cap;
    let mut harness = ServingHarness::new(config, 1, 1);
    let peer_id = peer(0x60);
    let mut remote = harness.connect_ready(peer_id.clone(), 60).await;

    remote
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1,
        })
        .expect("the first request queues");
    let first = query_from(next_contract_action(&mut harness.actions).await, &peer_id);
    remote
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(2),
            count: 1,
        })
        .expect("the next request waits behind backlog");

    harness
        .handle
        .send(BlockSyncEvent::BlockRangeResponseReady {
            request_id: first.request_id,
            peer: peer_id.clone(),
            start_height: first.start,
            requested_count: first.count,
            blocks: vec![(block::Height(1), body.clone(), body_size)],
        })
        .await
        .expect("the real reactor accepts the controlled body response");
    let block_payload_bytes = u64::try_from(
        BlockSyncMessage::Block(body)
            .encode_frame()
            .expect("the synthetic block encodes")
            .payload
            .len(),
    )
    .expect("frame length fits u64");
    wait_until("the stopped reader holds the block lease", || {
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding
            == block_payload_bytes
    })
    .await;
    let snapshot = harness.handle.serving_regulation_snapshot();
    assert!(snapshot.node_outstanding <= snapshot.node_outstanding_capacity);
    assert!(snapshot.max_peer_backlog <= cost.response_cap);
    assert!(
        harness.actions.try_recv().is_err(),
        "stopped reader admitted new work"
    );

    assert!(matches!(
        remote.recv().await.expect("queued block decodes"),
        Some(BlockSyncMessage::Block(_))
    ));
    let resumed = query_from(next_contract_action(&mut harness.actions).await, &peer_id);
    assert_eq!(resumed.start, block::Height(2));
}

#[tokio::test(start_paused = true)]
async fn gb_rl_08_handoff_failures_hold_rollback_or_settle_exactly_once() {
    let config = regulated_config(512);
    let cost = serving_cost(&config, 1).expect("test request cost is valid");
    let regulator = GetBlocksServingRegulator::new(config.clone(), 1);
    let peer_id = peer(0x80);
    let session = regulator.session(peer_id.clone(), 80);
    let before = regulator.snapshot();

    let (routine_tx, mut routine_rx) = mpsc::channel(1);
    routine_tx
        .try_send(RoutineToReactor::RequeryNeeded)
        .expect("the routine channel starts full");
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let request = session
        .try_retain_input(block::Height(1), 1)
        .expect("the first decoded request fits its pending-input bounds");
    let task = tokio::spawn(admit_and_forward_get_blocks(
        session.clone(),
        routine_tx,
        peer_id.clone(),
        request,
        CancellationToken::new(),
        done_tx,
    ));
    wait_until("the full handoff retains its admission ownership", || {
        regulator.snapshot().node_outstanding == cost.response_cap
    })
    .await;
    assert!(!task.is_finished());
    assert_eq!(regulator.snapshot().node_outstanding, cost.response_cap);
    assert!(matches!(
        routine_rx.recv().await,
        Some(RoutineToReactor::RequeryNeeded)
    ));
    let request = routine_rx
        .recv()
        .await
        .expect("held attempt sends once capacity returns");
    assert!(matches!(request, RoutineToReactor::ServeGetBlocks { .. }));
    assert_eq!(
        done_rx.await.expect("admission task reports its outcome"),
        ServingAdmissionOutcome::Sent
    );
    drop(request);
    task.await.expect("admission task does not panic");
    assert_eq!(regulator.snapshot(), before);

    // Cancellation wins even when the routine channel has an immediately
    // available slot, so an ending session cannot forward newly admitted work.
    let (cancelled_tx, mut cancelled_rx) = mpsc::channel(1);
    let (cancelled_done_tx, cancelled_done_rx) = tokio::sync::oneshot::channel();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let request = session
        .try_retain_input(block::Height(1), 1)
        .expect("the cancelled decoded request fits its pending-input bounds");
    admit_and_forward_get_blocks(
        session.clone(),
        cancelled_tx,
        peer_id.clone(),
        request,
        cancelled,
        cancelled_done_tx,
    )
    .await;
    assert_eq!(
        cancelled_done_rx
            .await
            .expect("cancelled admission reports its outcome"),
        ServingAdmissionOutcome::Cancelled
    );
    assert!(cancelled_rx.try_recv().is_err());
    assert_eq!(regulator.snapshot(), before);

    let (closed_tx, closed_rx) = mpsc::channel(1);
    drop(closed_rx);
    let (closed_done_tx, closed_done_rx) = tokio::sync::oneshot::channel();
    let request = session
        .try_retain_input(block::Height(1), 1)
        .expect("the closed-channel request fits its pending-input bounds");
    admit_and_forward_get_blocks(
        session,
        closed_tx,
        peer_id,
        request,
        CancellationToken::new(),
        closed_done_tx,
    )
    .await;
    assert_eq!(
        closed_done_rx
            .await
            .expect("closed-channel outcome is reported"),
        ServingAdmissionOutcome::ChannelClosed
    );
    let after_closed = regulator.snapshot();
    assert_eq!(after_closed.node_rate_balance, before.node_rate_balance);
    assert_eq!(after_closed.node_outstanding, before.node_outstanding);

    // Fill the real driver channel. A committed request must receive a terminal
    // response rather than disappearing when its state query cannot be queued.
    let mut harness = ServingHarness::new(config, 4, 1);
    let peer_id = peer(0x81);
    let mut remote = harness.connect_ready(peer_id.clone(), 81).await;
    let actions_tx = harness
        .handle
        .routine_wiring
        .as_ref()
        .expect("the spawned reactor exposes routine wiring")
        .actions
        .clone();
    while actions_tx
        .try_send(BlockSyncAction::Misbehavior {
            peer: peer_id.clone(),
            reason: BlockSyncMisbehavior::InvalidBlock,
        })
        .is_ok()
    {}
    remote
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1,
        })
        .expect("the request enters the routine while the driver channel is full");
    wait_until("action-channel rejection queues a terminal frame", || {
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding
            == GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
    })
    .await;
    assert!(matches!(
        remote.recv().await.expect("terminal frame decodes"),
        Some(BlockSyncMessage::RangeUnavailable { .. })
    ));
    wait_until(
        "the action-channel rejection releases its terminal lease",
        || {
            harness
                .handle
                .serving_regulation_snapshot()
                .node_outstanding
                == 0
        },
    )
    .await;
    assert_eq!(
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding,
        0
    );

    // The driver's common failure/timeout result follows the same regulated
    // terminal and settlement path.
    while harness.actions.try_recv().is_ok() {}
    remote
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(2),
            count: 1,
        })
        .expect("the driver-failure request enters the routine");
    let failed = query_from(next_contract_action(&mut harness.actions).await, &peer_id);
    finish_unavailable(&harness.handle, &peer_id, failed).await;
    assert!(matches!(
        remote
            .recv()
            .await
            .expect("driver-failure terminal decodes"),
        Some(BlockSyncMessage::RangeUnavailable { .. })
    ));
    wait_until("the driver-failure terminal releases its lease", || {
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding
            == 0
    })
    .await;
    assert_eq!(
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding,
        0
    );
}

#[tokio::test(start_paused = true)]
async fn gb_rl_09_session_end_settles_permit_but_frame_leases_survive_until_drop() {
    let corpus = SyntheticBlockCorpus::generate(1, 0x9009, SyntheticBlockShape::default());
    let body = corpus
        .block_at(block::Height(1))
        .expect("the synthetic body exists");
    let body_size = corpus
        .size_at(block::Height(1))
        .expect("the synthetic body has a size");
    let config = regulated_config(MAX_BS_RESPONSE_BYTES);
    let mut harness = ServingHarness::new(config, 4, 2);
    let peer_id = peer(0x90);
    let mut older = harness.connect_ready(peer_id.clone(), 90).await;
    older
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1,
        })
        .expect("the old session request queues");
    let query = query_from(next_contract_action(&mut harness.actions).await, &peer_id);
    harness
        .handle
        .send(BlockSyncEvent::BlockRangeResponseReady {
            request_id: query.request_id,
            peer: peer_id.clone(),
            start_height: query.start,
            requested_count: query.count,
            blocks: vec![(block::Height(1), body.clone(), body_size)],
        })
        .await
        .expect("the controlled response reaches the real reactor");
    let queued_bytes = u64::try_from(
        BlockSyncMessage::Block(body)
            .encode_frame()
            .expect("the synthetic body encodes")
            .payload
            .len(),
    )
    .expect("frame length fits u64")
        + GET_BLOCKS_TERMINAL_PAYLOAD_BYTES;
    wait_until("both old-session frames hold leases", || {
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding
            == queued_bytes
    })
    .await;
    let rate_after_response = harness
        .handle
        .serving_peer_rate_balance(&peer_id)
        .expect("the identity rate bucket remains cached");

    let mut replacement = harness
        .peers
        .connect_peer(peer_id.clone(), 91, ServicePeerDirection::Outbound)
        .expect("the replacement session connects");
    wait_until("the replacement cancels the old session", || {
        older.cancel_token().is_cancelled()
    })
    .await;
    assert_eq!(
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding,
        queued_bytes,
        "queued frame leases remain owned after their session ends"
    );

    harness
        .handle
        .send(BlockSyncEvent::BlockRangeResponseFinished {
            request_id: query.request_id,
            peer: peer_id.clone(),
            start_height: query.start,
            requested_count: query.count,
            returned_count: 0,
        })
        .await
        .expect("the stale completion reaches the reactor");
    await_test_barrier(
        "stale completion reactor barrier",
        harness.handle.barrier_for_test(),
    )
    .await
    .expect("the reactor processes the stale completion before observation");
    assert_eq!(
        harness.handle.serving_peer_rate_balance(&peer_id),
        Some(rate_after_response),
        "a stale completion must not refund a retired permit"
    );
    assert_eq!(
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding,
        queued_bytes
    );

    assert!(matches!(
        older.recv().await.expect("old block decodes"),
        Some(BlockSyncMessage::Block(_))
    ));
    assert!(matches!(
        older.recv().await.expect("old terminal decodes"),
        Some(BlockSyncMessage::BlocksDone { .. })
    ));
    wait_until("dropping both old frame leases releases all bytes", || {
        harness
            .handle
            .serving_regulation_snapshot()
            .node_outstanding
            == 0
    })
    .await;
    drain_outbound_status(&mut replacement).await;
    assert!(
        replacement
            .recv_timeout(std::time::Duration::from_nanos(1))
            .await
            .expect("replacement stream remains decodable")
            .is_none(),
        "old-session responses must not reach the replacement"
    );
}

#[test]
fn gb_rl_10a_generated_hostile_flood_stays_within_all_declared_bounds() {
    const CASES_VARIABLE: &str = "ZAKURA_REGULATED_LOAD_CASES";
    const SEED_VARIABLE: &str = "ZAKURA_REGULATED_LOAD_SEED";
    let generated = GeneratedTestConfig::from_env(CASES_VARIABLE, SEED_VARIABLE, 64)
        .expect("regulated-load case and seed overrides must be valid positive numbers");
    generated.announce(
        "GetBlocks regulated-load histories",
        CASES_VARIABLE,
        SEED_VARIABLE,
    );
    let mut runner = generated.runner(file!());
    runner
        .run(&regulated_load_case_strategy(), |case| {
            replay_regulated_load_history(case.clone()).map_err(TestCaseError::fail)
        })
        .expect("generated hostile histories respect every configured serving bound");
}

#[derive(Clone, Debug)]
struct GeneratedRegulatedLoadCase {
    peer_count: usize,
    local_count_limit: u32,
    inflight_limit: u32,
    response_byte_limit: u32,
    peer_rate_slots: u64,
    peer_backlog_slots: u64,
    node_rate_slots: u64,
    node_outstanding_slots: u64,
    refill_divisor: u64,
    operations: Vec<GeneratedLoadOperation>,
}

#[derive(Copy, Clone, Debug)]
enum GeneratedLoadOperation {
    Request { peer: u8, start: u8, count: u32 },
    FinishOne { peer: u8 },
    Drain { peer: u8 },
    AdvanceMillis(u16),
    Reconnect { peer: u8 },
}

fn regulated_load_case_strategy() -> impl Strategy<Value = GeneratedRegulatedLoadCase> {
    let request_count = prop_oneof![
        Just(1u32),
        Just(2u32),
        Just(127u32),
        Just(128u32),
        1u32..=128u32,
    ];
    let operation = prop_oneof![
        6 => (any::<u8>(), any::<u8>(), request_count).prop_map(|(peer, start, count)| {
            GeneratedLoadOperation::Request { peer, start, count }
        }),
        2 => any::<u8>().prop_map(|peer| GeneratedLoadOperation::FinishOne { peer }),
        2 => any::<u8>().prop_map(|peer| GeneratedLoadOperation::Drain { peer }),
        1 => (0u16..=2_000u16).prop_map(GeneratedLoadOperation::AdvanceMillis),
        1 => any::<u8>().prop_map(|peer| GeneratedLoadOperation::Reconnect { peer }),
    ];
    (
        1usize..=8,
        prop_oneof![Just(1u32), Just(2u32), Just(127u32), Just(128u32)],
        prop_oneof![Just(1u32), Just(2u32), Just(8u32), 1u32..=8u32],
        prop_oneof![Just(1u32), Just(512u32), Just(4_096u32)],
        1u64..=3,
        1u64..=3,
        1u64..=8,
        1u64..=8,
        1u64..=4,
        proptest::collection::vec(operation, 16..=64),
    )
        .prop_map(
            |(
                peer_count,
                local_count_limit,
                inflight_limit,
                response_byte_limit,
                peer_rate_slots,
                peer_backlog_slots,
                node_rate_slots,
                node_outstanding_slots,
                refill_divisor,
                mut operations,
            )| {
                let mut required = vec![
                    GeneratedLoadOperation::Request {
                        peer: 0,
                        start: 1,
                        count: 1,
                    },
                    GeneratedLoadOperation::Request {
                        peer: 0,
                        start: 1,
                        count: 128,
                    },
                    GeneratedLoadOperation::FinishOne { peer: 0 },
                    GeneratedLoadOperation::Drain { peer: 0 },
                    GeneratedLoadOperation::AdvanceMillis(1_000),
                    GeneratedLoadOperation::Reconnect { peer: 0 },
                ];
                required.append(&mut operations);
                GeneratedRegulatedLoadCase {
                    peer_count,
                    local_count_limit,
                    inflight_limit,
                    response_byte_limit,
                    peer_rate_slots,
                    peer_backlog_slots,
                    node_rate_slots,
                    node_outstanding_slots,
                    refill_divisor,
                    operations: required,
                }
            },
        )
}

fn replay_regulated_load_history(case: GeneratedRegulatedLoadCase) -> Result<(), String> {
    Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .map_err(|error| format!("failed to build regulated-load runtime: {error}"))?
        .block_on(async move {
            let mut config = ZakuraBlockSyncConfig {
                max_blocks_per_response: case.local_count_limit,
                max_inflight_requests: case.inflight_limit,
                max_response_bytes: case.response_byte_limit,
                ..ZakuraBlockSyncConfig::default()
            };
            let largest = serving_cost(&config, 128).map_err(str::to_string)?;
            config.get_blocks_serving_regulation = GetBlocksServingRegulationConfig {
                peer_rate_bytes_per_second: largest.charge / case.refill_divisor,
                peer_rate_capacity_bytes: largest.charge * case.peer_rate_slots,
                peer_backlog_bytes: largest.response_cap * case.peer_backlog_slots,
                node_rate_bytes_per_second: largest.charge / case.refill_divisor,
                node_rate_capacity_bytes: largest.charge * case.node_rate_slots,
                node_outstanding_bytes: largest.response_cap * case.node_outstanding_slots,
            };
            if let Err(error) = super::super::super::serving_regulation::validate_config(&config) {
                return Err(format!("generated regulation config is invalid: {error}"));
            }
            let mut harness = ServingHarness::new(config.clone(), 8, case.peer_count);
            let mut remotes = Vec::with_capacity(case.peer_count);
            let mut peer_ids = Vec::with_capacity(case.peer_count);
            let mut connection_ids = Vec::with_capacity(case.peer_count);
            let mut pending = vec![Vec::<ServingQuery>::new(); case.peer_count];
            for index in 0..case.peer_count {
                let byte = u8::try_from(index + 1).map_err(|error| error.to_string())?;
                let peer_id = peer(byte);
                let remote = harness
                    .connect_ready(peer_id.clone(), u64::from(byte))
                    .await;
                peer_ids.push(peer_id);
                connection_ids.push(u64::from(byte));
                remotes.push(Some(remote));
            }

            for operation in case.operations {
                let peer_index = match operation {
                    GeneratedLoadOperation::Request { peer, .. }
                    | GeneratedLoadOperation::FinishOne { peer }
                    | GeneratedLoadOperation::Drain { peer }
                    | GeneratedLoadOperation::Reconnect { peer } => {
                        usize::from(peer) % case.peer_count
                    }
                    GeneratedLoadOperation::AdvanceMillis(_) => 0,
                };
                match operation {
                    GeneratedLoadOperation::Request { start, count, .. } => {
                        let start = block::Height(u32::from(start) % (TIP.0 + 3));
                        let remote = remotes[peer_index]
                            .as_ref()
                            .expect("every generated peer has a live remote");
                        let _ = remote.try_send(BlockSyncMessage::GetBlocks {
                            start_height: start,
                            count,
                        });
                    }
                    GeneratedLoadOperation::FinishOne { .. } => {
                        if !pending[peer_index].is_empty() {
                            let query = pending[peer_index].remove(0);
                            finish_unavailable(&harness.handle, &peer_ids[peer_index], query).await;
                        }
                    }
                    GeneratedLoadOperation::Drain { .. } => {
                        let remote = remotes[peer_index]
                            .as_mut()
                            .expect("every generated peer has a live remote");
                        while remote
                            .recv_timeout(Duration::from_nanos(1))
                            .await
                            .map_err(|error| error.to_string())?
                            .is_some()
                        {}
                    }
                    GeneratedLoadOperation::AdvanceMillis(millis) => {
                        tokio::time::advance(Duration::from_millis(u64::from(millis))).await;
                    }
                    GeneratedLoadOperation::Reconnect { .. } => {
                        let old = remotes[peer_index]
                            .take()
                            .expect("every generated peer has a live remote");
                        old.cancel();
                        drop(old);
                        connection_ids[peer_index] = connection_ids[peer_index]
                            .checked_add(256)
                            .expect("generated connection ids fit u64");
                        remotes[peer_index] = Some(
                            harness
                                .connect_ready(
                                    peer_ids[peer_index].clone(),
                                    connection_ids[peer_index],
                                )
                                .await,
                        );
                        pending[peer_index].clear();
                    }
                }

                // This lane checks invariants at a defined reactor boundary. Peer
                // reads and admission waiters may remain pending because bounded
                // backpressure is part of the generated state under test.
                await_test_barrier(
                    "generated-load reactor boundary",
                    harness.handle.barrier_for_test(),
                )
                .await?;
                while let Ok(action) = harness.actions.try_recv() {
                    match action {
                        BlockSyncAction::QueryBlocksByHeightRange {
                            request_id,
                            peer,
                            start,
                            count,
                        } => {
                            let index = peer_ids
                                .iter()
                                .position(|candidate| *candidate == peer)
                                .ok_or_else(|| format!("query for unknown peer {peer:?}"))?;
                            pending[index].push(ServingQuery {
                                request_id,
                                start,
                                count,
                            });
                        }
                        BlockSyncAction::QueryNeededBlocks { .. }
                        | BlockSyncAction::Misbehavior { .. } => {}
                        action => {
                            return Err(format!("unexpected generated load action: {action:?}"));
                        }
                    }
                }
                assert_generated_load_bounds(&harness, &config, &peer_ids, &pending)?;
            }

            for remote in remotes.into_iter().flatten() {
                remote.cancel();
            }
            wait_until("generated history releases every response owner", || {
                let snapshot = harness.handle.serving_regulation_snapshot();
                snapshot.node_outstanding == 0 && snapshot.pending_inputs == 0
            })
            .await;
            Ok(())
        })
}

fn assert_generated_load_bounds(
    harness: &ServingHarness,
    config: &ZakuraBlockSyncConfig,
    peer_ids: &[ZakuraPeerId],
    pending: &[Vec<ServingQuery>],
) -> Result<(), String> {
    let snapshot = harness.handle.serving_regulation_snapshot();
    let regulation = &config.get_blocks_serving_regulation;
    let session_pending_capacity =
        pending_input_capacity_per_session(config).map_err(str::to_owned)?;
    let node_pending_capacity =
        pending_input_capacity(config, peer_ids.len()).map_err(str::to_owned)?;
    if snapshot.node_rate_capacity != regulation.node_rate_capacity_bytes
        || snapshot.node_outstanding_capacity != regulation.node_outstanding_bytes
        || snapshot.session_pending_input_capacity != session_pending_capacity
        || snapshot.pending_input_capacity != node_pending_capacity
        || snapshot.node_rate_balance > regulation.node_rate_capacity_bytes
        || snapshot.node_outstanding > regulation.node_outstanding_bytes
        || snapshot.pending_inputs > node_pending_capacity
        || snapshot.pending_inputs != snapshot.aggregate_session_pending_inputs
        || snapshot.max_session_pending_inputs > session_pending_capacity
        || snapshot.node_outstanding != snapshot.aggregate_peer_backlog
        || snapshot.aggregate_peer_backlog > regulation.node_outstanding_bytes
        || snapshot.max_peer_backlog > regulation.peer_backlog_bytes
    {
        return Err(format!("serving bound exceeded: {snapshot:?}"));
    }
    let max_inflight = usize::try_from(config.max_inflight_requests)
        .map_err(|_| "the generated inflight cap does not fit usize".to_owned())?;
    for (peer, requests) in peer_ids.iter().zip(pending) {
        if requests.len() > max_inflight {
            return Err(format!(
                "peer {peer:?} exceeded its serving ledger cap: {} > {}",
                requests.len(),
                config.max_inflight_requests
            ));
        }
        if harness
            .handle
            .serving_peer_rate_balance(peer)
            .is_some_and(|balance| balance > regulation.peer_rate_capacity_bytes)
        {
            return Err(format!("peer {peer:?} exceeded its rate capacity"));
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct NativeServingQuery {
    peer: ZakuraPeerId,
    observed_at: Instant,
}

#[derive(Copy, Clone, Debug, Default)]
struct NativeResponseSummary {
    blocks: u32,
    payload_bytes: u64,
}

struct NativeServingRig {
    node: ZakuraTestNode,
    driver: JoinHandle<()>,
    queries: mpsc::UnboundedReceiver<NativeServingQuery>,
    submitted: mpsc::UnboundedReceiver<block::Height>,
}

fn native_candidate_config() -> ZakuraBlockSyncConfig {
    ZakuraBlockSyncConfig {
        max_blocks_per_response: 16,
        max_inflight_requests: 64,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
        peer_limits: ServicePeerLimits {
            inbound_queue_depth: 64,
            outbound_queue_depth: 128,
            ..ServicePeerLimits::default()
        },
        ..ZakuraBlockSyncConfig::default()
    }
}

async fn spawn_native_serving_rig(
    seed: u64,
    corpus: SyntheticBlockCorpus,
    initial_height: block::Height,
    config: ZakuraBlockSyncConfig,
) -> Result<NativeServingRig, BoxError> {
    let genesis = corpus
        .block_at(block::Height::MIN)
        .expect("the native serving corpus includes genesis");
    let initial = corpus
        .block_at(initial_height)
        .expect("the native serving corpus includes its initial frontier");
    let apply = MockApplyFrontier::with_committed_height(corpus.clone(), initial_height);
    let frontiers = apply.frontiers();
    let default_limits = ZakuraLocalLimits::from_config(&Config::default());
    let expected_send_window = default_limits.send_window();
    let mut limits = default_limits;
    limits.max_pending_handshakes = 16;
    limits.max_open_streams = 16;
    limits.max_inbound_queue_depth = 64;
    limits.message_rate_per_second = 2_048;
    limits.stream_open_rate_per_second = 64;

    let node = ZakuraTestNode::builder(seed)
        .limits(limits)
        .max_connections_per_ip(16)
        .header_sync_driver(
            Network::Mainnet,
            (block::Height::MIN, genesis.hash()),
            FullStateFrontiers {
                finalized_height: frontiers.finalized_height,
                verified_block_tip: frontiers.verified_block_tip,
                verified_block_hash: frontiers.verified_block_hash,
            },
            Some((initial_height, initial.hash())),
        )
        .block_sync_config(config)
        .spawn()
        .await?;
    assert_eq!(
        node.limits().send_window(),
        expected_send_window,
        "native load tests must exercise the production-default send window"
    );
    let mut actions = node
        .take_block_sync_actions()
        .await
        .expect("the native node exposes its real block-sync driver seam");
    let handle = node
        .block_sync()
        .expect("header-sync startup also enables native block sync");
    let (query_tx, queries) = mpsc::unbounded_channel();
    let (submitted_tx, submitted) = mpsc::unbounded_channel();
    let driver = tokio::spawn(async move {
        while let Some(action) = actions.recv().await {
            let event = match action {
                BlockSyncAction::QueryNeededBlocks {
                    query_id,
                    from,
                    best_header_tip,
                    scope,
                    ..
                } => {
                    let frontiers = apply.frontiers();
                    let first_missing = frontiers
                        .verified_block_tip
                        .next()
                        .unwrap_or(block::Height::MAX);
                    let from = from.max(first_missing);
                    let blocks = if from <= best_header_tip {
                        corpus.metas_between(from, best_header_tip)
                    } else {
                        Vec::new()
                    };
                    BlockSyncEvent::ScopedNeededBlocks {
                        query_id,
                        scope,
                        body_anchor: zakura_header_chain::Frontier::new(
                            frontiers.verified_block_tip,
                            frontiers.verified_block_hash,
                        ),
                        blocks,
                    }
                }
                BlockSyncAction::QueryBlocksByHeightRange {
                    request_id,
                    peer,
                    start,
                    count,
                } => {
                    let _ = query_tx.send(NativeServingQuery {
                        peer: peer.clone(),
                        observed_at: Instant::now(),
                    });
                    BlockSyncEvent::BlockRangeResponseReady {
                        request_id,
                        peer,
                        start_height: start,
                        requested_count: count,
                        blocks: corpus.blocks_in_range(
                            start,
                            count,
                            apply.frontiers().verified_block_tip,
                        ),
                    }
                }
                BlockSyncAction::SubmitBlock {
                    owner,
                    source,
                    token,
                    block,
                } => {
                    let height = block
                        .coinbase_height()
                        .expect("the synthetic native body has a coinbase height");
                    let hash = block.hash();
                    let outcome = apply.apply(&block);
                    if handle
                        .send(BlockSyncEvent::BlockApplyFinished {
                            owner,
                            source,
                            token,
                            height,
                            hash,
                            outcome: test_block_apply_outcome(outcome.result),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                    if outcome.result == BlockApplyResult::Committed {
                        let _ = submitted_tx.send(height);
                        if handle
                            .send(BlockSyncEvent::ChainTipGrow(outcome.frontiers))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    continue;
                }
                BlockSyncAction::RecordBodyUnavailable { .. }
                | BlockSyncAction::RecordBodyInvalid { .. }
                | BlockSyncAction::RestartBodyAvailability { .. }
                | BlockSyncAction::RetryBodyAvailability { .. }
                | BlockSyncAction::Misbehavior { .. } => continue,
            };
            if handle.send(event).await.is_err() {
                break;
            }
        }
    });

    Ok(NativeServingRig {
        node,
        driver,
        queries,
        submitted,
    })
}

fn native_peer_status(
    corpus: &SyntheticBlockCorpus,
    servable_low: block::Height,
    servable_high: block::Height,
) -> BlockSyncStatus {
    let tip_hash = corpus
        .block_at(servable_high)
        .expect("the advertised native tip exists")
        .hash();
    BlockSyncStatus {
        servable_low,
        servable_high,
        tip_hash,
        max_blocks_per_response: 16,
        max_inflight_requests: 64,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
    }
}

async fn connect_native_block_sync_peer(
    node: &ZakuraTestNode,
    seed: u64,
    status: BlockSyncStatus,
) -> Result<(ZakuraPeerId, HostilePeer), BoxError> {
    let remote =
        HostilePeer::connect_native_with_capabilities(node, seed, ZAKURA_CAP_BLOCK_SYNC).await?;
    let peer_id = remote.id()?;
    let peer_set = node.supervisor().subscribe();
    await_until(
        "native block-sync peer registration",
        Duration::from_secs(5),
        || peer_set.borrow().contains(&peer_id),
    )
    .await?;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (_, message) = recv_native_block_sync_message(&remote).await?;
            if matches!(message, BlockSyncMessage::Status(_)) {
                return Ok::<(), BoxError>(());
            }
        }
    })
    .await
    .map_err(|_| -> BoxError { "timed out waiting for native block-sync Status".into() })??;
    send_native_block_sync_message(&remote, &BlockSyncMessage::Status(status)).await?;

    let registry = node
        .block_sync()
        .expect("native block sync remains enabled")
        .routine_wiring
        .as_ref()
        .expect("the native reactor exposes routine wiring to contract tests")
        .registry
        .clone();
    await_until(
        "native block-sync Status registration",
        Duration::from_secs(5),
        || registry.has_received_status(&peer_id),
    )
    .await?;
    Ok((peer_id, remote))
}

async fn send_native_block_sync_message(
    peer: &HostilePeer,
    message: &BlockSyncMessage,
) -> Result<(), BoxError> {
    peer.send_raw_frame(ZAKURA_STREAM_BLOCK_SYNC, message.encode_frame()?)
        .await
}

async fn recv_native_block_sync_message(
    peer: &HostilePeer,
) -> Result<(usize, BlockSyncMessage), BoxError> {
    let frame = peer.recv_ordered_frame(ZAKURA_STREAM_BLOCK_SYNC).await?;
    let payload_bytes = frame.payload.len();
    Ok((payload_bytes, BlockSyncMessage::decode_frame(frame)?))
}

async fn drain_native_response(
    peer: &HostilePeer,
    timeout: Duration,
) -> Result<NativeResponseSummary, BoxError> {
    tokio::time::timeout(timeout, async {
        let mut summary = NativeResponseSummary::default();
        loop {
            let (payload_bytes, message) = recv_native_block_sync_message(peer).await?;
            match message {
                BlockSyncMessage::Status(_) => {}
                BlockSyncMessage::Block(_) => {
                    summary.blocks = summary.blocks.saturating_add(1);
                    summary.payload_bytes = summary
                        .payload_bytes
                        .saturating_add(u64::try_from(payload_bytes).expect("usize fits u64"));
                }
                BlockSyncMessage::BlocksDone { returned, .. } => {
                    assert_eq!(returned, summary.blocks);
                    summary.payload_bytes = summary
                        .payload_bytes
                        .saturating_add(u64::try_from(payload_bytes).expect("usize fits u64"));
                    return Ok(summary);
                }
                BlockSyncMessage::RangeUnavailable { .. } => {
                    summary.payload_bytes = summary
                        .payload_bytes
                        .saturating_add(u64::try_from(payload_bytes).expect("usize fits u64"));
                    return Ok(summary);
                }
                message => {
                    return Err(format!("unexpected native serving frame: {message:?}").into())
                }
            }
        }
    })
    .await
    .map_err(|_| -> BoxError { "timed out draining native GetBlocks response".into() })?
}

fn ceil_rate_delay(bytes: u64, bytes_per_second: u64) -> Duration {
    let whole_seconds = bytes / bytes_per_second;
    let remainder = bytes % bytes_per_second;
    let remainder_nanos = if remainder == 0 {
        0
    } else {
        remainder
            .saturating_mul(1_000_000_000)
            .div_ceil(bytes_per_second)
    };
    Duration::from_secs(whole_seconds).saturating_add(Duration::from_nanos(remainder_nanos))
}

async fn wait_for_native_query(
    queries: &mut mpsc::UnboundedReceiver<NativeServingQuery>,
    expected_peer: &ZakuraPeerId,
    timeout: Duration,
) -> Result<(Instant, usize), BoxError> {
    tokio::time::timeout(timeout, async {
        let mut preceding = 0usize;
        loop {
            let query = queries
                .recv()
                .await
                .ok_or_else(|| -> BoxError { "native serving query channel closed".into() })?;
            if query.peer == *expected_peer {
                return Ok((query.observed_at, preceding));
            }
            preceding = preceding.saturating_add(1);
        }
    })
    .await
    .map_err(|_| -> BoxError { "timed out waiting for native serving query".into() })?
}

fn process_rss_bytes() -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let kib = std::str::from_utf8(&output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    kib.checked_mul(1_024)
}

#[track_caller]
fn require_native_load_release_profile() {
    #[cfg(debug_assertions)]
    panic!("native load contracts measure deployed behavior; rerun with `cargo test --release`");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "native QUIC regulated-load lane; run explicitly on a developer machine"]
async fn gb_rl_10b_native_reading_flood_preserves_honest_tiny_and_full_service(
) -> Result<(), BoxError> {
    require_native_load_release_profile();
    let _guard = zakura_test::init();
    run_native_reading_flood_case("tiny", 8 * 1_024, 0x10b0).await?;
    run_native_reading_flood_case(
        "full",
        usize::try_from(block::MAX_BLOCK_BYTES).expect("maximum block bytes fit usize"),
        0x10bf,
    )
    .await?;
    Ok(())
}

#[allow(clippy::print_stderr)] // report opt-in load measurements to the invoking developer
async fn run_native_reading_flood_case(
    label: &'static str,
    target_block_bytes: usize,
    seed: u64,
) -> Result<(), BoxError> {
    const HOSTILE_PEERS: usize = 15;
    const REQUEST_COUNT: u32 = 16;

    let corpus = SyntheticBlockCorpus::generate(
        REQUEST_COUNT,
        seed,
        SyntheticBlockShape {
            target_block_bytes: Some(target_block_bytes),
        },
    );
    let config = native_candidate_config();
    let request_timeout = config.request_timeout;
    let node_rate = config
        .get_blocks_serving_regulation
        .node_rate_bytes_per_second;
    let cost = serving_cost(&config, REQUEST_COUNT).expect("the native request cost is valid");
    let mut rig = spawn_native_serving_rig(seed, corpus.clone(), TIP, config).await?;
    let status = native_peer_status(&corpus, block::Height::MIN, TIP);
    let mut peers = Vec::with_capacity(HOSTILE_PEERS + 1);
    for index in 0..=HOSTILE_PEERS {
        let peer_seed = seed
            .checked_add(u64::try_from(index).expect("peer index fits u64"))
            .and_then(|value| value.checked_add(1))
            .expect("test peer seeds fit u64");
        peers.push(connect_native_block_sync_peer(&rig.node, peer_seed, status).await?);
    }

    let request = BlockSyncMessage::GetBlocks {
        start_height: block::Height(1),
        count: REQUEST_COUNT,
    };
    for (_, remote) in peers.iter().take(HOSTILE_PEERS) {
        send_native_block_sync_message(remote, &request).await?;
    }
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    let mut admitted_before_honest = 0usize;
    while rig.queries.try_recv().is_ok() {
        admitted_before_honest = admitted_before_honest.saturating_add(1);
    }
    let available_tokens = rig
        .node
        .block_sync()
        .expect("native block sync remains enabled")
        .serving_regulation_snapshot()
        .node_rate_balance;
    let pending_hostiles = HOSTILE_PEERS.saturating_sub(admitted_before_honest);
    let charges_waiting = u64::try_from(pending_hostiles.saturating_add(1))
        .expect("native peer count fits u64")
        .checked_mul(cost.charge)
        .expect("bounded native charge total fits u64");
    let fair_reference =
        ceil_rate_delay(charges_waiting.saturating_sub(available_tokens), node_rate)
            .saturating_add(Duration::from_millis(500));

    let (honest_id, honest) = peers.last().expect("the honest peer was connected");
    let honest_sent_at = Instant::now();
    send_native_block_sync_message(honest, &request).await?;
    let honest_wait = async {
        let observation =
            wait_for_native_query(&mut rig.queries, honest_id, request_timeout).await?;
        let latency = observation.0.saturating_duration_since(honest_sent_at);
        eprintln!("GB-RL-10b {label}: observed honest admission after {latency:?}");
        Ok::<_, BoxError>(observation)
    };
    let honest_response_wait = async {
        let response = drain_native_response(honest, request_timeout)
            .await
            .map_err(|error| -> BoxError {
                format!("{label} honest response failed: {error}").into()
            })?;
        Ok::<_, BoxError>((Instant::now(), response))
    };
    let hostile_response_reads = join_all(peers.iter().take(HOSTILE_PEERS).enumerate().map(
        |(index, (_, peer))| async move {
            drain_native_response(peer, Duration::from_secs(30))
                .await
                .map_err(|error| -> BoxError {
                    format!("{label} hostile response {index} failed: {error}").into()
                })
        },
    ));
    let (honest_observation, honest_response, hostile_responses) =
        tokio::join!(honest_wait, honest_response_wait, hostile_response_reads);
    let (honest_observed_at, hostile_queries_after_send) = honest_observation?;
    let honest_admission_latency = honest_observed_at.saturating_duration_since(honest_sent_at);
    assert!(
        honest_admission_latency < request_timeout,
        "{label} honest admission took {honest_admission_latency:?}, exceeding {request_timeout:?}"
    );
    let (honest_completed_at, honest_response) = honest_response?;
    let honest_completion_latency = honest_completed_at.saturating_duration_since(honest_sent_at);
    assert!(
        honest_completion_latency < request_timeout,
        "{label} honest terminal response took {honest_completion_latency:?}, exceeding {request_timeout:?}"
    );
    let _: Vec<_> = hostile_responses.into_iter().collect::<Result<_, _>>()?;
    assert!(honest_response.blocks > 0);
    assert!(honest_response.payload_bytes <= cost.response_cap);
    eprintln!(
        "GB-RL-10b {label}: admission_latency={honest_admission_latency:?} \
         completion_latency={honest_completion_latency:?} fair_reference={fair_reference:?} \
         hostile_admitted_ahead={} response_blocks={} response_payload_bytes={} rss_bytes={:?}",
        admitted_before_honest.saturating_add(hostile_queries_after_send),
        honest_response.blocks,
        honest_response.payload_bytes,
        process_rss_bytes(),
    );

    rig.driver.abort();
    for (_, peer) in peers {
        peer.shutdown().await;
    }
    rig.node.shutdown().await;
    Ok(())
}

struct StoppedNativePeer {
    peer: HostilePeer,
    udp_bytes_before: u64,
}

async fn connect_and_saturate_stopped_readers(
    rig: &NativeServingRig,
    corpus: &SyntheticBlockCorpus,
    seed: u64,
    peer_count: usize,
    response_cap: u64,
) -> Result<Vec<StoppedNativePeer>, BoxError> {
    let status = native_peer_status(corpus, block::Height::MIN, TIP);
    let mut stopped = Vec::with_capacity(peer_count);
    for index in 0..peer_count {
        let peer_seed = seed
            .checked_add(u64::try_from(index).expect("peer index fits u64"))
            .expect("test peer seeds fit u64");
        let (_, peer) = connect_native_block_sync_peer(&rig.node, peer_seed, status).await?;
        stopped.push(StoppedNativePeer {
            udp_bytes_before: peer.received_udp_bytes(),
            peer,
        });
    }

    let request = BlockSyncMessage::GetBlocks {
        start_height: block::Height(1),
        count: 16,
    };
    // The first response can move into QUIC's send window. The second request
    // on each ordered stream is what leaves application-owned frame leases
    // behind a peer that never reads.
    for _ in 0..2 {
        for stopped_peer in &stopped {
            send_native_block_sync_message(&stopped_peer.peer, &request).await?;
        }
    }

    let handle = rig
        .node
        .block_sync()
        .expect("native block sync remains enabled");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut max_outstanding = 0u64;
    loop {
        let snapshot = handle.serving_regulation_snapshot();
        max_outstanding = max_outstanding.max(snapshot.node_outstanding);
        let udp_deltas: Vec<_> = stopped
            .iter()
            .map(|peer| {
                peer.peer
                    .received_udp_bytes()
                    .saturating_sub(peer.udp_bytes_before)
            })
            .collect();
        if snapshot.node_outstanding.saturating_add(response_cap)
            > snapshot.node_outstanding_capacity
            && udp_deltas.iter().all(|bytes| *bytes >= 24 * 1_024 * 1_024)
        {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "stopped readers did not exhaust the node budget: snapshot={snapshot:?}, \
                 max_outstanding={max_outstanding}, udp_deltas={udp_deltas:?}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(stopped)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "native QUIC stopped-reader lane; takes at least one transport write timeout"]
#[allow(clippy::print_stderr)] // report opt-in load measurements to the invoking developer
async fn gb_rl_10c_native_stopped_readers_stay_bounded_reclaim_and_restore_service(
) -> Result<(), BoxError> {
    const STOPPED_PEERS: usize = 9;
    const TRANSPORT_SLACK: Duration = Duration::from_secs(2);

    require_native_load_release_profile();
    let _guard = zakura_test::init();
    let corpus = SyntheticBlockCorpus::generate(
        16,
        0x10c0,
        SyntheticBlockShape {
            target_block_bytes: Some(
                usize::try_from(block::MAX_BLOCK_BYTES).expect("maximum block bytes fit usize"),
            ),
        },
    );
    let config = native_candidate_config();
    let cost = serving_cost(&config, 16).expect("the stopped-reader request cost is valid");
    let mut rig = spawn_native_serving_rig(0x10c0, corpus.clone(), TIP, config).await?;
    let honest_status = native_peer_status(&corpus, block::Height::MIN, TIP);
    let (honest_id, honest) =
        connect_native_block_sync_peer(&rig.node, 0x10cf, honest_status).await?;
    let rss_before = process_rss_bytes();
    let stopped = connect_and_saturate_stopped_readers(
        &rig,
        &corpus,
        0x10d0,
        STOPPED_PEERS,
        cost.response_cap,
    )
    .await?;
    let handle = rig
        .node
        .block_sync()
        .expect("native block sync remains enabled");
    let saturated = handle.serving_regulation_snapshot();
    assert!(saturated.node_outstanding <= saturated.node_outstanding_capacity);
    assert!(
        saturated.max_peer_backlog <= config_get_peer_backlog(&native_candidate_config()),
        "per-peer application backlog exceeded its configured budget: {saturated:?}"
    );
    let rss_saturated = process_rss_bytes();
    while rig.queries.try_recv().is_ok() {}

    let request = BlockSyncMessage::GetBlocks {
        start_height: block::Height(1),
        count: 16,
    };
    let honest_sent_at = Instant::now();
    send_native_block_sync_message(&honest, &request).await?;
    let resume_deadline = Duration::from_secs(10).saturating_add(TRANSPORT_SLACK);
    let (honest_observed_at, _) =
        wait_for_native_query(&mut rig.queries, &honest_id, resume_deadline).await?;
    let resume_latency = honest_observed_at.saturating_duration_since(honest_sent_at);
    assert!(resume_latency <= resume_deadline);
    let honest_response = drain_native_response(&honest, Duration::from_secs(15)).await?;
    assert!(honest_response.blocks > 0);

    await_until(
        "all stopped-reader frame leases are reclaimed",
        Duration::from_secs(20),
        || handle.serving_regulation_snapshot().node_outstanding == 0,
    )
    .await?;
    let reclaimed = handle.serving_regulation_snapshot();
    assert_eq!(reclaimed.aggregate_peer_backlog, 0);
    assert_eq!(reclaimed.max_peer_backlog, 0);

    let mut aggregate_udp_bytes = 0u64;
    for stopped_peer in &stopped {
        let delta = stopped_peer
            .peer
            .received_udp_bytes()
            .saturating_sub(stopped_peer.udp_bytes_before);
        assert!(delta > 0, "each stopped peer receives transport traffic");
        aggregate_udp_bytes = aggregate_udp_bytes.saturating_add(delta);
    }
    let transport_send_window = rig.node.limits().send_window();
    eprintln!(
        "GB-RL-10c: resume_latency={resume_latency:?} saturated_app_bytes={} \
         transport_send_window={transport_send_window} aggregate_udp_bytes={aggregate_udp_bytes} \
         rss_before={rss_before:?} \
         rss_saturated={rss_saturated:?} rss_reclaimed={:?}",
        saturated.node_outstanding,
        process_rss_bytes(),
    );

    rig.driver.abort();
    honest.shutdown().await;
    for stopped_peer in stopped {
        stopped_peer.peer.shutdown().await;
    }
    rig.node.shutdown().await;
    Ok(())
}

fn config_get_peer_backlog(config: &ZakuraBlockSyncConfig) -> u64 {
    config.get_blocks_serving_regulation.peer_backlog_bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "native QUIC full-duplex regulated-load lane; run explicitly"]
#[allow(clippy::print_stderr)] // report opt-in load measurements to the invoking developer
async fn gb_rl_11_pipelined_serving_requests_keep_same_stream_download_live() -> Result<(), BoxError>
{
    const INITIAL_TIP: block::Height = block::Height(16);
    const TARGET_TIP: block::Height = block::Height(19);
    const STOPPED_PEERS: usize = 3;

    require_native_load_release_profile();
    let _guard = zakura_test::init();
    let corpus = SyntheticBlockCorpus::generate(
        TARGET_TIP.0,
        0x1100,
        SyntheticBlockShape {
            target_block_bytes: Some(
                usize::try_from(block::MAX_BLOCK_BYTES).expect("maximum block bytes fit usize"),
            ),
        },
    );
    let mut config = native_candidate_config();
    let cost = serving_cost(&config, 16).expect("the full-duplex request cost is valid");
    config.get_blocks_serving_regulation.node_outstanding_bytes = cost
        .response_cap
        .checked_mul(2)
        .expect("two native response reservations fit u64");
    let request_timeout = config.request_timeout;
    let mut rig = spawn_native_serving_rig(0x1100, corpus.clone(), INITIAL_TIP, config).await?;
    let duplex_status = native_peer_status(&corpus, block::Height(17), TARGET_TIP);
    let (duplex_id, duplex) =
        connect_native_block_sync_peer(&rig.node, 0x1101, duplex_status).await?;
    let stopped = connect_and_saturate_stopped_readers(
        &rig,
        &corpus,
        0x1110,
        STOPPED_PEERS,
        cost.response_cap,
    )
    .await?;
    while rig.queries.try_recv().is_ok() {}

    let block_sync = rig
        .node
        .block_sync()
        .expect("native block sync remains enabled");
    block_sync
        .send(BlockSyncEvent::HeaderTipChanged {
            height: TARGET_TIP,
            hash: corpus
                .block_at(TARGET_TIP)
                .expect("the target block exists")
                .hash(),
        })
        .await?;
    let (download_start, download_count) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (_, message) = recv_native_block_sync_message(&duplex).await?;
            match message {
                BlockSyncMessage::Status(_) => {}
                BlockSyncMessage::GetBlocks {
                    start_height,
                    count,
                } => return Ok::<_, BoxError>((start_height, count)),
                message => return Err(format!("unexpected full-duplex frame: {message:?}").into()),
            }
        }
    })
    .await
    .map_err(|_| -> BoxError { "timed out waiting for native download GetBlocks".into() })??;

    let serving_requests = [
        BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 16,
        },
        BlockSyncMessage::GetBlocks {
            start_height: block::Height(2),
            count: 15,
        },
    ];
    let serving_request_sent_at = Instant::now();
    for request in &serving_requests {
        send_native_block_sync_message(&duplex, request).await?;
    }
    let download_blocks = corpus.blocks_in_range(download_start, download_count, TARGET_TIP);
    for (_, block, _) in &download_blocks {
        send_native_block_sync_message(&duplex, &BlockSyncMessage::Block(block.clone())).await?;
    }
    send_native_block_sync_message(
        &duplex,
        &BlockSyncMessage::BlocksDone {
            start_height: download_start,
            returned: u32::try_from(download_blocks.len()).expect("download range fits u32"),
        },
    )
    .await?;

    let mut saw_duplex_query = false;
    while let Ok(query) = rig.queries.try_recv() {
        saw_duplex_query |= query.peer == duplex_id;
    }
    assert!(
        !saw_duplex_query,
        "the saturated node admitted the duplex peer's serving request too early"
    );

    let expected: std::collections::BTreeSet<_> = download_blocks
        .iter()
        .map(|(height, _, _)| *height)
        .collect();
    tokio::time::timeout(request_timeout, async {
        let mut submitted = std::collections::BTreeSet::new();
        while !expected.is_subset(&submitted) {
            let height = rig
                .submitted
                .recv()
                .await
                .ok_or_else(|| -> BoxError { "native submission channel closed".into() })?;
            submitted.insert(height);
        }
        Ok::<(), BoxError>(())
    })
    .await
    .map_err(|_| -> BoxError { "same-stream download stalled past request timeout".into() })??;
    let download_latency = serving_request_sent_at.elapsed();
    assert!(download_latency < request_timeout);

    let mut saw_duplex_query = false;
    while let Ok(query) = rig.queries.try_recv() {
        saw_duplex_query |= query.peer == duplex_id;
    }
    assert!(
        !saw_duplex_query,
        "download responses must pass while the serving request remains admission-delayed"
    );

    join_all(
        stopped
            .into_iter()
            .map(|stopped_peer| stopped_peer.peer.shutdown()),
    )
    .await;
    let mut served_payload_bytes = 0u64;
    for _ in &serving_requests {
        let _ = wait_for_native_query(&mut rig.queries, &duplex_id, request_timeout).await?;
        let served_response = drain_native_response(&duplex, Duration::from_secs(15)).await?;
        assert!(served_response.blocks > 0);
        served_payload_bytes = served_payload_bytes.saturating_add(served_response.payload_bytes);
    }
    await_until(
        "full-duplex serving leases drain",
        Duration::from_secs(5),
        || block_sync.serving_regulation_snapshot().node_outstanding == 0,
    )
    .await?;
    eprintln!(
        "GB-RL-11: delayed_serving_and_download_latency={download_latency:?} \
         downloaded_blocks={} served_payload_bytes={} rss_bytes={:?}",
        expected.len(),
        served_payload_bytes,
        process_rss_bytes(),
    );

    rig.driver.abort();
    duplex.shutdown().await;
    rig.node.shutdown().await;
    Ok(())
}
