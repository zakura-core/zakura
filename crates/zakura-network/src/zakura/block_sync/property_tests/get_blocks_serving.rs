//! Stateful properties for serving inbound `GetBlocks` requests.
//!
//! Unlike the adjacent wire-contract tests, these cases send real framed
//! messages through `BlockSyncService`, the peer routine, the reactor, and the
//! state-driver action seam. Each generated case uses a paused current-thread
//! runtime so failures shrink without depending on wall-clock timing.
//!
//! | ID | Serving property | Pre-fix result |
//! | --- | --- | --- |
//! | GS-01 | A peer routine reads no frames before reactor admission | Fails |
//! | GS-02 | An old disconnect cannot remove a replacement session | Fails |
//! | GS-03 | An old state response cannot cross into a replacement session | Fails |
//! | GS-04 | Only a live request completion releases its serving slot | Fails |
//! | GS-05 | One saturated peer does not prevent another peer from progressing | Passes |
//! | GS-06 | A response is the largest contiguous prefix within the byte cap | Passes |
//!
//! The first four properties intentionally fail against the implementation
//! under test in this draft. They are executable counterexamples for three
//! focused production fixes. The property bodies must stay unchanged while
//! those fixes are developed; only the adjacent API adapter may track a changed
//! action or event shape.

use std::{cell::Cell, collections::HashMap, env};

use proptest::{
    prelude::*,
    test_runner::{Config as ProptestConfig, RngSeed, TestCaseError, TestCaseResult, TestRunner},
};
use tokio::{
    runtime::Builder,
    sync::{mpsc, watch},
    time::{sleep, timeout, Duration},
};
use tokio_util::sync::CancellationToken;

use super::super::super::{
    config::{BlockSyncStatus, MAX_BS_RESPONSE_BYTES},
    spawn_block_sync_reactor,
    wire::*,
    BlockSyncAction, BlockSyncEvent, BlockSyncFrontiers, BlockSyncService, BlockSyncStartup,
    ZakuraBlockSyncConfig,
};
use super::super::{
    block_size, mainnet_blocks_1_to_3, next_action, peer, wait_for_outbound_block,
    wait_for_outbound_blocks_done, wait_for_outbound_range_unavailable, wait_for_outbound_status,
};
use super::get_blocks_serving_api::ServingQuery;
use crate::zakura::{
    framed_channel, FramedRecv, FramedSend, Peer, Service, ServicePeerDirection, ZakuraConnId,
    ZakuraPeerId,
};
use zakura_chain::block;

const DEFAULT_SERVING_PROPTEST_SEED: u64 = 8_650_902;

struct TestPeer {
    id: ZakuraPeerId,
    conn_id: ZakuraConnId,
    inbound: FramedSend,
    outbound: FramedRecv,
}

async fn connect_peer(
    service: &BlockSyncService,
    id: ZakuraPeerId,
    conn_id: ZakuraConnId,
    tip: (block::Height, block::Hash),
) -> TestPeer {
    let (inbound, inbound_recv) = framed_channel(256);
    let (outbound_send, mut outbound) = framed_channel(256);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_recv, outbound_send))]);
    service.add_peer(Peer::new_with_conn_id_and_direction(
        conn_id,
        id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    wait_for_outbound_status(&mut outbound).await;
    inbound
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: tip.0,
                tip_hash: tip.1,
                max_blocks_per_response: 16,
                max_inflight_requests: 8,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("test Status encodes"),
        )
        .await
        .expect("test Status queues");

    TestPeer {
        id,
        conn_id,
        inbound,
        outbound,
    }
}

async fn request_blocks(peer: &TestPeer, start: u32, count: u32) {
    peer.inbound
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(start),
                count,
            }
            .encode_frame()
            .expect("test GetBlocks encodes"),
        )
        .await
        .expect("test GetBlocks queues");
}

async fn next_serving_query(
    actions: &mut mpsc::Receiver<BlockSyncAction>,
) -> Result<ServingQuery, TestCaseError> {
    loop {
        match ServingQuery::from_action(next_action(actions).await) {
            Ok(Some(query)) => return Ok(query),
            Ok(None) => {}
            Err(action) => {
                return Err(TestCaseError::fail(format!(
                    "unexpected action before serving query: {action:?}"
                )));
            }
        }
    }
}

async fn wait_for_unavailable(
    outbound: &mut FramedRecv,
) -> Result<(block::Height, u32), TestCaseError> {
    timeout(
        Duration::from_millis(10),
        wait_for_outbound_range_unavailable(outbound),
    )
    .await
    .map_err(|_| TestCaseError::fail("timed out waiting for RangeUnavailable"))
}

async fn assert_no_terminal_response(outbound: &mut FramedRecv) -> TestCaseResult {
    loop {
        let received = timeout(Duration::from_millis(1), outbound.recv()).await;
        let Ok(frame) = received else {
            return Ok(());
        };
        let Some(frame) = frame else {
            return Ok(());
        };
        match BlockSyncMessage::decode_frame(frame)
            .map_err(|error| TestCaseError::fail(format!("outbound frame must decode: {error}")))?
        {
            BlockSyncMessage::Status(_) | BlockSyncMessage::GetBlocks { .. } => {}
            message => {
                return Err(TestCaseError::fail(format!(
                    "a stale or unknown event produced outbound message type {}",
                    message.message_type()
                )));
            }
        }
    }
}

fn serving_property_cases() -> u32 {
    env::var("ZAKURA_SERVING_PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64)
}

fn serving_runtime() -> tokio::runtime::Runtime {
    Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("the current-thread property runtime builds")
}

fn serving_runner() -> TestRunner {
    let mut config = ProptestConfig::with_source_file(file!());
    config.cases = serving_property_cases();
    config.failure_persistence = None;
    config.rng_seed = RngSeed::Fixed(
        env::var("ZAKURA_SERVING_PROPTEST_SEED")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_SERVING_PROPTEST_SEED),
    );
    TestRunner::new(config)
}

fn distinct_height_triple() -> impl Strategy<Value = [u32; 3]> {
    (1u32..=3, 1u32..=3, 1u32..=3)
        .prop_filter("the generated heights must be distinct", |(a, b, c)| {
            a != b && a != c && b != c
        })
        .prop_map(|(a, b, c)| [a, b, c])
}

#[derive(Clone, Debug)]
struct AdmissionCase {
    peer_seed: u8,
    max_blocks_per_response: u32,
    max_inflight_requests: u32,
    start: u32,
    count: u32,
}

fn admission_case() -> impl Strategy<Value = AdmissionCase> {
    (0x80u8..=0xef, 1u32..=16, 1u32..=8, 1u32..=3, 1u32..=16).prop_map(
        |(peer_seed, max_blocks_per_response, max_inflight_requests, start, count)| AdmissionCase {
            peer_seed,
            max_blocks_per_response,
            max_inflight_requests,
            start,
            count,
        },
    )
}

async fn run_admission_case(case: AdmissionCase) -> TestCaseResult {
    let blocks = mainnet_blocks_1_to_3();
    let tip = (block::Height(3), blocks[2].hash());
    let config = ZakuraBlockSyncConfig::default();
    let (_tip_tx, tip_rx) = watch::channel(tip);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: tip.0,
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_rx,
        config.clone(),
    );
    let (mut handle, _actions, reactor_task) = spawn_block_sync_reactor(startup);
    let registry = handle
        .routine_wiring
        .as_ref()
        .expect("the spawned reactor exposes routine wiring")
        .registry
        .clone();
    reactor_task.abort();
    let _ = reactor_task.await;

    let (lifecycle, mut held_lifecycle) = mpsc::unbounded_channel();
    handle.lifecycle = lifecycle;
    let service = BlockSyncService::new_with_handle_for_test(config, handle);
    let peer_id = peer(case.peer_seed);
    let (inbound, inbound_recv) = framed_channel(16);
    let (outbound_send, _outbound) = framed_channel(16);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_recv, outbound_send))]);
    service.add_peer(Peer::new_with_conn_id_and_direction(
        1,
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    let connected_session = match held_lifecycle
        .recv()
        .await
        .expect("the held lifecycle receives peer admission")
    {
        BlockSyncEvent::PeerConnected(session) => session,
        event => {
            return Err(TestCaseError::fail(format!(
                "expected peer admission, got {event:?}"
            )));
        }
    };

    inbound
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: tip.0,
                tip_hash: tip.1,
                max_blocks_per_response: case.max_blocks_per_response,
                max_inflight_requests: case.max_inflight_requests,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("generated Status encodes"),
        )
        .await
        .map_err(|error| TestCaseError::fail(format!("generated Status queues: {error}")))?;
    inbound
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(case.start),
                count: case.count,
            }
            .encode_frame()
            .expect("generated GetBlocks encodes"),
        )
        .await
        .map_err(|error| TestCaseError::fail(format!("generated GetBlocks queues: {error}")))?;
    sleep(Duration::from_millis(1)).await;

    let read_before_admission = registry.has_received_status(&peer_id);
    connected_session.cancel_token().cancel();
    prop_assert!(
        !read_before_admission,
        "the peer routine read Status before the reactor resolved admission"
    );
    Ok(())
}

#[test]
fn property_reactor_admission_precedes_peer_frames() {
    let mut runner = serving_runner();
    runner
        .run(&admission_case(), |case| {
            serving_runtime().block_on(run_admission_case(case))
        })
        .expect("GS-01: a peer routine read frames before reactor admission");
}

#[derive(Clone, Debug)]
struct ReconnectCase {
    heights: [u32; 3],
    count: u32,
}

fn reconnect_case() -> impl Strategy<Value = ReconnectCase> {
    (distinct_height_triple(), 1u32..=3)
        .prop_map(|(heights, count)| ReconnectCase { heights, count })
}

async fn run_stale_disconnect_case(case: ReconnectCase) -> TestCaseResult {
    let blocks = mainnet_blocks_1_to_3();
    let tip = (block::Height(3), blocks[2].hash());
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 3,
        max_inflight_requests: 1,
        ..ZakuraBlockSyncConfig::default()
    };
    let (_tip_tx, tip_rx) = watch::channel(tip);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: tip.0,
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let old_peer = connect_peer(&service, peer(0xd1), 1, tip).await;

    service.remove_peer(&old_peer.id, old_peer.conn_id);
    let mut current_peer = connect_peer(&service, old_peer.id.clone(), 2, tip).await;
    request_blocks(&current_peer, case.heights[0], case.count).await;
    let current_query = next_serving_query(&mut actions).await?;

    handle
        .send(current_query.finished_event(0))
        .await
        .map_err(|error| TestCaseError::fail(format!("current completion queues: {error}")))?;

    prop_assert_eq!(
        wait_for_unavailable(&mut current_peer.outbound).await?,
        (current_query.start(), current_query.count()),
        "the replacement session must survive an old disconnect"
    );
    reactor_task.abort();
    Ok(())
}

#[test]
fn property_stale_disconnect_preserves_replacement_session() {
    let mut runner = serving_runner();
    runner
        .run(&reconnect_case(), |case| {
            serving_runtime().block_on(run_stale_disconnect_case(case))
        })
        .expect("GS-02: an old disconnect removed a replacement session");
}

async fn run_stale_response_case(case: ReconnectCase) -> TestCaseResult {
    let blocks = mainnet_blocks_1_to_3();
    let tip = (block::Height(3), blocks[2].hash());
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 3,
        max_inflight_requests: 1,
        ..ZakuraBlockSyncConfig::default()
    };
    let (_tip_tx, tip_rx) = watch::channel(tip);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: tip.0,
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let old_peer = connect_peer(&service, peer(0xd2), 1, tip).await;

    request_blocks(&old_peer, case.heights[0], case.count).await;
    let old_query = next_serving_query(&mut actions).await?;
    service.remove_peer(&old_peer.id, old_peer.conn_id);
    timeout(Duration::from_millis(10), async {
        while handle.peer_snapshot().outbound_peers != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| TestCaseError::fail("old session did not disconnect"))?;

    let mut current_peer = connect_peer(&service, old_peer.id.clone(), 2, tip).await;
    request_blocks(&current_peer, case.heights[1], case.count).await;
    let current_query = next_serving_query(&mut actions).await?;
    let old_block =
        blocks[usize::try_from(case.heights[0] - 1).expect("generated height fits usize")].clone();
    handle
        .send(old_query.ready_event(vec![(block::Height(case.heights[0]), old_block, 1)]))
        .await
        .map_err(|error| TestCaseError::fail(format!("stale response queues: {error}")))?;

    assert_no_terminal_response(&mut current_peer.outbound).await?;
    request_blocks(&current_peer, case.heights[2], case.count).await;
    prop_assert_eq!(
        wait_for_unavailable(&mut current_peer.outbound).await?,
        (block::Height(case.heights[2]), case.count),
        "the current request must retain its serving slot"
    );

    handle
        .send(current_query.finished_event(0))
        .await
        .map_err(|error| TestCaseError::fail(format!("current completion queues: {error}")))?;
    reactor_task.abort();
    Ok(())
}

#[test]
fn property_stale_state_response_cannot_cross_sessions() {
    let mut runner = serving_runner();
    runner
        .run(&reconnect_case(), |case| {
            serving_runtime().block_on(run_stale_response_case(case))
        })
        .expect("GS-03: an old state response crossed into a replacement session");
}

#[derive(Clone, Debug)]
struct CompletionCase {
    heights: [u32; 3],
    duplicate: bool,
}

fn completion_case() -> impl Strategy<Value = CompletionCase> {
    (distinct_height_triple(), any::<bool>())
        .prop_map(|(heights, duplicate)| CompletionCase { heights, duplicate })
}

async fn run_completion_case(case: CompletionCase) -> TestCaseResult {
    let blocks = mainnet_blocks_1_to_3();
    let tip = (block::Height(3), blocks[2].hash());
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 3,
        max_inflight_requests: 1,
        ..ZakuraBlockSyncConfig::default()
    };
    let (_tip_tx, tip_rx) = watch::channel(tip);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: tip.0,
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let mut peer = connect_peer(&service, peer(0xd3), 1, tip).await;

    request_blocks(&peer, case.heights[0], 1).await;
    let completed_query = next_serving_query(&mut actions).await?;
    handle
        .send(completed_query.finished_event(0))
        .await
        .map_err(|error| TestCaseError::fail(format!("first completion queues: {error}")))?;
    prop_assert_eq!(
        wait_for_unavailable(&mut peer.outbound).await?,
        (completed_query.start(), completed_query.count())
    );

    request_blocks(&peer, case.heights[1], 1).await;
    let live_query = next_serving_query(&mut actions).await?;
    let stale_completion = if case.duplicate {
        completed_query.finished_event(0)
    } else {
        live_query
            .with_start(block::Height(case.heights[2]))
            .finished_event(0)
    };
    handle
        .send(stale_completion)
        .await
        .map_err(|error| TestCaseError::fail(format!("stale completion queues: {error}")))?;

    assert_no_terminal_response(&mut peer.outbound).await?;
    request_blocks(&peer, case.heights[2], 1).await;
    prop_assert_eq!(
        wait_for_unavailable(&mut peer.outbound).await?,
        (block::Height(case.heights[2]), 1),
        "a stale completion must not release the live request's slot"
    );

    handle
        .send(live_query.finished_event(0))
        .await
        .map_err(|error| TestCaseError::fail(format!("live completion queues: {error}")))?;
    reactor_task.abort();
    Ok(())
}

#[test]
fn property_only_live_completion_releases_serving_slot() {
    let mut runner = serving_runner();
    runner
        .run(&completion_case(), |case| {
            serving_runtime().block_on(run_completion_case(case))
        })
        .expect("GS-04: a stale completion released a live request's serving slot");
}

#[derive(Clone, Debug)]
struct SpamCase {
    inflight_cap: u32,
    excess_requests: u32,
    spam_start: u32,
    honest_start: u32,
}

fn spam_case() -> impl Strategy<Value = SpamCase> {
    (1u32..=4, 1u32..=64, 1u32..=3, 1u32..=3).prop_map(
        |(inflight_cap, excess_requests, spam_start, honest_start)| SpamCase {
            inflight_cap,
            excess_requests,
            spam_start,
            honest_start,
        },
    )
}

async fn run_spam_case(case: SpamCase) -> TestCaseResult {
    let blocks = mainnet_blocks_1_to_3();
    let tip = (block::Height(3), blocks[2].hash());
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 3,
        max_inflight_requests: case.inflight_cap,
        ..ZakuraBlockSyncConfig::default()
    };
    let (_tip_tx, tip_rx) = watch::channel(tip);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: tip.0,
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let mut spammer = connect_peer(&service, peer(0xe1), 1, tip).await;
    let mut honest = connect_peer(&service, peer(0xe2), 2, tip).await;

    for _ in 0..case.inflight_cap {
        request_blocks(&spammer, case.spam_start, 1).await;
        let query = next_serving_query(&mut actions).await?;
        prop_assert_eq!(query.peer(), &spammer.id);
    }
    for _ in 0..case.excess_requests {
        request_blocks(&spammer, case.spam_start, 1).await;
    }
    request_blocks(&honest, case.honest_start, 1).await;

    let honest_query = next_serving_query(&mut actions).await?;
    prop_assert_eq!(honest_query.peer(), &honest.id);
    prop_assert_eq!(honest_query.start(), block::Height(case.honest_start));
    prop_assert_eq!(honest_query.count(), 1);

    let honest_block = blocks
        [usize::try_from(case.honest_start - 1).expect("generated height fits usize")]
    .clone();
    handle
        .send(honest_query.ready_event(vec![(
            block::Height(case.honest_start),
            honest_block.clone(),
            usize::try_from(block_size(&honest_block)).expect("block size fits usize"),
        )]))
        .await
        .map_err(|error| TestCaseError::fail(format!("honest completion queues: {error}")))?;
    prop_assert_eq!(
        wait_for_outbound_block(&mut honest.outbound).await.hash(),
        honest_block.hash()
    );
    prop_assert_eq!(
        wait_for_outbound_blocks_done(&mut honest.outbound).await,
        (block::Height(case.honest_start), 1)
    );

    for _ in 0..case.excess_requests {
        prop_assert_eq!(
            wait_for_unavailable(&mut spammer.outbound).await?,
            (block::Height(case.spam_start), 1)
        );
    }

    reactor_task.abort();
    Ok(())
}

#[test]
#[allow(clippy::print_stdout)]
fn property_saturated_peer_does_not_block_honest_peer() {
    let mut runner = serving_runner();
    let scenarios = Cell::new(0u64);
    let requests = Cell::new(0u64);
    runner
        .run(&spam_case(), |case| {
            scenarios.set(scenarios.get().saturating_add(1));
            requests.set(
                requests.get().saturating_add(u64::from(
                    case.inflight_cap
                        .saturating_add(case.excess_requests)
                        .saturating_add(1),
                )),
            );
            serving_runtime().block_on(run_spam_case(case))
        })
        .expect("GS-05: serving contention blocked an honest peer");
    println!(
        "serving contention: {} generated scenarios, {} real GetBlocks requests",
        scenarios.get(),
        requests.get()
    );
}

#[derive(Clone, Debug)]
struct ByteCapCase {
    cap: u32,
    declared_sizes: [u32; 3],
}

fn byte_cap_case() -> impl Strategy<Value = ByteCapCase> {
    let arbitrary = (1u32..=6_000_000, prop::array::uniform3(1u32..=2_000_000)).prop_map(
        |(cap, declared_sizes)| ByteCapCase {
            cap,
            declared_sizes,
        },
    );
    let near_boundary = (
        prop::array::uniform3(1u32..=2_000_000),
        1usize..=3,
        -1i8..=1,
    )
        .prop_map(|(declared_sizes, prefix_len, offset)| {
            let boundary = declared_sizes[..prefix_len].iter().copied().sum::<u32>();
            let cap = match offset {
                -1 => boundary.saturating_sub(1).max(1),
                0 => boundary,
                1 => boundary.saturating_add(1),
                _ => unreachable!("generated offset is in -1..=1"),
            };
            ByteCapCase {
                cap,
                declared_sizes,
            }
        });

    prop_oneof![1 => arbitrary, 3 => near_boundary]
}

async fn run_byte_cap_case(case: ByteCapCase) -> TestCaseResult {
    let blocks = mainnet_blocks_1_to_3();
    let tip = (block::Height(3), blocks[2].hash());
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 3,
        max_inflight_requests: 1,
        max_response_bytes: case.cap,
        ..ZakuraBlockSyncConfig::default()
    };
    let (_tip_tx, tip_rx) = watch::channel(tip);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: tip.0,
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let mut peer = connect_peer(&service, peer(0xf1), 1, tip).await;
    request_blocks(&peer, 1, 3).await;
    let query = next_serving_query(&mut actions).await?;
    prop_assert_eq!(query.count(), 3);

    let mut total = 0u64;
    let expected_prefix = case
        .declared_sizes
        .iter()
        .take_while(|size| {
            let next = total.saturating_add(u64::from(**size));
            if next > u64::from(case.cap) {
                false
            } else {
                total = next;
                true
            }
        })
        .count();
    let response_blocks = blocks
        .iter()
        .zip(case.declared_sizes)
        .map(|(block, size)| {
            (
                block.coinbase_height().expect("test block has a height"),
                block.clone(),
                usize::try_from(size).expect("u32 declared size fits usize"),
            )
        })
        .collect();
    handle
        .send(query.ready_event(response_blocks))
        .await
        .map_err(|error| TestCaseError::fail(format!("byte-cap response queues: {error}")))?;

    if expected_prefix == 0 {
        prop_assert_eq!(
            wait_for_unavailable(&mut peer.outbound).await?,
            (block::Height(1), 3)
        );
    } else {
        for expected in blocks.iter().take(expected_prefix) {
            prop_assert_eq!(
                wait_for_outbound_block(&mut peer.outbound).await.hash(),
                expected.hash()
            );
        }
        prop_assert_eq!(
            wait_for_outbound_blocks_done(&mut peer.outbound).await,
            (
                block::Height(1),
                u32::try_from(expected_prefix).expect("three-block prefix fits u32")
            )
        );
    }

    reactor_task.abort();
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn response_accepts_a_prefix_ending_exactly_at_the_byte_cap() {
    run_byte_cap_case(ByteCapCase {
        cap: 5,
        declared_sizes: [2, 3, 4],
    })
    .await
    .unwrap();
}

#[test]
#[allow(clippy::print_stdout)]
fn property_response_is_largest_contiguous_prefix_within_byte_cap() {
    let mut runner = serving_runner();
    let scenarios = Cell::new(0u64);
    runner
        .run(&byte_cap_case(), |case| {
            scenarios.set(scenarios.get().saturating_add(1));
            serving_runtime().block_on(run_byte_cap_case(case))
        })
        .expect("GS-06: a served response violated its byte cap");
    println!(
        "response byte cap: {} generated scenarios and real GetBlocks responses",
        scenarios.get()
    );
}
