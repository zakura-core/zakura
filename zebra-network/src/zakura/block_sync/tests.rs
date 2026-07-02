use std::{collections::HashMap, future};

use proptest::{prop_assert, prop_assert_eq};

use super::*;
use super::{
    config::{
        BS_CHECKPOINT_RANGE_BYTE_FLOOR, BS_PER_BLOCK_WORST_CASE_BYTES, DEFAULT_BS_FANOUT,
        DEFAULT_BS_FLOOR_PEER_AVOID_COOLDOWN, DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES,
        DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BLOCKS, DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BYTES,
        DEFAULT_BS_MAX_RESPONSE_BYTES, DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES,
        DEFAULT_BS_NO_PROGRESS_PEER_COOLDOWN, DEFAULT_BS_REQUEST_TIMEOUT, MAX_BS_INFLIGHT_REQUESTS,
        MAX_BS_RESPONSE_BYTES,
    },
    reactor::node_id_from_block_peer_id,
    reorder::*,
    request::*,
    sequencer::*,
    state::*,
};
use crate::zakura::{
    framed_channel,
    testkit::{TraceCapture, TraceValue},
    ChainFrontier, FramedRecv, FramedSend, Frontier, FrontierChange, FrontierUpdate, Peer, Service,
    ServicePeerSnapshot, ServiceRegistry, StreamMode, ZakuraBlockSyncCandidateState,
    ZakuraSyncExchange,
};
use zebra_chain::{
    fmt::HexDebug,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::Transaction,
    transparent,
};
use zebra_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES, BLOCK_MAINNET_3_BYTES};

fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
}

fn mainnet_block(bytes: &[u8]) -> Arc<block::Block> {
    Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
}

fn mainnet_blocks_1_to_3() -> Vec<Arc<block::Block>> {
    vec![
        mainnet_block(&BLOCK_MAINNET_1_BYTES),
        mainnet_block(&BLOCK_MAINNET_2_BYTES),
        mainnet_block(&BLOCK_MAINNET_3_BYTES),
    ]
}

fn raw_block_payload(block: &Arc<block::Block>) -> Arc<[u8]> {
    let frame = BlockSyncMessage::Block(block.clone())
        .encode_frame()
        .expect("test block frame encodes");

    Arc::from(frame.payload.into_boxed_slice())
}

fn forked_block(block: &Arc<block::Block>, nonce_tag: u8) -> Arc<block::Block> {
    let mut fork = block.as_ref().clone();
    let mut header = *fork.header;
    header.nonce = HexDebug([nonce_tag; 32]);
    fork.header = Arc::new(header);
    Arc::new(fork)
}

fn block_with_bad_merkle_root(
    block: &Arc<block::Block>,
    extra_tx: &Arc<block::Block>,
) -> Arc<block::Block> {
    let mut bad_block = block.as_ref().clone();
    bad_block
        .transactions
        .push(extra_tx.transactions[0].clone());

    assert_eq!(bad_block.hash(), block.hash());
    assert_eq!(bad_block.coinbase_height(), block.coinbase_height());
    assert_ne!(
        bad_block
            .transactions
            .iter()
            .collect::<block::merkle::Root>(),
        bad_block.header.merkle_root
    );

    Arc::new(bad_block)
}

/// Build `count` internally-consistent blocks at the sequential heights
/// `1..=count`.
///
/// Each block is mainnet block 1 with its coinbase height rewritten and its
/// header merkle root recomputed, so it has a distinct hash and a header that
/// commits to its transactions. The real test vectors
/// only cover a handful of contiguous heights, which is too few to flood the
/// per-peer wire queue, so the body-flood test synthesizes its own chain.
fn fake_sequential_blocks(count: u32) -> Vec<Arc<block::Block>> {
    let template = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    (1..=count)
        .map(|height| fake_block_at_height(&template, block::Height(height)))
        .collect()
}

fn fake_blocks_in_range(start: u32, end: u32) -> Vec<Arc<block::Block>> {
    let template = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    (start..=end)
        .map(|height| fake_block_at_height(&template, block::Height(height)))
        .collect()
}

fn fake_block_at_height(template: &Arc<block::Block>, height: block::Height) -> Arc<block::Block> {
    let mut block = template.as_ref().clone();

    let mut coinbase = block.transactions[0].clone();
    let input = match Arc::make_mut(&mut coinbase) {
        Transaction::V1 { inputs, .. }
        | Transaction::V2 { inputs, .. }
        | Transaction::V3 { inputs, .. }
        | Transaction::V4 { inputs, .. }
        | Transaction::V5 { inputs, .. } => &mut inputs[0],
        Transaction::V6 { inputs, .. } => &mut inputs[0],
    };
    match input {
        transparent::Input::Coinbase {
            height: coinbase_height,
            ..
        } => *coinbase_height = height,
        _ => panic!("template block must start with a coinbase input"),
    }
    block.transactions[0] = coinbase;

    // Rewriting the coinbase changes the merkle root, so recompute it to keep the
    // synthesized block internally consistent (its header commits to its txs).
    let merkle_root = block.transactions.iter().collect::<block::merkle::Root>();
    let mut header = *block.header;
    header.merkle_root = merkle_root;
    block.header = Arc::new(header);

    Arc::new(block)
}

fn block_size(block: &block::Block) -> u32 {
    u32::try_from(
        block
            .zcash_serialize_to_vec()
            .expect("test block serializes")
            .len(),
    )
    .expect("test block size fits u32")
}

fn status() -> BlockSyncStatus {
    BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(42),
        tip_hash: block::Hash([7; 32]),
        max_blocks_per_response: 16,
        max_inflight_requests: 4,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
    }
}

fn immediate_body_download_config() -> ZakuraBlockSyncConfig {
    ZakuraBlockSyncConfig {
        max_blocks_per_response: MAX_BS_BLOCKS_PER_REQUEST,
        ..ZakuraBlockSyncConfig::default()
    }
}

fn test_frontier(height: u32) -> Frontier {
    let hash_byte = u8::try_from(height % 251).expect("height modulo 251 fits in u8");
    Frontier::new(block::Height(height), block::Hash([hash_byte; 32]))
}

fn test_frontier_update(
    finalized: u32,
    verified_body: u32,
    best_header: u32,
    change: FrontierChange,
) -> FrontierUpdate {
    FrontierUpdate {
        frontier: ChainFrontier {
            finalized: test_frontier(finalized),
            verified_body: test_frontier(verified_body),
            best_header: test_frontier(best_header),
        },
        change,
    }
}

fn exchange_block_sync_startup(
    initial: FrontierUpdate,
    config: ZakuraBlockSyncConfig,
) -> (ZakuraSyncExchange, BlockSyncStartup) {
    let exchange = ZakuraSyncExchange::new(initial, ZakuraTrace::noop());
    let frontier = initial.frontier;
    let startup = BlockSyncStartup::new_with_exchange(
        BlockSyncFrontiers {
            finalized_height: frontier.finalized.height,
            verified_block_tip: frontier.verified_body.height,
            verified_block_hash: frontier.verified_body.hash,
        },
        (frontier.best_header.height, frontier.best_header.hash),
        exchange.subscribe_frontier(),
        config,
    );

    (exchange, startup)
}

fn round_trip(message: BlockSyncMessage) {
    let encoded = message.encode().expect("message encodes");
    let decoded = BlockSyncMessage::decode(&encoded).expect("message decodes");

    assert_eq!(decoded, message);
}

async fn next_event(events: &mut mpsc::Receiver<BlockSyncEvent>) -> BlockSyncEvent {
    tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("block-sync event should arrive")
        .expect("block-sync event channel should stay open")
}

async fn next_action(actions: &mut mpsc::Receiver<BlockSyncAction>) -> BlockSyncAction {
    tokio::time::timeout(Duration::from_secs(1), actions.recv())
        .await
        .expect("block-sync action should arrive")
        .expect("block-sync action channel should stay open")
}

async fn wait_for_query_needed_blocks(
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    verified_block_tip: block::Height,
    best_header_tip: block::Height,
) {
    loop {
        match next_action(actions).await {
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip: actual_verified,
                best_header_tip: actual_best,
            } if actual_verified == verified_block_tip && actual_best == best_header_tip => return,
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before target QueryNeededBlocks: {action:?}"),
        }
    }
}

/// Push one decoded stream-6 message to a peer's inbound stream as a real frame.
///
/// The inbound data flow is inverted: a peer's frames are decoded and dispatched
/// by its per-peer pipe-routine, not the reactor. Tests that previously injected a
/// `BlockSyncEvent::WireMessage{peer,msg}` shortcut now push the encoded frame onto
/// the peer's `inbound_tx` (the same `FramedSend` `connect_peer_with_status`
/// returns), exactly as the real transport would deliver it.
async fn send_inbound(inbound_tx: &FramedSend, msg: BlockSyncMessage) {
    inbound_tx
        .send(msg.encode_frame().expect("message encodes"))
        .await
        .expect("inbound frame queues onto the peer stream");
}

async fn next_outbound_message(outbound: &mut FramedRecv) -> BlockSyncMessage {
    let frame = tokio::time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .expect("outbound frame arrives")
        .expect("outbound channel is live");
    BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
}

async fn wait_for_outbound_block(outbound: &mut FramedRecv) -> Arc<block::Block> {
    loop {
        match next_outbound_message(outbound).await {
            BlockSyncMessage::Block(block) => return block,
            BlockSyncMessage::Status(_) | BlockSyncMessage::GetBlocks { .. } => {}
            msg => panic!("unexpected outbound message before block: {msg:?}"),
        }
    }
}

async fn wait_for_outbound_blocks_done(outbound: &mut FramedRecv) -> (block::Height, u32) {
    loop {
        match next_outbound_message(outbound).await {
            BlockSyncMessage::BlocksDone {
                start_height,
                returned,
            } => return (start_height, returned),
            BlockSyncMessage::Status(_) | BlockSyncMessage::GetBlocks { .. } => {}
            msg => panic!("unexpected outbound message before BlocksDone: {msg:?}"),
        }
    }
}

async fn wait_for_outbound_range_unavailable(outbound: &mut FramedRecv) -> (block::Height, u32) {
    loop {
        match next_outbound_message(outbound).await {
            BlockSyncMessage::RangeUnavailable {
                start_height,
                count,
            } => return (start_height, count),
            BlockSyncMessage::Status(_) | BlockSyncMessage::GetBlocks { .. } => {}
            msg => panic!("unexpected outbound message before RangeUnavailable: {msg:?}"),
        }
    }
}

/// Read this peer's real outbound until the node sends it a `GetBlocks`, returning
/// the requested range. The peer is implicit in `outbound` (the node→peer stream
/// the routine writes its real `try_send_get_blocks` to), so unlike the old
/// mirror-based `wait_for_getblocks` there is no `peer` to return: a test that
/// needs to assert *which* peer was asked reads that peer's own `outbound`. Skips
/// `Status` frames, which interleave with requests as status refreshes.
async fn wait_for_outbound_getblocks(outbound: &mut FramedRecv) -> (block::Height, u32) {
    loop {
        match next_outbound_message(outbound).await {
            BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } => return (start_height, count),
            BlockSyncMessage::Status(_) => {}
            msg => panic!("unexpected outbound message before GetBlocks: {msg:?}"),
        }
    }
}

/// Wait for the node's `Status` advertisement on this peer's real outbound — the
/// connect status the reactor sends on `PeerConnected`, and later refreshes.
/// Replaces the mirror-based `wait_for_connect_status`; the peer is implicit in
/// `outbound`, and the advertised status is returned for tests that assert on it.
async fn wait_for_outbound_status(outbound: &mut FramedRecv) -> BlockSyncStatus {
    match next_outbound_message(outbound).await {
        BlockSyncMessage::Status(status) => status,
        msg => panic!("unexpected outbound message before Status: {msg:?}"),
    }
}

/// Multi-peer replacement for the old mirror-based `wait_for_getblocks` in tests
/// where work assignment between peers is nondeterministic. The previous mirror
/// serialized every peer's outbound on one global `actions` channel and returned
/// which peer was asked; now the only real signal is each peer's own outbound
/// `FramedRecv`, so we `select!` across all of them and return the `(peer, start,
/// count)` of whichever peer the routine actually sent a `GetBlocks` to. Skips
/// `Status` frames (status refreshes interleave with requests). `peers` pairs each
/// peer id with its own outbound; the returned peer is the one whose stream the
/// `GetBlocks` arrived on, so peer-targeting is still proven against the real wire.
async fn wait_for_getblocks_across(
    peers: &mut [(ZakuraPeerId, &mut FramedRecv)],
) -> (ZakuraPeerId, block::Height, u32) {
    use futures::stream::{FuturesUnordered, StreamExt};
    loop {
        let (peer_id, frame) = {
            let mut recvs: FuturesUnordered<_> = peers
                .iter_mut()
                .map(|(peer_id, outbound)| {
                    let peer_id = peer_id.clone();
                    async move {
                        let frame = outbound.recv().await.expect("outbound channel is live");
                        (peer_id, frame)
                    }
                })
                .collect();
            tokio::time::timeout(Duration::from_secs(1), recvs.next())
                .await
                .expect("an outbound frame arrives on some peer")
                .expect("at least one peer outbound is live")
        };
        match BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes") {
            BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } => return (peer_id, start_height, count),
            BlockSyncMessage::Status(_) => {}
            msg => panic!("unexpected outbound message before GetBlocks: {msg:?}"),
        }
    }
}

async fn drain_parent_first_actions(
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    verified_tip: &mut block::Height,
    expected_new_fork: Option<&[Arc<block::Block>]>,
) {
    while let Ok(Some(action)) =
        tokio::time::timeout(Duration::from_millis(25), actions.recv()).await
    {
        match action {
            BlockSyncAction::SubmitBlock { block, .. } => {
                let height = block
                    .coinbase_height()
                    .expect("submitted test block has height");
                assert_eq!(
                    Some(height),
                    next_height(*verified_tip),
                    "block sync must submit only the contiguous parent-first prefix"
                );
                if let Some(new_fork) = expected_new_fork {
                    let expected_hash = match height.0 {
                        2 => new_fork[1].hash(),
                        3 => new_fork[2].hash(),
                        _ => panic!("unexpected post-reset submitted height: {height:?}"),
                    };
                    assert_eq!(
                        block.hash(),
                        expected_hash,
                        "post-reset submissions must follow the re-derived fork"
                    );
                }
                *verified_tip = height;
            }
            BlockSyncAction::Misbehavior {
                reason: BlockSyncMisbehavior::InvalidBlock | BlockSyncMisbehavior::UnsolicitedBlock,
                ..
            } => {}
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action while draining body responses: {action:?}"),
        }
    }
}

/// Build a `DownloadWindow` (the per-peer adaptive outbound window that per-peer routines moved
/// off `PeerBlockState` into the spawned `PeerRoutine`). The window math is the
/// same the routine drives; these unit tests pin it in isolation.
fn download_window() -> DownloadWindow {
    DownloadWindow::new(&ZakuraBlockSyncConfig::default())
}

fn test_delivery_snapshot(now: Instant) -> DeliverySnapshot {
    DeliverySnapshot {
        delivered: 0,
        delivered_at: now,
    }
}

fn window_request(height: u32) -> OutstandingBlockRange {
    let byte = u8::try_from(height).expect("test heights fit in u8");
    let now = Instant::now();
    OutstandingBlockRange {
        request: BlockRangeRequest {
            start_height: block::Height(height),
            count: 1,
            anchor_hash: block::Hash([byte; 32]),
            estimated_bytes: 1,
            expected_blocks: vec![ExpectedBlock {
                height: block::Height(height),
                hash: block::Hash([byte; 32]),
                estimated_bytes: 1,
            }],
        },
        queued_at: now,
        deadline: now,
        delivery_snapshot: test_delivery_snapshot(now),
        delivered_bytes: 0,
        received: ReceivedBlockTracker::default(),
    }
}

fn window_request_range(start: u32, count: u32) -> OutstandingBlockRange {
    let byte = u8::try_from(start).expect("test heights fit in u8");
    let now = Instant::now();
    OutstandingBlockRange {
        request: BlockRangeRequest {
            start_height: block::Height(start),
            count,
            anchor_hash: block::Hash([byte; 32]),
            estimated_bytes: u64::from(count),
            expected_blocks: (start..start + count)
                .map(|height| ExpectedBlock {
                    height: block::Height(height),
                    hash: block::Hash([u8::try_from(height).expect("test heights fit in u8"); 32]),
                    estimated_bytes: 1,
                })
                .collect(),
        },
        queued_at: now,
        deadline: now,
        delivery_snapshot: test_delivery_snapshot(now),
        delivered_bytes: 0,
        received: ReceivedBlockTracker::default(),
    }
}

#[test]
fn block_liveness_disconnects_silent_active_peer_after_default_timeout() {
    let config = ZakuraBlockSyncConfig::default();
    let timeout = config.effective_liveness_timeout();
    assert_eq!(timeout, Duration::from_secs(32));

    let now = Instant::now();
    let mut window = download_window();
    window.outstanding.push(window_request(1));
    window.arm_liveness(now, timeout);

    assert_eq!(
        window.check_liveness(now + timeout - Duration::from_millis(1)),
        LivenessOutcome::Ok
    );
    assert_eq!(
        window.check_liveness(now + timeout),
        LivenessOutcome::Disconnect
    );
}

#[test]
fn block_liveness_never_disconnects_idle_peer() {
    let now = Instant::now();
    let mut window = download_window();

    assert_eq!(window.check_liveness(now), LivenessOutcome::Ok);

    window.block_liveness_deadline = Some(now);
    assert_eq!(window.check_liveness(now), LivenessOutcome::Disarm);
    window.disarm_liveness_if_idle();
    assert_eq!(window.block_liveness_deadline, None);
    assert_eq!(window.check_liveness(now), LivenessOutcome::Ok);
}

#[test]
fn block_liveness_progress_before_deadline_keeps_peer_alive() {
    let timeout = ZakuraBlockSyncConfig::default().effective_liveness_timeout();
    let mut now = Instant::now();
    let mut window = download_window();
    window.outstanding.push(window_request(1));
    window.arm_liveness(now, timeout);

    for _ in 0..4 {
        now += timeout - Duration::from_millis(1);
        assert_eq!(window.check_liveness(now), LivenessOutcome::Ok);
        window.note_block_progress(now, timeout);
        assert_eq!(window.block_liveness_deadline, Some(now + timeout));
    }
}

#[test]
fn block_liveness_disarms_when_outstanding_drains() {
    let timeout = ZakuraBlockSyncConfig::default().effective_liveness_timeout();
    let now = Instant::now();
    let mut window = download_window();
    window.outstanding.push(window_request(1));
    window.arm_liveness(now, timeout);

    window.outstanding.clear();
    window.disarm_liveness_if_idle();

    assert_eq!(window.block_liveness_deadline, None);
    assert_eq!(window.check_liveness(now + timeout), LivenessOutcome::Ok);
}

#[test]
fn block_liveness_resuming_after_idle_gets_fresh_deadline() {
    let timeout = ZakuraBlockSyncConfig::default().effective_liveness_timeout();
    let now = Instant::now();
    let mut window = download_window();
    window.outstanding.push(window_request(1));
    window.arm_liveness(now, timeout);
    window.outstanding.clear();
    window.disarm_liveness_if_idle();

    let resumed = now + Duration::from_secs(60);
    window.outstanding.push(window_request(2));
    window.arm_liveness(resumed, timeout);

    assert_eq!(window.block_liveness_deadline, Some(resumed + timeout));
}

#[test]
fn block_liveness_multi_block_range_progress_resets_each_body() {
    let timeout = ZakuraBlockSyncConfig::default().effective_liveness_timeout();
    let start = Instant::now();
    let mut window = download_window();
    window.outstanding.push(window_request_range(1, 3));
    window.arm_liveness(start, timeout);

    let first = start + Duration::from_secs(4);
    window.note_block_progress(first, timeout);
    assert_eq!(window.block_liveness_deadline, Some(first + timeout));

    let second = first + Duration::from_secs(4);
    assert_eq!(window.check_liveness(second), LivenessOutcome::Ok);
    window.note_block_progress(second, timeout);
    assert_eq!(window.block_liveness_deadline, Some(second + timeout));

    let third = second + Duration::from_secs(4);
    assert_eq!(window.check_liveness(third), LivenessOutcome::Ok);
    window.note_block_progress(third, timeout);
    assert_eq!(window.block_liveness_deadline, Some(third + timeout));
}

// The old `BlockRangeScheduler` single-pass timeout-retry bias
// (`scheduler_retry_after_timeout_*`) is removed: the WorkQueue has no
// per-peer assignment to bias, so a returned height is simply contestable by any
// servable peer. The peer-local timeout bias is re-introduced in per-peer routines. The
// reactor-level locality property is still covered by
// `reactor_timeout_recovery_is_local_and_healthy_peer_keeps_filling`.
#[test]
fn work_queue_returned_height_is_contestable_by_any_peer() {
    let queue = work_queue_with(0, [needed(1, BlockSizeEstimate::Advertised(100))]);

    // A peer takes the floor height (it leaves `pending` → `in_flight`).
    let taken = queue.take_in_range(block::Height(1), block::Height(1), 1);
    assert_eq!(taken.len(), 1);
    assert!(queue.in_flight_contains(block::Height(1)));
    assert!(!queue.pending_contains(block::Height(1)));

    // Its request times out: the height returns to `pending`, where any servable
    // peer (not just the original holder) can take it again. No bias toward or
    // away from any particular peer exists in WorkQueue.
    queue.return_items([block::Height(1)]);
    assert!(queue.pending_contains(block::Height(1)));
    assert_eq!(
        queue
            .take_in_range(block::Height(1), block::Height(1), 1)
            .len(),
        1,
        "a returned height must be immediately re-takable"
    );
}

async fn connect_peer_with_status(
    service: &BlockSyncService,
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    byte: u8,
    servable_high: block::Height,
    tip_hash: block::Hash,
    max_inflight_requests: u32,
    max_response_bytes: u32,
) -> (ZakuraPeerId, FramedSend, FramedRecv) {
    connect_peer_with_status_message(
        service,
        actions,
        byte,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high,
            tip_hash,
            max_blocks_per_response: 16,
            max_inflight_requests,
            max_response_bytes,
        },
    )
    .await
}

async fn connect_peer_with_status_message(
    service: &BlockSyncService,
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    byte: u8,
    status: BlockSyncStatus,
) -> (ZakuraPeerId, FramedSend, FramedRecv) {
    let peer = peer(byte);
    let (inbound_tx, inbound_rx) = framed_channel(16);
    let (outbound_tx, mut outbound_rx) = framed_channel(16);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    // Read the connect-status the reactor sends on `PeerConnected` off this peer's
    // own real outbound (reading the peer's own stream proves targeting). The
    // `actions` channel is still passed through for callers that drain it.
    let _ = actions;
    wait_for_outbound_status(&mut outbound_rx).await;
    inbound_tx
        .send(
            BlockSyncMessage::Status(status)
                .encode_frame()
                .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    (peer, inbound_tx, outbound_rx)
}

fn needed(height: u32, size: BlockSizeEstimate) -> (block::Height, block::Hash, BlockSizeEstimate) {
    (block::Height(height), block::Hash([height as u8; 32]), size)
}

/// Build a fresh `WorkQueue` seeded above-floor with the given needed items, for
/// the WorkQueue unit tests that replaced the old `BlockRangeScheduler` tests.
fn work_queue_with(
    floor: u32,
    items: impl IntoIterator<Item = (block::Height, block::Hash, BlockSizeEstimate)>,
) -> super::work_queue::WorkQueue {
    let queue = super::work_queue::WorkQueue::new(block::Height(floor));
    queue.set_estimate_floor_for_tests(1);
    queue.extend(items);
    queue
}

fn block_meta(block: &Arc<block::Block>) -> BlockSyncBlockMeta {
    BlockSyncBlockMeta {
        height: block.coinbase_height().expect("test block has height"),
        hash: block.hash(),
        size: BlockSizeEstimate::Advertised(block_size(block)),
    }
}

#[test]
fn block_sync_config_defaults_and_round_trips() {
    let default = ZakuraBlockSyncConfig::default();
    assert_eq!(default.max_blocks_per_response, 1);
    assert_eq!(default.max_inflight_requests, 32000);
    assert_eq!(
        default.max_inflight_block_bytes,
        DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES
    );
    assert_eq!(
        default.max_reorder_lookahead_bytes,
        DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BYTES
    );
    assert_eq!(
        default.max_reorder_lookahead_blocks,
        DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BLOCKS
    );
    assert_eq!(
        default.floor_peer_avoid_cooldown,
        DEFAULT_BS_FLOOR_PEER_AVOID_COOLDOWN
    );
    assert_eq!(
        default.no_progress_peer_cooldown,
        DEFAULT_BS_NO_PROGRESS_PEER_COOLDOWN
    );
    assert_eq!(
        default.effective_max_reorder_lookahead_bytes(),
        DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BYTES
    );
    assert_eq!(
        default.floor_request_byte_reservation(),
        u64::from(DEFAULT_BS_MAX_RESPONSE_BYTES)
    );
    assert_eq!(
        default.effective_floor_peer_avoid_cooldown(),
        DEFAULT_BS_FLOOR_PEER_AVOID_COOLDOWN
    );
    assert_eq!(
        default.effective_no_progress_peer_cooldown(),
        DEFAULT_BS_NO_PROGRESS_PEER_COOLDOWN
    );
    assert!(default.validate().is_ok());
    assert_eq!(
        default.max_submitted_block_applies,
        DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES
    );
    assert_eq!(
        default.submitted_apply_limit(),
        DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES
    );
    assert_eq!(default.request_timeout, DEFAULT_BS_REQUEST_TIMEOUT);
    assert_eq!(default.fanout, DEFAULT_BS_FANOUT);

    let encoded = toml::to_string(&default).expect("block-sync config serializes");
    let decoded: ZakuraBlockSyncConfig =
        toml::from_str(&encoded).expect("block-sync config deserializes");
    assert_eq!(decoded, default);

    let config: crate::Config = toml::from_str(
        r#"
        [zakura.block_sync]
        max_submitted_block_applies = 9
        "#,
    )
    .expect("nested Zakura block-sync config deserializes");
    assert_eq!(config.zakura.block_sync.max_submitted_block_applies, 9);
    assert_eq!(
        config.zakura.block_sync.submitted_apply_limit(),
        DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES,
    );
}

#[test]
fn config_validate_rejects_degenerate_values() {
    let mut config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: 0,
        ..ZakuraBlockSyncConfig::default()
    };
    assert!(config.validate().is_err());

    config = ZakuraBlockSyncConfig {
        max_reorder_lookahead_bytes: 0,
        ..ZakuraBlockSyncConfig::default()
    };
    assert!(config.validate().is_err());

    config = ZakuraBlockSyncConfig {
        max_reorder_lookahead_blocks: 0,
        ..ZakuraBlockSyncConfig::default()
    };
    assert!(config.validate().is_err());

    // A positive budget below the checkpoint-range floor is no longer rejected by
    // `validate`: it is clamped up to the floor (with a warning) at load instead,
    // so older configs keep starting. See
    // `config_clamps_below_floor_inflight_block_bytes`.
    config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_CHECKPOINT_RANGE_BYTE_FLOOR - 1,
        ..ZakuraBlockSyncConfig::default()
    };
    assert!(config.validate().is_ok());

    config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_CHECKPOINT_RANGE_BYTE_FLOOR,
        max_reorder_lookahead_bytes: BS_CHECKPOINT_RANGE_BYTE_FLOOR,
        ..ZakuraBlockSyncConfig::default()
    };
    assert!(config.validate().is_ok());

    config = ZakuraBlockSyncConfig {
        request_timeout: Duration::ZERO,
        ..ZakuraBlockSyncConfig::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn config_clamps_below_floor_inflight_block_bytes() {
    // A positive budget below the checkpoint-range floor is clamped up to the
    // floor so checkpoint sync cannot deadlock (instead of refusing to start).
    let mut below = ZakuraBlockSyncConfig {
        // 256 MiB, the historical `v4.5.0-zakura-blocksync.toml` value, which is
        // below the ~802 MB checkpoint-range floor.
        max_inflight_block_bytes: 256 * 1024 * 1024,
        ..ZakuraBlockSyncConfig::default()
    };
    assert!(below.max_inflight_block_bytes < BS_CHECKPOINT_RANGE_BYTE_FLOOR);
    below.clamp_inflight_block_bytes_to_floor();
    assert_eq!(
        below.max_inflight_block_bytes,
        BS_CHECKPOINT_RANGE_BYTE_FLOOR
    );

    // A budget at or above the floor is left untouched.
    let mut at_floor = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_CHECKPOINT_RANGE_BYTE_FLOOR + 1,
        ..ZakuraBlockSyncConfig::default()
    };
    at_floor.clamp_inflight_block_bytes_to_floor();
    assert_eq!(
        at_floor.max_inflight_block_bytes,
        BS_CHECKPOINT_RANGE_BYTE_FLOOR + 1
    );

    // Zero is left untouched so `validate` still rejects it as a misconfiguration.
    let mut zero = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: 0,
        ..ZakuraBlockSyncConfig::default()
    };
    zero.clamp_inflight_block_bytes_to_floor();
    assert_eq!(zero.max_inflight_block_bytes, 0);
    assert!(zero.validate().is_err());
}

#[test]
fn config_deserialize_clamps_below_floor_inflight_block_bytes() {
    // Regression: an older config with a too-small `max_inflight_block_bytes`
    // (e.g. the stored `v4.5.0-zakura-blocksync.toml`) must still load -- clamped
    // up to the checkpoint-range floor -- rather than being rejected at startup.
    let config: crate::Config = toml::from_str(
        r#"
        [zakura.block_sync]
        max_inflight_block_bytes = 268435456
        "#,
    )
    .expect("a below-floor max_inflight_block_bytes config still loads");
    assert_eq!(
        config.zakura.block_sync.max_inflight_block_bytes,
        BS_CHECKPOINT_RANGE_BYTE_FLOOR,
    );
}

#[test]
fn codec_round_trips_every_message_variant() {
    round_trip(BlockSyncMessage::Status(status()));
    round_trip(BlockSyncMessage::GetBlocks {
        start_height: block::Height(10),
        count: 3,
    });
    round_trip(BlockSyncMessage::Block(mainnet_block(
        &BLOCK_MAINNET_1_BYTES,
    )));
    round_trip(BlockSyncMessage::BlocksDone {
        start_height: block::Height(10),
        returned: 3,
    });
    round_trip(BlockSyncMessage::RangeUnavailable {
        start_height: block::Height(10),
        count: 3,
    });
}

#[test]
fn codec_round_trips_block_near_max_block_bytes() {
    let block = Arc::new(zebra_chain::block::tests::generate::large_multi_transaction_block());
    let serialized_len = block
        .zcash_serialize_to_vec()
        .expect("large test block serializes")
        .len();
    let max_block_bytes =
        usize::try_from(block::MAX_BLOCK_BYTES).expect("max block size fits in usize");

    assert!(
        serialized_len <= max_block_bytes && serialized_len > max_block_bytes - 1000,
        "test block should be close to the consensus cap, got {serialized_len}"
    );
    round_trip(BlockSyncMessage::Block(block));
}

#[test]
fn codec_rejects_malformed_discriminator_and_truncated_payload() {
    assert!(matches!(
        BlockSyncMessage::decode(&[99]),
        Err(BlockSyncWireError::UnknownMessageType(99))
    ));

    assert!(matches!(
        BlockSyncMessage::decode(&[MSG_BS_GET_BLOCKS, 1, 0]),
        Err(BlockSyncWireError::Io(_))
    ));
}

#[test]
fn codec_classifies_payloads_above_old_raw_stream6_cap() {
    let old_max_bs_message_bytes =
        usize::try_from(block::MAX_BLOCK_BYTES).expect("max block bytes fits in usize") + 1;
    let payload = vec![99; old_max_bs_message_bytes + 1];

    assert!(payload.len() <= MAX_BS_MESSAGE_BYTES);
    assert!(matches!(
        BlockSyncMessage::decode(&payload),
        Err(BlockSyncWireError::UnknownMessageType(99))
    ));
}

#[test]
fn codec_rejects_oversized_frame_and_oversized_block() {
    let oversized_payload = vec![0; MAX_BS_MESSAGE_BYTES + 1];
    assert!(matches!(
        BlockSyncMessage::decode(&oversized_payload),
        Err(BlockSyncWireError::OversizedPayload { .. })
    ));

    let oversized_block =
        Arc::new(zebra_chain::block::tests::generate::oversized_multi_transaction_block());
    assert!(matches!(
        BlockSyncMessage::Block(oversized_block).encode(),
        Err(BlockSyncWireError::OversizedBlock { .. })
            | Err(BlockSyncWireError::OversizedPayload { .. })
    ));
}

#[test]
fn codec_rejects_count_and_returned_over_cap() {
    let over_cap = MAX_BS_BLOCKS_PER_REQUEST + 1;

    assert!(matches!(
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(1),
            returned: 0,
        }
        .encode(),
        Err(BlockSyncWireError::ZeroBlockCount)
    ));

    let mut zero_count_get_blocks = vec![MSG_BS_GET_BLOCKS];
    zero_count_get_blocks.extend_from_slice(&1u32.to_le_bytes());
    zero_count_get_blocks.extend_from_slice(&0u32.to_le_bytes());
    assert!(matches!(
        BlockSyncMessage::decode(&zero_count_get_blocks),
        Err(BlockSyncWireError::ZeroBlockCount)
    ));

    let mut zero_count_range_unavailable = vec![MSG_BS_RANGE_UNAVAILABLE];
    zero_count_range_unavailable.extend_from_slice(&1u32.to_le_bytes());
    zero_count_range_unavailable.extend_from_slice(&0u32.to_le_bytes());
    assert!(matches!(
        BlockSyncMessage::decode(&zero_count_range_unavailable),
        Err(BlockSyncWireError::ZeroBlockCount)
    ));

    assert!(matches!(
        BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: over_cap,
        }
        .encode(),
        Err(BlockSyncWireError::BlockCountLimit { .. })
    ));

    assert!(matches!(
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(1),
            returned: over_cap,
        }
        .encode(),
        Err(BlockSyncWireError::BlockCountLimit { .. })
    ));

    assert!(matches!(
        BlockSyncMessage::RangeUnavailable {
            start_height: block::Height(1),
            count: over_cap,
        }
        .encode(),
        Err(BlockSyncWireError::BlockCountLimit { .. })
    ));
}

#[test]
fn frame_decode_rejects_mismatched_unknown_flags_and_trailing_payload() {
    let frame = Frame {
        message_type: u16::from(MSG_BS_GET_BLOCKS),
        flags: 1,
        payload: BlockSyncMessage::GetBlocks {
            start_height: block::Height(1),
            count: 1,
        }
        .encode()
        .expect("message encodes"),
    };
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame),
        Err(BlockSyncWireError::UnsupportedFlags(1))
    ));

    let mut payload = BlockSyncMessage::Status(status())
        .encode()
        .expect("message encodes");
    payload.push(0);
    assert!(matches!(
        BlockSyncMessage::decode(&payload),
        Err(BlockSyncWireError::TrailingBytes)
    ));

    let frame = Frame {
        message_type: u16::from(MSG_BS_BLOCK),
        flags: 0,
        payload: BlockSyncMessage::Status(status())
            .encode()
            .expect("message encodes"),
    };
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame),
        Err(BlockSyncWireError::MismatchedFrameMessageType { .. })
    ));
}

#[test]
fn status_decode_clamps_peer_capacity_advertisements() {
    let mut payload = Vec::new();
    payload.push(MSG_BS_STATUS);
    payload.extend_from_slice(&block::Height(1).0.to_le_bytes());
    payload.extend_from_slice(&block::Height(2).0.to_le_bytes());
    block::Hash([9; 32])
        .zcash_serialize(&mut payload)
        .expect("hash serializes");
    payload.extend_from_slice(&u32::MAX.to_le_bytes());
    payload.extend_from_slice(&u32::MAX.to_le_bytes());
    payload.extend_from_slice(&u32::MAX.to_le_bytes());

    let BlockSyncMessage::Status(status) =
        BlockSyncMessage::decode(&payload).expect("status decodes")
    else {
        panic!("expected status message");
    };

    assert_eq!(status.max_blocks_per_response, MAX_BS_BLOCKS_PER_REQUEST);
    assert_eq!(status.max_inflight_requests, MAX_BS_INFLIGHT_REQUESTS);
    assert_eq!(status.max_response_bytes, MAX_BS_RESPONSE_BYTES);
}

#[test]
fn aggregate_response_cap_is_not_the_per_frame_cap() {
    assert!(
        MAX_BS_RESPONSE_BYTES > u32::try_from(MAX_BS_MESSAGE_BYTES).expect("frame cap fits u32"),
        "range responses are multiple independently-capped block frames"
    );
    assert_eq!(
        ZakuraBlockSyncConfig::default().advertised_max_response_bytes(),
        MAX_BS_RESPONSE_BYTES
    );
}

// The byte-budget reservation and per-peer fairness logic moved from the old
// `BlockRangeScheduler::next_for_peer` into the reactor's `fill_peer` issuance
// path (commit-1 lossless worst-case reservation + commit-3 per-peer byte cap).
// Those are now exercised end-to-end by the reactor integration tests below
// (`reactor_fill_loop_*`, `reactor_keeps_issuing_*`); the WorkQueue itself only
// owns dedup / servable-range eligibility, asserted here.

#[test]
fn work_queue_take_dedups_a_height_across_peers() {
    // fanout = 1: a height taken by one peer leaves `pending`, so a second peer
    // querying the same range cannot also take it.
    let queue = work_queue_with(
        0,
        [
            needed(1, BlockSizeEstimate::Advertised(10_000)),
            needed(2, BlockSizeEstimate::Advertised(10_000)),
            needed(3, BlockSizeEstimate::Advertised(10_000)),
        ],
    );

    let first = queue.take_in_range(
        block::Height(1),
        block::Height(3),
        MAX_BS_BLOCKS_PER_REQUEST as usize,
    );
    assert_eq!(
        first.iter().map(|(height, _)| height.0).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(queue.in_flight_len(), 3);

    let second = queue.take_in_range(
        block::Height(1),
        block::Height(3),
        MAX_BS_BLOCKS_PER_REQUEST as usize,
    );
    assert!(
        second.is_empty(),
        "a height taken by one peer must not be re-takable by another (dedup)"
    );
}

#[test]
fn work_queue_extend_dedups_against_pending_in_flight_and_floor() {
    let queue = work_queue_with(
        5,
        [
            // At/below the floor: rejected.
            needed(3, BlockSizeEstimate::Advertised(100)),
            needed(5, BlockSizeEstimate::Advertised(100)),
            // Above the floor: accepted.
            needed(6, BlockSizeEstimate::Advertised(100)),
            needed(7, BlockSizeEstimate::Advertised(100)),
        ],
    );
    assert_eq!(queue.pending_len(), 2);
    assert!(!queue.pending_contains(block::Height(5)));

    // Take h6 into `in_flight`, then re-extend with h6 (in flight) and h7
    // (already pending): both are skipped, only a genuinely new height inserts.
    queue.take_in_range(block::Height(6), block::Height(6), 1);
    let inserted = queue.extend([
        needed(6, BlockSizeEstimate::Advertised(100)), // in flight
        needed(7, BlockSizeEstimate::Advertised(100)), // already pending
        needed(8, BlockSizeEstimate::Advertised(100)), // new
    ]);
    assert_eq!(inserted, 1, "only the genuinely new height is inserted");
    assert!(queue.pending_contains(block::Height(8)));
    assert!(!queue.pending_contains(block::Height(6)));
}

#[test]
fn work_queue_take_respects_servable_range_contiguity_and_max() {
    // Heights 10,11,12 then a gap then 20,21.
    let queue = work_queue_with(
        0,
        [
            needed(10, BlockSizeEstimate::Advertised(100)),
            needed(11, BlockSizeEstimate::Advertised(100)),
            needed(12, BlockSizeEstimate::Advertised(100)),
            needed(20, BlockSizeEstimate::Advertised(100)),
            needed(21, BlockSizeEstimate::Advertised(100)),
        ],
    );

    // `max` bounds the chunk.
    let chunk = queue.take_in_range(block::Height(10), block::Height(30), 2);
    assert_eq!(
        chunk.iter().map(|(height, _)| height.0).collect::<Vec<_>>(),
        vec![10, 11]
    );

    // The chunk stops at the gap (12 then nothing until 20): a take over the full
    // range returns only the contiguous run 12, not 12,20,21.
    let run = queue.take_in_range(
        block::Height(12),
        block::Height(30),
        MAX_BS_BLOCKS_PER_REQUEST as usize,
    );
    assert_eq!(
        run.iter().map(|(height, _)| height.0).collect::<Vec<_>>(),
        vec![12],
        "take must stop at the first gap so the chunk is one contiguous request"
    );

    // `low` excludes lower heights; the next contiguous run is 20,21.
    let high = queue.take_in_range(
        block::Height(20),
        block::Height(30),
        MAX_BS_BLOCKS_PER_REQUEST as usize,
    );
    assert_eq!(
        high.iter().map(|(height, _)| height.0).collect::<Vec<_>>(),
        vec![20, 21]
    );
}

#[test]
fn work_queue_budgeted_take_respects_count_cap() {
    let queue = work_queue_with(
        0,
        (1..=4).map(|height| needed(height, BlockSizeEstimate::Advertised(100))),
    );

    let taken = queue.take_in_range_budgeted(block::Height(1), block::Height(4), 2, u64::MAX);

    assert_eq!(
        taken.iter().map(|(height, _)| height.0).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn work_queue_budgeted_take_respects_estimated_byte_cap() {
    let queue = work_queue_with(
        0,
        [
            needed(1, BlockSizeEstimate::Advertised(100)),
            needed(2, BlockSizeEstimate::Advertised(150)),
            needed(3, BlockSizeEstimate::Advertised(1)),
        ],
    );

    let taken = queue.take_in_range_budgeted(block::Height(1), block::Height(3), 3, 250);

    assert_eq!(
        taken.iter().map(|(height, _)| height.0).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(queue.pending_contains(block::Height(3)));
}

#[test]
fn work_queue_budgeted_take_stops_at_gaps() {
    let queue = work_queue_with(
        0,
        [
            needed(10, BlockSizeEstimate::Advertised(100)),
            needed(11, BlockSizeEstimate::Advertised(100)),
            needed(13, BlockSizeEstimate::Advertised(100)),
        ],
    );

    let taken = queue.take_in_range_budgeted(block::Height(10), block::Height(13), 3, u64::MAX);

    assert_eq!(
        taken.iter().map(|(height, _)| height.0).collect::<Vec<_>>(),
        vec![10, 11]
    );
    assert!(queue.pending_contains(block::Height(13)));
}

#[test]
fn work_queue_budgeted_take_takes_one_oversized_first_item_for_progress() {
    let queue = work_queue_with(
        0,
        [
            needed(1, BlockSizeEstimate::Advertised(500)),
            needed(2, BlockSizeEstimate::Advertised(1)),
        ],
    );

    let taken = queue.take_in_range_budgeted(block::Height(1), block::Height(2), 2, 100);

    assert_eq!(
        taken.iter().map(|(height, _)| height.0).collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(taken[0].1.estimated_bytes, 500);
    assert!(queue.pending_contains(block::Height(2)));
}

#[test]
fn work_queue_budgeted_take_preserves_estimates_through_take_and_return() {
    let queue = work_queue_with(0, [needed(10, BlockSizeEstimate::Advertised(12_345))]);

    let taken = queue.take_in_range_budgeted(block::Height(10), block::Height(10), 1, 1);
    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].1.estimated_bytes, 12_345);

    queue.return_items([block::Height(10)]);
    let retaken = queue.take_in_range_budgeted(block::Height(10), block::Height(10), 1, 1);
    assert_eq!(retaken.len(), 1);
    assert_eq!(retaken[0].1.estimated_bytes, 12_345);
}

#[test]
fn admission_blocks_above_floor_at_cap_but_keeps_floor_fundable() {
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: 40_000_000,
        max_reorder_lookahead_bytes: 500,
        max_reorder_lookahead_blocks: 4,
        ..ZakuraBlockSyncConfig::default()
    };
    let snapshot = super::admission::AdmissionSnapshot {
        download_floor: block::Height(10),
        reorder_buffered_bytes: 500,
        reorder_buffered_blocks: 1,
        applying_buffered_bytes: 0,
        applying_buffered_blocks: 0,
        sequencer_input_queued_bytes: 0,
        reserved_above_floor_bytes: 0,
        reserved_above_floor_blocks: 0,
        budget_available: 40_000_000,
    };

    let floor = super::admission::admission_decision(&config, snapshot, block::Height(11), 1_000)
        .expect("floor rescue remains admitted at the look-ahead cap");
    assert_eq!(floor.priority, super::admission::RequestPriority::Floor);
    assert_eq!(floor.max_request_bytes, 1_000);

    assert_eq!(
        super::admission::admission_decision(&config, snapshot, block::Height(12), 1_000),
        None,
        "above-floor work stops at the look-ahead cap"
    );

    let under_cap = super::admission::AdmissionSnapshot {
        reorder_buffered_bytes: 100,
        budget_available: 40_000_000,
        ..snapshot
    };
    let above =
        super::admission::admission_decision(&config, under_cap, block::Height(12), u64::MAX)
            .expect("above-floor work is admitted below the cap");
    assert_eq!(
        above.priority,
        super::admission::RequestPriority::AboveFloor
    );
    assert_eq!(above.max_request_bytes, 400);
}

#[test]
fn admission_counts_inflight_to_sequencer_bytes() {
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: 64_000_000,
        max_reorder_lookahead_bytes: 1_000,
        max_reorder_lookahead_blocks: 10,
        ..ZakuraBlockSyncConfig::default()
    };
    let snapshot = super::admission::AdmissionSnapshot {
        download_floor: block::Height(10),
        reorder_buffered_bytes: 200,
        reorder_buffered_blocks: 1,
        applying_buffered_bytes: 200,
        applying_buffered_blocks: 1,
        sequencer_input_queued_bytes: 600,
        reserved_above_floor_bytes: 0,
        reserved_above_floor_blocks: 0,
        budget_available: 64_000_000,
    };

    assert_eq!(
        super::admission::admission_decision(&config, snapshot, block::Height(12), 1_000),
        None,
        "above-floor admission includes bytes already queued to the sequencer"
    );
}

#[test]
fn total_resident_plateaus_under_commit_stall() {
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: 64_000_000,
        max_reorder_lookahead_bytes: 1_000,
        max_reorder_lookahead_blocks: 10,
        ..ZakuraBlockSyncConfig::default()
    };
    let snapshot = super::admission::AdmissionSnapshot {
        download_floor: block::Height(10),
        reorder_buffered_bytes: 300,
        reorder_buffered_blocks: 1,
        applying_buffered_bytes: 700,
        applying_buffered_blocks: 1,
        sequencer_input_queued_bytes: 0,
        reserved_above_floor_bytes: 0,
        reserved_above_floor_blocks: 0,
        budget_available: 64_000_000,
    };

    assert_eq!(
        super::admission::admission_decision(&config, snapshot, block::Height(12), 1_000),
        None,
        "above-floor admission includes applying bytes held during a commit stall"
    );
}

#[test]
fn floor_priority_request_does_not_buffer_above_floor_past_cap() {
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: 64_000_000,
        max_reorder_lookahead_bytes: 1_000,
        max_reorder_lookahead_blocks: 10,
        ..ZakuraBlockSyncConfig::default()
    };
    let capped = super::admission::AdmissionSnapshot {
        download_floor: block::Height(10),
        reorder_buffered_bytes: 1_000,
        reorder_buffered_blocks: 1,
        applying_buffered_bytes: 0,
        applying_buffered_blocks: 0,
        sequencer_input_queued_bytes: 0,
        reserved_above_floor_bytes: 0,
        reserved_above_floor_blocks: 0,
        budget_available: 64_000_000,
    };

    assert_eq!(
        super::admission::admission_decision(&config, capped, block::Height(12), 1_000),
        None,
        "the above-floor tail of a floor-starting request is refused at the cap"
    );
    assert_eq!(
        super::admission::admission_decision(&config, capped, block::Height(11), 1_000)
            .expect("floor height remains fundable")
            .priority,
        super::admission::RequestPriority::Floor
    );
}

#[test]
fn work_queue_force_cancel_and_owner_timeout_release_once() {
    let queue = work_queue_with(0, [needed(1, BlockSizeEstimate::Advertised(100))]);
    let mut budget = ByteBudget::new(1_000);
    let taken = queue.take_in_range(block::Height(1), block::Height(1), 1);
    assert_eq!(taken.len(), 1);
    assert!(budget.try_reserve(100));
    assert_eq!(queue.mark_reserved([block::Height(1)]), 100);
    assert_eq!(budget.reserved(), 100);

    let watchdog_released = queue.release_and_return_items([block::Height(1)]);
    budget.release(watchdog_released);
    assert_eq!(watchdog_released, 100);
    assert_eq!(budget.reserved(), 0);
    assert!(queue.pending_contains(block::Height(1)));

    let late_owner_released = queue.release_and_return_items([block::Height(1)]);
    budget.release(late_owner_released);
    assert_eq!(late_owner_released, 0);
    assert_eq!(budget.reserved(), 0);
    assert!(queue.pending_contains(block::Height(1)));
}

#[test]
fn work_queue_reserved_bytes_counter_matches_scan_across_transitions() {
    // Exercise every ledger transition and assert the O(1) `reserved_bytes` counter
    // never drifts from the O(n) ground-truth scan.
    let queue = work_queue_with(
        0,
        (1..=6).map(|h| needed(h, BlockSizeEstimate::Advertised(100))),
    );
    let check = |label: &str| {
        assert_eq!(
            queue.reserved_bytes(),
            queue.reserved_bytes_scanned(),
            "reserved_bytes counter drifted from scan after {label}"
        );
    };
    // Nothing reserved yet: all pending items are `Released`.
    assert_eq!(queue.reserved_bytes(), 0);
    check("seed");

    // take + mark_reserved: Released -> Reserved.
    let taken = queue.take_in_range(block::Height(1), block::Height(6), 6);
    assert_eq!(taken.len(), 6);
    assert_eq!(queue.mark_reserved((1..=6).map(block::Height)), 600);
    assert_eq!(queue.reserved_bytes(), 600);
    check("mark_reserved");

    // settle: Reserved -> Held drops the reservation for height 1.
    queue
        .settle_active_reserved_height(block::Height(1), 80)
        .expect("height 1 is reserved");
    assert_eq!(queue.reserved_bytes(), 500);
    check("settle");

    // mark_held_direct: Reserved -> Held drops height 2's reservation.
    queue.mark_held_direct(block::Height(2), 90);
    assert_eq!(queue.reserved_bytes(), 400);
    check("mark_held_direct");

    // release_heights: Reserved -> Released for height 3.
    queue.release_heights([block::Height(3)]);
    assert_eq!(queue.reserved_bytes(), 300);
    check("release_heights");

    // release_reserved_and_return_items: only the still-reserved height 4 releases.
    let released = queue.release_reserved_and_return_items([block::Height(4)]);
    assert_eq!(released, 100);
    assert_eq!(queue.reserved_bytes(), 200);
    check("release_reserved_and_return_items");

    // advance_floor drops the committed `<= floor` prefix. Heights 1 (Held) and 2
    // (Held) contribute no reservation; height 3 is already Released. Only heights
    // 5 and 6 remain reserved above the new floor.
    let released = queue.advance_floor(block::Height(4));
    assert_eq!(released, 0, "heights <= 4 owned no live reservation");
    assert!(!queue.pending_contains(block::Height(1)));
    assert!(!queue.in_flight_contains(block::Height(2)));
    assert_eq!(queue.reserved_bytes(), 200);
    check("advance_floor");

    // reset_above drops the `> floor` suffix (heights 5 and 6), releasing their
    // reservations and zeroing the counter.
    let released = queue.reset_above(block::Height(4));
    assert_eq!(released, 200);
    assert_eq!(queue.reserved_bytes(), 0);
    assert!(!queue.in_flight_contains(block::Height(5)));
    assert!(!queue.in_flight_contains(block::Height(6)));
    check("reset_above");
}

#[test]
fn work_queue_advance_floor_drops_only_committed_prefix() {
    let queue = work_queue_with(
        0,
        (1..=10).map(|h| needed(h, BlockSizeEstimate::Advertised(100))),
    );
    // Reserve a contiguous run so we can see the reservation accounting on GC.
    let _ = queue.take_in_range(block::Height(1), block::Height(10), 10);
    assert_eq!(queue.mark_reserved((1..=10).map(block::Height)), 1000);

    // advance_floor to 4: drops heights 1..=4 (still reserved -> released), keeps 5..=10.
    let released = queue.advance_floor(block::Height(4));
    assert_eq!(released, 400, "reserved bytes for the dropped 1..=4 prefix");
    for h in 1..=4 {
        assert!(!queue.in_flight_contains(block::Height(h)));
    }
    for h in 5..=10 {
        assert!(queue.in_flight_contains(block::Height(h)));
    }
    assert_eq!(queue.reserved_bytes(), 600);
    assert_eq!(queue.reserved_bytes(), queue.reserved_bytes_scanned());

    // A lower floor is a no-op (floor only advances).
    assert_eq!(queue.advance_floor(block::Height(2)), 0);
    assert_eq!(queue.reserved_bytes(), 600);
}

#[test]
fn watchdog_after_held_settle_releases_once() {
    let queue = work_queue_with(0, [needed(1, BlockSizeEstimate::Advertised(100))]);
    let mut budget = ByteBudget::new(1_000);
    let taken = queue.take_in_range(block::Height(1), block::Height(1), 1);
    assert_eq!(taken.len(), 1);
    assert!(budget.try_reserve(100));
    assert_eq!(queue.mark_reserved([block::Height(1)]), 100);

    let delta = queue
        .settle_active_reserved_height(block::Height(1), 80)
        .expect("active reserved height settles");
    assert_eq!(delta, -20);
    budget.release(20);
    assert_eq!(budget.reserved(), 80);

    let watchdog_released = queue.release_reserved_and_return_items([block::Height(1)]);
    budget.release(watchdog_released);
    assert_eq!(
        watchdog_released, 0,
        "watchdog must not release a held body owned by the sequencer handoff"
    );
    assert!(queue.in_flight_contains(block::Height(1)));
    assert_eq!(budget.reserved(), 80);

    budget.release(80);
    assert_eq!(queue.advance_floor(block::Height(1)), 0);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn late_delivery_after_watchdog_cancellation_does_not_resurrect_released_claim() {
    let queue = work_queue_with(0, [needed(1, BlockSizeEstimate::Advertised(100))]);
    let mut budget = ByteBudget::new(1_000);
    let taken = queue.take_in_range(block::Height(1), block::Height(1), 1);
    assert_eq!(taken.len(), 1);
    assert!(budget.try_reserve(100));
    assert_eq!(queue.mark_reserved([block::Height(1)]), 100);

    let watchdog_released = queue.release_reserved_and_return_items([block::Height(1)]);
    budget.release(watchdog_released);
    assert_eq!(budget.reserved(), 0);
    assert!(queue.pending_contains(block::Height(1)));

    assert_eq!(
        queue.settle_active_reserved_height(block::Height(1), 80),
        None,
        "a late body cannot settle a claim the watchdog already released"
    );
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn mark_held_direct_does_not_orphan_a_charge() {
    let queue = work_queue_with(0, [needed(1, BlockSizeEstimate::Advertised(100))]);
    let mut budget = ByteBudget::new(1_000);
    let taken = queue.take_in_range(block::Height(1), block::Height(1), 1);
    assert_eq!(taken.len(), 1);
    assert!(budget.try_reserve(100));
    assert_eq!(queue.mark_reserved([block::Height(1)]), 100);

    assert!(budget.try_reserve(80));
    let old_charge = queue.mark_held_direct(block::Height(1), 80);
    budget.release(old_charge);
    assert_eq!(old_charge, 100);
    assert_eq!(budget.reserved(), 80);

    let released = queue.release_heights([block::Height(1)]);
    budget.release(released);
    assert_eq!(released, 80);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn release_reserved_mixed_reserved_held_conserves_budget() {
    let queue = work_queue_with(
        0,
        [
            needed(1, BlockSizeEstimate::Advertised(100)),
            needed(2, BlockSizeEstimate::Advertised(100)),
        ],
    );
    let mut budget = ByteBudget::new(1_000);
    let taken = queue.take_in_range(block::Height(1), block::Height(2), 2);
    assert_eq!(taken.len(), 2);
    assert!(budget.try_reserve(200));
    assert_eq!(
        queue.mark_reserved([block::Height(1), block::Height(2)]),
        200
    );

    let delta = queue
        .settle_active_reserved_height(block::Height(1), 80)
        .expect("active height settles");
    assert_eq!(delta, -20);
    budget.release(20);
    assert_eq!(budget.reserved(), 180);

    let released = queue.advance_floor(block::Height(2));
    budget.release(released);
    assert_eq!(
        released, 100,
        "WorkQueue releases only the still-reserved height; held bytes are released by Sequencer"
    );
    assert_eq!(budget.reserved(), 80);

    budget.release(80);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn work_queue_take_does_not_clamp_high_to_floor() {
    // The download floor is NOT an upper bound on a take: a peer fetches as far
    // above the floor as its servable range allows.
    let queue = work_queue_with(
        0,
        (100..=104)
            .map(|height| needed(height, BlockSizeEstimate::Advertised(100)))
            .collect::<Vec<_>>(),
    );
    // Floor stays at 0; heights are far above it.
    let taken = queue.take_in_range(
        block::Height(100),
        block::Height(104),
        MAX_BS_BLOCKS_PER_REQUEST as usize,
    );
    assert_eq!(
        taken.iter().map(|(height, _)| height.0).collect::<Vec<_>>(),
        vec![100, 101, 102, 103, 104],
        "heights far above the floor are takable; the floor never clamps the take"
    );
}

#[test]
fn work_queue_preserves_work_item_estimate_through_take_and_return() {
    // The size estimate (feeds the SizeMismatch tolerance check) is preserved as
    // a height moves pending → in_flight → pending. Estimator overridden so the
    // clamp is wide enough to keep the hinted size.
    let queue = work_queue_with(0, [needed(10, BlockSizeEstimate::Advertised(12_345))]);
    let taken = queue.take_in_range(block::Height(10), block::Height(10), 1);
    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].1.estimated_bytes, 12_345);
    assert_eq!(taken[0].1.hash, block::Hash([10; 32]));

    queue.return_items([block::Height(10)]);
    let retaken = queue.take_in_range(block::Height(10), block::Height(10), 1);
    assert_eq!(
        retaken[0].1.estimated_bytes, 12_345,
        "return_items must restore the stored WorkItem unchanged"
    );
}

#[test]
fn work_queue_advance_floor_and_reset_above_gc_both_maps() {
    let queue = work_queue_with(
        0,
        (1..=6)
            .map(|height| needed(height, BlockSizeEstimate::Advertised(100)))
            .collect::<Vec<_>>(),
    );
    // h1,h2 in flight; h3..h6 pending.
    queue.take_in_range(block::Height(1), block::Height(2), 2);
    assert_eq!(queue.in_flight_len(), 2);
    assert_eq!(queue.pending_len(), 4);

    // advance_floor GCs <= floor from both maps (committed → never re-fetch).
    queue.advance_floor(block::Height(3));
    assert!(!queue.in_flight_contains(block::Height(1)));
    assert!(!queue.pending_contains(block::Height(3)));
    assert!(queue.pending_contains(block::Height(4)));

    // reset_above drops > floor from both maps (reset dropped their buffers).
    queue.take_in_range(block::Height(4), block::Height(4), 1); // h4 → in flight
    queue.reset_above(block::Height(4));
    assert!(queue.in_flight_contains(block::Height(4)));
    assert!(!queue.pending_contains(block::Height(5)));
    assert!(!queue.pending_contains(block::Height(6)));
    assert_eq!(queue.pending_len(), 0);
}

#[test]
fn work_queue_height_is_in_exactly_one_set() {
    // : a height is in exactly one of {below-floor (gone), pending, in_flight}.
    let queue = work_queue_with(0, [needed(10, BlockSizeEstimate::Advertised(100))]);
    let in_one_set = |height: block::Height| -> usize {
        usize::from(queue.pending_contains(height)) + usize::from(queue.in_flight_contains(height))
    };
    let h = block::Height(10);

    assert_eq!(in_one_set(h), 1, "pending only after extend");
    queue.take_in_range(h, h, 1);
    assert_eq!(in_one_set(h), 1, "in_flight only after take");
    queue.return_items([h]);
    assert_eq!(in_one_set(h), 1, "pending only after return");
    queue.take_in_range(h, h, 1);
    queue.advance_floor(h);
    assert_eq!(in_one_set(h), 0, "gone after the floor commits past it");
}

#[tokio::test]
async fn reactor_fill_loop_saturates_multiple_slots_in_one_pass() {
    let config = immediate_body_download_config();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // Peer serves heights 1..=4 and accepts four concurrent single-block requests.
    let (_peer_id, _inbound, mut outbound) = connect_peer_with_status_message(
        &service,
        &mut actions,
        41,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(4),
            tip_hash: block::Hash([4; 32]),
            max_blocks_per_response: 1,
            max_inflight_requests: 4,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        },
    )
    .await;

    tip_tx
        .send((block::Height(4), block::Hash([4; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: block::Hash([1; 32]),
                size: BlockSizeEstimate::Advertised(1_000),
            },
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: block::Hash([2; 32]),
                size: BlockSizeEstimate::Advertised(1_000),
            },
            BlockSyncBlockMeta {
                height: block::Height(3),
                hash: block::Hash([3; 32]),
                size: BlockSizeEstimate::Advertised(1_000),
            },
            BlockSyncBlockMeta {
                height: block::Height(4),
                hash: block::Hash([4; 32]),
                size: BlockSizeEstimate::Advertised(1_000),
            },
        ]))
        .await
        .expect("needed metadata queues");

    // The fill-loop opens all four slots from the single NeededBlocks event.
    // Pre-fill-loop scheduling issued only one GetBlocks per scheduling event,
    // so this would time out on the second request.
    // Reading this peer's own outbound proves all four requests targeted it.
    let mut heights = Vec::new();
    for _ in 0..4 {
        let (start_height, count) = wait_for_outbound_getblocks(&mut outbound).await;
        assert_eq!(count, 1);
        heights.push(start_height.0);
    }
    heights.sort_unstable();
    assert_eq!(heights, vec![1, 2, 3, 4]);

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_suppresses_duplicate_needed_block_query_until_response() {
    let config = immediate_body_download_config();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config,
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let best_hash = block::Hash([4; 32]);

    handle
        .send(BlockSyncEvent::HeaderTipChanged {
            height: block::Height(4),
            hash: best_hash,
        })
        .await
        .expect("header-tip event queues");
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(4)).await;

    handle
        .send(BlockSyncEvent::HeaderTipChanged {
            height: block::Height(4),
            hash: best_hash,
        })
        .await
        .expect("duplicate header-tip event queues");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), actions.recv())
            .await
            .is_err(),
        "same pending needed-block query should not be dispatched twice",
    );

    handle
        .send(BlockSyncEvent::NeededBlocks(Vec::new()))
        .await
        .expect("needed-block response queues");
    handle
        .send(BlockSyncEvent::HeaderTipChanged {
            height: block::Height(4),
            hash: best_hash,
        })
        .await
        .expect("header-tip event after response queues");
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(4)).await;

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_suppresses_needed_block_query_when_work_already_covers_tip() {
    let config = immediate_body_download_config();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config,
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let best_hash = block::Hash([4; 32]);

    handle
        .send(BlockSyncEvent::HeaderTipChanged {
            height: block::Height(4),
            hash: best_hash,
        })
        .await
        .expect("header-tip event queues");
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(4)).await;

    handle
        .send(BlockSyncEvent::NeededBlocks(
            (1..=4)
                .map(|height| BlockSyncBlockMeta {
                    height: block::Height(height),
                    hash: block::Hash([u8::try_from(height).expect("test height fits u8"); 32]),
                    size: BlockSizeEstimate::Advertised(1_000),
                })
                .collect(),
        ))
        .await
        .expect("needed-block response queues");
    handle
        .send(BlockSyncEvent::HeaderTipChanged {
            height: block::Height(4),
            hash: best_hash,
        })
        .await
        .expect("covered header-tip event queues");

    assert!(
        tokio::time::timeout(Duration::from_millis(100), actions.recv())
            .await
            .is_err(),
        "producer should not re-query when pending/in-flight work already covers the header tip",
    );

    reactor_task.abort();
}

/// Every connected peer that advertises an in-flight window must have that
/// window filled in one scheduling pass when there is enough needed work to go
/// around — not just the first peer.
///
/// This is the multi-peer complement to
/// `reactor_fill_loop_saturates_multiple_slots_in_one_pass`: with three peers
/// each advertising four concurrent single-block slots and twelve needed
/// heights, the fill loop must fan out four requests to *each* peer rather than
/// pouring all twelve into the lowest-id peer (or stopping after one peer's
/// window). A regression here is what makes download budget collect on a single
/// peer while the rest sit idle with free slots.
#[tokio::test]
async fn reactor_fill_loop_saturates_every_peer_window_not_just_one() {
    let config = immediate_body_download_config();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // Three peers, each willing to serve heights 1..=12 and accept four
    // concurrent single-block requests. The budget is ample, so the fill order
    // (rotated per pass) does not matter here: every peer's window is saturated
    // in the single pass regardless of which peer the rotation starts at.
    let mut peer_ids = Vec::new();
    // Keep every peer's stream handles alive for the whole test: dropping them
    // closes the channels and tears the peer down before it can serve.
    let mut peer_streams = Vec::new();
    for byte in [41u8, 42, 43] {
        let (peer_id, inbound, outbound) = connect_peer_with_status_message(
            &service,
            &mut actions,
            byte,
            BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(12),
                tip_hash: block::Hash([12; 32]),
                max_blocks_per_response: 1,
                max_inflight_requests: 4,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            },
        )
        .await;
        peer_ids.push(peer_id);
        peer_streams.push((inbound, outbound));
    }

    tip_tx
        .send((block::Height(12), block::Hash([12; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(
            (1..=12)
                .map(|height| BlockSyncBlockMeta {
                    height: block::Height(height),
                    hash: block::Hash([height as u8; 32]),
                    size: BlockSizeEstimate::Advertised(1_000),
                })
                .collect(),
        ))
        .await
        .expect("needed metadata queues");

    // Collect all twelve requests and tally them per peer. With fanout=1 each
    // height is assignable to exactly one peer, so a correct fill loop hands
    // four distinct heights to each of the three peers. Which peer receives which
    // height is nondeterministic (the routines race for shared work), so observe
    // every peer's real outbound and attribute each `GetBlocks` to the peer whose
    // stream it arrived on.
    let mut outbound_by_peer: Vec<(ZakuraPeerId, &mut FramedRecv)> = peer_ids
        .iter()
        .cloned()
        .zip(peer_streams.iter_mut().map(|(_, outbound)| outbound))
        .collect();
    let mut per_peer: HashMap<ZakuraPeerId, Vec<u32>> = HashMap::new();
    for _ in 0..12 {
        let (peer, start_height, count) = wait_for_getblocks_across(&mut outbound_by_peer).await;
        assert_eq!(count, 1);
        per_peer.entry(peer).or_default().push(start_height.0);
    }

    assert_eq!(
        per_peer.len(),
        3,
        "all three peers must receive requests, not just the first; got {per_peer:?}"
    );
    for peer_id in &peer_ids {
        let issued = per_peer
            .get(peer_id)
            .unwrap_or_else(|| panic!("peer {peer_id:?} received no requests: {per_peer:?}"));
        assert_eq!(
            issued.len(),
            4,
            "peer {peer_id:?} window of 4 was not saturated: {per_peer:?}"
        );
    }

    let mut all_heights: Vec<u32> = per_peer.values().flatten().copied().collect();
    all_heights.sort_unstable();
    assert_eq!(
        all_heights,
        (1..=12).collect::<Vec<_>>(),
        "every needed height must be requested exactly once across peers"
    );

    reactor_task.abort();
}

/// Under a budget that only covers one in-flight request at a time, issuance
/// must rotate across the status-ready peers rather than always pouring the
/// single budgeted request into the lowest-node-id peer.
///
/// Per-peer routines replace the central full-pass `fill_rotation_cursor`: per-peer routines
/// race for the shared work, with the per-peer byte cap as the fairness mechanism
/// for multi-height work. For a single contested height with fanout=1 there is no
/// per-round rotation guarantee (whichever routine's `take_in_range` wins the race
/// gets it). The invariant this still pins is that the contested height stays
/// **contestable** and is re-offered every time a peer fails it — no wedge, no
/// stall — which is the property that actually matters for liveness. (The
/// honest-peer-eventually-offered property is also covered by
/// `reactor_does_not_wedge_honest_peer_under_range_unavailable_spam`.)
#[tokio::test]
async fn reactor_budget_constrained_issuance_rotates_across_peers() {
    let config = immediate_body_download_config();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // Three peers, each able to serve height 1 with a single in-flight slot.
    // Distinct ascending id bytes give the old sorted order a fixed lowest peer
    // (0x41) that a regression would always pick first.
    let mut peer_inbounds = HashMap::new();
    // Keep every peer's outbound handle alive too (dropping it closes the channel
    // and tears the peer down before it can be offered work), paired with the peer
    // id so the observed `GetBlocks` can be attributed to the peer whose stream it
    // arrived on.
    let mut peer_outbounds = Vec::new();
    for byte in [0x41u8, 0x42, 0x43] {
        let (peer_id, inbound, outbound) = connect_peer_with_status_message(
            &service,
            &mut actions,
            byte,
            BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block::Hash([1; 32]),
                max_blocks_per_response: 1,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            },
        )
        .await;
        peer_inbounds.insert(peer_id.clone(), inbound);
        peer_outbounds.push((peer_id, outbound));
    }

    tip_tx
        .send((block::Height(1), block::Hash([1; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block::Hash([1; 32]),
            size: BlockSizeEstimate::Advertised(1_000),
        }]))
        .await
        .expect("needed metadata queues");

    // A single contested height with fanout=1 can be assigned to exactly one
    // peer at a time. Each pass offers it to the rotation-start peer; that peer
    // answers `RangeUnavailable`, which re-queues the range and runs another full
    // pass. The rotating cursor advances once per pass, so over several passes
    // the offered peer rotates instead of pinning to the lowest-id peer (which is
    // what the old static sorted order did).
    let range_unavailable = BlockSyncMessage::RangeUnavailable {
        start_height: block::Height(1),
        count: 1,
    }
    .encode_frame()
    .expect("RangeUnavailable frame encodes");

    // Each round the contested height must be re-offered to *some* connected peer
    // (no wedge), and that peer answers `RangeUnavailable` to return it to the
    // shared queue for the next round. Observe every peer's real outbound and learn
    // which peer the routine actually asked.
    let mut outbound_by_peer: Vec<(ZakuraPeerId, &mut FramedRecv)> = peer_outbounds
        .iter_mut()
        .map(|(peer_id, outbound)| (peer_id.clone(), outbound))
        .collect();
    for round in 0..6 {
        let (peer, start_height, count) = wait_for_getblocks_across(&mut outbound_by_peer).await;
        assert_eq!(start_height, block::Height(1));
        assert_eq!(count, 1, "fanout=1 yields single-block requests");
        let inbound = peer_inbounds.get(&peer).unwrap_or_else(|| {
            panic!("round {round}: served peer {peer:?} must be one of the connected peers")
        });
        inbound
            .send(range_unavailable.clone())
            .await
            .expect("RangeUnavailable frame queues");
    }

    reactor_task.abort();
}

/// One peer whose request times out enters local recovery and must not block the
/// other peers from being filled out of the same shared work.
///
/// This is the timeout-locality invariant: a slow peer's recovery is local to
/// that peer. The retry path re-queues the timed-out range to a *different*
/// servable peer, so the healthy peer keeps making progress while the slow peer
/// is in recovery rather than the whole download stalling behind one straggler.
#[tokio::test]
async fn reactor_timeout_recovery_is_local_and_healthy_peer_keeps_filling() {
    let mut config = immediate_body_download_config();
    config.fanout = 1;
    // A request timeout long enough that the opening pass fans both heights out
    // before anything expires, but short enough that the slow peer's unanswered
    // request still times out within the test window.
    config.request_timeout = Duration::from_millis(400);
    config.max_inflight_block_bytes = BS_PER_BLOCK_WORST_CASE_BYTES * 64;

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // Two peers, both able to serve heights 1..=2 with a single in-flight slot.
    // One will be left holding an unanswered request (the slow peer); the other
    // answers and must keep being filled from the same shared work — including
    // the slow peer's range once it times out and re-queues.
    let status = || BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(2),
        tip_hash: block::Hash([2; 32]),
        max_blocks_per_response: 1,
        max_inflight_requests: 1,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
    };
    let (peer_a, a_in, mut a_out) =
        connect_peer_with_status_message(&service, &mut actions, 0x41, status()).await;
    let (peer_b, b_in, mut b_out) =
        connect_peer_with_status_message(&service, &mut actions, 0x42, status()).await;
    let mut inbounds = HashMap::from([(peer_a.clone(), a_in), (peer_b.clone(), b_in)]);

    tip_tx
        .send((block::Height(2), block::Hash([2; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: block::Hash([1; 32]),
                size: BlockSizeEstimate::Advertised(1_000),
            },
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: block::Hash([2; 32]),
                size: BlockSizeEstimate::Advertised(1_000),
            },
        ]))
        .await
        .expect("needed metadata queues");

    // Observe both peers' real outbounds; which peer is offered which height is
    // nondeterministic, so attribute each `GetBlocks` to the peer whose stream it
    // arrived on.
    let mut outbound_by_peer: Vec<(ZakuraPeerId, &mut FramedRecv)> =
        vec![(peer_a.clone(), &mut a_out), (peer_b.clone(), &mut b_out)];

    // Opening pass: with fanout=1 each peer is offered one of the two heights.
    let first = wait_for_getblocks_across(&mut outbound_by_peer).await;
    let second = wait_for_getblocks_across(&mut outbound_by_peer).await;
    let mut offered: HashMap<ZakuraPeerId, block::Height> = HashMap::new();
    offered.insert(first.0.clone(), first.1);
    offered.insert(second.0.clone(), second.1);
    assert_eq!(
        offered.len(),
        2,
        "both peers must be offered a height in the opening pass: {offered:?}"
    );

    // Pick one peer to be the straggler (it never answers) and the other to be
    // healthy. The healthy peer answers `RangeUnavailable` for its own range so
    // it frees its slot without committing anything; the straggler's range then
    // times out and re-queues. Because timeout recovery is local to the
    // straggler, the healthy peer must keep being offered the re-queued shared
    // work rather than the whole download stalling behind the straggler.
    let healthy = peer_b.clone();
    let healthy_in = inbounds.remove(&healthy).expect("healthy peer inbound");

    let healthy_offers = tokio::time::timeout(Duration::from_secs(3), async {
        let mut count = 0usize;
        loop {
            // Whenever the healthy peer is offered a range, free its slot with a
            // `RangeUnavailable` so it can be offered the next range; the slow
            // peer never answers and stays in timeout recovery.
            let (peer, start_height, count_blocks) =
                wait_for_getblocks_across(&mut outbound_by_peer).await;
            if peer == healthy {
                count += 1;
                if count >= 2 {
                    break count;
                }
                healthy_in
                    .send(
                        BlockSyncMessage::RangeUnavailable {
                            start_height,
                            count: count_blocks,
                        }
                        .encode_frame()
                        .expect("RangeUnavailable frame encodes"),
                    )
                    .await
                    .expect("RangeUnavailable frame queues");
            }
        }
    })
    .await
    .expect(
        "the healthy peer must keep being offered shared work while the slow peer \
         is in timeout recovery; a stall here means a straggler wedged the pass",
    );
    assert!(
        healthy_offers >= 2,
        "the healthy peer was filled repeatedly despite the slow peer's timeout recovery"
    );

    reactor_task.abort();
}

#[tokio::test]
async fn block_liveness_disconnects_silent_peer_and_traces_reason() {
    let mut capture =
        TraceCapture::for_test("block_liveness_disconnects_silent_peer_and_traces_reason")
            .expect("trace capture initializes");
    let mut config = immediate_body_download_config();
    config.fanout = 1;
    config.request_timeout = Duration::from_millis(400);
    config.max_inflight_block_bytes = BS_PER_BLOCK_WORST_CASE_BYTES * 64;

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let mut startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    startup.trace = ZakuraTrace::new(capture.tracer(), "01");
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    let peer = peer(0x51);
    let (inbound_tx, inbound_rx) = framed_channel(16);
    let (outbound_tx, mut outbound_rx) = framed_channel(16);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    let connection_cancel = CancellationToken::new();
    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        connection_cancel.clone(),
    ));
    wait_for_outbound_status(&mut outbound_rx).await;

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block::Hash([1; 32]),
                max_blocks_per_response: 1,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(1), block::Hash([1; 32])))
        .expect("tip watch is live");
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(1)).await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block::Hash([1; 32]),
            size: BlockSizeEstimate::Advertised(1_000),
        }]))
        .await
        .expect("needed metadata queues");

    let (start_height, count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(start_height, block::Height(1));
    assert_eq!(count, 1);

    tokio::time::timeout(Duration::from_secs(3), connection_cancel.cancelled())
        .await
        .expect("silent active peer is disconnected by block-progress liveness");

    capture.flush().await;
    let reader = capture.reader().expect("trace rows load");
    reader.table("block_sync").assert_row(
        bs_trace::BLOCK_PEER_PROTOCOL_REJECT,
        &[
            (
                bs_trace::REASON,
                TraceValue::Str("block_sync_no_block_progress"),
            ),
            (bs_trace::OUTSTANDING, TraceValue::U64(1)),
        ],
    );

    reactor_task.abort();
}

// The old covered-prefix / assigned-key / queued-retry-ordering scheduler tests
// (`scheduler_partial_*`, `scheduler_drops_*`, `scheduler_splits_*`,
// `scheduler_retries_only_uncovered_suffix`, `scheduler_keeps_queued_*`,
// `scheduler_releases_budget_*`) are removed: the WorkQueue replaces the
// covered/assigned bookkeeping with `in_flight`, the byte budget moves to the
// reactor's `fill_peer`, and a `BTreeMap` keeps ascending order by construction.
// The remaining WorkQueue-owned behaviors (dedup, range eligibility, GC, the
// size-estimate clamp, and ordering diagnostics) are asserted here; commit-1 /
// commit-3 byte behavior is covered by the reactor integration tests.

#[test]
fn work_queue_estimate_clamps_hint_between_floor_and_max_block_bytes() {
    use super::work_queue::DEFAULT_BS_SIZE_FLOOR_BYTES;

    // Default estimator: Unknown -> worst case; tiny hints clamp up to the floor;
    // huge hints clamp down to MAX_BLOCK_BYTES; ordinary hints pass through.
    let queue = super::work_queue::WorkQueue::new(block::Height(0));
    queue.extend([
        needed(1, BlockSizeEstimate::Unknown),
        needed(2, BlockSizeEstimate::Advertised(1)), // below the floor
        needed(3, BlockSizeEstimate::Advertised(12_345)),
        needed(4, BlockSizeEstimate::Confirmed(u32::MAX)), // above MAX_BLOCK_BYTES
    ]);
    let item = |height| {
        queue
            .take_in_range(block::Height(height), block::Height(height), 1)
            .pop()
            .expect("height present")
            .1
            .estimated_bytes
    };
    assert_eq!(item(1), block::MAX_BLOCK_BYTES);
    assert_eq!(item(2), DEFAULT_BS_SIZE_FLOOR_BYTES);
    assert_eq!(item(3), 12_345);
    assert_eq!(item(4), block::MAX_BLOCK_BYTES);

    // The test estimator override changes the floor clamp.
    let tuned = super::work_queue::WorkQueue::new(block::Height(0));
    tuned.set_estimate_floor_for_tests(100);
    tuned.extend([
        needed(10, BlockSizeEstimate::Unknown),
        needed(11, BlockSizeEstimate::Advertised(50)), // below the tuned floor
    ]);
    assert_eq!(
        tuned
            .take_in_range(block::Height(10), block::Height(10), 1)
            .pop()
            .unwrap()
            .1
            .estimated_bytes,
        block::MAX_BLOCK_BYTES
    );
    assert_eq!(
        tuned
            .take_in_range(block::Height(11), block::Height(11), 1)
            .pop()
            .unwrap()
            .1
            .estimated_bytes,
        100
    );
}

#[test]
fn work_queue_diagnostics_report_runs_min_and_max() {
    // Two contiguous runs (10..=12 and 20..=21) plus an in-flight height feed the
    // BLOCK_SYNC_STATE trace remaps (queue_len -> runs, queue_blocks -> pending,
    // queue_min_start -> min_pending, covered_max_end -> max_in_flight).
    let queue = work_queue_with(
        0,
        [
            needed(10, BlockSizeEstimate::Advertised(100)),
            needed(11, BlockSizeEstimate::Advertised(100)),
            needed(12, BlockSizeEstimate::Advertised(100)),
            needed(20, BlockSizeEstimate::Advertised(100)),
            needed(21, BlockSizeEstimate::Advertised(100)),
        ],
    );
    assert_eq!(queue.pending_run_count(), 2);
    assert_eq!(queue.pending_len(), 5);
    assert_eq!(queue.min_pending(), Some(block::Height(10)));
    assert_eq!(queue.max_in_flight(), None);

    queue.take_in_range(block::Height(20), block::Height(21), 2);
    assert_eq!(queue.pending_run_count(), 1, "the 20..=21 run was taken");
    assert_eq!(queue.min_pending(), Some(block::Height(10)));
    assert_eq!(queue.max_in_flight(), Some(block::Height(21)));
    assert_eq!(
        queue.hash_for_height(block::Height(21)),
        Some(block::Hash([21; 32]))
    );
    assert_eq!(
        queue.hash_for_height(block::Height(12)),
        Some(block::Hash([12; 32]))
    );
    assert_eq!(queue.hash_for_height(block::Height(99)), None);
}

#[test]
fn work_queue_keeps_pending_ordered_by_height() {
    // A BTreeMap keeps the lowest needed height first regardless of extend order,
    // so a newly-needed lower height never sits behind later queued work.
    let queue = super::work_queue::WorkQueue::new(block::Height(0));
    queue.extend([needed(20, BlockSizeEstimate::Advertised(100))]);
    queue.extend([needed(10, BlockSizeEstimate::Advertised(100))]);
    assert_eq!(queue.min_pending(), Some(block::Height(10)));
    let taken = queue.take_in_range(block::Height(1), block::Height(30), 1);
    assert_eq!(
        taken[0].0,
        block::Height(10),
        "lower work must be taken before higher work"
    );
}

#[test]
fn reorder_drains_only_contiguous_prefix_without_releasing_budget() {
    let mut reorder = ReorderBuffer::new();
    let mut budget = ByteBudget::new(10_000);
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);

    // The reorder buffer no longer touches the budget on insert: a received body
    // already owns its (shrunk) reservation, so the caller reserves and the buffer
    // only takes ownership. Model that by reserving the actual bytes here.
    assert!(budget.try_reserve(300));
    assert_eq!(
        reorder.insert(block::Height(3), block.clone(), 300, peer(0)),
        ReorderInsertResult::Inserted
    );
    assert!(reorder.drain_contiguous_prefix(block::Height(0)).is_empty());
    assert_eq!(reorder.buffered_bytes(), 300);
    assert_eq!(budget.reserved(), 300);

    assert!(budget.try_reserve(100));
    assert_eq!(
        reorder.insert(block::Height(1), block.clone(), 100, peer(0)),
        ReorderInsertResult::Inserted
    );
    let released = reorder.drain_contiguous_prefix(block::Height(0));
    assert_eq!(
        released
            .iter()
            .map(|(height, _, bytes, _)| (*height, *bytes))
            .collect::<Vec<_>>(),
        vec![(block::Height(1), 100)]
    );
    // Draining the contiguous prefix hands bytes to the apply stage; it does not
    // release the budget, which the apply finish releases later.
    assert_eq!(reorder.buffered_bytes(), 300);
    assert_eq!(budget.reserved(), 400);
    budget.release(100);

    assert!(budget.try_reserve(200));
    assert_eq!(
        reorder.insert(block::Height(2), block.clone(), 200, peer(0)),
        ReorderInsertResult::Inserted
    );
    let released = reorder.drain_contiguous_prefix(block::Height(1));
    assert_eq!(
        released
            .iter()
            .map(|(height, _, bytes, _)| (*height, *bytes))
            .collect::<Vec<_>>(),
        vec![(block::Height(2), 200), (block::Height(3), 300)]
    );
    assert_eq!(budget.reserved(), 500);
    budget.release(500);

    assert!(budget.try_reserve(200));
    assert_eq!(
        reorder.insert(block::Height(2), block.clone(), 200, peer(0)),
        ReorderInsertResult::Inserted
    );
    assert!(budget.try_reserve(300));
    assert_eq!(
        reorder.insert(block::Height(3), block, 300, peer(0)),
        ReorderInsertResult::Inserted
    );
    // `drop_from`/`drop_through`/`clear` return the bytes their dropped bodies
    // held; the reactor releases that reservation (the reorder buffer, owned by
    // the `Sequencer`, never touches the budget directly).
    budget.release(reorder.drop_from(block::Height(3)));
    assert_eq!(reorder.buffered_bytes(), 200);
    assert_eq!(budget.reserved(), 200);
    budget.release(reorder.drop_through(block::Height(2)));
    assert_eq!(reorder.buffered_bytes(), 0);
    assert_eq!(budget.reserved(), 0);
    assert!(budget.try_reserve(300));
    assert_eq!(
        reorder.insert(
            block::Height(3),
            mainnet_block(&BLOCK_MAINNET_1_BYTES),
            300,
            peer(0)
        ),
        ReorderInsertResult::Inserted
    );
    budget.release(reorder.clear());
    assert_eq!(reorder.buffered_bytes(), 0);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn shed_top_for_floor_starvation_funds_lowest_pending_by_dropping_top() {
    let worst = BS_PER_BLOCK_WORST_CASE_BYTES;
    // Budget holds exactly two worst-case blocks, both consumed by buffered bodies
    // (heights 5 and 6). Height 1 — the commit-unblocking floor gap — is pending
    // and unfunded: this is the "download ran ahead, budget full, low height
    // needs (re-)requesting" shape that wedged sync.
    let mut budget = ByteBudget::new(2 * worst);
    let work = work_queue_with(
        0,
        [
            needed(1, BlockSizeEstimate::Unknown),
            needed(5, BlockSizeEstimate::Unknown),
            needed(6, BlockSizeEstimate::Unknown),
        ],
    );
    let mut seq = test_sequencer(0, 100);
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);

    // Heights 5 and 6 are taken (pending -> in_flight) and buffered, each holding a
    // worst-case block of budget; height 1 stays pending and unfunded.
    work.take_in_range(block::Height(5), block::Height(6), 2);
    for height in [5u32, 6] {
        assert!(budget.try_reserve(worst));
        seq.accept_body(
            block::Height(height),
            block::Hash([height as u8; 32]),
            block.clone(),
            worst,
            peer(0),
        );
    }
    assert_eq!(budget.available(), 0, "budget saturated by buffered bodies");
    assert!(work.pending_contains(block::Height(1)));

    // The floor-reservation rescue drops the top buffered body (6) — the
    // one furthest from the floor — releasing its budget and returning its height
    // to `pending` for later re-fetch, so the lower floor-gap request can now be
    // funded. Without this the budget stays full and height 1 can never be
    // requested (the wedge).
    let shed = super::sequencer_task::shed_top_for_floor_starvation(&mut budget, &work, &mut seq);
    assert!(shed, "the top buffered body is shed");
    assert!(
        budget.available() >= worst,
        "freed budget can now fund the floor-gap request"
    );
    assert!(
        !seq.reorder_contains(block::Height(6)),
        "the top body is evicted"
    );
    assert!(
        seq.reorder_contains(block::Height(5)),
        "the lower buffered body is kept"
    );
    assert!(
        work.pending_contains(block::Height(1)),
        "the floor gap is still pending and now fundable"
    );
    assert!(
        work.pending_contains(block::Height(6)),
        "the evicted height is returned to pending for re-fetch"
    );
}

#[test]
fn shed_top_for_floor_starvation_funds_outstanding_floor_by_dropping_top() {
    let worst = BS_PER_BLOCK_WORST_CASE_BYTES;
    let mut budget = ByteBudget::new(2 * worst);
    let work = work_queue_with(
        0,
        [
            needed(1, BlockSizeEstimate::Unknown),
            needed(5, BlockSizeEstimate::Unknown),
            needed(6, BlockSizeEstimate::Unknown),
        ],
    );
    let mut seq = test_sequencer(0, 100);
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);

    // Height 1 is outstanding on a slow peer, so it is not pending. The top
    // reorder bodies saturate the budget and must still be shed to make the
    // floor watchdog's re-request fundable.
    work.take_in_range(block::Height(1), block::Height(1), 1);
    for height in [5u32, 6] {
        work.take_in_range(block::Height(height), block::Height(height), 1);
        assert!(budget.try_reserve(worst));
        seq.accept_body(
            block::Height(height),
            block::Hash([height as u8; 32]),
            block.clone(),
            worst,
            peer(0),
        );
    }
    assert_eq!(budget.available(), 0, "budget saturated by buffered bodies");
    assert!(
        !work.pending_contains(block::Height(1)),
        "the floor gap is outstanding, not pending"
    );

    let shed = super::sequencer_task::shed_top_for_floor_starvation(&mut budget, &work, &mut seq);
    assert!(
        shed,
        "the top buffered body is shed even while the floor gap is outstanding"
    );
    assert!(
        budget.available() >= worst,
        "freed budget can now fund the watchdog floor re-request"
    );
    assert!(
        !seq.reorder_contains(block::Height(6)),
        "the top body is evicted"
    );
    assert!(
        seq.reorder_contains(block::Height(5)),
        "the lower buffered body is kept"
    );
    assert!(
        work.pending_contains(block::Height(6)),
        "the evicted height is returned to pending for re-fetch"
    );
}

#[test]
fn shed_top_until_available_self_funds_floor_reservation() {
    let worst = BS_PER_BLOCK_WORST_CASE_BYTES;
    let mut budget = ByteBudget::new(3 * worst);
    let work = work_queue_with(
        0,
        [
            needed(1, BlockSizeEstimate::Unknown),
            needed(8, BlockSizeEstimate::Unknown),
            needed(9, BlockSizeEstimate::Unknown),
            needed(10, BlockSizeEstimate::Unknown),
        ],
    );
    let mut seq = test_sequencer(0, 100);
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);

    // The floor has already been taken for a request whose reservation lost a
    // budget race. The speculative bodies saturate the budget, so the floor
    // reservation path must pop enough high-tail bodies and retry.
    work.take_in_range(block::Height(1), block::Height(1), 1);
    for height in [8u32, 9, 10] {
        work.take_in_range(block::Height(height), block::Height(height), 1);
        assert!(budget.try_reserve(worst));
        seq.accept_body(
            block::Height(height),
            block::Hash([height as u8; 32]),
            block.clone(),
            worst,
            peer(0),
        );
    }
    assert_eq!(budget.available(), 0);

    let shed =
        super::sequencer_task::shed_top_until_available(&mut budget, &work, &mut seq, 2 * worst);
    assert!(shed, "the high tail is popped to fund the floor request");
    assert!(
        budget.available() >= 2 * worst,
        "the pop frees the requested floor bytes"
    );
    assert!(
        !seq.reorder_contains(block::Height(10)) && !seq.reorder_contains(block::Height(9)),
        "the furthest buffered bodies are evicted first"
    );
    assert!(
        seq.reorder_contains(block::Height(8)),
        "nearer buffered body is kept once the request is fundable"
    );
}

// ---- Sequencer commit pipeline ----

fn test_sequencer(verified_tip: u32, submitted_apply_limit: usize) -> Sequencer {
    Sequencer::new(block::Height(verified_tip), submitted_apply_limit)
}

#[test]
fn sequencer_accept_body_buffers_then_reports_duplicate() {
    let mut seq = test_sequencer(0, 4);
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let hash = block.hash();
    // First arrival above the floor buffers the body and reports its covered
    // height; the reorder buffer takes ownership of the reservation.
    assert_eq!(
        seq.accept_body(block::Height(1), hash, block.clone(), 100, peer(0)),
        AcceptOutcome::Buffered {
            covered: block::Height(1)
        }
    );
    assert!(seq.reorder_contains(block::Height(1)));
    // A second arrival of the same buffered height is redundant; its bytes are
    // handed back for the reactor to release.
    assert_eq!(
        seq.accept_body(block::Height(1), hash, block, 100, peer(0)),
        AcceptOutcome::Redundant { release_bytes: 100 }
    );
}

#[test]
fn sequencer_retains_raw_bytes_for_non_contiguous_backlog() {
    let mut seq = test_sequencer(0, 4);
    let blocks = mainnet_blocks_1_to_3();
    let block1 = blocks[0].clone();
    let block2 = blocks[1].clone();
    let distinguishable_decoded_block2 = forked_block(&block2, 99);

    assert_ne!(distinguishable_decoded_block2.hash(), block2.hash());
    assert_eq!(
        distinguishable_decoded_block2.coinbase_height(),
        block2.coinbase_height()
    );

    let block2_body = BufferedBlockBody::from_decoded_block(
        distinguishable_decoded_block2.clone(),
        Some(raw_block_payload(&block2)),
    );

    assert_eq!(
        seq.accept_buffered_body(block::Height(2), block2.hash(), block2_body, 200, peer(0)),
        AcceptOutcome::Buffered {
            covered: block::Height(2)
        }
    );
    assert!(seq.drain_ready_into_applying().is_empty());
    assert!(seq.reorder_contains(block::Height(2)));

    assert_eq!(
        seq.accept_body(block::Height(1), block1.hash(), block1, 100, peer(0)),
        AcceptOutcome::Buffered {
            covered: block::Height(1)
        }
    );
    assert_eq!(
        seq.drain_ready_into_applying(),
        vec![block::Height(1), block::Height(2)]
    );
    assert_eq!(seq.applying_hash(block::Height(2)), Some(block2.hash()));
    assert_ne!(
        seq.applying_hash(block::Height(2)),
        Some(distinguishable_decoded_block2.hash())
    );
}

#[test]
fn sequencer_applying_counters_match_scan_across_transitions() {
    // Assert the O(1) applying counters (buffered bytes, submitted count/bytes)
    // never drift from a full scan across insert / submit / unsubmit / remove /
    // commit-release / reset.
    let mut seq = test_sequencer(0, 8);
    let blocks = mainnet_blocks_1_to_3();
    let check = |seq: &Sequencer, label: &str| {
        assert_eq!(
            seq.applying_buffered_bytes(),
            seq.applying_buffered_bytes_scanned(),
            "applying_buffered_bytes drifted after {label}"
        );
        assert_eq!(
            seq.submitted_applying_count(),
            seq.submitted_applying_count_scanned(),
            "submitted_applying_count drifted after {label}"
        );
        assert_eq!(
            seq.submitted_applying_bytes(),
            seq.submitted_applying_bytes_scanned(),
            "submitted_applying_bytes drifted after {label}"
        );
        assert_eq!(
            seq.unsubmitted_applying_count(),
            seq.applying_len() - seq.submitted_applying_count(),
            "unsubmitted derivation wrong after {label}"
        );
    };

    // Buffer heights 1..=3 with distinct sizes and drain them into `applying`.
    for (i, block) in blocks.iter().enumerate() {
        let height = (i + 1) as u32;
        seq.accept_body(
            block::Height(height),
            block.hash(),
            block.clone(),
            100 * u64::from(height),
            peer(0),
        );
    }
    assert_eq!(
        seq.drain_ready_into_applying(),
        vec![block::Height(1), block::Height(2), block::Height(3)]
    );
    assert_eq!(seq.applying_buffered_bytes(), 600);
    assert_eq!(seq.submitted_applying_count(), 0);
    check(&seq, "drain");

    // Submit heights 1 and 2.
    let item1 = seq
        .prepare_submit(block::Height(1))
        .expect("height 1 applying");
    let _ = seq
        .prepare_submit(block::Height(2))
        .expect("height 2 applying");
    assert_eq!(seq.submitted_applying_count(), 2);
    assert_eq!(seq.submitted_applying_bytes(), 300);
    assert_eq!(seq.unsubmitted_applying_count(), 1);
    check(&seq, "submit 1,2");

    // Roll back the submit for height 1.
    seq.unsubmit(block::Height(1), item1.token);
    assert_eq!(seq.submitted_applying_count(), 1);
    assert_eq!(seq.submitted_applying_bytes(), 200);
    check(&seq, "unsubmit 1");

    // Remove the still-submitted height 2 directly.
    seq.remove_applying(block::Height(2));
    assert_eq!(seq.submitted_applying_count(), 0);
    assert_eq!(seq.submitted_applying_bytes(), 0);
    assert_eq!(seq.applying_buffered_bytes(), 400);
    check(&seq, "remove submitted 2");

    // Commit through height 1: releases the applied body (height 1) from `applying`.
    seq.advance_verified_tip(block::Height(1), true);
    assert_eq!(seq.applying_buffered_bytes(), 300);
    check(&seq, "advance_verified_tip");

    // Reset drops all applying state and zeroes the counters.
    seq.reset_to(block::Height(0), false);
    assert_eq!(seq.applying_buffered_bytes(), 0);
    assert_eq!(seq.submitted_applying_count(), 0);
    assert_eq!(seq.submitted_applying_bytes(), 0);
    check(&seq, "reset");
}

#[test]
fn sequencer_accept_body_rejects_at_or_below_floor() {
    let mut seq = test_sequencer(5, 4);
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    assert_eq!(
        seq.accept_body(block::Height(5), block.hash(), block.clone(), 100, peer(0)),
        AcceptOutcome::Redundant { release_bytes: 100 }
    );
    assert!(!seq.reorder_contains(block::Height(5)));
}

#[test]
fn sequencer_drains_contiguous_prefix_into_applying_and_advances_floor() {
    let mut seq = test_sequencer(0, 8);
    let blocks = mainnet_blocks_1_to_3();
    // Buffer heights 1 and 3, leaving a gap at 2.
    seq.accept_body(
        block::Height(1),
        blocks[0].hash(),
        blocks[0].clone(),
        100,
        peer(0),
    );
    seq.accept_body(
        block::Height(3),
        blocks[2].hash(),
        blocks[2].clone(),
        300,
        peer(0),
    );
    // Only the contiguous prefix above the floor (height 1) drains.
    assert_eq!(seq.drain_ready_into_applying(), vec![block::Height(1)]);
    assert_eq!(seq.floor(), block::Height(1));
    assert!(seq.applying_contains(block::Height(1)));
    assert_eq!(seq.applying_len(), 1);
    // Filling the gap lets 2 and 3 drain together and advances the floor to 3.
    seq.accept_body(
        block::Height(2),
        blocks[1].hash(),
        blocks[1].clone(),
        200,
        peer(0),
    );
    assert_eq!(
        seq.drain_ready_into_applying(),
        vec![block::Height(2), block::Height(3)]
    );
    assert_eq!(seq.floor(), block::Height(3));
    assert_eq!(seq.reorder_len(), 0);
}

#[test]
fn sequencer_submits_within_window_and_rolls_back_on_unsubmit() {
    let mut seq = test_sequencer(0, 2);
    let blocks = mainnet_blocks_1_to_3();
    for (index, block) in blocks.iter().enumerate() {
        let height = block::Height(index as u32 + 1);
        seq.accept_body(height, block.hash(), block.clone(), 100, peer(0));
    }
    assert_eq!(seq.drain_ready_into_applying().len(), 3);
    // The submission window of 2 caps the eligible heights.
    assert_eq!(
        seq.submittable_heights(),
        vec![block::Height(1), block::Height(2)]
    );
    let item1 = seq.prepare_submit(block::Height(1)).expect("applying at 1");
    let item2 = seq.prepare_submit(block::Height(2)).expect("applying at 2");
    assert_eq!((item1.token, item2.token), (1, 2));
    assert_eq!(seq.submitted_applying_count(), 2);
    // The window is now full, so nothing else is submittable.
    assert!(seq.submittable_heights().is_empty());
    // Rolling back a failed dispatch frees the slot and re-offers the height.
    seq.unsubmit(block::Height(2), item2.token);
    assert_eq!(seq.submitted_applying_count(), 1);
    assert_eq!(seq.submittable_heights(), vec![block::Height(2)]);
}

#[test]
fn sequencer_records_and_decrements_submitted_applies() {
    let mut seq = test_sequencer(0, 4);
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let hash = block.hash();
    assert!(!seq.has_submitted_apply(block::Height(1), hash));
    seq.record_submitted_apply(block::Height(1), hash);
    assert!(seq.has_submitted_apply(block::Height(1), hash));
    assert!(seq.submitted_contains(block::Height(1)));
    seq.decrement_submitted_apply(block::Height(1), hash);
    assert!(!seq.has_submitted_apply(block::Height(1), hash));
    assert!(!seq.submitted_contains(block::Height(1)));
}

#[test]
fn sequencer_release_applied_through_clears_submitted_applies() {
    let mut seq = test_sequencer(0, 1);
    let blocks = mainnet_blocks_1_to_3();
    for (index, block) in blocks.iter().enumerate() {
        let height = block::Height(index as u32 + 1);
        seq.accept_body(height, block.hash(), block.clone(), 100, peer(0));
    }
    seq.drain_ready_into_applying();

    let item = seq
        .prepare_submit(block::Height(1))
        .expect("height 1 is applying");
    seq.record_submitted_apply(item.height, item.hash);
    assert!(seq.submitted_contains(block::Height(1)));
    assert!(
        seq.submittable_heights().is_empty(),
        "submitted-apply window is full"
    );

    assert_eq!(seq.release_applied_through(block::Height(1)), 100);
    assert!(!seq.submitted_contains(block::Height(1)));
    assert_eq!(seq.submittable_heights(), vec![block::Height(2)]);
}

#[test]
fn sequencer_advance_verified_tip_releases_bytes_and_reports_change() {
    let mut seq = test_sequencer(0, 8);
    let blocks = mainnet_blocks_1_to_3();
    seq.accept_body(
        block::Height(1),
        blocks[0].hash(),
        blocks[0].clone(),
        100,
        peer(0),
    );
    seq.accept_body(
        block::Height(2),
        blocks[1].hash(),
        blocks[1].clone(),
        200,
        peer(0),
    );
    // Advancing the verified tip drops buffered bodies at or below it and reports
    // their bytes for the reactor to release.
    let advance = seq.advance_verified_tip(block::Height(2), false);
    assert!(advance.changed);
    assert_eq!(advance.release_bytes, 300);
    assert_eq!(seq.verified_tip(), block::Height(2));
    assert!(seq.floor() >= block::Height(2));
    // A no-op advance to the same tip frees nothing and reports unchanged.
    let advance = seq.advance_verified_tip(block::Height(2), false);
    assert!(!advance.changed);
    assert_eq!(advance.release_bytes, 0);
}

#[test]
fn sequencer_reset_clears_buffers_and_pins_floor_and_tip() {
    let mut seq = test_sequencer(0, 8);
    let blocks = mainnet_blocks_1_to_3();
    seq.accept_body(
        block::Height(1),
        blocks[0].hash(),
        blocks[0].clone(),
        100,
        peer(0),
    );
    seq.drain_ready_into_applying();
    seq.accept_body(
        block::Height(2),
        blocks[1].hash(),
        blocks[1].clone(),
        200,
        peer(0),
    );
    // Reset drops everything (one applying@100 + one reorder@200) and pins the
    // floor/tip to the reset target.
    let released = seq.reset_to(block::Height(0), false);
    assert_eq!(released, 300);
    assert_eq!(seq.floor(), block::Height(0));
    assert_eq!(seq.verified_tip(), block::Height(0));
    assert_eq!(seq.applying_len(), 0);
    assert_eq!(seq.reorder_len(), 0);
}

#[test]
fn sequencer_reject_drops_successors_and_rolls_floor_back() {
    let mut seq = test_sequencer(0, 8);
    let blocks = mainnet_blocks_1_to_3();
    for (index, block) in blocks.iter().enumerate() {
        let height = block::Height(index as u32 + 1);
        seq.accept_body(height, block.hash(), block.clone(), 100, peer(0));
    }
    seq.drain_ready_into_applying();
    assert_eq!(seq.floor(), block::Height(3));
    // A reject at height 2 drops applying >= 2 (200 bytes) and rolls the floor
    // back below 2, never below the verified tip.
    let released = seq.release_applying_blocks_from(block::Height(2));
    assert_eq!(released, 200);
    assert!(seq.applying_contains(block::Height(1)));
    assert!(!seq.applying_contains(block::Height(2)));
    seq.reset_floor_below(block::Height(2));
    assert_eq!(seq.floor(), block::Height(1));
    // The committed prefix at or below height 1 is releasable.
    assert_eq!(seq.release_applied_through(block::Height(1)), 100);
    assert_eq!(seq.applying_len(), 0);
}

#[test]
fn sequencer_release_applying_blocks_from_keeps_submitted_counters_consistent() {
    // `release_applying_blocks_from` removes each height through `remove_applying`,
    // which must decrement the O(1) submitted counters for any *submitted* body it
    // drops. Existing reject coverage only releases unsubmitted bodies, so this
    // exercises the submitted-counter branch of that path.
    let mut seq = test_sequencer(0, 8);
    let blocks = mainnet_blocks_1_to_3();
    for (index, block) in blocks.iter().enumerate() {
        let height = block::Height(index as u32 + 1);
        seq.accept_body(
            height,
            block.hash(),
            block.clone(),
            100 * (index as u64 + 1),
            peer(0),
        );
    }
    seq.drain_ready_into_applying();
    // Submit heights 2 and 3 so the released prefix (>= 2) is all submitted work.
    let _ = seq
        .prepare_submit(block::Height(2))
        .expect("height 2 applying");
    let _ = seq
        .prepare_submit(block::Height(3))
        .expect("height 3 applying");
    assert_eq!(seq.submitted_applying_count(), 2);
    assert_eq!(seq.submitted_applying_bytes(), 200 + 300);

    let released = seq.release_applying_blocks_from(block::Height(2));
    assert_eq!(released, 500);
    assert_eq!(seq.applying_len(), 1);
    // Only the unsubmitted height 1 (100 bytes) survives; the submitted counters
    // shed exactly the released bodies' contribution.
    assert_eq!(seq.applying_buffered_bytes(), 100);
    assert_eq!(seq.submitted_applying_count(), 0);
    assert_eq!(seq.submitted_applying_bytes(), 0);
    // Every maintained counter still agrees with a full scan.
    assert_eq!(
        seq.applying_buffered_bytes(),
        seq.applying_buffered_bytes_scanned()
    );
    assert_eq!(
        seq.submitted_applying_count(),
        seq.submitted_applying_count_scanned()
    );
    assert_eq!(
        seq.submitted_applying_bytes(),
        seq.submitted_applying_bytes_scanned()
    );
}

#[test]
fn sequencer_unsubmit_ignores_stale_or_mismatched_token() {
    // `unsubmit` only rolls back (and decrements the submitted counters) when the
    // token still matches the live submission, so a stale rollback cannot clobber a
    // newer one or double-decrement the O(1) counters.
    let mut seq = test_sequencer(0, 4);
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    seq.accept_body(block::Height(1), block.hash(), block.clone(), 100, peer(0));
    seq.drain_ready_into_applying();
    let item = seq
        .prepare_submit(block::Height(1))
        .expect("height 1 applying");
    assert_eq!(seq.submitted_applying_count(), 1);
    assert_eq!(seq.submitted_applying_bytes(), 100);

    // A rollback carrying a non-matching token is ignored: the submission and the
    // counters are untouched.
    seq.unsubmit(block::Height(1), item.token + 1);
    assert_eq!(seq.submitted_applying_count(), 1);
    assert_eq!(seq.submitted_applying_bytes(), 100);
    assert_eq!(
        seq.submitted_applying_count(),
        seq.submitted_applying_count_scanned()
    );
    assert_eq!(
        seq.submitted_applying_bytes(),
        seq.submitted_applying_bytes_scanned()
    );

    // The matching rollback frees the slot exactly once.
    seq.unsubmit(block::Height(1), item.token);
    assert_eq!(seq.submitted_applying_count(), 0);
    assert_eq!(seq.submitted_applying_bytes(), 0);

    // Replaying the now-stale token must not decrement a second time.
    seq.unsubmit(block::Height(1), item.token);
    assert_eq!(seq.submitted_applying_count(), 0);
    assert_eq!(seq.submitted_applying_bytes(), 0);
    assert_eq!(
        seq.submitted_applying_count(),
        seq.submitted_applying_count_scanned()
    );
    assert_eq!(
        seq.submitted_applying_bytes(),
        seq.submitted_applying_bytes_scanned()
    );
}

#[test]
fn sequencer_reorder_max_height_reports_highest_buffered() {
    let mut seq = test_sequencer(0, 8);
    let blocks = mainnet_blocks_1_to_3();
    // Empty reorder buffer has no top.
    assert_eq!(seq.reorder_max_height(), None);
    // Buffer heights 1 and 3, leaving a gap at 2; the top is the highest buffered.
    seq.accept_body(
        block::Height(1),
        blocks[0].hash(),
        blocks[0].clone(),
        100,
        peer(0),
    );
    seq.accept_body(
        block::Height(3),
        blocks[2].hash(),
        blocks[2].clone(),
        300,
        peer(0),
    );
    assert_eq!(seq.reorder_max_height(), Some(block::Height(3)));
    // Draining the contiguous prefix removes height 1 but leaves the top (3).
    seq.drain_ready_into_applying();
    assert_eq!(seq.reorder_max_height(), Some(block::Height(3)));
    // Filling the gap drains 2 and 3 out, emptying the reorder buffer.
    seq.accept_body(
        block::Height(2),
        blocks[1].hash(),
        blocks[1].clone(),
        200,
        peer(0),
    );
    seq.drain_ready_into_applying();
    assert_eq!(seq.reorder_max_height(), None);
}

#[test]
fn sequencer_keeps_whole_body_for_contiguous_height() {
    // The retain-for-backlog trim only applies to *non-contiguous* bodies. A body
    // arriving at the next contiguous height above the floor is kept whole, so its
    // decoded block — not a re-decode of the raw payload — drains into `applying`.
    // This is the mirror of `sequencer_retains_raw_bytes_for_non_contiguous_backlog`.
    let mut seq = test_sequencer(0, 4);
    let block1 = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    // A decoded block distinguishable from its own raw payload lets us observe which
    // half of `DecodedWithRawFramePayload` survived acceptance.
    let distinguishable_decoded_block1 = forked_block(&block1, 99);
    assert_ne!(distinguishable_decoded_block1.hash(), block1.hash());

    let body = BufferedBlockBody::from_decoded_block(
        distinguishable_decoded_block1.clone(),
        Some(raw_block_payload(&block1)),
    );
    // Height 1 is the next contiguous height above the floor (0).
    assert_eq!(
        seq.accept_buffered_body(block::Height(1), block1.hash(), body, 100, peer(0)),
        AcceptOutcome::Buffered {
            covered: block::Height(1)
        }
    );
    assert_eq!(seq.drain_ready_into_applying(), vec![block::Height(1)]);
    // The kept decoded block drained in, not a re-decode of the raw payload.
    assert_eq!(
        seq.applying_hash(block::Height(1)),
        Some(distinguishable_decoded_block1.hash())
    );
}

#[test]
fn reorder_fuzzes_arrival_order_as_parent_first() {
    let orders = [
        [1, 2, 3, 4],
        [4, 3, 2, 1],
        [2, 4, 1, 3],
        [3, 1, 4, 2],
        [2, 1, 4, 3],
    ];

    for order in orders {
        let mut reorder = ReorderBuffer::new();
        let mut budget = ByteBudget::new(10_000);
        let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
        let mut tip = block::Height(0);
        let mut released_all = Vec::new();

        for height in order {
            assert!(budget.try_reserve(100));
            assert_eq!(
                reorder.insert(block::Height(height), block.clone(), 100, peer(0)),
                ReorderInsertResult::Inserted
            );
            for (released, _, bytes, _) in reorder.drain_contiguous_prefix(tip) {
                assert_eq!(released, block::Height(tip.0 + 1));
                tip = released;
                released_all.push(released);
                budget.release(bytes);
            }
        }

        assert_eq!(
            released_all,
            vec![
                block::Height(1),
                block::Height(2),
                block::Height(3),
                block::Height(4)
            ]
        );
        assert_eq!(budget.reserved(), 0);
    }
}

/// Build an outstanding three-block range whose worst-case reservation is already
/// held against `budget`, mirroring what the scheduler does at send time.
/// Per-height size-estimate reservation used by the budget-accounting tests.
const THREE_BLOCK_ESTIMATE: u64 = 1_000;

fn outstanding_three_block_range(budget: &mut ByteBudget) -> OutstandingBlockRange {
    let request = BlockRangeRequest {
        start_height: block::Height(1),
        count: 3,
        anchor_hash: block::Hash([1; 32]),
        // Size-estimate reservation: each block reserves its size hint, so the
        // request reserves the sum of the per-height estimates below.
        estimated_bytes: THREE_BLOCK_ESTIMATE * 3,
        expected_blocks: vec![
            ExpectedBlock {
                height: block::Height(1),
                hash: block::Hash([1; 32]),
                estimated_bytes: 1_000,
            },
            ExpectedBlock {
                height: block::Height(2),
                hash: block::Hash([2; 32]),
                estimated_bytes: 1_000,
            },
            ExpectedBlock {
                height: block::Height(3),
                hash: block::Hash([3; 32]),
                estimated_bytes: 1_000,
            },
        ],
    };
    assert!(budget.try_reserve(request.estimated_bytes));
    let now = Instant::now();
    OutstandingBlockRange {
        request,
        queued_at: now,
        deadline: now,
        delivery_snapshot: test_delivery_snapshot(now),
        delivered_bytes: 0,
        received: ReceivedBlockTracker::default(),
    }
}

#[test]
fn outstanding_range_accumulates_delivered_bytes_for_bbr_sample() {
    let mut budget = ByteBudget::new(THREE_BLOCK_ESTIMATE * 3);
    let mut outstanding = outstanding_three_block_range(&mut budget);

    for (height, bytes) in [
        (block::Height(1), 700),
        (block::Height(2), 800),
        (block::Height(3), 900),
    ] {
        outstanding.record_body_bytes(bytes);
        outstanding.mark_received(height);
    }

    assert!(outstanding.is_complete());
    assert_eq!(outstanding.delivered_bytes, 700 + 800 + 900);
}

#[test]
fn block_budget_ledger_settles_and_releases_current_charge() {
    let mut under = BlockBudgetLedger::reserved(1_000);
    assert_eq!(under.current_charge(), 1_000);
    assert_eq!(under.settle(700), -300);
    assert_eq!(under.current_charge(), 700);
    assert_eq!(under.release(), 700);
    assert_eq!(under.release(), 0);

    let mut equal = BlockBudgetLedger::reserved(1_000);
    assert_eq!(equal.settle(1_000), 0);
    assert_eq!(equal.release(), 1_000);

    let mut over = BlockBudgetLedger::reserved(1_000);
    assert_eq!(over.settle(1_300), 300);
    assert_eq!(over.current_charge(), 1_300);
    assert_eq!(over.release(), 1_300);

    let mut released = BlockBudgetLedger::Released;
    assert_eq!(released.settle(900), 0);
    assert_eq!(released.current_charge(), 0);
    assert_eq!(released.release(), 0);
}

#[test]
fn budget_audit_catches_injected_drift() {
    let mut budget = ByteBudget::new(1_000);
    assert!(budget.try_reserve(400));
    assert!(budget.audit(400, "test matching audit"));
    assert!(
        !budget.audit(300, "test injected drift"),
        "audit reports mismatched derived accounting"
    );
    budget.release(400);
    assert!(budget.audit(0, "test released audit"));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(256))]

    #[test]
    fn admission_decision_respects_lookahead_bounds(
        reorder_bytes in 0u64..2_000,
        applying_bytes in 0u64..2_000,
        input_bytes in 0u64..2_000,
        reserved_bytes in 0u64..2_000,
        reorder_blocks in 0u64..20,
        applying_blocks in 0u64..20,
        reserved_blocks in 0u64..20,
    ) {
        let config = ZakuraBlockSyncConfig {
            max_inflight_block_bytes: 64_000_000,
            max_reorder_lookahead_bytes: 1_000,
            max_reorder_lookahead_blocks: 10,
            ..ZakuraBlockSyncConfig::default()
        };
        let snapshot = super::admission::AdmissionSnapshot {
            download_floor: block::Height(10),
            reorder_buffered_bytes: reorder_bytes,
            reorder_buffered_blocks: reorder_blocks,
            applying_buffered_bytes: applying_bytes,
            applying_buffered_blocks: applying_blocks,
            sequencer_input_queued_bytes: input_bytes,
            reserved_above_floor_bytes: reserved_bytes,
            reserved_above_floor_blocks: reserved_blocks,
            budget_available: 64_000_000,
        };
        let held_bytes = reorder_bytes
            .saturating_add(applying_bytes)
            .saturating_add(input_bytes)
            .saturating_add(reserved_bytes);
        let held_blocks = reorder_blocks
            .saturating_add(applying_blocks)
            .saturating_add(reserved_blocks);
        let above = super::admission::admission_decision(
            &config,
            snapshot,
            block::Height(12),
            1_000,
        );
        if held_bytes >= config.effective_max_reorder_lookahead_bytes()
            || held_blocks >= u64::from(config.max_reorder_lookahead_blocks)
        {
            prop_assert_eq!(above, None);
        } else {
            prop_assert!(above.is_some());
        }

        let floor = super::admission::admission_decision(
            &config,
            snapshot,
            block::Height(11),
            1_000,
        );
        prop_assert_eq!(
            floor.expect("floor remains admitted while budget is available").priority,
            super::admission::RequestPriority::Floor
        );
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(256))]

    #[test]
    fn block_budget_ledger_mirrors_byte_budget(
        estimate in 1u64..1_000_000,
        actual in 1u64..1_000_000,
        release_before_receipt in proptest::bool::ANY,
    ) {
        let mut ledger = BlockBudgetLedger::reserved(estimate);
        let mut budget = ByteBudget::new(u64::MAX);
        prop_assert!(budget.try_reserve(estimate));

        if release_before_receipt {
            let released = ledger.release();
            budget.release(released);
            prop_assert_eq!(budget.reserved(), 0);
            let delta = ledger.settle(actual);
            prop_assert_eq!(delta, 0);
            prop_assert_eq!(budget.reserved(), 0);
            prop_assert_eq!(ledger.current_charge(), 0);
            let released = ledger.release();
            budget.release(released);
            prop_assert_eq!(budget.reserved(), 0);
        } else {
            let delta = ledger.settle(actual);
            if delta >= 0 {
                budget.charge(u64::try_from(delta).expect("positive test delta fits in u64"));
            } else {
                budget.release(u64::try_from(-delta).expect("negative test delta fits in u64"));
            }
            prop_assert_eq!(ledger.current_charge(), actual);
            prop_assert_eq!(budget.reserved(), actual);

            let released = ledger.release();
            budget.release(released);
            prop_assert_eq!(budget.reserved(), 0);
            prop_assert_eq!(ledger.current_charge(), 0);
        }
    }
}

/// The global reservation settles to the current held body bytes across the
/// download -> buffer -> apply -> commit path, and releases exactly once across
/// timeout/duplicate/short-response paths.
#[test]
fn budget_reservation_never_exceeds_max_and_only_shrinks_per_block() {
    let estimate = THREE_BLOCK_ESTIMATE;
    let max = estimate * 3;

    // Happy path: download -> shrink-on-receipt -> buffer -> apply -> commit.
    {
        let mut budget = ByteBudget::new(max);
        let mut reorder = ReorderBuffer::new();
        let mut outstanding = outstanding_three_block_range(&mut budget);
        assert_eq!(budget.reserved(), max);
        assert!(budget.reserved() <= budget.max_bytes_for_test());

        let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
        // Receive each height: release `estimate - actual`, keep `actual` reserved,
        // and hand `actual` to the reorder buffer without re-reserving.
        let actuals = [700u64, 800, 900]; // each < its estimate, varies per block
        for (index, height) in [block::Height(1), block::Height(2), block::Height(3)]
            .into_iter()
            .enumerate()
        {
            let before = budget.reserved();
            let actual = actuals[index];
            budget.settle(estimate, actual);
            outstanding.mark_received(height);
            assert_eq!(
                reorder.insert(height, block.clone(), actual, peer(0)),
                ReorderInsertResult::Inserted
            );
            // Per-block reservation only shrank (estimate -> actual), never grew.
            assert!(budget.reserved() <= before);
            assert!(budget.reserved() <= budget.max_bytes_for_test());
        }
        assert!(outstanding.is_complete());
        assert_eq!(outstanding.reserved_bytes(), 0);
        assert_eq!(budget.reserved(), 700 + 800 + 900);

        // Commit: draining to apply carries the actual bytes; the apply finish
        // releases them.
        let mut floor = block::Height(0);
        let mut applied_bytes = 0;
        for (_height, _block, bytes, _peer) in reorder.drain_contiguous_prefix(floor) {
            applied_bytes += bytes;
            floor = block::Height(floor.0 + 1);
        }
        assert_eq!(applied_bytes, 700 + 800 + 900);
        budget.release(applied_bytes);
        assert_eq!(budget.reserved(), 0);
    }

    // Timeout / short-response path: heights that never buffer release exactly
    // their size-estimate share, with no leak and no double-release.
    {
        let mut budget = ByteBudget::new(max);
        let mut outstanding = outstanding_three_block_range(&mut budget);
        assert_eq!(budget.reserved(), estimate * 3);
        // A short response delivers only height 1; release its estimate's slack.
        let actual = 700u64;
        budget.settle(estimate, actual);
        outstanding.mark_received(block::Height(1));
        // The remaining two unreceived heights still reserve their estimate each.
        assert_eq!(outstanding.reserved_bytes(), estimate * 2);
        assert!(budget.reserved() <= budget.max_bytes_for_test());
        // On timeout the outstanding range releases its still-reserved estimate.
        budget.release(outstanding.reserved_bytes());
        // Plus the actual bytes held for the one received-but-not-buffered height.
        budget.release(actual);
        assert_eq!(budget.reserved(), 0);
    }

    // Duplicate path: a duplicate body reserves nothing extra and releases its
    // actual bytes, so the buffer's reservation is unchanged.
    {
        let mut budget = ByteBudget::new(max);
        let mut reorder = ReorderBuffer::new();
        let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
        assert!(budget.try_reserve(1_000));
        assert_eq!(
            reorder.insert(block::Height(1), block.clone(), 1_000, peer(0)),
            ReorderInsertResult::Inserted
        );
        // A second body for the same height is a duplicate; reserve-then-release
        // leaves the reservation exactly where it was.
        assert!(budget.try_reserve(1_000));
        assert_eq!(
            reorder.insert(block::Height(1), block, 1_000, peer(0)),
            ReorderInsertResult::Duplicate
        );
        budget.release(1_000);
        assert_eq!(budget.reserved(), 1_000);
        assert!(budget.reserved() <= budget.max_bytes_for_test());
    }
}

/// A request range at the maximum advertised block count (128) fills the
/// `ReceivedBlockTracker`'s `u128` bitset exactly (offsets `0..=127`): every height —
/// including the top-bit height 128 — must be markable, complete, and fully released,
/// so no height silently falls off the end of the bitset. Guards the boundary that the
/// `MAX_BS_BLOCKS_PER_REQUEST <= u128::BITS` const assertion in `state.rs` protects.
#[test]
fn received_tracker_handles_a_full_range_at_the_bitset_boundary() {
    let count = MAX_BS_BLOCKS_PER_REQUEST;
    assert_eq!(count, u128::BITS, "the cap is sized to the bitset width");
    // Heights `1..=128`, offsets `0..=127`; the helper keeps heights within `u8`.
    let mut outstanding = window_request_range(1, count);
    assert_eq!(outstanding.reserved_bytes(), u64::from(count));

    for height in 1..=count {
        assert!(
            !outstanding.has_received(block::Height(height)),
            "height {height} should start unreceived",
        );
        outstanding.mark_received(block::Height(height));
        assert!(
            outstanding.has_received(block::Height(height)),
            "height {height} (offset {}) must be markable — the bitset must cover the \
             whole range",
            height - 1,
        );
    }

    assert!(
        outstanding.is_complete(),
        "a fully-received {count}-block range must report complete",
    );
    assert_eq!(
        outstanding.reserved_bytes(),
        0,
        "every height received ⇒ no reserved bytes remain (no offset fell off the bitset)",
    );
}

/// A body whose actual serialized size exceeds its advertised size hint is still
/// accepted and buffered, and the byte budget charges the overshoot so it cannot
/// issue more work while under-counting held bodies.
#[test]
fn underestimated_body_is_buffered_and_charges_budget_delta() {
    let hint = 1_000u64;
    // Budget holds several hint-sized shares: enough to reserve this body's hint.
    let mut budget = ByteBudget::new(hint * 4);
    let mut reorder = ReorderBuffer::new();

    let request = BlockRangeRequest {
        start_height: block::Height(1),
        count: 1,
        anchor_hash: block::Hash([1; 32]),
        // Size-estimate reservation: the per-height hint, not worst case.
        estimated_bytes: hint,
        expected_blocks: vec![ExpectedBlock {
            height: block::Height(1),
            hash: block::Hash([1; 32]),
            estimated_bytes: hint,
        }],
    };
    assert!(budget.try_reserve(request.estimated_bytes));
    let now = Instant::now();
    let mut outstanding = OutstandingBlockRange {
        request,
        queued_at: now,
        deadline: now,
        delivery_snapshot: test_delivery_snapshot(now),
        delivered_bytes: 0,
        received: ReceivedBlockTracker::default(),
    };
    assert_eq!(budget.reserved(), hint);

    // The body's actual serialized size is far larger than the hint (but still
    // <= MAX_BLOCK_BYTES).
    let actual = hint * 50;
    assert!(actual < BS_PER_BLOCK_WORST_CASE_BYTES);
    assert!(actual > hint);

    // Receipt: settle toward the actual size. Because actual exceeds the reserved
    // hint, the budget charges the delta even though the body is already admitted.
    budget.settle(hint, actual);
    outstanding.mark_received(block::Height(1));
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    assert_eq!(
        reorder.insert(block::Height(1), block, actual, peer(0)),
        ReorderInsertResult::Inserted,
        "an underestimated body must still buffer; a received body is never dropped"
    );
    assert_eq!(reorder.buffered_bytes(), actual);
    assert_eq!(budget.reserved(), actual);
    assert_eq!(budget.available(), 0);
    assert!(
        !budget.try_reserve(hint),
        "charging the underestimated delta closes the budget gate"
    );
    budget.release(reorder.drop_through(block::Height(1)));
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn block_sync_stream_declares_kind_capability_version_and_frame_cap() {
    let stream = block_sync_streams()
        .first()
        .copied()
        .expect("block sync declares one stream");

    assert_eq!(stream.kind, ZAKURA_STREAM_BLOCK_SYNC);
    assert_eq!(stream.version, ZAKURA_BLOCK_SYNC_STREAM_VERSION);
    assert_eq!(stream.capability, ZAKURA_CAP_BLOCK_SYNC);
    assert_eq!(stream.mode, StreamMode::Ordered);
    assert_eq!(stream.frame_cap, MAX_BS_FRAME_BYTES);
}

#[tokio::test]
async fn service_registry_routes_block_sync_by_exact_capability_and_version() {
    let service = Arc::new(BlockSyncService::new(ZakuraBlockSyncConfig::default()));
    let registry =
        ServiceRegistry::new(vec![service]).expect("block-sync service declares unique kind");
    let peer = peer(1);

    assert_eq!(
        registry.capability_for_stream(ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_BLOCK_SYNC_STREAM_VERSION),
        Some(ZAKURA_CAP_BLOCK_SYNC)
    );
    assert!(registry
        .capability_for_stream(
            ZAKURA_STREAM_BLOCK_SYNC,
            ZAKURA_BLOCK_SYNC_STREAM_VERSION + 1
        )
        .is_none());
    assert_eq!(
        registry
            .ordered_streams_for_negotiated(ZAKURA_CAP_BLOCK_SYNC)
            .iter()
            .map(|stream| stream.kind)
            .collect::<Vec<_>>(),
        vec![ZAKURA_STREAM_BLOCK_SYNC]
    );
    assert!(registry.ordered_streams_for_negotiated(0).is_empty());
    assert!(registry.wants_ordered_stream(
        ZAKURA_STREAM_BLOCK_SYNC,
        ZAKURA_CAP_BLOCK_SYNC,
        &peer,
        ServicePeerDirection::Inbound,
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn inert_reactor_parks_after_header_tip_watch_closes() {
    let _service = BlockSyncService::new(ZakuraBlockSyncConfig::default());

    let elapsed = tokio::time::timeout(Duration::from_secs(1), future::pending::<()>()).await;

    assert!(
        elapsed.is_err(),
        "paused-time timeout only elapses if the inert reactor has no always-ready branch"
    );
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "state-backed block sync must have exactly one frontier source")]
fn state_backed_reactor_panics_with_two_frontier_sources() {
    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let (_frontier_tx, frontier_rx) =
        watch::channel(test_frontier_update(0, 0, 0, FrontierChange::Snapshot));
    let mut startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        ZakuraBlockSyncConfig::default(),
    );
    startup.frontier_updates = Some(frontier_rx);

    let (_handle, _actions, _task) = spawn_block_sync_reactor(startup);
}

#[tokio::test]
async fn add_peer_emits_events_and_round_trips_status_over_framed_path() {
    let (service, mut events) = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
    let peer = peer(2);
    let cancel_token = CancellationToken::new();
    let (inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        streams,
        cancel_token,
    ));

    let session = match next_event(&mut events).await {
        BlockSyncEvent::PeerConnected(session) => session,
        event => panic!("expected PeerConnected, got {event:?}"),
    };
    assert_eq!(session.peer_id(), &peer);
    assert_eq!(service.peer_count(), 1);
    let _outbound_rx = outbound_rx;

    // The inbound data flow is inverted: with no reactor wiring (`new_for_test`),
    // `add_peer` drains inbound frames rather than emitting a `WireMessage` event
    // (the production inbound path is the per-peer pipe-routine, exercised by the
    // reactor tests with real wiring). The frame still queues onto the framed
    // stream; this asserts the framed inbound path is live and consumed.
    inbound_tx
        .send(
            BlockSyncMessage::Status(status())
                .encode_frame()
                .expect("status frame encodes"),
        )
        .await
        .expect("inbound status queues");

    service.remove_peer(&peer);
    assert_eq!(service.peer_count(), 0);
    assert!(session.cancel_token().is_cancelled());
}

#[tokio::test]
async fn stale_block_sync_teardown_keeps_replacement_session() {
    let (service, mut events) = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
    let peer = peer(92);

    let (old_inbound_tx, old_inbound_rx) = framed_channel(4);
    let (old_outbound_tx, _old_outbound_rx) = framed_channel(4);
    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (old_inbound_rx, old_outbound_tx))]),
        CancellationToken::new(),
    ));
    assert!(matches!(
        next_event(&mut events).await,
        BlockSyncEvent::PeerConnected(session) if session.peer_id() == &peer
    ));

    let (new_inbound_tx, new_inbound_rx) = framed_channel(4);
    let (new_outbound_tx, _new_outbound_rx) = framed_channel(4);
    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (new_inbound_rx, new_outbound_tx))]),
        CancellationToken::new(),
    ));
    assert!(matches!(
        next_event(&mut events).await,
        BlockSyncEvent::PeerConnected(session) if session.peer_id() == &peer
    ));
    assert_eq!(service.peer_count(), 1);

    drop(old_inbound_tx);
    tokio::time::sleep(Duration::from_millis(50)).await;

    if let Ok(Some(BlockSyncEvent::PeerDisconnected(disconnected))) =
        tokio::time::timeout(Duration::from_millis(50), events.recv()).await
    {
        panic!("stale teardown disconnected replacement session for {disconnected:?}");
    }
    // The replacement session remains installed (the stale teardown did not
    // disconnect it): `peer_count` stays 1 and the live replacement record was
    // never removed by the old session's teardown. (The previous check that routed a
    // `send_action(SendMessage)` through the record only exercised the removed
    // test-only source-pump scaffolding.)
    assert_eq!(service.peer_count(), 1);

    drop(new_inbound_tx);
}

#[tokio::test]
async fn lifecycle_events_bypass_full_bounded_wire_queue() {
    let mut config = ZakuraBlockSyncConfig::default();
    config.peer_limits.inbound_queue_depth = 1;
    let (events, _event_rx) = mpsc::channel(config.peer_limits.inbound_queue_depth);
    // Fill the bounded wire-event queue (per-peer routines deleted `WireMessage`; any event that
    // rides the bounded `events` channel proves lifecycle bypass — use a header-tip
    // change).
    events
        .try_send(BlockSyncEvent::HeaderTipChanged {
            height: block::Height(1),
            hash: block::Hash([1; 32]),
        })
        .expect("test fills bounded wire queue");
    let (lifecycle, mut lifecycle_rx) = mpsc::unbounded_channel();
    let (_peers_tx, peers) = watch::channel(ServicePeerSnapshot::new(0, 0, config.peer_limits));
    let (_status_tx, status) = watch::channel(config.initial_status());
    let (_candidates_tx, candidates) = watch::channel(ZakuraBlockSyncCandidateState::default());
    let handle = BlockSyncHandle {
        events,
        lifecycle,
        peers,
        status,
        candidates,
        // No reactor wiring: `add_peer` drains inbound (no routine), and this test
        // only checks the lifecycle-bypass plumbing.
        routine_wiring: None,
    };
    let service = BlockSyncService::new_with_handle_for_test(config, handle);
    let peer = peer(91);
    let (inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, _outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    let _inbound_tx = inbound_tx;

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), lifecycle_rx.recv())
            .await
            .expect("lifecycle event arrives")
            .expect("lifecycle channel stays open"),
        BlockSyncEvent::PeerConnected(session) if session.peer_id() == &peer
    ));

    service.remove_peer(&peer);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), lifecycle_rx.recv())
            .await
            .expect("lifecycle event arrives")
            .expect("lifecycle channel stays open"),
        BlockSyncEvent::PeerDisconnected(disconnected) if disconnected == peer
    ));
}

#[tokio::test]
async fn add_peer_decode_failure_reports_malformed_and_cancels_connection() {
    // inverted inbound flow: the per-peer pipe-routine decodes inbound frames in its own
    // task. A malformed frame is `MalformedMessage` misbehavior AND a fatal protocol
    // reject for the whole connection (the routine returns `Err(SinkReject::protocol)`,
    // which `handle_pipe_exit` turns into a connection cancel). With real reactor
    // wiring the routine runs; we observe the `Misbehavior(MalformedMessage)` action
    // and the connection-cancel.
    let config = ZakuraBlockSyncConfig::default();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, _reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    let peer = peer(3);
    let (inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, _outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    let connection_cancel = CancellationToken::new();

    service.add_peer(Peer::new(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        streams,
        connection_cancel.clone(),
    ));

    inbound_tx
        .send(Frame {
            message_type: u16::from(MSG_BS_STATUS),
            flags: 0,
            payload: Vec::new(),
        })
        .await
        .expect("malformed inbound frame queues");

    // The routine reports `MalformedMessage` (skip the connect Status mirror).
    let reported = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::Misbehavior { peer: got, reason } => break (got, reason),
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before malformed-message report: {action:?}"),
        }
    };
    assert_eq!(
        reported,
        (peer.clone(), BlockSyncMisbehavior::MalformedMessage)
    );

    // The malformed frame is a fatal protocol reject for the whole connection.
    tokio::time::timeout(Duration::from_secs(1), connection_cancel.cancelled())
        .await
        .expect("malformed frame cancels the connection");
}

#[tokio::test]
async fn registry_add_peer_requires_negotiated_block_sync_capability() {
    let (service, mut events) = BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
    let registry = ServiceRegistry::new(vec![Arc::new(service)])
        .expect("block-sync service declares unique kind");
    let peer = peer(4);
    let (inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, _outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    registry.add_peer(Peer::new(peer, None, 0, streams, CancellationToken::new()));
    drop(inbound_tx);

    assert!(
        tokio::time::timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "without cap 1<<3 the registry must not deliver kind-6 streams"
    );
}

#[tokio::test]
async fn wants_peer_rejects_when_configured_slot_cap_is_reached() {
    let config = ZakuraBlockSyncConfig {
        peer_limits: ServicePeerLimits {
            max_inbound_peers: 0,
            max_outbound_peers: 2,
            ..ServicePeerLimits::default()
        },
        ..ZakuraBlockSyncConfig::default()
    };
    let (service, mut events) = BlockSyncService::new_for_test(config);
    let inbound_peer = peer(5);

    assert!(!service.wants_peer(
        &inbound_peer,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Inbound
    ));
    assert!(service.wants_peer(
        &inbound_peer,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound
    ));

    let mut inbound_senders = Vec::new();
    for byte in 6..=7 {
        let peer_id = peer(byte);
        let (inbound_tx, inbound_rx) = framed_channel(4);
        let (outbound_tx, _outbound_rx) = framed_channel(4);
        let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

        service.add_peer(Peer::new_with_direction(
            peer_id.clone(),
            None,
            ZAKURA_CAP_BLOCK_SYNC,
            ServicePeerDirection::Outbound,
            streams,
            CancellationToken::new(),
        ));

        assert!(matches!(
            next_event(&mut events).await,
            BlockSyncEvent::PeerConnected(session) if session.peer_id() == &peer_id
        ));
        inbound_senders.push(inbound_tx);
    }

    assert_eq!(service.peer_count(), 2);
    assert!(!service.wants_peer(
        &peer(8),
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound
    ));

    let (_inbound_tx, inbound_rx) = framed_channel(4);
    let (outbound_tx, _outbound_rx) = framed_channel(4);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer(8),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    assert_eq!(service.peer_count(), 2);
}

#[tokio::test]
async fn reactor_drives_tip_to_getblocks_to_submit_over_framed_path() {
    let config = immediate_body_download_config();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer = peer(40);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block::Hash([1; 32]),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let block_hash = block.hash();
    let block_size = u32::try_from(
        block
            .zcash_serialize_to_vec()
            .expect("block serializes")
            .len(),
    )
    .expect("test block size fits u32");
    tip_tx
        .send((block::Height(1), block_hash))
        .expect("tip watch is live");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip,
                best_header_tip,
            } => {
                assert_eq!(verified_block_tip, block::Height(0));
                // The startup query carries best_header_tip 0; wait for the
                // tip-1 query (there is no near-tip pause to suppress either).
                if best_header_tip == block::Height(1) {
                    break;
                }
            }
            action => panic!("unexpected action before query: {action:?}"),
        }
    }

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block_hash,
            size: BlockSizeEstimate::Advertised(block_size),
        }]))
        .await
        .expect("needed metadata queues");

    // Reading this peer's own real outbound proves the GetBlocks targeted it.
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .expect("outbound frame arrives")
            .expect("outbound channel is live");
        if let BlockSyncMessage::GetBlocks {
            start_height,
            count,
        } = BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
        {
            assert_eq!(start_height, block::Height(1));
            assert_eq!(count, 1);
            break;
        }
    }

    inbound_tx
        .send(
            BlockSyncMessage::Block(block.clone())
                .encode_frame()
                .expect("block frame encodes"),
        )
        .await
        .expect("block frame queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock {
                block: submitted, ..
            } => {
                assert_eq!(submitted.hash(), block_hash);
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before submit: {action:?}"),
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_keeps_submitted_body_budget_until_apply_finishes() {
    let blocks = mainnet_blocks_1_to_3();
    let block1_size = block_size(&blocks[0]);
    let mut config = immediate_body_download_config();
    // One block's size hint of budget: size-based reservation now means one
    // advertised body fills the budget, throttling to one in-flight request.
    config.max_inflight_block_bytes = u64::from(block1_size);

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(41);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(2),
                tip_hash: blocks[1].hash(),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(2), blocks[1].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: blocks[0].hash(),
                size: BlockSizeEstimate::Advertised(block1_size),
            },
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: blocks[1].hash(),
                size: BlockSizeEstimate::Advertised(block1_size),
            },
        ]))
        .await
        .expect("needed metadata queues");

    let (start_height, count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(start_height, block::Height(1));
    assert_eq!(count, 1);

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");

    let submit_token = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => {
                assert_eq!(block.hash(), blocks[0].hash());
                break token;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before submit: {action:?}"),
        }
    };
    assert_eq!(handle.local_status().servable_high, block::Height(0));

    // The submitted-but-unapplied block must keep the body budget full, so no new
    // GetBlocks may issue. a routine that consumed work pings the reactor to
    // re-query, so a budget-orthogonal `QueryNeededBlocks` may appear in this
    // window (the producer self-gates; it is idempotent and downloads nothing).
    // Tolerate only that; any GetBlocks/SubmitBlock here would be the regression.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    loop {
        match tokio::time::timeout_at(deadline, actions.recv()).await {
            Err(_) => break,
            Ok(Some(BlockSyncAction::QueryNeededBlocks { .. })) => {}
            Ok(other) => panic!(
                "submitted-but-not-applied block should keep body budget full; got {other:?}"
            ),
        }
    }

    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token: submit_token,
            height: block::Height(1),
            hash: blocks[0].hash(),
            result: BlockApplyResult::Committed,
            local_frontier: Some(BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(1),
                verified_block_hash: blocks[0].hash(),
            }),
        })
        .await
        .expect("apply-finished event queues");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if handle.local_status().servable_high == block::Height(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("apply completion frontier advances advertised status");

    let (start_height, count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(start_height, block::Height(2));
    assert_eq!(count, 1);

    reactor_task.abort();
}

/// Pins the Sequencer task producer-filter substitution `height > request_floor &&
/// !work.in_flight_contains(height)`. A height that has been received and is held
/// in the commit pipeline (buffered / applying / submitted) was taken into the
/// WorkQueue's `in_flight` at issuance and stays there until it commits, so a
/// later `NeededBlocks` snapshot that still lists it must NOT cause the reactor to
/// re-issue a `GetBlocks` for it. This is the `in_flight ⟺ held` invariant the
/// reactor relies on now that it can no longer read the Sequencer's
/// reorder/applying/submitted membership directly.
#[tokio::test]
async fn reactor_does_not_requeue_held_height_reported_still_needed() {
    let blocks = mainnet_blocks_1_to_3();
    let block1_size = block_size(&blocks[0]);
    let block2_size = block_size(&blocks[1]);
    let mut config = immediate_body_download_config();
    config.max_inflight_block_bytes = BS_PER_BLOCK_WORST_CASE_BYTES * 4;

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(73);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    // Peer can serve heights 1..=2; the body for height 1 has a missing parent
    // (height 1 needs height 0's tip), so once received it is held in the commit
    // pipeline (reorder/applying) rather than immediately committing.
    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(2),
                tip_hash: blocks[1].hash(),
                max_blocks_per_response: 1,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(2), blocks[1].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: blocks[0].hash(),
                size: BlockSizeEstimate::Advertised(block1_size),
            },
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: blocks[1].hash(),
                size: BlockSizeEstimate::Advertised(block2_size),
            },
        ]))
        .await
        .expect("needed metadata queues");

    // The peer's count cap is 1, so it requests height 1 first.
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 1)
    );

    // Deliver height 1's body. It is taken from `pending` into `in_flight`, then
    // received and held (buffered then drained to applying, then submitted). It
    // never returns to `pending` while held.
    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("body frame queues");
    // Wait for the held body to be submitted, confirming it is now in the commit
    // pipeline (and still claimed in `in_flight`).
    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => {
                assert_eq!(block.hash(), blocks[0].hash());
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before submit: {action:?}"),
        }
    }

    // State re-reports both heights as still needed (its snapshot has no
    // visibility into our in-memory commit pipeline). The producer must NOT
    // re-issue a GetBlocks for the held height 1; only the genuinely-missing
    // height 2 may be requested.
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: blocks[0].hash(),
                size: BlockSizeEstimate::Advertised(block1_size),
            },
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: blocks[1].hash(),
                size: BlockSizeEstimate::Advertised(block2_size),
            },
        ]))
        .await
        .expect("re-reported needed metadata queues");

    // A re-request of the held height would land on this peer's own real
    // outbound, so watch the wire for any GetBlocks of height 1.
    let saw_requeue_of_held_height = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            match outbound_rx.recv().await {
                Some(frame) => {
                    if let BlockSyncMessage::GetBlocks {
                        start_height: block::Height(1),
                        ..
                    } = BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
                    {
                        return true;
                    }
                }
                None => return false,
            }
        }
    })
    .await;
    assert!(
        saw_requeue_of_held_height.is_err(),
        "a held (in_flight) height reported still needed must not be re-requested",
    );

    reactor_task.abort();
}

/// A real body whose serialized size dwarfs its advertised size hint is still
/// accepted, buffered, and submitted. Worst case (not the hint) is reserved at
/// send time, so shrink-on-receipt always has room and never drops the body.
///
/// A/B: under the old release-then-reserve scheme the request reserved only the
/// hint-sized estimate, so a body larger than the hint re-reserved more than was
/// released and, against a tight budget, hit `BudgetFull` and dropped a valid
/// body. With worst-case reservation this path is unreachable.
#[tokio::test]
async fn reactor_buffers_body_larger_than_its_size_hint() {
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    // Budget holds exactly one worst-case share, so a hint-sized re-reservation
    // would have left no headroom for an underestimated body.
    config.max_inflight_block_bytes = BS_PER_BLOCK_WORST_CASE_BYTES;

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        41,
        block::Height(1),
        blocks[0].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    tip_tx
        .send((block::Height(1), blocks[0].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    // Advertise a 1-byte size hint, far below the real body size, with a
    // tolerance that admits the deviation without misbehavior scoring.
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: blocks[0].hash(),
            size: BlockSizeEstimate::Advertised(1),
        }]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 1)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("body queues");

    let submitted = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match next_action(&mut actions).await {
                BlockSyncAction::SubmitBlock { block, .. } => break block.hash(),
                // The preserved size-deviation check reports the hint mismatch but
                // must not drop the body.
                BlockSyncAction::Misbehavior {
                    reason: BlockSyncMisbehavior::SizeMismatch,
                    ..
                }
                | BlockSyncAction::QueryNeededBlocks { .. } => {}
                action => panic!("unexpected action before submit: {action:?}"),
            }
        }
    })
    .await
    .expect("underestimated body must still be buffered and submitted, not dropped");
    assert_eq!(submitted, blocks[0].hash());

    reactor_task.abort();
}

/// A stalled commit must not pace downloads. The refill low-water mark counts
/// only the download pipeline (`queued` + `outstanding`), never the commit
/// pipeline (`reorder` + `applying`), so a block stuck in `applying` (submitted
/// but never apply-finished) does not stop the reactor from re-querying and
/// downloading higher heights — downloads run ahead of commit, bounded only by
/// the in-flight byte budget.
///
/// A/B: before the decoupling, block 1 held in `applying` satisfies the
/// low-water mark (sized to one block here), so the reactor never issues the
/// second `QueryNeededBlocks` and this test times out waiting for it.
#[tokio::test]
async fn reactor_downloads_run_ahead_of_stalled_commit() {
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    // Low-water mark = 1 status peer * advertised inflight (1) * advertised
    // blocks-per-response (1) = 1 block, so a single block held in the commit
    // pipeline is enough to (before the fix) satisfy it. The default 4 GiB byte
    // budget stays far from binding for these tiny blocks.
    config.max_inflight_requests = 1;
    config.max_blocks_per_response = 1;

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status_message(
        &service,
        &mut actions,
        41,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(3),
            tip_hash: blocks[2].hash(),
            max_blocks_per_response: 1,
            max_inflight_requests: 4,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        },
    )
    .await;

    // A header tip above the verified tip opens the download window.
    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(3)).await;

    // Supply only height 1, download it, and submit it — then leave it stuck in
    // `applying` by never sending BlockApplyFinished (a stalled commit).
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[0])]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 1)
    );
    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");
    let _submit_token = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => {
                assert_eq!(block.hash(), blocks[0].hash());
                break token;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before submit: {action:?}"),
        }
    };

    // The commit is now stalled: block 1 sits in `applying` (awaiting an
    // apply-finished that never comes), holding its byte reservation. The
    // download floor advanced to 1 on submit, but `queued` and `outstanding`
    // are both empty. A header-tip bump must still make the reactor re-query
    // and download height 2 — the download pipeline is empty even though the
    // commit pipeline is not.
    tip_tx
        .send((block::Height(4), block::Hash([4; 32])))
        .expect("tip watch is live");
    wait_for_query_needed_blocks(&mut actions, block::Height(1), block::Height(4)).await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[1])]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 1),
        "the reactor must download height 2 ahead of the stalled commit of height 1",
    );

    reactor_task.abort();
}

fn add_outbound_block_sync_peer(
    service: &BlockSyncService,
    byte: u8,
    held: &mut Vec<(FramedSend, FramedRecv)>,
) -> ZakuraPeerId {
    let peer_id = peer(byte);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    held.push((inbound_tx, outbound_rx));
    peer_id
}

/// A connection-symmetry collision resolves by the loser re-registering an
/// already-present peer (adopting the winner's incoming stream). That
/// replacement must succeed even when the per-direction cap is full — the peer
/// is already counted — while a genuinely new peer at the cap is still rejected.
#[tokio::test]
async fn block_sync_add_peer_replaces_same_peer_even_at_full_cap() {
    let mut config = immediate_body_download_config();
    config.peer_limits.max_outbound_peers = 1;
    config.peer_limits.max_inbound_peers = 0;
    let (service, mut events) = BlockSyncService::new_for_test(config);

    // Keep every stream handle alive so the per-peer pipes are not torn down.
    let mut held = Vec::new();

    // Peer A fills the only outbound slot.
    let peer_a = add_outbound_block_sync_peer(&service, 41, &mut held);
    match next_event(&mut events).await {
        BlockSyncEvent::PeerConnected(session) => assert_eq!(session.peer_id(), &peer_a),
        event => panic!("expected PeerConnected for peer A, got {event:?}"),
    }
    assert_eq!(service.peer_count(), 1);

    // A distinct, new peer at the full cap is rejected: no session is created.
    let _peer_b = add_outbound_block_sync_peer(&service, 42, &mut held);
    let quiet = tokio::time::timeout(Duration::from_millis(100), events.recv()).await;
    assert!(
        quiet.is_err(),
        "a new peer at a full per-direction cap must be rejected",
    );
    assert_eq!(service.peer_count(), 1);

    // Re-registering peer A (the collision adoption) replaces its session even
    // though the cap is full, because A is already counted. The stale-session
    // teardown keys on the session id, so only a fresh PeerConnected is emitted.
    let peer_a_again = add_outbound_block_sync_peer(&service, 41, &mut held);
    assert_eq!(peer_a_again, peer_a);
    match next_event(&mut events).await {
        BlockSyncEvent::PeerConnected(session) => assert_eq!(session.peer_id(), &peer_a),
        event => panic!("expected PeerConnected for replaced peer A, got {event:?}"),
    }
    assert_eq!(
        service.peer_count(),
        1,
        "the replacement must not leave two sessions for the same peer",
    );
}

#[tokio::test]
async fn reactor_keeps_applying_body_after_non_advancing_duplicate_result() {
    let blocks = mainnet_blocks_1_to_3();
    let block1_size = block_size(&blocks[0]);
    let mut config = immediate_body_download_config();
    // One block's size hint of budget: size-based reservation now means one
    // advertised body fills the budget, throttling to one in-flight request.
    config.max_inflight_block_bytes = u64::from(block1_size);

    let (_tip_tx, tip_rx) = watch::channel((block::Height(2), blocks[1].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(2), blocks[1].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        42,
        block::Height(2),
        blocks[1].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: blocks[0].hash(),
                size: BlockSizeEstimate::Advertised(block1_size),
            },
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: blocks[1].hash(),
                size: BlockSizeEstimate::Advertised(block1_size),
            },
        ]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 1)
    );

    send_inbound(&inbound_tx, BlockSyncMessage::Block(blocks[0].clone())).await;
    let submit_token = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => {
                assert_eq!(block.hash(), blocks[0].hash());
                break token;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before submit: {action:?}"),
        }
    };

    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token: submit_token,
            height: block::Height(1),
            hash: blocks[0].hash(),
            result: BlockApplyResult::Duplicate,
            local_frontier: Some(BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(0),
                verified_block_hash: block::Hash([0; 32]),
            }),
        })
        .await
        .expect("non-advancing duplicate completion queues");

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: blocks[0].hash(),
                size: BlockSizeEstimate::Advertised(block1_size),
            },
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: blocks[1].hash(),
                size: BlockSizeEstimate::Advertised(block1_size),
            },
        ]))
        .await
        .expect("needed metadata after duplicate queues");

    // A re-request would land on this peer's own real outbound, so watch the wire.
    let no_duplicate_request = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(frame) = outbound_rx.recv().await {
            if let BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } = BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
            {
                panic!("non-advancing duplicate result re-requested {start_height:?}/{count}");
            }
        }
    })
    .await;
    assert!(
        no_duplicate_request.is_err(),
        "reactor should keep waiting after a duplicate result that did not advance the frontier",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_keeps_active_response_when_needed_snapshot_omits_inflight_height() {
    let blocks = fake_sequential_blocks(3);
    let config = immediate_body_download_config();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(42);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(3),
                tip_hash: blocks[2].hash(),
                max_blocks_per_response: 16,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks.iter().map(block_meta).collect(),
        ))
        .await
        .expect("needed metadata queues");

    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 3)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("first block queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } if block.hash() == blocks[0].hash() => break,
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before first submit: {action:?}"),
        }
    }

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[2])]))
        .await
        .expect("newer needed metadata queues");

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[1].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("second block queues");

    let mut submitted_second = false;
    while let Ok(Some(action)) = tokio::time::timeout(Duration::from_secs(1), actions.recv()).await
    {
        match action {
            BlockSyncAction::SubmitBlock { block, .. } if block.hash() == blocks[1].hash() => {
                submitted_second = true;
                break;
            }
            BlockSyncAction::Misbehavior {
                peer,
                reason: BlockSyncMisbehavior::UnsolicitedBlock,
            } if peer == peer_id => panic!("in-flight body was misclassified as unsolicited"),
            BlockSyncAction::QueryNeededBlocks { .. } | BlockSyncAction::SubmitBlock { .. } => {}
            action => panic!("unexpected action after second block: {action:?}"),
        }
    }

    assert!(
        submitted_second,
        "second body from the original active response should remain correlated",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_ignores_unmatched_body_for_currently_needed_height() {
    let blocks = mainnet_blocks_1_to_3();
    let config = immediate_body_download_config();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(142);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    wait_for_outbound_status(&mut outbound_rx).await;

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(2),
                servable_high: block::Height(3),
                tip_hash: blocks[2].hash(),
                max_blocks_per_response: 16,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(1), blocks[0].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    // inverted inbound flow: the body is decoded by the peer's pipe-routine in its own
    // task, racing the reactor's producer `work.extend`. The reactor extends the
    // WorkQueue BEFORE publishing the candidate set, so waiting for height 1 to
    // appear in the candidate watch deterministically confirms the work is in
    // `pending` before we deliver the (unmatched-but-needed) body — without this
    // the routine could decode the body before `extend` runs and wrongly score it.
    let mut candidates = handle.subscribe_candidate_state();
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[0])]))
        .await
        .expect("needed metadata queues");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if candidates
                .borrow_and_update()
                .missing_block_bodies
                .contains(&block::Height(1))
            {
                return;
            }
            candidates.changed().await.expect("candidate watch is live");
        }
    })
    .await
    .expect("producer extends the needed height into the WorkQueue");

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("unmatched needed block queues");

    let quiet = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            if let BlockSyncAction::Misbehavior { reason, .. } = next_action(&mut actions).await {
                return reason;
            }
        }
    })
    .await;
    assert!(
        quiet.is_err(),
        "unmatched body for a currently needed height should not be hard misbehavior",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_accepts_unmatched_body_for_queued_height() {
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    config.max_inflight_block_bytes = u64::from(block_size(&blocks[0]));
    let (_tip_tx, tip_rx) = watch::channel((block::Height(1), blocks[0].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(1), blocks[0].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        143,
        block::Height(1),
        blocks[0].hash(),
        1,
        1,
    )
    .await;

    // Queue height 1 with a deliberately oversized advertised hint so its send-time
    // reservation (the estimate) exceeds the one-actual-block budget: no GetBlocks
    // can issue, leaving the height queued without an outstanding request. The body,
    // arriving unsolicited, reserves only its small actual size and is still
    // buffered. Under the old worst-case reservation this gap was implicit; with
    // size-based reservation the oversized hint recreates it.
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: blocks[0].hash(),
            size: BlockSizeEstimate::Advertised(
                u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES).expect("worst case fits u32"),
            ),
        }]))
        .await
        .expect("needed metadata queues");

    // No GetBlocks may issue (the body must stay queued without an outstanding
    // request); a request would land on this peer's own real outbound.
    let no_getblocks = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            if let Some(frame) = outbound_rx.recv().await {
                if let BlockSyncMessage::GetBlocks { .. } =
                    BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
                {
                    return;
                }
            } else {
                return;
            }
        }
    })
    .await;
    assert!(
        no_getblocks.is_err(),
        "test setup requires the body to remain queued without an outstanding request",
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("unmatched queued block queues");

    let submitted = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let BlockSyncAction::SubmitBlock { block, .. } = next_action(&mut actions).await {
                return block.hash();
            }
        }
    })
    .await
    .expect("unmatched queued body is submitted");
    assert_eq!(submitted, blocks[0].hash());

    reactor_task.abort();
}

// Removed by the per-peer routine design: `reactor_accepts_queued_body_from_
// recently_disconnected_peer` exercised the reactor's "late body" path — a body
// arriving for a peer with no live routine, demuxed by the reactor's
// `handle_late_body`/`accept_unmatched_queued_body`. The inverted data flow removes
// that path entirely: a peer's frames are decoded by its own per-peer pipe-routine,
// so once the peer disconnects its stream is closed and the routine has exited —
// there is no transport over which a late body could arrive, and no reactor inbound
// demux to accept one. The live-peer unmatched-queued-body acceptance is still
// covered by `reactor_accepts_unmatched_body_for_queued_height` (the routine's
// `accept_unmatched_queued_body`, driven by a real inbound frame just above).

#[tokio::test]
async fn reactor_queries_needed_blocks_above_submitted_floor() {
    let blocks = mainnet_blocks_1_to_3();
    let block1_size = block_size(&blocks[0]);
    let block2_size = block_size(&blocks[1]);
    let mut config = immediate_body_download_config();
    config.max_inflight_block_bytes = BS_PER_BLOCK_WORST_CASE_BYTES * 2;

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(43);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(3),
                tip_hash: blocks[2].hash(),
                max_blocks_per_response: 2,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: blocks[0].hash(),
                size: BlockSizeEstimate::Advertised(block1_size),
            },
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: blocks[1].hash(),
                size: BlockSizeEstimate::Advertised(block2_size),
            },
        ]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 2)
    );

    for block in blocks.iter().take(2) {
        inbound_tx
            .send(
                BlockSyncMessage::Block(block.clone())
                    .encode_frame()
                    .expect("block encodes"),
            )
            .await
            .expect("block queues");
    }

    let mut submitted = Vec::new();
    while submitted.len() < 2 {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => {
                submitted.push((
                    block.coinbase_height().expect("test block has height"),
                    token,
                ));
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before checkpoint submissions: {action:?}"),
        }
    }
    assert_eq!(
        submitted
            .iter()
            .map(|(height, _token)| *height)
            .collect::<Vec<_>>(),
        vec![block::Height(1), block::Height(2)]
    );

    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token: submitted[0].1,
            height: block::Height(1),
            hash: blocks[0].hash(),
            result: BlockApplyResult::Committed,
            local_frontier: None,
        })
        .await
        .expect("apply-finished event queues");

    // routines ping the producer on a low-water timer, so an early query can
    // fire while the contiguous prefix is still draining into `applying` (floor
    // still 1). Wait for the query whose lower bound has reached the submitted
    // floor (2) — that is the one that must skip the already-submitted bodies.
    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip: block::Height(2),
                best_header_tip,
            } => {
                assert_eq!(
                    best_header_tip,
                    block::Height(3),
                    "missing-body query must skip already submitted contiguous bodies",
                );
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before needed-block query: {action:?}"),
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_retries_submitted_body_after_apply_rejection() {
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let block_bytes = block_size(&block);
    let mut config = immediate_body_download_config();
    config.max_inflight_block_bytes = BS_PER_BLOCK_WORST_CASE_BYTES;

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(42);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block.hash(),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(1), block.hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block.hash(),
            size: BlockSizeEstimate::Advertised(block_bytes),
        }]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 1)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(block.clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");
    let submit_token = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock {
                token,
                block: submitted,
            } => {
                assert_eq!(submitted.hash(), block.hash());
                break token;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before submit: {action:?}"),
        }
    };

    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token: submit_token,
            height: block::Height(1),
            hash: block.hash(),
            result: BlockApplyResult::Rejected,
            local_frontier: None,
        })
        .await
        .expect("apply-finished event queues");
    // the rejection rollback (`reset_above` + floor reset) runs on the
    // Sequencer task while routines independently re-query, so re-supply the needed
    // metadata on every `QueryNeededBlocks` (idempotent — filtered while the height
    // is still in flight, re-extended once the rollback clears it) and wait for the
    // re-request that proves capacity/coverage were released.
    let retry_meta = vec![BlockSyncBlockMeta {
        height: block::Height(1),
        hash: block.hash(),
        size: BlockSizeEstimate::Advertised(block_bytes),
    }];
    loop {
        tokio::select! {
            biased;
            frame = outbound_rx.recv() => {
                let frame = frame.expect("outbound channel is live");
                match BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes") {
                    BlockSyncMessage::GetBlocks {
                        start_height: block::Height(1),
                        count,
                    } => {
                        assert_eq!(
                            count, 1,
                            "apply rejection must release capacity and clear submitted coverage"
                        );
                        break;
                    }
                    BlockSyncMessage::Status(_) => {}
                    msg => panic!("unexpected outbound message before retry request: {msg:?}"),
                }
            }
            action = actions.recv() => {
                match action.expect("action channel is live") {
                    BlockSyncAction::QueryNeededBlocks { .. } => {
                        handle
                            .send(BlockSyncEvent::NeededBlocks(retry_meta.clone()))
                            .await
                            .expect("needed metadata queues after rejection");
                    }
                    BlockSyncAction::Misbehavior { .. } => {}
                    action => panic!("unexpected action before retry request: {action:?}"),
                }
            }
        }
    }

    reactor_task.abort();
}

/// footgun regression: downloads gate ONLY on byte budget + per-peer slots,
/// never on floor-distance / near-tip lag. With the verified floor at 0 and
/// needed heights far above it (1..=4 with the header tip near 1000), a peer with
/// free slots and ample budget MUST keep issuing GetBlocks — there is no
/// near-tip pause. The deleted `reactor_pauses_new_body_downloads_near_tip_by_default`
/// asserted the opposite; reintroducing any lag/near-tip gate fails this test
/// (the GetBlocks would never arrive).
#[tokio::test]
async fn reactor_keeps_issuing_far_above_floor_with_no_near_tip_pause() {
    // The *default* config previously paused within 2 blocks of the header tip;
    // here the needed heights sit far below a high header tip, but the point is
    // that issuance proceeds regardless of how close to (or far from) the tip we
    // are — only budget + slots gate it.
    let config = ZakuraBlockSyncConfig::default();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // Peer serves heights 1..=4 with four concurrent single-block slots.
    let (_peer_id, _inbound, mut outbound) = connect_peer_with_status_message(
        &service,
        &mut actions,
        41,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(4),
            tip_hash: block::Hash([4; 32]),
            max_blocks_per_response: 1,
            max_inflight_requests: 4,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        },
    )
    .await;

    // Header tip far above the floor (lag is huge — would have *not* paused) and
    // also exercises that being far from the tip does not gate either.
    tip_tx
        .send((block::Height(1_000), block::Hash([9; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(
            (1..=4)
                .map(|height| BlockSyncBlockMeta {
                    height: block::Height(height),
                    hash: block::Hash([height as u8; 32]),
                    size: BlockSizeEstimate::Advertised(1_000),
                })
                .collect(),
        ))
        .await
        .expect("needed metadata queues");

    // All four slots fill: no near-tip pause exists to gate issuance.
    let mut heights = Vec::new();
    for _ in 0..4 {
        let (start_height, count) = wait_for_outbound_getblocks(&mut outbound).await;
        assert_eq!(count, 1);
        heights.push(start_height.0);
    }
    heights.sort_unstable();
    assert_eq!(
        heights,
        vec![1, 2, 3, 4],
        "a peer with free slots and budget must keep issuing regardless of floor distance"
    );

    reactor_task.abort();
}

#[tokio::test]
async fn routine_refills_after_budget_release_no_missed_wake() {
    // missed-wake guard at the routine level: a routine blocked on an
    // exhausted byte budget must re-fill when budget is freed. The budget holds
    // exactly one worst-case block, so the first GetBlocks exhausts it; delivering
    // that body releases budget (shrink + commit) and the routine must issue the
    // next GetBlocks without any external nudge.
    let mut config = immediate_body_download_config();
    config.max_inflight_block_bytes = BS_PER_BLOCK_WORST_CASE_BYTES;
    config.max_blocks_per_response = 1;
    config.request_timeout = Duration::from_secs(300);
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status_message(
        &service,
        &mut actions,
        70,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(2),
            tip_hash: blocks[1].hash(),
            max_blocks_per_response: 1,
            max_inflight_requests: 4,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        },
    )
    .await;

    tip_tx
        .send((block::Height(2), blocks[1].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[0]),
            block_meta(&blocks[1]),
        ]))
        .await
        .expect("needed metadata queues");

    // Only one worst-case block fits the budget, so exactly one GetBlocks is
    // issued first; the routine then parks blocked on the exhausted byte budget.
    let (first_start, _count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(first_start, block::Height(1));

    // Deliver height 1's body. Its worst-case reservation shrinks to the actual
    // size, but the remaining reservation still leaves under one worst-case block
    // free, so the routine cannot yet issue height 2 — the budget is freed only
    // when height 1 *commits* and the Sequencer releases its reservation.
    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block frame encodes"),
        )
        .await
        .expect("block frame queues");

    // Drive height 1 to commit: drain its SubmitBlock and report it applied. The
    // Sequencer task then releases the byte budget and the capacity notify must
    // wake the budget-blocked routine to issue the next GetBlocks (the     // missed-wake guarantee — a release between the routine's fill-check and its
    // await must not be lost).
    let token = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => {
                assert_eq!(block.coinbase_height(), Some(block::Height(1)));
                break token;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before SubmitBlock: {action:?}"),
        }
    };
    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token,
            height: block::Height(1),
            hash: blocks[0].hash(),
            result: BlockApplyResult::Committed,
            local_frontier: None,
        })
        .await
        .expect("apply-finished event queues");

    let (second_start, _count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(
        second_start,
        block::Height(2),
        "freeing the byte budget must wake the routine to issue the next request (no missed wake)"
    );

    reactor_task.abort();
}

#[tokio::test]
async fn routine_disconnect_returns_outstanding_and_releases_budget() {
    // disconnect-mid-fetch guard: cancelling a routine with unreceived
    // outstanding must return those heights to `work.pending` and release their
    // budget, so a fresh peer is offered the same height. Driven black-box: the
    // first peer takes the height, then disconnects mid-fetch; a second peer must
    // be offered it.
    let mut config = immediate_body_download_config();
    config.request_timeout = Duration::from_secs(300);
    config.peer_limits.max_outbound_peers = 4;
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (peer_a, _a_in, mut a_out) = connect_peer_with_status(
        &service,
        &mut actions,
        0x71,
        block::Height(1),
        blocks[0].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    tip_tx
        .send((block::Height(1), blocks[0].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[0])]))
        .await
        .expect("needed metadata queues");

    let (start, _count) = wait_for_outbound_getblocks(&mut a_out).await;
    assert_eq!(start, block::Height(1));

    // Disconnect peer A mid-fetch (it never answers). Its routine's `Drop` guard
    // must return height 1 to `pending` and release its reservation.
    service.remove_peer(&peer_a);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if service.peer_count() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnect releases the block-sync peer slot");

    // A fresh peer that can serve height 1 must be offered it again — proving the
    // height returned to `pending` (and its budget was released so the new request
    // can reserve).
    let (_peer_b, _b_in, mut b_out) = connect_peer_with_status(
        &service,
        &mut actions,
        0x72,
        block::Height(1),
        blocks[0].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    let offered = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (start, _count) = wait_for_outbound_getblocks(&mut b_out).await;
            if start == block::Height(1) {
                return true;
            }
        }
    })
    .await
    .expect("a fresh peer must be offered the re-queued height after a disconnect mid-fetch");
    assert!(offered);

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_reserves_size_hint_per_block_not_worst_case() {
    // The issuance path (`try_fill`) reserves the advertised size hint per block,
    // NOT `BS_PER_BLOCK_WORST_CASE_BYTES`. The global byte budget = 3 worst-case
    // blocks; the peer advertises a generous slot/response/block-count budget and
    // tiny 1 KiB size hints. Under the old worst-case reservation the budget would
    // bound the request to 3 blocks; because the reservation now honors the 1 KiB
    // hints, the full 16-block range fits under the budget and the request is the
    // whole 16-block range.
    let budget_blocks = 3u32;
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: u64::from(budget_blocks) * BS_PER_BLOCK_WORST_CASE_BYTES,
        // Generous per-request block count (the default is 1) so the count cap is
        // not the binding constraint — the byte budget is.
        max_blocks_per_response: 16,
        ..ZakuraBlockSyncConfig::default()
    };

    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // One slot, a generous response-byte and per-request block count, so neither
    // the slot count, the peer's response-byte cap, nor the block-count cap is the
    // binding constraint — only the per-peer byte cap is.
    let (_peer_id, _inbound, mut outbound) = connect_peer_with_status_message(
        &service,
        &mut actions,
        51,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(16),
            tip_hash: block::Hash([16; 32]),
            max_blocks_per_response: 16,
            max_inflight_requests: 1,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        },
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(
            (1..=16)
                .map(|height| BlockSyncBlockMeta {
                    height: block::Height(height),
                    hash: block::Hash([height as u8; 32]),
                    // Tiny hint: if the reservation honored this instead of the
                    // worst case, the whole 16-block range would fit under the cap.
                    size: BlockSizeEstimate::Advertised(1_000),
                })
                .collect(),
        ))
        .await
        .expect("needed metadata queues");

    let (_start_height, count) = wait_for_outbound_getblocks(&mut outbound).await;
    assert_eq!(
        count, 16,
        "the request must cover the full 16-block range: the 1 KiB size hints (not a \
         worst-case-per-block reservation) drive the budget, so all 16 fit under a \
         {budget_blocks}-worst-case-block budget",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_packs_small_estimates_under_peer_response_byte_cap() {
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: 4 * BS_PER_BLOCK_WORST_CASE_BYTES,
        max_blocks_per_response: 4,
        ..ZakuraBlockSyncConfig::default()
    };

    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    let one_worst_case_response =
        u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES).expect("worst-case block size fits u32");
    let (_peer_id, _inbound, mut outbound) = connect_peer_with_status_message(
        &service,
        &mut actions,
        52,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(4),
            tip_hash: block::Hash([4; 32]),
            max_blocks_per_response: 4,
            max_inflight_requests: 1,
            max_response_bytes: one_worst_case_response,
        },
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(
            (1..=4)
                .map(|height| BlockSyncBlockMeta {
                    height: block::Height(height),
                    hash: block::Hash([height as u8; 32]),
                    size: BlockSizeEstimate::Advertised(1_000),
                })
                .collect(),
        ))
        .await
        .expect("needed metadata queues");

    let (start_height, count) = wait_for_outbound_getblocks(&mut outbound).await;
    assert_eq!(start_height, block::Height(1));
    assert_eq!(
        count, 4,
        "small advertised estimates should pack more than the one worst-case block \
         allowed by the peer response byte cap",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_tiny_estimates_pack_into_one_worst_case_budget_block() {
    // A global budget of one worst-case block (2 MB) now holds many tiny-hint
    // bodies, because each reserves its size hint (clamped to the 1 KiB floor),
    // not a worst-case share. All 4 advertised heights fit in one request.
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES,
        max_blocks_per_response: 4,
        ..ZakuraBlockSyncConfig::default()
    };

    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    let (_peer_id, _inbound, mut outbound) = connect_peer_with_status_message(
        &service,
        &mut actions,
        53,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(4),
            tip_hash: block::Hash([4; 32]),
            max_blocks_per_response: 4,
            max_inflight_requests: 1,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        },
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(
            (1..=4)
                .map(|height| BlockSyncBlockMeta {
                    height: block::Height(height),
                    hash: block::Hash([u8::try_from(height).expect("test height fits u8"); 32]),
                    size: BlockSizeEstimate::Advertised(1),
                })
                .collect(),
        ))
        .await
        .expect("needed metadata queues");

    let (start_height, count) = wait_for_outbound_getblocks(&mut outbound).await;
    assert_eq!(start_height, block::Height(1));
    assert_eq!(
        count, 4,
        "all 4 tiny-hint heights pack into one worst-case-block budget, because each \
         reserves its 1 KiB-floored size hint rather than a worst-case share",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_zero_pause_threshold_preserves_lag_one_downloads() {
    let config = immediate_body_download_config();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config,
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);

    handle
        .send(BlockSyncEvent::HeaderTipChanged {
            height: block::Height(1),
            hash: block::Hash([1; 32]),
        })
        .await
        .expect("header-tip event queues");

    // A lag of 1 above the floor must still query needed blocks: there is no
    // near-tip pause. (The startup query carries best_header_tip 0; the tip-1
    // event carries best_header_tip 1.)
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(0),
            best_header_tip: block::Height(1),
        }
    ) {}

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_keeps_block_sync_peer_after_catch_up_and_reuses_later() {
    // Block-sync streams stay open even when this node is locally caught up: a
    // synced node can still be the server a fresh peer needs for historical
    // bodies, so closing the stream after every local catch-up would starve fresh
    // Zakura-only nodes between checkpoint windows.
    let mut config = immediate_body_download_config();
    config.peer_limits.max_outbound_peers = 1;
    let (_tip_tx, tip_rx) = watch::channel((block::Height(4), block::Hash([4; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(4), block::Hash([4; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        71,
        block::Height(6),
        block::Hash([6; 32]),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::HeaderTipChanged {
            height: block::Height(3),
            hash: block::Hash([3; 32]),
        })
        .await
        .expect("header-tip event queues");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: block::Hash([1; 32]),
                size: BlockSizeEstimate::Unknown,
            },
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: block::Hash([2; 32]),
                size: BlockSizeEstimate::Unknown,
            },
            BlockSyncBlockMeta {
                height: block::Height(3),
                hash: block::Hash([3; 32]),
                size: BlockSizeEstimate::Unknown,
            },
        ]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 3)
    );

    for height in [block::Height(1), block::Height(2)] {
        let hash_byte = u8::try_from(height.0).expect("test height fits in u8");
        handle
            .send(BlockSyncEvent::StateFrontiersChanged(BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: height,
                verified_block_hash: block::Hash([hash_byte; 32]),
            }))
            .await
            .expect("frontier event queues");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(handle.peer_snapshot().outbound_peers, 1);
        assert_eq!(service.peer_count(), 1);
    }

    inbound_tx
        .send(
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(1),
                returned: 3,
            }
            .encode_frame()
            .expect("BlocksDone encodes"),
        )
        .await
        .expect("BlocksDone queues");

    handle
        .send(BlockSyncEvent::StateFrontiersChanged(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(3),
            verified_block_hash: block::Hash([3; 32]),
        }))
        .await
        .expect("caught-up frontier event queues");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        handle.peer_snapshot().outbound_peers,
        1,
        "caught-up nodes must keep block-sync peers so they can serve fresh nodes",
    );
    assert_eq!(service.peer_count(), 1);

    handle
        .send(BlockSyncEvent::HeaderTipChanged {
            height: block::Height(6),
            hash: block::Hash([6; 32]),
        })
        .await
        .expect("later header-tip event queues");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(3),
            best_header_tip: block::Height(6),
        }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(4),
                hash: block::Hash([4; 32]),
                size: BlockSizeEstimate::Unknown,
            },
            BlockSyncBlockMeta {
                height: block::Height(5),
                hash: block::Hash([5; 32]),
                size: BlockSizeEstimate::Unknown,
            },
            BlockSyncBlockMeta {
                height: block::Height(6),
                hash: block::Hash([6; 32]),
                size: BlockSizeEstimate::Unknown,
            },
        ]))
        .await
        .expect("new needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(4), 3),
        "the retained block-sync peer should be reused after later header growth",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_accepts_multi_block_range_and_submits_parent_first() {
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 4,
        ..ZakuraBlockSyncConfig::default()
    };
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer = peer(43);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(3),
                tip_hash: blocks[2].hash(),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");
    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks
                .iter()
                .map(|block| BlockSyncBlockMeta {
                    height: block.coinbase_height().expect("test block has height"),
                    hash: block.hash(),
                    size: BlockSizeEstimate::Advertised(block_size(block)),
                })
                .collect(),
        ))
        .await
        .expect("needed metadata queues");

    let (start_height, count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(start_height, block::Height(1));
    assert_eq!(count, 3);

    for index in [1usize, 2, 0] {
        inbound_tx
            .send(
                BlockSyncMessage::Block(blocks[index].clone())
                    .encode_frame()
                    .expect("block encodes"),
            )
            .await
            .expect("block queues");
    }

    let mut submitted = Vec::new();
    while submitted.len() < 3 {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => submitted.push(
                block
                    .coinbase_height()
                    .expect("submitted test block has height"),
            ),
            BlockSyncAction::Misbehavior { reason, .. } => {
                panic!("honest multi-block response was misclassified: {reason:?}")
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before all submits: {action:?}"),
        }
    }
    assert_eq!(
        submitted,
        vec![block::Height(1), block::Height(2), block::Height(3)]
    );

    reactor_task.abort();
}

/// Every solicited body in a burst must reach the apply stage — the inbound
/// peer->reactor path must never silently drop a block body under load.
///
/// This is the regression guard for the production "drop-through" stall. The
/// per-peer wire queue is bounded; the old inbound pump forwarded decoded
/// messages with a non-blocking `try_send` and dropped solicited block bodies
/// once that queue filled during a body flood. A single dropped body wedges
/// `body_download_floor`, and because checkpoint-range commits wait
/// indefinitely for a contiguous range, the wedge never clears — every block
/// above it sits in `applying` forever and sync stalls at a checkpoint. The fix
/// makes the pump backpressure instead of dropping, so the flood always drains.
///
/// The harness makes a drop *fatal* rather than self-healing, which is what our
/// earlier tests missed: a one-slot wire queue forces the drop, a single
/// in-flight request stops the reactor from working around the gap, and a very
/// long request timeout means a dropped body is never re-requested within the
/// deadline. So a regression hangs here (outer timeout) instead of slowly
/// recovering and passing.
#[tokio::test]
async fn reactor_backpressures_inbound_body_flood_without_dropping_bodies() {
    const FLOOD: u32 = 64;
    let blocks = fake_sequential_blocks(FLOOD);
    let tip = blocks.last().expect("flood is non-empty").clone();
    let tip_height = tip.coinbase_height().expect("tip has height");

    let mut config = immediate_body_download_config();
    // One-slot wire queue: a pump that outruns the reactor by more than one
    // frame must backpressure, never drop.
    config.peer_limits.inbound_queue_depth = 1;
    // Hold the whole flood in flight at once so nothing pauses on the byte
    // budget; the inbound flood, not the budget, is what this test exercises.
    config.max_inflight_block_bytes = u64::MAX;
    // A dropped body must not be quietly re-requested and healed before the
    // deadline — that would hide the very regression this test guards.
    config.request_timeout = Duration::from_secs(300);

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer = peer(64);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: tip_height,
                tip_hash: tip.hash(),
                max_blocks_per_response: FLOOD,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");
    tip_tx
        .send((tip_height, tip.hash()))
        .expect("tip watch is live");

    // Feed solicited bodies from a dedicated task so the drive loop never blocks
    // on inbound backpressure while it still needs to drain reactor actions.
    let feed_blocks = blocks.clone();
    let (feed_tx, mut feed_rx) = mpsc::unbounded_channel::<u32>();
    let feeder = tokio::spawn(async move {
        while let Some(height) = feed_rx.recv().await {
            let frame = BlockSyncMessage::Block(feed_blocks[(height - 1) as usize].clone())
                .encode_frame()
                .expect("block frame encodes");
            if inbound_tx.send(frame).await.is_err() {
                break;
            }
        }
    });

    let metas: Vec<_> = blocks
        .iter()
        .map(|block| BlockSyncBlockMeta {
            height: block.coinbase_height().expect("test block has height"),
            hash: block.hash(),
            size: BlockSizeEstimate::Advertised(block_size(block)),
        })
        .collect();

    let drive = async {
        let mut submitted = std::collections::HashSet::new();
        while submitted.len() < FLOOD as usize {
            // Multiplex the node's real outbound (the `GetBlocks` requests that
            // tell us which heights to feed) with the action channel (needed-blocks
            // queries and submissions). No per-source timeout: if a regression
            // drops a body the reactor simply stops, and the outer deadline below
            // reports the failure.
            tokio::select! {
                biased;
                frame = outbound_rx.recv() => {
                    let Some(frame) = frame else { break };
                    if let BlockSyncMessage::GetBlocks {
                        start_height,
                        count,
                    } = BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
                    {
                        for height in start_height.0..start_height.0 + count {
                            feed_tx.send(height).expect("feeder task stays open");
                        }
                    }
                }
                action = actions.recv() => {
                    let Some(action) = action else { break };
                    match action {
                        BlockSyncAction::QueryNeededBlocks { .. } => {
                            handle
                                .send(BlockSyncEvent::NeededBlocks(metas.clone()))
                                .await
                                .expect("needed metadata queues");
                        }
                        BlockSyncAction::SubmitBlock { block, .. } => {
                            submitted.insert(
                                block.coinbase_height().expect("submitted block has height"),
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
        submitted
    };

    let submitted = tokio::time::timeout(Duration::from_secs(20), drive)
        .await
        .expect(
        "flooded bodies must all reach the apply stage; a dropped inbound body wedges block sync",
    );

    let expected: std::collections::HashSet<_> = (1..=FLOOD).map(block::Height).collect();
    assert_eq!(
        submitted, expected,
        "every solicited body in the flood must be submitted exactly once, with no drops"
    );

    feeder.abort();
    reactor_task.abort();
}

#[tokio::test]
async fn reactor_restarted_at_genesis_queries_and_schedules_without_tip_change() {
    let config = immediate_body_download_config();
    let blocks = mainnet_blocks_1_to_3();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(3), blocks[2].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(3), blocks[2].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    match next_action(&mut actions).await {
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip,
            best_header_tip,
        } => {
            assert_eq!(verified_block_tip, block::Height(0));
            assert_eq!(best_header_tip, block::Height(3));
        }
        action => panic!("restart from genesis must query missing bodies, got {action:?}"),
    }

    let (_peer, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        67,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks.iter().map(block_meta).collect(),
        ))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 3),
        "restart from genesis must schedule scratch body sync from height 1"
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");
    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => {
                assert_eq!(block.hash(), blocks[0].hash());
                assert_eq!(block.coinbase_height(), Some(block::Height(1)));
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before first scratch submit: {action:?}"),
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_accepts_blocks_done_after_completed_range() {
    let config = immediate_body_download_config();
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (peer, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        68,
        block::Height(2),
        blocks[1].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    tip_tx
        .send((block::Height(2), blocks[1].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[0])]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 1)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");
    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => {
                assert_eq!(block.hash(), blocks[0].hash());
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before submit: {action:?}"),
        }
    }

    inbound_tx
        .send(
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(1),
                returned: 1,
            }
            .encode_frame()
            .expect("BlocksDone encodes"),
        )
        .await
        .expect("BlocksDone queues");

    while let Ok(Some(action)) =
        tokio::time::timeout(Duration::from_millis(200), actions.recv()).await
    {
        if let BlockSyncAction::Misbehavior {
            peer: action_peer,
            reason,
        } = action
        {
            assert_ne!(
                (action_peer, reason),
                (peer.clone(), BlockSyncMisbehavior::UnsolicitedDone),
                "a valid terminator after a completed block response must not be scored"
            );
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_retries_missing_heights_after_partial_blocks_done() {
    let config = immediate_body_download_config();
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        69,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks.iter().map(block_meta).collect(),
        ))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 3)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[0].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");
    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => {
                assert_eq!(block.hash(), blocks[0].hash());
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before first submit: {action:?}"),
        }
    }

    inbound_tx
        .send(
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(1),
                returned: 1,
            }
            .encode_frame()
            .expect("BlocksDone encodes"),
        )
        .await
        .expect("BlocksDone queues");

    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 2),
        "partial responses must retry the contiguous missing suffix"
    );

    reactor_task.abort();
}

// `reactor_does_not_retry_missing_height_already_in_flight` was a fanout=2 test:
// it required the same range to be assigned to two peers and asserted that a
// partial response from one did not re-request heights the *other* still held.
// Fanout > 1 is removed in WorkQueue (a height is taken by exactly one peer), so the
// "in flight on another peer" scenario no longer exists. The structural property
// — a taken (in_flight) height is not re-takable — is now covered by the
// WorkQueue unit test `work_queue_take_dedups_a_height_across_peers`.

#[tokio::test]
async fn checkpoint_hole_disconnect_retries_first_missing_height_with_fresh_peer() {
    // Worst-case reservation caps a request at ~16 blocks (down from 128), so the
    // priming prefix is kept short enough to submit within the priming window
    // while still placing the hole behind several scheduled requests.
    const FIRST_NEEDED: u32 = 801;
    const PREFIX_END: u32 = 864;
    const HOLE_START: u32 = 865;
    const HOLE_END: u32 = 872;
    const LAST_METADATA: u32 = 933;
    const BEST_HEADER_TIP: u32 = 10_400;

    let blocks = fake_blocks_in_range(FIRST_NEEDED, LAST_METADATA);
    let block_at = |height: u32| -> Arc<block::Block> {
        let index =
            usize::try_from(height - FIRST_NEEDED).expect("test height is inside block vector");
        blocks[index].clone()
    };
    let metas: Vec<_> = blocks.iter().map(block_meta).collect();
    let prefix: std::collections::HashSet<_> =
        (FIRST_NEEDED..=PREFIX_END).map(block::Height).collect();
    let sparse_above_hole: std::collections::HashSet<_> = [873, 888, 905, 920]
        .into_iter()
        .map(block::Height)
        .collect();

    let mut config = immediate_body_download_config();
    config.fanout = 1;
    config.max_inflight_block_bytes = u64::MAX;
    config.request_timeout = Duration::from_secs(300);
    config.peer_limits.max_outbound_peers = 1;
    config.peer_limits.inbound_queue_depth = 128;
    // Worst-case reservation caps a request at ~16 blocks, so the prefix needs
    // more concurrent requests; keep the outbound queue wide enough that a fill
    // pass never overflows it and cancels the peer.
    config.peer_limits.outbound_queue_depth = 128;

    let (_tip_tx, tip_rx) = watch::channel((block::Height(BEST_HEADER_TIP), block::Hash([10; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(800),
            verified_block_tip: block::Height(800),
            verified_block_hash: block::Hash([8; 32]),
        },
        (block::Height(BEST_HEADER_TIP), block::Hash([10; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    let (old_peer, old_inbound, mut old_outbound) = connect_peer_with_status_message(
        &service,
        &mut actions,
        70,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(LAST_METADATA),
            tip_hash: block_at(LAST_METADATA).hash(),
            max_blocks_per_response: MAX_BS_BLOCKS_PER_REQUEST,
            // Worst-case reservation caps a request at `max_response_bytes /
            // MAX_BLOCK_BYTES` (~16) blocks, so allow more concurrent requests to
            // cover the checkpoint prefix within the priming window.
            max_inflight_requests: 8,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        },
    )
    .await;
    handle
        .send(BlockSyncEvent::NeededBlocks(metas.clone()))
        .await
        .expect("checkpoint metadata queues after peer connection");

    let (feed_tx, mut feed_rx) = mpsc::unbounded_channel::<u32>();
    let blocks_for_feeder = blocks.clone();
    let feeder = tokio::spawn(async move {
        while let Some(height) = feed_rx.recv().await {
            let index = usize::try_from(height - FIRST_NEEDED)
                .expect("fed test height is inside block vector");
            let frame = BlockSyncMessage::Block(blocks_for_feeder[index].clone())
                .encode_frame()
                .expect("block frame encodes");
            if old_inbound.send(frame).await.is_err() {
                break;
            }
        }
    });

    let mut requests: Vec<(block::Height, u32)> = Vec::new();
    let mut submitted = std::collections::HashSet::new();
    let primed = tokio::time::timeout(Duration::from_secs(40), async {
        while !prefix.is_subset(&submitted)
            || !requests.iter().any(|(start, count)| {
                *start <= block::Height(HOLE_START) && start.0.saturating_add(*count) > HOLE_END
            })
        {
            // The old peer's `GetBlocks` arrive on its own real outbound (reading
            // that stream proves they targeted it); needed-block queries and
            // submissions come over the action channel.
            tokio::select! {
                biased;
                frame = old_outbound.recv() => {
                    let frame = frame.expect("old peer outbound is live");
                    if let BlockSyncMessage::GetBlocks {
                        start_height,
                        count,
                    } = BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
                    {
                        requests.push((start_height, count));
                        let end_height = start_height
                            .0
                            .checked_add(count)
                            .expect("test request height range fits u32");
                        for height in start_height.0..end_height {
                            let height = block::Height(height);
                            if height.0 <= PREFIX_END || sparse_above_hole.contains(&height) {
                                feed_tx.send(height.0).expect("feeder task stays open");
                            }
                        }
                    }
                }
                action = actions.recv() => {
                    match action.expect("block-sync action channel should stay open") {
                        BlockSyncAction::QueryNeededBlocks {
                            verified_block_tip,
                            best_header_tip,
                        } => {
                            // queries fire at various floor states as commits
                            // advance the floor (it starts at 800 and climbs as the
                            // prefix commits), so the lower bound is `>= 800`, not
                            // exactly 800.
                            assert!(verified_block_tip >= block::Height(800));
                            assert_eq!(best_header_tip, block::Height(BEST_HEADER_TIP));
                            handle
                                .send(BlockSyncEvent::NeededBlocks(metas.clone()))
                                .await
                                .expect("checkpoint metadata queues");
                        }
                        BlockSyncAction::SubmitBlock { block, .. } => {
                            let height =
                                block.coinbase_height().expect("submitted block has height");
                            assert!(
                                submitted.insert(height),
                                "height {height:?} was submitted more than once before the hole retry"
                            );
                        }
                        action => {
                            panic!("unexpected action while priming checkpoint hole: {action:?}")
                        }
                    }
                }
            }
        }
    })
    .await;
    assert!(
        primed.is_ok(),
        "checkpoint-hole priming timed out: requests={requests:?}, submitted={} of {}",
        submitted.len(),
        prefix.len()
    );

    assert!(
        requests
            .iter()
            .any(|(start, count)| *start <= block::Height(HOLE_START)
                && start.0.saturating_add(*count) > HOLE_END),
        "the initial in-flight requests must cover the missing checkpoint hole"
    );

    service.remove_peer(&old_peer);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if handle.peer_snapshot().outbound_peers == 0 && service.peer_count() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnect releases the old block-sync peer slot");

    let (_new_peer, _new_inbound, mut new_outbound) = connect_peer_with_status_message(
        &service,
        &mut actions,
        71,
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(LAST_METADATA),
            tip_hash: block_at(LAST_METADATA).hash(),
            max_blocks_per_response: MAX_BS_BLOCKS_PER_REQUEST,
            max_inflight_requests: 8,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        },
    )
    .await;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // The fresh peer's `GetBlocks` arrive on its own real outbound (reading
            // that stream proves the request targeted it).
            tokio::select! {
                biased;
                frame = new_outbound.recv() => {
                    let frame = frame.expect("new peer outbound is live");
                    if let BlockSyncMessage::GetBlocks {
                        start_height,
                        count,
                    } = BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
                    {
                        assert_eq!(
                            start_height,
                            block::Height(HOLE_START),
                            "after a partial response disconnect, the first fresh request must fill \
                             the checkpoint hole instead of jumping above it or reusing a stale \
                             assignment"
                        );
                        assert!(count >= 1);
                        break;
                    }
                }
                action = actions.recv() => {
                    match action.expect("block-sync action channel should stay open") {
                        BlockSyncAction::QueryNeededBlocks { .. } => {
                            handle
                                .send(BlockSyncEvent::NeededBlocks(metas.clone()))
                                .await
                                .expect("post-disconnect metadata queues");
                        }
                        BlockSyncAction::SubmitBlock { block, .. } => {
                            let height =
                                block.coinbase_height().expect("submitted block has height");
                            assert!(
                                submitted.insert(height),
                                "height {height:?} was resubmitted before the checkpoint hole was retried"
                            );
                        }
                        action => {
                            panic!(
                                "unexpected action while waiting for checkpoint-hole retry: {action:?}"
                            )
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("fresh peer should request the checkpoint hole after disconnect");

    feeder.abort();
    reactor_task.abort();
}

#[tokio::test]
async fn reactor_reset_mid_download_drops_stale_anchors_and_releases_budget() {
    let mut config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES * 2,
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(1), blocks[0].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(1), blocks[0].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        47,
        block::Height(3),
        blocks[2].hash(),
        1,
        u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES * 2).unwrap_or(u32::MAX),
    )
    .await;

    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(1),
            best_header_tip: block::Height(3),
        }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: blocks[1].hash(),
                size: BlockSizeEstimate::Advertised(10_000),
            },
            BlockSyncBlockMeta {
                height: block::Height(3),
                hash: blocks[2].hash(),
                size: BlockSizeEstimate::Advertised(10_000),
            },
        ]))
        .await
        .expect("old-fork needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 2)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[2].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("out-of-order old-fork block queues");

    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        }))
        .await
        .expect("reset event queues");
    // the reset (FrontierReset) and the producer are decoupled across tasks,
    // and routines re-query on a low-water ping, so several `QueryNeededBlocks`
    // can race the in-flight reset clear. Re-supply the new-fork metadata on every
    // query (idempotent: it is filtered while the stale height is still in flight,
    // and re-extended once the Sequencer's `reset_above` clears it) and wait for
    // the height-2 request that proves stale bytes were released.
    let new_fork_meta = vec![BlockSyncBlockMeta {
        height: block::Height(2),
        hash: block::Hash([92; 32]),
        size: BlockSizeEstimate::Advertised(20_000),
    }];
    loop {
        tokio::select! {
            biased;
            frame = outbound_rx.recv() => {
                let frame = frame.expect("outbound channel is live");
                match BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes") {
                    BlockSyncMessage::GetBlocks {
                        start_height: block::Height(2),
                        count,
                    } => {
                        assert_eq!(
                            count, 1,
                            "a full-budget new fork request can only be scheduled if stale bytes were released"
                        );
                        break;
                    }
                    BlockSyncMessage::GetBlocks { .. } | BlockSyncMessage::Status(_) => {}
                    msg => panic!("unexpected outbound message before new fork request: {msg:?}"),
                }
            }
            action = actions.recv() => {
                match action.expect("action channel is live") {
                    BlockSyncAction::QueryNeededBlocks { .. } => {
                        handle
                            .send(BlockSyncEvent::NeededBlocks(new_fork_meta.clone()))
                            .await
                            .expect("new-fork needed metadata queues");
                    }
                    // The honest in-flight height-3 body (within the peer's advertised
                    // servable range) that races the in-place reset must NOT be scored
                    // `UnsolicitedBlock` — `ignore_servable_range_response` drops it
                    // quietly so a reorg does not churn honest peers. Any misbehavior
                    // here is a regression and falls through to the panic below.
                    action => panic!("unexpected action before new fork request: {action:?}"),
                }
            }
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_forward_reset_preserves_submitted_successor_body() {
    let mut config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES * 2,
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(1), blocks[0].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(1), blocks[0].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        49,
        block::Height(3),
        blocks[2].hash(),
        1,
        u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES * 2).unwrap_or(u32::MAX),
    )
    .await;

    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(1),
            best_header_tip: block::Height(3),
        }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[1]),
            block_meta(&blocks[2]),
        ]))
        .await
        .expect("initial needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 2)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[1].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("contiguous body queues");
    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => {
                assert_eq!(block.hash(), blocks[1].hash());
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before first submit: {action:?}"),
        }
    }

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[2].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("successor body queues");
    let successor_token = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => {
                assert_eq!(block.hash(), blocks[2].hash());
                break token;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before successor submit: {action:?}"),
        }
    };

    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(2),
            verified_block_hash: blocks[1].hash(),
        }))
        .await
        .expect("forward reset event queues");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), actions.recv())
            .await
            .is_err(),
        "forward reset must not re-query or re-submit the preserved successor body",
    );

    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token: successor_token,
            height: block::Height(3),
            hash: blocks[2].hash(),
            result: BlockApplyResult::Committed,
            local_frontier: None,
        })
        .await
        .expect("successor apply result queues");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), actions.recv())
            .await
            .is_err(),
        "at-tip successor apply must not trigger a redundant needed-block query",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_forward_reset_preserves_future_outstanding_body() {
    let mut config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES * 2,
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(1), blocks[0].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(1), blocks[0].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        72,
        block::Height(3),
        blocks[2].hash(),
        1,
        u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES * 2).unwrap_or(u32::MAX),
    )
    .await;

    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(1),
            best_header_tip: block::Height(3),
        }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[1]),
            block_meta(&blocks[2]),
        ]))
        .await
        .expect("initial needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 2)
    );

    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(2),
            verified_block_hash: blocks[1].hash(),
        }))
        .await
        .expect("forward reset event queues");

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[2].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("successor body queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => {
                assert_eq!(block.hash(), blocks[2].hash());
                break;
            }
            BlockSyncAction::Misbehavior { peer, reason } => {
                assert_ne!(
                    (peer, reason),
                    (peer_id.clone(), BlockSyncMisbehavior::UnsolicitedBlock),
                    "forward reset must not drop a still-active successor request"
                );
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            BlockSyncAction::QueryBlocksByHeightRange { .. } => {}
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_forward_reset_preserves_buffered_successor_body() {
    let mut config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES * 2,
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(1), blocks[0].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(1), blocks[0].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        73,
        block::Height(3),
        blocks[2].hash(),
        1,
        u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES * 2).unwrap_or(u32::MAX),
    )
    .await;

    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(1),
            best_header_tip: block::Height(3),
        }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[1]),
            block_meta(&blocks[2]),
        ]))
        .await
        .expect("initial needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 2)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[2].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("successor body queues");
    inbound_tx
        .send(
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(2),
                returned: 2,
            }
            .encode_frame()
            .expect("BlocksDone encodes"),
        )
        .await
        .expect("BlocksDone queues");

    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(2),
            verified_block_hash: blocks[1].hash(),
        }))
        .await
        .expect("forward reset event queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => {
                assert_eq!(
                    block.hash(),
                    blocks[2].hash(),
                    "buffered successor must submit without a duplicate download"
                );
                break;
            }
            BlockSyncAction::Misbehavior { peer, reason } => {
                assert_ne!(
                    (peer, reason),
                    (peer_id.clone(), BlockSyncMisbehavior::UnsolicitedBlock),
                    "forward reset must not drop a buffered successor body"
                );
            }
            BlockSyncAction::QueryNeededBlocks { .. }
            | BlockSyncAction::QueryBlocksByHeightRange { .. } => {}
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_destructive_forward_reset_does_not_rerequest_same_hash_in_flight_apply() {
    let mut config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES * 2,
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let blocks = mainnet_blocks_1_to_3();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(2), blocks[1].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(2), blocks[1].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        68,
        block::Height(2),
        blocks[1].hash(),
        1,
        u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES * 2).unwrap_or(u32::MAX),
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[0]),
            block_meta(&blocks[1]),
        ]))
        .await
        .expect("initial needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 2)
    );

    for block in blocks.iter().take(2) {
        inbound_tx
            .send(
                BlockSyncMessage::Block(block.clone())
                    .encode_frame()
                    .expect("block encodes"),
            )
            .await
            .expect("block queues");
    }

    let mut submitted = Vec::new();
    while submitted.len() < 2 {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => submitted.push((
                block.coinbase_height().expect("test block has height"),
                token,
            )),
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before submitted bodies: {action:?}"),
        }
    }
    assert_eq!(
        submitted
            .iter()
            .map(|(height, _)| *height)
            .collect::<Vec<_>>(),
        vec![block::Height(1), block::Height(2)]
    );

    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: block::Hash([99; 32]),
        }))
        .await
        .expect("destructive forward reset queues");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(1),
            best_header_tip: block::Height(2),
        }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[1])]))
        .await
        .expect("same-hash needed metadata queues");

    // Sequencer task: the destructive reset preserves the submitted-apply record for height 2
    // (`remember_released_applies`) but `reset_above` drops its WorkQueue
    // `in_flight` claim, and the reactor's producer filter is now the hash-blind
    // `in_flight_contains` structural check (it can no longer read the Sequencer's
    // per-hash `has_submitted_apply`). So the same-hash height MAY now be
    // re-requested. The safety invariant the original test pinned still holds and
    // is what we verify: a re-delivered same-hash body whose apply is still
    // pending is dropped as redundant by the Sequencer and is NOT re-submitted
    // (no double apply), because the preserved submitted-apply record makes
    // `accept_body` report it `Redundant`.
    if let Ok((re_start, re_count)) = tokio::time::timeout(
        Duration::from_millis(200),
        wait_for_outbound_getblocks(&mut outbound_rx),
    )
    .await
    {
        assert_eq!(
            (re_start, re_count),
            (block::Height(2), 1),
            "any same-hash re-request must target exactly the released height"
        );
        // Deliver the same-hash body for the re-request and assert it is dropped
        // as redundant (no SubmitBlock) — the no-double-apply guarantee.
        inbound_tx
            .send(
                BlockSyncMessage::Block(blocks[1].clone())
                    .encode_frame()
                    .expect("block encodes"),
            )
            .await
            .expect("same-hash body queues");
        let no_resubmit = tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                match actions.recv().await {
                    Some(BlockSyncAction::SubmitBlock { block, .. })
                        if block.hash() == blocks[1].hash() =>
                    {
                        panic!("same-hash body with a pending apply must not be re-submitted")
                    }
                    Some(_) => {}
                    None => break,
                }
            }
        })
        .await;
        assert!(
            no_resubmit.is_err(),
            "a redundant same-hash body must be dropped, not re-submitted",
        );
    }

    // A genuine fork to a different hash at height 2 reaches block sync as a reset
    // (reanchor), which `reset_above`s the WorkQueue and clears any stale
    // `in_flight` claim for height 2 before the producer re-fills — the path the
    // reset path relies on to install a new per-height hash (a bare
    // `NeededBlocks` never hash-corrects an in-flight height). After that reset the
    // different hash at the same height must schedule.
    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: block::Hash([99; 32]),
        }))
        .await
        .expect("fork reanchor reset queues");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(1),
            ..
        }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(2),
            hash: block::Hash([42; 32]),
            size: BlockSizeEstimate::Advertised(block_size(&blocks[1])),
        }]))
        .await
        .expect("new-fork needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 1),
        "different hash at the same height must still schedule after a fork reset"
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_ignores_stale_apply_completion_after_resubmit() {
    let config = immediate_body_download_config();
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let block_hash = block.hash();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(61);
    let (inbound_tx, inbound_rx) = framed_channel(16);
    let (outbound_tx, mut outbound_rx) = framed_channel(16);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    wait_for_outbound_status(&mut outbound_rx).await;

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block_hash,
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(1), block_hash))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block_hash,
            size: BlockSizeEstimate::Advertised(block_size(&block)),
        }]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 1)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(block.clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("first body frame queues");
    let stale_token = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => {
                assert_eq!(block.hash(), block_hash);
                break token;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before first submit: {action:?}"),
        }
    };

    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }))
        .await
        .expect("reset event queues");
    // the reset's `reset_above` runs on the Sequencer task while routines
    // re-query, so re-supply the needed metadata on every query (idempotent until
    // the reset clears the stale in-flight) and wait for the re-fetch.
    let reset_meta = vec![BlockSyncBlockMeta {
        height: block::Height(1),
        hash: block_hash,
        size: BlockSizeEstimate::Advertised(block_size(&block)),
    }];
    loop {
        tokio::select! {
            biased;
            frame = outbound_rx.recv() => {
                let frame = frame.expect("outbound channel is live");
                match BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes") {
                    BlockSyncMessage::GetBlocks {
                        start_height: block::Height(1),
                        count,
                    } => {
                        assert_eq!(count, 1);
                        break;
                    }
                    BlockSyncMessage::GetBlocks { .. } | BlockSyncMessage::Status(_) => {}
                    msg => panic!("unexpected outbound message before reset re-fetch: {msg:?}"),
                }
            }
            action = actions.recv() => {
                match action.expect("action channel is live") {
                    BlockSyncAction::QueryNeededBlocks { .. } => {
                        handle
                            .send(BlockSyncEvent::NeededBlocks(reset_meta.clone()))
                            .await
                            .expect("needed metadata queues after reset");
                    }
                    BlockSyncAction::Misbehavior { .. } => {}
                    action => panic!("unexpected action before reset re-fetch: {action:?}"),
                }
            }
        }
    }
    inbound_tx
        .send(
            BlockSyncMessage::Block(block.clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("second body frame queues");
    let current_token = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => {
                assert_eq!(block.hash(), block_hash);
                break token;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before second submit: {action:?}"),
        }
    };
    assert_ne!(stale_token, current_token);

    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token: stale_token,
            height: block::Height(1),
            hash: block_hash,
            result: BlockApplyResult::Duplicate,
            local_frontier: None,
        })
        .await
        .expect("stale apply-finished event queues");
    // The stale completion must not release the current submission: it produces no
    // new `SubmitBlock` (no re-submission). Routines ping the producer on a
    // low-water timer, so a benign `QueryNeededBlocks` is allowed and skipped; the
    // releasing signal we guard against is a fresh `SubmitBlock`.
    while let Ok(Some(action)) =
        tokio::time::timeout(Duration::from_millis(100), actions.recv()).await
    {
        if let BlockSyncAction::SubmitBlock { .. } = action {
            panic!(
                "stale apply completion released/re-submitted the current submission: {action:?}"
            );
        }
    }

    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token: current_token,
            height: block::Height(1),
            hash: block_hash,
            result: BlockApplyResult::Committed,
            local_frontier: None,
        })
        .await
        .expect("current apply-finished event queues");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), actions.recv())
            .await
            .is_err(),
        "at-tip apply completion must not trigger a redundant needed-block query",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_fast_forward_reset_clears_buffered_bodies_and_releases_budget() {
    let mut config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES * 2,
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(1), blocks[0].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(1), blocks[0].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        50,
        block::Height(4),
        block::Hash([4; 32]),
        1,
        u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES * 2).unwrap_or(u32::MAX),
    )
    .await;

    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(1),
            best_header_tip: block::Height(3),
        }
    ) {}

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: blocks[1].hash(),
                size: BlockSizeEstimate::Advertised(10_000),
            },
            BlockSyncBlockMeta {
                height: block::Height(3),
                hash: blocks[2].hash(),
                size: BlockSizeEstimate::Advertised(10_000),
            },
        ]))
        .await
        .expect("initial needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 2)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[2].clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("out-of-order body queues");

    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(3),
            verified_block_hash: blocks[2].hash(),
        }))
        .await
        .expect("fast-forward reset event queues");

    tip_tx
        .send((block::Height(4), block::Hash([4; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(3),
            best_header_tip: block::Height(4),
        }
    ) {}
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        handle.peer_snapshot().outbound_peers,
        1,
        "caught-up reset keeps the previous block-sync peer available for serving",
    );
    assert_eq!(service.peer_count(), 1);

    // the fast-forward commit + budget release run on the Sequencer task while
    // routines re-query, so re-supply the needed metadata on every query and wait
    // for the height-4 request that proves the buffered bytes were released.
    let post_reset_meta = vec![BlockSyncBlockMeta {
        height: block::Height(4),
        hash: block::Hash([4; 32]),
        size: BlockSizeEstimate::Advertised(20_000),
    }];
    handle
        .send(BlockSyncEvent::NeededBlocks(post_reset_meta.clone()))
        .await
        .expect("post-reset needed metadata queues");
    loop {
        tokio::select! {
            biased;
            frame = outbound_rx.recv() => {
                let frame = frame.expect("outbound channel is live");
                match BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes") {
                    BlockSyncMessage::GetBlocks {
                        start_height: block::Height(4),
                        count,
                    } => {
                        assert_eq!(
                            count, 1,
                            "a full-budget request after fast-forward Reset requires releasing buffered bytes"
                        );
                        break;
                    }
                    BlockSyncMessage::GetBlocks { .. } | BlockSyncMessage::Status(_) => {}
                    msg => panic!("unexpected outbound message before fast-forward re-fetch: {msg:?}"),
                }
            }
            action = actions.recv() => {
                match action.expect("action channel is live") {
                    BlockSyncAction::QueryNeededBlocks { .. } => {
                        handle
                            .send(BlockSyncEvent::NeededBlocks(post_reset_meta.clone()))
                            .await
                            .expect("post-reset needed metadata queues");
                    }
                    BlockSyncAction::Misbehavior { .. } => {}
                    action => panic!("unexpected action before fast-forward re-fetch: {action:?}"),
                }
            }
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_fuzzes_arrival_order_across_fork_parent_first() {
    #[derive(Copy, Clone)]
    enum ForkBody {
        Old(usize),
        New(usize),
    }

    let cases = vec![
        (
            "stale-high-before-reset",
            vec![3],
            vec![],
            vec![ForkBody::Old(2), ForkBody::New(3), ForkBody::New(2)],
        ),
        (
            "old-prefix-before-reset",
            vec![2, 1],
            vec![],
            vec![ForkBody::Old(3), ForkBody::New(2), ForkBody::New(3)],
        ),
        (
            "stale-before-new-needed",
            vec![2],
            vec![3],
            vec![ForkBody::New(2), ForkBody::Old(2), ForkBody::New(3)],
        ),
        (
            "new-out-of-order-with-stale-tail",
            vec![],
            vec![2],
            vec![ForkBody::New(3), ForkBody::Old(3), ForkBody::New(2)],
        ),
    ];

    for (case, old_before_reset, old_before_new_needed, after_new_needed) in cases {
        let mut config = ZakuraBlockSyncConfig {
            max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES * 3,
            ..immediate_body_download_config()
        };
        config.peer_limits.outbound_queue_depth = 16;
        let old_blocks = mainnet_blocks_1_to_3();
        let new_blocks = vec![
            forked_block(&old_blocks[0], 101),
            forked_block(&old_blocks[1], 102),
            forked_block(&old_blocks[2], 103),
        ];
        let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
        let startup = BlockSyncStartup::new(
            BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(0),
                verified_block_hash: block::Hash([0; 32]),
            },
            (block::Height(0), block::Hash([0; 32])),
            tip_rx,
            config.clone(),
        );
        let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
        let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
        let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
            &service,
            &mut actions,
            51,
            block::Height(4),
            old_blocks[2].hash(),
            1,
            u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES * 3).unwrap_or(u32::MAX),
        )
        .await;

        tip_tx
            .send((block::Height(3), old_blocks[2].hash()))
            .expect("tip watch is live");
        while !matches!(
            next_action(&mut actions).await,
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip: block::Height(0),
                best_header_tip: block::Height(3),
            }
        ) {}
        handle
            .send(BlockSyncEvent::NeededBlocks(
                old_blocks
                    .iter()
                    .map(|block| BlockSyncBlockMeta {
                        height: block.coinbase_height().expect("test block has height"),
                        hash: block.hash(),
                        size: BlockSizeEstimate::Advertised(block_size(block)),
                    })
                    .collect(),
            ))
            .await
            .expect("old-fork needed metadata queues");
        assert_eq!(
            wait_for_outbound_getblocks(&mut outbound_rx).await,
            (block::Height(1), 3),
            "{case}: old fork request schedules"
        );

        let mut submitted_tip = block::Height(0);
        for height in old_before_reset {
            inbound_tx
                .send(
                    BlockSyncMessage::Block(old_blocks[height - 1].clone())
                        .encode_frame()
                        .expect("old-fork block encodes"),
                )
                .await
                .expect("old-fork block queues");
            drain_parent_first_actions(&mut actions, &mut submitted_tip, None).await;
        }

        handle
            .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(1),
                verified_block_hash: new_blocks[0].hash(),
            }))
            .await
            .expect("reset event queues");
        while !matches!(
            next_action(&mut actions).await,
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip: block::Height(1),
                best_header_tip: block::Height(3),
            }
        ) {}
        submitted_tip = block::Height(1);

        tip_tx
            .send((block::Height(3), new_blocks[2].hash()))
            .expect("tip watch is live");
        while !matches!(
            next_action(&mut actions).await,
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip: block::Height(1),
                best_header_tip: block::Height(3),
            }
        ) {}

        for height in old_before_new_needed {
            inbound_tx
                .send(
                    BlockSyncMessage::Block(old_blocks[height - 1].clone())
                        .encode_frame()
                        .expect("stale old-fork block encodes"),
                )
                .await
                .expect("stale old-fork block queues");
            drain_parent_first_actions(&mut actions, &mut submitted_tip, Some(&new_blocks)).await;
        }

        // reset and the producer are decoupled across tasks, so re-supply the
        // new-fork metadata on every query until the height-2 re-fetch appears.
        let new_fork_meta = vec![
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: new_blocks[1].hash(),
                size: BlockSizeEstimate::Advertised(block_size(&new_blocks[1])),
            },
            BlockSyncBlockMeta {
                height: block::Height(3),
                hash: new_blocks[2].hash(),
                size: BlockSizeEstimate::Advertised(block_size(&new_blocks[2])),
            },
        ];
        handle
            .send(BlockSyncEvent::NeededBlocks(new_fork_meta.clone()))
            .await
            .expect("new-fork needed metadata queues");
        loop {
            tokio::select! {
                biased;
                frame = outbound_rx.recv() => {
                    let frame = frame.expect("outbound channel is live");
                    match BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes") {
                        BlockSyncMessage::GetBlocks {
                            start_height: block::Height(2),
                            count,
                        } => {
                            assert_eq!(
                                count, 2,
                                "{case}: new fork request schedules after reset"
                            );
                            break;
                        }
                        BlockSyncMessage::GetBlocks { .. } | BlockSyncMessage::Status(_) => {}
                        msg => panic!("{case}: unexpected outbound message before new fork re-fetch: {msg:?}"),
                    }
                }
                action = actions.recv() => {
                    match action.expect("action channel is live") {
                        BlockSyncAction::QueryNeededBlocks { .. } => {
                            handle
                                .send(BlockSyncEvent::NeededBlocks(new_fork_meta.clone()))
                                .await
                                .expect("new-fork needed metadata queues");
                        }
                        BlockSyncAction::Misbehavior { .. } => {}
                        action => panic!("{case}: unexpected action before new fork re-fetch: {action:?}"),
                    }
                }
            }
        }

        for body in after_new_needed {
            let block = match body {
                ForkBody::Old(height) => old_blocks[height - 1].clone(),
                ForkBody::New(height) => new_blocks[height - 1].clone(),
            };
            inbound_tx
                .send(
                    BlockSyncMessage::Block(block)
                        .encode_frame()
                        .expect("fork body encodes"),
                )
                .await
                .expect("fork body queues");
            drain_parent_first_actions(&mut actions, &mut submitted_tip, Some(&new_blocks)).await;
        }
        assert_eq!(
            submitted_tip,
            block::Height(3),
            "{case}: new fork bodies submit parent-first through height 3"
        );

        handle
            .send(BlockSyncEvent::ChainTipGrow(BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(3),
                verified_block_hash: new_blocks[2].hash(),
            }))
            .await
            .expect("post-submit grow event queues");
        tip_tx
            .send((block::Height(4), block::Hash([4; 32])))
            .expect("tip watch is live");
        while !matches!(
            next_action(&mut actions).await,
            BlockSyncAction::QueryNeededBlocks {
                verified_block_tip: block::Height(3),
                best_header_tip: block::Height(4),
            }
        ) {}
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            handle.peer_snapshot().outbound_peers,
            1,
            "{case}: caught-up fork handling keeps the old block-sync peer available for serving",
        );
        assert_eq!(service.peer_count(), 1);
        handle
            .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
                height: block::Height(4),
                hash: block::Hash([4; 32]),
                size: BlockSizeEstimate::Advertised(60_000),
            }]))
            .await
            .expect("post-fuzz needed metadata queues");
        assert_eq!(
            wait_for_outbound_getblocks(&mut outbound_rx).await,
            (block::Height(4), 1),
            "{case}: byte budget returns to baseline after reset and submissions"
        );

        reactor_task.abort();
    }
}

#[tokio::test]
async fn reactor_competing_fork_download_switches_to_current_header_hashes() {
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(1), blocks[0].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(1), blocks[0].hash()),
        tip_rx,
        immediate_body_download_config(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(
        immediate_body_download_config(),
        handle.clone(),
    );
    let (peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        48,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    tip_tx
        .send((block::Height(3), blocks[2].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[1]),
            block_meta(&blocks[2]),
        ]))
        .await
        .expect("old fork metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 2)
    );

    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        }))
        .await
        .expect("reset event queues");
    // reset (`reset_above`) and the producer are decoupled, so re-supply the
    // new-fork metadata on every query until the height-2 re-fetch appears.
    let new_fork_meta = vec![BlockSyncBlockMeta {
        height: block::Height(2),
        hash: block::Hash([222; 32]),
        size: BlockSizeEstimate::Advertised(block_size(&blocks[1])),
    }];
    loop {
        tokio::select! {
            biased;
            frame = outbound_rx.recv() => {
                let frame = frame.expect("outbound channel is live");
                match BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes") {
                    BlockSyncMessage::GetBlocks {
                        start_height: block::Height(2),
                        count,
                    } => {
                        assert_eq!(count, 1);
                        break;
                    }
                    BlockSyncMessage::GetBlocks { .. } | BlockSyncMessage::Status(_) => {}
                    msg => panic!("unexpected outbound message before new fork re-fetch: {msg:?}"),
                }
            }
            action = actions.recv() => {
                match action.expect("action channel is live") {
                    BlockSyncAction::QueryNeededBlocks { .. } => {
                        handle
                            .send(BlockSyncEvent::NeededBlocks(new_fork_meta.clone()))
                            .await
                            .expect("new fork metadata queues");
                    }
                    BlockSyncAction::Misbehavior { .. } => {}
                    action => panic!("unexpected action before new fork re-fetch: {action:?}"),
                }
            }
        }
    }

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[1].clone())
                .encode_frame()
                .expect("old-fork body encodes"),
        )
        .await
        .expect("old-fork body queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, peer_id);
                assert_eq!(reason, BlockSyncMisbehavior::InvalidBlock);
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before stale body rejection: {action:?}"),
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_legacy_commit_dedups_inflight_request_and_reuses_budget() {
    let mut config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES,
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let blocks = mainnet_blocks_1_to_3();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, _inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        49,
        block::Height(2),
        blocks[1].hash(),
        1,
        u32::try_from(BS_PER_BLOCK_WORST_CASE_BYTES).unwrap_or(u32::MAX),
    )
    .await;

    tip_tx
        .send((block::Height(2), blocks[1].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: blocks[0].hash(),
            size: BlockSizeEstimate::Advertised(10_000),
        }]))
        .await
        .expect("first needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 1)
    );

    handle
        .send(BlockSyncEvent::ChainTipGrow(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        }))
        .await
        .expect("legacy commit grow event queues");

    tip_tx
        .send((block::Height(2), blocks[1].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(1),
            best_header_tip: block::Height(2),
        }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(2),
            hash: blocks[1].hash(),
            size: BlockSizeEstimate::Advertised(10_000),
        }]))
        .await
        .expect("second needed metadata queues");

    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 1),
        "legacy commit must release the duplicate in-flight reservation"
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_treats_duplicate_buffered_blocks_as_benign() {
    let config = immediate_body_download_config();
    let blocks = [
        mainnet_block(&BLOCK_MAINNET_1_BYTES),
        mainnet_block(&BLOCK_MAINNET_2_BYTES),
    ];
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(44);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    wait_for_outbound_status(&mut outbound_rx).await;
    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(2),
                tip_hash: blocks[1].hash(),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    tip_tx
        .send((block::Height(2), blocks[1].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks
                .iter()
                .map(|block| BlockSyncBlockMeta {
                    height: block.coinbase_height().expect("test block has height"),
                    hash: block.hash(),
                    size: BlockSizeEstimate::Advertised(block_size(block)),
                })
                .collect(),
        ))
        .await
        .expect("needed metadata queues");

    let (start_height, count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(start_height, block::Height(1));
    assert_eq!(count, 2);

    for block in [&blocks[1], &blocks[1], &blocks[0]] {
        inbound_tx
            .send(
                BlockSyncMessage::Block(block.clone())
                    .encode_frame()
                    .expect("block encodes"),
            )
            .await
            .expect("block queues");
    }

    let mut submitted = Vec::new();
    while submitted.len() < 2 {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => submitted.push(
                block
                    .coinbase_height()
                    .expect("submitted test block has height"),
            ),
            BlockSyncAction::Misbehavior { reason, .. } => {
                panic!("duplicate buffered body was misclassified: {reason:?}")
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before submit: {action:?}"),
        }
    }
    assert_eq!(submitted, vec![block::Height(1), block::Height(2)]);

    let quiet = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(action) = actions.recv().await {
            if let BlockSyncAction::Misbehavior { reason, .. } = action {
                panic!("duplicate buffered body was misclassified after submit: {reason:?}");
            }
        }
    })
    .await;
    assert!(quiet.is_err());

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_accepts_rapid_status_growth_without_spam_score() {
    let config = ZakuraBlockSyncConfig::default();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    drop(tip_tx);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle);
    let peer_id = peer(46);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    wait_for_outbound_status(&mut outbound_rx).await;

    for servable_high in [block::Height(1), block::Height(2)] {
        let hash_byte = u8::try_from(servable_high.0).expect("test height fits in u8");
        inbound_tx
            .send(
                BlockSyncMessage::Status(BlockSyncStatus {
                    servable_low: block::Height(1),
                    servable_high,
                    tip_hash: block::Hash([hash_byte; 32]),
                    max_blocks_per_response: 4,
                    max_inflight_requests: 1,
                    max_response_bytes: MAX_BS_RESPONSE_BYTES,
                })
                .encode_frame()
                .expect("status encodes"),
            )
            .await
            .expect("status frame queues");
    }

    let quiet = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(action) = actions.recv().await {
            if let BlockSyncAction::Misbehavior { reason, .. } = action {
                panic!("rapid status growth was misclassified: {reason:?}");
            }
        }
    })
    .await;
    assert!(quiet.is_err());

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_ignores_redundant_status_burst_without_spam_score() {
    let config = ZakuraBlockSyncConfig::default();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    drop(tip_tx);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle);
    let peer_id = peer(47);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    wait_for_outbound_status(&mut outbound_rx).await;

    let status = BlockSyncMessage::Status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(1),
        tip_hash: block::Hash([1; 32]),
        max_blocks_per_response: 4,
        max_inflight_requests: 1,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
    })
    .encode_frame()
    .expect("status encodes");
    for _ in 0..3 {
        inbound_tx
            .send(status.clone())
            .await
            .expect("redundant status queues");
    }

    let quiet = tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(action) = actions.recv().await {
            if let BlockSyncAction::Misbehavior { reason, .. } = action {
                panic!("redundant status burst was misclassified: {reason:?}");
            }
        }
    })
    .await;
    assert!(quiet.is_err());

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_rejects_block_hash_mismatch_without_hard_drop_for_size_mismatch() {
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        immediate_body_download_config(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(
        immediate_body_download_config(),
        handle.clone(),
    );
    let peer = peer(41);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block::Hash([1; 32]),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status queues");
    tip_tx
        .send((block::Height(1), block::Hash([9; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block::Hash([9; 32]),
            size: BlockSizeEstimate::Advertised(1),
        }]))
        .await
        .expect("needed metadata queues");
    // The node's GetBlocks lands on this peer's real outbound.
    while !matches!(
        BlockSyncMessage::decode_frame(
            tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
                .await
                .expect("outbound frame arrives")
                .expect("outbound channel is live")
        )
        .expect("frame decodes"),
        BlockSyncMessage::GetBlocks { .. }
    ) {}

    inbound_tx
        .send(
            BlockSyncMessage::Block(mainnet_block(&BLOCK_MAINNET_1_BYTES))
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::Misbehavior { reason, .. } => {
                assert_eq!(reason, BlockSyncMisbehavior::InvalidBlock);
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before invalid-block report: {action:?}"),
        }
    }

    reactor_task.abort();
}

// Anti-drift guard for the outbound block-sync send path.
//
// Production (and now all) block-sync scheduling sends outbound `GetBlocks`
// *directly* through `BlockSyncPeerSession` (the per-peer routine's
// `try_send_get_blocks`). There is no per-peer action-pump "source" mirroring the
// send onto a second path, so a single scheduled request can never produce more
// than one outbound frame.
//
// This production-shaped scheduling test locks in that single-sourced outbound
// contract: one scheduled request yields EXACTLY ONE outbound `GetBlocks` frame
// (the authoritative direct session send), and guards against a future
// double-send regression if any mirroring path were ever reintroduced.
#[tokio::test]
async fn scheduled_get_blocks_is_sent_once_via_session() {
    let config = immediate_body_download_config();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // Admit a peer through the production `add_peer` path with an observable
    // outbound transport channel.
    let peer = peer(57);
    let (inbound_tx, inbound_rx) = framed_channel(16);
    let (outbound_tx, mut outbound_rx) = framed_channel(16);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    // The peer can serve exactly one block above our tip; publish the header tip
    // and the needed metadata so the reactor schedules exactly one request.
    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block::Hash([9; 32]),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status queues");
    tip_tx
        .send((block::Height(1), block::Hash([9; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block::Hash([9; 32]),
            size: BlockSizeEstimate::Advertised(1),
        }]))
        .await
        .expect("needed metadata queues");

    // Drain the outbound stream for a bounded idle window and count `GetBlocks`
    // frames. A single scheduled request must produce exactly one outbound
    // `GetBlocks`: the authoritative direct send through `BlockSyncPeerSession`
    // (the per-peer routine's `try_send_get_blocks`). There is no longer any
    // per-peer source action pump that could (double-)send a second copy.
    let mut get_blocks = 0usize;
    let mut frames = 0usize;
    while frames < 16 {
        match tokio::time::timeout(Duration::from_millis(300), outbound_rx.recv()).await {
            Ok(Some(frame)) => {
                frames += 1;
                if matches!(
                    BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes"),
                    BlockSyncMessage::GetBlocks { .. }
                ) {
                    get_blocks += 1;
                }
            }
            _ => break,
        }
    }
    assert_eq!(
        get_blocks, 1,
        "one scheduled request must produce exactly one outbound GetBlocks via \
         BlockSyncPeerSession; a different count would mean a mirroring path is \
         (double-)sending in addition to the authoritative direct path",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_scores_peer_whose_invalid_body_is_rejected_by_consensus() {
    let request_bytes: u32 = 10_000;
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: BS_PER_BLOCK_WORST_CASE_BYTES * 2,
        ..immediate_body_download_config()
    };

    let blocks = mainnet_blocks_1_to_3();
    // A body that keeps block 1's header (so it passes the reactor's hash and
    // height gates) but carries an extra transaction, so its merkle root no
    // longer matches the header. The reactor no longer recomputes the merkle
    // root at ingress, so this body reaches consensus, which rejects it.
    let bad_body = block_with_bad_merkle_root(&blocks[0], &blocks[1]);
    assert_eq!(bad_body.hash(), blocks[0].hash());

    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (bad_peer, bad_inbound, mut bad_outbound) = connect_peer_with_status(
        &service,
        &mut actions,
        40,
        block::Height(1),
        blocks[0].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    tip_tx
        .send((block::Height(1), blocks[0].hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: blocks[0].hash(),
            size: BlockSizeEstimate::Advertised(request_bytes),
        }]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut bad_outbound).await,
        (block::Height(1), 1)
    );

    bad_inbound
        .send(
            BlockSyncMessage::Block(bad_body)
                .encode_frame()
                .expect("bad block frame encodes"),
        )
        .await
        .expect("bad block frame queues");

    // The merkle-invalid body is no longer filtered at ingress: it is buffered
    // and submitted to consensus.
    let submit_token = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => {
                assert_eq!(block.hash(), blocks[0].hash());
                break token;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before invalid body submit: {action:?}"),
        }
    };

    // Consensus rejects the invalid body. The reactor must attribute the
    // rejection to the peer that delivered it and score the peer as misbehavior,
    // rather than silently rolling back scheduling state and letting the peer
    // keep feeding invalid bodies for needed heights.
    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token: submit_token,
            height: block::Height(1),
            hash: blocks[0].hash(),
            result: BlockApplyResult::Rejected,
            local_frontier: None,
        })
        .await
        .expect("apply-finished event queues");

    // the apply-rejection `Misbehavior` is emitted by the Sequencer task while
    // the routines independently ping `RequeryNeeded`, so one or more
    // `QueryNeededBlocks` can race ahead of the misbehavior report. Skip queries
    // and wait for the misbehavior; if it never arrives the `next_action` timeout
    // fails the test (the peer was not scored).
    let scored = loop {
        if let BlockSyncAction::Misbehavior { peer, reason } = next_action(&mut actions).await {
            assert_eq!(peer, bad_peer);
            assert_eq!(reason, BlockSyncMisbehavior::InvalidBlock);
            break true;
        }
    };
    assert!(
        scored,
        "a consensus apply rejection must score the peer that delivered the body",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_serves_committed_blocks_with_count_and_byte_clamps() {
    let blocks = mainnet_blocks_1_to_3();
    let block1_size = block_size(&blocks[0]);
    let mut config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 2,
        max_response_bytes: block1_size,
        ..ZakuraBlockSyncConfig::default()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let (_tip_tx, tip_rx) = watch::channel((block::Height(4), block::Hash([4; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(3),
            verified_block_hash: blocks[2].hash(),
        },
        (block::Height(4), block::Hash([4; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        60,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    inbound_tx
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(1),
                count: 10,
            }
            .encode_frame()
            .expect("GetBlocks frame encodes"),
        )
        .await
        .expect("GetBlocks frame queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                assert_eq!(peer, peer_id);
                assert_eq!(start, block::Height(1));
                assert_eq!(count, 2);
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before block range query: {action:?}"),
        }
    }

    handle
        .send(BlockSyncEvent::BlockRangeResponseReady {
            peer: peer_id.clone(),
            start_height: block::Height(1),
            requested_count: 2,
            blocks: vec![
                (
                    block::Height(1),
                    blocks[0].clone(),
                    usize::try_from(block1_size).expect("block size fits usize"),
                ),
                (
                    block::Height(2),
                    blocks[1].clone(),
                    usize::try_from(block_size(&blocks[1])).expect("block size fits usize"),
                ),
            ],
        })
        .await
        .expect("served block response queues");

    assert_eq!(
        wait_for_outbound_block(&mut outbound_rx).await.hash(),
        blocks[0].hash()
    );
    assert_eq!(
        wait_for_outbound_blocks_done(&mut outbound_rx).await,
        (block::Height(1), 1),
        "max_response_bytes clamps the served response to one body"
    );

    inbound_tx
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(4),
                count: 1,
            }
            .encode_frame()
            .expect("GetBlocks frame encodes"),
        )
        .await
        .expect("above-tip GetBlocks frame queues");

    assert_eq!(
        wait_for_outbound_range_unavailable(&mut outbound_rx).await,
        (block::Height(4), 1)
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_never_serves_reorder_buffer_bodies() {
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    config.peer_limits.outbound_queue_depth = 16;
    let (_tip_tx, tip_rx) = watch::channel((block::Height(3), blocks[2].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(3), blocks[2].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        61,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[2])]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(3), 1)
    );
    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[2].clone())
                .encode_frame()
                .expect("block frame encodes"),
        )
        .await
        .expect("block frame queues");

    let quiet = tokio::time::timeout(Duration::from_millis(50), async {
        while let Some(action) = actions.recv().await {
            if matches!(action, BlockSyncAction::SubmitBlock { .. }) {
                panic!("height 3 must stay buffered behind the height 2 gap");
            }
        }
    })
    .await;
    assert!(quiet.is_err());

    inbound_tx
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(3),
                count: 1,
            }
            .encode_frame()
            .expect("GetBlocks frame encodes"),
        )
        .await
        .expect("GetBlocks frame queues");

    assert_eq!(
        wait_for_outbound_range_unavailable(&mut outbound_rx).await,
        (block::Height(3), 1),
        "uncommitted reorder-buffer body must not be served"
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_schedules_gap_below_buffered_reorder_run() {
    // Regression for the mainnet stuck-at-0 deadlock: a body run received above
    // an open gap must not starve the gap below it. The state reports every
    // header-known, body-missing height (it cannot see our in-memory reorder
    // buffer), so a re-query returns already-buffered heights too. With
    // multi-peer fanout the held range lingers in the scheduler queue. Because
    // `refresh_needed` builds one maximal contiguous range and `ensure` rejects
    // any range overlapping a queued one, the gap below the held run would never
    // be scheduled and `body_download_floor` would freeze forever while we
    // re-requested the already-held blocks. The reactor must drop already-held
    // heights from the needed set so the gap gets scheduled.
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    config.fanout = 3;
    config.peer_limits.outbound_queue_depth = 16;
    let (_tip_tx, tip_rx) = watch::channel((block::Height(3), blocks[2].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(3), blocks[2].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        63,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    // Height 2 is momentarily not offered, so we fetch and buffer height 3 in
    // the reorder buffer above the open height-2 gap.
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[2])]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(3), 1)
    );
    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[2].clone())
                .encode_frame()
                .expect("block frame encodes"),
        )
        .await
        .expect("block frame queues");

    // Height 3 is now buffered in the reorder buffer and marked covered. Drain
    // to quiescence: this both lets the reactor finish processing the body and
    // asserts the core fix — a buffered (covered) height is NEVER re-requested.
    // The production deadlock re-requested the held run thousands of times via
    // the retry path (which bypasses the `needed`-set filter), pinning the queue
    // and every peer slot so the gap below the run never got a request. A
    // re-request would land on this peer's own outbound, so watch the real wire.
    loop {
        tokio::select! {
            biased;
            frame = outbound_rx.recv() => {
                let frame = frame.expect("outbound channel is live");
                match BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes") {
                    BlockSyncMessage::GetBlocks { start_height, .. } => {
                        panic!("buffered (covered) height {start_height:?} must not be re-requested")
                    }
                    BlockSyncMessage::Status(_) => {}
                    msg => panic!("unexpected outbound message after buffering block: {msg:?}"),
                }
            }
            action = tokio::time::timeout(Duration::from_millis(50), actions.recv()) => {
                match action {
                    Ok(Some(BlockSyncAction::QueryNeededBlocks { .. })) => {}
                    Ok(Some(other)) => panic!("unexpected action after buffering block: {other:?}"),
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }

    // The state still reports both 2 and 3 as body-missing because the reorder
    // buffer is invisible to it. With height 3 covered, the reactor must schedule
    // the gap at height 2 rather than staying trapped on the buffered run.
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[1]),
            block_meta(&blocks[2]),
        ]))
        .await
        .expect("needed metadata queues");

    let (got_start, _count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(
        got_start,
        block::Height(2),
        "reactor must schedule the gap at height 2 below the buffered reorder run",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_debounces_status_advertisements_on_serving_tip_change() {
    let mut config = ZakuraBlockSyncConfig {
        status_refresh_interval: Duration::from_secs(60),
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let (_tip_tx, tip_rx) = watch::channel((block::Height(4), block::Hash([4; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(4), block::Hash([4; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, _inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        62,
        block::Height(4),
        block::Hash([4; 32]),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    // `connect_peer_with_status` already read (and asserted) the connect-time
    // `Status` off this peer's real outbound.

    handle
        .send(BlockSyncEvent::StateFrontiersChanged(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }))
        .await
        .expect("unchanged frontier queues");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), outbound_rx.recv())
            .await
            .is_err(),
        "unchanged serving range must not advertise"
    );

    handle
        .send(BlockSyncEvent::StateFrontiersChanged(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: block::Hash([1; 32]),
        }))
        .await
        .expect("changed frontier queues");
    match next_outbound_message(&mut outbound_rx).await {
        BlockSyncMessage::Status(status) => {
            assert_eq!(status.servable_high, block::Height(1));
            assert_eq!(handle.local_status().servable_high, block::Height(1));
        }
        msg => panic!("expected debounced Status after serving tip change, got {msg:?}"),
    }

    for height in [2, 3] {
        let hash_byte = u8::try_from(height).expect("test height fits in u8");
        handle
            .send(BlockSyncEvent::StateFrontiersChanged(BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(height),
                verified_block_hash: block::Hash([hash_byte; 32]),
            }))
            .await
            .expect("burst frontier queues");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), outbound_rx.recv())
            .await
            .is_err(),
        "rapid serving-tip changes must be debounced to one Status per window"
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_retries_status_to_peer_without_status_when_local_status_unchanged() {
    let mut config = ZakuraBlockSyncConfig {
        status_refresh_interval: Duration::from_millis(50),
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, _actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle);
    let peer = peer(63);
    let (_inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer,
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    assert!(matches!(
        next_outbound_message(&mut outbound_rx).await,
        BlockSyncMessage::Status(_)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), outbound_rx.recv())
            .await
            .is_err(),
        "initial Status send must consume the peer refresh allowance"
    );
    assert!(matches!(
        next_outbound_message(&mut outbound_rx).await,
        BlockSyncMessage::Status(_)
    ));

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_replies_to_status_after_status_send_allowance_reopens() {
    let mut config = ZakuraBlockSyncConfig {
        status_refresh_interval: Duration::from_millis(50),
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, _actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle);
    let peer = peer(64);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);

    service.add_peer(Peer::new_with_direction(
        peer,
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    assert!(matches!(
        next_outbound_message(&mut outbound_rx).await,
        BlockSyncMessage::Status(_)
    ));
    inbound_tx
        .send(
            BlockSyncMessage::Status(status())
                .encode_frame()
                .expect("inbound status frame encodes"),
        )
        .await
        .expect("inbound status queues");
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    inbound_tx
        .send(
            BlockSyncMessage::Status(status())
                .encode_frame()
                .expect("inbound status frame encodes"),
        )
        .await
        .expect("second inbound status queues");

    assert!(matches!(
        next_outbound_message(&mut outbound_rx).await,
        BlockSyncMessage::Status(_)
    ));

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_exchange_watch_converges_to_latest_valid_frontier() {
    let initial = test_frontier_update(0, 0, 0, FrontierChange::Snapshot);
    let (exchange, startup) =
        exchange_block_sync_startup(initial, immediate_body_download_config());

    exchange.publish_frontier(
        test_frontier_update(0, 0, 3, FrontierChange::HeaderAdvanced),
        "test",
    );
    exchange.publish_frontier(
        test_frontier_update(0, 0, 2, FrontierChange::HeaderAdvanced),
        "test",
    );
    exchange.publish_frontier(
        test_frontier_update(0, 0, 5, FrontierChange::HeaderAdvanced),
        "test",
    );
    exchange.publish_frontier(
        test_frontier_update(0, 0, 5, FrontierChange::HeaderAdvanced),
        "test",
    );

    let (_handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);

    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(5)).await;
    assert_eq!(
        exchange.current_frontier().frontier.best_header,
        test_frontier(5)
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_exchange_progress_retries_after_empty_needed_blocks() {
    let initial = test_frontier_update(0, 0, 0, FrontierChange::Snapshot);
    let (exchange, startup) =
        exchange_block_sync_startup(initial, immediate_body_download_config());
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);

    exchange.publish_frontier(
        test_frontier_update(0, 0, 3, FrontierChange::HeaderAdvanced),
        "test",
    );
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(3)).await;

    handle
        .send(BlockSyncEvent::NeededBlocks(Vec::new()))
        .await
        .expect("empty needed-blocks event queues");

    exchange.publish_frontier(
        test_frontier_update(0, 0, 4, FrontierChange::HeaderAdvanced),
        "test",
    );
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(4)).await;

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_exchange_body_progress_retries_after_header_tip_stops() {
    let initial = test_frontier_update(0, 0, 0, FrontierChange::Snapshot);
    let (exchange, startup) =
        exchange_block_sync_startup(initial, immediate_body_download_config());
    let (_handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);

    exchange.publish_frontier(
        test_frontier_update(0, 0, 3, FrontierChange::HeaderAdvanced),
        "test",
    );
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(3)).await;

    exchange.publish_frontier(
        test_frontier_update(0, 1, 0, FrontierChange::VerifiedGrow),
        "test",
    );
    wait_for_query_needed_blocks(&mut actions, block::Height(1), block::Height(3)).await;

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_exchange_coalesced_header_advance_catches_body_frontier_up() {
    let initial = test_frontier_update(0, 0, 0, FrontierChange::Snapshot);
    let (exchange, startup) =
        exchange_block_sync_startup(initial, immediate_body_download_config());

    exchange.publish_frontier(
        test_frontier_update(0, 3, 0, FrontierChange::VerifiedGrow),
        "test",
    );
    exchange.publish_frontier(
        test_frontier_update(0, 0, 3, FrontierChange::HeaderAdvanced),
        "test",
    );

    let (handle, _actions, reactor_task) = spawn_block_sync_reactor(startup);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let status = handle.local_status();
            if status.servable_high == block::Height(3) {
                assert_eq!(status.tip_hash, test_frontier(3).hash);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("coalesced header update catches the body frontier up");

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_exchange_ignores_stale_grow_but_accepts_reset() {
    let initial = test_frontier_update(0, 5, 10, FrontierChange::Snapshot);
    let (exchange, startup) =
        exchange_block_sync_startup(initial, immediate_body_download_config());
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);

    wait_for_query_needed_blocks(&mut actions, block::Height(5), block::Height(10)).await;

    exchange.publish_frontier(
        test_frontier_update(0, 4, 10, FrontierChange::VerifiedGrow),
        "test",
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), actions.recv())
            .await
            .is_err(),
        "stale lower VerifiedGrow must not trigger a lower body query"
    );
    assert_eq!(handle.local_status().servable_high, block::Height(5));

    exchange.publish_frontier(
        test_frontier_update(0, 4, 0, FrontierChange::VerifiedReset),
        "test",
    );
    wait_for_query_needed_blocks(&mut actions, block::Height(4), block::Height(10)).await;
    assert_eq!(handle.local_status().servable_high, block::Height(4));

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_preserves_successor_work_across_stale_finalized_reset() {
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    config.peer_limits.outbound_queue_depth = 16;
    config.request_timeout = Duration::from_secs(300);

    let (_tip_tx, tip_rx) = watch::channel((block::Height(3), blocks[2].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(2),
            verified_block_tip: block::Height(2),
            verified_block_hash: blocks[1].hash(),
        },
        (block::Height(3), blocks[2].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        72,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[2])]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(3), 1)
    );

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[2].clone())
                .encode_frame()
                .expect("block frame encodes"),
        )
        .await
        .expect("block frame queues");

    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::SubmitBlock { .. }
    ) {}

    handle
        .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
            finalized_height: block::Height(2),
            verified_block_tip: block::Height(2),
            verified_block_hash: blocks[1].hash(),
        }))
        .await
        .expect("stale finalized reset queues");
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[2])]))
        .await
        .expect("duplicate needed metadata queues");

    // A re-request of the already-submitted successor would land on this peer's
    // own outbound, so watch the real wire for any GetBlocks of height 3.
    loop {
        tokio::select! {
            biased;
            frame = outbound_rx.recv() => {
                let frame = frame.expect("outbound channel is live");
                if let BlockSyncMessage::GetBlocks {
                    start_height: block::Height(3),
                    ..
                } = BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes")
                {
                    panic!("stale finalized reset made an already-submitted successor requestable again");
                }
            }
            action = tokio::time::timeout(Duration::from_millis(100), actions.recv()) => {
                match action {
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_exchange_reanchor_lowers_only_best_header_target() {
    let initial = test_frontier_update(0, 5, 10, FrontierChange::Snapshot);
    let (exchange, startup) =
        exchange_block_sync_startup(initial, immediate_body_download_config());
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);

    wait_for_query_needed_blocks(&mut actions, block::Height(5), block::Height(10)).await;

    exchange.publish_frontier(
        test_frontier_update(0, 1, 7, FrontierChange::HeaderReanchored),
        "test",
    );
    wait_for_query_needed_blocks(&mut actions, block::Height(5), block::Height(7)).await;
    assert_eq!(handle.local_status().servable_high, block::Height(5));

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_exchange_reanchor_releases_stale_submitted_bodies() {
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    // Worst-case reservation: budget for exactly the three in-flight bodies.
    config.max_inflight_block_bytes =
        BS_PER_BLOCK_WORST_CASE_BYTES * u64::try_from(blocks.len()).expect("block count fits u64");
    config.request_timeout = Duration::from_secs(300);

    let initial = test_frontier_update(0, 0, 3, FrontierChange::Snapshot);
    let (exchange, startup) = exchange_block_sync_startup(initial, config.clone());
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        66,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks.iter().map(block_meta).collect(),
        ))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 3)
    );

    for block in &blocks {
        inbound_tx
            .send(
                BlockSyncMessage::Block(block.clone())
                    .encode_frame()
                    .expect("block frame encodes"),
            )
            .await
            .expect("block frame queues");
    }

    let mut submitted = Vec::new();
    while submitted.len() < blocks.len() {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => submitted.push(
                block
                    .coinbase_height()
                    .expect("submitted test block has height"),
            ),
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before all submitted bodies: {action:?}"),
        }
    }
    assert_eq!(
        submitted,
        vec![block::Height(1), block::Height(2), block::Height(3)]
    );

    exchange.publish_frontier(
        test_frontier_update(0, 0, 1, FrontierChange::HeaderReanchored),
        "test",
    );
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(1)).await;

    exchange.publish_frontier(
        test_frontier_update(0, 0, 3, FrontierChange::HeaderAdvanced),
        "test",
    );
    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(3)).await;

    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks.iter().map(block_meta).collect(),
        ))
        .await
        .expect("needed metadata after reanchor queues");
    let (got_start, got_count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(got_start, block::Height(1));
    assert_eq!(
        got_count, 3,
        "reanchored headers must release old submitted bodies and request them again",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_clamps_tiny_submitted_apply_config_above_checkpoint_range() {
    let blocks = fake_sequential_blocks(4);
    let mut config = immediate_body_download_config();
    config.max_inflight_block_bytes = u64::MAX;
    config.max_submitted_block_applies = 2;
    config.request_timeout = Duration::from_secs(300);

    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(4), blocks[3].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    wait_for_query_needed_blocks(&mut actions, block::Height(0), block::Height(4)).await;
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        67,
        block::Height(4),
        blocks[3].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks.iter().map(block_meta).collect(),
        ))
        .await
        .expect("needed metadata queues");
    let (_start, count) = wait_for_outbound_getblocks(&mut outbound_rx).await;
    assert_eq!(count, 4);

    for block in &blocks {
        inbound_tx
            .send(
                BlockSyncMessage::Block(block.clone())
                    .encode_frame()
                    .expect("block frame encodes"),
            )
            .await
            .expect("block frame queues");
    }

    let mut submitted = Vec::new();
    while submitted.len() < 4 {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { block, .. } => submitted.push(
                block
                    .coinbase_height()
                    .expect("submitted test block has height"),
            ),
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action while collecting submissions: {action:?}"),
        }
    }
    assert_eq!(
        submitted,
        vec![
            block::Height(1),
            block::Height(2),
            block::Height(3),
            block::Height(4),
        ],
        "tiny configured submit caps are raised to the checkpoint-safe floor"
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_ignores_stale_non_reset_frontier_updates() {
    let (_tip_tx, tip_rx) = watch::channel((block::Height(3600), block::Hash([36; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(3200),
            verified_block_tip: block::Height(3200),
            verified_block_hash: block::Hash([32; 32]),
        },
        (block::Height(3600), block::Hash([36; 32])),
        tip_rx,
        immediate_body_download_config(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);

    assert!(matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks {
            verified_block_tip: block::Height(3200),
            best_header_tip: block::Height(3600),
        }
    ));

    handle
        .send(BlockSyncEvent::ChainTipGrow(BlockSyncFrontiers {
            finalized_height: block::Height(3200),
            verified_block_tip: block::Height(2913),
            verified_block_hash: block::Hash([29; 32]),
        }))
        .await
        .expect("stale grow event queues");

    assert!(
        tokio::time::timeout(Duration::from_millis(50), actions.recv())
            .await
            .is_err(),
        "stale lower grow frontier must not query from the lower height"
    );
    assert_eq!(handle.local_status().servable_high, block::Height(3200));

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_retries_matched_range_unavailable_without_scoring_peer() {
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    config.peer_limits.outbound_queue_depth = 16;
    let (_tip_tx, tip_rx) = watch::channel((block::Height(2), blocks[1].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(2), blocks[1].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        63,
        block::Height(2),
        blocks[1].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[0]),
            block_meta(&blocks[1]),
        ]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 2)
    );

    inbound_tx
        .send(
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(1),
                count: 2,
            }
            .encode_frame()
            .expect("RangeUnavailable frame encodes"),
        )
        .await
        .expect("RangeUnavailable frame queues");

    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 2),
        "matched RangeUnavailable should retry the original range without scoring the serving peer",
    );
    assert_eq!(handle.peer_snapshot().outbound_peers, 1);

    inbound_tx
        .send(
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(2),
                count: 1,
            }
            .encode_frame()
            .expect("RangeUnavailable frame encodes"),
        )
        .await
        .expect("unmatched RangeUnavailable frame queues");

    // routines re-query on a low-water ping, so a benign `QueryNeededBlocks`
    // may appear; the unmatched RangeUnavailable must NOT score the peer or trigger
    // a fresh GetBlocks for the already-in-flight range. A fresh request would land
    // on this peer's own outbound, so watch the real wire while draining advisory
    // queries / misbehavior off the action channel.
    loop {
        tokio::select! {
            biased;
            frame = outbound_rx.recv() => {
                let frame = frame.expect("outbound channel is live");
                match BlockSyncMessage::decode_frame(frame).expect("outbound frame decodes") {
                    BlockSyncMessage::GetBlocks { .. } => {
                        panic!("unmatched RangeUnavailable must not trigger a fresh request")
                    }
                    BlockSyncMessage::Status(_) => {}
                    msg => panic!("unexpected outbound message after unmatched RangeUnavailable: {msg:?}"),
                }
            }
            action = tokio::time::timeout(Duration::from_millis(50), actions.recv()) => {
                match action {
                    Ok(Some(BlockSyncAction::Misbehavior { .. })) => {
                        panic!("unmatched RangeUnavailable must not score the serving peer")
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }
    assert_eq!(handle.peer_snapshot().outbound_peers, 1);

    reactor_task.abort();
}

/// Regression guard for F-88604: two misbehaving peers that sort ahead of an honest
/// peer and spam `RangeUnavailable` for a contested range do **not** wedge body sync
/// — the honest peer is still offered the range and makes progress.
///
/// The audit flagged the unpenalized retry path as a possible wedge (two peers
/// re-occupying the whole fanout). Verified here that it is not: `handle_range_unavailable`
/// reschedules immediately after each response, and with one in-flight request per
/// peer the other misbehaving peer is still busy holding its stale request when the
/// first frees its slot, so the honest peer claims the freed companion slot. The
/// behavior is bounded churn, not a liveness wedge, so no peer-scoring guard is added.
/// This test fails if a future change ever lets the fanout peers lock the honest peer
/// out.
#[tokio::test]
async fn reactor_does_not_wedge_honest_peer_under_range_unavailable_spam() {
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    config.fanout = 2;
    // A long request timeout ensures the timeout-driven retry self-heal cannot mask
    // the wedge within the test window.
    config.request_timeout = Duration::from_secs(300);
    let (_tip_tx, tip_rx) = watch::channel((block::Height(2), blocks[1].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(2), blocks[1].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // Two misbehaving peers (ids 0x01, 0x02 sort first) and one honest peer (0x03).
    let (m1, m1_in, mut m1_out) = connect_peer_with_status(
        &service,
        &mut actions,
        0x01,
        block::Height(2),
        blocks[1].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    let (m2, m2_in, mut m2_out) = connect_peer_with_status(
        &service,
        &mut actions,
        0x02,
        block::Height(2),
        blocks[1].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    let (h, _h_in, mut h_out) = connect_peer_with_status(
        &service,
        &mut actions,
        0x03,
        block::Height(2),
        blocks[1].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[0]),
            block_meta(&blocks[1]),
        ]))
        .await
        .expect("needed metadata queues");

    // Every time a misbehaving peer is offered the range it answers RangeUnavailable.
    // The honest peer must eventually be offered the range. Which peer the routine
    // races to offer is nondeterministic, so observe every peer's real outbound and
    // attribute each `GetBlocks` to the peer whose stream it arrived on.
    let range_unavailable = || {
        BlockSyncMessage::RangeUnavailable {
            start_height: block::Height(1),
            count: 2,
        }
        .encode_frame()
        .expect("RangeUnavailable frame encodes")
    };
    let mut outbound_by_peer: Vec<(ZakuraPeerId, &mut FramedRecv)> = vec![
        (m1.clone(), &mut m1_out),
        (m2.clone(), &mut m2_out),
        (h.clone(), &mut h_out),
    ];
    let mut honest_offered = false;
    for _ in 0..16 {
        let (peer, _start, _count) = wait_for_getblocks_across(&mut outbound_by_peer).await;
        if peer == h {
            honest_offered = true;
            break;
        } else if peer == m1 {
            m1_in
                .send(range_unavailable())
                .await
                .expect("m1 RangeUnavailable queues");
        } else if peer == m2 {
            m2_in
                .send(range_unavailable())
                .await
                .expect("m2 RangeUnavailable queues");
        }
    }

    assert!(
        honest_offered,
        "honest peer must be offered the contested range after both fanout peers fail it"
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_range_unavailable_retries_only_unverified_suffix() {
    let blocks = mainnet_blocks_1_to_3();
    let mut config = immediate_body_download_config();
    config.peer_limits.outbound_queue_depth = 16;
    let (_tip_tx, tip_rx) = watch::channel((block::Height(2), blocks[1].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(2), blocks[1].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        64,
        block::Height(2),
        blocks[1].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[0]),
            block_meta(&blocks[1]),
        ]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(1), 2)
    );

    handle
        .send(BlockSyncEvent::ChainTipGrow(BlockSyncFrontiers {
            finalized_height: block::Height(1),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        }))
        .await
        .expect("frontier grow queues");

    inbound_tx
        .send(
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(1),
                count: 2,
            }
            .encode_frame()
            .expect("RangeUnavailable frame encodes"),
        )
        .await
        .expect("RangeUnavailable frame queues");

    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 1),
        "late RangeUnavailable must not retry the already verified prefix",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_backpressures_serving_slots_without_scoring_peer() {
    let mut config = ZakuraBlockSyncConfig {
        max_inflight_requests: 1,
        ..ZakuraBlockSyncConfig::default()
    };
    config.peer_limits.outbound_queue_depth = 16;
    let blocks = mainnet_blocks_1_to_3();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(2), blocks[1].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(2), blocks[1].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        63,
        block::Height(1),
        blocks[0].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    for _ in 0..2 {
        inbound_tx
            .send(
                BlockSyncMessage::GetBlocks {
                    start_height: block::Height(1),
                    count: 1,
                }
                .encode_frame()
                .expect("GetBlocks frame encodes"),
            )
            .await
            .expect("GetBlocks frame queues");
    }
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryBlocksByHeightRange { .. }
    ) {}

    assert_eq!(
        wait_for_outbound_range_unavailable(&mut outbound_rx).await,
        (block::Height(1), 1),
        "serving-slot saturation should backpressure the requester, not score it as spam",
    );
    assert_eq!(handle.peer_snapshot().outbound_peers, 1);

    handle
        .send(BlockSyncEvent::BlockRangeResponseFinished {
            peer: peer_id.clone(),
            start_height: block::Height(1),
            requested_count: 1,
            returned_count: 1,
        })
        .await
        .expect("serving slot release queues");

    reactor_task.abort();
}

/// A full per-peer serving queue must drop the serving send, never disconnect
/// the peer.
///
/// The reactor serves `Status`/`Block`/`BlocksDone`/`RangeUnavailable` with
/// non-blocking `try_send_*`. On `Full` it drops the frame and bumps a
/// `*.serve_queue_full` metric; only a genuine send *error* cancels the peer.
/// A regression that collapsed those two arms (cancelling on `Full`) would
/// disconnect honest-but-slow peers under serving load — a self-inflicted DoS.
/// This guards the distinction: a saturated serving queue leaves the peer
/// connected, and serving resumes once the queue drains.
#[tokio::test]
async fn reactor_full_serving_queue_drops_without_disconnecting_peer() {
    let blocks = mainnet_blocks_1_to_3();
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 16,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
        ..ZakuraBlockSyncConfig::default()
    };
    // Tip == verified tip: the reactor is caught up, so it only *serves* and
    // never issues its own downloads into the tiny outbound queue below.
    let (_tip_tx, tip_rx) = watch::channel((block::Height(3), blocks[2].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(3),
            verified_block_hash: blocks[2].hash(),
        },
        (block::Height(3), blocks[2].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // A two-slot outbound queue, wired by hand so we keep the peer's
    // cancellation token: serving three bodies plus a terminator is four sends
    // through two slots, so `try_send` is guaranteed to hit `Full`.
    let peer_id = peer(66);
    let cancel = CancellationToken::new();
    let (inbound_tx, inbound_rx) = framed_channel(16);
    let (outbound_tx, mut outbound_rx) = framed_channel(2);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        cancel.clone(),
    ));
    wait_for_outbound_status(&mut outbound_rx).await;
    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(3),
                tip_hash: blocks[2].hash(),
                max_blocks_per_response: 16,
                max_inflight_requests: 8,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status frame queues");

    // The peer asks for the whole committed range; answer it so the reactor
    // serves all three bodies into the saturated queue.
    inbound_tx
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(1),
                count: 3,
            }
            .encode_frame()
            .expect("GetBlocks frame encodes"),
        )
        .await
        .expect("GetBlocks frame queues");
    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                assert_eq!(peer, peer_id);
                assert_eq!(start, block::Height(1));
                assert_eq!(count, 3);
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before block range query: {action:?}"),
        }
    }
    let served: Vec<_> = blocks
        .iter()
        .map(|block| {
            (
                block.coinbase_height().expect("test block has height"),
                block.clone(),
                usize::try_from(block_size(block)).expect("block size fits usize"),
            )
        })
        .collect();
    handle
        .send(BlockSyncEvent::BlockRangeResponseReady {
            peer: peer_id.clone(),
            start_height: block::Height(1),
            requested_count: 3,
            blocks: served,
        })
        .await
        .expect("served block response queues");

    // Let the reactor finish the serve, including the sends that hit `Full`.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !cancel.is_cancelled(),
        "a full serving queue must drop sends, not cancel the peer",
    );
    assert_eq!(handle.peer_snapshot().outbound_peers, 1);

    // Self-heal: drain the queue, release the serving slot, and re-request. The
    // earlier drop neither wedged nor scored the peer, so a fresh serve lands.
    while tokio::time::timeout(
        Duration::from_millis(100),
        next_outbound_message(&mut outbound_rx),
    )
    .await
    .is_ok()
    {}
    handle
        .send(BlockSyncEvent::BlockRangeResponseFinished {
            peer: peer_id.clone(),
            start_height: block::Height(1),
            requested_count: 3,
            returned_count: 3,
        })
        .await
        .expect("serving slot release queues");
    inbound_tx
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(1),
                count: 1,
            }
            .encode_frame()
            .expect("GetBlocks frame encodes"),
        )
        .await
        .expect("re-request GetBlocks frame queues");
    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::QueryBlocksByHeightRange { start, count, .. } => {
                assert_eq!(start, block::Height(1));
                assert_eq!(count, 1);
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before re-request query: {action:?}"),
        }
    }
    handle
        .send(BlockSyncEvent::BlockRangeResponseReady {
            peer: peer_id.clone(),
            start_height: block::Height(1),
            requested_count: 1,
            blocks: vec![(
                block::Height(1),
                blocks[0].clone(),
                usize::try_from(block_size(&blocks[0])).expect("block size fits usize"),
            )],
        })
        .await
        .expect("re-served block response queues");
    assert_eq!(
        wait_for_outbound_block(&mut outbound_rx).await.hash(),
        blocks[0].hash(),
        "serving resumes once the queue drains",
    );
    assert!(!cancel.is_cancelled());

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_publishes_block_sync_candidate_gap() {
    let config = immediate_body_download_config();
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer_id = peer(77);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer_id.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));
    wait_for_outbound_status(&mut outbound_rx).await;

    tip_tx
        .send((block::Height(2), block::Hash([2; 32])))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            BlockSyncBlockMeta {
                height: block::Height(2),
                hash: block::Hash([2; 32]),
                size: BlockSizeEstimate::Unknown,
            },
            BlockSyncBlockMeta {
                height: block::Height(1),
                hash: block::Hash([1; 32]),
                size: BlockSizeEstimate::Unknown,
            },
        ]))
        .await
        .expect("needed blocks event queues");

    let mut candidates = handle.subscribe_candidate_state();
    let observed = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            candidates
                .changed()
                .await
                .expect("candidate watch remains open");
            let state = candidates.borrow().clone();
            if state.missing_block_bodies == vec![block::Height(1), block::Height(2)] {
                return state;
            }
        }
    })
    .await
    .unwrap_or_else(|_| handle.candidate_state());
    assert_eq!(
        observed.missing_block_bodies,
        vec![block::Height(1), block::Height(2)]
    );
    assert!(
        observed.admitted_node_ids.is_empty(),
        "a peer without block-sync status must not satisfy body-sync demand"
    );

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(2),
                tip_hash: block::Hash([2; 32]),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status queues");

    let peer_node_id =
        node_id_from_block_peer_id(&peer_id).expect("test peer id is a valid node id");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            candidates
                .changed()
                .await
                .expect("candidate watch remains open");
            if candidates.borrow().admitted_node_ids == vec![peer_node_id] {
                return;
            }
        }
    })
    .await
    .expect("status-bearing peer is published as an admitted candidate");

    handle
        .send(BlockSyncEvent::StateFrontiersChanged(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(2),
            verified_block_hash: block::Hash([2; 32]),
        }))
        .await
        .expect("frontier event queues");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if handle.candidate_state().missing_block_bodies.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("candidate state clears after the gap is gone");

    reactor_task.abort();
}

#[tokio::test]
async fn oversize_body_policy_reports_size_mismatch_and_retries_without_buffering() {
    let mut config = ZakuraBlockSyncConfig {
        size_deviation_tolerance: 100,
        ..immediate_body_download_config()
    };
    config.peer_limits.outbound_queue_depth = 8;
    let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let peer = peer(42);
    let (inbound_tx, inbound_rx) = framed_channel(8);
    let (outbound_tx, mut outbound_rx) = framed_channel(8);
    let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
    service.add_peer(Peer::new_with_direction(
        peer,
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        streams,
        CancellationToken::new(),
    ));

    inbound_tx
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: block::Hash([1; 32]),
                max_blocks_per_response: 4,
                max_inflight_requests: 1,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .expect("status encodes"),
        )
        .await
        .expect("status queues");
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    tip_tx
        .send((block::Height(1), block.hash()))
        .expect("tip watch is live");
    while !matches!(
        next_action(&mut actions).await,
        BlockSyncAction::QueryNeededBlocks { .. }
    ) {}
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![BlockSyncBlockMeta {
            height: block::Height(1),
            hash: block.hash(),
            size: BlockSizeEstimate::Advertised(1),
        }]))
        .await
        .expect("needed metadata queues");
    // The node's GetBlocks lands on this peer's real outbound.
    while !matches!(
        BlockSyncMessage::decode_frame(
            tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
                .await
                .expect("outbound frame arrives")
                .expect("outbound channel is live")
        )
        .expect("frame decodes"),
        BlockSyncMessage::GetBlocks { .. }
    ) {}

    inbound_tx
        .send(
            BlockSyncMessage::Block(block.clone())
                .encode_frame()
                .expect("block encodes"),
        )
        .await
        .expect("block queues");

    loop {
        match next_action(&mut actions).await {
            BlockSyncAction::Misbehavior { reason, .. } => {
                assert_eq!(reason, BlockSyncMisbehavior::SizeMismatch);
                break;
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action during size mismatch test: {action:?}"),
        }
    }

    let no_submit = tokio::time::timeout(Duration::from_millis(200), async {
        while let Some(action) = actions.recv().await {
            if matches!(action, BlockSyncAction::SubmitBlock { .. }) {
                return false;
            }
        }
        true
    })
    .await
    .unwrap_or(true);
    assert!(
        no_submit,
        "oversize body is not submitted after SizeMismatch"
    );

    reactor_task.abort();
}

// SECURITY AUDIT (candidate claude-block-sync-unsolicited-blocksdone-not-rejected /
// codex-blocksync-unsolicited-blocksdone-not-rejected): SR-6/SR-7 response
// correlation + fail-closed.
//
// `handle_blocks_done` reports `UnsolicitedDone` only when the peer is *unknown*.
// For a known, active peer that sends a valid `BlocksDone` with no matching
// outstanding request, the `if let Some(index)` body is skipped and the reactor
// falls through to `schedule()` with no `else` reporting `UnsolicitedDone`.
// `UnsolicitedDone` is a *hard* block-sync misbehavior (`block_sync_misbehavior_is_hard`
// in zebrad start.rs), so the production driver `drive_block_sync_actions`
// disconnects on the first offense -- but this branch never emits it, so an
// admitted peer can stream uncorrelated response terminators forever and stay
// connected.
//
// This test asserts the SAFE behavior (the reactor must report `UnsolicitedDone`).
// It currently FAILS, which is the reproduction. Do not weaken it to pass; the
// fix is to add the missing `else` branch in `handle_blocks_done`.
#[tokio::test]
async fn reactor_known_peer_unsolicited_blocks_done_is_reported_as_misbehavior() {
    let config = ZakuraBlockSyncConfig::default();
    let blocks = mainnet_blocks_1_to_3();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(1), blocks[0].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(1), blocks[0].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // Connect a peer that advertises no downloadable work (servable_high == our
    // verified tip), so the reactor never schedules a GetBlocks and the peer has
    // zero outstanding requests. The peer is known/active (received_status=true).
    let (peer_id, inbound_tx, _outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        63,
        block::Height(1),
        blocks[0].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    // Surprising/hostile input: a valid `BlocksDone` terminator with a start
    // height that matches no outstanding request (there are none).
    inbound_tx
        .send(
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(7),
                returned: 1,
            }
            .encode_frame()
            .expect("BlocksDone frame encodes"),
        )
        .await
        .expect("BlocksDone frame queues");

    // Expected safe behavior: the reactor reports `UnsolicitedDone` for this peer
    // (which the production driver maps to a hard disconnect). Collect actions for
    // a bounded window and assert it appears.
    let mut saw_unsolicited_done = false;
    while let Ok(Some(action)) = tokio::time::timeout(Duration::from_secs(1), actions.recv()).await
    {
        if let BlockSyncAction::Misbehavior { peer, reason } = action {
            if peer == peer_id && reason == BlockSyncMisbehavior::UnsolicitedDone {
                saw_unsolicited_done = true;
                break;
            }
        }
    }

    assert!(
        saw_unsolicited_done,
        "a known peer's unsolicited BlocksDone with no matching outstanding request \
         must be reported as Misbehavior::UnsolicitedDone (SR-6/SR-7), but the reactor \
         silently tolerated it and kept the peer connected",
    );

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_ignores_unmatched_response_for_height_active_on_another_request() {
    let config = immediate_body_download_config();
    let blocks = mainnet_blocks_1_to_3();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(2), blocks[1].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(2), blocks[1].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    let (peer1, inbound1, mut outbound1) = connect_peer_with_status(
        &service,
        &mut actions,
        65,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    let (peer2, inbound2, mut outbound2) = connect_peer_with_status(
        &service,
        &mut actions,
        66,
        block::Height(3),
        blocks[2].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[1])]))
        .await
        .expect("needed metadata queues");
    // Which peer the routine offers the request to is nondeterministic, so observe
    // both peers' real outbounds and learn which one was asked.
    let (requested_peer, start_height, _count) = {
        let mut outbound_by_peer: Vec<(ZakuraPeerId, &mut FramedRecv)> = vec![
            (peer1.clone(), &mut outbound1),
            (peer2.clone(), &mut outbound2),
        ];
        wait_for_getblocks_across(&mut outbound_by_peer).await
    };
    assert_eq!(start_height, block::Height(2));

    // Simulate a later state snapshot that omits the active in-flight height.
    // The scheduler retains the outstanding request for correlation, but
    // `needed_heights` no longer contains the height, matching the production
    // late-response race after retries/reconnects.
    handle
        .send(BlockSyncEvent::NeededBlocks(Vec::new()))
        .await
        .expect("empty needed metadata queues");

    // The peer that did NOT get the request sends the body+terminator as real
    // inbound frames; its routine must drop them (another peer holds the active
    // request) without scoring misbehavior.
    let (late_peer, late_inbound) = if requested_peer == peer1 {
        (peer2, inbound2)
    } else {
        (peer1, inbound1)
    };
    send_inbound(&late_inbound, BlockSyncMessage::Block(blocks[1].clone())).await;
    send_inbound(
        &late_inbound,
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(2),
            returned: 1,
        },
    )
    .await;

    while let Ok(Some(action)) =
        tokio::time::timeout(Duration::from_millis(200), actions.recv()).await
    {
        if let BlockSyncAction::Misbehavior { peer, reason } = action {
            assert_ne!(
                peer, late_peer,
                "late response for an active request was reported as {reason:?}"
            );
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_ignores_duplicate_response_at_body_download_floor() {
    let config = immediate_body_download_config();
    let blocks = mainnet_blocks_1_to_3();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(2), blocks[1].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(2), blocks[1].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (peer_id, inbound_tx, mut outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        65,
        block::Height(2),
        blocks[1].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[1])]))
        .await
        .expect("needed metadata queues");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound_rx).await,
        (block::Height(2), 1)
    );
    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[1].clone())
                .encode_frame()
                .expect("block frame encodes"),
        )
        .await
        .expect("block frame queues");

    let (token, hash) = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => break (token, block.hash()),
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action while waiting for submit: {action:?}"),
        }
    };

    // Simulate the verifier reporting success before the frontier mirror has
    // delivered the matching verified-tip update. The applying entry is gone,
    // but `body_download_floor` still proves this height was already accepted.
    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token,
            height: block::Height(2),
            hash,
            result: BlockApplyResult::Committed,
            local_frontier: None,
        })
        .await
        .expect("apply result queues");

    inbound_tx
        .send(
            BlockSyncMessage::Block(blocks[1].clone())
                .encode_frame()
                .expect("duplicate block frame encodes"),
        )
        .await
        .expect("duplicate block frame queues");
    inbound_tx
        .send(
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(2),
                returned: 1,
            }
            .encode_frame()
            .expect("duplicate terminator frame encodes"),
        )
        .await
        .expect("duplicate terminator frame queues");

    while let Ok(Some(action)) =
        tokio::time::timeout(Duration::from_millis(200), actions.recv()).await
    {
        if let BlockSyncAction::Misbehavior { peer, reason } = action {
            assert_ne!(
                peer, peer_id,
                "duplicate response at body_download_floor was reported as {reason:?}"
            );
        }
    }

    reactor_task.abort();
}

#[tokio::test]
async fn reactor_ignores_matched_duplicate_response_at_body_download_floor() {
    // fanout = 1: a height is requested from exactly one peer. The duplicate body
    // that must be ignored at the floor instead arrives unsolicited from the
    // *other* connected peer after the height has committed.
    let blocks = mainnet_blocks_1_to_3();
    let block2_size = block_size(&blocks[1]);
    let mut config = immediate_body_download_config();
    config.max_inflight_block_bytes = BS_PER_BLOCK_WORST_CASE_BYTES * 2;

    let (_tip_tx, tip_rx) = watch::channel((block::Height(4), block::Hash([4; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(4), block::Hash([4; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (peer_a, inbound_a, mut outbound_a) = connect_peer_with_status(
        &service,
        &mut actions,
        66,
        block::Height(4),
        block::Hash([4; 32]),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    let (peer_b, inbound_b, mut outbound_b) = connect_peer_with_status(
        &service,
        &mut actions,
        67,
        block::Height(4),
        block::Hash([4; 32]),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![block_meta(&blocks[1])]))
        .await
        .expect("needed metadata queues");
    // Which peer the routine offers the request to is nondeterministic (the routines
    // race for the shared work), so observe both peers' real outbounds and learn
    // which one was asked.
    let mut outbound_by_peer: Vec<(ZakuraPeerId, &mut FramedRecv)> = vec![
        (peer_a.clone(), &mut outbound_a),
        (peer_b.clone(), &mut outbound_b),
    ];
    let first_request = wait_for_getblocks_across(&mut outbound_by_peer).await;
    assert_eq!((first_request.1, first_request.2), (block::Height(2), 1));
    // The other peer is the one that will deliver the ignored duplicate.
    let (other_peer, requested_inbound, other_inbound) = if first_request.0 == peer_a {
        (peer_b.clone(), &inbound_a, &inbound_b)
    } else {
        (peer_a.clone(), &inbound_b, &inbound_a)
    };

    send_inbound(
        requested_inbound,
        BlockSyncMessage::Block(blocks[1].clone()),
    )
    .await;

    let (token, hash) = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::SubmitBlock { token, block } => break (token, block.hash()),
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action while waiting for submit: {action:?}"),
        }
    };
    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            token,
            height: block::Height(2),
            hash,
            result: BlockApplyResult::Committed,
            local_frontier: None,
        })
        .await
        .expect("apply result queues");

    // A late duplicate body for the now-committed height arrives from the other
    // peer as a real inbound frame; it sits at/below the body-download floor and
    // must be ignored without permanently consuming reorder budget.
    let _ = &other_peer;
    send_inbound(other_inbound, BlockSyncMessage::Block(blocks[1].clone())).await;

    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[2]),
            BlockSyncBlockMeta {
                height: block::Height(4),
                hash: block::Hash([4; 32]),
                size: BlockSizeEstimate::Advertised(block2_size),
            },
        ]))
        .await
        .expect("next needed metadata queues");

    // The commit pipeline now runs on its own task and reports the committed
    // floor back asynchronously, so the late duplicate can momentarily reach the
    // unmatched-queued path and transiently reserve before the Sequencer reports
    // it `Redundant` and releases. The invariant that still must hold is that the
    // duplicate consumes no budget *permanently*: both remaining heights (3 and 4)
    // must still get requested, each exactly once, within the two-block budget.
    // Collect GetBlocks until 3 and 4 are both covered and assert no double-fetch.
    let mut requested: Vec<block::Height> = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (peer, start, count) = wait_for_getblocks_across(&mut outbound_by_peer).await;
            assert!(
                peer == peer_a || peer == peer_b,
                "request should target one of the connected peers"
            );
            for offset in 0..count {
                if let Some(height) = height_after_count(start, offset) {
                    requested.push(height);
                }
            }
            if requested.contains(&block::Height(3)) && requested.contains(&block::Height(4)) {
                break;
            }
        }
    })
    .await
    .expect("both remaining heights are requested within the unchanged budget");
    requested.sort_unstable();
    let mut deduped = requested.clone();
    deduped.dedup();
    assert_eq!(
        requested, deduped,
        "a matched duplicate response at the body floor must not consume reorder budget \
         (no height should be fetched twice)"
    );
    assert!(
        requested.contains(&block::Height(3)) && requested.contains(&block::Height(4)),
        "both needed heights must be fetched once the duplicate releases its transient reservation"
    );

    reactor_task.abort();
}

// Per-peer routines removed the reactor "late response after disconnect"
// path that the original first half of this test exercised: a disconnected peer's
// `FramedRecv` is closed and its routine has exited, so there is no transport for a
// late frame and no reactor inbound demux to ignore one. The surviving, still-
// meaningful guarantee — a connected peer's routine hard-scores an unsolicited
// `BlocksDone` correlating to no outstanding request — is kept and exercised over a
// real inbound frame.
#[tokio::test]
async fn reactor_scores_unsolicited_terminator_from_connected_peer() {
    let config = ZakuraBlockSyncConfig::default();
    let blocks = mainnet_blocks_1_to_3();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(1), blocks[0].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(1),
            verified_block_hash: blocks[0].hash(),
        },
        (block::Height(1), blocks[0].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config.clone(), handle.clone());

    let (peer_id, inbound_tx, _outbound_rx) = connect_peer_with_status(
        &service,
        &mut actions,
        64,
        block::Height(1),
        blocks[0].hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    send_inbound(
        &inbound_tx,
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(7),
            returned: 1,
        },
    )
    .await;

    let mut saw_unsolicited_done = false;
    while let Ok(Some(action)) = tokio::time::timeout(Duration::from_secs(1), actions.recv()).await
    {
        if let BlockSyncAction::Misbehavior { peer, reason } = action {
            if peer == peer_id && reason == BlockSyncMisbehavior::UnsolicitedDone {
                saw_unsolicited_done = true;
                break;
            }
        }
    }
    assert!(
        saw_unsolicited_done,
        "an unsolicited terminator from a connected peer is hard-scored UnsolicitedDone"
    );

    reactor_task.abort();
}

/// Misbehavior is record-only: a repeatedly-misbehaving peer is still *observed*
/// (the reactor emits a `Misbehavior` action as a record) but is **not**
/// disconnected. Peer scoring no longer drives disconnects, so what used to be a
/// "soft" offense crossing the threshold-of-3 now leaves the peer connected.
#[tokio::test]
async fn repeated_misbehavior_is_recorded_without_disconnecting_the_peer() {
    let config = ZakuraBlockSyncConfig::default();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(0), block::Hash([0; 32])),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    // Connect the probe peer with a real pipe-routine (per-peer routines) so its inbound frames
    // are decoded and dispatched.
    let probe = peer(7);
    let (probe_inbound_tx, probe_inbound_rx) = framed_channel(8);
    let (probe_outbound_tx, _probe_outbound_rx) = framed_channel(8);
    service.add_peer(Peer::new_with_direction(
        probe.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        HashMap::from([(
            ZAKURA_STREAM_BLOCK_SYNC,
            (probe_inbound_rx, probe_outbound_tx),
        )]),
        CancellationToken::new(),
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.peer_count() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("probe peer connects");

    // Each `GetBlocks` from a peer that has not sent a Status is `GetBlocksSpam`
    // (formerly a "soft" offense that disconnected at a threshold of 3). Send well
    // past the old threshold.
    for _ in 0..8 {
        send_inbound(
            &probe_inbound_tx,
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(1),
                count: 1,
            },
        )
        .await;
    }

    // The violation is still recorded: the reactor emits a best-effort
    // `Misbehavior` action for the spamming peer.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let BlockSyncAction::Misbehavior { peer, reason } = next_action(&mut actions).await {
                if peer == probe && reason == BlockSyncMisbehavior::GetBlocksSpam {
                    break;
                }
            }
        }
    })
    .await
    .expect("a misbehavior action is recorded for the spamming peer");

    // But the peer is never torn down: misbehavior no longer cancels the session.
    // Give the reactor ample time to (not) act, then confirm the peer remains.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        service.peer_count(),
        1,
        "misbehavior is record-only: a repeatedly-misbehaving peer must NOT be disconnected",
    );

    reactor_task.abort();
}
