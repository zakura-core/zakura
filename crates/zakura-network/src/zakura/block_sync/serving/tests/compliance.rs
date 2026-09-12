//! Real serving operations under bounded capacity, cancellation, and load.

use super::super::super::tests::fake_blocks_in_range;
use super::*;
use crate::zakura::testkit::await_until;
use proptest::prelude::*;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU64, Weak},
};
use zakura_test::{allocations::measure, execution::ExecutionProbe};

const DEADLINE: Duration = Duration::from_secs(5);

mod load;

#[derive(Debug)]
struct ControlledSource {
    encoded: Arc<BTreeMap<block::Height, Vec<u8>>>,
    probe: Arc<ExecutionProbe>,
    decoded: Arc<Mutex<Vec<Weak<block::Block>>>>,
    peak_decoded: Arc<AtomicU64>,
    fail: bool,
}

impl ControlledSource {
    fn new(start: u32, count: u32, large: bool, blocked: bool) -> Arc<Self> {
        let mut bodies = fake_blocks_in_range(start, start + count - 1);
        if large {
            for body in &mut bodies {
                let body = Arc::make_mut(body);
                let tx = Arc::make_mut(&mut body.transactions[0]);
                let outputs = match tx {
                    zakura_chain::transaction::Transaction::V1 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V2 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V3 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V4 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V5 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V6 { outputs, .. } => outputs,
                };
                outputs[0].lock_script =
                    zakura_chain::transparent::Script::new(&vec![0; 1_900_000]);
                Arc::make_mut(&mut body.header).merkle_root = body.transactions.iter().collect();
            }
        }
        Arc::new(Self {
            encoded: Arc::new(
                bodies
                    .into_iter()
                    .map(|body| {
                        (
                            body.coinbase_height().unwrap(),
                            body.zcash_serialize_to_vec().unwrap(),
                        )
                    })
                    .collect(),
            ),
            probe: ExecutionProbe::new(blocked, false),
            decoded: Arc::new(Mutex::new(Vec::new())),
            peak_decoded: Arc::new(AtomicU64::new(0)),
            fail: false,
        })
    }

    fn live_decoded_bytes(&self) -> u64 {
        self.decoded
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .map(|body| body.attributed_memory_size_bytes())
            .sum()
    }

    fn fixture(self: &Arc<Self>, workers: usize, depth: usize, peer: u8) -> Fixture {
        let config = config(workers);
        let regulator = GetBlocksServingRegulator::new(config.clone());
        self.shared_fixture(
            depth,
            peer,
            config,
            regulator,
            Arc::new(PeerRegistry::new()),
        )
    }

    fn shared_fixture(
        self: &Arc<Self>,
        depth: usize,
        peer: u8,
        config: ZakuraBlockSyncConfig,
        regulator: GetBlocksServingRegulator,
        registry: Arc<PeerRegistry>,
    ) -> Fixture {
        let f = Fixture::with_resources(self.clone(), depth, config, regulator, registry, peer);
        f.status.send_modify(|status| {
            status.servable_low = *self.encoded.first_key_value().unwrap().0;
            status.servable_high = *self.encoded.last_key_value().unwrap().0;
        });
        f.session.mark_status_received();
        f
    }
}

impl BlockRangeSource for ControlledSource {
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, crate::BoxError>> {
        let encoded = self.encoded.clone();
        let probe = self.probe.clone();
        let fail = self.fail;
        let decoded = self.decoded.clone();
        let peak_decoded = self.peak_decoded.clone();
        // The source stores bytes like a database. Each returned body is decoded
        // into a distinct allocation rather than cloning a cached Arc fixture.
        let job = Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let (start, count, cap, lease) = request.into_parts();
                assert!(lease.try_start());
                let operation = probe.start();
                if fail {
                    return Err(io::Error::other("controlled storage failure").into());
                }
                let (blocks, allocations) = measure(|| {
                    let mut blocks = Vec::new();
                    let mut bytes = 0usize;
                    for offset in 0..count {
                        if lease.is_cancelled() {
                            break;
                        }
                        let Some(height) = start.0.checked_add(offset).map(block::Height) else {
                            break;
                        };
                        let Some(encoded) = encoded.get(&height) else {
                            break;
                        };
                        // Decode the one lookahead body before discovering it
                        // does not fit, as a real state read is allowed to do.
                        let body =
                            Arc::new(block::Block::zcash_deserialize(encoded.as_slice()).unwrap());
                        let next = bytes + encoded.len();
                        if next > usize::try_from(cap).unwrap() {
                            break;
                        }
                        blocks.push((height, body, encoded.len()));
                        bytes = next;
                    }
                    blocks
                });
                probe.allocations(allocations);
                {
                    let mut live = decoded.lock().unwrap();
                    live.retain(|body| body.strong_count() > 0);
                    live.extend(blocks.iter().map(|(_, body, _)| Arc::downgrade(body)));
                    let bytes: u64 = live
                        .iter()
                        .filter_map(Weak::upgrade)
                        .map(|body| body.attributed_memory_size_bytes())
                        .sum();
                    peak_decoded.fetch_max(bytes, Ordering::Relaxed);
                }
                operation.finish();
                Ok(BlockRangeReadResult::new(blocks, lease))
            })
            .await?
        });
        job
    }
}

fn config(workers: usize) -> ZakuraBlockSyncConfig {
    let mut config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 128,
        max_inflight_requests: 16,
        ..ZakuraBlockSyncConfig::default()
    };
    config.get_blocks_regulation.node_active_requests = workers;
    config.peer_limits.max_inbound_peers = 8;
    config.peer_limits.max_outbound_peers = 8;
    config
}

async fn request(f: &Fixture, start: u32, count: u32) {
    time::timeout(
        DEADLINE,
        f.requests.send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(start),
                count,
            }
            .encode_frame()
            .unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
}

async fn response(
    f: &mut Fixture,
    source: &ControlledSource,
    start: u32,
    requested: u32,
    cap: u32,
    count_cap: u32,
) -> usize {
    let encoded = source.encoded.clone();
    let fail = source.fail;
    // Decoding large expected responses must not block the task polling other
    // peers' frame deadlines when these checkers run together with join_all.
    let expected = tokio::task::spawn_blocking(move || {
        let mut expected = Vec::new();
        let mut bytes = 0usize;
        for offset in 0..requested.min(count_cap) {
            let Some(encoded) = encoded.get(&block::Height(start + offset)) else {
                break;
            };
            if fail || bytes + encoded.len() > usize::try_from(cap).unwrap() {
                break;
            }
            bytes += encoded.len();
            expected.push(
                block::Block::zcash_deserialize(encoded.as_slice())
                    .unwrap()
                    .hash(),
            );
        }
        expected
    })
    .await
    .unwrap();
    for (offset, hash) in expected.iter().enumerate() {
        let BlockSyncMessage::Block(body) = f.next().await else {
            panic!("C07 missing expected prefix body");
        };
        assert_eq!(
            body.coinbase_height().unwrap().0,
            start + u32::try_from(offset).unwrap()
        );
        assert_eq!(body.hash(), *hash);
    }
    let terminal = f.next().await;
    if expected.is_empty() {
        assert_eq!(
            terminal,
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(start),
                count: requested
            }
        );
    } else {
        assert_eq!(
            terminal,
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(start),
                returned: u32::try_from(expected.len()).unwrap()
            }
        );
    }
    expected.len()
}

#[tokio::test]
async fn c01_actual_operations_stay_bounded_with_more_commitments_than_workers() {
    let source = ControlledSource::new(100, 3, false, true);
    let _release = source.probe.release_on_drop();
    let config = config(2);
    let regulator = GetBlocksServingRegulator::new(config.clone());
    let registry = Arc::new(PeerRegistry::new());
    let mut peers = Vec::new();
    for peer in 1..=8 {
        let f = source.shared_fixture(1, peer, config.clone(), regulator.clone(), registry.clone());
        request(&f, 100, 3).await;
        peers.push(f);
    }
    source.probe.wait_started(2).await;
    assert_eq!(source.probe.snapshot().running, 2);
    assert_eq!(regulator.snapshot().node_active, 2);
    assert!(peers
        .iter()
        .all(|f| !f.session.cancel_token().is_cancelled()));
    source.probe.release();
    futures::future::join_all(
        peers
            .iter_mut()
            .map(|f| response(f, &source, 100, 3, config.max_response_bytes, 128)),
    )
    .await;
    source.probe.wait_finished(8).await;
    assert_eq!(source.probe.snapshot().started, 8);
    assert!(source.probe.snapshot().peak_running <= 2);
    for f in peers {
        f.finish().await;
    }
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[tokio::test]
async fn c02_retained_output_stops_read_ahead_and_resumes_without_refill() {
    let source = ControlledSource::new(100, 6, false, false);
    let mut f = source.fixture(1, 1, 1);
    request(&f, 100, 3).await;
    source.probe.wait_finished(1).await;
    let held = time::timeout(DEADLINE, f.data.recv())
        .await
        .unwrap()
        .unwrap();
    request(&f, 103, 3).await;
    // One pending decoded request plus the one-slot input queue is the declared
    // request read-ahead. Extra queued bytes must eventually backpressure.
    assert_eq!(source.probe.snapshot().started, 1);
    let mut write = Box::pin(held.write_with(|frame| async move {
        assert_eq!(
            BlockSyncMessage::decode_frame(frame)
                .unwrap()
                .message_type(),
            3
        );
        std::future::pending::<Result<(), ()>>().await
    }));
    assert!(futures::poll!(&mut write).is_pending());
    assert_eq!(source.probe.snapshot().started, 1);
    // Finish the current response's other frames before releasing the first
    // write. The next database job must still wait for that actual owner.
    for _ in 0..3 {
        f.next().await;
    }
    assert_eq!(source.probe.snapshot().started, 1);
    tokio::task::yield_now().await;
    request(&f, 106, 3).await;
    assert!(
        time::timeout(Duration::from_millis(20), request(&f, 109, 3))
            .await
            .is_err()
    );
    drop(write);
    source.probe.wait_started(2).await;
    response(&mut f, &source, 103, 3, config(1).max_response_bytes, 128).await;
    f.finish().await;
}

async fn check_reconnects(same_identity: bool) {
    let source = ControlledSource::new(100, 3, false, true);
    let _release = source.probe.release_on_drop();
    let config = config(2);
    let regulator = GetBlocksServingRegulator::new(config.clone());
    let registry = Arc::new(PeerRegistry::new());
    for attempt in 0..8u8 {
        let peer = if same_identity { 1 } else { attempt + 1 };
        let mut f =
            source.shared_fixture(1, peer, config.clone(), regulator.clone(), registry.clone());
        request(&f, 100, 3).await;
        if attempt < if same_identity { 1 } else { 2 } {
            source.probe.wait_started(usize::from(attempt) + 1).await;
        }
        tokio::task::yield_now().await;
        f.task.abort();
        assert!(time::timeout(DEADLINE, &mut f.task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
        registry.remove_session(f.session.peer_id(), f.session.session_id());
        drop(f);
        assert!(source.probe.snapshot().running <= 2);
        assert!(regulator.snapshot().node_active <= 2);
    }
    assert_eq!(
        source.probe.snapshot().started,
        if same_identity { 1 } else { 2 }
    );
    source.probe.release();
    let started = source.probe.snapshot().started;
    source.probe.wait_finished(started).await;
    let mut replacement = source.shared_fixture(1, 1, config.clone(), regulator.clone(), registry);
    request(&replacement, 100, 3).await;
    response(
        &mut replacement,
        &source,
        100,
        3,
        config.max_response_bytes,
        128,
    )
    .await;
    replacement.finish().await;
    assert_eq!(regulator.snapshot().node_active, 0);
    assert_eq!(source.probe.snapshot().running, 0);
}

#[tokio::test]
async fn c03_same_identity_reconnects_cannot_start_another_blocked_job() {
    check_reconnects(true).await;
}

#[tokio::test]
async fn c03_distinct_identity_churn_cannot_exceed_the_node_job_limit() {
    check_reconnects(false).await;
}

async fn check_encode_cancellation(hold_finished_result: bool) {
    let config = config(1);
    let regulator = GetBlocksServingRegulator::new(config);
    let session = regulator.session(ZakuraPeerId::new(vec![1; 32]).unwrap());
    let request = session
        .decode_request(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(100),
                count: 1,
            }
            .encode_frame()
            .unwrap(),
        )
        .unwrap();
    let mut permit = session.admit_request(&request).await;
    let probe = ExecutionProbe::new(!hold_finished_result, hold_finished_result);
    let _release = probe.release_on_drop();
    permit.encode_probe = Some(probe.clone());
    let (send, recv) = worker_framed_channel(1);
    let block = fake_blocks_in_range(100, 100).pop().unwrap();
    let mut task = AbortOnDropHandle::new(tokio::spawn(async move {
        send_response(&send, &mut permit, BlockSyncMessage::Block(block)).await
    }));
    probe.wait_started(1).await;
    if hold_finished_result {
        await_until("encoder reached its retained result", DEADLINE, || {
            probe.snapshot().largest_allocation > 0
        })
        .await
        .unwrap();
    }
    task.abort();
    assert!(time::timeout(DEADLINE, &mut task)
        .await
        .unwrap()
        .unwrap_err()
        .is_cancelled());
    drop(recv);
    assert_eq!(probe.snapshot().running, 1);
    assert_eq!(
        regulator.snapshot().node_active,
        1,
        "C04 actual encode/result owner survives caller abort"
    );
    let mut next = Box::pin(session.admit_request(&request));
    assert!(futures::poll!(&mut next).is_pending());
    probe.release();
    drop(time::timeout(DEADLINE, next).await.unwrap());
    probe.wait_finished(1).await;
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[tokio::test]
async fn c04_aborted_caller_keeps_running_encoding_charged() {
    check_encode_cancellation(false).await;
}

#[tokio::test]
async fn c04_unobserved_encode_result_keeps_its_lease_until_disposal() {
    check_encode_cancellation(true).await;
}

#[tokio::test]
async fn c05_real_decode_and_encode_allocations_are_bounded_for_small_and_large_blocks() {
    for large in [false, true] {
        let source = ControlledSource::new(100, 3, large, false);
        let mut f = source.fixture(1, 1, 1);
        // This fixture declares a two-body response budget. A third decoded
        // lookahead is permitted, but no second response can run for this peer.
        let body_bytes = source.encoded.values().map(Vec::len).max().unwrap();
        let cap = u32::try_from(body_bytes * 2).unwrap();
        f.status
            .send_modify(|status| status.max_response_bytes = cap);
        request(&f, 100, 3).await;
        source.probe.wait_finished(1).await;
        assert!(
            source.live_decoded_bytes() > 0,
            "C05 observes independently decoded retained objects"
        );
        let held = time::timeout(DEADLINE, f.data.recv())
            .await
            .unwrap()
            .unwrap();
        let (frame, guard) = held.into_parts();
        assert!(
            frame.payload.capacity() <= 2 * (body_bytes + 1),
            "C05 Vec encoding growth remains bounded"
        );
        let decoded_sizes: Vec<_> = source
            .encoded
            .values()
            .map(|bytes| {
                block::Block::zcash_deserialize(bytes.as_slice())
                    .unwrap()
                    .attributed_memory_size_bytes()
            })
            .collect();
        let maximum_decoded = *decoded_sizes.iter().max().unwrap();
        let measured = source.probe.snapshot();
        assert!(measured.largest_allocation > 0);
        // Each V1 fixture owns distinct transaction allocations. Allow their Arc
        // controls and one bounded lookahead, independently of wire accounting.
        let allocation_envelope = maximum_decoded * 4 + 64 * 1024;
        assert!(
            u64::try_from(measured.peak_operation_bytes).unwrap() <= allocation_envelope,
            "C05 actual storage allocation peak: {measured:?}, envelope={allocation_envelope}"
        );
        assert_eq!(f.regulator.snapshot().node_active, 1);
        f.session.cancel_token().cancel();
        let regulator = f.regulator.clone();
        f.finish().await;
        assert_eq!(
            regulator.snapshot().node_active,
            1,
            "retained output owns capacity"
        );
        drop((frame, guard));
        let replacement = regulator.session(ZakuraPeerId::new(vec![1; 32]).unwrap());
        let request = replacement
            .decode_request(
                BlockSyncMessage::GetBlocks {
                    start_height: block::Height(100),
                    count: 1,
                }
                .encode_frame()
                .unwrap(),
            )
            .unwrap();
        drop(
            time::timeout(DEADLINE, replacement.admit_request(&request))
                .await
                .unwrap(),
        );
        assert_eq!(regulator.snapshot().node_active, 0);
        assert_eq!(source.live_decoded_bytes(), 0);
    }
}

#[tokio::test]
async fn c06_storage_error_returns_a_legal_empty_response_without_a_peer_fault() {
    let mut source = ControlledSource::new(100, 3, false, false);
    Arc::get_mut(&mut source).unwrap().fail = true;
    let mut f = source.fixture(1, 1, 1);
    request(&f, 100, 3).await;
    response(&mut f, &source, 100, 3, config(1).max_response_bytes, 128).await;
    assert!(!f.session.cancel_token().is_cancelled());
    source.probe.wait_finished(1).await;
    f.finish().await;
}

async fn check_serving(
    start: u32,
    count: u32,
    available: u32,
    depth: usize,
    cap_count: u32,
    large: bool,
) {
    let source = ControlledSource::new(start, count, large, false);
    let mut f = source.fixture(1, depth, 1);
    f.status.send_modify(|status| {
        // A status prefix models a gap at the end of the committed range.
        status.servable_high = block::Height(start + available.saturating_sub(1));
        status.max_blocks_per_response = cap_count;
        if available == 0 {
            status.servable_low = block::Height(start + count);
            status.servable_high = status.servable_low;
        }
    });
    request(&f, start, count).await;
    let expected = available.min(count).min(cap_count);
    let returned = if expected == 0 {
        assert_eq!(
            f.next().await,
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(start),
                count
            }
        );
        0
    } else {
        response(
            &mut f,
            &source,
            start,
            count,
            config(1).max_response_bytes,
            expected,
        )
        .await
    };
    assert!(returned <= usize::try_from(expected).unwrap());
    assert!(!f.session.cancel_token().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn c07_maximal_count_and_byte_limited_serving_use_real_frames() {
    check_serving(100, 128, 128, 1, 128, false).await;
    check_serving(10_000, 20, 20, 2, 128, true).await;
    check_serving(block::Height::MAX.0 - 127, 128, 128, 3, 128, false).await;
    check_serving(100, 3, 0, 1, 128, false).await;
    check_serving(100, 3, 1, 1, 128, false).await;
}

#[tokio::test]
async fn c07_waiting_request_uses_the_latest_serving_limits() {
    let source = ControlledSource::new(100, 3, false, false);
    let mut f = source.fixture(1, 1, 1);
    let other = f.regulator.session(ZakuraPeerId::new(vec![2; 32]).unwrap());
    let held_request = other
        .decode_request(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(200),
                count: 1,
            }
            .encode_frame()
            .unwrap(),
        )
        .unwrap();
    let held = other.admit_request(&held_request).await;
    request(&f, 100, 3).await;
    tokio::task::yield_now().await;
    assert_eq!(source.probe.snapshot().started, 0);
    f.status
        .send_modify(|status| status.max_blocks_per_response = 1);
    drop(held);
    response(&mut f, &source, 100, 3, config(1).max_response_bytes, 1).await;
    f.finish().await;
}

proptest! {
    #[test]
    fn c07_generated_serving_range_boundaries(start in 1u32..10_000, count in 1u32..=128,
        available in 0u32..=128, depth in 1usize..=3, cap_count in 1u32..=128) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check_serving(start, count, available.min(count), depth, cap_count, false));
    }
}

#[tokio::test]
async fn r02_buffered_overlap_shapes_are_not_admitted_while_a_terminal_is_owned() {
    // Identical, containing, contained, suffix/endpoint, adjacent and disjoint.
    // Bytes may wait behind backpressure. Admission becomes legal only after
    // the old ending finishes, so this does not call buffered bytes a violation.
    for (start, count) in [
        (100, 3),
        (98, 7),
        (99, 3),
        (101, 1),
        (101, 3),
        (102, 3),
        (103, 2),
        (105, 1),
    ] {
        let source = ControlledSource::new(98, 9, false, false);
        let mut f = source.fixture(1, 1, 1);
        request(&f, 100, 3).await;
        for _ in 0..3 {
            assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
        }
        let ending = time::timeout(DEADLINE, f.data.recv())
            .await
            .unwrap()
            .unwrap();
        let mut held_write = Box::pin(ending.write_with(|frame| async move {
            assert_eq!(
                BlockSyncMessage::decode_frame(frame).unwrap(),
                BlockSyncMessage::BlocksDone {
                    start_height: block::Height(100),
                    returned: 3
                }
            );
            std::future::pending::<Result<(), ()>>().await
        }));
        assert!(futures::poll!(&mut held_write).is_pending());
        request(&f, start, count).await;
        tokio::task::yield_now().await;
        assert_eq!(
            source.probe.snapshot().started,
            1,
            "R02 premature second admission for {start}/{count}"
        );
        drop(held_write);
        response(
            &mut f,
            &source,
            start,
            count,
            config(1).max_response_bytes,
            128,
        )
        .await;
        f.finish().await;
    }
}

#[derive(Debug)]
struct FaultSource {
    before_execution: bool,
    bodies: Vec<Arc<block::Block>>,
    starts: Arc<AtomicUsize>,
}

impl BlockRangeSource for FaultSource {
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, crate::BoxError>> {
        let (start, _, _, lease) = request.into_parts();
        let before = self.before_execution;
        let starts = self.starts.clone();
        let mut bodies = self.bodies.clone();
        Box::pin(async move {
            if before {
                return Err(
                    io::Error::other("storage readiness failed before starting a job").into(),
                );
            }
            tokio::task::spawn_blocking(move || {
                assert!(lease.try_start());
                starts.fetch_add(1, Ordering::Relaxed);
                // Invalid local storage data: the second encoding exceeds the
                // actual Block codec limit after a legal prefix was delivered.
                let last = Arc::make_mut(&mut bodies[1]);
                let tx = last.transactions[0].clone();
                last.transactions =
                    vec![tx; 2_100_000 / last.transactions[0].zcash_serialized_size() + 1];
                let result = bodies
                    .into_iter()
                    .enumerate()
                    .map(|(index, body)| {
                        (
                            block::Height(start.0 + u32::try_from(index).unwrap()),
                            body.clone(),
                            body.zcash_serialized_size(),
                        )
                    })
                    .collect();
                BlockRangeReadResult::new(result, lease)
            })
            .await
            .map_err(Into::into)
        })
    }
}

#[tokio::test]
async fn c06_readiness_failure_starts_no_job_and_is_not_peer_misconduct() {
    let starts = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(FaultSource {
        before_execution: true,
        bodies: Vec::new(),
        starts: starts.clone(),
    });
    let mut f = Fixture::new(source);
    f.session.mark_status_received();
    f.request().await;
    assert_eq!(
        f.next().await,
        BlockSyncMessage::RangeUnavailable {
            start_height: block::Height(1),
            count: 2
        }
    );
    assert_eq!(starts.load(Ordering::Relaxed), 0);
    assert!(!f.session.cancel_token().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn c06_encoding_failure_after_a_prefix_does_not_send_an_invalid_ending() {
    let starts = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(FaultSource {
        before_execution: false,
        bodies: fake_blocks_in_range(1, 2),
        starts: starts.clone(),
    });
    let mut f = Fixture::new(source);
    f.session.mark_status_received();
    f.request().await;
    assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
    assert!(matches!(
        time::timeout(DEADLINE, &mut f.task).await.unwrap().unwrap(),
        Err(crate::zakura::SinkReject::Local(_))
    ));
    assert!(
        time::timeout(Duration::from_millis(20), f.data.recv())
            .await
            .is_err(),
        "C06 no RangeUnavailable after a body and no invented terminal"
    );
    assert_eq!(starts.load(Ordering::Relaxed), 1);
    assert_eq!(f.regulator.snapshot().node_active, 0);
    assert!(!f.session.cancel_token().is_cancelled());
}

#[tokio::test]
async fn c06_closed_output_releases_finished_resources_before_and_after_a_body() {
    for after_prefix in [false, true] {
        let source = ControlledSource::new(100, 3, false, false);
        let mut f = source.fixture(1, 1, 1);
        request(&f, 100, 3).await;
        if after_prefix {
            assert!(matches!(f.next().await, BlockSyncMessage::Block(_)));
        }
        drop(f.data);
        assert!(matches!(
            time::timeout(DEADLINE, &mut f.task).await.unwrap().unwrap(),
            Err(crate::zakura::SinkReject::Local(_))
        ));
        source.probe.wait_finished(1).await;
        assert_eq!(source.live_decoded_bytes(), 0);
        assert_eq!(f.regulator.snapshot().node_active, 0);
        assert!(!f.session.cancel_token().is_cancelled());
    }
}

#[tokio::test]
async fn c07_storage_gaps_exact_byte_fits_and_first_body_too_large() {
    for cap_case in 0..4 {
        let mut source = ControlledSource::new(100, 3, false, false);
        if cap_case == 0 {
            Arc::make_mut(&mut Arc::get_mut(&mut source).unwrap().encoded)
                .remove(&block::Height(101));
        }
        let bytes = source.encoded[&block::Height(100)].len();
        let cap = u32::try_from(match cap_case {
            2 => bytes - 1,
            3 => 2 * bytes,
            _ => bytes,
        })
        .unwrap();
        let mut f = source.fixture(1, 1, 1);
        f.status
            .send_modify(|status| status.max_response_bytes = cap);
        request(&f, 100, 3).await;
        response(&mut f, &source, 100, 3, cap, 128).await;
        f.finish().await;
    }
}

#[tokio::test]
async fn c05_actual_large_encoder_peak_and_retained_frame_follow_the_declared_bound() {
    let source = ControlledSource::new(100, 1, true, false);
    let encoded = &source.encoded[&block::Height(100)];
    let body = Arc::new(block::Block::zcash_deserialize(encoded.as_slice()).unwrap());
    let regulator = GetBlocksServingRegulator::new(config(1));
    let session = regulator.session(ZakuraPeerId::new(vec![1; 32]).unwrap());
    let request = session
        .decode_request(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(100),
                count: 1,
            }
            .encode_frame()
            .unwrap(),
        )
        .unwrap();
    let mut permit = session.admit_request(&request).await;
    let probe = ExecutionProbe::new(false, false);
    permit.encode_probe = Some(probe.clone());
    let (send, mut recv) = worker_framed_channel(1);
    send_response(&send, &mut permit, BlockSyncMessage::Block(body))
        .await
        .unwrap();
    probe.wait_finished(1).await;
    let measured = probe.snapshot();
    let payload_bound = 2 * (encoded.len() + 1);
    assert!(
        measured.peak_operation_bytes <= payload_bound + 4096,
        "C05 actual encoder peak {measured:?}, payload bound={payload_bound}"
    );
    assert!(measured.largest_allocation >= encoded.len());
    drop(permit);
    let held = recv.recv().await.unwrap();
    let (frame, guard) = held.into_parts();
    assert!(frame.payload.capacity() <= payload_bound);
    assert_eq!(regulator.snapshot().node_active, 1);
    drop((frame, guard));
    assert_eq!(regulator.snapshot().node_active, 0);
}
