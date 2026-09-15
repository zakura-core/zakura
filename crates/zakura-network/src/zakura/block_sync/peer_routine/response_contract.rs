//! Checks what a peer may send after we request blocks.
//!
//! The fixture uses the real request queue and frame decoder. Tests compare the
//! receiver with the original requested hashes, received prefix, and ending.
//! Local deadlines and changes to the chain must not rewrite that wire contract.

use super::super::{
    request::BlockSizeEstimate, sequencer_task::initial_view, state::ByteBudget,
    tests::fake_blocks_in_range, BlockSyncFrontiers, CwndUnit,
};
use super::*;
use crate::zakura::{
    framed_channel,
    transport::{worker_framed_channel, FramedWorkerRecv},
    Frame, FramedSend, SessionGuard,
};
use proptest::prelude::*;
use std::sync::{atomic::AtomicU64, Mutex};

const DEADLINE: Duration = Duration::from_secs(2);

mod identity;
mod indexed_matching;
mod lifetime;
mod limits;
mod retention;
mod terminal_counts;

struct Fixture {
    routine: PeerRoutine,
    outbound: FramedWorkerRecv,
    _inbound: FramedSend,
    bodies: mpsc::Receiver<SequencedBody>,
    events: mpsc::Receiver<RoutineToReactor>,
    view: watch::Sender<SequencerView>,
    guard: SessionGuard,
    blocks: Vec<Arc<block::Block>>,
}

#[tokio::test]
async fn the_current_response_follows_request_order_not_vector_order() {
    // Ending a range swap-removes it, so the vector is not in request order, and two
    // requests issued in one clock tick would tie on a timestamp. Order comes from
    // the monotonic request id instead.
    let mut fixture = Fixture::with_initial_status(
        1,
        3,
        0x47,
        Some(BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(3),
            max_blocks_per_response: 1,
            max_inflight_requests: 8,
            ..ZakuraBlockSyncConfig::default().initial_status()
        }),
    );
    fixture.routine.try_fill().await;
    assert_eq!(
        fixture.routine.window.outstanding.len(),
        3,
        "a one-block response cap issues one request per height"
    );
    let second = fixture.routine.window.outstanding[1]
        .request
        .owner
        .request_id;

    // Ending the first range moves the last one into its slot, so the newest range
    // now sits ahead of the one that should answer next.
    fixture.routine.window.remove_outstanding(0);
    assert!(
        fixture.routine.window.outstanding[0]
            .request
            .owner
            .request_id
            > second,
        "the swap left a newer range in front"
    );

    // Both were issued in the same clock tick, which a timestamp cannot separate:
    // `min_by_key` would then return whichever the swap left first.
    let tie = fixture.routine.window.outstanding[0].queued_at;
    for range in &mut fixture.routine.window.outstanding {
        range.queued_at = tie;
    }

    let index = fixture.routine.current_response_index().unwrap();
    assert_eq!(
        fixture.routine.window.outstanding[index]
            .request
            .owner
            .request_id,
        second,
        "the earliest issued response still owns the body",
    );
}

#[tokio::test]
async fn work_refused_only_by_a_retained_range_gets_no_retry_deadline() {
    // A retained range clears on its ending or on connection close, and both wake
    // the routine on their own. Scheduling a timer instead spins: the fill loop
    // takes the covered height, the overlap refuses it, and the deadline is now.
    let mut fixture = Fixture::new(1, 4);
    fixture.publish().await;
    let covered = fixture.routine.window.outstanding[0].request.start_height;
    assert!(
        fixture.routine.retry_avoid.is_empty(),
        "nothing has failed yet"
    );

    let now = Instant::now();
    assert_eq!(
        fixture.routine.retry_filter_wake_deadline(now, [covered]),
        None,
        "a retained range must not schedule an immediate retry",
    );
    assert_eq!(
        fixture
            .routine
            .retry_filter_wake_deadline(now, [block::Height(10_000)]),
        Some(now),
        "work refused for any other reason still retries at once",
    );
}

impl Fixture {
    fn new(start: u32, count: u32) -> Self {
        Self::for_peer(start, count, 0x47)
    }

    fn for_peer(start: u32, count: u32, peer_byte: u8) -> Self {
        Self::with_initial_status(start, count, peer_byte, None)
    }

    fn with_initial_status(
        start: u32,
        count: u32,
        peer_byte: u8,
        initial_status: Option<BlockSyncStatus>,
    ) -> Self {
        let blocks = fake_blocks_in_range(start, start + count - 1);
        let config = ZakuraBlockSyncConfig {
            max_blocks_per_response: 128,
            bbr_cwnd_unit: CwndUnit::Blocks,
            initial_block_probe_requests: 128,
            ..ZakuraBlockSyncConfig::default()
        };
        let peer = ZakuraPeerId::new(vec![peer_byte; 32]).unwrap();
        let registry = Arc::new(PeerRegistry::new());
        let generation = registry
            .admit_session(
                &peer,
                crate::zakura::ServicePeerDirection::Outbound,
                &config,
                0,
                Instant::now(),
            )
            .generation();
        let cancel = CancellationToken::new();
        let (send, outbound) = worker_framed_channel(8);
        let session = BlockSyncPeerSession::for_test_with_session_id(
            peer.clone(),
            generation,
            send,
            cancel.clone(),
        );
        let (inbound, recv) = framed_channel(8);
        let (body_tx, bodies) = mpsc::channel(256);
        let (event_tx, events) = mpsc::channel(256);
        let (view, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(start - 1),
            verified_block_tip: block::Height(start - 1),
            verified_block_hash: block::Hash([0; 32]),
        }));
        let work = Arc::new(WorkQueue::new(block::Height(start - 1)));
        work.set_estimate_floor_for_tests(1);
        work.extend(
            super::super::test_work_scope(),
            blocks.iter().map(|body| {
                (
                    body.coinbase_height().unwrap(),
                    body.hash(),
                    BlockSizeEstimate::Confirmed(
                        u32::try_from(body.zcash_serialized_size()).unwrap(),
                    ),
                )
            }),
        );
        let mut routine = PeerRoutine::new(
            super::super::tests::mainnet_decoder(),
            peer,
            0,
            session,
            recv,
            config.clone(),
            false,
            generation,
            ByteBudget::new(256 * 1024 * 1024),
            work,
            registry,
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            body_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            event_tx,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        routine.handle_status(initial_status.unwrap_or(BlockSyncStatus {
            servable_low: block::Height(start),
            servable_high: block::Height(start + count - 1),
            max_blocks_per_response: 128,
            max_inflight_requests: 8,
            ..config.initial_status()
        }));
        Self {
            routine,
            outbound,
            _inbound: inbound,
            bodies,
            events,
            view,
            guard: block_sync_guard(),
            blocks,
        }
    }

    /// Model two connections using the same node-owned queue and generation source.
    fn share_work_from(&mut self, other: &Self) {
        self.routine.work = other.routine.work.clone();
        self.routine.budget = other.routine.budget.clone();
        self.routine.registry = other.routine.registry.clone();
        self.routine.generation = self
            .routine
            .registry
            .admit_session(
                &self.routine.peer,
                crate::zakura::ServicePeerDirection::Outbound,
                &self.routine.config,
                self.routine.conn_id,
                Instant::now(),
            )
            .generation();
        let status = BlockSyncStatus {
            servable_low: self.routine.servable_low,
            servable_high: self.routine.servable_high,
            max_blocks_per_response: self.routine.max_blocks_per_response,
            max_response_bytes: self.routine.max_response_bytes,
            max_inflight_requests: self.routine.window.max_inflight_requests,
            ..self.routine.config.initial_status()
        };
        self.routine
            .registry
            .upsert_status(&self.routine.peer, self.routine.generation, status);
        assert_ne!(self.routine.generation, other.routine.generation);
    }

    async fn publish(&mut self) {
        self.routine.try_fill().await;
        assert_eq!(
            self.routine.window.outstanding.len(),
            1,
            "fixture publishes one range"
        );
        let queued = time::timeout(DEADLINE, self.outbound.recv())
            .await
            .unwrap()
            .unwrap();
        let expected: Vec<_> = self.blocks.iter().map(|b| b.hash()).collect();
        let reserved: Vec<_> = self.routine.window.outstanding[0]
            .request
            .expected_blocks
            .iter()
            .map(|b| b.hash)
            .collect();
        assert_eq!(
            reserved, expected,
            "R01: authorization exists before the writer sees bytes"
        );
        queued.write_with(|frame| async move {
            assert!(matches!(BlockSyncMessage::decode_frame(frame).unwrap(),
                BlockSyncMessage::GetBlocks { count, .. } if usize::try_from(count).unwrap() == expected.len()));
            Ok::<_, std::convert::Infallible>(())
        }).await.unwrap();
        self.clear_events();
    }

    fn clear_events(&mut self) {
        while self.events.try_recv().is_ok() {}
    }

    async fn frame(&mut self, frame: Frame) -> Result<(), SinkReject> {
        time::timeout(DEADLINE, self.routine.handle_frame(&mut self.guard, frame))
            .await
            .expect("a controlled receiver transition must complete")
    }

    async fn deliver(&mut self, message: BlockSyncMessage) -> Result<(), SinkReject> {
        self.frame(message.encode_frame().unwrap()).await
    }

    async fn body(&mut self, index: usize) {
        let body = self.blocks[index].clone();
        self.deliver(BlockSyncMessage::Block(body.clone()))
            .await
            .unwrap();
        let received = self
            .bodies
            .try_recv()
            .expect("a matched useful body reaches the handler");
        assert_eq!(received.hash, body.hash());
        assert_eq!(received.peer, self.routine.peer);
        self.assert_no_peer_fault();
    }

    fn assert_no_peer_fault(&mut self) {
        while let Ok(event) = self.events.try_recv() {
            assert!(
                !matches!(event, RoutineToReactor::Misbehavior { .. }),
                "a legal response/local transition is not misconduct: {event:?}"
            );
        }
    }

    async fn rejects(&mut self, message: BlockSyncMessage, rule: &str) {
        let before = self.bodies.len();
        let result = self.deliver(message).await;
        assert!(
            matches!(result, Err(SinkReject::Protocol(_))),
            "{rule}: invalid response must disconnect, got {result:?}"
        );
        assert_eq!(
            self.bodies.len(),
            before,
            "{rule}: rejection precedes handler delivery"
        );
    }

    /// A legal but useless body spends one part of the current response and is
    /// dropped without a peer fault or a handler delivery.
    async fn discards(&mut self, message: BlockSyncMessage, consumed_after: u64, rule: &str) {
        let before = self.bodies.len();
        let body_bytes = u64::try_from(
            message.encode_frame().unwrap().payload.len()
                - super::super::wire::BLOCK_SYNC_MESSAGE_TYPE_BYTES,
        )
        .unwrap();
        let bytes_before = self.routine.window.outstanding[0].response.consumed_bytes();
        let result = self.deliver(message).await;
        assert!(
            result.is_ok(),
            "{rule}: a mismatched body inside a range is not a fault, got {result:?}"
        );
        assert_eq!(
            self.bodies.len(),
            before,
            "{rule}: a discarded body never reaches the handler"
        );
        assert_eq!(
            self.routine.window.outstanding[0]
                .response
                .consumed_objects(),
            consumed_after,
            "{rule}: the discarded body spends response credit"
        );
        assert_eq!(
            self.routine.window.outstanding[0].response.consumed_bytes(),
            bytes_before + body_bytes,
            "{rule}: the discarded body is charged its own wire bytes"
        );
        self.assert_no_peer_fault();
    }

    fn done(&self, returned: u32) -> BlockSyncMessage {
        BlockSyncMessage::BlocksDone {
            start_height: self.blocks[0].coinbase_height().unwrap(),
            returned,
        }
    }

    fn unavailable(&self, count: u32) -> BlockSyncMessage {
        BlockSyncMessage::RangeUnavailable {
            start_height: self.blocks[0].coinbase_height().unwrap(),
            count,
        }
    }

    fn assert_live(&self, received: usize, rule: &str) {
        // Adapt this observation if authorization moves out of DownloadWindow.
        // Counting queued bytes or accepted bodies cannot stand in for a terminal.
        assert_eq!(
            self.routine.window.outstanding.len(),
            1,
            "{rule}: the exchange remains authorized until its terminal or connection closure"
        );
        let range = &self.routine.window.outstanding[0];
        assert_eq!(
            range.request.count,
            u32::try_from(self.blocks.len()).unwrap()
        );
        for (index, body) in self.blocks.iter().enumerate() {
            let height = body.coinbase_height().unwrap();
            assert_eq!(
                range
                    .request
                    .expected_blocks
                    .iter()
                    .find(|part| part.height == height)
                    .map(|part| part.hash),
                Some(body.hash())
            );
            assert_eq!(range.has_received(height), index < received);
        }
    }
}

/// Observe the production decode after authorization, so a refused response can
/// prove it never allocated a full block. The successful-body control checks that
/// the same observer records allocations when decoding really happens.
pub(super) fn observe_decode(
    probe: &Option<Arc<zakura_test::execution::ExecutionProbe>>,
    decoder: ZcashDecoder,
    frame: Frame,
) -> Result<(BlockSyncMessage, Option<RawBlockPayload>), super::super::BlockSyncWireError> {
    if let Some(probe) = probe {
        let (decoded, allocations) = zakura_test::allocations::measure(|| {
            BlockSyncMessage::decode_frame_with_raw_block_payload(frame, decoder)
        });
        probe.allocations(allocations);
        decoded
    } else {
        BlockSyncMessage::decode_frame_with_raw_block_payload(frame, decoder)
    }
}
