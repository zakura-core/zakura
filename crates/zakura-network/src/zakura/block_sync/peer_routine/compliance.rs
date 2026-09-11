//! Executable requirements from PR 747. A failing assertion is a conformance gap.
//!
//! The fixture drives publication, frame decoding, and receiver transitions. The
//! oracle below knows only the requested hashes, consumed prefix, and terminal.

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

impl Fixture {
    fn new(start: u32, count: u32) -> Self {
        Self::for_peer(start, count, 0x47)
    }

    fn for_peer(start: u32, count: u32, peer_byte: u8) -> Self {
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
        routine.handle_status(BlockSyncStatus {
            servable_low: block::Height(start),
            servable_high: block::Height(start + count - 1),
            max_blocks_per_response: 128,
            max_inflight_requests: 8,
            ..config.initial_status()
        });
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

#[tokio::test]
async fn r01_published_request_accepts_an_immediate_response() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.assert_live(1, "R01");
}

#[tokio::test]
async fn r06_done_count_must_equal_consumed_prefix() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.rejects(f.done(3), "R06 returned=3 after one body").await;
}

#[tokio::test]
async fn r06_done_before_any_body_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.rejects(f.done(1), "R06 terminal before a body").await;
}

#[tokio::test]
async fn r06_done_cannot_close_a_different_start() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.rejects(
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(101),
            returned: 1,
        },
        "R06 wrong range identity",
    )
    .await;
}

#[tokio::test]
async fn r07_unavailable_count_must_equal_original_request() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.rejects(f.unavailable(2), "R07 wrong unavailable count")
        .await;
}

#[tokio::test]
async fn r07_unavailable_after_a_body_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.rejects(f.unavailable(3), "R07 unavailable after body")
        .await;
}

#[tokio::test]
async fn r07_unavailable_without_a_request_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.rejects(f.unavailable(3), "R07 no live request").await;
}

#[tokio::test]
async fn r07_legal_unavailable_requeues_only_still_needed_heights() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.view.send_modify(|view| {
        view.download_floor = block::Height(100);
        view.verified_tip = block::Height(100);
        view.finalized = block::Height(100);
    });
    f.deliver(f.unavailable(3)).await.unwrap();
    f.assert_no_peer_fault();
    assert!(!f.routine.work.pending_contains(block::Height(100)));
    assert!(f.routine.work.pending_contains(block::Height(101)));
    assert!(f.routine.work.pending_contains(block::Height(102)));
}

#[tokio::test]
async fn r08_last_body_does_not_consume_the_terminal() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    for index in 0..3 {
        f.body(index).await;
    }
    f.assert_live(3, "R08");
    f.deliver(f.done(3)).await.unwrap();
    assert!(f.routine.window.outstanding.is_empty());
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn r08_duplicate_terminal_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.deliver(f.unavailable(3)).await.unwrap();
    f.rejects(f.unavailable(3), "R08 duplicate unavailable")
        .await;
}

#[tokio::test]
async fn r08_different_second_terminal_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.deliver(f.done(1)).await.unwrap();
    f.rejects(f.unavailable(3), "R08 second terminal of another kind")
        .await;
}

#[tokio::test]
async fn r04_later_correct_hash_is_not_the_next_expected_part() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.rejects(
        BlockSyncMessage::Block(f.blocks[1].clone()),
        "R04 out of order",
    )
    .await;
}

#[tokio::test]
async fn r04_wrong_hash_must_disconnect_before_handler() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    let mut wrong = (*f.blocks[0]).clone();
    Arc::make_mut(&mut wrong.header).nonce[0] ^= 1;
    f.rejects(
        BlockSyncMessage::Block(Arc::new(wrong)),
        "R04 wrong expected hash",
    )
    .await;
}

#[tokio::test]
async fn r05_duplicate_body_cannot_consume_a_part_twice() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R05 duplicate body",
    )
    .await;
}

#[tokio::test]
async fn r05_body_after_terminal_has_no_authorization() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.deliver(f.done(1)).await.unwrap();
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R05 consumed exchange",
    )
    .await;
}

#[tokio::test]
async fn r03_needed_height_does_not_authorize_an_unsolicited_body() {
    let mut f = Fixture::new(100, 3);
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R03 needed is not authorized",
    )
    .await;
}

#[tokio::test]
async fn r03_servable_height_does_not_authorize_an_unsolicited_body() {
    let mut f = Fixture::new(100, 3);
    f.routine.work.reset_above(block::Height(99));
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R03 advertised is not authorized",
    )
    .await;
}

#[tokio::test]
async fn r09_deadline_does_not_revoke_a_written_request() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    let deadline = f.routine.window.outstanding[0].deadline;
    f.routine
        .expire_due_timeouts(deadline + Duration::from_millis(1));
    f.assert_live(0, "R09 local deadline");
    f.body(0).await;
    f.deliver(f.done(1)).await.unwrap();
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn r10_finality_does_not_consume_network_response_parts() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.view.send_modify(|view| {
        view.download_floor = block::Height(102);
        view.verified_tip = block::Height(102);
        view.finalized = block::Height(102);
    });
    f.routine.gc_committed_outstanding();
    f.assert_live(0, "R10 finality is not a peer response");
    f.deliver(f.unavailable(3)).await.unwrap();
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn r10_reorganization_keeps_the_original_response_identity() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.routine.work.reset_above(block::Height(99));
    f.view.send_modify(|view| view.reset_epoch += 1);
    f.routine.on_view_changed();
    f.assert_live(0, "R10 selected-chain reset");
}

#[tokio::test]
async fn r02_retry_cannot_send_an_overlapping_live_range() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    let deadline = f.routine.window.outstanding[0].deadline;
    f.routine
        .expire_due_timeouts(deadline + Duration::from_millis(1));
    f.routine.retry_avoid.clear();
    f.routine.try_fill().await;
    assert!(
        time::timeout(Duration::from_millis(20), f.outbound.recv())
            .await
            .is_err(),
        "R02: a local deadline cannot authorize a second overlapping wire request"
    );
}

#[tokio::test]
async fn r11_connection_drop_releases_only_unreceived_work() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    let work = f.routine.work.clone();
    let budget = f.routine.budget.clone();
    let registry = f.routine.registry.clone();
    let peer = f.routine.peer.clone();
    drop(f);
    assert_eq!(budget.reserved(), 0);
    assert!(!work.pending_contains(block::Height(100)));
    assert!(work.pending_contains(block::Height(101)));
    assert!(work.pending_contains(block::Height(102)));
    assert!(!registry.peer_has_outstanding_height(&peer, block::Height(101)));
}

#[tokio::test]
async fn r03_another_peers_request_does_not_authorize_this_peer() {
    let mut a = Fixture::for_peer(100, 3, 1);
    a.publish().await;
    let mut b = Fixture::for_peer(100, 3, 2);
    b.routine.work = a.routine.work.clone();
    b.routine.registry = a.routine.registry.clone();
    b.rejects(
        BlockSyncMessage::Block(b.blocks[0].clone()),
        "R03 another peer owns the request",
    )
    .await;
}

#[tokio::test]
async fn r03_local_floor_does_not_authorize_an_unsolicited_body() {
    let mut f = Fixture::new(100, 3);
    f.view
        .send_modify(|view| view.download_floor = block::Height(102));
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R03 obsolete is not authorized",
    )
    .await;
}

#[tokio::test]
async fn r04_body_beyond_the_authorized_range_is_invalid() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.routine.servable_high = block::Height(200);
    let extra = fake_blocks_in_range(103, 103).pop().unwrap();
    f.rejects(BlockSyncMessage::Block(extra), "R04 body beyond range")
        .await;
}

#[tokio::test]
async fn r05_separately_authorized_peers_can_deliver_the_same_block() {
    let mut a = Fixture::for_peer(100, 3, 1);
    let mut b = Fixture::for_peer(100, 3, 2);
    a.publish().await;
    b.publish().await;
    assert_eq!(a.blocks[0].hash(), b.blocks[0].hash());
    a.body(0).await;
    b.body(0).await;
    a.deliver(a.done(1)).await.unwrap();
    b.deliver(b.done(1)).await.unwrap();
    a.assert_no_peer_fault();
    b.assert_no_peer_fault();
}

#[tokio::test]
async fn r09_reassigned_work_still_consumes_the_original_peers_response() {
    let mut a = Fixture::for_peer(100, 3, 1);
    a.publish().await;
    let deadline = a.routine.window.outstanding[0].deadline;
    a.routine
        .expire_due_timeouts(deadline + Duration::from_millis(1));
    let mut b = Fixture::for_peer(100, 3, 2);
    b.routine.work = a.routine.work.clone();
    b.publish().await;
    b.body(0).await;
    a.deliver(BlockSyncMessage::Block(a.blocks[0].clone()))
        .await
        .unwrap();
    a.assert_no_peer_fault();
    a.assert_live(1, "R09 original authorization after competing delivery");
    a.deliver(a.done(1)).await.unwrap();
    b.deliver(b.done(1)).await.unwrap();
}

#[tokio::test]
async fn r10_lost_local_interest_does_not_revoke_expected_hashes() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.routine.work.reset_above(block::Height(99));
    f.routine.gc_obsolete_outstanding();
    f.assert_live(0, "R10 changed local interest");
}

#[tokio::test]
async fn r11_terminals_pending_still_occupy_protocol_inflight_capacity() {
    let mut f = Fixture::new(100, 3);
    f.routine.window.max_inflight_requests = 1;
    f.publish().await;
    for index in 0..3 {
        f.body(index).await;
    }
    f.routine.work.extend(
        super::super::test_work_scope(),
        fake_blocks_in_range(103, 103).iter().map(|body| {
            (
                block::Height(103),
                body.hash(),
                BlockSizeEstimate::Confirmed(u32::try_from(body.zcash_serialized_size()).unwrap()),
            )
        }),
    );
    f.routine.servable_high = block::Height(103);
    f.routine.try_fill().await;
    assert!(
        time::timeout(Duration::from_millis(20), f.outbound.recv())
            .await
            .is_err(),
        "R11: the missing terminal occupies the only advertised protocol slot"
    );
}

async fn check_receiver_byte_cap(excess: bool, underestimated: bool) {
    let mut f = Fixture::new(100, 3);
    let actual: u32 = f
        .blocks
        .iter()
        .map(|b| u32::try_from(b.zcash_serialized_size()).unwrap())
        .sum();
    // Estimates affect local scheduling, not the peer's response contract. Small
    // estimates let the request reach the count boundary before the byte bound.
    f.routine.work = Arc::new(WorkQueue::new(block::Height(99)));
    f.routine.work.set_estimate_floor_for_tests(1);
    f.routine.work.extend(
        super::super::test_work_scope(),
        f.blocks.iter().map(|body| {
            (
                body.coinbase_height().unwrap(),
                body.hash(),
                BlockSizeEstimate::Advertised(1),
            )
        }),
    );
    if !underestimated {
        f.routine.config.size_deviation_tolerance = u32::MAX;
    }
    f.routine.max_response_bytes = actual - u32::from(excess);
    f.publish().await;
    for index in 0..2 {
        f.body(index).await;
    }
    if excess {
        f.rejects(
            BlockSyncMessage::Block(f.blocks[2].clone()),
            "R12 cumulative bodies exceed advertised bytes by one",
        )
        .await;
    } else {
        f.body(2).await;
        // Tags and the nine-byte terminal are excluded from the body-byte limit.
        f.deliver(f.done(3)).await.unwrap();
        f.assert_no_peer_fault();
    }
}

#[tokio::test]
async fn r12_cumulative_body_bytes_cannot_exceed_the_advertised_cap() {
    check_receiver_byte_cap(true, false).await;
}

#[tokio::test]
async fn r12_exact_body_byte_limit_excludes_tags_and_terminal() {
    check_receiver_byte_cap(false, false).await;
}

#[tokio::test]
async fn r12_internal_estimates_do_not_create_undeclared_peer_obligations() {
    check_receiver_byte_cap(false, true).await;
}

#[tokio::test]
async fn f04_invalid_body_identity_is_rejected_before_waiting_for_handler_capacity() {
    let mut f = Fixture::new(100, 3);
    let held: Vec<_> = (0..256)
        .map(|_| {
            f.routine
                .sequencer_input
                .clone()
                .try_reserve_owned()
                .unwrap()
        })
        .collect();
    let result = time::timeout(
        Duration::from_millis(100),
        f.routine.handle_frame(
            &mut f.guard,
            BlockSyncMessage::Block(f.blocks[0].clone())
                .encode_frame()
                .unwrap(),
        ),
    )
    .await;
    drop(held);
    assert!(
        matches!(result, Ok(Err(SinkReject::Protocol(_)))),
        "F04: an unauthorized response must not wait on downstream capacity: {result:?}"
    );
}

#[tokio::test]
async fn f04_absent_reservation_is_checked_before_allocating_a_decoded_body() {
    let mut f = Fixture::new(100, 3);
    let probe = zakura_test::execution::ExecutionProbe::new(false, false);
    f.routine.decode_probe = Some(probe.clone());
    let result = f
        .deliver(BlockSyncMessage::Block(f.blocks[0].clone()))
        .await;
    let allocation = probe.snapshot().largest_allocation;
    assert_eq!(
        allocation, 0,
        "F04 no reservation exists to fund a Block decode, result={result:?}"
    );
    assert!(matches!(result, Err(SinkReject::Protocol(_))));
    assert!(f.bodies.is_empty());
}

#[tokio::test]
async fn c06_local_handler_closure_is_not_peer_misconduct() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.bodies.close();
    let result = f
        .deliver(BlockSyncMessage::Block(f.blocks[0].clone()))
        .await;
    assert!(matches!(result, Err(SinkReject::Local(_))), "{result:?}");
    f.assert_no_peer_fault();
}

async fn check_done_count(start: u32, requested: u32, consumed: u32, returned: u32) {
    let mut f = Fixture::new(start, requested);
    f.publish().await;
    for index in 0..usize::try_from(consumed).unwrap() {
        f.body(index).await;
    }
    if returned == consumed && consumed > 0 {
        f.deliver(f.done(returned)).await.unwrap();
        f.assert_no_peer_fault();
        assert!(f.routine.window.outstanding.is_empty());
        for offset in consumed..requested {
            assert!(f
                .routine
                .work
                .pending_contains(block::Height(start + offset)));
        }
    } else {
        f.rejects(f.done(returned), "R06 generated terminal identity/count")
            .await;
    }
}

proptest! {
    #[test]
    fn r06_generated_terminal_counts(
        start in 1u32..10_000,
        requested in 1u32..=128,
        prefix in 0u32..=128,
        returned in 1u32..=128,
    ) {
        let consumed = prefix.min(requested);
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check_done_count(start, requested, consumed, returned));
    }

    #[test]
    fn r07_generated_unavailable_identity(start in 1u32..10_000, count in 1u32..=128, delta in 1u32..=127) {
        // Shrinking preserves the count violation, including at both boundaries.
        let wrong = (count - 1 + delta) % 128 + 1;
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let mut f = Fixture::new(start, count);
            f.publish().await;
            f.rejects(f.unavailable(wrong), "R07 generated wrong count").await;
        });
    }
}

#[derive(Clone, Debug)]
enum LocalChange {
    Deadline,
    Finality,
    Reset,
    LostInterest,
}

async fn legal_history(count: u32, prefix: usize, changes: Vec<LocalChange>) {
    let mut f = Fixture::new(100, count);
    f.publish().await;
    for index in 0..prefix {
        f.body(index).await;
    }
    f.assert_live(prefix, "R01/R08 model after each consumed prefix");
    for change in changes {
        match change {
            LocalChange::Deadline => {
                let deadline = f.routine.window.outstanding[0].deadline;
                f.routine
                    .expire_due_timeouts(deadline + Duration::from_millis(1));
            }
            LocalChange::Finality => {
                f.view.send_modify(|view| {
                    view.download_floor = block::Height(99 + count);
                    view.verified_tip = view.download_floor;
                    view.finalized = view.download_floor;
                });
                f.routine.gc_committed_outstanding();
            }
            LocalChange::Reset => {
                f.routine.work.reset_above(block::Height(99));
                f.view.send_modify(|view| view.reset_epoch += 1);
                f.routine.on_view_changed();
            }
            LocalChange::LostInterest => {
                f.routine.work.reset_above(block::Height(99));
                f.routine.gc_obsolete_outstanding();
            }
        }
        f.assert_live(prefix, &format!("R09/R10 model after {change:?}"));
        f.assert_no_peer_fault();
    }
    // The peer is still allowed to close its original exchange even when local
    // work changed. Consumed network parts are independent of verification need.
    let ending = if prefix == 0 {
        f.unavailable(count)
    } else {
        f.done(u32::try_from(prefix).unwrap())
    };
    f.deliver(ending).await.unwrap();
    assert!(f.routine.window.outstanding.is_empty());
    f.assert_no_peer_fault();
}

#[tokio::test]
async fn r01_r08_mandatory_legal_terminal_and_maximal_count_histories() {
    for count in [1, 2, 128] {
        for prefix in [0, 1, usize::try_from(count).unwrap()] {
            legal_history(count, prefix, Vec::new()).await;
        }
    }
}

proptest! {
    #[test]
    fn r09_r10_generated_local_changes_preserve_live_authorization(
        count in 1u32..=128, prefix in 0usize..=128,
        changes in prop::collection::vec(prop_oneof![Just(LocalChange::Deadline), Just(LocalChange::Finality), Just(LocalChange::Reset), Just(LocalChange::LostInterest)], 1..9),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(legal_history(count, prefix.min(usize::try_from(count).unwrap()), changes));
    }
}

#[tokio::test]
async fn r11_connection_closure_cleans_each_publication_and_response_phase() {
    // Includes queued but unclaimed, written, partial body, and terminal pending.
    for phase in 0usize..=4 {
        let mut f = Fixture::new(100, 3);
        if phase == 0 {
            f.routine.try_fill().await;
        } else {
            f.publish().await;
        }
        for index in 0..phase.saturating_sub(1) {
            f.body(index).await;
        }
        let work = f.routine.work.clone();
        let budget = f.routine.budget.clone();
        let registry = f.routine.registry.clone();
        let peer = f.routine.peer.clone();
        drop(f);
        assert_eq!(budget.reserved(), 0, "R11 closure phase={phase}");
        for index in 0..3 {
            let height = block::Height(100 + u32::try_from(index).unwrap());
            assert!(!registry.peer_has_outstanding_height(&peer, height));
            assert_eq!(
                work.pending_contains(height),
                index >= phase.saturating_sub(1)
            );
        }
    }
}

#[tokio::test]
async fn r08_done_cannot_be_consumed_twice_after_a_full_response() {
    let mut f = Fixture::new(100, 1);
    f.publish().await;
    f.body(0).await;
    f.deliver(f.done(1)).await.unwrap();
    f.rejects(f.done(1), "R08 duplicate Done after complete body prefix")
        .await;
}

#[tokio::test]
async fn r07_unavailable_cannot_close_another_start() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.rejects(
        BlockSyncMessage::RangeUnavailable {
            start_height: block::Height(101),
            count: 3,
        },
        "R07 wrong request identity",
    )
    .await;
}

#[tokio::test]
async fn r05_duplicate_body_remains_invalid_after_local_finality_advances() {
    let mut f = Fixture::new(100, 3);
    f.publish().await;
    f.body(0).await;
    f.view
        .send_modify(|view| view.download_floor = block::Height(100));
    f.rejects(
        BlockSyncMessage::Block(f.blocks[0].clone()),
        "R05 consumed part below local floor",
    )
    .await;
}
