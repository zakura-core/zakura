//! Receipt lifecycle tests and a scaling probe for per-request bookkeeping.
//!
//! Run with `cargo test -p zakura-network --release --lib request_bookkeeping_scaling
//! -- --ignored --nocapture`. Each request event should cost about the same at
//! every window size. A per-request cost that grows with the window means that
//! some event still scans or rebuilds the whole window.

use super::super::request::BlockSizeEstimate;
use super::*;
use crate::zakura::regulation::ReservationPool;
use crate::zakura::transport::ByteBudget;
use zakura_chain::serialization::ZcashDeserializeInto;

/// A routine with `window` single-block requests' worth of work and capacity.
pub(super) struct Probe {
    pub(super) routine: PeerRoutine,
    pub(super) outbound: FramedRecv,
    pub(super) blocks: Vec<Arc<block::Block>>,
    _bodies: mpsc::Receiver<SequencedBody>,
    _events: mpsc::Receiver<RoutineToReactor>,
}

/// [`probe_in`] with a private pool of `window` entries.
fn probe(window: u32) -> Probe {
    probe_in(
        window,
        ReservationPool::new(usize::try_from(window).unwrap()).unwrap(),
    )
}

/// A routine that may open `window` requests at once, without waiting for a
/// body, for work at heights `1..=window`. Its requests reserve from `pool`.
pub(super) fn probe_in(window: u32, pool: ReservationPool) -> Probe {
    let config = ZakuraBlockSyncConfig {
        max_inflight_requests: window,
        initial_inflight_requests: window,
        max_inflight_block_bytes: u64::MAX / 4,
        max_reorder_lookahead_bytes: u64::MAX / 4,
        request_timeout: Duration::from_secs(3600),
        initial_block_probe_requests: window,
        max_requests_without_block_progress: window,
        ..ZakuraBlockSyncConfig::default()
    };
    let peer = ZakuraPeerId::new(vec![7u8; 32]).expect("test peer id is within bounds");
    let cancel = CancellationToken::new();
    let capacity = usize::try_from(window).unwrap() + 16;
    let (out_send, outbound) = crate::zakura::framed_channel(capacity);
    let (_in_send, in_recv) = crate::zakura::framed_channel(16);
    let session = BlockSyncPeerSession::for_test(peer.clone(), out_send, cancel.clone());
    let registry = Arc::new(PeerRegistry::new());
    let generation = registry
        .admit_session(&peer, session.direction(), &config, 0, Instant::now())
        .generation();
    let budget = ByteBudget::new(config.max_inflight_block_bytes);
    let work = Arc::new(WorkQueue::new(block::Height(0), budget.clone()));
    let (sequencer_input_tx, bodies) = mpsc::channel(capacity);
    let (routine_to_reactor_tx, events) = mpsc::channel(capacity);
    let (_view_tx, view_rx) = watch::channel(super::super::sequencer_task::initial_view(
        super::super::BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
    ));
    let (requester, serving) = super::tests::test_authorization(
        &session,
        &in_recv,
        &config,
        pool,
        &CancellationToken::new(),
    );
    let mut routine = PeerRoutine::new(
        peer,
        0,
        session,
        in_recv,
        requester,
        serving,
        config.clone(),
        true,
        generation,
        budget,
        Arc::clone(&work),
        registry,
        Arc::new(std::sync::Mutex::new(ThroughputMeter::new(Instant::now()))),
        sequencer_input_tx,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        routine_to_reactor_tx,
        view_rx,
        cancel,
        crate::zakura::trace::ZakuraTrace::noop(),
    );
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(window),
        max_blocks_per_response: 1,
        max_inflight_requests: window,
        ..BlockSyncStatus::default()
    });

    let template: Arc<block::Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .expect("block vector parses"),
    );
    let blocks: Vec<_> = (1..=window)
        .map(|height| super::super::tests::fake_block_at_height(&template, block::Height(height)))
        .collect();
    work.set_estimate_floor_for_tests(100);
    work.extend(
        super::super::test_work_scope(),
        blocks.iter().map(|block| {
            (
                block.coinbase_height().unwrap(),
                block.hash(),
                // Small estimates keep the startup byte window above the request count.
                BlockSizeEstimate::Confirmed(100),
            )
        }),
    );
    Probe {
        routine,
        outbound,
        blocks,
        _bodies: bodies,
        _events: events,
    }
}

async fn deliver(routine: &mut PeerRoutine, message: BlockSyncMessage) {
    let frame = message.encode_frame().expect("test messages are encodable");
    routine
        .handle_frame(&mut block_sync_guard(), frame)
        .await
        .expect("the routine accepts the frame");
}

/// Exercise the production receipt future while the sequencer queue is full.
#[tokio::test(flavor = "current_thread")]
async fn receipt_settlement_survives_handoff_and_releases_on_cancel() {
    for (matched, cancel_handoff) in [(true, false), (true, true), (false, false), (false, true)] {
        let Probe {
            mut routine,
            mut outbound,
            blocks,
            _bodies,
            _events,
        } = probe(1);
        routine.try_fill().await;
        outbound.recv().await.unwrap();
        if !matched {
            let key = routine
                .window
                .outstanding_key_for_height(block::Height(1))
                .unwrap();
            routine.window.outstanding.remove(key).unwrap();
        }
        let mut budget = routine.budget.clone();
        let work = routine.work.clone();
        let charge = work.reserved_bytes();
        assert!(charge > 0);
        assert!(budget.try_reserve(17));
        let (sender, mut receiver) = mpsc::channel(1);
        let slot = sender.clone().reserve_owned().await.unwrap();
        routine.sequencer_input = sender;
        let resident = routine.sequencer_input_bytes.clone();
        let mut handoff =
            Box::pin(routine.handle_body(blocks[0].clone(), None, None, Arc::from([])));

        assert!(futures::poll!(&mut handoff).is_pending());
        assert_eq!(
            work.reserved_bytes(),
            0,
            "receipt ended the ledger reservation"
        );
        assert_eq!(
            budget.reserved(),
            charge + 17,
            "handoff still owns the charge"
        );
        assert!(resident.load(std::sync::atomic::Ordering::Relaxed) > 0);

        if cancel_handoff {
            drop(handoff);
            drop(slot);
            assert!(receiver.try_recv().is_err());
        } else {
            drop(slot);
            tokio::time::timeout(Duration::from_secs(1), handoff)
                .await
                .unwrap();
            assert_eq!(budget.reserved(), 17);
            drop(
                receiver
                    .try_recv()
                    .expect("completed handoff queued the body"),
            );
        }
        assert_eq!(resident.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(
            budget.reserved(),
            17,
            "settlement preserves unrelated charges"
        );
        drop(routine);
        assert_eq!(
            budget.reserved(),
            17,
            "routine cleanup cannot release twice"
        );
        budget.release(17);
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual scaling probe; run in release with --nocapture"]
#[allow(clippy::print_stderr)] // Report the per-event cost with --nocapture.
async fn request_bookkeeping_scaling() {
    use super::super::regulated::live_requester::SESSION_RESERVATIONS;

    // One session holds at most its pool share, so the largest window is that share.
    let largest = u32::try_from(SESSION_RESERVATIONS).expect("the session share fits u32");
    for window in [largest / 8, largest / 4, largest / 2, largest] {
        let Probe {
            mut routine,
            mut outbound,
            blocks,
            _bodies,
            _events,
        } = probe(window);

        let started = Instant::now();
        while routine.window.outstanding.len() < blocks.len() {
            let before = routine.window.outstanding.len();
            routine.try_fill().await;
            assert!(
                routine.window.outstanding.len() > before,
                "fill stalled at {before} of {window} requests: {:?}",
                routine.fill_stop_trace_at.keys().collect::<Vec<_>>()
            );
        }
        let fill = started.elapsed();
        routine.window.outstanding.assert_consistent();
        while outbound.try_recv().is_ok() {}

        let started = Instant::now();
        for block in &blocks {
            let start_height = block.coinbase_height().unwrap();
            deliver(&mut routine, BlockSyncMessage::Block(block.clone())).await;
            deliver(
                &mut routine,
                BlockSyncMessage::BlocksDone {
                    start_height,
                    returned: 1,
                },
            )
            .await;
            // The run loop refills after every frame.
            routine.try_fill().await;
        }
        let settle = started.elapsed();
        assert!(routine.window.outstanding.is_empty());
        assert_eq!(routine.registry.total_unreceived(), 0);

        eprintln!(
            "window={window:>6} fill={:>9.1?} ({:>7.2?}/request) settle={:>9.1?} ({:>7.2?}/response)",
            fill,
            fill / window,
            settle,
            settle / window,
        );
    }
}
