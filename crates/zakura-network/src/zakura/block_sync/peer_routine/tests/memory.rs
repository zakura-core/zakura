use super::*;
use crate::zakura::block_sync::sequencer_task::{SequencedBody, SequencerView};
use crate::zakura::BlockSyncMessage;
use crate::zakura::{
    block_sync::{events::RoutineToReactor, MSG_BS_GET_BLOCKS},
    regulation::{ConnectionResponseMemory, ResponseMemory},
    FramedRecv, FramedSend, SinkReject,
};

struct Fixture {
    routine: PeerRoutine,
    session: BlockSyncPeerSession,
    output: FramedRecv,
    _input: FramedSend,
    _view: watch::Sender<SequencerView>,
    _bodies: mpsc::Receiver<SequencedBody>,
    reports: mpsc::Receiver<RoutineToReactor>,
    work: Arc<WorkQueue>,
    budget: ByteBudget,
}

impl Fixture {
    fn new(memory: ConnectionResponseMemory, has_work: bool) -> Self {
        let budget = ByteBudget::new(1_000_000);
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        if has_work {
            work.extend(
                crate::zakura::block_sync::test_work_scope(),
                [(
                    block::Height(1),
                    block::Hash([1; 32]),
                    BlockSizeEstimate::Confirmed(1_000),
                )],
            );
        }
        let cancel = CancellationToken::new();
        let (send, output) = framed_channel(4);
        let (input, recv) = framed_channel(4);
        let peer = ZakuraPeerId::new(vec![27; 32]).unwrap();
        let session = BlockSyncPeerSession::for_test(peer.clone(), send, cancel.clone())
            .with_response_memory_for_test(memory);
        let (bodies, body_rx) = mpsc::channel(4);
        let (reports, report_rx) = mpsc::channel(4);
        let (view, view_rx) = watch::channel(initial_view(BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        }));
        let mut routine = PeerRoutine::new(
            peer,
            0,
            session.clone(),
            recv,
            ZakuraBlockSyncConfig::default(),
            true,
            0,
            budget.clone(),
            work.clone(),
            Arc::new(PeerRegistry::new()),
            Arc::new(Mutex::new(ThroughputMeter::new(Instant::now()))),
            bodies,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            reports,
            view_rx,
            cancel,
            ZakuraTrace::noop(),
        );
        routine.received_status = true;
        routine.servable_low = block::Height(1);
        routine.servable_high = block::Height(1);
        Self {
            routine,
            session,
            output,
            _input: input,
            _view: view,
            _bodies: body_rx,
            reports: report_rx,
            work,
            budget,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn metadata_capacity_reduces_the_batch_before_taking_work() {
    let node = ResponseMemory::new(1024, 1024);
    let mut f = Fixture::new(node.connection(), true);
    f.routine.max_blocks_per_response = 128;
    f.routine.config.max_blocks_per_response = 128;
    f.routine.servable_high = block::Height(128);
    f.work.extend(
        crate::zakura::block_sync::test_work_scope(),
        (2u8..=128).map(|height| {
            (
                block::Height(u32::from(height)),
                block::Hash([height; 32]),
                BlockSizeEstimate::Confirmed(1_000),
            )
        }),
    );
    let preferred = f.routine.request_count_cap();
    assert_eq!(preferred, 128);
    let (funded_count, reservation) = f.routine.authorize_request_metadata().unwrap();
    assert!(funded_count > 0 && funded_count < preferred);
    assert!(node.reserved_for_test() <= 1024);
    drop(reservation);
    assert_eq!(node.reserved_for_test(), 0);
    f.routine.try_fill().await;
    let frame = f.output.try_recv().unwrap();
    let BlockSyncMessage::GetBlocks { count, .. } = BlockSyncMessage::decode_frame(frame).unwrap()
    else {
        panic!("the funded request must be GetBlocks");
    };
    assert!(usize::try_from(count).unwrap() <= funded_count);
    assert!(node.reserved_for_test() > 0);
    assert!(node.reserved_for_test() <= 1024);
    assert_eq!(f.work.pending_len() + f.work.in_flight_len(), 128);
    assert!(!f.session.connection_is_closed_for_test());
    assert!(!f.session.cancel_token().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn metadata_exhaustion_preserves_work_and_wakes_on_another_connection_release() {
    let node = ResponseMemory::new(4096, 4096);
    let other_connection = node.connection();
    let held = other_connection.try_reserve(4096).unwrap();
    let mut f = Fixture::new(node.connection(), true);
    f.routine.try_fill().await;
    assert!(f.routine.response_memory_waiting);
    assert_eq!(f.work.pending_len(), 1);
    assert_eq!(f.work.in_flight_len(), 0);
    assert_eq!(f.budget.reserved(), 0);
    assert!(f.output.try_recv().is_err());
    let running = tokio::spawn(f.routine.run());
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(!running.is_finished());
    assert!(!f.session.connection_is_closed_for_test());
    assert!(!f.session.cancel_token().is_cancelled());
    while let Ok(report) = f.reports.try_recv() {
        assert!(!matches!(report, RoutineToReactor::Misbehavior { .. }));
    }
    drop(held);
    let request = timeout(Duration::from_secs(1), f.output.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.message_type, u16::from(MSG_BS_GET_BLOCKS));
    assert_eq!(f.work.pending_len(), 0);
    assert!(f.budget.reserved() > 0);
    f.session.cancel_token().cancel();
    let result = timeout(Duration::from_secs(1), running)
        .await
        .unwrap()
        .unwrap();
    assert!(!matches!(result, Err(SinkReject::Protocol(_))));
    assert!(other_connection.try_reserve(4096).is_some());
}

#[tokio::test(start_paused = true)]
async fn idle_receiver_does_not_spin_on_its_own_provisional_memory_release() {
    let node = ResponseMemory::new(4096, 4096);
    let f = Fixture::new(node.connection(), false);
    let running = tokio::spawn(f.routine.run());
    // An idle fill may prepare and return a record. Its release must not cause
    // another immediate fill forever, preventing this timer from advancing.
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(!running.is_finished());
    assert!(!f.session.cancel_token().is_cancelled());
    f.session.cancel_token().cancel();
    assert!(timeout(Duration::from_secs(1), running)
        .await
        .unwrap()
        .unwrap()
        .is_ok());
    assert!(node.connection().try_reserve(4096).is_some());
}
