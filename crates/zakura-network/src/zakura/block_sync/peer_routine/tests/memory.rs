//! Keep work pending when response accounting is full, then wake on release.

use super::*;
use crate::zakura::block_sync::sequencer_task::{SequencedBody, SequencerView};
use crate::zakura::{
    block_sync::{events::RoutineToReactor, MSG_BS_GET_BLOCKS},
    regulation::{ConnectionResponseMemory, ResponseMemory, ResponseScope},
    FramedRecv, FramedSend, SinkReject,
};
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Wake, Waker},
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
            crate::zakura::block_sync::tests::mainnet_decoder(),
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
async fn metadata_exhaustion_preserves_work_and_wakes_on_another_connection_release() {
    let setup = ResponseMemory::node_setup_bytes_for_test()
        + 2 * ResponseMemory::setup_bytes_for_test()
        + ResponseScope::setup_bytes_for_test();
    let node = ResponseMemory::new(setup + 4096, setup + 4096);
    let other_connection = node.connection();
    let mut f = Fixture::new(node.connection(), true);
    let held = other_connection.try_reserve(4096).unwrap();
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
    let setup = ResponseMemory::node_setup_bytes_for_test()
        + ResponseMemory::setup_bytes_for_test()
        + ResponseScope::setup_bytes_for_test();
    let node = ResponseMemory::new(setup + 4096, setup + 4096);
    let memory = node.connection();
    let f = Fixture::new(memory.clone(), false);
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
    assert!(memory.try_reserve(4096).is_some());
}

/// Count scheduler wakeups, including ones that merely retry a still-full pool.
#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn connection_exhaustion_ignores_other_connections_memory_releases() {
    let setup = ResponseMemory::node_setup_bytes_for_test()
        + 2 * ResponseMemory::setup_bytes_for_test()
        + ResponseScope::setup_bytes_for_test();
    let connection_limit =
        ResponseMemory::setup_bytes_for_test() + ResponseScope::setup_bytes_for_test() + 4096;
    let node = ResponseMemory::new(setup + 8192, connection_limit);
    let memory = node.connection();
    let other_connection = node.connection();
    let mut f = Fixture::new(memory.clone(), true);
    let held = memory.try_reserve(4096).unwrap();
    let wake_count = Arc::new(WakeCount::default());
    let waker = Waker::from(wake_count.clone());
    let mut context = Context::from_waker(&waker);
    let running = f.routine.run();
    tokio::pin!(running);
    assert!(running.as_mut().poll(&mut context).is_pending());
    assert_eq!(f.work.pending_len(), 1);
    assert_eq!(f.budget.reserved(), 0);
    assert!(f.output.try_recv().is_err());
    wake_count.0.store(0, Ordering::SeqCst);

    // This connection has no room, but the node does. Completions elsewhere
    // cannot help it and must not schedule this real receiver for another poll.
    for _ in 0..32 {
        drop(other_connection.try_reserve(1024).unwrap());
        assert_eq!(
            wake_count.0.load(Ordering::SeqCst),
            0,
            "an unrelated metadata release must not wake a connection-full receiver"
        );
    }

    drop(held);
    assert!(wake_count.0.load(Ordering::SeqCst) > 0);
    assert!(running.as_mut().poll(&mut context).is_pending());
    let request = f.output.try_recv().unwrap();
    assert_eq!(request.message_type, u16::from(MSG_BS_GET_BLOCKS));
    assert_eq!(f.work.pending_len(), 0);
    assert!(f.budget.reserved() > 0);
    while let Ok(report) = f.reports.try_recv() {
        assert!(!matches!(report, RoutineToReactor::Misbehavior { .. }));
    }
    f.session.cancel_token().cancel();
    // The request was written above. Cancelling its unfinished response closes
    // the connection locally, without accusing the peer of a protocol fault.
    let completion = running.as_mut().poll(&mut context);
    assert!(
        matches!(completion, Poll::Ready(Err(SinkReject::Connection(_)))),
        "{completion:?}"
    );
}
