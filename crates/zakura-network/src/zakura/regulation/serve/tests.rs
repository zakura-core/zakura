//! Serve's properties, one test each, then an operation-sequence proptest.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Wake, Waker},
    time::Duration,
};

use tokio::sync::Semaphore;
use zakura_test::execution::ExecutionProbe;

use super::*;
use crate::zakura::regulation::OutputByteBudget;
use crate::zakura::{
    framed_channel,
    regulation::{
        test_family::{decode, Probe, GET},
        SlotBudget,
    },
    FramedRecv,
};

mod sequences;

/// What one test request makes `produce` do.
#[derive(Clone, Debug, Default)]
pub(super) struct Job {
    /// Parts to send before the ending.
    pub(super) parts: u32,
    /// Bytes of each part.
    pub(super) part_len: usize,
    /// Wait for a permit here before producing.
    pub(super) gate: Option<Arc<Semaphore>>,
    /// Run a blocking operation under this probe first.
    pub(super) blocking: Option<Arc<ExecutionProbe>>,
    /// Let the blocking operation outlive `produce`.
    pub(super) detach_blocking: bool,
    /// Fail locally after this many parts.
    pub(super) fail_after: Option<u32>,
    /// Return from `produce` after the ending and hold this gate, so the
    /// ending is queued while execution continues.
    pub(super) hold_after: Option<Arc<Semaphore>>,
    panic_cap: bool,
    panic_produce: bool,
}

/// A `Produce` that follows each request's [`Job`].
#[derive(Debug, Default)]
pub(super) struct Scripted;

impl Produce for Scripted {
    type Request = Job;
    type Message = Probe;

    fn response_cap(&self, job: &Job) -> ResponseCap {
        assert!(!job.panic_cap, "injected dispatch panic");
        ResponseCap {
            frames: job.parts,
            // A part's payload is its bytes plus a one-byte count; the ending
            // is four bytes.
            bytes: u64::from(job.parts) * (job.part_len as u64 + 1) + 4,
        }
    }

    async fn produce(
        &self,
        job: &Job,
        lease: WorkLease,
        mut sink: ResponseSink<Probe>,
    ) -> Result<Responded, ServeEnd> {
        assert!(!job.panic_produce, "injected producer panic");
        if let Some(probe) = job.blocking.clone() {
            let lease = lease.clone();
            let work = tokio::task::spawn_blocking(move || {
                let _lease = lease;
                probe.start().finish();
            });
            if !job.detach_blocking {
                work.await.expect("the blocking operation does not panic");
            }
        }
        if let Some(gate) = &job.gate {
            gate.acquire()
                .await
                .expect("test gates are never closed")
                .forget();
        }
        if lease.is_cancelled() {
            return Err(ServeEnd::Cancelled);
        }
        for part in 0..job.parts {
            if job.fail_after == Some(part) {
                return Err(ServeEnd::LocalFault("scripted failure".into()));
            }
            sink.send(&Probe::Part(vec![7; job.part_len]))
                .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        }
        let responded = sink
            .finish(&Probe::Done(job.parts))
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        if let Some(gate) = &job.hold_after {
            gate.acquire()
                .await
                .expect("test gates are never closed")
                .forget();
        }
        Ok(responded)
    }

    fn local_failure(&self, _job: &Job, sent: SinkProgress) -> Probe {
        Probe::Failed(sent.frames)
    }
}

pub(super) const LIMITS: ServeLimits = ServeLimits {
    peer_output_responses: 128,
    node_output_responses: 256,
    node_execution: 2,
    peer_execution: 2,
    peer_output_bytes: 1 << 20,
    node_output_bytes: 1 << 20,
};

pub(super) fn capacity(limits: ServeLimits) -> ServeCapacity {
    ServeCapacity::new("test", &GET, limits).expect("the test limits are valid")
}

pub(super) fn peer(n: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![n; 32]).expect("32 bytes is a valid peer id")
}

/// One served session, its output, and its cancellation.
pub(super) struct Session {
    pub(super) serve: Serve<Scripted>,
    pub(super) output: FramedRecv,
    pub(super) cancel: CancellationToken,
}

pub(super) fn session(capacity: &ServeCapacity, peer_n: u8, advertised: u32) -> Session {
    let (send, output) = framed_channel(64);
    let cancel = CancellationToken::new();
    Session {
        serve: capacity.session(
            Arc::new(Scripted),
            &peer(peer_n),
            advertised,
            send,
            cancel.clone(),
            cancel.clone(),
            crate::zakura::CloseCause::default(),
        ),
        output,
        cancel,
    }
}

/// The next message the session writes.
pub(super) async fn next(output: &mut FramedRecv) -> Probe {
    let frame = tokio::time::timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("the session writes within five seconds")
        .expect("the session's output stays open");
    decode(&frame)
}

/// Let every spawned task run until it waits.
pub(super) async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

fn job() -> Job {
    Job::default()
}

fn gated(gate: &Arc<Semaphore>) -> Job {
    Job {
        gate: Some(gate.clone()),
        ..job()
    }
}

#[tokio::test]
async fn a_request_ends_with_its_parts_then_one_ending() {
    let capacity = capacity(LIMITS);
    let mut session = session(&capacity, 1, 4);
    session
        .serve
        .admit(Job {
            parts: 2,
            part_len: 3,
            ..job()
        })
        .unwrap();
    assert_eq!(next(&mut session.output).await, Probe::Part(vec![7; 3]));
    assert_eq!(next(&mut session.output).await, Probe::Part(vec![7; 3]));
    assert_eq!(next(&mut session.output).await, Probe::Done(2));
    settle().await;
    assert!(
        session.output.try_recv().is_err(),
        "one ending, then nothing"
    );
    assert_eq!(session.serve.open(), 0);
    assert_eq!(capacity.node_output_held(), 0);
    assert_eq!(capacity.node_execution_held(), 0);
}

#[tokio::test]
async fn output_is_whole_responses_in_admission_order() {
    let capacity = capacity(LIMITS);
    let mut session = session(&capacity, 1, 4);
    let first = Arc::new(Semaphore::new(0));
    session
        .serve
        .admit(Job {
            parts: 1,
            part_len: 1,
            ..gated(&first)
        })
        .unwrap();
    session
        .serve
        .admit(Job {
            parts: 1,
            part_len: 2,
            ..job()
        })
        .unwrap();
    // The second response is produced first but waits behind the first.
    settle().await;
    assert!(session.output.try_recv().is_err());
    first.add_permits(1);
    assert_eq!(next(&mut session.output).await, Probe::Part(vec![7; 1]));
    assert_eq!(next(&mut session.output).await, Probe::Done(1));
    assert_eq!(next(&mut session.output).await, Probe::Part(vec![7; 2]));
    assert_eq!(next(&mut session.output).await, Probe::Done(1));
}

#[tokio::test]
async fn peak_running_produce_steps_stay_within_node_slots() {
    let capacity = capacity(LIMITS);
    let probe = ExecutionProbe::new(true, false);
    let _release = probe.release_on_drop();
    let mut sessions: Vec<_> = (1..=3).map(|n| session(&capacity, n, 4)).collect();
    for session in &sessions {
        for _ in 0..4 {
            session
                .serve
                .admit(Job {
                    blocking: Some(probe.clone()),
                    ..job()
                })
                .unwrap();
        }
    }
    probe.wait_started(LIMITS.node_execution).await;
    settle().await;
    assert_eq!(probe.snapshot().running, LIMITS.node_execution);
    probe.release();
    for session in &mut sessions {
        for _ in 0..4 {
            assert_eq!(next(&mut session.output).await, Probe::Done(0));
        }
    }
    assert_eq!(probe.snapshot().peak_running, LIMITS.node_execution);
    assert_eq!(probe.snapshot().finished, 12);
}

#[tokio::test]
async fn exactly_limit_open_requests_never_fault_when_resent_at_each_ending() {
    let capacity = capacity(LIMITS);
    let mut session = session(&capacity, 1, 3);
    for _ in 0..3 {
        session.serve.admit(job()).unwrap();
    }
    for _ in 0..100 {
        for _ in 0..3 {
            assert_eq!(next(&mut session.output).await, Probe::Done(0));
            session.serve.admit(job()).unwrap();
        }
    }
    assert_eq!(capacity.over_limit_count(), 0);
}

#[tokio::test]
async fn requests_within_twice_the_limit_are_served_and_traced_and_one_more_faults() {
    let capacity = capacity(LIMITS);
    let mut session = session(&capacity, 1, 3);
    let gate = Arc::new(Semaphore::new(0));
    for _ in 0..6 {
        session.serve.admit(gated(&gate)).unwrap();
    }
    assert_eq!(capacity.over_limit_count(), 3);
    assert_eq!(
        session.serve.admit(gated(&gate)),
        Err(ServeViolation::OverCommitted { open: 7, limit: 3 })
    );
    gate.add_permits(6);
    for _ in 0..6 {
        assert_eq!(next(&mut session.output).await, Probe::Done(0));
    }
}

#[tokio::test]
async fn the_enforced_limit_is_the_highest_advertised() {
    let capacity = capacity(LIMITS);
    let session = session(&capacity, 1, 1);
    session.serve.advertise(3);
    session.serve.advertise(1);
    let gate = Arc::new(Semaphore::new(0));
    for _ in 0..6 {
        session.serve.admit(gated(&gate)).unwrap();
    }
    assert!(session.serve.admit(gated(&gate)).is_err());
    // The row's maximum caps any advertisement.
    session.serve.advertise(u32::MAX);
    for _ in 0..2 {
        session.serve.admit(gated(&gate)).unwrap();
    }
    assert!(session.serve.admit(gated(&gate)).is_err());
}

#[tokio::test]
async fn a_retired_sessions_running_jobs_do_not_count_against_the_next_session() {
    let capacity = capacity(LIMITS);
    let gate = Arc::new(Semaphore::new(0));
    let old = session(&capacity, 1, 2);
    for _ in 0..4 {
        old.serve.admit(gated(&gate)).unwrap();
    }
    settle().await;
    old.cancel.cancel();
    drop(old.serve);
    let mut new = session(&capacity, 1, 2);
    for _ in 0..4 {
        new.serve.admit(job()).unwrap();
    }
    // The old jobs still hold the peer's execution slots, which the new
    // session shares, so its jobs wait; they never fault.
    settle().await;
    assert_eq!(capacity.peer_held(&peer(1)).0, 2);
    gate.add_permits(4);
    for _ in 0..4 {
        assert_eq!(next(&mut new.output).await, Probe::Done(0));
    }
}

#[tokio::test]
async fn cancellation_lets_produce_finish_before_its_slot_returns() {
    let capacity = capacity(LIMITS);
    let gate = Arc::new(Semaphore::new(0));
    let session = session(&capacity, 1, 4);
    session.serve.admit(gated(&gate)).unwrap();
    settle().await;
    session.cancel.cancel();
    settle().await;
    assert_eq!(
        capacity.node_execution_held(),
        1,
        "cancellation does not abort produce"
    );
    gate.add_permits(1);
    settle().await;
    assert_eq!(capacity.node_execution_held(), 0);
}

#[tokio::test]
async fn cancellation_keeps_the_node_slot_until_the_work_ends() {
    let capacity = capacity(LIMITS);
    let probe = ExecutionProbe::new(true, false);
    let _release = probe.release_on_drop();
    let session = session(&capacity, 1, 4);
    session
        .serve
        .admit(Job {
            blocking: Some(probe.clone()),
            ..job()
        })
        .unwrap();
    probe.wait_started(1).await;
    session.cancel.cancel();
    settle().await;
    assert_eq!(capacity.node_execution_held(), 1);
    probe.release();
    probe.wait_finished(1).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while capacity.node_execution_held() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the slot frees once the blocking operation ends");
}

#[tokio::test]
async fn detached_work_keeps_peer_capacity_across_reconnect_after_output_drains() {
    let capacity = capacity(ServeLimits {
        peer_execution: 1,
        node_execution: 2,
        ..LIMITS
    });
    let probe = ExecutionProbe::new(true, false);
    let _release = probe.release_on_drop();
    let mut old = session(&capacity, 1, 4);
    old.serve
        .admit(Job {
            blocking: Some(probe.clone()),
            detach_blocking: true,
            ..job()
        })
        .unwrap();
    probe.wait_started(1).await;
    assert_eq!(next(&mut old.output).await, Probe::Done(0));
    old.cancel.cancel();
    drop(old);
    settle().await;
    assert_eq!(capacity.active_and_waiting(), (0, 0));
    assert_eq!(capacity.node_output_held(), 0);
    assert_eq!(capacity.node_output_responses.reserved(), 0);
    assert_eq!(capacity.node_execution_held(), 1);
    assert_eq!(probe.snapshot().running, 1);

    let mut new = session(&capacity, 1, 4);
    new.serve.admit(job()).unwrap();
    settle().await;
    assert_eq!(
        capacity.active_and_waiting(),
        (0, 1),
        "the reconnect waits on the old work's peer slot despite a free node slot"
    );
    assert!(new.output.try_recv().is_err());

    probe.release();
    probe.wait_finished(1).await;
    assert_eq!(next(&mut new.output).await, Probe::Done(0));
    settle().await;
    assert_eq!(capacity.node_execution_held(), 0);
    assert_eq!(capacity.node_output_held(), 0);
}

/// Record how many node slots are free when the peer slot's waiter wakes.
struct ObserveNode {
    node: SlotBudget,
    free_at_wake: AtomicUsize,
}

impl Wake for ObserveNode {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let free = usize::from(self.node.try_reserve().is_some());
        self.free_at_wake.store(free, Ordering::SeqCst);
    }
}

#[test]
fn the_node_slot_returns_before_the_peer_slot() {
    // #976's regression seed: a waiter that the freed peer slot admits must
    // find the node slot already returned.
    let peer = SlotBudget::new(1).unwrap();
    let node = SlotBudget::new(1).unwrap();
    let slots = ExecutionSlots::new(
        peer.try_reserve().unwrap(),
        node.try_reserve().unwrap(),
        PeerBudgets {
            output_responses: SlotBudget::new(1).unwrap(),
            execution: peer.clone(),
            output: OutputByteBudget::new(1).unwrap(),
        },
    );
    let observer = Arc::new(ObserveNode {
        node: node.clone(),
        free_at_wake: AtomicUsize::new(usize::MAX),
    });
    let waker = Waker::from(observer.clone());
    let mut waiting = Box::pin(peer.reserve());
    assert!(waiting
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    drop(slots);
    assert_eq!(observer.free_at_wake.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_non_reading_peer_holds_output_bytes_but_no_execution_slot() {
    let capacity = capacity(LIMITS);
    // A one-frame output queue that nobody reads.
    let (send, _output) = framed_channel(1);
    let cancel = CancellationToken::new();
    let serve = capacity.session(
        Arc::new(Scripted),
        &peer(1),
        4,
        send,
        cancel.clone(),
        cancel.clone(),
        crate::zakura::CloseCause::default(),
    );
    for _ in 0..4 {
        serve
            .admit(Job {
                parts: 3,
                part_len: 64,
                ..job()
            })
            .unwrap();
    }
    settle().await;
    assert_eq!(capacity.node_execution_held(), 0);
    assert!(capacity.node_output_held() > 0);
    assert_eq!(serve.open(), 0, "queued endings free their commitments");
    cancel.cancel();
}

#[tokio::test]
async fn output_bytes_return_only_when_the_last_frame_is_written() {
    let capacity = capacity(LIMITS);
    let mut session = session(&capacity, 1, 4);
    session
        .serve
        .admit(Job {
            parts: 2,
            part_len: 8,
            ..job()
        })
        .unwrap();
    settle().await;
    // Every frame sits in the transport queue; none is written.
    assert_eq!(capacity.node_execution_held(), 0);
    assert!(capacity.node_output_held() > 0);
    next(&mut session.output).await;
    next(&mut session.output).await;
    assert!(capacity.node_output_held() > 0, "the ending is unwritten");
    assert_eq!(capacity.node_output_responses.reserved(), 1);
    next(&mut session.output).await;
    assert_eq!(capacity.node_output_held(), 0);
    assert_eq!(capacity.peer_held(&peer(1)), (0, 0));
    assert_eq!(capacity.node_output_responses.reserved(), 0);
}

/// Tiny responses must stop on bookkeeping capacity before encoded bytes fill.
#[tokio::test]
async fn tiny_responses_with_a_blocked_writer_hold_response_slots() {
    let capacity = capacity(ServeLimits {
        peer_output_responses: 32,
        node_output_responses: 64,
        peer_output_bytes: 64 * 1024,
        node_output_bytes: 64 * 1024,
        ..LIMITS
    });
    let (send, output) = framed_channel(1);
    let cancel = CancellationToken::new();
    let mut session = Session {
        serve: capacity.session(
            Arc::new(Scripted),
            &peer(1),
            4,
            send,
            cancel.clone(),
            cancel.clone(),
            crate::zakura::CloseCause::default(),
        ),
        output,
        cancel,
    };
    for _ in 0..32 {
        session.serve.admit(job()).unwrap();
        settle().await;
        assert_eq!(session.serve.open(), 0);
    }
    let peer_budgets = capacity.peer(&peer(1));
    assert_eq!(capacity.node_output_responses.reserved(), 32);
    assert_eq!(peer_budgets.output_responses.reserved(), 32);
    assert_eq!(capacity.node_output_held(), 32 * 12);
    assert_eq!(capacity.node_execution_held(), 0);

    // The reader still admits the bounded commitment margin without waiting.
    for _ in 0..8 {
        session.serve.admit(job()).unwrap();
        settle().await;
    }
    assert_eq!(session.serve.open(), 8);
    assert_eq!(capacity.node_output_held(), 32 * 12);
    assert!(!session.cancel.is_cancelled());

    // Completing one write lets exactly one waiting response run.
    assert_eq!(next(&mut session.output).await, Probe::Done(0));
    settle().await;
    assert_eq!(session.serve.open(), 7);
    assert_eq!(capacity.node_output_responses.reserved(), 32);
    assert_eq!(capacity.node_output_held(), 32 * 12);

    session.cancel.cancel();
    drop(session);
    settle().await;
    assert_eq!(capacity.node_output_responses.reserved(), 0);
    assert_eq!(peer_budgets.output_responses.reserved(), 0);
    assert_eq!(capacity.node_output_held(), 0);
}

#[tokio::test]
async fn response_slots_bound_all_peers_and_survive_session_replacement() {
    let capacity = capacity(ServeLimits {
        peer_output_responses: 2,
        node_output_responses: 3,
        ..LIMITS
    });
    let old = session(&capacity, 1, 4);
    for _ in 0..2 {
        old.serve.admit(job()).unwrap();
        settle().await;
    }
    old.cancel.cancel();
    settle().await;
    // The transport still owns the old session's two unwritten endings.
    let replacement = session(&capacity, 1, 4);
    replacement.serve.admit(job()).unwrap();
    let mut other = session(&capacity, 2, 4);
    other.serve.admit(job()).unwrap();
    settle().await;
    assert_eq!(replacement.serve.open(), 1);
    assert_eq!(other.serve.open(), 0);
    assert_eq!(capacity.node_output_responses.reserved(), 3);
    other.serve.admit(job()).unwrap();
    settle().await;
    assert_eq!(other.serve.open(), 1);
    assert_eq!(capacity.node_output_held(), 3 * 12);
    next(&mut other.output).await;
    settle().await;
    assert_eq!(other.serve.open(), 0);
    drop(old);
    settle().await;
    assert_eq!(replacement.serve.open(), 0);
    replacement.cancel.cancel();
    other.cancel.cancel();
    drop(replacement);
    drop(other);
    settle().await;
    assert_eq!(capacity.node_output_responses.reserved(), 0);
}

#[tokio::test]
async fn a_retired_session_keeps_its_peer_budget_until_output_is_written() {
    let capacity = capacity(LIMITS);
    let Session {
        serve,
        mut output,
        cancel,
    } = session(&capacity, 1, 4);
    serve.admit(job()).unwrap();
    settle().await;
    cancel.cancel();
    drop(serve);
    settle().await;
    let held = capacity.node_output_held();
    assert!(held > 0, "the ending remains in the transport queue");
    assert_eq!(
        capacity.peer_held(&peer(1)),
        (0, held),
        "a reconnect must share the budget of the unwritten ending"
    );
    assert_eq!(next(&mut output).await, Probe::Done(0));
    assert!(output.recv().await.is_none());
    assert_eq!(capacity.peer_held(&peer(1)), (0, 0));
}

#[tokio::test]
async fn a_stalled_produce_on_one_peer_does_not_block_another() {
    let capacity = capacity(ServeLimits {
        peer_execution: 1,
        ..LIMITS
    });
    let gate = Arc::new(Semaphore::new(0));
    let stalled = session(&capacity, 1, 4);
    stalled.serve.admit(gated(&gate)).unwrap();
    stalled.serve.admit(gated(&gate)).unwrap();
    let mut other = session(&capacity, 2, 4);
    other.serve.admit(job()).unwrap();
    assert_eq!(next(&mut other.output).await, Probe::Done(0));
    gate.add_permits(2);
}

#[tokio::test]
async fn a_local_failure_after_a_prefix_sends_the_failure_ending() {
    let capacity = capacity(LIMITS);
    let mut session = session(&capacity, 1, 4);
    session
        .serve
        .admit(Job {
            parts: 3,
            part_len: 1,
            fail_after: Some(2),
            ..job()
        })
        .unwrap();
    assert_eq!(next(&mut session.output).await, Probe::Part(vec![7]));
    assert_eq!(next(&mut session.output).await, Probe::Part(vec![7]));
    assert_eq!(next(&mut session.output).await, Probe::Failed(2));
    // The session stays usable.
    session.serve.admit(job()).unwrap();
    assert_eq!(next(&mut session.output).await, Probe::Done(0));
    assert!(!session.cancel.is_cancelled());
}

#[tokio::test]
async fn the_ending_frees_the_commitment_before_execution_ends() {
    let capacity = capacity(LIMITS);
    let session = session(&capacity, 1, 1);
    let hold = Arc::new(Semaphore::new(0));
    let completed = session
        .serve
        .admit_tracked(Job {
            hold_after: Some(hold.clone()),
            ..job()
        })
        .unwrap();
    settle().await;
    assert!(
        *completed.borrow(),
        "ending publication completes the tracked request before the producer exits"
    );
    assert_eq!(session.serve.open(), 0);
    assert_eq!(capacity.node_execution_held(), 1);
    hold.add_permits(1);
}

#[tokio::test]
async fn cancelling_a_session_frees_every_budget() {
    let capacity = capacity(LIMITS);
    let gate = Arc::new(Semaphore::new(0));
    let session = session(&capacity, 1, 4);
    for _ in 0..8 {
        session.serve.admit(gated(&gate)).unwrap();
    }
    settle().await;
    session.cancel.cancel();
    gate.add_permits(8);
    settle().await;
    assert_eq!(session.serve.open(), 0);
    assert_eq!(capacity.node_execution_held(), 0);
    assert_eq!(capacity.node_output_held(), 0);
    assert_eq!(capacity.peer_held(&peer(1)), (0, 0));
    assert_eq!(capacity.active_and_waiting(), (0, 0));
}

/// A sink for one `Get`, its queued frames, and its session's commitments.
fn sink(
    cap: ResponseCap,
) -> (
    ResponseSink<Probe>,
    mpsc::UnboundedReceiver<ResponseFrame>,
    Arc<Commitments>,
) {
    let (sink, queued, commitments, _core) = sink_and_core(cap);
    (sink, queued, commitments)
}

/// [`sink`], and the core its serving task would keep.
fn sink_and_core(
    cap: ResponseCap,
) -> (
    ResponseSink<Probe>,
    mpsc::UnboundedReceiver<ResponseFrame>,
    Arc<Commitments>,
    Arc<Mutex<SinkCore>>,
) {
    let budget = OutputByteBudget::new(1).unwrap();
    let grant =
        || futures::FutureExt::now_or_never(budget.grant(0)).expect("an empty grant never waits");
    let commitments = Arc::new(Commitments {
        open: AtomicU32::new(1),
        limit: AtomicU32::new(1),
    });
    let (frames, queued) = mpsc::unbounded_channel();
    let core = SinkCore::new(
        &GET,
        crate::zakura::regulation::test_family::RULES,
        cap,
        frames,
        Arc::new(ResponseGrants {
            _node: grant(),
            _node_response: SlotBudget::new(1).unwrap().try_reserve().unwrap(),
            _peer: grant(),
            _peer_response: SlotBudget::new(1).unwrap().try_reserve().unwrap(),
            _peer_budgets: capacity(LIMITS).peer(&peer(1)),
        }),
        Commitment(Some(commitments.clone()), None),
    );
    let core = Arc::new(Mutex::new(core));
    (ResponseSink::new(core.clone()), queued, commitments, core)
}

fn open(commitments: &Commitments) -> u32 {
    commitments.open.load(Ordering::Acquire)
}

#[test]
fn ending_publication_frees_the_count_before_a_peer_can_replace_its_request() {
    for tracked in [false, true] {
        let counts = Arc::new(Commitments {
            open: AtomicU32::new(1),
            limit: AtomicU32::new(1),
        });
        let (completed, receiver) = watch::channel(false);
        let mut old = Commitment(Some(counts.clone()), tracked.then_some(completed));
        let replacement = old
            .queue_ending(|| {
                // Publication can wake a writer on another thread before it returns.
                assert_eq!(counts.open.fetch_add(1, Ordering::AcqRel), 0);
                Ok::<_, ()>(Commitment(Some(counts.clone()), None))
            })
            .unwrap();
        if tracked {
            assert!(*receiver.borrow());
        }
        drop(old);
        assert_eq!(
            open(&counts),
            1,
            "the old commitment must not release twice"
        );
        drop(replacement);
        assert_eq!(open(&counts), 0);
    }
}

#[test]
fn failed_ending_publication_releases_its_commitment_once() {
    let (sink, queued, counts, core) = sink_and_core(ResponseCap {
        frames: 1,
        bytes: 64,
    });
    drop(queued);
    assert!(matches!(
        sink.finish(&Probe::Done(0)),
        Err(SinkError::Closed)
    ));
    assert_eq!(open(&counts), 0);
    drop(core);
    assert_eq!(
        open(&counts),
        0,
        "cleanup must not release an ended lease twice"
    );
}

#[test]
fn the_sink_refuses_messages_that_do_not_answer_the_request() {
    let (mut sink, mut queued, commitments) = sink(ResponseCap {
        frames: 4,
        bytes: 64,
    });
    for (message, refused) in [
        (
            Probe::Status(1),
            SinkError::NotAResponse { message_type: 5 },
        ),
        (Probe::Pong(1), SinkError::NotAResponse { message_type: 7 }),
        (Probe::Get(1), SinkError::NotAResponse { message_type: 1 }),
        (Probe::Done(1), SinkError::EndingOnSend { message_type: 3 }),
    ] {
        assert_eq!(sink.send(&message), Err(refused));
    }
    let (sink, _, _) = self::sink(ResponseCap {
        frames: 4,
        bytes: 64,
    });
    assert_eq!(
        sink.finish(&Probe::Part(vec![1])).unwrap_err(),
        SinkError::NotAnEnding { message_type: 2 }
    );
    assert!(queued.try_recv().is_err());
    assert_eq!(open(&commitments), 1);
}

#[test]
fn the_sink_keeps_room_for_the_largest_ending() {
    // Two one-byte parts take two bytes each; the ending takes four.
    let cap = ResponseCap {
        frames: 3,
        bytes: 2 + 2 + 4,
    };
    let (mut sink, mut queued, commitments, core) = sink_and_core(cap);
    sink.send(&Probe::Part(vec![1])).unwrap();
    sink.send(&Probe::Part(vec![1])).unwrap();
    // A third part fits the frame count, but not beside the ending.
    let sent = SinkProgress {
        frames: 2,
        bytes: 4,
    };
    assert_eq!(
        sink.send(&Probe::Part(vec![1])),
        Err(SinkError::OverCap { cap, sent })
    );
    assert_eq!(sink.progress(), sent);
    assert_eq!(open(&commitments), 1);
    sink.finish(&Probe::Done(2)).unwrap();
    assert_eq!(open(&commitments), 0, "the ending frees the commitment");
    // The serving task sees the ending and queues no failure ending.
    assert!(core.lock().unwrap().ended());
    let ends: Vec<_> = std::iter::from_fn(|| queued.try_recv().ok())
        .map(|frame| frame.ends)
        .collect();
    assert_eq!(ends, [false, false, true]);
}

#[test]
fn the_sink_refuses_frames_beyond_the_frame_budget() {
    let cap = ResponseCap {
        frames: 1,
        bytes: 1024,
    };
    let (mut sink, _queued, _) = sink(cap);
    sink.send(&Probe::Part(vec![1])).unwrap();
    assert!(matches!(
        sink.send(&Probe::Part(vec![1])),
        Err(SinkError::OverCap { .. })
    ));
}

#[test]
fn a_cap_below_the_ending_is_raised_to_hold_it() {
    for bytes in [0, 1, 3, u64::MAX] {
        let (sink, _queued, commitments) = sink(ResponseCap { frames: 0, bytes });
        assert_eq!(sink.cap().bytes, bytes.max(4));
        sink.finish(&Probe::Failed(0)).unwrap();
        assert_eq!(open(&commitments), 0);
    }
}

#[test]
fn a_part_the_row_allows_but_the_cap_refuses_queues_nothing() {
    for bytes in [0, 1, 32, 64, 65 + 4] {
        let (mut sink, mut queued, _) = sink(ResponseCap { frames: 1, bytes });
        let result = sink.send(&Probe::Part(vec![1; 64]));
        assert_eq!(result.is_ok(), bytes >= 65 + 4, "cap of {bytes} bytes");
        assert_eq!(queued.try_recv().is_ok(), result.is_ok());
    }
}

#[tokio::test]
#[allow(clippy::print_stderr)] // Report the retained-byte measurement with --nocapture.
async fn queued_requests_allocate_only_commitments_until_dispatch() {
    const COUNT: u32 = 64_000;
    // Exercise the production protocol ceiling without starting a producer.
    const REQUEST: crate::zakura::MessageRule = crate::zakura::MessageRule {
        role: crate::zakura::MessageRole::Request {
            max_in_flight: COUNT / 2,
            cadence: None,
        },
        ..GET
    };
    let capacity = ServeCapacity::new("queued-memory", &REQUEST, LIMITS).unwrap();
    let Session {
        serve,
        output: _output,
        cancel,
    } = session(&capacity, 1, COUNT / 2);
    let (completions, allocations) = zakura_test::allocations::measure(|| {
        (0..COUNT)
            .map(|_| serve.admit_tracked(job()).unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(serve.open(), COUNT);
    // Includes the queue, tracked completion watches and retained receivers.
    // A response channel per queued job alone used several KiB per request.
    let ceiling = usize::try_from(COUNT).unwrap() * 512;
    assert!(allocations.retained_bytes < ceiling, "{allocations:?}");
    eprintln!("queued requests={COUNT}, {allocations:?}");
    cancel.cancel();
    settle().await;
    assert_eq!(serve.open(), 0, "cancelled jobs release their commitments");
    assert!(completions.iter().all(|completed| *completed.borrow()));
}

#[tokio::test]
async fn serving_output_keeps_a_queue_slot_for_control_messages() {
    let capacity = capacity(LIMITS);
    let (send, mut output) = framed_channel(4);
    let cancel = CancellationToken::new();
    let serve = capacity.session(
        Arc::new(Scripted),
        &peer(1),
        4,
        send.clone(),
        cancel.clone(),
        cancel.clone(),
        crate::zakura::CloseCause::default(),
    );
    serve
        .admit(Job {
            parts: 10,
            part_len: 1,
            ..job()
        })
        .unwrap();
    settle().await;
    let control = crate::zakura::wire_codec::encode_frame(&Probe::Ping(7)).unwrap();
    send.try_send(control)
        .expect("serving must leave room for our control traffic");
    let mut messages = Vec::new();
    for _ in 0..12 {
        messages.push(next(&mut output).await);
    }
    assert!(messages[..4].contains(&Probe::Ping(7)));
    assert_eq!(messages.last(), Some(&Probe::Done(10)));
    cancel.cancel();
}

#[tokio::test]
async fn serving_panics_close_the_connection_even_with_a_blocked_writer() {
    for panic_cap in [true, false] {
        let capacity = capacity(LIMITS);
        let (send, output) = framed_channel(1);
        let cancel = CancellationToken::new();
        let connection = CancellationToken::new();
        let close_cause = crate::zakura::CloseCause::default();
        let serve = capacity.session(
            Arc::new(Scripted),
            &peer(1),
            4,
            send.clone(),
            cancel.clone(),
            connection.clone(),
            close_cause.clone(),
        );
        // The first frame occupies the only queue slot. The writer waits on
        // the rest of this response and cannot observe dispatch's channel closing.
        serve
            .admit(Job {
                parts: 3,
                part_len: 8,
                ..job()
            })
            .unwrap();
        settle().await;
        assert_eq!(send.capacity(), 0);
        assert!(!connection.is_cancelled());
        serve
            .admit(Job {
                panic_cap,
                panic_produce: !panic_cap,
                ..job()
            })
            .unwrap();
        serve.admit(job()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), connection.cancelled())
            .await
            .unwrap();
        assert!(cancel.is_cancelled());
        assert_eq!(close_cause.get_or("cancelled"), "service_panic");
        settle().await;
        assert_eq!(serve.open(), 0);
        drop(output);
        settle().await;
        assert_eq!(capacity.node_execution_held(), 0);
        assert_eq!(capacity.node_output_held(), 0);
        assert_eq!(capacity.node_output_responses.reserved(), 0);
        let mut healthy = session(&capacity, 2, 4);
        healthy.serve.admit(job()).unwrap();
        assert!(matches!(next(&mut healthy.output).await, Probe::Done(0)));
        assert!(!healthy.cancel.is_cancelled());
        healthy.cancel.cancel();
    }
}

/// A panic during writer cleanup follows the same connection-wide supervision path.
#[tokio::test]
async fn a_writer_panic_records_the_cause_and_cancels_the_connection() {
    #[derive(Debug)]
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("injected writer cleanup panic");
        }
    }
    let (send, output) = framed_channel(1);
    drop(output);
    let cancel = CancellationToken::new();
    let connection = CancellationToken::new();
    let close_cause = crate::zakura::CloseCause::default();
    let tasks = super::ServingTasks {
        peer: peer(1),
        cancel: cancel.clone(),
        connection: connection.clone(),
        close_cause: close_cause.clone(),
    };
    let (order, responses) = tokio::sync::mpsc::unbounded_channel();
    let (frames, response) = tokio::sync::mpsc::unbounded_channel();
    frames
        .send(super::ResponseFrame {
            frame: crate::zakura::wire_codec::encode_frame(&Probe::Done(0)).unwrap(),
            guard: crate::zakura::FrameGuard::new(Arc::new(PanicOnDrop)),
            ends: true,
        })
        .unwrap();
    order.send(response).unwrap();
    tasks.spawn(super::write_in_order(responses, send, cancel.clone()));
    tokio::time::timeout(Duration::from_secs(5), connection.cancelled())
        .await
        .unwrap();
    assert!(cancel.is_cancelled());
    assert_eq!(close_cause.get_or("cancelled"), "service_panic");
}

