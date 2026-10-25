//! The controlled header port under the real scheduler. Each test stops a
//! task at every scheduling point it reaches and acts there as the run
//! may, which polled futures can't reach. Runs only in the simulator
//! build, `RUSTFLAGS="--cfg zakura_sim"`.
#![cfg(zakura_sim)]

use std::{
    future::{poll_fn, Future},
    pin::pin,
    sync::{mpsc, Arc},
    task::{Context, Poll, Waker},
    time::Duration,
};

use zakura_chain::block;
use zakura_header_chain::{Frontier, HeaderWorkAuthority, SourceId};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, ApplyHeaderTargetOutcome, Port, RetainedHeaderPath,
};
use zakura_scheduler::{At, Op, RunOutcome, Scheduler, SchedulerConfig, SimInstant, TaskId};
use zakura_storage::header::{
    controlled::{ControlledPort, OpId},
    fixture::HeaderFixture,
    Outcome,
};

/// Polls `future` once, outside any task. The engine port runs each
/// operation on its first poll.
fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the engine port completes each operation on its first poll"),
    }
}

/// A scheduler with 2 workers at virtual time zero.
fn scheduler() -> Scheduler {
    Scheduler::new(
        SchedulerConfig {
            workers: 2,
            ..SchedulerConfig::default()
        },
        SimInstant::ZERO,
    )
}

/// A controlled port whose engine holds one header on the anchor, and that
/// header's frontier.
fn chain() -> (Arc<ControlledPort>, Frontier) {
    let fixture = HeaderFixture::regtest();
    let port = Arc::new(ControlledPort::new(&fixture));
    let anchor = fixture.anchor();
    let headers = fixture.extend(anchor, 1, 0);
    let request = fixture.target_request(&port.engine().snapshot(), anchor, &headers, 1);
    let prepared = ready(port.engine().prepare_header_target(request)).expect("fixture prepares");
    assert_eq!(
        ready(port.engine().apply_header_target(prepared)).expect("fixture applies"),
        ApplyHeaderTargetOutcome::Applied
    );
    (port, Frontier::new(block::Height(1), headers[0].hash()))
}

/// Peer 1's acquire of the path from the anchor to `target`.
fn acquire(port: &ControlledPort, target: Frontier) -> AcquirePath {
    AcquirePath {
        source: SourceId::from_digest([1; 32]),
        session_id: 1,
        scope: HeaderWorkAuthority::for_target(&port.engine().snapshot(), target.hash),
        target_tip_hash: target.hash,
        locator_hashes: vec![HeaderFixture::regtest().anchor().hash],
    }
}

/// The leased path in `reply`.
fn acquired(reply: AcquirePathReply) -> RetainedHeaderPath {
    match reply {
        AcquirePathReply::Acquired(path) => *path,
        other => panic!("expected a lease, got {other:?}"),
    }
}

/// Spawns a task that submits peer 1's acquire of `target`, leaves its
/// reply unread for 10 s, then drops the call. Runs it until it waits.
fn spawn_unread_acquire(
    sched: &mut Scheduler,
    port: &Arc<ControlledPort>,
    target: Frontier,
) -> TaskId {
    let caller = port.clone();
    let task = sched
        .spawn_root("unread", async move {
            let mut call = caller.acquire_header_path(acquire(&caller, target));
            poll_fn(|cx| {
                assert!(call.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            zakura_rt::time::sleep(Duration::from_secs(10)).await;
            drop(call);
        })
        .task();
    step(sched, task, |_| {});
    task
}

/// Runs `task` until it isn't ready, calling `probe` at each scheduling
/// point it stops at. Returns those points.
fn step(sched: &mut Scheduler, task: TaskId, mut probe: impl FnMut(&At)) -> Vec<At> {
    let mut points = Vec::new();
    while sched.ready().iter().any(|ready| ready.task == task) {
        if let RunOutcome::Reached(at) = sched.run(task) {
            probe(&at);
            points.push(at);
        }
    }
    points
}

/// Whether the run could list pending operations now. It asks from another
/// thread, since a lock held by a suspended task on this one would hang it.
fn listing_is_free(port: &Arc<ControlledPort>) -> bool {
    let (done, finished) = mpsc::channel();
    let port = port.clone();
    std::thread::spawn(move || {
        port.pending();
        let _ = done.send(());
    });
    finished.recv_timeout(Duration::from_secs(5)).is_ok()
}

/// #1718 review P1: a task dropping an unread `CapacityBusy` reply that holds
/// the last sender of its capacity signal reaches a scheduling point in that
/// sender's drop. The run must still be able to list pending operations
/// there, so the reply can't be dropped under the bookkeeping lock.
#[test]
fn dropping_an_unread_reply_never_suspends_under_the_bookkeeping_lock() {
    let mut sched = scheduler();
    let (port, target) = chain();
    let held = acquired(
        ready(port.engine().acquire_header_path(acquire(&port, target))).expect("acquires"),
    );
    let task = spawn_unread_acquire(&mut sched, &port, target);
    let completion = port.complete(OpId(1), Outcome::Engine).expect("completes");
    assert!(completion.delivered);
    // Releasing the lease fires and drops the registry's copy of the
    // signal, so the unread reply holds the last sender.
    ready(port.engine().release_header_path(held)).expect("releases");

    sched.advance_to(SimInstant::from_start(Duration::from_secs(10)));
    let mut blocked = None;
    let points = step(&mut sched, task, |at| {
        if blocked.is_none() && !listing_is_free(&port) {
            blocked = Some(at.to_string());
        }
    });
    if let Some(at) = blocked {
        // The suspended task never releases the lock, so shutting the run
        // down would hang.
        std::mem::forget(sched);
        panic!("the bookkeeping lock was held at {at}");
    }
    assert!(
        points.iter().any(|at| at.op == Op::Release),
        "the reply's drop reached no release point: {points:?}"
    );
}
