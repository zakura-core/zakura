//! The controlled header port under the real scheduler. Each test stops a
//! task at every scheduling point it reaches and acts there as the run
//! may, which polled futures can't reach. Runs only in the simulator
//! build, `RUSTFLAGS="--cfg zakura_sim"`.
#![cfg(zakura_sim)]

use std::{
    future::{poll_fn, Future},
    panic::{catch_unwind, AssertUnwindSafe},
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
    controlled::{BindError, CompleteError, ControlledPort, Fact, FactKind, OpId},
    evidence::{OwnerModel, ResultIdentity},
    fixture::HeaderFixture,
    lease::LeaseEnd,
    HeaderOp, Outcome, Release,
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

/// Operations the competing task submits, more than any drop has points.
const COMPETING: u64 = 32;

/// Spawns a task that submits [`COMPETING`] membership reads and keeps
/// them pending, so the run always has a completion to pick. Runs it until
/// it waits. They get IDs 1 to `COMPETING`.
fn spawn_competing(sched: &mut Scheduler, port: &Arc<ControlledPort>, target: Frontier) {
    let caller = port.clone();
    let task = sched
        .spawn_root("competing", async move {
            let mut calls: Vec<_> = (0..COMPETING)
                .map(|_| caller.selected_chain_contains(target.hash))
                .collect();
            poll_fn(|cx| {
                for call in &mut calls {
                    assert!(call.as_mut().poll(cx).is_pending());
                }
                Poll::Ready(())
            })
            .await;
            zakura_rt::time::sleep(Duration::from_secs(1_000)).await;
        })
        .task();
    step(sched, task, |_| {});
}

/// A task drops an unread `Acquired` reply, after another acquire for the
/// same peer, completed through the controlled port, registered a capacity
/// waiter if `with_waiter`. At every
/// scheduling point the drop reaches, acts as the run may there: takes the
/// lease lock, checks the facts so far and the actual owner state against
/// an [`OwnerModel`], checks that a lease already gone has its
/// `ReplyDropped` recorded, lists pending operations, and completes a
/// competing operation. Returns what went wrong, and every event.
fn abandon_with_probes(with_waiter: bool) -> (Vec<String>, Vec<Fact>) {
    let mut sched = scheduler();
    let (port, target) = chain();
    spawn_competing(&mut sched, &port, target);
    let task = spawn_unread_acquire(&mut sched, &port, target);
    let unread = OpId(COMPETING + 1);
    let completion = port.complete(unread, Outcome::Engine).expect("completes");
    assert!(completion.delivered);
    let ResultIdentity::Acquired {
        lease: unread_lease,
        ..
    } = completion.result
    else {
        panic!("expected a lease, got {:?}", completion.result);
    };
    let waiter = with_waiter.then(|| {
        let mut call = port.acquire_header_path(acquire(&port, target));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(call.as_mut().poll(&mut cx).is_pending());
        port.complete(OpId(COMPETING + 2), Outcome::Engine)
            .expect("the refusal completes");
        match call.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(AcquirePathReply::CapacityBusy(signal))) => signal,
            other => panic!("expected CapacityBusy, got {other:?}"),
        }
    });

    sched.advance_to(SimInstant::from_start(Duration::from_secs(10)));
    let mut problems = Vec::new();
    let mut events = port.take_facts();
    let mut model = OwnerModel::new();
    assert_eq!(
        model.check_boundary(&events, &port.owner_state()),
        Ok(()),
        "the setup matches the model"
    );
    let mut competing = (1..=COMPETING).map(OpId);
    step(&mut sched, task, |at| {
        let Ok(live) = catch_unwind(AssertUnwindSafe(|| port.engine().live_leases())) else {
            problems.push(format!("the lease lock was held at {at}"));
            return;
        };
        let facts = port.take_facts();
        let state = port.owner_state();
        if let Err(failure) = model.check_boundary(&facts, &state) {
            problems.push(format!("the owner model failed with {failure:?} at {at}"));
        }
        events.extend(facts);
        let slot_held = state.operations.iter().any(|op| op.id == unread);
        let lease_held = state.leases.iter().any(|lease| lease.lease == unread_lease);
        if slot_held != lease_held {
            problems.push(format!(
                "the reply's slot and its lease changed apart at {at}"
            ));
        }
        let recorded = events
            .iter()
            .any(|fact| matches!(fact.kind, FactKind::ReplyDropped { .. }));
        if live == 0 && !recorded {
            problems.push(format!("the lease was gone before its record at {at}"));
        }
        if !listing_is_free(&port) {
            problems.push(format!("the bookkeeping lock was held at {at}"));
            return;
        }
        let id = competing.next().expect("enough competing operations");
        let completion = port.complete(id, Outcome::Engine).expect("completes");
        if !completion.lease_ends.is_empty() {
            problems.push(format!(
                "operation {} took lease ends {:?} at {at}",
                id.0, completion.lease_ends
            ));
        }
        let facts = port.take_facts();
        if let Err(failure) = model.check_boundary(&facts, &port.owner_state()) {
            problems.push(format!(
                "the owner model failed with {failure:?} after completing {} at {at}",
                id.0
            ));
        }
        events.extend(facts);
    });
    if !problems.is_empty() {
        // A task suspended with a lock held never releases it, so shutting
        // the run down would hang.
        std::mem::forget(sched);
        return (problems, events);
    }
    let facts = port.take_facts();
    if let Err(failure) = model.check_boundary(&facts, &port.owner_state()) {
        problems.push(format!(
            "the owner model failed with {failure:?} after the drop"
        ));
    }
    events.extend(facts);
    if let Some(waiter) = waiter {
        assert!(waiter.is_released(), "the waiter's signal fired");
    }
    (problems, events)
}

/// The capability ends of the unread acquire's `ReplyDropped` transaction.
fn reply_dropped_ends(facts: &[Fact]) -> Vec<LeaseEnd> {
    let txn = facts
        .iter()
        .find_map(|fact| match fact.kind {
            FactKind::ReplyDropped { id } if id == OpId(COMPETING + 1) => Some(fact.txn),
            _ => None,
        })
        .expect("the unread reply was dropped");
    facts
        .iter()
        .filter(|fact| fact.txn == txn)
        .filter_map(|fact| match &fact.kind {
            FactKind::Lease(lease) => LeaseEnd::of(lease),
            _ => None,
        })
        .collect()
}

/// C2: a port binds only inside an active run, here the scheduler's, only
/// while pristine, and only once.
#[test]
fn binding_needs_an_active_run() {
    let (port, target) = chain();
    let (leased, _) = chain();
    acquired(
        ready(
            leased
                .engine()
                .acquire_header_path(acquire(&leased, target)),
        )
        .expect("acquires"),
    );
    assert_eq!(port.bind_to_run(), Err(BindError::NotInRun));
    let sched = scheduler();
    assert_eq!(port.bind_to_run(), Ok(()));
    assert_eq!(port.bind_to_run(), Err(BindError::AlreadyBound));
    assert_eq!(leased.bind_to_run(), Err(BindError::NotPristine));
    drop(sched);
}

/// A port bound to a run refuses use outside it, after that run ends or in
/// a later run: a call polled then submits and publishes nothing, a
/// completion is refused, and the port reports the refusal, so its
/// evidence never carries another clock's instants.
#[test]
fn a_bound_port_refuses_use_outside_its_run() {
    for later in [false, true] {
        let (port, target) = chain();
        let sched = scheduler();
        assert_eq!(port.bind_to_run(), Ok(()));
        drop(sched);
        let other = later.then(scheduler);
        assert!(!port.refused_outside_run());
        let mut call = port.selected_chain_contains(target.hash);
        assert!(pin!(&mut call)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert!(port.take_facts().is_empty(), "later run: {later}");
        assert!(port.pending().is_empty());
        assert!(port.refused_outside_run());
        assert_eq!(
            port.complete(OpId(1), Outcome::Engine),
            Err(CompleteError::OutsideRun)
        );
        drop(call);
        drop(other);
    }
}

/// A call submitted and completed during its run, then taken or dropped
/// after that run ends or in a later run, is refused too: taking returns
/// nothing, dropping leaves the reply, and neither publishes.
#[test]
fn a_bound_port_refuses_a_reply_outside_its_run() {
    for later in [false, true] {
        for take in [false, true] {
            let (port, target) = chain();
            let sched = scheduler();
            assert_eq!(port.bind_to_run(), Ok(()));
            let mut call = port.selected_chain_contains(target.hash);
            assert!(pin!(&mut call)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
            assert!(port.complete(OpId(1), Outcome::Engine).is_ok());
            assert!(!port.take_facts().is_empty());
            drop(sched);
            let other = later.then(scheduler);
            if take {
                assert!(pin!(&mut call)
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending());
            }
            drop(call);
            assert!(port.take_facts().is_empty(), "later {later}, take {take}");
            assert!(port.refused_outside_run(), "later {later}, take {take}");
            drop(other);
        }
    }
}

/// #1721 review P1: abandoning an unread `Acquired` reply fires the capacity
/// signal another acquire waits on, a scheduling point. The run must still
/// be able to take the lease lock there, or its next completion panics
/// outside a task, so signals fire only after the lock is released.
#[test]
fn abandoning_with_a_capacity_waiter_never_suspends_under_a_lock() {
    let (problems, events) = abandon_with_probes(true);
    assert!(problems.is_empty(), "{problems:#?}");
    assert!(matches!(
        reply_dropped_ends(&events).as_slice(),
        [LeaseEnd {
            release: Release::LeaseAbandonedAcquire,
            ..
        }]
    ));
}

/// #1721 review P2: a completion the run picks while a task abandons an
/// unread `Acquired` reply never takes that lease's end, which stays with
/// the abandonment's `ReplyDropped`. And the run never sees the lease gone
/// before that record, so events follow state.
#[test]
fn an_abandoned_lease_end_stays_with_its_reply() {
    let (problems, events) = abandon_with_probes(false);
    assert!(problems.is_empty(), "{problems:#?}");
    assert!(matches!(
        reply_dropped_ends(&events).as_slice(),
        [LeaseEnd {
            release: Release::LeaseAbandonedAcquire,
            ..
        }]
    ));
    let completed = events
        .iter()
        .filter(|fact| {
            matches!(&fact.kind, FactKind::Completed(completion)
                if completion.op == HeaderOp::SelectedChainContains)
        })
        .count();
    assert!(completed > 0, "no competing completion ran during the drop");
}
