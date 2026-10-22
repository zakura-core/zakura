//! The fixture corpus from `scheduler.md`: planted races the scheduler must
//! find, fixed versions it must never flag, and the mechanics under them.

mod support;

use std::{
    cell::{Cell, RefCell},
    future::poll_fn,
    rc::Rc,
    task::{Poll, Waker},
};

use support::*;
use zakura_scheduler::{hooks, Scheduler, SchedulerConfig, SimInstant, Status, TaskExit};

/// Seeds to try when a planted race must be found.
const FIND: u64 = 2_000;
/// Seeds that must all pass on correct code.
const CLEAN: u64 = 5_000;

/// Two tasks load and then store one counter.
fn lost_update(s: &mut Scheduler) -> Check {
    let c = shared(Atomic::default());
    for label in ["inc_a", "inc_b"] {
        let c = c.clone();
        s.spawn_root(label, async move {
            let v = c.load();
            c.store(v + 1);
        });
    }
    Box::new(move |_| (c.peek() != 2).then(|| format!("counter = {}, want 2", c.peek())))
}

/// Check capacity under one lock, then reserve under a second lock.
fn snapshot_reserve(s: &mut Scheduler) -> Check {
    let avail = shared(Mutex::new(1i64));
    for label in ["res_a", "res_b"] {
        let avail = avail.clone();
        s.spawn_root(label, async move {
            let ok = *avail.lock() >= 1;
            if ok {
                *avail.lock() -= 1;
            }
        });
    }
    Box::new(move |_| {
        let v = *avail.lock();
        (v < 0).then(|| format!("over-committed: available = {v}"))
    })
}

/// Discovery's `wait_complete` shape: check a flag, then create `notified()`.
/// The fixed version registers before checking.
fn missed_notify(fixed: bool) -> impl Fn(&mut Scheduler) -> Check {
    move |s| {
        let flag = shared(Atomic::default());
        let notify = shared(Notify::default());
        let timed_out = shared(Cell::new(false));
        let (f, n, t) = (flag.clone(), notify.clone(), timed_out.clone());
        s.spawn_root("waiter", async move {
            let notified = if fixed {
                let mut w = n.notified();
                w.as_mut().enable();
                if f.load() == 1 {
                    return;
                }
                w
            } else {
                if f.load() == 1 {
                    return;
                }
                n.notified()
            };
            if let Either::Right(()) = biased(notified, sleep(2_000)).await {
                t.set(true);
            }
        });
        s.spawn_root("marker", async move {
            flag.store(1);
            notify.notify_waiters();
        });
        Box::new(move |_| {
            timed_out
                .get()
                .then(|| "waiter missed the notification".into())
        })
    }
}

/// A biased select polls the timer before the cancel branch, so it can pick
/// the timeout after the cancel already happened. The fixed version polls
/// the cancel first.
fn timer_cancel(fixed: bool) -> impl Fn(&mut Scheduler) -> Check {
    move |s| {
        let token = shared(Token::default());
        let bad = shared(Cell::new(false));
        let (tk, b) = (token.clone(), bad.clone());
        s.spawn_root("timer", async move {
            let timed_out = if fixed {
                matches!(biased(tk.cancelled(), sleep(10)).await, Either::Right(()))
            } else {
                matches!(biased(sleep(10), tk.cancelled()).await, Either::Left(()))
            };
            if timed_out && tk.peek() {
                b.set(true);
            }
        });
        s.spawn_root("closer", async move {
            sleep(10).await;
            token.cancel();
        });
        Box::new(move |_| bad.get().then(|| "timeout chosen after cancel".into()))
    }
}

/// Opposite lock order. With `noise`, an unrelated task is also ready.
fn ab_ba(noise: bool) -> impl Fn(&mut Scheduler) -> Check {
    move |s| {
        if noise {
            s.spawn_root("noise", async {
                sleep(3).await;
            });
        }
        let a = shared(Mutex::new(()));
        let b = shared(Mutex::new(()));
        let (a2, b2) = (a.clone(), b.clone());
        s.spawn_root("ab", async move {
            let _x = a.lock();
            let _y = b.lock();
        });
        s.spawn_root("ba", async move {
            let _x = b2.lock();
            let _y = a2.lock();
        });
        Box::new(|r| (r.status == Status::Deadlock).then(|| "deadlock".into()))
    }
}

/// A std-style lock held across an await.
fn mutex_across_await(s: &mut Scheduler) -> Check {
    let m = shared(Mutex::new(0u32));
    let m2 = m.clone();
    s.spawn_root("holder", async move {
        let mut g = m.lock();
        sleep(10).await;
        *g += 1;
    });
    s.spawn_root("waiter", async move {
        *m2.lock() += 1;
    });
    Box::new(|r| (r.status != Status::Quiescent).then(|| format!("{:?}", r.status)))
}

/// Abort a task, then assume its resources are gone. Awaiting the handle
/// first makes that true.
fn abort(join: bool) -> impl Fn(&mut Scheduler) -> Check {
    move |s| {
        struct Res(Rc<Cell<bool>>);
        impl Drop for Res {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let released = shared(Cell::new(false));
        let bad = shared(Cell::new(false));
        let r = released.clone();
        let h = s.spawn_root("worker", async move {
            let _res = Res(r);
            sleep(100).await;
        });
        let b = bad.clone();
        s.spawn_root("aborter", async move {
            sleep(1).await;
            h.abort();
            if join {
                assert_eq!(h.await, TaskExit::Cancelled);
            }
            hooks::preempt(zakura_scheduler::Point::here(zakura_scheduler::Op::Yield));
            if !released.get() {
                b.set(true);
            }
        });
        Box::new(move |_| {
            bad.get()
                .then(|| "aborted task still held its resource".into())
        })
    }
}

/// Requires the run to end with every task finished.
fn quiescent(r: &RunResult) -> Option<String> {
    (r.status != Status::Quiescent).then(|| format!("{:?}", r.status))
}

#[test]
fn finds_planted_races_with_two_workers() {
    assert!(first_violation(2, FIND, lost_update).is_some());
    assert!(first_violation(2, FIND, snapshot_reserve).is_some());
    assert!(first_violation(2, FIND, missed_notify(false)).is_some());
    assert!(first_violation(2, FIND, timer_cancel(false)).is_some());
    assert!(first_violation(2, FIND, abort(false)).is_some());
}

#[test]
fn fixed_versions_never_fail() {
    assert_eq!(first_violation(2, CLEAN, missed_notify(true)), None);
    assert_eq!(first_violation(2, CLEAN, timer_cancel(true)), None);
    assert_eq!(first_violation(2, CLEAN, abort(true)), None);
}

#[test]
fn one_worker_hides_races_inside_a_poll() {
    assert_eq!(first_violation(1, FIND, lost_update), None);
    assert_eq!(first_violation(1, FIND, snapshot_reserve), None);
    assert_eq!(first_violation(1, FIND, missed_notify(false)), None);
    // Timer vs. cancel crosses a poll boundary, so one worker still finds it.
    assert!(first_violation(1, FIND, timer_cancel(false)).is_some());
}

#[test]
fn opposite_lock_order_reports_deadlock() {
    assert!(first_violation(2, FIND, ab_ba(false)).is_some());
    // With an unrelated task ready, the deadlocked pair holds both workers.
    // The lock-cycle check still reports a deadlock, not starvation.
    assert!(first_violation(2, FIND, ab_ba(true)).is_some());
    for seed in 0..FIND {
        let (r, _) = run_fixture(2, seed, ab_ba(true));
        assert_ne!(r.status, Status::WorkerStarved, "seed {seed}");
    }
}

#[test]
fn lock_held_across_await_starves_one_worker() {
    let starved = (0..FIND).any(|seed| {
        let (r, _) = run_fixture(1, seed, mutex_across_await);
        r.status == Status::WorkerStarved
    });
    assert!(starved);
    assert_eq!(first_violation(2, CLEAN, mutex_across_await), None);
}

#[test]
fn only_timers_pending_is_never_deadlock() {
    let timers = |s: &mut Scheduler| -> Check {
        for (label, ms) in [("t100", 100), ("t5", 5)] {
            s.spawn_root(label, async move { sleep(ms).await });
        }
        Box::new(quiescent)
    };
    assert_eq!(first_violation(2, CLEAN, timers), None);
}

#[test]
fn external_wait_is_not_deadlock() {
    let (r, _) = run_fixture(2, 0, |s| {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        s.spawn_root("ext", async move {
            let _ = external(rx).await;
        });
        Box::new(move |_| {
            drop(tx);
            None
        })
    });
    assert_eq!(r.status, Status::WaitingOnExternal);
}

#[test]
fn panic_is_isolated_and_stuck_task_is_cancelled() {
    for seed in 0..200 {
        let drops = shared(Cell::new(0));
        let survivor_done = shared(Cell::new(false));
        let (d, done) = (drops.clone(), survivor_done.clone());
        let (r, _) = run_fixture(2, seed, move |s| {
            s.spawn_root("boom", async {
                sleep(1).await;
                panic!("planted panic");
            });
            s.spawn_root("survivor", async move {
                sleep(2).await;
                done.set(true);
            });
            struct OnDrop(Rc<Cell<u32>>);
            impl Drop for OnDrop {
                fn drop(&mut self) {
                    // Preemption points during shutdown must do nothing.
                    hooks::preempt(zakura_scheduler::Point::here(zakura_scheduler::Op::Yield));
                    self.0.set(self.0.get() + 1);
                }
            }
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let guard = OnDrop(d);
            s.spawn_root("stuck", async move {
                let _guard = guard;
                let _ = external(rx).await;
            });
            let tx = RefCell::new(Some(tx));
            Box::new(move |_| {
                drop(tx.take());
                None
            })
        });
        assert_eq!(r.panics, vec!["planted panic".to_string()], "seed {seed}");
        assert!(survivor_done.get(), "seed {seed}");
        assert_eq!(r.status, Status::WaitingOnExternal, "seed {seed}");
        assert_eq!(r.report.cancelled, 1, "seed {seed}");
        assert_eq!(r.report.unwound, 0, "seed {seed}");
        assert_eq!(drops.get(), 1, "seed {seed}");
    }
}

/// Records whether the thread was unwinding when it was dropped.
struct RecordUnwinding(Rc<Cell<Option<bool>>>);

impl Drop for RecordUnwinding {
    fn drop(&mut self) {
        self.0.set(Some(std::thread::panicking()));
    }
}

/// A preemption point with no shared state behind it.
#[track_caller]
fn point() {
    hooks::preempt(zakura_scheduler::Point::here(zakura_scheduler::Op::Yield));
}

#[test]
fn shutdown_drops_idle_tasks_outside_an_unwind() {
    let saw = shared(Cell::new(None));
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let guard = RecordUnwinding(saw.clone());
    let idle = s.spawn_root("idle", async move {
        let _guard = guard;
        std::future::pending::<()>().await
    });
    assert_eq!(s.run(idle.task()), zakura_scheduler::RunOutcome::Waiting);
    let report = s.shutdown();
    assert_eq!(saw.get(), Some(false));
    assert_eq!((report.cancelled, report.unwound), (1, 0));
}

#[test]
fn shutdown_drop_can_take_a_contended_lock() {
    use zakura_scheduler::RunOutcome::{Reached, Waiting};
    // `holder` keeps the lock across an await, or stops mid-poll holding it.
    // `dropper`'s destructor needs the same lock. A forced unwind used to run
    // that destructor while panicking, so `park` refused and the process
    // aborted.
    for mid_poll in [false, true] {
        struct LockOnDrop {
            mutex: Rc<Mutex<()>>,
            _record: RecordUnwinding,
        }
        impl Drop for LockOnDrop {
            fn drop(&mut self) {
                let _g = self.mutex.lock();
            }
        }
        let saw = shared(Cell::new(None));
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let m = shared(Mutex::new(()));
        let guard = LockOnDrop {
            mutex: m.clone(),
            _record: RecordUnwinding(saw.clone()),
        };
        let dropper = s.spawn_root("dropper", async move {
            let _guard = guard;
            std::future::pending::<()>().await
        });
        let holder = s.spawn_root("holder", async move {
            let g = m.lock();
            if mid_poll {
                point();
                drop(g);
            }
            std::future::pending::<()>().await
        });
        assert_eq!(s.run(dropper.task()), Waiting);
        assert!(matches!(s.run(holder.task()), Reached(_))); // before lock
        if mid_poll {
            assert!(matches!(s.run(holder.task()), Reached(_))); // holding it
        } else {
            assert_eq!(s.run(holder.task()), Waiting); // holding it
        }
        let report = s.shutdown();
        assert_eq!(saw.get(), Some(false), "mid_poll {mid_poll}");
        assert_eq!(
            (report.cancelled, report.unwound, report.panicked),
            (2, 0, 0),
            "mid_poll {mid_poll}"
        );
    }
}

#[test]
fn shutdown_finishes_a_release_sequence_in_a_drop() {
    // Like a three-permit reservation, with a point after each release. A
    // forced unwind used to stop the destructor after the first one.
    struct Reservation(Rc<Cell<u32>>);
    impl Drop for Reservation {
        fn drop(&mut self) {
            for _ in 0..3 {
                self.0.set(self.0.get() + 1);
                point();
            }
        }
    }
    let released = shared(Cell::new(0));
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let reservation = Reservation(released.clone());
    let task = s.spawn_root("task", async move {
        drop(reservation);
        std::future::pending::<()>().await
    });
    assert!(matches!(
        s.run(task.task()),
        zakura_scheduler::RunOutcome::Reached(_)
    ));
    assert_eq!(released.get(), 1);
    let report = s.shutdown();
    assert_eq!(released.get(), 3);
    assert_eq!((report.cancelled, report.unwound), (1, 0));
}

#[test]
fn panic_in_a_leftover_drop_leaves_no_run_behind() {
    use std::sync::Arc;
    struct PanicOnDrop;
    // `Waker::noop()` has no drop to panic in, so this needs its own waker.
    #[allow(clippy::manual_noop_waker)]
    impl std::task::Wake for PanicOnDrop {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("leftover drop panicked");
        }
    }
    let shutdown = std::panic::catch_unwind(|| {
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let task = s.spawn_root(
            "task",
            poll_fn(|_| {
                // Once this copy drops, the timer holds the last reference,
                // so shutdown drops the waker as a leftover.
                let waker = Waker::from(Arc::new(PanicOnDrop));
                let deadline = SimInstant::from_start(std::time::Duration::from_secs(1));
                hooks::register_timer(deadline, &waker);
                Poll::<()>::Pending
            }),
        );
        s.run(task.task());
        s.shutdown()
    });
    assert!(shutdown.is_err(), "the leftover's drop panicked");
    // Used to fail with "this thread already has a running Scheduler".
    drop(Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO));
}

#[test]
fn shutdown_force_unwinds_only_a_lock_cycle() {
    use zakura_scheduler::RunOutcome::Parked;
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let a = shared(Mutex::new(()));
    let b = shared(Mutex::new(()));
    let (a2, b2) = (a.clone(), b.clone());
    let ab = s.spawn_root("ab", async move {
        let _x = a.lock();
        point();
        let _y = b.lock();
    });
    let ba = s.spawn_root("ba", async move {
        let _x = b2.lock();
        point();
        let _y = a2.lock();
    });
    for task in [ab, ab, ba, ba, ab, ba] {
        s.run(task.task());
    }
    assert!(matches!(s.run(ab.task()), Parked(_)));
    assert!(matches!(s.run(ba.task()), Parked(_)));
    assert_eq!(s.status(), Status::Deadlock);
    // Unwinding `ab` releases `a`, so `ba` finishes on its own.
    let report = s.shutdown();
    assert_eq!((report.cancelled, report.unwound), (0, 1));
}

#[test]
fn self_wake_and_child_wakers_complete() {
    let self_wake = |s: &mut Scheduler| -> Check {
        s.spawn_root("self_waker", async {
            let mut first = true;
            poll_fn(|cx| {
                if first {
                    first = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await
        });
        Box::new(quiescent)
    };
    let child_waker = |s: &mut Scheduler| -> Check {
        s.spawn_root("joiner", async {
            selective_join(sleep(5), sleep(10)).await
        });
        Box::new(quiescent)
    };
    assert_eq!(first_violation(2, CLEAN, self_wake), None);
    assert_eq!(first_violation(2, CLEAN, child_waker), None);
}

#[test]
fn same_seed_replays_the_same_run() {
    let seed = first_violation(2, FIND, missed_notify(false)).expect("the race is findable");
    let (a, va) = run_fixture(2, seed, missed_notify(false));
    let (b, vb) = run_fixture(2, seed, missed_notify(false));
    assert_eq!(a.picks, b.picks);
    assert_eq!(va, vb);
    assert!(va.is_some());
}

#[test]
fn ready_reports_each_task_point() {
    let config = SchedulerConfig::default();
    let mut s = Scheduler::new(config, SimInstant::ZERO);
    let c = shared(Atomic::default());
    let c2 = c.clone();
    let h = s.spawn_root("reader", async move {
        c2.load();
        sleep(1).await;
    });
    let at = |s: &Scheduler| s.ready()[0].at.to_string();
    assert_eq!(at(&s), "poll#1");
    s.run(h.task());
    assert_eq!(at(&s), "load#1");
    assert!(s.ready()[0].mid_poll);
    s.run(h.task());
    assert_eq!(at(&s), "sleep#1");
    s.run(h.task());
    assert_eq!(s.status(), Status::WaitingOnTime);
    let deadline = s.next_deadline().expect("the sleep registered a timer");
    assert_eq!(s.advance_to(deadline).len(), 1);
    assert_eq!(at(&s), "poll#2");
    assert!(!s.ready()[0].mid_poll);
}

#[test]
fn task_names_follow_spawn_lineage() {
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let ids = shared(RefCell::new(Vec::new()));
    let ids2 = ids.clone();
    let parent = s.spawn_root("parent", async move {
        for _ in 0..2 {
            ids2.borrow_mut()
                .push(hooks::spawn("child", async {}).task());
        }
    });
    let other = s.spawn_root("parent", async {});
    s.run(parent.task());
    let names: Vec<String> = ids.borrow().iter().map(|t| s.task_name(*t)).collect();
    assert_eq!(names, ["parent.1/child.1", "parent.1/child.2"]);
    assert_eq!(s.task_name(other.task()), "parent.2");
}

#[test]
fn stale_waker_from_an_earlier_run_is_ignored() {
    let kept: Rc<RefCell<Option<Waker>>> = shared(RefCell::new(None));
    let k = kept.clone();
    let mut first = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let h = first.spawn_root(
        "keeper",
        poll_fn(move |cx| {
            *k.borrow_mut() = Some(cx.waker().clone());
            Poll::<()>::Pending
        }),
    );
    first.run(h.task());
    drop(first);

    let mut second = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let h = second.spawn_root("sleeper", poll_fn(|_| Poll::<()>::Pending));
    second.run(h.task());
    assert_eq!(second.status(), Status::Deadlock);
    kept.borrow()
        .as_ref()
        .expect("the first run kept a waker")
        .wake_by_ref();
    assert_eq!(second.status(), Status::Deadlock);
}

#[test]
fn waker_from_another_thread_panics() {
    let kept: Rc<RefCell<Option<Waker>>> = shared(RefCell::new(None));
    let k = kept.clone();
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let h = s.spawn_root(
        "keeper",
        poll_fn(move |cx| {
            *k.borrow_mut() = Some(cx.waker().clone());
            Poll::<()>::Pending
        }),
    );
    s.run(h.task());
    let waker = kept.borrow_mut().take().expect("the task kept its waker");
    let woke = std::thread::spawn(move || waker.wake()).join();
    assert!(woke.is_err());
}

#[test]
fn hooks_do_nothing_outside_a_run() {
    assert!(!hooks::in_run());
    assert_eq!(hooks::now(), None);
    assert_eq!(hooks::new_object(), None);
    // Returns at once instead of suspending.
    hooks::preempt(zakura_scheduler::Point::here(zakura_scheduler::Op::Lock));
}

#[test]
fn spawn_from_setup_code_is_ready_at_once() {
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let ran = shared(Cell::new(false));
    let flag = ran.clone();
    // Outside any task, as when a harness builds a component that spawns.
    let handle = hooks::spawn("setup", async move { flag.set(true) });
    assert_eq!(
        s.ready().iter().map(|t| t.task).collect::<Vec<_>>(),
        [handle.task()]
    );
    s.run(handle.task());
    assert!(ran.get());
    assert_eq!(s.status(), Status::Quiescent);
}

/// A future that, on its first poll, runs `before_point`, reaches a
/// preemption point, and returns `Pending`. Its second poll is `Ready`.
fn pending_after_point(
    before_point: impl Fn(&Waker) + 'static,
) -> impl std::future::Future<Output = ()> {
    let mut polled = false;
    poll_fn(move |cx| {
        if polled {
            return Poll::Ready(());
        }
        polled = true;
        before_point(cx.waker());
        hooks::preempt(zakura_scheduler::Point::here(zakura_scheduler::Op::Yield));
        Poll::Pending
    })
}

#[test]
fn self_wake_survives_a_preemption_point() {
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let h = s.spawn_root("waker", pending_after_point(|w| w.wake_by_ref()));
    s.run(h.task());
    s.run(h.task());
    // The wake came earlier in the same poll, so the task must poll again.
    assert_eq!(s.status(), Status::Running);
    s.run(h.task());
    assert_eq!(s.status(), Status::Quiescent);
}

#[test]
fn wake_while_suspended_mid_poll_is_kept() {
    let slot: Rc<RefCell<Option<Waker>>> = shared(RefCell::new(None));
    let (put, take) = (slot.clone(), slot.clone());
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let a = s.spawn_root(
        "sleeper",
        pending_after_point(move |w| {
            *put.borrow_mut() = Some(w.clone());
        }),
    );
    let b = s.spawn_root("waker", async move {
        take.borrow_mut()
            .take()
            .expect("the sleeper stored its waker")
            .wake();
    });
    s.run(a.task()); // stops at the point, mid-poll, waker stored
    s.run(b.task()); // wakes the sleeper while it's suspended mid-poll
    s.run(a.task()); // the sleeper's poll returns Pending
    assert_eq!(s.status(), Status::Running);
    s.run(a.task());
    assert_eq!(s.status(), Status::Quiescent);
}

#[test]
fn spawn_past_the_task_limit_panics_only_the_spawner() {
    let config = SchedulerConfig {
        max_tasks: 2,
        ..SchedulerConfig::default()
    };
    let mut s = Scheduler::new(config, SimInstant::ZERO);
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let receiver = s.spawn_root("receiver", async move {
        let _ = rx.await;
    });
    let spawner = s.spawn_root("spawner", async move {
        // The rejected child owns the sender. Dropping it wakes the receiver.
        hooks::spawn("child", async move {
            let _tx = tx;
        });
    });
    s.run(receiver.task());
    let outcome = s.run(spawner.task());
    assert!(
        matches!(&outcome, zakura_scheduler::RunOutcome::Panicked(msg) if msg.contains("max_tasks")),
        "{outcome:?}"
    );
    // The receiver saw the sender drop and can finish.
    s.run(receiver.task());
    assert_eq!(s.status(), Status::Quiescent);
}

#[test]
fn lock_cycle_is_deadlock_even_with_other_parked_tasks() {
    // Three workers: an AB/BA pair plus `waiter`, parked on a lock that the
    // ready `holder` owns. The pair can never move, so this is a deadlock even
    // though `waiter`'s lock holder isn't parked.
    let config = SchedulerConfig {
        workers: 3,
        ..SchedulerConfig::default()
    };
    let mut s = Scheduler::new(config, SimInstant::ZERO);
    let a = shared(Mutex::new(()));
    let b = shared(Mutex::new(()));
    let m = shared(Mutex::new(()));
    let (a2, b2, m2) = (a.clone(), b.clone(), m.clone());
    let ab = s.spawn_root("ab", async move {
        let _x = a.lock();
        let _y = b.lock();
    });
    let ba = s.spawn_root("ba", async move {
        let _x = b2.lock();
        let _y = a2.lock();
    });
    let holder = s.spawn_root("holder", async move {
        let _g = m.lock();
        sleep(10).await;
    });
    let waiter = s.spawn_root("waiter", async move {
        let _g = m2.lock();
    });
    use zakura_scheduler::RunOutcome::{Parked, Reached, Waiting};
    for (task, want_parked) in [
        (ab, false),
        (ab, false),
        (ba, false),
        (ba, false),
        (ab, true),
        (ba, true),
    ] {
        let out = s.run(task.task());
        assert_eq!(matches!(out, Parked(_)), want_parked, "{out:?}");
    }
    assert!(matches!(s.run(holder.task()), Reached(_))); // before lock
    assert!(matches!(s.run(holder.task()), Reached(_))); // before sleep
    assert_eq!(s.run(holder.task()), Waiting); // lock held across the sleep
    assert!(matches!(s.run(waiter.task()), Reached(_)));
    assert!(matches!(s.run(waiter.task()), Parked(_)));
    let deadline = s.next_deadline().expect("holder's sleep is pending");
    s.advance_to(deadline);
    // `holder` is ready, but all three workers are held by parked tasks.
    assert_eq!(s.status(), Status::Deadlock);
}

#[test]
fn repeated_join_polls_register_one_waker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    struct Counter(Arc<AtomicUsize>);
    impl std::task::Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let wakes = Arc::new(AtomicUsize::new(0));
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let target = s.spawn_root("target", async { sleep(10).await });
    let w = wakes.clone();
    let joiner = s.spawn_root(
        "joiner",
        poll_fn(move |_| {
            // The same waker polls the handle many times, as a select loop would.
            let waker = Waker::from(Arc::new(Counter(w.clone())));
            let mut cx = std::task::Context::from_waker(&waker);
            let mut handle = target;
            for _ in 0..100 {
                assert!(
                    std::future::Future::poll(std::pin::Pin::new(&mut handle), &mut cx)
                        .is_pending()
                );
            }
            Poll::Ready(())
        }),
    );
    s.run(joiner.task());
    while s.status() == Status::Running || s.status() == Status::WaitingOnTime {
        if s.status() == Status::WaitingOnTime {
            let d = s.next_deadline().expect("a timer is pending");
            s.advance_to(d);
        } else {
            let t = s.ready()[0].task;
            s.run(t);
        }
    }
    assert_eq!(s.status(), Status::Quiescent);
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
}
