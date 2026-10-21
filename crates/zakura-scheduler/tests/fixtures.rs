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
fn panic_is_isolated_and_stuck_task_is_unwound() {
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
        assert_eq!(r.report.unwound, 1, "seed {seed}");
        assert_eq!(drops.get(), 1, "seed {seed}");
    }
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
