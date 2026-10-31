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
fn shutdown_leaks_a_mid_poll_spin_with_no_setter() {
    if leaks_in_a_child("shutdown_leaks_a_mid_poll_spin_with_no_setter") {
        return;
    }
    let report = shutdown_report(|s| {
        let flag = shared(Atomic::default());
        let spinner = s.spawn_root("spinner", async move {
            // Nothing ever stores, and every load is a point.
            while flag.load() == 0 {}
        });
        assert!(matches!(
            s.run(spinner.task()),
            zakura_scheduler::RunOutcome::Reached(_)
        ));
    });
    assert_eq!(
        (report.cancelled, report.unwound, report.leaked),
        (0, 0, 1),
        "the spinner was leaked, not cancelled or unwound"
    );
}

/// Spins on `flag` when dropped, as a guard that waits for another task in
/// its destructor would.
struct SpinsOnDrop(Rc<Atomic>);

impl Drop for SpinsOnDrop {
    fn drop(&mut self) {
        while self.0.load() == 0 {}
    }
}

/// Runs `setup` on a fresh thread and returns the cleanup report, or panics
/// if shutdown takes longer than a minute.
fn shutdown_report(
    setup: impl FnOnce(&mut Scheduler) + Send + 'static,
) -> zakura_scheduler::CleanupReport {
    shutdown_report_with(SchedulerConfig::default(), setup)
}

/// [`shutdown_report`] with `config`.
fn shutdown_report_with(
    config: SchedulerConfig,
    setup: impl FnOnce(&mut Scheduler) + Send + 'static,
) -> zakura_scheduler::CleanupReport {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut s = Scheduler::new(config, SimInstant::ZERO);
        setup(&mut s);
        let _ = done.send(s.shutdown());
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("shutdown finished")
}

#[test]
fn shutdown_leaks_a_stuck_task_whose_drop_spins() {
    if leaks_in_a_child("shutdown_leaks_a_stuck_task_whose_drop_spins") {
        return;
    }
    let report = shutdown_report(|s| {
        let flag = shared(Atomic::default());
        let spinner = s.spawn_root("spinner", async move {
            // Unwinding would run this guard, which spins forever.
            let _guard = SpinsOnDrop(flag.clone());
            while flag.load() == 0 {}
        });
        assert!(matches!(
            s.run(spinner.task()),
            zakura_scheduler::RunOutcome::Reached(_)
        ));
    });
    assert_eq!((report.leaked, report.unwound), (1, 0));
}

/// Sets `flag` when dropped, as a guard that signals another task would.
struct SetsOnDrop(Rc<Atomic>);

impl Drop for SetsOnDrop {
    fn drop(&mut self) {
        self.0.store(1);
    }
}

#[test]
fn a_leak_leaks_the_waiters_that_depend_on_it() {
    if leaks_in_a_child("a_leak_leaks_the_waiters_that_depend_on_it") {
        return;
    }
    use zakura_scheduler::RunOutcome::{Parked, Reached};
    let report = shutdown_report(|s| {
        let lock = shared(Mutex::new(()));
        let flag = shared(Atomic::default());
        let never = shared(Atomic::default());
        let (a_lock, a_flag) = (lock.clone(), flag.clone());
        // A holds the lock and spins. Unwinding A would set the flag and
        // release the lock, but leaking A does neither.
        let a = s.spawn_root("a", async move {
            let _held = a_lock.lock();
            let _signal = SetsOnDrop(a_flag);
            while never.load() == 0 {}
        });
        // B parks on A's lock, holding a guard that waits for the flag.
        let b = s.spawn_root("b", async move {
            let _waits = SpinsOnDrop(flag);
            drop(lock.lock());
        });
        for _ in 0..4 {
            assert!(matches!(s.run(a.task()), Reached(_)));
        }
        while !matches!(s.run(b.task()), Parked(_)) {}
    });
    assert_eq!((report.leaked, report.unwound), (2, 0));
}

/// Counts its drops, as a guard with a visible destructor would.
struct CountsDrops(Rc<Cell<u32>>);

impl Drop for CountsDrops {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn a_stuck_task_stops_the_drain() {
    if leaks_in_a_child("a_stuck_task_stops_the_drain") {
        return;
    }
    let report = shutdown_report(|s| {
        let never = shared(Atomic::default());
        let spinner = s.spawn_root("spinner", async move { while never.load() == 0 {} });
        assert!(matches!(
            s.run(spinner.task()),
            zakura_scheduler::RunOutcome::Reached(_)
        ));
        // Ready tasks the drain would cancel after the spinner, running
        // their guards' destructors.
        let drops = shared(Cell::new(0));
        for _ in 0..3 {
            let guard = CountsDrops(drops.clone());
            s.spawn_root("ready", async move {
                let _guard = guard;
                std::future::pending::<()>().await
            });
        }
    });
    assert_eq!(
        (report.leaked, report.cancelled),
        (4, 0),
        "once the spinner was stuck, nothing was cancelled"
    );
}

/// A waker whose wake panics, as a buggy joiner's might.
struct PanicsOnWake;

impl std::task::Wake for PanicsOnWake {
    fn wake(self: std::sync::Arc<Self>) {
        panic!("this waker panics");
    }
}

#[test]
fn a_panicking_join_waker_unwinds_nothing_after_a_leak() {
    if leaks_in_a_child("a_panicking_join_waker_unwinds_nothing_after_a_leak") {
        return;
    }
    use std::future::Future;
    use zakura_scheduler::RunOutcome::{Parked, Reached};
    let (done, finished) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        // Signals when the thread ends, even by a panic.
        struct Done(std::sync::mpsc::Sender<()>);
        impl Drop for Done {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let _done = Done(done);
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let lock = shared(Mutex::new(()));
        let flag = shared(Atomic::default());
        let never = shared(Atomic::default());
        let (a_lock, a_flag) = (lock.clone(), flag.clone());
        let mut a = s.spawn_root("a", async move {
            let _held = a_lock.lock();
            let _signal = SetsOnDrop(a_flag);
            while never.load() == 0 {}
        });
        let b = s.spawn_root("b", async move {
            let _waits = SpinsOnDrop(flag);
            drop(lock.lock());
        });
        for _ in 0..4 {
            assert!(matches!(s.run(a.task()), Reached(_)));
        }
        while !matches!(s.run(b.task()), Parked(_)) {}
        // A joiner of A whose waker panics when A is finished.
        let waker = Waker::from(std::sync::Arc::new(PanicsOnWake));
        let polled = std::pin::Pin::new(&mut a).poll(&mut std::task::Context::from_waker(&waker));
        assert!(polled.is_pending());
        // The waker's panic ends this thread. It must not force-unwind B on
        // the way out.
        let _ = s.shutdown();
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the shutdown thread ended");
}

thread_local! {
    /// The flag a timer waker's `Drop` hands to the task it spawns. See
    /// `a_spawn_from_a_leftover_drop_is_leaked`.
    static LEFTOVER_FLAG: RefCell<Option<Rc<Atomic>>> = const { RefCell::new(None) };
}

/// A waker whose last drop spawns a task holding a guard that spins on
/// [`LEFTOVER_FLAG`].
struct SpawnsOnDrop;

// `Waker::noop()` has no drop to spawn from, so this needs its own waker.
#[allow(clippy::manual_noop_waker)]
impl std::task::Wake for SpawnsOnDrop {
    fn wake(self: std::sync::Arc<Self>) {}
}

impl Drop for SpawnsOnDrop {
    fn drop(&mut self) {
        let flag = LEFTOVER_FLAG
            .with(|f| f.borrow().clone())
            .expect("the test sets the flag first");
        // Built outside the future, so dropping it unpolled runs the guard.
        let waits = SpinsOnDrop(flag);
        hooks::spawn("b", async move {
            let _waits = waits;
            std::future::pending::<()>().await
        });
    }
}

#[test]
fn a_spawn_from_a_leftover_drop_is_leaked() {
    if leaks_in_a_child("a_spawn_from_a_leftover_drop_is_leaked") {
        return;
    }
    let report = shutdown_report(|s| {
        let flag = shared(Atomic::default());
        let never = shared(Atomic::default());
        LEFTOVER_FLAG.with(|f| *f.borrow_mut() = Some(flag.clone()));
        // A spins until the cleanup bound stops it, so its guard, which
        // would set the flag, never runs.
        let a = s.spawn_root("a", async move {
            let _signal = SetsOnDrop(flag);
            while never.load() == 0 {}
        });
        assert!(matches!(
            s.run(a.task()),
            zakura_scheduler::RunOutcome::Reached(_)
        ));
        // A leftover timer whose waker spawns B when shutdown drops it. B's
        // guard waits on A's flag.
        let waker = Waker::from(std::sync::Arc::new(SpawnsOnDrop));
        hooks::register_timer(
            SimInstant::from_start(std::time::Duration::from_secs(3600)),
            &waker,
        );
    });
    // The run is abandoned with the timer still registered, so its waker
    // never drops and B is never spawned. Only A is leaked.
    assert_eq!((report.leaked, report.cancelled), (1, 0));
}

#[test]
fn a_rejected_spawn_from_a_leftover_drop_runs_nothing() {
    if leaks_in_a_child("a_rejected_spawn_from_a_leftover_drop_runs_nothing") {
        return;
    }
    // With room for one task, B's spawn would be rejected, and a rejected
    // spawn drops its future at once, guard and all.
    let config = SchedulerConfig {
        max_tasks: 1,
        ..SchedulerConfig::default()
    };
    let report = shutdown_report_with(config, |s| {
        let flag = shared(Atomic::default());
        let never = shared(Atomic::default());
        LEFTOVER_FLAG.with(|f| *f.borrow_mut() = Some(flag.clone()));
        let a = s.spawn_root("a", async move {
            let _signal = SetsOnDrop(flag);
            while never.load() == 0 {}
        });
        assert!(matches!(
            s.run(a.task()),
            zakura_scheduler::RunOutcome::Reached(_)
        ));
        let waker = Waker::from(std::sync::Arc::new(SpawnsOnDrop));
        hooks::register_timer(
            SimInstant::from_start(std::time::Duration::from_secs(3600)),
            &waker,
        );
    });
    assert_eq!((report.leaked, report.cancelled), (1, 0));
}

/// Sets its flag when woken or dropped.
struct SetsFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl std::task::Wake for SetsFlag {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Drop for SetsFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn no_code_runs_after_a_task_is_stuck() {
    use std::{
        future::Future,
        sync::{atomic::AtomicBool, Arc},
    };
    if leaks_in_a_child("no_code_runs_after_a_task_is_stuck") {
        return;
    }
    let (join, timer, spawned) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let flags = (join.clone(), timer.clone(), spawned.clone());
    let report = shutdown_report(move |s| {
        let never = shared(Atomic::default());
        let mut spinner = s.spawn_root("spinner", async move { while never.load() == 0 {} });
        assert!(matches!(
            s.run(spinner.task()),
            zakura_scheduler::RunOutcome::Reached(_)
        ));
        // A joiner's waker, a timer's waker, and a queued spawn's future.
        // Waking or dropping any of them sets its flag.
        let joiner = Waker::from(Arc::new(SetsFlag(join)));
        let polled =
            std::pin::Pin::new(&mut spinner).poll(&mut std::task::Context::from_waker(&joiner));
        assert!(polled.is_pending());
        hooks::register_timer(
            SimInstant::from_start(std::time::Duration::from_secs(3600)),
            &Waker::from(Arc::new(SetsFlag(timer))),
        );
        let guard = SetsFlag(spawned);
        hooks::spawn("queued", async move {
            let _guard = guard;
            std::future::pending::<()>().await
        });
    });
    let (join, timer, spawned) = flags;
    let set = |flag: &AtomicBool| flag.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        (set(&join), set(&timer), set(&spawned)),
        (false, false, false),
        "no waker woke or dropped, and no future dropped"
    );
    assert_eq!(report.leaked, 2, "the spinner and the queued spawn");
}

#[test]
fn a_caught_unwind_that_gets_stuck_abandons_the_run() {
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{atomic::AtomicBool, Arc},
    };
    use zakura_scheduler::RunOutcome::{Parked, Reached};
    if leaks_in_a_child("a_caught_unwind_that_gets_stuck_abandons_the_run") {
        return;
    }
    let b_ran = Arc::new(AtomicBool::new(false));
    let flag = b_ran.clone();
    let report = shutdown_report(move |s| {
        let (l1, l2) = (shared(Mutex::new(())), shared(Mutex::new(())));
        let b_flag = shared(Atomic::default());
        let never = shared(Atomic::default());
        let (a_l1, a_l2, a_waits) = (l1.clone(), l2.clone(), b_flag);
        // A holds l1, waits for l2 in a lock cycle with B, catches the
        // forced unwind, then spins until the cleanup bound stops it. Its
        // guard waits on a flag B never sets.
        let a = s.spawn_root("a", async move {
            let _waits = SpinsOnDrop(a_waits);
            let _l1 = a_l1.lock();
            point();
            let _ = catch_unwind(AssertUnwindSafe(|| drop(a_l2.lock())));
            while never.load() == 0 {}
        });
        // B holds l2 and waits for l1. Its guard would set B's ran flag.
        let b = s.spawn_root("b", async move {
            let _ran = SetsFlag(flag);
            let _l2 = l2.lock();
            point();
            drop(l1.lock());
        });
        for task in [a.task(), a.task(), b.task(), b.task()] {
            assert!(matches!(s.run(task), Reached(_)));
        }
        for task in [a.task(), b.task()] {
            let parked = (0..10).any(|_| matches!(s.run(task), Parked(_)));
            assert!(parked, "the task parks in the lock cycle");
        }
    });
    assert!(
        !b_ran.load(std::sync::atomic::Ordering::SeqCst),
        "B's guard never ran"
    );
    assert_eq!((report.leaked, report.cancelled), (2, 0), "A and B");
}

#[test]
fn a_panic_in_a_direct_drop_unwinds_nothing() {
    if !leaks_in_a_child("a_panic_in_a_direct_drop_unwinds_nothing") {
        panic_in_shutdown_unwinds_nothing(drop);
    }
}

#[test]
fn a_panic_in_shutdown_unwinds_nothing() {
    if !leaks_in_a_child("a_panic_in_shutdown_unwinds_nothing") {
        panic_in_shutdown_unwinds_nothing(|s| {
            s.shutdown();
        });
    }
}

/// Ends a scheduler with `end` while a joiner's waker panics during its
/// shutdown, and checks that a task parked on a lock was never unwound.
fn panic_in_shutdown_unwinds_nothing(end: fn(Scheduler)) {
    use std::{
        future::Future,
        sync::{atomic::AtomicBool, Arc},
    };
    use zakura_scheduler::RunOutcome::{Parked, Waiting};
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let (done, finished) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        // Signals when the thread ends, even by a panic.
        struct Done(std::sync::mpsc::Sender<()>);
        impl Drop for Done {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let _done = Done(done);
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let lock = shared(Mutex::new(()));
        let held = lock.clone();
        // A waits forever, and its joiner's waker panics when A is cancelled.
        let mut a = s.spawn_root("a", std::future::pending::<()>());
        // H holds the lock across an await, so B stays parked on it.
        let h = s.spawn_root("h", async move {
            let _held = held.lock();
            std::future::pending::<()>().await
        });
        // B parks on the lock. Unwinding B would run its guard.
        let b = s.spawn_root("b", async move {
            let _ran = SetsFlag(flag);
            drop(lock.lock());
        });
        assert_eq!(s.run(a.task()), Waiting);
        while s.run(h.task()) != Waiting {}
        while !matches!(s.run(b.task()), Parked(_)) {}
        let waker = Waker::from(std::sync::Arc::new(PanicsOnWake));
        let polled = std::pin::Pin::new(&mut a).poll(&mut std::task::Context::from_waker(&waker));
        assert!(polled.is_pending());
        // The waker's panic ends this thread.
        end(s);
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the thread ended");
    assert!(
        !ran.load(std::sync::atomic::Ordering::SeqCst),
        "B was not unwound"
    );
}

/// When woken, polls a join handle and keeps the exit it reports.
struct RecordsExit {
    handle: zakura_scheduler::JoinHandle,
    exit: std::sync::Mutex<Option<TaskExit>>,
}

impl std::task::Wake for RecordsExit {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        use std::future::Future;
        let mut handle = self.handle;
        let noop = std::task::Context::from_waker(Waker::noop());
        let mut cx = noop;
        if let Poll::Ready(exit) = std::pin::Pin::new(&mut handle).poll(&mut cx) {
            *self.exit.lock().unwrap() = Some(exit);
        }
    }
}

/// Locks its mutex when dropped, as a cancellation destructor might.
struct LocksOnDrop(Rc<Mutex<()>>);

impl Drop for LocksOnDrop {
    fn drop(&mut self) {
        drop(self.0.lock());
    }
}

#[test]
fn a_forced_unwind_out_of_a_cancellation_drop_is_a_cancel() {
    use std::{future::Future, sync::Arc};
    use zakura_scheduler::RunOutcome::Waiting;
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let (la, lb) = (shared(Mutex::new(())), shared(Mutex::new(())));
    let (a_la, a_lb) = (la.clone(), lb.clone());
    // A and B each hold a lock across an await, and their cancellation
    // destructors each park on the other's lock.
    let mut a = s.spawn_root("a", async move {
        let _held = a_la.lock();
        let _then = LocksOnDrop(a_lb);
        std::future::pending::<()>().await
    });
    let b = s.spawn_root("b", async move {
        let _held = lb.lock();
        let _then = LocksOnDrop(la);
        std::future::pending::<()>().await
    });
    while s.run(a.task()) != Waiting {}
    while s.run(b.task()) != Waiting {}
    let joiner = Arc::new(RecordsExit {
        handle: a,
        exit: std::sync::Mutex::new(None),
    });
    let waker = Waker::from(joiner.clone());
    assert!(std::pin::Pin::new(&mut a)
        .poll(&mut std::task::Context::from_waker(&waker))
        .is_pending());
    let report = s.shutdown();
    assert_eq!(
        *joiner.exit.lock().unwrap(),
        Some(TaskExit::Cancelled),
        "the forced unwind ended A as cancelled"
    );
    assert_eq!((report.unwound, report.panicked), (1, 0));
}

#[test]
fn long_but_finite_cleanup_finishes() {
    let report = shutdown_report(|s| {
        let flag = shared(Atomic::default());
        let worker = s.spawn_root("worker", async move {
            let _guard = SpinsOnDrop(flag.clone());
            // More points than the first cleanup bound allowed, then the
            // store the guard waits on.
            for _ in 0..100_002 {
                point();
            }
            flag.store(1);
        });
        assert!(matches!(
            s.run(worker.task()),
            zakura_scheduler::RunOutcome::Reached(_)
        ));
    });
    assert_eq!((report.leaked, report.unwound, report.cancelled), (0, 0, 0));
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
    // Shutdown panics, so the scheduler leaks the run state on purpose
    // rather than drop it while panicking. The body runs in a child with
    // LeakSanitizer off, so ASan builds don't report that leak.
    if !in_child("panic_in_a_leftover_drop_leaves_no_run_behind") {
        let status = rerun_in_child(
            "panic_in_a_leftover_drop_leaves_no_run_behind",
            &[("ASAN_OPTIONS", "detect_leaks=0")],
        )
        .wait_with_output()
        .expect("the child is ours to wait on")
        .status;
        assert!(status.success(), "the child failed: {status}");
        return;
    }
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
    let payload = woke.expect_err("a wake from another thread panics");
    let msg = payload
        .downcast_ref::<String>()
        .expect("assert_eq! panics with a formatted message");
    assert!(msg.contains("from another OS thread"), "{msg}");
    // The wake had no effect on the run.
    assert_eq!(s.status(), Status::Deadlock);
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

/// One step past each of `n` preemption points, then a `Pending`.
fn points_then_pending(
    n: usize,
    before_points: impl Fn(&Waker) + 'static,
    polls: Rc<Cell<u32>>,
) -> impl std::future::Future<Output = ()> {
    poll_fn(move |cx| {
        polls.set(polls.get() + 1);
        if polls.get() > 1 {
            return Poll::Ready(());
        }
        before_points(cx.waker());
        for _ in 0..n {
            point();
        }
        Poll::Pending
    })
}

/// Runs `task` until its poll returns `Pending`.
fn run_to_pending(s: &mut Scheduler, task: zakura_scheduler::TaskId) {
    while s.run(task) != zakura_scheduler::RunOutcome::Waiting {}
}

/// The tasks in `ready()`, in its order.
fn ready_ids(s: &Scheduler) -> Vec<zakura_scheduler::TaskId> {
    s.ready().iter().map(|t| t.task).collect()
}

#[test]
fn worker_limit_gates_new_polls_but_not_resumes() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use zakura_scheduler::RunOutcome::{Completed, Reached};
    for workers in [1u16, 2] {
        let config = SchedulerConfig {
            workers,
            ..SchedulerConfig::default()
        };
        let mut s = Scheduler::new(config, SimInstant::ZERO);
        let held: Vec<_> = (0..workers)
            .map(|_| {
                s.spawn_root("held", async {
                    point();
                    point();
                })
            })
            .collect();
        let late = s.spawn_root("late", async {});
        for h in &held {
            assert!(matches!(s.run(h.task()), Reached(_)));
        }
        // Every worker is held mid-poll, so `late` can't start its poll.
        assert!(s.ready().iter().all(|t| t.mid_poll), "workers {workers}");
        assert!(!ready_ids(&s).contains(&late.task()), "workers {workers}");
        let refused = catch_unwind(AssertUnwindSafe(|| s.run(late.task())));
        assert!(refused.is_err(), "workers {workers}");
        // A task already mid-poll resumes with no free worker.
        assert!(matches!(s.run(held[0].task()), Reached(_)));
        assert!(!ready_ids(&s).contains(&late.task()), "workers {workers}");
        // Its poll ends, which frees its worker for `late`.
        assert_eq!(s.run(held[0].task()), Completed);
        let late_ready = s.ready().into_iter().find(|t| t.task == late.task());
        assert!(late_ready.is_some_and(|t| !t.mid_poll), "workers {workers}");
    }
}

#[test]
fn wake_during_a_poll_survives_any_number_of_points() {
    for points in [1, 5, 50] {
        // The task wakes itself, then stops at every point before `Pending`.
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let polls = shared(Cell::new(0));
        let h = s.spawn_root(
            "self_waker",
            points_then_pending(points, |w| w.wake_by_ref(), polls.clone()),
        );
        run_to_pending(&mut s, h.task());
        assert_eq!(ready_ids(&s), [h.task()], "points {points}");
        assert!(!s.ready()[0].mid_poll);
        s.run(h.task());
        assert_eq!((s.status(), polls.get()), (Status::Quiescent, 2));
        drop(s);

        // Another task wakes it while it's suspended at the first point.
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let slot: Rc<RefCell<Option<Waker>>> = shared(RefCell::new(None));
        let (put, take) = (slot.clone(), slot.clone());
        let polls = shared(Cell::new(0));
        let sleeper = s.spawn_root(
            "sleeper",
            points_then_pending(
                points,
                move |w| *put.borrow_mut() = Some(w.clone()),
                polls.clone(),
            ),
        );
        let waker = s.spawn_root("waker", async move {
            take.borrow_mut().take().expect("stored").wake();
        });
        s.run(sleeper.task());
        s.run(waker.task());
        run_to_pending(&mut s, sleeper.task());
        assert_eq!(ready_ids(&s), [sleeper.task()], "points {points}");
        s.run(sleeper.task());
        assert_eq!((s.status(), polls.get()), (Status::Quiescent, 2));
    }
}

#[test]
fn duplicate_wakes_leave_one_ready_entry() {
    let kept: Rc<RefCell<Option<Waker>>> = shared(RefCell::new(None));
    let polls = shared(Cell::new(0));
    let (k, p) = (kept.clone(), polls.clone());
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let h = s.spawn_root(
        "waiter",
        poll_fn(move |cx| {
            p.set(p.get() + 1);
            *k.borrow_mut() = Some(cx.waker().clone());
            Poll::<()>::Pending
        }),
    );
    s.run(h.task());
    let wake = |n| {
        let waker = kept.borrow().clone().expect("the task stored its waker");
        (0..n).for_each(|_| waker.wake_by_ref());
    };
    // While waiting, five wakes make one ready entry and one more poll.
    wake(5);
    assert_eq!(ready_ids(&s), [h.task()]);
    s.run(h.task());
    assert_eq!((s.status(), polls.get()), (Status::Deadlock, 2));
    // Wakes from a task's poll, while the waiter is already ready, add nothing.
    let mut steps = 0;
    let waker = kept.borrow().clone().expect("stored");
    let again = s.spawn_root("waker", async move {
        waker.wake_by_ref();
        point();
        waker.wake_by_ref();
        waker.wake_by_ref();
    });
    while !again.is_finished() {
        s.run(again.task());
        steps += 1;
    }
    assert!(steps > 1, "the waker stopped at its point");
    assert_eq!(ready_ids(&s), [h.task()]);
    s.run(h.task());
    assert_eq!((s.status(), polls.get()), (Status::Deadlock, 3));
}

#[test]
fn ready_set_is_canonical_whatever_the_wake_order() {
    let orders = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for order in orders {
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let wakers: Rc<RefCell<Vec<Option<Waker>>>> = shared(RefCell::new(vec![None; 3]));
        let ids: Vec<_> = (0..3)
            .map(|i| {
                let w = wakers.clone();
                s.spawn_root(
                    "task",
                    poll_fn(move |cx| {
                        w.borrow_mut()[i] = Some(cx.waker().clone());
                        Poll::<()>::Pending
                    }),
                )
                .task()
            })
            .collect();
        for id in &ids {
            s.run(*id);
        }
        for i in order {
            wakers.borrow()[i].clone().expect("stored").wake();
        }
        let first = s.ready();
        assert_eq!(ready_ids(&s), ids, "order {order:?}");
        assert_eq!(s.ready(), first, "order {order:?}");
    }
}

/// A parent with two children, each loading a counter.
fn family(s: &mut Scheduler) -> Check {
    let c = shared(Atomic::default());
    s.spawn_root("parent", async move {
        for _ in 0..2 {
            let c = c.clone();
            hooks::spawn("child", async move {
                c.load();
            });
        }
    });
    Box::new(|_| None)
}

#[test]
fn unrelated_root_does_not_rename_task_paths() {
    let schedule = [
        "parent.1",
        "parent.1/child.2",
        "parent.1/child.1",
        "parent.1/child.2",
        "parent.1/child.1",
    ];
    let (alone, _) = replay(2, &schedule, family).expect("the schedule replays");
    // The unrelated root spawns first, so every numeric id shifts by one.
    let (with_noise, _) = replay(2, &schedule, |s| {
        s.spawn_root("unrelated", async {
            hooks::spawn("child", async {});
        });
        family(s)
    })
    .expect("the schedule replays");
    let family_picks = |r: &RunResult| -> Vec<String> {
        r.picks
            .iter()
            .filter(|p| p.starts_with("parent.1"))
            .cloned()
            .collect()
    };
    assert_eq!(family_picks(&alone), family_picks(&with_noise));
    assert_eq!(family_picks(&alone).len(), schedule.len());
    assert!(with_noise
        .picks
        .iter()
        .any(|p| p.starts_with("unrelated.1/child.1:")));
}

/// The fixed lost update: one read-modify-write.
fn lost_update_fixed(s: &mut Scheduler) -> Check {
    let c = shared(Atomic::default());
    for label in ["inc_a", "inc_b"] {
        let c = c.clone();
        s.spawn_root(label, async move {
            c.fetch_add(1);
        });
    }
    Box::new(move |_| (c.peek() != 2).then(|| format!("counter = {}, want 2", c.peek())))
}

/// The fixed snapshot then reserve: check and reserve under one lock.
fn snapshot_reserve_fixed(s: &mut Scheduler) -> Check {
    let avail = shared(Mutex::new(1i64));
    for label in ["res_a", "res_b"] {
        let avail = avail.clone();
        s.spawn_root(label, async move {
            let mut g = avail.lock();
            if *g >= 1 {
                *g -= 1;
            }
        });
    }
    Box::new(move |_| {
        let v = *avail.lock();
        (v < 0).then(|| format!("over-committed: available = {v}"))
    })
}

/// Saved schedules that expose the lost update and snapshot then reserve:
/// both tasks start their polls, then alternate inside them.
const LOST_UPDATE_SCHEDULE: &[&str] = &[
    "inc_a.1", "inc_b.1", "inc_a.1", "inc_b.1", "inc_a.1", "inc_b.1",
];
const SNAPSHOT_RESERVE_SCHEDULE: &[&str] = &[
    "res_a.1", "res_b.1", "res_a.1", "res_b.1", "res_a.1", "res_b.1",
];

#[test]
fn saved_schedules_expose_planted_races_and_pass_fixed_versions() {
    type Fixture = fn(&mut Scheduler) -> Check;
    let cases: [(&[&str], Fixture, Fixture); 2] = [
        (LOST_UPDATE_SCHEDULE, lost_update, lost_update_fixed),
        (
            SNAPSHOT_RESERVE_SCHEDULE,
            snapshot_reserve,
            snapshot_reserve_fixed,
        ),
    ];
    for (schedule, planted, fixed) in cases {
        let (_, violation) = replay(2, schedule, planted).expect("replays with 2 workers");
        assert!(violation.is_some(), "{schedule:?}");
        let (r, violation) = replay(2, schedule, fixed).expect("replays with 2 workers");
        assert_eq!(violation, None, "{schedule:?}");
        assert_eq!(r.status, Status::Quiescent);
        // One worker rules out the interleaving: the second task can't start
        // its poll while the first is mid-poll.
        let refused = replay(1, schedule, planted).map(|_| ());
        assert_eq!(refused, Err(format!("step 1: {} isn't ready", schedule[1])));
    }
}

/// Saved schedules for the lock-cycle and starvation cases.
const AB_BA_WITH_NOISE_SCHEDULE: &[&str] = &[
    "noise.1", "noise.1", "ab.1", "ab.1", "ba.1", "ba.1", "ab.1", "ba.1",
];
const STARVATION_SCHEDULE: &[&str] = &["holder.1", "holder.1", "holder.1", "waiter.1", "waiter.1"];

#[test]
fn saved_schedules_for_lock_cycle_and_starvation() {
    // `noise` waits on its timer while the pair parks. Once time moves, it's
    // ready but has no worker, and the cycle still reports `Deadlock`. That's
    // the one shape `status()` already handles. With a spare worker, or with
    // the timer still pending, the cycle shows as `Running` or
    // `WaitingOnTime` instead (#1635).
    let (r, _) = replay(2, AB_BA_WITH_NOISE_SCHEDULE, ab_ba(true)).expect("replays");
    assert_eq!(r.status, Status::Deadlock);
    assert!(
        r.picks.contains(&"advance-time".to_string()),
        "{:?}",
        r.picks
    );
    // `waiter` parks on the lock `holder` keeps across its sleep. With one
    // worker, `holder` can't resume. With two, the run completes.
    let (r, _) = replay(1, STARVATION_SCHEDULE, mutex_across_await).expect("replays");
    assert_eq!(r.status, Status::WorkerStarved);
    let (r, violation) = replay(2, STARVATION_SCHEDULE, mutex_across_await).expect("replays");
    assert_eq!((r.status, violation), (Status::Quiescent, None));
}

#[test]
fn sim_owned_wait_is_not_an_external_wait() {
    // A storage completion the harness hasn't delivered yet: the task waits on
    // an ordinary channel, with no external mark.
    let (tx, rx) = tokio::sync::oneshot::channel::<u32>();
    let got = shared(Cell::new(None));
    let g = got.clone();
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let h = s.spawn_root("storage_wait", async move { g.set(rx.await.ok()) });
    assert_eq!(s.run(h.task()), zakura_scheduler::RunOutcome::Waiting);
    // The scheduler alone sees no way on. The harness decides it can deliver.
    assert_eq!(s.status(), Status::Deadlock);
    tx.send(7).expect("the task still waits");
    assert_eq!(s.status(), Status::Running);
    s.run(h.task());
    assert_eq!((s.status(), got.get()), (Status::Quiescent, Some(7)));
}

/// Counts drops of a resource a task owns.
struct Owned(Rc<Cell<u32>>);

impl Drop for Owned {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn abort_then_join_releases_each_resource_once() {
    use zakura_scheduler::RunOutcome::{Reached, Waiting};
    for state in ["before first poll", "waiting", "mid-poll"] {
        let drops = shared(Cell::new(0));
        let polls = shared(Cell::new(0));
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let (res, p) = (Owned(drops.clone()), polls.clone());
        let target = s.spawn_root("target", async move {
            let _res = res;
            poll_fn(|_| {
                p.set(p.get() + 1);
                point();
                Poll::<()>::Pending
            })
            .await
        });
        match state {
            "waiting" => {
                assert!(matches!(s.run(target.task()), Reached(_)));
                assert_eq!(s.run(target.task()), Waiting);
            }
            "mid-poll" => assert!(matches!(s.run(target.task()), Reached(_))),
            _ => {}
        }
        let polls_at_abort = polls.get();
        target.abort();
        assert_eq!(drops.get(), 0, "{state}: abort doesn't release at once");
        let joined = shared(Cell::new(None));
        let (j, d) = (joined.clone(), drops.clone());
        s.spawn_root("joiner", async move {
            let exit = target.await;
            j.set(Some((exit, d.get())));
        });
        let mut picks = Vec::new();
        let status = drive(&mut s, 0, &mut picks, &mut Vec::new());
        assert_eq!(status, Status::Quiescent, "{state}");
        assert_eq!(
            joined.get(),
            Some((TaskExit::Cancelled, 1)),
            "{state}: released by the time join returns"
        );
        assert_eq!(polls.get(), polls_at_abort, "{state}: no poll after abort");
        let report = s.shutdown();
        assert_eq!((drops.get(), report.cancelled), (1, 0), "{state}");
    }
}

#[test]
fn completion_racing_abort_has_one_result() {
    use zakura_scheduler::RunOutcome::{Completed, Reached};
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let target = s.spawn_root("target", async { point() });
    assert!(matches!(s.run(target.task()), Reached(_)));
    // The abort lands mid-poll, and that same poll then finishes.
    target.abort();
    let exits = shared(RefCell::new(Vec::new()));
    for _ in 0..2 {
        let e = exits.clone();
        s.spawn_root("joiner", async move {
            let exit = target.await;
            e.borrow_mut().push(exit);
        });
    }
    assert_eq!(s.run(target.task()), Completed);
    // An abort after the end does nothing.
    target.abort();
    let status = drive(&mut s, 0, &mut Vec::new(), &mut Vec::new());
    assert_eq!(status, Status::Quiescent);
    assert_eq!(*exits.borrow(), [TaskExit::Completed, TaskExit::Completed]);
    assert_eq!(s.shutdown().cancelled, 0);
}

#[test]
fn shutdown_drops_a_spawn_from_a_destructor_unpolled() {
    struct SpawnOnDrop {
        polled: Rc<Cell<bool>>,
        dropped: Rc<Cell<u32>>,
    }
    impl Drop for SpawnOnDrop {
        fn drop(&mut self) {
            let (polled, owned) = (self.polled.clone(), Owned(self.dropped.clone()));
            hooks::spawn("late", async move {
                let _owned = owned;
                polled.set(true);
            });
        }
    }
    let polled = shared(Cell::new(false));
    let dropped = shared(Cell::new(0));
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let guard = SpawnOnDrop {
        polled: polled.clone(),
        dropped: dropped.clone(),
    };
    let idle = s.spawn_root("idle", async move {
        let _guard = guard;
        std::future::pending::<()>().await
    });
    assert_eq!(s.run(idle.task()), zakura_scheduler::RunOutcome::Waiting);
    let report = s.shutdown();
    assert!(!polled.get(), "a shutdown spawn is never polled");
    assert_eq!(dropped.get(), 1, "its future is dropped");
    assert_eq!((report.cancelled, report.unwound), (2, 0));
}

/// An AB/BA pair where `ab` takes its second lock inside a `catch_unwind`,
/// as production handlers wrap work. Returns whether the handler ran and
/// whether a guard dropped in the frame saw an unwind.
fn cycle_in_catch_unwind(rethrow: bool) -> (CleanupReportSummary, bool, Option<bool>) {
    use zakura_scheduler::RunOutcome::Parked;
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let a = shared(Mutex::new(()));
    let b = shared(Mutex::new(()));
    let (a2, b2) = (a.clone(), b.clone());
    let caught = shared(Cell::new(false));
    let saw = shared(Cell::new(None));
    let (c, w) = (caught.clone(), saw.clone());
    let ab = s.spawn_root("ab", async move {
        let _x = a.lock();
        point();
        let guard = RecordUnwinding(w);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = guard;
            drop(b.lock());
        }));
        if let Err(payload) = result {
            c.set(true);
            if rethrow {
                std::panic::resume_unwind(payload);
            }
        }
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
    let r = s.shutdown();
    let summary = CleanupReportSummary {
        cancelled: r.cancelled,
        unwound: r.unwound,
        unwind_failures: r.unwind_failures,
    };
    (summary, caught.get(), saw.get())
}

/// The cleanup counts [`cycle_in_catch_unwind`] checks.
#[derive(Debug, PartialEq, Eq)]
struct CleanupReportSummary {
    cancelled: usize,
    unwound: usize,
    unwind_failures: usize,
}

#[test]
fn forced_unwind_passes_through_a_production_catch_unwind() {
    let stacks_before = unaccounted_stacks();
    for rethrow in [false, true] {
        let (report, caught, saw) = cycle_in_catch_unwind(rethrow);
        // Only the parked pair is affected, and only `ab` is force-unwound:
        // that releases `a`, so `ba` finishes in the drain.
        assert_eq!(
            report,
            CleanupReportSummary {
                cancelled: 0,
                unwound: 1,
                unwind_failures: 0
            },
            "rethrow {rethrow}"
        );
        assert!(caught, "the handler sees the forced unwind as a panic");
        assert_eq!(
            saw,
            Some(true),
            "a guard in the frame drops while unwinding"
        );
        assert_eq!(
            unaccounted_stacks(),
            stacks_before,
            "rethrow {rethrow}: no stack leaked"
        );
        // The thread can start another run.
        drop(Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO));
    }
}

/// An observation log that the harness freezes before shutdown, as the
/// simulation does with its property verdict.
#[derive(Default)]
struct Log {
    frozen: bool,
    events: Vec<&'static str>,
    rejected: Vec<&'static str>,
}

impl Log {
    /// Records `event`, or rejects it once the log is frozen.
    fn record(&mut self, event: &'static str) {
        if self.frozen {
            self.rejected.push(event);
        } else {
            self.events.push(event);
        }
    }
}

#[test]
fn cleanup_cannot_change_a_frozen_verdict() {
    use zakura_scheduler::RunOutcome::Parked;
    struct RecordOnDrop(Rc<RefCell<Log>>, &'static str, bool);
    impl Drop for RecordOnDrop {
        fn drop(&mut self) {
            self.0.borrow_mut().record(self.1);
            if self.2 {
                panic!("cleanup panicked");
            }
        }
    }
    let log = shared(RefCell::new(Log::default()));
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    // A live violation, then an idle task whose drop records and panics.
    let l = log.clone();
    let violator = s.spawn_root(
        "violator",
        async move { l.borrow_mut().record("violation") },
    );
    let guard = RecordOnDrop(log.clone(), "drop", true);
    let idle = s.spawn_root("idle", async move {
        let _guard = guard;
        std::future::pending::<()>().await
    });
    // A lock cycle whose handler reports the forced unwind as a failure.
    let a = shared(Mutex::new(()));
    let b = shared(Mutex::new(()));
    let (a2, b2, l) = (a.clone(), b.clone(), log.clone());
    let ab = s.spawn_root("ab", async move {
        let _x = a.lock();
        point();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(b.lock())));
        if caught.is_err() {
            l.borrow_mut().record("handler");
        }
    });
    let ba = s.spawn_root("ba", async move {
        let _x = b2.lock();
        point();
        let _y = a2.lock();
    });
    s.run(violator.task());
    s.run(idle.task());
    for task in [ab, ab, ba, ba, ab, ba] {
        s.run(task.task());
    }
    assert!(matches!(s.run(ab.task()), Parked(_)));
    assert!(matches!(s.run(ba.task()), Parked(_)));
    assert_eq!(s.status(), Status::Deadlock);

    // The harness freezes the verdict, then shuts down.
    let verdict = log.borrow().events.clone();
    log.borrow_mut().frozen = true;
    let report = s.shutdown();
    assert_eq!(log.borrow().events, verdict);
    assert_eq!(verdict, ["violation"]);
    let mut rejected = log.borrow().rejected.clone();
    rejected.sort_unstable();
    assert_eq!(rejected, ["drop", "handler"]);
    // The cleanup panic is a cleanup-report count, not a live outcome.
    assert_eq!(
        (report.cancelled, report.panicked, report.unwound),
        (0, 1, 1)
    );
}

#[test]
fn task_panic_reaches_the_harness_with_a_replaced_panic_hook() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    // The explorer replaces libfuzzer-sys's abort-on-panic hook with one that
    // returns. Under such a hook a task panic is still a result.
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let calls = hook_calls.clone();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
    }));
    let (r, _) = run_fixture(2, 0, |s| {
        s.spawn_root("boom", async { panic!("planted panic") });
        s.spawn_root("survivor", async { point() });
        Box::new(|_| None)
    });
    std::panic::set_hook(previous);
    assert_eq!(r.panics, ["planted panic"]);
    assert_eq!(r.status, Status::Quiescent, "the survivor finished");
    assert!(hook_calls.load(Ordering::SeqCst) >= 1);
}

/// Reruns `test` in a child with LeakSanitizer off, for a test that leaks
/// tasks on purpose, and checks it passed. Returns true in the parent, which
/// should then return. In the child, returns false so the body runs.
fn leaks_in_a_child(test: &str) -> bool {
    use std::process::{Command, Stdio};
    if in_child(test) {
        return false;
    }
    // Added to any ASan options already set, rather than replacing them.
    let asan = match std::env::var("ASAN_OPTIONS") {
        Ok(options) if !options.is_empty() => format!("{options}:detect_leaks=0"),
        _ => "detect_leaks=0".to_owned(),
    };
    let exe = std::env::current_exe().expect("the test binary has a path");
    let output = Command::new(exe)
        .args(["--exact", test, "--test-threads=1", "--nocapture"])
        .env(CHILD_TEST, test)
        .env("ASAN_OPTIONS", asan)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("the test binary can rerun itself");
    assert!(
        output.status.success(),
        "the child failed: {}\n{}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    true
}

/// Names the one test a child process started by [`rerun_in_child`] runs.
const CHILD_TEST: &str = "ZAKURA_SCHEDULER_CHILD_TEST";

/// True in the child process that [`rerun_in_child`] started for `test`.
fn in_child(test: &str) -> bool {
    std::env::var_os(CHILD_TEST).is_some_and(|t| t == test)
}

/// Reruns this test binary for the single test `test`, with `envs` set. The
/// child's stdout, uncaptured by the test harness, is piped back.
fn rerun_in_child(test: &str, envs: &[(&str, &str)]) -> std::process::Child {
    use std::process::{Command, Stdio};
    let exe = std::env::current_exe().expect("the test binary has a path");
    Command::new(exe)
        .args(["--exact", test, "--test-threads=1", "--nocapture"])
        .env(CHILD_TEST, test)
        .envs(envs.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("the test binary can rerun itself")
}

/// What the watchdog child prints right before its cleanup starts.
const CLEANUP_STARTED: &str = "zakura-scheduler watchdog child: cleanup started";

/// Hangs in cleanup on purpose, but only in the child process that
/// [`cleanup_that_never_reaches_a_point_trips_the_watchdog`] starts.
/// Anywhere else it returns at once.
#[test]
fn watchdog_child_spins_in_cleanup() {
    use std::io::Write;
    if !in_child("watchdog_child_spins_in_cleanup") {
        return;
    }
    // A destructor that waits for a flag only another task sets, with no
    // preemption point, so the scheduler can never switch to that task. A cap
    // on points per task during shutdown wouldn't stop it either. One that
    // spins through points is leaked at the cleanup point bound instead, as
    // `cleanup_that_spins_through_points_is_leaked` checks.
    struct SpinOnDrop(Rc<Cell<bool>>);
    impl Drop for SpinOnDrop {
        fn drop(&mut self) {
            while !self.0.get() {}
        }
    }
    let flag = shared(Cell::new(false));
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let guard = SpinOnDrop(flag.clone());
    let spinner = s.spawn_root("spinner", async move {
        let _guard = guard;
        std::future::pending::<()>().await
    });
    s.spawn_root("setter", async move { flag.set(true) });
    assert_eq!(s.run(spinner.task()), zakura_scheduler::RunOutcome::Waiting);
    // The test harness prints `test <name> ... ` first, without a newline.
    writeln!(std::io::stdout(), "{CLEANUP_STARTED}").expect("stdout is piped to the parent");
    s.shutdown();
}

#[test]
fn cleanup_that_spins_through_points_is_leaked() {
    if leaks_in_a_child("cleanup_that_spins_through_points_is_leaked") {
        return;
    }
    let report = shutdown_report(|s| {
        // A destructor that waits, through points, for a flag only another
        // task sets. Points don't switch during shutdown, so the setter never
        // runs, and the cleanup point bound stops the destructor instead.
        struct SpinOnDrop(Rc<Cell<bool>>);
        impl Drop for SpinOnDrop {
            fn drop(&mut self) {
                while !self.0.get() {
                    point();
                }
            }
        }
        let flag = shared(Cell::new(false));
        let guard = SpinOnDrop(flag.clone());
        let spinner = s.spawn_root("spinner", async move {
            let _guard = guard;
            std::future::pending::<()>().await
        });
        s.spawn_root("setter", async move { flag.set(true) });
        assert_eq!(s.run(spinner.task()), zakura_scheduler::RunOutcome::Waiting);
    });
    // The spinner stuck inside its cancellation, so it and the setter are
    // both leaked.
    assert_eq!((report.leaked, report.cancelled), (2, 0));
}

#[test]
fn cleanup_that_never_reaches_a_point_trips_the_watchdog() {
    use std::{
        io::{BufRead, BufReader},
        sync::mpsc,
        time::{Duration, Instant},
    };
    let mut child = rerun_in_child("watchdog_child_spins_in_cleanup", &[]);
    // The child must reach its cleanup, so a slow or hung startup can't pass.
    let stdout = child.stdout.take().expect("the child's stdout is piped");
    let (started, wait_started) = mpsc::channel();
    std::thread::spawn(move || {
        let reached = BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
            .any(|line| line.ends_with(CLEANUP_STARTED));
        if reached {
            let _ = started.send(());
        }
    });
    if wait_started.recv_timeout(Duration::from_secs(30)).is_err() {
        let _ = child.kill();
        let status = child.wait().expect("the child can be reaped");
        panic!("the child never reached its cleanup: {status}");
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("the child is ours to wait on") {
            panic!("the spinning cleanup ended on its own: {status}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    // The watchdog fires. That's a harness failure, not a property result.
    child.kill().expect("the child is still running");
    child.wait().expect("the killed child can be reaped");
}

/// Stacks the pool can't account for: allocated, but neither freed nor idle.
/// Between runs that's only stacks an unwind failure leaked.
fn unaccounted_stacks() -> u64 {
    let pool = zakura_scheduler::stack_pool();
    pool.allocated - pool.freed - pool.idle as u64 // idle is at most STACK_POOL_LIMIT
}

/// Runs one task that finishes, with stacks of `stack_size` bytes.
fn one_clean_run(stack_size: usize) {
    let config = SchedulerConfig {
        stack_size,
        ..SchedulerConfig::default()
    };
    let mut s = Scheduler::new(config, SimInstant::ZERO);
    let task = s.spawn_root("task", async {});
    assert_eq!(s.run(task.task()), zakura_scheduler::RunOutcome::Completed);
    assert_eq!(s.shutdown().unwind_failures, 0);
}

#[test]
fn changing_the_stack_size_frees_the_pool_without_a_false_leak() {
    let before = unaccounted_stacks();
    one_clean_run(64 * 1024);
    // A new size empties the pool, which frees its 64 KiB stack.
    one_clean_run(128 * 1024);
    assert_eq!(
        unaccounted_stacks(),
        before,
        "{:?}",
        zakura_scheduler::stack_pool()
    );
}

#[test]
fn stacks_past_the_pool_limit_are_freed_without_a_false_leak() {
    let before = unaccounted_stacks();
    let config = SchedulerConfig {
        stack_size: 64 * 1024,
        ..SchedulerConfig::default()
    };
    let mut s = Scheduler::new(config, SimInstant::ZERO);
    // More live tasks than the pool keeps. Shutdown cancels them all, and the
    // pool frees the stacks past its limit.
    for _ in 0..1_030 {
        s.spawn_root("idle", std::future::pending::<()>());
    }
    assert_eq!(s.shutdown().cancelled, 1_030);
    assert_eq!(zakura_scheduler::stack_pool().idle, 1_024);
    assert_eq!(
        unaccounted_stacks(),
        before,
        "{:?}",
        zakura_scheduler::stack_pool()
    );
}

#[test]
fn draw_kind_names_are_pinned() {
    use zakura_scheduler::DrawKind::{HashSeed, Rng, Select, StreamMap};
    // Draw identities and traces carry these names, so a rename fails here.
    assert_eq!(
        [Rng, Select, HashSeed, StreamMap].map(|kind| kind.name()),
        ["rng", "select", "hash-seed", "stream-map"]
    );
}
