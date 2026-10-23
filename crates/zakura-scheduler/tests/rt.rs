//! `zakura-rt`'s sim primitives running on the scheduler. These only build
//! with `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::{Cell, RefCell},
    future::pending,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use support::*;
use zakura_rt::{
    sim,
    sync::{
        atomic::{AtomicU64 as RtAtomicU64, Ordering as RtOrdering},
        Mutex as RtMutex, Notify, Semaphore,
    },
    task::{self, JoinSet},
    time::{self, Instant},
};
use zakura_scheduler::{Scheduler, SchedulerConfig, SimInstant, Status};

/// Seeds to try when a planted race must be found.
const FIND: u64 = 2_000;
/// Seeds that must all pass on correct code.
const CLEAN: u64 = 2_000;

#[test]
fn scheduler_is_the_rt_executor_for_its_run() {
    assert!(sim::executor().is_none());
    let sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    assert!(sim::executor().is_some());
    drop(sched);
    assert!(sim::executor().is_none());
}

/// An executor for holding `zakura-rt`'s run slot. It's never called.
struct Unused;

impl sim::Executor for Unused {
    fn preempt(&self, _: sim::Point) {}
    fn park(&self, _: sim::ObjectId, _: Option<sim::TaskId>) -> bool {
        unreachable!()
    }
    fn unpark_all(&self, _: sim::ObjectId) {}
    fn spawn(&self, _: &'static str, _: sim::BoxTask) -> sim::TaskId {
        unreachable!()
    }
    fn try_spawn(&self, _: &'static str, _: sim::BoxTask) -> Option<sim::TaskId> {
        unreachable!()
    }
    fn abort(&self, _: sim::TaskId) {}
    fn is_finished(&self, _: sim::TaskId) -> bool {
        unreachable!()
    }
    fn poll_join(
        &self,
        _: sim::TaskId,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<sim::TaskExit> {
        unreachable!()
    }
    fn now(&self) -> Duration {
        unreachable!()
    }
    fn register_timer(&self, _: Duration, _: &std::task::Waker) -> sim::TimerId {
        unreachable!()
    }
    fn cancel_timer(&self, _: sim::TimerId) {}
    fn new_object(&self) -> sim::ObjectId {
        unreachable!()
    }
    fn current_task(&self) -> Option<sim::TaskId> {
        None
    }
    fn set_external_wait(&self, _: bool) {}
    fn draw(&self, _: sim::DrawSite, _: u64) -> u64 {
        unreachable!()
    }
}

#[test]
fn failed_rt_enter_leaves_no_scheduler_run_behind() {
    let other = sim::enter(&Unused);
    let entered = std::panic::catch_unwind(|| {
        drop(Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO));
    });
    assert!(entered.is_err(), "zakura-rt already had a run");
    drop(other);
    // Used to fail with "this thread already has a running Scheduler".
    drop(Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO));
}

/// Two tasks read a counter, stop in `zakura-rt`, then write it back. With
/// `yield_now` they stop with `yield_now`, and otherwise at an expired sleep,
/// which is one preemption point inside the poll.
fn gap_between_load_and_store(yield_now: bool) -> impl Fn(&mut Scheduler) -> Check {
    move |s| {
        let counter = shared(Cell::new(0));
        for label in ["inc_a", "inc_b"] {
            let counter = counter.clone();
            s.spawn_root(label, async move {
                let v = counter.get();
                if yield_now {
                    task::yield_now().await;
                } else {
                    time::sleep(Duration::ZERO).await;
                }
                counter.set(v + 1);
            });
        }
        Box::new(move |_| (counter.get() != 2).then(|| format!("counter = {}", counter.get())))
    }
}

#[test]
fn rt_points_preempt_inside_a_poll() {
    assert!(first_violation(2, FIND, gap_between_load_and_store(false)).is_some());
    // The point is mid-poll, so with one worker the other task can't run there.
    assert_eq!(
        first_violation(1, FIND, gap_between_load_and_store(false)),
        None
    );
}

#[test]
fn rt_yield_now_frees_the_worker() {
    // `yield_now` returns `Pending` once, so even one worker finds the race.
    assert!(first_violation(1, FIND, gap_between_load_and_store(true)).is_some());
}

#[test]
fn spawn_join_panic_and_abort_go_through_the_scheduler() {
    let (result, violation) = run_fixture(2, 0, |s| {
        let seen = shared(RefCell::new(None));
        let out = seen.clone();
        s.spawn_root("root", async move {
            let value = task::spawn(async { 5 }).await.unwrap();
            let panicked = task::spawn(async { panic!("boom") }).await.unwrap_err();
            let payload = *panicked.into_panic().downcast::<&str>().unwrap();
            let sleeper = task::spawn(time::sleep(Duration::from_secs(10)));
            sleeper.abort();
            let cancelled = sleeper.await.unwrap_err().is_cancelled();
            *out.borrow_mut() = Some((value, payload, cancelled));
        });
        Box::new(move |r| {
            let seen = *seen.borrow();
            (seen != Some((5, "boom", true)) || r.status != Status::Quiescent)
                .then(|| format!("{seen:?}, {:?}", r.status))
        })
    });
    assert_eq!(violation, None);
    assert_eq!(result.panics, ["boom"]);
    assert_eq!(
        result.report.timers_left, 0,
        "the aborted sleep cancelled its timer"
    );
    assert!(
        result
            .picks
            .iter()
            .any(|p| p.starts_with("root.1/") && p.contains("rt.rs")),
        "rt spawns are children of the spawning task, labeled by file: {:?}",
        result.picks
    );
}

#[test]
fn named_spawns_take_their_label() {
    let (result, violation) = run_fixture(1, 0, |s| {
        s.spawn_root("root", async {
            task::spawn_named("worker", async {}).await.unwrap();
            task::spawn_named("worker", async {}).await.unwrap();
        });
        Box::new(|_| None)
    });
    assert_eq!(violation, None);
    for name in ["root.1/worker.1", "root.1/worker.2"] {
        assert!(
            result
                .picks
                .iter()
                .any(|p| p.starts_with(&format!("{name}:"))),
            "{name} in {:?}",
            result.picks
        );
    }
}

#[test]
fn rt_time_reads_the_virtual_clock() {
    for seed in 0..20 {
        let (result, violation) = run_fixture(2, seed, |s| {
            let elapsed = shared(Cell::new(None));
            let out = elapsed.clone();
            s.spawn_root("root", async move {
                let start = Instant::now();
                time::sleep(Duration::from_secs(3600)).await;
                let timed_out = time::timeout(Duration::from_secs(5), pending::<()>())
                    .await
                    .is_err();
                assert!(timed_out);
                out.set(Some(start.elapsed()));
            });
            Box::new(move |_| {
                (elapsed.get() != Some(Duration::from_secs(3605)))
                    .then(|| format!("elapsed {:?}", elapsed.get()))
            })
        });
        assert_eq!(violation, None, "seed {seed}");
        let advances = result.picks.iter().filter(|p| *p == "advance-time").count();
        assert_eq!(advances, 2, "one advance per deadline");
    }
}

#[test]
fn join_set_tasks_join_in_finish_order() {
    let (_, violation) = run_fixture(2, 3, |s| {
        let order = shared(RefCell::new(Vec::new()));
        let out = order.clone();
        s.spawn_root("root", async move {
            let mut set = JoinSet::new();
            for secs in [3, 1, 2] {
                set.spawn(async move {
                    time::sleep(Duration::from_secs(secs)).await;
                    secs
                });
            }
            while let Some(done) = set.join_next().await {
                out.borrow_mut().push(done.unwrap());
            }
        });
        Box::new(move |_| (*order.borrow() != [1, 2, 3]).then(|| format!("{:?}", order.borrow())))
    });
    assert_eq!(violation, None);
}

#[test]
fn rt_try_spawn_past_the_task_limit_returns_none() {
    let config = SchedulerConfig {
        max_tasks: 2,
        ..SchedulerConfig::default()
    };
    let mut s = Scheduler::new(config, SimInstant::ZERO);
    let spawned = shared(Cell::new(None));
    let out = spawned.clone();
    let root = s.spawn_root("root", async move {
        let child = task::try_spawn(async {}).is_some();
        let past_limit = task::try_spawn(async {}).is_some();
        out.set(Some((child, past_limit)));
    });
    while !root.is_finished() {
        s.run(root.task());
    }
    assert_eq!(spawned.get(), Some((true, false)));
}

#[test]
fn rt_spawn_from_setup_code_is_ready_at_once() {
    let (result, violation) = run_fixture(2, 0, |_| {
        // `zakura-rt`'s spawn takes `Send` futures, as tokio's does.
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        // Setup code outside any task, as when a harness builds a component
        // that spawns its own tasks.
        drop(task::spawn(
            async move { flag.store(true, Ordering::Relaxed) },
        ));
        Box::new(move |_| {
            (!ran.load(Ordering::Relaxed)).then(|| "the setup spawn never ran".to_owned())
        })
    });
    assert_eq!(violation, None);
    assert_eq!(result.status, Status::Quiescent);
}

#[test]
fn sleep_reset_into_the_past_fires_at_the_next_advance() {
    let (result, violation) = run_fixture(2, 0, |s| {
        s.spawn_root("root", async {
            let mut early = std::pin::pin!(time::sleep(Duration::from_secs(10)));
            assert!(futures_poll_once(early.as_mut()).is_pending());
            time::sleep(Duration::from_secs(20)).await;
            // Registers a timer for 5 s while the clock reads 20 s.
            early
                .as_mut()
                .reset(Instant::now() - Duration::from_secs(15));
            time::sleep(Duration::from_secs(1)).await;
        });
        Box::new(|_| None)
    });
    assert_eq!(violation, None);
    assert_eq!(result.status, Status::Quiescent);
}

#[test]
fn clamped_timer_cancels() {
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let root = sched.spawn_root("root", async {
        time::sleep(Duration::from_secs(20)).await;
        let mut early = std::pin::pin!(time::sleep(Duration::from_secs(1)));
        assert!(futures_poll_once(early.as_mut()).is_pending());
        // A deadline 15 s in the past registers at the current 20 s.
        early
            .as_mut()
            .reset(Instant::now() - Duration::from_secs(15));
        assert_eq!(
            zakura_scheduler::hooks::now().map(|now| now.since_start()),
            Some(Duration::from_secs(20))
        );
        // Dropping the sleep cancels the timer under its clamped deadline.
    });
    while !root.is_finished() {
        match sched.status() {
            Status::Running => {
                sched.run(root.task());
            }
            Status::WaitingOnTime => {
                let next = sched.next_deadline().expect("a timer is pending");
                sched.advance_to(next);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(sched.next_deadline(), None, "the clamped timer was removed");
    assert_eq!(sched.shutdown().timers_left, 0);
}

#[test]
fn second_joiner_gets_stand_in() {
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let executor = sim::executor().expect("the scheduler is installed");
    let task = executor.spawn("boom", Box::pin(async { panic!("boom") }));
    let ready = sched.ready()[0].task;
    assert!(matches!(
        sched.run(ready),
        zakura_scheduler::RunOutcome::Panicked(_)
    ));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut payload = || match executor.poll_join(task, &mut cx) {
        std::task::Poll::Ready(sim::TaskExit::Panicked(payload)) => payload,
        other => panic!("expected a panic, got {other:?}"),
    };
    assert_eq!(*payload().downcast::<&str>().unwrap(), "boom");
    let stand_in = *payload().downcast::<&str>().unwrap();
    assert!(
        stand_in.contains("another joiner took the payload"),
        "{stand_in}"
    );
    sched.shutdown();
}

/// Polls `future` once with a no-op waker.
fn futures_poll_once<F: std::future::Future>(
    future: std::pin::Pin<&mut F>,
) -> std::task::Poll<F::Output> {
    future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

/// Reads the virtual clock when dropped, as a destructor that logs a
/// timestamp would.
struct ReadsClockOnDrop(Arc<AtomicBool>);

impl Drop for ReadsClockOnDrop {
    fn drop(&mut self) {
        let _ = Instant::now();
        self.0.store(true, Ordering::Relaxed);
    }
}

#[test]
fn setup_spawn_dropped_at_shutdown_reads_the_clock() {
    let dropped = Arc::new(AtomicBool::new(false));
    let sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let guard = ReadsClockOnDrop(dropped.clone());
    // Spawned from setup code, and never run before shutdown.
    drop(task::spawn(async move {
        let _guard = guard;
        pending::<()>().await
    }));
    let report = sched.shutdown();
    assert!(dropped.load(Ordering::Relaxed));
    assert_eq!(report.cancelled, 1);
}

#[test]
fn unjoined_panic_payload_dropped_at_shutdown_reads_the_clock() {
    let dropped = Arc::new(AtomicBool::new(false));
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let guard = ReadsClockOnDrop(dropped.clone());
    let task = task::spawn(async move { std::panic::panic_any(guard) });
    let ready = sched.ready()[0].task;
    assert!(matches!(
        sched.run(ready),
        zakura_scheduler::RunOutcome::Panicked(_)
    ));
    assert!(
        !dropped.load(Ordering::Relaxed),
        "nobody joined the payload"
    );
    drop(task);
    sched.shutdown();
    assert!(dropped.load(Ordering::Relaxed));
}

#[test]
fn same_seed_replays_rt_programs() {
    let picks = |seed| {
        run_fixture(2, seed, gap_between_load_and_store(true))
            .0
            .picks
    };
    for seed in 0..50 {
        assert_eq!(picks(seed), picks(seed), "seed {seed}");
    }
}

/// Discovery's `wait_complete` shape on `zakura-rt`'s `Notify`: check a flag,
/// then create `notified()`. The fixed version creates and enables the
/// future before checking.
fn missed_notify(fixed: bool) -> impl Fn(&mut Scheduler) -> Check {
    move |s| {
        let flag = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        let timed_out = Arc::new(AtomicBool::new(false));
        let (f, n, t) = (flag.clone(), notify.clone(), timed_out.clone());
        s.spawn_root("waiter", async move {
            let wait = async {
                if fixed {
                    let mut notified = std::pin::pin!(n.notified());
                    notified.as_mut().enable();
                    if !f.load(Ordering::Relaxed) {
                        notified.await;
                    }
                } else if !f.load(Ordering::Relaxed) {
                    n.notified().await;
                }
            };
            if time::timeout(Duration::from_secs(2), wait).await.is_err() {
                t.store(true, Ordering::Relaxed);
            }
        });
        s.spawn_root("marker", async move {
            flag.store(true, Ordering::Relaxed);
            notify.notify_waiters();
        });
        Box::new(move |_| {
            timed_out
                .load(Ordering::Relaxed)
                .then(|| "timed out".to_owned())
        })
    }
}

#[test]
fn rt_notify_finds_the_missed_wakeup() {
    assert!(first_violation(2, FIND, missed_notify(false)).is_some());
    assert_eq!(first_violation(2, CLEAN, missed_notify(true)), None);
}

/// `SessionReservation::drop`'s shape: one task holds a permit from each of
/// two pools and drops them in turn. A second task takes from both and
/// reports "full" if it gets only the first.
fn back_to_back_release(s: &mut Scheduler) -> Check {
    let a = Arc::new(Semaphore::new(1));
    let b = Arc::new(Semaphore::new(1));
    let partial = Arc::new(AtomicBool::new(false));
    let (ha, hb) = (
        Arc::clone(&a).try_acquire_owned().unwrap(),
        Arc::clone(&b).try_acquire_owned().unwrap(),
    );
    s.spawn_root("holder", async move {
        drop(ha);
        drop(hb);
    });
    let p = partial.clone();
    s.spawn_root("reserver", async move {
        if let Ok(_first) = a.try_acquire() {
            if b.try_acquire().is_err() {
                p.store(true, Ordering::Relaxed);
            }
        }
    });
    Box::new(move |_| {
        partial
            .load(Ordering::Relaxed)
            .then(|| "false full".to_owned())
    })
}

#[test]
fn release_points_split_back_to_back_drops() {
    assert!(first_violation(2, FIND, back_to_back_release).is_some());
}

/// Two tasks load and then store one `zakura-rt` atomic.
fn rt_lost_update(s: &mut Scheduler) -> Check {
    let counter = Arc::new(RtAtomicU64::new(0));
    for label in ["inc_a", "inc_b"] {
        let counter = counter.clone();
        s.spawn_root(label, async move {
            let v = counter.load(RtOrdering::SeqCst);
            counter.store(v + 1, RtOrdering::SeqCst);
        });
    }
    Box::new(move |_| {
        let v = counter.load(RtOrdering::SeqCst);
        (v != 2).then(|| format!("counter = {v}"))
    })
}

#[test]
fn rt_atomics_find_the_lost_update() {
    assert!(first_violation(2, FIND, rt_lost_update).is_some());
    assert_eq!(first_violation(1, FIND, rt_lost_update), None);
}

/// Checks capacity under one lock, then reserves under a second lock.
fn rt_snapshot_reserve(fixed: bool) -> impl Fn(&mut Scheduler) -> Check {
    move |s| {
        let available = Arc::new(RtMutex::new(1i64));
        for label in ["res_a", "res_b"] {
            let available = available.clone();
            s.spawn_root(label, async move {
                if fixed {
                    let mut guard = available.lock().unwrap();
                    if *guard >= 1 {
                        *guard -= 1;
                    }
                } else {
                    let ok = *available.lock().unwrap() >= 1;
                    if ok {
                        *available.lock().unwrap() -= 1;
                    }
                }
            });
        }
        Box::new(move |r| {
            let v = *available.lock().unwrap();
            (v < 0 || r.status != Status::Quiescent).then(|| format!("{v}, {:?}", r.status))
        })
    }
}

#[test]
fn rt_mutex_parks_contended_tasks() {
    assert!(first_violation(2, FIND, rt_snapshot_reserve(false)).is_some());
    assert_eq!(first_violation(2, CLEAN, rt_snapshot_reserve(true)), None);
}

/// Two tasks take two `zakura-rt` mutexes in opposite orders.
fn rt_ab_ba(s: &mut Scheduler) -> Check {
    let a = Arc::new(RtMutex::new(()));
    let b = Arc::new(RtMutex::new(()));
    for (label, first, second) in [("ab", a.clone(), b.clone()), ("ba", b, a)] {
        s.spawn_root(label, async move {
            let _first = first.lock().unwrap();
            let _second = second.lock().unwrap();
        });
    }
    Box::new(|r| (r.status == Status::Deadlock).then(|| "deadlock".to_owned()))
}

#[test]
fn rt_mutex_reports_a_lock_cycle_as_deadlock() {
    assert!(first_violation(2, FIND, rt_ab_ba).is_some());
}

#[test]
fn rt_mutex_held_across_await_starves_one_worker() {
    let held_across_await = |s: &mut Scheduler| -> Check {
        let lock = Arc::new(RtMutex::new(()));
        let held = lock.clone();
        s.spawn_root("holder", async move {
            #[allow(clippy::await_holding_lock)]
            {
                let _guard = held.lock().unwrap();
                time::sleep(Duration::from_secs(1)).await;
            }
        });
        s.spawn_root("waiter", async move {
            drop(lock.lock().unwrap());
        });
        Box::new(|r| (r.status == Status::WorkerStarved).then(|| "starved".to_owned()))
    };
    assert!(first_violation(1, FIND, held_across_await).is_some());
    assert_eq!(first_violation(2, CLEAN, held_across_await), None);
}

/// Runs `task` until it returns `Pending`, which leaves it waiting at a poll
/// boundary.
fn run_until_waiting(s: &mut Scheduler, task: zakura_scheduler::TaskId) {
    while s.run(task) != zakura_scheduler::RunOutcome::Waiting {}
}

#[test]
fn shutdown_leaves_a_mutex_held_by_an_idle_task_unpoisoned() {
    // A forced unwind dropped the guard while panicking, which poisons a std
    // mutex. Shutdown now cancels the idle holder instead.
    let lock = Arc::new(RtMutex::new(()));
    let held = lock.clone();
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let holder = s.spawn_root("holder", async move {
        #[allow(clippy::await_holding_lock)]
        {
            let _guard = held.lock().unwrap();
            pending::<()>().await;
        }
    });
    run_until_waiting(&mut s, holder.task());
    let report = s.shutdown();
    assert!(!lock.is_poisoned());
    assert_eq!((report.cancelled, report.unwound), (1, 0));
}

#[test]
fn shutdown_lets_a_drop_take_a_contended_mutex() {
    // `dropper`'s destructor locks the mutex `holder` keeps across an await.
    // A forced unwind ran that destructor while panicking, so `park` refused
    // and the contended-lock assert aborted the process.
    struct LockOnDrop(Arc<RtMutex<()>>, Arc<AtomicBool>);
    impl Drop for LockOnDrop {
        fn drop(&mut self) {
            drop(self.0.lock().unwrap());
            self.1.store(true, Ordering::Relaxed);
        }
    }
    let lock = Arc::new(RtMutex::new(()));
    let locked = Arc::new(AtomicBool::new(false));
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let guard = LockOnDrop(lock.clone(), locked.clone());
    let dropper = s.spawn_root("dropper", async move {
        let _guard = guard;
        pending::<()>().await
    });
    let holder = s.spawn_root("holder", async move {
        #[allow(clippy::await_holding_lock)]
        {
            let _guard = lock.lock().unwrap();
            pending::<()>().await;
        }
    });
    run_until_waiting(&mut s, dropper.task());
    run_until_waiting(&mut s, holder.task());
    let report = s.shutdown();
    assert!(locked.load(Ordering::Relaxed));
    assert_eq!((report.cancelled, report.unwound), (2, 0));
}

#[test]
fn mutex_kept_across_runs_gets_a_fresh_identity() {
    use zakura_scheduler::RunOutcome::Parked;
    let kept = Arc::new(RtMutex::new(()));
    // Run 1: the first lock made by task 0 names the mutex.
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let used = kept.clone();
    let task = s.spawn_root("user", async move { drop(used.lock().unwrap()) });
    while s.status() == Status::Running {
        s.run(task.task());
    }
    drop(s);

    // Run 2: task 0 locks a fresh mutex first, so it gets the same identity
    // the kept mutex cached in run 1.
    let fresh = Arc::new(RtMutex::new(()));
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let (f, k) = (fresh.clone(), kept.clone());
    let holder = s.spawn_root("holder", async move {
        #[allow(clippy::await_holding_lock)]
        {
            let _f = f.lock().unwrap();
            let _k = k.lock().unwrap();
            pending::<()>().await;
        }
    });
    let wait_fresh = s.spawn_root("wait_fresh", async move { drop(fresh.lock().unwrap()) });
    let wait_kept = s.spawn_root("wait_kept", async move { drop(kept.lock().unwrap()) });
    run_until_waiting(&mut s, holder.task());
    let parked_on = |s: &mut Scheduler, task: zakura_scheduler::JoinHandle| loop {
        if let Parked(object) = s.run(task.task()) {
            break object;
        }
    };
    let fresh_id = parked_on(&mut s, wait_fresh);
    let kept_id = parked_on(&mut s, wait_kept);
    assert_ne!(fresh_id, kept_id);
}

/// Runs one task that draws an `rng()` value and builds a map, with `seed`
/// answering every draw. Returns the draws seen, the value, and key order.
fn drawing_run(seed: u64) -> (Vec<zakura_scheduler::Draw>, u64, Vec<u32>) {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let out = Rc::new(RefCell::new(None));
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let log = seen.clone();
    sched.set_draws(move |draw| {
        log.borrow_mut().push(*draw);
        seed
    });
    let result = out.clone();
    let root = sched.spawn_root("root", async move {
        use rand_10::Rng;
        let value = zakura_rt::rand::rng().next_u64();
        let map: zakura_rt::collections::HashMap<u32, ()> = (0..32).map(|k| (k, ())).collect();
        *result.borrow_mut() = Some((value, map.keys().copied().collect()));
    });
    while sched.status() == Status::Running {
        sched.run(root.task());
    }
    sched.shutdown();
    let (value, order) = out.borrow_mut().take().unwrap();
    let draws = seen.borrow().clone();
    (draws, value, order)
}

#[test]
fn draws_reach_the_scheduler_draw_source() {
    let (draws, value, order) = drawing_run(7);
    assert_eq!(value, drawing_run(7).1, "the stream replays from its seed");
    assert_ne!(value, drawing_run(8).1, "the seed changes the stream");
    let kinds: Vec<_> = draws.iter().map(|d| (d.kind, d.bound)).collect();
    assert_eq!(
        kinds,
        [
            (zakura_scheduler::DrawKind::Rng, 0),
            (zakura_scheduler::DrawKind::HashSeed, 0)
        ]
    );
    assert!(
        draws.iter().all(|d| d.task.is_some()),
        "both came from the task"
    );
    assert!(draws[0].site.file().ends_with("rt.rs"));

    assert_eq!(order, drawing_run(7).2, "one seed, one order");
    assert!(
        (0..8).any(|seed| drawing_run(seed).2 != order),
        "the seed changes iteration order"
    );
}

#[test]
fn select_point_reports_the_callers_site() {
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let root = sched.spawn_root("root", async {
        zakura_rt::select! {
            () = std::future::ready(()) => {},
        }
    });
    sched.run(root.task());
    let at = sched.ready()[0].at;
    assert_eq!(at.op, zakura_scheduler::Op::Select);
    let site = at.site.expect("a select point has a site");
    assert!(site.file().ends_with("tests/rt.rs"), "{site}");
}

/// Runs `f` as one task on a scheduler with no draw source, so every `Plan`
/// draw answers 0, as once a fuzz input's bytes run out.
fn with_zero_draws<T: 'static>(f: impl FnOnce() -> T + 'static) -> T {
    let out = Rc::new(RefCell::new(None));
    let result = out.clone();
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let root = sched.spawn_root("root", async move {
        *result.borrow_mut() = Some(f());
    });
    while sched.status() == Status::Running {
        sched.run(root.task());
    }
    sched.shutdown();
    let value = out.borrow_mut().take();
    value.expect("the task ran to completion")
}

#[test]
fn rng_values_are_not_stuck_at_zero_when_draws_are() {
    use rand_10::Rng;
    // The handler's session seed loops until it draws a nonzero value, so a
    // stream stuck at 0 hung it.
    let values = with_zero_draws(|| {
        let mut rng = zakura_rt::rand::rng();
        (0..4).map(|_| rng.next_u64()).collect::<Vec<_>>()
    });
    assert!(values.iter().all(|v| *v != 0), "{values:?}");
}

#[test]
fn rng_ranges_and_shuffles_spread_when_draws_are_zero() {
    use rand_10::{seq::SliceRandom, RngExt};
    let (digits, shuffles) = with_zero_draws(|| {
        let mut rng = zakura_rt::rand::rng();
        let digits: std::collections::BTreeSet<u32> =
            (0..1_000).map(|_| rng.random_range(0..10)).collect();
        let shuffles: std::collections::BTreeSet<Vec<u8>> = (0..1_000)
            .map(|_| {
                let mut values: Vec<u8> = (0..8).collect();
                values.shuffle(&mut rng);
                values
            })
            .collect();
        (digits, shuffles.len())
    });
    // One-byte draws used to make `random_range(0..10)` always 0, and an
    // 8-element shuffle reach only 29 of its 40,320 orders.
    assert_eq!(digits.len(), 10);
    assert!(shuffles > 900, "{shuffles} distinct shuffles");
}
