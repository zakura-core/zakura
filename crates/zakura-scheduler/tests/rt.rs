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

/// A preemption point with no shared state behind it.
#[track_caller]
fn point() {
    zakura_scheduler::hooks::preempt(zakura_scheduler::Point::here(zakura_scheduler::Op::Yield));
}

/// `n` seconds.
fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// How many times the run advanced time.
fn advances(result: &RunResult) -> usize {
    result.picks.iter().filter(|p| *p == "advance-time").count()
}

#[test]
fn futures_unordered_polls_only_the_woken_child() {
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::{future::Future, pin::Pin};
    for seed in 0..50 {
        let (_, violation) = run_fixture(2, seed, |s| {
            let polls = shared(RefCell::new([0u32; 3]));
            let order = shared(RefCell::new(Vec::new()));
            let (p, o) = (polls.clone(), order.clone());
            let (tx, rx) = zakura_rt::sync::oneshot::channel::<()>();
            s.spawn_root("sender", async move {
                time::sleep(secs(15)).await;
                let _ = tx.send(());
            });
            s.spawn_root("parent", async move {
                // Two timers and a channel, each polled only when its own
                // waker fires.
                let children: [Pin<Box<dyn Future<Output = ()>>>; 3] = [
                    Box::pin(time::sleep(secs(10))),
                    Box::pin(time::sleep(secs(20))),
                    Box::pin(async move {
                        let _ = rx.await;
                    }),
                ];
                let set = FuturesUnordered::new();
                for (i, mut child) in children.into_iter().enumerate() {
                    let p = p.clone();
                    set.push(std::future::poll_fn(move |cx| {
                        p.borrow_mut()[i] += 1;
                        child.as_mut().poll(cx).map(|()| i)
                    }));
                }
                let mut set = std::pin::pin!(set);
                while let Some(i) = set.next().await {
                    o.borrow_mut().push(i);
                }
            });
            Box::new(move |r| {
                let got = (*polls.borrow(), order.borrow().clone());
                (got != ([2, 2, 2], vec![0, 2, 1]) || r.status != Status::Quiescent)
                    .then(|| format!("{got:?}, {:?}", r.status))
            })
        });
        assert_eq!(violation, None, "seed {seed}");
    }
}

#[test]
fn select_branch_wakes_from_its_own_timer_or_channel() {
    // The channel fires first at 5 s, or the 10 s timer beats it.
    for (send_at, want, want_advances) in [(5, "recv", 1), (15, "sleep", 2)] {
        for seed in 0..20 {
            let (result, violation) = run_fixture(2, seed, |s| {
                let (tx, mut rx) = zakura_rt::sync::mpsc::channel::<u32>(1);
                s.spawn_root("sender", async move {
                    time::sleep(secs(send_at)).await;
                    let _ = tx.send(1).await;
                });
                let seen = shared(Cell::new(None));
                let out = seen.clone();
                s.spawn_root("root", async move {
                    let start = Instant::now();
                    let got = zakura_rt::select! {
                        () = time::sleep(secs(10)) => "sleep",
                        _ = rx.recv() => "recv",
                    };
                    out.set(Some((got, start.elapsed())));
                });
                Box::new(move |r| {
                    let want = Some((want, secs(send_at.min(10))));
                    (seen.get() != want || r.report.timers_left != 0)
                        .then(|| format!("{:?}, {} timers", seen.get(), r.report.timers_left))
                })
            });
            assert_eq!(violation, None, "send at {send_at}, seed {seed}");
            // The losing sleep dropped its timer, so time never visits it.
            assert_eq!(advances(&result), want_advances, "send at {send_at}");
        }
    }
}

#[test]
fn watch_lock_blocks_task_switches_until_it_drops() {
    use zakura_scheduler::{Op, RunOutcome};
    for seed in 0..200 {
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let (tx, rx) = zakura_rt::sync::watch::channel(0u64);
        let holding = shared(Cell::new(false));
        let counter = Arc::new(RtAtomicU64::new(0));
        let (h, c) = (holding.clone(), counter.clone());
        // Keeps the channel open after the reader ends.
        let keep = rx.clone();
        let reader = s.spawn_root("reader", async move {
            let value = rx.borrow();
            h.set(true);
            // Both points are skipped: switching to the writer here would
            // block the run's thread on tokio's lock.
            c.store(*value, RtOrdering::SeqCst);
            c.fetch_add(1, RtOrdering::SeqCst);
            h.set(false);
            drop(value);
            c.load(RtOrdering::SeqCst);
        });
        let (h, c) = (holding.clone(), counter.clone());
        s.spawn_root("writer", async move {
            let _keep = keep;
            tx.send(1).unwrap();
            tx.send_modify(|v| {
                h.set(true);
                c.store(*v, RtOrdering::SeqCst);
                h.set(false);
                *v += 1;
            });
            c.load(RtOrdering::SeqCst);
        });
        let mut walk = Walk(seed);
        let mut reader_load_point = false;
        while s.status() == Status::Running {
            let ready = s.ready();
            let task = ready[walk.pick(ready.len())].task;
            let outcome = s.run(task);
            assert!(!holding.get(), "seed {seed}: switched under a watch lock");
            if let RunOutcome::Reached(at) = outcome {
                assert!(
                    !matches!(at.op, Op::AtomicStore | Op::AtomicRmw),
                    "seed {seed}: a point under the lock suspended"
                );
                reader_load_point |= task == reader.task() && at.op == Op::AtomicLoad;
            }
        }
        assert_eq!(s.status(), Status::Quiescent, "seed {seed}");
        assert!(
            reader_load_point,
            "seed {seed}: points resume after release"
        );
        assert_eq!(sim::suppressed_points(), 3, "seed {seed}");
        s.shutdown();
    }
}

/// A waker that counts its wakes.
struct CountWakes(std::sync::atomic::AtomicUsize);

impl std::task::Wake for CountWakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A counting waker and its count.
fn counting_waker() -> (std::task::Waker, Arc<CountWakes>) {
    let count = Arc::new(CountWakes(std::sync::atomic::AtomicUsize::new(0)));
    (std::task::Waker::from(count.clone()), count)
}

#[test]
fn repolled_sleep_keeps_only_the_latest_waker() {
    use std::{future::Future, task::Context};
    let (w1, c1) = counting_waker();
    let (w2, c2) = counting_waker();
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let root = s.spawn_root("root", async move {
        // A cancelled registration: polled once, then dropped.
        {
            let mut early = std::pin::pin!(time::sleep(secs(5)));
            assert!(early
                .as_mut()
                .poll(&mut Context::from_waker(&w1))
                .is_pending());
        }
        // A re-polled one: the second poll's waker replaces the first's.
        let mut sleep = std::pin::pin!(time::sleep(secs(10)));
        assert!(sleep
            .as_mut()
            .poll(&mut Context::from_waker(&w1))
            .is_pending());
        assert!(sleep
            .as_mut()
            .poll(&mut Context::from_waker(&w2))
            .is_pending());
        time::sleep(secs(20)).await;
    });
    run_until_waiting(&mut s, root.task());
    assert_eq!(s.next_deadline(), Some(SimInstant::from_start(secs(10))));
    assert_eq!(
        s.advance_to(SimInstant::from_start(secs(10))).len(),
        1,
        "one registration for the re-polled sleep"
    );
    assert_eq!(
        (c1.0.load(Ordering::SeqCst), c2.0.load(Ordering::SeqCst)),
        (0, 1)
    );
    s.advance_to(SimInstant::from_start(secs(3_600)));
    assert_eq!(
        c1.0.load(Ordering::SeqCst),
        0,
        "the cancelled one never fires"
    );
}

#[test]
fn task_woken_at_the_first_deadline_cancels_a_later_timer() {
    for seed in 0..100 {
        let (result, violation) = run_fixture(2, seed, |s| {
            let notify = Arc::new(Notify::new());
            let fired = shared(Cell::new(false));
            let (n, f) = (notify.clone(), fired.clone());
            s.spawn_root("waiter", async move {
                zakura_rt::select! {
                    biased;
                    () = n.notified() => {}
                    () = time::sleep(secs(20)) => f.set(true),
                }
            });
            s.spawn_root("canceller", async move {
                time::sleep(secs(10)).await;
                notify.notify_one();
            });
            Box::new(move |r| {
                (fired.get() || r.status != Status::Quiescent || r.report.timers_left != 0)
                    .then(|| format!("fired {}, {:?}", fired.get(), r.status))
            })
        });
        assert_eq!(violation, None, "seed {seed}");
        assert_eq!(advances(&result), 1, "seed {seed}: only the 10 s deadline");
    }
}

/// Every order of three items.
const ORDERS: [[usize; 3]; 6] = [
    [0, 1, 2],
    [0, 2, 1],
    [1, 0, 2],
    [1, 2, 0],
    [2, 0, 1],
    [2, 1, 0],
];

#[test]
fn mutex_release_lets_every_waiter_retry_and_one_wins() {
    use zakura_scheduler::RunOutcome::{Parked, Reached};
    for order in ORDERS {
        let config = SchedulerConfig {
            workers: 4,
            ..SchedulerConfig::default()
        };
        let mut s = Scheduler::new(config, SimInstant::ZERO);
        let lock = Arc::new(RtMutex::new(()));
        let winners = shared(RefCell::new(Vec::new()));
        let l = lock.clone();
        let holder = s.spawn_root("holder", async move {
            let guard = l.lock().unwrap();
            point();
            drop(guard);
        });
        let waiters: Vec<_> = (0..3)
            .map(|i| {
                let (l, w) = (lock.clone(), winners.clone());
                s.spawn_root("waiter", async move {
                    let guard = l.lock().unwrap();
                    w.borrow_mut().push(i);
                    point();
                    drop(guard);
                })
            })
            .collect();
        assert!(matches!(s.run(holder.task()), Reached(_))); // at lock
        assert!(matches!(s.run(holder.task()), Reached(_))); // holding it
        for w in &waiters {
            assert!(matches!(s.run(w.task()), Reached(_)));
            assert!(matches!(s.run(w.task()), Parked(_)));
        }
        assert!(matches!(s.run(holder.task()), Reached(_))); // released
        let ready = s.ready();
        for w in &waiters {
            assert!(
                ready.iter().any(|t| t.task == w.task() && t.mid_poll),
                "every waiter retries"
            );
        }
        let [first, second, third] = order.map(|i| waiters[i].task());
        assert!(matches!(s.run(first), Reached(_)), "{order:?}");
        assert!(matches!(s.run(second), Parked(_)), "{order:?}");
        assert!(matches!(s.run(third), Parked(_)), "{order:?}");
        assert_eq!(*winners.borrow(), [order[0]]);
        let status = drive(&mut s, 0, &mut Vec::new(), &mut Vec::new());
        assert_eq!(status, Status::Quiescent);
        assert_eq!(winners.borrow().len(), 3);
    }
}

#[test]
fn semaphore_grants_permits_in_fifo_order_and_skips_cancelled_waiters() {
    for cancel_b in [false, true] {
        for seed in 0..50 {
            let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
            let sem = Arc::new(Semaphore::new(0));
            let order = shared(RefCell::new(Vec::new()));
            let waiters: Vec<_> = ['a', 'b', 'c']
                .into_iter()
                .map(|name| {
                    let (sem, o) = (sem.clone(), order.clone());
                    s.spawn_root("waiter", async move {
                        let _permit = sem.acquire().await.unwrap();
                        o.borrow_mut().push(name);
                        point();
                    })
                })
                .collect();
            // Queue them in name order.
            for w in &waiters {
                run_until_waiting(&mut s, w.task());
            }
            if cancel_b {
                waiters[1].abort();
            }
            let release = sem.clone();
            s.spawn_root("releaser", async move { release.add_permits(1) });
            let status = drive(&mut s, seed, &mut Vec::new(), &mut Vec::new());
            assert_eq!(status, Status::Quiescent, "seed {seed}");
            let want: &[char] = if cancel_b {
                &['a', 'c']
            } else {
                &['a', 'b', 'c']
            };
            assert_eq!(*order.borrow(), want, "seed {seed}");
            assert_eq!(sem.available_permits(), 1, "seed {seed}: no permit lost");
            s.shutdown();
        }
    }
}

#[test]
fn bounded_channel_admits_senders_in_fifo_order_and_skips_cancelled_ones() {
    for cancel_second in [false, true] {
        for seed in 0..50 {
            let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
            let (tx, mut rx) = zakura_rt::sync::mpsc::channel::<u32>(1);
            tx.try_send(0).expect("the channel starts empty");
            let senders: Vec<_> = (1..=3)
                .map(|v| {
                    let tx = tx.clone();
                    s.spawn_root("sender", async move {
                        let _ = tx.send(v).await;
                    })
                })
                .collect();
            drop(tx);
            for sender in &senders {
                run_until_waiting(&mut s, sender.task());
            }
            if cancel_second {
                senders[1].abort();
            }
            let got = shared(RefCell::new(Vec::new()));
            let g = got.clone();
            s.spawn_root("receiver", async move {
                while let Some(v) = rx.recv().await {
                    g.borrow_mut().push(v);
                }
            });
            let status = drive(&mut s, seed, &mut Vec::new(), &mut Vec::new());
            assert_eq!(status, Status::Quiescent, "seed {seed}");
            let want: &[u32] = if cancel_second {
                &[0, 1, 3]
            } else {
                &[0, 1, 2, 3]
            };
            assert_eq!(*got.borrow(), want, "seed {seed}");
            s.shutdown();
        }
    }
}

/// Sets its flag when dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn timeout_cancels_its_timer_or_drops_its_future() {
    for seed in 0..50 {
        // The value arrives at 5 s, inside the 10 s timeout. Its timer must go,
        // or time would visit 10 s on the way to 25 s.
        let (result, violation) = run_fixture(2, seed, |s| {
            let (tx, rx) = zakura_rt::sync::oneshot::channel::<u32>();
            s.spawn_root("sender", async move {
                time::sleep(secs(5)).await;
                let _ = tx.send(5);
            });
            let seen = shared(Cell::new(None));
            let out = seen.clone();
            s.spawn_root("root", async move {
                let start = Instant::now();
                let value = time::timeout(secs(10), rx).await.ok().and_then(Result::ok);
                time::sleep(secs(20)).await;
                out.set(Some((value, start.elapsed())));
            });
            Box::new(move |_| {
                (seen.get() != Some((Some(5), secs(25)))).then(|| format!("{:?}", seen.get()))
            })
        });
        assert_eq!(violation, None, "seed {seed}");
        assert_eq!(advances(&result), 2, "seed {seed}: 5 s and 25 s only");

        // The inner future never finishes, so the timeout drops it at 10 s.
        let (_, violation) = run_fixture(2, seed, |s| {
            let dropped = Arc::new(AtomicBool::new(false));
            let seen = shared(Cell::new(None));
            let (d, out) = (dropped.clone(), seen.clone());
            s.spawn_root("root", async move {
                let start = Instant::now();
                let guard = DropFlag(d.clone());
                let inner = async move {
                    let _guard = guard;
                    pending::<()>().await
                };
                let elapsed = time::timeout(secs(10), inner).await.is_err();
                out.set(Some((elapsed, d.load(Ordering::SeqCst), start.elapsed())));
            });
            Box::new(move |_| {
                (seen.get() != Some((true, true, secs(10)))).then(|| format!("{:?}", seen.get()))
            })
        });
        assert_eq!(violation, None, "seed {seed}");
    }
}

#[test]
fn biased_select_keeps_source_order_whatever_the_draws() {
    let mut unbiased_firsts = std::collections::BTreeSet::new();
    for draw in 0..8 {
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        s.set_draws(move |_| draw);
        let seen = shared(Cell::new(None));
        let out = seen.clone();
        let root = s.spawn_root("root", async move {
            let biased = zakura_rt::select! {
                biased;
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
            };
            let unbiased = zakura_rt::select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
            };
            out.set(Some((biased, unbiased)));
        });
        while s.status() == Status::Running {
            s.run(root.task());
        }
        let (biased, unbiased) = seen.get().expect("the task finished");
        assert_eq!(biased, 1, "draw {draw}");
        unbiased_firsts.insert(unbiased);
    }
    assert_eq!(unbiased_firsts.len(), 2, "draws move the unbiased start");
}

/// Ticks of a 10 ms interval with work of these lengths after each tick, as
/// (when the tick was due, when it was seen), in ms from the start. Long work
/// misses ticks. The 3 ms work stays inside tokio's 5 ms tolerance.
const WORK_MS: [u64; 9] = [0, 25, 0, 3, 0, 12, 41, 7, 0];

/// [`WORK_MS`] on the sim `interval`, run on the scheduler.
fn sim_ticks(behavior: time::MissedTickBehavior) -> Vec<(u128, u128)> {
    let ticks = shared(RefCell::new(Vec::new()));
    let out = ticks.clone();
    let (_, violation) = run_fixture(2, 0, move |s| {
        s.spawn_root("ticker", async move {
            let start = Instant::now();
            let mut interval = time::interval(Duration::from_millis(10));
            interval.set_missed_tick_behavior(behavior);
            for work in WORK_MS {
                let due = interval.tick().await;
                let seen = start.elapsed().as_millis();
                out.borrow_mut().push(((due - start).as_millis(), seen));
                time::sleep(Duration::from_millis(work)).await;
            }
        });
        Box::new(|r| (r.status != Status::Quiescent).then(|| format!("{:?}", r.status)))
    });
    assert_eq!(violation, None);
    ticks.take()
}

/// [`WORK_MS`] on tokio's own `interval`, with tokio's clock paused.
fn tokio_ticks(behavior: tokio::time::MissedTickBehavior) -> Vec<(u128, u128)> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a current-thread runtime builds");
    runtime.block_on(async {
        let start = tokio::time::Instant::now();
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        interval.set_missed_tick_behavior(behavior);
        let mut ticks = Vec::new();
        for work in WORK_MS {
            let due = interval.tick().await;
            ticks.push(((due - start).as_millis(), start.elapsed().as_millis()));
            tokio::time::sleep(Duration::from_millis(work)).await;
        }
        ticks
    })
}

#[test]
fn interval_ticks_match_tokio_for_every_missed_tick_behavior() {
    use tokio::time::MissedTickBehavior::{Burst, Delay, Skip};
    let mut seen = Vec::new();
    for behavior in [Burst, Delay, Skip] {
        let sim = sim_ticks(behavior);
        assert_eq!(sim, tokio_ticks(behavior), "{behavior:?}");
        assert_eq!(sim[0], (0, 0), "{behavior:?}: the first tick is immediate");
        seen.push(sim);
    }
    // The work misses ticks, so each behavior takes a different path.
    assert!(seen[0] != seen[1] && seen[1] != seen[2] && seen[0] != seen[2]);
}

#[test]
fn rt_join_returns_the_output_when_completion_races_abort() {
    use zakura_scheduler::RunOutcome::{Completed, Reached};
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let target = task::spawn(async {
        point();
        5u32
    });
    let id = s.ready()[0].task;
    while !matches!(s.run(id), Reached(at) if at.op == zakura_scheduler::Op::Yield) {}
    target.abort();
    let joined = shared(RefCell::new(None));
    let out = joined.clone();
    s.spawn_root("joiner", async move {
        let joined = target.await;
        *out.borrow_mut() = Some(joined);
    });
    assert_eq!(s.run(id), Completed);
    let status = drive(&mut s, 0, &mut Vec::new(), &mut Vec::new());
    assert_eq!(status, Status::Quiescent);
    assert!(
        matches!(*joined.borrow(), Some(Ok(5))),
        "{:?}",
        joined.borrow()
    );
}

/// Counts drops of a task-owned resource.
struct DropCount(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn join_set_abort_all_shutdown_and_drop_release_every_task() {
    use std::sync::atomic::AtomicUsize;
    for mode in ["abort_all", "shutdown", "drop"] {
        for seed in 0..30 {
            let drops = Arc::new(AtomicUsize::new(0));
            let polls = Arc::new(AtomicUsize::new(0));
            let seen = shared(Cell::new(None));
            let (d, p, out) = (drops.clone(), polls.clone(), seen.clone());
            let (result, violation) = run_fixture(2, seed, move |s| {
                s.spawn_root("root", async move {
                    let mut set = JoinSet::new();
                    for _ in 0..3 {
                        let (owned, p) = (DropCount(d.clone()), p.clone());
                        set.spawn(async move {
                            let _owned = owned;
                            std::future::poll_fn(|_| {
                                p.fetch_add(1, Ordering::SeqCst);
                                std::task::Poll::<()>::Pending
                            })
                            .await
                        });
                    }
                    task::yield_now().await;
                    // Each count is read right after the last abort request,
                    // with no point between.
                    let (polls_at_abort, released) = match mode {
                        "abort_all" => {
                            set.abort_all();
                            let polls_at_abort = p.load(Ordering::SeqCst);
                            let mut cancelled = 0;
                            while let Some(result) = set.join_next().await {
                                cancelled += usize::from(result.unwrap_err().is_cancelled());
                            }
                            assert_eq!(cancelled, 3);
                            (polls_at_abort, Some(d.load(Ordering::SeqCst)))
                        }
                        "shutdown" => {
                            set.shutdown().await;
                            (p.load(Ordering::SeqCst), Some(d.load(Ordering::SeqCst)))
                        }
                        _ => {
                            drop(set);
                            (p.load(Ordering::SeqCst), None)
                        }
                    };
                    out.set(Some((polls_at_abort, released)));
                });
                Box::new(|_| None)
            });
            assert_eq!(violation, None);
            let (polls_at_abort, released) = seen.get().expect("the root finished");
            if mode != "drop" {
                assert_eq!(
                    released,
                    Some(3),
                    "{mode}, seed {seed}: released when joined"
                );
            }
            assert_eq!(result.status, Status::Quiescent, "{mode}, seed {seed}");
            assert_eq!(drops.load(Ordering::SeqCst), 3, "{mode}, seed {seed}");
            assert_eq!(
                polls.load(Ordering::SeqCst),
                polls_at_abort,
                "{mode}, seed {seed}: no poll after the abort"
            );
            assert_eq!(result.report.cancelled, 0, "{mode}, seed {seed}");
        }
    }
}

#[test]
fn dropping_a_plain_handle_detaches_but_abort_on_drop_aborts() {
    for seed in 0..30 {
        let done = [(); 3].map(|()| Arc::new(AtomicBool::new(false)));
        let flags = done.clone();
        let (result, violation) = run_fixture(2, seed, move |s| {
            s.spawn_root("root", async move {
                let [plain, aborting, detached] = flags;
                let finish = |flag: Arc<AtomicBool>| async move {
                    time::sleep(secs(1)).await;
                    flag.store(true, Ordering::SeqCst);
                };
                drop(task::spawn(finish(plain)));
                drop(task::AbortOnDropHandle::new(task::spawn(finish(aborting))));
                drop(task::AbortOnDropHandle::new(task::spawn(finish(detached))).detach());
            });
            Box::new(|_| None)
        });
        assert_eq!(violation, None);
        assert_eq!(result.status, Status::Quiescent, "seed {seed}");
        let finished = done.map(|flag| flag.load(Ordering::SeqCst));
        assert_eq!(finished, [true, false, true], "seed {seed}");
    }
}

#[test]
fn consecutive_runs_reset_rt_per_run_state() {
    let suppressing_run = || {
        let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let (_tx, rx) = zakura_rt::sync::watch::channel(0u64);
        let counter = Arc::new(RtAtomicU64::new(0));
        let root = s.spawn_root("root", async move {
            let value = rx.borrow();
            counter.store(*value, RtOrdering::SeqCst);
        });
        while s.status() == Status::Running {
            s.run(root.task());
        }
        s.shutdown();
    };
    suppressing_run();
    assert_eq!(sim::suppressed_points(), 1, "the last run's count");
    let s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    assert_eq!(sim::suppressed_points(), 0, "a new run starts from zero");
    drop(s);
}

/// `SchedulerConfig::utc_origin` sets the run's UTC at virtual time zero,
/// and UTC then moves with virtual time. Unset, the run uses `zakura-rt`'s
/// default origin.
#[test]
fn rt_utc_now_starts_at_the_configured_origin() {
    let read = |utc_origin, start| {
        let config = SchedulerConfig {
            utc_origin,
            ..SchedulerConfig::default()
        };
        let mut s = Scheduler::new(config, start);
        let seen = shared(Cell::new(None));
        let out = seen.clone();
        let root = s.spawn_root("root", async move {
            let first = time::utc_now();
            time::sleep(Duration::from_secs(5)).await;
            out.set(Some((first, time::utc_now())));
        });
        while !root.is_finished() {
            if s.ready().is_empty() {
                let next = s.next_deadline().expect("the root task waits on its timer");
                s.advance_to(next);
            } else {
                s.run(root.task());
            }
        }
        seen.get().expect("the root task finished")
    };
    let origin = std::time::UNIX_EPOCH + Duration::from_secs(1_000_000);
    let five = Duration::from_secs(5);
    assert_eq!(
        read(Some(origin), SimInstant::ZERO),
        (origin, origin + five)
    );
    let start = SimInstant::from_start(Duration::from_secs(60));
    assert_eq!(
        read(Some(origin), start),
        (
            origin + Duration::from_secs(60),
            origin + Duration::from_secs(65)
        )
    );
    let default = std::time::UNIX_EPOCH + sim::DEFAULT_UTC_ORIGIN_SINCE_EPOCH;
    assert_eq!(read(None, SimInstant::ZERO), (default, default + five));
}

/// The latest whole second `SystemTime` can hold on this platform.
fn latest_system_time() -> std::time::SystemTime {
    let (mut low, mut high) = (0u64, u64::MAX);
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        match std::time::UNIX_EPOCH.checked_add(Duration::from_secs(mid)) {
            Some(_) => low = mid,
            None => high = mid - 1,
        }
    }
    std::time::UNIX_EPOCH + Duration::from_secs(low)
}

/// Runs `f` and returns its panic message, or `None` if it didn't panic.
fn panic_message(f: impl FnOnce()) -> Option<String> {
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).err()?;
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
}

/// Moving virtual time past what `SystemTime` can hold above the UTC origin
/// panics in the harness, at `advance_to`, rather than in the task that
/// next reads `utc_now`. Up to that point the run is fine.
#[test]
fn rt_advancing_past_the_utc_range_panics_in_the_harness() {
    let config = SchedulerConfig {
        utc_origin: Some(latest_system_time() - Duration::from_secs(1)),
        ..SchedulerConfig::default()
    };
    let mut s = Scheduler::new(config, SimInstant::ZERO);
    let read = shared(Cell::new(false));
    let out = read.clone();
    let root = s.spawn_root("root", async move {
        time::sleep(Duration::from_secs(2)).await;
        time::utc_now();
        out.set(true);
    });
    s.run(root.task());
    s.advance_to(SimInstant::from_start(Duration::from_secs(1)));
    let message = panic_message(|| {
        s.advance_to(SimInstant::from_start(Duration::from_secs(2)));
    });
    assert!(
        message.as_deref().is_some_and(|m| m.contains("utc_origin")),
        "{message:?}"
    );
    assert!(
        !read.get(),
        "the task never read an unrepresentable UTC time"
    );
}

/// A start instant past what `SystemTime` can hold above the UTC origin
/// panics in `Scheduler::new`.
#[test]
fn rt_a_start_past_the_utc_range_panics_at_construction() {
    let config = SchedulerConfig {
        utc_origin: Some(latest_system_time()),
        ..SchedulerConfig::default()
    };
    let message = panic_message(|| {
        drop(Scheduler::new(
            config,
            SimInstant::from_start(Duration::from_secs(2)),
        ));
    });
    assert!(
        message.as_deref().is_some_and(|m| m.contains("utc_origin")),
        "{message:?}"
    );
}
