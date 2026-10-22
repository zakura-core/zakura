//! `zakura-rt`'s sim primitives running on the scheduler. These only build
//! with `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::{Cell, RefCell},
    future::pending,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use support::*;
use zakura_rt::{
    sim,
    task::{self, JoinSet},
    time::{self, Instant},
};
use zakura_scheduler::{Scheduler, SchedulerConfig, SimInstant, Status};

/// Seeds to try when a planted race must be found.
const FIND: u64 = 2_000;

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
