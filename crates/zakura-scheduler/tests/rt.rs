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

/// Two tasks read a counter, yield through `zakura-rt`, then write it back.
fn yield_between_load_and_store(s: &mut Scheduler) -> Check {
    let counter = shared(Cell::new(0));
    for label in ["inc_a", "inc_b"] {
        let counter = counter.clone();
        s.spawn_root(label, async move {
            let v = counter.get();
            task::yield_now().await;
            counter.set(v + 1);
        });
    }
    Box::new(move |_| (counter.get() != 2).then(|| format!("counter = {}", counter.get())))
}

#[test]
fn rt_points_preempt_inside_a_poll() {
    assert!(first_violation(2, FIND, yield_between_load_and_store).is_some());
    // The yield is mid-poll, so with one worker the other task can't run there.
    assert_eq!(first_violation(1, FIND, yield_between_load_and_store), None);
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
fn same_seed_replays_rt_programs() {
    let picks = |seed| run_fixture(2, seed, yield_between_load_and_store).0.picks;
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
    assert_eq!(value, 7);
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
