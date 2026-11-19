//! Generated programs that drive the real scheduler and check it after every
//! step: task readiness and worker ownership (S1.1, S1.2), and timers (S2.1).
//!
//! Each generated task records where it handed control back and which wakes
//! it fired. The readiness check compares the scheduler's ready set and each
//! run's outcome with those records. It never steps a second copy of the
//! scheduler. The timer check keeps the pending timers in a map ordered by
//! deadline, then registration, and compares each advance's fired timers with
//! the map's due prefix.

#[allow(unused_imports)]
mod support;

use std::{
    cell::RefCell,
    future::poll_fn,
    rc::Rc,
    sync::{Arc, Mutex as StdMutex},
    task::{Poll, Wake, Waker},
    time::Duration,
};

use support::Walk;
use zakura_scheduler::{
    hooks, ObjectId, Op, Point, RunOutcome, Scheduler, SchedulerConfig, SimInstant, Status, TaskId,
    TimerId,
};

/// One step of a generated task.
#[derive(Clone, Copy, Debug)]
enum Act {
    /// A preemption point.
    Point,
    /// Wakes another task (or itself) through the waker it last polled with.
    Wake(usize),
    /// Returns `Pending`.
    Pending,
    /// Takes a lock: a point, then park until it's free.
    Lock(usize),
    /// Releases a lock this task holds.
    Unlock(usize),
    /// Spawns a child running the given script from [`Program::children`].
    Spawn(usize),
}

/// A generated program: root task scripts, and the scripts their spawns run.
struct Program {
    roots: Vec<Vec<Act>>,
    children: Vec<Vec<Act>>,
}

/// State shared by every generated task. Never borrowed across a hook that
/// can suspend.
struct World {
    wakers: Vec<Option<Waker>>,
    holders: Vec<Option<TaskId>>,
    locks: Vec<ObjectId>,
    /// The scripts [`Act::Spawn`] runs.
    children: Vec<Vec<Act>>,
    /// The index of each task whose code ran, in order.
    log: Vec<usize>,
    /// Per task: where its code last handed control back, recorded just
    /// before it did. `None` until its first poll.
    stops: Vec<Option<Stop>>,
    /// Per task: a wake fired through its waker since its current or last
    /// poll started.
    woken: Vec<bool>,
    /// Per task: the lock it is parked on, until the park returns.
    parked_on: Vec<Option<usize>>,
    /// Per task: whether the lock it parked on was released since it parked.
    unparked: Vec<bool>,
}

/// Where a task's code handed control back to the scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    /// At a preemption point.
    Point,
    /// By returning `Pending`.
    Pending,
    /// By parking on a held lock.
    Park,
    /// By returning `Ready`.
    Done,
}

impl World {
    /// Records that task `me` stops at `stop`.
    fn stop(&mut self, me: usize, stop: Stop) {
        self.stops[me] = Some(stop);
    }

    /// A task that needs no worker to resume: stopped at a point, or
    /// parked on a lock that was released since.
    fn resumable(&self, task: usize) -> bool {
        match self.stops[task] {
            Some(Stop::Point) => true,
            Some(Stop::Park) => self.unparked[task],
            _ => false,
        }
    }

    /// A task that may start a poll: never polled, or woken since its last
    /// poll returned `Pending`.
    fn pollable(&self, task: usize) -> bool {
        match self.stops[task] {
            None => true,
            Some(Stop::Pending) => self.woken[task],
            _ => false,
        }
    }
}

/// A generated task's future. It records itself in the log each time its
/// code runs, which is at a poll start and after each suspension.
fn task_future(
    me: usize,
    acts: Vec<Act>,
    world: Rc<RefCell<World>>,
) -> impl std::future::Future<Output = ()> {
    let mut pc = 0;
    poll_fn(move |cx| {
        {
            let mut w = world.borrow_mut();
            w.log.push(me);
            w.wakers[me] = Some(cx.waker().clone());
            // A poll consumes every wake that came before it.
            w.woken[me] = false;
        }
        while let Some(&act) = acts.get(pc) {
            pc += 1;
            match act {
                Act::Point => {
                    world.borrow_mut().stop(me, Stop::Point);
                    hooks::preempt(Point::here(Op::Yield));
                    world.borrow_mut().log.push(me);
                }
                Act::Wake(j) => {
                    let waker = world.borrow().wakers[j].clone();
                    if let Some(waker) = waker {
                        world.borrow_mut().woken[j] = true;
                        waker.wake();
                    }
                }
                Act::Pending => {
                    world.borrow_mut().stop(me, Stop::Pending);
                    return Poll::Pending;
                }
                Act::Lock(m) => {
                    world.borrow_mut().stop(me, Stop::Point);
                    hooks::preempt(Point::here(Op::Lock));
                    world.borrow_mut().log.push(me);
                    loop {
                        let (holder, obj) = {
                            let w = world.borrow();
                            (w.holders[m], w.locks[m])
                        };
                        match holder {
                            None => break,
                            Some(holder) => {
                                {
                                    let mut w = world.borrow_mut();
                                    w.stop(me, Stop::Park);
                                    w.parked_on[me] = Some(m);
                                    w.unparked[me] = false;
                                }
                                assert!(hooks::park(obj, Some(holder)));
                                let mut w = world.borrow_mut();
                                w.log.push(me);
                                w.parked_on[me] = None;
                            }
                        }
                    }
                    world.borrow_mut().holders[m] = hooks::current_task();
                }
                Act::Unlock(m) => {
                    let obj = {
                        let mut w = world.borrow_mut();
                        w.holders[m] = None;
                        for t in 0..w.parked_on.len() {
                            if w.parked_on[t] == Some(m) {
                                w.unparked[t] = true;
                            }
                        }
                        w.locks[m]
                    };
                    hooks::unpark_all(obj);
                }
                Act::Spawn(c) => {
                    let (child, acts) = {
                        let mut w = world.borrow_mut();
                        w.wakers.push(None);
                        w.stops.push(None);
                        w.woken.push(false);
                        w.parked_on.push(None);
                        w.unparked.push(false);
                        (w.wakers.len() - 1, w.children[c].clone())
                    };
                    let spawned = hooks::spawn("child", task_future(child, acts, world.clone()));
                    // Task ids follow spawn order, so the test's indexes match.
                    assert_eq!(spawned.task().index(), child);
                }
            }
        }
        world.borrow_mut().stop(me, Stop::Done);
        Poll::Ready(())
    })
}

/// A random program: up to four root tasks of up to eight steps over two
/// locks. Roots may spawn children, which run scripts of the same shape but
/// spawn nothing. A task never takes a lock it holds, and releases what it
/// holds at the end. Wakes target root tasks.
fn program(walk: &mut Walk) -> Program {
    let roots = 1 + walk.pick(4);
    let mut children = Vec::new();
    let script = |walk: &mut Walk, children: &mut Vec<Vec<Act>>, can_spawn: bool| {
        let mut held = [false; 2];
        let mut acts = Vec::new();
        for _ in 0..=walk.pick(8) {
            let m = walk.pick(2);
            acts.push(match walk.pick(6) {
                0 => Act::Point,
                1 => Act::Wake(walk.pick(roots)),
                2 => Act::Pending,
                3 if can_spawn => {
                    children.push(Vec::new());
                    Act::Spawn(children.len() - 1)
                }
                _ if held[m] => {
                    held[m] = false;
                    Act::Unlock(m)
                }
                _ => {
                    held[m] = true;
                    Act::Lock(m)
                }
            });
        }
        acts.extend((0..2).filter(|m| held[*m]).map(Act::Unlock));
        acts
    };
    let roots: Vec<Vec<Act>> = (0..roots)
        .map(|_| script(walk, &mut children, true))
        .collect();
    for child in &mut children {
        *child = script(walk, &mut Vec::new(), false);
    }
    Program { roots, children }
}

/// Runs one generated program with `workers`, picking a random ready task at
/// each step, and checks the scheduler against what the tasks recorded.
fn check_readiness(workers: u16, seed: u64) {
    let mut walk = Walk(seed);
    let Program { roots, children } = program(&mut walk);
    let config = SchedulerConfig {
        workers,
        stack_size: 64 * 1024,
        ..SchedulerConfig::default()
    };
    let mut s = Scheduler::new(config, SimInstant::ZERO);
    let world = Rc::new(RefCell::new(World {
        wakers: vec![None; roots.len()],
        holders: vec![None; 2],
        locks: (0..2)
            .map(|_| hooks::new_object().expect("inside a run"))
            .collect(),
        children,
        log: Vec::new(),
        stops: vec![None; roots.len()],
        woken: vec![false; roots.len()],
        parked_on: vec![None; roots.len()],
        unparked: vec![false; roots.len()],
    }));
    for (i, acts) in roots.into_iter().enumerate() {
        let root = s.spawn_root("t", task_future(i, acts, world.clone()));
        assert_eq!(root.task().index(), i);
    }
    let workers = usize::from(workers);
    let ctx = |step: usize| format!("seed {seed}, workers {workers}, step {step}");
    for step in 0.. {
        let ready = s.ready();
        let held = s.mid_poll().len();
        {
            let w = world.borrow();
            assert!(held <= workers, "{held} tasks hold a worker, {}", ctx(step));
            // S1.1: a task is ready exactly when it can make progress. A
            // task stopped mid-poll keeps its worker and is always ready. A
            // task at a poll boundary needs a free worker.
            for task in 0..w.stops.len() {
                let entry = ready.iter().find(|r| r.task.index() == task);
                let want = if w.resumable(task) {
                    Some(true)
                } else if w.pollable(task) && held < workers {
                    Some(false)
                } else {
                    None
                };
                assert_eq!(
                    entry.map(|r| r.mid_poll),
                    want,
                    "task {task} stopped at {:?}, woken {}, {}",
                    w.stops[task],
                    w.woken[task],
                    ctx(step)
                );
            }
        }
        if ready.is_empty() {
            let status = s.status();
            let finished = world
                .borrow()
                .stops
                .iter()
                .all(|stop| *stop == Some(Stop::Done));
            if finished {
                assert_eq!(status, Status::Quiescent, "{}", ctx(step));
            } else {
                assert!(
                    matches!(status, Status::Deadlock | Status::WorkerStarved),
                    "{status:?}, {}",
                    ctx(step)
                );
            }
            break;
        }
        let pick = ready[walk.pick(ready.len())].task;
        let me = pick.index();
        let before = world.borrow().log.len();
        let outcome = s.run(pick);
        let w = world.borrow();
        let stop = match outcome {
            RunOutcome::Reached(_) => Stop::Point,
            RunOutcome::Waiting => Stop::Pending,
            RunOutcome::Parked(_) => Stop::Park,
            RunOutcome::Completed => Stop::Done,
            other => panic!("{other:?}, {}", ctx(step)),
        };
        assert_eq!(Some(stop), w.stops[me], "outcome, {}", ctx(step));
        let ran = &w.log[before..];
        assert!(
            !ran.is_empty() && ran.iter().all(|t| *t == me),
            "only task {me} may run, but {ran:?} did, {}",
            ctx(step)
        );
    }
    s.shutdown();
}

#[test]
fn generated_programs_keep_the_readiness_invariants() {
    for workers in [1, 2, 3] {
        for seed in 0..1_000 {
            check_readiness(workers, seed);
        }
    }
}

/// A waker that records the virtual time it fired at.
struct RecordFire {
    index: usize,
    fired: Arc<StdMutex<Vec<(usize, SimInstant)>>>,
}

impl Wake for RecordFire {
    fn wake(self: Arc<Self>) {
        let now = hooks::now().expect("timers fire inside a run");
        self.fired.lock().unwrap().push((self.index, now));
    }
}

/// Runs a random sequence of timer registrations, cancellations, and time
/// advances, and checks each advance against the pending timers the test
/// registered and did not cancel.
fn check_timers(seed: u64) {
    let mut walk = Walk(seed);
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let fired = Arc::new(StdMutex::new(Vec::new()));
    // Pending timers by (deadline, registration order), as the scheduler
    // orders them.
    let mut pending: std::collections::BTreeMap<(SimInstant, usize), TimerId> = Default::default();
    let mut cancelled = Vec::new();
    let mut registered = 0;
    let ms = Duration::from_millis;
    for _ in 0..60 {
        let now = s.now();
        match walk.pick(4) {
            // Zero delay, repeated equal deadlines, and later deadlines.
            0 | 1 => {
                let deadline = now.checked_add(ms(walk.pick(4) as u64)).unwrap();
                let waker = Waker::from(Arc::new(RecordFire {
                    index: registered,
                    fired: fired.clone(),
                }));
                let id = hooks::register_timer(deadline, &waker);
                assert_eq!(id.deadline(), deadline);
                pending.insert((deadline, registered), id);
                registered += 1;
            }
            2 if !pending.is_empty() => {
                let key = *pending.keys().nth(walk.pick(pending.len())).unwrap();
                hooks::cancel_timer(pending.remove(&key).unwrap());
                cancelled.push(key.1);
            }
            _ => {
                let to = match (walk.pick(2), s.next_deadline()) {
                    (0, Some(next)) => next,
                    _ => now.checked_add(ms(walk.pick(3) as u64)).unwrap(),
                };
                let later = pending.split_off(&(to, usize::MAX));
                let due = std::mem::replace(&mut pending, later);
                fired.lock().unwrap().clear();
                let ids = s.advance_to(to);
                assert_eq!(s.now(), to, "seed {seed}");
                assert_eq!(
                    ids,
                    due.values().copied().collect::<Vec<_>>(),
                    "seed {seed}"
                );
                let want: Vec<(usize, SimInstant)> = due.keys().map(|(_, i)| (*i, to)).collect();
                assert_eq!(*fired.lock().unwrap(), want, "seed {seed}");
            }
        }
        assert_eq!(
            s.next_deadline(),
            pending.keys().next().map(|(d, _)| *d),
            "seed {seed}"
        );
        assert!(s.now() >= now, "time moved backwards, seed {seed}");
    }
    // Cancelled timers never fire, even long after their deadlines.
    fired.lock().unwrap().clear();
    s.advance_to(SimInstant::from_start(Duration::from_secs(3_600)));
    let late = fired.lock().unwrap().clone();
    assert!(
        late.iter().all(|(i, _)| !cancelled.contains(i)),
        "seed {seed}"
    );
    assert_eq!(late.len(), pending.len(), "seed {seed}");
}

#[test]
fn generated_timer_sequences_fire_in_deadline_order() {
    for seed in 0..2_000 {
        check_timers(seed);
    }
}

#[test]
fn advancing_into_the_past_panics() {
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    s.advance_to(SimInstant::from_start(Duration::from_secs(2)));
    let back = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        s.advance_to(SimInstant::from_start(Duration::from_secs(1)));
    }));
    assert!(back.is_err());
    assert_eq!(s.now(), SimInstant::from_start(Duration::from_secs(2)));
}
