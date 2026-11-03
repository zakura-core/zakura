//! Generated programs checked against small independent models of the
//! scheduler: task readiness and worker ownership (S1.1, S1.2), and timers
//! (S2.1).

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
        }
        while let Some(&act) = acts.get(pc) {
            pc += 1;
            match act {
                Act::Point => {
                    hooks::preempt(Point::here(Op::Yield));
                    world.borrow_mut().log.push(me);
                }
                Act::Wake(j) => {
                    let waker = world.borrow().wakers[j].clone();
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
                Act::Pending => return Poll::Pending,
                Act::Lock(m) => {
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
                                assert!(hooks::park(obj, Some(holder)));
                                world.borrow_mut().log.push(me);
                            }
                        }
                    }
                    world.borrow_mut().holders[m] = hooks::current_task();
                }
                Act::Unlock(m) => {
                    let obj = {
                        let mut w = world.borrow_mut();
                        w.holders[m] = None;
                        w.locks[m]
                    };
                    hooks::unpark_all(obj);
                }
                Act::Spawn(c) => {
                    let (child, acts) = {
                        let mut w = world.borrow_mut();
                        w.wakers.push(None);
                        (w.wakers.len() - 1, w.children[c].clone())
                    };
                    let spawned = hooks::spawn("child", task_future(child, acts, world.clone()));
                    // Task ids follow spawn order, so they match the model's.
                    assert_eq!(spawned.task().index(), child);
                }
            }
        }
        Poll::Ready(())
    })
}

/// Where the model has a task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Ready at a poll boundary. Starting it needs a free worker.
    Boundary,
    /// Ready mid-poll. It holds a worker.
    MidPoll,
    /// Parked on a lock mid-poll. It holds a worker.
    Parked(usize),
    /// Returned `Pending` and waits for a wake.
    Waiting,
    /// Being run right now.
    Running,
    Done,
}

/// The independent model: what each task does next, from its script alone.
struct ModelTask {
    acts: Vec<Act>,
    pc: usize,
    phase: Phase,
    /// Woken during the current poll.
    woken: bool,
    /// Has polled at least once, so other tasks hold its waker.
    has_waker: bool,
    /// Stopped at a lock's point, so it resumes by trying to take the lock.
    taking: Option<usize>,
}

/// The kind of [`RunOutcome`] the model predicts.
#[derive(Debug, PartialEq, Eq)]
enum Kind {
    Reached,
    Waiting,
    Parked,
    Completed,
}

impl ModelTask {
    /// A task that hasn't started, ready at its first poll.
    fn new(acts: Vec<Act>) -> Self {
        ModelTask {
            acts,
            pc: 0,
            phase: Phase::Boundary,
            woken: false,
            has_waker: false,
            taking: None,
        }
    }
}

/// Every task's state, indexed like [`TaskId::index`].
struct Model {
    tasks: Vec<ModelTask>,
    children: Vec<Vec<Act>>,
    holders: Vec<Option<usize>>,
    workers: usize,
}

impl Model {
    /// Tasks that hold a worker.
    fn held(&self) -> usize {
        self.tasks
            .iter()
            .filter(|t| matches!(t.phase, Phase::MidPoll | Phase::Parked(_)))
            .count()
    }

    /// The tasks `run` should accept, with whether each is mid-poll.
    fn eligible(&self) -> Vec<(usize, bool)> {
        let free = self.held() < self.workers;
        self.tasks
            .iter()
            .enumerate()
            .filter_map(|(i, t)| match t.phase {
                Phase::MidPoll => Some((i, true)),
                Phase::Boundary if free => Some((i, false)),
                _ => None,
            })
            .collect()
    }

    /// A wake through task `j`'s waker.
    fn wake(&mut self, j: usize) {
        let t = &mut self.tasks[j];
        if !t.has_waker {
            return;
        }
        match t.phase {
            Phase::Waiting => t.phase = Phase::Boundary,
            Phase::MidPoll | Phase::Parked(_) | Phase::Running => t.woken = true,
            Phase::Boundary | Phase::Done => {}
        }
    }

    /// Runs task `i` to its next stop, as the scheduler should.
    fn run(&mut self, i: usize) -> Kind {
        if self.tasks[i].phase == Phase::Boundary {
            self.tasks[i].has_waker = true;
            self.tasks[i].woken = false;
        }
        self.tasks[i].phase = Phase::Running;
        if let Some(m) = self.tasks[i].taking {
            if let Some(kind) = self.take(i, m) {
                return kind;
            }
        }
        loop {
            let t = &mut self.tasks[i];
            let Some(&act) = t.acts.get(t.pc) else {
                t.phase = Phase::Done;
                return Kind::Completed;
            };
            t.pc += 1;
            match act {
                Act::Point => {
                    t.phase = Phase::MidPoll;
                    return Kind::Reached;
                }
                Act::Wake(j) => self.wake(j),
                Act::Pending => {
                    t.phase = if t.woken {
                        Phase::Boundary
                    } else {
                        Phase::Waiting
                    };
                    t.woken = false;
                    return Kind::Waiting;
                }
                Act::Lock(m) => {
                    t.taking = Some(m);
                    t.phase = Phase::MidPoll;
                    return Kind::Reached;
                }
                Act::Unlock(m) => {
                    self.holders[m] = None;
                    for t in &mut self.tasks {
                        if t.phase == Phase::Parked(m) {
                            t.phase = Phase::MidPoll;
                        }
                    }
                }
                Act::Spawn(c) => {
                    // Admitted when the step returns, ready at a poll boundary.
                    let acts = self.children[c].clone();
                    self.tasks.push(ModelTask::new(acts));
                }
            }
        }
    }

    /// Task `i` tries to take lock `m`. Returns `Parked` if it's held.
    fn take(&mut self, i: usize, m: usize) -> Option<Kind> {
        if self.holders[m].is_some() {
            self.tasks[i].phase = Phase::Parked(m);
            return Some(Kind::Parked);
        }
        self.holders[m] = Some(i);
        self.tasks[i].taking = None;
        None
    }
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

/// Runs one generated program with `workers`, checking each step against
/// the model.
fn check_program(workers: u16, seed: u64) {
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
        children: children.clone(),
        log: Vec::new(),
    }));
    for (i, acts) in roots.iter().enumerate() {
        let root = s.spawn_root("t", task_future(i, acts.clone(), world.clone()));
        assert_eq!(root.task().index(), i);
    }
    let mut model = Model {
        tasks: roots.into_iter().map(ModelTask::new).collect(),
        children,
        holders: vec![None; 2],
        workers: usize::from(workers),
    };
    let ctx = |step: usize| format!("seed {seed}, workers {workers}, step {step}");
    for step in 0.. {
        let want = model.eligible();
        let ready = s.ready();
        let got: Vec<(usize, bool)> = ready.iter().map(|r| (r.task.index(), r.mid_poll)).collect();
        assert_eq!(got, want, "eligible tasks, {}", ctx(step));
        assert!(model.held() <= usize::from(workers), "{}", ctx(step));
        if want.is_empty() {
            let status = s.status();
            if model.tasks.iter().all(|t| t.phase == Phase::Done) {
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
        let (pick, _) = want[walk.pick(want.len())];
        let before = world.borrow().log.len();
        let task = ready
            .iter()
            .find(|r| r.task.index() == pick)
            .expect("the model's pick is ready")
            .task;
        let outcome = s.run(task);
        let kind = match outcome {
            RunOutcome::Reached(_) => Kind::Reached,
            RunOutcome::Waiting => Kind::Waiting,
            RunOutcome::Parked(_) => Kind::Parked,
            RunOutcome::Completed => Kind::Completed,
            other => panic!("{other:?}, {}", ctx(step)),
        };
        assert_eq!(kind, model.run(pick), "outcome, {}", ctx(step));
        let ran = world.borrow().log[before..].to_vec();
        assert!(
            !ran.is_empty() && ran.iter().all(|t| *t == pick),
            "only task {pick} may run, but {ran:?} did, {}",
            ctx(step)
        );
    }
    s.shutdown();
}

#[test]
fn generated_programs_match_the_readiness_model() {
    for workers in [1, 2, 3] {
        for seed in 0..1_000 {
            check_program(workers, seed);
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
/// advances against an ordered model of pending timers.
fn check_timers(seed: u64) {
    let mut walk = Walk(seed);
    let mut s = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let fired = Arc::new(StdMutex::new(Vec::new()));
    // Pending timers by (deadline, registration order), as the scheduler
    // orders them.
    let mut model: std::collections::BTreeMap<(SimInstant, usize), TimerId> = Default::default();
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
                model.insert((deadline, registered), id);
                registered += 1;
            }
            2 if !model.is_empty() => {
                let key = *model.keys().nth(walk.pick(model.len())).unwrap();
                hooks::cancel_timer(model.remove(&key).unwrap());
                cancelled.push(key.1);
            }
            _ => {
                let to = match (walk.pick(2), s.next_deadline()) {
                    (0, Some(next)) => next,
                    _ => now.checked_add(ms(walk.pick(3) as u64)).unwrap(),
                };
                let later = model.split_off(&(to, usize::MAX));
                let due = std::mem::replace(&mut model, later);
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
            model.keys().next().map(|(d, _)| *d),
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
    assert_eq!(late.len(), model.len(), "seed {seed}");
}

#[test]
fn generated_timer_sequences_match_the_ordered_model() {
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
