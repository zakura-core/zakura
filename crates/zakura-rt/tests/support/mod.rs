//! A minimal [`Executor`] for testing the simulator build without
//! `zakura-scheduler`.
//!
//! It polls ready tasks in id order, and when none is ready it jumps virtual
//! time to the next timer. Preemption points are only recorded, since tasks
//! here aren't coroutines and can't stop inside a poll.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    mem,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    rc::Rc,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use zakura_rt::sim::{self, BoxTask, Executor, ObjectId, Op, Point, TaskExit, TaskId, TimerId};

/// What a finished run left behind.
pub struct Report {
    /// Each preemption point, with the raw id of the task that reached it.
    pub points: Vec<(Option<u64>, Op)>,
    /// Virtual time when the run went idle.
    pub now: Duration,
    /// Timers still registered when the run went idle.
    pub timers: usize,
}

impl Report {
    /// How many points of kind `op` any task reached.
    pub fn count(&self, op: Op) -> usize {
        self.points.iter().filter(|(_, seen)| *seen == op).count()
    }
}

/// Runs `future` as the root task until no task is ready and no timer is
/// left, then returns its output.
///
/// Re-raises the root task's panic, so `#[should_panic]` tests see it.
pub fn run<F>(future: F) -> (F::Output, Report)
where
    F: Future + 'static,
{
    let guard = sim::enter(&EXECUTOR);
    with(|s| *s = State::default());
    let output = Rc::new(RefCell::new(None));
    let slot = Rc::clone(&output);
    let root = EXECUTOR.spawn(
        "root",
        Box::pin(async move {
            let value = future.await;
            *slot.borrow_mut() = Some(value);
        }),
    );
    loop {
        while let Some(task) = pop_ready() {
            poll_task(task);
        }
        if !advance_time() {
            break;
        }
    }
    let (report, root_exit) = with(|s| {
        let report = Report {
            points: mem::take(&mut s.points),
            now: s.now,
            timers: s.timers.len(),
        };
        (report, s.tasks[index(root)].exit.take())
    });
    // Drop unfinished tasks outside the state borrow, since their
    // destructors call back into the executor.
    let unfinished: Vec<BoxTask> =
        with(|s| s.tasks.iter_mut().filter_map(|t| t.future.take()).collect());
    drop(unfinished);
    drop(guard);
    if let Some(TaskExit::Panicked(payload)) = root_exit {
        resume_unwind(payload);
    }
    let value = output
        .borrow_mut()
        .take()
        .expect("the root task ran to completion, since the run only stops when nothing can run");
    (value, report)
}

static EXECUTOR: TestExecutor = TestExecutor;

struct TestExecutor;

#[derive(Default)]
struct State {
    now: Duration,
    tasks: Vec<Task>,
    ready: Arc<Mutex<BTreeSet<u64>>>,
    timers: BTreeMap<TimerId, Waker>,
    next_timer: u64,
    next_object: u32,
    current: Option<TaskId>,
    points: Vec<(Option<u64>, Op)>,
}

#[derive(Default)]
struct Task {
    future: Option<BoxTask>,
    exit: Option<TaskExit>,
    finished: bool,
    cancel: bool,
    join_wakers: Vec<Waker>,
}

thread_local! {
    static STATE: RefCell<State> = RefCell::default();
}

/// Runs `f` with the run state borrowed. Never call back into zakura-rt
/// inside `f`.
fn with<R>(f: impl FnOnce(&mut State) -> R) -> R {
    STATE.with(|state| f(&mut state.borrow_mut()))
}

/// A task id as a vector index.
fn index(task: TaskId) -> usize {
    usize::try_from(task.get()).expect("test runs spawn far fewer than usize::MAX tasks")
}

/// Marks a task ready when woken.
struct TaskWaker {
    task: u64,
    ready: Arc<Mutex<BTreeSet<u64>>>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(self.task);
    }
}

/// The lowest ready task, if any.
fn pop_ready() -> Option<u64> {
    let ready = with(|s| Arc::clone(&s.ready));
    let next = ready
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .pop_first();
    next
}

/// Polls one task, or drops it if it was aborted.
fn poll_task(raw: u64) {
    let task = TaskId::new(raw);
    let (future, cancel, ready) = with(|s| {
        let t = &mut s.tasks[index(task)];
        (t.future.take(), t.cancel, Arc::clone(&s.ready))
    });
    let Some(mut future) = future else {
        return;
    };
    if cancel {
        drop(future);
        finish(task, TaskExit::Cancelled);
        return;
    }
    let waker = Waker::from(Arc::new(TaskWaker { task: raw, ready }));
    with(|s| s.current = Some(task));
    let polled = catch_unwind(AssertUnwindSafe(|| {
        future.as_mut().poll(&mut Context::from_waker(&waker))
    }));
    with(|s| s.current = None);
    match polled {
        Ok(Poll::Pending) => with(|s| s.tasks[index(task)].future = Some(future)),
        Ok(Poll::Ready(())) => {
            drop(future);
            finish(task, TaskExit::Completed);
        }
        Err(payload) => {
            drop(future);
            finish(task, TaskExit::Panicked(payload));
        }
    }
}

/// Records how `task` ended and wakes its joiners.
fn finish(task: TaskId, exit: TaskExit) {
    let joiners = with(|s| {
        let t = &mut s.tasks[index(task)];
        t.finished = true;
        t.exit = Some(exit);
        mem::take(&mut t.join_wakers)
    });
    for waker in joiners {
        waker.wake();
    }
}

/// Jumps to the next timer and fires every timer due then. False if no timer
/// is left.
fn advance_time() -> bool {
    let due = with(|s| {
        let next = s.timers.keys().next()?.deadline();
        s.now = next;
        let due: Vec<TimerId> = s
            .timers
            .keys()
            .take_while(|timer| timer.deadline() <= next)
            .copied()
            .collect();
        Some(
            due.iter()
                .filter_map(|timer| s.timers.remove(timer))
                .collect::<Vec<_>>(),
        )
    });
    match due {
        Some(wakers) => {
            wakers.into_iter().for_each(Waker::wake);
            true
        }
        None => false,
    }
}

impl Executor for TestExecutor {
    fn preempt(&self, point: Point) {
        with(|s| {
            let task = s.current.map(TaskId::get);
            s.points.push((task, point.op));
        });
    }

    fn park(&self, _wait: ObjectId, _held_by: Option<TaskId>) -> bool {
        false
    }

    fn unpark_all(&self, _wait: ObjectId) {}

    fn spawn(&self, _label: &'static str, task: BoxTask) -> TaskId {
        with(|s| {
            let raw = u64::try_from(s.tasks.len()).expect("task count fits in u64");
            s.tasks.push(Task {
                future: Some(task),
                ..Task::default()
            });
            s.ready
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(raw);
            TaskId::new(raw)
        })
    }

    fn abort(&self, task: TaskId) {
        with(|s| {
            let t = &mut s.tasks[index(task)];
            if !t.finished {
                t.cancel = true;
                s.ready
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(task.get());
            }
        });
    }

    fn is_finished(&self, task: TaskId) -> bool {
        with(|s| s.tasks[index(task)].finished)
    }

    fn poll_join(&self, task: TaskId, cx: &mut Context<'_>) -> Poll<TaskExit> {
        let waker = cx.waker().clone();
        with(|s| {
            let t = &mut s.tasks[index(task)];
            if t.finished {
                return Poll::Ready(t.exit.take().expect("each task's exit is joined once"));
            }
            if !t.join_wakers.iter().any(|w| w.will_wake(&waker)) {
                t.join_wakers.push(waker);
            }
            Poll::Pending
        })
    }

    fn now(&self) -> Duration {
        with(|s| s.now)
    }

    fn register_timer(&self, deadline: Duration, waker: &Waker) -> TimerId {
        let waker = waker.clone();
        with(|s| {
            s.next_timer += 1;
            let timer = TimerId::new(deadline, s.next_timer);
            s.timers.insert(timer, waker);
            timer
        })
    }

    fn cancel_timer(&self, timer: TimerId) {
        let removed = with(|s| s.timers.remove(&timer));
        drop(removed);
    }

    fn new_object(&self) -> ObjectId {
        with(|s| {
            s.next_object += 1;
            ObjectId {
                creator: s.current,
                ordinal: s.next_object,
            }
        })
    }

    fn current_task(&self) -> Option<TaskId> {
        with(|s| s.current)
    }

    fn set_external_wait(&self, _waiting: bool) {}
}
