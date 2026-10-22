//! The calls runtime primitives make into the running scheduler.
//!
//! These will back `zakura_rt::sim::Executor`. Outside a run and while a
//! thread is unwinding, every hook that would suspend returns at once, so
//! primitives behave like production. During shutdown, preemption points
//! return at once, but `park` still waits (see `Scheduler::shutdown`).

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use crate::{
    scheduler::{with_run, BoxTask, RunState, TaskYielder, Yield},
    Draw, DrawKind, ObjectId, Point, SimInstant, TaskExit, TaskId, TimerId,
};

/// True while this thread runs a simulation.
pub fn in_run() -> bool {
    with_run(|_| ()).is_some()
}

/// The task running now, or `None` outside a task.
pub fn current_task() -> Option<TaskId> {
    with_run(|s| s.current).flatten()
}

/// A preemption point. The scheduler may run other tasks before this returns.
///
/// Primitives call this before every operation on shared state.
pub fn preempt(point: Point) {
    if std::thread::panicking() {
        return;
    }
    let yielder = with_run(|s| {
        let task = s.current.filter(|_| !s.shutting_down)?;
        s.advance_point(task, point.op, Some(point.site));
        Some(s.tasks[task.index()].yielder)
    })
    .flatten();
    if let Some(yielder) = yielder {
        suspend(yielder, Yield::Preempt);
    }
}

/// Parks the current task until [`unpark_all`] on `wait`, keeping its worker.
///
/// `held_by` names the task holding `wait`, if known, so the scheduler can
/// tell a lock cycle from starved workers. Returns false without parking
/// outside a task or while the thread is unwinding. The caller then can't
/// wait, and should treat that as a bug.
pub fn park(wait: ObjectId, held_by: Option<TaskId>) -> bool {
    if std::thread::panicking() {
        return false;
    }
    let yielder = with_run(|s| {
        let task = s.current?;
        s.parked.entry(wait).or_default().push(task);
        match held_by {
            Some(holder) => s.holders.insert(wait, holder),
            None => s.holders.remove(&wait),
        };
        Some(s.tasks[task.index()].yielder)
    })
    .flatten();
    match yielder {
        Some(yielder) => {
            suspend(yielder, Yield::Park(wait));
            true
        }
        None => false,
    }
}

/// Makes every task parked on `wait` ready again. Each one retries, and the
/// caller's next pick decides who wins. Not a preemption point.
pub fn unpark_all(wait: ObjectId) {
    with_run(|s| {
        s.holders.remove(&wait);
        if let Some(tasks) = s.parked.remove(&wait) {
            s.ready.extend(tasks);
        }
    });
}

/// Spawns a child of the current task. It becomes ready once the current
/// step returns. From setup code outside any task, it spawns a root task
/// that is ready at once.
///
/// # Panics
///
/// Outside a run, or past the run's task limit.
pub fn spawn(label: &'static str, task: impl Future<Output = ()> + 'static) -> JoinHandle {
    JoinHandle::new(spawn_boxed(label, Box::pin(task)))
}

/// [`spawn`] for a task that is already boxed.
pub(crate) fn spawn_boxed(label: &'static str, task: BoxTask) -> TaskId {
    let queued = with_run(|s| s.queue_spawn(label, task))
        .expect("zakura-rt spawns only while this run's executor is installed, and the run state outlives that");
    queued.unwrap_or_else(|rejected| RunState::reject_spawn(rejected))
}

/// [`spawn_boxed`], but returns `None` past the run's task limit, or outside
/// a run, instead of panicking. The rejected task is dropped unpolled.
#[cfg(zakura_sim)]
pub(crate) fn try_spawn_boxed(label: &'static str, task: BoxTask) -> Option<TaskId> {
    // Drop a rejected task outside the borrow, since its destructor can run
    // arbitrary code.
    with_run(|s| s.queue_spawn(label, task))?.ok()
}

/// Requests cancellation of `task` without a preemption point. See
/// [`JoinHandle::abort`].
pub(crate) fn request_abort(task: TaskId) {
    with_run(|s| {
        if s.done.contains_key(&task) {
            return;
        }
        s.cancel_requested.insert(task);
        if s.waiting.remove(&task) {
            s.ready.insert(task);
        }
    });
}

/// The current virtual time, or `None` outside a run.
pub fn now() -> Option<SimInstant> {
    with_run(|s| s.now)
}

/// Registers `waker` to fire when time reaches `deadline`.
///
/// Store the `Waker` the timer's future was polled with, not the task's.
/// That way a wakeup reaches the right child future inside a `select!` or a
/// `FuturesUnordered`.
///
/// # Panics
///
/// Outside a run.
pub fn register_timer(deadline: SimInstant, waker: &Waker) -> TimerId {
    // Clone outside the borrow, since cloning a waker can run arbitrary code.
    let waker = waker.clone();
    with_run(|s| s.register_timer(deadline, waker))
        .expect("zakura-rt checks in_run before registering a virtual timer")
}

/// Removes a timer. A no-op if it already fired or the run ended.
pub fn cancel_timer(timer: TimerId) {
    // Drop the waker outside the borrow, since its destructor can be
    // arbitrary code.
    let removed = with_run(|s| s.timers.remove(&timer)).flatten();
    drop(removed);
}

/// A fresh identity for a runtime object, or `None` outside a run.
pub fn new_object() -> Option<ObjectId> {
    with_run(|s| s.new_object())
}

/// Marks whether the current task waits on a source outside the scheduler.
///
/// While any such task waits, an idle run reports
/// [`Status::WaitingOnExternal`](crate::Status::WaitingOnExternal) instead of
/// `Deadlock`.
pub fn set_external_wait(waiting: bool) {
    with_run(|s| {
        if let Some(task) = s.current {
            if waiting {
                s.external.insert(task);
            } else {
                s.external.remove(&task);
            }
        }
    });
}

/// Asks the run's draw source for a random choice: a value below `bound`, or
/// any `u64` when `bound` is 0. Not a preemption point. Returns 0 outside a
/// run or without a source.
pub fn draw(kind: DrawKind, site: &'static std::panic::Location<'static>, bound: u64) -> u64 {
    let Some((source, task)) = with_run(|s| (s.draws.take(), s.current)) else {
        return 0;
    };
    let Some(mut source) = source else {
        return 0;
    };
    // Called outside the borrow, since the source is the caller's code.
    let value = source(&Draw {
        kind,
        site,
        task,
        bound,
    });
    // Keep a source the caller set during the draw. Drop the loser outside
    // the borrow.
    let replaced = with_run(|s| match s.draws {
        None => {
            s.draws = Some(source);
            None
        }
        Some(_) => Some(source),
    });
    drop(replaced);
    match bound {
        0 => value,
        bound => value % bound,
    }
}

/// Suspends the current coroutine.
///
/// Doesn't suspend while the thread is unwinding, because suspending a
/// coroutine that corosensei is force-unwinding restarts the unwind.
#[allow(unsafe_code)]
fn suspend(yielder: *const TaskYielder, why: Yield) {
    if std::thread::panicking() || yielder.is_null() {
        return;
    }
    // SAFETY: the yielder lives on the current coroutine's stack for as long
    // as that coroutine runs. The scheduler sets `current` only while it has
    // resumed that coroutine, and hooks read the pointer through `current`,
    // so the pointer is live here. Resumes after a mid-poll suspend always
    // send `Cmd::Poll`, which this ignores.
    unsafe { (*yielder).suspend(why) };
}

/// A handle to a spawned task. Awaiting it waits for the task to end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinHandle {
    task: TaskId,
}

impl JoinHandle {
    /// Wraps a task id.
    pub(crate) fn new(task: TaskId) -> Self {
        JoinHandle { task }
    }

    /// The task this handle refers to.
    pub fn task(&self) -> TaskId {
        self.task
    }

    /// Requests cancellation, as tokio's abort does. A task waiting for a
    /// wakeup becomes ready, and its future is dropped the next time it runs.
    /// A task mid-poll is cancelled when that poll returns `Pending`.
    #[track_caller]
    pub fn abort(&self) {
        preempt(Point::here(crate::Op::Abort));
        request_abort(self.task);
    }

    /// True once the task has ended.
    pub fn is_finished(&self) -> bool {
        with_run(|s| s.done.contains_key(&self.task)).unwrap_or(false)
    }
}

impl Future for JoinHandle {
    type Output = TaskExit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<TaskExit> {
        // Clone outside the borrow, since cloning a waker can run arbitrary code.
        let waker = cx.waker().clone();
        let (poll, unused) = with_run(|s| match s.done.get(&self.task) {
            Some(exit) => (Poll::Ready(*exit), Some(waker)),
            None => {
                // A repeat poll from the same waiter, as in a `select!` loop,
                // must not grow the list.
                let wakers = s.join_wakers.entry(self.task).or_default();
                if wakers.iter().any(|w| w.will_wake(&waker)) {
                    (Poll::Pending, Some(waker))
                } else {
                    wakers.push(waker);
                    (Poll::Pending, None)
                }
            }
        })
        .expect("a JoinHandle is only polled inside the run that spawned it");
        // Drop outside the borrow too, for the same reason.
        drop(unused);
        poll
    }
}
