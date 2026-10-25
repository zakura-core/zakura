//! The interface between `zakura-rt` and the simulator's scheduler.
//!
//! Only the `--cfg zakura_sim` build has this module. A scheduler implements
//! [`Executor`] and installs it with [`enter`] for the length of a run. While
//! it's installed, the sim versions of the runtime names hand every task,
//! timer and preemption point to it. Outside a run, [`executor`] is `None` and
//! the same names behave like production.
//!
//! Production code never calls this module. Each runtime primitive places its
//! own preemption points.

use std::{
    any::Any,
    cell::Cell,
    fmt,
    future::Future,
    marker::PhantomData,
    panic::Location,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll, Waker},
    time::Duration,
};

/// A task future as the executor stores it.
///
/// It needn't be `Send`, because a run happens on one OS thread.
pub type BoxTask = Pin<Box<dyn Future<Output = ()> + 'static>>;

/// What a deterministic scheduler provides to the runtime primitives.
///
/// Virtual time is a [`Duration`] since the start of the run, and only the
/// scheduler moves it. Every method is called on the thread that called
/// [`enter`].
pub trait Executor {
    /// A preemption point. The scheduler may run other tasks before this
    /// returns. Does nothing outside a task, during shutdown, or while the
    /// thread is unwinding.
    fn preempt(&self, point: Point);

    /// Parks the current task until [`Executor::unpark_all`] on `wait`. The
    /// task keeps its simulated worker. Only synchronous waits, such as a
    /// contended `Mutex`, park. Async waits return `Pending` instead.
    ///
    /// `held_by` names the task holding `wait`, if known, so the scheduler can
    /// tell a lock cycle from starved workers. Returns false without parking
    /// when the current task can't park, such as while it is unwinding.
    fn park(&self, wait: ObjectId, held_by: Option<TaskId>) -> bool;

    /// Makes every task parked on `wait` ready again. Not a preemption point.
    fn unpark_all(&self, wait: ObjectId);

    /// Spawns `task` as a child of the current task, or as a root task outside
    /// one. `label` names the spawn site in task identities.
    fn spawn(&self, label: &'static str, task: BoxTask) -> TaskId;

    /// Spawns `task` as [`Executor::spawn`] does, or returns `None` and drops
    /// it unpolled when the run can't take another task. Code that spawns
    /// from a `Drop` impl uses this, since a panic there could abort.
    fn try_spawn(&self, label: &'static str, task: BoxTask) -> Option<TaskId>;

    /// Requests cancellation. The task's future is dropped at its next poll
    /// boundary, as with tokio's abort. A no-op once the task has ended.
    fn abort(&self, task: TaskId);

    /// True once `task` has ended.
    fn is_finished(&self, task: TaskId) -> bool;

    /// Polls for the end of `task`, registering `cx`'s waker while it runs.
    ///
    /// Returns each task's exit once. A panic payload moves to the caller.
    fn poll_join(&self, task: TaskId, cx: &mut Context<'_>) -> Poll<TaskExit>;

    /// The current virtual time.
    fn now(&self) -> Duration;

    /// Registers `waker` to fire when virtual time reaches `deadline`.
    ///
    /// Primitives pass the `Waker` they were polled with, so the wakeup
    /// reaches the right child future inside a `select!`.
    fn register_timer(&self, deadline: Duration, waker: &Waker) -> TimerId;

    /// Removes a timer. A no-op if it already fired.
    fn cancel_timer(&self, timer: TimerId);

    /// A fresh identity for a runtime object, such as a `Mutex` or a channel.
    fn new_object(&self) -> ObjectId;

    /// The task running now, or `None` in setup code between steps.
    fn current_task(&self) -> Option<TaskId>;

    /// Marks whether the current task waits on a source outside the
    /// scheduler, so an idle run isn't reported as a deadlock.
    fn set_external_wait(&self, waiting: bool);

    /// A random choice for `site`, taken from the run's `Plan`. Returns a
    /// value below `bound`, or any `u64` when `bound` is 0.
    fn draw(&self, site: DrawSite, bound: u64) -> u64;

    /// UTC at virtual time zero. [`crate::time::utc_now`] adds virtual time
    /// to it. The default, [`DEFAULT_UTC_ORIGIN_SINCE_EPOCH`] after the Unix
    /// epoch, keeps a run that sets no origin deterministic.
    ///
    /// The executor must keep the origin plus [`Executor::now`] within what
    /// `SystemTime` can hold, refusing to move time past it, so a task's
    /// `utc_now` never overflows.
    fn utc_origin(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + DEFAULT_UTC_ORIGIN_SINCE_EPOCH
    }
}

/// [`Executor::utc_origin`]'s default, as time since the Unix epoch:
/// 2026-01-01T00:00:00Z.
pub const DEFAULT_UTC_ORIGIN_SINCE_EPOCH: Duration = Duration::from_secs(1_767_225_600);

/// The run installed on a thread.
#[derive(Clone, Copy)]
struct Run {
    executor: &'static dyn Executor,
    id: RunId,
}

thread_local! {
    static RUN: Cell<Option<Run>> = const { Cell::new(None) };
    /// How many `watch` locks this thread holds now. See [`WatchLockHeld`].
    static WATCH_LOCKS: Cell<u32> = const { Cell::new(0) };
    /// Points skipped in the current or last run because a `watch` lock was
    /// held.
    static SUPPRESSED: Cell<u64> = const { Cell::new(0) };
}

/// The next [`RunId`]. Process-wide, so an id never repeats on any thread.
static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

/// One run, unique within the process.
///
/// Task, timer and object ids are only meaningful in the run that assigned
/// them, and executors reuse them across runs. So every sim handle that holds
/// an id also holds its run, and reaches the executor through
/// [`executor_for`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(u64);

impl RunId {
    /// The raw id, counting runs started in this process from 1.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "run {}", self.0)
    }
}

/// The executor of the run on this thread, or `None` outside a run.
pub fn executor() -> Option<&'static dyn Executor> {
    current().map(|run| run.executor)
}

/// The run on this thread, or `None` outside a run.
pub fn run_id() -> Option<RunId> {
    current().map(|run| run.id)
}

/// The executor of `run`, or `None` once that run has ended or on another
/// thread. A handle from an earlier run then acts as if its task or timer
/// ended with the run, instead of reaching a later run's task or timer with
/// the same id.
pub fn executor_for(run: RunId) -> Option<&'static dyn Executor> {
    current()
        .filter(|current| current.id == run)
        .map(|current| current.executor)
}

/// The run on this thread, if any.
fn current() -> Option<Run> {
    RUN.try_with(Cell::get).ok().flatten()
}

/// Installs `executor` for a run on this thread, until the guard drops.
///
/// # Panics
///
/// If this thread is already in a run. Runs don't nest.
pub fn enter(executor: &'static dyn Executor) -> RunGuard {
    RUN.with(|cell| {
        assert!(
            cell.get().is_none(),
            "a zakura-rt sim run is already active on this thread"
        );
        let id = RunId(NEXT_RUN.fetch_add(1, Ordering::Relaxed));
        cell.set(Some(Run { executor, id }));
    });
    let _ = SUPPRESSED.try_with(|cell| cell.set(0));
    crate::collections::reset_run_seed();
    crate::rand::reset_run_stream();
    RunGuard {
        _not_send: PhantomData,
    }
}

/// Ends the run started by [`enter`] when dropped.
#[must_use = "the run ends when the guard drops"]
pub struct RunGuard {
    // The executor is installed per thread, so the guard must stay on it.
    _not_send: PhantomData<*const ()>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        let _ = RUN.try_with(|cell| cell.set(None));
        crate::collections::reset_run_seed();
        crate::rand::reset_run_stream();
    }
}

impl fmt::Debug for RunGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RunGuard")
    }
}

/// A task's identity, as the executor assigns it. Only meaningful in its run
/// (see [`RunId`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(u64);

impl TaskId {
    /// Wraps the executor's raw id.
    pub const fn new(raw: u64) -> Self {
        TaskId(raw)
    }

    /// The executor's raw id.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A runtime object's identity: the creating task (or `None` for setup code)
/// and a creation ordinal, so it stays stable when unrelated tasks change.
/// Only meaningful in its run (see [`RunId`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId {
    /// The task that created the object, or `None` for setup code.
    pub creator: Option<TaskId>,
    /// How many objects that creator made before this one.
    pub ordinal: u32,
}

/// A registered timer. Ordered by deadline, then by registration. Only
/// meaningful in its run (see [`RunId`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerId {
    deadline: Duration,
    seq: u64,
}

impl TimerId {
    /// A timer that fires at `deadline`, the `seq`-th the executor registered.
    pub const fn new(deadline: Duration, seq: u64) -> Self {
        TimerId { deadline, seq }
    }

    /// When this timer fires, in virtual time.
    pub const fn deadline(self) -> Duration {
        self.deadline
    }

    /// The executor's registration counter for this timer.
    pub const fn seq(self) -> u64 {
        self.seq
    }
}

/// How a task ended.
pub enum TaskExit {
    /// The future returned `Ready`.
    Completed,
    /// The task was aborted before it finished.
    Cancelled,
    /// The future panicked. Holds the panic payload.
    Panicked(Box<dyn Any + Send + 'static>),
}

impl fmt::Debug for TaskExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskExit::Completed => f.write_str("Completed"),
            TaskExit::Cancelled => f.write_str("Cancelled"),
            TaskExit::Panicked(_) => f.write_str("Panicked(..)"),
        }
    }
}

/// The operation a task is about to perform at a preemption point.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Op {
    /// `Mutex::lock` and `try_lock`.
    Lock,
    /// Right after a release, such as a guard or permit drop.
    Release,
    /// A channel send or reserve.
    Send,
    /// A channel receive.
    Recv,
    /// `Notify::notify_one` or `notify_waiters`.
    Notify,
    /// `Notify::notified` creation.
    Notified,
    /// A `Semaphore` acquire or permit count read.
    Acquire,
    /// `CancellationToken::cancel`.
    Cancel,
    /// `CancellationToken::is_cancelled` or the first poll of `cancelled`.
    Cancelled,
    /// An atomic load.
    AtomicLoad,
    /// An atomic store.
    AtomicStore,
    /// An atomic read-modify-write.
    AtomicRmw,
    /// A task spawn.
    Spawn,
    /// `JoinHandle::is_finished`.
    Join,
    /// `JoinHandle::abort`.
    Abort,
    /// A timer's registration or expiry.
    Sleep,
    /// Polling the branches of a `select!`.
    Select,
    /// A `watch` read or write.
    Watch,
}

impl Op {
    /// Every operation, in declaration order. Executors test their mapping
    /// over it, so a new variant must be added here too.
    pub const ALL: [Op; 18] = [
        Op::Lock,
        Op::Release,
        Op::Send,
        Op::Recv,
        Op::Notify,
        Op::Notified,
        Op::Acquire,
        Op::Cancel,
        Op::Cancelled,
        Op::AtomicLoad,
        Op::AtomicStore,
        Op::AtomicRmw,
        Op::Spawn,
        Op::Join,
        Op::Abort,
        Op::Sleep,
        Op::Select,
        Op::Watch,
    ];

    /// The short name traces use, such as `lock` in `lock#2`. An executor's
    /// own operation names must agree with these.
    pub const fn name(self) -> &'static str {
        match self {
            Op::Lock => "lock",
            Op::Release => "release",
            Op::Send => "send",
            Op::Recv => "recv",
            Op::Notify => "notify",
            Op::Notified => "notified",
            Op::Acquire => "acquire",
            Op::Cancel => "cancel",
            Op::Cancelled => "cancelled",
            Op::AtomicLoad => "load",
            Op::AtomicStore => "store",
            Op::AtomicRmw => "rmw",
            Op::Spawn => "spawn",
            Op::Join => "join",
            Op::Abort => "abort",
            Op::Sleep => "sleep",
            Op::Select => "select",
            Op::Watch => "watch",
        }
    }
}

// `ALL` lists each variant once, in declaration order.
const _: () = {
    let mut i = 0;
    while i < Op::ALL.len() {
        // A fieldless enum's discriminant is its declaration index.
        assert!(Op::ALL[i] as usize == i);
        i += 1;
    }
};

/// Where a random choice comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum DrawKind {
    /// The seed of the run's `zakura_rt::rand::rng()` stream, drawn once by
    /// the first value.
    Rng,
    /// The first branch an unbiased `select!` polls.
    Select,
    /// The run's hash seed, drawn once by the first hashed collection.
    HashSeed,
    /// The first stream a `StreamMap` polls.
    StreamMap,
}

impl DrawKind {
    /// Every draw kind, in declaration order. Executors test their mapping
    /// over it, so a new variant must be added here too.
    pub const ALL: [DrawKind; 4] = [
        DrawKind::Rng,
        DrawKind::Select,
        DrawKind::HashSeed,
        DrawKind::StreamMap,
    ];
}

// `ALL` lists each variant once, in declaration order.
const _: () = {
    let mut i = 0;
    while i < DrawKind::ALL.len() {
        // A fieldless enum's discriminant is its declaration index.
        assert!(DrawKind::ALL[i] as usize == i);
        i += 1;
    }
};

/// A random choice's kind and the source location that asked for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrawSite {
    /// What the draw is for.
    pub kind: DrawKind,
    /// Where production code asked for it.
    pub site: &'static Location<'static>,
}

/// A preemption point, as a runtime primitive reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Point {
    /// The operation about to run.
    pub op: Op,
    /// The object it touches, if the primitive has one.
    pub object: Option<ObjectId>,
    /// Where production code called the primitive.
    pub site: &'static Location<'static>,
}

impl Point {
    /// A point for `op` at the caller's source location.
    #[track_caller]
    pub fn here(op: Op) -> Self {
        Point::at(op, Location::caller())
    }

    /// A point for `op` at `site`, for primitives that recorded their call
    /// site earlier, such as a timer polled after it was created.
    pub fn at(op: Op, site: &'static Location<'static>) -> Self {
        Point {
            op,
            object: None,
            site,
        }
    }
}

/// A preemption point for `op` at `site`, if this thread is in a run.
///
/// Wrappers call this before each operation on shared state. Outside a run
/// it does nothing, so the wrapper behaves like the type it wraps.
pub(crate) fn point(op: Op, site: &'static Location<'static>) {
    if let Some(executor) = executor() {
        preempt(executor, Point::at(op, site));
    }
}

/// `executor`'s preemption point, unless this thread holds a `watch` lock.
///
/// Every runtime primitive preempts through here. Tokio's `watch` keeps its
/// value behind a std `RwLock`, so a switch to a task that writes the value
/// would block the run's only thread. While a `watch` `Ref` or
/// `send_modify` closure holds that lock, points are skipped and counted in
/// [`suppressed_points`].
pub(crate) fn preempt(executor: &dyn Executor, point: Point) {
    if WATCH_LOCKS.try_with(Cell::get).unwrap_or(0) > 0 {
        let _ = SUPPRESSED.try_with(|cell| cell.set(cell.get().saturating_add(1)));
        return;
    }
    executor.preempt(point);
}

/// How many preemption points the current run on this thread, or the last
/// one, skipped because a `watch` lock was held. A harness can read it to see
/// how much of a run went unexplored.
pub fn suppressed_points() -> u64 {
    SUPPRESSED.try_with(Cell::get).unwrap_or(0)
}

/// Marks this thread as holding a `watch` lock until it drops. See
/// [`preempt`].
///
/// A `Ref` isn't `Send`, so a spawned task can't hold one across an await,
/// and the count never leaks into another task's poll.
pub(crate) struct WatchLockHeld(PhantomData<*const ()>);

impl WatchLockHeld {
    /// Counts one more held `watch` lock on this thread.
    pub(crate) fn new() -> Self {
        let _ = WATCH_LOCKS.try_with(|cell| cell.set(cell.get().saturating_add(1)));
        WatchLockHeld(PhantomData)
    }
}

impl Drop for WatchLockHeld {
    fn drop(&mut self) {
        let _ = WATCH_LOCKS.try_with(|cell| cell.set(cell.get().saturating_sub(1)));
    }
}

/// Tokio, for the sim `select!`, which expands to tokio's macro.
#[doc(hidden)]
pub use tokio as __tokio;

/// The branch a sim `select!` polls first, with the poll's preemption point.
///
/// Not for direct use. `zakura_rt::select!` calls it on every poll, through
/// tokio's internal start-index rule. A biased select always starts at 0.
/// Outside a run, an unbiased one uses tokio's thread RNG, as in production.
#[doc(hidden)]
#[track_caller]
pub fn select_start(branches: u32, biased: bool) -> u32 {
    let site = Location::caller();
    point(Op::Select, site);
    if biased {
        return 0;
    }
    start_index(DrawKind::Select, site, branches)
}

/// A start index below `n` for a `kind` choice at `site`: a draw from the
/// run's `Plan` inside a run, or tokio's thread RNG outside one, as in
/// production. `n` must be nonzero.
pub(crate) fn start_index(kind: DrawKind, site: &'static Location<'static>, n: u32) -> u32 {
    match executor() {
        Some(executor) => {
            let start = executor.draw(DrawSite { kind, site }, u64::from(n));
            u32::try_from(start).expect("a draw below `n` fits in u32")
        }
        None => tokio::macros::support::thread_rng_n(n),
    }
}
