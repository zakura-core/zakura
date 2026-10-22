//! Deterministic task scheduling for Zakura's P2P simulator.
//!
//! Every task of a run is a stackful coroutine, and the whole run happens on
//! one OS thread. A task can stop at any preemption point, even inside
//! synchronous code such as a contended `Mutex::lock`.
//!
//! The scheduler never chooses what runs next. [`Scheduler::ready`] reports
//! which tasks can run, and the caller (`zakura-sim`) picks one and passes it
//! to [`Scheduler::run`]. Virtual time moves only through
//! [`Scheduler::advance_to`]. So every run is a function of the caller's
//! choices, and replaying the same choices replays the run.
//!
//! Runtime primitives reach the running scheduler through [`hooks`]. Outside a
//! run every hook does nothing or returns `None`, so code that calls them keeps
//! working in ordinary tests.
//!
//! The design is in the `scheduler.md` plan. This crate doesn't yet implement
//! `zakura_rt::sim::Executor`, and it has no `draw` for random choices. Both
//! wait for the runtime's sim build.

use std::{fmt, panic::Location, time::Duration};

pub mod hooks;
mod scheduler;

pub use hooks::JoinHandle;
pub use scheduler::Scheduler;

/// A task's identity within one run, assigned in spawn order.
///
/// Spawn order follows the caller's choices, so ids replay with them. Use
/// [`Scheduler::task_name`] for an identity that survives unrelated code
/// changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(pub(crate) u32);

impl TaskId {
    /// The task's position in spawn order.
    pub fn index(self) -> usize {
        self.0 as usize // u32 always fits in usize on supported targets
    }
}

/// A runtime object, such as a `Mutex`, that tasks can park on.
///
/// The identity is the creating task (or `None` outside any task) and a
/// creation ordinal, so it stays stable when unrelated tasks change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId {
    /// The task that created the object, or `None` for setup code.
    pub creator: Option<TaskId>,
    /// How many objects that creator made before this one.
    pub ordinal: u32,
}

/// A registered timer. Ordered by deadline, then by registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerId {
    deadline: SimInstant,
    seq: u64,
}

impl TimerId {
    /// When this timer fires.
    pub fn deadline(self) -> SimInstant {
        self.deadline
    }
}

/// A point in virtual time, measured from the start of the run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimInstant(Duration);

impl SimInstant {
    /// The start of a run.
    pub const ZERO: SimInstant = SimInstant(Duration::ZERO);

    /// The instant `since_start` after the start of the run.
    pub fn from_start(since_start: Duration) -> Self {
        SimInstant(since_start)
    }

    /// How long after the start of the run this instant is.
    pub fn since_start(self) -> Duration {
        self.0
    }

    /// This instant plus `d`, or `None` on overflow.
    pub fn checked_add(self, d: Duration) -> Option<Self> {
        self.0.checked_add(d).map(SimInstant)
    }

    /// How long after `earlier` this instant is, or zero if it isn't later.
    pub fn saturating_duration_since(self, earlier: SimInstant) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}

/// The operation a task is about to perform at a preemption point.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Op {
    /// The top of a task's coroutine, before a poll starts.
    Poll,
    /// `Mutex::lock` and `try_lock`.
    Lock,
    /// A channel send or reserve.
    Send,
    /// A channel receive.
    Recv,
    /// `Notify::notify_one` or `notify_waiters`.
    Notify,
    /// `Notify::notified` creation.
    Notified,
    /// A `Semaphore` acquire.
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
    /// A timer registration.
    Sleep,
    /// Polling the branches of a `select!`.
    Select,
    /// `yield_now`.
    Yield,
    /// `JoinHandle::abort`.
    Abort,
    /// A `watch` read or write.
    Watch,
}

impl Op {
    /// The short name used in task identities, such as `lock` in `lock#2`.
    pub fn name(self) -> &'static str {
        match self {
            Op::Poll => "poll",
            Op::Lock => "lock",
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
            Op::Sleep => "sleep",
            Op::Select => "select",
            Op::Yield => "yield",
            Op::Abort => "abort",
            Op::Watch => "watch",
        }
    }
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
        Point {
            op,
            object: None,
            site: Location::caller(),
        }
    }
}

/// Where a ready task will resume: its `n`-th point of kind `op`.
///
/// A task at the top of its coroutine is at `poll#n`, where `n` counts its
/// polls. Displayed as `kind#n`, the form traces use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct At {
    /// The operation the task stopped before.
    pub op: Op,
    /// How many points of this kind the task has reached, this one included.
    pub n: u32,
    /// The point's source location. `None` at the top of the coroutine.
    pub site: Option<&'static Location<'static>>,
}

impl fmt::Display for At {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.op.name(), self.n)
    }
}

/// A task that [`Scheduler::run`] accepts now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadyTask {
    /// The task.
    pub task: TaskId,
    /// Where it will resume.
    pub at: At,
    /// True if it stopped mid-poll, so it already holds a worker.
    pub mid_poll: bool,
}

/// How a call to [`Scheduler::run`] ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    /// The task reached a preemption point and stays ready.
    Reached(At),
    /// The task's future returned `Pending`. It waits for its `Waker` and
    /// released its worker.
    Waiting,
    /// The task parked on a sync wait and keeps its worker.
    Parked(ObjectId),
    /// The task finished.
    Completed,
    /// The task was aborted. Its future was dropped at a poll boundary.
    Cancelled,
    /// The task panicked. Other tasks keep running.
    Panicked(String),
}

/// How a task ended, as its [`JoinHandle`] reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskExit {
    /// The future returned `Ready`.
    Completed,
    /// The task was aborted.
    Cancelled,
    /// The task panicked.
    Panicked,
}

/// What a run can do next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// At least one task is ready.
    Running,
    /// No task is ready and a timer is pending, so the caller can advance time.
    WaitingOnTime,
    /// No task is ready, no timer is pending, and some task waits on a source
    /// marked external with [`hooks::set_external_wait`].
    WaitingOnExternal,
    /// A task is ready, but every worker is held by a parked task, and no
    /// parked tasks wait on each other in a cycle. More workers would let the
    /// run continue.
    WorkerStarved,
    /// No task can ever run again.
    Deadlock,
    /// Every task finished.
    Quiescent,
}

/// Settings for one run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchedulerConfig {
    /// Simulated runtime workers. A task holds one from the start of a poll
    /// until the poll returns. At least 1. One worker hides every race inside
    /// a single poll, so scenarios should use 2 or more.
    pub workers: u16,
    /// The most tasks one run may spawn. A spawn past it panics in the
    /// spawning task.
    pub max_tasks: u32,
    /// Stack size for each task, in bytes. Untouched pages cost no memory.
    pub stack_size: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            workers: 2,
            max_tasks: 100_000,
            // Tokio's default worker stack, so any poll that fits on a
            // production worker fits here.
            stack_size: 2 * 1024 * 1024,
        }
    }
}

/// What [`Scheduler::shutdown`] found and cleaned up.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// Unfinished tasks cancelled at a poll boundary, with their futures
    /// dropped outside any unwind.
    pub cancelled: usize,
    /// Tasks that panicked while shutdown ran them to a poll boundary or
    /// dropped their futures.
    pub panicked: usize,
    /// Tasks still parked after the drain, such as a lock cycle, whose
    /// coroutines were force-unwound.
    pub unwound: usize,
    /// Forced unwinds that a task's own code caught and didn't rethrow. The
    /// stack of such a task is leaked rather than reused.
    pub unwind_failures: usize,
    /// Timers still registered at shutdown.
    pub timers_left: usize,
}
