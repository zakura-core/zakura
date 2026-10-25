//! The [`Scheduler`] that `zakura-sim` drives, and the run state the hooks
//! reach through a thread-local.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread::{self, ThreadId},
    time::Duration,
};

use corosensei::{stack::DefaultStack, Coroutine, CoroutineResult, Yielder};

use crate::{
    At, CleanupReport, Draw, ObjectId, Op, ReadyTask, RunOutcome, SchedulerConfig, SimInstant,
    StackPool, Status, TaskExit, TaskId, TimerId,
};

pub(crate) type BoxTask = Pin<Box<dyn Future<Output = ()>>>;

/// Answers a run's random draws. See [`Scheduler::set_draws`].
pub(crate) type DrawSource = Box<dyn FnMut(&Draw) -> u64>;

/// What the scheduler asks a coroutine to do when it resumes it.
#[derive(Clone, Copy)]
pub(crate) enum Cmd {
    Poll,
    /// Drop the future without polling it. Only sent at a poll boundary.
    Cancel,
    /// Unwind from where the task is suspended with a [`ForcedUnwind`]
    /// payload, which the coroutine's root catches. Shutdown sends it to a
    /// task still parked after the drain. At a poll boundary it acts as
    /// `Cancel`.
    Unwind,
}

/// The panic payload of a forced unwind. Shutdown drives the unwind itself,
/// rather than through corosensei's `force_unwind`, which resumes a task that
/// suspends mid-unwind instead of returning to the scheduler.
pub(crate) struct ForcedUnwind;

/// Why a coroutine handed control back.
pub(crate) enum Yield {
    /// A preemption point. The task stays ready and keeps its worker.
    Preempt,
    /// The future returned `Pending`. The task releases its worker.
    Pending,
    /// A sync wait. The task keeps its worker.
    Park(ObjectId),
}

type Co = Coroutine<Cmd, Yield, TaskExit, DefaultStack>;
pub(crate) type TaskYielder = Yielder<Cmd, Yield>;

/// Per-task state the hooks need.
pub(crate) struct TaskMeta {
    pub(crate) name: String,
    /// Set by the coroutine when it starts. Valid while the coroutine runs.
    pub(crate) yielder: *const TaskYielder,
    pub(crate) at: At,
    point_counts: BTreeMap<Op, u32>,
    child_counts: BTreeMap<&'static str, u32>,
}

/// One run's state. Never borrowed across a coroutine switch.
pub(crate) struct RunState {
    gen: u64,
    workers: usize,
    max_tasks: u32,
    pub(crate) tasks: Vec<TaskMeta>,
    pub(crate) current: Option<TaskId>,
    /// Tasks woken during their current poll. The poll may span several
    /// resumes, so a wake must survive until the poll returns `Pending`.
    woken: BTreeSet<TaskId>,
    pub(crate) ready: BTreeSet<TaskId>,
    pub(crate) waiting: BTreeSet<TaskId>,
    pub(crate) external: BTreeSet<TaskId>,
    mid_poll: BTreeSet<TaskId>,
    pub(crate) cancel_requested: BTreeSet<TaskId>,
    pub(crate) done: BTreeMap<TaskId, TaskExit>,
    pub(crate) join_wakers: BTreeMap<TaskId, Vec<Waker>>,
    pub(crate) now: SimInstant,
    /// UTC at virtual time zero, or `None` for `zakura-rt`'s default.
    #[cfg(zakura_sim)]
    pub(crate) utc_origin: Option<std::time::SystemTime>,
    pub(crate) timers: BTreeMap<TimerId, Waker>,
    next_timer: u64,
    spawn_queue: Vec<(TaskId, BoxTask)>,
    /// Tasks parked on each object, with the holder each one reported.
    pub(crate) parked: BTreeMap<ObjectId, Vec<TaskId>>,
    pub(crate) holders: BTreeMap<ObjectId, TaskId>,
    object_counts: BTreeMap<Option<TaskId>, u32>,
    root_counts: BTreeMap<&'static str, u32>,
    pub(crate) shutting_down: bool,
    /// Points each task passed during shutdown, where points don't switch.
    pub(crate) cleanup_points: BTreeMap<TaskId, u32>,
    /// Tasks that passed [`CLEANUP_POINT_LIMIT`] points during shutdown. The
    /// drain stops at the first, and shutdown abandons the run.
    pub(crate) stuck: BTreeSet<TaskId>,
    /// The task shutdown is force-unwinding. It's cleared when the task
    /// ends, so one still set afterwards caught the unwind and kept going.
    pub(crate) unwinding: Option<TaskId>,
    /// Tasks shutdown has force-unwound once. One that parks again is
    /// treated as stuck.
    unwind_tried: BTreeSet<TaskId>,
    /// Answers random draws. Taken out while it runs.
    pub(crate) draws: Option<DrawSource>,
    /// Panic payloads of tasks that panicked, until a joiner takes them.
    #[cfg(zakura_sim)]
    pub(crate) panics: BTreeMap<TaskId, Box<dyn std::any::Any + Send>>,
}

impl RunState {
    /// Records that `task` reached its next point of kind `op`.
    pub(crate) fn advance_point(
        &mut self,
        task: TaskId,
        op: Op,
        site: Option<&'static std::panic::Location<'static>>,
    ) -> At {
        let meta = &mut self.tasks[task.index()];
        let n = meta.point_counts.entry(op).or_insert(0);
        *n = n.saturating_add(1);
        meta.at = At { op, n: *n, site };
        meta.at
    }

    /// Queues a task. It becomes ready once the current step returns.
    ///
    /// Past the task limit, hands the future back unqueued. The caller must
    /// drop it after releasing the run-state borrow, because its destructor
    /// may wake other tasks.
    pub(crate) fn queue_spawn(
        &mut self,
        label: &'static str,
        task: BoxTask,
    ) -> Result<TaskId, BoxTask> {
        let len = u32::try_from(self.tasks.len()).unwrap_or(u32::MAX);
        if len >= self.max_tasks {
            return Err(task);
        }
        let id = TaskId(len);
        let parent = self.current;
        let ordinal = match parent {
            Some(p) => {
                let n = self.tasks[p.index()].child_counts.entry(label).or_insert(0);
                *n += 1;
                *n
            }
            None => {
                let n = self.root_counts.entry(label).or_insert(0);
                *n += 1;
                *n
            }
        };
        let name = match parent {
            Some(p) => format!("{}/{label}.{ordinal}", self.tasks[p.index()].name),
            None => format!("{label}.{ordinal}"),
        };
        self.tasks.push(TaskMeta {
            name,
            yielder: std::ptr::null(),
            at: At {
                op: Op::Poll,
                n: 1,
                site: None,
            },
            point_counts: BTreeMap::from([(Op::Poll, 1)]),
            child_counts: BTreeMap::new(),
        });
        self.spawn_queue.push((id, task));
        if parent.is_none() {
            // Setup code between steps. `run` admits it before its next step.
            self.ready.insert(id);
        }
        Ok(id)
    }

    /// Panics for a spawn past the task limit, after dropping the rejected
    /// future outside the run-state borrow.
    pub(crate) fn reject_spawn(rejected: BoxTask) -> ! {
        let limit = with_run(|s| s.max_tasks).unwrap_or(0);
        drop(rejected);
        panic!("run spawned more than max_tasks ({limit}) tasks");
    }

    /// Takes everything left in the run whose destructor can run arbitrary
    /// code: queued tasks, timer and join wakers, and unjoined panic payloads.
    fn take_leftovers(&mut self) -> Vec<Box<dyn std::any::Any>> {
        let mut left: Vec<Box<dyn std::any::Any>> = Vec::new();
        left.extend(
            std::mem::take(&mut self.spawn_queue)
                .into_iter()
                .map(|(_, task)| Box::new(task) as _),
        );
        left.extend(
            std::mem::take(&mut self.timers)
                .into_values()
                .map(|waker| Box::new(waker) as _),
        );
        left.extend(
            std::mem::take(&mut self.join_wakers)
                .into_values()
                .map(|wakers| Box::new(wakers) as _),
        );
        #[cfg(zakura_sim)]
        left.extend(
            std::mem::take(&mut self.panics)
                .into_values()
                .map(|payload| Box::new(payload) as _),
        );
        left
    }

    /// Allocates the next object identity for the current task.
    pub(crate) fn new_object(&mut self) -> ObjectId {
        let creator = self.current;
        let n = self.object_counts.entry(creator).or_insert(0);
        *n += 1;
        ObjectId {
            creator,
            ordinal: *n,
        }
    }

    /// Registers `waker` to fire at `deadline`.
    pub(crate) fn register_timer(&mut self, deadline: SimInstant, waker: Waker) -> TimerId {
        self.next_timer += 1;
        let id = TimerId {
            deadline,
            seq: self.next_timer,
        };
        self.timers.insert(id, waker);
        id
    }

    /// Tasks `run` accepts now. A task mid-poll already holds a worker. Any
    /// other ready task needs a free one.
    fn eligible(&self) -> impl Iterator<Item = TaskId> + '_ {
        let free = self.mid_poll.len() < self.workers;
        self.ready
            .iter()
            .copied()
            .filter(move |t| free || self.mid_poll.contains(t))
    }

    /// True if some parked tasks wait on each other in a cycle, so no number
    /// of extra workers would let them move. Other parked tasks don't matter.
    fn lock_cycle(&self) -> bool {
        // Each parked task waits on the holder of the object it parked on.
        let waits_on: BTreeMap<TaskId, TaskId> = self
            .parked
            .iter()
            .filter_map(|(obj, tasks)| Some((tasks, *self.holders.get(obj)?)))
            .flat_map(|(tasks, holder)| tasks.iter().map(move |t| (*t, holder)))
            .collect();
        waits_on.keys().any(|&start| {
            let mut seen = BTreeSet::new();
            let mut task = start;
            while let Some(&holder) = waits_on.get(&task) {
                if !seen.insert(task) {
                    return true;
                }
                task = holder;
            }
            false
        })
    }
}

thread_local! {
    static RUN: RefCell<Option<RunState>> = const { RefCell::new(None) };
    static NEXT_GEN: Cell<u64> = const { Cell::new(0) };
    static STACK_POOL: RefCell<Vec<DefaultStack>> = const { RefCell::new(Vec::new()) };
    static POOLED_SIZE: Cell<usize> = const { Cell::new(0) };
    static STACKS_ALLOCATED: Cell<u64> = const { Cell::new(0) };
    static STACKS_FREED: Cell<u64> = const { Cell::new(0) };
}

/// The most idle stacks one thread keeps. Pooled stacks keep the pages a run
/// touched, so the pool's memory follows real stack depth.
const STACK_POOL_LIMIT: usize = 1024;

/// The most steps one shutdown drain takes. Tasks that keep waking each other
/// past it are force-unwound instead, so cleanup always ends.
const DRAIN_STEP_LIMIT: usize = 100_000;

/// The most preemption points one task passes during shutdown. Points don't
/// switch then, so a task spinning on a value no one will set never reaches
/// a poll boundary. Past this it stops, and shutdown abandons the run. The limit is
/// high so long but finite cleanup still finishes, and a cleanup point costs
/// only a counter, so a stuck task costs well under a second.
pub(crate) const CLEANUP_POINT_LIMIT: u32 = 10_000_000;

/// Takes this thread's run state when dropped, even if shutdown panics, so
/// later runs on the thread can still start. During a panic it leaks what the
/// state still holds, since dropping that could panic again and abort.
struct ClearRun;

impl Drop for ClearRun {
    fn drop(&mut self) {
        let state = RUN.with(|r| r.borrow_mut().take());
        if std::thread::panicking() {
            std::mem::forget(state);
        } else {
            drop(state);
        }
    }
}

/// Runs `f` on this thread's run state, or returns `None` outside a run.
pub(crate) fn with_run<R>(f: impl FnOnce(&mut RunState) -> R) -> Option<R> {
    RUN.with(|r| r.borrow_mut().as_mut().map(f))
}

/// Takes a stack of `size` bytes from this thread's pool, or allocates one.
fn take_stack(size: usize) -> DefaultStack {
    if POOLED_SIZE.get() != size {
        let dropped = STACK_POOL.with(|p| std::mem::take(&mut *p.borrow_mut()));
        count_freed(dropped.len());
        POOLED_SIZE.set(size);
    }
    STACK_POOL
        .with(|p| p.borrow_mut().pop())
        .unwrap_or_else(|| {
            STACKS_ALLOCATED.set(STACKS_ALLOCATED.get().saturating_add(1));
            DefaultStack::new(size)
                .expect("stack allocation fails only when the process is out of address space")
        })
}

/// This thread's stack pool: idle stacks, and how many it ever allocated
/// and freed.
pub fn stack_pool() -> StackPool {
    StackPool {
        idle: STACK_POOL.with(|p| p.borrow().len()),
        allocated: STACKS_ALLOCATED.get(),
        freed: STACKS_FREED.get(),
    }
}

/// Returns a stack to this thread's pool, or frees it once the pool is full.
fn return_stack(stack: DefaultStack) {
    let full = STACK_POOL.with(|p| {
        let mut p = p.borrow_mut();
        if p.len() < STACK_POOL_LIMIT {
            p.push(stack);
            None
        } else {
            Some(stack)
        }
    });
    if let Some(stack) = full {
        drop(stack);
        count_freed(1);
    }
}

/// Adds `n` stacks to this thread's freed count.
fn count_freed(n: usize) {
    let n = u64::try_from(n).expect("a stack count fits in u64");
    STACKS_FREED.set(STACKS_FREED.get().saturating_add(n));
}

/// Wakes a task. Wakers from an earlier run carry an old generation and do
/// nothing.
struct TaskWaker {
    task: TaskId,
    gen: u64,
    thread: ThreadId,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // A run lives on one thread, so a wake from another thread means some
        // code escaped the simulation, and replay would no longer hold.
        assert_eq!(
            thread::current().id(),
            self.thread,
            "a simulated task's waker fired from another OS thread"
        );
        with_run(|s| {
            if s.gen != self.gen {
                return;
            }
            if s.waiting.remove(&self.task) {
                s.ready.insert(self.task);
            } else if s.mid_poll.contains(&self.task) {
                // Running, or suspended mid-poll at a point or a park.
                s.woken.insert(self.task);
            }
        });
    }
}

/// A deterministic scheduler for one run.
///
/// Only one `Scheduler` can exist per thread at a time, because the hooks
/// find it through a thread-local. Dropping it shuts the run down.
pub struct Scheduler {
    coroutines: Vec<Option<Co>>,
    stack_size: usize,
    shut_down: bool,
    /// Keeps this scheduler installed as `zakura-rt`'s executor until the
    /// run's state is gone.
    #[cfg(zakura_sim)]
    rt_run: Option<zakura_rt::sim::RunGuard>,
}

impl Scheduler {
    /// Starts a run on this thread with virtual time at `start`.
    ///
    /// # Panics
    ///
    /// If this thread already has a run, or `config.workers` is 0. In the
    /// simulator build, also if UTC at `start` doesn't fit in a `SystemTime`
    /// (see `zakura_rt::sim::Executor::utc_origin`).
    pub fn new(config: SchedulerConfig, start: SimInstant) -> Self {
        assert!(config.workers > 0, "a run needs at least one worker");
        #[cfg(zakura_sim)]
        crate::executor::assert_utc_in_range(config.utc_origin, start);
        let gen = NEXT_GEN.get() + 1;
        NEXT_GEN.set(gen);
        RUN.with(|r| {
            assert!(
                r.borrow().is_none(),
                "this thread already has a running Scheduler"
            );
        });
        // Enter before installing the state, so a panic here leaves no run
        // state behind to block later runs on this thread.
        #[cfg(zakura_sim)]
        let rt_run = zakura_rt::sim::enter(&crate::executor::EXECUTOR);
        RUN.with(|r| {
            *r.borrow_mut() = Some(RunState {
                gen,
                workers: usize::from(config.workers),
                max_tasks: config.max_tasks,
                tasks: Vec::new(),
                current: None,
                woken: BTreeSet::new(),
                ready: BTreeSet::new(),
                waiting: BTreeSet::new(),
                external: BTreeSet::new(),
                mid_poll: BTreeSet::new(),
                cancel_requested: BTreeSet::new(),
                done: BTreeMap::new(),
                join_wakers: BTreeMap::new(),
                now: start,
                #[cfg(zakura_sim)]
                utc_origin: config.utc_origin,
                timers: BTreeMap::new(),
                next_timer: 0,
                spawn_queue: Vec::new(),
                parked: BTreeMap::new(),
                holders: BTreeMap::new(),
                object_counts: BTreeMap::new(),
                root_counts: BTreeMap::new(),
                shutting_down: false,
                cleanup_points: BTreeMap::new(),
                stuck: BTreeSet::new(),
                unwinding: None,
                unwind_tried: BTreeSet::new(),
                draws: None,
                #[cfg(zakura_sim)]
                panics: BTreeMap::new(),
            });
        });
        Scheduler {
            coroutines: Vec::new(),
            stack_size: config.stack_size,
            shut_down: false,
            #[cfg(zakura_sim)]
            rt_run: Some(rt_run),
        }
    }

    /// Starts a task outside any parent task, such as a reactor's run loop.
    /// It is ready immediately.
    pub fn spawn_root(
        &mut self,
        label: &'static str,
        task: impl Future<Output = ()> + 'static,
    ) -> crate::JoinHandle {
        let queued = self.state(|s| {
            assert!(
                s.current.is_none(),
                "spawn_root is for setup code. Tasks spawn through hooks::spawn"
            );
            s.queue_spawn(label, Box::pin(task))
        });
        let id = queued.unwrap_or_else(|rejected| RunState::reject_spawn(rejected));
        self.admit();
        crate::JoinHandle::new(id)
    }

    /// Sets what answers the run's random draws: the `rng()` stream seed, unbiased
    /// `select!` starts, and the hash seed. `zakura-sim` passes its `Plan`.
    /// Without a source every draw is 0. The `Plan` answers exhausted
    /// `select!` and `StreamMap` draws with a rotating default instead, so
    /// they don't all start at branch 0.
    pub fn set_draws(&mut self, draws: impl FnMut(&Draw) -> u64 + 'static) {
        let old = self.state(|s| s.draws.replace(Box::new(draws)));
        // Dropped outside the borrow, since a closure's drop can run any code.
        drop(old);
    }

    /// The task's stable identity: its spawn path, such as
    /// `reactor.1/peer.2`, built from labels and per-parent spawn ordinals.
    ///
    /// The ordinal counts earlier spawns with the same label from the same
    /// parent, starting at 1. Root tasks share one count per label. So a new
    /// task elsewhere in the tree, or one with another label, renames
    /// nothing. Only an earlier same-label spawn from the same parent shifts
    /// later ordinals.
    pub fn task_name(&self, task: TaskId) -> String {
        self.state(|s| s.tasks[task.index()].name.clone())
    }

    /// Tasks that [`run`](Self::run) accepts now, sorted by [`TaskId`].
    pub fn ready(&self) -> Vec<ReadyTask> {
        self.state(|s| {
            s.eligible()
                .map(|task| ReadyTask {
                    task,
                    at: s.tasks[task.index()].at,
                    mid_poll: s.mid_poll.contains(&task),
                })
                .collect()
        })
    }

    /// Resumes `task` and runs it to its next preemption point, its next
    /// `Pending`, a park, or its end.
    ///
    /// # Panics
    ///
    /// If `task` isn't in [`ready`](Self::ready).
    pub fn run(&mut self, task: TaskId) -> RunOutcome {
        // Setup code may have spawned through the hooks since the last step.
        self.admit();
        self.state(|s| {
            assert!(
                s.eligible().any(|t| t == task),
                "run({task:?}) on a task that isn't ready"
            );
        });
        self.step(task)
    }

    /// Resumes a ready `task`, ignoring the worker limit. Shared by
    /// [`run`](Self::run) and the shutdown drain.
    fn step(&mut self, task: TaskId) -> RunOutcome {
        self.resume(task, None)
    }

    /// [`step`](Self::step), or with `forced` sent instead of the usual
    /// command, as shutdown's forced unwind does.
    fn resume(&mut self, task: TaskId, forced: Option<Cmd>) -> RunOutcome {
        let cmd = self.state(|s| {
            s.current = Some(task);
            s.ready.remove(&task);
            let starts_poll = s.mid_poll.insert(task);
            if let Some(cmd) = forced {
                cmd
            } else if starts_poll && s.cancel_requested.contains(&task) {
                Cmd::Cancel
            } else {
                Cmd::Poll
            }
        });
        let co = self.coroutines[task.index()]
            .as_mut()
            .expect("a ready task's coroutine is live until it finishes");
        let resumed = catch_unwind(AssertUnwindSafe(|| co.resume(cmd)));
        self.state(|s| s.current = None);

        let (outcome, exit) = match resumed {
            Ok(CoroutineResult::Yield(Yield::Preempt)) => {
                let at = self.state(|s| {
                    s.ready.insert(task);
                    s.tasks[task.index()].at
                });
                (RunOutcome::Reached(at), None)
            }
            Ok(CoroutineResult::Yield(Yield::Pending)) => {
                self.state(|s| {
                    s.mid_poll.remove(&task);
                    s.advance_point(task, Op::Poll, None);
                    // A wake during the poll, or an abort, makes it ready again.
                    if s.woken.remove(&task) || s.cancel_requested.contains(&task) {
                        s.ready.insert(task);
                    } else {
                        s.waiting.insert(task);
                    }
                });
                (RunOutcome::Waiting, None)
            }
            Ok(CoroutineResult::Yield(Yield::Park(obj))) => (RunOutcome::Parked(obj), None),
            Ok(CoroutineResult::Return(TaskExit::Completed)) => {
                (RunOutcome::Completed, Some(TaskExit::Completed))
            }
            Ok(CoroutineResult::Return(TaskExit::Cancelled)) => {
                (RunOutcome::Cancelled, Some(TaskExit::Cancelled))
            }
            Ok(CoroutineResult::Return(TaskExit::Panicked)) => {
                unreachable!("the coroutine body never returns Panicked")
            }
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                #[cfg(zakura_sim)]
                self.state(|s| s.panics.insert(task, payload));
                (RunOutcome::Panicked(msg), Some(TaskExit::Panicked))
            }
        };

        if let Some(exit) = exit {
            let co = self.coroutines[task.index()]
                .take()
                .expect("the coroutine was live when it was resumed");
            // A task that caught a forced unwind but still ended is unwound
            // as cleanly as one the unwind reached.
            self.state(|s| s.unwinding = s.unwinding.filter(|t| *t != task));
            return_stack(co.into_stack());
            self.finish(task, exit);
        }
        self.admit();
        outcome
    }

    /// Records that `task` ended, and wakes its joiners.
    fn finish(&mut self, task: TaskId, exit: TaskExit) {
        let joiners = self.state(|s| {
            s.ready.remove(&task);
            s.waiting.remove(&task);
            s.mid_poll.remove(&task);
            s.woken.remove(&task);
            s.cancel_requested.remove(&task);
            s.external.remove(&task);
            s.parked.values_mut().for_each(|p| p.retain(|t| *t != task));
            s.done.insert(task, exit);
            s.join_wakers.remove(&task).unwrap_or_default()
        });
        joiners.into_iter().for_each(Waker::wake);
    }

    /// The current virtual time.
    pub fn now(&self) -> SimInstant {
        self.state(|s| s.now)
    }

    /// The earliest pending timer deadline.
    pub fn next_deadline(&self) -> Option<SimInstant> {
        self.state(|s| s.timers.keys().next().map(|t| t.deadline))
    }

    /// Moves virtual time to `t` and fires every timer due by then. Returns
    /// the fired timers in deadline order.
    ///
    /// Calling this only when no task is ready keeps timeouts honest (see
    /// `scheduler.md`, Virtual time). The scheduler leaves that policy to the
    /// caller, so a scenario can model slow tasks on purpose.
    ///
    /// # Panics
    ///
    /// If `t` is earlier than the current time. In the simulator build, also
    /// if UTC at `t` doesn't fit in a `SystemTime`, so a task's `utc_now`
    /// can't overflow.
    pub fn advance_to(&mut self, t: SimInstant) -> Vec<TimerId> {
        let fired = self.state(|s| {
            assert!(t >= s.now, "virtual time can't move backwards");
            #[cfg(zakura_sim)]
            crate::executor::assert_utc_in_range(s.utc_origin, t);
            s.now = t;
            let later = s.timers.split_off(&TimerId {
                deadline: t.checked_add(Duration::from_nanos(1)).unwrap_or(t),
                seq: 0,
            });
            std::mem::replace(&mut s.timers, later)
        });
        let ids = fired.keys().copied().collect();
        fired.into_values().for_each(Waker::wake);
        ids
    }

    /// What the run can do next.
    pub fn status(&self) -> Status {
        self.state(|s| {
            let live = s.tasks.len() - s.done.len();
            if live == 0 {
                Status::Quiescent
            } else if s.eligible().next().is_some() {
                Status::Running
            } else if !s.ready.is_empty() {
                if s.lock_cycle() {
                    Status::Deadlock
                } else {
                    Status::WorkerStarved
                }
            } else if !s.timers.is_empty() {
                Status::WaitingOnTime
            } else if !s.external.is_empty() {
                Status::WaitingOnExternal
            } else {
                Status::Deadlock
            }
        })
    }

    /// Ends the run, in three stages:
    ///
    /// 1. Preemption points become no-ops, and every task stopped mid-poll
    ///    runs on to its poll boundary. `park` still waits, so a destructor
    ///    can take a contended lock.
    /// 2. Every task at a poll boundary is cancelled, as an abort would. Its
    ///    future is dropped on its own stack, outside any unwind.
    /// 3. Only a task that is still parked, as in a lock cycle, is
    ///    force-unwound, and whatever that releases drains as in stage 1. If
    ///    a task passed [`CLEANUP_POINT_LIMIT`] points in stage 1, shutdown
    ///    instead abandons the run without running any more code (see
    ///    `abandon`).
    ///
    /// Check properties before calling this. A force-unwound task inside a
    /// production `catch_unwind` sees the unwind as a panic.
    pub fn shutdown(mut self) -> CleanupReport {
        self.shutdown_inner()
    }

    /// Shared by [`shutdown`](Self::shutdown) and `Drop`.
    fn shutdown_inner(&mut self) -> CleanupReport {
        if self.shut_down {
            return CleanupReport::default();
        }
        self.shut_down = true;
        let clear_run = ClearRun;
        let timers_left = self.state(|s| {
            s.shutting_down = true;
            let live = (0..s.tasks.len())
                .map(|i| TaskId(u32::try_from(i).expect("queue_spawn caps tasks at u32")))
                .filter(|t| !s.done.contains_key(t));
            s.cancel_requested.extend(live);
            let waiting = std::mem::take(&mut s.waiting);
            s.ready.extend(waiting);
            s.timers.len()
        });
        let mut report = CleanupReport {
            timers_left,
            ..CleanupReport::default()
        };
        let mut abandoned = false;
        loop {
            self.drain(&mut report);
            if self.state(|s| !s.stuck.is_empty()) {
                report.leaked = self.abandon();
                abandoned = true;
                break;
            }
            // Every task left is parked, or the drain hit its step limit.
            let Some(task) = self
                .coroutines
                .iter()
                .position(Option::is_some)
                .map(|i| TaskId(u32::try_from(i).expect("queue_spawn caps tasks at u32")))
            else {
                break;
            };
            // A task that caught an earlier forced unwind and parked again
            // may never let go, so it counts as stuck.
            if !self.state(|s| s.unwind_tried.insert(task)) {
                self.state(|s| s.stuck.insert(task));
                continue;
            }
            if self.coroutines[task.index()]
                .as_ref()
                .is_some_and(|co| co.started() && !co.done())
            {
                report.unwound += 1;
            }
            self.state(|s| {
                s.unwinding = Some(task);
                s.parked.values_mut().for_each(|p| p.retain(|t| *t != task));
            });
            // Through `resume`, so whatever the task does next, finishing,
            // reaching a point, parking or getting stuck, comes back here.
            self.resume(task, Some(Cmd::Unwind));
            if self.state(|s| s.unwinding.take() == Some(task)) {
                // The task caught the unwind and kept going. It's live
                // again, and the next drain runs it as any other task.
                report.unwind_failures += 1;
            }
        }
        // Drop what's left outside the borrow, while the run and the
        // `zakura-rt` executor are both still installed, since destructors
        // may call hooks or queue more. Then nothing in the state runs code.
        if !abandoned {
            loop {
                let left = self.state(RunState::take_leftovers);
                if left.is_empty() {
                    break;
                }
                drop(left);
            }
        }
        drop(clear_run);
        // After the state, so destructors above still found `zakura-rt`'s
        // executor. On a panic, dropping the `Scheduler` drops this instead.
        #[cfg(zakura_sim)]
        drop(self.rt_run.take());
        report
    }

    /// Abandons the run once a task is stuck in cleanup, and returns how many
    /// tasks it leaked: every task still live, plus any spawn still queued.
    ///
    /// A stuck task's own destructors never run, and any other code may wait
    /// on what they would have done, such as a flag or an unlock. So from
    /// here no more user code runs at all: no unwind, drop, wake, or spawn.
    /// Every coroutine and the whole run state, with its queued spawns,
    /// timers, join wakers and panic payloads, are forgotten as they are,
    /// as [`ClearRun`] does during a panic. Their stacks count as leaked in
    /// [`StackPool`](crate::StackPool).
    ///
    /// After an abandoned run, the caller must not wait on anything it shares
    /// with the run's tasks, by dropping it or otherwise, since a leaked task
    /// never releases it. [`CleanupReport::leaked`](crate::CleanupReport) is
    /// nonzero exactly when a run was abandoned.
    fn abandon(&mut self) -> usize {
        let coroutines = std::mem::take(&mut self.coroutines);
        let live = coroutines.iter().filter(|co| co.is_some()).count();
        std::mem::forget(coroutines);
        let state = RUN.with(|r| r.borrow_mut().take());
        let queued = state.as_ref().map_or(0, |s| s.spawn_queue.len());
        std::mem::forget(state);
        live + queued
    }

    /// Runs ready tasks during shutdown until none is left: first any task
    /// stopped mid-poll, then each task at a poll boundary, which `step`
    /// cancels. Picks the lowest `TaskId`, so cleanup replays too. Stops as
    /// soon as a task is stuck, since shutdown then abandons the run.
    fn drain(&mut self, report: &mut CleanupReport) {
        for _ in 0..DRAIN_STEP_LIMIT {
            if self.state(|s| !s.stuck.is_empty()) {
                return;
            }
            // Setup code may have spawned since the last step.
            self.admit();
            let next = self.state(|s| {
                s.ready
                    .iter()
                    .find(|t| s.mid_poll.contains(t))
                    .or_else(|| s.ready.first())
                    .copied()
            });
            let Some(task) = next else { return };
            match self.step(task) {
                RunOutcome::Cancelled => report.cancelled += 1,
                RunOutcome::Panicked(_) => report.panicked += 1,
                _ => {}
            }
        }
    }

    /// Creates coroutines for tasks spawned since the last step, in id order.
    fn admit(&mut self) {
        let (queued, gen) = self.state(|s| (std::mem::take(&mut s.spawn_queue), s.gen));
        for (id, mut fut) in queued {
            debug_assert_eq!(id.index(), self.coroutines.len());
            let waker = Waker::from(Arc::new(TaskWaker {
                task: id,
                gen,
                thread: thread::current().id(),
            }));
            let co = Co::with_stack(
                take_stack(self.stack_size),
                move |yielder: &TaskYielder, mut cmd: Cmd| {
                    with_run(|s| s.tasks[id.index()].yielder = yielder as *const _);
                    let mut cx = Context::from_waker(&waker);
                    // Cancels the task by dropping its future. A destructor
                    // that parks can be force-unwound in turn, and that ends
                    // here too, as a cancel rather than a panic. Dropping
                    // after a forced unwind is outside any unwind, so the
                    // destructors still count points and can be stopped.
                    let cancel = |fut| {
                        match catch_unwind(AssertUnwindSafe(move || drop(fut))) {
                            Err(payload) if !payload.is::<ForcedUnwind>() => {
                                std::panic::resume_unwind(payload)
                            }
                            _ => {}
                        }
                        with_run(|s| s.unwinding = s.unwinding.filter(|t| *t != id));
                        TaskExit::Cancelled
                    };
                    loop {
                        match cmd {
                            Cmd::Poll => {}
                            Cmd::Cancel | Cmd::Unwind => return cancel(fut),
                        }
                        match catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(&mut cx))) {
                            Ok(Poll::Ready(())) => return TaskExit::Completed,
                            Ok(Poll::Pending) => cmd = yielder.suspend(Yield::Pending),
                            Err(payload) if payload.is::<ForcedUnwind>() => return cancel(fut),
                            Err(payload) => std::panic::resume_unwind(payload),
                        }
                    }
                },
            );
            self.coroutines.push(Some(co));
            self.state(|s| {
                s.ready.insert(id);
                // A task spawned during shutdown is dropped unpolled, as
                // tokio does.
                if s.shutting_down {
                    s.cancel_requested.insert(id);
                }
            });
        }
    }

    /// Runs `f` on this scheduler's run state.
    fn state<R>(&self, f: impl FnOnce(&mut RunState) -> R) -> R {
        with_run(f).expect("a Scheduler's run state lives until it shuts down")
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        // A shutdown that panics, here or in `shutdown`, can leave coroutines
        // suspended. Dropping one would force-unwind it through corosensei,
        // which can run its destructors past any bound, so forget them
        // instead, whether shutdown returns or unwinds.
        let shut_down = catch_unwind(AssertUnwindSafe(|| self.shutdown_inner()));
        std::mem::take(&mut self.coroutines)
            .into_iter()
            .flatten()
            .for_each(std::mem::forget);
        if let Err(payload) = shut_down {
            std::panic::resume_unwind(payload);
        }
    }
}
