//! Tasks, simulator build.
//!
//! Inside a run, [`spawn`] hands the task to the run's executor and the
//! handles here talk to it. Outside a run they wrap tokio's, so ordinary
//! `#[tokio::test]`s keep working. Whether a handle is simulated is fixed when
//! its task is spawned.

use std::{
    any::Any,
    fmt,
    future::Future,
    panic::Location,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
};

pub use tokio::task::coop;

use crate::sim::{self, Op, Point, RunId, TaskExit, TaskId};

/// Spawns `future` as a new task, as `tokio::spawn` does.
///
/// Inside a run, the executor runs it, and the spawn is a preemption point.
/// The task's label is the caller's source file, which changes when code
/// moves. Use [`spawn_named`] for a stable one.
#[track_caller]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_named(Location::caller().file(), future)
}

/// Spawns `future` as [`spawn`] does, with `label` naming the task.
///
/// The executor builds task identities from labels, such as
/// `header_sync_publisher.1/header_sync_page_job.2`, so a label that doesn't
/// depend on where the code lives keeps traces and replays stable across
/// refactors.
#[track_caller]
pub fn spawn_named<F>(label: &'static str, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (Some(executor), Some(run)) = (sim::executor(), sim::run_id()) else {
        return JoinHandle::tokio(tokio::spawn(future));
    };
    spawn_in_run(
        executor,
        run,
        label,
        Location::caller(),
        future,
        |label, task| Some(executor.spawn(label, task)),
    )
    .expect("the infallible executor spawn always returns a task")
}

/// Spawns `future` as [`spawn`] does, or returns `None` instead of
/// panicking when there's nowhere to run it.
///
/// Outside a run, that's when there's no tokio runtime. Inside a run, it's
/// when the executor can't take another task. For code that may run without
/// a runtime, such as a `Drop` impl.
#[track_caller]
pub fn try_spawn<F>(future: F) -> Option<JoinHandle<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (Some(executor), Some(run)) = (sim::executor(), sim::run_id()) else {
        return tokio::runtime::Handle::try_current()
            .ok()
            .map(|runtime| JoinHandle::tokio(runtime.spawn(future)));
    };
    let site = Location::caller();
    spawn_in_run(executor, run, site.file(), site, future, |label, task| {
        executor.try_spawn(label, task)
    })
}

/// Hands `future` to the run's executor through `spawn_task` under `label`,
/// after the spawn's preemption point at `site`. Returns `None` if
/// `spawn_task` refuses it.
fn spawn_in_run<F>(
    executor: &'static dyn sim::Executor,
    run: RunId,
    label: &'static str,
    site: &'static Location<'static>,
    future: F,
    spawn_task: impl FnOnce(&'static str, sim::BoxTask) -> Option<TaskId>,
) -> Option<JoinHandle<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    sim::preempt(executor, Point::at(Op::Spawn, site));
    let output = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&output);
    let task = spawn_task(
        label,
        Box::pin(async move {
            let value = future.await;
            *slot.lock().unwrap_or_else(PoisonError::into_inner) = Some(value);
        }),
    )?;
    Some(JoinHandle {
        inner: Inner::Sim { run, task, output },
    })
}

/// Runs `f` on tokio's blocking pool, as `tokio::task::spawn_blocking` does.
///
/// Inside a run, `f` runs as an ordinary task labeled `blocking`, in one
/// poll, on the run's one thread. Unlike tokio's blocking pool, it holds a
/// simulated worker while it runs.
#[track_caller]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    if sim::executor().is_none() {
        return JoinHandle::tokio(tokio::task::spawn_blocking(f));
    }
    spawn_named("blocking", async move { f() })
}

/// Lets other tasks run, as `tokio::task::yield_now` does.
///
/// Inside a run, the first poll wakes the task and returns `Pending`, so the
/// task frees its worker and stays ready. The next poll returns `Ready`.
pub fn yield_now() -> YieldNow {
    YieldNow {
        yielded: false,
        tokio: None,
    }
}

/// The future [`yield_now`] returns.
#[must_use = "futures do nothing unless polled"]
pub struct YieldNow {
    yielded: bool,
    tokio: Option<Pin<Box<dyn Future<Output = ()> + Send + Sync>>>,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.tokio.is_none() && sim::executor().is_some() {
            if self.yielded {
                return Poll::Ready(());
            }
            self.yielded = true;
            // The executor holds a wake that lands mid-poll until the poll
            // returns, the way tokio defers this wake.
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.tokio
            .get_or_insert_with(|| Box::pin(tokio::task::yield_now()))
            .as_mut()
            .poll(cx)
    }
}

impl fmt::Debug for YieldNow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("YieldNow").finish_non_exhaustive()
    }
}

/// An owned permission to join a task, as tokio's `JoinHandle`.
///
/// Dropping it detaches the task.
pub struct JoinHandle<T> {
    inner: Inner<T>,
}

enum Inner<T> {
    Tokio(tokio::task::JoinHandle<T>),
    Sim {
        run: RunId,
        task: TaskId,
        /// Filled by the task's wrapper future when it completes.
        output: Arc<Mutex<Option<T>>>,
    },
}

impl<T> JoinHandle<T> {
    /// Wraps a tokio handle.
    fn tokio(handle: tokio::task::JoinHandle<T>) -> Self {
        JoinHandle {
            inner: Inner::Tokio(handle),
        }
    }

    /// Requests cancellation, as tokio's abort does.
    #[track_caller]
    pub fn abort(&self) {
        self.abort_handle().abort_at(Location::caller());
    }

    /// True once the task has ended.
    #[track_caller]
    pub fn is_finished(&self) -> bool {
        self.abort_handle().is_finished_at(Location::caller())
    }

    /// A handle that can abort the task without joining it.
    pub fn abort_handle(&self) -> AbortHandle {
        match &self.inner {
            Inner::Tokio(handle) => AbortHandle {
                inner: AbortInner::Tokio(handle.abort_handle()),
            },
            Inner::Sim { run, task, .. } => AbortHandle {
                inner: AbortInner::Sim(*run, *task),
            },
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().inner {
            Inner::Tokio(handle) => Pin::new(handle)
                .poll(cx)
                .map_err(|error| JoinError(JoinErrorRepr::Tokio(error))),
            Inner::Sim { run, task, output } => {
                let executor = sim::executor_for(*run)
                    .expect("a sim JoinHandle is only polled inside the run that spawned it");
                executor.poll_join(*task, cx).map(|exit| match exit {
                    TaskExit::Completed => Ok(output
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take()
                        .expect(
                            "a completed task stored its output, and only this handle takes it",
                        )),
                    TaskExit::Cancelled => Err(JoinError(JoinErrorRepr::Cancelled)),
                    TaskExit::Panicked(payload) => {
                        Err(JoinError(JoinErrorRepr::Panic(Mutex::new(payload))))
                    }
                })
            }
        }
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Inner::Tokio(handle) => f.debug_tuple("JoinHandle").field(&handle.id()).finish(),
            Inner::Sim { task, .. } => f.debug_tuple("JoinHandle").field(task).finish(),
        }
    }
}

/// A handle that can abort a task, as tokio's `AbortHandle`.
#[derive(Clone)]
pub struct AbortHandle {
    inner: AbortInner,
}

#[derive(Clone)]
enum AbortInner {
    Tokio(tokio::task::AbortHandle),
    Sim(RunId, TaskId),
}

impl AbortHandle {
    /// Requests cancellation, as tokio's abort does. Inside a run this is a
    /// preemption point.
    #[track_caller]
    pub fn abort(&self) {
        self.abort_at(Location::caller());
    }

    /// True once the task has ended. Inside a run this is a preemption point.
    #[track_caller]
    pub fn is_finished(&self) -> bool {
        self.is_finished_at(Location::caller())
    }

    /// [`AbortHandle::abort`], reporting `site` as the call site.
    fn abort_at(&self, site: &'static Location<'static>) {
        match &self.inner {
            AbortInner::Tokio(handle) => handle.abort(),
            AbortInner::Sim(run, task) => {
                // After the run ends there's nothing left to abort.
                if let Some(executor) = sim::executor_for(*run) {
                    sim::preempt(executor, Point::at(Op::Abort, site));
                    executor.abort(*task);
                }
            }
        }
    }

    /// [`AbortHandle::is_finished`], reporting `site` as the call site.
    fn is_finished_at(&self, site: &'static Location<'static>) -> bool {
        match &self.inner {
            AbortInner::Tokio(handle) => handle.is_finished(),
            AbortInner::Sim(run, task) => sim::executor_for(*run).is_none_or(|executor| {
                sim::preempt(executor, Point::at(Op::Join, site));
                executor.is_finished(*task)
            }),
        }
    }
}

impl fmt::Debug for AbortHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            AbortInner::Tokio(handle) => handle.fmt(f),
            AbortInner::Sim(_, task) => f.debug_tuple("AbortHandle").field(task).finish(),
        }
    }
}

/// A task that failed to complete, as tokio's `JoinError`.
pub struct JoinError(JoinErrorRepr);

enum JoinErrorRepr {
    Tokio(tokio::task::JoinError),
    Cancelled,
    // The mutex makes the payload `Sync`, as tokio's `JoinError` is.
    Panic(Mutex<Box<dyn Any + Send + 'static>>),
}

impl JoinError {
    /// True if the task was aborted.
    pub fn is_cancelled(&self) -> bool {
        match &self.0 {
            JoinErrorRepr::Tokio(error) => error.is_cancelled(),
            JoinErrorRepr::Cancelled => true,
            JoinErrorRepr::Panic(_) => false,
        }
    }

    /// True if the task panicked.
    pub fn is_panic(&self) -> bool {
        match &self.0 {
            JoinErrorRepr::Tokio(error) => error.is_panic(),
            JoinErrorRepr::Cancelled => false,
            JoinErrorRepr::Panic(_) => true,
        }
    }

    /// The panic payload.
    ///
    /// # Panics
    ///
    /// If the task didn't panic.
    #[track_caller]
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        self.try_into_panic()
            .expect("`JoinError` reason is not a panic.")
    }

    /// The panic payload, or the error itself if the task didn't panic.
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, JoinError> {
        match self.0 {
            JoinErrorRepr::Tokio(error) => error
                .try_into_panic()
                .map_err(|error| JoinError(JoinErrorRepr::Tokio(error))),
            JoinErrorRepr::Panic(payload) => {
                Ok(payload.into_inner().unwrap_or_else(PoisonError::into_inner))
            }
            repr @ JoinErrorRepr::Cancelled => Err(JoinError(repr)),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            JoinErrorRepr::Tokio(error) => error.fmt(f),
            JoinErrorRepr::Cancelled => f.write_str("task was cancelled"),
            JoinErrorRepr::Panic(_) => f.write_str("task panicked"),
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            JoinErrorRepr::Tokio(error) => error.fmt(f),
            JoinErrorRepr::Cancelled => f.write_str("JoinError::Cancelled"),
            JoinErrorRepr::Panic(_) => f.write_str("JoinError::Panic(..)"),
        }
    }
}

impl std::error::Error for JoinError {}

impl From<JoinError> for std::io::Error {
    fn from(error: JoinError) -> Self {
        std::io::Error::other(error.to_string())
    }
}

/// A set of tasks joined in the order they finish, as tokio's `JoinSet`.
///
/// Dropping the set aborts every task in it.
pub struct JoinSet<T> {
    tasks: Vec<JoinHandle<T>>,
}

impl<T> JoinSet<T> {
    /// An empty set.
    pub fn new() -> Self {
        JoinSet { tasks: Vec::new() }
    }

    /// How many tasks the set holds, finished or not.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// True if the set holds no tasks.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Spawns `task` into the set. Like [`spawn`], it labels the task with
    /// the caller's file. [`JoinSetExt::spawn_named`] takes a stable label.
    #[track_caller]
    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.insert(spawn(task))
    }

    /// Adds the spawned `handle` to the set and returns its abort handle.
    fn insert(&mut self, handle: JoinHandle<T>) -> AbortHandle {
        let abort = handle.abort_handle();
        self.tasks.push(handle);
        abort
    }

    /// Waits for any task to end and removes it. `None` if the set is empty.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        std::future::poll_fn(|cx| self.poll_join_next(cx)).await
    }

    /// Polls for any ended task. Among tasks that ended, the earliest spawned
    /// is returned first.
    pub fn poll_join_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<T, JoinError>>> {
        if self.tasks.is_empty() {
            return Poll::Ready(None);
        }
        for i in 0..self.tasks.len() {
            if let Poll::Ready(result) = Pin::new(&mut self.tasks[i]).poll(cx) {
                self.tasks.remove(i);
                return Poll::Ready(Some(result));
            }
        }
        Poll::Pending
    }

    /// Aborts every task. They stay in the set until joined.
    #[track_caller]
    pub fn abort_all(&mut self) {
        let site = Location::caller();
        for task in &self.tasks {
            task.abort_handle().abort_at(site);
        }
    }

    /// Removes every task without aborting it.
    pub fn detach_all(&mut self) {
        self.tasks.clear();
    }

    /// Aborts every task and waits for all of them to end.
    pub async fn shutdown(&mut self) {
        self.abort_all();
        while self.join_next().await.is_some() {}
    }
}

/// Adds a labeled spawn to [`JoinSet`].
pub trait JoinSetExt<T> {
    /// Spawns `task` into the set as [`JoinSet::spawn`] does, with `label`
    /// naming the task as in [`spawn_named`].
    #[track_caller]
    fn spawn_named<F>(&mut self, label: &'static str, task: F) -> AbortHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;
}

impl<T: 'static> JoinSetExt<T> for JoinSet<T> {
    #[track_caller]
    fn spawn_named<F>(&mut self, label: &'static str, task: F) -> AbortHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.insert(spawn_named(label, task))
    }
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        JoinSet::new()
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        self.abort_all();
    }
}

impl<T> fmt::Debug for JoinSet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinSet").field("len", &self.len()).finish()
    }
}

/// A [`JoinHandle`] that aborts its task when dropped, as tokio-util's.
#[must_use = "dropping the handle aborts the task"]
pub struct AbortOnDropHandle<T>(Option<JoinHandle<T>>);

impl<T> AbortOnDropHandle<T> {
    /// Wraps `handle`.
    pub fn new(handle: JoinHandle<T>) -> Self {
        AbortOnDropHandle(Some(handle))
    }

    /// Requests cancellation.
    #[track_caller]
    pub fn abort(&self) {
        self.handle().abort();
    }

    /// True once the task has ended.
    #[track_caller]
    pub fn is_finished(&self) -> bool {
        self.handle().is_finished()
    }

    /// A handle that can abort the task without joining it.
    pub fn abort_handle(&self) -> AbortHandle {
        self.handle().abort_handle()
    }

    /// Unwraps the handle without aborting the task.
    pub fn detach(mut self) -> JoinHandle<T> {
        self.0
            .take()
            .expect("the handle is only taken here, which consumes self")
    }

    /// The wrapped handle.
    fn handle(&self) -> &JoinHandle<T> {
        self.0
            .as_ref()
            .expect("the handle is only taken by detach, which consumes self")
    }
}

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

impl<T> Future for AbortOnDropHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let handle = self
            .get_mut()
            .0
            .as_mut()
            .expect("the handle is only taken by detach, which consumes self");
        Pin::new(handle).poll(cx)
    }
}

impl<T> AsRef<JoinHandle<T>> for AbortOnDropHandle<T> {
    fn as_ref(&self) -> &JoinHandle<T> {
        self.handle()
    }
}

impl<T> fmt::Debug for AbortOnDropHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AbortOnDropHandle").field(&self.0).finish()
    }
}
