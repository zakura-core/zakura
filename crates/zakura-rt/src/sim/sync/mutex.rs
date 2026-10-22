//! The std-style `Mutex`, simulator build.
//!
//! Inside a run, a contended `lock` parks the task with its worker held,
//! instead of blocking the run's only OS thread. Every waiter retries when
//! the guard drops, and the scheduler's next pick decides who wins. `lock` is
//! a preemption point, and so is the moment right after a guard drops.
//! Poisoning works as in std. Outside a run it's std's mutex.

use std::{
    fmt,
    ops::{Deref, DerefMut},
    panic::Location,
    sync::{LockResult, PoisonError, TryLockError, TryLockResult},
};

use crate::sim::{executor, executor_for, point, run_id, ObjectId, Op, Point, RunId, TaskId};

/// A std-style mutex that parks contended tasks on the scheduler.
pub struct Mutex<T: ?Sized> {
    state: std::sync::Mutex<LockState>,
    inner: std::sync::Mutex<T>,
}

/// Who holds a [`Mutex`], for the scheduler's lock-cycle check.
struct LockState {
    /// The identity tasks park on, made on first use inside each run. A
    /// mutex kept across runs gets a new one, since ids repeat across runs.
    object: Option<(RunId, ObjectId)>,
    /// The holding task, if the holder is a sim task.
    holder: Option<TaskId>,
}

impl<T> Mutex<T> {
    /// An unlocked mutex holding `value`.
    pub const fn new(value: T) -> Self {
        Mutex {
            state: std::sync::Mutex::new(LockState {
                object: None,
                holder: None,
            }),
            inner: std::sync::Mutex::new(value),
        }
    }

    /// The value, or an error if a holder panicked.
    pub fn into_inner(self) -> LockResult<T> {
        self.inner.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Locks the mutex. Inside a run, a contended lock parks the task.
    ///
    /// # Panics
    ///
    /// If the lock is contended inside a run but the caller can't park, such
    /// as setup code outside any task or a task being unwound at shutdown.
    /// Blocking there would hang the run's only thread.
    #[track_caller]
    pub fn lock(&self) -> LockResult<MutexGuard<'_, T>> {
        let site = Location::caller();
        let Some(executor) = executor() else {
            return wrap(self.inner.lock(), self, site);
        };
        let object = self.object();
        executor.preempt(Point {
            op: Op::Lock,
            object: Some(object),
            site,
        });
        loop {
            match self.inner.try_lock() {
                Ok(guard) => return Ok(self.guard(guard, site)),
                Err(TryLockError::Poisoned(poisoned)) => {
                    return Err(PoisonError::new(self.guard(poisoned.into_inner(), site)));
                }
                Err(TryLockError::WouldBlock) => {
                    let holder = self.state().holder;
                    // This can abort the process when it fires in a
                    // destructor, so name what's needed to replay it.
                    assert!(
                        executor.park(object, holder),
                        "contended zakura-rt Mutex at {site} where the caller can't park: \
                         {}, task {:?}, held by {holder:?}{}",
                        run_id().map_or_else(|| "no run".to_owned(), |run| run.to_string()),
                        executor.current_task(),
                        if std::thread::panicking() {
                            ", while unwinding"
                        } else {
                            ""
                        },
                    );
                }
            }
        }
    }

    /// Locks the mutex if it's free now.
    #[track_caller]
    pub fn try_lock(&self) -> TryLockResult<MutexGuard<'_, T>> {
        let site = Location::caller();
        if let Some(executor) = executor() {
            executor.preempt(Point {
                op: Op::Lock,
                object: Some(self.object()),
                site,
            });
        }
        match self.inner.try_lock() {
            Ok(guard) => Ok(self.guard(guard, site)),
            Err(TryLockError::Poisoned(poisoned)) => Err(TryLockError::Poisoned(PoisonError::new(
                self.guard(poisoned.into_inner(), site),
            ))),
            Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
        }
    }

    /// True if a holder panicked.
    pub fn is_poisoned(&self) -> bool {
        self.inner.is_poisoned()
    }

    /// Clears the poisoned flag.
    pub fn clear_poison(&self) {
        self.inner.clear_poison();
    }

    /// The value, through a unique borrow, so no lock is needed.
    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        self.inner.get_mut()
    }

    /// The bookkeeping lock. Never held across a call into the executor.
    fn state(&self) -> std::sync::MutexGuard<'_, LockState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The identity tasks park on in the current run, made on first use.
    fn object(&self) -> ObjectId {
        let run = run_id().expect("objects are only made inside a run");
        if let Some((made_in, object)) = self.state().object {
            if made_in == run {
                return object;
            }
        }
        let object = executor()
            .expect("objects are only made inside a run")
            .new_object();
        self.state().object = Some((run, object));
        object
    }

    /// Wraps a std guard and records the holder.
    fn guard<'a>(
        &'a self,
        inner: std::sync::MutexGuard<'a, T>,
        site: &'static Location<'static>,
    ) -> MutexGuard<'a, T> {
        self.state().holder = executor().and_then(|executor| executor.current_task());
        MutexGuard {
            lock: self,
            inner: Some(inner),
            site,
        }
    }
}

/// Wraps a std lock result outside a run.
fn wrap<'a, T: ?Sized>(
    result: LockResult<std::sync::MutexGuard<'a, T>>,
    lock: &'a Mutex<T>,
    site: &'static Location<'static>,
) -> LockResult<MutexGuard<'a, T>> {
    match result {
        Ok(inner) => Ok(lock.guard(inner, site)),
        Err(poisoned) => Err(PoisonError::new(lock.guard(poisoned.into_inner(), site))),
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Mutex::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(value: T) -> Self {
        Mutex::new(value)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// Holds a [`Mutex`] locked. Dropping it unlocks, wakes every waiter, and
/// then preempts.
#[must_use = "if unused the Mutex will immediately unlock"]
pub struct MutexGuard<'a, T: ?Sized + 'a> {
    lock: &'a Mutex<T>,
    inner: Option<std::sync::MutexGuard<'a, T>>,
    site: &'static Location<'static>,
}

impl<T: ?Sized> MutexGuard<'_, T> {
    /// The std guard. Present until drop.
    fn inner(&self) -> &std::sync::MutexGuard<'_, T> {
        self.inner
            .as_ref()
            .expect("the std guard is only taken by drop")
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.inner()
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.inner
            .as_mut()
            .expect("the std guard is only taken by drop")
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        // Dropping the std guard unlocks it, and poisons it if this thread
        // is panicking.
        drop(self.inner.take());
        let object = {
            let mut state = self.lock.state();
            state.holder = None;
            state.object
        };
        // A guard kept past the run that made the identity has nobody to
        // wake in a later run.
        if let Some((run, object)) = object {
            if let Some(executor) = executor_for(run) {
                executor.unpark_all(object);
                point(Op::Release, self.site);
            }
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}
