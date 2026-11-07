//! Synchronization, simulator build.
//!
//! The channels, `Notify`, `Semaphore` and `CancellationToken` are tokio's
//! code in a thin wrapper that adds a preemption point before each operation
//! and after each release. They wait by returning `Pending`, as tokio's do,
//! and work the same inside and outside a run. The std-style `Mutex` and the
//! coordination atomics are reimplemented, so a contended lock parks the task
//! instead of blocking the run's thread. `Weak` is std's with points on its
//! reference count reads.

pub use std::sync::PoisonError;
pub use tokio::sync::{broadcast, futures, Barrier, Mutex as AsyncMutex};

pub mod atomic;
mod cancel;
pub mod mpsc;
mod mutex;
mod notify;
pub mod oneshot;
mod semaphore;
pub mod watch;
mod weak;

/// Metrics-only atomics: plain std atomics, with no preemption points, in
/// both builds. Use [`atomic`] for anything another task reads to decide
/// what to do.
pub mod counter {
    pub use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
}

pub use cancel::CancellationToken;
pub use mutex::{Mutex, MutexGuard};
pub use notify::Notify;
pub use semaphore::{OwnedSemaphorePermit, Semaphore};
pub use weak::{downgrade, Weak};
