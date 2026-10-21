//! Synchronization, simulator build.
//!
//! The channels, `Notify`, `Semaphore` and `CancellationToken` are tokio's
//! code in a thin wrapper that adds a preemption point before each operation
//! and after each release. They wait by returning `Pending`, as tokio's do,
//! and work the same inside and outside a run. The std `Mutex` and the
//! atomics are still std's.

pub use std::sync::{atomic, Mutex, MutexGuard, PoisonError};
pub use tokio::sync::{broadcast, futures, Barrier, Mutex as AsyncMutex};

mod cancel;
pub mod mpsc;
mod notify;
pub mod oneshot;
mod semaphore;
pub mod watch;

pub use cancel::CancellationToken;
pub use notify::Notify;
pub use semaphore::{OwnedSemaphorePermit, Semaphore};
