//! Spawn tasks without compiling tokio's per-task code once per future type.
//!
//! `tokio::spawn` instantiates its task machinery (`Harness`, `Core`, `Cell`,
//! `poll_future`, ...) for every distinct future type: for both the future and
//! its boxed form (`spawn` boxes futures above a size threshold), and for each
//! runtime scheduler. Spawning a type-erased future instead shares those copies
//! among all tasks with the same output type.
//!
//! Boxing costs one allocation per spawn and an indirect call per poll, so use
//! [`spawn_erased`] for tasks started once, per peer connection, or on a timer.
//! Keep `tokio::spawn` for tasks spawned per request, block, or transaction.

use std::{future::Future, pin::Pin};

use tokio::task::JoinHandle;

/// A spawned future boxed so that its type depends only on its output.
pub type ErasedFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Box `future` as an [`ErasedFuture`], for spawners other than [`spawn_erased`]
/// such as `JoinSet::spawn` or `Handle::spawn`.
pub fn erased<T>(future: impl Future<Output = T> + Send + 'static) -> ErasedFuture<T> {
    Box::pin(future)
}

/// Spawn `future` on the current runtime as an [`ErasedFuture`].
///
/// Behaves like [`tokio::spawn`], including panics when called outside a runtime.
#[track_caller]
pub fn spawn_erased<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<T> {
    tokio::spawn(erased(future))
}
