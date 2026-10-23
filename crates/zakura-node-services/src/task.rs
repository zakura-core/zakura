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

use tower::{buffer::Buffer, Service};
use zakura_rt::task::JoinHandle;

use crate::BoxError;

/// A spawned future boxed so that its type depends only on its output.
pub type ErasedFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Box `future` as an [`ErasedFuture`], for spawners other than [`spawn_erased`]
/// such as `JoinSet::spawn` or `Handle::spawn`.
pub fn erased<T>(future: impl Future<Output = T> + Send + 'static) -> ErasedFuture<T> {
    Box::pin(future)
}

/// Spawn `future` on the current runtime as an [`ErasedFuture`].
///
/// Behaves like [`zakura_rt::task::spawn`], which is [`tokio::spawn`] in
/// production, including panics when called outside a runtime. In the
/// simulator build the task runs on the simulator's scheduler inside a run.
#[track_caller]
pub fn spawn_erased<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<T> {
    zakura_rt::task::spawn(erased(future))
}

/// [`spawn_erased`] with `label` naming the task, through
/// [`zakura_rt::task::spawn_named`].
///
/// Production ignores the label. The simulator builds task identities from
/// it, so a named task keeps its identity when its spawn site moves.
#[track_caller]
pub fn spawn_erased_named<T: Send + 'static>(
    label: &'static str,
    future: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<T> {
    zakura_rt::task::spawn_named(label, erased(future))
}

/// Wrap `service` in a [`Buffer`], spawning its worker as an [`ErasedFuture`].
///
/// Behaves like [`Buffer::new`], which spawns its worker with `tokio::spawn`
/// and so compiles tokio's task code once per buffered service type.
#[track_caller]
pub fn spawn_buffer<S, Request>(service: S, bound: usize) -> Buffer<Request, S::Future>
where
    S: Service<Request> + Send + 'static,
    S::Future: Send,
    S::Error: Into<BoxError> + Send + Sync,
    Request: Send + 'static,
{
    let (buffer, worker) = Buffer::pair(service, bound);
    spawn_erased(worker);
    buffer
}
