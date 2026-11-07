//! Manual polling and wake tracking for batch worker tests.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) struct Task<T> {
    future: Pin<Box<T>>,
    wake_flag: Arc<WakeFlag>,
}

impl<T> Task<T> {
    pub(super) fn new(future: T) -> Self {
        Self {
            future: Box::pin(future),
            wake_flag: Arc::default(),
        }
    }

    pub(super) fn is_woken(&self) -> bool {
        self.wake_flag.0.load(Ordering::Acquire)
    }

    /// Clears previous notifications before polling or entering the context.
    pub(super) fn enter<R>(&mut self, f: impl FnOnce(&mut Context<'_>, Pin<&mut T>) -> R) -> R {
        self.wake_flag.0.store(false, Ordering::Release);
        let waker = Waker::from(self.wake_flag.clone());
        f(&mut Context::from_waker(&waker), self.future.as_mut())
    }
}

impl<F: Future> Task<F> {
    pub(super) fn poll(&mut self) -> Poll<F::Output> {
        self.enter(|cx, future| future.poll(cx))
    }
}

#[track_caller]
pub(super) fn ready<T>(poll: Poll<T>) -> T {
    match poll {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected a ready future"),
    }
}

#[track_caller]
pub(super) fn ready_err<T, E>(poll: Poll<Result<T, E>>, message: &str) -> E {
    match ready(poll) {
        Err(error) => error,
        Ok(_) => panic!("{message}"),
    }
}
