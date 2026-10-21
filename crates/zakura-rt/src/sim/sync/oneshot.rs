//! Tokio's `oneshot` channel with preemption points, simulator build.
//!
//! The send and the receive (its first poll, or `try_recv`) are points.

use std::{
    fmt,
    future::Future,
    panic::Location,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::sync::oneshot as tokio_oneshot;

pub use tokio_oneshot::error;

use self::error::{RecvError, TryRecvError};
use crate::sim::{point, Op};

/// A channel for one value, as tokio's.
#[track_caller]
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio_oneshot::channel();
    let receiver = Receiver {
        inner: rx,
        site: Location::caller(),
        polled: false,
    };
    (Sender { inner: tx }, receiver)
}

/// The sending half of a `oneshot` channel.
pub struct Sender<T> {
    inner: tokio_oneshot::Sender<T>,
}

impl<T: fmt::Debug> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Sender<T> {
    /// Sends `value`, or hands it back if the receiver is gone.
    #[track_caller]
    pub fn send(self, value: T) -> Result<(), T> {
        point(Op::Send, Location::caller());
        self.inner.send(value)
    }

    /// True if the receiver is gone.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Send, Location::caller());
        self.inner.is_closed()
    }

    /// Waits until the receiver is gone.
    #[track_caller]
    pub fn closed(&mut self) -> impl Future<Output = ()> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            self.inner.closed().await
        }
    }

    /// Polls for the receiver going away.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.inner.poll_closed(cx)
    }
}

/// The receiving half of a `oneshot` channel. Await it for the value.
pub struct Receiver<T> {
    inner: tokio_oneshot::Receiver<T>,
    site: &'static Location<'static>,
    /// Whether the receive point has passed.
    polled: bool,
}

impl<T: fmt::Debug> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Receiver<T> {
    /// Takes the value if it was sent.
    #[track_caller]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        point(Op::Recv, Location::caller());
        self.inner.try_recv()
    }

    /// Stops the sender from sending.
    #[track_caller]
    pub fn close(&mut self) {
        point(Op::Recv, Location::caller());
        self.inner.close();
    }

    /// True once the value was received or the channel closed.
    pub fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }

    /// True if no value is waiting.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.polled {
            self.polled = true;
            point(Op::Recv, self.site);
        }
        Pin::new(&mut self.inner).poll(cx)
    }
}
