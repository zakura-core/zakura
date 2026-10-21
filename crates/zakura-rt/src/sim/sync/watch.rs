//! Tokio's `watch` channel with preemption points, simulator build.
//!
//! Every read and write of the value is a point. Waiters still register on
//! tokio's internal notify shards, which tokio picks with its own RNG. Across
//! tasks that doesn't matter, because a wakeup only marks a task ready
//! (`runtime.md`, the `watch` caveat).

use std::{fmt, future::Future, panic::Location};

use tokio::sync::watch as tokio_watch;

pub use tokio_watch::{error, Ref};

use crate::sim::{point, Op};

/// A channel holding `init`, as tokio's.
pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio_watch::channel(init);
    (Sender { inner: tx }, Receiver { inner: rx })
}

/// The sending half of a `watch` channel.
pub struct Sender<T> {
    inner: tokio_watch::Sender<T>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            inner: self.inner.clone(),
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: Default> Default for Sender<T> {
    fn default() -> Self {
        Sender::new(T::default())
    }
}

impl<T> Sender<T> {
    /// A sender with no receivers yet.
    pub fn new(init: T) -> Self {
        Sender {
            inner: tokio_watch::Sender::new(init),
        }
    }

    /// Replaces the value if any receiver is left.
    #[track_caller]
    pub fn send(&self, value: T) -> Result<(), error::SendError<T>> {
        point(Op::Watch, Location::caller());
        self.inner.send(value)
    }

    /// Replaces the value and returns the old one.
    #[track_caller]
    pub fn send_replace(&self, value: T) -> T {
        point(Op::Watch, Location::caller());
        self.inner.send_replace(value)
    }

    /// Changes the value in place and notifies receivers.
    #[track_caller]
    pub fn send_modify<F: FnOnce(&mut T)>(&self, modify: F) {
        point(Op::Watch, Location::caller());
        self.inner.send_modify(modify);
    }

    /// Changes the value in place, and notifies receivers if `modify`
    /// returns true.
    #[track_caller]
    pub fn send_if_modified<F: FnOnce(&mut T) -> bool>(&self, modify: F) -> bool {
        point(Op::Watch, Location::caller());
        self.inner.send_if_modified(modify)
    }

    /// Reads the value.
    #[track_caller]
    pub fn borrow(&self) -> Ref<'_, T> {
        point(Op::Watch, Location::caller());
        self.inner.borrow()
    }

    /// A new receiver that has seen the current value.
    pub fn subscribe(&self) -> Receiver<T> {
        Receiver {
            inner: self.inner.subscribe(),
        }
    }

    /// True once every receiver is gone.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Watch, Location::caller());
        self.inner.is_closed()
    }

    /// Waits until every receiver is gone.
    #[track_caller]
    pub fn closed(&self) -> impl Future<Output = ()> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Watch, site);
            self.inner.closed().await
        }
    }

    /// Live receivers.
    pub fn receiver_count(&self) -> usize {
        self.inner.receiver_count()
    }

    /// Live senders.
    pub fn sender_count(&self) -> usize {
        self.inner.sender_count()
    }

    /// True if both senders feed the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }
}

/// The receiving half of a `watch` channel.
pub struct Receiver<T> {
    inner: tokio_watch::Receiver<T>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver {
            inner: self.inner.clone(),
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Receiver<T> {
    /// Reads the value without marking it seen.
    #[track_caller]
    pub fn borrow(&self) -> Ref<'_, T> {
        point(Op::Watch, Location::caller());
        self.inner.borrow()
    }

    /// Reads the value and marks it seen.
    #[track_caller]
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        point(Op::Watch, Location::caller());
        self.inner.borrow_and_update()
    }

    /// True if the value changed since this receiver last saw it.
    #[track_caller]
    pub fn has_changed(&self) -> Result<bool, error::RecvError> {
        point(Op::Watch, Location::caller());
        self.inner.has_changed()
    }

    /// Marks the current value unseen.
    pub fn mark_changed(&mut self) {
        self.inner.mark_changed();
    }

    /// Marks the current value seen.
    pub fn mark_unchanged(&mut self) {
        self.inner.mark_unchanged();
    }

    /// Waits for a value this receiver hasn't seen, and marks it seen.
    #[track_caller]
    pub fn changed(&mut self) -> impl Future<Output = Result<(), error::RecvError>> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Watch, site);
            self.inner.changed().await
        }
    }

    /// Waits until `f` accepts the value, and marks it seen.
    #[track_caller]
    pub fn wait_for<'a, F>(
        &'a mut self,
        f: F,
    ) -> impl Future<Output = Result<Ref<'a, T>, error::RecvError>> + 'a
    where
        F: FnMut(&T) -> bool + 'a,
    {
        let site = Location::caller();
        async move {
            point(Op::Watch, site);
            self.inner.wait_for(f).await
        }
    }

    /// True if both receivers watch the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }
}
