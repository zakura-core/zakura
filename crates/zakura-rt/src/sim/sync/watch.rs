//! Tokio's `watch` channel with preemption points, simulator build.
//!
//! Every read and write of the value is a point, and so are `subscribe` and
//! the moment right after the last sender drops. Waiters still register on
//! tokio's internal notify shards, which tokio picks with its own RNG. Across
//! tasks that doesn't matter, because a wakeup only marks a task ready
//! (`runtime.md`, the `watch` caveat).
//!
//! Tokio guards the value with a std `RwLock`. While a [`Ref`] or a
//! `send_modify` closure holds it, preemption points do nothing, so the
//! scheduler never switches to a writer that would block the run's thread.

use std::{fmt, future::Future, ops::Deref, panic::Location};

use tokio::sync::watch as tokio_watch;

pub use tokio_watch::error;

use crate::sim::{point, Op, WatchLockHeld};

/// A channel holding `init`, as tokio's.
#[track_caller]
pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio_watch::channel(init);
    (Sender::wrap(tx, Location::caller()), Receiver { inner: rx })
}

/// A borrowed `watch` value, holding tokio's read lock until it drops.
///
/// Preemption points do nothing while it lives (see the module docs).
pub struct Ref<'a, T> {
    inner: tokio_watch::Ref<'a, T>,
    _held: WatchLockHeld,
}

impl<'a, T> Ref<'a, T> {
    /// Wraps tokio's `Ref`, which already holds the lock.
    fn new(inner: tokio_watch::Ref<'a, T>) -> Self {
        Ref {
            inner,
            _held: WatchLockHeld::new(),
        }
    }

    /// True if the borrow marked a value this receiver hadn't seen.
    pub fn has_changed(&self) -> bool {
        self.inner.has_changed()
    }
}

impl<T> Deref for Ref<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: fmt::Debug> fmt::Debug for Ref<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// The sending half of a `watch` channel.
pub struct Sender<T> {
    /// Taken only by `drop`.
    inner: Option<tokio_watch::Sender<T>>,
    /// Where the channel was made, for the point after the last sender drops.
    site: &'static Location<'static>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender::wrap(self.tx().clone(), self.site)
    }
}

impl<T: fmt::Debug> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.tx().fmt(f)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // The last sender closes the channel for every receiver.
            let closing = inner.sender_count() == 1;
            drop(inner);
            if closing {
                point(Op::Release, self.site);
            }
        }
    }
}

impl<T: Default> Default for Sender<T> {
    #[track_caller]
    fn default() -> Self {
        Sender::new(T::default())
    }
}

impl<T> Sender<T> {
    /// A sender with no receivers yet.
    #[track_caller]
    pub fn new(init: T) -> Self {
        Sender::wrap(tokio_watch::Sender::new(init), Location::caller())
    }

    /// Wraps tokio's sender for a channel made at `site`.
    fn wrap(inner: tokio_watch::Sender<T>, site: &'static Location<'static>) -> Self {
        Sender {
            inner: Some(inner),
            site,
        }
    }

    /// Tokio's sender.
    fn tx(&self) -> &tokio_watch::Sender<T> {
        self.inner
            .as_ref()
            .expect("the sender is only taken by drop")
    }

    /// Replaces the value if any receiver is left.
    #[track_caller]
    pub fn send(&self, value: T) -> Result<(), error::SendError<T>> {
        point(Op::Watch, Location::caller());
        self.tx().send(value)
    }

    /// Replaces the value and returns the old one.
    #[track_caller]
    pub fn send_replace(&self, value: T) -> T {
        point(Op::Watch, Location::caller());
        self.tx().send_replace(value)
    }

    /// Changes the value in place and notifies receivers.
    #[track_caller]
    pub fn send_modify<F: FnOnce(&mut T)>(&self, modify: F) {
        point(Op::Watch, Location::caller());
        self.tx().send_modify(|value| {
            let _held = WatchLockHeld::new();
            modify(value);
        });
    }

    /// Changes the value in place, and notifies receivers if `modify`
    /// returns true.
    #[track_caller]
    pub fn send_if_modified<F: FnOnce(&mut T) -> bool>(&self, modify: F) -> bool {
        point(Op::Watch, Location::caller());
        self.tx().send_if_modified(|value| {
            let _held = WatchLockHeld::new();
            modify(value)
        })
    }

    /// Reads the value.
    #[track_caller]
    pub fn borrow(&self) -> Ref<'_, T> {
        point(Op::Watch, Location::caller());
        Ref::new(self.tx().borrow())
    }

    /// A new receiver that has seen the current value.
    #[track_caller]
    pub fn subscribe(&self) -> Receiver<T> {
        point(Op::Watch, Location::caller());
        Receiver {
            inner: self.tx().subscribe(),
        }
    }

    /// True once every receiver is gone.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Watch, Location::caller());
        self.tx().is_closed()
    }

    /// Waits until every receiver is gone.
    #[track_caller]
    pub fn closed(&self) -> impl Future<Output = ()> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Watch, site);
            self.tx().closed().await
        }
    }

    /// Live receivers.
    pub fn receiver_count(&self) -> usize {
        self.tx().receiver_count()
    }

    /// Live senders.
    pub fn sender_count(&self) -> usize {
        self.tx().sender_count()
    }

    /// True if both senders feed the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.tx().same_channel(other.tx())
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
        Ref::new(self.inner.borrow())
    }

    /// Reads the value and marks it seen.
    #[track_caller]
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        point(Op::Watch, Location::caller());
        Ref::new(self.inner.borrow_and_update())
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
        mut f: F,
    ) -> impl Future<Output = Result<Ref<'a, T>, error::RecvError>> + 'a
    where
        F: FnMut(&T) -> bool + 'a,
    {
        let site = Location::caller();
        async move {
            point(Op::Watch, site);
            // Tokio calls `f` with the value's lock held.
            let found = self
                .inner
                .wait_for(|value| {
                    let _held = WatchLockHeld::new();
                    f(value)
                })
                .await?;
            Ok(Ref::new(found))
        }
    }

    /// True if both receivers watch the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }
}
