//! Tokio's `Notify` with preemption points, simulator build.

use std::{fmt, panic::Location};

use tokio::sync::futures::Notified;

use crate::sim::{point, Op};

/// Tokio's `Notify`, with points before each notify and before `notified()`.
///
/// The `notified()` point sits at creation, not at the first poll, because
/// tokio reads the notify state when the future is created. A
/// `notify_waiters` after that wakes it even if it was never polled. So the
/// race that matters falls between a caller's check and `notified()`.
#[derive(Default)]
pub struct Notify {
    inner: tokio::sync::Notify,
}

impl Notify {
    /// A notify with no stored permit.
    pub fn new() -> Self {
        Notify {
            inner: tokio::sync::Notify::new(),
        }
    }

    /// Like [`Notify::new`], usable in statics.
    pub const fn const_new() -> Self {
        Notify {
            inner: tokio::sync::Notify::const_new(),
        }
    }

    /// A future that completes on the next notification. Keeps tokio's
    /// `Notified`, so `enable` and the stored permit work as in production.
    #[track_caller]
    pub fn notified(&self) -> Notified<'_> {
        point(Op::Notified, Location::caller());
        self.inner.notified()
    }

    /// Wakes one waiter, or stores a permit for the next one.
    #[track_caller]
    pub fn notify_one(&self) {
        point(Op::Notify, Location::caller());
        self.inner.notify_one();
    }

    /// Wakes the most recent waiter, or stores a permit.
    #[track_caller]
    pub fn notify_last(&self) {
        point(Op::Notify, Location::caller());
        self.inner.notify_last();
    }

    /// Wakes every waiter that exists now. Stores no permit.
    #[track_caller]
    pub fn notify_waiters(&self) {
        point(Op::Notify, Location::caller());
        self.inner.notify_waiters();
    }
}

impl fmt::Debug for Notify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}
