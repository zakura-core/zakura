//! tokio-util's `CancellationToken` with preemption points, simulator build.
//!
//! `cancel`, `is_cancelled`, and the first poll of `cancelled` are points.

use std::{
    fmt,
    future::Future,
    panic::Location,
    pin::Pin,
    task::{Context, Poll},
};

use tokio_util::sync as tokio_sync;

use crate::sim::{point, Op};

/// tokio-util's `CancellationToken`, with preemption points.
#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: tokio_sync::CancellationToken,
}

impl CancellationToken {
    /// A token that isn't cancelled.
    pub fn new() -> Self {
        CancellationToken::default()
    }

    /// A token that is cancelled with this one, but not the other way round.
    pub fn child_token(&self) -> Self {
        CancellationToken {
            inner: self.inner.child_token(),
        }
    }

    /// Cancels this token and its children.
    #[track_caller]
    pub fn cancel(&self) {
        self.cancel_at(Location::caller());
    }

    /// [`CancellationToken::cancel`], reporting `site` as the call site.
    fn cancel_at(&self, site: &'static Location<'static>) {
        point(Op::Cancel, site);
        self.inner.cancel();
    }

    /// True once cancelled.
    #[track_caller]
    pub fn is_cancelled(&self) -> bool {
        point(Op::Cancelled, Location::caller());
        self.inner.is_cancelled()
    }

    /// Waits until cancelled.
    #[track_caller]
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        WaitForCancellationFuture {
            inner: self.inner.cancelled(),
            site: Location::caller(),
            polled: false,
        }
    }

    /// Waits until cancelled, owning the token.
    #[track_caller]
    pub fn cancelled_owned(self) -> WaitForCancellationFutureOwned {
        WaitForCancellationFutureOwned {
            inner: self.inner.cancelled_owned(),
            site: Location::caller(),
            polled: false,
        }
    }

    /// A guard that cancels this token when dropped.
    #[track_caller]
    pub fn drop_guard(self) -> DropGuard {
        DropGuard {
            token: Some(self),
            site: Location::caller(),
        }
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

pin_project_lite::pin_project! {
    /// The future [`CancellationToken::cancelled`] returns.
    #[must_use = "futures do nothing unless polled"]
    pub struct WaitForCancellationFuture<'a> {
        #[pin]
        inner: tokio_sync::WaitForCancellationFuture<'a>,
        site: &'static Location<'static>,
        polled: bool,
    }
}

impl Future for WaitForCancellationFuture<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        if !*this.polled {
            *this.polled = true;
            point(Op::Cancelled, this.site);
        }
        this.inner.poll(cx)
    }
}

impl fmt::Debug for WaitForCancellationFuture<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

pin_project_lite::pin_project! {
    /// The future [`CancellationToken::cancelled_owned`] returns.
    #[must_use = "futures do nothing unless polled"]
    pub struct WaitForCancellationFutureOwned {
        #[pin]
        inner: tokio_sync::WaitForCancellationFutureOwned,
        site: &'static Location<'static>,
        polled: bool,
    }
}

impl Future for WaitForCancellationFutureOwned {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        if !*this.polled {
            *this.polled = true;
            point(Op::Cancelled, this.site);
        }
        this.inner.poll(cx)
    }
}

impl fmt::Debug for WaitForCancellationFutureOwned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// Cancels its token when dropped, unless disarmed.
#[must_use = "the token is cancelled when the guard drops"]
pub struct DropGuard {
    token: Option<CancellationToken>,
    site: &'static Location<'static>,
}

impl DropGuard {
    /// Returns the token without cancelling it.
    pub fn disarm(mut self) -> CancellationToken {
        self.token
            .take()
            .expect("the token is only taken here, which consumes the guard")
    }
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        if let Some(token) = &self.token {
            token.cancel_at(self.site);
        }
    }
}

impl fmt::Debug for DropGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DropGuard")
            .field("token", &self.token)
            .finish()
    }
}
