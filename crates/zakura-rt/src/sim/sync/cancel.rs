//! tokio-util's `CancellationToken` with preemption points, simulator build.
//!
//! `cancel`, `is_cancelled`, and the first poll of `cancelled` are points.
//! [`CancellationToken::on_cancel`] adds none: its hooks run synchronously
//! inside the `cancel` that cancels the token, after that call's point.

use std::{
    fmt,
    future::Future,
    mem,
    panic::Location,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError, Weak},
    task::{Context, Poll},
};

use tokio_util::sync as tokio_sync;

use crate::sim::{point, Op};

/// tokio-util's `CancellationToken`, with preemption points.
#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: tokio_sync::CancellationToken,
    /// This token's hooks, shared by its clones.
    hooks: Arc<Handle>,
}

/// A token's node in the hook tree, held by the token's clones and by the
/// futures that own its tokio token.
///
/// When the last holder drops, the token's own hooks are dropped unrun, since
/// nothing can cancel it any more. Its node stays while a descendant lives,
/// because tokio still cancels that descendant through its ancestors.
#[derive(Default)]
struct Handle(Arc<Hooks>);

impl Drop for Handle {
    /// Drops the token's pending hooks, outside the lock, since a hook's own
    /// drop may touch other tokens.
    fn drop(&mut self) {
        let pending = mem::take(&mut *lock(&self.0.pending));
        drop(pending);
    }
}

/// A token's cancellation hooks, and the path to its children's, which its
/// cancellation also runs.
#[derive(Default)]
struct Hooks {
    /// Hooks that haven't run, in the order they were added.
    pending: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
    /// The children's nodes. A node with no living token or descendant is
    /// skipped and pruned.
    children: Mutex<Vec<Weak<Hooks>>>,
    /// The parent's node, kept alive so the path from it to this node
    /// survives the parent's own tokens being dropped.
    _parent: Option<Arc<Hooks>>,
}

impl Hooks {
    /// Runs this token's pending hooks, then its children's, each once.
    ///
    /// Runs no hook while it holds a lock, so a hook may add hooks or cancel
    /// tokens itself.
    fn run(&self) {
        let pending = mem::take(&mut *lock(&self.pending));
        for hook in pending {
            hook();
        }
        let children: Vec<Arc<Hooks>> = lock(&self.children)
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for child in children {
            child.run();
        }
    }
}

/// Locks `mutex`. A hook that panicked left only a drained list behind, so a
/// poisoned lock still holds consistent state.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl CancellationToken {
    /// A token that isn't cancelled.
    pub fn new() -> Self {
        CancellationToken::default()
    }

    /// A token that is cancelled with this one, but not the other way round.
    pub fn child_token(&self) -> Self {
        let hooks = Arc::new(Hooks {
            _parent: Some(Arc::clone(&self.hooks.0)),
            ..Hooks::default()
        });
        let mut children = lock(&self.hooks.0.children);
        children.retain(|child| child.strong_count() > 0);
        children.push(Arc::downgrade(&hooks));
        CancellationToken {
            inner: self.inner.child_token(),
            hooks: Arc::new(Handle(hooks)),
        }
    }

    /// Runs `hook` once, when this token is cancelled, directly or through a
    /// parent. It runs synchronously inside that `cancel` call, after the
    /// call's preemption point, so it must not block or reach a point itself.
    /// If the token is already cancelled, `hook` runs now.
    ///
    /// Adding a hook is not a point, and neither is running one. Only the
    /// simulator build has this, for test observers.
    pub fn on_cancel(&self, hook: impl FnOnce() + Send + 'static) {
        let mut pending = lock(&self.hooks.0.pending);
        // tokio's token marks itself cancelled before this wrapper runs the
        // hooks, so a token it reports cancelled already took or will never
        // take its hooks.
        if self.inner.is_cancelled() {
            drop(pending);
            hook();
        } else {
            pending.push(Box::new(hook));
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
        self.hooks.0.run();
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
            _hooks: self.hooks,
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
        // The token's hooks, which run while this future owns the token.
        _hooks: Arc<Handle>,
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
