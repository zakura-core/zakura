//! Tokio's `mpsc` channels with preemption points, simulator build.
//!
//! Each send, reserve, receive, `is_closed` and `capacity` is a point, and so
//! is the moment right after an unused permit gives its slot back. A send
//! through a reserved permit is its own point too, since it enqueues the
//! message. Closing the channel by dropping its receiver or its last sender
//! is a release, with a point right after it.

use std::{
    fmt,
    future::Future,
    panic::Location,
    task::{Context, Poll},
};

use tokio::sync::mpsc as tokio_mpsc;

pub use tokio_mpsc::error;

use self::error::{SendError, TryRecvError, TrySendError};
use crate::sim::{point, Op};

/// A bounded channel with room for `buffer` messages, as tokio's.
#[track_caller]
pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
    let site = Location::caller();
    let (tx, rx) = tokio_mpsc::channel(buffer);
    (
        Sender::wrap(tx, site),
        Receiver {
            inner: Some(rx),
            site,
        },
    )
}

/// An unbounded channel, as tokio's.
#[track_caller]
pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let site = Location::caller();
    let (tx, rx) = tokio_mpsc::unbounded_channel();
    (
        UnboundedSender {
            inner: Some(tx),
            site,
        },
        UnboundedReceiver {
            inner: Some(rx),
            site,
        },
    )
}

/// The point right after `handle` drops, if dropping it closes its channel.
///
/// The sim handles keep tokio's in an `Option` that only `drop` and
/// consuming methods take, so this runs after tokio's handle is gone.
fn drop_closing<H>(
    handle: Option<H>,
    closes: impl FnOnce(&H) -> bool,
    site: &'static Location<'static>,
) {
    if let Some(handle) = handle {
        let closing = closes(&handle);
        drop(handle);
        if closing {
            point(Op::Release, site);
        }
    }
}

/// The message for a handle used after `drop` or a consuming method took it.
const TAKEN: &str = "a handle is only taken by drop or a method that consumes it";

/// The sending half of a bounded channel.
pub struct Sender<T> {
    inner: Option<tokio_mpsc::Sender<T>>,
    /// Where the channel was made, for the point after the last sender drops.
    site: &'static Location<'static>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender::wrap(self.tx().clone(), self.site)
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.tx().fmt(f)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // The last sender closes the channel.
        drop_closing(self.inner.take(), |tx| tx.strong_count() == 1, self.site);
    }
}

impl<T> Sender<T> {
    /// Wraps tokio's sender for a channel made at `site`.
    fn wrap(inner: tokio_mpsc::Sender<T>, site: &'static Location<'static>) -> Self {
        Sender {
            inner: Some(inner),
            site,
        }
    }

    /// Tokio's sender.
    fn tx(&self) -> &tokio_mpsc::Sender<T> {
        self.inner.as_ref().expect(TAKEN)
    }

    /// Takes tokio's sender, for a method that consumes `self`. The drop
    /// that follows finds nothing to close.
    fn into_tx(mut self) -> (tokio_mpsc::Sender<T>, &'static Location<'static>) {
        (self.inner.take().expect(TAKEN), self.site)
    }

    /// Waits for capacity, then sends `value`.
    #[track_caller]
    pub fn send(&self, value: T) -> impl Future<Output = Result<(), SendError<T>>> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            self.tx().send(value).await
        }
    }

    /// Sends `value` if there's capacity now.
    #[track_caller]
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        point(Op::Send, Location::caller());
        self.tx().try_send(value)
    }

    /// Waits for a slot and holds it.
    #[track_caller]
    pub fn reserve(&self) -> impl Future<Output = Result<Permit<'_, T>, SendError<()>>> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            let inner = self.tx().reserve().await?;
            Ok(Permit {
                inner: Some(inner),
                site,
            })
        }
    }

    /// Holds a slot if one is free now.
    #[track_caller]
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        let site = Location::caller();
        point(Op::Send, site);
        let inner = self.tx().try_reserve()?;
        Ok(Permit {
            inner: Some(inner),
            site,
        })
    }

    /// Holds `n` slots if they're free now. Each one sends a message.
    #[track_caller]
    pub fn try_reserve_many(&self, n: usize) -> Result<PermitIterator<'_, T>, TrySendError<()>> {
        let site = Location::caller();
        point(Op::Send, site);
        let inner = self.tx().try_reserve_many(n)?;
        Ok(PermitIterator { inner, site })
    }

    /// Waits for a slot and holds it, taking the sender.
    #[track_caller]
    pub fn reserve_owned(self) -> impl Future<Output = Result<OwnedPermit<T>, SendError<()>>> {
        let site = Location::caller();
        let (tx, channel) = self.into_tx();
        async move {
            point(Op::Send, site);
            let inner = tx.reserve_owned().await?;
            Ok(OwnedPermit {
                inner: Some(inner),
                site,
                channel,
            })
        }
    }

    /// Holds a slot if one is free now, taking the sender.
    #[track_caller]
    pub fn try_reserve_owned(self) -> Result<OwnedPermit<T>, TrySendError<Self>> {
        let site = Location::caller();
        point(Op::Send, site);
        let (tx, channel) = self.into_tx();
        match tx.try_reserve_owned() {
            Ok(inner) => Ok(OwnedPermit {
                inner: Some(inner),
                site,
                channel,
            }),
            Err(TrySendError::Full(inner)) => Err(TrySendError::Full(Sender::wrap(inner, channel))),
            Err(TrySendError::Closed(inner)) => {
                Err(TrySendError::Closed(Sender::wrap(inner, channel)))
            }
        }
    }

    /// Waits until the receiver is dropped or closed.
    #[track_caller]
    pub fn closed(&self) -> impl Future<Output = ()> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            self.tx().closed().await
        }
    }

    /// True if the receiver is dropped or closed.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Send, Location::caller());
        self.tx().is_closed()
    }

    /// Free slots now.
    #[track_caller]
    pub fn capacity(&self) -> usize {
        point(Op::Send, Location::caller());
        self.tx().capacity()
    }

    /// The channel's size.
    pub fn max_capacity(&self) -> usize {
        self.tx().max_capacity()
    }

    /// True if both senders feed the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.tx().same_channel(other.tx())
    }

    /// A sender that doesn't keep the channel open.
    pub fn downgrade(&self) -> WeakSender<T> {
        WeakSender {
            inner: self.tx().downgrade(),
            site: self.site,
        }
    }

    /// Live senders of this channel.
    pub fn strong_count(&self) -> usize {
        self.tx().strong_count()
    }

    /// Weak senders of this channel.
    pub fn weak_count(&self) -> usize {
        self.tx().weak_count()
    }
}

/// A bounded sender that doesn't keep the channel open.
pub struct WeakSender<T> {
    inner: tokio_mpsc::WeakSender<T>,
    site: &'static Location<'static>,
}

impl<T> Clone for WeakSender<T> {
    fn clone(&self) -> Self {
        WeakSender {
            inner: self.inner.clone(),
            site: self.site,
        }
    }
}

impl<T> fmt::Debug for WeakSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> WeakSender<T> {
    /// A sender, if the channel is still open.
    pub fn upgrade(&self) -> Option<Sender<T>> {
        self.inner
            .upgrade()
            .map(|inner| Sender::wrap(inner, self.site))
    }
}

/// A held slot in a bounded channel.
pub struct Permit<'a, T> {
    inner: Option<tokio_mpsc::Permit<'a, T>>,
    site: &'static Location<'static>,
}

impl<T> Permit<'_, T> {
    /// Sends `value` in the held slot. Inside a run this is a point.
    #[track_caller]
    pub fn send(mut self, value: T) {
        point(Op::Send, Location::caller());
        self.inner
            .take()
            .expect("the slot is only taken here, which consumes the permit")
            .send(value);
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            drop(inner);
            point(Op::Release, self.site);
        }
    }
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

/// Held slots from [`Sender::try_reserve_many`], one [`Permit`] each.
pub struct PermitIterator<'a, T> {
    inner: tokio_mpsc::PermitIterator<'a, T>,
    site: &'static Location<'static>,
}

impl<'a, T> Iterator for PermitIterator<'a, T> {
    type Item = Permit<'a, T>;

    fn next(&mut self) -> Option<Permit<'a, T>> {
        let inner = self.inner.next()?;
        Some(Permit {
            inner: Some(inner),
            site: self.site,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T> ExactSizeIterator for PermitIterator<'_, T> {}

impl<T> Drop for PermitIterator<'_, T> {
    fn drop(&mut self) {
        // Slots left in the iterator go back when it drops.
        if self.inner.len() > 0 {
            for permit in self.inner.by_ref() {
                drop(permit);
            }
            point(Op::Release, self.site);
        }
    }
}

impl<T> fmt::Debug for PermitIterator<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PermitIterator")
            .field("left", &self.inner.len())
            .finish()
    }
}

/// A held slot in a bounded channel, owning its sender.
pub struct OwnedPermit<T> {
    inner: Option<tokio_mpsc::OwnedPermit<T>>,
    site: &'static Location<'static>,
    /// Where the channel was made, for the sender it gives back.
    channel: &'static Location<'static>,
}

impl<T> OwnedPermit<T> {
    /// Sends `value` in the held slot and gives the sender back. Inside a run
    /// this is a point.
    #[track_caller]
    pub fn send(mut self, value: T) -> Sender<T> {
        point(Op::Send, Location::caller());
        let inner = self
            .inner
            .take()
            .expect("the slot is only taken here or in release, which consume the permit")
            .send(value);
        Sender::wrap(inner, self.channel)
    }

    /// Gives the slot back without sending, and the sender with it.
    pub fn release(mut self) -> Sender<T> {
        let inner = self
            .inner
            .take()
            .expect("the slot is only taken here or in send, which consume the permit")
            .release();
        point(Op::Release, self.site);
        Sender::wrap(inner, self.channel)
    }
}

impl<T> Drop for OwnedPermit<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            drop(inner);
            point(Op::Release, self.site);
        }
    }
}

impl<T> fmt::Debug for OwnedPermit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPermit").finish_non_exhaustive()
    }
}

/// The receiving half of a bounded channel.
pub struct Receiver<T> {
    /// Taken only by `drop`.
    inner: Option<tokio_mpsc::Receiver<T>>,
    /// Where the channel was made, for the point after the receiver drops.
    site: &'static Location<'static>,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.rx().fmt(f)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        drop_closing(self.inner.take(), |_| true, self.site);
    }
}

impl<T> Receiver<T> {
    /// Tokio's receiver.
    fn rx(&self) -> &tokio_mpsc::Receiver<T> {
        self.inner.as_ref().expect(TAKEN)
    }

    /// Tokio's receiver, mutably.
    fn rx_mut(&mut self) -> &mut tokio_mpsc::Receiver<T> {
        self.inner.as_mut().expect(TAKEN)
    }

    /// Waits for the next message. `None` once every sender is gone.
    #[track_caller]
    pub fn recv(&mut self) -> impl Future<Output = Option<T>> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Recv, site);
            self.rx_mut().recv().await
        }
    }

    /// Takes a message if one is queued now.
    #[track_caller]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        point(Op::Recv, Location::caller());
        self.rx_mut().try_recv()
    }

    /// Polls for the next message. Each call is a point.
    #[track_caller]
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        point(Op::Recv, Location::caller());
        self.rx_mut().poll_recv(cx)
    }

    /// Stops new sends. Queued messages can still be received.
    #[track_caller]
    pub fn close(&mut self) {
        point(Op::Recv, Location::caller());
        self.rx_mut().close();
    }

    /// True once every sender is gone or the channel is closed.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Recv, Location::caller());
        self.rx().is_closed()
    }

    /// True if no message is queued.
    pub fn is_empty(&self) -> bool {
        self.rx().is_empty()
    }

    /// Queued messages.
    pub fn len(&self) -> usize {
        self.rx().len()
    }

    /// Free slots now.
    #[track_caller]
    pub fn capacity(&self) -> usize {
        point(Op::Recv, Location::caller());
        self.rx().capacity()
    }

    /// The channel's size.
    pub fn max_capacity(&self) -> usize {
        self.rx().max_capacity()
    }
}

/// The sending half of an unbounded channel.
pub struct UnboundedSender<T> {
    /// Taken only by `drop`.
    inner: Option<tokio_mpsc::UnboundedSender<T>>,
    /// Where the channel was made, for the point after the last sender drops.
    site: &'static Location<'static>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        UnboundedSender {
            inner: Some(self.tx().clone()),
            site: self.site,
        }
    }
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.tx().fmt(f)
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        // The last sender closes the channel.
        drop_closing(self.inner.take(), |tx| tx.strong_count() == 1, self.site);
    }
}

impl<T> UnboundedSender<T> {
    /// Tokio's sender.
    fn tx(&self) -> &tokio_mpsc::UnboundedSender<T> {
        self.inner.as_ref().expect(TAKEN)
    }

    /// Sends `value`. Fails only once the receiver is gone.
    #[track_caller]
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        point(Op::Send, Location::caller());
        self.tx().send(value)
    }

    /// Waits until the receiver is dropped or closed.
    #[track_caller]
    pub fn closed(&self) -> impl Future<Output = ()> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            self.tx().closed().await
        }
    }

    /// True if the receiver is dropped or closed.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Send, Location::caller());
        self.tx().is_closed()
    }

    /// True if both senders feed the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.tx().same_channel(other.tx())
    }

    /// A sender that doesn't keep the channel open.
    pub fn downgrade(&self) -> WeakUnboundedSender<T> {
        WeakUnboundedSender {
            inner: self.tx().downgrade(),
            site: self.site,
        }
    }
}

/// An unbounded sender that doesn't keep the channel open.
pub struct WeakUnboundedSender<T> {
    inner: tokio_mpsc::WeakUnboundedSender<T>,
    site: &'static Location<'static>,
}

impl<T> Clone for WeakUnboundedSender<T> {
    fn clone(&self) -> Self {
        WeakUnboundedSender {
            inner: self.inner.clone(),
            site: self.site,
        }
    }
}

impl<T> fmt::Debug for WeakUnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> WeakUnboundedSender<T> {
    /// A sender, if the channel is still open.
    pub fn upgrade(&self) -> Option<UnboundedSender<T>> {
        self.inner.upgrade().map(|inner| UnboundedSender {
            inner: Some(inner),
            site: self.site,
        })
    }
}

/// The receiving half of an unbounded channel.
pub struct UnboundedReceiver<T> {
    /// Taken only by `drop`.
    inner: Option<tokio_mpsc::UnboundedReceiver<T>>,
    /// Where the channel was made, for the point after the receiver drops.
    site: &'static Location<'static>,
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.rx().fmt(f)
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        drop_closing(self.inner.take(), |_| true, self.site);
    }
}

impl<T> UnboundedReceiver<T> {
    /// Tokio's receiver.
    fn rx(&self) -> &tokio_mpsc::UnboundedReceiver<T> {
        self.inner.as_ref().expect(TAKEN)
    }

    /// Tokio's receiver, mutably.
    fn rx_mut(&mut self) -> &mut tokio_mpsc::UnboundedReceiver<T> {
        self.inner.as_mut().expect(TAKEN)
    }

    /// Waits for the next message. `None` once every sender is gone.
    #[track_caller]
    pub fn recv(&mut self) -> impl Future<Output = Option<T>> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Recv, site);
            self.rx_mut().recv().await
        }
    }

    /// Takes a message if one is queued now.
    #[track_caller]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        point(Op::Recv, Location::caller());
        self.rx_mut().try_recv()
    }

    /// Polls for the next message. Each call is a point.
    #[track_caller]
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        point(Op::Recv, Location::caller());
        self.rx_mut().poll_recv(cx)
    }

    /// Stops new sends. Queued messages can still be received.
    #[track_caller]
    pub fn close(&mut self) {
        point(Op::Recv, Location::caller());
        self.rx_mut().close();
    }

    /// True once every sender is gone or the channel is closed.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Recv, Location::caller());
        self.rx().is_closed()
    }

    /// True if no message is queued.
    pub fn is_empty(&self) -> bool {
        self.rx().is_empty()
    }

    /// Queued messages.
    pub fn len(&self) -> usize {
        self.rx().len()
    }
}
