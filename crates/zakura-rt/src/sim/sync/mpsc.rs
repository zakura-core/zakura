//! Tokio's `mpsc` channels with preemption points, simulator build.
//!
//! Each send, reserve, receive and `is_closed` is a point, and so is the
//! moment right after an unused permit gives its slot back. A send through a
//! reserved permit is its own point too, since it enqueues the message.

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
pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio_mpsc::channel(buffer);
    (Sender { inner: tx }, Receiver { inner: rx })
}

/// An unbounded channel, as tokio's.
pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (tx, rx) = tokio_mpsc::unbounded_channel();
    (
        UnboundedSender { inner: tx },
        UnboundedReceiver { inner: rx },
    )
}

/// The sending half of a bounded channel.
pub struct Sender<T> {
    inner: tokio_mpsc::Sender<T>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Sender<T> {
    /// Waits for capacity, then sends `value`.
    #[track_caller]
    pub fn send(&self, value: T) -> impl Future<Output = Result<(), SendError<T>>> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            self.inner.send(value).await
        }
    }

    /// Sends `value` if there's capacity now.
    #[track_caller]
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        point(Op::Send, Location::caller());
        self.inner.try_send(value)
    }

    /// Waits for a slot and holds it.
    #[track_caller]
    pub fn reserve(&self) -> impl Future<Output = Result<Permit<'_, T>, SendError<()>>> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            let inner = self.inner.reserve().await?;
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
        let inner = self.inner.try_reserve()?;
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
        let inner = self.inner.try_reserve_many(n)?;
        Ok(PermitIterator { inner, site })
    }

    /// Waits for a slot and holds it, taking the sender.
    #[track_caller]
    pub fn reserve_owned(self) -> impl Future<Output = Result<OwnedPermit<T>, SendError<()>>> {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            let inner = self.inner.reserve_owned().await?;
            Ok(OwnedPermit {
                inner: Some(inner),
                site,
            })
        }
    }

    /// Holds a slot if one is free now, taking the sender.
    #[track_caller]
    pub fn try_reserve_owned(self) -> Result<OwnedPermit<T>, TrySendError<Self>> {
        let site = Location::caller();
        point(Op::Send, site);
        match self.inner.try_reserve_owned() {
            Ok(inner) => Ok(OwnedPermit {
                inner: Some(inner),
                site,
            }),
            Err(TrySendError::Full(inner)) => Err(TrySendError::Full(Sender { inner })),
            Err(TrySendError::Closed(inner)) => Err(TrySendError::Closed(Sender { inner })),
        }
    }

    /// Waits until the receiver is dropped or closed.
    #[track_caller]
    pub fn closed(&self) -> impl Future<Output = ()> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            self.inner.closed().await
        }
    }

    /// True if the receiver is dropped or closed.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Send, Location::caller());
        self.inner.is_closed()
    }

    /// Free slots now.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// The channel's size.
    pub fn max_capacity(&self) -> usize {
        self.inner.max_capacity()
    }

    /// True if both senders feed the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }

    /// A sender that doesn't keep the channel open.
    pub fn downgrade(&self) -> WeakSender<T> {
        WeakSender {
            inner: self.inner.downgrade(),
        }
    }

    /// Live senders of this channel.
    pub fn strong_count(&self) -> usize {
        self.inner.strong_count()
    }

    /// Weak senders of this channel.
    pub fn weak_count(&self) -> usize {
        self.inner.weak_count()
    }
}

/// A bounded sender that doesn't keep the channel open.
pub struct WeakSender<T> {
    inner: tokio_mpsc::WeakSender<T>,
}

impl<T> Clone for WeakSender<T> {
    fn clone(&self) -> Self {
        WeakSender {
            inner: self.inner.clone(),
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
        self.inner.upgrade().map(|inner| Sender { inner })
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
        Sender { inner }
    }

    /// Gives the slot back without sending, and the sender with it.
    pub fn release(mut self) -> Sender<T> {
        let inner = self
            .inner
            .take()
            .expect("the slot is only taken here or in send, which consume the permit")
            .release();
        point(Op::Release, self.site);
        Sender { inner }
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
    inner: tokio_mpsc::Receiver<T>,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Receiver<T> {
    /// Waits for the next message. `None` once every sender is gone.
    #[track_caller]
    pub fn recv(&mut self) -> impl Future<Output = Option<T>> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Recv, site);
            self.inner.recv().await
        }
    }

    /// Takes a message if one is queued now.
    #[track_caller]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        point(Op::Recv, Location::caller());
        self.inner.try_recv()
    }

    /// Polls for the next message. Each call is a point.
    #[track_caller]
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        point(Op::Recv, Location::caller());
        self.inner.poll_recv(cx)
    }

    /// Stops new sends. Queued messages can still be received.
    #[track_caller]
    pub fn close(&mut self) {
        point(Op::Recv, Location::caller());
        self.inner.close();
    }

    /// True once every sender is gone or the channel is closed.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Recv, Location::caller());
        self.inner.is_closed()
    }

    /// True if no message is queued.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Queued messages.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Free slots now.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// The channel's size.
    pub fn max_capacity(&self) -> usize {
        self.inner.max_capacity()
    }
}

/// The sending half of an unbounded channel.
pub struct UnboundedSender<T> {
    inner: tokio_mpsc::UnboundedSender<T>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        UnboundedSender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> UnboundedSender<T> {
    /// Sends `value`. Fails only once the receiver is gone.
    #[track_caller]
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        point(Op::Send, Location::caller());
        self.inner.send(value)
    }

    /// Waits until the receiver is dropped or closed.
    #[track_caller]
    pub fn closed(&self) -> impl Future<Output = ()> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Send, site);
            self.inner.closed().await
        }
    }

    /// True if the receiver is dropped or closed.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Send, Location::caller());
        self.inner.is_closed()
    }

    /// True if both senders feed the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }

    /// A sender that doesn't keep the channel open.
    pub fn downgrade(&self) -> WeakUnboundedSender<T> {
        WeakUnboundedSender {
            inner: self.inner.downgrade(),
        }
    }
}

/// An unbounded sender that doesn't keep the channel open.
pub struct WeakUnboundedSender<T> {
    inner: tokio_mpsc::WeakUnboundedSender<T>,
}

impl<T> Clone for WeakUnboundedSender<T> {
    fn clone(&self) -> Self {
        WeakUnboundedSender {
            inner: self.inner.clone(),
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
        self.inner.upgrade().map(|inner| UnboundedSender { inner })
    }
}

/// The receiving half of an unbounded channel.
pub struct UnboundedReceiver<T> {
    inner: tokio_mpsc::UnboundedReceiver<T>,
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> UnboundedReceiver<T> {
    /// Waits for the next message. `None` once every sender is gone.
    #[track_caller]
    pub fn recv(&mut self) -> impl Future<Output = Option<T>> + '_ {
        let site = Location::caller();
        async move {
            point(Op::Recv, site);
            self.inner.recv().await
        }
    }

    /// Takes a message if one is queued now.
    #[track_caller]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        point(Op::Recv, Location::caller());
        self.inner.try_recv()
    }

    /// Polls for the next message. Each call is a point.
    #[track_caller]
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        point(Op::Recv, Location::caller());
        self.inner.poll_recv(cx)
    }

    /// Stops new sends. Queued messages can still be received.
    #[track_caller]
    pub fn close(&mut self) {
        point(Op::Recv, Location::caller());
        self.inner.close();
    }

    /// True once every sender is gone or the channel is closed.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Recv, Location::caller());
        self.inner.is_closed()
    }

    /// True if no message is queued.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Queued messages.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}
