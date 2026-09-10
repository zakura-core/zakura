//! Transport-owned framed stream handles.
//!
//! `FramedRecv` and `FramedSend` are the service-facing handles for application
//! stream frames. The transport applies each stream's declared `Stream::frame_cap`,
//! per-kind message-rate buckets, and idle freshness updates in its stream workers
//! before frames reach these handles.

use tokio::sync::mpsc;

use super::Frame;
use crate::zakura::regulation::FrameLease;

/// Receive half for bounded, rate-admitted Zakura frames.
#[derive(Debug)]
pub struct FramedRecv {
    receiver: FramedReceiver,
}

#[derive(Debug)]
enum FramedReceiver {
    Plain(mpsc::Receiver<Frame>),
    Queued(mpsc::Receiver<QueuedFrame>),
}

impl FramedRecv {
    /// Wrap a bounded frame receiver.
    pub fn new(receiver: mpsc::Receiver<Frame>) -> Self {
        Self {
            receiver: FramedReceiver::Plain(receiver),
        }
    }

    fn queued(receiver: mpsc::Receiver<QueuedFrame>) -> Self {
        Self {
            receiver: FramedReceiver::Queued(receiver),
        }
    }

    /// Receive the next admitted frame, or `None` after the transport closes the stream.
    pub async fn recv(&mut self) -> Option<Frame> {
        match &mut self.receiver {
            FramedReceiver::Plain(receiver) => receiver.recv().await,
            FramedReceiver::Queued(receiver) => {
                receiver.recv().await.map(|queued| queued.into_parts().0)
            }
        }
    }
}

/// Send half for bounded Zakura frames.
#[derive(Clone, Debug)]
pub struct FramedSend {
    sender: FramedSender,
}

#[derive(Clone, Debug)]
enum FramedSender {
    Plain(mpsc::Sender<Frame>),
    Queued(mpsc::Sender<QueuedFrame>),
}

impl FramedSend {
    /// Wrap a bounded frame sender.
    pub fn new(sender: mpsc::Sender<Frame>) -> Self {
        Self {
            sender: FramedSender::Plain(sender),
        }
    }

    fn queued(sender: mpsc::Sender<QueuedFrame>) -> Self {
        Self {
            sender: FramedSender::Queued(sender),
        }
    }

    /// Queue a frame for transport-owned encoding and writing.
    pub async fn send(&self, frame: Frame) -> Result<(), mpsc::error::SendError<Frame>> {
        match &self.sender {
            FramedSender::Plain(sender) => sender.send(frame).await,
            FramedSender::Queued(sender) => sender
                .send(QueuedFrame::plain(frame))
                .await
                .map_err(|error| mpsc::error::SendError(error.0.into_parts().0)),
        }
    }

    /// Try to queue a frame without waiting for capacity.
    pub fn try_send(&self, frame: Frame) -> Result<(), mpsc::error::TrySendError<Frame>> {
        match &self.sender {
            FramedSender::Plain(sender) => sender.try_send(frame),
            FramedSender::Queued(sender) => sender
                .try_send(QueuedFrame::plain(frame))
                .map_err(map_queued_try_send_error),
        }
    }

    /// Reserve a queue slot before attaching an outstanding-byte lease.
    ///
    /// `make_lease` is called only after the transport owns a queue slot. This
    /// prevents accounting from moving to the transport when the queue is full
    /// or closed.
    #[allow(dead_code)] // consumed by the GetBlocks policy in the stacked PR
    pub(crate) fn try_send_leased(
        &self,
        frame: Frame,
        make_lease: impl FnOnce() -> FrameLease,
    ) -> Result<(), LeasedSendError> {
        let FramedSender::Queued(sender) = &self.sender else {
            return Err(LeasedSendError::Unsupported(frame));
        };

        match sender.try_reserve() {
            Ok(slot) => {
                slot.send(QueuedFrame::leased(frame, make_lease()));
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(())) => Err(LeasedSendError::Full(frame)),
            Err(mpsc::error::TrySendError::Closed(())) => Err(LeasedSendError::Closed(frame)),
        }
    }

    /// Current free slots in the bounded transport queue.
    pub fn capacity(&self) -> usize {
        match &self.sender {
            FramedSender::Plain(sender) => sender.capacity(),
            FramedSender::Queued(sender) => sender.capacity(),
        }
    }

    /// Total slots in the bounded transport queue.
    pub fn max_capacity(&self) -> usize {
        match &self.sender {
            FramedSender::Plain(sender) => sender.max_capacity(),
            FramedSender::Queued(sender) => sender.max_capacity(),
        }
    }
}

/// Failure to queue a leased frame.
#[derive(Debug)]
#[allow(dead_code)] // consumed by the GetBlocks policy in the stacked PR
pub(crate) enum LeasedSendError {
    /// The bounded transport queue has no free slot.
    Full(Frame),
    /// The transport worker has closed its receive half.
    Closed(Frame),
    /// This handle wraps a compatibility channel without lease support.
    Unsupported(Frame),
}

#[allow(dead_code)] // consumed by the GetBlocks policy in the stacked PR
impl LeasedSendError {
    /// Recover the frame that was not queued.
    pub(crate) fn into_frame(self) -> Frame {
        match self {
            Self::Full(frame) | Self::Closed(frame) | Self::Unsupported(frame) => frame,
        }
    }

    /// Return whether the queue was temporarily full.
    pub(crate) fn is_full(&self) -> bool {
        matches!(self, Self::Full(_))
    }

    /// Return whether the worker permanently closed the queue.
    pub(crate) fn is_closed(&self) -> bool {
        matches!(self, Self::Closed(_))
    }
}

/// Frame plus optional byte ownership retained through its transport write.
#[derive(Debug)]
pub(crate) struct QueuedFrame {
    frame: Frame,
    lease: Option<FrameLease>,
}

impl QueuedFrame {
    fn plain(frame: Frame) -> Self {
        Self { frame, lease: None }
    }

    #[allow(dead_code)] // consumed through `try_send_leased` in the stacked PR
    fn leased(frame: Frame, lease: FrameLease) -> Self {
        Self {
            frame,
            lease: Some(lease),
        }
    }

    /// Split the frame from its lease while retaining both in the caller.
    pub(crate) fn into_parts(self) -> (Frame, Option<FrameLease>) {
        (self.frame, self.lease)
    }

    /// Run the transport write while retaining this frame's lease.
    pub(crate) async fn write_with<T, F, Fut>(self, write: F) -> T
    where
        F: FnOnce(Frame) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let (frame, _lease) = self.into_parts();
        write(frame).await
    }
}

/// Transport-worker receive half for outbound queued frames.
#[derive(Debug)]
pub(crate) struct FramedWorkerRecv {
    receiver: mpsc::Receiver<QueuedFrame>,
}

impl FramedWorkerRecv {
    /// Receive the next outbound queued frame.
    pub(crate) async fn recv(&mut self) -> Option<QueuedFrame> {
        self.receiver.recv().await
    }
}

/// Build the queue between a service and its transport worker.
pub(crate) fn worker_framed_channel(depth: usize) -> (FramedSend, FramedWorkerRecv) {
    let (sender, receiver) = mpsc::channel(depth);
    (FramedSend::queued(sender), FramedWorkerRecv { receiver })
}

/// Build a bounded in-memory framed channel for scaffolding and tests.
pub fn framed_channel(depth: usize) -> (FramedSend, FramedRecv) {
    let (sender, receiver) = mpsc::channel(depth);
    (FramedSend::queued(sender), FramedRecv::queued(receiver))
}

fn map_queued_try_send_error(
    error: mpsc::error::TrySendError<QueuedFrame>,
) -> mpsc::error::TrySendError<Frame> {
    match error {
        mpsc::error::TrySendError::Full(queued) => {
            mpsc::error::TrySendError::Full(queued.into_parts().0)
        }
        mpsc::error::TrySendError::Closed(queued) => {
            mpsc::error::TrySendError::Closed(queued.into_parts().0)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use super::*;
    use crate::zakura::regulation::{OutstandingByteBudget, OutstandingByteReservation};

    fn frame(message_type: u16) -> Frame {
        Frame {
            message_type,
            flags: 0,
            payload: vec![u8::try_from(message_type).unwrap_or(u8::MAX)],
        }
    }

    #[tokio::test]
    async fn public_channel_preserves_order_capacity_and_errors() {
        let (sender, mut receiver) = framed_channel(2);
        assert_eq!(sender.capacity(), 2);
        assert_eq!(sender.max_capacity(), 2);

        sender.try_send(frame(1)).expect("first slot is free");
        sender.try_send(frame(2)).expect("second slot is free");
        assert!(matches!(
            sender.try_send(frame(3)),
            Err(mpsc::error::TrySendError::Full(Frame {
                message_type: 3,
                ..
            }))
        ));
        assert_eq!(receiver.recv().await, Some(frame(1)));
        assert_eq!(receiver.recv().await, Some(frame(2)));

        drop(receiver);
        assert!(matches!(
            sender.try_send(frame(4)),
            Err(mpsc::error::TrySendError::Closed(Frame {
                message_type: 4,
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn public_constructors_keep_plain_channel_compatibility() {
        let (raw_sender, mut raw_receiver) = mpsc::channel(1);
        let sender = FramedSend::new(raw_sender);
        sender.send(frame(7)).await.expect("plain channel is open");
        assert_eq!(raw_receiver.recv().await, Some(frame(7)));

        let (raw_sender, raw_receiver) = mpsc::channel(1);
        let mut receiver = FramedRecv::new(raw_receiver);
        raw_sender
            .send(frame(8))
            .await
            .expect("plain channel is open");
        assert_eq!(receiver.recv().await, Some(frame(8)));
    }

    #[tokio::test]
    async fn queued_frame_holds_lease_until_transport_consumes_it() {
        let (sender, mut receiver) = worker_framed_channel(1);
        let budget = OutstandingByteBudget::new(10);
        let mut reservation = budget
            .try_reserve(10)
            .expect("the frame fits the budget")
            .expect("the budget has capacity");

        sender
            .try_send_leased(frame(1), || {
                OutstandingByteReservation::transfer_to_frame([&mut reservation], 10)
                    .expect("the reservation covers the frame")
            })
            .expect("the worker queue has a slot");
        drop(reservation);
        assert_eq!(budget.reserved(), 10);

        let queued = receiver.recv().await.expect("worker receives the frame");
        let (received, lease) = queued.into_parts();
        assert_eq!(received, frame(1));
        assert_eq!(budget.reserved(), 10);

        drop(lease);
        assert_eq!(budget.reserved(), 0);
    }

    #[tokio::test]
    async fn queued_frame_holds_lease_while_write_is_pending() {
        let (sender, mut receiver) = worker_framed_channel(1);
        let budget = OutstandingByteBudget::new(10);
        let mut reservation = budget
            .try_reserve(10)
            .expect("the frame fits the budget")
            .expect("the budget has capacity");
        sender
            .try_send_leased(frame(1), || {
                OutstandingByteReservation::transfer_to_frame([&mut reservation], 10)
                    .expect("the reservation covers the frame")
            })
            .expect("the worker queue has a slot");
        drop(reservation);
        let queued = receiver.recv().await.expect("worker receives the frame");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();

        let write = tokio::spawn(queued.write_with(move |_frame| async move {
            let _ = started_tx.send(());
            let _ = finish_rx.await;
        }));
        started_rx.await.expect("the write reaches its wait point");
        assert_eq!(budget.reserved(), 10);

        let _ = finish_tx.send(());
        write.await.expect("the write task should not panic");
        assert_eq!(budget.reserved(), 0);
    }

    #[tokio::test]
    async fn cancelling_pending_write_releases_lease() {
        let (sender, mut receiver) = worker_framed_channel(1);
        let budget = OutstandingByteBudget::new(10);
        let mut reservation = budget
            .try_reserve(10)
            .expect("the frame fits the budget")
            .expect("the budget has capacity");
        sender
            .try_send_leased(frame(1), || {
                OutstandingByteReservation::transfer_to_frame([&mut reservation], 10)
                    .expect("the reservation covers the frame")
            })
            .expect("the worker queue has a slot");
        drop(reservation);
        let queued = receiver.recv().await.expect("worker receives the frame");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let write = tokio::spawn(queued.write_with(move |_frame| async move {
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));
        started_rx.await.expect("the write reaches its wait point");
        assert_eq!(budget.reserved(), 10);

        write.abort();
        assert!(write
            .await
            .expect_err("the write should be cancelled")
            .is_cancelled());
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn failed_leased_send_does_not_create_a_lease() {
        let (sender, _receiver) = worker_framed_channel(1);
        sender.try_send(frame(1)).expect("the queue slot is free");
        let full_called = Arc::new(AtomicBool::new(false));
        let called_by_factory = full_called.clone();

        let result = sender.try_send_leased(frame(2), move || {
            called_by_factory.store(true, Ordering::SeqCst);
            FrameLease::empty_for_test()
        });

        assert!(matches!(result, Err(LeasedSendError::Full(_))));
        assert!(!full_called.load(Ordering::SeqCst));

        let (sender, receiver) = worker_framed_channel(1);
        drop(receiver);
        let closed_called = Arc::new(AtomicBool::new(false));
        let called_by_factory = closed_called.clone();
        let result = sender.try_send_leased(frame(3), move || {
            called_by_factory.store(true, Ordering::SeqCst);
            FrameLease::empty_for_test()
        });
        assert!(matches!(result, Err(LeasedSendError::Closed(_))));
        assert!(!closed_called.load(Ordering::SeqCst));

        let (raw_sender, _raw_receiver) = mpsc::channel(1);
        let sender = FramedSend::new(raw_sender);
        let unsupported_called = Arc::new(AtomicBool::new(false));
        let called_by_factory = unsupported_called.clone();
        let result = sender.try_send_leased(frame(4), move || {
            called_by_factory.store(true, Ordering::SeqCst);
            FrameLease::empty_for_test()
        });
        assert!(matches!(result, Err(LeasedSendError::Unsupported(_))));
        assert!(!unsupported_called.load(Ordering::SeqCst));
    }

    #[test]
    fn dropping_worker_queue_releases_queued_lease() {
        let (sender, receiver) = worker_framed_channel(1);
        let budget = OutstandingByteBudget::new(10);
        let mut reservation = budget
            .try_reserve(10)
            .expect("the frame fits the budget")
            .expect("the budget has capacity");
        sender
            .try_send_leased(frame(1), || {
                OutstandingByteReservation::transfer_to_frame([&mut reservation], 10)
                    .expect("the reservation covers the frame")
            })
            .expect("the worker queue has a slot");
        drop(reservation);
        assert_eq!(budget.reserved(), 10);

        drop(receiver);

        assert_eq!(budget.reserved(), 0);
    }
}
