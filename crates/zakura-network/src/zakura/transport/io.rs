//! Transport-owned framed stream handles.
//!
//! `FramedRecv` and `FramedSend` are the service-facing handles for application
//! stream frames. The transport applies each stream's declared `Stream::frame_cap`,
//! per-kind message-rate buckets, and idle freshness updates in its stream workers
//! before frames reach these handles.

use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::oneshot;
#[cfg(test)]
use tokio::time::{timeout, Duration};

use super::Frame;
use crate::zakura::regulation::FrameLease;

#[cfg(test)]
const TEST_BARRIER_TIMEOUT: Duration = Duration::from_secs(1);

/// Receive half for bounded, rate-admitted Zakura frames.
#[derive(Debug)]
pub struct FramedRecv {
    receiver: FramedReceiver,
    #[cfg(test)]
    barrier_receiver: Option<mpsc::UnboundedReceiver<oneshot::Sender<()>>>,
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
            #[cfg(test)]
            barrier_receiver: None,
        }
    }

    fn queued(receiver: mpsc::Receiver<QueuedFrame>) -> Self {
        Self {
            receiver: FramedReceiver::Queued(receiver),
            #[cfg(test)]
            barrier_receiver: None,
        }
    }

    /// Receive the next admitted frame, or `None` after the transport closes the stream.
    pub async fn recv(&mut self) -> Option<Frame> {
        #[cfg(test)]
        if let Some(barriers) = self.barrier_receiver.as_mut() {
            loop {
                tokio::select! {
                    biased;
                    frame = receive_frame(&mut self.receiver) => return frame,
                    barrier = barriers.recv() => {
                        let barrier = barrier?;
                        let _ = barrier.send(());
                    }
                }
            }
        }

        receive_frame(&mut self.receiver).await
    }
}

async fn receive_frame(receiver: &mut FramedReceiver) -> Option<Frame> {
    match receiver {
        FramedReceiver::Plain(receiver) => receiver.recv().await,
        FramedReceiver::Queued(receiver) => {
            receiver.recv().await.map(|queued| queued.into_parts().0)
        }
    }
}

/// Send half for bounded Zakura frames.
#[derive(Clone, Debug)]
pub struct FramedSend {
    sender: FramedSender,
    #[cfg(test)]
    barrier_sender: Option<mpsc::UnboundedSender<oneshot::Sender<()>>>,
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
            #[cfg(test)]
            barrier_sender: None,
        }
    }

    fn queued(sender: mpsc::Sender<QueuedFrame>) -> Self {
        Self {
            sender: FramedSender::Queued(sender),
            #[cfg(test)]
            barrier_sender: None,
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

    /// Reserve a queue slot, then create and attach a lease before an infallible send.
    ///
    /// `make_lease` is never called for a full, closed, or plain compatibility
    /// channel, so accounting ownership cannot move unless a transport-owned
    /// queue slot already belongs to this operation.
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
                let lease = make_lease();
                slot.send(QueuedFrame::leased(frame, lease));
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

    /// Wait until the receiving task has consumed every frame queued before
    /// this call and returned to its receive loop.
    #[cfg(test)]
    pub(crate) async fn barrier_for_test(&self) -> Result<(), &'static str> {
        let barriers = self
            .barrier_sender
            .as_ref()
            .ok_or("this framed sender has no test barrier")?;
        let (acknowledge, acknowledged) = oneshot::channel();
        barriers
            .send(acknowledge)
            .map_err(|_| "the framed receiver closed before the test barrier")?;
        timeout(TEST_BARRIER_TIMEOUT, acknowledged)
            .await
            .map_err(|_| "the framed receiver timed out before the test barrier")?
            .map_err(|_| "the framed receiver dropped the test barrier")
    }
}

/// Failure to queue a leased frame.
#[derive(Debug)]
pub(crate) enum LeasedSendError {
    /// The bounded transport queue has no available slot.
    Full(Frame),
    /// The transport worker has closed its receive half.
    Closed(Frame),
    /// The handle wraps a public compatibility channel without lease support.
    Unsupported(Frame),
}

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
}

/// Frame plus optional accounting held until transport write completion or drop.
#[derive(Debug)]
pub(crate) struct QueuedFrame {
    frame: Frame,
    lease: Option<FrameLease>,
}

impl QueuedFrame {
    fn plain(frame: Frame) -> Self {
        Self { frame, lease: None }
    }

    fn leased(frame: Frame, lease: FrameLease) -> Self {
        Self {
            frame,
            lease: Some(lease),
        }
    }

    /// Split the frame from its lease while keeping both owned by the caller.
    pub(crate) fn into_parts(self) -> (Frame, Option<FrameLease>) {
        (self.frame, self.lease)
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
    #[cfg(test)]
    {
        let mut framed_send = FramedSend::queued(sender);
        let mut framed_recv = FramedRecv::queued(receiver);
        let (barrier_sender, barrier_receiver) = mpsc::unbounded_channel();
        framed_send.barrier_sender = Some(barrier_sender);
        framed_recv.barrier_receiver = Some(barrier_receiver);
        (framed_send, framed_recv)
    }
    #[cfg(not(test))]
    {
        (FramedSend::queued(sender), FramedRecv::queued(receiver))
    }
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
    async fn public_framed_channel_preserves_order_capacity_and_errors() {
        let (sender, mut receiver) = framed_channel(2);
        assert_eq!(sender.capacity(), 2);
        assert_eq!(sender.max_capacity(), 2);

        sender
            .try_send(frame(1))
            .expect("the first queue slot is available");
        sender
            .try_send(frame(2))
            .expect("the second queue slot is available");
        assert_eq!(sender.capacity(), 0);
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
    async fn public_constructor_keeps_plain_channel_compatibility() {
        let (raw_sender, mut raw_receiver) = mpsc::channel(1);
        let sender = FramedSend::new(raw_sender);

        sender
            .send(frame(7))
            .await
            .expect("the plain compatibility channel is open");
        assert_eq!(raw_receiver.recv().await, Some(frame(7)));

        let (raw_sender, raw_receiver) = mpsc::channel(1);
        let mut receiver = FramedRecv::new(raw_receiver);
        raw_sender
            .send(frame(8))
            .await
            .expect("the plain compatibility channel is open");
        assert_eq!(receiver.recv().await, Some(frame(8)));
    }

    #[tokio::test]
    async fn worker_queue_holds_lease_until_the_written_envelope_drops() {
        let (sender, mut worker_receiver) = worker_framed_channel(1);
        let budget = OutstandingByteBudget::new(10);
        let mut reservation = budget
            .try_reserve_owned(10)
            .expect("the request fits the budget")
            .expect("the empty budget admits the reservation");

        sender
            .try_send_leased(frame(1), || {
                OutstandingByteReservation::transfer_all([&mut reservation], 10)
                    .expect("the reservation covers the frame")
            })
            .expect("the worker queue has a slot");
        drop(reservation);
        assert_eq!(budget.reserved(), 10);

        let queued = worker_receiver
            .recv()
            .await
            .expect("the worker receives the queued frame");
        let (received, lease) = queued.into_parts();
        assert_eq!(received, frame(1));
        assert_eq!(budget.reserved(), 10);

        drop(lease);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn dropped_worker_queue_releases_queued_leases() {
        let (sender, worker_receiver) = worker_framed_channel(1);
        let budget = OutstandingByteBudget::new(10);
        let mut reservation = budget
            .try_reserve_owned(10)
            .expect("the request fits the budget")
            .expect("the empty budget admits the reservation");
        sender
            .try_send_leased(frame(1), || {
                OutstandingByteReservation::transfer_all([&mut reservation], 10)
                    .expect("the reservation covers the frame")
            })
            .expect("the worker queue has a slot");
        drop(reservation);

        drop(worker_receiver);

        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn failed_leased_send_never_moves_accounting() {
        let (sender, _receiver) = worker_framed_channel(1);
        sender
            .try_send(frame(1))
            .expect("the only worker queue slot is available");
        let full_called = Arc::new(AtomicBool::new(false));
        let full_called_in_factory = full_called.clone();
        let result = sender.try_send_leased(frame(2), move || {
            full_called_in_factory.store(true, Ordering::SeqCst);
            FrameLease::empty_for_test()
        });
        assert!(matches!(result, Err(LeasedSendError::Full(_))));
        assert!(!full_called.load(Ordering::SeqCst));

        let (sender, receiver) = worker_framed_channel(1);
        drop(receiver);
        let closed_called = Arc::new(AtomicBool::new(false));
        let closed_called_in_factory = closed_called.clone();
        let result = sender.try_send_leased(frame(3), move || {
            closed_called_in_factory.store(true, Ordering::SeqCst);
            FrameLease::empty_for_test()
        });
        assert!(matches!(result, Err(LeasedSendError::Closed(_))));
        assert!(!closed_called.load(Ordering::SeqCst));

        let (raw_sender, _raw_receiver) = mpsc::channel(1);
        let sender = FramedSend::new(raw_sender);
        let unsupported_called = Arc::new(AtomicBool::new(false));
        let unsupported_called_in_factory = unsupported_called.clone();
        let result = sender.try_send_leased(frame(4), move || {
            unsupported_called_in_factory.store(true, Ordering::SeqCst);
            FrameLease::empty_for_test()
        });
        assert!(matches!(result, Err(LeasedSendError::Unsupported(_))));
        assert!(!unsupported_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn scaffolding_receive_releases_a_leased_frame() {
        let (sender, mut receiver) = framed_channel(1);
        let budget = OutstandingByteBudget::new(10);
        let mut reservation = budget
            .try_reserve_owned(10)
            .expect("the request fits the budget")
            .expect("the empty budget admits the reservation");
        sender
            .try_send_leased(frame(1), || {
                OutstandingByteReservation::transfer_all([&mut reservation], 10)
                    .expect("the reservation covers the frame")
            })
            .expect("the scaffolding queue has a slot");
        drop(reservation);
        assert_eq!(budget.reserved(), 10);

        assert_eq!(receiver.recv().await, Some(frame(1)));
        assert_eq!(budget.reserved(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn framed_barrier_times_out_when_receiver_stops_progressing() {
        let (sender, _receiver) = framed_channel(1);

        assert_eq!(
            sender.barrier_for_test().await,
            Err("the framed receiver timed out before the test barrier")
        );
    }
}
