//! Transport-owned framed stream handles.
//!
//! `FramedRecv` and `FramedSend` are the service-facing handles for application
//! stream frames. The transport applies each stream's declared `Stream::frame_cap`,
//! per-kind message-rate buckets, and idle freshness updates in its stream workers
//! before frames reach these handles.

use tokio::sync::mpsc;

use super::Frame;
use std::sync::Arc;

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
                while let Some(queued) = receiver.recv().await {
                    if let Some(claim) = &queued.claim {
                        if !claim.try_start() {
                            continue;
                        }
                        claim.written();
                    }
                    return Some(queued.frame);
                }
                None
            }
        }
    }
}

/// Send half for bounded Zakura frames.
#[derive(Clone, Debug)]
pub struct FramedSend {
    sender: FramedSender,
    session_resources: Option<Arc<dyn super::service::OrderedSessionResources>>,
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
            session_resources: None,
        }
    }

    fn queued(sender: mpsc::Sender<QueuedFrame>) -> Self {
        Self {
            sender: FramedSender::Queued(sender),
            session_resources: None,
        }
    }

    /// Keep service admission charged while application senders still own the session.
    pub(crate) fn with_session_resources(
        mut self,
        resources: Option<Arc<dyn super::service::OrderedSessionResources>>,
    ) -> Self {
        self.session_resources = resources;
        self
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

    /// Reserve queue space before encoding a response or sharing its ownership.
    pub(crate) fn try_reserve_guarded(&self) -> Result<GuardedFrameSlot<'_>, GuardedReserveError> {
        let FramedSender::Queued(sender) = &self.sender else {
            return Err(GuardedReserveError::Unsupported);
        };
        sender
            .try_reserve()
            .map(|permit| GuardedFrameSlot { permit, sender })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(()) => GuardedReserveError::Full,
                mpsc::error::TrySendError::Closed(()) => GuardedReserveError::Closed,
            })
    }

    /// Wait for queue space. Cancellation leaves response ownership with the caller.
    pub(crate) async fn reserve_guarded(
        &self,
    ) -> Result<GuardedFrameSlot<'_>, GuardedReserveError> {
        let FramedSender::Queued(sender) = &self.sender else {
            return Err(GuardedReserveError::Unsupported);
        };
        sender
            .reserve()
            .await
            .map(|permit| GuardedFrameSlot { permit, sender })
            .map_err(|_| GuardedReserveError::Closed)
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

/// One reserved queue slot. Dropping it returns capacity without sending a frame.
#[derive(Debug)]
pub(crate) struct GuardedFrameSlot<'a> {
    permit: mpsc::Permit<'a, QueuedFrame>,
    sender: &'a mpsc::Sender<QueuedFrame>,
}

impl GuardedFrameSlot<'_> {
    /// Transfer a validated frame and its ownership to the reserved queue slot.
    pub(crate) fn send(self, frame: Frame, guard: FrameGuard) {
        self.permit.send(QueuedFrame::guarded(frame, guard));
    }

    /// Publish a request whose ownership must be claimed before its first byte.
    /// A false result requires explicit settlement after publication unlocks:
    /// Tokio can retain a send made through a permit after its receiver drops.
    pub(crate) fn send_request(self, frame: Frame, claim: Arc<dyn FrameWriteClaim>) -> bool {
        if self.sender.is_closed() {
            return false;
        }
        self.permit.send(QueuedFrame {
            frame,
            guard: None,
            claim: Some(claim),
        });
        !self.sender.is_closed()
    }
}

/// Arbitrates an unwritten request against expiry and reset. Dropping a started
/// but unfinished claim must retire the stream session before another write.
pub(crate) trait FrameWriteClaim: std::fmt::Debug + Send + Sync {
    /// Atomically claim current ownership, or skip this obsolete frame.
    fn try_start(&self) -> bool;
    /// Mark the complete frame accepted by the transport write.
    fn written(&self);
}

/// Failure to reserve space for a guarded response.
#[derive(Debug)]
pub(crate) enum GuardedReserveError {
    /// The bounded transport queue has no free slot.
    Full,
    /// The transport worker has closed its receive half.
    Closed,
    /// This handle wraps a compatibility channel without guard support.
    Unsupported,
}

/// Shared service ownership held until a frame finishes its application write.
///
/// This is a completion guard, not a byte budget or acknowledgement of delivery.
/// QUIC owns its own bounded send buffers after the write accepts the frame.
#[derive(Clone, Debug)]
pub(crate) struct FrameGuard {
    _owner: Arc<dyn std::fmt::Debug + Send + Sync>,
}

impl FrameGuard {
    /// Share an existing work owner without acquiring more capacity.
    pub(crate) fn new<T: std::fmt::Debug + Send + Sync + 'static>(owner: Arc<T>) -> Self {
        Self { _owner: owner }
    }
}

/// Frame plus optional response ownership retained through its transport write.
#[derive(Debug)]
pub(crate) struct QueuedFrame {
    frame: Frame,
    guard: Option<FrameGuard>,
    claim: Option<Arc<dyn FrameWriteClaim>>,
}

impl QueuedFrame {
    fn plain(frame: Frame) -> Self {
        Self {
            frame,
            guard: None,
            claim: None,
        }
    }

    fn guarded(frame: Frame, guard: FrameGuard) -> Self {
        Self {
            frame,
            guard: Some(guard),
            claim: None,
        }
    }

    /// Split the frame from its guard while retaining both in the caller.
    pub(crate) fn into_parts(self) -> (Frame, Option<FrameGuard>) {
        (self.frame, self.guard)
    }

    /// Run the transport write while retaining this frame's guard.
    pub(crate) async fn write_with<E, F, Fut>(self, write: F) -> Result<(), E>
    where
        F: FnOnce(Frame) -> Fut,
        Fut: std::future::Future<Output = Result<(), E>>,
    {
        let Self {
            frame,
            guard: _guard,
            claim,
        } = self;
        if claim.as_ref().is_some_and(|claim| !claim.try_start()) {
            return Ok(());
        }
        write(frame).await?;
        if let Some(claim) = &claim {
            claim.written();
        }
        Ok(())
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
    use std::sync::Arc;

    use super::*;
    use crate::zakura::regulation::SlotBudget;

    fn frame(message_type: u16) -> Frame {
        Frame {
            message_type,
            flags: 0,
            payload: vec![u8::try_from(message_type).unwrap_or(u8::MAX); 10],
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
    async fn queued_frame_holds_guard_until_transport_consumes_it() {
        let (sender, mut receiver) = worker_framed_channel(1);
        let budget = SlotBudget::new(1).unwrap();
        let reservation = Arc::new(budget.try_reserve().expect("the producer is free"));

        sender
            .try_reserve_guarded()
            .expect("the worker queue has a slot")
            .send(frame(1), FrameGuard::new(reservation.clone()));
        drop(reservation);
        assert_eq!(budget.reserved(), 1);

        let queued = receiver.recv().await.expect("worker receives the frame");
        let (received, guard) = queued.into_parts();
        assert_eq!(received, frame(1));
        assert_eq!(budget.reserved(), 1);

        drop(guard);
        assert_eq!(budget.reserved(), 0);
    }

    #[tokio::test]
    async fn queued_frame_holds_guard_while_write_is_pending() {
        let (sender, mut receiver) = worker_framed_channel(1);
        let budget = SlotBudget::new(1).unwrap();
        let reservation = Arc::new(budget.try_reserve().expect("the producer is free"));
        sender
            .try_reserve_guarded()
            .expect("the worker queue has a slot")
            .send(frame(1), FrameGuard::new(reservation.clone()));
        drop(reservation);
        let queued = receiver.recv().await.expect("worker receives the frame");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();

        let write = tokio::spawn(queued.write_with(move |_frame| async move {
            let _ = started_tx.send(());
            let _ = finish_rx.await;
            Ok::<_, std::convert::Infallible>(())
        }));
        started_rx.await.expect("the write reaches its wait point");
        assert_eq!(budget.reserved(), 1);

        let _ = finish_tx.send(());
        write
            .await
            .expect("the write task should not panic")
            .unwrap();
        assert_eq!(budget.reserved(), 0);
    }

    #[tokio::test]
    async fn cancelling_pending_write_releases_guard() {
        let (sender, mut receiver) = worker_framed_channel(1);
        let budget = SlotBudget::new(1).unwrap();
        let reservation = Arc::new(budget.try_reserve().expect("the producer is free"));
        sender
            .try_reserve_guarded()
            .expect("the worker queue has a slot")
            .send(frame(1), FrameGuard::new(reservation.clone()));
        drop(reservation);
        let queued = receiver.recv().await.expect("worker receives the frame");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let write = tokio::spawn(queued.write_with(move |_frame| async move {
            let _ = started_tx.send(());
            std::future::pending::<Result<(), std::convert::Infallible>>().await
        }));
        started_rx.await.expect("the write reaches its wait point");
        assert_eq!(budget.reserved(), 1);

        write.abort();
        assert!(write
            .await
            .expect_err("the write should be cancelled")
            .is_cancelled());
        assert_eq!(budget.reserved(), 0);
    }

    #[tokio::test]
    async fn waiting_guarded_send_transfers_only_after_capacity_and_holds_through_write() {
        let (sender, mut receiver) = worker_framed_channel(1);
        sender.try_send(frame(1)).expect("filler fits");
        let budget = SlotBudget::new(1).unwrap();
        let reservation = Arc::new(budget.try_reserve().expect("the producer is free"));
        let mut pending = Box::pin(sender.reserve_guarded());
        assert!(futures::poll!(&mut pending).is_pending());
        assert_eq!(Arc::strong_count(&reservation), 1);
        assert_eq!(budget.reserved(), 1);
        drop(receiver.recv().await.expect("filler queued"));
        pending
            .await
            .expect("the freed slot admits the frame")
            .send(frame(2), FrameGuard::new(reservation.clone()));
        drop(reservation);
        assert_eq!(budget.reserved(), 1);
        let queued = receiver.recv().await.expect("guarded frame queued");
        let mut write = Box::pin(queued.write_with(|received| async move {
            assert_eq!(received, frame(2));
            std::future::pending::<Result<(), std::convert::Infallible>>().await
        }));
        assert!(futures::poll!(&mut write).is_pending());
        assert_eq!(budget.reserved(), 1);
        drop(write);
        assert_eq!(budget.reserved(), 0);
    }

    #[tokio::test]
    async fn cancelling_or_closing_a_guarded_queue_wait_keeps_the_callers_reservation() {
        let (sender, mut receiver) = worker_framed_channel(1);
        sender.try_send(frame(1)).expect("filler fits");
        let budget = SlotBudget::new(1).unwrap();
        let reservation = Arc::new(budget.try_reserve().expect("the producer is free"));
        let mut pending = Box::pin(sender.reserve_guarded());
        assert!(futures::poll!(&mut pending).is_pending());
        drop(pending);
        drop(receiver.recv().await.expect("filler queued"));
        assert_eq!(Arc::strong_count(&reservation), 1);
        assert_eq!(budget.reserved(), 1);
        drop(receiver);
        assert!(matches!(
            sender.reserve_guarded().await,
            Err(GuardedReserveError::Closed)
        ));
        assert_eq!(Arc::strong_count(&reservation), 1);
        assert_eq!(budget.reserved(), 1);
        drop(reservation);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn guarded_reservation_reports_full_closed_and_unsupported_queues() {
        let (sender, receiver) = worker_framed_channel(1);
        sender.try_send(frame(1)).unwrap();
        assert!(matches!(
            sender.try_reserve_guarded(),
            Err(GuardedReserveError::Full)
        ));
        drop(receiver);
        assert!(matches!(
            sender.try_reserve_guarded(),
            Err(GuardedReserveError::Closed)
        ));

        let (raw_sender, _raw_receiver) = mpsc::channel(1);
        let sender = FramedSend::new(raw_sender);
        assert!(matches!(
            sender.try_reserve_guarded(),
            Err(GuardedReserveError::Unsupported)
        ));
    }

    #[test]
    fn dropping_reserved_slot_returns_queue_capacity() {
        let (sender, _receiver) = worker_framed_channel(1);
        let slot = sender.try_reserve_guarded().unwrap();
        assert_eq!(sender.capacity(), 0);
        drop(slot);
        assert_eq!(sender.capacity(), 1);
    }

    #[test]
    fn dropping_worker_queue_releases_queued_guard() {
        let (sender, receiver) = worker_framed_channel(1);
        let budget = SlotBudget::new(1).unwrap();
        let reservation = Arc::new(budget.try_reserve().expect("the producer is free"));
        sender
            .try_reserve_guarded()
            .expect("the worker queue has a slot")
            .send(frame(1), FrameGuard::new(reservation.clone()));
        drop(reservation);
        assert_eq!(budget.reserved(), 1);

        drop(receiver);

        assert_eq!(budget.reserved(), 0);
    }
    #[tokio::test]
    async fn quic_backpressure_holds_producer_and_preserves_another_stream() {
        use crate::zakura::testkit::LocalEndpointFactory;
        use iroh::{
            endpoint::{Connection, TransportConfig, VarInt},
            protocol::{AcceptError, ProtocolHandler, Router},
        };
        use std::time::Duration;

        #[derive(Debug)]
        struct AcceptConnection(mpsc::Sender<Connection>);
        impl ProtocolHandler for AcceptConnection {
            async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
                let _ = self.0.send(connection).await;
                Ok(())
            }
        }

        const ALPN: &[u8] = b"/zakura/test/producer-backpressure";
        // Scale down the windows so a single bounded frame reaches flow control.
        let transport_config = || {
            let mut config = TransportConfig::default();
            config
                .stream_receive_window(VarInt::from_u32(16 * 1024))
                .receive_window(VarInt::from_u32(128 * 1024))
                .send_window(128 * 1024);
            config
        };
        let server = LocalEndpointFactory::with_transport_config(transport_config())
            .endpoint(92_001)
            .await
            .unwrap();
        let client = LocalEndpointFactory::with_transport_config(transport_config())
            .endpoint(92_002)
            .await
            .unwrap();
        let (accepted, mut incoming) = mpsc::channel(1);
        let router = Router::builder(server)
            .accept(ALPN, AcceptConnection(accepted))
            .spawn();
        let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
        let connection = client.connect(address, ALPN).await.unwrap();
        let remote = tokio::time::timeout(Duration::from_secs(5), incoming.recv())
            .await
            .unwrap()
            .unwrap();
        let (mut send, _recv) = connection.open_bi().await.unwrap();
        let producer = SlotBudget::new(1).unwrap();
        let owner = Arc::new(producer.try_reserve().unwrap());
        let (queue, mut writer) = worker_framed_channel(1);
        queue.try_reserve_guarded().unwrap().send(
            Frame {
                message_type: 1,
                flags: 0,
                payload: vec![0; 2_000_001],
            },
            FrameGuard::new(owner.clone()),
        );
        drop(owner);
        let queued = writer.recv().await.unwrap();
        let mut write = tokio::spawn(queued.write_with(move |frame| async move {
            send.write_all(&frame.payload).await.unwrap();
            send.finish().unwrap();
            Ok::<_, std::convert::Infallible>(())
        }));
        let (_remote_send, mut slow_read) =
            tokio::time::timeout(Duration::from_secs(5), remote.accept_bi())
                .await
                .unwrap()
                .unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(100), &mut write)
            .await
            .is_err());
        assert!(
            producer.try_reserve().is_none(),
            "a pending QUIC write retains the producer"
        );

        let (mut other_send, _other_recv) = connection.open_bi().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            other_send.write_all(b"progress").await.unwrap();
            other_send.finish().unwrap();
            let (_send, mut recv) = remote.accept_bi().await.unwrap();
            assert_eq!(recv.read_to_end(8).await.unwrap(), b"progress");
        })
        .await
        .expect("the blocked stream does not consume all connection credit");
        assert!(producer.try_reserve().is_none());
        tokio::time::timeout(Duration::from_secs(5), async {
            assert_eq!(
                slow_read.read_to_end(2_000_001).await.unwrap().len(),
                2_000_001
            );
            write.await.unwrap().unwrap();
        })
        .await
        .expect("draining the peer resumes the write");
        assert!(producer.try_reserve().is_some());
        connection.close(0u32.into(), b"done");
        client.close().await;
        router.shutdown().await.unwrap();
    }
}
