//! Transport-owned framed stream handles.
//!
//! `FramedRecv` and `FramedSend` are the service-facing handles for application
//! stream frames. The transport applies each stream's declared `Stream::frame_cap`,
//! per-kind message-rate buckets, and idle freshness updates in its stream workers
//! before frames reach these handles.
//!
//! A persistent service session closes after every application stream handle is
//! dropped and each member's queued writes finish. A retained receiver or sender
//! clone keeps the session alive. Transport failures still cancel all members.
//! After an application receiver closes, incoming frames still pass transport
//! checks and are discarded until the session retires.

use zakura_rt::sync::mpsc;

use super::{
    lifetime::{frame_wire_bytes, FrameGate, FrameGrant},
    Frame,
};
use crate::zakura::regulation::{PrecheckSlot, ResponsePrecheck, SlotBudget, SlotPermit};
use std::sync::{Arc, OnceLock};

/// Why a persistent stream ended before local cancellation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum OrderedStreamFailure {
    RemoteClose,
    WriteTimeout,
}

/// Preserve the first transport failure before cancelling the service session.
#[derive(Clone, Debug, Default)]
pub(crate) struct OrderedStreamFailureCause(Arc<OnceLock<OrderedStreamFailure>>);

impl OrderedStreamFailureCause {
    pub(crate) fn record(&self, failure: OrderedStreamFailure) {
        let _ = self.0.set(failure);
    }

    pub(crate) fn get(&self) -> Option<OrderedStreamFailure> {
        self.0.get().copied()
    }
}

/// One raw inbound frame and its admission, from the reader's allocation on.
#[derive(Debug)]
pub(crate) struct AdmittedFrame {
    pub(crate) frame: Frame,
    pub(crate) grant: FrameGrant,
}

/// Receive half for bounded, rate-admitted Zakura frames.
///
/// A transport-read frame stays charged after [`Self::recv`] returns it, so its
/// bytes stay funded while the service decodes it. The next receive, or dropping
/// this handle, returns them. A service that retains a frame longer owns that
/// retention through its own accounting.
#[derive(Debug)]
pub struct FramedRecv {
    receiver: FramedReceiver,
    failure_cause: Option<OrderedStreamFailureCause>,
    precheck: Option<PrecheckSlot>,
    /// The admission of the frame this handle returned last.
    returned: Option<FrameGrant>,
}

#[derive(Debug)]
enum FramedReceiver {
    Plain(mpsc::Receiver<Frame>),
    Queued(mpsc::UnboundedReceiver<QueuedFrame>),
    Admitted(mpsc::Receiver<AdmittedFrame>),
}

impl FramedRecv {
    /// Wrap a bounded frame receiver.
    pub fn new(receiver: mpsc::Receiver<Frame>) -> Self {
        Self {
            receiver: FramedReceiver::Plain(receiver),
            failure_cause: None,
            precheck: None,
            returned: None,
        }
    }

    /// Wrap the queue from a stream's transport reader.
    pub(crate) fn admitted(receiver: mpsc::Receiver<AdmittedFrame>) -> Self {
        Self {
            receiver: FramedReceiver::Admitted(receiver),
            failure_cause: None,
            precheck: None,
            returned: None,
        }
    }

    fn queued(receiver: mpsc::UnboundedReceiver<QueuedFrame>) -> Self {
        Self {
            receiver: FramedReceiver::Queued(receiver),
            failure_cause: None,
            precheck: None,
            returned: None,
        }
    }

    pub(crate) fn with_failure_cause(mut self, failure_cause: OrderedStreamFailureCause) -> Self {
        self.failure_cause = Some(failure_cause);
        self
    }

    /// Share the reader's precheck slot, so the service can attach one.
    pub(crate) fn with_precheck(mut self, precheck: PrecheckSlot) -> Self {
        self.precheck = Some(precheck);
        self
    }

    /// Check every response header against `precheck` before the reader
    /// allocates its payload. Attach it before the first receive call.
    ///
    /// Returns false if this stream has no transport reader, such as an
    /// in-process channel, or if the service already started receiving or
    /// attached a precheck.
    pub(crate) fn attach_precheck(&self, precheck: Arc<dyn ResponsePrecheck>) -> bool {
        self.precheck
            .as_ref()
            .is_some_and(|slot| slot.attach(precheck))
    }

    /// The transport observer of this stream's reader, if it has one.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn stream_tap(&self) -> Option<&Arc<super::StreamTap>> {
        self.precheck.as_ref().and_then(PrecheckSlot::tap)
    }

    /// Take the tag of the frame [`Self::recv`] or [`Self::try_recv`] is
    /// returning, so the regulation events it causes name it. Both call it
    /// once per frame they return, and never for a frame they skip.
    #[cfg(any(test, zakura_sim))]
    fn take_tag(&self) {
        if let Some(tap) = self.stream_tap() {
            tap.take_tag();
        }
    }

    /// Failure of any member, retained through session cancellation for service policy.
    pub(crate) fn failure(&self) -> Option<OrderedStreamFailure> {
        self.failure_cause.as_ref().and_then(|cause| cause.get())
    }

    /// Stop new ingress while preserving frames already in the bounded queue.
    pub(crate) fn close(&mut self) {
        match &mut self.receiver {
            FramedReceiver::Plain(receiver) => receiver.close(),
            FramedReceiver::Queued(receiver) => receiver.close(),
            FramedReceiver::Admitted(receiver) => receiver.close(),
        }
    }

    /// Receive an already queued frame without waiting for transport progress.
    pub(crate) fn try_recv(&mut self) -> Result<Frame, mpsc::error::TryRecvError> {
        if let Some(precheck) = &self.precheck {
            precheck.start();
        }
        self.returned = None;
        match &mut self.receiver {
            // The release arm stays a bare expression, so its build is unchanged.
            #[cfg(not(any(test, zakura_sim)))]
            FramedReceiver::Plain(receiver) => receiver.try_recv(),
            #[cfg(any(test, zakura_sim))]
            FramedReceiver::Plain(receiver) => {
                let frame = receiver.try_recv();
                if frame.is_ok() {
                    self.take_tag();
                }
                frame
            }
            FramedReceiver::Queued(receiver) => loop {
                if let Some(frame) = receiver.try_recv()?.receive() {
                    #[cfg(any(test, zakura_sim))]
                    self.take_tag();
                    return Ok(frame);
                }
            },
            FramedReceiver::Admitted(receiver) => {
                let AdmittedFrame { frame, grant } = receiver.try_recv()?;
                self.returned = Some(grant);
                #[cfg(any(test, zakura_sim))]
                self.take_tag();
                Ok(frame)
            }
        }
    }

    /// Receive the next admitted frame, or `None` after the transport closes the stream.
    pub async fn recv(&mut self) -> Option<Frame> {
        if let Some(precheck) = &self.precheck {
            precheck.start();
        }
        self.returned = None;
        match &mut self.receiver {
            // As in `try_recv`, the release arm is unchanged.
            #[cfg(not(any(test, zakura_sim)))]
            FramedReceiver::Plain(receiver) => receiver.recv().await,
            #[cfg(any(test, zakura_sim))]
            FramedReceiver::Plain(receiver) => {
                let frame = receiver.recv().await;
                if frame.is_some() {
                    self.take_tag();
                }
                frame
            }
            FramedReceiver::Queued(receiver) => {
                while let Some(queued) = receiver.recv().await {
                    if let Some(frame) = queued.receive() {
                        #[cfg(any(test, zakura_sim))]
                        self.take_tag();
                        return Some(frame);
                    }
                }
                None
            }
            FramedReceiver::Admitted(receiver) => {
                let AdmittedFrame { frame, grant } = receiver.recv().await?;
                self.returned = Some(grant);
                #[cfg(any(test, zakura_sim))]
                self.take_tag();
                Some(frame)
            }
        }
    }
}

/// Send half for bounded Zakura frames.
#[derive(Clone, Debug)]
pub struct FramedSend {
    sender: FramedSender,
    response_slots: Option<SlotBudget>,
    session_resources: Option<Arc<dyn super::service::SessionResources>>,
    /// Funds frames that arrive without a guard from another owner.
    output: Option<FrameGate>,
}

#[derive(Clone, Debug)]
enum FramedSender {
    Plain(mpsc::Sender<Frame>),
    Queued(FrameQueue),
}

/// The send half of a stream's ordered frame queue.
///
/// One channel carries every frame, so a terminal frame stays behind the data
/// sent before it. An ordinary frame holds one of `slots` until the receiver
/// dequeues it. A terminal frame takes no slot, because the slot its exchange
/// held since admission already funds its place. The channel is therefore
/// bounded by `depth` plus the exchange slots of the stream's service.
#[derive(Clone, Debug)]
struct FrameQueue {
    frames: mpsc::UnboundedSender<QueuedFrame>,
    slots: SlotBudget,
    depth: usize,
}

impl FrameQueue {
    fn new(depth: usize) -> (Self, mpsc::UnboundedReceiver<QueuedFrame>) {
        let (frames, receiver) = mpsc::unbounded_channel();
        let slots =
            SlotBudget::new(depth).expect("a transport queue has a semaphore-bounded capacity");
        (
            Self {
                frames,
                slots,
                depth,
            },
            receiver,
        )
    }

    /// Wait for an ordinary frame's slot, or `None` once the receiver closes.
    async fn slot(&self) -> Option<SlotPermit> {
        zakura_rt::select! {
            biased;
            () = self.frames.closed() => None,
            slot = self.slots.reserve() => Some(slot),
        }
    }

    fn try_slot(&self) -> Result<SlotPermit, mpsc::error::TrySendError<()>> {
        if self.frames.is_closed() {
            return Err(mpsc::error::TrySendError::Closed(()));
        }
        self.slots
            .try_reserve()
            .ok_or(mpsc::error::TrySendError::Full(()))
    }
}

impl FramedSend {
    /// Wrap a bounded frame sender.
    pub fn new(sender: mpsc::Sender<Frame>) -> Self {
        Self {
            sender: FramedSender::Plain(sender),
            response_slots: None,
            session_resources: None,
            output: None,
        }
    }

    fn queued(queue: FrameQueue) -> Self {
        let response_slots = SlotBudget::new(queue.depth.saturating_sub(1).max(1))
            .expect("a transport queue has a positive semaphore-bounded capacity");
        Self {
            response_slots: Some(response_slots),
            sender: FramedSender::Queued(queue),
            session_resources: None,
            output: None,
        }
    }

    /// Keep service admission charged while application senders still own the session.
    pub(crate) fn with_session_resources(
        mut self,
        resources: Option<Arc<dyn super::service::SessionResources>>,
    ) -> Self {
        self.session_resources = resources;
        self
    }

    /// Fund frames without another owner from `gate`.
    pub(crate) fn with_output_gate(mut self, gate: FrameGate) -> Self {
        self.output = Some(gate);
        self
    }

    /// Queue a frame for transport-owned encoding and writing.
    ///
    /// This takes free output capacity when called, not when first polled, so
    /// the returned future owns the frame's output from its creation. Dropping
    /// the future at any point, including before its first poll, returns it.
    pub fn send(
        &self,
        frame: Frame,
    ) -> impl std::future::Future<Output = Result<(), mpsc::error::SendError<Frame>>> + Send + '_
    {
        let bytes = frame_wire_bytes(frame.payload.len());
        let admitted = self.output.as_ref().and_then(|gate| gate.try_admit(bytes));
        async move {
            let queue = match &self.sender {
                FramedSender::Plain(sender) => return sender.send(frame).await,
                FramedSender::Queued(queue) => queue,
            };
            let output = match (admitted, &self.output) {
                (Some(grant), _) => Some(grant),
                (None, None) => None,
                (None, Some(gate)) => zakura_rt::select! {
                    biased;
                    () = queue.frames.closed() => return Err(mpsc::error::SendError(frame)),
                    grant = gate.admit(bytes) => Some(grant),
                },
            };
            // The queue's order is the order of these sends. Nothing after the
            // send can restate it, because the worker may already hold the frame.
            let Some(slot) = queue.slot().await else {
                return Err(mpsc::error::SendError(frame));
            };
            queue
                .frames
                .send(QueuedFrame::plain(frame, output, slot))
                .map_err(|error| mpsc::error::SendError(error.0.into_parts().0))
        }
    }

    /// Try to queue a frame without waiting for capacity.
    pub fn try_send(&self, frame: Frame) -> Result<(), mpsc::error::TrySendError<Frame>> {
        let queue = match &self.sender {
            FramedSender::Plain(sender) => return sender.try_send(frame),
            FramedSender::Queued(queue) => queue,
        };
        let slot = match queue.try_slot() {
            Ok(slot) => slot,
            Err(mpsc::error::TrySendError::Full(())) => {
                return Err(mpsc::error::TrySendError::Full(frame));
            }
            Err(mpsc::error::TrySendError::Closed(())) => {
                return Err(mpsc::error::TrySendError::Closed(frame));
            }
        };
        let output = match &self.output {
            None => None,
            Some(gate) => match gate.try_admit(frame_wire_bytes(frame.payload.len())) {
                Some(grant) => Some(grant),
                None => return Err(mpsc::error::TrySendError::Full(frame)),
            },
        };
        queue
            .frames
            .send(QueuedFrame::plain(frame, output, slot))
            .map_err(|error| mpsc::error::TrySendError::Closed(error.0.into_parts().0))
    }

    /// Reserve queue space before encoding a response or sharing its ownership.
    pub(crate) fn try_reserve_guarded(&self) -> Result<GuardedFrameSlot<'_>, GuardedReserveError> {
        let FramedSender::Queued(queue) = &self.sender else {
            return Err(GuardedReserveError::Unsupported);
        };
        queue
            .try_slot()
            .map(|slot| GuardedFrameSlot {
                slot,
                sender: &queue.frames,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(()) => GuardedReserveError::Full,
                mpsc::error::TrySendError::Closed(()) => GuardedReserveError::Closed,
            })
    }

    /// Wait for queue space. Cancellation leaves response ownership with the caller.
    pub(crate) async fn reserve_guarded(
        &self,
    ) -> Result<GuardedFrameSlot<'_>, GuardedReserveError> {
        let FramedSender::Queued(queue) = &self.sender else {
            return Err(GuardedReserveError::Unsupported);
        };
        queue
            .slot()
            .await
            .map(|slot| GuardedFrameSlot {
                slot,
                sender: &queue.frames,
            })
            .ok_or(GuardedReserveError::Closed)
    }

    /// Queue an exchange's terminal frame behind every frame sent before it,
    /// without waiting for a queue slot.
    ///
    /// `guard` must own the slot its exchange has held since admission. Those
    /// slots bound the terminal frames this queue holds beyond its depth, so
    /// full data queues and other exchanges' terminals cannot delay this one.
    pub(crate) fn queue_terminal(
        &self,
        frame: Frame,
        guard: FrameGuard,
    ) -> Result<(), GuardedReserveError> {
        let FramedSender::Queued(queue) = &self.sender else {
            return Err(GuardedReserveError::Unsupported);
        };
        queue
            .frames
            .send(QueuedFrame::guarded(frame, guard, None))
            .map_err(|_| GuardedReserveError::Closed)
    }

    /// Reserve a response without consuming the last queue slot for control.
    /// The response allowance is shared by all sender clones and held through
    /// the write. A one-slot queue makes progress as frames drain, but cannot
    /// provide independent control headroom.
    pub(crate) async fn reserve_response_guarded(
        &self,
    ) -> Result<ResponseFrameSlot<'_>, GuardedReserveError> {
        let budget = self
            .response_slots
            .as_ref()
            .ok_or(GuardedReserveError::Unsupported)?;
        let response = budget.reserve().await;
        let slot = self.reserve_guarded().await?;
        Ok(ResponseFrameSlot { slot, response })
    }

    /// Current free slots in the bounded transport queue.
    pub fn capacity(&self) -> usize {
        match &self.sender {
            FramedSender::Plain(sender) => sender.capacity(),
            FramedSender::Queued(queue) => queue.slots.available(),
        }
    }

    /// Total slots in the bounded transport queue.
    pub fn max_capacity(&self) -> usize {
        match &self.sender {
            FramedSender::Plain(sender) => sender.max_capacity(),
            FramedSender::Queued(queue) => queue.depth,
        }
    }
}

/// One reserved queue slot. Dropping it returns capacity without sending a frame.
#[derive(Debug)]
pub(crate) struct GuardedFrameSlot<'a> {
    slot: SlotPermit,
    sender: &'a mpsc::UnboundedSender<QueuedFrame>,
}

impl GuardedFrameSlot<'_> {
    /// Transfer a validated frame and its ownership to the reserved queue slot.
    ///
    /// A closed queue drops the frame and returns its guard's ownership.
    pub(crate) fn send(self, frame: Frame, guard: FrameGuard) {
        let _closed = self
            .sender
            .send(QueuedFrame::guarded(frame, guard, Some(self.slot)));
    }

    /// Publish a request whose ownership must be claimed before its first byte.
    /// A false result requires explicit settlement after publication unlocks:
    /// the receiver can close during this call and keep the frame unread.
    pub(crate) fn send_request(self, frame: Frame, claim: Arc<dyn FrameWriteClaim>) -> bool {
        if self.sender.is_closed() {
            return false;
        }
        let sent = self.sender.send(QueuedFrame {
            frame,
            guard: None,
            claim: Some(claim),
            output: None,
            queue_slot: Some(self.slot),
        });
        sent.is_ok() && !self.sender.is_closed()
    }
}

/// A response queue slot that leaves space for independent control traffic.
#[derive(Debug)]
pub(crate) struct ResponseFrameSlot<'a> {
    slot: GuardedFrameSlot<'a>,
    response: SlotPermit,
}

impl ResponseFrameSlot<'_> {
    pub(crate) fn send(self, frame: Frame, guard: FrameGuard) {
        self.slot
            .send(frame, FrameGuard::new(Arc::new((guard, self.response))));
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
    /// Funds a frame that has no guard from another owner.
    output: Option<FrameGrant>,
    /// This frame's place in the queue, which ends when the receiver dequeues
    /// it. A terminal frame's exchange slot funds its place instead.
    queue_slot: Option<SlotPermit>,
}

impl QueuedFrame {
    /// Complete an in-process channel delivery if its claim can still start.
    fn receive(self) -> Option<Frame> {
        if let Some(claim) = &self.claim {
            if !claim.try_start() {
                return None;
            }
            claim.written();
        }
        Some(self.frame)
    }

    fn plain(frame: Frame, output: Option<FrameGrant>, queue_slot: SlotPermit) -> Self {
        Self {
            frame,
            guard: None,
            claim: None,
            output,
            queue_slot: Some(queue_slot),
        }
    }

    fn guarded(frame: Frame, guard: FrameGuard, queue_slot: Option<SlotPermit>) -> Self {
        Self {
            frame,
            guard: Some(guard),
            claim: None,
            output: None,
            queue_slot,
        }
    }

    /// Split the frame from its guard while retaining both in the caller.
    pub(crate) fn into_parts(self) -> (Frame, Option<FrameGuard>) {
        (self.frame, self.guard)
    }

    /// Run the transport write while retaining this frame's guard.
    #[cfg(test)]
    pub(crate) async fn write_with<E, F, Fut>(self, write: F) -> Result<(), E>
    where
        F: FnOnce(Frame) -> Fut,
        Fut: std::future::Future<Output = Result<(), E>>,
    {
        self.encode_and_write_with(Ok, write).await
    }

    /// Encode the frame, then claim it and run the transport write while
    /// retaining this frame's guard.
    ///
    /// `encode` runs before the claim, so a frame that fails to encode starts
    /// nothing: its exchange stays unstarted, and retiring its session leaves
    /// the connection up.
    pub(crate) async fn encode_and_write_with<P, E, Enc, F, Fut>(
        self,
        encode: Enc,
        write: F,
    ) -> Result<(), E>
    where
        Enc: FnOnce(Frame) -> Result<P, E>,
        F: FnOnce(P) -> Fut,
        Fut: std::future::Future<Output = Result<(), E>>,
    {
        let Self {
            frame,
            guard: _guard,
            claim,
            output: _output,
            queue_slot: _,
        } = self;
        let encoded = encode(frame)?;
        if claim.as_ref().is_some_and(|claim| !claim.try_start()) {
            return Ok(());
        }
        write(encoded).await?;
        if let Some(claim) = &claim {
            claim.written();
        }
        Ok(())
    }
}

/// Transport-worker receive half for outbound queued frames.
#[derive(Debug)]
pub(crate) struct FramedWorkerRecv {
    receiver: mpsc::UnboundedReceiver<QueuedFrame>,
}

impl FramedWorkerRecv {
    /// Receive the next outbound queued frame, which frees its queue slot.
    pub(crate) async fn recv(&mut self) -> Option<QueuedFrame> {
        let mut queued = self.receiver.recv().await?;
        queued.queue_slot = None;
        Some(queued)
    }
}

/// Build the queue between a service and its transport worker.
pub(crate) fn worker_framed_channel(depth: usize) -> (FramedSend, FramedWorkerRecv) {
    let (queue, receiver) = FrameQueue::new(depth);
    (FramedSend::queued(queue), FramedWorkerRecv { receiver })
}

/// Build a bounded in-memory framed channel for scaffolding and tests.
pub fn framed_channel(depth: usize) -> (FramedSend, FramedRecv) {
    let (queue, receiver) = FrameQueue::new(depth);
    (FramedSend::queued(queue), FramedRecv::queued(receiver))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::FutureExt as _;

    use super::*;
    use crate::zakura::regulation::{OutputByteBudget, SlotBudget};

    fn frame(message_type: u16) -> Frame {
        Frame {
            message_type,
            flags: 0,
            payload: vec![u8::try_from(message_type).unwrap_or(u8::MAX); 10],
        }
    }

    fn guarded_queue() -> (FramedSend, FramedWorkerRecv, SlotBudget) {
        let (sender, receiver) = worker_framed_channel(1);
        let budget = SlotBudget::new(1).unwrap();
        let reservation = Arc::new(budget.try_reserve().expect("the producer is free"));
        sender
            .try_reserve_guarded()
            .expect("the worker queue has a slot")
            .send(frame(1), FrameGuard::new(reservation));
        (sender, receiver, budget)
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
        let (_sender, mut receiver, budget) = guarded_queue();
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
        let (_sender, mut receiver, budget) = guarded_queue();
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
        let (_sender, mut receiver, budget) = guarded_queue();
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
    async fn one_slot_queue_admits_requests_and_control_after_a_response_drains() {
        let (sender, mut receiver) = worker_framed_channel(1);
        sender
            .reserve_response_guarded()
            .await
            .unwrap()
            .send(frame(1), FrameGuard::new(Arc::new(())));
        assert!(matches!(
            sender.try_reserve_guarded(),
            Err(GuardedReserveError::Full)
        ));
        let mut control = Box::pin(sender.send(frame(2)));
        assert!(futures::poll!(&mut control).is_pending());
        let response = receiver.recv().await.unwrap();
        control.await.unwrap();
        assert_eq!(receiver.recv().await.unwrap().into_parts().0, frame(2));
        sender
            .try_reserve_guarded()
            .unwrap()
            .send(frame(3), FrameGuard::new(Arc::new(())));
        assert_eq!(receiver.recv().await.unwrap().into_parts().0, frame(3));
        drop(response);
        assert!(sender.reserve_response_guarded().await.is_ok());
    }

    #[tokio::test]
    async fn response_headroom_is_shared_and_held_until_the_write_ends() {
        let (sender, mut receiver) = worker_framed_channel(2);
        let clone = sender.clone();
        sender
            .reserve_response_guarded()
            .await
            .unwrap()
            .send(frame(1), FrameGuard::new(Arc::new(())));
        let mut pending = Box::pin(clone.reserve_response_guarded());
        assert!(futures::poll!(&mut pending).is_pending());

        let queued = receiver.recv().await.unwrap();
        let mut write = Box::pin(queued.write_with(|_| async {
            std::future::pending::<Result<(), std::convert::Infallible>>().await
        }));
        assert!(futures::poll!(&mut write).is_pending());
        assert!(futures::poll!(&mut pending).is_pending());
        sender
            .try_send(frame(2))
            .expect("control retains queue capacity");

        drop(write);
        let slot = pending.await.unwrap();
        assert_eq!(sender.response_slots.as_ref().unwrap().reserved(), 1);
        drop(slot);
        assert_eq!(sender.response_slots.as_ref().unwrap().reserved(), 0);
        drop(receiver);
        assert!(matches!(
            sender.reserve_response_guarded().await,
            Err(GuardedReserveError::Closed)
        ));
        assert_eq!(sender.response_slots.as_ref().unwrap().reserved(), 0);
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
        let (_sender, receiver, budget) = guarded_queue();
        assert_eq!(budget.reserved(), 1);

        drop(receiver);

        assert_eq!(budget.reserved(), 0);
    }
    /// A queue whose frames without another owner draw from `capacity` bytes.
    fn gated_queue(
        depth: usize,
        capacity: u64,
    ) -> (FramedSend, FramedWorkerRecv, OutputByteBudget) {
        let (sender, receiver) = worker_framed_channel(depth);
        let budget = OutputByteBudget::new(capacity).unwrap();
        let wide = OutputByteBudget::new(1 << 30).unwrap();
        let sender = sender.with_output_gate(FrameGate::new(budget.clone(), wide));
        (sender, receiver, budget)
    }

    /// The bytes [`frame`]`(1)` charges.
    fn frame_bytes() -> u64 {
        frame_wire_bytes(frame(1).payload.len())
    }

    /// The owner exists when `send` returns, before any poll, and dropping the
    /// unpolled future returns its bytes.
    #[test]
    fn an_unpolled_send_owns_its_output_and_releases_it_on_drop() {
        let (sender, _receiver, budget) = gated_queue(1, 1024);
        let unpolled = sender.send(frame(1));
        assert_eq!(budget.granted(), frame_bytes());
        drop(unpolled);
        assert_eq!(budget.granted(), 0);
    }

    #[tokio::test]
    async fn a_send_cancelled_at_its_queue_wait_releases_its_output() {
        let (sender, _receiver, budget) = gated_queue(1, 1024);
        sender.try_send(frame(1)).unwrap();
        let mut waiting = Box::pin(sender.send(frame(2)));
        assert!(futures::poll!(&mut waiting).is_pending());
        assert_eq!(budget.granted(), 2 * frame_bytes());
        drop(waiting);
        assert_eq!(budget.granted(), frame_bytes());
    }

    #[tokio::test]
    async fn output_stays_charged_through_the_queue_and_the_write() {
        let (sender, mut receiver, budget) = gated_queue(2, 1024);
        sender.send(frame(1)).await.unwrap();
        assert_eq!(budget.granted(), frame_bytes());

        let queued = receiver.recv().await.unwrap();
        assert_eq!(budget.granted(), frame_bytes(), "dequeue is not settlement");
        let mut write = Box::pin(queued.write_with(|_| async {
            std::future::pending::<Result<(), std::convert::Infallible>>().await
        }));
        assert!(futures::poll!(&mut write).is_pending());
        assert_eq!(budget.granted(), frame_bytes(), "a pending write holds it");
        drop(write);
        assert_eq!(budget.granted(), 0, "a cancelled write returns it");

        sender.send(frame(2)).await.unwrap();
        let queued = receiver.recv().await.unwrap();
        queued
            .write_with(|_| async { Ok::<_, std::convert::Infallible>(()) })
            .await
            .unwrap();
        assert_eq!(budget.granted(), 0, "a finished write returns it");
    }

    #[test]
    fn dropping_the_worker_queue_returns_queued_output() {
        let (sender, receiver, budget) = gated_queue(2, 1024);
        sender.try_send(frame(1)).unwrap();
        assert_eq!(budget.granted(), frame_bytes());
        drop(receiver);
        assert_eq!(budget.granted(), 0);
        assert!(matches!(
            sender.try_send(frame(2)),
            Err(mpsc::error::TrySendError::Closed(_))
        ));
        assert_eq!(budget.granted(), 0);
    }

    /// Output capacity and queue capacity report the same way to a caller, and
    /// the stream's floor slot keeps one frame moving at a saturated budget.
    #[tokio::test]
    async fn a_saturated_budget_reports_full_and_keeps_the_floor_frame() {
        let (sender, mut receiver, budget) = gated_queue(8, frame_bytes());
        sender.try_send(frame(1)).unwrap();
        sender.try_send(frame(2)).expect("the floor slot admits it");
        assert!(matches!(
            sender.try_send(frame(3)),
            Err(mpsc::error::TrySendError::Full(Frame {
                message_type: 3,
                ..
            }))
        ));
        let mut waiting = Box::pin(sender.send(frame(3)));
        assert!(futures::poll!(&mut waiting).is_pending());
        drop(receiver.recv().await.unwrap());
        waiting.await.expect("the freed bytes admit the frame");
        assert_eq!(budget.granted(), frame_bytes());
    }

    #[tokio::test]
    async fn a_send_waiting_for_output_ends_when_the_queue_closes() {
        let (sender, receiver, _budget) = gated_queue(8, frame_bytes());
        let _held = (sender.try_send(frame(1)), sender.try_send(frame(2)));
        let mut waiting = Box::pin(sender.send(frame(3)));
        assert!(futures::poll!(&mut waiting).is_pending());
        drop(receiver);
        assert!(matches!(
            waiting.await,
            Err(mpsc::error::SendError(Frame {
                message_type: 3,
                ..
            }))
        ));
    }

    /// Each terminal holds its exchange's own place. A queue of ordinary frames
    /// and another exchange's terminal delay none of them, and each stays behind
    /// the frames sent before it. Dequeuing still frees only ordinary slots.
    #[tokio::test]
    async fn terminals_queue_behind_a_full_queue_without_waiting() {
        let (sender, mut receiver) = worker_framed_channel(1);
        sender.try_send(frame(1)).unwrap();
        let owner = Arc::new(());
        for byte in 2..=4 {
            sender
                .queue_terminal(frame(byte), FrameGuard::new(owner.clone()))
                .unwrap();
        }
        assert_eq!(sender.capacity(), 0, "terminals take no ordinary slot");
        assert!(matches!(
            sender.try_send(frame(5)),
            Err(mpsc::error::TrySendError::Full(_))
        ));

        let mut next = Box::pin(sender.send(frame(5)));
        assert!(futures::poll!(&mut next).is_pending());
        for byte in 1..=4 {
            let queued = receiver.recv().await.unwrap();
            assert_eq!(queued.frame, frame(byte));
            drop(queued);
        }
        next.await.expect("the first dequeue freed the slot");
        assert_eq!(receiver.recv().await.unwrap().frame, frame(5));
        assert_eq!(Arc::strong_count(&owner), 1);
    }

    #[tokio::test]
    async fn a_terminal_on_a_closed_queue_returns_its_guard() {
        let (sender, receiver) = worker_framed_channel(1);
        drop(receiver);
        let owner = Arc::new(());
        assert!(matches!(
            sender.queue_terminal(frame(1), FrameGuard::new(owner.clone())),
            Err(GuardedReserveError::Closed)
        ));
        assert_eq!(Arc::strong_count(&owner), 1);
    }

    /// A guarded frame already has an owner, so it takes no output from the
    /// gate, and control traffic finds the slot that responses leave free.
    #[tokio::test]
    async fn control_uses_the_slot_behind_saturated_responses() {
        let (sender, mut receiver, budget) = gated_queue(3, 1);
        let _saturated = budget.hold_free().unwrap();
        for byte in 1..=2 {
            sender
                .reserve_response_guarded()
                .await
                .unwrap()
                .send(frame(byte), FrameGuard::new(Arc::new(())));
        }
        assert!(
            sender.reserve_response_guarded().now_or_never().is_none(),
            "responses leave the last queue slot free"
        );
        sender
            .try_reserve_guarded()
            .expect("control finds the free slot")
            .send(frame(3), FrameGuard::new(Arc::new(())));
        for byte in 1..=3 {
            let (received, _guard) = receiver.recv().await.unwrap().into_parts();
            assert_eq!(received, frame(byte), "control stays behind prior data");
        }
    }

    /// Many producers on real threads share one queue and one budget. Each
    /// producer's frames stay in its order, which only the channel decides, and
    /// the budget never counts a dequeued frame as missing: its grant was taken
    /// before the send and returns only with the receiver's value.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_channel_alone_orders_concurrent_producers() {
        const PRODUCERS: u16 = 4;
        const FRAMES: u16 = 500;
        let capacity = 16 * frame_bytes();
        let (sender, mut receiver, budget) = gated_queue(8, capacity);
        let mut producers = Vec::new();
        for producer in 0..PRODUCERS {
            let sender = sender.clone();
            producers.push(tokio::spawn(async move {
                for sequence in 0..FRAMES {
                    let mut payload = vec![0; 10];
                    payload[..2].copy_from_slice(&sequence.to_le_bytes());
                    sender
                        .send(Frame {
                            message_type: producer,
                            flags: 0,
                            payload,
                        })
                        .await
                        .unwrap();
                }
            }));
        }
        drop(sender);

        let mut next = [0u16; PRODUCERS as usize];
        while let Some(queued) = receiver.recv().await {
            assert!(budget.granted() <= capacity);
            let producer = usize::from(queued.frame.message_type);
            let payload = &queued.frame.payload;
            let sequence = u16::from_le_bytes([payload[0], payload[1]]);
            assert_eq!(
                sequence, next[producer],
                "producer {producer} kept its order"
            );
            next[producer] += 1;
            // The grant ends only with the write.
            queued
                .write_with(|_| async { Ok::<_, std::convert::Infallible>(()) })
                .await
                .unwrap();
        }
        for producer in producers {
            producer.await.unwrap();
        }
        assert_eq!(next, [FRAMES; PRODUCERS as usize]);
        assert_eq!(budget.granted(), 0, "every grant settled");
    }

    fn admitted(budget: &OutputByteBudget) -> AdmittedFrame {
        let wide = OutputByteBudget::new(1 << 30).unwrap();
        let grant = FrameGate::new(budget.clone(), wide)
            .try_admit(frame_bytes())
            .unwrap();
        AdmittedFrame {
            frame: frame(1),
            grant,
        }
    }

    /// A raw frame stays funded while the service decodes it, until its next
    /// receive starts or the handle drops. The grant does not outlive either.
    #[tokio::test]
    async fn a_received_frame_stays_charged_until_the_next_receive() {
        let budget = OutputByteBudget::new(1024).unwrap();
        let (sender, receiver) = mpsc::channel(4);
        let mut receiver = FramedRecv::admitted(receiver);
        sender.send(admitted(&budget)).await.unwrap();
        assert_eq!(budget.granted(), frame_bytes(), "the queue holds it");

        assert_eq!(receiver.recv().await, Some(frame(1)));
        assert_eq!(budget.granted(), frame_bytes(), "decoding holds it");

        let mut next = Box::pin(receiver.recv());
        assert!(futures::poll!(&mut next).is_pending());
        assert_eq!(budget.granted(), 0, "the next receive released it");
        sender.send(admitted(&budget)).await.unwrap();
        assert_eq!(next.await, Some(frame(1)));
        assert_eq!(budget.granted(), frame_bytes());

        sender.send(admitted(&budget)).await.unwrap();
        assert_eq!(budget.granted(), 2 * frame_bytes());
        assert_eq!(receiver.try_recv().unwrap(), frame(1));
        assert_eq!(
            budget.granted(),
            frame_bytes(),
            "`try_recv` moves ownership too"
        );

        drop(receiver);
        assert_eq!(budget.granted(), 0, "dropping the handle ends everything");
    }

    #[tokio::test]
    async fn closing_the_receiver_returns_queued_raw_frames_when_it_drops() {
        let budget = OutputByteBudget::new(1024).unwrap();
        let (sender, receiver) = mpsc::channel(4);
        let mut receiver = FramedRecv::admitted(receiver);
        sender.send(admitted(&budget)).await.unwrap();
        receiver.close();
        assert_eq!(
            budget.granted(),
            frame_bytes(),
            "closing keeps queued frames"
        );
        assert_eq!(receiver.recv().await, Some(frame(1)));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(budget.granted(), 0);
    }

    #[tokio::test]
    async fn quic_backpressure_holds_producer_and_preserves_another_stream() {
        use crate::zakura::testkit::LocalEndpointFactory;
        use futures::future::BoxFuture;
        use std::time::Duration;
        use zakura_quic::{Acceptor, Admit, Conn, IncomingInfo, QuicConfig};

        const ALPN: &[u8] = b"/zakura/test/producer-backpressure";

        #[derive(Debug)]
        struct AcceptConnection(mpsc::Sender<Conn>);
        impl Acceptor for AcceptConnection {
            fn admit(&self, _incoming: &IncomingInfo) -> Admit {
                Admit::Accept
            }

            fn alpns(&self) -> Vec<Vec<u8>> {
                vec![ALPN.to_vec()]
            }

            fn handle(&self, connection: Conn) -> BoxFuture<'static, ()> {
                let accepted = self.0.clone();
                Box::pin(async move {
                    let _ = accepted.send(connection).await;
                })
            }
        }

        // Scale down the windows so a single bounded frame reaches flow control.
        let quic_config = || QuicConfig {
            stream_receive_window_bytes: 64 * 1024,
            receive_window_bytes: 128 * 1024,
            send_window_bytes: 128 * 1024,
            ..QuicConfig::default()
        };
        let server = LocalEndpointFactory::new()
            .quic_config(quic_config())
            .endpoint(92_001)
            .await
            .unwrap();
        let client = LocalEndpointFactory::new()
            .quic_config(quic_config())
            .endpoint(92_002)
            .await
            .unwrap();
        let (accepted, mut incoming) = mpsc::channel(1);
        server.serve(AcceptConnection(accepted)).unwrap();
        let address = LocalEndpointFactory::node_addr(&server).await;
        let connection = client.connect(address, &[ALPN]).await.unwrap();
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
        client.shutdown().await;
        server.shutdown().await;
    }
}
