//! Capacity for pushed frames, such as subscription pages.
//!
//! No request admits a pushed frame, so `Serve` never sees it. It still takes
//! the budgets a served response takes, in the same order: response slots,
//! output bytes, then execution slots. So pushes and served responses share
//! every node-wide and per-peer bound. With a memory pool, the pool's lease
//! replaces the node-wide response and output gates.

use std::{future::Future, sync::Arc};

use super::{
    capacity::PeerBudgets, lease::ExecutionSlots, GateWait, ResponseGrants, ServeCapacity,
};
use crate::zakura::{Frame, FrameGuard, FramedSend, ZakuraPeerId, FRAME_HEADER_BYTES};

/// One peer's budgets for pushed frames.
#[derive(Clone, Debug)]
pub(crate) struct Push {
    capacity: ServeCapacity,
    peer: PeerBudgets,
    /// The pushing peer, used for memory-pool per-peer serving leases and
    /// acquisition reports.
    peer_id: ZakuraPeerId,
}

/// The serving budgets in use, as [`Push::in_use`] reads them.
#[cfg(any(test, zakura_sim))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ServeInUse {
    /// Node execution slots held.
    pub node_execution: usize,
    /// The peer's execution slots held.
    pub peer_execution: usize,
    /// Node output response slots held.
    pub node_responses: usize,
    /// The peer's output response slots held.
    pub peer_responses: usize,
    /// Node output bytes granted.
    pub node_output: u64,
    /// The peer's output bytes granted.
    pub peer_output: u64,
}

/// Capacity for one pushed frame.
#[derive(Debug)]
#[must_use = "dropping a push permit releases its capacity"]
pub(crate) struct PushPermit {
    grants: ResponseGrants,
    execution: ExecutionSlots,
}

impl ServeCapacity {
    /// Budgets for frames pushed to `peer`. They are the budgets the peer's
    /// served responses use.
    pub(crate) fn push(&self, peer: &ZakuraPeerId) -> Push {
        Push {
            capacity: self.clone(),
            peer: self.peer(peer),
            peer_id: peer.clone(),
        }
    }
}

impl Push {
    /// The serving budgets in use now, the node's and this push's peer's:
    /// execution slots, output response slots and output bytes, read with
    /// no simulator preemption point.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn in_use(&self) -> ServeInUse {
        ServeInUse {
            node_execution: self.capacity.node_execution.peek_reserved(),
            peer_execution: self.peer.execution.peek_reserved(),
            node_responses: self.capacity.node_output_responses.peek_reserved(),
            peer_responses: self.peer.output_responses.peek_reserved(),
            node_output: self.capacity.node_output.peek_granted(),
            peer_output: self.peer.output.peek_granted(),
        }
    }

    /// Wait for output for a frame of `payload_len` bytes and for execution
    /// slots (see [`ResponseGrants::acquire`]). Cancelling the wait takes
    /// nothing.
    pub(crate) async fn acquire(&self, payload_len: usize) -> PushPermit {
        // Widening usize to u64 is lossless on supported targets.
        let bytes = (payload_len + FRAME_HEADER_BYTES) as u64;
        let grants =
            ResponseGrants::acquire(&self.capacity, &self.peer, &self.peer_id, bytes, self)
                .await
                .expect("a push's gates wait until they grant");
        PushPermit {
            grants,
            execution: self.execution().await,
        }
    }

    /// Wait for execution slots alone, for state work a frame needs after
    /// [`PushPermit::reserve_send`] gave its slots back. The frame already
    /// holds its output, and execution comes after output as in
    /// [`Self::acquire`], so the wait cannot invert that order. Cancelling
    /// the wait takes nothing.
    pub(crate) async fn execution(&self) -> ExecutionSlots {
        let delayed = |bound| self.capacity.metrics.delayed(bound);
        let peer = wait(self.peer.execution.reserve(), || delayed("peer_execution")).await;
        #[cfg(any(test, zakura_sim))]
        self.report_acquired();
        let node = wait(self.capacity.node_execution.reserve(), || {
            delayed("node_execution")
        })
        .await;
        #[cfg(any(test, zakura_sim))]
        self.report_acquired();
        ExecutionSlots::new(peer, node, self.peer.clone())
    }

    /// Tells the capacity's acquisition report the budgets in use, right
    /// after this push took one of them, so an excess shows even if the
    /// push is cancelled waiting for the next.
    #[cfg(any(test, zakura_sim))]
    fn report_acquired(&self) {
        if let Some(reports) = &self.capacity.reports {
            (reports.acquired)(&self.peer_id, self.in_use());
        }
    }
}

/// A page's output grants and reserved transport slot. Dropping it sends nothing.
#[derive(Debug)]
pub(crate) struct PushSend<'a> {
    grants: ResponseGrants,
    slot: crate::zakura::transport::ResponseFrameSlot<'a>,
}

impl PushPermit {
    /// Wait for a response slot before spending subscription credit.
    /// Cancellation releases these grants and any partially acquired slot.
    /// Recheck the subscription after this wait, then commit and send without awaiting.
    pub(crate) async fn reserve_send(
        self,
        send: &FramedSend,
    ) -> Result<PushSend<'_>, crate::zakura::transport::GuardedReserveError> {
        let Self { grants, execution } = self;
        drop(execution);
        let slot = send.reserve_response_guarded().await?;
        Ok(PushSend { grants, slot })
    }
}

impl PushSend<'_> {
    /// Queue the page immediately after its subscription credit is committed,
    /// returning whether the transport queue took it
    /// ([`crate::zakura::transport::Queued`]).
    pub(crate) fn send(self, frame: Frame) -> crate::zakura::transport::Queued {
        // Widening usize to u64 is lossless on supported targets.
        let encoded_bytes = (frame.payload.len() + FRAME_HEADER_BYTES) as u64;
        let Self { mut grants, slot } = self;
        // The page owns its grants until the frame guard shares them.
        grants.shrink_unshared_to(encoded_bytes);
        slot.send(frame, FrameGuard::new(Arc::new(grants)))
    }
}

/// A push's waits never end before their grant.
impl GateWait for Push {
    async fn wait<T>(&self, bound: &'static str, acquire: impl Future<Output = T>) -> Option<T> {
        Some(wait(acquire, || self.capacity.metrics.delayed(bound)).await)
    }

    #[cfg(any(test, zakura_sim))]
    fn took_budget(&self) {
        self.report_acquired();
    }
}

/// Await `acquire`, counting a delay if it is not ready at once.
async fn wait<T>(acquire: impl Future<Output = T>, delayed: impl FnOnce()) -> T {
    zakura_rt::pin!(acquire);
    if let Some(ready) = futures::FutureExt::now_or_never(&mut acquire) {
        return ready;
    }
    delayed();
    acquire.await
}
