//! Capacity for pushed frames, such as subscription pages.
//!
//! No request admits a pushed frame, so `Serve` never sees it. It still takes
//! the budgets a served response takes, in the same order: peer output, node
//! output, peer execution, then node execution. So pushes and served
//! responses share every node-wide and per-peer bound.

use std::sync::Arc;

use super::{capacity::PeerBudgets, lease::ExecutionSlots, ResponseGrants, ServeCapacity};
use crate::zakura::{Frame, FrameGuard, FramedSend, ZakuraPeerId, FRAME_HEADER_BYTES};

/// One peer's budgets for pushed frames.
#[derive(Clone, Debug)]
pub(crate) struct Push {
    capacity: ServeCapacity,
    peer: PeerBudgets,
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
        }
    }
}

impl Push {
    /// Wait for output for a frame of `payload_len` bytes and for execution
    /// slots. Cancelling the wait takes nothing.
    pub(crate) async fn acquire(&self, payload_len: usize) -> PushPermit {
        // Widening usize to u64 is lossless on supported targets.
        let bytes = (payload_len + FRAME_HEADER_BYTES) as u64;
        let delayed = |bound| self.capacity.metrics.delayed(bound);
        let peer_output = &self.peer.output;
        let node_output = &self.capacity.node_output;
        let peer = wait(peer_output.grant(peer_output.clamp(bytes)), || {
            delayed("peer_output")
        })
        .await;
        let node = wait(node_output.grant(node_output.clamp(bytes)), || {
            delayed("node_output")
        })
        .await;
        let grants = ResponseGrants {
            _node: node,
            _peer: peer,
        };
        let peer = wait(self.peer.execution.reserve(), || delayed("peer_execution")).await;
        let node = wait(self.capacity.node_execution.reserve(), || {
            delayed("node_execution")
        })
        .await;
        PushPermit {
            grants,
            execution: ExecutionSlots::new(peer, node),
        }
    }
}

impl PushPermit {
    /// Queue `frame` on `send`. Execution slots return now; the output
    /// returns when the frame's transport write finishes.
    ///
    /// Returns false if the stream is closed.
    pub(crate) async fn send(self, send: &FramedSend, frame: Frame) -> bool {
        let Self { grants, execution } = self;
        drop(execution);
        match send.reserve_guarded().await {
            Ok(slot) => {
                slot.send(frame, FrameGuard::new(Arc::new(grants)));
                true
            }
            Err(_) => false,
        }
    }
}

/// Await `acquire`, counting a delay if it is not ready at once.
async fn wait<T>(acquire: impl std::future::Future<Output = T>, delayed: impl FnOnce()) -> T {
    tokio::pin!(acquire);
    if let Some(ready) = futures::FutureExt::now_or_never(&mut acquire) {
        return ready;
    }
    delayed();
    acquire.await
}
