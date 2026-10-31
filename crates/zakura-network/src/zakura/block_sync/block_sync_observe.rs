//! Synchronous block-sync effects and owner snapshots for simulator tests.
//!
//! Block sync's tasks report what they do to an installed
//! [`BlockSyncObserver`]: session admission, judged frames, request write
//! steps, byte-budget changes, the body handoff to the sequencer, sequencer
//! holder changes, serving leases and frames, and each owning task's snapshot
//! after an input. This module exists only in test and `zakura_sim` builds.
//! Production builds compile none of it, and every hook that reports to it is
//! behind the same `cfg`.
//!
//! Each owner snapshot is read by the task that owns the state, under the
//! locks that task takes anyway. The observer never takes a block-sync lock
//! itself, because the simulator can park a task while it holds one.

use std::{fmt, sync::Arc};

use zakura_chain::block;
use zakura_header_chain::BodyWorkOwner;

pub use super::{
    regulated::session::AdmittedRange,
    reorder::RetainedBody,
    sequencer::{SequencerOwnership, SubmissionOwnership},
    state::{RequestKey, RequestOwnership, RequestedHeight},
    work_queue::{RequestWritePhase, WorkItemOwnership, WorkItemPlace},
    BlockApplyToken,
};
pub use crate::zakura::transport::{BudgetChange, BudgetChangeKind};
use crate::zakura::{ServicePeerDirection, ZakuraConnId, ZakuraPeerId};

/// Receives block-sync events as they happen.
///
/// Called synchronously on the reporting task, sometimes while that task holds
/// a block-sync lock. An implementation must not block, yield, take a
/// `zakura_rt` lock, or call into block sync. A panic in `observe` unwinds the
/// reporting task.
pub trait BlockSyncObserver: Send + Sync + fmt::Debug {
    /// Records one event.
    fn observe(&self, event: BlockEvent);
}

/// The optional observer installed on block sync's startup.
#[derive(Clone, Default)]
pub struct ObserverSlot(pub Option<Arc<dyn BlockSyncObserver>>);

impl fmt::Debug for ObserverSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ObserverSlot")
            .field(&self.0.is_some())
            .finish()
    }
}

/// One block-sync event.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BlockEvent {
    /// The service admitted a session and is about to spawn its routine.
    SessionAdmitted {
        /// The remote peer.
        peer: ZakuraPeerId,
        /// The transport connection that carries the session.
        conn_id: ZakuraConnId,
        /// The service's ID for the session.
        session_id: u64,
        /// The routine generation the registry assigned.
        generation: u64,
        /// The connection's direction.
        direction: ServicePeerDirection,
    },
    /// A routine took a frame from its stream and is about to judge it. Every
    /// check the routine, its regulated session and the sequencer run on the
    /// frame follows this event.
    FrameJudged {
        /// The sending peer.
        peer: ZakuraPeerId,
        /// The connection the frame arrived on.
        conn_id: ZakuraConnId,
        /// The routine's generation.
        generation: u64,
        /// The frame's position on the routine's stream, counting from 1.
        seq: u64,
        /// The frame header's message type.
        message_type: u16,
        /// The frame header's flags.
        flags: u16,
        /// The payload length in bytes.
        payload_len: usize,
    },
    /// A request's write moved one step.
    Request {
        /// The request.
        owner: BodyWorkOwner,
        /// Its heights, in order.
        heights: Vec<block::Height>,
        /// The size estimate the request reserved for all of its heights.
        estimated_bytes: u64,
        /// The step.
        step: RequestStep,
    },
    /// The shared byte budget's counter changed.
    Budget(BudgetChange),
    /// A routine is about to send a body to the sequencer's input. The body's
    /// request charge stays on the byte budget until the matching
    /// [`HandoffEnded`]. The routine reports this before the send, and before
    /// the body's `Queued` holder, because the sequencer can dequeue the body
    /// as soon as the send succeeds.
    ///
    /// [`HandoffEnded`]: BlockEvent::HandoffEnded
    HandoffStarted {
        /// The delivering peer.
        peer: ZakuraPeerId,
        /// The request the body answered.
        owner: BodyWorkOwner,
        /// The body's height.
        height: block::Height,
        /// The budget bytes the routine releases when the handoff ends.
        charge: u64,
    },
    /// The routine released the charge of the body it handed off.
    HandoffEnded {
        /// The delivering peer.
        peer: ZakuraPeerId,
        /// The request the body answered.
        owner: BodyWorkOwner,
        /// The body's height.
        height: block::Height,
        /// The budget bytes released.
        charge: u64,
    },
    /// A retained body entered or left one of its holders. A transfer is a
    /// `Released` from one holder and a `Retained` into another, reported at
    /// the same boundary.
    Holder {
        /// The body.
        body: HeldBody,
        /// The holder.
        holder: Holder,
        /// Whether the body entered or left it.
        change: HolderChange,
    },
    /// A serving read received its execution lease, or dropped it.
    ServingLease {
        /// The requesting peer.
        peer: ZakuraPeerId,
        /// The serving session's connection.
        conn_id: ZakuraConnId,
        /// The serving session's routine generation.
        generation: u64,
        /// The range's first height.
        start: block::Height,
        /// The range's requested count.
        count: u32,
        /// Whether the lease was acquired or released.
        step: LeaseStep,
    },
    /// Serving judged a peer's request against the ranges it still serves:
    /// those whose endings it hadn't published by then.
    ServingAdmission {
        /// The requesting peer.
        peer: ZakuraPeerId,
        /// The request's first height.
        start: block::Height,
        /// The request's count.
        count: u32,
        /// The first and last heights of each range it still serves.
        live: Vec<(block::Height, block::Height)>,
    },
    /// Serving queued one response frame for a peer.
    ServingFrame {
        /// The requesting peer.
        peer: ZakuraPeerId,
        /// The serving session's connection.
        conn_id: ZakuraConnId,
        /// The serving session's routine generation.
        generation: u64,
        /// The range's first height.
        start: block::Height,
        /// The frame's message type.
        message_type: u8,
        /// The block bytes the frame carries, or 0 for an ending.
        body_bytes: u64,
    },
    /// A routine's owner state, read by the routine after it handled an input.
    Routine(RoutineSnapshot),
    /// The work queue's items, read by a task that changes the queue after it
    /// handled an input.
    WorkQueue(Vec<WorkItemOwnership>),
    /// The sequencer's retained bodies, read by the sequencer task.
    Sequencer(SequencerOwnership),
    /// The sequencer task is dropping and takes no more bodies. Each queued
    /// body released after this has no next holder: the task's drop
    /// releases the bodies left in its input, and a routine's send fails.
    SequencerClosed,
}

/// One step of a request's frame write.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RequestStep {
    /// The request reserved its heights and queued its frame.
    Published,
    /// Transport started writing the frame.
    Started,
    /// Transport wrote the frame.
    Written,
    /// The queue lost its receiver while the request was published, so the
    /// request returned its heights.
    DeliveryFailed,
    /// Transport skips the queued frame.
    Expired,
    /// A reset retired the request.
    Reset,
    /// The request's last owner dropped it in this phase.
    Dropped(RequestWritePhase),
}

/// A body that one or more sequencer holders retain.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeldBody {
    /// The body's height.
    pub height: block::Height,
    /// The request the body answered.
    pub owner: BodyWorkOwner,
    /// Wire bytes.
    pub bytes: u64,
}

/// One holder of a retained body.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum Holder {
    /// The sequencer's input queue, by the body's allocation address.
    Queued {
        /// See [`RetainedBody::allocation`].
        allocation: usize,
        /// The attributed size of the decoded block the queued body holds,
        /// measured from the block itself. 0 for raw bytes.
        decoded_attributed_memory_bytes: u64,
    },
    /// The reorder buffer.
    Reorder {
        /// See [`RetainedBody::allocation`].
        allocation: usize,
    },
    /// `applying`, waiting for or under submission.
    Applying {
        /// See [`RetainedBody::allocation`].
        allocation: usize,
    },
    /// The verifier driver, through a submission still attached to `applying`.
    Submitted {
        /// The submission's token.
        token: BlockApplyToken,
    },
    /// The verifier driver, through a submission detached from `applying`.
    Detached {
        /// The submission's token.
        token: BlockApplyToken,
    },
}

/// Whether a body entered or left a holder.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HolderChange {
    /// The holder took the body.
    Retained,
    /// The holder let the body go.
    Released,
}

/// Whether a serving read's lease was acquired or released.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LeaseStep {
    /// The read holds the lease.
    Acquired,
    /// The lease dropped.
    Released,
}

/// A routine's owner state after it handled an input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutineSnapshot {
    /// The routine's peer.
    pub peer: ZakuraPeerId,
    /// The routine's connection.
    pub conn_id: ZakuraConnId,
    /// The routine's generation.
    pub generation: u64,
    /// The routine's outstanding requests.
    pub requests: Vec<RequestOwnership>,
    /// The claims the routine published to the peer registry.
    pub claims: Vec<RegistryClaim>,
    /// The ranges the routine's serving session records.
    pub serving: Vec<AdmittedRange>,
    /// True for the routine's last snapshot, which its drop reports after it
    /// returned its work.
    pub ended: bool,
}

/// One unreceived height a routine published to the peer registry.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RegistryClaim {
    /// The claimed height.
    pub height: block::Height,
    /// The request that claims it.
    pub owner: BodyWorkOwner,
    /// The size estimate the request reserved for it.
    pub estimated_bytes: u64,
}

impl ObserverSlot {
    /// A byte-budget callback that reports each change as
    /// [`BlockEvent::Budget`], if an observer is installed.
    pub(crate) fn budget_observer(&self) -> Option<crate::zakura::transport::BudgetObserver> {
        let observer = self.0.clone()?;
        Some(crate::zakura::transport::BudgetObserver(Arc::new(
            move |change| observer.observe(BlockEvent::Budget(change)),
        )))
    }
}

impl SequencerOwnership {
    /// Each holder in this snapshot and the body it holds: one entry per
    /// reorder-buffer body, `applying` body and submission.
    pub fn holders(&self) -> Vec<(Holder, HeldBody)> {
        let held = |body: &RetainedBody| HeldBody {
            height: body.height,
            owner: body.owner,
            bytes: body.bytes,
        };
        let reorder = self.reorder.iter().map(|body| {
            let allocation = body.allocation;
            (Holder::Reorder { allocation }, held(body))
        });
        let applying = self.applying.iter().map(|body| {
            let allocation = body.allocation;
            (Holder::Applying { allocation }, held(body))
        });
        let submissions = self.submissions.iter().map(|submission| {
            let token = submission.token;
            let holder = if submission.detached {
                Holder::Detached { token }
            } else {
                Holder::Submitted { token }
            };
            let body = HeldBody {
                height: submission.height,
                owner: submission.owner,
                bytes: submission.bytes,
            };
            (holder, body)
        });
        reorder.chain(applying).chain(submissions).collect()
    }
}

/// Reports one serving session's admission, lease and frame events for its
/// peer. The session reports each request's live ranges once it drops the
/// ranges whose endings were published, before it checks the request. Serve's
/// dispatch reports each response's lease once it takes the execution slots,
/// before it spawns the response's task. The slots report the release once
/// they return, after the last lease clone drops. Serve's task reports each
/// failure ending, and the encoder reports each block frame and each ending
/// it queues.
#[derive(Clone, Debug)]
pub(crate) struct ServingReporter {
    observer: ObserverSlot,
    session: ServingIdentity,
}

/// A routine session: its connection and generation.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SessionKey {
    /// The session's connection.
    pub(crate) conn_id: ZakuraConnId,
    /// The session's routine generation.
    pub(crate) generation: u64,
}

/// Which session a serving read belongs to. A cancelled session's read can
/// still run when its replacement asks for the same range, and only the
/// connection and generation tell the two apart.
#[derive(Clone, Debug)]
pub(crate) struct ServingIdentity {
    /// The requesting peer.
    pub(crate) peer: ZakuraPeerId,
    /// The session's connection.
    pub(crate) conn_id: ZakuraConnId,
    /// The session's routine generation.
    pub(crate) generation: u64,
}

impl ServingReporter {
    /// A reporter for `session`.
    pub(crate) fn new(observer: ObserverSlot, session: ServingIdentity) -> Self {
        Self { observer, session }
    }

    /// Reports that the response for `count` heights from `start` took or
    /// released its lease.
    pub(crate) fn lease(&self, start: block::Height, count: u32, step: LeaseStep) {
        observe!(
            self.observer,
            BlockEvent::ServingLease {
                peer: self.session.peer.clone(),
                conn_id: self.session.conn_id,
                generation: self.session.generation,
                start,
                count,
                step,
            }
        );
    }

    /// Reports that serving judged a request for `count` heights from
    /// `start` against the `live` ranges.
    pub(crate) fn admission(
        &self,
        start: block::Height,
        count: u32,
        live: Vec<(block::Height, block::Height)>,
    ) {
        observe!(
            self.observer,
            BlockEvent::ServingAdmission {
                peer: self.session.peer.clone(),
                start,
                count,
                live,
            }
        );
    }

    /// Reports one queued frame of the response from `start`.
    pub(crate) fn frame(&self, start: block::Height, message_type: u8, body_bytes: u64) {
        observe!(
            self.observer,
            BlockEvent::ServingFrame {
                peer: self.session.peer.clone(),
                conn_id: self.session.conn_id,
                generation: self.session.generation,
                start,
                message_type,
                body_bytes,
            }
        );
    }
}
