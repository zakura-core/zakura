//! What a tapped block-sync node's owners hold, folded from its events.
//!
//! [`BlockSyncView`] keeps each owner's latest snapshot, which its owning
//! task published, and folds the effect events between snapshots: the byte
//! budget's counter, open handoffs, each holder's body, and held serving
//! leases. Checks compare it with independent models. An event the fold
//! can't apply, such as a release of something never retained, is kept in
//! [`BlockSyncView::anomalies`] for a check to judge, since it describes
//! the node, not the harness.

use std::collections::BTreeMap;

use zakura_chain::block::Height;
use zakura_header_chain::BodyWorkOwner;
use zakura_network::zakura::{
    block_sync_observe::{
        BlockEvent, BudgetChange, HeldBody, Holder, HolderChange, LeaseStep, RoutineSnapshot,
        SequencerOwnership, WorkItemOwnership,
    },
    ServicePeerDirection, ZakuraConnId, ZakuraPeerId,
};

/// An admitted session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    /// The remote peer.
    pub peer: ZakuraPeerId,
    /// The service's ID for the session.
    pub session_id: u64,
    /// The routine generation the registry assigned.
    pub generation: u64,
    /// The connection's direction.
    pub direction: ServicePeerDirection,
}

/// A body a routine handed to the sequencer while keeping its charge: one
/// unit of D2's `h`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handoff {
    /// The delivering peer.
    pub peer: ZakuraPeerId,
    /// The request the body answered.
    pub owner: BodyWorkOwner,
    /// The body's height.
    pub height: Height,
    /// The budget bytes it still charges.
    pub charge: u64,
}

/// A serving response's held execution lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    /// The requesting peer.
    pub peer: ZakuraPeerId,
    /// The range's first height.
    pub start: Height,
    /// The range's requested count.
    pub count: u32,
}

/// A tapped node's block-sync owner state after a step. See the module doc.
#[derive(Clone, Debug, Default)]
pub struct BlockSyncView {
    /// The last step whose events the view folds, or `None` before the
    /// tap's first observation.
    pub step: Option<u64>,
    /// Each admitted session, by connection.
    pub sessions: BTreeMap<ZakuraConnId, Session>,
    /// Each routine's latest snapshot, by connection and generation. A
    /// dropped routine's last snapshot stays.
    pub routines: BTreeMap<(ZakuraConnId, u64), RoutineSnapshot>,
    /// The work queue's latest items.
    pub work_queue: Vec<WorkItemOwnership>,
    /// The sequencer's latest snapshot.
    pub sequencer: SequencerOwnership,
    /// Whether the sequencer task closed.
    pub sequencer_closed: bool,
    /// The byte budget's counter after its latest change.
    pub reserved: u64,
    /// The budget's latest change, if any.
    pub last_budget_change: Option<BudgetChange>,
    /// Each handoff started and not yet ended, oldest first.
    pub handoffs: Vec<Handoff>,
    /// Each holder and the body it retains, by the holder events, oldest
    /// first.
    pub holders: Vec<(Holder, HeldBody)>,
    /// Each held serving lease, oldest first.
    pub leases: Vec<Lease>,
    /// Each submission token paired with the block storage operation that
    /// took it.
    pub submissions: BTreeMap<u64, u64>,
    /// Each block storage read paired with a lease, by operation, with the
    /// lease's first height.
    pub reads: BTreeMap<u64, Height>,
    /// Events the fold couldn't apply, in order.
    pub anomalies: Vec<String>,
}

impl BlockSyncView {
    /// Folds `event`.
    pub(crate) fn fold(&mut self, event: &BlockEvent) {
        match event {
            BlockEvent::SessionAdmitted {
                peer,
                conn_id,
                session_id,
                generation,
                direction,
            } => {
                self.sessions.insert(
                    *conn_id,
                    Session {
                        peer: peer.clone(),
                        session_id: *session_id,
                        generation: *generation,
                        direction: *direction,
                    },
                );
            }
            BlockEvent::Budget(change) => {
                if change.before != self.reserved {
                    self.anomalies.push(format!(
                        "the budget changed from {}, but the view held {}",
                        change.before, self.reserved
                    ));
                }
                self.reserved = change.after;
                self.last_budget_change = Some(*change);
            }
            BlockEvent::HandoffStarted {
                peer,
                owner,
                height,
                charge,
            } => self.handoffs.push(Handoff {
                peer: peer.clone(),
                owner: *owner,
                height: *height,
                charge: *charge,
            }),
            BlockEvent::HandoffEnded {
                peer,
                owner,
                height,
                charge,
            } => {
                let ended = Handoff {
                    peer: peer.clone(),
                    owner: *owner,
                    height: *height,
                    charge: *charge,
                };
                match self.handoffs.iter().position(|open| *open == ended) {
                    Some(at) => {
                        self.handoffs.remove(at);
                    }
                    None => self.anomalies.push(format!(
                        "a handoff at height {} ended without starting",
                        height.0
                    )),
                }
            }
            BlockEvent::Holder {
                body,
                holder,
                change,
            } => match change {
                HolderChange::Retained => self.holders.push((*holder, *body)),
                HolderChange::Released => {
                    match self
                        .holders
                        .iter()
                        .position(|held| *held == (*holder, *body))
                    {
                        Some(at) => {
                            self.holders.remove(at);
                        }
                        None => self.anomalies.push(format!(
                            "{holder:?} released the body at height {} without retaining it",
                            body.height.0
                        )),
                    }
                }
            },
            BlockEvent::ServingLease {
                peer,
                start,
                count,
                step,
            } => {
                let lease = Lease {
                    peer: peer.clone(),
                    start: *start,
                    count: *count,
                };
                match step {
                    LeaseStep::Acquired => self.leases.push(lease),
                    LeaseStep::Released => {
                        match self.leases.iter().position(|held| *held == lease) {
                            Some(at) => {
                                self.leases.remove(at);
                            }
                            None => self.anomalies.push(format!(
                                "serving released a lease from height {} it never acquired",
                                start.0
                            )),
                        }
                    }
                }
            }
            BlockEvent::Routine(snapshot) => {
                self.routines
                    .insert((snapshot.conn_id, snapshot.generation), snapshot.clone());
            }
            BlockEvent::WorkQueue(items) => self.work_queue.clone_from(items),
            BlockEvent::Sequencer(ownership) => self.sequencer.clone_from(ownership),
            BlockEvent::SequencerClosed => self.sequencer_closed = true,
            // Frames, requests and the serving frames change no owner state
            // that a snapshot doesn't report.
            _ => {}
        }
    }

    /// The bytes of open handoffs, D2's `h·B` term with each handoff's own
    /// charge.
    pub fn handoff_bytes(&self) -> u64 {
        self.handoffs.iter().map(|handoff| handoff.charge).sum()
    }
}
