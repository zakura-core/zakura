//! What a tapped block-sync node's owners hold, folded from its events.
//!
//! [`BlockSyncView`] keeps each owner's latest snapshot, which its owning
//! task published, and folds the effect events between snapshots: the byte
//! budget's counter, open handoffs, each holder's body, and held serving
//! leases. Checks compare it with independent models. An event the fold
//! can't apply, such as a release of something never retained, is kept in
//! [`BlockSyncView::anomalies`] for a check to judge, since it describes
//! the node, not the harness. Each sequencer snapshot is compared with the
//! folded holders it should match, and a mismatch is an anomaly too. A block
//! storage operation that doesn't pair with the owner that should have sent
//! it is kept in [`BlockSyncView::pairing`], for the same reason.
//!
//! Sessions are never pruned: block sync reports no session close yet, so a
//! closed session's entry stays.

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    rc::Rc,
};

use zakura_chain::block::Height;
use zakura_header_chain::BodyWorkOwner;
use zakura_network::zakura::{
    block_sync_observe::{
        BlockEvent, BudgetChange, HeldBody, Holder, HolderChange, LeaseStep, RoutineSnapshot,
        SequencerOwnership, WorkItemOwnership,
    },
    ServicePeerDirection, ZakuraConnId, ZakuraPeerId,
};

use crate::domain::block_sync::OwnerModel;
use crate::domain::block_sync_semantic::SemanticModel;

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
    /// The serving session's connection.
    pub conn_id: ZakuraConnId,
    /// The serving session's routine generation.
    pub generation: u64,
    /// The range's first height.
    pub start: Height,
    /// The range's requested count.
    pub count: u32,
}

/// How a block storage operation failed to pair with the owner that should
/// have sent it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PairingKind {
    /// No retained submission, or no held serving lease, came before it.
    Unpaired,
    /// It took a submission that an earlier operation already took.
    PairedTwice,
    /// Its submission holds another owner or height, or the sequencer
    /// released that submission while it was still open.
    Mispaired,
}

/// A block storage operation, or a deferral, that didn't pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PairingAnomaly {
    /// How it failed to pair.
    pub kind: PairingKind,
    /// The storage fact's sequence number.
    pub fact: u64,
    /// The operation and what it lacked.
    pub detail: String,
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
    /// Block storage operations that didn't pair, in order.
    pub pairing: Vec<PairingAnomaly>,
    /// The semantic checks' model of the run's log, which every clone of
    /// the view shares.
    pub semantic: Rc<RefCell<SemanticModel>>,
    /// The resource properties' owner model, which folds the events as the
    /// tap logs them.
    pub owners: OwnerModel,
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
                conn_id,
                generation,
                start,
                count,
                step,
            } => {
                let lease = Lease {
                    peer: peer.clone(),
                    conn_id: *conn_id,
                    generation: *generation,
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
            BlockEvent::Sequencer(ownership) => {
                self.compare_holders(ownership);
                self.sequencer.clone_from(ownership);
            }
            BlockEvent::SequencerClosed => self.sequencer_closed = true,
            // Frames, requests and the serving frames change no owner state
            // that a snapshot doesn't report.
            _ => {}
        }
    }

    /// Records an anomaly unless `ownership`'s holders, as a multiset, are
    /// the folded holders other than the work queue's.
    fn compare_holders(&mut self, ownership: &SequencerOwnership) {
        let mut counts: HashMap<(Holder, HeldBody), i64> = HashMap::new();
        for held in ownership.holders() {
            *counts.entry(held).or_default() += 1;
        }
        for held in &self.holders {
            if !matches!(held.0, Holder::Queued { .. }) {
                *counts.entry(*held).or_default() -= 1;
            }
        }
        let mut differ: Vec<_> = counts
            .into_iter()
            .filter(|(_, count)| *count != 0)
            .map(|((holder, body), count)| format!("{holder:?} at {} ({count:+})", body.height.0))
            .collect();
        if !differ.is_empty() {
            differ.sort();
            self.anomalies.push(format!(
                "the sequencer's snapshot differs from its holder events: {}",
                differ.join(", ")
            ));
        }
    }

    /// The bytes of open handoffs, D2's `h·B` term with each handoff's own
    /// charge.
    pub fn handoff_bytes(&self) -> u64 {
        self.handoffs.iter().map(|handoff| handoff.charge).sum()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use zakura_header_chain::{
        BodyWorkAuthority, BodyWorkEpoch, BranchId, HeaderGeneration, HeaderWorkAuthority,
        VerifiedGeneration,
    };
    use zakura_network::zakura::block_sync_observe::{
        BudgetChangeKind, RetainedBody, SubmissionOwnership,
    };

    use super::*;

    /// A body at height 3.
    fn body() -> HeldBody {
        HeldBody {
            height: Height(3),
            owner: BodyWorkAuthority {
                header: HeaderWorkAuthority {
                    header_generation: HeaderGeneration::new(1),
                    branch: BranchId::new(
                        zakura_chain::block::Hash([1; 32]),
                        zakura_chain::block::Hash([2; 32]),
                    ),
                },
                verified_generation: VerifiedGeneration::new(1),
                body_work_epoch: BodyWorkEpoch::default(),
            }
            .bind(1, NonZeroU64::MIN),
            bytes: 100,
        }
    }

    /// `holder` of [`body`] changing.
    fn holder(holder: Holder, change: HolderChange) -> BlockEvent {
        BlockEvent::Holder {
            body: body(),
            holder,
            change,
        }
    }

    /// A sequencer snapshot holding [`body`] in the reorder buffer at
    /// `allocation`, and submitted under token 9 if `submitted`.
    fn snapshot(allocation: usize, submitted: bool) -> BlockEvent {
        let held = body();
        BlockEvent::Sequencer(SequencerOwnership {
            reorder: vec![RetainedBody {
                height: held.height,
                owner: held.owner,
                bytes: held.bytes,
                admitted_estimate_bytes: None,
                allocation,
            }],
            applying: Vec::new(),
            submissions: submitted
                .then_some(SubmissionOwnership {
                    height: held.height,
                    owner: held.owner,
                    token: 9,
                    bytes: held.bytes,
                    detached: false,
                })
                .into_iter()
                .collect(),
        })
    }

    #[test]
    fn a_release_never_retained_is_an_anomaly() {
        let mut view = BlockSyncView::default();
        view.fold(&holder(
            Holder::Reorder { allocation: 1 },
            HolderChange::Released,
        ));
        assert_eq!(view.anomalies.len(), 1, "{:?}", view.anomalies);
        assert!(view.holders.is_empty());
    }

    #[test]
    fn a_budget_change_from_another_count_is_an_anomaly() {
        let mut view = BlockSyncView::default();
        view.fold(&BlockEvent::Budget(BudgetChange {
            kind: BudgetChangeKind::Reserved,
            before: 0,
            after: 100,
            limit: 1_000,
        }));
        assert!(view.anomalies.is_empty());
        view.fold(&BlockEvent::Budget(BudgetChange {
            kind: BudgetChangeKind::Released,
            before: 50,
            after: 0,
            limit: 1_000,
        }));
        assert_eq!(view.anomalies.len(), 1, "{:?}", view.anomalies);
        assert_eq!(view.reserved, 0);
    }

    #[test]
    fn a_snapshot_must_match_the_holder_events() {
        let mut view = BlockSyncView::default();
        view.fold(&holder(
            Holder::Queued { allocation: 1 },
            HolderChange::Retained,
        ));
        view.fold(&holder(
            Holder::Reorder { allocation: 1 },
            HolderChange::Retained,
        ));
        view.fold(&snapshot(1, false));
        assert!(view.anomalies.is_empty(), "{:?}", view.anomalies);
        // A submission no holder event retained.
        view.fold(&snapshot(1, true));
        assert_eq!(view.anomalies.len(), 1, "{:?}", view.anomalies);
        // A reorder body at another allocation than the events say.
        view.fold(&snapshot(2, false));
        assert_eq!(view.anomalies.len(), 2, "{:?}", view.anomalies);
    }
}
