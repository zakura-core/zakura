//! [`QUERY_WORK_AUTHORIZED`](super::QUERY_WORK_AUTHORIZED)'s and
//! [`FRONTIER_WORK_CONSISTENT`](super::FRONTIER_WORK_CONSISTENT)'s model:
//! the body-work authority the reactor adopted, and the body work each
//! owner held after it.
//!
//! The properties' binding (properties.md, Observation bindings):
//!
//! - **Contract.** `ReactorState::observe_committed_view` and
//!   `classify_needed_reply` in `block_sync/reactor/state.rs`, and
//!   `change_body_work_epoch` in `block_sync/sequencer_task/state.rs`. A
//!   committed view whose body-work authority differs from the adopted one
//!   retires the pending query, and only a reply under the adopted
//!   authority installs work. A new body-work epoch resets the sequencer and
//!   the work queue above the verified tip, so no older epoch's body work
//!   survives the transition.
//! - **Facts.** Each `BlockSyncFact::Reactor` transition, and the work
//!   queue's, routines' and sequencer's snapshots.
//! - **Capture boundary.** `ReactorIO::report_transition` right after each
//!   adoption and reply classification, and each owner's snapshot right
//!   after its own change.
//! - **Correlation.** The exact body-work authority: header generation,
//!   branch anchor and target hashes, verified generation and body-work
//!   epoch. Heights alone never match.
//! - **Evaluation.** The query check runs at every reply. The frontier check
//!   runs whenever no task can run, so every message the epoch change sent
//!   has been handled.
//! - **Availability.** The `Sim` tier, through the tap. Both read
//!   `Source::Storage` and `Source::View`.
//! - **Checker and witness.** `check_query_work_authorized` with
//!   `reply_under_retired_authority`, and `check_frontier_work_consistent`
//!   with `epoch_changed_with_work`.

use std::collections::BTreeMap;

use super::Finding;
use crate::{
    observe::{BlockSyncFact, WorkOwner},
    record::machine::{BodyAuthority, ReactorAction, ReactorReply},
};

/// One body-work owner's hold, as a snapshot reported it.
#[derive(Clone, Debug)]
struct Held {
    /// Where: `work-queue`, `routine` or `sequencer`.
    holder: &'static str,
    height: u32,
    owner: WorkOwner,
}

/// The adopted authority, and the body work each owner holds.
#[derive(Debug, Default)]
pub struct AuthorityModel {
    /// The authority of the reactor's latest `AuthorityChanged`, or its
    /// startup's.
    adopted: Option<BodyAuthority>,
    /// The verified tip the reactor's latest transition reported.
    verified_tip: u32,
    /// The work queue's taken items, by its latest snapshot.
    work: Vec<Held>,
    /// Each live routine's unreceived requested heights, by connection and
    /// generation, by its latest snapshot.
    routines: BTreeMap<(u64, u64), Vec<Held>>,
    /// The sequencer's reorder and `applying` bodies, by its latest
    /// snapshot.
    sequencer: Vec<Held>,
    /// The epoch the frontier check last found stale work under, so one
    /// lapse is reported once.
    reported: Option<u64>,
    /// Findings for `QUERY_WORK_AUTHORIZED`, not yet reported.
    pub(super) query_findings: Vec<Finding>,

    /// Accepted replies judged.
    pub accepted: u64,
    /// New body-work epochs the reactor adopted.
    pub epoch_changes: u64,
    /// Whether a reply reached the reactor under an authority other than
    /// the adopted one.
    pub saw_retired_reply: bool,
    /// Whether the reactor adopted a new body-work epoch while an owner held
    /// body work from an older one.
    pub saw_epoch_change_with_work: bool,
}

impl AuthorityModel {
    /// Folds `event`, at `step`.
    pub(super) fn fold(&mut self, step: u64, event: &BlockSyncFact) {
        match event {
            BlockSyncFact::Reactor { transition, state } => {
                self.verified_tip = state.verified_tip;
                // A node started on a committed view adopts its authority at
                // startup, with no `AuthorityChanged`. No transition but
                // `AuthorityChanged` changes the authority, so the first
                // state reports the startup's.
                if self.adopted.is_none() {
                    self.adopted.clone_from(&state.scope);
                }
                self.transition(step, transition);
            }
            BlockSyncFact::WorkQueue { items } => {
                self.work = items
                    .iter()
                    .filter_map(|item| {
                        Some(Held {
                            holder: "work-queue",
                            height: item.height,
                            owner: item.owner.clone()?,
                        })
                    })
                    .collect();
            }
            BlockSyncFact::Routine {
                connection,
                generation,
                requests,
                ended,
                ..
            } => {
                let key = (*connection, *generation);
                if *ended {
                    self.routines.remove(&key);
                    return;
                }
                let held = requests
                    .iter()
                    .flat_map(|request| {
                        request
                            .heights
                            .iter()
                            .filter(|height| !height.received)
                            .map(|height| Held {
                                holder: "routine",
                                height: height.height,
                                owner: request.owner.clone(),
                            })
                    })
                    .collect();
                self.routines.insert(key, held);
            }
            BlockSyncFact::Sequencer {
                reorder, applying, ..
            } => {
                self.sequencer = reorder
                    .iter()
                    .chain(applying)
                    .map(|body| Held {
                        holder: "sequencer",
                        height: body.body.height,
                        owner: body.body.owner.clone(),
                    })
                    .collect();
            }
            _ => {}
        }
    }

    /// Folds a reactor transition, at `step`.
    fn transition(&mut self, step: u64, transition: &ReactorAction) {
        match transition {
            ReactorAction::AuthorityChanged {
                authority,
                epoch_changed,
            } => {
                if *epoch_changed {
                    self.epoch_changes += 1;
                    if self.stale(authority.body_work_epoch).next().is_some() {
                        self.saw_epoch_change_with_work = true;
                    }
                }
                self.adopted = Some(authority.clone());
            }
            ReactorAction::QueryReply {
                query_id,
                scope,
                outcome,
            } => {
                if self.adopted.as_ref() == Some(scope) {
                    if *outcome == ReactorReply::Accepted {
                        self.accepted += 1;
                    }
                    return;
                }
                self.saw_retired_reply = true;
                if *outcome == ReactorReply::Accepted {
                    self.query_findings.push(Finding {
                        step,
                        expected: format!(
                            "a reply installs work only under the adopted authority {:?}",
                            self.adopted
                        ),
                        actual: format!("query {query_id}'s reply was accepted under {scope:?}"),
                    });
                }
            }
            _ => {}
        }
    }

    /// Every hold above the verified tip whose owner names a body-work
    /// epoch other than `epoch`. A taken item at or below the tip is a
    /// committed height's leftover, which holds no budget and which the
    /// next floor advance drops, so it isn't work.
    fn stale(&self, epoch: u64) -> impl Iterator<Item = &Held> {
        let tip = self.verified_tip;
        self.work
            .iter()
            .chain(self.routines.values().flatten())
            .chain(&self.sequencer)
            .filter(move |held| {
                held.height > tip && held.owner.body_work_epoch.is_some_and(|own| own != epoch)
            })
    }

    /// `FRONTIER_WORK_CONSISTENT`'s finding at `step`, where no task can
    /// run: a hold from an epoch other than the adopted one, reported once
    /// per adopted epoch.
    pub(super) fn judge_frontier(&mut self, step: u64) -> Option<Finding> {
        let epoch = self.adopted.as_ref()?.body_work_epoch;
        if self.reported == Some(epoch) {
            return None;
        }
        let stale: Vec<String> = self
            .stale(epoch)
            .map(|held| {
                format!(
                    "{} at {} under epoch {:?}",
                    held.holder, held.height, held.owner.body_work_epoch
                )
            })
            .collect();
        if stale.is_empty() {
            return None;
        }
        self.reported = Some(epoch);
        Some(Finding {
            step,
            expected: format!(
                "once no task can run, every owner holds only body-work epoch {epoch}'s work"
            ),
            actual: stale.join(", "),
        })
    }
}
