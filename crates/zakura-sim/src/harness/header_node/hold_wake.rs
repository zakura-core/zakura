//! `header-sync/hold-release-wakes`'s storage facts: which held operations'
//! replies reached the reactor, and which of those calls it handled.
//!
//! A hold keeps an operation pending until it opens, by a `Release` step or
//! by its own trigger, and a reply hold keeps a committed apply's reply
//! back the same way. Once the held operation completes with its reply
//! delivered to the reactor's waiting call, the reactor waits on nothing
//! else for it: its loop drains ready completions at the top of each turn,
//! and its `select!` has a completion arm. So the check opens one fact,
//! [`HoldWakeFact::Opened`], per held operation whose reply was delivered
//! to a reactor call, whatever the result, and records the reactor's
//! handling of that call as [`HoldWakeFact::Handled`]. An operation neither
//! kind of hold picked opens nothing, and neither does a reply nobody was
//! waiting for.
//!
//! A held serving read, a publisher page job's `read_header_path`, opens
//! [`HoldWakeFact::Served`] once it completes with a page delivered to that
//! job: the job then returns the page, and the publisher waits on nothing
//! else to hand it to its writer. The `PageQueued` for that read's page
//! answers it as [`HoldWakeFact::Queued`], joined by the read's peer,
//! session and lease. Each lease serves one read, so a page another read
//! of the session produced, for another subscription or a later page,
//! answers nothing. A read that completes
//! unavailable or failed opens nothing, since the publisher may retry or
//! stall on it. A held read completes only at a storage step, and a page is
//! queued only in a task's, so no step holds both, and the tap's pages,
//! which [`HoldWakeCheck::queued`] takes after the step's storage facts,
//! never answer a read the same step completed.
//!
//! The harness's storage source keeps the check, since only it knows which
//! operations a hold picked ([`super::policy::Predicates::holds_op`]). It
//! reads the step's storage facts and the reactor facts its tap logged, in
//! log order. Each fact takes the node's next stamp ([`super::stamp`]),
//! which the storage view hands the registry as its view-side facts.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

use zakura_scheduler::SimInstant;

use super::{policy::Predicates, stamp::Stamps};
use crate::observe::{ReactorFact, StorageFact, StorageRequest, StorageResult};

/// A fact the hold-wake check found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HoldWakeFact {
    /// Held operation `op` completed, and its reply reached the reactor's
    /// waiting call `call`.
    Opened {
        /// The reactor's port call.
        call: u64,
        /// The held storage operation.
        op: u64,
        /// When the reply was delivered.
        at: SimInstant,
    },
    /// The reactor handled call `call`, whose held operation `op` had
    /// opened a fact.
    Handled {
        /// The reactor's port call.
        call: u64,
        /// Its held storage operation.
        op: u64,
    },
    /// Held serving read `op`, a page read for `peer`'s session
    /// `session_id`, completed with a page delivered to its page job.
    Served {
        /// The held storage operation.
        op: u64,
        /// The mirror served, by its harness name.
        peer: String,
        /// The session the read's lease names.
        session_id: u64,
        /// When the page was delivered.
        at: SimInstant,
    },
    /// The publisher of session `session_id` handed served read `op`'s page
    /// to its writer.
    Queued {
        /// The served read.
        op: u64,
        /// The session.
        session_id: u64,
    },
}

/// What [`HoldWakeEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// Whether a harness that registers the property asked for the facts.
    armed: bool,
    /// Where its stamps come from: the node's, once its harness hands them
    /// over.
    stamps: Stamps,
    /// Each fact with its stamp, in stamp order.
    facts: Vec<(u64, HoldWakeFact)>,
}

/// One node's hold-wake facts, shared by its storage source and whoever
/// reads them. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct HoldWakeEvidence(Arc<Mutex<Store>>);

impl HoldWakeEvidence {
    /// Evidence a harness's storage source fills, for a harness that
    /// registers `header-sync/hold-release-wakes`. The default stays empty.
    pub fn armed() -> HoldWakeEvidence {
        HoldWakeEvidence(Arc::new(Mutex::new(Store {
            armed: true,
            ..Store::default()
        })))
    }

    /// Whether [`HoldWakeEvidence::armed`] made it.
    pub fn is_armed(&self) -> bool {
        self.with(|store| store.armed)
    }

    /// Stamps facts from `stamps`, the node's, from now on.
    pub(crate) fn stamp_with(&self, stamps: Stamps) {
        self.with(|store| store.stamps = stamps);
    }

    /// Runs `read` on the store. Nothing else is called while the lock is
    /// held.
    fn with<R>(&self, read: impl FnOnce(&mut Store) -> R) -> R {
        read(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Keeps `fact` with the next stamp.
    fn push(&self, fact: HoldWakeFact) {
        self.with(|store| {
            let stamp = store.stamps.next();
            store.facts.push((stamp, fact));
        });
    }

    /// The fact with `stamp`, if this store holds it.
    pub fn fact(&self, stamp: u64) -> Option<HoldWakeFact> {
        self.with(|store| {
            store
                .facts
                .binary_search_by_key(&stamp, |(at, _)| *at)
                .ok()
                .map(|index| store.facts[index].1.clone())
        })
    }

    /// The mirror served read `op` was for, by its harness name, if `op`
    /// opened a [`HoldWakeFact::Served`].
    pub fn served_peer(&self, op: u64) -> Option<String> {
        self.with(|store| {
            store.facts.iter().find_map(|(_, fact)| match fact {
                HoldWakeFact::Served {
                    op: served, peer, ..
                } if *served == op => Some(peer.clone()),
                _ => None,
            })
        })
    }

    /// Every fact so far, with its stamp.
    pub fn facts(&self) -> Vec<(u64, HoldWakeFact)> {
        self.with(|store| store.facts.clone())
    }
}

/// The check's state: the held operations a reactor call submitted, the
/// calls whose held reply was delivered, and the same for held serving
/// reads.
#[derive(Debug)]
pub(crate) struct HoldWakeCheck {
    evidence: HoldWakeEvidence,
    /// Each held operation a reactor call submitted, with that call, until
    /// it completes.
    held: BTreeMap<u64, u64>,
    /// Each call whose held reply was delivered, with its operation, until
    /// the reactor handles it.
    opened: BTreeMap<u64, u64>,
    /// Each held serving read, with its lease, until it completes.
    reading: BTreeMap<u64, Lease>,
    /// Each served read, with its lease, until its page is queued.
    served: BTreeMap<u64, Lease>,
}

impl HoldWakeCheck {
    /// A check that keeps its facts in `evidence`.
    pub(crate) fn new(evidence: HoldWakeEvidence) -> HoldWakeCheck {
        HoldWakeCheck {
            evidence,
            held: BTreeMap::new(),
            opened: BTreeMap::new(),
            reading: BTreeMap::new(),
            served: BTreeMap::new(),
        }
    }

    /// Notes a storage fact of the node's port, once `predicates` has
    /// counted it. `name` gives a peer's harness name from its storage
    /// name.
    pub(crate) fn storage(
        &mut self,
        fact: &StorageFact,
        predicates: &Predicates,
        name: impl Fn(&str) -> Option<String>,
    ) {
        match fact {
            StorageFact::Submitted {
                id,
                request:
                    StorageRequest::ReadHeaderPath {
                        lease: Some(lease), ..
                    },
                ..
            } if predicates.holds_op(*id) => {
                self.reading.insert(
                    *id,
                    Lease {
                        peer: lease.peer.clone(),
                        session_id: lease.session_id,
                        lease_id: lease.lease_id,
                    },
                );
            }
            StorageFact::Submitted {
                id,
                call: Some(call),
                ..
            } if predicates.holds_op(*id) => {
                self.held.insert(*id, *call);
            }
            StorageFact::Completed {
                id,
                delivered,
                result,
                ..
            } => {
                let at = zakura_scheduler::hooks::now().unwrap_or_default();
                if let Some(call) = self.held.remove(id) {
                    if *delivered {
                        self.opened.insert(call, *id);
                        self.evidence
                            .push(HoldWakeFact::Opened { call, op: *id, at });
                    }
                }
                if let Some(lease) = self.reading.remove(id) {
                    if let (true, StorageResult::Page { .. }, Some(mirror)) =
                        (*delivered, result, name(&lease.peer))
                    {
                        self.evidence.push(HoldWakeFact::Served {
                            op: *id,
                            peer: mirror,
                            session_id: lease.session_id,
                            at,
                        });
                        self.served.insert(*id, lease);
                    }
                }
            }
            _ => {}
        }
    }

    /// Notes that `peer`'s publisher on session `session_id` handed the
    /// page its read on lease `lease_id` produced to its writer, which
    /// answers that read if it was served.
    pub(crate) fn queued(&mut self, peer: &str, session_id: u64, lease_id: u64) {
        let answered = self.served.iter().find_map(|(op, lease)| {
            (lease.peer == peer && lease.session_id == session_id && lease.lease_id == lease_id)
                .then_some(*op)
        });
        if let Some(op) = answered {
            self.served.remove(&op);
            self.evidence.push(HoldWakeFact::Queued { op, session_id });
        }
    }

    /// Notes a reactor fact the node's tap logged.
    pub(crate) fn reactor(&mut self, fact: &ReactorFact) {
        if let ReactorFact::Handled { call, .. } = fact {
            if let Some(op) = self.opened.remove(call) {
                self.evidence
                    .push(HoldWakeFact::Handled { call: *call, op });
            }
        }
    }
}

/// A serving read's lease: its peer, as storage names it, its session and
/// the lease ID storage issued.
#[derive(Debug)]
struct Lease {
    peer: String,
    session_id: u64,
    lease_id: u64,
}

#[cfg(test)]
mod tests;
