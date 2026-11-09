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
use crate::observe::{ReactorFact, StorageFact};

/// A fact the hold-wake check found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
                .map(|index| store.facts[index].1)
        })
    }

    /// Every fact so far, with its stamp.
    pub fn facts(&self) -> Vec<(u64, HoldWakeFact)> {
        self.with(|store| store.facts.clone())
    }
}

/// The check's state: the held operations a reactor call submitted, and the
/// calls whose held reply was delivered.
#[derive(Debug)]
pub(crate) struct HoldWakeCheck {
    evidence: HoldWakeEvidence,
    /// Each held operation a reactor call submitted, with that call, until
    /// it completes.
    held: BTreeMap<u64, u64>,
    /// Each call whose held reply was delivered, with its operation, until
    /// the reactor handles it.
    opened: BTreeMap<u64, u64>,
}

impl HoldWakeCheck {
    /// A check that keeps its facts in `evidence`.
    pub(crate) fn new(evidence: HoldWakeEvidence) -> HoldWakeCheck {
        HoldWakeCheck {
            evidence,
            held: BTreeMap::new(),
            opened: BTreeMap::new(),
        }
    }

    /// Notes a storage fact of the node's port, once `predicates` has
    /// counted it.
    pub(crate) fn storage(&mut self, fact: &StorageFact, predicates: &Predicates) {
        match fact {
            StorageFact::Submitted {
                id,
                call: Some(call),
                ..
            } if predicates.holds_op(*id) => {
                self.held.insert(*id, *call);
            }
            StorageFact::Completed { id, delivered, .. } => {
                if let Some(call) = self.held.remove(id) {
                    if *delivered {
                        self.opened.insert(call, *id);
                        let at = zakura_scheduler::hooks::now().unwrap_or_default();
                        self.evidence
                            .push(HoldWakeFact::Opened { call, op: *id, at });
                    }
                }
            }
            _ => {}
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

#[cfg(test)]
mod tests;
