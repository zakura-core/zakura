//! `header-sync/stale-chain-effect-inert`'s check, on the node's storage
//! facts as its storage source logs them and the reactor's adoptions as
//! its tap receives them.
//!
//! It keeps apart the three things a stale apply could reach: storage's
//! commit, the completion's delivery, and the reactor's adoption.
//!
//! - **Commit.** An apply whose execution changes the engine carries an
//!   owner header generation and anchor equal to the engine's just before
//!   it ran. An apply executes once: as `Committed` when its reply is held,
//!   or as a `Completed` that no `Committed` came before. It changed the
//!   engine when the fact's state digest differs from the engine's before.
//!   The engine changes only in steps of one action each, so its
//!   generation, anchor and digest read at the end of the previous step's
//!   observation are the ones before. The rule is the fixed-finality one:
//!   the simulator never advances finality, so production's finality
//!   rebase, which can commit an owner from an older anchor after rebasing
//!   it, never runs here, and any other authority is stale.
//! - **Delivery** isn't judged here. A delivered completion's direct
//!   effects are `header-sync/stale-completion-inert`'s.
//! - **Adoption.** The reactor's `SnapshotAdopted` state version and
//!   header generation never decrease.
//!
//! The check applies once an apply changed the engine, or the reactor
//! adopted a second snapshot. A broken rule is a claim break, which the
//! tap hands [`Claim::StaleChainEffect`](super::claim::Claim) at its drain.
//! An apply execution, or any completion, whose submission storage never
//! reported is missing evidence, which fails the run.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

use zakura_network::zakura::observe::ReactorEvent;

use crate::observe::{StorageFact, StorageRequest, WorkScope};

/// The engine's state as the check compares it: its header generation,
/// its finality anchor's hash, and its state digest, as hex.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct EngineAt {
    /// The header generation.
    pub(super) generation: u64,
    /// The finality anchor's hash.
    pub(super) anchor: String,
    /// The state digest.
    pub(super) state: String,
}

/// What [`ChainEffectEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// The first drain that found the check applicable.
    applicable: Option<u64>,
    /// Whether a fact since the last drain made it applicable.
    pending: bool,
}

/// When the stale-chain check first applied, shared by the tap and whoever
/// reads it. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct ChainEffectEvidence(Arc<Mutex<Store>>);

impl ChainEffectEvidence {
    /// Runs `change` on the store. Nothing else is called while the lock
    /// is held.
    fn with<R>(&self, change: impl FnOnce(&mut Store) -> R) -> R {
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Whether a drain up to and including `step`'s found the check
    /// applicable.
    pub fn applicable_by(&self, step: u64) -> bool {
        self.with(|store| store.applicable.is_some_and(|found| found <= step))
    }

    /// Whether any drain found the check applicable.
    pub fn any(&self) -> bool {
        self.with(|store| store.applicable.is_some())
    }
}

/// The check's state.
pub(super) struct ChainEffect {
    /// The engine before the current step.
    engine: EngineAt,
    /// Each apply operation's owner scope, by storage operation.
    applies: HashMap<u64, WorkScope>,
    /// Every storage operation submitted so far.
    submitted: HashSet<u64>,
    /// The apply operations that committed with their reply held.
    committed: HashSet<u64>,
    /// The last adoption's state version and header generation.
    adopted: Option<(u64, u64)>,
    breaks: Vec<String>,
    /// The first missing-evidence failure.
    missing: Option<String>,
    evidence: ChainEffectEvidence,
}

impl ChainEffect {
    /// A check that keeps when it applied in `evidence`, on an engine at
    /// `engine` now.
    pub(super) fn new(evidence: ChainEffectEvidence, engine: EngineAt) -> ChainEffect {
        ChainEffect {
            engine,
            applies: HashMap::new(),
            submitted: HashSet::new(),
            committed: HashSet::new(),
            adopted: None,
            breaks: Vec::new(),
            missing: None,
            evidence,
        }
    }

    /// Judges `event`, which the reactor's own task reported when
    /// `on_reactor`.
    pub(super) fn observe(&mut self, event: &ReactorEvent, on_reactor: bool) {
        let ReactorEvent::SnapshotAdopted {
            state_version,
            header_generation,
            ..
        } = event
        else {
            return;
        };
        if !on_reactor {
            return;
        }
        let adopted = (state_version.get(), header_generation.get());
        if let Some((version, generation)) = self.adopted {
            self.applies();
            if adopted.0 < version || adopted.1 < generation {
                self.breaks.push(format!(
                    "StaleChainEffect: the reactor adopted state version {} at generation {} \
                     after version {version} at generation {generation}",
                    adopted.0, adopted.1
                ));
            }
        }
        self.adopted = Some(adopted);
    }

    /// Notes the node's storage facts one step's drain logged, in order,
    /// then that the engine is at `engine` once the step is done.
    pub(super) fn storage<'f>(
        &mut self,
        facts: impl IntoIterator<Item = &'f StorageFact>,
        engine: EngineAt,
    ) {
        for fact in facts {
            match fact {
                StorageFact::Submitted { id, request, .. } => {
                    self.submitted.insert(*id);
                    if let StorageRequest::ApplyHeaderTarget { owner, .. } = request {
                        self.applies.insert(*id, owner.scope.clone());
                    }
                }
                StorageFact::Committed { id, state, .. } => {
                    self.committed.insert(*id);
                    self.executed(*id, state);
                }
                StorageFact::Completed { id, .. } if !self.submitted.contains(id) => {
                    self.missing.get_or_insert(format!(
                        "ChainEffectJoin: operation {id} completed with no submission storage \
                         reported"
                    ));
                }
                StorageFact::Completed { id, state, .. }
                    if !self.committed.contains(id) && self.applies.contains_key(id) =>
                {
                    self.executed(*id, state);
                }
                _ => {}
            }
        }
        self.engine = engine;
    }

    /// The breaks found since the last call, or the first missing evidence.
    /// Marks the check applicable at `step` if a fact since the last call
    /// made it so.
    pub(super) fn drain(&mut self, step: u64) -> Result<Vec<String>, String> {
        // Marked even with evidence missing, so a break the tap takes ahead
        // of that failure keeps its trigger history.
        self.evidence.with(|store| {
            if std::mem::take(&mut store.pending) {
                store.applicable.get_or_insert(step);
            }
        });
        if let Some(missing) = &self.missing {
            return Err(missing.clone());
        }
        Ok(std::mem::take(&mut self.breaks))
    }

    /// The breaks found since the last call, taken even when
    /// [`drain`](Self::drain) failed with missing evidence, so the tap can
    /// report them ahead of that failure.
    pub(super) fn take_breaks(&mut self) -> Vec<String> {
        std::mem::take(&mut self.breaks)
    }

    /// Notes that a fact made the check applicable.
    fn applies(&self) {
        self.evidence.with(|store| store.pending = true);
    }

    /// Judges apply operation `id`'s execution, which left the engine's
    /// state digest at `state`.
    fn executed(&mut self, id: u64, state: &str) {
        let Some(owner) = self.applies.get(&id) else {
            self.missing.get_or_insert(format!(
                "ChainEffectJoin: apply operation {id} executed with no submission storage \
                 reported"
            ));
            return;
        };
        if state == self.engine.state {
            return;
        }
        self.applies();
        if owner.header_generation != self.engine.generation
            || owner.branch.anchor != self.engine.anchor
        {
            self.breaks.push(format!(
                "StaleChainEffect: apply operation {id} under generation {} and anchor {} \
                 changed the engine at generation {} and anchor {}",
                owner.header_generation,
                owner.branch.anchor,
                self.engine.generation,
                self.engine.anchor
            ));
        }
    }
}

#[cfg(test)]
mod tests;
