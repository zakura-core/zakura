//! `header-sync/reset-invalidates-work`'s check, on the reactor's facts as
//! its tap receives them and the node's storage facts as its storage source
//! logs them.
//!
//! "Latest" means the reactor's latest `SnapshotAdopted`, never the
//! engine's latest publication: a reset invalidates the node's work at the
//! unit of work that adopts it. A round is *current-started* when its
//! `Started` generation and anchor equal the latest adopted ones then. A
//! round started stale, or under a wrong anchor, is another property's, so
//! none of the rules below judge it.
//!
//! - **Retirement**, judged at every `CauseSet`, where the reactor ends or
//!   starts a unit of work. Every slot the queue's `Slots` lists that awaits
//!   its locator has the latest generation and anchor. Every slot holding a
//!   current-started round whose `Started` anchor is the latest one has the
//!   latest generation and anchor in its owner, and its round started under
//!   the latest generation, whatever its owner says now. The queue's own
//!   record is read, since it reports `RoundReleased` before it drops an
//!   entry. A round whose `Started` anchor isn't the latest one is the
//!   finality rebase path, which the simulator never reaches, so it isn't
//!   judged. The exception follows the round's start, never its owner now.
//! - **Dispatch.** A prepare or apply dispatched for a peer whose slot holds
//!   a live current-started round carries the latest generation and anchor
//!   in its owner, unless the round's `Started` anchor isn't the latest.
//! - **Changes advance the generation.** Every acted `Reset` names a header
//!   generation above the engine's before the step that reset it. The
//!   engine changes only in steps, one action each, so its generation read
//!   at the end of the previous step's observation is the one before.
//! - **Locator consumption.** Every ordinary `Started` is reported under a
//!   locator call's `Completion`, with that call's dispatched generation,
//!   and the read storage answered it with has a generation and anchor no
//!   older than the latest adopted ones then.
//!
//! The check applies once an adoption changes the generation while a
//! current-started round is live, an ordinary round starts, or a reset is
//! acted. A broken rule is a claim break, which the tap hands
//! [`Claim::ResetInvalidatesWork`](super::claim::Claim) at its drain. A
//! locator `Started` whose read storage never reported is missing evidence,
//! which fails the run.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, PoisonError},
};

use zakura_chain::block;
use zakura_network::zakura::{
    observe::{PortCall, ReactorCause, ReactorEvent, WorkSlot},
    HeaderTargetPurpose,
};

use crate::observe::{StorageFact, StorageResult};

/// A header generation and the finality anchor's hash, as authority names
/// them.
type Authority = (u64, block::Hash);

/// A locator read's result, as storage reported it.
#[derive(Clone, Copy, Debug)]
struct Read {
    generation: u64,
    anchor_height: u32,
}

/// A started round, as its `Started` reported it.
#[derive(Clone, Copy, Debug)]
struct Round {
    authority: Authority,
    /// Whether it started under the latest adopted authority.
    current: bool,
}

/// What [`RoundLifeEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// The first drain that found the check applicable.
    applicable: Option<u64>,
    /// Each applicable fact found since the last drain.
    pending: bool,
}

/// When the reset check first applied, shared by the tap and whoever reads
/// it. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct RoundLifeEvidence(Arc<Mutex<Store>>);

impl RoundLifeEvidence {
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

/// The check's state: what the facts so far say about adoption, slots,
/// rounds, locator reads and the engine's generation, and the breaks not
/// yet drained.
pub(super) struct RoundLife {
    /// The unit of work the reactor's last `CauseSet` named.
    cause: ReactorCause,
    /// The latest adopted generation and anchor, with the anchor's height.
    adopted: Option<(Authority, u32)>,
    /// The queue's slots, as its last `Slots` listed them.
    slots: Vec<WorkSlot>,
    /// Each started round, by incarnation.
    rounds: HashMap<u64, Round>,
    /// Each locator call's dispatched generation, by call.
    locators: HashMap<u64, u64>,
    /// The storage operation each reactor call submitted, by call.
    submitted: HashMap<u64, u64>,
    /// Each locator read's result, by storage operation.
    reads: HashMap<u64, Read>,
    /// The engine's header generation before the current step.
    engine: u64,
    /// The breaks already found, so a slot kept across units breaks once.
    reported: BTreeSet<String>,
    breaks: Vec<String>,
    /// The first missing-evidence failure.
    missing: Option<String>,
    evidence: RoundLifeEvidence,
}

impl RoundLife {
    /// A check that keeps when it applied in `evidence`, on an engine whose
    /// header generation is `engine` now.
    pub(super) fn new(evidence: RoundLifeEvidence, engine: u64) -> RoundLife {
        RoundLife {
            cause: ReactorCause::Idle,
            adopted: None,
            slots: Vec::new(),
            rounds: HashMap::new(),
            locators: HashMap::new(),
            submitted: HashMap::new(),
            reads: HashMap::new(),
            engine,
            reported: BTreeSet::new(),
            breaks: Vec::new(),
            missing: None,
            evidence,
        }
    }

    /// Judges `event`, which the reactor's own task reported when
    /// `on_reactor`, then notes what it says.
    pub(super) fn observe(&mut self, event: &ReactorEvent, on_reactor: bool) {
        if !on_reactor {
            return;
        }
        match event {
            ReactorEvent::CauseSet { cause } => {
                self.judge_slots();
                self.cause = *cause;
            }
            ReactorEvent::SnapshotAdopted {
                header_generation,
                finalized,
                ..
            } => {
                let latest = (
                    (header_generation.get(), finalized.hash),
                    finalized.height.0,
                );
                let changed = self
                    .adopted
                    .is_some_and(|(adopted, _)| adopted.0 != latest.0 .0);
                if changed && self.live_current_round() {
                    self.applies();
                }
                self.adopted = Some(latest);
            }
            ReactorEvent::Slots { slots } => self.slots = slots.clone(),
            ReactorEvent::Started {
                request, purpose, ..
            } => {
                let authority = authority_of(request.owner.header_authority());
                let current = self.latest() == Some(authority);
                self.rounds
                    .insert(request.incarnation, Round { authority, current });
                if *purpose == HeaderTargetPurpose::Normal {
                    self.judge_start(request.incarnation, authority);
                }
            }
            ReactorEvent::Dispatched { call, op } => match op {
                PortCall::Locator { scope, .. } => {
                    self.locators.insert(*call, scope.header_generation.get());
                }
                PortCall::Prepare { peer, owner, .. } | PortCall::Apply { peer, owner, .. } => {
                    let dispatched = authority_of(owner.header_authority());
                    let live = self.slots.iter().find_map(|slot| {
                        let round = self.rounds.get(&slot.incarnation)?;
                        (slot.peer == *peer && slot.phase.is_some() && round.current)
                            .then_some((slot, round))
                    });
                    if let (Some((slot, round)), Some(latest)) = (live, self.latest()) {
                        // The rebase exception follows the round's own start, so
                        // an owner rewritten to another anchor isn't excused.
                        if round.authority.1 == latest.1 && dispatched != latest {
                            let kind = if matches!(op, PortCall::Apply { .. }) {
                                "apply"
                            } else {
                                "prepare"
                            };
                            self.breaks.push(format!(
                                "ResetInvalidatesWork: call {call} dispatched round {}'s {kind} \
                                 under generation {} after the reactor adopted {}",
                                slot.incarnation, dispatched.0, latest.0
                            ));
                        }
                    }
                }
                PortCall::VctContext { .. } => {}
            },
            _ => {}
        }
    }

    /// Notes the node's storage facts one step's drain logged, in order,
    /// then that the engine's header generation is `engine` once the step
    /// is done.
    pub(super) fn storage<'f>(
        &mut self,
        facts: impl IntoIterator<Item = &'f StorageFact>,
        engine: u64,
    ) {
        for fact in facts {
            match fact {
                StorageFact::Submitted {
                    id,
                    call: Some(call),
                    ..
                } => {
                    self.submitted.insert(*call, *id);
                }
                StorageFact::Completed {
                    id,
                    result:
                        StorageResult::Locator {
                            header_generation,
                            finalized,
                            ..
                        },
                    ..
                } => {
                    self.reads.insert(
                        *id,
                        Read {
                            generation: *header_generation,
                            anchor_height: finalized.height,
                        },
                    );
                }
                StorageFact::Reset {
                    kind, generation, ..
                } => {
                    self.applies();
                    if *generation <= self.engine {
                        self.breaks.push(format!(
                            "ResetInvalidatesWork: an operator {kind} left the header generation at \
                             {generation}, from {}",
                            self.engine
                        ));
                    }
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
        if let Some(missing) = &self.missing {
            return Err(missing.clone());
        }
        self.evidence.with(|store| {
            if std::mem::take(&mut store.pending) {
                store.applicable.get_or_insert(step);
            }
        });
        Ok(std::mem::take(&mut self.breaks))
    }

    /// The latest adopted generation and anchor.
    fn latest(&self) -> Option<Authority> {
        self.adopted.map(|(authority, _)| authority)
    }

    /// Notes that a fact made the check applicable.
    fn applies(&self) {
        self.evidence.with(|store| store.pending = true);
    }

    /// Whether a slot holds a current-started round.
    fn live_current_round(&self) -> bool {
        self.slots.iter().any(|slot| {
            slot.phase.is_some()
                && self
                    .rounds
                    .get(&slot.incarnation)
                    .is_some_and(|round| round.current)
        })
    }

    /// Judges the retirement rules against the queue's slots.
    fn judge_slots(&mut self) {
        let Some(latest) = self.latest() else {
            return;
        };
        let mut found = Vec::new();
        for slot in &self.slots {
            let held = authority_of(slot.authority);
            match slot.phase {
                None if held != latest => found.push(format!(
                    "ResetInvalidatesWork: {}'s slot awaits its locator under generation {} \
                     after the reactor adopted {}",
                    hex_peer(slot),
                    held.0,
                    latest.0
                )),
                None => {}
                Some(_) => {
                    let Some(round) = self.rounds.get(&slot.incarnation) else {
                        continue;
                    };
                    if !round.current || round.authority.1 != latest.1 {
                        continue;
                    }
                    if round.authority.0 != latest.0 {
                        found.push(format!(
                            "ResetInvalidatesWork: round {}, started under generation {}, \
                             is still in the queue after the reactor adopted {}",
                            slot.incarnation, round.authority.0, latest.0
                        ));
                    } else if held != latest {
                        found.push(format!(
                            "ResetInvalidatesWork: round {}'s owner holds generation {} and \
                             anchor {} after the reactor adopted {} with its own",
                            slot.incarnation, held.0, held.1, latest.0
                        ));
                    }
                }
            }
        }
        for detail in found {
            if self.reported.insert(detail.clone()) {
                self.breaks.push(detail);
            }
        }
    }

    /// Judges round `incarnation`'s ordinary start under `authority`
    /// against the locator call whose completion it ran under.
    fn judge_start(&mut self, incarnation: u64, authority: Authority) {
        self.applies();
        let ReactorCause::Completion { call } = self.cause else {
            self.breaks.push(format!(
                "ResetInvalidatesWork: round {incarnation} started under {:?}, not a locator \
                 call's completion",
                self.cause
            ));
            return;
        };
        let Some(dispatched) = self.locators.get(&call).copied() else {
            self.breaks.push(format!(
                "ResetInvalidatesWork: round {incarnation} started under call {call}'s \
                 completion, which isn't a locator read"
            ));
            return;
        };
        if dispatched != authority.0 {
            self.breaks.push(format!(
                "ResetInvalidatesWork: round {incarnation} started under generation {}, from \
                 locator call {call} dispatched under {dispatched}",
                authority.0
            ));
        }
        let read = self
            .submitted
            .get(&call)
            .and_then(|op| self.reads.get(op))
            .copied();
        let Some(read) = read else {
            self.missing.get_or_insert(format!(
                "RoundLifeJoin: round {incarnation} started from locator call {call}, whose \
                 read storage never reported"
            ));
            return;
        };
        if let Some(((generation, _), anchor_height)) = self.adopted {
            if read.generation < generation || read.anchor_height < anchor_height {
                self.breaks.push(format!(
                    "ResetInvalidatesWork: round {incarnation} started from a locator read at \
                     generation {} and anchor height {}, older than the adopted {generation} \
                     and {anchor_height}",
                    read.generation, read.anchor_height
                ));
            }
        }
    }
}

/// `authority`'s generation and anchor hash.
fn authority_of(authority: zakura_header_chain::HeaderWorkAuthority) -> Authority {
    (
        authority.header_generation.get(),
        authority.branch.anchor_hash,
    )
}

/// `slot`'s peer as hex, by the digest storage attributes it with.
fn hex_peer(slot: &WorkSlot) -> String {
    super::tap::peer(&slot.peer)
}

#[cfg(test)]
mod tests;
