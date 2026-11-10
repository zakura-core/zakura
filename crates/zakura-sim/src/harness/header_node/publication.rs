//! `header-sync/state-publication-consistent`'s rules, judged at the
//! owner boundaries Rule 10 names: the engine's own records, the port's
//! publications on its selected-chain watch, the reactor's adoption, and
//! what the reactor's publishers queue or send.
//!
//! - **Published as committed.** Every engine run that commits a new state
//!   is published on the watch in the observation that recorded it: the
//!   observation's last publication is its last committed state's snapshot,
//!   and every publication is a snapshot some engine record holds. The
//!   engine's install and the watch's send are separate calls, so a
//!   skipped publication is a defect the engine records alone can't show.
//! - **Adoption is faithful.** Every `SnapshotAdopted` names a state version
//!   some engine record holds, and equals that record's snapshot in
//!   generation, anchor, tip, work, retention and the verified frontier and
//!   generation. The reactor's stored copy, which its `SnapshotStored`
//!   reports whole right after the store, is the engine's record of the
//!   adoption it follows, in every field, and an
//!   adoption is stored before its unit of work ends (the reactor's next
//!   `Sizes`) or the next adoption. The adoption, its `Status` and its tip
//!   come from the reactor's input, and the stored copy has readers of its
//!   own, so a defect in the store alone shows only here.
//! - **Status names the adoption.** Every queued `Status` advertises the
//!   latest adoption's anchor, tip, work and retention. `Status` has no
//!   generation field, so a mixed-generation `Status` shows as fields that
//!   match no single adoption (`StaleStatus`, hdr-4's).
//! - **Tip names the adoption.** Every adoption that changes the selected
//!   tip publishes it on the reactor's tip watch before the next adoption
//!   (`UnpublishedTip`), every tip publication reaches a receiver
//!   (`TipLost`) and names the latest adopted tip (`StaleTip`), all
//!   hdr-4's. A tip still owed when a settled run ends breaks too, which
//!   the harness reports from its end violations, since no task is left
//!   partway through publishing it.
//!
//! The records are the engine before the run and each engine run's
//! snapshot, an apply's, a reset's or a harness commit's
//! (`ControlledPort::harness_commit`), so every engine change has one. The
//! check applies once it judged an adoption or a commit's publication. A
//! broken rule is a claim break, which the tap hands
//! [`Claim::StatePublicationConsistent`](super::claim::Claim) at its drain.
//!
//! **Liveness facts.** A harness that arms the evidence
//! ([`PublicationEvidence::armed`]) also gets stamped facts, which the
//! property's obligations read: each commit ([`PublicationFact::Committed`])
//! and adoption, and, in a declared status fixture, each session's owed
//! `Status` and the queued one that pays it. A session is owed one at its
//! admission, and at the first adoption that changes what its `Status`
//! advertises while nothing it was queued advertises that adoption. Later
//! changes keep the debt open rather than opening another. A queued
//! `Status` that advertises the newest adoption pays it, and a stale one is
//! `StaleStatus` already.
//!
//! **Taint.** A run that changed the engine other than its rules allow,
//! such as a failed apply that installed its state anyway, is
//! `commit-only-authorized`'s commit-contents check's to report. What it
//! committed, published and was adopted is then that defect's consequence,
//! so this check judges nothing from that run on and the property is
//! `NotApplicable` from that drain on, as `expected-chain-selected` does
//! ([`super::fork_choice`]).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use zakura_header_chain::{EngineSnapshot, Frontier};
use zakura_network::zakura::{observe::ReactorEvent, Status, ZakuraPeerId};
use zakura_scheduler::SimInstant;
use zakura_storage::header::controlled::EngineRun;

use super::{
    adoption::{Adoption, STATUS_CHANGE_BOUND},
    stamp::Stamps,
};

/// How long a session in a status fixture may be owed its `Status`: the
/// publisher's change bound ([`STATUS_CHANGE_BOUND`], its
/// `MAX_CHANGE_DELAY`) plus its one-second publication floor
/// (`MIN_PUBLICATION_INTERVAL`), behind which it coalesces changes.
pub const STATUS_DEADLINE: Duration = STATUS_CHANGE_BOUND.saturating_add(Duration::from_secs(1));

/// A stamped fact an armed [`PublicationEvidence`] keeps for the
/// property's obligations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicationFact {
    /// An engine run committed state `state_version` at `at`.
    Committed {
        /// The committed state version.
        state_version: u64,
        /// When its record was read.
        at: SimInstant,
    },
    /// The reactor adopted state `state_version`.
    Adopted {
        /// The adopted state version.
        state_version: u64,
    },
    /// `peer`'s session `session_id` is owed a `Status` advertising the
    /// newest adoption, from `at`. Only a status fixture's evidence keeps
    /// it.
    StatusOwed {
        /// The session's peer.
        peer: ZakuraPeerId,
        /// The session.
        session_id: u64,
        /// When the debt opened.
        at: SimInstant,
    },
    /// A `Status` advertising the newest adoption was queued for session
    /// `session_id`, which paid what it was owed.
    StatusPaid {
        /// The session.
        session_id: u64,
    },
    /// The commit-contents check found a run corrupt, after which this
    /// check judges nothing and every debt is that check's
    /// ([`Publication::run`]).
    Tainted,
}

/// What an armed [`PublicationEvidence`] stamps facts for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Arming {
    /// Whether the evidence keeps facts at all.
    pub armed: bool,
    /// Whether the harness declared a status fixture, whose sessions are
    /// owed their `Status` in time.
    pub status: bool,
}

/// What [`PublicationEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// The first drain that found the check applicable.
    applicable: Option<u64>,
    /// Whether a fact since the last drain made it applicable.
    pending: bool,
    /// The first drain that found the check tainted.
    tainted: Option<u64>,
    /// Whether a run since the last drain tainted it.
    taint_pending: bool,
    /// An adopted tip not yet published on the reactor's tip watch.
    owed: Option<Frontier>,
    /// Whether the run settled, once it has ended.
    settled: Option<bool>,
    /// What the store keeps stamped facts for.
    arming: Arming,
    /// Where its stamps come from: the node's, once its tap hands them
    /// over.
    stamps: Stamps,
    /// Each stamped fact, in stamp order.
    facts: Vec<(u64, PublicationFact)>,
    /// Set when the run's log froze. Facts after it are dropped.
    frozen: bool,
    /// Each node's peer ID and harness name, which the harness's
    /// environment facts name it by.
    names: Vec<(ZakuraPeerId, String)>,
}

/// When the publication check first applied, and the tip it still owes,
/// shared by the tap and whoever reads it. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct PublicationEvidence(Arc<Mutex<Store>>);

impl PublicationEvidence {
    /// Evidence that also keeps the stamped facts `arming` asks for. The
    /// default keeps none.
    pub fn armed(arming: Arming) -> PublicationEvidence {
        PublicationEvidence(Arc::new(Mutex::new(Store {
            arming,
            ..Store::default()
        })))
    }

    /// What the evidence keeps stamped facts for.
    pub fn arming(&self) -> Arming {
        self.with(|store| store.arming)
    }

    /// Stamps facts from `stamps`, the node's, from now on.
    pub(super) fn stamp_with(&self, stamps: Stamps) {
        self.with(|store| store.stamps = stamps);
    }

    /// Stops keeping facts. The harness calls it when the run's log
    /// freezes, as [`super::adoption::AdoptionEvidence`] does.
    pub fn freeze(&self) {
        self.with(|store| store.frozen = true);
    }

    /// Keeps `fact` with the next stamp, if the evidence is armed, `status`
    /// facts only in a status fixture, and the log hasn't frozen.
    fn keep(&self, fact: PublicationFact) {
        self.with(|store| {
            let status = matches!(
                fact,
                PublicationFact::StatusOwed { .. } | PublicationFact::StatusPaid { .. }
            );
            if store.arming.armed && (store.arming.status || !status) && !store.frozen {
                let stamp = store.stamps.next();
                store.facts.push((stamp, fact));
            }
        });
    }

    /// Every stamped fact, in stamp order.
    pub fn facts(&self) -> Vec<(u64, PublicationFact)> {
        self.with(|store| store.facts.clone())
    }

    /// The fact with stamp `stamp`, if this store holds it.
    pub fn fact(&self, stamp: u64) -> Option<PublicationFact> {
        self.with(|store| {
            store
                .facts
                .binary_search_by_key(&stamp, |(at, _)| *at)
                .ok()
                .map(|index| store.facts[index].1.clone())
        })
    }

    /// The newest state version adopted by a fact stamped before `stamp`.
    pub fn adopted_before(&self, stamp: u64) -> Option<u64> {
        self.with(|store| {
            store
                .facts
                .iter()
                .take_while(|(at, _)| *at < stamp)
                .filter_map(|(_, fact)| match fact {
                    PublicationFact::Adopted { state_version } => Some(*state_version),
                    _ => None,
                })
                .max()
        })
    }

    /// Notes each node's peer ID and harness name, so a debt's peer can be
    /// matched with the harness's environment facts, which name peers.
    pub fn name_peers(&self, names: Vec<(ZakuraPeerId, String)>) {
        self.with(|store| store.names = names);
    }

    /// The harness name of the node with peer ID `peer`, if the harness
    /// named it ([`Self::name_peers`]).
    pub fn peer_name(&self, peer: &ZakuraPeerId) -> Option<String> {
        self.with(|store| {
            store
                .names
                .iter()
                .find(|(id, _)| id == peer)
                .map(|(_, name)| name.clone())
        })
    }

    /// The peer whose session `session_id` was owed a `Status`, if it was.
    pub fn owed_peer(&self, session_id: u64) -> Option<ZakuraPeerId> {
        self.with(|store| {
            store.facts.iter().find_map(|(_, fact)| match fact {
                PublicationFact::StatusOwed {
                    peer,
                    session_id: owed,
                    ..
                } if *owed == session_id => Some(peer.clone()),
                _ => None,
            })
        })
    }

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

    /// Whether a drain up to and including `step`'s found the check
    /// tainted, after which it judges nothing.
    pub fn tainted_by(&self, step: u64) -> bool {
        self.with(|store| store.tainted.is_some_and(|found| found <= step))
    }

    /// The adopted tip not yet published on the reactor's tip watch, if
    /// any.
    pub fn owed(&self) -> Option<Frontier> {
        self.with(|store| store.owed)
    }

    /// Notes how the run ended: whether it settled, with no task left
    /// partway through a synchronous call ([`super::adoption::settled`]).
    pub fn end(&self, settled: bool) {
        self.with(|store| store.settled = Some(settled));
    }

    /// The adopted tip a settled run ended still owing on the tip watch, if
    /// any: hdr-4's `UnpublishedTip` at the run's end.
    pub fn unpublished_at_settled_end(&self) -> Option<Frontier> {
        self.with(|store| {
            store
                .owed
                .filter(|_| store.settled == Some(true) && store.tainted.is_none())
        })
    }
}

/// The check's state.
pub(super) struct Publication {
    /// Each engine record's snapshot, by its state version.
    records: HashMap<u64, EngineSnapshot>,
    /// The state version of the newest record.
    newest: u64,
    /// The snapshots of the runs that committed a new state since the last
    /// publications were read, in order.
    committed: Vec<EngineSnapshot>,
    /// The latest adoption.
    adopted: Option<Adoption>,
    /// The state version of an adoption not yet stored.
    unstored: Option<u64>,
    /// Each admitted session the node still holds, in a status fixture.
    sessions: HashMap<u64, Session>,
    /// Whether the commit-contents check found a run corrupt, after which
    /// nothing is judged.
    tainted: bool,
    breaks: Vec<String>,
    evidence: PublicationEvidence,
}

impl Publication {
    /// A check whose first engine record is `seed`, the engine before the
    /// run, keeping when it applied in `evidence`.
    pub(super) fn new(evidence: PublicationEvidence, seed: EngineSnapshot) -> Publication {
        let newest = seed.state_version.get();
        Publication {
            records: HashMap::from([(newest, seed)]),
            newest,
            committed: Vec::new(),
            adopted: None,
            unstored: None,
            sessions: HashMap::new(),
            tainted: false,
            breaks: Vec::new(),
            evidence,
        }
    }

    /// Notes one engine `run`'s record. A run that left the state version
    /// where it found it committed nothing. `corrupt` says the
    /// commit-contents check found the run left the engine other than its
    /// rules allow, which taints the check from this run on.
    pub(super) fn run(&mut self, run: &EngineRun, corrupt: bool) {
        if corrupt && !self.tainted {
            self.tainted = true;
            self.committed.clear();
            self.evidence.with(|store| store.taint_pending = true);
            self.evidence.keep(PublicationFact::Tainted);
        }
        if self.tainted {
            return;
        }
        let version = run.snapshot.state_version.get();
        self.records.insert(version, run.snapshot.clone());
        if run.after.metadata.state_version != run.before.metadata.state_version {
            self.newest = version;
            self.committed.push(run.snapshot.clone());
            self.evidence.keep(PublicationFact::Committed {
                state_version: version,
                at: now(),
            });
        }
    }

    /// Judges the snapshots the port sent on its selected-chain watch in
    /// the observation whose engine runs the check just took.
    pub(super) fn publications(&mut self, published: &[EngineSnapshot]) {
        if self.tainted {
            return;
        }
        for snapshot in published {
            let version = snapshot.state_version.get();
            if self.records.get(&version) != Some(snapshot) {
                self.breaks.push(format!(
                    "StatePublicationConsistent: the port published state version {version}, \
                     which differs from every engine record: {snapshot:?}"
                ));
            }
        }
        let Some(committed) = self.committed.last() else {
            return;
        };
        self.applies();
        if published.last() != Some(committed) {
            self.breaks.push(format!(
                "StatePublicationConsistent: the engine committed state version {} and the \
                 port's last publication in that step was {:?}, not it",
                committed.state_version.get(),
                published
                    .last()
                    .map(|snapshot| snapshot.state_version.get())
            ));
        }
        self.committed.clear();
    }

    /// Judges `event`, which the reactor's own task reported when
    /// `on_reactor`.
    pub(super) fn observe(&mut self, event: &ReactorEvent, on_reactor: bool) {
        if !on_reactor || self.tainted {
            return;
        }
        match event {
            ReactorEvent::SnapshotAdopted { .. } => {
                let Some(super::adoption::AdoptionFact::Adopted(adoption)) =
                    super::adoption::AdoptionFact::of(event)
                else {
                    return;
                };
                self.adopted_one(adoption);
            }
            ReactorEvent::SnapshotStored { .. } => self.stored(event),
            ReactorEvent::Sizes { .. } => self.unit_ended("its unit of work ended"),
            ReactorEvent::SessionAdmitted {
                peer, session_id, ..
            } => self.admitted(peer, *session_id),
            ReactorEvent::SessionEnded { session_id, .. } => {
                // The node ending a session pays nothing it owed, and it's
                // owed nothing new.
                self.sessions.remove(session_id);
            }
            ReactorEvent::StatusQueued {
                session_id, status, ..
            } => {
                self.queued(*session_id, status);
                if let Some(adopted) = self
                    .adopted
                    .as_ref()
                    .filter(|adopted| !adopted.advertised_by(status))
                {
                    self.breaks.push(format!(
                        "StaleStatus: session {session_id} was queued a status for tip {:?}, \
                         anchor {:?}, while the reactor's adopted tip is {:?}, anchor {:?}",
                        Frontier::new(status.selected_tip_height, status.selected_tip_hash),
                        Frontier::new(status.work_anchor_height, status.work_anchor_hash),
                        adopted.header_best,
                        adopted.finalized,
                    ));
                }
            }
            ReactorEvent::TipPublished {
                height,
                hash,
                delivered,
            } => {
                let tip = Frontier::new(*height, *hash);
                if !delivered {
                    self.breaks.push(format!(
                        "TipLost: {tip:?} was published with no live receiver"
                    ));
                }
                if let Some(adopted) = self
                    .adopted
                    .as_ref()
                    .filter(|adopted| adopted.header_best != tip)
                {
                    self.breaks.push(format!(
                        "StaleTip: {tip:?} was published while the adopted tip is {:?}",
                        adopted.header_best
                    ));
                }
                self.evidence.with(|store| store.owed = None);
            }
            _ => {}
        }
    }

    /// Judges `adoption`, the reactor's newest.
    fn adopted_one(&mut self, adoption: Adoption) {
        self.applies();
        self.unit_ended("it adopted another");
        let version = adoption.state_version.get();
        self.unstored = Some(version);
        match self.records.get(&version) {
            None => self.breaks.push(format!(
                "StatePublicationConsistent: the reactor adopted state version {version}, \
                 which no engine record holds (the newest is {})",
                self.newest
            )),
            Some(record) => {
                let differs = [
                    (
                        "generation",
                        record.header_generation != adoption.header_generation,
                    ),
                    ("anchor", record.frontiers.finalized != adoption.finalized),
                    ("tip", record.frontiers.header_best != adoption.header_best),
                    (
                        "work",
                        record.header_best_score.suffix_work.as_u256() != adoption.suffix_work,
                    ),
                    (
                        "retention",
                        record.oldest_retained_height != adoption.oldest_retained,
                    ),
                    (
                        "verified generation",
                        record.verified_generation != adoption.verified_generation,
                    ),
                    (
                        "verified frontier",
                        record.frontiers.verified_best != adoption.verified_best,
                    ),
                ];
                let wrong: Vec<&str> = differs
                    .iter()
                    .filter(|(_, differs)| *differs)
                    .map(|(field, _)| *field)
                    .collect();
                if !wrong.is_empty() {
                    self.breaks.push(format!(
                        "StatePublicationConsistent: the reactor adopted state version \
                         {version} with its {wrong:?} differing from the engine's record: \
                         adopted {adoption:?}, recorded {record:?}"
                    ));
                }
            }
        }
        let owed = self.evidence.with(|store| store.owed);
        if let Some(tip) = owed {
            self.breaks.push(format!(
                "UnpublishedTip: the reactor adopted state version {version} with {tip:?}, \
                 its previous tip, still unpublished"
            ));
        }
        let changed = self
            .adopted
            .as_ref()
            .is_none_or(|adopted| adopted.header_best != adoption.header_best);
        if changed {
            self.evidence
                .with(|store| store.owed = Some(adoption.header_best));
        }
        self.evidence.keep(PublicationFact::Adopted {
            state_version: version,
        });
        for (session_id, session) in &mut self.sessions {
            let advertised = session
                .queued
                .as_ref()
                .is_some_and(|status| adoption.advertised_by(status));
            if !session.owed && !advertised {
                session.owed = true;
                self.evidence.keep(PublicationFact::StatusOwed {
                    peer: session.peer.clone(),
                    session_id: *session_id,
                    at: now(),
                });
            }
        }
        self.adopted = Some(adoption);
    }

    /// Judges the reactor's stored copy `event` reports
    /// (`SnapshotStored`): it must be the engine's record of the adoption
    /// it follows, whole, so a store that differs in any field, the mode
    /// included, or that left nothing breaks.
    fn stored(&mut self, event: &ReactorEvent) {
        let ReactorEvent::SnapshotStored { snapshot } = event else {
            return;
        };
        let adopted = self.unstored.take();
        let Some(stored) = snapshot else {
            self.breaks.push(format!(
                "StatePublicationConsistent: the reactor stored no committed snapshot after \
                 adopting state version {adopted:?}"
            ));
            return;
        };
        let version = stored.state_version.get();
        if adopted != Some(version) || self.records.get(&version) != Some(stored) {
            self.breaks.push(format!(
                "StatePublicationConsistent: the reactor stored {stored:?} as its committed \
                 snapshot after adopting state version {adopted:?}, whose engine record is \
                 {:?}",
                adopted.and_then(|adopted| self.records.get(&adopted))
            ));
        }
    }

    /// Breaks if an adoption is still unstored when `why` ends its chance.
    fn unit_ended(&mut self, why: &str) {
        if let Some(version) = self.unstored.take() {
            self.breaks.push(format!(
                "StatePublicationConsistent: the reactor adopted state version {version} and \
                 hadn't stored it when {why}"
            ));
        }
    }

    /// Notes `peer`'s admitted session `session_id`, which is owed its
    /// first `Status`.
    fn admitted(&mut self, peer: &ZakuraPeerId, session_id: u64) {
        if !self.evidence.arming().status {
            return;
        }
        self.sessions.insert(
            session_id,
            Session {
                peer: peer.clone(),
                owed: true,
                queued: None,
            },
        );
        self.evidence.keep(PublicationFact::StatusOwed {
            peer: peer.clone(),
            session_id,
            at: now(),
        });
    }

    /// Notes the `status` queued for `session_id`, which pays its debt if it
    /// advertises the newest adoption.
    fn queued(&mut self, session_id: u64, status: &Status) {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return;
        };
        session.queued = Some(status.clone());
        let newest = self
            .adopted
            .as_ref()
            .is_none_or(|adopted| adopted.advertised_by(status));
        if session.owed && newest {
            session.owed = false;
            self.evidence
                .keep(PublicationFact::StatusPaid { session_id });
        }
    }

    /// The breaks found since the last call. Marks the check applicable at
    /// `step` if a fact since the last call made it so.
    pub(super) fn drain(&mut self, step: u64) -> Vec<String> {
        // No property checks step 0, the setup observation's, whose drain
        // holds the reactor's startup adoption, so its breaks and
        // applicability wait for step 1's.
        if step == 0 {
            return Vec::new();
        }
        self.evidence.with(|store| {
            if std::mem::take(&mut store.pending) {
                store.applicable.get_or_insert(step);
            }
            if std::mem::take(&mut store.taint_pending) {
                store.tainted.get_or_insert(step);
            }
        });
        std::mem::take(&mut self.breaks)
    }

    /// Notes that a fact made the check applicable.
    fn applies(&self) {
        self.evidence.with(|store| store.pending = true);
    }
}

/// One admitted session's `Status` debt.
struct Session {
    /// Its peer.
    peer: ZakuraPeerId,
    /// Whether it's owed a `Status` advertising the newest adoption.
    owed: bool,
    /// The last `Status` queued for it.
    queued: Option<Status>,
}

/// The run's current time, or its start outside a step.
fn now() -> SimInstant {
    zakura_scheduler::hooks::now().unwrap_or_default()
}

#[cfg(test)]
mod tests;
