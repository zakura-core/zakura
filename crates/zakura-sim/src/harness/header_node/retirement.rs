//! `header-sync/peer-retirement-progress`'s departed-work rules, on the
//! reactor's facts as its tap receives them.
//!
//! A session *departs* when the reactor reports it ended
//! (`SessionEnded`) or replaced (`SessionAdmitted { replaced }`). The
//! reactor then retires its work, except a body repair whose prepare or
//! apply is in flight, which it keeps until that call is handled.
//!
//! - **Slots**, judged at every `CauseSet`, where the reactor ends or starts
//!   a unit of work (Rule 2). No slot the queue's `Slots` lists, awaiting
//!   or active, names a departed session, unless it's a *kept repair*: a
//!   repair slot whose prepare or apply was in flight when its session
//!   departed. That excuse lasts until the call's `Handled`, which comes
//!   after both its `Completion` and `Refresh` units. If the call was a
//!   prepare whose handler dispatched the repair's apply, the excuse moves to
//!   that apply. At the first `CauseSet` after the last of those, the slot
//!   must be gone.
//! - **Operations.** No `Started` and no apply `Dispatched` names a departed
//!   session, except the kept repair's one apply, dispatched by its
//!   prepare's handler. A start or dispatch inside a stale call's own
//!   handler is `header-sync/stale-completion-inert`'s, so neither rule
//!   judges it there.
//!
//! - **Hand-off**, only where the harness armed the evidence with its
//!   departure and timeout fixtures' two mirrors ([`RetirementEvidence::armed`],
//!   [`Roles`]): the *owner*, whose slot goes, and the *taker*, which must
//!   take its target up. The fixture is *ready* once the reactor admits the
//!   taker's session on the connection of the harness's one `Relink` of the
//!   taker, while the owner's session holds a slot. That session is the
//!   owner's for the rest of the run, and the admitted one the taker's.
//!   Readiness still reads the reactor's `SessionAdmitted`, joined to the
//!   harness's `Relink` fact by connection, since only the admission says
//!   the taker's new `Status` can reach the reactor. At the first
//!   `CauseSet` after the owner's slot goes, once ready, it checks from the
//!   facts that the fixture held (Rule 3): the harness hasn't ended the
//!   taker since, which only a later `Environment` fact naming it does, the
//!   taker holds no slot and dispatched no locator read for the target, the
//!   target isn't the node's adopted tip, and the last probe read found a
//!   locator permit free. Then it opens [`HandoffFact::Opened`]. A slot that
//!   goes before readiness opens nothing. A locator read for the target
//!   reaching storage from a live session other than the owner's
//!   ([`HandoffFact::Read`]) answers it, and so does the node adopting the
//!   target ([`HandoffFact::Reached`]) or the harness ending the taker, which
//!   the property reads from the harness's own fact. A deferred cached pass
//!   ([`HandoffFact::Deferred`]) moves its deadline to the pass's refill. The
//!   fixture rules out every other blocker by construction (Rule 8): the
//!   taker already holds the target, and storage never stalls its read.
//!
//! - **Timeout**, armed with the hand-off. The check keeps each ordinary
//!   round's request deadline itself, from the three events that arm it in
//!   production: the round's `Started`, each `PageStaged` on it, and its
//!   apply's `Dispatched`, which follows a successful prepare. Each arm of
//!   the owner's round, once ready, is a [`HandoffFact::Armed`]. One made
//!   while the fixture held obliges, and its obligation the request timeout
//!   after it expires (see the property). The round's next arm answers it,
//!   and so does a locator read for its target from another session, the
//!   node adopting its target, or the harness ending the taker: production
//!   retires a round past its deadline, and the cached pass then offers the
//!   target to the taker. The fixtures hold the owner's first locator read
//!   until the fixture is ready, so every arm comes after readiness.
//!
//! A probe read the fixture needed but the run never made leaves the check
//! unread ([`RetirementEvidence::unread`]), so the run can't pass without
//! it. Once the harness began stopping the node, nothing opens and no
//! departure makes the check applicable.
//!
//! The check applies once a session departs with a slot, as the slots stood
//! when the unit of work it departed in began, since the reactor retires a
//! session's work before it reports the end. A broken rule is a
//! claim break, which the tap hands [`Claim::PeerRetirementProgress`] at its
//! drain.
//!
//! [`Claim::PeerRetirementProgress`]: super::claim::Claim::PeerRetirementProgress

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

use zakura_chain::block;
use zakura_scheduler::SimInstant;

use zakura_header_chain::{BodyWorkOwner, HeaderSyncWorkOwner};
use zakura_network::zakura::{
    observe::{HeaderTargetPhase, PortCall, ReactorCause, ReactorEvent, RequestRef, WorkSlot},
    ZakuraPeerId,
};

use super::{
    ends::{EndEvidence, EndFact, EnvironmentAction},
    session::{SessionEvidence, SessionFact},
    stamp::Stamps,
};
use crate::observe::{StorageFact, StorageRequest};

/// A session, by its peer and session ID.
type Session = (ZakuraPeerId, u64);

/// Reads the node port's last fact sequence number: the port's own count,
/// so a fact the tap drains later is ordered against this read where both
/// happened.
pub(super) type PortSeq = Box<dyn Fn() -> u64 + Send + Sync>;

/// A slot's report key: its peer's digest, its session and its round.
type SlotKey = (Vec<u8>, u64, u64);

/// A prepare or apply the reactor dispatched and hasn't handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InFlight {
    /// The body repair it's for, if it's a repair's.
    repair: Option<BodyWorkOwner>,
    /// Whether it's an apply.
    apply: bool,
}

/// A kept repair's excuse: the call in flight that keeps its slot, and the
/// round it keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Excuse {
    /// The call.
    call: u64,
    /// Whether the call is the repair's apply.
    apply: bool,
    /// The repair.
    owner: BodyWorkOwner,
    /// The repair's round, as its `Started` named it.
    incarnation: u64,
}

/// The departure and timeout fixtures' two mirrors, by the names their
/// scenario gives them, as the module doc says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Roles {
    /// The mirror whose slot goes.
    pub owner: String,
    /// The mirror that must take the owner's target up.
    pub taker: String,
}

/// A hand-off fact the retirement check found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandoffFact {
    /// The fixture became ready: the reactor admitted the taker's session
    /// on the harness's relink while the owner's session held a slot.
    Ready {
        /// The owner's session.
        owner: (ZakuraPeerId, u64),
        /// The taker's session.
        taker: (ZakuraPeerId, u64),
    },
    /// The owner's slot for `target` went with the fixture holding, so the
    /// taker must take the target up.
    Opened {
        /// The target the slot pursued.
        target: block::Hash,
        /// The owner's session, whose slot went.
        departed: (ZakuraPeerId, u64),
        /// The slot's phase as the unit it went in began, or `None` if it
        /// was awaiting its locator.
        phase: Option<HeaderTargetPhase>,
        /// When the check found it.
        at: SimInstant,
    },
    /// A locator read for `target` from `session` reached storage.
    Read {
        /// The read's target.
        target: block::Hash,
        /// The session it's for.
        session: (ZakuraPeerId, u64),
    },
    /// The node adopted `target` as its header tip.
    Reached {
        /// The adopted tip.
        target: block::Hash,
    },
    /// The reactor's cached pass waits for its budget's refill at `until`.
    Deferred {
        /// The refill.
        until: SimInstant,
    },
    /// The owner's ordinary round `round`, for `target`, armed its request
    /// deadline for the `arm`th time, from 0, once the fixture was ready.
    Armed {
        /// The round, as its `Started` named it.
        round: u64,
        /// Its session, the owner's.
        session: (ZakuraPeerId, u64),
        /// Its target.
        target: block::Hash,
        /// How many times it armed before.
        arm: u32,
        /// Whether the fixture held at the arm, so it obliges.
        obliges: bool,
        /// When it armed.
        at: SimInstant,
    },
}

/// What [`RetirementEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// The first drain that found the check applicable.
    applicable: Option<u64>,
    /// Whether an applicable fact was found since the last drain.
    pending: bool,
    /// The fixture's mirrors, where the harness armed the hand-off rule.
    roles: Option<Roles>,
    /// Whether a trigger found no probe read to check a permit with.
    unread: bool,
    /// Where its stamps come from: the node's, once its tap watches it.
    stamps: Stamps,
    /// Each hand-off fact with its stamp, in stamp order.
    facts: Vec<(u64, HandoffFact)>,
    /// Whether a session departed, or a kept repair's excuse ended, since
    /// the reactor's last unit boundary.
    unjudged: bool,
    /// What the slot rule would find now that it hasn't reported, kept
    /// current as its inputs change, for a settled end.
    unjudged_breaks: Vec<String>,
    /// Whether the run ended idle, once it has ended.
    settled: Option<bool>,
}

/// When the departed-work check first applied, shared by the tap and
/// whoever reads it. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct RetirementEvidence(Arc<Mutex<Store>>);

impl RetirementEvidence {
    /// Evidence with the hand-off rule armed for the fixture `roles` names,
    /// one that leaves the taker one thing to wait for (Rule 8). The default
    /// keeps only the departed-work rule.
    pub fn armed(roles: Roles) -> RetirementEvidence {
        RetirementEvidence(Arc::new(Mutex::new(Store {
            roles: Some(roles),
            ..Store::default()
        })))
    }

    /// The fixture's mirrors, if the harness armed the hand-off rule.
    pub fn roles(&self) -> Option<Roles> {
        self.with(|store| store.roles.clone())
    }

    /// Whether a hand-off or deadline trigger came while the fixture was
    /// ready but no probe read had said whether a locator permit was free,
    /// so the fixture couldn't be checked (Rule 3).
    pub fn unread(&self) -> bool {
        self.with(|store| store.unread)
    }

    /// Stamps facts from `stamps`, the node's, from now on.
    pub(super) fn stamp_with(&self, stamps: Stamps) {
        self.with(|store| store.stamps = stamps);
    }

    /// Whether [`RetirementEvidence::armed`] made it.
    fn is_armed(&self) -> bool {
        self.with(|store| store.roles.is_some())
    }

    /// Keeps `fact` with the next stamp.
    fn push(&self, fact: HandoffFact) {
        self.with(|store| {
            let stamp = store.stamps.next();
            store.facts.push((stamp, fact));
        });
    }

    /// The hand-off fact with `stamp`, if this store holds it.
    pub fn fact(&self, stamp: u64) -> Option<HandoffFact> {
        self.with(|store| {
            store
                .facts
                .binary_search_by_key(&stamp, |(at, _)| *at)
                .ok()
                .map(|index| store.facts[index].1.clone())
        })
    }

    /// Every hand-off fact so far, with its stamp.
    pub fn facts(&self) -> Vec<(u64, HandoffFact)> {
        self.with(|store| store.facts.clone())
    }

    /// Runs `change` on the store. Nothing else is called while the lock
    /// is held.
    fn with<R>(&self, change: impl FnOnce(&mut Store) -> R) -> R {
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Whether a session departed, or a kept repair's excuse ended, since
    /// the reactor's last unit boundary, so the slot rule hasn't judged it
    /// yet (Rule 2).
    pub fn unjudged(&self) -> bool {
        self.with(|store| store.unjudged)
    }

    /// Records how the run ended: whether it settled, as
    /// [`super::adoption::settled`] decides.
    pub fn end(&self, end: &crate::RunEnd) {
        self.with(|store| store.settled = Some(super::adoption::settled(end)));
    }

    /// The slot rule's breaks at a settled end, judged on the state the run
    /// ended with, since a settled run is judged there (Rule 2). None if the
    /// run didn't settle, or everything the rule would find was already
    /// reported at a unit boundary.
    pub fn settled_breaks(&self) -> Vec<String> {
        self.with(|store| {
            if store.settled == Some(true) {
                store.unjudged_breaks.clone()
            } else {
                Vec::new()
            }
        })
    }

    /// Whether a drain up to and including `step`'s found the check
    /// applicable.
    pub fn applicable_by(&self, step: u64) -> bool {
        self.with(|store| store.applicable.is_some_and(|found| found <= step))
    }

    /// Whether any drain found the departed-work check applicable.
    pub fn applicable(&self) -> bool {
        self.with(|store| store.applicable.is_some())
    }

    /// Whether a hand-off or an obliging deadline arm opened before the
    /// fact with stamp `shutdown`, the harness's `Shutdown`, if there is one.
    pub fn opened_before(&self, shutdown: Option<u64>) -> bool {
        self.with(|store| {
            store.facts.iter().any(|(stamp, fact)| {
                shutdown.is_none_or(|shutdown| *stamp < shutdown)
                    && matches!(
                        fact,
                        HandoffFact::Opened { .. } | HandoffFact::Armed { obliges: true, .. }
                    )
            })
        })
    }
}

/// A ready fixture: its owner's and taker's sessions, and the stamp of the
/// harness's relink of the taker.
#[derive(Clone, Debug)]
struct Ready {
    /// The owner's session.
    owner: Session,
    /// The taker's session.
    taker: Session,
    /// The taker's relink fact's stamp. Any later `Environment` fact naming
    /// the taker ends it.
    relink: u64,
}

/// Whether the fixture held at a trigger (Rule 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fixture {
    /// It held.
    Holds,
    /// One of its conditions failed.
    Broken,
    /// No probe read said whether a locator permit was free.
    Unread,
}

/// The check's state: the queue's slots, the departed sessions, the calls in
/// flight and the kept repairs' excuses, and the breaks not yet drained.
pub(super) struct Retirement {
    /// The unit of work the reactor's last `CauseSet` named.
    cause: ReactorCause,
    /// The queue's slots, as its last `Slots` listed them.
    slots: Vec<WorkSlot>,
    /// The slots as the current unit of work began. The reactor retires a
    /// session's work before it reports the session's end.
    unit_slots: Vec<WorkSlot>,
    /// Each departed session.
    departed: HashSet<Session>,
    /// The node port's last fact sequence number when each session
    /// departed, which orders a later-drained submission against the
    /// departure at their origin (Rule 4).
    departed_at_seq: HashMap<Session, u64>,
    /// Reads the node port's last fact sequence number.
    port_seq: PortSeq,
    /// Each prepare or apply in flight, by call, with its session.
    in_flight: HashMap<u64, (Session, InFlight)>,
    /// Each live body repair's round, from its `Started` until its
    /// `Retired`.
    repairs: HashMap<BodyWorkOwner, u64>,
    /// Each kept repair's excuse: the call in flight that keeps its slot.
    excused: HashMap<Session, Excuse>,
    /// The slots already reported, so one kept across units breaks once.
    reported: BTreeSet<SlotKey>,
    breaks: Vec<String>,
    evidence: RetirementEvidence,
    /// The node's session view, which names each link's peer.
    sessions: SessionEvidence,
    /// The harness's environment actions.
    ends: EndEvidence,
    /// Each admitted session, by the connection its origin names.
    admitted: Vec<(u64, Session)>,
    /// The fixture, once it's ready.
    ready: Option<Ready>,
    /// Each locator call's session and target, by call.
    locators: HashMap<u64, (Session, block::Hash)>,
    /// The node's last adopted header tip.
    header_best: Option<block::Hash>,
    /// The last probe read's free locator permits.
    permits: Option<usize>,
    /// Each live ordinary round, by its owner: its round, session, target
    /// and how many times it armed its deadline.
    deadlines: HashMap<HeaderSyncWorkOwner, (u64, Session, block::Hash, u32)>,
}

impl Retirement {
    /// A check that keeps its facts in `evidence`, reads each link's peer
    /// from `sessions`, the harness's actions from `ends` and the node
    /// port's last fact sequence number from `port_seq`.
    pub(super) fn new(
        evidence: RetirementEvidence,
        sessions: SessionEvidence,
        ends: EndEvidence,
        port_seq: PortSeq,
    ) -> Retirement {
        Retirement {
            sessions,
            ends,
            admitted: Vec::new(),
            ready: None,
            locators: HashMap::new(),
            header_best: None,
            permits: None,
            deadlines: HashMap::new(),
            cause: ReactorCause::Idle,
            slots: Vec::new(),
            unit_slots: Vec::new(),
            departed: HashSet::new(),
            departed_at_seq: HashMap::new(),
            port_seq,
            in_flight: HashMap::new(),
            repairs: HashMap::new(),
            excused: HashMap::new(),
            reported: BTreeSet::new(),
            breaks: Vec::new(),
            evidence,
        }
    }

    /// Judges `event`, which the reactor's own task reported when
    /// `on_reactor`, then notes what it says. `stale_handler` says whether
    /// the reactor is inside a stale call's own handler.
    pub(super) fn observe(&mut self, event: &ReactorEvent, on_reactor: bool, stale_handler: bool) {
        if !on_reactor {
            return;
        }
        match event {
            ReactorEvent::CauseSet { cause } => {
                self.evidence.with(|store| {
                    store.unjudged = false;
                    store.unjudged_breaks.clear();
                });
                self.judge_slots();
                self.provisional();
                self.hand_off();
                self.cause = *cause;
                self.unit_slots.clone_from(&self.slots);
            }
            ReactorEvent::SnapshotAdopted { header_best, .. } => {
                self.header_best = Some(header_best.hash);
                if self.evidence.is_armed() {
                    self.evidence.push(HandoffFact::Reached {
                        target: header_best.hash,
                    });
                }
            }
            ReactorEvent::CachedPassDeferred { refill_in } => {
                let now = zakura_scheduler::hooks::now().unwrap_or_default();
                if let Some(until) = now.checked_add(*refill_in) {
                    if self.evidence.is_armed() {
                        self.evidence.push(HandoffFact::Deferred { until });
                    }
                }
            }
            ReactorEvent::Slots { slots } => {
                self.slots = slots.clone();
                self.provisional();
            }
            ReactorEvent::SessionAdmitted {
                peer,
                session_id,
                replaced,
                origin,
                ..
            } => {
                if let Some(replaced) = replaced {
                    self.depart(peer, *replaced);
                }
                if let Some(origin) = origin {
                    let session = (peer.clone(), *session_id);
                    self.admitted.push((origin.connection, session.clone()));
                    self.ready_on(origin.connection, session);
                }
            }
            ReactorEvent::SessionEnded {
                peer, session_id, ..
            } => self.depart(peer, *session_id),
            ReactorEvent::Started { request, .. } => {
                if let HeaderSyncWorkOwner::BodyRepair(repair) = request.owner {
                    self.repairs.insert(repair, request.incarnation);
                } else {
                    let session = (request.peer.clone(), request.session_id);
                    self.deadlines.insert(
                        request.owner,
                        (request.incarnation, session, request.target_tip_hash, 0),
                    );
                    self.arm(&request.owner, false);
                }
                let session = (request.peer.clone(), request.session_id);
                if !stale_handler && self.departed.contains(&session) {
                    self.breaks.push(format!(
                        "PeerRetirementProgress: request {} started for session {} of peer \
                         {}, after it departed",
                        request.request_id,
                        request.session_id,
                        crate::storage::hex(&request.peer.digest())
                    ));
                }
            }
            ReactorEvent::Dispatched { call, op } => match op {
                PortCall::Prepare { peer, owner, .. } => {
                    self.dispatched(*call, peer, owner, false);
                }
                PortCall::Apply { peer, owner, .. } => {
                    let session = (peer.clone(), owner.session_id());
                    if !stale_handler
                        && self.departed.contains(&session)
                        && !self.kept_apply(&session, owner)
                    {
                        self.breaks.push(format!(
                            "PeerRetirementProgress: call {call} applies session {} of peer {}, \
                             after it departed",
                            owner.session_id(),
                            crate::storage::hex(&peer.digest())
                        ));
                    }
                    self.dispatched(*call, peer, owner, true);
                    self.arm(owner, true);
                }
                PortCall::Locator {
                    peer,
                    session_id,
                    target_tip_hash,
                    ..
                } => {
                    self.locators
                        .insert(*call, ((peer.clone(), *session_id), *target_tip_hash));
                }
                PortCall::VctContext { .. } => {}
            },
            ReactorEvent::PageStaged { request, .. } => self.arm(&request.owner, true),
            ReactorEvent::Retired { request, .. }
                if matches!(request.owner, HeaderSyncWorkOwner::Header(_)) =>
            {
                self.deadlines.remove(&request.owner);
            }
            ReactorEvent::Handled { call, .. } => {
                self.in_flight.remove(call);
                self.end_excuses(|excuse| excuse.call == *call);
            }
            ReactorEvent::Retired {
                request:
                    RequestRef {
                        owner: HeaderSyncWorkOwner::BodyRepair(repair),
                        ..
                    },
                ..
            } => {
                self.repairs.remove(repair);
                self.end_excuses(|excuse| excuse.owner == *repair);
            }
            _ => {}
        }
    }

    /// Notes call `call`'s dispatch of `owner`'s prepare, or its apply when
    /// `apply`, to `peer`. A kept repair's apply, dispatched by its excused
    /// prepare's handler, takes the excuse over.
    fn dispatched(
        &mut self,
        call: u64,
        peer: &ZakuraPeerId,
        owner: &HeaderSyncWorkOwner,
        apply: bool,
    ) {
        let session = (peer.clone(), owner.session_id());
        let repair = match owner {
            HeaderSyncWorkOwner::BodyRepair(repair) => Some(*repair),
            HeaderSyncWorkOwner::Header(_) => None,
        };
        if apply && self.kept_apply(&session, owner) {
            if let Some(excuse) = self.excused.get_mut(&session) {
                excuse.call = call;
                excuse.apply = true;
            }
        }
        self.in_flight
            .insert(call, (session, InFlight { repair, apply }));
    }

    /// Whether an apply of `owner` for departed `session` now is its kept
    /// repair's one apply: dispatched by the handler of the excused
    /// prepare, for the same repair.
    fn kept_apply(&self, session: &Session, owner: &HeaderSyncWorkOwner) -> bool {
        let ReactorCause::Completion { call } = self.cause else {
            return false;
        };
        let HeaderSyncWorkOwner::BodyRepair(repair) = owner else {
            return false;
        };
        self.excused
            .get(session)
            .is_some_and(|excuse| excuse.call == call && !excuse.apply && excuse.owner == *repair)
    }

    /// Notes that `peer`'s session `session_id` departed, keeping the
    /// excuse of its repair whose prepare or apply is in flight, if any.
    fn depart(&mut self, peer: &ZakuraPeerId, session_id: u64) {
        let session = (peer.clone(), session_id);
        if !self.departed.insert(session.clone()) {
            return;
        }
        self.departed_at_seq
            .insert(session.clone(), (self.port_seq)());
        self.evidence.with(|store| store.unjudged = true);
        // A departure once the harness began stopping the node is its stop,
        // which makes nothing applicable.
        if !self.shut_down()
            && self
                .unit_slots
                .iter()
                .chain(&self.slots)
                .any(|slot| slot.peer == *peer && slot.session_id == session_id)
        {
            self.evidence.with(|store| store.pending = true);
        }
        // Only a live repair's call excuses, and only the round that call
        // is for, which must have a slot of this session as it departed.
        let excuse = self
            .in_flight
            .iter()
            .filter(|(_, (of, _))| *of == session)
            .filter_map(|(call, (_, flight))| {
                let owner = flight.repair?;
                let incarnation = *self.repairs.get(&owner)?;
                Some(Excuse {
                    call: *call,
                    apply: flight.apply,
                    owner,
                    incarnation,
                })
            })
            .filter(|excuse| {
                self.unit_slots.iter().chain(&self.slots).any(|slot| {
                    slot.peer == *peer
                        && slot.session_id == session_id
                        && slot.repair
                        && slot.incarnation == excuse.incarnation
                })
            })
            .max_by_key(|excuse| excuse.call);
        if let Some(excuse) = excuse {
            self.excused.insert(session, excuse);
        }
        // After the excuse, so a run that settles here isn't judged on the
        // slot it keeps.
        self.provisional();
    }

    /// Notes the storage fact with port sequence number `seq` the node's port
    /// logged: a locator read's submission, for the hand-off rule.
    pub(super) fn storage(&mut self, seq: u64, fact: &StorageFact) {
        if let StorageFact::Submitted {
            request: StorageRequest::ContinuationLocator,
            call: Some(call),
            ..
        } = fact
        {
            if let Some((session, target)) = self.locators.get(call) {
                // A departed session can't take the target up, whatever its
                // call still submits. The tap drains a poll's submissions
                // after its reactor facts, so the port's own sequence number
                // orders the two (Rule 4): a read counts only if the port
                // published it before the departure.
                let before_departure = self
                    .departed_at_seq
                    .get(session)
                    .is_none_or(|departed| seq <= *departed);
                if self.evidence.is_armed() && before_departure {
                    self.evidence.push(HandoffFact::Read {
                        target: *target,
                        session: session.clone(),
                    });
                }
            }
        }
    }

    /// Records that `owner`'s ordinary round armed its request deadline,
    /// counting it as a new arm when `again`, when the hand-off rule is
    /// armed. Only the owner's round records it, once the fixture is ready
    /// and until the harness begins stopping the node. Does nothing for a
    /// repair or a round it didn't see start.
    fn arm(&mut self, owner: &HeaderSyncWorkOwner, again: bool) {
        if !self.evidence.is_armed() {
            return;
        }
        let Some((round, session, target, arms)) = self.deadlines.get_mut(owner) else {
            return;
        };
        if again {
            *arms = arms.saturating_add(1);
        }
        let (round, session, target, arm) = (*round, session.clone(), *target, *arms);
        let Some(ready) = self.ready.clone() else {
            return;
        };
        if session != ready.owner || self.shut_down() {
            return;
        }
        let obliges = self.fixture(&ready, target) == Fixture::Holds;
        self.evidence.push(HandoffFact::Armed {
            round,
            session,
            target,
            arm,
            obliges,
            at: zakura_scheduler::hooks::now().unwrap_or_default(),
        });
    }

    /// Notes that the reactor admitted `session` on `connection`, which
    /// readies the fixture if it's the taker's admission on the harness's
    /// one relink of the taker, and the owner's live session holds a slot.
    fn ready_on(&mut self, connection: u64, session: Session) {
        let Some(roles) = self.evidence.roles() else {
            return;
        };
        if self.ready.is_some() {
            return;
        }
        let Some(relink) = self
            .ends
            .facts()
            .into_iter()
            .find_map(|(stamp, fact)| match fact {
                EndFact::Environment {
                    peer,
                    connection: Some(relinked),
                    action: EnvironmentAction::Relink,
                } if peer == roles.taker => Some((stamp, relinked)),
                _ => None,
            })
        else {
            return;
        };
        if relink.1 != connection {
            return;
        }
        let owner_links: HashSet<u64> = self
            .sessions
            .facts()
            .into_iter()
            .filter_map(|fact| match fact {
                SessionFact::LinkOpened {
                    peer, connection, ..
                } if peer == roles.owner => Some(connection),
                _ => None,
            })
            .collect();
        let Some(owner) = self
            .admitted
            .iter()
            .filter(|(on, of)| owner_links.contains(on) && !self.departed.contains(of))
            .map(|(_, of)| of.clone())
            .max_by_key(|(_, session_id)| *session_id)
        else {
            return;
        };
        if !self
            .slots
            .iter()
            .any(|slot| slot.peer == owner.0 && slot.session_id == owner.1)
        {
            return;
        }
        self.evidence.push(HandoffFact::Ready {
            owner: owner.clone(),
            taker: session.clone(),
        });
        self.ready = Some(Ready {
            owner,
            taker: session,
            relink: relink.0,
        });
    }

    /// Whether the fixture `ready` holds now for `target`, as the module doc
    /// says. A missing probe read marks the evidence unread.
    fn fixture(&self, ready: &Ready, target: block::Hash) -> Fixture {
        let taker_ended = self.ends.facts().into_iter().any(|(stamp, fact)| {
            stamp > ready.relink
                && matches!(
                    fact,
                    EndFact::Environment { ref peer, .. }
                        if self.evidence.roles().is_some_and(|roles| roles.taker == *peer)
                )
        });
        let (taker, read_for) = (&ready.taker, target);
        if taker_ended
            || self.header_best == Some(target)
            || self.slots.iter().any(|slot| slot.peer == taker.0)
            || self
                .locators
                .values()
                .any(|(of, read)| of == taker && *read == read_for)
        {
            return Fixture::Broken;
        }
        match self.permits {
            None => {
                self.evidence.with(|store| store.unread = true);
                Fixture::Unread
            }
            Some(0) => Fixture::Broken,
            Some(_) => Fixture::Holds,
        }
    }

    /// Whether the harness began stopping the node.
    fn shut_down(&self) -> bool {
        self.ends.facts().into_iter().any(|(_, fact)| {
            matches!(
                fact,
                EndFact::Environment {
                    action: EnvironmentAction::Shutdown,
                    ..
                }
            )
        })
    }

    /// Notes a probe read's free locator permits.
    pub(super) fn permits(&mut self, free: usize) {
        self.permits = Some(free);
    }

    /// Opens a hand-off if the owner's slot went since the last boundary,
    /// as the module doc says, once the fixture is ready and until the
    /// harness begins stopping the node.
    fn hand_off(&mut self) {
        let Some(ready) = self.ready.clone() else {
            return;
        };
        if self.shut_down() {
            return;
        }
        // A slot is its peer's and session's. Its target can narrow as it
        // moves into preparation, which isn't the slot going.
        let held = |slot: &WorkSlot| slot.peer == ready.owner.0 && slot.session_id == ready.owner.1;
        if self.slots.iter().any(held) {
            return;
        }
        let Some((target, phase)) = self
            .unit_slots
            .iter()
            .find(|slot| held(slot))
            .map(|slot| (slot.target_tip_hash, slot.phase))
        else {
            return;
        };
        if self.fixture(&ready, target) == Fixture::Holds {
            self.evidence.push(HandoffFact::Opened {
                target,
                departed: ready.owner,
                phase,
                at: zakura_scheduler::hooks::now().unwrap_or_default(),
            });
        }
    }

    /// Breaks for every slot that names a departed session and isn't a kept
    /// repair's.
    fn judge_slots(&mut self) {
        for (key, detail) in self.slot_breaks() {
            if self.reported.insert(key) {
                self.breaks.push(detail);
            }
        }
    }

    /// Ends every kept repair's excuse `ends` picks. The slot it kept must
    /// be gone at the next unit boundary, so until then the rule is
    /// unjudged, and a run that settles first is judged on what's left.
    fn end_excuses(&mut self, ends: impl Fn(&Excuse) -> bool) {
        let before = self.excused.len();
        self.excused.retain(|_, excuse| !ends(excuse));
        if self.excused.len() != before {
            self.evidence.with(|store| store.unjudged = true);
            self.provisional();
        }
    }

    /// Keeps what [`judge_slots`](Self::judge_slots) would find now and
    /// hasn't reported, as the slots, departures and excuses change, so a
    /// run that settles before the next unit boundary is still judged on the
    /// state it ended with (Rule 2).
    fn provisional(&mut self) {
        let pending: Vec<String> = self
            .slot_breaks()
            .into_iter()
            .filter(|(key, _)| !self.reported.contains(key))
            .map(|(_, detail)| detail)
            .collect();
        self.evidence.with(|store| store.unjudged_breaks = pending);
    }

    /// Each slot that names a departed session and isn't a kept repair's,
    /// with its report key and the break's text.
    fn slot_breaks(&self) -> Vec<(SlotKey, String)> {
        let mut found = Vec::new();
        for slot in &self.slots {
            let session = (slot.peer.clone(), slot.session_id);
            let kept = self
                .excused
                .get(&session)
                .is_some_and(|excuse| slot.repair && excuse.incarnation == slot.incarnation);
            if !self.departed.contains(&session) || kept {
                continue;
            }
            let key = (
                slot.peer.digest().to_vec(),
                slot.session_id,
                slot.incarnation,
            );
            found.push((
                key,
                format!(
                    "PeerRetirementProgress: a slot for session {} of peer {}, {}, outlived the \
                     unit of work it departed in",
                    slot.session_id,
                    crate::storage::hex(&slot.peer.digest()),
                    match slot.phase {
                        Some(phase) => format!("round {} in {phase:?}", slot.incarnation),
                        None => "awaiting its locator".to_owned(),
                    }
                ),
            ));
        }
        found
    }

    /// Records when the check first applied, then hands over the breaks
    /// found since the last drain, which the tap records at `step`.
    pub(super) fn drain(&mut self, step: u64) -> Vec<String> {
        self.evidence.with(|store| {
            if std::mem::take(&mut store.pending) {
                store.applicable.get_or_insert(step);
            }
        });
        std::mem::take(&mut self.breaks)
    }
}

#[cfg(test)]
mod tests;
