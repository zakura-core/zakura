//! The committed state a node's reactor adopted, and what it published from
//! it, as view-side evidence.
//!
//! The tap keeps each [`AdoptionFact`] here instead of in the observation
//! log, as it keeps retained-header ownership in its ledger, so pinned Plans
//! and traces keep their format. Each fact carries the virtual time the
//! reactor reported it at. The store also keeps the time of the tap's last
//! drain, which bounds how far a run got, and, once the run ends, whether it
//! settled.
//!
//! [`AdoptionEvidence::generation`] and [`AdoptionEvidence::status`]
//! check hdr-4's adopted-state rules over the facts. Its tip rule is
//! `header-sync/state-publication-consistent`'s
//! ([`super::publication`]). A rule whose deadline the run didn't reach is
//! [`Acceptance::Inconclusive`].

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{Frontier, HeaderGeneration, StateVersion, VerifiedGeneration};
use zakura_network::zakura::{
    observe::{EventQueue, ReactorCause, ReactorEvent, TakePoint},
    Status, ZakuraPeerId,
};

use super::stamp::Stamps;
use crate::{IdleStatus, Outcome, RunEnd};

/// How long after an adoption that changes a session's status the reactor
/// may wait before it queues that status or records a deferral:
/// `MAX_CHANGE_DELAY` in header sync's status publisher.
pub const STATUS_CHANGE_BOUND: Duration = Duration::from_secs(2);

/// How long after a deferral the reactor may wait before it tries again:
/// the publisher's 100 ms retry, raised to at most one second after its last
/// send by its publication floor.
pub const STATUS_RETRY_BOUND: Duration = Duration::from_secs(1);

/// A committed snapshot the reactor adopted, in the fields it schedules and
/// publishes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Adoption {
    /// The durable state version.
    pub state_version: StateVersion,
    /// The generation new work captures.
    pub header_generation: HeaderGeneration,
    /// The finality anchor.
    pub finalized: Frontier,
    /// The selected header tip.
    pub header_best: Frontier,
    /// Work after the anchor through `header_best`.
    pub suffix_work: U256,
    /// The lowest retained height.
    pub oldest_retained: block::Height,
    /// The generation new repair work captures.
    pub verified_generation: VerifiedGeneration,
    /// The verified body tip.
    pub verified_best: Frontier,
}

impl Adoption {
    /// Whether `other` advertises the same anchor, tip, work and retention,
    /// so a `Status` built from either is the same.
    pub fn advertises_as(&self, other: &Adoption) -> bool {
        self.finalized == other.finalized
            && self.header_best == other.header_best
            && self.suffix_work == other.suffix_work
            && self.oldest_retained == other.oldest_retained
    }

    /// Whether `status` advertises this adoption's anchor, tip, work and
    /// retention. Its serving limits don't come from the snapshot.
    pub fn advertised_by(&self, status: &Status) -> bool {
        status.work_anchor_height == self.finalized.height
            && status.work_anchor_hash == self.finalized.hash
            && status.selected_tip_height == self.header_best.height
            && status.selected_tip_hash == self.header_best.hash
            && status.suffix_cumulative_work == self.suffix_work
            && status.oldest_retained_height == self.oldest_retained
    }
}

/// One fact about the reactor's adopted state or what it published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdoptionFact {
    /// `SnapshotAdopted`.
    Adopted(Adoption),
    /// `StatusQueued`.
    StatusQueued {
        /// The peer.
        peer: ZakuraPeerId,
        /// The session.
        session_id: u64,
        /// The queued advertisement.
        status: Status,
    },
    /// `StatusDeferred`.
    StatusDeferred {
        /// The peer.
        peer: ZakuraPeerId,
        /// The session.
        session_id: u64,
    },
    /// `TipPublished`.
    TipPublished {
        /// The published tip.
        tip: Frontier,
        /// Whether a receiver saw it.
        delivered: bool,
    },
    /// A `Started` event's generation, which the generation rule reads.
    Started {
        /// The request's correlation ID.
        request_id: u64,
        /// The generation the round's work owner captured.
        generation: HeaderGeneration,
    },
    /// A locator call's `Dispatched` event's generation.
    LocatorDispatched {
        /// The call's number.
        call: u64,
        /// The generation the read was made under.
        generation: HeaderGeneration,
    },
    /// A `Commit` step put this state in the node's engine, outside header
    /// sync. The harness records it, not the reactor, before any task runs
    /// again, so it comes before every reactor fact that could read it.
    Published {
        /// The engine's state version after the commit.
        state_version: StateVersion,
        /// Its header generation.
        header_generation: HeaderGeneration,
    },
    /// `EventTaken`: the reactor took an event from `queue` at `point`,
    /// leaving `queued` more there.
    EventTaken {
        /// The queue.
        queue: EventQueue,
        /// Where in the turn.
        point: TakePoint,
        /// The events left in the queue.
        queued: usize,
    },
    /// `WatchObserved`: the run loop read committed snapshot
    /// `state_version` from its committed watch.
    WatchObserved {
        /// The read snapshot's state version.
        state_version: StateVersion,
    },
    /// The reactor reached its turn's cached pass: a `CauseSet` to `Pass`,
    /// once per turn of its run loop.
    Pass,
}

impl AdoptionFact {
    /// The fact `event` gives this store, if any.
    pub(super) fn of(event: &ReactorEvent) -> Option<Self> {
        use zakura_network::zakura::observe::PortCall;
        Some(match event {
            ReactorEvent::SnapshotAdopted {
                state_version,
                header_generation,
                finalized,
                header_best,
                suffix_work,
                oldest_retained,
                verified_generation,
                verified_best,
            } => AdoptionFact::Adopted(Adoption {
                state_version: *state_version,
                header_generation: *header_generation,
                finalized: *finalized,
                header_best: *header_best,
                suffix_work: *suffix_work,
                oldest_retained: *oldest_retained,
                verified_generation: *verified_generation,
                verified_best: *verified_best,
            }),
            ReactorEvent::StatusQueued {
                peer,
                session_id,
                status,
            } => AdoptionFact::StatusQueued {
                peer: peer.clone(),
                session_id: *session_id,
                status: status.clone(),
            },
            ReactorEvent::StatusDeferred { peer, session_id } => AdoptionFact::StatusDeferred {
                peer: peer.clone(),
                session_id: *session_id,
            },
            ReactorEvent::TipPublished {
                height,
                hash,
                delivered,
            } => AdoptionFact::TipPublished {
                tip: Frontier::new(*height, *hash),
                delivered: *delivered,
            },
            ReactorEvent::Started { request, .. } => AdoptionFact::Started {
                request_id: request.request_id,
                generation: request.owner.header_authority().header_generation,
            },
            ReactorEvent::Dispatched {
                call,
                op: PortCall::Locator { scope, .. },
            } => AdoptionFact::LocatorDispatched {
                call: *call,
                generation: scope.header_generation,
            },
            _ => return None,
        })
    }

    /// The turn fact `event` gives this store, if any: an `EventTaken`, a
    /// `WatchObserved`, or a `CauseSet` to `Pass`. All stay out of the log.
    pub(super) fn of_turn(event: &ReactorEvent) -> Option<Self> {
        match event {
            ReactorEvent::WatchObserved { state_version } => Some(AdoptionFact::WatchObserved {
                state_version: *state_version,
            }),
            ReactorEvent::EventTaken {
                queue,
                point,
                queued,
            } => Some(AdoptionFact::EventTaken {
                queue: *queue,
                point: *point,
                queued: *queued,
            }),
            ReactorEvent::CauseSet {
                cause: ReactorCause::Pass,
            } => Some(AdoptionFact::Pass),
            _ => None,
        }
    }

    /// Whether only this store keeps the fact. `Started` and `Dispatched`
    /// are logged too.
    pub(super) fn is_unlogged(&self) -> bool {
        matches!(
            self,
            AdoptionFact::Adopted(_)
                | AdoptionFact::StatusQueued { .. }
                | AdoptionFact::StatusDeferred { .. }
                | AdoptionFact::TipPublished { .. }
        )
    }
}

/// Whether a run that ended as `end` settled: it ended with no task ready,
/// idle with every task finished or waiting outside the scheduler, or after
/// a drain that ran out of ready tasks. Any other end, such as a drain cut
/// off at its bound, a violation or a harness failure, can stop a task
/// partway through a synchronous call.
pub fn settled(end: &RunEnd) -> bool {
    match end.outcome {
        Outcome::Idle { status } => matches!(
            status,
            IdleStatus::Quiescent | IdleStatus::WaitingOnExternal
        ),
        Outcome::Budget => end.drain.is_some_and(|drain| !drain.exhausted),
        Outcome::Violation | Outcome::Harness => false,
    }
}

/// An [`AdoptionFact`] and the virtual time since the run's start that the
/// reactor reported it at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stamped {
    /// Its stamp, from the node's ([`super::stamp`]).
    pub stamp: u64,
    /// When.
    pub at: Duration,
    /// What.
    pub fact: AdoptionFact,
}

/// The most events a correct run loop takes from one queue between a
/// committed-state change and its next watch read
/// ([`AdoptionEvidence::watch_read`]).
pub const WATCH_READ_EVENTS: usize = 2;

/// What [`AdoptionEvidence::watch_read`] found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchRead {
    /// Nothing was published.
    Nothing,
    /// Every publication was read in time.
    Holds,
    /// A publication is still waiting for its read or its third event.
    Open,
    /// The loop took too many events before reading a publication.
    Broken(String),
}

/// What a check found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Acceptance {
    /// The rule held over the whole run.
    Holds,
    /// The run ended before a fact the rule owes was due.
    Inconclusive(String),
    /// The rule broke.
    Violated(String),
}

/// What the observer and the view share.
#[derive(Debug, Default)]
struct Store {
    /// Where its stamps come from.
    stamps: Stamps,
    /// Each fact, in stamp order.
    facts: Vec<Stamped>,
    /// The time of the tap's last drain.
    horizon: Duration,
    /// Whether the run settled, once it has ended.
    settled: Option<bool>,
    /// Set when the run's log froze. Facts after it are dropped.
    frozen: bool,
}

/// One reactor's adoption facts, shared by its tap and the harness's view.
/// Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct AdoptionEvidence(Arc<Mutex<Store>>);

impl AdoptionEvidence {
    /// Locks the store. Every change completes under the lock, and nothing
    /// else is called while it's held.
    fn store(&self) -> std::sync::MutexGuard<'_, Store> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Evidence that stamps its facts from `stamps`.
    pub(super) fn stamped_by(stamps: Stamps) -> AdoptionEvidence {
        AdoptionEvidence(Arc::new(Mutex::new(Store {
            stamps,
            ..Store::default()
        })))
    }

    /// Records `fact`, reported at `at`, with the next stamp, unless the
    /// evidence is frozen.
    pub(super) fn record(&self, at: Duration, fact: AdoptionFact) {
        let mut store = self.store();
        if !store.frozen {
            let stamp = store.stamps.next();
            store.facts.push(Stamped { stamp, at, fact });
        }
    }

    /// Records that a `Commit` step left the node's engine at
    /// `state_version` and `header_generation`, at the run's current time.
    pub(super) fn publish(&self, state_version: StateVersion, header_generation: HeaderGeneration) {
        let at = zakura_scheduler::hooks::now()
            .map(|now| now.since_start())
            .unwrap_or_default();
        self.record(
            at,
            AdoptionFact::Published {
                state_version,
                header_generation,
            },
        );
    }

    /// Stops recording facts. The tap calls it when the run's log freezes,
    /// as the session view does: the scheduler's shutdown still runs a task
    /// stopped partway through a poll to its end, with the run's clock
    /// installed, and a fact it reports then would rewrite what the run ended
    /// with.
    pub(super) fn freeze(&self) {
        self.store().frozen = true;
    }

    /// Records that the run reached `at`.
    pub(super) fn reach(&self, at: Duration) {
        let mut store = self.store();
        store.horizon = store.horizon.max(at);
    }

    /// Records how the run ended, and whether it [`settled`].
    pub fn end(&self, end: &RunEnd) {
        self.store().settled = Some(settled(end));
    }

    /// Whether the run [`settled`] with a publication still waiting for its
    /// watch read under `queue` ([`WatchRead::Open`]). False before the run
    /// has ended ([`AdoptionEvidence::end`]).
    pub fn unread_at_settled_end(&self, queue: EventQueue) -> bool {
        let open = self.watch_read(queue) == WatchRead::Open;
        open && self.store().settled == Some(true)
    }

    /// Every fact so far, in the order the reactor reported them.
    pub fn facts(&self) -> Vec<Stamped> {
        self.store().facts.clone()
    }

    /// The fact with stamp `stamp`, if this store holds it.
    pub fn fact_at(&self, stamp: u64) -> Option<AdoptionFact> {
        let store = self.store();
        store
            .facts
            .binary_search_by_key(&stamp, |stamped| stamped.stamp)
            .ok()
            .map(|index| store.facts[index].fact.clone())
    }

    /// The latest time the run is known to have reached.
    pub fn horizon(&self) -> Duration {
        self.store().horizon
    }

    /// Every adoption so far, in order.
    pub fn adoptions(&self) -> Vec<Adoption> {
        self.store()
            .facts
            .iter()
            .filter_map(|stamped| match &stamped.fact {
                AdoptionFact::Adopted(adoption) => Some(adoption.clone()),
                _ => None,
            })
            .collect()
    }

    /// The watch rule under a flood of `queue`: after each `Published`
    /// state `v`, the run loop reads a state at or past `v` from its
    /// committed watch before it takes a third event from `queue`. Each turn
    /// checks the watch before taking at most one event from each queue
    /// with `try_recv`, and its `select!` takes at most one more, so a
    /// correct loop takes at most two events from either queue between a
    /// change and its next watch check.
    ///
    /// A publication that neither a read nor a third event has answered yet
    /// is [`WatchRead::Open`].
    pub fn watch_read(&self, queue: EventQueue) -> WatchRead {
        let store = self.store();
        let mut found = WatchRead::Nothing;
        for (index, published) in store.facts.iter().enumerate() {
            let AdoptionFact::Published { state_version, .. } = published.fact else {
                continue;
            };
            let mut taken = 0;
            let mut answered = false;
            for later in &store.facts[index + 1..] {
                match &later.fact {
                    AdoptionFact::WatchObserved {
                        state_version: read,
                    } if *read >= state_version => {
                        answered = true;
                        break;
                    }
                    AdoptionFact::EventTaken { queue: from, .. } if *from == queue => {
                        taken += 1;
                        if taken == WATCH_READ_EVENTS + 1 {
                            return WatchRead::Broken(format!(
                                "the reactor took {taken} {queue:?} events after state \
                                 {state_version:?} was published, with no watch read of it"
                            ));
                        }
                    }
                    _ => {}
                }
            }
            found = match (answered, found) {
                (false, _) | (_, WatchRead::Open) => WatchRead::Open,
                (true, _) => WatchRead::Holds,
            };
        }
        found
    }

    /// Whether some published state had `queue` compete with its watch
    /// read: the run loop took an event from `queue` after the publication
    /// and before any read of it. With
    /// [`AdoptionEvidence::unread_at_settled_end`], that's
    /// [`AdoptionEvidence::watch_read`]'s trigger history: a break of the
    /// rule either takes such an event first or settles with the
    /// publication unread, and a quiet run, whose watch read comes first,
    /// does neither.
    pub fn published_into_flood(&self, queue: EventQueue) -> bool {
        let store = self.store();
        store.facts.iter().enumerate().any(|(index, published)| {
            let AdoptionFact::Published { state_version, .. } = published.fact else {
                return false;
            };
            store.facts[index + 1..]
                .iter()
                .find_map(|later| match &later.fact {
                    AdoptionFact::WatchObserved {
                        state_version: read,
                    } if *read >= state_version => Some(false),
                    AdoptionFact::EventTaken { queue: from, .. } if *from == queue => Some(true),
                    _ => None,
                })
                .unwrap_or(false)
        })
    }

    /// The generation rule: every `Started` and locator `Dispatched` after an
    /// adoption carries that adoption's header generation. A safety rule, so
    /// it's never inconclusive.
    pub fn generation(&self) -> Acceptance {
        let store = self.store();
        let mut latest = None;
        for stamped in &store.facts {
            let (what, generation) = match &stamped.fact {
                AdoptionFact::Adopted(adoption) => {
                    latest = Some(adoption.header_generation);
                    continue;
                }
                AdoptionFact::Started {
                    request_id,
                    generation,
                } => (format!("request {request_id} started"), *generation),
                AdoptionFact::LocatorDispatched { call, generation } => {
                    (format!("locator call {call} was dispatched"), *generation)
                }
                _ => continue,
            };
            if let Some(adopted) = latest.filter(|adopted| *adopted != generation) {
                return Acceptance::Violated(format!(
                    "StaleGeneration: {what} under header generation {} after the reactor \
                     adopted {}",
                    generation.get(),
                    adopted.get()
                ));
            }
        }
        Acceptance::Holds
    }

    /// The status rule. Every queued `Status` advertises the latest adoption.
    /// Each session in `sessions` is owed, while it lasts:
    ///
    /// - its first status fact within [`STATUS_CHANGE_BOUND`] of starting,
    ///   since the reactor publishes to a session as soon as it admits it,
    /// - after an adoption that changes the advertised status, a
    ///   `StatusQueued` with it or a `StatusDeferred` within
    ///   [`STATUS_CHANGE_BOUND`],
    /// - after a deferral, another status fact within [`STATUS_RETRY_BOUND`].
    ///
    /// A later adoption that changes the status replaces what's owed, since
    /// the publisher only ever sends its latest desired status, and resets the
    /// deadline even when the session already holds that status, since the
    /// publisher then sends it again. One that doesn't change the status keeps
    /// the deadline. An adoption before a session starts owes it nothing
    /// beyond its first status, and a deadline at or after its end is owed
    /// nothing. The bounds assume the harness doesn't hold the reactor's task
    /// (`Sim::hold_tasks`), as the header harnesses don't. A run that ends before a deadline is
    /// inconclusive. Status facts for a session not in `sessions` are checked
    /// only for what they advertise.
    pub fn status(&self, sessions: &[SessionLife]) -> Acceptance {
        let store = self.store();
        let mut latest: Option<&Adoption> = None;
        let mut owed: BTreeMap<u64, Owed> = sessions
            .iter()
            .map(|life| {
                let owed = Owed {
                    due: Some(life.from + STATUS_CHANGE_BOUND),
                    from: life.from,
                    until: life.until,
                };
                (life.session_id, owed)
            })
            .collect();
        for stamped in &store.facts {
            if let Some((session, due)) = overdue(&owed, stamped.at) {
                return unsent(session, due);
            }
            match &stamped.fact {
                AdoptionFact::Adopted(adoption) => {
                    let changed = latest.is_none_or(|latest| !latest.advertises_as(adoption));
                    latest = Some(adoption);
                    if !changed {
                        continue;
                    }
                    for session in owed.values_mut().filter(|owed| owed.from <= stamped.at) {
                        session.due = Some(stamped.at + STATUS_CHANGE_BOUND);
                    }
                }
                AdoptionFact::StatusQueued {
                    session_id, status, ..
                } => {
                    if let Some(adoption) = latest.filter(|latest| !latest.advertised_by(status)) {
                        return Acceptance::Violated(format!(
                            "StaleStatus: session {session_id} was queued a status for tip \
                             {:?}, anchor {:?}, while the reactor's adopted tip is {:?}, \
                             anchor {:?}",
                            Frontier::new(status.selected_tip_height, status.selected_tip_hash),
                            Frontier::new(status.work_anchor_height, status.work_anchor_hash),
                            adoption.header_best,
                            adoption.finalized,
                        ));
                    }
                    if let Some(session) = owed.get_mut(session_id) {
                        session.due = None;
                    }
                }
                AdoptionFact::StatusDeferred { session_id, .. } => {
                    if let Some(session) = owed.get_mut(session_id) {
                        session.due = Some(stamped.at + STATUS_RETRY_BOUND);
                    }
                }
                _ => {}
            }
        }
        if let Some((session, due)) = overdue(&owed, store.horizon) {
            return unsent(session, due);
        }
        match owed
            .iter()
            .find_map(|(session, owed)| owed.pending().map(|due| (*session, due)))
        {
            Some((session, due)) => Acceptance::Inconclusive(format!(
                "the run ended at {:?}, before session {session}'s status was due at {due:?}",
                store.horizon
            )),
            None => Acceptance::Holds,
        }
    }
}

/// A session's lifetime, from the harness's side of its link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionLife {
    /// The session.
    pub session_id: u64,
    /// When it was linked, since the run's start.
    pub from: Duration,
    /// When it closed or was replaced, if it did.
    pub until: Option<Duration>,
}

/// What the status rule says one session is owed.
struct Owed {
    /// When its next status fact is due, if one is owed.
    due: Option<Duration>,
    /// When it started.
    from: Duration,
    /// When it ended, if it did.
    until: Option<Duration>,
}

impl Owed {
    /// The deadline still owed: one before the session's end, if any.
    fn pending(&self) -> Option<Duration> {
        self.due
            .filter(|due| self.until.is_none_or(|until| *due < until))
    }
}

/// The first session whose owed deadline is before `now`, and that deadline.
fn overdue(owed: &BTreeMap<u64, Owed>, now: Duration) -> Option<(u64, Duration)> {
    owed.iter().find_map(|(session, owed)| {
        owed.pending()
            .filter(|due| now > *due)
            .map(|due| (*session, due))
    })
}

/// The status rule's break for `session`, owed a status fact by `due`.
fn unsent(session: u64, due: Duration) -> Acceptance {
    Acceptance::Violated(format!(
        "StatusOverdue: session {session} was owed its latest status by {due:?}, and the \
         reactor neither queued it nor recorded a deferral"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::header_sync::{watch_end_violations, WATCH_OBSERVED_UNDER_FLOOD},
        end::Drain,
    };

    /// An adoption at generation `generation` whose tip is at `height`.
    fn adoption(generation: u64, height: u32) -> Adoption {
        let anchor = Frontier::new(block::Height(0), block::Hash([0; 32]));
        Adoption {
            state_version: StateVersion::new(generation),
            header_generation: HeaderGeneration::new(generation),
            finalized: anchor,
            header_best: tip(height),
            suffix_work: U256::from(height),
            oldest_retained: anchor.height,
            verified_generation: VerifiedGeneration::new(generation),
            verified_best: anchor,
        }
    }

    /// The tip at `height`.
    fn tip(height: u32) -> Frontier {
        let byte = u8::try_from(height).expect("test heights are small");
        Frontier::new(block::Height(height), block::Hash([byte; 32]))
    }

    /// The status `adoption` advertises.
    fn status(adoption: &Adoption) -> Status {
        Status {
            work_anchor_height: adoption.finalized.height,
            work_anchor_hash: adoption.finalized.hash,
            selected_tip_height: adoption.header_best.height,
            selected_tip_hash: adoption.header_best.hash,
            suffix_cumulative_work: adoption.suffix_work,
            oldest_retained_height: adoption.oldest_retained,
            max_headers_per_response: 1,
            max_subscriptions: 1,
            max_message_bytes: 1,
            tree_aux_schema_mask: 1,
        }
    }

    /// The test peer.
    fn peer() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![7; 32]).expect("32 bytes is a peer ID")
    }

    /// Evidence holding `facts`, each at its time in milliseconds, reaching
    /// `horizon_ms`.
    fn evidence(facts: Vec<(u64, AdoptionFact)>, horizon_ms: u64) -> AdoptionEvidence {
        let evidence = AdoptionEvidence::default();
        for (at, fact) in facts {
            evidence.record(Duration::from_millis(at), fact);
        }
        evidence.reach(Duration::from_millis(horizon_ms));
        evidence
    }

    /// `adoption` as a fact.
    fn adopted(adoption: &Adoption) -> AdoptionFact {
        AdoptionFact::Adopted(adoption.clone())
    }

    /// `status` queued on session 1.
    fn queued(status: Status) -> AdoptionFact {
        AdoptionFact::StatusQueued {
            peer: peer(),
            session_id: 1,
            status,
        }
    }

    /// A deferral on session 1.
    fn deferred() -> AdoptionFact {
        AdoptionFact::StatusDeferred {
            peer: peer(),
            session_id: 1,
        }
    }

    /// A wire take at `point` leaving `queued`.
    fn wire(point: TakePoint, queued: usize) -> AdoptionFact {
        AdoptionFact::EventTaken {
            queue: EventQueue::Wire,
            point,
            queued,
        }
    }

    /// State `version` published by a commit.
    fn commit(version: u64) -> AdoptionFact {
        AdoptionFact::Published {
            state_version: StateVersion::new(version),
            header_generation: HeaderGeneration::new(version),
        }
    }

    /// A watch read of state `version`.
    fn read(version: u64) -> AdoptionFact {
        AdoptionFact::WatchObserved {
            state_version: StateVersion::new(version),
        }
    }

    /// A loop that stops right after a publication, taking no event and
    /// reading nothing, leaves no competing event, so only the settled end
    /// shows it. That end is the trigger history and the break, unless the
    /// harness had begun a shutdown. A run that hasn't ended, or ended
    /// unsettled, shows neither, and nor does a read.
    #[test]
    fn a_publication_unread_at_a_settled_end_breaks_the_watch_rule() {
        let settled = RunEnd {
            outcome: Outcome::Budget,
            steps: 100,
            violation: None,
            drain: Some(Drain {
                steps: 10,
                exhausted: false,
            }),
            harness: None,
        };
        let cut = RunEnd {
            drain: Some(Drain {
                steps: 10,
                exhausted: true,
            }),
            ..settled.clone()
        };
        let wedged = evidence(vec![(0, commit(2))], 0);
        assert!(!wedged.unread_at_settled_end(EventQueue::Wire));
        wedged.end(&settled);
        assert!(!wedged.published_into_flood(EventQueue::Wire));
        assert!(wedged.unread_at_settled_end(EventQueue::Wire));
        let found = watch_end_violations(&wedged, EventQueue::Wire, false);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].property.to_string(), WATCH_OBSERVED_UNDER_FLOOD);
        assert!(watch_end_violations(&wedged, EventQueue::Wire, true).is_empty());

        let unsettled = evidence(vec![(0, commit(2))], 0);
        unsettled.end(&cut);
        assert!(!unsettled.unread_at_settled_end(EventQueue::Wire));
        assert!(watch_end_violations(&unsettled, EventQueue::Wire, false).is_empty());

        let read = evidence(vec![(0, commit(2)), (0, self::read(2))], 0);
        read.end(&settled);
        assert!(!read.unread_at_settled_end(EventQueue::Wire));
        assert!(watch_end_violations(&read, EventQueue::Wire, false).is_empty());
    }

    /// The watch rule over `facts`, all at time 0.
    fn watch(facts: Vec<AdoptionFact>) -> WatchRead {
        evidence(facts.into_iter().map(|fact| (0, fact)).collect(), 0).watch_read(EventQueue::Wire)
    }

    /// Two wire events and then a read of a later state answer a
    /// publication. A third wire event before any read breaks the rule, a
    /// read of an earlier state doesn't count, lifecycle events don't
    /// count against the wire rule, and a publication with no answer yet is
    /// open. No publication is nothing.
    #[test]
    fn the_watch_rule_counts_one_queue_after_each_publication() {
        use TakePoint::{Select, TurnStart};
        let lifecycle = AdoptionFact::EventTaken {
            queue: EventQueue::Lifecycle,
            point: TurnStart,
            queued: 3,
        };
        assert_eq!(
            watch(vec![wire(TurnStart, 4), AdoptionFact::Pass]),
            WatchRead::Nothing
        );
        assert_eq!(
            watch(vec![
                commit(2),
                wire(TurnStart, 4),
                wire(Select, 3),
                lifecycle.clone(),
                lifecycle.clone(),
                lifecycle,
                read(3),
            ]),
            WatchRead::Holds
        );
        assert!(matches!(
            watch(vec![
                commit(2),
                read(1),
                wire(TurnStart, 4),
                wire(Select, 3),
                wire(TurnStart, 2),
                read(2),
            ]),
            WatchRead::Broken(_)
        ));
        assert_eq!(
            watch(vec![commit(2), wire(TurnStart, 4), wire(Select, 3)]),
            WatchRead::Open
        );
        assert_eq!(
            watch(vec![commit(2), read(2), commit(3), wire(TurnStart, 4)]),
            WatchRead::Open
        );
    }

    /// A publication whose queue delivers an event before its watch read
    /// is published into a flood. One read first, or with no later event,
    /// isn't, and a lifecycle event doesn't count for the wire queue.
    #[test]
    fn a_publication_read_before_any_event_is_not_into_a_flood() {
        let flooded = |facts: Vec<AdoptionFact>| {
            evidence(facts.into_iter().map(|fact| (0, fact)).collect(), 0)
                .published_into_flood(EventQueue::Wire)
        };
        let lifecycle = AdoptionFact::EventTaken {
            queue: EventQueue::Lifecycle,
            point: TakePoint::TurnStart,
            queued: 1,
        };
        assert!(flooded(vec![
            commit(2),
            wire(TakePoint::TurnStart, 1),
            read(2)
        ]));
        assert!(!flooded(vec![
            commit(2),
            read(2),
            wire(TakePoint::TurnStart, 5)
        ]));
        assert!(!flooded(vec![wire(TakePoint::TurnStart, 5), commit(2)]));
        assert!(!flooded(vec![commit(2), lifecycle, read(2)]));
    }

    /// Whether `outcome` is a violation naming `rule`.
    fn violates(outcome: &Acceptance, rule: &str) -> bool {
        matches!(outcome, Acceptance::Violated(why) if why.starts_with(rule))
    }

    /// Work under the latest generation holds, and a round or locator read under
    /// an older one after a newer adoption breaks the rule.
    #[test]
    fn work_under_an_older_generation_breaks_the_generation_rule() {
        let (first, second) = (adoption(1, 1), adoption(2, 2));
        let locator = |generation| AdoptionFact::LocatorDispatched {
            call: 1,
            generation: HeaderGeneration::new(generation),
        };
        let started = |generation| AdoptionFact::Started {
            request_id: 1,
            generation: HeaderGeneration::new(generation),
        };
        let held = evidence(
            vec![
                (0, adopted(&first)),
                (0, locator(1)),
                (1, adopted(&second)),
                (1, started(2)),
            ],
            1,
        );
        assert_eq!(held.generation(), Acceptance::Holds);
        for stale in [locator(1), started(1)] {
            let broken = evidence(
                vec![(0, adopted(&first)), (1, adopted(&second)), (1, stale)],
                1,
            );
            assert!(violates(&broken.generation(), "StaleGeneration"));
        }
    }

    /// A session is owed its first status and each new one within their
    /// bounds while it lasts, a deferral owes a retry, and a later changed
    /// adoption replaces what's owed while an unchanged one keeps the
    /// deadline. A stale queued status breaks for any session, and a run
    /// ending before a deadline is inconclusive.
    #[test]
    fn a_session_is_owed_its_latest_status_while_it_lasts() {
        let (first, second, third) = (adoption(1, 1), adoption(2, 2), adoption(3, 3));
        let life = |until| {
            [SessionLife {
                session_id: 1,
                from: Duration::ZERO,
                until,
            }]
        };
        let live = life(None);
        let held = evidence(
            vec![
                (0, adopted(&first)),
                (0, queued(status(&first))),
                (10, adopted(&second)),
                (1_000, deferred()),
                // A later changed adoption replaces what's owed.
                (1_500, adopted(&third)),
                (2_000, queued(status(&third))),
            ],
            60_000,
        );
        assert_eq!(held.status(&live), Acceptance::Holds);

        let stale = evidence(
            vec![
                (0, adopted(&first)),
                (5, adopted(&second)),
                (5, queued(status(&first))),
            ],
            0,
        );
        assert!(violates(&stale.status(&live), "StaleStatus"));
        assert!(violates(&stale.status(&[]), "StaleStatus"));

        // A session that never gets a status breaks once its first is due.
        let silent = vec![(0, adopted(&first))];
        assert!(violates(
            &evidence(silent.clone(), 2_001).status(&live),
            "StatusOverdue"
        ));
        assert!(matches!(
            evidence(silent, 2_000).status(&live),
            Acceptance::Inconclusive(_)
        ));

        let unsent = vec![
            (0, adopted(&first)),
            (0, queued(status(&first))),
            (10, adopted(&second)),
        ];
        let overdue = evidence(unsent.clone(), 2_011);
        assert!(violates(&overdue.status(&live), "StatusOverdue"));
        assert_eq!(overdue.status(&[]), Acceptance::Holds);
        // Ending before the deadline owes nothing, and ending after it still
        // breaks the rule.
        assert_eq!(
            overdue.status(&life(Some(Duration::from_millis(2_010)))),
            Acceptance::Holds
        );
        assert!(violates(
            &overdue.status(&life(Some(Duration::from_millis(2_011)))),
            "StatusOverdue"
        ));
        let cut = evidence(unsent.clone(), 2_010);
        assert!(matches!(cut.status(&live), Acceptance::Inconclusive(_)));

        // An adoption that changes only the state version keeps the deadline.
        let same = Adoption {
            state_version: StateVersion::new(9),
            ..second.clone()
        };
        let mut postponed = unsent;
        postponed.push((1_900, adopted(&same)));
        postponed.push((3_000, queued(status(&second))));
        assert!(violates(
            &evidence(postponed, 3_000).status(&live),
            "StatusOverdue"
        ));

        // A session that starts after an adoption owes its first status from
        // its own start.
        let late = [SessionLife {
            session_id: 1,
            from: Duration::from_secs(10),
            until: None,
        }];
        let joined = evidence(
            vec![(0, adopted(&first)), (10_000, queued(status(&first)))],
            60_000,
        );
        assert_eq!(joined.status(&late), Acceptance::Holds);

        // Returning to the queued status still owes a status in time, which the
        // publisher sends again.
        let back = vec![
            (0, adopted(&first)),
            (0, queued(status(&first))),
            (1_000, adopted(&second)),
            (2_900, adopted(&first)),
        ];
        assert!(matches!(
            evidence(back.clone(), 3_100).status(&live),
            Acceptance::Inconclusive(_)
        ));
        let mut resent = back;
        resent.push((3_000, queued(status(&first))));
        assert_eq!(evidence(resent, 60_000).status(&live), Acceptance::Holds);

        let retry = vec![
            (0, adopted(&first)),
            (0, queued(status(&first))),
            (10, adopted(&second)),
            (100, deferred()),
        ];
        let unretried = evidence(retry.clone(), 1_101);
        assert!(violates(&unretried.status(&live), "StatusOverdue"));
        let mut retried = retry;
        retried.push((1_100, queued(status(&second))));
        assert_eq!(evidence(retried, 60_000).status(&live), Acceptance::Holds);
    }
}
