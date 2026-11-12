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
//! [`AdoptionEvidence::generation`] checks hdr-4's generation rule over the
//! facts. Its status and tip rules are
//! `header-sync/state-publication-consistent`'s
//! ([`super::publication`]), whose status deadline replaced
//! `StatusOverdue`.

use std::time::Duration;

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{Frontier, HeaderGeneration, StateVersion, VerifiedGeneration};
use zakura_network::zakura::{
    observe::{EventQueue, ReactorCause, ReactorEvent, TakePoint},
    Status, ZakuraPeerId,
};

use super::stamp::Stamps;
use crate::{
    stores::{debug_snapshot, EvidenceStores, Guard, Registered, Snapshot},
    RunEnd,
};

/// How long after an adoption that changes a session's status the
/// reactor's status publisher may wait before it queues that status:
/// `MAX_CHANGE_DELAY` in header sync's status publisher.
pub const STATUS_CHANGE_BOUND: Duration = Duration::from_secs(2);

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

/// Whether a run that ended as `end` settled: it ran dry at rest
/// ([`RunEnd::at_rest`]). Any other end can stop a task partway through a
/// synchronous call.
pub fn settled(end: &RunEnd) -> bool {
    end.at_rest()
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
    /// The loop took too many events before reading a publication: each
    /// publication it broke the rule for, in publication order, at least
    /// one.
    Broken(Vec<String>),
}

/// What answered one publication under the watch rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicationRead {
    /// A watch read at or past it.
    Read,
    /// A third event from the queue, first: its break.
    Broken,
    /// Neither, yet.
    Open,
}

/// What a check found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Acceptance {
    /// The rule held over the whole run.
    Holds,
    /// The run ended before a fact the rule owes was due.
    Open(String),
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
}

impl Snapshot for Store {
    /// The stamp counter, every fact, the horizon and the settled end.
    fn snapshot(&self) -> serde_json::Value {
        debug_snapshot(self)
    }
}

/// One reactor's adoption facts, shared by its tap and the harness's view.
/// Cloning shares the store.
#[derive(Clone, Debug)]
pub struct AdoptionEvidence(Registered<Store>);

impl AdoptionEvidence {
    /// Locks the store for reading. Nothing else is called while it's held.
    fn store(&self) -> Guard<'_, Store> {
        self.0.lock()
    }

    /// A store in `stores` with stamps of its own, for a harness with no
    /// tap to share them.
    pub fn new(stores: &EvidenceStores) -> AdoptionEvidence {
        AdoptionEvidence::stamped_by(stores, Stamps::default())
    }

    /// A store in `stores` that stamps its facts from `stamps`.
    pub(super) fn stamped_by(stores: &EvidenceStores, stamps: Stamps) -> AdoptionEvidence {
        AdoptionEvidence(stores.register(
            "adoption",
            Store {
                stamps,
                ..Store::default()
            },
        ))
    }

    /// Records `fact`, reported at `at`, with the next stamp, unless the
    /// evidence is frozen.
    pub(super) fn record(&self, at: Duration, fact: AdoptionFact) {
        self.0.write(|store| {
            let stamp = store.stamps.next();
            store.facts.push(Stamped { stamp, at, fact });
        });
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

    /// Records that the run reached `at`.
    pub(super) fn reach(&self, at: Duration) {
        self.0.write(|store| store.horizon = store.horizon.max(at));
    }

    /// Records how the run ended, and whether it [`settled`].
    pub fn end(&self, end: &RunEnd) {
        self.0.write(|store| store.settled = Some(settled(end)));
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
    /// is [`WatchRead::Open`]. Each publication whose third event came
    /// first is its own break.
    pub fn watch_read(&self, queue: EventQueue) -> WatchRead {
        let mut found = WatchRead::Nothing;
        let mut breaks = Vec::new();
        for (state_version, read) in self.publication_reads(queue) {
            found = match (read, found) {
                (PublicationRead::Broken, found) => {
                    breaks.push(format!(
                        "the reactor took {} {queue:?} events after state {state_version:?} \
                         was published, with no watch read of it",
                        WATCH_READ_EVENTS + 1
                    ));
                    found
                }
                (PublicationRead::Open, _) | (_, WatchRead::Open) => WatchRead::Open,
                (PublicationRead::Read, _) => WatchRead::Holds,
            };
        }
        if breaks.is_empty() {
            found
        } else {
            WatchRead::Broken(breaks)
        }
    }

    /// The first published state still waiting for its watch read under
    /// `queue` ([`WatchRead::Open`]), which an unread end names as its
    /// subject.
    pub fn unread_publication(&self, queue: EventQueue) -> Option<StateVersion> {
        self.publication_reads(queue)
            .into_iter()
            .find_map(|(state_version, read)| {
                (read == PublicationRead::Open).then_some(state_version)
            })
    }

    /// Each `Published` state in order, with what answered it under
    /// `queue` ([`AdoptionEvidence::watch_read`]'s rule).
    fn publication_reads(&self, queue: EventQueue) -> Vec<(StateVersion, PublicationRead)> {
        let store = self.store();
        let mut reads = Vec::new();
        for (index, published) in store.facts.iter().enumerate() {
            let AdoptionFact::Published { state_version, .. } = published.fact else {
                continue;
            };
            let mut taken = 0;
            let mut read = PublicationRead::Open;
            for later in &store.facts[index + 1..] {
                match &later.fact {
                    AdoptionFact::WatchObserved {
                        state_version: observed,
                    } if *observed >= state_version => {
                        read = PublicationRead::Read;
                        break;
                    }
                    AdoptionFact::EventTaken { queue: from, .. } if *from == queue => {
                        taken += 1;
                        if taken == WATCH_READ_EVENTS + 1 {
                            read = PublicationRead::Broken;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            reads.push((state_version, read));
        }
        reads
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
    /// it's never open.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::header_sync::{watch_end_violations, WATCH_OBSERVED_UNDER_FLOOD},
        end::Drain,
        Outcome,
    };
    use zakura_scheduler::Status as SchedulerStatus;

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

    /// Evidence holding `facts`, each at its time in milliseconds, reaching
    /// `horizon_ms`.
    fn evidence(facts: Vec<(u64, AdoptionFact)>, horizon_ms: u64) -> AdoptionEvidence {
        let evidence = AdoptionEvidence::new(&crate::stores::EvidenceStores::default());
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
            rest: Some(SchedulerStatus::Quiescent),
        };
        // A drain cut off at its bound is a harness failure.
        let cut = RunEnd {
            outcome: Outcome::Harness,
            drain: Some(Drain {
                steps: 10,
                exhausted: true,
            }),
            harness: Some(crate::HarnessFailure::DrainBound {
                open: "task:reactor.1:poll#1".to_owned(),
            }),
            rest: None,
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

    /// An adoption fact recorded after the node's stores froze is dropped.
    #[test]
    fn a_frozen_adoption_store_drops_a_fact() {
        crate::stores::assert_freeze_drops_writes(
            super::AdoptionEvidence::new,
            |adoption| adoption.record(Duration::ZERO, commit(1)),
            super::AdoptionEvidence::facts,
        );
    }

    /// Adoption evidence with no tap behind it.
    #[derive(Clone)]
    struct Adoptions(AdoptionEvidence);

    impl Default for Adoptions {
        /// An empty store in a handle of its own.
        fn default() -> Self {
            Adoptions(AdoptionEvidence::new(
                &crate::stores::EvidenceStores::default(),
            ))
        }
    }

    impl crate::domain::header_sync::AdoptionView for Adoptions {
        fn adoption(&self) -> &AdoptionEvidence {
            &self.0
        }
    }

    impl crate::property::HasView for Adoptions {
        type View = Adoptions;

        /// A copy sharing the store.
        fn view(&self) -> Adoptions {
            self.clone()
        }

        /// The store, as a node harness reports its own.
        fn snapshot(&self) -> Option<serde_json::Value> {
            Some(self.0 .0.stores().report_snapshot())
        }
    }

    impl crate::harness::scripted::Script for Adoptions {
        const NAME: &'static str = "test/two-unread-publications";
        /// Whether step 1's three events come from the lifecycle queue,
        /// rather than the wire's.
        type Case = bool;

        /// Both watch rules.
        fn properties(
        ) -> Vec<crate::property::Property<crate::harness::scripted::ScriptRun<Adoptions>>>
        {
            vec![
                crate::domain::header_sync::watch_observed_under_flood(),
                crate::domain::header_sync::watch_observed_under_lifecycle_flood(),
            ]
        }

        /// The schedule and the view.
        fn sources() -> crate::property::Sources {
            use crate::property::Source;
            crate::property::Sources::observed(&[Source::Schedule, Source::View])
        }

        /// At step 1: states 2 and 3 published back to back, then three
        /// events from the case's queue with no watch read, as the
        /// reactor's tap reports them.
        fn observe(&self, lifecycle: &bool, log: &mut crate::ObservationLog) {
            if log.step() != 1 {
                return;
            }
            for version in [2, 3] {
                self.0.record(Duration::ZERO, commit(version));
            }
            let queue = if *lifecycle {
                EventQueue::Lifecycle
            } else {
                EventQueue::Wire
            };
            for queued in [3, 2, 1] {
                self.0.record(
                    Duration::ZERO,
                    AdoptionFact::EventTaken {
                        queue,
                        point: TakePoint::TurnStart,
                        queued,
                    },
                );
            }
        }
    }

    /// C21 through the sidecar, for each watch rule: two states published
    /// back to back and both unread when the loop takes its third event
    /// from the flooded queue are two violations at that step, each naming
    /// its own state, in the full report recorded beside the trace and its
    /// fresh replay. The other queue's rule reports neither.
    #[test]
    fn every_unread_publication_at_a_boundary_reaches_the_sidecar() {
        use crate::{
            domain::header_sync::WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
            harness::scripted::{findings, sidecar},
        };
        for (lifecycle, flooded, other, queue) in [
            (
                false,
                WATCH_OBSERVED_UNDER_FLOOD,
                WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
                EventQueue::Wire,
            ),
            (
                true,
                WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
                WATCH_OBSERVED_UNDER_FLOOD,
                EventQueue::Lifecycle,
            ),
        ] {
            let full = sidecar::<Adoptions>(&lifecycle, &[]);
            let found = findings(&full, flooded);
            let violation = |version: u64| crate::Violation {
                subject: None,
                property: flooded.to_owned(),
                step: 1,
                witness: vec![1],
                expected: "each published state is read from the committed watch before a third \
                           event from the flooded queue"
                    .to_owned(),
                actual: format!(
                    "the reactor took 3 {queue:?} events after state {:?} was published, with \
                     no watch read of it",
                    StateVersion::new(version)
                ),
            };
            assert_eq!(found.violations, [violation(2), violation(3)], "{flooded}");
            assert_eq!(
                found.outcome,
                crate::PropertyOutcome::Violated { witness: vec![1] },
                "{flooded}"
            );
            assert!(findings(&full, other).violations.is_empty(), "{other}");
        }
    }
}
