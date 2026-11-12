//! `header-sync/capacity-release-wakes`'s facts, as the reactor tap finds
//! them.
//!
//! The property applies only where the harness declares the wake fixture
//! ([`WakeFixture`]), as `header-sync/release-wake` does, whose shape rules
//! out the other reasons an offer can wait. The check here follows the reactor's facts in order. It keeps an
//! offer that waited for header room (`HeadroomWait`), and at each budget
//! return (a `BudgetChanged` that gives headers back) it checks, from the
//! facts so far, that the fixture held:
//!
//! - the waiting offer's session is live, and its peer started no round
//! - no locator call is in flight
//! - no adopted snapshot changed `header_best` since the wait
//! - the last probe read found a locator permit free
//! - the fixture's body window, less the headers the budget holds, leaves
//!   at least one header of room
//!
//! If they all hold, the release opens the waiting peer's wake
//! ([`WakeFact::Opened`]). Otherwise it opens nothing, so a broken fixture
//! reads as not applicable, never as a missed wake. The waiting peer's
//! locator reaching storage ([`WakeFact::Submitted`]) answers the wake, and
//! so does its session ending ([`WakeFact::Ended`]) when the environment
//! ended it ([`EndedBy`]). A deferred cached pass ([`WakeFact::Deferred`])
//! moves its deadline to the pass's refill, but never past one refill
//! interval after the wake opened.
//!
//! Each fact takes the node's next stamp ([`super::stamp`]), which the
//! storage view hands the registry as its view-side facts.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    observe::{PortCall, ReactorEvent},
    ZakuraPeerId,
};
use zakura_scheduler::SimInstant;

use super::{session::SessionEvidence, stamp::Stamps};
use crate::stores::{debug_snapshot, EvidenceStores, Registered, Snapshot};
use crate::{
    observe::{StorageFact, StorageRequest},
    property::EndedBy,
};

/// The wake fixture a harness declares. It fixes one waiting mirror whose
/// only `Status` arrives once filling mirrors' held rounds use the room,
/// and a body lag that leaves `body_window` headers of room.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WakeFixture {
    /// The headers of room the node's body lag leaves.
    pub body_window: usize,
}

/// A fact the wake check found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeFact {
    /// A budget return left room for the offer `peer`'s session
    /// `session_id` waited with, with the fixture holding.
    Opened {
        /// The waiting peer.
        peer: ZakuraPeerId,
        /// Its session.
        session_id: u64,
        /// When the release opened it.
        at: SimInstant,
    },
    /// A locator read for `peer` reached storage.
    Submitted {
        /// The peer the read is for.
        peer: ZakuraPeerId,
    },
    /// `peer`'s session `session_id` ended or was replaced.
    Ended {
        /// The peer.
        peer: ZakuraPeerId,
        /// The ended session.
        session_id: u64,
        /// Who ended it, when the reactor reported the end.
        by: EndedBy,
    },
    /// The reactor's cached pass waits for its budget's refill at `until`.
    Deferred {
        /// The refill.
        until: SimInstant,
    },
}

/// What [`WakeEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// The fixture the scenario declared, if any.
    fixture: Option<WakeFixture>,
    /// Where its stamps come from: the node's, once its tap watches it.
    stamps: Stamps,
    /// Each fact with its stamp, in stamp order.
    facts: Vec<(u64, WakeFact)>,
}

impl Snapshot for Store {
    /// Everything it holds.
    fn snapshot(&self) -> serde_json::Value {
        debug_snapshot(self)
    }
}

/// One reactor's wake facts, shared by its tap and whoever reads them.
/// Cloning shares the store.
#[derive(Clone, Debug)]
pub struct WakeEvidence(Registered<Store>);

impl WakeEvidence {
    /// Evidence in `stores`, the node's, for a run that declares
    /// `fixture`, or none.
    pub fn new(stores: &EvidenceStores, fixture: Option<WakeFixture>) -> WakeEvidence {
        WakeEvidence(stores.register(
            "wake",
            Store {
                fixture,
                stamps: Stamps::default(),
                facts: Vec::new(),
            },
        ))
    }

    /// Declares `fixture` on prepared ports, as [`WakeEvidence::new`] with
    /// it does.
    pub fn declare(&self, fixture: WakeFixture) {
        self.change(|store| store.fixture = Some(fixture));
    }

    /// Stamps facts from `stamps`, the node's, from now on.
    pub(super) fn stamp_with(&self, stamps: Stamps) {
        self.change(|store| store.stamps = stamps);
    }

    /// Runs `read` on the store. Nothing else is called while the lock is
    /// held.
    fn with<R>(&self, read: impl FnOnce(&Store) -> R) -> R {
        self.0.read(read)
    }

    /// Runs `change` on the store, unless it's frozen, which drops the
    /// change.
    fn change<R>(&self, change: impl FnOnce(&mut Store) -> R) -> Option<R> {
        self.0.write(change)
    }

    /// The declared fixture, if any.
    pub fn fixture(&self) -> Option<WakeFixture> {
        self.with(|store| store.fixture)
    }

    /// Keeps `fact` with the next stamp.
    fn push(&self, fact: WakeFact) {
        self.change(|store| {
            let stamp = store.stamps.next();
            store.facts.push((stamp, fact));
        });
    }

    /// The fact with `stamp`, if any.
    pub fn fact(&self, stamp: u64) -> Option<WakeFact> {
        self.with(|store| {
            store
                .facts
                .binary_search_by_key(&stamp, |(at, _)| *at)
                .ok()
                .map(|index| store.facts[index].1.clone())
        })
    }

    /// Every fact so far, with its stamp.
    pub fn facts(&self) -> Vec<(u64, WakeFact)> {
        self.with(|store| store.facts.clone())
    }

    /// When the last wake a release opened opened, if any did.
    pub fn last_opened(&self) -> Option<SimInstant> {
        self.with(|store| {
            store.facts.iter().rev().find_map(|(_, fact)| match fact {
                WakeFact::Opened { at, .. } => Some(*at),
                _ => None,
            })
        })
    }

    /// Whether any release opened a wake.
    pub fn any_opened(&self) -> bool {
        self.with(|store| {
            store
                .facts
                .iter()
                .any(|(_, fact)| matches!(fact, WakeFact::Opened { .. }))
        })
    }
}

/// An offer that waited for room, as the check keeps it.
struct Wait {
    peer: ZakuraPeerId,
    session_id: u64,
    /// The node's `header_best` when it waited.
    header_best: Option<Frontier>,
}

/// The check's state: what the facts so far say about sessions, rounds,
/// locator calls, the adopted tip, the budget and the probe.
pub(super) struct WakeCheck {
    evidence: WakeEvidence,
    /// The node's session view, which says who ended a session.
    sessions: SessionEvidence,
    /// The offer waiting for room, if any.
    waiting: Option<Wait>,
    /// Each peer's live session.
    live: HashMap<ZakuraPeerId, u64>,
    /// The peers that started a round.
    started: HashSet<ZakuraPeerId>,
    /// Each locator call's peer, by call.
    locators: HashMap<u64, ZakuraPeerId>,
    /// The locator calls not yet ended.
    in_flight: HashSet<u64>,
    /// The last adopted `header_best`.
    header_best: Option<Frontier>,
    /// The headers the budget holds, reserved and owned, as its changes
    /// sum.
    claimed: i64,
    /// The last probe read's free locator permits.
    permits: Option<usize>,
}

impl WakeCheck {
    /// A check that keeps its facts in `evidence`, and reads who ended a
    /// session from `sessions`.
    pub(super) fn new(evidence: WakeEvidence, sessions: SessionEvidence) -> WakeCheck {
        WakeCheck {
            evidence,
            sessions,
            waiting: None,
            live: HashMap::new(),
            started: HashSet::new(),
            locators: HashMap::new(),
            in_flight: HashSet::new(),
            header_best: None,
            claimed: 0,
            permits: None,
        }
    }

    /// Notes `event`, which the reactor's own task reported at `now`.
    pub(super) fn observe(&mut self, event: &ReactorEvent, now: SimInstant) {
        let Some(fixture) = self.evidence.fixture() else {
            return;
        };
        match event {
            ReactorEvent::HeadroomWait {
                peer, session_id, ..
            } => {
                self.waiting = Some(Wait {
                    peer: peer.clone(),
                    session_id: *session_id,
                    header_best: self.header_best,
                });
            }
            ReactorEvent::BudgetChanged {
                reserved_delta,
                owned_delta,
                ..
            } => {
                self.claimed = self
                    .claimed
                    .saturating_add(*reserved_delta)
                    .saturating_add(*owned_delta);
                if reserved_delta.saturating_add(*owned_delta) < 0 {
                    self.released(fixture, now);
                }
            }
            ReactorEvent::CachedPassDeferred { refill_in } => {
                if let Some(until) = now.checked_add(*refill_in) {
                    self.evidence.push(WakeFact::Deferred { until });
                }
            }
            ReactorEvent::SessionAdmitted {
                peer,
                session_id,
                replaced,
                ..
            } => {
                if let Some(replaced) = replaced {
                    self.ended(peer, *replaced);
                }
                self.live.insert(peer.clone(), *session_id);
            }
            ReactorEvent::SessionEnded {
                peer, session_id, ..
            } => {
                if self.live.get(peer) == Some(session_id) {
                    self.live.remove(peer);
                }
                self.ended(peer, *session_id);
            }
            ReactorEvent::Started { request, .. } => {
                self.started.insert(request.peer.clone());
            }
            ReactorEvent::Dispatched {
                call,
                op: PortCall::Locator { peer, .. },
            } => {
                self.locators.insert(*call, peer.clone());
                self.in_flight.insert(*call);
            }
            ReactorEvent::CallEnded { call, .. } => {
                self.in_flight.remove(call);
            }
            ReactorEvent::SnapshotAdopted { header_best, .. } => {
                self.header_best = Some(*header_best);
            }
            _ => {}
        }
    }

    /// Notes a storage fact the node's port logged.
    pub(super) fn storage(&mut self, fact: &StorageFact) {
        if self.evidence.fixture().is_none() {
            return;
        }
        if let StorageFact::Submitted {
            request: StorageRequest::ContinuationLocator,
            call: Some(call),
            ..
        } = fact
        {
            if let Some(peer) = self.locators.get(call) {
                self.evidence
                    .push(WakeFact::Submitted { peer: peer.clone() });
            }
        }
    }

    /// Notes a probe read's free locator permits.
    pub(super) fn permits(&mut self, free: usize) {
        self.permits = Some(free);
    }

    /// Opens the waiting peer's wake at `now` if the fixture held at this
    /// release, as the module doc lists the checks.
    fn released(&mut self, fixture: WakeFixture, now: SimInstant) {
        let Some(wait) = self.waiting.as_ref() else {
            return;
        };
        let claimed = usize::try_from(self.claimed.max(0)).unwrap_or(usize::MAX);
        let held = self.live.get(&wait.peer) == Some(&wait.session_id)
            && !self.started.contains(&wait.peer)
            && self.in_flight.is_empty()
            && wait.header_best == self.header_best
            && self.permits.is_some_and(|free| free > 0)
            && fixture
                .body_window
                .checked_sub(claimed)
                .is_some_and(|room| room >= 1);
        if held {
            let wait = self.waiting.take().expect("it was just read");
            self.evidence.push(WakeFact::Opened {
                peer: wait.peer,
                session_id: wait.session_id,
                at: now,
            });
        }
    }

    /// Notes that `peer`'s session `session_id` ended, which ends its wait,
    /// and keeps who ended it, which decides whether that answers its wake.
    fn ended(&mut self, peer: &ZakuraPeerId, session_id: u64) {
        if self
            .waiting
            .as_ref()
            .is_some_and(|wait| &wait.peer == peer && wait.session_id == session_id)
        {
            self.waiting = None;
        }
        self.evidence.push(WakeFact::Ended {
            peer: peer.clone(),
            session_id,
            by: self.sessions.ended_by(session_id),
        });
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use zakura_network::zakura::ZakuraPeerId;
    use zakura_scheduler::SimInstant;

    use super::{WakeEvidence, WakeFact};
    use crate::{
        domain::header_sync::{capacity_release_wakes, WakeView, CAPACITY_RELEASE_WAKES},
        peer::peer_id,
        property::{Assumptions, HasView, Registry, RegistryChecker, Source, Sources},
        storage::hex,
        ActionSource, Boundary, Identity, MemorySink, ObservationLog, PlanInput, Sim, Violation,
    };

    /// Wake evidence with no tap behind it.
    #[derive(Clone)]
    struct Wakes(WakeEvidence);

    impl Default for Wakes {
        /// An empty store in a handle of its own.
        fn default() -> Self {
            Wakes(WakeEvidence::new(
                &crate::stores::EvidenceStores::default(),
                None,
            ))
        }
    }

    impl WakeView for Wakes {
        fn wake(&self) -> &WakeEvidence {
            &self.0
        }
    }

    impl HasView for Wakes {
        type View = Wakes;

        /// A copy sharing the store.
        fn view(&self) -> Wakes {
            self.clone()
        }

        /// The store's stamps.
        fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
            Some(
                self.0
                    .facts()
                    .into_iter()
                    .map(|(stamp, _)| stamp)
                    .filter(|stamp| after.is_none_or(|after| *stamp > after))
                    .collect(),
            )
        }
    }

    /// Opens each wake in `opened` at the boundary after step 1, as a tap
    /// would.
    struct Opens {
        wakes: WakeEvidence,
        opened: Vec<WakeFact>,
    }

    impl ActionSource for Opens {
        fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

        fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

        fn execute(&mut self, _: u64, _: &mut ObservationLog) {
            unreachable!("the script offers no actions");
        }

        /// Pushes every wake in `opened` after step 1.
        fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
            if log.step() == 1 {
                for fact in self.opened.drain(..) {
                    self.wakes.push(fact);
                }
            }
            Ok(())
        }

        /// Nothing: it finishes no submitted work.
        fn recovery(&self, _now: SimInstant, _out: &mut crate::RecoveryOffer) {}

        /// Nothing: it has no holds.
        fn enter_drain(&mut self, _log: &mut ObservationLog) {}

        /// None: it holds no submitted work.
        fn holds_work(&self) -> Option<String> {
            None
        }
    }

    /// A wake for `peer`'s session `session_id`.
    fn opened(peer: &ZakuraPeerId, session_id: u64) -> WakeFact {
        WakeFact::Opened {
            peer: peer.clone(),
            session_id,
            at: SimInstant::ZERO,
        }
    }

    /// What `capacity-release-wakes` expects of the wake for `peer`'s
    /// session `session_id`.
    fn discharged(peer: &ZakuraPeerId, session_id: u64) -> String {
        format!(
            "Session(\"{}#{session_id}\") discharged before quiescence",
            hex(&peer.digest())
        )
    }

    /// C21: two wakes one boundary opened, for two peers' sessions, that
    /// nothing answers are two violations of `capacity-release-wakes`,
    /// which expire together at quiescence, and the report keeps both,
    /// each naming its own session.
    #[test]
    fn every_unanswered_wake_at_a_boundary_is_reported() {
        let (first, second) = (peer_id([7; 32], 0), peer_id([7; 32], 1));
        let wakes = Wakes::default();
        let mut sim = Sim::new(crate::RunConfig::default(), PlanInput::Bytes(Vec::new()));
        sim.add_source(Box::new(Opens {
            wakes: wakes.0.clone(),
            opened: vec![opened(&first, 3), opened(&second, 4)],
        }));
        sim.scheduler().spawn_root("reactor", async {
            zakura_rt::task::yield_now().await;
        });
        let registry =
            Registry::new(vec![capacity_release_wakes::<Wakes>()]).expect("one property");
        let mut checker = RegistryChecker::new(
            registry,
            Rc::new(wakes),
            Assumptions::none(),
            &Sources::observed(&[Source::Schedule, Source::View]),
        );
        let report = sim.run(&mut checker, &mut MemorySink::default());
        let found = |expected: String| Violation {
            property: CAPACITY_RELEASE_WAKES.to_owned(),
            step: 2,
            witness: vec![1, 2],
            expected,
            actual: "the scheduler is Quiescent after step 2 with the obligation from step 1 open"
                .to_owned(),
        };
        assert_eq!(
            report.properties[0].violations,
            [found(discharged(&first, 3)), found(discharged(&second, 4))]
        );
    }
}
