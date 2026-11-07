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
//! locator reaching storage ([`WakeFact::Submitted`]) or its session ending
//! ([`WakeFact::Ended`]) answers the wake, and a deferred cached pass
//! ([`WakeFact::Deferred`]) moves its deadline to the pass's refill.
//!
//! Each fact gets a stamp, larger than every earlier one, which the
//! storage view hands the registry as its view-side facts.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

use serde::{Deserialize, Serialize};
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    observe::{PortCall, ReactorEvent},
    ZakuraPeerId,
};
use zakura_scheduler::SimInstant;

use crate::observe::{StorageFact, StorageRequest};

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
    /// Each fact with its stamp, in stamp order.
    facts: Vec<(u64, WakeFact)>,
}

/// One reactor's wake facts, shared by its tap and whoever reads them.
/// Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct WakeEvidence(Arc<Mutex<Store>>);

impl WakeEvidence {
    /// Evidence for a run that declares `fixture`, or none.
    pub fn new(fixture: Option<WakeFixture>) -> WakeEvidence {
        WakeEvidence(Arc::new(Mutex::new(Store {
            fixture,
            facts: Vec::new(),
        })))
    }

    /// Runs `read` on the store. Nothing else is called while the lock is
    /// held.
    fn with<R>(&self, read: impl FnOnce(&mut Store) -> R) -> R {
        read(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// The declared fixture, if any.
    pub fn fixture(&self) -> Option<WakeFixture> {
        self.with(|store| store.fixture)
    }

    /// Keeps `fact` with the next stamp.
    fn push(&self, fact: WakeFact) {
        self.with(|store| {
            let stamp = store.facts.last().map_or(1, |(stamp, _)| stamp + 1);
            store.facts.push((stamp, fact));
        });
    }

    /// The stamps after `after`, or all of them, in order.
    pub fn stamps_after(&self, after: Option<u64>) -> Vec<u64> {
        self.with(|store| {
            store
                .facts
                .iter()
                .map(|(stamp, _)| *stamp)
                .filter(|stamp| after.is_none_or(|after| *stamp > after))
                .collect()
        })
    }

    /// The fact with `stamp`, if any.
    pub fn fact(&self, stamp: u64) -> Option<WakeFact> {
        self.with(|store| {
            store
                .facts
                .iter()
                .find(|(at, _)| *at == stamp)
                .map(|(_, fact)| fact.clone())
        })
    }

    /// Every fact so far, with its stamp.
    pub fn facts(&self) -> Vec<(u64, WakeFact)> {
        self.with(|store| store.facts.clone())
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
    /// A check that keeps its facts in `evidence`.
    pub(super) fn new(evidence: WakeEvidence) -> WakeCheck {
        WakeCheck {
            evidence,
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
                    self.released(fixture);
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

    /// Opens the waiting peer's wake if the fixture held at this release,
    /// as the module doc lists the checks.
    fn released(&mut self, fixture: WakeFixture) {
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
            });
        }
    }

    /// Notes that `peer`'s session `session_id` ended, which ends its wait
    /// and answers its wake.
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
        });
    }
}
