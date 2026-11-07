//! `header-sync/stale-completion-inert`'s check, on the reactor's facts as
//! its tap receives them.
//!
//! Every fact the reactor's own task reports belongs to the unit of work
//! its last `CauseSet` named. A fact from another task belongs to none.
//!
//! A prepare or apply call is stale when its round retired, or its owner's
//! session ended or was replaced, before its `CallEnded`. A call whose
//! owner is a body repair's is stale only by its retirement, since the
//! reactor keeps a repair round when its peer departs. The call's own
//! `Dispatched` names that owner. A call that ended
//! `Dropped` runs no handler, so it's never stale.
//!
//! A stale call's handler, the facts under its `Completion` cause, may only
//! return its own round's budget. A charge, a return that doesn't join its
//! round, any credit, a retirement or release of another round, a start, a
//! reservation, or a prepare or apply dispatch breaks
//! [`Claim::StaleCompletionEffect`]. A return joins a round through its
//! reservation: a continuation's `ReservationOpened` names its round, a
//! first or repair reservation's `ReservationBound` does once its round
//! starts, and a staged lease names its round itself. Facts under any other
//! cause, such as the refresh right after the handler, aren't judged here.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

use zakura_header_chain::HeaderSyncWorkOwner;
use zakura_network::zakura::{
    observe::{BudgetOwner, CallEnd, PortCall, ReactorCause, ReactorEvent},
    ZakuraPeerId,
};

/// A round, by its peer and incarnation.
type Round = (ZakuraPeerId, u64);

/// Why a call went stale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Staleness {
    /// Its round retired.
    RoundRetired,
    /// Its owner's session ended.
    SessionEnded,
    /// Its owner's session was replaced.
    SessionReplaced,
}

/// A prepare or apply call that ended stale, and its handler's run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleWindow {
    /// The call's number.
    pub call: u64,
    /// Whether it was an apply, not a prepare.
    pub apply: bool,
    /// Why it was stale at its end.
    pub why: Staleness,
    /// The step whose drain found its end, once one did.
    pub step: Option<u64>,
    /// Whether its handler finished.
    pub handled: bool,
}

/// The stale calls one reactor's tap found, shared by the tap and whoever
/// reads them. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct StaleEvidence(Arc<Mutex<Vec<StaleWindow>>>);

impl StaleEvidence {
    /// Runs `change` on the windows. Nothing else is called while the lock
    /// is held.
    fn with<R>(&self, change: impl FnOnce(&mut Vec<StaleWindow>) -> R) -> R {
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Every stale call so far, in the order they ended.
    pub fn windows(&self) -> Vec<StaleWindow> {
        self.with(|windows| windows.clone())
    }

    /// Whether a drain up to and including `step`'s found a stale call.
    pub fn found_by(&self, step: u64) -> bool {
        self.with(|windows| {
            windows
                .iter()
                .any(|window| window.step.is_some_and(|found| found <= step))
        })
    }

    /// Whether any drain found a stale call.
    pub fn any(&self) -> bool {
        self.with(|windows| windows.iter().any(|window| window.step.is_some()))
    }
}

/// A prepare or apply call the check follows.
struct Call {
    apply: bool,
    peer: ZakuraPeerId,
    /// Its owner's session.
    session: u64,
    /// Whether its owner is a body repair's.
    repair: bool,
    /// The round whose headers it holds, once it holds them.
    round: Option<Round>,
    /// Why it's stale, once it ended stale.
    stale: Option<Staleness>,
}

/// The check's state: what the facts so far say about calls, rounds,
/// sessions and reservations, and the breaks not yet drained.
pub(super) struct StaleCheck {
    /// The unit of work the reactor's last `CauseSet` named.
    cause: ReactorCause,
    calls: HashMap<u64, Call>,
    retired: HashSet<Round>,
    ended: HashMap<(ZakuraPeerId, u64), Staleness>,
    /// Each reservation's round, once a fact names it.
    bound: HashMap<u64, Round>,
    breaks: Vec<String>,
    evidence: StaleEvidence,
}

impl StaleCheck {
    /// A check that keeps its stale calls in `evidence`.
    pub(super) fn new(evidence: StaleEvidence) -> StaleCheck {
        StaleCheck {
            cause: ReactorCause::Idle,
            calls: HashMap::new(),
            retired: HashSet::new(),
            ended: HashMap::new(),
            bound: HashMap::new(),
            breaks: Vec::new(),
            evidence,
        }
    }

    /// Judges `event`, which the reactor's own task reported when
    /// `on_reactor`, then notes what it says.
    pub(super) fn observe(&mut self, event: &ReactorEvent, on_reactor: bool) {
        if !on_reactor {
            return;
        }
        if let ReactorEvent::CauseSet { cause } = event {
            self.cause = *cause;
            return;
        }
        if let ReactorCause::Completion { call } = self.cause {
            self.judge(call, event);
        }
        self.note(event);
    }

    /// The breaks found since the last call, and the step their drain is.
    /// Stale calls found since then are stamped with `step`.
    pub(super) fn drain(&mut self, step: u64) -> Vec<String> {
        self.evidence.with(|windows| {
            for window in windows.iter_mut().filter(|window| window.step.is_none()) {
                window.step = Some(step);
            }
        });
        std::mem::take(&mut self.breaks)
    }

    /// Records a break if `event`, under call `call`'s handler, is an
    /// effect a stale handler must not have.
    fn judge(&mut self, call: u64, event: &ReactorEvent) {
        let Some(stale) = self.calls.get(&call).filter(|stale| stale.stale.is_some()) else {
            return;
        };
        let own = stale.round.as_ref();
        let other = |round: &Round| own != Some(round);
        let effect = match event {
            ReactorEvent::BudgetChanged {
                owner,
                reserved_delta,
                owned_delta,
            } => {
                if *reserved_delta > 0 || *owned_delta > 0 {
                    Some(format!("charged the budget for {owner:?}"))
                } else if (*reserved_delta < 0 || *owned_delta < 0)
                    && self.round_of(owner.as_ref()).as_ref() != own
                {
                    Some(format!("returned budget for {owner:?}, not its round's"))
                } else {
                    None
                }
            }
            ReactorEvent::CreditIssued {
                kind,
                peer,
                subscription,
                ..
            } => Some(format!(
                "gave {peer:?} {kind:?} credit on subscription {subscription}"
            )),
            ReactorEvent::Retired { request, .. }
                if other(&(request.peer.clone(), request.incarnation)) =>
            {
                Some(format!("retired round {}", request.incarnation))
            }
            ReactorEvent::RoundReleased {
                peer, incarnation, ..
            } if other(&(peer.clone(), *incarnation)) => {
                Some(format!("released round {incarnation}"))
            }
            ReactorEvent::Started { request, .. } => {
                Some(format!("started round {}", request.incarnation))
            }
            ReactorEvent::ReservationOpened { reservation, .. } => {
                Some(format!("opened reservation {reservation}"))
            }
            ReactorEvent::Dispatched {
                call: dispatched,
                op: PortCall::Prepare { .. } | PortCall::Apply { .. },
            } => Some(format!("dispatched call {dispatched}")),
            _ => None,
        };
        if let Some(effect) = effect {
            let kind = if stale.apply { "apply" } else { "prepare" };
            self.breaks.push(format!(
                "StaleCompletionEffect: stale {kind} call {call}'s handler {effect}"
            ));
        }
    }

    /// The round a budget change's `owner` joins, if a fact named it.
    fn round_of(&self, owner: Option<&BudgetOwner>) -> Option<Round> {
        match owner? {
            BudgetOwner::Reservation(reservation) => self.bound.get(reservation).cloned(),
            BudgetOwner::Lease {
                round: Some(round), ..
            } => Some(round.clone()),
            BudgetOwner::Lease {
                reservation,
                round: None,
            } => self.bound.get(reservation).cloned(),
        }
    }

    /// Notes what `event` says about calls, rounds, sessions and
    /// reservations.
    fn note(&mut self, event: &ReactorEvent) {
        match event {
            ReactorEvent::Dispatched { call, op } => {
                let (apply, peer, owner) = match op {
                    PortCall::Prepare { peer, owner, .. } => (false, peer, owner),
                    PortCall::Apply { peer, owner, .. } => (true, peer, owner),
                    _ => return,
                };
                self.calls.insert(
                    *call,
                    Call {
                        apply,
                        peer: peer.clone(),
                        session: owner.session_id(),
                        repair: matches!(owner, HeaderSyncWorkOwner::BodyRepair(_)),
                        round: None,
                        stale: None,
                    },
                );
            }
            ReactorEvent::HeadersHeld {
                call,
                peer,
                incarnation,
                ..
            } => {
                if let Some(held) = self.calls.get_mut(call) {
                    held.round = Some((peer.clone(), *incarnation));
                }
            }
            ReactorEvent::CallEnded { call, end } if *end != CallEnd::Dropped => {
                self.ended_call(*call)
            }
            ReactorEvent::Handled { call, .. } => self.evidence.with(|windows| {
                if let Some(window) = windows.iter_mut().rev().find(|window| window.call == *call) {
                    window.handled = true;
                }
            }),
            ReactorEvent::Retired { request, .. } => {
                self.retired
                    .insert((request.peer.clone(), request.incarnation));
            }
            ReactorEvent::SessionEnded {
                peer, session_id, ..
            } => {
                self.ended
                    .insert((peer.clone(), *session_id), Staleness::SessionEnded);
            }
            ReactorEvent::SessionAdmitted {
                peer,
                replaced: Some(replaced),
                ..
            } => {
                self.ended
                    .insert((peer.clone(), *replaced), Staleness::SessionReplaced);
            }
            ReactorEvent::ReservationOpened {
                reservation,
                request: Some(request),
                ..
            }
            | ReactorEvent::ReservationBound {
                reservation,
                request,
                ..
            } => {
                self.bound
                    .insert(*reservation, (request.peer.clone(), request.incarnation));
            }
            _ => {}
        }
    }

    /// Decides whether call `call`, which just ended with a handler to run,
    /// is stale, and keeps its window if so.
    fn ended_call(&mut self, call: u64) {
        let Some(ended) = self.calls.get_mut(&call) else {
            return;
        };
        let Some(round) = ended.round.as_ref() else {
            return;
        };
        ended.stale = if self.retired.contains(round) {
            Some(Staleness::RoundRetired)
        } else if ended.repair {
            None
        } else {
            self.ended
                .get(&(ended.peer.clone(), ended.session))
                .copied()
        };
        if let Some(why) = ended.stale {
            let apply = ended.apply;
            self.evidence.with(|windows| {
                windows.push(StaleWindow {
                    call,
                    apply,
                    why,
                    step: None,
                    handled: false,
                })
            });
        }
    }
}

#[cfg(test)]
mod tests;
