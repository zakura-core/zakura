//! What a test records of its own inputs and the machine's outputs.
//!
//! The tracker never decides what the machine should do. It keeps simple
//! state that follows from what the test sent and what came back: the open
//! sessions, the calls, reservations, requests and bodies in flight, what
//! each peer told the node, which adverts went out to it, and how long the
//! charges it earned surely cool it. Each check
//! compares two of those facts, or one of them with what the machine
//! reports.

use std::{collections::BTreeMap, time::Duration};

use indexmap::{IndexMap, IndexSet};
use zakura_chain::{block::Height, transaction::UnminedTxId};
use zakura_node_services::mempool::port::{
    Change, Charge, LocalFault, RejectionScope, ReservationId,
};
use zakura_rt::collections::HashSet;

use crate::zakura::tx_relay::machine::{
    CallId, CloseReason, Granted, PeerKey, RelayEffect, RelayMachine, SessionId, Unreserved,
    Verdict, WaiterId, BASE_COOLDOWN, MAX_COOLDOWN, TRIED_PER_ENTRY,
};

/// The input a step answers, with what the record needs of it.
pub(crate) enum Cause {
    /// An input the record needs nothing of.
    Other,
    /// The session's peer sent a `Hello`.
    Hello(SessionId),
    /// The session's peer announced these ids, as far as the machine heard.
    Inv(SessionId, Vec<UnminedTxId>),
    /// A body frame for the id arrived on the session, and was read or not.
    Arrived(SessionId, UnminedTxId, bool),
    /// The decoded body for the id, on the session.
    Body(SessionId, UnminedTxId),
    /// The session's peer answered `NotFound`.
    NotFound(SessionId, UnminedTxId),
    /// The session's peer asked for the id.
    GetTxs(SessionId, UnminedTxId),
    /// The test ended the session.
    End(SessionId),
    /// The pool answered the call.
    Answer(CallId, Result<Granted, Unreserved>),
    /// The pool's verdict on the submission.
    Verdict(WaiterId, Verdict),
    /// The pool published these changes, and is on or off.
    Pool(Vec<Change>, bool),
    /// The IO handed over the whole pool, which is on or off.
    Resync(Vec<UnminedTxId>, bool),
    /// The IO saw the core stop.
    Stopped,
}

/// An open session.
pub(crate) struct Open {
    /// Its peer, and whether the peer is unconditional.
    peer: PeerKey,
    exempt: bool,
    /// Whether its peer sent a `Hello`.
    pub(crate) hello: bool,
    /// Ids its peer announced or supplied, from the step after, until a
    /// removal or its own `NotFound` withdraws them.
    told: IndexSet<UnminedTxId>,
    /// Ids that went out to it in an `Inv` and still stand.
    pub(crate) sent: IndexSet<UnminedTxId>,
}

/// What a granted reservation is used for.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Use {
    /// Granted for the id, in the step being recorded.
    Granted(UnminedTxId),
    /// A request to the session went out under it.
    Asked(SessionId, UnminedTxId),
    /// Given back, failed or submitted.
    Ended,
}

/// A body at the pool, and who supplied it.
#[derive(Copy, Clone)]
pub(crate) struct Body {
    pub(crate) peer: PeerKey,
    exempt: bool,
    pub(crate) id: UnminedTxId,
}

/// One run's record.
#[derive(Default)]
pub(crate) struct Tracker {
    /// The sessions the machine opened and nothing has closed.
    pub(crate) open: BTreeMap<SessionId, Open>,
    /// The most peers with an open session at once.
    peak_peers: usize,
    /// The calls the pool has not answered, with their ids.
    pub(crate) calls: BTreeMap<CallId, UnminedTxId>,
    /// Every reservation the test granted, and what became of it.
    reservations: BTreeMap<ReservationId, Use>,
    /// The requests on the wire, with the reservation each went out under.
    pub(crate) requests: IndexMap<(SessionId, UnminedTxId), ReservationId>,
    /// The bodies awaiting their verdicts.
    pub(crate) bodies: BTreeMap<WaiterId, Body>,
    /// The body the step being recorded answers.
    answered: Option<Body>,
    /// The pool, as the machine was shown it.
    shown: HashSet<UnminedTxId>,
    /// Whether the pool is on, as the machine was last told.
    active: bool,
    /// Whether the machine was told the core stopped.
    stopped: bool,
    /// For each peer a verdict charged, when its cooldown surely ends and
    /// when it may end at the latest. See [`Tracker::charged`].
    cooling: BTreeMap<PeerKey, (Duration, Duration)>,
    /// How often each kind of effect, answer and change happened.
    pub(crate) seen: BTreeMap<String, usize>,
}

impl Tracker {
    /// A record of a machine whose pool holds `pooled`, on if `active`.
    pub(crate) fn new(active: bool, pooled: &[UnminedTxId]) -> Self {
        let shown = pooled.iter().copied().collect();
        Self {
            shown,
            active,
            ..Self::default()
        }
    }

    /// The machine opened `session` for `peer`. The peer's bodies at the
    /// pool count as told from the start.
    pub(crate) fn opened(&mut self, session: SessionId, peer: PeerKey, exempt: bool) {
        let told = self.unsettled(peer).collect();
        let (hello, sent) = (false, IndexSet::new());
        self.open.insert(
            session,
            Open {
                peer,
                exempt,
                hello,
                told,
                sent,
            },
        );
        let peers: IndexSet<PeerKey> = self.open.values().map(|open| open.peer).collect();
        self.peak_peers = self.peak_peers.max(peers.len());
    }

    /// `session`'s IO took `ids` into an `Inv`.
    pub(crate) fn sent(&mut self, session: SessionId, ids: &[UnminedTxId]) {
        let pooled = ids.iter().all(|id| self.shown.contains(id));
        assert!(pooled, "an Inv carries only pooled ids");
        if let Some(open) = self.open.get_mut(&session) {
            open.sent.extend(ids.iter().copied());
        }
        let kind = if ids.is_empty() {
            "empty inv"
        } else {
            "inv taken"
        };
        self.saw(kind);
    }

    /// Counts one occurrence of `kind`.
    pub(crate) fn saw(&mut self, kind: impl Into<String>) {
        *self.seen.entry(kind.into()).or_default() += 1;
    }

    /// A verdict charged `peer` with `charge` at `now`. A cooldown lasts from
    /// [`BASE_COOLDOWN`] up to [`MAX_COOLDOWN`], and a ban exactly
    /// [`MAX_COOLDOWN`]: no test fills the cooldown table and then bans. A
    /// cooldown charged while the last may still run adds no strike, so only
    /// one after the last surely ended starts another.
    fn charged(&mut self, peer: PeerKey, charge: Charge, now: Duration) {
        let (sure, latest) = self.cooling.get(&peer).copied().unwrap_or_default();
        let ends = match charge {
            Charge::None => return,
            Charge::Ban => (now + MAX_COOLDOWN, now + MAX_COOLDOWN),
            Charge::Cooldown if now >= latest => (now + BASE_COOLDOWN, now + MAX_COOLDOWN),
            Charge::Cooldown => (sure, latest.max(now + MAX_COOLDOWN)),
        };
        self.cooling.insert(peer, ends);
    }

    /// Whether `peer`'s cooldown surely still runs at `now`.
    fn cools(&self, peer: PeerKey, now: Duration) -> bool {
        self.cooling.get(&peer).is_some_and(|(sure, _)| now < *sure)
    }

    /// The ids of `peer`'s bodies awaiting their verdicts.
    fn unsettled(&self, peer: PeerKey) -> impl Iterator<Item = UnminedTxId> + '_ {
        let bodies = self.bodies.values();
        bodies
            .filter(move |body| body.peer == peer)
            .map(|body| body.id)
    }

    /// Records the step `cause` started at `now`, with the machine shown
    /// `tip`, checking each effect against the record so far.
    pub(crate) fn step(
        &mut self,
        cause: &Cause,
        effects: &[RelayEffect],
        now: Duration,
        tip: Height,
    ) {
        self.before(cause);
        if let Cause::Verdict(
            _,
            Verdict::Rejected {
                charge, tip: at, ..
            },
        ) = cause
        {
            let body = self.answered.expect("the step answers a body");
            // A cooldown counts only for a verdict decided at the tip, and a
            // full cooldown table closes the peer and cools nothing.
            let counts = *charge == Charge::Ban || *at == tip;
            let full = effects.iter().any(|effect| match effect {
                RelayEffect::Close { reason, .. } => *reason == CloseReason::CooldownsFull,
                _ => false,
            });
            if counts && !body.exempt && !full {
                self.charged(body.peer, *charge, now);
            }
        }
        let (mut submitted, mut answers) = (false, 0);
        for effect in effects {
            match effect {
                RelayEffect::Reserve { call, id } => {
                    assert!(self.active && !self.stopped, "reserved from an off pool");
                    self.calls.insert(*call, *id);
                    self.saw("reserve");
                }
                RelayEffect::Ask { session, id } => {
                    let Cause::Answer(_, Ok(granted)) = cause else {
                        panic!("a request goes out only in the step granting it");
                    };
                    let asked = Use::Asked(*session, *id);
                    let used = self.reservations.insert(granted.reservation, asked);
                    assert_eq!(used, Some(Use::Granted(*id)), "a request under its grant");
                    let hello = self.open.get(session).is_some_and(|open| open.hello);
                    let live = hello && self.active && !granted.cancelled;
                    assert!(live, "a request after the Hello, under a live grant");
                    let peer = self.open[session].peer;
                    assert!(!self.cools(peer, now), "a request to a peer that cools");
                    self.requests.insert((*session, *id), granted.reservation);
                    self.saw("ask");
                }
                RelayEffect::Submit {
                    waiter,
                    reservation,
                    body,
                } => {
                    let Cause::Body(session, id) = cause else {
                        panic!("a body is submitted only in the step it arrives");
                    };
                    assert!(body.id() == *id && !self.stopped, "the body that arrived");
                    let used = self.reservations.insert(*reservation, Use::Ended);
                    let asked = Some(Use::Asked(*session, *id));
                    assert_eq!(used, asked, "a body under its request's reservation");
                    let open = &self.open[session];
                    let (peer, exempt) = (open.peer, open.exempt);
                    let cools = self.cools(peer, now);
                    assert!(!cools, "a body of a peer that cools reaches the pool");
                    self.bodies.insert(
                        *waiter,
                        Body {
                            peer,
                            exempt,
                            id: *id,
                        },
                    );
                    submitted = true;
                    self.saw("submit");
                }
                RelayEffect::Release { reservation } | RelayEffect::FetchFailed { reservation } => {
                    let used = self.reservations.insert(*reservation, Use::Ended);
                    let live = matches!(used, Some(Use::Granted(_) | Use::Asked(..)));
                    assert!(live, "{reservation:?} ends once, after its grant: {used:?}");
                    let failed = matches!(effect, RelayEffect::FetchFailed { .. });
                    self.saw(if failed { "fetch failed" } else { "release" });
                }
                RelayEffect::Advert { session, id } => {
                    let told = self.open.get(session).map(|open| open.told.contains(id));
                    assert_eq!(told, Some(false), "an advert, never back to its source");
                    self.saw("advert");
                }
                RelayEffect::Serve { session, id } | RelayEffect::NotFound { session, id } => {
                    let asked = matches!(cause, Cause::GetTxs(by, of) if by == session && of == id);
                    assert!(asked, "an answer in its GetTxs' step");
                    let open = self.open.get_mut(session).expect("a live session");
                    answers += 1;
                    if matches!(effect, RelayEffect::Serve { .. }) {
                        let served = self.shown.contains(id) && open.sent.contains(id);
                        assert!(served, "served only once sent, while pooled");
                        self.saw("serve");
                    } else {
                        open.sent.shift_remove(id);
                        self.saw("answer not found");
                    }
                }
                RelayEffect::Close { session, reason } => {
                    self.check_close(cause, *session, *reason);
                    self.close(*session);
                    self.saw(format!("close {reason:?}"));
                }
            }
        }
        if let Cause::GetTxs(session, _) = cause {
            let live = self.open.get(session).is_some_and(|open| open.hello);
            assert_eq!(answers, usize::from(live), "a GetTxs is answered");
        }
        self.after(cause, submitted);
    }

    /// What `cause` itself tells the record, before its step's effects.
    fn before(&mut self, cause: &Cause) {
        match cause {
            Cause::Hello(session) => {
                if let Some(open) = self.open.get_mut(session) {
                    open.hello = true;
                }
            }
            Cause::Arrived(session, id, false)
            | Cause::Body(session, id)
            | Cause::NotFound(session, id) => {
                // A peer's own `NotFound` withdraws what it told the node, so
                // its step may announce the id to it.
                let open = self.open.get_mut(session);
                if let (Cause::NotFound(..), Some(open)) = (cause, open) {
                    open.told.shift_remove(id);
                }
                if self.requests.shift_remove(&(*session, *id)).is_some() {
                    let kind = match cause {
                        Cause::Arrived(..) => "body not read",
                        Cause::Body(..) => "body read",
                        _ => "not found",
                    };
                    self.saw(format!("request ended: {kind}"));
                }
            }
            Cause::End(session) => self.close(*session),
            Cause::Answer(call, answer) => {
                let id = self.calls.remove(call).expect("an answer to a call made");
                if let Ok(granted) = answer {
                    let used = self
                        .reservations
                        .insert(granted.reservation, Use::Granted(id));
                    assert!(used.is_none(), "the test grants each reservation once");
                }
                self.stopped |= *answer == Err(Unreserved::Stopped);
                let kind = match answer {
                    Ok(granted) if granted.cancelled => "cancelled grant".to_owned(),
                    Ok(_) => "grant".to_owned(),
                    Err(Unreserved::Rejected { .. }) => "Rejected".to_owned(),
                    Err(refused) => format!("{refused:?}"),
                };
                self.saw(format!("answer {kind}"));
            }
            Cause::Verdict(waiter, verdict) => {
                let body = self.bodies.remove(waiter).expect("a submitted body");
                self.answered = Some(body);
                let stop = Verdict::LocalFault(LocalFault::Stopped);
                self.stopped |= *verdict == stop;
                let kind = match verdict {
                    Verdict::Rejected { scope, charge, .. } => {
                        let uncached = *scope == RejectionScope::Uncached;
                        let scope = if uncached { "uncached" } else { "cached" };
                        format!("Rejected {scope}, {charge:?}")
                    }
                    Verdict::LocalFault(fault) if *fault != LocalFault::Stopped => {
                        "LocalFault".into()
                    }
                    other => format!("{other:?}"),
                };
                self.saw(format!("verdict {kind}"));
            }
            Cause::Pool(changes, active) => {
                if self.active != *active {
                    self.saw(if *active { "pool on" } else { "pool off" });
                }
                self.active = *active;
                for change in changes {
                    match change {
                        Change::Added(id) => {
                            self.shown.insert(*id);
                            self.saw("added");
                        }
                        // Reading a removal withdraws the adverts and the
                        // skips that came before it.
                        Change::Removed { id, reason } => {
                            self.shown.remove(id);
                            for open in self.open.values_mut() {
                                open.told.shift_remove(id);
                                open.sent.shift_remove(id);
                            }
                            self.saw(format!("removed {reason:?}"));
                        }
                        Change::Refused(_) => self.saw("refused"),
                    }
                }
            }
            Cause::Resync(ids, active) => {
                self.active = *active;
                self.shown = ids.iter().copied().collect();
                let shown = &self.shown;
                for open in self.open.values_mut() {
                    open.sent.retain(|id| shown.contains(id));
                }
                self.saw("resync");
            }
            Cause::Stopped => self.stopped = true,
            _ => {}
        }
    }

    /// What the peer told the node in `cause`, which counts from after its
    /// step.
    fn after(&mut self, cause: &Cause, submitted: bool) {
        let (session, told): (SessionId, Vec<UnminedTxId>) = match cause {
            Cause::Inv(session, ids) => (*session, ids.clone()),
            Cause::Body(session, id) if submitted => (*session, vec![*id]),
            Cause::Verdict(..) => {
                // The supplier's sessions mark the body known again.
                let body = self.answered.take().expect("taken in its step");
                for open in self.open.values_mut().filter(|open| open.peer == body.peer) {
                    open.told.insert(body.id);
                }
                return;
            }
            _ => return,
        };
        if let Some(open) = self.open.get_mut(&session) {
            open.told.extend(told);
        }
    }

    /// Checks that `session` may be closed for `reason` in `cause`'s step.
    fn check_close(&self, cause: &Cause, session: SessionId, reason: CloseReason) {
        let open = self.open.get(&session).expect("a close of a live session");
        let sender = match cause {
            Cause::Hello(sender) | Cause::Arrived(sender, ..) | Cause::Body(sender, _) => {
                Some(sender)
            }
            Cause::NotFound(sender, _) | Cause::GetTxs(sender, _) => Some(sender),
            _ => None,
        };
        match reason {
            CloseReason::Violation(_) => assert_eq!(sender, Some(&session), "a violation's sender"),
            CloseReason::Unresponsive => {
                let asked = self.requests.keys().any(|(asked, _)| *asked == session);
                assert!(asked, "unresponsive with a request out");
            }
            CloseReason::Ban | CloseReason::RepeatedCooldown | CloseReason::CooldownsFull => {
                let Cause::Verdict(_, Verdict::Rejected { charge, .. }) = cause else {
                    panic!("a charge comes with a rejection");
                };
                let body = self.answered.expect("the step answers a body");
                let supplier = body.peer == open.peer && !body.exempt;
                assert!(supplier, "a charge closes only its supplier, never exempt");
                let ban = reason == CloseReason::Ban;
                let earned = if ban { Charge::Ban } else { Charge::Cooldown };
                assert_eq!(*charge, earned, "{reason:?} is that charge's close");
            }
        }
    }

    /// `session` ended. Its requests end with it, and its peer's other
    /// sessions hear of the peer's bodies at the pool.
    fn close(&mut self, session: SessionId) {
        let Some(closed) = self.open.remove(&session) else {
            return;
        };
        let before = self.requests.len();
        self.requests.retain(|(asked, _), _| *asked != session);
        for _ in self.requests.len()..before {
            self.saw("request ended: session ended");
        }
        let peer = closed.peer;
        let unsettled: Vec<UnminedTxId> = self.unsettled(peer).collect();
        for open in self.open.values_mut().filter(|open| open.peer == peer) {
            open.told.extend(unsettled.iter().copied());
        }
    }

    /// Compares the record with what the machine holds.
    pub(crate) fn check(&self, machine: &RelayMachine) {
        let live = machine.session_ids().eq(self.open.keys().copied());
        assert!(live, "the machine's sessions are the open ones");
        let made = |call: &CallId| machine.calls.contains_key(call);
        let owed = machine.calls.len() == self.calls.len() && self.calls.keys().all(made);
        assert!(owed, "the machine waits for the calls the pool owes");
        assert_eq!(machine.waiters.len(), self.bodies.len(), "and the verdicts");
        let (mut granted, mut asked) = (0, 0);
        for used in self.reservations.values() {
            granted += usize::from(matches!(used, Use::Granted(_)));
            asked += usize::from(matches!(used, Use::Asked(..)));
        }
        assert_eq!(granted, 0, "a grant is used or given back in its step");
        let held = machine.reservations_held();
        assert_eq!(held, asked, "requests hold the reservations");
        // An entry's tries are at most one more than the cap or the most
        // peers connected at once.
        let tries = machine.reactor.tries().values().map(IndexSet::len).max();
        let bound = TRIED_PER_ENTRY.max(self.peak_peers) + 1;
        assert!(tries.unwrap_or(0) <= bound, "the tries bound");
    }

    /// Nothing is outstanding: every call answered, every request ended,
    /// every body decided, and every reservation ended.
    pub(crate) fn assert_drained(&self) {
        let asked = self.calls.len() + self.requests.len();
        assert_eq!(asked, 0, "no call is unanswered, no request on the wire");
        assert!(self.bodies.is_empty(), "no body awaits its verdict");
        let ended = self.reservations.values().all(|used| *used == Use::Ended);
        assert!(ended, "every reservation ended");
    }
}
