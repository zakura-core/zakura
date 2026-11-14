//! A check of each storage call's identity, retained-header ownership and
//! header capacity accounting.
//!
//! [`HeaderWorkLedger`] folds one reactor's events and refuses a history that
//! attributes a call or its headers to the wrong work. It checks events against
//! direct facts, the call's own `Dispatched` and the peer's latest `Started`,
//! and infers nothing from ordering. The reactor's tests and the simulator's tap
//! share it.
//!
//! It also keeps the work queue's capacity accounting, from its reservations,
//! staged pages, lease returns and round releases:
//!
//! - A live reservation satisfies opened = staged + remaining, and an ended
//!   one opened = staged + released.
//! - A peer holds at most one live reservation. No reservation takes more
//!   than the budget's unclaimed capacity or its round's request limit, and
//!   an ordinary one takes at most the fair share.
//! - Each round starts on the reservation bound to it, opened for its
//!   purpose. A binding, a
//!   continuation and every staged page name exactly the request that
//!   started the round, and a page arrives on that request's session and
//!   subscription.
//! - Owned headers are staged minus returned. The calls holding a round's
//!   headers hold at most what it owns, for the owner that started it or that
//!   owner narrowed to its staged tip, and a prepare names the staged tip
//!   and sends exactly the round's staged headers, in order. A page is in
//!   its round, as those headers, when its staging reports. A call holds only
//!   leases staged for its own round. A lease never returns while a
//!   call holds its round.
//! - When the queue removes a round, its peer holds no reservation, the
//!   queue holds none of its leases, and every lease no call holds has
//!   returned. A call's leases of a released round return by the time its
//!   completion is handled. A round retires only once the queue released
//!   it. Every prepare and apply holds its round's headers. At teardown nothing stays reserved or
//!   owned.
//!
//! Most rules say the evidence is inconsistent, and [`HeaderWorkLedger::observe`]
//! refuses the event. A [`ClaimRule`] is one a production bug alone can
//! break and a property's claim names. The ledger records its break, still
//! applies the event, so later valid events don't look unknown, and goes on.
//! A round retired while the queue still holds it is a claim rule only when
//! a new snapshot made it obsolete.
//!
//! A claim that breaks may leave the ledger's accounting for an entity
//! unlike production's, so later checks of it would fail as integrity
//! rules, hiding the claim. Such a claim taints every entity it
//! implicates, in what it made unreliable:
//!
//! - `OverReservation`: its reservation's remainder.
//! - `UnstagedPage`: its round's staged count and headers.
//! - `ForeignLease`: the held round's and each lease's round's counts and
//!   holds.
//! - `UnstagedHeaders` and `HeldUnowned`: the held round's counts and
//!   holds, since its calls return leases it never counted staged.
//! - `WrongCall`: the held round's holds. `WrongRound`: the holds of the
//!   round the call held and of the one it should have. `BadHandoff`: the
//!   holds of the round its two calls hold in turn.
//!
//! A call's owner, payload or source being wrong leaves the accounting as
//! production keeps it, so `WrongOwner`, `WrongPayload` and `WrongSource`
//! taint nothing.
//!
//! A check that reads a tainted part doesn't judge, and never counts as
//! passed: a tainted round never settles, so the teardown property stays
//! inconclusive and doesn't count it stuck, and
//! [`HeaderWorkLedger::accounting_tainted`] stops the tap comparing the
//! ledger's totals with the probe's. Checks that don't read a tainted
//! part still judge: a round's queue-held leases, its source and first
//! ancestor, a call's owner up to the tip a bounded prepare narrows to, a
//! payload as long as the call holds, and a lease returning while a call
//! holds a round whose counts alone are tainted.

use std::collections::{BTreeMap, BTreeSet};

use zakura_chain::block;
use zakura_header_chain::{Frontier, HeaderSyncWorkOwner, SourceId};
use zakura_rt::collections::HashMap;

use super::{
    super::scheduler::peer_work::{
        HEADER_CHUNK_BUDGET_CAPACITY_V1, MAX_HEADER_CHUNK_RESERVATION_V1,
    },
    super::HeaderTargetPurpose,
    CallEnd, HeaderRequestTerminal, HeaderWorkEnd, PortCall, ReactorEvent, RequestRef,
    ReservationEnd, ReservationPurpose, StorageCallKind,
};
use crate::zakura::ZakuraPeerId;

/// The ownership one call holds.
#[derive(Debug)]
struct Held {
    incarnation: u64,
    headers: usize,
    ended: bool,
}

/// What the ledger knows about one dispatched call.
#[derive(Debug)]
struct CallRecord {
    op: PortCall,
    end: Option<CallEnd>,
    held: Option<Held>,
    /// Whether its completion was handled.
    handled: bool,
}

impl CallRecord {
    /// Whether this is a prepare or an apply that never held headers, which
    /// each takes from its round as it's dispatched.
    fn holds_nothing(&self) -> bool {
        matches!(self.op, PortCall::Prepare { .. } | PortCall::Apply { .. }) && self.held.is_none()
    }
}

/// A rule that a production bug alone can break and a property's claim
/// names. See the module doc.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimRule {
    /// A reported reservation takes the budget's claims past its capacity.
    OverBudget,
    /// The queue released a round with its peer still holding a
    /// reservation.
    LeakedReservation,
    /// A released round kept a lease: the queue kept one when it released
    /// the round, or one no call holds hadn't returned when the round was
    /// released or a call holding it was handled, or by teardown.
    LeakedLease,
    /// A call was handled with its round's headers still held, or still
    /// held them at teardown.
    OpenOwnership,
    /// A reservation never ended by teardown.
    OpenReservation,
    /// A round a new snapshot made obsolete retired while the queue still
    /// held it.
    UnreleasedObsoleteRound,
    /// A round retired while the queue still held it, for a reason other
    /// than a new snapshot, its peer's departure or a timeout.
    UnreleasedRound,
    /// A round retired for its session's departure or its request's timeout
    /// while the queue still held it.
    UnreleasedDepartedRound,
    /// A page was staged on a round another session or subscription
    /// requested.
    ForeignPage,
    /// A staged page left its round holding other headers than its earlier
    /// pages and then its own, in order, or a count that isn't what it
    /// staged.
    UnstagedPage,
    /// A page spent more headers than its reservation held, or reported
    /// another count left.
    OverReservation,
    /// A call held a round's headers for another owner than the one its
    /// pages were staged for.
    WrongOwner,
    /// A prepare sent other headers than its round's staged ones, in
    /// order, or toward another tip.
    WrongPayload,
    /// A prepare handed its headers to an apply for other work.
    BadHandoff,
    /// A call held, or ended, another round than its peer's current one or
    /// the one it held.
    WrongRound,
    /// A call that isn't the prepare or apply it names held headers.
    WrongCall,
    /// A call held headers with a lease staged for another round.
    ForeignLease,
    /// A round's calls held more headers than it owns.
    HeldUnowned,
    /// A call held a round that staged no page.
    UnstagedHeaders,
    /// A prepare supplied its round from another source than its peer's.
    WrongSource,
}

/// One break of a [`ClaimRule`], with its detail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimBreak {
    /// The rule.
    pub rule: ClaimRule,
    /// What broke it, led by the rule's name.
    pub detail: String,
}

/// Folds one reactor's events. See the module doc.
#[derive(Debug, Default)]
pub struct HeaderWorkLedger {
    calls: BTreeMap<u64, CallRecord>,
    last_incarnation: u64,
    /// Each peer's latest started, unretired round.
    rounds: HashMap<ZakuraPeerId, u64>,
    /// The handoff whose apply has not yet taken its headers. The reactor
    /// hands headers over in one step, so the next `HeadersHeld` resolves it.
    handoff: Option<Handoff>,
    /// The last reservation number seen, 0 before any.
    last_reservation: u64,
    /// Each live reservation, by number.
    reservations: BTreeMap<u64, Reservation>,
    /// The round a `ReservationBound` named, whose `Started` comes next.
    binding: Option<Binding>,
    /// Each started round, by incarnation, until the queue released it and
    /// its last lease returned.
    started: BTreeMap<u64, Round>,
    /// The claim breaks not yet taken, in order.
    claims: Vec<ClaimBreak>,
    /// Every round the queue has released, by incarnation.
    released: BTreeSet<u64>,
    /// Each live reservation whose remainder a claim made unreliable. See
    /// the module doc.
    tainted_reservations: BTreeSet<u64>,
    /// Each started round whose accounting a claim made unreliable, with
    /// what it made unreliable. See the module doc.
    tainted_rounds: BTreeMap<u64, RoundTaint>,
}

/// What a claim made unreliable about a round. See the module doc.
#[derive(Clone, Copy, Debug, Default)]
struct RoundTaint {
    /// Its staged count and headers.
    counts: bool,
    /// Which calls hold it, and how much.
    holds: bool,
}

/// One live reservation.
#[derive(Debug)]
struct Reservation {
    peer: ZakuraPeerId,
    purpose: ReservationPurpose,
    opened: usize,
    staged: usize,
    /// The round it backs, once it names or is bound to one.
    round: Option<u64>,
}

/// A reservation bound to a round that hasn't started yet.
#[derive(Debug)]
struct Binding {
    reservation: u64,
    request: RequestRef,
    max_headers: usize,
}

/// One started round's identity and accounting.
#[derive(Debug)]
struct Round {
    /// The request that started it.
    request: RequestRef,
    /// The most headers one of its requests asks for.
    max_headers: usize,
    staged: usize,
    returned: usize,
    /// Its staged headers' hashes, in order.
    hashes: Vec<block::Hash>,
    /// The ancestor its first page declared.
    ancestor: Option<Frontier>,
    /// Whether the queue released it.
    released: bool,
}

/// One staged page's headers, as `PageStaged` reports them.
struct StagedHeaders<'a> {
    /// How many headers the page carried.
    headers: usize,
    /// What its reservation kept.
    reserved_left: usize,
    /// The page's header hashes.
    hashes: &'a [block::Hash],
    /// The ancestor the page declared.
    ancestor: Frontier,
    /// Every hash the round stored, the page's and its earlier pages'.
    stored: &'a [block::Hash],
    /// How many headers the round held after the page.
    staged_total: usize,
}

/// What a prepare sends: the target it names, the hashes of the headers it
/// actually sends, the ancestor they extend and their supplier.
#[derive(Clone, Debug)]
struct Payload {
    target: block::Hash,
    hashes: Vec<block::Hash>,
    common_ancestor: Frontier,
    source: SourceId,
}

/// A prepare's headers on their way to an apply call.
#[derive(Debug)]
struct Handoff {
    prepare: u64,
    apply: u64,
    incarnation: u64,
    headers: usize,
}

impl HeaderWorkLedger {
    /// Checks `event`, and returns the broken rule's name and detail. A
    /// [`ClaimRule`]'s break is kept for [`take_claims`](Self::take_claims)
    /// instead.
    pub fn observe(&mut self, event: &ReactorEvent) -> Result<(), String> {
        match event {
            ReactorEvent::Dispatched { call, op } => {
                let record = CallRecord {
                    op: op.clone(),
                    end: None,
                    held: None,
                    handled: false,
                };
                if self.calls.insert(*call, record).is_some() {
                    return Err(format!("CallReused: call {call} dispatched twice"));
                }
            }
            ReactorEvent::CallEnded { call, end } => {
                let record = self.record_mut(*call)?;
                if record.end.is_some() {
                    return Err(format!("CallEndedTwice: call {call} ended again"));
                }
                record.end = Some(*end);
                // A teardown drop reports before the call's ownership ends, so
                // `finish` checks a dropped call instead.
                if *end != CallEnd::Dropped && record.holds_nothing() {
                    return Err(format!(
                        "UnheldCall: call {call} ended without ever holding its round's headers"
                    ));
                }
            }
            ReactorEvent::Handled { call, .. } => {
                let record = self.record_mut(*call)?;
                match record.end {
                    None => {
                        return Err(format!(
                            "MissingCallEnd: call {call} was handled before it ended"
                        ))
                    }
                    Some(CallEnd::Dropped) => {
                        return Err(format!(
                            "MissingCallEnd: call {call} was handled after the reactor dropped it"
                        ))
                    }
                    Some(_) => {}
                }
                record.handled = true;
                let open = record.held.as_ref().is_some_and(|held| !held.ended);
                let held_round = record.held.as_ref().map(|held| held.incarnation);
                if open {
                    self.claims.push(ClaimBreak {
                        rule: ClaimRule::OpenOwnership,
                        detail: format!(
                            "OpenOwnership: call {call} was handled with its headers still held"
                        ),
                    });
                }
                if let Some(handoff) = self.handoff.as_ref().filter(|h| h.apply == *call) {
                    return Err(format!(
                        "UnresolvedHandoff: call {call} was handled before it took the headers \
                         call {} handed it",
                        handoff.prepare
                    ));
                }
                if let Some(incarnation) = held_round {
                    self.check_returned(incarnation, &format!("call {call} was handled"));
                }
            }
            ReactorEvent::Started {
                request, purpose, ..
            } => self.start(request, purpose)?,
            ReactorEvent::Retired {
                request, reason, ..
            } => {
                if self.rounds.get(&request.peer) == Some(&request.incarnation) {
                    self.rounds.remove(&request.peer);
                }
                if self
                    .started
                    .get(&request.incarnation)
                    .is_some_and(|round| !round.released)
                {
                    // An obsolete round's retirement is the reset property's
                    // claim, and a departed or timed-out one's the retirement
                    // property's. Every other reason, a shutdown included, is
                    // the outstanding-work claim.
                    let claim = match reason {
                        HeaderRequestTerminal::SnapshotObsolete => ClaimBreak {
                            rule: ClaimRule::UnreleasedObsoleteRound,
                            detail: format!(
                                "UnreleasedObsoleteRound: obsolete round {} retired while the \
                                 queue still holds it",
                                request.incarnation
                            ),
                        },
                        HeaderRequestTerminal::Disconnected
                        | HeaderRequestTerminal::SessionReplaced
                        | HeaderRequestTerminal::TimedOut => ClaimBreak {
                            rule: ClaimRule::UnreleasedDepartedRound,
                            detail: format!(
                                "UnreleasedDepartedRound: round {} retired {} while the queue \
                                 still holds it",
                                request.incarnation,
                                reason.label()
                            ),
                        },
                        _ => ClaimBreak {
                            rule: ClaimRule::UnreleasedRound,
                            detail: format!(
                                "UnreleasedRound: round {} retired as {reason:?} while the queue \
                                 still holds it",
                                request.incarnation
                            ),
                        },
                    };
                    self.claims.push(claim);
                }
            }
            ReactorEvent::ReservationOpened {
                reservation,
                peer,
                purpose,
                headers,
                request,
            } => self.open(*reservation, peer, *purpose, *headers, request.as_ref())?,
            ReactorEvent::ReservationBound {
                reservation,
                request,
                max_headers,
            } => self.bind(*reservation, request, *max_headers)?,
            ReactorEvent::ReservationEnded {
                reservation,
                released,
                cause,
            } => {
                let tainted = self.tainted_reservations.remove(reservation);
                let live = self.reservation_mut(*reservation)?;
                let unstaged = live.opened.saturating_sub(live.staged);
                if !tainted
                    && (*released != unstaged
                        || (*cause == ReservationEnd::Staged && *released != 0))
                {
                    return Err(format!(
                        "ReleaseMismatch: reservation {reservation} ended as {cause:?} \
                         releasing {released} of the {unstaged} headers it still held"
                    ));
                }
                self.reservations.remove(reservation);
            }
            ReactorEvent::PageStaged {
                from_peer,
                from_session,
                request,
                reservation,
                subscription,
                headers,
                reserved_left,
                ancestor,
                hashes,
                stored,
                staged_total,
                ..
            } => {
                if from_peer != &request.peer
                    || *from_session != request.session_id
                    || *subscription != request.request_id
                {
                    self.claims.push(ClaimBreak {
                        rule: ClaimRule::ForeignPage,
                        detail: format!(
                            "ForeignPage: a page from session {from_session}, subscription \
                             {subscription} was staged on round {}, which session {} requested \
                             as {}",
                            request.incarnation, request.session_id, request.request_id
                        ),
                    });
                }
                self.stage(
                    *reservation,
                    request,
                    StagedHeaders {
                        headers: *headers,
                        reserved_left: *reserved_left,
                        hashes,
                        ancestor: *ancestor,
                        stored,
                        staged_total: *staged_total,
                    },
                )?;
            }
            ReactorEvent::LeaseReturned {
                peer,
                incarnation,
                headers,
            } => {
                let held = self.held_by_round().contains_key(incarnation);
                let taint = self.taint_of(*incarnation);
                let Some(round) = self
                    .started
                    .get_mut(incarnation)
                    .filter(|round| round.request.peer == *peer)
                else {
                    return Err(format!(
                        "UnknownRound: a lease returned for round {incarnation}, which no \
                         longer owns any or isn't its peer's"
                    ));
                };
                if (!taint.holds && held)
                    || (!taint.counts && round.returned + headers > round.staged)
                {
                    return Err(format!(
                        "LeaseReturnedEarly: round {incarnation} returned {headers} headers with \
                         {} of its {} staged returned, while a call holds it: {held}",
                        round.returned, round.staged
                    ));
                }
                // A tainted round may report more returned than it staged.
                round.returned = (round.returned + headers).min(round.staged);
                self.prune(*incarnation);
            }
            ReactorEvent::RoundReleased {
                peer,
                incarnation,
                queue_owned,
            } => {
                if let Some((reservation, _)) = self
                    .reservations
                    .iter()
                    .find(|(_, live)| live.peer == *peer)
                {
                    self.claims.push(ClaimBreak {
                        rule: ClaimRule::LeakedReservation,
                        detail: format!(
                            "LeakedReservation: the queue released round {incarnation} with \
                             its peer still holding reservation {reservation}"
                        ),
                    });
                }
                if *queue_owned != 0 {
                    self.claims.push(ClaimBreak {
                        rule: ClaimRule::LeakedLease,
                        detail: format!(
                            "LeakedLease: the queue released round {incarnation} still holding \
                             leases of {queue_owned} headers for its peer"
                        ),
                    });
                }
                let Some(round) = self
                    .started
                    .get_mut(incarnation)
                    .filter(|round| round.request.peer == *peer && !round.released)
                else {
                    return Err(format!(
                        "UnknownRound: the queue released round {incarnation}, which isn't its \
                         peer's unreleased round"
                    ));
                };
                round.released = true;
                self.released.insert(*incarnation);
                self.check_returned(
                    *incarnation,
                    &format!("the queue released round {incarnation}"),
                );
            }
            ReactorEvent::HeadersHeld {
                call,
                kind,
                peer,
                incarnation,
                headers,
                leases,
            } => {
                let mut found = Vec::new();
                let foreign: Vec<u64> = leases
                    .iter()
                    .filter(|round| **round != *incarnation)
                    .copied()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                if let Some(first) = foreign.first() {
                    // The claim reports the defect. These rounds' leases no
                    // longer settle as the ledger tracks them, so their
                    // settlement isn't judged after it.
                    for round in foreign.iter().chain([incarnation]) {
                        self.taint_round(*round, true, true);
                    }
                    let foreign = first;
                    found.push(ClaimBreak {
                        rule: ClaimRule::ForeignLease,
                        detail: format!(
                            "ForeignLease: call {call} holds round {incarnation} with a lease \
                             staged for round {foreign}"
                        ),
                    });
                }
                let current = self.rounds.get(peer).copied();
                let record = self.record_mut(*call)?;
                if record.held.is_some() {
                    self.claims.extend(found);
                    return Err(format!("HeldTwice: call {call} took headers again"));
                }
                // The call holds the headers whatever a claim below finds, as
                // production's did, so later facts settle against it.
                record.held = Some(Held {
                    incarnation: *incarnation,
                    headers: *headers,
                    ended: false,
                });
                let (expected_kind, call_peer, owner, payload) = match &record.op {
                    PortCall::Prepare {
                        peer,
                        source,
                        owner,
                        common_ancestor,
                        target,
                        hashes,
                    } => (
                        StorageCallKind::Prepare,
                        peer,
                        *owner,
                        Some(Payload {
                            target: target.hash,
                            hashes: hashes.clone(),
                            common_ancestor: *common_ancestor,
                            source: *source,
                        }),
                    ),
                    PortCall::Apply { peer, owner, .. } => {
                        (StorageCallKind::Apply, peer, *owner, None)
                    }
                    other => {
                        found.push(ClaimBreak {
                            rule: ClaimRule::WrongCall,
                            detail: format!(
                                "WrongCall: call {call} holds headers but asks for {other:?}"
                            ),
                        });
                        self.taint_round(*incarnation, false, true);
                        self.claims.extend(found);
                        return Ok(());
                    }
                };
                if expected_kind != *kind || call_peer != peer {
                    self.taint_round(*incarnation, false, true);
                    found.push(ClaimBreak {
                        rule: ClaimRule::WrongCall,
                        detail: format!(
                            "WrongCall: call {call} names {kind:?} for another peer or kind"
                        ),
                    });
                }
                if current != Some(*incarnation) {
                    for round in current.iter().chain([incarnation]) {
                        self.taint_round(*round, false, true);
                    }
                    found.push(ClaimBreak {
                        rule: ClaimRule::WrongRound,
                        detail: format!(
                            "WrongRound: call {call} holds round {incarnation}, but its peer's \
                             current round is {current:?}"
                        ),
                    });
                }
                if let Some(handoff) = self.handoff.take() {
                    if handoff.apply != *call
                        || handoff.incarnation != *incarnation
                        || handoff.headers != *headers
                    {
                        // The claims this hold already found are kept, so
                        // the tap reports them ahead of the failure.
                        self.claims.extend(found);
                        return Err(format!(
                            "BadHandoff: call {} handed call {} round {} with {} headers, and \
                             call {call} took round {incarnation} with {headers} next",
                            handoff.prepare, handoff.apply, handoff.incarnation, handoff.headers
                        ));
                    }
                }
                let held = self.hold(*call, owner, *incarnation, *headers, payload);
                // A round holding unstaged or unowned headers returns leases
                // the ledger never counted staged.
                if held.iter().any(|claim| {
                    matches!(
                        claim.rule,
                        ClaimRule::UnstagedHeaders | ClaimRule::HeldUnowned
                    )
                }) {
                    self.taint_round(*incarnation, true, true);
                }
                found.extend(held);
                self.claims.extend(found);
            }
            ReactorEvent::HeadersEnded {
                call,
                incarnation,
                headers,
                end,
            } => {
                let record = self.record_mut(*call)?;
                let Some(held) = record.held.as_mut() else {
                    return Err(format!("NotHeld: call {call} ended headers it never held"));
                };
                if held.ended {
                    return Err(format!("EndedTwice: call {call} ended its headers again"));
                }
                held.ended = true;
                let wrong =
                    (held.incarnation != *incarnation || held.headers != *headers).then(|| {
                        ClaimBreak {
                            rule: ClaimRule::WrongRound,
                            detail: format!(
                                "WrongRound: call {call} held round {} with {} headers, and ended \
                             round {incarnation} with {headers}",
                                held.incarnation, held.headers
                            ),
                        }
                    });
                if wrong.is_some() {
                    let held_round = held.incarnation;
                    for round in [held_round, *incarnation] {
                        self.taint_round(round, false, true);
                    }
                }
                self.claims.extend(wrong);
                if let HeaderWorkEnd::HandedToApply { apply } = end {
                    self.check_handoff(*call, *apply, *incarnation, *headers)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Requires `apply` to be an open apply call for the same work as the
    /// prepare `call`, which holds no headers yet. The next `HeadersHeld` must
    /// then be `apply`'s, with `incarnation` and `headers`.
    fn check_handoff(
        &mut self,
        call: u64,
        apply: u64,
        incarnation: u64,
        headers: usize,
    ) -> Result<(), String> {
        let prepare = self.calls.get(&call).map(|record| &record.op);
        let applied = self.calls.get(&apply);
        if applied.is_some_and(|record| record.end.is_some() || record.held.is_some())
            || self.handoff.is_some()
        {
            return Err(format!(
                "BadHandoff: call {call} handed its headers to call {apply}, which has ended or \
                 already holds headers, or another handoff is open"
            ));
        }
        match (prepare, applied.map(|record| &record.op)) {
            (
                Some(PortCall::Prepare {
                    peer,
                    source,
                    owner,
                    target,
                    ..
                }),
                Some(PortCall::Apply {
                    peer: apply_peer,
                    source: apply_source,
                    owner: apply_owner,
                    target_tip_hash,
                }),
            ) => {
                if peer != apply_peer
                    || source != apply_source
                    || owner != apply_owner
                    || target.hash != *target_tip_hash
                {
                    self.claims.push(ClaimBreak {
                        rule: ClaimRule::BadHandoff,
                        detail: format!(
                            "BadHandoff: call {call} handed its headers to call {apply}, which \
                             is an apply for other work"
                        ),
                    });
                    // Both calls hold the round in turn.
                    self.taint_round(incarnation, false, true);
                }
                self.handoff = Some(Handoff {
                    prepare: call,
                    apply,
                    incarnation,
                    headers,
                });
                Ok(())
            }
            _ => Err(format!(
                "BadHandoff: call {call} handed its headers to call {apply}, which is not an \
                 apply"
            )),
        }
    }

    /// Starts `request`'s round, which must be the next one and exactly the
    /// request its reservation was bound to.
    fn start(&mut self, request: &RequestRef, purpose: &HeaderTargetPurpose) -> Result<(), String> {
        if request.incarnation != self.last_incarnation + 1 {
            return Err(format!(
                "RoundGap: round {} started after round {}",
                request.incarnation, self.last_incarnation
            ));
        }
        let Some(binding) = self
            .binding
            .take()
            .filter(|binding| binding.request == *request)
        else {
            return Err(format!(
                "UnreservedStart: round {} started without a reservation bound to its request",
                request.incarnation
            ));
        };
        let reserved = self.reservation(binding.reservation)?;
        let (opened, reserved_for) = (reserved.opened, reserved.purpose);
        let backs = match purpose {
            HeaderTargetPurpose::Normal => ReservationPurpose::Ordinary,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. } => ReservationPurpose::Repair,
        };
        if reserved_for != backs {
            return Err(format!(
                "WrongPurpose: round {} started as {purpose:?} on reservation {}, opened for \
                 {reserved_for:?}",
                request.incarnation, binding.reservation
            ));
        }
        if opened > binding.max_headers {
            return Err(format!(
                "OverRequestLimit: round {} started on reservation {} of {opened} headers, \
                 above its {}-header request limit",
                request.incarnation, binding.reservation, binding.max_headers
            ));
        }
        self.last_incarnation = request.incarnation;
        self.rounds
            .insert(request.peer.clone(), request.incarnation);
        self.started.insert(
            request.incarnation,
            Round {
                request: request.clone(),
                max_headers: binding.max_headers,
                staged: 0,
                returned: 0,
                hashes: Vec::new(),
                ancestor: None,
                released: false,
            },
        );
        Ok(())
    }

    /// Records `peer`'s new reservation, checking its number, that `peer`
    /// held none, its size, and that a continuation names exactly `peer`'s
    /// current round, within that round's request limit.
    fn open(
        &mut self,
        reservation: u64,
        peer: &ZakuraPeerId,
        purpose: ReservationPurpose,
        headers: usize,
        request: Option<&RequestRef>,
    ) -> Result<(), String> {
        if reservation <= self.last_reservation {
            return Err(format!(
                "ReservationReused: reservation {reservation} opened after reservation {}",
                self.last_reservation
            ));
        }
        self.last_reservation = reservation;
        if let Some((held, _)) = self
            .reservations
            .iter()
            .find(|(_, live)| live.peer == *peer)
        {
            return Err(format!(
                "SecondReservation: reservation {reservation} opened for a peer that still \
                 holds reservation {held}"
            ));
        }
        if purpose == ReservationPurpose::Ordinary && headers > MAX_HEADER_CHUNK_RESERVATION_V1 {
            return Err(format!(
                "OverFairShare: ordinary reservation {reservation} holds {headers} headers, \
                 above the {MAX_HEADER_CHUNK_RESERVATION_V1}-header share"
            ));
        }
        let claimed = self.claimed();
        if !self.accounting_tainted()
            && claimed.saturating_add(headers) > HEADER_CHUNK_BUDGET_CAPACITY_V1
        {
            self.claims.push(ClaimBreak {
                rule: ClaimRule::OverBudget,
                detail: format!(
                    "OverBudget: reservation {reservation} holds {headers} headers with \
                     {claimed} already claimed of {HEADER_CHUNK_BUDGET_CAPACITY_V1}"
                ),
            });
        }
        let round = match request {
            Some(request) => {
                let round = self
                    .rounds
                    .get(peer)
                    .and_then(|current| self.started.get(current))
                    .filter(|round| round.request == *request)
                    .ok_or_else(|| {
                        format!(
                            "UnknownRound: reservation {reservation} continues round {}, which \
                             isn't exactly its peer's current round",
                            request.incarnation
                        )
                    })?;
                if headers > round.max_headers {
                    return Err(format!(
                        "OverRequestLimit: continuation {reservation} holds {headers} headers, \
                         above round {}'s {}-header request limit",
                        request.incarnation, round.max_headers
                    ));
                }
                Some(request.incarnation)
            }
            None => None,
        };
        self.reservations.insert(
            reservation,
            Reservation {
                peer: peer.clone(),
                purpose,
                opened: headers,
                staged: 0,
                round,
            },
        );
        Ok(())
    }

    /// Binds the unbound `reservation` to `request`, the round its peer
    /// starts next, which asks for at most `max_headers` per request.
    fn bind(
        &mut self,
        reservation: u64,
        request: &RequestRef,
        max_headers: usize,
    ) -> Result<(), String> {
        let next = self.last_incarnation + 1;
        let pending = self.binding.is_some();
        let live = self.reservation_mut(reservation)?;
        if live.round.is_some()
            || live.peer != request.peer
            || request.incarnation != next
            || pending
        {
            return Err(format!(
                "WrongReservation: reservation {reservation} was bound to round {}, which isn't \
                 its peer's next round {next}, or it was already bound",
                request.incarnation
            ));
        }
        live.round = Some(request.incarnation);
        self.binding = Some(Binding {
            reservation,
            request: request.clone(),
            max_headers,
        });
        Ok(())
    }

    /// Stages `page` on `request`'s round from `reservation`.
    fn stage(
        &mut self,
        reservation: u64,
        request: &RequestRef,
        page: StagedHeaders<'_>,
    ) -> Result<(), String> {
        let StagedHeaders {
            headers,
            reserved_left,
            hashes,
            ancestor,
            stored,
            staged_total,
        } = page;
        let current = self.rounds.get(&request.peer) == Some(&request.incarnation);
        if !current
            || self
                .started
                .get(&request.incarnation)
                .is_none_or(|round| round.request != *request)
        {
            return Err(format!(
                "UnknownRound: a page was staged on round {}, which isn't exactly its peer's \
                 current round",
                request.incarnation
            ));
        }
        let reservation_tainted = self.tainted_reservations.contains(&reservation);
        let round_tainted = self.taint_of(request.incarnation).counts;
        let live = self.reservation_mut(reservation)?;
        if live.round != Some(request.incarnation) || live.peer != request.peer {
            return Err(format!(
                "WrongReservation: a page of round {} spent reservation {reservation}, which \
                 backs round {:?}",
                request.incarnation, live.round
            ));
        }
        let unstaged = live.opened.saturating_sub(live.staged);
        let over = (!reservation_tainted
            && (headers == 0 || headers > unstaged || reserved_left != unstaged - headers))
            .then(|| ClaimBreak {
                rule: ClaimRule::OverReservation,
                detail: format!(
                    "OverReservation: a page of {headers} headers spent reservation \
                     {reservation}, which held {unstaged} and reports {reserved_left} left"
                ),
            });
        live.staged += headers;
        if over.is_some() {
            self.tainted_reservations.insert(reservation);
        }
        self.claims.extend(over);
        let round = self
            .started
            .get_mut(&request.incarnation)
            .expect("the round was found above");
        round.staged += headers;
        round.ancestor.get_or_insert(ancestor);
        let before = std::mem::replace(&mut round.hashes, stored.to_vec());
        let kept = stored.get(..before.len()) == Some(&before[..]);
        let added = stored.get(before.len()..) == Some(hashes);
        if !round_tainted
            && (hashes.len() != headers || !kept || !added || staged_total != round.staged)
        {
            let detail = format!(
                "UnstagedPage: a page of {headers} headers with {} hashes left round {} \
                 holding {staged_total} of the {} it staged, or holding other headers",
                hashes.len(),
                request.incarnation,
                round.staged
            );
            self.claims.push(ClaimBreak {
                rule: ClaimRule::UnstagedPage,
                detail,
            });
            self.taint_round(request.incarnation, true, false);
        }
        Ok(())
    }

    /// Requires call `call`, for `owner`, to hold `headers` more of round
    /// `incarnation` than its other calls do, within what the round owns,
    /// for the owner that started it or that owner narrowed to its staged
    /// tip.
    /// A prepare's `payload` must name the round's staged tip and send
    /// exactly its staged headers, as many as the call holds. Returns the
    /// claim breaks found.
    fn hold(
        &self,
        call: u64,
        owner: HeaderSyncWorkOwner,
        incarnation: u64,
        headers: usize,
        payload: Option<Payload>,
    ) -> Vec<ClaimBreak> {
        let claim = |rule, detail| ClaimBreak { rule, detail };
        let taint = self.taint_of(incarnation);
        let Some(round) = self
            .started
            .get(&incarnation)
            .filter(|round| taint.counts || round.staged > 0)
        else {
            return vec![claim(
                ClaimRule::UnstagedHeaders,
                format!(
                    "UnstagedHeaders: call {call} holds round {incarnation}, which staged no \
                     page"
                ),
            )];
        };
        let mut found = Vec::new();
        let tip = round.hashes.last().copied();
        // A narrowed owner is judged by the staged tip, which tainted counts
        // don't give, so then any tip passes.
        let same = if taint.counts {
            same_work_to_any_tip(owner, round.request.owner)
        } else {
            same_work(owner, round.request.owner, tip)
        };
        if !same {
            found.push(claim(
                ClaimRule::WrongOwner,
                format!(
                    "WrongOwner: call {call} holds round {incarnation} for {owner:?}, but its \
                     pages were staged for {:?}",
                    round.request.owner
                ),
            ));
        }
        let payload_checked = payload.clone();
        if let Some(payload) = payload.filter(|payload| {
            payload.hashes.len() != headers
                || (!taint.counts
                    && (tip != Some(payload.target) || payload.hashes != round.hashes))
        }) {
            found.push(claim(
                ClaimRule::WrongPayload,
                format!(
                    "WrongPayload: call {call} sends {} headers toward {:?} for round \
                     {incarnation}, which holds {headers} staged through {tip:?}, and they \
                     aren't its staged headers in order",
                    payload.hashes.len(),
                    payload.target
                ),
            ));
        }
        if let Some(payload) = payload_checked.as_ref() {
            if round.ancestor != Some(payload.common_ancestor) {
                found.push(claim(
                    ClaimRule::WrongPayload,
                    format!(
                        "WrongPayload: call {call} names ancestor {:?} for round {incarnation}, \
                         whose first page declared {:?}",
                        payload.common_ancestor, round.ancestor
                    ),
                ));
            }
            // A round's source is its peer's, as the reactor derives it.
            let derived = SourceId::from_digest(round.request.peer.digest());
            if payload.source != derived {
                found.push(claim(
                    ClaimRule::WrongSource,
                    format!(
                        "WrongSource: call {call} supplies round {incarnation} from {:?}, but \
                         its peer's source is {derived:?}",
                        payload.source
                    ),
                ));
            }
        }
        // This call already counts as holding its headers.
        let held = self.held_by_round().get(&incarnation).copied().unwrap_or(0);
        let owned = round.staged.saturating_sub(round.returned);
        if !taint.counts && !taint.holds && held > owned {
            found.push(claim(
                ClaimRule::HeldUnowned,
                format!(
                    "HeldUnowned: call {call} holds {headers} headers of round {incarnation}, \
                     so its calls hold {held} of the {owned} it owns"
                ),
            ));
        }
        found
    }

    /// Requires every lease of `incarnation`, if the queue released it and
    /// no call holds it, to have returned by `when`, as a `LeakedLease`
    /// claim, and forgets the round once it settled.
    fn check_returned(&mut self, incarnation: u64, when: &str) {
        if let Some(detail) = self.unreturned(incarnation, when) {
            self.claims.push(ClaimBreak {
                rule: ClaimRule::LeakedLease,
                detail,
            });
        }
        self.prune(incarnation);
    }

    /// The `LeakedLease` break if the queue released `incarnation` and, with
    /// no call holding it, it kept a lease at `when`.
    fn unreturned(&self, incarnation: u64, when: &str) -> Option<String> {
        if self.tainted_rounds.contains_key(&incarnation) {
            return None;
        }
        let held = self.held_by_round().contains_key(&incarnation);
        let round = self.started.get(&incarnation)?;
        (round.released && !held && round.returned != round.staged).then(|| {
            format!(
                "LeakedLease: {when} with {} of round {incarnation}'s {} staged headers \
                 unreturned and no call holding them",
                round.staged - round.returned,
                round.staged
            )
        })
    }

    /// Forgets `incarnation` once the queue released it and all its leases
    /// returned. A tainted round is never forgotten, so it never settles.
    fn prune(&mut self, incarnation: u64) {
        if !self.tainted_rounds.contains_key(&incarnation)
            && self
                .started
                .get(&incarnation)
                .is_some_and(|round| round.released && round.returned == round.staged)
        {
            self.started.remove(&incarnation);
        }
    }

    /// Every header the queue's accounting claims: [`Self::reserved`] and
    /// [`Self::owned`].
    fn claimed(&self) -> usize {
        self.reserved() + self.owned()
    }

    /// How many rounds the queue has released.
    pub fn released_rounds(&self) -> u64 {
        // The conversion is exact: a run releases far fewer than 2^64 rounds.
        self.released.len() as u64
    }

    /// How many rounds the queue released that haven't settled: some of
    /// their staged headers haven't returned, or a call that held their
    /// headers hasn't been handled. A dropped call has no handler, so it
    /// doesn't hold a round's settlement back.
    pub fn unsettled_rounds(&self) -> usize {
        self.released
            .iter()
            .filter(|incarnation| self.unsettled(**incarnation))
            .count()
    }

    /// Whether released round `incarnation` hasn't settled, as
    /// [`unsettled_rounds`](Self::unsettled_rounds) counts it.
    fn unsettled(&self, incarnation: u64) -> bool {
        self.started.contains_key(&incarnation)
            || self.calls.values().any(|call| {
                !call.handled
                    && call.end != Some(CallEnd::Dropped)
                    && call
                        .held
                        .as_ref()
                        .is_some_and(|held| held.incarnation == incarnation)
            })
    }

    /// How many released rounds a claim tainted, which never settle. See the
    /// module doc.
    pub fn tainted_rounds(&self) -> usize {
        self.released
            .iter()
            .filter(|incarnation| self.tainted_rounds.contains_key(incarnation))
            .count()
    }

    /// How many released rounds haven't settled with no call that holds
    /// their headers still unended, so storage has nothing left to answer
    /// that could settle them. A tainted round isn't judged stuck.
    pub fn stuck_rounds(&self) -> usize {
        self.released
            .iter()
            .filter(|incarnation| {
                !self.tainted_rounds.contains_key(incarnation)
                    && self.unsettled(**incarnation)
                    && !self.calls.values().any(|call| {
                        call.end.is_none()
                            && call
                                .held
                                .as_ref()
                                .is_some_and(|held| held.incarnation == **incarnation)
                    })
            })
            .count()
    }

    /// The claim breaks since the last call, in order.
    pub fn take_claims(&mut self) -> Vec<ClaimBreak> {
        std::mem::take(&mut self.claims)
    }

    /// Marks round `incarnation`'s `counts` or `holds` unreliable. See the
    /// module doc.
    fn taint_round(&mut self, incarnation: u64, counts: bool, holds: bool) {
        let taint = self.tainted_rounds.entry(incarnation).or_default();
        taint.counts |= counts;
        taint.holds |= holds;
    }

    /// What claims made unreliable about round `incarnation`.
    fn taint_of(&self, incarnation: u64) -> RoundTaint {
        self.tainted_rounds
            .get(&incarnation)
            .copied()
            .unwrap_or_default()
    }

    /// Whether a live reservation or a round the ledger keeps is tainted, so
    /// [`reserved`](Self::reserved) and [`owned`](Self::owned) aren't
    /// production's totals. See the module doc.
    pub fn accounting_tainted(&self) -> bool {
        !self.tainted_reservations.is_empty() || !self.tainted_rounds.is_empty()
    }

    /// The headers the queue's accounting holds reserved: each live
    /// reservation's unstaged ones.
    pub fn reserved(&self) -> usize {
        self.reservations
            .values()
            .map(|live| live.opened.saturating_sub(live.staged))
            .sum()
    }

    /// The headers the queue's accounting holds owned: each round's staged
    /// headers that haven't returned.
    pub fn owned(&self) -> usize {
        self.started
            .values()
            // A tainted round may have returned more than it staged.
            .map(|round| round.staged.saturating_sub(round.returned))
            .sum()
    }

    /// How many headers calls hold now, by round. A handoff's headers count
    /// for their round until its apply takes them.
    pub fn held_by_round(&self) -> BTreeMap<u64, usize> {
        let mut held = BTreeMap::new();
        let calls = self
            .calls
            .values()
            .filter_map(|record| record.held.as_ref())
            .filter(|held| !held.ended)
            .map(|held| (held.incarnation, held.headers));
        let handoff = self
            .handoff
            .iter()
            .map(|handoff| (handoff.incarnation, handoff.headers));
        for (incarnation, headers) in calls.chain(handoff) {
            *held.entry(incarnation).or_insert(0) += headers;
        }
        held
    }

    /// The live `reservation`.
    fn reservation(&self, reservation: u64) -> Result<&Reservation, String> {
        self.reservations.get(&reservation).ok_or_else(|| {
            format!("UnknownReservation: event names unopened or ended reservation {reservation}")
        })
    }

    /// The live `reservation`, to change.
    fn reservation_mut(&mut self, reservation: u64) -> Result<&mut Reservation, String> {
        self.reservations.get_mut(&reservation).ok_or_else(|| {
            format!("UnknownReservation: event names unopened or ended reservation {reservation}")
        })
    }

    /// Checks that a torn-down reactor left nothing open, once its calls
    /// were dropped or handled. A call left unended or unheld, a handoff no
    /// apply took, or a binding no round started breaks an integrity rule,
    /// which this returns. Headers a call still holds (`OpenOwnership`), a
    /// reservation never ended (`OpenReservation`) and a staged lease never
    /// returned (`LeakedLease`) are claim breaks, kept for
    /// [`take_claims`](Self::take_claims).
    pub fn finish(&mut self) -> Result<(), String> {
        if let Some(handoff) = &self.handoff {
            return Err(format!(
                "UnresolvedHandoff: call {} handed its headers to call {}, which never took \
                 them",
                handoff.prepare, handoff.apply
            ));
        }
        for (call, record) in &self.calls {
            if record.end.is_none() {
                return Err(format!("UnendedCall: call {call} never reported its end"));
            }
            if record.holds_nothing() {
                return Err(format!(
                    "UnheldCall: call {call} never held its round's headers"
                ));
            }
        }
        if let Some(binding) = &self.binding {
            return Err(format!(
                "UnresolvedBinding: reservation {} was bound to round {}, which never started",
                binding.reservation, binding.request.incarnation
            ));
        }
        let mut claims = Vec::new();
        for (call, record) in &self.calls {
            if record.held.as_ref().is_some_and(|held| !held.ended) {
                claims.push(ClaimBreak {
                    rule: ClaimRule::OpenOwnership,
                    detail: format!("OpenOwnership: call {call} never ended its headers"),
                });
            }
        }
        for reservation in self.reservations.keys() {
            claims.push(ClaimBreak {
                rule: ClaimRule::OpenReservation,
                detail: format!("OpenReservation: reservation {reservation} never ended"),
            });
        }
        for (incarnation, round) in &self.started {
            if !self.tainted_rounds.contains_key(incarnation) && round.returned != round.staged {
                claims.push(ClaimBreak {
                    rule: ClaimRule::LeakedLease,
                    detail: format!(
                        "LeakedLease: teardown finished with {} of round {incarnation}'s {} \
                         staged headers unreturned",
                        round.staged - round.returned,
                        round.staged
                    ),
                });
            }
        }
        self.claims.extend(claims);
        Ok(())
    }

    fn record_mut(&mut self, call: u64) -> Result<&mut CallRecord, String> {
        self.calls
            .get_mut(&call)
            .ok_or_else(|| format!("UnknownCall: event names undispatched call {call}"))
    }
}

/// Whether a call for `call` carries pages staged for `staged`, whose last
/// staged header is `tip`: the same owner, or the same ordinary owner
/// narrowed to `tip`, as a bounded prepare narrows it.
fn same_work(
    call: HeaderSyncWorkOwner,
    staged: HeaderSyncWorkOwner,
    tip: Option<block::Hash>,
) -> bool {
    if call == staged {
        return true;
    }
    match (call, staged) {
        (HeaderSyncWorkOwner::Header(call), HeaderSyncWorkOwner::Header(mut narrowed)) => {
            let Some(tip) = tip else {
                return false;
            };
            narrowed.authority.branch.target_tip_hash = tip;
            call == narrowed
        }
        _ => false,
    }
}

/// [`same_work`] for a round whose staged tip is unknown: the same owner,
/// or the same ordinary owner narrowed to any tip.
fn same_work_to_any_tip(call: HeaderSyncWorkOwner, staged: HeaderSyncWorkOwner) -> bool {
    match (call, staged) {
        (HeaderSyncWorkOwner::Header(call_owner), HeaderSyncWorkOwner::Header(_)) => same_work(
            call,
            staged,
            Some(call_owner.authority.branch.target_tip_hash),
        ),
        _ => call == staged,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use zakura_chain::block;
    use zakura_header_chain::{
        BranchId, HeaderGeneration, HeaderSyncWorkOwner, HeaderWorkAuthority, HeaderWorkOwner,
        SourceId,
    };

    use super::*;

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length")
    }

    fn owner() -> HeaderSyncWorkOwner {
        HeaderWorkOwner {
            authority: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId {
                    anchor_hash: block::Hash([1; 32]),
                    target_tip_hash: block::Hash([2; 32]),
                },
            },
            session_id: 1,
            request_id: NonZeroU64::new(1).expect("one is nonzero"),
        }
        .into()
    }

    /// `byte`'s peer's prepare of 5 headers through [`TIP`].
    fn prepare(byte: u8) -> PortCall {
        prepare_of(byte, owner(), 5)
    }

    /// `byte`'s peer's prepare for `owner` of `headers` headers through
    /// [`TIP`].
    fn prepare_of(byte: u8, owner: HeaderSyncWorkOwner, headers: usize) -> PortCall {
        PortCall::Prepare {
            peer: peer(byte),
            source: SourceId::from_digest([byte; 32]),
            owner,
            common_ancestor: zakura_header_chain::Frontier::new(block::Height(0), TIP),
            target: zakura_header_chain::Frontier::new(block::Height(5), TIP),
            hashes: vec![TIP; headers],
        }
    }

    fn apply(byte: u8) -> PortCall {
        PortCall::Apply {
            peer: peer(byte),
            source: SourceId::from_digest([byte; 32]),
            owner: owner(),
            target_tip_hash: TIP,
        }
    }

    /// Round `incarnation` of `byte`'s peer, on session 1 as request 1.
    fn request(byte: u8, incarnation: u64) -> RequestRef {
        RequestRef {
            peer: peer(byte),
            session_id: 1,
            request_id: 1,
            incarnation,
            owner: owner(),
            target_tip_hash: block::Hash([2; 32]),
        }
    }

    fn started(byte: u8, incarnation: u64) -> ReactorEvent {
        ReactorEvent::Started {
            request: request(byte, incarnation),
            purpose: crate::zakura::header_sync::HeaderTargetPurpose::Normal,
            locator: Vec::new(),
        }
    }

    /// An ordinary reservation `reservation` of `headers` for `byte`'s peer.
    fn opened(reservation: u64, byte: u8, headers: usize) -> ReactorEvent {
        ReactorEvent::ReservationOpened {
            reservation,
            peer: peer(byte),
            purpose: ReservationPurpose::Ordinary,
            headers,
            request: None,
        }
    }

    fn bound(reservation: u64, byte: u8, incarnation: u64) -> ReactorEvent {
        ReactorEvent::ReservationBound {
            reservation,
            request: request(byte, incarnation),
            max_headers: MAX_HEADER_CHUNK_RESERVATION_V1,
        }
    }

    /// The queue released `byte`'s peer's round `incarnation`.
    fn released(byte: u8, incarnation: u64) -> ReactorEvent {
        ReactorEvent::RoundReleased {
            peer: peer(byte),
            incarnation,
            queue_owned: 0,
        }
    }

    /// The tip every test page ends at.
    const TIP: block::Hash = block::Hash([9; 32]);

    /// A page of `headers` from `byte`'s peer on its round `incarnation`,
    /// spending `reservation`, which keeps `left`, after which the round
    /// holds `total`. Every test header hashes to [`TIP`].
    fn staged(
        reservation: u64,
        byte: u8,
        incarnation: u64,
        headers: usize,
        left: usize,
        total: usize,
    ) -> ReactorEvent {
        ReactorEvent::PageStaged {
            from_peer: peer(byte),
            from_session: 1,
            request: request(byte, incarnation),
            reservation,
            subscription: 1,
            frame: None,
            headers,
            reserved_left: left,
            ancestor: zakura_header_chain::Frontier::new(block::Height(0), TIP),
            hashes: vec![TIP; headers],
            stored: vec![TIP; total],
            staged_total: total,
        }
    }

    fn reservation_ended(reservation: u64, released: usize, cause: ReservationEnd) -> ReactorEvent {
        ReactorEvent::ReservationEnded {
            reservation,
            released,
            cause,
        }
    }

    fn returned(byte: u8, incarnation: u64, headers: usize) -> ReactorEvent {
        ReactorEvent::LeaseReturned {
            peer: peer(byte),
            incarnation,
            headers,
        }
    }

    fn retired(byte: u8, incarnation: u64) -> ReactorEvent {
        ReactorEvent::Retired {
            request: request(byte, incarnation),
            phase: super::super::HeaderTargetPhase::Receiving,
            purpose: crate::zakura::header_sync::HeaderTargetPurpose::Normal,
            reason: super::super::HeaderRequestTerminal::Disconnected,
        }
    }

    /// Round `incarnation` of `byte`'s peer started on reservation
    /// `incarnation` of 5 headers, with one page that spent it all.
    fn round(byte: u8, incarnation: u64) -> Vec<ReactorEvent> {
        vec![
            opened(incarnation, byte, 5),
            bound(incarnation, byte, incarnation),
            started(byte, incarnation),
            staged(incarnation, byte, incarnation, 5, 0, 5),
            reservation_ended(incarnation, 0, ReservationEnd::Staged),
        ]
    }

    /// Each of `rounds`, as [`round`], then `rest`.
    fn after(rounds: &[(u8, u64)], rest: Vec<ReactorEvent>) -> Vec<ReactorEvent> {
        let mut events: Vec<_> = rounds
            .iter()
            .flat_map(|(byte, incarnation)| round(*byte, *incarnation))
            .collect();
        events.extend(rest);
        events
    }

    fn held(call: u64, kind: StorageCallKind, byte: u8, incarnation: u64) -> ReactorEvent {
        ReactorEvent::HeadersHeld {
            call,
            kind,
            peer: peer(byte),
            incarnation,
            headers: 5,
            leases: Vec::new(),
        }
    }

    fn ended(call: u64, incarnation: u64, end: HeaderWorkEnd) -> ReactorEvent {
        ReactorEvent::HeadersEnded {
            call,
            incarnation,
            headers: 5,
            end,
        }
    }

    fn dispatched(call: u64, op: PortCall) -> ReactorEvent {
        ReactorEvent::Dispatched { call, op }
    }

    /// Folds `events`, and returns the first broken rule's name.
    fn fold(events: Vec<ReactorEvent>) -> Result<HeaderWorkLedger, String> {
        let mut ledger = HeaderWorkLedger::default();
        for event in &events {
            ledger.observe(event)?;
        }
        Ok(ledger)
    }

    fn refused_as(events: Vec<ReactorEvent>, rule: &str) {
        let error = fold(events).expect_err("the ledger refuses this history");
        assert!(error.starts_with(rule), "{error}");
    }

    /// Folds `events`, which the ledger accepts, and requires its first
    /// claim break to be `rule`'s.
    fn claimed_as(events: Vec<ReactorEvent>, rule: ClaimRule) -> HeaderWorkLedger {
        let mut ledger = fold(events).expect("a claim break doesn't refuse the history");
        let claims = ledger.take_claims();
        assert_eq!(
            claims.first().map(|claim| claim.rule),
            Some(rule),
            "{claims:?}"
        );
        ledger
    }

    #[test]
    fn a_prepare_handed_to_its_apply_folds_clean() {
        let mut ledger = fold(after(
            &[(1, 1)],
            vec![
                dispatched(1, prepare(1)),
                held(1, StorageCallKind::Prepare, 1, 1),
                ReactorEvent::CallEnded {
                    call: 1,
                    end: CallEnd::Replied,
                },
                dispatched(2, apply(1)),
                ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }),
                held(2, StorageCallKind::Apply, 1, 1),
                ReactorEvent::Handled {
                    call: 1,
                    panicked: false,
                },
                ReactorEvent::CallEnded {
                    call: 2,
                    end: CallEnd::Replied,
                },
                ended(2, 1, HeaderWorkEnd::Settled),
                ReactorEvent::Handled {
                    call: 2,
                    panicked: false,
                },
                returned(1, 1, 5),
                released(1, 1),
                retired(1, 1),
            ],
        ))
        .expect("the history is exact");
        ledger.finish().expect("every call ended");
    }

    /// #1845: a capture naming a retired round is a claim break.
    #[test]
    fn a_capture_must_name_its_peers_current_round() {
        claimed_as(
            after(
                &[(1, 1), (2, 2)],
                vec![
                    dispatched(1, prepare(1)),
                    held(1, StorageCallKind::Prepare, 1, 2),
                ],
            ),
            ClaimRule::WrongRound,
        );
    }

    /// #1846: a handoff to the prepare's own call fails, and one to an
    /// apply for other work is a claim break.
    #[test]
    fn a_handoff_must_name_an_apply_for_the_same_work() {
        let base = after(
            &[(1, 1)],
            vec![
                dispatched(1, prepare(1)),
                held(1, StorageCallKind::Prepare, 1, 1),
            ],
        );
        let mut own_call = base.clone();
        own_call.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 1 }));
        refused_as(own_call, "BadHandoff");

        let mut other_peer = base;
        other_peer.push(dispatched(2, apply(2)));
        other_peer.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        claimed_as(other_peer, ClaimRule::BadHandoff);
    }

    /// A handoff must reach an open apply, which takes the same round and
    /// count next; one that never arrives fails at teardown.
    #[test]
    fn a_handoff_must_reach_its_open_apply() {
        let base = after(
            &[(1, 1)],
            vec![
                dispatched(1, prepare(1)),
                held(1, StorageCallKind::Prepare, 1, 1),
                dispatched(2, apply(1)),
            ],
        );

        let mut ended_apply = base.clone();
        ended_apply.push(ReactorEvent::CallEnded {
            call: 2,
            end: CallEnd::Replied,
        });
        ended_apply.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        // The apply ended before taking any headers, which fails first.
        refused_as(ended_apply, "UnheldCall");

        // A continuation's page gives the round room for the apply's own hold.
        let mut holding_apply = base.clone();
        holding_apply.extend([
            ReactorEvent::ReservationOpened {
                reservation: 2,
                peer: peer(1),
                purpose: ReservationPurpose::Ordinary,
                headers: 5,
                request: Some(request(1, 1)),
            },
            staged(2, 1, 1, 5, 0, 10),
        ]);
        holding_apply.push(held(2, StorageCallKind::Apply, 1, 1));
        holding_apply.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        refused_as(holding_apply, "BadHandoff");

        let mut wrong_count = base.clone();
        wrong_count.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        wrong_count.push(ReactorEvent::HeadersHeld {
            call: 2,
            kind: StorageCallKind::Apply,
            peer: peer(1),
            incarnation: 1,
            headers: 4,
            leases: Vec::new(),
        });
        refused_as(wrong_count, "BadHandoff");

        // A hold that breaks the handoff keeps the claims it found first, so
        // the tap reports them ahead of the failure.
        let mut foreign_and_wrong = base.clone();
        foreign_and_wrong.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        foreign_and_wrong.push(ReactorEvent::HeadersHeld {
            call: 2,
            kind: StorageCallKind::Apply,
            peer: peer(1),
            incarnation: 1,
            headers: 4,
            leases: vec![9],
        });
        let mut ledger = HeaderWorkLedger::default();
        let error = foreign_and_wrong
            .iter()
            .find_map(|event| ledger.observe(event).err())
            .expect("the ledger refuses the handoff");
        assert!(error.starts_with("BadHandoff"), "{error}");
        let rules: Vec<_> = ledger.take_claims().iter().map(|c| c.rule).collect();
        assert_eq!(rules, [ClaimRule::ForeignLease]);

        let mut other_call = base.clone();
        other_call.push(dispatched(3, apply(1)));
        other_call.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        other_call.push(held(3, StorageCallKind::Apply, 1, 1));
        refused_as(other_call, "BadHandoff");

        let mut unresolved = base;
        unresolved.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        let mut ledger = fold(unresolved).expect("an open handoff folds");
        assert!(ledger
            .finish()
            .expect_err("the apply never took the headers")
            .starts_with("UnresolvedHandoff"));
    }

    /// #1842: `Handled` without `CallEnded` fails without waiting for teardown.
    #[test]
    fn a_handled_call_must_have_reported_its_end() {
        refused_as(
            vec![
                dispatched(1, prepare(1)),
                ReactorEvent::Handled {
                    call: 1,
                    panicked: false,
                },
            ],
            "MissingCallEnd",
        );
        refused_as(
            vec![
                dispatched(1, prepare(1)),
                ReactorEvent::CallEnded {
                    call: 1,
                    end: CallEnd::Dropped,
                },
                ReactorEvent::Handled {
                    call: 1,
                    panicked: false,
                },
            ],
            "MissingCallEnd",
        );
    }

    #[test]
    fn ownership_ends_once_with_its_exact_count() {
        let held_call = after(
            &[(1, 1)],
            vec![
                dispatched(1, prepare(1)),
                held(1, StorageCallKind::Prepare, 1, 1),
            ],
        );
        let mut twice = held_call.clone();
        twice.push(ended(1, 1, HeaderWorkEnd::Stale));
        twice.push(ended(1, 1, HeaderWorkEnd::Stale));
        refused_as(twice, "EndedTwice");

        let mut wrong = held_call.clone();
        wrong.push(ReactorEvent::HeadersEnded {
            call: 1,
            incarnation: 1,
            headers: 4,
            end: HeaderWorkEnd::Stale,
        });
        claimed_as(wrong, ClaimRule::WrongRound);

        let mut open = fold(held_call).expect("a held call folds");
        assert!(open
            .finish()
            .expect_err("a held call stays open")
            .starts_with("UnendedCall"));
    }

    #[test]
    fn rounds_count_without_gaps() {
        refused_as(vec![started(1, 2)], "RoundGap");
    }

    /// A round's reservation spent over two pages, a continuation, a prepare
    /// holding every owned header, and the leases returned once the round
    /// retires: the accounting folds clean, and the held projection names
    /// the prepare's round while it holds.
    #[test]
    fn a_reserved_round_stages_holds_and_returns_clean() {
        let continuation = ReactorEvent::ReservationOpened {
            reservation: 2,
            peer: peer(1),
            purpose: ReservationPurpose::Ordinary,
            headers: 4,
            request: Some(request(1, 1)),
        };
        let mut ledger = fold(vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 2, 3, 2),
            staged(1, 1, 1, 3, 0, 5),
            reservation_ended(1, 0, ReservationEnd::Staged),
            continuation,
            staged(2, 1, 1, 4, 0, 9),
            reservation_ended(2, 0, ReservationEnd::Staged),
            dispatched(1, prepare_of(1, owner(), 9)),
            ReactorEvent::HeadersHeld {
                call: 1,
                kind: StorageCallKind::Prepare,
                peer: peer(1),
                incarnation: 1,
                headers: 9,
                leases: Vec::new(),
            },
        ])
        .expect("the accounting is exact");
        assert_eq!(ledger.held_by_round(), BTreeMap::from([(1, 9)]));
        for event in [
            ReactorEvent::CallEnded {
                call: 1,
                end: CallEnd::Replied,
            },
            ReactorEvent::HeadersEnded {
                call: 1,
                incarnation: 1,
                headers: 9,
                end: HeaderWorkEnd::Stale,
            },
            ReactorEvent::Handled {
                call: 1,
                panicked: false,
            },
            returned(1, 1, 2),
            returned(1, 1, 3),
            returned(1, 1, 4),
            released(1, 1),
            retired(1, 1),
        ] {
            ledger.observe(&event).expect("the accounting is exact");
        }
        assert!(ledger.held_by_round().is_empty());
        ledger.finish().expect("nothing stays reserved or owned");
    }

    /// A second reservation for a peer that holds one, an ordinary one above
    /// the fair share, and one above the unclaimed budget each fail.
    #[test]
    fn a_reservation_fits_its_peer_its_share_and_the_budget() {
        refused_as(vec![opened(1, 1, 5), opened(2, 1, 5)], "SecondReservation");
        refused_as(
            vec![opened(1, 1, MAX_HEADER_CHUNK_RESERVATION_V1 + 1)],
            "OverFairShare",
        );
        let repair = |reservation, byte, headers| ReactorEvent::ReservationOpened {
            reservation,
            peer: peer(byte),
            purpose: ReservationPurpose::Repair,
            headers,
            request: None,
        };
        fold(vec![repair(1, 1, 401)]).expect("a repair may exceed the fair share");
        let mut over = fold(vec![
            repair(1, 1, HEADER_CHUNK_BUDGET_CAPACITY_V1 - 10),
            opened(2, 2, 11),
        ])
        .expect("a claim break doesn't refuse the history");
        let claims = over.take_claims();
        assert_eq!(claims.len(), 1, "{claims:?}");
        assert_eq!(claims[0].rule, ClaimRule::OverBudget);
        assert!(claims[0].detail.starts_with("OverBudget"), "{claims:?}");
        assert_eq!(
            over.reserved(),
            HEADER_CHUNK_BUDGET_CAPACITY_V1 + 1,
            "the over-budget reservation still counts"
        );
        refused_as(vec![opened(2, 1, 5), opened(1, 2, 5)], "ReservationReused");
    }

    /// A round needs its bound reservation, and a page needs its own round's
    /// reservation, session and subscription, within what it holds.
    #[test]
    fn pages_spend_their_own_rounds_reservation() {
        refused_as(vec![opened(1, 1, 5), started(1, 1)], "UnreservedStart");
        refused_as(vec![opened(1, 1, 5), bound(1, 2, 1)], "WrongReservation");
        let base = vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            opened(2, 2, 5),
        ];
        let mut other = base.clone();
        other.push(staged(2, 1, 1, 2, 3, 2));
        refused_as(other, "WrongReservation");
        let mut foreign = base.clone();
        foreign.push(ReactorEvent::PageStaged {
            from_peer: peer(1),
            from_session: 2,
            request: request(1, 1),
            reservation: 1,
            subscription: 1,
            frame: None,
            headers: 2,
            reserved_left: 3,
            ancestor: zakura_header_chain::Frontier::new(block::Height(0), TIP),
            hashes: vec![TIP; 2],
            stored: vec![TIP; 2],
            staged_total: 2,
        });
        claimed_as(foreign, ClaimRule::ForeignPage);
        let mut over = base.clone();
        over.push(staged(1, 1, 1, 6, 0, 6));
        claimed_as(over, ClaimRule::OverReservation);
        let mut miscounted = base.clone();
        miscounted.push(staged(1, 1, 1, 2, 2, 2));
        claimed_as(miscounted, ClaimRule::OverReservation);
        for left in [2, 6] {
            let mut miscounted_then_ended = base.clone();
            miscounted_then_ended.push(staged(1, 1, 1, 2, left, 2));
            miscounted_then_ended.push(reservation_ended(1, left, ReservationEnd::Released));
            claimed_as(miscounted_then_ended, ClaimRule::OverReservation);
        }
        let mut released = base;
        released.push(staged(1, 1, 1, 2, 3, 2));
        released.push(reservation_ended(1, 2, ReservationEnd::Released));
        refused_as(released, "ReleaseMismatch");
    }

    /// A call's prepare must name the owner its round's pages were staged
    /// for, and hold only headers that round owns.
    #[test]
    fn a_call_holds_its_rounds_own_pages() {
        let base = vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 3, 2, 3),
        ];
        let mut swapped = base.clone();
        let mut owner = owner();
        if let HeaderSyncWorkOwner::Header(header) = &mut owner {
            header.request_id = NonZeroU64::new(2).expect("two is nonzero");
        }
        swapped.push(dispatched(1, prepare_of(1, owner, 3)));
        swapped.push(ReactorEvent::HeadersHeld {
            call: 1,
            kind: StorageCallKind::Prepare,
            peer: peer(1),
            incarnation: 1,
            headers: 3,
            leases: Vec::new(),
        });
        claimed_as(swapped, ClaimRule::WrongOwner);
        let mut unowned = base;
        unowned.push(dispatched(1, apply(1)));
        unowned.push(ReactorEvent::HeadersHeld {
            call: 1,
            kind: StorageCallKind::Apply,
            peer: peer(1),
            incarnation: 1,
            headers: 4,
            leases: Vec::new(),
        });
        claimed_as(unowned, ClaimRule::HeldUnowned);
    }

    /// A round whose staged count a claim tainted still holds only its own
    /// owner's calls, each sending as many headers as it holds. The claim
    /// is recorded first.
    #[test]
    fn a_tainted_round_still_judges_its_owner_and_payload_length() {
        // The page reports the round holding none of the 3 it staged.
        let base = vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 3, 2, 0),
        ];
        let held = |headers| ReactorEvent::HeadersHeld {
            call: 1,
            kind: StorageCallKind::Prepare,
            peer: peer(1),
            incarnation: 1,
            headers,
            leases: Vec::new(),
        };
        let claimed = |events: Vec<ReactorEvent>, rules: &[ClaimRule]| {
            let mut ledger = fold(events).expect("claims don't refuse the history");
            let found: Vec<_> = ledger.take_claims().iter().map(|c| c.rule).collect();
            assert_eq!(found, rules);
        };
        let mut other = owner();
        if let HeaderSyncWorkOwner::Header(header) = &mut other {
            header.request_id = NonZeroU64::new(2).expect("two is nonzero");
        }
        let mut swapped = base.clone();
        swapped.extend([dispatched(1, prepare_of(1, other, 3)), held(3)]);
        claimed(swapped, &[ClaimRule::UnstagedPage, ClaimRule::WrongOwner]);
        let mut short = base.clone();
        short.extend([dispatched(1, prepare_of(1, owner(), 3)), held(2)]);
        claimed(short, &[ClaimRule::UnstagedPage, ClaimRule::WrongPayload]);
        // The staged tip is unknown, so a prepare narrowed to any tip, with
        // any headers, passes.
        let mut narrowed = owner();
        if let HeaderSyncWorkOwner::Header(header) = &mut narrowed {
            header.authority.branch.target_tip_hash = block::Hash([4; 32]);
        }
        let mut fine = base;
        fine.extend([dispatched(1, prepare_of(1, narrowed, 3)), held(3)]);
        claimed(fine, &[ClaimRule::UnstagedPage]);
    }

    /// When the queue releases a round, its peer holds no reservation, the
    /// queue holds none of its leases, and its unheld leases have returned. A
    /// held lease waits for its call, but no longer than its `Handled`. Each
    /// is a claim rule, so the ledger records the break and goes on.
    #[test]
    fn a_released_round_keeps_nothing_no_call_holds() {
        let base = vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 3, 2, 3),
        ];
        let mut clean = base.clone();
        clean.extend([
            reservation_ended(1, 2, ReservationEnd::Released),
            returned(1, 1, 3),
            released(1, 1),
            retired(1, 1),
        ]);
        fold(clean)
            .expect("the round retires once the queue released it")
            .finish()
            .expect("nothing stays reserved or owned");
        let mut leaked = base.clone();
        leaked.push(released(1, 1));
        let leaked = claimed_as(leaked, ClaimRule::LeakedReservation);
        assert_eq!(leaked.unsettled_rounds(), 1, "the round is still released");
        let mut kept_lease = base.clone();
        kept_lease.extend([
            reservation_ended(1, 2, ReservationEnd::Released),
            released(1, 1),
        ]);
        claimed_as(kept_lease, ClaimRule::LeakedLease);
        let mut twice = base.clone();
        twice.extend([
            reservation_ended(1, 2, ReservationEnd::Released),
            returned(1, 1, 3),
            released(1, 1),
            released(1, 1),
        ]);
        refused_as(twice, "UnknownRound");
        let mut queue_kept = base.clone();
        queue_kept.extend([
            reservation_ended(1, 2, ReservationEnd::Released),
            ReactorEvent::RoundReleased {
                peer: peer(1),
                incarnation: 1,
                queue_owned: 3,
            },
        ]);
        claimed_as(queue_kept, ClaimRule::LeakedLease);

        let mut held = base;
        held.extend([
            reservation_ended(1, 2, ReservationEnd::Released),
            dispatched(1, prepare_of(1, owner(), 3)),
            ReactorEvent::HeadersHeld {
                call: 1,
                kind: StorageCallKind::Prepare,
                peer: peer(1),
                incarnation: 1,
                headers: 3,
                leases: Vec::new(),
            },
        ]);
        let mut early = held.clone();
        early.push(returned(1, 1, 3));
        refused_as(early, "LeaseReturnedEarly");
        held.extend([
            released(1, 1),
            retired(1, 1),
            ReactorEvent::CallEnded {
                call: 1,
                end: CallEnd::Replied,
            },
            ReactorEvent::HeadersEnded {
                call: 1,
                incarnation: 1,
                headers: 3,
                end: HeaderWorkEnd::Stale,
            },
        ]);
        let mut unreturned = held.clone();
        unreturned.push(ReactorEvent::Handled {
            call: 1,
            panicked: false,
        });
        claimed_as(unreturned, ClaimRule::LeakedLease);
        // Handled without `HeadersEnded`: the call still holds the headers.
        let mut still_held = held[..held.len() - 1].to_vec();
        still_held.push(ReactorEvent::Handled {
            call: 1,
            panicked: false,
        });
        claimed_as(still_held, ClaimRule::OpenOwnership);
        // The leases are back, but the call that held them isn't handled
        // yet, so the round hasn't settled.
        let mut unhandled = held.clone();
        unhandled.push(returned(1, 1, 3));
        let unhandled = fold(unhandled).expect("the call's leases return before it's handled");
        assert_eq!(unhandled.unsettled_rounds(), 1);
        held.extend([
            returned(1, 1, 3),
            ReactorEvent::Handled {
                call: 1,
                panicked: false,
            },
        ]);
        let mut settled =
            fold(held).expect("the call's leases return before its completion is handled");
        assert_eq!(settled.released_rounds(), 1);
        assert_eq!(settled.unsettled_rounds(), 0);
        settled.finish().expect("nothing stays reserved or owned");
    }

    /// A released round is stuck once no call that holds its headers is
    /// still unended, whatever another round's calls do: round 1's prepare
    /// ended and was never handled, while round 2's prepare is still out,
    /// so only round 1 is stuck until round 2's ends too.
    #[test]
    fn each_released_round_is_stuck_by_its_own_calls() {
        let mut events = after(&[(1, 1), (2, 2)], Vec::new());
        for (call, byte) in [(1, 1), (2, 2)] {
            let incarnation = u64::from(byte);
            events.extend([
                dispatched(call, prepare_of(byte, owner(), 5)),
                held(call, StorageCallKind::Prepare, byte, incarnation),
                released(byte, incarnation),
                retired(byte, incarnation),
            ]);
        }
        events.push(ReactorEvent::CallEnded {
            call: 1,
            end: CallEnd::Replied,
        });
        let ledger = fold(events.clone()).expect("both rounds release while their calls hold them");
        assert_eq!(ledger.unsettled_rounds(), 2);
        assert_eq!(ledger.stuck_rounds(), 1);
        events.push(ReactorEvent::CallEnded {
            call: 2,
            end: CallEnd::Replied,
        });
        let ledger = fold(events).expect("the second call ends");
        assert_eq!(ledger.stuck_rounds(), 2);
    }

    /// A reservation fits its round's request limit: a 401-header repair for
    /// a 401-header request passes, one of 402 doesn't, and neither does a
    /// continuation above its round's limit.
    #[test]
    fn a_reservation_fits_its_rounds_request_limit() {
        let repair = |headers| ReactorEvent::ReservationOpened {
            reservation: 1,
            peer: peer(1),
            purpose: ReservationPurpose::Repair,
            headers,
            request: None,
        };
        let bound_to = |max_headers| ReactorEvent::ReservationBound {
            reservation: 1,
            request: request(1, 1),
            max_headers,
        };
        let repairing = ReactorEvent::Started {
            request: request(1, 1),
            purpose: crate::zakura::header_sync::HeaderTargetPurpose::SelectedAuxiliaryRepair {
                selected_target: zakura_header_chain::Frontier::new(block::Height(5), TIP),
                repair_generation: 1,
            },
            locator: Vec::new(),
        };
        fold(vec![repair(401), bound_to(401), repairing.clone()])
            .expect("a repair may reserve its whole request");
        refused_as(
            vec![repair(402), bound_to(401), repairing.clone()],
            "OverRequestLimit",
        );
        refused_as(vec![repair(5), bound_to(5), started(1, 1)], "WrongPurpose");
        refused_as(
            vec![opened(1, 1, 5), bound_to(5), repairing],
            "WrongPurpose",
        );
        let mut over = vec![
            opened(1, 1, 5),
            bound_to(5),
            started(1, 1),
            staged(1, 1, 1, 5, 0, 5),
            reservation_ended(1, 0, ReservationEnd::Staged),
        ];
        over.push(ReactorEvent::ReservationOpened {
            reservation: 2,
            peer: peer(1),
            purpose: ReservationPurpose::Ordinary,
            headers: 6,
            request: Some(request(1, 1)),
        });
        refused_as(over, "OverRequestLimit");
    }

    /// A binding, a continuation and a page must name exactly the request
    /// that started the round: one whose owner changed after `Started`
    /// fails.
    #[test]
    fn the_rounds_starting_request_names_all_its_work() {
        let mut changed = request(1, 1);
        changed.request_id = 2;
        let base = vec![opened(1, 1, 5), bound(1, 1, 1), started(1, 1)];
        let mut page = base.clone();
        page.push(ReactorEvent::PageStaged {
            from_peer: peer(1),
            from_session: 1,
            request: changed.clone(),
            reservation: 1,
            subscription: 2,
            frame: None,
            headers: 2,
            reserved_left: 3,
            ancestor: zakura_header_chain::Frontier::new(block::Height(0), TIP),
            hashes: vec![TIP; 2],
            stored: vec![TIP; 2],
            staged_total: 2,
        });
        refused_as(page, "UnknownRound");
        let mut continuation = base;
        continuation.extend([
            staged(1, 1, 1, 5, 0, 5),
            reservation_ended(1, 0, ReservationEnd::Staged),
            ReactorEvent::ReservationOpened {
                reservation: 2,
                peer: peer(1),
                purpose: ReservationPurpose::Ordinary,
                headers: 5,
                request: Some(changed.clone()),
            },
        ]);
        refused_as(continuation, "UnknownRound");
        refused_as(
            vec![
                opened(1, 1, 5),
                bound(1, 1, 1),
                ReactorEvent::Started {
                    request: changed,
                    purpose: crate::zakura::header_sync::HeaderTargetPurpose::Normal,
                    locator: Vec::new(),
                },
            ],
            "UnreservedStart",
        );
    }

    /// A prepare may narrow its owner to the round's staged tip, but not to
    /// any other target, and the round's calls together hold at most what it
    /// owns.
    #[test]
    fn a_prepare_narrows_only_to_its_staged_tip_within_what_it_owns() {
        let base = vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 3, 2, 3),
        ];
        let toward = |target| {
            let mut owner = owner();
            if let HeaderSyncWorkOwner::Header(header) = &mut owner {
                header.authority.branch.target_tip_hash = target;
            }
            prepare_of(1, owner, 3)
        };
        let hold = |call, headers| ReactorEvent::HeadersHeld {
            call,
            kind: StorageCallKind::Prepare,
            peer: peer(1),
            incarnation: 1,
            headers,
            leases: Vec::new(),
        };
        let mut narrowed = base.clone();
        narrowed.extend([dispatched(1, toward(TIP)), hold(1, 3)]);
        fold(narrowed).expect("a prepare narrowed to the staged tip holds the round");
        let mut elsewhere = base.clone();
        elsewhere.extend([dispatched(1, toward(block::Hash([8; 32]))), hold(1, 3)]);
        claimed_as(elsewhere, ClaimRule::WrongOwner);
        let mut doubled = base;
        doubled.extend([
            dispatched(1, prepare_of(1, owner(), 3)),
            hold(1, 3),
            dispatched(2, prepare_of(1, owner(), 3)),
            hold(2, 3),
        ]);
        claimed_as(doubled, ClaimRule::HeldUnowned);
    }

    /// A prepare that names another ancestor than its round's first page or
    /// another source than its peer's, and an apply that pursues another tip
    /// than its prepare, are claim breaks.
    #[test]
    fn a_handoff_keeps_its_ancestor_source_and_tip() {
        let base = vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 3, 2, 3),
        ];
        let hold = ReactorEvent::HeadersHeld {
            call: 1,
            kind: StorageCallKind::Prepare,
            peer: peer(1),
            incarnation: 1,
            headers: 3,
            leases: Vec::new(),
        };
        let with = |change: &dyn Fn(&mut PortCall)| {
            let mut prepare = prepare_of(1, owner(), 3);
            change(&mut prepare);
            let mut events = base.clone();
            events.extend([dispatched(1, prepare), hold.clone()]);
            events
        };
        let honest = with(&|_| {});
        let mut ledger = fold(honest.clone()).expect("an honest prepare folds");
        assert_eq!(ledger.take_claims(), Vec::new());
        claimed_as(
            with(&|prepare| {
                if let PortCall::Prepare {
                    common_ancestor, ..
                } = prepare
                {
                    *common_ancestor =
                        zakura_header_chain::Frontier::new(block::Height(1), block::Hash([8; 32]));
                }
            }),
            ClaimRule::WrongPayload,
        );
        claimed_as(
            with(&|prepare| {
                if let PortCall::Prepare { source, .. } = prepare {
                    *source = SourceId::from_digest([2; 32]);
                }
            }),
            ClaimRule::WrongSource,
        );
        let mut retargeted = honest;
        let mut apply = apply(1);
        if let PortCall::Apply {
            target_tip_hash, ..
        } = &mut apply
        {
            *target_tip_hash = block::Hash([8; 32]);
        }
        retargeted.extend([
            dispatched(2, apply),
            ReactorEvent::HeadersEnded {
                call: 1,
                incarnation: 1,
                headers: 3,
                end: HeaderWorkEnd::HandedToApply { apply: 2 },
            },
        ]);
        claimed_as(retargeted, ClaimRule::BadHandoff);
    }

    /// A prepare sends exactly its round's staged headers, through the staged
    /// tip, and a lease returns to the round of its own peer.
    #[test]
    fn a_prepare_sends_its_rounds_payload_and_leases_return_to_it() {
        let base = vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 3, 2, 3),
        ];
        let hold = ReactorEvent::HeadersHeld {
            call: 1,
            kind: StorageCallKind::Prepare,
            peer: peer(1),
            incarnation: 1,
            headers: 3,
            leases: Vec::new(),
        };
        let mut short = base.clone();
        short.extend([dispatched(1, prepare_of(1, owner(), 2)), hold.clone()]);
        claimed_as(short, ClaimRule::WrongPayload);
        let mut elsewhere = base.clone();
        elsewhere.extend([
            dispatched(
                1,
                PortCall::Prepare {
                    peer: peer(1),
                    source: SourceId::from_digest([1; 32]),
                    owner: owner(),
                    common_ancestor: zakura_header_chain::Frontier::new(block::Height(0), TIP),
                    target: zakura_header_chain::Frontier::new(
                        block::Height(5),
                        block::Hash([8; 32]),
                    ),
                    hashes: vec![block::Hash([8; 32]); 3],
                },
            ),
            hold.clone(),
        ]);
        claimed_as(elsewhere, ClaimRule::WrongPayload);
        // The same count toward and ending at the staged tip, but another
        // header in the middle.
        let mut substituted = base.clone();
        substituted.extend([
            dispatched(
                1,
                PortCall::Prepare {
                    peer: peer(1),
                    source: SourceId::from_digest([1; 32]),
                    owner: owner(),
                    common_ancestor: zakura_header_chain::Frontier::new(block::Height(0), TIP),
                    target: zakura_header_chain::Frontier::new(block::Height(5), TIP),
                    hashes: vec![TIP, block::Hash([8; 32]), TIP],
                },
            ),
            hold,
        ]);
        claimed_as(substituted, ClaimRule::WrongPayload);
        let mut foreign = base;
        foreign.extend([
            reservation_ended(1, 2, ReservationEnd::Released),
            returned(2, 1, 3),
        ]);
        refused_as(foreign, "UnknownRound");
    }

    /// A claim taints the count it made unreliable: the checks that read
    /// it don't judge, the round never settles or counts as stuck, and the
    /// totals aren't production's. A check that reads no tainted count,
    /// here the leases of a round whose reservation was tainted, still
    /// judges.
    #[test]
    fn a_claim_taints_only_the_counts_it_made_unreliable() {
        let base = vec![opened(1, 1, 5), bound(1, 1, 1), started(1, 1)];
        // The page reports 2 left of the 3 its reservation still held.
        let mut over = base.clone();
        over.extend([
            staged(1, 1, 1, 2, 2, 2),
            reservation_ended(1, 2, ReservationEnd::Released),
            ReactorEvent::RoundReleased {
                peer: peer(1),
                incarnation: 1,
                queue_owned: 2,
            },
        ]);
        let mut ledger = fold(over).expect("the claim's dependent checks don't judge");
        let rules: Vec<ClaimRule> = ledger.take_claims().iter().map(|c| c.rule).collect();
        // The round's own counts aren't tainted, so its queue-held lease
        // and its unreturned staged headers are both still judged.
        assert_eq!(
            rules,
            [
                ClaimRule::OverReservation,
                ClaimRule::LeakedLease,
                ClaimRule::LeakedLease
            ],
        );

        // The round's report holds 1 header of the 3 it staged, and then
        // returns all 3.
        let mut unstaged = base;
        unstaged.extend([
            staged(1, 1, 1, 3, 2, 1),
            reservation_ended(1, 2, ReservationEnd::Released),
            released(1, 1),
            returned(1, 1, 3),
            returned(1, 1, 1),
        ]);
        let mut ledger = fold(unstaged).expect("the claim's dependent checks don't judge");
        let rules: Vec<ClaimRule> = ledger.take_claims().iter().map(|c| c.rule).collect();
        assert_eq!(rules, [ClaimRule::UnstagedPage]);
        assert!(ledger.accounting_tainted());
        assert_eq!(
            ledger.unsettled_rounds(),
            1,
            "a tainted round never settles"
        );
        assert_eq!(ledger.stuck_rounds(), 0, "nor is it judged stuck");
        ledger.finish().expect("teardown is clean");
        assert_eq!(ledger.take_claims(), []);
    }

    /// A call claim taints the rounds it implicates, so their later
    /// settlement doesn't fail as an integrity rule. A call holding more
    /// than its round owns returns all it held, and its round never settles
    /// or counts as stuck. A call holding another round than its peer's
    /// current one keeps it while that round's lease returns.
    #[test]
    fn a_call_claim_taints_the_rounds_it_implicates() {
        let mut unowned = vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 3, 2, 3),
            reservation_ended(1, 2, ReservationEnd::Released),
            dispatched(1, apply(1)),
            ReactorEvent::HeadersHeld {
                call: 1,
                kind: StorageCallKind::Apply,
                peer: peer(1),
                incarnation: 1,
                headers: 4,
                leases: Vec::new(),
            },
            released(1, 1),
        ];
        unowned.extend([
            ReactorEvent::HeadersEnded {
                call: 1,
                incarnation: 1,
                headers: 4,
                end: HeaderWorkEnd::Refused,
            },
            returned(1, 1, 4),
            ReactorEvent::CallEnded {
                call: 1,
                end: CallEnd::Replied,
            },
            ReactorEvent::Handled {
                call: 1,
                panicked: false,
            },
            retired(1, 1),
        ]);
        let mut ledger = fold(unowned).expect("the claim's dependent checks don't judge");
        let rules: Vec<_> = ledger.take_claims().iter().map(|c| c.rule).collect();
        assert_eq!(rules, [ClaimRule::HeldUnowned]);
        assert_eq!(
            ledger.unsettled_rounds(),
            1,
            "a tainted round never settles"
        );
        assert_eq!(ledger.stuck_rounds(), 0, "nor is it judged stuck");
        ledger.finish().expect("teardown is clean");
        assert_eq!(ledger.take_claims(), []);

        let mut ledger = fold(after(
            &[(1, 1), (2, 2)],
            vec![
                dispatched(1, prepare(1)),
                held(1, StorageCallKind::Prepare, 1, 2),
                released(2, 2),
                returned(2, 2, 5),
            ],
        ))
        .expect("the claim's dependent checks don't judge");
        let rules: Vec<_> = ledger.take_claims().iter().map(|c| c.rule).collect();
        assert_eq!(rules.first(), Some(&ClaimRule::WrongRound), "{rules:?}");
        assert_eq!(ledger.stuck_rounds(), 0);
    }

    /// A staging report must show its page in the round: one whose policy
    /// holds fewer headers than the round staged is a claim break.
    #[test]
    fn a_staged_page_is_in_its_round() {
        claimed_as(
            vec![
                opened(1, 1, 5),
                bound(1, 1, 1),
                started(1, 1),
                staged(1, 1, 1, 3, 2, 0),
            ],
            ClaimRule::UnstagedPage,
        );
    }

    /// A page whose round stored other headers where it went fails, as does
    /// one that left an earlier page's header changed, and so does a call
    /// holding a lease staged for another round.
    #[test]
    fn a_round_stores_its_pages_and_holds_its_own_leases() {
        let base = vec![opened(1, 1, 5), bound(1, 1, 1), started(1, 1)];
        let mut misstored = base.clone();
        misstored.push(ReactorEvent::PageStaged {
            from_peer: peer(1),
            from_session: 1,
            request: request(1, 1),
            reservation: 1,
            subscription: 1,
            frame: None,
            headers: 2,
            reserved_left: 3,
            ancestor: zakura_header_chain::Frontier::new(block::Height(0), TIP),
            hashes: vec![TIP; 2],
            stored: vec![TIP, block::Hash([8; 32])],
            staged_total: 2,
        });
        claimed_as(misstored, ClaimRule::UnstagedPage);
        let mut rewritten = base.clone();
        rewritten.extend([
            staged(1, 1, 1, 2, 3, 2),
            ReactorEvent::PageStaged {
                from_peer: peer(1),
                from_session: 1,
                request: request(1, 1),
                reservation: 1,
                subscription: 1,
                frame: None,
                headers: 1,
                reserved_left: 2,
                ancestor: zakura_header_chain::Frontier::new(block::Height(0), TIP),
                hashes: vec![TIP],
                stored: vec![TIP, block::Hash([8; 32]), TIP],
                staged_total: 3,
            },
        ]);
        claimed_as(rewritten, ClaimRule::UnstagedPage);
        let mut foreign = base;
        foreign.extend([
            staged(1, 1, 1, 3, 2, 3),
            dispatched(1, prepare_of(1, owner(), 3)),
            ReactorEvent::HeadersHeld {
                call: 1,
                kind: StorageCallKind::Prepare,
                peer: peer(1),
                incarnation: 1,
                headers: 3,
                leases: vec![1, 2],
            },
        ]);
        claimed_as(foreign, ClaimRule::ForeignLease);
        // A claimed foreign lease leaves both rounds' settlement unjudged,
        // so neither a release, an early return nor a leak follows it.
        let mut ledger = fold(after(
            &[(1, 1), (2, 2)],
            vec![
                dispatched(1, prepare(1)),
                ReactorEvent::HeadersHeld {
                    call: 1,
                    kind: StorageCallKind::Prepare,
                    peer: peer(1),
                    incarnation: 1,
                    headers: 5,
                    leases: vec![1, 2],
                },
                ReactorEvent::RoundReleased {
                    peer: peer(2),
                    incarnation: 2,
                    queue_owned: 0,
                },
            ],
        ))
        .expect("a claimed foreign lease doesn't refuse the history");
        let rules: Vec<_> = ledger
            .take_claims()
            .iter()
            .map(|claim| claim.rule)
            .collect();
        assert_eq!(rules, [ClaimRule::ForeignLease]);
        for after_hold in [
            // Round 1's call holds only round 2's lease, and round 1's own
            // lease returns while the call is pending.
            vec![ReactorEvent::LeaseReturned {
                peer: peer(1),
                incarnation: 1,
                headers: 5,
            }],
            // Round 2 releases and returns its lease while round 1's call
            // still holds one of round 2's.
            vec![
                ReactorEvent::RoundReleased {
                    peer: peer(2),
                    incarnation: 2,
                    queue_owned: 0,
                },
                ReactorEvent::LeaseReturned {
                    peer: peer(2),
                    incarnation: 2,
                    headers: 5,
                },
            ],
        ] {
            let mut events = vec![
                dispatched(1, prepare(1)),
                ReactorEvent::HeadersHeld {
                    call: 1,
                    kind: StorageCallKind::Prepare,
                    peer: peer(1),
                    incarnation: 1,
                    headers: 5,
                    leases: vec![2],
                },
            ];
            events.extend(after_hold);
            let mut ledger = fold(after(&[(1, 1), (2, 2)], events))
                .expect("a claimed foreign lease doesn't refuse the history");
            let rules: Vec<_> = ledger
                .take_claims()
                .iter()
                .map(|claim| claim.rule)
                .collect();
            assert_eq!(rules, [ClaimRule::ForeignLease]);
            assert_eq!(ledger.stuck_rounds(), 0);
        }
    }

    /// A prepare or apply that ends without holding its round's headers
    /// fails, and so does one teardown drops that never held them.
    #[test]
    fn every_prepare_and_apply_holds_its_rounds_headers() {
        let base = vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 5, 0, 5),
            reservation_ended(1, 0, ReservationEnd::Staged),
            dispatched(1, prepare(1)),
        ];
        let mut ended = base.clone();
        ended.push(ReactorEvent::CallEnded {
            call: 1,
            end: CallEnd::Replied,
        });
        refused_as(ended, "UnheldCall");
        let mut dropped = base;
        dropped.push(ReactorEvent::CallEnded {
            call: 1,
            end: CallEnd::Dropped,
        });
        let mut ledger = fold(dropped).expect("a dropped call is checked at teardown");
        assert!(ledger
            .finish()
            .expect_err("the call never held its headers")
            .starts_with("UnheldCall"));
    }

    /// A round that retires for its session's departure or its timeout while
    /// the queue still holds it is the retirement property's claim break,
    /// and the ledger goes on.
    #[test]
    fn an_unreleased_departed_round_is_a_claim() {
        use super::super::HeaderRequestTerminal::{Disconnected, SessionReplaced, TimedOut};
        for terminal in [Disconnected, SessionReplaced, TimedOut] {
            let mut departed = retired(1, 1);
            if let ReactorEvent::Retired { reason, .. } = &mut departed {
                *reason = terminal;
            }
            let mut ledger = fold(vec![
                opened(1, 1, 5),
                bound(1, 1, 1),
                started(1, 1),
                staged(1, 1, 1, 3, 2, 3),
                departed,
            ])
            .expect("a departed round's early retirement is a claim break");
            let claims = ledger.take_claims();
            assert_eq!(claims.len(), 1, "{terminal:?}: {claims:?}");
            assert_eq!(claims[0].rule, ClaimRule::UnreleasedDepartedRound);
            assert!(
                claims[0].detail.starts_with("UnreleasedDepartedRound"),
                "{claims:?}"
            );
            ledger
                .observe(&ReactorEvent::RoundReleased {
                    peer: peer(1),
                    incarnation: 1,
                    queue_owned: 0,
                })
                .expect("the round's release still applies");
        }
    }

    /// An obsolete round that retires while the queue still holds it is a
    /// claim break, not a refusal, and the ledger goes on: the queue's later
    /// release of the round still applies.
    #[test]
    fn an_unreleased_obsolete_round_is_a_claim() {
        let mut obsolete = retired(1, 1);
        if let ReactorEvent::Retired { reason, .. } = &mut obsolete {
            *reason = super::super::HeaderRequestTerminal::SnapshotObsolete;
        }
        let mut ledger = fold(vec![
            opened(1, 1, 5),
            bound(1, 1, 1),
            started(1, 1),
            staged(1, 1, 1, 3, 2, 3),
            obsolete,
        ])
        .expect("an obsolete round's early retirement is a claim break");
        let claims = ledger.take_claims();
        assert_eq!(claims.len(), 1, "{claims:?}");
        assert_eq!(claims[0].rule, ClaimRule::UnreleasedObsoleteRound);
        assert!(
            claims[0].detail.starts_with("UnreleasedObsoleteRound"),
            "{claims:?}"
        );
        ledger
            .observe(&ReactorEvent::RoundReleased {
                peer: peer(1),
                incarnation: 1,
                queue_owned: 0,
            })
            .expect("the round's release still applies");
    }

    /// A round retired for a reason no other property owns, a shutdown
    /// included, while the queue still holds it, is the outstanding-work
    /// claim, and the ledger goes on.
    #[test]
    fn an_unreleased_round_for_another_reason_is_a_claim() {
        use super::super::HeaderRequestTerminal::{Shutdown, TargetRejected};
        for terminal in [TargetRejected, Shutdown] {
            let mut ended = retired(1, 1);
            if let ReactorEvent::Retired { reason, .. } = &mut ended {
                *reason = terminal;
            }
            let mut ledger = fold(vec![
                opened(1, 1, 5),
                bound(1, 1, 1),
                started(1, 1),
                staged(1, 1, 1, 3, 2, 3),
                ended,
            ])
            .expect("the round's early retirement is a claim break");
            let claims = ledger.take_claims();
            assert_eq!(claims.len(), 1, "{terminal:?}: {claims:?}");
            assert_eq!(claims[0].rule, ClaimRule::UnreleasedRound);
            assert!(
                claims[0].detail.starts_with("UnreleasedRound"),
                "{claims:?}"
            );
            ledger
                .observe(&ReactorEvent::RoundReleased {
                    peer: peer(1),
                    incarnation: 1,
                    queue_owned: 0,
                })
                .expect("the round's release still applies");
        }
    }

    /// At teardown every reservation has ended, which is a claim rule.
    #[test]
    fn teardown_leaves_no_reservation() {
        let mut ledger = fold(vec![opened(1, 1, 5)]).expect("an open reservation folds");
        ledger
            .finish()
            .expect("an open reservation is a claim break");
        let claims = ledger.take_claims();
        assert_eq!(claims.len(), 1, "{claims:?}");
        assert_eq!(claims[0].rule, ClaimRule::OpenReservation);
        assert!(
            claims[0].detail.starts_with("OpenReservation"),
            "{claims:?}"
        );
    }
}
