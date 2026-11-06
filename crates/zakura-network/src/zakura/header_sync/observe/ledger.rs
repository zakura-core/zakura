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

use std::collections::BTreeMap;

use zakura_chain::block;
use zakura_header_chain::HeaderSyncWorkOwner;
use zakura_rt::collections::HashMap;

use super::{
    super::scheduler::peer_work::{
        HEADER_CHUNK_BUDGET_CAPACITY_V1, MAX_HEADER_CHUNK_RESERVATION_V1,
    },
    super::HeaderTargetPurpose,
    CallEnd, HeaderWorkEnd, PortCall, ReactorEvent, RequestRef, ReservationEnd, ReservationPurpose,
    StorageCallKind,
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
}

impl CallRecord {
    /// Whether this is a prepare or an apply that never held headers, which
    /// each takes from its round as it's dispatched.
    fn holds_nothing(&self) -> bool {
        matches!(self.op, PortCall::Prepare { .. } | PortCall::Apply { .. }) && self.held.is_none()
    }
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
    /// The hashes the round stored where the page went.
    stored: &'a [block::Hash],
    /// How many headers the round held after the page.
    staged_total: usize,
}

/// What a prepare sends: the target it names and the hashes of the headers
/// it actually sends.
#[derive(Clone, Debug)]
struct Payload {
    target: block::Hash,
    hashes: Vec<block::Hash>,
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
    /// Checks `event`, and returns the broken rule's name and detail.
    pub fn observe(&mut self, event: &ReactorEvent) -> Result<(), String> {
        match event {
            ReactorEvent::Dispatched { call, op } => {
                let record = CallRecord {
                    op: op.clone(),
                    end: None,
                    held: None,
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
                if record.held.as_ref().is_some_and(|held| !held.ended) {
                    return Err(format!(
                        "OpenOwnership: call {call} was handled with its headers still held"
                    ));
                }
                let held_round = record.held.as_ref().map(|held| held.incarnation);
                if let Some(handoff) = self.handoff.as_ref().filter(|h| h.apply == *call) {
                    return Err(format!(
                        "UnresolvedHandoff: call {call} was handled before it took the headers \
                         call {} handed it",
                        handoff.prepare
                    ));
                }
                if let Some(incarnation) = held_round {
                    self.check_returned(incarnation, &format!("call {call} was handled"))?;
                }
            }
            ReactorEvent::Started { request, purpose } => self.start(request, purpose)?,
            ReactorEvent::Retired { request, .. } => {
                if self.rounds.get(&request.peer) == Some(&request.incarnation) {
                    self.rounds.remove(&request.peer);
                }
                if self
                    .started
                    .get(&request.incarnation)
                    .is_some_and(|round| !round.released)
                {
                    return Err(format!(
                        "UnreleasedRound: round {} retired while the queue still holds it",
                        request.incarnation
                    ));
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
                let live = self.reservation_mut(*reservation)?;
                let unstaged = live.opened - live.staged;
                if *released != unstaged || (*cause == ReservationEnd::Staged && *released != 0) {
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
                hashes,
                stored,
                staged_total,
                ..
            } => {
                if from_peer != &request.peer
                    || *from_session != request.session_id
                    || *subscription != request.request_id
                {
                    return Err(format!(
                        "ForeignPage: a page from session {from_session}, subscription \
                         {subscription} was staged on round {}, which session {} requested as \
                         {}",
                        request.incarnation, request.session_id, request.request_id
                    ));
                }
                self.stage(
                    *reservation,
                    request,
                    StagedHeaders {
                        headers: *headers,
                        reserved_left: *reserved_left,
                        hashes,
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
                if held || round.returned + headers > round.staged {
                    return Err(format!(
                        "LeaseReturnedEarly: round {incarnation} returned {headers} headers with \
                         {} of its {} staged returned, while a call holds it: {held}",
                        round.returned, round.staged
                    ));
                }
                round.returned += headers;
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
                    return Err(format!(
                        "LeakedReservation: the queue released round {incarnation} with its \
                         peer still holding reservation {reservation}"
                    ));
                }
                if *queue_owned != 0 {
                    return Err(format!(
                        "LeakedLease: the queue released round {incarnation} still holding \
                         leases of {queue_owned} headers for its peer"
                    ));
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
                self.check_returned(
                    *incarnation,
                    &format!("the queue released round {incarnation}"),
                )?;
            }
            ReactorEvent::HeadersHeld {
                call,
                kind,
                peer,
                incarnation,
                headers,
                leases,
            } => {
                if let Some(foreign) = leases.iter().find(|round| **round != *incarnation) {
                    return Err(format!(
                        "ForeignLease: call {call} holds round {incarnation} with a lease \
                         staged for round {foreign}"
                    ));
                }
                let current = self.rounds.get(peer).copied();
                let record = self.record_mut(*call)?;
                let (expected_kind, call_peer, owner, payload) = match &record.op {
                    PortCall::Prepare {
                        peer,
                        owner,
                        target,
                        hashes,
                        ..
                    } => (
                        StorageCallKind::Prepare,
                        peer,
                        *owner,
                        Some(Payload {
                            target: target.hash,
                            hashes: hashes.clone(),
                        }),
                    ),
                    PortCall::Apply { peer, owner, .. } => {
                        (StorageCallKind::Apply, peer, *owner, None)
                    }
                    other => {
                        return Err(format!(
                            "WrongCall: call {call} holds headers but asks for {other:?}"
                        ))
                    }
                };
                if expected_kind != *kind || call_peer != peer {
                    return Err(format!(
                        "WrongCall: call {call} names {kind:?} for another peer or kind"
                    ));
                }
                if current != Some(*incarnation) {
                    return Err(format!(
                        "WrongRound: call {call} holds round {incarnation}, but its peer's \
                         current round is {current:?}"
                    ));
                }
                if record.held.is_some() {
                    return Err(format!("HeldTwice: call {call} took headers again"));
                }
                if let Some(handoff) = self.handoff.take() {
                    if handoff.apply != *call
                        || handoff.incarnation != *incarnation
                        || handoff.headers != *headers
                    {
                        return Err(format!(
                            "BadHandoff: call {} handed call {} round {} with {} headers, and \
                             call {call} took round {incarnation} with {headers} next",
                            handoff.prepare, handoff.apply, handoff.incarnation, handoff.headers
                        ));
                    }
                }
                self.hold(*call, owner, *incarnation, *headers, payload)?;
                let record = self.record_mut(*call)?;
                record.held = Some(Held {
                    incarnation: *incarnation,
                    headers: *headers,
                    ended: false,
                });
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
                if held.incarnation != *incarnation || held.headers != *headers {
                    return Err(format!(
                        "WrongRound: call {call} held round {} with {} headers, and ended round \
                         {incarnation} with {headers}",
                        held.incarnation, held.headers
                    ));
                }
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
                    ..
                }),
                Some(PortCall::Apply {
                    peer: apply_peer,
                    source: apply_source,
                    owner: apply_owner,
                }),
            ) if peer == apply_peer && source == apply_source && owner == apply_owner => {
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
                 apply for the same work"
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
        if claimed + headers > HEADER_CHUNK_BUDGET_CAPACITY_V1 {
            return Err(format!(
                "OverBudget: reservation {reservation} holds {headers} headers with {claimed} \
                 already claimed of {HEADER_CHUNK_BUDGET_CAPACITY_V1}"
            ));
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
        let live = self.reservation_mut(reservation)?;
        if live.round != Some(request.incarnation) || live.peer != request.peer {
            return Err(format!(
                "WrongReservation: a page of round {} spent reservation {reservation}, which \
                 backs round {:?}",
                request.incarnation, live.round
            ));
        }
        let unstaged = live.opened - live.staged;
        if headers == 0 || headers > unstaged || reserved_left != unstaged - headers {
            return Err(format!(
                "OverReservation: a page of {headers} headers spent reservation {reservation}, \
                 which held {unstaged} and reports {reserved_left} left"
            ));
        }
        live.staged += headers;
        let round = self
            .started
            .get_mut(&request.incarnation)
            .expect("the round was found above");
        round.staged += headers;
        round.hashes.extend_from_slice(stored);
        if hashes.len() != headers || stored != hashes || staged_total != round.staged {
            return Err(format!(
                "UnstagedPage: a page of {headers} headers with {} hashes left round {} \
                 holding {staged_total} of the {} it staged, or holding other headers",
                hashes.len(),
                request.incarnation,
                round.staged
            ));
        }
        Ok(())
    }

    /// Requires call `call`, for `owner`, to hold `headers` more of round
    /// `incarnation` than its other calls do, within what the round owns,
    /// for the owner that started it or that owner narrowed to its staged
    /// tip.
    /// A prepare's `payload` must name the round's staged tip and send
    /// exactly its staged headers, as many as the call holds.
    fn hold(
        &self,
        call: u64,
        owner: HeaderSyncWorkOwner,
        incarnation: u64,
        headers: usize,
        payload: Option<Payload>,
    ) -> Result<(), String> {
        let Some(round) = self
            .started
            .get(&incarnation)
            .filter(|round| round.staged > 0)
        else {
            return Err(format!(
                "UnstagedHeaders: call {call} holds round {incarnation}, which staged no page"
            ));
        };
        if !same_work(owner, round.request.owner, round.hashes.last().copied()) {
            return Err(format!(
                "WrongOwner: call {call} holds round {incarnation} for {owner:?}, but its pages \
                 were staged for {:?}",
                round.request.owner
            ));
        }
        let tip = round.hashes.last().copied();
        if let Some(payload) = payload.filter(|payload| {
            tip != Some(payload.target)
                || payload.hashes != round.hashes
                || payload.hashes.len() != headers
        }) {
            return Err(format!(
                "WrongPayload: call {call} sends {} headers toward {:?} for round \
                 {incarnation}, which holds {headers} staged through {tip:?}, and they aren't \
                 its staged headers in order",
                payload.hashes.len(),
                payload.target
            ));
        }
        let held = self.held_by_round().get(&incarnation).copied().unwrap_or(0);
        let owned = round.staged - round.returned;
        if held + headers > owned {
            return Err(format!(
                "HeldUnowned: call {call} holds {headers} headers of round {incarnation}, whose \
                 calls already hold {held} of the {owned} it owns"
            ));
        }
        Ok(())
    }

    /// Requires every lease of `incarnation`, if the queue released it and
    /// no call holds it, to have returned by `when`.
    fn check_returned(&mut self, incarnation: u64, when: &str) -> Result<(), String> {
        if let Some(error) = self.unreturned(incarnation, when) {
            return Err(error);
        }
        self.prune(incarnation);
        Ok(())
    }

    /// The `LeakedLease` break if the queue released `incarnation` and, with
    /// no call holding it, it kept a lease at `when`.
    fn unreturned(&self, incarnation: u64, when: &str) -> Option<String> {
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
    /// returned.
    fn prune(&mut self, incarnation: u64) {
        if self
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

    /// The headers the queue's accounting holds reserved: each live
    /// reservation's unstaged ones.
    pub fn reserved(&self) -> usize {
        self.reservations
            .values()
            .map(|live| live.opened - live.staged)
            .sum()
    }

    /// The headers the queue's accounting holds owned: each round's staged
    /// headers that haven't returned.
    pub fn owned(&self) -> usize {
        self.started
            .values()
            .map(|round| round.staged - round.returned)
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

    /// Requires every call to have ended, every held header to have left its
    /// call, every handoff to have reached its apply, every reservation to
    /// have ended and every lease to have returned, once the reactor's
    /// teardown is done.
    pub fn finish(&self) -> Result<(), String> {
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
            if record.held.as_ref().is_some_and(|held| !held.ended) {
                return Err(format!(
                    "OpenOwnership: call {call} never ended its headers"
                ));
            }
        }
        if let Some(reservation) = self.reservations.keys().next() {
            return Err(format!(
                "OpenReservation: reservation {reservation} never ended"
            ));
        }
        if let Some(binding) = &self.binding {
            return Err(format!(
                "UnresolvedBinding: reservation {} was bound to round {}, which never started",
                binding.reservation, binding.request.incarnation
            ));
        }
        if let Some((incarnation, round)) = self
            .started
            .iter()
            .find(|(_, round)| round.returned != round.staged)
        {
            return Err(format!(
                "LeakedLease: teardown finished with {} of round {incarnation}'s {} staged \
                 headers unreturned",
                round.staged - round.returned,
                round.staged
            ));
        }
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
            target: zakura_header_chain::Frontier::new(block::Height(5), TIP),
            hashes: vec![TIP; headers],
        }
    }

    fn apply(byte: u8) -> PortCall {
        PortCall::Apply {
            peer: peer(byte),
            source: SourceId::from_digest([byte; 32]),
            owner: owner(),
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
            hashes: vec![TIP; headers],
            stored: vec![TIP; headers],
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

    #[test]
    fn a_prepare_handed_to_its_apply_folds_clean() {
        let ledger = fold(after(
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

    /// #1845: a capture naming a retired round fails.
    #[test]
    fn a_capture_must_name_its_peers_current_round() {
        refused_as(
            after(
                &[(1, 1), (2, 2)],
                vec![
                    dispatched(1, prepare(1)),
                    held(1, StorageCallKind::Prepare, 1, 2),
                ],
            ),
            "WrongRound",
        );
    }

    /// #1846: a handoff to the prepare's own call fails.
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
        refused_as(other_peer, "BadHandoff");
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

        let mut other_call = base.clone();
        other_call.push(dispatched(3, apply(1)));
        other_call.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        other_call.push(held(3, StorageCallKind::Apply, 1, 1));
        refused_as(other_call, "BadHandoff");

        let mut unresolved = base;
        unresolved.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        let ledger = fold(unresolved).expect("an open handoff folds");
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
        refused_as(wrong, "WrongRound");

        let open = fold(held_call).expect("a held call folds");
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
        refused_as(
            vec![
                repair(1, 1, HEADER_CHUNK_BUDGET_CAPACITY_V1 - 10),
                opened(2, 2, 11),
            ],
            "OverBudget",
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
            hashes: vec![TIP; 2],
            stored: vec![TIP; 2],
            staged_total: 2,
        });
        refused_as(foreign, "ForeignPage");
        let mut over = base.clone();
        over.push(staged(1, 1, 1, 6, 0, 6));
        refused_as(over, "OverReservation");
        let mut miscounted = base.clone();
        miscounted.push(staged(1, 1, 1, 2, 2, 2));
        refused_as(miscounted, "OverReservation");
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
        refused_as(swapped, "WrongOwner");
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
        refused_as(unowned, "HeldUnowned");
    }

    /// When the queue releases a round, its peer holds no reservation, the
    /// queue holds none of its leases, and its unheld leases have returned. A
    /// held lease waits for its call, but no longer than its `Handled`.
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
        refused_as(leaked, "LeakedReservation");
        let mut kept_lease = base.clone();
        kept_lease.extend([
            reservation_ended(1, 2, ReservationEnd::Released),
            released(1, 1),
        ]);
        refused_as(kept_lease, "LeakedLease");
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
        refused_as(queue_kept, "LeakedLease");

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
        refused_as(unreturned, "LeakedLease");
        held.extend([
            returned(1, 1, 3),
            ReactorEvent::Handled {
                call: 1,
                panicked: false,
            },
        ]);
        fold(held)
            .expect("the call's leases return before its completion is handled")
            .finish()
            .expect("nothing stays reserved or owned");
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
        refused_as(elsewhere, "WrongOwner");
        let mut doubled = base;
        doubled.extend([
            dispatched(1, prepare_of(1, owner(), 3)),
            hold(1, 3),
            dispatched(2, prepare_of(1, owner(), 3)),
            hold(2, 3),
        ]);
        refused_as(doubled, "HeldUnowned");
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
        refused_as(short, "WrongPayload");
        let mut elsewhere = base.clone();
        elsewhere.extend([
            dispatched(
                1,
                PortCall::Prepare {
                    peer: peer(1),
                    source: SourceId::from_digest([1; 32]),
                    owner: owner(),
                    target: zakura_header_chain::Frontier::new(
                        block::Height(5),
                        block::Hash([8; 32]),
                    ),
                    hashes: vec![block::Hash([8; 32]); 3],
                },
            ),
            hold.clone(),
        ]);
        refused_as(elsewhere, "WrongPayload");
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
                    target: zakura_header_chain::Frontier::new(block::Height(5), TIP),
                    hashes: vec![TIP, block::Hash([8; 32]), TIP],
                },
            ),
            hold,
        ]);
        refused_as(substituted, "WrongPayload");
        let mut foreign = base;
        foreign.extend([
            reservation_ended(1, 2, ReservationEnd::Released),
            returned(2, 1, 3),
        ]);
        refused_as(foreign, "UnknownRound");
    }

    /// A staging report must show its page in the round: one whose policy
    /// holds fewer headers than the round staged fails.
    #[test]
    fn a_staged_page_is_in_its_round() {
        refused_as(
            vec![
                opened(1, 1, 5),
                bound(1, 1, 1),
                started(1, 1),
                staged(1, 1, 1, 3, 2, 0),
            ],
            "UnstagedPage",
        );
    }

    /// A page whose round stored other headers where it went fails, and so
    /// does a call holding a lease staged for another round.
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
            hashes: vec![TIP; 2],
            stored: vec![TIP, block::Hash([8; 32])],
            staged_total: 2,
        });
        refused_as(misstored, "UnstagedPage");
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
        refused_as(foreign, "ForeignLease");
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
        let ledger = fold(dropped).expect("a dropped call is checked at teardown");
        assert!(ledger
            .finish()
            .expect_err("the call never held its headers")
            .starts_with("UnheldCall"));
    }

    /// A round retires only once the queue released it: a `Retired` for a
    /// round the queue still holds fails at once.
    #[test]
    fn a_round_retires_only_once_released() {
        refused_as(
            vec![
                opened(1, 1, 5),
                bound(1, 1, 1),
                started(1, 1),
                staged(1, 1, 1, 3, 2, 3),
                retired(1, 1),
            ],
            "UnreleasedRound",
        );
    }

    /// At teardown every reservation has ended.
    #[test]
    fn teardown_leaves_no_reservation() {
        let ledger = fold(vec![opened(1, 1, 5)]).expect("an open reservation folds");
        assert!(ledger
            .finish()
            .expect_err("the reservation never ended")
            .starts_with("OpenReservation"));
    }
}
