//! One session's machine: everything a peer routine decides.
//!
//! A [`PeerMachine`] owns its peer's [`PeerState`], its own reserve calls,
//! reservations, submissions and timers, and its place in the wanted
//! table's ring. What every routine shares, the wanted table and the
//! reactor, comes in as a [`Shared`] on each input, and the pool as the
//! snapshot the routine last read. An effect on another session, such as a
//! charge that closes the peer's other sessions or the go passing to the
//! next one, goes out as a [`NodeEffect`] for the caller to route.

use std::time::Duration;

use zakura_chain::transaction::{UnminedTx, UnminedTxId};
use zakura_node_services::mempool::port::{ChainTip, Change, RejectionScope, ReservationId};
use zakura_rt::collections::{HashMap, HashSet};

use super::{
    file,
    peer::{Hello, PeerState, Pending, Reserving},
    reactor::SessionAdmission,
    wanted::{Admission, Changes},
    CallId, CloseReason, EntryGen, Granted, HaveRoom, Logged, Mail, NodeEffect, Paid, PeerKey,
    PoolView, RelayEffect, SessionId, Shared, Time, Unreserved, Verdict, Violation, WaiterId,
    HAVE_RETRY, INITIAL_WINDOW, MAX_OUTSTANDING_BYTES, MAX_SERVES, MAX_TX_BYTES, PARK_COOLDOWN,
    PENDING_CAP, PENDING_PER_PEER, PENDING_SHARE,
};
use crate::zakura::tx_relay::wire::{have_payload_bound, MAX_HAVE_ENTRIES};

/// A reserve call on its way to the pool.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Call {
    /// The id it reserves for.
    pub(crate) id: UnminedTxId,
    /// The pool's tip in the snapshot the caller held when it made the call.
    pub(crate) tip: ChainTip,
}

/// Who submitted a body, from which snapshot.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Submitted {
    /// The session whose claim the submission holds until its verdict,
    /// even after the session ends.
    pub(crate) session: SessionId,
    /// The pool's tip in its snapshot at the check before submitting.
    pub(crate) tip: ChainTip,
    /// Whether its supplier's address was one of
    /// `network.unconditional_peers` when the body arrived. Such a body is
    /// never charged, even when its verdict comes after the session ended.
    pub(crate) exempt: bool,
}

/// One session's machine. See the module docs.
#[derive(Debug)]
pub(crate) struct PeerMachine {
    session: SessionId,
    now: Time,
    /// Everything about its peer.
    pub(crate) state: PeerState,
    /// Which id each of its reservations is for, outside submissions.
    pub(crate) reservations: HashMap<ReservationId, UnminedTxId>,
    /// Who submitted each of its bodies awaiting a verdict, from which
    /// snapshot.
    pub(crate) submitted_at: HashMap<WaiterId, Submitted>,
    /// Its reserve calls the pool has not answered.
    pub(crate) calls: HashMap<CallId, Call>,
    /// No held advert is due before this, so a step with nothing due skips
    /// looking at the held adverts. It may be early, after a held advert
    /// leaves another way, which costs one look.
    held_due: Option<Time>,
    /// What comes due with no input, apart from the wanted table's own
    /// times, the backoff and its peer's cooldown. See [`Timers`].
    timers: Timers,
    /// No `Have` goes before this: the last try found no room for every
    /// requested body, its last `Have` unwritten, or the byte rate short.
    have_due: Option<Time>,
    /// Where it reads the wanted table's ring from.
    ring_cursor: u64,
    /// Its peer's park let it in, so a stall closes its connection.
    readmitted: bool,
    /// It ended, and handed what it held to the table and the reactor.
    ended: bool,
}

impl PeerMachine {
    /// A session of `peer` opens. It gets the go once every older session
    /// of the peer ended. While the peer's park runs, it is parked at once
    /// and ends before it begins.
    pub(crate) fn connect(
        shared: &mut Shared,
        pool: &dyn PoolView,
        peer: PeerKey,
        now: Time,
    ) -> Self {
        let session = shared.new_session();
        let admission = shared.reactor.admit(peer, now);
        let mut machine = Self {
            session,
            now,
            state: PeerState::new(peer),
            reservations: HashMap::default(),
            submitted_at: HashMap::default(),
            calls: HashMap::default(),
            held_due: None,
            timers: Timers::default(),
            have_due: None,
            ring_cursor: shared.wanted.ring_end(),
            readmitted: admission == SessionAdmission::Readmitted,
            ended: false,
        };
        if admission == SessionAdmission::Parked {
            // It never joins the reactor, so it has nothing to hand back.
            machine.ended = true;
            shared.effects.push(RelayEffect::Park { session });
            return machine;
        }
        machine.enter(shared, pool, now);
        if shared.reactor.connect(peer, session) {
            machine.open(shared, pool);
        }
        machine.settle(shared, pool);
        machine
    }

    /// The session.
    pub(crate) fn session(&self) -> SessionId {
        self.session
    }

    /// Whether it ended.
    pub(crate) fn is_ended(&self) -> bool {
        self.ended
    }

    /// The time of its last input.
    #[cfg(zakura_sim)]
    pub(crate) fn clock(&self) -> Time {
        self.now
    }

    /// Whether its peer's park let it in, so a stall closes its connection.
    #[cfg(zakura_sim)]
    pub(crate) fn readmitted(&self) -> bool {
        self.readmitted
    }

    // Inputs from its peer.

    /// The peer's `Hello` arrived.
    pub(crate) fn hello(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        hello: Hello,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if self.ended {
            return;
        }
        if self.state.hello.is_some() {
            self.violation(shared, Violation::HelloTwice);
        } else if hello.serves == 0
            || hello.serves > MAX_SERVES
            || hello.max_tx_bytes > MAX_TX_BYTES
        {
            self.violation(shared, Violation::BadHello);
        } else {
            self.state.hello = Some(hello);
            self.state.window = INITIAL_WINDOW.min(hello.serves);
            // Announcements it made before can join the table now, if it
            // has the go.
            if shared.reactor.has_go(self.state.peer, self.session) {
                let waiting: Vec<(UnminedTxId, u32)> = std::mem::take(&mut self.state.waiting)
                    .into_iter()
                    .collect();
                self.take_ids(shared, pool, &waiting);
            }
        }
        self.settle(shared, pool);
    }

    /// A `Have` arrived with these ids and sizes.
    pub(crate) fn have(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        entries: &[(UnminedTxId, u32)],
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if self.ended {
            return;
        }
        let peer = self.state.peer;
        if shared.reactor.is_cooling(peer, self.now) {
            // A cooling peer's ids are not taken, but it still has them, so
            // none is announced back to it.
            let ids = entries.iter().map(|(id, _)| (*id, shared.short(*id)));
            self.state.mark_all_known(ids, pool.last_seq());
            return self.settle(shared, pool);
        }
        let hello = self.state.hello.is_some();
        if shared.reactor.has_go(peer, self.session) && hello {
            self.take_ids(shared, pool, entries);
        } else {
            // A session waiting for the go keeps its announcements to itself,
            // filtered and bounded as the pending set is, so one peer never
            // holds two shares of the table. So does one without a `Hello`,
            // which could never be asked, so it cannot keep an entry alive.
            let ids = entries.iter().map(|(id, _)| (*id, shared.short(*id)));
            self.state.mark_all_known(ids, pool.last_seq());
            let state = &mut self.state;
            for (id, size) in entries.iter().copied() {
                if pool.holds(&id) || size > MAX_TX_BYTES || state.waiting.contains_key(&id) {
                    continue;
                }
                if state.waiting.len() < PENDING_PER_PEER {
                    state.waiting.insert(id, size);
                } else {
                    state.overflow = state.overflow.saturating_add(1);
                }
            }
        }
        self.settle(shared, pool);
    }

    /// A `Tx` arrived answering the request for `id`, carrying `body`. Its
    /// size on the wire is the body's own size. `exempt` is whether the
    /// peer's address was one of `network.unconditional_peers` when the body
    /// arrived. See [`Submitted::exempt`].
    pub(crate) fn tx(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        id: UnminedTxId,
        body: UnminedTx,
        exempt: bool,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if !self.ended {
            self.receive(shared, pool, id, body, exempt);
        }
        self.settle(shared, pool);
    }

    /// A `Tx` for `id` with a body of `size` bytes arrived, not yet decoded.
    /// Returns whether to decode the body and hand it to [`Self::tx`]. An
    /// answer to nothing is a violation, and a late body or one from a peer
    /// that started cooling is dropped unread, all as [`Self::tx`] does,
    /// so the IO never decodes a body the machine would not read.
    pub(crate) fn tx_arrived(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        id: UnminedTxId,
        size: u32,
        now: Time,
    ) -> bool {
        self.enter(shared, pool, now);
        let read = !self.ended && self.check_arrival(shared, id, size);
        self.settle(shared, pool);
        read
    }

    /// The checks on a body that need only its id and size, in [`Self::tx`]'s
    /// order. Returns whether the body is still wanted. Otherwise the answer
    /// is settled: a violation, or dropped unread.
    fn check_arrival(&mut self, shared: &mut Shared, id: UnminedTxId, size: u32) -> bool {
        let Some(request) = self.state.requests.get(&id).copied() else {
            self.violation(shared, Violation::Unrequested);
            return false;
        };
        let peer = self.state.peer;
        let Some(reservation) = request.reservation else {
            // Late: the node stopped waiting. The body is dropped before it
            // is checked, with no penalty.
            self.state.requests.shift_remove(&id);
            self.state.reannounced.remove(&id);
            return false;
        };
        if shared.reactor.is_cooling(peer, self.now) {
            // The peer started cooling while the body was in flight. Dropped
            // unread, as the legacy adapter drops it.
            self.state.requests.shift_remove(&id);
            self.state.reannounced.remove(&id);
            self.reservations.remove(&reservation);
            shared.give_back(reservation);
            shared
                .wanted
                .release(id, request.generation, self.session, self.now, true);
            return false;
        }
        if size != request.size {
            self.violation(shared, Violation::WrongSize);
            return false;
        }
        true
    }

    /// Checks the body of `id` and, if it passes, submits it, exempt from
    /// charges if `exempt`.
    fn receive(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        id: UnminedTxId,
        body: UnminedTx,
        exempt: bool,
    ) {
        let seq = pool.last_seq();
        let size = u32::try_from(body.size()).unwrap_or(u32::MAX);
        let short = shared.short(id);
        if !self.check_arrival(shared, id, size) {
            return;
        }
        if body.id() != id {
            return self.violation(shared, Violation::WrongId);
        }
        let request = self
            .state
            .requests
            .shift_remove(&id)
            .expect("check_arrival passed, so the request is outstanding");
        let reservation = request
            .reservation
            .expect("check_arrival passed, so the request holds a reservation");
        self.state.reannounced.remove(&id);
        self.state.mark_known(id, short, seq);
        // The check before submitting passed: a request keeps its
        // reservation only while it holds the current claim. From here on
        // the body goes to the pool whatever is decided.
        let submitted = Submitted {
            session: self.session,
            tip: pool.tip(),
            exempt,
        };
        self.submit(shared, id, request.generation, reservation, body, submitted);
    }

    /// A `NotFound` arrived.
    pub(crate) fn not_found(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        id: UnminedTxId,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if self.ended {
            return;
        }
        let short = shared.short(id);
        let Some(request) = self.state.requests.shift_remove(&id) else {
            self.violation(shared, Violation::Unrequested);
            return self.settle(shared, pool);
        };
        // An old answer the peer has since contradicted by announcing the id
        // again leaves what it told this node alone.
        let contradicted = self.state.reannounced.remove(&id);
        if contradicted.is_none() {
            // The peer says it does not have it, so it is no longer one to
            // skip when the id is admitted.
            self.state.forget_known(short);
        }
        // Either way the peer had its try at this entry.
        self.tried(shared, id, request.generation);
        if let Some(reservation) = request.reservation {
            // The fetch ended without a body, which the pool logs. A
            // `NotFound` past the deadline held no slot, and the deadline
            // already shrank the window.
            self.state.not_admitted();
            self.reservations.remove(&reservation);
            shared
                .effects
                .push(RelayEffect::FetchFailed { reservation });
            if !shared.stopped {
                shared.wanted.room();
            }
            shared
                .wanted
                .release(id, request.generation, self.session, self.now, false);
        }
        match contradicted {
            // The announcement that overtook this answer is taken now, as
            // it would have been had it come after. One try per entry still
            // holds: if the entry lives on, this peer has tried it.
            Some(size) => self.take_ids(shared, pool, &[(id, size)]),
            // If the pool already holds the id, an admission this session
            // skipped while the request was out is announced now.
            None => self.reconsider_advert(shared, pool, id, request.admission_seen),
        }
        self.settle(shared, pool);
    }

    /// A `GetTxs` arrived.
    pub(crate) fn get_txs(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        id: UnminedTxId,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if self.ended {
            return;
        }
        let size = pool.size(&id);
        let Some(hello) = self.state.hello else {
            self.violation(shared, Violation::NoHelloFirst);
            return self.settle(shared, pool);
        };
        // A conformant peer counts a request outstanding at least as long
        // as we do: until its answer arrives, and we count it only until
        // the IO starts writing the answer.
        if self.state.serving.contains(&id) {
            self.violation(shared, Violation::RequestedTwice);
            return self.settle(shared, pool);
        }
        if self.state.serving.len() >= MAX_SERVES {
            self.violation(shared, Violation::TooManyRequests);
            return self.settle(shared, pool);
        }
        // Exact ids: this compares the authorizing data too. A body above the
        // requester's limit is never sent.
        let announced = self.state.announced.is_sent(&id);
        let servable =
            announced && pool.holds(&id) && size.is_some_and(|size| size <= hello.max_tx_bytes);
        if !servable && announced {
            // The answer withdraws the advert the peer asked from, so a later
            // admission is announced again. A queued one, of a later
            // admission, has not reached the peer yet and stays.
            self.state.announced.remove(&id);
        }
        let session = self.session;
        if servable {
            self.state.serving.insert(id);
            shared.effects.push(RelayEffect::Serve { session, id });
        } else {
            shared.effects.push(RelayEffect::NotFound { session, id });
        }
        self.settle(shared, pool);
    }

    /// The IO started writing its answer to the peer's request for `id`,
    /// the body or, if the pool let it go meanwhile, a `NotFound`. The
    /// request no longer counts against what our `Hello` serves.
    pub(crate) fn served(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        id: UnminedTxId,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        self.state.serving.remove(&id);
        self.settle(shared, pool);
    }

    /// A body [`Self::tx_arrived`] let the IO decode does not decode as one
    /// transaction: a broken wire rule.
    pub(crate) fn undecodable(&mut self, shared: &mut Shared, pool: &dyn PoolView, now: Time) {
        self.enter(shared, pool, now);
        if !self.ended {
            self.violation(shared, Violation::Undecodable);
        }
        self.settle(shared, pool);
    }

    /// The session ended: the peer left, the connection closed, or the
    /// session was replaced.
    pub(crate) fn end(&mut self, shared: &mut Shared, pool: &dyn PoolView, now: Time) {
        self.enter(shared, pool, now);
        self.end_session(shared);
    }

    // Inputs from the node and the pool.

    /// Time passed.
    pub(crate) fn tick(&mut self, shared: &mut Shared, pool: &dyn PoolView, now: Time) {
        self.enter(shared, pool, now);
        self.settle(shared, pool);
    }

    /// The pool answered this session's reserve call `call`. Once the
    /// session ended, the answer goes to the reactor, which holds its calls.
    pub(crate) fn reserve_answer(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        call: CallId,
        answer: Result<Granted, Unreserved>,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        match self.calls.remove(&call) {
            Some(made) if !self.ended => self.deliver(shared, pool, call, made, answer),
            // Otherwise it is not a call this session has open. A grant
            // still goes back, and a `Stopped` answer still says the core
            // stopped.
            _ => {
                if !(self.ended && shared.ended_call_answer(call, answer)) {
                    shared.answer_unknown(answer);
                }
            }
        }
        self.settle(shared, pool);
    }

    /// The pool's verdict on this session's submission `waiter`. Once the
    /// session ended, the verdict goes to the reactor, which holds its
    /// submissions.
    pub(crate) fn verdict(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        waiter: WaiterId,
        verdict: Verdict,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if self.ended {
            shared.ended_verdict(pool, waiter, verdict, self.now);
            return;
        }
        // Whether the window was full with this body still counted.
        let full = self.in_flight(shared) >= self.state.window;
        let Some((id, generation)) = self.state.submissions.remove(&waiter) else {
            return self.settle(shared, pool);
        };
        if verdict != Verdict::Admitted {
            self.state.not_admitted();
        } else if full {
            let serves = self
                .state
                .hello
                .map_or(INITIAL_WINDOW, |hello| hello.serves);
            self.state.window = self.state.window.saturating_add(1).min(serves);
        }
        let submitted = self
            .submitted_at
            .remove(&waiter)
            .expect("every submission records its submitter");
        // The supplier's sessions learn of the body again as the verdict
        // arrives, so however many ids its peer announced since, the skip set
        // still holds this one when the admission is read.
        let peer = self.state.peer;
        let short = shared.short(id);
        self.state.mark_known(id, short, pool.last_seq());
        shared.node.push(NodeEffect::Known {
            peer,
            id,
            except: Some(self.session),
        });
        shared.apply_verdict(pool, peer, id, generation, submitted, verdict, self.now);
        self.end_if_closed(shared);
        self.settle(shared, pool);
    }

    /// The pool cancelled the fetch under `reservation`: its token fired.
    pub(crate) fn fetch_cancelled(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        reservation: ReservationId,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if let Some(id) = self.reservations.get(&reservation).copied() {
            self.cancel_fetch(shared, id, reservation);
        }
        self.settle(shared, pool);
    }

    /// The pool published a batch, which is in `batch`, and `pool` is the
    /// pool after it. If the batch turned the pool on again, a session with
    /// the go opens again first, as a restart does.
    pub(crate) fn pool_changed(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        batch: &[Logged],
        activated: bool,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if self.ended {
            return;
        }
        if activated && shared.reactor.has_go(self.state.peer, self.session) {
            self.open(shared, pool);
        }
        self.read_log(shared, pool, batch);
        self.settle(shared, pool);
    }

    /// The IO lost its place in the pool's change log and read the whole
    /// pool again, which `pool` now is. A session with the go rebuilds from
    /// it, as one that falls off the log does: an advert it sent still
    /// stands for an id the pool holds, and what it finds is announced with
    /// the usual window and delay. A snapshot that turns the pool on opens
    /// it instead, with the whole pool.
    pub(crate) fn pool_resynced(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        activated: bool,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if self.ended {
            return;
        }
        // A snapshot that turns the pool on, such as the first one the
        // routine reads, opens a session with the go, as `pool_changed`
        // does, so every one announces the whole pool whatever its rank.
        if activated {
            if shared.reactor.has_go(self.state.peer, self.session) {
                self.open(shared, pool);
            }
        } else {
            self.rebuild(shared, pool);
        }
        self.settle(shared, pool);
    }

    /// This session's routine reports its connection's smoothed round-trip
    /// time. It counts at the next ranking.
    pub(crate) fn observe_rtt(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        rtt: Duration,
        now: Time,
    ) {
        self.enter(shared, pool, now);
        if !self.ended {
            shared
                .reactor
                .observe_rtt(self.state.peer, self.session, rtt);
        }
        self.settle(shared, pool);
    }

    /// Reads what another session's step left it, before its own next input.
    pub(crate) fn read_mail(&mut self, shared: &mut Shared, pool: &dyn PoolView, mail: Mail) {
        match mail {
            Mail::Closed => self.end_session(shared),
            Mail::Go => self.open(shared, pool),
            // Another session's verdict on a body this session's peer
            // supplied: the skip set marks it known again, as the
            // supplier's own session does.
            Mail::Known(id) => {
                let short = shared.short(id);
                self.state.mark_known(id, short, pool.last_seq());
            }
        }
    }

    // Outputs and observations.

    /// The `Have` to send now, if one may go: as [`Self::take_have`] takes
    /// them, up to [`MAX_HAVE_ENTRIES`] of its queued announcements. One goes
    /// only while the data writer keeps room for every body the peer may
    /// have requested, and once `room` says the byte rate pays for the
    /// frame and the last `Have` is written. Otherwise the session tries
    /// again when the rate pays, or after [`HAVE_RETRY`], since a write
    /// wakes nothing.
    pub(crate) fn next_have(
        &mut self,
        pool: &dyn PoolView,
        room: HaveRoom<impl FnOnce(u64) -> Paid>,
        now: Time,
    ) -> Vec<(UnminedTxId, u32)> {
        let queued = self.state.announced.queued().min(MAX_HAVE_ENTRIES);
        if self.ended || queued == 0 || self.have_due.is_some_and(|due| now < due) {
            return Vec::new();
        }
        self.have_due = None;
        let wait = if room.free <= MAX_SERVES {
            Some(now.saturating_add(HAVE_RETRY))
        } else {
            // Widening usize to u64 is lossless on supported targets.
            match (room.paid)(have_payload_bound(queued) as u64) {
                Paid::Now => None,
                Paid::At(at) => Some(at).filter(|at| *at > now),
                Paid::Unwritten => Some(now.saturating_add(HAVE_RETRY)),
            }
        };
        if let Some(at) = wait {
            self.have_due = Some(at);
            self.timers.push(at, Timer::Have);
            return Vec::new();
        }
        self.take_have(pool, MAX_HAVE_ENTRIES)
    }

    /// Takes up to `max` of its queued announcements that the pool shows
    /// now, oldest first, with their sizes, for one `Have` frame. They count
    /// as sent: the frame goes on the data stream ahead of any body it lets
    /// the peer request. A child waits for a parent whose announcement has
    /// not gone out yet, so the peer never fetches the child first and
    /// holds it waiting for the parent.
    pub(crate) fn take_have(&mut self, pool: &dyn PoolView, max: usize) -> Vec<(UnminedTxId, u32)> {
        let held = &self.state.held;
        self.state.announced.take(
            max,
            |id| pool.holds(id).then(|| pool.size(id)).flatten(),
            |id| pool.parents(id),
            |parent| held.contains(*parent),
        )
    }

    /// How many of its announcements wait for a `Have`.
    #[cfg(test)]
    pub(crate) fn queued_have(&self) -> usize {
        self.state.announced.queued()
    }

    /// The earliest time after now that something of this session's
    /// becomes due with no other input: a deadline, a held advert, the end
    /// of its peer's cooldown, the end of the backoff while it holds ids, or
    /// the end of the retry delay of the id it would claim next.
    pub(crate) fn next_deadline(&self, shared: &Shared) -> Option<Time> {
        if self.ended {
            return None;
        }
        let now = self.now;
        let timer = self
            .timers
            .iter()
            .find(|(at, timer)| *at > now && self.timer_stands(*at, *timer))
            .map(|(at, _)| at);
        let holds = self.state.pending.len() > 0;
        let backoff = shared.wanted.backoff_until(now).filter(|_| holds);
        let cooling = shared.reactor.cooling_until(self.state.peer, now);
        let ready = self.state.pending.next_ready_after(now);
        [timer, backoff, cooling, ready].into_iter().flatten().min()
    }

    /// How many timers it keeps, standing or not.
    #[cfg(test)]
    pub(crate) fn timers_len(&self) -> usize {
        self.timers.set.len()
    }

    /// When each of its requests' timers comes due, and when its held
    /// adverts' timers do, for the composed invariants.
    #[cfg(test)]
    pub(crate) fn check_timers(&self) {
        let now = self.now;
        for (timer, key) in &self.timers.latest {
            let indexed = self.timers.set.get(key) == Some(timer);
            let held = matches!(timer, Timer::Held);
            assert!(indexed && !held, "relay/timer-index");
        }
        let mut held_timers: HashSet<Time> = HashSet::default();
        for ((at, _), timer) in &self.timers.set {
            match timer {
                Timer::Held => {
                    held_timers.insert(*at);
                }
                _ => assert!(self.timers.latest.contains_key(timer), "relay/timer-index"),
            }
        }
        let due = |timer: Timer| self.timers.latest.get(&timer).map(|(at, _)| *at);
        for (id, request) in &self.state.requests {
            let (timer, at) = match request.gives_up_at {
                None => (Timer::Deadline(*id), request.deadline),
                Some(at) => (Timer::GiveUp(*id), at),
            };
            assert_eq!(due(timer), Some(at), "relay/request-timer");
        }
        for (_, at) in self.state.held.iter() {
            let timed = held_timers.contains(&at);
            let early = self.held_due.is_some_and(|due| due <= at);
            assert!(at > now && timed && early, "relay/held-advert-timer");
        }
        if let Some(at) = self.have_due.filter(|at| *at > now) {
            assert_eq!(due(Timer::Have), Some(at), "relay/have-retry-timer");
        }
    }

    // The step loop.

    /// Moves time to `now` and applies what is due before the input. Time
    /// never goes back: an older `now` counts as the current time.
    pub(crate) fn enter(&mut self, shared: &mut Shared, pool: &dyn PoolView, now: Time) {
        self.now = self.now.max(now);
        if self.ended {
            return;
        }
        shared.refresh(self.now);
        self.expire(shared);
        self.release_adverts(shared, pool);
    }

    /// Files again what the wanted table's ring says changed, then claims
    /// what it can.
    pub(crate) fn settle(&mut self, shared: &mut Shared, pool: &dyn PoolView) {
        if self.ended {
            return;
        }
        self.fill(shared, pool);
        self.drop_stale_timers();
    }

    /// Ends this session if a charge in this step closed its peer.
    fn end_if_closed(&mut self, shared: &mut Shared) {
        let session = self.session;
        let closed = shared.node.contains(&NodeEffect::Closed(session));
        if closed {
            shared
                .node
                .retain(|effect| *effect != NodeEffect::Closed(session));
            self.end_session(shared);
        }
    }

    /// Whether `timer` still comes due at `at`: its source still holds that
    /// time. A timer whose source moved or ended stays in [`Timers`] until
    /// [`Self::drop_stale_timers`] or [`Self::expire`] reaches it.
    fn timer_stands(&self, at: Time, timer: Timer) -> bool {
        match timer {
            Timer::Deadline(id) => self
                .state
                .requests
                .get(&id)
                .is_some_and(|request| request.gives_up_at.is_none() && request.deadline == at),
            Timer::GiveUp(id) => self
                .state
                .requests
                .get(&id)
                .is_some_and(|request| request.gives_up_at == Some(at)),
            Timer::Held => self.state.held.has_due(at),
            Timer::Have => self.have_due == Some(at),
        }
    }

    /// Drops the timers at the front that are due or no longer stand, so the
    /// next look at the front starts at a live one.
    fn drop_stale_timers(&mut self) {
        while let Some((at, timer)) = self.timers.first() {
            if at > self.now && self.timer_stands(at, timer) {
                break;
            }
            self.timers.pop_first();
        }
    }

    /// Reads one published batch from the cursor. The pool is the pool
    /// after the batch.
    fn read_log(&mut self, shared: &mut Shared, pool: &dyn PoolView, batch: &[Logged]) {
        let (peer, session) = (self.state.peer, self.session);
        if !shared.reactor.has_go(peer, session) {
            return;
        }
        let last_seq = pool.last_seq();
        if self.state.cursor >= last_seq {
            return;
        }
        let rank = shared.reactor.rank(session);
        for logged in batch {
            let (seq, short) = (logged.seq, logged.short);
            if seq <= self.state.cursor {
                continue;
            }
            match logged.change {
                Change::Added(id) => {
                    let size = pool.size(&id);
                    let state = &mut self.state;
                    if let Some(request) = state.requests.get_mut(&id) {
                        // Skipped below while the request is out. A later
                        // `NotFound` counts the advert's delay from now.
                        request.admission_seen.get_or_insert(self.now);
                    }
                    // Only an id's last change in the batch decides an
                    // advert, so an addition the batch also removes and
                    // adds again is announced once. Every session reads
                    // from the batch's start, so the batch's last change is
                    // also its read's.
                    if !logged.last
                        || !pool.live()
                        || !pool.holds(&id)
                        || state.announced.contains_key(&id)
                        || (state.skips(id, short) || shared.reactor.holds_for(peer, id))
                        || state.held.contains(id)
                    {
                        continue;
                    }
                    if shared.fan_out.at_once(rank, size) {
                        state.announced.insert(id, seq);
                        shared.effects.push(RelayEffect::Advert { session, id });
                    } else {
                        let due = self.now.saturating_add(shared.fan_out.advert_delay);
                        // One timer stands for every advert due then.
                        if state.held.push(id, due, short) {
                            self.timers.push(due, Timer::Held);
                        }
                        self.held_due = Some(self.held_due.map_or(due, |held| held.min(due)));
                    }
                }
                Change::Removed { id, .. } => self.state.forget(id, short, seq),
                Change::Refused(_) => {}
            }
        }
        self.state.cursor = last_seq;
    }

    /// A session with the go that lost its place in the log rebuilds from
    /// the snapshot. An advert it sent still stands for an id the pool
    /// holds, and a held one keeps its deadline. What it finds follows the
    /// same fan-out as a read: at once inside the id's window, after the
    /// advert delay outside it. The skip set is not pruned.
    fn rebuild(&mut self, shared: &mut Shared, pool: &dyn PoolView) {
        let (peer, session) = (self.state.peer, self.session);
        if !shared.reactor.has_go(peer, session) {
            return;
        }
        let rank = shared.reactor.rank(session);
        let last_seq = pool.last_seq();
        let state = &mut self.state;
        state.announced.retain(|id, _| pool.holds(id));
        state.held.retain(|id| pool.holds(id));
        if pool.live() {
            for id in pool.ids() {
                let short = shared.short(id);
                if let Some(request) = state.requests.get_mut(&id) {
                    request.admission_seen.get_or_insert(self.now);
                }
                if state.announced.contains_key(&id)
                    || state.held.contains(id)
                    || state.skips(id, short)
                    || shared.reactor.holds_for(peer, id)
                {
                    continue;
                }
                if shared.fan_out.at_once(rank, pool.size(&id)) {
                    state.announced.insert(id, last_seq);
                    shared.effects.push(RelayEffect::Advert { session, id });
                } else {
                    let due = self.now.saturating_add(shared.fan_out.advert_delay);
                    // One timer stands for every advert due then.
                    if state.held.push(id, due, short) {
                        self.timers.push(due, Timer::Held);
                    }
                    self.held_due = Some(self.held_due.map_or(due, |held| held.min(due)));
                }
            }
        }
        state.cursor = last_seq;
    }

    /// This session's body of `id`, fetched for `generation` under
    /// `reservation`, goes to the pool, as `submitted` records.
    fn submit(
        &mut self,
        shared: &mut Shared,
        id: UnminedTxId,
        generation: EntryGen,
        reservation: ReservationId,
        body: UnminedTx,
        submitted: Submitted,
    ) {
        self.reservations.remove(&reservation);
        if shared.stopped {
            // The port would answer at once that the core stopped.
            shared.effects.push(RelayEffect::Release { reservation });
            shared
                .wanted
                .release(id, generation, self.session, self.now, false);
            return;
        }
        let waiter = shared.new_waiter();
        self.state.submissions.insert(waiter, (id, generation));
        self.submitted_at.insert(waiter, submitted);
        shared.effects.push(RelayEffect::Submit {
            waiter,
            reservation,
            body,
        });
    }

    /// The pool no longer needs the body this session was fetching.
    fn cancel_fetch(&mut self, shared: &mut Shared, id: UnminedTxId, reservation: ReservationId) {
        self.reservations.remove(&reservation);
        shared.give_back(reservation);
        let Some(request) = self.state.requests.get_mut(&id) else {
            return;
        };
        request.reservation = None;
        let generation = request.generation;
        // The token says only that the pool no longer needs the body, not
        // why. The id is asked about again, and the current pool answers.
        shared
            .wanted
            .release(id, generation, self.session, self.now, false);
    }

    /// Closes this session for a broken wire rule.
    fn violation(&mut self, shared: &mut Shared, violation: Violation) {
        shared.effects.push(RelayEffect::Close {
            session: self.session,
            reason: CloseReason::Violation(violation),
        });
        self.end_session(shared);
    }

    /// Ends this session in one step: frees its reservations, claims and
    /// probe permit, gives up its pending ids, and hands its submissions and
    /// open reserve calls to the reactor with their generations.
    pub(crate) fn end_session(&mut self, shared: &mut Shared) {
        if self.ended {
            return;
        }
        self.ended = true;
        let (peer, session, now) = (self.state.peer, self.session, self.now);
        // Ending with a claim out is the peer's try at that entry, so neither
        // a replacement session nor a later announcement asks it again.
        let attempts: Vec<(UnminedTxId, EntryGen)> = self
            .state
            .reserving
            .iter()
            .map(|(id, reserving)| (*id, reserving.pending.generation))
            .chain(
                self.state
                    .requests
                    .iter()
                    .filter(|(_, request)| request.reservation.is_some())
                    .map(|(id, request)| (*id, request.generation)),
            )
            .collect();
        for (id, generation) in attempts {
            self.tried(shared, id, generation);
        }
        // Its reserve calls still in flight are answered to the reactor, and
        // their grants go back as they arrive. Until then they count against
        // the peer's next session's window.
        shared.reactor.hand_calls(peer, self.state.reserving.len());
        for call in self.calls.keys() {
            shared.ended_calls.insert(*call, peer);
        }
        self.calls.clear();
        for (id, reserving) in &self.state.reserving {
            shared
                .wanted
                .release(*id, reserving.pending.generation, session, now, false);
            if let Some(generation) = reserving.probe {
                shared.wanted.return_probe(generation);
            }
        }
        for (id, request) in &self.state.requests {
            if let Some(reservation) = request.reservation {
                self.reservations.remove(&reservation);
                shared.give_back(reservation);
                shared
                    .wanted
                    .release(*id, request.generation, session, now, false);
            }
        }
        // A submission keeps its claim until its verdict, so nobody fetches
        // the id again while the pool still decides it.
        for pending in self.state.pending.iter() {
            shared.wanted.unhold(pending.id, pending.generation);
        }
        self.state.pending.clear(&mut shared.pending);
        for (waiter, submitted) in self.submitted_at.drain() {
            shared.ended_submitted.insert(waiter, submitted);
        }
        let handed: Vec<(WaiterId, (UnminedTxId, EntryGen))> = self
            .state
            .submissions
            .iter()
            .map(|(waiter, held)| (*waiter, *held))
            .collect();
        if let Some(next) = shared.reactor.end(peer, session, handed) {
            shared.node.push(NodeEffect::Go(next));
        }
    }

    /// This session got the go: it announces the whole pool, less what its
    /// peer still has in verification, and takes in the ids it was
    /// announced while waiting.
    pub(crate) fn open(&mut self, shared: &mut Shared, pool: &dyn PoolView) {
        if self.ended {
            return;
        }
        let (peer, session) = (self.state.peer, self.session);
        let seq = pool.last_seq();
        for id in shared.reactor.unsettled_ids(peer) {
            let short = shared.short(id);
            self.state.mark_known(id, short, seq);
        }
        // A session that had the go before, such as one the pool's restart
        // reopens, may not have read everything. It starts again from the
        // snapshot, as a resync does.
        let state = &mut self.state;
        state.announced.retain(|id, _| pool.holds(id));
        state.held.retain(|id| pool.holds(id));
        state.cursor = seq;
        if pool.live() {
            for id in pool.ids() {
                let short = shared.short(id);
                if !state.announced.contains_key(&id)
                    && !(state.skips(id, short) || shared.reactor.holds_for(peer, id))
                {
                    state.announced.insert(id, seq);
                    state.held.remove(id);
                    shared.effects.push(RelayEffect::Advert { session, id });
                }
            }
        }
        // Without a `Hello` it could never be asked, so its announcements
        // wait for one.
        if state.hello.is_some() {
            let waiting: Vec<(UnminedTxId, u32)> = std::mem::take(&mut self.state.waiting)
                .into_iter()
                .collect();
            self.take_ids(shared, pool, &waiting);
        }
    }

    /// Takes announced ids into this session, which has the go: its pending
    /// set, and the table.
    fn take_ids(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        entries: &[(UnminedTxId, u32)],
    ) {
        let mut fresh = Vec::new();
        let mut taken: HashSet<UnminedTxId> = HashSet::default();
        let ids = entries.iter().map(|(id, _)| (*id, shared.short(*id)));
        self.state.mark_all_known(ids, pool.last_seq());
        let state = &mut self.state;
        for (id, size) in entries.iter().copied() {
            // Announcing an id while a request for it is outstanding says
            // the peer has it now. A `NotFound` for that request, which can
            // arrive later on the other stream, is still the peer's try, but
            // leaves what the peer told this node alone, and the
            // announcement is taken afresh once that request ends. So both
            // orders of the two end the same way.
            if state.requests.contains_key(&id) {
                state.reannounced.insert(id, size);
            }
            if pool.holds(&id) || size > MAX_TX_BYTES {
                continue;
            }
            let entry = shared.wanted.entry(id);
            let current = entry.map(|entry| entry.generation);
            // No session holds it, though a claim may: the body of a session
            // that ended can still wait for its verdict. Such an id has no
            // source left if that verdict leaves it undecided, so it is a
            // first source. The claim still stops a second fetch meanwhile.
            let first = entry.is_none_or(|entry| entry.holders == 0);
            // A peer that tried this entry is not asked again, however often
            // it announces the id, until the entry ends.
            if current.is_some_and(|generation| shared.wanted.has_tried(state.peer, id, generation))
            {
                continue;
            }
            let already = current.is_some_and(|generation| state.holds(id, generation));
            // A request whose fetch was given up, such as one for an older
            // generation, does not stop a fresh announcement. The id is asked
            // for again only once that request is answered.
            let busy = state.reserving.contains_key(&id)
                || state
                    .requests
                    .get(&id)
                    .is_some_and(|request| request.reservation.is_some())
                || current.is_some_and(|generation| state.submissions.holds(id, generation));
            if already || busy || !taken.insert(id) {
                continue;
            }
            fresh.push((id, size, first));
        }
        // Each id's room is decided before the table changes, so an id that
        // does not fit leaves no holder behind. See `PENDING_CAP` for the
        // rule.
        let mut dropped: u64 = 0;
        for (id, size, first) in fresh {
            let share = self.state.pending.len() < PENDING_SHARE;
            let records = shared.pending;
            let node = records < PENDING_CAP || (first && records < 2 * PENDING_CAP);
            if self.state.room() == 0 || !(share || node) {
                dropped = dropped.saturating_add(1);
                continue;
            }
            // The set keeps one reference per id, and one it still holds
            // here names an entry that ended, which its next read of the
            // ring would drop.
            if let Some(stale) = self.state.pending.of(id) {
                self.drop_decided(shared, id, stale.generation);
            }
            let generation = shared.wanted.hold(id, self.now);
            self.state.pending.push(
                Pending {
                    id,
                    generation,
                    size,
                },
                &mut shared.pending,
            );
            file(&mut self.state, &shared.wanted, id);
        }
        self.state.overflow = self.state.overflow.saturating_add(dropped);
    }

    /// Reconsiders announcing `id` to its peer, as a read of its admission
    /// would: at once inside its window, after the advert delay outside it,
    /// or not at all if the peer is still one to skip. The delay counts
    /// from `seen`, when the session first read the admission, if it did.
    fn reconsider_advert(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        id: UnminedTxId,
        seen: Option<Time>,
    ) {
        if !pool.live() || !pool.holds(&id) {
            return;
        }
        let (peer, session) = (self.state.peer, self.session);
        let short = shared.short(id);
        let state = &mut self.state;
        if !shared.reactor.has_go(peer, session)
            || state.announced.contains_key(&id)
            || state.held.contains(id)
            || state.skips(id, short)
            || shared.reactor.holds_for(peer, id)
        {
            return;
        }
        let rank = shared.reactor.rank(session);
        let window = shared.fan_out.window(pool.size(&id));
        let now = self.now;
        let due = seen
            .unwrap_or(now)
            .saturating_add(shared.fan_out.advert_delay);
        let at_once = rank.is_some_and(|rank| rank < window) || due <= now;
        if at_once {
            state.announced.insert(id, pool.last_seq());
            shared.effects.push(RelayEffect::Advert { session, id });
        } else {
            // One timer stands for every advert due then.
            if state.held.push(id, due, short) {
                self.timers.push(due, Timer::Held);
            }
            self.held_due = Some(self.held_due.map_or(due, |held| held.min(due)));
        }
    }

    /// Sends held adverts that are due.
    fn release_adverts(&mut self, shared: &mut Shared, pool: &dyn PoolView) {
        let now = self.now;
        if self.ended || self.held_due.is_none_or(|due| due > now) {
            return;
        }
        let due = self.state.held.take_due(now);
        self.held_due = self.state.held.first_due();
        let (peer, session) = (self.state.peer, self.session);
        for (id, short) in due {
            let state = &mut self.state;
            if pool.holds(&id)
                && !state.announced.contains_key(&id)
                && !(state.skips(id, short) || shared.reactor.holds_for(peer, id))
            {
                state.announced.insert(id, pool.last_seq());
                shared.effects.push(RelayEffect::Advert { session, id });
            }
        }
    }

    /// This session's peer tried `id` in `generation`: it answered
    /// `NotFound`, missed the deadline, ended with the claim out, or
    /// supplied a body the pool rejected without settling the id. It is not
    /// asked for this entry again. This session drops its reference now, and
    /// the peer's others when they read the ring. Once no holder is left,
    /// the entry ends.
    fn tried(&mut self, shared: &mut Shared, id: UnminedTxId, generation: EntryGen) {
        shared.tried(self.state.peer, id, generation);
        self.drop_tried_reference(shared, id, generation);
    }

    /// Drops this session's pending reference to `id` in `generation`,
    /// which its peer tried.
    fn drop_tried_reference(&mut self, shared: &mut Shared, id: UnminedTxId, generation: EntryGen) {
        if self
            .state
            .take_pending_of(id, generation, &mut shared.pending)
            .is_some()
        {
            shared.wanted.unhold(id, generation);
        }
    }

    /// Applies request deadlines that passed.
    fn expire(&mut self, shared: &mut Shared) {
        let now = self.now;
        let mut missed = Vec::new();
        let mut unresponsive = false;
        while let Some((at, timer)) = self.timers.first() {
            if at > now {
                break;
            }
            self.timers.pop_first();
            if !self.timer_stands(at, timer) {
                continue;
            }
            match timer {
                Timer::Deadline(id) => missed.push(id),
                Timer::GiveUp(_) => unresponsive = true,
                // The IO looks at the adverts after every step.
                Timer::Held | Timer::Have => {}
            }
        }
        // In the order of a walk over the session's requests, so the effects
        // come out as that walk made them.
        missed.sort_by_key(|id| self.state.requests.get_index_of(id));
        // A deadline set twice to the same time has two timers.
        missed.dedup();
        // A late peer is asked for less at once, as block sync's window dips
        // once per batch of timeouts. A fetch the pool cancelled counts too,
        // since the peer was still late.
        if !missed.is_empty() {
            self.state.window = (self.state.window / 2).max(1);
        }
        let mut freed = Vec::new();
        for id in missed {
            let request = self.state.requests.get_mut(&id).expect("the timer stands");
            // The wire deadline runs whether or not the pool still holds
            // the reservation: a fetch the pool cancelled is no try, but
            // left unanswered it still ends the session.
            let span = request.deadline.saturating_sub(request.sent);
            let gives_up_at = request.deadline.saturating_add(span);
            request.gives_up_at = Some(gives_up_at);
            if let Some(reservation) = request.reservation.take() {
                freed.push((id, request.generation, reservation));
            }
            // Both can be due at once when the input comes late.
            if gives_up_at <= now {
                unresponsive = true;
            } else {
                self.timers.push(gives_up_at, Timer::GiveUp(id));
            }
        }
        for (id, generation, reservation) in freed {
            // A missed deadline is the peer's try at this entry.
            self.tried(shared, id, generation);
            self.reservations.remove(&reservation);
            shared.give_back(reservation);
            shared
                .wanted
                .release(id, generation, self.session, now, false);
        }
        // A request on the wire stays written until it is answered, so a
        // peer that leaves one unanswered for a further deadline stalled,
        // and loses the session, which frees its slots.
        if unresponsive {
            self.stalled(shared, CloseReason::Unresponsive);
        }
    }

    /// Its peer stalled, row 16 of the close table. The first stall parks
    /// the session: it ends, its connection stays, so the peer's other
    /// services keep going, and the peer's next sessions are refused for
    /// [`PARK_COOLDOWN`]. A stall in the session the park lets in afterwards
    /// closes the connection for `reason`, as block sync does. Neither is a
    /// charge.
    fn stalled(&mut self, shared: &mut Shared, reason: CloseReason) {
        let session = self.session;
        let until = self.now.saturating_add(PARK_COOLDOWN);
        if !self.readmitted && shared.reactor.park(self.state.peer, until, self.now) {
            shared.effects.push(RelayEffect::Park { session });
        } else {
            shared.effects.push(RelayEffect::Close { session, reason });
        }
        self.end_session(shared);
    }

    /// Its peer stopped reading what this node writes: the IO found one of
    /// its writers' queues full, or a write passed the transport's write
    /// deadline. See [`Self::stalled`].
    pub(crate) fn stopped_reading(&mut self, shared: &mut Shared, pool: &dyn PoolView, now: Time) {
        self.enter(shared, pool, now);
        if !self.ended {
            self.stalled(shared, CloseReason::StoppedReading);
        }
        self.settle(shared, pool);
    }

    /// Claims and starts reserve calls while this session has free slots,
    /// its oldest ready id first. During a backoff only the probe's holder
    /// may.
    fn fill(&mut self, shared: &mut Shared, pool: &dyn PoolView) {
        loop {
            self.refile_ring(shared);
            if self.ended || !pool.live() || shared.stopped {
                return;
            }
            let probe = match shared.wanted.admission(self.session, self.now) {
                Admission::Closed => return,
                Admission::Open => None,
                Admission::Probe(generation) => Some(generation),
            };
            let Some(pending) = self.next_claim(shared) else {
                return;
            };
            shared
                .wanted
                .claim(pending.id, pending.generation, self.session);
            if let Some(generation) = probe {
                shared.wanted.take_probe(self.session, generation);
            }
            let call = shared.new_call();
            self.state.reserving.insert(
                pending.id,
                Reserving {
                    pending,
                    probe,
                    call,
                },
            );
            self.calls.insert(
                call,
                Call {
                    id: pending.id,
                    tip: pool.tip(),
                },
            );
            shared.effects.push(RelayEffect::Reserve {
                call,
                id: pending.id,
            });
        }
    }

    /// The caller of `call` receives the pool's answer.
    fn deliver(
        &mut self,
        shared: &mut Shared,
        pool: &dyn PoolView,
        call: CallId,
        made: Call,
        answer: Result<Granted, Unreserved>,
    ) {
        let (id, session, now) = (made.id, self.session, self.now);
        let Some(reserving) = self
            .state
            .reserving
            .get(&id)
            .filter(|reserving| reserving.call == call)
            .copied()
        else {
            return shared.answer_unknown(answer);
        };
        self.state.reserving.shift_remove(&id);
        let Reserving { pending, probe, .. } = reserving;
        let peer = self.state.peer;
        // A rejection holds only for the chain it was made on. The caller
        // knows the tip it called at and the one it holds now, and a
        // same-height reorg changes the tip's hash.
        let chain_unchanged = pool.tip() == made.tip;
        let still_claimed = shared
            .wanted
            .current(id, pending.generation)
            .is_some_and(|entry| entry.claim == Some(session));
        let wanted = &mut shared.wanted;
        match answer {
            Err(Unreserved::Stopped) => {
                // The core stopped. `tx-relay` claims nothing more.
                shared.stopped = true;
                wanted.release(id, pending.generation, session, now, false);
                return_probe(wanted, probe);
            }
            Ok(granted) if granted.cancelled => {
                // A cancelled token says nothing about why, so it decides
                // nothing: the slot goes back and the id is asked about
                // again, and the current pool answers.
                shared.give_back(granted.reservation);
                let wanted = &mut shared.wanted;
                wanted.release(id, pending.generation, session, now, false);
                return_probe(wanted, probe);
            }
            Ok(granted) => {
                wanted.room();
                let cooling = shared.reactor.is_cooling(peer, now);
                if still_claimed && pool.live() && !cooling {
                    self.state.send(pending, granted.reservation, now);
                    // Sending moves every unmissed deadline of the session.
                    for (id, request) in &self.state.requests {
                        if request.gives_up_at.is_none() {
                            self.timers.push(request.deadline, Timer::Deadline(*id));
                        }
                    }
                    self.reservations.insert(granted.reservation, id);
                    shared.effects.push(RelayEffect::Ask { session, id });
                } else {
                    // The peer started cooling, the pool turned off, or the
                    // claim is gone: nothing is asked for.
                    shared.give_back(granted.reservation);
                    shared
                        .wanted
                        .release(id, pending.generation, session, now, cooling);
                }
                return_probe(&mut shared.wanted, probe);
            }
            Err(Unreserved::NoRoom) => {
                wanted.release_no_room(id, pending.generation, session, now);
                wanted.no_room(now, probe);
            }
            Err(Unreserved::Pending) => {
                wanted.release_pending(id, pending.generation, session, now);
                return_probe(wanted, probe);
            }
            Err(Unreserved::Pooled) => {
                // It settles the id only if the caller's current snapshot
                // still holds it: the pool may have restarted since. The
                // pool answers `Pooled` by mined id, so the check is by
                // mined id too: another authorizing-data variant settles it.
                let pooled = pool.holds_mined(&id.mined_id());
                wanted.release(id, pending.generation, session, now, !pooled);
                if pooled {
                    wanted.decide(id, pending.generation);
                }
                return_probe(wanted, probe);
            }
            Err(Unreserved::Rejected { scope }) => {
                // The same rule as a verdict's rejection.
                let remembered =
                    chain_unchanged && pool.live() && !matches!(scope, RejectionScope::Uncached);
                wanted.release(id, pending.generation, session, now, !remembered);
                if remembered {
                    wanted.decide(id, pending.generation);
                }
                return_probe(wanted, probe);
            }
            Err(Unreserved::Inactive) => {
                // The pool turned off, perhaps before a snapshot said so. The
                // id waits a retry delay, so a missed turn-off and on again
                // costs one delay, not a busy loop.
                wanted.release(id, pending.generation, session, now, true);
                return_probe(wanted, probe);
            }
        }
    }

    /// Ids reserving, requested or awaiting a verdict on this session, with
    /// what its peer's ended sessions left: what its window counts.
    fn in_flight(&self, shared: &Shared) -> usize {
        self.state
            .in_flight()
            .saturating_add(shared.reactor.unsettled_count(self.state.peer))
    }

    /// The oldest id this session may claim now, if it has a free slot.
    fn next_claim(&self, shared: &Shared) -> Option<Pending> {
        let (peer, session, state) = (self.state.peer, self.session, &self.state);
        if !shared.reactor.has_go(peer, session) || shared.reactor.is_cooling(peer, self.now) {
            return None;
        }
        let hello = state.hello?;
        debug_assert!(state.window <= hello.serves, "the Hello caps the window");
        if self.in_flight(shared) >= state.window {
            return None;
        }
        let asked = state.asked_bytes();
        // The id ready longest goes first: since its first announcement, or
        // since its retry delay ended. Neither fresh work nor retries starve
        // the other. The filed entries are in that order, and one filed
        // later than now is still in its retry delay, as is every one after
        // it, since an entry is ready no earlier than its retry time.
        for ((ready, _), pending) in state.pending.ready() {
            if ready > self.now {
                return None;
            }
            // A request given up but still on the wire, or a reserve call,
            // for the id holds it back.
            if state.requests.contains_key(&pending.id) || state.reserving.contains_key(&pending.id)
            {
                continue;
            }
            // The oldest ready id waits for bytes to free rather than let
            // smaller ones keep passing it.
            if asked.saturating_add(u64::from(pending.size)) > MAX_OUTSTANDING_BYTES {
                return None;
            }
            return Some(pending);
        }
        None
    }

    /// Files again every id the ring says changed since this session last
    /// read it, or every id it holds if it fell behind.
    fn refile_ring(&mut self, shared: &mut Shared) {
        let (changes, end) = shared.wanted.changes_since(self.ring_cursor);
        self.ring_cursor = end;
        let ids: Vec<UnminedTxId> = match changes {
            Changes::Ids(ids) => ids,
            Changes::Overrun => self
                .state
                .pending
                .iter()
                .map(|pending| pending.id)
                .collect(),
        };
        for id in ids {
            self.refile(shared, id);
        }
    }

    /// Files `id` again if this session holds it. A reference to an entry
    /// that was decided, or to an older generation, goes, and so does a
    /// fetch of it this session still had. A reference to an entry its peer
    /// tried goes too.
    fn refile(&mut self, shared: &mut Shared, id: UnminedTxId) {
        let Some(held) = self.state.pending.of(id) else {
            return;
        };
        let current = shared.wanted.entry(id).map(|entry| entry.generation);
        if current != Some(held.generation) {
            self.drop_decided(shared, id, held.generation);
            return;
        }
        if shared
            .wanted
            .has_tried(self.state.peer, id, held.generation)
        {
            self.drop_tried_reference(shared, id, held.generation);
        } else {
            file(&mut self.state, &shared.wanted, id);
        }
    }

    /// Drops this session's references to `id` in `generation`, which was
    /// decided, and gives back a fetch of it the session still had.
    fn drop_decided(&mut self, shared: &mut Shared, id: UnminedTxId, generation: EntryGen) {
        self.state.drop_decided(id, generation, &mut shared.pending);
        if let Some(reservation) = self
            .state
            .requests
            .get_mut(&id)
            .filter(|request| request.generation == generation)
            .and_then(|request| request.reservation.take())
        {
            self.reservations.remove(&reservation);
            shared.give_back(reservation);
        }
        if let Some(reserving) = self
            .state
            .reserving
            .get(&id)
            .filter(|reserving| reserving.pending.generation == generation)
            .copied()
        {
            self.state.reserving.shift_remove(&id);
            return_probe(&mut shared.wanted, reserving.probe);
        }
    }
}

/// Frees the probe permit a call held, if it held one.
fn return_probe(wanted: &mut super::wanted::WantedIds, probe: Option<u64>) {
    if let Some(generation) = probe {
        wanted.return_probe(generation);
    }
}

/// Something of one session's that comes due with no input. Each names
/// where its time is kept, which [`PeerMachine::timer_stands`] reads.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Timer {
    /// A request's wire deadline, while it has not missed it.
    Deadline(UnminedTxId),
    /// A request that missed its deadline ends the session at this time.
    GiveUp(UnminedTxId),
    /// One of the held adverts comes due.
    Held,
    /// The session may try its next `Have` again.
    Have,
}

/// Timers in the order they come due, so the next is at the front. A timer
/// is added each time its source sets a time. A held advert's timer is not
/// taken out when the advert leaves another way, so one may no longer stand;
/// readers check. Every other kind has one time at once, so a newer push
/// replaces the older entry, and repeated sends cannot pile them up.
#[derive(Clone, Debug, Default)]
pub(crate) struct Timers {
    pub(crate) set: std::collections::BTreeMap<(Time, u64), Timer>,
    /// Where each timer of a kind with one time at once sits in `set`.
    pub(crate) latest: HashMap<Timer, (Time, u64)>,
    next: u64,
}

impl Timers {
    /// Adds `timer`, due at `at`, in place of any earlier entry of the same
    /// request.
    pub(crate) fn push(&mut self, at: Time, timer: Timer) {
        self.next = self.next.saturating_add(1);
        let key = (at, self.next);
        // A held advert's timer stands for every advert due then, so each
        // distinct due time keeps its own.
        if !matches!(timer, Timer::Held) {
            if let Some(old) = self.latest.insert(timer, key) {
                self.set.remove(&old);
            }
        }
        self.set.insert(key, timer);
    }

    /// The first timer, if any.
    pub(crate) fn first(&self) -> Option<(Time, Timer)> {
        self.set
            .first_key_value()
            .map(|((at, _), timer)| (*at, *timer))
    }

    /// Takes the first timer out.
    pub(crate) fn pop_first(&mut self) {
        if let Some((key, timer)) = self.set.pop_first() {
            if self.latest.get(&timer) == Some(&key) {
                self.latest.remove(&timer);
            }
        }
    }

    /// Every timer, first due first.
    fn iter(&self) -> impl Iterator<Item = (Time, Timer)> + '_ {
        self.set.iter().map(|((at, _), timer)| (*at, *timer))
    }
}
