//! An independent model of `tx-relay`, the contract its machines are built
//! against.
//!
//! It restates the M2 plan's rules and never calls the code it checks. It
//! drives M1's [`PoolModel`] the way `tx-relay` drives the pool core, and it
//! owns that pool, so every pool step it causes or is handed is routed in
//! the order the pool produced it.
//!
//! The model has three owners, as the real service does:
//!
//! - [`peer::PeerModel`], one per session: everything about one peer. Its
//!   pending ids with the size it announced, the ids it tried, its
//!   outstanding requests and their deadlines, its submissions, its limit of
//!   [`MAX_REQUESTS_PER_PEER`], and what it was announced.
//! - [`wanted::WantedModel`], one per node: the ids this node wants, each
//!   with its claim, holder count and timing. No peer facts.
//! - [`reactor::ReactorModel`], one per node: cooldowns, which session of a
//!   peer may fetch, the submissions of ended sessions, and the ranking.
//!
//! The rules:
//!
//! - **Claim, reserve, then ask.** A session with a free slot claims the
//!   oldest eligible id in its pending set, reserves a slot at the pool, and
//!   only then sends `GetTxs`. One claim per id.
//! - **The limit of 5 is counted once.** Ids requested or awaiting a verdict
//!   on a session, plus the peer's unsettled submissions from ended
//!   sessions, never exceed [`MAX_REQUESTS_PER_PEER`]. A new session gets the
//!   go only after every older session of its peer ended.
//! - **Dialed peers first.** A session this node did not dial waits
//!   [`INBOUND_DELAY`] after an id's first announcement.
//! - **Each announcer is tried until it answers once.** `NotFound` marks the
//!   id tried for that session. A deadline frees the pool's slot and the
//!   claim, so another announcer can try, and the timed-out session waits
//!   [`INBOUND_DELAY`] after the release before it may claim the id again.
//!   A late body is dropped before it reaches the pool.
//! - **The table has no cap of its own.** An entry lives while a pending set
//!   holds it or a session claims it, so each peer fills only its own share.
//! - **A full pool costs one probe per backoff.** After a `NoRoom` nobody
//!   claims until the backoff ends or a verdict frees a slot.
//! - **A verdict's charge goes to the peer that supplied the body**, even if
//!   its session ended. `Cooldown` follows today's `PeerCooldowns`, and a
//!   second cooldown closes the peer. `Ban` closes it.
//! - **Announce what the pool admits, once, and never back to its source.**
//!   A session announces an added or revealed id the pool holds, unless it
//!   already did or its peer announced or supplied it. Sessions inside the
//!   id's window announce at once, the rest after [`ADVERT_DELAY`].
//! - **Serve only what was announced.** A `GetTxs` gets the body only for an
//!   exact id this session announced and the pool still holds.
//!
//! Left out, because other code owns them and keeps their own tests: the
//! byte rate on `Inv`, `Serve`'s grants and its over-limit rule, frames, and
//! the wake-up mechanics of the real routine. The model is synchronous, so
//! it has no races between tasks. Each input applies whatever is due first,
//! as the real routine's wake-ups must.

pub(crate) mod peer;
pub(crate) mod reactor;
pub(crate) mod wanted;

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    time::Duration,
};

use zakura_chain::transaction::UnminedTxId;
use zakura_node_services::mempool::port::{
    model::{
        Effect, JobId, ModelTransition, ModelTx, ModelUnreserved, ModelVerdict, Outcome, PoolModel,
        Step, WaiterId,
    },
    Change, Charge, Generation, GenerationRead, Intent, ReservationId,
};

use self::{
    peer::{Hello, PeerModel, Pending},
    reactor::ReactorModel,
    wanted::WantedModel,
};

/// Time since the model started.
pub(crate) type Time = Duration;

/// Ids requested or awaiting a verdict for one peer, at most.
pub(crate) const MAX_REQUESTS_PER_PEER: usize = 5;

/// Ids one session may hold pending: a whole pool at the default size.
pub(crate) const PENDING_PER_PEER: usize = 8_000;

/// Ids one session remembers trying.
pub(crate) const TRIED_PER_PEER: usize = 8_000;

/// Ids one session remembers its peer announcing or supplying.
pub(crate) const KNOWN_CAP: usize = 2 * 8_000 + PENDING_PER_PEER;

/// How long a peer this node did not dial waits after an id's first
/// announcement, and a timed-out session waits after a release.
pub(crate) const INBOUND_DELAY: Duration = Duration::from_secs(2);

/// The fixed part of a request's deadline.
pub(crate) const BASE_DEADLINE: Duration = Duration::from_secs(8);

/// The slowest link a deadline allows for, in bytes a second.
pub(crate) const MIN_RATE: u64 = 100_000;

/// Paid `Inv` bytes a sender may hold that are not yet written to QUIC.
pub(crate) const SENDER_ALLOWANCE: u64 = 512 * 1024;

/// The first retry delay for an id, doubling to [`RETRY_MAX`].
pub(crate) const RETRY_FIRST: Duration = Duration::from_secs(1);

/// The longest retry delay for an id.
pub(crate) const RETRY_MAX: Duration = Duration::from_secs(30);

/// The first `NoRoom` backoff, doubling to [`NO_ROOM_MAX`].
pub(crate) const NO_ROOM_FIRST: Duration = Duration::from_secs(1);

/// The longest `NoRoom` backoff.
pub(crate) const NO_ROOM_MAX: Duration = Duration::from_secs(30);

/// How long a session outside an id's window waits to announce it.
pub(crate) const ADVERT_DELAY: Duration = Duration::from_secs(2);

/// The window for the smallest transactions.
pub(crate) const WINDOW_SMALL: usize = 16;

/// The window for the largest transactions.
pub(crate) const WINDOW_FULL: usize = 4;

/// Sizes at or below this get [`WINDOW_SMALL`].
pub(crate) const SMALL_TX_BYTES: u32 = 10_000;

/// The protocol's largest transaction, which gets [`WINDOW_FULL`].
pub(crate) const MAX_TX_BYTES: u32 = 2_000_000;

/// The most requests a `Hello` may say it serves: the table's in-flight
/// maximum.
pub(crate) const MAX_SERVES: usize = 16;

/// Today's first cooldown.
pub(crate) const BASE_COOLDOWN: Duration = Duration::from_secs(10 * 60);

/// Today's longest cooldown.
pub(crate) const MAX_COOLDOWN: Duration = Duration::from_secs(2 * 60 * 60);

/// Peers with a cooldown history, at most.
pub(crate) const MAX_COOLDOWN_PEERS: usize = 20_000;

/// Names one session. A peer that reconnects or replaces its session gets a
/// new one.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SessionId(pub(crate) u64);

/// Names one peer: its v2 peer id.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PeerKey(pub(crate) u64);

/// One life of a wanted id.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct EntryGen(pub(crate) u64);

/// Why the node closes a peer's connection.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum CloseReason {
    /// The peer broke a wire rule.
    Violation(Violation),
    /// A body it supplied earned a ban.
    Ban,
    /// A body it supplied earned a second cooldown.
    RepeatedCooldown,
}

/// A broken wire rule.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Violation {
    /// A second `Hello`.
    HelloTwice,
    /// A `Hello` outside the protocol's limits.
    BadHello,
    /// A state message before the peer's `Hello`.
    NoHelloFirst,
    /// An answer to nothing outstanding.
    Unrequested,
    /// A body of another size than the peer announced.
    WrongSize,
    /// A body that does not hash to the requested id.
    WrongId,
}

/// What `tx-relay` does on the wire.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RelayEffect {
    /// Send `GetTxs` for `id`.
    Ask {
        /// The session.
        session: SessionId,
        /// The id.
        id: UnminedTxId,
    },
    /// Announce `id` in an `Inv`.
    Advert {
        /// The session.
        session: SessionId,
        /// The id.
        id: UnminedTxId,
    },
    /// Answer a `GetTxs` with the body.
    Serve {
        /// The session.
        session: SessionId,
        /// The id.
        id: UnminedTxId,
    },
    /// Answer a `GetTxs` with `NotFound`.
    NotFound {
        /// The session.
        session: SessionId,
        /// The id.
        id: UnminedTxId,
    },
    /// Close the session's connection.
    Close {
        /// The session.
        session: SessionId,
        /// Why.
        reason: CloseReason,
    },
}

/// Who waits for a verdict.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum VerdictOwner {
    /// A live session.
    Session(SessionId),
    /// The reactor, for a session that ended.
    Reactor,
}

/// `tx-relay` with the pool behind it.
#[derive(Debug)]
pub(crate) struct RelayModel {
    now: Time,
    pool: PoolModel,
    /// The stable generation the pool has applied, which a verification
    /// reports as read.
    generation: Generation,
    pool_live: bool,
    wanted: WantedModel,
    reactor: ReactorModel,
    sessions: BTreeMap<SessionId, PeerModel>,
    next_session: u64,
    /// Which session holds each of `tx-relay`'s reservations, and for which
    /// id.
    reservations: HashMap<ReservationId, (SessionId, UnminedTxId)>,
    /// Who waits for each of `tx-relay`'s submissions.
    waiters: HashMap<WaiterId, VerdictOwner>,
    /// The size each id was announced or supplied with, for its window.
    sizes: HashMap<UnminedTxId, u32>,
    /// Whether a cooldown earned now may start: the node is current and its
    /// tip is the one the verdict was decided at.
    may_cool: bool,
    /// How long sessions outside an id's window wait. `None` never announces
    /// to them.
    advert_delay: Option<Duration>,
    /// Pool steps waiting to be routed, oldest first.
    inbox: VecDeque<Step>,
    /// What `tx-relay` did, in order, until the test takes it.
    effects: Vec<RelayEffect>,
    /// Verification jobs the pool started and has not finished.
    jobs: Vec<JobId>,
    /// Pool effects for other callers, such as the legacy adapter.
    outside: Vec<Effect>,
}

impl RelayModel {
    /// `tx-relay` on an empty, active pool.
    pub(crate) fn new(pool: PoolModel) -> Self {
        Self {
            now: Duration::ZERO,
            pool,
            generation: Generation(0),
            pool_live: true,
            wanted: WantedModel::default(),
            reactor: ReactorModel::default(),
            sessions: BTreeMap::new(),
            next_session: 0,
            reservations: HashMap::new(),
            waiters: HashMap::new(),
            sizes: HashMap::new(),
            may_cool: true,
            advert_delay: Some(ADVERT_DELAY),
            inbox: VecDeque::new(),
            effects: Vec::new(),
            jobs: Vec::new(),
            outside: Vec::new(),
        }
    }

    /// The same model with another advert delay. `None` announces only to
    /// the window.
    pub(crate) fn with_advert_delay(mut self, delay: Option<Duration>) -> Self {
        self.advert_delay = delay;
        self
    }

    // Inputs from peers.

    /// A session of `peer` opened. It gets the go once every older session
    /// of the peer ended.
    pub(crate) fn connect(&mut self, peer: PeerKey, dialed: bool, now: Time) -> SessionId {
        self.enter(now);
        self.next_session = self.next_session.saturating_add(1);
        let session = SessionId(self.next_session);
        self.sessions.insert(session, PeerModel::new(peer, dialed));
        if self.reactor.connect(peer, session) {
            self.open(session);
        }
        self.settle();
        session
    }

    /// The peer's `Hello` arrived.
    pub(crate) fn hello(&mut self, session: SessionId, hello: Hello, now: Time) {
        self.enter(now);
        let Some(state) = self.sessions.get_mut(&session) else {
            return;
        };
        if state.hello.is_some() {
            self.violation(session, Violation::HelloTwice);
        } else if hello.serves == 0
            || hello.serves > MAX_SERVES
            || hello.max_tx_bytes > MAX_TX_BYTES
        {
            self.violation(session, Violation::BadHello);
        } else {
            state.hello = Some(hello);
        }
        self.settle();
    }

    /// An `Inv` arrived with these ids and sizes.
    pub(crate) fn inv(&mut self, session: SessionId, entries: &[(UnminedTxId, u32)], now: Time) {
        self.enter(now);
        let Some(peer) = self.sessions.get(&session).map(|state| state.peer) else {
            return;
        };
        if self.reactor.is_cooling(peer, self.now) {
            self.settle();
            return;
        }
        if self.reactor.has_go(peer, session) {
            self.take_ids(session, entries);
        } else {
            // A session waiting for the go keeps its announcements to itself,
            // within the same bound, so one peer never holds two shares of
            // the table.
            let state = self.sessions.get_mut(&session).expect("checked above");
            for (id, size) in entries.iter().copied() {
                state.mark_known(id);
                if state.waiting.len() < PENDING_PER_PEER {
                    state.waiting.push((id, size));
                } else {
                    state.overflow = state.overflow.saturating_add(1);
                }
            }
        }
        self.settle();
    }

    /// Takes announced ids into a session that has the go: its pending set,
    /// and the table.
    fn take_ids(&mut self, session: SessionId, entries: &[(UnminedTxId, u32)]) {
        let visible: HashSet<UnminedTxId> = self.pool.visible().into_iter().collect();
        let mut fresh = Vec::new();
        {
            let Some(state) = self.sessions.get_mut(&session) else {
                return;
            };
            for (id, size) in entries.iter().copied() {
                state.mark_known(id);
                if visible.contains(&id) || size > MAX_TX_BYTES {
                    continue;
                }
                let current = self.wanted.entry(id).map(|entry| entry.generation);
                let already = current.is_some_and(|generation| {
                    state.holds(id, generation) || state.has_tried(id, generation)
                });
                let asked = state.requests.contains_key(&id)
                    || state.submissions.values().any(|(held, _)| *held == id);
                if already || asked || fresh.iter().any(|(held, _)| *held == id) {
                    continue;
                }
                fresh.push((id, size));
            }
            // Room is decided before the table changes, so an id that does
            // not fit leaves no holder behind.
            let fits = state.room().min(fresh.len());
            let dropped = fresh.len().saturating_sub(fits);
            state.overflow = state
                .overflow
                .saturating_add(u64::try_from(dropped).unwrap_or(u64::MAX));
            fresh.truncate(fits);
        }
        for (id, size) in fresh {
            let generation = self.wanted.hold(id, self.now);
            self.sizes.entry(id).or_insert(size);
            self.sessions
                .get_mut(&session)
                .expect("checked above")
                .pending
                .push(Pending {
                    id,
                    generation,
                    size,
                });
        }
    }

    /// A `Tx` arrived, with the size it had on the wire.
    pub(crate) fn tx(
        &mut self,
        session: SessionId,
        id: UnminedTxId,
        body: ModelTx,
        size: u32,
        now: Time,
    ) {
        self.enter(now);
        let Some(state) = self.sessions.get_mut(&session) else {
            return;
        };
        let Some(request) = state.requests.get(&id).copied() else {
            self.violation(session, Violation::Unrequested);
            self.settle();
            return;
        };
        if size != request.size {
            self.violation(session, Violation::WrongSize);
            self.settle();
            return;
        }
        if body.id != id {
            self.violation(session, Violation::WrongId);
            self.settle();
            return;
        }
        state.requests.shift_remove(&id);
        state.mark_known(id);
        let peer = state.peer;
        let Some(reservation) = request.reservation else {
            // Late: the node stopped waiting. Dropped before it is decoded.
            self.settle();
            return;
        };
        self.reservations.remove(&reservation);
        if self.reactor.is_cooling(peer, self.now) {
            // The peer started cooling while the body was in flight.
            let step = self.pool.release(reservation, self.now);
            self.inbox.push_back(step);
            self.wanted
                .release(id, request.generation, session, self.now, true);
            self.settle();
            return;
        }
        self.sizes.entry(id).or_insert(size);
        let (waiter, step) = self.pool.submit(reservation, body, self.now);
        self.waiters.insert(waiter, VerdictOwner::Session(session));
        self.sessions
            .get_mut(&session)
            .expect("checked above")
            .submissions
            .insert(waiter, (id, request.generation));
        self.inbox.push_back(step);
        self.settle();
    }

    /// A `NotFound` arrived.
    pub(crate) fn not_found(&mut self, session: SessionId, id: UnminedTxId, now: Time) {
        self.enter(now);
        let Some(state) = self.sessions.get_mut(&session) else {
            return;
        };
        let Some(request) = state.requests.shift_remove(&id) else {
            self.violation(session, Violation::Unrequested);
            self.settle();
            return;
        };
        if state.take_pending(id).is_some() {
            self.wanted.unhold(id, request.generation);
        }
        state.mark_tried(id, request.generation);
        if let Some(reservation) = request.reservation {
            self.reservations.remove(&reservation);
            let step = self.pool.release(reservation, self.now);
            self.inbox.push_back(step);
            self.wanted
                .release(id, request.generation, session, self.now, false);
        }
        self.settle();
    }

    /// A `GetTxs` arrived.
    pub(crate) fn get_txs(&mut self, session: SessionId, id: UnminedTxId, now: Time) {
        self.enter(now);
        let Some(state) = self.sessions.get(&session) else {
            return;
        };
        if state.hello.is_none() {
            self.violation(session, Violation::NoHelloFirst);
            self.settle();
            return;
        }
        // The pool holds exact ids, so this compares the authorizing data
        // too.
        let servable = state.announced.contains(&id) && self.pool.visible().contains(&id);
        self.effects.push(if servable {
            RelayEffect::Serve { session, id }
        } else {
            RelayEffect::NotFound { session, id }
        });
        self.settle();
    }

    /// The session's reader fell off the change log.
    pub(crate) fn resync(&mut self, session: SessionId, now: Time) {
        self.enter(now);
        let visible = self.pool.visible();
        let visible_set: HashSet<UnminedTxId> = visible.iter().copied().collect();
        if let Some(state) = self.sessions.get_mut(&session) {
            if self.reactor.has_go(state.peer, session) {
                state.announced.retain(|id| visible_set.contains(id));
                state.held.retain(|(id, _)| visible_set.contains(id));
                for id in visible {
                    if !state.announced.contains(&id) && !state.knows(id) {
                        state.announced.insert(id);
                        self.effects.push(RelayEffect::Advert { session, id });
                    }
                }
            }
        }
        self.settle();
    }

    /// The session ended: the peer left, the connection closed, or the
    /// session was replaced.
    pub(crate) fn end(&mut self, session: SessionId, now: Time) {
        self.enter(now);
        self.end_session(session);
        self.settle();
    }

    // Inputs from the node.

    /// Time passed.
    pub(crate) fn tick(&mut self, now: Time) {
        self.enter(now);
        self.settle();
    }

    /// The outcome of a verification job.
    pub(crate) fn outcome(&mut self, job: JobId, outcome: Outcome, now: Time) {
        self.enter(now);
        self.jobs.retain(|running| *running != job);
        let stable = GenerationRead::Stable(self.generation);
        let step = self.pool.outcome(job, outcome, stable, stable, self.now);
        self.inbox.push_back(step);
        self.settle();
    }

    /// Another caller, such as the legacy adapter or a chain transition,
    /// uses the pool. The closure returns its answer and the step that
    /// caused, including [`PoolModel::effects`] after an input with an
    /// answer of its own.
    pub(crate) fn outside<R>(
        &mut self,
        now: Time,
        f: impl FnOnce(&mut PoolModel, Time) -> (R, Step),
    ) -> R {
        self.enter(now);
        let (answer, step) = f(&mut self.pool, self.now);
        self.inbox.push_back(step);
        self.settle();
        answer
    }

    /// The best chain changed.
    pub(crate) fn transition(&mut self, transition: ModelTransition, now: Time) {
        self.enter(now);
        self.generation = transition.generation;
        let step = self.pool.transition(transition, self.now);
        self.inbox.push_back(step);
        self.settle();
    }

    /// The node fell behind the tip and the pool turned off.
    pub(crate) fn deactivate(&mut self, now: Time) {
        self.enter(now);
        self.pool_live = false;
        let step = self.pool.deactivate(self.now);
        self.inbox.push_back(step);
        self.settle();
    }

    /// The pool is on again.
    pub(crate) fn activate(&mut self, now: Time) {
        self.enter(now);
        self.pool
            .activate(zakura_chain::block::Height(100), self.generation, self.now);
        let step = self.pool.effects();
        self.inbox.push_back(step);
        self.pool_live = true;
        let opened: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(session, state)| self.reactor.has_go(state.peer, **session))
            .map(|(session, _)| *session)
            .collect();
        for session in opened {
            self.open(session);
        }
        self.settle();
    }

    /// Whether a cooldown earned now may start.
    pub(crate) fn set_may_cool(&mut self, may_cool: bool) {
        self.may_cool = may_cool;
    }

    /// The reactor's ranking.
    pub(crate) fn set_ranking(&mut self, ranking: Vec<SessionId>) {
        self.reactor.set_ranking(ranking);
    }

    // Outputs and observations.

    /// What `tx-relay` did since the last call.
    pub(crate) fn take_effects(&mut self) -> Vec<RelayEffect> {
        std::mem::take(&mut self.effects)
    }

    /// The verification jobs the pool is running.
    pub(crate) fn jobs(&self) -> &[JobId] {
        &self.jobs
    }

    /// Pool effects addressed to other callers since the last call.
    pub(crate) fn take_outside(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.outside)
    }

    /// The pool.
    pub(crate) fn pool(&self) -> &PoolModel {
        &self.pool
    }

    /// The wanted table.
    pub(crate) fn wanted(&self) -> &WantedModel {
        &self.wanted
    }

    /// A session's state.
    pub(crate) fn session(&self, session: SessionId) -> Option<&PeerModel> {
        self.sessions.get(&session)
    }

    /// The reactor's state.
    pub(crate) fn reactor(&self) -> &ReactorModel {
        &self.reactor
    }

    /// Ids requested or awaiting a verdict for `peer`, over all its
    /// sessions and its ended ones.
    pub(crate) fn in_flight(&self, peer: PeerKey) -> usize {
        self.sessions
            .values()
            .filter(|state| state.peer == peer)
            .map(PeerModel::in_flight)
            .sum::<usize>()
            .saturating_add(self.reactor.unsettled_count(peer))
    }

    /// `tx-relay`'s reservations at the pool.
    pub(crate) fn reservations_held(&self) -> usize {
        self.reservations.len()
    }

    /// The earliest time something becomes due with no other input: a
    /// timed id, a deadline, an advert, the backoff, or a cooldown.
    pub(crate) fn next_deadline(&self) -> Option<Time> {
        let mut next: Option<Time> = self.wanted.backoff_until();
        let mut consider = |time: Time| {
            if time > self.now {
                next = Some(next.map_or(time, |next| next.min(time)));
            }
        };
        for (id, entry) in self.wanted.entries() {
            consider(entry.retry_at);
            consider(entry.first_seen.saturating_add(INBOUND_DELAY));
            if let Some(released) = entry.released_at {
                consider(released.saturating_add(INBOUND_DELAY));
            }
            let _ = id;
        }
        for state in self.sessions.values() {
            for request in state.requests.values() {
                if request.reservation.is_some() {
                    consider(request.deadline);
                }
            }
            for (_, due) in &state.held {
                consider(*due);
            }
            if let Some(until) = self.reactor.cooling_until(state.peer, self.now) {
                consider(until);
            }
        }
        next
    }

    // The step loop.

    /// Moves time to `now` and applies what is due before the input.
    fn enter(&mut self, now: Time) {
        assert!(now >= self.now, "time does not go back");
        self.now = now;
        let step = self.pool.tick(now);
        self.inbox.push_back(step);
        self.drain();
        self.expire();
        self.release_adverts();
    }

    /// Routes every pool step, then lets every session claim what it can.
    fn settle(&mut self) {
        self.drain();
        self.fill();
        self.drain();
    }

    /// Routes pool steps in the order the pool produced them. A step's
    /// changes come first, because the core publishes a batch before it
    /// delivers its verdicts.
    fn drain(&mut self) {
        while let Some(step) = self.inbox.pop_front() {
            if !step.changes.is_empty() {
                self.publish(&step.changes);
            }
            for effect in step.effects {
                self.route(effect);
            }
        }
    }

    /// Hands one pool effect to its owner.
    fn route(&mut self, effect: Effect) {
        match effect {
            Effect::Start { job, .. } => self.jobs.push(job),
            Effect::Cancel(job) => self.jobs.retain(|running| *running != job),
            Effect::Verdict { waiter, verdict } => match self.waiters.remove(&waiter) {
                Some(owner) => self.verdict(owner, waiter, verdict),
                None => self.outside.push(Effect::Verdict { waiter, verdict }),
            },
            Effect::CancelFetch(reservation) => match self.reservations.remove(&reservation) {
                Some((session, id)) => self.cancel_fetch(session, id, reservation),
                None => self.outside.push(Effect::CancelFetch(reservation)),
            },
            other => self.outside.push(other),
        }
    }

    /// A verdict for one of `tx-relay`'s submissions.
    fn verdict(&mut self, owner: VerdictOwner, waiter: WaiterId, verdict: ModelVerdict) {
        let (peer, id, generation, session) = match owner {
            VerdictOwner::Session(session) => {
                let state = self
                    .sessions
                    .get_mut(&session)
                    .expect("a session hands its submissions over when it ends");
                let (id, generation) = state
                    .submissions
                    .remove(&waiter)
                    .expect("a waiter's owner holds it");
                (state.peer, id, Some(generation), Some(session))
            }
            VerdictOwner::Reactor => {
                let (peer, id) = self
                    .reactor
                    .settle(waiter)
                    .expect("a waiter's owner holds it");
                (
                    peer,
                    id,
                    self.wanted.entry(id).map(|entry| entry.generation),
                    None,
                )
            }
        };
        // A verdict frees a pool slot.
        self.wanted.room();
        match verdict {
            ModelVerdict::Admitted => self.decided(id, generation),
            ModelVerdict::Rejected { charge, .. } => {
                self.decided(id, generation);
                self.charge(peer, charge);
            }
            ModelVerdict::NoRoom | ModelVerdict::LocalFault(_) => {
                if let (Some(session), Some(generation)) = (session, generation) {
                    self.wanted.release(id, generation, session, self.now, true);
                }
            }
        }
    }

    /// The pool no longer needs the body `session` was fetching.
    fn cancel_fetch(&mut self, session: SessionId, id: UnminedTxId, reservation: ReservationId) {
        let step = self.pool.release(reservation, self.now);
        self.inbox.push_back(step);
        let Some(state) = self.sessions.get_mut(&session) else {
            return;
        };
        let Some(request) = state.requests.get_mut(&id) else {
            return;
        };
        request.reservation = None;
        let generation = request.generation;
        if self.pool_live {
            // A block mined it.
            self.decided(id, Some(generation));
        } else {
            self.wanted
                .release(id, generation, session, self.now, false);
        }
    }

    /// `id` is decided. Its entry leaves, and every pending set drops it.
    fn decided(&mut self, id: UnminedTxId, generation: Option<EntryGen>) {
        let Some(generation) = generation else {
            return;
        };
        if self.wanted.decide(id, generation) {
            for state in self.sessions.values_mut() {
                state
                    .pending
                    .retain(|pending| !(pending.id == id && pending.generation == generation));
            }
        }
    }

    /// Applies a body's charge to the peer that supplied it.
    fn charge(&mut self, peer: PeerKey, charge: Charge) {
        match charge {
            Charge::None => {}
            Charge::Cooldown => {
                if !self.may_cool {
                    return;
                }
                if let Some(length) = self.reactor.record_invalid(peer, self.now) {
                    if length > BASE_COOLDOWN {
                        self.close_peer(peer, CloseReason::RepeatedCooldown);
                    }
                }
            }
            Charge::Ban => self.close_peer(peer, CloseReason::Ban),
        }
    }

    /// Closes every session of `peer`.
    fn close_peer(&mut self, peer: PeerKey, reason: CloseReason) {
        for session in self.reactor.sessions_of(peer) {
            self.effects.push(RelayEffect::Close { session, reason });
            self.end_session(session);
        }
    }

    /// Closes `session` for a broken wire rule.
    fn violation(&mut self, session: SessionId, violation: Violation) {
        self.effects.push(RelayEffect::Close {
            session,
            reason: CloseReason::Violation(violation),
        });
        self.end_session(session);
    }

    /// Ends `session`: frees its reservations and claims, gives up its
    /// pending ids, and hands its submissions to the reactor.
    fn end_session(&mut self, session: SessionId) {
        let Some(state) = self.sessions.remove(&session) else {
            return;
        };
        for (id, request) in &state.requests {
            if let Some(reservation) = request.reservation {
                self.reservations.remove(&reservation);
                let step = self.pool.release(reservation, self.now);
                self.inbox.push_back(step);
                self.wanted
                    .release(*id, request.generation, session, self.now, false);
            }
        }
        for (id, generation) in state.submissions.values() {
            self.wanted
                .release(*id, *generation, session, self.now, false);
        }
        for pending in &state.pending {
            self.wanted.unhold(pending.id, pending.generation);
        }
        for waiter in state.submissions.keys() {
            self.waiters.insert(*waiter, VerdictOwner::Reactor);
        }
        let handed = state
            .submissions
            .iter()
            .map(|(waiter, (id, _))| (*waiter, *id));
        if let Some(next) = self.reactor.end(state.peer, session, handed) {
            self.open(next);
        }
    }

    /// `session` got the go: it announces the whole pool, less what its peer
    /// still has in verification.
    fn open(&mut self, session: SessionId) {
        // Nothing is announced while the pool is off. The pool starts empty
        // when it comes back.
        let visible = if self.pool_live {
            self.pool.visible()
        } else {
            Vec::new()
        };
        let Some(state) = self.sessions.get_mut(&session) else {
            return;
        };
        let peer = state.peer;
        for (_, id) in self.reactor.unsettled(peer) {
            state.mark_known(id);
        }
        for id in visible {
            if !state.announced.contains(&id) && !state.knows(id) {
                state.announced.insert(id);
                self.effects.push(RelayEffect::Advert { session, id });
            }
        }
        let waiting = std::mem::take(&mut state.waiting);
        self.take_ids(session, &waiting);
    }

    /// Every session reads one published batch.
    fn publish(&mut self, changes: &[Change]) {
        // The snapshot published with a batch is the pool after the whole
        // batch, so an id the batch adds and then hides or removes is not in
        // it.
        let mut gone_later: HashMap<UnminedTxId, usize> = HashMap::new();
        for (index, change) in changes.iter().enumerate() {
            if let Change::Removed { id, .. } | Change::Hidden(id) = change {
                gone_later.insert(*id, index);
            }
        }
        let delay = self.advert_delay;
        let now = self.now;
        let sessions: Vec<SessionId> = self.sessions.keys().copied().collect();
        for session in sessions {
            let window = |id: UnminedTxId, sizes: &HashMap<UnminedTxId, u32>| {
                window(sizes.get(&id).copied())
            };
            let rank = self.reactor.rank(session);
            let state = self.sessions.get_mut(&session).expect("listed above");
            // Every session reads removals. Only one that may announce
            // announces.
            let announces = self.reactor.has_go(state.peer, session) && self.pool_live;
            for (index, change) in changes.iter().enumerate() {
                match *change {
                    Change::Added(id) | Change::Revealed { id, .. } => {
                        if !announces
                            || gone_later.get(&id).is_some_and(|gone| *gone > index)
                            || state.announced.contains(&id)
                            || state.knows(id)
                        {
                            continue;
                        }
                        if rank.is_some_and(|rank| rank < window(id, &self.sizes)) {
                            state.announced.insert(id);
                            self.effects.push(RelayEffect::Advert { session, id });
                        } else if let Some(delay) = delay {
                            if !state.held.iter().any(|(held, _)| *held == id) {
                                state.held.push((id, now.saturating_add(delay)));
                            }
                        }
                    }
                    Change::Hidden(id) => state.held.retain(|(held, _)| *held != id),
                    Change::Removed { id, .. } => state.forget(id),
                    Change::Refused(_) => {}
                }
            }
        }
    }

    /// Sends held adverts that are due.
    fn release_adverts(&mut self) {
        let now = self.now;
        if !self
            .sessions
            .values()
            .any(|state| state.held.iter().any(|(_, at)| *at <= now))
        {
            return;
        }
        let visible: HashSet<UnminedTxId> = self.pool.visible().into_iter().collect();
        let now = self.now;
        for (session, state) in &mut self.sessions {
            let (due, waiting): (Vec<_>, Vec<_>) =
                state.held.drain(..).partition(|(_, at)| *at <= now);
            state.held = waiting;
            for (id, _) in due {
                if visible.contains(&id) && !state.announced.contains(&id) && !state.knows(id) {
                    state.announced.insert(id);
                    self.effects.push(RelayEffect::Advert {
                        session: *session,
                        id,
                    });
                }
            }
        }
    }

    /// Applies request deadlines that passed.
    fn expire(&mut self) {
        let now = self.now;
        let mut freed = Vec::new();
        for (session, state) in &mut self.sessions {
            for (id, request) in &mut state.requests {
                if request.deadline > now {
                    continue;
                }
                if let Some(reservation) = request.reservation.take() {
                    freed.push((*session, *id, request.generation, reservation));
                    let count = state
                        .timed_out
                        .entry((*id, request.generation))
                        .or_insert(0);
                    *count = count.saturating_add(1);
                }
            }
        }
        for (session, id, generation, reservation) in freed {
            self.reservations.remove(&reservation);
            let step = self.pool.release(reservation, self.now);
            self.inbox.push_back(step);
            self.wanted
                .release(id, generation, session, self.now, false);
        }
        self.drain();
    }

    /// Every session with a free slot claims, reserves and asks, oldest
    /// session first.
    fn fill(&mut self) {
        if !self.pool_live {
            return;
        }
        let sessions: Vec<SessionId> = self.sessions.keys().copied().collect();
        'sessions: for session in sessions {
            loop {
                if self.wanted.in_backoff(self.now) {
                    return;
                }
                let Some(pending) = self.next_claim(session) else {
                    continue 'sessions;
                };
                let id = pending.id;
                self.wanted.claim(id, pending.generation, session);
                let answer = self.pool.reserve(id, Intent::Fetch, self.now);
                let step = self.pool.effects();
                self.inbox.push_back(step);
                match answer {
                    Ok(reservation) => {
                        assert!(
                            !reservation.joined && reservation.wants_body,
                            "a fetch of an undecided id is refused, never joined"
                        );
                        self.wanted.room();
                        // The id stays pending while it is requested, so
                        // this peer can be asked again after a late answer.
                        let state = self.sessions.get_mut(&session).expect("listed above");
                        state.send(pending, reservation.id, self.now);
                        self.reservations.insert(reservation.id, (session, id));
                        self.effects.push(RelayEffect::Ask { session, id });
                    }
                    Err(ModelUnreserved::NoRoom) => {
                        self.wanted
                            .release(id, pending.generation, session, self.now, false);
                        self.wanted.no_room(self.now);
                        self.drain();
                        return;
                    }
                    Err(ModelUnreserved::Pending) => {
                        self.wanted
                            .release(id, pending.generation, session, self.now, true);
                    }
                    Err(ModelUnreserved::Pooled | ModelUnreserved::Rejected(_)) => {
                        self.wanted
                            .release(id, pending.generation, session, self.now, false);
                        self.decided(id, Some(pending.generation));
                    }
                    Err(ModelUnreserved::Inactive) => {
                        self.wanted
                            .release(id, pending.generation, session, self.now, false);
                        self.drain();
                        return;
                    }
                }
                self.drain();
            }
        }
    }

    /// The oldest id `session` may claim now, if it has a free slot.
    fn next_claim(&self, session: SessionId) -> Option<Pending> {
        let state = self.sessions.get(&session)?;
        let peer = state.peer;
        if !self.reactor.has_go(peer, session) || self.reactor.is_cooling(peer, self.now) {
            return None;
        }
        let hello = state.hello?;
        let in_flight = state
            .in_flight()
            .saturating_add(self.reactor.unsettled_count(peer));
        if in_flight >= MAX_REQUESTS_PER_PEER || state.requests.len() >= hello.serves {
            return None;
        }
        state.pending.iter().copied().find(|pending| {
            let Some(entry) = self.wanted.current(pending.id, pending.generation) else {
                return false;
            };
            if entry.claim.is_some() || self.now < entry.retry_at {
                return false;
            }
            if !state.dialed && self.now < entry.first_seen.saturating_add(INBOUND_DELAY) {
                return false;
            }
            if state.requests.contains_key(&pending.id) {
                return false;
            }
            if state
                .timed_out
                .contains_key(&(pending.id, pending.generation))
            {
                let released = entry.released_at.unwrap_or(entry.first_seen);
                if self.now < released.saturating_add(INBOUND_DELAY) {
                    return false;
                }
            }
            true
        })
    }

    /// Checks the rules every reachable state keeps.
    ///
    /// # Panics
    ///
    /// If one is broken.
    pub(crate) fn assert_invariants(&self) {
        self.pool.assert_invariants();
        assert!(self.inbox.is_empty(), "every pool step is routed");

        // The limit of 5, once per peer.
        let peers: HashSet<PeerKey> = self.sessions.values().map(|state| state.peer).collect();
        for peer in peers {
            assert!(
                self.in_flight(peer) <= MAX_REQUESTS_PER_PEER,
                "peer {peer:?} has more than {MAX_REQUESTS_PER_PEER} in flight"
            );
        }

        let mut holders: HashMap<(UnminedTxId, EntryGen), usize> = HashMap::new();
        let mut reserved: HashMap<UnminedTxId, usize> = HashMap::new();
        let mut pending_total = 0usize;
        for (session, state) in &self.sessions {
            let go = self.reactor.has_go(state.peer, *session);
            if go {
                assert!(state.waiting.is_empty(), "the go takes the waiting ids in");
            } else {
                assert!(
                    state.requests.is_empty()
                        && state.submissions.is_empty()
                        && state.pending.is_empty(),
                    "only the session with the go fetches or holds table entries"
                );
                assert!(state.waiting.len() <= PENDING_PER_PEER);
            }
            if let Some(hello) = state.hello {
                assert!(
                    state.requests.len() <= hello.serves,
                    "within the peer's Hello"
                );
            } else {
                assert!(state.requests.is_empty(), "no request before the Hello");
            }
            assert!(state.pending.len() <= PENDING_PER_PEER);
            assert!(state.tried.len() <= TRIED_PER_PEER);
            assert!(state.known.len() <= KNOWN_CAP);
            pending_total = pending_total.saturating_add(state.pending.len());
            for pending in &state.pending {
                assert!(
                    !state.has_tried(pending.id, pending.generation),
                    "pending and tried are apart"
                );
                *holders.entry((pending.id, pending.generation)).or_default() += 1;
            }
            for (id, request) in &state.requests {
                if let Some(reservation) = request.reservation {
                    assert_eq!(
                        self.reservations.get(&reservation),
                        Some(&(*session, *id)),
                        "every live request's reservation is recorded"
                    );
                    *reserved.entry(*id).or_default() += 1;
                    let entry = self
                        .wanted
                        .current(*id, request.generation)
                        .expect("a live request's entry is current");
                    assert_eq!(
                        entry.claim,
                        Some(*session),
                        "a live request holds the claim"
                    );
                }
            }
            for (held, _) in &state.held {
                assert!(
                    !state.announced.contains(held),
                    "a held advert was not sent yet"
                );
            }
        }
        for (id, count) in reserved {
            assert!(
                count <= 1,
                "tx-relay holds at most one reservation for {id:?}"
            );
        }
        assert_eq!(
            self.reservations.len(),
            self.sessions
                .values()
                .flat_map(|state| state.requests.values())
                .filter(|request| request.reservation.is_some())
                .count(),
            "no reservation without a live request"
        );

        for (id, entry) in self.wanted.entries() {
            let counted = holders.remove(&(*id, entry.generation)).unwrap_or(0);
            assert_eq!(entry.holders, counted, "holder count of {id:?}");
            assert!(
                entry.holders > 0 || entry.claim.is_some(),
                "an entry nobody holds or claims is gone"
            );
            if let Some(session) = entry.claim {
                let state = self
                    .sessions
                    .get(&session)
                    .expect("a claim belongs to a live session");
                let live_request = state
                    .requests
                    .get(id)
                    .is_some_and(|request| request.reservation.is_some());
                let submitted = state
                    .submissions
                    .values()
                    .any(|(held, generation)| held == id && *generation == entry.generation);
                assert!(
                    live_request || submitted,
                    "a claim covers a fetch or a submission"
                );
            }
        }
        assert!(holders.is_empty(), "every pending reference has its entry");
        assert!(
            self.wanted.len()
                <= pending_total
                    .saturating_add(self.reservations.len())
                    .saturating_add(
                        self.sessions
                            .values()
                            .map(|state| state.submissions.len())
                            .sum()
                    ),
            "the table is bounded by the pending sets and the claims"
        );

        let pool: HashSet<UnminedTxId> = self
            .pool
            .visible()
            .into_iter()
            .chain(self.pool.hidden())
            .collect();
        for state in self.sessions.values() {
            for id in &state.announced {
                assert!(pool.contains(id), "the announced set holds only pooled ids");
            }
        }
    }
}

/// The fan-out window for a transaction of `size` bytes. Unknown sizes get
/// the smallest window, as block relay treats a block of unknown size.
pub(crate) fn window(size: Option<u32>) -> usize {
    let Some(size) = size else {
        return WINDOW_FULL;
    };
    let size = u64::from(size.clamp(SMALL_TX_BYTES, MAX_TX_BYTES));
    let below_full = u64::from(MAX_TX_BYTES).saturating_sub(size);
    let range = u64::from(MAX_TX_BYTES - SMALL_TX_BYTES);
    let spread = WINDOW_SMALL - WINDOW_FULL;
    let extra = u64::try_from(spread)
        .unwrap_or(u64::MAX)
        .saturating_mul(below_full)
        / range;
    WINDOW_FULL + usize::try_from(extra).unwrap_or(spread)
}
