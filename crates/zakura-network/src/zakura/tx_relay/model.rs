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
//!   pending ids with the size it announced, the ids it must wait for, its
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
//! - **Announcers not yet asked go first.** A `NotFound` takes the id out of
//!   that session's pending set, so it is asked again only if its peer
//!   announces the id again. A deadline frees the pool's slot and the claim,
//!   so another announcer can try. Either way the session waits
//!   [`INBOUND_DELAY`] after each release before it may claim that id
//!   again, so others claim it first. Only a missed deadline doubles the
//!   next one. A late body is dropped before it reaches the pool.
//! - **A rejection on the same chain settles the id for every holder.**
//!   Validity depends only on the chain, so a pool restart that clears the
//!   rejection cache does not make the transaction valid again. A rejection
//!   from before a chain change only makes the id be asked about again.
//! - **One step here is several in the machines.** A decision drops the id
//!   from every pending set at once. The real routines learn of it from the
//!   table's events, so a machine's state is compared with the model after
//!   leaving out references to entries that are no longer current, which the
//!   generation makes harmless meanwhile. For that to hold, a routine drops
//!   any reference whose entry is no longer current before it counts its
//!   room or skips an announced id as one it already holds.
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
//! Two calls cross a task boundary in the real service, and the model keeps
//! them apart so tests can interleave them: a reserve call is answered by a
//! separate input ([`RelayModel::with_manual_reserves`]), and each session
//! reads the change log from its own cursor with the snapshot the log leads
//! to ([`RelayModel::with_manual_reads`]). Everything else is one step.
//!
//! Left out, because other code owns them and keeps their own tests: the
//! byte rate on `Inv`, `Serve`'s grants and its over-limit rule, frames, and
//! the release ring and wake-up mechanics of the real routine. Each input
//! applies whatever is due first, as the real routine's wake-ups must.

pub(crate) mod peer;
pub(crate) mod reactor;
pub(crate) mod wanted;

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    time::Duration,
};

use indexmap::IndexSet;
use zakura_chain::{block::Height, transaction::UnminedTxId};
use zakura_node_services::mempool::port::{
    model::{
        Effect, JobId, ModelReservation, ModelTransition, ModelTx, ModelUnreserved, ModelVerdict,
        Outcome, PoolModel, Step, WaiterId,
    },
    Change, Charge, Generation, GenerationRead, Intent, ReservationId,
};

use self::{
    peer::{Hello, PeerModel, Pending, Reserving},
    reactor::ReactorModel,
    wanted::{Admission, WantedModel},
};

/// Time since the model started.
pub(crate) type Time = Duration;

/// Ids requested or awaiting a verdict for one peer, at most.
pub(crate) const MAX_REQUESTS_PER_PEER: usize = 5;

/// Ids one session may hold pending: a whole pool at the default size.
pub(crate) const PENDING_PER_PEER: usize = 8_000;

/// Ids one session remembers it must wait for after a release, because it
/// answered `NotFound` for them.
pub(crate) const DEFERRED_PER_PEER: usize = 8_000;

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

/// A transaction's size unless a test gives another: a small transparent
/// one.
pub(crate) const DEFAULT_TX_BYTES: u32 = 300;

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

/// A reserve call on its way to the pool.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Call {
    session: SessionId,
    id: UnminedTxId,
    /// Names this call, so an old call's answer never acts on a newer call
    /// for the same id.
    call: u64,
    /// The chain generation of the snapshot the caller held when it made
    /// the call.
    generation: Generation,
}

/// The pool's answer to a reserve call, on its way back.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Answer {
    call: Call,
    answer: Result<ModelReservation, ModelUnreserved>,
    /// The pool cancelled the granted reservation before the caller
    /// received it. The caller sees only the cancelled token, not why.
    cancelled: bool,
}

/// A verdict on its way back to `tx-relay`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Delivery {
    waiter: WaiterId,
    owner: VerdictOwner,
    verdict: ModelVerdict,
    /// The tip the pool decided it at.
    decided_at: Height,
}

/// `tx-relay` with the pool behind it.
#[derive(Debug)]
pub(crate) struct RelayModel {
    now: Time,
    pool: PoolModel,
    /// The tip and stable generation the pool has applied. A verification
    /// reports this generation as read, and a verdict is decided at this
    /// tip.
    tip: Height,
    generation: Generation,
    /// The node's best tip, which can run ahead of the pool's.
    best_tip: Height,
    /// Whether the node is close to the network's tip.
    current: bool,
    pool_live: bool,
    wanted: WantedModel,
    reactor: ReactorModel,
    sessions: BTreeMap<SessionId, PeerModel>,
    next_session: u64,
    /// Which session holds each of `tx-relay`'s reservations, and for which
    /// id.
    reservations: HashMap<ReservationId, (SessionId, UnminedTxId)>,
    /// Who waits for each of `tx-relay`'s submissions, and the chain
    /// generation of the snapshot its submitter held when it submitted.
    waiters: HashMap<WaiterId, VerdictOwner>,
    submitted_at: HashMap<WaiterId, Generation>,
    /// The serialized size of every transaction, which the real routine
    /// reads from the snapshot. The model's transactions carry no size, so
    /// the test gives one function for all of them.
    tx_size: fn(UnminedTxId) -> u32,
    /// The short id the real skip set keeps for each transaction. Two ids
    /// with the same short id collide.
    short_id: fn(UnminedTxId) -> u64,
    /// How long sessions outside an id's window wait. `None` never announces
    /// to them.
    advert_delay: Option<Duration>,
    /// Whether reserve calls are answered at once. Off, they wait for
    /// [`Self::answer_reserve`].
    auto_reserves: bool,
    /// Whether sessions read the change log at once. Off, they wait for
    /// [`Self::read`].
    auto_reads: bool,
    /// Whether verdicts reach `tx-relay` at once. Off, they wait for
    /// [`Self::deliver_verdict`].
    auto_verdicts: bool,
    /// The change log, newest last, at most `log_capacity` entries.
    log: VecDeque<(u64, Change)>,
    log_capacity: usize,
    last_seq: u64,
    /// The visible pool and the hidden transactions, as the log says.
    visible: IndexSet<UnminedTxId>,
    hidden: HashSet<UnminedTxId>,
    /// Reserve calls the pool has not processed, oldest first.
    calls: VecDeque<Call>,
    /// Answers the pool decided and the caller has not received.
    answers: VecDeque<Answer>,
    /// Verdicts the pool decided and `tx-relay` has not received.
    deliveries: VecDeque<Delivery>,
    /// Names the next reserve call.
    next_call: u64,
    /// Pool steps waiting to be routed, oldest first.
    inbox: VecDeque<Step>,
    /// What `tx-relay` did, in order, until the test takes it.
    effects: Vec<RelayEffect>,
    /// Verification jobs the pool started and has not finished.
    jobs: Vec<(JobId, UnminedTxId, Time)>,
    /// Pool effects for other callers, such as the legacy adapter.
    outside: Vec<Effect>,
}

impl RelayModel {
    /// `tx-relay` on an empty, active pool at height 100.
    pub(crate) fn new(pool: PoolModel) -> Self {
        Self {
            now: Duration::ZERO,
            pool,
            tip: Height(100),
            generation: Generation(0),
            best_tip: Height(100),
            current: true,
            pool_live: true,
            wanted: WantedModel::default(),
            reactor: ReactorModel::default(),
            sessions: BTreeMap::new(),
            next_session: 0,
            reservations: HashMap::new(),
            waiters: HashMap::new(),
            submitted_at: HashMap::new(),
            tx_size: |_| DEFAULT_TX_BYTES,
            short_id: default_short_id,
            advert_delay: Some(ADVERT_DELAY),
            auto_reserves: true,
            auto_reads: true,
            auto_verdicts: true,
            log: VecDeque::new(),
            log_capacity: usize::MAX,
            last_seq: 0,
            visible: IndexSet::new(),
            hidden: HashSet::new(),
            calls: VecDeque::new(),
            answers: VecDeque::new(),
            deliveries: VecDeque::new(),
            next_call: 0,
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

    /// The same model with reserve calls processed by
    /// [`Self::process_reserve`] and their answers received by
    /// [`Self::deliver_reserve`], so calls and answers can be in flight
    /// together and the pool can act between deciding and delivering.
    pub(crate) fn with_manual_reserves(mut self) -> Self {
        self.auto_reserves = false;
        self
    }

    /// The same model with verdicts received only by
    /// [`Self::deliver_verdict`], so the chain can move between a verdict's
    /// decision and its delivery.
    pub(crate) fn with_manual_verdicts(mut self) -> Self {
        self.auto_verdicts = false;
        self
    }

    /// The same model with another short id for each transaction, so tests
    /// can force collisions in the skip set.
    pub(crate) fn with_short_id(mut self, short_id: fn(UnminedTxId) -> u64) -> Self {
        self.short_id = short_id;
        self
    }

    /// The same model with another size for each transaction.
    pub(crate) fn with_tx_size(mut self, tx_size: fn(UnminedTxId) -> u32) -> Self {
        self.tx_size = tx_size;
        self
    }

    /// The same model with sessions reading the change log only on
    /// [`Self::read`], and a log of `capacity` entries, so a session can lag
    /// and fall off it.
    pub(crate) fn with_manual_reads(mut self, capacity: usize) -> Self {
        self.auto_reads = false;
        self.log_capacity = capacity.max(1);
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
            return self.settle();
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
            return self.settle();
        };
        if self.reactor.is_cooling(peer, self.now) {
            return self.settle();
        }
        if self.reactor.has_go(peer, session) {
            self.take_ids(session, entries);
        } else {
            // A session waiting for the go keeps its announcements to itself,
            // filtered and bounded as the pending set is, so one peer never
            // holds two shares of the table.
            let seq = self.last_seq;
            let short = self.short_id;
            let state = self.sessions.get_mut(&session).expect("checked above");
            for (id, size) in entries.iter().copied() {
                state.mark_known(short(id), seq);
                if self.visible.contains(&id)
                    || size > MAX_TX_BYTES
                    || state.waiting.contains_key(&id)
                {
                    continue;
                }
                if state.waiting.len() < PENDING_PER_PEER {
                    state.waiting.insert(id, size);
                } else {
                    state.overflow = state.overflow.saturating_add(1);
                }
            }
        }
        self.settle();
    }

    /// A `Tx` arrived for `id`, carrying `body`. Its size on the wire is
    /// the body's own size.
    pub(crate) fn tx(&mut self, session: SessionId, id: UnminedTxId, body: ModelTx, now: Time) {
        self.enter(now);
        let seq = self.last_seq;
        let size = (self.tx_size)(body.id);
        let short = (self.short_id)(id);
        let Some(state) = self.sessions.get_mut(&session) else {
            return self.settle();
        };
        let Some(request) = state.requests.get(&id).copied() else {
            self.violation(session, Violation::Unrequested);
            self.settle();
            return;
        };
        let peer = state.peer;
        let Some(reservation) = request.reservation else {
            // Late: the node stopped waiting. The body is dropped before it
            // is checked or decoded, with no penalty.
            state.requests.shift_remove(&id);
            state.reannounced.remove(&id);
            self.settle();
            return;
        };
        if self.reactor.is_cooling(peer, self.now) {
            // The peer started cooling while the body was in flight. Dropped
            // unread, as the legacy adapter drops it.
            state.requests.shift_remove(&id);
            state.reannounced.remove(&id);
            self.reservations.remove(&reservation);
            let step = self.pool.release(reservation, self.now);
            self.inbox.push_back(step);
            self.wanted
                .release(id, request.generation, session, self.now, true);
            self.settle();
            return;
        }
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
        state.reannounced.remove(&id);
        state.mark_known(short, seq);
        self.reservations.remove(&reservation);
        let (waiter, step) = self.pool.submit(reservation, body, self.now);
        self.waiters.insert(waiter, VerdictOwner::Session(session));
        self.submitted_at.insert(waiter, self.generation);
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
            return self.settle();
        };
        let Some(request) = state.requests.shift_remove(&id) else {
            self.violation(session, Violation::Unrequested);
            self.settle();
            return;
        };
        // An answer about an older generation leaves a newer pending entry
        // alone, and so does an old answer the peer has since contradicted by
        // announcing the id again.
        let contradicted = state.reannounced.remove(&id);
        if !contradicted && state.take_pending_of(id, request.generation).is_some() {
            self.wanted.unhold(id, request.generation);
        }
        // Either way it waits after the release, so announcers not yet asked
        // claim it first. Only a missed deadline doubles the next one.
        let now = self.now;
        state.defer(id, request.generation, now.saturating_add(INBOUND_DELAY));
        if let Some(reservation) = request.reservation {
            // The fetch ended without a body, which the pool logs.
            self.reservations.remove(&reservation);
            let step = self.pool.fetch_failed(reservation, self.now);
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
            return self.settle();
        };
        let Some(hello) = state.hello else {
            self.violation(session, Violation::NoHelloFirst);
            self.settle();
            return;
        };
        // Exact ids: this compares the authorizing data too. A body above the
        // requester's limit is never sent.
        let servable = state.announced.contains_key(&id)
            && self.visible.contains(&id)
            && (self.tx_size)(id) <= hello.max_tx_bytes;
        self.effects.push(if servable {
            RelayEffect::Serve { session, id }
        } else {
            RelayEffect::NotFound { session, id }
        });
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

    /// The pool processes the oldest reserve call and the caller receives
    /// the oldest answer. Returns whether there was a call or an answer.
    pub(crate) fn answer_reserve(&mut self, now: Time) -> bool {
        self.enter(now);
        let processed = self.calls.pop_front().map(|call| self.process(call));
        let delivered = self.answers.pop_front().map(|answer| self.deliver(answer));
        self.settle();
        processed.is_some() || delivered.is_some()
    }

    /// The pool processes the oldest reserve call. Its answer waits for
    /// [`Self::deliver_reserve`]. Returns whether there was one.
    pub(crate) fn process_reserve(&mut self, now: Time) -> bool {
        self.enter(now);
        let processed = self.calls.pop_front().map(|call| self.process(call));
        self.settle();
        processed.is_some()
    }

    /// The caller receives the oldest answer the pool decided. Returns
    /// whether there was one.
    pub(crate) fn deliver_reserve(&mut self, now: Time) -> bool {
        self.enter(now);
        let delivered = self.answers.pop_front().map(|answer| self.deliver(answer));
        self.settle();
        delivered.is_some()
    }

    /// `session` receives the oldest answer the pool decided for it, ahead
    /// of other callers' answers. Returns whether there was one.
    pub(crate) fn deliver_reserve_for(&mut self, session: SessionId, now: Time) -> bool {
        self.enter(now);
        let index = self
            .answers
            .iter()
            .position(|answer| answer.call.session == session);
        let delivered = index
            .and_then(|index| self.answers.remove(index))
            .map(|answer| self.deliver(answer));
        self.settle();
        delivered.is_some()
    }

    /// `tx-relay` receives the oldest verdict the pool decided. Returns
    /// whether there was one.
    pub(crate) fn deliver_verdict(&mut self, now: Time) -> bool {
        self.enter(now);
        let delivered = self
            .deliveries
            .pop_front()
            .map(|delivery| self.verdict(delivery));
        self.settle();
        delivered.is_some()
    }

    /// `session` gives up a reserve call it made for `id`, before or after
    /// the pool processed it. The claim and any probe permit go back.
    pub(crate) fn cancel_reserve(&mut self, session: SessionId, id: UnminedTxId, now: Time) {
        self.enter(now);
        if let Some(reserving) = self
            .sessions
            .get_mut(&session)
            .and_then(|state| state.reserving.shift_remove(&id))
        {
            self.wanted
                .release(id, reserving.pending.generation, session, self.now, false);
            self.return_probe(reserving.probe);
        }
        self.settle();
    }

    /// `session` reads the change log from its cursor, with the snapshot the
    /// log leads to.
    pub(crate) fn read(&mut self, session: SessionId, now: Time) {
        self.enter(now);
        self.read_log(session);
        self.settle();
    }

    /// The outcome of a verification job.
    pub(crate) fn outcome(&mut self, job: JobId, outcome: Outcome, now: Time) {
        self.enter(now);
        self.jobs.retain(|(running, _, _)| *running != job);
        let stable = GenerationRead::Stable(self.generation);
        let step = self.pool.outcome(job, outcome, stable, stable, self.now);
        self.inbox.push_back(step);
        self.settle();
    }

    /// The best chain changed and the pool applies it.
    pub(crate) fn transition(&mut self, transition: ModelTransition, now: Time) {
        self.enter(now);
        self.tip = transition.tip;
        self.best_tip = transition.tip;
        self.generation = transition.generation;
        let step = self.pool.transition(transition, self.now);
        self.inbox.push_back(step);
        self.settle();
    }

    /// Another caller, such as the legacy adapter, uses the pool. The
    /// closure returns its answer and the step that caused, including
    /// [`PoolModel::effects`] after an input with an answer of its own.
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

    /// The node fell behind the tip and the pool turned off.
    pub(crate) fn deactivate(&mut self, now: Time) {
        self.enter(now);
        self.pool_live = false;
        let step = self.pool.deactivate(self.now);
        self.inbox.push_back(step);
        self.settle();
    }

    /// The pool is on again, at the latest tip.
    pub(crate) fn activate(&mut self, now: Time) {
        self.enter(now);
        self.pool.activate(self.tip, self.generation, self.now);
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

    /// Whether the node is close to the network's tip.
    pub(crate) fn set_current(&mut self, current: bool) {
        self.current = current;
    }

    /// The node's best tip moved on without the pool applying it yet.
    pub(crate) fn set_best_tip(&mut self, tip: Height) {
        self.best_tip = tip;
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
    pub(crate) fn jobs(&self) -> Vec<JobId> {
        self.jobs.iter().map(|(job, _, _)| *job).collect()
    }

    /// The transaction each running job verifies, and when the pool started
    /// it.
    pub(crate) fn job_txs(&self) -> &[(JobId, UnminedTxId, Time)] {
        &self.jobs
    }

    /// Pool effects addressed to other callers since the last call.
    pub(crate) fn take_outside(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.outside)
    }

    /// Reserve calls the pool has not processed, and answers the caller has
    /// not received.
    pub(crate) fn calls_in_flight(&self) -> usize {
        self.calls.len().saturating_add(self.answers.len())
    }

    /// Every change the pool logged that the model still keeps, oldest
    /// first.
    pub(crate) fn logged(&self) -> Vec<Change> {
        self.log.iter().map(|(_, change)| *change).collect()
    }

    /// Verdicts `tx-relay` has not received.
    pub(crate) fn verdicts_in_flight(&self) -> usize {
        self.deliveries.len()
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

    /// Ids reserving, requested or awaiting a verdict for `peer`, over all
    /// its sessions and its ended ones.
    pub(crate) fn in_flight(&self, peer: PeerKey) -> usize {
        self.sessions
            .values()
            .filter(|state| state.peer == peer)
            .map(PeerModel::in_flight)
            .sum::<usize>()
            .saturating_add(self.reactor.unsettled_count(peer))
    }

    /// Submissions still waiting for a verdict, over every owner.
    pub(crate) fn waiters_outstanding(&self) -> usize {
        self.waiters.len()
    }

    /// `tx-relay`'s reservations at the pool.
    pub(crate) fn reservations_held(&self) -> usize {
        self.reservations.len()
    }

    /// The earliest time after now that something becomes due with no other
    /// input: a timed id, a deadline, an advert, the end of the backoff, or a
    /// cooldown.
    pub(crate) fn next_deadline(&self) -> Option<Time> {
        let now = self.now;
        let mut next: Option<Time> = None;
        let mut consider = |time: Time| {
            if time > now {
                next = Some(next.map_or(time, |next| next.min(time)));
            }
        };
        if let Some(until) = self.wanted.backoff_until(now) {
            consider(until);
        }
        for (_, entry) in self.wanted.entries() {
            consider(entry.retry_at);
            consider(entry.first_seen.saturating_add(INBOUND_DELAY));
            if let Some(released) = entry.released_at {
                consider(released.saturating_add(INBOUND_DELAY));
            }
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
            for (_, _, until) in &state.deferred {
                consider(*until);
            }
            if let Some(until) = self.reactor.cooling_until(state.peer, now) {
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
                self.publish(step.changes);
            }
            for effect in step.effects {
                self.route(effect);
            }
        }
    }

    /// Appends a published batch to the log. With automatic reads, every
    /// session reads it at once.
    fn publish(&mut self, changes: Vec<Change>) {
        for change in changes {
            match change {
                Change::Added(id) => {
                    self.visible.insert(id);
                }
                Change::Revealed { id, .. } => {
                    self.hidden.remove(&id);
                    self.visible.insert(id);
                }
                Change::Hidden(id) => {
                    self.visible.shift_remove(&id);
                    self.hidden.insert(id);
                }
                Change::Removed { id, .. } => {
                    self.visible.shift_remove(&id);
                    self.hidden.remove(&id);
                }
                Change::Refused(_) => {}
            }
            self.last_seq = self.last_seq.saturating_add(1);
            if self.log.len() >= self.log_capacity {
                self.log.pop_front();
            }
            self.log.push_back((self.last_seq, change));
        }
        if self.auto_reads {
            let sessions: Vec<SessionId> = self.sessions.keys().copied().collect();
            for session in sessions {
                self.read_log(session);
            }
        }
    }

    /// One session reads the log from its cursor. The snapshot it reads
    /// with is the pool after the newest entry.
    fn read_log(&mut self, session: SessionId) {
        let Some(state) = self.sessions.get(&session) else {
            return;
        };
        if !self.reactor.has_go(state.peer, session) {
            return;
        }
        let rank = self.reactor.rank(session);
        let oldest = self
            .log
            .front()
            .map_or(self.last_seq.saturating_add(1), |(seq, _)| *seq);
        let state = self.sessions.get_mut(&session).expect("checked above");
        if state.cursor >= self.last_seq {
            return;
        }
        if state.cursor.saturating_add(1) < oldest {
            // It fell off the log and cannot know what it missed, so it
            // rebuilds from the snapshot. The skip set is not pruned: a
            // snapshot cannot tell a hidden transaction from a removed one.
            let visible = &self.visible;
            state.announced.retain(|id, _| visible.contains(id));
            state.held.retain(|(id, _)| visible.contains(id));
            if self.pool_live {
                for id in visible.iter().copied() {
                    if !state.announced.contains_key(&id) && !state.knows((self.short_id)(id)) {
                        state.announced.insert(id, self.last_seq);
                        state.held.retain(|(held, _)| *held != id);
                        self.effects.push(RelayEffect::Advert { session, id });
                    }
                }
            }
            state.cursor = self.last_seq;
            return;
        }
        // Only an id's last change in this read decides an advert, so an
        // addition the read also sees removed and added again is announced
        // once.
        let mut last: HashMap<UnminedTxId, u64> = HashMap::new();
        for (seq, change) in &self.log {
            if *seq > state.cursor {
                let id = match *change {
                    Change::Added(id)
                    | Change::Revealed { id, .. }
                    | Change::Hidden(id)
                    | Change::Removed { id, .. }
                    | Change::Refused(id) => id,
                };
                last.insert(id, *seq);
            }
        }
        for (seq, change) in &self.log {
            if *seq <= state.cursor {
                continue;
            }
            match *change {
                Change::Added(id) | Change::Revealed { id, .. } => {
                    if last.get(&id) != Some(seq)
                        || !self.pool_live
                        || !self.visible.contains(&id)
                        || state.announced.contains_key(&id)
                        || state.knows((self.short_id)(id))
                        || state.held.iter().any(|(held, _)| *held == id)
                    {
                        continue;
                    }
                    let window = window(Some((self.tx_size)(id)));
                    if rank.is_some_and(|rank| rank < window) {
                        state.announced.insert(id, *seq);
                        self.effects.push(RelayEffect::Advert { session, id });
                    } else if let Some(delay) = self.advert_delay {
                        state.held.push((id, self.now.saturating_add(delay)));
                    }
                }
                Change::Hidden(id) => state.held.retain(|(held, _)| *held != id),
                Change::Removed { id, .. } => state.forget(id, (self.short_id)(id), *seq),
                Change::Refused(_) => {}
            }
        }
        state.cursor = self.last_seq;
    }

    /// Hands one pool effect to its owner.
    fn route(&mut self, effect: Effect) {
        match effect {
            Effect::Start { job, tx, .. } => self.jobs.push((job, tx, self.now)),
            Effect::Cancel(job) => self.jobs.retain(|(running, _, _)| *running != job),
            Effect::Verdict { waiter, verdict } => match self.waiters.get(&waiter).copied() {
                Some(owner) => {
                    let delivery = Delivery {
                        waiter,
                        owner,
                        verdict,
                        decided_at: self.tip,
                    };
                    if self.auto_verdicts {
                        self.verdict(delivery);
                    } else {
                        self.deliveries.push_back(delivery);
                    }
                }
                None => self.outside.push(Effect::Verdict { waiter, verdict }),
            },
            Effect::CancelFetch(reservation) => {
                if let Some((session, id)) = self.reservations.remove(&reservation) {
                    self.cancel_fetch(session, id, reservation);
                } else if let Some(answer) = self
                    .answers
                    .iter_mut()
                    .find(|answer| answer.answer.is_ok_and(|granted| granted.id == reservation))
                {
                    // Granted, and cancelled before its caller heard.
                    answer.cancelled = true;
                } else {
                    self.outside.push(Effect::CancelFetch(reservation));
                }
            }
            other => self.outside.push(other),
        }
    }

    /// A verdict for one of `tx-relay`'s submissions reaches its owner. The
    /// owner may have changed since the pool decided it, when a session
    /// ended, so it is looked up again.
    fn verdict(&mut self, delivery: Delivery) {
        let Delivery {
            waiter,
            verdict,
            decided_at,
            ..
        } = delivery;
        let Some(owner) = self.waiters.remove(&waiter) else {
            return;
        };
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
                (state.peer, id, generation, Some(session))
            }
            VerdictOwner::Reactor => {
                let (peer, id, generation) = self
                    .reactor
                    .settle(waiter)
                    .expect("a waiter's owner holds it");
                (peer, id, generation, None)
            }
        };
        let submitted_at = self
            .submitted_at
            .remove(&waiter)
            .expect("every submission records its generation");
        // A verdict frees a pool slot.
        self.wanted.room();
        match verdict {
            ModelVerdict::Admitted => {
                // It settles the id only if the current snapshot holds it.
                if self.visible.contains(&id) {
                    self.verdict_decides(id);
                } else if let Some(session) = session {
                    self.wanted
                        .release(id, generation, session, self.now, false);
                }
            }
            ModelVerdict::Rejected { charge, .. } => {
                if self.generation == submitted_at {
                    self.verdict_decides(id);
                } else if let Some(session) = session {
                    // The chain changed since the body was submitted, and the
                    // rejection may be forgotten. Another announcer may still
                    // supply it.
                    self.wanted.release(id, generation, session, self.now, true);
                }
                self.charge(peer, charge, decided_at);
            }
            ModelVerdict::NoRoom | ModelVerdict::LocalFault(_) => {
                if let Some(session) = session {
                    self.wanted.release(id, generation, session, self.now, true);
                }
            }
        }
    }

    /// A confirmed verdict is about the id on this chain, so it settles the
    /// id's current entry for every holder, whichever life of the entry the
    /// body was fetched in. If another announcer is fetching it, that fetch
    /// ends with it.
    fn verdict_decides(&mut self, id: UnminedTxId) {
        if let Some(current) = self.wanted.entry(id).map(|entry| entry.generation) {
            self.decided(id, current);
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
        // The token says only that the pool no longer needs the body, not
        // why. The id is asked about again, and the current pool answers.
        self.wanted
            .release(id, generation, session, self.now, false);
    }

    /// `id` is decided in `generation`. Its entry leaves, and every pending
    /// set drops it. A decision about an older generation changes nothing.
    fn decided(&mut self, id: UnminedTxId, generation: EntryGen) {
        let claim = self
            .wanted
            .current(id, generation)
            .and_then(|entry| entry.claim);
        if !self.wanted.decide(id, generation) {
            return;
        }
        // A fetch of the decided id ends with it: its slot goes back, and the
        // request stays outstanding with its answer dropped. A reserve call
        // in flight finds its claim gone and gives back what it gets.
        if let Some(session) = claim {
            if let Some(state) = self.sessions.get_mut(&session) {
                if let Some(request) = state
                    .requests
                    .get_mut(&id)
                    .filter(|request| request.generation == generation)
                {
                    if let Some(reservation) = request.reservation.take() {
                        self.reservations.remove(&reservation);
                        let step = self.pool.release(reservation, self.now);
                        self.inbox.push_back(step);
                    }
                }
                if let Some(reserving) = state
                    .reserving
                    .get(&id)
                    .filter(|reserving| reserving.pending.generation == generation)
                    .copied()
                {
                    state.reserving.shift_remove(&id);
                    if let Some(probe) = reserving.probe {
                        self.wanted.return_probe(probe);
                    }
                }
            }
        }
        for state in self.sessions.values_mut() {
            state.drop_decided(id, generation);
        }
    }

    /// Applies a body's charge to the peer that supplied it. `decided_at` is
    /// the tip the verdict was decided at.
    fn charge(&mut self, peer: PeerKey, charge: Charge, decided_at: Height) {
        match charge {
            Charge::None => {}
            Charge::Cooldown => {
                // The legacy adapter's two conditions.
                if !self.current || self.best_tip != decided_at {
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

    /// Ends `session` in one step: frees its reservations, claims and probe
    /// permit, gives up its pending ids, and hands its submissions to the
    /// reactor with their generations.
    fn end_session(&mut self, session: SessionId) {
        let Some(state) = self.sessions.remove(&session) else {
            return;
        };
        for (id, reserving) in &state.reserving {
            self.wanted
                .release(*id, reserving.pending.generation, session, self.now, false);
            if let Some(generation) = reserving.probe {
                self.wanted.return_probe(generation);
            }
        }
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
            .map(|(waiter, held)| (*waiter, *held));
        if let Some(next) = self.reactor.end(state.peer, session, handed) {
            self.open(next);
        }
    }

    /// `session` got the go: it announces the whole pool, less what its peer
    /// still has in verification, and takes in the ids it was announced
    /// while waiting.
    fn open(&mut self, session: SessionId) {
        let Some(state) = self.sessions.get_mut(&session) else {
            return;
        };
        let peer = state.peer;
        let seq = self.last_seq;
        for id in self.reactor.unsettled_ids(peer) {
            state.mark_known((self.short_id)(id), seq);
        }
        // A session that had the go before, such as one the pool's restart
        // reopens, may not have read everything. It starts again from the
        // snapshot, as a resync does.
        let visible = &self.visible;
        state.announced.retain(|id, _| visible.contains(id));
        state.held.retain(|(id, _)| visible.contains(id));
        state.cursor = seq;
        if self.pool_live {
            for id in self.visible.iter().copied() {
                if !state.announced.contains_key(&id) && !state.knows((self.short_id)(id)) {
                    state.announced.insert(id, seq);
                    state.held.retain(|(held, _)| *held != id);
                    self.effects.push(RelayEffect::Advert { session, id });
                }
            }
        }
        let waiting: Vec<(UnminedTxId, u32)> =
            std::mem::take(&mut state.waiting).into_iter().collect();
        self.take_ids(session, &waiting);
    }

    /// Takes announced ids into a session that has the go: its pending set,
    /// and the table.
    fn take_ids(&mut self, session: SessionId, entries: &[(UnminedTxId, u32)]) {
        let mut fresh = Vec::new();
        let seq = self.last_seq;
        let short = self.short_id;
        {
            let Some(state) = self.sessions.get_mut(&session) else {
                return;
            };
            for (id, size) in entries.iter().copied() {
                state.mark_known(short(id), seq);
                if self.visible.contains(&id) || size > MAX_TX_BYTES {
                    continue;
                }
                let current = self.wanted.entry(id).map(|entry| entry.generation);
                // A peer that answered `NotFound` is taken back by a fresh
                // announcement: it says the peer has the id now.
                let already = current.is_some_and(|generation| state.holds(id, generation));
                // A request whose fetch was given up, such as one for an older
                // generation, does not stop a fresh announcement. The id is
                // asked for again only once that request is answered.
                let busy = state.reserving.contains_key(&id)
                    || state
                        .requests
                        .get(&id)
                        .is_some_and(|request| request.reservation.is_some())
                    || state
                        .submissions
                        .values()
                        .any(|(held, generation)| *held == id && Some(*generation) == current);
                // Announcing an id it already holds while a request for it is
                // outstanding says it has the id now. A `NotFound` for that
                // request, which can arrive later on the other stream, then
                // does not take the id out of its pending set.
                if let Some(generation) = current {
                    if state.requests.contains_key(&id) && state.holds(id, generation) {
                        state.reannounced.insert(id);
                    }
                }
                if already || busy || fresh.iter().any(|(held, _)| *held == id) {
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

    /// Sends held adverts that are due.
    fn release_adverts(&mut self) {
        let now = self.now;
        for (session, state) in &mut self.sessions {
            if !state.held.iter().any(|(_, at)| *at <= now) {
                continue;
            }
            let (due, waiting): (Vec<_>, Vec<_>) =
                state.held.drain(..).partition(|(_, at)| *at <= now);
            state.held = waiting;
            for (id, _) in due {
                if self.visible.contains(&id)
                    && !state.announced.contains_key(&id)
                    && !state.knows((self.short_id)(id))
                {
                    state.announced.insert(id, self.last_seq);
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
            let mut expired = Vec::new();
            for (id, request) in &mut state.requests {
                if request.deadline > now {
                    continue;
                }
                if let Some(reservation) = request.reservation.take() {
                    expired.push((*id, request.generation, reservation));
                }
            }
            for (id, generation, reservation) in expired {
                freed.push((*session, id, generation, reservation));
                if state.holds(id, generation) {
                    let count = state.timed_out.entry((id, generation)).or_insert(0);
                    *count = count.saturating_add(1);
                }
                state.defer(id, generation, now.saturating_add(INBOUND_DELAY));
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

    /// Every session with a free slot claims and starts a reserve call,
    /// oldest session first. During a backoff only the probe's holder may.
    fn fill(&mut self) {
        if !self.pool_live {
            return;
        }
        let sessions: Vec<SessionId> = self.sessions.keys().copied().collect();
        'sessions: for session in sessions {
            loop {
                let probe = match self.wanted.admission(session, self.now) {
                    Admission::Closed => return,
                    Admission::Open => None,
                    Admission::Probe(generation) => Some(generation),
                };
                let Some(pending) = self.next_claim(session) else {
                    continue 'sessions;
                };
                self.wanted.claim(pending.id, pending.generation, session);
                if let Some(generation) = probe {
                    self.wanted.take_probe(session, generation);
                }
                self.next_call = self.next_call.saturating_add(1);
                let call = self.next_call;
                self.sessions
                    .get_mut(&session)
                    .expect("listed above")
                    .reserving
                    .insert(
                        pending.id,
                        Reserving {
                            pending,
                            probe,
                            call,
                        },
                    );
                let call = Call {
                    session,
                    id: pending.id,
                    call,
                    generation: self.generation,
                };
                if self.auto_reserves {
                    self.process(call);
                    self.drain();
                } else {
                    self.calls.push_back(call);
                }
            }
        }
    }

    /// The pool processes one reserve call. With automatic reserves its
    /// answer is received at once.
    fn process(&mut self, call: Call) {
        // A command already sent cannot be taken back, so a call its caller
        // gave up still runs, and its answer goes nowhere. A slot it was
        // granted is given back when that answer arrives.
        let answer = self.pool.reserve(call.id, Intent::Fetch, self.now);
        let step = self.pool.effects();
        self.inbox.push_back(step);
        let answer = Answer {
            call,
            answer,
            cancelled: false,
        };
        if self.auto_reserves {
            self.deliver(answer);
        } else {
            self.answers.push_back(answer);
        }
    }

    /// The caller receives the pool's answer to its reserve call.
    fn deliver(&mut self, answer: Answer) {
        let Answer {
            call,
            answer,
            cancelled,
        } = answer;
        let session = call.session;
        let id = call.id;
        let Some(reserving) = self
            .sessions
            .get_mut(&session)
            .filter(|state| {
                state
                    .reserving
                    .get(&id)
                    .is_some_and(|reserving| reserving.call == call.call)
            })
            .and_then(|state| state.reserving.shift_remove(&id))
        else {
            // The caller gave up the call or ended. A granted slot goes
            // straight back.
            if let Ok(reservation) = answer {
                let step = self.pool.release(reservation.id, self.now);
                self.inbox.push_back(step);
            }
            return;
        };
        let Reserving { pending, probe, .. } = reserving;
        let peer = self.sessions.get(&session).expect("checked above").peer;
        // A rejection holds only for the chain it was made on. The caller
        // knows the generation it called at and the one it holds now, and
        // a same-height reorg changes the generation.
        let chain_unchanged = self.generation == call.generation;
        let still_claimed = self
            .wanted
            .current(id, pending.generation)
            .is_some_and(|entry| entry.claim == Some(session));
        match answer {
            Ok(reservation) if cancelled => {
                // A cancelled token says nothing about why, so it decides
                // nothing: the slot goes back and the id is asked about
                // again, and the current pool answers.
                let step = self.pool.release(reservation.id, self.now);
                self.inbox.push_back(step);
                self.wanted
                    .release(id, pending.generation, session, self.now, false);
                self.return_probe(probe);
            }
            Ok(reservation) => {
                assert!(
                    !reservation.joined && reservation.wants_body,
                    "a fetch of an undecided id is refused, never joined"
                );
                self.wanted.room();
                let cooling = self.reactor.is_cooling(peer, self.now);
                if still_claimed && self.pool_live && !cooling {
                    let state = self.sessions.get_mut(&session).expect("checked above");
                    state.send(pending, reservation.id, self.now);
                    self.reservations.insert(reservation.id, (session, id));
                    self.effects.push(RelayEffect::Ask { session, id });
                } else {
                    // The peer started cooling, the pool turned off, or the
                    // claim is gone: nothing is asked for.
                    let step = self.pool.release(reservation.id, self.now);
                    self.inbox.push_back(step);
                    self.wanted
                        .release(id, pending.generation, session, self.now, cooling);
                }
                self.return_probe(probe);
            }
            Err(ModelUnreserved::NoRoom) => {
                self.wanted
                    .release(id, pending.generation, session, self.now, false);
                self.wanted.no_room(self.now, probe);
            }
            Err(ModelUnreserved::Pending) => {
                self.wanted
                    .release(id, pending.generation, session, self.now, true);
                self.return_probe(probe);
            }
            Err(ModelUnreserved::Pooled) => {
                // It settles the id only if the caller's current snapshot
                // still holds it: the pool may have restarted since.
                // The pool answers `Pooled` by mined id, so the check is by
                // mined id too: another authorizing-data variant settles it.
                let pooled = self
                    .visible
                    .iter()
                    .any(|visible| visible.mined_id() == id.mined_id());
                self.wanted
                    .release(id, pending.generation, session, self.now, !pooled);
                if pooled {
                    self.decided(id, pending.generation);
                }
                self.return_probe(probe);
            }
            Err(ModelUnreserved::Rejected(_)) => {
                self.wanted
                    .release(id, pending.generation, session, self.now, !chain_unchanged);
                if chain_unchanged {
                    self.decided(id, pending.generation);
                }
                self.return_probe(probe);
            }
            Err(ModelUnreserved::Inactive) => {
                self.wanted
                    .release(id, pending.generation, session, self.now, false);
                self.return_probe(probe);
            }
        }
    }

    fn return_probe(&mut self, probe: Option<u64>) {
        if let Some(generation) = probe {
            self.wanted.return_probe(generation);
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
        let asking = state.requests.len().saturating_add(state.reserving.len());
        if in_flight >= MAX_REQUESTS_PER_PEER || asking >= hello.serves {
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
            if state.requests.contains_key(&pending.id) || state.reserving.contains_key(&pending.id)
            {
                return false;
            }
            // Its own wait after its own release. Another session's release
            // never restarts it.
            if state
                .waits_until(pending.id, pending.generation)
                .is_some_and(|until| self.now < until)
            {
                return false;
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
        self.reactor.assert_invariants();
        assert!(self.inbox.is_empty(), "every pool step is routed");
        let pool_visible: HashSet<UnminedTxId> = self.pool.visible().into_iter().collect();
        let mirror: HashSet<UnminedTxId> = self.visible.iter().copied().collect();
        assert_eq!(mirror, pool_visible, "the log's view is the pool's");
        let pool_hidden: HashSet<UnminedTxId> = self.pool.hidden().into_iter().collect();
        assert_eq!(
            self.hidden, pool_hidden,
            "the log's hidden set is the pool's"
        );

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
                        && state.reserving.is_empty()
                        && state.submissions.is_empty()
                        && state.pending.is_empty(),
                    "only the session with the go fetches or holds table entries"
                );
                assert!(state.waiting.len() <= PENDING_PER_PEER);
            }
            if let Some(hello) = state.hello {
                assert!(
                    state.requests.len() + state.reserving.len() <= hello.serves,
                    "within the peer's Hello"
                );
            } else {
                assert!(
                    state.requests.is_empty() && state.reserving.is_empty(),
                    "no request before the Hello"
                );
            }
            assert!(state.pending.len() <= PENDING_PER_PEER);
            assert!(state.deferred.len() <= DEFERRED_PER_PEER);
            assert!(state.known.len() <= KNOWN_CAP);
            for id in &state.reannounced {
                assert!(
                    state.requests.contains_key(id),
                    "a re-announcement is noted only beside an outstanding request"
                );
            }
            for key in state.timed_out.keys() {
                assert!(
                    state.holds(key.0, key.1),
                    "timeout history only for pending ids"
                );
            }
            pending_total = pending_total.saturating_add(state.pending.len());
            for pending in &state.pending {
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
                        .unwrap_or_else(|| {
                            panic!(
                                "a live request's entry is current: {id:?} {request:?} entry {:?} pending {:?} visible {} hidden {} live {}",
                                self.wanted.entry(*id),
                                state.pending,
                                self.visible.contains(id),
                                self.hidden.contains(id),
                                self.pool_live,
                            )
                        });
                    assert_eq!(
                        entry.claim,
                        Some(*session),
                        "a live request holds the claim"
                    );
                }
            }
            for (held, _) in &state.held {
                assert!(
                    !state.announced.contains_key(held),
                    "a held advert was not sent yet"
                );
            }
            if state.cursor == self.last_seq && go {
                for id in state.announced.keys() {
                    assert!(
                        self.visible.contains(id) || self.hidden.contains(id),
                        "a session that read the whole log announced only pooled ids"
                    );
                }
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
        // Every submission has exactly one owner and one recorded
        // generation, and every owner's submission has a waiter.
        assert_eq!(self.waiters.len(), self.submitted_at.len());
        let mut owned = 0usize;
        for (waiter, owner) in &self.waiters {
            assert!(
                self.submitted_at.contains_key(waiter),
                "a waiter has its generation"
            );
            match owner {
                VerdictOwner::Session(session) => assert!(
                    self.sessions
                        .get(session)
                        .is_some_and(|state| state.submissions.contains_key(waiter)),
                    "a session's waiter is in its submissions"
                ),
                VerdictOwner::Reactor => assert!(
                    self.reactor.holds_unsettled(*waiter),
                    "the reactor's waiter is unsettled"
                ),
            }
            owned += 1;
        }
        let held: usize = self
            .sessions
            .values()
            .map(|state| state.submissions.len())
            .sum::<usize>()
            + self.reactor.unsettled_total();
        assert_eq!(owned, held, "every submission has a waiter");
        if let Some((holder, generation)) = self.wanted.probe() {
            assert!(
                self.sessions.get(&holder).is_some_and(|state| state
                    .reserving
                    .values()
                    .any(|reserving| reserving.probe == Some(generation))),
                "the probe permit belongs to a reserve call in flight"
            );
        }

        for (id, entry) in self.wanted.entries() {
            let counted = holders.remove(&(*id, entry.generation)).unwrap_or(0);
            assert_eq!(entry.holders, counted, "holder count of {id:?}");
            assert!(entry.holders > 0, "an entry nobody holds is gone");
            if let Some(session) = entry.claim {
                let state = self
                    .sessions
                    .get(&session)
                    .expect("a claim belongs to a live session");
                assert!(
                    state.holds(*id, entry.generation),
                    "the claiming session holds the id pending"
                );
                let fetching = state.reserving.contains_key(id)
                    || state
                        .requests
                        .get(id)
                        .is_some_and(|request| request.reservation.is_some());
                let submitted = state
                    .submissions
                    .values()
                    .any(|(held, generation)| held == id && *generation == entry.generation);
                assert!(
                    fetching || submitted,
                    "a claim covers a reserve call, a fetch or a submission"
                );
            }
        }
        assert!(holders.is_empty(), "every pending reference has its entry");
        assert!(
            self.wanted.len() <= pending_total,
            "the table is bounded by the pending sets"
        );
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

/// A short id from the id's bytes, with the authorizing data folded in, so
/// ids collide only when a test asks them to.
fn default_short_id(id: UnminedTxId) -> u64 {
    let mut bytes = id.mined_id().0;
    if let UnminedTxId::Witnessed(wtx) = id {
        for (byte, auth) in bytes.iter_mut().zip(wtx.auth_digest.0) {
            *byte ^= auth.rotate_left(3);
        }
    }
    let mut short = [0u8; 8];
    short.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(short)
}
