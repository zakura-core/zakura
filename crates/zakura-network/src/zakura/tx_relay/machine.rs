//! The pure `tx-relay` machine.
//!
//! [`RelayMachine`] has three owners:
//!
//! - [`peer::PeerState`], one per session: everything about one peer. Its
//!   pending ids with the size it announced, its outstanding requests and
//!   their deadlines, its submissions, its limit of
//!   [`MAX_REQUESTS_PER_PEER`], and what it was announced.
//! - [`wanted::WantedIds`], one per node: the ids this node wants, each with
//!   its claim, holder count and timing. No peer facts.
//! - [`reactor::Reactor`], one per node: cooldowns, which session of a peer
//!   may fetch, the submissions of ended sessions, which peers tried each
//!   wanted entry, and the ranking.
//!
//! It does no IO and calls nothing. The IO hands it peer messages, the
//! pool's answers and the clock as inputs, and carries out its
//! [`RelayEffect`]s.
//!
//! The pool is behind effects. A reserve call goes out as
//! [`RelayEffect::Reserve`] and its answer comes back through
//! [`RelayMachine::reserve_answer`]. A body goes out as
//! [`RelayEffect::Submit`] and its verdict comes back through
//! [`RelayMachine::verdict`]. A reservation given back goes out as
//! [`RelayEffect::Release`], which the IO carries out by dropping the
//! [`Reservation`](zakura_node_services::mempool::port::Reservation), or as
//! [`RelayEffect::FetchFailed`]. The pool's change log and snapshot come in
//! through [`RelayMachine::pool_changed`], and a fetch the pool cancelled
//! through [`RelayMachine::fetch_cancelled`].
//!
//! Reserve answers, verdicts and pool changes are separate inputs, in
//! whatever order the IO receives them. A body that passes its check goes to
//! the pool in the same step, so no checked body waits in the machine. Every
//! session reads each batch of changes as it arrives, so only the latest
//! batch is kept. Each input applies whatever is due first. Time is a
//! [`Duration`] since a base the IO holds.
//!
//! The rules:
//!
//! - **Claim, reserve, then ask.** A session with a free slot claims an
//!   eligible id in its pending set, reserves a slot at the pool, and only
//!   then sends `GetTxs`. One claim per id. The id ready longest goes
//!   first, over every session: ready since its first announcement, the
//!   end of its retry delay, or the end of its last claim that left it
//!   undecided, whichever is last. Every such end counts, whether a session
//!   gave up or the pool answered, except a `NoRoom` answer, after which the
//!   backoff waits rather than the id. So an id whose attempts keep ending
//!   undecided, whoever holds it, queues behind work that was already
//!   waiting. A slot given back is room, so it ends a `NoRoom` backoff. So
//!   neither fresh ids nor retries starve the other, and an older session
//!   cannot keep taking the room a verdict frees. A grant whose caller gave
//!   up goes straight back.
//! - **A session is one connection's `tx-relay` streams.** A connection
//!   carries at most one at a time, so closing a session's connection ends
//!   that session only. A session can end and be reopened on the same
//!   connection, and a closed peer can redial at once, so a charge closes
//!   only live sessions and leaves the rest to the cooldown on the peer id.
//! - **A stopped core ends claiming for good.** The port has no way to
//!   attach another core. A stopped core publishes nothing more, so the
//!   last snapshot still shows an active pool and no fetch token fires. The
//!   machine learns of the stop from a `Stopped` answer or verdict, or from
//!   [`RelayMachine::pool_stopped`], and from then on claims nothing. Its
//!   outstanding requests end by their deadlines or bodies, and nothing goes
//!   back to the core. An `Inactive` answer while the snapshot shows an
//!   active pool waits a retry delay, so a missed restart costs one delay.
//! - **One attempt at a time.** A claim lasts from the reserve call until
//!   the body's verdict, even if the session ends meanwhile, so no other
//!   fetch of the same entry starts while the pool decides it. A verdict
//!   acts only on the entry its body was fetched for and releases only its
//!   own claim.
//! - **The limit of 5 is counted once.** Ids reserving, requested or
//!   awaiting a verdict on a session, plus the reserve calls and unsettled
//!   submissions its peer's ended sessions left, never exceed
//!   [`MAX_REQUESTS_PER_PEER`]. A new session gets the go only after every
//!   older session of its peer ended.
//! - **One try per peer.** A peer that tried an id isn't asked for it again
//!   while another holder hasn't tried, so delaying an honest holder costs
//!   an attacker one peer per try. A peer has tried an entry when it
//!   answered `NotFound`, reached the deadline without an answer, ended a
//!   session with the claim out, or supplied a body the pool rejected
//!   without settling the id. What this node caused is no try: a `NoRoom`
//!   answer, a stopped core, a local fault, a cancelled fetch, a reserve
//!   call given up, or an admission a pool restart erased. The reactor
//!   keeps the tries for the entry's life, by peer, so announcing the id
//!   again or reconnecting gives no second try, and every session of the
//!   peer drops its reference. Past [`TRIED_PER_ENTRY`] peers at an entry
//!   it forgets the oldest one with no session left, and none while every
//!   one is connected. So a peer can try an entry again only after it
//!   disconnected and that many others tried it, and its new session is
//!   younger than every holder that waited, which goes first. Each try
//!   costs a peer that is connected, or a reconnection. An entry's tries
//!   are at most one more than the larger of the cap and the most peers
//!   connected at once, which bounds their memory. The entry ends
//!   once no holder is left, that is once every holder has tried, and a
//!   later announcement starts a fresh one in which every peer may try
//!   again. Only a session with the go and a `Hello` holds ids, so every
//!   holder can be asked: one without a `Hello` keeps its announcements
//!   waiting, as one without the go does. There are no turns and no waits
//!   beyond the id's own retry delay: a holder that has not tried claims
//!   the id at its next free slot, after its own older ids. So two holders
//!   can still keep an entry going for a while: one that cools keeps its
//!   references for up to [`BASE_COOLDOWN`], and one with a deep backlog of
//!   older ids reaches the id only after them. A late body is dropped
//!   before it reaches the pool, and a timeout never doubles a deadline,
//!   since that peer is not asked for the entry again.
//! - **A peer that leaves a request unanswered loses the session.** From
//!   its deadline on, a request keeps its peer's slot and its answer is
//!   dropped unread. Freeing only the relay's slot would leak the
//!   transport's reservation, which `regulation::reservations` keeps, from
//!   a node-wide pool, until the peer answers or the session ends, so
//!   ending the session is the only thing that frees it, as block sync
//!   disconnects a peer that stops reading. If one more deadline passes
//!   without an answer, the session ends as unresponsive, so a peer that
//!   never answers cannot hold its slots for ever. That time is fixed when
//!   the request misses its deadline, and later requests never move it.
//!   The deadline runs whether or not the pool still holds the
//!   reservation: a fetch the pool cancelled is no try, but left unanswered
//!   it still ends the session. Arrival is when the machine handles the
//!   message, not when its frame reached the reader or the transport, so a
//!   body handled just after its deadline is late, which costs one fetch
//!   from another announcer and no charge. A session therefore never has
//!   two requests for one id on the wire.
//! - **A deadline covers what the peer has outstanding, up to a cap.** Each
//!   new request moves every outstanding deadline out to cover all the
//!   peer's outstanding bodies from then, at the slowest supported rate,
//!   but never past its own request's sent time plus [`peer::max_span`], so
//!   a peer that keeps adding requests cannot hold an id forever. The cost:
//!   a slow peer that sends later bodies first can time out on an earlier
//!   one, which is its try at that id, and the id is asked of another
//!   announcer, one duplicate fetch.
//! - **A rejection of a scope the pool caches settles the id for every
//!   holder,** if it arrives while the caller's snapshot shows the pool on,
//!   at the same tip the caller's snapshot showed when it made the reserve
//!   call or submitted the body, for a verdict and a reserve answer alike.
//!   The port's rejection carries no tip hash, so this is the check a
//!   caller can make: a tip change at any point between the call and the
//!   answer, even one before the pool decided, only makes the id be asked
//!   about again. The snapshot is the only chain source: while the pool is
//!   off it keeps its last tip, so a rejection arriving then is asked about
//!   again too. That includes one from the pool's contents, such as a
//!   conflict, which the pool also keeps until the tip changes. The port
//!   shows no restart or cache clearing a caller can rely on seeing, so
//!   neither is checked: as with today's rejection cache, an announcement
//!   held when the rejection arrives is dropped, and the id is asked about
//!   again only when a peer announces it again. The current pool then
//!   answers. An [`RejectionScope::Uncached`] rejection, or one from before
//!   a tip change, lets another announcer try after a retry delay. Since
//!   the claim is held until the verdict, a rejection never ends another
//!   announcer's fetch.
//! - **A body is checked against the table before it is submitted.** If its
//!   entry is gone or no longer its session's claim, the body is dropped
//!   and its pool slot goes back. A body that passes goes to the pool under
//!   its reservation whatever happens next: a decision, a cancelled fetch,
//!   or the session ending. The pool answers it, which frees the slot.
//! - **Overflow is counted and is never a violation.** An announcement
//!   that finds its session's pending set full is dropped.
//! - **The skip set is bounded.** A session never tells its peer about an
//!   id the peer still has pending or in flight, on this session or in a
//!   body the reactor holds for it, and the supplier's sessions mark an id
//!   known again when its verdict arrives, as the newest entry. Past that,
//!   only a peer that announces [`KNOWN_CAP`] or more other ids between its
//!   body's verdict and reading the admission, or that has no session when
//!   the verdict arrives and reconnects after it, can be told of its own
//!   body, which costs that peer one advert and nobody else anything.
//! - **An advert stands until the session answers `NotFound` for it or
//!   reads the id's removal,** and a re-admission after either is announced
//!   afresh. A session that falls off the change log cannot see removals,
//!   since a snapshot cannot show whether an id left and came back, so it
//!   keeps its adverts for visible ids and its held deadlines, and
//!   announces what it finds with the usual window and delay. An advert's
//!   delay counts from the first admission the session saw: one held
//!   across a removal and re-admission it has not read yet can fire on the
//!   earlier schedule. That moves one advert earlier, never later.
//! - **A peer's own `NotFound` withdraws what it told this node.** Unless a
//!   newer announcement contradicts it, its skip-set entry for the id goes,
//!   so a later admission is announced to it.
//! - **A decision drops the id from every pending set at once.**
//! - **The table has no cap of its own.** An entry lives while a pending set
//!   holds it or a claim covers it, so each peer fills only its own share,
//!   plus its at most [`MAX_REQUESTS_PER_PEER`] unsettled submissions.
//! - **A full pool costs one probe per backoff.** After a `NoRoom` nobody
//!   claims until the backoff ends or a verdict frees a slot.
//! - **A verdict's charge goes to the peer that supplied the body**, even if
//!   its session ended. `Cooldown` follows today's `PeerCooldowns`, and a
//!   second cooldown closes the peer's live sessions. `Ban` cools the peer
//!   for [`MAX_COOLDOWN`] and closes its live sessions. A cooldown is kept
//!   by peer id, so it holds across a session end and a reconnect. The
//!   table keeps at most [`MAX_COOLDOWN_PEERS`] peers and never drops a
//!   cooldown to make room, so fresh identities can't shed one by filling
//!   it: a peer not in the full table that earns a cooldown or a ban is
//!   closed and not cooled.
//! - **Announce what the pool admits, once, and never back to its source.**
//!   A session announces an added id the pool holds, unless it already did
//!   or its peer announced or supplied it. Sessions inside the id's window
//!   announce at once, the rest after [`FanOut::advert_delay`].
//! - **Serve only what was announced.** A `GetTxs` gets the body only for an
//!   exact id this session announced and the pool still holds.
//!
//! Left out, because the IO owns them: the byte rate on `Inv`, the places a
//! peer's requests to this node take and the close for one past them, and
//! frames.

pub(crate) mod peer;
pub(crate) mod reactor;
pub(crate) mod wanted;

#[cfg(test)]
mod budget_tests;
#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use indexmap::IndexSet;
use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTx, UnminedTxId},
};
use zakura_node_services::mempool::port::{
    ChainTip, Change, Charge, LocalFault, RejectionScope, ReservationId,
};
use zakura_rt::collections::{HashMap, HashSet};

use self::{
    peer::{Hello, PeerState, Pending, ReadyKey, Request, Reserving},
    reactor::{Reactor, Recorded},
    wanted::{Admission, Entry, WantedIds},
};

/// Time since a base the IO holds.
pub(crate) type Time = Duration;

/// Ids requested or awaiting a verdict for one peer, at most.
pub(crate) const MAX_REQUESTS_PER_PEER: usize = 5;

/// Ids one session may hold pending: a whole pool at the default size.
pub(crate) const PENDING_PER_PEER: usize = 8_000;

/// How many peers the reactor remembers as having tried one wanted entry
/// before it forgets any. Past that it forgets the oldest peer with no
/// session left, so a peer can try an entry again only after it
/// disconnected and this many others tried it since (*provisional*).
pub(crate) const TRIED_PER_ENTRY: usize = 64;

/// Ids one session remembers its peer announcing or supplying.
pub(crate) const KNOWN_CAP: usize = 2 * 8_000 + PENDING_PER_PEER;

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

/// How often the reactor ranks the sessions again.
pub(crate) const RANK_INTERVAL: Duration = Duration::from_secs(30);

/// Every this many places in the ranking, a random session takes the next
/// one, as block relay does.
pub(crate) const RANDOM_EVERY: usize = 4;

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

/// Names one reserve call, so its answer finds it.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CallId(pub(crate) u64);

/// Names one submission, so its verdict finds it.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct WaiterId(pub(crate) u64);

/// How many sessions hear of an admission at once, and when the rest do.
/// The default is the plan's: [`ADVERT_DELAY`], and windows from
/// [`WINDOW_SMALL`] to [`WINDOW_FULL`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct FanOut {
    /// How long sessions outside an id's window wait to announce it. Zero
    /// announces to every session at once.
    pub(crate) advert_delay: Duration,
    /// The window for transactions of [`SMALL_TX_BYTES`] or less. At least
    /// `window_full`.
    pub(crate) window_small: usize,
    /// The window for transactions of [`MAX_TX_BYTES`] and of unknown size.
    /// At least 1.
    pub(crate) window_full: usize,
}

impl Default for FanOut {
    fn default() -> Self {
        Self {
            advert_delay: ADVERT_DELAY,
            window_small: WINDOW_SMALL,
            window_full: WINDOW_FULL,
        }
    }
}

impl FanOut {
    /// Whether a session at `rank` announces an id of `size` bytes as soon
    /// as it learns of it: inside the window, or with no delay at all. A
    /// zero delay never holds an advert, since a timer due at once would be
    /// dropped before it fired.
    pub(crate) fn at_once(&self, rank: Option<usize>, size: Option<u32>) -> bool {
        self.advert_delay.is_zero() || rank.is_some_and(|rank| rank < self.window(size))
    }

    /// The fan-out window for a transaction of `size` bytes. It falls in a
    /// straight line from `window_small` at [`SMALL_TX_BYTES`] to
    /// `window_full` at [`MAX_TX_BYTES`]. Unknown sizes get `window_full`, as
    /// block relay treats a block of unknown size.
    pub(crate) fn window(&self, size: Option<u32>) -> usize {
        let Some(size) = size else {
            return self.window_full;
        };
        let size = u64::from(size.clamp(SMALL_TX_BYTES, MAX_TX_BYTES));
        let below_full = u64::from(MAX_TX_BYTES).saturating_sub(size);
        let range = u64::from(MAX_TX_BYTES - SMALL_TX_BYTES);
        let spread = self.window_small.saturating_sub(self.window_full);
        let extra = u64::try_from(spread)
            .unwrap_or(u64::MAX)
            .saturating_mul(below_full)
            / range;
        self.window_full
            .saturating_add(usize::try_from(extra).unwrap_or(spread))
    }
}

/// How the machine is set up.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelayConfig {
    /// The fan-out settings.
    pub(crate) fan_out: FanOut,
    /// The key of the hash that gives each transaction its short id in the
    /// skip sets, so a peer can't pick ids that collide.
    pub(crate) short_id_key: u64,
}

/// What the pool's snapshot says alongside a batch of changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotInfo {
    /// Whether the pool is on.
    pub(crate) active: bool,
    /// The tip the pool is valid for.
    pub(crate) tip: ChainTip,
    /// The serialized size of each transaction the batch added, as the
    /// snapshot holds it. A visible id with no size gets the smallest window
    /// and is never served.
    pub(crate) sizes: Vec<(UnminedTxId, u32)>,
}

/// A granted reservation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Granted {
    /// The reservation.
    pub(crate) reservation: ReservationId,
    /// Its fetch token had already fired when the IO received it.
    pub(crate) cancelled: bool,
}

/// Why the pool granted nothing:
/// [`Unreserved`](zakura_node_services::mempool::port::Unreserved) without
/// the error detail.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Unreserved {
    /// The pool is off.
    Inactive,
    /// The transaction is in the pool.
    Pooled,
    /// The id is undecided: another caller is bringing its body in.
    Pending,
    /// A cached rejection covers the id.
    Rejected {
        /// How long the rejection is remembered.
        scope: RejectionScope,
    },
    /// The node's limit is reached.
    NoRoom,
    /// The core has stopped.
    Stopped,
}

/// The pool's one answer to a submission:
/// [`Verdict`](zakura_node_services::mempool::port::Verdict) without the
/// error detail.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Verdict {
    /// The transaction is in the pool.
    Admitted,
    /// The core refused the transaction.
    Rejected {
        /// How long the rejection is remembered.
        scope: RejectionScope,
        /// What the body earned its supplier.
        charge: Charge,
        /// The height of the tip the pool held when it decided.
        tip: Height,
    },
    /// The node's limit was reached.
    NoRoom,
    /// A failure on this node.
    LocalFault(LocalFault),
}

/// Why the node closes a peer's connection.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum CloseReason {
    /// The peer broke a wire rule.
    Violation(Violation),
    /// A body it supplied earned a ban.
    Ban,
    /// A body it supplied earned a second cooldown.
    RepeatedCooldown,
    /// A body it supplied earned a cooldown, and the full cooldown table had
    /// no room for the peer.
    CooldownsFull,
    /// It left a request unanswered for one more deadline after missing it.
    Unresponsive,
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

/// What `tx-relay` does, on the wire and at the pool.
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
    /// Reserve a slot at the pool to fetch `id`, and answer through
    /// [`RelayMachine::reserve_answer`] with `call`.
    Reserve {
        /// The call.
        call: CallId,
        /// The id.
        id: UnminedTxId,
    },
    /// Submit `body` under `reservation`, and answer its verdict through
    /// [`RelayMachine::verdict`] with `waiter`.
    Submit {
        /// The submission.
        waiter: WaiterId,
        /// The reservation it is submitted under.
        reservation: ReservationId,
        /// The body.
        body: UnminedTx,
    },
    /// Drop the reservation, unsubmitted.
    Release {
        /// The reservation.
        reservation: ReservationId,
    },
    /// Tell the pool the fetch under `reservation` ended without a body.
    FetchFailed {
        /// The reservation.
        reservation: ReservationId,
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
    /// The session's peer, which a call of an ended session counts against.
    peer: PeerKey,
    id: UnminedTxId,
    /// Names this call, so an old call's answer never acts on a newer call
    /// for the same id.
    call: CallId,
    /// The pool's tip in the snapshot the caller held when it made the call.
    tip: ChainTip,
}

/// Who submitted a body, from which snapshot.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Submitted {
    /// The session whose claim the submission holds until its verdict,
    /// even after the session ends.
    session: SessionId,
    /// The pool's tip in its snapshot at the check before submitting.
    tip: ChainTip,
    /// Whether its supplier is exempt from charges, as its session was.
    exempt: bool,
}

/// `tx-relay`'s state, without the pool.
#[derive(Debug)]
pub(crate) struct RelayMachine {
    now: Time,
    /// The tip the pool is valid for, as its snapshot shows it. Late answers
    /// are confirmed against it.
    pool_tip: ChainTip,
    /// The node's best tip, which can run ahead of the pool's. No cooldown
    /// applies until the IO sets it.
    best_tip: Height,
    /// Whether the node is close to the network's tip.
    current: bool,
    pool_live: bool,
    wanted: WantedIds,
    reactor: Reactor,
    sessions: BTreeMap<SessionId, PeerState>,
    /// The sessions that may hold each wanted id pending, so a change to
    /// its entry re-files it only where it is held. It may name a session
    /// that no longer holds the id, which [`Self::refile`] drops.
    holders: HashMap<UnminedTxId, BTreeSet<SessionId>>,
    next_session: u64,
    /// Which session holds each of `tx-relay`'s reservations, and for which
    /// id.
    reservations: HashMap<ReservationId, (SessionId, UnminedTxId)>,
    /// Who waits for each of `tx-relay`'s submissions, and who submitted
    /// it from which snapshot.
    waiters: HashMap<WaiterId, VerdictOwner>,
    submitted_at: HashMap<WaiterId, Submitted>,
    /// Names the next submission.
    next_waiter: u64,
    /// `tx-relay` was told the core stopped, so it claims nothing more and
    /// a slot given back is no room.
    stopped: bool,
    /// The parameters of the keyed short-id hash.
    short_id: blake2b_simd::Params,
    /// How many sessions hear of an admission at once, and when the rest do.
    fan_out: FanOut,
    /// The latest batch of changes, with their log positions. Every session
    /// with the go reads each batch as it arrives, so an older one is never
    /// read again.
    log: Vec<Logged>,
    /// No held advert is due before this, so a step with nothing due skips
    /// looking at every session's held adverts. It may be early, after a
    /// held advert leaves another way, which costs one look.
    held_due: Option<Time>,
    /// What comes due with no input, apart from the wanted table's own
    /// times and the backoff. See [`Timers`].
    timers: Timers,
    last_seq: u64,
    /// The pool, as the changes say, oldest first.
    visible: IndexSet<UnminedTxId>,
    /// How many visible ids share each mined id, so a `Pooled` answer is
    /// checked with one lookup.
    visible_mined: HashMap<transaction::Hash, usize>,
    /// The serialized size of each visible transaction, as the snapshot
    /// says.
    sizes: HashMap<UnminedTxId, u32>,
    /// Reserve calls the pool has not answered.
    calls: HashMap<CallId, Call>,
    /// Names the next reserve call.
    next_call: u64,
    /// What `tx-relay` did, in order, until the IO takes it.
    effects: Vec<RelayEffect>,
}

impl RelayMachine {
    /// `tx-relay` on the pool `snapshot` shows, whose visible transactions
    /// are `visible` with their sizes, oldest first.
    pub(crate) fn new(
        config: RelayConfig,
        snapshot: SnapshotInfo,
        visible: Vec<(UnminedTxId, u32)>,
    ) -> Self {
        let mut short_id = blake2b_simd::Params::new();
        short_id
            .hash_length(8)
            .key(&config.short_id_key.to_le_bytes())
            .personal(b"Zakura_TxShortId");
        Self {
            now: Duration::ZERO,
            pool_tip: snapshot.tip,
            best_tip: Height(0),
            current: true,
            pool_live: snapshot.active,
            wanted: WantedIds::default(),
            reactor: Reactor::default(),
            sessions: BTreeMap::new(),
            holders: HashMap::default(),
            next_session: 0,
            reservations: HashMap::default(),
            waiters: HashMap::default(),
            submitted_at: HashMap::default(),
            next_waiter: 0,
            stopped: false,
            short_id,
            fan_out: config.fan_out,
            log: Vec::new(),
            held_due: None,
            timers: Timers::default(),
            last_seq: 0,
            visible: visible.iter().map(|(id, _)| *id).collect(),
            visible_mined: mined_counts(visible.iter().map(|(id, _)| *id)),
            sizes: visible.into_iter().collect(),
            calls: HashMap::default(),
            next_call: 0,
            effects: Vec::new(),
        }
    }

    // Inputs from peers.

    /// A session of `peer` opened. If `exempt`, the bodies it supplies are
    /// never charged, even when their verdict comes after it ends. It gets
    /// the go once every older session of the peer ended.
    pub(crate) fn connect(&mut self, peer: PeerKey, exempt: bool, now: Time) -> SessionId {
        self.enter(now);
        self.next_session = self.next_session.saturating_add(1);
        let session = SessionId(self.next_session);
        self.sessions.insert(session, PeerState::new(peer, exempt));
        // A cooldown counts toward the next deadline only while its peer has
        // a session, so a new session brings it back.
        if let Some(until) = self.reactor.cooling_until(peer, self.now) {
            self.timers.push(until, Timer::Cooling(peer));
        }
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
            // Announcements it made before can join the table now, if it
            // has the go.
            let peer = state.peer;
            if self.reactor.has_go(peer, session) {
                let waiting: Vec<(UnminedTxId, u32)> =
                    std::mem::take(&mut state.waiting).into_iter().collect();
                self.take_ids(session, &waiting);
            }
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
            // A cooling peer's ids are not taken, but it still has them, so
            // none is announced back to it.
            let seq = self.last_seq;
            let shorts: Vec<u64> = entries.iter().map(|(id, _)| self.short(*id)).collect();
            let state = self.sessions.get_mut(&session).expect("checked above");
            for short in shorts {
                state.mark_known(short, seq);
            }
            return self.settle();
        }
        let hello = self
            .sessions
            .get(&session)
            .is_some_and(|state| state.hello.is_some());
        if self.reactor.has_go(peer, session) && hello {
            self.take_ids(session, entries);
        } else {
            // A session waiting for the go keeps its announcements to itself,
            // filtered and bounded as the pending set is, so one peer never
            // holds two shares of the table. So does one without a `Hello`,
            // which could never be asked, so it cannot keep an entry alive.
            let seq = self.last_seq;
            let shorts: Vec<u64> = entries.iter().map(|(id, _)| self.short(*id)).collect();
            let state = self.sessions.get_mut(&session).expect("checked above");
            for ((id, size), short) in entries.iter().copied().zip(shorts) {
                state.mark_known(short, seq);
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

    /// A `Tx` arrived answering the request for `id`, carrying `body`. Its
    /// size on the wire is the body's own size.
    pub(crate) fn tx(&mut self, session: SessionId, id: UnminedTxId, body: UnminedTx, now: Time) {
        self.enter(now);
        self.receive(session, id, body);
        self.settle();
    }

    /// A `Tx` for `id` with a body of `size` bytes arrived, not yet decoded.
    /// Returns whether to decode the body and hand it to [`Self::tx`]. An
    /// answer to nothing is a violation, and a late body or one from a peer
    /// that started cooling is dropped unread, all as [`Self::tx`] does,
    /// so the IO never decodes a body the machine would not read.
    pub(crate) fn tx_arrived(
        &mut self,
        session: SessionId,
        id: UnminedTxId,
        size: u32,
        now: Time,
    ) -> bool {
        self.enter(now);
        let read = self.check_arrival(session, id, size);
        self.settle();
        read
    }

    /// The checks on a body that need only its id and size, in [`Self::tx`]'s
    /// order. Returns whether the body is still wanted. Otherwise the answer
    /// is settled: a violation, or dropped unread.
    fn check_arrival(&mut self, session: SessionId, id: UnminedTxId, size: u32) -> bool {
        let Some(state) = self.sessions.get_mut(&session) else {
            return false;
        };
        let Some(request) = state.requests.get(&id).copied() else {
            self.violation(session, Violation::Unrequested);
            return false;
        };
        let peer = state.peer;
        let Some(reservation) = request.reservation else {
            // Late: the node stopped waiting. The body is dropped before it
            // is checked, with no penalty.
            state.requests.shift_remove(&id);
            state.reannounced.remove(&id);
            return false;
        };
        if self.reactor.is_cooling(peer, self.now) {
            // The peer started cooling while the body was in flight. Dropped
            // unread, as the legacy adapter drops it.
            state.requests.shift_remove(&id);
            state.reannounced.remove(&id);
            self.reservations.remove(&reservation);
            self.give_back(reservation);
            self.wanted
                .release(id, request.generation, session, self.now, true);
            return false;
        }
        if size != request.size {
            self.violation(session, Violation::WrongSize);
            return false;
        }
        true
    }

    /// `session` checks the body of `id` and, if it passes, submits it.
    fn receive(&mut self, session: SessionId, id: UnminedTxId, body: UnminedTx) {
        let seq = self.last_seq;
        let size = u32::try_from(body.size()).unwrap_or(u32::MAX);
        let short = self.short(id);
        if !self.check_arrival(session, id, size) {
            return;
        }
        if body.id() != id {
            return self.violation(session, Violation::WrongId);
        }
        let state = self
            .sessions
            .get_mut(&session)
            .expect("check_arrival passed, so the session exists");
        let request = state
            .requests
            .shift_remove(&id)
            .expect("check_arrival passed, so the request is outstanding");
        let reservation = request
            .reservation
            .expect("check_arrival passed, so the request holds a reservation");
        state.reannounced.remove(&id);
        state.mark_known(short, seq);
        // The check before submitting passed: a request keeps its
        // reservation only while it holds the current claim. From here on
        // the body goes to the pool whatever is decided.
        self.submit(session, id, request.generation, reservation, body);
    }

    /// A `NotFound` arrived.
    pub(crate) fn not_found(&mut self, session: SessionId, id: UnminedTxId, now: Time) {
        self.enter(now);
        let short = self.short(id);
        let Some(state) = self.sessions.get_mut(&session) else {
            return self.settle();
        };
        let Some(request) = state.requests.shift_remove(&id) else {
            self.violation(session, Violation::Unrequested);
            self.settle();
            return;
        };
        // An old answer the peer has since contradicted by announcing the id
        // again leaves what it told this node alone.
        let contradicted = state.reannounced.remove(&id);
        if contradicted.is_none() {
            // The peer says it does not have it, so it is no longer one to
            // skip when the id is admitted.
            state.forget_known(short);
        }
        // Either way the peer had its try at this entry.
        let peer = state.peer;
        self.tried(peer, id, request.generation);
        if let Some(reservation) = request.reservation {
            // The fetch ended without a body, which the pool logs.
            self.reservations.remove(&reservation);
            self.effects.push(RelayEffect::FetchFailed { reservation });
            if !self.stopped {
                self.wanted.room();
            }
            self.wanted
                .release(id, request.generation, session, self.now, false);
        }
        match contradicted {
            // The announcement that overtook this answer is taken now, as
            // it would have been had it come after. One try per entry still
            // holds: if the entry lives on, this peer has tried it.
            Some(size) => self.take_ids(session, &[(id, size)]),
            // If the pool already holds the id, an admission this session
            // skipped while the request was out is announced now.
            None => self.reconsider_advert(session, id, request.admission_seen),
        }
        self.settle();
    }

    /// A `GetTxs` arrived.
    pub(crate) fn get_txs(&mut self, session: SessionId, id: UnminedTxId, now: Time) {
        self.enter(now);
        let size = self.sizes.get(&id).copied();
        let Some(state) = self.sessions.get_mut(&session) else {
            return self.settle();
        };
        let Some(hello) = state.hello else {
            self.violation(session, Violation::NoHelloFirst);
            self.settle();
            return;
        };
        // Exact ids: this compares the authorizing data too. A body above the
        // requester's limit is never sent.
        let servable = state.announced.is_sent(&id)
            && self.visible.contains(&id)
            && size.is_some_and(|size| size <= hello.max_tx_bytes);
        if !servable && state.announced.is_sent(&id) {
            // The answer withdraws the advert the peer asked from, so a later
            // admission is announced again. A queued one, of a later
            // admission, has not reached the peer yet and stays.
            state.announced.remove(&id);
        }
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

    // Inputs from the node and the pool.

    /// Time passed.
    pub(crate) fn tick(&mut self, now: Time) {
        self.enter(now);
        self.settle();
    }

    /// The pool answered the reserve call `call`.
    pub(crate) fn reserve_answer(
        &mut self,
        call: CallId,
        answer: Result<Granted, Unreserved>,
        now: Time,
    ) {
        self.enter(now);
        match self.calls.remove(&call) {
            Some(call) => {
                if !self.sessions.contains_key(&call.session) {
                    self.reactor.call_answered(call.peer);
                }
                self.deliver(call, answer)
            }
            // Not a call this machine made. A grant still goes back, and a
            // `Stopped` answer still says the core stopped.
            None => match answer {
                Ok(granted) => self.give_back(granted.reservation),
                Err(Unreserved::Stopped) => self.stopped = true,
                Err(_) => {}
            },
        }
        self.settle();
    }

    /// The pool's verdict on the submission `waiter`.
    pub(crate) fn verdict(&mut self, waiter: WaiterId, verdict: Verdict, now: Time) {
        self.enter(now);
        self.apply_verdict(waiter, verdict);
        self.settle();
    }

    /// The pool cancelled the fetch under `reservation`: its token fired.
    pub(crate) fn fetch_cancelled(&mut self, reservation: ReservationId, now: Time) {
        self.enter(now);
        if let Some((session, id)) = self.reservations.get(&reservation).copied() {
            self.cancel_fetch(session, id, reservation);
        }
        self.settle();
    }

    /// The pool published `changes`, one batch, and `snapshot` is the pool
    /// after it. A snapshot that turns the pool on again reopens every
    /// session with the go first, as a restart does. Every session with the
    /// go reads the batch at once.
    pub(crate) fn pool_changed(&mut self, changes: Vec<Change>, snapshot: SnapshotInfo, now: Time) {
        self.enter(now);
        let activated = !self.pool_live && snapshot.active;
        self.pool_live = snapshot.active;
        self.pool_tip = snapshot.tip;
        if activated {
            let opened: Vec<SessionId> = self
                .sessions
                .iter()
                .filter(|(session, state)| self.reactor.has_go(state.peer, **session))
                .map(|(session, _)| *session)
                .collect();
            for session in opened {
                self.open(session);
            }
        }
        self.publish(changes, snapshot.sizes);
        self.settle();
    }

    /// The IO lost its place in the pool's change log, so it hands over the
    /// whole pool, oldest first, with `snapshot`. Every session with the go
    /// rebuilds from it, as one that falls off the log does: an advert it
    /// sent still stands for an id the pool holds, and what it finds is
    /// announced with the usual window and delay. A snapshot that turns the
    /// pool on opens them instead, with the whole pool.
    pub(crate) fn pool_resynced(
        &mut self,
        visible: Vec<(UnminedTxId, u32)>,
        snapshot: SnapshotInfo,
        now: Time,
    ) {
        self.enter(now);
        let activated = !self.pool_live && snapshot.active;
        self.pool_live = snapshot.active;
        self.pool_tip = snapshot.tip;
        self.visible = visible.iter().map(|(id, _)| *id).collect();
        self.visible_mined = mined_counts(self.visible.iter().copied());
        self.sizes = visible.into_iter().collect();
        self.sizes.extend(snapshot.sizes);
        self.log.clear();
        let sessions: Vec<SessionId> = self.sessions.keys().copied().collect();
        for session in sessions {
            // A snapshot that turns the pool on, such as the first one the
            // actor reads, opens each session with the go, as
            // `pool_changed` does, so every one announces the whole pool
            // whatever its rank.
            if activated {
                let has_go = self
                    .sessions
                    .get(&session)
                    .is_some_and(|state| self.reactor.has_go(state.peer, session));
                if has_go {
                    self.open(session);
                }
            } else {
                self.rebuild(session);
            }
        }
        self.settle();
    }

    /// The core behind the port stopped for good, which the IO saw some
    /// other way than through an answer or a verdict. `tx-relay` claims
    /// nothing more.
    pub(crate) fn pool_stopped(&mut self, now: Time) {
        self.enter(now);
        self.stopped = true;
        self.settle();
    }

    /// Whether the node is close to the network's tip.
    pub(crate) fn set_current(&mut self, current: bool) {
        self.current = current;
    }

    /// The node's best tip moved, perhaps ahead of the pool's.
    pub(crate) fn set_best_tip(&mut self, tip: Height) {
        self.best_tip = tip;
    }

    /// `session`'s routine reports its connection's smoothed round-trip
    /// time.
    // Unused until the IO reports round-trip times, which a later change wires in.
    #[allow(dead_code)]
    pub(crate) fn observe_rtt(&mut self, session: SessionId, rtt: Duration, now: Time) {
        self.enter(now);
        // A report that arrives after its session ended is ignored.
        if let Some(peer) = self.sessions.get(&session).map(|state| state.peer) {
            self.reactor.observe_rtt(peer, session, rtt);
        }
        self.settle();
    }

    // Outputs and observations.

    /// Takes up to `max` of `session`'s queued announcements that the pool
    /// shows now, oldest first, with their sizes, for one `Inv` frame. They
    /// count as sent: the frame goes on the data stream ahead of any body
    /// it lets the peer request.
    pub(crate) fn take_inv(&mut self, session: SessionId, max: usize) -> Vec<(UnminedTxId, u32)> {
        let Some(state) = self.sessions.get_mut(&session) else {
            return Vec::new();
        };
        let (visible, sizes) = (&self.visible, &self.sizes);
        state.announced.take(max, |id| {
            visible
                .contains(id)
                .then(|| sizes.get(id).copied())
                .flatten()
        })
    }

    /// How many timers the machine keeps, standing or not.
    #[cfg(test)]
    pub(crate) fn timers_len(&self) -> usize {
        self.timers.set.len()
    }

    /// How many sessions the holders index names for `id`.
    #[cfg(test)]
    pub(crate) fn holders_of(&self, id: UnminedTxId) -> usize {
        self.holders.get(&id).map_or(0, BTreeSet::len)
    }

    /// Every live session.
    #[cfg(test)]
    pub(crate) fn session_ids(&self) -> impl Iterator<Item = SessionId> + '_ {
        self.sessions.keys().copied()
    }

    /// How many of `session`'s announcements wait for an `Inv`.
    pub(crate) fn queued_inv(&self, session: SessionId) -> usize {
        self.sessions
            .get(&session)
            .map_or(0, |state| state.announced.queued())
    }

    /// What `tx-relay` did since the last call, in order.
    pub(crate) fn take_effects(&mut self) -> Vec<RelayEffect> {
        std::mem::take(&mut self.effects)
    }

    /// A session's state.
    #[cfg(test)]
    pub(crate) fn session(&self, session: SessionId) -> Option<&PeerState> {
        self.sessions.get(&session)
    }

    /// The wanted table.
    #[cfg(test)]
    pub(crate) fn wanted(&self) -> &WantedIds {
        &self.wanted
    }

    /// The pool, as the changes say, oldest first.
    #[cfg(test)]
    pub(crate) fn visible(&self) -> &IndexSet<UnminedTxId> {
        &self.visible
    }

    /// Reserve calls the pool has not answered.
    #[cfg(test)]
    pub(crate) fn calls_in_flight(&self) -> usize {
        self.calls.len()
    }

    /// `tx-relay`'s reservations at the pool, outside submissions.
    #[cfg(test)]
    pub(crate) fn reservations_held(&self) -> usize {
        self.reservations.len()
    }

    /// Checks what holds between any two inputs. Each check states one rule
    /// of the module docs, or that an index kept for speed agrees with the
    /// state it indexes, worked out again the slow way.
    ///
    /// # Panics
    ///
    /// If one is broken, naming it.
    #[cfg(test)]
    pub(crate) fn assert_invariants(&self) {
        let (now, reactor, wanted) = (self.now, &self.reactor, &self.wanted);

        // The reactor's sessions, the go and the ranking.
        let mut by_peer: BTreeMap<PeerKey, BTreeSet<SessionId>> = BTreeMap::new();
        for (session, state) in &self.sessions {
            by_peer.entry(state.peer).or_default().insert(*session);
        }
        assert_eq!(reactor.by_peer(), &by_peer, "relay/reactor-sessions");
        assert_eq!(reactor.go().len(), by_peer.len(), "relay/go-per-peer");
        for (peer, sessions) in &by_peer {
            let oldest = reactor.go().get(peer) == sessions.first();
            assert!(oldest, "relay/go-is-the-oldest-session");
        }
        let with_go: BTreeSet<SessionId> = reactor.go().values().copied().collect();
        let ranked: BTreeSet<SessionId> = reactor.ranked().collect();
        assert_eq!(ranked, with_go, "relay/ranking-places-sessions-with-go");
        let cooldowns = reactor.cooldowns_len();
        assert!(cooldowns <= MAX_COOLDOWN_PEERS, "relay/cooldown-bound");

        // Each session: what it may hold, its bounds and its requests.
        let mut in_flight: BTreeMap<PeerKey, usize> = BTreeMap::new();
        let mut reservations = HashMap::default();
        let mut references: HashMap<(UnminedTxId, EntryGen), usize> = HashMap::default();
        let mut holders: HashMap<UnminedTxId, BTreeSet<SessionId>> = HashMap::default();
        for (session, state) in &self.sessions {
            let go = with_go.contains(session);
            let fetches = go && state.hello.is_some();
            let idle = state.pending.len() == 0 && state.in_flight() == 0;
            assert!(fetches || idle, "relay/only-go-and-hello-hold-or-fetch");
            let waits = !state.waiting.is_empty();
            assert!(!(fetches && waits), "relay/go-and-hello-take-waiting");
            let told = state.announced.iter().next().is_some() || state.held.first_due().is_some();
            assert!(go || !told, "relay/only-go-is-announced-to");
            let read = state.cursor == self.last_seq;
            assert!(read || !go, "relay/go-reads-every-batch");
            let held = state.pending.len().max(state.waiting.len());
            assert!(held <= PENDING_PER_PEER, "relay/pending-bound");
            assert!(state.known.len() <= KNOWN_CAP, "relay/known-bound");
            let asking = state.requests.len() + state.reserving.len();
            let served = state.hello.is_none_or(|hello| asking <= hello.serves);
            assert!(served, "relay/asks-within-hello-serves");
            *in_flight.entry(state.peer).or_default() += state.in_flight();
            let asked = |id: &UnminedTxId| state.requests.contains_key(id);
            let twice = state.reserving.keys().any(asked);
            assert!(!twice, "relay/one-request-per-id-on-the-wire");
            let beside = state.reannounced.keys().all(asked);
            assert!(beside, "relay/reannounced-beside-its-request");
            for (id, request) in &state.requests {
                let cap = request.sent.saturating_add(peer::max_span());
                assert!(request.deadline <= cap, "relay/deadline-cap");
                // Each input applies what is due at its time first, and sets
                // only later times.
                let unspent = match request.gives_up_at {
                    None => request.deadline > now,
                    Some(at) => request.reservation.is_none() && at > now,
                };
                assert!(unspent, "relay/no-resources-past-a-deadline");
                if let Some(reservation) = request.reservation {
                    reservations.insert(reservation, (*session, *id));
                }
            }
            // The claim queue, worked out again from the entries.
            let mut claimable: HashMap<UnminedTxId, ReadyKey> = HashMap::default();
            for pending in &state.pending {
                let entry = wanted.current(pending.id, pending.generation);
                let entry = entry.expect("relay/pending-names-the-current-entry");
                let key = (pending.id, entry.generation);
                *references.entry(key).or_default() += 1;
                let once = holders.entry(pending.id).or_default().insert(*session);
                assert!(once, "relay/one-reference-per-id");
                let tried = reactor.has_tried(state.peer, pending.id, pending.generation);
                assert!(!tried, "relay/one-try-per-peer");
                if entry.claim.is_none() {
                    claimable.insert(pending.id, ready_key(entry));
                }
            }
            let filed = state.pending.filed();
            let lined = state.pending.ready().count() == filed.len();
            let indexed = lined && filed.len() == claimable.len();
            assert!(indexed, "relay/claim-queue-index");
            for (id, key, at) in filed {
                let placed = claimable.get(&id) == Some(&key) && at.is_some_and(|at| at.id == id);
                assert!(placed, "relay/claim-queue-index");
            }
        }
        let ended = reactor.ended_calls().keys();
        let unsettled = reactor.unsettled().keys().chain(ended);
        let peers: BTreeSet<PeerKey> = in_flight.keys().chain(unsettled).copied().collect();
        for peer in peers {
            let live = in_flight.get(&peer).copied().unwrap_or(0);
            let count = live + reactor.unsettled_count(peer);
            assert!(count <= MAX_REQUESTS_PER_PEER, "relay/limit-counted-once");
        }
        assert_eq!(self.reservations, reservations, "relay/reservations-index");
        assert_eq!(self.holders, holders, "relay/holders-index");

        // The table keeps nothing without a holder or a claim.
        for (id, entry) in wanted.entries() {
            let counted = references.get(&(*id, entry.generation)).copied();
            assert_eq!(entry.holders, counted.unwrap_or(0), "relay/holder-count");
            let alive = entry.holders > 0 || entry.claim.is_some();
            assert!(alive, "relay/entry-held-or-claimed");
            let woken = entry.retry_at <= now || wanted.retry_indexed(*id, entry.retry_at);
            assert!(woken, "relay/retry-index");
        }
        for ((id, generation), peers) in reactor.tries() {
            let current = wanted.current(*id, *generation).is_some();
            assert!(current && !peers.is_empty(), "relay/tries-last-the-entry");
        }

        // One attempt at a time: each attempt holds its entry's claim, and
        // each claim covers one attempt. A body awaiting its verdict has one
        // waiter, owned by its live session or by the reactor.
        let mut attempts: HashMap<(UnminedTxId, EntryGen), SessionId> = HashMap::default();
        let mut attempt = |id: UnminedTxId, generation: EntryGen, session: SessionId| {
            let other = attempts.insert((id, generation), session);
            assert!(other.is_none(), "relay/one-attempt-at-a-time");
            let claim = wanted.current(id, generation).and_then(|entry| entry.claim);
            assert_eq!(claim, Some(session), "relay/attempt-holds-its-claim");
        };
        let mut waiting = 0;
        for (session, state) in &self.sessions {
            let reserving = state.reserving.iter();
            let reserving = reserving.map(|(id, held)| (*id, held.pending.generation));
            let requests = state.requests.iter();
            let requests = requests.filter(|(_, request)| request.reservation.is_some());
            let requests = requests.map(|(id, request)| (*id, request.generation));
            let submissions = state.submissions.iter().map(|(_, held)| *held);
            for (id, generation) in reserving.chain(requests).chain(submissions) {
                attempt(id, generation, *session);
                let pending = state.pending.holds(id, generation);
                assert!(pending, "relay/fetched-id-stays-pending");
            }
            for waiter in state.submissions.keys() {
                let owner = self.waiters.get(waiter) == Some(&VerdictOwner::Session(*session));
                let submitter = self.submitted_at[waiter].session == *session;
                assert!(owner && submitter, "relay/waiter-of-a-live-session");
                waiting += 1;
            }
        }
        for unsettled in reactor.unsettled().values() {
            assert!(!unsettled.is_empty(), "relay/no-empty-unsettled-record");
            for (waiter, (id, generation)) in unsettled {
                let submitted = self.submitted_at.get(waiter).expect("relay/submitter-kept");
                let ended = !self.sessions.contains_key(&submitted.session);
                let owner = self.waiters.get(waiter) == Some(&VerdictOwner::Reactor);
                assert!(ended && owner, "relay/handed-to-the-reactor-on-end");
                attempt(*id, *generation, submitted.session);
                waiting += 1;
            }
        }
        for (id, entry) in wanted.entries() {
            if let Some(session) = entry.claim {
                let attempt = attempts.get(&(*id, entry.generation));
                assert_eq!(attempt, Some(&session), "relay/claim-covers-an-attempt");
            }
        }
        assert_eq!(self.waiters.len(), waiting, "relay/one-waiter-per-body");
        assert_eq!(self.submitted_at.len(), waiting, "relay/submitter-kept");

        // Reserve calls: each is a live session's reserve call, or counts
        // against the peer of the session that ended.
        let mut ended_calls: HashMap<PeerKey, usize> = HashMap::default();
        for (name, call) in &self.calls {
            match self.sessions.get(&call.session) {
                Some(state) => {
                    let held = state.reserving.get(&call.id);
                    let own = held.is_some_and(|held| held.call == *name);
                    assert!(own, "relay/call-is-its-sessions");
                }
                None => *ended_calls.entry(call.peer).or_default() += 1,
            }
        }
        assert_eq!(reactor.ended_calls(), &ended_calls, "relay/ended-calls");
        let called = |held: &Reserving| self.calls.contains_key(&held.call);
        for state in self.sessions.values() {
            assert!(state.reserving.values().all(called), "relay/call-in-flight");
        }
        if let Some((session, generation)) = wanted.probe() {
            let state = self.sessions.get(&session);
            let mut calls = state.into_iter().flat_map(|state| state.reserving.values());
            let probing = calls.any(|held| held.probe == Some(generation));
            assert!(probing, "relay/probe-is-a-call-in-flight");
        }

        // Timers: one entry per source with one time at once, and one for
        // every time something comes due.
        for (timer, key) in &self.timers.latest {
            let indexed = self.timers.set.get(key) == Some(timer);
            let held = matches!(timer, Timer::Held(_));
            assert!(indexed && !held, "relay/timer-index");
        }
        let mut held_timers: HashSet<(Time, SessionId)> = HashSet::default();
        for ((at, _), timer) in &self.timers.set {
            match timer {
                Timer::Held(session) => {
                    held_timers.insert((*at, *session));
                }
                _ => assert!(self.timers.latest.contains_key(timer), "relay/timer-index"),
            }
        }
        let due = |timer: Timer| self.timers.latest.get(&timer).map(|(at, _)| *at);
        for (session, state) in &self.sessions {
            for (id, request) in &state.requests {
                let (timer, at) = match request.gives_up_at {
                    None => (Timer::Deadline(*session, *id), request.deadline),
                    Some(at) => (Timer::GiveUp(*session, *id), at),
                };
                assert_eq!(due(timer), Some(at), "relay/request-timer");
            }
            for (_, at) in state.held.iter() {
                let timed = held_timers.contains(&(at, *session));
                let early = self.held_due.is_some_and(|due| due <= at);
                assert!(at > now && timed && early, "relay/held-advert-timer");
            }
        }
        for peer in by_peer.keys() {
            if let Some(until) = reactor.cooling_until(*peer, now) {
                let timed = due(Timer::Cooling(*peer)) == Some(until);
                assert!(timed, "relay/cooldown-timer");
            }
        }

        // The pool as the changes say, and what each session is told of it.
        let mined = mined_counts(self.visible.iter().copied());
        assert_eq!(self.visible_mined, mined, "relay/mined-id-counts");
        let sized = self.sizes.keys().all(|id| self.visible.contains(id));
        assert!(sized, "relay/sizes-of-visible-ids");
        for state in self.sessions.values() {
            // What the peer has pending or in flight, here or in a body the
            // reactor holds for it.
            let busy = |id: &UnminedTxId| {
                state.pending.contains(*id)
                    || state.waiting.contains_key(id)
                    || state.reserving.contains_key(id)
                    || state.requests.contains_key(id)
                    || state.submissions.contains(*id)
                    || reactor.holds_for(state.peer, *id)
            };
            for (id, _) in state.announced.iter() {
                assert!(self.visible.contains(id), "relay/advert-of-a-pooled-id");
                assert!(!busy(id), "relay/no-advert-of-an-id-in-flight");
            }
            for (id, _) in state.held.iter() {
                let pooled = self.visible.contains(&id) && !busy(&id);
                assert!(pooled, "relay/no-advert-of-an-id-in-flight");
                let announced = state.announced.contains_key(&id);
                assert!(!announced, "relay/held-advert-not-yet-announced");
            }
        }
    }

    /// The earliest time after now that something becomes due with no other
    /// input: a timed id, a deadline, an advert, the end of the backoff, or a
    /// cooldown.
    pub(crate) fn next_deadline(&self) -> Option<Time> {
        let now = self.now;
        // Each source keeps its times in order and skips the ones that no
        // longer stand, so the first left is its earliest.
        let timer = self
            .timers
            .iter()
            .find(|(at, timer)| *at > now && self.timer_stands(*at, *timer))
            .map(|(at, _)| at);
        [
            self.wanted.backoff_until(now),
            self.wanted.next_wake(now),
            timer,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Whether `timer` still comes due at `at`: its source still holds that
    /// time. A timer whose source moved or ended stays in [`Timers`] until
    /// [`Self::drop_stale_timers`] or [`Self::expire`] reaches it.
    fn timer_stands(&self, at: Time, timer: Timer) -> bool {
        match timer {
            Timer::Deadline(session, id) => self
                .request(session, id)
                .is_some_and(|request| request.gives_up_at.is_none() && request.deadline == at),
            Timer::GiveUp(session, id) => self
                .request(session, id)
                .is_some_and(|request| request.gives_up_at == Some(at)),
            Timer::Held(session) => self
                .sessions
                .get(&session)
                .is_some_and(|state| state.held.has_due(at)),
            // A cooldown counts only while its peer has a session.
            Timer::Cooling(peer) => {
                self.reactor.has_sessions(peer)
                    && self.reactor.cooling_until(peer, self.now) == Some(at)
            }
        }
    }

    /// `session`'s request for `id`, if it has one.
    fn request(&self, session: SessionId, id: UnminedTxId) -> Option<&Request> {
        self.sessions.get(&session)?.requests.get(&id)
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
        self.wanted.drop_stale(self.now);
    }

    // The step loop.

    /// Moves time to `now` and applies what is due before the input. Time
    /// never goes back: an older `now` counts as the current time.
    fn enter(&mut self, now: Time) {
        self.now = self.now.max(now);
        let now = self.now;
        if self.reactor.refresh_due(now) {
            self.reactor.refresh(now);
        }
        self.expire();
        self.release_adverts();
    }

    /// Lets every session claim what it can, then forgets the tries of
    /// entries that ended.
    fn settle(&mut self) {
        self.fill();
        self.drop_stale_timers();
        // An entry that ended takes its tries with it, so a later
        // announcement starts afresh.
        for (id, generation) in self.wanted.take_ended() {
            self.reactor.forget_tries(id, generation);
        }
    }

    /// The short id the skip sets keep for `id`: a keyed hash of its exact
    /// bytes, so two ids collide only by chance.
    fn short(&self, id: UnminedTxId) -> u64 {
        let mut state = self.short_id.to_state();
        state.update(&id.mined_id().0);
        if let Some(auth) = id.auth_digest() {
            state.update(&auth.0);
        }
        let mut short = [0u8; 8];
        short.copy_from_slice(&state.finalize().as_bytes()[..8]);
        u64::from_le_bytes(short)
    }

    /// Gives `reservation` back to the pool unsubmitted. A slot given back
    /// is room, unless the core stopped.
    fn give_back(&mut self, reservation: ReservationId) {
        self.effects.push(RelayEffect::Release { reservation });
        if !self.stopped {
            self.wanted.room();
        }
    }

    /// Adds `id` to the visible pool.
    fn show(&mut self, id: UnminedTxId) {
        if self.visible.insert(id) {
            *self.visible_mined.entry(id.mined_id()).or_default() += 1;
        }
    }

    /// Takes `id` out of the visible pool.
    fn unshow(&mut self, id: UnminedTxId) {
        if self.visible.shift_remove(&id) {
            if let Some(count) = self.visible_mined.get_mut(&id.mined_id()) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.visible_mined.remove(&id.mined_id());
                }
            }
        }
    }

    /// Applies a published batch to the mirror and the log, then every
    /// session reads it.
    fn publish(&mut self, changes: Vec<Change>, sizes: Vec<(UnminedTxId, u32)>) {
        // Every session with the go read the last batch, and one that gets
        // the go later starts from the snapshot.
        self.log.clear();
        // What each read needs of a change but every session shares: the
        // id's short id and whether this is the id's last change in the
        // batch, worked out once here rather than once per session.
        let mut last: HashMap<UnminedTxId, usize> = HashMap::default();
        for change in changes {
            match change {
                Change::Added(id) => {
                    self.show(id);
                }
                Change::Removed { id, .. } => {
                    self.unshow(id);
                    self.sizes.remove(&id);
                }
                // Another caller's attempt at it ended undecided, so a retry
                // that waited for that caller need not wait any longer.
                Change::Refused(id) => self.wanted.other_ended(id, self.now),
            }
            self.last_seq = self.last_seq.saturating_add(1);
            let id = change_id(&change);
            if let Some(earlier) = last.insert(id, self.log.len()) {
                self.log[earlier].last = false;
            }
            let short = match change {
                Change::Added(_) | Change::Removed { .. } => self.short(id),
                Change::Refused(_) => 0,
            };
            self.log.push(Logged {
                seq: self.last_seq,
                change,
                short,
                last: true,
            });
        }
        for (id, size) in sizes {
            if self.visible.contains(&id) {
                self.sizes.insert(id, size);
            }
        }
        let sessions: Vec<SessionId> = self.sessions.keys().copied().collect();
        for session in sessions {
            self.read_log(session);
        }
    }

    /// One session with the go reads the latest batch from its cursor. The
    /// snapshot it reads with is the pool after the batch.
    fn read_log(&mut self, session: SessionId) {
        let Some(state) = self.sessions.get(&session) else {
            return;
        };
        if !self.reactor.has_go(state.peer, session) {
            return;
        }
        if state.cursor >= self.last_seq {
            return;
        }
        let rank = self.reactor.rank(session);
        let log = std::mem::take(&mut self.log);
        for logged in &log {
            let state = self.sessions.get(&session).expect("checked above");
            let (seq, short) = (&logged.seq, logged.short);
            if *seq <= state.cursor {
                continue;
            }
            match logged.change {
                Change::Added(id) => {
                    let size = self.sizes.get(&id).copied();
                    let state = self.sessions.get_mut(&session).expect("checked above");
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
                        || !self.pool_live
                        || !self.visible.contains(&id)
                        || state.announced.contains_key(&id)
                        || (state.skips(id, short) || self.reactor.holds_for(state.peer, id))
                        || state.held.contains(id)
                    {
                        continue;
                    }
                    if self.fan_out.at_once(rank, size) {
                        state.announced.insert(id, *seq);
                        self.effects.push(RelayEffect::Advert { session, id });
                    } else {
                        let due = self.now.saturating_add(self.fan_out.advert_delay);
                        // One timer stands for every advert of the session due then.
                        if state.held.push(id, due, short) {
                            self.timers.push(due, Timer::Held(session));
                        }
                        self.held_due = Some(self.held_due.map_or(due, |held| held.min(due)));
                    }
                }
                Change::Removed { id, .. } => {
                    let state = self.sessions.get_mut(&session).expect("checked above");
                    state.forget(id, short, *seq);
                }
                Change::Refused(_) => {}
            }
        }
        self.log = log;
        let last_seq = self.last_seq;
        if let Some(state) = self.sessions.get_mut(&session) {
            state.cursor = last_seq;
        }
    }

    /// A session with the go that lost its place in the log rebuilds from
    /// the snapshot. An advert it sent still stands for an id the pool
    /// holds, and a held one keeps its deadline. What it finds follows the
    /// same fan-out as a read: at once inside the id's window, after the
    /// advert delay outside it. The skip set is not pruned.
    fn rebuild(&mut self, session: SessionId) {
        let Some(state) = self.sessions.get(&session) else {
            return;
        };
        if !self.reactor.has_go(state.peer, session) {
            return;
        }
        let rank = self.reactor.rank(session);
        let peer = state.peer;
        let shorts: Vec<u64> = self.visible.iter().map(|id| self.short(*id)).collect();
        let state = self.sessions.get_mut(&session).expect("checked above");
        let visible = &self.visible;
        state.announced.retain(|id, _| visible.contains(id));
        state.held.retain(|id| visible.contains(id));
        if self.pool_live {
            for (id, short) in self.visible.iter().copied().zip(shorts) {
                if let Some(request) = state.requests.get_mut(&id) {
                    request.admission_seen.get_or_insert(self.now);
                }
                if state.announced.contains_key(&id)
                    || state.held.contains(id)
                    || state.skips(id, short)
                    || self.reactor.holds_for(peer, id)
                {
                    continue;
                }
                if self.fan_out.at_once(rank, self.sizes.get(&id).copied()) {
                    state.announced.insert(id, self.last_seq);
                    self.effects.push(RelayEffect::Advert { session, id });
                } else {
                    let due = self.now.saturating_add(self.fan_out.advert_delay);
                    // One timer stands for every advert of the session due then.
                    if state.held.push(id, due, short) {
                        self.timers.push(due, Timer::Held(session));
                    }
                    self.held_due = Some(self.held_due.map_or(due, |held| held.min(due)));
                }
            }
        }
        state.cursor = self.last_seq;
    }

    /// The verdict on `waiter` reaches its owner. The owner may have changed
    /// since the submission, when a session ended, so it is looked up here.
    fn apply_verdict(&mut self, waiter: WaiterId, verdict: Verdict) {
        let Some(owner) = self.waiters.remove(&waiter) else {
            return;
        };
        let (peer, id, generation) = match owner {
            VerdictOwner::Session(session) => {
                let state = self
                    .sessions
                    .get_mut(&session)
                    .expect("a session hands its submissions over when it ends");
                let (id, generation) = state
                    .submissions
                    .remove(&waiter)
                    .expect("a waiter's owner holds it");
                (state.peer, id, generation)
            }
            VerdictOwner::Reactor => {
                let (peer, id, generation) = self
                    .reactor
                    .settle(waiter)
                    .expect("a waiter's owner holds it");
                (peer, id, generation)
            }
        };
        let submitted = self
            .submitted_at
            .remove(&waiter)
            .expect("every submission records its submitter");
        // The supplier's sessions learn of the body again as the verdict
        // arrives, so however many ids its peer announced since, the skip set
        // still holds this one when the admission is read.
        let short = self.short(id);
        let seq = self.last_seq;
        for state in self
            .sessions
            .values_mut()
            .filter(|state| state.peer == peer)
        {
            state.mark_known(short, seq);
        }
        // The submission held the claim since its check, so no other fetch
        // of this entry started meanwhile. A verdict acts only on the entry
        // its body was fetched for, and releases only its own claim.
        let release = |relay: &mut Self, retry: bool| {
            relay
                .wanted
                .release(id, generation, submitted.session, relay.now, retry);
        };
        // A body the pool rejected without settling the id was its
        // supplier's try. A full pool, a stopped core, a local fault or an
        // admission a restart erased is this node's doing, not the peer's.
        let tried_then_release = |relay: &mut Self, retry: bool| {
            relay.tried(peer, id, generation);
            release(relay, retry);
        };
        // A verdict frees a pool slot.
        self.wanted.room();
        match verdict {
            Verdict::Admitted => {
                // It settles the id only if the current snapshot holds it.
                if self.visible.contains(&id) {
                    self.decided(id, generation);
                } else {
                    release(self, false);
                }
            }
            Verdict::Rejected { scope, charge, tip } => {
                // A rejection of a scope the pool caches, on the chain it was
                // made on, settles the id when it arrives. See the module
                // docs for why a restart or a cleared cache is not checked.
                let remembered = !matches!(scope, RejectionScope::Uncached)
                    && self.pool_live
                    && self.pool_tip == submitted.tip;
                if remembered {
                    self.decided(id, generation);
                } else {
                    tried_then_release(self, true);
                }
                // The charge was earned against the tip the pool decided at,
                // which a verdict kept for a later reservation carries. An
                // unconditional peer is never charged, as the legacy adapter
                // never charges it.
                if !submitted.exempt {
                    self.charge(peer, charge, tip);
                }
            }
            Verdict::LocalFault(LocalFault::Stopped) => {
                self.stopped = true;
                release(self, false);
            }
            Verdict::NoRoom => release(self, true),
            Verdict::LocalFault(_) => release(self, true),
        }
    }

    /// `session`'s body of `id`, fetched for `generation` under
    /// `reservation`, goes to the pool.
    fn submit(
        &mut self,
        session: SessionId,
        id: UnminedTxId,
        generation: EntryGen,
        reservation: ReservationId,
        body: UnminedTx,
    ) {
        self.reservations.remove(&reservation);
        if self.stopped {
            // The port would answer at once that the core stopped.
            self.effects.push(RelayEffect::Release { reservation });
            self.wanted
                .release(id, generation, session, self.now, false);
            return;
        }
        self.next_waiter = self.next_waiter.saturating_add(1);
        let waiter = WaiterId(self.next_waiter);
        let state = self
            .sessions
            .get_mut(&session)
            .expect("only a live session receives a body");
        state.submissions.insert(waiter, (id, generation));
        let exempt = state.exempt;
        self.waiters.insert(waiter, VerdictOwner::Session(session));
        self.submitted_at.insert(
            waiter,
            Submitted {
                session,
                tip: self.pool_tip,
                exempt,
            },
        );
        self.effects.push(RelayEffect::Submit {
            waiter,
            reservation,
            body,
        });
    }

    /// The pool no longer needs the body `session` was fetching.
    fn cancel_fetch(&mut self, session: SessionId, id: UnminedTxId, reservation: ReservationId) {
        self.reservations.remove(&reservation);
        self.give_back(reservation);
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
                if let Some(reservation) = state
                    .requests
                    .get_mut(&id)
                    .filter(|request| request.generation == generation)
                    .and_then(|request| request.reservation.take())
                {
                    self.reservations.remove(&reservation);
                    // The same as `give_back`, which can't borrow `self` here.
                    self.effects.push(RelayEffect::Release { reservation });
                    if !self.stopped {
                        self.wanted.room();
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
                match self.reactor.record_invalid(peer, self.now) {
                    Recorded::AlreadyCooling => {}
                    Recorded::Started(length) => {
                        self.timers
                            .push(self.now.saturating_add(length), Timer::Cooling(peer));
                        if length > BASE_COOLDOWN {
                            self.close_peer(peer, CloseReason::RepeatedCooldown);
                        }
                    }
                    Recorded::TableFull => self.close_peer(peer, CloseReason::CooldownsFull),
                }
            }
            Charge::Ban => {
                // Every session of the peer closes, so its cooldown needs no
                // timer until one connects.
                self.reactor.ban(peer, self.now);
                self.close_peer(peer, CloseReason::Ban);
            }
        }
    }

    /// Closes every live session of `peer`.
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
        // Ending with a claim out is the peer's try at that entry, so neither
        // a replacement session nor a later announcement asks it again.
        let attempts: Vec<(UnminedTxId, EntryGen)> = state
            .reserving
            .iter()
            .map(|(id, reserving)| (*id, reserving.pending.generation))
            .chain(
                state
                    .requests
                    .iter()
                    .filter(|(_, request)| request.reservation.is_some())
                    .map(|(id, request)| (*id, request.generation)),
            )
            .collect();
        for (id, generation) in attempts {
            self.tried(state.peer, id, generation);
        }
        // Its reserve calls still in flight are answered to nobody, and their
        // grants go back as they arrive. Until then they count against the
        // peer's 5.
        self.reactor.hand_calls(state.peer, state.reserving.len());
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
                self.give_back(reservation);
                self.wanted
                    .release(*id, request.generation, session, self.now, false);
            }
        }
        // A submission keeps its claim until its verdict, so nobody fetches
        // the id again while the pool still decides it.
        for pending in &state.pending {
            self.wanted.unhold(pending.id, pending.generation);
            forget_holder(&mut self.holders, pending.id, session);
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
        let Some(state) = self.sessions.get(&session) else {
            return;
        };
        let peer = state.peer;
        let seq = self.last_seq;
        let unsettled: Vec<u64> = self
            .reactor
            .unsettled_ids(peer)
            .into_iter()
            .map(|id| self.short(id))
            .collect();
        let shorts: Vec<u64> = if self.pool_live {
            self.visible.iter().map(|id| self.short(*id)).collect()
        } else {
            Vec::new()
        };
        let state = self.sessions.get_mut(&session).expect("checked above");
        for short in unsettled {
            state.mark_known(short, seq);
        }
        // A session that had the go before, such as one the pool's restart
        // reopens, may not have read everything. It starts again from the
        // snapshot, as a resync does.
        let visible = &self.visible;
        state.announced.retain(|id, _| visible.contains(id));
        state.held.retain(|id| visible.contains(id));
        state.cursor = seq;
        if self.pool_live {
            for (id, short) in self.visible.iter().copied().zip(shorts) {
                if !state.announced.contains_key(&id)
                    && !(state.skips(id, short) || self.reactor.holds_for(state.peer, id))
                {
                    state.announced.insert(id, seq);
                    state.held.remove(id);
                    self.effects.push(RelayEffect::Advert { session, id });
                }
            }
        }
        // Without a `Hello` it could never be asked, so its announcements
        // wait for one.
        if state.hello.is_some() {
            let waiting: Vec<(UnminedTxId, u32)> =
                std::mem::take(&mut state.waiting).into_iter().collect();
            self.take_ids(session, &waiting);
        }
    }

    /// Takes announced ids into a session that has the go: its pending set,
    /// and the table.
    fn take_ids(&mut self, session: SessionId, entries: &[(UnminedTxId, u32)]) {
        let mut fresh = Vec::new();
        let mut taken: HashSet<UnminedTxId> = HashSet::default();
        let seq = self.last_seq;
        let shorts: Vec<u64> = entries.iter().map(|(id, _)| self.short(*id)).collect();
        {
            let Some(state) = self.sessions.get_mut(&session) else {
                return;
            };
            for ((id, size), short) in entries.iter().copied().zip(shorts) {
                state.mark_known(short, seq);
                // Announcing an id while a request for it is outstanding says
                // the peer has it now. A `NotFound` for that request, which
                // can arrive later on the other stream, is still the peer's
                // try, but leaves what the peer told this node alone, and
                // the announcement is taken afresh once that request ends.
                // So both orders of the two end the same way.
                if state.requests.contains_key(&id) {
                    state.reannounced.insert(id, size);
                }
                if self.visible.contains(&id) || size > MAX_TX_BYTES {
                    continue;
                }
                let current = self.wanted.entry(id).map(|entry| entry.generation);
                // A peer that tried this entry is not asked again, however
                // often it announces the id, until the entry ends.
                if current
                    .is_some_and(|generation| self.reactor.has_tried(state.peer, id, generation))
                {
                    continue;
                }
                let already = current.is_some_and(|generation| state.holds(id, generation));
                // A request whose fetch was given up, such as one for an older
                // generation, does not stop a fresh announcement. The id is
                // asked for again only once that request is answered.
                let busy = state.reserving.contains_key(&id)
                    || state
                        .requests
                        .get(&id)
                        .is_some_and(|request| request.reservation.is_some())
                    || current.is_some_and(|generation| state.submissions.holds(id, generation));
                if already || busy || !taken.insert(id) {
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
            let state = self.sessions.get_mut(&session).expect("checked above");
            state.pending.push(Pending {
                id,
                generation,
                size,
            });
            file(state, &self.wanted, id);
            self.holders.entry(id).or_default().insert(session);
        }
    }

    /// `session` reconsiders announcing `id` to its peer, as a read of its
    /// admission would: at once inside its window, after the advert delay
    /// outside it, or not at all if the peer is still one to skip. The delay
    /// counts from `seen`, when the session first read the admission, if it
    /// did.
    fn reconsider_advert(&mut self, session: SessionId, id: UnminedTxId, seen: Option<Time>) {
        if !self.pool_live || !self.visible.contains(&id) {
            return;
        }
        let short = self.short(id);
        let Some(state) = self.sessions.get(&session) else {
            return;
        };
        if !self.reactor.has_go(state.peer, session)
            || state.announced.contains_key(&id)
            || state.held.contains(id)
            || state.skips(id, short)
            || self.reactor.holds_for(state.peer, id)
        {
            return;
        }
        let rank = self.reactor.rank(session);
        let window = self.fan_out.window(self.sizes.get(&id).copied());
        let seq = self.last_seq;
        let now = self.now;
        let state = self.sessions.get_mut(&session).expect("checked above");
        let due = seen
            .unwrap_or(now)
            .saturating_add(self.fan_out.advert_delay);
        let at_once = rank.is_some_and(|rank| rank < window) || due <= now;
        if at_once {
            state.announced.insert(id, seq);
            self.effects.push(RelayEffect::Advert { session, id });
        } else {
            // One timer stands for every advert of the session due then.
            if state.held.push(id, due, short) {
                self.timers.push(due, Timer::Held(session));
            }
            self.held_due = Some(self.held_due.map_or(due, |held| held.min(due)));
        }
    }

    /// Sends held adverts that are due.
    fn release_adverts(&mut self) {
        let now = self.now;
        if self.held_due.is_none_or(|due| due > now) {
            return;
        }
        let due: Vec<(SessionId, Vec<(UnminedTxId, u64)>)> = self
            .sessions
            .iter_mut()
            .map(|(session, state)| (*session, state.held.take_due(now)))
            .filter(|(_, due)| !due.is_empty())
            .collect();
        self.held_due = self
            .sessions
            .values()
            .filter_map(|state| state.held.first_due())
            .min();
        for (session, ids) in due {
            for (id, short) in ids {
                let state = self.sessions.get_mut(&session).expect("listed above");
                if self.visible.contains(&id)
                    && !state.announced.contains_key(&id)
                    && !(state.skips(id, short) || self.reactor.holds_for(state.peer, id))
                {
                    state.announced.insert(id, self.last_seq);
                    self.effects.push(RelayEffect::Advert { session, id });
                }
            }
        }
    }

    /// `peer` tried `id` in `generation`: it answered `NotFound`, missed the
    /// deadline, ended with the claim out, or supplied a body the pool
    /// rejected without settling the id. It is not asked for this entry
    /// again, so every session of the peer drops its reference. Once no
    /// holder is left, the entry ends.
    fn tried(&mut self, peer: PeerKey, id: UnminedTxId, generation: EntryGen) {
        // A try at an entry that already ended would never be forgotten,
        // and no later entry reads it.
        if self.wanted.current(id, generation).is_some() {
            self.reactor.mark_tried(peer, id, generation);
        }
        for (session, state) in self
            .sessions
            .iter_mut()
            .filter(|(_, state)| state.peer == peer)
        {
            if state.take_pending_of(id, generation).is_some() {
                self.wanted.unhold(id, generation);
                if !state.pending.contains(id) {
                    forget_holder(&mut self.holders, id, *session);
                }
            }
        }
    }

    /// Applies request deadlines that passed.
    fn expire(&mut self) {
        let now = self.now;
        let mut missed = Vec::new();
        let mut unresponsive = BTreeSet::new();
        while let Some((at, timer)) = self.timers.first() {
            if at > now {
                break;
            }
            self.timers.pop_first();
            if !self.timer_stands(at, timer) {
                continue;
            }
            match timer {
                Timer::Deadline(session, id) => missed.push((session, id)),
                Timer::GiveUp(session, _) => {
                    unresponsive.insert(session);
                }
                Timer::Held(_) | Timer::Cooling(_) => {}
            }
        }
        // In the order of a walk over every session's requests, so the
        // effects come out as that walk made them.
        missed.sort_by_key(|(session, id)| {
            (*session, self.sessions[session].requests.get_index_of(id))
        });
        // A deadline set twice to the same time has two timers.
        missed.dedup();
        let mut freed = Vec::new();
        for (session, id) in missed {
            let state = self.sessions.get_mut(&session).expect("the timer stands");
            let peer = state.peer;
            let request = state.requests.get_mut(&id).expect("the timer stands");
            // The wire deadline runs whether or not the pool still holds
            // the reservation: a fetch the pool cancelled is no try, but
            // left unanswered it still ends the session.
            let span = request.deadline.saturating_sub(request.sent);
            let gives_up_at = request.deadline.saturating_add(span);
            request.gives_up_at = Some(gives_up_at);
            if let Some(reservation) = request.reservation.take() {
                freed.push((session, peer, id, request.generation, reservation));
            }
            // Both can be due at once when the input comes late.
            if gives_up_at <= now {
                unresponsive.insert(session);
            } else {
                self.timers.push(gives_up_at, Timer::GiveUp(session, id));
            }
        }
        for (session, peer, id, generation, reservation) in freed {
            // A missed deadline is the peer's try at this entry.
            self.tried(peer, id, generation);
            self.reservations.remove(&reservation);
            self.give_back(reservation);
            self.wanted
                .release(id, generation, session, self.now, false);
        }
        // A request on the wire stays written until it is answered, so a
        // peer that leaves one unanswered for a further deadline loses the
        // session, which frees its slots.
        for session in unresponsive {
            self.effects.push(RelayEffect::Close {
                session,
                reason: CloseReason::Unresponsive,
            });
            self.end_session(session);
        }
    }

    /// Every session with a free slot claims and starts a reserve call,
    /// the id ready longest first. During a backoff only the probe's holder
    /// may.
    fn fill(&mut self) {
        // Each free slot goes to the id ready longest over every session, so
        // an older session cannot keep taking the room a verdict frees. Each
        // claim changes its entry, so the filing is brought up to date
        // before every pick, and on every step, so the changes never pile up.
        loop {
            self.refile_changed();
            if !self.pool_live || self.stopped {
                return;
            }
            let mut best: Option<((Time, u64), SessionId, Pending, Option<u64>)> = None;
            for session in self.sessions.keys().copied() {
                let probe = match self.wanted.admission(session, self.now) {
                    Admission::Closed => continue,
                    Admission::Open => None,
                    Admission::Probe(generation) => Some(generation),
                };
                let Some(pending) = self.next_claim(session) else {
                    continue;
                };
                let ready = self.ready_at(pending);
                if best.is_none_or(|(oldest, ..)| ready < oldest) {
                    best = Some((ready, session, pending, probe));
                }
            }
            let Some((_, session, pending, probe)) = best else {
                return;
            };
            self.wanted.claim(pending.id, pending.generation, session);
            if let Some(generation) = probe {
                self.wanted.take_probe(session, generation);
            }
            self.next_call = self.next_call.saturating_add(1);
            let call = CallId(self.next_call);
            let state = self.sessions.get_mut(&session).expect("listed above");
            let state_peer = state.peer;
            state.reserving.insert(
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
                    session,
                    peer: state_peer,
                    id: pending.id,
                    call,
                    tip: self.pool_tip,
                },
            );
            self.effects.push(RelayEffect::Reserve {
                call,
                id: pending.id,
            });
        }
    }

    /// When `pending` became ready to claim: at its first announcement, when
    /// its retry delay ended, or when its last claim ended undecided,
    /// whichever is last. Ties go by place in line, so an id released at the
    /// same moment another arrives goes behind it.
    fn ready_at(&self, pending: Pending) -> ReadyKey {
        self.wanted
            .current(pending.id, pending.generation)
            .map_or((Time::MAX, u64::MAX), ready_key)
    }

    /// The caller of `call` receives the pool's answer.
    fn deliver(&mut self, call: Call, answer: Result<Granted, Unreserved>) {
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
            // straight back, and a `Stopped` answer is still the core's.
            match answer {
                Ok(granted) => self.give_back(granted.reservation),
                Err(Unreserved::Stopped) => self.stopped = true,
                Err(_) => {}
            }
            return;
        };
        let Reserving { pending, probe, .. } = reserving;
        let peer = self.sessions.get(&session).expect("checked above").peer;
        // A rejection holds only for the chain it was made on. The caller
        // knows the tip it called at and the one it holds now, and a
        // same-height reorg changes the tip's hash.
        let chain_unchanged = self.pool_tip == call.tip;
        let still_claimed = self
            .wanted
            .current(id, pending.generation)
            .is_some_and(|entry| entry.claim == Some(session));
        match answer {
            Err(Unreserved::Stopped) => {
                // The core stopped. `tx-relay` claims nothing more.
                self.stopped = true;
                self.wanted
                    .release(id, pending.generation, session, self.now, false);
                self.return_probe(probe);
            }
            Ok(granted) if granted.cancelled => {
                // A cancelled token says nothing about why, so it decides
                // nothing: the slot goes back and the id is asked about
                // again, and the current pool answers.
                self.give_back(granted.reservation);
                self.wanted
                    .release(id, pending.generation, session, self.now, false);
                self.return_probe(probe);
            }
            Ok(granted) => {
                self.wanted.room();
                let cooling = self.reactor.is_cooling(peer, self.now);
                if still_claimed && self.pool_live && !cooling {
                    let state = self.sessions.get_mut(&session).expect("checked above");
                    state.send(pending, granted.reservation, self.now);
                    // Sending moves every unmissed deadline of the session.
                    for (id, request) in &state.requests {
                        if request.gives_up_at.is_none() {
                            self.timers
                                .push(request.deadline, Timer::Deadline(session, *id));
                        }
                    }
                    self.reservations.insert(granted.reservation, (session, id));
                    self.effects.push(RelayEffect::Ask { session, id });
                } else {
                    // The peer started cooling, the pool turned off, or the
                    // claim is gone: nothing is asked for.
                    self.give_back(granted.reservation);
                    self.wanted
                        .release(id, pending.generation, session, self.now, cooling);
                }
                self.return_probe(probe);
            }
            Err(Unreserved::NoRoom) => {
                self.wanted
                    .release_no_room(id, pending.generation, session, self.now);
                self.wanted.no_room(self.now, probe);
            }
            Err(Unreserved::Pending) => {
                self.wanted
                    .release_pending(id, pending.generation, session, self.now);
                self.return_probe(probe);
            }
            Err(Unreserved::Pooled) => {
                // It settles the id only if the caller's current snapshot
                // still holds it: the pool may have restarted since.
                // The pool answers `Pooled` by mined id, so the check is by
                // mined id too: another authorizing-data variant settles it.
                let pooled = self.visible_mined.contains_key(&id.mined_id());
                self.wanted
                    .release(id, pending.generation, session, self.now, !pooled);
                if pooled {
                    self.decided(id, pending.generation);
                }
                self.return_probe(probe);
            }
            Err(Unreserved::Rejected { scope }) => {
                // The same rule as a verdict's rejection.
                let remembered =
                    chain_unchanged && self.pool_live && !matches!(scope, RejectionScope::Uncached);
                self.wanted
                    .release(id, pending.generation, session, self.now, !remembered);
                if remembered {
                    self.decided(id, pending.generation);
                }
                self.return_probe(probe);
            }
            Err(Unreserved::Inactive) => {
                // The pool turned off, perhaps before a snapshot said so. The
                // id waits a retry delay, so a missed turn-off and on again
                // costs one delay, not a busy loop.
                self.wanted
                    .release(id, pending.generation, session, self.now, true);
                self.return_probe(probe);
            }
        }
    }

    /// Frees the probe permit a call held, if it held one.
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
        // The id ready longest goes first: since its first announcement, or
        // since its retry delay ended. Neither fresh work nor retries starve
        // the other. The filed entries are in that order, and one filed
        // later than now is still in its retry delay, as is every one after
        // it, since an entry is ready no earlier than its retry time.
        for ((ready, _), pending) in state.pending.ready() {
            if *ready > self.now {
                return None;
            }
            // A request given up but still on the wire, or a reserve call,
            // for the id holds it back.
            if state.requests.contains_key(&pending.id) || state.reserving.contains_key(&pending.id)
            {
                continue;
            }
            return Some(*pending);
        }
        None
    }

    /// Re-files every id whose entry changed since the last call, so every
    /// holder's filed entries are current before a claim.
    fn refile_changed(&mut self) {
        for id in self.wanted.take_changed() {
            self.refile(id);
        }
    }

    /// Files `id` again in every session that holds it, and forgets the
    /// sessions that no longer do.
    fn refile(&mut self, id: UnminedTxId) {
        let Some(holders) = self.holders.get_mut(&id) else {
            return;
        };
        let (sessions, wanted) = (&mut self.sessions, &self.wanted);
        holders.retain(|session| {
            let Some(state) = sessions.get_mut(session) else {
                return false;
            };
            if !state.pending.contains(id) {
                state.pending.unfile(id);
                return false;
            }
            file(state, wanted, id);
            true
        });
        if holders.is_empty() {
            self.holders.remove(&id);
        }
    }
}

/// Something that comes due with no input. Each names where its time is
/// kept, which [`RelayMachine::timer_stands`] reads.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Timer {
    /// A request's wire deadline, while it has not missed it.
    Deadline(SessionId, UnminedTxId),
    /// A request that missed its deadline ends its session at this time.
    GiveUp(SessionId, UnminedTxId),
    /// One of the session's held adverts comes due.
    Held(SessionId),
    /// The peer's cooldown ends.
    Cooling(PeerKey),
}

/// Timers in the order they come due, so the next is at the front. A timer
/// is added each time its source sets a time. A held advert's timer is not
/// taken out when the advert leaves another way, so one may no longer stand;
/// readers check. Every other kind has one time at once, so a newer push
/// replaces the older entry, and peer churn or repeated sends cannot pile
/// them up.
#[derive(Clone, Debug, Default)]
struct Timers {
    set: BTreeMap<(Time, u64), Timer>,
    /// Where each timer of a kind with one time at once sits in `set`.
    latest: HashMap<Timer, (Time, u64)>,
    next: u64,
}

impl Timers {
    /// Adds `timer`, due at `at`, in place of any earlier entry of the same
    /// request or peer.
    fn push(&mut self, at: Time, timer: Timer) {
        self.next = self.next.saturating_add(1);
        let key = (at, self.next);
        // A held advert's timer stands for every advert due then, so each
        // distinct due time keeps its own.
        if !matches!(timer, Timer::Held(_)) {
            if let Some(old) = self.latest.insert(timer, key) {
                self.set.remove(&old);
            }
        }
        self.set.insert(key, timer);
    }

    /// The first timer, if any.
    fn first(&self) -> Option<(Time, Timer)> {
        self.set
            .first_key_value()
            .map(|((at, _), timer)| (*at, *timer))
    }

    /// Takes the first timer out.
    fn pop_first(&mut self) {
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

/// How many of `ids` share each mined id.
fn mined_counts(ids: impl IntoIterator<Item = UnminedTxId>) -> HashMap<transaction::Hash, usize> {
    let mut counts: HashMap<transaction::Hash, usize> = HashMap::default();
    for id in ids {
        *counts.entry(id.mined_id()).or_default() += 1;
    }
    counts
}

/// Takes `session` out of `id`'s holders once it holds no pending
/// reference to `id`, so ended sessions and spent tries don't stay in the
/// index while another session keeps the entry alive.
fn forget_holder(
    holders: &mut HashMap<UnminedTxId, BTreeSet<SessionId>>,
    id: UnminedTxId,
    session: SessionId,
) {
    if let Some(sessions) = holders.get_mut(&id) {
        sessions.remove(&session);
        if sessions.is_empty() {
            holders.remove(&id);
        }
    }
}

/// Where `entry` stands in line. See [`RelayMachine::ready_at`].
fn ready_key(entry: &Entry) -> ReadyKey {
    let ready = entry
        .first_seen
        .max(entry.retry_at)
        .max(entry.requeued_at.unwrap_or_default());
    (ready, entry.order)
}

/// Files `id` in `state` if its session may claim it now, apart from timing
/// and its own calls, and unfiles it otherwise. It may when the session
/// holds the current entry pending and nobody claims it.
fn file(state: &mut PeerState, wanted: &WantedIds, id: UnminedTxId) {
    let claimable = wanted
        .entry(id)
        .filter(|entry| entry.claim.is_none() && state.pending.holds(id, entry.generation));
    match claimable
        .and_then(|entry| Some((state.pending.get(id, entry.generation)?, ready_key(entry))))
    {
        Some((pending, key)) => state.pending.file(pending, key),
        None => state.pending.unfile(id),
    }
}

/// One change in the latest batch, with what every session's read of it
/// shares.
#[derive(Clone, Debug)]
struct Logged {
    /// The change's position in the pool's log.
    seq: u64,
    /// The change.
    change: Change,
    /// The id's short id, for the changes a read checks skip sets with, and
    /// 0 for the others.
    short: u64,
    /// Whether no later change in the batch names the same id.
    last: bool,
}

/// The id a change is about.
fn change_id(change: &Change) -> UnminedTxId {
    match *change {
        Change::Added(id) | Change::Removed { id, .. } | Change::Refused(id) => id,
    }
}
