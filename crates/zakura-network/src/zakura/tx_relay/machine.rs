//! The pure `tx-relay` machines.
//!
//! Each session's routine runs a [`PeerMachine`], which owns everything
//! about its peer while the session is live: its [`peer::PeerState`], with
//! its pending ids and the size it announced for each, its outstanding
//! requests and their deadlines, its submissions, its limit of
//! [`MAX_REQUESTS_PER_PEER`] and what it was announced, and also its own
//! reserve calls, reservations and timers. What every routine shares is a
//! [`Shared`]:
//!
//! - [`wanted::WantedIds`]: the ids this node wants, each with its claim,
//!   holder count, timing and the peers that tried it, and a ring of the ids
//!   whose entry changed, which each routine reads from its own cursor.
//! - [`reactor::Reactor`]: cooldowns and parks, which session of a peer may
//!   fetch, the submissions and reserve calls of ended sessions, and the
//!   ranking.
//!
//! The machines do no IO and call nothing. The IO hands each one its peer's
//! messages, the pool's answers and the clock as inputs, with the pool as
//! the snapshot its routine last read, and carries out the
//! [`RelayEffect`]s. A step's effect on another session, a close or the go,
//! is a [`NodeEffect`] the IO routes to that session, and [`Marks`] says
//! when every session looks again. The tests compose every session's
//! machine as one `RelayMachine`, which wakes sessions by those rules.
//!
//! The pool is behind effects. A reserve call goes out as
//! [`RelayEffect::Reserve`] and its answer comes back through
//! [`PeerMachine::reserve_answer`]. A body goes out as
//! [`RelayEffect::Submit`] and its verdict comes back through
//! [`PeerMachine::verdict`]. A reservation given back goes out as
//! [`RelayEffect::Release`], which the IO carries out by dropping the
//! [`Reservation`](zakura_node_services::mempool::port::Reservation), or as
//! [`RelayEffect::FetchFailed`]. The pool's change log and snapshot come in
//! through [`PeerMachine::pool_changed`], and a fetch the pool cancelled
//! through [`PeerMachine::fetch_cancelled`].
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
//!   then sends `GetTxs`. One claim per id. Each session takes its id ready
//!   longest first: ready since its first announcement, the end of its
//!   retry delay, or the end of its last claim that left it undecided,
//!   whichever is last. Every such end counts, whether a session gave up or
//!   the pool answered, except a `NoRoom` answer, after which the backoff
//!   waits rather than the id. So an id whose attempts keep ending
//!   undecided, whoever holds it, queues behind work that was already
//!   waiting. A slot given back is room, so it ends a `NoRoom` backoff. So
//!   neither fresh ids nor retries starve the other. A grant whose caller
//!   gave up goes straight back.
//! - **A session is one connection's `tx-relay` streams.** A connection
//!   carries at most one at a time, so closing a session's connection ends
//!   that session only. A session can end and be reopened on the same
//!   connection, and a closed peer can redial at once, so a charge closes
//!   only live sessions and leaves the rest to the cooldown on the peer id.
//! - **A stopped core ends claiming for good.** The port has no way to
//!   attach another core. A stopped core publishes nothing more, so the
//!   last snapshot still shows an active pool and no fetch token fires. The
//!   machine learns of the stop from a `Stopped` answer or verdict, or from
//!   the IO seeing the core go, and from then on claims nothing. Its
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
//!   call given up, or an admission a pool restart erased. The table keeps
//!   the tries on the entry, by peer, for its life, so announcing the id
//!   again or reconnecting gives no second try, and every session of the
//!   peer drops its reference when it reads the ring. Past [`TRIED_PER_ENTRY`] peers at an entry
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
//!   dropped unread. If one more deadline passes without an answer, the
//!   peer stalled, so a peer that never answers cannot hold its slots for
//!   ever. That time is fixed when the request misses its deadline, and
//!   later requests never move it.
//! - **A stall parks the session first.** A peer stalls when it leaves a
//!   request unanswered as above, or stops reading until one of this node's
//!   writers is full. The first stall ends only the session, so the peer's
//!   other services keep going, and the peer's next sessions are refused
//!   for [`PARK_COOLDOWN`]. A stall in the session the park lets in
//!   afterwards closes the connection, as block sync does. Neither is a
//!   charge.
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
//! - **A decision drops the id from every pending set**, each as its
//!   routine reads the ring. A session that still held a fetch of the
//!   decided entry gives its reservation back then.
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
//!   announce at once, the rest after [`FanOut::advert_delay`]. An `Inv`
//!   carries an id only once none of its in-pool parents still waits to go
//!   out to that session.
//! - **An `Inv` waits for room.** One goes only while the data writer keeps
//!   room for every body the peer may have requested, once the session's
//!   last `Inv` is written, and once the byte rate pays for it.
//! - **Serve only what was announced, within our `Hello`.** A `GetTxs` gets
//!   the body only for an exact id this session announced and the pool
//!   still holds. A peer has at most [`MAX_SERVES`] requests whose answers
//!   the IO has not started, and never two for one id.
//!
//! Every close is a row of the spec's `tx-relay` close table, which names
//! its owner. Left out, because the IO and the transport own them: paying
//! the `Inv` byte rate and charging it on receipt, and decoding and writing
//! frames.

pub(crate) mod peer;
pub(crate) mod reactor;
pub(crate) mod session;
pub(crate) mod wanted;

#[cfg(test)]
mod budget_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet};
use std::{fmt, time::Duration};

#[cfg(test)]
use indexmap::IndexSet;
use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTx, UnminedTxId},
};
use zakura_node_services::mempool::port::{
    ChainTip, Change, Charge, LocalFault, RejectionScope, ReservationId,
};
use zakura_rt::collections::HashMap;

#[cfg(test)]
use self::peer::Hello;
pub(crate) use self::session::PeerMachine;
use self::{
    peer::{PeerState, ReadyKey},
    reactor::{Reactor, Recorded},
    session::Submitted,
    wanted::{Entry, WantedIds},
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

/// How long a session waits to try its next `Inv` again while its last one
/// is unwritten or its data writer has no room. A write frees both, and
/// wakes nothing.
pub(crate) const INV_RETRY: Duration = Duration::from_millis(50);

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

/// Peers with a cooldown history, at most. The parks are bounded the same.
pub(crate) const MAX_COOLDOWN_PEERS: usize = 20_000;

/// How long a peer whose session stalled has its sessions refused, as
/// block sync's no-progress cooldown.
pub(crate) const PARK_COOLDOWN: Duration = Duration::from_secs(180);

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

/// What the pool's snapshot says alongside a batch of changes, for the
/// composed machine.
#[cfg(test)]
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

/// Why the node closes a peer's connection. Each names its row of the
/// spec's `tx-relay` close table, in `docs/specs/peer-message-regulation.md`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum CloseReason {
    /// The peer broke a wire rule.
    Violation(Violation),
    /// A body it supplied earned a ban. Row 13.
    Ban,
    /// A body it supplied earned a second cooldown. Row 14.
    RepeatedCooldown,
    /// A body it supplied earned a cooldown, and the full cooldown table had
    /// no room for the peer. Row 15.
    CooldownsFull,
    /// It left a request unanswered for one more deadline after missing it,
    /// in the session its park let in. Row 16.
    Unresponsive,
    /// It stopped reading what this node writes, in the session its park
    /// let in. Row 16.
    StoppedReading,
}

/// A broken wire rule, with its row of the close table. See [`CloseReason`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Violation {
    /// A second `Hello`. Row 4.
    HelloTwice,
    /// A `Hello` outside the protocol's limits. Row 5.
    BadHello,
    /// A `GetTxs` before the peer's `Hello`. Row 6.
    NoHelloFirst,
    /// An answer to nothing outstanding. Row 9.
    Unrequested,
    /// A body of another size than the peer announced. Row 10.
    WrongSize,
    /// A body that does not hash to the requested id. Row 11.
    WrongId,
    /// A body that does not decode as one transaction. Row 12.
    Undecodable,
    /// A request for an id the peer already has outstanding with us. Row 7.
    RequestedTwice,
    /// A request past the [`MAX_SERVES`] our `Hello` serves at once. Row 8.
    TooManyRequests,
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
    /// End `session` but not its connection: its peer stalled, or its
    /// peer's park still runs.
    Park {
        /// The session.
        session: SessionId,
    },
    /// Reserve a slot at the pool to fetch `id`, and answer through
    /// [`PeerMachine::reserve_answer`] with `call`.
    Reserve {
        /// The call.
        call: CallId,
        /// The id.
        id: UnminedTxId,
    },
    /// Submit `body` under `reservation`, and answer its verdict through
    /// [`PeerMachine::verdict`] with `waiter`.
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

/// When the connection's `Inv` byte rate lets a frame go, as the IO's
/// metered sender answers.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Paid {
    /// Now.
    Now,
    /// Once the rate pays for the frame, at this time.
    At(Time),
    /// Once this session's last `Inv` is written.
    Unwritten,
}

/// What the IO knows when the machine decides whether an `Inv` goes now.
/// See [`PeerMachine::next_inv`].
pub(crate) struct InvRoom<F> {
    /// Places free in the data writer's queue.
    pub(crate) free: usize,
    /// When the byte rate pays for a frame of so many payload bytes.
    pub(crate) paid: F,
}

/// The pool as a routine sees it: the snapshot it last read.
pub(crate) trait PoolView: fmt::Debug {
    /// Whether the pool is on.
    fn live(&self) -> bool;
    /// The tip the pool is valid for.
    fn tip(&self) -> ChainTip;
    /// The change-log position the view is at.
    fn last_seq(&self) -> u64;
    /// Whether the pool holds exactly `id`, authorizing data and all.
    fn holds(&self, id: &UnminedTxId) -> bool;
    /// Whether the pool holds a transaction with these effects.
    fn holds_mined(&self, id: &transaction::Hash) -> bool;
    /// The serialized size of `id`, if the pool holds exactly it.
    fn size(&self, id: &UnminedTxId) -> Option<u32>;
    /// Every id the pool holds.
    fn ids(&self) -> Vec<UnminedTxId>;
    /// The ids the pool holds whose outputs `id` spends.
    fn parents(&self, id: &UnminedTxId) -> Vec<UnminedTxId>;
}

/// What one session's step does to another session, for the IO to route.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum NodeEffect {
    /// A charge closed `session`, whose [`RelayEffect::Close`] is already
    /// out. It ends.
    Closed(SessionId),
    /// `session` got the go: it opens.
    Go(SessionId),
    /// A verdict on a body `peer` supplied arrived: its sessions other than
    /// `except` mark `id` known.
    Known {
        /// The supplier.
        peer: PeerKey,
        /// The body's id.
        id: UnminedTxId,
        /// The session that marked it already.
        except: Option<SessionId>,
    },
    /// The reactor got the answer to a reserve call an ended session of
    /// `peer` made, which frees one of the peer's requests: its sessions
    /// look again.
    Look(PeerKey),
}

impl NodeEffect {
    /// The sessions this effect reaches, of the live `sessions` with their
    /// peers, and the mail each reads at its next step, if any. Each one it
    /// reaches looks again.
    pub(crate) fn reach(
        self,
        sessions: impl IntoIterator<Item = (SessionId, PeerKey)>,
    ) -> (Vec<SessionId>, Option<Mail>) {
        let of_peer = |peer: PeerKey, except: Option<SessionId>| -> Vec<SessionId> {
            sessions
                .into_iter()
                .filter(|(session, of)| *of == peer && Some(*session) != except)
                .map(|(session, _)| session)
                .collect()
        };
        match self {
            Self::Closed(session) => (vec![session], Some(Mail::Closed)),
            Self::Go(session) => (vec![session], Some(Mail::Go)),
            Self::Known { peer, id, except } => (of_peer(peer, except), Some(Mail::Known(id))),
            Self::Look(peer) => (of_peer(peer, None), None),
        }
    }
}

/// What one session's step leaves another to read at its next step. See
/// [`PeerMachine::read_mail`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Mail {
    /// A charge closed it.
    Closed,
    /// It got the go.
    Go,
    /// A verdict on a body its peer supplied arrived.
    Known(UnminedTxId),
}

/// Where the shared state stood before a step. Once the ring moved or
/// claims opened up, every session looks again: any of them may hold a
/// changed id, or may claim now.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Marks {
    ring_end: u64,
    openings: u64,
}

impl Marks {
    /// The marks of `shared` now.
    pub(crate) fn of(shared: &Shared) -> Self {
        Self {
            ring_end: shared.wanted.ring_end(),
            openings: shared.wanted.openings(),
        }
    }

    /// Whether every session looks again after the step these marks came
    /// before.
    pub(crate) fn wake_all(self, shared: &Shared) -> bool {
        self != Self::of(shared)
    }
}

/// What every session's routine shares: the wanted table, the reactor, the
/// node's view of the chain, and what ended sessions left behind.
#[derive(Debug)]
pub(crate) struct Shared {
    /// The ids this node wants.
    pub(crate) wanted: WantedIds,
    /// Node-wide state about peers and sessions.
    pub(crate) reactor: Reactor,
    /// The node's best tip, which can run ahead of the pool's. No cooldown
    /// applies until the IO sets it.
    best_tip: Height,
    /// Whether the node is close to the network's tip.
    current: bool,
    /// `tx-relay` was told the core stopped, so it claims nothing more and
    /// a slot given back is no room.
    pub(crate) stopped: bool,
    /// How many sessions hear of an admission at once, and when the rest do.
    pub(crate) fan_out: FanOut,
    /// The parameters of the keyed short-id hash.
    short_id: blake2b_simd::Params,
    /// The newest log position whose refusals were applied. Every routine
    /// reads the log, and the first to read a refusal applies it.
    log_read: u64,
    next_session: u64,
    next_call: u64,
    next_waiter: u64,
    /// The reserve calls of ended sessions, with their peers. Their answers
    /// come here.
    pub(crate) ended_calls: HashMap<CallId, PeerKey>,
    /// Who submitted each body an ended session left awaiting its verdict.
    pub(crate) ended_submitted: HashMap<WaiterId, Submitted>,
    /// What `tx-relay` did, in order, until the IO takes it.
    pub(crate) effects: Vec<RelayEffect>,
    /// Effects on other sessions, in order, until the IO routes them.
    pub(crate) node: Vec<NodeEffect>,
}

impl Shared {
    /// The shared state of a new `tx-relay`.
    pub(crate) fn new(config: RelayConfig) -> Self {
        let mut short_id = blake2b_simd::Params::new();
        short_id
            .hash_length(8)
            .key(&config.short_id_key.to_le_bytes())
            .personal(b"Zakura_TxShortId");
        Self {
            wanted: WantedIds::default(),
            reactor: Reactor::default(),
            best_tip: Height(0),
            current: true,
            stopped: false,
            fan_out: config.fan_out,
            short_id,
            log_read: 0,
            next_session: 0,
            next_call: 0,
            next_waiter: 0,
            ended_calls: HashMap::default(),
            ended_submitted: HashMap::default(),
            effects: Vec::new(),
            node: Vec::new(),
        }
    }

    /// Whether the node is close to the network's tip.
    pub(crate) fn set_current(&mut self, current: bool) {
        self.current = current;
    }

    /// The node's best tip moved, perhaps ahead of the pool's.
    pub(crate) fn set_best_tip(&mut self, tip: Height) {
        self.best_tip = tip;
    }

    /// Names a new session.
    pub(crate) fn new_session(&mut self) -> SessionId {
        self.next_session = self.next_session.saturating_add(1);
        SessionId(self.next_session)
    }

    /// Names a new reserve call.
    pub(crate) fn new_call(&mut self) -> CallId {
        self.next_call = self.next_call.saturating_add(1);
        CallId(self.next_call)
    }

    /// Names a new submission.
    pub(crate) fn new_waiter(&mut self) -> WaiterId {
        self.next_waiter = self.next_waiter.saturating_add(1);
        WaiterId(self.next_waiter)
    }

    /// The short id the skip sets keep for `id`: a keyed hash of its exact
    /// bytes, so two ids collide only by chance.
    pub(crate) fn short(&self, id: UnminedTxId) -> u64 {
        let mut state = self.short_id.to_state();
        state.update(&id.mined_id().0);
        if let Some(auth) = id.auth_digest() {
            state.update(&auth.0);
        }
        let mut short = [0u8; 8];
        short.copy_from_slice(&state.finalize().as_bytes()[..8]);
        u64::from_le_bytes(short)
    }

    /// Ranks the sessions again if the refresh is due at `now`.
    pub(crate) fn refresh(&mut self, now: Time) {
        if self.reactor.refresh_due(now) {
            self.reactor.refresh(now);
        }
    }

    /// Gives `reservation` back to the pool unsubmitted. A slot given back
    /// is room, unless the core stopped.
    pub(crate) fn give_back(&mut self, reservation: ReservationId) {
        self.effects.push(RelayEffect::Release { reservation });
        if !self.stopped {
            self.wanted.room();
        }
    }

    /// An answer to a reserve call no live session made: its grant goes
    /// back, and a `Stopped` answer still says the core stopped.
    pub(crate) fn answer_unknown(&mut self, answer: Result<Granted, Unreserved>) {
        match answer {
            Ok(granted) => self.give_back(granted.reservation),
            Err(Unreserved::Stopped) => self.stopped = true,
            Err(_) => {}
        }
    }

    /// The pool answered `call`, which a session that ended made. Returns
    /// whether one did.
    pub(crate) fn ended_call_answer(
        &mut self,
        call: CallId,
        answer: Result<Granted, Unreserved>,
    ) -> bool {
        let Some(peer) = self.ended_calls.remove(&call) else {
            return false;
        };
        self.reactor.call_answered(peer);
        self.node.push(NodeEffect::Look(peer));
        self.answer_unknown(answer);
        true
    }

    /// The pool's verdict on `waiter`, which a session that ended submitted.
    /// Returns whether one did.
    pub(crate) fn ended_verdict(
        &mut self,
        pool: &dyn PoolView,
        waiter: WaiterId,
        verdict: Verdict,
        now: Time,
    ) -> bool {
        let Some((peer, id, generation)) = self.reactor.settle(waiter) else {
            return false;
        };
        let submitted = self
            .ended_submitted
            .remove(&waiter)
            .expect("every submission records its submitter");
        // The supplier's sessions learn of the body again as the verdict
        // arrives.
        self.node.push(NodeEffect::Known {
            peer,
            id,
            except: None,
        });
        self.apply_verdict(pool, peer, id, generation, submitted, verdict, now);
        true
    }

    /// What a verdict on a body of `id`, fetched for `generation` and
    /// supplied by `peer`, does to the table and the peer. The submission
    /// held the claim since its check, so no other fetch of this entry
    /// started meanwhile. A verdict acts only on the entry its body was
    /// fetched for, and releases only its own claim.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_verdict(
        &mut self,
        pool: &dyn PoolView,
        peer: PeerKey,
        id: UnminedTxId,
        generation: EntryGen,
        submitted: Submitted,
        verdict: Verdict,
        now: Time,
    ) {
        let session = submitted.session;
        // A verdict frees a pool slot.
        self.wanted.room();
        match verdict {
            Verdict::Admitted => {
                // It settles the id only if the current snapshot holds it.
                if pool.holds(&id) {
                    self.wanted.decide(id, generation);
                } else {
                    self.wanted.release(id, generation, session, now, false);
                }
            }
            Verdict::Rejected { scope, charge, tip } => {
                // A rejection of a scope the pool caches, on the chain it was
                // made on, settles the id when it arrives. See the module
                // docs for why a restart or a cleared cache is not checked.
                let remembered = !matches!(scope, RejectionScope::Uncached)
                    && pool.live()
                    && pool.tip() == submitted.tip;
                if remembered {
                    self.wanted.decide(id, generation);
                } else {
                    // A body the pool rejected without settling the id was
                    // its supplier's try.
                    self.tried(peer, id, generation);
                    self.wanted.release(id, generation, session, now, true);
                }
                // The charge was earned against the tip the pool decided at,
                // which a verdict kept for a later reservation carries. An
                // unconditional peer is never charged, as the legacy adapter
                // never charges it.
                if !submitted.exempt {
                    self.charge(peer, charge, tip, now);
                }
            }
            // A full pool, a stopped core or a local fault is this node's
            // doing, not the peer's.
            Verdict::LocalFault(LocalFault::Stopped) => {
                self.stopped = true;
                self.wanted.release(id, generation, session, now, false);
            }
            Verdict::NoRoom | Verdict::LocalFault(_) => {
                self.wanted.release(id, generation, session, now, true);
            }
        }
    }

    /// The changes a read of the pool's log found, as each session reads
    /// them: each with its log position, its id's short id, and whether no
    /// later change in the read names the same id. A refusal ends a retry
    /// that waited for another caller, such as the legacy adapter.
    pub(crate) fn log(
        &mut self,
        changes: impl IntoIterator<Item = (u64, Change)>,
        now: Time,
    ) -> Vec<Logged> {
        let mut read: Vec<Logged> = Vec::new();
        let mut last: HashMap<UnminedTxId, usize> = HashMap::default();
        for (seq, change) in changes {
            if let Change::Refused(id) = change {
                if seq > self.log_read {
                    self.wanted.other_ended(id, now);
                }
            }
            self.log_read = self.log_read.max(seq);
            let id = change_id(&change);
            if let Some(earlier) = last.insert(id, read.len()) {
                read[earlier].last = false;
            }
            let short = match change {
                Change::Added(_) | Change::Removed { .. } => self.short(id),
                Change::Refused(_) => 0,
            };
            read.push(Logged {
                seq,
                change,
                short,
                last: true,
            });
        }
        read
    }

    /// `peer` tried `id` in `generation`. Its sessions drop their references
    /// when they read the ring.
    pub(crate) fn tried(&mut self, peer: PeerKey, id: UnminedTxId, generation: EntryGen) {
        let reactor = &self.reactor;
        self.wanted
            .mark_tried(id, generation, peer, |tried| reactor.has_sessions(*tried));
    }

    /// Applies a body's charge to the peer that supplied it. `decided_at` is
    /// the tip the verdict was decided at.
    fn charge(&mut self, peer: PeerKey, charge: Charge, decided_at: Height, now: Time) {
        match charge {
            Charge::None => {}
            Charge::Cooldown => {
                // The legacy adapter's two conditions.
                if !self.current || self.best_tip != decided_at {
                    return;
                }
                match self.reactor.record_invalid(peer, now) {
                    Recorded::AlreadyCooling => {}
                    Recorded::Started(length) => {
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
                self.reactor.ban(peer, now);
                self.close_peer(peer, CloseReason::Ban);
            }
        }
    }

    /// Closes every live session of `peer`.
    fn close_peer(&mut self, peer: PeerKey, reason: CloseReason) {
        for session in self.reactor.sessions_of(peer) {
            self.effects.push(RelayEffect::Close { session, reason });
            self.node.push(NodeEffect::Closed(session));
        }
    }
}

/// One change in a published batch, with what every session's read of it
/// shares.
#[derive(Clone, Debug)]
pub(crate) struct Logged {
    /// The change's position in the pool's log.
    pub(crate) seq: u64,
    /// The change.
    pub(crate) change: Change,
    /// The id's short id, for the changes a read checks skip sets with, and
    /// 0 for the others.
    pub(crate) short: u64,
    /// Whether no later change in the batch names the same id.
    pub(crate) last: bool,
}

/// The pool as the published changes describe it, with the sizes the
/// snapshots gave, for the composed machine.
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct Mirror {
    live: bool,
    tip: ChainTip,
    last_seq: u64,
    /// The pool, oldest first.
    visible: IndexSet<UnminedTxId>,
    /// How many visible ids share each mined id, so a `Pooled` answer is
    /// checked with one lookup.
    visible_mined: HashMap<transaction::Hash, usize>,
    /// The serialized size of each visible transaction.
    sizes: HashMap<UnminedTxId, u32>,
    /// Each transaction's parents, which count while they are visible.
    parents: HashMap<UnminedTxId, Vec<UnminedTxId>>,
}

#[cfg(test)]
impl Mirror {
    /// The pool `snapshot` shows, whose visible transactions are `visible`
    /// with their sizes, oldest first.
    pub(crate) fn new(snapshot: &SnapshotInfo, visible: Vec<(UnminedTxId, u32)>) -> Self {
        Self {
            live: snapshot.active,
            tip: snapshot.tip,
            last_seq: 0,
            visible: visible.iter().map(|(id, _)| *id).collect(),
            visible_mined: mined_counts(visible.iter().map(|(id, _)| *id)),
            sizes: visible.into_iter().collect(),
            parents: HashMap::default(),
        }
    }

    /// Makes `parents` the transactions whose outputs `child` spends.
    pub(crate) fn set_parents(&mut self, child: UnminedTxId, parents: Vec<UnminedTxId>) {
        self.parents.insert(child, parents);
    }

    /// Applies a published batch and the snapshot after it. Returns the
    /// batch as each session reads it.
    pub(crate) fn publish(
        &mut self,
        shared: &mut Shared,
        changes: Vec<Change>,
        snapshot: SnapshotInfo,
        now: Time,
    ) -> Vec<Logged> {
        self.live = snapshot.active;
        self.tip = snapshot.tip;
        let mut read = Vec::with_capacity(changes.len());
        for change in changes {
            match change {
                Change::Added(id) => self.show(id),
                Change::Removed { id, .. } => {
                    self.unshow(id);
                    self.sizes.remove(&id);
                }
                Change::Refused(_) => {}
            }
            self.last_seq = self.last_seq.saturating_add(1);
            read.push((self.last_seq, change));
        }
        for (id, size) in snapshot.sizes {
            if self.visible.contains(&id) {
                self.sizes.insert(id, size);
            }
        }
        shared.log(read, now)
    }

    /// Takes the whole pool from a resync: `visible`, oldest first, with the
    /// sizes `snapshot` adds.
    pub(crate) fn resync(&mut self, visible: Vec<(UnminedTxId, u32)>, snapshot: SnapshotInfo) {
        self.live = snapshot.active;
        self.tip = snapshot.tip;
        self.visible = visible.iter().map(|(id, _)| *id).collect();
        self.visible_mined = mined_counts(self.visible.iter().copied());
        self.sizes = visible.into_iter().collect();
        self.sizes.extend(snapshot.sizes);
    }

    /// The pool, oldest first.
    pub(crate) fn visible(&self) -> &IndexSet<UnminedTxId> {
        &self.visible
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

    /// Checks the mirror's own indexes.
    fn assert_invariants(&self) {
        let mined = mined_counts(self.visible.iter().copied());
        assert_eq!(self.visible_mined, mined, "relay/mined-id-counts");
        let sized = self.sizes.keys().all(|id| self.visible.contains(id));
        assert!(sized, "relay/sizes-of-visible-ids");
    }
}

#[cfg(test)]
impl PoolView for Mirror {
    fn live(&self) -> bool {
        self.live
    }

    fn tip(&self) -> ChainTip {
        self.tip
    }

    fn last_seq(&self) -> u64 {
        self.last_seq
    }

    fn holds(&self, id: &UnminedTxId) -> bool {
        self.visible.contains(id)
    }

    fn holds_mined(&self, id: &transaction::Hash) -> bool {
        self.visible_mined.contains_key(id)
    }

    fn size(&self, id: &UnminedTxId) -> Option<u32> {
        self.sizes.get(id).copied()
    }

    fn ids(&self) -> Vec<UnminedTxId> {
        self.visible.iter().copied().collect()
    }

    fn parents(&self, id: &UnminedTxId) -> Vec<UnminedTxId> {
        let parents = self.parents.get(id).into_iter().flatten();
        parents
            .filter(|parent| self.visible.contains(*parent))
            .copied()
            .collect()
    }
}

/// `tx-relay` as one machine, for the tests: every session's
/// [`PeerMachine`], the [`Shared`] state, and the pool as the published
/// changes describe it. An input goes to its session, and then only the
/// sessions the step woke tick, by the rules the routines wake by. See
/// [`Self::wake`].
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct RelayMachine {
    now: Time,
    shared: Shared,
    pool: Mirror,
    machines: BTreeMap<SessionId, PeerMachine>,
}

#[cfg(test)]
impl RelayMachine {
    /// `tx-relay` on the pool `snapshot` shows, whose visible transactions
    /// are `visible` with their sizes, oldest first.
    pub(crate) fn new(
        config: RelayConfig,
        snapshot: SnapshotInfo,
        visible: Vec<(UnminedTxId, u32)>,
    ) -> Self {
        Self {
            now: Duration::ZERO,
            shared: Shared::new(config),
            pool: Mirror::new(&snapshot, visible),
            machines: BTreeMap::new(),
        }
    }

    // Inputs from peers.

    /// A session of `peer` opened. See [`PeerMachine::connect`].
    pub(crate) fn connect(&mut self, peer: PeerKey, exempt: bool, now: Time) -> SessionId {
        self.now = self.now.max(now);
        let marks = Marks::of(&self.shared);
        let machine = PeerMachine::connect(&mut self.shared, &self.pool, peer, exempt, self.now);
        let session = machine.session();
        self.machines.insert(session, machine);
        self.wake(marks, BTreeSet::new());
        session
    }

    /// The peer's `Hello` arrived.
    pub(crate) fn hello(&mut self, session: SessionId, hello: Hello, now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.hello(shared, pool, hello, now);
        });
    }

    /// An `Inv` arrived with these ids and sizes.
    pub(crate) fn inv(&mut self, session: SessionId, entries: &[(UnminedTxId, u32)], now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.inv(shared, pool, entries, now);
        });
    }

    /// A `Tx` arrived answering the request for `id`, carrying `body`.
    pub(crate) fn tx(&mut self, session: SessionId, id: UnminedTxId, body: UnminedTx, now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.tx(shared, pool, id, body, now);
        });
    }

    /// A `Tx` for `id` with a body of `size` bytes arrived, not yet decoded.
    /// See [`PeerMachine::tx_arrived`].
    pub(crate) fn tx_arrived(
        &mut self,
        session: SessionId,
        id: UnminedTxId,
        size: u32,
        now: Time,
    ) -> bool {
        let mut read = false;
        self.on(session, now, |machine, shared, pool, now| {
            read = machine.tx_arrived(shared, pool, id, size, now);
        });
        read
    }

    /// A `NotFound` arrived.
    pub(crate) fn not_found(&mut self, session: SessionId, id: UnminedTxId, now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.not_found(shared, pool, id, now);
        });
    }

    /// A `GetTxs` arrived.
    pub(crate) fn get_txs(&mut self, session: SessionId, id: UnminedTxId, now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.get_txs(shared, pool, id, now);
        });
    }

    /// The IO started writing its answer to `session`'s request for `id`.
    /// See [`PeerMachine::served`].
    pub(crate) fn served(&mut self, session: SessionId, id: UnminedTxId, now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.served(shared, pool, id, now);
        });
    }

    /// `session`'s peer stopped reading what this node writes. See
    /// [`PeerMachine::stopped_reading`].
    pub(crate) fn stopped_reading(&mut self, session: SessionId, now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.stopped_reading(shared, pool, now);
        });
    }

    /// A body `tx_arrived` let the IO decode does not decode.
    pub(crate) fn undecodable(&mut self, session: SessionId, now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.undecodable(shared, pool, now);
        });
    }

    /// The session ended: the peer left, the connection closed, or the
    /// session was replaced.
    pub(crate) fn end(&mut self, session: SessionId, now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.end(shared, pool, now);
        });
    }

    // Inputs from the node and the pool.

    /// Time passed: each session with something due by now wakes, as its
    /// routine's timer does.
    pub(crate) fn tick(&mut self, now: Time) {
        self.now = self.now.max(now);
        let marks = Marks::of(&self.shared);
        let due = self
            .machines
            .iter()
            .filter(|(_, machine)| {
                machine
                    .next_deadline(&self.shared)
                    .is_some_and(|at| at <= self.now)
            })
            .map(|(session, _)| *session)
            .collect();
        self.wake(marks, due);
    }

    /// The pool answered the reserve call `call`.
    pub(crate) fn reserve_answer(
        &mut self,
        call: CallId,
        answer: Result<Granted, Unreserved>,
        now: Time,
    ) {
        self.now = self.now.max(now);
        let marks = Marks::of(&self.shared);
        let owner = self
            .machines
            .iter()
            .find(|(_, machine)| machine.calls.contains_key(&call))
            .map(|(session, _)| *session);
        match owner {
            Some(session) => {
                let machine = self.machines.get_mut(&session).expect("found above");
                machine.reserve_answer(&mut self.shared, &self.pool, call, answer, self.now);
            }
            None => {
                if !self.shared.ended_call_answer(call, answer) {
                    self.shared.answer_unknown(answer);
                }
            }
        }
        self.wake(marks, BTreeSet::new());
    }

    /// The pool's verdict on the submission `waiter`.
    pub(crate) fn verdict(&mut self, waiter: WaiterId, verdict: Verdict, now: Time) {
        self.now = self.now.max(now);
        let marks = Marks::of(&self.shared);
        let owner = self
            .machines
            .iter()
            .find(|(_, machine)| machine.state.submissions.contains_waiter(waiter))
            .map(|(session, _)| *session);
        match owner {
            Some(session) => {
                let machine = self.machines.get_mut(&session).expect("found above");
                machine.verdict(&mut self.shared, &self.pool, waiter, verdict, self.now);
            }
            None => {
                self.shared
                    .ended_verdict(&self.pool, waiter, verdict, self.now);
            }
        }
        self.wake(marks, BTreeSet::new());
    }

    /// The pool cancelled the fetch under `reservation`: its token fired.
    pub(crate) fn fetch_cancelled(&mut self, reservation: ReservationId, now: Time) {
        let owner = self
            .machines
            .iter()
            .find(|(_, machine)| machine.reservations.contains_key(&reservation))
            .map(|(session, _)| *session);
        if let Some(session) = owner {
            self.on(session, now, |machine, shared, pool, now| {
                machine.fetch_cancelled(shared, pool, reservation, now);
            });
        }
    }

    /// The pool published `changes`, one batch, and `snapshot` is the pool
    /// after it. A snapshot that turns the pool on again reopens every
    /// session with the go first, on the pool before the batch, as a
    /// restart does. Then every session with the go reads the batch.
    pub(crate) fn pool_changed(&mut self, changes: Vec<Change>, snapshot: SnapshotInfo, now: Time) {
        self.now = self.now.max(now);
        let marks = Marks::of(&self.shared);
        let activated = !self.pool.live && snapshot.active;
        let mut woken = BTreeSet::new();
        if activated {
            self.pool.live = true;
            self.pool.tip = snapshot.tip;
            for machine in self.machines.values_mut() {
                if self
                    .shared
                    .reactor
                    .has_go(machine.state.peer, machine.session())
                {
                    machine.open(&mut self.shared, &self.pool);
                }
            }
            woken = self.route();
        }
        let batch = self
            .pool
            .publish(&mut self.shared, changes, snapshot, self.now);
        for machine in self.machines.values_mut() {
            machine.pool_changed(&mut self.shared, &self.pool, &batch, false, self.now);
        }
        self.wake(marks, woken);
    }

    /// The IO lost its place in the pool's change log, so it hands over the
    /// whole pool, oldest first, with `snapshot`. See
    /// [`PeerMachine::pool_resynced`].
    pub(crate) fn pool_resynced(
        &mut self,
        visible: Vec<(UnminedTxId, u32)>,
        snapshot: SnapshotInfo,
        now: Time,
    ) {
        self.now = self.now.max(now);
        let marks = Marks::of(&self.shared);
        let activated = !self.pool.live && snapshot.active;
        self.pool.resync(visible, snapshot);
        for machine in self.machines.values_mut() {
            machine.pool_resynced(&mut self.shared, &self.pool, activated, self.now);
        }
        self.wake(marks, BTreeSet::new());
    }

    /// The core behind the port stopped for good, which the IO saw some
    /// other way than through an answer or a verdict. `tx-relay` claims
    /// nothing more. Every routine sees it.
    pub(crate) fn pool_stopped(&mut self, now: Time) {
        self.now = self.now.max(now);
        let marks = Marks::of(&self.shared);
        self.shared.stopped = true;
        let every = self.machines.keys().copied().collect();
        self.wake(marks, every);
    }

    /// The node's best tip moved, perhaps ahead of the pool's.
    pub(crate) fn set_best_tip(&mut self, tip: Height) {
        self.shared.set_best_tip(tip);
    }

    /// Makes `parents` the transactions whose outputs `child` spends, in
    /// the pool as the published changes describe it.
    pub(crate) fn set_parents(&mut self, child: UnminedTxId, parents: Vec<UnminedTxId>) {
        self.pool.set_parents(child, parents);
    }

    /// The parents of `id` the pool holds.
    pub(crate) fn parents(&self, id: &UnminedTxId) -> Vec<UnminedTxId> {
        self.pool.parents(id)
    }

    /// `session`'s routine reports its connection's smoothed round-trip
    /// time.
    pub(crate) fn observe_rtt(&mut self, session: SessionId, rtt: Duration, now: Time) {
        self.on(session, now, |machine, shared, pool, now| {
            machine.observe_rtt(shared, pool, rtt, now);
        });
    }

    // Outputs and observations.

    /// The `Inv` `session` may send now. See [`PeerMachine::next_inv`].
    pub(crate) fn next_inv(
        &mut self,
        session: SessionId,
        room: InvRoom<impl FnOnce(u64) -> Paid>,
        now: Time,
    ) -> Vec<(UnminedTxId, u32)> {
        let (pool, now) = (&self.pool, self.now.max(now));
        self.machines
            .get_mut(&session)
            .map(|machine| machine.next_inv(pool, room, now))
            .unwrap_or_default()
    }

    /// Takes up to `max` of `session`'s queued announcements. See
    /// [`PeerMachine::take_inv`].
    pub(crate) fn take_inv(&mut self, session: SessionId, max: usize) -> Vec<(UnminedTxId, u32)> {
        let pool = &self.pool;
        self.machines
            .get_mut(&session)
            .map(|machine| machine.take_inv(pool, max))
            .unwrap_or_default()
    }

    /// How many of `session`'s announcements wait for an `Inv`.
    pub(crate) fn queued_inv(&self, session: SessionId) -> usize {
        self.machines
            .get(&session)
            .map_or(0, PeerMachine::queued_inv)
    }

    /// What `tx-relay` did since the last call, in order.
    pub(crate) fn take_effects(&mut self) -> Vec<RelayEffect> {
        std::mem::take(&mut self.shared.effects)
    }

    /// The earliest time after now that some session has something due with
    /// no other input, which is when its routine's timer fires.
    pub(crate) fn next_deadline(&self) -> Option<Time> {
        self.machines
            .values()
            .filter_map(|machine| machine.next_deadline(&self.shared))
            .min()
    }

    // The step loop.

    /// Hands an input to `session`'s machine, then wakes the sessions the
    /// step owes a look.
    fn on(
        &mut self,
        session: SessionId,
        now: Time,
        input: impl FnOnce(&mut PeerMachine, &mut Shared, &Mirror, Time),
    ) {
        self.now = self.now.max(now);
        let marks = Marks::of(&self.shared);
        if let Some(machine) = self.machines.get_mut(&session) {
            input(machine, &mut self.shared, &self.pool, self.now);
        }
        self.wake(marks, BTreeSet::new());
    }

    /// Ticks the sessions owed a look, as their routines wake, until a pass
    /// owes none: those in `woken`, each one an effect names, and every
    /// session once the ring moved or claims opened up since `marks`. Then
    /// forgets the sessions that ended. A session nothing woke does not
    /// step, so a missing wake shows up as work left undone.
    fn wake(&mut self, mut marks: Marks, mut woken: BTreeSet<SessionId>) {
        loop {
            woken.extend(self.route());
            if marks.wake_all(&self.shared) {
                woken.extend(self.machines.keys().copied());
            }
            marks = Marks::of(&self.shared);
            if woken.is_empty() {
                break;
            }
            for session in std::mem::take(&mut woken) {
                if let Some(machine) = self.machines.get_mut(&session) {
                    machine.tick(&mut self.shared, &self.pool, self.now);
                }
            }
        }
        self.machines.retain(|_, machine| !machine.is_ended());
    }

    /// Hands each effect on other sessions to the sessions it reaches, which
    /// read their mail at once, as at their next step, until none is left.
    /// Returns the sessions reached.
    fn route(&mut self) -> BTreeSet<SessionId> {
        let mut reached = BTreeSet::new();
        while !self.shared.node.is_empty() {
            for effect in std::mem::take(&mut self.shared.node) {
                let live = self.machines.iter();
                let (to, mail) = effect.reach(live.map(|(id, machine)| (*id, machine.state.peer)));
                for session in to {
                    let Some(machine) = self.machines.get_mut(&session) else {
                        continue;
                    };
                    if let Some(mail) = mail {
                        machine.read_mail(&mut self.shared, &self.pool, mail);
                    }
                    reached.insert(session);
                }
            }
        }
        reached
    }
}

/// Read-only views for the tests and the composed invariants.
#[cfg(test)]
impl RelayMachine {
    /// How many timers the sessions keep, standing or not.
    pub(crate) fn timers_len(&self) -> usize {
        self.machines.values().map(PeerMachine::timers_len).sum()
    }

    /// Every live session.
    pub(crate) fn session_ids(&self) -> impl Iterator<Item = SessionId> + '_ {
        self.machines.keys().copied()
    }

    /// A session's state.
    pub(crate) fn session(&self, session: SessionId) -> Option<&PeerState> {
        self.machines.get(&session).map(|machine| &machine.state)
    }

    /// The wanted table.
    pub(crate) fn wanted(&self) -> &WantedIds {
        &self.shared.wanted
    }

    /// The reactor.
    pub(crate) fn reactor(&self) -> &Reactor {
        &self.shared.reactor
    }

    /// The reactor, to set up a state no inputs reach quickly.
    pub(crate) fn reactor_mut(&mut self) -> &mut Reactor {
        &mut self.shared.reactor
    }

    /// The pool, as the changes say, oldest first.
    pub(crate) fn visible(&self) -> &IndexSet<UnminedTxId> {
        self.pool.visible()
    }

    /// Whether `call` is a reserve call the pool has not answered.
    pub(crate) fn has_call(&self, call: CallId) -> bool {
        self.shared.ended_calls.contains_key(&call)
            || self
                .machines
                .values()
                .any(|machine| machine.calls.contains_key(&call))
    }

    /// Reserve calls the pool has not answered.
    pub(crate) fn calls_in_flight(&self) -> usize {
        let live: usize = self
            .machines
            .values()
            .map(|machine| machine.calls.len())
            .sum();
        live + self.shared.ended_calls.len()
    }

    /// Bodies awaiting their verdicts.
    pub(crate) fn waiters_len(&self) -> usize {
        let live: usize = self
            .machines
            .values()
            .map(|machine| machine.state.submissions.len())
            .sum();
        live + self.shared.ended_submitted.len()
    }

    /// Whether `reservation` is one of `tx-relay`'s, outside submissions.
    pub(crate) fn holds_reservation(&self, reservation: ReservationId) -> bool {
        self.machines
            .values()
            .any(|machine| machine.reservations.contains_key(&reservation))
    }

    /// `tx-relay`'s reservations at the pool, outside submissions.
    pub(crate) fn reservations_held(&self) -> usize {
        self.machines
            .values()
            .map(|machine| machine.reservations.len())
            .sum()
    }

    /// Checks what holds between any two inputs. Each check states one rule
    /// of the module docs, or that an index kept for speed agrees with the
    /// state it indexes, worked out again the slow way.
    ///
    /// # Panics
    ///
    /// If one is broken, naming it.
    pub(crate) fn assert_invariants(&self) {
        let (now, reactor, wanted) = (self.now, &self.shared.reactor, &self.shared.wanted);
        let sessions = || {
            self.machines
                .iter()
                .map(|(session, machine)| (session, &machine.state))
        };

        // The reactor's sessions, the go and the ranking.
        let mut by_peer: BTreeMap<PeerKey, BTreeSet<SessionId>> = BTreeMap::new();
        for (session, state) in sessions() {
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
        let mut references: HashMap<(UnminedTxId, EntryGen), usize> = HashMap::default();
        for (session, machine) in &self.machines {
            let state = &machine.state;
            let go = with_go.contains(session);
            let fetches = go && state.hello.is_some();
            let idle = state.pending.len() == 0 && state.in_flight() == 0;
            assert!(fetches || idle, "relay/only-go-and-hello-hold-or-fetch");
            let waits = !state.waiting.is_empty();
            assert!(!(fetches && waits), "relay/go-and-hello-take-waiting");
            let told = state.announced.iter().next().is_some() || state.held.first_due().is_some();
            assert!(go || !told, "relay/only-go-is-announced-to");
            let read = state.cursor == self.pool.last_seq;
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
            let mut reservations: HashMap<ReservationId, UnminedTxId> = HashMap::default();
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
                    reservations.insert(reservation, *id);
                }
            }
            assert_eq!(
                machine.reservations, reservations,
                "relay/reservations-index"
            );
            // The claim queue, worked out again from the entries.
            let mut claimable: HashMap<UnminedTxId, ReadyKey> = HashMap::default();
            let mut once: zakura_rt::collections::HashSet<UnminedTxId> = Default::default();
            for pending in &state.pending {
                let entry = wanted.current(pending.id, pending.generation);
                let entry = entry.expect("relay/pending-names-the-current-entry");
                let key = (pending.id, entry.generation);
                *references.entry(key).or_default() += 1;
                assert!(once.insert(pending.id), "relay/one-reference-per-id");
                let tried = entry.tried.contains(&state.peer);
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
            machine.check_timers();
        }
        let ended = reactor.ended_calls().keys();
        let unsettled = reactor.unsettled().keys().chain(ended);
        let peers: BTreeSet<PeerKey> = in_flight.keys().chain(unsettled).copied().collect();
        for peer in peers {
            let live = in_flight.get(&peer).copied().unwrap_or(0);
            let count = live + reactor.unsettled_count(peer);
            assert!(count <= MAX_REQUESTS_PER_PEER, "relay/limit-counted-once");
        }

        // The table keeps nothing without a holder or a claim.
        for (id, entry) in wanted.entries() {
            let counted = references.get(&(*id, entry.generation)).copied();
            assert_eq!(entry.holders, counted.unwrap_or(0), "relay/holder-count");
            let alive = entry.holders > 0 || entry.claim.is_some();
            assert!(alive, "relay/entry-held-or-claimed");
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
        for (session, machine) in &self.machines {
            let state = &machine.state;
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
            let waiters: BTreeSet<WaiterId> = state.submissions.keys().copied().collect();
            let submitters: BTreeSet<WaiterId> = machine.submitted_at.keys().copied().collect();
            assert_eq!(waiters, submitters, "relay/waiter-of-a-live-session");
            for submitted in machine.submitted_at.values() {
                assert_eq!(
                    submitted.session, *session,
                    "relay/waiter-of-a-live-session"
                );
            }
        }
        let mut handed = 0;
        for unsettled in reactor.unsettled().values() {
            assert!(!unsettled.is_empty(), "relay/no-empty-unsettled-record");
            for (waiter, (id, generation)) in unsettled {
                let submitted = self.shared.ended_submitted.get(waiter);
                let submitted = submitted.expect("relay/handed-to-the-reactor-on-end");
                let ended = !self.machines.contains_key(&submitted.session);
                assert!(ended, "relay/handed-to-the-reactor-on-end");
                attempt(*id, *generation, submitted.session);
                handed += 1;
            }
        }
        assert_eq!(
            self.shared.ended_submitted.len(),
            handed,
            "relay/submitter-kept"
        );
        for (id, entry) in wanted.entries() {
            if let Some(session) = entry.claim {
                let attempt = attempts.get(&(*id, entry.generation));
                assert_eq!(attempt, Some(&session), "relay/claim-covers-an-attempt");
            }
        }

        // Reserve calls: each is a live session's reserve call, or counts
        // against the peer of the session that ended.
        let mut ended_calls: HashMap<PeerKey, usize> = HashMap::default();
        for peer in self.shared.ended_calls.values() {
            *ended_calls.entry(*peer).or_default() += 1;
        }
        assert_eq!(reactor.ended_calls(), &ended_calls, "relay/ended-calls");
        for machine in self.machines.values() {
            for (name, call) in &machine.calls {
                let held = machine.state.reserving.get(&call.id);
                let own = held.is_some_and(|held| held.call == *name);
                assert!(own, "relay/call-is-its-sessions");
            }
            let called = |held: &peer::Reserving| machine.calls.contains_key(&held.call);
            assert!(
                machine.state.reserving.values().all(called),
                "relay/call-in-flight"
            );
        }
        if let Some((session, generation)) = wanted.probe() {
            let machine = self.machines.get(&session);
            let mut calls = machine
                .into_iter()
                .flat_map(|machine| machine.state.reserving.values());
            let probing = calls.any(|held| held.probe == Some(generation));
            assert!(probing, "relay/probe-is-a-call-in-flight");
        }

        // The pool as the changes say, and what each session is told of it.
        self.pool.assert_invariants();
        for (_, state) in sessions() {
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
                assert!(self.pool.holds(id), "relay/advert-of-a-pooled-id");
                assert!(!busy(id), "relay/no-advert-of-an-id-in-flight");
            }
            for (id, _) in state.held.iter() {
                let pooled = self.pool.holds(&id) && !busy(&id);
                assert!(pooled, "relay/no-advert-of-an-id-in-flight");
                let announced = state.announced.contains_key(&id);
                assert!(!announced, "relay/held-advert-not-yet-announced");
            }
        }
    }
}

/// How many of `ids` share each mined id.
#[cfg(test)]
fn mined_counts(ids: impl IntoIterator<Item = UnminedTxId>) -> HashMap<transaction::Hash, usize> {
    let mut counts: HashMap<transaction::Hash, usize> = HashMap::default();
    for id in ids {
        *counts.entry(id.mined_id()).or_default() += 1;
    }
    counts
}

/// Where `entry` stands in line: when it became ready to claim, at its first
/// announcement, when its retry delay ended, or when its last claim ended
/// undecided, whichever is last. Ties go by place in line, so an id released
/// at the same moment another arrives goes behind it.
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

/// The id a change is about.
fn change_id(change: &Change) -> UnminedTxId {
    match *change {
        Change::Added(id) | Change::Removed { id, .. } | Change::Refused(id) => id,
    }
}
