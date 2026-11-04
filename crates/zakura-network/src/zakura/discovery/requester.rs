//! The requester side of discovery's queries: one reservation per query kind.
//!
//! The source reserves a query's response before the query's first byte is
//! written, and the reader claims that reservation before it decodes the
//! response. A response without a live reservation, a second response, and a
//! response above the reserved size are protocol violations, because only the
//! response itself or the session's end removes a reservation.
//!
//! Each reservation owns its query's
//! [`Exchange`](crate::zakura::regulation::Exchange). Retiring the session's
//! [`WriterFence`] skips a query that was never written, and closes the
//! connection if a written query is still unanswered: no receiver remains for
//! its answer.
//!
//! [`QueryState`] holds the logical side of each live query: what it asked
//! and whether the queue accepted it. [`Queries`] applies its transitions
//! under the one lock that also guards the reservations, so the transport
//! precheck and the claim read one ledger. Only a claim mints an
//! [`AdmittedAnswer`].
//!
//! A claim ends the query's protocol authority, not its resources. The
//! [`ClaimedAnswer`] it returns keeps the query's pool entry and its kind
//! through decode, verification, and import, so a session holds at most one
//! answer of each kind from its query's first byte until that answer settles.
//!
//! A `Hello` holds no reservation and asks nothing of this ledger. The session
//! counts one in verification beside its claimed answers, so a closing session
//! waits for its own verdict on both.

use std::sync::Arc;

use serde::Serialize;
use zakura_rt::{
    collections::HashMap,
    sync::{watch, CancellationToken, Mutex, MutexGuard, PoisonError},
};

use super::{
    protocol::{
        DiscoveryMessage, GetPeers, GetServices, MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES,
    },
    wire::{peers_cap, services_cap, GET_PEERS, GET_SERVICES, LARGEST_PEERS, LARGEST_SERVICES},
};
use crate::zakura::{
    regulation::{
        sizing, ClaimRefused, PoolEntry, ReservationPool, Reservations, ResponseCap,
        ResponsePrecheck, WriterFence,
    },
    wire_codec::{encode_frame, WireMessage},
    FrameRejection, FramedRecv, FramedSend,
};

/// A discovery query, keying its response's reservation.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum QueryKind {
    Peers,
    Services,
}

/// Every query kind. A session holds at most one live query of each.
const QUERY_KINDS: [QueryKind; 2] = [QueryKind::Peers, QueryKind::Services];

impl QueryKind {
    /// The kind that a `message_type` frame answers, if it answers a query.
    fn answered_by(message_type: u16) -> Option<Self> {
        match u8::try_from(message_type) {
            Ok(MSG_DISCOVERY_PEERS) => Some(Self::Peers),
            Ok(MSG_DISCOVERY_SERVICES) => Some(Self::Services),
            _ => None,
        }
    }
}

/// What a live query asked, which bounds its answer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Asked {
    /// A `GetPeers` with this limit.
    Peers { limit: u16 },
    /// A `GetServices`, whose wanted list bounds the answer's summaries.
    Services(GetServices),
}

impl Asked {
    fn kind(&self) -> QueryKind {
        match self {
            Self::Peers { .. } => QueryKind::Peers,
            Self::Services(_) => QueryKind::Services,
        }
    }

    fn request_type(&self) -> u16 {
        match self {
            Self::Peers { .. } => GET_PEERS.message_type,
            Self::Services(_) => GET_SERVICES.message_type,
        }
    }

    /// The largest answer to this query. A reservation admits its row's
    /// largest ending, so the claim checks this tighter cap too.
    fn cap(&self) -> ResponseCap {
        match self {
            Self::Peers { limit } => peers_cap(*limit),
            Self::Services(query) => services_cap(query.wanted_services.len()),
        }
    }

    /// Checks a decoded answer against what the query asked. The decoder
    /// already checked that summaries are unique, so a wanted list also
    /// bounds their count.
    pub(super) fn check(&self, answer: &DiscoveryMessage) -> Result<(), &'static str> {
        match (self, answer) {
            (Self::Peers { limit }, DiscoveryMessage::Peers { records })
                if records.len() > usize::from(*limit) =>
            {
                Err("Zakura discovery PEERS carries more records than its GetPeers limit")
            }
            (Self::Services(query), DiscoveryMessage::Services(services))
                if !services
                    .summaries
                    .iter()
                    .all(|summary| query.wants(&summary.service_id)) =>
            {
                Err("Zakura discovery SERVICES carries a summary its GetServices did not ask for")
            }
            _ => Ok(()),
        }
    }
}

/// The node-wide pool of discovery reservations.
///
/// It holds enough entries for sessions that each reserve the largest `Peers`
/// and the largest `Services` to keep twice the bandwidth-delay product
/// reserved, so it never limits a node that keeps its sessions within the
/// peer limits.
pub(super) fn reservation_pool() -> ReservationPool {
    let session_bytes = LARGEST_PEERS.output_bytes() + LARGEST_SERVICES.output_bytes();
    ReservationPool::new(QUERY_KINDS.len() * sizing::reservation_entries(session_bytes))
        .expect("the derived pool size is nonzero and far below the semaphore maximum")
}

/// Why a query was not sent.
#[derive(Debug)]
pub(super) enum QueryUnsent {
    /// The previous query of this kind is unanswered, or the node-wide pool is
    /// full. The next exchange tries again.
    NotDue,
    /// The session ended.
    Closed,
}

/// How far one live query got.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum QueryPhase {
    /// Reserved, and not yet accepted by the queue. A failed write releases
    /// it.
    Unsent,
    /// The queue accepted it. Only its answer or the session's retirement
    /// ends it.
    Queued,
}

/// One live query.
#[derive(Debug)]
struct LiveQuery {
    asked: Asked,
    phase: QueryPhase,
}

/// A query of this kind is already live.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct QueryLive;

/// An answer that claimed its live query, exactly once.
///
/// Only [`QueryState::claim`] mints one, so holding one proves the claim.
#[derive(Debug)]
pub(super) struct AdmittedAnswer {
    asked: Asked,
}

impl AdmittedAnswer {
    /// The kind of query this answers.
    pub(super) fn kind(&self) -> QueryKind {
        self.asked.kind()
    }

    /// Checks the decoded answer against what its query asked.
    pub(super) fn check(&self, answer: &DiscoveryMessage) -> Result<(), &'static str> {
        self.asked.check(answer)
    }
}

/// What each of one session's live queries asked, and how far it got.
///
/// A session holds at most one live query of each kind. A query is
/// [`QueryPhase::Unsent`] from its reservation until the queue accepts it,
/// then [`QueryPhase::Queued`]. Its answer's claim ends it in either phase,
/// because the peer can answer before the writer learns that the queue
/// accepted the query. Retiring the session drops every live query.
#[derive(Debug, Default)]
pub(super) struct QueryState {
    live: HashMap<QueryKind, LiveQuery>,
}

impl QueryState {
    /// Records `asked` as an unsent live query, before its first byte is
    /// queued.
    pub(super) fn reserve(&mut self, asked: Asked) -> Result<(), QueryLive> {
        let kind = asked.kind();
        if self.live.contains_key(&kind) {
            return Err(QueryLive);
        }
        self.live.insert(
            kind,
            LiveQuery {
                asked,
                phase: QueryPhase::Unsent,
            },
        );
        Ok(())
    }

    /// Records that the queue accepted the live `kind` query. Its answer may
    /// have claimed it already.
    pub(super) fn queued(&mut self, kind: QueryKind) {
        if let Some(query) = self.live.get_mut(&kind) {
            query.phase = QueryPhase::Queued;
        }
    }

    /// Releases the unsent `kind` query. Returns whether one was released:
    /// its answer may have claimed it already.
    pub(super) fn release_unsent(&mut self, kind: QueryKind) -> bool {
        let unsent = self
            .live
            .get(&kind)
            .is_some_and(|query| query.phase == QueryPhase::Unsent);
        if unsent {
            self.live.remove(&kind);
        }
        unsent
    }

    /// Claims the live `kind` query for an answer of `payload_len` bytes.
    /// Returns `None` if no query of `kind` is live.
    ///
    /// The claim ends the query even when the answer is larger than its
    /// query allows, because that answer is a violation.
    pub(super) fn claim(
        &mut self,
        kind: QueryKind,
        payload_len: usize,
    ) -> Option<Result<AdmittedAnswer, ClaimRefused>> {
        let LiveQuery { asked, .. } = self.live.remove(&kind)?;
        let cap = asked.cap().bytes;
        // Widening usize to u64 is lossless on supported targets.
        if payload_len as u64 > cap {
            return Some(Err(ClaimRefused::OverBytes { bytes: cap }));
        }
        Some(Ok(AdmittedAnswer { asked }))
    }

    /// Live queries.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    fn len(&self) -> usize {
        self.live.len()
    }
}

/// What one session delivered and has not settled: its claimed answers of each
/// kind, and each `Hello` in verification.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct Settling {
    peers: usize,
    services: usize,
    hellos: usize,
}

impl Settling {
    fn of(&mut self, held: Held) -> &mut usize {
        match held {
            Held::Answer(QueryKind::Peers) => &mut self.peers,
            Held::Answer(QueryKind::Services) => &mut self.services,
            Held::Hello => &mut self.hellos,
        }
    }

    fn holds(mut self, kind: QueryKind) -> bool {
        *self.of(Held::Answer(kind)) > 0
    }

    fn is_empty(self) -> bool {
        self == Self::default()
    }
}

/// What one [`SettlingHold`] holds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Held {
    /// A claimed answer. It also holds its pool entry beside this hold.
    Answer(QueryKind),
    /// A `Hello` in verification. It holds a closing session's wait and nothing
    /// else: no reservation, no pool entry, and no query kind.
    Hello,
}

/// One answer or `Hello` that a session's reader has not settled.
///
/// The reader holds it through decode, verification, and import. Dropping it
/// settles on every path, including a reader cancelled inside verification.
/// Settlement needs no lock, so a destructor that runs at shutdown cannot
/// contend with a paused lock holder.
#[derive(Debug)]
#[must_use = "dropping the hold settles what it holds"]
pub(super) struct SettlingHold {
    held: Held,
    settling: Arc<watch::Sender<Settling>>,
}

impl SettlingHold {
    fn new(held: Held, settling: &Arc<watch::Sender<Settling>>) -> Self {
        settling.send_modify(|settling| *settling.of(held) += 1);
        Self {
            held,
            settling: settling.clone(),
        }
    }
}

impl Drop for SettlingHold {
    fn drop(&mut self) {
        self.settling
            .send_modify(|settling| *settling.of(self.held) -= 1);
    }
}

/// The runtime owner of one claimed answer: its [`AdmittedAnswer`], the pool
/// entry its query reserved, and its kind's place in the session.
#[derive(Debug)]
#[must_use = "dropping a claimed answer settles it"]
pub(super) struct ClaimedAnswer {
    admitted: AdmittedAnswer,
    /// Frees before `_settling`, so a waiter that settlement wakes finds the
    /// entry free.
    _entry: PoolEntry,
    _settling: SettlingHold,
}

impl ClaimedAnswer {
    /// The evidence that this answer claimed its live query.
    pub(super) fn admitted(&self) -> &AdmittedAnswer {
        &self.admitted
    }
}

/// One session's reservations and its live queries. One lock guards both,
/// so a claim reads what its own query asked.
#[derive(Debug)]
struct LiveQueries {
    reservations: Reservations<QueryKind>,
    state: QueryState,
}

impl LiveQueries {
    fn new() -> Self {
        Self {
            reservations: Reservations::new(DiscoveryMessage::RULES, QUERY_KINDS.len()),
            state: QueryState::default(),
        }
    }
}

/// One session's live queries.
#[derive(Clone, Debug)]
pub(super) struct Queries {
    live: Arc<Mutex<LiveQueries>>,
    /// The session's claimed answers that have not settled.
    settling: Arc<watch::Sender<Settling>>,
    pool: ReservationPool,
    fence: WriterFence,
    send: FramedSend,
    cancel: CancellationToken,
    /// Whether the transport reader charges each queued answer to the
    /// reservations, so the claim releases it.
    transport_admits: bool,
}

impl Queries {
    /// Queries for one session, sent on `send` until `cancel`. The transport
    /// reader of `recv` checks each answer's header before it reads the
    /// payload. Retiring the queries retires `fence`.
    pub(super) fn new(
        pool: ReservationPool,
        fence: WriterFence,
        recv: &FramedRecv,
        send: FramedSend,
        cancel: CancellationToken,
    ) -> Self {
        let live = Arc::new(Mutex::new(LiveQueries::new()));
        // In-process channels have no transport reader. The claim applies the
        // same checks before decode.
        let transport_admits = recv.attach_precheck(Arc::new(QueriesPrecheck(live.clone())));
        Self {
            live,
            settling: Arc::new(watch::Sender::new(Settling::default())),
            pool,
            fence,
            send,
            cancel,
            transport_admits,
        }
    }

    fn lock(&self) -> MutexGuard<'_, LiveQueries> {
        lock(&self.live)
    }

    /// Reserves the answer to `query`, then sends it.
    pub(super) async fn send_get_peers(&self, query: GetPeers) -> Result<(), QueryUnsent> {
        let asked = Asked::Peers { limit: query.limit };
        self.send_query(asked, &DiscoveryMessage::GetPeers(query))
            .await
    }

    /// Reserves the answer to `query`, then sends it.
    pub(super) async fn send_get_services(&self, query: GetServices) -> Result<(), QueryUnsent> {
        let message = DiscoveryMessage::GetServices(query.clone());
        self.send_query(Asked::Services(query), &message).await
    }

    /// Reserves the answer to `message`, which asks `asked`, then sends it.
    /// A query that is never written gives its reservation back.
    ///
    /// A dropped send can leave its reservation live until [`Self::retire`].
    async fn send_query(
        &self,
        asked: Asked,
        message: &DiscoveryMessage,
    ) -> Result<(), QueryUnsent> {
        let frame = encode_frame(message)
            .expect("this node's queries encode; see peer_sample_exclusions_always_encode");
        let exchange = self.fence.open().ok_or(QueryUnsent::Closed)?;
        let writer = exchange.writer();
        let entry = self.pool.try_entry().ok_or(QueryUnsent::NotDue)?;
        let (kind, request_type, cap) = (asked.kind(), asked.request_type(), asked.cap());
        {
            let live = &mut *self.lock();
            // The claim counts its answer under this lock, so this check and
            // the reservation below read one ledger.
            if self.settling.borrow().holds(kind) {
                return Err(QueryUnsent::NotDue);
            }
            live.state
                .reserve(asked)
                .map_err(|QueryLive| QueryUnsent::NotDue)?;
            if live
                .reservations
                .reserve_fenced(kind, request_type, cap, entry, exchange)
                .is_err()
            {
                live.state.release_unsent(kind);
                return Err(QueryUnsent::NotDue);
            }
        }

        let sent = zakura_rt::select! {
            biased;
            () = self.cancel.cancelled() => false,
            sent = self.send.send_fenced(frame, &writer) => sent.is_ok(),
        };
        let live = &mut *self.lock();
        if sent {
            live.state.queued(kind);
            return Ok(());
        }
        if live.state.release_unsent(kind) {
            live.reservations.retract(&kind);
        }
        Err(QueryUnsent::Closed)
    }

    /// Claims the live query that a `message_type` frame of `payload_len`
    /// bytes answers, before its payload is decoded. Returns the claimed
    /// answer, which keeps the query's resources until it drops, or `None` if
    /// the frame answers no query.
    pub(super) fn claim(
        &self,
        message_type: u16,
        payload_len: usize,
    ) -> Result<Option<ClaimedAnswer>, ClaimRefused> {
        let Some(kind) = QueryKind::answered_by(message_type) else {
            return Ok(None);
        };
        self.claim_locked(&mut self.lock(), kind, message_type, payload_len)
            .map(Some)
    }

    /// [`Self::claim`] of a `kind` answer, with the live queries already
    /// locked. The claim takes the queued frame and counts its answer under
    /// this one lock.
    fn claim_locked(
        &self,
        live: &mut LiveQueries,
        kind: QueryKind,
        message_type: u16,
        payload_len: usize,
    ) -> Result<ClaimedAnswer, ClaimRefused> {
        if self.transport_admits {
            live.reservations.take_admitted(message_type, payload_len);
        }
        let (_ended, entry) =
            live.reservations
                .claim_end_holding_entry(&kind, message_type, payload_len)?;
        let admitted = live
            .state
            .claim(kind, payload_len)
            .expect("every live reservation records its live query")?;
        Ok(ClaimedAnswer {
            admitted,
            _entry: entry,
            _settling: SettlingHold::new(Held::Answer(kind), &self.settling),
        })
    }

    /// Counts one `Hello` in verification until the returned hold drops.
    pub(super) fn verifying_hello(&self) -> SettlingHold {
        SettlingHold::new(Held::Hello, &self.settling)
    }

    /// Resolves once this session holds nothing it read and has not processed:
    /// no claimed answer, no `Hello` in verification, and no answer the transport
    /// reader queued. An unanswered query does not hold it, so a silent peer
    /// costs no wait. A `Hello` the reader has not reached holds no reservation,
    /// so it does not hold this either.
    pub(super) async fn answers_settled(&self) {
        let mut settling = self.settling.subscribe();
        loop {
            let settled = {
                let live = self.lock();
                // A claim takes the queued frame and counts its answer under
                // this lock, so the two reads see one ledger.
                settling.borrow_and_update().is_empty() && !live.reservations.has_queued()
            };
            if settled {
                return;
            }
            // The sender lives in `self`, so the wait cannot fail. A
            // settlement or the reader's claim of the queued frame changes
            // the count.
            let _ = settling.changed().await;
        }
    }

    /// Ends the session's queries: an unwritten query is skipped, and an
    /// unanswered written query closes the connection. The reservations
    /// return to the pool, even while the transport reader outlives the
    /// session.
    pub(super) fn retire(&self) {
        self.fence.retire();
        let retired = std::mem::replace(&mut *self.lock(), LiveQueries::new());
        // The exchanges take the fence's lock as they drop.
        drop(retired);
    }
}

/// No holder panics, so the lock is never poisoned.
fn lock(live: &Mutex<LiveQueries>) -> MutexGuard<'_, LiveQueries> {
    live.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The transport reader's header check for answers. It charges each queued
/// answer to the reservations, so the queue holds at most the answers this
/// node is owed.
#[derive(Debug)]
struct QueriesPrecheck(Arc<Mutex<LiveQueries>>);

impl ResponsePrecheck for QueriesPrecheck {
    fn check(&self, message_type: u16, payload_len: usize) -> Result<(), FrameRejection> {
        lock(&self.0)
            .reservations
            .admit(message_type, payload_len)
            .map_err(ClaimRefused::frame_rejection)
    }
}

#[cfg(any(test, feature = "p2p-fuzz"))]
mod model;
#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use model::{
    replay_discovery_query_bytes, replay_discovery_settlement_bytes, DiscoveryQueryReplaySummary,
    DiscoverySettlementReplaySummary,
};
