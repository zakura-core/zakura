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
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum QueryKind {
    Peers,
    Services,
}

/// Every query kind. A session holds at most one live query of each.
const QUERY_KINDS: [QueryKind; 2] = [QueryKind::Peers, QueryKind::Services];

impl QueryKind {
    /// The kind's name in trace rows.
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Peers => "peers",
            Self::Services => "services",
        }
    }

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
    fn of(&mut self, kind: QueryKind) -> &mut usize {
        match kind {
            QueryKind::Peers => &mut self.peers,
            QueryKind::Services => &mut self.services,
        }
    }

    fn holds(mut self, kind: QueryKind) -> bool {
        *self.of(kind) > 0
    }

    fn is_empty(self) -> bool {
        self == Self::default()
    }
}

/// One answer that a session has claimed and not settled.
///
/// The reader holds it through decode, verification, and import. Dropping it
/// settles the answer on every path, including a reader cancelled inside
/// verification. Settlement needs no lock, so a destructor that runs at
/// shutdown cannot contend with a paused lock holder.
#[derive(Debug)]
struct SettlingAnswer {
    kind: QueryKind,
    settling: Arc<watch::Sender<Settling>>,
}

impl SettlingAnswer {
    fn new(kind: QueryKind, settling: &Arc<watch::Sender<Settling>>) -> Self {
        settling.send_modify(|settling| *settling.of(kind) += 1);
        Self {
            kind,
            settling: settling.clone(),
        }
    }
}

impl Drop for SettlingAnswer {
    fn drop(&mut self) {
        self.settling
            .send_modify(|settling| *settling.of(self.kind) -= 1);
    }
}

/// One `Hello` that a session's reader is verifying.
///
/// It holds a closing session's wait and nothing else: no reservation, no pool
/// entry, and no query kind. Dropping it settles the `Hello` on every path.
#[derive(Debug)]
#[must_use = "dropping the hold settles the Hello"]
pub(super) struct SettlingHello {
    settling: Arc<watch::Sender<Settling>>,
}

impl Drop for SettlingHello {
    fn drop(&mut self) {
        self.settling.send_modify(|settling| settling.hellos -= 1);
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
    _settling: SettlingAnswer,
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
        let live = &mut *self.lock();
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
        Ok(Some(ClaimedAnswer {
            admitted,
            _entry: entry,
            _settling: SettlingAnswer::new(kind, &self.settling),
        }))
    }

    /// Counts one `Hello` in verification until the returned hold drops.
    pub(super) fn verifying_hello(&self) -> SettlingHello {
        self.settling.send_modify(|settling| settling.hellos += 1);
        SettlingHello {
            settling: self.settling.clone(),
        }
    }

    /// Resolves once this session holds nothing it read and has not processed:
    /// no claimed answer, no `Hello` in verification, and no answer the transport
    /// reader queued. An unanswered query does not hold it, so a silent peer
    /// costs no wait. A `Hello` the reader has not reached holds no reservation,
    /// so it does not hold this either.
    pub(super) async fn answers_settled(&self) {
        let mut settling = self.settling.subscribe();
        loop {
            // The sender lives in `self`, so the waits cannot fail.
            let _ = settling.wait_for(|settling| settling.is_empty()).await;
            if !self.lock().reservations.has_queued() {
                return;
            }
            // The reader's claim of the queued frame changes the count.
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
pub use tests::{replay_discovery_query_bytes, DiscoveryQueryReplaySummary};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod tests {
    use std::collections::{hash_map::Entry, HashMap, HashSet};

    use super::*;
    use crate::zakura::discovery::protocol::ZakuraServiceId;
    #[cfg(test)]
    use crate::zakura::{
        discovery::protocol::{DiscoveryServiceSummary, ServiceSummaryEnvelope, Services},
        framed_channel,
        regulation::{PrecheckSlot, UNFINISHED_EXCHANGE},
        CloseCause,
    };

    #[cfg(test)]
    const PEERS: u16 = MSG_DISCOVERY_PEERS as u16;

    #[cfg(test)]
    impl Queries {
        /// Claims as [`Queries::claim`] does, and returns what the admitted
        /// answer's query asked.
        fn claim_asked(
            &self,
            message_type: u16,
            payload_len: usize,
        ) -> Result<Option<Asked>, ClaimRefused> {
            self.claim(message_type, payload_len)
                .map(|answer| answer.map(|answer| answer.admitted.asked.clone()))
        }
    }

    #[cfg(test)]
    fn get_peers(limit: u16) -> GetPeers {
        GetPeers {
            limit,
            wanted_services: Vec::new(),
            exclude_node_ids: Vec::new(),
        }
    }

    /// The payload length of a `Peers` with `records` maximal records.
    #[cfg(test)]
    fn peers_len(records: u16) -> usize {
        usize::try_from(peers_cap(records).bytes).expect("a small cap fits usize")
    }

    #[cfg(test)]
    struct Session {
        queries: Queries,
        /// The peer's end of the stream that carries this node's queries.
        written: FramedRecv,
        connection: CancellationToken,
        close_cause: CloseCause,
        cancel: CancellationToken,
    }

    /// One session's queries, answered on `recv`.
    #[cfg(test)]
    fn session_with(pool: ReservationPool, recv: &FramedRecv) -> Session {
        let (send, written) = framed_channel(4);
        let connection = CancellationToken::new();
        let close_cause = CloseCause::new();
        let cancel = CancellationToken::new();
        let fence = WriterFence::new(connection.clone(), close_cause.clone());
        Session {
            queries: Queries::new(pool, fence, recv, send, cancel.clone()),
            written,
            connection,
            close_cause,
            cancel,
        }
    }

    /// One session's queries on an in-process channel, which has no
    /// transport reader.
    #[cfg(test)]
    fn session(pool: &ReservationPool) -> Session {
        let (_send, recv) = framed_channel(1);
        session_with(pool.clone(), &recv)
    }

    /// A query is sent only once its previous answer arrives, and each
    /// answer claims its reservation exactly once.
    #[tokio::test]
    async fn one_get_peers_is_live_and_its_answer_claims_it_once() {
        let Session {
            queries,
            mut written,
            ..
        } = session(&reservation_pool());

        assert!(matches!(
            queries.claim_asked(PEERS, 2),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        queries
            .send_get_peers(get_peers(3))
            .await
            .expect("the first query is sent");
        assert!(matches!(
            queries.send_get_peers(get_peers(3)).await,
            Err(QueryUnsent::NotDue)
        ));
        written.recv().await.expect("the query was queued");

        assert_eq!(
            queries.claim_asked(PEERS, peers_len(3)),
            Ok(Some(Asked::Peers { limit: 3 }))
        );
        assert!(matches!(
            queries.claim_asked(PEERS, 2),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("an answered query frees its kind");
        written.recv().await.expect("the query was queued");
        assert!(matches!(
            queries.claim_asked(PEERS, peers_len(1) + 1),
            Err(ClaimRefused::OverBytes { .. })
        ));
        assert_eq!(queries.claim_asked(GET_PEERS.message_type, 2), Ok(None));
    }

    fn get_services(wanted_services: Vec<ZakuraServiceId>) -> GetServices {
        GetServices { wanted_services }
    }

    /// The payload length of a `Services` with the most summaries that a
    /// `GetServices` wanting `wanted_len` services allows.
    #[cfg(test)]
    fn services_len(wanted_len: usize) -> usize {
        usize::try_from(services_cap(wanted_len).bytes).expect("a small cap fits usize")
    }

    #[cfg(test)]
    const SERVICES: u16 = MSG_DISCOVERY_SERVICES as u16;

    /// A `GetServices` reservation is independent of the `GetPeers` one. Its
    /// answer claims it once, returns the query, and stays within
    /// `42 + min(n, 8) × 294` bytes.
    #[tokio::test]
    async fn get_services_is_reserved_beside_get_peers_and_claimed_once() {
        let Session {
            queries,
            mut written,
            ..
        } = session(&reservation_pool());
        let wanted = get_services(vec![ZakuraServiceId::discovery()]);

        assert!(matches!(
            queries.claim_asked(SERVICES, services_len(0)),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("GetPeers is sent");
        queries
            .send_get_services(wanted.clone())
            .await
            .expect("GetServices is sent beside GetPeers");
        assert!(matches!(
            queries.send_get_services(get_services(Vec::new())).await,
            Err(QueryUnsent::NotDue)
        ));
        written.recv().await.expect("GetPeers was queued");
        written.recv().await.expect("GetServices was queued");

        assert_eq!(
            queries.claim_asked(SERVICES, services_len(1)),
            Ok(Some(Asked::Services(wanted.clone())))
        );
        assert!(matches!(
            queries.claim_asked(SERVICES, services_len(1)),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        assert_eq!(
            queries.claim_asked(PEERS, peers_len(1)),
            Ok(Some(Asked::Peers { limit: 1 }))
        );

        // One wanted service allows one summary, below the row's largest
        // answer.
        queries
            .send_get_services(wanted)
            .await
            .expect("an answered query frees its kind");
        written.recv().await.expect("GetServices was queued");
        assert!(matches!(
            queries.claim_asked(SERVICES, services_len(1) + 1),
            Err(ClaimRefused::OverBytes { .. })
        ));

        // An empty wanted list reserves the largest answer.
        queries
            .send_get_services(get_services(Vec::new()))
            .await
            .expect("an answered query frees its kind");
        written.recv().await.expect("GetServices was queued");
        assert_eq!(
            queries.claim_asked(SERVICES, services_len(0)),
            Ok(Some(Asked::Services(get_services(Vec::new()))))
        );
    }

    /// A wanted list bounds the answer's summaries, and an empty list allows
    /// any service.
    #[test]
    fn a_services_answer_holds_only_wanted_summaries() -> Result<(), crate::BoxError> {
        let summary = ServiceSummaryEnvelope::discovery(&DiscoveryServiceSummary {
            peer_exchange_slots_free: 1,
            max_records_per_response: 32,
            expected_disconnect_after_exchange: false,
        })?;
        let services = |summaries| {
            DiscoveryMessage::Services(Services {
                node_id: zakura_quic::NodeSecretKey::from_bytes(&[1; 32]).public(),
                expires_at_unix_secs: u64::MAX,
                summaries,
            })
        };
        let asked = |wanted| Asked::Services(get_services(wanted));

        let discovery = vec![ZakuraServiceId::discovery()];
        let header_sync = vec![ZakuraServiceId::header_sync()];
        assert_eq!(
            asked(Vec::new()).check(&services(vec![summary.clone()])),
            Ok(())
        );
        assert_eq!(
            asked(discovery).check(&services(vec![summary.clone()])),
            Ok(())
        );
        assert!(asked(header_sync.clone())
            .check(&services(vec![summary]))
            .is_err());
        assert_eq!(asked(header_sync).check(&services(Vec::new())), Ok(()));
        Ok(())
    }

    /// The transport reader queues a `Services` only while a `GetServices`
    /// is owed, and refuses one above its reservation. A live `GetPeers`
    /// lends it no credit.
    #[tokio::test]
    async fn the_transport_reader_queues_only_owed_services() {
        let slot = PrecheckSlot::default();
        let (_send, recv) = framed_channel(1);
        let recv = recv.with_precheck(slot.clone());
        let Session {
            queries,
            mut written,
            ..
        } = session_with(reservation_pool(), &recv);
        let precheck = slot.get().expect("the queries attach their precheck");

        assert_eq!(
            precheck.check(SERVICES, services_len(0)),
            Err(FrameRejection::Unsolicited)
        );
        queries
            .send_get_peers(get_peers(32))
            .await
            .expect("GetPeers is sent");
        queries
            .send_get_services(get_services(Vec::new()))
            .await
            .expect("the query is sent");
        written.recv().await.expect("GetPeers was queued");
        written.recv().await.expect("the query was queued");
        assert!(matches!(
            precheck.check(SERVICES, services_len(0) + 1),
            Err(FrameRejection::AboveReservation { .. })
        ));
        precheck
            .check(SERVICES, services_len(0))
            .expect("the owed answer queues");
        assert!(matches!(
            precheck.check(SERVICES, services_len(0)),
            Err(FrameRejection::AboveOwed { .. })
        ));
    }

    /// The transport reader queues at most the one `Peers` this node is owed,
    /// even while a `GetServices` is owed too. Its claim releases the queued
    /// answer.
    #[tokio::test]
    async fn the_transport_reader_queues_at_most_the_owed_peers() {
        let slot = PrecheckSlot::default();
        let (_send, recv) = framed_channel(1);
        let recv = recv.with_precheck(slot.clone());
        let Session {
            queries,
            mut written,
            ..
        } = session_with(reservation_pool(), &recv);
        let precheck = slot.get().expect("the queries attach their precheck");

        assert_eq!(
            precheck.check(PEERS, peers_len(1)),
            Err(FrameRejection::Unsolicited)
        );
        queries
            .send_get_services(get_services(Vec::new()))
            .await
            .expect("GetServices is sent");
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("the query is sent");
        written.recv().await.expect("GetServices was queued");
        written.recv().await.expect("the query was queued");

        precheck
            .check(PEERS, peers_len(1))
            .expect("the owed answer queues");
        assert!(matches!(
            precheck.check(PEERS, peers_len(1)),
            Err(FrameRejection::AboveOwed { .. })
        ));
        assert_eq!(
            queries.claim_asked(PEERS, peers_len(1)),
            Ok(Some(Asked::Peers { limit: 1 }))
        );
        assert_eq!(
            precheck.check(PEERS, peers_len(1)),
            Err(FrameRejection::Unsolicited)
        );

        // The claim released the queued answer, so the next answer queues.
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("an answered query frees its kind");
        written.recv().await.expect("the query was queued");
        precheck
            .check(PEERS, peers_len(1))
            .expect("the next owed answer queues");
    }

    /// A query that is never written gives its reservation and pool entry
    /// back, so the next exchange can send one.
    #[tokio::test]
    async fn an_unwritten_query_gives_its_reservation_back() {
        let pool = reservation_pool();

        let Session {
            queries, cancel, ..
        } = session(&pool);
        cancel.cancel();
        assert!(matches!(
            queries.send_get_peers(get_peers(1)).await,
            Err(QueryUnsent::Closed)
        ));
        assert_eq!(pool.held(), 0);
        assert_eq!(queries.lock().state.len(), 0);

        let Session {
            queries, written, ..
        } = session(&pool);
        drop(written);
        assert!(matches!(
            queries.send_get_peers(get_peers(1)).await,
            Err(QueryUnsent::Closed)
        ));
        assert_eq!(pool.held(), 0);
        assert_eq!(queries.lock().state.len(), 0);
    }

    /// A full node-wide pool defers the query to the next exchange.
    #[tokio::test]
    async fn a_full_pool_defers_the_query() {
        let pool = ReservationPool::new(1).expect("one entry is a valid pool");
        let _held = pool.try_entry().expect("the pool has one entry");
        let Session { queries, .. } = session(&pool);
        assert!(matches!(
            queries.send_get_peers(get_peers(1)).await,
            Err(QueryUnsent::NotDue)
        ));
    }

    /// Retiring the queries closes the connection only when a written query
    /// is unanswered, and returns every reservation to the pool.
    #[tokio::test]
    async fn retiring_closes_the_connection_only_for_an_unanswered_written_query() {
        let pool = reservation_pool();

        // Queued but never written: the transport skips it.
        let Session {
            queries,
            mut written,
            connection,
            ..
        } = session(&pool);
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("the query is queued");
        queries.retire();
        assert!(!connection.is_cancelled());
        assert!(
            written.try_recv().is_err(),
            "a fenced query is never written"
        );
        assert_eq!(pool.held(), 0);

        // Written and answered.
        let Session {
            queries,
            mut written,
            connection,
            ..
        } = session(&pool);
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("the query is queued");
        written.recv().await.expect("the query is written");
        queries
            .claim(PEERS, 2)
            .expect("the answer claims the query");
        queries.retire();
        assert!(!connection.is_cancelled());

        // Written and unanswered.
        let Session {
            queries,
            mut written,
            connection,
            close_cause,
            ..
        } = session(&pool);
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("the query is queued");
        written.recv().await.expect("the query is written");
        queries.retire();
        assert!(connection.is_cancelled());
        assert_eq!(close_cause.get_or("none"), UNFINISHED_EXCHANGE);
        assert_eq!(pool.held(), 0, "retiring returns every reservation");
    }

    /// One step of a live-query operation sequence.
    #[derive(Clone, Debug)]
    enum QueryOp {
        /// Reserve a query of `kind`, sized by `size`.
        Reserve {
            kind: QueryKind,
            size: usize,
        },
        Queued(QueryKind),
        ReleaseUnsent(QueryKind),
        /// Claim `kind` with an answer `excess` bytes above its query's cap,
        /// or empty when `excess` is `None`.
        Claim {
            kind: QueryKind,
            excess: Option<u64>,
        },
    }

    /// A claim ends the query's protocol authority. The answer keeps the pool
    /// entry and its kind until it settles, so a refresh cannot stack a second
    /// answer behind one that waits for verification.
    #[tokio::test]
    async fn a_claimed_answer_holds_its_entry_and_kind_until_it_settles() {
        let pool = reservation_pool();
        let Session {
            queries,
            mut written,
            ..
        } = session(&pool);
        queries.send_get_peers(get_peers(1)).await.unwrap();
        written.recv().await.expect("the query was queued");

        let answer = queries
            .claim(PEERS, peers_len(1))
            .expect("the answer fits its query")
            .expect("the frame answers a query");
        assert_eq!(pool.held(), 1, "the claimed answer keeps its pool entry");
        assert!(matches!(
            queries.claim(PEERS, 2),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        assert!(
            matches!(
                queries.send_get_peers(get_peers(1)).await,
                Err(QueryUnsent::NotDue)
            ),
            "an unsettled answer keeps its kind"
        );
        queries
            .send_get_services(get_services(Vec::new()))
            .await
            .expect("the other kind stays independent");
        assert_eq!(pool.held(), 2);

        drop(answer);
        assert_eq!(pool.held(), 1, "settlement frees exactly its own entry");
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("settlement frees the kind");
    }

    /// A `Hello` in verification holds the settled wait, and it takes no pool
    /// entry and blocks no query.
    #[tokio::test]
    async fn a_hello_in_verification_holds_only_the_settled_wait() {
        let pool = reservation_pool();
        let Session {
            queries,
            written: _written,
            ..
        } = session(&pool);

        let hello = queries.verifying_hello();
        assert_eq!(pool.held(), 0);
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("a Hello in verification blocks no query");
        let waiting = queries.clone();
        let settled = tokio::spawn(async move { waiting.answers_settled().await });
        tokio::task::yield_now().await;
        assert!(!settled.is_finished(), "the Hello holds the wait");
        drop(hello);
        zakura_rt::time::timeout(std::time::Duration::from_secs(5), settled)
            .await
            .expect("settlement wakes the wait")
            .unwrap();
    }

    /// `answers_settled` waits for a claimed answer and ignores a query the
    /// peer has not answered.
    #[tokio::test]
    async fn answers_settled_waits_for_a_claimed_answer_only() {
        let Session {
            queries,
            written: _written,
            ..
        } = session(&reservation_pool());
        queries.send_get_peers(get_peers(1)).await.unwrap();
        queries
            .send_get_services(get_services(Vec::new()))
            .await
            .unwrap();
        zakura_rt::time::timeout(std::time::Duration::from_secs(5), queries.answers_settled())
            .await
            .expect("an unanswered query holds nothing");

        let answer = queries.claim(PEERS, peers_len(1)).unwrap().unwrap();
        let waiting = queries.clone();
        let settled = tokio::spawn(async move { waiting.answers_settled().await });
        tokio::task::yield_now().await;
        assert!(!settled.is_finished(), "the claimed answer holds the wait");
        drop(answer);
        zakura_rt::time::timeout(std::time::Duration::from_secs(5), settled)
            .await
            .expect("settlement wakes the wait")
            .unwrap();
    }

    /// A queued answer holds the wait until its claim and settlement, on the
    /// real transport precheck.
    #[tokio::test]
    async fn answers_settled_waits_for_a_frame_the_transport_queued() {
        let slot = PrecheckSlot::default();
        let (_peer, recv) = framed_channel(1);
        let recv = recv.with_precheck(slot.clone());
        let Session {
            queries,
            written: _written,
            ..
        } = session_with(reservation_pool(), &recv);
        queries.send_get_peers(get_peers(1)).await.unwrap();
        slot.get()
            .expect("the session attached its precheck")
            .check(PEERS, peers_len(1))
            .expect("the transport admits the owed answer");

        let waiting = queries.clone();
        let settled = tokio::spawn(async move { waiting.answers_settled().await });
        tokio::task::yield_now().await;
        assert!(!settled.is_finished(), "the queued answer holds the wait");
        let answer = queries.claim(PEERS, peers_len(1)).unwrap().unwrap();
        tokio::task::yield_now().await;
        assert!(!settled.is_finished(), "the claimed answer holds the wait");
        drop(answer);
        zakura_rt::time::timeout(std::time::Duration::from_secs(5), settled)
            .await
            .expect("settlement wakes the wait")
            .unwrap();
    }

    #[cfg(test)]
    fn query_kind() -> impl proptest::strategy::Strategy<Value = QueryKind> {
        proptest::sample::select(QUERY_KINDS.to_vec())
    }

    #[cfg(test)]
    fn query_op() -> impl proptest::strategy::Strategy<Value = QueryOp> {
        use proptest::prelude::*;
        prop_oneof![
            3 => (query_kind(), 0usize..4).prop_map(|(kind, size)| QueryOp::Reserve { kind, size }),
            2 => query_kind().prop_map(QueryOp::Queued),
            1 => query_kind().prop_map(QueryOp::ReleaseUnsent),
            3 => (query_kind(), proptest::option::of(0u64..2))
                .prop_map(|(kind, excess)| QueryOp::Claim { kind, excess }),
        ]
    }

    /// The query a `Reserve` step asks. `size` is a `GetPeers` limit or the
    /// length of a `GetServices` wanted list.
    fn asked_for(kind: QueryKind, size: usize) -> Asked {
        let wanted = [
            ZakuraServiceId::discovery(),
            ZakuraServiceId::header_sync(),
            ZakuraServiceId::block_sync(),
        ];
        match kind {
            QueryKind::Peers => Asked::Peers {
                limit: u16::try_from(size).expect("test sizes are small"),
            },
            QueryKind::Services => Asked::Services(get_services(wanted[..size.min(3)].to_vec())),
        }
    }

    /// The model's view of one live query.
    #[derive(Clone, Debug)]
    struct QueryModel {
        asked: Asked,
        queued: bool,
        /// Distinguishes this reservation from earlier ones of its kind.
        reservation: usize,
    }

    /// The largest answer the model allows for `asked`, restated from the
    /// wire caps.
    fn model_cap(asked: &Asked) -> u64 {
        match asked {
            Asked::Peers { limit } => 2 + u64::from(*limit) * 648,
            Asked::Services(query) => {
                let wanted = query.wanted_services.len();
                let count = if wanted == 0 { 8 } else { wanted.min(8) };
                42 + u64::try_from(count).expect("eight summaries fit u64") * 294
            }
        }
    }

    const PROPERTY: &str = "discovery/query-ownership";

    /// Applicable operations in a decoder-v1 query history.
    #[derive(Debug, Default, Eq, PartialEq)]
    pub struct DiscoveryQueryReplaySummary {
        /// Operations checked against the model.
        pub transitions: usize,
        /// Fresh reservations.
        pub reserved: usize,
        /// Reservations refused because the kind is live.
        pub refused: usize,
        /// Unsent reservations released.
        pub released: usize,
        /// Answers admitted by their own reservation.
        pub admitted: usize,
        /// Answers rejected above their reservation's cap.
        pub oversized: usize,
        /// Claims without a live reservation.
        pub stale: usize,
    }

    /// Replay decoder-v1 bytes against the query ownership model.
    /// Each byte chooses a kind, operation and bounded size. Read at most 512 bytes.
    pub fn replay_discovery_query_bytes(data: &[u8]) -> DiscoveryQueryReplaySummary {
        run(decode(data), |_| {})
    }

    fn decode(data: &[u8]) -> impl Iterator<Item = QueryOp> + '_ {
        data.iter().take(512).map(|byte| {
            let kind = if byte & 1 == 0 {
                QueryKind::Peers
            } else {
                QueryKind::Services
            };
            match (byte >> 1) % 4 {
                0 => QueryOp::Reserve {
                    kind,
                    size: usize::from(byte >> 3) % 4,
                },
                1 => QueryOp::Queued(kind),
                2 => QueryOp::ReleaseUnsent(kind),
                _ => QueryOp::Claim {
                    kind,
                    excess: match (byte >> 3) % 3 {
                        0 => None,
                        1 => Some(0),
                        _ => Some(1),
                    },
                },
            }
        })
    }

    fn run(
        ops: impl IntoIterator<Item = QueryOp>,
        mut after_step: impl FnMut(&mut QueryState),
    ) -> DiscoveryQueryReplaySummary {
        let mut state = QueryState::default();
        let mut model: HashMap<QueryKind, QueryModel> = HashMap::new();
        let mut reservations = 0usize;
        let mut admitted = HashSet::new();
        let mut summary = DiscoveryQueryReplaySummary::default();
        for op in ops {
            summary.transitions += 1;
            match op {
                QueryOp::Reserve { kind, size } => {
                    let asked = asked_for(kind, size);
                    let expected = match model.entry(kind) {
                        Entry::Occupied(_) => Err(QueryLive),
                        Entry::Vacant(slot) => {
                            reservations += 1;
                            slot.insert(QueryModel {
                                asked: asked.clone(),
                                queued: false,
                                reservation: reservations,
                            });
                            Ok(())
                        }
                    };
                    summary.reserved += usize::from(expected.is_ok());
                    summary.refused += usize::from(expected.is_err());
                    assert_eq!(state.reserve(asked), expected, "{PROPERTY}");
                }
                QueryOp::Queued(kind) => {
                    state.queued(kind);
                    if let Some(query) = model.get_mut(&kind) {
                        query.queued = true;
                    }
                }
                QueryOp::ReleaseUnsent(kind) => {
                    let expected = model.get(&kind).is_some_and(|query| !query.queued);
                    if expected {
                        model.remove(&kind);
                    }
                    summary.released += usize::from(expected);
                    assert_eq!(state.release_unsent(kind), expected, "{PROPERTY}");
                }
                QueryOp::Claim { kind, excess } => {
                    let expected = model.remove(&kind);
                    let payload_len = match (&expected, excess) {
                        (Some(query), Some(excess)) => model_cap(&query.asked) + excess,
                        _ => 0,
                    };
                    let claimed = state.claim(
                        kind,
                        usize::try_from(payload_len).expect("test caps fit usize"),
                    );
                    match (expected, claimed) {
                        (None, None) => {
                            summary.stale += 1;
                        }
                        (Some(query), Some(Ok(answer))) => {
                            summary.admitted += 1;
                            assert!(payload_len <= model_cap(&query.asked), "{PROPERTY}");
                            assert_eq!(answer.kind(), kind, "{PROPERTY}");
                            assert_eq!(&answer.asked, &query.asked, "{PROPERTY}");
                            assert!(
                                admitted.insert(query.reservation),
                                "{PROPERTY}: duplicate admission"
                            );
                        }
                        (Some(query), Some(Err(refused))) => {
                            summary.oversized += 1;
                            let cap = model_cap(&query.asked);
                            assert!(payload_len > cap, "{PROPERTY}");
                            assert_eq!(
                                refused,
                                ClaimRefused::OverBytes { bytes: cap },
                                "{PROPERTY}"
                            );
                        }
                        (expected, claimed) => panic!(
                            "{PROPERTY}: model {expected:?} disagrees with claim {claimed:?}"
                        ),
                    }
                }
            }

            after_step(&mut state);
            assert_eq!(state.len(), model.len(), "{PROPERTY}");
            assert!(state.len() <= QUERY_KINDS.len(), "{PROPERTY}");
            for (kind, query) in &model {
                let live = state
                    .live
                    .get(kind)
                    .expect("the model's live query is live");
                assert_eq!(&live.asked, &query.asked, "{PROPERTY}");
                assert_eq!(live.phase == QueryPhase::Queued, query.queued, "{PROPERTY}");
            }
        }
        summary
    }

    #[cfg(test)]
    proptest::proptest! {
        #[test]
        fn live_queries_match_independent_model(ops in proptest::collection::vec(query_op(), 1..96)) {
            run(ops, |_| {});
        }

        #[test]
        fn query_bytes_match_the_same_model(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..=512)) {
            replay_discovery_query_bytes(&bytes);
        }
    }

    #[test]
    #[allow(
        clippy::print_stdout,
        reason = "CI retains these applicability counters"
    )]
    fn query_witness_exercises_ownership_and_caps() {
        let summary = replay_discovery_query_bytes(&[8, 24, 2, 4, 14, 6, 1, 23, 0, 4]);
        assert_eq!(
            summary,
            DiscoveryQueryReplaySummary {
                transitions: 10,
                reserved: 3,
                refused: 1,
                released: 1,
                admitted: 1,
                oversized: 1,
                stale: 1,
            }
        );
        assert_eq!(
            replay_discovery_query_bytes(&[]),
            DiscoveryQueryReplaySummary::default()
        );
        println!("tier=Machine target=discovery_query decoder=1 {summary:?}");
    }

    #[test]
    #[should_panic(expected = "discovery/query-ownership")]
    fn query_witness_rejects_losing_a_reservation() {
        run(decode(&[0]), |state| {
            state.live.clear();
        });
    }

    #[test]
    #[should_panic(expected = "discovery/query-ownership")]
    fn query_witness_rejects_releasing_a_queued_query() {
        run(decode(&[0, 2]), |state| {
            state
                .live
                .retain(|_, query| query.phase != QueryPhase::Queued);
        });
    }
}
