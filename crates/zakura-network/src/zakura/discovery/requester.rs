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

use std::sync::Arc;

use zakura_rt::{
    collections::HashMap,
    sync::{CancellationToken, Mutex, MutexGuard, PoisonError},
};

use super::{
    protocol::{
        DiscoveryMessage, GetPeers, GetServices, MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES,
    },
    wire::{peers_cap, services_cap, GET_PEERS, GET_SERVICES, LARGEST_PEERS, LARGEST_SERVICES},
};
use crate::zakura::{
    regulation::{
        sizing, ClaimRefused, ReservationPool, Reservations, ResponseCap, ResponsePrecheck,
        WriterFence,
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
    #[cfg(test)]
    fn len(&self) -> usize {
        self.live.len()
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
    /// bytes answers, before its payload is decoded. Returns the admitted
    /// answer, or `None` if the frame answers no query.
    pub(super) fn claim(
        &self,
        message_type: u16,
        payload_len: usize,
    ) -> Result<Option<AdmittedAnswer>, ClaimRefused> {
        let Some(kind) = QueryKind::answered_by(message_type) else {
            return Ok(None);
        };
        let live = &mut *self.lock();
        if self.transport_admits {
            live.reservations.take_admitted(message_type, payload_len);
        }
        live.reservations
            .claim_end(&kind, message_type, payload_len)?;
        live.state
            .claim(kind, payload_len)
            .expect("every live reservation records its live query")
            .map(Some)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{
        discovery::protocol::{
            DiscoveryServiceSummary, ServiceSummaryEnvelope, Services, ZakuraServiceId,
        },
        framed_channel,
        regulation::{PrecheckSlot, UNFINISHED_EXCHANGE},
        CloseCause,
    };

    const PEERS: u16 = MSG_DISCOVERY_PEERS as u16;

    impl Queries {
        /// Claims as [`Queries::claim`] does, and returns what the admitted
        /// answer's query asked.
        fn claim_asked(
            &self,
            message_type: u16,
            payload_len: usize,
        ) -> Result<Option<Asked>, ClaimRefused> {
            self.claim(message_type, payload_len)
                .map(|answer| answer.map(|answer| answer.asked))
        }
    }

    fn get_peers(limit: u16) -> GetPeers {
        GetPeers {
            limit,
            wanted_services: Vec::new(),
            exclude_node_ids: Vec::new(),
        }
    }

    /// The payload length of a `Peers` with `records` maximal records.
    fn peers_len(records: u16) -> usize {
        usize::try_from(peers_cap(records).bytes).expect("a small cap fits usize")
    }

    struct Session {
        queries: Queries,
        /// The peer's end of the stream that carries this node's queries.
        written: FramedRecv,
        connection: CancellationToken,
        close_cause: CloseCause,
        cancel: CancellationToken,
    }

    /// One session's queries, answered on `recv`.
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
    fn services_len(wanted_len: usize) -> usize {
        usize::try_from(services_cap(wanted_len).bytes).expect("a small cap fits usize")
    }

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

    fn query_kind() -> impl proptest::strategy::Strategy<Value = QueryKind> {
        proptest::sample::select(QUERY_KINDS.to_vec())
    }

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
            Asked::Peers { limit } => peers_cap(*limit).bytes,
            Asked::Services(query) => services_cap(query.wanted_services.len()).bytes,
        }
    }

    proptest::proptest! {
        /// After every step the live queries match an independent model: one
        /// live query of each kind, a failed write releases only an unsent
        /// query, and each reservation admits at most one answer, which
        /// carries what its own query asked.
        #[test]
        fn live_queries_match_independent_model(
            ops in proptest::collection::vec(query_op(), 1..96),
        ) {
            let mut state = QueryState::default();
            let mut model: std::collections::HashMap<QueryKind, QueryModel> =
                std::collections::HashMap::new();
            let mut reservations = 0usize;
            let mut admitted = std::collections::HashSet::new();
            for op in ops {
                match op {
                    QueryOp::Reserve { kind, size } => {
                        let asked = asked_for(kind, size);
                        let expected = if model.contains_key(&kind) {
                            Err(QueryLive)
                        } else {
                            reservations += 1;
                            model.insert(kind, QueryModel {
                                asked: asked.clone(),
                                queued: false,
                                reservation: reservations,
                            });
                            Ok(())
                        };
                        proptest::prop_assert_eq!(state.reserve(asked), expected);
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
                        proptest::prop_assert_eq!(state.release_unsent(kind), expected);
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
                            (None, None) => {}
                            (Some(query), Some(Ok(answer))) => {
                                proptest::prop_assert!(payload_len <= model_cap(&query.asked));
                                proptest::prop_assert_eq!(answer.kind(), kind);
                                proptest::prop_assert_eq!(&answer.asked, &query.asked);
                                proptest::prop_assert!(
                                    admitted.insert(query.reservation),
                                    "a reservation admits at most one answer"
                                );
                            }
                            (Some(query), Some(Err(refused))) => {
                                let cap = model_cap(&query.asked);
                                proptest::prop_assert!(payload_len > cap);
                                proptest::prop_assert_eq!(
                                    refused,
                                    ClaimRefused::OverBytes { bytes: cap }
                                );
                            }
                            (expected, claimed) => proptest::prop_assert!(
                                false,
                                "model {expected:?} disagrees with claim {claimed:?}"
                            ),
                        }
                    }
                }

                proptest::prop_assert_eq!(state.len(), model.len());
                proptest::prop_assert!(state.len() <= QUERY_KINDS.len());
                for (kind, query) in &model {
                    let live = state.live.get(kind).expect("the model's live query is live");
                    proptest::prop_assert_eq!(&live.asked, &query.asked);
                    proptest::prop_assert_eq!(live.phase == QueryPhase::Queued, query.queued);
                }
            }
        }
    }
}
