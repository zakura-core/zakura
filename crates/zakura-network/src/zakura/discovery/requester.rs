//! The requester side of discovery's queries: one reservation per query kind.
//!
//! The source reserves a query's response before the query's first byte is
//! written, and the reader claims that reservation before it decodes the
//! response. A response without a live reservation, a second response, and a
//! response above the reserved size are protocol violations, because only the
//! response itself or the session's end removes a reservation.
//!
//! Each reservation owns its query's [`Exchange`]. Retiring the session's
//! [`WriterFence`] skips a query that was never written, and closes the
//! connection if a written query is still unanswered: no receiver remains for
//! its answer.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use tokio_util::sync::CancellationToken;

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
enum QueryKind {
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

/// One session's reservations and what each live query asked. One lock
/// guards both, so a claim reads what its own query asked.
#[derive(Debug)]
struct LiveQueries {
    reservations: Reservations<QueryKind>,
    asked: HashMap<QueryKind, Asked>,
}

impl LiveQueries {
    fn new() -> Self {
        Self {
            reservations: Reservations::new(DiscoveryMessage::RULES, QUERY_KINDS.len()),
            asked: HashMap::new(),
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
        let kind = asked.kind();
        {
            let mut live = self.lock();
            live.reservations
                .reserve_fenced(kind, asked.request_type(), asked.cap(), entry, exchange)
                .map_err(|_| QueryUnsent::NotDue)?;
            live.asked.insert(kind, asked);
        }

        let sent = tokio::select! {
            biased;
            () = self.cancel.cancelled() => false,
            sent = self.send.send_fenced(frame, &writer) => sent.is_ok(),
        };
        if !sent {
            let mut live = self.lock();
            live.reservations.retract(&kind);
            live.asked.remove(&kind);
            return Err(QueryUnsent::Closed);
        }
        Ok(())
    }

    /// Claims the live query that a `message_type` frame of `payload_len`
    /// bytes answers, before its payload is decoded. Returns what the query
    /// asked, or `None` if the frame answers no query.
    pub(super) fn claim(
        &self,
        message_type: u16,
        payload_len: usize,
    ) -> Result<Option<Asked>, ClaimRefused> {
        let Some(kind) = QueryKind::answered_by(message_type) else {
            return Ok(None);
        };
        let mut live = self.lock();
        if self.transport_admits {
            live.reservations.take_admitted(message_type, payload_len);
        }
        live.reservations
            .claim_end(&kind, message_type, payload_len)?;
        let asked = live
            .asked
            .remove(&kind)
            .expect("every live reservation records what its query asked");
        let cap = asked.cap().bytes;
        // Widening usize to u64 is lossless on supported targets.
        if payload_len as u64 > cap {
            return Err(ClaimRefused::OverBytes { bytes: cap });
        }
        Ok(Some(asked))
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
            queries.claim(PEERS, 2),
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
            queries.claim(PEERS, peers_len(3)),
            Ok(Some(Asked::Peers { limit: 3 }))
        );
        assert!(matches!(
            queries.claim(PEERS, 2),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        queries
            .send_get_peers(get_peers(1))
            .await
            .expect("an answered query frees its kind");
        written.recv().await.expect("the query was queued");
        assert!(matches!(
            queries.claim(PEERS, peers_len(1) + 1),
            Err(ClaimRefused::OverBytes { .. })
        ));
        assert_eq!(queries.claim(GET_PEERS.message_type, 2), Ok(None));
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
            queries.claim(SERVICES, services_len(0)),
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
            queries.claim(SERVICES, services_len(1)),
            Ok(Some(Asked::Services(wanted.clone())))
        );
        assert!(matches!(
            queries.claim(SERVICES, services_len(1)),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        assert_eq!(
            queries.claim(PEERS, peers_len(1)),
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
            queries.claim(SERVICES, services_len(1) + 1),
            Err(ClaimRefused::OverBytes { .. })
        ));

        // An empty wanted list reserves the largest answer.
        queries
            .send_get_services(get_services(Vec::new()))
            .await
            .expect("an answered query frees its kind");
        written.recv().await.expect("GetServices was queued");
        assert_eq!(
            queries.claim(SERVICES, services_len(0)),
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
            queries.claim(PEERS, peers_len(1)),
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
        assert!(queries.lock().asked.is_empty());

        let Session {
            queries, written, ..
        } = session(&pool);
        drop(written);
        assert!(matches!(
            queries.send_get_peers(get_peers(1)).await,
            Err(QueryUnsent::Closed)
        ));
        assert_eq!(pool.held(), 0);
        assert!(queries.lock().asked.is_empty());
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
}
