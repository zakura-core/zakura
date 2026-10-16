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

use std::sync::{Arc, Mutex, PoisonError};

use tokio_util::sync::CancellationToken;

use super::{
    protocol::{
        DiscoveryMessage, ZakuraServiceId, MAX_DISCOVERY_RECORDS_PER_RESPONSE, MSG_DISCOVERY_PEERS,
        MSG_DISCOVERY_SERVICES,
    },
    wire::{peers_cap, services_cap, GET_PEERS, GET_SERVICES},
};
use crate::zakura::{
    regulation::{
        sizing, ClaimRefused, FencedSendError, ReservationPool, Reservations, SharedReservations,
        WriterFence,
    },
    wire_codec::{encode_frame, WireMessage},
    CloseCause, FramedRecv, FramedSend,
};

/// A discovery query, keying its response's reservation.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum QueryKind {
    Peers,
    Services,
}

/// The node-wide pool of discovery reservations.
///
/// It holds enough entries for sessions that each reserve the largest `Peers`
/// and the largest `Services` to keep twice the bandwidth-delay product
/// reserved, so it never limits a node that keeps its sessions within the
/// peer limits.
pub(super) fn reservation_pool() -> ReservationPool {
    let largest_peers = peers_cap(
        u16::try_from(MAX_DISCOVERY_RECORDS_PER_RESPONSE).expect("the largest limit fits u16"),
    );
    let session_bytes = largest_peers.output_bytes() + services_cap(0).output_bytes();
    ReservationPool::new(2 * sizing::reservation_entries(session_bytes))
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

/// What a live query asked for, which bounds its answer.
#[derive(Debug, Default)]
struct Snapshots {
    /// The `GetPeers` limit, which bounds the record count of `Peers`.
    peers_limit: u16,
    /// The `GetServices` wanted list. A nonempty list bounds the service IDs
    /// of `Services`.
    wanted_services: Vec<ZakuraServiceId>,
}

/// One session's live queries.
#[derive(Clone, Debug)]
pub(super) struct Queries {
    reservations: SharedReservations<QueryKind>,
    snapshots: Arc<Mutex<Snapshots>>,
    pool: ReservationPool,
    fence: WriterFence,
}

impl Queries {
    /// Queries for one session. Retiring them closes `connection` if a
    /// written query is unanswered.
    pub(super) fn new(
        pool: ReservationPool,
        connection: CancellationToken,
        close_cause: CloseCause,
    ) -> Self {
        Self {
            reservations: SharedReservations::new(Reservations::new(DiscoveryMessage::RULES, 2)),
            snapshots: Arc::default(),
            pool,
            fence: WriterFence::new(connection, close_cause),
        }
    }

    /// Checks each response header on `recv`'s transport reader before its
    /// payload is read. In-process channels have no transport reader; the
    /// reader's claim applies the same checks before decode.
    pub(super) fn attach_precheck(&self, recv: &FramedRecv) {
        recv.attach_precheck(Arc::new(self.reservations.clone()));
    }

    /// Reserves the answer to `message`, a `GetPeers` or a `GetServices`, then
    /// sends the request on `send`. A request that is never written gives its
    /// reservation back.
    pub(super) async fn send_query(
        &self,
        send: &FramedSend,
        cancel: &CancellationToken,
        message: DiscoveryMessage,
    ) -> Result<(), QueryUnsent> {
        let (kind, request_type, cap) = match &message {
            DiscoveryMessage::GetPeers { limit, .. } => {
                (QueryKind::Peers, GET_PEERS.message_type, peers_cap(*limit))
            }
            DiscoveryMessage::GetServices(query) => (
                QueryKind::Services,
                GET_SERVICES.message_type,
                services_cap(query.wanted_services.len()),
            ),
            _ => unreachable!("send_query sends only GetPeers and GetServices"),
        };
        let frame = encode_frame(&message).map_err(|error| {
            tracing::debug!(?error, ?kind, "failed to encode Zakura discovery query");
            QueryUnsent::NotDue
        })?;
        let exchange = self.fence.open().ok_or(QueryUnsent::Closed)?;
        let writer = exchange.writer();
        let entry = self.pool.try_entry().ok_or(QueryUnsent::NotDue)?;
        self.reservations
            .lock()
            .reserve_fenced(kind, request_type, cap, entry, exchange)
            .map_err(|_| QueryUnsent::NotDue)?;
        {
            let mut snapshots = self
                .snapshots
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match message {
                DiscoveryMessage::GetPeers { limit, .. } => snapshots.peers_limit = limit,
                DiscoveryMessage::GetServices(query) => {
                    snapshots.wanted_services = query.wanted_services;
                }
                _ => unreachable!("the match above admits only GetPeers and GetServices"),
            }
        }

        let sent = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(FencedSendError::Closed),
            sent = send.send_fenced(frame, &writer) => sent,
        };
        match sent {
            Ok(()) => Ok(()),
            Err(error) => {
                self.reservations.lock().retract(&kind);
                Err(match error {
                    FencedSendError::Fenced | FencedSendError::Closed => QueryUnsent::Closed,
                })
            }
        }
    }

    /// Claims the live `GetPeers` reservation for a `Peers` of `payload_len`
    /// bytes, before its payload is decoded. Returns the request's limit.
    ///
    /// The reservation admits the row's largest ending, as every reservation
    /// does, so the claim also checks the request's own cap,
    /// `2 + limit × 648` bytes.
    pub(super) fn claim_peers(&self, payload_len: usize) -> Result<u16, ClaimRefused> {
        self.reservations.lock().claim_end(
            &QueryKind::Peers,
            u16::from(MSG_DISCOVERY_PEERS),
            payload_len,
        )?;
        let limit = self
            .snapshots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .peers_limit;
        let cap = peers_cap(limit).bytes;
        // Widening usize to u64 is lossless on supported targets.
        if payload_len as u64 > cap {
            return Err(ClaimRefused::OverBytes { bytes: cap });
        }
        Ok(limit)
    }

    /// Claims the live `GetServices` reservation for a `Services` of
    /// `payload_len` bytes, before its payload is decoded. Returns the
    /// request's wanted list.
    ///
    /// As with [`Self::claim_peers`], the claim also checks the request's own
    /// cap, `42 + min(n, 8) × 294` bytes for `n` wanted services.
    pub(super) fn claim_services(
        &self,
        payload_len: usize,
    ) -> Result<Vec<ZakuraServiceId>, ClaimRefused> {
        self.reservations.lock().claim_end(
            &QueryKind::Services,
            u16::from(MSG_DISCOVERY_SERVICES),
            payload_len,
        )?;
        let wanted = std::mem::take(
            &mut self
                .snapshots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .wanted_services,
        );
        let cap = services_cap(wanted.len()).bytes;
        // Widening usize to u64 is lossless on supported targets.
        if payload_len as u64 > cap {
            return Err(ClaimRefused::OverBytes { bytes: cap });
        }
        Ok(wanted)
    }

    /// Ends the session's queries: an unwritten query is skipped, and an
    /// unanswered written query closes the connection.
    pub(super) fn retire(&self) {
        self.fence.retire();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{
        discovery::protocol::GetServices, framed_channel, regulation::UNFINISHED_EXCHANGE,
    };

    fn get_peers(limit: u16) -> DiscoveryMessage {
        DiscoveryMessage::GetPeers {
            limit,
            wanted_services: Vec::new(),
            exclude_node_ids: Vec::new(),
        }
    }

    /// The payload length of a `Peers` with `records` maximal records.
    fn peers_len(records: u16) -> usize {
        usize::try_from(peers_cap(records).bytes).expect("a small cap fits usize")
    }

    fn new_queries() -> (Queries, CancellationToken, CloseCause) {
        let connection = CancellationToken::new();
        let close_cause = CloseCause::new();
        let queries = Queries::new(reservation_pool(), connection.clone(), close_cause.clone());
        (queries, connection, close_cause)
    }

    /// A query is sent only once its previous answer arrives, and each
    /// answer claims its reservation exactly once.
    #[tokio::test]
    async fn one_get_peers_is_live_and_its_answer_claims_it_once() {
        let (queries, _connection, _close_cause) = new_queries();
        let (send, mut recv) = framed_channel(4);
        let cancel = CancellationToken::new();

        assert!(matches!(
            queries.claim_peers(2),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        queries
            .send_query(&send, &cancel, get_peers(3))
            .await
            .expect("the first query is sent");
        assert!(matches!(
            queries.send_query(&send, &cancel, get_peers(3)).await,
            Err(QueryUnsent::NotDue)
        ));
        recv.recv().await.expect("the query was queued");

        assert_eq!(queries.claim_peers(peers_len(3)), Ok(3));
        assert!(matches!(
            queries.claim_peers(2),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        queries
            .send_query(&send, &cancel, get_peers(1))
            .await
            .expect("an answered query frees its kind");
        recv.recv().await.expect("the query was queued");
        assert!(matches!(
            queries.claim_peers(peers_len(1) + 1),
            Err(ClaimRefused::OverBytes { .. })
        ));
    }

    fn get_services(wanted_services: Vec<ZakuraServiceId>) -> DiscoveryMessage {
        DiscoveryMessage::GetServices(GetServices { wanted_services })
    }

    /// A `GetServices` reservation is independent of the `GetPeers` one. Its
    /// answer claims it once, returns the wanted list, and stays within
    /// `42 + min(n, 8) × 294` bytes.
    #[tokio::test]
    async fn get_services_is_reserved_beside_get_peers_and_claimed_once() {
        let (queries, _connection, _close_cause) = new_queries();
        let (send, mut recv) = framed_channel(8);
        let cancel = CancellationToken::new();
        let wanted = vec![ZakuraServiceId::discovery()];
        let services_len =
            |wanted| usize::try_from(services_cap(wanted).bytes).expect("a small cap fits usize");

        assert!(matches!(
            queries.claim_services(services_len(0)),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        queries
            .send_query(&send, &cancel, get_peers(1))
            .await
            .expect("GetPeers is sent");
        queries
            .send_query(&send, &cancel, get_services(wanted.clone()))
            .await
            .expect("GetServices is sent beside GetPeers");
        assert!(matches!(
            queries
                .send_query(&send, &cancel, get_services(Vec::new()))
                .await,
            Err(QueryUnsent::NotDue)
        ));
        recv.recv().await.expect("GetPeers was queued");
        recv.recv().await.expect("GetServices was queued");

        assert_eq!(queries.claim_services(services_len(1)), Ok(wanted));
        assert!(matches!(
            queries.claim_services(services_len(1)),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        assert_eq!(queries.claim_peers(peers_len(1)), Ok(1));

        // One wanted service allows one summary, below the row's largest
        // answer.
        queries
            .send_query(
                &send,
                &cancel,
                get_services(vec![ZakuraServiceId::discovery()]),
            )
            .await
            .expect("an answered query frees its kind");
        recv.recv().await.expect("GetServices was queued");
        assert!(matches!(
            queries.claim_services(services_len(1) + 1),
            Err(ClaimRefused::OverBytes { .. })
        ));

        // An empty wanted list reserves the largest answer.
        queries
            .send_query(&send, &cancel, get_services(Vec::new()))
            .await
            .expect("an answered query frees its kind");
        recv.recv().await.expect("GetServices was queued");
        assert_eq!(queries.claim_services(services_len(0)), Ok(Vec::new()));
    }

    /// Retiring the queries closes the connection only when a written query
    /// is unanswered.
    #[tokio::test]
    async fn retiring_closes_the_connection_only_for_an_unanswered_written_query() {
        let (send, mut recv) = framed_channel(4);
        let cancel = CancellationToken::new();

        // Queued but never written: the transport skips it.
        let (queries, connection, _close_cause) = new_queries();
        queries
            .send_query(&send, &cancel, get_peers(1))
            .await
            .expect("the query is queued");
        queries.retire();
        assert!(!connection.is_cancelled());
        assert!(recv.try_recv().is_err(), "a fenced query is never written");

        // Written and answered.
        let (queries, connection, _close_cause) = new_queries();
        queries
            .send_query(&send, &cancel, get_peers(1))
            .await
            .expect("the query is queued");
        recv.recv().await.expect("the query is written");
        queries.claim_peers(2).expect("the answer claims the query");
        queries.retire();
        assert!(!connection.is_cancelled());

        // Written and unanswered.
        let (queries, connection, close_cause) = new_queries();
        queries
            .send_query(&send, &cancel, get_peers(1))
            .await
            .expect("the query is queued");
        recv.recv().await.expect("the query is written");
        queries.retire();
        assert!(connection.is_cancelled());
        assert_eq!(close_cause.get_or("none"), UNFINISHED_EXCHANGE);
    }
}
