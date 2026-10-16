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
    protocol::{DiscoveryMessage, MAX_DISCOVERY_RECORDS_PER_RESPONSE, MSG_DISCOVERY_PEERS},
    wire::{peers_cap, GET_PEERS},
};
use crate::zakura::{
    regulation::{
        sizing, ClaimRefused, FencedSendError, ReservationPool, Reservations, ResponsePrecheck,
        SharedReservations, WriterFence,
    },
    wire_codec::{encode_frame, WireMessage},
    CloseCause, FrameRejection, FramedRecv, FramedSend,
};

/// A discovery query, keying its response's reservation.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum QueryKind {
    Peers,
}

/// The node-wide pool of discovery reservations.
///
/// It holds enough `GetPeers` reservations to keep twice the bandwidth-delay
/// product reserved, so it never limits a node that keeps its sessions within
/// the peer limits.
pub(super) fn reservation_pool() -> ReservationPool {
    let largest = peers_cap(
        u16::try_from(MAX_DISCOVERY_RECORDS_PER_RESPONSE).expect("the largest limit fits u16"),
    );
    ReservationPool::new(sizing::reservation_entries(largest.output_bytes()))
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

/// One session's live queries.
#[derive(Clone, Debug)]
pub(super) struct Queries {
    reservations: SharedReservations<QueryKind>,
    /// The `GetPeers` limit of the live reservation, which bounds the record
    /// count of its answer.
    peers_limit: Arc<Mutex<u16>>,
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
            reservations: SharedReservations::new(Reservations::new(DiscoveryMessage::RULES, 1)),
            peers_limit: Arc::new(Mutex::new(0)),
            pool,
            fence: WriterFence::new(connection, close_cause),
        }
    }

    /// Checks each response header on `recv`'s transport reader before its
    /// payload is read. In-process channels have no transport reader; the
    /// reader's claim applies the same checks before decode.
    pub(super) fn attach_precheck(&self, recv: &FramedRecv) {
        recv.attach_precheck(Arc::new(PeersPrecheck(self.reservations.clone())));
    }

    /// Reserves the answer to a `GetPeers` with `limit`, then sends the
    /// request on `send`. A request that is never written gives its
    /// reservation back.
    pub(super) async fn send_get_peers(
        &self,
        send: &FramedSend,
        cancel: &CancellationToken,
        message: DiscoveryMessage,
    ) -> Result<(), QueryUnsent> {
        let DiscoveryMessage::GetPeers { limit, .. } = message else {
            unreachable!("send_get_peers sends only GetPeers");
        };
        let frame = encode_frame(&message).map_err(|error| {
            tracing::debug!(?error, "failed to encode Zakura discovery GetPeers");
            QueryUnsent::NotDue
        })?;
        let exchange = self.fence.open().ok_or(QueryUnsent::Closed)?;
        let writer = exchange.writer();
        let entry = self.pool.try_entry().ok_or(QueryUnsent::NotDue)?;
        self.reservations
            .lock()
            .reserve_fenced(
                QueryKind::Peers,
                GET_PEERS.message_type,
                peers_cap(limit),
                entry,
                exchange,
            )
            .map_err(|_| QueryUnsent::NotDue)?;
        *self
            .peers_limit
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = limit;

        let sent = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(FencedSendError::Closed),
            sent = send.send_fenced(frame, &writer) => sent,
        };
        match sent {
            Ok(()) => Ok(()),
            Err(error) => {
                self.reservations.lock().retract(&QueryKind::Peers);
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
        let limit = *self
            .peers_limit
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let cap = peers_cap(limit).bytes;
        // Widening usize to u64 is lossless on supported targets.
        if payload_len as u64 > cap {
            return Err(ClaimRefused::OverBytes { bytes: cap });
        }
        Ok(limit)
    }

    /// Ends the session's queries: an unwritten query is skipped, and an
    /// unanswered written query closes the connection.
    pub(super) fn retire(&self) {
        self.fence.retire();
    }
}

/// The transport reader's header check for `Peers`.
///
/// `Services` keeps its row checks alone until its own reservation lands.
#[derive(Debug)]
struct PeersPrecheck(SharedReservations<QueryKind>);

impl ResponsePrecheck for PeersPrecheck {
    fn check(&self, message_type: u16, payload_len: usize) -> Result<(), FrameRejection> {
        if message_type != u16::from(MSG_DISCOVERY_PEERS) {
            return Ok(());
        }
        self.0.check(message_type, payload_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{framed_channel, regulation::UNFINISHED_EXCHANGE};

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
            .send_get_peers(&send, &cancel, get_peers(3))
            .await
            .expect("the first query is sent");
        assert!(matches!(
            queries.send_get_peers(&send, &cancel, get_peers(3)).await,
            Err(QueryUnsent::NotDue)
        ));
        recv.recv().await.expect("the query was queued");

        assert_eq!(queries.claim_peers(peers_len(3)), Ok(3));
        assert!(matches!(
            queries.claim_peers(2),
            Err(ClaimRefused::Unsolicited { .. })
        ));
        queries
            .send_get_peers(&send, &cancel, get_peers(1))
            .await
            .expect("an answered query frees its kind");
        recv.recv().await.expect("the query was queued");
        assert!(matches!(
            queries.claim_peers(peers_len(1) + 1),
            Err(ClaimRefused::OverBytes { .. })
        ));
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
            .send_get_peers(&send, &cancel, get_peers(1))
            .await
            .expect("the query is queued");
        queries.retire();
        assert!(!connection.is_cancelled());
        assert!(recv.try_recv().is_err(), "a fenced query is never written");

        // Written and answered.
        let (queries, connection, _close_cause) = new_queries();
        queries
            .send_get_peers(&send, &cancel, get_peers(1))
            .await
            .expect("the query is queued");
        recv.recv().await.expect("the query is written");
        queries.claim_peers(2).expect("the answer claims the query");
        queries.retire();
        assert!(!connection.is_cancelled());

        // Written and unanswered.
        let (queries, connection, close_cause) = new_queries();
        queries
            .send_get_peers(&send, &cancel, get_peers(1))
            .await
            .expect("the query is queued");
        recv.recv().await.expect("the query is written");
        queries.retire();
        assert!(connection.is_cancelled());
        assert_eq!(close_cause.get_or("none"), UNFINISHED_EXCHANGE);
    }
}
