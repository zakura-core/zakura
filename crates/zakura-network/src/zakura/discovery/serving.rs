//! Serving discovery's request rows through [`Serve`](crate::zakura::regulation::Serve).
//!
//! The reader admits each request without waiting and keeps reading. `Serve`
//! takes response slots, output bytes, and execution before `produce` runs,
//! and sends exactly one response per admitted request.

use zakura_quic::NodeId;

use super::{
    protocol::{
        DiscoveryMessage, ZakuraDiscoveryHandle, ZakuraServiceId,
        MAX_DISCOVERY_RECORDS_PER_RESPONSE,
    },
    wire::{peers_cap, GET_PEERS},
};
use crate::zakura::regulation::{
    sizing, Produce, Responded, ResponseCap, ResponseSink, ServeCapacity, ServeEnd, ServeLimits,
    SinkProgress, WorkLease,
};

/// Node-wide `produce` steps for one discovery request row. Each step takes
/// the book lock for microseconds, so more steps would only queue on it.
const NODE_EXECUTION: usize = 4;
/// One peer has at most one request open, so one step serves it.
const PEER_EXECUTION: usize = 1;

/// The serving limits of a discovery request row whose largest response is
/// `largest` payload bytes.
fn serving_limits(largest: ResponseCap) -> ServeLimits {
    ServeLimits {
        node_execution: NODE_EXECUTION,
        peer_execution: PEER_EXECUTION,
        ..sizing::serve_limits(largest.output_bytes(), sizing::TARGET_RTT)
    }
}

/// The node-wide `GetPeers` serving capacity, shared by every session.
pub(super) fn get_peers_capacity() -> ServeCapacity {
    let largest = peers_cap(
        u16::try_from(MAX_DISCOVERY_RECORDS_PER_RESPONSE).expect("the largest limit fits u16"),
    );
    ServeCapacity::new("discovery", &GET_PEERS, serving_limits(largest))
        .expect("the discovery serving limits are nonzero and fit their budgets")
}

/// A decoded `GetPeers`.
#[derive(Debug)]
pub(super) struct PeersQuery {
    pub(super) limit: u16,
    pub(super) wanted_services: Vec<ZakuraServiceId>,
    /// Raw node ids in ascending order, as the decoder checked.
    pub(super) exclude_node_ids: Vec<[u8; 32]>,
}

/// Answers one session's `GetPeers` with a sample from the book.
#[derive(Debug)]
pub(super) struct PeerSampler {
    pub(super) handle: ZakuraDiscoveryHandle,
    /// The authenticated requester, which keys its disclosure window.
    pub(super) requester: NodeId,
}

impl Produce for PeerSampler {
    type Request = PeersQuery;
    type Message = DiscoveryMessage;

    fn response_cap(&self, query: &PeersQuery) -> ResponseCap {
        peers_cap(query.limit)
    }

    async fn produce(
        &self,
        query: &PeersQuery,
        lease: WorkLease,
        sink: ResponseSink<DiscoveryMessage>,
    ) -> Result<Responded, ServeEnd> {
        if lease.is_cancelled() {
            return Err(ServeEnd::Cancelled);
        }
        let records = self
            .handle
            .sample_peers(
                self.requester,
                usize::from(query.limit),
                &query.wanted_services,
                &query.exclude_node_ids,
            )
            .await;
        sink.finish(&DiscoveryMessage::Peers { records })
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))
    }

    /// An empty `Peers` is a legal answer to any `GetPeers`.
    fn local_failure(&self, _query: &PeersQuery, _sent: SinkProgress) -> DiscoveryMessage {
        DiscoveryMessage::Peers {
            records: Vec::new(),
        }
    }
}
