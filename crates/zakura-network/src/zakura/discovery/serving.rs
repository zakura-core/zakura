//! Serving discovery's request rows through [`Serve`](crate::zakura::regulation::Serve).
//!
//! The reader admits each request without waiting and keeps reading. `Serve`
//! takes response slots, output bytes, and execution before `produce` runs,
//! and sends exactly one response per admitted request.
//!
//! Discovery's limits are its own, not the throughput target's: a requester
//! asks each row at most once per answer, and every answer is small.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use zakura_quic::NodeId;

use super::{
    protocol::{DiscoveryMessage, GetPeers, ZakuraDiscoveryHandle},
    wire::{peers_cap, GET_PEERS, LARGEST_PEERS},
};
use crate::zakura::{
    regulation::{
        Produce, Responded, ResponseCap, ResponseSink, Serve, ServeCapacity, ServeEnd, ServeLimits,
        SinkProgress, WorkLease,
    },
    CloseCause, FramedSend, MessageRole, MessageRule, ZakuraPeerId, DEFAULT_ZAKURA_MAX_CONNECTIONS,
};

/// Node-wide `produce` steps for one discovery request row. Each step takes
/// the book lock for microseconds, so more steps would only queue on it.
const NODE_EXECUTION: usize = 4;
/// Each row allows one request in flight per peer, so one step serves it.
const PEER_EXECUTION: usize = 1;
/// Unsent answers one peer may hold on a row. A conformant requester asks
/// again only after it reads the answer, so it holds at most one. The second
/// lets a reconnected peer's answer pass one that its retired session left
/// unsent. A peer that stops reading makes its next request wait, and
/// requests past the commitment margin then close the connection.
const PEER_OUTPUT_RESPONSES: usize = 2;
/// Unsent answers the node may hold on a row: every connection's share at
/// the default connection limit.
const NODE_OUTPUT_RESPONSES: usize = PEER_OUTPUT_RESPONSES * DEFAULT_ZAKURA_MAX_CONNECTIONS;

/// The serving limits of a discovery request row whose largest response is
/// `largest` payload bytes.
fn serving_limits(largest: ResponseCap) -> ServeLimits {
    let largest = largest.output_bytes();
    // Widening usize to u64 is lossless on supported targets.
    ServeLimits {
        node_execution: NODE_EXECUTION,
        peer_execution: PEER_EXECUTION,
        peer_output_responses: PEER_OUTPUT_RESPONSES,
        node_output_responses: NODE_OUTPUT_RESPONSES,
        peer_output_bytes: PEER_OUTPUT_RESPONSES as u64 * largest,
        node_output_bytes: NODE_OUTPUT_RESPONSES as u64 * largest,
    }
}

/// The in-flight limit this node advertises for `row`: the limit the row
/// declares.
fn advertised(row: &MessageRule) -> u32 {
    let MessageRole::Request { max_in_flight, .. } = row.role else {
        unreachable!("discovery serves only request rows");
    };
    max_in_flight
}

/// The node-wide capacity of discovery's request rows, shared by every session.
#[derive(Clone, Debug)]
pub(super) struct ServingCapacity {
    get_peers: ServeCapacity,
}

/// One session's serving of discovery's request rows.
#[derive(Debug)]
pub(super) struct Serving {
    pub(super) get_peers: Serve<PeerSampler>,
}

impl ServingCapacity {
    pub(super) fn new() -> Self {
        Self {
            get_peers: ServeCapacity::new("discovery", &GET_PEERS, serving_limits(LARGEST_PEERS))
                .expect("the discovery serving limits are nonzero and fit their budgets"),
        }
    }

    /// Serves one session's requests on `send`. Cancelling `cancel` ends the
    /// session's serving, and a serving panic closes `connection`.
    pub(super) fn session(
        &self,
        sampler: PeerSampler,
        peer: &ZakuraPeerId,
        send: FramedSend,
        cancel: CancellationToken,
        connection: CancellationToken,
        close_cause: CloseCause,
    ) -> Serving {
        Serving {
            get_peers: self.get_peers.session(
                Arc::new(sampler),
                peer,
                advertised(&GET_PEERS),
                send,
                cancel,
                connection,
                close_cause,
            ),
        }
    }
}

/// Answers one session's `GetPeers` with a sample from the book.
#[derive(Debug)]
pub(super) struct PeerSampler {
    pub(super) handle: ZakuraDiscoveryHandle,
    /// The authenticated requester, which keys its disclosure window.
    pub(super) requester: NodeId,
}

impl Produce for PeerSampler {
    type Request = GetPeers;
    type Message = DiscoveryMessage;

    fn response_cap(&self, query: &GetPeers) -> ResponseCap {
        peers_cap(query.limit)
    }

    async fn produce(
        &self,
        query: &GetPeers,
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
    fn local_failure(&self, _query: &GetPeers, _sent: SinkProgress) -> DiscoveryMessage {
        DiscoveryMessage::Peers {
            records: Vec::new(),
        }
    }
}
