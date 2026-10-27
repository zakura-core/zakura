//! Serving discovery's request rows through [`Serve`].
//!
//! The reader admits each request without waiting and keeps reading. `Serve`
//! takes response slots, output bytes, and execution before `produce` runs,
//! and sends exactly one response per admitted request.
//!
//! Discovery's limits are its own, not the throughput target's: a requester
//! asks each row at most once per answer, and every answer is small.

use std::sync::Arc;

use zakura_quic::NodeId;
use zakura_rt::sync::CancellationToken;

use super::{
    protocol::{
        BlockSyncServiceSummary, DiscoveryMessage, DiscoveryWireError, GetPeers, GetServices,
        HeaderSyncServiceSummary, ServiceSummaryEnvelope, ZakuraDiscoveryHandle, ZakuraServiceId,
    },
    wire::{peers_cap, services_cap, GET_PEERS, GET_SERVICES, LARGEST_PEERS, LARGEST_SERVICES},
};
use crate::zakura::{
    regulation::{
        Produce, Responded, ResponseCap, ResponseSink, ResponsesWritten, Serve, ServeCapacity,
        ServeEnd, ServeLimits, SinkProgress, WorkLease,
    },
    BlockSyncHandle, CloseCause, FramedSend, HeaderSyncHandle, MessageRole, MessageRule,
    ZakuraPeerId,
};

/// Node-wide `produce` steps for one discovery request row. Each step reads
/// local state for microseconds, so more steps would only queue.
const NODE_EXECUTION: usize = 4;
/// Each row allows one request in flight per peer, so one step serves it.
const PEER_EXECUTION: usize = 1;
/// Unsent answers one peer may hold on a row. A conformant requester asks
/// again only after it reads the answer, so it holds at most one. The second
/// lets a reconnected peer's answer pass one that its retired session left
/// unsent. A peer that stops reading makes its next request wait, and
/// requests past the commitment margin then close the connection.
const PEER_OUTPUT_RESPONSES: usize = 2;
/// The serving limits of a discovery request row whose largest response is
/// `largest` payload bytes, on a node that admits `max_connections`.
///
/// The node may hold every connection's share of unsent answers, so the pool
/// follows the effective connection cap rather than a compile-time default.
fn serving_limits(largest: ResponseCap, max_connections: usize) -> ServeLimits {
    let largest = largest.output_bytes();
    let node_output_responses = PEER_OUTPUT_RESPONSES.saturating_mul(max_connections.max(1));
    // Widening usize to u64 is lossless on supported targets.
    ServeLimits {
        node_execution: NODE_EXECUTION,
        peer_execution: PEER_EXECUTION,
        peer_output_responses: PEER_OUTPUT_RESPONSES,
        node_output_responses,
        peer_output_bytes: PEER_OUTPUT_RESPONSES as u64 * largest,
        node_output_bytes: (node_output_responses as u64).saturating_mul(largest),
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
    get_services: ServeCapacity,
}

/// One session's serving of discovery's request rows.
#[derive(Debug)]
pub(super) struct Serving {
    pub(super) get_peers: Serve<PeerSampler>,
    pub(super) get_services: Serve<ServiceReporter>,
}

impl ServingCapacity {
    /// Builds the node-wide capacity for a node that admits `max_connections`.
    pub(super) fn new(max_connections: usize) -> Self {
        let row = |row, largest| {
            ServeCapacity::new("discovery", row, serving_limits(largest, max_connections))
                .expect("the discovery serving limits are nonzero and fit their budgets")
        };
        Self {
            get_peers: row(&GET_PEERS, LARGEST_PEERS),
            get_services: row(&GET_SERVICES, LARGEST_SERVICES),
        }
    }

    /// Serves one session's requests on `send`. Cancelling `cancel` ends the
    /// session's serving, and a serving panic closes `connection`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn session(
        &self,
        sampler: PeerSampler,
        reporter: ServiceReporter,
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
                send.clone(),
                cancel.clone(),
                connection.clone(),
                close_cause.clone(),
            ),
            get_services: self.get_services.session(
                Arc::new(reporter),
                peer,
                advertised(&GET_SERVICES),
                send,
                cancel,
                connection,
                close_cause,
            ),
        }
    }
}

impl Serving {
    /// Waits until this session has written every answer it admitted.
    pub(super) fn answers_written(&self) -> AnswersWritten {
        AnswersWritten([
            self.get_peers.responses_written(),
            self.get_services.responses_written(),
        ])
    }
}

/// Waits until one session has written every admitted `Peers` and `Services`.
#[derive(Clone, Debug)]
pub(super) struct AnswersWritten([ResponsesWritten; 2]);

impl AnswersWritten {
    /// Resolves once both rows have written every admitted answer.
    pub(super) async fn wait(&self) {
        for row in &self.0 {
            row.wait().await;
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

/// Answers one session's `GetServices` with this node's live summaries.
#[derive(Debug)]
pub(super) struct ServiceReporter {
    pub(super) handle: ZakuraDiscoveryHandle,
    pub(super) header_sync: Option<HeaderSyncHandle>,
    pub(super) block_sync: Option<BlockSyncHandle>,
}

impl ServiceReporter {
    /// Summarizes each local service that `query` wants. The filter applies
    /// before any summary is built.
    async fn summaries(
        &self,
        query: &GetServices,
    ) -> Result<Vec<ServiceSummaryEnvelope>, DiscoveryWireError> {
        let mut summaries = Vec::new();

        if query.wants(&ZakuraServiceId::header_sync()) {
            if let Some(header_sync) = &self.header_sync {
                let (best_height, best_hash) = header_sync.best_header_tip();
                let summary = HeaderSyncServiceSummary::from_snapshot(
                    best_height,
                    best_hash,
                    None,
                    true,
                    header_sync.peer_snapshot(),
                );
                summaries.push(ServiceSummaryEnvelope::header_sync(&summary)?);
            }
        }

        if query.wants(&ZakuraServiceId::discovery()) {
            let summary = self.handle.local_discovery_summary().await;
            summaries.push(ServiceSummaryEnvelope::discovery(&summary)?);
        }

        if query.wants(&ZakuraServiceId::block_sync()) {
            if let Some(block_sync) = &self.block_sync {
                let summary = BlockSyncServiceSummary::from_status_and_snapshot(
                    block_sync.local_status(),
                    block_sync.peer_snapshot(),
                );
                summaries.push(ServiceSummaryEnvelope::block_sync(&summary)?);
            }
        }

        Ok(summaries)
    }
}

impl Produce for ServiceReporter {
    type Request = GetServices;
    type Message = DiscoveryMessage;

    fn response_cap(&self, query: &GetServices) -> ResponseCap {
        services_cap(query.wanted_services.len())
    }

    async fn produce(
        &self,
        query: &GetServices,
        lease: WorkLease,
        sink: ResponseSink<DiscoveryMessage>,
    ) -> Result<Responded, ServeEnd> {
        if lease.is_cancelled() {
            return Err(ServeEnd::Cancelled);
        }
        let summaries = self
            .summaries(query)
            .await
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        sink.finish(&DiscoveryMessage::Services(
            self.handle.local_services_response(summaries),
        ))
        .map_err(|error| ServeEnd::LocalFault(error.to_string()))
    }

    /// A `Services` with no summaries is a legal answer to any `GetServices`.
    fn local_failure(&self, _query: &GetServices, _sent: SinkProgress) -> DiscoveryMessage {
        DiscoveryMessage::Services(self.handle.local_services_response(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_node_pool_follows_the_effective_connection_cap() {
        for max_connections in [1, 124, 256] {
            for largest in [LARGEST_PEERS, LARGEST_SERVICES] {
                let limits = serving_limits(largest, max_connections);
                assert_eq!(
                    limits.node_output_responses,
                    PEER_OUTPUT_RESPONSES * max_connections
                );
                // Every test cap is at most 256, so the cast to u64 is lossless.
                assert_eq!(
                    limits.node_output_bytes,
                    limits.peer_output_bytes * max_connections as u64
                );
            }
        }
    }
}
