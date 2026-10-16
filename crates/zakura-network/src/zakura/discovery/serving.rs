//! Serving discovery's request rows through [`Serve`](crate::zakura::regulation::Serve).
//!
//! The reader admits each request without waiting and keeps reading. `Serve`
//! takes response slots, output bytes, and execution before `produce` runs,
//! and sends exactly one response per admitted request.

use zakura_quic::NodeId;

use super::{
    protocol::{
        BlockSyncServiceSummary, DiscoveryMessage, DiscoveryWireError, GetServices,
        HeaderSyncServiceSummary, ServiceSummaryEnvelope, ZakuraDiscoveryHandle, ZakuraServiceId,
        MAX_DISCOVERY_RECORDS_PER_RESPONSE,
    },
    wire::{peers_cap, services_cap, GET_PEERS, GET_SERVICES},
};
use crate::zakura::{
    regulation::{
        sizing, Produce, Responded, ResponseCap, ResponseSink, ServeCapacity, ServeEnd,
        ServeLimits, SinkProgress, WorkLease,
    },
    BlockSyncHandle, HeaderSyncHandle,
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

/// The node-wide `GetServices` serving capacity, shared by every session.
pub(super) fn get_services_capacity() -> ServeCapacity {
    ServeCapacity::new("discovery", &GET_SERVICES, serving_limits(services_cap(0)))
        .expect("the discovery serving limits are nonzero and fit their budgets")
}

/// Answers one session's `GetServices` with this node's live summaries.
#[derive(Debug)]
pub(super) struct ServiceReporter {
    pub(super) handle: ZakuraDiscoveryHandle,
    pub(super) header_sync: Option<HeaderSyncHandle>,
    pub(super) block_sync: Option<BlockSyncHandle>,
}

impl ServiceReporter {
    /// Summarizes each local service that `wanted` names, or every local
    /// service when `wanted` is empty. The filter applies before any summary
    /// is built.
    async fn summaries(
        &self,
        wanted: &[ZakuraServiceId],
    ) -> Result<Vec<ServiceSummaryEnvelope>, DiscoveryWireError> {
        let mut summaries = Vec::new();

        if service_wanted(wanted, &ZakuraServiceId::header_sync()) {
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

        if service_wanted(wanted, &ZakuraServiceId::discovery()) {
            let summary = self.handle.local_discovery_summary().await;
            summaries.push(ServiceSummaryEnvelope::discovery(&summary)?);
        }

        if service_wanted(wanted, &ZakuraServiceId::block_sync()) {
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
            .summaries(&query.wanted_services)
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

fn service_wanted(wanted: &[ZakuraServiceId], service_id: &ZakuraServiceId) -> bool {
    wanted.is_empty() || wanted.iter().any(|wanted| wanted == service_id)
}
