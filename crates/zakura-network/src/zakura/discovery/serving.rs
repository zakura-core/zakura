//! Discovery requests served through the shared serving loop.
//!
//! Each request kind is one [`Serve`] implementation. The reader hands a
//! decoded request to its session and moves on; sampling and summary work run
//! off the reader, and a reply waits for queue space instead of being dropped.

use iroh::EndpointId;

use crate::zakura::{
    regulation::{
        PeerServeLimits, Responded, ResponseSink, Serve, ServeCapacity, ServeEnd, WorkLease,
    },
    BlockSyncHandle, Frame, HeaderSyncHandle, FRAME_HEADER_BYTES,
};

use super::{
    pipe::DISCOVERY_FRAME_MESSAGE_TYPE,
    protocol::{
        BlockSyncServiceSummary, DiscoveryMessage, GetServices, HeaderSyncServiceSummary,
        ServiceSummaryEnvelope, ZakuraDiscoveryHandle, ZakuraServiceId,
        MAX_DISCOVERY_MESSAGE_BYTES,
    },
};

#[cfg(test)]
mod tests;

/// Largest discovery response frame.
// The cast is lossless: the discovery message cap is 16 KiB.
const MAX_DISCOVERY_RESPONSE_FRAME: u32 = (MAX_DISCOVERY_MESSAGE_BYTES + FRAME_HEADER_BYTES) as u32;

/// Node-wide discovery requests that may execute at once.
const DISCOVERY_NODE_SERVE_SLOTS: usize = 4;

/// Serving capacity shared by every discovery session: one request and one
/// unwritten response per peer.
pub(super) fn discovery_serve_capacity() -> ServeCapacity {
    ServeCapacity::new(
        DISCOVERY_NODE_SERVE_SLOTS,
        PeerServeLimits {
            execution_slots: 1,
            output_bytes: MAX_DISCOVERY_RESPONSE_FRAME,
        },
    )
}

/// A decoded `GetPeers` request.
#[derive(Debug)]
pub(super) struct GetPeersRequest {
    pub(super) limit: u16,
    pub(super) wanted_services: Vec<ZakuraServiceId>,
    pub(super) exclude_node_ids: Vec<EndpointId>,
}

/// Answers `GetPeers` with a sample of the address book.
#[derive(Debug)]
pub(super) struct GetPeersServe {
    pub(super) handle: ZakuraDiscoveryHandle,
    /// The requester, which the sample excludes.
    pub(super) peer_node_id: EndpointId,
}

impl Serve for GetPeersServe {
    type Request = GetPeersRequest;

    fn response_cap(&self, _request: &GetPeersRequest) -> u32 {
        MAX_DISCOVERY_RESPONSE_FRAME
    }

    async fn produce(
        &self,
        request: GetPeersRequest,
        lease: WorkLease,
        sink: ResponseSink,
    ) -> Result<Responded, ServeEnd> {
        if lease.is_cancelled() {
            return Err(ServeEnd::Cancelled);
        }
        let records = self
            .handle
            .sample_peers(
                self.peer_node_id,
                usize::from(request.limit),
                &request.wanted_services,
                &request.exclude_node_ids,
            )
            .await;
        sink.respond(discovery_frame(DiscoveryMessage::Peers { records })?)
    }
}

/// Answers `GetServices` with this node's first-party service summaries.
#[derive(Debug)]
pub(super) struct GetServicesServe {
    pub(super) handle: ZakuraDiscoveryHandle,
    pub(super) header_sync: Option<HeaderSyncHandle>,
    pub(super) block_sync: Option<BlockSyncHandle>,
}

impl Serve for GetServicesServe {
    type Request = GetServices;

    fn response_cap(&self, _request: &GetServices) -> u32 {
        MAX_DISCOVERY_RESPONSE_FRAME
    }

    async fn produce(
        &self,
        query: GetServices,
        lease: WorkLease,
        sink: ResponseSink,
    ) -> Result<Responded, ServeEnd> {
        if lease.is_cancelled() {
            return Err(ServeEnd::Cancelled);
        }
        let mut summaries = Vec::new();
        if service_wanted(&query.wanted_services, &ZakuraServiceId::header_sync()) {
            if let Some(header_sync) = &self.header_sync {
                let (best_height, best_hash) = header_sync.best_header_tip();
                let summary = HeaderSyncServiceSummary::from_snapshot(
                    best_height,
                    best_hash,
                    None,
                    true,
                    header_sync.peer_snapshot(),
                );
                summaries.push(ServiceSummaryEnvelope::header_sync(&summary).map_err(local_fault)?);
            }
        }
        if service_wanted(&query.wanted_services, &ZakuraServiceId::discovery()) {
            let summary = self.handle.local_discovery_summary().await;
            summaries.push(ServiceSummaryEnvelope::discovery(&summary).map_err(local_fault)?);
        }
        if service_wanted(&query.wanted_services, &ZakuraServiceId::block_sync()) {
            if let Some(block_sync) = &self.block_sync {
                let summary = BlockSyncServiceSummary::from_status_and_snapshot(
                    block_sync.local_status(),
                    block_sync.peer_snapshot(),
                );
                summaries.push(ServiceSummaryEnvelope::block_sync(&summary).map_err(local_fault)?);
            }
        }
        let services = self.handle.local_services_response(summaries);
        sink.respond(discovery_frame(DiscoveryMessage::Services(services))?)
    }
}

pub(super) fn service_wanted(
    wanted_services: &[ZakuraServiceId],
    service_id: &ZakuraServiceId,
) -> bool {
    wanted_services.is_empty() || wanted_services.iter().any(|wanted| wanted == service_id)
}

fn discovery_frame(message: DiscoveryMessage) -> Result<Frame, ServeEnd> {
    Ok(Frame {
        message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
        flags: 0,
        payload: message.encode().map_err(local_fault)?,
    })
}

fn local_fault(error: impl std::fmt::Display) -> ServeEnd {
    ServeEnd::LocalFault(error.to_string())
}
