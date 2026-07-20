use serde::Serialize;
#[cfg(test)]
use serde_json::Value;
use zakura_chain::{block, parallel::commitment_aux::BlockCommitmentRoots};
use zakura_jsonl_trace::{impl_jsonl_trace_event, JsonlTraceEvent};

use super::super::{
    config::HeaderSyncStatus,
    error::HeaderSyncWireError,
    events::{
        HeaderSyncAction, HeaderSyncCommitFailureKind, HeaderSyncEvent, HeaderSyncMisbehavior,
        HeaderSyncOperationKind, HeaderSyncWireRequestIdentity,
    },
    state::RangeRequest,
    validation::count_between,
    wire::HeaderSyncMessage,
};
use super::HeaderSyncReactor;
#[cfg(test)]
use crate::zakura::trace::header_sync_trace as hs_trace;
use crate::zakura::{
    trace::{
        ordered_send_error_label, peer_label as trace_peer_label, HEADER_SYNC_TABLE,
        QUEUE_SEND_TABLE,
    },
    OrderedSendError, ServicePeerDirection, ZakuraPeerId,
};

#[derive(Default, Serialize)]
pub(super) struct TreeAuxTraceSummary {
    #[serde(rename = "tree_aux_roots_len")]
    len: u32,
    #[serde(rename = "first_root_height", skip_serializing_if = "Option::is_none")]
    first_height: Option<block::Height>,
    #[serde(rename = "last_root_height", skip_serializing_if = "Option::is_none")]
    last_height: Option<block::Height>,
}

impl TreeAuxTraceSummary {
    pub(super) fn new(roots: &[BlockCommitmentRoots]) -> Self {
        Self {
            len: u32::try_from(roots.len()).unwrap_or(u32::MAX),
            first_height: roots.first().map(|root| root.height),
            last_height: roots.last().map(|root| root.height),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "event")]
enum HeaderTraceEvent {
    #[serde(rename = "header_event_received")]
    EventReceived {
        #[serde(flatten)]
        projection: HeaderEventProjection,
    },
    #[serde(rename = "header_action_dispatched")]
    ActionDispatched {
        #[serde(flatten)]
        action: HeaderActionProjection,
    },
    #[serde(rename = "header_status_sent")]
    StatusSent {
        #[serde(flatten)]
        status: StatusProjection,
    },
    #[serde(rename = "header_status_received")]
    StatusReceived {
        #[serde(flatten)]
        status: StatusProjection,
    },
    #[serde(rename = "header_peer_connected")]
    PeerConnected {
        peer: String,
        direction: &'static str,
        active_connections: u64,
    },
    #[serde(rename = "header_peer_disconnected")]
    PeerDisconnected {
        peer: String,
        active_connections: u64,
    },
    #[serde(rename = "header_get_headers_sent")]
    GetHeadersSent {
        peer: String,
        session_id: u64,
        stream_version: u16,
        request_id: u64,
        range_start: block::Height,
        range_count: u32,
        advertised_cap: u32,
        finalized: bool,
        want_tree_aux_roots: bool,
        range_priority: &'static str,
        verified_block_tip: block::Height,
        finalized_height: block::Height,
        best_header_tip: block::Height,
    },
    #[serde(rename = "header_headers_received")]
    HeadersReceived {
        peer: String,
        range_start: block::Height,
        range_count: u32,
        advertised_cap: u32,
        expected_count: u32,
        in_flight_count: u64,
        want_tree_aux_roots: bool,
        #[serde(flatten)]
        roots: TreeAuxTraceSummary,
    },
    #[serde(rename = "header_headers_served")]
    HeadersServed {
        peer: String,
        range_start: block::Height,
        range_count: u32,
        expected_count: u32,
        want_tree_aux_roots: bool,
        #[serde(flatten)]
        roots: TreeAuxTraceSummary,
    },
    #[serde(rename = "header_range_committed")]
    RangeCommitted {
        range_start: block::Height,
        range_count: u32,
        reason: Option<&'static str>,
    },
    #[serde(rename = "header_range_rejected")]
    RangeRejected {
        #[serde(skip_serializing_if = "Option::is_none")]
        peer: Option<String>,
        range_start: block::Height,
        range_count: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        anchor_hash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        validation_stage: Option<&'static str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_kind: Option<&'static str>,
        reason: Option<&'static str>,
    },
    #[serde(rename = "header_new_block_received")]
    NewBlockReceived {
        peer: String,
        height: block::Height,
        hash: String,
    },
    #[serde(rename = "header_new_block_forwarded")]
    NewBlockForwarded {
        source_peer: String,
        peer: String,
        height: block::Height,
        hash: String,
        destination_peer_count: u64,
    },
    #[serde(rename = "header_new_block_deduped")]
    NewBlockDeduped {
        peer: String,
        height: block::Height,
        hash: String,
        reason: &'static str,
    },
    #[serde(rename = "header_peer_violation")]
    PeerViolation { peer: String, reason: &'static str },
    #[serde(rename = "header_peer_violation_recorded")]
    PeerViolationRecorded { peer: String, reason: &'static str },
    #[serde(rename = "header_frontier_advanced")]
    FrontierAdvanced { height: block::Height, hash: String },
    #[serde(rename = "header_frontier_reanchored")]
    FrontierReanchored { height: block::Height, hash: String },
    #[serde(rename = "header_missing_bodies_reported")]
    MissingBodiesReported {
        range_start: block::Height,
        range_count: u32,
    },
    #[serde(rename = "header_maintenance_wakeup")]
    MaintenanceWakeup,
}

impl_jsonl_trace_event!(HeaderTraceEvent, HEADER_SYNC_TABLE);

pub(super) fn event_received(event: &HeaderSyncEvent) -> impl JsonlTraceEvent {
    HeaderTraceEvent::EventReceived {
        projection: HeaderEventProjection::from(event),
    }
}

pub(super) fn action_dispatched(action: &HeaderSyncAction) -> impl JsonlTraceEvent {
    HeaderTraceEvent::ActionDispatched {
        action: HeaderActionProjection::from(action),
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum HeaderEventProjection {
    PeerConnected {
        peer: String,
    },
    PeerDisconnected {
        peer: String,
    },
    AdvisoryHeaderSummary {
        peer: String,
        height: block::Height,
    },
    FullBlockCommitted {
        height: block::Height,
        hash: String,
    },
    NewBlockAccepted {
        peer: String,
        height: block::Height,
        hash: String,
    },
    NewBlockDuplicate {
        peer: String,
        height: block::Height,
        hash: String,
    },
    NewBlockAcceptedNonBestChain {
        peer: String,
        height: block::Height,
        hash: String,
    },
    NewBlockRejected {
        peer: String,
        hash: String,
    },
    #[cfg(test)]
    WireMessage {
        reason: &'static str,
        peer: String,
        #[serde(flatten)]
        message: HeaderMessageProjection,
    },
    SessionWireMessage {
        reason: &'static str,
        peer: String,
        #[serde(flatten)]
        message: HeaderMessageProjection,
    },
    WireHeaders {
        peer: String,
        range_count: u64,
    },
    WireGetHeaders {
        peer: String,
        range_start: block::Height,
        range_count: u32,
    },
    WireDecodeFailed {
        error_kind: &'static str,
        peer: String,
        #[serde(flatten)]
        error: WireErrorProjection,
    },
    WireProtocolFailure {
        reason: &'static str,
        error_kind: &'static str,
        peer: String,
        #[serde(flatten)]
        error: WireErrorProjection,
    },
    StateFrontiersChanged {
        finalized_height: block::Height,
        verified_block_tip: block::Height,
    },
    VctRootRepairRequested {
        height: block::Height,
        range_count: u64,
        generation: u64,
    },
    VctRootRepairResolved {
        generation: u64,
    },
    BestHeaderTipLoaded {
        height: block::Height,
        hash: String,
    },
    HeaderRangeOperationCompleted {
        peer: String,
        session_id: u64,
        request_id: u64,
        operation_kind: &'static str,
        hash: String,
    },
    HeaderRangeOperationFailed {
        peer: String,
        session_id: u64,
        request_id: u64,
        operation_kind: &'static str,
        reason: &'static str,
    },
    HeaderRangeResponseFinished {
        peer: String,
        range_start: block::Height,
        range_count: u32,
        expected_count: u32,
    },
    HeaderRangeResponseReady {
        peer: String,
        range_start: block::Height,
        range_count: u64,
        expected_count: u32,
    },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum HeaderActionProjection {
    #[cfg(test)]
    SendMessage {
        reason: &'static str,
        peer: String,
        #[serde(flatten)]
        message: HeaderMessageProjection,
    },
    #[cfg(test)]
    ForwardNewBlock {
        #[serde(skip_serializing_if = "Option::is_none")]
        source_peer: Option<String>,
        peer: String,
        height: block::Height,
        hash: String,
    },
    Misbehavior {
        peer: String,
        reason: &'static str,
    },
    NewBlockReceived {
        peer: String,
        height: block::Height,
        hash: String,
    },
    QueryHeadersByHeightRange {
        peer: String,
        range_start: block::Height,
        range_count: u32,
    },
    CommitHeaderRange {
        peer: String,
        session_id: u64,
        request_id: u64,
        operation_kind: &'static str,
        range_start: block::Height,
        range_count: u64,
    },
    QueryBestHeaderTip,
    QueryMissingBlockBodies {
        range_start: block::Height,
        range_count: u32,
    },
    BodyGaps {
        range_start: block::Height,
        range_count: u32,
    },
    HeaderAdvanced {
        height: block::Height,
        hash: String,
    },
    HeaderReanchored {
        height: block::Height,
        hash: String,
        range_start: block::Height,
    },
}

#[derive(Serialize)]
struct StatusProjection {
    peer: String,
    height: block::Height,
    hash: String,
    range_start: block::Height,
    advertised_cap: u32,
    in_flight_count: u16,
}

#[derive(Serialize)]
#[serde(untagged)]
enum HeaderMessageProjection {
    Status {
        height: block::Height,
        hash: String,
        range_start: block::Height,
        advertised_cap: u32,
        in_flight_count: u16,
    },
    Headers {
        range_count: u64,
    },
    GetHeaders {
        range_start: block::Height,
        range_count: u32,
    },
    NewBlock {
        hash: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        height: Option<block::Height>,
    },
}

#[derive(Default, Serialize)]
struct WireErrorProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    root_mismatch_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expected_root_height: Option<block::Height>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actual_root_height: Option<block::Height>,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_root_height: Option<block::Height>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_root_height: Option<block::Height>,
}

#[derive(Serialize)]
struct QueueSendTraceEvent {
    event: &'static str,
    service: &'static str,
    message: &'static str,
    peer: String,
    error: &'static str,
    queue_capacity: u64,
    queue_max_capacity: u64,
    #[serde(flatten)]
    context: QueueSendProjection,
}

impl_jsonl_trace_event!(QueueSendTraceEvent, QUEUE_SEND_TABLE);

pub(super) enum QueueSendContext<'a> {
    Status,
    Headers {
        start_height: block::Height,
        requested_count: u32,
        returned_count: u32,
    },
    NewBlock {
        source: &'a ZakuraPeerId,
        destination: &'a ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    },
}

#[derive(Serialize)]
#[serde(untagged)]
enum QueueSendProjection {
    Empty {},
    Headers {
        range_start: block::Height,
        range_count: u32,
        returned: u32,
    },
    NewBlock {
        source_peer: String,
        destination_peer: String,
        height: block::Height,
        hash: String,
    },
}

impl From<QueueSendContext<'_>> for QueueSendProjection {
    fn from(context: QueueSendContext<'_>) -> Self {
        match context {
            QueueSendContext::Status => Self::Empty {},
            QueueSendContext::Headers {
                start_height,
                requested_count,
                returned_count,
            } => Self::Headers {
                range_start: start_height,
                range_count: requested_count,
                returned: returned_count,
            },
            QueueSendContext::NewBlock {
                source,
                destination,
                height,
                hash,
            } => Self::NewBlock {
                source_peer: peer_label(source),
                destination_peer: peer_label(destination),
                height,
                hash: hash_label(hash),
            },
        }
    }
}

impl HeaderSyncReactor {
    pub(super) fn trace_status_sent(&self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::StatusSent {
                status: StatusProjection::new(peer, status),
            });
    }

    pub(super) fn trace_status_received(&self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::StatusReceived {
                status: StatusProjection::new(peer, status),
            });
    }

    pub(super) fn trace_peer_connected(
        &self,
        peer: &ZakuraPeerId,
        direction: ServicePeerDirection,
        active_connections: usize,
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::PeerConnected {
                peer: peer_label(peer),
                direction: direction.trace_label(),
                active_connections: saturating_usize(active_connections),
            });
    }

    pub(super) fn trace_peer_disconnected(&self, peer: &ZakuraPeerId, active_connections: usize) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::PeerDisconnected {
                peer: peer_label(peer),
                active_connections: saturating_usize(active_connections),
            });
    }

    pub(super) fn trace_get_headers_sent(
        &self,
        range: RangeRequest,
        advertised_cap: u32,
        identity: &HeaderSyncWireRequestIdentity,
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::GetHeadersSent {
                peer: peer_label(&identity.peer),
                session_id: identity.session_id,
                stream_version: super::super::wire::ZAKURA_HEADER_SYNC_STREAM_VERSION,
                request_id: identity.request_id.get(),
                range_start: range.start_height(),
                range_count: range.count(),
                advertised_cap,
                finalized: range.finalized,
                want_tree_aux_roots: range.want_tree_aux_roots,
                range_priority: range.priority.label(),
                verified_block_tip: self.state.verified_block_tip,
                finalized_height: self.state.finalized_height,
                best_header_tip: self.state.best_header_tip,
            });
    }

    pub(super) fn trace_headers_received(
        &self,
        peer: &ZakuraPeerId,
        range: RangeRequest,
        headers: &[std::sync::Arc<block::Header>],
        advertised_cap: u32,
        in_flight_count: usize,
        tree_aux_roots: &[BlockCommitmentRoots],
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::HeadersReceived {
                peer: peer_label(peer),
                range_start: range.start_height(),
                range_count: u32::try_from(headers.len()).unwrap_or(u32::MAX),
                advertised_cap,
                expected_count: range.count(),
                in_flight_count: saturating_usize(in_flight_count),
                want_tree_aux_roots: range.want_tree_aux_roots,
                roots: TreeAuxTraceSummary::new(tree_aux_roots),
            });
    }

    pub(super) fn trace_headers_served(
        &self,
        peer: &ZakuraPeerId,
        start_height: block::Height,
        requested_count: u32,
        returned_count: u32,
        want_tree_aux_roots: bool,
        roots: TreeAuxTraceSummary,
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::HeadersServed {
                peer: peer_label(peer),
                range_start: start_height,
                range_count: returned_count,
                expected_count: requested_count,
                want_tree_aux_roots,
                roots,
            });
    }

    pub(super) fn trace_range_committed(&self, start_height: block::Height, count: u32) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::RangeCommitted {
                range_start: start_height,
                range_count: count,
                reason: None,
            });
    }

    pub(super) fn trace_range_commit_failed(
        &self,
        peer: &ZakuraPeerId,
        start_height: block::Height,
        count: u32,
        reason: &'static str,
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::RangeRejected {
                peer: Some(peer_label(peer)),
                range_start: start_height,
                range_count: count,
                anchor_hash: None,
                validation_stage: None,
                error_kind: None,
                reason: Some(reason),
            });
    }

    pub(super) fn trace_range_validation_rejected(
        &self,
        peer: &ZakuraPeerId,
        range: RangeRequest,
        count: u32,
        validation_stage: &'static str,
        error_kind: &'static str,
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::RangeRejected {
                peer: Some(peer_label(peer)),
                range_start: range.start_height(),
                range_count: count,
                anchor_hash: range.anchor_hash.map(hash_label),
                validation_stage: Some(validation_stage),
                error_kind: Some(error_kind),
                reason: Some(misbehavior_reason_label(
                    HeaderSyncMisbehavior::InvalidRange,
                )),
            });
    }

    pub(super) fn trace_new_block_received(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::NewBlockReceived {
                peer: peer_label(peer),
                height,
                hash: hash_label(hash),
            });
    }

    pub(super) fn trace_new_block_forwarded(
        &self,
        source: &ZakuraPeerId,
        destination: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        destination_count: usize,
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::NewBlockForwarded {
                source_peer: peer_label(source),
                peer: peer_label(destination),
                height,
                hash: hash_label(hash),
                destination_peer_count: saturating_usize(destination_count),
            });
    }

    pub(super) fn trace_new_block_deduped(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        reason: &'static str,
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::NewBlockDeduped {
                peer: peer_label(peer),
                height,
                hash: hash_label(hash),
                reason,
            });
    }

    pub(super) fn trace_peer_violation(&self, peer: &ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::PeerViolation {
                peer: peer_label(peer),
                reason: misbehavior_reason_label(reason),
            });
    }

    pub(super) fn trace_peer_violation_recorded(
        &self,
        peer: &ZakuraPeerId,
        reason: HeaderSyncMisbehavior,
    ) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::PeerViolationRecorded {
                peer: peer_label(peer),
                reason: misbehavior_reason_label(reason),
            });
    }

    pub(super) fn trace_frontier_advanced(&self, height: block::Height, hash: block::Hash) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::FrontierAdvanced {
                height,
                hash: hash_label(hash),
            });
    }

    pub(super) fn trace_frontier_reanchored(&self, height: block::Height, hash: block::Hash) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::FrontierReanchored {
                height,
                hash: hash_label(hash),
            });
    }

    pub(super) fn trace_missing_bodies(&self, from: block::Height, to: block::Height) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::MissingBodiesReported {
                range_start: from,
                range_count: count_between(from, to),
            });
    }

    pub(super) fn trace_maintenance_wakeup(&self) {
        self.startup
            .trace
            .emit_event(|| HeaderTraceEvent::MaintenanceWakeup);
    }

    pub(super) fn trace_queue_send_failed(
        &self,
        peer: &ZakuraPeerId,
        message: &'static str,
        error: &OrderedSendError,
        queue_capacity: usize,
        queue_max_capacity: usize,
        context: QueueSendContext<'_>,
    ) {
        self.startup.trace.emit_event(|| QueueSendTraceEvent {
            event: "queue_send_failed",
            service: "header_sync",
            message,
            peer: peer_label(peer),
            error: ordered_send_error_label(error),
            queue_capacity: saturating_usize(queue_capacity),
            queue_max_capacity: saturating_usize(queue_max_capacity),
            context: context.into(),
        });
    }
}

impl From<&HeaderSyncEvent> for HeaderEventProjection {
    fn from(event: &HeaderSyncEvent) -> Self {
        match event {
            HeaderSyncEvent::PeerConnected(session) => Self::PeerConnected {
                peer: peer_label(session.peer_id()),
            },
            HeaderSyncEvent::PeerDisconnected(peer) => Self::PeerDisconnected {
                peer: peer_label(peer),
            },
            HeaderSyncEvent::AdvisoryHeaderSummary { peer, summary } => {
                Self::AdvisoryHeaderSummary {
                    peer: peer_label(peer),
                    height: summary.best_height,
                }
            }
            HeaderSyncEvent::FullBlockCommitted { height, hash, .. } => Self::FullBlockCommitted {
                height: *height,
                hash: hash_label(*hash),
            },
            HeaderSyncEvent::NewBlockAccepted {
                peer, height, hash, ..
            } => Self::NewBlockAccepted {
                peer: peer_label(peer),
                height: *height,
                hash: hash_label(*hash),
            },
            HeaderSyncEvent::NewBlockDuplicate { peer, height, hash } => Self::NewBlockDuplicate {
                peer: peer_label(peer),
                height: *height,
                hash: hash_label(*hash),
            },
            HeaderSyncEvent::NewBlockAcceptedNonBestChain { peer, height, hash } => {
                Self::NewBlockAcceptedNonBestChain {
                    peer: peer_label(peer),
                    height: *height,
                    hash: hash_label(*hash),
                }
            }
            HeaderSyncEvent::NewBlockRejected { peer, hash } => Self::NewBlockRejected {
                peer: peer_label(peer),
                hash: hash_label(*hash),
            },
            #[cfg(test)]
            HeaderSyncEvent::WireMessage { peer, msg } => Self::WireMessage {
                reason: header_sync_message_label(msg),
                peer: peer_label(peer),
                message: msg.into(),
            },
            HeaderSyncEvent::SessionWireMessage { peer, msg, .. } => Self::SessionWireMessage {
                reason: header_sync_message_label(msg),
                peer: peer_label(peer),
                message: msg.into(),
            },
            HeaderSyncEvent::WireHeaders { peer, headers, .. } => Self::WireHeaders {
                peer: peer_label(peer),
                range_count: saturating_usize(headers.len()),
            },
            HeaderSyncEvent::WireGetHeaders {
                peer,
                start_height,
                count,
                ..
            } => Self::WireGetHeaders {
                peer: peer_label(peer),
                range_start: *start_height,
                range_count: *count,
            },
            HeaderSyncEvent::WireDecodeFailed { peer, error } => Self::WireDecodeFailed {
                error_kind: header_sync_wire_error_kind(error),
                peer: peer_label(peer),
                error: error.as_ref().into(),
            },
            HeaderSyncEvent::WireProtocolFailure {
                peer,
                reason,
                error,
            } => Self::WireProtocolFailure {
                reason: misbehavior_reason_label(*reason),
                error_kind: header_sync_wire_error_kind(error),
                peer: peer_label(peer),
                error: error.as_ref().into(),
            },
            HeaderSyncEvent::StateFrontiersChanged(frontiers) => Self::StateFrontiersChanged {
                finalized_height: frontiers.finalized_height,
                verified_block_tip: frontiers.verified_block_tip,
            },
            HeaderSyncEvent::VctRootRepairRequested {
                height,
                generation,
                expected_hashes,
                ..
            } => Self::VctRootRepairRequested {
                height: *height,
                range_count: saturating_usize(expected_hashes.len()),
                generation: *generation,
            },
            HeaderSyncEvent::VctRootRepairResolved { generation } => Self::VctRootRepairResolved {
                generation: *generation,
            },
            HeaderSyncEvent::BestHeaderTipLoaded {
                tip_height,
                tip_hash,
            } => Self::BestHeaderTipLoaded {
                height: *tip_height,
                hash: hash_label(*tip_hash),
            },
            HeaderSyncEvent::HeaderRangeOperationCompleted {
                operation,
                tip_hash,
            } => Self::HeaderRangeOperationCompleted {
                peer: peer_label(&operation.wire_request.peer),
                session_id: operation.wire_request.session_id,
                request_id: operation.wire_request.request_id.get(),
                operation_kind: operation_kind_label(operation.op_kind),
                hash: hash_label(*tip_hash),
            },
            HeaderSyncEvent::HeaderRangeOperationFailed { operation, kind } => {
                Self::HeaderRangeOperationFailed {
                    peer: peer_label(&operation.wire_request.peer),
                    session_id: operation.wire_request.session_id,
                    request_id: operation.wire_request.request_id.get(),
                    operation_kind: operation_kind_label(operation.op_kind),
                    reason: commit_failure_reason_label(*kind),
                }
            }
            HeaderSyncEvent::HeaderRangeResponseFinished {
                peer,
                start_height,
                requested_count,
                returned_count,
                ..
            } => Self::HeaderRangeResponseFinished {
                peer: peer_label(peer),
                range_start: *start_height,
                range_count: *returned_count,
                expected_count: *requested_count,
            },
            HeaderSyncEvent::HeaderRangeResponseReady {
                peer,
                start_height,
                requested_count,
                headers,
                ..
            } => Self::HeaderRangeResponseReady {
                peer: peer_label(peer),
                range_start: *start_height,
                range_count: saturating_usize(headers.len()),
                expected_count: *requested_count,
            },
        }
    }
}

impl From<&HeaderSyncAction> for HeaderActionProjection {
    fn from(action: &HeaderSyncAction) -> Self {
        match action {
            #[cfg(test)]
            HeaderSyncAction::SendMessage { peer, msg, .. } => Self::SendMessage {
                reason: header_sync_message_label(msg),
                peer: peer_label(peer),
                message: msg.into(),
            },
            #[cfg(test)]
            HeaderSyncAction::ForwardNewBlock {
                source,
                peer,
                height,
                hash,
                ..
            } => Self::ForwardNewBlock {
                source_peer: source.as_ref().map(peer_label),
                peer: peer_label(peer),
                height: *height,
                hash: hash_label(*hash),
            },
            HeaderSyncAction::Misbehavior { peer, reason } => Self::Misbehavior {
                peer: peer_label(peer),
                reason: misbehavior_reason_label(*reason),
            },
            HeaderSyncAction::NewBlockReceived {
                peer, height, hash, ..
            } => Self::NewBlockReceived {
                peer: peer_label(peer),
                height: *height,
                hash: hash_label(*hash),
            },
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer, start, count, ..
            } => Self::QueryHeadersByHeightRange {
                peer: peer_label(peer),
                range_start: *start,
                range_count: *count,
            },
            HeaderSyncAction::CommitHeaderRange {
                operation, payload, ..
            } => Self::CommitHeaderRange {
                peer: peer_label(&operation.wire_request.peer),
                session_id: operation.wire_request.session_id,
                request_id: operation.wire_request.request_id.get(),
                operation_kind: operation_kind_label(operation.op_kind),
                range_start: payload.range().start(),
                range_count: u64::from(payload.range().count()),
            },
            HeaderSyncAction::QueryBestHeaderTip => Self::QueryBestHeaderTip,
            HeaderSyncAction::QueryMissingBlockBodies { from, limit } => {
                Self::QueryMissingBlockBodies {
                    range_start: *from,
                    range_count: *limit,
                }
            }
            HeaderSyncAction::BodyGaps { from, to } => Self::BodyGaps {
                range_start: *from,
                range_count: count_between(*from, *to),
            },
            HeaderSyncAction::HeaderAdvanced { height, hash } => Self::HeaderAdvanced {
                height: *height,
                hash: hash_label(*hash),
            },
            HeaderSyncAction::HeaderReanchored { old, new } => Self::HeaderReanchored {
                height: new.0,
                hash: hash_label(new.1),
                range_start: old.0,
            },
        }
    }
}

impl StatusProjection {
    fn new(peer: &ZakuraPeerId, status: HeaderSyncStatus) -> Self {
        Self {
            peer: peer_label(peer),
            height: status.tip_height,
            hash: hash_label(status.tip_hash),
            range_start: status.anchor_height,
            advertised_cap: status.max_headers_per_response,
            in_flight_count: status.max_inflight_requests,
        }
    }
}

impl From<&HeaderSyncMessage> for HeaderMessageProjection {
    fn from(msg: &HeaderSyncMessage) -> Self {
        match msg {
            HeaderSyncMessage::Status(status) => Self::Status {
                height: status.tip_height,
                hash: hash_label(status.tip_hash),
                range_start: status.anchor_height,
                advertised_cap: status.max_headers_per_response,
                in_flight_count: status.max_inflight_requests,
            },
            HeaderSyncMessage::Headers { headers, .. } => Self::Headers {
                range_count: saturating_usize(headers.len()),
            },
            HeaderSyncMessage::GetHeaders {
                start_height,
                count,
                ..
            } => Self::GetHeaders {
                range_start: *start_height,
                range_count: *count,
            },
            HeaderSyncMessage::NewBlock(block) => Self::NewBlock {
                hash: hash_label(block.hash()),
                height: block.coinbase_height(),
            },
        }
    }
}

impl From<&HeaderSyncWireError> for WireErrorProjection {
    fn from(error: &HeaderSyncWireError) -> Self {
        match error {
            HeaderSyncWireError::TreeAuxRootHeightMismatch {
                offset,
                expected_height,
                root_height,
                first_root_height,
                last_root_height,
            } => Self {
                root_mismatch_offset: Some(saturating_usize(*offset)),
                expected_root_height: Some(*expected_height),
                actual_root_height: Some(*root_height),
                first_root_height: Some(*first_root_height),
                last_root_height: Some(*last_root_height),
            },
            _ => Self::default(),
        }
    }
}

fn peer_label(peer: &ZakuraPeerId) -> String {
    trace_peer_label(peer)
}

fn hash_label(hash: block::Hash) -> String {
    format!("{hash}")
}

fn saturating_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn header_sync_message_label(msg: &HeaderSyncMessage) -> &'static str {
    match msg {
        HeaderSyncMessage::Status(_) => "status",
        HeaderSyncMessage::Headers { .. } => "headers",
        HeaderSyncMessage::GetHeaders { .. } => "get_headers",
        HeaderSyncMessage::NewBlock(_) => "new_block",
    }
}

pub(super) fn set_header_connectivity_gauges(connected_peers: usize, healthy_peers: usize) {
    // Active Zakura reactor sessions are bounded by the configured connection
    // limit, far below f64's exact integer range.
    metrics::gauge!("zakura.p2p.reactor.active_connections", "reactor" => "header_sync")
        .set(connected_peers as f64);
    metrics::gauge!("zakura.p2p.connected_peers").set(connected_peers as f64);
    metrics::gauge!("zakura.p2p.healthy_peers").set(healthy_peers as f64);
}

pub(super) fn record_wire_validation_metrics(error: &HeaderSyncWireError) {
    let error_kind = header_sync_wire_error_kind(error);
    metrics::counter!(
        "sync.header.validation.rejected",
        "stage" => "wire",
        "error_kind" => error_kind
    )
    .increment(1);
    if matches!(error, HeaderSyncWireError::TreeAuxRootHeightMismatch { .. }) {
        metrics::counter!("sync.header.tree_aux.height_mismatch").increment(1);
    }
}

pub(super) fn header_sync_wire_error_kind(error: &HeaderSyncWireError) -> &'static str {
    match error {
        HeaderSyncWireError::OversizedPayload { .. } => "oversized_payload",
        HeaderSyncWireError::HeaderCountLimit { .. } => "header_count_limit",
        HeaderSyncWireError::InvalidRangeGeometry { .. } => "invalid_range_geometry",
        HeaderSyncWireError::BodySizeCountMismatch { .. } => "body_size_count_mismatch",
        HeaderSyncWireError::TreeAuxRootCountMismatch { .. } => "tree_aux_root_count_mismatch",
        HeaderSyncWireError::TreeAuxRootHeightMismatch { .. } => "tree_aux_root_height_mismatch",
        HeaderSyncWireError::InvalidBoolMarker { .. } => "invalid_bool_marker",
        HeaderSyncWireError::UnrequestedTreeAuxRoots => "unrequested_tree_aux_roots",
        HeaderSyncWireError::UnsolicitedHeaders => "unsolicited_headers",
        HeaderSyncWireError::MissingRequestId { .. } => "missing_request_id",
        HeaderSyncWireError::ZeroHeaderRequestCount => "zero_header_request_count",
        HeaderSyncWireError::HeightOutOfRange(_) => "height_out_of_range",
        HeaderSyncWireError::UnknownMessageType(_) => "unknown_message_type",
        HeaderSyncWireError::UnknownFrameMessageType(_) => "unknown_frame_message_type",
        HeaderSyncWireError::UnsupportedFlags(_) => "unsupported_flags",
        HeaderSyncWireError::MismatchedFrameMessageType { .. } => "mismatched_frame_message_type",
        HeaderSyncWireError::TrailingBytes => "trailing_bytes",
        HeaderSyncWireError::NonContiguousHeaders => "non_contiguous_headers",
        HeaderSyncWireError::FirstHeaderDoesNotLink => "first_header_does_not_link",
        HeaderSyncWireError::WrongEquihashSolutionSize => "wrong_equihash_solution_size",
        HeaderSyncWireError::InvalidDifficultyThreshold => "invalid_difficulty_threshold",
        HeaderSyncWireError::DifficultyFilter { .. } => "difficulty_filter",
        HeaderSyncWireError::NumericOverflow(_) => "numeric_overflow",
        HeaderSyncWireError::Io(_) => "io",
        HeaderSyncWireError::Serialization(_) => "serialization",
        HeaderSyncWireError::Time(_) => "time",
        HeaderSyncWireError::Equihash(_) => "equihash",
        HeaderSyncWireError::BlockingTask(_) => "blocking_task",
    }
}

fn misbehavior_reason_label(reason: HeaderSyncMisbehavior) -> &'static str {
    match reason {
        HeaderSyncMisbehavior::InvalidStatus => "invalid_status",
        HeaderSyncMisbehavior::UnsolicitedHeaders => "unsolicited_headers",
        HeaderSyncMisbehavior::EmptyHeaders => "empty_headers",
        HeaderSyncMisbehavior::ResponseTooLong => "response_too_long",
        HeaderSyncMisbehavior::InvalidRange => "invalid_range",
        HeaderSyncMisbehavior::MalformedMessage => "malformed_message",
        HeaderSyncMisbehavior::StatusSpam => "status_spam",
        HeaderSyncMisbehavior::NewBlockSpam => "new_block_spam",
        HeaderSyncMisbehavior::GetHeadersSpam => "get_headers_spam",
        HeaderSyncMisbehavior::GetHeadersTooLong => "get_headers_too_long",
        HeaderSyncMisbehavior::UnknownPeer => "unknown_peer",
        HeaderSyncMisbehavior::InvalidNewBlock => "invalid_new_block",
    }
}

pub(super) fn commit_failure_reason_label(kind: HeaderSyncCommitFailureKind) -> &'static str {
    match kind {
        HeaderSyncCommitFailureKind::InvalidPeerRange => "invalid_peer_range",
        HeaderSyncCommitFailureKind::Local => "local",
    }
}

fn operation_kind_label(kind: HeaderSyncOperationKind) -> &'static str {
    match kind {
        HeaderSyncOperationKind::CommitHeaders => "commit_headers",
        HeaderSyncOperationKind::AuthenticateRoots => "authenticate_roots",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io, sync::Arc};

    use chrono::Utc;
    use tokio_util::sync::CancellationToken;
    use zakura_chain::{
        block::BlockTimeError,
        orchard,
        parameters::Network,
        sapling,
        serialization::{SerializationError, ZcashDeserializeInto},
        work::equihash,
    };
    use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;

    use crate::zakura::{
        framed_channel,
        header_sync::{
            events::{
                HeaderSyncFrontiers, HeaderSyncOperationIdentity, HeaderSyncOperationKind,
                HeaderSyncRequestId, HeaderSyncWireRequestIdentity,
            },
            service::HeaderSyncPeerSession,
            state::RangePriority,
            HeaderRangePayload,
        },
        HeaderSyncServiceSummary,
    };

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
    }

    fn block() -> Arc<block::Block> {
        BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .map(Arc::new)
            .expect("block test vector parses")
    }

    fn request_id() -> HeaderSyncRequestId {
        HeaderSyncRequestId::new(1).expect("test request id is non-zero")
    }

    fn operation(peer: ZakuraPeerId) -> HeaderSyncOperationIdentity {
        HeaderSyncOperationIdentity {
            wire_request: HeaderSyncWireRequestIdentity {
                peer,
                session_id: 7,
                request_id: request_id(),
            },
            op_kind: HeaderSyncOperationKind::CommitHeaders,
        }
    }

    fn root(height: block::Height) -> BlockCommitmentRoots {
        BlockCommitmentRoots {
            height,
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx: 0,
            orchard_tx: 0,
            ironwood_tx: 0,
            auth_data_root: block::merkle::AuthDataRoot::from([0; 32]),
        }
    }

    fn status() -> HeaderSyncStatus {
        HeaderSyncStatus {
            tip_height: block::Height(9),
            tip_hash: block::Hash([9; 32]),
            anchor_height: block::Height(3),
            max_headers_per_response: 17,
            max_inflight_requests: 2,
        }
    }

    fn row_kind(row: &serde_json::Map<String, Value>) -> &str {
        row.get(hs_trace::KIND)
            .and_then(Value::as_str)
            .expect("every event and action row has a kind")
    }

    fn event_row(event: &HeaderSyncEvent) -> serde_json::Map<String, Value> {
        serde_json::to_value(HeaderEventProjection::from(event))
            .expect("event projection serializes")
            .as_object()
            .expect("event projection is an object")
            .clone()
    }

    fn action_row(action: &HeaderSyncAction) -> serde_json::Map<String, Value> {
        serde_json::to_value(HeaderActionProjection::from(action))
            .expect("action projection serializes")
            .as_object()
            .expect("action projection is an object")
            .clone()
    }

    fn message_row(message: &HeaderSyncMessage) -> serde_json::Map<String, Value> {
        serde_json::to_value(HeaderMessageProjection::from(message))
            .expect("message projection serializes")
            .as_object()
            .expect("message projection is an object")
            .clone()
    }

    #[test]
    fn typed_outer_schemas_preserve_nulls_omissions_and_flattening() {
        let peer = peer(7);
        let received = HeaderTraceEvent::EventReceived {
            projection: HeaderEventProjection::PeerDisconnected {
                peer: peer_label(&peer),
            },
        };
        assert_eq!(
            serde_json::to_value(received).expect("received event serializes"),
            serde_json::json!({
                "event": hs_trace::HEADER_EVENT_RECEIVED,
                "kind": "peer_disconnected",
                "peer": peer_label(&peer),
            })
        );

        let committed = HeaderTraceEvent::RangeCommitted {
            range_start: block::Height(3),
            range_count: 2,
            reason: None,
        };
        assert_eq!(
            serde_json::to_value(committed).expect("committed range serializes"),
            serde_json::json!({
                "event": hs_trace::HEADER_RANGE_COMMITTED,
                "range_start": 3,
                "range_count": 2,
                "reason": null,
            })
        );

        let rejected = HeaderTraceEvent::RangeRejected {
            peer: Some(peer_label(&peer)),
            range_start: block::Height(4),
            range_count: 1,
            anchor_hash: None,
            validation_stage: None,
            error_kind: None,
            reason: Some("local"),
        };
        assert_eq!(
            serde_json::to_value(rejected).expect("rejected range serializes"),
            serde_json::json!({
                "event": hs_trace::HEADER_RANGE_REJECTED,
                "peer": peer_label(&peer),
                "range_start": 4,
                "range_count": 1,
                "reason": "local",
            })
        );

        let queue = QueueSendTraceEvent {
            event: "queue_send_failed",
            service: "header_sync",
            message: "headers",
            peer: peer_label(&peer),
            error: "full",
            queue_capacity: 0,
            queue_max_capacity: 8,
            context: QueueSendProjection::Headers {
                range_start: block::Height(5),
                range_count: 3,
                returned: 2,
            },
        };
        assert_eq!(
            serde_json::to_value(queue).expect("queue event serializes"),
            serde_json::json!({
                "event": "queue_send_failed",
                "service": "header_sync",
                "message": "headers",
                "peer": peer_label(&peer),
                "error": "full",
                "queue_capacity": 0,
                "queue_max_capacity": 8,
                "range_start": 5,
                "range_count": 3,
                "returned": 2,
            })
        );
    }

    #[test]
    fn every_event_metric_label_matches_its_trace_kind() {
        let peer = peer(1);
        let block = block();
        let header = block.header.clone();
        let hash = block.hash();
        let (send, _recv) = framed_channel(1);
        let session = HeaderSyncPeerSession::from_parts_with_direction(
            peer.clone(),
            ServicePeerDirection::Inbound,
            send,
            CancellationToken::new(),
        );
        let frontiers = HeaderSyncFrontiers {
            finalized_height: block::Height(2),
            verified_block_tip: block::Height(4),
            verified_block_hash: block::Hash([4; 32]),
        };
        let summary = HeaderSyncServiceSummary {
            best_height: block::Height(8),
            best_hash: block::Hash([8; 32]),
            finalized_height: Some(block::Height(2)),
            serving_headers: true,
            inbound_slots_free: 1,
            inbound_slots_max: 2,
            outbound_slots_free: 3,
            outbound_slots_max: 4,
        };
        let events = vec![
            HeaderSyncEvent::PeerConnected(session),
            HeaderSyncEvent::PeerDisconnected(peer.clone()),
            HeaderSyncEvent::AdvisoryHeaderSummary {
                peer: peer.clone(),
                summary,
            },
            HeaderSyncEvent::FullBlockCommitted {
                height: block::Height(1),
                hash,
            },
            HeaderSyncEvent::NewBlockAccepted {
                peer: peer.clone(),
                height: block::Height(1),
                hash,
                block: block.clone(),
            },
            HeaderSyncEvent::NewBlockDuplicate {
                peer: peer.clone(),
                height: block::Height(1),
                hash,
            },
            HeaderSyncEvent::NewBlockAcceptedNonBestChain {
                peer: peer.clone(),
                height: block::Height(1),
                hash,
            },
            HeaderSyncEvent::NewBlockRejected {
                peer: peer.clone(),
                hash,
            },
            HeaderSyncEvent::WireMessage {
                peer: peer.clone(),
                msg: HeaderSyncMessage::Status(status()),
            },
            HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 7,
                msg: HeaderSyncMessage::Status(status()),
            },
            HeaderSyncEvent::WireHeaders {
                peer: peer.clone(),
                session_id: 7,
                request_id: request_id(),
                headers: vec![header.clone()],
                body_sizes: vec![1],
                tree_aux_roots: Vec::new(),
            },
            HeaderSyncEvent::WireGetHeaders {
                peer: peer.clone(),
                session_id: 7,
                request_id: request_id(),
                start_height: block::Height(5),
                count: 6,
                want_tree_aux_roots: true,
            },
            HeaderSyncEvent::WireDecodeFailed {
                peer: peer.clone(),
                error: Arc::new(HeaderSyncWireError::TrailingBytes),
            },
            HeaderSyncEvent::WireProtocolFailure {
                peer: peer.clone(),
                reason: HeaderSyncMisbehavior::MalformedMessage,
                error: Arc::new(HeaderSyncWireError::TrailingBytes),
            },
            HeaderSyncEvent::StateFrontiersChanged(frontiers),
            HeaderSyncEvent::VctRootRepairRequested {
                height: block::Height(5),
                generation: 8,
                anchor_hash: block::Hash([4; 32]),
                expected_hashes: vec![(block::Height(5), block::Hash([5; 32]))],
            },
            HeaderSyncEvent::VctRootRepairResolved { generation: 8 },
            HeaderSyncEvent::BestHeaderTipLoaded {
                tip_height: block::Height(7),
                tip_hash: block::Hash([7; 32]),
            },
            HeaderSyncEvent::HeaderRangeOperationCompleted {
                operation: operation(peer.clone()),
                tip_hash: block::Hash([7; 32]),
            },
            HeaderSyncEvent::HeaderRangeOperationFailed {
                operation: operation(peer.clone()),
                kind: HeaderSyncCommitFailureKind::InvalidPeerRange,
            },
            HeaderSyncEvent::HeaderRangeResponseFinished {
                peer: peer.clone(),
                session_id: 7,
                request_id: request_id(),
                start_height: block::Height(5),
                requested_count: 3,
                returned_count: 2,
            },
            HeaderSyncEvent::HeaderRangeResponseReady {
                peer,
                session_id: 7,
                request_id: request_id(),
                start_height: block::Height(5),
                requested_count: 3,
                want_tree_aux_roots: false,
                headers: vec![header],
                body_sizes: vec![1],
                tree_aux_roots: Vec::new(),
            },
        ];

        for event in events {
            let row = event_row(&event);
            assert_eq!(row_kind(&row), event.metrics_label(), "{event:?}");
            assert!(
                !row.contains_key("block")
                    && !row.contains_key("headers")
                    && !row.contains_key("body_sizes")
                    && !row.contains_key("tree_aux_roots"),
                "trace row leaked payload fields: {row:?}"
            );
        }
    }

    #[test]
    fn every_production_and_test_action_has_the_expected_trace_kind() {
        let peer = peer(2);
        let block = block();
        let header = block.header.clone();
        let hash = block.hash();
        let actions = vec![
            (
                HeaderSyncAction::SendMessage {
                    peer: peer.clone(),
                    request_id: None,
                    msg: HeaderSyncMessage::Status(status()),
                },
                "send_message",
            ),
            (
                HeaderSyncAction::ForwardNewBlock {
                    source: Some(peer.clone()),
                    peer: peer.clone(),
                    height: block::Height(1),
                    hash,
                    block: block.clone(),
                },
                "forward_new_block",
            ),
            (
                HeaderSyncAction::CommitHeaderRange {
                    operation: operation(peer.clone()),
                    anchor: block::Hash([0; 32]),
                    payload: HeaderRangePayload::new(block::Height(1), vec![header], vec![1], None)
                        .expect("test payload is aligned"),
                    finalized: false,
                },
                "commit_header_range",
            ),
            (
                HeaderSyncAction::QueryBestHeaderTip,
                "query_best_header_tip",
            ),
            (
                HeaderSyncAction::QueryHeadersByHeightRange {
                    peer: peer.clone(),
                    session_id: 7,
                    request_id: request_id(),
                    start: block::Height(3),
                    count: 4,
                    want_tree_aux_roots: true,
                },
                "query_headers_by_height_range",
            ),
            (
                HeaderSyncAction::QueryMissingBlockBodies {
                    from: block::Height(3),
                    limit: 4,
                },
                "query_missing_block_bodies",
            ),
            (
                HeaderSyncAction::Misbehavior {
                    peer: peer.clone(),
                    reason: HeaderSyncMisbehavior::InvalidRange,
                },
                "misbehavior",
            ),
            (
                HeaderSyncAction::BodyGaps {
                    from: block::Height(3),
                    to: block::Height(5),
                },
                "body_gaps",
            ),
            (
                HeaderSyncAction::HeaderAdvanced {
                    height: block::Height(5),
                    hash,
                },
                "header_advanced",
            ),
            (
                HeaderSyncAction::HeaderReanchored {
                    old: (block::Height(4), block::Hash([4; 32])),
                    new: (block::Height(5), hash),
                },
                "header_reanchored",
            ),
            (
                HeaderSyncAction::NewBlockReceived {
                    peer,
                    height: block::Height(1),
                    hash,
                    block,
                },
                "new_block_received",
            ),
        ];

        for (action, expected_kind) in actions {
            let row = action_row(&action);
            assert_eq!(row_kind(&row), expected_kind, "{action:?}");
            assert!(
                !row.contains_key("block")
                    && !row.contains_key("headers")
                    && !row.contains_key("body_sizes")
                    && !row.contains_key("tree_aux_roots"),
                "trace row leaked payload fields: {row:?}"
            );
        }
    }

    #[test]
    fn operation_identity_events_preserve_exact_field_order() {
        let peer = peer(6);
        let operation = operation(peer.clone());
        let tip_hash = block::Hash([7; 32]);
        let completed = HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation: operation.clone(),
            tip_hash,
        };
        assert_eq!(
            serde_json::to_string(&HeaderEventProjection::from(&completed))
                .expect("completed operation projection serializes"),
            format!(
                r#"{{"kind":"header_range_operation_completed","peer":"{}","session_id":7,"request_id":1,"operation_kind":"commit_headers","hash":"{}"}}"#,
                peer_label(&peer),
                hash_label(tip_hash),
            )
        );

        let failed = HeaderSyncEvent::HeaderRangeOperationFailed {
            operation: operation.clone(),
            kind: HeaderSyncCommitFailureKind::InvalidPeerRange,
        };
        assert_eq!(
            serde_json::to_string(&HeaderEventProjection::from(&failed))
                .expect("failed operation projection serializes"),
            format!(
                r#"{{"kind":"header_range_operation_failed","peer":"{}","session_id":7,"request_id":1,"operation_kind":"commit_headers","reason":"invalid_peer_range"}}"#,
                peer_label(&peer),
            )
        );

        let payload = HeaderRangePayload::new(
            block::Height(1),
            vec![block().header.clone()],
            vec![1],
            None,
        )
        .expect("test payload is aligned");
        let action = HeaderSyncAction::CommitHeaderRange {
            operation,
            anchor: block::Hash([0; 32]),
            payload,
            finalized: false,
        };
        assert_eq!(
            serde_json::to_string(&HeaderActionProjection::from(&action))
                .expect("commit action projection serializes"),
            format!(
                r#"{{"kind":"commit_header_range","peer":"{}","session_id":7,"request_id":1,"operation_kind":"commit_headers","range_start":1,"range_count":1}}"#,
                peer_label(&peer),
            )
        );
    }

    #[test]
    fn message_labels_and_fields_are_exact_and_omit_payloads() {
        let block = block();
        let block_hash = format!("{}", block.hash());
        let messages = [
            HeaderSyncMessage::Status(status()),
            HeaderSyncMessage::Headers {
                headers: vec![block.header.clone()],
                body_sizes: vec![123],
                tree_aux_roots: Vec::new(),
            },
            HeaderSyncMessage::GetHeaders {
                start_height: block::Height(4),
                count: 5,
                want_tree_aux_roots: true,
            },
            HeaderSyncMessage::NewBlock(block),
        ];
        let expected = [
            (
                "status",
                vec![
                    (hs_trace::HEIGHT, Value::from(9)),
                    (
                        hs_trace::HASH,
                        Value::from(format!("{}", block::Hash([9; 32]))),
                    ),
                    (hs_trace::RANGE_START, Value::from(3)),
                    (hs_trace::ADVERTISED_CAP, Value::from(17)),
                    (hs_trace::IN_FLIGHT_COUNT, Value::from(2)),
                ],
            ),
            ("headers", vec![(hs_trace::RANGE_COUNT, Value::from(1))]),
            (
                "get_headers",
                vec![
                    (hs_trace::RANGE_START, Value::from(4)),
                    (hs_trace::RANGE_COUNT, Value::from(5)),
                ],
            ),
            (
                "new_block",
                vec![
                    (hs_trace::HASH, Value::from(block_hash)),
                    (hs_trace::HEIGHT, Value::from(1)),
                ],
            ),
        ];

        for (index, message) in messages.iter().enumerate() {
            let row = message_row(message);
            assert_eq!(header_sync_message_label(message), expected[index].0);
            let expected_row: serde_json::Map<_, _> = expected[index]
                .1
                .iter()
                .map(|(key, value)| ((*key).to_string(), value.clone()))
                .collect();
            assert_eq!(row, expected_row);
            assert!(!row.contains_key("block") && !row.contains_key("headers"));
        }
    }

    #[tokio::test]
    async fn every_wire_error_has_a_stable_kind() {
        let threshold = block()
            .header
            .difficulty_threshold
            .to_expanded()
            .expect("test block difficulty expands");
        let now = Utc::now();
        let errors = vec![
            (
                HeaderSyncWireError::OversizedPayload { actual: 2, max: 1 },
                "oversized_payload",
            ),
            (
                HeaderSyncWireError::HeaderCountLimit { actual: 2, max: 1 },
                "header_count_limit",
            ),
            (
                HeaderSyncWireError::InvalidRangeGeometry {
                    start: block::Height(1),
                    count: 0,
                },
                "invalid_range_geometry",
            ),
            (
                HeaderSyncWireError::BodySizeCountMismatch {
                    headers: 2,
                    body_sizes: 1,
                },
                "body_size_count_mismatch",
            ),
            (
                HeaderSyncWireError::TreeAuxRootCountMismatch {
                    headers: 2,
                    roots: 1,
                },
                "tree_aux_root_count_mismatch",
            ),
            (
                HeaderSyncWireError::TreeAuxRootHeightMismatch {
                    offset: 1,
                    expected_height: block::Height(2),
                    root_height: block::Height(3),
                    first_root_height: block::Height(3),
                    last_root_height: block::Height(4),
                },
                "tree_aux_root_height_mismatch",
            ),
            (
                HeaderSyncWireError::InvalidBoolMarker {
                    field: "flag",
                    value: 2,
                },
                "invalid_bool_marker",
            ),
            (
                HeaderSyncWireError::UnrequestedTreeAuxRoots,
                "unrequested_tree_aux_roots",
            ),
            (
                HeaderSyncWireError::UnsolicitedHeaders,
                "unsolicited_headers",
            ),
            (
                HeaderSyncWireError::MissingRequestId { message: "Headers" },
                "missing_request_id",
            ),
            (
                HeaderSyncWireError::ZeroHeaderRequestCount,
                "zero_header_request_count",
            ),
            (
                HeaderSyncWireError::HeightOutOfRange(u32::MAX),
                "height_out_of_range",
            ),
            (
                HeaderSyncWireError::UnknownMessageType(9),
                "unknown_message_type",
            ),
            (
                HeaderSyncWireError::UnknownFrameMessageType(9),
                "unknown_frame_message_type",
            ),
            (
                HeaderSyncWireError::UnsupportedFlags(1),
                "unsupported_flags",
            ),
            (
                HeaderSyncWireError::MismatchedFrameMessageType {
                    frame: 1,
                    payload: 2,
                },
                "mismatched_frame_message_type",
            ),
            (HeaderSyncWireError::TrailingBytes, "trailing_bytes"),
            (
                HeaderSyncWireError::NonContiguousHeaders,
                "non_contiguous_headers",
            ),
            (
                HeaderSyncWireError::FirstHeaderDoesNotLink,
                "first_header_does_not_link",
            ),
            (
                HeaderSyncWireError::WrongEquihashSolutionSize,
                "wrong_equihash_solution_size",
            ),
            (
                HeaderSyncWireError::InvalidDifficultyThreshold,
                "invalid_difficulty_threshold",
            ),
            (
                HeaderSyncWireError::DifficultyFilter {
                    hash: block::Hash([1; 32]),
                    threshold,
                },
                "difficulty_filter",
            ),
            (
                HeaderSyncWireError::NumericOverflow("test"),
                "numeric_overflow",
            ),
            (HeaderSyncWireError::Io(io::Error::other("test")), "io"),
            (
                HeaderSyncWireError::Serialization(SerializationError::Parse("test")),
                "serialization",
            ),
            (
                HeaderSyncWireError::Time(BlockTimeError::InvalidBlockTime(
                    now,
                    block::Height(1),
                    block::Hash([1; 32]),
                    now,
                )),
                "time",
            ),
            (
                HeaderSyncWireError::Equihash(equihash::Error::InvalidSolutionSize {
                    network: Network::Mainnet,
                }),
                "equihash",
            ),
        ];
        for (error, expected) in errors {
            assert_eq!(header_sync_wire_error_kind(&error), expected);
        }

        let join_error = tokio::spawn(async { panic!("intentional test panic") })
            .await
            .expect_err("panicking task returns a join error");
        assert_eq!(
            header_sync_wire_error_kind(&HeaderSyncWireError::BlockingTask(join_error)),
            "blocking_task"
        );
    }

    #[test]
    fn labels_nulls_structured_errors_ranges_and_root_summaries_are_stable() {
        let misbehaviors = [
            (HeaderSyncMisbehavior::InvalidStatus, "invalid_status"),
            (
                HeaderSyncMisbehavior::UnsolicitedHeaders,
                "unsolicited_headers",
            ),
            (HeaderSyncMisbehavior::EmptyHeaders, "empty_headers"),
            (HeaderSyncMisbehavior::ResponseTooLong, "response_too_long"),
            (HeaderSyncMisbehavior::InvalidRange, "invalid_range"),
            (HeaderSyncMisbehavior::MalformedMessage, "malformed_message"),
            (HeaderSyncMisbehavior::StatusSpam, "status_spam"),
            (HeaderSyncMisbehavior::NewBlockSpam, "new_block_spam"),
            (HeaderSyncMisbehavior::GetHeadersSpam, "get_headers_spam"),
            (
                HeaderSyncMisbehavior::GetHeadersTooLong,
                "get_headers_too_long",
            ),
            (HeaderSyncMisbehavior::UnknownPeer, "unknown_peer"),
            (HeaderSyncMisbehavior::InvalidNewBlock, "invalid_new_block"),
        ];
        for (reason, expected) in misbehaviors {
            assert_eq!(misbehavior_reason_label(reason), expected);
        }
        assert_eq!(
            commit_failure_reason_label(HeaderSyncCommitFailureKind::InvalidPeerRange),
            "invalid_peer_range"
        );
        assert_eq!(
            commit_failure_reason_label(HeaderSyncCommitFailureKind::Local),
            "local"
        );

        let committed = serde_json::to_value(HeaderTraceEvent::RangeCommitted {
            range_start: block::Height(1),
            range_count: 1,
            reason: None,
        })
        .expect("range event serializes");
        assert_eq!(committed[hs_trace::REASON], Value::Null);

        let no_source = action_row(&HeaderSyncAction::ForwardNewBlock {
            source: None,
            peer: peer(3),
            height: block::Height(1),
            hash: block().hash(),
            block: block(),
        });
        assert!(!no_source.contains_key(hs_trace::SOURCE_PEER));

        let mismatch = HeaderSyncWireError::TreeAuxRootHeightMismatch {
            offset: 7,
            expected_height: block::Height(8),
            root_height: block::Height(9),
            first_root_height: block::Height(10),
            last_root_height: block::Height(11),
        };
        let row = serde_json::to_value(WireErrorProjection::from(&mismatch))
            .expect("wire error projection serializes");
        assert_eq!(row[hs_trace::ROOT_MISMATCH_OFFSET], Value::from(7));
        assert_eq!(row[hs_trace::EXPECTED_ROOT_HEIGHT], Value::from(8));
        assert_eq!(row[hs_trace::ACTUAL_ROOT_HEIGHT], Value::from(9));
        assert_eq!(row[hs_trace::FIRST_ROOT_HEIGHT], Value::from(10));
        assert_eq!(row[hs_trace::LAST_ROOT_HEIGHT], Value::from(11));

        assert_eq!(count_between(block::Height(3), block::Height(5)), 3);
        assert_eq!(count_between(block::Height(5), block::Height(3)), 0);
        let committed = event_row(&HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation: operation(peer(4)),
            tip_hash: block::Hash([5; 32]),
        });
        assert_eq!(committed[hs_trace::SESSION_ID], Value::from(7));
        assert_eq!(committed[hs_trace::REQUEST_ID], Value::from(1));
        assert_eq!(committed["operation_kind"], "commit_headers");
        let gaps = action_row(&HeaderSyncAction::BodyGaps {
            from: block::Height(3),
            to: block::Height(5),
        });
        assert_eq!(gaps[hs_trace::RANGE_COUNT], Value::from(3));

        let empty = serde_json::to_value(TreeAuxTraceSummary::default())
            .expect("empty root summary serializes");
        assert_eq!(empty, serde_json::json!({ "tree_aux_roots_len": 0 }));

        let roots = TreeAuxTraceSummary::new(&[root(block::Height(8)), root(block::Height(9))]);
        assert_eq!(roots.len, 2);
        let roots = serde_json::to_value(roots).expect("root summary serializes");
        assert_eq!(roots[hs_trace::FIRST_ROOT_HEIGHT], Value::from(8));
        assert_eq!(roots[hs_trace::LAST_ROOT_HEIGHT], Value::from(9));

        assert_eq!(RangePriority::Forward.label(), "forward");
        assert_eq!(RangePriority::Repair.label(), "repair");
    }
}
