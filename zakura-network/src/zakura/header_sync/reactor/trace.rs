use serde_json::{Number, Value};
use zakura_chain::{block, parallel::commitment_aux::BlockCommitmentRoots};

use super::super::{
    config::HeaderSyncStatus,
    error::HeaderSyncWireError,
    events::{
        HeaderRootAuthenticationFailureKind, HeaderSyncAction, HeaderSyncCommitFailureKind,
        HeaderSyncEvent, HeaderSyncMisbehavior, HeaderSyncOperationKind, HeaderSyncRequestId,
    },
    state::RangeRequest,
    validation::count_between,
    wire::HeaderSyncMessage,
};
use super::HeaderSyncReactor;
use crate::zakura::{
    trace::{
        header_sync_trace as hs_trace, ordered_send_error_label, peer_label as trace_peer_label,
        queue_send_trace as qs_trace, HEADER_SYNC_TABLE, QUEUE_SEND_TABLE,
    },
    OrderedSendError, ServicePeerDirection, ZakuraPeerId,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct GetHeadersTraceMeta {
    pub(super) request_id: HeaderSyncRequestId,
    pub(super) session_id: u64,
    pub(super) stream_version: u16,
}

#[derive(Default)]
pub(super) struct TreeAuxTraceSummary {
    len: u32,
    first_height: Option<block::Height>,
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

    fn insert_into(&self, row: &mut serde_json::Map<String, Value>) {
        if let Some(height) = self.first_height {
            insert_height(row, hs_trace::FIRST_ROOT_HEIGHT, height);
        }
        if let Some(height) = self.last_height {
            insert_height(row, hs_trace::LAST_ROOT_HEIGHT, height);
        }
    }
}

impl HeaderSyncReactor {
    pub(super) fn trace_event_received(&self, event: &HeaderSyncEvent) {
        self.emit_trace(hs_trace::HEADER_EVENT_RECEIVED, |row| {
            trace_event_fields(row, event);
        });
    }

    pub(super) fn trace_action_dispatched(&self, action: &HeaderSyncAction) {
        self.emit_trace(hs_trace::HEADER_ACTION_DISPATCHED, |row| {
            trace_action_fields(row, action);
        });
    }

    pub(super) fn trace_status_sent(&self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        self.emit_trace(hs_trace::HEADER_STATUS_SENT, |row| {
            trace_status_fields(row, peer, status);
        });
    }

    pub(super) fn trace_status_received(&self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        self.emit_trace(hs_trace::HEADER_STATUS_RECEIVED, |row| {
            trace_status_fields(row, peer, status);
        });
    }

    pub(super) fn trace_peer_connected(
        &self,
        peer: &ZakuraPeerId,
        direction: ServicePeerDirection,
        active_connections: usize,
    ) {
        self.emit_trace(hs_trace::HEADER_PEER_CONNECTED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(row, "direction", Some(direction.trace_label()));
            insert_u64(
                row,
                hs_trace::ACTIVE_CONNECTIONS,
                u64::try_from(active_connections).unwrap_or(u64::MAX),
            );
        });
    }

    pub(super) fn trace_peer_disconnected(&self, peer: &ZakuraPeerId, active_connections: usize) {
        self.emit_trace(hs_trace::HEADER_PEER_DISCONNECTED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_u64(
                row,
                hs_trace::ACTIVE_CONNECTIONS,
                u64::try_from(active_connections).unwrap_or(u64::MAX),
            );
        });
    }

    pub(super) fn trace_get_headers_sent(
        &self,
        peer: &ZakuraPeerId,
        range: RangeRequest,
        count: u32,
        advertised_cap: u32,
        meta: GetHeadersTraceMeta,
    ) {
        self.emit_trace(hs_trace::HEADER_GET_HEADERS_SENT, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_u64(row, hs_trace::SESSION_ID, meta.session_id);
            insert_u64(
                row,
                hs_trace::STREAM_VERSION,
                u64::from(meta.stream_version),
            );
            insert_u64(row, hs_trace::REQUEST_ID, meta.request_id.get());
            insert_height(row, hs_trace::RANGE_START, range.start_height());
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_u64(row, hs_trace::ADVERTISED_CAP, u64::from(advertised_cap));
            insert_bool(row, hs_trace::FINALIZED, range.finalized);
            insert_bool(
                row,
                hs_trace::WANT_TREE_AUX_ROOTS,
                range.want_tree_aux_roots,
            );
            insert_optional_str(row, hs_trace::RANGE_PRIORITY, Some(range.priority.label()));
            insert_height(
                row,
                hs_trace::VERIFIED_BLOCK_TIP,
                self.state.verified_block_tip,
            );
            insert_height(row, hs_trace::FINALIZED_HEIGHT, self.state.finalized_height);
            insert_height(row, hs_trace::BEST_HEADER_TIP, self.state.best_header_tip);
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn trace_headers_received(
        &self,
        peer: &ZakuraPeerId,
        start_height: block::Height,
        count: u32,
        expected_count: u32,
        advertised_cap: u32,
        in_flight_count: usize,
        want_tree_aux_roots: bool,
        tree_aux_roots: &[BlockCommitmentRoots],
    ) {
        self.emit_trace(hs_trace::HEADER_HEADERS_RECEIVED, |row| {
            let roots = TreeAuxTraceSummary::new(tree_aux_roots);
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_u64(row, hs_trace::ADVERTISED_CAP, u64::from(advertised_cap));
            insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(expected_count));
            insert_u64(row, hs_trace::IN_FLIGHT_COUNT, in_flight_count as u64);
            insert_bool(row, hs_trace::WANT_TREE_AUX_ROOTS, want_tree_aux_roots);
            insert_u64(row, hs_trace::TREE_AUX_ROOTS_LEN, u64::from(roots.len));
            roots.insert_into(row);
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
        self.emit_trace(hs_trace::HEADER_HEADERS_SERVED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(returned_count));
            insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(requested_count));
            insert_bool(row, hs_trace::WANT_TREE_AUX_ROOTS, want_tree_aux_roots);
            insert_u64(row, hs_trace::TREE_AUX_ROOTS_LEN, u64::from(roots.len));
            roots.insert_into(row);
        });
    }

    pub(super) fn trace_range_event(
        &self,
        event: &'static str,
        start_height: block::Height,
        count: u32,
        peer: Option<&ZakuraPeerId>,
        reason: Option<&'static str>,
    ) {
        self.emit_trace(event, |row| {
            if let Some(peer) = peer {
                insert_peer(row, hs_trace::PEER, peer);
            }
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_optional_str(row, hs_trace::REASON, reason);
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
        self.emit_trace(hs_trace::HEADER_RANGE_REJECTED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, range.start_height());
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            if let Some(anchor_hash) = range.anchor_hash {
                insert_hash(row, hs_trace::ANCHOR_HASH, anchor_hash);
            }
            insert_optional_str(row, hs_trace::VALIDATION_STAGE, Some(validation_stage));
            insert_optional_str(row, hs_trace::ERROR_KIND, Some(error_kind));
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(
                    HeaderSyncMisbehavior::InvalidRange,
                )),
            );
        });
    }

    pub(super) fn trace_new_block_received(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    ) {
        self.emit_trace(hs_trace::HEADER_NEW_BLOCK_RECEIVED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
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
        self.emit_trace(hs_trace::HEADER_NEW_BLOCK_FORWARDED, |row| {
            insert_peer(row, hs_trace::SOURCE_PEER, source);
            insert_peer(row, hs_trace::PEER, destination);
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
            insert_u64(
                row,
                hs_trace::DESTINATION_PEER_COUNT,
                destination_count as u64,
            );
        });
    }

    pub(super) fn trace_new_block_deduped(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        reason: &'static str,
    ) {
        self.emit_trace(hs_trace::HEADER_NEW_BLOCK_DEDUPED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
            insert_optional_str(row, hs_trace::REASON, Some(reason));
        });
    }

    pub(super) fn trace_peer_violation(&self, peer: &ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        self.emit_trace(hs_trace::HEADER_PEER_VIOLATION, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(reason)),
            );
        });
    }

    pub(super) fn trace_peer_violation_recorded(
        &self,
        peer: &ZakuraPeerId,
        reason: HeaderSyncMisbehavior,
    ) {
        self.emit_trace(hs_trace::HEADER_PEER_VIOLATION_RECORDED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(reason)),
            );
        });
    }

    pub(super) fn trace_frontier_advanced(&self, height: block::Height, hash: block::Hash) {
        self.emit_trace(hs_trace::HEADER_FRONTIER_ADVANCED, |row| {
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
        });
    }

    pub(super) fn trace_frontier_reanchored(&self, height: block::Height, hash: block::Hash) {
        self.emit_trace(hs_trace::HEADER_FRONTIER_REANCHORED, |row| {
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
        });
    }

    pub(super) fn trace_missing_bodies(&self, from: block::Height, to: block::Height) {
        self.emit_trace(hs_trace::HEADER_MISSING_BODIES_REPORTED, |row| {
            insert_height(row, hs_trace::RANGE_START, from);
            insert_u64(
                row,
                hs_trace::RANGE_COUNT,
                u64::from(count_between(from, to)),
            );
        });
    }

    pub(super) fn trace_queue_send_failed(
        &self,
        peer: &ZakuraPeerId,
        message: &'static str,
        error: &OrderedSendError,
        queue_capacity: usize,
        queue_max_capacity: usize,
        build: impl FnOnce(&mut serde_json::Map<String, Value>),
    ) {
        self.startup.trace.emit_with(QUEUE_SEND_TABLE, |row| {
            row.insert(
                qs_trace::EVENT.to_string(),
                Value::String(qs_trace::QUEUE_SEND_FAILED.to_string()),
            );
            insert_optional_str(row, qs_trace::SERVICE, Some("header_sync"));
            insert_optional_str(row, qs_trace::MESSAGE, Some(message));
            insert_peer(row, qs_trace::PEER, peer);
            insert_optional_str(row, qs_trace::ERROR, Some(ordered_send_error_label(error)));
            insert_u64(
                row,
                qs_trace::QUEUE_CAPACITY,
                u64::try_from(queue_capacity).unwrap_or(u64::MAX),
            );
            insert_u64(
                row,
                qs_trace::QUEUE_MAX_CAPACITY,
                u64::try_from(queue_max_capacity).unwrap_or(u64::MAX),
            );
            build(row);
        });
    }

    pub(super) fn emit_trace(
        &self,
        event: &'static str,
        build: impl FnOnce(&mut serde_json::Map<String, Value>),
    ) {
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.to_string(),
                Value::String(event.to_string()),
            );
            build(row);
        });
    }
}

fn trace_event_fields(row: &mut serde_json::Map<String, Value>, event: &HeaderSyncEvent) {
    match event {
        HeaderSyncEvent::PeerConnected(session) => {
            insert_optional_str(row, hs_trace::KIND, Some("peer_connected"));
            insert_peer(row, hs_trace::PEER, session.peer_id());
        }
        HeaderSyncEvent::PeerDisconnected(peer) => {
            insert_optional_str(row, hs_trace::KIND, Some("peer_disconnected"));
            insert_peer(row, hs_trace::PEER, peer);
        }
        HeaderSyncEvent::AdvisoryHeaderSummary { peer, summary } => {
            insert_optional_str(row, hs_trace::KIND, Some("advisory_header_summary"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, summary.best_height);
        }
        HeaderSyncEvent::FullBlockCommitted { height, hash, .. } => {
            insert_optional_str(row, hs_trace::KIND, Some("full_block_committed"));
            insert_height(row, hs_trace::HEIGHT, *height);
            insert_hash(row, hs_trace::HASH, *hash);
        }
        HeaderSyncEvent::NewBlockAccepted {
            peer, height, hash, ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("new_block_accepted"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, *height);
            insert_hash(row, hs_trace::HASH, *hash);
        }
        HeaderSyncEvent::NewBlockDuplicate { peer, height, hash } => {
            insert_optional_str(row, hs_trace::KIND, Some("new_block_duplicate"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, *height);
            insert_hash(row, hs_trace::HASH, *hash);
        }
        HeaderSyncEvent::NewBlockAcceptedNonBestChain { peer, height, hash } => {
            insert_optional_str(
                row,
                hs_trace::KIND,
                Some("new_block_accepted_non_best_chain"),
            );
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, *height);
            insert_hash(row, hs_trace::HASH, *hash);
        }
        HeaderSyncEvent::NewBlockRejected { peer, hash } => {
            insert_optional_str(row, hs_trace::KIND, Some("new_block_rejected"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_hash(row, hs_trace::HASH, *hash);
        }
        #[cfg(test)]
        HeaderSyncEvent::WireMessage { peer, msg } => {
            insert_optional_str(row, hs_trace::KIND, Some("wire_message"));
            insert_optional_str(row, hs_trace::REASON, Some(header_sync_message_label(msg)));
            insert_peer(row, hs_trace::PEER, peer);
            trace_header_sync_message_fields(row, msg);
        }
        HeaderSyncEvent::SessionWireMessage { peer, msg, .. } => {
            insert_optional_str(row, hs_trace::KIND, Some("session_wire_message"));
            insert_optional_str(row, hs_trace::REASON, Some(header_sync_message_label(msg)));
            insert_peer(row, hs_trace::PEER, peer);
            trace_header_sync_message_fields(row, msg);
        }
        HeaderSyncEvent::WireHeaders {
            wire_request,
            entries,
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("wire_headers"));
            insert_peer(row, hs_trace::PEER, &wire_request.peer);
            insert_u64(row, hs_trace::RANGE_COUNT, entries.len() as u64);
        }
        HeaderSyncEvent::WireGetHeaders {
            peer,
            start_height,
            count,
            ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("wire_get_headers"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, *start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
        }
        HeaderSyncEvent::WireDecodeFailed { peer, error } => {
            insert_optional_str(row, hs_trace::KIND, Some("wire_decode_failed"));
            insert_optional_str(
                row,
                hs_trace::ERROR_KIND,
                Some(header_sync_wire_error_kind(error)),
            );
            insert_peer(row, hs_trace::PEER, peer);
            trace_wire_error_fields(row, error);
        }
        HeaderSyncEvent::WireProtocolFailure {
            peer,
            reason,
            error,
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("wire_protocol_failure"));
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(*reason)),
            );
            insert_optional_str(
                row,
                hs_trace::ERROR_KIND,
                Some(header_sync_wire_error_kind(error)),
            );
            insert_peer(row, hs_trace::PEER, peer);
            trace_wire_error_fields(row, error);
        }
        HeaderSyncEvent::StateFrontiersChanged(frontiers) => {
            insert_optional_str(row, hs_trace::KIND, Some("state_frontiers_changed"));
            insert_height(row, "finalized_height", frontiers.finalized_height);
            insert_height(row, "verified_block_tip", frontiers.verified_block_tip);
        }
        HeaderSyncEvent::HeaderRootAuthStateChanged(state) => {
            insert_optional_str(row, hs_trace::KIND, Some("header_root_auth_state_changed"));
            if let Some(state) = state {
                insert_height(row, hs_trace::HEIGHT, state.authenticated_height);
                insert_height(
                    row,
                    "completed_checkpoint_height",
                    state.completed_checkpoint_height,
                );
            }
        }
        HeaderSyncEvent::VctRootRepairRequested {
            height,
            generation,
            expected_hashes,
            ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("vct_root_repair_requested"));
            insert_height(row, hs_trace::HEIGHT, *height);
            insert_u64(row, hs_trace::RANGE_COUNT, expected_hashes.len() as u64);
            insert_u64(row, "generation", *generation);
        }
        HeaderSyncEvent::VctRootRepairResolved { generation } => {
            insert_optional_str(row, hs_trace::KIND, Some("vct_root_repair_resolved"));
            insert_u64(row, "generation", *generation);
        }
        HeaderSyncEvent::BestHeaderTipLoaded {
            tip_height,
            tip_hash,
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("best_header_tip_loaded"));
            insert_height(row, hs_trace::HEIGHT, *tip_height);
            insert_hash(row, hs_trace::HASH, *tip_hash);
        }
        HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation,
            tip_hash,
        } => {
            insert_optional_str(
                row,
                hs_trace::KIND,
                Some("header_range_operation_completed"),
            );
            insert_peer(row, hs_trace::PEER, &operation.wire_request.peer);
            insert_u64(row, hs_trace::SESSION_ID, operation.wire_request.session_id);
            insert_u64(
                row,
                hs_trace::REQUEST_ID,
                operation.wire_request.request_id.get(),
            );
            insert_optional_str(
                row,
                "operation_kind",
                Some(operation_kind_label(operation.op_kind)),
            );
            insert_hash(row, hs_trace::HASH, *tip_hash);
        }
        HeaderSyncEvent::HeaderRangeOperationFailed { operation, kind } => {
            insert_optional_str(row, hs_trace::KIND, Some("header_range_operation_failed"));
            insert_peer(row, hs_trace::PEER, &operation.wire_request.peer);
            insert_u64(row, hs_trace::SESSION_ID, operation.wire_request.session_id);
            insert_u64(
                row,
                hs_trace::REQUEST_ID,
                operation.wire_request.request_id.get(),
            );
            insert_optional_str(
                row,
                "operation_kind",
                Some(operation_kind_label(operation.op_kind)),
            );
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(commit_failure_reason_label(*kind)),
            );
        }
        HeaderSyncEvent::HeaderRootAuthenticationCompleted { operation } => {
            insert_optional_str(
                row,
                hs_trace::KIND,
                Some("header_root_authentication_completed"),
            );
            insert_peer(row, hs_trace::PEER, &operation.wire_request.peer);
            insert_u64(row, hs_trace::SESSION_ID, operation.wire_request.session_id);
            insert_u64(
                row,
                hs_trace::REQUEST_ID,
                operation.wire_request.request_id.get(),
            );
        }
        HeaderSyncEvent::HeaderRootAuthenticationFailed { operation, kind } => {
            insert_optional_str(
                row,
                hs_trace::KIND,
                Some("header_root_authentication_failed"),
            );
            insert_peer(row, hs_trace::PEER, &operation.wire_request.peer);
            insert_u64(row, hs_trace::SESSION_ID, operation.wire_request.session_id);
            insert_u64(
                row,
                hs_trace::REQUEST_ID,
                operation.wire_request.request_id.get(),
            );
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(match kind {
                    HeaderRootAuthenticationFailureKind::Stale => "stale",
                    HeaderRootAuthenticationFailureKind::InvalidPeerRange => "invalid_peer_range",
                    HeaderRootAuthenticationFailureKind::Local => "local",
                }),
            );
        }
        HeaderSyncEvent::HeaderRangeResponseFinished {
            peer,
            start_height,
            requested_count,
            returned_count,
            ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("header_range_response_finished"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, *start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*returned_count));
            insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(*requested_count));
        }
        HeaderSyncEvent::HeaderRangeResponseReady {
            peer,
            start_height,
            requested_count,
            headers,
            ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("header_range_response_ready"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, *start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, headers.len() as u64);
            insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(*requested_count));
        }
    }
}

fn trace_action_fields(row: &mut serde_json::Map<String, Value>, action: &HeaderSyncAction) {
    match action {
        #[cfg(test)]
        HeaderSyncAction::SendMessage { peer, msg, .. } => {
            insert_optional_str(row, hs_trace::KIND, Some("send_message"));
            insert_optional_str(row, hs_trace::REASON, Some(header_sync_message_label(msg)));
            insert_peer(row, hs_trace::PEER, peer);
            trace_header_sync_message_fields(row, msg);
        }
        #[cfg(test)]
        HeaderSyncAction::ForwardNewBlock {
            source,
            peer,
            height,
            hash,
            ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("forward_new_block"));
            if let Some(source) = source {
                insert_peer(row, hs_trace::SOURCE_PEER, source);
            }
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, *height);
            insert_hash(row, hs_trace::HASH, *hash);
        }
        HeaderSyncAction::Misbehavior { peer, reason } => {
            insert_optional_str(row, hs_trace::KIND, Some("misbehavior"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(*reason)),
            );
        }
        HeaderSyncAction::NewBlockReceived {
            peer, height, hash, ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("new_block_received"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, *height);
            insert_hash(row, hs_trace::HASH, *hash);
        }
        HeaderSyncAction::QueryHeadersByHeightRange {
            peer, start, count, ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("query_headers_by_height_range"));
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, *start);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
        }
        HeaderSyncAction::CommitHeaderRange {
            operation, payload, ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("commit_header_range"));
            insert_peer(row, hs_trace::PEER, &operation.wire_request.peer);
            insert_u64(row, hs_trace::SESSION_ID, operation.wire_request.session_id);
            insert_u64(
                row,
                hs_trace::REQUEST_ID,
                operation.wire_request.request_id.get(),
            );
            insert_optional_str(
                row,
                "operation_kind",
                Some(operation_kind_label(operation.op_kind)),
            );
            insert_height(row, hs_trace::RANGE_START, payload.range().start());
            insert_u64(
                row,
                hs_trace::RANGE_COUNT,
                u64::from(payload.range().count()),
            );
        }
        HeaderSyncAction::AuthenticateHeaderRoots {
            operation, payload, ..
        } => {
            insert_optional_str(row, hs_trace::KIND, Some("authenticate_header_roots"));
            insert_peer(row, hs_trace::PEER, &operation.wire_request.peer);
            insert_u64(row, hs_trace::SESSION_ID, operation.wire_request.session_id);
            insert_u64(
                row,
                hs_trace::REQUEST_ID,
                operation.wire_request.request_id.get(),
            );
            insert_height(row, hs_trace::RANGE_START, payload.range().start());
            insert_u64(
                row,
                hs_trace::RANGE_COUNT,
                u64::from(payload.range().count()),
            );
        }
        HeaderSyncAction::QueryBestHeaderTip => {
            insert_optional_str(row, hs_trace::KIND, Some("query_best_header_tip"));
        }
        HeaderSyncAction::QueryMissingBlockBodies { from, limit } => {
            insert_optional_str(row, hs_trace::KIND, Some("query_missing_block_bodies"));
            insert_height(row, hs_trace::RANGE_START, *from);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*limit));
        }
        HeaderSyncAction::BodyGaps { from, to } => {
            insert_optional_str(row, hs_trace::KIND, Some("body_gaps"));
            insert_height(row, hs_trace::RANGE_START, *from);
            insert_u64(
                row,
                hs_trace::RANGE_COUNT,
                u64::from(count_between(*from, *to)),
            );
        }
        HeaderSyncAction::HeaderAdvanced { height, hash } => {
            insert_optional_str(row, hs_trace::KIND, Some("header_advanced"));
            insert_height(row, hs_trace::HEIGHT, *height);
            insert_hash(row, hs_trace::HASH, *hash);
        }
        HeaderSyncAction::HeaderReanchored { old, new } => {
            insert_optional_str(row, hs_trace::KIND, Some("header_reanchored"));
            insert_height(row, hs_trace::HEIGHT, new.0);
            insert_hash(row, hs_trace::HASH, new.1);
            insert_height(row, hs_trace::RANGE_START, old.0);
        }
    }
}

fn trace_status_fields(
    row: &mut serde_json::Map<String, Value>,
    peer: &ZakuraPeerId,
    status: HeaderSyncStatus,
) {
    insert_peer(row, hs_trace::PEER, peer);
    insert_height(row, hs_trace::HEIGHT, status.tip_height);
    insert_hash(row, hs_trace::HASH, status.tip_hash);
    insert_height(row, hs_trace::RANGE_START, status.anchor_height);
    insert_u64(
        row,
        hs_trace::ADVERTISED_CAP,
        u64::from(status.max_headers_per_response),
    );
    insert_u64(
        row,
        hs_trace::IN_FLIGHT_COUNT,
        u64::from(status.max_inflight_requests),
    );
}

fn trace_header_sync_message_fields(
    row: &mut serde_json::Map<String, Value>,
    msg: &HeaderSyncMessage,
) {
    match msg {
        HeaderSyncMessage::Status(status) => {
            insert_height(row, hs_trace::HEIGHT, status.tip_height);
            insert_hash(row, hs_trace::HASH, status.tip_hash);
            insert_height(row, hs_trace::RANGE_START, status.anchor_height);
            insert_u64(
                row,
                hs_trace::ADVERTISED_CAP,
                u64::from(status.max_headers_per_response),
            );
            insert_u64(
                row,
                hs_trace::IN_FLIGHT_COUNT,
                u64::from(status.max_inflight_requests),
            );
        }
        HeaderSyncMessage::Headers { headers, .. } => {
            insert_u64(
                row,
                hs_trace::RANGE_COUNT,
                u64::try_from(headers.len()).unwrap_or(u64::MAX),
            );
        }
        HeaderSyncMessage::GetHeaders {
            start_height,
            count,
            ..
        } => {
            insert_height(row, hs_trace::RANGE_START, *start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
        }
        HeaderSyncMessage::NewBlock(block) => {
            insert_hash(row, hs_trace::HASH, block.hash());
            if let Some(height) = block.coinbase_height() {
                insert_height(row, hs_trace::HEIGHT, height);
            }
        }
    }
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

fn trace_wire_error_fields(row: &mut serde_json::Map<String, Value>, error: &HeaderSyncWireError) {
    if let HeaderSyncWireError::TreeAuxRootHeightMismatch {
        offset,
        expected_height,
        root_height,
        first_root_height,
        last_root_height,
    } = error
    {
        insert_u64(
            row,
            hs_trace::ROOT_MISMATCH_OFFSET,
            u64::try_from(*offset).unwrap_or(u64::MAX),
        );
        insert_height(row, hs_trace::EXPECTED_ROOT_HEIGHT, *expected_height);
        insert_height(row, hs_trace::ACTUAL_ROOT_HEIGHT, *root_height);
        insert_height(row, hs_trace::FIRST_ROOT_HEIGHT, *first_root_height);
        insert_height(row, hs_trace::LAST_ROOT_HEIGHT, *last_root_height);
    }
}

pub(super) fn header_sync_wire_error_kind(error: &HeaderSyncWireError) -> &'static str {
    match error {
        HeaderSyncWireError::OversizedPayload { .. } => "oversized_payload",
        HeaderSyncWireError::HeaderCountLimit { .. } => "header_count_limit",
        HeaderSyncWireError::InvalidRange { .. } => "invalid_range",
        HeaderSyncWireError::EmptyHeaderRangePayload => "empty_header_range_payload",
        HeaderSyncWireError::EntryHeightMismatch { .. } => "entry_height_mismatch",
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

pub(super) fn insert_peer(
    row: &mut serde_json::Map<String, Value>,
    key: &'static str,
    peer: &ZakuraPeerId,
) {
    row.insert(key.to_string(), Value::String(trace_peer_label(peer)));
}

pub(super) fn insert_height(
    row: &mut serde_json::Map<String, Value>,
    key: &'static str,
    height: block::Height,
) {
    insert_u64(row, key, u64::from(height.0));
}

pub(super) fn insert_hash(
    row: &mut serde_json::Map<String, Value>,
    key: &'static str,
    hash: block::Hash,
) {
    row.insert(key.to_string(), Value::String(format!("{hash}")));
}

pub(super) fn insert_u64(row: &mut serde_json::Map<String, Value>, key: &'static str, value: u64) {
    row.insert(key.to_string(), Value::Number(Number::from(value)));
}

fn insert_bool(row: &mut serde_json::Map<String, Value>, key: &'static str, value: bool) {
    row.insert(key.to_string(), Value::Bool(value));
}

fn insert_optional_str(
    row: &mut serde_json::Map<String, Value>,
    key: &'static str,
    value: Option<&'static str>,
) {
    row.insert(
        key.to_string(),
        value.map_or(Value::Null, |value| Value::String(value.to_string())),
    );
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
            events::HeaderSyncFrontiers, service::HeaderSyncPeerSession, state::RangePriority,
        },
        HeaderRangeEntry, HeaderRangePayload, HeaderSyncOperationIdentity, HeaderSyncOperationKind,
        HeaderSyncServiceSummary, HeaderSyncWireRequestIdentity,
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
        let mut row = serde_json::Map::new();
        trace_event_fields(&mut row, event);
        row
    }

    fn action_row(action: &HeaderSyncAction) -> serde_json::Map<String, Value> {
        let mut row = serde_json::Map::new();
        trace_action_fields(&mut row, action);
        row
    }

    fn message_row(message: &HeaderSyncMessage) -> serde_json::Map<String, Value> {
        let mut row = serde_json::Map::new();
        trace_header_sync_message_fields(&mut row, message);
        row
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
                wire_request: HeaderSyncWireRequestIdentity {
                    peer: peer.clone(),
                    session_id: 7,
                    request_id: request_id(),
                },
                entries: vec![HeaderRangeEntry {
                    height: block::Height(1),
                    header: header.clone(),
                    body_size: 1,
                    tree_aux_root: None,
                }],
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
                    payload: HeaderRangePayload::new(vec![HeaderRangeEntry {
                        height: block::Height(1),
                        header,
                        body_size: 1,
                        tree_aux_root: None,
                    }])
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
                HeaderSyncWireError::EmptyHeaderRangePayload,
                "empty_header_range_payload",
            ),
            (
                HeaderSyncWireError::EntryHeightMismatch {
                    offset: 1,
                    expected_height: block::Height(2),
                    entry_height: block::Height(3),
                },
                "entry_height_mismatch",
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

        let mut row = serde_json::Map::new();
        insert_optional_str(&mut row, "present", Some("value"));
        insert_optional_str(&mut row, "absent", None);
        assert_eq!(row["present"], Value::from("value"));
        assert_eq!(row["absent"], Value::Null);

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
        let mut row = serde_json::Map::new();
        trace_wire_error_fields(&mut row, &mismatch);
        assert_eq!(row[hs_trace::ROOT_MISMATCH_OFFSET], Value::from(7));
        assert_eq!(row[hs_trace::EXPECTED_ROOT_HEIGHT], Value::from(8));
        assert_eq!(row[hs_trace::ACTUAL_ROOT_HEIGHT], Value::from(9));
        assert_eq!(row[hs_trace::FIRST_ROOT_HEIGHT], Value::from(10));
        assert_eq!(row[hs_trace::LAST_ROOT_HEIGHT], Value::from(11));

        assert_eq!(count_between(block::Height(3), block::Height(5)), 3);
        assert_eq!(count_between(block::Height(5), block::Height(3)), 0);
        let committed = event_row(&HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation: operation(peer(3)),
            tip_hash: block::Hash([5; 32]),
        });
        assert_eq!(committed[hs_trace::REQUEST_ID], Value::from(1));
        let gaps = action_row(&HeaderSyncAction::BodyGaps {
            from: block::Height(3),
            to: block::Height(5),
        });
        assert_eq!(gaps[hs_trace::RANGE_COUNT], Value::from(3));

        let empty = TreeAuxTraceSummary::default();
        let mut row = serde_json::Map::new();
        empty.insert_into(&mut row);
        assert!(row.is_empty());

        let roots = TreeAuxTraceSummary::new(&[root(block::Height(8)), root(block::Height(9))]);
        assert_eq!(roots.len, 2);
        roots.insert_into(&mut row);
        assert_eq!(row[hs_trace::FIRST_ROOT_HEIGHT], Value::from(8));
        assert_eq!(row[hs_trace::LAST_ROOT_HEIGHT], Value::from(9));

        assert_eq!(RangePriority::Forward.label(), "forward");
        assert_eq!(RangePriority::Repair.label(), "repair");
    }
}
