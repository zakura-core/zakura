//! Typed JSONL events owned by the block-sync subsystem.

use std::time::Duration;

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::JsonlTraceEvent;

use super::{
    events::{BlockApplyResult, BlockSyncAction, BlockSyncEvent, BlockSyncMisbehavior},
    wire::BlockSyncMessage,
    BlockSyncStatus,
};
use crate::zakura::{
    trace::{ordered_send_error_label, peer_label, BLOCK_SYNC_TABLE, QUEUE_SEND_TABLE},
    OrderedSendError, ZakuraPeerId,
};

/// A sparse, typed projection shared by block-sync event variants.
///
/// Optional fields are omitted, matching the historical map emitter. The only
/// field with two historical JSON types is `received_status`: disconnect rows
/// use a boolean, while fill-stop snapshots use numeric `0`/`1`.
#[derive(Default, Serialize)]
pub(super) struct BlockTraceFields {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range_start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub servable_low: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub servable_high: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serialized_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decoded_attributed_memory_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_input_decoded_attributed_memory_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reorder_decoded_attributed_memory_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applying_decoded_attributed_memory_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_pipeline_decoded_attributed_memory_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prepare_elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub apply_token: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_floor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_download_floor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor_gap_height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor_gap_state: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor_gap_servable_peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor_gap_available_peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor_gap_outstanding_peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor_gap_oldest_outstanding_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor_gap_next_deadline_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_block_tip: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_header_tip: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_lag: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applying: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submitted_applies: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reorder: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outstanding: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_available: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_reserved: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_reserved_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_bytes_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_blocks_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_bytes_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_blocks_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_blocked_on_budget: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers_wanting_slots: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_connections: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers_with_status: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub needed_min: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub needed_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_len: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_blocks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_capacity: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_max_capacity: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_min_start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned_len: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_body_work: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refill_low_water: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub covered_max_end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_stop_reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_sent: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_status: Option<BoolOrU64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub released_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub returned_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub already_pending_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub released_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_flight_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_input_queued_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_input_queued_blocks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_input_capacity: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_input_capacity_before: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_input_max_capacity: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reorder_buffered_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applying_buffered_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unsubmitted_applying_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_flight_submission_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_flight_submission_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retained_pipeline_wire_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outbound_peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_peers_with_status: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outbound_peers_with_status: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_slot_capacity: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_slot_effective_window: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_slot_available: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_slot_saturated_peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_slots: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_outstanding: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normal_slots: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor_slots: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_work: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unreceived_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_min_height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_max_height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_range_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_send_elapsed_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_queue_elapsed_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decode_permit_wait_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_attempt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_verified_tip: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_download_floor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preserve_active_successors: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_has_successor_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_outstanding_conflicts_at_tip: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_tip_matches_local_work: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_local_successor_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests_without_block_progress: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_progress_request_cap: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_progress_proven: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_cwnd: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_rtprop_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_btlbw_milliblocks_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_cwnd_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_inflight_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_btlbw_bytes_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_delivered: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_phase: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_smoothed_elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_delay_cap: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbr_reliability_permille: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor_bypass: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_block_age_ms: Option<u64>,
}

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum BoolOrU64 {
    Bool(bool),
    U64(u64),
}

/// A block-sync JSONL row.
#[derive(Serialize)]
pub(super) struct BlockTraceEvent {
    event: &'static str,
    #[serde(flatten)]
    pub fields: BlockTraceFields,
}

impl BlockTraceEvent {
    pub(super) fn new(event: &'static str) -> Self {
        Self {
            event,
            fields: BlockTraceFields::default(),
        }
    }

    pub(super) fn build(event: &'static str, build: impl FnOnce(&mut BlockTraceFields)) -> Self {
        let mut row = Self::new(event);
        build(&mut row.fields);
        row
    }
}

impl JsonlTraceEvent for BlockTraceEvent {
    const TABLE: zakura_jsonl_trace::JsonlTraceTable = BLOCK_SYNC_TABLE;
}

/// A typed queue-send failure originating in block sync.
#[derive(Serialize)]
pub(super) struct QueueSendFailedEvent {
    event: &'static str,
    service: &'static str,
    message: &'static str,
    peer: String,
    error: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    queue_capacity: u64,
    queue_max_capacity: u64,
    #[serde(flatten)]
    message_fields: MessageFields,
}

impl QueueSendFailedEvent {
    pub(super) fn new(
        peer: &ZakuraPeerId,
        message: &BlockSyncMessage,
        error: &OrderedSendError,
        reason: Option<&'static str>,
        queue_capacity: usize,
        queue_max_capacity: usize,
    ) -> Self {
        Self::build(
            peer,
            message,
            error,
            reason,
            queue_capacity,
            queue_max_capacity,
            MessageFields::new(message),
        )
    }

    /// Peer routines historically projected range fields only for GetBlocks.
    pub(super) fn peer_routine(
        peer: &ZakuraPeerId,
        message: &BlockSyncMessage,
        error: &OrderedSendError,
        queue_capacity: usize,
        queue_max_capacity: usize,
    ) -> Self {
        let message_fields = match message {
            BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } => MessageFields {
                range_start: Some(height(*start_height)),
                range_count: Some(u64::from(*count)),
                ..MessageFields::default()
            },
            _ => MessageFields::default(),
        };
        Self::build(
            peer,
            message,
            error,
            None,
            queue_capacity,
            queue_max_capacity,
            message_fields,
        )
    }

    fn build(
        peer: &ZakuraPeerId,
        message: &BlockSyncMessage,
        error: &OrderedSendError,
        reason: Option<&'static str>,
        queue_capacity: usize,
        queue_max_capacity: usize,
        message_fields: MessageFields,
    ) -> Self {
        Self {
            event: "queue_send_failed",
            service: "block_sync",
            message: block_sync_message_label(message),
            peer: peer_label(peer),
            error: ordered_send_error_label(error),
            reason,
            queue_capacity: saturating_usize(queue_capacity),
            queue_max_capacity: saturating_usize(queue_max_capacity),
            message_fields,
        }
    }
}

impl JsonlTraceEvent for QueueSendFailedEvent {
    const TABLE: zakura_jsonl_trace::JsonlTraceTable = QUEUE_SEND_TABLE;
}

#[derive(Default, Serialize)]
struct MessageFields {
    #[serde(skip_serializing_if = "Option::is_none")]
    range_start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash: Option<String>,
}

impl MessageFields {
    fn new(message: &BlockSyncMessage) -> Self {
        match message {
            BlockSyncMessage::Status(status) => Self {
                range_start: Some(height(status.servable_low)),
                height: Some(height(status.servable_high)),
                ..Self::default()
            },
            BlockSyncMessage::Block(block) => Self {
                height: block.coinbase_height().map(height),
                hash: Some(hash(block.hash())),
                ..Self::default()
            },
            BlockSyncMessage::BlocksDone {
                start_height,
                returned,
            } => Self {
                range_start: Some(height(*start_height)),
                range_count: Some(u64::from(*returned)),
                ..Self::default()
            },
            BlockSyncMessage::RangeUnavailable {
                start_height,
                count,
            }
            | BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } => Self {
                range_start: Some(height(*start_height)),
                range_count: Some(u64::from(*count)),
                ..Self::default()
            },
        }
    }
}

pub(super) fn project_message(row: &mut BlockTraceFields, message: &BlockSyncMessage) {
    let fields = MessageFields::new(message);
    row.range_start = fields.range_start;
    row.range_count = fields.range_count;
    row.height = fields.height;
    row.hash = fields.hash;
}

#[derive(Serialize)]
pub(super) struct BlockEventReceived {
    event: &'static str,
    #[serde(flatten)]
    detail: BlockEventDetail,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BlockEventDetail {
    PeerConnected {
        peer: String,
    },
    PeerDisconnected {
        peer: String,
    },
    HeaderTipChanged {
        height: u64,
        hash: String,
    },
    StateFrontiersChanged {
        verified_block_tip: u64,
        hash: String,
    },
    ChainTipGrow {
        verified_block_tip: u64,
        hash: String,
    },
    ChainTipReset {
        verified_block_tip: u64,
        hash: String,
    },
    NeededBlocks {
        range_count: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        range_start: Option<u64>,
    },
    BlockApplyFinished {
        apply_token: u64,
        height: u64,
        hash: String,
        result: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        verified_block_tip: Option<u64>,
    },
    BlockRangeResponseFinished {
        peer: String,
        range_start: u64,
        range_count: u64,
        expected_count: u64,
    },
    BlockRangeResponseReady {
        peer: String,
        range_start: u64,
        range_count: u64,
        expected_count: u64,
    },
}

impl BlockEventReceived {
    pub(super) fn new(event: &BlockSyncEvent) -> Self {
        let detail = match event {
            BlockSyncEvent::PeerConnected(session) => BlockEventDetail::PeerConnected {
                peer: peer_label(session.peer_id()),
            },
            BlockSyncEvent::PeerDisconnected(peer) => BlockEventDetail::PeerDisconnected {
                peer: peer_label(peer),
            },
            BlockSyncEvent::HeaderTipChanged { height: h, hash: v } => {
                BlockEventDetail::HeaderTipChanged {
                    height: height(*h),
                    hash: hash(*v),
                }
            }
            BlockSyncEvent::StateFrontiersChanged(frontiers) => {
                BlockEventDetail::StateFrontiersChanged {
                    verified_block_tip: height(frontiers.verified_block_tip),
                    hash: hash(frontiers.verified_block_hash),
                }
            }
            BlockSyncEvent::ChainTipGrow(frontiers) => BlockEventDetail::ChainTipGrow {
                verified_block_tip: height(frontiers.verified_block_tip),
                hash: hash(frontiers.verified_block_hash),
            },
            BlockSyncEvent::ChainTipReset(frontiers) => BlockEventDetail::ChainTipReset {
                verified_block_tip: height(frontiers.verified_block_tip),
                hash: hash(frontiers.verified_block_hash),
            },
            BlockSyncEvent::NeededBlocks(blocks) => BlockEventDetail::NeededBlocks {
                range_count: saturating_usize(blocks.len()),
                range_start: blocks.first().map(|block| height(block.height)),
            },
            BlockSyncEvent::BlockApplyFinished {
                token,
                height: h,
                hash: event_hash,
                result,
                local_frontier,
            } => BlockEventDetail::BlockApplyFinished {
                apply_token: *token,
                height: height(*h),
                hash: hash(
                    local_frontier
                        .as_ref()
                        .map_or(*event_hash, |frontier| frontier.verified_block_hash),
                ),
                result: block_apply_result_label(*result),
                verified_block_tip: local_frontier
                    .as_ref()
                    .map(|frontier| height(frontier.verified_block_tip)),
            },
            BlockSyncEvent::BlockRangeResponseFinished {
                peer,
                start_height,
                requested_count,
                returned_count,
            } => BlockEventDetail::BlockRangeResponseFinished {
                peer: peer_label(peer),
                range_start: height(*start_height),
                range_count: u64::from(*returned_count),
                expected_count: u64::from(*requested_count),
            },
            BlockSyncEvent::BlockRangeResponseReady {
                peer,
                start_height,
                requested_count,
                blocks,
            } => BlockEventDetail::BlockRangeResponseReady {
                peer: peer_label(peer),
                range_start: height(*start_height),
                range_count: saturating_usize(blocks.len()),
                expected_count: u64::from(*requested_count),
            },
        };
        Self {
            event: "block_event_received",
            detail,
        }
    }
}

impl JsonlTraceEvent for BlockEventReceived {
    const TABLE: zakura_jsonl_trace::JsonlTraceTable = BLOCK_SYNC_TABLE;
}

#[derive(Serialize)]
pub(super) struct BlockActionDispatched {
    event: &'static str,
    #[serde(flatten)]
    detail: BlockActionDetail,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BlockActionDetail {
    QueryNeededBlocks {
        range_start: u64,
        range_count: u64,
        best_header_tip: u64,
    },
    QueryBlocksByHeightRange {
        peer: String,
        range_start: u64,
        range_count: u64,
    },
    SubmitBlock {
        apply_token: u64,
        hash: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        height: Option<u64>,
    },
    Misbehavior {
        peer: String,
        reason: &'static str,
    },
}

impl BlockActionDispatched {
    pub(super) fn new(action: &BlockSyncAction) -> Self {
        let detail = match action {
            BlockSyncAction::QueryNeededBlocks {
                from,
                limit,
                best_header_tip,
            } => BlockActionDetail::QueryNeededBlocks {
                range_start: height(*from),
                range_count: u64::from(*limit),
                best_header_tip: height(*best_header_tip),
            },
            BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                BlockActionDetail::QueryBlocksByHeightRange {
                    peer: peer_label(peer),
                    range_start: height(*start),
                    range_count: u64::from(*count),
                }
            }
            BlockSyncAction::SubmitBlock { token, block } => BlockActionDetail::SubmitBlock {
                apply_token: *token,
                hash: hash(block.hash()),
                height: block.coinbase_height().map(height),
            },
            BlockSyncAction::Misbehavior { peer, reason } => BlockActionDetail::Misbehavior {
                peer: peer_label(peer),
                reason: block_misbehavior_label(*reason),
            },
        };
        Self {
            event: "block_action_dispatched",
            detail,
        }
    }
}

impl JsonlTraceEvent for BlockActionDispatched {
    const TABLE: zakura_jsonl_trace::JsonlTraceTable = BLOCK_SYNC_TABLE;
}

pub(super) fn block_sync_message_label(message: &BlockSyncMessage) -> &'static str {
    match message {
        BlockSyncMessage::Status(_) => "status",
        BlockSyncMessage::Block(_) => "block",
        BlockSyncMessage::BlocksDone { .. } => "blocks_done",
        BlockSyncMessage::RangeUnavailable { .. } => "range_unavailable",
        BlockSyncMessage::GetBlocks { .. } => "get_blocks",
    }
}

pub(super) fn block_apply_result_label(result: BlockApplyResult) -> &'static str {
    match result {
        BlockApplyResult::Committed => "committed",
        BlockApplyResult::Duplicate => "duplicate",
        BlockApplyResult::Rejected => "rejected",
        BlockApplyResult::TimedOut => "timed_out",
    }
}

fn block_misbehavior_label(reason: BlockSyncMisbehavior) -> &'static str {
    match reason {
        BlockSyncMisbehavior::MalformedMessage => "malformed_message",
        BlockSyncMisbehavior::UnsolicitedBlock => "unsolicited_block",
        BlockSyncMisbehavior::GetBlocksTooLong => "get_blocks_too_long",
        BlockSyncMisbehavior::GetBlocksSpam => "get_blocks_spam",
        BlockSyncMisbehavior::InvalidBlock => "invalid_block",
        BlockSyncMisbehavior::SizeMismatch => "size_mismatch",
        BlockSyncMisbehavior::InvalidStatus => "invalid_status",
        BlockSyncMisbehavior::UnsolicitedDone => "unsolicited_done",
        BlockSyncMisbehavior::RangeUnavailable => "range_unavailable",
        BlockSyncMisbehavior::StatusSpam => "status_spam",
    }
}

pub(super) fn peer(peer: &ZakuraPeerId) -> String {
    peer_label(peer)
}

pub(super) fn height(height: block::Height) -> u64 {
    u64::from(height.0)
}

pub(super) fn hash(hash: block::Hash) -> String {
    hash.to_string()
}

pub(super) fn elapsed_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(super) fn elapsed_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

pub(super) fn saturating_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn status_fields(row: &mut BlockTraceFields, status: BlockSyncStatus) {
    row.range_start = Some(height(status.servable_low));
    row.height = Some(height(status.servable_high));
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn block_sync_core_avoids_legacy_trace_builders() {
        for (name, source) in [
            ("reactor", include_str!("reactor.rs")),
            ("peer_routine", include_str!("peer_routine.rs")),
            ("sequencer_task", include_str!("sequencer_task.rs")),
        ] {
            for forbidden in [
                "emit_block(",
                "BlockTraceFields",
                "BlockTraceEvent",
                "bs_trace::",
            ] {
                assert!(
                    !source.contains(forbidden),
                    "{name} must not contain trace construction `{forbidden}`"
                );
            }
        }
    }

    fn value(event: BlockTraceEvent) -> serde_json::Value {
        serde_json::to_value(event).expect("typed block trace event serializes")
    }

    #[test]
    fn sparse_fields_are_omitted_and_numeric_fields_stay_numeric() {
        let event = BlockTraceEvent::build("block_body_submitted", |row| {
            row.height = Some(42);
            row.apply_token = Some(7);
        });
        assert_eq!(
            value(event),
            json!({"event": "block_body_submitted", "height": 42, "apply_token": 7})
        );
    }

    #[test]
    fn received_status_preserves_both_historical_json_types() {
        let disconnected = BlockTraceEvent::build("block_peer_disconnected", |row| {
            row.received_status = Some(BoolOrU64::Bool(true));
        });
        let fill_stop = BlockTraceEvent::build("block_fill_stop", |row| {
            row.received_status = Some(BoolOrU64::U64(1));
        });
        assert_eq!(value(disconnected)["received_status"], json!(true));
        assert_eq!(value(fill_stop)["received_status"], json!(1));
    }

    #[test]
    fn optional_message_fields_are_omitted() {
        let message = BlockSyncMessage::Status(BlockSyncStatus {
            servable_low: block::Height(10),
            servable_high: block::Height(20),
            tip_hash: block::Hash([7; 32]),
            max_blocks_per_response: 1,
            max_inflight_requests: 1,
            max_response_bytes: 1,
        });
        assert_eq!(
            serde_json::to_value(MessageFields::new(&message))
                .expect("message projection serializes"),
            json!({"range_start": 10, "height": 20})
        );
    }

    #[test]
    fn queue_send_failure_schema_is_exact() {
        let event = QueueSendFailedEvent {
            event: "queue_send_failed",
            service: "block_sync",
            message: "get_blocks",
            peer: "p".into(),
            error: "full",
            reason: None,
            queue_capacity: 2,
            queue_max_capacity: 4,
            message_fields: MessageFields {
                range_start: Some(10),
                range_count: Some(3),
                ..MessageFields::default()
            },
        };
        assert_eq!(
            serde_json::to_value(event).expect("queue failure serializes"),
            json!({
                "event": "queue_send_failed",
                "service": "block_sync",
                "message": "get_blocks",
                "peer": "p",
                "error": "full",
                "queue_capacity": 2,
                "queue_max_capacity": 4,
                "range_start": 10,
                "range_count": 3
            })
        );
    }

    #[test]
    fn received_event_variants_have_exact_flattened_schemas() {
        let cases = [
            (
                BlockEventDetail::PeerConnected { peer: "p".into() },
                json!({"kind": "peer_connected", "peer": "p"}),
            ),
            (
                BlockEventDetail::PeerDisconnected { peer: "p".into() },
                json!({"kind": "peer_disconnected", "peer": "p"}),
            ),
            (
                BlockEventDetail::HeaderTipChanged {
                    height: 1,
                    hash: "h".into(),
                },
                json!({"kind": "header_tip_changed", "height": 1, "hash": "h"}),
            ),
            (
                BlockEventDetail::StateFrontiersChanged {
                    verified_block_tip: 1,
                    hash: "h".into(),
                },
                json!({"kind": "state_frontiers_changed", "verified_block_tip": 1, "hash": "h"}),
            ),
            (
                BlockEventDetail::ChainTipGrow {
                    verified_block_tip: 1,
                    hash: "h".into(),
                },
                json!({"kind": "chain_tip_grow", "verified_block_tip": 1, "hash": "h"}),
            ),
            (
                BlockEventDetail::ChainTipReset {
                    verified_block_tip: 1,
                    hash: "h".into(),
                },
                json!({"kind": "chain_tip_reset", "verified_block_tip": 1, "hash": "h"}),
            ),
            (
                BlockEventDetail::NeededBlocks {
                    range_count: 2,
                    range_start: None,
                },
                json!({"kind": "needed_blocks", "range_count": 2}),
            ),
            (
                BlockEventDetail::BlockApplyFinished {
                    apply_token: 3,
                    height: 1,
                    hash: "h".into(),
                    result: "committed",
                    verified_block_tip: Some(1),
                },
                json!({"kind": "block_apply_finished", "apply_token": 3, "height": 1, "hash": "h", "result": "committed", "verified_block_tip": 1}),
            ),
            (
                BlockEventDetail::BlockRangeResponseFinished {
                    peer: "p".into(),
                    range_start: 1,
                    range_count: 2,
                    expected_count: 3,
                },
                json!({"kind": "block_range_response_finished", "peer": "p", "range_start": 1, "range_count": 2, "expected_count": 3}),
            ),
            (
                BlockEventDetail::BlockRangeResponseReady {
                    peer: "p".into(),
                    range_start: 1,
                    range_count: 2,
                    expected_count: 3,
                },
                json!({"kind": "block_range_response_ready", "peer": "p", "range_start": 1, "range_count": 2, "expected_count": 3}),
            ),
        ];

        for (detail, expected) in cases {
            let actual = serde_json::to_value(BlockEventReceived {
                event: "block_event_received",
                detail,
            })
            .expect("received event serializes");
            assert_eq!(actual, merge_event(expected, "block_event_received"));
        }
    }

    #[test]
    fn dispatched_action_variants_have_exact_flattened_schemas() {
        let cases = [
            (
                BlockActionDetail::QueryNeededBlocks {
                    range_start: 1,
                    range_count: 2,
                    best_header_tip: 3,
                },
                json!({"kind": "query_needed_blocks", "range_start": 1, "range_count": 2, "best_header_tip": 3}),
            ),
            (
                BlockActionDetail::QueryBlocksByHeightRange {
                    peer: "p".into(),
                    range_start: 1,
                    range_count: 2,
                },
                json!({"kind": "query_blocks_by_height_range", "peer": "p", "range_start": 1, "range_count": 2}),
            ),
            (
                BlockActionDetail::SubmitBlock {
                    apply_token: 3,
                    hash: "h".into(),
                    height: None,
                },
                json!({"kind": "submit_block", "apply_token": 3, "hash": "h"}),
            ),
            (
                BlockActionDetail::Misbehavior {
                    peer: "p".into(),
                    reason: "invalid_block",
                },
                json!({"kind": "misbehavior", "peer": "p", "reason": "invalid_block"}),
            ),
        ];

        for (detail, expected) in cases {
            let actual = serde_json::to_value(BlockActionDispatched {
                event: "block_action_dispatched",
                detail,
            })
            .expect("dispatched action serializes");
            assert_eq!(actual, merge_event(expected, "block_action_dispatched"));
        }
    }

    fn merge_event(mut value: serde_json::Value, event: &'static str) -> serde_json::Value {
        value
            .as_object_mut()
            .expect("expected schema is an object")
            .insert("event".to_string(), json!(event));
        value
    }
}
