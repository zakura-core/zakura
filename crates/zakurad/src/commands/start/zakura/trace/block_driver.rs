//! Semantic commit-state tracing for the block-sync driver.

use std::time::Instant;

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{saturating_count, saturating_millis, JsonlTraceEvent};
use zakura_network::zakura::{
    commit_state_trace as event, zakura_trace_peer_label, BlockApplyResult, BlockApplyToken,
    BlockSyncAction, BlockSyncFrontiers, BlockSyncMisbehavior, ZakuraPeerId, ZakuraTrace,
    COMMIT_STATE_TABLE,
};

use super::super::block_sync_driver::BlockApplyClass;

const SOURCE: &str = "block_sync_driver";

#[derive(Serialize)]
struct DriverEvent<F> {
    event: &'static str,
    source: &'static str,
    #[serde(flatten)]
    fields: F,
}

impl<F: Serialize> JsonlTraceEvent for DriverEvent<F> {
    const TABLE: zakura_jsonl_trace::JsonlTraceTable = COMMIT_STATE_TABLE;
}

#[derive(Serialize)]
struct Action<'a> {
    action: &'a str,
}

#[derive(Serialize)]
#[serde(untagged)]
enum ReceivedAction {
    Misbehavior {
        action: &'static str,
        peer: String,
        reason: &'static str,
    },
    NeededBlocks(NeededRange),
    BlockRange {
        action: &'static str,
        peer: String,
        range_start: u64,
        range_count: u64,
    },
    SubmitBlock {
        action: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        height: Option<u64>,
        hash: String,
        apply_token: u64,
    },
}

#[derive(Serialize)]
struct NeededRange {
    action: &'static str,
    range_start: u64,
    range_count: u64,
    best_header_tip: u64,
}

#[derive(Serialize)]
struct NeededSuccess {
    action: &'static str,
    range_count: u64,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct NeededFailure<'a> {
    action: &'static str,
    result: &'static str,
    reason: &'a str,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct Range<'a> {
    action: &'static str,
    peer: String,
    range_start: u64,
    range_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_count: Option<u64>,
}

#[derive(Serialize)]
struct ApplyIdentity {
    height: u64,
    hash: String,
    apply_token: u64,
    apply_class: &'static str,
}

#[derive(Serialize)]
struct ApplyFinished {
    action: &'static str,
    height: u64,
    hash: String,
    result: &'static str,
    apply_token: u64,
    local_frontier: bool,
}

#[derive(Serialize)]
struct CommitFinished {
    height: u64,
    hash: String,
    result: &'static str,
    apply_token: u64,
    apply_class: &'static str,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct CommitStalled {
    height: u64,
    hash: String,
    apply_token: u64,
    apply_class: &'static str,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct FrontierQuery {
    height: u64,
    hash: String,
    apply_token: u64,
    #[serde(flatten)]
    frontiers: Option<Frontiers>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_frontier: Option<bool>,
}

#[derive(Serialize)]
struct Frontiers {
    finalized_height: u64,
    verified_block_tip: u64,
    verified_block_hash: String,
}

impl From<&BlockSyncFrontiers> for Frontiers {
    fn from(value: &BlockSyncFrontiers) -> Self {
        Self {
            finalized_height: value.finalized_height.0.into(),
            verified_block_tip: value.verified_block_tip.0.into(),
            verified_block_hash: value.verified_block_hash.to_string(),
        }
    }
}

#[derive(Serialize)]
struct SubmitQueued {
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u64>,
    hash: String,
    apply_token: u64,
    apply_class: &'static str,
    queue_len: u64,
    in_flight_count: u64,
}

#[derive(Serialize)]
struct RefreshAttempt {
    verified_block_tip: u64,
    attempts_remaining: u64,
}

pub(crate) trait BlockDriverTraceExt {
    fn trace_block_action_received(&self, action: &BlockSyncAction);
    fn trace_needed_blocks_query_started(
        &self,
        from: block::Height,
        limit: u32,
        best_header_tip: block::Height,
    );
    fn trace_needed_blocks_query_succeeded(&self, count: usize, started: Instant);
    fn trace_needed_blocks_query_failed(&self, reason: &str, started: Instant);
    fn trace_block_reactor_event(&self, action: &'static str);
    fn trace_block_range_query_started(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
    );
    fn trace_block_range_query_succeeded(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: usize,
        started: Instant,
    );
    fn trace_block_range_event(
        &self,
        action: &'static str,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
    );
    fn trace_block_range_query_failed(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        reason: &str,
        started: Instant,
    );
    fn trace_block_range_query_timed_out(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        started: Instant,
    );
    fn trace_block_range_finished(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        requested: u32,
        returned: u32,
    );
    fn trace_block_submit_queued(
        &self,
        token: BlockApplyToken,
        class: BlockApplyClass,
        block: &block::Block,
        queue_len: usize,
        in_flight_count: usize,
    );
    fn trace_block_apply_finished(
        &self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        result: BlockApplyResult,
        local_frontier: bool,
    );
    fn trace_block_commit_started(
        &self,
        token: BlockApplyToken,
        class: BlockApplyClass,
        height: block::Height,
        hash: block::Hash,
    );
    fn trace_block_commit_finished(
        &self,
        token: BlockApplyToken,
        class: BlockApplyClass,
        height: block::Height,
        hash: block::Hash,
        result: BlockApplyResult,
        started: Instant,
    );
    fn trace_block_commit_stalled(
        &self,
        token: BlockApplyToken,
        class: BlockApplyClass,
        height: block::Height,
        hash: block::Hash,
        elapsed: std::time::Duration,
    );
    fn trace_block_frontier_query_started(
        &self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
    );
    fn trace_block_frontier_query_finished(
        &self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        frontiers: Option<&BlockSyncFrontiers>,
    );
    fn trace_checkpoint_refresh_attempt(&self, attempts_remaining: usize, tip: block::Height);
    fn trace_checkpoint_refresh_sent(&self, frontiers: &BlockSyncFrontiers);
}

impl BlockDriverTraceExt for ZakuraTrace {
    fn trace_block_action_received(&self, action: &BlockSyncAction) {
        self.emit_event(|| DriverEvent {
            event: event::ACTION_RECEIVED,
            source: SOURCE,
            fields: match action {
                BlockSyncAction::Misbehavior { peer, reason } => ReceivedAction::Misbehavior {
                    action: "misbehavior",
                    peer: zakura_trace_peer_label(peer),
                    reason: misbehavior_label(*reason),
                },
                BlockSyncAction::QueryNeededBlocks {
                    from,
                    limit,
                    best_header_tip,
                } => ReceivedAction::NeededBlocks(NeededRange {
                    action: "query_needed_blocks",
                    range_start: from.0.into(),
                    range_count: (*limit).into(),
                    best_header_tip: best_header_tip.0.into(),
                }),
                BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                    ReceivedAction::BlockRange {
                        action: "query_blocks_by_height_range",
                        peer: zakura_trace_peer_label(peer),
                        range_start: start.0.into(),
                        range_count: (*count).into(),
                    }
                }
                BlockSyncAction::SubmitBlock { token, block } => ReceivedAction::SubmitBlock {
                    action: "submit_block",
                    apply_token: *token,
                    hash: block.hash().to_string(),
                    height: block.coinbase_height().map(|height| height.0.into()),
                },
            },
        });
    }

    fn trace_needed_blocks_query_started(
        &self,
        from: block::Height,
        limit: u32,
        best_header_tip: block::Height,
    ) {
        emit(self, event::STATE_READ_START, || NeededRange {
            action: "query_needed_blocks",
            range_start: from.0.into(),
            range_count: limit.into(),
            best_header_tip: best_header_tip.0.into(),
        });
    }

    fn trace_needed_blocks_query_succeeded(&self, count: usize, started: Instant) {
        emit(self, event::STATE_READ_SUCCESS, || NeededSuccess {
            action: "query_needed_blocks",
            range_count: saturating_count(count),
            elapsed_ms: saturating_millis(started.elapsed()),
        });
    }

    fn trace_needed_blocks_query_failed(&self, reason: &str, started: Instant) {
        emit(self, event::STATE_READ_ERROR, || NeededFailure {
            action: "query_needed_blocks",
            result: "error",
            reason,
            elapsed_ms: saturating_millis(started.elapsed()),
        });
    }

    fn trace_block_reactor_event(&self, action: &'static str) {
        emit(self, event::REACTOR_EVENT_SENT, || Action { action });
    }

    fn trace_block_range_query_started(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
    ) {
        range(
            self,
            event::STATE_READ_START,
            "query_blocks_by_height_range",
            peer,
            start,
            count.into(),
            None,
            None,
            None,
            None,
        );
    }

    fn trace_block_range_query_succeeded(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: usize,
        started: Instant,
    ) {
        range(
            self,
            event::STATE_READ_SUCCESS,
            "query_blocks_by_height_range",
            peer,
            start,
            saturating_count(count),
            None,
            None,
            Some(saturating_millis(started.elapsed())),
            None,
        );
    }

    fn trace_block_range_event(
        &self,
        action: &'static str,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
    ) {
        range(
            self,
            event::REACTOR_EVENT_SENT,
            action,
            peer,
            start,
            count.into(),
            None,
            None,
            None,
            None,
        );
    }

    fn trace_block_range_query_failed(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        reason: &str,
        started: Instant,
    ) {
        range(
            self,
            event::STATE_READ_ERROR,
            "query_blocks_by_height_range",
            peer,
            start,
            count.into(),
            Some("error"),
            Some(reason),
            Some(saturating_millis(started.elapsed())),
            None,
        );
    }

    fn trace_block_range_query_timed_out(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        started: Instant,
    ) {
        range(
            self,
            event::STATE_READ_TIMEOUT,
            "query_blocks_by_height_range",
            peer,
            start,
            count.into(),
            None,
            None,
            Some(saturating_millis(started.elapsed())),
            None,
        );
    }

    fn trace_block_range_finished(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        requested: u32,
        returned: u32,
    ) {
        range(
            self,
            event::REACTOR_EVENT_SENT,
            "block_range_response_finished",
            peer,
            start,
            returned.into(),
            None,
            None,
            None,
            Some(requested.into()),
        );
    }

    fn trace_block_submit_queued(
        &self,
        token: BlockApplyToken,
        class: BlockApplyClass,
        block: &block::Block,
        queue_len: usize,
        in_flight_count: usize,
    ) {
        self.emit_event(|| DriverEvent {
            event: event::BLOCK_SUBMIT_QUEUED,
            source: SOURCE,
            fields: SubmitQueued {
                apply_token: token,
                apply_class: class_label(class),
                hash: block.hash().to_string(),
                height: block.coinbase_height().map(|height| height.0.into()),
                queue_len: saturating_count(queue_len),
                in_flight_count: saturating_count(in_flight_count),
            },
        });
    }

    fn trace_block_apply_finished(
        &self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        result: BlockApplyResult,
        local_frontier: bool,
    ) {
        emit(self, event::REACTOR_EVENT_SENT, || ApplyFinished {
            action: "block_apply_finished",
            apply_token: token,
            height: height.0.into(),
            hash: hash.to_string(),
            result: result_label(result),
            local_frontier,
        });
    }

    fn trace_block_commit_started(
        &self,
        token: BlockApplyToken,
        class: BlockApplyClass,
        height: block::Height,
        hash: block::Hash,
    ) {
        emit(self, event::COMMIT_START, || ApplyIdentity {
            apply_token: token,
            apply_class: class_label(class),
            height: height.0.into(),
            hash: hash.to_string(),
        });
    }

    fn trace_block_commit_finished(
        &self,
        token: BlockApplyToken,
        class: BlockApplyClass,
        height: block::Height,
        hash: block::Hash,
        result: BlockApplyResult,
        started: Instant,
    ) {
        emit(self, event::COMMIT_FINISH, || CommitFinished {
            apply_token: token,
            apply_class: class_label(class),
            height: height.0.into(),
            hash: hash.to_string(),
            result: result_label(result),
            elapsed_ms: saturating_millis(started.elapsed()),
        });
    }

    fn trace_block_commit_stalled(
        &self,
        token: BlockApplyToken,
        class: BlockApplyClass,
        height: block::Height,
        hash: block::Hash,
        elapsed: std::time::Duration,
    ) {
        emit(self, event::COMMIT_STALLED, || CommitStalled {
            apply_token: token,
            apply_class: class_label(class),
            height: height.0.into(),
            hash: hash.to_string(),
            elapsed_ms: saturating_millis(elapsed),
        });
    }

    fn trace_block_frontier_query_started(
        &self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
    ) {
        emit(self, event::FRONTIER_QUERY_START, || FrontierQuery {
            apply_token: token,
            height: height.0.into(),
            hash: hash.to_string(),
            local_frontier: None,
            frontiers: None,
        });
    }

    fn trace_block_frontier_query_finished(
        &self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        frontiers: Option<&BlockSyncFrontiers>,
    ) {
        emit(self, event::FRONTIER_QUERY_FINISH, || FrontierQuery {
            apply_token: token,
            height: height.0.into(),
            hash: hash.to_string(),
            local_frontier: Some(frontiers.is_some()),
            frontiers: frontiers.map(Frontiers::from),
        });
    }

    fn trace_checkpoint_refresh_attempt(&self, attempts_remaining: usize, tip: block::Height) {
        emit(self, event::CHECKPOINT_REFRESH_ATTEMPT, || RefreshAttempt {
            attempts_remaining: saturating_count(attempts_remaining),
            verified_block_tip: tip.0.into(),
        });
    }

    fn trace_checkpoint_refresh_sent(&self, frontiers: &BlockSyncFrontiers) {
        emit(self, event::CHECKPOINT_REFRESH_SENT, || {
            Frontiers::from(frontiers)
        });
    }
}

fn emit<F: Serialize>(trace: &ZakuraTrace, name: &'static str, fields: impl FnOnce() -> F) {
    trace.emit_event(|| DriverEvent {
        event: name,
        source: SOURCE,
        fields: fields(),
    });
}

#[allow(clippy::too_many_arguments)]
fn range(
    trace: &ZakuraTrace,
    name: &'static str,
    action: &'static str,
    peer: &ZakuraPeerId,
    start: block::Height,
    count: u64,
    result: Option<&'static str>,
    reason: Option<&str>,
    elapsed_ms: Option<u64>,
    requested_count: Option<u64>,
) {
    trace.emit_event(|| DriverEvent {
        event: name,
        source: SOURCE,
        fields: Range {
            action,
            peer: zakura_trace_peer_label(peer),
            range_start: start.0.into(),
            range_count: count,
            result,
            reason,
            elapsed_ms,
            requested_count,
        },
    });
}

fn class_label(class: BlockApplyClass) -> &'static str {
    match class {
        BlockApplyClass::Checkpoint => "checkpoint",
        BlockApplyClass::Full => "full",
    }
}

fn result_label(result: BlockApplyResult) -> &'static str {
    match result {
        BlockApplyResult::Committed => "committed",
        BlockApplyResult::Duplicate => "duplicate",
        BlockApplyResult::Rejected => "rejected",
        BlockApplyResult::TimedOut => "timed_out",
    }
}

fn misbehavior_label(reason: BlockSyncMisbehavior) -> &'static str {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_driver_schema_preserves_key_order_and_sparse_fields() {
        let row = DriverEvent {
            event: event::FRONTIER_QUERY_FINISH,
            source: SOURCE,
            fields: FrontierQuery {
                apply_token: 7,
                height: 42,
                hash: "abcd".to_string(),
                local_frontier: Some(true),
                frontiers: Some(Frontiers {
                    finalized_height: 40,
                    verified_block_tip: 42,
                    verified_block_hash: "dcba".to_string(),
                }),
            },
        };

        let serialized = serde_json::to_string(&row).expect("block driver row serializes");
        assert_eq!(
            serialized,
            r#"{"event":"frontier_query_finish","source":"block_sync_driver","height":42,"hash":"abcd","apply_token":7,"finalized_height":40,"verified_block_tip":42,"verified_block_hash":"dcba","local_frontier":true}"#
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&serialized).expect("valid JSON"),
            serde_json::json!({
                "event": "frontier_query_finish",
                "source": "block_sync_driver",
                "apply_token": 7,
                "height": 42,
                "hash": "abcd",
                "local_frontier": true,
                "finalized_height": 40,
                "verified_block_tip": 42,
                "verified_block_hash": "dcba",
            })
        );

        let sparse = DriverEvent {
            event: event::FRONTIER_QUERY_START,
            source: SOURCE,
            fields: FrontierQuery {
                apply_token: 7,
                height: 42,
                hash: "abcd".to_string(),
                local_frontier: None,
                frontiers: None,
            },
        };
        assert_eq!(
            serde_json::to_string(&sparse).expect("sparse row serializes"),
            r#"{"event":"frontier_query_start","source":"block_sync_driver","height":42,"hash":"abcd","apply_token":7}"#
        );

        let apply = DriverEvent {
            event: event::COMMIT_FINISH,
            source: SOURCE,
            fields: CommitFinished {
                height: 42,
                hash: "abcd".to_string(),
                result: "committed",
                apply_token: 7,
                apply_class: "full",
                elapsed_ms: 9,
            },
        };
        assert_eq!(
            serde_json::to_string(&apply).expect("apply row serializes"),
            r#"{"event":"commit_finish","source":"block_sync_driver","height":42,"hash":"abcd","result":"committed","apply_token":7,"apply_class":"full","elapsed_ms":9}"#
        );
    }
}
