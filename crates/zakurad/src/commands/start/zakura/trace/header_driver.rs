//! Semantic commit-state tracing for the header-sync driver.

use std::time::Instant;

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{saturating_count, saturating_millis, JsonlTraceEvent};
use zakura_network::zakura::{
    commit_state_trace as event, zakura_trace_peer_label, HeaderSyncAction,
    HeaderSyncCommitFailureKind, HeaderSyncMisbehavior, ZakuraPeerId, ZakuraTrace,
    COMMIT_STATE_TABLE,
};

use super::super::header_sync_driver::{
    header_range_commit_error_debug, header_range_commit_error_label,
};

const SOURCE: &str = "header_sync_driver";

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
#[serde(untagged)]
enum ReceivedAction {
    HeaderRange {
        action: &'static str,
        peer: String,
        range_start: u64,
        range_count: u64,
    },
    Action {
        action: &'static str,
    },
    Range {
        action: &'static str,
        range_start: u64,
        range_count: u64,
    },
    Misbehavior {
        action: &'static str,
        peer: String,
        reason: &'static str,
    },
    Tip {
        action: &'static str,
        height: u64,
        hash: String,
    },
    Reanchored {
        action: &'static str,
        height: u64,
        hash: String,
        best_header_tip: u64,
    },
    PeerTip {
        action: &'static str,
        peer: String,
        height: u64,
        hash: String,
    },
}

#[derive(Serialize)]
struct CommitFinish {
    action: &'static str,
    peer: String,
    height: u64,
    hash: String,
    result: &'static str,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct ReactorEvent {
    action: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer: Option<String>,
    height: u64,
    hash: String,
    range_count: u64,
}

#[derive(Serialize)]
struct RangeEvent<'a> {
    action: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer: Option<String>,
    range_start: u64,
    range_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_count: Option<u64>,
}

#[derive(Serialize)]
struct Tip {
    height: u64,
    hash: String,
}

#[derive(Serialize)]
struct PeerTip {
    action: &'static str,
    peer: String,
    height: u64,
    hash: String,
}

#[derive(Serialize)]
struct RangeSuccess {
    action: &'static str,
    peer: String,
    range_start: u64,
    range_count: u64,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct HeaderRangeCommitStart {
    action: &'static str,
    peer: String,
    hash: String,
    range_start: u64,
    range_count: u64,
    tree_aux_roots_len: u64,
}

#[derive(Serialize)]
struct HeaderRangeCommitFinish {
    action: &'static str,
    peer: String,
    range_start: u64,
    range_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tree_aux_roots_len: Option<u64>,
    result: &'static str,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct HeaderRangeCommitError {
    action: &'static str,
    peer: String,
    hash: String,
    range_start: u64,
    range_count: u64,
    result: &'static str,
    error_variant: &'static str,
    error_debug: String,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct BestHeaderTip {
    action: &'static str,
    hash: String,
    best_header_tip: u64,
}

#[derive(Serialize)]
struct UnpeeredRangeSuccess {
    action: &'static str,
    range_start: u64,
    range_count: u64,
    elapsed_ms: u64,
}

pub(crate) trait HeaderDriverTraceExt {
    fn trace_header_action_received(&self, action: &HeaderSyncAction);
    fn trace_header_commit_finished(
        &self,
        action: &'static str,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        result: &'static str,
        started: Instant,
    );
    fn trace_header_reactor_event(
        &self,
        action: &'static str,
        peer: Option<&ZakuraPeerId>,
        height: block::Height,
        hash: block::Hash,
        count: u32,
    );
    fn trace_header_range_finished(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        requested: u32,
        returned: u32,
    );
    fn trace_header_state_read_started(
        &self,
        action: &'static str,
        peer: Option<&ZakuraPeerId>,
        start: block::Height,
        count: u32,
    );
    fn trace_header_state_read_failed(
        &self,
        action: &'static str,
        peer: Option<&ZakuraPeerId>,
        start: block::Height,
        count: u32,
        reason: &str,
        started: Instant,
    );
    fn trace_block_sync_notified(&self, height: block::Height, hash: block::Hash);
    fn trace_new_block_commit_started(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    );
    fn trace_header_range_query_succeeded(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: usize,
        started: Instant,
    );
    fn trace_header_range_commit_started(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        tree_aux_roots_len: u32,
        anchor: block::Hash,
    );
    fn trace_header_range_commit_finished(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        tree_aux_roots_len: Option<u32>,
        result: &'static str,
        started: Instant,
    );
    #[allow(clippy::too_many_arguments)]
    fn trace_header_range_commit_failed(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        anchor: block::Hash,
        kind: HeaderSyncCommitFailureKind,
        error: &(dyn std::error::Error + Send + Sync + 'static),
        started: Instant,
    );
    fn trace_best_header_tip_query_started(&self);
    fn trace_best_header_tip_query_succeeded(&self, height: block::Height, hash: block::Hash);
    fn trace_missing_block_bodies_query_succeeded(
        &self,
        from: block::Height,
        count: usize,
        started: Instant,
    );
}

impl HeaderDriverTraceExt for ZakuraTrace {
    fn trace_header_action_received(&self, action: &HeaderSyncAction) {
        emit(self, event::ACTION_RECEIVED, || match action {
            HeaderSyncAction::CommitHeaderRange {
                operation, payload, ..
            } => ReceivedAction::HeaderRange {
                action: "commit_header_range",
                peer: zakura_trace_peer_label(&operation.wire_request.peer),
                range_start: payload.range().start().0.into(),
                range_count: payload.range().count().into(),
            },
            HeaderSyncAction::QueryBestHeaderTip => ReceivedAction::Action {
                action: "query_best_header_tip",
            },
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer, start, count, ..
            } => ReceivedAction::HeaderRange {
                action: "query_headers_by_height_range",
                peer: zakura_trace_peer_label(peer),
                range_start: start.0.into(),
                range_count: (*count).into(),
            },
            HeaderSyncAction::QueryMissingBlockBodies { from, limit } => ReceivedAction::Range {
                action: "query_missing_block_bodies",
                range_start: from.0.into(),
                range_count: (*limit).into(),
            },
            HeaderSyncAction::Misbehavior { peer, reason } => ReceivedAction::Misbehavior {
                action: "misbehavior",
                peer: zakura_trace_peer_label(peer),
                reason: misbehavior_label(*reason),
            },
            HeaderSyncAction::BodyGaps { from, to } => ReceivedAction::Range {
                action: "body_gaps",
                range_start: from.0.into(),
                range_count: to.0.saturating_sub(from.0).saturating_add(1).into(),
            },
            HeaderSyncAction::HeaderAdvanced { height, hash } => ReceivedAction::Tip {
                action: "header_advanced",
                height: height.0.into(),
                hash: hash.to_string(),
            },
            HeaderSyncAction::HeaderReanchored { old, new } => ReceivedAction::Reanchored {
                action: "header_reanchored",
                best_header_tip: old.0 .0.into(),
                height: new.0 .0.into(),
                hash: new.1.to_string(),
            },
            HeaderSyncAction::NewBlockReceived {
                peer, height, hash, ..
            } => ReceivedAction::PeerTip {
                action: "new_block_received",
                peer: zakura_trace_peer_label(peer),
                height: height.0.into(),
                hash: hash.to_string(),
            },
        });
    }

    fn trace_header_commit_finished(
        &self,
        action: &'static str,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        result: &'static str,
        started: Instant,
    ) {
        emit(self, event::COMMIT_FINISH, || CommitFinish {
            action,
            peer: zakura_trace_peer_label(peer),
            height: height.0.into(),
            hash: hash.to_string(),
            result,
            elapsed_ms: saturating_millis(started.elapsed()),
        });
    }

    fn trace_header_reactor_event(
        &self,
        action: &'static str,
        peer: Option<&ZakuraPeerId>,
        height: block::Height,
        hash: block::Hash,
        count: u32,
    ) {
        emit(self, event::REACTOR_EVENT_SENT, || ReactorEvent {
            action,
            peer: peer.map(zakura_trace_peer_label),
            height: height.0.into(),
            hash: hash.to_string(),
            range_count: count.into(),
        });
    }

    fn trace_header_range_finished(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        requested: u32,
        returned: u32,
    ) {
        emit(self, event::REACTOR_EVENT_SENT, || RangeEvent {
            action: "header_range_response_finished",
            peer: Some(zakura_trace_peer_label(peer)),
            range_start: start.0.into(),
            range_count: returned.into(),
            reason: None,
            elapsed_ms: None,
            requested_count: Some(requested.into()),
        });
    }

    fn trace_header_state_read_started(
        &self,
        action: &'static str,
        peer: Option<&ZakuraPeerId>,
        start: block::Height,
        count: u32,
    ) {
        emit(self, event::STATE_READ_START, || RangeEvent {
            action,
            peer: peer.map(zakura_trace_peer_label),
            range_start: start.0.into(),
            range_count: count.into(),
            reason: None,
            elapsed_ms: None,
            requested_count: None,
        });
    }

    fn trace_header_state_read_failed(
        &self,
        action: &'static str,
        peer: Option<&ZakuraPeerId>,
        start: block::Height,
        count: u32,
        reason: &str,
        started: Instant,
    ) {
        emit(self, event::STATE_READ_ERROR, || RangeEvent {
            action,
            peer: peer.map(zakura_trace_peer_label),
            range_start: start.0.into(),
            range_count: count.into(),
            reason: Some(reason),
            elapsed_ms: Some(saturating_millis(started.elapsed())),
            requested_count: None,
        });
    }

    fn trace_block_sync_notified(&self, height: block::Height, hash: block::Hash) {
        emit(self, event::BLOCK_SYNC_NOTIFY_SENT, || Tip {
            height: height.0.into(),
            hash: hash.to_string(),
        });
    }

    fn trace_new_block_commit_started(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    ) {
        emit(self, event::COMMIT_START, || PeerTip {
            action: "new_block",
            peer: zakura_trace_peer_label(peer),
            height: height.0.into(),
            hash: hash.to_string(),
        });
    }

    fn trace_header_range_query_succeeded(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: usize,
        started: Instant,
    ) {
        emit(self, event::STATE_READ_SUCCESS, || RangeSuccess {
            action: "query_headers_by_height_range",
            peer: zakura_trace_peer_label(peer),
            range_start: start.0.into(),
            range_count: saturating_count(count),
            elapsed_ms: saturating_millis(started.elapsed()),
        });
    }

    fn trace_header_range_commit_started(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        tree_aux_roots_len: u32,
        anchor: block::Hash,
    ) {
        emit(self, event::COMMIT_START, || HeaderRangeCommitStart {
            action: "commit_header_range",
            peer: zakura_trace_peer_label(peer),
            range_start: start.0.into(),
            range_count: count.into(),
            tree_aux_roots_len: tree_aux_roots_len.into(),
            hash: anchor.to_string(),
        });
    }

    fn trace_header_range_commit_finished(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        tree_aux_roots_len: Option<u32>,
        result: &'static str,
        started: Instant,
    ) {
        emit(self, event::COMMIT_FINISH, || HeaderRangeCommitFinish {
            action: "commit_header_range",
            peer: zakura_trace_peer_label(peer),
            range_start: start.0.into(),
            range_count: count.into(),
            tree_aux_roots_len: tree_aux_roots_len.map(Into::into),
            result,
            elapsed_ms: saturating_millis(started.elapsed()),
        });
    }

    fn trace_header_range_commit_failed(
        &self,
        peer: &ZakuraPeerId,
        start: block::Height,
        count: u32,
        anchor: block::Hash,
        kind: HeaderSyncCommitFailureKind,
        error: &(dyn std::error::Error + Send + Sync + 'static),
        started: Instant,
    ) {
        emit(self, event::COMMIT_FINISH, || HeaderRangeCommitError {
            action: "commit_header_range",
            peer: zakura_trace_peer_label(peer),
            range_start: start.0.into(),
            range_count: count.into(),
            result: match kind {
                HeaderSyncCommitFailureKind::InvalidPeerRange => "invalid_peer_range",
                HeaderSyncCommitFailureKind::Local => "local_error",
            },
            hash: anchor.to_string(),
            error_variant: header_range_commit_error_label(error),
            error_debug: header_range_commit_error_debug(error),
            elapsed_ms: saturating_millis(started.elapsed()),
        });
    }

    fn trace_best_header_tip_query_started(&self) {
        emit(self, event::STATE_READ_START, || ReceivedAction::Action {
            action: "query_best_header_tip",
        });
    }

    fn trace_best_header_tip_query_succeeded(&self, height: block::Height, hash: block::Hash) {
        emit(self, event::STATE_READ_SUCCESS, || BestHeaderTip {
            action: "query_best_header_tip",
            best_header_tip: height.0.into(),
            hash: hash.to_string(),
        });
    }

    fn trace_missing_block_bodies_query_succeeded(
        &self,
        from: block::Height,
        count: usize,
        started: Instant,
    ) {
        emit(self, event::STATE_READ_SUCCESS, || UnpeeredRangeSuccess {
            action: "missing_block_bodies",
            range_start: from.0.into(),
            range_count: saturating_count(count),
            elapsed_ms: saturating_millis(started.elapsed()),
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

fn misbehavior_label(reason: HeaderSyncMisbehavior) -> &'static str {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_driver_schema_preserves_order_and_optional_branches() {
        let committed = DriverEvent {
            event: event::COMMIT_FINISH,
            source: SOURCE,
            fields: HeaderRangeCommitFinish {
                action: "commit_header_range",
                peer: "peer".to_string(),
                range_start: 10,
                range_count: 3,
                tree_aux_roots_len: Some(3),
                result: "committed",
                elapsed_ms: 9,
            },
        };
        assert_eq!(
            serde_json::to_string(&committed).expect("header row serializes"),
            r#"{"event":"commit_finish","source":"header_sync_driver","action":"commit_header_range","peer":"peer","range_start":10,"range_count":3,"tree_aux_roots_len":3,"result":"committed","elapsed_ms":9}"#
        );

        let unexpected = DriverEvent {
            event: event::COMMIT_FINISH,
            source: SOURCE,
            fields: HeaderRangeCommitFinish {
                action: "commit_header_range",
                peer: "peer".to_string(),
                range_start: 10,
                range_count: 3,
                tree_aux_roots_len: None,
                result: "unexpected_response",
                elapsed_ms: 9,
            },
        };
        let serialized = serde_json::to_string(&unexpected).expect("header row serializes");
        assert_eq!(
            serialized,
            r#"{"event":"commit_finish","source":"header_sync_driver","action":"commit_header_range","peer":"peer","range_start":10,"range_count":3,"result":"unexpected_response","elapsed_ms":9}"#
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&serialized).expect("valid JSON"),
            serde_json::json!({
                "event": "commit_finish",
                "source": "header_sync_driver",
                "action": "commit_header_range",
                "peer": "peer",
                "range_start": 10,
                "range_count": 3,
                "result": "unexpected_response",
                "elapsed_ms": 9,
            })
        );

        let failed = DriverEvent {
            event: event::COMMIT_FINISH,
            source: SOURCE,
            fields: HeaderRangeCommitError {
                action: "commit_header_range",
                peer: "peer".to_string(),
                hash: "abcd".to_string(),
                range_start: 10,
                range_count: 3,
                result: "invalid_peer_range",
                error_variant: "invalid_range",
                error_debug: "debug".to_string(),
                elapsed_ms: 9,
            },
        };
        assert_eq!(
            serde_json::to_string(&failed).expect("failed header row serializes"),
            r#"{"event":"commit_finish","source":"header_sync_driver","action":"commit_header_range","peer":"peer","hash":"abcd","range_start":10,"range_count":3,"result":"invalid_peer_range","error_variant":"invalid_range","error_debug":"debug","elapsed_ms":9}"#
        );
    }
}
