//! Typed JSONL events emitted by the zakurad Zakura sync drivers.

use serde::Serialize;
use zakura_chain::block;
use zakura_network::zakura::{
    commit_state_trace as cs_trace, zakura_trace_peer_label, BlockSyncFrontiers, ZakuraPeerId,
    ZakuraTrace, COMMIT_STATE_TABLE,
};

#[derive(Debug, Serialize)]
pub(crate) struct CommitStateEvent {
    event: &'static str,
    source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range_start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tree_aux_roots_len: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_variant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_debug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    apply_token: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    apply_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finalized_height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verified_block_tip: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verified_block_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_header_tip: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_ms: Option<u64>,
    #[serde(rename = "queue_len", skip_serializing_if = "Option::is_none")]
    peer_queue_len: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_flight_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_frontier: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempts_remaining: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_count: Option<u64>,
}

impl CommitStateEvent {
    fn new(event: &'static str, source: &'static str) -> Self {
        Self {
            event,
            source,
            action: None,
            peer: None,
            height: None,
            hash: None,
            range_start: None,
            range_count: None,
            tree_aux_roots_len: None,
            result: None,
            error_variant: None,
            error_debug: None,
            reason: None,
            apply_token: None,
            apply_class: None,
            finalized_height: None,
            verified_block_tip: None,
            verified_block_hash: None,
            best_header_tip: None,
            elapsed_ms: None,
            peer_queue_len: None,
            in_flight_count: None,
            local_frontier: None,
            attempts_remaining: None,
            requested_count: None,
        }
    }
}

zakura_jsonl_trace::impl_jsonl_trace_event!(CommitStateEvent, COMMIT_STATE_TABLE);

pub(crate) fn emit_commit_state(
    trace: &ZakuraTrace,
    event: &'static str,
    source: &'static str,
    build: impl FnOnce(&mut CommitStateEvent),
) {
    trace.emit_event(|| {
        let mut event = CommitStateEvent::new(event, source);
        build(&mut event);
        event
    });
}

pub(crate) fn insert_cs_height(
    event: &mut CommitStateEvent,
    key: &'static str,
    height: block::Height,
) {
    insert_cs_u64(event, key, u64::from(height.0));
}

pub(crate) fn insert_cs_hash(event: &mut CommitStateEvent, key: &'static str, hash: block::Hash) {
    let value = hash.to_string();
    match key {
        cs_trace::HASH => event.hash = Some(value),
        cs_trace::VERIFIED_BLOCK_HASH => event.verified_block_hash = Some(value),
        _ => unreachable!("unsupported commit-state hash field: {key}"),
    }
}

pub(crate) fn insert_cs_peer(event: &mut CommitStateEvent, key: &'static str, peer: &ZakuraPeerId) {
    match key {
        cs_trace::PEER => event.peer = Some(zakura_trace_peer_label(peer)),
        _ => unreachable!("unsupported commit-state peer field: {key}"),
    }
}

pub(crate) fn insert_cs_u64(event: &mut CommitStateEvent, key: &'static str, value: u64) {
    match key {
        cs_trace::HEIGHT => event.height = Some(value),
        cs_trace::RANGE_START => event.range_start = Some(value),
        cs_trace::RANGE_COUNT => event.range_count = Some(value),
        cs_trace::TREE_AUX_ROOTS_LEN => event.tree_aux_roots_len = Some(value),
        cs_trace::APPLY_TOKEN => event.apply_token = Some(value),
        cs_trace::FINALIZED_HEIGHT => event.finalized_height = Some(value),
        cs_trace::VERIFIED_BLOCK_TIP => event.verified_block_tip = Some(value),
        cs_trace::BEST_HEADER_TIP => event.best_header_tip = Some(value),
        cs_trace::ELAPSED_MS => event.elapsed_ms = Some(value),
        cs_trace::QUEUE_LEN => event.peer_queue_len = Some(value),
        cs_trace::IN_FLIGHT_COUNT => event.in_flight_count = Some(value),
        "attempts_remaining" => event.attempts_remaining = Some(value),
        "requested_count" => event.requested_count = Some(value),
        _ => unreachable!("unsupported commit-state numeric field: {key}"),
    }
}

pub(crate) fn insert_cs_bool(event: &mut CommitStateEvent, key: &'static str, value: bool) {
    match key {
        cs_trace::LOCAL_FRONTIER => event.local_frontier = Some(value),
        _ => unreachable!("unsupported commit-state boolean field: {key}"),
    }
}

pub(crate) fn insert_cs_str(event: &mut CommitStateEvent, key: &'static str, value: &str) {
    let value = value.to_string();
    match key {
        cs_trace::ACTION => event.action = Some(value),
        cs_trace::RESULT => event.result = Some(value),
        cs_trace::ERROR_VARIANT => event.error_variant = Some(value),
        cs_trace::ERROR_DEBUG => event.error_debug = Some(value),
        cs_trace::REASON => event.reason = Some(value),
        cs_trace::APPLY_CLASS => event.apply_class = Some(value),
        _ => unreachable!("unsupported commit-state string field: {key}"),
    }
}

pub(crate) fn insert_cs_frontiers(event: &mut CommitStateEvent, frontiers: &BlockSyncFrontiers) {
    event.finalized_height = Some(u64::from(frontiers.finalized_height.0));
    event.verified_block_tip = Some(u64::from(frontiers.verified_block_tip.0));
    event.verified_block_hash = Some(frontiers.verified_block_hash.to_string());
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn optional_fields_are_omitted() {
        let event = CommitStateEvent::new(cs_trace::STATE_READ_START, "header_sync_driver");
        assert_eq!(
            serde_json::to_value(event).expect("event serializes"),
            json!({"event": "state_read_start", "source": "header_sync_driver"})
        );
    }

    #[test]
    fn typed_fields_preserve_scalar_schema() {
        let mut event = CommitStateEvent::new(cs_trace::COMMIT_FINISH, "block_sync_driver");
        insert_cs_height(&mut event, cs_trace::HEIGHT, block::Height(42));
        insert_cs_hash(&mut event, cs_trace::HASH, block::Hash([7; 32]));
        insert_cs_bool(&mut event, cs_trace::LOCAL_FRONTIER, false);
        insert_cs_u64(&mut event, cs_trace::QUEUE_LEN, 3);
        let value = serde_json::to_value(event).expect("event serializes");
        assert_eq!(value["height"], json!(42));
        assert_eq!(value["queue_len"], json!(3));
        assert!(value.get("peer_queue_len").is_none());
        assert_eq!(value["local_frontier"], json!(false));
        assert!(value["hash"].as_str().is_some());
    }
}
