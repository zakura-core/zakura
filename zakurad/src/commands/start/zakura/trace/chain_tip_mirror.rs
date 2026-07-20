//! Semantic commit-state tracing for the chain-tip mirror.

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::JsonlTraceEvent;
use zakura_network::zakura::{commit_state_trace as event, ZakuraTrace, COMMIT_STATE_TABLE};

const SOURCE: &str = "chain_tip_mirror";

#[derive(Serialize)]
struct MirrorEvent<F> {
    event: &'static str,
    source: &'static str,
    #[serde(flatten)]
    fields: F,
}

impl<F: Serialize> JsonlTraceEvent for MirrorEvent<F> {
    const TABLE: zakura_jsonl_trace::JsonlTraceTable = COMMIT_STATE_TABLE;
}

#[derive(Serialize)]
struct ActionTip {
    action: &'static str,
    height: u64,
    hash: String,
}

#[derive(Serialize)]
struct FinalizedTip {
    action: &'static str,
    finalized_height: u64,
}

#[derive(Serialize)]
struct Frontiers {
    action: &'static str,
    finalized_height: u64,
    verified_block_tip: u64,
    verified_block_hash: String,
}

#[derive(Serialize)]
struct TipResult {
    action: &'static str,
    height: u64,
    hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'static str>,
}

#[derive(Serialize)]
struct Failure<'a> {
    action: &'static str,
    reason: &'a str,
}

pub(crate) trait ChainTipMirrorTraceExt {
    fn trace_chain_tip_action(
        &self,
        action: &zakura_state::TipAction,
        height: block::Height,
        hash: block::Hash,
    );
    fn trace_finalized_tip_read(&self, height: block::Height);
    fn trace_mirror_frontier_derived(
        &self,
        finalized: block::Height,
        verified: (block::Height, block::Hash),
    );
    fn trace_mirror_frontier_sent(
        &self,
        finalized: block::Height,
        verified: (block::Height, block::Hash),
    );
    fn trace_committed_tip_lookup_started(&self, height: block::Height, hash: block::Hash);
    fn trace_committed_tip_lookup_finished(
        &self,
        height: block::Height,
        hash: block::Hash,
        result: &'static str,
    );
    fn trace_full_block_committed(&self, height: block::Height, hash: block::Hash);
    fn trace_committed_tip_lookup_failed(&self, reason: &str);
}

impl ChainTipMirrorTraceExt for ZakuraTrace {
    fn trace_chain_tip_action(
        &self,
        action: &zakura_state::TipAction,
        height: block::Height,
        hash: block::Hash,
    ) {
        emit(self, event::CHAIN_TIP_ACTION, || ActionTip {
            action: tip_action_label(action),
            height: height.0.into(),
            hash: hash.to_string(),
        });
    }

    fn trace_finalized_tip_read(&self, height: block::Height) {
        emit(self, event::STATE_READ_SUCCESS, || FinalizedTip {
            action: "finalized_tip",
            finalized_height: height.0.into(),
        });
    }

    fn trace_mirror_frontier_derived(
        &self,
        finalized: block::Height,
        verified: (block::Height, block::Hash),
    ) {
        emit(self, event::FRONTIER_DERIVED, || Frontiers {
            action: "sync_exchange_frontier_derived",
            finalized_height: finalized.0.into(),
            verified_block_tip: verified.0 .0.into(),
            verified_block_hash: verified.1.to_string(),
        });
    }

    fn trace_mirror_frontier_sent(
        &self,
        finalized: block::Height,
        verified: (block::Height, block::Hash),
    ) {
        emit(self, event::FRONTIER_DERIVED, || Frontiers {
            action: "sync_exchange_frontier_sent",
            finalized_height: finalized.0.into(),
            verified_block_tip: verified.0 .0.into(),
            verified_block_hash: verified.1.to_string(),
        });
    }

    fn trace_committed_tip_lookup_started(&self, height: block::Height, hash: block::Hash) {
        emit(self, event::STATE_READ_START, || TipResult {
            action: "committed_tip_block",
            height: height.0.into(),
            hash: hash.to_string(),
            result: None,
        });
    }

    fn trace_committed_tip_lookup_finished(
        &self,
        height: block::Height,
        hash: block::Hash,
        result: &'static str,
    ) {
        emit(self, event::STATE_READ_SUCCESS, || TipResult {
            action: "committed_tip_block",
            height: height.0.into(),
            hash: hash.to_string(),
            result: Some(result),
        });
    }

    fn trace_full_block_committed(&self, height: block::Height, hash: block::Hash) {
        emit(self, event::REACTOR_EVENT_SENT, || TipResult {
            action: "full_block_committed",
            height: height.0.into(),
            hash: hash.to_string(),
            result: None,
        });
    }

    fn trace_committed_tip_lookup_failed(&self, reason: &str) {
        emit(self, event::STATE_READ_ERROR, || Failure {
            action: "committed_tip_block",
            reason,
        });
    }
}

fn emit<F: Serialize>(trace: &ZakuraTrace, name: &'static str, fields: impl FnOnce() -> F) {
    trace.emit_event(|| MirrorEvent {
        event: name,
        source: SOURCE,
        fields: fields(),
    });
}

fn tip_action_label(action: &zakura_state::TipAction) -> &'static str {
    match action {
        zakura_state::TipAction::Grow { .. } => "grow",
        zakura_state::TipAction::Reset { .. } => "reset",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mirror_schema_preserves_source_order_and_sparse_result() {
        let row = MirrorEvent {
            event: event::STATE_READ_START,
            source: SOURCE,
            fields: TipResult {
                action: "committed_tip_block",
                height: 12,
                hash: "abcd".to_string(),
                result: None,
            },
        };
        assert_eq!(
            serde_json::to_string(&row).expect("mirror row serializes"),
            r#"{"event":"state_read_start","source":"chain_tip_mirror","action":"committed_tip_block","height":12,"hash":"abcd"}"#
        );
    }
}
