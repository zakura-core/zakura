//! Typed trace events for the shared sync exchange.

use serde::Serialize;

use super::{ChainFrontier, FrontierChange, FrontierUpdate, ZakuraSyncExchange};
use crate::zakura::{commit_state_trace as cs_trace, COMMIT_STATE_TABLE};

#[derive(Debug, Serialize)]
pub(super) struct SyncFrontierTransition {
    event: &'static str,
    source: &'static str,
    sequence: u64,
    cause: &'static str,
    result: &'static str,
    old_finalized_height: u64,
    old_finalized_hash: String,
    old_verified_body_height: u64,
    old_verified_body_hash: String,
    old_best_header_height: u64,
    old_best_header_hash: String,
    new_finalized_height: u64,
    new_finalized_hash: String,
    new_verified_body_height: u64,
    new_verified_body_hash: String,
    new_best_header_height: u64,
    new_best_header_hash: String,
}

impl SyncFrontierTransition {
    pub(super) fn new(
        sequence: u64,
        source: &'static str,
        old: FrontierUpdate,
        new: FrontierUpdate,
        result: &'static str,
    ) -> Self {
        let ChainFrontier {
            finalized: old_finalized,
            verified_body: old_verified_body,
            best_header: old_best_header,
        } = old.frontier;
        let ChainFrontier {
            finalized: new_finalized,
            verified_body: new_verified_body,
            best_header: new_best_header,
        } = new.frontier;

        Self {
            event: cs_trace::SYNC_FRONTIER_TRANSITION,
            source,
            sequence,
            cause: frontier_change_label(new.change),
            result,
            old_finalized_height: u64::from(old_finalized.height.0),
            old_finalized_hash: old_finalized.hash.to_string(),
            old_verified_body_height: u64::from(old_verified_body.height.0),
            old_verified_body_hash: old_verified_body.hash.to_string(),
            old_best_header_height: u64::from(old_best_header.height.0),
            old_best_header_hash: old_best_header.hash.to_string(),
            new_finalized_height: u64::from(new_finalized.height.0),
            new_finalized_hash: new_finalized.hash.to_string(),
            new_verified_body_height: u64::from(new_verified_body.height.0),
            new_verified_body_hash: new_verified_body.hash.to_string(),
            new_best_header_height: u64::from(new_best_header.height.0),
            new_best_header_hash: new_best_header.hash.to_string(),
        }
    }
}

zakura_jsonl_trace::impl_jsonl_trace_event!(SyncFrontierTransition, COMMIT_STATE_TABLE);

impl ZakuraSyncExchange {
    pub(super) fn trace_transition(
        &self,
        sequence: u64,
        source: &'static str,
        old: FrontierUpdate,
        new: FrontierUpdate,
        result: &'static str,
    ) {
        self.inner
            .trace
            .emit_event(|| SyncFrontierTransition::new(sequence, source, old, new, result));
    }
}

fn frontier_change_label(change: FrontierChange) -> &'static str {
    match change {
        FrontierChange::Snapshot => "snapshot",
        FrontierChange::VerifiedGrow => "verified_grow",
        FrontierChange::VerifiedReset => "verified_reset",
        FrontierChange::HeaderAdvanced => "header_advanced",
        FrontierChange::HeaderReanchored => "header_reanchored",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use zakura_chain::block::{Hash, Height};

    use super::*;
    use crate::zakura::Frontier;

    fn frontier(height: u32, seed: u8) -> ChainFrontier {
        ChainFrontier {
            finalized: Frontier::new(Height(height), Hash([seed; 32])),
            verified_body: Frontier::new(Height(height + 1), Hash([seed + 1; 32])),
            best_header: Frontier::new(Height(height + 2), Hash([seed + 2; 32])),
        }
    }

    #[test]
    fn transition_schema_is_flat_and_complete() {
        let old = FrontierUpdate {
            frontier: frontier(1, 1),
            change: FrontierChange::Snapshot,
        };
        let new = FrontierUpdate {
            frontier: frontier(4, 4),
            change: FrontierChange::VerifiedGrow,
        };
        let value = serde_json::to_value(SyncFrontierTransition::new(
            8, "driver", old, new, "accepted",
        ))
        .expect("event serializes");
        assert_eq!(value["event"], json!("sync_frontier_transition"));
        assert_eq!(value["sequence"], json!(8));
        assert_eq!(value["cause"], json!("verified_grow"));
        assert_eq!(value["old_finalized_height"], json!(1));
        assert_eq!(value["new_best_header_height"], json!(6));
        assert_eq!(value.as_object().expect("object").len(), 17);
    }
}
