//! Typed JSONL events for native discovery.

use serde::Serialize;

use super::candidate_dialer::DiscoveryDialResult;
use crate::zakura::{
    trace::{discovery_trace as d_trace, peer_label, DISCOVERY_TABLE},
    ZakuraPeerId,
};

#[derive(Debug, Serialize)]
pub(super) struct DiscoveryDialResultEvent {
    event: &'static str,
    result: &'static str,
    peer: Option<String>,
}

impl DiscoveryDialResultEvent {
    pub(super) fn new(node_id: &[u8], result: DiscoveryDialResult) -> Self {
        let peer = ZakuraPeerId::new(node_id.to_vec())
            .map(|peer| peer_label(&peer))
            .ok();
        Self {
            event: d_trace::DISCOVERY_DIAL_RESULT,
            result: result.label(),
            peer,
        }
    }
}

zakura_jsonl_trace::impl_jsonl_trace_event!(DiscoveryDialResultEvent, DISCOVERY_TABLE);

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn discovery_result_schema_preserves_null_invalid_peer() {
        let event = DiscoveryDialResultEvent::new(&[0; 1_025], DiscoveryDialResult::Failed);
        assert_eq!(
            serde_json::to_value(event).expect("event serializes"),
            json!({
                "event": "discovery_dial_result",
                "result": "failed",
                "peer": null,
            })
        );
    }
}
