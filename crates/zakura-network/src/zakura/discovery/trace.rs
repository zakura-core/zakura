//! Typed JSONL events for native discovery.

use serde::Serialize;

use super::candidate_dialer::DiscoveryDialResult;
use crate::{
    zakura::{
        trace::{discovery_trace as d_trace, peer_label, DISCOVERY_TABLE},
        ZakuraPeerId,
    },
    BoxError,
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

/// The discovery reader stage that rejected a peer's frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ViolationStage {
    /// The frame answers no live query, or is larger than its query reserved.
    Claim,
    /// The payload does not decode.
    Decode,
    /// The decoded answer does not match its query.
    Answer,
    /// The decoded message failed its handler: a bad record, a request past the
    /// serving limits, or `Services` that another node authored.
    Handle,
}

impl ViolationStage {
    fn label(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Decode => "decode",
            Self::Answer => "answer",
            Self::Handle => "handle",
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct DiscoveryViolationEvent {
    event: &'static str,
    peer: String,
    session_id: u64,
    message_type: u16,
    stage: &'static str,
    error: String,
}

impl DiscoveryViolationEvent {
    pub(super) fn new(
        peer: &ZakuraPeerId,
        session_id: u64,
        message_type: u16,
        stage: ViolationStage,
        error: &BoxError,
    ) -> Self {
        Self {
            event: d_trace::DISCOVERY_VIOLATION,
            peer: peer_label(peer),
            session_id,
            message_type,
            stage: stage.label(),
            error: error.to_string(),
        }
    }
}

zakura_jsonl_trace::impl_jsonl_trace_event!(DiscoveryViolationEvent, DISCOVERY_TABLE);

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

    #[test]
    fn violation_schema_carries_stage_and_error() {
        let peer = ZakuraPeerId::new(vec![7; 32]).expect("a 32-byte id is a valid peer id");
        let error: BoxError = "Peers answers no GetPeers".into();
        let event = DiscoveryViolationEvent::new(&peer, 3, 2, ViolationStage::Claim, &error);
        assert_eq!(
            serde_json::to_value(event).expect("event serializes"),
            json!({
                "event": "discovery_violation",
                "peer": peer_label(&peer),
                "session_id": 3,
                "message_type": 2,
                "stage": "claim",
                "error": "Peers answers no GetPeers",
            })
        );
    }
}
