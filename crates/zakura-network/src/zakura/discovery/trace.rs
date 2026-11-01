//! Typed JSONL events for native discovery.

use serde::Serialize;

use super::candidate_dialer::DiscoveryDialResult;
use super::requester::QueryKind;
use crate::{
    zakura::{
        trace::{discovery_trace as d_trace, peer_label, DISCOVERY_TABLE},
        ZakuraPeerId,
    },
    BoxError,
};

/// The dial loop that owns an attempt. Each loop numbers its attempts on its
/// own, so a row's owner and attempt number together name the attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DialOwner {
    /// The candidate dialer, which numbers attempts across all its peers.
    Candidate,
    /// One peer's redial supervisor, which numbers that peer's attempts.
    Redial,
}

impl DialOwner {
    fn label(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Redial => "redial",
        }
    }
}

/// How far one dial attempt got.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DialPhase {
    /// The attempt's owner recorded it. The candidate dialer has also reserved
    /// its IPs by then.
    Recorded,
    /// The peer appeared in the registrations during the attempt.
    RegistrationSeen,
    /// The peer left the registrations during the attempt.
    RegistrationEnded,
    /// The attempt's worker returned its classified result.
    WorkerFinished,
    /// Shutdown aborted the attempt's worker.
    WorkerCancelled,
    /// The attempt's worker panicked.
    WorkerPanicked,
}

impl DialPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::RegistrationSeen => "registration_seen",
            Self::RegistrationEnded => "registration_ended",
            Self::WorkerFinished => "worker_finished",
            Self::WorkerCancelled => "worker_cancelled",
            Self::WorkerPanicked => "worker_panicked",
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct DiscoveryDialPhaseEvent {
    event: &'static str,
    peer: Option<String>,
    owner: &'static str,
    attempt: u64,
    phase: &'static str,
}

impl DiscoveryDialPhaseEvent {
    pub(super) fn new(node_id: &[u8], owner: DialOwner, attempt: u64, phase: DialPhase) -> Self {
        let peer = ZakuraPeerId::new(node_id.to_vec())
            .map(|peer| peer_label(&peer))
            .ok();
        Self {
            event: d_trace::DISCOVERY_DIAL_PHASE,
            peer,
            owner: owner.label(),
            attempt,
            phase: phase.label(),
        }
    }
}

zakura_jsonl_trace::impl_jsonl_trace_event!(DiscoveryDialPhaseEvent, DISCOVERY_TABLE);

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

/// How far one claimed query answer got.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AnswerPhase {
    /// The answer claimed its query, before decode.
    Claimed,
    /// The answer's import finished and counted toward the exchange.
    Imported,
    /// The answer settled without counting: a rejection, a replaced session,
    /// or a reader cancelled inside verification.
    Dropped,
}

impl AnswerPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Imported => "imported",
            Self::Dropped => "dropped",
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct DiscoveryAnswerEvent {
    event: &'static str,
    peer: String,
    session_id: u64,
    query: &'static str,
    phase: &'static str,
}

impl DiscoveryAnswerEvent {
    pub(super) fn new(
        peer: &ZakuraPeerId,
        session_id: u64,
        query: QueryKind,
        phase: AnswerPhase,
    ) -> Self {
        Self {
            event: d_trace::DISCOVERY_ANSWER,
            peer: peer_label(peer),
            session_id,
            query: query.label(),
            phase: phase.label(),
        }
    }
}

zakura_jsonl_trace::impl_jsonl_trace_event!(DiscoveryAnswerEvent, DISCOVERY_TABLE);

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
    fn dial_phase_schema_carries_the_attempt_identity() {
        let peer = ZakuraPeerId::new(vec![7; 32]).expect("a 32-byte id is a valid peer id");
        let event = DiscoveryDialPhaseEvent::new(
            &[7; 32],
            DialOwner::Candidate,
            5,
            DialPhase::WorkerCancelled,
        );
        assert_eq!(
            serde_json::to_value(event).expect("event serializes"),
            json!({
                "event": "discovery_dial_phase",
                "peer": peer_label(&peer),
                "owner": "candidate",
                "attempt": 5,
                "phase": "worker_cancelled",
            })
        );
    }

    #[test]
    fn answer_schema_names_the_query_and_phase() {
        let peer = ZakuraPeerId::new(vec![7; 32]).expect("a 32-byte id is a valid peer id");
        let event = DiscoveryAnswerEvent::new(&peer, 3, QueryKind::Services, AnswerPhase::Dropped);
        assert_eq!(
            serde_json::to_value(event).expect("event serializes"),
            json!({
                "event": "discovery_answer",
                "peer": peer_label(&peer),
                "session_id": 3,
                "query": "services",
                "phase": "dropped",
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
