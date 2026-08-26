use std::sync::{Arc, Mutex};

use blake2b_simd::Params;
use serde::Serialize;

use crate::zakura::{ControlHandshakeEvent, ControlHandshakeObserver, ControlHandshakeRole};

use super::{
    model::HandshakeOutcome,
    scenario::{FaultAction, HandshakeScenario, MessageMutation},
};

/// One stable event that excludes backend task ids and addresses.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct CanonicalEvent {
    pub sequence: u64,
    pub role: &'static str,
    pub action: &'static str,
    pub value: u64,
}

/// A canonical logical trace shared by every backend.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub(super) struct CanonicalTrace(pub Vec<CanonicalEvent>);

impl CanonicalTrace {
    pub fn hash(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("canonical trace serialization cannot fail");
        Params::new()
            .hash_length(32)
            .hash(&bytes)
            .to_hex()
            .to_string()
    }

    pub fn record_terminal(&mut self, outcome: &HandshakeOutcome) {
        let action = match outcome {
            HandshakeOutcome::Established => "outcome-established",
            HandshakeOutcome::NeutralClose(_) => "outcome-neutral-close",
            HandshakeOutcome::PeerViolation(_) => "outcome-peer-violation",
            HandshakeOutcome::LocalFault(_) => "outcome-local-fault",
            HandshakeOutcome::ResourceRejected(_) => "outcome-resource-rejected",
        };
        self.push("runner", action, 0);
        self.push("runner", "pending-role-futures", 0);
        self.push("runner", "open-owned-streams", 0);
    }

    fn push(&mut self, role: &'static str, action: &'static str, value: u64) {
        let sequence = u64::try_from(self.0.len()).expect("trace lengths fit u64");
        self.0.push(CanonicalEvent {
            sequence,
            role,
            action,
            value,
        });
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct TraceRecorder(Arc<Mutex<CanonicalTrace>>);

impl TraceRecorder {
    pub fn for_scenario(scenario: &HandshakeScenario) -> Self {
        let recorder = Self::default();
        let mutation = match scenario.mutation {
            MessageMutation::None => "mutation-none",
            MessageMutation::HelloZeroLength => "mutation-hello-zero-length",
            MessageMutation::HelloOversizeLength => "mutation-hello-oversize-length",
            MessageMutation::HelloBadMagic => "mutation-hello-bad-magic",
            MessageMutation::HelloUnsupportedControlVersion => {
                "mutation-hello-unsupported-control-version"
            }
            MessageMutation::HelloWrongProtocolVersion => "mutation-hello-wrong-protocol-version",
            MessageMutation::HelloWrongPath => "mutation-hello-wrong-path",
            MessageMutation::HelloWrongRole => "mutation-hello-wrong-role",
            MessageMutation::HelloWrongNetwork => "mutation-hello-wrong-network",
            MessageMutation::HelloWrongChain => "mutation-hello-wrong-chain",
            MessageMutation::HelloWrongIdentity => "mutation-hello-wrong-identity",
            MessageMutation::HelloUpgradeNonce => "mutation-hello-upgrade-nonce",
            MessageMutation::HelloTranscript => "mutation-hello-transcript",
            MessageMutation::HelloMissingCapability => "mutation-hello-missing-capability",
            MessageMutation::HelloUnsupportedChannel => "mutation-hello-unsupported-channel",
            MessageMutation::HelloZeroLimit => "mutation-hello-zero-limit",
            MessageMutation::HelloTrailingByte => "mutation-hello-trailing-byte",
            MessageMutation::AckBadMagic => "mutation-ack-bad-magic",
            MessageMutation::AckZeroLength => "mutation-ack-zero-length",
            MessageMutation::AckOversizeLength => "mutation-ack-oversize-length",
            MessageMutation::AckWrongProtocolVersion => "mutation-ack-wrong-protocol-version",
            MessageMutation::AckWrongRemoteNonce => "mutation-ack-wrong-remote-nonce",
            MessageMutation::AckExtraCapability => "mutation-ack-extra-capability",
            MessageMutation::AckExtraChannel => "mutation-ack-extra-channel",
            MessageMutation::AckZeroLimit => "mutation-ack-zero-limit",
            MessageMutation::AckLimitAboveRequest => "mutation-ack-limit-above-request",
            MessageMutation::AckTrailingByte => "mutation-ack-trailing-byte",
        };
        let (fault, value) = match scenario.fault {
            FaultAction::None => ("fault-none", 0),
            FaultAction::DelayMillis(delay) => ("fault-delay-millis", u64::from(delay)),
            FaultAction::ShortWrites(bytes) => ("fault-short-writes", u64::from(bytes)),
            FaultAction::StallBeforeHello => ("fault-stall-before-hello", 0),
            FaultAction::StallBeforeAck => ("fault-stall-before-ack", 0),
            FaultAction::CloseBeforeHello => ("fault-close-before-hello", 0),
            FaultAction::CloseBeforeAck => ("fault-close-before-ack", 0),
            FaultAction::CancelBeforeHello => ("fault-cancel-before-hello", 0),
            FaultAction::CancelBeforeAck => ("fault-cancel-before-ack", 0),
        };
        {
            let mut trace = recorder
                .0
                .lock()
                .expect("the handshake trace mutex is not poisoned");
            trace.push("runner", "scenario-start", scenario.seed);
            trace.push("runner", mutation, 0);
            trace.push("runner", fault, value);
        }
        recorder
    }

    pub fn snapshot(&self) -> CanonicalTrace {
        self.0
            .lock()
            .expect("the handshake trace mutex is not poisoned")
            .clone()
    }
}

impl ControlHandshakeObserver for TraceRecorder {
    fn record(&self, event: ControlHandshakeEvent) {
        let (role, action, value) = match event {
            ControlHandshakeEvent::Started(role) => (role, "started", 0),
            ControlHandshakeEvent::LengthRead(role, length) => {
                (role, "length-read", u64::from(length))
            }
            ControlHandshakeEvent::PayloadRead(role, length) => {
                let length = u64::try_from(length).expect("payload lengths fit u64");
                (role, "payload-read", length)
            }
            ControlHandshakeEvent::MessageValidated(role) => (role, "validated", 0),
            ControlHandshakeEvent::MessageWritten(role, length) => {
                let length = u64::try_from(length).expect("payload lengths fit u64");
                (role, "message-written", length)
            }
            ControlHandshakeEvent::Established(role) => (role, "established", 0),
        };
        let role = match role {
            ControlHandshakeRole::Initiator => "initiator",
            ControlHandshakeRole::Responder => "responder",
        };
        let mut trace = self
            .0
            .lock()
            .expect("the handshake trace mutex is not poisoned");
        trace.push(role, action, value);
    }
}
