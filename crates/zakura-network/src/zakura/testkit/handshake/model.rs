use serde::Serialize;

use crate::zakura::{NativeHandshakeNegotiated, ZakuraAcceptedLimits};

use super::scenario::{FaultAction, HandshakeScenario, MessageMutation};

/// The policy-level result that every backend reports.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) enum HandshakeOutcome {
    Established,
    NeutralClose(&'static str),
    PeerViolation(&'static str),
    LocalFault(&'static str),
    ResourceRejected(&'static str),
}

/// A small state model that does not perform I/O or decode bytes.
pub(super) struct ReferenceModel;

impl ReferenceModel {
    pub fn evaluate(scenario: &HandshakeScenario) -> HandshakeOutcome {
        match scenario.fault {
            FaultAction::StallBeforeHello => {
                return HandshakeOutcome::LocalFault("transport");
            }
            FaultAction::CloseBeforeHello => {
                return HandshakeOutcome::LocalFault("transport");
            }
            FaultAction::CancelBeforeHello => {
                return HandshakeOutcome::LocalFault("transport");
            }
            FaultAction::None
            | FaultAction::DelayMillis(_)
            | FaultAction::ShortWrites(_)
            | FaultAction::StallBeforeAck
            | FaultAction::CloseBeforeAck
            | FaultAction::CancelBeforeAck => {}
        }

        match scenario.mutation {
            MessageMutation::HelloZeroLength
            | MessageMutation::HelloOversizeLength
            | MessageMutation::HelloBadMagic
            | MessageMutation::HelloWrongPath
            | MessageMutation::HelloWrongRole
            | MessageMutation::HelloWrongIdentity
            | MessageMutation::HelloUpgradeNonce
            | MessageMutation::HelloTranscript
            | MessageMutation::HelloTrailingByte => {
                return HandshakeOutcome::PeerViolation("invalid-control");
            }
            MessageMutation::HelloWrongNetwork => {
                return HandshakeOutcome::NeutralClose("wrong-network");
            }
            MessageMutation::HelloUnsupportedControlVersion
            | MessageMutation::HelloWrongProtocolVersion
            | MessageMutation::HelloWrongChain
            | MessageMutation::HelloMissingCapability
            | MessageMutation::HelloUnsupportedChannel => {
                return HandshakeOutcome::NeutralClose("policy-mismatch");
            }
            MessageMutation::HelloZeroLimit => {
                return HandshakeOutcome::NeutralClose("resource-limit");
            }
            MessageMutation::None
            | MessageMutation::AckBadMagic
            | MessageMutation::AckZeroLength
            | MessageMutation::AckOversizeLength
            | MessageMutation::AckWrongProtocolVersion
            | MessageMutation::AckWrongRemoteNonce
            | MessageMutation::AckExtraCapability
            | MessageMutation::AckExtraChannel
            | MessageMutation::AckZeroLimit
            | MessageMutation::AckLimitAboveRequest
            | MessageMutation::AckTrailingByte => {}
        }

        if matches!(
            scenario.fault,
            FaultAction::StallBeforeAck
                | FaultAction::CloseBeforeAck
                | FaultAction::CancelBeforeAck
        ) {
            return HandshakeOutcome::LocalFault("transport");
        }

        match scenario.mutation {
            MessageMutation::None => HandshakeOutcome::Established,
            MessageMutation::AckLimitAboveRequest => {
                HandshakeOutcome::NeutralClose("resource-limit")
            }
            MessageMutation::AckWrongProtocolVersion
            | MessageMutation::AckExtraCapability
            | MessageMutation::AckExtraChannel => HandshakeOutcome::NeutralClose("policy-mismatch"),
            MessageMutation::AckZeroLimit => HandshakeOutcome::NeutralClose("resource-limit"),
            MessageMutation::AckBadMagic
            | MessageMutation::AckZeroLength
            | MessageMutation::AckOversizeLength
            | MessageMutation::AckWrongRemoteNonce
            | MessageMutation::AckTrailingByte => {
                HandshakeOutcome::PeerViolation("invalid-control")
            }
            MessageMutation::HelloZeroLength
            | MessageMutation::HelloOversizeLength
            | MessageMutation::HelloBadMagic
            | MessageMutation::HelloUnsupportedControlVersion
            | MessageMutation::HelloWrongProtocolVersion
            | MessageMutation::HelloWrongPath
            | MessageMutation::HelloWrongRole
            | MessageMutation::HelloWrongNetwork
            | MessageMutation::HelloWrongChain
            | MessageMutation::HelloWrongIdentity
            | MessageMutation::HelloUpgradeNonce
            | MessageMutation::HelloTranscript
            | MessageMutation::HelloMissingCapability
            | MessageMutation::HelloUnsupportedChannel
            | MessageMutation::HelloZeroLimit
            | MessageMutation::HelloTrailingByte => {
                unreachable!("hello mutations return before the ack phase")
            }
        }
    }

    pub fn negotiated(scenario: &HandshakeScenario) -> Option<NativeHandshakeNegotiated> {
        if Self::evaluate(scenario) != HandshakeOutcome::Established {
            return None;
        }
        let (initiator_config, initiator_limits, responder_config, responder_limits) =
            scenario.policies();
        let requested = initiator_limits.initial_limits();
        let responder = responder_limits.initial_limits();
        Some(NativeHandshakeNegotiated {
            limits: ZakuraAcceptedLimits {
                max_frame_bytes: requested.max_frame_bytes.min(responder.max_frame_bytes),
                max_message_bytes: requested.max_message_bytes.min(responder.max_message_bytes),
                max_open_streams: requested.max_open_streams.min(responder.max_open_streams),
                max_inbound_queue_depth: requested
                    .max_inbound_queue_depth
                    .min(responder.max_inbound_queue_depth),
                idle_timeout_millis: requested
                    .idle_timeout_millis
                    .min(responder.idle_timeout_millis),
            },
            accepted_capabilities: initiator_config.supported_capabilities
                & responder_config.supported_capabilities,
        })
    }
}
