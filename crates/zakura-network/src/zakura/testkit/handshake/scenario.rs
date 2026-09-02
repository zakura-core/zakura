use std::time::Duration;

use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use zakura_chain::parameters::Network;

use crate::{
    zakura::{
        ZakuraHandshakeConfig, ZakuraLocalLimits, ZakuraPeerId, DEFAULT_ZAKURA_KEEP_ALIVE_INTERVAL,
        DEFAULT_ZAKURA_MAX_CONNECTIONS, DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
        DEFAULT_ZAKURA_MESSAGE_RATE_PER_SECOND, DEFAULT_ZAKURA_PRELUDE_TIMEOUT,
        DEFAULT_ZAKURA_QUIC_IDLE_TIMEOUT, DEFAULT_ZAKURA_STREAM_OPEN_RATE_PER_SECOND,
    },
    Config,
};

/// The fixed two-peer topology for the handshake MVP.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct HandshakeTopology {
    pub initiator: u8,
    pub responder: u8,
}

/// One named mutation of a conformant outbound control message.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum MessageMutation {
    None,
    HelloZeroLength,
    HelloOversizeLength,
    HelloBadMagic,
    HelloUnsupportedControlVersion,
    HelloWrongProtocolVersion,
    HelloWrongPath,
    HelloWrongRole,
    HelloWrongNetwork,
    HelloWrongChain,
    HelloWrongIdentity,
    HelloUpgradeNonce,
    HelloTranscript,
    HelloMissingCapability,
    HelloUnsupportedChannel,
    HelloZeroLimit,
    HelloTrailingByte,
    AckBadMagic,
    AckZeroLength,
    AckOversizeLength,
    AckWrongProtocolVersion,
    AckWrongRemoteNonce,
    AckExtraCapability,
    AckExtraChannel,
    AckZeroLimit,
    AckLimitAboveRequest,
    AckTrailingByte,
}

/// One transport or scheduling fault in the MVP overlap vocabulary.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum FaultAction {
    None,
    DelayMillis(u16),
    ShortWrites(u8),
    StallBeforeHello,
    StallBeforeAck,
    CloseBeforeHello,
    CloseBeforeAck,
    CancelBeforeHello,
    CancelBeforeAck,
}

/// A replayable input to every handshake backend.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct HandshakeScenario {
    pub seed: u64,
    pub topology: HandshakeTopology,
    pub mutation: MessageMutation,
    pub fault: FaultAction,
    pub initiator_frame_bytes: u32,
    pub responder_frame_bytes: u32,
    pub timeout_millis: u16,
}

impl HandshakeScenario {
    pub fn conformant(seed: u64) -> Self {
        Self {
            seed,
            topology: HandshakeTopology {
                initiator: 1,
                responder: 2,
            },
            mutation: MessageMutation::None,
            fault: FaultAction::None,
            initiator_frame_bytes: 64 * 1024,
            responder_frame_bytes: 32 * 1024,
            timeout_millis: 200,
        }
    }

    pub fn with_mutation(mut self, mutation: MessageMutation) -> Self {
        self.mutation = mutation;
        self
    }

    pub fn with_fault(mut self, fault: FaultAction) -> Self {
        self.fault = fault;
        self
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.timeout_millis))
    }

    pub fn initiator_id(&self) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![self.topology.initiator; 32])
            .expect("the fixed 32-byte initiator identity is valid")
    }

    pub fn policies(
        &self,
    ) -> (
        ZakuraHandshakeConfig,
        ZakuraLocalLimits,
        ZakuraHandshakeConfig,
        ZakuraLocalLimits,
    ) {
        let mut initiator_config = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let mut responder_config = initiator_config;
        initiator_config.supported_capabilities = 0b111;
        responder_config.supported_capabilities = 0b101;
        responder_config.required_capabilities = 0b001;
        let initiator_limits = local_limits(self.initiator_frame_bytes, self.timeout());
        let responder_limits = local_limits(self.responder_frame_bytes, self.timeout());
        (
            initiator_config,
            initiator_limits,
            responder_config,
            responder_limits,
        )
    }
}

fn local_limits(max_frame_bytes: u32, control_timeout: Duration) -> ZakuraLocalLimits {
    let defaults = Config::default();
    let handshake = ZakuraHandshakeConfig::for_network(&defaults.network);
    ZakuraLocalLimits {
        max_connections: DEFAULT_ZAKURA_MAX_CONNECTIONS,
        max_pending_handshakes: DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
        quic_idle_timeout: DEFAULT_ZAKURA_QUIC_IDLE_TIMEOUT,
        keep_alive_interval: DEFAULT_ZAKURA_KEEP_ALIVE_INTERVAL,
        prelude_timeout: DEFAULT_ZAKURA_PRELUDE_TIMEOUT,
        control_timeout,
        stream_open_rate_per_second: DEFAULT_ZAKURA_STREAM_OPEN_RATE_PER_SECOND,
        message_rate_per_second: DEFAULT_ZAKURA_MESSAGE_RATE_PER_SECOND,
        max_frame_bytes,
        max_message_bytes: handshake.max_message_bytes.min(max_frame_bytes),
        max_open_streams: handshake.max_open_streams,
        max_inbound_queue_depth: handshake.max_inbound_queue_depth,
    }
}

pub(super) fn handshake_scenarios() -> impl Strategy<Value = HandshakeScenario> {
    let mutation = prop_oneof![
        8 => Just(MessageMutation::None),
        1 => Just(MessageMutation::HelloZeroLength),
        1 => Just(MessageMutation::HelloOversizeLength),
        1 => Just(MessageMutation::HelloBadMagic),
        1 => Just(MessageMutation::HelloUnsupportedControlVersion),
        1 => Just(MessageMutation::HelloWrongProtocolVersion),
        1 => Just(MessageMutation::HelloWrongPath),
        1 => Just(MessageMutation::HelloWrongRole),
        1 => Just(MessageMutation::HelloWrongNetwork),
        1 => Just(MessageMutation::HelloWrongChain),
        1 => Just(MessageMutation::HelloWrongIdentity),
        1 => Just(MessageMutation::HelloUpgradeNonce),
        1 => Just(MessageMutation::HelloTranscript),
        1 => Just(MessageMutation::HelloMissingCapability),
        1 => Just(MessageMutation::HelloUnsupportedChannel),
        1 => Just(MessageMutation::HelloZeroLimit),
        1 => Just(MessageMutation::HelloTrailingByte),
        1 => Just(MessageMutation::AckBadMagic),
        1 => Just(MessageMutation::AckZeroLength),
        1 => Just(MessageMutation::AckOversizeLength),
        1 => Just(MessageMutation::AckWrongProtocolVersion),
        1 => Just(MessageMutation::AckWrongRemoteNonce),
        1 => Just(MessageMutation::AckExtraCapability),
        1 => Just(MessageMutation::AckExtraChannel),
        1 => Just(MessageMutation::AckZeroLimit),
        1 => Just(MessageMutation::AckLimitAboveRequest),
        1 => Just(MessageMutation::AckTrailingByte),
    ];
    let fault = prop_oneof![
        12 => Just(FaultAction::None),
        2 => (0u16..40).prop_map(FaultAction::DelayMillis),
        2 => (1u8..=8).prop_map(FaultAction::ShortWrites),
        1 => Just(FaultAction::StallBeforeHello),
        1 => Just(FaultAction::StallBeforeAck),
        1 => Just(FaultAction::CloseBeforeHello),
        1 => Just(FaultAction::CloseBeforeAck),
        1 => Just(FaultAction::CancelBeforeHello),
        1 => Just(FaultAction::CancelBeforeAck),
    ];

    (
        any::<u64>(),
        mutation,
        fault,
        16_384u32..=131_072,
        16_384u32..=131_072,
        80u16..=300,
    )
        .prop_map(
            |(
                seed,
                mutation,
                fault,
                initiator_frame_bytes,
                responder_frame_bytes,
                timeout_millis,
            )| {
                HandshakeScenario {
                    seed,
                    topology: HandshakeTopology {
                        initiator: 1,
                        responder: 2,
                    },
                    mutation,
                    fault,
                    initiator_frame_bytes,
                    responder_frame_bytes,
                    timeout_millis,
                }
            },
        )
}
