//! Per-connection inputs for hostile control handshakes and stream preludes.

use std::{net::IpAddr, time::Duration};

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{
    StreamPrelude, ZakuraControlAck, ZakuraControlHello, ZakuraControlRole, ZakuraHandshakePath,
    ZakuraNetworkId,
};

/// One control field to corrupt. Each variant reaches a distinct production guard.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ControlFault {
    #[default]
    /// Keep the conformant field values.
    None,
    /// Change the first magic byte.
    Magic,
    /// Offer an unsupported version.
    Version,
    /// Disagree with the ALPN protocol.
    Protocol,
    /// Claim a legacy upgrade on a native connection.
    Path,
    /// Send the responder role in a hello.
    Role,
    /// Claim Mainnet while the fixture runs Regtest.
    Network,
    /// Change the genesis hash.
    Chain,
    /// Change the authenticated public key.
    Identity,
    /// Require unsupported channels.
    RequiredChannel,
    /// Offer a zero frame limit.
    FrameLimit,
    /// Offer a zero message limit.
    MessageLimit,
    /// Offer zero streams.
    StreamLimit,
    /// Offer zero queue entries.
    QueueLimit,
    /// Offer a zero idle timeout.
    IdleLimit,
    /// Set a native initiator upgrade nonce.
    InitiatorNonce,
    /// Set a native responder upgrade nonce.
    ResponderNonce,
    /// Set a native upgrade transcript.
    Transcript,
    /// Change the echoed nonce.
    NonceEcho,
    /// Accept a capability absent from the hello.
    UnofferedCapability,
    /// Accept more streams than the hello requested.
    LimitsAboveRequest,
    /// Declare a control payload above the hard cap.
    Oversize,
}

impl ControlFault {
    /// The label attached to the bytes the peer sends.
    pub fn label(self) -> &'static str {
        match self {
            Self::None => "conformant",
            Self::Magic => "bad-magic",
            Self::Version => "bad-version",
            Self::Protocol => "bad-protocol",
            Self::Path => "bad-path",
            Self::Role => "bad-role",
            Self::Network => "wrong-network",
            Self::Chain => "wrong-chain",
            Self::Identity => "identity-mismatch",
            Self::RequiredChannel => "missing-capability",
            Self::FrameLimit => "bad-frame-limit",
            Self::MessageLimit => "bad-message-limit",
            Self::StreamLimit => "bad-stream-limit",
            Self::QueueLimit => "bad-queue-limit",
            Self::IdleLimit => "bad-idle-limit",
            Self::InitiatorNonce => "nonzero-initiator-nonce",
            Self::ResponderNonce => "nonzero-responder-nonce",
            Self::Transcript => "nonzero-native-transcript",
            Self::NonceEcho => "bad-nonce-echo",
            Self::UnofferedCapability => "unoffered-capability",
            Self::LimitsAboveRequest => "limits-above-request",
            Self::Oversize => "oversize",
        }
    }

    pub(crate) fn hello(self, hello: &mut ZakuraControlHello) {
        match self {
            Self::None | Self::Oversize => {}
            Self::Magic => hello.magic[0] ^= 1,
            Self::Version => hello.control_version = 0,
            Self::Protocol => hello.selected_zakura_protocol = 0,
            Self::Path => hello.handshake_path = ZakuraHandshakePath::Upgraded,
            Self::Role => hello.role = ZakuraControlRole::Responder,
            Self::Network => hello.network_id = ZakuraNetworkId::Mainnet,
            Self::Chain => hello.chain_id[0] ^= 1,
            Self::Identity => hello.iroh_node_id[0] ^= 1,
            Self::RequiredChannel => hello.required_channels = u64::MAX,
            Self::FrameLimit => hello.initial_limits.max_frame_bytes = 0,
            Self::MessageLimit => hello.initial_limits.max_message_bytes = 0,
            Self::StreamLimit => hello.initial_limits.max_open_streams = 0,
            Self::QueueLimit => hello.initial_limits.max_inbound_queue_depth = 0,
            Self::IdleLimit => hello.initial_limits.idle_timeout_millis = 0,
            Self::InitiatorNonce => hello.initiator_upgrade_nonce[0] = 1,
            Self::ResponderNonce => hello.responder_upgrade_nonce[0] = 1,
            Self::Transcript => hello.legacy_upgrade_transcript[0] = 1,
            Self::NonceEcho | Self::UnofferedCapability | Self::LimitsAboveRequest => {
                panic!("an ack fault requires a listening peer")
            }
        }
    }

    pub(crate) fn ack(self, ack: &mut ZakuraControlAck, offered: u64) {
        match self {
            Self::None | Self::Oversize => {}
            Self::Magic => ack.magic[0] ^= 1,
            Self::Version => ack.control_version = 0,
            Self::Protocol => ack.selected_zakura_protocol = 0,
            Self::NonceEcho => ack.remote_peer_nonce[0] ^= 1,
            Self::UnofferedCapability => {
                ack.accepted_capabilities |= 1 << (!offered).trailing_zeros()
            }
            Self::LimitsAboveRequest => ack.accepted_limits.max_open_streams += 1,
            Self::FrameLimit => ack.accepted_limits.max_frame_bytes = 0,
            Self::MessageLimit => ack.accepted_limits.max_message_bytes = 0,
            Self::StreamLimit => ack.accepted_limits.max_open_streams = 0,
            Self::QueueLimit => ack.accepted_limits.max_inbound_queue_depth = 0,
            Self::IdleLimit => ack.accepted_limits.idle_timeout_millis = 0,
            _ => panic!("a hello fault requires a dialing peer"),
        }
    }
}

/// One field of a stream prelude to corrupt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PreludeFault {
    #[default]
    /// Keep the conformant field values.
    None,
    /// Change the first magic byte.
    Magic,
    /// Name an unknown stream kind.
    Kind,
    /// Open discovery without negotiating its capability.
    Capability,
    /// Offer an unsupported version.
    Version,
    /// Offer a frame cap smaller than the stream requires.
    FrameCap,
    /// Attach an ID to a persistent stream.
    UnexpectedRequestId,
    /// Omit the ID on a request stream.
    MissingRequestId,
    /// Open an ordered stream from the ineligible side.
    IneligibleOpener,
}

impl PreludeFault {
    /// The label attached to the peer's prelude.
    pub fn label(self) -> &'static str {
        match self {
            Self::None => "conformant",
            Self::Magic => "bad-magic",
            Self::Kind => "unknown-kind",
            Self::Capability => "unnegotiated-capability",
            Self::Version => "unselected-version",
            Self::FrameCap => "undersized-cap",
            Self::UnexpectedRequestId => "unexpected-request-id",
            Self::MissingRequestId => "request-without-id",
            Self::IneligibleOpener => "ineligible-opener",
        }
    }

    pub(crate) fn apply(self, prelude: &mut StreamPrelude) {
        use zakura_network::zakura::{ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_DISCOVERY};
        match self {
            Self::None | Self::IneligibleOpener => {}
            Self::Magic => prelude.magic[0] ^= 1,
            Self::Kind => prelude.stream_kind = u16::MAX,
            Self::Capability => prelude.stream_kind = ZAKURA_STREAM_DISCOVERY,
            Self::Version => prelude.stream_version = 0,
            Self::FrameCap => prelude.max_frame_bytes = 1,
            Self::UnexpectedRequestId => prelude.request_id = Some(1),
            Self::MissingRequestId => {
                prelude.stream_kind = ZAKURA_STREAM_BLOCK_SYNC;
                prelude.request_id = None;
            }
        }
    }
}

/// Scripted transport timing. Milliseconds survive scenario serialization exactly.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HandshakeSpec {
    /// Field corruption on the hello or ack, according to the peer's role.
    pub control: ControlFault,
    /// Field corruption on each prelude the peer opens.
    pub prelude: PreludeFault,
    /// Delay before the peer opens its control stream.
    pub open_delay_ms: u64,
    /// Delay before the four-byte control length.
    pub length_delay_ms: u64,
    /// Delay between the length and payload.
    pub payload_delay_ms: u64,
    /// Delay before each control byte; zero retains the original bulk write.
    pub byte_delay_ms: u64,
    /// Delay before each prelude byte.
    pub prelude_byte_delay_ms: u64,
    /// Abandon the write after this many length and payload bytes.
    pub abandon_after: Option<usize>,
    /// Address shared by several peer endpoints; each endpoint retains its own port.
    pub bind_ip: Option<IpAddr>,
    /// Peer index whose identity this connection uses, for simultaneous duplicates.
    pub identity: Option<usize>,
    /// Attempts after the first failed handshake.
    pub retries: u8,
    /// Delay between failed attempts.
    pub retry_delay_ms: u64,
}

pub(crate) async fn delay(ms: u64) {
    if ms != 0 {
        zakura_rt::time::sleep(Duration::from_millis(ms)).await;
    }
}
