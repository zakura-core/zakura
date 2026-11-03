//! The Zakura control handshake and stream prelude, scripted from the
//! public wire types, in both roles.
//!
//! A scripted peer offers or accepts whatever capability mask its scenario
//! gives it, so a test can show what the node negotiates from it. Nonces
//! come from `zakura_rt::rand`, so the `Plan` draws them in the peer's task.

use rand_10::Rng;
use zakura_network::{
    zakura::{
        v9::RegtestMessage, zakura_protocol_for_alpn, StreamPrelude, ZakuraControlAck,
        ZakuraControlHello, ZakuraControlRole, ZakuraControlValidation, ZakuraHandshakeConfig,
        ZakuraHandshakePath, ZakuraLocalLimits, CONTROL_ACK_MAGIC, CONTROL_HELLO_MAGIC,
        CONTROL_VERSION, MAX_CONTROL_PAYLOAD_BYTES, STREAM_PRELUDE_MAGIC,
    },
    Config,
};
use zakura_quic::{Conn, NodeId, RecvStream, SendStream};

/// The peer's side of a handshake: the network config it validates against
/// and the limits it offers, both production's on Regtest.
pub(crate) struct Local {
    config: ZakuraHandshakeConfig,
    limits: ZakuraLocalLimits,
}

impl Local {
    /// Production's handshake config and limits on Regtest.
    pub(crate) fn regtest() -> Self {
        let config = Config {
            network: zakura_chain::parameters::Network::new_regtest(Default::default()),
            ..Config::default()
        };
        Local {
            config: ZakuraHandshakeConfig::for_network(&config.network),
            limits: ZakuraLocalLimits::from_config(&config),
        }
    }
}

/// A fresh nonce, drawn from the `Plan` in the calling task.
fn nonce() -> [u8; 32] {
    let mut nonce = [0; 32];
    zakura_rt::rand::rng().fill_bytes(&mut nonce);
    nonce
}

/// The Zakura protocol `conn`'s ALPN selects.
fn protocol(conn: &Conn) -> Result<u16, String> {
    zakura_protocol_for_alpn(conn.alpn()).ok_or_else(|| "the ALPN isn't a Zakura protocol".into())
}

/// Writes one control payload with its 4-byte length, then finishes the
/// stream, as the node's control writer does.
async fn write_control(send: &mut SendStream, payload: &[u8]) -> Result<(), String> {
    let len = u32::try_from(payload.len()).map_err(|_| "a control payload fits u32")?;
    send.write_all(&len.to_le_bytes())
        .await
        .map_err(|error| format!("control length: {error}"))?;
    send.write_all(payload)
        .await
        .map_err(|error| format!("control payload: {error}"))?;
    let _ = send.finish();
    Ok(())
}

/// Reads one control payload, refusing a length past the protocol's hard
/// cap before it allocates.
async fn read_control(recv: &mut RecvStream) -> Result<Vec<u8>, String> {
    let mut len = [0; 4];
    recv.read_exact(&mut len)
        .await
        .map_err(|error| format!("control length: {error}"))?;
    let len = usize::try_from(u32::from_le_bytes(len)).map_err(|_| "u32 fits usize")?;
    if len == 0 || len > MAX_CONTROL_PAYLOAD_BYTES {
        return Err(format!("a control payload of {len} bytes"));
    }
    let mut payload = vec![0; len];
    recv.read_exact(&mut payload)
        .await
        .map_err(|error| format!("control payload: {error}"))?;
    Ok(payload)
}

/// Runs the initiator's side on `conn`: sends a hello offering
/// `capabilities` from `local_id`, then reads and validates the ack, nonce
/// echo included. Returns the mask the ack accepted.
pub(crate) async fn initiate(
    conn: &Conn,
    local_id: NodeId,
    capabilities: u64,
    local: &Local,
) -> Result<u64, String> {
    let protocol = protocol(conn)?;
    let peer_nonce = nonce();
    let hello = ZakuraControlHello {
        magic: CONTROL_HELLO_MAGIC,
        control_version: CONTROL_VERSION,
        selected_zakura_protocol: protocol,
        handshake_path: ZakuraHandshakePath::Native,
        role: ZakuraControlRole::Initiator,
        network_id: local.config.network_id,
        chain_id: local.config.chain_id,
        iroh_node_id: local_id.as_bytes().to_vec(),
        peer_nonce,
        initiator_upgrade_nonce: [0; 32],
        responder_upgrade_nonce: [0; 32],
        legacy_upgrade_transcript: [0; 32],
        capabilities,
        required_channels: 0,
        initial_limits: local.limits.initial_limits(),
    };
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|error| format!("control stream: {error}"))?;
    let hello = hello.encode().map_err(|error| error.to_string())?;
    write_control(&mut send, &hello).await?;
    let ack = ZakuraControlAck::decode(&read_control(&mut recv).await?)
        .map_err(|error| format!("ack: {error}"))?;
    ack.validate(
        protocol,
        peer_nonce,
        ack.peer_nonce,
        capabilities,
        &local.limits.initial_limits(),
        &local.config,
    )
    .map_err(|error| format!("ack: {error}"))?;
    Ok(ack.accepted_capabilities)
}

/// How a responder echoes the initiator's nonce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Echo {
    /// As sent, as the protocol requires.
    Faithful,
    /// With every bit flipped, so it never matches.
    Wrong,
}

/// Runs the responder's side on `conn`, whose initiator authenticated as
/// `remote_id`: reads and validates the hello, then acks it with the nonce
/// echoed as `echo` says and the hello's mask intersected with
/// `capabilities`. Returns that intersection.
pub(crate) async fn respond(
    conn: &Conn,
    remote_id: NodeId,
    capabilities: u64,
    echo: Echo,
    local: &Local,
) -> Result<u64, String> {
    let protocol = protocol(conn)?;
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|error| format!("control stream: {error}"))?;
    let hello = ZakuraControlHello::decode(&read_control(&mut recv).await?)
        .map_err(|error| format!("hello: {error}"))?;
    hello
        .validate(&ZakuraControlValidation {
            local: &local.config,
            authenticated_remote_id: remote_id.as_bytes(),
            selected_zakura_protocol: protocol,
            handshake_path: ZakuraHandshakePath::Native,
            remote_role: ZakuraControlRole::Initiator,
            initiator_upgrade_nonce: [0; 32],
            responder_upgrade_nonce: [0; 32],
            legacy_upgrade_transcript: [0; 32],
        })
        .map_err(|error| format!("hello: {error}"))?;
    let accepted = hello.capabilities & capabilities;
    let echoed = match echo {
        Echo::Faithful => hello.peer_nonce,
        Echo::Wrong => hello.peer_nonce.map(|byte| !byte),
    };
    let ack = ZakuraControlAck {
        magic: CONTROL_ACK_MAGIC,
        control_version: CONTROL_VERSION,
        selected_zakura_protocol: protocol,
        peer_nonce: nonce(),
        remote_peer_nonce: echoed,
        accepted_capabilities: accepted,
        accepted_channels: 0,
        accepted_limits: hello.initial_limits,
    }
    .encode()
    .map_err(|error| error.to_string())?;
    write_control(&mut send, &ack).await?;
    Ok(accepted)
}

/// Writes the prelude of a persistent `kind` stream at `version`, with
/// Regtest header sync's frame cap.
pub(crate) async fn write_prelude(
    send: &mut SendStream,
    kind: u16,
    version: u16,
) -> Result<(), String> {
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: kind,
        stream_version: version,
        request_id: None,
        max_frame_bytes: RegtestMessage::LAYOUT[0].frame_cap,
    }
    .encode()
    .map_err(|error| error.to_string())?;
    send.write_all(&prelude)
        .await
        .map_err(|error| format!("prelude: {error}"))
}

/// Reads a stream prelude: the fixed fields, then the request ID if its
/// flag says one follows, then the frame cap.
pub(crate) async fn read_prelude(recv: &mut RecvStream) -> Result<StreamPrelude, String> {
    // Magic, kind, version and the request ID's flag.
    let mut bytes = vec![0; 9];
    recv.read_exact(&mut bytes)
        .await
        .map_err(|error| format!("prelude: {error}"))?;
    let rest = if bytes[8] == 1 { 12 } else { 4 };
    let mut tail = vec![0; rest];
    recv.read_exact(&mut tail)
        .await
        .map_err(|error| format!("prelude: {error}"))?;
    bytes.extend(tail);
    StreamPrelude::decode(&bytes).map_err(|error| format!("prelude: {error}"))
}
