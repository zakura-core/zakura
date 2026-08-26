//! Native control-handshake logic above the transport boundary.

use std::{future::Future, time::Duration};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use iroh::endpoint::{RecvStream, SendStream};

use super::{
    control_payload_length_is_admissible, ZakuraAcceptedLimits, ZakuraControlAck,
    ZakuraControlHello, ZakuraControlRole, ZakuraControlValidation, ZakuraHandshakeConfig,
    ZakuraHandshakePath, ZakuraInitialLimits, ZakuraLimits, ZakuraLocalLimits, ZakuraPeerId,
    CONTROL_ACK_MAGIC, CONTROL_HELLO_MAGIC, CONTROL_VERSION, ZAKURA_PROTOCOL_VERSION_1,
};
use crate::zakura::ZakuraHandlerError;

const CONTROL_LENGTH_BYTES: usize = 4;

/// The role that produced a canonical handshake event.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum ControlHandshakeRole {
    Initiator,
    Responder,
}

/// A backend-independent event from the production handshake core.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum ControlHandshakeEvent {
    Started(ControlHandshakeRole),
    LengthRead(ControlHandshakeRole, u32),
    PayloadRead(ControlHandshakeRole, usize),
    MessageValidated(ControlHandshakeRole),
    MessageWritten(ControlHandshakeRole, usize),
    Established(ControlHandshakeRole),
}

/// Receives canonical events without coupling the handshake to one tracer.
pub(crate) trait ControlHandshakeObserver: Send + Sync {
    fn record(&self, event: ControlHandshakeEvent);
}

/// Discards canonical events in production paths that use the existing trace.
pub(crate) struct NoopControlHandshakeObserver;

impl ControlHandshakeObserver for NoopControlHandshakeObserver {
    fn record(&self, _event: ControlHandshakeEvent) {}
}

/// Supplies deterministic or wall-clock sleeps to handshake deadlines.
pub(crate) trait ControlHandshakeClock: Send + Sync {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;
}

/// Uses Tokio time for the production Iroh adapter.
pub(crate) struct TokioControlHandshakeClock;

impl ControlHandshakeClock for TokioControlHandshakeClock {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Reads exact byte counts from one ordered control stream.
pub(crate) trait ControlRead: Send {
    fn read_exact<'a>(
        &'a mut self,
        bytes: &'a mut [u8],
    ) -> impl Future<Output = Result<(), ZakuraHandlerError>> + Send;
}

/// Writes complete buffers to one ordered control stream.
pub(crate) trait ControlWrite: Send {
    fn write_all<'a>(
        &'a mut self,
        bytes: &'a [u8],
    ) -> impl Future<Output = Result<(), ZakuraHandlerError>> + Send;

    fn finish(&mut self) -> Result<(), ZakuraHandlerError>;
}

impl ControlRead for RecvStream {
    async fn read_exact(&mut self, bytes: &mut [u8]) -> Result<(), ZakuraHandlerError> {
        RecvStream::read_exact(self, bytes).await?;
        Ok(())
    }
}

impl ControlWrite for SendStream {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), ZakuraHandlerError> {
        SendStream::write_all(self, bytes).await?;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), ZakuraHandlerError> {
        // Preserve the existing Iroh adapter behavior. The peer can close its
        // receive half after it reads the complete control message.
        let _ = SendStream::finish(self);
        Ok(())
    }
}

/// The values that both roles expose after a native handshake.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeHandshakeNegotiated {
    pub(crate) limits: ZakuraAcceptedLimits,
    pub(crate) accepted_capabilities: u64,
}

async fn with_deadline<T>(
    clock: &impl ControlHandshakeClock,
    duration: Duration,
    phase: &'static str,
    future: impl Future<Output = Result<T, ZakuraHandlerError>> + Send,
) -> Result<T, ZakuraHandlerError> {
    tokio::select! {
        result = future => result,
        () = clock.sleep(duration) => Err(ZakuraHandlerError::Timeout(phase)),
    }
}

/// Reads one bounded length-prefixed control payload.
pub(crate) async fn read_control_payload<R: ControlRead>(
    recv: &mut R,
    max_bytes: u32,
    read_timeout: Duration,
    role: ControlHandshakeRole,
    clock: &impl ControlHandshakeClock,
    observer: &impl ControlHandshakeObserver,
) -> Result<Vec<u8>, ZakuraHandlerError> {
    let mut len_bytes = [0; CONTROL_LENGTH_BYTES];
    with_deadline(
        clock,
        read_timeout,
        "control length",
        recv.read_exact(&mut len_bytes),
    )
    .await?;
    let len = (&len_bytes[..]).read_u32::<LittleEndian>()?;
    observer.record(ControlHandshakeEvent::LengthRead(role, len));
    if !control_payload_length_is_admissible(len, max_bytes) {
        return Err(ZakuraHandlerError::Oversize);
    }

    let len = usize::try_from(len).expect("u32 lengths fit usize on supported targets");
    let mut bytes = vec![0; len];
    with_deadline(
        clock,
        read_timeout,
        "control payload",
        recv.read_exact(&mut bytes),
    )
    .await?;
    observer.record(ControlHandshakeEvent::PayloadRead(role, bytes.len()));
    Ok(bytes)
}

/// Writes one length-prefixed control payload and closes the send half.
pub(crate) async fn write_control_payload<W: ControlWrite>(
    send: &mut W,
    bytes: &[u8],
    write_timeout: Duration,
    role: ControlHandshakeRole,
    clock: &impl ControlHandshakeClock,
    observer: &impl ControlHandshakeObserver,
) -> Result<(), ZakuraHandlerError> {
    let len_value = u32::try_from(bytes.len()).map_err(|_| ZakuraHandlerError::Oversize)?;
    let mut len = Vec::with_capacity(CONTROL_LENGTH_BYTES);
    len.write_u32::<LittleEndian>(len_value)?;
    with_deadline(
        clock,
        write_timeout,
        "control length write",
        send.write_all(&len),
    )
    .await?;
    with_deadline(
        clock,
        write_timeout,
        "control payload write",
        send.write_all(bytes),
    )
    .await?;
    send.finish()?;
    observer.record(ControlHandshakeEvent::MessageWritten(role, bytes.len()));
    Ok(())
}

/// Constructs the native hello that the initiator must send.
pub(crate) fn native_control_hello(
    handshake_config: &ZakuraHandshakeConfig,
    limits: &ZakuraLocalLimits,
    local_peer_id: &ZakuraPeerId,
    local_nonce: [u8; 32],
) -> ZakuraControlHello {
    ZakuraControlHello {
        magic: CONTROL_HELLO_MAGIC,
        control_version: CONTROL_VERSION,
        selected_zakura_protocol: ZAKURA_PROTOCOL_VERSION_1,
        handshake_path: ZakuraHandshakePath::Native,
        role: ZakuraControlRole::Initiator,
        network_id: handshake_config.network_id,
        chain_id: handshake_config.chain_id,
        iroh_node_id: local_peer_id.as_bytes().to_vec(),
        peer_nonce: local_nonce,
        initiator_upgrade_nonce: [0; 32],
        responder_upgrade_nonce: [0; 32],
        legacy_upgrade_transcript: [0; 32],
        capabilities: handshake_config.supported_capabilities,
        required_channels: 0,
        initial_limits: limits.initial_limits(),
    }
}

/// Applies the responder ceilings to the initiator's requested limits.
pub(crate) fn accepted_native_limits(
    local: &ZakuraLocalLimits,
    remote: &ZakuraInitialLimits,
) -> ZakuraAcceptedLimits {
    ZakuraLimits {
        max_frame_bytes: remote.max_frame_bytes.min(local.max_frame_bytes),
        max_message_bytes: remote.max_message_bytes.min(local.max_message_bytes),
        max_open_streams: remote.max_open_streams.min(local.max_open_streams),
        max_inbound_queue_depth: remote
            .max_inbound_queue_depth
            .min(local.max_inbound_queue_depth),
        idle_timeout_millis: remote
            .idle_timeout_millis
            .min(local.initial_limits().idle_timeout_millis),
    }
}

/// Runs the initiator role after its transport opens a bidirectional stream.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_native_initiator_control<S: ControlWrite, R: ControlRead>(
    send: &mut S,
    recv: &mut R,
    limits: &ZakuraLocalLimits,
    handshake_config: &ZakuraHandshakeConfig,
    local_peer_id: &ZakuraPeerId,
    local_nonce: [u8; 32],
    clock: &impl ControlHandshakeClock,
    observer: &impl ControlHandshakeObserver,
) -> Result<NativeHandshakeNegotiated, ZakuraHandlerError> {
    let role = ControlHandshakeRole::Initiator;
    observer.record(ControlHandshakeEvent::Started(role));
    let hello = native_control_hello(handshake_config, limits, local_peer_id, local_nonce);
    write_control_payload(
        send,
        &hello.encode()?,
        limits.control_timeout,
        role,
        clock,
        observer,
    )
    .await?;
    let ack_bytes = read_control_payload(
        recv,
        handshake_config.max_control_frame_bytes,
        limits.control_timeout,
        role,
        clock,
        observer,
    )
    .await?;
    let ack = ZakuraControlAck::decode(&ack_bytes)?;
    ack.validate(
        ZAKURA_PROTOCOL_VERSION_1,
        local_nonce,
        ack.peer_nonce,
        &limits.initial_limits(),
        handshake_config,
    )?;
    observer.record(ControlHandshakeEvent::MessageValidated(role));
    observer.record(ControlHandshakeEvent::Established(role));
    Ok(NativeHandshakeNegotiated {
        limits: ack.accepted_limits,
        accepted_capabilities: ack.accepted_capabilities,
    })
}

/// Runs the responder role after its transport accepts a bidirectional stream.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_native_responder_control<S: ControlWrite, R: ControlRead>(
    send: &mut S,
    recv: &mut R,
    limits: &ZakuraLocalLimits,
    handshake_config: &ZakuraHandshakeConfig,
    remote_peer_id: &ZakuraPeerId,
    local_nonce: [u8; 32],
    clock: &impl ControlHandshakeClock,
    observer: &impl ControlHandshakeObserver,
) -> Result<NativeHandshakeNegotiated, ZakuraHandlerError> {
    let role = ControlHandshakeRole::Responder;
    observer.record(ControlHandshakeEvent::Started(role));
    let hello_bytes = read_control_payload(
        recv,
        handshake_config.max_control_frame_bytes,
        limits.control_timeout,
        role,
        clock,
        observer,
    )
    .await?;
    let hello = ZakuraControlHello::decode(&hello_bytes)?;
    hello.validate(&ZakuraControlValidation {
        local: handshake_config,
        authenticated_remote_id: remote_peer_id.as_bytes(),
        selected_zakura_protocol: ZAKURA_PROTOCOL_VERSION_1,
        handshake_path: ZakuraHandshakePath::Native,
        remote_role: ZakuraControlRole::Initiator,
        initiator_upgrade_nonce: [0; 32],
        responder_upgrade_nonce: [0; 32],
        legacy_upgrade_transcript: [0; 32],
    })?;
    observer.record(ControlHandshakeEvent::MessageValidated(role));

    let accepted_limits = accepted_native_limits(limits, &hello.initial_limits);
    let ack = ZakuraControlAck {
        magic: CONTROL_ACK_MAGIC,
        control_version: CONTROL_VERSION,
        selected_zakura_protocol: hello.selected_zakura_protocol,
        peer_nonce: local_nonce,
        remote_peer_nonce: hello.peer_nonce,
        accepted_capabilities: hello.capabilities & handshake_config.supported_capabilities,
        accepted_channels: hello.required_channels & handshake_config.supported_channels,
        accepted_limits,
    };
    write_control_payload(
        send,
        &ack.encode()?,
        limits.control_timeout,
        role,
        clock,
        observer,
    )
    .await?;
    observer.record(ControlHandshakeEvent::Established(role));
    Ok(NativeHandshakeNegotiated {
        limits: accepted_limits,
        accepted_capabilities: ack.accepted_capabilities,
    })
}
