use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::zakura::{
    ControlRead, ControlWrite, NativeHandshakeNegotiated, ZakuraControlAck, ZakuraControlHello,
    ZakuraControlRole, ZakuraFailureClass, ZakuraHandlerError, ZakuraHandshakePath,
    ZakuraNetworkId, MAX_CONTROL_PAYLOAD_BYTES,
};

use super::{
    model::HandshakeOutcome,
    scenario::{FaultAction, HandshakeScenario, MessageMutation},
    trace::CanonicalTrace,
};

/// The backend-independent report for one completed scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RunReport {
    pub outcome: HandshakeOutcome,
    pub negotiated: Option<NativeHandshakeNegotiated>,
    pub trace: CanonicalTrace,
    pub trace_hash: String,
    pub runtime_audit: Option<String>,
    pub pending_tasks: usize,
    pub open_streams: usize,
}

#[derive(Serialize)]
struct ReplayArtifact<'a> {
    backend: &'a str,
    scenario: &'a HandshakeScenario,
    outcome: &'a HandshakeOutcome,
    trace: &'a CanonicalTrace,
    trace_hash: &'a str,
    runtime_audit: &'a Option<String>,
}

pub(super) fn persist_failure(
    backend: &str,
    scenario: &HandshakeScenario,
    result: &RunReport,
) -> PathBuf {
    let directory =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/handshake-sim/failures");
    fs::create_dir_all(&directory).expect("the handshake replay directory can be created");
    let path = directory.join(format!(
        "{backend}-{}-{}.json",
        scenario.seed,
        &result.trace_hash[..16]
    ));
    let artifact = ReplayArtifact {
        backend,
        scenario,
        outcome: &result.outcome,
        trace: &result.trace,
        trace_hash: &result.trace_hash,
        runtime_audit: &result.runtime_audit,
    };
    let bytes =
        serde_json::to_vec_pretty(&artifact).expect("a handshake replay artifact serializes");
    fs::write(&path, bytes).expect("a handshake replay artifact can be written");
    path
}

pub(super) fn load_scenario(path: &Path) -> (String, HandshakeScenario) {
    let bytes = fs::read(path).expect("the handshake replay artifact can be read");
    let mut artifact: serde_json::Value =
        serde_json::from_slice(&bytes).expect("the handshake replay artifact is valid JSON");
    let backend = artifact
        .get("backend")
        .and_then(serde_json::Value::as_str)
        .expect("the handshake replay artifact names its backend")
        .to_owned();
    let scenario = serde_json::from_value(
        artifact
            .get_mut("scenario")
            .expect("the handshake replay artifact contains a scenario")
            .take(),
    )
    .expect("the handshake replay scenario is valid");
    (backend, scenario)
}

pub(super) fn report(
    initiator: Result<NativeHandshakeNegotiated, ZakuraHandlerError>,
    responder: Result<NativeHandshakeNegotiated, ZakuraHandlerError>,
    mut trace: CanonicalTrace,
    runtime_audit: Option<String>,
) -> RunReport {
    let outcome = match (&initiator, &responder) {
        (Ok(left), Ok(right)) if left == right => HandshakeOutcome::Established,
        (Err(left), Err(right)) => prefer_causal_outcome(classify(left), classify(right)),
        (Err(error), _) | (_, Err(error)) => classify(error),
        (Ok(_), Ok(_)) => HandshakeOutcome::LocalFault("negotiation-disagreement"),
    };
    let negotiated = match (initiator, responder) {
        (Ok(left), Ok(right)) if left == right => Some(left),
        _ => None,
    };
    trace.record_terminal(&outcome);
    let trace_hash = trace.hash();
    RunReport {
        outcome,
        negotiated,
        trace,
        trace_hash,
        runtime_audit,
        pending_tasks: 0,
        open_streams: 0,
    }
}

fn prefer_causal_outcome(left: HandshakeOutcome, right: HandshakeOutcome) -> HandshakeOutcome {
    match (&left, &right) {
        (HandshakeOutcome::LocalFault(_), _) => right,
        (_, HandshakeOutcome::LocalFault(_)) => left,
        _ => left,
    }
}

fn classify(error: &ZakuraHandlerError) -> HandshakeOutcome {
    match error {
        ZakuraHandlerError::Timeout(_) => HandshakeOutcome::LocalFault("transport"),
        ZakuraHandlerError::Closed
        | ZakuraHandlerError::Io(_)
        | ZakuraHandlerError::IrohConnection(_)
        | ZakuraHandlerError::IrohConnect(_)
        | ZakuraHandlerError::IrohRemoteId(_)
        | ZakuraHandlerError::IrohWrite(_)
        | ZakuraHandlerError::IrohRead(_)
        | ZakuraHandlerError::IrohClosedStream(_) => HandshakeOutcome::LocalFault("transport"),
        ZakuraHandlerError::ResourceLimit(_) => {
            HandshakeOutcome::ResourceRejected("local-resource")
        }
        ZakuraHandlerError::Validation(error) => match error.failure_class() {
            ZakuraFailureClass::Neutral => HandshakeOutcome::NeutralClose(match error {
                crate::zakura::ZakuraValidationError::WrongNetwork => "wrong-network",
                crate::zakura::ZakuraValidationError::ResourceLimit => "resource-limit",
                _ => "policy-mismatch",
            }),
            ZakuraFailureClass::PotentiallyPunitive => {
                HandshakeOutcome::PeerViolation("invalid-control")
            }
        },
        ZakuraHandlerError::Oversize
        | ZakuraHandlerError::OversizeFrame { .. }
        | ZakuraHandlerError::Protocol(_) => HandshakeOutcome::PeerViolation("invalid-control"),
        ZakuraHandlerError::InvalidBootstrapPeer
        | ZakuraHandlerError::InvalidSecretKey
        | ZakuraHandlerError::InvalidLocalLimits
        | ZakuraHandlerError::RateLimited => HandshakeOutcome::LocalFault("local-error"),
    }
}

pub(super) fn mutate_payload(
    scenario: &HandshakeScenario,
    from_initiator: bool,
    payload: &[u8],
) -> Vec<u8> {
    let mutation = scenario.mutation;
    if from_initiator {
        let mut hello = match ZakuraControlHello::decode(payload) {
            Ok(hello) => hello,
            Err(_) => return payload.to_vec(),
        };
        match mutation {
            MessageMutation::HelloZeroLength | MessageMutation::HelloOversizeLength => {
                return payload.to_vec();
            }
            MessageMutation::HelloBadMagic => hello.magic[0] ^= 0xff,
            MessageMutation::HelloUnsupportedControlVersion => {
                hello.control_version = hello.control_version.wrapping_add(1);
            }
            MessageMutation::HelloWrongProtocolVersion => {
                hello.selected_zakura_protocol = hello.selected_zakura_protocol.wrapping_add(1);
            }
            MessageMutation::HelloWrongPath => {
                hello.handshake_path = ZakuraHandshakePath::Upgraded;
            }
            MessageMutation::HelloWrongRole => hello.role = ZakuraControlRole::Responder,
            MessageMutation::HelloWrongNetwork => hello.network_id = ZakuraNetworkId::Testnet,
            MessageMutation::HelloWrongChain => hello.chain_id[0] ^= 0xff,
            MessageMutation::HelloWrongIdentity => hello.iroh_node_id[0] ^= 0xff,
            MessageMutation::HelloUpgradeNonce => hello.initiator_upgrade_nonce[0] = 1,
            MessageMutation::HelloTranscript => hello.legacy_upgrade_transcript[0] = 1,
            MessageMutation::HelloMissingCapability => hello.capabilities = 0,
            MessageMutation::HelloUnsupportedChannel => hello.required_channels = 1,
            MessageMutation::HelloZeroLimit => hello.initial_limits.max_frame_bytes = 0,
            MessageMutation::HelloTrailingByte => {
                let mut bytes = payload.to_vec();
                bytes.push(0xff);
                return bytes;
            }
            _ => return payload.to_vec(),
        }
        return hello
            .encode()
            .expect("a named hello mutation remains encodable");
    }

    let mut ack = match ZakuraControlAck::decode(payload) {
        Ok(ack) => ack,
        Err(_) => return payload.to_vec(),
    };
    match mutation {
        MessageMutation::AckZeroLength | MessageMutation::AckOversizeLength => {
            return payload.to_vec();
        }
        MessageMutation::AckBadMagic => ack.magic[0] ^= 0xff,
        MessageMutation::AckWrongProtocolVersion => {
            ack.selected_zakura_protocol = ack.selected_zakura_protocol.wrapping_add(1);
        }
        MessageMutation::AckWrongRemoteNonce => ack.remote_peer_nonce[0] ^= 0xff,
        MessageMutation::AckExtraCapability => ack.accepted_capabilities |= 1 << 63,
        MessageMutation::AckExtraChannel => ack.accepted_channels |= 1,
        MessageMutation::AckZeroLimit => ack.accepted_limits.max_frame_bytes = 0,
        MessageMutation::AckLimitAboveRequest => {
            ack.accepted_limits.max_frame_bytes = scenario.initiator_frame_bytes.saturating_add(1);
        }
        MessageMutation::AckTrailingByte => {
            let mut bytes = payload.to_vec();
            bytes.push(0xff);
            return bytes;
        }
        _ => return payload.to_vec(),
    }
    ack.encode()
        .expect("a named ack mutation remains encodable")
}

pub(super) fn frame_payload(
    scenario: &HandshakeScenario,
    from_initiator: bool,
    payload: &[u8],
) -> Vec<u8> {
    let payload = mutate_payload(scenario, from_initiator, payload);
    let declared = match (from_initiator, scenario.mutation) {
        (true, MessageMutation::HelloZeroLength) | (false, MessageMutation::AckZeroLength) => 0,
        (true, MessageMutation::HelloOversizeLength)
        | (false, MessageMutation::AckOversizeLength) => u32::try_from(MAX_CONTROL_PAYLOAD_BYTES)
            .expect("the hard control payload cap fits u32")
            .saturating_add(1),
        _ => u32::try_from(payload.len()).expect("control payloads fit u32"),
    };
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&declared.to_le_bytes());
    if declared != 0 && usize::try_from(declared).ok() == Some(payload.len()) {
        framed.extend_from_slice(&payload);
    }
    framed
}

pub(super) struct TokioRead<R>(pub R);

impl<R> ControlRead for TokioRead<R>
where
    R: AsyncRead + Unpin + Send,
{
    async fn read_exact(&mut self, bytes: &mut [u8]) -> Result<(), ZakuraHandlerError> {
        AsyncReadExt::read_exact(&mut self.0, bytes).await?;
        Ok(())
    }
}

pub(super) struct TokioWrite<W> {
    inner: W,
    scenario: HandshakeScenario,
    from_initiator: bool,
    length: Option<Vec<u8>>,
}

impl<W> TokioWrite<W> {
    pub fn new(inner: W, scenario: &HandshakeScenario, from_initiator: bool) -> Self {
        Self {
            inner,
            scenario: scenario.clone(),
            from_initiator,
            length: None,
        }
    }

    fn stalls(&self) -> bool {
        matches!(
            (self.from_initiator, self.scenario.fault),
            (true, FaultAction::StallBeforeHello) | (false, FaultAction::StallBeforeAck)
        )
    }

    fn closes(&self) -> bool {
        matches!(
            (self.from_initiator, self.scenario.fault),
            (true, FaultAction::CloseBeforeHello) | (false, FaultAction::CloseBeforeAck)
        )
    }

    fn cancels(&self) -> bool {
        matches!(
            (self.from_initiator, self.scenario.fault),
            (true, FaultAction::CancelBeforeHello) | (false, FaultAction::CancelBeforeAck)
        )
    }
}

impl<W> ControlWrite for TokioWrite<W>
where
    W: AsyncWrite + Unpin + Send,
{
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), ZakuraHandlerError> {
        if self.length.is_none() {
            self.length = Some(bytes.to_vec());
            return Ok(());
        }
        if self.stalls() {
            std::future::pending::<()>().await;
        }
        if self.closes() {
            return Err(ZakuraHandlerError::Closed);
        }
        if self.cancels() {
            return Err(ZakuraHandlerError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "injected handshake cancellation",
            )));
        }
        let framed = frame_payload(&self.scenario, self.from_initiator, bytes);
        let chunk_bytes = match self.scenario.fault {
            FaultAction::ShortWrites(chunk_bytes) => usize::from(chunk_bytes),
            _ => framed.len().max(1),
        };
        for chunk in framed.chunks(chunk_bytes) {
            AsyncWriteExt::write_all(&mut self.inner, chunk).await?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), ZakuraHandlerError> {
        Ok(())
    }
}
