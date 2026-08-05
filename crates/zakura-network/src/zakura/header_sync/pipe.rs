use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    events::{HeaderSyncEvent, HeaderSyncHandle, HeaderSyncRequestId},
    service::{ExpectedHeadersResponse, HeaderSyncPeerCommand},
    HeaderSyncCodec, MSG_HS_HEADERS, MSG_HS_HEADERS_OUTCOME,
};
use crate::zakura::{Frame, FramedRecv, SinkReject, ZakuraPeerId};

/// Grace period for a response to a request the local reactor has retired.
const CANCELLED_RESPONSE_GRACE: Duration = Duration::from_secs(30);
/// Maximum cancelled response IDs remembered by one peer pipe.
const MAX_CANCELLED_RESPONSE_IDS: usize = 64;

/// Run the sole peer-owned header-sync decode pipe.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_peer(
    handle: HeaderSyncHandle,
    codec: HeaderSyncCodec,
    peer: ZakuraPeerId,
    session_id: u64,
    direction: crate::zakura::ServicePeerDirection,
    mut commands: mpsc::UnboundedReceiver<HeaderSyncPeerCommand>,
    mut recv: FramedRecv,
    cancel: CancellationToken,
) -> Result<(), SinkReject> {
    let mut expected = HashMap::<HeaderSyncRequestId, ExpectedHeadersResponse>::new();
    let mut cancelled = CancelledResponseIds::default();
    loop {
        enum Input {
            Frame(Frame),
            Command(HeaderSyncPeerCommand),
            Done,
        }

        let input = tokio::select! {
            biased;
            () = cancel.cancelled() => Input::Done,
            command = commands.recv() => match command {
                Some(command) => Input::Command(command),
                None => Input::Done,
            },
            frame = recv.recv() => match frame {
                Some(frame) => Input::Frame(frame),
                None => Input::Done,
            },
        };
        let frame = match input {
            Input::Done => return Ok(()),
            Input::Command(command) => {
                apply_command(&mut expected, &mut cancelled, command);
                continue;
            }
            Input::Frame(frame) => frame,
        };

        while let Ok(command) = commands.try_recv() {
            apply_command(&mut expected, &mut cancelled, command);
        }

        let message_type = u8::try_from(frame.message_type).ok();
        let expected_response = if matches!(
            message_type,
            Some(MSG_HS_HEADERS) | Some(MSG_HS_HEADERS_OUTCOME)
        ) {
            let request_id =
                HeaderSyncCodec::peek_response_request_id(&frame).map_err(protocol_reject)?;
            let response = expected.remove(&request_id);
            if response.is_none() && cancelled.take(request_id) {
                continue;
            }
            let Some(response) = response else {
                emit_pipe_violation(
                    &handle,
                    &peer,
                    session_id,
                    direction,
                    "unsolicited_response",
                );
                return Err(protocol_reject("unsolicited header-sync response"));
            };
            Some(response)
        } else {
            None
        };
        let response_context = expected_response.as_ref().and_then(|response| {
            (message_type == Some(MSG_HS_HEADERS)).then_some(response.context)
        });
        let msg = codec
            .decode_frame(frame, response_context)
            .map_err(|error| {
                let reason = match error {
                    super::HeaderSyncWireError::UnknownMessageType(_)
                    | super::HeaderSyncWireError::UnknownFrameMessageType(_)
                    | super::HeaderSyncWireError::MismatchedFrameMessageType { .. } => {
                        "unknown_message_type"
                    }
                    _ => "malformed_message",
                };
                emit_pipe_violation(&handle, &peer, session_id, direction, reason);
                protocol_reject(error)
            })?;
        let event = match expected_response {
            Some(response) => HeaderSyncEvent::SessionResponse {
                peer: peer.clone(),
                session_id,
                scope: response.scope,
                msg,
            },
            None => HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id,
                msg,
            },
        };
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            result = handle.send(event) => {
                result.map_err(|error| SinkReject::local(error.to_string()))?;
            }
        }
    }
}

fn emit_pipe_violation(
    handle: &HeaderSyncHandle,
    peer: &ZakuraPeerId,
    session_id: u64,
    direction: crate::zakura::ServicePeerDirection,
    reason: &'static str,
) {
    use crate::zakura::trace::{header_sync_trace as hs_trace, peer_label, HEADER_SYNC_TABLE};
    let direction = match direction {
        crate::zakura::ServicePeerDirection::Inbound => "inbound",
        crate::zakura::ServicePeerDirection::Outbound => "outbound",
    };
    handle.trace.emit_with(HEADER_SYNC_TABLE, |row| {
        row.insert(
            hs_trace::EVENT.into(),
            hs_trace::HEADER_PEER_VIOLATION.into(),
        );
        row.insert(hs_trace::PEER.into(), peer_label(peer).into());
        row.insert(hs_trace::SESSION_ID.into(), session_id.into());
        row.insert(hs_trace::DIRECTION.into(), direction.into());
        row.insert(hs_trace::REASON.into(), reason.into());
        row.insert(hs_trace::BOUNDARY.into(), "pipe".into());
        row.insert(hs_trace::DISPOSITION.into(), "disconnect".into());
    });
}

fn apply_command(
    expected: &mut HashMap<HeaderSyncRequestId, ExpectedHeadersResponse>,
    cancelled: &mut CancelledResponseIds,
    command: HeaderSyncPeerCommand,
) {
    match command {
        HeaderSyncPeerCommand::Reserve(response) => {
            cancelled.remove(response.request_id);
            expected.insert(response.request_id, response);
        }
        HeaderSyncPeerCommand::Cancel(request_id) => {
            if expected.remove(&request_id).is_some() {
                cancelled.insert(request_id);
            }
        }
    }
}

#[derive(Default)]
struct CancelledResponseIds {
    ids: VecDeque<(HeaderSyncRequestId, tokio::time::Instant)>,
}

impl CancelledResponseIds {
    fn insert(&mut self, request_id: HeaderSyncRequestId) {
        self.expire();
        self.remove(request_id);
        while self.ids.len() >= MAX_CANCELLED_RESPONSE_IDS {
            self.ids.pop_front();
        }
        self.ids.push_back((
            request_id,
            tokio::time::Instant::now() + CANCELLED_RESPONSE_GRACE,
        ));
    }

    fn take(&mut self, request_id: HeaderSyncRequestId) -> bool {
        self.expire();
        let Some(index) = self.ids.iter().position(|(id, _)| *id == request_id) else {
            return false;
        };
        self.ids.remove(index);
        true
    }

    fn remove(&mut self, request_id: HeaderSyncRequestId) {
        self.ids.retain(|(id, _)| *id != request_id);
    }

    fn expire(&mut self) {
        let now = tokio::time::Instant::now();
        while self.ids.front().is_some_and(|(_, expiry)| *expiry <= now) {
            self.ids.pop_front();
        }
    }
}

fn protocol_reject(error: impl std::fmt::Display) -> SinkReject {
    SinkReject::protocol(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        error.to_string(),
    ))
}
