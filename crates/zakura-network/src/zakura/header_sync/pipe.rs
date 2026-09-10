use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    events::{Event, HeaderSyncHandle, HeaderSyncRequestId},
    service::{ExpectedHeadersResponse, PeerCommand},
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
    mut commands: mpsc::UnboundedReceiver<PeerCommand>,
    mut recv: FramedRecv,
    cancel: CancellationToken,
) -> Result<(), SinkReject> {
    let mut expected = HashMap::<HeaderSyncRequestId, ExpectedHeadersResponse>::new();
    let mut cancelled = CancelledResponseIds::default();
    loop {
        enum Input {
            Frame(Frame),
            Command(PeerCommand),
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
            Some(response) => Event::SessionResponse {
                peer: peer.clone(),
                session_id,
                scope: response.scope,
                msg,
            },
            None => Event::WireMessage {
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
    command: PeerCommand,
) {
    match command {
        PeerCommand::Reserve(response) => {
            cancelled.remove(response.request_id);
            expected.insert(response.request_id, response);
        }
        PeerCommand::Cancel(request_id) => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{framed_channel, header_sync::*, ServicePeerSnapshot};
    use tokio::sync::watch;
    use zakura_chain::{block, parameters::Network};

    fn peer() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![7; 32]).expect("test peer ID has the required length")
    }

    fn scope() -> zakura_header_chain::HeaderWorkAuthority {
        zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(2),
            branch: zakura_header_chain::BranchId::new(block::Hash([0; 32]), block::Hash([3; 32])),
        }
    }

    fn handle(codec: HeaderSyncCodec) -> (HeaderSyncHandle, mpsc::Receiver<Event>) {
        let (events, receiver) = mpsc::channel(4);
        let (lifecycle, _) = mpsc::unbounded_channel();
        let (_, tip) = watch::channel((block::Height(0), block::Hash([0; 32])));
        let (_, peers) = watch::channel(ServicePeerSnapshot::default());
        let (_, candidates) = watch::channel(Default::default());
        (
            HeaderSyncHandle {
                events,
                lifecycle,
                tip,
                peers,
                candidates,
                codec,
                trace: crate::zakura::ZakuraTrace::noop(),
            },
            receiver,
        )
    }

    #[tokio::test]
    async fn discriminator_four_is_always_headers_outcome() {
        let codec = HeaderSyncCodec::new(Network::Mainnet, 1024, 1, 0);
        let outcome = HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
            request_id: 1,
            target_tip_hash: block::Hash([3; 32]),
            outcome: HeadersOutcomeCode::Busy,
        });
        let frame = codec.encode_frame(&outcome).expect("outcome encodes");
        let (send, recv) = framed_channel(1);
        send.send(frame).await.expect("pipe input remains open");
        drop(send);
        let (handle, mut events) = handle(codec.clone());
        let (commands_tx, commands) = mpsc::unbounded_channel();
        commands_tx
            .send(PeerCommand::Reserve(ExpectedHeadersResponse {
                request_id: HeaderSyncRequestId::new(1).expect("one is nonzero"),
                scope: scope(),
                context: HeaderSyncDecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchema::None,
                },
            }))
            .expect("the pipe command receiver is open");

        run_peer(
            handle,
            codec,
            peer(),
            1,
            crate::zakura::ServicePeerDirection::Inbound,
            commands,
            recv,
            CancellationToken::new(),
        )
        .await
        .expect("canonical outcome is accepted");

        assert!(matches!(
            events.recv().await,
            Some(Event::SessionResponse {
                scope: response_scope,
                msg: HeaderSyncMessage::HeadersOutcome(_),
                ..
            }) if response_scope == scope()
        ));
    }

    #[tokio::test]
    async fn unsolicited_and_mismatched_responses_are_protocol_rejected() {
        for (reserved_id, response_id) in [(None, 1), (Some(1), 2)] {
            let codec = HeaderSyncCodec::new(Network::Mainnet, 1024, 1, 0);
            let frame = codec
                .encode_frame(&HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                    request_id: response_id,
                    target_tip_hash: block::Hash([3; 32]),
                    outcome: HeadersOutcomeCode::Busy,
                }))
                .expect("the response fixture encodes");
            let (send, recv) = framed_channel(1);
            send.send(frame).await.expect("pipe input remains open");
            drop(send);
            let (handle, mut events) = handle(codec.clone());
            let (commands_tx, commands) = mpsc::unbounded_channel();
            if let Some(request_id) = reserved_id {
                commands_tx
                    .send(PeerCommand::Reserve(ExpectedHeadersResponse {
                        request_id: HeaderSyncRequestId::new(request_id)
                            .expect("the fixture request ID is nonzero"),
                        scope: scope(),
                        context: HeaderSyncDecodeContext {
                            max_header_count: 1,
                            requested_tree_aux_schema: AuxSchema::None,
                        },
                    }))
                    .expect("the pipe command receiver is open");
            }

            let result = run_peer(
                handle,
                codec,
                peer(),
                1,
                crate::zakura::ServicePeerDirection::Inbound,
                commands,
                recv,
                CancellationToken::new(),
            )
            .await;
            assert!(
                matches!(result, Err(SinkReject::Protocol(_))),
                "an unsolicited or mismatched response is peer-attributable"
            );
            assert!(
                events.try_recv().is_err(),
                "a rejected response never reaches the reactor"
            );
        }
    }

    #[tokio::test]
    async fn locally_cancelled_response_is_dropped_but_unknown_response_is_rejected() {
        let codec = HeaderSyncCodec::new(Network::Mainnet, 1024, 1, 0);
        let late_response = codec
            .encode_frame(&HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                request_id: 1,
                target_tip_hash: block::Hash([3; 32]),
                outcome: HeadersOutcomeCode::Busy,
            }))
            .expect("the late response fixture encodes");
        let (send, recv) = framed_channel(1);
        send.send(late_response)
            .await
            .expect("pipe input remains open");
        drop(send);
        let (handle, mut events) = handle(codec.clone());
        let (commands_tx, commands) = mpsc::unbounded_channel();
        let request_id = HeaderSyncRequestId::new(1).expect("one is nonzero");
        commands_tx
            .send(PeerCommand::Reserve(ExpectedHeadersResponse {
                request_id,
                scope: scope(),
                context: HeaderSyncDecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchema::None,
                },
            }))
            .expect("the pipe command receiver is open");
        commands_tx
            .send(PeerCommand::Cancel(request_id))
            .expect("the pipe command receiver is open");

        run_peer(
            handle,
            codec,
            peer(),
            1,
            crate::zakura::ServicePeerDirection::Inbound,
            commands,
            recv,
            CancellationToken::new(),
        )
        .await
        .expect("a response to a locally cancelled request is not peer-attributable");
        assert!(
            events.try_recv().is_err(),
            "late response never reaches the reactor"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn cancelled_response_grace_is_bounded_and_expires() {
        let mut cancelled = CancelledResponseIds::default();
        for id in 1..=u64::try_from(MAX_CANCELLED_RESPONSE_IDS + 1)
            .expect("the small cancellation cap fits in u64")
        {
            cancelled.insert(HeaderSyncRequestId::new(id).expect("fixture IDs are nonzero"));
        }
        assert_eq!(cancelled.ids.len(), MAX_CANCELLED_RESPONSE_IDS);
        assert!(
            !cancelled.take(HeaderSyncRequestId::new(1).expect("one is nonzero")),
            "the oldest cancellation is evicted at the hard cap"
        );

        tokio::time::advance(CANCELLED_RESPONSE_GRACE).await;
        assert!(
            !cancelled.take(
                HeaderSyncRequestId::new(
                    u64::try_from(MAX_CANCELLED_RESPONSE_IDS + 1)
                        .expect("the small cancellation cap fits in u64")
                )
                .expect("fixture IDs are nonzero")
            ),
            "a response is peer-attributable again after local cancellation grace expires"
        );
    }
}
