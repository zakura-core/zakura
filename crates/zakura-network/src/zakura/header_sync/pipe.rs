use tokio_util::sync::CancellationToken;

use super::{
    events::{Event, HeaderSyncHandle},
    service::ResponseReservations,
    HeaderSyncCodec, MSG_HS_HEADERS, MSG_HS_HEADERS_OUTCOME,
};
use crate::zakura::{regulation::Verdict, FramedRecv, SinkReject, ZakuraPeerId};

/// Run the sole peer-owned header-sync decode pipe.
///
/// A response must claim the reservation its request made before it is
/// decoded. A response to a request the reactor already retired still claims
/// its reservation and reaches the reactor, which drops it as late.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_peer(
    handle: HeaderSyncHandle,
    codec: HeaderSyncCodec,
    peer: ZakuraPeerId,
    session_id: u64,
    direction: crate::zakura::ServicePeerDirection,
    reservations: ResponseReservations,
    mut recv: FramedRecv,
    cancel: CancellationToken,
) -> Result<(), SinkReject> {
    loop {
        let frame = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            frame = recv.recv() => match frame {
                Some(frame) => frame,
                None => return Ok(()),
            },
        };

        let message_type = u8::try_from(frame.message_type).ok();
        let expected_response = if matches!(
            message_type,
            Some(MSG_HS_HEADERS) | Some(MSG_HS_HEADERS_OUTCOME)
        ) {
            let request_id =
                HeaderSyncCodec::peek_response_request_id(&frame).map_err(protocol_reject)?;
            let claimed = reservations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .claim(&request_id, frame.payload.len());
            match claimed {
                Ok(response) => Some(response),
                Err(refused) => {
                    emit_pipe_violation(&handle, &peer, session_id, direction, refused.label());
                    return Verdict::from(refused).into_stream_result();
                }
            }
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

fn protocol_reject(error: impl std::fmt::Display) -> SinkReject {
    SinkReject::protocol(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        error.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{
        framed_channel,
        header_sync::{
            service::{ExpectedHeadersResponse, MAX_HS_LIVE_RESPONSE_RESERVATIONS},
            *,
        },
        regulation::Reservations,
        ServicePeerSnapshot,
    };
    use std::sync::{Arc, Mutex};
    use tokio::sync::{mpsc, watch};
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

    fn expected(request_id: u64) -> ExpectedHeadersResponse {
        ExpectedHeadersResponse {
            request_id: HeaderSyncRequestId::new(request_id).expect("fixture IDs are nonzero"),
            scope: scope(),
            context: HeaderSyncDecodeContext {
                max_header_count: 1,
                requested_tree_aux_schema: AuxSchema::None,
            },
        }
    }

    /// A reservation map with one live reservation per listed request ID.
    fn reserved(request_ids: &[u64]) -> ResponseReservations {
        let mut map = Reservations::new(MAX_HS_LIVE_RESPONSE_RESERVATIONS);
        for request_id in request_ids {
            let response = expected(*request_id);
            map.reserve(response.request_id, MAX_HS_MESSAGE_BYTES, response)
                .expect("fixtures stay below the reservation cap");
        }
        Arc::new(Mutex::new(map))
    }

    fn outcome_frame(codec: &HeaderSyncCodec, request_id: u64) -> Frame {
        codec
            .encode_frame(&HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                request_id,
                target_tip_hash: block::Hash([3; 32]),
                outcome: HeadersOutcomeCode::Busy,
            }))
            .expect("the outcome fixture encodes")
    }

    /// Feed `frames` to a fresh pipe and return its result and delivered events.
    async fn run_frames(
        reservations: ResponseReservations,
        frames: Vec<Frame>,
    ) -> (Result<(), SinkReject>, Vec<Event>) {
        let codec = HeaderSyncCodec::new(Network::Mainnet, 1024, 1, 0);
        let (send, recv) = framed_channel(frames.len().max(1));
        for frame in frames {
            send.send(frame).await.expect("pipe input remains open");
        }
        drop(send);
        let (handle, mut events) = handle(codec.clone());
        let result = run_peer(
            handle,
            codec,
            peer(),
            1,
            crate::zakura::ServicePeerDirection::Inbound,
            reservations,
            recv,
            CancellationToken::new(),
        )
        .await;
        let mut delivered = Vec::new();
        while let Ok(event) = events.try_recv() {
            delivered.push(event);
        }
        (result, delivered)
    }

    #[tokio::test]
    async fn discriminator_four_is_always_headers_outcome() {
        let codec = HeaderSyncCodec::new(Network::Mainnet, 1024, 1, 0);
        let (result, events) = run_frames(reserved(&[1]), vec![outcome_frame(&codec, 1)]).await;
        result.expect("canonical outcome is accepted");
        assert!(matches!(
            events.as_slice(),
            [Event::SessionResponse {
                scope: response_scope,
                msg: HeaderSyncMessage::HeadersOutcome(_),
                ..
            }] if *response_scope == scope()
        ));
    }

    #[tokio::test]
    async fn unsolicited_duplicate_and_mismatched_responses_disconnect() {
        let codec = HeaderSyncCodec::new(Network::Mainnet, 1024, 1, 0);
        for (reserved_ids, response_ids) in
            [(vec![], vec![1]), (vec![1], vec![2]), (vec![1], vec![1, 1])]
        {
            let frames = response_ids
                .iter()
                .map(|request_id| outcome_frame(&codec, *request_id))
                .collect();
            let (result, events) = run_frames(reserved(&reserved_ids), frames).await;
            assert!(
                matches!(result, Err(SinkReject::Protocol(_))),
                "an unsolicited or duplicate response is peer-attributable"
            );
            assert_eq!(
                events.len(),
                response_ids.len() - 1,
                "only the claimed response reaches the reactor"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_response_still_claims_its_reservation_and_reaches_the_reactor() {
        let codec = HeaderSyncCodec::new(Network::Mainnet, 1024, 1, 0);
        let reservations = reserved(&[1]);
        // The reactor has retired request 1 long ago; the reservation remains.
        tokio::time::advance(std::time::Duration::from_secs(24 * 60 * 60)).await;
        let (result, events) =
            run_frames(reservations.clone(), vec![outcome_frame(&codec, 1)]).await;
        result.expect("a late response is not a violation");
        assert_eq!(
            events.len(),
            1,
            "the reactor decides that the response is late"
        );
        assert_eq!(reservations.lock().expect("not poisoned").len(), 0);
    }
}
