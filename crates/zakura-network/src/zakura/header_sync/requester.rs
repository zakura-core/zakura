use super::{service::PreparedGetHeaders, state::*, wire::LOCAL_MAX_HS_INFLIGHT_PER_PEER, *};
use crate::zakura::OrderedSendError;

#[derive(Clone, Debug)]
pub(super) struct HeaderRequesterHandle {
    commands: mpsc::Sender<HeaderRequesterCommand>,
}

impl HeaderRequesterHandle {
    pub(super) fn try_send(
        &self,
        command: HeaderRequesterCommand,
    ) -> Result<(), mpsc::error::TrySendError<HeaderRequesterCommand>> {
        self.commands.try_send(command)
    }
}

pub(super) struct HeaderRequesterCommand {
    pub(super) range: RangeRequest,
    pub(super) wire_request: HeaderSyncWireRequestIdentity,
    pub(super) prepared: PreparedGetHeaders,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct HeaderRequesterId {
    pub(super) peer: ZakuraPeerId,
    pub(super) session_id: u64,
    pub(super) generation: u64,
}

#[derive(Debug)]
pub(super) enum HeaderRequesterEvent {
    Completed {
        requester_id: HeaderRequesterId,
        wire_request: HeaderSyncWireRequestIdentity,
        range: RangeRequest,
        result: Result<(), OrderedSendError>,
    },
    Stopped {
        requester_id: HeaderRequesterId,
    },
}

pub(super) fn spawn_header_requester(
    session: HeaderSyncPeerSession,
    requester_id: HeaderRequesterId,
    events: mpsc::UnboundedSender<HeaderRequesterEvent>,
    shutdown: CancellationToken,
) -> HeaderRequesterHandle {
    let (commands, receiver) = mpsc::channel(usize::from(LOCAL_MAX_HS_INFLIGHT_PER_PEER));
    tokio::spawn(run_header_requester(
        session,
        requester_id,
        receiver,
        events,
        shutdown,
    ));
    HeaderRequesterHandle { commands }
}

async fn run_header_requester(
    session: HeaderSyncPeerSession,
    requester_id: HeaderRequesterId,
    mut commands: mpsc::Receiver<HeaderRequesterCommand>,
    events: mpsc::UnboundedSender<HeaderRequesterEvent>,
    shutdown: CancellationToken,
) {
    let cancel = session.cancel_token();
    let _exit = HeaderRequesterExit {
        requester_id: requester_id.clone(),
        events: events.clone(),
    };

    loop {
        let command = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = cancel.cancelled() => return,
            command = commands.recv() => {
                let Some(command) = command else {
                    return;
                };
                command
            }
        };
        let HeaderRequesterCommand {
            range,
            wire_request,
            prepared,
        } = command;
        debug_assert_eq!(wire_request.request_id, prepared.request_id());
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = cancel.cancelled() => return,
            result = prepared.send() => result.map(|_| ()),
        };
        let transport_closed = matches!(result, Err(OrderedSendError::Closed));
        if events
            .send(HeaderRequesterEvent::Completed {
                requester_id: requester_id.clone(),
                wire_request,
                range,
                result,
            })
            .is_err()
            || transport_closed
        {
            return;
        }
    }
}

struct HeaderRequesterExit {
    requester_id: HeaderRequesterId,
    events: mpsc::UnboundedSender<HeaderRequesterEvent>,
}

impl Drop for HeaderRequesterExit {
    fn drop(&mut self) {
        let _ = self.events.send(HeaderRequesterEvent::Stopped {
            requester_id: self.requester_id.clone(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SESSION_ID: u64 = 41;
    const TEST_GENERATION: u64 = 73;

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
    }

    fn range(start: u32, count: u32) -> RangeRequest {
        RangeRequest {
            range: CheckedHeaderRange::from_count(block::Height(start), count)
                .expect("test range is non-empty and bounded"),
            anchor_hash: None,
            finalized: false,
            want_tree_aux_roots: true,
            priority: RangePriority::Forward,
        }
    }

    fn requester_id(peer: ZakuraPeerId) -> HeaderRequesterId {
        HeaderRequesterId {
            peer,
            session_id: TEST_SESSION_ID,
            generation: TEST_GENERATION,
        }
    }

    fn wire_request(
        requester_id: &HeaderRequesterId,
        request_id: HeaderSyncRequestId,
    ) -> HeaderSyncWireRequestIdentity {
        HeaderSyncWireRequestIdentity {
            peer: requester_id.peer.clone(),
            session_id: requester_id.session_id,
            request_id,
        }
    }

    fn session(
        peer: ZakuraPeerId,
        depth: usize,
    ) -> (
        HeaderSyncPeerSession,
        crate::zakura::FramedRecv,
        CancellationToken,
    ) {
        let (send, recv) = crate::zakura::framed_channel(depth);
        let cancel = CancellationToken::new();
        let session = HeaderSyncPeerSession::from_parts_with_direction_and_session_id(
            peer,
            crate::zakura::ServicePeerDirection::Inbound,
            send,
            cancel.clone(),
            TEST_SESSION_ID,
        );
        (session, recv, cancel)
    }

    async fn next_event(
        events: &mut mpsc::UnboundedReceiver<HeaderRequesterEvent>,
    ) -> HeaderRequesterEvent {
        time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("requester event arrives before timeout")
            .expect("requester event channel stays open")
    }

    #[tokio::test]
    async fn prepared_request_is_published_and_completed() {
        let peer = peer(1);
        let requester_id = requester_id(peer.clone());
        let (session, mut frames, _cancel) = session(peer, 1);
        let range = range(5, 2);
        let prepared = session
            .prepare_get_headers(
                range.start_height(),
                range.count(),
                range.want_tree_aux_roots,
            )
            .expect("valid test request is prepared");
        let request_id = prepared.request_id();
        let wire_request = wire_request(&requester_id, request_id);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let requester =
            spawn_header_requester(session, requester_id.clone(), events_tx, shutdown.clone());

        requester
            .try_send(HeaderRequesterCommand {
                range,
                wire_request: wire_request.clone(),
                prepared,
            })
            .expect("requester command queue has capacity");

        match next_event(&mut events_rx).await {
            HeaderRequesterEvent::Completed {
                requester_id: completed_requester_id,
                wire_request: completed_wire_request,
                range: completed_range,
                result,
            } => {
                assert_eq!(completed_requester_id, requester_id);
                assert_eq!(completed_range, range);
                assert_eq!(completed_wire_request, wire_request);
                result.expect("prepared request is published");
            }
            event => panic!("unexpected requester event: {event:?}"),
        }
        let frame = frames.recv().await.expect("request frame is queued");
        let (message, decoded_request_id) =
            HeaderSyncMessage::decode(&frame.payload, HeaderSyncDecodeContext::control())
                .expect("request frame decodes");
        assert_eq!(
            message,
            HeaderSyncMessage::GetHeaders {
                start_height: range.start_height(),
                count: range.count(),
                want_tree_aux_roots: range.want_tree_aux_roots,
            }
        );
        assert_eq!(decoded_request_id, Some(request_id));

        shutdown.cancel();
        assert!(matches!(
            next_event(&mut events_rx).await,
            HeaderRequesterEvent::Stopped {
                requester_id: stopped_requester_id
            } if stopped_requester_id == requester_id
        ));
    }

    #[tokio::test]
    async fn closed_transport_reports_completion_failure_then_stops() {
        let peer = peer(2);
        let requester_id = requester_id(peer.clone());
        let (session, frames, _cancel) = session(peer, 1);
        let range = range(9, 1);
        let prepared = session
            .prepare_get_headers(
                range.start_height(),
                range.count(),
                range.want_tree_aux_roots,
            )
            .expect("valid test request is prepared");
        let request_id = prepared.request_id();
        let wire_request = wire_request(&requester_id, request_id);
        drop(frames);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let requester = spawn_header_requester(
            session,
            requester_id.clone(),
            events_tx,
            CancellationToken::new(),
        );

        requester
            .try_send(HeaderRequesterCommand {
                range,
                wire_request: wire_request.clone(),
                prepared,
            })
            .expect("requester command queue has capacity");

        match next_event(&mut events_rx).await {
            HeaderRequesterEvent::Completed {
                requester_id: completed_requester_id,
                wire_request: completed_wire_request,
                range: completed_range,
                result: Err(OrderedSendError::Closed),
            } => {
                assert_eq!(completed_requester_id, requester_id);
                assert_eq!(completed_range, range);
                assert_eq!(completed_wire_request, wire_request);
            }
            event => panic!("unexpected requester event: {event:?}"),
        }
        assert!(matches!(
            next_event(&mut events_rx).await,
            HeaderRequesterEvent::Stopped {
                requester_id: stopped_requester_id
            } if stopped_requester_id == requester_id
        ));
    }
}
