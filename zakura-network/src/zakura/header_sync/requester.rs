use super::{state::*, wire::LOCAL_MAX_HS_INFLIGHT_PER_PEER, *};
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

#[derive(Copy, Clone, Debug)]
pub(super) struct HeaderRequesterCommand {
    pub(super) range: RangeRequest,
    pub(super) expected_max_count: u32,
    pub(super) purpose: RangePurpose,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum HeaderRequesterFailure {
    Closed,
    Encode,
}

#[derive(Debug)]
pub(super) enum HeaderRequesterEvent {
    Register {
        peer: ZakuraPeerId,
        session_id: u64,
        generation: u64,
        command: HeaderRequesterCommand,
        request_id: HeaderSyncRequestId,
        registered: tokio::sync::oneshot::Sender<()>,
    },
    Sent {
        peer: ZakuraPeerId,
        session_id: u64,
        generation: u64,
        command: HeaderRequesterCommand,
        request_id: HeaderSyncRequestId,
    },
    Failed {
        peer: ZakuraPeerId,
        session_id: u64,
        generation: u64,
        command: HeaderRequesterCommand,
        request_id: Option<HeaderSyncRequestId>,
        reason: HeaderRequesterFailure,
    },
    Stopped {
        peer: ZakuraPeerId,
        session_id: u64,
        generation: u64,
    },
}

pub(super) fn spawn_header_requester(
    session: HeaderSyncPeerSession,
    generation: u64,
    events: mpsc::UnboundedSender<HeaderRequesterEvent>,
    shutdown: CancellationToken,
) -> HeaderRequesterHandle {
    let (commands, receiver) = mpsc::channel(usize::from(LOCAL_MAX_HS_INFLIGHT_PER_PEER));
    tokio::spawn(run_header_requester(
        session, generation, receiver, events, shutdown,
    ));
    HeaderRequesterHandle { commands }
}

async fn run_header_requester(
    session: HeaderSyncPeerSession,
    generation: u64,
    mut commands: mpsc::Receiver<HeaderRequesterCommand>,
    events: mpsc::UnboundedSender<HeaderRequesterEvent>,
    shutdown: CancellationToken,
) {
    let peer = session.peer_id().clone();
    let session_id = session.session_id();
    let cancel = session.cancel_token();
    let _exit = HeaderRequesterExit {
        peer: peer.clone(),
        session_id,
        generation,
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

        let prepared = match session.prepare_get_headers(
            command.range.start_height,
            command.expected_max_count,
            command.range.want_tree_aux_roots,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                let reason = if matches!(error, OrderedSendError::Closed) {
                    HeaderRequesterFailure::Closed
                } else {
                    HeaderRequesterFailure::Encode
                };
                if events
                    .send(HeaderRequesterEvent::Failed {
                        peer: peer.clone(),
                        session_id,
                        generation,
                        command,
                        request_id: None,
                        reason,
                    })
                    .is_err()
                    || reason == HeaderRequesterFailure::Closed
                {
                    return;
                }
                continue;
            }
        };
        let request_id = prepared.request_id();
        let (registered, registration) = tokio::sync::oneshot::channel();
        if events
            .send(HeaderRequesterEvent::Register {
                peer: peer.clone(),
                session_id,
                generation,
                command,
                request_id,
                registered,
            })
            .is_err()
        {
            return;
        }
        let registered = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = cancel.cancelled() => return,
            registered = registration => registered,
        };
        if registered.is_err() {
            return;
        }

        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = cancel.cancelled() => return,
            result = prepared.send() => result,
        };
        match result {
            Ok(request_id) => {
                if events
                    .send(HeaderRequesterEvent::Sent {
                        peer: peer.clone(),
                        session_id,
                        generation,
                        command,
                        request_id,
                    })
                    .is_err()
                {
                    return;
                }
            }
            Err(error) => {
                let reason = if matches!(error, OrderedSendError::Closed) {
                    HeaderRequesterFailure::Closed
                } else {
                    HeaderRequesterFailure::Encode
                };
                if events
                    .send(HeaderRequesterEvent::Failed {
                        peer: peer.clone(),
                        session_id,
                        generation,
                        command,
                        request_id: Some(request_id),
                        reason,
                    })
                    .is_err()
                    || reason == HeaderRequesterFailure::Closed
                {
                    return;
                }
            }
        }
    }
}

struct HeaderRequesterExit {
    peer: ZakuraPeerId,
    session_id: u64,
    generation: u64,
    events: mpsc::UnboundedSender<HeaderRequesterEvent>,
}

impl Drop for HeaderRequesterExit {
    fn drop(&mut self) {
        let _ = self.events.send(HeaderRequesterEvent::Stopped {
            peer: self.peer.clone(),
            session_id: self.session_id,
            generation: self.generation,
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

    fn command(start: u32, expected_max_count: u32) -> HeaderRequesterCommand {
        HeaderRequesterCommand {
            range: RangeRequest {
                start_height: block::Height(start),
                count: expected_max_count.max(1),
                anchor_hash: None,
                finalized: false,
                want_tree_aux_roots: true,
                priority: RangePriority::Forward,
            },
            expected_max_count,
            purpose: RangePurpose::Sync,
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

    fn assert_command_eq(actual: HeaderRequesterCommand, expected: HeaderRequesterCommand) {
        assert_eq!(actual.range, expected.range);
        assert_eq!(actual.expected_max_count, expected.expected_max_count);
        assert_eq!(actual.purpose, expected.purpose);
    }

    #[tokio::test]
    async fn request_waits_for_reactor_registration_before_publication() {
        let peer = peer(1);
        let (session, mut frames, _cancel) = session(peer.clone(), 1);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let requester =
            spawn_header_requester(session, TEST_GENERATION, events_tx, shutdown.clone());
        let command = command(5, 2);

        requester
            .try_send(command)
            .expect("requester command queue has capacity");

        let (request_id, registered) = match next_event(&mut events_rx).await {
            HeaderRequesterEvent::Register {
                peer: event_peer,
                session_id,
                generation,
                command: registered_command,
                request_id,
                registered,
            } => {
                assert_eq!(event_peer, peer);
                assert_eq!(session_id, TEST_SESSION_ID);
                assert_eq!(generation, TEST_GENERATION);
                assert_command_eq(registered_command, command);
                (request_id, registered)
            }
            event => panic!("unexpected requester event: {event:?}"),
        };
        assert!(time::timeout(Duration::from_millis(20), frames.recv())
            .await
            .is_err());
        registered
            .send(())
            .expect("requester waits for reactor registration");

        match next_event(&mut events_rx).await {
            HeaderRequesterEvent::Sent {
                peer: event_peer,
                session_id,
                generation,
                command: sent,
                request_id: sent_request_id,
            } => {
                assert_eq!(event_peer, peer);
                assert_eq!(session_id, TEST_SESSION_ID);
                assert_eq!(generation, TEST_GENERATION);
                assert_command_eq(sent, command);
                assert_eq!(sent_request_id, request_id);
            }
            event => panic!("unexpected requester event: {event:?}"),
        }
        let frame = time::timeout(Duration::from_secs(1), frames.recv())
            .await
            .expect("request frame arrives before timeout")
            .expect("request frame channel stays open");
        let (message, decoded_request_id) =
            HeaderSyncMessage::decode(&frame.payload, HeaderSyncDecodeContext::control())
                .expect("request frame decodes");
        assert_eq!(
            message,
            HeaderSyncMessage::GetHeaders {
                start_height: command.range.start_height,
                count: command.expected_max_count,
                want_tree_aux_roots: command.range.want_tree_aux_roots,
            }
        );
        assert_eq!(decoded_request_id, Some(request_id));

        shutdown.cancel();
        assert!(matches!(
            next_event(&mut events_rx).await,
            HeaderRequesterEvent::Stopped {
                peer: event_peer,
                session_id: TEST_SESSION_ID,
                generation: TEST_GENERATION,
            } if event_peer == peer
        ));
    }

    #[tokio::test]
    async fn encode_failure_reports_the_command_and_requester_continues() {
        let peer = peer(2);
        let (session, mut frames, _cancel) = session(peer.clone(), 1);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let requester =
            spawn_header_requester(session, TEST_GENERATION, events_tx, shutdown.clone());
        let invalid = command(7, 0);
        let valid = command(8, 1);

        requester
            .try_send(invalid)
            .expect("requester command queue has capacity");
        match next_event(&mut events_rx).await {
            HeaderRequesterEvent::Failed {
                peer: event_peer,
                session_id,
                generation,
                command: failed,
                request_id,
                reason,
            } => {
                assert_eq!(event_peer, peer);
                assert_eq!(session_id, TEST_SESSION_ID);
                assert_eq!(generation, TEST_GENERATION);
                assert_command_eq(failed, invalid);
                assert_eq!(request_id, None);
                assert_eq!(reason, HeaderRequesterFailure::Encode);
            }
            event => panic!("unexpected requester event: {event:?}"),
        }

        requester
            .try_send(valid)
            .expect("requester remains available after an encode failure");
        let registered = match next_event(&mut events_rx).await {
            HeaderRequesterEvent::Register {
                command: registered_command,
                registered,
                ..
            } => {
                assert_command_eq(registered_command, valid);
                registered
            }
            event => panic!("unexpected requester event: {event:?}"),
        };
        registered
            .send(())
            .expect("requester waits for reactor registration");
        assert!(matches!(
            next_event(&mut events_rx).await,
            HeaderRequesterEvent::Sent { command: sent, .. }
                if sent.range == valid.range
                    && sent.expected_max_count == valid.expected_max_count
                    && sent.purpose == valid.purpose
        ));
        time::timeout(Duration::from_secs(1), frames.recv())
            .await
            .expect("valid request frame arrives before timeout")
            .expect("request frame channel stays open");

        shutdown.cancel();
        assert!(matches!(
            next_event(&mut events_rx).await,
            HeaderRequesterEvent::Stopped { .. }
        ));
    }

    #[tokio::test]
    async fn closed_send_reports_failure_then_stops() {
        let peer = peer(3);
        let (session, frames, _cancel) = session(peer.clone(), 1);
        drop(frames);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let requester = spawn_header_requester(
            session,
            TEST_GENERATION,
            events_tx,
            CancellationToken::new(),
        );
        let command = command(9, 1);

        requester
            .try_send(command)
            .expect("requester command queue has capacity");

        let registered = match next_event(&mut events_rx).await {
            HeaderRequesterEvent::Register {
                command: registered_command,
                registered,
                ..
            } => {
                assert_command_eq(registered_command, command);
                registered
            }
            event => panic!("unexpected requester event: {event:?}"),
        };
        registered
            .send(())
            .expect("requester waits for reactor registration");

        match next_event(&mut events_rx).await {
            HeaderRequesterEvent::Failed {
                peer: event_peer,
                session_id,
                generation,
                command: failed,
                request_id,
                reason,
            } => {
                assert_eq!(event_peer, peer);
                assert_eq!(session_id, TEST_SESSION_ID);
                assert_eq!(generation, TEST_GENERATION);
                assert_command_eq(failed, command);
                assert!(request_id.is_some());
                assert_eq!(reason, HeaderRequesterFailure::Closed);
            }
            event => panic!("unexpected requester event: {event:?}"),
        }
        assert!(matches!(
            next_event(&mut events_rx).await,
            HeaderRequesterEvent::Stopped {
                peer: event_peer,
                session_id: TEST_SESSION_ID,
                generation: TEST_GENERATION,
            } if event_peer == peer
        ));
    }
}
