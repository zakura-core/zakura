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

#[derive(Clone, Debug)]
pub(super) enum HeaderRequesterEvent {
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

        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = cancel.cancelled() => return,
            result = session.send_get_headers(
                command.range.start_height,
                command.expected_max_count,
                command.range.want_tree_aux_roots,
            ) => result,
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
