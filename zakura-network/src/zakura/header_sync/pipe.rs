use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    events::{HeaderSyncEvent, HeaderSyncHandle, HeaderSyncRequestId},
    service::{ExpectedHeadersResponse, HeaderSyncPeerCommand},
    HeaderSyncCodec, MSG_HS_HEADERS,
};
use crate::zakura::{Frame, FramedRecv, SinkReject, ZakuraPeerId};

/// Run the sole peer-owned header-sync decode pipe.
pub(super) async fn run_peer(
    handle: HeaderSyncHandle,
    codec: HeaderSyncCodec,
    peer: ZakuraPeerId,
    session_id: u64,
    mut commands: mpsc::UnboundedReceiver<HeaderSyncPeerCommand>,
    mut recv: FramedRecv,
    cancel: CancellationToken,
) -> Result<(), SinkReject> {
    let mut expected = HashMap::<HeaderSyncRequestId, ExpectedHeadersResponse>::new();
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
                apply_command(&mut expected, command);
                continue;
            }
            Input::Frame(frame) => frame,
        };

        while let Ok(command) = commands.try_recv() {
            apply_command(&mut expected, command);
        }

        let response_context = if u8::try_from(frame.message_type).ok() == Some(MSG_HS_HEADERS) {
            let request_id =
                HeaderSyncCodec::peek_headers_request_id(&frame).map_err(protocol_reject)?;
            expected
                .remove(&request_id)
                .map(|response| response.context)
        } else {
            None
        };
        let msg = codec
            .decode_frame(frame, response_context)
            .map_err(protocol_reject)?;
        handle
            .try_send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id,
                msg,
            })
            .map_err(|error| SinkReject::local(error.to_string()))?;
    }
}

fn apply_command(
    expected: &mut HashMap<HeaderSyncRequestId, ExpectedHeadersResponse>,
    command: HeaderSyncPeerCommand,
) {
    match command {
        HeaderSyncPeerCommand::Reserve(response) => {
            expected.insert(response.request_id, response);
        }
        HeaderSyncPeerCommand::Cancel(request_id) => {
            expected.remove(&request_id);
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

    fn handle(codec: HeaderSyncCodec) -> (HeaderSyncHandle, mpsc::Receiver<HeaderSyncEvent>) {
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
        let (_commands_tx, commands) = mpsc::unbounded_channel();

        run_peer(
            handle,
            codec,
            peer(),
            1,
            commands,
            recv,
            CancellationToken::new(),
        )
        .await
        .expect("canonical outcome is accepted");

        assert!(matches!(
            events.recv().await,
            Some(HeaderSyncEvent::SessionWireMessage {
                msg: HeaderSyncMessage::HeadersOutcome(_),
                ..
            })
        ));
    }
}
