use zakura_chain::{block, parameters::Network};
use zakura_rt::{sync::CancellationToken, task::JoinHandle};

use super::{
    events::{Event, HeaderSyncHandle},
    service::PeerSession,
    v9::{message_type, Headers, Message, Publisher},
    wire::{HeaderSyncWireError, TreeAuxWire},
};
use crate::zakura::{wire_codec::decode_frame, FramedRecv, SinkReject};

/// Run the sole peer-owned header-sync session reader.
///
/// It decodes each frame, applies each `SubscribeHeaders` update to this
/// session's publisher in stream order, and admits each page and outcome
/// through the session's subscriber before the reactor sees it. A frame that
/// breaks a rule ends the session as a protocol violation. The reader also
/// ends with the publisher task, and re-raises the task's panic.
pub(super) async fn run_peer<const HEADER_BYTES: usize>(
    handle: HeaderSyncHandle,
    session: PeerSession,
    publisher: Publisher,
    mut publishing: JoinHandle<()>,
    mut recv: FramedRecv,
    cancel: CancellationToken,
) -> Result<(), SinkReject> {
    let peer = session.peer_id().clone();
    let session_id = session.session_id();
    loop {
        let frame = zakura_rt::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            ended = &mut publishing => {
                if let Err(error) = ended {
                    if error.is_panic() {
                        std::panic::resume_unwind(error.into_panic());
                    }
                }
                return Ok(());
            }
            frame = recv.recv() => match frame {
                Some(frame) => frame,
                None => return Ok(()),
            },
        };
        let payload_len = frame.payload.len();
        if matches!(
            frame.message_type,
            message_type::HEADERS | message_type::HEADERS_OUTCOME
        ) {
            session
                .subscriber()
                .admit(&frame)
                .map_err(|fault| violation(&handle, &session, fault.label(), fault))?;
        }
        let message = decode_frame::<Message<HEADER_BYTES>>(&frame)
            .map_err(|error| violation(&handle, &session, "malformed_message", error))?;
        let event = match message {
            Message::Status(status) => {
                session.subscriber().observe(&status);
                Event::Status {
                    peer: peer.clone(),
                    session_id,
                    status,
                }
            }
            Message::SubscribeHeaders(update) => {
                publisher
                    .apply(&update)
                    .await
                    .map_err(|fault| violation(&handle, &session, "subscription_update", fault))?;
                continue;
            }
            Message::Headers(page) => {
                check_records(&page, &handle.network)
                    .map_err(|error| violation(&handle, &session, "malformed_message", error))?;
                let admitted = session
                    .subscriber()
                    .receive_page(&page, payload_len)
                    .map_err(|fault| violation(&handle, &session, fault.label(), fault))?;
                Event::Page {
                    peer: peer.clone(),
                    session_id,
                    page,
                    admitted,
                }
            }
            Message::HeadersOutcome(outcome) => {
                let ended = session
                    .subscriber()
                    .receive_outcome(&outcome)
                    .map_err(|fault| violation(&handle, &session, fault.label(), fault))?;
                Event::Outcome {
                    peer: peer.clone(),
                    session_id,
                    ended,
                }
            }
        };
        zakura_rt::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            result = handle.send(event) => {
                result.map_err(|error| SinkReject::local(error.to_string()))?;
            }
        }
    }
}

/// Check each schema-1 record against its header's height and the network's
/// activation defaults, which the codec cannot see.
fn check_records(page: &Headers, network: &Network) -> Result<(), HeaderSyncWireError> {
    // The decoder rejects a page whose last height overflows.
    let heights = (page.common_ancestor_height.0 + 1..).map(block::Height);
    for (height, entry) in heights.zip(&page.entries) {
        if let Some(record) = &entry.tree_aux {
            record.validate_for(height, network)?;
        }
    }
    Ok(())
}

/// Trace a protocol violation by `session`'s peer, and return the rejection
/// that ends the session.
fn violation(
    handle: &HeaderSyncHandle,
    session: &PeerSession,
    reason: &'static str,
    error: impl std::fmt::Display,
) -> SinkReject {
    use crate::zakura::trace::{header_sync_trace as hs_trace, peer_label, HEADER_SYNC_TABLE};
    let direction = match session.direction() {
        crate::zakura::ServicePeerDirection::Inbound => "inbound",
        crate::zakura::ServicePeerDirection::Outbound => "outbound",
    };
    handle.trace.emit_with(HEADER_SYNC_TABLE, |row| {
        row.insert(
            hs_trace::EVENT.into(),
            hs_trace::HEADER_PEER_VIOLATION.into(),
        );
        row.insert(hs_trace::PEER.into(), peer_label(session.peer_id()).into());
        row.insert(hs_trace::SESSION_ID.into(), session.session_id().into());
        row.insert(hs_trace::DIRECTION.into(), direction.into());
        row.insert(hs_trace::REASON.into(), reason.into());
        row.insert(hs_trace::BOUNDARY.into(), "pipe".into());
        row.insert(hs_trace::DISPOSITION.into(), "disconnect".into());
    });
    SinkReject::protocol(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        error.to_string(),
    ))
}
