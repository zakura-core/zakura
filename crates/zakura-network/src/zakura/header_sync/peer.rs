//! The header-sync peer routine: one session's reader.
//!
//! [`PeerState`] routes each admitted message. This module holds the
//! stream, the trust boundary that admits and decodes each frame, and the
//! publisher task the reader watches.

use zakura_chain::{block, parameters::Network};
use zakura_rt::{sync::CancellationToken, task::JoinHandle};

use super::{
    events::HeaderSyncHandle,
    service::PeerSession,
    v9::{Headers, Message, Publisher},
    wire::{HeaderSyncWireError, TreeAuxWire},
};
use crate::zakura::{wire_codec::decode_frame, Frame, FramedRecv, SinkReject};

mod state;

use state::{admits_before_decode, PeerInput, PeerOutput, PeerState};

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
    let state = PeerState::new(session.peer_id().clone(), session.session_id());
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
        let input = admit::<HEADER_BYTES>(&handle, &session, frame)?;
        match state.on_input(input) {
            PeerOutput::Apply(update) => {
                publisher
                    .apply(&update)
                    .await
                    .map_err(|fault| violation(&handle, &session, "subscription_update", fault))?;
            }
            PeerOutput::Forward(event) => {
                zakura_rt::select! {
                    biased;
                    () = cancel.cancelled() => return Ok(()),
                    result = handle.send(event) => {
                        result.map_err(|error| SinkReject::local(error.to_string()))?;
                    }
                }
            }
        }
    }
}

/// Admit and decode one `frame` at the session's trust boundary.
///
/// The subscriber checks a page or outcome header against its credit before
/// the payload is decoded. A decoded `Status` updates the subscriber's caps,
/// a page passes its network-dependent record checks and the subscriber's
/// page checks, and an outcome passes the subscriber's window check. A frame
/// that breaks a rule is a protocol violation.
fn admit<const HEADER_BYTES: usize>(
    handle: &HeaderSyncHandle,
    session: &PeerSession,
    frame: Frame,
) -> Result<PeerInput, SinkReject> {
    let payload_len = frame.payload.len();
    if admits_before_decode(frame.message_type) {
        session
            .subscriber()
            .admit(&frame)
            .map_err(|fault| violation(handle, session, fault.label(), fault))?;
    }
    let message = decode_frame::<Message<HEADER_BYTES>>(&frame)
        .map_err(|error| violation(handle, session, "malformed_message", error))?;
    Ok(match message {
        Message::Status(status) => {
            session.subscriber().observe(&status);
            PeerInput::Status(status)
        }
        Message::SubscribeHeaders(update) => PeerInput::Update(update),
        Message::Headers(page) => {
            check_records(&page, &handle.network)
                .map_err(|error| violation(handle, session, "malformed_message", error))?;
            let admitted = session
                .subscriber()
                .receive_page(&page, payload_len)
                .map_err(|fault| violation(handle, session, fault.label(), fault))?;
            PeerInput::Page { page, admitted }
        }
        Message::HeadersOutcome(outcome) => {
            let ended = session
                .subscriber()
                .receive_outcome(&outcome)
                .map_err(|fault| violation(handle, session, fault.label(), fault))?;
            PeerInput::Outcome(ended)
        }
    })
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
