//! The session reader's routing: which frames the subscriber admits before
//! decoding, and where each admitted message goes. It holds no stream,
//! channel, or lock.

use super::super::{
    events::Event,
    v9::{message_type, Admitted, Ended, Headers, Status, SubscribeHeaders},
};
use crate::zakura::ZakuraPeerId;

/// A decoded message that passed every check at the trust boundary. A page
/// or outcome carries the token its subscriber returned on admission.
#[derive(Debug)]
pub(super) enum PeerInput {
    Status(Status),
    Update(SubscribeHeaders),
    Page { page: Headers, admitted: Admitted },
    Outcome(Ended),
}

/// What the reader does with one admitted message.
#[derive(Debug)]
pub(super) enum PeerOutput {
    /// Apply the update to the session's publisher before the next frame,
    /// so updates keep stream order.
    Apply(SubscribeHeaders),
    /// Send the event to the reactor.
    Forward(Event),
}

/// One session reader's routing state: the session it reads.
#[derive(Debug)]
pub(super) struct PeerState {
    peer: ZakuraPeerId,
    session_id: u64,
}

impl PeerState {
    /// The routing state of session `session_id` with `peer`.
    pub(super) fn new(peer: ZakuraPeerId, session_id: u64) -> Self {
        Self { peer, session_id }
    }

    /// Route one admitted message.
    pub(super) fn on_input(&self, input: PeerInput) -> PeerOutput {
        let peer = self.peer.clone();
        let session_id = self.session_id;
        match input {
            PeerInput::Status(status) => PeerOutput::Forward(Event::Status {
                peer,
                session_id,
                status,
            }),
            PeerInput::Update(update) => PeerOutput::Apply(update),
            PeerInput::Page { page, admitted } => PeerOutput::Forward(Event::Page {
                peer,
                session_id,
                page,
                admitted,
            }),
            PeerInput::Outcome(ended) => PeerOutput::Forward(Event::Outcome {
                peer,
                session_id,
                ended,
            }),
        }
    }
}

/// Whether the subscriber checks a frame of `message_type` against its
/// credit before the payload is decoded: pages and terminal outcomes.
pub(super) fn admits_before_decode(message_type: u16) -> bool {
    matches!(
        message_type,
        message_type::HEADERS | message_type::HEADERS_OUTCOME
    )
}
