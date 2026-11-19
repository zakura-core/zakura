//! The session reader's routing: which frames the subscriber admits before
//! decoding, where each admitted message goes, and how often a `Status`
//! reaches the reactor. It holds no stream, channel, or lock.

use zakura_rt::time::Instant;

use super::super::{
    events::Event,
    v9::{message_type, Admitted, Ended, Headers, Status, SubscribeHeaders, STATUS_CADENCE},
};
use crate::zakura::{regulation::SharedBucket, Cadence, ZakuraPeerId};

/// The meter on the `Status` events one connection's sessions send the
/// reactor. The connection keeps it, so a reopened session gets no fresh
/// burst (F-799).
///
/// `Status` shares its stream with subscription updates and pages, so the
/// stream's reader forwards an exhausted `Status` instead of parking. This
/// meter bounds the reactor's work instead: two events back to back, then
/// one per row refill interval. A conformant sender's `Status` frames come
/// at least one send interval, two refill intervals, apart, so each finds a
/// token. A `Status` that finds none waits, and a newer one replaces it,
/// because the reactor keeps only each peer's latest.
pub(in crate::zakura::header_sync) const STATUS_EVENTS: Cadence = Cadence {
    capacity: 2,
    ..STATUS_CADENCE
};

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
    /// Nothing yet: a `Status` waits for the meter. See
    /// [`PeerState::status_due_at`].
    Hold,
}

/// One session reader's routing state: the session it reads, and its
/// `Status` meter.
#[derive(Debug)]
pub(super) struct PeerState {
    peer: ZakuraPeerId,
    session_id: u64,
    /// The connection's [`STATUS_EVENTS`] meter.
    status_meter: SharedBucket,
    /// The latest `Status` the meter held back.
    held_status: Option<Status>,
}

impl PeerState {
    /// The routing state of session `session_id` with `peer`, whose
    /// connection keeps `status_meter`.
    pub(super) fn new(peer: ZakuraPeerId, session_id: u64, status_meter: SharedBucket) -> Self {
        Self {
            peer,
            session_id,
            status_meter,
            held_status: None,
        }
    }

    /// Route one admitted message that arrived at `now`.
    pub(super) fn on_input(&mut self, input: PeerInput, now: Instant) -> PeerOutput {
        let peer = self.peer.clone();
        let session_id = self.session_id;
        match input {
            PeerInput::Status(status) => {
                self.held_status = Some(status);
                self.release_status(now).unwrap_or(PeerOutput::Hold)
            }
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

    /// When the held `Status` may go, if one is held.
    pub(super) fn status_due_at(&self) -> Option<Instant> {
        self.held_status.as_ref()?;
        self.status_meter.next_token_at()
    }

    /// The held `Status`'s event, if one is held and the meter has a token at
    /// `now`.
    pub(super) fn release_status(&mut self, now: Instant) -> Option<PeerOutput> {
        self.held_status.as_ref()?;
        self.status_meter.take(now).ok()?;
        let status = self.held_status.take()?;
        Some(PeerOutput::Forward(Event::Status {
            peer: self.peer.clone(),
            session_id: self.session_id,
            status,
        }))
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::zakura::header_sync::{config::REGTEST_HEADER_BYTES, v9::test_support::status};

    fn status_at(height: u32) -> Status {
        let target = zakura_header_chain::Frontier::new(
            zakura_chain::block::Height(height),
            zakura_chain::block::Hash([u8::try_from(height % 256).unwrap(); 32]),
        );
        status::<REGTEST_HEADER_BYTES>(target)
    }

    fn forwarded_height(output: PeerOutput) -> u32 {
        match output {
            PeerOutput::Forward(Event::Status { status, .. }) => status.selected_tip_height.0,
            other => panic!("expected a Status event, got {other:?}"),
        }
    }

    /// hsync-19: a `Status` flood reaches the reactor at most
    /// `2 + elapsed / 500 ms` times, each released at its exact token
    /// boundary, and the reactor gets the latest value.
    #[test]
    fn a_status_flood_reaches_the_reactor_at_the_meter_rate() {
        let peer = ZakuraPeerId::new(vec![7; 32]).unwrap();
        let mut state = PeerState::new(peer, 1, SharedBucket::new(STATUS_EVENTS));
        let start = Instant::now();
        let mut forwarded = Vec::new();
        for height in 1..=100 {
            match state.on_input(PeerInput::Status(status_at(height)), start) {
                PeerOutput::Hold => {}
                output => forwarded.push(forwarded_height(output)),
            }
        }
        assert_eq!(forwarded, [1, 2]);
        let refill = STATUS_EVENTS.refill_interval;
        assert_eq!(state.status_due_at(), Some(start + refill));
        assert!(state
            .release_status(start + refill - Duration::from_nanos(1))
            .is_none());
        let released = state
            .release_status(start + refill)
            .expect("a token returned");
        assert_eq!(forwarded_height(released), 100, "the latest Status goes");
        assert_eq!(state.status_due_at(), None, "nothing is held");

        // Keep flooding for ten seconds: at most one event per refill.
        let mut events = 3u32;
        let mut height = 100;
        let mut now = start + refill;
        while now < start + Duration::from_secs(10) {
            now += Duration::from_millis(10);
            height += 1;
            if !matches!(
                state.on_input(PeerInput::Status(status_at(height)), now),
                PeerOutput::Hold
            ) {
                events += 1;
            }
            let elapsed = u32::try_from((now - start).as_nanos() / refill.as_nanos()).unwrap();
            assert!(events <= STATUS_EVENTS.capacity + elapsed);
        }
        assert_eq!(events, STATUS_EVENTS.capacity + 20);
    }

    /// F-799: a session reopened on the same connection shares its
    /// connection's meter, so it gets no fresh burst.
    #[test]
    fn a_reopened_session_gets_no_fresh_status_burst() {
        let peer = ZakuraPeerId::new(vec![7; 32]).unwrap();
        let meter = SharedBucket::new(STATUS_EVENTS);
        let start = Instant::now();
        let mut first = PeerState::new(peer.clone(), 1, meter.clone());
        for height in 1..=STATUS_EVENTS.capacity {
            let output = first.on_input(PeerInput::Status(status_at(height)), start);
            assert_eq!(forwarded_height(output), height);
        }
        let mut reopened = PeerState::new(peer, 2, meter);
        assert!(matches!(
            reopened.on_input(PeerInput::Status(status_at(10)), start),
            PeerOutput::Hold
        ));
        assert_eq!(
            reopened.status_due_at(),
            Some(start + STATUS_EVENTS.refill_interval)
        );
    }

    /// A sender at the row's send interval never waits.
    #[test]
    fn a_conformant_status_sender_is_never_held() {
        let peer = ZakuraPeerId::new(vec![7; 32]).unwrap();
        let mut state = PeerState::new(peer, 1, SharedBucket::new(STATUS_EVENTS));
        let start = Instant::now();
        for second in 0..100 {
            let now = start + STATUS_CADENCE.send_interval * second;
            let output = state.on_input(PeerInput::Status(status_at(second + 1)), now);
            assert_eq!(forwarded_height(output), second + 1);
        }
    }
}
