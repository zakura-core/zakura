//! The `discovery_session` machine target: decoder-v1 histories through one
//! session's [`PeerState`], shared by its proptest and its cargo-fuzz target.
//!
//! [`replay_discovery_session_bytes`] drives the production state as
//! [`PeerIO`](super::PeerIO) does (see
//! [`machine_test`](crate::zakura::machine_test)): it answers each output with
//! that output's own input. It checks the state's invariants after every
//! input, and compares each step only with facts that follow from the inputs
//! it chose: which parts of the exchange it delivered, how the initial wait
//! ended, and which ownership checks preceded a close. It never re-derives the
//! session's next step. Named scenarios in `tests.rs` pin the rules.

use super::super::{
    protocol::GetServices,
    requester::{AdmittedAnswer, Asked, QueryKind, QueryState},
};
use super::state::{
    CloseReason, InitialWait, Ownership, PeerInput, PeerOutput, PeerState, SessionCharge,
};
use crate::zakura::machine_test::MAX_HISTORY_BYTES;

/// An answer to a `kind` query, minted by claiming a fresh reservation.
pub(super) fn admitted(kind: QueryKind) -> AdmittedAnswer {
    let mut queries = QueryState::default();
    let asked = match kind {
        QueryKind::Peers => Asked::Peers { limit: 1 },
        QueryKind::Services => Asked::Services(GetServices {
            wanted_services: Vec::new(),
        }),
    };
    queries
        .reserve(asked)
        .expect("a fresh state has no live query");
    queries
        .claim(kind, 0)
        .expect("the query is live")
        .expect("an empty answer fits any query")
}

/// One step of a generated session.
#[derive(Clone, Debug)]
pub(super) enum Event {
    /// The reader records the peer's `Hello`.
    Hello,
    /// The reader records an imported answer of this kind.
    Answer(QueryKind),
    /// IO answers the pending output with the outcome that `choice` selects.
    Outcome(u8),
}

/// The input IO may answer `pending` with, as `choice` selects it. The wait
/// reports completion only once the exchange is `complete`.
fn outcome(pending: PeerOutput, choice: u8, complete: bool) -> PeerInput {
    match pending {
        PeerOutput::Exchange => PeerInput::Exchanged {
            sent: !choice.is_multiple_of(4),
        },
        PeerOutput::WaitInitial { .. } => PeerInput::InitialWaitEnded(match choice % 3 {
            0 => InitialWait::Cancelled,
            2 if complete => InitialWait::Complete,
            _ => InitialWait::TimedOut,
        }),
        PeerOutput::CheckCurrentSession => PeerInput::CurrentSession {
            current: !choice.is_multiple_of(4),
        },
        PeerOutput::CheckSoleOwner => PeerInput::Owners(match choice % 3 {
            0 => Ownership::Shared,
            1 => Ownership::Superseded,
            _ => Ownership::Sole,
        }),
        PeerOutput::Refresh { .. } => PeerInput::Refreshed {
            sent: !choice.is_multiple_of(4),
        },
        PeerOutput::WaitForAnswers { .. } => PeerInput::AnswersSettled,
        PeerOutput::MarkShortLived => PeerInput::ShortLivedMarked,
        PeerOutput::End { .. } => unreachable!("the replay stops at the end"),
    }
}

/// Whether `input` reports that the session is cancelled or no longer current.
fn session_gone(input: PeerInput) -> bool {
    matches!(
        input,
        PeerInput::Exchanged { sent: false }
            | PeerInput::InitialWaitEnded(InitialWait::Cancelled)
            | PeerInput::CurrentSession { current: false }
            | PeerInput::Owners(Ownership::Superseded)
            | PeerInput::Refreshed { sent: false }
    )
}

const PROPERTY: &str = "discovery/session-policy";

/// Applicable transitions in a decoder-v1 discovery session history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct DiscoverySessionReplaySummary {
    /// IO outcomes applied to the state.
    pub transitions: usize,
    /// Session success or failure charges.
    pub charges: usize,
    /// Ownership checks after answer settlement.
    pub closing_owner_checks: usize,
    /// Connection close requests after the required checks.
    pub closes: usize,
}

/// Replay at most [`MAX_HISTORY_BYTES`] bytes through [`PeerState`].
///
/// Decoder v1. Each byte's low bits select `Hello`, `Peers`, `Services` or an
/// IO outcome. Its high bits select the legal outcome for the pending output.
/// A broken property panics with `discovery/session-policy`.
pub fn replay_discovery_session_bytes(data: &[u8]) -> DiscoverySessionReplaySummary {
    let events = data
        .iter()
        .take(MAX_HISTORY_BYTES)
        .map(|byte| match byte & 3 {
            0 => Event::Hello,
            1 => Event::Answer(QueryKind::Peers),
            2 => Event::Answer(QueryKind::Services),
            _ => Event::Outcome(byte >> 2),
        });
    run(events, |_, _| {})
}

/// Drives `events` through a fresh [`PeerState`], and calls
/// `after_transition` with each input and the step it returned, so a test can
/// plant a fault.
pub(super) fn run(
    events: impl IntoIterator<Item = Event>,
    mut after_transition: impl FnMut(PeerInput, &mut super::state::PeerStep),
) -> DiscoverySessionReplaySummary {
    let mut state = PeerState::default();
    let mut pending = state.start().next;
    assert_eq!(pending, PeerOutput::Exchange, "{PROPERTY}: initial output");
    // What the test delivered: the `Hello`, the `Peers` and the `Services`.
    let (mut hello, mut peers, mut services) = (false, false, false);
    let mut summary = DiscoverySessionReplaySummary::default();
    // Whether the initial wait reported a completed exchange.
    let mut wait_completed = false;
    let mut sole_then_settled = false;
    let mut replaced = false;
    for event in events {
        match event {
            Event::Hello => {
                state.record_hello();
                hello = true;
            }
            Event::Answer(kind) => {
                state.record_answer(&admitted(kind));
                match kind {
                    QueryKind::Peers => peers = true,
                    QueryKind::Services => services = true,
                }
            }
            Event::Outcome(choice) => {
                let complete = hello && peers && services;
                let input = outcome(pending, choice, complete);
                match input {
                    PeerInput::InitialWaitEnded(InitialWait::Complete) => wait_completed = true,
                    PeerInput::CurrentSession { current: false }
                    | PeerInput::Owners(Ownership::Superseded) => replaced = true,
                    PeerInput::Owners(Ownership::Shared) => sole_then_settled = false,
                    PeerInput::AnswersSettled => {
                        sole_then_settled = true;
                        summary.closing_owner_checks += 1;
                    }
                    _ => {}
                }
                let mut step = state.on_input(input);
                after_transition(input, &mut step);
                state.assert_invariants();
                summary.transitions += 1;
                match step.charge {
                    Some(SessionCharge::Succeed) => {
                        assert!(complete, "{PROPERTY}: success without a complete exchange");
                        summary.charges += 1;
                    }
                    Some(SessionCharge::Fail) => {
                        assert!(
                            matches!(step.next, PeerOutput::End { close: None }),
                            "{PROPERTY}: a failure charge ends the session"
                        );
                        summary.charges += 1;
                    }
                    Some(SessionCharge::Retire) => {
                        panic!("{PROPERTY}: only IO's close retires the record")
                    }
                    None => {}
                }
                assert!(summary.charges <= 1, "{PROPERTY}: one session charge");
                if session_gone(input) {
                    assert_eq!(
                        step.next,
                        PeerOutput::End { close: None },
                        "{PROPERTY}: a session that is gone ends without a close"
                    );
                }
                match step.next {
                    PeerOutput::End { close: Some(close) } => {
                        assert!(
                            sole_then_settled,
                            "{PROPERTY}: answers settled before close"
                        );
                        // IO closes with no wait after this check, so a
                        // sibling that gains the connection is never closed
                        // out.
                        assert_eq!(
                            input,
                            PeerInput::Owners(Ownership::Sole),
                            "{PROPERTY}: the owner check directly precedes a close"
                        );
                        assert!(!replaced, "{PROPERTY}: replacement forbids close");
                        assert_eq!(step.charge, None, "{PROPERTY}: a close charges at IO's end");
                        // IO retires a closed session, so only a completed
                        // initial wait leaves its success charge.
                        assert_eq!(
                            summary.charges,
                            usize::from(wait_completed),
                            "{PROPERTY}: a closed session's charges"
                        );
                        assert_eq!(
                            close.reason == CloseReason::ExchangeComplete,
                            complete,
                            "{PROPERTY}: close reason"
                        );
                        summary.closes += 1;
                        break;
                    }
                    PeerOutput::End { close: None } => {
                        assert_eq!(
                            summary.charges, 1,
                            "{PROPERTY}: a session that keeps its connection is charged once"
                        );
                        break;
                    }
                    next => pending = next,
                }
            }
        }
        assert_eq!(
            state.is_complete(),
            hello && peers && services,
            "{PROPERTY}: exchange completion"
        );
    }
    summary
}
