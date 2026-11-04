//! The independent model of one session's writer, shared by its proptest and
//! its cargo-fuzz target. The model restates the writer's documented steps and
//! never calls the code it checks.

use super::super::{
    protocol::GetServices,
    requester::{AdmittedAnswer, Asked, QueryKind, QueryState},
    wire::SEND_INTERVAL,
};
use super::state::{
    Close, CloseReason, InitialWait, Ownership, PeerInput, PeerOutput, PeerState, PeerStep,
    SessionCharge, DISCOVERY_ANSWER_WRITE_DEADLINE, DISCOVERY_INITIAL_EXCHANGE_TIMEOUT,
};

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

pub(super) fn then(next: PeerOutput) -> PeerStep {
    PeerStep { charge: None, next }
}

pub(super) fn charged(charge: SessionCharge, next: PeerOutput) -> PeerStep {
    PeerStep {
        charge: Some(charge),
        next,
    }
}

pub(super) const WAIT_INITIAL: PeerOutput = PeerOutput::WaitInitial {
    timeout: DISCOVERY_INITIAL_EXCHANGE_TIMEOUT,
};

pub(super) const REFRESH: PeerOutput = PeerOutput::Refresh {
    interval: SEND_INTERVAL,
};

pub(super) const WAIT_FOR_ANSWERS: PeerOutput = PeerOutput::WaitForAnswers {
    deadline: DISCOVERY_ANSWER_WRITE_DEADLINE,
};

pub(super) const END_OPEN: PeerOutput = PeerOutput::End { close: None };

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

/// The writer's documented steps, restated as the original sequential code
/// would run them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Line {
    /// `exchange()`; a failure ends the initial exchange.
    Exchange,
    /// The select over cancellation, completion, and the timeout.
    Wait,
    /// `is_current_session` after the wait.
    Current {
        completed: bool,
    },
    /// `peer_has_other_service_owner`, then `is_current_session` when none.
    Owners,
    /// `refresh_after_interval()` while another service owns the connection.
    Refresh,
    /// The select over cancellation, written answers, and the deadline.
    Answers,
    /// `mark_short_lived_exchange`.
    ShortLived,
    /// The last owner check, right before the close.
    ClosingOwners,
    Done,
}

/// The independent model of one session.
#[derive(Debug)]
struct Model {
    hello: bool,
    peers: bool,
    services: bool,
    line: Line,
    /// `Some(timed_out)` once the initial exchange continues past its wait.
    continued: Option<bool>,
}

impl Model {
    fn complete(&self) -> bool {
        self.hello && self.peers && self.services
    }

    /// The legal outcome that `choice` selects for the pending line. The
    /// wait reports completion only once the exchange is complete.
    fn outcome(&self, choice: u8) -> PeerInput {
        match self.line {
            Line::Exchange => PeerInput::Exchanged {
                sent: !choice.is_multiple_of(4),
            },
            Line::Wait => PeerInput::InitialWaitEnded(match choice % 3 {
                0 => InitialWait::Cancelled,
                2 if self.complete() => InitialWait::Complete,
                _ => InitialWait::TimedOut,
            }),
            Line::Current { .. } => PeerInput::CurrentSession {
                current: !choice.is_multiple_of(4),
            },
            Line::Owners | Line::ClosingOwners => PeerInput::Owners(match choice % 3 {
                0 => Ownership::Shared,
                1 => Ownership::Superseded,
                _ => Ownership::Sole,
            }),
            Line::Refresh => PeerInput::Refreshed {
                sent: !choice.is_multiple_of(4),
            },
            Line::Answers => PeerInput::AnswersSettled,
            Line::ShortLived => PeerInput::ShortLivedMarked,
            Line::Done => unreachable!("the generator stops at the end"),
        }
    }

    /// The step the original code takes after `input`.
    fn after(&mut self, input: PeerInput) -> PeerStep {
        let (line, step) = match (self.line, input) {
            (Line::Exchange, PeerInput::Exchanged { sent }) => {
                if sent {
                    (Line::Wait, then(WAIT_INITIAL))
                } else {
                    (Line::Done, charged(SessionCharge::Fail, END_OPEN))
                }
            }
            (Line::Wait, PeerInput::InitialWaitEnded(wait)) => match wait {
                InitialWait::Cancelled => (Line::Done, charged(SessionCharge::Fail, END_OPEN)),
                wait => (
                    Line::Current {
                        completed: wait == InitialWait::Complete,
                    },
                    then(PeerOutput::CheckCurrentSession),
                ),
            },
            (Line::Current { completed }, PeerInput::CurrentSession { current }) => {
                if !current {
                    (Line::Done, charged(SessionCharge::Fail, END_OPEN))
                } else {
                    self.continued = Some(!completed);
                    let charge = completed.then_some(SessionCharge::Succeed);
                    (
                        Line::Owners,
                        PeerStep {
                            charge,
                            next: PeerOutput::CheckSoleOwner,
                        },
                    )
                }
            }
            (Line::Owners, PeerInput::Owners(owners)) => {
                let other_owner = owners == Ownership::Shared;
                let current = owners == Ownership::Sole;
                if other_owner {
                    (Line::Refresh, then(REFRESH))
                } else if current {
                    (Line::Answers, then(WAIT_FOR_ANSWERS))
                } else {
                    (Line::Done, self.settle_open())
                }
            }
            (Line::Refresh, PeerInput::Refreshed { sent }) => {
                if sent {
                    (Line::Owners, then(PeerOutput::CheckSoleOwner))
                } else {
                    (Line::Done, self.settle_open())
                }
            }
            (Line::Answers, PeerInput::AnswersSettled) => {
                (Line::ShortLived, then(PeerOutput::MarkShortLived))
            }
            (Line::ShortLived, PeerInput::ShortLivedMarked) => {
                (Line::ClosingOwners, then(PeerOutput::CheckSoleOwner))
            }
            (Line::ClosingOwners, PeerInput::Owners(owners)) => match owners {
                Ownership::Shared => (Line::Refresh, then(REFRESH)),
                Ownership::Superseded => (Line::Done, self.settle_open()),
                Ownership::Sole => {
                    let reason = if self.complete() {
                        CloseReason::ExchangeComplete
                    } else {
                        CloseReason::ExchangeTimedOut
                    };
                    let close = Close { reason };
                    (Line::Done, then(PeerOutput::End { close: Some(close) }))
                }
            },
            (line, input) => unreachable!("the model answered {line:?} with {input:?}"),
        };
        self.line = line;
        step
    }

    /// The `else if initial == TimedOut` branch of a session that keeps its
    /// connection.
    fn settle_open(&self) -> PeerStep {
        let charge = match self.continued {
            Some(true) if self.complete() => Some(SessionCharge::Succeed),
            Some(true) => Some(SessionCharge::Fail),
            _ => None,
        };
        PeerStep {
            charge,
            next: END_OPEN,
        }
    }
}

const PROPERTY: &str = "discovery/session-policy";

/// Applicable transitions in a decoder-v1 discovery session history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct DiscoverySessionReplaySummary {
    /// IO outcomes checked against the model.
    pub transitions: usize,
    /// Session success or failure charges.
    pub charges: usize,
    /// Ownership checks after answer settlement.
    pub closing_owner_checks: usize,
    /// Connection close requests after the required checks.
    pub closes: usize,
}

/// Replay at most 512 bytes through the discovery peer's existing model.
///
/// Low bits select Hello, Peers, Services or an IO outcome. High bits select the
/// legal outcome for the current step. Model disagreements name the property ID.
pub fn replay_discovery_session_bytes(data: &[u8]) -> DiscoverySessionReplaySummary {
    let events = data.iter().take(512).map(|byte| match byte & 3 {
        0 => Event::Hello,
        1 => Event::Answer(QueryKind::Peers),
        2 => Event::Answer(QueryKind::Services),
        _ => Event::Outcome(byte >> 2),
    });
    run(events, |_, _| {})
}

pub(super) fn run(
    events: impl IntoIterator<Item = Event>,
    mut after_transition: impl FnMut(PeerInput, &mut PeerStep),
) -> DiscoverySessionReplaySummary {
    let mut state = PeerState::default();
    let mut model = Model {
        hello: false,
        peers: false,
        services: false,
        line: Line::Exchange,
        continued: None,
    };
    assert_eq!(
        state.start(),
        then(PeerOutput::Exchange),
        "{PROPERTY}: initial output"
    );
    let mut summary = DiscoverySessionReplaySummary::default();
    let mut sole_then_settled = false;
    let mut replaced = false;
    for event in events {
        match event {
            Event::Hello => {
                state.record_hello();
                model.hello = true;
            }
            Event::Answer(kind) => {
                state.record_answer(&admitted(kind));
                match kind {
                    QueryKind::Peers => model.peers = true,
                    QueryKind::Services => model.services = true,
                }
            }
            Event::Outcome(choice) => {
                let input = model.outcome(choice);
                match input {
                    PeerInput::CurrentSession { current: false }
                    | PeerInput::Owners(Ownership::Superseded) => replaced = true,
                    PeerInput::Owners(Ownership::Shared) => sole_then_settled = false,
                    PeerInput::AnswersSettled => {
                        sole_then_settled = true;
                        summary.closing_owner_checks += 1;
                    }
                    _ => {}
                }
                let expected = model.after(input);
                let mut step = state.on_input(input);
                after_transition(input, &mut step);
                assert_eq!(step, expected, "{PROPERTY}: next operation");
                summary.transitions += 1;
                match step.charge {
                    Some(SessionCharge::Succeed | SessionCharge::Fail) => summary.charges += 1,
                    Some(SessionCharge::Retire) => {
                        panic!("{PROPERTY}: only IO's close retires the record")
                    }
                    None => {}
                }
                if let PeerOutput::End { close: Some(_) } = step.next {
                    assert!(
                        sole_then_settled,
                        "{PROPERTY}: answers settled before close"
                    );
                    // IO closes with no wait after this check, so a sibling
                    // that gains the connection is never closed out.
                    assert_eq!(
                        input,
                        PeerInput::Owners(Ownership::Sole),
                        "{PROPERTY}: the owner check directly precedes a close"
                    );
                    assert!(!replaced, "{PROPERTY}: replacement forbids close");
                    assert_eq!(step.charge, None, "{PROPERTY}: a close charges at IO's end");
                    summary.closes += 1;
                }
                assert!(summary.charges <= 1, "{PROPERTY}: one session charge");
                if let PeerOutput::End { .. } = step.next {
                    break;
                }
            }
        }
        assert_eq!(
            state.is_complete(),
            model.complete(),
            "{PROPERTY}: exchange completion"
        );
    }
    summary
}
