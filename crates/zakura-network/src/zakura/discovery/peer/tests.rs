//! Machine tests for one discovery session's [`PeerState`], checked against an
//! independent model.
//!
//! The model restates the writer's documented steps. It never calls the code
//! it checks. Answers come from a [`QueryState`] claim, the only way to mint
//! an [`AdmittedAnswer`].

use proptest::prelude::*;

use super::super::{
    protocol::GetServices,
    requester::{AdmittedAnswer, Asked, QueryKind, QueryState},
    wire::SEND_INTERVAL,
};
use super::state::{
    CloseReason, InitialWait, Ownership, PeerInput, PeerOutput, PeerState, PeerStep, SessionCharge,
    DISCOVERY_ANSWER_WRITE_DEADLINE, DISCOVERY_INITIAL_EXCHANGE_TIMEOUT,
};

/// An answer to a `kind` query, minted by claiming a fresh reservation.
fn admitted(kind: QueryKind) -> AdmittedAnswer {
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

fn then(next: PeerOutput) -> PeerStep {
    PeerStep { charge: None, next }
}

fn charged(charge: SessionCharge, next: PeerOutput) -> PeerStep {
    PeerStep {
        charge: Some(charge),
        next,
    }
}

const WAIT_INITIAL: PeerOutput = PeerOutput::WaitInitial {
    timeout: DISCOVERY_INITIAL_EXCHANGE_TIMEOUT,
};
const REFRESH: PeerOutput = PeerOutput::Refresh {
    interval: SEND_INTERVAL,
};
const WAIT_FOR_ANSWERS: PeerOutput = PeerOutput::WaitForAnswers {
    deadline: DISCOVERY_ANSWER_WRITE_DEADLINE,
};
const END_OPEN: PeerOutput = PeerOutput::End { close: None };

/// A session that sent its exchange and waits for its answers.
fn waiting_session() -> PeerState {
    let mut state = PeerState::default();
    assert_eq!(state.start(), then(PeerOutput::Exchange));
    assert_eq!(
        state.on_input(PeerInput::Exchanged { sent: true }),
        then(WAIT_INITIAL)
    );
    state
}

/// Records every part of the exchange.
fn complete(state: &mut PeerState) {
    state.record_hello();
    state.record_answer(&admitted(QueryKind::Peers));
    state.record_answer(&admitted(QueryKind::Services));
}

/// Ends the initial wait with `wait` on a current session.
fn settle_initial(state: &mut PeerState, wait: InitialWait) -> PeerStep {
    assert_eq!(
        state.on_input(PeerInput::InitialWaitEnded(wait)),
        then(PeerOutput::CheckCurrentSession)
    );
    state.on_input(PeerInput::CurrentSession { current: true })
}

/// Without a `Hello` the exchange never completes, so the initial wait times
/// out. The session is charged when it leaves the connection open, and
/// retired when it closes it.
#[test]
fn a_missing_hello_times_out_the_initial_exchange() {
    for (owners, end) in [
        (
            Ownership::Superseded,
            charged(SessionCharge::Fail, END_OPEN),
        ),
        (
            Ownership::Sole,
            charged(
                SessionCharge::Retire,
                PeerOutput::End {
                    close: Some(CloseReason::ExchangeTimedOut),
                },
            ),
        ),
    ] {
        let mut state = waiting_session();
        state.record_answer(&admitted(QueryKind::Peers));
        state.record_answer(&admitted(QueryKind::Services));
        assert!(!state.is_complete(), "the exchange needs the peer's Hello");

        assert_eq!(
            settle_initial(&mut state, InitialWait::TimedOut),
            then(PeerOutput::CheckSoleOwner)
        );
        let step = state.on_input(PeerInput::Owners(owners));
        if owners == Ownership::Sole {
            assert_eq!(step, then(WAIT_FOR_ANSWERS));
            assert_eq!(
                state.on_input(PeerInput::AnswersSettled),
                then(PeerOutput::MarkShortLived)
            );
            assert_eq!(state.on_input(PeerInput::ShortLivedMarked), end);
        } else {
            assert_eq!(step, end);
        }
    }
}

/// A second claim of one query mints nothing, and a second answer of one
/// kind adds no progress.
#[test]
fn duplicate_answers_count_once() {
    let mut queries = QueryState::default();
    queries
        .reserve(Asked::Peers { limit: 1 })
        .expect("a fresh state has no live query");
    let answer = queries
        .claim(QueryKind::Peers, 0)
        .expect("the query is live")
        .expect("an empty answer fits");
    assert!(
        queries.claim(QueryKind::Peers, 0).is_none(),
        "a claimed query admits no second answer"
    );

    let mut state = waiting_session();
    state.record_hello();
    state.record_answer(&answer);
    state.record_answer(&admitted(QueryKind::Peers));
    assert!(
        !state.is_complete(),
        "two Peers answers do not answer GetServices"
    );
    state.record_answer(&admitted(QueryKind::Services));
    assert!(state.is_complete());
}

/// While another admitted service owns the connection, the session refreshes
/// once per send interval and never closes the connection.
#[test]
fn another_admitted_service_keeps_the_connection_and_the_session_refreshes() {
    let mut state = waiting_session();
    complete(&mut state);
    assert_eq!(
        settle_initial(&mut state, InitialWait::Complete),
        charged(SessionCharge::Succeed, PeerOutput::CheckSoleOwner)
    );
    for _ in 0..3 {
        assert_eq!(
            state.on_input(PeerInput::Owners(Ownership::Shared)),
            then(REFRESH)
        );
        assert_eq!(
            state.on_input(PeerInput::Refreshed { sent: true }),
            then(PeerOutput::CheckSoleOwner)
        );
    }
    assert_eq!(
        state.on_input(PeerInput::Owners(Ownership::Shared)),
        then(REFRESH)
    );
    assert_eq!(
        state.on_input(PeerInput::Refreshed { sent: false }),
        then(END_OPEN),
        "a completed session that ends with its stream leaves the connection open"
    );
}

/// A timed-out session that completes during its refreshes resets the
/// failure count when it ends.
#[test]
fn a_late_completion_settles_a_timed_out_session_as_a_success() {
    let mut state = waiting_session();
    assert_eq!(
        settle_initial(&mut state, InitialWait::TimedOut),
        then(PeerOutput::CheckSoleOwner)
    );
    assert_eq!(
        state.on_input(PeerInput::Owners(Ownership::Shared)),
        then(REFRESH)
    );
    complete(&mut state);
    assert_eq!(
        state.on_input(PeerInput::Refreshed { sent: false }),
        charged(SessionCharge::Succeed, END_OPEN)
    );
}

/// A sole owner waits for its admitted answers before it closes. Progress
/// during that wait does not end the session.
#[test]
fn a_pending_answer_holds_the_close_until_it_settles() {
    let mut state = waiting_session();
    complete(&mut state);
    settle_initial(&mut state, InitialWait::Complete);
    assert_eq!(
        state.on_input(PeerInput::Owners(Ownership::Sole)),
        then(WAIT_FOR_ANSWERS)
    );
    state.record_hello();
    state.record_answer(&admitted(QueryKind::Services));
    assert_eq!(
        state.on_input(PeerInput::AnswersSettled),
        then(PeerOutput::MarkShortLived)
    );
    assert_eq!(
        state.on_input(PeerInput::ShortLivedMarked),
        charged(
            SessionCharge::Retire,
            PeerOutput::End {
                close: Some(CloseReason::ExchangeComplete),
            },
        )
    );
}

/// A replaced session neither succeeds nor closes the connection, even when
/// its exchange completed.
#[test]
fn a_replaced_session_cannot_complete_or_close() {
    let mut state = waiting_session();
    complete(&mut state);
    assert_eq!(
        state.on_input(PeerInput::InitialWaitEnded(InitialWait::Complete)),
        then(PeerOutput::CheckCurrentSession)
    );
    assert_eq!(
        state.on_input(PeerInput::CurrentSession { current: false }),
        charged(SessionCharge::Fail, END_OPEN)
    );

    // Replaced after its initial exchange, once no other service owns the
    // connection.
    let mut state = waiting_session();
    complete(&mut state);
    settle_initial(&mut state, InitialWait::Complete);
    assert_eq!(
        state.on_input(PeerInput::Owners(Ownership::Superseded)),
        then(END_OPEN)
    );
}

/// A session whose stream breaks before its exchange is charged once.
#[test]
fn a_broken_initial_exchange_is_charged() {
    let mut state = PeerState::default();
    state.start();
    assert_eq!(
        state.on_input(PeerInput::Exchanged { sent: false }),
        charged(SessionCharge::Fail, END_OPEN)
    );

    let mut state = waiting_session();
    complete(&mut state);
    assert_eq!(
        state.on_input(PeerInput::InitialWaitEnded(InitialWait::Cancelled)),
        charged(SessionCharge::Fail, END_OPEN)
    );
}

/// One step of a generated session.
#[derive(Clone, Debug)]
enum Event {
    /// The reader records the peer's `Hello`.
    Hello,
    /// The reader records an imported answer of this kind.
    Answer(QueryKind),
    /// IO answers the pending output with the outcome that `choice` selects.
    Outcome(u8),
}

fn event() -> impl Strategy<Value = Event> {
    prop_oneof![
        1 => Just(Event::Hello),
        2 => proptest::sample::select(vec![QueryKind::Peers, QueryKind::Services])
            .prop_map(Event::Answer),
        5 => any::<u8>().prop_map(Event::Outcome),
    ]
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
            Line::Owners => PeerInput::Owners(match choice % 3 {
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
                let reason = if self.continued == Some(true) {
                    CloseReason::ExchangeTimedOut
                } else {
                    CloseReason::ExchangeComplete
                };
                (
                    Line::Done,
                    charged(
                        SessionCharge::Retire,
                        PeerOutput::End {
                            close: Some(reason),
                        },
                    ),
                )
            }
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Every step matches the independent model. Across the session: the
    /// connection closes only after a sole-owner check and the answer wait,
    /// a close always retires the session's record, a replaced session never
    /// closes the connection, and at most one success or failure is charged.
    #[test]
    fn sessions_match_independent_model(events in proptest::collection::vec(event(), 1..64)) {
        let mut state = PeerState::default();
        let mut model = Model {
            hello: false,
            peers: false,
            services: false,
            line: Line::Exchange,
            continued: None,
        };
        prop_assert_eq!(state.start(), then(PeerOutput::Exchange));

        let mut counted = 0;
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
                        PeerInput::Owners(Ownership::Sole) => sole_then_settled = false,
                        PeerInput::AnswersSettled => sole_then_settled = true,
                        _ => {}
                    }
                    let expected = model.after(input);
                    let step = state.on_input(input);
                    prop_assert_eq!(step, expected);

                    match step.charge {
                        Some(SessionCharge::Succeed | SessionCharge::Fail) => counted += 1,
                        Some(SessionCharge::Retire) => prop_assert!(
                            matches!(step.next, PeerOutput::End { close: Some(_) }),
                            "only a close retires the record"
                        ),
                        None => {}
                    }
                    prop_assert!(counted <= 1, "a session is charged at most once");
                    if let PeerOutput::End { close } = step.next {
                        if close.is_some() {
                            prop_assert!(sole_then_settled, "a close follows the answer wait");
                            prop_assert!(!replaced, "a replaced session never closes");
                            prop_assert_eq!(step.charge, Some(SessionCharge::Retire));
                        }
                        break;
                    }
                }
            }
            prop_assert_eq!(state.is_complete(), model.complete());
        }
    }
}
