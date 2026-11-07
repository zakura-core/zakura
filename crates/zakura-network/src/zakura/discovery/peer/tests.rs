//! Machine tests for one discovery session's [`PeerState`], checked against an
//! independent model.
//!
//! The model restates the writer's documented steps. It never calls the code
//! it checks. Answers come from a [`QueryState`] claim, the only way to mint
//! an [`AdmittedAnswer`].

use proptest::prelude::*;

use super::super::requester::{Asked, QueryKind, QueryState};
use super::model::*;
use super::state::{
    Close, CloseReason, InitialWait, Ownership, PeerInput, PeerOutput, PeerState, PeerStep,
    SessionCharge,
};

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
/// out. The session is charged when it leaves the connection open. When it
/// asks to close the connection, IO retires it at the close.
#[test]
fn a_missing_hello_times_out_the_initial_exchange() {
    for (owners, end) in [
        (
            Ownership::Superseded,
            charged(SessionCharge::Fail, END_OPEN),
        ),
        (
            Ownership::Sole,
            then(PeerOutput::End {
                close: Some(Close {
                    reason: CloseReason::ExchangeTimedOut,
                }),
            }),
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
            assert_eq!(
                state.on_input(PeerInput::ShortLivedMarked),
                then(PeerOutput::CheckSoleOwner)
            );
            assert_eq!(state.on_input(PeerInput::Owners(Ownership::Sole)), end);
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

/// An exchange that the close's answer wait finishes closes as complete, not
/// as timed out.
#[test]
fn an_answer_that_settles_during_the_close_wait_completes_the_exchange() {
    let mut state = waiting_session();
    assert_eq!(
        settle_initial(&mut state, InitialWait::TimedOut),
        then(PeerOutput::CheckSoleOwner)
    );
    assert_eq!(
        state.on_input(PeerInput::Owners(Ownership::Sole)),
        then(WAIT_FOR_ANSWERS)
    );
    complete(&mut state);
    assert_eq!(
        state.on_input(PeerInput::AnswersSettled),
        then(PeerOutput::MarkShortLived)
    );
    assert_eq!(
        state.on_input(PeerInput::ShortLivedMarked),
        then(PeerOutput::CheckSoleOwner)
    );
    assert_eq!(
        state.on_input(PeerInput::Owners(Ownership::Sole)),
        then(PeerOutput::End {
            close: Some(Close {
                reason: CloseReason::ExchangeComplete,
            }),
        })
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
        then(PeerOutput::CheckSoleOwner)
    );
    assert_eq!(
        state.on_input(PeerInput::Owners(Ownership::Sole)),
        then(PeerOutput::End {
            close: Some(Close {
                reason: CloseReason::ExchangeComplete,
            }),
        })
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

fn event() -> impl Strategy<Value = Event> {
    prop_oneof![
        1 => Just(Event::Hello),
        2 => proptest::sample::select(vec![QueryKind::Peers, QueryKind::Services])
            .prop_map(Event::Answer),
        5 => any::<u8>().prop_map(Event::Outcome),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn sessions_match_independent_model(events in proptest::collection::vec(event(), 1..64)) {
        run(events, |_, _| {});
    }

    #[test]
    fn discovery_session_bytes_match_the_same_model(bytes in proptest::collection::vec(any::<u8>(), 0..=512)) {
        replay_discovery_session_bytes(&bytes);
    }
}

const WITNESS: &[u8] = &[0, 1, 2, 7, 11, 7, 11, 3, 3, 11];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn discovery_session_witness_reaches_close_after_recheck() {
    let summary = replay_discovery_session_bytes(WITNESS);
    assert_eq!(
        summary,
        DiscoverySessionReplaySummary {
            transitions: 7,
            charges: 1,
            closing_owner_checks: 1,
            closes: 1
        }
    );
    assert_eq!(
        replay_discovery_session_bytes(&[]),
        DiscoverySessionReplaySummary::default()
    );
    println!("tier=Machine target=discovery_session decoder=1 {summary:?}");
}

fn closing_events() -> impl Iterator<Item = Event> {
    [
        Event::Hello,
        Event::Answer(QueryKind::Peers),
        Event::Answer(QueryKind::Services),
    ]
    .into_iter()
    .chain([1, 2, 1, 2, 0, 0, 2].into_iter().map(Event::Outcome))
}

#[test]
#[should_panic(expected = "discovery/session-policy")]
fn discovery_session_witness_rejects_skipping_the_owner_recheck() {
    run(closing_events(), |input, step| {
        if input == PeerInput::ShortLivedMarked {
            step.next = PeerOutput::End {
                close: Some(Close {
                    reason: CloseReason::ExchangeComplete,
                }),
            };
        }
    });
}

/// A sibling that gains the connection during the close's answer wait keeps
/// it: the last owner check finds it, and the session refreshes instead.
#[test]
fn a_sibling_found_at_the_last_owner_check_keeps_the_connection() {
    let mut state = waiting_session();
    complete(&mut state);
    settle_initial(&mut state, InitialWait::Complete);
    assert_eq!(
        state.on_input(PeerInput::Owners(Ownership::Sole)),
        then(WAIT_FOR_ANSWERS)
    );
    assert_eq!(
        state.on_input(PeerInput::AnswersSettled),
        then(PeerOutput::MarkShortLived)
    );
    assert_eq!(
        state.on_input(PeerInput::ShortLivedMarked),
        then(PeerOutput::CheckSoleOwner)
    );
    assert_eq!(
        state.on_input(PeerInput::Owners(Ownership::Shared)),
        then(REFRESH)
    );
}

#[test]
#[should_panic(expected = "discovery/session-policy")]
fn discovery_session_witness_rejects_losing_the_success_charge() {
    run(closing_events(), |_, step| {
        if step.charge == Some(SessionCharge::Succeed) {
            step.charge = None;
        }
    });
}

#[tokio::test]
async fn completion_wait_observes_progress_before_and_after_polling() {
    use super::SharedPeerState;
    use futures::{poll, FutureExt};

    for complete_before_poll in [false, true] {
        let state = SharedPeerState::default();
        let wait = state.wait_complete();
        zakura_rt::pin!(wait);
        if !complete_before_poll {
            assert!(poll!(wait.as_mut()).is_pending());
        }
        state.record_hello();
        state.record_answer(&admitted(QueryKind::Peers));
        state.record_answer(&admitted(QueryKind::Services));
        assert!(wait.now_or_never().is_some());
    }
}
