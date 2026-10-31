//! Generated histories and scenarios for the publisher model.

use proptest::prelude::*;

use super::*;
use crate::zakura::header_sync::v9::test_support::OUTCOME_CODES;

fn sequence() -> impl Strategy<Value = Sequence> {
    prop_oneof![4 => Just(Sequence::Next), 1 => Just(Sequence::Repeat)]
}

fn stale() -> impl Strategy<Value = Option<usize>> {
    prop::option::weighted(0.2, any::<usize>())
}

fn answer() -> impl Strategy<Value = Answer> {
    let own = OUTCOME_CODES
        .into_iter()
        .filter(|code| *code != HeadersOutcomeCode::SubscriptionClosed)
        .collect::<Vec<_>>();
    prop_oneof![
        prop::sample::select(own).prop_map(Answer::End),
        Just(Answer::Local),
        Just(Answer::Capacity),
        Just(Answer::Retry),
        Just(Answer::Extends),
        Just(Answer::ReachedAtOpen),
    ]
}

/// Steps that stop the publisher are rare, so most histories run long.
fn step() -> impl Strategy<Value = Step> {
    let id = 1..=IDS;
    prop_oneof![
        12 => (id.clone(), any::<bool>(), 1..=5u32, 1..=6u32, 1..=6u32).prop_map(
            |(id, at_target, target, objects, units)| Step::Open {
                id,
                at_target,
                target,
                objects,
                units,
            }
        ),
        12 => (id.clone(), sequence(), 0..8usize, 0..=4u32, 0..=4u32).prop_map(
            |(id, sequence, ack, objects, units)| Step::Grant {
                id,
                sequence,
                ack,
                objects,
                units,
            }
        ),
        12 => (id, sequence(), 0..8usize)
            .prop_map(|(id, sequence, ack)| Step::Close { id, sequence, ack }),
        16 => Just(Step::NextJob),
        10 => (1..=3u32, 0..=2u64, stale())
            .prop_map(|(headers, shave, stale)| Step::Page { headers, shave, stale }),
        10 => (answer(), stale()).prop_map(|(answer, stale)| Step::Finish { answer, stale }),
        12 => Just(Step::EndDue),
        12 => Just(Step::NextTerminal),
        12 => prop::bool::weighted(0.9).prop_map(Step::TerminalWritten),
        1 => Just(Step::Cancel),
        1 => Just(Step::Closed),
        10 => prop::option::weighted(0.9, (0..=2u8, 0..=8u32))
            .prop_map(|tip| Step::Select(tip.map(|(branch, height)| Tip { branch, height }))),
        4 => any::<usize>().prop_map(Step::Release),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(std::env::var("HEADER_SYNC_PROPTEST_CASES").ok().and_then(|cases| cases.parse().ok()).unwrap_or(256)))]

    /// Generated publisher histories keep page position contiguous, spend
    /// only granted credit, end each subscription once in its window, and
    /// let no stale job result change anything.
    #[test]
    fn publisher_state_matches_the_model(steps in prop::collection::vec(step(), 1..64)) {
        run(steps, |_, _| {});
    }
}

/// The worked scenario: a job for subscription A, A ends and a new A opens,
/// and the old job's page arrives under a changed selected chain. State sends
/// no old page, spends none of the new A's credit, and keeps serving it.
#[test]
fn a_stale_page_after_replacement_spends_no_replacement_credit() {
    let a = 1;
    let mut machine = Machine::new();
    machine.run(&Step::Open {
        id: a,
        at_target: false,
        target: 5,
        objects: 6,
        units: 6,
    });
    machine.run(&Step::NextJob);
    let old = machine
        .model
        .in_progress
        .clone()
        .expect("A has a page to read");
    let close = |id| Step::Close {
        id,
        sequence: Sequence::Next,
        ack: 0,
    };
    machine.run(&close(a));
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    machine.run(&Step::TerminalWritten(true));
    // Another subscription's `Open` clears A's tombstone, so A can open again.
    machine.run(&Step::Open {
        id: 2,
        at_target: false,
        target: 5,
        objects: 1,
        units: 1,
    });
    machine.run(&close(2));
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    machine.run(&Step::TerminalWritten(true));
    machine.run(&Step::Open {
        id: a,
        at_target: false,
        target: 5,
        objects: 6,
        units: 6,
    });
    machine.run(&Step::Select(Some(Tip {
        branch: 1,
        height: 7,
    })));
    let page = Page {
        parent: frontier(0, 0),
        last: frontier(0, 3),
        target: frontier(0, 5),
        count: 3,
        reaches_target: false,
        payload_len: usize::try_from(fixed() + 3 * entry()).expect("small page"),
    };
    assert!(
        !machine.owner.state.commit(old.job.key, &page),
        "no old page is sent"
    );
    let latest = machine.selected();
    machine.owner.state.finish(
        old.job.key,
        Output::Stall(Stall::Local, "test"),
        latest.as_ref(),
    );
    assert_eq!(
        machine
            .owner
            .state
            .publications
            .credit(&a)
            .map(|credit| credit.consumed()),
        Some(Totals::default()),
        "the new A spent no credit"
    );
    // The old result stalled nothing: the new A gets its first page job.
    machine.run(&Step::NextJob);
    let new = machine
        .model
        .in_progress
        .as_ref()
        .expect("the new A has a page to read");
    assert_eq!((new.job.id, new.job.parent), (a, None));
    assert_ne!(new.job.key, old.job.key);
}

/// A capacity stall waits for its own signal: releasing an older one leaves
/// it stalled, and releasing its own lets the next job start.
#[test]
fn a_capacity_stall_waits_for_its_own_signal() {
    let mut machine = Machine::new();
    machine.run(&Step::Open {
        id: 1,
        at_target: false,
        target: 5,
        objects: 6,
        units: 6,
    });
    for _ in 0..2 {
        // A selected-chain change clears the previous stall.
        machine.run(&Step::Select(None));
        machine.run(&Step::NextJob);
        assert!(machine.model.in_progress.is_some(), "the path has a page");
        machine.run(&Step::Finish {
            answer: Answer::Capacity,
            stale: None,
        });
    }
    let [older, own] = <[_; 2]>::try_from(
        machine
            .signals
            .iter()
            .map(|(wait, _)| *wait)
            .collect::<Vec<_>>(),
    )
    .expect("two refusals");
    machine.owner.state.capacity_released(&[older]);
    machine.run(&Step::NextJob);
    assert!(machine.model.in_progress.is_none(), "still stalled");
    machine.owner.state.capacity_released(&[own]);
    machine.model.subs.get_mut(&1).expect("live").stall = None;
    machine.run(&Step::NextJob);
    assert!(
        machine.model.in_progress.is_some(),
        "the release lets it retry"
    );
}

/// While a terminal outcome waits for the stream, another subscription's job
/// in progress still commits its page. No new job starts until the write
/// finishes.
#[test]
fn another_subscriptions_page_commits_while_a_terminal_waits() {
    let mut machine = Machine::new();
    for id in [1, 2] {
        machine.run(&Step::Open {
            id,
            at_target: false,
            target: 5,
            objects: 6,
            units: 6,
        });
    }
    machine.run(&Step::NextJob);
    let running = machine
        .model
        .in_progress
        .clone()
        .expect("subscription 1 has a page to read");
    assert_eq!(running.job.id, 1);
    machine.run(&Step::Close {
        id: 2,
        sequence: Sequence::Next,
        ack: 0,
    });
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    assert!(
        machine.writing.is_some(),
        "subscription 2's outcome is written"
    );
    machine.run(&Step::Page {
        headers: 3,
        shave: 0,
        stale: None,
    });
    assert_eq!(machine.model.subs[&1].pages, 1, "the page committed");
    machine.run(&Step::NextJob);
    assert!(machine.model.in_progress.is_none(), "the write blocks jobs");
    machine.run(&Step::TerminalWritten(true));
    machine.run(&Step::NextJob);
    assert!(
        machine.model.in_progress.is_some(),
        "the next job starts after the write"
    );
}

/// Two subscriptions that close together, with the first outcome taken.
fn two_closed_one_writing() -> Machine {
    let mut machine = Machine::new();
    for id in [1, 2] {
        machine.run(&Step::Open {
            id,
            at_target: false,
            target: 5,
            objects: 6,
            units: 6,
        });
        machine.run(&Step::Close {
            id,
            sequence: Sequence::Next,
            ack: 0,
        });
    }
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    assert_eq!(machine.model.queued.len(), 1, "one outcome still waits");
    // The second outcome waits for the first write.
    machine.run(&Step::NextTerminal);
    assert_eq!(machine.model.queued.len(), 1, "one write at a time");
    machine
}

/// Cancellation during a terminal write stops the publisher: the queued
/// outcome is never offered, and no job starts.
#[test]
fn cancellation_during_a_terminal_write_stops_the_publisher() {
    let mut machine = two_closed_one_writing();
    machine.run(&Step::Cancel);
    assert_eq!(machine.model.exit, Some(Exit::Cancelled));
}

/// A stream that refuses a terminal outcome stops the publisher: the queued
/// outcome is never offered, and no job starts.
#[test]
fn a_refused_terminal_write_stops_the_publisher() {
    let mut machine = two_closed_one_writing();
    machine.run(&Step::TerminalWritten(false));
    assert_eq!(machine.model.exit, Some(Exit::Closed));
}

/// A queued terminal outcome keeps its slot, its permit keeps it through the
/// write, and only the finished write frees it.
#[test]
fn a_terminal_holds_its_slot_until_its_write_finishes() {
    let mut machine = Machine::new();
    machine.run(&Step::Open {
        id: 1,
        at_target: false,
        target: 5,
        objects: 6,
        units: 6,
    });
    assert_eq!(machine.owner.slots.held(), 1);
    machine.run(&Step::Close {
        id: 1,
        sequence: Sequence::Next,
        ack: 0,
    });
    machine.run(&Step::EndDue);
    assert_eq!(machine.owner.state.publications.keys().count(), 0);
    assert_eq!(machine.owner.slots.held(), 1, "the queued outcome holds it");
    machine.run(&Step::NextTerminal);
    assert_eq!(machine.owner.slots.held(), 1, "the permit holds it");
    machine.run(&Step::TerminalWritten(true));
    assert_eq!(machine.owner.slots.held(), 0, "the write freed it");
}

/// A write report under another slot's identity changes nothing, even a
/// refusal: only the outcome being written can stop the publisher.
#[test]
fn a_terminal_write_report_settles_only_its_own_slot() {
    let mut machine = two_closed_one_writing();
    let first = machine.writing.as_ref().expect("one is being written").slot;
    machine.run(&Step::TerminalWritten(true));
    machine.run(&Step::NextTerminal);
    let second = machine.writing.as_ref().expect("the next is written").slot;
    assert_ne!(first, second);
    assert_eq!(
        machine.owner.state.terminal_written(first, false),
        ControlFlow::Continue(()),
        "the stale report is ignored"
    );
    assert!(
        machine.owner.state.next_terminal().is_none(),
        "the second outcome is still being written"
    );
    assert_eq!(
        machine.owner.state.terminal_written(second, false),
        ControlFlow::Break(Exit::Closed)
    );
}

/// State names a capacity release by value. The owner reuses one wait per
/// signal, forgets a signal once no stall waits on it, and a stale job's
/// refusal leaves no stall and no signal behind.
#[test]
fn the_owner_holds_a_capacity_signal_only_while_a_stall_waits() {
    let mut machine = Machine::new();
    machine.run(&Step::Open {
        id: 1,
        at_target: false,
        target: 5,
        objects: 6,
        units: 6,
    });
    machine.run(&Step::NextJob);
    let stale = machine.model.in_progress.clone().expect("a page to read");
    machine.run(&Step::Finish {
        answer: Answer::Retry,
        stale: None,
    });
    machine.run(&Step::NextJob);
    let job = machine.model.in_progress.clone().expect("a page to read");
    let signal = ServingCapacitySignal::default();
    let first = machine.owner.waits.register(signal.clone());
    assert_eq!(machine.owner.waits.register(signal.clone()), first);

    machine
        .owner
        .capacity_busy(stale.job.key, signal.clone(), None);
    assert!(
        machine.owner.capacity_signals().is_empty(),
        "a stale refusal"
    );

    machine
        .owner
        .capacity_busy(job.job.key, signal.clone(), None);
    assert_eq!(machine.owner.capacity_signals(), [signal.clone()]);
    assert_eq!(machine.owner.state.capacity_waits().len(), 1);

    signal.release();
    machine.owner.capacity_released();
    assert!(machine.owner.state.capacity_waits().is_empty());
    assert!(machine.owner.capacity_signals().is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn publisher_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_header_publisher_bytes(&bytes);
    }
}

const WITNESS: &[u8] = &[
    0, 0, 4, 35, 10, 0, 0, 0, 15, 0, 0, 0, 8, 0, 0, 0, 23, 0, 0, 0, 24, 0, 0, 0, 25, 0, 0, 0, 15,
    0, 0, 128,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "witnesses print applicability counters"
)]
fn publisher_witness_exercises_credit_terminal_and_stale_page() {
    let summary = replay_header_publisher_bytes(WITNESS);
    assert_eq!(
        summary,
        HeaderPublisherReplaySummary {
            transitions: 8,
            jobs: 1,
            pages: 1,
            headers: 1,
            bytes: 263,
            stale_results: 1,
            terminals: 1
        }
    );
    assert_eq!(
        replay_header_publisher_bytes(&[]),
        HeaderPublisherReplaySummary::default()
    );
    println!("tier=Machine target=header_publisher decoder=1 {summary:?}");
}

#[test]
#[should_panic(expected = "header/publisher-policy")]
fn publisher_witness_rejects_lost_credit() {
    run(decode(WITNESS), |machine, step| {
        if matches!(step, Step::Open { .. }) {
            machine.owner.state.publications.end(&1);
        }
    });
}

#[test]
#[should_panic(expected = "header/publisher-policy")]
fn publisher_witness_rejects_lost_job() {
    run(decode(WITNESS), |machine, step| {
        if matches!(step, Step::Open { .. }) {
            machine.owner.state.next_job::<HB>(None, page_limits());
        }
    });
}

#[test]
#[should_panic(expected = "header/publisher-policy")]
fn publisher_witness_rejects_lost_terminal() {
    run(decode(WITNESS), |machine, step| {
        if matches!(step, Step::EndDue) {
            machine.owner.state.next_terminal();
        }
    });
}
