//! The publisher machine target's shared tests, and named scenarios that pin
//! each rule the target does not re-derive.

use super::super::state::{in_window, page_room};
use super::*;
use crate::zakura::{machine_test::machine_target_tests, regulation::Totals};

/// Run `steps` over a new machine, calling `after_step` after each and checking
/// the result.
fn run(steps: impl IntoIterator<Item = Step>, mut after_step: impl FnMut(&mut Machine, &Step)) {
    let mut machine = Machine::new();
    for step in steps {
        if machine.exit.is_some() {
            break;
        }
        machine.run(&step);
        after_step(&mut machine, &step);
        machine.check();
    }
}

fn open(id: u64, at_target: bool, target: u32, objects: u32, units: u32) -> Step {
    Step::Open {
        id,
        at_target,
        target,
        objects,
        units,
    }
}

/// A subscription on the path to height 5 with credit for six headers.
fn open_path(id: u64) -> Step {
    open(id, false, 5, 6, 6)
}

fn grant(id: u64, objects: u32, units: u32) -> Step {
    Step::Grant {
        id,
        sequence: Sequence::Next,
        ack: 0,
        objects,
        units,
    }
}

fn close(id: u64) -> Step {
    Step::Close {
        id,
        sequence: Sequence::Next,
        ack: 0,
    }
}

fn finish(answer: Answer) -> Step {
    Step::Finish {
        answer,
        stale: None,
    }
}

fn page(headers: u32) -> Step {
    Step::Page {
        headers,
        shave: 0,
        stale: None,
    }
}

fn select(branch: u8, height: u32) -> Step {
    Step::Select(Some(Tip { branch, height }))
}

/// The subscription of the job in progress, if one started.
fn job_id(machine: &Machine) -> Option<u64> {
    machine.job.as_ref().map(|record| record.job.id)
}

/// Run `steps` over a new machine, then end what is due and return the
/// outcomes queued.
fn outcomes_after(steps: &[Step]) -> Vec<HeadersOutcomeCode> {
    let mut machine = Machine::new();
    for step in steps {
        machine.run(step);
    }
    machine.run(&Step::EndDue);
    machine.queued.iter().copied().collect()
}

/// A follower of the selected tip at (0, 8) whose locator was its target at
/// height 3, with one page of one header sent and no credit left.
fn spent_follower() -> Machine {
    let mut machine = Machine::new();
    for step in [
        open(1, true, 3, 1, 1),
        Step::NextJob,
        // The locator is the target, so the first job reaches it at `Open`.
        page(1),
        select(0, 8),
        Step::NextJob,
        page(1),
    ] {
        machine.run(&step);
    }
    assert_eq!(machine.subs[&1].last, Some(frontier(0, 4)));
    assert_eq!(machine.subs[&1].unspent(), Sum::default());
    machine
}

machine_target_tests! {
    replay: replay_publisher_bytes,
    target: "header_publisher_policy",
    decoder: 2,
    witness: WITNESS => PublisherCounters {
        opened: 1,
        updated: 1,
        refused: 0,
        jobs: 1,
        pages: 1,
        results: 0,
        stale: 1,
        capacity_stalls: 0,
        ended: 1,
        written: 1,
        stopped: 0,
    },
}

/// Two subscriptions with work take turns: after serving one, state serves
/// the next ID, then wraps around.
#[test]
fn two_subscriptions_take_turns() {
    let mut machine = Machine::new();
    machine.run(&open_path(1));
    machine.run(&open_path(2));
    let mut served = Vec::new();
    for _ in 0..3 {
        machine.run(&Step::NextJob);
        served.extend(job_id(&machine));
        machine.run(&finish(Answer::Retry));
    }
    assert_eq!(served, [1, 2, 1]);
}

/// The specification's outcome windows, and how state moves an own outcome
/// into its window.
#[test]
fn outcome_windows() {
    use HeadersOutcomeCode::*;
    // (outcome, pages sent, reached, Close received) => legal
    for (outcome, pages_sent, reached, close_received, legal) in [
        (NoLocatorIntersection, false, false, false, true),
        (NoLocatorIntersection, true, false, false, false),
        (TargetNotRetained, false, true, false, true),
        (TargetNotRetained, true, true, false, false),
        (TargetNotSelected, false, false, false, true),
        (TargetNotSelected, true, false, false, false),
        (HistoryPruned, true, false, false, true),
        (HistoryPruned, false, true, false, false),
        (SubscriptionSuperseded, true, true, false, true),
        (SubscriptionClosed, false, false, false, false),
        (SubscriptionClosed, true, true, true, true),
    ] {
        assert_eq!(
            is_legal(outcome, pages_sent, reached, close_received),
            legal,
            "{outcome:?} pages_sent={pages_sent} reached={reached} close={close_received}"
        );
    }
    // (outcome, pages sent, reached) => queued
    for (outcome, pages_sent, reached, queued) in [
        (NoLocatorIntersection, false, false, NoLocatorIntersection),
        (TargetNotRetained, false, true, TargetNotRetained),
        (TargetNotSelected, false, false, TargetNotSelected),
        (NoLocatorIntersection, true, false, HistoryPruned),
        (TargetNotSelected, true, false, HistoryPruned),
        (TargetNotRetained, true, true, SubscriptionSuperseded),
        (HistoryPruned, false, false, HistoryPruned),
        (HistoryPruned, true, true, SubscriptionSuperseded),
        (SubscriptionSuperseded, false, false, SubscriptionSuperseded),
    ] {
        assert_eq!(
            in_window(outcome, pages_sent, reached),
            queued,
            "{outcome:?} pages_sent={pages_sent} reached={reached}"
        );
    }
}

/// An own outcome before the first page keeps its code, and after a page it
/// becomes `HistoryPruned` on the path.
#[test]
fn an_outcome_moves_into_its_window() {
    use HeadersOutcomeCode::*;
    let before = [
        open_path(1),
        Step::NextJob,
        finish(Answer::End(TargetNotSelected)),
    ];
    assert_eq!(outcomes_after(&before), [TargetNotSelected]);
    let after = [
        open_path(1),
        Step::NextJob,
        page(1),
        Step::NextJob,
        finish(Answer::End(TargetNotSelected)),
    ];
    assert_eq!(outcomes_after(&after), [HistoryPruned]);
}

/// What each storage reply means.
#[test]
fn storage_replies() {
    use HeadersOutcomeCode::*;
    use SelectedChainMembership::{NotRetained, NotSelected, Selected};
    let (a, b, c) = (frontier(0, 1), frontier(0, 2), frontier(0, 3));
    let local = |reason| Output::Stall(Stall::Local, reason);

    assert_eq!(on_probe(Selected(c)), None);
    assert_eq!(on_probe(NotSelected), Some(Output::End(TargetNotSelected)));
    assert_eq!(on_probe(NotRetained), Some(Output::End(TargetNotRetained)));

    // (refusal, following) => output
    for (refused, following, output) in [
        (
            LeaseRefused::NoLocatorIntersection,
            false,
            Output::End(NoLocatorIntersection),
        ),
        (
            LeaseRefused::NoLocatorIntersection,
            true,
            Output::End(SubscriptionSuperseded),
        ),
        (
            LeaseRefused::TargetNotRetained,
            false,
            Output::End(TargetNotRetained),
        ),
        (LeaseRefused::TargetNotRetained, true, local("selected_tip")),
        (
            LeaseRefused::HistoryPruned,
            false,
            Output::End(HistoryPruned),
        ),
        (
            LeaseRefused::HistoryPruned,
            true,
            Output::End(HistoryPruned),
        ),
        (LeaseRefused::Busy, false, local("acquire")),
        (LeaseRefused::Busy, true, local("acquire")),
    ] {
        assert_eq!(
            on_lease_refused(refused, following),
            output,
            "{refused:?} following={following}"
        );
    }

    // (parent, ancestor, target) => output
    assert_eq!(
        on_lease_acquired(None, c, c),
        Some(Output::ReachedAtOpen(c))
    );
    assert_eq!(on_lease_acquired(None, a, c), None);
    assert_eq!(on_lease_acquired(Some(a), a, c), None);
    assert_eq!(
        on_lease_acquired(Some(a), b, c),
        Some(local("lease_parent"))
    );

    assert_eq!(on_empty_page(false), Output::End(HistoryPruned));
    assert_eq!(on_empty_page(true), local("aux_unavailable"));

    let tip = c.hash;
    assert_eq!(
        on_following_check(Selected(b), b, tip),
        Output::Extends(tip)
    );
    assert_eq!(
        on_following_check(Selected(a), b, tip),
        Output::End(SubscriptionSuperseded)
    );
    assert_eq!(
        on_following_check(NotSelected, b, tip),
        Output::End(SubscriptionSuperseded)
    );
    assert_eq!(
        on_following_check(NotRetained, b, tip),
        Output::End(SubscriptionSuperseded)
    );
}

/// How many headers fit the next page: the advertised count and bytes, and
/// the unspent headers and bytes. Regtest entries take 181 bytes and a page's
/// fixed part 82.
#[test]
fn page_room_takes_the_tightest_limit() {
    let limits = |max_headers, max_bytes| PageLimits {
        max_headers,
        max_bytes,
        aux_schema_mask: AuxSchema::V1.mask_bit(),
    };
    let unspent = |objects, bytes| Totals { objects, bytes };
    // (max headers, max bytes, unspent headers, unspent bytes) => room
    for (max_headers, max_bytes, objects, bytes, room) in [
        (3, 10_000, 10, 10_000, 3),
        (10, 82 + 2 * 181, 10, 10_000, 2),
        (10, 10_000, 4, 10_000, 4),
        (10, 10_000, 10, 82 + 5 * 181, 5),
        (10, 10_000, 10, 82 + 5 * 181 - 1, 4),
        (10, 10_000, 10, 81, 0),
        (10, 10_000, 0, 10_000, 0),
    ] {
        assert_eq!(
            page_room(
                unspent(objects, bytes),
                AuxSchema::None,
                limits(max_headers, max_bytes),
                HB
            ),
            room,
            "max ({max_headers}, {max_bytes}) unspent ({objects}, {bytes})"
        );
    }
}

/// A follower whose selected tip drops to the height of its last header, on
/// another branch, ends as superseded without a state read.
#[test]
fn a_follower_is_superseded_when_the_tip_drops_to_its_last_header() {
    let mut machine = Machine::new();
    for step in [
        open(1, true, 3, 6, 6),
        Step::NextJob,
        page(1),
        select(1, 3),
        Step::NextJob,
    ] {
        machine.run(&step);
    }
    assert_eq!(job_id(&machine), None, "no state read");
    machine.run(&Step::EndDue);
    assert_eq!(machine.queued, [HeadersOutcomeCode::SubscriptionSuperseded]);
}

/// A follower at the selected tip waits for a higher one.
#[test]
fn a_follower_at_the_tip_waits() {
    let mut machine = Machine::new();
    for step in [
        open(1, true, 3, 6, 6),
        Step::NextJob,
        page(1),
        select(0, 3),
        Step::NextJob,
    ] {
        machine.run(&step);
    }
    assert_eq!(job_id(&machine), None);
    machine.run(&Step::EndDue);
    assert!(machine.queued.is_empty());
}

/// This node's own outcome, queued before `Close` arrived, wins over
/// `SubscriptionClosed`.
#[test]
fn an_own_outcome_wins_over_close() {
    use HeadersOutcomeCode::*;
    let steps = [
        open_path(1),
        Step::NextJob,
        finish(Answer::End(TargetNotSelected)),
        close(1),
    ];
    assert_eq!(outcomes_after(&steps), [TargetNotSelected]);
}

/// An outcome found after `Close` arrived is dropped, so the subscription
/// ends as closed.
#[test]
fn an_outcome_after_close_ends_as_closed() {
    use HeadersOutcomeCode::*;
    let steps = [
        open_path(1),
        Step::NextJob,
        close(1),
        finish(Answer::End(TargetNotSelected)),
    ];
    assert_eq!(outcomes_after(&steps), [SubscriptionClosed]);
}

/// A stall waits for the next selected-chain change.
#[test]
fn a_selected_chain_change_ends_a_stall() {
    let mut machine = Machine::new();
    for step in [
        open_path(1),
        Step::NextJob,
        finish(Answer::Local),
        Step::NextJob,
    ] {
        machine.run(&step);
    }
    assert_eq!(job_id(&machine), None, "stalled");
    machine.run(&select(0, 5));
    machine.run(&Step::NextJob);
    assert_eq!(job_id(&machine), Some(1));
}

/// A stall is not recorded when the selected chain moved since the job read
/// it, because the change that would end it already passed.
#[test]
fn no_stall_after_the_chain_moved() {
    let mut machine = Machine::new();
    for step in [
        open_path(1),
        select(0, 5),
        Step::NextJob,
        select(0, 6),
        finish(Answer::Local),
        Step::NextJob,
    ] {
        machine.run(&step);
    }
    assert_eq!(job_id(&machine), Some(1));
}

/// A follower with no credit checks each new tip once, and a page lets it
/// check the same tip again from its new last header.
#[test]
fn a_page_lets_a_follower_check_the_same_tip_again() {
    let mut machine = spent_follower();
    machine.run(&Step::NextJob);
    let check = machine.job.clone().expect("a check of the tip");
    assert_eq!(
        (check.job.count, check.job.lease_target),
        (0, frontier(0, 8).hash)
    );
    // Storage finds the tip extending the last header.
    machine.run(&page(1));
    machine.run(&Step::NextJob);
    assert_eq!(job_id(&machine), None, "the tip was checked");
    machine.run(&grant(1, 1, 2));
    machine.run(&Step::NextJob);
    machine.run(&page(1));
    assert_eq!(machine.subs[&1].last, Some(frontier(0, 5)));
    machine.run(&Step::NextJob);
    let check = machine
        .job
        .clone()
        .expect("a check from the new last header");
    assert_eq!(
        (check.job.count, check.job.parent),
        (0, Some(frontier(0, 5)))
    );
}

/// A check answered on a chain the selection has since left records no tip,
/// even when the tip hash is the same again.
#[test]
fn a_check_on_a_moved_chain_records_no_tip() {
    let mut machine = spent_follower();
    machine.run(&Step::NextJob);
    let check = machine.job.clone().expect("a check of the tip");
    let moved = snapshot(frontier(0, 8), 2, frontier(0, 0));
    machine.owner.state.finish(
        check.job.key,
        Output::Extends(check.job.lease_target),
        Some(&moved),
    );
    let again = machine
        .owner
        .state
        .next_job::<HB>(Some(&moved))
        .expect("the tip is checked again");
    assert_eq!((again.id, again.count), (1, 0));
}

/// A `Close` with a repeated sequence reports the sequence, even when its
/// acknowledgement is unknown too.
#[test]
fn a_close_checks_its_sequence_first() {
    let mut machine = Machine::new();
    machine.run(&open_path(1));
    let update = SubscribeHeaders {
        operation: Operation::Close,
        subscription_id: 1,
        update_sequence: 0,
        initial_target_tip_hash: frontier(0, 5).hash,
        acknowledged: Cursor {
            height: block::Height(99),
            hash: block::Hash([0xee; 32]),
            headers: 0,
            bytes: 0,
        },
        locator_hashes: Vec::new(),
        added_credit: Credit {
            objects: 0,
            bytes: 0,
        },
        tree_aux_schema: AuxSchema::None,
    };
    assert_eq!(
        machine.owner.apply(&update),
        Err(SubscriptionFault::Sequence {
            expected: 1,
            got: 0
        }
        .into())
    );
}

/// The worked scenario: a job for subscription A, A ends and a new A opens,
/// and the old job's page arrives under a changed selected chain. State sends
/// no old page, spends none of the new A's credit, and keeps serving it.
#[test]
fn a_stale_page_after_replacement_spends_no_replacement_credit() {
    let a = 1;
    let mut machine = Machine::new();
    machine.run(&open_path(a));
    machine.run(&Step::NextJob);
    let old = machine.job.clone().expect("A has a page to read");
    machine.run(&close(a));
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    machine.run(&Step::TerminalWritten(true));
    // Another subscription's `Open` clears A's tombstone, so A can open again.
    machine.run(&open(2, false, 5, 1, 1));
    machine.run(&close(2));
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    machine.run(&Step::TerminalWritten(true));
    machine.run(&open_path(a));
    machine.run(&select(1, 7));
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
            .publications()
            .credit(&a)
            .map(|credit| credit.consumed()),
        Some(Totals::default()),
        "the new A spent no credit"
    );
    // The old result stalled nothing: the new A gets its first page job.
    machine.run(&Step::NextJob);
    let new = machine.job.as_ref().expect("the new A has a page to read");
    assert_eq!((new.job.id, new.job.parent), (a, None));
    assert_ne!(new.job.key, old.job.key);
}

/// A capacity stall waits for its own signal: releasing an older one leaves
/// it stalled, and releasing its own lets the next job start.
#[test]
fn a_capacity_stall_waits_for_its_own_signal() {
    let mut machine = Machine::new();
    machine.run(&open_path(1));
    for _ in 0..2 {
        // A selected-chain change clears the previous stall.
        machine.run(&Step::Select(None));
        machine.run(&Step::NextJob);
        assert!(machine.job.is_some(), "the path has a page");
        machine.run(&finish(Answer::Capacity));
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
    assert!(machine.job.is_none(), "still stalled");
    machine.owner.state.capacity_released(&[own]);
    machine.run(&Step::NextJob);
    assert!(machine.job.is_some(), "the release lets it retry");
}

/// While a terminal outcome waits for the stream, another subscription's job
/// in progress still commits its page. No new job starts until the write
/// finishes.
#[test]
fn another_subscriptions_page_commits_while_a_terminal_waits() {
    let mut machine = Machine::new();
    machine.run(&open_path(1));
    machine.run(&open_path(2));
    machine.run(&Step::NextJob);
    assert_eq!(job_id(&machine), Some(1));
    machine.run(&close(2));
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    assert!(
        machine.writing.is_some(),
        "subscription 2's outcome is written"
    );
    machine.run(&page(3));
    assert_eq!(machine.subs[&1].sent.len(), 1, "the page committed");
    machine.run(&Step::NextJob);
    assert!(machine.job.is_none(), "the write blocks jobs");
    machine.run(&Step::TerminalWritten(true));
    machine.run(&Step::NextJob);
    assert!(machine.job.is_some(), "the next job starts after the write");
}

/// Two subscriptions that close together, with the first outcome taken.
fn two_closed_one_writing() -> Machine {
    let mut machine = Machine::new();
    for id in [1, 2] {
        machine.run(&open_path(id));
        machine.run(&close(id));
    }
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    assert_eq!(machine.queued.len(), 1, "one outcome still waits");
    // The second outcome waits for the first write.
    machine.run(&Step::NextTerminal);
    assert_eq!(machine.queued.len(), 1, "one write at a time");
    machine
}

/// Cancellation during a terminal write stops the publisher: the queued
/// outcome is never offered, and no job starts.
#[test]
fn cancellation_during_a_terminal_write_stops_the_publisher() {
    let mut machine = two_closed_one_writing();
    machine.run(&Step::Cancel);
    assert_eq!(machine.exit, Some(Exit::Cancelled));
}

/// A stream that refuses a terminal outcome stops the publisher: the queued
/// outcome is never offered, and no job starts.
#[test]
fn a_refused_terminal_write_stops_the_publisher() {
    let mut machine = two_closed_one_writing();
    machine.run(&Step::TerminalWritten(false));
    assert_eq!(machine.exit, Some(Exit::Closed));
}

/// A queued terminal outcome keeps its slot, its permit keeps it through the
/// write, and only the finished write frees it.
#[test]
fn a_terminal_holds_its_slot_until_its_write_finishes() {
    let mut machine = Machine::new();
    machine.run(&open_path(1));
    assert_eq!(machine.owner.slots.held(), 1);
    machine.run(&close(1));
    machine.run(&Step::EndDue);
    assert_eq!(machine.owner.state.publications().keys().count(), 0);
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
    machine.run(&open_path(1));
    machine.run(&Step::NextJob);
    let stale = machine.job.clone().expect("a page to read");
    machine.run(&finish(Answer::Retry));
    machine.run(&Step::NextJob);
    let job = machine.job.clone().expect("a page to read");
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
    assert_eq!(
        machine.owner.capacity_signals(),
        std::slice::from_ref(&signal)
    );
    assert_eq!(machine.owner.state.capacity_waits().len(), 1);

    signal.release();
    machine.owner.capacity_released();
    assert!(machine.owner.state.capacity_waits().is_empty());
    assert!(machine.owner.capacity_signals().is_empty());
}

/// Open, grant, read a page, close, end, take and write the outcome, then a
/// stale page.
const WITNESS: &[u8] = &[
    0, 0, 4, 35, 10, 0, 0, 0, 15, 0, 0, 0, 8, 0, 0, 0, 23, 0, 0, 0, 24, 0, 0, 0, 25, 0, 0, 0, 15,
    0, 0, 128,
];

/// The witness's history records one step per operation and what the
/// publisher showed after each. It reaches credit, a terminal write, and a
/// stale page.
#[test]
fn publisher_trace_records_the_witness() {
    let history = trace_header_publisher_bytes(WITNESS);
    assert_eq!(history.steps.len(), WITNESS.len() / 4);
    for (index, step) in history.steps.iter().enumerate() {
        assert_eq!(step.step, index + 1);
        assert_eq!(step.input, WITNESS[index * 4..(index + 1) * 4]);
        assert_eq!(step.failure, None);
    }
    assert_eq!(history.steps[0].actual.subscriptions.len(), 1);
    assert_eq!(history.steps[1].actual.job, Some(1));
    assert_eq!(history.steps[2].actual.subscriptions[0].consumed, (1, 263));
    assert_eq!(history.steps[4].actual.terminals, 1);
    assert!(history.steps[5].actual.writing);
    assert_eq!(history.steps[6].actual.slots, 0);
    // The last operation's page answers the settled job, so it changes nothing.
    assert_eq!(history.steps[7].actual, history.steps[6].actual);
    assert_eq!(
        trace_header_publisher_bytes(&[]),
        HeaderPublisherHistory::default()
    );
}

/// A short final operation keeps its original bytes.
#[test]
fn publisher_trace_keeps_a_short_final_operation() {
    let history = trace_header_publisher_bytes(&[0, 0]);
    assert_eq!(history.steps.len(), 1);
    assert_eq!(history.steps[0].input, [0, 0]);
}

/// A publisher that stops ends its history at that operation.
#[test]
fn publisher_trace_stops_with_the_publisher() {
    let history = trace_header_publisher_bytes(&[30, 0, 0, 0, 10, 0, 0, 0]);
    assert_eq!(history.steps.len(), 1);
    assert_eq!(history.steps[0].actual.exit, Some("cancelled"));
}

proptest::proptest! {
    /// Any byte history records no failure.
    #[test]
    fn publisher_trace_records_no_failure(
        bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..=MAX_HISTORY_BYTES)
    ) {
        for step in trace_header_publisher_bytes(&bytes).steps {
            proptest::prop_assert_eq!(step.failure, None);
        }
    }
}

/// A subscription that leaves the ledger behind the task's back breaks the
/// ledger check.
#[test]
#[should_panic(expected = "header/publisher-policy")]
fn publisher_witness_rejects_a_lost_subscription() {
    run(decode(WITNESS), |machine, step| {
        if matches!(step, Step::Open { .. }) {
            let update = SubscribeHeaders {
                operation: Operation::Close,
                subscription_id: 1,
                update_sequence: 1,
                initial_target_tip_hash: machine.subs[&1].target.hash,
                acknowledged: machine.subs[&1].acknowledged,
                locator_hashes: Vec::new(),
                added_credit: Credit {
                    objects: 0,
                    bytes: 0,
                },
                tree_aux_schema: AuxSchema::None,
            };
            machine.owner.apply(&update).expect("a well-formed Close");
            machine.owner.end_due();
        }
    });
}

/// A job started behind the task's back breaks the job check.
#[test]
#[should_panic(expected = "header/publisher-policy")]
fn publisher_witness_rejects_lost_job() {
    run(decode(WITNESS), |machine, step| {
        if matches!(step, Step::Open { .. }) {
            machine.owner.state.next_job::<HB>(None);
        }
    });
}

/// A terminal outcome taken behind the task's back breaks the order check.
#[test]
#[should_panic(expected = "header/publisher-policy")]
fn publisher_witness_rejects_lost_terminal() {
    run(decode(WITNESS), |machine, step| {
        if matches!(step, Step::EndDue) {
            machine.owner.state.next_terminal();
        }
    });
}
