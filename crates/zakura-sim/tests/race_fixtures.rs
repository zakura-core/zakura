//! The six race fixtures from `scheduler.md`, run through `Sim` (S4.4).
//! Every planted variant fails on some input, the fixes never do, and one
//! worker hides the races inside a poll, as on the bare scheduler. Each
//! fixture's witness test runs one input that reaches the race's trigger
//! history, on the race and on the fix.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_sim::{
    harness::{
        fixtures::{
            AbBa, AbortWithoutJoin, Fixture, LostUpdate, MissedNotify, Race, Scenario,
            SnapshotReserve, TimerCancel, Variant,
        },
        run_harness,
    },
    witness::{check_witness, witness, Expect, WitnessError, WitnessScenario},
    MemorySink, Outcome, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// Inputs to try when a planted race must be found.
const FIND: usize = 500;

/// Inputs that must all pass on fixed code.
const CLEAN: usize = 2_000;

/// Runs fixture `F` under `variant` and `plan`.
fn run<F: Fixture>(variant: Variant, workers: u16, plan: PlanInput) -> RunReport {
    run_harness::<Race<F>>(
        &Scenario { variant, workers },
        plan,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{}: {error}", F::NAME))
}

/// How many of `count` inputs violate `F`'s property under `variant`.
fn violations<F: Fixture>(variant: Variant, workers: u16, count: usize) -> usize {
    inputs(count, 64)
        .into_iter()
        .filter(|plan| {
            let report = run::<F>(variant, workers, plan.clone());
            assert_ne!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
            assert_eq!(
                report.cleanup.leaked,
                0,
                "{}: shutdown leaked no task",
                F::NAME
            );
            match &report.end.violation {
                Some(violation) => {
                    assert_eq!(violation.property, F::NAME);
                    true
                }
                None => {
                    // A passing run counts only if the check applied.
                    let held = report.properties.iter().any(|r| {
                        r.property == F::NAME
                            && matches!(r.outcome, PropertyOutcome::Held { applicable } if applicable > 0)
                    });
                    assert!(held, "{}: {:?}", F::NAME, report.properties);
                    false
                }
            }
        })
        .count()
}

/// Checks `F` on 2 workers: every planted variant fails on some input, and
/// the fix fails on none.
fn finds_the_race_and_the_fix_holds<F: Fixture>() {
    for variant in Variant::ALL {
        if variant == Variant::Fixed {
            assert_eq!(violations::<F>(variant, 2, CLEAN), 0, "{} fixed", F::NAME);
        } else {
            assert!(
                violations::<F>(variant, 2, FIND) > 0,
                "{} {variant:?}",
                F::NAME
            );
        }
    }
}

#[test]
fn lost_update() {
    finds_the_race_and_the_fix_holds::<LostUpdate>();
}

#[test]
fn snapshot_reserve() {
    finds_the_race_and_the_fix_holds::<SnapshotReserve>();
}

#[test]
fn missed_notify() {
    finds_the_race_and_the_fix_holds::<MissedNotify>();
}

#[test]
fn timer_cancel() {
    finds_the_race_and_the_fix_holds::<TimerCancel>();
}

#[test]
fn ab_ba() {
    finds_the_race_and_the_fix_holds::<AbBa>();
}

#[test]
fn abort_without_join() {
    finds_the_race_and_the_fix_holds::<AbortWithoutJoin>();
}

/// As on the bare scheduler, one worker runs a poll without a switch, so
/// races inside one poll disappear. Timer against cancel crosses a poll
/// boundary, so one worker still finds it.
#[test]
fn one_worker_hides_races_inside_a_poll() {
    assert_eq!(violations::<LostUpdate>(Variant::Base, 1, FIND), 0);
    assert_eq!(violations::<SnapshotReserve>(Variant::Base, 1, FIND), 0);
    assert_eq!(violations::<MissedNotify>(Variant::Base, 1, FIND), 0);
    assert!(violations::<TimerCancel>(Variant::Base, 1, FIND) > 0);
}

/// `F` under `variant` on 2 workers, with fuzz input `input`.
fn witnessed<F: Fixture>(variant: Variant, input: &[u8]) -> WitnessScenario<Race<F>> {
    WitnessScenario {
        scenario: Scenario {
            variant,
            workers: 2,
        },
        input: Plan::from_fuzz(input),
    }
}

/// `F`'s witness test: on `input`, the planted race reaches its trigger
/// history and violates the property, and the fix reaches it too and holds.
fn witness_test<F: Fixture>(input: &[u8]) {
    check_witness(
        witnessed::<F>(Variant::Base, input),
        F::NAME,
        Expect::Violated,
    );
    check_witness(
        witnessed::<F>(Variant::Fixed, input),
        F::NAME,
        Expect::Holds,
    );
}

// The empty input runs the tasks in round-robin order, which interleaves
// the first three fixtures' tasks.

#[test]
fn lost_update_witness() {
    witness_test::<LostUpdate>(&[]);
}

#[test]
fn snapshot_reserve_witness() {
    witness_test::<SnapshotReserve>(&[]);
}

#[test]
fn ab_ba_witness() {
    witness_test::<AbBa>(&[]);
}

#[test]
fn missed_notify_witness() {
    witness_test::<MissedNotify>(&[
        0x14, 0xb1, 0x51, 0x74, 0xfe, 0x95, 0x35, 0x0a, 0xef, 0xac, 0xa9, 0x97, 0xb5, 0xca, 0xa1,
        0xc9, 0x18,
    ]);
}

#[test]
fn timer_cancel_witness() {
    witness_test::<TimerCancel>(&[0x38, 0x44, 0x6a, 0x3e, 0x1c, 0x9d, 0xd9, 0x80, 0x20]);
}

#[test]
fn abort_without_join_witness() {
    witness_test::<AbortWithoutJoin>(&[0xbd, 0xe4, 0xe8, 0xf2, 0xe1]);
}

/// An input that runs one incrementer's add before the other reaches its
/// own holds on the fix without reaching the race, so its witness test
/// fails.
#[test]
fn serialized_increments_lose_the_trigger_history() {
    assert_eq!(
        witness(
            witnessed::<LostUpdate>(Variant::Fixed, &[0x64, 0xac]),
            LostUpdate::NAME,
            Expect::Holds
        ),
        Err(WitnessError::LostTriggerHistory)
    );
}

/// `F` under the planted race on `plan` holds without reaching the trigger
/// history, so its witness test fails.
fn harmless_history<F: Fixture>(plan: PlanInput) {
    let report = run::<F>(Variant::Base, 2, plan.clone());
    assert_eq!(report.end.violation, None, "{}", F::NAME);
    let scenario: WitnessScenario<Race<F>> = WitnessScenario {
        scenario: Scenario {
            variant: Variant::Base,
            workers: 2,
        },
        input: plan,
    };
    assert_eq!(
        witness(scenario, F::NAME, Expect::Holds),
        Err(WitnessError::LostTriggerHistory),
        "{}",
        F::NAME
    );
}

/// The marker stores the flag before the waiter checks it, so the waiter
/// exits before the notification.
#[test]
fn a_waiter_that_saw_the_flag_misses_the_trigger() {
    harmless_history::<MissedNotify>(Plan::from_fuzz(&[]));
}

/// One task reserves before the other checks, so the check sees no
/// capacity.
#[test]
fn a_check_after_the_reservation_misses_the_trigger() {
    let plan = inputs(5, 64).pop().expect("five inputs");
    harmless_history::<SnapshotReserve>(plan);
}

/// The cancel lands inside the timer's deciding poll, after the poll read
/// the token.
#[test]
fn a_cancel_inside_the_deciding_poll_misses_the_trigger() {
    harmless_history::<TimerCancel>(Plan::from_fuzz(&[]));
}

/// The worker unwinds before the aborter checks the resource.
#[test]
fn an_unwind_before_the_check_misses_the_trigger() {
    let plan = inputs(20, 64)
        .into_iter()
        .find(|plan| {
            run::<AbortWithoutJoin>(Variant::Base, 2, plan.clone())
                .end
                .violation
                .is_none()
        })
        .expect("some input unwinds the worker first");
    harmless_history::<AbortWithoutJoin>(plan);
}
