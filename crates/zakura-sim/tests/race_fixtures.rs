//! The six race fixtures from `scheduler.md`, run through `Sim` (S4.4).
//! Every planted variant fails on some input, the fixes never do, and one
//! worker hides the races inside a poll, as on the bare scheduler.
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
    MemorySink, Outcome, PlanInput, RunReport,
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
                None => false,
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
