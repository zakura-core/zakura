//! The `race` seeds: each race fixture's fixed variant on `fixtures/all`,
//! and each fixture's saved violating schedule on its own harness.

use super::{draws::byte_input, Seed, SeedSet};
use crate::{
    harness::fixtures::{All, Variant, Which},
    harness::Subsystem,
    witness::Expect,
};

/// Short Plan `index` of [`super::draws::byte_inputs`]' stream of Plans up
/// to 64 bytes long.
fn short_plan(index: usize) -> Vec<u8> {
    byte_input(index + 1, 64, index)
}

/// `fixtures/all`'s prefix that picks `fixture`, `variant` and one worker.
fn pick(fixture: Which, variant: Variant) -> Vec<u8> {
    let index = |found: Option<usize>| {
        // Six fixtures and five variants fit a byte.
        found.expect("every fixture and variant is listed") as u8
    };
    vec![
        index(Which::ALL.iter().position(|which| *which == fixture)),
        index(Variant::ALL.iter().position(|which| *which == variant)),
        0,
    ]
}

/// A race harness's prefix that picks the planted race on two workers.
const PLANTED_ON_TWO_WORKERS: [u8; 2] = [0, 1];

/// `fixture`'s saved violating schedule: the planted race on two workers,
/// then Plan bytes that reach the race's trigger history. The first three
/// fixtures' races need no steering, so their Plans are empty, which runs
/// the tasks in round-robin order and interleaves them. The others take
/// the short Plan a search found to reach the race.
fn schedule(fixture: Which) -> Vec<u8> {
    let plan = match fixture {
        Which::LostUpdate | Which::SnapshotReserve | Which::AbBa => Vec::new(),
        Which::AbortWithoutJoin => short_plan(6),
        Which::MissedNotify => short_plan(14),
        Which::TimerCancel => short_plan(17),
    };
    [&PLANTED_ON_TWO_WORKERS[..], &plan].concat()
}

/// Every `race` seed.
pub(super) fn seeds() -> Vec<Seed> {
    Which::ALL
        .into_iter()
        .flat_map(|fixture| {
            let property = fixture.property();
            let name = property.trim_start_matches("race/");
            [
                Seed::new(
                    format!("race-fixtures-{name}-fixed"),
                    All::NAME,
                    "crates/zakura-sim/tests/race_fixtures.rs",
                    pick(fixture, Variant::Fixed),
                ),
                Seed::new(
                    format!("race-schedule-{name}"),
                    property,
                    "crates/zakura-sim/tests/race_schedules.rs",
                    schedule(fixture),
                )
                .witness(property, Expect::Violated)
                .in_set(SeedSet::KnownFailure),
            ]
        })
        .collect()
}
