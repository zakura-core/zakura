//! Each race fixture's violating schedule, found by a bounded search.
//!
//! A schedule is the planted race on two workers, then Plan bytes under
//! which the race reaches its trigger history and violates the fixture's
//! property, while the fix reaches the same history and holds. The search
//! tries the empty Plan, which runs the tasks round-robin, then a fixed
//! stream of short Plans.

use zakura_sim::{
    harness::{
        fixtures::{Fixture, Race, Scenario, Variant},
        Subsystem,
    },
    witness::{witness, Expect, WitnessScenario},
    Plan,
};

use super::byte_inputs;

/// How many short Plans, after the empty one, the search tries.
pub const SCHEDULE_PLANS: usize = 32;

/// The longest short Plan the search tries.
pub const SCHEDULE_PLAN_BYTES: usize = 64;

/// A race harness's prefix that picks the planted race on two workers.
pub const PLANTED_ON_TWO_WORKERS: [u8; 2] = [0, 1];

/// `F` under `variant` on two workers, with Plan bytes `plan`.
pub fn on_two_workers<F: Fixture>(variant: Variant, plan: &[u8]) -> WitnessScenario<Race<F>> {
    WitnessScenario {
        scenario: Scenario {
            variant,
            workers: 2,
        },
        input: Plan::from_fuzz(plan),
    }
}

/// Whether the planted race violates `F`'s property with its trigger
/// history under `plan`, and the fix holds with the same history.
fn reaches<F: Fixture>(plan: &[u8]) -> bool {
    witness(
        on_two_workers::<F>(Variant::Base, plan),
        F::NAME,
        Expect::Violated,
    )
    .is_ok()
        && witness(
            on_two_workers::<F>(Variant::Fixed, plan),
            F::NAME,
            Expect::Holds,
        )
        .is_ok()
}

/// `F`'s schedule's Plan bytes: the empty Plan if it reaches the race, or
/// else the first of [`SCHEDULE_PLANS`] short Plans that does.
pub fn schedule_plan<F: Fixture>() -> Vec<u8> {
    std::iter::once(Vec::new())
        .chain(byte_inputs(SCHEDULE_PLANS, SCHEDULE_PLAN_BYTES))
        .find(|plan| reaches::<F>(plan))
        .unwrap_or_else(|| {
            panic!(
                "{}: no Plan of the empty one and {SCHEDULE_PLANS} short ones reaches the race",
                F::NAME
            )
        })
}

/// `F`'s schedule as a full fuzz input of its own harness: the planted
/// race on two workers, then [`schedule_plan`].
pub fn schedule_input<F: Fixture>() -> Vec<u8> {
    assert_eq!(
        Race::<F>::draw(&PLANTED_ON_TWO_WORKERS),
        Scenario {
            variant: Variant::Base,
            workers: 2,
        },
        "the prefix picks the planted race on two workers"
    );
    [&PLANTED_ON_TWO_WORKERS[..], &schedule_plan::<F>()].concat()
}
