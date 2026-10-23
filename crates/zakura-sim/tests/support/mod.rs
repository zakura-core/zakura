//! Shared helpers for `zakura-sim`'s integration tests.

#![allow(dead_code)]

use zakura_scheduler::SchedulerConfig;
use zakura_sim::{Checker, Plan, PlanInput, RunConfig, RunReport, Sim};

/// A run config with `workers` workers, small stacks, and `budget` steps.
pub fn config(workers: u16, budget: u64) -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig {
            workers,
            // Test tasks are tiny, and a small stack keeps the pool cheap.
            stack_size: 64 * 1024,
            ..SchedulerConfig::default()
        },
        budget,
        drain_bound: 1_000,
        ..RunConfig::default()
    }
}

/// Runs one simulation: `setup` spawns tasks and adds sources, then the run
/// goes to its end under `checker`.
pub fn run(
    config: RunConfig,
    plan: PlanInput,
    setup: impl FnOnce(&mut Sim),
    checker: &mut dyn Checker,
) -> RunReport {
    let mut sim = Sim::new(config, plan);
    setup(&mut sim);
    sim.run(checker)
}

/// `count` byte inputs of up to `max_len` bytes, the same on every call.
pub fn inputs(count: usize, max_len: usize) -> Vec<PlanInput> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move || {
        // SplitMix64.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    (0..count)
        .map(|_| {
            // The remainder is at most max_len, which is a usize.
            let len = (next() % (max_len as u64 + 1)) as usize;
            // Keeping the low byte is the point.
            let bytes: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            Plan::from_fuzz(&bytes)
        })
        .collect()
}

/// The picks of `report`, as identity strings.
pub fn picks(report: &RunReport) -> Vec<&str> {
    report.picks.iter().map(|p| p.identity.as_str()).collect()
}

/// The instants, in nanoseconds, that `report`'s time advances went to.
pub fn advances(report: &RunReport) -> Vec<u128> {
    picks(report)
        .iter()
        .filter_map(|p| p.strip_prefix("advance-time:"))
        .map(|ns| ns.parse().expect("advance-time picks name an instant"))
        .collect()
}
