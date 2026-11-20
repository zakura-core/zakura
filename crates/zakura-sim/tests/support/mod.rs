//! Shared helpers for `zakura-sim`'s integration tests.

#![allow(dead_code)]

pub mod feed;
pub mod header_inputs;
pub mod prepare_fault;
pub mod script;
pub mod steer;

use zakura_scheduler::SchedulerConfig;
use zakura_sim::{Checker, MemorySink, Plan, PlanInput, RunConfig, RunReport, Sim};

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
    recorded(config, plan, setup, checker).0
}

/// Runs one simulation as [`run`] does, and also returns its records as
/// JSON lines.
pub fn recorded(
    config: RunConfig,
    plan: PlanInput,
    setup: impl FnOnce(&mut Sim),
    checker: &mut dyn Checker,
) -> (RunReport, Vec<String>) {
    let mut sim = Sim::new(config, plan);
    setup(&mut sim);
    let mut sink = MemorySink::default();
    let report = sim.run(checker, &mut sink);
    (report, sink.lines())
}

/// `count` byte inputs of up to `max_len` bytes, the same on every call.
pub fn inputs(count: usize, max_len: usize) -> Vec<PlanInput> {
    byte_inputs(count, max_len)
        .iter()
        .map(|bytes| Plan::from_fuzz(bytes))
        .collect()
}

pub use zakura_sim::seed::draws::byte_inputs;

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
