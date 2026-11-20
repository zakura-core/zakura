//! Shared helpers for `zakura-sim`'s integration tests.

#![allow(dead_code)]

pub mod discovery_inputs;
pub mod feed;
pub mod header_inputs;
pub mod plan_search;
pub mod prepare_fault;
pub mod qa_store;
pub mod race_schedule;
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

/// The search seed [`reaches`] draws from. A test needs a state reached,
/// not a particular input, so every test shares one seed.
pub const REACH_SEED: u64 = 0;

/// The first run, of at most `runs` from `generate`'s search on `harness`
/// with cycled draws and a PCT schedule, for which `overlap` returns
/// `Some`. Fails the test if none does within `runs` runs.
///
/// A test uses this in place of a pinned input: the search finds the
/// overlap again on every build, so a production change that moves a fixed
/// input off the overlap fails here instead of passing without the overlap.
pub fn reaches<T, F>(
    harness: &str,
    overlap: F,
    runs: u64,
) -> zakura_sim::explore::generate::Reached<T>
where
    T: Send + 'static,
    F: Fn(&RunReport) -> Option<T> + Clone + Send + 'static,
{
    reaches_with(
        harness,
        &zakura_sim::explore::generate::ReachSearch::new(REACH_SEED, runs),
        overlap,
    )
}

/// [`reaches`] under `search`, for a test that builds its scenario in code
/// or steers the schedule toward its overlap.
pub fn reaches_with<T, F>(
    harness: &str,
    search: &zakura_sim::explore::generate::ReachSearch,
    overlap: F,
) -> zakura_sim::explore::generate::Reached<T>
where
    T: Send + 'static,
    F: Fn(&RunReport) -> Option<T> + Clone + Send + 'static,
{
    let entry = zakura_sim::harness::find(harness)
        .unwrap_or_else(|error| panic!("{harness} isn't a registered harness: {error}"));
    zakura_sim::explore::generate::reach(entry, search, overlap)
        .unwrap_or_else(|| panic!("no run of {} on {harness} reached the overlap", search.runs))
}
