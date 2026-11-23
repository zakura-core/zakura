//! Memory-plan and report helpers for the memory-budget witnesses.

use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::memory::{MemoryPlan, MemoryPlanError},
    Config,
};
use zakura_sim::{PropertyOutcome, RunReport};

/// The smallest P2P budget `B` that `MemoryPlan::size_for_node` accepts on
/// `network` with the default config: the required bytes its refusal of
/// `B = 0` reports.
pub fn smallest_accepted_budget_b(network: &Network) -> u64 {
    let config = Config {
        network: network.clone(),
        ..Config::default()
    };
    match MemoryPlan::size_for_node(0, &config, network, None, &[]) {
        Err(MemoryPlanError::BudgetTooSmall(refusal)) => refusal.required_bytes,
        other => panic!("a zero budget is refused for its size on {network:?}: {other:?}"),
    }
}

/// `id`'s outcome in `report`.
pub fn outcome<'a>(report: &'a RunReport, id: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|p| p.property == id)
        .unwrap_or_else(|| panic!("report is missing property `{id}`"))
        .outcome
}

/// Whether `id` held in `report` and applied at least once.
pub fn held(report: &RunReport, id: &str) -> bool {
    matches!(outcome(report, id), PropertyOutcome::Held { applicable } if *applicable > 0)
}
