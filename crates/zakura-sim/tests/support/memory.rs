//! Memory-plan, run and report helpers for the memory-budget witnesses.

use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::memory::{MemoryPlan, MemoryPlanError},
    Config,
};
use zakura_sim::{
    harness::{
        header_sync::Dialer,
        run_prepared,
        sync::{MirrorServices, SyncMirror, SyncScenario},
        sync_wire::SyncWireScenario,
        HarnessError, NetScenario, Subsystem,
    },
    witness::{judge_report, Expect},
    MemorySink, Plan, PropertyOutcome, RunReport,
};

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

/// Assert that each of `ids` held in `report` and applied at least once.
pub fn assert_held(report: &RunReport, ids: &[&str]) {
    for id in ids {
        assert!(held(report, id), "{id}: {:?}", outcome(report, id));
    }
}

/// Run `scenario` of `S` once from `prepared` with an empty input, and
/// assert that no property was violated. A harness failure panics.
pub fn run<S: Subsystem>(scenario: &S::Scenario, prepared: S::Prepared) -> RunReport {
    let report = match run_prepared::<S>(
        scenario,
        prepared,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    ) {
        Ok(report) => report,
        Err(HarnessError::Run { failure, report }) => panic!("{failure}: {:?}", report.end),
        Err(error) => panic!("{error}"),
    };
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    report
}

/// Judge `id` on `report`, a run of `S`, as a passing witness: it held, and
/// its trigger history fired.
pub fn assert_witness_holds<S: Subsystem>(report: &RunReport, id: &str) {
    if let Err(error) = judge_report::<S>(report, id, Expect::Holds) {
        panic!("{}: {id}: {error}", S::NAME);
    }
}

/// A mirror that `dialer` connects and that runs `services`.
pub fn mirror(dialer: Dialer, services: MirrorServices) -> SyncMirror {
    SyncMirror {
        dialer,
        services: Some(services),
    }
}

/// A `sync/wire` scenario syncing `blocks` blocks of `body_bytes` from
/// `mirrors` for at most `steps` steps at the P2P budget `budget_b`.
pub fn wire_scenario(
    blocks: u32,
    body_bytes: u32,
    mirrors: Vec<SyncMirror>,
    steps: u64,
    budget_b: u64,
) -> SyncWireScenario {
    SyncWireScenario {
        net: NetScenario::default(),
        sync: SyncScenario {
            blocks,
            body_bytes,
            mirrors,
            steps: Some(steps),
            ..SyncScenario::default()
        },
        windows: None,
        superseded_hold: None,
        memory_budget_bytes: Some(budget_b),
    }
}
