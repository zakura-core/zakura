//! hdr-9c.2b: `header-sync/commit-only-authorized` and
//! `header-sync/reset-invalidates-work` through a successful VCT body
//! repair, on `header-sync/repair`. On each committed repair input both
//! properties apply and hold, and the repair's apply commits and leaves the
//! engine's header generation unchanged, so it adds no header to the chain.
#![cfg(zakura_sim)]

use zakura_sim::{
    domain::header_sync::{COMMIT_ONLY_AUTHORIZED, RESET_INVALIDATES_WORK},
    harness::{
        header_sync::{RepairHarness, RepairScenario},
        run_prepared, Subsystem,
    },
    observe::{Observation, StorageFact, StorageRequest, StorageResult},
    MemorySink, Plan, PropertyOutcome, RunReport,
};

/// The committed repair inputs, `tests/fixtures/header-repair-*.bin`, each
/// with its fixture: a scenario byte, then Plan bytes.
const INPUTS: [(&str, RepairScenario, &[u8]); 3] = [
    (
        "header-repair-applied",
        RepairScenario::Applied,
        include_bytes!("fixtures/header-repair-applied.bin"),
    ),
    (
        "header-repair-prepare-departs",
        RepairScenario::PrepareDeparts,
        include_bytes!("fixtures/header-repair-prepare-departs.bin"),
    ),
    (
        "header-repair-apply-departs",
        RepairScenario::ApplyDeparts,
        include_bytes!("fixtures/header-repair-apply-departs.bin"),
    ),
];

/// `scenario`'s run under `input`'s Plan, after its scenario byte.
fn run(stem: &str, scenario: RepairScenario, input: &[u8]) -> RunReport {
    let ports = RepairHarness::prepare(&scenario);
    let plan = Plan::from_fuzz(&input[RepairHarness::SCENARIO_BYTES..]);
    let report =
        run_prepared::<RepairHarness>(&scenario, ports, plan, &mut MemorySink::default(), None)
            .unwrap_or_else(|error| panic!("{stem}: {error}"));
    assert_eq!(report.end.violation, None, "{stem}: {:?}", report.end);
    assert_eq!(report.end.harness, None, "{stem}: {:?}", report.end);
    report
}

/// The node's storage facts, in order.
fn storage(report: &RunReport) -> Vec<&StorageFact> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage { fact, .. } => Some(fact),
            _ => None,
        })
        .collect()
}

/// Each committed repair input holds both properties, and they apply, and
/// its repair's one apply commits at the header generation the completion
/// before it left.
#[test]
fn a_successful_repair_holds_commit_and_reset_properties() {
    for (stem, scenario, input) in INPUTS {
        let report = run(stem, scenario, input);
        for property in [COMMIT_ONLY_AUTHORIZED, RESET_INVALIDATES_WORK] {
            let outcome = report
                .properties
                .iter()
                .find(|reported| reported.property == property)
                .map(|reported| reported.outcome.clone());
            assert!(
                matches!(outcome, Some(PropertyOutcome::Held { applicable }) if applicable > 0),
                "{stem}: {property}: {outcome:?}"
            );
        }
        let facts = storage(&report);
        let applies: Vec<u64> = facts
            .iter()
            .filter_map(|fact| match fact {
                StorageFact::Submitted {
                    id,
                    request: StorageRequest::ApplyHeaderTarget { owner, .. },
                    ..
                } if owner.kind == "body-repair" => Some(*id),
                _ => None,
            })
            .collect();
        let [apply] = applies[..] else {
            panic!("{stem}: the repair applies once: {applies:?}");
        };
        let mut before = None;
        let mut applied = None;
        for fact in &facts {
            if let StorageFact::Completed {
                id,
                result,
                generation,
                ..
            } = fact
            {
                if *id == apply {
                    applied = Some((result.clone(), *generation, before));
                    break;
                }
                before = Some(*generation);
            }
        }
        let Some((result, generation, before)) = applied else {
            panic!("{stem}: the repair's apply completes");
        };
        assert_eq!(result, StorageResult::Applied, "{stem}");
        assert_eq!(
            Some(generation),
            before,
            "{stem}: the repair's apply leaves the header generation as it was"
        );
    }
}
