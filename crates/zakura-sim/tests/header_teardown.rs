//! hdr-6a: `header-sync/teardown-returns-baseline`'s round settlement on
//! `header-sync/storage`. Each released round returns its reservation and
//! leases once its calls are handled. The ownership ledger checks that at
//! the events, and the property reports a break at the step that found it.
#![cfg(zakura_sim)]

use zakura_sim::{
    domain::header_sync::{CHUNK_BUDGET_BOUND, TEARDOWN_RETURNS_BASELINE},
    harness::{self, header_sync::Storage, Subsystem},
    witness::Expect,
    MemorySink, Plan,
};

/// The committed close-and-relink seed (`tests/header_storage_seeds.rs`),
/// whose node releases rounds as its mirrors close and finish.
const CLOSE_RELINK_INPUT: &[u8] = include_bytes!("fixtures/header-storage-close-relink.bin");

/// `header-sync/teardown-returns-baseline` holds on the committed
/// close-and-relink seed, and the run reaches its trigger history: the
/// queue releases a round. `cargo xtask planted
/// header-sync/teardown-returns-baseline` checks that the same input
/// violates it under each planted patch.
#[test]
fn the_close_relink_input_holds_teardown_returns_baseline() {
    let entry = harness::find(Storage::NAME).expect("the storage harness is registered");
    let (prefix, plan) = CLOSE_RELINK_INPUT.split_at(Storage::SCENARIO_BYTES);
    let result = (entry.witness)(
        &entry.scenario(prefix),
        Plan::from_fuzz(plan),
        TEARDOWN_RETURNS_BASELINE,
        Expect::Holds,
        &mut MemorySink::default(),
    );
    assert_eq!(result, Ok(()));
}

/// The two header-sync properties' reports and the full log depend only
/// on the scenario and the plan: the close-and-relink seed's scenario under
/// plan A twice, then B, then A again, gives A's every time, and B's log
/// differs. The reports count each step the round and budget evidence
/// judged, so they change with that evidence.
#[test]
fn round_evidence_repeats_a_a_and_a_b_a() {
    let entry = harness::find(Storage::NAME).expect("the storage harness is registered");
    let scenario = entry.scenario(&CLOSE_RELINK_INPUT[..Storage::SCENARIO_BYTES]);
    let plans: Vec<&[u8]> = vec![&CLOSE_RELINK_INPUT[Storage::SCENARIO_BYTES..], &[7; 256]];
    let runs: Vec<(String, String)> = [plans[0], plans[0], plans[1], plans[0]]
        .into_iter()
        .map(|plan| {
            let report = (entry.run)(&scenario, Plan::from_fuzz(plan), &mut MemorySink::default())
                .unwrap_or_else(|error| panic!("{error}"));
            let reports: Vec<_> = report
                .properties
                .iter()
                .filter(|property| {
                    [TEARDOWN_RETURNS_BASELINE, CHUNK_BUDGET_BOUND]
                        .contains(&property.property.as_str())
                })
                .collect();
            assert_eq!(reports.len(), 2, "{:?}", report.properties);
            (
                format!("{:?}", report.log.entries()),
                format!("{reports:?}"),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}
