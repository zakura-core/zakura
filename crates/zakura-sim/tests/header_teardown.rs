//! hdr-6a: `header-sync/teardown-returns-baseline`'s round settlement on
//! `header-sync/storage`. Each released round returns its reservation and
//! leases once its calls are handled. The ownership ledger checks that at
//! the events, and the property reports a break at the step that found it.
#![cfg(zakura_sim)]

use zakura_sim::{
    domain::header_sync::TEARDOWN_RETURNS_BASELINE,
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
