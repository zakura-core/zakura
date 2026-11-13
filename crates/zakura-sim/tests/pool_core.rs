//! M1.8: the pool core under the simulator. The scenario and its checks are
//! the `pool/core` harness. This file holds the passing witnesses and the
//! committed seed's builder.
#![cfg(zakura_sim)]

use zakura_sim::{
    domain::pool::{self, lock, SharedEvidence},
    harness::{
        pool::{PoolCoreHarness, PoolScenario},
        Subsystem,
    },
    witness::{witness_prepared, Expect},
    NullSink, PlanInput,
};

/// The committed seed: the default scenario's two prefix bytes, and no Plan
/// bytes, so every pick is the default.
const SEED: &[u8] = include_bytes!("fixtures/pool-core.bin");

/// The committed seed's scenario and Plan bytes.
fn seed() -> (PoolScenario, Vec<u8>) {
    let scenario = PoolCoreHarness::draw(&SEED[..PoolCoreHarness::SCENARIO_BYTES]);
    assert_eq!(
        scenario,
        PoolScenario::default(),
        "the seed runs the default scenario"
    );
    (scenario, SEED[PoolCoreHarness::SCENARIO_BYTES..].to_vec())
}

/// What the evidence shows, for a failure's message.
fn summary(shared: &SharedEvidence) -> Vec<String> {
    let evidence = lock(shared);
    vec![
        format!(
            "answered {} of {}",
            evidence.submits_answered, evidence.submits_started
        ),
        format!("charges {:?}", evidence.charges),
        format!("peak burst {}", evidence.peak_burst),
        format!("passed {:?}", evidence.passed),
        format!("removals {}", evidence.removals_seen),
        format!("mined {:?}", evidence.mined),
        format!("before reorg {:?}", evidence.before_reorg),
        format!("last visible {:?}", evidence.last_visible),
        format!("snapshots {}", evidence.snapshots_checked),
        format!("end {:?}", evidence.end),
    ]
}

/// Each pool property applies and holds on the committed seed, through its
/// trigger history.
#[test]
fn the_pool_core_seed_holds_every_pool_property() {
    let (scenario, plan) = seed();
    for property in pool::IDS {
        let shared = SharedEvidence::default();
        witness_prepared::<PoolCoreHarness>(
            &scenario,
            shared.clone(),
            PlanInput::Bytes(plan.clone()),
            property,
            Expect::Holds,
            &mut NullSink,
        )
        .unwrap_or_else(|error| panic!("{property}: {error}\n{:#?}", summary(&shared)));
    }
}

/// Writes the committed seed: the default scenario's prefix and no Plan
/// bytes.
#[test]
#[ignore = "regenerates a committed fixture"]
fn regenerate_pool_core_seed() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/pool-core.bin");
    std::fs::write(path, [5u8, 0b110]).expect("the fixture directory exists");
}
