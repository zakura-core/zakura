//! `header-sync/storage`'s scenario seeds, `header-storage-*` in
//! `zakura_sim::seed`. Each is a scenario's canonical prefix and 1,024 Plan
//! bytes, so a fuzz campaign starts from inputs that use the whole action
//! grammar instead of a unit that never reaches it.
//!
//! Most of these scenarios were first built for generator 2, and generator 3
//! encodes each as generator 2 did. So a generator-2 input differs from its
//! generator-3 seed only in the version byte, and [`grammar::migrate`]
//! carries it over with its meaning kept.
#![cfg(zakura_sim)]

use zakura_sim::{
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_sync::{grammar, Storage, StorageScenario},
        Subsystem,
    },
    seed::{self, Seed},
    NullSink, Outcome,
};

/// The generator before generator 3.
const OLD_GENERATOR: u8 = 2;

/// The seeds whose scenarios generator 2 could encode.
const MIGRATED: [&str; 7] = [
    "header-storage-default",
    "header-storage-close-relink",
    "header-storage-grow",
    "header-storage-holds",
    "header-storage-triggers",
    "header-storage-stale-prepare",
    BARE,
];

/// The bare default prefix, which runs the default Plan.
const BARE: &str = "header-sync-storage-default";

/// The scenario seeds: `header-sync/storage`'s seeds outside the fuzz-12
/// witnesses, `header-storage-w-*`, which
/// `tests/header_storage_witness_seeds.rs` runs.
fn scenario_seeds() -> Vec<Seed> {
    seed::all()
        .into_iter()
        .filter(|seed| seed.harness == Storage::NAME && !seed.name.starts_with("header-storage-w-"))
        .collect()
}

/// Each scenario seed but [`BARE`] carries 1,024 Plan bytes after its
/// prefix. Each runs to a defined end that the explorer classifies clean,
/// and a seed with control steps takes some.
#[test]
fn scenario_seeds_run_clean() {
    let entry = harness::find(Storage::NAME).unwrap();
    let seeds = scenario_seeds();
    assert_eq!(seeds.len(), 10);
    for seed in seeds {
        let name = &seed.name;
        let plan = seed.input().len() - Storage::SCENARIO_BYTES;
        assert_eq!(plan, if name == BARE { 0 } else { 1_024 }, "{name}");
        let scenario = Storage::draw(&seed.input()[..Storage::SCENARIO_BYTES]);
        let run = (entry.run_bytes)(seed.input(), &mut NullSink, None);
        let report = run
            .report()
            .unwrap_or_else(|| panic!("{name}: {:?}", run.result));
        assert!(
            matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
            "{name}: {:?}",
            report.end
        );
        assert_eq!(run.leaked(), 0, "{name}");
        assert_eq!(
            classify(entry, &run).outcome,
            ExploreOutcome::Clean,
            "{name}"
        );
        let controls = report
            .picks
            .iter()
            .filter(|pick| pick.identity.starts_with("peer:mirror-"))
            .count();
        assert_eq!(
            controls > 0,
            !scenario.controls.is_empty(),
            "{name}: took {controls} control steps"
        );
    }
}

/// Each seed generator 2 could encode, under version byte 2, is a
/// generator-2 input that migrates to the seed, Plan bytes and all.
/// Generator 2 decodes it to the scenario generator 3 decodes the seed to,
/// and generator 3 rejects its version byte and runs the default scenario.
#[test]
fn generator_2_inputs_migrate_to_their_seeds() {
    let prefix = Storage::SCENARIO_BYTES;
    for name in MIGRATED {
        let seed = seed::find(name).unwrap_or_else(|| panic!("{name} is registered"));
        let mut old = seed.input().to_vec();
        old[0] = OLD_GENERATOR;
        assert_eq!(
            grammar::v2::decode(&old[..prefix]).unwrap(),
            Storage::draw(&seed.input()[..prefix]),
            "{name}"
        );
        assert_eq!(grammar::migrate(&old).unwrap(), seed.input(), "{name}");
        assert_eq!(
            Storage::draw(&old[..prefix]),
            StorageScenario::default(),
            "{name}"
        );
    }
}
