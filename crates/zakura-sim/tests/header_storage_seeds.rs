//! `header-sync/storage`'s committed seeds, `tests/fixtures/header-storage-*`.
//! Each is a scenario's canonical prefix and 1,024 Plan bytes, so a fuzz
//! campaign starts from inputs that use the whole action grammar instead
//! of a unit that never reaches it.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf};

use support::byte_inputs;
use zakura_sim::{
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_sync::{grammar, Storage, StorageScenario},
        Subsystem,
    },
    seed, NullSink, Outcome,
};

/// The Plan bytes after each seed's prefix.
const PLAN_BYTES: usize = 1_024;

/// A seed's file stem and scenario.
type Seed = (&'static str, fn() -> StorageScenario);

/// Every seed.
const SEEDS: [Seed; 5] = [
    ("header-storage-default", StorageScenario::default),
    ("header-storage-close-relink", StorageScenario::close_relink),
    ("header-storage-grow", StorageScenario::grow),
    ("header-storage-holds", StorageScenario::holds),
    ("header-storage-triggers", StorageScenario::triggers),
];

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Seed `index`'s full fuzz input: its scenario's canonical prefix, then
/// its own slice of [`byte_inputs`].
fn input(index: usize, scenario: &StorageScenario) -> Vec<u8> {
    let stream = byte_inputs(64, 1_024).concat();
    let plan = stream
        .get(index * PLAN_BYTES..(index + 1) * PLAN_BYTES)
        .expect("64 inputs of up to 1,024 bytes hold every seed's Plan");
    grammar::input(scenario, plan).unwrap_or_else(|error| panic!("{scenario:?}: {error}"))
}

/// Each committed seed is its scenario's input, longer than the prefix, and
/// runs to a defined end that the explorer classifies clean. A seed with
/// control steps takes some.
#[test]
fn committed_seeds_are_their_scenarios_and_run_clean() {
    let entry = harness::find(Storage::NAME).unwrap();
    for (index, (stem, scenario)) in SEEDS.iter().enumerate() {
        let scenario = scenario();
        let committed = fs::read(fixtures().join(format!("{stem}.bin")))
            .unwrap_or_else(|error| panic!("{stem}: {error}"));
        assert_eq!(
            committed,
            input(index, &scenario),
            "{stem}: the committed seed changed; regenerate the seeds if that's intended"
        );
        assert!(committed.len() > Storage::SCENARIO_BYTES, "{stem}");
        assert_eq!(
            Storage::draw(&committed[..Storage::SCENARIO_BYTES]),
            scenario,
            "{stem}"
        );
        let run = (entry.run_bytes)(&committed, &mut NullSink, None);
        let report = run
            .report()
            .unwrap_or_else(|| panic!("{stem}: {:?}", run.result));
        assert!(
            matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
            "{stem}: {:?}",
            report.end
        );
        assert_eq!(run.leaked(), 0, "{stem}");
        assert_eq!(
            classify(entry, &run).outcome,
            ExploreOutcome::Clean,
            "{stem}"
        );
        let controls = report
            .picks
            .iter()
            .filter(|pick| pick.identity.starts_with("peer:mirror-"))
            .count();
        assert_eq!(
            controls > 0,
            !scenario.controls.is_empty(),
            "{stem}: took {controls} control steps"
        );
    }
}

/// Rewrites every seed and its sidecar. Run it with `RUSTFLAGS="--cfg
/// zakura_sim" cargo test -p zakura-sim --test header_storage_seeds --
/// --ignored regenerate_storage_seeds`.
#[test]
#[ignore = "rewrites the committed seeds"]
fn regenerate_storage_seeds() {
    for (index, (stem, scenario)) in SEEDS.iter().enumerate() {
        let scenario = scenario();
        let input = input(index, &scenario);
        let base = fixtures().join(stem);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let sidecar = format!(
            "# The committed fuzz input `{stem}.bin`: the harness that runs it, the\n\
             # generator version it was built for, and its SHA-256. Its 344-byte\n\
             # prefix is the scenario's canonical encoding, then 1,024 Plan bytes.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/header_storage_seeds.rs\"\n",
            harness = Storage::NAME,
            generator = Storage::GENERATOR,
            digest = seed::sha256(&input),
            scenario = seed::scenario_sha256(
                &serde_json::to_value(&scenario).expect("scenarios serialize")
            ),
        );
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
    }
}
