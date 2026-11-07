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

/// A seed's file stem, scenario, and which [`PLAN_BYTES`] slice of the
/// [`byte_inputs`] stream its Plan is.
type Seed = (&'static str, fn() -> StorageScenario, usize);

/// The witness each seed carries, by stem: a property its run holds, which a
/// planted case's patch on the same input violates.
const WITNESSES: [(&str, &str); 3] = [
    (
        "header-storage-close-relink",
        "header-sync/chunk-budget-bound",
    ),
    (
        "header-storage-close-relink",
        "header-sync/teardown-returns-baseline",
    ),
    (
        "header-storage-stale-prepare",
        "header-sync/stale-completion-inert",
    ),
];

/// Every seed. `header-storage-stale-prepare` is the trigger scenario
/// under the slice whose run ends a prepare after its round retired, while
/// the same peer's next round is active.
const SEEDS: [Seed; 6] = [
    ("header-storage-default", StorageScenario::default, 0),
    (
        "header-storage-close-relink",
        StorageScenario::close_relink,
        1,
    ),
    ("header-storage-grow", StorageScenario::grow, 2),
    ("header-storage-holds", StorageScenario::holds, 3),
    ("header-storage-triggers", StorageScenario::triggers, 4),
    (
        "header-storage-stale-prepare",
        StorageScenario::triggers,
        11,
    ),
];

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A seed's full fuzz input: its scenario's canonical prefix, then slice
/// `index` of [`byte_inputs`].
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
    for (stem, scenario, index) in SEEDS {
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
    for (stem, scenario, index) in SEEDS {
        let scenario = scenario();
        let input = input(index, &scenario);
        let base = fixtures().join(stem);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let mut sidecar = format!(
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
        for (_, property) in WITNESSES.iter().filter(|(seed, _)| *seed == stem) {
            sidecar.push_str(&format!(
                "\n[[witness]]\nproperty = \"{property}\"\nexpect = \"holds\"\n"
            ));
        }
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
    }
}
