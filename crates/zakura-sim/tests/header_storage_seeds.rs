//! `header-sync/storage`'s committed seeds, `tests/fixtures/header-storage-*`.
//! Each is a scenario's canonical prefix and 1,024 Plan bytes, so a fuzz
//! campaign starts from inputs that use the whole action grammar instead
//! of a unit that never reaches it.
//!
//! The three drawn witnesses `tests/explore_state.rs` reads were built for
//! generator 3 and name no source. Every other storage seed here was built
//! for generator 2 and migrated to generator 3 with its meaning kept. Each
//! generator-2 source is kept in `tests/fixtures/sources/`, named by its
//! digest, which the seed's sidecar names in `derived_from`. Generator 3
//! encodes each of their scenarios as generator 2 did, so a seed differs
//! from its source only in the version byte.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf};

use support::{byte_inputs, storage_witnesses};
use zakura_sim::{
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_sync::{grammar, Storage, StorageScenario},
        Subsystem,
    },
    seed, NullSink, Outcome,
};

/// The generator the storage seeds were built for before generator 3.
const SOURCE_GENERATOR: u8 = 2;

/// The kept generator-2 source whose digest is `sha256`, or `None` if
/// there's none.
fn kept_source(sha256: &str) -> Option<Vec<u8>> {
    fs::read(
        fixtures()
            .join(seed::SOURCES_DIR)
            .join(format!("{sha256}.bin")),
    )
    .ok()
}

/// The digest of `input`'s generator-2 source, which is kept: the same
/// bytes under version byte 2.
///
/// # Panics
///
/// If no such source is kept, since a seed that wasn't migrated has none.
fn source_digest(input: &[u8]) -> String {
    let mut source = input.to_vec();
    source[0] = SOURCE_GENERATOR;
    let digest = seed::sha256(&source);
    assert!(
        kept_source(&digest).is_some(),
        "no generator-2 source {digest} is kept, so the input wasn't migrated"
    );
    digest
}

/// The Plan bytes after each seed's prefix.
const PLAN_BYTES: usize = 1_024;

/// A seed's file stem, scenario, and which [`PLAN_BYTES`] slice of the
/// [`byte_inputs`] stream its Plan is.
type Seed = (&'static str, fn() -> StorageScenario, usize);

/// The witness each seed carries, by stem: a property its run holds, which a
/// planted case's patch on the same input violates.
const WITNESSES: [(&str, &str); 6] = [
    (
        "header-storage-close-relink",
        "header-sync/chunk-budget-bound",
    ),
    (
        "header-storage-close-relink",
        "header-sync/teardown-returns-baseline",
    ),
    (
        "header-storage-close-relink",
        "header-sync/outstanding-work-bounded",
    ),
    (
        "header-storage-stale-prepare",
        "header-sync/stale-completion-inert",
    ),
    ("header-storage-default", "harness/direct-dispatch"),
    (
        "header-storage-stale-prepare-steps",
        "header-sync/stale-completion-inert",
    ),
];

/// The seeds generator 3 is the first to draw, so they name no source: the
/// witness scenarios `tests/explore_state.rs` draws from their seeds, so
/// the fuzzer can reach the states they show, and the stale planted cases'
/// input ([`stale_prepare_steps`]).
const DRAWN: [Seed; 4] = [
    (
        "header-storage-locator-dropped",
        storage_witnesses::dropped_locator,
        12,
    ),
    (
        "header-storage-apply-at-close",
        storage_witnesses::apply_at_close,
        13,
    ),
    (
        "header-storage-competing-mirrors",
        storage_witnesses::competing_mirrors,
        10,
    ),
    (
        "header-storage-stale-prepare-steps",
        stale_prepare_steps,
        11,
    ),
];

/// `header-storage-stale-prepare`'s scenario less its last control step, a
/// close on a dropped read that never runs, under the same Plan slice: the
/// same run, with every step it declares carried out. The stale planted
/// cases run it, since acceptance judges their runs' declared actions.
fn stale_prepare_steps() -> StorageScenario {
    let mut scenario = StorageScenario::triggers();
    scenario
        .controls
        .pop()
        .expect("the trigger scenario has control steps");
    scenario
}

/// Every migrated seed. `header-storage-stale-prepare` is the trigger scenario
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
    for (stem, scenario, index) in SEEDS.into_iter().chain(DRAWN) {
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
    let migrated = SEEDS.into_iter().map(|seed| (seed, true));
    for ((stem, scenario, index), migrated) in migrated.chain(DRAWN.map(|seed| (seed, false))) {
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
        if migrated {
            sidecar.push_str(&format!(
                "\n[derived_from]\nsha256 = \"{}\"\ngenerator = {SOURCE_GENERATOR}\n",
                source_digest(&input)
            ));
        }
        for (_, property) in WITNESSES.iter().filter(|(seed, _)| *seed == stem) {
            sidecar.push_str(&format!(
                "\n[[witness]]\nproperty = \"{property}\"\nexpect = \"holds\"\n"
            ));
        }
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
    }
}

/// Every committed storage seed but the drawn witnesses is a generator-2
/// seed migrated with its meaning kept: its sidecar names a kept source by
/// digest and generator,
/// generator 2 decodes the source to the scenario generator 3 decodes the
/// seed to, migrating the source gives the seed back, Plan bytes and all,
/// and the two differ only in the version byte.
#[test]
fn committed_storage_seeds_are_migrated_generator_2_seeds() {
    let report = seed::load(&[fixtures()], Some(Storage::NAME)).expect("the fixtures list");
    assert!(report.rejected.is_empty(), "{:?}", report.rejected);
    let stems: Vec<String> = report
        .admitted
        .iter()
        .map(|seed| {
            seed.path
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    // `tests/header_storage_witness_seeds.rs` owns the item 7 witnesses.
    let witness = |stem: &str| stem.starts_with("header-storage-w-");
    let ours = stems.iter().filter(|stem| !witness(stem)).count();
    assert_eq!(ours, SEEDS.len() + DRAWN.len() + 1, "{stems:?}");
    assert!(stems.contains(&"header-sync-storage-default".to_owned()));
    for (seed, name) in report.admitted.iter().zip(&stems) {
        let stem = seed.path.display();
        if witness(name)
            || DRAWN
                .iter()
                .any(|(drawn, ..)| seed.path.ends_with(format!("{drawn}.bin")))
        {
            assert_eq!(seed.sidecar.derived_from, None, "{stem}");
            continue;
        }
        let from = seed
            .sidecar
            .derived_from
            .as_ref()
            .unwrap_or_else(|| panic!("{stem} names its source"));
        assert_eq!(from.generator, u32::from(SOURCE_GENERATOR), "{stem}");
        let source = kept_source(&from.sha256).unwrap_or_else(|| panic!("{stem}"));
        assert_eq!(seed::sha256(&source), from.sha256, "{stem}");
        let mut relabelled = seed.input.clone();
        relabelled[0] = SOURCE_GENERATOR;
        assert_eq!(source, relabelled, "{stem}");
        let prefix = Storage::SCENARIO_BYTES;
        assert_eq!(
            grammar::v2::decode(&source[..prefix]).unwrap(),
            Storage::draw(&seed.input[..prefix]),
            "{stem}"
        );
        assert_eq!(grammar::migrate(&source).unwrap(), seed.input, "{stem}");
        // The source no longer draws its scenario: generator 3 rejects its
        // version byte, and it runs the default scenario.
        assert_eq!(
            Storage::draw(&source[..prefix]),
            StorageScenario::default(),
            "{stem}"
        );
    }
}
