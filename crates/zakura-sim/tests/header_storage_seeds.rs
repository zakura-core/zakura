//! `header-sync/storage`'s named scenarios, each built in code as its
//! canonical prefix, then 1,024 Plan bytes, so a run uses the whole action
//! grammar instead of a unit that never reaches it.
//!
//! Most of these scenarios were first built for generator 2, and generator 3
//! encodes each as generator 2 did. So a generator-2 input differs from its
//! generator-3 input only in the version byte, and [`grammar::migrate`]
//! carries it over with its meaning kept.
#![cfg(zakura_sim)]

mod support;

use support::header_inputs::{storage_input, storage_plans};
use zakura_sim::{
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_sync::{grammar, storage_witnesses, Storage, StorageScenario},
        Subsystem,
    },
    NullSink, Outcome,
};

/// The generator before generator 3.
const OLD_GENERATOR: u8 = 2;

/// How many Plans of [`storage_plans`] each scenario runs under.
const PLANS: usize = 2;

/// The scenarios generator 2 could encode, by name.
fn migrated() -> [(&'static str, StorageScenario); 5] {
    [
        ("default", StorageScenario::default()),
        ("close-relink", StorageScenario::close_relink()),
        ("grow", StorageScenario::grow()),
        ("holds", StorageScenario::holds()),
        ("triggers", StorageScenario::triggers()),
    ]
}

/// Every named scenario outside the fuzz-12 witnesses, which
/// `tests/header_storage_witness_seeds.rs` runs.
fn scenarios() -> Vec<(&'static str, StorageScenario)> {
    let mut scenarios = migrated().to_vec();
    scenarios.extend([
        ("competing-mirrors", storage_witnesses::competing_mirrors()),
        ("locator-dropped", storage_witnesses::dropped_locator()),
        ("apply-at-close", storage_witnesses::apply_at_close()),
    ]);
    scenarios
}

/// Each named scenario, under each of the first [`PLANS`] Plans, runs to a
/// defined end that the explorer classifies clean, and a scenario with
/// control steps takes some. The default scenario's bare prefix runs the
/// default Plan to the same kind of end.
#[test]
fn scenario_seeds_run_clean() {
    let entry = harness::find(Storage::NAME).unwrap();
    let plans = storage_plans();
    let bare = storage_input(&StorageScenario::default(), &[]);
    let inputs = scenarios()
        .into_iter()
        .flat_map(|(name, scenario)| {
            plans[..PLANS].iter().enumerate().map(move |(index, plan)| {
                (
                    format!("{name} under Plan {index}"),
                    storage_input(&scenario, plan),
                )
            })
        })
        .chain([("the bare default prefix".to_owned(), bare)]);
    for (name, input) in inputs {
        let scenario = Storage::draw(&input[..Storage::SCENARIO_BYTES]);
        let run = (entry.run_bytes)(&input, &mut NullSink, None);
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

/// Each scenario generator 2 could encode, under version byte 2, is a
/// generator-2 input that migrates to the generator-3 input, Plan bytes
/// and all. Generator 2 decodes it to the scenario generator 3 decodes the
/// generator-3 input to, and generator 3 rejects its version byte and runs
/// the default scenario.
#[test]
fn generator_2_inputs_migrate_to_their_seeds() {
    let prefix = Storage::SCENARIO_BYTES;
    let plan = &storage_plans()[0];
    let named = migrated()
        .into_iter()
        .map(|(name, scenario)| (name, storage_input(&scenario, plan)));
    let bare = (
        "bare default",
        storage_input(&StorageScenario::default(), &[]),
    );
    for (name, input) in named.chain([bare]) {
        let mut old = input.clone();
        old[0] = OLD_GENERATOR;
        assert_eq!(
            grammar::v2::decode(&old[..prefix]).unwrap(),
            Storage::draw(&input[..prefix]),
            "{name}"
        );
        assert_eq!(grammar::migrate(&old).unwrap(), input, "{name}");
        assert_eq!(
            Storage::draw(&old[..prefix]),
            StorageScenario::default(),
            "{name}"
        );
    }
}
