//! The seed registry (explorer implementation plan, E3a-1): every seed
//! builds and validates, and every planted case and work-list entry that
//! names a seed names one the registry builds.
#![cfg(zakura_sim)]

use std::{collections::BTreeSet, fs, path::Path};

use zakura_sim::{
    harness::HARNESSES,
    planted,
    seed::{self, Seed, SeedError, SeedSet},
    witness::Expect,
};

/// Every seed in every set validates, and no two share a name. The corpus
/// holds seeds, since fuzz campaigns and target checks start from it.
#[test]
fn every_seed_validates() {
    for set in [SeedSet::Corpus, SeedSet::KnownFailure, SeedSet::Slow] {
        let report = seed::load(set, None);
        let failures: Vec<String> = report
            .rejected
            .iter()
            .map(|failed| format!("{}: {}", failed.seed.name, failed.error))
            .collect();
        assert!(failures.is_empty(), "{set:?}: {failures:#?}");
    }
    assert!(!seed::load(SeedSet::Corpus, None).admitted.is_empty());
    let mut names = BTreeSet::new();
    for seed in seed::all() {
        assert!(
            names.insert(seed.name.clone()),
            "two seeds are named {}",
            seed.name
        );
    }
}

/// Each case under `qa/planted/` sits beside its patch and names a property
/// a harness reports. A seed case names a seed of any set that carries a
/// `holds` witness of the case's property, as `cargo xtask planted` checks
/// it unpatched. A typed case names a committed scenario, and a Plan file
/// if it gives one. A search case names a harness that reports the case's
/// property, and commits no input.
#[test]
fn planted_cases_name_their_witnesses() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let catalogue = root.join(planted::PLANTED_DIR);
    let seeds = seed::all();
    let reported: BTreeSet<String> = HARNESSES
        .iter()
        .flat_map(|entry| (entry.properties)())
        .map(|property| property.id.to_string())
        .collect();
    let patches = planted::files(&catalogue, "patch").unwrap();
    let cases = planted::files(&catalogue, "toml").unwrap();
    assert_eq!(
        cases.len(),
        patches.len(),
        "each case file has a patch, and each patch a case file"
    );
    for case in &cases {
        assert!(
            patches.contains(&case.with_extension("patch")),
            "{}: the runner finds no patch beside it",
            case.display()
        );
        let id = planted::property_id(&catalogue, case)
            .unwrap_or_else(|| panic!("{}: no property", case.display()));
        assert!(
            reported.contains(&id),
            "{}: no harness reports {id}",
            case.display()
        );
        let table: toml::Table = toml::from_str(&fs::read_to_string(case).unwrap())
            .unwrap_or_else(|error| panic!("{}: {error}", case.display()));
        // A typed case runs a committed scenario under Plan bytes, a
        // scripted Plan or the empty Plan, not a seed.
        if let Some(typed) = table.get("typed").and_then(toml::Value::as_table) {
            assert!(
                !(typed.contains_key("plan") && typed.contains_key("script")),
                "{}: both a plan and a script",
                case.display()
            );
            for file in ["scenario", "plan", "script"] {
                let Some(path) = typed.get(file).and_then(toml::Value::as_str) else {
                    assert_ne!(file, "scenario", "{}: no scenario", case.display());
                    continue;
                };
                assert!(root.join(path).is_file(), "{}: no {path}", case.display());
            }
            continue;
        }
        if let Some(search) = table.get("search").and_then(toml::Value::as_table) {
            let harness = search
                .get("harness")
                .and_then(toml::Value::as_str)
                .unwrap_or_else(|| panic!("{}: the search names no harness", case.display()));
            let entry = HARNESSES
                .iter()
                .find(|entry| entry.name == harness)
                .unwrap_or_else(|| panic!("{}: no harness is named {harness}", case.display()));
            assert!(
                (entry.properties)()
                    .iter()
                    .any(|property| property.id == id),
                "{}: {harness} doesn't report {id}",
                case.display()
            );
            assert!(
                !table.contains_key("seed") && !table.contains_key("typed"),
                "{}: a search case names no other witness",
                case.display()
            );
            continue;
        }
        let name = table["seed"]
            .as_str()
            .unwrap_or_else(|| panic!("{}: `seed` isn't a name", case.display()));
        let seed = seeds
            .iter()
            .find(|seed| seed.name == name)
            .unwrap_or_else(|| panic!("{}: no seed is named {name}", case.display()));
        assert!(
            seed.witness
                .iter()
                .any(|witness| witness.property == id && witness.expect == "holds"),
            "{}: {name} carries no `holds` witness of {id}",
            case.display()
        );
    }
    assert!(!cases.is_empty(), "the planted cases are committed");
}

/// A corpus seed that passes [`seed::check`], to break one field at a time.
fn good() -> Seed {
    let seed = seed::find("discovery-wait-complete").expect("the seed is registered");
    assert_eq!(seed::check(&seed), Ok(()));
    seed
}

/// A seed for an unregistered harness, or with input for another generator,
/// is rejected.
#[test]
fn a_seed_must_fit_its_harness() {
    let mut seed = good();
    seed.harness = "no/such-harness".to_owned();
    assert_eq!(
        seed::check(&seed),
        Err(SeedError::UnknownHarness("no/such-harness".to_owned()))
    );
    let seed = seed::find("header-storage-default").expect("the seed is registered");
    let mut input = seed.input().to_vec();
    // Generator 2's prefix, which the current header-storage generator
    // decodes as its fallback scenario.
    input[0] = 2;
    let seed = Seed::new(seed.name, &seed.harness, &seed.source, input);
    assert_eq!(seed::check(&seed), Err(SeedError::OtherGenerator));
}

/// A witness must name a registered property, once, and a seed must name
/// its source.
#[test]
fn bad_witnesses_are_rejected() {
    let seed = good().witness("discovery/no-such-property", Expect::Holds);
    assert_eq!(
        seed::check(&seed),
        Err(SeedError::Unregistered(
            "discovery/no-such-property".to_owned()
        ))
    );
    let property = good().witness[0].property.clone();
    let seed = good().witness(&property, Expect::Applicable);
    assert_eq!(
        seed::check(&seed),
        Err(SeedError::DuplicateWitness(property))
    );
    let mut seed = good();
    seed.source = " ".to_owned();
    assert_eq!(seed::check(&seed), Err(SeedError::NoSource));
}

/// A seed's witnesses fit its set: only a known failure expects a violation,
/// and a known failure must.
#[test]
fn witnesses_fit_the_set() {
    let mut violated = good();
    let property = violated.witness.remove(0).property;
    assert!(violated.witness.is_empty());
    let violated = violated.witness(&property, Expect::Violated);
    for set in [SeedSet::Corpus, SeedSet::Slow] {
        assert_eq!(
            seed::check(&violated.clone().in_set(set)),
            Err(SeedError::WrongSet(set))
        );
    }
    assert_eq!(seed::check(&violated.in_set(SeedSet::KnownFailure)), Ok(()));
    assert_eq!(
        seed::check(&good().in_set(SeedSet::KnownFailure)),
        Err(SeedError::WrongSet(SeedSet::KnownFailure))
    );
}
