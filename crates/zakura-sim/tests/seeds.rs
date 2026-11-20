//! The QA store's seed loader (explorer implementation plan, E3a-1): a
//! store's index and inputs read back as the seeds written, and the seed
//! check rejects a seed that doesn't fit its harness, witnesses or set.
//! These seeds are built in code and written to a temporary store: the real
//! seeds live in the Space, which repository tests don't read. With
//! `ZAKURA_QA_STORE` set, two more tests check the store's seeds and
//! planted cases.
#![cfg(zakura_sim)]

use std::{collections::BTreeSet, fs};

use zakura_sim::{
    harness::HARNESSES,
    planted,
    seed::{self, Index, Seed, SeedError, SeedSet},
    witness::Expect,
};

/// With `ZAKURA_QA_STORE` set, every stored seed in every set validates,
/// and no two share a name. The corpus holds seeds, since fuzz campaigns
/// and target checks start from it.
#[test]
fn every_stored_seed_validates() {
    if planted::store_root().is_none() {
        return;
    }
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


/// With `ZAKURA_QA_STORE` set, as the joint-run droplet's suite step sets
/// it, each case under the store's `qa/planted/` sits beside its patch and names a property
/// a harness reports. A seed case names a seed of any set that carries a
/// `holds` witness of the case's property, as `cargo xtask planted` checks
/// it unpatched. A typed case names a stored scenario, and a Plan file if
/// it gives one. A search case names a harness that reports the case's
/// property, and commits no input.
#[test]
fn planted_cases_name_their_witnesses() {
    let Some(root) = planted::store_root() else {
        return;
    };
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
        // A typed case runs a stored scenario under Plan bytes, a
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
    assert!(!cases.is_empty(), "the store holds the planted cases");
}

/// A corpus seed that passes [`seed::check`], to break one field at a time:
/// the verification-capacity harness's default Plan.
fn good() -> Seed {
    let seed = Seed::new(
        "discovery-verification-capacity",
        "discovery/verification-capacity",
        "crates/zakura-sim/tests/seeds.rs",
        vec![0],
    )
    .witness("discovery/verification-capacity", Expect::Holds);
    assert_eq!(seed::check(&seed), Ok(()));
    seed
}

/// A store's seeds read back with their harness, set, source, witnesses and
/// input, sorted by name, and load in their set.
#[test]
fn a_written_store_reads_back() {
    let store = tempfile::tempdir().expect("a temporary store");
    let slow = Seed::new("a-slow", "discovery/verification-capacity", "s.rs", vec![1])
        .in_set(SeedSet::Slow);
    Index::write(store.path(), &[good(), slow.clone()]).expect("the store is written");
    let seeds = seed::read(store.path()).expect("the store reads");
    let names: Vec<&str> = seeds.iter().map(|seed| seed.name.as_str()).collect();
    assert_eq!(names, ["a-slow", "discovery-verification-capacity"]);
    assert_eq!(seeds[0].set, SeedSet::Slow);
    assert_eq!(seeds[0].input(), slow.input());
    assert_eq!(seeds[1].witness, good().witness);
    assert_eq!(seeds[1].source, good().source);

    let corpus = seed::load_in(seeds.clone(), SeedSet::Corpus, None);
    assert_eq!(corpus.admitted.len(), 1, "{:?}", corpus.rejected);
    assert!(corpus.rejected.is_empty());
    let other = seed::load_in(seeds, SeedSet::Corpus, Some("discovery/wire"));
    assert!(other.admitted.is_empty());
    assert_eq!(other.other_harnesses, 1);
}

/// An input that isn't the one the index names fails the seed's check, and
/// a name that leaves the seed directory fails the read.
#[test]
fn a_changed_input_or_a_path_name_is_rejected() {
    let store = tempfile::tempdir().expect("a temporary store");
    Index::write(store.path(), &[good()]).expect("the store is written");
    let input = store
        .path()
        .join("seeds/discovery-verification-capacity.bin");
    fs::write(&input, [1]).expect("the input is rewritten");
    let seeds = seed::read(store.path()).expect("the store reads");
    assert!(matches!(
        seed::check(&seeds[0]),
        Err(SeedError::InputChanged { .. })
    ));
    let report = seed::load_in(seeds, SeedSet::Corpus, None);
    assert_eq!(report.rejected.len(), 1);

    let index = store.path().join("seeds/index.json");
    let text = fs::read_to_string(&index).expect("the index reads");
    fs::write(
        &index,
        text.replace("discovery-verification-capacity", "../escape"),
    )
    .expect("the index is rewritten");
    assert!(seed::read(store.path()).is_err());
}

/// Two seeds with one name are both rejected.
#[test]
fn a_duplicate_name_is_rejected() {
    let report = seed::load_in(vec![good(), good()], SeedSet::Corpus, None);
    assert!(report.admitted.is_empty());
    assert!(report
        .rejected
        .iter()
        .all(|failed| failed.error == SeedError::DuplicateName));
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
    // Generator 2's prefix, which the current header-storage generator
    // decodes as its fallback scenario.
    let seed = Seed::new("old", "header-sync/storage", "s.rs", vec![2]);
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
