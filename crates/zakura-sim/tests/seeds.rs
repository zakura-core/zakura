//! The committed fuzz seeds and their validation against the current
//! generators (explorer implementation plan, E3a-1).
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;
use support::storage_witnesses;
use tempfile::TempDir;
use zakura_sim::{
    harness::{
        block_sync::BlockScenario,
        discovery, discovery_service,
        fixtures::{PickScenario, Variant, Which},
        header_sync::{FloodScenario, RepairScenario, StorageScenario, WakeScenario},
        sync::SavedSync,
        sync_wire::{self, SavedWire, CURATED},
        wire_plans::SavedPlan,
        HARNESSES,
    },
    planted,
    seed::{self, SeedError, SeedKind, SeedReport},
};

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// [`seed::load`] on `dirs`, which list.
fn load(dirs: &[PathBuf], harness: Option<&str>) -> SeedReport {
    seed::load(dirs, harness).expect("the directories list")
}

/// Every committed seed passes validation, and none is left out.
#[test]
fn committed_seeds_validate() {
    let report = load(&[fixtures()], None);
    let failures: Vec<String> = report
        .rejected
        .iter()
        .chain(&report.quarantined)
        .map(|failed| format!("{}: {}", failed.path.display(), failed.error))
        .collect();
    assert!(failures.is_empty(), "{failures:#?}");
    let inputs = fs::read_dir(fixtures())
        .expect("the fixture directory lists")
        .filter(|entry| {
            entry
                .as_ref()
                .expect("the directory lists")
                .path()
                .extension()
                .is_some_and(|e| e == seed::SEED_EXTENSION)
        })
        .count();
    assert_eq!(report.admitted.len(), inputs);
    assert!(inputs > 0, "the S4.7 input is committed");
    assert!(report.admitted.iter().all(|seed| seed.sidecar.required));
}

/// The scenario each committed seed is meant to run, by its file's stem,
/// built with the scenario's own constructor rather than the generator.
fn intended(stem: &str) -> Option<serde_json::Value> {
    fn value(scenario: impl Serialize) -> Option<serde_json::Value> {
        Some(serde_json::to_value(scenario).expect("scenarios serialize"))
    }
    if let Some(plan) = SavedPlan::ALL
        .into_iter()
        .find(|plan| stem == format!("header-wire-{}", plan.name()))
    {
        return value(plan.scenario());
    }
    if let Some(plan) = discovery_service::SavedPlan::ALL
        .into_iter()
        .find(|plan| stem == format!("discovery-wire-{}", plan.name()))
    {
        return value(plan.scenario());
    }
    if let Some(fixture) = Which::ALL.into_iter().find(|fixture| {
        stem == format!(
            "race-fixtures-{}-fixed",
            fixture.property().trim_start_matches("race/")
        )
    }) {
        return value(PickScenario {
            fixture,
            variant: Variant::Fixed,
            workers: 1,
        });
    }
    if let Some(saved) = SavedWire::ALL
        .into_iter()
        .find(|saved| stem == format!("sync-wire-{}", saved.name()))
    {
        return value(saved.scenario());
    }
    if let Some((_, prefix)) = CURATED
        .into_iter()
        .find(|(name, _)| stem == format!("sync-wire-{name}"))
    {
        return value(sync_wire::combined(&prefix));
    }
    if let Some(saved) = SavedSync::ALL
        .into_iter()
        .find(|saved| stem == format!("sync-storage-{}", saved.name()))
    {
        return value(saved.scenario());
    }
    if let Some((_, scenario)) = storage_witnesses::witness_seeds()
        .into_iter()
        .find(|(witness, _)| witness == stem)
    {
        return value(scenario);
    }
    match stem {
        "header-sync-storage-default" => value(StorageScenario::default()),
        "block-reactor-identity" => value(zakura_sim::record::machine::MachineScenario {
            input: vec![0, 0, 1, 200, 1, 0, 1, 0, 4, 0, 30, 0, 0, 9, 1, 200],
        }),
        "block-reactor-policy" => value(zakura_sim::record::machine::MachineScenario {
            input: vec![
                0, 0, 1, 200, 0, 8, 2, 200, 7, 0, 200, 0, 3, 0, 0, 0, 1, 0, 0, 1, 6, 2, 1, 0, 6, 3,
                0, 0, 5, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 200, 2, 0, 1, 0, 7, 0, 200, 0, 3, 0, 0, 0,
                4, 0, 30, 0, 6, 0, 3, 0,
            ],
        }),
        "block-work-ownership" => value(zakura_sim::record::machine::MachineScenario {
            input: vec![
                0, 0, 0, 9, 1, 0, 0, 1, 2, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 9, 1, 0, 0, 1, 2, 1, 0, 0,
                5, 0, 0, 0, 4, 1, 0, 0, 5, 1, 0, 0,
            ],
        }),
        "header-reactor-policy" => value(zakura_sim::record::machine::MachineScenario {
            input: include_bytes!("fixtures/header-reactor-policy.bin").to_vec(),
        }),
        "header-publisher-policy" => value(zakura_sim::record::machine::MachineScenario {
            input: include_bytes!("fixtures/header-publisher-policy.bin").to_vec(),
        }),
        "block-work-funding" => value(zakura_sim::record::machine::MachineScenario {
            input: include_bytes!("fixtures/block-work-funding.bin").to_vec(),
        }),
        "block-decoded-retention" => value(zakura_sim::record::machine::MachineScenario {
            input: include_bytes!("fixtures/block-decoded-retention.bin").to_vec(),
        }),
        "block-work-owner-cap" => value(zakura_sim::record::machine::MachineScenario {
            input: include_bytes!("fixtures/block-work-owner-cap.bin").to_vec(),
        }),
        "discovery-wait-complete" => value(discovery::Scenario::default()),
        "discovery-verification-capacity" => value(discovery::CapacityScenario::default()),
        "discovery-record-book" => value(zakura_sim::record::machine::MachineScenario {
            input: include_bytes!("fixtures/discovery-record-book.bin").to_vec(),
        }),
        "discovery-session-installation" => value(zakura_sim::record::machine::MachineScenario {
            input: include_bytes!("fixtures/discovery-session-installation.bin").to_vec(),
        }),
        "block-sync-handoff" => value(BlockScenario::default()),
        "block-sync-stuck-floor" => value(BlockScenario::stuck_floor()),
        "block-sync-paused-handoffs" | "block-sync-floor-lane-race" => {
            value(BlockScenario::paused_handoffs())
        }
        "block-sync-above-window" => value(BlockScenario::above_window()),
        "block-sync-peers-leave" => value(BlockScenario::peers_leave()),
        "block-sync-reset-mid-flight" | "block-sync-stale-request-body" => {
            value(BlockScenario::reset_mid_flight())
        }
        "block-sync-stale-beside-current" => value(BlockScenario::stale_beside_current()),
        "block-sync-stalled-reset" => value(BlockScenario::stalled_reset()),
        "block-sync-decoded-input-stall" => value(BlockScenario::decoded_input_stall()),
        "block-sync-capacity-frontier-first" | "block-sync-capacity-reply-first" => {
            value(BlockScenario::capacity_release())
        }
        "header-storage-default" => value(StorageScenario::default()),
        "header-storage-close-relink" => value(StorageScenario::close_relink()),
        "header-storage-grow" => value(StorageScenario::grow()),
        "header-storage-holds" => value(StorageScenario::holds()),
        "header-storage-triggers" | "header-storage-stale-prepare" => {
            value(StorageScenario::triggers())
        }
        "header-storage-locator-dropped" => value(storage_witnesses::dropped_locator()),
        "header-storage-apply-at-close" => value(storage_witnesses::apply_at_close()),
        "header-storage-competing-mirrors" => value(storage_witnesses::competing_mirrors()),
        "header-release-wake" => value(WakeScenario::Release),
        "header-release-wake-deferred" => value(WakeScenario::Deferred),
        "header-flood-wire" => value(FloodScenario::Wire),
        "header-flood-lifecycle" => value(FloodScenario::Lifecycle),
        "header-repair-applied" => value(RepairScenario::Applied),
        "header-repair-prepare-departs" => value(RepairScenario::PrepareDeparts),
        "header-repair-apply-departs" => value(RepairScenario::ApplyDeparts),
        "header-repair-supplier-pruned" => value(RepairScenario::SupplierPruned),
        _ => None,
    }
}

/// Each committed seed's sidecar records its intended scenario, so a seed
/// that passes validation decodes to that scenario. A new seed needs a row
/// in [`intended`].
#[test]
fn committed_seeds_record_their_intended_scenarios() {
    let report = load(&[fixtures()], None);
    for seed in &report.admitted {
        let stem = seed.path.file_stem().and_then(|s| s.to_str()).unwrap();
        let scenario = intended(stem).unwrap_or_else(|| panic!("{stem} has no intended scenario"));
        assert_eq!(
            seed.sidecar.scenario_sha256,
            seed::scenario_sha256(&scenario),
            "{stem}"
        );
    }
}

/// Each planted case's committed input is a seed whose sidecar carries a
/// witness of the case's property expecting `holds`, as `cargo xtask
/// planted` checks it unpatched. The test reads every case file at any
/// depth under `qa/planted/`, and each must sit beside a patch the runner
/// finds, so no case escapes either. Each case's property is one a
/// harness reports.
#[test]
fn planted_inputs_carry_their_witnesses() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let catalogue = root.join(planted::PLANTED_DIR);
    let report = load(&[fixtures()], None);
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
        // A typed case runs a committed scenario, not a seed.
        if let Some(typed) = table.get("typed").and_then(toml::Value::as_table) {
            for file in ["scenario", "plan"] {
                let path = typed[file].as_str().unwrap();
                assert!(root.join(path).is_file(), "{}: no {path}", case.display());
            }
            continue;
        }
        let input = root.join(table["input"].as_str().unwrap());
        let seed = report
            .admitted
            .iter()
            .find(|seed| seed.path == fixtures().join(input.file_name().unwrap()))
            .unwrap_or_else(|| panic!("{}: its input is no seed", case.display()));
        assert!(
            seed.sidecar
                .witness
                .iter()
                .any(|witness| witness.property == id && witness.expect == "holds"),
            "{}: {} carries no `holds` witness of {id}",
            case.display(),
            seed.path.display()
        );
    }
    assert!(!cases.is_empty(), "the planted cases are committed");
}

/// A directory holding one seed, `seed.bin`, built from a committed seed.
struct Scratch {
    dir: TempDir,
}

impl Scratch {
    /// Copies the committed seed `stem` and its sidecar, rewritten by
    /// `edit`.
    fn copy(stem: &str, edit: impl FnOnce(String) -> String) -> Scratch {
        let scratch = Scratch {
            dir: TempDir::new().unwrap(),
        };
        let bytes = fs::read(fixtures().join(format!("{stem}.bin"))).unwrap();
        let sidecar = fs::read_to_string(fixtures().join(format!("{stem}.toml"))).unwrap();
        scratch.write(&bytes, Some(&edit(sidecar)));
        scratch
    }

    /// Writes `seed.bin`, and `seed.toml` if given.
    fn write(&self, bytes: &[u8], sidecar: Option<&str>) {
        fs::write(self.dir.path().join("seed.bin"), bytes).unwrap();
        if let Some(sidecar) = sidecar {
            fs::write(self.dir.path().join("seed.toml"), sidecar).unwrap();
        }
    }

    /// [`load`] on the directory.
    fn load(&self, harness: Option<&str>) -> SeedReport {
        load(&[self.dir.path().to_owned()], harness)
    }

    /// The one seed's failure, which must be a rejection.
    fn rejected(&self) -> SeedError {
        let report = self.load(None);
        assert!(report.admitted.is_empty() && report.quarantined.is_empty());
        let [failed] = <[_; 1]>::try_from(report.rejected).expect("one seed is rejected");
        failed.error
    }
}

/// The sidecar of the committed seed `stem`, with `from` replaced by `to`.
fn replace(from: &'static str, to: &'static str) -> impl FnOnce(String) -> String {
    move |sidecar: String| {
        assert!(sidecar.contains(from), "{from}");
        sidecar.replacen(from, to, 1)
    }
}

/// A seed built for another generator is rejected with both versions.
#[test]
fn another_generator_is_rejected() {
    let scratch = Scratch::copy(
        "discovery-wait-complete",
        replace("generator = 1", "generator = 7"),
    );
    assert_eq!(
        scratch.rejected(),
        SeedError::Generator {
            seed: 7,
            harness: 1
        }
    );
}

/// A seed whose bytes changed is rejected with both digests.
#[test]
fn changed_bytes_are_rejected() {
    let scratch = Scratch::copy("discovery-wait-complete", |sidecar| sidecar);
    let mut bytes = fs::read(fixtures().join("discovery-wait-complete.bin")).unwrap();
    bytes.push(0);
    scratch.write(&bytes, None);
    assert!(
        matches!(scratch.rejected(), SeedError::Digest { actual, .. } if actual == seed::sha256(&bytes))
    );
}

/// A seed without a sidecar, and a sidecar without a seed, are rejected.
#[test]
fn missing_halves_are_rejected() {
    let scratch = Scratch::copy("discovery-wait-complete", |sidecar| sidecar);
    fs::remove_file(scratch.dir.path().join("seed.toml")).unwrap();
    assert_eq!(scratch.rejected(), SeedError::NoSidecar);
    let scratch = Scratch::copy("discovery-wait-complete", |sidecar| sidecar);
    fs::remove_file(scratch.dir.path().join("seed.bin")).unwrap();
    assert_eq!(scratch.rejected(), SeedError::NoInput);
}

/// A sidecar with an unknown field, or one naming no harness, is rejected
/// even when the caller asks for another harness's seeds.
#[test]
fn unreadable_sidecars_are_rejected_for_every_harness() {
    let scratch = Scratch::copy(
        "discovery-wait-complete",
        replace("source =", "sauce = \"x\"\nsource ="),
    );
    assert!(matches!(scratch.rejected(), SeedError::Sidecar(_)));
    let scratch = Scratch::copy(
        "discovery-wait-complete",
        replace("\"discovery/wait-complete\"", "\"discovery/nowhere\""),
    );
    let report = scratch.load(Some("block-sync/storage"));
    assert_eq!(report.other_harnesses, 0);
    assert_eq!(
        report.rejected[0].error,
        SeedError::UnknownHarness("discovery/nowhere".to_owned())
    );
}

/// A seed whose prefix decodes to another scenario than its sidecar records
/// is rejected with the decoded scenario's digest.
#[test]
fn another_scenario_is_rejected() {
    let reset = fs::read_to_string(fixtures().join("block-sync-reset-mid-flight.toml")).unwrap();
    let reset_digest = toml::from_str::<seed::Sidecar>(&reset)
        .unwrap()
        .scenario_sha256;
    let scratch = Scratch::copy("block-sync-stale-beside-current", |sidecar| {
        let stale = toml::from_str::<seed::Sidecar>(&sidecar).unwrap();
        sidecar.replace(&stale.scenario_sha256, &reset_digest)
    });
    let stale = load(&[fixtures()], Some("block-sync/storage"))
        .admitted
        .into_iter()
        .find(|seed| seed.path.ends_with("block-sync-stale-beside-current.bin"))
        .unwrap();
    assert_eq!(
        scratch.rejected(),
        SeedError::Scenario {
            recorded: reset_digest,
            decoded: stale.sidecar.scenario_sha256,
        }
    );
}

/// A witness must name a registered property, once, with an expectation.
#[test]
fn bad_witnesses_are_rejected() {
    let scratch = Scratch::copy(
        "discovery-wait-complete",
        replace(
            "discovery/wait-complete-wakes",
            "discovery/no-such-property",
        ),
    );
    assert_eq!(
        scratch.rejected(),
        SeedError::Unregistered("discovery/no-such-property".to_owned())
    );
    let scratch = Scratch::copy(
        "discovery-wait-complete",
        replace("expect = \"holds\"", "expect = \"maybe\""),
    );
    assert!(matches!(scratch.rejected(), SeedError::Expect { .. }));
    let scratch = Scratch::copy("discovery-wait-complete", |sidecar| {
        format!(
            "{sidecar}\n[[witness]]\nproperty = \"discovery/wait-complete-wakes\"\n\
             expect = \"violated\"\n"
        )
    });
    assert_eq!(
        scratch.rejected(),
        SeedError::DuplicateWitness("discovery/wait-complete-wakes".to_owned())
    );
    let scratch = Scratch::copy(
        "discovery-wait-complete",
        replace(
            "source = \"crates/zakura-sim/tests/witness.rs\"",
            "source = \" \"",
        ),
    );
    assert_eq!(scratch.rejected(), SeedError::NoSource);
}

/// The committed stale-beside-current seed as a `plan` seed: its Plan bytes
/// alone, with `prefix` as the given sidecar text.
fn plan_seed(prefix: &str) -> Scratch {
    let scratch = Scratch {
        dir: TempDir::new().unwrap(),
    };
    let input = fs::read(fixtures().join("block-sync-stale-beside-current.bin")).unwrap();
    let plan = &input[2..];
    let sidecar = fs::read_to_string(fixtures().join("block-sync-stale-beside-current.toml"))
        .unwrap()
        .replace(&seed::sha256(&input), &seed::sha256(plan))
        .replacen("source =", &format!("kind = \"plan\"\n{prefix}source ="), 1);
    scratch.write(plan, Some(&sidecar));
    scratch
}

/// A `plan` seed joins its prefix to its Plan bytes. Its prefix must decode
/// to the recorded scenario, and it can't be left out.
#[test]
fn plan_seeds_take_their_scenario_from_the_prefix() {
    let report = plan_seed("prefix = \"ff06\"\n").load(None);
    let [admitted] = <[_; 1]>::try_from(report.admitted).expect("the plan seed is admitted");
    assert_eq!(admitted.sidecar.kind, SeedKind::Plan);
    assert_eq!(
        admitted.input,
        fs::read(fixtures().join("block-sync-stale-beside-current.bin")).unwrap()
    );
    // `ff 0c` is reset-mid-flight's tag.
    assert!(matches!(
        plan_seed("prefix = \"ff0c\"\n").rejected(),
        SeedError::Scenario { .. }
    ));
    assert_eq!(plan_seed("").rejected(), SeedError::NoPrefix);
    assert_eq!(
        plan_seed("prefix = \"ff\"\n").rejected(),
        SeedError::Prefix {
            prefix: "ff".to_owned(),
            scenario_bytes: 2
        }
    );
    let scratch = Scratch::copy(
        "block-sync-stale-beside-current",
        replace("source =", "prefix = \"ff06\"\nsource ="),
    );
    assert_eq!(scratch.rejected(), SeedError::PrefixOnInput);
}

/// An optional seed that fails is quarantined with its reason, and a seed
/// of another harness than the one asked for is only counted. An optional
/// seed naming no registered harness is quarantined whichever harness is
/// asked for.
#[test]
fn optional_seeds_are_quarantined_and_other_harnesses_counted() {
    let scratch = Scratch::copy(
        "discovery-wait-complete",
        replace("generator = 1", "generator = 7\nrequired = false"),
    );
    let report = scratch.load(None);
    assert!(report.admitted.is_empty() && report.rejected.is_empty());
    assert_eq!(
        report.quarantined[0].error,
        SeedError::Generator {
            seed: 7,
            harness: 1
        }
    );
    let report = scratch.load(Some("block-sync/storage"));
    assert_eq!(report.other_harnesses, 1);
    assert!(report.quarantined.is_empty());
    let scratch = Scratch::copy(
        "discovery-wait-complete",
        replace(
            "\"discovery/wait-complete\"",
            "\"discovery/retired\"\nrequired = false",
        ),
    );
    let report = scratch.load(Some("block-sync/storage"));
    assert!(report.admitted.is_empty() && report.rejected.is_empty());
    assert_eq!(report.other_harnesses, 0);
    assert_eq!(
        report.quarantined[0].error,
        SeedError::UnknownHarness("discovery/retired".to_owned())
    );
}

/// A sidecar edit.
type Edit = Box<dyn Fn(String) -> String>;

/// A copy of the committed `header-storage-default` seed, its sidecar with
/// `edit` applied, its bytes with `bytes` applied, and every kept source, in
/// a new directory, and the errors validating it gives.
fn migrated_default(
    edit: impl Fn(String) -> String,
    bytes: impl Fn(&mut Vec<u8>),
) -> Vec<SeedError> {
    let dir = TempDir::new().unwrap();
    let mut input = fs::read(fixtures().join("header-storage-default.bin")).unwrap();
    let sidecar = fs::read_to_string(fixtures().join("header-storage-default.toml")).unwrap();
    let digest = seed::sha256(&input);
    bytes(&mut input);
    let sidecar = edit(sidecar.replace(&digest, &seed::sha256(&input)));
    fs::write(dir.path().join("seed.bin"), &input).unwrap();
    fs::write(dir.path().join("seed.toml"), sidecar).unwrap();
    let sources = dir.path().join(seed::SOURCES_DIR);
    fs::create_dir(&sources).unwrap();
    for source in fs::read_dir(fixtures().join(seed::SOURCES_DIR)).unwrap() {
        let source = source.unwrap().path();
        fs::copy(&source, sources.join(source.file_name().unwrap())).unwrap();
    }
    let report = load(&[dir.path().to_path_buf()], None);
    report
        .rejected
        .into_iter()
        .map(|failed| failed.error)
        .collect()
}

/// A migrated seed's `derived_from` must name a kept source that migrates to
/// the seed. One naming another seed's source, one naming a source that
/// isn't kept, and one claiming the generator the seed is at are all
/// rejected, and the unchanged copy is admitted.
#[test]
fn derived_seeds_must_migrate_from_their_kept_source() {
    let keep = |sidecar: String| sidecar;
    assert_eq!(migrated_default(keep, |_| {}), []);
    const DEFAULT_SOURCE: &str = "251c8894a914f36210e128139a57ffbb34a6ea871511f1d91387ace64e538148";
    const GROW_SOURCE: &str = "13a6f50ed23fbeabd2398d65f6da2ec0807869b1b64f185031c14b5ead5e7ee5";
    let cases: [(Edit, &str); 3] = [
        (
            Box::new(|sidecar: String| sidecar.replace(DEFAULT_SOURCE, GROW_SOURCE)),
            "migrates to other bytes",
        ),
        (
            Box::new(|sidecar: String| sidecar.replace(DEFAULT_SOURCE, &"ab".repeat(32))),
            "names no kept source",
        ),
        (
            Box::new(|sidecar: String| sidecar.replace("generator = 2", "generator = 3")),
            "not one older",
        ),
    ];
    for (edit, reason) in cases {
        let [error] = &migrated_default(edit, |_| {})[..] else {
            panic!("{reason}: one rejection");
        };
        assert!(matches!(error, SeedError::Derived(_)), "{error:?}");
        assert!(
            error.to_string().contains(reason),
            "{error} doesn't say {reason}"
        );
    }
}

/// A seed whose prefix carries another generator's version byte is
/// rejected, though the harness would draw the default scenario for it and
/// the sidecar records that scenario's digest.
#[test]
fn a_seed_for_another_generator_is_rejected() {
    let drop_source =
        |sidecar: String| sidecar[..sidecar.find("[derived_from]").unwrap()].to_owned();
    assert_eq!(
        migrated_default(drop_source, |input| input[0] = 2),
        [SeedError::OtherGenerator]
    );
}
