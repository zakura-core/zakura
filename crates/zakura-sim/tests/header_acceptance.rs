//! hdr-14's acceptance manifest: its static checks against the registry,
//! `COVERAGE` and the committed directories, each with a control that one
//! defect makes it refuse (Rule 16).
//!
//! Every test here is listed in `COMPOSITION_CHECKS`, so the acceptance run
//! executes each by its exact name at the subject commit.
#![cfg(zakura_sim)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use zakura_sim::{
    harness::{
        self,
        coverage::{Bypass, COVERAGE},
        header_acceptance::{
            self,
            contracts::{self, Contract, ContractId, ContractState, NarrowingTest},
            required::{gate_rule, required_keys, Inventories, Registered, GATE_OBLIGATIONS},
            rows::{self, NativeClass, HEADER_HARNESSES},
            sim_test, CaseRef, CaseRun, Expected, ExpectedId, ExpectedResult, Found, Gate,
            HistoryRef, Key, Manifest, RunId, TestRef, Tier, COMPOSITION_CHECKS, GATE_REQUIREMENTS,
            NARROWING, NATIVE_MUTATIONS, NESTS, REQUIRED_CONTRACTS,
        },
        HARNESSES,
    },
    property::Status,
};

/// The repository root.
fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every header ID the header harnesses register.
fn registered_ids() -> BTreeSet<String> {
    header_acceptance::registered()
        .into_iter()
        .map(|registration| registration.property)
        .collect()
}

/// The header harnesses that register `property`. The sim-wide IDs are
/// registered everywhere, and the manifest covers only header harnesses.
fn registering(property: &str) -> BTreeSet<&'static str> {
    HARNESSES
        .iter()
        .filter(|entry| HEADER_HARNESSES.contains(&entry.name))
        .filter(|entry| entry.status(property).is_some())
        .map(|entry| entry.name)
        .collect()
}

#[test]
fn the_rows_equal_the_registered_header_ids() {
    let rows = header_acceptance::rows();
    let listed: BTreeSet<String> = rows.iter().map(|row| row.property.to_owned()).collect();
    assert_eq!(listed.len(), rows.len(), "one row per property");
    assert_eq!(listed, registered_ids());
    for row in &rows {
        assert!(
            gate_rule(row.property).is_some(),
            "{}: no gate rule",
            row.property
        );
        // Both ways: every harness the row names registers it, and every
        // harness that registers it is named.
        let named: BTreeSet<&str> = row.harnesses.iter().copied().collect();
        assert_eq!(named, registering(row.property), "{}", row.property);
        for registration in header_acceptance::registered()
            .iter()
            .filter(|registration| registration.property == row.property)
        {
            assert_eq!(registration.status, row.status, "{}", row.property);
        }
        assert_eq!(
            row.status == Status::Draft,
            row.promotion.is_some(),
            "{}: a Draft names its promotion",
            row.property
        );
    }
    assert_eq!(tier_errors(&rows), Vec::<String>::new());
    // A control: the reactor row's tier changed from Machine to Sim.
    let mut retiered = rows.clone();
    let at = retiered
        .iter()
        .position(|row| row.property == "header/reactor-policy")
        .unwrap();
    assert_eq!(retiered[at].tier, Tier::Machine);
    retiered[at].tier = Tier::Sim;
    assert_eq!(tier_errors(&retiered).len(), 1);
}

/// Every row whose tier isn't its harnesses': `Machine` when every harness
/// that registers it is a machine harness
/// ([`HarnessEntry::machine`](harness::HarnessEntry::machine)),
/// `Sim` when none is, and an error when they're mixed.
fn tier_errors(rows: &[header_acceptance::Row]) -> Vec<String> {
    let mut errors = Vec::new();
    for row in rows {
        let machine: BTreeSet<bool> = row
            .harnesses
            .iter()
            .map(|name| {
                HARNESSES
                    .iter()
                    .find(|entry| entry.name == *name)
                    .is_some_and(|entry| entry.machine)
            })
            .collect();
        let tier = match machine.iter().collect::<Vec<_>>().as_slice() {
            [true] => Some(Tier::Machine),
            [false] => Some(Tier::Sim),
            _ => None,
        };
        if tier != Some(row.tier) {
            errors.push(format!(
                "{}: tier {:?}, harnesses {tier:?}",
                row.property, row.tier
            ));
        }
    }
    errors
}

/// The harnesses that are header harnesses by their own registrations: a
/// name or a registered ID in the header families.
fn header_harnesses_by_registration() -> BTreeSet<&'static str> {
    let header = |id: &str| id.starts_with("header-sync/") || id.starts_with("header/");
    HARNESSES
        .iter()
        .filter(|entry| {
            header(entry.name)
                || (entry.properties)()
                    .iter()
                    .any(|property| header(&property.id))
        })
        .map(|entry| entry.name)
        .collect()
}

/// A header harness missing from the manifest's list would be outside every
/// other check, so the list must equal the harnesses that register header
/// IDs.
#[test]
fn the_header_harness_list_is_complete() {
    let listed: BTreeSet<&str> = HEADER_HARNESSES.iter().copied().collect();
    assert_eq!(listed, header_harnesses_by_registration());
    // A control: a list missing one header harness differs.
    let mut short = listed.clone();
    short.remove("header-sync/release-wake");
    assert_ne!(short, header_harnesses_by_registration());
}

/// Whether `registered` equals `matrix` exactly, as `(harness, ID, status)`
/// triples.
fn matrix_matches(
    registered: &[Registered],
    matrix: &[(&str, &str, Status)],
) -> Result<(), String> {
    let actual: Vec<(String, String, String)> = registered
        .iter()
        .map(|r| {
            (
                r.harness.clone(),
                r.property.clone(),
                format!("{:?}", r.status),
            )
        })
        .collect();
    let expected: Vec<(String, String, String)> = matrix
        .iter()
        .map(|(h, id, status)| ((*h).to_owned(), (*id).to_owned(), format!("{status:?}")))
        .collect();
    let (actual_set, expected_set): (BTreeSet<_>, BTreeSet<_>) = (
        actual.iter().cloned().collect(),
        expected.iter().cloned().collect(),
    );
    if actual_set.len() != actual.len() || expected_set.len() != expected.len() {
        return Err("a duplicate triple".into());
    }
    if actual_set != expected_set {
        return Err(format!(
            "registered but not in the matrix: {:?}; in the matrix but not registered: {:?}",
            actual_set.difference(&expected_set).collect::<Vec<_>>(),
            expected_set.difference(&actual_set).collect::<Vec<_>>()
        ));
    }
    Ok(())
}

#[test]
fn the_registrations_equal_the_harness_matrix() {
    assert_eq!(
        matrix_matches(
            &header_acceptance::registered(),
            &header_acceptance::harness_matrix()
        ),
        Ok(())
    );
}

/// A harness that stops registering an ID fails the matrix check even while
/// another harness still registers it, so the union of IDs is unchanged. So
/// does a changed status.
#[test]
fn the_matrix_check_refuses_one_id_moved_between_harnesses() {
    let mut registered = header_acceptance::registered();
    let before: BTreeSet<String> = registered.iter().map(|r| r.property.clone()).collect();
    registered.retain(|r| {
        !(r.harness == "header-sync/storage" && r.property == "header-sync/chunk-budget-bound")
    });
    let after: BTreeSet<String> = registered.iter().map(|r| r.property.clone()).collect();
    assert_eq!(before, after, "the union of IDs stays the same");
    assert!(matrix_matches(&registered, &header_acceptance::harness_matrix()).is_err());
    let mut drafted = header_acceptance::registered();
    drafted[2].status = Status::Draft;
    assert!(matrix_matches(&drafted, &header_acceptance::harness_matrix()).is_err());
}

/// Every planted case under `root` whose property is a header one, as
/// `(property, name)`. The property is the patch's directory relative to
/// `root`, at any depth, as the planted runner reads it.
fn planted_cases_in(root: &Path, header: &dyn Fn(&str) -> bool) -> Vec<(String, String)> {
    /// Collects every `.patch` under `dir`.
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else if path.extension().is_some_and(|ext| ext == "patch") {
                let property = path.parent().unwrap().strip_prefix(root).unwrap();
                out.push((
                    property.to_str().unwrap().to_owned(),
                    path.file_stem().unwrap().to_str().unwrap().to_owned(),
                ));
            }
        }
    }
    let mut cases = Vec::new();
    walk(root, root, &mut cases);
    cases.retain(|(property, _)| header(property));
    cases.sort();
    cases
}

/// Every planted case of a header ID, or under the header families.
fn planted_cases() -> Vec<(String, String)> {
    let ids = registered_ids();
    planted_cases_in(&repo().join("qa/planted"), &|property| {
        ids.contains(property)
            || property.starts_with("header-sync/")
            || property.starts_with("header/")
    })
}

/// A planted case's harness is read as the planted runner reads its case:
/// from its fuzz input's sidecar or its `[typed]` table, as TOML, so a
/// trailing comment is a comment. A case naming both, or neither, is
/// refused.
#[test]
fn a_planted_case_s_harness_is_read_as_the_runner_reads_it() {
    let repo = tempfile::tempdir().unwrap();
    let case = |property: &str, name: &str, toml: &str| {
        let dir = repo.path().join("qa/planted").join(property);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{name}.toml")), toml).unwrap();
    };
    std::fs::create_dir_all(repo.path().join("fixtures")).unwrap();
    std::fs::write(
        repo.path().join("fixtures/in.toml"),
        "harness = \"header/publisher-policy\" # a machine\nsha256 = \"00\"\n",
    )
    .unwrap();
    case(
        "p",
        "fuzz",
        "input = \"fixtures/in.bin\" # the committed input\n",
    );
    case(
        "p",
        "typed-node",
        "[typed]\nharness = \"header-sync/storage\"\nscenario = \"s.json\"\nplan = \"p.bin\"\n",
    );
    case(
        "p",
        "typed-machine",
        "[typed]\nharness = \"header/reactor-policy\"\nscenario = \"s.json\"\nplan = \"p.bin\"\n",
    );
    case(
        "p",
        "both",
        "input = \"fixtures/in.bin\"\n[typed]\nharness = \"header-sync/storage\"\nscenario = \"s.json\"\nplan = \"p.bin\"\n",
    );
    case("p", "neither", "checks = []\n");
    let read = |name: &str| header_acceptance::planted_harness(repo.path(), "p", name);
    assert_eq!(read("fuzz").as_deref(), Ok("header/publisher-policy"));
    assert_eq!(read("typed-node").as_deref(), Ok("header-sync/storage"));
    assert_eq!(
        read("typed-machine").as_deref(),
        Ok("header/reactor-policy")
    );
    assert!(read("both").is_err());
    assert!(read("neither").is_err());
    assert!(read("missing").is_err());
}

#[test]
fn the_planted_scan_reads_nested_properties() {
    let root = tempfile::tempdir().unwrap();
    for (dir, name) in [
        ("header-sync/s5v9/status/reserved-flags-closes", "flags"),
        ("header-sync/s5v9/headers/reserved-flags-closes", "flags"),
        ("session/honest-never-punished", "punished"),
        ("block-sync/retained-byte-bound", "wide"),
    ] {
        std::fs::create_dir_all(root.path().join(dir)).unwrap();
        std::fs::write(root.path().join(dir).join(format!("{name}.patch")), "").unwrap();
    }
    let cases = planted_cases_in(root.path(), &|property| {
        property.starts_with("header-sync/") || property == "session/honest-never-punished"
    });
    assert_eq!(
        cases,
        vec![
            (
                "header-sync/s5v9/headers/reserved-flags-closes".to_owned(),
                "flags".to_owned()
            ),
            (
                "header-sync/s5v9/status/reserved-flags-closes".to_owned(),
                "flags".to_owned()
            ),
            (
                "session/honest-never-punished".to_owned(),
                "punished".to_owned()
            ),
        ]
    );
}

/// Every native-mutation directory under `qa/native-mutations/header-sync/`.
fn native_dirs() -> Vec<String> {
    let mut dirs: Vec<String> = std::fs::read_dir(repo().join("qa/native-mutations/header-sync"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .map(|path| path.file_name().unwrap().to_str().unwrap().to_owned())
        .collect();
    dirs.sort();
    dirs
}

/// Every `(file, test, property)` `COVERAGE` names for a header ID.
fn coverage_triples() -> Vec<(String, String, String)> {
    let ids = registered_ids();
    let mut tests = BTreeSet::new();
    for row in COVERAGE.iter().filter(|row| ids.contains(row.property)) {
        let witness = row.passing.witness();
        tests.insert((
            witness.file.to_owned(),
            witness.test.to_owned(),
            row.property.to_owned(),
        ));
        if let Bypass::Test(bypass) = row.bypass {
            tests.insert((
                bypass.file.to_owned(),
                bypass.test.to_owned(),
                row.property.to_owned(),
            ));
        }
    }
    tests.into_iter().collect()
}

/// Every `(package, file, test)` `COVERAGE` names for a header ID. A
/// `COVERAGE` witness is a `zakura-sim` test.
fn coverage_tests() -> BTreeSet<(String, String, String)> {
    coverage_triples()
        .into_iter()
        .map(|(file, test, _)| ("zakura-sim".to_owned(), file, test))
        .collect()
}

/// The committed required keys.
fn committed_required(manifest: &Manifest) -> BTreeMap<Key, BTreeSet<Gate>> {
    let registered = header_acceptance::registered();
    let needs = header_acceptance::needs();
    let planted = planted_cases();
    let retired: BTreeSet<&str> = NATIVE_MUTATIONS
        .iter()
        .filter(|m| matches!(m.class, NativeClass::Retired { .. }))
        .map(|m| m.patch)
        .collect();
    let native: Vec<String> = native_dirs()
        .into_iter()
        .filter(|dir| !retired.contains(dir.as_str()))
        .collect();
    let coverage = coverage_triples();
    let contracts = header_acceptance::contract_table();
    let planted_harnesses: BTreeMap<(String, String), String> = planted
        .iter()
        .filter_map(|(property, name)| {
            let harness = header_acceptance::planted_harness(&repo(), property, name).ok()?;
            Some(((property.clone(), name.clone()), harness))
        })
        .collect();
    required_keys(&Inventories {
        registered: &registered,
        needs: &needs,
        planted: &planted,
        planted_harnesses: &planted_harnesses,
        native: &native,
        coverage_tests: &coverage,
        contracts: &contracts,
        required_contracts: REQUIRED_CONTRACTS,
        narrowing: NARROWING,
        requirements: GATE_OBLIGATIONS,
        composition: COMPOSITION_CHECKS,
        expected: &manifest.expected,
        histories: &rows::history_targets(),
        machine: rows::is_machine,
    })
}

/// Every problem with how `manifest` covers `required`: a key no result
/// covers or two do, a result covering an unrequired key or one that
/// doesn't name it ([`Key::covers`]), and gate tags that differ from the
/// key's.
fn coverage_errors(manifest: &Manifest, required: &BTreeMap<Key, BTreeSet<Gate>>) -> Vec<String> {
    let mut errors = Vec::new();
    let mut covered: BTreeMap<&Key, usize> = BTreeMap::new();
    for result in &manifest.expected {
        let tags: BTreeSet<Gate> = result.gates.iter().copied().collect();
        for key in &result.keys {
            *covered.entry(key).or_default() += 1;
            match required.get(key) {
                None => errors.push(format!("{key:?} isn't required")),
                Some(gates) if *gates != tags => {
                    errors.push(format!("{key:?}: tags {tags:?}, key {gates:?}"))
                }
                Some(_) => {}
            }
            if !key.covers(&result.expected, &manifest.narrowing) {
                errors.push(format!("{key:?} isn't what {:?} names", result.expected));
            }
        }
    }
    for key in required.keys() {
        match covered.get(key) {
            None => errors.push(format!("{key:?} is uncovered")),
            Some(1) => {}
            Some(n) => errors.push(format!("{key:?} is covered {n} times")),
        }
    }
    errors
}

/// The position in `manifest` of the expected result covering `key`.
fn covering(manifest: &Manifest, key: &Key) -> usize {
    manifest
        .expected
        .iter()
        .position(|result| result.keys.contains(key))
        .unwrap_or_else(|| panic!("nothing covers {key:?}"))
}

#[test]
fn every_required_key_is_covered_once_with_its_gates() {
    let manifest = Manifest::committed();
    let required = committed_required(&manifest);
    assert_eq!(coverage_errors(&manifest, &required), Vec::<String>::new());
    for id in registered_ids() {
        assert!(required.contains_key(&Key::passing(&id)), "{id}");
    }
    // A patched run found at the end owes a cleanup comparison, and one
    // stopped at a step doesn't.
    let patched = |name: &'static str| Key::Cleanup {
        run: RunId::case(
            CaseRef {
                property: "header-sync/teardown-returns-baseline",
                name,
            },
            CaseRun::Patched,
        ),
    };
    assert!(required.contains_key(&patched("forgotten-completion")));
    assert!(!required.contains_key(&patched("kept-queue-lease")));
    let refused = |change: &dyn Fn(&mut Manifest)| {
        let mut changed = manifest.clone();
        change(&mut changed);
        assert!(!coverage_errors(&changed, &required).is_empty());
    };

    // Every planted case's harness is read.
    for (property, name) in planted_cases() {
        assert!(
            header_acceptance::planted_harness(&repo(), &property, &name).is_ok(),
            "{property}/{name}"
        );
    }
    // A planted run's replay removed from the manifest is still required
    // when the keys are derived again, since the planted inventory, not the
    // manifest, says which runs owe one: here a machine case's patched run.
    let machine_patched = RunId::Case {
        property: "header/publisher-policy".into(),
        name: "lost-credit".into(),
        run: CaseRun::Patched,
    };
    let mut dropped = manifest.clone();
    dropped
        .expected
        .retain(|r| !matches!(&r.expected, Expected::Replays { run } if *run == machine_patched));
    assert_eq!(dropped.expected.len() + 1, manifest.expected.len());
    assert!(!coverage_errors(&dropped, &committed_required(&dropped)).is_empty());
    // With no expected result at all, the planted runs' replays and a node
    // case's cleanups are still required: they don't come from the rows.
    let mut empty = manifest.clone();
    empty.expected.clear();
    let bare = committed_required(&empty);
    let node = |run| RunId::Case {
        property: "header-sync/stale-completion-inert".into(),
        name: "stale-charge".into(),
        run,
    };
    for key in [
        Key::Replay {
            run: machine_patched.clone(),
        },
        Key::Replay {
            run: node(CaseRun::Draft),
        },
        Key::Cleanup {
            run: node(CaseRun::Baseline),
        },
    ] {
        assert!(bare.contains_key(&key), "{key:?}");
    }
    assert!(!bare.contains_key(&Key::Replay {
        run: RunId::Case {
            property: "header/publisher-policy".into(),
            name: "lost-credit".into(),
            run: CaseRun::Draft,
        }
    }));

    // A required key with its expected result removed.
    refused(&|m| {
        m.expected.retain(|r| {
            !r.keys
                .contains(&Key::passing("header-sync/stale-completion-inert"))
        })
    });
    // One gate tag dropped.
    refused(&|m| {
        let at = covering(m, &Key::passing("header-sync/chunk-budget-bound"));
        m.expected[at].gates.clear();
    });
    // A gate requirement's result dropped, with every row intact.
    refused(&|m| {
        m.expected.retain(|r| {
            !r.keys.contains(&Key::Requirement {
                name: "gate-2/despite-close/close-companion".into(),
            })
        })
    });
    // A transitional Open left beside the result that replaces it.
    refused(&|m| {
        let key = Key::passing("header-sync/stale-completion-inert");
        let mut open = m.expected[covering(m, &key)].clone();
        open.expected = Expected::Open {
            reason: "replaced",
            owner: "test",
            approval: header_acceptance::Approval::Pending,
            until: Some("hdr-14.4"),
        };
        m.expected.push(open);
    });

    // Payload controls: the key stays, and its result names something one
    // field off.
    refused(&|m| {
        let case = CaseRef {
            property: "header-sync/stale-completion-inert",
            name: "stale-charge",
        };
        let at = covering(
            m,
            &Key::Replay {
                run: RunId::case(case, CaseRun::Draft),
            },
        );
        m.expected[at].expected = Expected::Replays {
            run: RunId::case(case, CaseRun::Patched),
        };
    });
    refused(&|m| {
        let test = sim_test(
            "tests/header_node.rs",
            "planted_operation_ids_violate_the_property",
        );
        let at = covering(
            m,
            &Key::Witness {
                package: test.package.into(),
                file: test.file.into(),
                test: test.test.into(),
            },
        );
        m.expected[at].expected = Expected::Passes {
            test: sim_test(
                "tests/header_node.rs",
                "direct_dispatch_guard_reports_a_violation",
            ),
        };
    });
    refused(&|m| {
        let at = covering(m, &Key::passing("header-sync/stale-completion-inert"));
        if let Expected::Holds { property, .. } = &mut m.expected[at].expected {
            *property = "header-sync/chunk-budget-bound";
        }
    });
    refused(&|m| {
        let at = covering(m, &Key::catch("header-sync/stale-completion-inert"));
        if let Expected::Caught { case, .. } = &mut m.expected[at].expected {
            case.name = "stale-credit";
        }
    });
    // An approved standing Open copied onto another standing obligation
    // of the same gate, or the machine cleanup exemption copied onto a node
    // history's cleanup key.
    refused(&|m| {
        let from = m.expected[covering(
            m,
            &Key::Requirement {
                name: "gate-3/no-request-row".into(),
            },
        )]
        .expected
        .clone();
        let at = covering(
            m,
            &Key::Requirement {
                name: "gate-3/credit-fits-window".into(),
            },
        );
        m.expected[at].expected = from;
    });
    refused(&|m| {
        let at = covering(
            m,
            &Key::Cleanup {
                run: rows::counted_histories()[0].into(),
            },
        );
        m.expected[at].expected = rows::MACHINE_CLEANUP;
    });
    // A native mutation's or a narrowing entry's transitional Open with
    // only another owner, or only another reason.
    for key in [
        Key::Native {
            patch: "dropped-owner-check".into(),
        },
        Key::NarrowingTest { entry: "N1".into() },
    ] {
        refused(&|m| {
            let at = covering(m, &key);
            if let Expected::Open { owner, .. } = &mut m.expected[at].expected {
                *owner = "Evan's side";
            }
        });
        refused(&|m| {
            let at = covering(m, &key);
            if let Expected::Open { reason, .. } = &mut m.expected[at].expected {
                *reason = "another reason for the same PR";
            }
        });
    }
    // A transitional row naming another listed PR than its key's owner, or
    // only another owner, or only another reason.
    refused(&|m| {
        let at = covering(m, &Key::catch("header-sync/storage-operation-ids"));
        if let Expected::Open { until, .. } = &mut m.expected[at].expected {
            *until = Some("hdr-14.5");
        }
    });
    refused(&|m| {
        let at = covering(m, &Key::catch("header-sync/storage-operation-ids"));
        if let Expected::Open { owner, .. } = &mut m.expected[at].expected {
            *owner = "Evan's side";
        }
    });
    refused(&|m| {
        let at = covering(m, &Key::catch("header-sync/storage-operation-ids"));
        if let Expected::Open { reason, .. } = &mut m.expected[at].expected {
            *reason = "some other reason hdr-14.4 owns";
        }
    });
    // A non-primary counted history's whole-report pass removed, or judging
    // another registered target with the same gates.
    // Each control's history is named by its whole `HistoryRef`, and must be
    // a counted one.
    let counted = |history: HistoryRef| -> HistoryRef {
        assert!(rows::counted_histories().contains(&history), "{history:?}");
        history
    };
    let plan_1 = || -> HistoryRef {
        counted(HistoryRef {
            harness: "header-sync/storage",
            witness: sim_test(
                "tests/header_node.rs",
                "node_syncs_from_a_mirror_on_controlled_storage",
            ),
            case: "plan 1 of inputs(32, 256)",
        })
    };
    let plan_1_key = || Key::History {
        run: plan_1().into(),
        target: "header-sync/storage-operation-ids".into(),
    };
    refused(&|m| m.expected.retain(|r| !r.keys.contains(&plan_1_key())));
    refused(&|m| {
        let at = covering(m, &plan_1_key());
        if let Expected::Holds { property, .. } = &mut m.expected[at].expected {
            *property = "harness/direct-dispatch";
        }
    });
    // A conformant control's whole-report pass removed while its test's
    // `Passes` stays.
    refused(&|m| {
        let control = counted(HistoryRef {
            harness: "header-sync/wire",
            witness: sim_test(
                "tests/header_wire_witness.rs",
                "each_invalid_label_witness_has_a_conformant_control",
            ),
            case: "headers/max-payload",
        });
        m.expected.retain(
            |r| !matches!(r.expected, Expected::Holds { history, .. } if history == control),
        )
    });
    // A missing-source test's Open naming another listed PR than hdr-14.3.
    refused(&|m| {
        let at = m
            .expected
            .iter()
            .position(|r| matches!(r.keys[..], [Key::MissingSource { .. }]))
            .unwrap();
        if let Expected::Open { until, .. } = &mut m.expected[at].expected {
            *until = Some("hdr-14.5");
        }
    });
    // Two transitional requirements owned by the same PR with their Opens
    // swapped.
    refused(&|m| {
        let at = |m: &Manifest, name: &str| {
            covering(
                m,
                &Key::Requirement {
                    name: name.to_owned(),
                },
            )
        };
        let (a, b) = (
            at(m, "gate-2/despite-close/close-companion"),
            at(m, "gate-2/despite-close/staging-companion"),
        );
        let swapped = m.expected[a].expected.clone();
        m.expected[a].expected = m.expected[b].expected.clone();
        m.expected[b].expected = swapped;
    });
    // A witness test's gate tag dropped.
    refused(&|m| {
        let test = sim_test(
            "tests/header_node_capacity.rs",
            "the_close_relink_input_holds_chunk_budget_bound",
        );
        let at = covering(
            m,
            &Key::Witness {
                package: test.package.into(),
                file: test.file.into(),
                test: test.test.into(),
            },
        );
        m.expected[at].gates.clear();
    });
    // A native integrity result one field off its classification: another
    // diagnostic, or another history.
    refused(&|m| {
        let at = covering(
            m,
            &Key::Native {
                patch: "raw-budget-lease".into(),
            },
        );
        if let Expected::IntegrityCaught { diagnostic, .. } = &mut m.expected[at].expected {
            *diagnostic = "ReservedMismatch";
        }
    });
    refused(&|m| {
        let at = covering(
            m,
            &Key::Native {
                patch: "raw-budget-lease".into(),
            },
        );
        if let Expected::IntegrityCaught { history, .. } = &mut m.expected[at].expected {
            history.case = "plan 1 of inputs(16, 256)";
        }
    });
    refused(&|m| {
        let at = covering(
            m,
            &Key::Native {
                patch: "short-cooldown".into(),
            },
        );
        if let Expected::Open { until, .. } = &mut m.expected[at].expected {
            *until = Some("hdr-14.6");
        }
    });
}

/// The witness test `COVERAGE` names for `property`'s passing evidence. A
/// `COVERAGE` witness is a `zakura-sim` test.
fn coverage_witness(property: &str) -> Option<TestRef> {
    COVERAGE
        .iter()
        .find(|row| row.property == property)
        .map(|row| {
            let witness = row.passing.witness();
            sim_test(witness.file, witness.test)
        })
}

/// Every row whose passing history isn't from the test `COVERAGE` names
/// for its property, unless its passing key is an `Open`.
/// A test-only witness exemption: the property, the `COVERAGE` test that
/// justifies it, and why.
type Exemption = (&'static str, TestRef, &'static str);

fn witness_association_errors(
    rows: &[header_acceptance::Row],
    exemptions: &[Exemption],
) -> Vec<String> {
    let mut errors = Vec::new();
    for row in rows {
        let passing = row
            .expected
            .iter()
            .find(|result| result.keys.contains(&Key::passing(row.property)))
            .expect("every row covers its passing key");
        let history = match passing.expected {
            Expected::Holds { history, .. } | Expected::StaysNotApplicable { history, .. } => {
                history
            }
            Expected::Open { .. } => continue,
            _ => {
                errors.push(format!("{}: its passing key isn't a history", row.property));
                continue;
            }
        };
        // Every history runs on a header harness that registers its target.
        if !registering(row.property).contains(history.harness) {
            errors.push(format!("{}: history on {}", row.property, history.harness));
            continue;
        }
        // A test-only witness's COVERAGE test is a required Passes, so only
        // the requirement to use that test is waived.
        let coverage = coverage_witness(row.property);
        // The exemption's whole TestRef must be that one.
        let exempt = exemptions
            .iter()
            .any(|(id, test, _)| *id == row.property && coverage == Some(*test));
        if exempt {
            continue;
        }
        if coverage != Some(history.witness) {
            errors.push(format!(
                "{}: history from {:?}",
                row.property, history.witness
            ));
        }
    }
    errors
}

/// The history a row's `Holds` names.
fn holds_history(row: &header_acceptance::Row) -> HistoryRef {
    row.expected
        .iter()
        .find_map(|result| match result.expected {
            Expected::Holds { history, .. } => Some(history),
            _ => None,
        })
        .expect("the row has a Holds")
}

#[test]
fn the_rows_are_consistent_with_coverage() {
    let rows = header_acceptance::rows();
    let witnesses: BTreeSet<(String, String, String)> = rows::witness_tests()
        .into_iter()
        .map(|test| {
            (
                test.package.to_owned(),
                test.file.to_owned(),
                test.test.to_owned(),
            )
        })
        .collect();
    assert_eq!(witnesses, coverage_tests());
    assert_eq!(
        witness_association_errors(&rows, rows::TEST_ONLY_WITNESSES),
        Vec::<String>::new()
    );
    let by_id: BTreeMap<&str, _> = rows.iter().map(|row| (row.property, row)).collect();
    let ids = registered_ids();
    for coverage in COVERAGE.iter().filter(|row| ids.contains(row.property)) {
        let row = by_id[coverage.property];
        if let Bypass::Planted(name) = coverage.bypass {
            assert!(
                row.expected.iter().any(|result| matches!(
                    result.expected,
                    Expected::Caught { case, .. } | Expected::MachineCaught { case, .. }
                        if case.property == coverage.property && case.name == name
                )),
                "{}: no catch for its COVERAGE bypass {name}",
                coverage.property
            );
        }
    }
    // A control: two rows' histories swapped keeps the global test set and
    // fails the per-property association.
    let mut swapped = rows.clone();
    let at = |id: &str| rows.iter().position(|row| row.property == id).unwrap();
    let (a, b) = (
        at("header-sync/stale-completion-inert"),
        at("header-sync/chunk-budget-bound"),
    );
    let (history_a, history_b) = (holds_history(&rows[a]), holds_history(&rows[b]));
    for (row, history) in [(a, history_b), (b, history_a)] {
        for result in &mut swapped[row].expected {
            if let Expected::Holds { history: h, .. } = &mut result.expected {
                *h = history;
            }
        }
    }
    assert_eq!(
        witness_association_errors(&swapped, rows::TEST_ONLY_WITNESSES).len(),
        2
    );
    // Only the harness changed, to one that doesn't register the property.
    let mut moved = rows.clone();
    for result in &mut moved[a].expected {
        if let Expected::Holds { history, .. } = &mut result.expected {
            history.harness = "header-sync/smoke";
        }
    }
    assert_eq!(
        witness_association_errors(&moved, rows::TEST_ONLY_WITNESSES).len(),
        1
    );
    // Only the witness's package changed: the same file and test name in
    // another package aren't the COVERAGE test.
    let mut repackaged = rows.clone();
    for result in &mut repackaged[a].expected {
        if let Expected::Holds { history, .. } = &mut result.expected {
            history.witness.package = "xtask";
        }
    }
    assert_eq!(
        witness_association_errors(&repackaged, rows::TEST_ONLY_WITNESSES).len(),
        1
    );

    // The test-only exception: sim/no-livelock's history may come from a
    // header harness's test, but not from a test-only harness.
    let livelock = at("sim/no-livelock");
    let mut excepted = rows.clone();
    let mut history = holds_history(&rows[at("sim/no-task-panic")]);
    excepted[livelock].expected[0].expected = Expected::Holds {
        property: "sim/no-livelock",
        history,
        not_applicable: &[],
    };
    assert_eq!(
        witness_association_errors(&excepted, rows::TEST_ONLY_WITNESSES),
        Vec::<String>::new()
    );
    history.harness = "test/probe";
    excepted[livelock].expected[0].expected = Expected::Holds {
        property: "sim/no-livelock",
        history,
        not_applicable: &[],
    };
    assert_eq!(
        witness_association_errors(&excepted, rows::TEST_ONLY_WITNESSES).len(),
        1
    );
    // Nor from an inventoried history on a harness that doesn't register
    // it, such as the reactor machine's.
    excepted[livelock].expected[0].expected = Expected::Holds {
        property: "sim/no-livelock",
        history: *rows::counted_histories()
            .iter()
            .find(|h| h.harness == "header/reactor-policy")
            .unwrap(),
        not_applicable: &[],
    };
    assert_eq!(
        witness_association_errors(&excepted, rows::TEST_ONLY_WITNESSES).len(),
        1
    );
    // An exemption names its COVERAGE test: one naming another test no
    // longer waives the history's witness.
    let mut history = holds_history(&rows[at("sim/no-task-panic")]);
    history.harness = "header-sync/smoke";
    excepted[livelock].expected[0].expected = Expected::Holds {
        property: "sim/no-livelock",
        history,
        not_applicable: &[],
    };
    assert_eq!(
        witness_association_errors(&excepted, rows::TEST_ONLY_WITNESSES),
        Vec::<String>::new()
    );
    for changed in [
        |test: TestRef| TestRef {
            test: "another_test",
            ..test
        },
        |test: TestRef| TestRef {
            package: "xtask",
            ..test
        },
    ] {
        let other: Vec<Exemption> = rows::TEST_ONLY_WITNESSES
            .iter()
            .map(|&(id, test, why)| (id, changed(test), why))
            .collect();
        assert_eq!(witness_association_errors(&excepted, &other).len(), 1);
    }
}

/// One witness test's pinned histories: harness, file, test, the target
/// (`None` for the generated ID each case names) and the cases.
type PinGroup = (
    &'static str,
    &'static str,
    &'static str,
    Option<&'static str>,
    Vec<String>,
);

/// Every execution each counted witness test runs, with the property it's
/// run for, written from the tests themselves apart from
/// `rows::history_targets`. A fault witness runs each history for the
/// generated ID its case names.
fn pinned_histories() -> BTreeSet<(HistoryRef, &'static str)> {
    const WIRE: &str = "header-sync/wire";
    const STORAGE: &str = "header-sync/storage";
    const WITNESS: &str = "tests/header_wire_witness.rs";
    let plans = |prefix: &str, input: &str, n: usize| -> Vec<String> {
        (0..n)
            .map(|i| format!("{prefix}plan {i} of {input}"))
            .collect()
    };
    let owned = |cases: &[&str]| -> Vec<String> { cases.iter().map(|c| (*c).to_owned()).collect() };
    let mut honest = owned(&["both ways", "redial", "bystander"]);
    honest.extend(plans("lossy ", "inputs(2, 16384)", 2));
    let mut progress = owned(&["committed plan", "empty plan"]);
    progress.extend(plans("", "inputs(4, 16384)", 4));
    let faults = |row: &str, rules: &[&str]| -> Vec<String> {
        rules
            .iter()
            .map(|rule| format!("{row}/{rule}-closes"))
            .collect()
    };
    let mut status = owned(&["undeclared-type-closes"]);
    status.extend(faults(
        "status",
        &[
            "reserved-flags",
            "short-payload",
            "long-payload",
            "bad-content",
        ],
    ));
    status.push("status/cadence-never-closes".into());
    // `None` targets the generated ID the case names.
    let groups: Vec<PinGroup> = vec![
        (
            WIRE,
            WITNESS,
            "every_generated_property_applies_in_its_witness",
            Some("header-sync/s5v9/conformant-never-closes"),
            owned(&["both-ways"]),
        ),
        (
            WIRE,
            WITNESS,
            "status_faults_apply_in_their_witnesses",
            None,
            status,
        ),
        (
            WIRE,
            WITNESS,
            "subscribe_headers_faults_apply_in_their_witnesses",
            None,
            faults(
                "subscribe-headers",
                &[
                    "reserved-flags",
                    "short-payload",
                    "long-payload",
                    "over-live",
                    "over-credit",
                    "after-close",
                    "bad-cursor",
                    "bad-content",
                ],
            ),
        ),
        (
            WIRE,
            WITNESS,
            "headers_faults_apply_in_their_witnesses",
            None,
            faults(
                "headers",
                &[
                    "reserved-flags",
                    "short-payload",
                    "long-payload",
                    "unsolicited",
                    "bad-content",
                ],
            ),
        ),
        (
            WIRE,
            WITNESS,
            "headers_outcome_faults_apply_in_their_witnesses",
            None,
            faults(
                "headers-outcome",
                &[
                    "reserved-flags",
                    "short-payload",
                    "long-payload",
                    "unsolicited",
                    "bad-content",
                ],
            ),
        ),
        (
            WIRE,
            WITNESS,
            "each_invalid_label_witness_has_a_conformant_control",
            Some("header-sync/s5v9/conformant-never-closes"),
            owned(&[
                "status/min-payload",
                "subscribe-headers/min-payload",
                "subscribe-headers/second-open",
                "headers/min-payload",
                "headers/max-payload",
                "headers-outcome/min-payload",
            ]),
        ),
        (
            WIRE,
            WITNESS,
            "honest_peer_never_punished",
            Some("session/honest-never-punished"),
            honest,
        ),
        (
            WIRE,
            "tests/header_wire_progress.rs",
            "the_progress_fixture_holds_honest_progress",
            Some("header-sync/honest-progress"),
            progress,
        ),
        (
            STORAGE,
            "tests/header_node.rs",
            "node_syncs_from_a_mirror_on_controlled_storage",
            Some("header-sync/storage-operation-ids"),
            plans("", "inputs(32, 256)", 32),
        ),
        (
            STORAGE,
            "tests/header_node_capacity.rs",
            "the_close_relink_input_holds_chunk_budget_bound",
            Some("header-sync/chunk-budget-bound"),
            owned(&["committed close-relink input"]),
        ),
        (
            STORAGE,
            "tests/header_teardown.rs",
            "the_close_relink_input_holds_teardown_returns_baseline",
            Some("header-sync/teardown-returns-baseline"),
            owned(&["committed close-relink input"]),
        ),
        (
            STORAGE,
            "tests/header_stale.rs",
            "the_stale_prepare_input_holds_stale_completion_inert",
            Some("header-sync/stale-completion-inert"),
            owned(&["committed stale-prepare input"]),
        ),
        (
            "header-sync/release-wake",
            "tests/header_release_wake.rs",
            "the_release_wake_input_holds_capacity_release_wakes",
            Some("header-sync/capacity-release-wakes"),
            owned(&["committed release input"]),
        ),
        (
            "header-sync/flood",
            "tests/header_flood_watch.rs",
            "the_committed_flood_inputs_hold_their_watch_rules",
            Some("header-sync/watch-observed-under-flood"),
            owned(&["committed wire flood input"]),
        ),
        (
            "header-sync/flood",
            "tests/header_flood_watch.rs",
            "the_committed_flood_inputs_hold_their_watch_rules",
            Some("header-sync/watch-observed-under-lifecycle-flood"),
            owned(&["committed lifecycle flood input"]),
        ),
        (
            "header-sync/smoke",
            "tests/harness.rs",
            "a_clean_run_holds_no_task_panic",
            Some("sim/no-task-panic"),
            owned(&["empty input"]),
        ),
        (
            "header/reactor-policy",
            "tests/replay_exact.rs",
            "reactor_machine_records_and_replays_without_transport",
            Some("header/reactor-policy"),
            owned(&["REACTOR_INPUT"]),
        ),
        (
            "header/publisher-policy",
            "tests/replay_exact.rs",
            "publisher_machine_records_and_replays_without_transport",
            Some("header/publisher-policy"),
            owned(&["PUBLISHER_INPUT"]),
        ),
    ];
    // Built once per call for a fixed test table, so the leaks are bounded.
    let leak = |text: String| -> &'static str { Box::leak(text.into_boxed_str()) };
    groups
        .into_iter()
        .flat_map(|(harness, file, test, target, cases)| {
            cases.into_iter().map(move |case| {
                let target = match target {
                    Some(target) => target,
                    None => leak(format!("header-sync/s5v9/{case}")),
                };
                (
                    HistoryRef {
                        harness,
                        witness: sim_test(file, test),
                        case: leak(case),
                    },
                    target,
                )
            })
        })
        .collect()
}

/// Every counted history names its case, every history a row counts is in
/// the history inventory, and the inventory names each once.
#[test]
fn every_history_names_its_case() {
    let manifest = Manifest::committed();
    let inventory = rows::counted_histories();
    let unique: BTreeSet<HistoryRef> = inventory.iter().copied().collect();
    assert_eq!(unique.len(), inventory.len(), "each history once");
    for result in &manifest.expected {
        if let Expected::Holds { history, .. } | Expected::StaysNotApplicable { history, .. } =
            result.expected
        {
            assert!(
                unique.contains(&history),
                "{history:?} isn't in the inventory"
            );
        }
    }
    // A history a test runs but the inventory leaves out owes nothing, so
    // the inventory must name every execution of every counted witness,
    // by its full identity.
    let targets: BTreeSet<(HistoryRef, &str)> = rows::history_targets().into_iter().collect();
    assert_eq!(targets, pinned_histories());
    // A history differing from a pinned one only in its witness's file, in
    // its harness, or in the property it's run for, isn't that history.
    let (first, target) = rows::history_targets()[0];
    for changed in [
        (
            HistoryRef {
                witness: sim_test("tests/header_wire_progress.rs", first.witness.test),
                ..first
            },
            target,
        ),
        (
            HistoryRef {
                harness: "header-sync/storage",
                ..first
            },
            target,
        ),
        (first, "header-sync/s5v9/status/sender-cadence"),
    ] {
        let mut swapped = targets.clone();
        swapped.remove(&(first, target));
        swapped.insert(changed);
        assert_ne!(swapped, pinned_histories(), "{changed:?}");
    }
    for result in &manifest.expected {
        match result.expected {
            Expected::Holds { history, .. }
            | Expected::StaysNotApplicable { history, .. }
            | Expected::IntegrityCaught { history, .. } => {
                assert!(!history.case.is_empty(), "{history:?}")
            }
            _ => {}
        }
        for key in &result.keys {
            if let Key::Replay {
                run: RunId::History { case, .. },
            }
            | Key::Cleanup {
                run: RunId::History { case, .. },
            } = key
            {
                assert!(!case.is_empty(), "{key:?}");
            }
        }
    }
}

/// Every problem with how `cases` are named: a case named by no row or by
/// two, a named case that doesn't exist, a declared harness that isn't the
/// one its committed input's sidecar names or doesn't register its
/// property, and a result kind that doesn't match that harness's
/// [`HarnessEntry::machine`](harness::HarnessEntry::machine).
/// The harness case `(property, name)`'s committed input runs on, as its
/// input's sidecar names it.
fn case_harness(property: &str, name: &str) -> Result<String, String> {
    header_acceptance::planted_harness(&repo(), property, name)
}

fn case_errors(expected: &[ExpectedResult], cases: &[(String, String)]) -> Vec<String> {
    let mut named: BTreeMap<(String, String), Vec<(bool, &str)>> = BTreeMap::new();
    for result in expected {
        let (case, machine, declared) = match result.expected {
            Expected::Caught { case, harness, .. } => (case, false, harness),
            Expected::MachineCaught { case, harness } => (case, true, harness),
            _ => continue,
        };
        named
            .entry((case.property.to_owned(), case.name.to_owned()))
            .or_default()
            .push((machine, declared));
    }
    let mut errors = Vec::new();
    for case in cases {
        match named.get(case).map(Vec::as_slice) {
            Some([(machine, declared)]) => match harness::find(declared) {
                _ if case_harness(&case.0, &case.1).as_deref() != Ok(*declared) => {
                    errors.push(format!(
                        "{case:?}: declared {declared}, its input runs on {:?}",
                        case_harness(&case.0, &case.1)
                    ))
                }
                Ok(entry) if entry.status(&case.0).is_none() => {
                    errors.push(format!("{case:?}: {declared} doesn't register it"))
                }
                Ok(entry) if entry.machine != *machine => {
                    errors.push(format!("{case:?}: listed machine {machine} on {declared}"))
                }
                Ok(_) => {}
                Err(error) => errors.push(format!("{case:?}: {error}")),
            },
            None | Some([]) => errors.push(format!("{case:?} is named by no row")),
            Some(_) => errors.push(format!("{case:?} is named twice")),
        }
    }
    for case in named.keys() {
        if !cases.contains(case) {
            errors.push(format!("{case:?} doesn't exist"));
        }
    }
    errors
}

#[test]
fn every_planted_case_is_named_by_one_row_of_its_kind() {
    let manifest = Manifest::committed();
    let cases = planted_cases();
    assert!(cases.iter().any(|case| case.0 == "header/reactor-policy"));
    assert!(cases.iter().any(|case| case.0 == "header/publisher-policy"));
    assert_eq!(
        case_errors(&manifest.expected, &cases),
        Vec::<String>::new()
    );

    let changed = |property: &str, name: &str, change: &dyn Fn(&mut Expected)| {
        let mut expected = manifest.expected.clone();
        for result in &mut expected {
            match result.expected {
                Expected::Caught { case, .. } | Expected::MachineCaught { case, .. }
                    if case.property == property && case.name == name =>
                {
                    change(&mut result.expected)
                }
                _ => {}
            }
        }
        assert!(
            !case_errors(&expected, &cases).is_empty(),
            "{property}/{name}"
        );
    };
    // A machine case listed as a Caught, and a node case as a MachineCaught.
    changed("header/publisher-policy", "lost-credit", &|e| {
        if let Expected::MachineCaught { case, harness } = *e {
            *e = Expected::Caught {
                case,
                harness,
                set: &["header/publisher-policy"],
                found: Found::AtStep,
                companions: &[],
                not_applicable: &[],
            };
        }
    });
    changed("header-sync/stale-completion-inert", "stale-charge", &|e| {
        if let Expected::Caught { case, harness, .. } = *e {
            *e = Expected::MachineCaught { case, harness };
        }
    });
    // Only the declared harness changed: to another machine, to a node
    // harness that doesn't register the case's property, and to a node
    // harness that registers it but isn't the one its input runs on.
    changed("header/publisher-policy", "lost-credit", &|e| {
        if let Expected::MachineCaught { harness, .. } = e {
            *harness = "header/reactor-policy";
        }
    });
    changed("header-sync/stale-completion-inert", "stale-charge", &|e| {
        if let Expected::Caught { harness, .. } = e {
            *harness = "header-sync/smoke";
        }
    });
    for other in ["header-sync/release-wake", "header-sync/wire"] {
        changed("header-sync/stale-completion-inert", "stale-charge", &|e| {
            if let Expected::Caught { harness, .. } = e {
                *harness = other;
            }
        });
    }
}

/// Every problem with `mutations` against the native directories `dirs`,
/// the planted `cases` and `expected`: an unlisted directory, a moved
/// mutation whose directory remains or whose planted case is missing or
/// named by no catch, and a stale move.
fn native_errors(
    mutations: &[rows::NativeMutation],
    dirs: &[String],
    cases: &[(String, String)],
    expected: &[ExpectedResult],
) -> Vec<String> {
    let mut errors = Vec::new();
    // Every mutation there has been keeps a classification.
    for name in rows::NATIVE_INVENTORY {
        if !mutations.iter().any(|m| m.patch == *name) {
            errors.push(format!("{name}: in the inventory but unclassified"));
        }
    }
    for mutation in mutations {
        if !rows::NATIVE_INVENTORY.contains(&mutation.patch) {
            errors.push(format!(
                "{}: classified but not in the inventory",
                mutation.patch
            ));
        }
    }
    let present: Vec<String> = mutations
        .iter()
        .filter(|m| !matches!(m.class, NativeClass::Moved { .. }))
        .map(|m| m.patch.to_owned())
        .collect();
    if present != dirs {
        errors.push(format!("directories {dirs:?}, listed {present:?}"));
    }
    for mutation in mutations {
        match mutation.class {
            NativeClass::ToMove { property, until } => {
                if until.is_empty() {
                    errors.push(format!("{}: no PR", mutation.patch));
                }
                if cases.contains(&(property.to_owned(), mutation.patch.to_owned())) {
                    errors.push(format!("{}: moved but still listed", mutation.patch));
                }
            }
            NativeClass::Moved { property, case } => {
                if case != mutation.patch {
                    errors.push(format!(
                        "{}: moved to {case}, not a case of its own name",
                        mutation.patch
                    ));
                }
                let named = expected.iter().any(|result| {
                    matches!(result.expected, Expected::Caught { case: c, .. }
                        if c.property == property && c.name == case)
                });
                if !cases.contains(&(property.to_owned(), case.to_owned())) || !named {
                    errors.push(format!(
                        "{}: its planted case {property}/{case} is missing or unnamed",
                        mutation.patch
                    ));
                }
            }
            NativeClass::Integrity { diagnostic, .. } => {
                if diagnostic.is_empty() {
                    errors.push(format!("{}: no diagnostic", mutation.patch));
                }
            }
            NativeClass::Retired { reason } => {
                if reason.is_empty() {
                    errors.push(format!("{}: no reason", mutation.patch));
                }
            }
        }
    }
    errors
}

/// What a native mutation is pinned as, apart from `NATIVE_MUTATIONS`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativePin {
    /// It moves under this property in this PR.
    Move(&'static str, &'static str),
    /// It stays native as an integrity mutation on this history, caught
    /// with this diagnostic.
    Integrity(HistoryRef, &'static str),
    /// It's retired.
    Retired,
}

/// The probe-accounting history the integrity mutations run, written from
/// item 7 apart from the rows.
const PROBE_ACCOUNTING: HistoryRef = HistoryRef {
    harness: "header-sync/storage",
    witness: sim_test(
        "tests/header_node_capacity.rs",
        "the_probe_matches_the_accounting_through_close_relink_and_shutdown",
    ),
    case: "plan 0 of inputs(16, 256)",
};

/// Where each native mutation goes, written from the plan's item 7 and
/// the hdr-9–13 plan's evidence-rule classification apart from
/// `NATIVE_MUTATIONS`.
const NATIVE_PINS: &[(&str, NativePin)] = &[
    (
        "consumed-locator-permit",
        NativePin::Move("header-sync/teardown-returns-baseline", "hdr-14.4"),
    ),
    (
        "cooldown-session-gate",
        NativePin::Move("header-sync/header-fault-penalized", "hdr-14.4"),
    ),
    (
        "dropped-owner-check",
        NativePin::Move("header-sync/stale-completion-inert", "hdr-14.4"),
    ),
    (
        "foreign-leases",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.2a"),
    ),
    (
        "forgotten-locator-permit",
        NativePin::Move("header-sync/teardown-returns-baseline", "hdr-14.4"),
    ),
    (
        "late-fault-unpenalized",
        NativePin::Move("header-sync/header-fault-penalized", "hdr-14.4"),
    ),
    (
        "misstored-page",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.1a"),
    ),
    (
        "own9-join",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.1a"),
    ),
    (
        "owner-swap",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.2a"),
    ),
    (
        "raw-budget-lease",
        NativePin::Integrity(PROBE_ACCOUNTING, "OwnedMismatch"),
    ),
    (
        "raw-budget-reservation",
        NativePin::Integrity(PROBE_ACCOUNTING, "ReservedMismatch"),
    ),
    ("readmission-unchecked", NativePin::Retired),
    (
        "reordered-payload",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.2a"),
    ),
    (
        "secondary-reservation",
        NativePin::Move("header-sync/outstanding-work-bounded", "hdr-9d.1"),
    ),
    (
        "session-view",
        NativePin::Move("header-sync/peer-retirement-progress", "hdr-10b"),
    ),
    (
        "short-cooldown",
        NativePin::Move("header-sync/header-fault-penalized", "hdr-14.4"),
    ),
    (
        "short-payload",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.2a"),
    ),
    (
        "shutdown-kept-queue",
        NativePin::Move("header-sync/teardown-returns-baseline", "hdr-14.4"),
    ),
    (
        "skewed-remaining",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.1b"),
    ),
    (
        "skipped-retention",
        NativePin::Move("header-sync/outstanding-work-bounded", "hdr-9d.1"),
    ),
    (
        "skipped-staging",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.1a"),
    ),
    (
        "stale-locator-round",
        NativePin::Move("header-sync/reset-invalidates-work", "hdr-9a.2"),
    ),
    (
        "stale-status-cache",
        NativePin::Move("header-sync/state-publication-consistent", "hdr-12b"),
    ),
    (
        "swapped-payload",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.2a"),
    ),
    (
        "unreleased-retirement",
        NativePin::Move("header-sync/peer-retirement-progress", "hdr-10b"),
    ),
    (
        "unsent-tip",
        NativePin::Move("header-sync/state-publication-consistent", "hdr-12b"),
    ),
];

/// Every native mutation whose classification disagrees with
/// [`NATIVE_PINS`]: another kind, or a move with another property or PR,
/// or an integrity mutation on another history.
fn native_pin_errors(mutations: &[rows::NativeMutation]) -> Vec<String> {
    let mut errors = Vec::new();
    for mutation in mutations {
        let pin = NATIVE_PINS
            .iter()
            .find(|(patch, _)| *patch == mutation.patch)
            .map(|(_, pin)| *pin);
        let ok = match (mutation.class, pin) {
            (NativeClass::ToMove { property, until }, Some(NativePin::Move(p, pr))) => {
                property == p && until == pr
            }
            (NativeClass::Moved { property, .. }, Some(NativePin::Move(p, _))) => property == p,
            (
                NativeClass::Integrity {
                    history,
                    diagnostic,
                    ..
                },
                Some(NativePin::Integrity(pinned, pinned_diagnostic)),
            ) => history == pinned && diagnostic == pinned_diagnostic,
            (NativeClass::Retired { .. }, Some(NativePin::Retired)) => true,
            _ => false,
        };
        if !ok {
            errors.push(format!(
                "{}: {:?}, pinned {pin:?}",
                mutation.patch, mutation.class
            ));
        }
    }
    errors
}

#[test]
fn every_native_mutation_is_classified() {
    let manifest = Manifest::committed();
    let cases = planted_cases();
    assert_eq!(
        native_errors(NATIVE_MUTATIONS, &native_dirs(), &cases, &manifest.expected),
        Vec::<String>::new()
    );
    assert_eq!(native_pin_errors(NATIVE_MUTATIONS), Vec::<String>::new());
    // A move's PR or destination changed in the classification itself.
    for (until, property) in [
        ("hdr-14.5", "header-sync/stale-completion-inert"),
        ("hdr-14.4", "header-sync/teardown-returns-baseline"),
    ] {
        let mut changed = NATIVE_MUTATIONS.to_vec();
        let at = changed
            .iter()
            .position(|m| m.patch == "dropped-owner-check")
            .unwrap();
        changed[at].class = NativeClass::ToMove { property, until };
        assert_eq!(native_pin_errors(&changed).len(), 1, "{property} {until}");
    }
    // An integrity mutation retired, the retired one made an integrity
    // mutation, and an integrity mutation's history case changed.
    let integrity = NATIVE_MUTATIONS
        .iter()
        .find(|m| m.patch == "raw-budget-lease")
        .unwrap()
        .class;
    for (patch, class) in [
        (
            "raw-budget-reservation",
            NativeClass::Retired {
                reason: "no longer reachable",
            },
        ),
        ("readmission-unchecked", integrity),
    ] {
        let mut changed = NATIVE_MUTATIONS.to_vec();
        let at = changed.iter().position(|m| m.patch == patch).unwrap();
        changed[at].class = class;
        assert_eq!(native_pin_errors(&changed).len(), 1, "{patch}");
    }
    let mut changed = NATIVE_MUTATIONS.to_vec();
    let at = changed
        .iter()
        .position(|m| m.patch == "raw-budget-reservation")
        .unwrap();
    if let NativeClass::Integrity { history, .. } = &mut changed[at].class {
        history.case = "plan 99 of inputs(16, 256)";
    }
    assert_eq!(native_pin_errors(&changed).len(), 1);
    let mut changed = NATIVE_MUTATIONS.to_vec();
    let at = changed
        .iter()
        .position(|m| m.patch == "raw-budget-lease")
        .unwrap();
    if let NativeClass::Integrity { diagnostic, .. } = &mut changed[at].class {
        *diagnostic = "ReservedMismatch";
    }
    assert_eq!(native_pin_errors(&changed).len(), 1);
    for result in &manifest.expected {
        if let Some(Key::Native { patch }) = result.keys.first() {
            let class = NATIVE_MUTATIONS
                .iter()
                .find(|m| m.patch == patch)
                .unwrap()
                .class;
            match (class, &result.expected) {
                (NativeClass::Integrity { .. }, Expected::IntegrityCaught { .. })
                | (NativeClass::ToMove { .. }, Expected::Open { until: Some(_), .. }) => {}
                (class, expected) => panic!("{patch}: {class:?} as {:?}", expected.kind()),
            }
        }
    }
    // A mutation whose directory and classification are both removed is
    // refused, since the inventory still names it.
    let dropped: Vec<rows::NativeMutation> = NATIVE_MUTATIONS
        .iter()
        .filter(|m| m.patch != "dropped-owner-check")
        .copied()
        .collect();
    let fewer: Vec<String> = native_dirs()
        .into_iter()
        .filter(|dir| dir != "dropped-owner-check")
        .collect();
    assert!(!native_errors(&dropped, &fewer, &cases, &manifest.expected).is_empty());
    // A move's record outlives its directory: once moved, its planted case
    // must exist under its property and a catch must name it.
    let mut moved = NATIVE_MUTATIONS.to_vec();
    let at = moved
        .iter()
        .position(|m| m.patch == "dropped-owner-check")
        .unwrap();
    let dirs: Vec<String> = native_dirs()
        .into_iter()
        .filter(|dir| dir != "dropped-owner-check")
        .collect();
    // Moved, it needs a planted case of its own name under its property,
    // named by a Caught. An existing, caught case of another name doesn't
    // stand in for it.
    let mut with_case = cases.clone();
    with_case.push((
        "header-sync/stale-completion-inert".into(),
        "dropped-owner-check".into(),
    ));
    let mut named = manifest.expected.clone();
    let caught = named
        .iter()
        .find(|r| {
            matches!(r.expected, Expected::Caught { case, .. }
            if case.property == "header-sync/stale-completion-inert")
        })
        .unwrap()
        .clone();
    let mut own = caught;
    if let Expected::Caught { case, .. } = &mut own.expected {
        case.name = "dropped-owner-check";
    }
    named.push(own);
    for (property, case, cases, expected, ok) in [
        (
            "header-sync/stale-completion-inert",
            "dropped-owner-check",
            &with_case,
            &named,
            true,
        ),
        (
            "header-sync/stale-completion-inert",
            "stale-charge",
            &cases,
            &manifest.expected,
            false,
        ),
        (
            "header-sync/stale-completion-inert",
            "dropped-owner-check",
            &cases,
            &manifest.expected,
            false,
        ),
        (
            "header-sync/teardown-returns-baseline",
            "dropped-owner-check",
            &with_case,
            &named,
            false,
        ),
    ] {
        moved[at].class = NativeClass::Moved { property, case };
        let errors = native_errors(&moved, &dirs, cases, expected);
        assert_eq!(errors.is_empty(), ok, "{property}/{case}: {errors:?}");
    }
}

/// An approved standing exclusion counts only on the key it was granted
/// for, with its exact reason and approval.
#[test]
fn standing_exclusions_bind_to_their_key() {
    let grant = rows::StandingExclusion {
        key: ("catch", "header-sync/s5v9/headers/over-owed-closes", ""),
        owner: "hdr-14",
        reason: "unreachable",
        approval: header_acceptance::PLAN,
    };
    let open = Expected::Open {
        reason: "unreachable",
        owner: "hdr-14",
        approval: header_acceptance::PLAN,
        until: None,
    };
    let own = Key::catch("header-sync/s5v9/headers/over-owed-closes");
    assert!(rows::exclusion_granted(&[grant], &own, &open));
    let other = Key::catch("header-sync/s5v9/headers-outcome/over-owed-closes");
    assert!(!rows::exclusion_granted(&[grant], &other, &open));
    assert!(!rows::exclusion_granted(
        &[grant],
        &Key::passing("header-sync/s5v9/headers/over-owed-closes"),
        &open
    ));
    let reworded = Expected::Open {
        reason: "another reason",
        owner: "hdr-14",
        approval: header_acceptance::PLAN,
        until: None,
    };
    assert!(!rows::exclusion_granted(&[grant], &own, &reworded));
    let reowned = Expected::Open {
        reason: "unreachable",
        owner: "someone else",
        approval: header_acceptance::PLAN,
        until: None,
    };
    assert!(!rows::exclusion_granted(&[grant], &own, &reowned));
    // A missing-source grant is for one source of one harness.
    let source_grant = rows::StandingExclusion {
        key: ("missing-source", "header-sync/storage", "View"),
        ..grant
    };
    let missing = |source: &str| Key::MissingSource {
        harness: "header-sync/storage".into(),
        source: source.into(),
    };
    assert!(rows::exclusion_granted(
        &[source_grant],
        &missing("View"),
        &open
    ));
    assert!(!rows::exclusion_granted(
        &[source_grant],
        &missing("Storage"),
        &open
    ));
    // With nothing granted on main, no approved standing Open satisfies a
    // catch key.
    assert!(!own.covers(&open, NARROWING));
}

#[test]
fn scripted_bypasses_never_satisfy_a_catch() {
    for result in Manifest::committed().expected {
        for key in &result.keys {
            if matches!(key, Key::Catch { .. } | Key::Case { .. }) {
                assert!(
                    !matches!(result.expected, Expected::Passes { .. }),
                    "{key:?} is satisfied by a test"
                );
            }
        }
    }
    let test = Expected::Passes {
        test: sim_test(
            "tests/header_node.rs",
            "planted_operation_ids_violate_the_property",
        ),
    };
    assert!(!Key::catch("header-sync/storage-operation-ids").covers(&test, NARROWING));
}

/// Every `#[test]` or `#[tokio::test]` function in `source`, by its full
/// libtest name inside the file (`module::name` for one in an inline
/// module), and whether its own attributes or an enclosing module's keep it
/// from running: an `#[ignore]`, or a `cfg` other than the two the
/// acceptance run's test builds turn on, `test` and `zakura_sim`. Doc
/// comments and other attributes may sit between the test attribute and
/// the `fn`.
fn test_fns(source: &str) -> BTreeMap<String, bool> {
    use proc_macro2::{Delimiter, TokenStream, TokenTree};
    /// Whether `attribute` keeps the item it's on from running in the
    /// acceptance run's test builds.
    fn gates(attribute: &str) -> bool {
        if attribute.starts_with("ignore") {
            return true;
        }
        match attribute.strip_prefix("cfg") {
            Some(predicate) if !predicate.starts_with("_attr") => {
                predicate.parse::<TokenStream>().map_or(true, |tokens| {
                    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
                    match tokens.as_slice() {
                        [TokenTree::Group(group)] => !enabled(group.stream()),
                        _ => true,
                    }
                })
            }
            // A `cfg_attr` could add an `#[ignore]`, so it gates.
            Some(_) => true,
            None => false,
        }
    }
    /// Whether a `cfg` predicate holds in the acceptance run's test builds,
    /// where `test` and `zakura_sim` are on and nothing else is known on:
    /// `all`, `any` and `not` evaluate, and any other option is off.
    fn enabled(predicate: TokenStream) -> bool {
        let tokens: Vec<TokenTree> = predicate.into_iter().collect();
        match tokens.as_slice() {
            [TokenTree::Ident(name)] => name == "test" || name == "zakura_sim",
            [TokenTree::Ident(name), TokenTree::Group(args)] => {
                let mut parts = Vec::new();
                let mut part = Vec::new();
                for token in args.stream() {
                    match &token {
                        TokenTree::Punct(punct) if punct.as_char() == ',' => {
                            parts.push(part.drain(..).collect::<TokenStream>());
                        }
                        _ => part.push(token),
                    }
                }
                if !part.is_empty() {
                    parts.push(part.into_iter().collect());
                }
                match name.to_string().as_str() {
                    "all" => parts.into_iter().all(enabled),
                    "any" => parts.into_iter().any(enabled),
                    "not" => match <[TokenStream; 1]>::try_from(parts) {
                        Ok([only]) => !enabled(only),
                        Err(_) => false,
                    },
                    _ => false,
                }
            }
            _ => false,
        }
    }
    /// Collects `stream`'s test functions under module `path` into `out`,
    /// each gated if `gated` (an enclosing module is) or its own attributes
    /// gate it.
    fn walk(stream: TokenStream, path: &[String], gated: bool, out: &mut BTreeMap<String, bool>) {
        let tokens: Vec<TokenTree> = stream.into_iter().collect();
        let (mut test, mut cfg) = (false, false);
        let mut at = 0;
        while at < tokens.len() {
            match &tokens[at] {
                TokenTree::Punct(punct) if punct.as_char() == '#' => {
                    if let Some(TokenTree::Group(group)) = tokens.get(at + 1) {
                        if group.delimiter() == Delimiter::Bracket {
                            let attribute = group.stream().to_string();
                            test |= attribute == "test"
                                || attribute == "tokio :: test"
                                || attribute.starts_with("tokio :: test (");
                            cfg |= gates(&attribute);
                            at += 2;
                            continue;
                        }
                    }
                }
                TokenTree::Ident(ident) if ident == "mod" => {
                    if let (Some(TokenTree::Ident(name)), Some(TokenTree::Group(body))) =
                        (tokens.get(at + 1), tokens.get(at + 2))
                    {
                        let mut inner = path.to_vec();
                        inner.push(name.to_string());
                        walk(body.stream(), &inner, gated || cfg, out);
                        at += 3;
                        (test, cfg) = (false, false);
                        continue;
                    }
                }
                TokenTree::Ident(ident) if ident == "fn" => {
                    if let Some(TokenTree::Ident(name)) = tokens.get(at + 1) {
                        if test {
                            let mut full = path.to_vec();
                            full.push(name.to_string());
                            out.insert(full.join("::"), gated || cfg);
                        }
                    }
                    (test, cfg) = (false, false);
                }
                TokenTree::Punct(punct) if punct.as_char() == ';' => (test, cfg) = (false, false),
                TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => {
                    (test, cfg) = (false, false)
                }
                _ => {}
            }
            at += 1;
        }
    }
    let mut out = BTreeMap::new();
    if let Ok(tokens) = source.parse::<TokenStream>() {
        walk(tokens, &[], false, &mut out);
    }
    out
}

/// The module path libtest names `file`'s items under, inside the target
/// that owns it, by Cargo's target layout: none for an integration test or
/// a crate root (`src/lib.rs`, `src/main.rs`, `src/bin/<x>.rs`,
/// `src/bin/<x>/main.rs`), `a::b` for `src/a/b.rs` or `src/a/b/mod.rs`,
/// and for a binary's own module, counted from its root, `a` for
/// `src/bin/<x>/a.rs`. The runner resolves the owning target the same way.
fn module_prefix(file: &str) -> Vec<String> {
    let Some(inside) = file.strip_prefix("src/") else {
        return Vec::new();
    };
    let inside = match inside.strip_prefix("bin/") {
        Some(binary) => match binary.split_once('/') {
            Some((_, within)) => within,
            None => return Vec::new(),
        },
        None => inside,
    };
    let inside = inside.strip_suffix(".rs").unwrap_or(inside);
    let mut path: Vec<String> = inside.split('/').map(str::to_owned).collect();
    if matches!(path.last().map(String::as_str), Some("mod")) {
        path.pop();
    }
    if matches!(path.as_slice(), [root] if root == "lib" || root == "main") {
        path.clear();
    }
    path
}

/// A test's module path is counted from the root of the target that owns
/// its file: a library module's from `src/`, a binary's own from its root
/// under `src/bin/`, and none for any crate root.
#[test]
fn a_test_s_module_path_is_counted_from_its_target() {
    let path = |file: &str| module_prefix(file).join("::");
    for root in [
        "tests/a.rs",
        "src/lib.rs",
        "src/main.rs",
        "src/bin/worker.rs",
        "src/bin/worker/main.rs",
    ] {
        assert_eq!(path(root), "", "{root}");
    }
    assert_eq!(path("src/a/b.rs"), "a::b");
    assert_eq!(path("src/a/b/mod.rs"), "a::b");
    assert_eq!(path("src/bin/worker/util.rs"), "util");
    assert_eq!(path("src/bin/worker/util/mod.rs"), "util");
}

/// Checks `test` names exactly one test function, by its package, its
/// file and its full libtest name, module path included, and that the
/// function isn't behind a `cfg` or ignored.
fn test_exists(test: TestRef) -> Result<(), String> {
    let crate_dir = repo().join("crates").join(test.package);
    if !crate_dir.join("Cargo.toml").exists() {
        return Err(format!("{}: unknown package {}", test.test, test.package));
    }
    let source = std::fs::read_to_string(crate_dir.join(test.file))
        .map_err(|error| format!("{}/{}: {error}", test.package, test.file))?;
    let prefix = module_prefix(test.file);
    let found = test_fns(&source);
    let named = found.iter().find(|(name, _)| {
        let mut full = prefix.clone();
        full.push((*name).clone());
        full.join("::") == test.test
    });
    match named {
        None => Err(format!(
            "{}/{} has no test {}",
            test.package, test.file, test.test
        )),
        Some((_, true)) => Err(format!("{} is behind a cfg or ignored", test.test)),
        Some((_, false)) => Ok(()),
    }
}

#[test]
fn every_referenced_test_exists() {
    let manifest = Manifest::committed();
    let mut tests = BTreeSet::new();
    for result in &manifest.expected {
        match result.expected {
            Expected::Passes { test } => {
                tests.insert(test);
            }
            Expected::Holds { history, .. }
            | Expected::StaysNotApplicable { history, .. }
            | Expected::IntegrityCaught { history, .. } => {
                tests.insert(history.witness);
            }
            _ => {}
        }
    }
    for contract in &manifest.contracts {
        tests.extend(contract.proving.iter().copied());
    }
    for entry in NARROWING {
        if let NarrowingTest::Test(test) = entry.test {
            tests.insert(test);
        }
    }
    for test in tests {
        assert_eq!(test_exists(test), Ok(()));
    }
    for test in COMPOSITION_CHECKS {
        assert_eq!(test_exists(sim_test(rows::ACCEPTANCE_TESTS, test)), Ok(()));
    }
    // C3 is proved by its whole suite: every runnable test of
    // `tests/replay_exact.rs`, both ways.
    let suite = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join(contracts::REPLAY_EXACT),
    )
    .unwrap();
    let runnable = |source: &str| -> BTreeSet<String> {
        test_fns(source)
            .into_iter()
            .filter(|(_, gated)| !gated)
            .map(|(name, _)| name)
            .collect()
    };
    let c3 = |manifest: &Manifest| -> BTreeSet<String> {
        manifest
            .contracts
            .iter()
            .find(|contract| contract.id == ContractId::C3)
            .unwrap()
            .proving
            .iter()
            .filter(|test| test.package == "zakura-sim" && test.file == contracts::REPLAY_EXACT)
            .map(|test| test.test.to_owned())
            .collect()
    };
    assert_eq!(c3(&manifest), runnable(&suite));
    // C10 and C16 are proved by exactly the tests written here, by full
    // identity, apart from the contracts table: C10 by each evidence
    // source's checkpoint controls (the reactor tap's, the storage
    // checkpoint's and the transport checkpoint's), C16 by its two registry
    // tests.
    let proving = |manifest: &Manifest, id: ContractId| -> BTreeSet<TestRef> {
        manifest
            .contracts
            .iter()
            .find(|contract| contract.id == id)
            .unwrap()
            .proving
            .iter()
            .copied()
            .collect()
    };
    let pinned = |tests: &[(&'static str, &'static str)]| -> BTreeSet<TestRef> {
        tests
            .iter()
            .map(|(file, test)| sim_test(file, test))
            .collect()
    };
    let tap = "tests/header_tap.rs";
    let storage = "tests/header_controlled.rs";
    let c10 = pinned(&[
        (tap, "each_tap_is_checked_on_its_own"),
        (tap, "the_tap_is_checked_live_after_the_last_observation"),
        (tap, "a_tap_the_run_never_observes_fails_at_setup"),
        (tap, "a_skipped_tap_observation_ends_the_run_there"),
        (
            tap,
            "tap_events_delivered_anywhere_but_the_run_log_end_the_run",
        ),
        (tap, "tap_events_must_reach_the_log_unchanged"),
        (tap, "a_tap_from_a_dropped_run_fails_a_later_one"),
        (tap, "a_tap_dropped_during_the_run_fails_it"),
        (tap, "an_emission_outside_the_run_fails_it"),
        (
            storage,
            "storage_delivered_anywhere_but_the_run_log_ends_the_run",
        ),
        (storage, "a_skipped_observation_ends_the_run_there"),
        (storage, "storage_the_run_never_observes_fails_at_setup"),
        (storage, "storage_entries_must_reach_the_log_unchanged"),
        (storage, "storage_dropped_during_the_run_fails_it"),
        (storage, "storage_dropped_after_the_last_step_fails_the_run"),
        (
            "tests/header_wire.rs",
            "a_lost_or_altered_session_row_fails_the_run",
        ),
    ]);
    let c16 = pinned(&[
        (
            "tests/registry.rs",
            "known_and_active_failures_at_one_boundary",
        ),
        ("tests/registry.rs", "every_violation_retained"),
    ]);
    assert_eq!(proving(&manifest, ContractId::C10), c10);
    assert_eq!(proving(&manifest, ContractId::C16), c16);
    // A control: one storage test dropped, its file still represented.
    let mut dropped = manifest.clone();
    let at = dropped
        .contracts
        .iter()
        .position(|contract| contract.id == ContractId::C10)
        .unwrap();
    let kept: Vec<TestRef> = dropped.contracts[at]
        .proving
        .iter()
        .copied()
        .filter(|test| {
            *test
                != sim_test(
                    "tests/header_controlled.rs",
                    "storage_dropped_after_the_last_step_fails_the_run",
                )
        })
        .collect();
    dropped.contracts[at].proving = Box::leak(kept.into_boxed_slice());
    assert_ne!(proving(&dropped, ContractId::C10), c10);
    // One key off: a pinned test swapped for another test in the same
    // package and file, for each pinned set.
    for (id, pinned, from, to) in [
        (
            ContractId::C10,
            &c10,
            sim_test("tests/header_tap.rs", "each_tap_is_checked_on_its_own"),
            sim_test("tests/header_tap.rs", "another_tap_test"),
        ),
        (
            ContractId::C16,
            &c16,
            sim_test("tests/registry.rs", "every_violation_retained"),
            sim_test("tests/registry.rs", "another_registry_test"),
        ),
    ] {
        let mut swapped = manifest.clone();
        let at = swapped
            .contracts
            .iter()
            .position(|contract| contract.id == id)
            .unwrap();
        let tests: Vec<TestRef> = swapped.contracts[at]
            .proving
            .iter()
            .map(|test| if *test == from { to } else { *test })
            .collect();
        swapped.contracts[at].proving = Box::leak(tests.into_boxed_slice());
        assert_ne!(proving(&swapped, id), *pinned, "{id:?}");
    }
    // A control: a suite test C3 doesn't name.
    assert_ne!(
        c3(&manifest),
        runnable(&format!(
            "{suite}
#[test]
fn an_unnamed_replay_test() {{}}
"
        ))
    );
    assert!(test_exists(sim_test("tests/header_acceptance.rs", "no_such_test")).is_err());
    assert!(test_exists(sim_test("tests/no_such_file.rs", "a_test")).is_err());
    let elsewhere = TestRef {
        package: "xtask",
        ..sim_test("tests/header_acceptance.rs", "every_referenced_test_exists")
    };
    assert!(test_exists(elsewhere).is_err());
    // A unit test in another workspace package, by its module path.
    let xtask = TestRef {
        package: "xtask",
        file: "src/planted.rs",
        test: "planted::tests::planted_passes_only_on_its_own_id",
    };
    assert_eq!(test_exists(xtask), Ok(()));
    assert!(test_exists(TestRef {
        test: "planted::wrong::planted_passes_only_on_its_own_id",
        ..xtask
    })
    .is_err());
    assert!(test_exists(TestRef {
        test: "planted_passes_only_on_its_own_id",
        ..xtask
    })
    .is_err());
    // The scan reads full names through doc comments, stacked attributes and
    // inline modules, and a cfg on the function itself.
    let found = test_fns(
        "#[test]\n/// Doc.\n#[allow(unused)]\nfn a() {}\n\
         mod m {\n    #[test]\n    fn a() {}\n}\n\
         #[test]\n#[cfg(unix)]\nfn b() {}\n\
         #[test]\n#[ignore = \"slow\"]\nfn c() {}\n\
         #[tokio::test]\nasync fn d() {}\n\
         #[cfg(any())]\nmod off {\n    #[test]\n    fn f() {}\n}\n\
         #[test]\n#[cfg(all(test, zakura_sim))]\nfn h() {}\n\
         #[test]\n#[cfg(all(test, windows))]\nfn i() {}\n\
         #[test]\n#[cfg(not(feature = \"slow\"))]\nfn j() {}\n\
         #[cfg(test)]\nmod on {\n    #[test]\n    #[cfg(zakura_sim)]\n    fn g() {}\n}\n\
         #[tokio::test(flavor = \"multi_thread\")]\nasync fn e() {}\n\
         fn not_a_test() {}\n",
    );
    let expected: BTreeMap<String, bool> = [
        ("a", false),
        ("m::a", false),
        ("b", true),
        ("c", true),
        ("d", false),
        ("e", false),
        ("off::f", true),
        ("on::g", false),
        ("h", false),
        ("i", true),
        ("j", false),
    ]
    .map(|(name, cfg)| (name.to_owned(), cfg))
    .into_iter()
    .collect();
    assert_eq!(found, expected);
    assert_eq!(module_prefix("src/planted.rs"), ["planted"]);
    assert_eq!(module_prefix("src/a/mod.rs"), ["a"]);
    assert!(module_prefix("src/main.rs").is_empty());
    assert!(module_prefix("tests/t.rs").is_empty());
    assert!(test_exists(TestRef {
        package: "no-such-package",
        ..xtask
    })
    .is_err());
    // A real `#[tokio::test]` resolves, and an ignored test doesn't.
    assert_eq!(
        test_exists(TestRef {
            package: "zakura-storage",
            file: "tests/header_controlled.rs",
            test: "sweeps_and_abandonment_belong_to_their_completion",
        }),
        Ok(())
    );
    assert!(test_exists(sim_test(
        "tests/replay_exact.rs",
        "regenerate_discovery_book_input"
    ))
    .is_err());
}

/// Checks a case's `set` holds its own ID, and each other member is a
/// broader claim [`NESTS`] lists over it.
fn set_allowed(case: CaseRef, set: &[&str]) -> Result<(), String> {
    if !set.contains(&case.property) {
        return Err(format!("{}'s set leaves out its own ID", case.name));
    }
    for member in set.iter().filter(|member| **member != case.property) {
        let nested = NESTS
            .iter()
            .any(|nest| nest.broader == *member && nest.over.contains(&case.property));
        if !nested {
            return Err(format!(
                "{member} isn't a broader claim over {}",
                case.property
            ));
        }
    }
    Ok(())
}

#[test]
fn every_nested_set_is_allowed_by_nests() {
    for result in Manifest::committed().expected {
        if let Expected::Caught { case, set, .. } = result.expected {
            assert_eq!(set_allowed(case, set), Ok(()));
        }
    }
    let cadence = CaseRef {
        property: "header-sync/s5v9/status/cadence-never-closes",
        name: "punished-burst",
    };
    assert_eq!(
        set_allowed(
            cadence,
            &[cadence.property, "header-sync/s5v9/conformant-never-closes"]
        ),
        Ok(())
    );
    // The honest-peer case's planned set: punishing a peer marked honest
    // breaks conformant-never-closes too.
    let honest = CaseRef {
        property: "session/honest-never-punished",
        name: "punished-honest",
    };
    assert_eq!(
        set_allowed(
            honest,
            &[honest.property, "header-sync/s5v9/conformant-never-closes"]
        ),
        Ok(())
    );
    // An unrelated ID refused, even when the observed set would equal it.
    assert!(set_allowed(
        cadence,
        &[cadence.property, "header-sync/chunk-budget-bound"]
    )
    .is_err());
    assert!(set_allowed(cadence, &["header-sync/s5v9/conformant-never-closes"]).is_err());
    // The broader claim the wrong way round.
    let broad = CaseRef {
        property: "header-sync/s5v9/conformant-never-closes",
        name: "punished",
    };
    assert!(set_allowed(
        broad,
        &[
            broad.property,
            "header-sync/s5v9/status/cadence-never-closes"
        ]
    )
    .is_err());
}

/// Every problem with `table` against [`REQUIRED_CONTRACTS`]: a required ID
/// with no row or two, a row no ID requires, and a satisfied contract with
/// no proving test.
fn contract_errors(table: &[Contract]) -> Vec<String> {
    let mut errors = Vec::new();
    for id in REQUIRED_CONTRACTS {
        let rows = table.iter().filter(|contract| contract.id == *id).count();
        if rows != 1 {
            errors.push(format!("{id:?} has {rows} rows"));
        }
    }
    for contract in table {
        if !REQUIRED_CONTRACTS.contains(&contract.id) {
            errors.push(format!("{:?} isn't required", contract.id));
        }
        if contract.state == ContractState::Satisfied && contract.proving.is_empty() {
            errors.push(format!(
                "{:?} is satisfied with no proving test",
                contract.id
            ));
        }
    }
    errors
}

#[test]
fn the_contracts_table_has_one_row_per_required_contract() {
    let table = header_acceptance::contract_table();
    assert_eq!(contract_errors(&table), Vec::<String>::new());
    let mut deleted = table.clone();
    deleted.retain(|contract| contract.id != ContractId::C18);
    assert!(!contract_errors(&deleted).is_empty());
    let mut doubled = table.clone();
    doubled.push(table[0]);
    assert!(!contract_errors(&doubled).is_empty());
    let mut unproved = table.clone();
    unproved[0].state = ContractState::Satisfied;
    assert!(!contract_errors(&unproved).is_empty());
}

#[test]
fn every_narrowing_entry_has_a_test_or_its_pr() {
    for entry in NARROWING {
        match entry.test {
            NarrowingTest::Test(_) => {}
            NarrowingTest::Pending { until } => assert!(!until.is_empty(), "{}", entry.entry),
        }
        assert!(!entry.contracts.is_empty());
        for contract in entry.contracts {
            assert!(REQUIRED_CONTRACTS.contains(contract), "{}", entry.entry);
        }
    }
}

/// What a transitional row may wait for: the later hdr-14 PRs, Adam's
/// harness fixes, the hdr-9 to hdr-12 PRs that move native mutations, and
/// E1, whose reason-bearing `NotApplicable` the expected-NA lists need.
/// A transitional row and the result that replaces it would cover one key
/// twice, which `every_required_key_is_covered_once_with_its_gates` refuses.
/// A PR that merges leaves this list, so any `Open` still naming it fails
/// `open_errors`, including a gate requirement's, whose replacement is a
/// `Permit::Test` that PR switches to.
const OPEN_PRS: &[&str] = &[
    "hdr-14.3",
    "hdr-14.4",
    "hdr-14.5",
    "hdr-14.6",
    "hdr-14.7a",
    "hdr-14.7b",
    "hdr-14.8",
    "hf-1",
    "hf-2",
    "hf-3",
    "hf-4",
    "hf-6",
    "hf-7",
    "hf-8",
    "hf-9",
    "hdr-9a.2",
    "hdr-9c.1a",
    "hdr-9c.1b",
    "hdr-9c.2a",
    "hdr-9d.1",
    "hdr-10b",
    "hdr-12b",
    "E1",
];

#[test]
fn every_transitional_row_names_an_open_pr() {
    let manifest = Manifest::committed();
    assert_eq!(open_errors(&manifest.expected), Vec::<String>::new());
    // An Open whose owner is empty, or a transitional catch kept open once a
    // Caught for its property exists, even one covering only its own case.
    let mut ownerless = manifest.expected.clone();
    let at = ownerless
        .iter()
        .position(|r| matches!(r.expected, Expected::Open { .. }))
        .unwrap();
    if let Expected::Open { owner, .. } = &mut ownerless[at].expected {
        *owner = "";
    }
    assert!(!open_errors(&ownerless).is_empty());
    let penalized = "header-sync/header-fault-penalized";
    let mut replaced = manifest.expected.clone();
    let caught = replaced
        .iter()
        .find_map(|r| match r.expected {
            Expected::Caught { .. } => Some(r.expected.clone()),
            _ => None,
        })
        .unwrap();
    let Expected::Caught {
        harness,
        found,
        not_applicable,
        companions,
        ..
    } = caught
    else {
        unreachable!()
    };
    replaced.push(header_acceptance::ExpectedResult {
        keys: vec![Key::Case {
            property: penalized.into(),
            name: "late-fault".into(),
        }],
        gates: vec![Gate::G2],
        expected: Expected::Caught {
            case: CaseRef {
                property: penalized,
                name: "late-fault",
            },
            harness,
            set: &["header-sync/header-fault-penalized"],
            found,
            not_applicable,
            companions,
        },
    });
    assert!(!open_errors(&replaced).is_empty());
}

/// Every problem with the `Open`s in `expected`: an empty reason or owner,
/// a transitional `until` naming no open PR, and a transitional passing or
/// catch key some other expected result would already satisfy.
fn open_errors(expected: &[ExpectedResult]) -> Vec<String> {
    let mut errors = Vec::new();
    for result in expected {
        let Expected::Open {
            until,
            reason,
            owner,
            ..
        } = result.expected
        else {
            continue;
        };
        if reason.is_empty() || owner.is_empty() {
            errors.push(format!(
                "{:?}: an Open without its reason or owner",
                result.keys
            ));
        }
        let Some(until) = until else { continue };
        if !OPEN_PRS.contains(&until) {
            errors.push(format!("{:?}: {until}", result.keys));
        }
        for key in &result.keys {
            if !matches!(key, Key::Passing { .. } | Key::Catch { .. }) {
                continue;
            }
            let replaced = expected.iter().any(|other| {
                !matches!(other.expected, Expected::Open { .. })
                    && key.covers(&other.expected, NARROWING)
            });
            if replaced {
                errors.push(format!("{key:?}: still open beside its replacement"));
            }
        }
    }
    errors
}

#[test]
fn the_gate_requirements_match_their_obligations() {
    let results: BTreeMap<&str, BTreeSet<Gate>> = GATE_REQUIREMENTS
        .iter()
        .map(|r| (r.name, r.gates.iter().copied().collect()))
        .collect();
    let obligations: BTreeMap<&str, BTreeSet<Gate>> = GATE_OBLIGATIONS
        .iter()
        .map(|o| (o.name, o.gates.iter().copied().collect()))
        .collect();
    assert_eq!(
        results.len(),
        GATE_REQUIREMENTS.len(),
        "one result per name"
    );
    assert_eq!(
        obligations.len(),
        GATE_OBLIGATIONS.len(),
        "one obligation per name"
    );
    assert_eq!(results, obligations);
    for requirement in GATE_REQUIREMENTS {
        let key = Key::Requirement {
            name: requirement.name.into(),
        };
        assert!(
            key.covers(&requirement.expected, NARROWING),
            "{}",
            requirement.name
        );
    }
    // Another requirement's result, with the same gate, doesn't satisfy it:
    // the standing no-request-row exclusion copied onto a crossing history.
    let standing = GATE_REQUIREMENTS
        .iter()
        .find(|r| r.name == "gate-3/no-request-row")
        .unwrap();
    let crossing = Key::Requirement {
        name: "gate-3/crossing/crossing-close".into(),
    };
    assert!(!crossing.covers(&standing.expected, NARROWING));
    // A requirement's own Open with only its owner changed.
    for name in ["gate-2/bad-content-close-claim", "gate-1/a-a-and-a-b-a"] {
        let requirement = GATE_REQUIREMENTS.iter().find(|r| r.name == name).unwrap();
        let mut changed = requirement.expected.clone();
        if let Expected::Open { owner, .. } = &mut changed {
            *owner = "someone else";
        }
        let key = Key::Requirement { name: name.into() };
        assert!(!key.covers(&changed, NARROWING), "{name}");
    }
}

/// The wire witness file.
const WIRE_WITNESS: &str = "tests/header_wire_witness.rs";

/// Each history a row judges for an ID other than the one it's run for, as
/// `(history, ID)`: `both-ways`, run for `conformant-never-closes`,
/// also judges each sender rule; each `over-owed-closes` stays
/// `NotApplicable` in its row's `unsolicited` history; and the storage
/// witness's first plan judges `harness/direct-dispatch`.
const SHARED_HISTORIES: &[(HistoryRef, &str)] = &[
    (
        HistoryRef {
            harness: "header-sync/wire",
            witness: sim_test(
                WIRE_WITNESS,
                "every_generated_property_applies_in_its_witness",
            ),
            case: "both-ways",
        },
        "header-sync/s5v9/status/sender-cadence",
    ),
    (
        HistoryRef {
            harness: "header-sync/wire",
            witness: sim_test(
                WIRE_WITNESS,
                "every_generated_property_applies_in_its_witness",
            ),
            case: "both-ways",
        },
        "header-sync/s5v9/subscribe-headers/sender-within-live",
    ),
    (
        HistoryRef {
            harness: "header-sync/wire",
            witness: sim_test(
                WIRE_WITNESS,
                "every_generated_property_applies_in_its_witness",
            ),
            case: "both-ways",
        },
        "header-sync/s5v9/subscribe-headers/sender-within-credit",
    ),
    (
        HistoryRef {
            harness: "header-sync/wire",
            witness: sim_test(
                WIRE_WITNESS,
                "every_generated_property_applies_in_its_witness",
            ),
            case: "both-ways",
        },
        "header-sync/s5v9/headers/sender-answers-open",
    ),
    (
        HistoryRef {
            harness: "header-sync/wire",
            witness: sim_test(
                WIRE_WITNESS,
                "every_generated_property_applies_in_its_witness",
            ),
            case: "both-ways",
        },
        "header-sync/s5v9/headers-outcome/sender-ends-once",
    ),
    (
        HistoryRef {
            harness: "header-sync/wire",
            witness: sim_test(WIRE_WITNESS, "headers_faults_apply_in_their_witnesses"),
            case: "headers/unsolicited-closes",
        },
        "header-sync/s5v9/headers/over-owed-closes",
    ),
    (
        HistoryRef {
            harness: "header-sync/wire",
            witness: sim_test(
                WIRE_WITNESS,
                "headers_outcome_faults_apply_in_their_witnesses",
            ),
            case: "headers-outcome/unsolicited-closes",
        },
        "header-sync/s5v9/headers-outcome/over-owed-closes",
    ),
    (
        HistoryRef {
            harness: "header-sync/storage",
            witness: sim_test(
                "tests/header_node.rs",
                "node_syncs_from_a_mirror_on_controlled_storage",
            ),
            case: "plan 0 of inputs(32, 256)",
        },
        "harness/direct-dispatch",
    ),
];

/// Every row history that isn't pinned for its row's property: a row's
/// `Holds` or `StaysNotApplicable` must judge a history pinned for that ID,
/// or one [`SHARED_HISTORIES`] names.
fn row_history_errors(rows: &[header_acceptance::Row]) -> Vec<String> {
    let pinned = pinned_histories();
    let mut errors = Vec::new();
    for row in rows {
        for result in &row.expected {
            let (Expected::Holds {
                property, history, ..
            }
            | Expected::StaysNotApplicable {
                property, history, ..
            }) = result.expected
            else {
                continue;
            };
            let shared = SHARED_HISTORIES
                .iter()
                .any(|(shared, id)| *shared == history && *id == property);
            if !pinned.contains(&(history, property)) && !shared {
                errors.push(format!("{property}: judges {history:?}"));
            }
        }
    }
    errors
}

/// Each row judges a history pinned for its own ID, so a row naming
/// another case of the right witness test is refused before the manifest
/// attaches each history's own pass.
#[test]
fn every_row_judges_a_pinned_history() {
    let rows = rows::rows();
    assert_eq!(row_history_errors(&rows), Vec::<String>::new());
    let mut swapped = rows.clone();
    let at = swapped
        .iter()
        .position(|row| row.property == "header-sync/s5v9/status/short-payload-closes")
        .unwrap();
    for result in &mut swapped[at].expected {
        if let Expected::Holds { history, .. } = &mut result.expected {
            history.case = "status/long-payload-closes";
        }
    }
    assert_eq!(row_history_errors(&swapped).len(), 1);
    // A shared history one field off, here only its harness, is refused.
    let mut moved = rows.clone();
    let at = moved
        .iter()
        .position(|row| row.property == "harness/direct-dispatch")
        .unwrap();
    for result in &mut moved[at].expected {
        if let Expected::Holds { history, .. } = &mut result.expected {
            history.harness = "header-sync/release-wake";
        }
    }
    assert_eq!(row_history_errors(&moved).len(), 1);
}

/// Every pair of expected results that share an [`ExpectedId`] but don't
/// expect exactly the same thing. The tool runs one task per identity, and
/// its one result serves every key of every expectation with that
/// identity, so sharing is sound only between identical expectations.
fn identity_errors(expected: &[ExpectedResult]) -> Vec<String> {
    let mut by_id: BTreeMap<ExpectedId, BTreeSet<String>> = BTreeMap::new();
    for result in expected {
        if let Some(id) = result.expected.id() {
            by_id
                .entry(id)
                .or_default()
                .insert(format!("{:?}", result.expected));
        }
    }
    by_id
        .into_iter()
        .filter(|(_, expectations)| expectations.len() > 1)
        .map(|(id, _)| format!("{id:?} names different expectations"))
        .collect()
}

/// Each result identity names one expectation. C16's proving test is also
/// gate 2's same-step test, so two expected results share its `Passes`
/// identity, and the one result serves both keys with each one's gates.
#[test]
fn each_result_identity_names_one_expectation() {
    let manifest = Manifest::committed();
    assert_eq!(identity_errors(&manifest.expected), Vec::<String>::new());
    let shared = sim_test(
        "tests/registry.rs",
        "known_and_active_failures_at_one_boundary",
    );
    let sharing: Vec<&ExpectedResult> = manifest
        .expected
        .iter()
        .filter(|r| matches!(r.expected, Expected::Passes { test } if test == shared))
        .collect();
    assert_eq!(sharing.len(), 2, "{shared:?}");
    assert_ne!(sharing[0].keys, sharing[1].keys);
    // A control: a second catch of one case on another harness shares the
    // case's identity but expects something else.
    let mut changed = manifest.expected.clone();
    let mut caught = changed
        .iter()
        .find(|r| matches!(r.expected, Expected::Caught { .. }))
        .unwrap()
        .clone();
    if let Expected::Caught { harness, .. } = &mut caught.expected {
        *harness = "header-sync/release-wake";
    }
    changed.push(caught);
    assert_eq!(identity_errors(&changed).len(), 1);
}

/// Every test in this file is a composition check the acceptance run
/// executes at the subject commit, and every check is a test here.
#[test]
fn the_composition_checks_are_every_static_check() {
    let source =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(rows::ACCEPTANCE_TESTS))
            .unwrap();
    let tests = |source: &str| -> BTreeSet<String> { test_fns(source).into_keys().collect() };
    let listed: BTreeSet<String> = COMPOSITION_CHECKS.iter().map(|c| (*c).to_owned()).collect();
    assert_eq!(tests(&source), listed);
    let added = format!("{source}\n#[test]\nfn an_unlisted_check() {{}}\n");
    assert_ne!(tests(&added), listed);
    for unlisted in [
        "#[test]\n#[allow(clippy::unwrap_used)]\nfn an_unlisted_check() {}",
        "#[test]\n/// A documented check.\nfn an_unlisted_check() {}",
        "mod nested {\n    #[test]\n    fn the_rows_equal_the_registered_header_ids() {}\n}",
    ] {
        assert_ne!(
            tests(&format!("{source}\n{unlisted}\n")),
            listed,
            "{unlisted}"
        );
    }
}

/// Every header harness's needs are read, so each owes its missing-source
/// keys, and a harness the reader doesn't know is refused.
#[test]
fn every_header_harness_has_its_needs_read() {
    assert!(header_acceptance::needs_of(rows::HEADER_HARNESSES).is_ok());
    let mut added = rows::HEADER_HARNESSES.to_vec();
    added.push("header-sync/unread");
    assert!(header_acceptance::needs_of(&added).is_err());
}
