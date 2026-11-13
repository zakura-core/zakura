//! hdr-14's acceptance manifest: its static checks against the registry,
//! `COVERAGE` and the committed directories, each with a control that one
//! defect makes it refuse, and the gate evaluator's negative controls and
//! positive tests (Rule 16).
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
            contracts::{
                self, Contract, ContractId, ContractState, Narrowing, NarrowingTest, RunBy,
            },
            evaluate::{
                Actions, Actual, Companion, End, ExecMeta, Execution, Report, ResultFile,
                ResultRecord, TestOutcome, TestRan,
            },
            reasons::{NA_REASONS, NOT_HEADER},
            required::{gate_rule, required_keys, Inventories, Registered, GATE_OBLIGATIONS},
            rows::{self, NativeClass, HEADER_HARNESSES},
            sim_test, Approval, CaseRef, CaseRun, Expected, ExpectedId, ExpectedResult, Found,
            Gate, HistoryRef, Key, Manifest, ObligationState, RunId, TestRef, Tier,
            COMPOSITION_CHECKS, GATE_REQUIREMENTS, NARROWING, NATIVE_MUTATIONS, NESTS,
            REQUIRED_CONTRACTS,
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

/// Every planted case of header sync, as the tool's plan counts them
/// (shared IDs' cases only on a header harness, `tool.rs`'s
/// `is_header_case`).
fn planted_cases() -> Vec<(String, String)> {
    header_acceptance::tool::planted_cases(&repo()).expect("the planted directories read")
}

/// The planted inventory reads a property at any depth under
/// `qa/planted`, keeps the header families' and the header IDs' cases, and
/// skips the rest. A shared ID's case with no readable harness is kept.
#[test]
fn the_planted_scan_reads_nested_properties() {
    let repo = tempfile::tempdir().unwrap();
    for (dir, name) in [
        ("header-sync/s5v9/status/reserved-flags-closes", "flags"),
        ("header-sync/s5v9/headers/reserved-flags-closes", "flags"),
        ("session/honest-never-punished", "punished"),
        ("block-sync/retained-byte-bound", "wide"),
    ] {
        let dir = repo.path().join("qa/planted").join(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{name}.patch")), "").unwrap();
    }
    let owned = |property: &str, name: &str| (property.to_owned(), name.to_owned());
    assert_eq!(
        header_acceptance::tool::planted_cases(repo.path()),
        Ok(vec![
            owned("header-sync/s5v9/headers/reserved-flags-closes", "flags"),
            owned("header-sync/s5v9/status/reserved-flags-closes", "flags"),
            owned("session/honest-never-punished", "punished"),
        ])
    );
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
    let native = native_dirs();
    let coverage = coverage_triples();
    let contracts = header_acceptance::contract_table();
    let planted_harnesses: BTreeMap<(String, String), String> = planted
        .iter()
        .filter_map(|(property, name)| {
            let harness = header_acceptance::planted_harness(&repo(), property, name).ok()?;
            Some(((property.clone(), name.clone()), harness))
        })
        .collect();
    let harness_caught: BTreeSet<(String, String)> = planted
        .iter()
        .filter(|(property, name)| {
            header_acceptance::planted_harness_caught(&repo(), property, name).unwrap_or(false)
        })
        .cloned()
        .collect();
    required_keys(&Inventories {
        registered: &registered,
        needs: &needs,
        planted: &planted,
        planted_harnesses: &planted_harnesses,
        harness_caught: &harness_caught,
        native: &native,
        coverage_tests: &coverage,
        contracts: &contracts,
        required_contracts: REQUIRED_CONTRACTS,
        narrowing: NARROWING,
        requirements: GATE_OBLIGATIONS,
        composition: COMPOSITION_CHECKS,
        reasons: NA_REASONS,
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
            approval: Approval::Pending,
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
    // A narrowing entry's key covered by another test than its entry's.
    refused(&|m| {
        let at = covering(m, &Key::NarrowingTest { entry: "N1".into() });
        if let Expected::Passes { test } = &mut m.expected[at].expected {
            test.test = "another_test";
        }
    });
    // No native mutation is still to move, so none takes a transitional
    // Open, even the one a move to a listed PR would write.
    refused(&|m| {
        let at = covering(
            m,
            &Key::Native {
                patch: "raw-budget-lease".into(),
            },
        );
        m.expected[at].expected = rows::native_open("hdr-14.7b");
    });
    // A transitional row naming another listed PR than its key's owner, or
    // only another owner, or only another reason.
    refused(&|m| {
        let at = covering(m, &Key::catch("sim/no-livelock"));
        if let Expected::Open { until, .. } = &mut m.expected[at].expected {
            *until = Some("hdr-14.5");
        }
    });
    refused(&|m| {
        let at = covering(m, &Key::catch("sim/no-livelock"));
        if let Expected::Open { owner, .. } = &mut m.expected[at].expected {
            *owner = "Evan's side";
        }
    });
    refused(&|m| {
        let at = covering(m, &Key::catch("sim/no-livelock"));
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
    // A tier exclusion copied from another tier of the same row, the
    // approved Real one onto Table.
    refused(&|m| {
        let id = "header-sync/chunk-budget-bound";
        let from = m.expected[covering(
            m,
            &Key::Tier {
                property: id.into(),
                tier: "Real".into(),
            },
        )]
        .expected
        .clone();
        let at = covering(
            m,
            &Key::Tier {
                property: id.into(),
                tier: "Table".into(),
            },
        );
        m.expected[at].expected = from;
    });
    // A tier approval is one rule's: approving chunk-budget-bound's Table
    // exclusion leaves another rule's Table exclusion pending, and a Table
    // result covers its key only once listed for that rule.
    let approved = [(
        "header-sync/chunk-budget-bound",
        header_acceptance::Tier::Table,
        header_acceptance::Approval::Adam { record: "a test" },
    )];
    let approval = |property| match rows::tier_exclusion_in(
        &approved,
        property,
        header_acceptance::Tier::Table,
    ) {
        Expected::Open { approval, .. } => approval,
        _ => unreachable!(),
    };
    assert!(approval("header-sync/chunk-budget-bound").approved());
    assert!(!approval("header-sync/teardown-returns-baseline").approved());
    let table = Key::Tier {
        property: "header-sync/chunk-budget-bound".into(),
        tier: "Table".into(),
    };
    assert!(!table.covers(
        &Expected::Passes {
            test: sim_test("tests/model_agreement.rs", "a_table_suite")
        },
        NARROWING
    ));
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
            &Key::MissingSource {
                harness: "header-sync/storage".into(),
                source: "Storage".into(),
            },
        );
        m.expected[at].expected = Expected::Passes {
            test: sim_test(
                "tests/header_acceptance_sources.rs",
                "wire_fails_without_each_source",
            ),
        };
    });
    // The retired mutation's catch of another failure than its pinned
    // witness's, or any Open, such as a ToMove's, isn't its own.
    refused(&|m| {
        let at = covering(
            m,
            &Key::Native {
                patch: "readmission-unchecked".into(),
            },
        );
        if let Expected::UnitCaught { witness, .. } = &mut m.expected[at].expected {
            witness.failure = "assertion `left == right` failed: cooldown 0ns";
        }
    });
    refused(&|m| {
        let at = covering(
            m,
            &Key::Native {
                patch: "readmission-unchecked".into(),
            },
        );
        m.expected[at].expected = rows::native_open("hdr-14.7b");
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
        // Both exempt rows judge a header history that isn't their COVERAGE
        // test's, so each fails once its exemption names another test.
        assert_eq!(witness_association_errors(&excepted, &other).len(), 2);
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
            "tests/header_reset_invalidates.rs",
            "every_witness_holds_and_applies",
            Some("header-sync/reset-invalidates-work"),
            [
                "held-locator ",
                "held-prepare ",
                "held-apply ",
                "held-reply ",
                "two-forks ",
                "session-replaced-read ",
                "tip-reuse ",
                "completion-before-adoption ",
                "quiet-reset ",
                "quiet-reset-deep ",
            ]
            .iter()
            .flat_map(|name| plans(name, "inputs(8, 256)", 8))
            .collect(),
        ),
        (
            STORAGE,
            "tests/header_stale_chain.rs",
            "every_witness_holds_and_applies",
            Some("header-sync/stale-chain-effect-inert"),
            [
                "pre-reset-commit ",
                "refused-across-reset ",
                "fork-stale-apply ",
                "reset-inside-status-floor ",
            ]
            .iter()
            .flat_map(|name| plans(name, "inputs(8, 256)", 8))
            .collect(),
        ),
        (
            STORAGE,
            "tests/header_authority.rs",
            "every_witness_holds_and_applies",
            Some("header-sync/commit-only-authorized"),
            [
                "forks-sharing-a-header ",
                "multi-page-sync ",
                "relinked-mid-round ",
                "harness-commit ",
            ]
            .iter()
            .flat_map(|name| plans(name, "inputs(8, 256)", 8))
            .collect(),
        ),
        (
            STORAGE,
            "tests/header_chain_selected.rs",
            "every_witness_holds_and_applies",
            Some("header-sync/expected-chain-selected"),
            [
                "stronger-first ",
                "stronger-last ",
                "three-forks ",
                "equal-work ",
                "heavier-offered-to-longer ",
                "invalidated-then-reconsidered ",
                "harness-commit ",
            ]
            .iter()
            .flat_map(|name| plans(name, "inputs(8, 256)", 8))
            .collect(),
        ),
        (
            STORAGE,
            "tests/header_publication.rs",
            "every_witness_holds_and_applies",
            Some("header-sync/state-publication-consistent"),
            [
                "startup-only ",
                "quiet-reset ",
                "invalidate-only ",
                "grow ",
                "reset-inside-status-floor ",
                "harness-commit ",
                "sync ",
                "served ",
                "served-one-header ",
                "served-from-a-lower-locator ",
                "served-in-pages ",
                "served-after-growth ",
                "served-across-a-reset ",
            ]
            .iter()
            .flat_map(|name| plans(name, "inputs(8, 256)", 8))
            .collect(),
        ),
        (
            STORAGE,
            "tests/header_outstanding.rs",
            "the_committed_witnesses_hold_outstanding_work_bounded",
            Some("header-sync/outstanding-work-bounded"),
            owned(&[
                "header-authority-forks-sharing-a-header",
                "header-authority-relinked-mid-round",
                "header-reset-held-apply",
                "header-reset-held-reply",
                "header-reset-quiet-reset",
                "header-reset-quiet-reset-deep",
                "header-outstanding-churn",
                "header-outstanding-serving-churn",
                "header-outstanding-serving-replaced",
                "header-outstanding-capacity-waits",
            ]),
        ),
        (
            "header-sync/repair",
            "tests/header_repair.rs",
            "the_committed_repair_inputs_hold_outstanding_work_bounded",
            Some("header-sync/outstanding-work-bounded"),
            owned(&[
                "committed header-repair-applied input",
                "committed header-repair-prepare-departs input",
                "committed header-repair-apply-departs input",
                "committed header-repair-supplier-pruned input",
            ]),
        ),
        (
            "header-sync/liveness",
            "tests/header_hold_release.rs",
            "the_committed_fixtures_match_their_names",
            Some("header-sync/hold-release-wakes"),
            owned(&[
                "header-hold-held-locator",
                "header-hold-held-prepare",
                "header-hold-held-apply",
                "header-hold-serving-read",
            ]),
        ),
        (
            "header-sync/liveness",
            "tests/header_recovery_progress.rs",
            "the_committed_fixtures_match_their_names",
            Some("header-sync/bounded-adversary-progress"),
            owned(&[
                "header-recovery-successor",
                "header-recovery-closes",
                "header-recovery-faults",
                "header-recovery-departure",
            ]),
        ),
        (
            "header-sync/liveness",
            "tests/header_peer_retirement.rs",
            "the_committed_fixtures_match_their_names",
            Some("header-sync/peer-retirement-progress"),
            owned(&[
                "header-retire-timeout",
                "header-retire-late-page",
                "header-retire-awaiting-close",
                "header-retire-preparing-close",
                "header-retire-receiving-close",
                "header-retire-receiving-relink",
                "header-retire-preparing-relink",
                "header-retire-applying-relink",
            ]),
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
            "the_close_relink_input_holds_stale_completion_inert",
            Some("header-sync/stale-completion-inert"),
            owned(&["committed close-relink input"]),
        ),
    ];
    // Gate 5's mixed resource witness, run for each of the four resource
    // rules.
    let mut groups = groups;
    for target in [
        "header-sync/chunk-budget-bound",
        "header-sync/teardown-returns-baseline",
        "header-sync/stale-completion-inert",
        "header-sync/outstanding-work-bounded",
    ] {
        groups.push((
            STORAGE,
            "tests/header_mixed_resources.rs",
            "the_mixed_fixture_holds_every_resource_rule",
            Some(target),
            owned(&["header-resources-mixed"]),
        ));
    }
    groups.extend([
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
        (
            "header-sync/wire",
            "tests/header_wire_prepare_fault.rs",
            "the_late_fault_fixture_penalizes_after_the_session_ended",
            Some("header-sync/header-fault-penalized"),
            owned(&["committed late-fault input"]),
        ),
    ]);
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
    assert_eq!(
        targets.len(),
        rows::history_targets().len(),
        "each history once per target"
    );
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
        // A duplicate native mutation's key shares its covering case's
        // catch, which the case's own row names, and so does a contract's
        // proving catch.
        if result
            .keys
            .iter()
            .all(|key| matches!(key, Key::Native { .. } | Key::ContractCatch { .. }))
        {
            continue;
        }
        let (case, machine, declared) = match result.expected {
            Expected::Caught { case, harness, .. }
            | Expected::HarnessCaught { case, harness, .. } => (case, false, harness),
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
                patched_not_applicable: None,
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
/// named by no catch, a duplicate whose covering case is, and a stale
/// move.
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
            NativeClass::Retired { reason, witness } => {
                if reason.is_empty() {
                    errors.push(format!("{}: no reason", mutation.patch));
                }
                if witness.failure.is_empty() || witness.shows.is_empty() {
                    errors.push(format!("{}: no required failure", mutation.patch));
                }
            }
            NativeClass::Duplicate { property, case } => {
                let named = expected.iter().any(|result| {
                    matches!(result.expected, Expected::Caught { case: c, .. }
                        if c.property == property && c.name == case)
                });
                if !cases.contains(&(property.to_owned(), case.to_owned())) || !named {
                    errors.push(format!(
                        "{}: its covering case {property}/{case} is missing or unnamed",
                        mutation.patch
                    ));
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
    /// It's retired, caught by this test outside the simulator, by its
    /// full libtest name.
    Retired(&'static str),
    /// It's retired as a duplicate of this planted case: its property,
    /// then its name.
    Duplicate(&'static str, &'static str),
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
        NativePin::Duplicate("header-sync/commit-only-authorized", "stored-order"),
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
    (
        "readmission-unchecked",
        NativePin::Retired(
            "zakura::header_sync::reactor::tests::observer::\
             a_late_fault_cools_down_the_peer_and_refuses_its_reconnect",
        ),
    ),
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
        NativePin::Duplicate(
            "header-sync/commit-only-authorized",
            "dropped-payload-header",
        ),
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
        NativePin::Duplicate("header-sync/outstanding-work-bounded", "retention-skipped"),
    ),
    (
        "skipped-staging",
        NativePin::Move("header-sync/commit-only-authorized", "hdr-9c.1a"),
    ),
    (
        "stale-locator-round",
        NativePin::Move("header-sync/reset-invalidates-work", "hdr-14.5"),
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
            (NativeClass::Retired { witness, .. }, Some(NativePin::Retired(test))) => {
                witness.test.test == test
            }
            (NativeClass::Duplicate { property, case }, Some(NativePin::Duplicate(p, c))) => {
                property == p && case == c
            }
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
        ("hdr-14.5", "header-sync/commit-only-authorized"),
        ("hdr-9c.2a", "header-sync/teardown-returns-baseline"),
    ] {
        let mut changed = NATIVE_MUTATIONS.to_vec();
        let at = changed.iter().position(|m| m.patch == "own9-join").unwrap();
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
    let retired = NATIVE_MUTATIONS
        .iter()
        .find(|m| m.patch == "readmission-unchecked")
        .unwrap()
        .class;
    let NativeClass::Retired { witness, .. } = retired else {
        panic!("readmission-unchecked is retired");
    };
    for (patch, class) in [
        (
            "raw-budget-reservation",
            NativeClass::Retired {
                reason: "no longer reachable",
                witness,
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
                (
                    NativeClass::Retired { witness, .. },
                    Expected::UnitCaught {
                        patch: p,
                        witness: w,
                    },
                ) if p == patch && *w == witness => {}
                (NativeClass::Duplicate { property, case }, Expected::Caught { case: c, .. })
                    if c.property == property && c.name == case => {}
                (class, expected) => panic!("{patch}: {class:?} as {:?}", expected.kind()),
            }
        }
    }
    // A retired mutation stays a required key, on gate 6, which only its
    // witness's catch verifies.
    assert_eq!(
        committed_required(&manifest).get(&Key::Native {
            patch: "readmission-unchecked".into(),
        }),
        Some(&BTreeSet::from([Gate::G6]))
    );
    // A duplicate keeps its directory, so it stays a required key on gate
    // 6, which only its covering case's catch verifies. It's refused when
    // that case is missing or no catch names it, and its pin refuses it
    // reclassified as a plain retirement or onto another case.
    assert_eq!(
        committed_required(&manifest).get(&Key::Native {
            patch: "short-payload".into(),
        }),
        Some(&BTreeSet::from([Gate::G6]))
    );
    let covering = (
        "header-sync/commit-only-authorized".to_owned(),
        "dropped-payload-header".to_owned(),
    );
    let uncovered: Vec<(String, String)> =
        cases.iter().filter(|c| **c != covering).cloned().collect();
    assert_eq!(
        native_errors(
            NATIVE_MUTATIONS,
            &native_dirs(),
            &uncovered,
            &manifest.expected
        )
        .len(),
        1
    );
    let unnamed: Vec<ExpectedResult> = manifest
        .expected
        .iter()
        .filter(|r| {
            !matches!(r.expected, Expected::Caught { case, .. }
                if case.property == covering.0 && case.name == covering.1)
        })
        .cloned()
        .collect();
    assert_eq!(
        native_errors(NATIVE_MUTATIONS, &native_dirs(), &cases, &unnamed).len(),
        1
    );
    for class in [
        NativeClass::Retired {
            reason: "a duplicate",
            witness,
        },
        NativeClass::Duplicate {
            property: "header-sync/commit-only-authorized",
            case: "stored-order",
        },
    ] {
        let mut changed = NATIVE_MUTATIONS.to_vec();
        let at = changed
            .iter()
            .position(|m| m.patch == "short-payload")
            .unwrap();
        changed[at].class = class;
        assert_eq!(native_pin_errors(&changed).len(), 1, "{class:?}");
    }
    // A mutation whose directory and classification are both removed is
    // refused, since the inventory still names it: here the moved
    // `own9-join`, whose directory is gone.
    assert!(!native_dirs().iter().any(|dir| dir == "own9-join"));
    let dropped: Vec<rows::NativeMutation> = NATIVE_MUTATIONS
        .iter()
        .filter(|m| m.patch != "own9-join")
        .copied()
        .collect();
    assert!(!native_errors(&dropped, &native_dirs(), &cases, &manifest.expected).is_empty());
    // A move's record outlives its directory: once moved, its planted case
    // must exist under its property and a catch must name it. The
    // committed move holds. Without its planted case, without the Caught
    // naming it, onto an existing, caught case of another name, or under
    // another property, it's refused.
    let own9 = (
        "header-sync/commit-only-authorized".to_owned(),
        "own9-join".to_owned(),
    );
    assert!(cases.contains(&own9));
    let without_case: Vec<(String, String)> =
        cases.iter().filter(|c| **c != own9).cloned().collect();
    let unnamed: Vec<ExpectedResult> = manifest
        .expected
        .iter()
        .filter(|r| {
            !matches!(r.expected, Expected::Caught { case, .. }
                if case.property == own9.0 && case.name == own9.1)
        })
        .cloned()
        .collect();
    assert_eq!(unnamed.len() + 1, manifest.expected.len());
    let mut moved = NATIVE_MUTATIONS.to_vec();
    let at = moved.iter().position(|m| m.patch == "own9-join").unwrap();
    for (property, case, cases, expected, ok) in [
        (
            "header-sync/commit-only-authorized",
            "own9-join",
            &cases,
            &manifest.expected,
            true,
        ),
        (
            "header-sync/commit-only-authorized",
            "own9-join",
            &without_case,
            &manifest.expected,
            false,
        ),
        (
            "header-sync/commit-only-authorized",
            "own9-join",
            &cases,
            &unnamed,
            false,
        ),
        (
            "header-sync/commit-only-authorized",
            "wrong-peer",
            &cases,
            &manifest.expected,
            false,
        ),
        (
            "header-sync/teardown-returns-baseline",
            "own9-join",
            &cases,
            &manifest.expected,
            false,
        ),
    ] {
        moved[at].class = NativeClass::Moved { property, case };
        let errors = native_errors(&moved, &native_dirs(), cases, expected);
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
/// from running: an `#[ignore]`, or a `cfg` the acceptance run's test
/// builds leave off. They turn on `test` and `zakura_sim`, and run on
/// Linux, the only host whose record counts (`Containment::Subreaper`), so
/// `unix` and `target_os = "linux"` are on too. Doc comments and other
/// attributes may sit between the test attribute and the `fn`.
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
    /// where `test`, `zakura_sim`, `unix` and `target_os = "linux"` are on
    /// and nothing else is known on: `all`, `any` and `not` evaluate, and
    /// any other option is off.
    fn enabled(predicate: TokenStream) -> bool {
        let tokens: Vec<TokenTree> = predicate.into_iter().collect();
        match tokens.as_slice() {
            [TokenTree::Ident(name)] => name == "test" || name == "zakura_sim" || name == "unix",
            [TokenTree::Ident(name), TokenTree::Punct(eq), TokenTree::Literal(value)]
                if eq.as_char() == '=' =>
            {
                name == "target_os" && value.to_string() == "\"linux\""
            }
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

/// Every proving catch of `manifest`'s contracts that doesn't name a
/// committed planted case with a row's `Caught`.
fn catch_errors(manifest: &Manifest) -> Vec<String> {
    let mut errors = Vec::new();
    for contract in &manifest.contracts {
        for case in contract.proving_catches {
            let patch = repo()
                .join("qa/planted")
                .join(case.property)
                .join(format!("{}.patch", case.name));
            if !patch.is_file() {
                errors.push(format!(
                    "{:?}'s catch {}/{} has no planted case",
                    contract.id, case.property, case.name
                ));
            }
            let caught = manifest.expected.iter().any(|result| {
                result
                    .keys
                    .iter()
                    .any(|key| matches!(key, Key::Case { .. }))
                    && matches!(
                        result.expected,
                        Expected::Caught { case: c, .. } | Expected::HarnessCaught { case: c, .. }
                            if c == *case
                    )
            });
            if !caught {
                errors.push(format!(
                    "{:?}'s catch {}/{} has no row's Caught",
                    contract.id, case.property, case.name
                ));
            }
        }
    }
    errors
}

#[test]
fn every_referenced_test_exists() {
    let manifest = Manifest::committed();
    // Every proving catch names a committed case and a row's catch of it.
    assert_eq!(catch_errors(&manifest), Vec::<String>::new());
    assert!(manifest
        .contracts
        .iter()
        .any(|contract| !contract.proving_catches.is_empty()));
    for (case, error) in [
        (
            CaseRef {
                property: "header-sync/header-fault-penalized",
                name: "no-such-case",
            },
            "has no planted case",
        ),
        (
            CaseRef {
                property: "session/honest-never-punished",
                name: "no-such-case",
            },
            "has no row's Caught",
        ),
    ] {
        let mut named = manifest.clone();
        named.contracts[0].proving_catches = Box::leak(Box::new([case]));
        assert!(
            catch_errors(&named).iter().any(|e| e.contains(error)),
            "{case:?}: {:?}",
            catch_errors(&named)
        );
    }
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
         #[test]\n#[cfg(target_os = \"linux\")]\nfn k() {}\n\
         #[test]\n#[cfg(target_os = \"macos\")]\nfn l() {}\n\
         #[test]\n#[cfg(not(unix))]\nfn n() {}\n\
         #[cfg(test)]\nmod on {\n    #[test]\n    #[cfg(zakura_sim)]\n    fn g() {}\n}\n\
         #[tokio::test(flavor = \"multi_thread\")]\nasync fn e() {}\n\
         fn not_a_test() {}\n",
    );
    let expected: BTreeMap<String, bool> = [
        ("a", false),
        ("m::a", false),
        ("b", false),
        ("c", true),
        ("d", false),
        ("e", false),
        ("off::f", true),
        ("on::g", false),
        ("h", false),
        ("i", true),
        ("j", false),
        ("k", false),
        ("l", true),
        ("n", true),
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
    // A real `#[cfg(unix)]` runner test resolves, since only a Linux run's
    // record counts.
    assert_eq!(
        test_exists(TestRef {
            test: "planted::tests::a_hung_patched_run_is_killed_and_never_caught",
            ..xtask
        }),
        Ok(())
    );
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
    unproved[0].proving = &[];
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

/// What a transitional row may wait for. A PR that merges leaves this
/// list, so any `Open` still naming it fails `open_errors`, including a
/// gate requirement's, whose replacement is a `Permit::Test` that PR
/// switches to. A transitional row and the result that replaces it would
/// cover one key twice, which `every_required_key_is_covered_once_with_its_gates`
/// refuses.
///
/// hdr-14.8's frozen-head run is still to come, and the node cleanup keys
/// wait for its per-run cleanup record. hdr-14.4 and hdr-9d.1 merged
/// without the evidence their rows wait for: `sim/no-livelock`'s
/// header-harness catch, and the bound table's per-row fixtures. They stay here, named, until those are built or Adam
/// approves standing exclusions for them.
const OPEN_PRS: &[&str] = &["hdr-14.4", "hdr-14.8", "hdr-9d.1"];

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
    let livelock = "sim/no-livelock";
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
        patched_not_applicable,
        companions,
        ..
    } = caught
    else {
        unreachable!()
    };
    replaced.push(header_acceptance::ExpectedResult {
        keys: vec![Key::Case {
            property: livelock.into(),
            name: "spin".into(),
        }],
        gates: vec![Gate::G2],
        expected: Expected::Caught {
            case: CaseRef {
                property: livelock,
                name: "spin",
            },
            harness,
            set: &["sim/no-livelock"],
            found,
            not_applicable,
            patched_not_applicable,
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
    // Each crossing requirement is its own history's test. Another
    // history's test doesn't satisfy it, nor does its old transitional
    // Open: a conformant crossing isn't the over-window Grant's close.
    for (name, own) in CROSSINGS {
        let key = Key::Requirement {
            name: format!("gate-3/crossing/{name}"),
        };
        assert!(
            key.covers(&Expected::Passes { test: own }, NARROWING),
            "{name}"
        );
        for (other, test) in CROSSINGS {
            if other != name {
                assert!(!key.covers(&Expected::Passes { test }, NARROWING), "{name}");
            }
        }
        let transitional = Expected::Open {
            reason: "a crossed update, labelled and judged",
            owner: "Adam's side",
            approval: Approval::Pending,
            until: Some("hdr-14.6"),
        };
        assert!(!key.covers(&transitional, NARROWING), "{name}");
    }
    // Each row's bad-content requirement is that row's fault-close test,
    // since a content rejection owes the close. Another row's test doesn't
    // satisfy it, nor does its old transitional Open, nor the
    // disconnect-mid-prepare witness: a fault found after an environment
    // disconnect owes the penalty, not a close.
    for (row, own) in BAD_CONTENT_CLOSES {
        let key = Key::Requirement {
            name: format!("gate-2/bad-content-fault-close/{row}"),
        };
        assert!(
            key.covers(&Expected::Passes { test: own }, NARROWING),
            "{row}"
        );
        for (other, test) in BAD_CONTENT_CLOSES {
            if other != row {
                assert!(!key.covers(&Expected::Passes { test }, NARROWING), "{row}");
            }
        }
        assert!(
            !key.covers(&Expected::Passes { test: MID_PREPARE }, NARROWING),
            "{row}"
        );
        let transitional = Expected::Open {
            reason: "the bad-content history carries the fault-close assertion where the close \
                     is owed",
            owner: "Adam's side",
            approval: Approval::Pending,
            until: Some("hdr-14.6"),
        };
        assert!(!key.covers(&transitional, NARROWING), "{row}");
    }
    // A requirement's own Open with only its owner changed, or its own
    // test with only its name changed.
    for name in [
        "gate-2/bad-content-close-claim",
        "gate-1/topology-freshness",
        "gate-1/a-a-and-a-b-a",
        "gate-1/replay-store-control",
    ] {
        let requirement = GATE_REQUIREMENTS.iter().find(|r| r.name == name).unwrap();
        let mut changed = requirement.expected.clone();
        match &mut changed {
            Expected::Open { owner, .. } => *owner = "someone else",
            Expected::Passes { test } => test.test = "another_test",
            other => panic!("{name}: {other:?}"),
        }
        let key = Key::Requirement { name: name.into() };
        assert!(key.covers(&requirement.expected, NARROWING), "{name}");
        assert!(!key.covers(&changed, NARROWING), "{name}");
    }
}

/// The wire witness file.
const WIRE_WITNESS: &str = "tests/header_wire_witness.rs";

/// Each row's bad-content fault-close test, by row.
const BAD_CONTENT_CLOSES: [(&str, TestRef); 4] = [
    (
        "status",
        sim_test(
            "tests/header_wire_table.rs",
            "status_bad_content_closes_with_a_fault",
        ),
    ),
    (
        "subscribe-headers",
        sim_test(
            "tests/header_wire_table.rs",
            "subscribe_headers_bad_content_closes_with_a_fault",
        ),
    ),
    (
        "headers",
        sim_test(
            "tests/header_wire_table.rs",
            "headers_bad_content_closes_with_a_fault",
        ),
    ),
    (
        "headers-outcome",
        sim_test(
            "tests/header_wire_table.rs",
            "headers_outcome_bad_content_closes_with_a_fault",
        ),
    ),
];

/// Each gate-3 crossing history's test, by requirement name.
const CROSSINGS: [(&str, TestRef); 3] = [
    (
        "unretained-open-then-outcome",
        sim_test(
            "tests/header_wire_crossing.rs",
            "an_unretained_open_then_outcome_crossing_is_conformant_and_unpunished",
        ),
    ),
    (
        "crossing-close",
        sim_test(
            "tests/header_wire_crossing.rs",
            "a_crossing_close_is_conformant_and_unpunished",
        ),
    ),
    (
        "over-window-grant",
        sim_test(
            "tests/header_wire_crossing.rs",
            "an_over_window_crossing_grant_is_bad_cursor_and_closed",
        ),
    ),
];

/// The disconnect-mid-prepare witness, whose fault came after its session
/// ended, so it owes the penalty and no close.
const MID_PREPARE: TestRef = sim_test(
    "tests/header_wire_prepare_fault.rs",
    "a_disconnect_mid_prepare_is_still_penalized",
);

/// Each history a row judges for an ID other than the one it's run for, as
/// `(history, ID)`: `both-ways`, run for `conformant-never-closes`,
/// also judges each sender rule; each `over-owed-closes` stays
/// `NotApplicable` in its row's `unsolicited` history; the storage
/// witness's first plan judges `harness/direct-dispatch`; and the first
/// hold-release fixture judges `sim/no-livelock`.
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
    // `sim/no-livelock`'s COVERAGE witness runs test/probe, so its row
    // judges the first hold-release fixture, run for hold-release-wakes.
    (
        HistoryRef {
            harness: "header-sync/liveness",
            witness: sim_test(
                "tests/header_hold_release.rs",
                "the_committed_fixtures_match_their_names",
            ),
            case: "header-hold-held-locator",
        },
        "sim/no-livelock",
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

/// Every test in this file that runs is a composition check the
/// acceptance run executes at the subject commit, and every check is a
/// test here that runs ([`test_fns`]). An `#[ignore]`d tool, such as
/// `regenerate_raw_join_baseline`, isn't a check, and a listed check
/// `#[ignore]`d no longer runs, so it fails this.
#[test]
fn the_composition_checks_are_every_static_check() {
    let source =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(rows::ACCEPTANCE_TESTS))
            .unwrap();
    let tests = |source: &str| -> BTreeSet<String> {
        test_fns(source)
            .into_iter()
            .filter(|(_, gated)| !gated)
            .map(|(name, _)| name)
            .collect()
    };
    let listed: BTreeSet<String> = COMPOSITION_CHECKS.iter().map(|c| (*c).to_owned()).collect();
    assert_eq!(tests(&source), listed);
    let added = format!("{source}\n#[test]\nfn an_unlisted_check() {{}}\n");
    assert_ne!(tests(&added), listed);
    let tool = format!("{source}\n#[test]\n#[ignore = \"a tool\"]\nfn an_unlisted_tool() {{}}\n");
    assert_eq!(tests(&tool), listed);
    let gated = source.replace(
        "#[test]\nfn the_composition_checks_are_every_static_check",
        "#[test]\n#[ignore]\nfn the_composition_checks_are_every_static_check",
    );
    assert_ne!(gated, source);
    assert_ne!(tests(&gated), listed);
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

/// A text's fingerprint: the first 16 hex digits of the SHA-256 of `text`'s
/// token stream, so comments and whitespace don't count and any other edit
/// does.
fn fingerprint(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let tokens = text
        .parse::<proc_macro2::TokenStream>()
        .map_or_else(|_| text.to_owned(), |tokens| tokens.to_string());
    Sha256::digest(tokens.as_bytes())
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The fingerprint of `NARROWING`'s source in `contracts` (that file's
/// text) and every top-level `const`, `static` and `fn` there that it
/// reaches by name, transitively, in source order, by the C20
/// [`fingerprint`]. A type isn't a value item, so a new contract variant or
/// `Narrowing` field doesn't move it.
fn narrowing_print(contracts: &str) -> String {
    use syn::spanned::Spanned;
    let parsed = syn::parse_file(contracts).expect("contracts.rs parses");
    let lines: Vec<&str> = contracts.lines().collect();
    let source = |span: proc_macro2::Span| -> String {
        let (start, end) = (span.start(), span.end());
        let mut text = Vec::new();
        for line in start.line..=end.line {
            let chars: Vec<char> = lines.get(line - 1).unwrap_or(&"").chars().collect();
            let from = if line == start.line { start.column } else { 0 };
            let to = if line == end.line {
                end.column
            } else {
                chars.len()
            };
            text.push(
                chars[from.min(chars.len())..to.min(chars.len())]
                    .iter()
                    .collect::<String>(),
            );
        }
        text.join("\n")
    };
    let mut items: BTreeMap<String, (usize, String)> = BTreeMap::new();
    for item in &parsed.items {
        let name = match item {
            syn::Item::Const(item) => item.ident.to_string(),
            syn::Item::Static(item) => item.ident.to_string(),
            syn::Item::Fn(item) => item.sig.ident.to_string(),
            _ => continue,
        };
        items.insert(name, (item.span().start().line, source(item.span())));
    }
    /// Every identifier in `stream`, groups included.
    fn names(stream: proc_macro2::TokenStream, out: &mut BTreeSet<String>) {
        for token in stream {
            match token {
                proc_macro2::TokenTree::Ident(ident) => {
                    out.insert(ident.to_string());
                }
                proc_macro2::TokenTree::Group(group) => names(group.stream(), out),
                _ => {}
            }
        }
    }
    let mut reached = BTreeSet::from(["NARROWING".to_owned()]);
    let mut queue = vec!["NARROWING".to_owned()];
    while let Some(name) = queue.pop() {
        let (_, text) = &items[&name];
        let mut named = BTreeSet::new();
        names(text.parse().expect("an item's text parses"), &mut named);
        for name in named {
            if items.contains_key(&name) && reached.insert(name.clone()) {
                queue.push(name);
            }
        }
    }
    let mut texts: Vec<&(usize, String)> = reached.iter().map(|name| &items[name]).collect();
    texts.sort();
    fingerprint(
        &texts
            .iter()
            .map(|(_, text)| text.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// `NARROWING`'s releases match their pin: its source and what it reaches
/// in `contracts.rs` are as last reread. A harness added to `PEERLESS`, a
/// changed `every_execution`, or a helper it starts calling, changed
/// later, moves the fingerprint. A comment, or an unrelated item or type,
/// doesn't.
#[test]
fn the_narrowing_rules_match_their_pin() {
    let contracts = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/harness/header_acceptance/contracts.rs"),
    )
    .unwrap();
    let print = narrowing_print(&contracts);
    assert_eq!(
        print,
        contracts::NARROWING_FINGERPRINT,
        "NARROWING or what it reaches changed: reread what each entry releases, then update \
         NARROWING_FINGERPRINT to {print}"
    );
    let edit = |from: &str, to: &str| {
        assert!(contracts.contains(from), "{from}");
        narrowing_print(&contracts.replacen(from, to, 1))
    };
    assert_ne!(
        edit(
            "    \"header-sync/repair\",\n];",
            "    \"header-sync/repair\",\n    \"header-sync/wire\",\n];"
        ),
        print,
        "a harness added to PEERLESS"
    );
    assert_ne!(
        edit("!ctx.executions.is_empty()\n", "ctx.executions.len() > 1\n"),
        print,
        "every_execution changed"
    );
    let helped = |body: &str| {
        narrowing_print(
            &contracts
                .replacen(
                    "!ctx.executions.is_empty()\n",
                    "!ctx.executions.is_empty() && helper()\n",
                    1,
                )
                .replacen(
                    "/// Whether `ctx` has executions",
                    &format!(
                        "fn helper() -> bool {{ {body} }}\n\n/// Whether `ctx` has executions"
                    ),
                    1,
                ),
        )
    };
    assert_ne!(
        helped("true"),
        helped("false"),
        "a helper every_execution calls, changed"
    );
    assert_eq!(
        edit(
            "/// The node harnesses with no peers.",
            "// A comment.\n/// The node harnesses with no peers."
        ),
        print,
        "a comment"
    );
    assert_eq!(
        edit(
            "    ContractId::C22,\n];",
            "    ContractId::C22,\n    ContractId::C21,\n];"
        ),
        print,
        "an unrelated item"
    );
}

/// The variant names of `pub enum {name}` in the crate source file `file`:
/// each identifier that opens a line at the enum body's top level.
fn enum_variants(file: &str, name: &str) -> BTreeSet<String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(file);
    let text = std::fs::read_to_string(&path).expect("the enum's source file reads");
    let open = format!("pub enum {name} {{\n");
    let body = &text[text.find(&open).expect("the enum is defined there") + open.len()..];
    let body = &body[..body.find("\n}\n").expect("the enum's body closes")];
    body.lines()
        .filter_map(|line| line.strip_prefix("    "))
        .filter(|line| line.starts_with(|c: char| c.is_ascii_uppercase()))
        .map(|line| {
            line.split(|c: char| !c.is_ascii_alphanumeric())
                .next()
                .expect("a line has a first word")
                .to_owned()
        })
        .collect()
}

/// The files that name `Pending`'s variants without producing them: its
/// definition, the registry that judges marks, and the inventory itself.
const PENDING_CONSUMERS: [&str; 3] = [
    "property/verdict.rs",
    "property/registry.rs",
    "harness/header_acceptance/reasons.rs",
];

/// Every site the crate's `src` names outside test code and
/// [`PENDING_CONSUMERS`], with the files that name it, relative to `src`.
/// Naming a site is producing it, so the scan sees a mark however it's
/// spelled ([`marked_sites`]).
fn pending_producers() -> BTreeMap<String, BTreeSet<String>> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("src reads") {
            let path = entry.expect("an entry reads").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    walk(&src, &mut files);
    let mut producers: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for file in files {
        let relative = file
            .strip_prefix(&src)
            .expect("under src")
            .display()
            .to_string();
        let test_file = file.file_name().is_some_and(|name| name == "tests.rs")
            || file.components().any(|part| part.as_os_str() == "tests");
        if test_file || PENDING_CONSUMERS.contains(&relative.as_str()) {
            continue;
        }
        let text = std::fs::read_to_string(&file).expect("a source file reads");
        for site in marked_sites(&text, &relative) {
            producers.entry(site).or_default().insert(relative.clone());
        }
    }
    producers
}

/// Each `Pending` site the Rust source `text`, the file `file`, names as a
/// `Pending::Site` path outside test code, in order: a mark as `cx.pending`,
/// as `CheckCtx::pending(cx, ...)`, or through a variable alike. A `use`
/// that imports `Pending`'s variants or renames `Pending` would hide a
/// path, so it fails the scan.
fn marked_sites(text: &str, file: &str) -> Vec<String> {
    use syn::visit::Visit;
    /// Collects the site paths.
    struct Sites<'a> {
        variants: &'a BTreeSet<String>,
        file: &'a str,
        found: Vec<String>,
    }
    /// Whether `attrs` mark test-only code: `#[test]`, or a `cfg` whose
    /// whole condition is `test`. Any other condition that names `test`,
    /// such as `any(test, zakura_sim)` or `not(test)`, can hold in a
    /// simulator build, so its code is scanned.
    fn test_only(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attr| {
            attr.path().is_ident("test")
                || (attr.path().is_ident("cfg")
                    && attr
                        .meta
                        .require_list()
                        .is_ok_and(|list| list.tokens.to_string() == "test"))
        })
    }
    /// Fails on a `use` tree that imports from `Pending` or renames it.
    fn check_use(tree: &syn::UseTree, file: &str) {
        match tree {
            syn::UseTree::Path(path) if path.ident == "Pending" => {
                panic!("{file}: a `use` imports Pending's variants, which the scan can't see")
            }
            syn::UseTree::Path(path) => check_use(&path.tree, file),
            syn::UseTree::Rename(rename) if rename.ident == "Pending" => {
                panic!("{file}: a `use` renames Pending, which the scan can't see")
            }
            syn::UseTree::Group(group) => group.items.iter().for_each(|tree| check_use(tree, file)),
            _ => {}
        }
    }
    impl<'ast> Visit<'ast> for Sites<'_> {
        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !test_only(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }

        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if !test_only(&item.attrs) {
                syn::visit::visit_item_fn(self, item);
            }
        }

        fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
            check_use(&item.tree, self.file);
        }

        fn visit_path(&mut self, path: &'ast syn::Path) {
            let names: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
            if let [.., kind, site] = &names[..] {
                if kind == "Pending" && self.variants.contains(site) {
                    self.found.push(site.clone());
                }
            }
            syn::visit::visit_path(self, path);
        }
    }
    let variants = enum_variants("src/property/verdict.rs", "Pending");
    let parsed = syn::parse_file(text).unwrap_or_else(|error| panic!("{file} parses: {error}"));
    let mut sites = Sites {
        variants: &variants,
        file,
        found: Vec::new(),
    };
    sites.visit_file(&parsed);
    sites.found
}

/// C20: the reason inventory lists every `NotApplicable` reason exactly
/// once, in `NA_REASONS` for the ones a header ID can carry or `NOT_HEADER`
/// for the rest, and each in the trace's typed form. The complete set comes
/// from the enums' definitions, and a pending site's side from the checks
/// that mark it: a site a header source marks is a header reason, unless
/// the dry end turns it into missing evidence, and each header site has a
/// header producer. The match is exhaustive too, so a new reason or
/// `Pending` site fails to compile here until it's classified.
#[test]
fn the_not_applicable_inventory_lists_every_reason() {
    use zakura_sim::{property::Pending, NaReason};
    let header = |reason: NaReason| match reason {
        NaReason::NeverApplied
        | NaReason::Undeclared
        | NaReason::Excluded
        | NaReason::AssumptionsNeverHeld
        | NaReason::AssumptionLost
        | NaReason::StoppedByViolation
        | NaReason::HarnessFailed
        | NaReason::NodeStopped
        | NaReason::Unsettled {
            site:
                Pending::FaultOpen
                | Pending::TeardownOpen
                | Pending::WatchOpen
                | Pending::UnreadTrigger
                | Pending::UnjudgedDeparture
                | Pending::TipUnpublished,
        } => true,
        // A dry end turns an unread boundary into missing evidence.
        NaReason::Unsettled {
            site: Pending::UnreadBoundary | Pending::BlockTeardownOpen,
        } => false,
    };
    let listed: Vec<NaReason> = NA_REASONS.iter().map(|entry| entry.reason).collect();
    let excluded: Vec<NaReason> = NOT_HEADER.iter().map(|(reason, _)| *reason).collect();
    let all: BTreeSet<NaReason> = listed.iter().chain(&excluded).copied().collect();
    assert_eq!(
        all.len(),
        listed.len() + excluded.len(),
        "a reason is listed twice"
    );
    // Every variant, as the enums define them.
    let site_of = |reason: &NaReason| match reason {
        NaReason::Unsettled { site } => Some(format!("{site:?}")),
        _ => None,
    };
    let variant_of = |reason: &NaReason| match site_of(reason) {
        Some(_) => "Unsettled".to_owned(),
        None => format!("{reason:?}"),
    };
    assert_eq!(
        all.iter().map(variant_of).collect::<BTreeSet<_>>(),
        enum_variants("src/check.rs", "NaReason"),
        "every reason"
    );
    assert_eq!(
        all.iter().filter_map(site_of).collect::<BTreeSet<_>>(),
        enum_variants("src/property/verdict.rs", "Pending"),
        "every pending site"
    );
    // The one site a header check marks that no header ID ends with: the
    // registry's dry end fails the run at it instead, as
    // `tests/drain.rs`'s
    // `a_dry_end_fails_an_unread_boundary_as_missing_evidence` shows.
    let converted = [Pending::UnreadBoundary].map(|site| format!("{site:?}"));
    let header_sites: BTreeSet<String> = listed.iter().filter_map(site_of).collect();
    // The scan reads a site however the mark is spelled: whatever its
    // receiver, through `CheckCtx::pending`, or through a variable (Terra 3,
    // rounds 3 and 7). Test-only code doesn't count, but code a simulator
    // build compiles does (Terra, round 8), and a `use` that would hide a
    // site's path fails it.
    let text = "fn check(cx: &mut Cx) {\n\
                    cx.pending(Pending::FaultOpen);\n\
                    ctx.pending(Pending::BlockTeardownOpen);\n\
                    CheckCtx::pending(cx, Pending::TeardownOpen);\n\
                    let site = Pending::WatchOpen;\n\
                    cx.pending(site);\n\
                    let ops = self.port.pending();\n\
                }\n\
                #[cfg(test)]\n\
                mod tests {\n\
                    fn t() { let _ = Pending::UnreadTrigger; }\n\
                }\n\
                #[cfg(any(test, zakura_sim))]\n\
                fn sim() { cx.pending(Pending::UnjudgedDeparture); }\n\
                #[cfg(not(test))]\n\
                fn live() { cx.pending(Pending::UnreadBoundary); }\n";
    assert_eq!(
        marked_sites(text, "t.rs"),
        [
            "FaultOpen",
            "BlockTeardownOpen",
            "TeardownOpen",
            "WatchOpen",
            "UnjudgedDeparture",
            "UnreadBoundary"
        ]
    );
    for hidden in [
        "use crate::property::Pending::FaultOpen;",
        "use crate::property::Pending as Mark;",
    ] {
        let scanned = std::panic::catch_unwind(|| marked_sites(hidden, "t.rs"));
        assert!(scanned.is_err(), "{hidden}");
    }
    let producers = pending_producers();
    let by_header = |site: &String| {
        producers
            .get(site)
            .is_some_and(|files| files.iter().any(|file| file.contains("header")))
    };
    for site in producers.keys().filter(|site| by_header(site)) {
        assert!(
            header_sites.contains(site) || converted.contains(site),
            "a header check marks {site}, which NA_REASONS doesn't list"
        );
    }
    for site in &header_sites {
        assert!(by_header(site), "no header check marks {site}");
    }
    for site in &converted {
        assert!(
            NOT_HEADER
                .iter()
                .any(|(reason, _)| site_of(reason).as_ref() == Some(site)),
            "{site}"
        );
    }
    for entry in NA_REASONS {
        assert!(
            header(entry.reason),
            "{:?} isn't a header reason",
            entry.reason
        );
        assert!(!entry.meaning.is_empty(), "{:?}", entry.reason);
        let json = serde_json::to_value(entry.reason).unwrap();
        assert_eq!(
            serde_json::from_value::<NaReason>(json).ok(),
            Some(entry.reason)
        );
    }
    for (reason, why) in NOT_HEADER {
        assert!(!header(*reason), "{reason:?} is a header reason");
        assert!(!why.is_empty(), "{reason:?}");
    }
}

// The evaluator's tests run on a small manifest built here: one node
// harness `t/node` registering `t/a` and `t/b`, a peerless node harness
// `t/quiet` registering `t/q`, which the test narrowing entry releases from
// C11, and a machine harness `t/machine`.

/// The node harness.
const NODE: &str = "t/node";
/// The peerless node harness.
const QUIET: &str = "t/quiet";
/// The machine harness.
const MACHINE: &str = "t/machine";
/// The subject commit.
const S: &str = "subject";
/// The subject's source digest.
const SOURCE: &str = "source-of-subject";
/// The subject as the caller established it.
const SUBJECT: header_acceptance::Subject<'static> = header_acceptance::Subject {
    commit: S,
    source: SOURCE,
};

/// A node-harness history of test `test`.
const fn history(harness: &'static str, test: &'static str) -> HistoryRef {
    HistoryRef {
        harness,
        witness: sim_test("tests/t.rs", test),
        case: "only",
    }
}

/// `t/a`'s history, where `t/b` stays `NotApplicable` with [`UNREACHED`].
const HISTORY: HistoryRef = history(NODE, "t_holds");
/// `t/q`'s history.
const QUIET_HISTORY: HistoryRef = history(QUIET, "t_quiet_holds");
/// `t/b`'s history, where it stays `NotApplicable`.
const STAYS_HISTORY: HistoryRef = history(NODE, "t_stays");
/// The native mutation's history: `raw-budget-lease`'s, as its
/// classification names it.
const NATIVE_HISTORY: HistoryRef = HistoryRef {
    harness: NATIVE_HARNESS,
    witness: sim_test(
        "tests/header_node_capacity.rs",
        "the_probe_matches_the_accounting_through_close_relink_and_shutdown",
    ),
    case: "plan 0 of inputs(16, 256)",
};
/// The harness the native mutation's history runs on.
const NATIVE_HARNESS: &str = "header-sync/storage";
/// Why `t/b` doesn't apply on [`HISTORY`]: a benign reason, since a
/// completed counted run accepts no other for a non-target property.
const UNREACHED: &str = "never-applied";
/// Why `t/b` never applies.
const NEVER: &str = "never applies";
/// `t/b`'s expected `NotApplicable` on [`HISTORY`].
const HOLDS_NA: &[header_acceptance::ExpectedNa] = &[header_acceptance::ExpectedNa {
    property: "t/b",
    reason: UNREACHED,
}];
/// A node planted case found at a step.
const CASE: CaseRef = CaseRef {
    property: "t/a",
    name: "bug",
};
/// A node planted case found at the end.
const END_CASE: CaseRef = CaseRef {
    property: "t/a",
    name: "late-bug",
};
/// A machine planted case.
const MACHINE_CASE: CaseRef = CaseRef {
    property: MACHINE,
    name: "machine-bug",
};
/// The native mutation, a real integrity one, since `Key::Native` binds to
/// its classification.
const NATIVE: &str = "raw-budget-lease";
/// Its diagnostic.
const DIAGNOSTIC: &str = "OwnedMismatch";
/// The proving tests and a plain test.
const T3: TestRef = sim_test("tests/t.rs", "c3_holds");
const T5: TestRef = sim_test("tests/t.rs", "c5_holds");
const T11: TestRef = sim_test("tests/t.rs", "c11_holds");
const TN: TestRef = sim_test("tests/t.rs", "n1_holds");
const TP: TestRef = sim_test("tests/t.rs", "a_test");
/// A standing gate obligation the real `GATE_OBLIGATIONS` permits as an
/// approved standing `Open`.
const STANDING: &str = "gate-3/no-request-row";

/// A satisfied contract with its proving tests.
fn satisfied(id: ContractId, test: &'static [TestRef]) -> Contract {
    Contract {
        id,
        owner: "test",
        statement: "test",
        proving: test,
        proving_catches: &[],
        state: ContractState::Satisfied,
        delivery: "test",
    }
}

/// The test narrowing entry: `t/quiet` results don't read C11.
fn quiet_entry(test: NarrowingTest) -> Narrowing {
    Narrowing {
        entry: "N1",
        contracts: &[ContractId::C11],
        releases: |ctx| ctx.harness.as_deref() == Some(QUIET),
        reason: "t/quiet has no peers",
        test,
    }
}

/// An expected result.
fn er(keys: Vec<Key>, gates: &[Gate], expected: Expected) -> ExpectedResult {
    ExpectedResult {
        keys,
        gates: gates.to_vec(),
        expected,
    }
}

/// A test's witness key.
fn witness_key(test: TestRef) -> Key {
    Key::Witness {
        package: test.package.into(),
        file: test.file.into(),
        test: test.test.into(),
    }
}

/// The node history's `Holds` identity.
fn holds_id() -> ExpectedId {
    ExpectedId::Holds {
        property: "t/a".into(),
        run: HISTORY.into(),
    }
}

/// The `StaysNotApplicable` identity.
fn stays_id() -> ExpectedId {
    ExpectedId::StaysNotApplicable {
        property: "t/b".into(),
        run: STAYS_HISTORY.into(),
    }
}

/// The test manifest.
fn manifest() -> Manifest {
    let contract_tests = [
        (ContractId::C3, T3),
        (ContractId::C5, T5),
        (ContractId::C11, T11),
    ]
    .map(|(id, test)| {
        er(
            vec![header_acceptance::contracts::contract_test_key(id, test)],
            &[],
            Expected::Passes { test },
        )
    });
    let mut expected = vec![
        er(
            vec![Key::passing("t/a")],
            &[Gate::G2],
            Expected::Holds {
                property: "t/a",
                history: HISTORY,
                not_applicable: HOLDS_NA,
            },
        ),
        er(
            vec![Key::passing("t/q")],
            &[Gate::G5],
            Expected::Holds {
                property: "t/q",
                history: QUIET_HISTORY,
                not_applicable: &[],
            },
        ),
        er(
            vec![Key::passing("t/b")],
            &[Gate::G2],
            Expected::StaysNotApplicable {
                property: "t/b",
                history: STAYS_HISTORY,
                na_reason: zakura_sim::NaReason::NeverApplied,
                reason: NEVER,
                approval: Approval::Adam { record: "test" },
                not_applicable: &[],
            },
        ),
        er(
            vec![
                Key::catch("t/a"),
                Key::Case {
                    property: "t/a".into(),
                    name: "bug".into(),
                },
            ],
            &[Gate::G2],
            Expected::Caught {
                case: CASE,
                harness: NODE,
                set: &["t/a"],
                found: Found::AtStep,
                companions: &["close"],
                not_applicable: &[],
                patched_not_applicable: None,
            },
        ),
        er(
            vec![Key::Case {
                property: "t/a".into(),
                name: "late-bug".into(),
            }],
            &[Gate::G2],
            Expected::Caught {
                case: END_CASE,
                harness: NODE,
                set: &["t/a"],
                found: Found::AtEnd,
                companions: &[],
                not_applicable: &[],
                patched_not_applicable: None,
            },
        ),
        er(
            vec![Key::catch(MACHINE)],
            &[Gate::G5],
            Expected::MachineCaught {
                case: MACHINE_CASE,
                harness: MACHINE,
            },
        ),
        er(
            vec![Key::Replay {
                run: HISTORY.into(),
            }],
            &[Gate::G1],
            Expected::Replays {
                run: HISTORY.into(),
            },
        ),
        er(
            vec![Key::Native {
                patch: NATIVE.into(),
            }],
            &[Gate::G6],
            Expected::IntegrityCaught {
                patch: NATIVE,
                history: NATIVE_HISTORY,
                diagnostic: DIAGNOSTIC,
                not_applicable: native_na(),
            },
        ),
        er(
            vec![Key::Requirement {
                name: STANDING.into(),
            }],
            &[Gate::G3],
            GATE_REQUIREMENTS
                .iter()
                .find(|r| r.name == STANDING)
                .unwrap()
                .expected
                .clone(),
        ),
        er(vec![witness_key(TP)], &[], Expected::Passes { test: TP }),
        er(
            vec![Key::NarrowingTest { entry: "N1".into() }],
            &[],
            Expected::Passes { test: TN },
        ),
    ];
    expected.extend(contract_tests);
    Manifest {
        expected,
        contracts: vec![
            satisfied(ContractId::C3, &[T3]),
            satisfied(ContractId::C5, &[T5]),
            satisfied(ContractId::C11, &[T11]),
        ],
        required_contracts: vec![ContractId::C3, ContractId::C5, ContractId::C11],
        narrowing: vec![quiet_entry(NarrowingTest::Test(TN))],
        machines: vec![MACHINE],
        matrix: vec![
            (NODE, "t/a", Status::Active),
            (NODE, "t/b", Status::Active),
            (QUIET, "t/q", Status::Active),
            (MACHINE, MACHINE, Status::Active),
            (NATIVE_HARNESS, "t/s", Status::Active),
        ]
        .into_iter()
        .chain(
            native_na()
                .iter()
                .map(|na| (NATIVE_HARNESS, na.property, Status::Active)),
        )
        .collect(),
    }
}

/// The `NotApplicable` reports the native mutation's committed
/// classification expects on its clean runs.
fn native_na() -> &'static [header_acceptance::ExpectedNa] {
    NATIVE_MUTATIONS
        .iter()
        .find_map(|mutation| match mutation.class {
            NativeClass::Integrity { not_applicable, .. } if mutation.patch == NATIVE => {
                Some(not_applicable)
            }
            _ => None,
        })
        .expect("the native mutation is an integrity one")
}

/// The required keys: exactly what the test manifest covers, with its
/// gates.
fn required(manifest: &Manifest) -> BTreeMap<Key, BTreeSet<Gate>> {
    manifest
        .expected
        .iter()
        .flat_map(|result| {
            result
                .keys
                .iter()
                .map(|key| (key.clone(), result.gates.iter().copied().collect()))
        })
        .collect()
}

/// Who runs `run` by its role: a planted case's baseline and patched
/// runs are the planted runner's, every other execution the tool's.
fn runner_of(run: &RunId) -> RunBy {
    match run {
        RunId::Case {
            run: CaseRun::Baseline | CaseRun::Patched,
            property,
            ..
        } if property != header_acceptance::NATIVE_DIR => RunBy::Planted,
        _ => RunBy::Tool,
    }
}

/// The trace digest the fixtures give `run`.
fn trace_of(run: &RunId) -> String {
    format!("trace of {run:?}")
}

/// A completed execution of `run` on `harness`, reporting `reports`.
fn execution(run: RunId, harness: &str, reports: &[(&str, Report)]) -> Execution {
    Execution {
        trace: trace_of(&run),
        meta: ExecMeta {
            harness: harness.into(),
            draft_override: Vec::new(),
            run_by: runner_of(&run),
            input: None,
        },
        run,
        end: End::Completed,
        reports: reports
            .iter()
            .map(|(id, report)| ((*id).to_owned(), report.clone()))
            .collect(),
        trigger: true,
        leaked: 0,
        actions: Actions::Complete,
    }
}

/// `Held` with applicability.
fn held() -> Report {
    Report::Held { applicable: 3 }
}

/// `NotApplicable` for `reason`.
fn na(reason: &str) -> Report {
    Report::NotApplicable {
        reason: reason.into(),
    }
}

/// A test result for `test`, which ran `ran` with `outcome`, from a binary
/// embedding the subject's source, in a checkout of it.
fn test_actual(ran: TestRef, outcome: TestOutcome) -> Actual {
    Actual::Test {
        ran: vec![TestRan {
            package: ran.package.into(),
            file: ran.file.into(),
            test: ran.test.into(),
            outcome,
        }],
        binary_source: Some(SOURCE.into()),
        checkout_source: Some(SOURCE.into()),
    }
}

/// A passing test's record.
fn test_record(test: TestRef) -> ResultRecord {
    ResultRecord {
        expected: Expected::Passes { test }.id().unwrap(),
        commit: S.into(),
        actual: test_actual(test, TestOutcome::Passed),
    }
}

/// A node case's genuine three runs, found `found`.
fn caught_actual(case: CaseRef, found: Found) -> Actual {
    let mut patched = execution(
        RunId::case(case, CaseRun::Patched),
        NODE,
        &[("t/a", Report::Violated), ("t/b", held())],
    );
    if found == Found::AtStep {
        patched.end = End::Violation {
            property: "t/a".into(),
        };
        patched.actions = Actions::Prefix;
    }
    let mut draft = execution(
        RunId::case(case, CaseRun::Draft),
        NODE,
        &[("t/a", Report::Violated), ("t/b", held())],
    );
    draft.meta.draft_override = vec!["t/a".into()];
    Actual::Caught {
        baseline: execution(
            RunId::case(case, CaseRun::Baseline),
            NODE,
            &[("t/a", held()), ("t/b", held())],
        ),
        patched,
        companions: if case == CASE {
            vec![Companion {
                name: "close".into(),
                trace: draft.trace.clone(),
                run: draft.run.clone(),
                held: true,
            }]
        } else {
            Vec::new()
        },
        draft,
    }
}

/// A genuine result file for the test manifest.
fn results() -> ResultFile {
    let record = |expected: ExpectedId, actual: Actual| ResultRecord {
        expected,
        commit: S.into(),
        actual,
    };
    let native = |run| {
        let mut reports = vec![("t/s", held())];
        reports.extend(
            native_na()
                .iter()
                .map(|listed| (listed.property, na(listed.reason))),
        );
        let mut run = execution(RunId::native(NATIVE, run), NATIVE_HARNESS, &reports);
        run.meta.input = Some(NATIVE_HISTORY.into());
        run
    };
    let mut patched_native = native(CaseRun::Patched);
    patched_native.end = End::HarnessFailure {
        diagnostic: DIAGNOSTIC.into(),
    };
    ResultFile {
        subject: S.into(),
        subject_source: SOURCE.into(),
        results: vec![
            record(
                holds_id(),
                Actual::Run(execution(
                    HISTORY.into(),
                    NODE,
                    &[("t/a", held()), ("t/b", na(UNREACHED))],
                )),
            ),
            record(
                ExpectedId::Holds {
                    property: "t/q".into(),
                    run: QUIET_HISTORY.into(),
                },
                Actual::Run(execution(QUIET_HISTORY.into(), QUIET, &[("t/q", held())])),
            ),
            record(
                stays_id(),
                Actual::Run(execution(
                    STAYS_HISTORY.into(),
                    NODE,
                    &[("t/a", held()), ("t/b", na(UNREACHED))],
                )),
            ),
            record(
                ExpectedId::Caught(RunId::case(CASE, CaseRun::Patched)),
                caught_actual(CASE, Found::AtStep),
            ),
            record(
                ExpectedId::Caught(RunId::case(END_CASE, CaseRun::Patched)),
                caught_actual(END_CASE, Found::AtEnd),
            ),
            record(
                ExpectedId::MachineCaught(RunId::case(MACHINE_CASE, CaseRun::Patched)),
                Actual::MachineCaught {
                    baseline: execution(
                        RunId::case(MACHINE_CASE, CaseRun::Baseline),
                        MACHINE,
                        &[(MACHINE, held())],
                    ),
                    patched: Execution {
                        end: End::Violation {
                            property: MACHINE.into(),
                        },
                        ..execution(
                            RunId::case(MACHINE_CASE, CaseRun::Patched),
                            MACHINE,
                            &[(MACHINE, Report::Violated)],
                        )
                    },
                },
            ),
            record(
                ExpectedId::Replays(HISTORY.into()),
                Actual::Replay {
                    run: HISTORY.into(),
                    of: trace_of(&HISTORY.into()),
                    meta: ExecMeta {
                        harness: NODE.into(),
                        draft_override: Vec::new(),
                        run_by: RunBy::Tool,
                        input: None,
                    },
                    exact: true,
                    reports_equal: true,
                    stores_equal: true,
                    fresh: true,
                },
            ),
            record(
                ExpectedId::IntegrityCaught(RunId::native(NATIVE, CaseRun::Patched)),
                Actual::IntegrityCaught {
                    unpatched: native(CaseRun::Unpatched),
                    patched: patched_native,
                    restored: native(CaseRun::Restored),
                },
            ),
            test_record(TP),
            test_record(TN),
            test_record(T3),
            test_record(T5),
            test_record(T11),
        ],
    }
}

/// A native mutation retired as a duplicate, a real one, since
/// `Key::Native` binds to its classification.
const DUPLICATE: &str = "short-payload";
/// The planted case its classification names as covering it.
const DUPLICATE_CASE: CaseRef = CaseRef {
    property: "header-sync/commit-only-authorized",
    name: "dropped-payload-header",
};
/// The node harness the covering case runs on here.
const DUPLICATE_NODE: &str = "t/commit-node";

/// The duplicate's native key.
fn duplicate_key() -> Key {
    Key::Native {
        patch: DUPLICATE.into(),
    }
}

/// The covering case's catch.
fn duplicate_caught() -> Expected {
    Expected::Caught {
        case: DUPLICATE_CASE,
        harness: DUPLICATE_NODE,
        set: &[DUPLICATE_CASE.property],
        found: Found::AtStep,
        companions: &[],
        not_applicable: &[],
        patched_not_applicable: None,
    }
}

/// The test manifest and its genuine results with the duplicate added: the
/// covering case's catch on its case key, the duplicate's native key on
/// gate 6 sharing that catch, and the catch's three runs.
fn with_duplicate() -> (Manifest, ResultFile) {
    let mut manifest = manifest();
    manifest.expected.push(er(
        vec![Key::Case {
            property: DUPLICATE_CASE.property.into(),
            name: DUPLICATE_CASE.name.into(),
        }],
        &[Gate::G2],
        duplicate_caught(),
    ));
    manifest
        .expected
        .push(er(vec![duplicate_key()], &[Gate::G6], duplicate_caught()));
    manifest
        .matrix
        .push((DUPLICATE_NODE, DUPLICATE_CASE.property, Status::Active));
    let property = DUPLICATE_CASE.property;
    let run = |which, report| {
        execution(
            RunId::case(DUPLICATE_CASE, which),
            DUPLICATE_NODE,
            &[(property, report)],
        )
    };
    let mut patched = run(CaseRun::Patched, Report::Violated);
    patched.end = End::Violation {
        property: property.into(),
    };
    patched.actions = Actions::Prefix;
    let mut draft = run(CaseRun::Draft, Report::Violated);
    draft.meta.draft_override = vec![property.into()];
    let mut results = results();
    results.results.push(ResultRecord {
        expected: duplicate_caught().id().unwrap(),
        commit: S.into(),
        actual: Actual::Caught {
            baseline: run(CaseRun::Baseline, held()),
            patched,
            draft,
            companions: Vec::new(),
        },
    });
    (manifest, results)
}

/// The state [`with_duplicate`]'s evaluation gives the duplicate's key
/// once `defect` changes its manifest or results, against the keys the
/// unchanged manifest requires.
fn duplicate_state(defect: impl FnOnce(&mut Manifest, &mut ResultFile)) -> ObligationState {
    let (mut manifest, mut results) = with_duplicate();
    let required = required(&manifest);
    defect(&mut manifest, &mut results);
    header_acceptance::evaluate(&manifest, &required, &results, SUBJECT).keys[&duplicate_key()]
        .clone()
}

/// The state the test manifest's evaluation gives `key`.
fn state(manifest: &Manifest, results: &ResultFile, key: &Key) -> ObligationState {
    header_acceptance::evaluate(manifest, &required(manifest), results, SUBJECT)
        .keys
        .get(key)
        .cloned()
        .unwrap_or_else(|| panic!("{key:?} isn't a required key"))
}

/// Whether `state` is blocked.
fn blocked(state: &ObligationState) -> bool {
    matches!(state, ObligationState::Blocked { .. })
}

/// The record in `results` named `id`.
fn record_mut<'r>(results: &'r mut ResultFile, id: &ExpectedId) -> &'r mut ResultRecord {
    results
        .results
        .iter_mut()
        .find(|record| &record.expected == id)
        .unwrap()
}

/// The catch's three runs and companions in `results`.
type CaughtRuns<'r> = (
    &'r mut Execution,
    &'r mut Execution,
    &'r mut Execution,
    &'r mut Vec<Companion>,
);

/// The at-step catch's runs in `results`.
fn caught_runs(results: &mut ResultFile, case: CaseRef) -> CaughtRuns<'_> {
    match &mut record_mut(
        results,
        &ExpectedId::Caught(RunId::case(case, CaseRun::Patched)),
    )
    .actual
    {
        Actual::Caught {
            baseline,
            patched,
            draft,
            companions,
        } => (baseline, patched, draft, companions),
        _ => unreachable!(),
    }
}

/// The run of history result `id` in `results`.
fn run_of<'r>(results: &'r mut ResultFile, id: &ExpectedId) -> &'r mut Execution {
    match &mut record_mut(results, id).actual {
        Actual::Run(run) => run,
        _ => unreachable!(),
    }
}

/// The machine catch's runs in `results`.
fn machine_runs(results: &mut ResultFile) -> (&mut Execution, &mut Execution) {
    match &mut record_mut(
        results,
        &ExpectedId::MachineCaught(RunId::case(MACHINE_CASE, CaseRun::Patched)),
    )
    .actual
    {
        Actual::MachineCaught { baseline, patched } => (baseline, patched),
        _ => unreachable!(),
    }
}

/// The integrity mutation's result in `results`.
fn native_actual(results: &mut ResultFile) -> &mut Actual {
    &mut record_mut(
        results,
        &ExpectedId::IntegrityCaught(RunId::native(NATIVE, CaseRun::Patched)),
    )
    .actual
}

/// The replay's result in `results`.
fn replay_actual(results: &mut ResultFile) -> &mut Actual {
    &mut record_mut(results, &ExpectedId::Replays(HISTORY.into())).actual
}

/// Every genuine result verifies its keys. The approved `StaysNotApplicable`
/// among them verifies against the report a real run gives it, its target
/// `NotApplicable(never-applied)`, the typed reason its row names, while
/// the row's approved prose says why; another reason blocks it.
#[test]
fn the_evaluator_accepts_genuine_results() {
    let manifest = manifest();
    assert_ne!(NEVER, zakura_sim::NaReason::NeverApplied.name());
    let mut undeclared = results();
    run_of(&mut undeclared, &stays_id())
        .reports
        .insert("t/b".into(), na("undeclared"));
    assert!(blocked(&state(
        &manifest,
        &undeclared,
        &Key::passing("t/b")
    )));
    let evaluation =
        header_acceptance::evaluate(&manifest, &required(&manifest), &results(), SUBJECT);
    assert!(evaluation.stray.is_empty(), "{:?}", evaluation.stray);
    for (key, state) in &evaluation.keys {
        let expected = if matches!(key, Key::Requirement { .. }) {
            matches!(state, ObligationState::Excluded { .. })
        } else {
            *state == ObligationState::Verified
        };
        assert!(expected, "{key:?}: {state:?}");
    }
    assert!(evaluation.accepted());
    assert!(
        !evaluation.fully_accepted(),
        "the standing exclusion is excluded"
    );
    assert!(evaluation.gates.values().all(|passes| *passes));

    // A duplicate native mutation's key shares its covering case's catch,
    // and that catch verifies it.
    let (with, results_with) = with_duplicate();
    let evaluation = header_acceptance::evaluate(&with, &required(&with), &results_with, SUBJECT);
    assert!(evaluation.stray.is_empty(), "{:?}", evaluation.stray);
    assert_eq!(evaluation.keys[&duplicate_key()], ObligationState::Verified);
    assert!(evaluation.gates[&Gate::G6]);

    // A bad-content close requirement verifies on its row's fault-close
    // test, beside the disconnect-mid-prepare witness, which owes the
    // penalty and no close and verifies on its own key. Swapping the
    // requirement onto that witness's pass blocks it.
    let (row, close) = BAD_CONTENT_CLOSES[2];
    let requirement = Key::Requirement {
        name: format!("gate-2/bad-content-fault-close/{row}"),
    };
    let mut owed = manifest.clone();
    owed.expected.push(er(
        vec![requirement.clone()],
        &[Gate::G2],
        Expected::Passes { test: close },
    ));
    owed.expected.push(er(
        vec![witness_key(MID_PREPARE)],
        &[Gate::G2],
        Expected::Passes { test: MID_PREPARE },
    ));
    let mut passed = results();
    passed.results.push(test_record(close));
    passed.results.push(test_record(MID_PREPARE));
    for key in [&requirement, &witness_key(MID_PREPARE)] {
        assert_eq!(
            state(&owed, &passed, key),
            ObligationState::Verified,
            "{key:?}"
        );
    }
    let mut swapped = owed.clone();
    let at = covering(&swapped, &requirement);
    swapped.expected[at].expected = Expected::Passes { test: MID_PREPARE };
    assert!(blocked(&state(&swapped, &passed, &requirement)));

    // A catch stopped at a step is a prefix: another property may still be
    // open there.
    let mut pending = results();
    caught_runs(&mut pending, CASE)
        .1
        .reports
        .insert("t/b".into(), na("stopped-by-violation"));
    assert_eq!(
        state(&manifest, &pending, &Key::catch("t/a")),
        ObligationState::Verified
    );

    // A set judged on the Draft run: the patched run stops at the case's
    // own ID alone, and the Draft run, with the whole set as its override,
    // violates every member by its end, which the baseline doesn't (the
    // Draft-set rule, `evaluate::caught_draft`). The patched run with the
    // broader ID violated too, or stopped by it, a Draft run that doesn't
    // violate it, and a baseline that violates a member each block the
    // case. That last one is the reviewers' unrelated bug: a member broken
    // in both runs isn't the patch's doing.
    let late = Key::Case {
        property: "t/a".into(),
        name: "late-bug".into(),
    };
    let mut later = manifest.clone();
    let at = covering(&later, &late);
    if let Expected::Caught { set, found, .. } = &mut later.expected[at].expected {
        *set = &["t/a", "t/b"];
        *found = Found::AtStepSetOnDraft;
    }
    let judged_with = |stopped_by: &str, baseline_b: Report, patched_b: Report, draft_b: Report| {
        let mut file = results();
        let (baseline, patched, draft, _) = caught_runs(&mut file, END_CASE);
        baseline.reports.insert("t/b".into(), baseline_b);
        patched.end = End::Violation {
            property: stopped_by.into(),
        };
        patched.actions = Actions::Prefix;
        patched.reports.insert("t/b".into(), patched_b);
        draft.reports.insert("t/b".into(), draft_b);
        draft.meta.draft_override = vec!["t/a".into(), "t/b".into()];
        state(&later, &file, &late)
    };
    let judged = |stopped_by: &str, patched_b: Report, draft_b: Report| {
        judged_with(stopped_by, held(), patched_b, draft_b)
    };
    assert_eq!(
        judged("t/a", held(), Report::Violated),
        ObligationState::Verified
    );
    assert!(blocked(&judged("t/a", Report::Violated, Report::Violated)));
    assert!(blocked(&judged("t/b", held(), Report::Violated)));
    assert!(blocked(&judged("t/a", held(), held())));
    assert!(blocked(&judged_with(
        "t/a",
        Report::Violated,
        held(),
        Report::Violated
    )));
    // The same Draft run with no trigger, as a failed witness leaves it,
    // blocks however its reports read.
    let mut file = results();
    let (baseline, patched, draft, _) = caught_runs(&mut file, END_CASE);
    baseline.reports.insert("t/b".into(), held());
    patched.end = End::Violation {
        property: "t/a".into(),
    };
    patched.actions = Actions::Prefix;
    patched.reports.insert("t/b".into(), held());
    draft.reports.insert("t/b".into(), Report::Violated);
    draft.meta.draft_override = vec!["t/a".into(), "t/b".into()];
    draft.trigger = false;
    assert!(blocked(&state(&later, &file, &late)));
}

/// An unpatched storage history and a release-wake history that pass, on
/// their real harnesses' registered inventories, verify (Rule 16's
/// positive tests).
#[test]
fn the_evaluator_accepts_real_storage_and_release_wake_histories() {
    let committed = Manifest::committed();
    let mut m = Manifest {
        expected: Vec::new(),
        contracts: Vec::new(),
        required_contracts: Vec::new(),
        narrowing: Vec::new(),
        machines: Vec::new(),
        matrix: Vec::new(),
    };
    let mut r = ResultFile {
        subject: S.into(),
        subject_source: SOURCE.into(),
        results: Vec::new(),
    };
    let mut keys = Vec::new();
    for (harness, test) in [
        (
            "header-sync/storage",
            "the_close_relink_input_holds_chunk_budget_bound",
        ),
        (
            "header-sync/release-wake",
            "the_release_wake_input_holds_capacity_release_wakes",
        ),
    ] {
        let (history, target) = rows::history_targets()
            .into_iter()
            .find(|(h, _)| h.harness == harness && h.witness.test == test)
            .unwrap();
        let ids: Vec<(&str, Report)> = committed
            .matrix
            .iter()
            .filter(|(h, _, _)| *h == harness)
            .map(|&(_, id, _)| (id, held()))
            .collect();
        m.matrix
            .extend(committed.matrix.iter().filter(|(h, _, _)| *h == harness));
        let key = Key::History {
            run: history.into(),
            target: target.into(),
        };
        m.expected.push(er(
            vec![key.clone()],
            &[Gate::G1],
            Expected::Holds {
                property: target,
                history,
                not_applicable: &[],
            },
        ));
        r.results.push(ResultRecord {
            expected: ExpectedId::Holds {
                property: target.into(),
                run: history.into(),
            },
            commit: S.into(),
            actual: Actual::Run(execution(history.into(), harness, &ids)),
        });
        keys.push(key);
    }
    let evaluation = header_acceptance::evaluate(&m, &required(&m), &r, SUBJECT);
    assert!(evaluation.stray.is_empty(), "{:?}", evaluation.stray);
    for key in &keys {
        assert_eq!(evaluation.keys[key], ObligationState::Verified, "{key:?}");
    }
    // The same storage history with one registered ID unreported fails.
    if let Actual::Run(run) = &mut r.results[0].actual {
        let first = run.reports.keys().next().cloned().unwrap();
        run.reports.remove(&first);
    }
    let evaluation = header_acceptance::evaluate(&m, &required(&m), &r, SUBJECT);
    assert!(blocked(&evaluation.keys[&keys[0]]));
}

/// Applies `defect` to a genuine result file and requires `key` to be
/// blocked and acceptance to fail.
fn refused(key: &Key, defect: impl FnOnce(&mut ResultFile)) {
    let manifest = manifest();
    let mut results = results();
    defect(&mut results);
    let evaluation =
        header_acceptance::evaluate(&manifest, &required(&manifest), &results, SUBJECT);
    assert!(
        blocked(&evaluation.keys[key]),
        "{key:?}: {:?}",
        evaluation.keys[key]
    );
    assert!(!evaluation.accepted());
}

#[test]
fn the_evaluator_refuses_each_single_defect() {
    let holds = Key::passing("t/a");
    let stays = Key::passing("t/b");
    let catch = Key::catch("t/a");
    let late = Key::Case {
        property: "t/a".into(),
        name: "late-bug".into(),
    };
    let machine = Key::catch(MACHINE);
    let native = Key::Native {
        patch: NATIVE.into(),
    };
    let replay = Key::Replay {
        run: HISTORY.into(),
    };
    let test = witness_key(TP);
    // A missing, duplicated or other-commit result.
    refused(&holds, |r| {
        r.results.retain(|record| record.expected != holds_id())
    });
    refused(&holds, |r| {
        let copy = record_mut(r, &holds_id()).clone();
        r.results.push(copy);
    });
    refused(&holds, |r| {
        record_mut(r, &holds_id()).commit = "other".into()
    });
    // A test skipped, failed, run twice, not run, or run from another file.
    let tp = Expected::Passes { test: TP }.id().unwrap();
    for outcome in [TestOutcome::Skipped, TestOutcome::Failed] {
        refused(&test, |r| {
            record_mut(r, &tp).actual = test_actual(TP, outcome)
        });
    }
    refused(&test, |r| {
        if let Actual::Test { ran, .. } = &mut record_mut(r, &tp).actual {
            let again = ran[0].clone();
            ran.push(again);
        }
    });
    refused(&test, |r| {
        if let Actual::Test { ran, .. } = &mut record_mut(r, &tp).actual {
            ran.clear();
        }
    });
    refused(&test, |r| {
        record_mut(r, &tp).actual =
            test_actual(sim_test("tests/u.rs", TP.test), TestOutcome::Passed)
    });
    // A counted run that failed the harness, leaked a task, ended on a
    // stopping violation, skipped or stopped its declared actions, or lost
    // its target's applicability or trigger.
    let defects: [fn(&mut Execution); 7] = [
        |run| {
            run.end = End::HarnessFailure {
                diagnostic: DIAGNOSTIC.into(),
            }
        },
        |run| run.leaked = 1,
        |run| {
            run.end = End::Violation {
                property: "t/a".into(),
            }
        },
        |run| run.actions = Actions::Incomplete,
        |run| run.actions = Actions::Prefix,
        |run| {
            run.reports
                .insert("t/a".into(), Report::Held { applicable: 0 });
        },
        |run| run.trigger = false,
    ];
    for defect in defects {
        refused(&holds, |r| defect(run_of(r, &holds_id())));
    }
    // The other reports: the expected NotApplicable with another reason, or
    // held instead; a missing or extra report; another property left
    // unsettled; and a Draft override on a normal run.
    let report_defects: [fn(&mut Execution); 7] = [
        |run| {
            run.reports.insert("t/b".into(), na("another reason"));
        },
        |run| {
            run.reports.insert("t/b".into(), held());
        },
        |run| {
            run.reports.remove("t/b");
        },
        |run| {
            run.reports.insert("t/c".into(), held());
        },
        |run| {
            let b = run.reports.remove("t/b").unwrap();
            run.reports.insert("t/c".into(), b);
        },
        |run| {
            run.reports
                .insert("t/b".into(), na("unsettled:teardown-open"));
        },
        |run| run.meta.draft_override = vec!["t/a".into()],
    ];
    for defect in report_defects {
        refused(&holds, |r| defect(run_of(r, &holds_id())));
    }
    // A StaysNotApplicable with another reason, an unrelated violation or a
    // missing report.
    let stays_defects: [fn(&mut Execution); 3] = [
        |run| {
            run.reports.insert("t/b".into(), na("undeclared"));
        },
        |run| {
            run.reports.insert("t/a".into(), Report::Violated);
        },
        |run| {
            run.reports.remove("t/a");
        },
    ];
    for defect in stays_defects {
        refused(&stays, |r| defect(run_of(r, &stays_id())));
    }
    // A catch whose violated set is off by one ID, in the patched or Draft
    // run, or whose target the patched run doesn't violate.
    refused(&catch, |r| {
        caught_runs(r, CASE)
            .1
            .reports
            .insert("t/b".into(), Report::Violated);
    });
    refused(&catch, |r| {
        caught_runs(r, CASE)
            .2
            .reports
            .insert("t/b".into(), Report::Violated);
    });
    refused(&catch, |r| {
        caught_runs(r, CASE).1.reports.insert("t/a".into(), held());
    });
    // A Draft run with the right violated set, but another property
    // left open or unexpectedly NotApplicable (rule 16).
    refused(&catch, |r| {
        caught_runs(r, CASE)
            .2
            .reports
            .insert("t/b".into(), na("stopped-by-violation"));
    });
    refused(&catch, |r| {
        caught_runs(r, CASE)
            .2
            .reports
            .insert("t/b".into(), na("why"));
    });
    // Stopped by a property outside its set, completed where its row says
    // a step, a harness failure, or a lost patched trigger.
    refused(&catch, |r| {
        caught_runs(r, CASE).1.end = End::Violation {
            property: "t/b".into(),
        }
    });
    refused(&catch, |r| caught_runs(r, CASE).1.end = End::Completed);
    refused(&catch, |r| {
        caught_runs(r, CASE).1.end = End::HarnessFailure {
            diagnostic: "x".into(),
        }
    });
    refused(&catch, |r| caught_runs(r, CASE).1.trigger = false);
    // A baseline that doesn't hold or lost its trigger, a Draft run that
    // doesn't complete, a Draft run without its override, and an override
    // on the patched or baseline run.
    refused(&catch, |r| {
        caught_runs(r, CASE)
            .0
            .reports
            .insert("t/a".into(), Report::Violated);
    });
    refused(&catch, |r| caught_runs(r, CASE).0.trigger = false);
    refused(&catch, |r| caught_runs(r, CASE).2.actions = Actions::Prefix);
    refused(&catch, |r| {
        caught_runs(r, CASE).2.meta.draft_override.clear()
    });
    refused(&late, |r| {
        caught_runs(r, END_CASE).1.meta.draft_override = vec!["t/a".into()]
    });
    refused(&catch, |r| {
        caught_runs(r, CASE).0.meta.draft_override = vec!["t/a".into()]
    });
    // A companion shown false, missing, twice, or judged on another run.
    refused(&catch, |r| caught_runs(r, CASE).3[0].held = false);
    refused(&catch, |r| caught_runs(r, CASE).3[0].name = "open".into());
    refused(&catch, |r| caught_runs(r, CASE).3.clear());
    refused(&catch, |r| {
        let again = caught_runs(r, CASE).3[0].clone();
        caught_runs(r, CASE).3.push(again);
    });
    refused(&catch, |r| {
        caught_runs(r, CASE).3[0].run = RunId::case(CASE, CaseRun::Patched)
    });
    refused(&catch, |r| {
        caught_runs(r, CASE).3[0].run = RunId::case(END_CASE, CaseRun::Draft)
    });
    // An inexact replay, unequal reports or stores, a replay that ran on
    // another harness, or one with an override its run doesn't carry.
    for field in 0..3 {
        refused(&replay, |r| {
            if let Actual::Replay {
                exact,
                reports_equal,
                stores_equal,
                ..
            } = replay_actual(r)
            {
                *[exact, reports_equal, stores_equal][field] = false;
            }
        });
    }
    refused(&replay, |r| {
        if let Actual::Replay { meta, .. } = replay_actual(r) {
            meta.harness = QUIET.into();
        }
    });
    refused(&replay, |r| {
        if let Actual::Replay { meta, .. } = replay_actual(r) {
            meta.draft_override = vec!["t/a".into()];
        }
    });
    // A replay of an execution its own result didn't count: the history
    // ran on another harness, or was killed, while the replay is unchanged.
    refused(&replay, |r| {
        if let Actual::Run(run) = &mut record_mut(r, &holds_id()).actual {
            run.meta.harness = QUIET.into();
        }
    });
    refused(&replay, |r| {
        if let Actual::Run(run) = &mut record_mut(r, &holds_id()).actual {
            run.end = End::Killed;
        }
    });
    // A result of the wrong kind.
    refused(&holds, |r| {
        record_mut(r, &holds_id()).actual = test_actual(TP, TestOutcome::Passed)
    });
    // A machine catch that fails its harness, a baseline stopped by a
    // violation, or a patched run that lost its trigger.
    refused(&machine, |r| {
        machine_runs(r).1.end = End::HarnessFailure {
            diagnostic: "panic".into(),
        }
    });
    refused(&machine, |r| {
        machine_runs(r).0.end = End::Violation {
            property: MACHINE.into(),
        }
    });
    refused(&machine, |r| machine_runs(r).1.trigger = false);
    // An integrity mutation whose clean history violates a property, stops
    // its actions, leaks, or reports nothing, whose diagnostic differs, or
    // whose runs ran another history.
    let clean_defects: [fn(&mut Execution); 5] = [
        |run| {
            run.reports.insert("t/s".into(), Report::Violated);
        },
        |run| run.actions = Actions::Incomplete,
        |run| run.leaked = 1,
        |run| run.reports.clear(),
        |run| run.trigger = false,
    ];
    for defect in clean_defects {
        refused(&native, |r| {
            if let Actual::IntegrityCaught { restored, .. } = native_actual(r) {
                defect(restored);
            }
        });
        refused(&native, |r| {
            if let Actual::IntegrityCaught { unpatched, .. } = native_actual(r) {
                defect(unpatched);
            }
        });
    }
    refused(&native, |r| {
        if let Actual::IntegrityCaught { patched, .. } = native_actual(r) {
            patched.end = End::HarnessFailure {
                diagnostic: "ReservedMismatch".into(),
            };
        }
    });
    // Each of the three runs names the history it ran. One run, only, of
    // another history on the same harness.
    // Each of the three runs names the history it ran: each one, alone,
    // naming a history that differs from it in one identity field.
    let native_run: RunId = NATIVE_HISTORY.into();
    let one_off: Vec<RunId> = {
        let RunId::History {
            harness,
            package,
            file,
            test,
            case,
        } = native_run.clone()
        else {
            unreachable!()
        };
        let with = |f: &dyn Fn(&mut [&mut String; 5])| {
            let (mut h, mut p, mut fi, mut name, mut c) = (
                harness.clone(),
                package.clone(),
                file.clone(),
                test.clone(),
                case.clone(),
            );
            f(&mut [&mut h, &mut p, &mut fi, &mut name, &mut c]);
            RunId::History {
                harness: h,
                package: p,
                file: fi,
                test: name,
                case: c,
            }
        };
        (0..5)
            .map(|at| with(&|fields| fields[at].push_str("-other")))
            .collect()
    };
    for which in [CaseRun::Unpatched, CaseRun::Patched, CaseRun::Restored] {
        for other in &one_off {
            refused(&native, |r| {
                if let Actual::IntegrityCaught {
                    unpatched,
                    patched,
                    restored,
                } = native_actual(r)
                {
                    let run = match which {
                        CaseRun::Unpatched => unpatched,
                        CaseRun::Patched => patched,
                        _ => restored,
                    };
                    run.meta.input = Some(other.clone());
                }
            });
        }
    }
    // A non-native run that names an input.
    refused(&Key::passing("t/a"), |r| {
        run_of(r, &holds_id()).meta.input = Some(HISTORY.into())
    });
    // A duplicate native mutation blocks unless its covering case is
    // caught: the case never ran, its patched run held, the catch covering
    // it is gone, or it shares another case's catch.
    let covering = duplicate_caught().id().unwrap();
    for state in [
        duplicate_state(|_, r| r.results.retain(|record| record.expected != covering)),
        duplicate_state(|_, r| {
            let patched = caught_runs(r, DUPLICATE_CASE).1;
            patched.end = End::Completed;
            patched.actions = Actions::Complete;
            patched
                .reports
                .insert(DUPLICATE_CASE.property.into(), held());
        }),
        duplicate_state(|m, _| m.expected.retain(|e| e.keys != [duplicate_key()])),
        duplicate_state(|m, _| {
            let other = m
                .expected
                .iter()
                .find(|e| matches!(e.expected, Expected::Caught { case, .. } if case == CASE))
                .unwrap()
                .expected
                .clone();
            for result in &mut m.expected {
                if result.keys == [duplicate_key()] {
                    result.expected = other.clone();
                }
            }
        }),
    ] {
        assert!(blocked(&state), "{state:?}");
    }
}

/// Manifest-side defects: each must block the key it touches.
#[test]
fn the_evaluator_refuses_manifest_defects() {
    let genuine = results();
    let with = |change: &dyn Fn(&mut Manifest), key: &Key| {
        let mut manifest = self::manifest();
        let before = required(&manifest);
        change(&mut manifest);
        let evaluation = header_acceptance::evaluate(&manifest, &before, &genuine, SUBJECT);
        assert!(
            blocked(&evaluation.keys[key]),
            "{key:?}: {:?}",
            evaluation.keys[key]
        );
        evaluation
    };
    // A required key with no expected result.
    with(
        &|m| {
            m.expected
                .retain(|r| !r.keys.contains(&Key::passing("t/a")))
        },
        &Key::passing("t/a"),
    );
    // A gate tag dropped, which fails its gate too.
    let evaluation = with(&|m| m.expected[0].gates.clear(), &Key::passing("t/a"));
    assert!(!evaluation.gates[&Gate::G2]);
    // An expected result whose payload doesn't name its key.
    with(
        &|m| {
            if let Expected::Holds { property, .. } = &mut m.expected[0].expected {
                *property = "t/b";
            }
        },
        &Key::passing("t/a"),
    );
    // An unapproved standing Open, a transitional one, and an unapproved
    // NotApplicable reason.
    let standing = Key::Requirement {
        name: STANDING.into(),
    };
    for (approval, until) in [
        (Approval::Pending, None),
        (Approval::Adam { record: "x" }, Some("hdr-14.3")),
    ] {
        with(
            &|m| {
                for result in &mut m.expected {
                    if let Expected::Open {
                        approval: a,
                        until: u,
                        ..
                    } = &mut result.expected
                    {
                        *a = approval;
                        *u = until;
                    }
                }
            },
            &standing,
        );
    }
    with(
        &|m| {
            for result in &mut m.expected {
                if let Expected::StaysNotApplicable { approval, .. } = &mut result.expected {
                    *approval = Approval::Pending;
                }
            }
        },
        &Key::passing("t/b"),
    );
    // A machine case listed as a node catch, and a node case as a machine
    // one.
    with(&|m| m.machines.clear(), &Key::catch(MACHINE));
    with(&|m| m.machines.push(NODE), &Key::catch("t/a"));
    // A result naming no expected result is stray.
    let mut stray = results();
    stray
        .results
        .push(test_record(sim_test("tests/t.rs", "unlisted")));
    let manifest = self::manifest();
    let evaluation = header_acceptance::evaluate(&manifest, &required(&manifest), &stray, SUBJECT);
    assert!(!evaluation.stray.is_empty());
    assert!(!evaluation.accepted());
}

/// A harness-caught case (Adam's ruling on question 7) verifies only with
/// its baseline holding and its patched run ending at its own property's
/// missing evidence that says its detail, violating nothing. Every other
/// end blocks it, and a contract's proving catch may name it.
#[test]
fn the_evaluator_judges_a_harness_caught_case() {
    const STUCK: CaseRef = CaseRef {
        property: "t/a",
        name: "stuck",
    };
    const DETAIL: &str = "with a boundary unread";
    let key = Key::Case {
        property: "t/a".into(),
        name: "stuck".into(),
    };
    let caught = Expected::HarnessCaught {
        case: STUCK,
        harness: NODE,
        detail: DETAIL,
        not_applicable: &[],
    };
    let mut m = manifest();
    m.expected
        .push(er(vec![key.clone()], &[Gate::G2], caught.clone()));
    let id = caught.id().unwrap();
    let ended = |diagnostic: &str| {
        let mut patched = execution(
            RunId::case(STUCK, CaseRun::Patched),
            NODE,
            &[("t/a", na("harness-failed")), ("t/b", na("harness-failed"))],
        );
        patched.end = End::HarnessFailure {
            diagnostic: diagnostic.into(),
        };
        patched
    };
    let with = |patched: Execution| {
        let mut results = results();
        results.results.push(ResultRecord {
            expected: id.clone(),
            commit: S.into(),
            actual: Actual::HarnessCaught {
                baseline: execution(
                    RunId::case(STUCK, CaseRun::Baseline),
                    NODE,
                    &[("t/a", held()), ("t/b", held())],
                ),
                patched,
            },
        });
        results
    };
    let missing = format!("t/a lacks evidence after step 9: the run ran dry after step 9 {DETAIL}");
    assert_eq!(
        state(&m, &with(ended(&missing)), &key),
        ObligationState::Verified
    );

    // Another property's missing evidence, another detail, another harness
    // failure, no step, a completed run or a stop at a violation blocks it.
    for diagnostic in [
        format!("t/b lacks evidence after step 9: {DETAIL}"),
        "t/a lacks evidence after step 9: a gap".to_owned(),
        format!("t/a lacks evidence after step : {DETAIL}"),
        format!("source 3's evidence failed: {DETAIL}"),
    ] {
        assert!(
            blocked(&state(&m, &with(ended(&diagnostic)), &key)),
            "{diagnostic}"
        );
    }
    let mut completed = ended(&missing);
    completed.end = End::Completed;
    assert!(blocked(&state(&m, &with(completed), &key)));
    let mut stopped = ended(&missing);
    stopped.end = End::Violation {
        property: "t/a".into(),
    };
    assert!(blocked(&state(&m, &with(stopped), &key)));
    // A violation beside the missing evidence, or a leaked task, blocks it.
    let mut violating = ended(&missing);
    violating.reports.insert("t/b".into(), Report::Violated);
    assert!(blocked(&state(&m, &with(violating), &key)));
    let mut leaking = ended(&missing);
    leaking.leaked = 1;
    assert!(blocked(&state(&m, &with(leaking), &key)));
    // A Caught's record never stands in for it.
    let mut wrong = with(ended(&missing));
    wrong.results.last_mut().unwrap().actual = caught_actual(STUCK, Found::AtStep);
    assert!(blocked(&state(&m, &wrong, &key)));

    // As a contract's proving catch, it decides that contract.
    let catch_key = contracts::contract_catch_key(ContractId::C3, STUCK);
    m.contracts[0].proving_catches = &[STUCK];
    m.expected.push(er(vec![catch_key.clone()], &[], caught));
    assert_eq!(
        state(&m, &with(ended(&missing)), &catch_key),
        ObligationState::Verified
    );
    let other = format!("t/b lacks evidence after step 9: {DETAIL}");
    assert!(blocked(&state(&m, &with(ended(&other)), &catch_key)));
    assert!(blocked(&state(
        &m,
        &with(ended(&other)),
        &Key::passing("t/a")
    )));
}

#[test]
fn the_evaluator_applies_contracts_and_narrowing() {
    let holds = Key::passing("t/a");
    let quiet = Key::passing("t/q");
    let replay = Key::Replay {
        run: HISTORY.into(),
    };
    let standing = Key::Requirement {
        name: STANDING.into(),
    };
    let excluded = |reason| ContractState::Excluded {
        reason,
        approval: Approval::Adam { record: "test" },
    };
    let blocked_c11 = |m: &mut Manifest| {
        m.contracts[2].state = ContractState::Blocked { waiting_for: "E8" };
    };

    // A blocked contract blocks every result it applies to.
    let mut m = manifest();
    m.contracts[0].state = ContractState::Blocked { waiting_for: "E1" };
    assert!(blocked(&state(&m, &results(), &holds)));

    // A proving catch decides its contract as a proving test does: C3,
    // proved by `CASE`'s catch too, counts only while that catch's own
    // judgment passes, and only the `Caught` of exactly that case covers
    // its key.
    let catch_key = contracts::contract_catch_key(ContractId::C3, CASE);
    let caught_of = |m: &Manifest, case: CaseRef| {
        m.expected
            .iter()
            .find(|r| matches!(r.expected, Expected::Caught { case: c, .. } if c == case))
            .unwrap()
            .expected
            .clone()
    };
    let mut proved = manifest();
    proved.contracts[0].proving_catches = &[CASE];
    let caught = caught_of(&proved, CASE);
    proved
        .expected
        .push(er(vec![catch_key.clone()], &[], caught));
    assert_eq!(
        state(&proved, &results(), &catch_key),
        ObligationState::Verified
    );
    assert_eq!(
        state(&proved, &results(), &holds),
        ObligationState::Verified
    );
    let mut lost = results();
    caught_runs(&mut lost, CASE).1.trigger = false;
    assert!(blocked(&state(&proved, &lost, &catch_key)));
    assert!(blocked(&state(&proved, &lost, &holds)));
    let mut other = manifest();
    other.contracts[0].proving_catches = &[CASE];
    let late = caught_of(&other, END_CASE);
    other.expected.push(er(vec![catch_key.clone()], &[], late));
    assert!(blocked(&state(&other, &results(), &catch_key)));
    assert!(blocked(&state(&other, &results(), &holds)));

    // A satisfied contract whose proving test failed, is missing, ran at
    // another commit, or ran another test blocks.
    let t3 = Expected::Passes { test: T3 }.id().unwrap();
    let t3_defects: [fn(&mut ResultFile); 4] = [
        |r| {
            let t3 = Expected::Passes { test: T3 }.id().unwrap();
            record_mut(r, &t3).actual = test_actual(T3, TestOutcome::Failed)
        },
        |r| {
            let t3 = Expected::Passes { test: T3 }.id().unwrap();
            r.results.retain(|record| record.expected != t3)
        },
        |r| {
            let t3 = Expected::Passes { test: T3 }.id().unwrap();
            record_mut(r, &t3).commit = "other".into()
        },
        |r| {
            let t3 = Expected::Passes { test: T3 }.id().unwrap();
            record_mut(r, &t3).actual = test_actual(T5, TestOutcome::Passed)
        },
    ];
    let _ = t3;
    for defect in t3_defects {
        let mut results = results();
        defect(&mut results);
        assert!(blocked(&state(&manifest(), &results, &holds)));
    }

    // A required contract with its row deleted blocks.
    let mut m = manifest();
    m.contracts
        .retain(|contract| contract.id != ContractId::C11);
    assert!(blocked(&state(&m, &results(), &holds)));

    // An approved exclusion excludes, an unapproved one blocks.
    let mut m = manifest();
    m.contracts[0].state = excluded("left out");
    assert!(matches!(
        state(&m, &results(), &holds),
        ObligationState::Excluded { .. }
    ));
    m.contracts[0].state = ContractState::Excluded {
        reason: "left out",
        approval: Approval::Pending,
    };
    assert!(blocked(&state(&m, &results(), &holds)));

    // A transitional Open stays blocked under an approved exclusion.
    let mut m = manifest();
    m.contracts[0].state = excluded("left out");
    for result in &mut m.expected {
        if let Expected::Open { until, .. } = &mut result.expected {
            *until = Some("hdr-14.3");
        }
    }
    assert!(blocked(&state(&m, &results(), &standing)));

    // One excluded and one blocked dependency: blocked. So is an approved
    // standing Open beside a blocked contract.
    let mut m = manifest();
    m.contracts[0].state = excluded("left out");
    blocked_c11(&mut m);
    assert!(blocked(&state(&m, &results(), &holds)));
    assert!(blocked(&state(&m, &results(), &standing)));

    // The narrowing entry releases t/quiet from a blocked C11, and only it.
    let mut m = manifest();
    blocked_c11(&mut m);
    assert_eq!(state(&m, &results(), &quiet), ObligationState::Verified);
    assert!(blocked(&state(&m, &results(), &holds)));
    // A replay of a t/node run claiming t/quiet's harness isn't released.
    let mut claimed = results();
    if let Actual::Replay { meta, .. } = replay_actual(&mut claimed) {
        meta.harness = QUIET.into();
    }
    assert!(blocked(&state(&m, &claimed, &replay)));

    // An excluded C11 excludes exactly what the entry doesn't release, and
    // a missing peerless result stays blocked.
    let mut m = manifest();
    m.contracts[2].state = excluded("no peer checkpoint");
    assert_eq!(state(&m, &results(), &quiet), ObligationState::Verified);
    assert!(matches!(
        state(&m, &results(), &holds),
        ObligationState::Excluded { .. }
    ));
    let mut missing = results();
    missing.results.retain(|record| {
        record.expected
            != ExpectedId::Holds {
                property: "t/q".into(),
                run: QUIET_HISTORY.into(),
            }
    });
    assert!(blocked(&state(&m, &missing, &quiet)));
    // Whether C11 applies is a fact of the expected result's identity, not
    // of its record: a missing or failing peerless result is still released
    // from a blocked C11, so with nothing else applying it blocks on its own
    // failure, and with an approved excluded C3 it's excluded.
    let mut m = manifest();
    blocked_c11(&mut m);
    assert!(blocked(&state(&m, &missing, &quiet)));
    m.contracts[0].state = excluded("no replay");
    assert!(matches!(
        state(&m, &missing, &quiet),
        ObligationState::Excluded { .. }
    ));
    let mut failing = results();
    run_of(
        &mut failing,
        &ExpectedId::Holds {
            property: "t/q".into(),
            run: QUIET_HISTORY.into(),
        },
    )
    .leaked = 1;
    assert!(matches!(
        state(&m, &failing, &quiet),
        ObligationState::Excluded { .. }
    ));
    // A result C11 does apply to, blocked, stays blocked beside C3's
    // exclusion.
    assert!(blocked(&state(&m, &results(), &holds)));

    // An entry whose test is pending or failed releases nothing.
    let mut m = manifest();
    blocked_c11(&mut m);
    let mut pending = m.clone();
    pending.narrowing = vec![quiet_entry(NarrowingTest::Pending { until: "hdr-14.7a" })];
    for result in &mut pending.expected {
        if result
            .keys
            .contains(&Key::NarrowingTest { entry: "N1".into() })
        {
            result.expected = Expected::Open {
                reason: "pending",
                owner: "test",
                approval: Approval::Pending,
                until: Some("hdr-14.7a"),
            };
        }
    }
    assert!(blocked(&state(&pending, &results(), &quiet)));
    let mut failed = results();
    record_mut(&mut failed, &Expected::Passes { test: TN }.id().unwrap()).actual =
        test_actual(TN, TestOutcome::Failed);
    assert!(blocked(&state(&m, &failed, &quiet)));

    // A key that shares a contract test's result keeps its own
    // dependencies: with C11 blocked, the contract-test key verifies and
    // the witness key on the same result doesn't.
    let mut shared = manifest();
    blocked_c11(&mut shared);
    for result in &mut shared.expected {
        if matches!(result.expected, Expected::Passes { test } if test == T3) {
            result.keys.push(witness_key(T3));
        }
    }
    let evaluation = header_acceptance::evaluate(&shared, &required(&shared), &results(), SUBJECT);
    assert_eq!(
        evaluation.keys[&header_acceptance::contracts::contract_test_key(ContractId::C3, T3)],
        ObligationState::Verified
    );
    assert!(blocked(&evaluation.keys[&witness_key(T3)]));

    // A blocked C5 blocks the other contracts' proving tests, so
    // everything. So does C5's own proving test from a binary that doesn't
    // embed the subject's source, and a subject source the tool couldn't
    // read.
    let mut m = manifest();
    m.contracts[1].state = ContractState::Blocked { waiting_for: "E7" };
    assert!(blocked(&state(&m, &results(), &quiet)));
    let t5 = Expected::Passes { test: T5 }.id().unwrap();
    for source in [None, Some("another-source".to_owned())] {
        let mut stale = results();
        if let Actual::Test { binary_source, .. } = &mut record_mut(&mut stale, &t5).actual {
            *binary_source = source;
        }
        assert!(blocked(&state(&manifest(), &stale, &quiet)));
        let c5_key = header_acceptance::contracts::contract_test_key(ContractId::C5, T5);
        assert!(blocked(&state(&manifest(), &stale, &c5_key)));
    }
    // Nor does C5's own test from S's binary run in a checkout that has
    // since moved, or one never measured: only its checkout differs, and
    // C5's bootstrap still verifies with both of S's.
    let c5_key = header_acceptance::contracts::contract_test_key(ContractId::C5, T5);
    assert_eq!(
        state(&manifest(), &results(), &c5_key),
        ObligationState::Verified
    );
    for checkout in [None, Some("another-source".to_owned())] {
        let mut moved = results();
        if let Actual::Test {
            checkout_source, ..
        } = &mut record_mut(&mut moved, &t5).actual
        {
            *checkout_source = checkout;
        }
        assert!(blocked(&state(&manifest(), &moved, &quiet)));
        assert!(blocked(&state(&manifest(), &moved, &c5_key)));
    }
    // An unknown digest anchors nothing, even when the caller's subject,
    // the result file and every test binary all agree on it: C5's own test
    // blocks, so its dependents do, and so does a composition check.
    let unknown_subject = header_acceptance::Subject {
        commit: S,
        source: "unknown",
    };
    let check = "a_static_check";
    let mut m = manifest();
    m.expected.push(er(
        vec![Key::Composition { test: check.into() }],
        &[],
        Expected::Passes {
            test: sim_test(rows::ACCEPTANCE_TESTS, check),
        },
    ));
    let mut unknown = results();
    unknown
        .results
        .push(test_record(sim_test(rows::ACCEPTANCE_TESTS, check)));
    unknown.subject_source = "unknown".into();
    for record in &mut unknown.results {
        if let Actual::Test { binary_source, .. } = &mut record.actual {
            *binary_source = Some("unknown".into());
        }
    }
    let evaluation = header_acceptance::evaluate(&m, &required(&m), &unknown, unknown_subject);
    assert!(evaluation.stray.is_empty(), "{:?}", evaluation.stray);
    let c5_key = header_acceptance::contracts::contract_test_key(ContractId::C5, T5);
    assert!(blocked(&evaluation.keys[&c5_key]));
    assert!(blocked(&evaluation.keys[&quiet]));
    assert!(blocked(
        &evaluation.keys[&Key::Composition { test: check.into() }]
    ));
    assert!(!evaluation.accepted());

    // The committed manifest, with every contract as it stands on main,
    // blocks everything on an empty result file.
    let committed = Manifest::committed();
    let empty = ResultFile {
        subject: S.into(),
        subject_source: SOURCE.into(),
        results: Vec::new(),
    };
    let evaluation =
        header_acceptance::evaluate(&committed, &committed_required(&committed), &empty, SUBJECT);
    assert!(evaluation.stray.is_empty(), "{:?}", evaluation.stray);
    assert!(!evaluation.accepted());
    assert!(evaluation.gates.values().all(|passes| !passes));
}

/// Each result below matches its expected result in every identity field
/// but one, and must stop counting.
#[test]
fn the_evaluator_joins_on_full_identity() {
    let holds = Key::passing("t/a");
    let catch = Key::catch("t/a");
    let replay = Key::Replay {
        run: HISTORY.into(),
    };
    let other = |history: HistoryRef| -> RunId { history.into() };
    // The history's harness, file, test, package or case differs, in the
    // result's claimed identity, and in the execution it carries.
    for changed in [
        HistoryRef {
            harness: QUIET,
            ..HISTORY
        },
        HistoryRef {
            witness: sim_test("tests/u.rs", HISTORY.witness.test),
            ..HISTORY
        },
        HistoryRef {
            witness: sim_test(HISTORY.witness.file, "t_other"),
            ..HISTORY
        },
        HistoryRef {
            case: "other",
            ..HISTORY
        },
        HistoryRef {
            witness: TestRef {
                package: "zakura-network",
                ..HISTORY.witness
            },
            ..HISTORY
        },
    ] {
        refused(&holds, |r| {
            record_mut(r, &holds_id()).expected = ExpectedId::Holds {
                property: "t/a".into(),
                run: other(changed),
            }
        });
        refused(&holds, |r| run_of(r, &holds_id()).run = other(changed));
    }
    // The same history, claimed for another property.
    refused(&holds, |r| {
        record_mut(r, &holds_id()).expected = ExpectedId::Holds {
            property: "t/b".into(),
            run: HISTORY.into(),
        }
    });
    // The execution ran on another harness.
    refused(&holds, |r| {
        run_of(r, &holds_id()).meta.harness = QUIET.into()
    });
    // A catch's runs swapped between labels or cases.
    refused(&catch, |r| {
        caught_runs(r, CASE).1.run = RunId::case(CASE, CaseRun::Draft);
    });
    refused(&catch, |r| {
        caught_runs(r, CASE).1.run = RunId::case(END_CASE, CaseRun::Patched);
    });
    refused(&catch, |r| {
        caught_runs(r, CASE).2.run = RunId::case(
            CaseRef {
                property: "t/b",
                ..CASE
            },
            CaseRun::Draft,
        );
    });
    // A replay of another execution.
    refused(&replay, |r| {
        if let Actual::Replay { run, .. } = replay_actual(r) {
            *run = other(HistoryRef {
                case: "other",
                ..HISTORY
            });
        }
    });
    // A test result claimed for the same name in another file or package,
    // and a result that ran the same name from another file.
    let test = witness_key(TP);
    let tp = Expected::Passes { test: TP }.id().unwrap();
    for changed in [
        sim_test("tests/u.rs", TP.test),
        TestRef {
            package: "xtask",
            ..TP
        },
    ] {
        refused(&test, |r| {
            record_mut(r, &tp).expected = Expected::Passes { test: changed }.id().unwrap()
        });
        refused(&test, |r| {
            record_mut(r, &tp).actual = test_actual(changed, TestOutcome::Passed)
        });
    }
}

/// Contracts and narrowing decided only through their own required keys,
/// and narrowing inputs taken only from the expected execution's role.
#[test]
fn the_evaluator_decides_contracts_through_their_keys() {
    let quiet = Key::passing("t/q");
    let holds = Key::passing("t/a");
    let blocked_c11 = |m: &mut Manifest| {
        m.contracts[2].state = ContractState::Blocked { waiting_for: "E8" };
    };
    // The narrowing entry's key removed from the manifest, with its passing
    // record still in the result file, releases nothing.
    let mut m = manifest();
    blocked_c11(&mut m);
    let before = required(&m);
    m.expected
        .retain(|r| !r.keys.contains(&Key::NarrowingTest { entry: "N1".into() }));
    let evaluation = header_acceptance::evaluate(&m, &before, &results(), SUBJECT);
    assert!(blocked(&evaluation.keys[&quiet]));
    // A proving test's key covered twice, or with a gate tag, leaves its
    // contract unproven.
    let t3_key = header_acceptance::contracts::contract_test_key(ContractId::C3, T3);
    let mut doubled = manifest();
    let before = required(&doubled);
    let at = doubled
        .expected
        .iter()
        .position(|r| r.keys.contains(&t3_key))
        .unwrap();
    let copy = doubled.expected[at].clone();
    doubled.expected.push(copy);
    assert!(blocked(
        &header_acceptance::evaluate(&doubled, &before, &results(), SUBJECT).keys[&holds]
    ));
    let mut tagged = manifest();
    let before = required(&tagged);
    tagged.expected[at].gates = vec![Gate::G1];
    assert!(blocked(
        &header_acceptance::evaluate(&tagged, &before, &results(), SUBJECT).keys[&holds]
    ));

    // A Draft replay that lost its override isn't released from a blocked
    // C9 by an entry that releases runs with an empty override, so an
    // excluded C11 can't turn its failure into an exclusion.
    const TN2: TestRef = sim_test("tests/t.rs", "n2_holds");
    const TC9: TestRef = sim_test("tests/t.rs", "c9_holds");
    let draft_run = RunId::case(CASE, CaseRun::Draft);
    let mut m = manifest();
    m.contracts[2].state = ContractState::Excluded {
        reason: "no peer checkpoint",
        approval: Approval::Adam { record: "test" },
    };
    m.contracts.push(Contract {
        state: ContractState::Blocked { waiting_for: "E5" },
        ..satisfied(ContractId::C9, &[TC9])
    });
    m.required_contracts.push(ContractId::C9);
    m.narrowing.push(Narrowing {
        entry: "N2",
        contracts: &[ContractId::C9],
        releases: |ctx| {
            !ctx.executions.is_empty() && ctx.executions.iter().all(|(empty, _)| *empty)
        },
        reason: "only Draft runs read the override",
        test: NarrowingTest::Test(TN2),
    });
    m.expected.push(er(
        vec![Key::NarrowingTest { entry: "N2".into() }],
        &[],
        Expected::Passes { test: TN2 },
    ));
    let replay = Key::Replay {
        run: draft_run.clone(),
    };
    m.expected.push(er(
        vec![replay.clone()],
        &[Gate::G1],
        Expected::Replays {
            run: draft_run.clone(),
        },
    ));
    let mut r = results();
    r.results.push(test_record(TN2));
    r.results.push(ResultRecord {
        expected: ExpectedId::Replays(draft_run.clone()),
        commit: S.into(),
        actual: Actual::Replay {
            run: draft_run.clone(),
            of: trace_of(&draft_run),
            meta: ExecMeta {
                harness: NODE.into(),
                draft_override: Vec::new(),
                run_by: RunBy::Tool,
                input: None,
            },
            exact: true,
            reports_equal: true,
            stores_equal: true,
            fresh: true,
        },
    });
    let evaluation = header_acceptance::evaluate(&m, &required(&m), &r, SUBJECT);
    assert_eq!(
        evaluation.keys[&Key::NarrowingTest { entry: "N2".into() }],
        ObligationState::Verified
    );
    assert!(
        blocked(&evaluation.keys[&replay]),
        "{:?}",
        evaluation.keys[&replay]
    );
    // With its override restored it still needs C9: the role, not the
    // record, decides.
    if let Actual::Replay { meta, .. } = &mut r.results.last_mut().unwrap().actual {
        meta.draft_override = vec!["t/a".into()];
    }
    let evaluation = header_acceptance::evaluate(&m, &required(&m), &r, SUBJECT);
    assert!(blocked(&evaluation.keys[&replay]));

    // A replay of a planted patched run isn't released from a blocked C4
    // by an entry for tool-run executions, whatever its record says.
    const TN5: TestRef = sim_test("tests/t.rs", "n5_holds");
    const TC4: TestRef = sim_test("tests/t.rs", "c4_holds");
    let patched_run = RunId::case(CASE, CaseRun::Patched);
    let mut m = manifest();
    m.contracts.push(Contract {
        state: ContractState::Blocked { waiting_for: "E6" },
        ..satisfied(ContractId::C4, &[TC4])
    });
    m.required_contracts.push(ContractId::C4);
    m.narrowing.push(Narrowing {
        entry: "N5",
        contracts: &[ContractId::C4],
        releases: |ctx| {
            !ctx.executions.is_empty() && ctx.executions.iter().all(|(_, by)| *by == RunBy::Tool)
        },
        reason: "only the planted runner's runs read C4",
        test: NarrowingTest::Test(TN5),
    });
    m.expected.push(er(
        vec![Key::NarrowingTest { entry: "N5".into() }],
        &[],
        Expected::Passes { test: TN5 },
    ));
    let replay = Key::Replay {
        run: patched_run.clone(),
    };
    m.expected.push(er(
        vec![replay.clone()],
        &[Gate::G1],
        Expected::Replays {
            run: patched_run.clone(),
        },
    ));
    let mut r = results();
    r.results.push(test_record(TN5));
    r.results.push(ResultRecord {
        expected: ExpectedId::Replays(patched_run.clone()),
        commit: S.into(),
        actual: Actual::Replay {
            of: trace_of(&patched_run),
            run: patched_run,
            meta: ExecMeta {
                harness: NODE.into(),
                draft_override: Vec::new(),
                run_by: RunBy::Planted,
                input: None,
            },
            exact: true,
            reports_equal: true,
            stores_equal: true,
            fresh: true,
        },
    });
    let evaluation = header_acceptance::evaluate(&m, &required(&m), &r, SUBJECT);
    assert_eq!(
        evaluation.keys[&Key::NarrowingTest { entry: "N5".into() }],
        ObligationState::Verified
    );
    assert!(blocked(&evaluation.keys[&replay]));
    // The tool's own history replay is released.
    assert_eq!(
        evaluation.keys[&Key::Replay {
            run: HISTORY.into()
        }],
        ObligationState::Verified
    );
}

/// Each execution's runner matches its role, a completed catch and a
/// `StaysNotApplicable` get the whole-history rule, a machine catch's
/// actions and leaks count, replays and companions name their execution's
/// own trace, and precedence holds for an excluded C5.
#[test]
fn the_evaluator_binds_runners_and_traces() {
    let catch = Key::catch("t/a");
    let late = Key::Case {
        property: "t/a".into(),
        name: "late-bug".into(),
    };
    let stays = Key::passing("t/b");
    let machine = Key::catch(MACHINE);
    let native = Key::Native {
        patch: NATIVE.into(),
    };
    let replay = Key::Replay {
        run: HISTORY.into(),
    };
    // The runner each record names must be its role's.
    refused(&replay, |r| {
        if let Actual::Replay { meta, .. } = replay_actual(r) {
            meta.run_by = RunBy::Planted;
        }
    });
    refused(&catch, |r| caught_runs(r, CASE).0.meta.run_by = RunBy::Tool);
    refused(&catch, |r| {
        caught_runs(r, CASE).2.meta.run_by = RunBy::Planted
    });
    // A completed end-found catch with an unrelated open obligation or an
    // unexpected NotApplicable.
    refused(&late, |r| {
        caught_runs(r, END_CASE)
            .1
            .reports
            .insert("t/b".into(), na("stopped-by-violation"));
    });
    refused(&late, |r| {
        caught_runs(r, END_CASE)
            .1
            .reports
            .insert("t/b".into(), na("why"));
    });
    // A StaysNotApplicable that lost its trigger.
    refused(&stays, |r| run_of(r, &stays_id()).trigger = false);
    // A machine baseline that didn't run its actions, or either machine run
    // leaking.
    refused(&machine, |r| {
        machine_runs(r).0.actions = Actions::Incomplete
    });
    refused(&machine, |r| machine_runs(r).0.leaked = 1);
    refused(&machine, |r| machine_runs(r).1.leaked = 1);
    // A replay of another trace of the counted execution, and a companion
    // judged on another trace of the Draft run.
    refused(&replay, |r| {
        if let Actual::Replay { of, .. } = replay_actual(r) {
            *of = "another trace".into();
        }
    });
    refused(&catch, |r| {
        caught_runs(r, CASE).3[0].trace = "another trace".into()
    });
    // An integrity clean run with an unlisted NotApplicable.
    refused(&native, |r| {
        if let Actual::IntegrityCaught { restored, .. } = native_actual(r) {
            restored.reports.insert("t/s".into(), na("why"));
        }
    });

    // An excluded C5 excludes a contract whose proving test passed, and so
    // its dependents. One whose proving test is missing, failed or from
    // another commit still blocks them.
    let mut m = manifest();
    m.contracts[1].state = ContractState::Excluded {
        reason: "no provenance",
        approval: Approval::Adam { record: "test" },
    };
    assert!(matches!(
        state(&m, &results(), &Key::passing("t/a")),
        ObligationState::Excluded { .. }
    ));
    let t3 = Expected::Passes { test: T3 }.id().unwrap();
    let mut missing = results();
    missing.results.retain(|record| record.expected != t3);
    let mut failed = results();
    record_mut(&mut failed, &t3).actual = test_actual(T3, TestOutcome::Failed);
    let mut stale = results();
    record_mut(&mut stale, &t3).commit = "another commit".into();
    for broken in [missing, failed, stale] {
        assert!(blocked(&state(&m, &broken, &Key::passing("t/a"))));
    }

    // Two narrowing entries of one name release nothing.
    let mut m = manifest();
    m.contracts[2].state = ContractState::Blocked { waiting_for: "E8" };
    m.narrowing.push(Narrowing {
        releases: |ctx| ctx.harness.as_deref() == Some(NODE),
        ..quiet_entry(NarrowingTest::Test(sim_test("tests/t.rs", "absent")))
    });
    assert!(blocked(&state(&m, &results(), &Key::passing("t/q"))));
    assert!(blocked(&state(&m, &results(), &Key::passing("t/a"))));

    // A composition check that didn't pass fails every gate, not only the
    // overall acceptance.
    let check = "a_static_check";
    let mut m = manifest();
    m.expected.push(er(
        vec![Key::Composition { test: check.into() }],
        &[],
        Expected::Passes {
            test: sim_test(rows::ACCEPTANCE_TESTS, check),
        },
    ));
    let evaluation = header_acceptance::evaluate(&m, &required(&m), &results(), SUBJECT);
    assert!(evaluation.gates.values().all(|passes| !passes));
    let mut passing = results();
    passing
        .results
        .push(test_record(sim_test(rows::ACCEPTANCE_TESTS, check)));
    let evaluation = header_acceptance::evaluate(&m, &required(&m), &passing, SUBJECT);
    assert!(evaluation.gates.values().all(|passes| *passes));
    // A composition check carries its own provenance anchor, so an excluded
    // C5 leaves it verified, and a gate can pass with that exclusion. One
    // whose binary doesn't embed the subject's digest blocks, and fails
    // every gate, whatever C5's standing.
    m.contracts[1].state = ContractState::Excluded {
        reason: "no provenance",
        approval: Approval::Adam { record: "test" },
    };
    let evaluation = header_acceptance::evaluate(&m, &required(&m), &passing, SUBJECT);
    assert_eq!(
        evaluation.keys[&Key::Composition { test: check.into() }],
        ObligationState::Verified
    );
    let mut unanchored = passing.clone();
    if let Actual::Test { binary_source, .. } = &mut unanchored.results.last_mut().unwrap().actual {
        *binary_source = None;
    }
    for contracts in [m.contracts.clone(), manifest().contracts] {
        let m = Manifest {
            contracts,
            ..m.clone()
        };
        let evaluation = header_acceptance::evaluate(&m, &required(&m), &unanchored, SUBJECT);
        assert!(blocked(
            &evaluation.keys[&Key::Composition { test: check.into() }]
        ));
        assert!(evaluation.gates.values().all(|passes| !passes));
    }

    // A clean integrity run reporting a Held with no applicable step.
    refused(&native, |r| {
        if let Actual::IntegrityCaught { restored, .. } = native_actual(r) {
            restored
                .reports
                .insert("t/s".into(), Report::Held { applicable: 0 });
        }
    });

    // A record whose execution claims another history's identity indexes
    // nothing: its trace neither disputes the genuine history's nor
    // supplies one.
    let mut misbound = results();
    run_of(&mut misbound, &holds_id()).run = QUIET_HISTORY.into();
    let genuine = manifest();
    let evaluation = header_acceptance::evaluate(&genuine, &required(&genuine), &misbound, SUBJECT);
    assert_eq!(
        evaluation.keys[&Key::passing("t/q")],
        ObligationState::Verified
    );
    assert!(blocked(&evaluation.keys[&Key::passing("t/a")]));
    assert!(blocked(&evaluation.keys[&replay]));

    // A result file whose two digests agree with each other but not with
    // the subject the caller established is stray, and C5's bootstrap
    // fails.
    let mut dirty = results();
    dirty.subject_source = "dirty-checkout".into();
    let t5 = Expected::Passes { test: T5 }.id().unwrap();
    if let Actual::Test { binary_source, .. } = &mut record_mut(&mut dirty, &t5).actual {
        *binary_source = Some("dirty-checkout".into());
    }
    let evaluation = header_acceptance::evaluate(&genuine, &required(&genuine), &dirty, SUBJECT);
    assert!(!evaluation.stray.is_empty());
    assert!(blocked(
        &evaluation.keys[&header_acceptance::contracts::contract_test_key(ContractId::C5, T5)]
    ));
    assert!(!evaluation.accepted());
    assert!(evaluation.gates.values().all(|passes| !passes));

    // A machine catch's patched run with failed action coverage, or a
    // completed one that checked only a prefix.
    refused(&machine, |r| {
        machine_runs(r).1.actions = Actions::Incomplete
    });
    refused(&machine, |r| {
        let (_, patched) = machine_runs(r);
        patched.end = End::Completed;
        patched.actions = Actions::Prefix;
    });

    // An approved standing Open that names no owner blocks.
    let mut m = manifest();
    let at = m
        .expected
        .iter()
        .position(|r| matches!(r.expected, Expected::Open { until: None, .. }))
        .unwrap();
    let key = m.expected[at].keys[0].clone();
    if let Expected::Open { owner, .. } = &mut m.expected[at].expected {
        *owner = "";
    }
    assert!(blocked(&state(&m, &results(), &key)), "{key:?}");

    // Any test whose record says its binary came from another source blocks
    // its key, not only C5's own.
    let t3 = Expected::Passes { test: T3 }.id().unwrap();
    let t3_key = header_acceptance::contracts::contract_test_key(ContractId::C3, T3);
    let mut stale = results();
    if let Actual::Test { binary_source, .. } = &mut record_mut(&mut stale, &t3).actual {
        *binary_source = Some("old-source".into());
    }
    assert!(blocked(&state(&manifest(), &stale, &t3_key)));
    assert!(blocked(&state(&manifest(), &stale, &Key::passing("t/a"))));
    // So does one from S's binary that ran in another checkout, whose files
    // it read, or in one never measured.
    for checkout in [Some("old-source".to_owned()), None] {
        let mut moved = results();
        if let Actual::Test {
            checkout_source, ..
        } = &mut record_mut(&mut moved, &t3).actual
        {
            *checkout_source = checkout;
        }
        assert!(blocked(&state(&manifest(), &moved, &t3_key)));
    }
    // So does one whose digest couldn't be read, for a proving test and for
    // a narrowing test, which then releases nothing.
    let mut unread = results();
    if let Actual::Test { binary_source, .. } = &mut record_mut(&mut unread, &t3).actual {
        *binary_source = None;
    }
    assert!(blocked(&state(&manifest(), &unread, &t3_key)));
    assert!(blocked(&state(&manifest(), &unread, &Key::passing("t/a"))));
    let tn = Expected::Passes { test: TN }.id().unwrap();
    let mut unread = results();
    if let Actual::Test { binary_source, .. } = &mut record_mut(&mut unread, &tn).actual {
        *binary_source = None;
    }
    let mut m = manifest();
    m.contracts[2].state = ContractState::Blocked { waiting_for: "E8" };
    assert!(blocked(&state(
        &m,
        &unread,
        &Key::NarrowingTest { entry: "N1".into() }
    )));
    assert!(blocked(&state(&m, &unread, &Key::passing("t/q"))));

    // A proving test's result covering another contract's key with the same
    // test, or a narrowing test's covering another entry's, proves nothing
    // for the key the required inventory names.
    let mut m = manifest();
    let at = m
        .expected
        .iter()
        .position(|r| r.keys.contains(&t3_key))
        .unwrap();
    m.expected[at].keys = vec![header_acceptance::contracts::contract_test_key(
        ContractId::C11,
        T3,
    )];
    let fixed = required(&manifest());
    let evaluation = header_acceptance::evaluate(&m, &fixed, &results(), SUBJECT);
    assert!(blocked(&evaluation.keys[&t3_key]));
    assert!(blocked(&evaluation.keys[&Key::passing("t/a")]));
    let mut m = manifest();
    m.contracts[2].state = ContractState::Blocked { waiting_for: "E8" };
    let entry = m.narrowing[0].entry;
    let at = m
        .expected
        .iter()
        .position(|r| {
            r.keys.contains(&Key::NarrowingTest {
                entry: entry.into(),
            })
        })
        .unwrap();
    m.expected[at].keys = vec![Key::NarrowingTest {
        entry: "N-other".into(),
    }];
    let evaluation = header_acceptance::evaluate(&m, &fixed, &results(), SUBJECT);
    assert!(blocked(
        &evaluation.keys[&Key::NarrowingTest {
            entry: entry.into()
        }]
    ));
    assert!(blocked(&evaluation.keys[&Key::passing("t/q")]));

    // A replay naming an input, or run in the recording process.
    refused(&replay, |r| {
        if let Actual::Replay { meta, .. } = replay_actual(r) {
            meta.input = Some(QUIET_HISTORY.into());
        }
    });
    refused(&replay, |r| {
        if let Actual::Replay { fresh, .. } = replay_actual(r) {
            *fresh = false;
        }
    });

    // A missing narrowing test fails every gate, even with every contract
    // satisfied so that no result needs it.
    let mut no_n = results();
    let tn = Expected::Passes { test: TN }.id().unwrap();
    no_n.results.retain(|record| record.expected != tn);
    let genuine_m = manifest();
    let evaluation = header_acceptance::evaluate(&genuine_m, &required(&genuine_m), &no_n, SUBJECT);
    assert!(!evaluation.accepted());
    assert!(evaluation.gates.values().all(|passes| !passes));

    // A contract exclusion, or a standing Open, with no reason or an empty
    // approval record, blocks instead of excluding.
    for (reason, record) in [("", "a record"), ("a reason", "")] {
        let mut m = manifest();
        m.contracts[0].state = ContractState::Excluded {
            reason,
            approval: Approval::Adam { record },
        };
        assert!(
            blocked(&state(&m, &results(), &Key::passing("t/a"))),
            "{reason:?} {record:?}"
        );
        let mut m = manifest();
        let at = m
            .expected
            .iter()
            .position(|r| matches!(r.expected, Expected::Open { until: None, .. }))
            .unwrap();
        let key = m.expected[at].keys[0].clone();
        if let Expected::Open {
            reason: r,
            approval,
            ..
        } = &mut m.expected[at].expected
        {
            *r = reason;
            *approval = Approval::Adam { record };
        }
        assert!(blocked(&state(&m, &results(), &key)), "{key:?}");
    }

    // An approved StaysNotApplicable reason with an empty approval record,
    // or an empty reason.
    let stays = Key::passing("t/b");
    for (empty_reason, empty_record) in [(false, true), (true, false)] {
        let mut m = manifest();
        let r = results();
        for result in &mut m.expected {
            if let Expected::StaysNotApplicable {
                reason, approval, ..
            } = &mut result.expected
            {
                if empty_reason {
                    *reason = "";
                }
                if empty_record {
                    *approval = Approval::Adam { record: "" };
                }
            }
        }
        assert!(
            blocked(&state(&m, &r, &stays)),
            "{empty_reason} {empty_record}"
        );
    }

    // An empty or unreadable trace names no execution.
    for unusable in ["", "missing"] {
        refused(&catch, |r| {
            let (_, _, draft, companions) = caught_runs(r, CASE);
            draft.trace = unusable.into();
            companions[0].trace = unusable.into();
        });
        refused(&replay, |r| {
            let id = holds_id();
            run_of(r, &id).trace = unusable.into();
            if let Actual::Replay { of, .. } = replay_actual(r) {
                *of = unusable.into();
            }
        });
    }
    // A trace counts only from the expected record at the subject commit,
    // named once: the counted execution under another commit, or repeated
    // under another commit, supplies no trace, and the same execution under
    // a stray identity neither supplies nor disputes one.
    refused(&replay, |r| {
        let id = holds_id();
        record_mut(r, &id).commit = "another commit".into();
    });
    let counted = || {
        results()
            .results
            .into_iter()
            .find(|record| record.expected == holds_id())
            .unwrap()
    };
    refused(&replay, |r| {
        let mut repeated = counted();
        repeated.commit = "another commit".into();
        r.results.push(repeated);
    });
    let mut extra = results();
    let mut stray = counted();
    stray.expected = ExpectedId::Holds {
        property: "t/unlisted".into(),
        run: HISTORY.into(),
    };
    if let Actual::Run(run) = &mut stray.actual {
        run.trace = "another trace".into();
    }
    extra.results.push(stray);
    let manifest = manifest();
    let evaluation = header_acceptance::evaluate(&manifest, &required(&manifest), &extra, SUBJECT);
    assert_eq!(evaluation.keys[&replay], ObligationState::Verified);
    assert!(!evaluation.stray.is_empty());

    // A run that drops a NotApplicable its row lists.
    refused(&Key::passing("t/a"), |r| {
        let id = holds_id();
        run_of(r, &id).reports.remove("t/b");
    });

    // A result for no expected identity, or an expected result with no key,
    // fails every gate.
    let mut unknown = results();
    unknown
        .results
        .push(test_record(sim_test("tests/t.rs", "unlisted")));
    let evaluation =
        header_acceptance::evaluate(&manifest, &required(&manifest), &unknown, SUBJECT);
    assert!(!evaluation.stray.is_empty());
    assert!(evaluation.gates.values().all(|passes| !passes));
    let mut keyless = manifest.clone();
    keyless.expected.push(er(
        Vec::new(),
        &[Gate::G1],
        Expected::Passes {
            test: sim_test("tests/t.rs", "keyless"),
        },
    ));
    let mut with_keyless = results();
    with_keyless
        .results
        .push(test_record(sim_test("tests/t.rs", "keyless")));
    let evaluation =
        header_acceptance::evaluate(&keyless, &required(&keyless), &with_keyless, SUBJECT);
    assert!(!evaluation.stray.is_empty());
    assert!(!evaluation.accepted());
    assert!(evaluation.gates.values().all(|passes| !passes));
}

/// The identity definitions outside `refs.rs` that Rule 25's scan
/// ([`defined_refs`]) allows, as `(file, name, why)`, with the file
/// relative to the crate. None is an identity an acceptance assertion
/// joins on: each predates Rule 25 (adopted 2026-11-12) and keys a
/// model's, harness's or explorer's own state as its facts name it, or
/// names something that isn't a history's identity at all.
const OUTSIDE_REFS: &[(&str, &str, &str)] = &[
    (
        "src/check.rs",
        "FrameRef",
        "defined beside the findings whose subject carries it; refs.rs re-exports it as Rule 25's \
         frame ref",
    ),
    (
        "src/trace/meta.rs",
        "BlobRef",
        "a trace meta's pointer to a stored file, not a history identity",
    ),
    (
        "src/trace/meta.rs",
        "PropertyRef",
        "a trace meta's pointer to a registered property, not a history identity",
    ),
    (
        "src/trace/meta.rs",
        "InputRef",
        "a trace meta's pointer to its input file, not a history identity",
    ),
    (
        "src/trace/meta.rs",
        "PlanRef",
        "a trace meta's pointer to its Plan file, not a history identity",
    ),
    (
        "src/record/machine.rs",
        "CallRef",
        "a machine record's storage call, compared whole by its derive",
    ),
    (
        "src/harness/header_acceptance.rs",
        "TestRef",
        "the manifest's name for a test, which results join whole through ExpectedId",
    ),
    (
        "src/harness/header_acceptance.rs",
        "HistoryRef",
        "the manifest's name for a history, which results join whole through ExpectedId",
    ),
    (
        "src/harness/header_acceptance.rs",
        "CaseRef",
        "the manifest's name for a planted case, which results join whole through ExpectedId",
    ),
    (
        "src/harness/header_node/authority/finding.rs",
        "EngineRunRef",
        "the controlled port's record of an engine run, compared whole by its derive",
    ),
    (
        "src/peer/adapter.rs",
        "StreamKey",
        "the sender adapter's per-stream state, keyed as the transport names a stream",
    ),
    (
        "src/peer/discovery/sender.rs",
        "StreamKey",
        "discovery's sender model's per-stream state, keyed as the transport names a stream",
    ),
    (
        "src/peer/evidence.rs",
        "FrameKey",
        "the wire evidence's reader state, keyed as transport facts name a frame, with no stream \
         kind or version; the pages it hands acceptance are keyed by FrameRef",
    ),
    (
        "src/explore/state/crossing.rs",
        "FrameKey",
        "fuzz-13's crossing join over transport facts, which name no stream kind or version; it \
         hands each crossed update out as its whole FrameRef",
    ),
    (
        "src/explore/state/crossing.rs",
        "SubscriptionKey",
        "fuzz-13's crossing join's candidates, per subscription on a stream",
    ),
    (
        "src/explore/state/collector.rs",
        "SentFrame",
        "the explorer's state collector's peer frames, a coverage feature's key",
    ),
    (
        "src/domain/connection.rs",
        "PeerConn",
        "xsync's connection model, keyed as its observations name a peer's connection",
    ),
    (
        "src/domain/session.rs",
        "SessionRow",
        "the punishment model's session rows, as its observations name them",
    ),
    (
        "src/harness/header_node/authority.rs",
        "Subscription",
        "hdr-9's authority evidence's index of subscriptions as the reactor names them",
    ),
    (
        "src/harness/header_node/retirement.rs",
        "Session",
        "hdr-10's retirement evidence's index of sessions as the reactor names them",
    ),
    (
        "src/harness/header_node/stale.rs",
        "Round",
        "the stale-completion evidence's index of rounds as the reactor names them",
    ),
    (
        "src/harness/header_node/round_life.rs",
        "Authority",
        "hdr-9's round-life index of authority as the reactor names it",
    ),
    (
        "src/harness/header_node/round_life.rs",
        "Subscription",
        "hdr-9's round-life index of rounds by the reactor's subscription",
    ),
    (
        "src/harness/header_node/retirement.rs",
        "SlotKey",
        "hdr-10's retirement report index of slots",
    ),
    (
        "src/domain/block_sync.rs",
        "RequestKey",
        "block sync's model of its requests, outside header sync",
    ),
    (
        "tests/header_node_reservations.rs",
        "StreamKey",
        "a reservation witness's grouping of frames by stream, not an acceptance join",
    ),
    (
        "tests/header_wire.rs",
        "SessionKey",
        "the wire harness's admission test, which compares a Session row to its admission",
    ),
];

/// Whether `ty` is a part of an identity, and if so whether it's a number:
/// an unsigned integer is, and a string, peer ID, stream, hash or byte
/// vector is a part that isn't, through a reference or an `Option`.
/// Anything else isn't a part of an identity.
fn identity_part(ty: &syn::Type) -> Option<bool> {
    match ty {
        syn::Type::Reference(reference) => identity_part(&reference.elem),
        syn::Type::Path(path) => {
            let last = path.path.segments.last()?;
            let argument = || match &last.arguments {
                syn::PathArguments::AngleBracketed(args) => match args.args.first()? {
                    syn::GenericArgument::Type(ty) => Some(ty),
                    _ => None,
                },
                _ => None,
            };
            match last.ident.to_string().as_str() {
                "u8" | "u16" | "u32" | "u64" | "u128" | "usize" => Some(true),
                "Option" => identity_part(argument()?),
                "Vec" => matches!(identity_part(argument()?), Some(true)).then_some(false),
                "String" | "str" | "ZakuraPeerId" | "StreamId" | "Hash" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Each identity definition the Rust source `text`, the file `file`,
/// makes, in order: a struct, enum, union, trait or type alias named
/// `*Ref`, a type alias for an identity tuple a join could key on,
/// two or more [`identity_part`]s, at least one a number, and an
/// implementation of Rule 25's `Ref` trait by any path, as `impl Ref for
/// <type>`, whatever the type is named. It finds them at any depth, in
/// test code and function bodies too. An associated type in an `impl` or
/// trait isn't a definition of its own.
fn defined_refs(text: &str, file: &str) -> Vec<String> {
    use syn::visit::Visit;
    /// Collects the definitions' names.
    #[derive(Default)]
    struct Defined(Vec<String>);
    impl Defined {
        /// Notes `ident` if it's named `*Ref`.
        fn named(&mut self, ident: &syn::Ident) {
            let name = ident.to_string();
            if name.len() > 3 && name.ends_with("Ref") {
                self.0.push(name);
            }
        }
    }
    impl<'ast> Visit<'ast> for Defined {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            self.named(&item.ident);
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            self.named(&item.ident);
            syn::visit::visit_item_enum(self, item);
        }

        fn visit_item_union(&mut self, item: &'ast syn::ItemUnion) {
            self.named(&item.ident);
            syn::visit::visit_item_union(self, item);
        }

        fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
            self.named(&item.ident);
            syn::visit::visit_item_trait(self, item);
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            let implements_ref = item
                .trait_
                .as_ref()
                .and_then(|(_, path, _)| path.segments.last())
                .is_some_and(|last| last.ident == "Ref");
            if implements_ref {
                let ty = match &*item.self_ty {
                    syn::Type::Path(path) => path
                        .path
                        .segments
                        .last()
                        .map_or_else(String::new, |last| last.ident.to_string()),
                    _ => "a type with no path".to_owned(),
                };
                self.0.push(format!("impl Ref for {ty}"));
            }
            syn::visit::visit_item_impl(self, item);
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            let parts: Option<Vec<bool>> = match &*item.ty {
                syn::Type::Tuple(tuple) if tuple.elems.len() >= 2 => {
                    tuple.elems.iter().map(identity_part).collect()
                }
                _ => None,
            };
            match parts {
                Some(parts) if parts.contains(&true) => self.0.push(item.ident.to_string()),
                _ => self.named(&item.ident),
            }
            syn::visit::visit_item_type(self, item);
        }
    }
    let parsed = syn::parse_file(text).unwrap_or_else(|error| panic!("{file} parses: {error}"));
    let mut defined = Defined::default();
    defined.visit_file(&parsed);
    defined.0
}

/// Every identity definition ([`defined_refs`]) in the crate's `src` and
/// `tests` outside `refs.rs`, as `(file, name)` with the file relative to
/// the crate.
fn refs_outside_refs() -> BTreeSet<(String, String)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("the directory reads") {
            let path = entry.expect("an entry reads").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    let krate = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    walk(&krate.join("src"), &mut files);
    walk(&krate.join("tests"), &mut files);
    let mut found = BTreeSet::new();
    for file in files {
        let relative = file
            .strip_prefix(krate)
            .expect("under the crate")
            .display()
            .to_string();
        if relative == "src/harness/header_acceptance/refs.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&file).expect("a source file reads");
        for name in defined_refs(&text, &relative) {
            found.insert((relative.clone(), name));
        }
    }
    found
}

/// Rule 25's source scan: no identity a join could key on is defined
/// outside `refs.rs`, in the crate's `src` or `tests`, apart from the
/// reasoned [`OUTSIDE_REFS`], which must each still be there, and nothing
/// outside `refs.rs` implements `Ref`, however the trait or the type is
/// named. The control shows the scan finds a planted local `*Ref`, at the
/// top level, in a test module and in a function body, a tuple alias, and
/// a `Ref` implementation for a type of any name by the trait's bare,
/// module or crate path, and leaves an associated type, a one-type alias
/// and another trait's implementation alone.
#[test]
fn identity_refs_are_defined_only_in_refs() {
    let allowed: BTreeSet<(String, String)> = OUTSIDE_REFS
        .iter()
        .map(|(file, name, why)| {
            assert!(!why.is_empty(), "{file} {name} has no reason");
            ((*file).to_owned(), (*name).to_owned())
        })
        .collect();
    assert_eq!(
        allowed.len(),
        OUTSIDE_REFS.len(),
        "an entry is listed twice"
    );
    assert_eq!(refs_outside_refs(), allowed);

    let planted = "struct UpdateRef { connection: u64, quic_stream: u64, seq: u64 }\n\
                   type FrameKey = (u64, u64, u64);\n\
                   #[cfg(test)]\n\
                   mod tests {\n\
                       enum SessionRef { One }\n\
                       fn t() { struct LocalRef(u64); type Pair<'a> = (&'a str, u64); }\n\
                   }\n\
                   type Slot = (Vec<u8>, Option<u64>, block::Hash, ZakuraPeerId);\n\
                   impl Subsystem for X { type View = (u64, u32); }\n\
                   type Version = (u64);\n\
                   type Unit = ();\n\
                   type Names = (&'static str, String);\n\
                   type Seed = (&'static str, Scenario, usize);\n\
                   struct Ref;\n\
                   impl Ref for Dispatch {}\n\
                   impl refs::Ref for Paired {}\n\
                   fn u() { impl zakura_sim::harness::header_acceptance::refs::Ref for Release {} }\n\
                   impl Serialize for Release {}\n";
    assert_eq!(
        defined_refs(planted, "t.rs"),
        [
            "UpdateRef",
            "FrameKey",
            "SessionRef",
            "LocalRef",
            "Pair",
            "Slot",
            "impl Ref for Dispatch",
            "impl Ref for Paired",
            "impl Ref for Release",
        ]
    );
}

/// Rule 25's registered one-key-off controls, one per join site: the ref,
/// the function that joins it (`<ref>::joined` or `<ref>::joins`), by its
/// file relative to the crate and its name, and the test, by file and name,
/// whose typed `assert_one_key_off::<ref>` control reaches that function
/// ([`every_ref_has_a_one_key_off_control`]).
const REF_CONTROLS: &[(&str, &str, &str, &str, &str)] = &[
    (
        "FrameRef",
        "src/harness/header_acceptance/companions.rs",
        "rejected_page",
        "src/harness/header_acceptance/companions.rs",
        "the_rejected_page_joins_its_judged_frame",
    ),
    (
        "StreamRef",
        "src/harness/header_acceptance/companions.rs",
        "rejected_page",
        "src/harness/header_acceptance/companions.rs",
        "the_rejected_page_s_session_joins_its_stream",
    ),
    (
        "FrameRef",
        "src/harness/header_acceptance/companions.rs",
        "fault_close",
        "src/harness/header_acceptance/companions.rs",
        "the_fault_close_joins_the_rejected_frames_connection",
    ),
    (
        "SessionRef",
        "src/harness/header_acceptance/companions.rs",
        "fault_close",
        "src/harness/header_acceptance/companions.rs",
        "the_fault_close_joins_the_rejected_frames_connection",
    ),
    (
        "ReadRef",
        "src/harness/header_acceptance/companions.rs",
        "staged_page",
        "src/harness/header_acceptance/companions.rs",
        "the_staged_page_joins_the_rejected_frame",
    ),
    (
        "PageRef",
        "src/harness/header_acceptance/companions.rs",
        "staged_page",
        "src/harness/header_acceptance/companions.rs",
        "the_staged_page_joins_the_rejected_frame",
    ),
    (
        "FindingRef",
        "src/harness/header_acceptance/companions.rs",
        "path_break",
        "src/harness/header_acceptance/companions.rs",
        "the_path_break_names_the_sent_page",
    ),
    (
        "PageRef",
        "src/harness/header_acceptance/companions.rs",
        "path_break",
        "src/harness/header_acceptance/companions.rs",
        "the_path_break_names_the_sent_page",
    ),
    (
        "PeerNameRef",
        "src/harness/header_acceptance/companions.rs",
        "page_peer",
        "src/harness/header_acceptance/companions.rs",
        "the_page_s_peer_joins_its_harness_name",
    ),
    (
        "SessionRef",
        "src/harness/header_acceptance/actions.rs",
        "admitted_on",
        "src/harness/header_acceptance/actions.rs",
        "a_session_end_joins_its_admission_by_its_whole_ref",
    ),
    (
        "LinkRef",
        "src/harness/header_acceptance/actions.rs",
        "ends",
        "src/harness/header_acceptance/actions.rs",
        "a_link_act_joins_its_whole_link",
    ),
    (
        "LinkRef",
        "src/harness/header_acceptance/actions.rs",
        "names_its_link",
        "src/harness/header_acceptance/actions.rs",
        "a_relink_fact_names_its_whole_link",
    ),
    (
        "ControlRef",
        "src/harness/header_acceptance/actions.rs",
        "own_effect",
        "src/harness/header_acceptance/actions.rs",
        "a_control_effect_joins_its_whole_cause",
    ),
    (
        "TriggerRef",
        "src/harness/header_acceptance/tool.rs",
        "witness_judges",
        "src/harness/header_acceptance/tool.rs",
        "a_witness_judges_its_whole_trigger",
    ),
    (
        "FindingSubject",
        "src/witness.rs",
        "joined",
        "src/witness.rs",
        "a_member_at_the_break_s_step_joins_by_its_punishment",
    ),
    (
        "DispatchRef",
        "tests/header_mixed_resources.rs",
        "dispatched",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "CallOpRef",
        "tests/header_mixed_resources.rs",
        "ended",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "StaleCallRef",
        "tests/header_mixed_resources.rs",
        "ended_stale",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "WorkRef",
        "tests/header_mixed_resources.rs",
        "retired_before",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "WorkRef",
        "tests/header_mixed_resources.rs",
        "reserved",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "ReleaseRef",
        "tests/header_mixed_resources.rs",
        "released",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "PeerNameRef",
        "tests/header_mixed_resources.rs",
        "links_of",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "ConnectionRef",
        "src/harness/header_acceptance/companions.rs",
        "fault_close",
        "src/harness/header_acceptance/companions.rs",
        "the_fault_close_joins_the_rejected_frames_connection",
    ),
    (
        "FrameRef",
        "tests/header_wire_crossing.rs",
        "judged_at",
        "tests/header_wire_crossing.rs",
        "an_over_window_crossing_grant_is_bad_cursor_and_closed",
    ),
    (
        "ConnectionRef",
        "tests/header_wire_crossing.rs",
        "closed_on_its_judgment",
        "tests/header_wire_crossing.rs",
        "an_over_window_crossing_grant_is_bad_cursor_and_closed",
    ),
    (
        "ConnectionRef",
        "tests/header_wire_crossing.rs",
        "unpunished",
        "tests/header_wire_crossing.rs",
        "a_crossing_close_is_conformant_and_unpunished",
    ),
    (
        "QuicStreamRef",
        "tests/header_wire_crossing.rs",
        "act_frame",
        "tests/header_wire_crossing.rs",
        "an_act_s_frame_joins_its_quic_stream",
    ),
    (
        "StorageOpRef",
        "tests/header_mixed_resources.rs",
        "reply_crosses_reset",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "StorageRef",
        "tests/header_mixed_resources.rs",
        "reply_crosses_reset",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "StampRef",
        "src/harness/header_acceptance/actions.rs",
        "cause_of",
        "src/harness/header_acceptance/actions.rs",
        "a_fact_joins_its_cause_by_its_whole_stamp",
    ),
    (
        "PickRef",
        "src/harness/header_acceptance/actions.rs",
        "next_declared",
        "src/harness/header_acceptance/actions.rs",
        "a_pick_joins_its_declared_action_whole",
    ),
    (
        "PickRef",
        "src/harness/header_acceptance/actions.rs",
        "is_harness_driven",
        "src/harness/header_acceptance/actions.rs",
        "a_harness_driven_pick_joins_its_whole_identity",
    ),
    (
        "LinkRef",
        "src/harness/header_acceptance/actions.rs",
        "opened",
        "src/harness/header_acceptance/actions.rs",
        "a_redial_joins_its_whole_link",
    ),
    (
        "CallRef",
        "src/harness/header_sync.rs",
        "storage_call_dispatched",
        "src/harness/header_sync.rs",
        "only_a_reactor_dispatch_meets_the_direct_dispatch_trigger",
    ),
    (
        "PeerFrameRef",
        "src/table.rs",
        "sent",
        "tests/generated_triggers.rs",
        "a_judged_frame_joins_its_whole_sent_frame",
    ),
    (
        "ConnectionRef",
        "src/table.rs",
        "judged_reaches",
        "tests/generated_triggers.rs",
        "an_effect_joins_a_frame_judged_on_its_own_connection",
    ),
    (
        "ConnectionRef",
        "src/check.rs",
        "punishment",
        "src/check.rs",
        "a_punishment_names_the_frame_judged_on_its_own_connection",
    ),
    (
        "ConnectionRef",
        "tests/header_wire_prepare_fault.rs",
        "bad_page_closes",
        "tests/header_wire_prepare_fault.rs",
        "a_bad_page_joins_the_close_of_its_own_connection",
    ),
    (
        "StorageOpRef",
        "tests/header_node_reservations.rs",
        "first_prepare_ran",
        "tests/header_node_reservations.rs",
        "a_prepare_joins_its_pick_and_completion_by_its_whole_operation",
    ),
    (
        "StorageOpRef",
        "tests/header_node_reservations.rs",
        "runs_prepare",
        "tests/header_node_reservations.rs",
        "a_prepare_joins_its_pick_and_completion_by_its_whole_operation",
    ),
    (
        "SessionRef",
        "tests/header_mixed_resources.rs",
        "admitted",
        "tests/header_mixed_resources.rs",
        "the_mixed_joins_fail_one_identity_off",
    ),
    (
        "SessionIdRef",
        "tests/header_node_session_ends.rs",
        "earlier_end",
        "tests/header_node_session_ends.rs",
        "a_view_joins_its_earlier_life_by_its_session",
    ),
    (
        "SessionIdRef",
        "tests/header_node_session_ends.rs",
        "first_shown",
        "tests/header_node_session_ends.rs",
        "a_view_joins_its_earlier_life_by_its_session",
    ),
    (
        "ConnectionRef",
        "tests/header_node_session_ends.rs",
        "same_ends",
        "tests/header_node_session_ends.rs",
        "an_end_joins_its_cancellation_by_its_connection",
    ),
    (
        "FrameRef",
        "tests/header_wire_crossing.rs",
        "act_wrote",
        "tests/header_wire_crossing.rs",
        "a_crossed_update_joins_the_act_s_frame",
    ),
    (
        "SourceFrameRef",
        "src/table.rs",
        "effects_follow_trigger",
        "tests/generated_triggers.rs",
        "a_work_joins_its_whole_source_frame",
    ),
    (
        "ExchangeRef",
        "src/table.rs",
        "effects_follow_trigger",
        "tests/generated_triggers.rs",
        "a_serving_effect_joins_its_whole_exchange",
    ),
    (
        "ConnectionRef",
        "src/table.rs",
        "judged",
        "tests/generated_triggers.rs",
        "a_punishment_counts_the_frames_judged_on_its_connection",
    ),
    (
        "ConnectionRef",
        "src/table.rs",
        "closed",
        "tests/table_properties.rs",
        "a_fault_after_its_own_connection_closed_owes_nothing",
    ),
    (
        "CallRef",
        "src/harness/header_node/fault.rs",
        "reactor",
        "src/harness/header_node/fault.rs",
        "a_dropped_call_takes_only_the_reply_it_held",
    ),
    (
        "CallRef",
        "src/harness/header_node/fault.rs",
        "dropped_in_stop",
        "src/harness/header_node/fault.rs",
        "a_stop_clears_only_the_dropped_call_it_names",
    ),
    (
        "StampRef",
        "src/harness/header_node/adoption.rs",
        "watch_read",
        "src/harness/header_node/adoption.rs",
        "a_break_joins_the_stamp_of_the_event_that_made_it",
    ),
    (
        "StampRef",
        "tests/header_node_ends.rs",
        "cause_of",
        "tests/header_node_ends.rs",
        "a_fact_takes_the_cause_of_its_whole_stamp",
    ),
    (
        "CallRef",
        "tests/header_node_ends.rs",
        "recorded_as_logged",
        "tests/header_node_ends.rs",
        "a_recorded_drop_joins_the_logged_call",
    ),
    (
        "SessionIdRef",
        "tests/header_wire_prepare_fault.rs",
        "penalized_late",
        "tests/header_wire_prepare_fault.rs",
        "a_late_penalty_joins_its_origin_session",
    ),
    (
        "ControlRef",
        "src/harness/header_acceptance/actions.rs",
        "grew",
        "src/harness/header_acceptance/actions.rs",
        "a_grow_joins_its_whole_step",
    ),
];

/// What [`join_scan`] finds in one Rust source file.
#[derive(Default, Debug, Clone)]
struct JoinScan {
    /// Each function's name, with every name its body calls or names: a
    /// call's or path's last segment and a method's name, at any depth,
    /// macro arguments included. A `let` closure counts as a function of
    /// its own, named `<function>::<binding>` after the function it's in.
    names: BTreeMap<String, BTreeSet<String>>,
    /// Each typed join, as `(ref, function)`: a call of `<ref>::joined` or
    /// `<ref>::joins` inside that named function.
    joins: BTreeSet<(String, String)>,
    /// The function of each untyped join: a method call of `.joined` or
    /// `.joins`, whose ref the scan can't see.
    untyped: BTreeSet<String>,
    /// Each typed control, as `(test, ref, names)`: a call of
    /// `assert_one_key_off::<ref>` in a running test, with the names its
    /// closure argument calls or names.
    controls: Vec<(String, String, BTreeSet<String>)>,
}

/// The expressions a macro call's arguments hold, if `syn` parses them: a
/// comma-separated list, or `matches!`'s expression, pattern and guard.
fn macro_exprs(mac: &syn::Macro) -> Vec<syn::Expr> {
    use syn::parse::Parser;
    if mac.path.is_ident("matches") {
        let parse = |input: syn::parse::ParseStream<'_>| {
            let scrutinee: syn::Expr = input.parse()?;
            input.parse::<syn::Token![,]>()?;
            syn::Pat::parse_multi_with_leading_vert(input)?;
            let mut exprs = vec![scrutinee];
            if input.parse::<Option<syn::Token![if]>>()?.is_some() {
                exprs.push(input.parse()?);
            }
            input.parse::<Option<syn::Token![,]>>()?;
            Ok(exprs)
        };
        return parse.parse2(mac.tokens.clone()).unwrap_or_default();
    }
    syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated
        .parse2(mac.tokens.clone())
        .map(|exprs| exprs.into_iter().collect())
        .unwrap_or_default()
}

/// Every name `expr` calls or names ([`JoinScan::names`]).
fn names_in(expr: &syn::Expr) -> BTreeSet<String> {
    use syn::visit::Visit;
    /// Collects the names.
    #[derive(Default)]
    struct Names(BTreeSet<String>);
    impl<'ast> Visit<'ast> for Names {
        fn visit_path(&mut self, path: &'ast syn::Path) {
            if let Some(last) = path.segments.last() {
                self.0.insert(last.ident.to_string());
            }
            syn::visit::visit_path(self, path);
        }

        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            self.0.insert(call.method.to_string());
            syn::visit::visit_expr_method_call(self, call);
        }

        fn visit_macro(&mut self, mac: &'ast syn::Macro) {
            for expr in macro_exprs(mac) {
                self.visit_expr(&expr);
            }
        }
    }
    let mut names = Names::default();
    names.visit_expr(expr);
    names.0
}

/// What the Rust source `text`, the file `file`, holds of Rule 25's joins
/// and controls ([`JoinScan`]). Code compiled out by a `cfg` other than
/// `test` or `zakura_sim` is skipped, and a control counts only inside a
/// `#[test]` function that isn't `#[ignore]`d.
fn join_scan(text: &str, file: &str) -> JoinScan {
    use syn::visit::Visit;
    /// Whether `attrs` compile their item out of a test run: a `cfg` that
    /// isn't `test` or `zakura_sim`.
    fn compiled_out(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attr| {
            attr.path().is_ident("cfg")
                && attr.meta.require_list().is_ok_and(|list| {
                    let tokens = list.tokens.to_string();
                    tokens != "test" && tokens != "zakura_sim"
                })
        })
    }
    /// Walks the file, by enclosing function and running test.
    #[derive(Default)]
    struct Walk {
        functions: Vec<String>,
        scopes: Vec<String>,
        test: Option<String>,
        scan: JoinScan,
    }
    impl Walk {
        /// Notes `name` as named by every enclosing scope.
        fn name(&mut self, name: String) {
            for scope in &self.scopes {
                self.scan
                    .names
                    .entry(scope.clone())
                    .or_default()
                    .insert(name.clone());
            }
        }

        /// Walks a function named `name` with `attrs`, by `walk`.
        fn function(
            &mut self,
            name: String,
            attrs: &[syn::Attribute],
            walk: impl FnOnce(&mut Self),
        ) {
            if compiled_out(attrs) {
                return;
            }
            let runs = attrs.iter().any(|attr| attr.path().is_ident("test"))
                && !attrs.iter().any(|attr| attr.path().is_ident("ignore"));
            let outer = self.test.clone();
            if runs && outer.is_none() {
                self.test = Some(name.clone());
            }
            self.scan.names.entry(name.clone()).or_default();
            self.functions.push(name.clone());
            self.scopes.push(name);
            walk(self);
            self.scopes.pop();
            self.functions.pop();
            self.test = outer;
        }
    }
    impl<'ast> Visit<'ast> for Walk {
        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !compiled_out(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }

        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            self.function(item.sig.ident.to_string(), &item.attrs, |walk| {
                syn::visit::visit_item_fn(walk, item)
            });
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            self.function(item.sig.ident.to_string(), &item.attrs, |walk| {
                syn::visit::visit_impl_item_fn(walk, item)
            });
        }

        fn visit_local(&mut self, local: &'ast syn::Local) {
            let closure = local
                .init
                .as_ref()
                .is_some_and(|init| matches!(*init.expr, syn::Expr::Closure(_)));
            match &local.pat {
                syn::Pat::Ident(binding) if closure => {
                    let within = self.functions.last().cloned().unwrap_or_default();
                    let name = format!("{within}::{}", binding.ident);
                    self.scan.names.entry(name.clone()).or_default();
                    self.scopes.push(name);
                    syn::visit::visit_local(self, local);
                    self.scopes.pop();
                }
                _ => syn::visit::visit_local(self, local),
            }
        }

        fn visit_path(&mut self, path: &'ast syn::Path) {
            if let Some(last) = path.segments.last() {
                self.name(last.ident.to_string());
            }
            syn::visit::visit_path(self, path);
        }

        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            self.name(call.method.to_string());
            if call.method == "joined" || call.method == "joins" {
                let within = self.functions.last().cloned().unwrap_or_default();
                self.scan.untyped.insert(within);
            }
            syn::visit::visit_expr_method_call(self, call);
        }

        fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
            if let syn::Expr::Path(path) = &*call.func {
                let segments: Vec<&syn::PathSegment> = path.path.segments.iter().collect();
                if let [.., owner, last] = segments[..] {
                    if last.ident == "joined" || last.ident == "joins" {
                        let within = self.functions.last().cloned().unwrap_or_default();
                        self.scan.joins.insert((owner.ident.to_string(), within));
                    }
                }
                let typed = segments
                    .last()
                    .filter(|last| last.ident == "assert_one_key_off")
                    .and_then(|last| match &last.arguments {
                        syn::PathArguments::AngleBracketed(args) => args.args.first(),
                        _ => None,
                    })
                    .and_then(|arg| match arg {
                        syn::GenericArgument::Type(syn::Type::Path(ty)) => ty.path.segments.last(),
                        _ => None,
                    });
                if let (Some(test), Some(ty), Some(closure)) =
                    (&self.test, typed, call.args.iter().nth(1))
                {
                    self.scan.controls.push((
                        test.clone(),
                        ty.ident.to_string(),
                        names_in(closure),
                    ));
                }
            }
            syn::visit::visit_expr_call(self, call);
        }

        fn visit_macro(&mut self, mac: &'ast syn::Macro) {
            for expr in macro_exprs(mac) {
                self.visit_expr(&expr);
            }
        }
    }
    let parsed = syn::parse_file(text).unwrap_or_else(|error| panic!("{file} parses: {error}"));
    let mut walk = Walk::default();
    walk.visit_file(&parsed);
    walk.scan
}

/// Whether some name in `from`, named inside the function `within`,
/// reaches the function `to` through `names`, each function's names
/// ([`JoinScan::names`]): by naming it, or by naming a function that does.
/// A name resolves to a `let` closure of the function it's named in, if it
/// has one, but inside that closure itself, and otherwise to every
/// function of that name, so two functions
/// of one name are one and this over-approximates. A call through a trait
/// object or a function the scan can't see doesn't reach.
fn reaches(
    names: &BTreeMap<String, BTreeSet<String>>,
    (within, from): (&str, &BTreeSet<String>),
    to: &str,
) -> bool {
    let resolve = |owner: &str, name: &str| {
        let local = format!("{owner}::{name}");
        if names.contains_key(&local) {
            local
        } else {
            name.to_owned()
        }
    };
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut next: Vec<String> = from.iter().map(|name| resolve(within, name)).collect();
    while let Some(node) = next.pop() {
        if node == to {
            return true;
        }
        if seen.insert(node.clone()) {
            // A closure can't name itself, so its own binding inside it is
            // the item the binding shadows.
            let (owner, own) = node.split_once("::").unwrap_or((&node, ""));
            let owner = owner.to_owned();
            let own = own.to_owned();
            next.extend(
                names
                    .get(&node)
                    .into_iter()
                    .flatten()
                    .map(|name| match name == &own {
                        true => name.clone(),
                        false => resolve(&owner, name),
                    }),
            );
        }
    }
    false
}

/// The types `refs.rs`, as `text`, implements `Ref` for ([`defined_refs`]).
fn ref_impls(text: &str) -> BTreeSet<String> {
    defined_refs(text, "refs.rs")
        .into_iter()
        .filter_map(|found| found.strip_prefix("impl Ref for ").map(str::to_owned))
        .collect()
}

/// Every Rust source file of the crate's `src` and `tests`, relative to the
/// crate, with its text, but `refs.rs`, which defines the join, and the
/// one-key-off template.
fn crate_sources() -> BTreeMap<String, String> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("the directory reads") {
            let path = entry.expect("an entry reads").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    let krate = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    walk(&krate.join("src"), &mut files);
    walk(&krate.join("tests"), &mut files);
    files
        .into_iter()
        .map(|file| {
            let relative = file
                .strip_prefix(krate)
                .expect("under the crate")
                .display()
                .to_string();
            let text = std::fs::read_to_string(&file).expect("a source file reads");
            (relative, text)
        })
        .filter(|(file, _)| {
            file != "src/harness/header_acceptance/refs.rs"
                && file != "tests/support/one_key_off.rs"
        })
        .collect()
}

/// Why Rule 25's controls don't cover the joins `scans` found, file by
/// file ([`join_scan`]), each finding one
/// line: a typed join no [`REF_CONTROLS`] entry registers, an entry naming
/// a join that isn't there, an untyped join, a `Ref` type with no
/// registered join, and an entry whose test has no typed control of its
/// ref whose closure reaches its join function ([`reaches`], through the
/// join's and the test's files).
fn control_gaps(
    scans: &BTreeMap<String, JoinScan>,
    refs: &str,
    registry: &[(&str, &str, &str, &str, &str)],
) -> Vec<String> {
    let mut gaps = Vec::new();
    let found: BTreeSet<(String, String, String)> = scans
        .iter()
        .flat_map(|(file, scan)| {
            scan.joins
                .iter()
                .map(|(ty, within)| (ty.clone(), file.clone(), within.clone()))
        })
        .collect();
    let registered: BTreeSet<(String, String, String)> = registry
        .iter()
        .map(|(ty, file, within, ..)| ((*ty).to_owned(), (*file).to_owned(), (*within).to_owned()))
        .collect();
    if registered.len() != registry.len() {
        gaps.push("a join is registered twice".to_owned());
    }
    for (ty, file, within) in found.difference(&registered) {
        gaps.push(format!(
            "{file} {within} joins {ty} with no registered control"
        ));
    }
    for (ty, file, within) in registered.difference(&found) {
        gaps.push(format!(
            "{file} {within} doesn't join {ty}, as it's registered to"
        ));
    }
    for (file, scan) in scans {
        for within in &scan.untyped {
            gaps.push(format!(
                "{file} {within} joins by a method call, not by its ref's type"
            ));
        }
    }
    let joined: BTreeSet<String> = registered.iter().map(|(ty, ..)| ty.clone()).collect();
    for ty in ref_impls(refs).difference(&joined) {
        gaps.push(format!("{ty} implements Ref but nothing joins it"));
    }
    for (ty, file, within, test_file, test) in registry {
        let (Some(at), Some(tested)) = (scans.get(*file), scans.get(*test_file)) else {
            gaps.push(format!("{file} or {test_file} isn't a source"));
            continue;
        };
        let mut names = at.names.clone();
        for (name, called) in &tested.names {
            names
                .entry(name.clone())
                .or_default()
                .extend(called.iter().cloned());
        }
        let covered = tested.controls.iter().any(|(control, of, closure)| {
            control == test && of == ty && reaches(&names, (test, closure), within)
        });
        if !covered {
            gaps.push(format!(
                "{test_file} {test} has no typed {ty} control reaching {file} {within}"
            ));
        }
    }
    gaps
}

/// Rule 25's control check: every typed join in the crate's `src` and
/// `tests` (`<ref>::joined` or `<ref>::joins`) is registered in
/// [`REF_CONTROLS`] by its function, nothing else is, no join is untyped,
/// every `Ref` has a join, and each registered test runs a typed
/// one-key-off control of its ref whose closure reaches the join's
/// function ([`control_gaps`]). The controls show each fails it: the real
/// trigger control untyped, renamed, `#[ignore]`d, not a test or compiled
/// out; its closure no longer reaching the join; a template test that
/// joins nothing registered in place of the real one; an unregistered and
/// an untyped planted join; and a planted `Ref` nothing joins.
#[test]
fn every_ref_has_a_one_key_off_control() {
    let sources = crate_sources();
    let scans: BTreeMap<String, JoinScan> = sources
        .iter()
        .map(|(file, text)| (file.clone(), join_scan(text, file)))
        .collect();
    let krate = Path::new(env!("CARGO_MANIFEST_DIR"));
    let refs = std::fs::read_to_string(krate.join("src/harness/header_acceptance/refs.rs"))
        .expect("refs.rs reads");
    assert_eq!(
        control_gaps(&scans, &refs, REF_CONTROLS),
        Vec::<String>::new()
    );

    let tool = "src/harness/header_acceptance/tool.rs";
    let gap = "src/harness/header_acceptance/tool.rs a_witness_judges_its_whole_trigger has no \
               typed TriggerRef control reaching src/harness/header_acceptance/tool.rs \
               witness_judges";
    let typed = "assert_one_key_off::<TriggerRef>(&trigger, |want| {\n            witness_judges(";
    assert_eq!(sources[tool].matches(typed).count(), 1);
    let test = "    #[test]\n    fn a_witness_judges_its_whole_trigger";
    for broken in [
        sources[tool].replace(
            typed,
            "assert_one_key_off(&trigger, |want| {\n            witness_judges(",
        ),
        sources[tool].replace(
            typed,
            "assert_one_key_off::<TriggerRef>(&trigger, |want| {\n            bound_witness(",
        ),
        sources[tool].replace(
            test,
            "    #[test]\n    #[ignore]\n    fn a_witness_judges_its_whole_trigger",
        ),
        sources[tool].replace(test, "    fn a_witness_judges_its_whole_trigger"),
        sources[tool].replace(
            test,
            "    #[test]\n    #[cfg(any())]\n    fn a_witness_judges_its_whole_trigger",
        ),
        sources[tool].replace(
            "fn a_witness_judges_its_whole_trigger",
            "fn a_witness_judges_its_whole_trigger_gone",
        ),
    ] {
        assert_ne!(broken, sources[tool]);
        let mut scans = scans.clone();
        scans.insert(tool.to_owned(), join_scan(&broken, tool));
        assert_eq!(control_gaps(&scans, &refs, REF_CONTROLS), [gap]);
    }

    // A template control, typed but comparing a frame with itself, in
    // place of the rejected page's real frame control (round 8's gap).
    let companions = "src/harness/header_acceptance/companions.rs";
    let templated: Vec<_> = REF_CONTROLS
        .iter()
        .map(|entry| match entry {
            ("FrameRef", file, "rejected_page", ..) => (
                "FrameRef",
                *file,
                "rejected_page",
                companions,
                "a_template_control",
            ),
            other => *other,
        })
        .collect();
    let mut planted = scans.clone();
    planted.insert(
        companions.to_owned(),
        join_scan(
            &format!(
                "{}\n#[test]\nfn a_template_control() {{ let frame = frame(); \
             assert_one_key_off::<FrameRef>(&frame, |want| *want == frame); }}\n",
                sources[companions]
            ),
            companions,
        ),
    );
    assert_eq!(
        control_gaps(&planted, &refs, &templated),
        [format!(
            "{companions} a_template_control has no typed FrameRef control reaching \
             {companions} rejected_page"
        )]
    );

    for (planted, gap) in [
        (
            "fn planted(frames: &[FrameRef], want: &FrameRef) -> usize { FrameRef::joined(want, frames, |f| Some(f.clone())).len() }",
            "src/harness/header_acceptance/tool.rs planted joins FrameRef with no registered control",
        ),
        (
            "fn planted(frames: &[FrameRef], want: &FrameRef) -> usize { want.joined(frames, |f| Some(f.clone())).len() }",
            "src/harness/header_acceptance/tool.rs planted joins by a method call, not by its ref's type",
        ),
    ] {
        let mut scans = scans.clone();
        let text = format!("{}\n{planted}\n", sources[tool]);
        scans.insert(tool.to_owned(), join_scan(&text, tool));
        assert_eq!(control_gaps(&scans, &refs, REF_CONTROLS), [gap]);
    }
    // The redial's control calling only the `Ended` branch's join, as the
    // one registered for the old whole `show` did (round 9's gap): each
    // branch is its own function, so it no longer reaches `opened`.
    let actions = "src/harness/header_acceptance/actions.rs";
    let opened = "assert_one_key_off::<LinkRef>(&redialed, |link| sessions.opened(link));";
    assert_eq!(sources[actions].matches(opened).count(), 1);
    let ended_only = sources[actions].replace(
        opened,
        "assert_one_key_off::<LinkRef>(&redialed, |link| sessions.ends(link, PEER_CLOSED) > 0);",
    );
    let mut redial = scans.clone();
    redial.insert(actions.to_owned(), join_scan(&ended_only, actions));
    assert_eq!(
        control_gaps(&redial, &refs, REF_CONTROLS),
        [format!(
            "{actions} a_redial_joins_its_whole_link has no typed LinkRef control reaching \
             {actions} opened"
        )]
    );
    let unjoined = format!("{refs}\npub struct NewRef;\nimpl Ref for NewRef {{}}\n");
    assert_eq!(
        control_gaps(&scans, &unjoined, REF_CONTROLS),
        ["NewRef implements Ref but nothing joins it"]
    );
}

/// The acceptance tool's source directory, relative to the crate.
const ACCEPTANCE_SRC: &str = "src/harness/header_acceptance";

/// Every read in the Rust source `text` outside test code, as the name of
/// the function it's in, in order: a call of `fs::read`,
/// `fs::read_to_string`, `fs::read_dir`, `File::open`, `read_bounded`,
/// `TraceReader::open` or `open_prefix`, `serde_json::from_reader` or a
/// `read_json` helper, however its path is spelled, other than the binding
/// module's own (`binding::read_bounded`), and a method call of
/// `read_dir`, `read_to_string` or `read_to_end`. A `use` that imports one
/// of those names, or renames one, would hide a call, so it fails the scan.
fn artifact_reads(text: &str, file: &str) -> Vec<String> {
    use syn::visit::Visit;
    /// The last segment of each read path, with the segment before it if
    /// that names its owner.
    const READS: &[(&str, &str)] = &[
        ("fs", "read"),
        ("fs", "read_to_string"),
        ("fs", "read_dir"),
        ("File", "open"),
        ("", "read_bounded"),
        ("TraceReader", "open"),
        ("TraceReader", "open_prefix"),
        ("serde_json", "from_reader"),
        ("", "read_json"),
    ];
    /// Whether `names`, a path's segments, call one of [`READS`] other than
    /// through `binding`.
    fn reads(names: &[String]) -> bool {
        if matches!(names, [.., module, _] if module == "binding") {
            return false;
        }
        READS.iter().any(|(owner, last)| match names {
            [.., before, at] => at == last && (owner.is_empty() || before == owner),
            [at] => at == last && owner.is_empty(),
            [] => false,
        })
    }
    /// Collects the reads, by enclosing function.
    struct Reads<'a> {
        file: &'a str,
        within: Vec<String>,
        found: Vec<String>,
    }
    /// Whether `attrs` mark test-only code: `#[test]`, or `#[cfg(test)]`.
    fn test_only(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attr| {
            attr.path().is_ident("test")
                || (attr.path().is_ident("cfg")
                    && attr
                        .meta
                        .require_list()
                        .is_ok_and(|list| list.tokens.to_string() == "test"))
        })
    }
    /// Fails on a `use` tree importing a read by name.
    fn check_use(tree: &syn::UseTree, path: &mut Vec<String>, file: &str) {
        match tree {
            syn::UseTree::Path(at) => {
                path.push(at.ident.to_string());
                check_use(&at.tree, path, file);
                path.pop();
            }
            syn::UseTree::Name(syn::UseName { ident: name })
            | syn::UseTree::Rename(syn::UseRename { ident: name, .. })
                if {
                    let mut names = path.clone();
                    names.push(name.to_string());
                    reads(&names)
                } =>
            {
                panic!("{file}: a `use` imports a read by name, which the scan can't see")
            }
            syn::UseTree::Rename(rename)
                if READS
                    .iter()
                    .any(|(owner, _)| !owner.is_empty() && rename.ident == owner) =>
            {
                panic!("{file}: a `use` renames a read's owner, which the scan can't see")
            }
            syn::UseTree::Group(group) => group
                .items
                .iter()
                .for_each(|tree| check_use(tree, path, file)),
            _ => {}
        }
    }
    impl Reads<'_> {
        /// Notes a read at `path` inside the current function.
        fn path(&mut self, path: &syn::Path) {
            let names: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
            if reads(&names) {
                self.note();
            }
        }

        /// Notes a read inside the current function.
        fn note(&mut self) {
            let within = self.within.last().cloned().unwrap_or_default();
            self.found.push(within);
        }
    }
    impl<'ast> Visit<'ast> for Reads<'_> {
        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !test_only(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }

        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if !test_only(&item.attrs) {
                self.within.push(item.sig.ident.to_string());
                syn::visit::visit_item_fn(self, item);
                self.within.pop();
            }
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !test_only(&item.attrs) {
                self.within.push(item.sig.ident.to_string());
                syn::visit::visit_impl_item_fn(self, item);
                self.within.pop();
            }
        }

        fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
            check_use(&item.tree, &mut Vec::new(), self.file);
        }

        fn visit_expr_path(&mut self, expr: &'ast syn::ExprPath) {
            self.path(&expr.path);
            syn::visit::visit_expr_path(self, expr);
        }

        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            if ["read_dir", "read_to_string", "read_to_end"]
                .iter()
                .any(|name| call.method == name)
            {
                self.note();
            }
            syn::visit::visit_expr_method_call(self, call);
        }
    }
    let parsed = syn::parse_file(text).unwrap_or_else(|error| panic!("{file} parses: {error}"));
    let mut reads = Reads {
        file,
        within: Vec::new(),
        found: Vec::new(),
    };
    reads.visit_file(&parsed);
    reads.found
}

/// The files [`every_artifact_read_goes_through_binding`] reads, relative
/// to the crate: the acceptance tool's parent module, every Rust file
/// under [`ACCEPTANCE_SRC`], at any depth, but `binding.rs`, which is the
/// boundary, and the `header-acceptance` command, which reads the plan and
/// the trace it judges companions on.
fn artifact_read_scope() -> Vec<String> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("the acceptance source reads") {
            let path = entry.expect("an entry reads").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    let krate = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut paths = vec![
        krate.join(format!("{ACCEPTANCE_SRC}.rs")),
        krate.join("src/bin/header-acceptance.rs"),
    ];
    walk(&krate.join(ACCEPTANCE_SRC), &mut paths);
    let mut files: Vec<String> = paths
        .iter()
        .map(|path| {
            path.strip_prefix(krate)
                .expect("under the crate")
                .display()
                .to_string()
        })
        .filter(|file| *file != format!("{ACCEPTANCE_SRC}/binding.rs"))
        .collect();
    files.sort();
    files
}

/// Rule 25's enforcement: in [`artifact_read_scope`], every read goes
/// through `binding.rs`, a run artifact by its kind and S's committed files
/// by `binding::committed_text` and its siblings, so no reader binds an
/// artifact itself or reads one unbound, and there's no exemption to keep
/// honest. A planted bypass, however its read is spelled, fails the scan.
#[test]
fn every_artifact_read_goes_through_binding() {
    let krate = Path::new(env!("CARGO_MANIFEST_DIR"));
    let scope = artifact_read_scope();
    for file in [
        "src/harness/header_acceptance.rs",
        "src/harness/header_acceptance/tool.rs",
        "src/bin/header-acceptance.rs",
    ] {
        assert!(scope.iter().any(|at| at == file), "{file} is scanned");
    }
    let mut found: BTreeSet<(String, String)> = BTreeSet::new();
    let mut texts = BTreeMap::new();
    for file in scope {
        let text = std::fs::read_to_string(krate.join(&file)).expect("a source file reads");
        for within in artifact_reads(&text, &file) {
            found.insert((file.clone(), within));
        }
        texts.insert(file, text);
    }
    assert!(
        found.is_empty(),
        "reads outside binding.rs, which must read through it: {found:?}"
    );

    let tool = &texts["src/harness/header_acceptance/tool.rs"];
    for bypass in [
        "fn planted(dir: &Path) { let _ = std::fs::read_to_string(dir.join(\"replay.json\")); }",
        "fn planted(dir: &Path) { let _ = fs::read(dir); }",
        "fn planted(dir: &Path) { let _ = read_json::<Value>(dir); }",
        "fn planted(dir: &Path) { let _ = crate::trace::TraceReader::open(dir); }",
        "struct P; impl P { fn planted(dir: &Path) { let _ = std::fs::File::open(dir); } }",
        "fn planted(dir: &Path) { let _ = dir.read_dir(); }",
    ] {
        let planted = format!("{tool}\n{bypass}\n");
        assert!(
            artifact_reads(&planted, "tool.rs").contains(&"planted".to_owned()),
            "{bypass}"
        );
    }
    let in_test =
        format!("{tool}\n#[cfg(test)]\nfn planted() {{ let _ = std::fs::read(\"x\"); }}\n");
    assert!(!artifact_reads(&in_test, "tool.rs").contains(&"planted".to_owned()));
    for hidden in [
        "use std::fs::read_to_string;",
        "use std::fs::{read as slurp};",
        "use std::fs as files;",
    ] {
        let planted = format!("{tool}\n{hidden}\n");
        assert!(
            std::panic::catch_unwind(|| artifact_reads(&planted, "tool.rs")).is_err(),
            "{hidden}"
        );
    }
}

/// The words a name of an identity holds, as one of its `_`-separated
/// parts: a peer, connection, session, subscription, stream, frame or
/// page, an operation, call or sequence number, a storage instance, a
/// peer's incarnation, a link, a version, an ID, a serialized attribution,
/// a store's stamp for a fact, or a trace's pick.
const IDENTITY_WORDS: &[&str] = &[
    "peer",
    "conn",
    "connection",
    "session",
    "subscription",
    "stream",
    "frame",
    "page",
    "op",
    "call",
    "seq",
    "instance",
    "incarnation",
    "link",
    "version",
    "id",
    "attribution",
    "stamp",
    "pick",
];

/// The comparisons [`raw_joins`] finds that aren't identity joins, as
/// `(file, function, names, why)`: the comparison's identity names, sorted
/// and space-separated, and why it compares no identity.
const NOT_RAW_JOINS: &[(&str, &str, &str, &str)] = &[
    (
        "src/domain/header_sync.rs",
        "commit_only_authorized_reports_the_overgrant_update_and_no_page",
        "attribution",
        "asserts the claims store's whole serialized attribution against the finding's own, and its subject's kind; the finding's join is the witness judge's `FindingSubject`",
    ),
    (
        "src/harness/header_acceptance/actions.rs",
        "a_link_act_joins_its_whole_link",
        "PEER_CLOSED link",
        "counts the link's ends for one reason, after the control of `ends`' join",
    ),
    (
        "src/harness/header_acceptance/actions.rs",
        "a_pick_joins_its_declared_action_whole",
        "peer pick_ref",
        "checks the pick parser's output for a fixed pick string, not a join",
    ),
    (
        "src/harness/header_acceptance/actions.rs",
        "a_pick_joins_its_declared_action_whole",
        "pick_ref",
        "checks the pick parser refuses a pick with no action, not a join",
    ),
    (
        "src/harness/header_acceptance/actions.rs",
        "harness_driven",
        "pick",
        "finds the `Recover` steps by their action's name, a kind; the driven picks it builds join by `PickRef`",
    ),
    (
        "src/harness/header_acceptance/actions.rs",
        "of",
        "link",
        "builds each peer's link list, skipping a link it already holds; `show` joins it by `LinkRef`",
    ),
    (
        "src/harness/header_acceptance/companions.rs",
        "attributed_page",
        "attribution",
        "filters a record's rule and subject kind; the page it names joins by `PageRef`",
    ),
    (
        "src/harness/header_acceptance/companions.rs",
        "the_fault_close_joins_the_rejected_frames_connection",
        "connection frame page peer quic_stream seq session subscription version",
        "checks the rejected page read from the fixture is the one the test built; `fault_close`'s joins have their own controls",
    ),
    (
        "src/harness/header_acceptance/companions.rs",
        "the_fault_close_joins_the_rejected_frames_connection",
        "page",
        "asserts the fault close judge's result, `Ok`, for the fixture's page; the join is inside the judge, under its control",
    ),
    (
        "src/harness/header_acceptance/companions.rs",
        "the_path_break_names_the_sent_page",
        "page",
        "asserts the path break judge's result, `Ok`, for the fixture's page; the join is inside the judge, under its control",
    ),
    (
        "src/harness/header_acceptance/companions.rs",
        "the_sent_page_comes_from_the_frames_evidence",
        "page sent_page",
        "asserts the sent page the judge finds for the fixture; the join is inside the judge, under its control",
    ),
    (
        "src/harness/header_acceptance/companions.rs",
        "the_staged_page_joins_the_rejected_frame",
        "page staged_page",
        "asserts the staged page judge's result, `Ok`, for the fixture's page; the join is inside the judge, under its control",
    ),
    (
        "src/harness/header_acceptance/companions.rs",
        "the_trace_judge_judges_the_recorded_draft_run",
        "id",
        "checks the trace judge returns the digest of the trace it read (round 9's C2-6), not a join of records",
    ),
    (
        "src/harness/header_acceptance/evaluate.rs",
        "caught_draft",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "src/harness/header_acceptance/history.rs",
        "declarations_read_back_in_order",
        "run_id",
        "checks a declaration reads back the run, task and role the test wrote, not a join",
    ),
    (
        "src/harness/header_acceptance/not_applicable.rs",
        "lists",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "src/harness/header_acceptance/not_applicable.rs",
        "history_expected",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "src/harness/header_acceptance/rows.rs",
        "hand_written_rows",
        "id",
        "a property ID choosing its row's watched set",
    ),
    (
        "src/harness/header_acceptance/tool.rs",
        "a_a_and_a_b_a",
        "id",
        "trace digests, compared to show two recordings are one run, not joined",
    ),
    (
        "src/harness/header_acceptance/tool.rs",
        "a_run_s_actions_follow_its_harness",
        "pick",
        "removes the recording's last relink pick, the test's planted change, not a judgment",
    ),
    (
        "src/harness/header_acceptance/tool.rs",
        "a_typed_case_is_planned_beside_a_fuzz_case",
        "id",
        "expected-result IDs, the manifest's keys, not run evidence",
    ),
    (
        "src/harness/header_acceptance/tool.rs",
        "draft_witness",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/evidence_inventory.rs",
        "c14_is_proved_by_every_registered_store_s_producer",
        "id",
        "contract IDs, the manifest's names for contracts",
    ),
    (
        "tests/header_acceptance.rs",
        "visit_expr_method_call",
        "call",
        "compares a parsed method call's name, Rust syntax, not run evidence",
    ),
    (
        "tests/header_acceptance_narrowing.rs",
        "n1_peerless_and_machine_results_read_no_peer_evidence",
        "peer_kinds",
        "asserts no input shows a peer kind, a kind check with no join",
    ),
    (
        "tests/header_acceptance_narrowing.rs",
        "not_tool_recorded",
        "id",
        "expected-result IDs, the manifest's keys, not run evidence",
    ),
    (
        "tests/header_acceptance_narrowing.rs",
        "peer_kinds",
        "PEER_KINDS",
        "filters observation kinds by the peer kinds' names, a kind check with no join",
    ),
    (
        "tests/header_catch_not_applicable.rs",
        "each_declared_list_matches_its_baseline",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/header_catch_not_applicable.rs",
        "every_node_catch_declares_its_not_applicable",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/header_catch_not_applicable.rs",
        "no_list_accepts_an_incomplete_reason",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/header_catch_not_applicable.rs",
        "the_patched_list_judges_the_draft_run",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/header_catch_not_applicable.rs",
        "violating",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/header_mixed_resources.rs",
        "witnessed",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/header_wire_crossing.rs",
        "held_applied",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/header_wire_witness.rs",
        "joins_the_frames",
        "PEER_TO_NODE link",
        "steers the picks the reserved-flags script takes; the witness judges the recorded run, not this",
    ),
    (
        "tests/header_wire_witness.rs",
        "joins_the_frames",
        "first_frame seq",
        "steers the picks the reserved-flags script takes; the witness judges the recorded run, not this",
    ),
    (
        "tests/header_wire_witness.rs",
        "joins_the_frames",
        "id",
        "steers the picks the reserved-flags script takes; the witness judges the recorded run, not this",
    ),
    (
        "tests/header_wire_witness.rs",
        "two_skipped_reserved_flags_frames_keep_two_findings_at_one_boundary",
        "ID",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),    (
        "src/check.rs",
        "a_punishment_names_the_frame_judged_on_its_own_connection",
        "connection frame_on quic_stream seq version",
        "checks the punishment's subject is the frame the test planted, an output check; `punishment`'s join is its `ConnectionRef`, whose control follows",
    ),
    (
        "src/harness/header_node/adoption.rs",
        "every_unread_publication_at_a_settled_end_reaches_the_sidecar",
        "version",
        "checks each finding's detail names the state the test published, an output's text, after the subjects are checked whole",
    ),
    (
        "src/harness/header_node/publication/tests.rs",
        "an_adoption_stamps_its_debts_in_session_order",
        "session",
        "checks the debts' order against the test's own sorted session list, an order, not a join",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "a_ledger_handoff_claim_names_both_calls",
        "Attribution",
        "checks the checker's findings for a planted history against the attribution the test expects, an output check; the checker's own joins are C22's",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "a_ledger_hold_claim_names_its_call_and_round",
        "Attribution attribution call",
        "checks the checker's findings for a planted history against the attribution the test expects, an output check; the checker's own joins are C22's",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "a_ledger_hold_claim_names_its_call_and_round",
        "attribution",
        "checks a hold's serialized attribution has no page, a field's absence, not a join",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "a_ledger_page_claim_names_its_page",
        "Attribution page",
        "checks the checker's findings for a planted history against the attribution the test expects, an output check; the checker's own joins are C22's",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "an_open_credit_break_names_its_update_and_no_page",
        "Attribution attribution",
        "checks the checker's findings for a planted history against the attribution the test expects, an output check; the checker's own joins are C22's",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "an_open_credit_break_names_its_update_and_no_page",
        "attribution",
        "checks an update's serialized attribution has no page, a field's absence, not a join",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "attributed",
        "attribution",
        "checks a break has an attribution exactly when its claim kind is an authority claim, a kind check",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "each_finding_joins_on_its_full_identity",
        "Call call op",
        "checks the checker's findings for a planted history against the attribution the test expects, an output check; the checker's own joins are C22's",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "each_finding_joins_on_its_full_identity",
        "page",
        "checks the checker's findings for a planted history against the attribution the test expects, an output check; the checker's own joins are C22's",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "each_ledger_rule_the_property_reports_has_its_authority_rule",
        "Attribution call ledger_attribution",
        "checks the checker's findings for a planted history against the attribution the test expects, an output check; the checker's own joins are C22's",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "each_ledger_rule_the_property_reports_has_its_authority_rule",
        "ledger_attribution",
        "checks a claim rule with no authority rule maps to none, a table lookup, not a join",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "findings_one_key_apart_stay_distinct",
        "attribution",
        "checks a finding's subject is the test's own planted copy with its frame moved, an output check",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "findings_one_key_apart_stay_distinct",
        "frame",
        "checks a path finding names the frame the test planted, an output check",
    ),
    (
        "src/harness/header_node/authority/tests.rs",
        "findings_one_key_apart_stay_distinct",
        "page",
        "checks a finding renders as a finding the test builds from the same page, an output check of the rendering",
    ),
    (
        "src/table.rs",
        "effects_follow_trigger",
        "id stream",
        "keeps the effects on the template's own stream kind and row, a property's row, not another event's identity",
    ),
    (
        "src/table.rs",
        "punishments_follow_conformant",
        "frame",
        "checks a judged frame's label, a kind",
    ),
    (
        "src/table.rs",
        "punishments_follow_conformant",
        "frame id stream",
        "checks a judged frame is on the template's stream kind, a property's row, not another event's identity",
    ),
    (
        "tests/header_acceptance.rs",
        "genuine_history_record",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/header_despite_close.rs",
        "the_fault_close_holds_on_the_unpatched_rejection",
        "page session",
        "pins the fixture's one refusal to the rejected page's session and fault; the companion's join is `fault_close`'s `SessionRef`, whose control follows",
    ),
    (
        "tests/header_mixed_resources.rs",
        "witnessed",
        "new_connection old_connection",
        "checks a peer's two links are on two connections, the opposite of a join; each link joins its admission by `SessionRef`",
    ),
    (
        "tests/header_mixed_resources.rs",
        "witnessed",
        "new_session old_session",
        "checks a peer's two links are two sessions, the opposite of a join; each link joins its admission by `SessionRef`",
    ),
    (
        "tests/header_node_session_ends.rs",
        "a_link_the_harness_closes_shows_in_the_next_view",
        "pick",
        "checks the view's first pick's category and key prefix, kinds",
    ),
    (
        "tests/header_node_session_ends.rs",
        "a_link_the_harness_closes_shows_in_the_next_view",
        "session_id",
        "counts the view's sessions against the test's own link count",
    ),
    (
        "tests/header_release_wake.rs",
        "a_bypassed_wake_s_patched_draft_run_settles",
        "id",
        "property IDs, the manifest's and harness's names for properties, not run evidence",
    ),
    (
        "tests/header_release_wake.rs",
        "a_bypassed_wake_s_patched_draft_run_settles",
        "pick",
        "finds the fixture's one fixed release pick by its literal, to count it and read its step",
    ),
    (
        "tests/header_wire_crossing.rs",
        "conformant_controls",
        "connection",
        "checks the checker accepts a fault on another connection, a result; `unpunished` joins by `ConnectionRef`",
    ),
    (
        "tests/header_wire_prepare_fault.rs",
        "a_bad_page_joins_the_close_of_its_own_connection",
        "page",
        "checks the closes `bad_page_closes` finds for the planted page, a result, before the control of its `ConnectionRef` join",
    ),
    (
        "tests/table_properties.rs",
        "every_cadence_punishment_at_a_boundary_is_reported",
        "id",
        "checks the report's violations of one property ID against the ones the test expects, an output check",
    ),
    (
        "tests/table_properties.rs",
        "every_unjustified_punishment_is_a_violation",
        "id",
        "checks the report's violations of one property ID against the ones the test expects, an output check",
    ),
];

/// The PR base, hdr-14's parent: code there predates Rule 25, so only its
/// raw joins may stay raw ([`RAW_JOIN_BASELINE`]).
const RAW_JOIN_BASE: &str = "3675f1c486";

/// The committed raw-join baseline, relative to the crate: each hit
/// [`raw_joins`] finds in [`raw_join_scope`] at [`RAW_JOIN_BASE`] that's
/// still at the head, one per line as `file<TAB>function<TAB>comparison`,
/// which [`regenerate_raw_join_baseline`] writes. Taking only base hits, it
/// never grows to hold a new one, and as head code changes it only shrinks.
const RAW_JOIN_BASELINE: &str = "tests/fixtures/raw_join_baseline.txt";

/// One raw identity comparison [`raw_joins`] finds.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RawJoin {
    /// The function it's in, by name.
    function: String,
    /// Its identity names, sorted and space-separated.
    names: String,
    /// Its source text with all whitespace and trailing commas removed, so
    /// a reformat keeps it ([`normalized`]).
    text: String,
}

/// The files [`raw_joins`] reads, relative to the crate: every Rust file
/// under `src` and `tests`, test modules included, but `refs.rs`, where
/// the join is defined. `files` lists the crate's files, relative to it.
fn raw_join_scope(files: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut scope: Vec<String> = files
        .into_iter()
        .filter(|file| file.starts_with("src/") || file.starts_with("tests/"))
        .filter(|file| file.ends_with(".rs"))
        .filter(|file| file != "src/harness/header_acceptance/refs.rs")
        .collect();
    scope.sort();
    scope
}

/// Every file of the crate at the head, relative to it.
fn head_files() -> Vec<String> {
    fn walk(dir: &Path, krate: &Path, out: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).expect("the directory reads") {
            let path = entry.expect("an entry reads").path();
            if path.is_dir() {
                walk(&path, krate, out);
            } else {
                let relative = path.strip_prefix(krate).expect("under the crate");
                out.push(relative.display().to_string());
            }
        }
    }
    let krate = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    walk(&krate.join("src"), krate, &mut files);
    walk(&krate.join("tests"), krate, &mut files);
    files
}

/// `text` with every whitespace character removed, and a comma before a
/// closing bracket, so a comparison rustfmt rewraps keeps its key.
fn normalized(text: &str) -> String {
    let mut out: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    for close in [")", "]", "}"] {
        out = out.replace(&format!(",{close}"), close);
    }
    out
}

/// The source between `start` and `end` of `lines`, a file's lines,
/// [`normalized`].
fn span_text(
    lines: &[&str],
    start: proc_macro2::LineColumn,
    end: proc_macro2::LineColumn,
) -> String {
    let mut text = String::new();
    for line in start.line..=end.line {
        let chars: Vec<char> = lines
            .get(line.saturating_sub(1))
            .copied()
            .unwrap_or("")
            .chars()
            .collect();
        let from = if line == start.line { start.column } else { 0 };
        let to = if line == end.line {
            end.column
        } else {
            chars.len()
        };
        let from = from.min(chars.len());
        text.extend(&chars[from..to.clamp(from, chars.len())]);
        text.push('\n');
    }
    normalized(&text)
}

/// Each raw identity comparison in the Rust source `text`, the file `file`
/// ([`RawJoin`]): an `==` or `!=`, a `contains` call, or an `assert_eq!`
/// or `assert_ne!` (and their `debug_` and `prop_` forms) of two
/// arguments, either side of which names an identity, by a path, field,
/// method or JSON key one of whose `_`-separated parts is an
/// [`IDENTITY_WORDS`] word. Macro arguments count, a `matches!` guard and a
/// match guard included, and test code counts like any other. A one-key-off
/// control's closure doesn't, since it judges a join's result.
///
/// It's still blind to a join the types make, such as a map or set keyed by
/// an identity (`get`, `entry`, `contains_key`, `insert`); a comparison
/// through a helper, variable or field whose name holds no identity word,
/// such as `a == b`; a pattern that matches an identity's literal value;
/// an ordering comparison (`<`, `>=`); and a string join by prefix
/// (`starts_with`, `strip_prefix`).
fn raw_joins(text: &str, file: &str) -> BTreeSet<RawJoin> {
    use syn::{spanned::Spanned, visit::Visit};
    /// Whether `name` holds an identity word as one of its parts.
    fn identity(name: &str) -> bool {
        name.split('_')
            .any(|part| IDENTITY_WORDS.contains(&part.to_ascii_lowercase().as_str()))
    }
    /// Every identity name `expr` holds.
    fn identities(expr: &syn::Expr) -> BTreeSet<String> {
        /// Collects them.
        #[derive(Default)]
        struct Names(BTreeSet<String>);
        impl<'ast> Visit<'ast> for Names {
            fn visit_ident(&mut self, ident: &'ast proc_macro2::Ident) {
                let name = ident.to_string();
                if identity(&name) {
                    self.0.insert(name);
                }
            }

            fn visit_expr_index(&mut self, index: &'ast syn::ExprIndex) {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(key),
                    ..
                }) = &*index.index
                {
                    if identity(&key.value()) {
                        self.0.insert(key.value());
                    }
                }
                syn::visit::visit_expr_index(self, index);
            }

            fn visit_macro(&mut self, mac: &'ast syn::Macro) {
                for expr in macro_exprs(mac) {
                    self.visit_expr(&expr);
                }
            }
        }
        let mut names = Names::default();
        names.visit_expr(expr);
        names.0
    }
    /// The assertion macros whose first two arguments are compared.
    const ASSERTS: &[&str] = &[
        "assert_eq",
        "assert_ne",
        "debug_assert_eq",
        "debug_assert_ne",
        "prop_assert_eq",
        "prop_assert_ne",
    ];
    /// Collects the comparisons, by enclosing function.
    struct Raw<'a> {
        lines: Vec<&'a str>,
        within: Vec<String>,
        found: BTreeSet<RawJoin>,
    }
    impl Raw<'_> {
        /// Notes a comparison of `sides`, from the first's start to the
        /// last's end, if one names an identity.
        fn compare(&mut self, sides: &[&syn::Expr]) {
            let names: BTreeSet<String> = sides.iter().flat_map(|side| identities(side)).collect();
            let (Some(first), Some(last)) = (sides.first(), sides.last()) else {
                return;
            };
            if !names.is_empty() {
                let names: Vec<String> = names.into_iter().collect();
                self.found.insert(RawJoin {
                    function: self.within.last().cloned().unwrap_or_default(),
                    names: names.join(" "),
                    text: span_text(&self.lines, first.span().start(), last.span().end()),
                });
            }
        }
    }
    impl<'ast> Visit<'ast> for Raw<'_> {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            self.within.push(item.sig.ident.to_string());
            syn::visit::visit_item_fn(self, item);
            self.within.pop();
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            self.within.push(item.sig.ident.to_string());
            syn::visit::visit_impl_item_fn(self, item);
            self.within.pop();
        }

        fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
            self.within.push(item.sig.ident.to_string());
            syn::visit::visit_trait_item_fn(self, item);
            self.within.pop();
        }

        fn visit_expr_binary(&mut self, binary: &'ast syn::ExprBinary) {
            if matches!(binary.op, syn::BinOp::Eq(_) | syn::BinOp::Ne(_)) {
                self.compare(&[&binary.left, &binary.right]);
            }
            syn::visit::visit_expr_binary(self, binary);
        }

        fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
            let control = matches!(&*call.func, syn::Expr::Path(path)
                if path.path.segments.last().is_some_and(|last| last.ident == "assert_one_key_off"));
            if control {
                // A control's closure judges a join's result, as `==` of
                // what the join found; the join itself is elsewhere.
                if let Some(positive) = call.args.first() {
                    self.visit_expr(positive);
                }
            } else {
                syn::visit::visit_expr_call(self, call);
            }
        }

        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            if call.method == "contains" {
                let mut sides: Vec<&syn::Expr> = vec![&call.receiver];
                sides.extend(call.args.iter());
                self.compare(&sides);
            }
            syn::visit::visit_expr_method_call(self, call);
        }

        fn visit_macro(&mut self, mac: &'ast syn::Macro) {
            let exprs = macro_exprs(mac);
            let asserts = mac
                .path
                .segments
                .last()
                .is_some_and(|last| ASSERTS.iter().any(|name| last.ident == name));
            if let (true, [left, right, ..]) = (asserts, exprs.as_slice()) {
                self.compare(&[left, right]);
            }
            for expr in &exprs {
                self.visit_expr(expr);
            }
        }
    }
    let parsed = syn::parse_file(text).unwrap_or_else(|error| panic!("{file} parses: {error}"));
    let mut raw = Raw {
        lines: text.lines().collect(),
        within: Vec::new(),
        found: BTreeSet::new(),
    };
    raw.visit_file(&parsed);
    raw.found
}

/// The committed [`RAW_JOIN_BASELINE`], as `(file, function, text)`.
fn raw_join_baseline() -> BTreeSet<(String, String, String)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(RAW_JOIN_BASELINE);
    std::fs::read_to_string(&path)
        .expect("the raw-join baseline is committed")
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let mut parts = line.splitn(3, '\t');
            let mut part = || {
                parts
                    .next()
                    .unwrap_or_else(|| panic!("a baseline line has three fields: {line}"))
                    .to_owned()
            };
            (part(), part(), part())
        })
        .collect()
}

/// Why `found`, each file's [`raw_joins`], breaks Rule 25's ratchet, one
/// line each: a hit the `baseline`, as `(file, function, text)`, doesn't
/// list and `allowed`, as `(file, function, names)`, doesn't excuse; a
/// baseline line no hit matches any more, which must go; and an `allowed`
/// entry that excuses no hit the baseline doesn't already list.
fn raw_join_gaps(
    found: &BTreeSet<(String, RawJoin)>,
    baseline: &BTreeSet<(String, String, String)>,
    allowed: &BTreeSet<(String, String, String)>,
) -> Vec<String> {
    let mut gaps = Vec::new();
    let based = |file: &str, hit: &RawJoin| {
        baseline.contains(&(file.to_owned(), hit.function.clone(), hit.text.clone()))
    };
    let new: Vec<&(String, RawJoin)> = found
        .iter()
        .filter(|(file, hit)| !based(file, hit))
        .collect();
    for (file, hit) in &new {
        if !allowed.contains(&(file.clone(), hit.function.clone(), hit.names.clone())) {
            gaps.push(format!(
                "{file} {} joins {} raw: {}",
                hit.function, hit.names, hit.text
            ));
        }
    }
    for (file, function, text) in baseline {
        let still = found
            .iter()
            .any(|(at, hit)| at == file && hit.function == *function && hit.text == *text);
        if !still {
            gaps.push(format!(
                "the baseline lists {file} {function} {text}, which is gone: run \
                 `regenerate_raw_join_baseline`"
            ));
        }
    }
    for (file, function, names) in allowed {
        let excuses = new
            .iter()
            .any(|(at, hit)| at == file && hit.function == *function && hit.names == *names);
        if !excuses {
            gaps.push(format!(
                "NOT_RAW_JOINS lists {file} {function} {names}, which excuses nothing new"
            ));
        }
    }
    gaps
}

/// Every hit [`raw_joins`] finds in [`raw_join_scope`] at the head.
fn head_raw_joins() -> BTreeSet<(String, RawJoin)> {
    let krate = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut found = BTreeSet::new();
    for file in raw_join_scope(head_files()) {
        let text = std::fs::read_to_string(krate.join(&file)).expect("a source file reads");
        for hit in raw_joins(&text, &file) {
            found.insert((file.clone(), hit));
        }
    }
    found
}

/// Rule 25's raw-join scan, as a ratchet on the code hdr-14 adds: in
/// [`raw_join_scope`], every comparison that names an identity
/// ([`raw_joins`]) is one the PR base already had ([`RAW_JOIN_BASELINE`]),
/// or one [`NOT_RAW_JOINS`] lists, with why it joins no identity. Each
/// baseline line and each listed one is still needed ([`raw_join_gaps`]).
/// Any other identity join goes through a typed ref. A planted raw join on
/// a peer, an operation, a JSON attribution, by `contains` or by
/// `assert_eq!` fails the scan, and a comparison of a kind or a count, or a
/// control's closure judging a join's result, doesn't. So does a hit
/// neither list holds, a gone baseline line, and an allowlist entry
/// excusing only baseline hits.
#[test]
fn identity_joins_go_through_refs() {
    let found = head_raw_joins();
    let baseline = raw_join_baseline();
    let allowed: BTreeSet<(String, String, String)> = NOT_RAW_JOINS
        .iter()
        .map(|(file, within, names, why)| {
            assert!(!why.is_empty(), "{file} {within} has no reason");
            (
                (*file).to_owned(),
                (*within).to_owned(),
                (*names).to_owned(),
            )
        })
        .collect();
    assert_eq!(
        allowed.len(),
        NOT_RAW_JOINS.len(),
        "an entry is listed twice"
    );
    let gaps = raw_join_gaps(&found, &baseline, &allowed);
    assert!(gaps.is_empty(), "Rule 25's raw-join ratchet: {gaps:#?}");

    // The ratchet's own controls, on one planted hit.
    let hit = |names: &str, text: &str| RawJoin {
        function: "p".into(),
        names: names.into(),
        text: text.into(),
    };
    let planted: BTreeSet<(String, RawJoin)> =
        [("tests/t.rs".to_owned(), hit("peer", "name==peer"))].into();
    let key = |a: &str, b: &str, c: &str| (a.to_owned(), b.to_owned(), c.to_owned());
    let none = BTreeSet::new();
    assert_eq!(raw_join_gaps(&planted, &none, &none).len(), 1);
    let based = [key("tests/t.rs", "p", "name==peer")].into();
    assert_eq!(raw_join_gaps(&planted, &based, &none), Vec::<String>::new());
    let excused = [key("tests/t.rs", "p", "peer")].into();
    assert_eq!(
        raw_join_gaps(&planted, &none, &excused),
        Vec::<String>::new()
    );
    let gone = [key("tests/t.rs", "p", "name==other_peer")].into();
    assert_eq!(raw_join_gaps(&planted, &gone, &excused).len(), 1);
    assert_eq!(raw_join_gaps(&planted, &based, &excused).len(), 1);

    for (planted, names) in [
        ("fn p(a: &str, b: &str) -> bool { a == b }", None),
        ("fn p(kind: &str) -> bool { kind == \"invalidate\" }", None),
        ("fn p(frames: &[u64]) -> bool { frames.len() == 1 }", None),
        ("fn p(peer: &str, name: &str) -> bool { name == peer }", Some("peer")),
        ("fn p(id: u64, op: u64) -> bool { matches!(id, x if x == op) }", Some("op")),
        (
            "fn p(record: &Value, want: &Value) -> bool { record[\"attribution\"] == *want }",
            Some("attribution"),
        ),
        ("fn p(links: &[u64], conn: u64) -> bool { links.contains(&conn) }", Some("conn")),
        (
            "fn p(causes: &[(u64, usize)], stamp: u64) -> bool { causes.iter().any(|(at, _)| *at == stamp) }",
            Some("stamp"),
        ),
        (
            "fn p(declared: &[String], pick: &str) -> bool { declared.iter().any(|action| action == pick) }",
            Some("pick"),
        ),
        (
            "fn p(peer: &PeerNameRef) { assert_one_key_off::<PeerNameRef>(peer, |p| p.peer == peer.peer); }",
            None,
        ),
        (
            "fn p(o: &Obs, c: u64) -> bool { match o { Obs::Closed { connection } if *connection == c => true, _ => false } }",
            Some("connection"),
        ),
        ("fn p(a: &Act, b: &Frame) { assert_eq!(a.frame, b.clone()); }", Some("frame")),
        ("fn p(a: u64, session: u64) { assert_ne!(a, session, \"{a}\"); }", Some("session")),
        ("#[cfg(test)]\nmod t { #[test]\nfn p() { assert_eq!(1, conn()); } }", Some("conn")),
        ("fn p(a: u64, b: u64) { assert_eq!(a, b); }", None),
    ] {
        let found: BTreeSet<(String, String)> = raw_joins(planted, "src/t.rs")
            .into_iter()
            .map(|hit| (hit.function, hit.names))
            .collect();
        let want: BTreeSet<(String, String)> = names
            .map(|names| ("p".to_owned(), names.to_owned()))
            .into_iter()
            .collect();
        assert_eq!(found, want, "{planted}");
    }
    // A reformat keeps a hit's text, so the baseline still lists it.
    let one = raw_joins(
        "fn p(peer: &str, name: &str) -> bool { name == peer }",
        "t.rs",
    );
    let wrapped = raw_joins(
        "fn p(peer: &str, name: &str) -> bool {\n    name\n        == peer\n}",
        "t.rs",
    );
    assert_eq!(one, wrapped);
    assert_eq!(
        one.iter().map(|hit| hit.text.as_str()).collect::<Vec<_>>(),
        ["name==peer"]
    );
}

/// Rewrites [`RAW_JOIN_BASELINE`]: every hit [`raw_joins`] finds in
/// [`raw_join_scope`] at [`RAW_JOIN_BASE`], read by `git show`, that the
/// head still has. It never adds a hit the base didn't have, so a raw join
/// new in hdr-14 must become a typed ref with its control, or go in
/// [`NOT_RAW_JOINS`] with why it joins no identity.
#[test]
#[ignore = "rewrites the committed raw-join baseline from the PR base"]
fn regenerate_raw_join_baseline() {
    let krate = Path::new(env!("CARGO_MANIFEST_DIR"));
    let git = |args: &[&str]| -> String {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(krate)
            .args(args)
            .output()
            .expect("git runs");
        assert!(out.status.success(), "git {args:?}: {out:?}");
        String::from_utf8(out.stdout).expect("git prints UTF-8")
    };
    let listed = git(&[
        "ls-tree",
        "-r",
        "--name-only",
        RAW_JOIN_BASE,
        "--",
        "src",
        "tests",
    ]);
    let head = head_raw_joins();
    let mut lines = BTreeSet::new();
    for file in raw_join_scope(listed.lines().map(str::to_owned)) {
        let text = git(&["show", &format!("{RAW_JOIN_BASE}:./{file}")]);
        for hit in raw_joins(&text, &file) {
            let at = (file.clone(), hit.clone());
            if head.contains(&at) {
                lines.insert(format!("{file}\t{}\t{}", hit.function, hit.text));
            }
        }
    }
    let mut out = format!(
        "# Rule 25's raw-join baseline: every raw identity comparison the PR base \
         ({RAW_JOIN_BASE}) had that the head still has, as file, function and \
         comparison, tab-separated. Written by `regenerate_raw_join_baseline` in \
         tests/header_acceptance.rs. Don't add lines by hand.\n"
    );
    for line in lines {
        out.push_str(&line);
        out.push('\n');
    }
    std::fs::write(krate.join(RAW_JOIN_BASELINE), out).expect("the fixture directory exists");
}

/// Every node harness's missing-source keys are covered by that harness's
/// own missing-source test, and only a machine's by the machine's standing
/// `Open`. A node harness the mapping misses, as `header-sync/liveness`
/// once was, would fall to the machine's `Open` and fail here.
#[test]
fn each_node_harness_s_missing_sources_are_its_own_test_s() {
    let manifest = Manifest::committed();
    let needs = header_acceptance::needs();
    let harnesses: BTreeSet<&str> = needs.iter().map(|(harness, _)| harness.as_str()).collect();
    assert!(harnesses.contains("header-sync/liveness"));
    for (harness, source) in &needs {
        let key = Key::MissingSource {
            harness: harness.clone(),
            source: source.clone(),
        };
        let covering: Vec<&Expected> = manifest
            .expected
            .iter()
            .filter(|result| result.keys.contains(&key))
            .map(|result| &result.expected)
            .collect();
        let [expected] = covering.as_slice() else {
            panic!("{key:?} is covered {} times", covering.len());
        };
        if rows::is_machine(harness) {
            assert_eq!(rows::missing_source_test(harness), None, "{harness}");
            assert!(matches!(expected, Expected::Open { .. }), "{key:?}");
            continue;
        }
        let test = rows::missing_source_test(harness)
            .unwrap_or_else(|| panic!("{harness} is a node harness with no missing-source test"));
        assert_eq!(test.file, "tests/header_acceptance_sources.rs", "{harness}");
        assert!(
            matches!(expected, Expected::Passes { test: t } if *t == test),
            "{key:?} is covered by {expected:?}, not {test:?}"
        );
    }
}

/// A node history's genuine whole-report record under `manifest`: every ID
/// its harness registers reports as the history's own `Holds` expects,
/// `NotApplicable` with its listed reason or `Held`, and the run completed
/// with its trigger.
fn genuine_history_record(manifest: &Manifest, history: HistoryRef) -> ResultRecord {
    let at = covering(
        manifest,
        &Key::NaList {
            run: history.into(),
        },
    );
    let Expected::Holds { not_applicable, .. } = manifest.expected[at].expected else {
        panic!("{history:?}'s list is judged by its Holds");
    };
    let reports: Vec<(&str, Report)> = manifest
        .matrix
        .iter()
        .filter(|(harness, _, _)| *harness == history.harness)
        .map(|&(_, id, _)| {
            let report = not_applicable
                .iter()
                .find(|na| na.property == id)
                .map_or_else(held, |listed| na(listed.reason));
            (id, report)
        })
        .collect();
    ResultRecord {
        expected: manifest.expected[at].expected.id().unwrap(),
        commit: S.into(),
        actual: Actual::Run(execution(history.into(), history.harness, &reports)),
    }
}

/// The obligations hf-6, hf-7, E1, E9 and hf-1 to hf-3 delivered verify on
/// their own evidence and block without it. Contracts are cleared, so only
/// each key's own result decides: a gate requirement and a `NotApplicable`
/// reason on its test's pass, and a node history's `NotApplicable` list on
/// that history's whole-report pass, which judges every report against the
/// list its row carries. A list one report off, or a row whose list is
/// dropped, blocks it.
#[test]
fn delivered_obligations_verify_on_their_evidence() {
    use header_acceptance::required as obligation;
    let mut manifest = Manifest::committed();
    manifest.contracts.clear();
    manifest.required_contracts.clear();
    let required = required(&manifest);
    let mut tests: Vec<(Key, TestRef)> = [
        (
            "gate-1/topology-freshness",
            obligation::TOPOLOGY_FRESHNESS_TEST,
        ),
        (
            "gate-1/store-inventory/registered",
            obligation::STORE_INVENTORY_REGISTERED_TEST,
        ),
        (
            "gate-1/store-inventory/checkpoint-and-producer",
            obligation::STORE_INVENTORY_PRODUCER_TEST,
        ),
        (
            "gate-2/end-boundary-preservation/registry",
            obligation::END_BOUNDARY_REGISTRY_TEST,
        ),
        (
            "gate-2/end-boundary-preservation/fault-store",
            obligation::END_BOUNDARY_FAULT_STORE_TEST,
        ),
    ]
    .into_iter()
    .map(|(name, test)| (Key::Requirement { name: name.into() }, test))
    .collect();
    for entry in NA_REASONS {
        let test = rows::reason_test(entry.reason)
            .unwrap_or_else(|| panic!("{:?} has a test that shows it", entry.reason));
        tests.push((
            Key::NaReason {
                reason: entry.reason,
            },
            test,
        ));
    }
    // A storage and a wire history, each with a non-empty list.
    let histories: Vec<HistoryRef> = ["header-sync/storage", "header-sync/wire"]
        .into_iter()
        .map(|harness| {
            rows::counted_histories()
                .into_iter()
                .find(|history| {
                    history.harness == harness
                        && !header_acceptance::not_applicable::history_expected(*history).is_empty()
                })
                .unwrap_or_else(|| panic!("a counted {harness} history lists a report"))
        })
        .collect();
    let lists: Vec<Key> = histories
        .iter()
        .map(|&history| Key::NaList {
            run: history.into(),
        })
        .collect();
    let genuine = ResultFile {
        subject: S.into(),
        subject_source: SOURCE.into(),
        // Two reasons share a test, whose one result serves both keys.
        results: tests
            .iter()
            .map(|&(_, test)| test)
            .collect::<BTreeSet<TestRef>>()
            .into_iter()
            .map(test_record)
            .chain(
                histories
                    .iter()
                    .map(|&history| genuine_history_record(&manifest, history)),
            )
            .collect(),
    };
    let states = |manifest: &Manifest, results: &ResultFile| {
        header_acceptance::evaluate(manifest, &required, results, SUBJECT).keys
    };
    let verified = states(&manifest, &genuine);
    for key in tests.iter().map(|(key, _)| key).chain(&lists) {
        assert_eq!(verified[key], ObligationState::Verified, "{key:?}");
    }
    // Without their results, every one blocks.
    let none = ResultFile {
        results: Vec::new(),
        ..genuine.clone()
    };
    let missing = states(&manifest, &none);
    for key in tests.iter().map(|(key, _)| key).chain(&lists) {
        assert!(blocked(&missing[key]), "{key:?}: {:?}", missing[key]);
    }
    // A failed test blocks its key.
    let mut failed = genuine.clone();
    let (key, test) = &tests[0];
    record_mut(&mut failed, &Expected::Passes { test: *test }.id().unwrap()).actual =
        test_actual(*test, TestOutcome::Failed);
    assert!(blocked(&states(&manifest, &failed)[key]));
    // A listed report that held instead, or a row that drops its list,
    // blocks the history's list key.
    let history = histories[1];
    let mut off = genuine.clone();
    let id = genuine_history_record(&manifest, history).expected;
    let Actual::Run(run) = &mut record_mut(&mut off, &id).actual else {
        unreachable!()
    };
    let listed = header_acceptance::not_applicable::history_expected(history)[0].property;
    run.reports.insert(listed.into(), held());
    assert!(blocked(&states(&manifest, &off)[&lists[1]]));
    let mut dropped = manifest.clone();
    for result in &mut dropped.expected {
        if let Expected::Holds {
            history: h,
            not_applicable,
            ..
        } = &mut result.expected
        {
            if *h == history {
                *not_applicable = &[];
            }
        }
    }
    assert!(blocked(&states(&dropped, &genuine)[&lists[1]]));
}

/// Every node-harness history a `Holds` or `StaysNotApplicable` judges
/// matches exactly one group of the recorded `NotApplicable` reports, so
/// its whole-report pass expects what its run reports beside its target,
/// and every node history's list key is judged by that pass. A history
/// that matches none would expect every other ID to hold.
#[test]
fn every_node_history_lists_its_not_applicable_reports() {
    use header_acceptance::not_applicable::{history_expected, history_groups_matched};
    let manifest = Manifest::committed();
    let mut histories = BTreeSet::new();
    for result in &manifest.expected {
        if let Expected::Holds {
            history,
            not_applicable,
            ..
        }
        | Expected::StaysNotApplicable {
            history,
            not_applicable,
            ..
        } = result.expected
        {
            if rows::is_machine(history.harness) {
                continue;
            }
            assert_eq!(
                history_groups_matched(history),
                1,
                "{history:?} matches one group"
            );
            assert_eq!(
                not_applicable,
                history_expected(history),
                "{history:?}'s row carries its recorded list"
            );
            histories.insert(RunId::from(history));
        }
    }
    for history in rows::counted_histories() {
        if !rows::is_machine(history.harness) {
            assert!(histories.contains(&RunId::from(history)), "{history:?}");
        }
    }
    let judged: Vec<HistoryRef> = manifest
        .expected
        .iter()
        .filter_map(|result| match result.expected {
            Expected::Holds { history, .. } | Expected::StaysNotApplicable { history, .. } => {
                Some(history)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        header_acceptance::not_applicable::history_groups_unmatched(&judged),
        Vec::<String>::new(),
        "every group lists a judged history"
    );
}
