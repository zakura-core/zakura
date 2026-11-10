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
            reasons::{self, NA_REASONS, NOT_HEADER},
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
    header_acceptance::tool::planted_cases(&repo())
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
        vec![
            owned("header-sync/s5v9/headers/reserved-flags-closes", "flags"),
            owned("header-sync/s5v9/status/reserved-flags-closes", "flags"),
            owned("session/honest-never-punished", "punished"),
        ]
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
    // The retired mutation's Open with another reason, or a ToMove's Open,
    // isn't its own.
    refused(&|m| {
        let at = covering(
            m,
            &Key::Native {
                patch: "readmission-unchecked".into(),
            },
        );
        if let Expected::Open { reason, .. } = &mut m.expected[at].expected {
            *reason = "covered by a zakura-network unit test";
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
                (
                    NativeClass::Retired { .. },
                    Expected::Open {
                        until: Some("hdr-14.7b"),
                        ..
                    },
                ) => {}
                (class, expected) => panic!("{patch}: {class:?} as {:?}", expected.kind()),
            }
        }
    }
    // A retired mutation stays a required key, blocked by its transitional
    // Open until its witness runs.
    assert_eq!(
        committed_required(&manifest).get(&Key::Native {
            patch: "readmission-unchecked".into(),
        }),
        Some(&BTreeSet::from([Gate::G6]))
    );
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
/// E1, whose reason-bearing `NotApplicable` the expected-NA lists and the
/// reason tests need.
/// A transitional row and the result that replaces it would cover one key
/// twice, which `every_required_key_is_covered_once_with_its_gates` refuses.
/// A PR that merges leaves this list, so any `Open` still naming it fails
/// `open_errors`, including a gate requirement's, whose replacement is a
/// `Permit::Test` that PR switches to.
const OPEN_PRS: &[&str] = &[
    "hdr-14.4",
    "hdr-14.5",
    "hdr-14.6",
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

/// One `NotApplicable` construction the scan found: its file, its
/// qualified function and its trimmed source line.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Construction {
    file: String,
    function: String,
    line: String,
    /// Its enclosing function's [`fingerprint`].
    fingerprint: String,
    /// Whether the statement before it in its block calls
    /// `missing_evidence`, which makes it a harness failure, not a reason.
    after_missing: bool,
}

/// Walks a parsed file, collecting every `Verdict::NotApplicable` and
/// `PropertyOutcome::NotApplicable` in expression position, by its
/// qualified function. Comparisons and patterns aren't constructions.
/// Anything the scan can't see through, an alias of the variant or a macro
/// other than a known assertion naming it, is an error.
struct Scan<'a> {
    file: &'a str,
    /// Every file the scan reads, by its path relative to the crate.
    files: &'a BTreeSet<String>,
    lines: Vec<&'a str>,
    scope: Vec<String>,
    /// The enclosing functions' fingerprints, innermost last.
    prints: Vec<String>,
    /// `Verdict`'s definition and each `impl` block for it in this file, as
    /// `(line, source text)`.
    verdict: Vec<(usize, String)>,
    /// Set while visiting a statement that follows a `missing_evidence`
    /// call in its block.
    after_missing: bool,
    /// The inline modules enclosing the item being visited.
    modules: Vec<String>,
    found: Vec<Construction>,
    errors: Vec<String>,
    /// Every function's text, by its qualified name, with the module it
    /// sits in.
    functions: Vec<(String, String, String)>,
    /// Every module-scope `use` item's text, by its module.
    uses: Vec<(String, String)>,
}

impl Scan<'_> {
    /// Whether `path` ends in `NotApplicable`, whatever qualifies it, so an
    /// alias such as `V::NotApplicable` or `Self::NotApplicable` counts.
    fn names_variant(path: &syn::Path) -> bool {
        path.segments
            .last()
            .is_some_and(|segment| segment.ident == "NotApplicable")
    }

    /// The source text from `start` to `end`, lines kept.
    fn source(&self, start: proc_macro2::LineColumn, end: proc_macro2::LineColumn) -> String {
        let mut text = Vec::new();
        for line in start.line..=end.line {
            let source: Vec<char> = self.lines.get(line - 1).unwrap_or(&"").chars().collect();
            let from = if line == start.line { start.column } else { 0 };
            let to = if line == end.line {
                end.column
            } else {
                source.len()
            };
            text.push(
                source[from.min(source.len())..to.min(source.len())]
                    .iter()
                    .collect::<String>(),
            );
        }
        text.join("\n")
    }

    /// Visits a function whose signature starts at `start` and whose body
    /// ends at `end`, under `name`, with its [`fingerprint`].
    fn function(
        &mut self,
        name: String,
        start: proc_macro2::LineColumn,
        end: proc_macro2::LineColumn,
        visit: impl FnOnce(&mut Self),
    ) {
        let text = self.source(start, end);
        let print = fingerprint(&text);
        self.scope.push(name);
        self.functions
            .push((self.scope.join("::"), self.modules.join("::"), text));
        self.prints.push(print);
        visit(self);
        self.prints.pop();
        self.scope.pop();
    }

    /// The trimmed source line `span` starts on.
    fn line(&self, span: proc_macro2::Span) -> String {
        let line = span.start().line;
        self.lines
            .get(line.saturating_sub(1))
            .map(|line| line.trim().to_owned())
            .unwrap_or_default()
    }

    /// Records a construction at `span`.
    fn construction(&mut self, span: proc_macro2::Span) {
        let line = self.line(span);
        if self.scope.is_empty() {
            self.errors.push(format!(
                "{}: a construction outside a function: {line}",
                self.file
            ));
        }
        self.found.push(Construction {
            file: self.file.to_owned(),
            function: self.scope.join("::"),
            line,
            fingerprint: self.prints.last().cloned().unwrap_or_default(),
            after_missing: self.after_missing,
        });
    }

    /// Sees through a macro that names the variant: a known pattern or
    /// assertion macro's expression arguments are parsed and visited like
    /// any other expression, its pattern isn't a construction, and an
    /// `assert_eq!` or `assert_ne!` argument that's the bare variant is a
    /// comparison. Any other macro naming it, or a known one that doesn't
    /// parse, can't be seen through.
    fn check_macro(&mut self, mac: &syn::Macro) {
        use syn::parse::Parser;
        let name = mac
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default();
        // A macro other than a value-building one that names a verdict type
        // could generate a definition or impl for it, which the pin
        // wouldn't see.
        let value_macro = matches!(
            name.as_str(),
            "matches"
                | "assert"
                | "assert_eq"
                | "assert_ne"
                | "debug_assert"
                | "debug_assert_eq"
                | "debug_assert_ne"
                | "vec"
                | "format"
                | "write"
                | "writeln"
                | "panic"
                | "unreachable"
        );
        let names_type = mac.tokens.clone().into_iter().any(|token| {
            VERDICT_TYPES
                .iter()
                .any(|verdict| names_ident(&token, verdict))
        });
        if names_type && !value_macro {
            let line = self.line(mac.path.segments[0].ident.span());
            self.errors.push(format!(
                "{}: {name}! names a verdict type, so it could generate an impl: {line}",
                self.file
            ));
        }
        let names = mac
            .tokens
            .clone()
            .into_iter()
            .any(|token| contains_variant(&token));
        // A value macro naming either is seen through, so an item in its
        // arguments, an impl inside a block, is visited too.
        if !names && !names_type {
            return;
        }
        let args = || {
            syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated
                .parse2(mac.tokens.clone())
        };
        let parsed: syn::Result<Vec<syn::Expr>> = match name.as_str() {
            "matches" => (|input: syn::parse::ParseStream<'_>| {
                let scrutinee: syn::Expr = input.parse()?;
                input.parse::<syn::Token![,]>()?;
                syn::Pat::parse_multi_with_leading_vert(input)?;
                let mut exprs = vec![scrutinee];
                if input.peek(syn::Token![if]) {
                    input.parse::<syn::Token![if]>()?;
                    exprs.push(input.parse()?);
                }
                if input.peek(syn::Token![,]) {
                    input.parse::<syn::Token![,]>()?;
                }
                Ok(exprs)
            })
            .parse2(mac.tokens.clone()),
            "assert" | "debug_assert" | "vec" | "format" | "write" | "writeln" | "panic"
            | "unreachable" => args().map(|args| args.into_iter().collect()),
            "assert_eq" | "assert_ne" | "debug_assert_eq" | "debug_assert_ne" => {
                args().map(|args| {
                    args.into_iter()
                        .filter(|arg| {
                            !matches!(arg, syn::Expr::Path(p) if Self::names_variant(&p.path))
                        })
                        .collect()
                })
            }
            _ => Err(syn::Error::new(
                mac.path.segments[0].ident.span(),
                "unknown",
            )),
        };
        match parsed {
            Ok(exprs) => {
                for expr in &exprs {
                    syn::visit::Visit::visit_expr(self, expr);
                }
            }
            Err(_) => {
                let line = self.line(mac.path.segments[0].ident.span());
                self.errors.push(format!(
                    "{}: {name}! names NotApplicable or a verdict type and can't be seen \
                     through: {line}",
                    self.file
                ));
            }
        }
    }
}

/// The last identifier of `ty`'s path, through any parentheses or
/// invisible groups around it, or `None` for a type that isn't a path.
fn type_name(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Path(path) => path.path.segments.last().map(|s| s.ident.to_string()),
        syn::Type::Paren(inner) => type_name(&inner.elem),
        syn::Type::Group(inner) => type_name(&inner.elem),
        _ => None,
    }
}

/// Whether `tree` renames a verdict type anywhere: `Verdict as V`, or
/// `Verdict::{self as V}` under a path ending in one.
fn renames_verdict(tree: &syn::UseTree, under_verdict: bool) -> bool {
    match tree {
        syn::UseTree::Path(path) => renames_verdict(
            &path.tree,
            VERDICT_TYPES.contains(&path.ident.to_string().as_str()),
        ),
        syn::UseTree::Rename(rename) => {
            VERDICT_TYPES.contains(&rename.ident.to_string().as_str())
                || (under_verdict && rename.ident == "self")
        }
        syn::UseTree::Group(group) => group
            .items
            .iter()
            .any(|item| renames_verdict(item, under_verdict)),
        syn::UseTree::Name(_) | syn::UseTree::Glob(_) => false,
    }
}

/// Whether `token` is or contains the identifier `NotApplicable`.
fn contains_variant(token: &proc_macro2::TokenTree) -> bool {
    names_ident(token, "NotApplicable")
}

/// Whether `token` is or contains the identifier `name`.
fn names_ident(token: &proc_macro2::TokenTree, name: &str) -> bool {
    match token {
        proc_macro2::TokenTree::Ident(ident) => ident == name,
        proc_macro2::TokenTree::Group(group) => {
            group.stream().into_iter().any(|t| names_ident(&t, name))
        }
        _ => false,
    }
}

/// Whether `stmt` is a `missing_evidence(..)` method call.
fn calls_missing_evidence(stmt: &syn::Stmt) -> bool {
    matches!(stmt, syn::Stmt::Expr(syn::Expr::MethodCall(call), _) if call.method == "missing_evidence")
}

impl<'ast> syn::visit::Visit<'ast> for Scan<'_> {
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        use syn::spanned::Spanned;
        let start = item.sig.span().start();
        let end = item.block.brace_token.span.close().end();
        self.function(item.sig.ident.to_string(), start, end, |scan| {
            syn::visit::visit_item_fn(scan, item);
        });
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        use syn::spanned::Spanned;
        let start = item.sig.span().start();
        let end = item.block.brace_token.span.close().end();
        self.function(item.sig.ident.to_string(), start, end, |scan| {
            syn::visit::visit_impl_item_fn(scan, item);
        });
    }

    fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
        // The verdict types' definitions, attributes included, are pinned:
        // a derive or `cfg_attr` can generate a constructor the scan can't
        // see.
        if VERDICT_TYPES.contains(&item.ident.to_string().as_str()) {
            use syn::spanned::Spanned;
            let span = item.span();
            self.verdict
                .push((span.start().line, self.source(span.start(), span.end())));
        }
        syn::visit::visit_item_enum(self, item);
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let name = type_name(&item.self_ty).unwrap_or_else(|| "impl".to_owned());
        // An impl for a type that names a verdict type without being one,
        // such as `Identity<Verdict>`, or for a projection, could be an impl
        // for it the pin can't see.
        {
            use syn::spanned::Spanned;
            let span = item.self_ty.span();
            let written = self.source(span.start(), span.end());
            let names_verdict = written
                .parse::<proc_macro2::TokenStream>()
                .is_ok_and(|tokens| {
                    tokens
                        .into_iter()
                        .any(|token| VERDICT_TYPES.iter().any(|v| names_ident(&token, v)))
                });
            let projection =
                matches!(&*item.self_ty, syn::Type::Path(path) if path.qself.is_some());
            if (names_verdict && !VERDICT_TYPES.contains(&name.as_str())) || projection {
                self.errors.push(format!(
                    "{}: an impl for {written} could be one for a verdict type",
                    self.file
                ));
            }
        }
        // Every `impl` block for a verdict type is pinned with its
        // definition. Aliases of the types are refused, so the name the
        // impl is written for is the type's own.
        if VERDICT_TYPES.contains(&name.as_str()) {
            use syn::spanned::Spanned;
            let span = item.span();
            self.verdict
                .push((span.start().line, self.source(span.start(), span.end())));
        }
        self.scope.push(name);
        syn::visit::visit_item_impl(self, item);
        self.scope.pop();
    }

    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        // A module loaded from a `#[path]` file must be a file the scan
        // reads ([`crate_sources`] follows each one).
        if item.content.is_none() {
            if let Some(target) = path_target(self.file, item) {
                if !self.files.contains(&target) {
                    self.errors.push(format!(
                        "{}: module {} loads {target}, which the scan doesn't read",
                        self.file, item.ident
                    ));
                }
            } else if !self.file.starts_with("src/") {
                // The scan follows a file outside `src/` only through its
                // `#[path]`, so a module it declares is one it doesn't read.
                self.errors.push(format!(
                    "{}: module {} is outside the files the scan reads",
                    self.file, item.ident
                ));
            }
            // A `path` chosen through `cfg_attr` loads a file the scan
            // can't pick.
            let conditional_path = item.attrs.iter().any(|attr| {
                attr.path().is_ident("cfg_attr")
                    && attr.meta.require_list().is_ok_and(|list| {
                        list.tokens
                            .clone()
                            .into_iter()
                            .any(|token| names_ident(&token, "path"))
                    })
            });
            if conditional_path {
                self.errors.push(format!(
                    "{}: module {} picks its file through cfg_attr",
                    self.file, item.ident
                ));
            }
        }
        // Test modules build fixtures, not reasons.
        let test = item.attrs.iter().any(|attr| {
            attr.path().is_ident("cfg")
                && attr
                    .meta
                    .require_list()
                    .is_ok_and(|list| list.tokens.to_string() == "test")
        });
        if !test {
            self.scope.push(item.ident.to_string());
            self.modules.push(item.ident.to_string());
            syn::visit::visit_item_mod(self, item);
            self.modules.pop();
            self.scope.pop();
        }
    }

    fn visit_block(&mut self, block: &'ast syn::Block) {
        let outer = self.after_missing;
        for (at, stmt) in block.stmts.iter().enumerate() {
            self.after_missing = at > 0 && calls_missing_evidence(&block.stmts[at - 1]);
            self.visit_stmt(stmt);
        }
        self.after_missing = outer;
    }

    fn visit_expr(&mut self, expr: &'ast syn::Expr) {
        match expr {
            syn::Expr::Path(path) if Self::names_variant(&path.path) => {
                let span = path.path.segments.last().map(|s| s.ident.span()).unwrap();
                self.construction(span);
            }
            syn::Expr::Struct(construct) if Self::names_variant(&construct.path) => {
                let span = construct
                    .path
                    .segments
                    .last()
                    .map(|s| s.ident.span())
                    .unwrap();
                self.construction(span);
                syn::visit::visit_expr_struct(self, construct);
            }
            // A comparison reads the variant; it doesn't construct it.
            syn::Expr::Binary(binary)
                if matches!(binary.op, syn::BinOp::Eq(_) | syn::BinOp::Ne(_)) =>
            {
                for side in [&*binary.left, &*binary.right] {
                    if !matches!(side, syn::Expr::Path(p) if Self::names_variant(&p.path)) {
                        self.visit_expr(side);
                    }
                }
            }
            _ => syn::visit::visit_expr(self, expr),
        }
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        // Included source isn't a file the scan reads, so it can't be
        // inventoried.
        if mac
            .path
            .segments
            .last()
            .is_some_and(|s| s.ident == "include")
        {
            self.errors.push(format!(
                "{}: include! hides source from the scan",
                self.file
            ));
        }
        self.check_macro(mac);
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        if renames_verdict(&item.tree, false) {
            self.errors
                .push(format!("{}: a use item renames a verdict type", self.file));
        }
        if self.prints.is_empty() {
            use syn::spanned::Spanned;
            let span = item.span();
            let text = self.source(span.start(), span.end());
            self.uses.push((self.modules.join("::"), text));
        }
        syn::visit::visit_item_use(self, item);
    }

    fn visit_use_rename(&mut self, rename: &'ast syn::UseRename) {
        let name = rename.ident.to_string();
        if name == "NotApplicable" || VERDICT_TYPES.contains(&name.as_str()) {
            self.errors.push(format!(
                "{}: {name} renamed to {}",
                self.file, rename.rename
            ));
        }
    }

    fn visit_impl_item_type(&mut self, item: &'ast syn::ImplItemType) {
        // An associated type set to a verdict type is an alias of it.
        if type_name(&item.ty).is_some_and(|name| VERDICT_TYPES.contains(&name.as_str())) {
            self.errors.push(format!(
                "{}: associated type {} aliases a verdict type",
                self.file, item.ident
            ));
        }
        syn::visit::visit_impl_item_type(self, item);
    }

    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        // A type alias of a verdict type would hide an impl for it from the
        // pin, so none is allowed.
        {
            if type_name(&item.ty).is_some_and(|name| VERDICT_TYPES.contains(&name.as_str())) {
                self.errors.push(format!(
                    "{}: {} aliases a verdict type",
                    self.file, item.ident
                ));
            }
        }
        syn::visit::visit_item_type(self, item);
    }
}

/// What [`scan`] read from the crate's sources.
struct Scanned {
    /// Every construction.
    found: Vec<Construction>,
    /// Every error the scan hit.
    errors: Vec<String>,
    /// Every function's text, by `(file, qualified function)`, with the
    /// module it sits in.
    functions: BTreeMap<(String, String), (String, String)>,
    /// Every module-scope `use` item's text, by `(file, module)`.
    uses: BTreeMap<(String, String), Vec<String>>,
}

/// Every construction under `src/` and every error the scan hit, with
/// each function's text and each module's `use` items, from the files in
/// `sources` as `(path relative to the crate, text)`.
///
/// It also checks the verdict types' pin: the [`fingerprint`] of the
/// [`VERDICT_TYPES`]' definitions and every `impl` block for them, in file
/// and line order, must be [`reasons::VERDICT_FINGERPRINT`].
fn scan(sources: &[(String, String)]) -> Scanned {
    let mut found = Vec::new();
    let mut errors = Vec::new();
    let mut verdict = Vec::new();
    let mut functions = BTreeMap::new();
    let mut uses: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    let files: BTreeSet<String> = sources.iter().map(|(file, _)| file.clone()).collect();
    for (file, text) in sources {
        let parsed = match syn::parse_file(text) {
            Ok(parsed) => parsed,
            Err(error) => {
                errors.push(format!("{file}: {error}"));
                continue;
            }
        };
        let mut scan = Scan {
            file,
            files: &files,
            lines: text.lines().collect(),
            scope: Vec::new(),
            prints: Vec::new(),
            verdict: Vec::new(),
            after_missing: false,
            modules: Vec::new(),
            found: Vec::new(),
            errors: Vec::new(),
            functions: Vec::new(),
            uses: Vec::new(),
        };
        syn::visit::Visit::visit_file(&mut scan, &parsed);
        found.extend(scan.found);
        errors.extend(scan.errors);
        for (function, module, text) in scan.functions {
            functions.insert((file.clone(), function), (module, text));
        }
        for (module, text) in scan.uses {
            uses.entry((file.clone(), module)).or_default().push(text);
        }
        verdict.extend(
            scan.verdict
                .into_iter()
                .map(|(line, text)| (file.clone(), line, text)),
        );
    }
    verdict.sort();
    let pinned = fingerprint(
        &verdict
            .iter()
            .map(|(_, _, text)| text.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
    );
    if pinned != reasons::VERDICT_FINGERPRINT {
        errors.push(format!(
            "the verdict types' definitions or impls changed: reread every NotApplicable meaning in reasons.rs, then update VERDICT_FINGERPRINT from {} to {pinned}",
            reasons::VERDICT_FINGERPRINT
        ));
    }
    Scanned {
        found,
        errors,
        functions,
        uses,
    }
}

/// The types whose `NotApplicable` is a reason: their definitions and
/// impls are pinned together by [`reasons::VERDICT_FINGERPRINT`].
const VERDICT_TYPES: [&str; 2] = ["Verdict", "PropertyOutcome"];

/// A function's or a type's fingerprint (C20): the first 16 hex digits of
/// the SHA-256 of `text`'s token stream, so comments and whitespace don't
/// count and any other edit does. [`reasons::NA_REASONS`] pins each
/// producer's, so a producer's meanings are reread whenever it changes. A
/// function it calls is that function's owner's contract (Rule 23), so
/// only the producer's own text counts.
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

/// Every identifier `text` uses as a path segment, followed by `::` or
/// anywhere in a `use` item, groups and renames included, each with the
/// trimmed source line it's on, once per use. A field or a local of the
/// same name isn't a path, and comments aren't tokens.
fn idents(text: &str) -> BTreeMap<String, Vec<String>> {
    /// Collects `stream`'s path identifiers into `out`, all of them inside
    /// a `use` item when `in_use`.
    fn walk(
        stream: proc_macro2::TokenStream,
        in_use: bool,
        lines: &[&str],
        out: &mut BTreeMap<String, Vec<String>>,
    ) {
        use proc_macro2::{Spacing, TokenTree};
        let tokens: Vec<TokenTree> = stream.into_iter().collect();
        let mut using = in_use;
        for (at, token) in tokens.iter().enumerate() {
            match token {
                TokenTree::Ident(ident) => {
                    let path = matches!(
                        (tokens.get(at + 1), tokens.get(at + 2)),
                        (Some(TokenTree::Punct(a)), Some(TokenTree::Punct(b)))
                            if a.as_char() == ':' && a.spacing() == Spacing::Joint && b.as_char() == ':'
                    );
                    if using || path {
                        let line = lines
                            .get(ident.span().start().line.saturating_sub(1))
                            .map(|line| line.trim().to_owned())
                            .unwrap_or_default();
                        out.entry(ident.to_string()).or_default().push(line);
                    }
                    if ident == "use" {
                        using = true;
                    }
                }
                TokenTree::Group(group) => walk(group.stream(), using, lines, out),
                TokenTree::Punct(punct) if punct.as_char() == ';' && !in_use => using = false,
                _ => {}
            }
        }
    }
    let lines: Vec<&str> = text.lines().collect();
    let mut out = BTreeMap::new();
    if let Ok(tokens) = text.parse::<proc_macro2::TokenStream>() {
        walk(tokens, false, &lines, &mut out);
    }
    out
}

/// Every `.rs` file under the crate's `src/`, as `(path, text)`.
fn crate_sources() -> Vec<(String, String)> {
    /// Collects `.rs` files under `dir`.
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned();
                out.push((relative, std::fs::read_to_string(&path).unwrap()));
            }
        }
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    walk(root, &root.join("src"), &mut sources);
    // A `#[path]` module can load a file outside `src/`, so each one's
    // target is read too, until no new file turns up.
    let mut at = 0;
    while at < sources.len() {
        let (file, text) = sources[at].clone();
        for target in path_targets(&file, &text) {
            if !sources.iter().any(|(known, _)| *known == target) {
                if let Ok(text) = std::fs::read_to_string(root.join(&target)) {
                    sources.push((target, text));
                }
            }
        }
        at += 1;
    }
    sources.sort();
    sources
}

/// The files `text`'s `#[path]` modules load, relative to the crate, as
/// [`path_target`] resolves them.
fn path_targets(file: &str, text: &str) -> Vec<String> {
    /// Collects each file-backed module's `#[path]` target.
    struct Targets<'a> {
        file: &'a str,
        out: Vec<String>,
    }
    impl<'ast> syn::visit::Visit<'ast> for Targets<'_> {
        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if item.content.is_none() {
                self.out.extend(path_target(self.file, item));
            }
            syn::visit::visit_item_mod(self, item);
        }
    }
    let Ok(parsed) = syn::parse_file(text) else {
        return Vec::new();
    };
    let mut targets = Targets {
        file,
        out: Vec::new(),
    };
    syn::visit::Visit::visit_file(&mut targets, &parsed);
    targets.out
}

/// The file module `item`, declared in `file`, loads through its
/// `#[path = "..."]`, relative to the crate: the attribute's path taken
/// from `file`'s directory, `..` resolved. `None` without the attribute.
fn path_target(file: &str, item: &syn::ItemMod) -> Option<String> {
    let value = item.attrs.iter().find_map(|attr| match &attr.meta {
        syn::Meta::NameValue(pair) if pair.path.is_ident("path") => match &pair.value {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(path),
                ..
            }) => Some(path.value()),
            _ => None,
        },
        _ => None,
    })?;
    let mut parts: Vec<String> = Path::new(file)
        .parent()
        .into_iter()
        .flat_map(|dir| dir.components())
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .collect();
    for part in Path::new(&value).components() {
        match part {
            std::path::Component::ParentDir => {
                parts.pop();
            }
            std::path::Component::CurDir => {}
            other => parts.push(other.as_os_str().to_string_lossy().into_owned()),
        }
    }
    Some(parts.join("/"))
}

/// The path identifiers of all header-side text ([`reasons::Basis::NotNamedByHeader`]),
/// each by where it is: every file under [`reasons::HEADER_SIDE`], whole,
/// and for each [`NA_REASONS`] producer elsewhere, its own text and its
/// module scope's `use` items.
fn header_side_idents(
    sources: &[(String, String)],
    functions: &BTreeMap<(String, String), (String, String)>,
    uses: &BTreeMap<(String, String), Vec<String>>,
) -> Vec<(String, BTreeMap<String, Vec<String>>)> {
    let header_file = |file: &str| {
        reasons::HEADER_SIDE
            .iter()
            .any(|prefix| file.starts_with(prefix))
    };
    let mut side: Vec<(String, BTreeMap<String, Vec<String>>)> = sources
        .iter()
        .filter(|(file, _)| header_file(file))
        .map(|(file, text)| (file.clone(), idents(text)))
        .collect();
    for producer in NA_REASONS.iter().filter(|p| !header_file(p.file)) {
        let at = (producer.file.to_owned(), producer.function.to_owned());
        // A listed producer the scan doesn't find is its own error.
        let Some((module, text)) = functions.get(&at) else {
            continue;
        };
        side.push((
            format!("{} {}", producer.file, producer.function),
            idents(text),
        ));
        for item in uses
            .get(&(producer.file.to_owned(), module.clone()))
            .into_iter()
            .flatten()
        {
            side.push((
                format!("{}'s module-scope use items", producer.file),
                idents(item),
            ));
        }
    }
    side
}

/// Constructions by `(file, function)`, each as `(line, fingerprint,
/// after_missing)` in source order.
type Producers = BTreeMap<(String, String), Vec<(String, String, bool)>>;

/// Every difference between a scan of `sources` and the inventory, with
/// the scan's own errors: a construction in a header-reachable file that
/// no entry lists at its function and position, a listed one the scan
/// doesn't find there, a producer whose text changed since its meanings
/// were read, a [`NOT_HEADER`] file the inventory also lists, and one whose
/// [`reasons::Basis`] doesn't hold.
fn scan_errors(sources: &[(String, String)]) -> Vec<String> {
    let Scanned {
        found,
        mut errors,
        functions,
        uses,
    } = scan(sources);
    let not_header: BTreeSet<&str> = NOT_HEADER.iter().map(|n| n.file).collect();
    let mut by_function: Producers = BTreeMap::new();
    for found in found
        .into_iter()
        .filter(|f| !not_header.contains(f.file.as_str()))
    {
        by_function
            .entry((found.file, found.function))
            .or_default()
            .push((found.line, found.fingerprint, found.after_missing));
    }
    let listed: Producers = NA_REASONS
        .iter()
        .map(|producer| {
            (
                (producer.file.to_owned(), producer.function.to_owned()),
                producer
                    .constructions
                    .iter()
                    .map(|c| {
                        (
                            c.line.to_owned(),
                            producer.fingerprint.to_owned(),
                            c.kind == reasons::NaKind::MissingEvidence,
                        )
                    })
                    .collect(),
            )
        })
        .collect();
    // Each excluded file is excluded on a basis the scan checks: no
    // header-side text names its module, or it names no `Verdict` or
    // `PropertyOutcome`.
    let header_side = header_side_idents(sources, &functions, &uses);
    for excluded in NOT_HEADER {
        match excluded.basis {
            reasons::Basis::NotNamedByHeader => {
                for (place, idents) in &header_side {
                    for line in idents.get(excluded.module).into_iter().flatten() {
                        errors.push(format!(
                            "{place} names {}, which NOT_HEADER excludes: {line}",
                            excluded.module
                        ));
                    }
                }
            }
            reasons::Basis::NamesNoVerdict => {
                let names = sources.iter().any(|(file, text)| {
                    file == excluded.file
                        && text
                            .parse::<proc_macro2::TokenStream>()
                            .is_ok_and(|tokens| {
                                tokens.into_iter().any(|token| {
                                    names_ident(&token, "Verdict")
                                        || names_ident(&token, "PropertyOutcome")
                                })
                            })
                });
                if names {
                    errors.push(format!("{} names a verdict type", excluded.file));
                }
            }
        }
    }
    if listed.len() != NA_REASONS.len() {
        errors.push("a producer is listed twice".into());
    }
    // A producer's constructions without their fingerprints, to tell a
    // changed text from a changed construction.
    let shape = |lines: &[(String, String, bool)]| -> Vec<(String, bool)> {
        lines
            .iter()
            .map(|(line, _, missing)| (line.clone(), *missing))
            .collect()
    };
    for (at, lines) in &by_function {
        match listed.get(at) {
            Some(listed) if listed == lines => {}
            Some(listed) if shape(listed) == shape(lines) => errors.push(format!(
                "{} {} changed: reread the meanings of its NotApplicable constructions in reasons.rs, then update its fingerprint from {} to {}",
                at.0,
                at.1,
                listed.first().map_or("", |(_, print, _)| print.as_str()),
                lines.first().map_or("", |(_, print, _)| print.as_str()),
            )),
            Some(listed) => errors.push(format!("{at:?}: found {lines:?}, listed {listed:?}")),
            None => errors.push(format!("{at:?}: found {lines:?}, unlisted")),
        }
    }
    for at in listed.keys() {
        if !by_function.contains_key(at) {
            errors.push(format!("{at:?}: listed, not found"));
        }
        if not_header.contains(at.0.as_str()) {
            errors.push(format!("{at:?}: listed in a file NOT_HEADER names"));
        }
    }
    errors
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

#[test]
fn the_not_applicable_inventory_equals_a_source_scan() {
    let sources = crate_sources();
    assert_eq!(scan_errors(&sources), Vec::<String>::new());
    for excluded in NOT_HEADER {
        assert!(
            sources.iter().any(|(f, _)| f == excluded.file),
            "{} doesn't exist",
            excluded.file
        );
    }
    // A prefix that matches nothing, after a rename, would leave header
    // files outside the header-side text.
    for prefix in reasons::HEADER_SIDE {
        assert!(
            sources.iter().any(|(f, _)| f.starts_with(prefix)),
            "no file is under HEADER_SIDE's {prefix}"
        );
    }
}

/// `sources` with `extra` appended to `file`, or with `from` replaced by
/// `to` in it.
fn edited(
    sources: &[(String, String)],
    file: &str,
    edit: &dyn Fn(&str) -> String,
) -> Vec<(String, String)> {
    sources
        .iter()
        .map(|(name, text)| {
            (
                name.clone(),
                if name == file {
                    edit(text)
                } else {
                    text.clone()
                },
            )
        })
        .collect()
}

/// One defect at a time fails the scan: a new producer, a second
/// construction in a listed function, a construction moved to a same-named
/// function in another file, a construction hidden after a `//` in a string,
/// an alias, an unknown macro, and a construction swapped for another line
/// at the same count. A comparison, a pattern and a comment aren't
/// constructions.
#[test]
fn the_source_scan_refuses_an_unlisted_producer() {
    let sources = crate_sources();
    let header = "src/domain/header_sync.rs";
    let refused = |changed: Vec<(String, String)>, what: &str| {
        assert!(!scan_errors(&changed).is_empty(), "{what}");
    };
    refused(
        edited(&sources, header, &|t| {
            format!("{t}\nfn check_new<S>() -> Verdict {{\n    Verdict::NotApplicable\n}}\n")
        }),
        "new producer",
    );
    refused(
        edited(&sources, header, &|t| {
            t.replacen(
                "        Verdict::NotApplicable\n",
                "        let _ = Verdict::NotApplicable;\n        Verdict::NotApplicable\n",
                1,
            )
        }),
        "second construction",
    );
    refused(
        edited(&sources, "src/table.rs", &|t| {
            format!("{t}\nfn check_teardown() -> Verdict {{\n    Verdict::NotApplicable\n}}\n")
        }),
        "same function name in another file",
    );
    refused(
        edited(&sources, header, &|t| {
            format!(
                "{t}\nfn check_url() -> Verdict {{\n    (\"https://x.invalid\", Verdict::NotApplicable).1\n}}\n"
            )
        }),
        "a construction after a // inside a string",
    );
    refused(
        edited(&sources, header, &|t| {
            format!("{t}\nuse crate::property::Verdict::NotApplicable as Na;\n")
        }),
        "an alias",
    );
    refused(
        edited(&sources, header, &|t| {
            format!(
                "{t}\nfn check_vec() -> Vec<Verdict> {{\n    vec![Verdict::NotApplicable]\n}}\n"
            )
        }),
        "an unknown macro",
    );
    refused(
        edited(&sources, header, &|t| {
            t.replacen(
                "        _ => Verdict::NotApplicable,\n",
                "        _ => { Verdict::NotApplicable }\n",
                1,
            )
        }),
        "a construction on another line at the same count",
    );
    for (extra, what) in [
        (
            "use crate::property::Verdict as V;\nfn check_alias() -> V {\n    V::NotApplicable\n}\n",
            "an enum alias",
        ),
        (
            "fn check_struct() -> Verdict {\n    Verdict::NotApplicable {}\n}\n",
            "a struct-literal constructor",
        ),
        (
            "fn check_guard(v: bool) -> Verdict {\n    matches!(Some(()), Some(()) if { if v { return Verdict::NotApplicable; } true });\n    Verdict::Holds\n}\n",
            "a return in an allowed macro's guard",
        ),
        (
            "fn check_replace(mut verdict: Verdict) -> Verdict {\n    assert_eq!(std::mem::replace(&mut verdict, Verdict::NotApplicable), Verdict::Holds);\n    verdict\n}\n",
            "a construction in an assertion's argument",
        ),
        (
            "fn check_try(input: Option<bool>) -> Result<bool, Verdict> {\n    Ok(matches!(input.ok_or(Verdict::NotApplicable)?, true))\n}\n",
            "a construction propagated out of a pattern macro's scrutinee",
        ),
        (
            "fn check_opaque() {\n    assert!(Verdict::NotApplicable => 1);\n}\n",
            "a known macro that doesn't parse",
        ),
        (
            "fn check_break(ready: bool) -> Verdict {\n    'done: {\n        let _ = matches!((), () if ready || (break 'done Verdict::NotApplicable));\n        Verdict::Holds\n    }\n}\n",
            "a construction broken out of an allowed macro's guard",
        ),
        (
            "use crate::domain::shared::{bounds};\n",
            "a grouped import of an excluded module",
        ),
        (
            "use crate::domain::shared;\nfn check_module() {\n    let _ = shared::bounds::bound::<(), ()>;\n}\n",
            "an excluded module reached through its parent's import",
        ),
        (
            "fn check_reach() {\n    let _ = crate::domain::shared::bounds::bound;\n}\n",
            "a header file naming an excluded module",
        ),
        (
            "use crate::domain::discovery as found;\n",
            "a header file aliasing an excluded module",
        ),
        (
            "fn check_rows() {\n    let _ = crate::domain::block_sync::BUDGET_LEDGERS_AGREE;\n}\n",
            "a header file naming block sync's checks",
        ),
    ] {
        refused(edited(&sources, header, &|t| format!("{t}\n{extra}")), what);
    }
    // A shared producer's own text naming an excluded module, and an
    // import in its module scope redirected to one, its text unchanged,
    // each refused as naming it, not only by a changed fingerprint.
    let names_excluded = |changed: Vec<(String, String)>, what: &str| {
        let errors = scan_errors(&changed);
        assert!(
            errors
                .iter()
                .any(|e| e.contains("which NOT_HEADER excludes")),
            "{what}: {errors:?}"
        );
    };
    names_excluded(
        edited(&sources, "src/harness/mod.rs", &|t| {
            t.replacen(
                "        Phase::Drain => Verdict::NotApplicable,\n",
                "        Phase::Drain => {\n            let _ = block_sync::Storage::NAME;\n            Verdict::NotApplicable\n        }\n",
                1,
            )
        }),
        "a shared producer naming an excluded module",
    );
    names_excluded(
        edited(&sources, "src/table.rs", &|t| {
            format!("{t}\nuse crate::domain::block_sync::check_ledgers as judged;\n")
        }),
        "a shared producer's module-scope import of an excluded module",
    );
    // The evaluator starting to name a verdict type.
    refused(
        edited(
            &sources,
            "src/harness/header_acceptance/evaluate.rs",
            &|t| format!("{t}\nuse crate::property::Verdict;\n"),
        ),
        "an excluded file losing its basis",
    );
    // A construction's outer condition changed, its innermost one and its
    // line kept.
    refused(
        edited(&sources, "src/domain/header_sync_progress.rs", &|t| {
            t.replacen(
                "    if !read_now {\n        return Verdict::NotApplicable;\n    }",
                "    if cx.step > 0 {\n        if !read_now {\n            return Verdict::NotApplicable;\n        }\n    }",
                1,
            )
        }),
        "an outer condition added",
    );
    // A matched name bound to another value, its match and arm kept.
    refused(
        edited(&sources, "src/domain/header_sync.rs", &|t| {
            t.replacen(
                "let boundary = view.capacity().boundary(step);",
                "let boundary = view.capacity().boundary(step.saturating_sub(1));",
                1,
            )
        }),
        "a matched name bound to another value",
    );
    // A `let ... else` initializer changed, its inner branch and line kept.
    refused(
        edited(&sources, "src/harness/machine.rs", &|t| {
            t.replacen(
                "let Some(failure) = step.failure() else {",
                "let Some(failure) = step.failure().filter(|failure| failure.property_failure) else {",
                1,
            )
        }),
        "a let-else initializer changed",
    );
    // NotApplicable made a derived default, which generates a constructor
    // the scan can't see.
    refused(
        edited(&sources, "src/property/verdict.rs", &|t| {
            t.replacen(
                "    NotApplicable,\n",
                "    #[default]\n    NotApplicable,\n",
                1,
            )
        }),
        "a derived-default NotApplicable",
    );
    // A match arm's construction answering another matched value.
    refused(
        edited(&sources, "src/domain/header_sync.rs", &|t| {
            t.replacen(
                "    match boundary {\n        Some(BoundaryRead::Read) => Verdict::Holds,",
                "    match view.capacity().boundary(step.saturating_sub(1)) {\n        Some(BoundaryRead::Read) => Verdict::Holds,",
                1,
            )
        }),
        "a match on another value",
    );
    // A re-export in a shared producer's module scope, which its producer
    // could call.
    names_excluded(
        edited(&sources, "src/harness/mod.rs", &|t| {
            format!("{t}\npub use block_sync::Storage as ReexportedStorage;\n")
        }),
        "a re-export in a shared producer's module scope",
    );
    // One of two identical returns given another trigger.
    refused(
        edited(&sources, "src/domain/header_sync_progress.rs", &|t| {
            t.replacen("if first != Some(cx.step) {", "if first.is_none() {", 1)
        }),
        "an identical return's trigger changed",
    );
    // Moving missing_evidence between two identical returns changes their
    // kinds, which the inventory's kinds must follow.
    refused(
        edited(&sources, "src/domain/header_sync_progress.rs", &|t| {
            t.replacen(
                "    if first != Some(cx.step) {\n        return Verdict::NotApplicable;",
                "    if first != Some(cx.step) {\n        cx.missing_evidence(\"moved\");\n        return Verdict::NotApplicable;",
                1,
            )
        }),
        "missing_evidence moved before another identical line",
    );
    // The C20 fingerprint rule: any edit to a producer's own text changes
    // its fingerprint, and any change to `Verdict` or its impls changes the
    // type's. Each of round 6's counterexamples is refused.
    let replaced = |file: &str, from: &'static str, to: &'static str| {
        let from = from.to_owned();
        let to = to.to_owned();
        edited(&sources, file, &move |t| {
            assert!(t.contains(&from), "{from}");
            t.replacen(&from, &to, 1)
        })
    };
    for (changed, what) in [
        (
            replaced(
                header,
                "        Some(BoundaryRead::Read) | None => {}\n",
                "        Some(BoundaryRead::Read) if step % 2 == 0 => {}\n        Some(BoundaryRead::Read) | None => {}\n",
            ),
            "an earlier arm's guard in check_chunk_budget_bound",
        ),
        (
            replaced(
                "src/property/registry.rs",
                "None if state.applicable > 0 =>",
                "None if state.applicable > 1 =>",
            ),
            "the registry's applicability guard before its fallback arm",
        ),
        (
            replaced(
                "src/domain/header_sync_progress.rs",
                "    if !read_now {\n        return Verdict::NotApplicable;",
                "    let read_now = read_now && cx.step > 3;\n    if !read_now {\n        return Verdict::NotApplicable;",
            ),
            "a recorded condition's name rebound after its initializer",
        ),
        (
            replaced(
                "src/property/verdict.rs",
                "    /// The check didn't apply at this step.\n    NotApplicable,",
                "    /// The check didn't apply at this step.\n    #[cfg_attr(zakura_sim, default)]\n    NotApplicable,",
            ),
            "a conditional default on NotApplicable",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\nimpl Default for crate::property::Verdict {{\n    fn default() -> Self {{\n        Self::Holds\n    }}\n}}\n")
            }),
            "a new impl block for Verdict",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\nuse crate::property::Verdict as V;\nimpl Default for V {{\n    fn default() -> Self {{\n        Self::Holds\n    }}\n}}\n")
            }),
            "an impl for Verdict through an import alias",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\ntype V = crate::property::Verdict;\nimpl Default for V {{\n    fn default() -> Self {{\n        Self::Holds\n    }}\n}}\n")
            }),
            "an impl for Verdict through a type alias",
        ),
        (
            edited(&sources, header, &|t| format!("{t}\ninclude!(\"checks.inc\");\n")),
            "an included source",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\nuse crate::property::Verdict::{{self as V}};\nimpl Default for V {{\n    fn default() -> Self {{\n        Self::Holds\n    }}\n}}\n")
            }),
            "an impl for Verdict through a grouped self rename",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\nimpl Default for (crate::property::Verdict) {{\n    fn default() -> Self {{\n        Self::Holds\n    }}\n}}\n")
            }),
            "an impl for a parenthesized Verdict",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\ntype V = (crate::property::Verdict);\nimpl Default for V {{\n    fn default() -> Self {{\n        Self::Holds\n    }}\n}}\n")
            }),
            "an impl through a parenthesized type alias",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\nmacro_rules! default_of {{\n    ($t:ty) => {{\n        impl Default for $t {{\n            fn default() -> Self {{\n                Self::Holds\n            }}\n        }}\n    }};\n}}\ndefault_of!(crate::property::Verdict);\n")
            }),
            "an impl a macro generates for Verdict",
        ),
        (
            edited(&sources, "src/lib.rs", &|t| {
                format!("{t}\n#[path = \"../outside/checks.rs\"]\nmod outside_checks;\n")
            }),
            "a module loaded from a file the scan doesn't read",
        ),
        (
            edited(&sources, "src/lib.rs", &|t| {
                format!("{t}\n#[cfg_attr(zakura_sim, path = \"../outside/checks.rs\")]\nmod outside_checks;\n")
            }),
            "a module whose file cfg_attr picks",
        ),
        (
            edited(&sources, "build/provenance.rs", &|t| format!("{t}\nmod detail;\n")),
            "a module an out-of-tree file declares",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\ntype Identity<T> = T;\nimpl Default for Identity<crate::property::Verdict> {{\n    fn default() -> Self {{\n        Self::Holds\n    }}\n}}\n")
            }),
            "an impl through a generic identity alias",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\ntrait Id {{\n    type Output;\n}}\nstruct Marker;\nimpl Id for Marker {{\n    type Output = crate::property::Verdict;\n}}\nimpl Default for <Marker as Id>::Output {{\n    fn default() -> Self {{\n        Self::Holds\n    }}\n}}\n")
            }),
            "an impl through an associated type",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\nfn check_hidden() {{\n    assert!({{\n        impl Default for crate::property::Verdict {{\n            fn default() -> Self {{\n                Self::Holds\n            }}\n        }}\n        true\n    }});\n}}\n")
            }),
            "an impl inside a value macro's argument",
        ),
        (
            edited(&sources, "src/check.rs", &|t| {
                t.replacen(
                    "pub enum PropertyOutcome {",
                    "#[derive(Default)]\npub enum PropertyOutcome {",
                    1,
                )
            }),
            "a derive on PropertyOutcome",
        ),
    ] {
        let errors = scan_errors(&changed);
        assert!(!errors.is_empty(), "{what}");
    }
    // A producer whose text changed but whose constructions didn't says
    // what to reread.
    let errors = scan_errors(&replaced(
        "src/property/registry.rs",
        "None if state.applicable > 0 =>",
        "None if state.applicable > 1 =>",
    ));
    assert!(
        errors.iter().any(|e| e.starts_with(
            "src/property/registry.rs outcome changed: reread the meanings of its NotApplicable constructions in reasons.rs"
        )),
        "{errors:?}"
    );
    // A comment or whitespace in a producer isn't an edit.
    assert_eq!(
        scan_errors(&replaced(
            "src/property/registry.rs",
            "None if state.applicable > 0 =>",
            "None if  state.applicable > 0 /* applied */ =>",
        )),
        Vec::<String>::new()
    );

    // The reachable sender helper's entry removed from the inventory is
    // refused too, since downstream.rs isn't a file NOT_HEADER names.
    let found = scan(&sources).found;
    assert!(found
        .iter()
        .any(|f| f.file == "src/downstream.rs" && f.function == "check_with"));
    for (changed, what) in [
        (
            edited(&sources, header, &|t| {
                format!(
                    "{t}\nfn check_cmp(v: Verdict) -> bool {{\n    v == Verdict::NotApplicable\n}}\n"
                )
            }),
            "a comparison",
        ),
        (
            edited(&sources, header, &|t| {
                format!(
                    "{t}\nfn check_pat(v: Verdict) -> bool {{\n    matches!(v, Verdict::NotApplicable)\n}}\n"
                )
            }),
            "a pattern macro",
        ),
        (
            edited(&sources, header, &|t| {
                format!("{t}\n// Verdict::NotApplicable\n")
            }),
            "a comment",
        ),
        // Other teams naming their own modules (Evan's examples) don't touch
        // the scan: an unrelated same-named module, a new coverage row, and
        // a reference in a shared producer's file outside the producer and
        // its imports.
        (
            edited(&sources, "src/peer/discovery/content.rs", &|t| {
                format!("{t}\nfn unix_now_again() {{\n    let _ = crate::peer::discovery::unix_now;\n}}\n")
            }),
            "an unrelated module of an excluded module's name",
        ),
        (
            edited(&sources, "src/harness/coverage.rs", &|t| {
                format!("{t}\nfn another_row() {{\n    let _ = crate::domain::block_sync::BUDGET_LEDGERS_AGREE;\n}}\n")
            }),
            "a new coverage reference to block sync",
        ),
        (
            edited(&sources, "src/harness/mod.rs", &|t| {
                format!("{t}\nfn other_entry() {{\n    let _ = block_sync::Storage::NAME;\n}}\n")
            }),
            "a reference outside a shared file's producer",
        ),
        // The accepted gap (Rule 23): a helper in a shared file, which a
        // producer could later call, isn't followed until the producer's
        // own text changes.
        (
            edited(&sources, "src/table.rs", &|t| {
                format!(
                    "{t}\nfn helper() {{\n    let _ = crate::domain::block_sync::BlockSync;\n}}\n"
                )
            }),
            "a new helper in a shared file",
        ),
        (
            edited(&sources, "src/harness/mod.rs", &|t| {
                format!("{t}\nfn reach() {{\n    let _ = crate::domain::discovery::wait_complete_wakes::<()>;\n}}\n")
            }),
            "a call added in a shared file outside its producer",
        ),
    ] {
        assert_eq!(scan_errors(&changed), Vec::<String>::new(), "{what}");
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
/// Why `t/b` doesn't apply on [`HISTORY`].
const UNREACHED: &str = "unreached here";
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
                not_applicable: &[],
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
        ],
    }
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
        reason: Some(reason.into()),
    }
}

/// A test result for `test`, which ran `ran` with `outcome`, from a binary
/// embedding the subject's source.
fn test_actual(ran: TestRef, outcome: TestOutcome) -> Actual {
    Actual::Test {
        ran: vec![TestRan {
            package: ran.package.into(),
            file: ran.file.into(),
            test: ran.test.into(),
            outcome,
        }],
        binary_source: Some(SOURCE.into()),
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
        let mut run = execution(
            RunId::native(NATIVE, run),
            NATIVE_HARNESS,
            &[("t/s", held())],
        );
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
                    &[("t/a", held()), ("t/b", na(NEVER))],
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

#[test]
fn the_evaluator_accepts_genuine_results() {
    let manifest = manifest();
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

    // A catch stopped at a step is a prefix: another property may still be
    // open there.
    let mut pending = results();
    caught_runs(&mut pending, CASE).1.reports.insert(
        "t/b".into(),
        Report::Inconclusive {
            reason: "obligation-open".into(),
        },
    );
    assert_eq!(
        state(&manifest, &pending, &Key::catch("t/a")),
        ObligationState::Verified
    );
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
    // held instead; a missing or extra report; another property
    // Inconclusive; and a Draft override on a normal run.
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
            run.reports.insert(
                "t/b".into(),
                Report::Inconclusive {
                    reason: "teardown-open".into(),
                },
            );
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
            run.reports.insert("t/b".into(), na(UNREACHED));
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
    // Inconclusive or unexpectedly NotApplicable (rule 16).
    refused(&catch, |r| {
        caught_runs(r, CASE).2.reports.insert(
            "t/b".into(),
            Report::Inconclusive {
                reason: "obligation-open".into(),
            },
        );
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
    // A completed end-found catch with an unrelated Inconclusive or an
    // unexpected NotApplicable.
    refused(&late, |r| {
        caught_runs(r, END_CASE).1.reports.insert(
            "t/b".into(),
            Report::Inconclusive {
                reason: "obligation-open".into(),
            },
        );
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
    // or an empty reason its report repeats.
    let stays = Key::passing("t/b");
    for (empty_reason, empty_record) in [(false, true), (true, false)] {
        let mut m = manifest();
        let mut r = results();
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
        if empty_reason {
            if let Some(Report::NotApplicable { reason }) =
                run_of(&mut r, &stays_id()).reports.get_mut("t/b")
            {
                *reason = Some(String::new());
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
