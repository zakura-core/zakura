//! The planted catches' declared `NotApplicable` reports, judged through
//! the evaluator against genuine baseline reports (hdr-14.5 review). They
//! live apart from `header_acceptance.rs`, whose every test is a
//! composition check the inventories pin.
//!
//! Only baselines run here: a patched run needs the case's patched build.
//! The patched lists `not_applicable::PATCHED` declares come from each
//! case's recorded patched and `Draft` runs, so these tests check only that
//! the evaluator judges those runs by them.
#![cfg(zakura_sim)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use zakura_sim::harness::{
    self,
    header_acceptance::{
        self,
        contracts::RunBy,
        evaluate::{
            Actions, Actual, Companion, End, ExecMeta, Execution, Report, ResultFile, ResultRecord,
        },
        CaseRef, CaseRun, Expected, ExpectedNa, ExpectedResult, Gate, Key, Manifest,
        ObligationState, RunId,
    },
};

/// The subject commit the fixtures' results name.
const S: &str = "subject";
/// The subject's source digest.
const SOURCE: &str = "source-of-subject";
/// The subject as the caller established it.
const SUBJECT: header_acceptance::Subject<'static> = header_acceptance::Subject {
    commit: S,
    source: SOURCE,
};
/// `header-sync/wire`.
const WIRE: &str = "header-sync/wire";

/// The repository root.
fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every key the manifest's expected results cover, with their gates.
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

/// A completed execution of `run` on `harness`, reporting `reports`, run by
/// whoever its role says: a planted case's baseline and patched runs are
/// the planted runner's, its `Draft` run the tool's.
fn execution(run: RunId, harness: &str, reports: &[(String, Report)]) -> Execution {
    let run_by = match run {
        RunId::Case {
            run: CaseRun::Baseline | CaseRun::Patched,
            ..
        } => RunBy::Planted,
        _ => RunBy::Tool,
    };
    Execution {
        trace: format!("trace of {run:?}"),
        meta: ExecMeta {
            harness: harness.into(),
            draft_override: Vec::new(),
            run_by,
            input: None,
        },
        run,
        end: End::Completed,
        reports: reports.iter().cloned().collect(),
        trigger: true,
        leaked: 0,
        actions: Actions::Complete,
    }
}

/// Whether `state` blocks.
fn blocked(state: &ObligationState) -> bool {
    matches!(state, ObligationState::Blocked { .. })
}

/// The committed `Caught` of `case`.
fn caught(committed: &Manifest, case: CaseRef) -> ExpectedResult {
    committed
        .expected
        .iter()
        .find(|result| matches!(result.expected, Expected::Caught { case: c, .. } if c == case))
        .expect("a row names the case's catch")
        .clone()
}

/// `reports` with every member of `set` violated.
fn violating(reports: &[(String, Report)], set: &[&str]) -> Vec<(String, Report)> {
    reports
        .iter()
        .map(|(id, report)| {
            let report = if set.contains(&id.as_str()) {
                Report::Violated
            } else {
                report.clone()
            };
            (id.clone(), report)
        })
        .collect()
}

/// The state the evaluator gives `result`, an at-step wire `Caught`, on a
/// manifest of it alone, given its baseline's reports and its `Draft`
/// run's. Its patched run is the `Draft` run's reports stopped at the
/// break. The despite-close case's companions are shown held on the
/// `Draft` run.
fn judged(
    committed: &Manifest,
    result: &ExpectedResult,
    baseline: &[(String, Report)],
    draft: &[(String, Report)],
) -> ObligationState {
    use zakura_sim::harness::header_acceptance::companions;
    let Expected::Caught { case, set, .. } = result.expected else {
        unreachable!("judged takes a Caught");
    };
    let baseline = execution(RunId::case(case, CaseRun::Baseline), WIRE, baseline);
    let mut patched = execution(RunId::case(case, CaseRun::Patched), WIRE, draft);
    patched.end = End::Violation {
        property: case.property.into(),
    };
    patched.actions = Actions::Prefix;
    let mut draft = execution(RunId::case(case, CaseRun::Draft), WIRE, draft);
    draft.meta.draft_override = set.iter().map(|id| (*id).to_owned()).collect();
    let companions = if case == companions::DESPITE_CLOSE {
        companions::COMPANIONS
            .iter()
            .map(|name| Companion {
                name: (*name).into(),
                trace: draft.trace.clone(),
                run: draft.run.clone(),
                held: true,
            })
            .collect()
    } else {
        Vec::new()
    };
    let results = ResultFile {
        subject: S.into(),
        subject_source: SOURCE.into(),
        results: vec![ResultRecord {
            expected: result.expected.id().expect("a Caught has an identity"),
            commit: S.into(),
            actual: Actual::Caught {
                baseline,
                patched,
                draft,
                companions,
            },
        }],
    };
    let manifest = Manifest {
        expected: vec![result.clone()],
        contracts: Vec::new(),
        required_contracts: Vec::new(),
        narrowing: Vec::new(),
        machines: committed.machines.clone(),
        matrix: committed
            .matrix
            .iter()
            .filter(|(harness, _, _)| *harness == WIRE)
            .copied()
            .collect(),
    };
    let key = Key::Case {
        property: case.property.into(),
        name: case.name.into(),
    };
    header_acceptance::evaluate(&manifest, &required(&manifest), &results, SUBJECT).keys[&key]
        .clone()
}

/// `result` with its lists replaced.
fn with_lists(
    result: &ExpectedResult,
    counted: &'static [ExpectedNa],
    patched: Option<&'static [ExpectedNa]>,
) -> ExpectedResult {
    let mut result = result.clone();
    if let Expected::Caught {
        not_applicable,
        patched_not_applicable,
        ..
    } = &mut result.expected
    {
        *not_applicable = counted;
        *patched_not_applicable = patched;
    }
    result
}

/// `harness`'s reports on the committed input of planted case `case`,
/// unpatched: the case's baseline, as `cargo xtask planted` records it
/// from the case's `.toml`.
fn baseline(case: CaseRef, harness: &str) -> Vec<(String, Report)> {
    use zakura_sim::{NullSink, Plan, PlanInput, PropertyOutcome};
    let root = repo();
    let read = |file: &str| {
        std::fs::read(root.join(file)).unwrap_or_else(|error| panic!("{file}: {error}"))
    };
    let path = format!("qa/planted/{}/{}.toml", case.property, case.name);
    let text = String::from_utf8(read(&path)).expect("a UTF-8 case file");
    let file: toml::Table = toml::from_str(&text).expect("a case file");
    let string = |table: &toml::Table, key: &str| table.get(key)?.as_str().map(str::to_owned);
    let entry = harness::find(harness).expect("registered");
    let (scenario, plan) = match (string(&file, "input"), file.get("typed")) {
        (Some(input), None) => {
            let input = read(&input);
            let plan = input[entry.scenario_bytes.min(input.len())..].to_vec();
            (entry.scenario(&input), PlanInput::Bytes(plan))
        }
        (None, Some(typed)) => {
            let typed = typed.as_table().expect("a [typed] table");
            let scenario = string(typed, "scenario").expect("a typed scenario");
            let scenario = serde_json::from_slice(&read(&scenario)).expect("a JSON scenario");
            let plan = match (string(typed, "plan"), string(typed, "script")) {
                (Some(plan), None) => PlanInput::Bytes(read(&plan)),
                (None, Some(script)) => PlanInput::Script(
                    serde_json::from_slice::<Plan>(&read(&script)).expect("a scripted Plan"),
                ),
                _ => panic!("{path} names Plan bytes or a script"),
            };
            (scenario, plan)
        }
        _ => panic!("{path} names an input or a typed case"),
    };
    let report = (entry.run)(&scenario, plan, &mut NullSink)
        .unwrap_or_else(|error| panic!("{path}: the baseline completes: {error}"));
    report
        .properties
        .into_iter()
        .map(|property| {
            let report = match property.outcome {
                PropertyOutcome::Held { applicable } => Report::Held {
                    applicable: applicable.into(),
                },
                PropertyOutcome::NotApplicable { reason } => Report::NotApplicable {
                    reason: reason.name().into_owned(),
                },
                PropertyOutcome::Violated { .. } => Report::Violated,
            };
            (property.property, report)
        })
        .collect()
}

/// The committed reserved-flags page's catch and the despite-close case
/// verify through the evaluator with each case's genuine baseline report,
/// whose untouched wire IDs stay `NotApplicable`, and their rows' declared
/// lists. Their patched and `Draft` runs are the baseline with the target
/// violated. An empty list, the rows' old one, blocks each, and so does
/// one entry's reason moved.
#[test]
fn the_wire_catches_declare_their_baselines_not_applicable() {
    use zakura_sim::harness::header_acceptance::{companions, not_applicable};
    let committed = Manifest::committed();
    for case in [
        CaseRef {
            property: "header-sync/s5v9/headers/reserved-flags-closes",
            name: "reserved-flags-skipped",
        },
        companions::DESPITE_CLOSE,
    ] {
        let result = caught(&committed, case);
        let Expected::Caught { set, .. } = result.expected else {
            unreachable!("found as a Caught");
        };
        let reports = baseline(case, WIRE);
        assert!(
            reports
                .iter()
                .any(|(_, report)| matches!(report, Report::NotApplicable { .. })),
            "{case:?}: the baseline leaves wire IDs NotApplicable"
        );
        let draft = violating(&reports, set);
        let with = |declared| {
            judged(
                &committed,
                &with_lists(&result, declared, None),
                &reports,
                &draft,
            )
        };
        let declared = not_applicable::expected(case);
        assert_eq!(with(declared), ObligationState::Verified, "{case:?}");
        assert!(blocked(&with(&[])), "{case:?}");
        let moved: &'static [ExpectedNa] = Box::leak(
            declared
                .iter()
                .enumerate()
                .map(|(at, na)| ExpectedNa {
                    property: na.property,
                    reason: if at == 0 {
                        "assumption-lost"
                    } else {
                        na.reason
                    },
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        assert!(blocked(&with(moved)), "{case:?}");
    }
}

/// The patched list judges the runs past the break. `target-page-refused`'s
/// `Draft` run, reporting what its patched list says, verifies; judged by
/// its baseline's list instead, it blocks; and a violation of an ID
/// outside its set blocks it whatever its lists say.
#[test]
fn the_patched_list_judges_the_draft_run() {
    let committed = Manifest::committed();
    let case = CaseRef {
        property: "header-sync/s5v9/conformant-never-closes",
        name: "target-page-refused",
    };
    let result = caught(&committed, case);
    let Expected::Caught {
        set,
        not_applicable,
        patched_not_applicable,
        ..
    } = result.expected
    else {
        unreachable!("found as a Caught");
    };
    let patched = patched_not_applicable.expect("its Draft run reports otherwise");
    assert_ne!(patched, not_applicable);
    let reports = baseline(case, WIRE);
    let draft: Vec<(String, Report)> = reports
        .iter()
        .map(|(id, _)| {
            let report = if set.contains(&id.as_str()) {
                Report::Violated
            } else if let Some(na) = patched.iter().find(|na| na.property == id) {
                Report::NotApplicable {
                    reason: na.reason.into(),
                }
            } else {
                Report::Held { applicable: 1 }
            };
            (id.clone(), report)
        })
        .collect();
    assert_eq!(
        judged(&committed, &result, &reports, &draft),
        ObligationState::Verified
    );
    let unlisted = with_lists(&result, not_applicable, None);
    assert!(blocked(&judged(&committed, &unlisted, &reports, &draft)));
    let outside = violating(&draft, &["sim/no-livelock"]);
    assert!(blocked(&judged(&committed, &result, &reports, &outside)));
}

/// A completed counted run's `NotApplicable` reports must be benign
/// whatever its lists say. `target-page-refused`'s `Draft` run verifies
/// reporting its patched list, and blocks once its patched list accepts an
/// ID's incomplete `unsettled:watch-open` and the run reports exactly that.
/// No committed list, counted or patched, names a reason that isn't benign.
#[test]
fn no_list_accepts_an_incomplete_reason() {
    let committed = Manifest::committed();
    let case = CaseRef {
        property: "header-sync/s5v9/conformant-never-closes",
        name: "target-page-refused",
    };
    let result = caught(&committed, case);
    let Expected::Caught {
        set,
        not_applicable,
        patched_not_applicable,
        ..
    } = result.expected
    else {
        unreachable!("found as a Caught");
    };
    let patched = patched_not_applicable.expect("its Draft run reports otherwise");
    let reports = baseline(case, WIRE);
    let draft_of = |list: &[ExpectedNa]| -> Vec<(String, Report)> {
        reports
            .iter()
            .map(|(id, _)| {
                let report = if set.contains(&id.as_str()) {
                    Report::Violated
                } else if let Some(na) = list.iter().find(|na| na.property == id) {
                    Report::NotApplicable {
                        reason: na.reason.into(),
                    }
                } else {
                    Report::Held { applicable: 1 }
                };
                (id.clone(), report)
            })
            .collect()
    };
    assert_eq!(
        judged(&committed, &result, &reports, &draft_of(patched)),
        ObligationState::Verified
    );
    let open = reports
        .iter()
        .map(|(id, _)| id.as_str())
        .find(|id| !set.contains(id) && !patched.iter().any(|na| na.property == *id))
        .expect("some ID holds past the break");
    let open: &'static str = Box::leak(open.to_owned().into_boxed_str());
    let mut listed = patched.to_vec();
    listed.push(ExpectedNa {
        property: open,
        reason: "unsettled:watch-open",
    });
    let listed: &'static [ExpectedNa] = Box::leak(listed.into_boxed_slice());
    let accepting = with_lists(&result, not_applicable, Some(listed));
    assert!(blocked(&judged(
        &committed,
        &accepting,
        &reports,
        &draft_of(listed)
    )));
    let benign = ["never-applied", "undeclared", "excluded"];
    for result in &committed.expected {
        let Expected::Caught {
            case,
            not_applicable,
            patched_not_applicable,
            ..
        } = result.expected
        else {
            continue;
        };
        for na in not_applicable
            .iter()
            .chain(patched_not_applicable.unwrap_or_default())
        {
            assert!(benign.contains(&na.reason), "{case:?}: {na:?}");
        }
    }
}

/// Every node catch declares its runs' `NotApplicable` reports, each an ID
/// its harness registers other than the target, and any patched list names
/// no member of its set.
#[test]
fn every_node_catch_declares_its_not_applicable() {
    let committed = Manifest::committed();
    let mut cases = BTreeSet::new();
    for result in &committed.expected {
        let Expected::Caught {
            case,
            harness,
            set,
            not_applicable,
            patched_not_applicable,
            ..
        } = result.expected
        else {
            continue;
        };
        cases.insert((case.property, case.name));
        assert!(!not_applicable.is_empty(), "{case:?}");
        let registered = |id| {
            committed
                .matrix
                .iter()
                .any(|&(on, registered, _)| on == harness && registered == id)
        };
        for na in not_applicable {
            assert!(
                na.property != case.property && registered(na.property),
                "{case:?}: {}",
                na.property
            );
        }
        for na in patched_not_applicable.unwrap_or_default() {
            assert!(
                !set.contains(&na.property) && registered(na.property),
                "{case:?}: {}",
                na.property
            );
        }
    }
    assert_eq!(cases.len(), 216, "every node planted case");
}

/// A catch's case, harness and declared list.
type Declared = (CaseRef, &'static str, &'static [ExpectedNa]);

/// A declared list's IDs and reasons, sorted.
type Shape = Vec<(&'static str, &'static str)>;

/// Each node catch's declared list is exactly its genuine baseline's
/// `NotApplicable` reports, with every other ID but its target held and
/// none violated: the whole-report rule the evaluator applies to the
/// baseline. Running all 216 baselines takes minutes, so this runs the
/// first case, by target and name, of each distinct list on each harness,
/// which is one per shape `not_applicable` declares.
#[test]
fn each_declared_list_matches_its_baseline() {
    let committed = Manifest::committed();
    let mut cases: BTreeMap<(&str, &str), Declared> = BTreeMap::new();
    for result in &committed.expected {
        if let Expected::Caught {
            case,
            harness,
            not_applicable,
            ..
        } = result.expected
        {
            cases.insert((case.property, case.name), (case, harness, not_applicable));
        }
    }
    let mut shapes: BTreeMap<(&str, Shape), Declared> = BTreeMap::new();
    for (case, harness, declared) in cases.into_values() {
        let mut shape: Shape = declared.iter().map(|na| (na.property, na.reason)).collect();
        shape.sort_unstable();
        shapes
            .entry((harness, shape))
            .or_insert((case, harness, declared));
    }
    let cases: Vec<_> = shapes.into_values().collect();
    let threads = std::thread::available_parallelism().map_or(4, usize::from);
    let mismatches: Vec<String> = std::thread::scope(|scope| {
        let workers: Vec<_> = cases
            .chunks(cases.len().div_ceil(threads))
            .map(|chunk| {
                scope.spawn(move || {
                    let mut found = Vec::new();
                    for &(case, harness, declared) in chunk {
                        let reported: BTreeSet<(String, String)> = baseline(case, harness)
                            .into_iter()
                            .filter(|(id, _)| id != case.property)
                            .filter_map(|(id, report)| match report {
                                Report::Held { .. } => None,
                                Report::NotApplicable { reason } => Some((id, reason)),
                                other => Some((id, format!("{other:?}"))),
                            })
                            .collect();
                        let declared: BTreeSet<(String, String)> = declared
                            .iter()
                            .map(|na| (na.property.to_owned(), na.reason.to_owned()))
                            .collect();
                        if reported != declared {
                            found.push(format!(
                                "{case:?}: reported {:?}, declared {:?}",
                                reported.difference(&declared).collect::<Vec<_>>(),
                                declared.difference(&reported).collect::<Vec<_>>()
                            ));
                        }
                    }
                    found
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("a baseline run doesn't panic"))
            .collect()
    });
    assert_eq!(mismatches, Vec::<String>::new());
}
