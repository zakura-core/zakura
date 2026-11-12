//! The planted catches' declared `NotApplicable` reports, judged through
//! the evaluator against genuine baseline reports (hdr-14.5 review). They
//! live apart from `header_acceptance.rs`, whose every test is a
//! composition check the inventories pin.
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
        CaseRef, CaseRun, Expected, Gate, Key, Manifest, ObligationState, RunId,
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
fn execution(run: RunId, harness: &str, reports: &[(&str, Report)]) -> Execution {
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
        reports: reports
            .iter()
            .map(|(id, report)| ((*id).to_owned(), report.clone()))
            .collect(),
        trigger: true,
        leaked: 0,
        actions: Actions::Complete,
    }
}

/// Whether `state` blocks.
fn blocked(state: &ObligationState) -> bool {
    matches!(state, ObligationState::Blocked { .. })
}

/// `header-sync/wire`'s reports on `scenario` under the Plan bytes
/// `plan`, unpatched: a planted case's baseline.
fn wire_baseline(
    scenario: &zakura_sim::harness::header_sync::WireScenario,
    plan: &[u8],
) -> Vec<(String, Report)> {
    use zakura_sim::{
        harness::{header_sync::HeaderWire, run_prepared, HarnessError, Subsystem},
        NullSink, Plan, PropertyOutcome,
    };
    let ports = HeaderWire::prepare(scenario);
    let report = match run_prepared::<HeaderWire>(
        scenario,
        ports,
        Plan::from_fuzz(plan),
        &mut NullSink,
        None,
    ) {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("the wire harness runs the case's input: {error}"),
    };
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
    use zakura_sim::harness::{
        header_acceptance::{companions, not_applicable},
        header_sync::WireScenario,
    };
    const WIRE: &str = "header-sync/wire";
    let committed = Manifest::committed();
    let read = |file: &str| std::fs::read(repo().join(file)).expect("a committed fixture");
    let typed: WireScenario = serde_json::from_slice(&read(
        "crates/zakura-sim/tests/fixtures/header-wire-planted-headers-reserved-flags.json",
    ))
    .expect("a wire scenario");
    let input = read("crates/zakura-sim/tests/fixtures/header-wire-bad-content.bin");
    let entry = harness::find(WIRE).expect("registered");
    let fuzzed: WireScenario =
        serde_json::from_value(entry.scenario(&input)).expect("a wire scenario");
    let cases = [
        (
            CaseRef {
                property: "header-sync/s5v9/headers/reserved-flags-closes",
                name: "reserved-flags-skipped",
            },
            wire_baseline(&typed, &[]),
        ),
        (
            companions::DESPITE_CLOSE,
            wire_baseline(&fuzzed, &input[entry.scenario_bytes.min(input.len())..]),
        ),
    ];
    for (case, reports) in cases {
        let result = committed
            .expected
            .iter()
            .find(|result| matches!(result.expected, Expected::Caught { case: c, .. } if c == case))
            .expect("a row names the case's catch")
            .clone();
        let Expected::Caught { set, .. } = result.expected else {
            unreachable!("found as a Caught");
        };
        assert!(
            reports
                .iter()
                .any(|(_, report)| matches!(report, Report::NotApplicable { .. })),
            "{case:?}: the baseline leaves wire IDs NotApplicable"
        );
        let reported: Vec<(&str, Report)> = reports
            .iter()
            .map(|(id, report)| (id.as_str(), report.clone()))
            .collect();
        let violating: Vec<(&str, Report)> = reported
            .iter()
            .map(|(id, report)| {
                let report = if set.contains(id) {
                    Report::Violated
                } else {
                    report.clone()
                };
                (*id, report)
            })
            .collect();
        let baseline = execution(RunId::case(case, CaseRun::Baseline), WIRE, &reported);
        let mut patched = execution(RunId::case(case, CaseRun::Patched), WIRE, &violating);
        patched.end = End::Violation {
            property: case.property.into(),
        };
        patched.actions = Actions::Prefix;
        let mut draft = execution(RunId::case(case, CaseRun::Draft), WIRE, &violating);
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
        let with = |declared| {
            let mut result = result.clone();
            if let Expected::Caught { not_applicable, .. } = &mut result.expected {
                *not_applicable = declared;
            }
            let manifest = Manifest {
                expected: vec![result],
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
            header_acceptance::evaluate(&manifest, &required(&manifest), &results, SUBJECT).keys
                [&key]
                .clone()
        };
        let declared = not_applicable::expected(case);
        assert_eq!(with(declared), ObligationState::Verified, "{case:?}");
        assert!(blocked(&with(&[])), "{case:?}");
        let moved: &'static [header_acceptance::ExpectedNa] = Box::leak(
            declared
                .iter()
                .enumerate()
                .map(|(at, na)| header_acceptance::ExpectedNa {
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

/// Every generated ID's catch and the despite-close case declare their
/// runs' `NotApplicable` reports, each a wire ID other than the target.
/// (`stale-locator-round` declares its own on the storage harness, beside
/// its row's other storage catches, which declare none yet.)
#[test]
fn every_wire_catch_declares_its_not_applicable() {
    use zakura_sim::harness::header_acceptance::companions::DESPITE_CLOSE;
    let committed = Manifest::committed();
    let wire: BTreeSet<&str> = committed
        .matrix
        .iter()
        .filter(|(harness, _, _)| *harness == "header-sync/wire")
        .map(|(_, id, _)| *id)
        .collect();
    let mut caught = 0;
    for result in &committed.expected {
        let Expected::Caught {
            case,
            not_applicable,
            ..
        } = result.expected
        else {
            continue;
        };
        if !case.property.starts_with("header-sync/s5v9/") && case != DESPITE_CLOSE {
            continue;
        }
        caught += 1;
        assert!(!not_applicable.is_empty(), "{case:?}");
        for na in not_applicable {
            assert!(
                na.property != case.property && wire.contains(na.property),
                "{case:?}: {}",
                na.property
            );
        }
    }
    assert_eq!(
        caught, 30,
        "the 29 generated catches and the despite-close case"
    );
}
