//! The acceptance tool's integration (hdr-14.7b): gate 1's A/A and A/B/A
//! comparison and its replay store control, and the tool's whole path over
//! two planted cases, from recording through evaluation.
//!
//! Recordings and replays here go through the `zakura-sim` binary, as the
//! acceptance runner makes them, and land at the paths `tool`'s artifact
//! contract gives them.
#![cfg(zakura_sim)]

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;
use zakura_sim::{
    harness::{
        self,
        header_acceptance::{
            contracts::RunBy,
            evaluate::{Actual, End},
            sim_test,
            tool::{self, ReplayRun, Which, FULL_REPORT_FILE, REPLAY_DIR, REPLAY_FILE},
            HistoryRef, RunId,
        },
        RecordInput,
    },
    record::FullReport,
};

/// The committed fixture `name`, a full fuzz input.
fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// Runs the `zakura-sim` binary with `args`, returning whether it exited 0
/// and its combined output.
fn sim(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs");
    let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&output.stderr));
    (output.status.success(), log)
}

/// Writes `dir`'s fresh replay as the runner does: a separate `zakura-sim
/// replay --report` child, and its end as [`REPLAY_FILE`].
fn replay(dir: &Path) {
    let replay = dir.join(REPLAY_DIR);
    std::fs::create_dir_all(&replay).expect("the temp dir is writable");
    let report = replay.join(FULL_REPORT_FILE);
    let (exited_ok, log) = sim(&[
        "replay",
        dir.to_str().expect("the path is UTF-8"),
        "--report",
        report.to_str().expect("the path is UTF-8"),
    ]);
    let ran = ReplayRun {
        exited_ok,
        killed: false,
        log,
    };
    std::fs::write(
        replay.join(REPLAY_FILE),
        serde_json::to_string(&ran).expect("it serializes"),
    )
    .expect("the temp dir is writable");
}

/// Reads and rewrites the full report at `path` through `edit`.
fn edit_report(path: &Path, edit: impl FnOnce(&mut FullReport)) {
    let mut report: FullReport =
        serde_json::from_str(&std::fs::read_to_string(path).expect("the report exists"))
            .expect("it reads");
    edit(&mut report);
    std::fs::write(path, report.to_line()).expect("the temp dir is writable");
}

/// Changes one stored fact in `report`'s snapshot: the first store with a
/// non-empty snapshot gets one more character. Every outcome and violation
/// stays as it was.
fn change_one_fact(report: &mut FullReport) {
    let stores = report
        .snapshot
        .as_mut()
        .and_then(Value::as_array_mut)
        .expect("a node run keeps its stores");
    let store = stores
        .iter_mut()
        .find(|store| {
            store["snapshot"]
                .as_str()
                .is_some_and(|text| text.len() > 2)
        })
        .expect("some store holds a fact");
    let text = store["snapshot"].as_str().expect("found as a string");
    store["snapshot"] = Value::String(format!("{text} "));
}

/// The exact, reports-equal and stores-equal flags of a replay read back.
fn flags(actual: Actual) -> (bool, bool, bool, bool) {
    match actual {
        Actual::Replay {
            exact,
            reports_equal,
            stores_equal,
            fresh,
            ..
        } => (exact, reports_equal, stores_equal, fresh),
        other => panic!("{other:?}"),
    }
}

/// A history identity for the fixtures.
fn history() -> RunId {
    HistoryRef {
        harness: "header-sync/storage",
        witness: sim_test("tests/t.rs", "t"),
        case: "only",
    }
    .into()
}

/// Gate 1's replay store control: a recorded node run replays fresh with
/// its reports and stores equal, and a changed stored fact with every
/// report the same fails the comparison. Changed in the replay's report,
/// only the stores differ. Changed in the recording's, the replay itself
/// refuses the trace, so it isn't exact either.
#[test]
fn a_changed_stored_fact_fails_the_replay_comparison() {
    let input = fixture("header-storage-close-relink.bin");
    let recorded = |dir: &Path| {
        let (ok, log) = sim(&[
            "record",
            "--sim",
            "header-sync/storage",
            "--input",
            input.to_str().expect("the path is UTF-8"),
            "--out",
            dir.to_str().expect("the path is UTF-8"),
        ]);
        assert!(ok, "{log}");
    };
    let dir = tempfile::tempdir().expect("a temp dir can be created");
    recorded(dir.path());
    replay(dir.path());
    let read = |dir: &Path| flags(tool::read_replay(dir, history(), RunBy::Tool));
    assert_eq!(read(dir.path()), (true, true, true, true));

    edit_report(
        &dir.path().join(REPLAY_DIR).join(FULL_REPORT_FILE),
        change_one_fact,
    );
    assert_eq!(read(dir.path()), (true, true, false, true));

    let dir = tempfile::tempdir().expect("a temp dir can be created");
    recorded(dir.path());
    edit_report(&dir.path().join(FULL_REPORT_FILE), change_one_fact);
    replay(dir.path());
    let (exact, _, stores_equal, _) = read(dir.path());
    assert!(!exact && !stores_equal);
}

/// Gate 1's full-report binding control: runs A and B, one mirror serving
/// the same fork shape on two fixture branches, have the same outcomes and
/// different stores. Each reads back as itself. With A's
/// recording and replay reports copied beside B's trace and B's own
/// successful replay log, B's recording reads as no run and its replay's
/// reports don't compare, and so with only the replay's report swapped. A
/// report that names no trace is no report of B's either.
#[test]
fn another_run_s_full_reports_beside_a_trace_are_refused() {
    let plan = tempfile::tempdir().expect("a temp dir can be created");
    let plan = plan.path().join("plan.bin");
    std::fs::write(&plan, b"").expect("the temp dir is writable");
    let recorded = |branch: u8| {
        let scenario = serde_json::json!({
            "controls": [],
            "mirrors": [{
                "chain": {"main": 2, "fork": {"branch": branch, "len": 3}},
                "dialer": "mirror",
            }],
            "node": {"main": 0, "fork": null},
            "policy": {"base": "success-only", "bound_ms": 5000, "holds": []},
        })
        .to_string();
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        let (ok, log) = sim(&[
            "record",
            "--sim",
            "header-sync/storage",
            "--scenario",
            &scenario,
            "--plan",
            plan.to_str().expect("the path is UTF-8"),
            "--out",
            dir.path().to_str().expect("the path is UTF-8"),
        ]);
        assert!(ok, "{log}");
        replay(dir.path());
        dir
    };
    let full = |dir: &Path| -> FullReport {
        serde_json::from_str(
            &std::fs::read_to_string(dir.join(FULL_REPORT_FILE)).expect("the report exists"),
        )
        .expect("it reads")
    };
    let outcomes = |report: &FullReport| -> Vec<(String, zakura_sim::PropertyOutcome)> {
        report
            .properties
            .iter()
            .map(|found| (found.property.clone(), found.outcome.clone()))
            .collect()
    };
    let execution =
        |dir: &Path| tool::read_execution(dir, history(), "header-sync/storage", RunBy::Tool).end;
    let read = |dir: &Path| flags(tool::read_replay(dir, history(), RunBy::Tool));
    let a = recorded(1);
    let b = recorded(2);
    let (a_full, b_full) = (full(a.path()), full(b.path()));
    assert_eq!(outcomes(&a_full), outcomes(&b_full));
    assert_ne!(a_full.snapshot, b_full.snapshot);
    for dir in [a.path(), b.path()] {
        assert!(!matches!(execution(dir), End::Failed { .. }), "{dir:?}");
        assert_eq!(read(dir), (true, true, true, true), "{dir:?}");
    }

    let copy = |from: &Path, to: &Path| std::fs::copy(from, to).map(|_| ()).expect("it copies");
    let refused =
        |end: End| matches!(end, End::Failed { why } if why.contains("isn't a report of"));
    copy(
        &a.path().join(FULL_REPORT_FILE),
        &b.path().join(FULL_REPORT_FILE),
    );
    let replayed = |dir: &Path| dir.join(REPLAY_DIR).join(FULL_REPORT_FILE);
    copy(&replayed(a.path()), &replayed(b.path()));
    assert!(refused(execution(b.path())));
    assert_eq!(read(b.path()), (true, false, false, true));

    let b = recorded(2);
    copy(&replayed(a.path()), &replayed(b.path()));
    assert!(!matches!(execution(b.path()), End::Failed { .. }));
    assert_eq!(read(b.path()), (true, false, false, true));

    let b = recorded(2);
    edit_report(&b.path().join(FULL_REPORT_FILE), |report| {
        report.trace = None
    });
    assert!(refused(execution(b.path())));
}

/// The runner contract's `record` arguments for a declared history record
/// it through the `zakura-sim` binary: `--scenario` takes the declaration's
/// JSON text, and the recorded trace runs the declared scenario and Plan.
/// The scenario file's path in its place, as the contract once read, is
/// refused as not JSON.
#[test]
fn a_declared_history_records_through_the_contracts_arguments() {
    use zakura_sim::{
        harness::{header_acceptance::history, header_sync::StorageScenario},
        PlanInput,
    };
    let root = tempfile::tempdir().expect("a temp dir can be created");
    let scenario = StorageScenario::default();
    let storage = "header-sync/storage";
    history::declare_into(
        root.path(),
        storage,
        "only",
        &scenario,
        &PlanInput::Bytes(vec![7, 1]),
    )
    .expect("the temp dir is writable");
    let declaration = root.path().join("0");
    let out = root.path().join("run");
    let mut args = vec!["record".to_owned()];
    args.extend(tool::record_args(storage, &declaration).expect("it names one Plan"));
    args.extend(["--out".to_owned(), out.display().to_string()]);
    let (ok, log) = sim(&args.iter().map(String::as_str).collect::<Vec<_>>());
    assert!(ok, "{log}");
    let trace = zakura_sim::trace::TraceReader::open(&out).expect("the trace reads back");
    let described = (harness::find(storage).expect("it's registered").meta)(
        &serde_json::to_value(&scenario).expect("it serializes"),
    )
    .expect("the scenario fits");
    assert_eq!(trace.meta.scenario, described.scenario);

    let path = declaration.join(history::SCENARIO_FILE);
    let mut by_path = args.clone();
    by_path[4] = path.display().to_string();
    by_path.pop();
    by_path.push(root.path().join("by-path").display().to_string());
    let (ok, log) = sim(&by_path.iter().map(String::as_str).collect::<Vec<_>>());
    assert!(!ok && log.contains("--scenario isn't JSON"), "{log}");
}

/// Every committed fixture of `harness`, sorted.
fn fixtures_of(harness: &str) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(fixture(""))
        .expect("the fixtures directory is readable")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
        .filter(|path| {
            std::fs::read_to_string(path)
                .ok()
                .and_then(|text| toml::from_str::<toml::Table>(&text).ok())
                .is_some_and(|table| {
                    table.get("harness").and_then(toml::Value::as_str) == Some(harness)
                })
        })
        .map(|path| path.with_extension("bin"))
        .filter(|path| path.exists())
        .collect();
    found.sort();
    found
}

/// Records `input` on `harness` into `dir`, in this process.
fn record_in_process(harness: &str, input: &[u8], dir: &Path) -> Result<(), String> {
    let entry = harness::find(harness).map_err(|error| error.to_string())?;
    harness::record(entry, RecordInput::Input(input), dir)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Gate 1's A/A and A/B/A comparison: for each header harness, its first
/// committed history A runs twice and again after a distinct B in this one
/// process, with the same trace, full report and store snapshot each time.
/// A harness with one committed history takes the empty input as B. Its controls: a run with no full report is a missing
/// comparison, an A result changed only after B fails, and a B that
/// resolves to A's execution fails.
#[test]
fn each_history_is_the_same_alone_and_after_another() {
    for harness in [
        "header-sync/storage",
        "header-sync/wire",
        "header-sync/release-wake",
        "header-sync/repair",
        "header/reactor-policy",
        "header/publisher-policy",
    ] {
        let fixtures = fixtures_of(harness);
        let a = std::fs::read(&fixtures[0]).expect("a fixture is readable");
        let b = match fixtures.get(1) {
            Some(path) => std::fs::read(path).expect("a fixture is readable"),
            None => Vec::new(),
        };
        let root = tempfile::tempdir().expect("a temp dir can be created");
        let compared = tool::a_a_and_a_b_a(root.path(), &mut |which, dir| {
            record_in_process(harness, if which == Which::A { &a } else { &b }, dir)
        });
        assert_eq!(compared, Ok(()), "{harness}");
    }

    let a = std::fs::read(fixture("header-storage-close-relink.bin")).expect("it's readable");
    let b = std::fs::read(fixture("header-storage-default.bin")).expect("it's readable");
    let storage = "header-sync/storage";
    let control = |change: &dyn Fn(Which, usize, &Path)| {
        let root = tempfile::tempdir().expect("a temp dir can be created");
        let mut runs = 0;
        tool::a_a_and_a_b_a(root.path(), &mut |which, dir| {
            record_in_process(storage, if which == Which::A { &a } else { &b }, dir)?;
            change(which, runs, dir);
            runs += 1;
            Ok(())
        })
    };
    // No full report beside A's second run.
    let missing = control(&|_, run, dir| {
        if run == 1 {
            std::fs::remove_file(dir.join(FULL_REPORT_FILE)).expect("it exists");
        }
    });
    assert!(
        missing
            .as_ref()
            .is_err_and(|why| why.contains("missing comparison")),
        "{missing:?}"
    );
    // One stored fact of A's run after B changed.
    let after = control(&|_, run, dir| {
        if run == 3 {
            edit_report(&dir.join(FULL_REPORT_FILE), change_one_fact);
        }
    });
    assert!(
        after.as_ref().is_err_and(|why| why.contains("after B")),
        "{after:?}"
    );
    // B recorded from A's input.
    let root = tempfile::tempdir().expect("a temp dir can be created");
    let same = tool::a_a_and_a_b_a(root.path(), &mut |_, dir| {
        record_in_process(storage, &a, dir)
    });
    assert!(
        same.as_ref()
            .is_err_and(|why| why.contains("B resolves to A")),
        "{same:?}"
    );
}

/// The SHA-256 of `path`'s bytes in hex, as the tool names a trace.
fn digest(path: &Path) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(std::fs::read(path).expect("the file exists"))
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The `zakura-sim record` arguments that run `input` on `harness`, from
/// the repository `repo`.
fn input_args(repo: &Path, input: &tool::CaseInput) -> Vec<String> {
    let path = |file: &str| repo.join(file).display().to_string();
    match input {
        tool::CaseInput::Fuzz { input } => vec!["--input".into(), path(input)],
        tool::CaseInput::Typed { scenario, plan } => vec![
            "--scenario".into(),
            std::fs::read_to_string(repo.join(scenario)).expect("the scenario is readable"),
            "--plan".into(),
            path(plan),
        ],
        tool::CaseInput::Scripted { scenario, script } => vec![
            "--scenario".into(),
            std::fs::read_to_string(repo.join(scenario)).expect("the scenario is readable"),
            "--script".into(),
            path(script),
        ],
    }
}

/// The two planted cases the whole-path test runs: the despite-close case
/// on `header-sync/wire`, with its three companions, and a stale-completion
/// case on `header-sync/storage`, with none. Their patches differ.
const TWO_CASES: [(&str, &str); 2] = [
    ("header-sync/commit-only-authorized", "staged-despite-close"),
    ("header-sync/stale-completion-inert", "stale-charge"),
];

/// The tool's whole path over two planted cases, from recording through
/// evaluation, with every artifact made as the runner's contract gives it,
/// through the `zakura-sim` and `header-acceptance` binaries. Each case
/// gets its baseline, patched and `Draft` runs, each replayed fresh, its
/// provenance, verdict and target checks, and its companions. The tool
/// reads each run from its own case's directory with its own trace, binds
/// it to that case's committed input, reads the `Draft` run's override as
/// the case's set and its companions on that trace, and gives each run's
/// replay exact with its reports and stores equal.
///
/// Rebuilding each case's patch is the runner's (xtask's `rebuild_case`),
/// which this test can't do, so every run here comes from this build: the
/// patched runs don't violate, and each `Draft` run is refused as not from
/// its case's own binary. So both cases block, which the record shows,
/// each with its links in its own directory only.
#[test]
fn two_planted_cases_run_through_the_tool() {
    use zakura_sim::harness::header_acceptance::{
        companions::COMPANIONS,
        evaluate::End,
        tool::{RunBinding, Task},
        Expected, Manifest,
    };
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest = Manifest::committed();
    let plan = tool::plan(&manifest, &repo).expect("the committed manifest plans");
    let source = zakura_sim::trace::BuildInfo::current().source;
    let run = RunBinding {
        run_id: "two-cases".into(),
        subject: "s".into(),
        subject_source: source.clone(),
        containment: tool::Containment::Subreaper,
    };
    let artifacts = tempfile::tempdir().expect("a temp dir can be created");
    std::fs::write(
        artifacts.path().join(tool::RUN_FILE),
        serde_json::to_string(&run).expect("it serializes"),
    )
    .expect("the temp dir is writable");
    for (index, task) in plan.tasks.iter().enumerate() {
        let dir = artifacts.path().join(index.to_string());
        std::fs::create_dir_all(&dir).expect("the temp dir is writable");
        std::fs::write(
            dir.join(tool::TASK_FILE),
            tool::task_binding(index, task).to_string(),
        )
        .expect("the temp dir is writable");
    }
    let path = |dir: &Path| dir.to_str().expect("the path is UTF-8").to_owned();
    let mut indices = Vec::new();
    for (property, name) in TWO_CASES {
        let (index, task) = plan
            .tasks
            .iter()
            .enumerate()
            .find(|(_, task)| {
                matches!(task, Task::Case { property: p, name: n, .. } if p == property && n == name)
            })
            .expect("each case has a task");
        let Task::Case {
            harness,
            input,
            patched: witness,
            patch,
            draft: Some(set),
            companions,
            ..
        } = task
        else {
            panic!("{property}/{name} is a node case");
        };
        indices.push(index);
        let dir = artifacts.path().join(index.to_string());
        let record = |out: &str, extra: &[&str]| {
            let mut args: Vec<String> = vec!["record".into(), "--sim".into(), harness.clone()];
            args.extend(input_args(&repo, input));
            args.extend(["--out".into(), path(&dir.join(out))]);
            args.extend(extra.iter().map(|arg| (*arg).to_owned()));
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            sim(&args);
            replay(&dir.join(out));
        };
        record("baseline", &["--witness", property, "--expect", "holds"]);
        // The patched run expects what the case's task names, as the
        // planted runner records it.
        let beside = witness.beside.join(",");
        let mut patched = vec!["--witness", property, "--expect", &witness.expect];
        if !beside.is_empty() {
            patched.extend(["--also-violates", &beside]);
        }
        record("patched", &patched);
        record("draft", &["--draft", &set.join(",")]);
        let checks = [
            (
                "provenance.json",
                serde_json::json!({
                    "source": source,
                    "patch_sha256": digest(&repo.join(patch)),
                    "refused": null,
                }),
            ),
            (
                "verdict.json",
                serde_json::json!({"property": property, "name": name, "refused": null}),
            ),
            (
                "target.json",
                serde_json::json!({
                    "property": property, "name": name, "target": null, "refused": null,
                }),
            ),
        ];
        for (file, check) in checks {
            std::fs::write(dir.join(file), check.to_string()).expect("the temp dir is writable");
        }
        if !companions.is_empty() {
            let out = Command::new(env!("CARGO_BIN_EXE_header-acceptance"))
                .args([
                    "companions",
                    "--trace",
                    &path(&dir.join("draft")),
                    "--out",
                    &path(&dir.join("draft").join(tool::COMPANIONS_FILE)),
                ])
                .output()
                .expect("the binary runs");
            assert!(out.status.success(), "{out:?}");
        }
    }

    let (results, record) = tool::evaluate_artifacts(&repo, &plan, artifacts.path(), &run);
    let mut traces = std::collections::BTreeSet::new();
    for (&index, (property, name)) in indices.iter().zip(TWO_CASES) {
        let dir = artifacts.path().join(index.to_string());
        let Task::Case {
            expected,
            draft: Some(set),
            companions: declared,
            ..
        } = &plan.tasks[index]
        else {
            unreachable!("found as a node case");
        };
        let caught = results
            .results
            .iter()
            .find(|result| &result.expected == expected)
            .expect("each case gives a result");
        let Actual::Caught {
            baseline,
            patched,
            draft,
            companions,
        } = &caught.actual
        else {
            panic!("{name} is read as a Caught: {:?}", caught.actual);
        };
        for (execution, run) in [
            (baseline, "baseline"),
            (patched, "patched"),
            (draft, "draft"),
        ] {
            assert_eq!(execution.trace, digest(&dir.join(run).join("trace.jsonl")));
            assert!(!execution.reports.is_empty(), "{name}/{run}");
        }
        // One build records the same input to the same trace, so the
        // patched run, which this test can't rebuild, is the baseline's.
        // The Draft run's override makes its own.
        assert_eq!(patched.trace, baseline.trace, "{name}");
        for (execution, run) in [(baseline, "baseline"), (draft, "draft")] {
            assert!(traces.insert(execution.trace.clone()), "{name}/{run}");
        }
        assert_eq!(baseline.end, End::Completed, "{name}: {baseline:?}");
        assert_eq!(baseline.meta.run_by, RunBy::Planted);
        assert!(baseline.trigger, "{name}");
        assert_eq!(draft.meta.run_by, RunBy::Tool);
        assert_eq!(&draft.meta.draft_override, set);
        assert!(
            matches!(&draft.end, End::Failed { why } if why.contains("patched checkout")),
            "{name}: {:?}",
            draft.end
        );
        let names: Vec<&str> = companions.iter().map(|c| c.name.as_str()).collect();
        let declared: Vec<&str> = declared.iter().map(String::as_str).collect();
        assert_eq!(names, declared, "{name}");
        if !declared.is_empty() {
            assert_eq!(names, COMPANIONS);
        }
        assert!(companions
            .iter()
            .all(|c| c.trace == draft.trace && c.run == draft.run));
        // Each of the case's counted runs replayed fresh and exactly, with
        // its reports and stores equal.
        for role in ["patched", "draft"] {
            let replayed = manifest
                .expected
                .iter()
                .find_map(|result| match &result.expected {
                    Expected::Replays {
                        run:
                            run @ RunId::Case {
                                property: p,
                                name: n,
                                run: r,
                            },
                    } if p == property
                        && n == name
                        && serde_json::to_value(r).ok() == Some(Value::from(role)) =>
                    {
                        result.expected.id().map(|id| (id, run.clone()))
                    }
                    _ => None,
                })
                .expect("the manifest replays each counted run");
            let replay = results
                .results
                .iter()
                .find(|result| result.expected == replayed.0)
                .expect("each replay gives a result");
            assert_eq!(
                flags(replay.actual.clone()),
                (true, true, true, true),
                "{name}/{role}"
            );
            let Actual::Replay { of, run, .. } = &replay.actual else {
                unreachable!("read as a replay");
            };
            assert_eq!(*run, replayed.1);
            assert_eq!(*of, digest(&dir.join(role).join("trace.jsonl")));
        }
        // The record blocks the case, linking only its own directory.
        let entry = record
            .blocked
            .iter()
            .find(|entry| entry.links.result.as_ref() == Some(expected))
            .expect("the case is blocked");
        assert!(entry
            .links
            .artifacts
            .iter()
            .all(|artifact| artifact.starts_with(&format!("{index}/"))));
    }
    assert!(!record.accepted);
}
