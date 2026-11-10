//! The loop runner's classes on synthetic runs, its run failures, and its
//! report's schema. No test builds the simulator.

use std::{collections::BTreeMap, fs, path::Path, process::Command, time::Duration};

use serde_json::json;

use super::{
    build_failure,
    classify::{self, read_record, Class, Ended, Observed, Reason, Record},
    parse_flags, run_measured, summarize, test_executables, BaselineReport, BuildIdentity,
    CaseReport, LoopCase, Mutant, Report, Runner, Slot, WitnessSpec,
};

const NAMED: &str = "block-sync/retained-byte-bound";
const OTHER: &str = "block-sync/reserved-byte-bound";

/// An end record on thread `t1` whose run `stopped_by` stopped, with each
/// of `outcomes` as `(id, outcome, applicable)`.
fn end(stopped_by: Option<&str>, outcomes: &[(&str, &str, u64)]) -> Record {
    end_on("t1", stopped_by, outcomes)
}

fn end_on(thread: &str, stopped_by: Option<&str>, outcomes: &[(&str, &str, u64)]) -> Record {
    Record::End {
        thread: thread.to_owned(),
        stopped_by: stopped_by.map(str::to_owned),
        violated: outcomes
            .iter()
            .filter(|(_, outcome, _)| *outcome == "violated")
            .map(|(id, _, _)| (*id).to_owned())
            .collect(),
        harness: None,
        outcomes: outcomes
            .iter()
            .map(|(id, outcome, n)| ((*id).to_owned(), ((*outcome).to_owned(), *n)))
            .collect(),
    }
}

fn judged(error: Option<&str>, trigger: &str) -> Record {
    Record::Witness {
        thread: "t1".to_owned(),
        property: NAMED.to_owned(),
        error: error.map(str::to_owned),
        trigger_history: trigger.to_owned(),
    }
}

fn observed(ended: Ended, records: Vec<Record>) -> Observed {
    Observed {
        ended,
        listed: None,
        records,
        unpaired: 0,
    }
}

fn classified(observed: &Observed) -> (Class, Option<Reason>) {
    let result = classify::mutant(NAMED, &[], observed);
    (result.class, result.reason)
}

#[test]
fn the_named_id_violated_kills_by_the_named_rule() {
    let run = observed(
        Ended::Exited(1),
        vec![
            end(Some(NAMED), &[(NAMED, "violated", 0)]),
            judged(Some("violated"), "reached"),
        ],
    );
    assert_eq!(classified(&run), (Class::KilledByNamedRule, None));
}

#[test]
fn another_id_violated_first_kills_by_another_rule_and_names_it() {
    let run = observed(
        Ended::Exited(101),
        vec![
            end(None, &[(NAMED, "held", 3)]),
            end(
                Some(OTHER),
                &[(OTHER, "violated", 0), (NAMED, "violated", 0)],
            ),
        ],
    );
    let result = classify::mutant(NAMED, &[], &run);
    assert_eq!(result.class, Class::KilledByOtherRule);
    assert_eq!(result.other.as_deref(), Some(OTHER));
    // A listed co-violation that stops the run, beside the named one, is
    // the named rule's kill.
    let also = [OTHER.to_owned()];
    assert_eq!(
        classify::mutant(NAMED, &also, &run).class,
        Class::KilledByNamedRule
    );
    // The step loop's own panic property is another rule.
    let panicked = observed(
        Ended::Exited(1),
        vec![end(
            Some("sim/no-task-panic"),
            &[("sim/no-task-panic", "violated", 0)],
        )],
    );
    let result = classify::mutant(NAMED, &also, &panicked);
    assert_eq!(result.class, Class::KilledByOtherRule);
    assert_eq!(result.other.as_deref(), Some("sim/no-task-panic"));
}

#[test]
fn a_passing_witness_survives_held() {
    let run = observed(Ended::Exited(0), vec![end(None, &[(NAMED, "held", 4)])]);
    assert_eq!(classified(&run), (Class::Survived, Some(Reason::Held)));
}

#[test]
fn not_applicable_and_a_lost_trigger_survive_with_their_reasons() {
    let never = observed(
        Ended::Exited(0),
        vec![end(None, &[(NAMED, "not-applicable", 0)])],
    );
    assert_eq!(
        classified(&never),
        (Class::Survived, Some(Reason::NotApplicable))
    );
    let judged_not_applicable = observed(
        Ended::Exited(1),
        vec![
            end(None, &[(NAMED, "not-applicable", 0)]),
            judged(Some("not-applicable"), "lost"),
        ],
    );
    assert_eq!(
        classified(&judged_not_applicable),
        (Class::Survived, Some(Reason::NotApplicable))
    );
    let lost = observed(
        Ended::Exited(1),
        vec![
            end(None, &[(NAMED, "held", 2)]),
            judged(Some("lost-trigger-history"), "lost"),
        ],
    );
    assert_eq!(
        classified(&lost),
        (Class::Survived, Some(Reason::TriggerLost))
    );
    // A violation without its trigger history isn't a kill.
    let violated_off_trigger = observed(
        Ended::Exited(1),
        vec![
            end(Some(NAMED), &[(NAMED, "violated", 0)]),
            judged(Some("violated"), "lost"),
        ],
    );
    assert_eq!(
        classified(&violated_off_trigger),
        (Class::Survived, Some(Reason::TriggerLost))
    );
    // Another thread's judgement doesn't speak for this run.
    let elsewhere = observed(
        Ended::Exited(1),
        vec![
            end(Some(NAMED), &[(NAMED, "violated", 0)]),
            Record::Witness {
                thread: "t2".to_owned(),
                property: NAMED.to_owned(),
                error: Some("lost-trigger-history".to_owned()),
                trigger_history: "lost".to_owned(),
            },
        ],
    );
    assert_eq!(classified(&elsewhere), (Class::KilledByNamedRule, None));
}

#[test]
fn run_failures_are_harness_failures_with_their_reasons() {
    let held = || vec![end(None, &[(NAMED, "held", 1)])];
    for (run, reason) in [
        (observed(Ended::TimedOut, held()), Reason::Timeout),
        (observed(Ended::Signaled(9), held()), Reason::Oom),
        (observed(Ended::Signaled(6), held()), Reason::Panic),
        // A witness that fails with no violation failed outside the run.
        (observed(Ended::Exited(101), held()), Reason::Panic),
        (
            observed(Ended::Exited(101), Vec::new()),
            Reason::NoEndRecord,
        ),
        (
            Observed {
                listed: Some(0),
                ..observed(Ended::Exited(0), Vec::new())
            },
            Reason::ZeroTests,
        ),
        (
            observed(
                Ended::Exited(1),
                vec![Record::End {
                    thread: "t1".to_owned(),
                    stopped_by: None,
                    violated: Vec::new(),
                    harness: Some("deadlock".to_owned()),
                    outcomes: BTreeMap::new(),
                }],
            ),
            Reason::HarnessEnd,
        ),
        // Records from another build don't count.
        (
            Observed {
                unpaired: 3,
                ..observed(Ended::Exited(0), Vec::new())
            },
            Reason::NoEndRecord,
        ),
    ] {
        assert_eq!(
            classified(&run),
            (Class::HarnessFailure, Some(reason)),
            "{run:?}"
        );
        assert!(!Class::HarnessFailure.is_result());
    }
}

#[test]
fn a_baseline_must_hold_the_property_cleanly() {
    let held = observed(Ended::Exited(0), vec![end(None, &[(NAMED, "held", 2)])]);
    assert_eq!(classify::baseline(NAMED, &held), None);
    for run in [
        observed(Ended::Exited(0), vec![end(None, &[(NAMED, "held", 0)])]),
        observed(
            Ended::Exited(0),
            vec![end(None, &[(NAMED, "not-applicable", 0)])],
        ),
        observed(Ended::Exited(101), vec![end(None, &[(NAMED, "held", 2)])]),
        observed(
            Ended::Exited(1),
            vec![end(Some(OTHER), &[(OTHER, "violated", 0)])],
        ),
        observed(Ended::TimedOut, vec![end(None, &[(NAMED, "held", 2)])]),
        observed(Ended::Exited(0), Vec::new()),
    ] {
        assert!(classify::baseline(NAMED, &run).is_some(), "{run:?}");
    }
}

#[test]
fn records_are_read_from_the_hooks_files_and_paired_by_source() {
    let end_file = json!({
        "seq": 0, "thread": "ThreadId(2)", "source": "abc",
        "end": {
            "outcome": "violation", "steps": 9,
            "violation": {"property": NAMED, "step": 9, "witness": [9], "expected": "", "actual": ""},
            "drain": null, "cleanup": {},
            "properties": [
                {"property": NAMED, "outcome": {"violated": {"witness": [9]}}},
                {"property": OTHER, "outcome": {"held": {"applicable": 4}}},
                {"property": "sim/no-livelock", "outcome": "not-applicable"},
            ],
        },
    });
    let record = read_record(&end_file.to_string(), "abc").unwrap().unwrap();
    assert_eq!(
        record,
        Record::End {
            thread: "ThreadId(2)".to_owned(),
            stopped_by: Some(NAMED.to_owned()),
            violated: vec![NAMED.to_owned()],
            harness: None,
            outcomes: [
                (NAMED.to_owned(), ("violated".to_owned(), 0)),
                (OTHER.to_owned(), ("held".to_owned(), 4)),
                (
                    "sim/no-livelock".to_owned(),
                    ("not-applicable".to_owned(), 0)
                ),
            ]
            .into_iter()
            .collect(),
        }
    );
    assert_eq!(read_record(&end_file.to_string(), "other").unwrap(), None);
    let witness_file = json!({
        "seq": 1, "thread": "ThreadId(2)", "source": "abc",
        "witness": {"property": NAMED, "expect": "holds", "error": "violated", "trigger_history": "reached"},
    });
    assert_eq!(
        read_record(&witness_file.to_string(), "abc").unwrap(),
        Some(Record::Witness {
            thread: "ThreadId(2)".to_owned(),
            property: NAMED.to_owned(),
            error: Some("violated".to_owned()),
            trigger_history: "reached".to_owned(),
        })
    );
    assert!(read_record(r#"{"source":"abc"}"#, "abc").is_err());
}

#[test]
fn a_compile_error_is_did_not_compile_and_a_killed_build_is_broken() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("build.log");
    let error = json!({
        "reason": "compiler-message",
        "package_id": "path+file:///repo/crates/zakura-network#0.1.0",
        "message": {
            "level": "error", "message": "mismatched types",
            "spans": [{"file_name": "src/zakura/guard.rs", "line_start": 7}],
        },
    });
    fs::write(&log, format!("{error}\n")).unwrap();
    assert!(matches!(
        build_failure(Ended::Exited(101), &log),
        super::BuildFailure::Rejected(_)
    ));
    assert!(matches!(
        build_failure(Ended::Signaled(9), &log),
        super::BuildFailure::Broken(_)
    ));
    fs::write(&log, "").unwrap();
    assert!(matches!(
        build_failure(Ended::Exited(101), &log),
        super::BuildFailure::Broken(_)
    ));
}

#[test]
fn test_executables_come_from_cargos_artifact_messages() {
    let log = [
        json!({"reason": "compiler-artifact", "target": {"name": "block_table", "kind": ["test"]},
               "profile": {"test": true}, "executable": "/t/block_table-1"}),
        json!({"reason": "compiler-artifact", "target": {"name": "zakura_sim", "kind": ["lib"]},
               "profile": {"test": false}, "executable": null}),
        json!({"reason": "build-finished", "success": true}),
    ]
    .map(|line| line.to_string())
    .join("\n");
    let found = test_executables(&log);
    assert_eq!(found.len(), 1);
    assert_eq!(found["block_table"], Path::new("/t/block_table-1"));
}

/// A git repository at `dir` with one commit of `file`, and its revision.
fn repository(dir: &Path) -> String {
    fs::write(dir.join("file.rs"), "fn limit() -> u32 { 1 }\n").unwrap();
    for args in [
        &["init", "-q"][..],
        &["add", "."],
        &[
            "-c",
            "user.name=T",
            "-c",
            "user.email=t@example.invalid",
            "commit",
            "-qm",
            "pin",
        ],
    ] {
        assert!(Command::new("git")
            .current_dir(dir)
            .args(args)
            .status()
            .unwrap()
            .success());
    }
    let head = Command::new("git")
        .current_dir(dir)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    String::from_utf8(head.stdout).unwrap().trim().to_owned()
}

/// One case of `patch` whose baseline `passed`, and the runner and slot
/// that run it in the repository `tree`, with nothing to build.
fn one_case(
    tree: &Path,
    out: &Path,
    patch: &Path,
    passed: bool,
) -> (Runner, Slot, LoopCase, BaselineReport) {
    let runner = Runner {
        out: out.to_path_buf(),
        work: out.join("work"),
        pinned: repository(tree),
        memory_max: None,
        build_jobs: 1,
    };
    let slot = Slot {
        tree: tree.to_path_buf(),
        target: out.join("target"),
    };
    let case = LoopCase {
        label: "bounds/raise/raise.patch".to_owned(),
        patch: patch.to_path_buf(),
        listed_in: tree.join("cases.json"),
        property: NAMED.to_owned(),
        witness: WitnessSpec::Test {
            test: "t".to_owned(),
            test_target: "block_table".to_owned(),
        },
        entry: Some("bounds".to_owned()),
        table: None,
        also_violates: Vec::new(),
    };
    let baseline = BaselineReport {
        passed,
        detail: (!passed).then(|| "a run violates sim/no-task-panic".to_owned()),
        millis: 10,
    };
    (runner, slot, case, baseline)
}

#[test]
fn a_rejected_patch_does_not_apply_and_a_failing_baseline_runs_no_mutant() {
    let tree = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let patch = out.path().join("raise.patch");
    fs::write(
        &patch,
        "--- a/file.rs\n+++ b/file.rs\n@@ -1 +1 @@\n-fn limit() -> u32 { 9 }\n+fn limit() -> u32 { 2 }\n",
    )
    .unwrap();
    let (runner, slot, case, baseline) = one_case(tree.path(), out.path(), &patch, true);
    let runnables = [super::Runnable::Test {
        target: "block_table".to_owned(),
        test: "t".to_owned(),
    }];
    let mutant = Mutant {
        patch: patch.clone(),
        cases: vec![0],
    };
    let cases = [case];
    let reports = runner
        .mutant(&slot, &mutant, &cases, &runnables, &[baseline], None)
        .unwrap();
    assert_eq!(reports[0].1.result.class, Class::PatchDoesNotApply);

    // A patch that applies, against a failing baseline, gives
    // `baseline-fails` and builds nothing.
    fs::write(
        &patch,
        "--- a/file.rs\n+++ b/file.rs\n@@ -1 +1 @@\n-fn limit() -> u32 { 1 }\n+fn limit() -> u32 { 2 }\n",
    )
    .unwrap();
    let failing = BaselineReport {
        passed: false,
        detail: Some("a run violates sim/no-task-panic".to_owned()),
        millis: 10,
    };
    let reports = runner
        .mutant(&slot, &mutant, &cases, &runnables, &[failing], None)
        .unwrap();
    assert_eq!(reports[0].1.result.class, Class::BaselineFails);
    assert_eq!(reports[0].1.millis, None);
    assert!(!out.path().join("target").exists(), "nothing was built");
}

#[test]
fn the_report_round_trips_and_counts_by_property() {
    let case = |class, reason, property: &str| CaseReport {
        patch: "bounds/raise/raise.patch".to_owned(),
        patch_sha256: "00".repeat(32),
        property: property.to_owned(),
        entry: Some("bounds".to_owned()),
        table: Some("block-sync/s6v3".to_owned()),
        witness: WitnessSpec::Planted {
            planted: "qa/planted/x/y.toml".into(),
        },
        also_violates: vec![OTHER.to_owned()],
        result: classify::Classified {
            other: (class == Class::KilledByOtherRule).then(|| OTHER.to_owned()),
            ..classify::Classified::new(class, reason, "why")
        },
        baseline: BaselineReport {
            passed: class != Class::BaselineFails,
            detail: None,
            millis: 1200,
        },
        millis: Some(3400),
        peak_rss_kib: Some(51_200),
        log: Some("mutants/x/witness.log".into()),
        rerun_alone: reason == Some(Reason::Oom),
    };
    let cases = vec![
        case(Class::KilledByNamedRule, None, NAMED),
        case(Class::KilledByOtherRule, None, NAMED),
        case(Class::Survived, Some(Reason::Held), NAMED),
        case(Class::Survived, Some(Reason::TriggerLost), NAMED),
        case(Class::DidNotCompile, None, OTHER),
        case(Class::PatchDoesNotApply, None, OTHER),
        case(Class::BaselineFails, None, OTHER),
        case(Class::HarnessFailure, Some(Reason::Oom), OTHER),
    ];
    let report = Report {
        schema: 1,
        pinned: "a".repeat(40),
        prompt_sha256: "b".repeat(64),
        build: BuildIdentity {
            source: "c".repeat(64),
            rustc: "rustc 1.97.0".to_owned(),
            profile: "planted".to_owned(),
            rustflags: "--cfg zakura_sim".to_owned(),
        },
        host: "zakura-sim-nov06".to_owned(),
        jobs: 4,
        memory_max: Some(16 << 30),
        summary: summarize(&cases),
        cases,
    };
    let text = serde_json::to_string_pretty(&report).unwrap();
    let back: Report = serde_json::from_str(&text).unwrap();
    assert_eq!(back, report);
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    for key in [
        "schema",
        "pinned",
        "prompt_sha256",
        "build",
        "host",
        "cases",
        "summary",
    ] {
        assert!(value.get(key).is_some(), "{key}");
    }
    let first = &value["cases"][0];
    for key in [
        "patch",
        "patch_sha256",
        "property",
        "table",
        "witness",
        "result",
        "baseline",
        "millis",
        "peak_rss_kib",
        "log",
    ] {
        assert!(first.get(key).is_some(), "{key}");
    }
    assert_eq!(first["result"]["class"], "killed-by-named-rule");
    assert_eq!(value["cases"][1]["result"]["other"], OTHER);
    assert_eq!(value["cases"][3]["result"]["reason"], "trigger-lost");
    assert_eq!(
        value["summary"][NAMED]["results"],
        json!({"killed-by-named-rule": 1, "killed-by-other-rule": 1, "survived/held": 1, "survived/trigger-lost": 1})
    );
    assert_eq!(
        value["summary"][OTHER]["run_failures"],
        json!({"patch-does-not-apply": 1, "baseline-fails": 1, "harness-failure/oom": 1})
    );
    assert_eq!(
        value["summary"][OTHER]["results"],
        json!({"did-not-compile": 1})
    );
}

#[test]
fn flags_parse_and_refuse_bad_values() {
    let flags = parse_flags(
        [
            "--out",
            "out",
            "--planted",
            "block-sync/",
            "--planted",
            "block/",
            "--jobs",
            "6",
            "--memory-max",
            "off",
            "--only",
            NAMED,
        ]
        .map(str::to_owned)
        .into_iter(),
    )
    .unwrap();
    assert!(flags.out.is_absolute());
    assert_eq!(flags.planted, ["block-sync/", "block/"]);
    assert_eq!(flags.jobs, 6);
    assert_eq!(flags.memory_max, Some(None));
    assert_eq!(flags.only, [NAMED]);
    for bad in [
        &["--jobs", "0", "--out", "o"][..],
        &["--memory-max", "lots", "--out", "o"],
        &["--work", "w"],
        &["--out", "o", "--out", "p"],
    ] {
        assert!(
            parse_flags(bad.iter().map(|s| (*s).to_owned())).is_err(),
            "{bad:?}"
        );
    }
}

#[cfg(unix)]
#[test]
fn a_child_reports_its_exit_or_its_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("child.log");
    let (ended, peak) = run_measured(
        Command::new("sh").args(["-c", "exit 3"]),
        &log,
        Some(Duration::from_secs(30)),
    )
    .unwrap();
    assert_eq!(ended, Ended::Exited(3));
    assert!(peak.is_some_and(|kib| kib > 0));
    let (ended, _) = run_measured(
        Command::new("sleep").arg("30"),
        &log,
        Some(Duration::from_millis(100)),
    )
    .unwrap();
    assert_eq!(ended, Ended::TimedOut);
}
