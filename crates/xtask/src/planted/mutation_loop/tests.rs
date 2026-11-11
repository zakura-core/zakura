//! The loop runner's classes on synthetic runs, its run failures, and its
//! report's schema. No test builds the simulator.

use std::{collections::BTreeMap, fs, path::Path, process::Command};

#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use super::{run_measured, Built, Runnable};

use serde_json::json;

use super::{
    build_failure,
    classify::{self, read_record, Class, Ended, Observed, Reason, Record, Trigger},
    parse_flags, selects, summarize, test_executables, BaselineReport, BuildIdentity, CaseReport,
    LoopCase, Mutant, Report, Runner, Slot, WitnessSpec,
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

fn judged(error: Option<&str>, trigger: Trigger) -> Record {
    Record::Witness {
        thread: "t1".to_owned(),
        property: NAMED.to_owned(),
        error: error.map(str::to_owned),
        trigger_history: trigger,
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
            judged(Some("violated"), Trigger::Reached),
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
            judged(Some("not-applicable"), Trigger::Lost),
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
            judged(Some("lost-trigger-history"), Trigger::Lost),
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
            judged(Some("violated"), Trigger::Lost),
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
                trigger_history: Trigger::Lost,
            },
        ],
    );
    assert_eq!(classified(&elsewhere), (Class::KilledByNamedRule, None));
}

#[test]
fn the_trigger_comes_from_the_judgement_after_the_deciding_run() {
    // A kill whose run a judgement follows carries that judgement's trigger.
    let judged_kill = observed(
        Ended::Exited(1),
        vec![
            end(Some(NAMED), &[(NAMED, "violated", 0)]),
            judged(Some("violated"), Trigger::Reached),
        ],
    );
    assert_eq!(classify::trigger(NAMED, &judged_kill), Trigger::Reached);
    // A test witness that runs no judgement still kills, and its trigger is
    // unjudged.
    let unjudged_kill = observed(
        Ended::Exited(101),
        vec![end(Some(NAMED), &[(NAMED, "violated", 0)])],
    );
    assert_eq!(classified(&unjudged_kill), (Class::KilledByNamedRule, None));
    assert_eq!(classify::trigger(NAMED, &unjudged_kill), Trigger::Unjudged);
    // A judgement after another thread's run doesn't speak for this one.
    let elsewhere = observed(
        Ended::Exited(1),
        vec![
            end(Some(NAMED), &[(NAMED, "violated", 0)]),
            end_on("t2", None, &[(NAMED, "held", 1)]),
            Record::Witness {
                thread: "t2".to_owned(),
                property: NAMED.to_owned(),
                error: None,
                trigger_history: Trigger::Reached,
            },
        ],
    );
    assert_eq!(classify::trigger(NAMED, &elsewhere), Trigger::Unjudged);
    // A survivor carries the last judgement's trigger.
    let survivor = observed(
        Ended::Exited(1),
        vec![
            end(None, &[(NAMED, "held", 2)]),
            judged(Some("no-view"), Trigger::NoView),
        ],
    );
    assert_eq!(classify::trigger(NAMED, &survivor), Trigger::NoView);
}

#[test]
fn a_violation_that_stopped_no_run_decides_nothing() {
    const DRAFT: &str = "block-sync/reactor-query-lifecycle";
    let records = || vec![end(None, &[(NAMED, "held", 3), (DRAFT, "violated", 0)])];
    // The witness passes, so the named ID survives `held`, and the detail
    // names the other violation.
    let passed = classify::mutant(NAMED, &[], &observed(Ended::Exited(0), records()));
    assert_eq!(
        (passed.class, passed.reason),
        (Class::Survived, Some(Reason::Held))
    );
    assert_eq!(passed.other, None);
    assert!(passed.detail.contains(DRAFT), "{}", passed.detail);
    // A judgement that failed only at that violation survives `held` too.
    let mut unrelated = records();
    unrelated.push(judged(Some("unrelated"), Trigger::Reached));
    assert_eq!(
        classified(&observed(Ended::Exited(1), unrelated)),
        (Class::Survived, Some(Reason::Held))
    );
    // A failed witness with no judgement isn't a kill.
    assert_eq!(
        classified(&observed(Ended::Exited(101), records())),
        (Class::HarnessFailure, Some(Reason::Panic))
    );
    // The named ID's own violation decides, though it stopped no run.
    let own = observed(
        Ended::Exited(101),
        vec![end(None, &[(NAMED, "violated", 0), (DRAFT, "violated", 0)])],
    );
    assert_eq!(classified(&own), (Class::KilledByNamedRule, None));
    // Another ID's violation that stopped its run still decides.
    let stopped = observed(
        Ended::Exited(101),
        vec![end(
            Some(OTHER),
            &[(OTHER, "violated", 0), (DRAFT, "violated", 0)],
        )],
    );
    let result = classify::mutant(NAMED, &[], &stopped);
    assert_eq!(result.class, Class::KilledByOtherRule);
    assert_eq!(result.other.as_deref(), Some(OTHER));
}

#[test]
fn an_inconclusive_named_id_survives_inconclusive_not_held() {
    let inconclusive = observed(
        Ended::Exited(0),
        vec![end(None, &[(NAMED, "inconclusive", 0)])],
    );
    assert_eq!(
        classified(&inconclusive),
        (Class::Survived, Some(Reason::Inconclusive))
    );
    // A failed judgement of an inconclusive outcome gives the same reason.
    let judged_inconclusive = observed(
        Ended::Exited(1),
        vec![
            end(None, &[(NAMED, "inconclusive", 0)]),
            judged(Some("inconclusive"), Trigger::None),
        ],
    );
    assert_eq!(
        classified(&judged_inconclusive),
        (Class::Survived, Some(Reason::Inconclusive))
    );
    // Some run that held the named ID with nonzero applicability is `held`,
    // as a baseline requires.
    let one_held = observed(
        Ended::Exited(0),
        vec![
            end(None, &[(NAMED, "inconclusive", 0)]),
            end(None, &[(NAMED, "held", 3)]),
        ],
    );
    assert_eq!(classified(&one_held), (Class::Survived, Some(Reason::Held)));
    // Held with zero applicability never applied.
    let never = observed(Ended::Exited(0), vec![end(None, &[(NAMED, "held", 0)])]);
    assert_eq!(
        classified(&never),
        (Class::Survived, Some(Reason::NotApplicable))
    );
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

/// A run that ended at a harness failure with the named ID violated but
/// nothing stopping it is a harness failure, not a kill. A violation that
/// stopped the run before the failure still kills.
#[test]
fn a_harness_failure_outranks_an_unstopped_named_violation() {
    let failed = |stopped_by: Option<&str>| {
        let mut record = end(stopped_by, &[(NAMED, "violated", 1)]);
        if let Record::End { harness, .. } = &mut record {
            *harness = Some("deadlock".to_owned());
        }
        record
    };
    assert_eq!(
        classified(&observed(Ended::Exited(1), vec![failed(None)])),
        (Class::HarnessFailure, Some(Reason::HarnessEnd))
    );
    assert_eq!(
        classified(&observed(Ended::Exited(1), vec![failed(Some(NAMED))])).0,
        Class::KilledByNamedRule
    );
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

#[cfg(unix)]
#[test]
fn a_baseline_the_memory_cap_killed_reruns_alone() {
    use std::os::unix::fs::PermissionsExt as _;

    let tree = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    fs::create_dir_all(tree.path().join("crates/zakura-sim")).unwrap();
    let (runner, slot, case, _) = one_case(tree.path(), out.path(), &out.path().join("p"), true);
    // A test binary that SIGKILLs itself on its first run, as the memory cap
    // does, and holds the named ID on its second.
    let marker = out.path().join("killed-once");
    let end_file = json!({
        "seq": 0, "thread": "t1", "source": "abc",
        "end": {"properties": [{"property": NAMED, "outcome": {"held": {"applicable": 2}}}]},
    });
    let binary = out.path().join("block_table");
    fs::write(
        &binary,
        format!(
            "#!/bin/sh\n\
             if [ \"$1\" = --list ]; then echo 't: test'; exit 0; fi\n\
             if [ ! -e '{marker}' ]; then : > '{marker}'; kill -9 $$; fi\n\
             printf '%s' '{end_file}' > \"$ZAKURA_SIM_END_RECORDS/1-0000000000.json\"\n",
            marker = marker.display(),
        ),
    )
    .unwrap();
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
    let built = Built {
        sim: None,
        tests: [("block_table".to_owned(), binary)].into_iter().collect(),
    };
    let runnables = [Runnable::Test {
        target: "block_table".to_owned(),
        test: "t".to_owned(),
    }];
    let baselines = runner
        .baselines(&[case], &runnables, &slot, &built, "abc")
        .unwrap();
    assert!(marker.exists(), "the first run was killed");
    assert_eq!(baselines[0].detail, None);
    assert!(baselines[0].passed);
    assert!(baselines[0].rerun_alone);
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
            trigger_history: Trigger::Reached,
        })
    );
    assert!(read_record(r#"{"source":"abc"}"#, "abc").is_err());
    for trigger in [json!("unjudged"), json!("maybe"), json!(null)] {
        let mut file = witness_file.clone();
        file["witness"]["trigger_history"] = trigger;
        assert!(read_record(&file.to_string(), "abc").is_err(), "{file}");
    }
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
        rerun_alone: false,
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
        alone: false,
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
        rerun_alone: false,
    };
    let reports = runner
        .mutant(&slot, &mutant, &cases, &runnables, &[failing], None)
        .unwrap();
    assert_eq!(reports[0].1.result.class, Class::BaselineFails);
    assert_eq!(reports[0].1.millis, None);
    assert_eq!(reports[0].1.trigger, None);
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
        trigger: match class {
            Class::KilledByNamedRule => Some(Trigger::Unjudged),
            Class::Survived => Some(Trigger::Lost),
            _ => None,
        },
        baseline: BaselineReport {
            passed: class != Class::BaselineFails,
            detail: None,
            millis: 1200,
            rerun_alone: class == Class::BaselineFails,
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
        case(Class::Survived, Some(Reason::Inconclusive), NAMED),
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
        "trigger",
        "baseline",
        "millis",
        "peak_rss_kib",
        "log",
    ] {
        assert!(first.get(key).is_some(), "{key}");
    }
    assert_eq!(first["result"]["class"], "killed-by-named-rule");
    assert_eq!(value["cases"][1]["result"]["other"], OTHER);
    assert_eq!(first["trigger"], "unjudged");
    assert!(value["cases"][1].get("trigger").is_none());
    assert_eq!(value["cases"][3]["result"]["reason"], "trigger-lost");
    assert_eq!(value["cases"][3]["trigger"], "lost");
    assert_eq!(value["cases"][4]["result"]["reason"], "inconclusive");
    assert_eq!(value["cases"][7]["baseline"]["rerun_alone"], true);
    assert!(first["baseline"].get("rerun_alone").is_none());
    assert_eq!(
        value["summary"][NAMED]["results"],
        json!({"killed-by-named-rule": 1, "killed-by-other-rule": 1, "survived/held": 1, "survived/trigger-lost": 1, "survived/inconclusive": 1})
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

/// `--planted` selects whole path components or one case, so a prefix that
/// names a directory doesn't also select a sibling that starts the same way.
#[test]
fn planted_prefixes_end_at_a_component() {
    let case = "block-sync/reserved-byte-bound/above-floor-charge.patch";
    assert!(selects("block-sync/", case));
    assert!(selects("block-sync", case));
    assert!(selects("block-sync/reserved-byte-bound", case));
    assert!(selects(
        "block-sync/reserved-byte-bound/above-floor-charge",
        case
    ));
    assert!(!selects("block", case));
    assert!(!selects("block-sync/reserved", case));
    assert!(selects(
        "block",
        "block/work-funding/lost-reservation-release.patch"
    ));
}

/// A case watches its work-list entry's paths beside its patch's own: a
/// change to `admission.rs`, the reserved-byte instance's code, reruns
/// `budget-ignores-charges`, whose patch changes only the shared
/// `guard.rs`.
#[test]
fn a_case_watches_its_work_list_entrys_paths() {
    let repo_root = crate::repo_root().expect("xtask lives in the workspace");
    let cases = super::native_cases(&repo_root, &repo_root.join("qa/native-mutations"))
        .expect("the committed cases parse");
    let case = cases
        .iter()
        .find(|case| case.label.ends_with("budget-ignores-charges.patch"))
        .expect("the bounds entry keeps budget-ignores-charges");
    let watched = super::watched(&repo_root, case).expect("the work list parses");
    for path in [
        "crates/zakura-network/src/zakura/block_sync/admission.rs",
        "crates/zakura-network/src/zakura/transport/guard.rs",
        "crates/zakura-sim/src/domain/shared/bounds.rs",
        "qa/native-mutations/work-list.toml",
    ] {
        assert!(
            crate::planted::is_affected(&watched, &[path.into()]),
            "{path}"
        );
    }
    // Another instance's code doesn't affect it.
    assert!(!crate::planted::is_affected(
        &watched,
        &["crates/zakura-network/src/zakura/block_sync/sequencer_task.rs".into()]
    ));
}
