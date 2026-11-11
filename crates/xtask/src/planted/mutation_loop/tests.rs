//! The loop runner's classes on synthetic runs, its run failures, and its
//! report's schema. No test builds the simulator.

use std::{collections::BTreeMap, fs, path::Path, process::Command};

#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use super::run_measured;
use super::{Built, Runnable};

use serde_json::json;

use super::{
    build_failure,
    classify::{
        self, read_record, Applicability, Class, Ended, GenerateFacts, GenerateStop, Observed,
        Reason, Record, Trigger,
    },
    generate::{
        baseline_lane, campaign_class, co_violations, confirmed, no_generator as no_generator_of,
        plan, write_exclusions, FollowUp, GenBaseline, GenerateConfig, GenerateMode,
        GeneratorBaselineFailure, GeneratorReport, Kill, Lanes, PropertyFinding, Search, Searched,
        Unpatched,
    },
    parse_flags, selects, summarize, test_executables, BaselineReport, BuildIdentity, CaseReport,
    GenerateFlags, LoopCase, Mutant, Report, Runner, Slot, WitnessSpec,
};
use crate::planted::lane::{campaign_facts, Campaign, CampaignFacts};

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
        generate: GenerateFlags::default(),
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
        .mutant(&slot, &mutant, &cases, &runnables, &[baseline], None, None)
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
        .mutant(&slot, &mutant, &cases, &runnables, &[failing], None, None)
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
        generator: Vec::new(),
        no_generator: None,
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
        generate: None,
        property_findings: Vec::new(),
        generator_baseline_failures: Vec::new(),
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

// The generator lane (mut-4.3 and mut-4.4).

/// A search's report that stopped at `stop`, with `NAMED`'s outcome
/// counts `named`, after 40 runs, the last at draw 41 if it stopped.
fn search(stop: GenerateStop, named: Option<Applicability>) -> GenerateFacts {
    let stopped = stop != GenerateStop::Clean;
    GenerateFacts {
        stop,
        kill_run: stopped.then_some(41),
        executed: 40,
        millis: 900,
        input: stopped.then(|| "/out/search/ab.bin".into()),
        family: stopped.then(|| "stuck-floor".to_owned()),
        properties: named
            .map(|counts| BTreeMap::from([(NAMED.to_owned(), counts)]))
            .unwrap_or_default(),
    }
}

fn violation(id: &str) -> GenerateStop {
    GenerateStop::Violation {
        property: id.to_owned(),
    }
}

fn generated(
    also: &[&str],
    observed: &Observed,
    facts: Option<&GenerateFacts>,
) -> (Class, Option<Reason>) {
    let also: Vec<String> = also.iter().map(|id| (*id).to_owned()).collect();
    let result = classify::generated(NAMED, &also, observed, facts);
    (result.class, result.reason)
}

#[test]
fn a_search_that_stops_at_a_violation_kills_by_its_rule() {
    let paired = |id| {
        observed(
            Ended::Exited(0),
            vec![end(Some(id), &[(id, "violated", 0)])],
        )
    };
    assert_eq!(
        generated(&[], &paired(NAMED), Some(&search(violation(NAMED), None))),
        (Class::KilledByNamedRule, None)
    );
    let other = classify::generated(
        NAMED,
        &[],
        &paired(OTHER),
        Some(&search(violation(OTHER), None)),
    );
    assert_eq!(other.class, Class::KilledByOtherRule);
    assert_eq!(other.other.as_deref(), Some(OTHER));
    assert!(other.detail.contains("run 42"), "{}", other.detail);

    // An `also_violates` ID kills by the named rule only beside the named
    // ID's violation in the same run.
    let both = observed(
        Ended::Exited(0),
        vec![end(
            Some(OTHER),
            &[(OTHER, "violated", 0), (NAMED, "violated", 0)],
        )],
    );
    let facts = search(violation(OTHER), None);
    assert_eq!(
        generated(&[OTHER], &both, Some(&facts)),
        (Class::KilledByNamedRule, None)
    );
    assert_eq!(
        generated(&[OTHER], &paired(OTHER), Some(&facts)),
        (Class::KilledByOtherRule, None)
    );

    // A violation without an end record from this build decides nothing.
    let unpaired = Observed {
        unpaired: 1,
        ..observed(Ended::Exited(0), Vec::new())
    };
    assert_eq!(
        generated(&[], &unpaired, Some(&search(violation(NAMED), None))),
        (Class::HarnessFailure, Some(Reason::NoEndRecord))
    );
}

/// An `Active` violation that stopped no step, found at the run's end,
/// writes an end record with no `stopped_by`. It decides the kill as the
/// witness lane's rule does.
#[test]
fn an_end_of_run_violation_kills_by_its_rule() {
    let at_end = |id| observed(Ended::Exited(0), vec![end(None, &[(id, "violated", 0)])]);
    assert_eq!(
        generated(&[], &at_end(NAMED), Some(&search(violation(NAMED), None))),
        (Class::KilledByNamedRule, None)
    );
    let other = classify::generated(
        NAMED,
        &[],
        &at_end(OTHER),
        Some(&search(violation(OTHER), None)),
    );
    assert_eq!(
        (other.class, other.other.as_deref()),
        (Class::KilledByOtherRule, Some(OTHER))
    );
    // A run that ended at a harness failure decides only by a violation
    // that stopped it.
    let harness_end = observed(
        Ended::Exited(0),
        vec![Record::End {
            thread: "t1".to_owned(),
            stopped_by: None,
            violated: vec![NAMED.to_owned()],
            harness: Some("schedule exhausted".to_owned()),
            outcomes: BTreeMap::new(),
        }],
    );
    assert_eq!(
        generated(&[], &harness_end, Some(&search(violation(NAMED), None))),
        (Class::HarnessFailure, Some(Reason::NoEndRecord))
    );
}

#[test]
fn a_clean_search_survives_by_its_outcome_counts() {
    let clean = observed(Ended::Exited(0), Vec::new());
    let counts = |held, inconclusive| Applicability {
        held,
        violated: 0,
        inconclusive,
        not_applicable: 40 - held - inconclusive,
    };
    for (named, expected) in [
        (Some(counts(3, 1)), (Class::Survived, Some(Reason::Held))),
        (
            Some(counts(0, 2)),
            (Class::Survived, Some(Reason::Inconclusive)),
        ),
        (
            Some(counts(0, 0)),
            (Class::Survived, Some(Reason::NotApplicable)),
        ),
        (None, (Class::HarnessFailure, Some(Reason::NoEndRecord))),
    ] {
        assert_eq!(
            generated(&[], &clean, Some(&search(GenerateStop::Clean, named))),
            expected,
            "{named:?}"
        );
    }
}

#[test]
fn a_search_that_cannot_give_a_result_is_a_harness_failure() {
    let clean = search(GenerateStop::Clean, None);
    for (ended, facts, reason) in [
        (Ended::Signaled(9), Some(&clean), Reason::Oom),
        (Ended::TimedOut, Some(&clean), Reason::Timeout),
        (Ended::Signaled(6), Some(&clean), Reason::Panic),
        (Ended::Exited(0), None, Reason::Panic),
        (Ended::Exited(2), None, Reason::Panic),
    ] {
        assert_eq!(
            generated(&[], &observed(ended, Vec::new()), facts),
            (Class::HarnessFailure, Some(reason)),
            "{ended:?} {facts:?}"
        );
    }
    let done = observed(Ended::Exited(0), Vec::new());
    for (stop, reason) in [
        (
            GenerateStop::HarnessFailure {
                reason: "schedule exhausted".to_owned(),
            },
            Reason::HarnessEnd,
        ),
        (GenerateStop::Abandoned { leaked: 3 }, Reason::Abandoned),
        (
            GenerateStop::Panic {
                message: "boom".to_owned(),
            },
            Reason::Panic,
        ),
    ] {
        assert_eq!(
            generated(&[], &done, Some(&search(stop.clone(), None))),
            (Class::HarnessFailure, Some(reason)),
            "{stop:?}"
        );
    }
}

/// An `inproc` search that ended `ended` with report `facts` and
/// `records`.
fn inproc(ended: Ended, records: Vec<Record>, facts: Option<GenerateFacts>) -> Search {
    Search {
        searched: Searched::Inproc(super::Ran {
            observed: observed(ended, records),
            millis: 900,
            peak_rss_kib: None,
            log: "/out/x/witness.log".into(),
            generated: facts,
        }),
        millis: 900,
        dir: "/out/x".into(),
    }
}

#[test]
fn a_kill_names_its_id_and_input_and_needs_its_confirmation() {
    let killed = inproc(
        Ended::Exited(0),
        vec![end(Some(OTHER), &[(OTHER, "violated", 0)])],
        Some(search(violation(OTHER), None)),
    );
    let (result, kill) = killed.classify(NAMED, &[]);
    assert_eq!(result.class, Class::KilledByOtherRule);
    assert_eq!(
        kill,
        Some(Kill {
            id: OTHER.to_owned(),
            input: Some("/out/search/ab.bin".into()),
        })
    );
    let clean = inproc(
        Ended::Exited(0),
        Vec::new(),
        Some(search(GenerateStop::Clean, Some(Applicability::default()))),
    );
    assert_eq!(clean.classify(NAMED, &[]).1, None);

    // A kill whose fresh confirmation fails is a harness failure.
    let unconfirmed = confirmed(
        result.clone(),
        &FollowUp::failed("missing witness".to_owned()),
    );
    assert_eq!(unconfirmed.class, Class::HarnessFailure);
    assert_eq!(unconfirmed.reason, Some(Reason::Unconfirmed));
    assert!(unconfirmed.detail.contains("missing witness"));
    let kept = FollowUp {
        ok: true,
        trace: Some("x/confirmation/confirm/trace".into()),
        detail: None,
    };
    assert_eq!(confirmed(result.clone(), &kept), result);
}

/// A search that ended at a run failure on the unpatched build is a
/// failure to rerun alone, never a property finding. Only a confirmed
/// `Active` violation is a finding.
#[test]
fn only_a_confirmed_unpatched_violation_is_a_property_finding() {
    let stopped = inproc(
        Ended::Exited(0),
        vec![end(Some(NAMED), &[(NAMED, "violated", 0)])],
        Some(search(violation(NAMED), None)),
    );
    assert_eq!(stopped.unpatched(), Unpatched::Violation(NAMED.to_owned()));
    let clean = inproc(
        Ended::Exited(0),
        Vec::new(),
        Some(search(GenerateStop::Clean, None)),
    );
    assert_eq!(clean.unpatched(), Unpatched::Clean);
    assert_eq!(baseline_lane(&Unpatched::Clean, None), (None, None));
    for (ended, reason) in [
        (Ended::Signaled(9), Reason::Oom),
        (Ended::TimedOut, Reason::Timeout),
        (Ended::Signaled(11), Reason::Panic),
    ] {
        let failed = inproc(ended, Vec::new(), Some(search(violation(NAMED), None)));
        let unpatched = failed.unpatched();
        assert!(
            matches!(&unpatched, Unpatched::Failed { result, rerun: true }
                if result.reason == Some(reason)),
            "{ended:?}: {unpatched:?}"
        );
        let (lane, failure) = baseline_lane(&unpatched, None);
        assert_eq!(lane.unwrap().class, Class::BaselineFails);
        assert_eq!(failure.unwrap().reason, Some(reason));
    }
    // A harness failure on the pinned revision isn't a finding, and a run
    // alone wouldn't change it.
    let harness_end = inproc(
        Ended::Exited(0),
        Vec::new(),
        Some(search(
            GenerateStop::HarnessFailure {
                reason: "x".to_owned(),
            },
            None,
        )),
    );
    assert!(matches!(
        harness_end.unpatched(),
        Unpatched::Failed { rerun: false, .. }
    ));

    let violated = Unpatched::Violation(NAMED.to_owned());
    let kept = FollowUp {
        ok: true,
        trace: Some("t".into()),
        detail: None,
    };
    let (lane, failure) = baseline_lane(&violated, Some(&kept));
    let lane = lane.unwrap();
    assert_eq!((lane.class, lane.reason), (Class::BaselineFails, None));
    assert_eq!(failure, None, "a confirmed violation is a finding");
    let (lane, failure) = baseline_lane(&violated, Some(&FollowUp::failed("lost".to_owned())));
    assert_eq!(lane.unwrap().reason, Some(Reason::Unconfirmed));
    assert_eq!(failure.unwrap().reason, Some(Reason::Unconfirmed));

    // A failed baseline makes that harness and seed's lane `baseline-fails`
    // for every case on it, with nothing run.
    let tree = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let (runner, slot, case, _) = one_case(tree.path(), out.path(), &tree.path().join("p"), true);
    let lanes = Lanes {
        flags: GenerateFlags {
            runs: 50,
            seeds: vec![7, 8],
            ..GenerateFlags::default()
        },
        plans: vec![Ok("block-sync/storage".to_owned())],
        baselines: BTreeMap::from([
            (
                ("block-sync/storage".to_owned(), 7),
                GenBaseline {
                    failure: Some(classify::Classified::new(
                        Class::BaselineFails,
                        None,
                        "the unpatched search violates x",
                    )),
                    millis: 10,
                },
            ),
            (
                ("block-sync/storage".to_owned(), 8),
                GenBaseline {
                    failure: Some(classify::Classified::new(
                        Class::BaselineFails,
                        Some(Reason::Timeout),
                        "the search passed its deadline",
                    )),
                    millis: 10,
                },
            ),
        ]),
        exclude: out.path().join("exclude.txt"),
        corpus: out.path().join("corpus"),
        targets: BTreeMap::new(),
    };
    let reports = runner
        .generator_lanes(
            &slot,
            &Built::default(),
            &lanes,
            &[(0, &case)],
            out.path(),
            "source",
            None,
        )
        .unwrap();
    let lanes = &reports[&0];
    assert_eq!(lanes.len(), 2);
    assert!(lanes
        .iter()
        .all(|lane| lane.result.class == Class::BaselineFails && lane.runs.is_none()));
    assert_eq!(lanes[1].seed, 8);
    assert_eq!(lanes[1].result.reason, Some(Reason::Timeout));
    assert!(!out.path().join("generate").exists(), "nothing ran");
}

#[test]
fn a_campaign_lane_gets_the_same_classes() {
    let facts = |violated: &[&str], applied| CampaignFacts {
        status: "complete".to_owned(),
        violated: violated.iter().map(|id| (*id).to_owned()).collect(),
        active: violated.iter().map(|id| (*id).to_owned()).collect(),
        applied,
        runs: 1000,
        applicable_runs: if applied { 10 } else { 0 },
        ..CampaignFacts::default()
    };
    let class = |also: &[&str], facts: &CampaignFacts| {
        let also: Vec<String> = also.iter().map(|id| (*id).to_owned()).collect();
        let result = campaign_class(NAMED, &also, facts);
        (result.class, result.reason, result.other)
    };
    assert_eq!(
        class(&[], &facts(&[NAMED], true)),
        (Class::KilledByNamedRule, None, None)
    );
    assert_eq!(
        class(&[OTHER], &facts(&[NAMED, OTHER], true)),
        (Class::KilledByNamedRule, None, None)
    );
    assert_eq!(
        class(&[], &facts(&[NAMED, OTHER], true)),
        (Class::KilledByOtherRule, None, Some(OTHER.to_owned()))
    );
    assert_eq!(
        class(&[OTHER], &facts(&[OTHER], true)),
        (Class::KilledByOtherRule, None, Some(OTHER.to_owned()))
    );
    assert_eq!(
        class(&[], &facts(&[], true)),
        (Class::Survived, Some(Reason::Held), None)
    );
    assert_eq!(
        class(&[], &facts(&[], false)),
        (Class::Survived, Some(Reason::NotApplicable), None)
    );
    let timed_out = CampaignFacts {
        killed: vec!["artifacts/timeout-1".to_owned()],
        ..facts(&[NAMED], true)
    };
    assert_eq!(
        class(&[], &timed_out),
        (Class::HarnessFailure, Some(Reason::Timeout), None)
    );
    let saved = CampaignFacts {
        failures: vec!["sim/inputs/harness-failure/a.bin".to_owned()],
        ..facts(&[], true)
    };
    assert_eq!(
        class(&[], &saved),
        (Class::HarnessFailure, Some(Reason::HarnessEnd), None)
    );

    // A campaign that didn't complete, or whose evidence falls short, gives
    // no survivor.
    for short in [
        CampaignFacts {
            status: "worker-stopped".to_owned(),
            ..facts(&[], true)
        },
        CampaignFacts {
            incomplete: vec!["\"runs\"".to_owned()],
            ..facts(&[], false)
        },
        CampaignFacts {
            initial_inputs: 3,
            ..facts(&[], true)
        },
    ] {
        assert_eq!(
            class(&[], &short),
            (Class::HarnessFailure, Some(Reason::Incomplete), None),
            "{short:?}"
        );
    }
}

/// A `known` or `draft` input a campaign saved is a note, as in an
/// `inproc` search: no kill, no baseline finding.
#[test]
fn a_campaign_counts_only_active_violations() {
    let end = json!({"failures": [
        {"path": "runs/r/workers/0/sim/inputs/known/x/y/aa.bin"},
        {"path": "runs/r/workers/0/sim/inputs/draft/z/bb.bin"},
    ]});
    let evidence = json!({"status": "complete", "unapplied": [], "totals": {"runs": 9}});
    let facts = campaign_facts(NAMED, Some(&end), &evidence, 0);
    assert_eq!(
        facts.violated,
        ["x/y", "z"],
        "the pilot's reading is unchanged"
    );
    assert!(facts.active.is_empty());
    let result = campaign_class(NAMED, &[], &facts);
    assert_eq!(
        (result.class, result.reason),
        (Class::Survived, Some(Reason::Held))
    );
    let campaign = |end: serde_json::Value, evidence: serde_json::Value| Search {
        searched: Searched::Campaign(Ok(Campaign {
            end: Some(end),
            evidence,
            initial_inputs: 0,
            identity: serde_json::Value::Null,
        })),
        millis: 10,
        dir: "/out/c".into(),
    };
    assert_eq!(
        campaign(end.clone(), evidence.clone()).unpatched(),
        Unpatched::Clean
    );
    let active = json!({"failures": [
        {"path": "runs/r/workers/0/sim/inputs/active/x/y/aa.bin"},
    ]});
    assert_eq!(
        campaign(active, evidence).unpatched(),
        Unpatched::Violation("x/y".to_owned())
    );
    let stopped = json!({"status": "worker-stopped", "unapplied": []});
    assert!(matches!(
        campaign(end, stopped).unpatched(),
        Unpatched::Failed { result, rerun: false } if result.reason == Some(Reason::Incomplete)
    ));
}

/// A kill's confirmation allows the other IDs its run violated, from an
/// end record that a violation stopped, from one that ended violated, or
/// from a campaign's saved violations.
#[test]
fn a_kills_confirmation_allows_its_co_violations() {
    let at_end = inproc(
        Ended::Exited(0),
        vec![end(None, &[(NAMED, "violated", 0), (OTHER, "violated", 0)])],
        Some(search(violation(NAMED), None)),
    );
    assert_eq!(co_violations(&at_end.searched, NAMED), [OTHER]);
    let stopped = inproc(
        Ended::Exited(0),
        vec![end(
            Some(OTHER),
            &[(OTHER, "violated", 0), (NAMED, "violated", 0)],
        )],
        Some(search(violation(OTHER), None)),
    );
    assert_eq!(co_violations(&stopped.searched, OTHER), [NAMED]);
    let campaign = Searched::Campaign(Ok(Campaign {
        end: Some(json!({"failures": [
            {"path": "w/sim/inputs/active/a/x.bin"},
            {"path": "w/sim/inputs/active/b/y.bin"},
            {"path": "w/sim/inputs/known/c/z.bin"},
        ]})),
        evidence: json!({"status": "complete"}),
        initial_inputs: 0,
        identity: serde_json::Value::Null,
    }));
    assert_eq!(co_violations(&campaign, "a"), ["b"]);
}

#[test]
fn each_case_gets_a_generator_harness_or_a_reason() {
    let case = |property: &str| LoopCase {
        label: format!("{property}.patch"),
        patch: "p".into(),
        listed_in: "cases.json".into(),
        property: property.to_owned(),
        witness: WitnessSpec::Test {
            test: "t".to_owned(),
            test_target: "tt".to_owned(),
        },
        entry: None,
        table: None,
        also_violates: Vec::new(),
    };
    let record = |harness: &str| Runnable::Record {
        harness: harness.to_owned(),
        args: Vec::new(),
        property: NAMED.to_owned(),
    };
    let cases = [case(NAMED), case(NAMED), case(OTHER), case("x/unreported")];
    let runnables = [
        record("block-sync/s6v3"),
        record("block-sync/planted"),
        record("block-sync/s6v3"),
        record("block-sync/s6v3"),
    ];
    let map = BTreeMap::from([
        (
            NAMED.to_owned(),
            vec![
                "block-sync/s6v3".to_owned(),
                "block-sync/storage".to_owned(),
            ],
        ),
        ("x/unreported".to_owned(), Vec::new()),
    ]);
    let no_generator = BTreeMap::from([(OTHER.to_owned(), "refused frames".to_owned())]);
    let plans = plan(&cases, &runnables, &map, &no_generator, None, None);
    assert_eq!(
        plans[0],
        Ok("block-sync/s6v3".to_owned()),
        "its own harness"
    );
    assert_eq!(plans[1], Ok("block-sync/s6v3".to_owned()), "the first one");
    assert_eq!(plans[2], Err("no-generator: refused frames".to_owned()));
    assert!(plans[3].is_err());
    let forced = plan(&cases, &runnables, &map, &no_generator, Some("h"), None);
    assert!(forced.iter().all(|plan| plan == &Ok("h".to_owned())));
    let targets = BTreeMap::from([("block-sync/storage".to_owned(), "storage".to_owned())]);
    let libfuzzer = plan(
        &cases,
        &runnables,
        &map,
        &no_generator,
        None,
        Some(&targets),
    );
    assert_eq!(
        libfuzzer[0],
        Err("no fuzz target runs block-sync/s6v3".to_owned())
    );

    // The work list's records parse.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let recorded = no_generator_of(&root).unwrap();
    assert!(!recorded.is_empty());
}

#[test]
fn exclusions_list_every_committed_input_once() {
    let tree = tempfile::tempdir().unwrap();
    for (path, bytes) in [
        ("qa/planted/a/witness.bin", &b"one"[..]),
        ("qa/native-mutations/inputs/b.bin", b"two"),
        ("crates/zakura-sim/tests/fixtures/c.bin", b"one"),
        ("qa/planted/a/witness.toml", b"not an input"),
        ("elsewhere/d.bin", b"three"),
    ] {
        let path = tree.path().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    let file = tree.path().join("out/exclude.txt");
    assert_eq!(write_exclusions(tree.path(), &file).unwrap(), 2);
    let text = fs::read_to_string(&file).unwrap();
    assert_eq!(text.lines().count(), 2);
    assert!(text.lines().all(|line| line.len() == 64));
}

#[test]
fn generate_flags_parse_and_refuse_bad_values() {
    let flags = parse_flags(
        [
            "--out",
            "out",
            "--generate-runs",
            "2000",
            "--generate-seeds",
            "1, 2,3",
            "--generate-harness",
            "block-sync/storage",
            "--generate-mode",
            "libfuzzer",
            "--generate-seconds",
            "30",
        ]
        .map(str::to_owned)
        .into_iter(),
    )
    .unwrap();
    assert_eq!(
        flags.generate,
        GenerateFlags {
            runs: 2000,
            seeds: vec![1, 2, 3],
            harness: Some("block-sync/storage".to_owned()),
            mode: GenerateMode::Libfuzzer,
            seconds: 30,
        }
    );
    assert!(flags.generate.on());
    let off = parse_flags(
        ["--out", "o", "--generate-harness", "auto"]
            .map(str::to_owned)
            .into_iter(),
    )
    .unwrap();
    assert!(!off.generate.on());
    assert_eq!(off.generate.harness, None);
    assert_eq!(off.generate.mode, GenerateMode::Inproc);
    for bad in [
        &["--out", "o", "--generate-runs", "many"][..],
        &["--out", "o", "--generate-seeds", "1,,2"],
        &["--out", "o", "--generate-mode", "afl"],
        &["--out", "o", "--generate-seconds", "0"],
    ] {
        assert!(
            parse_flags(bad.iter().map(|s| (*s).to_owned())).is_err(),
            "{bad:?}"
        );
    }
}

#[test]
fn the_report_keeps_generator_lanes_and_findings_apart() {
    let lane = |class, reason| GeneratorReport {
        violated: (class == Class::KilledByNamedRule).then(|| NAMED.to_owned()),
        runs: Some(42),
        millis: Some(900),
        input: Some("mutants/x/generate/s/search/ab.bin".into()),
        family: Some("stuck-floor".to_owned()),
        confirmation: Some(FollowUp {
            ok: true,
            trace: Some("mutants/x/confirmation/confirm/trace".into()),
            detail: None,
        }),
        shrunk: Some(FollowUp::failed("`zakura-sim shrink` exited 1".to_owned())),
        log: Some("mutants/x/generate/s".into()),
        ..GeneratorReport::unsearched(
            "block-sync/storage",
            11,
            GenerateMode::Inproc,
            classify::Classified::new(class, reason, "why"),
        )
    };
    let case = |generator: Vec<GeneratorReport>, no_generator: Option<&str>| CaseReport {
        patch: "bounds/raise/raise.patch".to_owned(),
        patch_sha256: "00".repeat(32),
        property: NAMED.to_owned(),
        entry: None,
        table: None,
        witness: WitnessSpec::Test {
            test: "t".to_owned(),
            test_target: "block_table".to_owned(),
        },
        also_violates: Vec::new(),
        result: classify::Classified::new(Class::Survived, Some(Reason::Held), "why"),
        trigger: None,
        baseline: BaselineReport {
            passed: true,
            detail: None,
            millis: 10,
            rerun_alone: false,
        },
        millis: Some(10),
        peak_rss_kib: None,
        log: None,
        rerun_alone: false,
        generator,
        no_generator: no_generator.map(str::to_owned),
    };
    let cases = vec![
        case(
            vec![
                lane(Class::KilledByNamedRule, None),
                lane(Class::Survived, Some(Reason::NotApplicable)),
                lane(Class::BaselineFails, None),
                lane(Class::HarnessFailure, Some(Reason::Unconfirmed)),
            ],
            None,
        ),
        case(Vec::new(), Some("no-generator: refused frames")),
    ];
    let report = Report {
        schema: 2,
        pinned: "a".repeat(40),
        prompt_sha256: "b".repeat(64),
        build: BuildIdentity {
            source: "c".repeat(64),
            rustc: "rustc 1.97.0".to_owned(),
            profile: "planted".to_owned(),
            rustflags: "--cfg zakura_sim".to_owned(),
        },
        host: "h".to_owned(),
        jobs: 1,
        memory_max: None,
        generate: Some(GenerateConfig {
            mode: GenerateMode::Inproc,
            runs: 2000,
            seeds: vec![11],
            harness: "auto".to_owned(),
            seconds: None,
            excluded_inputs: 120,
        }),
        property_findings: vec![PropertyFinding {
            harness: "discovery/wire".to_owned(),
            seed: 23,
            mode: GenerateMode::Inproc,
            property: Some(OTHER.to_owned()),
            detail: format!("run 7 of the search violates {OTHER}"),
            runs: Some(7),
            millis: 300,
            input: Some("baseline/generate/d/search/cd.bin".into()),
            family: Some("untagged".to_owned()),
            confirmation: Some(FollowUp {
                ok: true,
                trace: Some("baseline/generate/d/x/confirmation/confirm/trace".into()),
                detail: None,
            }),
            shrunk: None,
            log: "baseline/generate/d".into(),
        }],
        generator_baseline_failures: vec![GeneratorBaselineFailure {
            harness: "block-sync/storage".to_owned(),
            seed: 11,
            mode: GenerateMode::Inproc,
            result: classify::Classified::new(
                Class::HarnessFailure,
                Some(Reason::Oom),
                "the memory cap killed the search",
            ),
            rerun_alone: true,
            log: "baseline/generate/b-alone".into(),
        }],
        summary: summarize(&cases),
        cases,
    };
    let text = serde_json::to_string_pretty(&report).unwrap();
    let back: Report = serde_json::from_str(&text).unwrap();
    assert_eq!(back, report);
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["generate"]["mode"], "inproc");
    assert!(value["generate"].get("seconds").is_none());
    assert_eq!(value["property_findings"][0]["property"], OTHER);
    assert_eq!(
        value["generator_baseline_failures"][0]["result"]["reason"],
        "oom"
    );
    assert_eq!(value["generator_baseline_failures"][0]["rerun_alone"], true);
    let lane = &value["cases"][0]["generator"][0];
    for key in [
        "harness",
        "seed",
        "mode",
        "result",
        "violated",
        "runs",
        "millis",
        "input",
        "family",
        "confirmation",
        "shrunk",
        "log",
    ] {
        assert!(lane.get(key).is_some(), "{key}");
    }
    assert_eq!(lane["result"]["class"], "killed-by-named-rule");
    assert!(value["cases"][1].get("generator").is_none());
    assert_eq!(
        value["cases"][1]["no_generator"],
        "no-generator: refused frames"
    );
    let summary = &value["summary"][NAMED];
    assert_eq!(summary["results"], json!({"survived/held": 2}));
    assert_eq!(
        summary["generator_results"],
        json!({"killed-by-named-rule": 1, "survived/not-applicable": 1})
    );
    assert_eq!(
        summary["generator_run_failures"],
        json!({"baseline-fails": 1, "harness-failure/unconfirmed": 1, "no-generator": 1})
    );

    // A report without generator lanes reads back without them.
    let plain = serde_json::to_value(Report {
        generate: None,
        property_findings: Vec::new(),
        generator_baseline_failures: Vec::new(),
        ..report
    })
    .unwrap();
    assert!(plain.get("generate").is_none());
    assert!(plain.get("property_findings").is_none());
}
