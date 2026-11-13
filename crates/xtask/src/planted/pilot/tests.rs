//! The pilot runner's classification and report tests, on synthetic
//! observations. None builds or runs a simulator; one runs a shell script
//! in its place.

use super::*;
#[cfg(unix)]
use crate::planted::wait_until_executable;
use crate::planted::{end_of, Run};

const ID: &str = "block/reactor-policy";
const OTHER: &str = "sim/no-task-panic";
const HELD: &str = r#"{"held":{"applicable":4}}"#;
const VIOLATED: &str = r#"{"violated":{"witness":[3]}}"#;
/// A `not-applicable` outcome whose check never applied.
const NEVER_APPLIED: &str = r#"{"not-applicable":{"reason":"never-applied"}}"#;

/// The planted runner's reading of an `end` line with `violation`, and
/// `reports` as (property, outcome JSON) pairs.
fn run(violation: Option<&str>, reports: &[(&str, &str)]) -> Run {
    let properties: Vec<String> = reports
        .iter()
        .map(|(property, outcome)| format!(r#"{{"property":"{property}","outcome":{outcome}}}"#))
        .collect();
    let violation = violation.map_or_else(String::new, |property| {
        format!(r#""violation":{{"property":"{property}"}},"#)
    });
    let line = format!(
        r#"{{"end":{{"outcome":"idle",{violation}"steps":4,"cleanup":{{}},"properties":[{}]}}}}"#,
        properties.join(",")
    );
    end_of(ID, &line).expect("the line is an end record")
}

/// `run` whose witness test failed with `why`.
fn witness_failed(run: Run, why: &str) -> Run {
    match run {
        Run::Ended {
            violation,
            others_violated,
            outcome,
            harness,
            ..
        } => Run::Ended {
            violation,
            others_violated,
            outcome,
            harness,
            witness: Some(why.to_owned()),
        },
        failed => failed,
    }
}

/// A clean quiescent trace with `applicable` evaluations of the intended
/// property.
fn trace(applicable: Option<u64>) -> TraceFacts {
    TraceFacts {
        build: Value::Null,
        harness: ID.to_owned(),
        input_sha256: "ab".repeat(32),
        steps: 4,
        status: Some("quiescent".to_owned()),
        cleanup: BTreeMap::from([("cancelled".to_owned(), 0), ("leaked".to_owned(), 0)]),
        applicable,
    }
}

fn recorded(run: Run, trace: Option<TraceFacts>) -> Recorded {
    Recorded {
        run,
        trace,
        unpaired: None,
    }
}

/// A sensitivity case whose baseline holds with 4 applicable evaluations,
/// whose patch applies and compiles, and whose mutant run is `mutant`.
fn sensitivity_run(mutant: Run, confirmation: Option<Result<(), String>>) -> SensitivityRun {
    SensitivityRun {
        equivalent: None,
        broken_build: None,
        baseline: Some(Ok(recorded(run(None, &[(ID, HELD)]), Some(trace(Some(4)))))),
        patch: Some(Ok(())),
        mutant: Some(Ok(recorded(mutant, Some(trace(None))))),
        confirmation,
    }
}

fn class(run: &SensitivityRun) -> Class {
    classify::sensitivity(ID, run).class
}

/// A confirmed violation of the intended ID, on a passing baseline, is the
/// only intended violation.
#[test]
fn sensitivity_counts_only_a_confirmed_intended_violation() {
    let caught = run(Some(ID), &[(ID, VIOLATED)]);
    assert_eq!(
        class(&sensitivity_run(caught.clone(), Some(Ok(())))),
        Class::IntendedViolation
    );
    assert_eq!(
        class(&sensitivity_run(caught.clone(), None)),
        Class::HarnessFailure
    );
    let replay = classify::sensitivity(
        ID,
        &sensitivity_run(
            caught,
            Some(Err("the confirmed trace doesn't replay Exact".to_owned())),
        ),
    );
    assert_eq!(replay.class, Class::HarnessFailure);
    assert!(replay.reason.contains("Exact"), "{}", replay.reason);
}

/// A violation of another ID, or of the intended ID beside another, never
/// counts as the intended violation.
#[test]
fn sensitivity_rejects_wrong_id_failures() {
    for mutant in [
        run(Some(OTHER), &[(ID, HELD), (OTHER, VIOLATED)]),
        run(None, &[(ID, VIOLATED), (OTHER, VIOLATED)]),
    ] {
        let result = classify::sensitivity(ID, &sensitivity_run(mutant, Some(Ok(()))));
        assert_eq!(result.class, Class::OtherViolation);
        assert!(result.reason.contains(OTHER), "{}", result.reason);
    }
}

/// A violation without its trigger history, or whose fresh confirmation
/// loses it, is a harness failure that names the missing witness.
#[test]
fn sensitivity_rejects_missing_witnesses() {
    let lost = witness_failed(run(Some(ID), &[(ID, VIOLATED)]), "lost its trigger history");
    let result = classify::sensitivity(ID, &sensitivity_run(lost, Some(Ok(()))));
    assert_eq!(result.class, Class::HarnessFailure);
    assert!(
        result.reason.starts_with("missing witness"),
        "{}",
        result.reason
    );

    let why = confirmed_trace(
        ChildEnd::Exited(1),
        Some(&serde_json::json!({"status": "witness-lost", "detail": null})),
    )
    .expect_err("a lost witness doesn't confirm");
    assert!(why.starts_with("missing witness"), "{why}");
    let caught = run(Some(ID), &[(ID, VIOLATED)]);
    assert_eq!(
        class(&sensitivity_run(caught, Some(Err(why)))),
        Class::HarnessFailure
    );
}

/// A baseline that violates, never applies, applies zero times, fails its
/// witness, doesn't settle, or doesn't pair with the catalogue's build
/// blocks the case, even if the mutant violates the intended ID.
#[test]
fn sensitivity_rejects_failed_baselines() {
    let caught = run(Some(ID), &[(ID, VIOLATED)]);
    let unsettled = TraceFacts {
        status: Some("deadlock".to_owned()),
        ..trace(Some(4))
    };
    let mut unpaired = recorded(run(None, &[(ID, HELD)]), Some(trace(Some(4))));
    unpaired.unpaired = Some("the original build doesn't pair: rustc differ".to_owned());
    for baseline in [
        Err("the original build fails".to_owned()),
        Ok(recorded(
            run(Some(ID), &[(ID, VIOLATED)]),
            Some(trace(None)),
        )),
        Ok(recorded(
            run(None, &[(ID, HELD), (OTHER, VIOLATED)]),
            Some(trace(Some(4))),
        )),
        Ok(recorded(
            run(None, &[(ID, NEVER_APPLIED)]),
            Some(trace(None)),
        )),
        Ok(recorded(run(None, &[(ID, HELD)]), Some(trace(Some(0))))),
        Ok(recorded(
            witness_failed(run(None, &[(ID, HELD)]), "lost its trigger history"),
            Some(trace(Some(4))),
        )),
        Ok(recorded(run(None, &[(ID, HELD)]), Some(unsettled))),
        Ok(recorded(run(None, &[(ID, HELD)]), None)),
        Ok(unpaired),
    ] {
        let mut case = sensitivity_run(caught.clone(), Some(Ok(())));
        case.baseline = Some(baseline);
        assert_eq!(class(&case), Class::BaselineFailure, "{:?}", case.baseline);
    }
}

/// A patch that doesn't apply, changes undeclared files or doesn't compile
/// makes an invalid mutant, never a survivor or a detection.
#[test]
fn sensitivity_rejects_broken_patches() {
    let mut case = sensitivity_run(run(None, &[(ID, HELD)]), None);
    case.patch = Some(Err("the patch doesn't apply: a.rs".to_owned()));
    case.mutant = None;
    assert_eq!(class(&case), Class::InvalidMutant);
}

/// A run that reports nothing for the intended ID, ends at a harness
/// failure, leaks state, crashes, or doesn't pair with the original's build
/// has incomplete observations: a harness failure.
#[test]
fn sensitivity_rejects_incomplete_observations() {
    let leaky = TraceFacts {
        cleanup: BTreeMap::from([("leaked".to_owned(), 2)]),
        ..trace(None)
    };
    let mut leaked = sensitivity_run(run(Some(ID), &[(ID, VIOLATED)]), Some(Ok(())));
    leaked.mutant = Some(Ok(recorded(run(Some(ID), &[(ID, VIOLATED)]), Some(leaky))));
    let mut unpaired = sensitivity_run(run(Some(ID), &[(ID, VIOLATED)]), Some(Ok(())));
    if let Some(Ok(mutant)) = &mut unpaired.mutant {
        mutant.unpaired = Some("the mutant build doesn't pair: source differ".to_owned());
    }
    let mut missing = sensitivity_run(run(None, &[]), None);
    missing.mutant = None;
    for case in [
        sensitivity_run(run(None, &[]), None),
        sensitivity_run(
            run(
                None,
                &[(ID, r#"{"not-applicable":{"reason":"harness-failed"}}"#)],
            ),
            None,
        ),
        sensitivity_run(Run::Failed("ran past its deadline".to_owned()), None),
        leaked,
        unpaired,
        missing,
    ] {
        assert_eq!(class(&case), Class::HarnessFailure, "{case:?}");
    }
}

/// Each `not-applicable` reason, read from its serialized end record: only
/// `never-applied` is [`Class::NotApplicable`], and every other reason,
/// benign or not, is incomplete observations.
#[test]
fn sensitivity_classifies_each_not_applicable_reason() {
    for (reason, expected) in [
        (r#""never-applied""#, Class::NotApplicable),
        (r#""undeclared""#, Class::HarnessFailure),
        (r#""excluded""#, Class::HarnessFailure),
        (r#""assumptions-never-held""#, Class::HarnessFailure),
        (r#""assumption-lost""#, Class::HarnessFailure),
        (r#""stopped-by-violation""#, Class::HarnessFailure),
        (r#""harness-failed""#, Class::HarnessFailure),
        (r#""node-stopped""#, Class::HarnessFailure),
        (
            r#"{"unsettled":{"site":"watch-open"}}"#,
            Class::HarnessFailure,
        ),
    ] {
        let outcome = format!(r#"{{"not-applicable":{{"reason":{reason}}}}}"#);
        assert_eq!(
            class(&sensitivity_run(run(None, &[(ID, &outcome)]), None)),
            expected,
            "{reason}"
        );
    }
}

/// A held mutant run survives, a never-applying one is not applicable, and
/// only review metadata makes a case equivalent.
#[test]
fn sensitivity_names_survivors_and_equivalents() {
    assert_eq!(
        class(&sensitivity_run(run(None, &[(ID, HELD)]), None)),
        Class::SurvivedBudget
    );
    assert_eq!(
        class(&sensitivity_run(run(None, &[(ID, NEVER_APPLIED)]), None)),
        Class::NotApplicable
    );
    let review = SensitivityRun {
        equivalent: Some("the branch is unreachable within the harness".to_owned()),
        ..SensitivityRun::default()
    };
    assert_eq!(class(&review), Class::Equivalent);
    // Nothing that ran makes a case equivalent.
    assert_ne!(class(&SensitivityRun::default()), Class::Equivalent);
}

/// A complete, clean campaign from the empty corpus.
fn campaign() -> CampaignFacts {
    CampaignFacts {
        status: "complete".to_owned(),
        applied: true,
        runs: 1000,
        applicable_runs: 600,
        millis: 600_000,
        ..CampaignFacts::default()
    }
}

/// A mutant campaign that the intended finding stopped.
fn found(property: &str) -> CampaignFacts {
    CampaignFacts {
        status: "worker-stopped".to_owned(),
        violated: vec![property.to_owned()],
        incomplete: vec![r#"{"reason":"restart-limit","worker":0}"#.to_owned()],
        intended_input: (property == ID)
            .then(|| "runs/x/workers/0/sim/inputs/active/a.bin".to_owned()),
        ..campaign()
    }
}

fn discovery_run(mutant: CampaignFacts, confirmation: Option<Result<(), String>>) -> DiscoveryRun {
    DiscoveryRun {
        equivalent: None,
        broken_build: None,
        baseline: Some(Ok(campaign())),
        patch: Some(Ok(())),
        mutant: Some(Ok(mutant)),
        confirmation,
    }
}

fn discovered(run: &DiscoveryRun) -> Class {
    classify::discovery(ID, &[], run).class
}

/// A confirmed finding of the intended ID is an intended violation, though
/// the finding stopped the campaign early. Another ID's finding isn't.
#[test]
fn discovery_counts_only_a_confirmed_intended_finding() {
    assert_eq!(
        discovered(&discovery_run(found(ID), Some(Ok(())))),
        Class::IntendedViolation
    );
    assert_eq!(
        discovered(&discovery_run(found(ID), None)),
        Class::HarnessFailure
    );
    assert_eq!(
        discovered(&discovery_run(
            found(ID),
            Some(Err(
                "missing witness: the fresh confirmation is witness-lost".to_owned()
            ))
        )),
        Class::HarnessFailure
    );
    assert_eq!(
        discovered(&discovery_run(found(OTHER), Some(Ok(())))),
        Class::OtherViolation
    );
    let mut both = found(ID);
    both.violated.push(OTHER.to_owned());
    assert_eq!(
        discovered(&discovery_run(both, Some(Ok(())))),
        Class::OtherViolation
    );
}

/// A baseline campaign that violates anything, leaves a failure, stops
/// early, starts from a nonempty corpus or doesn't pair blocks every
/// mutant finding.
#[test]
fn discovery_rejects_failed_baselines() {
    let mut cases = vec![
        Err(NoCampaign::Failed(
            "the original simulator fails to build".to_owned(),
        )),
        Ok(found(OTHER)),
        Ok(CampaignFacts {
            failures: vec!["runs/x/workers/0/sim/inputs/nondeterministic/a.bin".to_owned()],
            ..campaign()
        }),
        Ok(CampaignFacts {
            status: "interrupted".to_owned(),
            ..campaign()
        }),
        Ok(CampaignFacts {
            initial_inputs: 3,
            ..campaign()
        }),
    ];
    cases.push(Ok(CampaignFacts {
        unpaired: Some("the original build doesn't pair: rustc differ".to_owned()),
        ..campaign()
    }));
    for baseline in cases {
        let mut run = discovery_run(found(ID), Some(Ok(())));
        run.baseline = Some(baseline);
        assert_eq!(
            discovered(&run),
            Class::BaselineFailure,
            "{:?}",
            run.baseline
        );
    }
    // A baseline that never applies the property still passes.
    let mut run = discovery_run(found(ID), Some(Ok(())));
    run.baseline = Some(Ok(CampaignFacts {
        applied: false,
        ..campaign()
    }));
    assert_eq!(discovered(&run), Class::IntendedViolation);
}

/// A baseline campaign whose `start` ran past its deadline or died from a
/// signal, or whose worker timed out or crashed without a violation, is a
/// harness failure, even beside a violation: it never counts as a baseline
/// failure, and it blocks the mutant's finding.
#[test]
fn a_baseline_campaign_that_times_out_or_is_killed_is_a_harness_failure() {
    let timeout = "runs/x/workers/0/artifacts/timeout-1".to_owned();
    let crash = "runs/x/workers/0/artifacts/crash-2".to_owned();
    for baseline in [
        Err(NoCampaign::Killed(
            "`zakura-sim campaign start` ran past 900 s".to_owned(),
        )),
        Err(NoCampaign::Killed(
            "`zakura-sim campaign start` died from signal 9".to_owned(),
        )),
        Ok(CampaignFacts {
            killed: vec![timeout.clone()],
            ..campaign()
        }),
        Ok(CampaignFacts {
            killed: vec![crash],
            ..campaign()
        }),
        Ok(CampaignFacts {
            killed: vec![timeout],
            ..found(OTHER)
        }),
    ] {
        let mut run = discovery_run(found(ID), Some(Ok(())));
        run.baseline = Some(baseline);
        let result = classify::discovery(ID, &[], &run);
        assert_eq!(result.class, Class::HarnessFailure, "{:?}", run.baseline);
        assert_eq!(
            classify::discovery_baseline(&run.baseline).map(|failed| failed.class),
            Some(Class::HarnessFailure),
            "{:?}",
            run.baseline
        );
    }
}

/// A campaign that stops without a finding, leaves another failure, or
/// starts from a nonempty corpus is a harness failure. A complete campaign
/// that never applies the property is not applicable, and only one that
/// applied it survives.
#[test]
fn discovery_rejects_incomplete_observations() {
    for mutant in [
        CampaignFacts {
            status: "worker-stopped".to_owned(),
            incomplete: vec![r#"{"reason":"restart-limit","worker":0}"#.to_owned()],
            ..campaign()
        },
        CampaignFacts {
            failures: vec!["runs/x/workers/0/sim/inputs/nondeterministic/a.bin".to_owned()],
            ..campaign()
        },
        CampaignFacts {
            incomplete: vec![r#"{"reason":"partial-stats","worker":0,"pid":7}"#.to_owned()],
            ..campaign()
        },
        CampaignFacts {
            initial_inputs: 1,
            ..found(ID)
        },
    ] {
        assert_eq!(
            discovered(&discovery_run(mutant.clone(), Some(Ok(())))),
            Class::HarnessFailure,
            "{mutant:?}"
        );
    }
    let mut failed = discovery_run(campaign(), None);
    failed.mutant = Some(Err(NoCampaign::Failed(
        "the store holds no single run manifest".to_owned(),
    )));
    assert_eq!(discovered(&failed), Class::HarnessFailure);
    failed.mutant = Some(Err(NoCampaign::Killed(
        "`zakura-sim campaign start` ran past 900 s".to_owned(),
    )));
    assert_eq!(discovered(&failed), Class::HarnessFailure);
    assert_eq!(
        discovered(&discovery_run(
            CampaignFacts {
                applied: false,
                applicable_runs: 0,
                ..campaign()
            },
            None
        )),
        Class::NotApplicable
    );
    assert_eq!(
        discovered(&discovery_run(campaign(), None)),
        Class::SurvivedBudget
    );
}

/// A broken patch or a mutant fuzz target that doesn't compile makes an
/// invalid mutant, and review metadata alone an equivalent one.
#[test]
fn discovery_rejects_broken_patches() {
    let mut run = discovery_run(campaign(), None);
    run.patch = Some(Err("the mutant fuzz target doesn't compile".to_owned()));
    run.mutant = None;
    assert_eq!(discovered(&run), Class::InvalidMutant);
    run.equivalent = Some("reviewed".to_owned());
    assert_eq!(discovered(&run), Class::Equivalent);
}

/// The store's failure files sort into violations by property, crashes a
/// violation explains, worker deaths and other failures; traces go with
/// their inputs.
#[test]
fn campaign_facts_read_saved_failures() {
    let worker = "runs/m/workers/0";
    let end = |paths: &[&str]| {
        serde_json::json!({
            "failures": paths.iter().map(|path| serde_json::json!({"path": path, "sha256": "00"})).collect::<Vec<_>>(),
            "workers": [{"index": 0, "millis": 1500}],
        })
    };
    let evidence = serde_json::json!({
        "status": "worker-stopped",
        "totals": {"runs": 42},
        "complete_totals": {"properties": {ID: {"held": 5, "violated": 0}}},
        "unapplied": [],
        "incomplete": [{"reason": "restart-limit", "worker": 0}],
    });
    let intended = format!("{worker}/sim/inputs/active/block/reactor-policy/aa.bin");
    let crash = format!("{worker}/artifacts/crash-bb");
    let trace = format!("{worker}/sim/traces/block/reactor-policy/cc/trace.jsonl");
    let facts = campaign_facts(
        ID,
        Some(&end(&[&intended, &crash, &trace])),
        &evidence,
        0,
        &[],
    );
    assert_eq!(facts.violated, [ID]);
    assert!(facts.failures.is_empty(), "{:?}", facts.failures);
    assert_eq!(facts.intended_input.as_deref(), Some(intended.as_str()));
    assert_eq!(
        (facts.runs, facts.applicable_runs, facts.millis),
        (42, 5, 1500)
    );
    assert_eq!(facts.status, "worker-stopped");
    assert_eq!(facts.incomplete.len(), 1);
    assert!(facts.applied);

    let timeout = format!("{worker}/artifacts/timeout-dd");
    let nondeterministic =
        format!("{worker}/sim/inputs/nondeterministic/block/reactor-policy/ee.bin");
    let facts = campaign_facts(
        ID,
        Some(&end(&[&crash, &timeout, &nondeterministic])),
        &evidence,
        0,
        &[],
    );
    assert!(facts.violated.is_empty());
    assert_eq!(facts.failures, [nondeterministic]);
    assert_eq!(facts.killed, [timeout, crash]);

    let unapplied = serde_json::json!({"status": "complete", "unapplied": [ID], "incomplete": []});
    assert!(!campaign_facts(ID, None, &unapplied, 0, &[]).applied);
    assert_eq!(
        campaign_facts(ID, None, &serde_json::json!({}), 0, &[]).status,
        "none"
    );
}

/// Paired campaigns get the catalogue's bounds, and their lane's seed and
/// arm: their arguments differ only in the store, the name and the build.
#[test]
fn paired_campaigns_share_their_budget() {
    let catalogue = committed_catalogue();
    let bounds = &catalogue.discovery;
    let case = &catalogue.cases[0];
    let corpus = Path::new("qa/mutation-pilot/discovery-empty");
    let lane = Lane {
        seed: 11,
        arm: Arm::On,
    };
    let baseline = start_args(
        bounds,
        "t-on-11",
        case,
        "aa",
        lane,
        corpus,
        Path::new("s/baseline"),
    );
    let mutant = start_args(
        bounds,
        "c-on-11",
        case,
        "bb",
        lane,
        corpus,
        Path::new("s/mutant"),
    );
    let differ: Vec<usize> = (0..baseline.len())
        .filter(|i| baseline[*i] != mutant[*i])
        .collect();
    assert_eq!(baseline.len(), mutant.len());
    let flags: Vec<&OsString> = differ.iter().map(|i| &baseline[i - 1]).collect();
    assert_eq!(flags, ["--store", "--campaign", "--build"]);
    let text: Vec<String> = baseline
        .iter()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    let flag = |name: &str| {
        text[text
            .iter()
            .position(|a| a == name)
            .expect("the flag is passed")
            + 1]
        .clone()
    };
    assert_eq!(flag("--seconds"), "600");
    assert_eq!(flag("--max-restarts"), "0");
    assert_eq!(flag("--fixtures"), "qa/mutation-pilot/discovery-empty");
    assert!(!text.iter().any(|a| a == "--parent" || a == "--corpus"));
}

/// An original must match the catalogue's build and be clean; a mutant
/// must share every other field, be dirty, and change the source.
#[test]
fn paired_builds_share_their_identity() {
    let expected = serde_json::json!({"git": "g", "rustc": "r", "source": "s", "dirty": false});
    let original = expected.clone();
    assert_eq!(
        paired_identity(&expected, &original, &["git", "rustc"], false),
        None
    );
    let other_rustc = serde_json::json!({"git": "g", "rustc": "q", "source": "s", "dirty": false});
    assert!(
        paired_identity(&expected, &other_rustc, &["git", "rustc"], false)
            .expect("rustc differs")
            .contains("rustc")
    );
    let mutant = serde_json::json!({"git": "g", "rustc": "r", "source": "t", "dirty": true});
    assert_eq!(
        paired_identity(&expected, &mutant, &["git", "rustc"], true),
        None
    );
    let unchanged = serde_json::json!({"git": "g", "rustc": "r", "source": "s", "dirty": true});
    assert!(
        paired_identity(&expected, &unchanged, &["git", "rustc"], true)
            .expect("the patch changed no source")
            .contains("source")
    );
    assert!(paired_identity(&expected, &mutant, &["git", "rustc"], false).is_some());
}

/// A trace's meta and end records give its build, input, settling and the
/// intended property's applicability.
#[test]
fn traces_give_their_facts() {
    let text = [
        r#"{"meta":{"sim":"block/reactor-policy","input":{"sha256":"ab","len":3},"build":{"git":"g"}}}"#,
        r#"{"step":1}"#,
        r#"{"end":{"outcome":"idle","status":"quiescent","steps":4,"cleanup":{"cancelled":0,"leaked":0},"properties":[{"property":"block/reactor-policy","outcome":{"held":{"applicable":4}}}]}}"#,
    ]
    .join("\n");
    let facts = parse_trace(&text, ID).expect("the trace parses");
    assert_eq!(facts.harness, ID);
    assert_eq!(facts.input_sha256, "ab");
    assert_eq!(facts.applicable, Some(4));
    assert_eq!(facts.status.as_deref(), Some("quiescent"));
    assert_eq!(facts.build, serde_json::json!({"git": "g"}));
    assert_eq!(
        parse_trace(&text, OTHER)
            .expect("the trace parses")
            .applicable,
        None
    );
    assert!(parse_trace("", ID).is_err());
    assert!(parse_trace(r#"{"step":1}"#, ID).is_err());
}

/// A confirmation must say `confirmed`, exit 0 and name its trace.
#[test]
fn confirmations_must_confirm() {
    let confirmed = serde_json::json!({"status": "confirmed", "trace": "traces/a/b"});
    assert_eq!(
        confirmed_trace(ChildEnd::Exited(0), Some(&confirmed)),
        Ok(PathBuf::from("traces/a/b"))
    );
    assert!(confirmed_trace(ChildEnd::TimedOut, Some(&confirmed)).is_err());
    assert!(confirmed_trace(ChildEnd::Exited(1), None).is_err());
    let other = serde_json::json!({"status": "other-property", "detail": OTHER});
    assert!(confirmed_trace(ChildEnd::Exited(1), Some(&other))
        .expect_err("another property doesn't confirm")
        .contains(OTHER));
    let untraced = serde_json::json!({"status": "confirmed"});
    assert!(confirmed_trace(ChildEnd::Exited(0), Some(&untraced)).is_err());
}

/// A patch must change exactly its declared production files.
#[test]
fn patches_change_only_declared_files() {
    let declared = BTreeMap::from([("crates/a.rs".to_owned(), "00".to_owned())]);
    let changed = |paths: &[&str]| {
        paths
            .iter()
            .map(|p| (*p).to_owned())
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(undeclared(&changed(&["crates/a.rs"]), &declared), None);
    let extra = undeclared(&changed(&["crates/a.rs", "crates/b.rs"]), &declared)
        .expect("b.rs isn't declared");
    assert!(extra.contains("crates/b.rs"), "{extra}");
    assert!(undeclared(&changed(&[]), &declared)
        .expect("a.rs is unchanged")
        .contains("crates/a.rs"));
}

/// `prepare_patch` rejects a patch that doesn't apply and one that changes
/// an undeclared file, in a real checkout.
#[test]
fn broken_patches_are_invalid_mutants() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let root = dir.path();
    let git_ok = |args: &[&str]| {
        let status = git(root)
            .args(["-c", "user.name=pilot", "-c", "user.email=pilot@test"])
            .args(args)
            .status()
            .expect("git runs");
        assert!(status.success(), "git {args:?}");
    };
    fs::write(root.join("a.rs"), "fn a() {}\n").expect("a.rs is written");
    fs::write(root.join("b.rs"), "fn b() {}\n").expect("b.rs is written");
    git_ok(&["init", "-q"]);
    git_ok(&["add", "."]);
    git_ok(&["commit", "-q", "-m", "base"]);
    let mut case = committed_catalogue().cases[0].clone();
    case.patch = PathBuf::from("p.patch");
    case.production_files = BTreeMap::from([("a.rs".to_owned(), String::new())]);
    let patch = |text: &str| fs::write(root.join("p.patch"), text).expect("the patch is written");
    let reset = || {
        git_ok(&["checkout", "-q", "--", "."]);
    };

    patch("--- a/a.rs\n+++ b/a.rs\n@@ -1 +1 @@\n-fn z() {}\n+fn c() {}\n");
    let why = prepare_patch(root, &case)
        .expect("git runs")
        .expect_err("the context isn't in a.rs");
    assert!(why.starts_with("the patch doesn't apply"), "{why}");

    patch("--- a/b.rs\n+++ b/b.rs\n@@ -1 +1 @@\n-fn b() {}\n+fn c() {}\n");
    let why = prepare_patch(root, &case)
        .expect("git runs")
        .expect_err("b.rs isn't declared");
    assert!(why.contains("undeclared files [b.rs"), "{why}");
    reset();

    patch("--- a/a.rs\n+++ b/a.rs\n@@ -1 +1 @@\n-fn a() {}\n+fn c() {}\n");
    // The patch file itself is untracked, so declare it for this check.
    case.production_files
        .insert("p.patch".to_owned(), String::new());
    assert_eq!(prepare_patch(root, &case).expect("git runs"), Ok(()));
}

/// The pilot reads each case's patch at the pinned revision. A later
/// change to the patch in the tree the pilot runs from leaves the run
/// alone, and a patch that exists in that tree but not at the pin fails
/// the pin check, naming the case.
#[test]
fn patches_come_from_the_pinned_revision() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let root = dir.path().join("repo");
    fs::create_dir(&root).expect("the repository directory is made");
    let git_ok = |args: &[&str]| {
        let status = git(&root)
            .args(["-c", "user.name=pilot", "-c", "user.email=pilot@test"])
            .args(args)
            .status()
            .expect("git runs");
        assert!(status.success(), "git {args:?}");
    };
    let pinned = "--- a/a.rs\n+++ b/a.rs\n@@ -1 +1 @@\n-fn a() {}\n+fn pinned() {}\n";
    fs::write(root.join("a.rs"), "fn a() {}\n").expect("a.rs is written");
    fs::write(root.join("w.bin"), "witness").expect("the witness is written");
    fs::write(root.join("p.patch"), pinned).expect("the patch is written");
    git_ok(&["init", "-q"]);
    git_ok(&["add", "."]);
    git_ok(&["commit", "-q", "-m", "pin"]);
    let pin = head(&root).expect("the pin has a commit");
    let mut case = committed_catalogue().cases[0].clone();
    case.patch = PathBuf::from("p.patch");
    case.patch_sha256 = file_sha256(&root.join("p.patch")).expect("the patch hashes");
    case.witness = PathBuf::from("w.bin");
    case.witness_sha256 = file_sha256(&root.join("w.bin")).expect("the witness hashes");
    case.production_files = BTreeMap::from([(
        "a.rs".to_owned(),
        file_sha256(&root.join("a.rs")).expect("a.rs hashes"),
    )]);
    // After the pin, the tree's patch changes another line, and a new
    // case's patch appears only in the tree.
    fs::write(
        root.join("p.patch"),
        "--- a/a.rs\n+++ b/a.rs\n@@ -1 +1 @@\n-fn a() {}\n+fn later() {}\n",
    )
    .expect("the patch is rewritten");
    fs::write(root.join("q.patch"), pinned).expect("the new patch is written");
    git_ok(&["add", "."]);
    git_ok(&["commit", "-q", "-m", "later"]);

    let tree = dir.path().join("pinned");
    add_worktree(&root, &tree, &pin).expect("the pin checks out");
    check_pins(&tree, &pin, &[&case]).expect("the pin-era patch matches its hash");
    assert_eq!(prepare_patch(&tree, &case).expect("git runs"), Ok(()));
    assert_eq!(
        fs::read_to_string(tree.join("a.rs")).expect("a.rs is read"),
        "fn pinned() {}\n"
    );

    let mut unpinned = case.clone();
    unpinned.id = "later/case".to_owned();
    unpinned.patch = PathBuf::from("q.patch");
    let why = check_pins(&tree, &pin, &[&case, &unpinned])
        .expect_err("q.patch isn't at the pin")
        .to_string();
    assert!(why.contains(&pin), "{why}");
    assert!(why.contains("later/case: q.patch"), "{why}");
    assert!(!why.contains(&case.id), "{why}");
    remove_worktree(&root, &tree).expect("the worktree is removed");
}

/// The committed catalogue.
fn committed_catalogue() -> Catalogue {
    let root = crate::repo_root().expect("xtask lives in the workspace");
    serde_json::from_slice(&fs::read(root.join(CATALOGUE)).expect("the catalogue is committed"))
        .expect("the catalogue parses")
}

/// Zero selected cases or seeds, and an unknown case or seed, fail instead
/// of producing an empty report.
#[test]
fn zero_selected_tests_fail() {
    let catalogue = committed_catalogue();
    assert_eq!(select(&catalogue.cases, &[]).expect("every case").len(), 10);
    let one = select(&catalogue.cases, &[catalogue.cases[3].id.clone()]).expect("one case");
    assert_eq!(one.len(), 1);
    assert!(select(&catalogue.cases, &["no/such/case".to_owned()])
        .expect_err("the ID isn't in the catalogue")
        .contains("no/such/case"));
    assert!(select(&[], &[])
        .expect_err("no case")
        .contains("zero cases"));
    assert_eq!(select_allocated(&[11, 23], &[], "seed"), Ok(vec![11, 23]));
    assert_eq!(select_allocated(&[11, 23], &[23], "seed"), Ok(vec![23]));
    assert!(select_allocated(&[11, 23], &[5], "seed").is_err());
    assert!(select_allocated::<u32>(&[], &[], "seed").is_err());
    assert!(select_allocated(&[11, 23, 11], &[], "seed")
        .expect_err("a seed allocated twice")
        .contains("twice"));
    assert_eq!(
        select_allocated(&[Arm::On, Arm::Off], &[Arm::Off], "arm"),
        Ok(vec![Arm::Off])
    );
    assert!(classify::summarize(std::iter::empty()).is_err());
    assert!(sensitivity_report(meta(), Vec::new()).is_err());
    let unseeded = DiscoveryCase {
        id: "a".to_owned(),
        fault_class: "f".to_owned(),
        property: ID.to_owned(),
        target: "t".to_owned(),
        seeds: Vec::new(),
        arms: Vec::new(),
        cleanup: None,
    };
    assert!(discovery_report(meta(), vec![unseeded], Vec::new()).is_err());
}

fn meta() -> Meta {
    Meta {
        experiment: "sensitivity".to_owned(),
        catalogue_sha256: "00".to_owned(),
        built_revision: "r".to_owned(),
        runner_revision: "h".to_owned(),
        reviewed_cases: 10,
    }
}

fn classified(class: Class) -> Classified {
    Classified {
        class,
        reason: String::new(),
    }
}

/// The rate counts every result but baseline failures, invalid mutants and
/// equivalent mutants, keeps not-applicable results in, and splits by
/// fault class.
#[test]
fn reports_count_classes_and_rates() {
    let row = |fault_class: &str, class: Class| SensitivityCase {
        id: format!("{fault_class}/{class:?}"),
        fault_class: fault_class.to_owned(),
        property: ID.to_owned(),
        harness: ID.to_owned(),
        result: classified(class),
        baseline: None,
        mutant: None,
        artifacts: PathBuf::new(),
        cleanup: None,
    };
    let report = sensitivity_report(
        meta(),
        vec![
            row("stale", Class::IntendedViolation),
            row("stale", Class::NotApplicable),
            row("stale", Class::BaselineFailure),
            row("queue", Class::IntendedViolation),
            row("queue", Class::SurvivedBudget),
            row("queue", Class::InvalidMutant),
            row("queue", Class::Equivalent),
            row("queue", Class::HarnessFailure),
            row("queue", Class::OtherViolation),
        ],
    )
    .expect("the report has results");
    let summary = &report.summary;
    assert_eq!(
        (summary.results, summary.denominator, summary.intended),
        (9, 6, 2)
    );
    assert_eq!(summary.classes.len(), 8);
    assert_eq!(summary.classes[&Class::NotApplicable], 1);
    let stale = &summary.by_fault_class["stale"];
    assert_eq!(
        (stale.results, stale.denominator, stale.intended),
        (3, 2, 1)
    );
    let queue = &summary.by_fault_class["queue"];
    assert_eq!(
        (queue.results, queue.denominator, queue.intended),
        (6, 4, 1)
    );
    let json = serde_json::to_value(&report).expect("the report serializes");
    assert_eq!(json["experiment"], "sensitivity");
    assert_eq!(json["cases"][0]["result"]["class"], "intended-violation");
    assert_eq!(json["summary"]["classes"]["not-applicable"], 1);
    // Sensitivity has no arms.
    assert!(report.by_arm.is_empty());
    assert!(json.get("by_arm").is_none());

    // Discovery counts each seed of each case, in all and in each arm.
    let seed = |seed: u32, class: Class| SeedResult {
        seed,
        arm: if seed == 23 { Arm::Off } else { Arm::On },
        result: classified(class),
        baseline: None,
        mutant: None,
        store: None,
    };
    let case = DiscoveryCase {
        id: "a".to_owned(),
        fault_class: "stale".to_owned(),
        property: ID.to_owned(),
        target: "t".to_owned(),
        seeds: vec![
            seed(11, Class::IntendedViolation),
            seed(23, Class::NotApplicable),
            seed(47, Class::BaselineFailure),
        ],
        arms: Vec::new(),
        cleanup: None,
    };
    let excluded = Excluded {
        id: "b".to_owned(),
        fault_class: "stale".to_owned(),
        reason: "never applied".to_owned(),
    };
    let report = discovery_report(meta(), vec![case], vec![excluded.clone()])
        .expect("the report has results");
    assert_eq!(
        (
            report.summary.results,
            report.summary.denominator,
            report.summary.intended
        ),
        (3, 2, 1)
    );
    assert_eq!(report.summary.by_fault_class["stale"].results, 3);
    assert_eq!(report.excluded, vec![excluded]);
    let arm = |arm: Arm| {
        let summary = &report.by_arm[&arm];
        (summary.results, summary.denominator, summary.intended)
    };
    assert_eq!((arm(Arm::On), arm(Arm::Off)), ((2, 1, 1), (1, 1, 0)));
    let json = serde_json::to_value(&report).expect("the report serializes");
    assert_eq!(json["by_arm"]["off"]["classes"]["not-applicable"], 1);
}

/// Discovery leaves out exactly the cases the catalogue excludes, the two
/// record-book cases whose target never applied its property in
/// calibration. Sensitivity still selects them, a discovery selection of
/// only excluded cases fails, and an exclusion needs a reason.
#[test]
fn discovery_leaves_out_excluded_cases() {
    let catalogue = committed_catalogue();
    // Sensitivity runs every selected case, the record-book cases included.
    let cases = select(&catalogue.cases, &[]).expect("every case");
    assert_eq!(cases.len(), 10);
    let (scored, excluded) = discovery_cases(&cases).expect("some case is scored");
    assert_eq!(scored.len(), 8);
    let ids: Vec<&str> = excluded.iter().map(|case| case.id.as_str()).collect();
    assert_eq!(
        ids,
        [
            "discovery/record-book/keep-old-relay-attribution",
            "discovery/record-book/wrong-eviction-victim"
        ]
    );
    assert!(excluded.iter().all(|case| !case.reason.is_empty()));
    assert!(scored
        .iter()
        .all(|case| case.target != "discovery_record_book"));
    let only = select(&catalogue.cases, &[ids[0].to_owned()]).expect("one case");
    assert!(discovery_cases(&only)
        .expect_err("only an excluded case")
        .contains("excluded"));
    assert_eq!(check_exclusions(&catalogue.cases), Ok(()));
    for blank in ["", " "] {
        let mut cases = catalogue.cases.clone();
        cases[0].discovery_excluded = Some(blank.to_owned());
        assert!(check_exclusions(&cases)
            .expect_err("an exclusion needs a reason")
            .contains(&cases[0].id));
    }
}

/// The failure kinds a store's paths name.
#[test]
fn failure_paths_name_their_kind() {
    let worker = "runs/m/workers/0";
    assert_eq!(
        failure_kind(&format!("{worker}/sim/inputs/known/a/b/cc.bin")),
        Failure::Violation("a/b".to_owned())
    );
    assert_eq!(
        failure_kind("runs/m/compact-sim/inputs/active/a/cc.bin"),
        Failure::Violation("a".to_owned())
    );
    assert_eq!(
        failure_kind(&format!("{worker}/sim/inputs/harness/cc.bin")),
        Failure::Other
    );
    assert_eq!(
        failure_kind(&format!("{worker}/sim/inputs/active/cc.bin")),
        Failure::Other
    );
    assert_eq!(
        failure_kind(&format!("{worker}/artifacts/oom-1")),
        Failure::Other
    );
    assert_eq!(
        failure_kind("runs/m/compact-artifacts/crash-1"),
        Failure::Crash
    );
    assert_eq!(
        failure_kind(&format!("{worker}/sim/traces/a/t/trace.jsonl")),
        Failure::Companion
    );
}

/// Every committed case pins production files, and its toolchain parses
/// from the catalogue. The patch hashes pin the patches at the pinned
/// revision, which a shallow checkout lacks, so `check_pins` checks them
/// when a run starts.
#[test]
fn the_committed_catalogue_pins_its_cases() {
    let catalogue = committed_catalogue();
    assert_eq!(toolchain(&catalogue.baseline_build), Ok("1.99.0"));
    for case in &catalogue.cases {
        assert!(!case.production_files.is_empty(), "{}", case.id);
        assert!(
            case.equivalent.is_none(),
            "{}: no review marked it equivalent",
            case.id
        );
    }
}

/// An interrupted child stops the pilot with an error, so no result reads
/// it as an invalid mutant or a harness failure. Every other end passes
/// through. A set interrupt flag stops the pilot before its report.
#[test]
fn an_interrupt_stops_the_pilot() {
    let error = stop_on_interrupt(ChildEnd::Interrupted).expect_err("an interrupt stops the pilot");
    assert_eq!(error.to_string(), INTERRUPTED_ERROR);
    for end in [
        ChildEnd::Exited(0),
        ChildEnd::Exited(1),
        ChildEnd::TimedOut,
        ChildEnd::Signaled(9),
    ] {
        assert_eq!(stop_on_interrupt(end).expect("not an interrupt"), end);
    }
    let error = check_interrupt(&AtomicBool::new(true)).expect_err("no report after an interrupt");
    assert_eq!(error.to_string(), INTERRUPTED_ERROR);
    check_interrupt(&AtomicBool::new(false)).expect("no interrupt");
}

/// A witness run that dies mid-trace, from a crash or at its deadline,
/// leaves a trace with no end record, and a run can write its witness
/// result without a trace. Each is a harness failure for a mutant, naming
/// how the run ended, and the experiment goes on. For the original, a run
/// past its deadline is a harness failure, and the others are baseline
/// failures.
#[cfg(unix)]
#[test]
fn a_witness_run_that_dies_mid_trace_is_classified() {
    use std::os::unix::fs::PermissionsExt as _;

    const PARTIAL: &str = "printf '%s\\n' '{\"meta\":{}}' '{\"step\":1}' > \"$out/trace.jsonl\"";
    let dir = tempfile::tempdir().expect("a temporary directory");
    let catalogue = committed_catalogue();
    let case = &catalogue.cases[0];
    let partial_then = |death: &str| format!("{PARTIAL}\n{death}");
    for (name, writes, ended, why, original) in [
        (
            "crash",
            partial_then("exit 1"),
            "exited with code 1",
            "no end record",
            Class::BaselineFailure,
        ),
        (
            "hang",
            partial_then("exec sleep 30"),
            "ran past its deadline",
            "no end record",
            Class::HarnessFailure,
        ),
        (
            "untraced",
            "echo '{\"passed\":true,\"error\":null}' > \"$out/witness.json\"".to_owned(),
            "exited with code 0",
            "trace is unreadable",
            Class::BaselineFailure,
        ),
    ] {
        // A stand-in for `zakura-sim record` that writes `writes` into its
        // `--out` directory.
        let sim = dir.path().join(name);
        fs::write(
            &sim,
            format!(
                "#!/bin/sh\n\
                 [ \"$1\" = record ] || exit 0\n\
                 while [ $# -gt 0 ]; do [ \"$1\" = --out ] && out=$2; shift; done\n\
                 mkdir -p \"$out\"\n\
                 {writes}\n"
            ),
        )
        .expect("the script is written");
        fs::set_permissions(&sim, fs::Permissions::from_mode(0o755))
            .expect("the script is executable");
        wait_until_executable(&sim);
        let recorded = witness_run(
            &sim,
            dir.path(),
            case,
            &catalogue,
            false,
            &dir.path().join(format!("{name}-out")),
            Duration::from_millis(500),
            &AtomicBool::new(false),
        )
        .expect("the run's output doesn't stop the experiment");
        let (Run::Failed(failed) | Run::Killed(failed)) = &recorded.run else {
            panic!("{name}: the run didn't fail: {:?}", recorded.run);
        };
        assert!(failed.contains(ended), "{name}: {failed}");
        assert!(failed.contains(why), "{name}: {failed}");
        assert_eq!(recorded.trace, None, "{name}");

        let baseline = classify::sensitivity_baseline(ID, &Some(Ok(recorded.clone())))
            .expect("the baseline fails");
        assert_eq!(baseline.class, original, "{name}");
        assert!(
            baseline.reason.contains(ended),
            "{name}: {}",
            baseline.reason
        );

        let mutant = SensitivityRun {
            mutant: Some(Ok(recorded)),
            ..sensitivity_run(run(None, &[(ID, HELD)]), None)
        };
        let result = classify::sensitivity(ID, &mutant);
        assert_eq!(result.class, Class::HarnessFailure, "{name}");
        assert!(result.reason.contains(ended), "{name}: {}", result.reason);
    }
}

/// `--work` and `--out` come back absolute, against the current
/// directory: the builds run cargo from their worktree, which would read a
/// relative `CARGO_TARGET_DIR` under the worktree.
#[test]
fn relative_flags_become_absolute() {
    let args = ["--work", "pilot-work", "--out", "pilot-out", "--seed", "11"];
    let flags = parse_flags(args.into_iter().map(str::to_owned)).expect("the flags parse");
    let cwd = std::env::current_dir().expect("a current directory");
    assert_eq!(flags.work, Some(cwd.join("pilot-work")));
    assert_eq!(flags.out, cwd.join("pilot-out"));
    assert_eq!(flags.seeds, [11]);
    let absolute =
        parse_flags(["--out", "/a/b"].into_iter().map(str::to_owned)).expect("the flags parse");
    assert_eq!(absolute.out, Path::new("/a/b"));
    assert_eq!(absolute.work, None);
    assert!(parse_flags(["--work", "w"].into_iter().map(str::to_owned)).is_err());
}

/// A campaign that left a libFuzzer timeout beside a confirmed finding of
/// the intended ID is a harness failure: a timeout never counts as
/// detection.
#[test]
fn a_timeout_beside_a_finding_is_a_harness_failure() {
    let worker = "runs/m/workers/0";
    let intended = format!("{worker}/sim/inputs/active/block/reactor-policy/aa.bin");
    let crash = format!("{worker}/artifacts/crash-bb");
    let timeout = format!("{worker}/artifacts/timeout-dd");
    let failures: Vec<Value> = [&intended, &crash, &timeout]
        .iter()
        .map(|path| serde_json::json!({"path": path, "sha256": "00"}))
        .collect();
    let end = serde_json::json!({
        "failures": failures,
        "workers": [{"index": 0, "millis": 1500}],
    });
    let evidence = serde_json::json!({
        "status": "worker-stopped",
        "totals": {"runs": 42},
        "unapplied": [],
        "incomplete": [{"reason": "restart-limit", "worker": 0}],
    });
    let facts = campaign_facts(ID, Some(&end), &evidence, 0, &[]);
    assert_eq!(facts.violated, [ID]);
    assert!(facts.failures.is_empty(), "{:?}", facts.failures);
    assert_eq!(facts.killed, std::slice::from_ref(&timeout));
    let result = classify::discovery(ID, &[], &discovery_run(facts, Some(Ok(()))));
    assert_eq!(result.class, Class::HarnessFailure, "{}", result.reason);
    assert!(result.reason.contains(&timeout), "{}", result.reason);
    // Another ID's finding beside a timeout doesn't count either.
    let mut other = found(OTHER);
    other.killed = vec![timeout];
    assert_eq!(
        discovered(&discovery_run(other, Some(Ok(())))),
        Class::HarnessFailure
    );
}

/// A witness run that passes its deadline, or that a signal kills, fails
/// though it already wrote a whole trace and its witness result. Its trace
/// isn't scored: the original's run and a mutant's are each a harness
/// failure, never a baseline failure, and a mutant's is one even beside a
/// confirmed violation.
#[cfg(unix)]
#[test]
fn a_witness_run_that_times_out_or_is_killed_is_not_scored() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let catalogue = committed_catalogue();
    let case = &catalogue.cases[0];
    let property = &case.property;
    // A whole trace of the intended violation on a paired mutant build.
    let mut build = catalogue.baseline_build.clone();
    build["dirty"] = Value::Bool(true);
    build["source"] = Value::String("mutated".to_owned());
    let meta = serde_json::json!({"meta": {
        "sim": case.harness,
        "input": {"sha256": case.witness_sha256},
        "build": build,
    }});
    let end = serde_json::json!({"end": {
        "outcome": "idle",
        "violation": {"property": property},
        "status": "quiescent",
        "steps": 4,
        "cleanup": {},
        "properties": [{"property": property, "outcome": {"violated": {"witness": [3]}}}],
    }});
    let trace = dir.path().join("trace.jsonl");
    fs::write(&trace, format!("{meta}\n{{\"step\":1}}\n{end}\n")).expect("the trace is written");
    for (name, death, ended) in [
        ("hang", "exec sleep 30", "ran past its deadline"),
        ("killed", "kill -KILL $$", "died from signal 9"),
    ] {
        // A stand-in for `zakura-sim record` that writes the whole trace and
        // a passing witness result, then dies by `death`.
        let sim = dir.path().join(name);
        fs::write(
            &sim,
            format!(
                "#!/bin/sh\n\
                 [ \"$1\" = record ] || exit 0\n\
                 while [ $# -gt 0 ]; do [ \"$1\" = --out ] && out=$2; shift; done\n\
                 mkdir -p \"$out\"\n\
                 cp '{}' \"$out/trace.jsonl\"\n\
                 echo '{{\"passed\":true,\"error\":null}}' > \"$out/witness.json\"\n\
                 {death}\n",
                trace.display()
            ),
        )
        .expect("the script is written");
        fs::set_permissions(&sim, fs::Permissions::from_mode(0o755))
            .expect("the script is executable");
        wait_until_executable(&sim);
        let recorded = witness_run(
            &sim,
            dir.path(),
            case,
            &catalogue,
            true,
            &dir.path().join(format!("{name}-out")),
            Duration::from_millis(500),
            &AtomicBool::new(false),
        )
        .expect("the run's end doesn't stop the experiment");
        let Run::Killed(failed) = &recorded.run else {
            panic!("{name}: a run that {ended} was scored: {:?}", recorded.run);
        };
        assert!(failed.contains(ended), "{name}: {failed}");

        let baseline = classify::sensitivity_baseline(property, &Some(Ok(recorded.clone())))
            .expect("the baseline fails");
        assert_eq!(baseline.class, Class::HarnessFailure, "{name}");
        assert!(
            baseline.reason.contains(ended),
            "{name}: {}",
            baseline.reason
        );
        // The baseline's harness failure is the case's result, and no
        // mutant runs beside it.
        let original = SensitivityRun {
            baseline: Some(Ok(recorded.clone())),
            ..sensitivity_run(run(Some(ID), &[(ID, VIOLATED)]), Some(Ok(())))
        };
        assert_eq!(class(&original), Class::HarnessFailure, "{name}");

        let mutant = SensitivityRun {
            mutant: Some(Ok(recorded)),
            ..sensitivity_run(run(None, &[(ID, HELD)]), Some(Ok(())))
        };
        let result = classify::sensitivity(property, &mutant);
        assert_eq!(result.class, Class::HarnessFailure, "{name}");
        assert!(result.reason.contains(ended), "{name}: {}", result.reason);
    }
}

/// A cargo JSON message from package `package` at `level`, with a source
/// span if `located`.
fn cargo_message(package: &str, level: &str, text: &str, located: bool) -> String {
    let spans = if located {
        serde_json::json!([{"file_name": "crates/zakura-network/src/a.rs", "line_start": 12}])
    } else {
        serde_json::json!([])
    };
    serde_json::json!({
        "reason": "compiler-message",
        "package_id": package,
        "message": {"level": level, "message": text, "spans": spans, "rendered": text},
    })
    .to_string()
}

/// Only a build that exits with a code after rustc reports an error at a
/// source location is rejected. A killed compiler, a full disk, a failed
/// link or an internal compiler error is a broken build.
#[test]
fn only_a_located_compile_error_rejects_a_build() {
    const WORKSPACE: &str = "path+file:///w/crates/zakura-network#13.0.0";
    const REGISTRY: &str = "registry+https://github.com/rust-lang/crates.io-index#serde@1.0.0";
    let rejected = |failure: BuildFailure| matches!(failure, BuildFailure::Rejected(_));
    let sim = |end: ChildEnd, log: &[String]| {
        rejected(sim_build_failure(end, &log.join("\n"), "why".to_owned()))
    };
    let mismatched = cargo_message(WORKSPACE, "error", "mismatched types", true);
    let progress = "   Compiling zakura-network v13.0.0 (/w/crates/zakura-network)".to_owned();
    assert!(sim(
        ChildEnd::Exited(101),
        &[progress.clone(), mismatched.clone()]
    ));
    let BuildFailure::Rejected(why) =
        sim_build_failure(ChildEnd::Exited(101), &mismatched, "see log".to_owned())
    else {
        panic!("a located error rejects the build");
    };
    assert!(
        why.contains("mismatched types at crates/zakura-network/src/a.rs:12"),
        "{why}"
    );
    for (end, log) in [
        // The compiler was killed for memory after an error.
        (ChildEnd::Signaled(9), vec![mismatched.clone()]),
        (ChildEnd::TimedOut, vec![mismatched.clone()]),
        // A full disk and a failed link have no source location.
        (
            ChildEnd::Exited(101),
            vec![cargo_message(
                WORKSPACE,
                "error",
                "failed to write: No space left on device (os error 28)",
                false,
            )],
        ),
        (
            ChildEnd::Exited(101),
            vec![cargo_message(
                WORKSPACE,
                "error",
                "linking with `cc` failed",
                false,
            )],
        ),
        // Cargo reports a compiler that a signal killed only in its text.
        (
            ChildEnd::Exited(101),
            vec![
                progress.clone(),
                "error: could not compile `zakura-network` (lib)".to_owned(),
                "process didn't exit successfully: `rustc` (signal: 9, SIGKILL: kill)".to_owned(),
            ],
        ),
        // A warning, an internal compiler error and a dependency's error.
        (
            ChildEnd::Exited(101),
            vec![cargo_message(WORKSPACE, "warning", "unused", true)],
        ),
        (
            ChildEnd::Exited(101),
            vec![cargo_message(
                WORKSPACE,
                "error: internal compiler error",
                "unexpected panic",
                true,
            )],
        ),
        (
            ChildEnd::Exited(101),
            vec![cargo_message(REGISTRY, "error", "mismatched types", true)],
        ),
    ] {
        assert!(!sim(end, &log), "{end}: {log:?}");
    }

    let fuzz = |end: ChildEnd, log: &str| rejected(fuzz_build_failure(end, log, "why".to_owned()));
    let located = "   Compiling zakura-network v13.0.0\n\
                   error[E0425]: cannot find value `x` in this scope\n  \
                   --> /w/crates/zakura-network/src/a.rs:12:5\n   |\n\
                   error: could not compile `zakura-network` (lib) due to 1 previous error\n\
                   Error: cargo fuzz build of block_reactor_policy failed with exit status: 1\n";
    assert!(fuzz(ChildEnd::Exited(1), located));
    assert!(fuzz(
        ChildEnd::Exited(1),
        "error: unused variable: `y`\n --> crates/a.rs:3:9\n"
    ));
    for (end, log) in [
        (ChildEnd::Signaled(9), located),
        (
            ChildEnd::Exited(1),
            "error: could not compile `zakura-network` (lib)\n\nCaused by:\n  \
             process didn't exit successfully: `rustc` (signal: 9, SIGKILL: kill)\n",
        ),
        (
            ChildEnd::Exited(1),
            "error: failed to write /w/target/a.rlib: No space left on device (os error 28)\n\n\
             error: could not compile `zakura-network` (lib) due to 1 previous error\n",
        ),
        (
            ChildEnd::Exited(1),
            "error: linking with `cc` failed: exit status: 1\n  |\n  = note: ld: out of memory\n",
        ),
        (
            ChildEnd::Exited(101),
            "error: internal compiler error: unexpected panic\n --> crates/a.rs:3:9\n",
        ),
        (
            ChildEnd::Exited(1),
            "warning: unused import\n --> crates/a.rs:1:5\n",
        ),
    ] {
        assert!(!fuzz(end, log), "{end}: {log}");
    }
}

/// A broken build is a harness failure, not an invalid mutant or a
/// baseline failure, whichever build broke. A mutant fuzz target that
/// doesn't compile makes every later seed invalid, and one whose build
/// broke fails only its own seed.
#[test]
fn a_broken_build_is_a_harness_failure() {
    let broke = "the mutant build broke: `cargo build` died from signal 9".to_owned();
    let mut sensitivity = SensitivityRun {
        broken_build: Some(broke.clone()),
        ..SensitivityRun::default()
    };
    let result = classify::sensitivity(ID, &sensitivity);
    assert_eq!(result.class, Class::HarnessFailure);
    assert_eq!(result.reason, broke);
    // A mutant build that broke after a passing baseline.
    sensitivity = SensitivityRun {
        broken_build: Some(broke.clone()),
        patch: None,
        mutant: None,
        confirmation: None,
        ..sensitivity_run(run(None, &[(ID, HELD)]), None)
    };
    assert_eq!(class(&sensitivity), Class::HarnessFailure);

    let mut discovery = discovery_run(campaign(), None);
    discovery.patch = None;
    discovery.mutant = None;
    discovery.broken_build = Some(broke.clone());
    assert_eq!(discovered(&discovery), Class::HarnessFailure);
    // An original whose build broke.
    discovery = DiscoveryRun {
        broken_build: Some(broke),
        ..DiscoveryRun::default()
    };
    assert_eq!(discovered(&discovery), Class::HarnessFailure);

    let sim = PathBuf::from("zakura-sim");
    let mut mutant = Ok(sim.clone());
    let mut first = discovery_run(campaign(), None);
    first.mutant = None;
    fuzz_build_failed(
        &mut first,
        &mut mutant,
        BuildFailure::Broken("the mutant fuzz target's build broke".to_owned()),
    );
    assert_eq!(discovered(&first), Class::HarnessFailure);
    assert_eq!(mutant, Ok(sim), "a broken build fails only its seed");
    fuzz_build_failed(
        &mut first,
        &mut mutant,
        BuildFailure::Rejected("the mutant fuzz target doesn't compile".to_owned()),
    );
    assert!(
        matches!(mutant, Err(BuildFailure::Rejected(_))),
        "a compile error holds for every later seed"
    );
}

/// `campaign_lane` reads a failed `zakura-sim campaign build`: a located
/// rustc error rejects the build, and a build that a signal killed, or
/// that reports no location, broke.
#[cfg(unix)]
#[test]
fn a_campaign_build_is_rejected_only_for_a_compile_error() {
    use std::os::unix::fs::PermissionsExt as _;

    // `campaign_lane` runs its children against the pilot's interrupt flag.
    let _flag = crate::sim_minimize::tests::INTERRUPT_FLAG_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let dir = tempfile::tempdir().expect("a temporary directory");
    let catalogue = committed_catalogue();
    let case = &catalogue.cases[0];
    let located = "printf '%s\\n' 'error[E0425]: cannot find value `x`' \
                   '  --> crates/a.rs:12:5' >&2";
    let full = "echo 'error: failed to write: No space left on device (os error 28)' >&2";
    for (name, body, compile_error) in [
        ("located", format!("{located}\nexit 1"), true),
        ("killed", format!("{located}\nkill -KILL $$"), false),
        ("full", format!("{full}\nexit 1"), false),
    ] {
        // A stand-in for `zakura-sim` whose `campaign build` runs `body`.
        let sim = dir.path().join(name);
        fs::write(
            &sim,
            format!("#!/bin/sh\n[ \"$1 $2\" = 'campaign build' ] || exit 0\n{body}\n"),
        )
        .expect("the script is written");
        fs::set_permissions(&sim, fs::Permissions::from_mode(0o755))
            .expect("the script is executable");
        wait_until_executable(&sim);
        let lane = campaign_lane(
            &sim,
            &dir.path().join(format!("{name}-lane")),
            &dir.path().join("fuzz-target"),
            case,
            Lane {
                seed: 11,
                arm: Arm::On,
            },
            &catalogue.discovery,
            &Pairing::of(&Pin::Catalogue(&catalogue), dir.path()),
            dir.path(),
            true,
        )
        .expect("the lane's build failure doesn't stop the experiment");
        match lane {
            Err(LaneError::Build(BuildFailure::Rejected(why))) => {
                assert!(compile_error, "{name}: rejected: {why}");
                assert!(why.contains("crates/a.rs:12:5"), "{name}: {why}");
            }
            Err(LaneError::Build(BuildFailure::Broken(why))) => {
                assert!(!compile_error, "{name}: broken: {why}");
            }
            lane => panic!("{name}: {lane:?}"),
        }
    }
}

/// `campaign_lane` reads a `zakura-sim campaign start` or `campaign
/// evidence` that a signal killed as a killed campaign, though `start` left
/// a manifest and snapshot: the original's lane is then a harness failure,
/// never a baseline failure.
#[cfg(unix)]
#[test]
fn a_campaign_that_a_signal_killed_is_not_read() {
    use std::os::unix::fs::PermissionsExt as _;

    // `campaign_lane` runs its children against the pilot's interrupt flag.
    let _flag = crate::sim_minimize::tests::INTERRUPT_FLAG_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    const RECORDS: &str = "mkdir -p \"$store/manifests\" \"$store/snapshots/s\"\n\
                           echo '{\"initial_snapshot\":\"s\"}' > \"$store/manifests/r.json\"\n\
                           echo '{\"inputs\":[]}' > \"$store/snapshots/s/snapshot.json\"";
    let dir = tempfile::tempdir().expect("a temporary directory");
    let catalogue = committed_catalogue();
    let case = &catalogue.cases[0];
    for (name, start, evidence, killed) in [
        (
            "start",
            format!("{RECORDS}\nkill -KILL $$"),
            "exit 0",
            "`zakura-sim campaign start` died from signal 9",
        ),
        (
            "evidence",
            format!("{RECORDS}\nexit 1"),
            "kill -KILL $$",
            "`zakura-sim campaign evidence` died from signal 9",
        ),
    ] {
        // A stand-in for `zakura-sim` whose `campaign build` makes one
        // build, and whose `campaign start` and `campaign evidence` run
        // `start` and `evidence`.
        let sim = dir.path().join(name);
        fs::write(
            &sim,
            format!(
                "#!/bin/sh\n\
                 store=$4\n\
                 case \"$1 $2\" in\n\
                 'campaign build') mkdir -p \"$store/builds/b\" ;;\n\
                 'campaign start') {start} ;;\n\
                 'campaign evidence') {evidence} ;;\n\
                 esac\n"
            ),
        )
        .expect("the script is written");
        fs::set_permissions(&sim, fs::Permissions::from_mode(0o755))
            .expect("the script is executable");
        wait_until_executable(&sim);
        let lane = campaign_lane(
            &sim,
            &dir.path().join(format!("{name}-lane")),
            &dir.path().join("fuzz-target"),
            case,
            Lane {
                seed: 11,
                arm: Arm::On,
            },
            &catalogue.discovery,
            &Pairing::of(&Pin::Catalogue(&catalogue), dir.path()),
            dir.path(),
            false,
        )
        .expect("the lane's death doesn't stop the experiment");
        let Err(LaneError::Killed(why)) = lane else {
            panic!("{name}: a killed campaign was read: {lane:?}");
        };
        assert!(why.contains(killed), "{name}: {why}");
        let baseline = classify::discovery_baseline(&Some(Err(NoCampaign::Killed(why))))
            .expect("the baseline fails");
        assert_eq!(baseline.class, Class::HarnessFailure, "{name}");
    }
}

/// A campaign's facts time its first intended finding by the process that
/// crashed on it, and count its unique abstract states and its longest
/// worker's time.
#[test]
fn campaign_facts_time_the_intended_finding() {
    let intended = "runs/m/workers/0/sim/inputs/active/block/reactor-policy/aa.bin";
    let end = serde_json::json!({
        "failures": [{"path": intended, "sha256": "aa"}],
        "workers": [{"index": 0, "millis": 3500}, {"index": 1, "millis": 9000}],
    });
    let evidence = serde_json::json!({
        "status": "worker-stopped",
        "totals": {"runs": 42, "features": {"1": 3, "4097": 1}},
        "unapplied": [],
        "incomplete": [],
    });
    let attempt = |millis: u64, crash: &str, sha256: &str| {
        serde_json::json!({
            "millis": millis,
            "artifacts": [{"path": format!("runs/m/workers/0/artifacts/{crash}"), "sha256": sha256}],
        })
    };
    // Worker 0's second process crashed on the intended input after 3.5 s
    // in all; worker 1's first process did after 5 s.
    let attempts = vec![
        vec![
            attempt(1000, "crash-1", "ff"),
            attempt(2500, "crash-2", "aa"),
        ],
        vec![attempt(5000, "crash-3", "aa")],
    ];
    let facts = campaign_facts(ID, Some(&end), &evidence, 0, &attempts);
    assert_eq!(facts.intended_input.as_deref(), Some(intended));
    assert_eq!(facts.first_intended_secs, Some(3.5));
    assert_eq!(facts.features_reached, 2);
    assert_eq!(facts.worker_secs, 9.0);

    // A timeout or a crash on another input doesn't time the finding.
    let unmatched = vec![vec![
        attempt(1000, "timeout-1", "aa"),
        attempt(1000, "crash-2", "ff"),
    ]];
    let facts = campaign_facts(ID, Some(&end), &evidence, 0, &unmatched);
    assert_eq!(facts.first_intended_secs, None);
    assert!(facts.intended_input.is_some());

    // Without an intended input, nothing times a finding.
    let other = serde_json::json!({
        "failures": [{"path": "runs/m/workers/0/sim/inputs/active/sim/no-task-panic/aa.bin", "sha256": "aa"}],
        "workers": [{"index": 0, "millis": 600_000}],
    });
    let facts = campaign_facts(ID, Some(&other), &evidence, 0, &attempts);
    assert_eq!(facts.intended_input, None);
    assert_eq!(facts.first_intended_secs, None);
    assert_eq!(facts.worker_secs, 600.0);
    assert_eq!(
        campaign_facts(ID, None, &serde_json::json!({}), 0, &[]).features_reached,
        0
    );
}

/// The Kaplan-Meier median counts a censored run at risk until it stops,
/// reaches one half exactly, and is missing when half the runs or more
/// end censored.
#[test]
fn the_median_time_to_detection_counts_censored_runs() {
    use compare::median_detection;
    assert_eq!(median_detection(&[]), None);
    let all: Vec<(f64, bool)> = (1..=10).map(|secs| (f64::from(secs), true)).collect();
    assert_eq!(median_detection(&all), Some(5.0));
    assert_eq!(
        median_detection(&[(4.0, true), (1.0, true), (3.0, true), (2.0, true)]),
        Some(2.0)
    );
    // The undetected share is 3/4 after 1 s, and 3/4 × 1/2 after 3 s.
    assert_eq!(
        median_detection(&[(1.0, true), (2.0, false), (3.0, true), (4.0, false)]),
        Some(3.0)
    );
    // A run censored early leaves the risk set: 2/3 after 2 s, 1/3 after 3 s.
    assert_eq!(
        median_detection(&[(1.0, false), (2.0, true), (3.0, true), (600.0, false)]),
        Some(3.0)
    );
    // A run censored at a detection time stays at risk through it.
    assert_eq!(median_detection(&[(10.0, true), (10.0, false)]), Some(10.0));
    // Three of four runs end censored at the budget.
    let budget = (600.0, false);
    assert_eq!(
        median_detection(&[(5.0, true), budget, budget, budget]),
        None
    );
    assert_eq!(compare::median(&mut []), None);
    assert_eq!(compare::median(&mut [3]), Some(3.0));
    assert_eq!(compare::median(&mut [4, 1]), Some(2.5));
    assert_eq!(compare::median(&mut [5, 1, 3]), Some(3.0));
}

/// Each arm counts its seeds as detected, censored or unscored, times the
/// detected and censored ones, and totals its original and mutant
/// campaigns' applicable runs and unique abstract states.
#[test]
fn each_arm_summarizes_its_seeds() {
    let facts =
        |applied: bool, applicable_runs: u64, features: usize, secs: Option<f64>| CampaignFacts {
            applied,
            applicable_runs,
            features_reached: features,
            worker_secs: 600.0,
            first_intended_secs: secs,
            intended_input: secs.map(|_| "i.bin".to_owned()),
            ..campaign()
        };
    let seed = |seed: u32, arm: Arm, class: Class, mutant: Option<CampaignFacts>| SeedResult {
        seed,
        arm,
        result: classified(class),
        baseline: Some(facts(
            true,
            100,
            10 + usize::try_from(seed).expect("a small seed"),
            None,
        )),
        mutant,
        store: None,
    };
    let seeds = vec![
        seed(
            1,
            Arm::On,
            Class::IntendedViolation,
            Some(facts(true, 5, 3, Some(30.0))),
        ),
        seed(
            1,
            Arm::Off,
            Class::SurvivedBudget,
            Some(facts(true, 50, 1, None)),
        ),
        seed(
            2,
            Arm::On,
            Class::IntendedViolation,
            Some(facts(true, 7, 5, Some(90.0))),
        ),
        seed(
            2,
            Arm::Off,
            Class::NotApplicable,
            Some(facts(false, 0, 1, None)),
        ),
        // A finding no crash timed counts at the campaign's end.
        seed(
            3,
            Arm::On,
            Class::IntendedViolation,
            Some(CampaignFacts {
                intended_input: Some("i.bin".to_owned()),
                ..facts(true, 9, 4, None)
            }),
        ),
        seed(
            3,
            Arm::Off,
            Class::HarnessFailure,
            Some(facts(true, 2, 2, Some(10.0))),
        ),
        // Another violation beside an intended input is censored at the
        // campaign's end, not detected at 20 s.
        seed(
            4,
            Arm::On,
            Class::OtherViolation,
            Some(facts(true, 1, 2, Some(20.0))),
        ),
        seed(4, Arm::Off, Class::BaselineFailure, None),
    ];
    let arms = compare::compare(&seeds);
    assert_eq!(
        arms.iter().map(|arm| arm.arm).collect::<Vec<_>>(),
        [Arm::On, Arm::Off]
    );
    let on = &arms[0];
    assert_eq!(
        (on.seeds, on.detected, on.censored, on.unscored),
        (4, 3, 1, 0)
    );
    // Undetected shares: 3/4 at 30 s, 1/2 at 90 s. Had seed 4 counted at
    // 20 s, the median would be 30 s.
    assert_eq!(on.median_detection_secs, Some(90.0));
    assert_eq!(
        (
            on.mutant.campaigns,
            on.mutant.applicable,
            on.mutant.applicable_runs
        ),
        (4, 4, 22)
    );
    assert_eq!(on.mutant.features_reached, Some(3.5));
    assert_eq!(
        (on.baseline.campaigns, on.baseline.applicable_runs),
        (4, 400)
    );
    assert_eq!(on.baseline.features_reached, Some(12.5));

    let off = &arms[1];
    assert_eq!(
        (off.seeds, off.detected, off.censored, off.unscored),
        (4, 0, 2, 2)
    );
    assert_eq!(off.median_detection_secs, None);
    // The harness failure's campaigns left facts, but the totals count
    // only the timed seeds 1 and 2.
    assert_eq!(
        (
            off.mutant.campaigns,
            off.mutant.applicable,
            off.mutant.applicable_runs
        ),
        (2, 1, 50)
    );
    assert_eq!(off.mutant.features_reached, Some(1.0));
    assert_eq!(
        (off.baseline.campaigns, off.baseline.applicable_runs),
        (2, 200)
    );
    assert_eq!(off.baseline.features_reached, Some(11.5));
    let json = serde_json::to_value(&arms).expect("the comparison serializes");
    assert_eq!(json[1]["arm"], "off");
    assert_eq!(json[1]["median_detection_secs"], Value::Null);
}

/// The plan runs each seed's arms together, one original campaign per
/// target, seed and arm, and one mutant campaign per case, seed and arm.
/// The committed catalogue's discovery runs five seeds in one arm.
#[test]
fn discovery_plans_paired_arms() {
    let planned = lanes(&[11, 23], &[Arm::On, Arm::Off]);
    let pairs: Vec<(u32, Arm)> = planned.iter().map(|lane| (lane.seed, lane.arm)).collect();
    assert_eq!(
        pairs,
        [(11, Arm::On), (11, Arm::Off), (23, Arm::On), (23, Arm::Off)]
    );
    assert_eq!(planned[1].dir(), Path::new("off/seed-11"));

    let catalogue = committed_catalogue();
    let bounds = &catalogue.discovery;
    assert_eq!(bounds.seeds.len(), 5);
    assert_eq!(bounds.feedback_arms, [Arm::On]);
    let cases = select(&catalogue.cases, &[]).expect("every case");
    let (scored, _) = discovery_cases(&cases).expect("some case is scored");
    let planned = lanes(&bounds.seeds, &bounds.feedback_arms);
    let baselines = baseline_lanes(&scored, &planned);
    let targets: BTreeSet<&str> = scored.iter().map(|case| case.target.as_str()).collect();
    assert_eq!(baselines.len(), targets.len() * planned.len());
    for case in &scored {
        for lane in &planned {
            assert!(
                baselines
                    .iter()
                    .any(|(other, at)| other.target == case.target && at == lane),
                "{} has no original campaign on {lane:?}",
                case.id
            );
        }
    }
    // Eight cases on four targets: 60 campaigns of 600 s.
    assert_eq!((scored.len(), targets.len(), planned.len()), (8, 4, 5));

    // An equivalent case needs no original campaign, and doesn't run one
    // for the cases on its target.
    let mut equivalent = scored[0].clone();
    equivalent.equivalent = Some("review".to_owned());
    assert!(baseline_lanes(&[&equivalent], &planned).is_empty());
    let mut sibling = scored[0].clone();
    sibling.id = "sibling".to_owned();
    let baselines = baseline_lanes(&[&equivalent, &sibling], &planned);
    assert_eq!(baselines.len(), planned.len());
    assert!(baselines.iter().all(|(case, _)| case.id == "sibling"));
}

/// The catalogue's and the case list's libFuzzer flags must each name a
/// fuzz target.
#[test]
fn libfuzzer_flags_name_fuzz_targets() {
    let root = crate::repo_root().expect("xtask lives in the workspace");
    let targets = crate::sim_minimize::fuzz_targets(&root).expect("the fuzz package's targets");
    let mut bounds = committed_catalogue().discovery;
    check_libfuzzer_args(&bounds, &targets).expect("the catalogue's flags name targets");
    check_libfuzzer_args(&committed_list().bounds, &targets)
        .expect("the list's flags name targets");
    bounds
        .libfuzzer_args
        .insert("no_such_target".to_owned(), vec!["-len_control=0".to_owned()]);
    let error = check_libfuzzer_args(&bounds, &targets).expect_err("an unknown target fails");
    assert!(error.contains("no_such_target"), "{error}");
}

/// Cases on one target share its original campaigns, and each reads the
/// original's facts for its own property.
#[test]
fn each_case_reads_the_original_campaign_for_its_own_property() {
    let (held, unapplied) = ("p/held", "p/unapplied");
    let intended = "runs/r/workers/0/sim/inputs/active/p/held/aa.bin";
    let campaign = Campaign {
        records: CampaignRecords {
            manifest: serde_json::json!({}),
            initial_inputs: 0,
            end: Some(serde_json::json!({
                "failures": [{"path": intended, "sha256": "aa"}],
                "workers": [{"index": 0, "millis": 1000}],
            })),
            attempts: Vec::new(),
            evidence: serde_json::json!({
                "status": "complete",
                "unapplied": [unapplied],
                "incomplete": [],
                "complete_totals": {"properties": {held: {"held": 4, "violated": 1}}},
            }),
        },
        unpaired: Some("why".to_owned()),
    };
    let facts = campaign.facts(held);
    assert!(facts.applied);
    assert_eq!(facts.applicable_runs, 5);
    assert_eq!(facts.intended_input.as_deref(), Some(intended));
    let facts = campaign.facts(unapplied);
    assert!(!facts.applied);
    assert_eq!(facts.applicable_runs, 0);
    assert_eq!(facts.intended_input, None);
    // The pairing verdict is the campaign's, whatever the property.
    assert_eq!(facts.unpaired.as_deref(), Some("why"));
}

/// One seed's arms pass `campaign start` the same arguments but the
/// store, the name and the feedback mode, which is `on` only in the on
/// arm. The fuzz target's collector runs in both arms; the mode sets
/// whether libFuzzer reads its features. A run whose fuzzer logged another
/// mode, or none, doesn't pair.
#[test]
fn each_arm_runs_with_its_own_feedback_mode() {
    let list = committed_list();
    let case = &list.cases[0];
    let corpus = Path::new("qa/mutation-pilot/discovery-empty");
    let args = |arm: Arm| {
        start_args(
            &list.bounds,
            &format!("c-{arm}-11"),
            case,
            "bb",
            Lane { seed: 11, arm },
            corpus,
            &Path::new("s").join(arm.mode()),
        )
    };
    let (on, off) = (args(Arm::On), args(Arm::Off));
    assert_eq!(on.len(), off.len());
    let flags: Vec<&OsString> = (0..on.len())
        .filter(|i| on[*i] != off[*i])
        .map(|i| &on[i - 1])
        .collect();
    assert_eq!(flags, ["--store", "--campaign", "--feedback"]);
    let feedback = |args: &[OsString]| {
        let at = args
            .iter()
            .position(|arg| arg == "--feedback")
            .expect("the mode is passed");
        args[at + 1].clone()
    };
    assert_eq!(feedback(&on), "on");
    assert_eq!(feedback(&off), "off");
    // The case's target gets the list's extra libFuzzer flags.
    assert_eq!(case.target, "header_sync_storage");
    let at = on
        .iter()
        .position(|arg| arg == "--libfuzzer-arg")
        .expect("the target's flag is passed");
    assert_eq!(on[at + 1], "-len_control=0");

    let manifest = |mode: &str| serde_json::json!({"feedback": {"mode": mode}});
    assert_eq!(arm_mismatch(&manifest("on"), Arm::On, true), None);
    assert_eq!(arm_mismatch(&manifest("off"), Arm::Off, true), None);
    assert!(arm_mismatch(&manifest("on"), Arm::Off, true)
        .expect("the modes differ")
        .contains("not the arm's off"));
    assert!(arm_mismatch(&serde_json::json!({}), Arm::On, true)
        .expect("no feedback was logged")
        .contains("no semantic feedback"));
    // A lone arm, as the pilot's discovery runs, pairs without a logged
    // mode, since no other arm compares with it.
    assert_eq!(arm_mismatch(&serde_json::json!({}), Arm::On, false), None);
    assert_eq!(Arm::parse("off"), Ok(Arm::Off));
    assert!(Arm::parse("both").is_err());
    let flags = parse_flags(
        ["--out", "/o", "--arm", "off"]
            .into_iter()
            .map(str::to_owned),
    )
    .expect("the flags parse");
    assert_eq!(flags.arms, [Arm::Off]);
    assert!(parse_flags(["--out", "/o", "--arm", "x"].into_iter().map(str::to_owned)).is_err());
}

/// `campaign_lane` passes its arm's mode to `campaign start`, rejects a run
/// whose fuzzer logged another mode, and times the intended finding from
/// the worker's attempts.
#[cfg(unix)]
#[test]
fn a_campaign_lane_reads_its_arm_and_attempts() {
    use std::os::unix::fs::PermissionsExt as _;

    // `campaign_lane` runs its children against the pilot's interrupt flag.
    let _flag = crate::sim_minimize::tests::INTERRUPT_FLAG_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let dir = tempfile::tempdir().expect("a temporary directory");
    let catalogue = committed_catalogue();
    let case = &catalogue.cases[0];
    assert_eq!(case.property, ID);
    // The records a run that found the intended property after 1.5 s
    // leaves; the manifest names the mode the fuzzer logged.
    let records = |mode: &str| {
        format!(
            "mkdir -p \"$store/manifests\" \"$store/snapshots/s\" \"$store/runs/r/workers/0\"\n\
             echo \"{{\\\"initial_snapshot\\\":\\\"s\\\",\\\"feedback\\\":{{\\\"mode\\\":\\\"{mode}\\\"}}}}\" \
             > \"$store/manifests/r.json\"\n\
             echo '{{\"inputs\":[]}}' > \"$store/snapshots/s/snapshot.json\"\n\
             echo '{{\"workers\":[{{\"index\":0,\"millis\":4000}}],\"failures\":[{{\"path\":\
             \"runs/r/workers/0/sim/inputs/active/block/reactor-policy/aa.bin\",\"sha256\":\"aa\"}}]}}' \
             > \"$store/runs/r/end.json\"\n\
             echo '{{\"millis\":1500,\"artifacts\":[{{\"path\":\"runs/r/workers/0/artifacts/crash-1\",\
             \"sha256\":\"aa\"}}]}}' > \"$store/runs/r/workers/0/attempts.jsonl\"\n\
             exit 1"
        )
    };
    let evidence = "echo '{\"status\":\"worker-stopped\",\"totals\":{\"runs\":9,\
                    \"features\":{\"7\":2}},\"unapplied\":[],\"incomplete\":[]}' > \"$8\"";
    // `$mode` is the value after `--feedback`.
    let passed = "mode=none; prev=\n\
                  for arg in \"$@\"; do [ \"$prev\" = --feedback ] && mode=$arg; prev=$arg; done\n";
    for (name, start, arm, mismatch) in [
        (
            "passed-on",
            format!("{passed}{}", records("$mode")),
            Arm::On,
            false,
        ),
        (
            "passed-off",
            format!("{passed}{}", records("$mode")),
            Arm::Off,
            false,
        ),
        ("always-on", records("on"), Arm::Off, true),
    ] {
        let sim = dir.path().join(name);
        fs::write(
            &sim,
            format!(
                "#!/bin/sh\n\
                 store=$4\n\
                 case \"$1 $2\" in\n\
                 'campaign build') mkdir -p \"$store/builds/b\" ;;\n\
                 'campaign start') {start} ;;\n\
                 'campaign evidence') {evidence} ;;\n\
                 esac\n"
            ),
        )
        .expect("the script is written");
        fs::set_permissions(&sim, fs::Permissions::from_mode(0o755))
            .expect("the script is executable");
        wait_until_executable(&sim);
        let campaign = campaign_lane(
            &sim,
            &dir.path().join(format!("{name}-lane")),
            &dir.path().join("fuzz-target"),
            case,
            Lane { seed: 11, arm },
            &catalogue.discovery,
            &Pairing::of(&Pin::Catalogue(&catalogue), dir.path()),
            dir.path(),
            true,
        )
        .expect("the lane runs")
        .unwrap_or_else(|error| panic!("{name}: the lane left no facts: {error}"));
        let facts = campaign.facts(&case.property);
        // The stand-in's manifest records no build identity, so the build
        // never pairs; only the feedback mode differs between the lanes.
        let unpaired = facts.unpaired.expect("the stand-in names no build");
        assert_eq!(
            unpaired.contains("feedback"),
            mismatch,
            "{name}: {unpaired}"
        );
        assert_eq!(facts.first_intended_secs, Some(1.5), "{name}");
        assert!(facts.intended_input.is_some(), "{name}");
        assert_eq!(facts.features_reached, 1, "{name}");
        assert_eq!(facts.worker_secs, 4.0, "{name}");
    }
}

/// The committed feedback comparison's case list, checked.
fn committed_list() -> feedback::List {
    let root = crate::repo_root().expect("xtask lives in the workspace");
    feedback::read(&root).expect("the committed list checks").1
}

/// The committed case list resolves each case to its planted patch, input
/// and production files, and freezes ten seeds in both arms: twelve cases
/// on five targets, 340 campaigns of 600 s.
#[test]
fn the_feedback_list_plans_paired_arms() {
    let list = committed_list();
    assert_eq!(list.bounds.seeds.len(), 10);
    assert_eq!(list.bounds.feedback_arms, [Arm::On, Arm::Off]);
    for case in &list.cases {
        assert_eq!(
            Path::new(&case.id).parent(),
            Some(Path::new(&case.property)),
            "{}: its property is its directory",
            case.id
        );
        assert!(!case.production_files.is_empty(), "{}", case.id);
        assert!(
            case.production_files
                .keys()
                .all(|path| path.starts_with("crates/")),
            "{}: {:?}",
            case.id,
            case.production_files
        );
    }
    let cases: Vec<&Case> = list.cases.iter().collect();
    let planned = lanes(&list.bounds.seeds, &list.bounds.feedback_arms);
    let targets: BTreeSet<&str> = cases.iter().map(|case| case.target.as_str()).collect();
    assert!(!targets.contains("block_sync_storage"));
    let originals = baseline_lanes(&cases, &planned).len();
    assert_eq!(originals, targets.len() * planned.len());
    assert_eq!(
        (
            cases.len(),
            targets.len(),
            originals + cases.len() * planned.len()
        ),
        (12, 5, 340)
    );
}

/// A synthetic repository for [`feedback::resolve`]: one planted input case,
/// `p/q/name`, whose input runs `h/one` and whose patch changes
/// `crates/a.rs`, and the fuzz targets `t_one` (`h/one`), `t_two` (`h/two`)
/// and `t_none`, which runs no harness.
fn planted_repo() -> (
    tempfile::TempDir,
    crate::planted::Case,
    BTreeMap<String, Option<String>>,
) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let root = dir.path();
    let write = |path: &str, text: &str| {
        let path = root.join(path);
        fs::create_dir_all(path.parent().expect("a file has a parent")).expect("the directory");
        fs::write(path, text).expect("the file is written");
    };
    write("crates/a.rs", "fn a() {}\n");
    write("in/x.bin", "x");
    write(
        "in/x.toml",
        &format!(
            "harness = \"h/one\"\nsha256 = \"{}\"\n",
            hex(&Sha256::digest(b"x"))
        ),
    );
    write(
        "qa/planted/p/q/name.patch",
        "--- a/crates/a.rs\n+++ b/crates/a.rs\n@@ -1 +1 @@\n-fn a() {}\n+fn a() { todo!() }\n",
    );
    let case = crate::planted::Case {
        property: "p/q".to_owned(),
        patch: root.join("qa/planted/p/q/name.patch"),
        witness: crate::planted::Witness::Input(PathBuf::from("in/x.bin")),
        checks: Vec::new(),
        also_violates: vec!["p/also".to_owned()],
        set: Vec::new(),
        target: None,
    };
    let targets = BTreeMap::from([
        ("t_one".to_owned(), Some("h/one".to_owned())),
        ("t_two".to_owned(), Some("h/two".to_owned())),
        ("t_none".to_owned(), None),
    ]);
    (dir, case, targets)
}

/// A listed case of `id` on `target`, violating `property`.
fn entry(id: &str, property: &str, target: &str) -> feedback::Entry {
    feedback::Entry {
        case: id.to_owned(),
        property: property.to_owned(),
        target: target.to_owned(),
        fault_class: "stale-result".to_owned(),
    }
}

/// A listed case resolves to its planted patch, input, harness, allowed
/// properties and patched production files, hashed in the tree.
#[test]
fn a_listed_case_resolves_to_its_planted_case() {
    let (dir, planted, targets) = planted_repo();
    let root = dir.path();
    let cases = feedback::resolve(
        root,
        &[entry("p/q/name", "p/q", "t_one")],
        &[planted],
        &targets,
    )
    .expect("the case resolves");
    let [case] = cases.as_slice() else {
        panic!("one case: {cases:?}");
    };
    assert_eq!(case.id, "p/q/name");
    assert_eq!(case.patch, Path::new("qa/planted/p/q/name.patch"));
    assert_eq!(case.harness, "h/one");
    assert_eq!(case.witness, Path::new("in/x.bin"));
    assert_eq!(case.also_violates, ["p/also"]);
    let file = |path: &str| file_sha256(&root.join(path)).expect("the file is hashed");
    assert_eq!(
        case.production_files,
        BTreeMap::from([("crates/a.rs".to_owned(), file("crates/a.rs"))])
    );
    assert_eq!(case.patch_sha256, file("qa/planted/p/q/name.patch"));
    assert_eq!(case.witness_sha256, file("in/x.bin"));
    // The runner's pin check in a checkout with the same files passes.
    check_pins(root, "HEAD", &[case]).expect("the tree holds the hashed files");
}

/// The list rejects a case with no planted patch, an unknown or
/// harness-less target, a property that isn't the case's, a target that
/// runs another harness, a case listed twice, a typed or nested case, and
/// an empty list.
#[test]
fn the_feedback_list_rejects_bad_cases() {
    let (dir, planted, targets) = planted_repo();
    let root = dir.path();
    let typed = crate::planted::Case {
        witness: crate::planted::Witness::Typed {
            harness: "h/one".to_owned(),
            scenario: PathBuf::from("s.json"),
            plan: PathBuf::from("s.plan"),
        },
        ..planted.clone()
    };
    let nested = crate::planted::Case {
        also_violates: Vec::new(),
        set: vec!["p/broader".to_owned()],
        ..planted.clone()
    };
    let good = entry("p/q/name", "p/q", "t_one");
    for (entries, planted, why) in [
        (
            vec![entry("p/q/missing", "p/q", "t_one")],
            &planted,
            "no planted case has the patch qa/planted/p/q/missing.patch",
        ),
        (
            vec![entry("p/q/name", "p/q", "nope")],
            &planted,
            "nope isn't a simulator fuzz target",
        ),
        (
            vec![entry("p/q/name", "p/q", "t_none")],
            &planted,
            "t_none runs no registered harness",
        ),
        (
            vec![entry("p/q/name", "p/other", "t_one")],
            &planted,
            "lists property p/other, but the planted case violates p/q",
        ),
        (
            vec![entry("p/q/name", "p/q", "t_two")],
            &planted,
            "t_two runs h/two, but the case's input runs h/one",
        ),
        (vec![good.clone(), good.clone()], &planted, "listed twice"),
        (vec![good.clone()], &typed, "a typed case has no fuzz input"),
        (vec![good.clone()], &nested, "nested case"),
        (Vec::new(), &planted, "the list names no case"),
    ] {
        let error = feedback::resolve(root, &entries, std::slice::from_ref(planted), &targets)
            .expect_err(why)
            .to_string();
        assert!(error.contains(why), "{why}: {error}");
    }
}

/// A mutant campaign may violate a property its case lists in
/// `also_violates` beside the intended one: a confirmed finding of both is
/// still detection. The listed property alone misses the patch, and an
/// unlisted one beside the intended finding is another violation, as the
/// planted runner judges.
#[test]
fn discovery_allows_listed_violations_beside_the_intended_one() {
    const ALSO: &str = "block/also";
    let also = [ALSO.to_owned()];
    let both = CampaignFacts {
        violated: vec![ID.to_owned(), ALSO.to_owned()],
        ..found(ID)
    };
    let judged = |facts: &CampaignFacts, also_violates: &[String]| {
        classify::discovery(
            ID,
            also_violates,
            &discovery_run(facts.clone(), Some(Ok(()))),
        )
    };
    assert_eq!(judged(&both, &also).class, Class::IntendedViolation);
    assert_eq!(judged(&both, &[]).class, Class::OtherViolation);
    let alone = judged(&found(ALSO), &also);
    assert_eq!(alone.class, Class::OtherViolation);
    assert!(
        alone.reason.contains("allows only beside"),
        "{}",
        alone.reason
    );
    let unlisted = CampaignFacts {
        violated: vec![ID.to_owned(), OTHER.to_owned()],
        ..found(ID)
    };
    assert_eq!(judged(&unlisted, &also).class, Class::OtherViolation);
}

/// A fresh confirmation lets the input violate the case's `also_violates`
/// beside its property, as `cargo xtask planted` confirms a case, and a
/// case without them passes no such flag.
#[cfg(unix)]
#[test]
fn confirmation_allows_the_cases_listed_violations() {
    use std::os::unix::fs::PermissionsExt as _;

    // `confirm_and_replay` runs its children against the pilot's interrupt
    // flag.
    let _flag = crate::sim_minimize::tests::INTERRUPT_FLAG_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let dir = tempfile::tempdir().expect("a temporary directory");
    let args = dir.path().join("args");
    // A stand-in for `zakura-sim` whose `confirm` records its arguments and
    // confirms, and whose `replay` matches.
    let sim = dir.path().join("sim");
    fs::write(
        &sim,
        format!(
            "#!/bin/sh\n\
             case \"$1\" in\n\
             confirm) echo \"$@\" > '{}'; mkdir -p \"$9\"; \
             echo '{{\"status\":\"confirmed\",\"trace\":\"t\"}}' > \"$9/confirmation.json\" ;;\n\
             esac\n",
            args.display()
        ),
    )
    .expect("the script is written");
    fs::set_permissions(&sim, fs::Permissions::from_mode(0o755)).expect("the script is executable");
    wait_until_executable(&sim);
    let mut case = committed_catalogue().cases[0].clone();
    for (also_violates, flag) in [
        (
            vec!["p/a".to_owned(), "p/b".to_owned()],
            Some("--also-violates p/a,p/b"),
        ),
        (Vec::new(), None),
    ] {
        case.also_violates = also_violates;
        let out = dir.path().join(format!("out-{}", case.also_violates.len()));
        let confirmed = confirm_and_replay(
            &sim,
            &case,
            Path::new("in.bin"),
            &out,
            Duration::from_secs(60),
        )
        .expect("the children run");
        assert_eq!(confirmed, Ok(()));
        let passed = fs::read_to_string(&args).expect("confirm ran");
        assert!(
            passed.contains(&format!("--property {}", case.property)),
            "{passed}"
        );
        match flag {
            Some(flag) => assert!(passed.contains(flag), "{passed}"),
            None => assert!(!passed.contains("--also-violates"), "{passed}"),
        }
    }
}
