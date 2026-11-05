//! The pilot runner's classification and report tests, on synthetic
//! observations. None builds or runs a simulator.

use super::*;
use crate::planted::{end_of, Run};

const ID: &str = "block/reactor-policy";
const OTHER: &str = "sim/no-task-panic";
const HELD: &str = r#"{"held":{"applicable":4}}"#;
const VIOLATED: &str = r#"{"violated":{"witness":[3]}}"#;

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
            other_violated,
            outcome,
            harness,
            ..
        } => Run::Ended {
            violation,
            other_violated,
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
            run(None, &[(ID, r#""not-applicable""#)]),
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

/// A run that reports nothing for the intended ID, ends inconclusive,
/// leaks state, crashes, or doesn't pair with the original's build has
/// incomplete observations: a harness failure.
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
                &[(ID, r#"{"inconclusive":{"reason":"harness-failed"}}"#)],
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

/// A held mutant run survives, a never-applying one is not applicable, and
/// only review metadata makes a case equivalent.
#[test]
fn sensitivity_names_survivors_and_equivalents() {
    assert_eq!(
        class(&sensitivity_run(run(None, &[(ID, HELD)]), None)),
        Class::SurvivedBudget
    );
    assert_eq!(
        class(&sensitivity_run(
            run(None, &[(ID, r#""not-applicable""#)]),
            None
        )),
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
        baseline: Some(Ok(campaign())),
        patch: Some(Ok(())),
        mutant: Some(Ok(mutant)),
        confirmation,
    }
}

fn discovered(run: &DiscoveryRun) -> Class {
    classify::discovery(ID, run).class
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
        Err("the original simulator fails to build".to_owned()),
        Ok(found(OTHER)),
        Ok(CampaignFacts {
            failures: vec!["runs/x/workers/0/artifacts/timeout-1".to_owned()],
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
    failed.mutant = Some(Err("`zakura-sim campaign start` ran past 900 s".to_owned()));
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
/// violation explains, and other failures; traces go with their inputs.
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
    let facts = campaign_facts(ID, Some(&end(&[&intended, &crash, &trace])), &evidence, 0);
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
    );
    assert!(facts.violated.is_empty());
    assert_eq!(facts.failures, [timeout, nondeterministic, crash]);

    let unapplied = serde_json::json!({"status": "complete", "unapplied": [ID], "incomplete": []});
    assert!(!campaign_facts(ID, None, &unapplied, 0).applied);
    assert_eq!(
        campaign_facts(ID, None, &serde_json::json!({}), 0).status,
        "none"
    );
}

/// Paired campaigns get the catalogue's bounds and seed: their arguments
/// differ only in the store, the name and the build.
#[test]
fn paired_campaigns_share_their_budget() {
    let catalogue = committed_catalogue();
    let bounds = &catalogue.discovery;
    let corpus = Path::new("qa/mutation-pilot/discovery-empty");
    let baseline = start_args(
        bounds,
        "t-11",
        "h",
        "aa",
        11,
        corpus,
        Path::new("s/baseline"),
    );
    let mutant = start_args(bounds, "c-11", "h", "bb", 11, corpus, Path::new("s/mutant"));
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
    let why = prepare_patch(root, root, &case)
        .expect("git runs")
        .expect_err("the context isn't in a.rs");
    assert!(why.starts_with("the patch doesn't apply"), "{why}");

    patch("--- a/b.rs\n+++ b/b.rs\n@@ -1 +1 @@\n-fn b() {}\n+fn c() {}\n");
    let why = prepare_patch(root, root, &case)
        .expect("git runs")
        .expect_err("b.rs isn't declared");
    assert!(why.contains("undeclared files [b.rs"), "{why}");
    reset();

    patch("--- a/a.rs\n+++ b/a.rs\n@@ -1 +1 @@\n-fn a() {}\n+fn c() {}\n");
    // The patch file itself is untracked, so declare it for this check.
    case.production_files
        .insert("p.patch".to_owned(), String::new());
    assert_eq!(prepare_patch(root, root, &case).expect("git runs"), Ok(()));
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
    assert_eq!(select_seeds(&[11, 23], &[]), Ok(vec![11, 23]));
    assert_eq!(select_seeds(&[11, 23], &[23]), Ok(vec![23]));
    assert!(select_seeds(&[11, 23], &[5]).is_err());
    assert!(select_seeds(&[], &[]).is_err());
    assert!(classify::summarize(std::iter::empty()).is_err());
    assert!(sensitivity_report(meta(), Vec::new()).is_err());
    let unseeded = DiscoveryCase {
        id: "a".to_owned(),
        fault_class: "f".to_owned(),
        property: ID.to_owned(),
        target: "t".to_owned(),
        seeds: Vec::new(),
        cleanup: None,
    };
    assert!(discovery_report(meta(), vec![unseeded]).is_err());
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

    // Discovery counts each seed of each case.
    let seed = |seed: u32, class: Class| SeedResult {
        seed,
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
        cleanup: None,
    };
    let report = discovery_report(meta(), vec![case]).expect("the report has results");
    assert_eq!(
        (
            report.summary.results,
            report.summary.denominator,
            report.summary.intended
        ),
        (3, 2, 1)
    );
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

/// Every committed case's patch and production pins exist, and its
/// toolchain parses from the catalogue.
#[test]
fn the_committed_catalogue_pins_its_cases() {
    let catalogue = committed_catalogue();
    assert_eq!(toolchain(&catalogue.baseline_build), Ok("1.99.0"));
    let root = crate::repo_root().expect("xtask lives in the workspace");
    for case in &catalogue.cases {
        assert_eq!(
            file_sha256(&root.join(&case.patch)).expect("the patch exists"),
            case.patch_sha256,
            "{}",
            case.id
        );
        assert!(!case.production_files.is_empty(), "{}", case.id);
        assert!(
            case.equivalent.is_none(),
            "{}: no review marked it equivalent",
            case.id
        );
    }
}
