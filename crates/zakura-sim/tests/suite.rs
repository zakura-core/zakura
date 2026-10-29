//! The full-suite manifest (campaign.md, C1): the committed `qa/suite.toml`
//! covers every harness, property, fuzz target, ignored test, planted case
//! and seed, and each gap it can have fails the check with its reason.
#![cfg(zakura_sim)]

use std::{collections::BTreeSet, process::Command};

use serde_json::Value;
use tempfile::TempDir;
use zakura_sim::{
    harness::{coverage::COVERAGE, HARNESSES},
    seed,
    suite::{self, Expect, MissingTarget, Pending, PendingKind, SuiteFile, NIGHTLY, SUITE_FILE},
};

/// The committed `qa/suite.toml`.
fn committed() -> SuiteFile {
    let text = std::fs::read_to_string(seed::repo_root().join(SUITE_FILE)).unwrap();
    toml::from_str(&text).unwrap()
}

/// The problems the manifest of this checkout has with `suite` as its
/// `qa/suite.toml`.
fn problems(suite: &SuiteFile) -> Vec<String> {
    match suite::manifest_of(&seed::repo_root(), suite, String::new()) {
        Ok(_) => Vec::new(),
        Err(problems) => problems.0,
    }
}

/// Removes the one entry of `entries` that `matches`.
fn remove<T>(entries: &mut Vec<T>, matches: impl Fn(&T) -> bool) {
    let before = entries.len();
    entries.retain(|entry| !matches(entry));
    assert_eq!(entries.len(), before - 1);
}

/// A pending row of `kind` for `id`.
fn pending(id: &str, kind: PendingKind) -> Pending {
    Pending {
        id: id.to_owned(),
        kind,
        owner: "none".to_owned(),
        plan: "none".to_owned(),
    }
}

/// Asserts that one of `problems` contains `text`.
fn assert_problem(problems: &[String], text: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(text)),
        "no problem says `{text}`: {problems:#?}"
    );
}

#[test]
fn the_committed_suite_covers_the_registry() {
    let manifest = suite::manifest(&seed::repo_root()).unwrap_or_else(|problems| {
        panic!("{problems}");
    });
    assert_eq!(manifest.harnesses.len(), HARNESSES.len());
    let pairs: usize = HARNESSES
        .iter()
        .map(|entry| (entry.properties)().len())
        .sum();
    assert_eq!(manifest.properties.len(), pairs);
    assert_eq!(manifest.witnesses.len(), 2 * COVERAGE.len());
    // Each harness has a seed set, and each one a target or a missing-target
    // row.
    for row in &manifest.harnesses {
        assert!(manifest.seed_sets.iter().any(|set| set.id == row.seed_set));
        assert_ne!(row.targets.is_empty(), row.missing_target.is_none());
    }
    // Each row a command decides names one that exists.
    let mut ids = BTreeSet::new();
    ids.extend(manifest.tests.iter().map(|row| row.id.clone()));
    ids.extend(manifest.planted.iter().map(|row| row.id.clone()));
    for row in &manifest.witnesses {
        match row.expect {
            Expect::Pass => assert!(
                row.decided_by.as_ref().is_some_and(|id| ids.contains(id)),
                "{row:?}"
            ),
            Expect::Unimplemented => assert!(row.pending.is_some(), "{row:?}"),
            other => panic!("{}: {other:?}", row.id),
        }
    }
    // Every executable row has a timeout.
    for (id, timeout) in manifest
        .tests
        .iter()
        .map(|row| (&row.id, row.timeout_secs))
        .chain(
            manifest
                .planted
                .iter()
                .map(|row| (&row.id, row.timeout_secs)),
        )
        .chain(
            manifest
                .seed_sets
                .iter()
                .map(|row| (&row.id, row.timeout_secs)),
        )
        .chain(
            manifest
                .seed_witnesses
                .iter()
                .map(|row| (&row.id, row.timeout_secs)),
        )
        .chain(
            manifest
                .targets
                .iter()
                .map(|row| (&row.id, row.timeout_secs)),
        )
    {
        assert!(timeout > 0, "{id}");
    }
}

#[test]
fn a_harness_without_a_target_fails() {
    let mut suite = committed();
    remove(&mut suite.missing_target, |row| {
        row.harness == "header-sync/wire"
    });
    assert_problem(
        &problems(&suite),
        "no target runs the harness `header-sync/wire`",
    );
}

#[test]
fn a_harness_both_run_and_missing_fails() {
    let mut suite = committed();
    suite.missing_target.push(MissingTarget {
        harness: "discovery/wait-complete".to_owned(),
        owner: "none".to_owned(),
        reason: "none".to_owned(),
    });
    assert_problem(&problems(&suite), "still lists it as a missing target");
}

#[test]
fn a_target_of_an_unregistered_harness_fails() {
    let mut suite = committed();
    for target in &mut suite.target {
        if target.name == "discovery_wait_complete" {
            target.harness = Some("no/such-harness".to_owned());
        }
    }
    let problems = problems(&suite);
    assert_problem(&problems, "names the harness `no/such-harness`");
    assert_problem(
        &problems,
        "no target runs the harness `discovery/wait-complete`",
    );
}

#[test]
fn an_unmapped_fuzz_target_fails() {
    let mut suite = committed();
    remove(&mut suite.excluded_target, |row| {
        row.name == "json_rpc_request"
    });
    remove(&mut suite.target, |row| row.name == "header_pursuit");
    let problems = problems(&suite);
    assert_problem(
        &problems,
        "qa/fuzz/rpc defines the target `json_rpc_request`, and qa/suite.toml neither maps nor \
         excludes it",
    );
    assert_problem(
        &problems,
        "qa/fuzz/header-chain defines the target `header_pursuit`",
    );
}

#[test]
fn an_ignored_test_the_suite_doesnt_list_fails() {
    let mut suite = committed();
    remove(&mut suite.ignored, |row| {
        row.test == "regenerate_saved_plans"
    });
    assert_problem(
        &problems(&suite),
        "crates/zakura-sim/tests/header_wire_lifecycle.rs::regenerate_saved_plans is ignored",
    );
}

#[test]
fn a_listed_test_that_isnt_ignored_fails() {
    let mut suite = committed();
    let mut row = suite.ignored[0].clone();
    row.file = "crates/zakura-sim/tests/harness.rs".to_owned();
    row.test = "a_clean_run_holds_no_task_panic".to_owned();
    suite.ignored.push(row);
    assert_problem(&problems(&suite), "isn't an ignored test");
}

#[test]
fn an_on_demand_witness_the_suite_doesnt_run_fails() {
    let mut suite = committed();
    for row in &mut suite.ignored {
        if row.test == "the_decoded_input_stall_input_holds_input_decoded_bound" {
            row.run = false;
            row.timeout_secs = None;
            row.reason = Some("none".to_owned());
        }
    }
    assert_problem(
        &problems(&suite),
        "the_decoded_input_stall_input_holds_input_decoded_bound runs in no row",
    );
}

#[test]
fn a_simulator_test_binary_the_suite_doesnt_list_fails() {
    let mut suite = committed();
    for package in &mut suite.sim_package {
        if package.name == "zakura-network" {
            package.tests = Some(vec!["sim_header_sync".to_owned()]);
        }
    }
    assert_problem(
        &problems(&suite),
        "crates/zakura-network/tests/sim_block_sync.rs builds only under",
    );
}

#[test]
fn a_registered_pending_row_fails() {
    let mut suite = committed();
    suite.pending.push(pending(
        "block-sync/stale-completion-inert",
        PendingKind::Property,
    ));
    suite
        .pending
        .push(pending("header-sync/wire", PendingKind::Harness));
    let problems = problems(&suite);
    assert_problem(
        &problems,
        "lists `block-sync/stale-completion-inert` as pending, and it's registered",
    );
    assert_problem(
        &problems,
        "lists `header-sync/wire` as pending, and it's registered",
    );
}

#[test]
fn every_problem_is_reported_at_once() {
    let mut suite = committed();
    remove(&mut suite.missing_target, |row| {
        row.harness == "block-sync/storage"
    });
    remove(&mut suite.ignored, |row| {
        row.test == "regenerate_wait_complete_input"
    });
    remove(&mut suite.target, |row| row.name == "race_fixtures");
    let problems = problems(&suite);
    assert_problem(&problems, "no target runs the harness `block-sync/storage`");
    assert_problem(&problems, "regenerate_wait_complete_input is ignored");
    assert_problem(&problems, "defines the target `race_fixtures`");
    assert_problem(&problems, "no target runs the harness `race/ab-ba`");
}

#[test]
fn the_nightly_pin_matches_the_workflows() {
    let workflows = seed::repo_root().join(".github/workflows");
    for file in ["header-fuzz.yml", "tests-unit.yml"] {
        let text = std::fs::read_to_string(workflows.join(file)).unwrap();
        assert!(text.contains(NIGHTLY), "{file} doesn't pin {NIGHTLY}");
    }
}

#[test]
fn suite_writes_the_manifest() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("manifest.json");
    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(["suite", "--out", out.to_str().unwrap()])
        .output()
        .expect("the binary runs");
    assert!(output.status.success(), "{output:?}");
    let manifest: Value = serde_json::from_slice(&std::fs::read(&out).unwrap()).unwrap();
    assert_eq!(manifest["schema"], suite::SCHEMA);
    assert_eq!(
        manifest["harnesses"].as_array().map(Vec::len),
        Some(HARNESSES.len())
    );
    // The embedded seed report is `seeds --report`'s.
    let report = dir.path().join("seeds.json");
    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(["seeds", "--report", report.to_str().unwrap()])
        .current_dir(seed::repo_root())
        .output()
        .expect("the binary runs");
    assert!(output.status.success(), "{output:?}");
    let report: Value = serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
    assert_eq!(manifest["seed_report"]["seeds"], report["seeds"]);
}
