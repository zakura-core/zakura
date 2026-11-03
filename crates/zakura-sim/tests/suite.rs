//! The full-suite manifest (campaign.md, C1): the committed `qa/suite.toml`
//! covers every harness, property, fuzz target, ignored test, planted case
//! and seed, and each gap it can have fails the check with its reason.
#![cfg(zakura_sim)]

use std::{collections::BTreeSet, fs, path::Path, process::Command};

use serde_json::Value;
use tempfile::TempDir;
use zakura_sim::{
    harness::{coverage::COVERAGE, HARNESSES},
    seed,
    suite::{
        self, ExcludedTarget, Expect, Ignored, MissingTarget, Pending, PendingKind, SimPackage,
        SuiteFile, Target, NIGHTLY, SUITE_FILE,
    },
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

/// A checkout in a temporary directory, with a workspace and a simulator
/// fuzz package:
///
/// - the package `demo` has a library with one unit test, and the test
///   targets `tests/flat.rs` and
///   `tests/nested/main.rs`. `nested` builds only in the simulator, and has
///   an ignored test at its top level and one in its child module
///   `helpers`;
/// - `qa/fuzz/sim` maps its target `mapped` to an unregistered harness, its
///   target `unmapped` to none, and `stale`, which it doesn't define, to
///   `fixtures/all`.
fn scratch() -> TempDir {
    let dir = TempDir::new().unwrap();
    for (file, text) in [
        (
            "Cargo.toml",
            "[workspace]\nmembers = [\"crates/demo\"]\nresolver = \"2\"\n",
        ),
        (
            "crates/demo/Cargo.toml",
            "[package]\nname = \"demo\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
        ),
        (
            "crates/demo/src/lib.rs",
            "#[cfg(test)]\nmod tests {\n    #[test]\n    fn unit() {}\n}\n",
        ),
        ("crates/demo/tests/flat.rs", "#[test]\nfn flat() {}\n"),
        (
            "crates/demo/tests/nested/main.rs",
            "#![cfg(zakura_sim)]\nmod helpers;\n\n#[test]\n#[ignore = \"slow\"]\nfn top() {}\n",
        ),
        (
            "crates/demo/tests/nested/helpers.rs",
            "#[test]\n#[ignore = \"slow\"]\nfn child() {}\n",
        ),
        (
            "qa/fuzz/sim/Cargo.toml",
            "[package]\nname = \"demo-fuzz\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n\
             [package.metadata.zakura-sim.targets]\nmapped = \"no/such-harness\"\n\
             stale = \"fixtures/all\"\n\n\
             [[bin]]\nname = \"mapped\"\npath = \"fuzz_targets/mapped.rs\"\n\n\
             [[bin]]\nname = \"unmapped\"\npath = \"fuzz_targets/unmapped.rs\"\n\n\
             [workspace]\n",
        ),
        ("qa/fuzz/sim/fuzz_targets/mapped.rs", "fn main() {}\n"),
        ("qa/fuzz/sim/fuzz_targets/unmapped.rs", "fn main() {}\n"),
    ] {
        let path = dir.path().join(file);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }
    dir
}

/// A suite for [`scratch`] that runs `demo`'s test targets `tests`, all of
/// them if `None`, and lists `ignored`.
fn scratch_suite(tests: Option<&[&str]>, ignored: &[&str]) -> SuiteFile {
    SuiteFile {
        schema: suite::SCHEMA,
        planted_timeout_secs: 1,
        seed_timeout_secs: 1,
        sim_package: vec![SimPackage {
            name: "demo".to_owned(),
            lib: true,
            tests: tests.map(|tests| tests.iter().map(|test| (*test).to_owned()).collect()),
            timeout_secs: 1,
        }],
        property_package: Vec::new(),
        ignored: ignored
            .iter()
            .map(|test| {
                let (file, test) = test.split_once("::").unwrap();
                Ignored {
                    file: file.to_owned(),
                    test: test.to_owned(),
                    run: true,
                    timeout_secs: Some(1),
                    reason: None,
                }
            })
            .collect(),
        lane: Vec::new(),
        target: ["mapped", "unmapped"]
            .into_iter()
            .map(|name| Target {
                package: "sim".to_owned(),
                name: name.to_owned(),
                covers: Vec::new(),
                smoke_secs: 1,
                args: Vec::new(),
                env: Vec::new(),
                timeout_secs: 1,
            })
            .collect(),
        excluded_target: Vec::new(),
        missing_target: Vec::new(),
        pending: Vec::new(),
    }
}

/// The problems the manifest of the checkout at `root` has with `suite`.
fn problems_at(root: &Path, suite: &SuiteFile) -> Vec<String> {
    match suite::manifest_of(root, suite, String::new()) {
        Ok(_) => Vec::new(),
        Err(problems) => problems.0,
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
fn a_target_covering_a_harness_its_harness_doesnt_compose_fails() {
    // `header-sync/smoke` reports no property, so no property check rejects
    // it, and `fixtures/all` checks none of `header-sync/wire`'s.
    let mut suite = committed();
    for harness in ["header-sync/smoke", "header-sync/wire"] {
        remove(&mut suite.missing_target, |row| row.harness == harness);
        for target in &mut suite.target {
            if target.name == "race_fixtures" {
                target.covers.push(harness.to_owned());
            }
        }
    }
    let problems = problems(&suite);
    for harness in ["header-sync/smoke", "header-sync/wire"] {
        assert_problem(
            &problems,
            &format!(
                "the target `race_fixtures` covers the harness `{harness}`, and its harness \
                 `fixtures/all` doesn't compose `{harness}`"
            ),
        );
    }
}

#[test]
fn planted_fixture_properties_expect_a_violation() {
    let manifest = suite::manifest(&seed::repo_root()).unwrap_or_else(|problems| {
        panic!("{problems}");
    });
    let expect = |id: &str| {
        manifest
            .properties
            .iter()
            .find(|row| row.id == id)
            .unwrap_or_else(|| panic!("no row {id}"))
            .expect
    };
    // The race harnesses and `fixtures/all` plant each race, and
    // `fixtures/fixed` only fixes it.
    for harness in ["race/lost-update", "fixtures/all"] {
        assert_eq!(
            expect(&format!("property/{harness}/race/lost-update")),
            Expect::Violated,
            "{harness}"
        );
    }
    assert_eq!(
        expect("property/fixtures/fixed/race/lost-update"),
        Expect::Holds
    );
    assert_eq!(
        expect("property/discovery/wait-complete/discovery/wait-complete-wakes"),
        Expect::Holds
    );
}

#[test]
fn an_ignore_the_scan_cant_classify_fails() {
    let dir = scratch();
    fs::write(
        dir.path().join("crates/demo/tests/flat.rs"),
        "#[test]\n#[cfg_attr(zakura_sim, ignore)]\nfn flat() {}\n",
    )
    .unwrap();
    assert_problem(
        &problems_at(dir.path(), &scratch_suite(None, &[])),
        "crates/demo/tests/flat.rs:2: the suite can't tell which test \
         `#[cfg_attr(zakura_sim, ignore)]` ignores",
    );
}

#[test]
fn a_sim_library_without_unit_tests_fails() {
    let dir = scratch();
    let suite = scratch_suite(None, &[]);
    let problem = "sim package `demo`'s library has no unit tests";
    assert!(!problems_at(dir.path(), &suite)
        .iter()
        .any(|found| found.contains(problem)),);
    fs::write(dir.path().join("crates/demo/src/lib.rs"), "").unwrap();
    assert_problem(&problems_at(dir.path(), &suite), problem);
}

#[test]
fn a_simulator_target_takes_its_harness_from_its_cargo_toml() {
    let manifest = suite::manifest(&seed::repo_root()).unwrap_or_else(|problems| {
        panic!("{problems}");
    });
    for (name, harness) in [
        ("discovery_wait_complete", "discovery/wait-complete"),
        ("race_fixtures", "fixtures/all"),
    ] {
        let row = manifest
            .targets
            .iter()
            .find(|row| row.name == name)
            .expect("the suite runs the target");
        assert_eq!(row.harness.as_deref(), Some(harness), "{name}");
        assert!(
            row.command
                .starts_with(&format!("cargo xtask sim-seeds --sim {harness} ")),
            "{}",
            row.command
        );
    }
}

#[test]
fn a_simulator_target_without_a_registered_harness_fails() {
    let dir = scratch();
    let problems = problems_at(dir.path(), &scratch_suite(None, &[]));
    assert_problem(
        &problems,
        "the target `mapped` names the harness `no/such-harness`, which isn't registered",
    );
    assert_problem(
        &problems,
        "qa/fuzz/sim defines the target `unmapped`, and its \
         `[package.metadata.zakura-sim.targets]` table maps it to no harness",
    );
    assert_problem(
        &problems,
        "qa/fuzz/sim/Cargo.toml maps `stale` to a harness, and defines no such target",
    );
}

#[test]
fn an_excluded_simulator_target_needs_no_harness() {
    let dir = scratch();
    let mut suite = scratch_suite(None, &[]);
    suite.target.retain(|target| target.name != "unmapped");
    suite.excluded_target.push(ExcludedTarget {
        package: "sim".to_owned(),
        name: "unmapped".to_owned(),
        reason: "runs no harness".to_owned(),
    });
    let problems = problems_at(dir.path(), &suite);
    assert!(
        !problems
            .iter()
            .any(|problem| problem.contains("`unmapped`")),
        "{problems:?}"
    );
}

#[test]
fn a_target_row_cant_name_its_harness() {
    let error = toml::from_str::<SuiteFile>(
        "schema = 1\nplanted-timeout-secs = 1\nseed-timeout-secs = 1\n\n[[target]]\n\
         package = \"sim\"\nname = \"race_fixtures\"\nharness = \"fixtures/all\"\n\
         smoke-secs = 1\ntimeout-secs = 1\n",
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("unknown field `harness`"),
        "{error}"
    );
}

#[test]
fn a_directory_test_target_the_suite_doesnt_list_fails() {
    let dir = scratch();
    let problems = problems_at(dir.path(), &scratch_suite(Some(&["flat"]), &[]));
    assert_problem(
        &problems,
        "crates/demo/tests/nested/main.rs builds only under `--cfg zakura_sim`, and \
         qa/suite.toml doesn't list its test target `nested`",
    );
}

#[test]
fn an_ignored_test_in_a_directory_test_target_fails() {
    let dir = scratch();
    let problems = problems_at(dir.path(), &scratch_suite(None, &[]));
    assert_problem(
        &problems,
        "crates/demo/tests/nested/main.rs::top is ignored",
    );
    assert_problem(
        &problems,
        "crates/demo/tests/nested/helpers.rs::child is ignored",
    );
    // Listed, both run in `nested`.
    let problems = problems_at(
        dir.path(),
        &scratch_suite(
            None,
            &[
                "crates/demo/tests/nested/main.rs::top",
                "crates/demo/tests/nested/helpers.rs::child",
            ],
        ),
    );
    assert!(
        !problems
            .iter()
            .any(|problem| problem.contains("crates/demo")),
        "{problems:#?}"
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
fn a_simulator_target_neither_excluded_nor_mapped_fails() {
    let dir = scratch();
    let mut suite = scratch_suite(None, &[]);
    suite.target.retain(|target| target.name != "unmapped");
    let problems = problems_at(dir.path(), &suite);
    assert_problem(
        &problems,
        "qa/fuzz/sim defines the target `unmapped`, and qa/suite.toml neither maps nor excludes it",
    );
    assert_problem(
        &problems,
        "qa/fuzz/sim defines the target `unmapped`, and its \
         `[package.metadata.zakura-sim.targets]` table maps it to no harness",
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
fn an_ignored_test_in_a_suites_child_module_fails() {
    let mut suite = committed();
    remove(&mut suite.ignored, |row| {
        row.test == "maximum_sessions_keep_header_serving_bounded"
    });
    assert_problem(
        &problems(&suite),
        "header_sync/v9/publisher/tests/load.rs::maximum_sessions_keep_header_serving_bounded is \
         ignored",
    );
}

#[test]
fn library_tests_run_by_their_full_path() {
    let manifest = suite::manifest(&seed::repo_root()).unwrap_or_else(|problems| {
        panic!("{problems}");
    });
    let row = manifest
        .tests
        .iter()
        .find(|row| {
            row.id
                .ends_with("/maximum_sessions_keep_header_serving_bounded")
        })
        .expect("the suite runs the load test");
    assert_eq!(
        row.command,
        "CARGO_TARGET_DIR=target/suite cargo test --locked -p zakura-network --features \
         zakura-testkit --lib -- --ignored --exact \
         zakura::header_sync::v9::publisher::tests::load::maximum_sessions_keep_header_serving_bounded"
    );
}

#[test]
fn a_target_runs_with_its_environment() {
    let manifest = suite::manifest(&seed::repo_root()).unwrap_or_else(|problems| {
        panic!("{problems}");
    });
    let row = manifest
        .targets
        .iter()
        .find(|row| row.name == "recovery_rows")
        .expect("the suite runs recovery_rows");
    assert!(
        row.command.contains(&format!(
            "&& ASAN_OPTIONS=detect_leaks=0 cargo +{NIGHTLY} fuzz run"
        )),
        "{}",
        row.command
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

#[test]
fn suite_refuses_to_run_in_another_checkout() {
    // A directory outside this checkout stands in for another checkout,
    // which a relative `--out` would write into.
    let dir = TempDir::new().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(["suite", "--out", "manifest.json"])
        .current_dir(dir.path())
        .output()
        .expect("the binary runs");
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("this zakura-sim was built in the checkout"),
        "{stderr}"
    );
    assert!(!dir.path().join("manifest.json").exists());
}

#[test]
fn another_checkout_is_one_with_another_worktree() {
    // `inner` is a git worktree nested in `outer`'s, and `outside` lies in
    // neither.
    let outer = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let inner = outer.path().join("inner");
    let plain = outer.path().join("plain");
    fs::create_dir(&inner).unwrap();
    fs::create_dir(&plain).unwrap();
    for dir in [outer.path(), inner.as_path()] {
        let status = Command::new("git")
            .args(["init", "-q"])
            .arg(dir)
            .status()
            .expect("git runs");
        assert!(status.success());
    }
    assert_eq!(suite::other_checkout(outer.path(), outer.path()), None);
    assert_eq!(suite::other_checkout(outer.path(), &plain), None);
    assert!(suite::other_checkout(outer.path(), &inner).is_some());
    assert!(suite::other_checkout(outer.path(), outside.path()).is_some());
}

#[test]
fn the_manifest_doesnt_depend_on_the_executable() {
    // A copy with a byte appended runs the same, and has another digest.
    let dir = TempDir::new().unwrap();
    let copy = dir.path().join("zakura-sim");
    let mut bytes = std::fs::read(env!("CARGO_BIN_EXE_zakura-sim")).unwrap();
    bytes.push(0);
    std::fs::write(&copy, bytes).unwrap();
    std::fs::set_permissions(&copy, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let mut manifests = Vec::new();
    for (binary, name) in [
        (
            std::path::PathBuf::from(env!("CARGO_BIN_EXE_zakura-sim")),
            "a.json",
        ),
        (copy, "b.json"),
    ] {
        let out = dir.path().join(name);
        let output = Command::new(binary)
            .args(["suite", "--out", out.to_str().unwrap()])
            .output()
            .expect("the binary runs");
        assert!(output.status.success(), "{output:?}");
        manifests.push(std::fs::read(&out).unwrap());
    }
    assert!(manifests[0] == manifests[1], "the two manifests differ");
}
