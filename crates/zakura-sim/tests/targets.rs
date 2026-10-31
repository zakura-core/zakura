//! Fuzz target readiness: every target `qa/suite.toml` declares resolves
//! to its registered harness and has curated seeds that apply the
//! harness's own properties, and `zakura-sim targets` gives each target
//! one status (explorer implementation plan, E5a, acceptance test 1).
//!
//! The build and the fuzzer run need the pinned nightly and cargo-fuzz, so
//! the nightly `zakura_sim targets` job runs them. These tests check the
//! committed tables and seeds, and the status each run's evidence gets.
#![cfg(zakura_sim)]

use std::{
    fs,
    path::{Path, PathBuf},
};

use zakura_sim::{
    campaign::{
        bypasses, declared_targets, judge_run, max_len, own_applicability, resolve, run_seed,
        SeedRun, TargetStatus, FUZZ_PACKAGE, SHARED_PROPERTIES,
    },
    explore::{
        stats::{Counts, ProcessStats, STATS_VERSION},
        ProcessExit, Worker,
    },
    harness::HARNESSES,
    seed,
    suite::{SuiteFile, SUITE_FILE},
};

/// The repository's root.
fn root() -> PathBuf {
    seed::repo_root()
}

/// `qa/suite.toml`.
fn suite() -> SuiteFile {
    toml::from_str(&fs::read_to_string(root().join(SUITE_FILE)).unwrap()).unwrap()
}

/// The simulator fuzz package's manifest.
fn manifest() -> String {
    fs::read_to_string(root().join(FUZZ_PACKAGE).join("Cargo.toml")).unwrap()
}

/// The committed seeds' directory.
fn fixtures() -> PathBuf {
    root().join("crates/zakura-sim/tests/fixtures")
}

/// Each declared target resolves to its registered harness, its harness has
/// admitted seeds and no rejected one, and the seeds apply each of the
/// harness's own properties and those of every harness the target covers.
/// A target without seeds, or whose seeds reach only the properties every
/// harness checks, fails here before the nightly job runs it.
#[test]
fn every_declared_target_runs_seeds_that_apply_its_properties() {
    let suite = suite();
    let manifest = manifest();
    let declared = declared_targets(&suite);
    assert!(!declared.is_empty(), "{SUITE_FILE} declares sim targets");
    for target in &declared {
        let entry = resolve(&manifest, &target.name).unwrap();
        let loaded = seed::load(&[fixtures()], Some(entry.name)).unwrap();
        assert!(
            loaded.rejected.is_empty(),
            "{}: {:?}",
            target.name,
            loaded.rejected
        );
        assert!(
            !loaded.admitted.is_empty(),
            "{} ({}) has no curated seed",
            target.name,
            entry.name
        );
        if let Some(max) = max_len(&target.args) {
            for seed in &loaded.admitted {
                assert!(
                    seed.input.len() <= max,
                    "{}: -max_len={max} cuts {} ({} bytes)",
                    target.name,
                    seed.path.display(),
                    seed.input.len()
                );
            }
        }
        let mut worker = Worker::new();
        let seeds: Vec<SeedRun> = loaded
            .admitted
            .iter()
            .map(|seed| {
                let (outcome, applied) = run_seed(&mut worker, entry, &seed.input);
                assert_eq!(outcome, "clean", "{}", seed.path.display());
                SeedRun {
                    file: seed.path.clone(),
                    sha256: seed::sha256(&seed.input),
                    outcome,
                    applied,
                }
            })
            .collect();
        let (applied, unapplied) = own_applicability(entry, &seeds);
        assert!(!applied.is_empty(), "{}", target.name);
        assert!(
            unapplied.is_empty(),
            "{}: no seed applies {unapplied:?}",
            target.name
        );
        let row = suite
            .target
            .iter()
            .find(|row| row.name == target.name)
            .unwrap();
        for covered in &row.covers {
            let covered = HARNESSES
                .iter()
                .find(|entry| entry.name == covered)
                .unwrap();
            let (_, unapplied) = own_applicability(covered, &seeds);
            assert!(
                unapplied.is_empty(),
                "{} covers {}, and no seed applies {unapplied:?}",
                target.name,
                covered.name
            );
        }
    }
}

/// A target the fuzz package doesn't map, or maps to an unregistered
/// harness, doesn't resolve.
#[test]
fn unmapped_targets_dont_resolve() {
    let mapped = "[package.metadata.zakura-sim.targets]\nknown = \"discovery/wait-complete\"\n\
                  ghost = \"no/such-harness\"\n";
    assert_eq!(
        resolve(mapped, "known").unwrap().name,
        "discovery/wait-complete"
    );
    assert!(resolve(mapped, "ghost")
        .unwrap_err()
        .contains("no harness registers"));
    assert!(resolve(mapped, "missing")
        .unwrap_err()
        .contains("maps `missing` to no harness"));
}

/// Only the simulator fuzz package's rows are declared targets.
#[test]
fn declared_targets_are_the_sim_packages() {
    let suite = suite();
    let declared: Vec<String> = declared_targets(&suite)
        .into_iter()
        .map(|target| target.name)
        .collect();
    let sim: Vec<String> = suite
        .target
        .iter()
        .filter(|target| target.package == "sim")
        .map(|target| target.name.clone())
        .collect();
    assert_eq!(declared, sim);
    assert!(!declared.contains(&"fork_transitions".to_owned()));
}

/// The discovery seed carries the lost-wakeup bypass, and the header
/// storage seeds carry none.
#[test]
fn bypasses_follow_their_committed_input() {
    let load = |harness| seed::load(&[fixtures()], Some(harness)).unwrap().admitted;
    let discovery = bypasses(&root(), &load("discovery/wait-complete")).unwrap();
    assert_eq!(discovery.len(), 1, "{discovery:?}");
    assert_eq!(discovery[0].property, "discovery/wait-complete-wakes");
    assert_eq!(
        discovery[0].patch,
        Path::new("qa/planted/discovery/wait-complete-wakes/lost-wakeup.patch")
    );
    assert!(bypasses(&root(), &load("header-sync/storage"))
        .unwrap()
        .is_empty());
}

/// The harness's own properties leave out the ones every harness checks.
#[test]
fn shared_properties_never_count_as_applied() {
    let entry = HARNESSES
        .iter()
        .find(|entry| entry.name == "discovery/wait-complete")
        .unwrap();
    let seeds = [SeedRun {
        file: PathBuf::from("seed.bin"),
        sha256: String::new(),
        outcome: "clean".to_owned(),
        applied: SHARED_PROPERTIES
            .iter()
            .map(|id| (*id).to_owned())
            .collect(),
    }];
    let (applied, unapplied) = own_applicability(entry, &seeds);
    assert_eq!(applied, Vec::<String>::new());
    assert_eq!(unapplied, ["discovery/wait-complete-wakes"]);
}

/// A stats file of `harness` with `runs`, complete or not.
fn stats(harness: &str, runs: u64, complete: bool) -> Result<ProcessStats, String> {
    Ok(ProcessStats {
        version: STATS_VERSION,
        harness: harness.to_owned(),
        pid: 1,
        started_unix: 0,
        written_unix: 0,
        complete,
        counts: Counts {
            runs,
            ..Counts::default()
        },
    })
}

/// libFuzzer's output for a run of `seeds`.
fn executed(seeds: &[(PathBuf, usize)]) -> Vec<u8> {
    seeds
        .iter()
        .map(|(seed, _)| format!("Running: {0}\nExecuted {0} in 1 ms\n", seed.display()))
        .collect::<String>()
        .into_bytes()
}

/// Each piece of evidence that falls short gets its own status, and only a
/// clean run with every seed counted and an own property applied is ready.
#[test]
fn each_shortfall_gets_its_own_status() {
    const HARNESS: &str = "discovery/wait-complete";
    let seeds = [(PathBuf::from("/s/a"), 10), (PathBuf::from("/s/b"), 4096)];
    let log = executed(&seeds);
    let applied = ["discovery/wait-complete-wakes".to_owned()];
    let ok = ProcessExit::Code(0);
    let judge = |exit, log: &[u8], files: &[Result<ProcessStats, String>], applied: &[String]| {
        judge_run(HARNESS, &seeds, Some(4096), exit, log, files, applied).0
    };

    // libFuzzer runs a list of files without the empty input, so a run of
    // each seed once is ready.
    assert_eq!(
        judge(ok, &log, &[stats(HARNESS, 2, true)], &applied),
        TargetStatus::Ready
    );
    // A leak check runs a seed twice.
    assert_eq!(
        judge(ok, &log, &[stats(HARNESS, 4, true)], &applied),
        TargetStatus::Ready
    );
    assert_eq!(
        judge(ok, &log, &[stats(HARNESS, 3, true)], &[]),
        TargetStatus::NoApplicableProperty
    );
    for exit in [
        ProcessExit::Code(1),
        ProcessExit::Signal(6),
        ProcessExit::TimedOut,
    ] {
        assert_eq!(
            judge(exit, &log, &[stats(HARNESS, 3, false)], &applied),
            TargetStatus::SeedsFailed,
            "{exit:?}"
        );
    }
    let mut sanitized = log.clone();
    sanitized.extend_from_slice(b"==1==ERROR: AddressSanitizer: heap-use-after-free\n");
    assert_eq!(
        judge(ok, &sanitized, &[stats(HARNESS, 3, true)], &applied),
        TargetStatus::SeedsFailed
    );
    assert_eq!(
        judge(ok, &log, &[stats("header-sync/storage", 3, true)], &applied),
        TargetStatus::WrongHarness
    );
    for files in [
        vec![],
        vec![stats(HARNESS, 3, true), stats(HARNESS, 3, true)],
        vec![Err("parsing stats-1.json: bad".to_owned())],
        vec![stats(HARNESS, 3, false)],
        vec![stats(HARNESS, 1, true)],
    ] {
        assert_eq!(
            judge(ok, &log, &files, &applied),
            TargetStatus::Unaccounted,
            "{files:?}"
        );
    }
    let one_seed = executed(&seeds[..1]);
    assert_eq!(
        judge(ok, &one_seed, &[stats(HARNESS, 3, true)], &applied),
        TargetStatus::Unaccounted
    );
}

/// The suite row's variables can't change where the target writes its
/// stats or the simulator's artifacts, or set the filter or the
/// abandonment limit, which would let a failing seed pass.
#[test]
fn row_variables_dont_override_the_checks() {
    use zakura_sim::{
        campaign::{smoke_command, DeclaredTarget},
        explore::{stats::STATS_VAR, ABANDON_LIMIT_VAR, EXPECT_VAR},
        trace::ARTIFACTS_VAR,
    };
    let target = DeclaredTarget {
        name: "discovery_wait_complete".to_owned(),
        args: Vec::new(),
        env: vec![
            format!("{STATS_VAR}=/elsewhere"),
            format!("{ARTIFACTS_VAR}=/elsewhere"),
            format!("{EXPECT_VAR}=discovery/wait-complete-wakes"),
            format!("{ABANDON_LIMIT_VAR}=1000"),
            "ASAN_OPTIONS=detect_leaks=0".to_owned(),
        ],
    };
    let dir = Path::new("/out/discovery_wait_complete");
    let command = smoke_command(
        Path::new("/out/fuzzer"),
        &target,
        &[PathBuf::from("/out/seed")],
        dir,
        std::time::Duration::from_secs(60),
    );
    let envs: std::collections::BTreeMap<_, _> = command
        .get_envs()
        .map(|(name, value)| (name.to_owned(), value.map(ToOwned::to_owned)))
        .collect();
    let get = |name: &str| envs.get(std::ffi::OsStr::new(name)).cloned();
    assert_eq!(get(STATS_VAR), Some(Some(dir.join("stats").into())));
    assert_eq!(get(ARTIFACTS_VAR), Some(Some(dir.join("sim").into())));
    assert_eq!(get(EXPECT_VAR), Some(None));
    assert_eq!(get(ABANDON_LIMIT_VAR), Some(None));
    assert_eq!(get("ASAN_OPTIONS"), Some(Some("detect_leaks=0".into())));
}

/// libFuzzer cuts an input longer than `-max_len`, even one it runs as a
/// file, so a seed longer than the row's `-max_len` never runs whole.
#[test]
fn a_seed_past_max_len_is_unaccounted() {
    const HARNESS: &str = "discovery/wait-complete";
    let args = |args: &[&str]| args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    assert_eq!(max_len(&args(&[])), None);
    assert_eq!(max_len(&args(&["-max_len=4096"])), Some(4096));
    assert_eq!(
        max_len(&args(&["-max_len=64", "-max_len=4096"])),
        Some(4096)
    );
    assert_eq!(max_len(&args(&["-max_len=4096", "-max_len=0"])), None);
    assert_eq!(max_len(&args(&["-rss_limit_mb=4096"])), None);

    let seeds = [(PathBuf::from("/s/a"), 10), (PathBuf::from("/s/b"), 4097)];
    let log = executed(&seeds);
    let applied = ["discovery/wait-complete-wakes".to_owned()];
    let judge = |max| {
        judge_run(
            HARNESS,
            &seeds,
            max,
            ProcessExit::Code(0),
            &log,
            &[stats(HARNESS, 2, true)],
            &applied,
        )
    };
    let (status, detail) = judge(Some(4096));
    assert_eq!(status, TargetStatus::Unaccounted);
    assert!(detail.unwrap().contains("/s/b"));
    assert_eq!(judge(Some(4097)).0, TargetStatus::Ready);
    assert_eq!(judge(None).0, TargetStatus::Ready);
}
