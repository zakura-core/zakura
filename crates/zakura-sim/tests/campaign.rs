//! Durable campaigns: runs end with one status, interrupted runs resume
//! with their corpus additions and failures, and snapshots survive an
//! interrupted compaction (explorer implementation plan, E3a-2, and
//! acceptance tests 3 and 4).
//!
//! A shell script stands in for the libFuzzer binary. It takes the flags
//! the supervisor passes, writes a corpus file every 50 ms until its
//! `-max_total_time`, and does what the files in its control directory
//! say: crash once or always, end at the abandonment limit, exit 0 early,
//! exit 1 once after its stats file is complete, log 8 MiB, log a feedback
//! line, hang in a merge, or leave a merge process behind that writes a
//! crash file later. With a `stats` control file, it writes
//! that file to `$ZAKURA_SIM_STATS` as its stats file, incomplete at start
//! and complete at a clean exit, as a real target does. It exits when its
//! parent does, as a real fuzzer does when a terminal's interrupt reaches
//! its process group.
#![cfg(zakura_sim)]

use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output},
    thread,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use zakura_sim::{
    campaign::{
        self, AttemptEnd, Budget, BuildIdentity, CampaignError, Compaction, Digest, End,
        Incomplete, Migration, Recorder, ResumeOptions, RunState, StartOptions, Status, Store,
        WorkerOutcome, WorkerPlan, ATTEMPTS_FILE, FUZZ_PACKAGE,
    },
    explore::{
        stats::{CleanupCounts, Counts, ProcessStats, PropertyCounts, STATS_VAR, STATS_VERSION},
        ProcessFailure, ABANDON_LIMIT_PANIC, EXPECT_VAR,
    },
    harness,
    property::Status as PropertyStatus,
    trace::BuildInfo,
};

/// The harness the tests fuzz. It has one committed seed.
const SIM: &str = "discovery/wait-complete";

/// The stand-in fuzzer. `@CONTROL@` and `@ABANDON@` are replaced.
const STUB: &str = r#"#!/usr/bin/env bash
control='@CONTROL@'
dirs=(); time=0; prefix=""; seed=0; runs=""; merge=""
for arg in "$@"; do
  case "$arg" in
    -max_total_time=*) time=${arg#*=} ;;
    -artifact_prefix=*) prefix=${arg#*=} ;;
    -seed=*) seed=${arg#*=} ;;
    -runs=*) runs=${arg#*=} ;;
    -merge=*) merge=${arg#*=} ;;
    -*) ;;
    *) dirs+=("$arg") ;;
  esac
done
parent=$PPID
touch "$control/ran"
echo "asan=$ASAN_OPTIONS" >&2
echo "expect=$ZAKURA_SIM_EXPECT" >&2
alive() { kill -0 "$parent" 2>/dev/null; }
stats() {
  if [ -n "$ZAKURA_SIM_STATS" ] && [ -e "$control/stats" ]; then
    mkdir -p "$ZAKURA_SIM_STATS"
    sed -e "s/@PID@/$$/" -e "s/@COMPLETE@/$1/" -e "s/@STARTED@/$started/" "$control/stats" \
      > "$ZAKURA_SIM_STATS/.partial-$$"
    mv "$ZAKURA_SIM_STATS/.partial-$$" "$ZAKURA_SIM_STATS/stats-$$.json"
  fi
}
if [ -e "$control/feedback" ]; then
  echo "zakura-sim feedback: backend=stub version=7 features=1 mode=on" >&2
fi
if [ "$runs" = 0 ]; then exit 0; fi
if [ "$merge" = 1 ]; then
  if [ -n "$ZAKURA_SIM_STATS" ]; then touch "$control/merge-had-stats"; fi
  if [ -e "$control/merge-orphan" ]; then
    # libFuzzer's outer merge runs an inner process, which can outlive it.
    ( sleep 2; printf 'late %s' "$BASHPID" > "${prefix}crash-late-$BASHPID" ) &
  fi
  if [ -e "$control/merge-hangs" ]; then
    touch "$control/merging"
    while alive; do sleep 0.05; done
    exit 1
  fi
  if [ -e "$control/merge-crash" ]; then
    # libFuzzer's merge restarts its inner process past a crashing input,
    # and still exits 0.
    printf 'merge crash %s' "$$" > "${prefix}crash-merge-$$"
    mkdir -p "$ZAKURA_SIM_ARTIFACTS/inputs/active/stub"
    printf 'merge crash %s' "$$" > "$ZAKURA_SIM_ARTIFACTS/inputs/active/stub/merge-$$.bin"
    echo "==$$==ERROR: AddressSanitizer: heap-buffer-overflow on address 0x1" >&2
    echo "MERGE-OUTER: attempt 2" >&2
  fi
  for dir in "${dirs[@]:1}"; do
    for file in "$dir"/*; do
      [ -f "$file" ] || continue
      case "$file" in *[0-7]) cp "$file" "${dirs[0]}/" ;; esac
    done
  done
  exit 0
fi
if [ -e "$control/chatty" ]; then
  yes 'a chatty target line' | head -c 8388608 >&2
fi
started=$(date +%s)
stats false
out=${dirs[0]}
end=$(( $(date +%s%N) + time * 1000000000 ))
i=0
while [ "$(date +%s%N)" -lt "$end" ]; do
  alive || exit 1
  printf 'input %s %s %s %s' "$seed" "$i" "$$" "$RANDOM" > "$out/.partial-$$"
  mv "$out/.partial-$$" "$out/unit-$$-$i"
  i=$((i + 1))
  if [ "$i" = 2 ]; then
    if mv "$control/crash" "$control/crash-taken-$$" 2>/dev/null || [ -e "$control/always-crash" ]; then
      printf 'crash %s' "$$" > "${prefix}crash-$$"
      mkdir -p "$ZAKURA_SIM_ARTIFACTS/inputs/active/stub"
      printf 'crash %s' "$$" > "$ZAKURA_SIM_ARTIFACTS/inputs/active/stub/$$.bin"
      echo "==$$== ERROR: libFuzzer: deadly signal" >&2
      exit 1
    fi
    if [ -e "$control/exit-early" ]; then
      echo "Done $i runs in 0 second(s)" >&2
      stats true
      exit 0
    fi
    if mv "$control/abandon" "$control/abandon-taken-$$" 2>/dev/null; then
      echo "panicked: @ABANDON@: 1 runs abandoned their cleanup" >&2
      exit 1
    fi
  fi
  sleep 0.05
done
echo "Done $i runs in $time second(s)" >&2
stats true
if mv "$control/fail-after-stats" "$control/fail-after-stats-taken-$$" 2>/dev/null; then
  exit 1
fi
exit 0
"#;

/// A store with the stand-in fuzzer saved as a build.
struct Fixture {
    /// Holds every directory.
    dir: TempDir,
    /// The store.
    store: Store,
    /// The stand-in's control directory.
    control: PathBuf,
    /// The stand-in's build.
    build: Digest,
}

impl Fixture {
    /// A new store with the stand-in saved under [`identity`].
    fn new() -> Fixture {
        let dir = TempDir::new().unwrap();
        let control = dir.path().join("control");
        fs::create_dir(&control).unwrap();
        let stub = dir.path().join("stub");
        fs::write(
            &stub,
            STUB.replace("@CONTROL@", control.to_str().unwrap())
                .replace("@ABANDON@", ABANDON_LIMIT_PANIC),
        )
        .unwrap();
        fs::set_permissions(&stub, fs::Permissions::from_mode(0o755)).unwrap();
        let store = Store::open(&dir.path().join("store")).unwrap();
        let build = store
            .save_build(&fs::read(&stub).unwrap(), identity())
            .unwrap()
            .binary;
        Fixture {
            dir,
            store,
            control,
            build,
        }
    }

    /// A copy of the stand-in that differs in a comment, so it's another
    /// binary.
    fn variant(&self, tag: &str) -> PathBuf {
        let path = self.dir.path().join(format!("stub-{tag}"));
        let stub = fs::read_to_string(self.store.fuzzer_path(&self.build)).unwrap();
        fs::write(&path, format!("{stub}# {tag}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// Creates the control file `name`.
    fn set(&self, name: &str) {
        fs::write(self.control.join(name), b"").unwrap();
    }

    /// Removes the control file `name`.
    fn unset(&self, name: &str) {
        let _ = fs::remove_file(self.control.join(name));
    }

    /// `start`'s options: `workers` workers of `seconds` each, two
    /// restarts, the committed fixtures.
    fn options(&self, workers: usize, seconds: u64) -> StartOptions {
        StartOptions {
            campaign: "test".to_owned(),
            harness: harness::find(SIM).unwrap(),
            build: self.build.clone(),
            workers: (0..workers)
                .map(|index| WorkerPlan {
                    index,
                    seed: 1 + index as u32,
                    cpus: None,
                })
                .collect(),
            budget: Budget {
                seconds,
                max_restarts: 2,
                max_len: 4096,
                timeout_secs: 1,
                rss_limit_mb: 2048,
                abandon_limit: 1,
                feedback_mode: None,
                sanitizer_options: Default::default(),
                compact_secs: None,
                libfuzzer_args: Vec::new(),
            },
            fixtures: vec![fixtures()],
            suite: None,
            artifacts: None,
            parent: None,
        }
    }

    /// `zakura-sim campaign <args> --store <store>`, run to its end.
    fn cli(&self, args: &[&str]) -> Output {
        cli(self.store.root(), args)
    }

    /// The `start` command line of a two-worker run of `seconds`.
    fn start_args<'a>(&'a self, seconds: &'a str) -> Vec<&'a str> {
        vec![
            "start",
            "--campaign",
            "test",
            "--sim",
            SIM,
            "--build",
            self.build.as_str(),
            "--workers",
            "2",
            "--seconds",
            seconds,
            "--timeout",
            "1",
            "--fixtures",
            FIXTURES,
        ]
    }
}

/// `zakura-sim campaign <args> --store <store>`, started.
fn spawn(store: &Path, args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .arg("campaign")
        .args(args)
        .arg("--store")
        .arg(store)
        .spawn()
        .unwrap()
}

/// `zakura-sim campaign <args> --store <store>`, run to its end.
fn cli(store: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .arg("campaign")
        .args(args)
        .arg("--store")
        .arg(store)
        .output()
        .unwrap()
}

/// The stand-in's identity: this checkout's commit, so the runner check
/// passes.
fn identity() -> BuildIdentity {
    BuildIdentity {
        package: FUZZ_PACKAGE.to_owned(),
        target: "stub".to_owned(),
        harness: SIM.to_owned(),
        git: BuildInfo::current().git,
        dirty: false,
        source: "stub-source".to_owned(),
        lockfile: "stub-lockfile".to_owned(),
        fuzz_lockfile: "stub-fuzz-lockfile".to_owned(),
        rustc: "stub-rustc".to_owned(),
        cargo_fuzz: "stub-cargo-fuzz".to_owned(),
        libfuzzer_sys: "0.0.0".to_owned(),
        rustflags: "--cfg zakura_sim".to_owned(),
        cargo_args: Vec::new(),
    }
}

/// The committed fixture directory.
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

/// [`FIXTURES`] as a path.
fn fixtures() -> PathBuf {
    PathBuf::from(FIXTURES)
}

/// Waits up to 30 s for `ready`.
fn wait_for(what: &str, ready: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

/// The digests of the regular files under `dir`, at any depth, skipping
/// names that start with `.`.
fn digests_under(dir: &Path) -> BTreeSet<Digest> {
    let mut digests = BTreeSet::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return digests;
    };
    for entry in entries {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            digests.extend(digests_under(&path));
        } else {
            digests.insert(Digest::of_file(&path).unwrap());
        }
    }
    digests
}

/// The input digests of `snapshot`.
fn inputs(store: &Store, snapshot: &Digest) -> BTreeSet<Digest> {
    store
        .verify_snapshot(snapshot)
        .unwrap()
        .index
        .inputs
        .into_iter()
        .map(|entry| entry.sha256)
        .collect()
}

/// Starts a campaign in `store` with `options`, runs it, and returns the
/// run and its end.
fn start(store: &Store, options: StartOptions) -> (Digest, End) {
    let prepared = campaign::prepare_start(store, options).unwrap();
    let run = prepared.manifest().clone();
    (run, prepared.run(store).unwrap())
}

/// The only run that no run continues.
fn leaf(store: &Store) -> Digest {
    let leaves = store.leaves().unwrap();
    assert_eq!(leaves.len(), 1, "{leaves:?}");
    leaves[0].clone()
}

/// Copies `from` to `to`, recursively, as moving a store to another host
/// does.
fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// The run a supervisor holds, once one does.
fn running(store: &Store) -> Digest {
    let find = || {
        store
            .leaves()
            .ok()?
            .into_iter()
            .find(|run| store.run_state(run).ok() == Some(RunState::Running))
    };
    wait_for("a run to start", || find().is_some());
    find().unwrap()
}

/// Kills `child`, a supervisor of `run`, and waits for the run's lock to
/// free and for its workers' stand-in fuzzers, which see their parent die,
/// to exit.
fn kill(store: &Store, mut child: Child, run: &Digest) {
    child.kill().unwrap();
    child.wait().unwrap();
    wait_for("the run's lock to free", || {
        store.run_state(run).unwrap() == RunState::Open
    });
    wait_for("the run's workers' fuzzers to exit", || !runs_a_worker(run));
}

/// True if a process's command line names one of `run`'s worker
/// directories, as each worker fuzzer's corpus directory does.
fn runs_a_worker(run: &Digest) -> bool {
    let workers = format!("{run}/workers/");
    fs::read_dir("/proc").unwrap().any(|entry| {
        let cmdline = fs::read(entry.unwrap().path().join("cmdline")).unwrap_or_default();
        String::from_utf8_lossy(&cmdline).contains(&workers)
    })
}

/// Starts the CLI command `args` and kills it once both workers of the run
/// it starts have written at least three corpus files and `also` holds.
/// Returns the run.
fn start_and_kill(store: &Store, args: &[&str], also: impl Fn() -> bool) -> Digest {
    let child = spawn(store.root(), args);
    let run = running(store);
    wait_for("both workers to add inputs", || {
        also()
            && (0..2).all(|worker| {
                let corpus = store.run_dir(&run).join(format!("workers/{worker}/corpus"));
                fs::read_dir(corpus).map_or(0, |entries| entries.count()) >= 3
            })
    });
    kill(store, child, &run);
    run
}

/// Closes `run` once its compaction's merge has stopped, waiting up to 30 s.
fn close_after_merge(store: &Store, run: &Digest) -> End {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match campaign::close(store, run) {
            Ok(end) => return end,
            Err(CampaignError::Refused(reason))
                if reason.contains("merge still runs") && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(20))
            }
            Err(error) => panic!("closing {run}: {error}"),
        }
    }
}

/// Every input the run's workers added, and every failure file they left.
fn left_behind(store: &Store, run: &Digest) -> (BTreeSet<Digest>, BTreeSet<Digest>) {
    let workers = store.run_dir(run).join("workers");
    let mut corpus = BTreeSet::new();
    let mut failures = BTreeSet::new();
    for worker in fs::read_dir(&workers).unwrap() {
        let worker = worker.unwrap().path();
        corpus.extend(digests_under(&worker.join("corpus")));
        failures.extend(digests_under(&worker.join("artifacts")));
        failures.extend(digests_under(&worker.join("sim")));
    }
    (corpus, failures)
}

/// Checks that the store's records verify, that each manifest is the file
/// its digest names, and that every run has exactly one end record.
fn assert_store_verifies(store: &Store) {
    let verified = campaign::verify(store).unwrap();
    assert_eq!(verified.open + verified.running, 0, "{verified:?}");
    for digest in store.manifests().unwrap().keys() {
        let bytes = fs::read(store.manifest_path(digest)).unwrap();
        assert_eq!(&Digest::of(&bytes), digest);
        assert_eq!(store.run_state(digest).unwrap(), RunState::Ended);
    }
}

/// Acceptance test 4 and E3a-2's resume test, locally: a run killed with
/// its supervisor, then resumed on a copy of the store as on another host,
/// keeps every input its workers added and every failure they left; so
/// does a second interruption. Each run ends with exactly one status, and
/// every record verifies against its digest.
#[test]
fn resumed_runs_keep_corpus_additions_and_failures() {
    let host_a = Fixture::new();
    host_a.set("crash");
    let first = start_and_kill(&host_a.store, &host_a.start_args("60"), || {
        !host_a.control.join("crash").exists()
    });
    let (added, failures) = left_behind(&host_a.store, &first);
    assert!(!failures.is_empty(), "a worker crashed once");
    let manifest = host_a.store.read_manifest(&first).unwrap();
    let seeds = inputs(&host_a.store, &manifest.initial_snapshot);
    assert_eq!(seeds.len(), 1, "the harness has one committed seed");

    // Another host: a copy of the store, without the cache of anything.
    let other = TempDir::new().unwrap();
    let host_b = other.path().join("store");
    copy_tree(host_a.store.root(), &host_b);
    let store = Store::open(&host_b).unwrap();
    let output = cli(&host_b, &["resume", "--seconds", "1"]);
    assert!(output.status.success(), "{output:?}");

    let first_end = campaign::read_end(&store, &first).unwrap();
    assert_eq!(first_end.status, Status::Interrupted);
    assert_eq!(first_end.recorded_by, Recorder::Close);
    let kept = inputs(&store, &first_end.snapshot);
    assert!(kept.is_superset(&seeds));
    assert!(kept.is_superset(&added), "every added input is kept");
    let listed: BTreeSet<Digest> = first_end
        .failures
        .iter()
        .map(|file| file.sha256.clone())
        .collect();
    assert_eq!(listed, failures, "every failure file is listed");

    let second = leaf(&store);
    let second_manifest = store.read_manifest(&second).unwrap();
    assert_eq!(second_manifest.resume_parent.as_ref(), Some(&first));
    assert_eq!(second_manifest.initial_snapshot, first_end.snapshot);
    assert_eq!(second_manifest.segment, 1);
    let second_end = campaign::read_end(&store, &second).unwrap();
    assert_eq!(second_end.status, Status::Complete, "{second_end:?}");
    assert!(second_end
        .workers
        .iter()
        .all(|worker| worker.outcome == WorkerOutcome::BudgetSpent));
    assert!(inputs(&store, &second_end.snapshot).is_superset(&kept));
    assert!(second_end.added > 0);

    // A second interruption on the new host, then a second resume.
    let third = start_and_kill(&store, &["resume", "--seconds", "60"], || true);
    let (added_third, _) = left_behind(&store, &third);
    let output = cli(&host_b, &["resume", "--seconds", "1"]);
    assert!(output.status.success(), "{output:?}");
    let third_end = campaign::read_end(&store, &third).unwrap();
    assert_eq!(third_end.status, Status::Interrupted);
    let fourth = leaf(&store);
    let fourth_end = campaign::read_end(&store, &fourth).unwrap();
    assert_eq!(fourth_end.status, Status::Complete);
    let last = inputs(&store, &fourth_end.snapshot);
    assert!(last.is_superset(&kept));
    assert!(last.is_superset(&added_third));
    assert!(last.is_superset(&inputs(&store, &second_end.snapshot)));

    // The first host's failures are still there, by digest.
    assert_store_verifies(&store);
    let manifests = store.manifests().unwrap();
    assert_eq!(manifests.len(), 4);
    let output = cli(&host_b, &["verify"]);
    assert!(output.status.success(), "{output:?}");
    // A fifth resume can't rewrite an end record.
    for run in [&first, &second, &third, &fourth] {
        assert!(matches!(
            campaign::close(&store, run),
            Err(CampaignError::Refused(_))
        ));
    }
}

/// A worker restarts a fuzzer that crashes or reaches its abandonment
/// limit, logs each process, and a worker whose fuzzer keeps crashing
/// stops at its restart limit with the run's status saying so.
#[test]
fn workers_restart_failed_fuzzers_up_to_their_limit() {
    let fixture = Fixture::new();
    fixture.set("abandon");
    let mut options = fixture.options(1, 2);
    options
        .budget
        .sanitizer_options
        .insert("ASAN_OPTIONS".to_owned(), "detect_leaks=0".to_owned());
    let (run, end) = start(&fixture.store, options);
    let log = fs::read_to_string(fixture.store.run_dir(&run).join("workers/0/log-0.txt")).unwrap();
    assert!(log.contains("asan=detect_leaks=0\n"), "{log}");
    assert_eq!(end.status, Status::Complete, "{end:?}");
    assert_eq!(end.workers[0].attempts, 2);
    let log = fs::read_to_string(
        fixture
            .store
            .run_dir(&run)
            .join("workers/0")
            .join(ATTEMPTS_FILE),
    )
    .unwrap();
    let attempts: Vec<campaign::Attempt> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(attempts[0].end, AttemptEnd::AbandonLimit);
    assert_eq!(attempts[1].end, AttemptEnd::Finished);
    assert_eq!(attempts[1].seed, attempts[0].seed + 1);

    let fixture = Fixture::new();
    fixture.set("always-crash");
    let (_, end) = start(&fixture.store, fixture.options(1, 30));
    assert_eq!(end.status, Status::WorkerStopped);
    assert_eq!(end.workers[0].outcome, WorkerOutcome::RestartLimit);
    assert_eq!(
        end.workers[0].attempts, 3,
        "the first process and 2 restarts"
    );
    // Each crash left a libFuzzer artifact and a saved input.
    assert_eq!(end.failures.len(), 6, "{:?}", end.failures);
    assert!(end.added > 0, "the crashed processes' additions are kept");
}

/// A fuzzer that exits 0 before its time limit hasn't spent its budget:
/// the worker restarts it, and stops at its restart limit, so the run
/// isn't complete.
#[test]
fn fuzzers_that_exit_early_leave_the_run_incomplete() {
    let fixture = Fixture::new();
    fixture.set("exit-early");
    let (run, end) = start(&fixture.store, fixture.options(1, 30));
    assert_eq!(end.status, Status::WorkerStopped, "{end:?}");
    assert_eq!(end.workers[0].outcome, WorkerOutcome::RestartLimit);
    assert_eq!(
        end.workers[0].attempts, 3,
        "the first process and 2 restarts"
    );
    let log = fs::read_to_string(
        fixture
            .store
            .run_dir(&run)
            .join("workers/0")
            .join(ATTEMPTS_FILE),
    )
    .unwrap();
    assert!(log.lines().all(|line| {
        serde_json::from_str::<campaign::Attempt>(line).unwrap().end == AttemptEnd::EarlyExit
    }));
    let output = fixture.cli(&["resume", "--seconds", "1"]);
    assert!(!output.status.success(), "{output:?}");
}

/// Acceptance test 3's compaction part: a compaction that hangs past its
/// limit, or whose supervisor is killed, leaves the run's snapshot and the
/// initial one intact, and the failures too. A compaction that finishes
/// saves a smaller snapshot, which the next run starts from.
#[test]
fn interrupted_compaction_keeps_every_snapshot() {
    let fixture = Fixture::new();
    fixture.set("crash");
    fixture.set("merge-hangs");
    let mut options = fixture.options(1, 1);
    options.budget.compact_secs = Some(1);
    let (run, end) = start(&fixture.store, options);
    assert_eq!(end.status, Status::Complete);
    assert_eq!(
        end.compaction,
        Some(Compaction::Failed {
            failure: ProcessFailure::Timeout
        })
    );
    assert_eq!(end.head(), &end.snapshot);
    let manifest = fixture.store.read_manifest(&run).unwrap();
    fixture
        .store
        .verify_snapshot(&manifest.initial_snapshot)
        .unwrap();
    assert!(inputs(&fixture.store, &end.snapshot).len() > 1);
    assert!(!end.failures.is_empty());

    // The supervisor dies while it compacts. The resumed run inherits its
    // parent's budget, so it compacts too, and the merge hangs.
    fixture.unset("merging");
    let child = spawn(fixture.store.root(), &["resume", "--seconds", "1"]);
    let second = running(&fixture.store);
    wait_for("the compaction to start", || {
        fixture.control.join("merging").exists()
    });
    kill(&fixture.store, child, &second);
    let closed = close_after_merge(&fixture.store, &second);
    assert_eq!(closed.status, Status::Interrupted);
    assert_eq!(closed.compaction, None);
    assert!(inputs(&fixture.store, &closed.snapshot)
        .is_superset(&inputs(&fixture.store, &end.snapshot)));
    fixture.store.verify_snapshot(&end.snapshot).unwrap();
    assert_store_verifies(&fixture.store);

    // A compaction that finishes.
    fixture.unset("merge-hangs");
    let output = fixture.cli(&["resume", "--seconds", "1"]);
    assert!(output.status.success(), "{output:?}");
    let third = leaf(&fixture.store);
    let compacted = campaign::read_end(&fixture.store, &third).unwrap();
    let Some(Compaction::Compacted {
        snapshot,
        inputs: kept,
    }) = &compacted.compaction
    else {
        panic!("the compaction finished: {compacted:?}");
    };
    assert!(
        *kept < compacted.inputs,
        "the stand-in's merge drops inputs"
    );
    assert_eq!(compacted.head(), snapshot);
    assert!(
        inputs(&fixture.store, snapshot).is_subset(&inputs(&fixture.store, &compacted.snapshot))
    );
    let output = fixture.cli(&["resume", "--seconds", "1"]);
    assert!(output.status.success(), "{output:?}");
    let fourth = leaf(&fixture.store);
    assert_eq!(
        &fixture
            .store
            .read_manifest(&fourth)
            .unwrap()
            .initial_snapshot,
        snapshot
    );
}

/// A fuzzer's log keeps the start and the end of its output, and drops the
/// middle, so a target that logs without bound fills neither the disk nor
/// the supervisor's memory. The run goes on as usual.
#[test]
fn a_fuzzers_log_keeps_the_start_and_end_of_its_output() {
    let fixture = Fixture::new();
    fixture.set("chatty");
    let (run, end) = start(&fixture.store, fixture.options(1, 1));
    assert_eq!(end.status, Status::Complete, "{end:?}");
    let log = fs::read(fixture.store.run_dir(&run).join("workers/0/log-0.txt")).unwrap();
    assert!(
        log.len() <= (2 << 20) + 4096,
        "the log holds {} bytes",
        log.len()
    );
    let log = String::from_utf8_lossy(&log);
    assert!(log.starts_with("asan="), "the start is kept");
    assert!(log.contains("bytes of output]"), "the drop is counted");
    assert!(log.trim_end().ends_with("second(s)"), "the end is kept");
}

/// No merge process writes after the end record: the supervisor kills the
/// merge's whole process group at its time limit, and `close` refuses
/// while a merge whose supervisor was killed still runs.
#[test]
fn orphaned_merge_processes_cant_write_after_the_end_record() {
    let fixture = Fixture::new();
    fixture.set("merge-hangs");
    fixture.set("merge-orphan");
    let mut options = fixture.options(1, 1);
    options.budget.compact_secs = Some(1);
    let (run, end) = start(&fixture.store, options);
    assert_eq!(
        end.compaction,
        Some(Compaction::Failed {
            failure: ProcessFailure::Timeout
        })
    );
    // Past the time the inner process would write its crash file.
    thread::sleep(Duration::from_millis(2500));
    let listed: BTreeSet<Digest> = end
        .failures
        .iter()
        .map(|file| file.sha256.clone())
        .collect();
    let compaction = fixture.store.run_dir(&run).join("compact-artifacts");
    assert!(
        digests_under(&compaction).is_subset(&listed),
        "a merge process wrote after the end record: {:?}",
        fs::read_dir(&compaction).unwrap().collect::<Vec<_>>()
    );

    // The supervisor dies while it compacts, and the inner process lives on.
    fixture.unset("merging");
    let child = spawn(fixture.store.root(), &["resume", "--seconds", "1"]);
    let second = running(&fixture.store);
    wait_for("the compaction to start", || {
        fixture.control.join("merging").exists()
    });
    kill(&fixture.store, child, &second);
    let refused = campaign::close(&fixture.store, &second).unwrap_err();
    assert!(
        refused.to_string().contains("merge still runs"),
        "{refused}"
    );
    let closed = close_after_merge(&fixture.store, &second);
    assert!(
        closed
            .failures
            .iter()
            .any(|file| file.path.contains("/compact-artifacts/crash-late-")),
        "{:?}",
        closed.failures
    );
    assert_store_verifies(&fixture.store);
}

/// The fuzzer runs without the supervisor's `ZAKURA_SIM_EXPECT`, which
/// would make the target ignore every failure but one property's, and
/// which the manifest doesn't record.
#[test]
fn fuzzers_run_without_the_supervisors_expected_property() {
    let fixture = Fixture::new();
    let build = fixture.build.as_str();
    let args = [
        "start",
        "--campaign",
        "test",
        "--sim",
        SIM,
        "--build",
        build,
        "--seconds",
        "1",
        "--fixtures",
        FIXTURES,
    ];
    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .arg("campaign")
        .args(args)
        .arg("--store")
        .arg(fixture.store.root())
        .env(EXPECT_VAR, "sim/no-task-panic")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let run = leaf(&fixture.store);
    let log = fs::read_to_string(fixture.store.run_dir(&run).join("workers/0/log-0.txt")).unwrap();
    assert!(log.contains("expect=\n"), "{log}");
}

/// A merge that crashes on an input finishes all the same, as libFuzzer's
/// does: the run records the compaction, and lists the crash file and the
/// target's saved input, both kept inside the store.
#[test]
fn a_merge_that_crashes_on_an_input_still_compacts() {
    let fixture = Fixture::new();
    fixture.set("merge-crash");
    let mut options = fixture.options(1, 1);
    options.budget.compact_secs = Some(30);
    let (run, end) = start(&fixture.store, options);
    assert!(
        matches!(end.compaction, Some(Compaction::Compacted { .. })),
        "{end:?}"
    );
    let run_dir = format!("runs/{run}/");
    let listed: Vec<&str> = end
        .failures
        .iter()
        .map(|file| file.path.as_str())
        .filter(|path| path.starts_with(&run_dir) && !path.contains("/workers/"))
        .collect();
    assert_eq!(listed.len(), 2, "{:?}", end.failures);
    assert!(
        listed.iter().any(|path| path.contains("/crash-merge-")),
        "{listed:?}"
    );
    assert!(
        listed.iter().any(|path| path.contains("/merge-")
            && path.ends_with(".bin")
            && !path.contains("/crash-")),
        "{listed:?}"
    );
    assert_store_verifies(&fixture.store);
}

/// Resume refuses another build identity, without running the build, or
/// another feedback, and a new segment starts from the seeds alone or
/// carries the corpus.
#[test]
fn incompatible_runs_start_a_new_segment() {
    let fixture = Fixture::new();
    fixture.set("feedback");
    let prepared = campaign::prepare_start(&fixture.store, fixture.options(1, 1)).unwrap();
    let run = prepared.manifest().clone();
    let manifest = fixture.store.read_manifest(&run).unwrap();
    let feedback = manifest.feedback.as_ref().expect("the stand-in logs it");
    assert_eq!(
        (
            feedback.backend.as_str(),
            feedback.version.as_str(),
            feedback.mode.as_str()
        ),
        ("stub", "7", "on")
    );
    let end = prepared.run(&fixture.store).unwrap();

    let other = fixture.dir.path().join("other-stub");
    fs::copy(fixture.store.fuzzer_path(&fixture.build), &other).unwrap();
    fs::write(
        &other,
        fs::read_to_string(&other).unwrap() + "# another build\n",
    )
    .unwrap();
    let mut changed = identity();
    changed.cargo_args = vec!["--sanitizer".to_owned(), "none".to_owned()];
    let other_build = fixture
        .store
        .save_build(&fs::read(&other).unwrap(), changed)
        .unwrap()
        .binary;
    fixture.unset("ran");
    let refused = campaign::prepare_resume(
        &fixture.store,
        ResumeOptions {
            build: Some(other_build.clone()),
            ..ResumeOptions::default()
        },
    )
    .unwrap_err();
    assert!(
        refused.to_string().contains("build cargo_args"),
        "{refused}"
    );
    assert!(
        !fixture.control.join("ran").exists(),
        "resume refuses the build without running it"
    );

    fixture.unset("feedback");
    let refused = campaign::prepare_resume(&fixture.store, ResumeOptions::default()).unwrap_err();
    assert!(refused.to_string().contains("feedback"), "{refused}");
    assert_eq!(
        fixture.store.manifests().unwrap().len(),
        1,
        "nothing written"
    );

    // A store holds one campaign.
    let refused = campaign::prepare_start(&fixture.store, fixture.options(1, 1)).unwrap_err();
    assert!(
        refused.to_string().contains("already holds a campaign"),
        "{refused}"
    );

    let mut options = fixture.options(1, 1);
    options.build = other_build.clone();
    options.parent = Some((run.clone(), Migration::Seeds));
    let fresh_prepared = campaign::prepare_start(&fixture.store, options).unwrap();
    let fresh = fresh_prepared.manifest().clone();
    let fresh_manifest = fixture.store.read_manifest(&fresh).unwrap();
    assert_eq!(fresh_manifest.segment, 2);
    assert_eq!(fresh_manifest.previous_segment.as_ref(), Some(&run));
    assert_eq!(fresh_manifest.migration, Some(Migration::Seeds));
    assert_eq!(
        inputs(&fixture.store, &fresh_manifest.initial_snapshot),
        inputs(&fixture.store, &manifest.initial_snapshot),
        "the seeds alone"
    );
    // The parent is continued now.
    let refused = campaign::prepare_resume(
        &fixture.store,
        ResumeOptions {
            parent: Some(run.clone()),
            ..ResumeOptions::default()
        },
    )
    .unwrap_err();
    assert!(
        refused.to_string().contains("already continued"),
        "{refused}"
    );
    let fresh_end = fresh_prepared.run(&fixture.store).unwrap();

    let mut options = fixture.options(1, 1);
    options.parent = Some((fresh.clone(), Migration::Carry));
    let carried = campaign::prepare_start(&fixture.store, options).unwrap();
    let carried_manifest = fixture.store.read_manifest(carried.manifest()).unwrap();
    assert_eq!(carried_manifest.segment, 3);
    assert_eq!(
        carried_manifest.initial_snapshot, fresh_end.snapshot,
        "the seeds are already in the carried snapshot"
    );
    assert!(inputs(&fixture.store, &end.snapshot).len() > 1);
}

/// A run's manifest names the harness its fuzzer runs: `start` refuses a
/// build whose target runs another harness than `--sim`.
#[test]
fn a_run_names_the_harness_its_fuzzer_runs() {
    let fixture = Fixture::new();
    let mut fixtures = identity();
    fixtures.target = "race_fixtures".to_owned();
    fixtures.harness = "fixtures/all".to_owned();
    let mut options = fixture.options(1, 1);
    options.build = fixture
        .store
        .save_build(&fs::read(fixture.variant("fixtures")).unwrap(), fixtures)
        .unwrap()
        .binary;
    let refused = campaign::prepare_start(&fixture.store, options).unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("runs harness fixtures/all, not"),
        "{refused}"
    );
    assert!(fixture.store.manifests().unwrap().is_empty());
}

/// A dirty build, or one with unknown provenance, resumes only on its own
/// binary, since its identity may not say what the binary was built from.
/// A build from an unknown commit can't start a run.
#[test]
fn dirty_builds_resume_only_on_their_own_binary() {
    let fixture = Fixture::new();
    let mut dirty = identity();
    dirty.dirty = true;
    let save = |tag: &str, identity: BuildIdentity| {
        fixture
            .store
            .save_build(&fs::read(fixture.variant(tag)).unwrap(), identity)
            .unwrap()
            .binary
    };
    let mut options = fixture.options(1, 1);
    options.build = save("dirty-a", dirty.clone());
    start(&fixture.store, options);
    let other = save("dirty-b", dirty);
    let refused = campaign::prepare_resume(
        &fixture.store,
        ResumeOptions {
            build: Some(other),
            ..ResumeOptions::default()
        },
    )
    .unwrap_err();
    assert!(refused.to_string().contains("build binary"), "{refused}");
    let resumed = campaign::prepare_resume(&fixture.store, ResumeOptions::default()).unwrap();
    resumed.run(&fixture.store).unwrap();

    let mut unknown = identity();
    unknown.git = "unknown".to_owned();
    let mut options = fixture.options(1, 1);
    options.build = save("unknown", unknown);
    let refused = campaign::prepare_start(&fixture.store, options).unwrap_err();
    assert!(refused.to_string().contains("unknown commit"), "{refused}");
}

/// A new segment carries a corpus only between clean builds, since a dirty
/// build can change the generator without changing the harness's versions.
/// It still starts from the seeds.
#[test]
fn dirty_builds_never_carry_a_corpus() {
    let fixture = Fixture::new();
    let mut dirty = identity();
    dirty.dirty = true;
    let dirty = fixture
        .store
        .save_build(&fs::read(fixture.variant("dirty")).unwrap(), dirty)
        .unwrap()
        .binary;
    let carry = |parent: &Digest, build: &Digest| {
        let mut options = fixture.options(1, 1);
        options.build = build.clone();
        options.parent = Some((parent.clone(), Migration::Carry));
        campaign::prepare_start(&fixture.store, options)
    };
    let mut options = fixture.options(1, 1);
    options.build = dirty.clone();
    let (dirty_run, _) = start(&fixture.store, options);

    // From a dirty build to a clean one.
    let refused = carry(&dirty_run, &fixture.build).unwrap_err();
    assert!(
        refused
            .to_string()
            .contains(&format!("fuzzer {dirty} is dirty")),
        "{refused}"
    );
    assert_eq!(
        fixture.store.manifests().unwrap().len(),
        1,
        "nothing written"
    );

    let mut options = fixture.options(1, 1);
    options.parent = Some((dirty_run, Migration::Seeds));
    let (clean_run, _) = start(&fixture.store, options);

    // From a clean build to a dirty one.
    let refused = carry(&clean_run, &dirty).unwrap_err();
    assert!(
        refused
            .to_string()
            .contains(&format!("fuzzer {dirty} is dirty")),
        "{refused}"
    );
    assert_eq!(
        fixture.store.manifests().unwrap().len(),
        2,
        "nothing written"
    );
    carry(&clean_run, &fixture.build).unwrap();
}

/// A snapshot is named by its index's digest, the same inputs share a
/// name, and a changed, missing, extra or resized input fails
/// verification. So does a changed manifest.
#[test]
fn snapshots_and_manifests_verify_against_their_digests() {
    let fixture = Fixture::new();
    let a = fixture
        .store
        .save_snapshot(&[
            campaign::Source::Input(b"one"),
            campaign::Source::Input(b"two"),
        ])
        .unwrap();
    let corpus = fixture.dir.path().join("corpus");
    fs::create_dir(&corpus).unwrap();
    fs::write(corpus.join("x"), b"two").unwrap();
    fs::write(corpus.join("y"), b"one").unwrap();
    fs::write(corpus.join(".partial-1"), b"half").unwrap();
    let b = fixture
        .store
        .save_snapshot(&[campaign::Source::Dir(&corpus)])
        .unwrap();
    assert_eq!(a, b);
    assert_eq!(a.index.inputs.len(), 2);
    let index = fs::read(fixture.store.snapshot_dir(&a.digest).join("snapshot.json")).unwrap();
    assert_eq!(Digest::of(&index), a.digest);

    let input = fixture
        .store
        .snapshot_inputs(&a.digest)
        .join(a.index.inputs[0].sha256.as_str());
    fs::write(&input, b"changed").unwrap();
    assert!(matches!(
        fixture.store.verify_snapshot(&a.digest),
        Err(CampaignError::Corrupt { .. })
    ));
    fs::remove_file(&input).unwrap();
    assert!(fixture.store.verify_snapshot(&a.digest).is_err());
    // A file of another length is refused by its length, unread.
    let sized = fixture
        .store
        .save_snapshot(&[campaign::Source::Input(b"five")])
        .unwrap();
    let unread = fixture
        .store
        .snapshot_inputs(&sized.digest)
        .join(Digest::of(b"five").as_str());
    fs::write(&unread, b"five and more").unwrap();
    fs::set_permissions(&unread, fs::Permissions::from_mode(0o000)).unwrap();
    assert!(
        matches!(
            fixture.store.verify_snapshot(&sized.digest),
            Err(CampaignError::Corrupt { .. })
        ),
        "{:?}",
        fixture.store.verify_snapshot(&sized.digest)
    );
    let extra = fixture
        .store
        .save_snapshot(&[campaign::Source::Input(b"three")])
        .unwrap();
    fs::write(
        fixture
            .store
            .snapshot_inputs(&extra.digest)
            .join(Digest::of(b"four").as_str()),
        b"four",
    )
    .unwrap();
    assert!(fixture.store.verify_snapshot(&extra.digest).is_err());

    let run = campaign::prepare_start(&fixture.store, fixture.options(1, 1))
        .unwrap()
        .manifest()
        .clone();
    let path = fixture.store.manifest_path(&run);
    let text = fs::read_to_string(&path).unwrap();
    fs::write(&path, text.replace("\"test\"", "\"tset\"")).unwrap();
    assert!(matches!(
        fixture.store.read_manifest(&run),
        Err(CampaignError::Corrupt { .. })
    ));
}

/// `close` reads a worker's attempts log line by line, and a garbled line,
/// such as one with bytes that aren't UTF-8, costs only that line.
#[test]
fn close_reads_past_a_garbled_attempts_line() {
    let fixture = Fixture::new();
    let child = spawn(fixture.store.root(), &fixture.start_args("60"));
    let run = running(&fixture.store);
    kill(&fixture.store, child, &run);
    let attempt = campaign::Attempt {
        attempt: 0,
        seed: 1,
        max_total_time: 60,
        pid: None,
        started_unix: 0,
        millis: 5,
        code: Some(1),
        signal: None,
        killed: false,
        end: AttemptEnd::Failure,
        failure: Some(ProcessFailure::Exit { code: 1 }),
        artifacts: Vec::new(),
    };
    let line = serde_json::to_string(&attempt).unwrap();
    let worker = fixture.store.run_dir(&run).join("workers/0");
    fs::create_dir_all(&worker).unwrap();
    let mut log = format!("{line}\n").into_bytes();
    log.extend_from_slice(b"{\"attempt\": \xff\xfe}\n");
    log.extend_from_slice(format!("{line}\n{{\"attempt\": 2, \"se").as_bytes());
    fs::write(worker.join(ATTEMPTS_FILE), log).unwrap();
    let end = campaign::close(&fixture.store, &run).unwrap();
    assert_eq!(end.workers[0].attempts, 2, "{end:?}");
    assert_eq!(end.workers[0].millis, 10);
}

/// `verify` and `resume` check an end record against its run's files: a
/// record that omits a failure file or names an older snapshot is refused.
/// A run whose lock a supervisor holds is running, even beside an end
/// record.
#[test]
fn end_records_match_their_runs() {
    let fixture = Fixture::new();
    fixture.set("crash");
    let (run, end) = start(&fixture.store, fixture.options(1, 1));
    assert!(!end.failures.is_empty() && end.added > 0, "{end:?}");
    let manifest = fixture.store.read_manifest(&run).unwrap();
    let path = fixture.store.run_dir(&run).join("end.json");
    let original = fs::read(&path).unwrap();
    let rewrite = |change: &dyn Fn(&mut End)| {
        let mut changed = end.clone();
        change(&mut changed);
        fs::remove_file(&path).unwrap();
        fs::write(&path, serde_json::to_vec_pretty(&changed).unwrap()).unwrap();
    };
    let refused = |what: &str| {
        let verified = campaign::verify(&fixture.store);
        assert!(
            matches!(verified, Err(CampaignError::Corrupt { .. })),
            "{what}: {verified:?}"
        );
        let resumed = campaign::prepare_resume(&fixture.store, ResumeOptions::default());
        assert!(
            matches!(resumed, Err(CampaignError::Corrupt { .. })),
            "{what}: {resumed:?}"
        );
    };
    rewrite(&|end| end.failures.clear());
    refused("a record without its failures");
    rewrite(&|end| {
        end.snapshot = manifest.initial_snapshot.clone();
        end.added = 0;
    });
    refused("a record with the initial snapshot");
    fs::remove_file(&path).unwrap();
    fs::write(&path, &original).unwrap();
    campaign::verify(&fixture.store).unwrap();

    let prepared = campaign::prepare_resume(&fixture.store, ResumeOptions::default()).unwrap();
    let next = fixture.store.run_dir(prepared.manifest()).join("end.json");
    fs::write(&next, &original).unwrap();
    assert_eq!(
        fixture.store.run_state(prepared.manifest()).unwrap(),
        RunState::Running
    );
}

/// A run that a supervisor holds can't be closed or resumed, and `status`
/// says it runs.
#[test]
fn a_running_run_is_left_alone() {
    let fixture = Fixture::new();
    let child = spawn(fixture.store.root(), &fixture.start_args("60"));
    let run = running(&fixture.store);
    let refused = campaign::close(&fixture.store, &run).unwrap_err();
    assert!(refused.to_string().contains("still going"), "{refused}");
    let refused = campaign::prepare_resume(&fixture.store, ResumeOptions::default()).unwrap_err();
    assert!(refused.to_string().contains("still going"), "{refused}");
    let output = fixture.cli(&["status"]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(": running"), "{stdout}");
    kill(&fixture.store, child, &run);
    let output = fixture.cli(&["close"]);
    assert!(output.status.success(), "{output:?}");
    let end: End = campaign::read_end(&fixture.store, &run).unwrap();
    assert_eq!(end.status, Status::Interrupted);
    assert!(end
        .workers
        .iter()
        .all(|worker| worker.outcome == WorkerOutcome::Unfinished));
}

/// The counts every stand-in process writes: one property applied, with
/// an assumption loss, one never applied, and features in two families
/// and outside them.
fn template_counts() -> Counts {
    let mut applied = PropertyCounts::new(PropertyStatus::Active);
    applied.held = 2;
    applied.evaluations = 5;
    applied.inconclusive.insert("assumption-lost".to_owned(), 1);
    applied.violated = 1;
    let mut unapplied = PropertyCounts::new(PropertyStatus::Draft);
    unapplied.not_applicable = 3;
    Counts {
        runs: 3,
        outcomes: [("clean".to_owned(), 2), ("fail".to_owned(), 1)].into(),
        ends: [("budget".to_owned(), 1), ("idle/quiescent".to_owned(), 2)].into(),
        drains_exhausted: 1,
        cleanup: CleanupCounts {
            abandoned: 1,
            leaked_tasks: 2,
            unwound: 1,
            panicked: 1,
            unwind_failures: 1,
        },
        properties: [
            ("p/applied".to_owned(), applied),
            ("p/unapplied".to_owned(), unapplied),
        ]
        .into(),
        features: [(0, 2), (16384, 1), (65280, 3)].into(),
    }
}

/// [`template_counts`] as a stats file of `harness`, whose process ID,
/// start and completeness the stand-in fills in.
fn stats_template(harness: &str) -> String {
    let stats = ProcessStats {
        version: STATS_VERSION,
        harness: harness.to_owned(),
        pid: 0,
        started_unix: 0,
        written_unix: 0,
        complete: false,
        counts: template_counts(),
    };
    let text = serde_json::to_string(&stats).unwrap();
    text.replace("\"pid\":0", "\"pid\":@PID@")
        .replace("\"started_unix\":0", "\"started_unix\":@STARTED@")
        .replace("\"complete\":false", "\"complete\":@COMPLETE@")
}

/// The properties no complete file saw apply when every file holds
/// [`template_counts`]: the harness's own, which no file names, and
/// `p/unapplied`, plus `p/applied` if `applied` is false.
fn unapplied(applied: bool) -> Vec<String> {
    let entry = harness::find(SIM).unwrap();
    let mut ids: Vec<String> = (entry.properties)()
        .into_iter()
        .map(|property| property.id)
        .chain(["p/unapplied".to_owned()])
        .chain((!applied).then(|| "p/applied".to_owned()))
        .collect();
    ids.sort();
    ids
}

/// Worker `worker`'s attempts log in run `run`.
fn attempts_log(store: &Store, run: &Digest, worker: usize) -> PathBuf {
    store
        .run_dir(run)
        .join("workers")
        .join(worker.to_string())
        .join(ATTEMPTS_FILE)
}

/// Worker `worker`'s logged attempts in run `run`.
fn read_attempts(store: &Store, run: &Digest, worker: usize) -> Vec<campaign::Attempt> {
    fs::read_to_string(attempts_log(store, run, worker))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// Rewrites worker `worker`'s attempts log in run `run` with `attempts`.
fn write_attempts(store: &Store, run: &Digest, worker: usize, attempts: &[campaign::Attempt]) {
    let text: String = attempts
        .iter()
        .map(|attempt| serde_json::to_string(attempt).unwrap() + "\n")
        .collect();
    fs::write(attempts_log(store, run, worker), text).unwrap();
}

/// `counts` added up `n` times.
fn times(counts: &Counts, n: usize) -> Counts {
    let mut sum = Counts::default();
    for _ in 0..n {
        sum.add(counts);
    }
    sum
}

impl Fixture {
    /// Makes the stand-in write [`template_counts`] as its stats file.
    fn write_stats(&self) {
        fs::write(self.control.join("stats"), stats_template(SIM)).unwrap();
    }
}

/// E4b-min: a run's evidence sums each worker's process counts, and the
/// run's, and passes every count through: outcomes, ends, budget exits,
/// cleanup, property applicability and assumption losses, and features.
/// The compaction's merge counts nothing, and `evidence --out` writes the
/// same evidence the library reads.
#[test]
fn evidence_sums_each_workers_processes() {
    let fixture = Fixture::new();
    fixture.write_stats();
    let mut args = fixture.start_args("1");
    args.extend(["--compact-secs", "10"]);
    // A stats directory the supervisor inherits reaches no fuzzer process.
    let inherited = fixture.dir.path().join("inherited");
    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .arg("campaign")
        .args(&args)
        .arg("--store")
        .arg(fixture.store.root())
        .env(STATS_VAR, &inherited)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let run = leaf(&fixture.store);
    let end = campaign::read_end(&fixture.store, &run).unwrap();
    assert!(matches!(end.compaction, Some(Compaction::Compacted { .. })));
    assert!(
        !fixture.control.join("merge-had-stats").exists(),
        "the compaction's merge gets no stats directory"
    );
    assert!(!inherited.exists());

    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    assert_eq!(evidence.run, run);
    assert_eq!(evidence.harness, SIM);
    assert_eq!(evidence.status, Some(Status::Complete));
    let template = template_counts();
    assert_eq!(evidence.workers.len(), 2);
    for (index, worker) in evidence.workers.iter().enumerate() {
        assert_eq!(worker.index, index);
        assert_eq!(worker.outcome, Some(WorkerOutcome::BudgetSpent));
        assert_eq!(worker.attempts, [(AttemptEnd::Finished, 1)].into());
        assert_eq!(worker.processes, 1);
        assert_eq!(worker.counts, template);
    }
    let doubled = times(&template, 2);
    assert_eq!(evidence.totals, doubled);
    assert_eq!(evidence.complete_totals, doubled);
    assert_eq!(evidence.unapplied, unapplied(true));
    assert_eq!(evidence.incomplete, []);

    let totals = &evidence.totals;
    assert_eq!(totals.runs, 6);
    assert_eq!(totals.outcomes["clean"], 4);
    assert_eq!(totals.outcomes["fail"], 2);
    assert_eq!(totals.ends["idle/quiescent"], 4);
    assert_eq!(totals.budget_exits(), 2);
    assert_eq!(totals.drains_exhausted, 2);
    assert_eq!(totals.cleanup.abandoned, 2);
    assert_eq!(totals.cleanup.leaked_tasks, 4);
    assert_eq!(totals.cleanup.unwound, 2);
    assert_eq!(totals.cleanup.panicked, 2);
    assert_eq!(totals.cleanup.unwind_failures, 2);
    let applied = &totals.properties["p/applied"];
    assert_eq!((applied.held, applied.evaluations), (4, 10));
    assert_eq!(applied.assumption_losses(), 2);
    assert_eq!(applied.violated, 2);
    assert_eq!(applied.status, PropertyStatus::Active);
    assert_eq!(totals.properties["p/unapplied"].not_applicable, 6);
    assert_eq!(
        totals.properties["p/unapplied"].status,
        PropertyStatus::Draft
    );
    assert_eq!(totals.features, [(0, 4), (16384, 2), (65280, 6)].into());
    assert_eq!(
        totals.feature_totals(),
        (
            3,
            [
                ("feature/budget", 1),
                ("feature/request-reset", 1),
                ("other", 1)
            ]
            .into()
        )
    );

    let out = fixture.dir.path().join("evidence.json");
    let output = fixture.cli(&["evidence", "--out", out.to_str().unwrap()]);
    assert!(output.status.success(), "{output:?}");
    let written: serde_json::Value = serde_json::from_slice(&fs::read(&out).unwrap()).unwrap();
    assert_eq!(written, serde_json::to_value(&evidence).unwrap());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let unapplied_line = format!("unapplied: {}", unapplied(true).join(", "));
    for line in [
        "worker 0: budget-spent; processes: finished 1; stats files: 1; 3 runs (clean 2, fail 1)",
        "total: 6 runs (clean 4, fail 2)",
        "budget exits: 2; drains exhausted: 2",
        "cleanup: abandoned 2 (4 tasks leaked), unwound 2, panicked 2, unwind failures 2",
        "features: 3 distinct (feature/budget 1, feature/request-reset 1, other 1)",
        "property p/applied [active]: held 4 (10 evaluations), not applicable 0, inconclusive 2 \
         (assumption-lost 2), assumption losses 2, violated 2",
        &unapplied_line,
    ] {
        assert!(stdout.lines().any(|got| got == line), "{line}\n{stdout}");
    }
}

/// E4b-min: the evidence lists every way a run's counts fall short. A
/// crashed process's file is partial and stays out of the complete totals.
/// A stats file the attempts log doesn't list, a process that left no
/// file, an unreadable file and another harness's file are each listed,
/// and only the complete files decide which properties applied.
#[test]
fn evidence_lists_incomplete_processes() {
    let fixture = Fixture::new();
    fixture.write_stats();
    fixture.set("crash");
    let (run, end) = start(&fixture.store, fixture.options(1, 2));
    assert_eq!(end.status, Status::Complete, "{end:?}");
    let attempts = read_attempts(&fixture.store, &run, 0);
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    assert_eq!(attempts[0].end, AttemptEnd::Failure);
    let crashed = attempts[0].pid.unwrap();
    let finished = attempts[1].pid.unwrap();

    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    let template = template_counts();
    assert_eq!(
        evidence.incomplete,
        [Incomplete::PartialStats {
            worker: 0,
            pid: crashed
        }]
    );
    let worker = &evidence.workers[0];
    assert_eq!(
        worker.attempts,
        [(AttemptEnd::Finished, 1), (AttemptEnd::Failure, 1)].into()
    );
    assert_eq!(worker.processes, 2);
    assert_eq!(evidence.totals, times(&template, 2));
    assert_eq!(evidence.complete_totals, template);

    let stats = fixture.store.run_dir(&run).join("workers/0/stats");
    let stray = 999_999;
    fs::write(
        stats.join(format!("stats-{stray}.json")),
        stats_template(SIM)
            .replace("@PID@", &stray.to_string())
            .replace("@STARTED@", "0")
            .replace("@COMPLETE@", "true"),
    )
    .unwrap();
    fs::write(stats.join("stats-7.json"), b"{").unwrap();
    fs::write(
        stats.join("stats-8.json"),
        stats_template("other/harness")
            .replace("@PID@", "8")
            .replace("@STARTED@", "0")
            .replace("@COMPLETE@", "true"),
    )
    .unwrap();
    fs::remove_file(stats.join(format!("stats-{finished}.json"))).unwrap();
    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    let mut reasons = evidence.incomplete.iter();
    assert_eq!(
        reasons.next(),
        Some(&Incomplete::MissingStats {
            worker: 0,
            attempt: 1,
            pid: Some(finished)
        })
    );
    assert_eq!(
        reasons.next(),
        Some(&Incomplete::PartialStats {
            worker: 0,
            pid: crashed
        })
    );
    assert_eq!(
        reasons.next(),
        Some(&Incomplete::UnloggedProcess {
            worker: 0,
            pid: stray
        })
    );
    let Some(Incomplete::UnreadableStats { file, error, .. }) = reasons.next() else {
        panic!("{:?}", evidence.incomplete);
    };
    assert_eq!(file, "stats-7.json");
    assert!(error.contains("parsing"), "{error}");
    let Some(Incomplete::UnreadableStats { file, error, .. }) = reasons.next() else {
        panic!("{:?}", evidence.incomplete);
    };
    assert_eq!(file, "stats-8.json");
    assert!(error.contains("other/harness"), "{error}");
    assert_eq!(reasons.next(), None);
    assert_eq!(evidence.totals, times(&template, 2));
    assert_eq!(
        evidence.complete_totals,
        Counts::default(),
        "the stray file is complete, but no logged process wrote it"
    );
    // Without a complete file a logged process wrote, no property applied.
    assert_eq!(evidence.unapplied, unapplied(false));
}

/// E4b-min: a stats file counts only for the logged process that wrote
/// it. A process ID an earlier attempt also had, and a stale copy from
/// before the attempt started, leave that attempt's file missing and the
/// file unlogged. A file named like a stats file, but not as a process
/// writes one, is unreadable rather than a second file for the same
/// process.
#[test]
fn evidence_matches_each_file_to_the_process_that_wrote_it() {
    let fixture = Fixture::new();
    fixture.write_stats();
    fixture.set("crash");
    let (run, end) = start(&fixture.store, fixture.options(1, 2));
    assert_eq!(end.status, Status::Complete, "{end:?}");
    let mut attempts = read_attempts(&fixture.store, &run, 0);
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    let stats = fixture.store.run_dir(&run).join("workers/0/stats");
    let (crashed, finished) = (attempts[0].pid.unwrap(), attempts[1].pid.unwrap());
    let template = template_counts();

    // The finished process reused the crashed one's ID, and overwrote its
    // file.
    fs::rename(
        stats.join(format!("stats-{finished}.json")),
        stats.join(format!("stats-{crashed}.json")),
    )
    .unwrap();
    let text = fs::read_to_string(stats.join(format!("stats-{crashed}.json"))).unwrap();
    fs::write(
        stats.join(format!("stats-{crashed}.json")),
        text.replace(
            &format!("\"pid\":{finished}"),
            &format!("\"pid\":{crashed}"),
        ),
    )
    .unwrap();
    attempts[1].pid = Some(crashed);
    write_attempts(&fixture.store, &run, 0, &attempts);
    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    assert_eq!(
        evidence.incomplete,
        [Incomplete::MissingStats {
            worker: 0,
            attempt: 0,
            pid: Some(crashed)
        }]
    );
    assert_eq!(evidence.workers[0].processes, 1);
    assert_eq!(evidence.totals, template);
    assert_eq!(evidence.complete_totals, template);

    // A stale copy: its process started before either attempt did.
    for attempt in &mut attempts {
        attempt.started_unix += 3600;
    }
    write_attempts(&fixture.store, &run, 0, &attempts);
    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    assert_eq!(
        evidence.incomplete,
        [
            Incomplete::MissingStats {
                worker: 0,
                attempt: 0,
                pid: Some(crashed)
            },
            Incomplete::MissingStats {
                worker: 0,
                attempt: 1,
                pid: Some(crashed)
            },
            Incomplete::UnloggedProcess {
                worker: 0,
                pid: crashed
            },
        ]
    );
    assert_eq!(evidence.totals, template);
    assert_eq!(evidence.complete_totals, Counts::default());
    assert_eq!(evidence.unapplied, unapplied(false));

    // A second name for the same process.
    for attempt in &mut attempts {
        attempt.started_unix -= 3600;
    }
    write_attempts(&fixture.store, &run, 0, &attempts);
    fs::copy(
        stats.join(format!("stats-{crashed}.json")),
        stats.join(format!("stats-0{crashed}.json")),
    )
    .unwrap();
    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    let Some(Incomplete::UnreadableStats { file, error, .. }) = evidence.incomplete.last() else {
        panic!("{:?}", evidence.incomplete);
    };
    assert_eq!(file, &format!("stats-0{crashed}.json"));
    assert!(error.contains("stats-<pid>.json"), "{error}");
    assert_eq!(evidence.incomplete.len(), 2, "{:?}", evidence.incomplete);
    assert_eq!(evidence.totals, template);
}

/// E4b-min: a complete stats file counts only if its process exited
/// cleanly: with code 0 and no failure in its output. A process that marks
/// its file complete and then exits 1, or exits 0 after a sanitizer
/// report, lists its attempt as an unclean exit, and its file stays out of
/// the complete totals.
#[test]
fn evidence_counts_a_complete_file_only_after_a_clean_exit() {
    let fixture = Fixture::new();
    fixture.write_stats();
    fixture.set("fail-after-stats");
    let (run, end) = start(&fixture.store, fixture.options(1, 1));
    assert_eq!(end.status, Status::Complete, "{end:?}");
    let mut attempts = read_attempts(&fixture.store, &run, 0);
    assert_eq!(attempts.len(), 1, "{attempts:?}");
    assert_eq!(attempts[0].code, Some(1));
    assert_eq!(attempts[0].end, AttemptEnd::Failure);
    let pid = attempts[0].pid.unwrap();
    let file = fixture
        .store
        .run_dir(&run)
        .join(format!("workers/0/stats/stats-{pid}.json"));
    assert!(ProcessStats::read(&file).unwrap().complete);
    let template = template_counts();
    let unclean = [Incomplete::UncleanExit {
        worker: 0,
        attempt: 0,
        pid,
    }];

    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    assert_eq!(evidence.incomplete, unclean);
    assert_eq!(evidence.totals, template);
    assert_eq!(evidence.complete_totals, Counts::default());
    assert_eq!(evidence.unapplied, unapplied(false));

    // Exit code 0 with a failure in the output is still unclean.
    attempts[0].code = Some(0);
    attempts[0].failure = Some(ProcessFailure::Sanitizer {
        report: "==1==ERROR: AddressSanitizer: heap-use-after-free".to_owned(),
    });
    write_attempts(&fixture.store, &run, 0, &attempts);
    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    assert_eq!(evidence.incomplete, unclean);
    assert_eq!(evidence.complete_totals, Counts::default());

    attempts[0].failure = None;
    attempts[0].end = AttemptEnd::Finished;
    write_attempts(&fixture.store, &run, 0, &attempts);
    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    assert_eq!(evidence.incomplete, []);
    assert_eq!(evidence.complete_totals, template);
    assert_eq!(evidence.unapplied, unapplied(true));
}

/// E4b-min: a run whose processes wrote no stats files, and an attempts
/// log written before attempts recorded process IDs, list every attempt as
/// missing its stats, and leave every one of the harness's properties
/// unapplied.
#[test]
fn evidence_without_stats_files_leaves_every_property_unapplied() {
    let fixture = Fixture::new();
    let (run, end) = start(&fixture.store, fixture.options(1, 1));
    assert_eq!(end.status, Status::Complete, "{end:?}");
    let mut attempts = read_attempts(&fixture.store, &run, 0);
    let pid = attempts[0].pid;
    assert!(pid.is_some());
    let missing = [Incomplete::MissingStats {
        worker: 0,
        attempt: 0,
        pid,
    }];
    let entry = harness::find(SIM).unwrap();
    let mut harness_ids: Vec<String> = (entry.properties)()
        .into_iter()
        .map(|property| property.id)
        .collect();
    harness_ids.sort();
    assert!(!harness_ids.is_empty());
    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    assert_eq!(evidence.incomplete, missing);
    assert_eq!(evidence.totals, Counts::default());
    assert_eq!(evidence.unapplied, harness_ids);

    attempts[0].pid = None;
    write_attempts(&fixture.store, &run, 0, &attempts);
    let line = fs::read_to_string(attempts_log(&fixture.store, &run, 0)).unwrap();
    let line = line.replace(",\"pid\":null", "");
    assert!(!line.contains("pid"), "{line}");
    fs::write(attempts_log(&fixture.store, &run, 0), line).unwrap();
    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    assert_eq!(
        evidence.incomplete,
        [Incomplete::MissingStats {
            worker: 0,
            attempt: 0,
            pid: None
        }]
    );
    assert_eq!(evidence.unapplied, harness_ids);
}

/// E4b-min: a worker that stops at its restart limit, and the workers of a
/// run whose supervisor was killed, are incomplete, before and after
/// `close`.
#[test]
fn evidence_lists_unfinished_workers() {
    let fixture = Fixture::new();
    fixture.write_stats();
    fixture.set("always-crash");
    let (run, end) = start(&fixture.store, fixture.options(1, 60));
    assert_eq!(end.status, Status::WorkerStopped, "{end:?}");
    let evidence = campaign::evidence(&fixture.store, &run).unwrap();
    assert_eq!(
        evidence.workers[0].outcome,
        Some(WorkerOutcome::RestartLimit)
    );
    assert_eq!(
        evidence.workers[0].attempts,
        [(AttemptEnd::Failure, 3)].into()
    );
    assert_eq!(
        evidence.incomplete[0],
        Incomplete::RestartLimit { worker: 0 }
    );
    assert_eq!(evidence.incomplete.len(), 4, "{:?}", evidence.incomplete);
    assert!(evidence.incomplete[1..]
        .iter()
        .all(|reason| matches!(reason, Incomplete::PartialStats { worker: 0, .. })));
    assert_eq!(evidence.complete_totals, Counts::default());

    let fixture = Fixture::new();
    fixture.write_stats();
    let run = start_and_kill(&fixture.store, &fixture.start_args("60"), || true);
    for closed in [false, true] {
        if closed {
            campaign::close(&fixture.store, &run).unwrap();
        }
        let evidence = campaign::evidence(&fixture.store, &run).unwrap();
        let expected = closed.then_some(Status::Interrupted);
        assert_eq!(evidence.status, expected);
        let outcome = closed.then_some(WorkerOutcome::Unfinished);
        assert!(evidence
            .workers
            .iter()
            .all(|worker| worker.outcome == outcome));
        let unfinished: Vec<&Incomplete> = evidence
            .incomplete
            .iter()
            .filter(|reason| matches!(reason, Incomplete::Unfinished { .. }))
            .collect();
        assert_eq!(
            unfinished,
            [
                &Incomplete::Unfinished { worker: 0 },
                &Incomplete::Unfinished { worker: 1 }
            ]
        );
        assert!(evidence
            .incomplete
            .iter()
            .any(|reason| matches!(reason, Incomplete::PartialStats { .. })));
    }
}
