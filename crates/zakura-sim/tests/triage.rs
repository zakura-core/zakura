//! E4a: `triage` confirms a saved input's failure in fresh processes,
//! classifies each through `classify_process`, and archives the input with
//! its evidence in a bundle that never changes.
#![cfg(zakura_sim)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::OnceLock,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::TempDir;
use zakura_rt::{sync::atomic::AtomicU64, task::yield_now};
use zakura_scheduler::SchedulerConfig;
use zakura_sim::{
    explore::{
        classify,
        triage::{
            bundles, primary_bucket, provenance_hash, signature, signature_hash, triage,
            verify_bundle, Children, TriageOptions, TriageStatus, Triaged, BUNDLE_INPUT_FILE,
            CHECKSUMS_FILE, RUNS_DIR, RUN_TRACE_DIR, SCENARIO_FILE, STAGING_DIR, TRIAGE_FILE,
        },
        Bucket, Classified, ExploreOutcome, ProcessClassified, ProcessExit, ProcessFailure,
        ProcessResult,
    },
    harness::{entry, record, HarnessEntry, RecordInput, Subsystem},
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Verdict},
    trace::{meta::BuildInfo, replay_with, ReplayOptions, TraceReader, INPUT_FILE},
    HarnessFailure, NullSink, RunConfig, Sim,
};

/// The [`Triage`] scenario: which case to run.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct Case {
    case: u8,
}

/// Yields a few times and exits.
const CLEAN: u8 = 0;
/// [`ACTIVE`] is violated at every step.
const ACTIVE_CASE: u8 = 1;
/// [`KNOWN`] is violated, and the run goes on.
const KNOWN_CASE: u8 = 2;
/// A task's name holds `:`, a harness failure.
const HARNESS_CASE: u8 = 3;
/// The task aborts the process at its second step.
const ABORT_CASE: u8 = 4;
/// The task blocks its thread for an hour.
const HANG_CASE: u8 = 5;
/// [`ACTIVE`] is violated in the first process that runs the input only.
const FIRST_ONLY_CASE: u8 = 6;
/// [`ACTIVE`] is violated in the first two processes that run the input:
/// both recordings, but not the replay.
const RECORDS_ONLY_CASE: u8 = 7;
/// The task exits the process with code 0 at its second step.
const EXIT_CASE: u8 = 8;
/// A task spins in cleanup, so shutdown abandons the run.
const ABANDON_CASE: u8 = 9;
/// [`ACTIVE`] is violated in every process, but its evidence differs in
/// the second process that runs the input.
const SECOND_DIFFERS_CASE: u8 = 10;
/// The task aborts the process at its second step, as in [`ABORT_CASE`].
/// The second process that runs the input deletes its trace's `input.bin`
/// first, so that trace doesn't read.
const SECOND_UNREADABLE_CASE: u8 = 11;

const ACTIVE: &str = "test/triage/active";
const KNOWN: &str = "test/triage/known";

/// The variable that names the file counting the processes that ran
/// [`FIRST_ONLY_CASE`], [`RECORDS_ONLY_CASE`], [`SECOND_DIFFERS_CASE`] or
/// [`SECOND_UNREADABLE_CASE`]: one byte per process.
const COUNTER: &str = "ZAKURA_SIM_TRIAGE_COUNTER";

/// A one-task harness whose first scenario byte picks a case.
struct Triage {
    case: u8,
    /// True if [`ACTIVE`] is violated in this run.
    active: bool,
    /// True if this is the second process that ran [`SECOND_DIFFERS_CASE`].
    second: bool,
}

impl HasView for Triage {
    type View = (u8, bool, bool);

    /// The case, whether [`ACTIVE`] is violated, and whether this is the
    /// second process that ran [`SECOND_DIFFERS_CASE`].
    fn view(&self) -> (u8, bool, bool) {
        (self.case, self.active, self.second)
    }
}

fn active_check(cx: &mut CheckCtx<'_, Triage>) -> Verdict {
    match cx.view() {
        (case, true, false) => cx.violated("no run", format!("case {case}")),
        (case, true, true) => cx.violated("no run", format!("case {case}, second process")),
        _ => Verdict::Holds,
    }
}

fn known_check(cx: &mut CheckCtx<'_, Triage>) -> Verdict {
    match cx.view() {
        (KNOWN_CASE, _, _) => cx.violated("no run", "the known case".to_owned()),
        _ => Verdict::Holds,
    }
}

/// A property with `id` and `status`.
fn property(
    id: &'static str,
    status: Status,
    check: fn(&mut CheckCtx<'_, Triage>) -> Verdict,
) -> Property<Triage> {
    Property {
        id: PropertyId::new(id),
        claim: "no run violates it",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Schedule],
        status,
        check,
        witness: None,
    }
}

/// How many processes ran a counted case before this one, counting this
/// one for the next.
fn count_process() -> usize {
    let path = std::env::var(COUNTER).expect("a counted case runs with its counter");
    let before = fs::read(&path).map_or(0, |bytes| bytes.len());
    let mut file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .unwrap();
    std::io::Write::write_all(&mut file, b".").unwrap();
    before
}

impl Subsystem for Triage {
    const NAME: &'static str = "test/triage";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 1;
    type Scenario = Case;
    type Prepared = ();

    /// One worker, small stacks, and short limits.
    fn config(_: &Case) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 1,
                stack_size: 64 * 1024,
                ..SchedulerConfig::default()
            },
            budget: 50,
            drain_bound: 10,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &Case) {}

    /// Spawns the case's task.
    fn build(sim: &mut Sim, scenario: &Case, (): ()) -> Self {
        let case = scenario.case;
        let (active, second) = match case {
            ACTIVE_CASE => (true, false),
            FIRST_ONLY_CASE => (count_process() == 0, false),
            RECORDS_ONLY_CASE => (count_process() < 2, false),
            SECOND_DIFFERS_CASE => (true, count_process() == 1),
            SECOND_UNREADABLE_CASE => {
                if count_process() == 1 {
                    let trace = RECORD_DIR.get().expect("the case runs in a recording");
                    fs::remove_file(trace.join(INPUT_FILE)).unwrap();
                }
                (false, false)
            }
            _ => (false, false),
        };
        let scheduler = sim.scheduler();
        match case {
            HARNESS_CASE => scheduler.spawn_root("bad:name", async {}),
            ABORT_CASE | SECOND_UNREADABLE_CASE => scheduler.spawn_root("tick", async {
                yield_now().await;
                std::process::abort();
            }),
            HANG_CASE => scheduler.spawn_root("tick", async {
                std::thread::sleep(Duration::from_secs(3600));
            }),
            EXIT_CASE => scheduler.spawn_root("tick", async {
                yield_now().await;
                std::process::exit(0);
            }),
            ABANDON_CASE => scheduler.spawn_root("spinner", async {
                // Nothing ever stores, and every load is a point, so
                // cleanup gets stuck and shutdown abandons the run.
                let flag = AtomicU64::new(0);
                while flag.load(zakura_rt::sync::atomic::Ordering::SeqCst) == 0 {}
            }),
            _ => scheduler.spawn_root("tick", async {
                for _ in 0..3 {
                    yield_now().await;
                }
            }),
        };
        Triage {
            case,
            active,
            second,
        }
    }

    /// One `Active` and one `Known` property.
    fn properties() -> Vec<Property<Self>> {
        vec![
            property(ACTIVE, Status::Active, active_check),
            property(KNOWN, Status::Known { issue: 1 }, known_check),
        ]
    }

    /// The first byte is the case.
    fn draw(prefix: &[u8]) -> Case {
        Case { case: prefix[0] }
    }
}

static TRIAGE: HarnessEntry = entry::<Triage>();

/// [`TRIAGE`] with the next generator.
static NEXT_GENERATOR: HarnessEntry = HarnessEntry {
    generator: Triage::GENERATOR + 1,
    ..entry::<Triage>()
};

/// The variable that makes [`triage_child`] record or replay.
const CHILD: &str = "ZAKURA_SIM_TRIAGE_CHILD";

/// The trace directory [`triage_child`] records to.
static RECORD_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Records or replays when run as a child, and does nothing otherwise.
///
/// - `record:<input>:<dir>` records the input in the file `<input>` to
///   `<dir>`, as `zakura-sim record --input` does.
/// - `replay:<dir>` replays the trace in `<dir>` Exact, and fails unless
///   it verified, as `zakura-sim replay` does.
#[test]
fn triage_child() {
    let Ok(mode) = std::env::var(CHILD) else {
        return;
    };
    let parts: Vec<&str> = mode.splitn(3, ':').collect();
    match parts[..] {
        ["record", input, dir] => {
            let input = fs::read(input).unwrap();
            RECORD_DIR.set(PathBuf::from(dir)).unwrap();
            record(&TRIAGE, RecordInput::Input(&input), Path::new(dir)).unwrap();
        }
        ["replay", dir] => {
            let trace = TraceReader::open(Path::new(dir)).unwrap();
            let report = replay_with(&trace, &TRIAGE, ReplayOptions::default()).unwrap();
            assert!(report.verified(), "{report:?}");
        }
        _ => panic!("unknown {CHILD} mode {mode}"),
    }
}

/// This test binary as [`triage_child`], without core dumps, with the
/// counter file `counter`.
struct TestChildren {
    counter: PathBuf,
}

impl TestChildren {
    fn command(&self, mode: String) -> Command {
        let mut command = Command::new("sh");
        command
            .args(["-c", "ulimit -c 0; exec \"$0\" \"$@\""])
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", "triage_child", "--nocapture", "--test-threads=1"])
            .env(CHILD, mode)
            .env(COUNTER, &self.counter);
        command
    }
}

impl Children for TestChildren {
    fn record(&self, input: &Path, out: &Path) -> Command {
        self.command(format!("record:{}:{}", input.display(), out.display()))
    }

    fn replay(&self, trace: &Path) -> Command {
        self.command(format!("replay:{}", trace.display()))
    }
}

/// A scratch directory with an archive under `archive/`.
struct Scratch {
    dir: TempDir,
}

impl Scratch {
    fn new() -> Self {
        Scratch {
            dir: TempDir::new().unwrap(),
        }
    }

    fn out(&self) -> PathBuf {
        self.dir.path().join("archive")
    }

    /// Where this build's bundles on `entry`'s harness go.
    fn root_of(&self, entry: &HarnessEntry) -> PathBuf {
        entry
            .name
            .split('/')
            .fold(self.out(), |dir, segment| dir.join(segment))
            .join(provenance_hash(entry, &BuildInfo::current()))
    }

    /// Where this build's bundles on [`TRIAGE`] go.
    fn root(&self) -> PathBuf {
        self.root_of(&TRIAGE)
    }

    /// Writes `input` to a file named `name`, as libFuzzer saves a crash.
    fn artifact(&self, name: &str, input: &[u8]) -> PathBuf {
        let path = self.dir.path().join(name);
        fs::write(&path, input).unwrap();
        path
    }

    /// Triages `input`, saved as `name`, with a counter of its own and
    /// `timeout` per child.
    fn triage(&self, name: &str, input: &[u8], timeout: Duration) -> Triaged {
        self.triage_on(&TRIAGE, name, input, timeout)
    }

    /// [`Scratch::triage`] on `entry`'s harness. The children always run
    /// [`TRIAGE`].
    fn triage_on(
        &self,
        entry: &HarnessEntry,
        name: &str,
        input: &[u8],
        timeout: Duration,
    ) -> Triaged {
        let artifact = self.artifact(name, input);
        let children = TestChildren {
            counter: self.dir.path().join(format!("{name}.count")),
        };
        let options = TriageOptions {
            child_timeout: timeout,
        };
        triage(entry, &children, &artifact, &self.out(), options).unwrap()
    }
}

/// Long enough for any case but [`HANG_CASE`] on a loaded machine.
const TIMEOUT: Duration = Duration::from_secs(60);

/// The SHA-256 of `bytes`, in lowercase hex.
fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The bundle's `triage.json`.
fn report(bundle: &Path) -> Value {
    serde_json::from_slice(&fs::read(bundle.join(TRIAGE_FILE)).unwrap()).unwrap()
}

/// The first recording's trace directory in `bundle`.
fn first_trace(bundle: &Path) -> PathBuf {
    bundle.join(RUNS_DIR).join("record-1").join(RUN_TRACE_DIR)
}

/// E4.1: an injected `Active` failure becomes a bundle that holds the
/// input and its trace. A fresh process replayed the trace Exact, and the
/// trace's run classifies as the in-process run does. Replaying the
/// bundle's trace again reproduces its `end` record.
#[test]
fn injected_failure_bundle_replays_exact() {
    let scratch = Scratch::new();
    let input = [ACTIVE_CASE, 4, 2];
    let triaged = scratch.triage("crash-active", &input, TIMEOUT);
    assert_eq!(triaged.status, TriageStatus::Confirmed, "{triaged:?}");
    assert_eq!(triaged.bucket, Some(Bucket::Active));
    let expected = format!("active {ACTIVE}");
    assert_eq!(triaged.signature.as_deref(), Some(expected.as_str()));
    assert_eq!(triaged.replay, Some(ProcessExit::Code(0)));
    assert!(triaged.archived);
    assert_eq!(
        triaged.bundle,
        scratch
            .root()
            .join("active")
            .join(signature_hash(&expected))
            .join(sha256(&input))
    );

    // The fresh process classifies as the in-process run does.
    let run = (TRIAGE.run_bytes)(&input, &mut NullSink, None);
    let in_process = classify(&TRIAGE, &run);
    for child in &triaged.records {
        assert_eq!(child.exit, ProcessExit::Code(0));
        assert_eq!(
            child.classified.result,
            ProcessResult::Ended(in_process.clone())
        );
    }

    let bundle = &triaged.bundle;
    assert_eq!(fs::read(bundle.join(BUNDLE_INPUT_FILE)).unwrap(), input);
    let scenario: Value =
        serde_json::from_slice(&fs::read(bundle.join(SCENARIO_FILE)).unwrap()).unwrap();
    assert_eq!(scenario, serde_json::json!({ "case": ACTIVE_CASE }));
    assert_eq!(verify_bundle(bundle).unwrap(), Vec::<String>::new());
    let report = report(bundle);
    assert_eq!(report["status"], "confirmed");
    assert_eq!(report["bucket"], "active");
    assert_eq!(report["signature"], expected.as_str());
    assert_eq!(report["input"]["sha256"], sha256(&input).as_str());
    assert_eq!(report["replay"]["verified"], true);
    assert_eq!(report["records"][0]["class"]["result"], "ended");
    assert_eq!(report["records"][1]["class"]["outcome"]["property"], ACTIVE);

    let trace = TraceReader::open(&first_trace(bundle)).unwrap();
    let replayed = replay_with(&trace, &TRIAGE, ReplayOptions::default()).unwrap();
    assert!(replayed.verified());
    assert_eq!(replayed.end, trace.end);
    assert!(
        fs::read_dir(scratch.out().join(STAGING_DIR))
            .unwrap()
            .next()
            .is_none(),
        "the bundle left staging"
    );
}

/// Each distinct input of one signature keeps its own bundle under that
/// signature, so the signature counts both. Triaging an archived input
/// again keeps the bundle that's there, unchanged.
#[test]
fn each_input_of_a_signature_keeps_its_bundle() {
    let scratch = Scratch::new();
    let first = scratch.triage("crash-1", &[ACTIVE_CASE], TIMEOUT);
    let second = scratch.triage("crash-2", &[ACTIVE_CASE, 1, 2, 3], TIMEOUT);
    assert_eq!(first.status, TriageStatus::Confirmed);
    assert_eq!(second.status, TriageStatus::Confirmed);
    assert_eq!(first.signature, second.signature);
    assert_ne!(first.bundle, second.bundle);
    assert_eq!(first.bundle.parent(), second.bundle.parent());
    let mut both = vec![first.bundle.clone(), second.bundle.clone()];
    both.sort();
    assert_eq!(bundles(&scratch.out()).unwrap(), both);

    let sums = fs::read(first.bundle.join(CHECKSUMS_FILE)).unwrap();
    let again = scratch.triage("crash-1-again", &[ACTIVE_CASE], TIMEOUT);
    assert_eq!(again.status, TriageStatus::Confirmed);
    assert_eq!(again.bundle, first.bundle);
    assert!(!again.archived);
    assert_eq!(fs::read(first.bundle.join(CHECKSUMS_FILE)).unwrap(), sums);
    assert_eq!(
        report(&first.bundle)["artifact"],
        scratch.dir.path().join("crash-1").display().to_string()
    );
    assert_eq!(bundles(&scratch.out()).unwrap(), both);
    assert!(fs::read_dir(scratch.out().join(STAGING_DIR))
        .unwrap()
        .next()
        .is_none());
}

/// The same bytes on the same harness after a generator change are another
/// run, so each status keeps their bundle beside the first one's. Triaging
/// them again on that generator keeps the bundle that's there.
#[test]
fn a_changed_generator_keeps_its_own_bundle() {
    let scratch = Scratch::new();
    let rows = [
        (CLEAN, TriageStatus::NotReproduced),
        (ACTIVE_CASE, TriageStatus::Confirmed),
        (ABORT_CASE, TriageStatus::Confirmed),
        (FIRST_ONLY_CASE, TriageStatus::Nondeterministic),
    ];
    let mut all = Vec::new();
    for (case, status) in rows {
        let input = [case];
        let first = scratch.triage_on(&TRIAGE, &format!("{case}-first"), &input, TIMEOUT);
        let next = scratch.triage_on(&NEXT_GENERATOR, &format!("{case}-next"), &input, TIMEOUT);
        for (triaged, entry) in [(&first, &TRIAGE), (&next, &NEXT_GENERATOR)] {
            assert_eq!(triaged.status, status, "case {case}: {triaged:?}");
            assert!(triaged.archived, "case {case}: {triaged:?}");
            assert!(
                triaged.bundle.starts_with(scratch.root_of(entry)),
                "case {case}: {triaged:?}"
            );
            assert_eq!(
                verify_bundle(&triaged.bundle).unwrap(),
                Vec::<String>::new()
            );
            assert_eq!(report(&triaged.bundle)["generator"], entry.generator);
        }
        assert_ne!(first.bundle, next.bundle, "case {case}");

        let again = scratch.triage_on(&NEXT_GENERATOR, &format!("{case}-again"), &input, TIMEOUT);
        assert_eq!(again.bundle, next.bundle, "case {case}");
        assert!(!again.archived, "case {case}");
        all.extend([first.bundle, next.bundle]);
    }
    all.sort();
    assert_eq!(bundles(&scratch.out()).unwrap(), all);
}

/// A bundle's provenance changes with the generator and with the build. Two
/// clean builds of one source share it, whatever their executables; two
/// dirty builds share it only with the same executable.
#[test]
fn provenance_names_the_generator_and_build() {
    let clean = BuildInfo {
        git: "1".repeat(40),
        dirty: false,
        source: "2".repeat(64),
        lockfile: "3".repeat(64),
        rustc: "rustc 1.98.0".to_owned(),
        binary: "4".repeat(64),
        ..BuildInfo::current()
    };
    let hash = |entry: &HarnessEntry, build: &BuildInfo| provenance_hash(entry, build);
    let base = hash(&TRIAGE, &clean);
    assert_ne!(hash(&NEXT_GENERATOR, &clean), base);
    let other_commit = BuildInfo {
        git: "5".repeat(40),
        ..clean.clone()
    };
    assert_ne!(hash(&TRIAGE, &other_commit), base);
    let other_binary = BuildInfo {
        binary: "6".repeat(64),
        ..clean.clone()
    };
    assert_eq!(hash(&TRIAGE, &other_binary), base);
    let dirty = BuildInfo {
        dirty: true,
        ..clean.clone()
    };
    let dirty_other_binary = BuildInfo {
        binary: "6".repeat(64),
        ..dirty.clone()
    };
    assert_ne!(hash(&TRIAGE, &dirty), base);
    assert_ne!(hash(&TRIAGE, &dirty_other_binary), hash(&TRIAGE, &dirty));
}

/// A row of [`buckets_for_harness_crash_and_nondeterminism`]: its name, the
/// case, and the status, bucket and signature triage gives it.
type Row<'a> = (&'a str, u8, TriageStatus, Option<Bucket>, Option<&'a str>);

/// E4.4: every class goes through `classify_process`, and each input lands
/// where its status and bucket say. A process crash is `crash`, a run whose
/// recordings disagree is `nondeterministic`, and a run whose replay
/// disagrees is `replay-failed`. Neither a clean run nor a run without its
/// `end` record is a pass.
#[test]
fn buckets_for_harness_crash_and_nondeterminism() {
    let scratch = Scratch::new();
    let known = format!("known {KNOWN}");
    let harness = format!(
        "harness {}",
        HarnessFailure::TaskNameHasColon {
            name: "bad:name.1".to_owned(),
        }
    );
    let rows: [Row<'_>; 8] = [
        (
            "known",
            KNOWN_CASE,
            TriageStatus::Confirmed,
            Some(Bucket::Known),
            Some(&known),
        ),
        (
            "harness",
            HARNESS_CASE,
            TriageStatus::Confirmed,
            Some(Bucket::Harness),
            Some(&harness),
        ),
        (
            "abort",
            ABORT_CASE,
            TriageStatus::Confirmed,
            Some(Bucket::Crash),
            Some("crash abort"),
        ),
        (
            "no-end",
            EXIT_CASE,
            TriageStatus::Confirmed,
            Some(Bucket::NoEnd),
            Some("no-end"),
        ),
        (
            "abandoned",
            ABANDON_CASE,
            TriageStatus::Confirmed,
            Some(Bucket::Abandoned),
            Some("abandoned"),
        ),
        ("clean", CLEAN, TriageStatus::NotReproduced, None, None),
        (
            "first-only",
            FIRST_ONLY_CASE,
            TriageStatus::Nondeterministic,
            None,
            None,
        ),
        (
            "records-only",
            RECORDS_ONLY_CASE,
            TriageStatus::ReplayFailed,
            None,
            None,
        ),
    ];
    for (name, case, status, bucket, expected) in rows {
        let input = [case];
        let triaged = scratch.triage(name, &input, TIMEOUT);
        assert_eq!(triaged.status, status, "{name}: {triaged:?}");
        assert_eq!(triaged.bucket, bucket, "{name}");
        assert_eq!(triaged.signature.as_deref(), expected, "{name}");
        let place = match (bucket, expected) {
            (Some(bucket), Some(signature)) => scratch
                .root()
                .join(bucket.name())
                .join(signature_hash(signature)),
            _ => scratch.root().join(status.name()),
        };
        assert_eq!(triaged.bundle, place.join(sha256(&input)), "{name}");
        assert_eq!(
            fs::read(triaged.bundle.join(BUNDLE_INPUT_FILE)).unwrap(),
            input,
            "{name}"
        );
        assert_eq!(
            verify_bundle(&triaged.bundle).unwrap(),
            Vec::<String>::new(),
            "{name}"
        );
        assert_eq!(report(&triaged.bundle)["status"], status.name(), "{name}");
    }

    // The aborted recordings left their steps, and no replay ran.
    let abort = scratch
        .root()
        .join("crash")
        .join(signature_hash("crash abort"));
    let aborted = report(&abort.join(sha256(&[ABORT_CASE])));
    for record in aborted["records"].as_array().unwrap() {
        assert_eq!(record["exit"], serde_json::json!({ "signal": 6 }));
        assert_eq!(record["class"]["failure"]["kind"], "abort");
        assert!(record["class"]["steps"].as_u64().unwrap() >= 1, "{record}");
    }
    assert_eq!(aborted["replay"], Value::Null);

    // The recordings that disagreed both stay in their bundle.
    let disagreed = scratch
        .root()
        .join("nondeterministic")
        .join(sha256(&[FIRST_ONLY_CASE]));
    let nondeterministic = report(&disagreed);
    assert_eq!(
        nondeterministic["records"][0]["signature"],
        format!("active {ACTIVE}")
    );
    assert_eq!(nondeterministic["records"][1]["signature"], Value::Null);
    for name in ["record-1", "record-2"] {
        let trace = disagreed.join(RUNS_DIR).join(name).join(RUN_TRACE_DIR);
        assert!(TraceReader::open(&trace).is_ok(), "{name}");
    }

    // The replay that disagreed failed, and its output stays.
    let replayed = scratch
        .root()
        .join("replay-failed")
        .join(sha256(&[RECORDS_ONLY_CASE]));
    let replay_failed = report(&replayed);
    assert_eq!(replay_failed["replay"]["verified"], false);
    assert_ne!(
        replay_failed["replay"]["exit"],
        serde_json::json!({ "code": 0 })
    );
}

/// Acceptance test 3, the triage child: a recording that hangs is killed
/// at its time limit, classifies as a timeout through `classify_process`,
/// and the input survives whole, both in place and in the bundle.
#[test]
fn hung_child_times_out_and_keeps_artifact() {
    let scratch = Scratch::new();
    let input = [HANG_CASE, 9];
    let started = Instant::now();
    let triaged = scratch.triage("timeout-1", &input, Duration::from_secs(1));
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(triaged.status, TriageStatus::Confirmed);
    assert_eq!(triaged.bucket, Some(Bucket::Crash));
    assert_eq!(triaged.signature.as_deref(), Some("crash timeout"));
    for child in &triaged.records {
        assert_eq!(child.exit, ProcessExit::TimedOut);
        assert_eq!(
            child.classified.result,
            ProcessResult::Crashed {
                failure: ProcessFailure::Timeout,
                end: None,
            }
        );
    }
    assert_eq!(triaged.replay, None);
    assert_eq!(
        fs::read(scratch.dir.path().join("timeout-1")).unwrap(),
        input
    );
    assert_eq!(
        fs::read(triaged.bundle.join(BUNDLE_INPUT_FILE)).unwrap(),
        input
    );
    assert_eq!(report(&triaged.bundle)["records"][0]["exit"], "timed-out");
    assert_eq!(
        verify_bundle(&triaged.bundle).unwrap(),
        Vec::<String>::new()
    );
}

/// A bundle's checksums catch a changed, a missing and an added file.
#[test]
fn verify_bundle_finds_changed_missing_and_added_files() {
    let scratch = Scratch::new();
    let bundle = scratch.triage("crash", &[KNOWN_CASE], TIMEOUT).bundle;
    fs::write(bundle.join(BUNDLE_INPUT_FILE), [0]).unwrap();
    fs::remove_file(bundle.join(SCENARIO_FILE)).unwrap();
    fs::write(bundle.join("extra"), b"").unwrap();
    let mut problems = verify_bundle(&bundle).unwrap();
    problems.sort();
    assert_eq!(
        problems,
        [
            format!("extra isn't in {CHECKSUMS_FILE}"),
            format!("{BUNDLE_INPUT_FILE} changed"),
            format!("{SCENARIO_FILE} is missing"),
        ]
    );
}

/// Two recordings that file the input alike but whose traces differ are
/// `nondeterministic`, even when the replay matches the first. The report
/// names the first record where they differ.
#[test]
fn recordings_whose_traces_differ_are_nondeterministic() {
    let scratch = Scratch::new();
    let input = [SECOND_DIFFERS_CASE];
    let triaged = scratch.triage("second-differs", &input, TIMEOUT);
    assert_eq!(
        signature(&triaged.records[0].classified),
        signature(&triaged.records[1].classified)
    );
    assert_eq!(
        triaged.status,
        TriageStatus::Nondeterministic,
        "{triaged:?}"
    );
    assert_eq!(
        triaged.bundle,
        scratch.root().join("nondeterministic").join(sha256(&input))
    );
    let difference = &report(&triaged.bundle)["difference"];
    assert!(difference["field"].as_str().is_some(), "{difference}");
}

/// A recording whose trace doesn't read can't be compared, so its input is
/// `nondeterministic`, even though both recordings crashed alike. The
/// report names the recording.
#[test]
fn an_unreadable_recording_is_not_confirmed() {
    let scratch = Scratch::new();
    let input = [SECOND_UNREADABLE_CASE];
    let triaged = scratch.triage("second-unreadable", &input, TIMEOUT);
    assert!(signature(&triaged.records[0].classified).is_some());
    assert_eq!(
        signature(&triaged.records[0].classified),
        signature(&triaged.records[1].classified)
    );
    assert_eq!(
        triaged.status,
        TriageStatus::Nondeterministic,
        "{triaged:?}"
    );
    let difference = &report(&triaged.bundle)["difference"];
    assert_eq!(difference["run"], "runs/record-2", "{difference}");
    assert!(difference["unread"].as_str().is_some(), "{difference}");
}

/// `bundle`'s name followed by `-<n>`, beside it.
fn beside(bundle: &Path, n: u32) -> PathBuf {
    let name = bundle.file_name().unwrap().to_str().unwrap();
    bundle.with_file_name(format!("{name}-{n}"))
}

/// Copies the directory `from` to `to`.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for item in fs::read_dir(from).unwrap() {
        let item = item.unwrap();
        let target = to.join(item.file_name());
        if item.file_type().unwrap().is_dir() {
            copy_dir(&item.path(), &target);
        } else {
            fs::copy(item.path(), target).unwrap();
        }
    }
}

/// A bundle in a triage's place that fails its checksums, or that names
/// another result, as a stale or misplaced copy does, doesn't stop the
/// triage from archiving. The new bundle goes beside it as
/// `<input sha256>-1`, the bundle that was there stays as it was, and a
/// later triage keeps the new one.
#[test]
fn a_bundle_that_fails_or_differs_keeps_the_new_one_beside_it() {
    let scratch = Scratch::new();
    let first = scratch.triage("active", &[ACTIVE_CASE], TIMEOUT);
    fs::write(first.bundle.join(SCENARIO_FILE), b"{}\n").unwrap();
    let damaged = verify_bundle(&first.bundle).unwrap();
    assert!(!damaged.is_empty());
    let fresh = scratch.triage("active-fresh", &[ACTIVE_CASE], TIMEOUT);
    assert_eq!(fresh.status, TriageStatus::Confirmed);
    assert!(fresh.archived, "{fresh:?}");
    assert_eq!(fresh.bundle, beside(&first.bundle, 1));
    assert_eq!(verify_bundle(&fresh.bundle).unwrap(), Vec::<String>::new());
    assert_eq!(verify_bundle(&first.bundle).unwrap(), damaged);
    let again = scratch.triage("active-again", &[ACTIVE_CASE], TIMEOUT);
    assert!(!again.archived, "{again:?}");
    assert_eq!(again.bundle, fresh.bundle);

    // The confirmed bundle, copied to where `[CLEAN]` goes, matches its
    // checksums but names another status and input.
    let misplaced = scratch.root().join("not-reproduced").join(sha256(&[CLEAN]));
    copy_dir(&fresh.bundle, &misplaced);
    assert_eq!(verify_bundle(&misplaced).unwrap(), Vec::<String>::new());
    let clean = scratch.triage("clean", &[CLEAN], TIMEOUT);
    assert_eq!(clean.status, TriageStatus::NotReproduced);
    assert!(clean.archived, "{clean:?}");
    assert_eq!(clean.bundle, beside(&misplaced, 1));
    assert_eq!(report(&clean.bundle)["input"]["sha256"], sha256(&[CLEAN]));

    let mut all = vec![first.bundle, fresh.bundle, misplaced, clean.bundle];
    all.sort();
    assert_eq!(bundles(&scratch.out()).unwrap(), all);
}

/// A staging bundle that a killed triage left, under a staging name this
/// process would pick, doesn't stop a later triage from archiving.
#[test]
fn staging_leftovers_dont_block_triage() {
    let scratch = Scratch::new();
    for n in 0..256 {
        let leftover = scratch
            .out()
            .join(STAGING_DIR)
            .join(format!("{}-{n}", std::process::id()));
        fs::create_dir_all(&leftover).unwrap();
        fs::write(leftover.join(BUNDLE_INPUT_FILE), [n as u8]).unwrap();
    }
    let triaged = scratch.triage("crash", &[ACTIVE_CASE], TIMEOUT);
    assert_eq!(triaged.status, TriageStatus::Confirmed);
    assert!(triaged.archived);
    assert_eq!(bundles(&scratch.out()).unwrap(), [triaged.bundle]);
}

/// A bundle that lost its `triage.json` or its checksums is still a bundle,
/// and its check names what's missing.
#[test]
fn verify_finds_bundles_without_their_report_or_checksums() {
    let scratch = Scratch::new();
    let report_lost = scratch.triage("known", &[KNOWN_CASE], TIMEOUT).bundle;
    let sums_lost = scratch.triage("clean", &[CLEAN], TIMEOUT).bundle;
    fs::remove_file(report_lost.join(TRIAGE_FILE)).unwrap();
    fs::remove_file(sums_lost.join(CHECKSUMS_FILE)).unwrap();
    let mut both = vec![report_lost.clone(), sums_lost.clone()];
    both.sort();
    assert_eq!(bundles(&scratch.out()).unwrap(), both);
    assert_eq!(
        verify_bundle(&report_lost).unwrap(),
        [format!("{TRIAGE_FILE} is missing")]
    );
    assert_eq!(
        verify_bundle(&sums_lost).unwrap(),
        [format!("{CHECKSUMS_FILE} is missing")]
    );
}

/// A failed child's buckets keep the class of the `end` record it wrote
/// first, beside `crash`. The signature names the primary bucket's failure,
/// and a sanitizer report's process ID and addresses don't split it.
#[test]
fn process_buckets_and_signatures() {
    let crashed = |failure: ProcessFailure, end: Option<Classified>| ProcessClassified {
        result: ProcessResult::Crashed { failure, end },
        steps: 3,
    };
    let failed_and_abandoned = Classified {
        outcome: ExploreOutcome::Fail {
            property: ACTIVE.to_owned(),
        },
        notes: vec![(KNOWN.to_owned(), Bucket::Known)],
        leaked: 1,
    };
    let crash = crashed(ProcessFailure::Abort, Some(failed_and_abandoned));
    assert_eq!(crash.bucket(), Some(Bucket::Crash));
    assert_eq!(
        crash.buckets(),
        [
            Bucket::Active,
            Bucket::Known,
            Bucket::Crash,
            Bucket::Abandoned
        ]
    );
    assert_eq!(primary_bucket(&crash), Some(Bucket::Crash));
    assert_eq!(signature(&crash).as_deref(), Some("crash abort"));

    let asan = |pid: u32, address: &str| {
        crashed(
            ProcessFailure::Sanitizer {
                report: format!(
                    "=={pid}==ERROR: AddressSanitizer: heap-use-after-free on address {address}"
                ),
            },
            None,
        )
    };
    let expected = "crash sanitizer ERROR: AddressSanitizer: heap-use-after-free on address";
    assert_eq!(signature(&asan(7, "0x1")).as_deref(), Some(expected));
    assert_eq!(signature(&asan(81, "0xbeef")).as_deref(), Some(expected));

    let ended = |outcome: ExploreOutcome, notes: Vec<(String, Bucket)>| ProcessClassified {
        result: ProcessResult::Ended(Classified {
            outcome,
            notes,
            leaked: 0,
        }),
        steps: 3,
    };
    let clean = ended(ExploreOutcome::Clean, vec![]);
    assert_eq!(clean.buckets(), []);
    assert_eq!(primary_bucket(&clean), None);
    assert_eq!(signature(&clean), None);
    let draft = ended(
        ExploreOutcome::Clean,
        vec![("test/triage/draft".to_owned(), Bucket::Draft)],
    );
    assert_eq!(draft.bucket(), None);
    assert_eq!(primary_bucket(&draft), Some(Bucket::Draft));
    assert_eq!(
        signature(&draft).as_deref(),
        Some("draft test/triage/draft")
    );
    // A `Known` violation reported after a `Draft` one still names the
    // primary bucket's property.
    let both = ended(
        ExploreOutcome::Clean,
        vec![
            ("test/triage/draft".to_owned(), Bucket::Draft),
            (KNOWN.to_owned(), Bucket::Known),
        ],
    );
    assert_eq!(primary_bucket(&both), Some(Bucket::Known));
    assert_eq!(signature(&both), Some(format!("known {KNOWN}")));
    // A harness failure keeps its run's `Active` violation in `notes`, and
    // still names the harness failure's reason.
    let harness = ended(
        ExploreOutcome::HarnessFailure {
            reason: "bad task name".to_owned(),
        },
        vec![(ACTIVE.to_owned(), Bucket::Active)],
    );
    assert_eq!(harness.buckets(), [Bucket::Active, Bucket::Harness]);
    assert_eq!(primary_bucket(&harness), Some(Bucket::Harness));
    assert_eq!(
        signature(&harness).as_deref(),
        Some("harness bad task name")
    );
    let no_end = ProcessClassified {
        result: ProcessResult::NoEnd,
        steps: 2,
    };
    assert_eq!(no_end.buckets(), [Bucket::NoEnd]);
    assert_eq!(signature(&no_end).as_deref(), Some("no-end"));
}

/// Runs the binary with `args`.
fn zakura_sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs")
}

/// The binary's `triage` runs `zakura-sim record` and `replay` as its
/// children on a registered harness, and `verify-archive` checks and
/// counts what it archived.
#[test]
fn the_binary_triages_and_verifies_an_archive() {
    let scratch = Scratch::new();
    // Discovery's scenario prefix is empty, so these are all Plan bytes.
    let input = scratch.artifact("input", &[0, 1, 7, 3, 9, 1, 4, 2]);
    let out = scratch.out();
    let output = zakura_sim(&[
        "triage",
        "--sim",
        "discovery/wait-complete",
        "--out",
        out.to_str().unwrap(),
        "--child-secs",
        "60",
        input.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{output:?}");
    let [bundle] = <[PathBuf; 1]>::try_from(bundles(&out).unwrap()).unwrap();
    let report = report(&bundle);
    let place = out
        .join("discovery")
        .join("wait-complete")
        .join(report["provenance"].as_str().unwrap());
    assert_eq!(
        bundle,
        place
            .join("not-reproduced")
            .join(sha256(&fs::read(&input).unwrap()))
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        stdout,
        format!(
            "{}: not-reproduced; archived in {}\n",
            input.display(),
            bundle.display()
        )
    );
    assert_eq!(report["harness"], "discovery/wait-complete");
    assert_eq!(report["replay"]["verified"], true);

    let output = zakura_sim(&["verify-archive", out.to_str().unwrap()]);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            "1 {}: not-reproduced\n1 bundles match their checksums\n",
            place
                .join("not-reproduced")
                .strip_prefix(&out)
                .unwrap()
                .display()
        )
    );

    fs::write(bundle.join(BUNDLE_INPUT_FILE), b"changed").unwrap();
    let output = zakura_sim(&["verify-archive", out.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("input.bin changed"), "{stderr}");

    let output = zakura_sim(&["triage", "--sim", "discovery/wait-complete"]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
}

/// The same bytes on two harnesses are two runs, so the binary keeps a
/// bundle for each in one archive, and both verify. Triaging them again on
/// the first harness keeps the bundle that's there.
#[test]
fn the_binary_keeps_each_harness_bundle_of_the_same_bytes() {
    let scratch = Scratch::new();
    let input = scratch.artifact("input", &[]);
    let out = scratch.out();
    let run = |harness: &str| {
        let output = zakura_sim(&[
            "triage",
            "--sim",
            harness,
            "--out",
            out.to_str().unwrap(),
            "--child-secs",
            "60",
            input.to_str().unwrap(),
        ]);
        assert!(output.status.success(), "{harness}: {output:?}");
        String::from_utf8(output.stdout).unwrap()
    };
    for harness in ["discovery/wait-complete", "fixtures/fixed"] {
        let stdout = run(harness);
        assert!(stdout.contains("; archived in "), "{harness}: {stdout}");
    }
    let found = bundles(&out).unwrap();
    let harnesses: Vec<Value> = found
        .iter()
        .map(|bundle| report(bundle)["harness"].clone())
        .collect();
    assert_eq!(harnesses, ["discovery/wait-complete", "fixtures/fixed"]);

    let stdout = run("discovery/wait-complete");
    assert!(stdout.contains("; already archived in "), "{stdout}");
    assert_eq!(bundles(&out).unwrap(), found);

    let output = zakura_sim(&["verify-archive", out.to_str().unwrap()]);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.ends_with("2 bundles match their checksums\n"),
        "{stdout}"
    );
}
