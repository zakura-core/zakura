//! E1c: every result class goes through one classification path, whether
//! the run happened in this process or in a fresh one. A run's parts stay
//! apart, so an abandoned cleanup never reads as a pass, and a failing input
//! is on disk before anything reruns it.
#![cfg(zakura_sim)]

use std::{
    path::Path,
    process::{Command, Output},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        mpsc,
    },
    thread,
};

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use zakura_rt::{sync::atomic::AtomicU64, task::yield_now};
use zakura_scheduler::SchedulerConfig;
use zakura_sim::{
    explore::{
        abandon_limit, classify, classify_process, input_path, save_failure, save_input, Bucket,
        Classified, ExploreOutcome, ExploreRun, ProcessClassified, ProcessExit, ProcessFailure,
        ProcessResult, Worker, DEFAULT_ABANDON_LIMIT, INPUTS_DIR,
    },
    harness::{entry, record, HarnessEntry, RecordInput, Subsystem},
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Verdict},
    trace::{FileSink, TraceReader, INPUT_FILE, TRACES_DIR, TRACE_FILE},
    Checker, HarnessFailure, NullSink, ObservationLog, PropertyOutcome, PropertyReport, RunConfig,
    RunEnd, Sim, StepCtx, Violation,
};

/// The [`Results`] scenario: which case to run.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct Case {
    case: u8,
}

/// Yields a few times and exits.
const CLEAN: u8 = 0;
/// [`ACTIVE`] is violated at the first step.
const ACTIVE_CASE: u8 = 1;
/// [`KNOWN`] is violated, and the run goes on.
const KNOWN_CASE: u8 = 2;
/// [`DRAFT`] is violated, and the run goes on.
const DRAFT_CASE: u8 = 3;
/// A task's name holds `:`, a harness failure.
const HARNESS_CASE: u8 = 4;
/// A task spins in cleanup, so shutdown abandons the run.
const ABANDON_CASE: u8 = 5;
/// [`ACTIVE`] is violated, and a task spins in cleanup.
const ACTIVE_ABANDON_CASE: u8 = 6;
/// The task aborts the process at its second step.
const ABORT_CASE: u8 = 7;
/// The task exits the process with code 0 at its second step.
const EXIT_CASE: u8 = 8;
/// [`ACTIVE`] is violated. The process aborts in the second build, which is
/// `record_failure`'s first rerun.
const RERUN_ABORT_CASE: u8 = 9;

const ACTIVE: &str = "test/results/active";
const KNOWN: &str = "test/results/known";
const DRAFT: &str = "test/results/draft";
/// The property [`ViolatesAtEnd`] reports, which the harness doesn't
/// register, so it counts as `Active`.
const AT_END: &str = "test/results/at-end";

/// Builds of [`Results`] in this process.
static BUILDS: AtomicUsize = AtomicUsize::new(0);

/// A one-task harness whose first scenario byte picks a [`Case`].
struct Results {
    case: u8,
}

impl HasView for Results {
    type View = u8;

    /// The case.
    fn view(&self) -> u8 {
        self.case
    }
}

/// Violated at every step of a run whose case is `CASE`.
fn violated_in<const CASE: u8>(cx: &mut CheckCtx<'_, Results>) -> Verdict {
    match cx.view() {
        case if case == CASE || (CASE == ACTIVE_CASE && active(case)) => {
            cx.violated("another case", format!("case {case}"))
        }
        _ => Verdict::Holds,
    }
}

/// True if `case` violates [`ACTIVE`].
fn active(case: u8) -> bool {
    matches!(case, ACTIVE_CASE | ACTIVE_ABANDON_CASE | RERUN_ABORT_CASE)
}

/// A property with `id` and `status`, violated in case `check`'s case.
fn property(
    id: &'static str,
    status: Status,
    check: fn(&mut CheckCtx<'_, Results>) -> Verdict,
) -> Property<Results> {
    Property {
        id: PropertyId::new(id),
        claim: "only another case violates it",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Schedule],
        status,
        check,
        witness: None,
    }
}

impl Subsystem for Results {
    const NAME: &'static str = "test/results";
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
        let build = BUILDS.fetch_add(1, SeqCst);
        let case = scenario.case;
        let scheduler = sim.scheduler();
        match case {
            HARNESS_CASE => scheduler.spawn_root("bad:name", async {}),
            ABANDON_CASE | ACTIVE_ABANDON_CASE => scheduler.spawn_root("spinner", async {
                // Nothing ever stores, and every load is a point, so
                // cleanup gets stuck and shutdown abandons the run.
                let flag = AtomicU64::new(0);
                while flag.load(zakura_rt::sync::atomic::Ordering::SeqCst) == 0 {}
            }),
            ABORT_CASE => scheduler.spawn_root("tick", async {
                yield_now().await;
                std::process::abort();
            }),
            EXIT_CASE => scheduler.spawn_root("tick", async {
                yield_now().await;
                std::process::exit(0);
            }),
            RERUN_ABORT_CASE if build > 0 => scheduler.spawn_root("tick", async {
                std::process::abort();
            }),
            _ => scheduler.spawn_root("tick", async {
                for _ in 0..3 {
                    yield_now().await;
                }
            }),
        };
        Results { case }
    }

    /// The three statuses, each violated in its own case.
    fn properties() -> Vec<Property<Self>> {
        vec![
            property(ACTIVE, Status::Active, violated_in::<ACTIVE_CASE>),
            property(KNOWN, Status::Known { issue: 1 }, violated_in::<KNOWN_CASE>),
            property(DRAFT, Status::Draft, violated_in::<DRAFT_CASE>),
        ]
    }

    /// The first byte is the case.
    fn draw(prefix: &[u8]) -> Case {
        Case { case: prefix[0] }
    }
}

static RESULTS: HarnessEntry = entry::<Results>();

/// A checker that reports [`AT_END`] violated from `at_end` only, as an
/// obligation that expires at run end does (review E-01).
struct ViolatesAtEnd;

impl Checker for ViolatesAtEnd {
    fn after_step(&mut self, _: &StepCtx<'_>) -> Option<Violation> {
        None
    }

    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        vec![PropertyReport::new(
            AT_END,
            PropertyOutcome::Violated {
                witness: Vec::new(),
            },
        )]
    }
}

/// Runs `case` through `run_bytes` as the fuzz loop does, with
/// [`ViolatesAtEnd`] as the extra checker if `at_end`, while a `FileSink`
/// writes the trace. Runs on a new thread, which parks for good if the run
/// leaked tasks. Returns the run and the trace's directory.
fn run_in_process(case: u8, at_end: bool) -> (ExploreRun, TempDir) {
    let dir = TempDir::new().unwrap();
    let out = dir.path().to_owned();
    let (done, result) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let input = [case];
        let scenario = (RESULTS.draw)(&input);
        let meta = (RESULTS.meta)(&scenario).unwrap();
        let mut sink = FileSink::create(&out, &meta).unwrap();
        let mut extra = ViolatesAtEnd;
        let checker = at_end.then_some(&mut extra as &mut dyn Checker);
        let run = (RESULTS.run_bytes)(&input, &mut sink, checker);
        sink.finish().unwrap();
        let leaked = run.leaked() > 0;
        done.send(run).unwrap();
        if leaked {
            loop {
                thread::park();
            }
        }
    });
    (result.recv().unwrap(), dir)
}

/// The class a fresh process gets from the trace in `dir`, after it exited
/// with `exit` and wrote `stderr`.
fn from_trace(exit: ProcessExit, stderr: &[u8], dir: &Path) -> ProcessClassified {
    classify_process(&RESULTS, exit, stderr, dir).unwrap()
}

/// The variable that makes [`results_child`] run a case, as
/// `<mode>:<directory>`.
const CHILD: &str = "ZAKURA_SIM_RESULTS_CHILD";

/// Runs a case when run as the child, and does nothing otherwise.
///
/// - `record:<case>:<dir>` records the case's input to `<dir>` as
///   `zakura-sim record --input` does.
/// - `save:<dir>` runs [`RERUN_ABORT_CASE`] fast, then saves its `Active`
///   failure under `<dir>` as the fuzz target does.
#[test]
fn results_child() {
    let Ok(mode) = std::env::var(CHILD) else {
        return;
    };
    let parts: Vec<&str> = mode.splitn(3, ':').collect();
    match parts[..] {
        ["record", case, dir] => {
            let input = [case.parse::<u8>().unwrap()];
            record(&RESULTS, RecordInput::Input(&input), Path::new(dir)).unwrap();
        }
        ["save", dir] => {
            let input = [RERUN_ABORT_CASE];
            let run = (RESULTS.run_bytes)(&input, &mut NullSink, None);
            assert_eq!(
                classify(&RESULTS, &run).outcome,
                ExploreOutcome::Fail {
                    property: ACTIVE.to_owned()
                }
            );
            let id = PropertyId::new(ACTIVE);
            let _ = save_failure(&RESULTS, &input, &run, &id, Bucket::Active, Path::new(dir));
        }
        _ => panic!("unknown {CHILD} mode {mode}"),
    }
}

/// Reruns this test binary as [`results_child`] in `mode`, without core
/// dumps, and returns its output.
fn child(mode: &str) -> Output {
    let output = Command::new("sh")
        .args(["-c", "ulimit -c 0; exec \"$0\" \"$@\""])
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "results_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, mode)
        .output()
        .expect("the test binary reruns itself");
    output
}

/// Acceptance test 2: one Active, one Known and one Draft violation, one
/// at-end violation, one harness failure, one cleanup abandonment and one
/// process crash, each through the shared classification path. The in-
/// process runs classify the same from their run and from their trace.
#[test]
fn every_class_through_one_path() {
    let fail = |property: &str| ExploreOutcome::Fail {
        property: property.to_owned(),
    };
    let rows: [(&str, u8, bool, ExploreOutcome, Vec<Bucket>); 7] = [
        ("clean", CLEAN, false, ExploreOutcome::Clean, vec![]),
        (
            "Active",
            ACTIVE_CASE,
            false,
            fail(ACTIVE),
            vec![Bucket::Active],
        ),
        (
            "Known",
            KNOWN_CASE,
            false,
            ExploreOutcome::Clean,
            vec![Bucket::Known],
        ),
        (
            "Draft",
            DRAFT_CASE,
            false,
            ExploreOutcome::Clean,
            vec![Bucket::Draft],
        ),
        ("at end", CLEAN, true, fail(AT_END), vec![Bucket::Active]),
        (
            "harness failure",
            HARNESS_CASE,
            false,
            ExploreOutcome::HarnessFailure {
                reason: HarnessFailure::TaskNameHasColon {
                    name: "bad:name.1".to_owned(),
                }
                .to_string(),
            },
            vec![Bucket::Harness],
        ),
        (
            "abandoned",
            ABANDON_CASE,
            false,
            ExploreOutcome::Abandoned { leaked: 1 },
            vec![Bucket::Abandoned],
        ),
    ];
    for (row, case, at_end, outcome, buckets) in rows {
        let (run, dir) = run_in_process(case, at_end);
        let classified = classify(&RESULTS, &run);
        assert_eq!(classified.outcome, outcome, "{row}");
        assert_eq!(classified.buckets(), buckets, "{row}");
        let traced = from_trace(ProcessExit::Code(0), b"", dir.path());
        assert_eq!(traced.result, ProcessResult::Ended(classified), "{row}");
        assert_eq!(traced.bucket(), outcome.bucket(), "{row}");
    }

    // The crash: a fresh process that records the input aborts mid-run,
    // and leaves its input and the steps it took.
    let dir = TempDir::new().unwrap();
    let output = child(&format!("record:{ABORT_CASE}:{}", dir.path().display()));
    let exit = ProcessExit::from(output.status);
    assert_eq!(exit, ProcessExit::Signal(6), "{output:?}");
    let crashed = from_trace(exit, &output.stderr, dir.path());
    assert_eq!(
        crashed.result,
        ProcessResult::Crashed {
            failure: ProcessFailure::Abort,
            end: None,
        }
    );
    assert_eq!(crashed.bucket(), Some(Bucket::Crash));
    assert!(crashed.steps >= 1, "{crashed:?}");
    assert_eq!(
        std::fs::read(dir.path().join(INPUT_FILE)).unwrap(),
        [ABORT_CASE]
    );
}

/// A run that both fails and abandons its cleanup keeps both, in the
/// process and in its trace.
#[test]
fn a_failure_keeps_its_abandonment() {
    let (run, dir) = run_in_process(ACTIVE_ABANDON_CASE, false);
    let classified = classify(&RESULTS, &run);
    assert_eq!(
        classified.outcome,
        ExploreOutcome::Fail {
            property: ACTIVE.to_owned()
        }
    );
    assert!(classified.leaked > 0, "{classified:?}");
    assert_eq!(classified.buckets(), [Bucket::Active, Bucket::Abandoned]);
    let traced = from_trace(ProcessExit::Code(0), b"", dir.path());
    assert_eq!(traced.result, ProcessResult::Ended(classified));
}

/// A process that exits with code 0 before its `end` record is `no-end`,
/// never a pass. One that never wrote its trace is too.
#[test]
fn a_run_without_end_is_no_end() {
    let dir = TempDir::new().unwrap();
    let output = child(&format!("record:{EXIT_CASE}:{}", dir.path().display()));
    let exit = ProcessExit::from(output.status);
    assert_eq!(exit, ProcessExit::Code(0), "{output:?}");
    let no_end = from_trace(exit, &output.stderr, dir.path());
    assert_eq!(no_end.result, ProcessResult::NoEnd);
    assert_eq!(no_end.bucket(), Some(Bucket::NoEnd));
    assert!(no_end.steps >= 1, "{no_end:?}");

    let empty = TempDir::new().unwrap();
    let none = from_trace(ProcessExit::Code(0), b"", empty.path());
    assert_eq!(
        none,
        ProcessClassified {
            result: ProcessResult::NoEnd,
            steps: 0,
        }
    );
    let killed = from_trace(ProcessExit::Signal(9), b"", empty.path());
    assert_eq!(
        killed.result,
        ProcessResult::Crashed {
            failure: ProcessFailure::Signal { signal: 9 },
            end: None,
        }
    );
}

/// Each process failure kind, from the exit and standard error.
#[test]
fn process_failure_kinds() {
    let asan = b"==7==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n\
        SUMMARY: AddressSanitizer: heap-use-after-free\n";
    let rows: [(ProcessExit, &[u8], Option<ProcessFailure>); 13] = [
        (ProcessExit::Code(0), b"", None),
        (ProcessExit::Code(0), b"warning: unrelated\n", None),
        (ProcessExit::TimedOut, b"", Some(ProcessFailure::Timeout)),
        (
            ProcessExit::Code(70),
            b"==9== ERROR: libFuzzer: timeout after 30 seconds\n",
            Some(ProcessFailure::Timeout),
        ),
        (
            ProcessExit::Code(1),
            asan,
            Some(ProcessFailure::Sanitizer {
                report: "==7==ERROR: AddressSanitizer: heap-use-after-free on address 0x1"
                    .to_owned(),
            }),
        ),
        (
            ProcessExit::Code(66),
            b"WARNING: ThreadSanitizer: data race (pid=5)\n",
            Some(ProcessFailure::Sanitizer {
                report: "WARNING: ThreadSanitizer: data race (pid=5)".to_owned(),
            }),
        ),
        (
            ProcessExit::Code(77),
            b"==4==WARNING: MemorySanitizer: use-of-uninitialized-value\n",
            Some(ProcessFailure::Sanitizer {
                report: "==4==WARNING: MemorySanitizer: use-of-uninitialized-value".to_owned(),
            }),
        ),
        (
            ProcessExit::Code(1),
            b"==8==ERROR: AddressSanitizer: out of memory: allocator is trying to allocate 0x1 bytes\n",
            Some(ProcessFailure::OutOfMemory),
        ),
        (
            ProcessExit::Signal(6),
            b"memory allocation of 1048576 bytes failed\n",
            Some(ProcessFailure::OutOfMemory),
        ),
        (
            ProcessExit::Code(71),
            b"==3== ERROR: libFuzzer: out-of-memory (used: 4097Mb; exceeds: 4096Mb)\n",
            Some(ProcessFailure::OutOfMemory),
        ),
        (ProcessExit::Signal(6), b"", Some(ProcessFailure::Abort)),
        (
            ProcessExit::Signal(11),
            b"",
            Some(ProcessFailure::Signal { signal: 11 }),
        ),
        (
            ProcessExit::Code(101),
            b"thread 'main' panicked at src/main.rs:1:1\n",
            Some(ProcessFailure::Exit { code: 101 }),
        ),
    ];
    for (exit, stderr, failure) in rows {
        assert_eq!(
            ProcessFailure::of(exit, stderr),
            failure,
            "{exit:?} {}",
            String::from_utf8_lossy(stderr)
        );
    }
}

/// A process that wrote its `end` record and then failed is a crash, and
/// keeps the run's class.
#[test]
fn a_crash_after_end_keeps_the_run() {
    let (run, dir) = run_in_process(KNOWN_CASE, false);
    let classified = classify(&RESULTS, &run);
    let crashed = from_trace(ProcessExit::Signal(6), b"", dir.path());
    assert_eq!(
        crashed.result,
        ProcessResult::Crashed {
            failure: ProcessFailure::Abort,
            end: Some(classified),
        }
    );
    assert_eq!(crashed.bucket(), Some(Bucket::Crash));
}

/// A failed process whose trace doesn't read as a valid prefix is still a
/// crash. Only a process that didn't fail makes such a trace an error.
#[test]
fn a_crash_with_an_invalid_trace_is_a_crash() {
    let (_, dir) = run_in_process(CLEAN, false);
    let mut trace = std::fs::OpenOptions::new()
        .append(true)
        .open(dir.path().join(TRACE_FILE))
        .unwrap();
    std::io::Write::write_all(&mut trace, b"not a record\n").unwrap();
    assert_eq!(
        from_trace(ProcessExit::Signal(11), b"", dir.path()),
        ProcessClassified {
            result: ProcessResult::Crashed {
                failure: ProcessFailure::Signal { signal: 11 },
                end: None,
            },
            steps: 0,
        }
    );
    assert!(classify_process(&RESULTS, ProcessExit::Code(0), b"", dir.path()).is_err());
}

/// Acceptance test 3, the worker: a process killed while it reruns a
/// failing input still leaves the original input, whole, under its digest.
/// The rerun's partial trace classifies as a crash.
#[test]
fn an_interrupted_rerun_keeps_the_original_input() {
    let root = TempDir::new().unwrap();
    let output = child(&format!("save:{}", root.path().display()));
    let exit = ProcessExit::from(output.status);
    assert_eq!(exit, ProcessExit::Signal(6), "{output:?}");
    let id = PropertyId::new(ACTIVE);
    let input = [RERUN_ABORT_CASE];
    let saved = input_path(root.path(), Bucket::Active, Some(&id), &input);
    assert_eq!(std::fs::read(&saved).unwrap(), input);
    let dir = saved.parent().unwrap();
    assert_eq!(names(dir), [saved.file_name().unwrap().to_str().unwrap()]);

    let traces = ACTIVE
        .split('/')
        .fold(root.path().join(TRACES_DIR), |dir, segment| {
            dir.join(segment)
        });
    let partial = names(&traces);
    assert_eq!(partial.len(), 1, "{partial:?}");
    let recording = traces.join(&partial[0]);
    assert!(
        TraceReader::open(&recording).is_err(),
        "it has no end record"
    );
    assert_eq!(
        from_trace(exit, &output.stderr, &recording).result,
        ProcessResult::Crashed {
            failure: ProcessFailure::Abort,
            end: None,
        }
    );
}

/// The names in `dir`, sorted.
fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

/// Each distinct input keeps its own file under its bucket and property, so
/// a property's first saved input never stops a later one from being kept.
#[test]
fn save_input_keeps_each_distinct_input() {
    let root = TempDir::new().unwrap();
    let id = PropertyId::new(KNOWN);
    let first = save_input(root.path(), Bucket::Known, Some(&id), b"first").unwrap();
    let again = save_input(root.path(), Bucket::Known, Some(&id), b"first").unwrap();
    let second = save_input(root.path(), Bucket::Known, Some(&id), b"second").unwrap();
    assert_eq!(first, again);
    assert_ne!(first, second);
    assert_eq!(first.parent(), second.parent());
    assert_eq!(
        first.parent().unwrap(),
        root.path()
            .join(INPUTS_DIR)
            .join("known")
            .join("test")
            .join("results")
            .join("known")
    );
    assert_eq!(names(first.parent().unwrap()).len(), 2, "no partial file");
    assert_eq!(std::fs::read(&second).unwrap(), b"second");
    let abandoned = save_input(root.path(), Bucket::Abandoned, None, b"first").unwrap();
    assert_eq!(
        abandoned.parent().unwrap(),
        root.path().join(INPUTS_DIR).join("abandoned")
    );
}

/// The worker counts abandoned runs, which the fuzz target compares with
/// the abandonment limit.
#[test]
fn the_worker_counts_abandoned_runs() {
    let mut worker = Worker::new();
    let outcome = |worker: &mut Worker, case: u8| {
        worker.explore(&RESULTS, &[case], |_, _, classified: &Classified| {
            classified.outcome.clone()
        })
    };
    assert_eq!(outcome(&mut worker, CLEAN), ExploreOutcome::Clean);
    assert_eq!(worker.abandoned(), 0);
    assert_eq!(
        outcome(&mut worker, ABANDON_CASE),
        ExploreOutcome::Abandoned { leaked: 1 }
    );
    assert_eq!(worker.abandoned(), 1);
    assert!(matches!(
        outcome(&mut worker, ACTIVE_ABANDON_CASE),
        ExploreOutcome::Fail { .. }
    ));
    assert_eq!(worker.abandoned(), 2, "a failure that abandoned counts");
    assert_eq!(worker.retired(), 2);
}

#[test]
fn abandon_limit_parses() {
    assert_eq!(abandon_limit(None), Ok(DEFAULT_ABANDON_LIMIT));
    assert_eq!(DEFAULT_ABANDON_LIMIT, 1);
    assert_eq!(abandon_limit(Some("")), Ok(DEFAULT_ABANDON_LIMIT));
    assert_eq!(abandon_limit(Some("4")), Ok(4));
    for bad in ["0", "-1", "many"] {
        assert!(abandon_limit(Some(bad)).is_err(), "{bad}");
    }
}
