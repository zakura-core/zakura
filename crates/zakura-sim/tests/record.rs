//! Record mode: each harness writes a trace that the reader accepts as
//! complete, and equal inputs write equal traces.
#![cfg(zakura_sim)]

use std::rc::Rc;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use zakura_scheduler::SchedulerConfig;
use zakura_sim::{
    harness::{self, entry, record, HarnessEntry, RecordError, RecordInput, Subsystem, HARNESSES},
    property::HasView,
    record::OutcomeKind,
    trace::{meta::PropertyRef, TraceReader, INPUT_FILE, TRACE_FILE},
    Checker, NoChecks, RunConfig, Sim,
};

/// Fixed bytes for a recording: a scenario prefix for harnesses that have
/// one, then Plan bytes.
const BYTES: &[u8] = &[0, 1, 7, 3, 9, 1, 4, 2, 8, 5, 6, 0, 2, 7];

/// A test harness with a one-byte scenario prefix, so record mode's prefix
/// split has a harness to run on.
struct Yielders;

/// How many tasks [`Yielders`] spawns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Tasks {
    tasks: u8,
}

impl HasView for Yielders {
    type View = ();

    /// Nothing.
    fn view(&self) {}
}

impl Subsystem for Yielders {
    const NAME: &'static str = "test/yielders";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 1;
    type Scenario = Tasks;

    /// 2 workers and small stacks.
    fn config(_: &Tasks) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 2,
                stack_size: 64 * 1024,
                ..SchedulerConfig::default()
            },
            ..RunConfig::default()
        }
    }

    /// Spawns the tasks, each yielding twice.
    fn build(sim: &mut Sim, scenario: &Tasks) -> Self {
        for _ in 0..scenario.tasks {
            sim.scheduler().spawn_root("yielder", async {
                zakura_rt::task::yield_now().await;
                zakura_rt::task::yield_now().await;
            });
        }
        Yielders
    }

    /// Checks nothing.
    fn checker(_: &Rc<Self>, _: &Tasks) -> Box<dyn Checker> {
        Box::new(NoChecks)
    }

    /// None.
    fn properties(_: &Tasks) -> Vec<PropertyRef> {
        Vec::new()
    }

    /// One to four tasks.
    fn draw(prefix: &[u8]) -> Tasks {
        Tasks {
            tasks: prefix[0] % 4 + 1,
        }
    }
}

/// The [`Yielders`] entry.
const YIELDERS: HarnessEntry = entry::<Yielders>();

#[test]
fn record_writes_a_complete_trace() {
    for entry in HARNESSES.iter().chain([&YIELDERS]) {
        let dir = TempDir::new().unwrap();
        let trace = record(entry, RecordInput::Input(BYTES), dir.path())
            .unwrap_or_else(|error| panic!("{}: {error}", entry.name));
        let read = TraceReader::open(dir.path()).unwrap();
        assert_eq!(read, trace);
        assert!(read.is_complete());
        assert_eq!(read.meta.sim, entry.name);
        assert_eq!(
            read.meta.scenario,
            (entry.draw)(&BYTES[..entry.scenario_bytes])
        );
        let input = read.meta.input.as_ref().expect("a full input is saved");
        assert_eq!(input.len, u64::try_from(BYTES.len()).unwrap());
        assert_eq!(std::fs::read(dir.path().join(INPUT_FILE)).unwrap(), BYTES);
        let end = read.end.expect("the trace is complete");
        assert_ne!(end.outcome, OutcomeKind::Harness);
        let reported: Vec<_> = end.properties.iter().map(|r| r.property.as_str()).collect();
        let declared: Vec<_> = read.meta.properties.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(
            reported, declared,
            "{}: meta names every reported property",
            entry.name
        );
        assert_eq!(end.steps, u64::try_from(read.steps.len()).unwrap());
    }
}

#[test]
fn recording_replays_the_same_bytes() {
    for entry in HARNESSES.iter().chain([&YIELDERS]) {
        let (a, b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let a = record(entry, RecordInput::Input(BYTES), a.path()).unwrap();
        let b = record(entry, RecordInput::Input(BYTES), b.path()).unwrap();
        assert_eq!(
            a.id, b.id,
            "{}: equal inputs write equal traces",
            entry.name
        );
    }
}

#[test]
fn recording_plan_bytes_writes_no_input() {
    let scenario = serde_json::json!({ "tasks": 2 });
    let dir = TempDir::new().unwrap();
    let trace = record(
        &YIELDERS,
        RecordInput::Plan {
            scenario: &scenario,
            plan: &BYTES[1..],
        },
        dir.path(),
    )
    .unwrap();
    assert_eq!(trace.meta.scenario, scenario);
    assert_eq!(trace.meta.input, None);
    assert!(!dir.path().join(INPUT_FILE).exists());

    // The same run from a full input differs only in meta.input.
    let full = TempDir::new().unwrap();
    let mut bytes = vec![1];
    bytes.extend_from_slice(&BYTES[1..]);
    let from_input = record(&YIELDERS, RecordInput::Input(&bytes), full.path()).unwrap();
    assert_eq!(from_input.meta.scenario, scenario);
    assert_eq!(from_input.steps, trace.steps);
    assert_eq!(from_input.end, trace.end);
}

#[test]
fn record_refuses_to_overwrite() {
    let dir = TempDir::new().unwrap();
    record(&YIELDERS, RecordInput::Input(BYTES), dir.path()).unwrap();
    let before = std::fs::read(dir.path().join(TRACE_FILE)).unwrap();
    let again = record(&YIELDERS, RecordInput::Input(&[1, 1]), dir.path());
    assert!(matches!(again, Err(RecordError::Io(_))));
    assert_eq!(std::fs::read(dir.path().join(TRACE_FILE)).unwrap(), before);
    assert_eq!(std::fs::read(dir.path().join(INPUT_FILE)).unwrap(), BYTES);
}

/// An input recorded into a directory that holds a Plan trace is refused
/// before `input.bin` is written, so the old trace gains no file its meta
/// doesn't name.
#[test]
fn record_into_a_plan_trace_writes_no_input() {
    let scenario = serde_json::json!({ "tasks": 2 });
    let dir = TempDir::new().unwrap();
    let plan = RecordInput::Plan {
        scenario: &scenario,
        plan: &BYTES[1..],
    };
    record(&YIELDERS, plan, dir.path()).unwrap();
    let again = record(&YIELDERS, RecordInput::Input(BYTES), dir.path());
    assert!(matches!(again, Err(RecordError::Io(_))));
    assert!(!dir.path().join(INPUT_FILE).exists());
}

#[test]
fn a_short_input_reads_missing_scenario_bytes_as_zero() {
    let dir = TempDir::new().unwrap();
    let trace = record(&YIELDERS, RecordInput::Input(&[]), dir.path()).unwrap();
    assert_eq!(trace.meta.scenario, serde_json::json!({ "tasks": 1 }));
    assert_eq!(trace.meta.input.unwrap().len, 0);
}

/// T1.6: bytes per step for the header-sync and discovery harnesses. The
/// number is recorded, not checked against a threshold.
#[test]
#[allow(clippy::print_stdout)]
fn bytes_per_step() {
    for name in ["header-sync/smoke", "discovery/wait-complete"] {
        let entry = harness::find(name).unwrap();
        let dir = TempDir::new().unwrap();
        let trace = record(entry, RecordInput::Input(BYTES), dir.path()).unwrap();
        let bytes = std::fs::metadata(dir.path().join(TRACE_FILE))
            .unwrap()
            .len();
        let steps = u64::try_from(trace.steps.len()).unwrap();
        assert!(steps > 0, "{name} took steps");
        // Integer division is enough for a report, and steps is nonzero.
        println!(
            "bytes_per_step {name}: {} bytes over {steps} steps = {} bytes per step",
            bytes,
            bytes / steps
        );
    }
}

/// A symlinked `out` is refused before anything is written, so the link's
/// target stays empty and a retry with a real directory works.
#[cfg(unix)]
#[test]
fn a_symlinked_out_is_refused_before_writing() {
    let dir = TempDir::new().unwrap();
    let target = dir.path().join("target");
    std::fs::create_dir(&target).unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    for out in [link.clone(), link.join(""), link.join(".")] {
        let error = record(&YIELDERS, RecordInput::Input(BYTES), &out).unwrap_err();
        assert!(
            matches!(error, RecordError::Refused(_)),
            "{}: {error}",
            out.display()
        );
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
    }
    record(&YIELDERS, RecordInput::Input(BYTES), &target).unwrap();
}
