//! Record mode: each harness writes a trace that the reader accepts as
//! complete, and equal inputs write equal traces.
#![cfg(zakura_sim)]

use tempfile::TempDir;
use zakura_sim::{
    harness::{self, record, RecordError, RecordInput, HARNESSES},
    record::OutcomeKind,
    trace::{TraceReader, INPUT_FILE, TRACE_FILE},
};

/// Fixed bytes for a recording: a scenario prefix that picks the first
/// variant on 2 workers, then Plan bytes.
const BYTES: &[u8] = &[0, 1, 7, 3, 9, 1, 4, 2, 8, 5, 6, 0, 2, 7];

#[test]
fn record_writes_a_complete_trace() {
    for entry in HARNESSES {
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
    for entry in HARNESSES {
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
    let entry = harness::find("race/timer-cancel").unwrap();
    let scenario = serde_json::json!({ "variant": "fixed", "workers": 1 });
    let dir = TempDir::new().unwrap();
    let trace = record(
        entry,
        RecordInput::Plan {
            scenario: &scenario,
            plan: &BYTES[2..],
        },
        dir.path(),
    )
    .unwrap();
    assert_eq!(trace.meta.scenario, scenario);
    assert_eq!(trace.meta.input, None);
    assert!(!dir.path().join(INPUT_FILE).exists());

    // The same run from a full input differs only in meta.input.
    let full = TempDir::new().unwrap();
    let mut bytes = vec![1, 0];
    bytes.extend_from_slice(&BYTES[2..]);
    let from_input = record(entry, RecordInput::Input(&bytes), full.path()).unwrap();
    assert_eq!(from_input.meta.scenario, scenario);
    assert_eq!(from_input.steps, trace.steps);
    assert_eq!(from_input.end, trace.end);
}

#[test]
fn record_refuses_to_overwrite() {
    let entry = harness::find("race/lost-update").unwrap();
    let dir = TempDir::new().unwrap();
    record(entry, RecordInput::Input(BYTES), dir.path()).unwrap();
    let before = std::fs::read(dir.path().join(TRACE_FILE)).unwrap();
    let again = record(entry, RecordInput::Input(&[1, 1]), dir.path());
    assert!(matches!(again, Err(RecordError::Io(_))));
    assert_eq!(std::fs::read(dir.path().join(TRACE_FILE)).unwrap(), before);
    assert_eq!(std::fs::read(dir.path().join(INPUT_FILE)).unwrap(), BYTES);
}

#[test]
fn a_short_input_reads_missing_scenario_bytes_as_zero() {
    let entry = harness::find("race/lost-update").unwrap();
    let dir = TempDir::new().unwrap();
    let trace = record(entry, RecordInput::Input(&[2]), dir.path()).unwrap();
    assert_eq!(trace.meta.scenario, (entry.draw)(&[2, 0]));
    assert_eq!(trace.meta.input.unwrap().len, 1);
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
