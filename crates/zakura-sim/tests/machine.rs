//! The shared machine adapter and machine shrinking, on a fixture machine.
#![cfg(zakura_sim)]

use std::{path::Path, sync::atomic::AtomicBool};

use serde_json::json;
use tempfile::TempDir;
use zakura_sim::{
    harness::{
        self,
        machine::{Machine, INPUT_BYTES},
        HarnessEntry, HarnessError, RecordInput, NO_TASK_PANIC,
    },
    record::machine::{FailurePhase, FixtureStep, MachineFailure, MachineStep},
    sim::NO_LIVELOCK,
    trace::{
        replay_with,
        shrink::{shrink_with, ShrinkBudget, ShrinkReport, REDUCED_DIR},
        ReplayOptions, Trace, TraceReader,
    },
    witness::{Expect, WitnessError},
    MemorySink, NullSink, PlanInput, PropertyOutcome, Record,
};

/// An operation with no effect.
const IDLE: [u8; 4] = [0, 0, 0, 0];
/// An operation that reaches the fixture's resource-bearing transition.
const CHARGE: [u8; 4] = [1, 0, 0, 0];
/// A property failure whose message is the same at every position.
const BREAK: [u8; 4] = [2, 0, 0, 0];
/// A property failure whose message names its position.
const BREAK_AT: [u8; 4] = [3, 0, 0, 0];
/// A driver failure, which is no property detection.
const PANIC: [u8; 4] = [4, 0, 0, 0];

/// Decodes four-byte operations and stops at the first failure.
fn history(input: &[u8]) -> Vec<(usize, MachineStep)> {
    let mut steps = Vec::new();
    for (index, operation) in input.chunks(4).enumerate() {
        let failure = |property_failure, message| {
            Some(MachineFailure {
                property_failure,
                phase: FailurePhase::Invariant,
                message,
            })
        };
        let failure = match operation[0] {
            2 => failure(true, "broke".to_owned()),
            3 => failure(true, format!("broke at operation {index}")),
            4 => failure(false, "panicked".to_owned()),
            _ => None,
        };
        let stop = failure.is_some();
        steps.push((
            index + 1,
            MachineStep::Fixture(FixtureStep {
                input: operation.to_vec(),
                failure,
            }),
        ));
        if stop {
            break;
        }
    }
    steps
}

/// Any operation other than [`IDLE`] reaches the fixture's trigger.
fn reached(steps: &[MachineStep], _: Expect) -> bool {
    steps
        .iter()
        .any(|step| matches!(step, MachineStep::Fixture(step) if step.input[0] != 0))
}

const FIXTURE: Machine = Machine {
    name: "fixtures/machine",
    identity: "fixture",
    claim: "The fixture machine's history reports no failure.",
    generator: 1,
    budget: 128,
    history,
    reached,
};

static ENTRY: HarnessEntry = zakura_sim::machine_entry!(FIXTURE);

/// The concatenation of `operations`.
fn input(operations: &[[u8; 4]]) -> Vec<u8> {
    operations.concat()
}

/// The trace of the full fuzz input `input`.
fn recorded(input: &[u8]) -> (TempDir, Trace) {
    let dir = TempDir::new().unwrap();
    let trace = harness::record(&ENTRY, RecordInput::Input(input), dir.path()).unwrap();
    (dir, trace)
}

/// The report and re-recorded trace of a shrink of `trace` into `out`.
fn shrunk(trace: &Trace, out: &Path) -> (ShrinkReport, Trace) {
    let report = shrink_with(
        trace,
        &ENTRY,
        ShrinkBudget::default(),
        out,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert!(report.fixed_point, "{report:?}");
    assert_eq!(report.rerecord_failure, None);
    let reduced = TraceReader::open(&out.join(REDUCED_DIR)).unwrap();
    (report, reduced)
}

/// The failure that ended `trace`.
fn failure(trace: &Trace) -> MachineFailure {
    trace
        .steps
        .last()
        .and_then(|step| step.machine.as_deref())
        .and_then(MachineStep::failure)
        .expect("the trace ends at a machine failure")
}

#[test]
fn shrinking_deletes_every_operation_the_failure_does_not_need() {
    let (_dir, trace) = recorded(&input(&[IDLE, CHARGE, BREAK, IDLE]));
    let out = TempDir::new().unwrap();
    let (report, reduced) = shrunk(&trace, out.path());

    assert_eq!(report.before.input_bytes, Some(16));
    assert_eq!(report.after.unwrap().input_bytes, Some(4));
    assert_eq!(reduced.meta.scenario, json!({ "input": BREAK }));
    assert_eq!(failure(&reduced), failure(&trace));
    replay_with(&reduced, &ENTRY, ReplayOptions::default()).unwrap();
}

#[test]
fn shrinking_keeps_operations_that_decide_the_exact_failure() {
    // Deleting either leading operation changes the failure's message.
    let (_dir, trace) = recorded(&input(&[IDLE, IDLE, BREAK_AT, IDLE]));
    let out = TempDir::new().unwrap();
    let (report, reduced) = shrunk(&trace, out.path());

    assert_eq!(report.after.unwrap().input_bytes, Some(12));
    assert_eq!(
        reduced.meta.scenario,
        json!({ "input": input(&[IDLE, IDLE, BREAK_AT]) })
    );
    assert_eq!(failure(&reduced), failure(&trace));
}

#[test]
fn a_recorded_machine_run_replays_exactly() {
    let (_dir, trace) = recorded(&input(&[CHARGE, IDLE]));
    assert_eq!(trace.steps.len(), 2);
    assert_eq!(trace.steps[0].pick, "machine:fixture:1");
    replay_with(&trace, &ENTRY, ReplayOptions::default()).unwrap();
}

#[test]
fn a_driver_failure_is_not_a_detection() {
    let scenario = (ENTRY.draw)(&PANIC);
    let error = (ENTRY.run)(&scenario, PlanInput::Bytes(Vec::new()), &mut NullSink).unwrap_err();
    assert!(matches!(error, HarnessError::Run { .. }), "{error}");
}

#[test]
fn a_harness_failure_leaves_no_property_held() {
    // CHARGE holds and reaches the trigger, then PANIC ends the run as a
    // harness failure. As in `Sim::run`, the property is inconclusive.
    let mut sink = MemorySink::default();
    let scenario = (ENTRY.draw)(&input(&[CHARGE, PANIC]));
    let error = (ENTRY.run)(&scenario, PlanInput::Bytes(Vec::new()), &mut sink).unwrap_err();
    let HarnessError::Run { report, .. } = error else {
        panic!("{error}");
    };
    let inconclusive = PropertyOutcome::Inconclusive {
        reason: "harness-failed".to_owned(),
    };
    assert_eq!(report.properties.len(), 1);
    assert_eq!(report.properties[0].outcome, inconclusive);
    let Some(Record::End(end)) = sink.records.last() else {
        panic!("the run ends with its end record");
    };
    assert_eq!(end.properties, report.properties);
}

#[test]
fn a_run_applies_only_once_it_reaches_the_trigger() {
    let outcome = |operations: &[[u8; 4]]| {
        let scenario = (ENTRY.draw)(&input(operations));
        let report = (ENTRY.run)(&scenario, PlanInput::Bytes(Vec::new()), &mut NullSink).unwrap();
        report.properties[0].outcome.clone()
    };
    assert_eq!(outcome(&[IDLE, IDLE]), PropertyOutcome::NotApplicable);
    assert_eq!(
        outcome(&[IDLE, CHARGE, IDLE]),
        PropertyOutcome::Held { applicable: 2 }
    );
    // A failure before the trigger still reads as a violation.
    let scenario = (ENTRY.draw)(&input(&[IDLE, BREAK]));
    let report = (ENTRY.run)(&scenario, PlanInput::Bytes(Vec::new()), &mut NullSink).unwrap();
    assert!(
        matches!(
            report.properties[0].outcome,
            PropertyOutcome::Violated { .. }
        ),
        "{report:?}"
    );
}

#[test]
fn a_short_input_keeps_its_length() {
    assert_eq!(ENTRY.scenario(&[1, 2]), json!({ "input": [1, 2] }));
    let long = vec![0; INPUT_BYTES + 1];
    assert_eq!(
        ENTRY.scenario(&long),
        json!({ "input": vec![0; INPUT_BYTES] })
    );
}

#[test]
fn a_witness_needs_its_trigger_history() {
    let witness = |operations: &[[u8; 4]], property: &str, expect| {
        (ENTRY.witness)(
            &(ENTRY.draw)(&input(operations)),
            PlanInput::Bytes(Vec::new()),
            property,
            expect,
            &mut MemorySink::default(),
        )
    };
    assert_eq!(witness(&[CHARGE], FIXTURE.name, Expect::Holds), Ok(()));
    assert_eq!(witness(&[BREAK], FIXTURE.name, Expect::Violated), Ok(()));
    assert_eq!(witness(&[CHARGE], FIXTURE.name, Expect::Applicable), Ok(()));
    // An idle-only run never applies, so no expectation passes on it.
    for expect in [Expect::Applicable, Expect::Holds] {
        assert_eq!(
            witness(&[IDLE], FIXTURE.name, expect),
            Err(WitnessError::Outcome(PropertyOutcome::NotApplicable))
        );
    }
    assert_eq!(
        witness(&[CHARGE], "no/such-property", Expect::Holds),
        Err(WitnessError::Unregistered)
    );
}

#[test]
fn a_machine_entry_checks_only_its_own_property() {
    let properties = (ENTRY.properties)();
    assert_eq!(properties.len(), 1);
    assert_eq!(properties[0].id, FIXTURE.name);
    assert_eq!(ENTRY.status(NO_TASK_PANIC), None);
    assert_eq!(ENTRY.status(NO_LIVELOCK), None);
}
