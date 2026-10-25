//! S4.7: discovery's `wait_complete` wakes once the initial exchange
//! completes. The scenario and its check are the `discovery/wait-complete`
//! harness. This file holds the regression and the input builder.
#![cfg(zakura_sim)]

mod support;

use support::{inputs, recorded};
use zakura_sim::{
    harness::{
        discovery::{run_config, spawn, WaitComplete, WaitCompleteWakes, PROPERTY},
        run_harness, HarnessError,
    },
    record::DrawRecord,
    record::Record,
    MemorySink, NoChecks, Outcome, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// Runs the harness under `plan`.
fn run_wait_complete(plan: PlanInput) -> RunReport {
    match run_harness::<WaitComplete>(&Default::default(), plan, &mut MemorySink::default(), None) {
        Ok(report) => report,
        Err(HarnessError::Run(failure)) => panic!("the run hit a harness failure: {failure}"),
        Err(error) => panic!("{error}"),
    }
}

/// Runs the scenario under `plan` with the check. Returns the report, the
/// records, and the checker, whose choices build inputs by hand.
fn run(plan: PlanInput) -> (RunReport, Vec<String>, WaitCompleteWakes) {
    let mut checker = WaitCompleteWakes::new();
    let (report, lines) = recorded(run_config(), plan, spawn, &mut checker);
    (report, lines, checker)
}

/// How many bytes choose among `range` values, as the `Plan` decodes them.
fn width(range: u128) -> usize {
    match range {
        0..=0x100 => 1,
        0x101..=0x1_0000 => 2,
        0x1_0001..=0x1_0000_0000 => 4,
        _ => 8,
    }
}

/// Appends `value` as `width(range)` little-endian bytes.
fn push(bytes: &mut Vec<u8>, value: u64, range: u128) {
    bytes.extend_from_slice(&value.to_le_bytes()[..width(range)]);
}

/// Appends a draw's answer, unless its bound makes it forced.
fn push_draw(bytes: &mut Vec<u8>, draw: &DrawRecord) {
    match draw.bound.0 {
        1 => {}
        0 => push(bytes, draw.value.0, 1 << 64),
        bound => push(bytes, draw.value.0, u128::from(bound)),
    }
}

/// Builds a fuzz input that holds the source back at its `notified` until
/// the sink has run all three marks. Each round reruns the input, finds the
/// first step where the round-robin default didn't hold the source back,
/// and rewrites the input from there.
///
/// Returns `Ok` with the input once it violates the property, or `Err` with
/// the finished input if holding the source back everywhere it can doesn't.
fn hold_source_back() -> Result<Vec<u8>, Vec<u8>> {
    let mut bytes = Vec::new();
    for _ in 0..1_000 {
        let (report, lines, checker) = run(Plan::from_fuzz(&bytes));
        if report.end.violation.is_some() {
            return Ok(bytes);
        }
        let step_of = |line: &String| match serde_json::from_str::<Record>(line).unwrap() {
            Record::Step(step) => Some(step),
            Record::Meta(_) | Record::Init(_) | Record::End(_) => None,
        };
        let steps: Vec<_> = lines.iter().filter_map(step_of).collect();
        let Some(deviation) = checker
            .choices()
            .iter()
            .position(|(_, picked, hold)| hold.is_some_and(|h| h != *picked))
        else {
            return Err(bytes);
        };
        let mut next = Vec::new();
        if let Ok(Record::Init(init)) = serde_json::from_str::<Record>(&lines[0]) {
            init.draws.iter().for_each(|d| push_draw(&mut next, d));
        }
        for (step, (_, picked, hold)) in steps.iter().zip(checker.choices()).take(deviation + 1) {
            let index = if step.step == steps[deviation].step {
                hold.expect("the deviation holds the source")
            } else {
                *picked
            };
            if step.options > 1 {
                // A step's index and option count fit in u64 and u128.
                push(&mut next, index as u64, step.options as u128);
            }
            if step.step < steps[deviation].step {
                step.draws.iter().for_each(|d| push_draw(&mut next, d));
            }
        }
        bytes = next;
    }
    panic!("no input found in 1,000 rounds");
}

/// The committed input that holds the source back, from
/// [`hold_source_back`] on the code before the fix.
const INPUT: &[u8] = include_bytes!("fixtures/discovery-wait-complete.bin");

/// The regression, checked against the bug. CI's `sim-build` job applies
/// `fixtures/discovery-wait-complete-revert.patch`, which puts the old
/// `wait_complete` back, then runs this test by name. Unpatched, it would
/// fail, so it's ignored.
#[test]
#[ignore = "needs fixtures/discovery-wait-complete-revert.patch applied"]
fn reverted_fix_misses_the_wakeup() {
    let report = run_wait_complete(Plan::from_fuzz(INPUT));
    let violation = report.end.violation.expect("the committed input violates");
    assert_eq!(violation.property, PROPERTY);
    let [load, notify, notified] = violation.witness[..] else {
        panic!("the witness is load, final notify, notified: {violation:?}");
    };
    assert!(
        load < notify && notify < notified,
        "the source checked, the sink notified, then the source created its future: {violation:?}"
    );

    // Unchecked, the run shows what that costs: the source sits out its
    // 2 s initial-exchange timeout.
    let (report, _) = recorded(run_config(), Plan::from_fuzz(INPUT), spawn, &mut NoChecks);
    assert!(
        report
            .picks
            .iter()
            .any(|p| p.identity == "advance-time:2000000000"),
        "the exchange timed out"
    );
}

#[test]
#[ignore = "rewrites the committed input; needs fixtures/discovery-wait-complete-revert.patch applied"]
fn regenerate_wait_complete_input() {
    let bytes = hold_source_back().expect("the reverted code misses the wakeup");
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/discovery-wait-complete.bin"
    );
    std::fs::write(path, &bytes).expect("the fixture directory exists");
}

/// Asserts `report` ended cleanly, held the property at least once, and
/// never timed out the exchange.
fn assert_wakes(report: &RunReport) {
    assert_ne!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
    assert_eq!(report.cleanup.leaked, 0, "shutdown leaked no task");
    assert_eq!(report.end.violation, None);
    assert!(
        matches!(report.properties[0].outcome, PropertyOutcome::Held { applicable } if applicable > 0),
        "the run reached the point the property checks: {:?}",
        report.properties
    );
    assert!(
        !report
            .picks
            .iter()
            .any(|p| p.identity == "advance-time:2000000000"),
        "the exchange completed before its timeout"
    );
}

#[test]
fn committed_input_wakes() {
    let report = run_wait_complete(Plan::from_fuzz(INPUT));
    assert_wakes(&report);
}

#[test]
fn holding_the_source_back_finds_no_violation() {
    let bytes = hold_source_back().expect_err("the fix wakes the source");
    let (report, _, _) = run(Plan::from_fuzz(&bytes));
    assert_wakes(&report);
}

#[test]
fn seed_sweep_wakes() {
    for plan in inputs(200, 64) {
        assert_wakes(&run_wait_complete(plan));
    }
}
