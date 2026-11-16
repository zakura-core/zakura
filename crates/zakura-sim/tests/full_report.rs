//! C8: `record` keeps a run's full report beside its trace, every
//! violation and the harness's evidence snapshot, and `replay` sends its
//! own and refuses a recording whose report differs.
#![cfg(zakura_sim)]

use std::path::Path;

use serde_json::json;
use tempfile::TempDir;
use zakura_sim::{
    harness::{self, RecordInput},
    record::{FullReport, FULL_REPORT_FILE},
    trace::{replay, ReplayError, ReplayOptions, Trace, TraceReader, MAX_REPORT_BYTES},
    PropertyOutcome,
};

/// The planted race's property, which its violating runs break.
const RACE: &str = "race/lost-update";

/// Records the registered harness `name` on the full fuzz `input`.
fn record_named(name: &str, input: &[u8]) -> TempDir {
    let dir = TempDir::new().unwrap();
    harness::record(
        harness::find(name).unwrap(),
        RecordInput::Input(input),
        dir.path(),
    )
    .unwrap();
    dir
}

/// The complete trace in `dir`.
fn open(dir: &Path) -> Trace {
    TraceReader::open(dir).unwrap()
}

/// The planted race's input that ends in its violation.
fn violating_input() -> Vec<u8> {
    // Variant 0 is the planted race, and an odd byte 1 gives two workers.
    for seed in 0u8..64 {
        let input: Vec<u8> = [0, 1]
            .into_iter()
            .chain((0..32u8).map(|i| seed.wrapping_mul(31).wrapping_add(i.wrapping_mul(seed | 1))))
            .collect();
        let dir = record_named(RACE, &input);
        if open(dir.path()).end.unwrap().violation.is_some() {
            return input;
        }
    }
    panic!("some input makes the planted race fire");
}

/// The full report `record` wrote beside the trace in `dir`.
fn full_report(dir: &Path) -> FullReport {
    let text =
        std::fs::read_to_string(dir.join(FULL_REPORT_FILE)).expect("record writes the report");
    serde_json::from_str(&text).expect("the report reads back")
}

/// Rewrites the full report beside the trace in `dir` through `edit`.
fn edit_report(dir: &Path, edit: impl FnOnce(&mut FullReport)) {
    let mut report = full_report(dir);
    edit(&mut report);
    std::fs::write(dir.join(FULL_REPORT_FILE), report.to_line()).unwrap();
}

/// The field a replay of the trace in `dir` found different, which must be
/// a nondeterminism.
fn refused(dir: &Path) -> &'static str {
    match replay(&open(dir), ReplayOptions::default()) {
        Err(ReplayError::Nondeterministic { field, .. }) => field,
        other => panic!("expected Nondeterministic, got {other:?}"),
    }
}

/// `record` writes the run's full report beside the trace: the violation,
/// which the `end` record leaves out, and the harness's snapshot. A replay
/// sends its own and matches it, and is refused when the recorded report
/// differs only in a second violation or in the snapshot.
#[test]
fn record_keeps_the_full_report_and_replay_matches_it() {
    let input = violating_input();
    let dir = record_named(RACE, &input);
    let recorded = full_report(dir.path());
    let race = recorded
        .properties
        .iter()
        .find(|found| found.property == RACE)
        .expect("the report has every property");
    assert!(
        matches!(race.outcome, PropertyOutcome::Violated { .. }),
        "{race:?}"
    );
    let end = open(dir.path()).end.unwrap();
    assert_eq!(race.violations.len(), 1);
    let stopped = end.violation.expect("the race ended the run");
    assert_eq!(race.violations[0].step, stopped.step);
    assert_eq!(recorded.snapshot, None);
    let replayed = replay(&open(dir.path()), ReplayOptions::default()).unwrap();
    assert!(replayed.verified(), "{replayed:?}");
    assert_eq!(replayed.report.as_ref(), Some(&recorded));

    edit_report(dir.path(), |report| {
        let race = report
            .properties
            .iter_mut()
            .find(|found| found.property == RACE)
            .unwrap();
        let again = race.violations[0].clone();
        race.violations.push(again);
    });
    assert_eq!(refused(dir.path()), "report.properties");

    let dir = record_named(RACE, &input);
    edit_report(dir.path(), |report| {
        report.snapshot = Some(json!({ "stored": [1] }));
    });
    assert_eq!(refused(dir.path()), "report.snapshot");

    let dir = record_named(RACE, &input);
    std::fs::write(dir.path().join(FULL_REPORT_FILE), "not a report\n").unwrap();
    assert_eq!(refused(dir.path()), "report");
}

/// A machine harness's trace keeps its full report too, with the outcomes
/// its `end` record holds.
#[test]
fn a_machine_trace_keeps_its_full_report() {
    let dir = record_named("discovery/record-book", &[0, 1, 2, 3, 4, 5, 6, 7]);
    let report = full_report(dir.path());
    let end = open(dir.path()).end.unwrap();
    let outcomes: Vec<(&str, &PropertyOutcome)> = report
        .properties
        .iter()
        .map(|found| (found.property.as_str(), &found.outcome))
        .collect();
    let ended: Vec<(&str, &PropertyOutcome)> = end
        .properties
        .iter()
        .map(|found| (found.property.as_str(), &found.outcome))
        .collect();
    assert_eq!(outcomes, ended);
    assert!(outcomes
        .iter()
        .any(|(property, _)| *property == "discovery/record-book"));
}

/// A recording never overwrites a full report already beside the trace, nor
/// writes through a symlink there: it refuses, and the file or the link's
/// target keeps its bytes.
#[test]
fn record_refuses_an_existing_full_report() {
    let input = violating_input();
    let entry = harness::find(RACE).unwrap();
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join(FULL_REPORT_FILE), "kept\n").unwrap();
    let result = harness::record(entry, RecordInput::Input(&input), dir.path());
    assert!(result.is_err(), "{result:?}");
    assert_eq!(
        std::fs::read_to_string(dir.path().join(FULL_REPORT_FILE)).unwrap(),
        "kept\n"
    );

    let dir = TempDir::new().unwrap();
    let target = dir.path().join("elsewhere");
    std::fs::write(&target, "kept\n").unwrap();
    std::os::unix::fs::symlink(&target, dir.path().join(FULL_REPORT_FILE)).unwrap();
    let result = harness::record(entry, RecordInput::Input(&input), dir.path());
    assert!(result.is_err(), "{result:?}");
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "kept\n");
}

/// A replay reads the recorded report only as a regular file within
/// `MAX_REPORT_BYTES`: a FIFO there, which would block an open, a symlink
/// and an oversized file each differ in `report` at once.
#[test]
fn replay_refuses_a_report_that_isnt_a_bounded_regular_file() {
    let input = violating_input();

    let dir = record_named(RACE, &input);
    let path = dir.path().join(FULL_REPORT_FILE);
    std::fs::remove_file(&path).unwrap();
    let made = std::process::Command::new("mkfifo")
        .arg(&path)
        .status()
        .unwrap();
    assert!(made.success(), "{made}");
    assert_eq!(refused(dir.path()), "report");

    let dir = record_named(RACE, &input);
    let path = dir.path().join(FULL_REPORT_FILE);
    let target = dir.path().join("elsewhere");
    std::fs::rename(&path, &target).unwrap();
    std::os::unix::fs::symlink(&target, &path).unwrap();
    assert_eq!(refused(dir.path()), "report");

    let dir = record_named(RACE, &input);
    std::fs::OpenOptions::new()
        .write(true)
        .open(dir.path().join(FULL_REPORT_FILE))
        .unwrap()
        .set_len(MAX_REPORT_BYTES + 1)
        .unwrap();
    assert_eq!(refused(dir.path()), "report");
}

/// The sidecar keeps how many of a property's violations the run's end
/// found, which the nested judge reads, and a replay refuses a recording
/// whose count differs.
#[test]
fn the_full_report_keeps_end_found() {
    let dir = record_named(RACE, &violating_input());
    let recorded = full_report(dir.path());
    assert!(recorded.properties.iter().all(|found| found.end_found == 0));
    edit_report(dir.path(), |report| {
        let race = report
            .properties
            .iter_mut()
            .find(|found| found.property == RACE)
            .unwrap();
        race.end_found = 1;
    });
    assert_eq!(refused(dir.path()), "report.properties");
}
