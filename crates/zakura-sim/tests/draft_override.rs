//! C9: a `Draft` override is run configuration that `record` writes into
//! `meta` and `replay` applies again, so a planted case's `Draft` run, which
//! goes on past its first violation, replays Exact.
#![cfg(zakura_sim)]

use std::{path::Path, process::Command};

use serde_json::Value;
use tempfile::TempDir;
use zakura_sim::{
    harness::{self, RecordInput},
    property::Status,
    trace::{
        replay, shrink, ReplayError, ReplayOptions, ReplayReport, ShrinkBudget, Trace, TraceReader,
        TRACE_FILE,
    },
    PropertyOutcome,
};

/// Marks a verified replay in the child's output.
const VERIFIED: &str = "zakura-sim-draft-verified";

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

/// Replays the trace in `dir` with `options`, on the harness `meta.sim`
/// names.
fn replayed(dir: &Path, options: ReplayOptions) -> Result<ReplayReport, ReplayError> {
    replay(&open(dir), options)
}

/// Rewrites `dir`'s `trace.jsonl` through `edit`, one JSON value per line.
fn edit(dir: &Path, edit: impl FnOnce(&mut Vec<Value>)) {
    let path = dir.join(TRACE_FILE);
    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    edit(&mut lines);
    let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
    std::fs::write(path, text).unwrap();
}

/// A run of `race/lost-update` that ends in a violation.
fn violating_race() -> TempDir {
    // Variant 0 is the planted race, and an odd byte 1 gives two workers.
    for seed in 0u8..64 {
        let input: Vec<u8> = [0, 1]
            .into_iter()
            .chain((0..32u8).map(|i| seed.wrapping_mul(31).wrapping_add(i.wrapping_mul(seed | 1))))
            .collect();
        let dir = record_named("race/lost-update", &input);
        if open(dir.path()).end.unwrap().violation.is_some() {
            return dir;
        }
    }
    panic!("some input makes the planted race fire");
}

/// Names the `Draft` trace that [`draft_replay_child`] replays.
const DRAFT_CHILD: &str = "ZAKURA_SIM_DRAFT_TRACE";

/// Replays the trace that [`DRAFT_CHILD`] names when run as the child, and
/// does nothing otherwise.
#[test]
fn draft_replay_child() {
    if let Some(dir) = std::env::var_os(DRAFT_CHILD) {
        let report =
            replayed(Path::new(&dir), ReplayOptions::default()).expect("the Draft trace replays");
        assert!(report.verified(), "{report:?}");
        #[allow(
            clippy::print_stdout,
            reason = "the parent test reads the child's stdout"
        )]
        {
            println!("\n{VERIFIED}");
        }
    }
}

/// C9: a `Draft` override is run configuration. Recorded under the
/// override, the planted race's run goes past its violation to its end, and
/// its meta names the override and the property's `draft` status. The trace
/// replays Exact in a fresh process, which applies `meta.draft` as `record`
/// did. With the override taken out of the meta, the harness no longer
/// describes that scenario, and an override naming an ID the harness
/// doesn't register is refused before anything is written.
#[test]
fn a_draft_override_records_and_replays() {
    const RACE: &str = "race/lost-update";
    let active = violating_race();
    let input = std::fs::read(active.path().join("input.bin")).unwrap();
    assert!(open(active.path()).end.unwrap().violation.is_some());

    let entry = harness::find(RACE).unwrap();
    let draft = [RACE.to_owned()];
    let dir = TempDir::new().unwrap();
    harness::with_draft(&draft, || {
        harness::record(entry, RecordInput::Input(&input), dir.path())
    })
    .unwrap();
    let trace = open(dir.path());
    assert_eq!(trace.meta.draft, draft);
    let status = trace
        .meta
        .properties
        .iter()
        .find(|property| property.id == RACE)
        .map(|property| property.status);
    assert_eq!(status, Some(Status::Draft));
    let end = trace.end.unwrap();
    assert_eq!(
        end.violation, None,
        "the Draft run goes on past its violation"
    );
    let outcome = end
        .properties
        .iter()
        .find(|report| report.property == RACE)
        .map(|report| &report.outcome);
    assert!(
        matches!(outcome, Some(PropertyOutcome::Violated { .. })),
        "{outcome:?}"
    );
    assert!(harness::draft().is_empty(), "the override ends with record");

    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "draft_replay_child", "--nocapture"])
        .env(DRAFT_CHILD, dir.path())
        .output()
        .expect("the test binary reruns itself");
    assert!(output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(VERIFIED),
        "{output:?}"
    );

    edit(dir.path(), |lines| {
        lines[0]["meta"].as_object_mut().unwrap().remove("draft");
    });
    assert!(
        matches!(
            replayed(dir.path(), ReplayOptions::default()),
            Err(ReplayError::UnsupportedScenario { .. })
        ),
        "a trace that lost its override isn't the harness's scenario"
    );

    let refused = TempDir::new().unwrap();
    let result = harness::with_draft(&["race/no-such-property".to_owned()], || {
        harness::record(entry, RecordInput::Input(&input), refused.path())
    });
    assert!(
        matches!(result, Err(harness::RecordError::UnregisteredDraft { .. })),
        "{result:?}"
    );
    assert!(!refused.path().join(TRACE_FILE).exists());
}

/// C9: shrinking a `Draft` trace keeps its run configuration. Every
/// candidate runs under `meta.draft`, so the reduced trace goes past its
/// violation as the original did, and its meta names the same override.
#[test]
fn shrinking_a_draft_trace_keeps_the_override() {
    const RACE: &str = "race/lost-update";
    let input = std::fs::read(violating_race().path().join("input.bin")).unwrap();
    let draft = [RACE.to_owned()];
    let dir = TempDir::new().unwrap();
    harness::with_draft(&draft, || {
        harness::record(
            harness::find(RACE).unwrap(),
            RecordInput::Input(&input),
            dir.path(),
        )
    })
    .unwrap();
    let out = TempDir::new().unwrap();
    shrink::shrink(
        &open(dir.path()),
        ShrinkBudget {
            candidates: 20,
            ..ShrinkBudget::default()
        },
        out.path(),
    )
    .expect("the Draft trace shrinks");
    let reduced = open(&out.path().join(shrink::REDUCED_DIR));
    assert_eq!(reduced.meta.draft, draft);
    let end = reduced.end.unwrap();
    assert_eq!(
        end.violation, None,
        "the reduced run goes past its violation"
    );
    assert!(end.properties.iter().any(|report| report.property == RACE
        && matches!(report.outcome, PropertyOutcome::Violated { .. })));
}
