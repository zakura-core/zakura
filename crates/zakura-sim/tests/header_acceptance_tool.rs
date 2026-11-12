//! The acceptance tool's integration (hdr-14.7b): gate 1's A/A and A/B/A
//! comparison and its replay store control.
//!
//! Recordings and replays here go through the `zakura-sim` binary, as the
//! acceptance runner makes them, and land at the paths `tool`'s artifact
//! contract gives them.
#![cfg(zakura_sim)]

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;
use zakura_sim::{
    harness::{
        self,
        header_acceptance::{
            contracts::RunBy,
            evaluate::Actual,
            sim_test,
            tool::{self, ReplayRun, Which, FULL_REPORT_FILE, REPLAY_DIR, REPLAY_FILE},
            HistoryRef, RunId,
        },
        RecordInput,
    },
    record::FullReport,
};

/// The committed fixture `name`, a full fuzz input.
fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// Runs the `zakura-sim` binary with `args`, returning whether it exited 0
/// and its combined output.
fn sim(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs");
    let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&output.stderr));
    (output.status.success(), log)
}

/// Writes `dir`'s fresh replay as the runner does: a separate `zakura-sim
/// replay --report` child, and its end as [`REPLAY_FILE`].
fn replay(dir: &Path) {
    let replay = dir.join(REPLAY_DIR);
    std::fs::create_dir_all(&replay).expect("the temp dir is writable");
    let report = replay.join(FULL_REPORT_FILE);
    let (exited_ok, log) = sim(&[
        "replay",
        dir.to_str().expect("the path is UTF-8"),
        "--report",
        report.to_str().expect("the path is UTF-8"),
    ]);
    let ran = ReplayRun {
        exited_ok,
        killed: false,
        log,
    };
    std::fs::write(
        replay.join(REPLAY_FILE),
        serde_json::to_string(&ran).expect("it serializes"),
    )
    .expect("the temp dir is writable");
}

/// Reads and rewrites the full report at `path` through `edit`.
fn edit_report(path: &Path, edit: impl FnOnce(&mut FullReport)) {
    let mut report: FullReport =
        serde_json::from_str(&std::fs::read_to_string(path).expect("the report exists"))
            .expect("it reads");
    edit(&mut report);
    std::fs::write(path, report.to_line()).expect("the temp dir is writable");
}

/// Changes one stored fact in `report`'s snapshot: the first store with a
/// non-empty snapshot gets one more character. Every outcome and violation
/// stays as it was.
fn change_one_fact(report: &mut FullReport) {
    let stores = report
        .snapshot
        .as_mut()
        .and_then(Value::as_array_mut)
        .expect("a node run keeps its stores");
    let store = stores
        .iter_mut()
        .find(|store| {
            store["snapshot"]
                .as_str()
                .is_some_and(|text| text.len() > 2)
        })
        .expect("some store holds a fact");
    let text = store["snapshot"].as_str().expect("found as a string");
    store["snapshot"] = Value::String(format!("{text} "));
}

/// The exact, reports-equal and stores-equal flags of a replay read back.
fn flags(actual: Actual) -> (bool, bool, bool, bool) {
    match actual {
        Actual::Replay {
            exact,
            reports_equal,
            stores_equal,
            fresh,
            ..
        } => (exact, reports_equal, stores_equal, fresh),
        other => panic!("{other:?}"),
    }
}

/// A history identity for the fixtures.
fn history() -> RunId {
    HistoryRef {
        harness: "header-sync/storage",
        witness: sim_test("tests/t.rs", "t"),
        case: "only",
    }
    .into()
}

/// Gate 1's replay store control: a recorded node run replays fresh with
/// its reports and stores equal, and a changed stored fact with every
/// report the same fails the comparison. Changed in the replay's report,
/// only the stores differ. Changed in the recording's, the replay itself
/// refuses the trace, so it isn't exact either.
#[test]
fn a_changed_stored_fact_fails_the_replay_comparison() {
    let input = fixture("header-storage-close-relink.bin");
    let recorded = |dir: &Path| {
        let (ok, log) = sim(&[
            "record",
            "--sim",
            "header-sync/storage",
            "--input",
            input.to_str().expect("the path is UTF-8"),
            "--out",
            dir.to_str().expect("the path is UTF-8"),
        ]);
        assert!(ok, "{log}");
    };
    let dir = tempfile::tempdir().expect("a temp dir can be created");
    recorded(dir.path());
    replay(dir.path());
    let read = |dir: &Path| flags(tool::read_replay(dir, history(), RunBy::Tool));
    assert_eq!(read(dir.path()), (true, true, true, true));

    edit_report(
        &dir.path().join(REPLAY_DIR).join(FULL_REPORT_FILE),
        change_one_fact,
    );
    assert_eq!(read(dir.path()), (true, true, false, true));

    let dir = tempfile::tempdir().expect("a temp dir can be created");
    recorded(dir.path());
    edit_report(&dir.path().join(FULL_REPORT_FILE), change_one_fact);
    replay(dir.path());
    let (exact, _, stores_equal, _) = read(dir.path());
    assert!(!exact && !stores_equal);
}

/// Every committed fixture of `harness`, sorted.
fn fixtures_of(harness: &str) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(fixture(""))
        .expect("the fixtures directory is readable")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
        .filter(|path| {
            std::fs::read_to_string(path)
                .ok()
                .and_then(|text| toml::from_str::<toml::Table>(&text).ok())
                .is_some_and(|table| {
                    table.get("harness").and_then(toml::Value::as_str) == Some(harness)
                })
        })
        .map(|path| path.with_extension("bin"))
        .filter(|path| path.exists())
        .collect();
    found.sort();
    found
}

/// Records `input` on `harness` into `dir`, in this process.
fn record_in_process(harness: &str, input: &[u8], dir: &Path) -> Result<(), String> {
    let entry = harness::find(harness).map_err(|error| error.to_string())?;
    harness::record(entry, RecordInput::Input(input), dir)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Gate 1's A/A and A/B/A comparison: for each header harness, its first
/// committed history A runs twice and again after a distinct B in this one
/// process, with the same trace, full report and store snapshot each time.
/// A harness with one committed history takes the empty input as B. Its controls: a run with no full report is a missing
/// comparison, an A result changed only after B fails, and a B that
/// resolves to A's execution fails.
#[test]
fn each_history_is_the_same_alone_and_after_another() {
    for harness in [
        "header-sync/storage",
        "header-sync/wire",
        "header-sync/release-wake",
        "header-sync/repair",
        "header/reactor-policy",
        "header/publisher-policy",
    ] {
        let fixtures = fixtures_of(harness);
        let a = std::fs::read(&fixtures[0]).expect("a fixture is readable");
        let b = match fixtures.get(1) {
            Some(path) => std::fs::read(path).expect("a fixture is readable"),
            None => Vec::new(),
        };
        let root = tempfile::tempdir().expect("a temp dir can be created");
        let compared = tool::a_a_and_a_b_a(root.path(), &mut |which, dir| {
            record_in_process(harness, if which == Which::A { &a } else { &b }, dir)
        });
        assert_eq!(compared, Ok(()), "{harness}");
    }

    let a = std::fs::read(fixture("header-storage-close-relink.bin")).expect("it's readable");
    let b = std::fs::read(fixture("header-storage-default.bin")).expect("it's readable");
    let storage = "header-sync/storage";
    let control = |change: &dyn Fn(Which, usize, &Path)| {
        let root = tempfile::tempdir().expect("a temp dir can be created");
        let mut runs = 0;
        tool::a_a_and_a_b_a(root.path(), &mut |which, dir| {
            record_in_process(storage, if which == Which::A { &a } else { &b }, dir)?;
            change(which, runs, dir);
            runs += 1;
            Ok(())
        })
    };
    // No full report beside A's second run.
    let missing = control(&|_, run, dir| {
        if run == 1 {
            std::fs::remove_file(dir.join(FULL_REPORT_FILE)).expect("it exists");
        }
    });
    assert!(
        missing
            .as_ref()
            .is_err_and(|why| why.contains("missing comparison")),
        "{missing:?}"
    );
    // One stored fact of A's run after B changed.
    let after = control(&|_, run, dir| {
        if run == 3 {
            edit_report(&dir.join(FULL_REPORT_FILE), change_one_fact);
        }
    });
    assert!(
        after.as_ref().is_err_and(|why| why.contains("after B")),
        "{after:?}"
    );
    // B recorded from A's input.
    let root = tempfile::tempdir().expect("a temp dir can be created");
    let same = tool::a_a_and_a_b_a(root.path(), &mut |_, dir| {
        record_in_process(storage, &a, dir)
    });
    assert!(
        same.as_ref()
            .is_err_and(|why| why.contains("B resolves to A")),
        "{same:?}"
    );
}
