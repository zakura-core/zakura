//! Each simulation run's begin and end records, for sim-7's acceptance
//! tool (the sim-7 plan's Rule 11: every run any tool makes, at any depth,
//! writes its own). They're written only when [`RUNS_VAR`] names a
//! directory, so an ordinary run pays one read of an unset variable per
//! process. Every harness run passes through
//! [`crate::harness::run_reading`] or, for a machine harness,
//! `harness::machine::run_checked`, which write them. Beside them,
//! [`crate::trace::end_records`] writes end records alone for the mutation
//! runner, which needs no begin, input or phase.

use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
};

use crate::sim::RunReport;

/// The variable naming the directory the records go to.
pub const RUNS_VAR: &str = "ZAKURA_SIM_ACCEPTANCE_RUNS";

/// The variable naming the phase a tool's child runs in, such as
/// `fuzz-target`, which each record carries. A run with none set is
/// `in-process`.
pub const PHASE_VAR: &str = "ZAKURA_SIM_ACCEPTANCE_PHASE";

/// [`RUNS_VAR`]'s directory, read once per process.
fn dir() -> Option<&'static Path> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| std::env::var_os(RUNS_VAR).map(PathBuf::from))
        .as_deref()
}

/// The variable carrying the acceptance task's binding (its run, index and
/// `task.json` digest), which each record repeats so a record from another
/// run or task is refused.
pub const BINDING_VAR: &str = "ZAKURA_SIM_ACCEPTANCE_BINDING";

/// [`BINDING_VAR`]'s JSON, read once per process.
fn binding() -> &'static serde_json::Value {
    static BINDING: OnceLock<serde_json::Value> = OnceLock::new();
    BINDING.get_or_init(|| {
        std::env::var(BINDING_VAR)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or(serde_json::Value::Null)
    })
}

/// [`PHASE_VAR`], read once per process.
fn phase() -> &'static str {
    static PHASE: OnceLock<String> = OnceLock::new();
    PHASE.get_or_init(|| std::env::var(PHASE_VAR).unwrap_or_else(|_| "in-process".to_owned()))
}

thread_local! {
    /// The SHA-256 of the full fuzz input the thread's current run is of,
    /// if its caller named one ([`with_input`]).
    static INPUT: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Runs `f` with `input`, a full fuzz input, as the input of the runs it
/// makes on this thread. Hashes nothing unless records are being written.
pub fn with_input<T>(input: &[u8], f: impl FnOnce() -> T) -> T {
    if dir().is_none() {
        return f();
    }
    let previous = INPUT.with(|slot| slot.replace(Some(crate::seed::sha256(input))));
    let out = f();
    INPUT.with(|slot| *slot.borrow_mut() = previous);
    out
}

/// One run's records: `<dir>/<pid>-<start>-<n>` with `.begin.json` or
/// `.end.json`, where `<start>` is when this process first wrote one, so a
/// reused process ID names other files.
pub(crate) struct Atom {
    /// The records' path without their suffix.
    stem: PathBuf,
    /// The record's identity fields: the harness, its input and the phase.
    identity: serde_json::Value,
}

/// Writes the begin record of a run of `harness` before it starts, if
/// records are being written. A begin with no end shows a run that never
/// finished. Panics if it can't write, so the row fails rather than missing
/// a run.
pub(crate) fn begin(harness: &str) -> Option<Atom> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    static START: OnceLock<u128> = OnceLock::new();
    let dir = dir()?;
    let start = START.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos())
    });
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let id = format!("{}-{start}-{n}", std::process::id());
    let atom = Atom {
        stem: dir.join(&id),
        identity: serde_json::json!({
            // Repeated inside, so a record copied under another run's name
            // is refused.
            "id": id,
            "harness": harness,
            "input_sha256": INPUT.with(|slot| slot.borrow().clone()),
            "phase": phase(),
            "binding": binding(),
        }),
    };
    write(&atom, "begin", &atom.identity);
    Some(atom)
}

/// Writes `atom`'s end record: its identity and the `end` record the run
/// wrote, cleanup included.
pub(crate) fn end(atom: Option<Atom>, report: &RunReport) {
    let Some(atom) = atom else { return };
    let mut record = atom.identity.clone();
    record["end"] =
        serde_json::to_value(report.end_record()).expect("an end record holds only plain data");
    write(&atom, "end", &record);
}

/// Writes `record` as `atom`'s new `<kind>.json`. A file already there,
/// another run's, panics rather than be overwritten.
fn write(atom: &Atom, kind: &str, record: &serde_json::Value) {
    use std::io::Write as _;
    let path = atom.stem.with_extension(format!("{kind}.json"));
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .and_then(|mut file| file.write_all(record.to_string().as_bytes()))
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
}
