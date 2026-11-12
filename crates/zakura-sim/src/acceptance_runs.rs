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

/// What a run's caller says about it: the full fuzz input it's of and the
/// role it plays in its tool's step, such as `witness` inside `zakura-sim
/// confirm`. It travels with the run onto any thread the run is moved to
/// ([`current`], [`within`]).
#[derive(Clone, Debug, Default)]
pub struct Context {
    /// The full input's SHA-256, if a caller named one.
    input: Option<String>,
    /// The role, if a caller named one.
    role: Option<String>,
}

thread_local! {
    /// The thread's current [`Context`].
    static CONTEXT: RefCell<Context> = RefCell::new(Context::default());
}

/// The thread's current context, to carry onto a thread a run moves to.
pub fn current() -> Context {
    CONTEXT.with(|slot| slot.borrow().clone())
}

/// Runs `f` with `context` as the thread's, restoring the old one after.
pub fn within<T>(context: Context, f: impl FnOnce() -> T) -> T {
    let previous = CONTEXT.with(|slot| slot.replace(context));
    let out = f();
    CONTEXT.with(|slot| *slot.borrow_mut() = previous);
    out
}

/// Runs `f` with `input`, a full fuzz input, as the input of the runs it
/// makes. Hashes nothing unless records are being written.
pub fn with_input<T>(input: &[u8], f: impl FnOnce() -> T) -> T {
    if dir().is_none() {
        return f();
    }
    let context = Context {
        input: Some(crate::seed::sha256(input)),
        ..current()
    };
    within(context, f)
}

/// Runs `f` with the input whose SHA-256 is `sha256` as the input of the
/// runs it makes, for a replay, which knows its input by its trace's digest
/// rather than its bytes.
pub fn with_input_digest<T>(sha256: Option<&str>, f: impl FnOnce() -> T) -> T {
    if dir().is_none() {
        return f();
    }
    let context = Context {
        input: sha256.map(str::to_owned),
        ..current()
    };
    within(context, f)
}

/// Runs `f` with `role` as the role of the runs it makes.
pub fn with_role<T>(role: &str, f: impl FnOnce() -> T) -> T {
    if dir().is_none() {
        return f();
    }
    let context = Context {
        role: Some(role.to_owned()),
        ..current()
    };
    within(context, f)
}

/// A run's full identity, which both its records carry: its id (its
/// records' file name, so a copy under another name is refused), harness,
/// input, the tool's phase, its role in that phase, and the acceptance
/// task's binding. One type, written here and read by the acceptance
/// evaluator.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunIdentity {
    /// `<pid>-<start>-<n>`, the records' file stem.
    pub id: String,
    /// The harness.
    pub harness: String,
    /// The full input's SHA-256, where a caller named it.
    pub input_sha256: Option<String>,
    /// The tool's phase ([`PHASE_VAR`]), `in-process` when none is set.
    pub phase: String,
    /// The run's role in that phase, where its caller named one.
    pub role: Option<String>,
    /// The acceptance task's binding ([`BINDING_VAR`]).
    pub binding: serde_json::Value,
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
    let context = current();
    let identity = RunIdentity {
        id: id.clone(),
        harness: harness.to_owned(),
        input_sha256: context.input,
        phase: phase().to_owned(),
        role: context.role,
        binding: binding().clone(),
    };
    let atom = Atom {
        stem: dir.join(&id),
        identity: serde_json::to_value(&identity).expect("an identity is plain data"),
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
