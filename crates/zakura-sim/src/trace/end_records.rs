//! The end-record hook (mutation plan, mut-7.3).
//!
//! A mutation runner runs a witness test and must learn which property its
//! runs violated. With [`END_RECORDS_VAR`] naming a directory, every run
//! writes its `end` record there, and every witness judgement writes its
//! result, each as one JSON file. With the variable unset or empty, nothing
//! is written. The runner reads these files and never reads the test's
//! output.
//!
//! A file is named `<pid>-<seq>.json`, where `seq` counts the files this
//! process wrote, so the files sort in the order each process wrote them.
//! Each file holds one object with `seq`, `thread` (the writing thread's
//! ID), `source` (the build's source digest, as
//! [`super::BuildInfo::source`] gives it), and
//! either `end`, the run's end record as a trace writes it, or `witness`,
//! one judgement ([`WitnessRecord`]). A witness test judges its run after
//! the run ends, so its `witness` file follows the run's `end` file on the
//! same thread.
//!
//! A caller that runs many runs and wants only one of their records, such
//! as `zakura-sim generate`, takes a [`hold`] for the length of its search.
//! While any hold lives, [`dir`] names no directory, so no run writes a
//! record. The caller then writes the record it keeps with [`write_end_to`].

use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    thread,
};

use serde::Serialize;
use serde_json::{json, Value};

use crate::record::EndRecord;

/// The variable that names the directory end records go to.
pub const END_RECORDS_VAR: &str = "ZAKURA_SIM_END_RECORDS";

/// The next file's number within this process.
static NEXT: AtomicU64 = AtomicU64::new(0);

/// How many [`Hold`]s live in this process.
static HELD: AtomicUsize = AtomicUsize::new(0);

/// While it lives, no run in this process writes an end record or a
/// witness judgement to [`dir`] ([`hold`]).
#[derive(Debug)]
#[must_use = "the hook resumes as soon as the hold drops"]
pub struct Hold(());

impl Drop for Hold {
    /// Releases the hold.
    fn drop(&mut self) {
        HELD.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Stops every run in this process, on any thread, from writing records
/// until the returned [`Hold`] drops.
pub fn hold() -> Hold {
    HELD.fetch_add(1, Ordering::SeqCst);
    Hold(())
}

/// Whether the run reached the property's trigger history, as a witness
/// judgement found it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TriggerState {
    /// The run reached it.
    Reached,
    /// The run didn't reach it.
    Lost,
    /// The trigger history reads the view, and no view shows the run's end.
    NoView,
    /// The property has no trigger-history check.
    None,
}

/// One witness judgement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WitnessRecord {
    /// The judged property.
    pub property: String,
    /// What the test expected, as `zakura-sim record --expect` names it.
    pub expect: &'static str,
    /// Why the judgement failed, as [`crate::witness::WitnessError::kind`]
    /// names it, or `None` if it passed.
    pub error: Option<&'static str>,
    /// Whether the run reached the property's trigger history, whatever
    /// the outcome the judgement read first.
    pub trigger_history: TriggerState,
}

/// The directory [`END_RECORDS_VAR`] names, or `None` when it's unset or
/// empty, or while a [`hold`] lives.
pub fn dir() -> Option<PathBuf> {
    if HELD.load(Ordering::SeqCst) > 0 {
        return None;
    }
    env::var_os(END_RECORDS_VAR)
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
}

/// Writes `end` to [`dir`], if the variable names one.
pub(crate) fn write_end(end: &EndRecord) {
    if let Some(dir) = dir() {
        write(&dir, "end", json!(end));
    }
}

/// Writes `end` to `dir`, whatever [`END_RECORDS_VAR`] says and whether or
/// not a [`hold`] lives, under the same name and form as [`write_end`].
///
/// # Panics
///
/// If the file can't be written, as [`write_end`].
pub fn write_end_to(dir: &Path, end: &EndRecord) {
    write(dir, "end", json!(end));
}

/// Writes `witness` to [`dir`], if the variable names one.
pub(crate) fn write_witness(witness: &WitnessRecord) {
    if let Some(dir) = dir() {
        write(&dir, "witness", json!(witness));
    }
}

/// Writes one file holding `value` under `key` into `dir`. It writes a
/// temporary file and renames it, so a reader never sees half a record.
///
/// # Panics
///
/// If the file can't be written. A runner that set the variable must not
/// read a missing record as a run that never happened.
fn write(dir: &Path, key: &str, value: Value) {
    let seq = NEXT.fetch_add(1, Ordering::SeqCst);
    let record = json!({
        "seq": seq,
        "thread": format!("{:?}", thread::current().id()),
        "source": env!("ZAKURA_SIM_BUILD_SOURCE"),
        key: value,
    });
    let name = format!("{}-{seq:010}", process::id());
    let written = fs::create_dir_all(dir)
        .and_then(|()| fs::write(dir.join(format!("{name}.tmp")), record.to_string()))
        .and_then(|()| {
            fs::rename(
                dir.join(format!("{name}.tmp")),
                dir.join(format!("{name}.json")),
            )
        });
    if let Err(error) = written {
        panic!(
            "{END_RECORDS_VAR} names {}, and its record can't be written: {error}",
            dir.display()
        );
    }
}
