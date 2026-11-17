//! Keeping a failing input on disk before anything reruns it (explorer
//! plan, Result contract).
//!
//! A rerun can hang, crash the process, or be killed with it. So the fuzz
//! target saves the original input first, by its digest, and only then
//! records the failure. Inputs are grouped by bucket and property, and each
//! distinct input keeps its own file, so a property's first saved failure
//! never stops a later input from being kept. An input whose rerun disagrees
//! moves to the `nondeterministic` bucket, so it's filed only once.

use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use sha2::{Digest, Sha256};

use super::{Bucket, ExploreRun};
use crate::{
    harness::{HarnessEntry, RecordError},
    property::PropertyId,
    trace::{meta::hex, record_failure, Trace},
};

/// The directory under the artifacts root that holds saved inputs.
pub const INPUTS_DIR: &str = "inputs";

/// The temporary file's number within this process.
static NEXT_PARTIAL: AtomicU64 = AtomicU64::new(0);

/// Where [`save_input`] saves `input`:
/// `<root>/inputs/<bucket>/[<property>/]<sha256>.bin`, with each segment of
/// `property` a directory.
pub fn input_path(
    root: &Path,
    bucket: Bucket,
    property: Option<&PropertyId>,
    input: &[u8],
) -> PathBuf {
    let dir = property
        .into_iter()
        .flat_map(|property| property.as_str().split('/'))
        .fold(root.join(INPUTS_DIR).join(bucket.name()), |dir, segment| {
            dir.join(segment)
        });
    dir.join(format!("{}.bin", hex(&Sha256::digest(input))))
}

/// Saves `input` at [`input_path`], unless a file is already there, and
/// returns the path.
///
/// The bytes go to a temporary file in the same directory, which is synced
/// and then renamed into place. A process killed during the save leaves no
/// partial file under the final name.
pub fn save_input(
    root: &Path,
    bucket: Bucket,
    property: Option<&PropertyId>,
    input: &[u8],
) -> io::Result<PathBuf> {
    let path = input_path(root, bucket, property, input);
    if fs::symlink_metadata(&path).is_ok() {
        return Ok(path);
    }
    let dir = path
        .parent()
        .expect("input_path names a file inside a directory");
    fs::create_dir_all(dir)?;
    let partial = dir.join(format!(
        ".partial-{}-{}",
        process::id(),
        NEXT_PARTIAL.fetch_add(1, Ordering::Relaxed)
    ));
    let written = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial)?;
        file.write_all(input)?;
        file.sync_all()?;
        fs::rename(&partial, &path)
    })();
    if let Err(error) = written {
        // The write's error matters more than a failed cleanup.
        let _ = fs::remove_file(&partial);
        return Err(error);
    }
    Ok(path)
}

/// The root [`save_failure`] records `bucket`'s traces under: `root` for
/// `active`, else `root/<bucket>`.
pub fn traces_root(root: &Path, bucket: Bucket) -> PathBuf {
    match bucket {
        Bucket::Active => root.to_owned(),
        bucket => root.join(bucket.name()),
    }
}

/// A failure [`save_failure`] saved.
#[derive(Debug)]
pub struct SavedFailure {
    /// The saved input.
    pub input: PathBuf,
    /// The recorded trace, or why `record_failure` kept none.
    pub trace: Result<Trace, RecordError>,
}

/// Saves `input` with [`save_input`] under `bucket` and `property`, then
/// records `property`'s violation in `run` with [`record_failure`].
///
/// The input is on disk before either rerun starts, so a rerun that hangs
/// or crashes the process can't lose it. An `Active` violation's trace goes
/// under `root`, and a `Known` or `Draft` one's under `root/<bucket>`. If a
/// rerun disagrees, the input moves to the `nondeterministic` bucket, since
/// nondeterminism isn't a property failure. If that move fails, the input
/// stays where it was saved.
///
/// Returns an error only if the input couldn't be saved, in which case
/// nothing reran.
pub fn save_failure(
    entry: &'static HarnessEntry,
    input: &[u8],
    run: &ExploreRun,
    property: &PropertyId,
    bucket: Bucket,
    root: &Path,
) -> io::Result<SavedFailure> {
    let saved = save_input(root, bucket, Some(property), input)?;
    let scenario = run
        .scenario
        .as_ref()
        .expect("run_bytes keeps the scenario of a run that reports a violation");
    let report = run
        .report()
        .expect("a run that reports a violation has a report");
    let trace = record_failure(
        entry,
        scenario,
        input,
        property,
        report,
        &traces_root(root, bucket),
    );
    let saved = match &trace {
        Err(RecordError::Nondeterministic { .. }) => {
            move_input(&saved, root, Bucket::Nondeterministic, property, input).unwrap_or(saved)
        }
        _ => saved,
    };
    Ok(SavedFailure {
        input: saved,
        trace,
    })
}

/// Moves the input saved at `from` to its [`input_path`] under `bucket`,
/// and returns the new path.
fn move_input(
    from: &Path,
    root: &Path,
    bucket: Bucket,
    property: &PropertyId,
    input: &[u8],
) -> io::Result<PathBuf> {
    let to = input_path(root, bucket, Some(property), input);
    fs::create_dir_all(
        to.parent()
            .expect("input_path names a file inside a directory"),
    )?;
    fs::rename(from, &to)?;
    Ok(to)
}
