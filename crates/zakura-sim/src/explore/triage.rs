//! Confirming a saved fuzz input's failure in fresh processes, and archiving
//! the input with its evidence (explorer plan, E4a).
//!
//! [`triage`] takes one input, such as a crash file libFuzzer saved or an
//! input the fuzz target kept under `inputs/`, and:
//!
//! 1. copies the input into a staging bundle under `<out>/.staging/`, and
//!    syncs it, before any child starts;
//! 2. records the input twice, each time in a fresh child process with a
//!    time limit, and classifies each child's run with [`classify_process`].
//!    A child that outlives its limit is killed, and its run classifies with
//!    [`ProcessExit::TimedOut`];
//! 3. if both runs filed the input alike and wrote their `end` record,
//!    replays the first run's trace Exact in a third fresh child;
//! 4. writes [`TRIAGE_FILE`] and [`CHECKSUMS_FILE`], and renames the bundle
//!    into place.
//!
//! A bundle never changes after its rename. A later triage that files the
//! same input in the same place keeps the bundle that's there. The input is
//! only read, so a triage killed at any point leaves it whole, and leaves
//! its copy in the staging bundle.
//!
//! Bundles go under `<out>/<bucket>/<signature hash>/<input sha256>/` for a
//! [`TriageStatus::Confirmed`] failure, and under
//! `<out>/<status>/<input sha256>/` for every other status. Each distinct
//! input keeps its own bundle, so a signature's first bundle never stops a
//! later input from being kept.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write as _},
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    process::{self, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{
    classify_process, Bucket, ExploreOutcome, ProcessClassified, ProcessExit, ProcessFailure,
    ProcessResult,
};
use crate::{
    harness::HarnessEntry,
    trace::{
        meta::{hex, BuildInfo},
        MAX_INPUT_BYTES,
    },
};

/// The staging directory under the archive root. A triage that was killed
/// leaves its partial bundle there.
pub const STAGING_DIR: &str = ".staging";

/// The bundle's copy of the input.
pub const BUNDLE_INPUT_FILE: &str = "input.bin";

/// The scenario the input's prefix draws, as JSON.
pub const SCENARIO_FILE: &str = "scenario.json";

/// What triage found, as JSON.
pub const TRIAGE_FILE: &str = "triage.json";

/// The SHA-256 of every other file in the bundle, in `sha256sum` format.
pub const CHECKSUMS_FILE: &str = "SHA256SUMS";

/// The directory that holds each child's run.
pub const RUNS_DIR: &str = "runs";

/// A child's trace directory, inside its run directory.
pub const RUN_TRACE_DIR: &str = "trace";

/// A child's standard output, inside its run directory.
pub const STDOUT_FILE: &str = "stdout.txt";

/// A child's standard error, inside its run directory.
pub const STDERR_FILE: &str = "stderr.txt";

/// The run directories of the two recordings and the replay.
const RECORD_DIRS: [&str; 2] = ["record-1", "record-2"];
const REPLAY_DIR: &str = "replay";

/// [`TRIAGE_FILE`]'s format version.
pub const TRIAGE_FORMAT: u32 = 1;

/// How long one child may run by default.
pub const DEFAULT_CHILD_TIMEOUT: Duration = Duration::from_secs(60);

/// How often triage checks a running child.
const POLL: Duration = Duration::from_millis(20);

/// `SIGKILL` on Linux and macOS, the signal [`Child::kill`] sends.
///
/// [`Child::kill`]: std::process::Child::kill
const SIGKILL: i32 = 9;

/// The staging bundle's number within this process.
static NEXT_STAGING: AtomicU64 = AtomicU64::new(0);

/// The child processes triage starts.
pub trait Children {
    /// A command that records the full fuzz input in the file `input` and
    /// writes its trace to `out`, as `zakura-sim record --input` does.
    fn record(&self, input: &Path, out: &Path) -> Command;

    /// A command that replays the trace in `trace` Exact, and exits with
    /// code 0 only if every pick, draw and observation matched, as
    /// `zakura-sim replay` does.
    fn replay(&self, trace: &Path) -> Command;
}

/// The `zakura-sim` binary at `exe`, running harness `harness`.
#[derive(Clone, Debug)]
pub struct SimChildren {
    /// The binary.
    pub exe: PathBuf,
    /// The registered harness's name.
    pub harness: String,
}

impl Children for SimChildren {
    /// `zakura-sim record --sim <harness> --input <input> --out <out>`.
    fn record(&self, input: &Path, out: &Path) -> Command {
        let mut command = Command::new(&self.exe);
        command
            .args(["record", "--sim", &self.harness, "--input"])
            .arg(input)
            .arg("--out")
            .arg(out);
        command
    }

    /// `zakura-sim replay <trace>`.
    fn replay(&self, trace: &Path) -> Command {
        let mut command = Command::new(&self.exe);
        command.arg("replay").arg(trace);
        command
    }
}

/// What triage concluded about an input. Each status means one thing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TriageStatus {
    /// Both fresh recordings filed the input under the same buckets and
    /// signature, and that is a failure. If they wrote their `end` record, a
    /// fresh replay of the first one's trace matched it Exact.
    Confirmed,
    /// Both fresh recordings ran clean, and a fresh replay of the first one's
    /// trace matched it Exact.
    NotReproduced,
    /// The two fresh recordings filed the input differently.
    Nondeterministic,
    /// Both fresh recordings filed the input alike, but a fresh replay of the
    /// first one's trace didn't match it Exact.
    ReplayFailed,
}

impl TriageStatus {
    /// The status's name, as the JSON and the archive's directories write
    /// it.
    pub fn name(self) -> &'static str {
        match self {
            TriageStatus::Confirmed => "confirmed",
            TriageStatus::NotReproduced => "not-reproduced",
            TriageStatus::Nondeterministic => "nondeterministic",
            TriageStatus::ReplayFailed => "replay-failed",
        }
    }
}

/// Triage's limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TriageOptions {
    /// The longest one child may run before triage kills it.
    pub child_timeout: Duration,
}

impl Default for TriageOptions {
    /// [`DEFAULT_CHILD_TIMEOUT`].
    fn default() -> Self {
        TriageOptions {
            child_timeout: DEFAULT_CHILD_TIMEOUT,
        }
    }
}

/// One child's run, as triage saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildRun {
    /// How the child ended.
    pub exit: ProcessExit,
    /// Its run, classified.
    pub classified: ProcessClassified,
}

/// What [`triage`] did with one input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Triaged {
    /// What it concluded.
    pub status: TriageStatus,
    /// The bucket the bundle is filed under, for a confirmed failure.
    pub bucket: Option<Bucket>,
    /// The confirmed failure's signature.
    pub signature: Option<String>,
    /// The two recordings.
    pub records: [ChildRun; 2],
    /// The replay's exit, if it ran.
    pub replay: Option<ProcessExit>,
    /// The bundle's directory.
    pub bundle: PathBuf,
    /// False if a bundle for the same input was already in that place, so
    /// this triage kept it and archived nothing.
    pub archived: bool,
}

/// The bucket a classified run is filed under: [`ProcessClassified::bucket`],
/// else the first of [`ProcessClassified::buckets`], which is a `Known` or
/// `Draft` violation in a run whose outcome is clean. `None` for a clean
/// run.
pub fn primary_bucket(classified: &ProcessClassified) -> Option<Bucket> {
    classified
        .bucket()
        .or_else(|| classified.buckets().first().copied())
}

/// The signature of a classified run: what triage groups its inputs by
/// (explorer.md, Terms). `None` for a clean run.
///
/// It's the primary bucket's name, then what identifies the failure in that
/// bucket: the property ID for a violation, the reason for a harness
/// failure, and the kind of a process failure. A sanitizer report keeps its
/// words up to the first that holds a digit, so a process ID or an address
/// doesn't split one failure.
pub fn signature(classified: &ProcessClassified) -> Option<String> {
    let failure = |failure: &ProcessFailure| match failure {
        ProcessFailure::Timeout => "timeout".to_owned(),
        ProcessFailure::Sanitizer { report } => format!("sanitizer {}", sanitizer_words(report)),
        ProcessFailure::OutOfMemory => "out-of-memory".to_owned(),
        ProcessFailure::Abort => "abort".to_owned(),
        ProcessFailure::Signal { signal } => format!("signal {signal}"),
        ProcessFailure::Exit { code } => format!("exit {code}"),
    };
    let bucket = primary_bucket(classified)?;
    let what = match &classified.result {
        ProcessResult::Crashed { failure: f, .. } => Some(failure(f)),
        ProcessResult::NoEnd => None,
        ProcessResult::Ended(run) => match &run.outcome {
            ExploreOutcome::Fail { property } => Some(property.clone()),
            ExploreOutcome::HarnessFailure { reason } => Some(reason.clone()),
            ExploreOutcome::Abandoned { .. } => None,
            // The first `Known` or `Draft` violation in the primary bucket.
            ExploreOutcome::Clean => run
                .notes
                .iter()
                .find(|(_, note)| *note == bucket)
                .map(|(property, _)| property.clone()),
        },
    };
    Some(match what {
        Some(what) => format!("{} {what}", bucket.name()),
        None => bucket.name().to_owned(),
    })
}

/// `report` without a leading `==<pid>==`, up to its first word that holds
/// a digit.
fn sanitizer_words(report: &str) -> String {
    let report = report
        .strip_prefix("==")
        .and_then(|rest| rest.split_once("=="))
        .map_or(report, |(_, rest)| rest);
    report
        .split_whitespace()
        .take_while(|word| !word.bytes().any(|byte| byte.is_ascii_digit()))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The directory name of `signature`: the first 16 hex digits of its
/// SHA-256.
pub fn signature_hash(signature: &str) -> String {
    hex(&Sha256::digest(signature.as_bytes()))[..16].to_owned()
}

/// Triages the input in the file `artifact` on `entry`'s harness, with
/// children from `children`, and archives it under `out`.
///
/// Returns an error if the input can't be read or is longer than a trace's
/// `input.bin` may be, if a child can't be started, or if the bundle can't
/// be written. The input itself is never changed.
pub fn triage(
    entry: &HarnessEntry,
    children: &dyn Children,
    artifact: &Path,
    out: &Path,
    options: TriageOptions,
) -> io::Result<Triaged> {
    let input = fs::read(artifact)?;
    if u64::try_from(input.len()).map_or(true, |len| len > MAX_INPUT_BYTES) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} is {} bytes, and a trace's input.bin holds at most {MAX_INPUT_BYTES}",
                artifact.display(),
                input.len()
            ),
        ));
    }
    let digest = hex(&Sha256::digest(&input));
    let staging = out.join(STAGING_DIR).join(format!(
        "{}-{}",
        process::id(),
        NEXT_STAGING.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&staging)?;
    let staged_input = staging.join(BUNDLE_INPUT_FILE);
    write_synced(&staged_input, &input)?;
    if let Some(scenario) = draw(entry, &input) {
        write_synced(
            &staging.join(SCENARIO_FILE),
            format!("{scenario:#}\n").as_bytes(),
        )?;
    }

    let record = |name: &str| -> io::Result<ChildRun> {
        let dir = staging.join(RUNS_DIR).join(name);
        let trace = dir.join(RUN_TRACE_DIR);
        let (exit, stderr) = run_child(
            children.record(&staged_input, &trace),
            &dir,
            options.child_timeout,
        )?;
        let classified = classify_process(entry, exit, &stderr, &trace).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the recording in {} exited with code 0, but its trace doesn't read: {error}",
                    trace.display()
                ),
            )
        })?;
        Ok(ChildRun { exit, classified })
    };
    let first = record(RECORD_DIRS[0])?;
    let second = record(RECORD_DIRS[1])?;

    let signatures = [signature(&first.classified), signature(&second.classified)];
    let alike =
        signatures[0] == signatures[1] && first.classified.buckets() == second.classified.buckets();
    let replay = if alike && matches!(first.classified.result, ProcessResult::Ended(_)) {
        let trace = staging
            .join(RUNS_DIR)
            .join(RECORD_DIRS[0])
            .join(RUN_TRACE_DIR);
        let (exit, _) = run_child(
            children.replay(&trace),
            &staging.join(RUNS_DIR).join(REPLAY_DIR),
            options.child_timeout,
        )?;
        Some(exit)
    } else {
        None
    };
    let replayed = replay.is_none_or(|exit| exit == ProcessExit::Code(0));
    let failed = signatures[0].is_some();
    let status = match (alike, replayed, failed) {
        (false, _, _) => TriageStatus::Nondeterministic,
        (true, false, _) => TriageStatus::ReplayFailed,
        (true, true, true) => TriageStatus::Confirmed,
        (true, true, false) => TriageStatus::NotReproduced,
    };
    let (bucket, signature) = match status {
        TriageStatus::Confirmed => (primary_bucket(&first.classified), signatures[0].clone()),
        _ => (None, None),
    };

    let record_json = |name: &str, run: &ChildRun, signature: &Option<String>| {
        json!({
            "dir": format!("{RUNS_DIR}/{name}"),
            "exit": run.exit,
            "class": run.classified,
            "buckets": run.classified.buckets(),
            "signature": signature,
        })
    };
    let report = json!({
        "format": TRIAGE_FORMAT,
        "status": status,
        "bucket": bucket,
        "signature": signature,
        "harness": entry.name,
        "generator": entry.generator,
        "artifact": artifact.display().to_string(),
        "input": { "sha256": digest, "len": input.len() },
        "records": [
            record_json(RECORD_DIRS[0], &first, &signatures[0]),
            record_json(RECORD_DIRS[1], &second, &signatures[1]),
        ],
        "replay": replay.map(|exit| json!({
            "dir": format!("{RUNS_DIR}/{REPLAY_DIR}"),
            "exit": exit,
            "verified": exit == ProcessExit::Code(0),
        })),
        "child_timeout_ms": u64::try_from(options.child_timeout.as_millis()).unwrap_or(u64::MAX),
        "build": BuildInfo::current(),
    });
    write_synced(
        &staging.join(TRIAGE_FILE),
        format!("{report:#}\n").as_bytes(),
    )?;
    let sums = checksums(&staging)?;
    write_synced(&staging.join(CHECKSUMS_FILE), sums.as_bytes())?;

    let bundle = match (&bucket, &signature) {
        (Some(bucket), Some(signature)) => out
            .join(bucket.name())
            .join(signature_hash(signature))
            .join(&digest),
        _ => out.join(status.name()).join(&digest),
    };
    let archived = place(&staging, &bundle)?;
    Ok(Triaged {
        status,
        bucket,
        signature,
        records: [first, second],
        replay,
        bundle,
        archived,
    })
}

/// The scenario the input's prefix draws, serialized, or `None` if the
/// harness's decoder panics on it. The recordings then crash the same way,
/// and their bundle shows it.
fn draw(entry: &HarnessEntry, input: &[u8]) -> Option<Value> {
    let mut prefix = vec![0u8; entry.scenario_bytes];
    let n = input.len().min(entry.scenario_bytes);
    prefix[..n].copy_from_slice(&input[..n]);
    panic::catch_unwind(AssertUnwindSafe(|| (entry.draw)(&prefix))).ok()
}

/// Runs `command` with its standard output and error in files under `dir`,
/// and kills it once it has run for `limit`. Returns how it ended and its
/// standard error.
fn run_child(
    mut command: Command,
    dir: &Path,
    limit: Duration,
) -> io::Result<(ProcessExit, Vec<u8>)> {
    fs::create_dir_all(dir)?;
    let stdout = File::create(dir.join(STDOUT_FILE))?;
    let stderr = File::create(dir.join(STDERR_FILE))?;
    let mut child = command
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()?;
    let deadline = Instant::now() + limit;
    let exit = loop {
        if let Some(status) = child.try_wait()? {
            break ProcessExit::from(status);
        }
        if Instant::now() >= deadline {
            child.kill()?;
            // A child that exited on its own just before the kill keeps its
            // own exit.
            break match ProcessExit::from(child.wait()?) {
                ProcessExit::Signal(SIGKILL) => ProcessExit::TimedOut,
                exit => exit,
            };
        }
        thread::sleep(POLL);
    };
    Ok((exit, fs::read(dir.join(STDERR_FILE))?))
}

/// Writes `bytes` to a new file at `path`, and syncs it.
fn write_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Moves the bundle at `staging` to `bundle`, unless a bundle is already
/// there. Returns true if it moved it. If one is there, it removes the
/// staging bundle and keeps the one that's there.
fn place(staging: &Path, bundle: &Path) -> io::Result<bool> {
    fs::create_dir_all(
        bundle
            .parent()
            .expect("a bundle's path names a directory inside the archive"),
    )?;
    if fs::symlink_metadata(bundle).is_ok() {
        fs::remove_dir_all(staging)?;
        return Ok(false);
    }
    match fs::rename(staging, bundle) {
        Ok(()) => Ok(true),
        // Another triage placed the same bundle first. Renaming a
        // directory onto a directory that isn't empty fails, so the first
        // rename wins.
        Err(_) if fs::symlink_metadata(bundle).is_ok() => {
            fs::remove_dir_all(staging)?;
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

/// [`CHECKSUMS_FILE`]'s text for the bundle in `dir`: one line per regular
/// file but [`CHECKSUMS_FILE`], as `<sha256>  <path>`, sorted by path.
/// Fails on a symlink or a special file.
fn checksums(dir: &Path) -> io::Result<String> {
    let mut text = String::new();
    for (path, digest) in file_digests(dir)? {
        if path != CHECKSUMS_FILE {
            text.push_str(&format!("{digest}  {path}\n"));
        }
    }
    Ok(text)
}

/// The SHA-256 of every regular file under `dir`, by its `/`-separated
/// path relative to `dir`. Fails on a symlink or a special file.
fn file_digests(dir: &Path) -> io::Result<BTreeMap<String, String>> {
    let mut digests = BTreeMap::new();
    let mut pending = vec![PathBuf::new()];
    while let Some(relative) = pending.pop() {
        for item in fs::read_dir(dir.join(&relative))? {
            let item = item?;
            let path = relative.join(item.file_name());
            let kind = item.file_type()?;
            if kind.is_dir() {
                pending.push(path);
            } else if kind.is_file() {
                let name = path
                    .iter()
                    .map(|part| part.to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                digests.insert(name, hex(&Sha256::digest(fs::read(dir.join(&path))?)));
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} is neither a file nor a directory",
                        dir.join(&path).display()
                    ),
                ));
            }
        }
    }
    Ok(digests)
}

/// What [`verify_bundle`] found wrong with a bundle, one entry per file.
/// Empty for a bundle whose files match its [`CHECKSUMS_FILE`].
pub fn verify_bundle(dir: &Path) -> io::Result<Vec<String>> {
    let listed = fs::read_to_string(dir.join(CHECKSUMS_FILE))?;
    let mut actual = file_digests(dir)?;
    actual.remove(CHECKSUMS_FILE);
    let mut problems = Vec::new();
    for line in listed.lines() {
        let Some((digest, path)) = line.split_once("  ") else {
            problems.push(format!("{CHECKSUMS_FILE} has a malformed line: {line}"));
            continue;
        };
        match actual.remove(path) {
            None => problems.push(format!("{path} is missing")),
            Some(found) if found != digest => problems.push(format!("{path} changed")),
            Some(_) => {}
        }
    }
    problems.extend(
        actual
            .into_keys()
            .map(|path| format!("{path} isn't in {CHECKSUMS_FILE}")),
    );
    Ok(problems)
}

/// Every bundle under the archive root `out`, sorted: each directory that
/// holds a [`TRIAGE_FILE`], outside [`STAGING_DIR`].
pub fn bundles(out: &Path) -> io::Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    let mut pending = vec![out.to_owned()];
    while let Some(dir) = pending.pop() {
        if fs::symlink_metadata(dir.join(TRIAGE_FILE)).is_ok() {
            found.push(dir);
            continue;
        }
        for item in fs::read_dir(&dir)? {
            let item = item?;
            if item.file_type()?.is_dir() && item.file_name() != STAGING_DIR {
                pending.push(item.path());
            }
        }
    }
    found.sort();
    Ok(found)
}
