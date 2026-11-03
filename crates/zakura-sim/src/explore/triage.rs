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
//! 3. compares the two runs' traces, if either wrote its `end` record. A
//!    trace that doesn't read counts as a difference;
//! 4. if both runs filed the input alike, their traces match, and they
//!    ended without failing, replays the first run's trace Exact in a third
//!    fresh child;
//! 5. writes [`TRIAGE_FILE`] and [`CHECKSUMS_FILE`], and renames the bundle
//!    into place.
//!
//! A bundle never changes after its rename. A later triage that files the
//! same input in the same place keeps the bundle that's there if that bundle
//! matches its checksums and its [`TRIAGE_FILE`] names the same result.
//! Otherwise, as with a stale or misplaced copy, triage keeps both, and
//! names its own bundle `<input sha256>-<n>` with the first free `n`. The
//! input is only read, so a triage killed at any point leaves it whole, and
//! leaves its copy in the staging bundle.
//!
//! Bundles go under `<out>/<harness>/<provenance>/`, where `<harness>` is
//! the harness's name and `<provenance>` is [`provenance_hash`] of its
//! generator and this build. Below that, a [`TriageStatus::Confirmed`]
//! failure goes under `<bucket>/<signature hash>/<input sha256>/`, and every
//! other status under `<status>/<input sha256>/`. The same bytes on another
//! harness, generator or build are another run, so they keep their own
//! bundle. Each distinct input keeps its own bundle too, so a signature's
//! first bundle never stops a later input from being kept.
//!
//! If [`Children::minimize`] gives a minimizer, a confirmed `Active` failure
//! also gets a minimized bundle beside its bundle, as [`minimize`]
//! describes. The failure's bundle is in place before the minimizer starts,
//! and never changes.

mod minimize;

pub use minimize::{
    is_minimized, verify, verify_minimized, Minimization, MINIMIZED_FILE, MINIMIZED_FORMAT,
    MINIMIZED_SUFFIX, MINIMIZER_DIR,
};

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write as _},
    panic::{self, AssertUnwindSafe},
    path::{self, Path, PathBuf},
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
        meta::{hex, BuildInfo, UNKNOWN},
        trace_difference, Invalid, ReadError, Trace, TraceReader, MAX_INPUT_BYTES,
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

    /// A command that minimizes the input in the file `input`, whose run
    /// violates the `Active` property `property`, into the directory `out`,
    /// which doesn't exist yet, as `cargo xtask sim-minimize` does. `None`,
    /// the default, if triage doesn't minimize.
    fn minimize(&self, input: &Path, property: &str, out: &Path) -> Option<Command> {
        let _ = (input, property, out);
        None
    }
}

/// The `zakura-sim` binary at `exe`, running harness `harness`, and
/// `cargo xtask sim-minimize` if `minimize` is set.
#[derive(Clone, Debug)]
pub struct SimChildren {
    /// The binary.
    pub exe: PathBuf,
    /// The registered harness's name.
    pub harness: String,
    /// How to minimize a confirmed `Active` failure, if triage should.
    pub minimize: Option<SimMinimize>,
}

/// `cargo xtask sim-minimize`'s target and budgets, as
/// [`SimChildren::minimize`] passes them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimMinimize {
    /// The workspace that `cargo xtask` runs in.
    pub workspace: PathBuf,
    /// The fuzz target.
    pub target: String,
    /// `tmin`'s attempts per step, `--runs`.
    pub runs: u32,
    /// The longest `tmin` may run, `--wall-secs`.
    pub wall: Duration,
    /// The longest one of its checks' children may run, `--child-secs`.
    pub child: Duration,
    /// The fuzz build's sanitizer, `--sanitizer`, if given.
    pub sanitizer: Option<String>,
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

    /// `cargo xtask sim-minimize <target> <input> --property <property>
    /// --out <out>` with [`SimMinimize`]'s budgets, in its workspace.
    /// `cargo xtask sim-triage` runs this binary in the simulator build, and
    /// xtask is an ordinary build, so the command drops that build's
    /// `RUSTFLAGS` and target directory.
    fn minimize(&self, input: &Path, property: &str, out: &Path) -> Option<Command> {
        let minimize = self.minimize.as_ref()?;
        // The command runs in the workspace, so a relative path would name
        // another file.
        let absolute = |path: &Path| path::absolute(path).unwrap_or_else(|_| path.to_owned());
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let mut command = Command::new(cargo);
        command
            .current_dir(&minimize.workspace)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_TARGET_DIR")
            .args(["xtask", "sim-minimize", &minimize.target])
            .arg(absolute(input))
            .args(["--property", property, "--out"])
            .arg(absolute(out))
            .args(["--runs", &minimize.runs.to_string()])
            .args(["--wall-secs", &minimize.wall.as_secs().to_string()])
            .args(["--child-secs", &minimize.child.as_secs().to_string()]);
        if let Some(sanitizer) = &minimize.sanitizer {
            command.args(["--sanitizer", sanitizer]);
        }
        Some(command)
    }
}

/// A copy of the running executable for [`SimChildren::exe`], so every
/// child of one batch runs the build that the batch's bundles name.
///
/// `cargo xtask sim-minimize` rebuilds `zakura-sim` in the target directory
/// that `cargo xtask sim-triage` runs it from. Without the copy, the
/// children of every later input in the batch would run that rebuild, while
/// [`TRIAGE_FILE`] and the provenance named this process's build. Dropping
/// it removes the copy. A killed triage leaves it under [`STAGING_DIR`].
#[derive(Debug)]
pub struct PinnedBinary {
    /// Its staging directory.
    dir: PathBuf,
    /// The copy.
    exe: PathBuf,
}

impl PinnedBinary {
    /// Copies the running executable into a new staging directory under
    /// the archive root `out`. Fails if the copy's SHA-256 isn't
    /// [`BuildInfo::current`]'s, as when the file was replaced after this
    /// process read it.
    pub fn new(out: &Path) -> io::Result<Self> {
        let build = BuildInfo::current();
        let running = std::env::current_exe()?;
        let dir = new_staging(out)?;
        // Built before the copy, so a failed copy drops it, which removes
        // the staging directory.
        let pinned = PinnedBinary {
            exe: dir.join(running.file_name().unwrap_or("zakura-sim".as_ref())),
            dir,
        };
        fs::copy(&running, &pinned.exe)?;
        let copied = hex(&Sha256::digest(fs::read(&pinned.exe)?));
        if copied != build.binary {
            return Err(io::Error::other(format!(
                "{} changed after this process started: its SHA-256 is {copied}, and this \
                 process's build has binary {}",
                running.display(),
                build.binary
            )));
        }
        Ok(pinned)
    }

    /// The copy's path.
    pub fn exe(&self) -> &Path {
        &self.exe
    }
}

impl Drop for PinnedBinary {
    /// Removes the copy and its staging directory.
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// What triage concluded about an input. Each status means one thing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TriageStatus {
    /// Both fresh recordings filed the input under the same buckets and
    /// signature, their traces match, and that is a failure. If both ended
    /// without failing, a fresh replay of the first one's trace matched it
    /// Exact.
    Confirmed,
    /// Both fresh recordings ran clean, their traces match, and a fresh
    /// replay of the first one's trace matched it Exact.
    NotReproduced,
    /// The two fresh recordings filed the input differently, their traces
    /// differ, or a trace doesn't read, so triage can't compare them.
    Nondeterministic,
    /// Both fresh recordings filed the input alike and their traces match,
    /// but a fresh replay of the first one's trace didn't match it Exact.
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
    /// False if a bundle that holds the same result was already in that
    /// place, so this triage kept it and archived nothing.
    pub archived: bool,
    /// What minimizing the input came to. `None` unless the input is a
    /// confirmed `Active` failure and [`Children::minimize`] gave a
    /// minimizer.
    pub minimization: Option<Minimization>,
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

/// The directory name of the provenance of a run on `entry`'s harness and
/// `build`: the first 16 hex digits of the SHA-256 of the harness's
/// generator and the build.
///
/// A clean build leaves its executable's hash out, so equivalent clean
/// builds of one source share it (see [`BuildInfo::equivalent`]). Any other
/// build keeps its executable's hash, since its recorded source may be
/// stale.
pub fn provenance_hash(entry: &HarnessEntry, build: &BuildInfo) -> String {
    let mut build = build.clone();
    if build.clean() {
        build.binary = UNKNOWN.to_owned();
    }
    let key = json!({ "generator": entry.generator, "build": build });
    hex(&Sha256::digest(key.to_string().as_bytes()))[..16].to_owned()
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
    let staging = new_staging(out)?;
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
            Some(options.child_timeout),
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
    let difference = recordings_difference(&staging);
    let alike = signatures[0] == signatures[1]
        && first.classified.buckets() == second.classified.buckets()
        && difference.is_none();
    let replay = if alike && matches!(first.classified.result, ProcessResult::Ended(_)) {
        let trace = staging
            .join(RUNS_DIR)
            .join(RECORD_DIRS[0])
            .join(RUN_TRACE_DIR);
        let (exit, _) = run_child(
            children.replay(&trace),
            &staging.join(RUNS_DIR).join(REPLAY_DIR),
            Some(options.child_timeout),
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
    let build = BuildInfo::current();
    let provenance = provenance_hash(entry, &build);
    let report = json!({
        "format": TRIAGE_FORMAT,
        "status": status,
        "bucket": bucket,
        "signature": signature,
        "harness": entry.name,
        "generator": entry.generator,
        "provenance": provenance,
        "artifact": artifact.display().to_string(),
        "input": { "sha256": digest, "len": input.len() },
        "records": [
            record_json(RECORD_DIRS[0], &first, &signatures[0]),
            record_json(RECORD_DIRS[1], &second, &signatures[1]),
        ],
        "difference": difference.map(|difference| match difference {
            Difference::Record { step, field } => json!({ "step": step, "field": field }),
            Difference::Unread { run, error } => json!({
                "run": format!("{RUNS_DIR}/{run}"),
                "unread": error,
            }),
        }),
        "replay": replay.map(|exit| json!({
            "dir": format!("{RUNS_DIR}/{REPLAY_DIR}"),
            "exit": exit,
            "verified": exit == ProcessExit::Code(0),
        })),
        "child_timeout_ms": u64::try_from(options.child_timeout.as_millis()).unwrap_or(u64::MAX),
        "build": build,
    });
    write_synced(
        &staging.join(TRIAGE_FILE),
        format!("{report:#}\n").as_bytes(),
    )?;
    let sums = checksums(&staging)?;
    write_synced(&staging.join(CHECKSUMS_FILE), sums.as_bytes())?;

    let root = entry
        .name
        .split('/')
        .fold(out.to_owned(), |dir, segment| dir.join(segment))
        .join(&provenance);
    let bundle = match (&bucket, &signature) {
        (Some(bucket), Some(signature)) => root
            .join(bucket.name())
            .join(signature_hash(signature))
            .join(&digest),
        _ => root.join(status.name()).join(&digest),
    };
    let (bundle, archived) = place(&staging, &bundle, TRIAGE_FILE, &RESULT_FIELDS, &report)?;

    // Only a confirmed `Active` failure has a property for the fuzz
    // target's filter to select.
    let minimization = match (status, bucket, &first.classified.result) {
        (TriageStatus::Confirmed, Some(Bucket::Active), ProcessResult::Ended(run)) => {
            match &run.outcome {
                ExploreOutcome::Fail { property } => {
                    minimize::minimize(children, &bundle, property, out, options)
                }
                _ => None,
            }
        }
        _ => None,
    };
    Ok(Triaged {
        status,
        bucket,
        signature,
        records: [first, second],
        replay,
        bundle,
        archived,
        minimization,
    })
}

/// A new, empty staging directory under the archive root `out`. A name that
/// a killed triage left is skipped, never reused.
fn new_staging(out: &Path) -> io::Result<PathBuf> {
    let staging_root = out.join(STAGING_DIR);
    fs::create_dir_all(&staging_root)?;
    loop {
        let dir = staging_root.join(format!(
            "{}-{}",
            process::id(),
            NEXT_STAGING.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&dir) {
            Ok(()) => return Ok(dir),
            // A killed triage whose process had this ID left it.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
}

/// How the two recordings' traces differ.
enum Difference {
    /// The first record where they differ, as [`trace_difference`] names
    /// it.
    Record { step: u64, field: &'static str },
    /// The recording in this run directory wrote a trace that doesn't read,
    /// or wrote none while the other wrote its `end` record, so triage can't
    /// compare them.
    Unread { run: &'static str, error: String },
}

/// How the two recordings' traces in `staging` differ. `None` if they
/// match, or if neither wrote its `end` record. A failed child's trace may
/// stop at any step, even before its first record, so two traces without
/// their `end` record aren't compared. A trace that doesn't read is a
/// difference, never a match.
fn recordings_difference(staging: &Path) -> Option<Difference> {
    let read = |run: &'static str| {
        let dir = staging.join(RUNS_DIR).join(run).join(RUN_TRACE_DIR);
        match TraceReader::open_prefix(&dir) {
            Ok(trace) => Ok(Some(trace)),
            // The child stopped before it wrote its first record.
            Err(ReadError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(ReadError::Invalid(Invalid::MissingMeta)) => Ok(None),
            Err(error) => Err(Difference::Unread {
                run,
                error: error.to_string(),
            }),
        }
    };
    let traces = match (read(RECORD_DIRS[0]), read(RECORD_DIRS[1])) {
        (Err(difference), _) | (_, Err(difference)) => return Some(difference),
        (Ok(first), Ok(second)) => [first, second],
    };
    let ended = |trace: &Option<Trace>| trace.as_ref().is_some_and(|trace| trace.end.is_some());
    if !traces.iter().any(ended) {
        return None;
    }
    match traces {
        [Some(first), Some(second)] => trace_difference(&first, &second)
            .map(|(step, field)| Difference::Record { step, field }),
        [first, _] => Some(Difference::Unread {
            run: RECORD_DIRS[usize::from(first.is_some())],
            error: "it wrote no trace, and the other recording wrote its end record".to_owned(),
        }),
    }
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
/// and kills it once it has run for `limit`, if given. Returns how it ended
/// and its standard error.
fn run_child(
    mut command: Command,
    dir: &Path,
    limit: Option<Duration>,
) -> io::Result<(ProcessExit, Vec<u8>)> {
    fs::create_dir_all(dir)?;
    let stdout = File::create(dir.join(STDOUT_FILE))?;
    let stderr = File::create(dir.join(STDERR_FILE))?;
    let mut child = command
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()?;
    let deadline = limit.map(|limit| Instant::now() + limit);
    let exit = loop {
        if let Some(status) = child.try_wait()? {
            break ProcessExit::from(status);
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
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

/// The [`TRIAGE_FILE`] fields that name a bundle's result. Two triages of
/// one input that agree on these filed it alike.
const RESULT_FIELDS: [&str; 8] = [
    "format",
    "status",
    "bucket",
    "signature",
    "harness",
    "generator",
    "provenance",
    "input",
];

/// Moves the bundle at `staging`, whose `file` holds `report`, to `bundle`,
/// or beside it. Returns where the bundle is, and true if it moved the
/// staging bundle there.
///
/// It tries `bundle`, then `<bundle>-1`, `<bundle>-2` and so on. At the
/// first name that's free, it moves the staging bundle there. At a name
/// that holds a bundle with the same result (see [`holds_result`]), it
/// removes the staging bundle and keeps that one. It never changes a bundle
/// that's there.
fn place(
    staging: &Path,
    bundle: &Path,
    file: &str,
    fields: &[&str],
    report: &Value,
) -> io::Result<(PathBuf, bool)> {
    fs::create_dir_all(
        bundle
            .parent()
            .expect("a bundle's path names a directory inside the archive"),
    )?;
    let name = bundle
        .file_name()
        .expect("a bundle's path ends in its input's SHA-256")
        .to_string_lossy();
    let mut n = 0u64;
    loop {
        let candidate = match n {
            0 => bundle.to_owned(),
            n => bundle.with_file_name(format!("{name}-{n}")),
        };
        if fs::symlink_metadata(&candidate).is_err() {
            match fs::rename(staging, &candidate) {
                Ok(()) => return Ok((candidate, true)),
                // Another triage placed a bundle there first. Renaming a
                // directory onto a directory that isn't empty fails, so the
                // first rename wins, and this triage checks that bundle as
                // it would any other.
                Err(_) if fs::symlink_metadata(&candidate).is_ok() => {}
                Err(error) => return Err(error),
            }
        }
        if holds_result(&candidate, file, fields, report) {
            fs::remove_dir_all(staging)?;
            return Ok((candidate, false));
        }
        n += 1;
    }
}

/// True if `dir` is a directory whose bundle passes [`verify`], whose
/// `file` agrees with `report` on every one of `fields`, and whose
/// [`BUNDLE_INPUT_FILE`]'s SHA-256 is the one `report` names. False if any
/// of that fails to read.
fn holds_result(dir: &Path, file: &str, fields: &[&str], report: &Value) -> bool {
    let is_dir = fs::symlink_metadata(dir).is_ok_and(|metadata| metadata.is_dir());
    let verified = || verify(dir).is_ok_and(|problems| problems.is_empty());
    let same_report = || {
        fs::read(dir.join(file))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .is_some_and(|found| fields.iter().all(|field| found[field] == report[field]))
    };
    let same_input = || {
        fs::read(dir.join(BUNDLE_INPUT_FILE)).is_ok_and(|input| {
            report["input"]["sha256"].as_str() == Some(hex(&Sha256::digest(input)).as_str())
        })
    };
    is_dir && verified() && same_report() && same_input()
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
    let listed = match fs::read_to_string(dir.join(CHECKSUMS_FILE)) {
        Ok(listed) => listed,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(vec![format!("{CHECKSUMS_FILE} is missing")]);
        }
        Err(error) => return Err(error),
    };
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

/// True if `name` names a bundle: an input's SHA-256, alone or followed by
/// `-<n>`, then, for a minimized bundle, [`MINIMIZED_SUFFIX`], alone or
/// followed by `-<n>`.
fn bundle_name(name: &str) -> bool {
    let name = minimize::original_name(name).unwrap_or(name);
    let (digest, n) = name.split_once('-').unwrap_or((name, "0"));
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        && !n.is_empty()
        && n.bytes().all(|byte| byte.is_ascii_digit())
}

/// Every bundle under the archive root `out`, sorted: each directory outside
/// [`STAGING_DIR`] that holds a [`TRIAGE_FILE`] or a [`MINIMIZED_FILE`], or
/// whose name is a bundle's, as every bundle's is (see [`bundle_name`]). A
/// bundle that lost its [`TRIAGE_FILE`] is still found, so
/// [`verify_bundle`] reports it.
pub fn bundles(out: &Path) -> io::Result<Vec<PathBuf>> {
    let digest_named = |dir: &Path| {
        dir.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(bundle_name)
    };
    let holds = |dir: &Path, file: &str| fs::symlink_metadata(dir.join(file)).is_ok();
    let mut found = Vec::new();
    let mut pending = vec![out.to_owned()];
    while let Some(dir) = pending.pop() {
        if digest_named(&dir) || holds(&dir, TRIAGE_FILE) || holds(&dir, MINIMIZED_FILE) {
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
