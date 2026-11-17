//! The supervisor that runs a manifest's workers, and the end record each
//! run gets exactly once.

use std::{
    collections::{BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    os::unix::process::CommandExt,
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use super::{
    manifest::{Budget, Manifest, WorkerPlan, SANITIZER_VARS},
    snapshot::Source,
    store::{
        now_unix, partial_path, read_record, record_bytes, replace, write_once, CampaignError,
        Digest, Store, STORE_VERSION,
    },
};
use crate::{
    explore::{
        feedback::FEEDBACK_VAR, stats::STATS_VAR, ProcessExit, ProcessFailure, ABANDON_LIMIT_PANIC,
        ABANDON_LIMIT_VAR, EXPECT_VAR,
    },
    trace::ARTIFACTS_VAR,
};

/// A run's end record.
pub(super) const END_FILE: &str = "end.json";

/// A worker's log of fuzzer processes, one [`Attempt`] per line.
pub const ATTEMPTS_FILE: &str = "attempts.jsonl";

/// A worker's [`WorkerEnd`], written once when it stops.
pub const OUTCOME_FILE: &str = "outcome.json";

/// A worker's current fuzzer process ID.
const PID_FILE: &str = "pid";

/// The directory of a run's workers.
const WORKERS_DIR: &str = "workers";

/// A worker's libFuzzer corpus: the inputs its processes add.
const CORPUS_DIR: &str = "corpus";

/// A worker's libFuzzer artifacts: crash, timeout and OOM inputs.
const ARTIFACTS_DIR: &str = "artifacts";

/// A worker's `ZAKURA_SIM_ARTIFACTS`: the target's saved inputs and traces.
const SIM_DIR: &str = "sim";

/// A worker's `ZAKURA_SIM_STATS`: each fuzzer process's stats file.
pub(super) const STATS_DIR: &str = "stats";

/// The compaction's libFuzzer artifacts: the inputs its merge crashed on.
const COMPACT_ARTIFACTS_DIR: &str = "compact-artifacts";

/// The compaction's `ZAKURA_SIM_ARTIFACTS`.
const COMPACT_SIM_DIR: &str = "compact-sim";

/// How long past its budget and per-input timeout a fuzzer process may run
/// before the supervisor kills it.
const GRACE: Duration = Duration::from_secs(60);

/// How often the supervisor checks a process with a deadline.
const POLL: Duration = Duration::from_millis(50);

/// How long the supervisor waits for a killed process group to go.
const GROUP_EXIT: Duration = Duration::from_secs(10);

/// How many bytes of a process's output its log keeps from the start. The
/// feedback line comes at the start.
const LOG_HEAD: usize = 1 << 20;

/// How many bytes of a process's output its log keeps from the end, where
/// libFuzzer and the sanitizers print their reports.
const LOG_TAIL: usize = 1 << 20;

/// How long the supervisor waits for a process's output to end once the
/// process has exited.
const OUTPUT_DRAIN: Duration = Duration::from_secs(10);

/// The longest line of an attempts log that `close` reads as an attempt.
/// The longest line the supervisor writes holds one line of a process's
/// log, at most [`LOG_HEAD`] + [`LOG_TAIL`] bytes, escaped as JSON.
const MAX_ATTEMPT_LINE: u64 = 64 << 20;

/// How a run ended. Each status means one thing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Status {
    /// The supervisor ran every worker to the end of its budget.
    Complete,
    /// The supervisor ran every worker until it stopped, and at least one
    /// stopped at its restart limit before its budget was spent.
    WorkerStopped,
    /// The supervisor stopped before it wrote the end record, and `close`
    /// wrote it from what the workers left.
    Interrupted,
}

/// Who wrote an end record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Recorder {
    /// The run's supervisor, after every worker stopped.
    Supervisor,
    /// `close`, or `resume` or `start` through it, after the supervisor
    /// stopped without one.
    Close,
}

/// Why a worker stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerOutcome {
    /// Its processes ran for its whole budget.
    BudgetSpent,
    /// Its process ended early once more than its restart limit allows.
    RestartLimit,
    /// It stopped with its supervisor.
    Unfinished,
}

/// How one fuzzer process ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AttemptEnd {
    /// It exited 0 at its time limit.
    Finished,
    /// It exited 0 before its time limit, without a failure. The worker
    /// restarts it, as after a failure, since its budget isn't spent.
    EarlyExit,
    /// The target reached its abandonment limit, and exited for the
    /// supervisor to restart it.
    AbandonLimit,
    /// It failed for any other reason: a crash, a property violation, a
    /// sanitizer report, a timeout or running out of memory.
    Failure,
}

/// A file the store holds, by its path relative to the store's root and
/// its digest.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRecord {
    /// The path, relative to the store's root, with `/` separators.
    pub path: String,
    /// The file's SHA-256.
    pub sha256: Digest,
}

/// One fuzzer process a worker ran: a line of its [`ATTEMPTS_FILE`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    /// The attempt's number in its worker, from 0.
    pub attempt: u32,
    /// libFuzzer's `-seed`.
    pub seed: u32,
    /// libFuzzer's `-max_total_time`.
    pub max_total_time: u64,
    /// Its process ID, which names its stats file. `None` in a record
    /// written before attempts recorded it.
    #[serde(default)]
    pub pid: Option<u32>,
    /// When it started, in seconds since the Unix epoch.
    pub started_unix: u64,
    /// How long it ran, in milliseconds.
    pub millis: u64,
    /// Its exit code, if it exited.
    pub code: Option<i32>,
    /// The signal that ended it, if one did. The supervisor's kill at the
    /// deadline is `killed`, not a signal.
    pub signal: Option<i32>,
    /// True if the supervisor killed it at its deadline.
    pub killed: bool,
    /// How it ended.
    pub end: AttemptEnd,
    /// How it failed, unless it finished.
    pub failure: Option<ProcessFailure>,
    /// The artifact files it wrote, such as `crash-<sha1>`.
    pub artifacts: Vec<FileRecord>,
}

/// How a worker ended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerEnd {
    /// The worker's number.
    pub index: usize,
    /// Why it stopped.
    pub outcome: WorkerOutcome,
    /// How many fuzzer processes it ran.
    pub attempts: u32,
    /// How long they ran in all, in milliseconds.
    pub millis: u64,
}

/// What compacting a run's snapshot came to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Compaction {
    /// libFuzzer's merge kept these inputs, saved as a new snapshot.
    Compacted {
        /// The compacted snapshot.
        snapshot: Digest,
        /// How many inputs it holds.
        inputs: usize,
    },
    /// The merge failed or reached its time limit. The run's snapshot is
    /// the uncompacted one.
    Failed {
        /// How.
        failure: ProcessFailure,
    },
}

/// A run's end record, written once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct End {
    /// [`STORE_VERSION`].
    pub version: u32,
    /// The run's manifest.
    pub manifest: Digest,
    /// How the run ended.
    pub status: Status,
    /// Who wrote this record.
    pub recorded_by: Recorder,
    /// When, in seconds since the Unix epoch.
    pub ended_unix: u64,
    /// How each worker ended.
    pub workers: Vec<WorkerEnd>,
    /// The union of the initial snapshot and every worker's corpus.
    pub snapshot: Digest,
    /// How many inputs it holds.
    pub inputs: usize,
    /// How many of them the initial snapshot lacked.
    pub added: usize,
    /// What compacting the snapshot came to, if the run compacted it.
    pub compaction: Option<Compaction>,
    /// Every file in the workers' and the compaction's artifact and target
    /// directories: the failing inputs and their traces.
    pub failures: Vec<FileRecord>,
}

impl End {
    /// The snapshot the next run starts from: the compacted one if the
    /// compaction succeeded, and else the run's snapshot.
    pub fn head(&self) -> &Digest {
        match &self.compaction {
            Some(Compaction::Compacted { snapshot, .. }) => snapshot,
            _ => &self.snapshot,
        }
    }
}

/// A run whose manifest `start` or `resume` wrote, and whose lock this
/// process took before it let go of the store's, so no other process can
/// close the run before it starts.
#[derive(Debug)]
pub struct Prepared {
    /// The run's manifest.
    manifest: Digest,
    /// The run's lock.
    lock: File,
}

impl Prepared {
    /// Locks the run of manifest `digest`.
    pub(super) fn lock(store: &Store, manifest: Digest) -> Result<Prepared, CampaignError> {
        Ok(Prepared {
            lock: store.lock_run(&manifest)?,
            manifest,
        })
    }

    /// The run's manifest.
    pub fn manifest(&self) -> &Digest {
        &self.manifest
    }

    /// Runs the run: supervises its workers until each one stops, then
    /// saves the run's snapshot, compacts it if the budget asks, and writes
    /// the end record.
    ///
    /// Fails without an end record if the store can't be read or written;
    /// the run then stays open, and `close` or `resume` ends it later.
    pub fn run(self, store: &Store) -> Result<End, CampaignError> {
        let Prepared { manifest, lock } = self;
        let ended = supervise(store, &manifest);
        drop(lock);
        ended
    }
}

/// [`Prepared::run`] for manifest `digest`, whose lock the caller holds.
fn supervise(store: &Store, digest: &Digest) -> Result<End, CampaignError> {
    let manifest = store.read_manifest(digest)?;
    let dir = store.run_dir(digest);
    if dir.join(END_FILE).exists() {
        return Err(already_ended(digest));
    }
    store.verify_snapshot(&manifest.initial_snapshot)?;
    store.read_build(&manifest.build.binary)?;
    let fuzzer = store.fuzzer_path(&manifest.build.binary);
    let snapshot = store.snapshot_inputs(&manifest.initial_snapshot);
    let ends: Vec<Result<WorkerEnd, CampaignError>> = thread::scope(|scope| {
        let handles: Vec<_> = manifest
            .workers
            .iter()
            .map(|plan| {
                let worker = Worker {
                    store,
                    fuzzer: &fuzzer,
                    budget: &manifest.budget,
                    plan,
                    dir: worker_dir(&dir, plan.index),
                    snapshot: &snapshot,
                };
                scope.spawn(move || worker.supervise())
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .expect("a worker's supervisor returns its errors rather than panicking")
            })
            .collect()
    });
    let workers = ends.into_iter().collect::<Result<Vec<_>, _>>()?;
    finish(store, digest, &manifest, workers, Recorder::Supervisor)
}

/// Ends open run `digest`, whose supervisor stopped before it wrote the
/// end record: saves the run's snapshot from what the workers left, and
/// writes the end record with [`Status::Interrupted`].
///
/// Refuses a run that a supervisor still holds, that has an end record,
/// or whose fuzzer or compaction process still runs.
pub fn close(store: &Store, digest: &Digest) -> Result<End, CampaignError> {
    let manifest = store.read_manifest(digest)?;
    let _lock = store.lock_run(digest)?;
    let dir = store.run_dir(digest);
    if dir.join(END_FILE).exists() {
        return Err(already_ended(digest));
    }
    // A merge whose supervisor stopped can still write crash files.
    if let Some(pid) = processes_with(&compaction_marker(digest)).first() {
        return Err(CampaignError::Refused(format!(
            "the compaction's merge still runs as process {pid}; stop it, then close the run"
        )));
    }
    let fuzzer = store.fuzzer_path(&manifest.build.binary);
    let mut workers = Vec::new();
    for plan in &manifest.workers {
        let worker = worker_dir(&dir, plan.index);
        if let Some(pid) = live_fuzzer(&worker, &fuzzer) {
            return Err(CampaignError::Refused(format!(
                "worker {}'s fuzzer still runs as process {pid}; stop it, then close the run",
                plan.index
            )));
        }
        workers.push(stopped_worker(plan, &worker)?);
    }
    finish(store, digest, &manifest, workers, Recorder::Close)
}

/// Reads run `digest`'s end record, and checks that it's this run's.
pub fn read_end(store: &Store, digest: &Digest) -> Result<End, CampaignError> {
    let path = store.run_dir(digest).join(END_FILE);
    let end: End = read_record(&path, None)?;
    if end.version != STORE_VERSION || &end.manifest != digest {
        return Err(CampaignError::corrupt(
            &path,
            format!(
                "it's the version {} record of run {}, not the version {STORE_VERSION} record \
                 of run {digest}",
                end.version, end.manifest
            ),
        ));
    }
    Ok(end)
}

/// Checks run `digest`'s end record against the run's own files: every
/// failure file it lists is in the store with its digest, every file in the
/// run's failure directories is listed, and its snapshot holds the initial
/// snapshot's inputs and every input in the workers' corpora. So a record
/// left by a stale or mixed copy of the store can't name an older snapshot
/// or leave a failure out. The snapshots' own inputs are
/// [`Store::verify_snapshot`]'s to check.
pub(super) fn verify_end(
    store: &Store,
    digest: &Digest,
    manifest: &Manifest,
    end: &End,
) -> Result<(), CampaignError> {
    verify_files(store, &end.failures)?;
    let listed: BTreeSet<&str> = end.failures.iter().map(|file| file.path.as_str()).collect();
    let dir = store.run_dir(digest);
    for failure_dir in failure_dirs(&dir, manifest) {
        for path in files_under(&failure_dir)? {
            if !listed.contains(store_path(store, &path)?.as_str()) {
                return Err(CampaignError::corrupt(
                    &path,
                    format!("it's a failure file of run {digest}, and its end record omits it"),
                ));
            }
        }
    }
    let held: BTreeSet<Digest> = store
        .read_index(&end.snapshot)?
        .inputs
        .into_iter()
        .map(|entry| entry.sha256)
        .collect();
    let snapshot = store.snapshot_dir(&end.snapshot);
    for entry in store.read_index(&manifest.initial_snapshot)?.inputs {
        if !held.contains(&entry.sha256) {
            return Err(CampaignError::corrupt(
                &snapshot,
                format!(
                    "run {digest} ended with it, and it lacks input {} of the run's initial \
                     snapshot",
                    entry.sha256
                ),
            ));
        }
    }
    for plan in &manifest.workers {
        for path in files_under(&worker_dir(&dir, plan.index).join(CORPUS_DIR))? {
            let input = Digest::of_file(&path)
                .map_err(CampaignError::io(format!("reading {}", path.display())))?;
            if !held.contains(&input) {
                return Err(CampaignError::corrupt(
                    &snapshot,
                    format!(
                        "run {digest} ended with it, and it lacks the worker input {}",
                        path.display()
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Checks that each of `files` is in the store with its digest.
fn verify_files(store: &Store, files: &[FileRecord]) -> Result<(), CampaignError> {
    for file in files {
        let relative = Path::new(&file.path);
        if !relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(CampaignError::Refused(format!(
                "the recorded path `{}` leaves the store",
                file.path
            )));
        }
        let path = store.root().join(relative);
        let actual = Digest::of_file(&path)
            .map_err(CampaignError::io(format!("reading {}", path.display())))?;
        if actual != file.sha256 {
            return Err(CampaignError::corrupt(
                &path,
                format!(
                    "it's recorded with digest {}, and has {actual}",
                    file.sha256
                ),
            ));
        }
    }
    Ok(())
}

/// One worker's supervisor.
struct Worker<'a> {
    /// The store, for paths in records.
    store: &'a Store,
    /// The fuzzer binary.
    fuzzer: &'a Path,
    /// The run's budget.
    budget: &'a Budget,
    /// The worker.
    plan: &'a WorkerPlan,
    /// The worker's directory.
    dir: PathBuf,
    /// The initial snapshot's inputs, which every worker reads.
    snapshot: &'a Path,
}

impl Worker<'_> {
    /// Runs fuzzer processes until the budget is spent or the restart
    /// limit is passed, logs each one, and writes the worker's outcome.
    fn supervise(&self) -> Result<WorkerEnd, CampaignError> {
        let corpus = self.dir.join(CORPUS_DIR);
        let artifacts = self.dir.join(ARTIFACTS_DIR);
        for dir in [&corpus, &artifacts] {
            fs::create_dir_all(dir)
                .map_err(CampaignError::io(format!("creating {}", dir.display())))?;
        }
        let budget = Duration::from_secs(self.budget.seconds);
        let mut spent = Duration::ZERO;
        let mut attempts = 0u32;
        let outcome = loop {
            let remaining = budget.saturating_sub(spent);
            if remaining.is_zero() {
                break WorkerOutcome::BudgetSpent;
            }
            // libFuzzer takes whole seconds; rounding up never stops a
            // worker short.
            let seconds = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
            let attempt = self.attempt(attempts, seconds, &corpus, &artifacts)?;
            spent += Duration::from_millis(attempt.millis);
            attempts += 1;
            // Only the time the processes ran spends the budget: a process
            // that exits 0 early hasn't fuzzed for it.
            if spent >= budget {
                break WorkerOutcome::BudgetSpent;
            }
            if attempts > self.budget.max_restarts {
                break WorkerOutcome::RestartLimit;
            }
        };
        let end = WorkerEnd {
            index: self.plan.index,
            outcome,
            attempts,
            millis: u64::try_from(spent.as_millis()).unwrap_or(u64::MAX),
        };
        let path = self.dir.join(OUTCOME_FILE);
        write_once(&path, &record_bytes(&end))
            .map_err(CampaignError::io(format!("writing {}", path.display())))?;
        Ok(end)
    }

    /// Runs fuzzer process `number` for `seconds`, and appends its
    /// [`Attempt`] to the worker's log.
    fn attempt(
        &self,
        number: u32,
        seconds: u64,
        corpus: &Path,
        artifacts: &Path,
    ) -> Result<Attempt, CampaignError> {
        let seed = self.plan.seed.wrapping_add(number);
        let before = file_names(artifacts)?;
        let mut command = fuzzer_command(
            self.fuzzer,
            self.plan.cpus.as_deref(),
            self.budget,
            &self.dir.join(SIM_DIR),
        );
        command
            .arg(corpus)
            .arg(self.snapshot)
            .arg(format!("-seed={seed}"))
            .arg(format!("-max_total_time={seconds}"))
            .arg(format!("-artifact_prefix={}/", artifacts.display()))
            .env(STATS_VAR, self.dir.join(STATS_DIR));
        let deadline = Duration::from_secs(seconds + self.budget.timeout_secs) + GRACE;
        let started_unix = now_unix();
        let started = Instant::now();
        let log = self.dir.join(format!("log-{number}.txt"));
        let pid_file = self.dir.join(PID_FILE);
        let mut pid = None;
        let (exit, output) = run_logged(command, &log, Some(deadline), None, |child| {
            pid = Some(child);
            // The pid file only lets `close` find a live fuzzer, so a
            // failed write costs nothing else.
            let _ = replace(&pid_file, format!("{child}\n").as_bytes());
        })?;
        let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let failure = ProcessFailure::of(exit, &output);
        let end = match &failure {
            None if millis >= seconds.saturating_mul(1000) => AttemptEnd::Finished,
            None => AttemptEnd::EarlyExit,
            Some(_) if String::from_utf8_lossy(&output).contains(ABANDON_LIMIT_PANIC) => {
                AttemptEnd::AbandonLimit
            }
            Some(_) => AttemptEnd::Failure,
        };
        let mut written = Vec::new();
        for name in file_names(artifacts)?.difference(&before) {
            written.push(file_record(self.store, &artifacts.join(name))?);
        }
        let attempt = Attempt {
            attempt: number,
            seed,
            max_total_time: seconds,
            pid,
            started_unix,
            millis,
            code: match exit {
                ProcessExit::Code(code) => Some(code),
                _ => None,
            },
            signal: match exit {
                ProcessExit::Signal(signal) => Some(signal),
                _ => None,
            },
            killed: exit == ProcessExit::TimedOut,
            end,
            failure,
            artifacts: written,
        };
        let path = self.dir.join(ATTEMPTS_FILE);
        let line = json_line(&attempt);
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .and_then(|mut file| {
                file.write_all(&line)?;
                file.sync_all()
            })
            .map_err(CampaignError::io(format!(
                "appending to {}",
                path.display()
            )))?;
        Ok(attempt)
    }
}

/// The fuzzer command for `budget`, before its corpus and per-process
/// flags: the binary, under `taskset -c <cpus>` if `cpus` is set, with the
/// budget's libFuzzer flags, its sanitizer variables and the target's
/// variables. The target saves its inputs and traces under `sim`, and runs
/// without [`EXPECT_VAR`], which would make it ignore every failure but one
/// property's, and without [`STATS_VAR`], which a worker's process adds.
pub(super) fn fuzzer_command(
    fuzzer: &Path,
    cpus: Option<&str>,
    budget: &Budget,
    sim: &Path,
) -> Command {
    let mut command = match cpus {
        Some(cpus) => {
            let mut command = Command::new("taskset");
            command.arg("-c").arg(cpus).arg(fuzzer);
            command
        }
        None => Command::new(fuzzer),
    };
    command
        .arg(format!("-max_len={}", budget.max_len))
        .arg(format!("-timeout={}", budget.timeout_secs))
        .arg(format!("-rss_limit_mb={}", budget.rss_limit_mb))
        .args(&budget.libfuzzer_args)
        .env(ARTIFACTS_VAR, sim)
        .env(ABANDON_LIMIT_VAR, budget.abandon_limit.to_string())
        .env_remove(EXPECT_VAR)
        .env_remove(STATS_VAR);
    match &budget.feedback_mode {
        Some(mode) => command.env(FEEDBACK_VAR, mode),
        None => command.env_remove(FEEDBACK_VAR),
    };
    for name in SANITIZER_VARS {
        match budget.sanitizer_options.get(*name) {
            Some(value) => command.env(name, value),
            None => command.env_remove(name),
        };
    }
    command
}

/// Runs `command` with its standard output and error in the file `log`,
/// calls `spawned` with its process ID, and waits for it, killing it at
/// `deadline` if one is given. Returns how it ended and the log.
///
/// The log keeps the output's first [`LOG_HEAD`] and last [`LOG_TAIL`]
/// bytes, with a line that counts the bytes dropped between them, so a
/// process that writes without bound fills neither the disk nor memory.
///
/// With `group`, text that each of the command's processes has in its
/// command line, the command runs in its own process group. At the
/// deadline, and again once the command exits, the whole group is killed,
/// and the call waits until no process with `group` in its command line is
/// left. So no descendant, such as libFuzzer's inner merge process, writes
/// after the call returns.
pub(super) fn run_logged(
    mut command: Command,
    log: &Path,
    deadline: Option<Duration>,
    group: Option<&str>,
    spawned: impl FnOnce(u32),
) -> Result<(ProcessExit, Vec<u8>), CampaignError> {
    let io = |what: &str| CampaignError::io(format!("{what} {}", log.display()));
    let mut file = File::create(log).map_err(io("creating"))?;
    let (output, stdout) = io::pipe().map_err(io("making the pipe for"))?;
    let stderr = stdout.try_clone().map_err(io("making the pipe for"))?;
    command.stdin(Stdio::null()).stdout(stdout).stderr(stderr);
    if group.is_some() {
        command.process_group(0);
    }
    let shown = format!("{:?}", command.get_program());
    let mut child = command
        .spawn()
        .map_err(CampaignError::io(format!("starting {shown}")))?;
    // The command holds the pipe's write ends, and the output ends only
    // once every holder closes them.
    drop(command);
    let (send, captured) = mpsc::channel();
    thread::spawn(move || {
        // The receiver is gone only if the wait below failed.
        let _ = send.send(capture(output, &mut file));
    });
    spawned(child.id());
    let waited = CampaignError::io(format!("waiting for {shown}"));
    let exit = match deadline {
        None => child.wait().map(ProcessExit::from).map_err(waited)?,
        Some(limit) => {
            let deadline = Instant::now() + limit;
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => break ProcessExit::from(status),
                    Ok(None) if Instant::now() >= deadline => {
                        // A process that exits between the check and the
                        // kill still counts as timed out.
                        if group.is_some() {
                            kill_group(child.id());
                        }
                        let _ = child.kill();
                        let _ = child.wait();
                        break ProcessExit::TimedOut;
                    }
                    Ok(None) => thread::sleep(POLL),
                    Err(error) => return Err(waited(error)),
                }
            }
        }
    };
    if let Some(marker) = group {
        end_group(child.id(), marker)?;
    }
    let output = captured
        .recv_timeout(OUTPUT_DRAIN)
        .map_err(|_| {
            CampaignError::Refused(format!(
                "{shown} exited, and a process it started still holds its output after {} s",
                OUTPUT_DRAIN.as_secs()
            ))
        })?
        .map_err(io("writing"))?;
    Ok((exit, output))
}

/// Copies `output` to `log` until it ends, keeping its first [`LOG_HEAD`]
/// and last [`LOG_TAIL`] bytes, and returns what it kept. The head goes to
/// `log` as it arrives, and the tail once the output ends, after a line
/// that counts the bytes dropped between them.
fn capture(mut output: impl Read, log: &mut File) -> io::Result<Vec<u8>> {
    let mut kept = Vec::new();
    let mut tail: VecDeque<u8> = VecDeque::new();
    let mut dropped = 0u64;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = match output.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        let (head, rest) = buffer[..read].split_at(read.min(LOG_HEAD - kept.len()));
        log.write_all(head)?;
        kept.extend_from_slice(head);
        tail.extend(rest);
        let excess = tail.len().saturating_sub(LOG_TAIL);
        tail.drain(..excess);
        // A usize always fits in a u64 on supported targets.
        dropped += excess as u64;
    }
    let head = kept.len();
    if dropped > 0 {
        kept.extend_from_slice(
            format!("\n[zakura-sim campaign: dropped {dropped} bytes of output]\n").as_bytes(),
        );
    }
    kept.extend(tail);
    log.write_all(&kept[head..])?;
    Ok(kept)
}

/// Sends SIGKILL to every process in process group `group`, by running
/// `kill`, since sending a signal from Rust needs `unsafe`. A group with no
/// process left is no error.
fn kill_group(group: u32) {
    let _ = Command::new("kill")
        .args(["-s", "KILL", "--", &format!("-{group}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Kills process group `group`, and waits up to [`GROUP_EXIT`] until no
/// process with `marker` in its command line is left.
fn end_group(group: u32, marker: &str) -> Result<(), CampaignError> {
    kill_group(group);
    let deadline = Instant::now() + GROUP_EXIT;
    loop {
        let left = processes_with(marker);
        if left.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(CampaignError::Refused(format!(
                "processes {left:?} of process group {group} still run after SIGKILL"
            )));
        }
        thread::sleep(POLL);
    }
}

/// The IDs of the running processes whose command line holds `marker`, from
/// `/proc`. A zombie's command line is empty, so zombies aren't listed.
fn processes_with(marker: &str) -> Vec<u32> {
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    let marker = marker.as_bytes();
    entries
        .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| {
            fs::read(format!("/proc/{pid}/cmdline"))
                .is_ok_and(|cmdline| cmdline.windows(marker.len()).any(|window| window == marker))
        })
        .collect()
}

/// Text that the command line of each process of run `digest`'s compaction
/// holds: the end of its `-artifact_prefix`, which libFuzzer's inner merge
/// process inherits. It doesn't depend on how the store's path is spelled.
fn compaction_marker(digest: &Digest) -> String {
    format!("{digest}/{COMPACT_ARTIFACTS_DIR}/")
}

/// Saves run `digest`'s snapshot, compacts it if the supervisor ends the
/// run and the budget asks, and writes the end record.
fn finish(
    store: &Store,
    digest: &Digest,
    manifest: &Manifest,
    workers: Vec<WorkerEnd>,
    recorded_by: Recorder,
) -> Result<End, CampaignError> {
    let dir = store.run_dir(digest);
    let initial = store.snapshot_inputs(&manifest.initial_snapshot);
    let corpora: Vec<PathBuf> = manifest
        .workers
        .iter()
        .map(|plan| worker_dir(&dir, plan.index).join(CORPUS_DIR))
        .collect();
    let sources: Vec<Source<'_>> = std::iter::once(Source::Dir(&initial))
        .chain(corpora.iter().map(|corpus| Source::Dir(corpus)))
        .collect();
    let initial_inputs = store
        .verify_snapshot(&manifest.initial_snapshot)?
        .index
        .inputs;
    let snapshot = store.save_snapshot(&sources)?;
    let compaction = match (recorded_by, manifest.budget.compact_secs) {
        (Recorder::Supervisor, Some(seconds)) => {
            Some(compact(store, digest, manifest, &snapshot.digest, seconds)?)
        }
        _ => None,
    };
    let mut failures = Vec::new();
    for failure_dir in failure_dirs(&dir, manifest) {
        for path in files_under(&failure_dir)? {
            failures.push(file_record(store, &path)?);
        }
    }
    let status = match recorded_by {
        Recorder::Close => Status::Interrupted,
        Recorder::Supervisor
            if workers
                .iter()
                .any(|worker| worker.outcome == WorkerOutcome::RestartLimit) =>
        {
            Status::WorkerStopped
        }
        Recorder::Supervisor => Status::Complete,
    };
    let end = End {
        version: STORE_VERSION,
        manifest: digest.clone(),
        status,
        recorded_by,
        ended_unix: now_unix(),
        workers,
        inputs: snapshot.index.inputs.len(),
        added: snapshot
            .index
            .inputs
            .len()
            .saturating_sub(initial_inputs.len()),
        snapshot: snapshot.digest,
        compaction,
        failures,
    };
    let path = dir.join(END_FILE);
    if !write_once(&path, &record_bytes(&end))
        .map_err(CampaignError::io(format!("writing {}", path.display())))?
    {
        return Err(already_ended(digest));
    }
    Ok(end)
}

/// Compacts `snapshot` with the fuzzer's `-merge=1` into a new directory,
/// within `seconds`, and saves the result as a new snapshot. `snapshot`
/// stays as it is whatever happens. The merge's crash files and the
/// target's saved inputs go under run `digest`'s directory, and every
/// process of the merge has stopped when this returns.
fn compact(
    store: &Store,
    digest: &Digest,
    manifest: &Manifest,
    snapshot: &Digest,
    seconds: u64,
) -> Result<Compaction, CampaignError> {
    let dir = &store.run_dir(digest);
    let out = partial_path(dir);
    let artifacts = dir.join(COMPACT_ARTIFACTS_DIR);
    for new in [&out, &artifacts] {
        fs::create_dir_all(new)
            .map_err(CampaignError::io(format!("creating {}", new.display())))?;
    }
    let mut command = fuzzer_command(
        &store.fuzzer_path(&manifest.build.binary),
        None,
        &manifest.budget,
        &dir.join(COMPACT_SIM_DIR),
    );
    command
        .arg(format!("-artifact_prefix={}/", artifacts.display()))
        .arg("-merge=1")
        .arg(&out)
        .arg(store.snapshot_inputs(snapshot));
    let (exit, output) = run_logged(
        command,
        &dir.join("compact.log"),
        Some(Duration::from_secs(seconds)),
        Some(&compaction_marker(digest)),
        |_| {},
    )?;
    // libFuzzer's merge restarts its inner process past an input that
    // crashes, so a merge that exits 0 finished, whatever reports its log
    // holds.
    let compaction = match ProcessFailure::of(exit, &output) {
        Some(failure) if exit != ProcessExit::Code(0) => Compaction::Failed { failure },
        _ => {
            let compacted = store.save_snapshot(&[Source::Dir(&out)])?;
            Compaction::Compacted {
                inputs: compacted.index.inputs.len(),
                snapshot: compacted.digest,
            }
        }
    };
    // Every kept input is in the new snapshot, or the merge failed.
    let _ = fs::remove_dir_all(&out);
    Ok(compaction)
}

/// A worker that stopped without its supervisor: its outcome record if it
/// wrote one, and else [`WorkerOutcome::Unfinished`] with the attempts it
/// logged.
fn stopped_worker(plan: &WorkerPlan, dir: &Path) -> Result<WorkerEnd, CampaignError> {
    let outcome = dir.join(OUTCOME_FILE);
    if outcome.exists() {
        return read_record(&outcome, None);
    }
    let attempts = read_attempts(dir)?;
    Ok(WorkerEnd {
        index: plan.index,
        outcome: WorkerOutcome::Unfinished,
        attempts: u32::try_from(attempts.len()).unwrap_or(u32::MAX),
        millis: attempts.iter().fold(0u64, |millis, attempt| {
            millis.saturating_add(attempt.millis)
        }),
    })
}

/// Every attempt in worker directory `dir`'s log, in order. A missing log
/// holds none. A line cut off when the supervisor stopped, or garbled on
/// disk, isn't an attempt.
pub(super) fn read_attempts(dir: &Path) -> Result<Vec<Attempt>, CampaignError> {
    let log = dir.join(ATTEMPTS_FILE);
    let io = || CampaignError::io(format!("reading {}", log.display()));
    let mut attempts = Vec::new();
    let mut reader = match File::open(&log) {
        Ok(file) => BufReader::new(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(attempts),
        Err(error) => return Err(io()(error)),
    };
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = (&mut reader)
            .take(MAX_ATTEMPT_LINE)
            .read_until(b'\n', &mut line)
            .map_err(io())?;
        if read == 0 {
            break;
        }
        // A usize always fits in a u64 on supported targets.
        if !line.ends_with(b"\n") && read as u64 == MAX_ATTEMPT_LINE {
            // Longer than any attempt the supervisor writes.
            reader.skip_until(b'\n').map_err(io())?;
            continue;
        }
        if let Ok(attempt) = serde_json::from_slice::<Attempt>(&line) {
            attempts.push(attempt);
        }
    }
    Ok(attempts)
}

/// The process ID in the worker's pid file, if that process still runs
/// `fuzzer`.
fn live_fuzzer(worker: &Path, fuzzer: &Path) -> Option<u32> {
    let pid: u32 = fs::read_to_string(worker.join(PID_FILE))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let exe = fs::read_link(format!("/proc/{pid}/exe")).ok()?;
    (fs::canonicalize(fuzzer).ok()? == exe).then_some(pid)
}

/// The directories of run `dir` whose files are failures: each worker's
/// libFuzzer artifacts and saved inputs, and the compaction's.
fn failure_dirs(dir: &Path, manifest: &Manifest) -> Vec<PathBuf> {
    manifest
        .workers
        .iter()
        .flat_map(|plan| {
            let worker = worker_dir(dir, plan.index);
            [worker.join(ARTIFACTS_DIR), worker.join(SIM_DIR)]
        })
        .chain([dir.join(COMPACT_ARTIFACTS_DIR), dir.join(COMPACT_SIM_DIR)])
        .collect()
}

/// Run `dir`'s worker `index`'s directory.
pub(super) fn worker_dir(dir: &Path, index: usize) -> PathBuf {
    dir.join(WORKERS_DIR).join(index.to_string())
}

/// The refusal for a run that already has its end record.
fn already_ended(digest: &Digest) -> CampaignError {
    CampaignError::Refused(format!("run {digest} already has its end record"))
}

/// The file at `path`, which is under the store's root, as a record.
fn file_record(store: &Store, path: &Path) -> Result<FileRecord, CampaignError> {
    Ok(FileRecord {
        path: store_path(store, path)?,
        sha256: Digest::of_file(path)
            .map_err(CampaignError::io(format!("reading {}", path.display())))?,
    })
}

/// `path`, which is under the store's root, relative to the root with `/`
/// separators, as a [`FileRecord`] holds it.
fn store_path(store: &Store, path: &Path) -> Result<String, CampaignError> {
    let relative = path.strip_prefix(store.root()).map_err(|_| {
        CampaignError::Refused(format!(
            "{} isn't under the store's root {}",
            path.display(),
            store.root().display()
        ))
    })?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

/// `value` as one line of JSON.
fn json_line(value: &impl Serialize) -> Vec<u8> {
    let mut line =
        serde_json::to_vec(value).expect("store records serialize to JSON without error");
    line.push(b'\n');
    line
}

/// The names of the regular files in `dir`. None if it doesn't exist.
fn file_names(dir: &Path) -> Result<BTreeSet<String>, CampaignError> {
    Ok(files_under(dir)?
        .into_iter()
        .filter(|path| path.parent() == Some(dir))
        .filter_map(|path| Some(path.file_name()?.to_string_lossy().into_owned()))
        .collect())
}

/// Every regular file under `dir`, at any depth, sorted, skipping names
/// that start with `.`. None if `dir` doesn't exist.
fn files_under(dir: &Path) -> Result<Vec<PathBuf>, CampaignError> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_owned()];
    while let Some(dir) = pending.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(CampaignError::Io {
                    context: format!("listing {}", dir.display()),
                    error,
                })
            }
        };
        for entry in entries {
            let entry = entry.map_err(CampaignError::io(format!("listing {}", dir.display())))?;
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            let kind = entry
                .file_type()
                .map_err(CampaignError::io(format!("listing {}", dir.display())))?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                found.push(entry.path());
            }
        }
    }
    found.sort();
    Ok(found)
}
