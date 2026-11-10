//! `cargo xtask header-acceptance`: header sync's acceptance run (hdr-14.7a,
//! item 9).
//!
//! It runs at one subject commit S, from a clean checkout. The caller's
//! xtask takes `--out`'s lock, removes the published record, checks S out
//! in a fresh run directory, `<out>/runs/<run id>/`, builds xtask there and
//! hands the run to it, which:
//!
//! 1. builds `zakura-sim` and `header-acceptance` under `--cfg zakura_sim`
//!    in its own target directory;
//! 2. asks `header-acceptance plan` for the tasks the manifest names;
//! 3. runs each task, every child with a deadline, writing its artifacts
//!    under `artifacts/<task index>/` in the run directory: each test by
//!    its exact name, each planted case through `cargo xtask planted` (the
//!    planted runner's baseline and patched runs, contract C4) with its
//!    patch rebuilt in its own target directory for the `Draft` run and its
//!    binary's provenance checked, and each integrity mutation's history
//!    unpatched, patched and restored;
//! 4. has `header-acceptance evaluate` assemble the result file and write
//!    the record, and prints its counts.
//!
//! S is `HEAD` of a checkout with no change at all, untracked files
//! included, so every input the run reads (the planted catalogue under
//! `qa/` as well as the build sources) is S's. Each property's planted
//! cases run in one `cargo xtask planted <property>` invocation, from
//! fresh case directories, and its target-mapped cases again through
//! `--targets` for their fuzz-target confirmation. A task's own setup
//! failure, such as a patch that doesn't apply, leaves its artifacts
//! missing, so its result blocks rather than stopping the record.
//!
//! Every result a task can't produce yet stays blocked in the record,
//! through the contract it needs. The record's three lists, verified,
//! excluded and blocked, are by obligation key.
//!
//! # Trust
//!
//! Run acceptance only on a reviewed commit, on a dedicated host. The
//! subject's build and tests run with the runner's full user access, as
//! `cargo build` and `cargo test` do. The runner is not a sandbox for
//! untrusted code: it catches honest defects and honest tools, and a
//! subject, host toolchain or environment hostile to the run is out of
//! model.
//!
//! # The record invariant
//!
//! `<out>/record.json` exists only if the last run to start fully
//! succeeded (Adam's rule, 2026-11-09). A run starts when, holding
//! `--out`'s lock, it retires the record by one rename, its first change
//! to `--out` ([`lock_out`]). A run that dies or fails before that has
//! changed nothing in `--out`. A started run keeps all its state in its
//! own run directory, and publishes the record last, by renaming a synced
//! copy into place ([`publish_record`]), with nothing after it that can
//! fail. Its result file stays in the run directory the record's artifacts
//! path names. Every child inherits the lock and [`RUN_MARK`], so while
//! anything a run started is alive, even after the runner is killed, no
//! other run starts or clears its directory. INT, TERM or HUP ends every
//! child the way its deadline would and fails the run ([`interrupted`]),
//! whose last decision is a check just before the record's rename. Each
//! build uses only the flags the runner sets and the committed profiles
//! ([`clean_build_env`]). The record names its subject, which a
//! reader compares, since runs from different checkouts may share `--out`.
//! `--out` must be on a local filesystem, where `flock` and `rename` hold.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        OnceLock,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::{json, Value};

use crate::{
    planted::{self, provenance},
    sim::SIM_RUSTFLAGS,
    sim_minimize::{self, ChildEnd},
    BoxError, UsageError,
};

/// The marker a killed child's run directory holds, as the reader's
/// `KILLED_FILE` names it.
const KILLED: &str = "killed";
/// A planted case's fuzz-target confirmation, as the reader's `TARGET_FILE`
/// names it.
const TARGET_FILE: &str = "target.json";
/// The planted runner's verdict on a case, as the reader's `VERDICT_FILE`
/// names it.
const VERDICT_FILE: &str = "verdict.json";
/// The task a directory is for, as the reader's `TASK_FILE` names it.
const TASK_FILE: &str = "task.json";

/// Set once this process adopts its orphaned descendants ([`contain`]).
static SUBREAPER: AtomicBool = AtomicBool::new(false);

/// The direct children [`child`] is running now. A sweep under the
/// subreaper leaves every other call's child and its subtree alone.
static LIVE: std::sync::Mutex<BTreeSet<i32>> = std::sync::Mutex::new(BTreeSet::new());

/// Held while this process spawns a child and records it in [`LIVE`], while
/// it runs `ps` ([`table`]), and while a sweep reads the table and signals.
/// So a sweep never sees a child of this process that isn't yet in
/// [`LIVE`], or another thread's `ps`, and takes it for an orphan.
static SPAWN: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Locks [`SPAWN`], ignoring poisoning: it guards no data.
fn spawn_lock() -> std::sync::MutexGuard<'static, ()> {
    SPAWN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A copy of [`LIVE`].
#[cfg(target_os = "linux")]
fn live_children() -> BTreeSet<i32> {
    LIVE.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Makes this process its descendants' subreaper on Linux
/// (`PR_SET_CHILD_SUBREAPER`, Adam's containment rule, 2026-11-09): an
/// orphaned descendant is reparented to it, not to PID 1, so a sweep finds
/// every one, however quickly its parent exits, and reaps it. Returns
/// whether it did. Elsewhere containment is the sampled [`Tree`], which is
/// best-effort, and the run's record says so, so it can't count for
/// acceptance.
fn contain() -> bool {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: `prctl` with `PR_SET_CHILD_SUBREAPER` takes plain integers
        // and touches no memory.
        let set = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == 0;
        SUBREAPER.store(set, Ordering::Relaxed);
        set
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Set when a child's descendants couldn't be confirmed gone ([`sweep`]),
/// which fails the run: a survivor could still write into
/// the work a later task reuses.
static ESCAPED: AtomicBool = AtomicBool::new(false);

/// The hidden first argument that has this command print a checkout's
/// source digest, so [`source_digest`] can read it in a child under the
/// deadline.
const SOURCE_DIGEST: &str = "--source-digest";

/// The default deadline for each child, in seconds.
const DEFAULT_DEADLINE_SECS: u64 = 3600;

/// What a run reads from its arguments.
struct Options {
    /// Where everything goes.
    out: PathBuf,
    /// Each child's deadline.
    deadline: Duration,
}

/// Reads `--out <dir>`, made absolute, and `--deadline-secs <n>`.
fn options(repo_root: &Path, args: impl Iterator<Item = String>) -> Result<Options, BoxError> {
    let mut out = None;
    let mut deadline = None;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" if out.is_none() => {
                out = Some(PathBuf::from(
                    args.next()
                        .ok_or(UsageError("expected a directory after `--out`"))?,
                ));
            }
            "--deadline-secs" if deadline.is_none() => {
                let secs: u64 = args
                    .next()
                    .ok_or(UsageError("expected seconds after `--deadline-secs`"))?
                    .parse()
                    .map_err(|_| UsageError("`--deadline-secs` takes a whole number"))?;
                deadline = Some(Duration::from_secs(secs));
            }
            _ => {
                return Err(Box::new(UsageError(
                    "expected at most one `--out <dir>` and one `--deadline-secs <n>`",
                )))
            }
        }
    }
    // Children run in other directories, so a relative `--out` is
    // resolved once, here, against this process's directory.
    let out = match out {
        Some(out) if out.is_relative() => env::current_dir()?.join(out),
        Some(out) => out,
        None => repo_root.join("target/header-acceptance"),
    };
    Ok(Options {
        out,
        deadline: deadline.unwrap_or(Duration::from_secs(DEFAULT_DEADLINE_SECS)),
    })
}

/// Removes, from this process's environment, the build settings an
/// operator's shell could otherwise carry into every Cargo build the run
/// starts: `CARGO_ENCODED_RUSTFLAGS`, which overrides the `RUSTFLAGS` a
/// build sets; `RUSTFLAGS` itself; and every `CARGO_PROFILE_*`, which
/// overrides the committed profiles (`planted` inherits `dev`). Every child
/// inherits the result, so each build uses only the flags the runner or the
/// planted runner sets and the profiles as committed. Cargo config files
/// outside the repository, such as `~/.cargo/config.toml`, are the
/// operator's. It runs first, before any thread or child.
fn clean_build_env() {
    let names: Vec<OsString> = env::vars_os()
        .map(|(name, _)| name)
        .filter(|name| {
            let name = name.to_string_lossy();
            name == "CARGO_ENCODED_RUSTFLAGS"
                || name == "RUSTFLAGS"
                || name.starts_with("CARGO_PROFILE_")
        })
        .collect();
    for name in names {
        env::remove_var(name);
    }
}

/// Runs the acceptance run for the checkout at `repo_root`.
pub(super) fn run(repo_root: &Path, args: impl Iterator<Item = String>) -> Result<(), BoxError> {
    let mut args = args.peekable();
    if args.peek().map(String::as_str) == Some(SOURCE_DIGEST) {
        args.next();
        let dir = args.next().ok_or(UsageError("expected a directory"))?;
        let read = provenance::Provenance::read(Path::new(&dir), provenance::SOURCE_PATHS);
        println!(
            "{}",
            json!({ "git": read.git, "dirty": read.dirty, "source": read.source })
        );
        return Ok(());
    }
    // A signal ends every child the way its deadline would, so nothing a
    // run started outlives an interrupted runner ([`interrupted`]).
    sim_minimize::install_interrupt_handler();
    clean_build_env();
    if args.peek().map(String::as_str) == Some(IN_SUBJECT) {
        args.next();
        let commit = args.next().ok_or(UsageError("expected S's commit"))?;
        return run_in_subject(repo_root, &commit, args);
    }
    let options = options(repo_root, args)?;
    let out = options.out.clone();
    // The run starts here, retiring the record (the record invariant, in
    // the module doc).
    let (_lock, kept) = lock_out(&out)?;
    mark_descendants(&out);
    let mut sessions = track_sessions(&out);
    contain();
    check_out_dir(repo_root, &options)?;
    let subject = subject_of(repo_root, &options)?;
    clear_stale_runs(repo_root, &options, kept.as_deref())?;
    sessions.cleared();
    // Everything after runs in a run directory of its own, created fresh,
    // and in a checkout of S inside it, so neither an earlier attempt nor a
    // change to the caller's checkout during the run can reach what it
    // reads.
    let run_dir = out.join(RUNS).join(run_id());
    fs::create_dir_all(out.join(RUNS))?;
    fs::create_dir(&run_dir)?;
    let run = Options {
        out: run_dir.clone(),
        deadline: options.deadline,
    };
    let root = run_dir.join(SUBJECT_DIR);
    add_worktree(repo_root, &run, &root, &subject.commit)?;
    let result = hand_off(&root, &run, &subject.commit);
    let removed = remove_worktrees_under(repo_root, &run, &run_dir);
    let log = result?;
    removed?;
    stop_if_interrupted()?;
    if ESCAPED.load(Ordering::Relaxed) {
        return Err(
            "a child's descendants couldn't be confirmed gone, so the record can't be trusted"
                .into(),
        );
    }
    for target in RUN_TARGETS {
        let target = run_dir.join(target);
        if target.exists() {
            fs::remove_dir_all(target)?;
        }
    }
    let summary = fs::read_to_string(&log)?;
    publish_record(&out, &run_dir)?;
    // Nothing after the record is published can fail the run.
    {
        use std::io::Write as _;
        let mut stdout = std::io::stdout().lock();
        let _ = write!(stdout, "{summary}");
        let _ = writeln!(stdout, "record: {}", out.join(RECORD).display());
    }
    Ok(())
}

/// The published record, directly under `--out`.
const RECORD: &str = "record.json";

/// What a starting run renames the published record to ([`lock_out`]).
const RETIRED_RECORD: &str = ".record.retired";

/// The directory under `--out` holding each run's own directory.
const RUNS: &str = "runs";

/// S's checkout inside a run directory.
const SUBJECT_DIR: &str = "subject";

/// The lock file under `--out` ([`lock_out`]).
const LOCK_FILE: &str = ".lock";

/// A run directory's target directories, dropped once the run succeeds.
const RUN_TARGETS: [&str; 5] = ["xtask-target", "build", "tests", "native", "cases"];

/// Takes `--out`'s lock ([`LOCK_FILE`]) for the whole run, or fails if
/// another run holds it, then starts the run: its first change to `--out`
/// is one rename that retires the published record, so from then on there
/// is none. Returns the lock and the run directory the retired record named
/// ([`published_run`]). The lock is a `flock`, which the kernel drops when
/// every holder of the open file is gone, killed included, so it's never
/// left stale. Every child inherits it. Elsewhere than Unix there is no
/// lock.
fn lock_out(out: &Path) -> Result<(fs::File, Option<PathBuf>), BoxError> {
    fs::create_dir_all(out)?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(out.join(LOCK_FILE))?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        // SAFETY: `flock` takes a file descriptor this process holds open
        // and touches no memory.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            // What a run on this output left running, though not
            // necessarily what holds the lock: a live run, or what survived
            // one ([`survivors`]).
            let found = match survivors(out) {
                Ok(pids) if pids.is_empty() => String::new(),
                Ok(pids) => format!(
                    "; detected processes of a run on this output: {pids:?} (a live run, or survivors of one)"
                ),
                Err(why) => format!("; its processes can't be listed: {why}"),
            };
            return Err(format!(
                "another header-acceptance run holds {}{found}",
                out.join(LOCK_FILE).display()
            )
            .into());
        }
    }
    let retired = out.join(RETIRED_RECORD);
    match fs::rename(out.join(RECORD), &retired) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("the published record can't be retired: {error}").into()),
    }
    let kept = published_run(&retired, out);
    remove_record(out)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        let fd = file.as_raw_fd();
        // SAFETY: `fcntl` takes a file descriptor this process holds open
        // and touches no memory.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        // SAFETY: as above.
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
            return Err("the lock can't be passed to the run's children".into());
        }
    }
    Ok((file, kept))
}

/// The variable every process a run starts carries, naming the run's
/// `--out`, so a later run can find one that outlived its runner even if it
/// closed the inherited lock ([`survivors`]).
const RUN_MARK: &str = "ZAKURA_HEADER_ACCEPTANCE_OUT";

/// Sets [`RUN_MARK`] for this process, so every child inherits it. It runs
/// once, at the run's start, before any thread or child.
fn mark_descendants(out: &Path) {
    env::set_var(RUN_MARK, resolved(out));
}

/// The file under `--out` where the runner records the session of each
/// child it starts ([`track_sessions`]).
const SESSIONS_FILE: &str = "sessions";

/// Set by the runner to its [`SESSIONS_FILE`]: then every child [`child`]
/// starts leads a session of its own, recorded there, which everything it
/// starts shares unless it leaves on purpose. Unset, as in the xtask built
/// at S, whose children are process groups inside its own session.
static SESSIONS: OnceLock<PathBuf> = OnceLock::new();

/// Has every child this process starts lead a recorded session ([`SESSIONS`]),
/// and returns a guard that drops `out`'s [`SESSIONS_FILE`] when the run
/// ends with every child's descendants confirmed gone.
fn track_sessions(out: &Path) -> SessionsGuard {
    let guard = SessionsGuard::new(out);
    let _ = SESSIONS.set(guard.file.clone());
    guard
}

/// Drops `--out`'s [`SESSIONS_FILE`] once the run ends, but only if the run
/// confirmed every session recorded before it had ended
/// ([`SessionsGuard::cleared`], after [`clear_stale_runs`]) and none of its
/// own children's descendants escaped ([`ESCAPED`]). Otherwise a later run
/// still needs the file to find them.
struct SessionsGuard {
    /// The file.
    file: PathBuf,
    /// Whether the run confirmed every earlier session ended.
    cleared: bool,
}

impl SessionsGuard {
    /// A guard for `out`'s file that keeps it until [`Self::cleared`].
    fn new(out: &Path) -> Self {
        Self {
            file: out.join(SESSIONS_FILE),
            cleared: false,
        }
    }

    /// Records that every session recorded before this run has ended.
    fn cleared(&mut self) {
        self.cleared = true;
    }
}

impl Drop for SessionsGuard {
    /// Removes the file once earlier sessions are confirmed ended and
    /// nothing of this run's escaped.
    fn drop(&mut self) {
        if self.cleared && !ESCAPED.load(Ordering::Relaxed) {
            let _ = fs::remove_file(&self.file);
        }
    }
}

/// Appends `pid`'s session to `sessions` as `<boot ID> <session ID> <start
/// ticks>`: the boot and its leader's start time tell it from a later
/// session that reuses the number. `pid` is this process's unreaped child,
/// so its ID is still its own. Elsewhere than Linux, nothing is recorded.
fn record_session(sessions: &Path, pid: i32) -> Result<(), BoxError> {
    #[cfg(target_os = "linux")]
    {
        use std::io::Write as _;
        let boot = boot_id().ok_or("no boot ID")?;
        let start = start_ticks(pid).ok_or("no start time")?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(sessions)?;
        writeln!(file, "{boot} {pid} {start}")?;
        file.sync_all()?;
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (sessions, pid);
    Ok(())
}

/// This boot's ID.
#[cfg(target_os = "linux")]
fn boot_id() -> Option<String> {
    Some(
        fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .ok()?
            .trim()
            .to_owned(),
    )
}

/// `pid`'s start time, in clock ticks since boot, the 22nd field of its
/// `stat`.
#[cfg(target_os = "linux")]
fn start_ticks(pid: i32) -> Option<u64> {
    stat_field(&fs::read(format!("/proc/{pid}/stat")).ok()?, 22)
}

/// Field `n` (counting from 1, the PID) of a `/proc/<pid>/stat` line, read
/// after the command name's last `)`, which may hold anything.
#[cfg(target_os = "linux")]
fn stat_field<T: std::str::FromStr>(stat: &[u8], n: usize) -> Option<T> {
    let close = stat.iter().rposition(|&byte| byte == b')')?;
    let fields = std::str::from_utf8(&stat[close + 1..]).ok()?;
    // The fields after the name start at the third, the state.
    fields
        .split_whitespace()
        .nth(n.checked_sub(3)?)?
        .parse()
        .ok()
}

/// Every other process that a run of `out` started and that still lives,
/// with or without the lock, from `/proc`. Each is in a session the runner
/// recorded ([`record_session`]) this boot, still the one recorded (its
/// number not since taken by a new leader), and started no earlier than
/// that session; a process's `stat` shows its session even when it's
/// non-dumpable. Or it carries `out`'s [`RUN_MARK`] where its environment
/// can be read. Only a process that leaves its session on purpose and hides
/// its environment escapes both. It fails, rather than finding none, when
/// the session record or the process table can't be read in full: the
/// record unreadable or malformed, no boot ID, `/proc` unlisted, or a
/// process's `stat` unreadable or unparsable while it still exists.
/// Elsewhere than Linux, none.
fn survivors(out: &Path) -> Result<Vec<i32>, String> {
    #[cfg(target_os = "linux")]
    {
        let gone = |error: &std::io::Error| {
            error.kind() == std::io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH)
        };
        let record = match fs::read_to_string(out.join(SESSIONS_FILE)) {
            Ok(record) => record,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(format!("the session record can't be read: {error}")),
        };
        let boot = boot_id().ok_or("this boot's ID can't be read")?;
        let mut sessions: BTreeMap<i32, u64> = BTreeMap::new();
        for line in record.lines() {
            let malformed = || format!("the session record has a malformed line {line:?}");
            let mut fields = line.split_whitespace();
            let (Some(recorded_boot), Some(session), Some(start), None) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                return Err(malformed());
            };
            let (Ok(session), Ok(start)) = (session.parse::<i32>(), start.parse::<u64>()) else {
                return Err(malformed());
            };
            // A leader alive now with another start time means the recorded
            // session ended and its number was reused.
            let reused = start_ticks(session).is_some_and(|now| now != start);
            if recorded_boot == boot && !reused {
                sessions.insert(session, start);
            }
        }
        let mark = format!("{RUN_MARK}={}", resolved(out).display()).into_bytes();
        let me = i32::try_from(std::process::id()).unwrap_or_default();
        let entries =
            fs::read_dir("/proc").map_err(|error| format!("/proc can't be listed: {error}"))?;
        let mut found = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| format!("/proc can't be listed: {error}"))?;
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<i32>().ok())
            else {
                continue;
            };
            if pid == me {
                continue;
            }
            let stat = match fs::read(entry.path().join("stat")) {
                Ok(stat) => stat,
                Err(error) if gone(&error) => continue,
                Err(error) => return Err(format!("process {pid}'s stat can't be read: {error}")),
            };
            let (Some(session), Some(started)) =
                (stat_field::<i32>(&stat, 6), stat_field::<u64>(&stat, 22))
            else {
                return Err(format!("process {pid}'s stat can't be parsed"));
            };
            let in_session = sessions
                .get(&session)
                .is_some_and(|since| started >= *since);
            // An unreadable environment (a non-dumpable process) is covered
            // by its session.
            let marked = fs::read(entry.path().join("environ"))
                .is_ok_and(|environ| environ.split(|&byte| byte == 0).any(|var| var == mark));
            if in_session || marked {
                found.push(pid);
            }
        }
        Ok(found)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = out;
        Ok(Vec::new())
    }
}

/// The run directory the record at `record` names its artifacts in, under
/// `out`'s run directories, if it names one.
fn published_run(record: &Path, out: &Path) -> Option<PathBuf> {
    let record: Value = serde_json::from_str(&fs::read_to_string(record).ok()?).ok()?;
    let artifacts = Path::new(record.get("artifacts")?.as_str()?);
    let mut parts = artifacts.components();
    match (parts.next(), parts.next()) {
        (Some(runs), Some(id)) if runs.as_os_str() == RUNS => {
            Some(out.join(RUNS).join(id.as_os_str()))
        }
        _ => None,
    }
}

/// Removes the published record, the retired one and any half-published
/// copy.
fn remove_record(out: &Path) -> Result<(), BoxError> {
    for file in [RECORD, RETIRED_RECORD] {
        let path = out.join(file);
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    for entry in fs::read_dir(out)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".record.") && name.ends_with(".tmp") {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// Removes every earlier run's directory under `--out` but `kept` (the
/// last published record's), with the worktrees registered inside them,
/// deepest first, then prunes the registrations of worktrees whose
/// directories are gone. It runs under the lock, and refuses while any
/// process an earlier run started is alive ([`survivors`]), so nothing
/// still uses them.
fn clear_stale_runs(
    repo_root: &Path,
    options: &Options,
    kept: Option<&Path>,
) -> Result<(), BoxError> {
    // A process an earlier run started may outlive its runner without the
    // lock, so the earlier runs' directories are only cleared once none is
    // left.
    let runs = options.out.join(RUNS);
    let survivors = survivors(&options.out)?;
    if !survivors.is_empty() {
        return Err(format!(
            "processes {survivors:?} from an earlier run of {} are still alive",
            options.out.display()
        )
        .into());
    }
    // Every session recorded so far has ended, this run's earlier children
    // included.
    let sessions = options.out.join(SESSIONS_FILE);
    if sessions.exists() {
        fs::remove_file(sessions)?;
    }
    let Ok(entries) = fs::read_dir(&runs) else {
        return Ok(());
    };
    let logs = tempfile::tempdir()?;
    let scratch = Options {
        out: logs.path().to_owned(),
        deadline: options.deadline,
    };
    for entry in entries {
        let dir = entry?.path();
        if Some(dir.as_path()) == kept {
            continue;
        }
        remove_worktrees_under(repo_root, &scratch, &dir)?;
        fs::remove_dir_all(&dir)?;
    }
    git(
        &scratch,
        repo_root,
        &["worktree".as_ref(), "prune".as_ref()],
    )?;
    Ok(())
}

/// Publishes `run_dir`'s record as `--out`'s: copies it, with its artifacts
/// path made relative to `--out`, to a temporary file in `--out`, syncs it,
/// and renames it into place, which is the moment the run counts. The
/// run's result file stays in `run_dir`, which the record's artifacts path
/// names. Syncing the directory after the rename is best-effort: the rename
/// already happened, and nothing after it may fail the run.
fn publish_record(out: &Path, run_dir: &Path) -> Result<(), BoxError> {
    use std::io::Write as _;
    let id = run_dir
        .file_name()
        .ok_or("a run directory has a name")?
        .to_string_lossy()
        .into_owned();
    let mut record: Value = serde_json::from_str(&fs::read_to_string(run_dir.join(RECORD))?)?;
    let artifacts = record
        .get("artifacts")
        .and_then(Value::as_str)
        .ok_or("the record names no artifacts")?;
    if Path::new(artifacts).is_relative() {
        record["artifacts"] = json!(Path::new(RUNS).join(&id).join(artifacts));
    }
    let staged = out.join(format!(".record.{id}.tmp"));
    let mut file = fs::File::create(&staged)?;
    file.write_all(serde_json::to_string_pretty(&record)?.as_bytes())?;
    file.sync_all()?;
    drop(file);
    // The run's last decision: a signal seen here fails it with nothing
    // published, and one after it doesn't undo the publication.
    if let Err(error) = stop_if_interrupted() {
        let _ = fs::remove_file(&staged);
        return Err(error);
    }
    fs::rename(&staged, out.join(RECORD))?;
    if let Ok(dir) = fs::File::open(out) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// The hidden first argument that has this command run the experiments as
/// the xtask built from S's own checkout ([`hand_off`]).
const IN_SUBJECT: &str = "--in-subject";

/// How long the xtask built from S gets for the whole run, which takes
/// hours: each of its own children has the run's per-child deadline too.
const IN_SUBJECT_DEADLINE: Duration = Duration::from_secs(7 * 24 * 3600);

/// Builds xtask in `root`, S's own checkout at `commit`, and runs the
/// experiments with it ([`run_in_subject`]), so the code that runs every
/// task and reads its artifacts is S's, not the caller's, whatever the
/// caller's checkout held when it started. Returns the evaluator's log.
fn hand_off(root: &Path, options: &Options, commit: &str) -> Result<PathBuf, BoxError> {
    let logs = options.out.join("logs");
    let target = options.out.join("xtask-target");
    let cargo_path = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut build = Command::new(&cargo_path);
    build
        .args(["build", "--locked", "-p", "xtask", "--target-dir"])
        .arg(&target)
        .env("CARGO_TERM_COLOR", "never")
        .current_dir(root);
    child(&mut build, &logs.join("xtask-build.log"), options.deadline)
        .exited_ok()
        .map_err(|end| format!("building xtask at S {end}"))?;
    let mut inner = Command::new(target.join("debug").join("xtask"));
    inner
        .args(["header-acceptance", IN_SUBJECT, commit, "--out"])
        .arg(&options.out)
        .arg("--deadline-secs")
        .arg(options.deadline.as_secs().to_string())
        .env("CARGO", &cargo_path)
        .current_dir(root);
    let log = logs.join("in-subject.log");
    child(&mut inner, &log, IN_SUBJECT_DEADLINE)
        .exited_ok()
        .map_err(|end| {
            format!(
                "the run at S {end}:\n{}",
                fs::read_to_string(&log).unwrap_or_default()
            )
        })?;
    Ok(logs.join("evaluate.log"))
}

/// The experiments, as the xtask built from S runs them ([`hand_off`]).
/// Its `--out` is the run's own directory, and `repo_root`, where it was
/// built, must be the checkout of S inside it, clean and at `commit`. The
/// caller set that checkout up, removes it, and publishes the record only
/// if this run exits 0.
fn run_in_subject(
    repo_root: &Path,
    commit: &str,
    args: impl Iterator<Item = String>,
) -> Result<(), BoxError> {
    let options = options(repo_root, args)?;
    let root = options.out.join(SUBJECT_DIR);
    if resolved(repo_root) != resolved(&root) {
        return Err(format!(
            "{IN_SUBJECT} runs only as the xtask built in {}",
            root.display()
        )
        .into());
    }
    let contained = contain();
    let subject = subject_of(repo_root, &options)?;
    if subject.commit != commit {
        return Err(format!(
            "the runner's checkout is at {}, not S {commit}",
            subject.commit
        )
        .into());
    }
    let run_id = options
        .out
        .file_name()
        .ok_or("a run directory has a name")?
        .to_string_lossy()
        .into_owned();
    let log = run_at(repo_root, &options, &subject, contained, &run_id)?;
    stop_if_interrupted()?;
    if ESCAPED.load(Ordering::Relaxed) {
        return Err(
            "a child's descendants couldn't be confirmed gone, so the record can't be trusted"
                .into(),
        );
    }
    print!("{}", fs::read_to_string(&log)?);
    Ok(())
}

/// Runs the acceptance run `run_id` in `root`, a clean checkout of
/// `subject`, with everything under its run directory, `--out`: builds the
/// tool, plans, runs every task and evaluates, and returns the evaluator's
/// log.
fn run_at(
    root: &Path,
    options: &Options,
    subject: &Subject,
    contained: bool,
    run_id: &str,
) -> Result<PathBuf, BoxError> {
    let out = &options.out;
    let logs = out.join("logs");
    let artifacts = out.join("artifacts");
    if artifacts.exists() {
        fs::remove_dir_all(&artifacts)?;
    }
    fs::create_dir_all(&artifacts)?;
    // The directory names the run that created it and its subject, which
    // the reader requires, so another attempt's or another subject's
    // artifacts can't be read as this run's.
    fs::write(
        artifacts.join("run.json"),
        serde_json::to_string_pretty(&json!({
            "run_id": run_id,
            "subject": subject.commit,
            "subject_source": subject.source,
            "containment": containment(contained),
        }))?,
    )?;

    let build = out.join("build");
    let mut cargo = cargo(root, &build);
    cargo.args([
        "build",
        "--locked",
        "--profile",
        planted::PROFILE,
        "-p",
        "zakura-sim",
        "--bin",
        "zakura-sim",
        "--bin",
        "header-acceptance",
    ]);
    child(&mut cargo, &logs.join("build.log"), options.deadline)
        .exited_ok()
        .map_err(|end| format!("building the simulator {end}"))?;
    let tool = build.join(planted::PROFILE).join("header-acceptance");

    let plan_file = out.join("plan.json");
    let mut plan = Command::new(&tool);
    plan.arg("plan")
        .arg("--repo")
        .arg(root)
        .arg("--out")
        .arg(&plan_file);
    child(&mut plan, &logs.join("plan.log"), options.deadline)
        .exited_ok()
        .map_err(|end| format!("`header-acceptance plan` {end}"))?;
    let plan: Value = serde_json::from_str(&fs::read_to_string(&plan_file)?)?;
    let tasks = plan
        .get("tasks")
        .and_then(Value::as_array)
        .ok_or("the plan has no tasks")?;

    let worktree = out.join("worktree");
    add_worktree(root, options, &worktree, &subject.commit)?;
    let result = run_tasks(root, options, subject, tasks, &artifacts, &worktree);
    let removed = remove_worktree(root, options, &worktree);
    result?;
    removed?;
    stop_if_interrupted()?;
    if ESCAPED.load(Ordering::Relaxed) {
        return Err(
            "a timed-out child's descendants couldn't be confirmed gone, so the \
                    run's later artifacts can't be trusted"
                .into(),
        );
    }
    // The run's checkout must still be exactly S.
    if subject_of(root, options)?.commit != subject.commit {
        return Err("the run's checkout of S moved".into());
    }

    let mut evaluate = Command::new(&tool);
    evaluate
        .arg("evaluate")
        .arg("--repo")
        .arg(root)
        .arg("--plan")
        .arg(&plan_file)
        .arg("--artifacts")
        .arg(&artifacts)
        .arg("--subject")
        .arg(&subject.commit)
        .arg("--subject-source")
        .arg(&subject.source)
        .arg("--run-id")
        .arg(run_id)
        .arg("--containment")
        .arg(containment(contained))
        .arg("--out")
        .arg(out);
    let log = logs.join("evaluate.log");
    child(&mut evaluate, &log, options.deadline)
        .exited_ok()
        .map_err(|end| format!("`header-acceptance evaluate` {end}"))?;
    Ok(log)
}

/// The subject commit S and its source digest.
struct Subject {
    /// `HEAD`'s commit.
    commit: String,
    /// The checkout's source digest, as `zakura-sim`'s build script computes
    /// it, which every binary built from S embeds.
    source: String,
}

/// S, from a clean checkout: `git status` lists no change anywhere,
/// untracked files included, and git can compute its source digest. A dirty
/// checkout has no subject, since what the run reads or builds from it
/// couldn't be bound to a commit.
fn subject_of(repo_root: &Path, options: &Options) -> Result<Subject, BoxError> {
    // Its logs go outside the checkout, which must stay as it is.
    let logs = tempfile::tempdir()?;
    let status = logs.path().join("status.log");
    let mut git = planted::git(repo_root);
    git.args(["status", "--porcelain", "--untracked-files=all"]);
    child(&mut git, &status, options.deadline)
        .exited_ok()
        .map_err(|end| format!("`git status` {end}"))?;
    let changed = fs::read_to_string(&status)?;
    if !changed.trim().is_empty() {
        return Err(
            format!("header-acceptance needs a clean checkout, and S has:\n{changed}").into(),
        );
    }
    let read = source_digest(repo_root, options.deadline, &logs.path().join("digest.log"))?;
    if read.dirty || read.git == provenance::UNKNOWN || read.source == provenance::UNKNOWN {
        return Err(
            "header-acceptance needs a checkout whose source digest git can compute".into(),
        );
    }
    Ok(Subject {
        commit: read.git,
        source: read.source,
    })
}

/// A `cargo` command in `dir` with the simulator's flags and `target` as
/// its target directory, and uncolored output, which the runner parses.
fn cargo(dir: &Path, target: &Path) -> Command {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut command = Command::new(cargo);
    command
        .env("RUSTFLAGS", SIM_RUSTFLAGS)
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_BUILD_WARNINGS", "warn")
        .env("CARGO_TERM_COLOR", "never")
        .current_dir(dir);
    command
}

/// A checkout's provenance, as `zakura-sim`'s build script reads it.
struct SourceRead {
    /// `HEAD`'s commit, or [`provenance::UNKNOWN`].
    git: String,
    /// Whether a source path has a change.
    dirty: bool,
    /// The source digest, or [`provenance::UNKNOWN`].
    source: String,
}

/// How the run contained its children: `subreaper` under [`contain`], and
/// `best-effort` without it.
fn containment(contained: bool) -> &'static str {
    if contained {
        "subreaper"
    } else {
        "best-effort"
    }
}

/// A fresh ID for this run: the time it started, to the nanosecond, and
/// this process's ID, so two attempts never share one.
fn run_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    format!("{nanos:x}-{}", std::process::id())
}

/// This xtask binary, for the helpers it runs ([`source_digest`]), so a
/// rebuild of the caller's xtask during the run can't change the code a
/// helper runs, or remove it. On Linux it's `/proc/<this process's ID>/exe`,
/// which runs the file this process runs even after it's replaced or
/// deleted, with nothing copied. It names this process's ID, not `self`,
/// because a child execs it from inside its `sh` gate ([`gated`]), where
/// `/proc/self/exe` is the shell. Elsewhere it's `current_exe()`, whose
/// file a rebuild can replace: best-effort, like containment there, and a
/// best-effort record never counts for acceptance.
fn own_exe() -> Result<PathBuf, BoxError> {
    #[cfg(target_os = "linux")]
    {
        Ok(PathBuf::from(format!("/proc/{}/exe", std::process::id())))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(env::current_exe()?)
    }
}

/// `dir`'s provenance, read by this command's [`SOURCE_DIGEST`] mode in a
/// child under `deadline`, its output in `log`, so the git commands the
/// read runs are killed with it if it hangs.
fn source_digest(dir: &Path, deadline: Duration, log: &Path) -> Result<SourceRead, BoxError> {
    let mut read = Command::new(own_exe()?);
    read.arg("header-acceptance").arg(SOURCE_DIGEST).arg(dir);
    child(&mut read, log, deadline)
        .exited_ok()
        .map_err(|end| format!("reading {}'s source digest {end}", dir.display()))?;
    let output = fs::read_to_string(log)?;
    let value: Value = serde_json::from_str(output.lines().last().unwrap_or_default())?;
    Ok(SourceRead {
        git: text(&value, "git")?,
        dirty: value
            .get("dirty")
            .and_then(Value::as_bool)
            .ok_or("the digest read has no dirty flag")?,
        source: text(&value, "source")?,
    })
}

/// [`source_digest`] of `dir`, its log under the run's logs.
fn checkout_digest(options: &Options, dir: &Path) -> Result<SourceRead, BoxError> {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let log = options.out.join("logs").join(format!(
        "digest-{}.log",
        CALLS.fetch_add(1, Ordering::Relaxed)
    ));
    source_digest(dir, options.deadline, &log)
}

/// Runs `git <args>` in `dir` through [`child`], under the run's deadline,
/// with its output in its own log, and says how it failed if it didn't
/// exit 0.
fn git(options: &Options, dir: &Path, args: &[&OsStr]) -> Result<(), String> {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let log = options
        .out
        .join("logs")
        .join(format!("git-{}.log", CALLS.fetch_add(1, Ordering::Relaxed)));
    let mut command = planted::git(dir);
    command.args(args);
    child(&mut command, &log, options.deadline)
        .exited_ok()
        .map_err(|end| {
            let args: Vec<_> = args.iter().map(|arg| arg.to_string_lossy()).collect();
            format!("`git {}` {end}", args.join(" "))
        })
}

/// How a child ended, with its log.
struct Ended {
    /// The end.
    end: ChildEnd,
    /// Where its output went.
    log: PathBuf,
}

impl Ended {
    /// `Ok` if it exited 0, or the end and its log in words.
    fn exited_ok(&self) -> Result<(), String> {
        match self.end {
            ChildEnd::Exited(0) => Ok(()),
            ref end => Err(format!("{end}; see {}", self.log.display())),
        }
    }

    /// Whether it exited 0.
    fn exited(&self) -> bool {
        matches!(self.end, ChildEnd::Exited(0))
    }

    /// Whether it ran past its deadline, a signal killed it, or the run was
    /// interrupted.
    fn killed(&self) -> bool {
        matches!(
            self.end,
            ChildEnd::TimedOut | ChildEnd::Signaled(_) | ChildEnd::Interrupted
        )
    }
}

/// Whether INT, TERM or HUP arrived since the run installed its handler
/// (`sim_minimize::install_interrupt_handler`). From then on [`child`]
/// starts nothing and ends whatever it's running as its deadline would, and
/// the run fails without publishing. The flag is never cleared, so cleanup
/// that needs a child, such as removing worktrees, is left to the next
/// run's [`clear_stale_runs`], as after a kill.
fn interrupted() -> bool {
    sim_minimize::INTERRUPTED.load(Ordering::SeqCst)
}

/// Fails once the run was [`interrupted`], so a task stops before it writes
/// an artifact from an interrupted child's outcome, and the run before it
/// publishes.
fn stop_if_interrupted() -> Result<(), BoxError> {
    if interrupted() {
        Err("the run was interrupted".into())
    } else {
        Ok(())
    }
}

/// Runs `command` to its end or `deadline`, its output in `log`. Every child
/// the tool launches goes through here, so none runs without a deadline.
/// The child gets its own process group, and while it runs its
/// descendants are tracked ([`Tree`]), including those in process groups
/// of their own, such as the planted runner's nested supervisors. However
/// it ends, by exiting, by a signal or at the deadline, every tracked
/// descendant still running is killed, and the call waits until none is
/// left ([`sweep`]).
fn child(command: &mut Command, log: &Path, deadline: Duration) -> Ended {
    let session = SESSIONS.get();
    let ended = |end| Ended {
        end,
        log: log.to_owned(),
    };
    // The deadline counts from before the spawn, so no delay of this
    // process's can stretch it.
    let started = Instant::now();
    let mut spawned = || -> Result<std::process::Child, BoxError> {
        if let Some(parent) = log.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = fs::File::create(log)?;
        // Under recorded sessions the child is gated ([`gated`]): it waits,
        // in its own session, until that session is recorded.
        let mut gate = session.map(|_| gated(command));
        let command = gate.as_mut().unwrap_or(command);
        command.stdout(file.try_clone()?).stderr(file);
        if session.is_some() {
            command.stdin(Stdio::piped());
            own_session(command);
        } else {
            command.stdin(Stdio::null());
            own_process_group(command);
        }
        die_with_parent(command);
        Ok(command.spawn()?)
    };
    let (mut process, mut live) = {
        let _spawn = spawn_lock();
        if interrupted() {
            return ended(ChildEnd::Interrupted);
        }
        let Ok(process) = spawned() else {
            return ended(ChildEnd::Signaled(-1));
        };
        let live = Live(i32::try_from(process.id()).ok());
        if let Some(pid) = live.0 {
            LIVE.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(pid);
        }
        (process, live)
    };
    // A child whose session can't be recorded can't be found if the runner
    // dies, so it isn't run.
    if let Some(sessions) = session {
        let recorded = live
            .0
            .is_some_and(|pid| record_session(sessions, pid).is_ok());
        // Released only once recorded: until then it can't start anything.
        let released = recorded
            && process.stdin.take().is_some_and(|mut gate| {
                use std::io::Write as _;
                gate.write_all(b"go\n").is_ok()
            });
        if !released {
            let _ = process.kill();
            reap_within(&mut process, REAP_DEADLINE);
            ESCAPED.store(true, Ordering::Relaxed);
            return ended(ChildEnd::Signaled(-1));
        }
    }
    let mut tree = Tree::of(process.id());
    let mut scanned = Instant::now();
    loop {
        let waited = process.try_wait();
        // An exit first seen at or past the deadline may have come after it,
        // so it counts as a timeout, never as the child's own end.
        let late = started.elapsed() >= deadline;
        match waited {
            Ok(Some(status)) => {
                // The leader is reaped, so its ID may name another process
                // now: only its tracked descendants and groups are swept.
                tree.forget_root();
                live.release();
                sweep(&mut tree);
                return ended(match status.code() {
                    _ if interrupted() => ChildEnd::Interrupted,
                    _ if late => ChildEnd::TimedOut,
                    Some(code) => ChildEnd::Exited(code),
                    None => ChildEnd::Signaled(signal_of(status)),
                });
            }
            Ok(None) if late || interrupted() => {
                end_unreaped(&mut process, &mut tree, &mut live);
                return ended(if late {
                    ChildEnd::TimedOut
                } else {
                    ChildEnd::Interrupted
                });
            }
            Ok(None) => {
                // Under the subreaper a sweep finds every descendant from
                // the table itself, so there's nothing to track.
                if !SUBREAPER.load(Ordering::Relaxed) && scanned.elapsed() >= TRACK_EVERY {
                    if let Some(table) = processes() {
                        tree.grow(&table);
                    }
                    scanned = Instant::now();
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) => {
                end_unreaped(&mut process, &mut tree, &mut live);
                return ended(ChildEnd::Signaled(-1));
            }
        }
    }
}

/// Keeps a direct child of [`child`]'s in [`LIVE`] until it's reaped or
/// the call returns.
struct Live(Option<i32>);

impl Live {
    /// Drops the reaped child from [`LIVE`], since its ID may now name
    /// another process.
    fn release(&mut self) {
        if let Some(pid) = self.0.take() {
            LIVE.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&pid);
        }
    }
}

impl Drop for Live {
    /// Releases the child if the call returns before it's reaped.
    fn drop(&mut self) {
        self.release();
    }
}

/// Ends `process`, a child that's still unreaped (at its deadline, or when
/// it can't be waited on), and everything it started. Under the subreaper
/// it kills and reaps the child first, so the child's own children pass to
/// this process, then sweeps. Otherwise it sweeps the tracked tree while
/// the child still leads its group, then kills it.
fn end_unreaped(process: &mut std::process::Child, tree: &mut Tree, live: &mut Live) {
    if SUBREAPER.load(Ordering::Relaxed) {
        let _ = process.kill();
        if reap_within(process, REAP_DEADLINE) {
            tree.forget_root();
            live.release();
        }
        sweep(tree);
    } else {
        sweep(tree);
        let _ = process.kill();
        reap_within(process, REAP_DEADLINE);
    }
}

/// How long a killed child gets to be reaped.
const REAP_DEADLINE: Duration = Duration::from_secs(10);

/// Waits up to `deadline` for the killed `process` to end and reaps it.
/// One that doesn't, such as one stuck in uninterruptible I/O, sets
/// [`ESCAPED`], which fails the run, rather than hanging it.
fn reap_within(process: &mut std::process::Child, deadline: Duration) -> bool {
    let started = Instant::now();
    loop {
        match process.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) if started.elapsed() < deadline => thread::sleep(Duration::from_millis(20)),
            _ => {
                ESCAPED.store(true, Ordering::Relaxed);
                return false;
            }
        }
    }
}

/// How often [`child`] reads the process table to track a running child's
/// descendants.
const TRACK_EVERY: Duration = Duration::from_millis(500);

/// A child's process tree as last read: its leader, every descendant seen
/// and every process group one of them was in.
struct Tree {
    /// The leader, while it's unreaped.
    root: Option<i32>,
    /// Every process seen in the tree.
    pids: BTreeSet<i32>,
    /// Every process group a process in the tree was in.
    groups: BTreeSet<i32>,
}

impl Tree {
    /// The tree of the just-spawned leader `root`, its own group's leader.
    fn of(root: u32) -> Tree {
        let root = i32::try_from(root).ok();
        Tree {
            root,
            pids: root.into_iter().collect(),
            groups: root.into_iter().collect(),
        }
    }

    /// Drops the leader's ID once it's reaped, since it may then name
    /// another process. Its group stays tracked through its members.
    fn forget_root(&mut self) {
        if let Some(root) = self.root.take() {
            self.pids.remove(&root);
        }
    }

    /// Grows the tree to a fixed point over `table`: children of processes
    /// in it and members of groups in it.
    fn grow(&mut self, table: &[(i32, i32, i32)]) {
        loop {
            let before = (self.pids.len(), self.groups.len());
            for &(pid, parent, group) in table {
                if self.pids.contains(&parent) || self.groups.contains(&group) {
                    self.pids.insert(pid);
                    self.groups.insert(group);
                }
            }
            if (self.pids.len(), self.groups.len()) == before {
                break;
            }
        }
    }

    /// The processes in `table` in the tree, as `(pid, group)`.
    fn alive(&self, table: &[(i32, i32, i32)]) -> Vec<(i32, i32)> {
        table
            .iter()
            .filter(|(pid, _, group)| self.pids.contains(pid) || self.groups.contains(group))
            .map(|(pid, _, group)| (*pid, *group))
            .collect()
    }
}

/// Puts `command`'s child in a process group of its own.
#[cfg(unix)]
fn own_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt as _;
    command.process_group(0);
}

/// Without process groups, only the child itself can be killed.
#[cfg(not(unix))]
fn own_process_group(_: &mut Command) {}

/// `command`, behind a gate: a shell, in the same directory with the same
/// environment, that waits for a line on its standard input and then
/// `exec`s the command, with no input, in its own process, so the command
/// keeps the shell's session. [`child`] writes the line only once the
/// session is recorded, so nothing the command starts can exist before
/// then. If the runner dies first, the shell reads end of input and exits
/// without running it.
fn gated(command: &Command) -> Command {
    let mut gate = Command::new("sh");
    gate.args(["-c", "read -r _ && exec \"$@\" </dev/null", "sh"])
        .arg(command.get_program())
        .args(command.get_args());
    for (key, value) in command.get_envs() {
        match value {
            Some(value) => gate.env(key, value),
            None => gate.env_remove(key),
        };
    }
    if let Some(dir) = command.get_current_dir() {
        gate.current_dir(dir);
    }
    gate
}

/// Puts `command`'s child in a session of its own, which also makes it its
/// own process group's leader.
#[cfg(unix)]
fn own_session(command: &mut Command) {
    use std::os::unix::process::CommandExt as _;
    // SAFETY: the closure runs in the forked child before `exec` and calls
    // only `setsid`, which is async-signal-safe, and builds its error without
    // allocating.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

/// Without sessions, a process group.
#[cfg(not(unix))]
fn own_session(command: &mut Command) {
    own_process_group(command);
}

/// Has `command`'s child killed when this process dies
/// (`PR_SET_PDEATHSIG`), so a killed runner doesn't leave its direct
/// children running. What they started still holds `--out`'s lock
/// ([`lock_out`]) until it ends.
#[cfg(target_os = "linux")]
fn die_with_parent(command: &mut Command) {
    use std::os::unix::process::CommandExt as _;
    let parent = i32::try_from(std::process::id()).unwrap_or_default();
    // SAFETY: the closure runs in the forked child before `exec` and calls
    // only `prctl` and `getppid`, which are async-signal-safe, and builds its
    // error without allocating.
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // The parent died before the call, so it would never signal.
            if libc::getppid() != parent {
                return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
            }
            Ok(())
        });
    }
}

/// No parent-death signal elsewhere.
#[cfg(not(target_os = "linux"))]
fn die_with_parent(_: &mut Command) {}

/// The signal that ended a child.
#[cfg(unix)]
fn signal_of(status: std::process::ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt as _;
    status.signal().unwrap_or_default()
}

/// No signals outside Unix.
#[cfg(not(unix))]
fn signal_of(_: std::process::ExitStatus) -> i32 {
    0
}

/// Kills every process in `tree` still running, with each of their
/// process groups, and waits, up to a few seconds, until `ps` lists none of
/// them. The tree grows from each table first, so a descendant reparented
/// since the last read is still found through its group. Only groups with a
/// live member are signalled, so a group ID is never one that's gone and
/// reused. If `ps` fails, it kills the unreaped leader's group if it can,
/// and sets [`ESCAPED`], as it does when something outlives every round:
/// either fails the run.
#[cfg(unix)]
fn sweep(tree: &mut Tree) {
    #[cfg(target_os = "linux")]
    if SUBREAPER.load(Ordering::Relaxed) {
        sweep_owned();
        return;
    }
    for _ in 0..100 {
        let Some(table) = processes() else {
            if let Some(root) = tree.root {
                // SAFETY: `kill` has no memory-safety preconditions. The
                // group is the unreaped leader's own.
                unsafe {
                    libc::kill(-root, libc::SIGKILL);
                }
            }
            ESCAPED.store(true, Ordering::Relaxed);
            return;
        };
        tree.grow(&table);
        let alive = tree.alive(&table);
        if alive.is_empty() {
            return;
        }
        let groups: BTreeSet<i32> = alive.iter().map(|(_, group)| *group).collect();
        for group in groups
            .into_iter()
            .filter(|group| tree.groups.contains(group))
        {
            // SAFETY: `kill` has no memory-safety preconditions. The group
            // has a live member in the tree, so it's the child's own or one
            // its descendants made.
            unsafe {
                libc::kill(-group, libc::SIGKILL);
            }
        }
        for (pid, _) in alive {
            // SAFETY: as above, for a live process in the tree itself.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    // Still alive after every round.
    ESCAPED.store(true, Ordering::Relaxed);
}

/// Without process groups, only the child itself is killed.
#[cfg(not(unix))]
fn sweep(_: &mut Tree) {}

/// The sweep under the subreaper (Adam's containment rule, 2026-11-09).
///
/// Every orphan comes back to this process, so while anything a child
/// started is still alive, this process has a child it hasn't reaped,
/// running or a zombie: the survivor itself or its topmost ancestor. Only
/// this process reaps its children, so each one's ID stays its own until
/// then, and the sweep never signals an ID that could name another
/// process. It kills each such child, less other [`child`] calls' own
/// children, which ends every thread in it and passes its children to this
/// process, and reaps it once it's gone. It repeats until a read of `/proc`
/// finds no such child at all ([`children_of`]). If `/proc` can't be read,
/// or a child outlives every round, it sets [`ESCAPED`].
#[cfg(target_os = "linux")]
fn sweep_owned() {
    let me = i32::try_from(std::process::id()).unwrap_or_default();
    for _ in 0..500 {
        let spawn = spawn_lock();
        let others = live_children();
        let Some(children) = children_of(me) else {
            ESCAPED.store(true, Ordering::Relaxed);
            return;
        };
        let children: Vec<i32> = children
            .into_iter()
            .filter(|pid| !others.contains(pid))
            .collect();
        if children.is_empty() {
            // With no other call's child running, the kernel must agree
            // there's no child at all, so none is hidden from `/proc`.
            if !others.is_empty() || no_children() {
                return;
            }
            ESCAPED.store(true, Ordering::Relaxed);
            return;
        }
        for &pid in &children {
            // SAFETY: `kill` has no memory-safety preconditions. `pid` is
            // this process's unreaped child, so the ID is still its own.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
        for &pid in &children {
            let mut status = 0;
            // SAFETY: `waitpid` writes only `status`, a live local. It reaps
            // this process's own child once every thread in it is gone.
            unsafe {
                libc::waitpid(pid, &mut status, libc::WNOHANG);
            }
        }
        drop(spawn);
        thread::sleep(Duration::from_millis(20));
    }
    ESCAPED.store(true, Ordering::Relaxed);
}

/// The ID of every process whose parent is `me`, zombies included, from
/// `/proc`, or `None` if that can't be confirmed. Read under [`SPAWN`], this
/// misses none of `me`'s unreaped children: one that exists for the whole
/// listing is in it, and one that becomes `me`'s child during it was passed
/// over by a child of `me`'s that's still unreaped, which is. An entry whose
/// `stat` is gone (`ENOENT`, `ESRCH`) has been reaped, so isn't one. Any
/// other read failure, or a `stat` it can't parse, makes it `None`.
#[cfg(target_os = "linux")]
fn children_of(me: i32) -> Option<Vec<i32>> {
    let mut children = Vec::new();
    for entry in fs::read_dir("/proc").ok()? {
        let entry = entry.ok()?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        else {
            continue;
        };
        let stat = match fs::read(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(libc::ESRCH) =>
            {
                continue
            }
            Err(_) => return None,
        };
        if parent_in_stat(&stat)? == me {
            children.push(pid);
        }
    }
    Some(children)
}

/// Whether the kernel says `me` has no child at all, as an empty
/// [`children_of`] listing claims: `waitid` on every child fails with
/// `ECHILD` only then. It leaves any child unreaped (`WNOWAIT`).
#[cfg(target_os = "linux")]
fn no_children() -> bool {
    // SAFETY: an all-zero `siginfo_t` is a valid value for `waitid` to
    // overwrite.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: `waitid` writes only `info`, a live local.
    let found = unsafe {
        libc::waitid(
            libc::P_ALL,
            0,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    found == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD)
}

/// The parent ID in a `/proc/<pid>/stat` line, read as bytes. The command
/// name before it is parenthesized, may hold anything and may be cut off
/// mid-character, so the fields are read after its last `)`: the state, then
/// the parent.
#[cfg(target_os = "linux")]
fn parent_in_stat(stat: &[u8]) -> Option<i32> {
    let close = stat.iter().rposition(|&byte| byte == b')')?;
    let fields = std::str::from_utf8(&stat[close + 1..]).ok()?;
    fields.split_whitespace().nth(1)?.parse().ok()
}

/// Every live process, as `(pid, parent, group)`, from [`table`]. A zombie
/// waiting for its parent is dead, so it's left out.
#[cfg(unix)]
fn processes() -> Option<Vec<(i32, i32, i32)>> {
    Some(
        table()?
            .into_iter()
            .filter(|(_, _, _, zombie)| !zombie)
            .map(|(pid, parent, group, _)| (pid, parent, group))
            .collect(),
    )
}

/// Every process, as `(pid, parent, group, zombie)`, from `ps`, less `ps`
/// itself, or `None` if `ps` fails or takes more than [`PS_DEADLINE`], when
/// it's killed.
#[cfg(unix)]
fn table() -> Option<Vec<(i32, i32, i32, bool)>> {
    let _spawn = spawn_lock();
    table_locked()
}

/// [`table`], for a caller already holding [`SPAWN`].
#[cfg(unix)]
fn table_locked() -> Option<Vec<(i32, i32, i32, bool)>> {
    let mut ps = Command::new("ps")
        .args(["-A", "-o", "pid=,ppid=,pgid=,stat="])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    // Its output is read on its own thread, so a full pipe can't stall it.
    let mut pipe = ps.stdout.take()?;
    let reader = thread::spawn(move || {
        let mut stdout = String::new();
        std::io::Read::read_to_string(&mut pipe, &mut stdout).map(|_| stdout)
    });
    let started = Instant::now();
    loop {
        match ps.try_wait() {
            Ok(Some(status)) if status.success() => break,
            Ok(None) if started.elapsed() < PS_DEADLINE => {
                thread::sleep(Duration::from_millis(10));
            }
            _ => {
                let _ = ps.kill();
                let _ = ps.wait();
                return None;
            }
        }
    }
    let stdout = reader.join().ok()?.ok()?;
    let own = i32::try_from(ps.id()).ok();
    Some(
        stdout
            .lines()
            .filter_map(|line| {
                let fields: Vec<&str> = line.split_whitespace().collect();
                match fields.as_slice() {
                    [pid, parent, group, stat, ..] => Some((
                        pid.parse().ok()?,
                        parent.parse().ok()?,
                        group.parse().ok()?,
                        stat.starts_with('Z'),
                    )),
                    _ => None,
                }
            })
            .filter(|(pid, ..)| Some(*pid) != own)
            .collect(),
    )
}

/// Without `ps`, no process table.
#[cfg(not(unix))]
fn processes() -> Option<Vec<(i32, i32, i32)>> {
    None
}

/// How long [`processes`] waits for `ps`.
#[cfg(unix)]
const PS_DEADLINE: Duration = Duration::from_secs(5);

/// Runs every task in `tasks`, writing artifacts under `artifacts`: each
/// property's planted cases first, through [`run_planted`], then every
/// task in order.
fn run_tasks(
    repo_root: &Path,
    options: &Options,
    subject: &Subject,
    tasks: &[Value],
    artifacts: &Path,
    worktree: &Path,
) -> Result<(), BoxError> {
    // Each task directory names the task it's for, as the reader's
    // `task_binding` does, before anything is written into it, so the
    // reader takes a directory only as its own task's.
    for (index, task) in tasks.iter().enumerate() {
        let dir = artifacts.join(index.to_string());
        fs::create_dir_all(&dir)?;
        fs::write(
            dir.join(TASK_FILE),
            json!({ "index": index, "task": task }).to_string(),
        )?;
    }
    // Each property's cases, as `(task index, case name)`.
    let mut by_property: BTreeMap<String, Vec<(usize, String)>> = BTreeMap::new();
    for (index, task) in tasks.iter().enumerate() {
        if let Some(case) = task.get("case") {
            by_property
                .entry(text(case, "property")?)
                .or_default()
                .push((index, text(case, "name")?));
        }
    }
    for (property, cases) in &by_property {
        stop_if_interrupted()?;
        run_planted(repo_root, options, property, cases, artifacts)?;
    }
    let mut broken: Option<String> = None;
    for (index, task) in tasks.iter().enumerate() {
        stop_if_interrupted()?;
        let dir = artifacts.join(index.to_string());
        fs::create_dir_all(&dir)?;
        if let Some(test) = task.get("test") {
            run_test(
                repo_root,
                options,
                Some(&subject.source),
                test,
                &dir,
                &options.out.join("tests"),
            )?;
        } else if task.get("case").is_some() || task.get("integrity").is_some() {
            // A scratch worktree that failed once can't be trusted for the
            // tasks after it, which leave their artifacts missing, so their
            // results block and the rest of the run still records.
            if let Some(why) = &broken {
                fs::write(dir.join("setup.log"), format!("not run: {why}\n"))?;
                continue;
            }
            let ran = match (task.get("case"), task.get("integrity")) {
                (Some(case), _) => case_task(options, subject, case, &dir, worktree)?,
                (_, Some(integrity)) => run_integrity(options, subject, integrity, &dir, worktree),
                _ => unreachable!("the task is a case or an integrity mutation"),
            };
            stop_if_interrupted()?;
            if let Err(why) = ran {
                fs::write(dir.join("setup.log"), format!("{why}\n"))?;
                broken = Some(format!(
                    "the scratch worktree failed at task {index}: {why}"
                ));
            }
        } else {
            return Err(format!("task {index} is of no known kind: {task}").into());
        }
    }
    Ok(())
}

/// Runs `property`'s planted cases (each `(task index, case name)` in
/// `cases`) through `cargo xtask planted <property>`, and copies each
/// case's baseline and patched runs under its task's directory. The
/// runner's case directories are cleared first, so every trace copied is
/// this invocation's, and a killed invocation marks both runs of every case
/// [`KILLED`]. Each case's [`VERDICT_FILE`] holds the runner's own verdict
/// on it ([`case_verdict`]), which covers its campaign as well as its
/// traces. Then it runs `--targets` for the property and writes each case's
/// [`TARGET_FILE`] from it.
fn run_planted(
    repo_root: &Path,
    options: &Options,
    property: &str,
    cases: &[(usize, String)],
    artifacts: &Path,
) -> Result<(), BoxError> {
    let stem = property.replace('/', "-");
    let ended = planted_child(repo_root, options, property, false, &stem)?;
    stop_if_interrupted()?;
    let log = fs::read_to_string(&ended.log).unwrap_or_default();
    for (index, name) in cases {
        let dir = artifacts.join(index.to_string());
        let slot = planted::case_index(repo_root, property, name)?;
        copy_planted_runs(repo_root, slot, &dir)?;
        // A verdict line carries no time, so one may have been printed after
        // the deadline: a killed invocation's every case is killed.
        if ended.killed() {
            for run in ["baseline", "patched"] {
                fs::write(dir.join(run).join(KILLED), "")?;
            }
        }
        let refused = case_verdict(&log, property, name, ended.killed()).err();
        fs::write(
            dir.join(VERDICT_FILE),
            serde_json::to_string_pretty(
                &json!({ "property": property, "name": name, "refused": refused }),
            )?,
        )?;
    }
    let ended = planted_child(repo_root, options, property, true, &stem)?;
    stop_if_interrupted()?;
    let log = fs::read_to_string(&ended.log).unwrap_or_default();
    for (index, name) in cases {
        let check = target_check(&log, ended.killed(), property, name);
        fs::write(
            artifacts.join(index.to_string()).join(TARGET_FILE),
            serde_json::to_string_pretty(&check)?,
        )?;
    }
    Ok(())
}

/// Clears the planted runner's case directories, then runs `cargo xtask
/// planted <property>`, with `--targets` if `targets`, under the deadline.
fn planted_child(
    repo_root: &Path,
    options: &Options,
    property: &str,
    targets: bool,
    stem: &str,
) -> Result<Ended, BoxError> {
    clear_case_dirs(&repo_root.join("target/planted"))?;
    let mut planted = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    planted
        .current_dir(repo_root)
        .env("CARGO_TERM_COLOR", "never")
        .args(["xtask", "planted"])
        .arg(property);
    if targets {
        planted.arg("--targets");
    }
    let suffix = if targets { "-targets" } else { "" };
    let log = options
        .out
        .join("logs")
        .join(format!("planted-{stem}{suffix}.log"));
    Ok(child(&mut planted, &log, options.deadline))
}

/// Removes every `case-<n>` directory the planted runner left in `work`.
fn clear_case_dirs(work: &Path) -> Result<(), BoxError> {
    if !work.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(work)? {
        let path = entry?.path();
        let case = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("case-"));
        if case && path.is_dir() {
            fs::remove_dir_all(&path)?;
        }
    }
    Ok(())
}

/// What a case's [`TARGET_FILE`] says, from its property's `--targets`
/// run's output `log`: the fuzz target the run listed the case under, if
/// any, and why it wasn't confirmed there, if it wasn't. The runner lists
/// every target's cases (`<target> (<harness>): <property> <patch>, ...`)
/// before it runs them, then prints each case's verdict (`<property>
/// <patch>: <verdict>`), which must be a catch. A killed run confirms
/// nothing ([`case_verdict`]), and neither does one that listed no target
/// and didn't say the property has no case on one.
fn target_check(log: &str, killed: bool, property: &str, name: &str) -> Value {
    let suffix = format!("/qa/planted/{property}/{name}.patch");
    let is_case = |item: &str| {
        item.strip_prefix(property)
            .and_then(|rest| rest.strip_prefix(' '))
            .is_some_and(|patch| patch.ends_with(&suffix))
    };
    let check = |target: Option<&str>, refused: Option<&str>| json!({ "property": property, "name": name, "target": target, "refused": refused });
    let refused = |target: Option<&str>, why: &str| check(target, Some(why));
    // A killed run's listings may be cut short, so it confirms nothing,
    // not even that a case has no target.
    if killed {
        return refused(None, "the --targets run was killed, so it confirms nothing");
    }
    let listings: Vec<(&str, &str)> = log
        .lines()
        .filter_map(|line| {
            let (head, list) = line.split_once("): ")?;
            let (target, harness) = head.split_once(" (")?;
            let plain = |word: &str| !word.is_empty() && !word.contains(char::is_whitespace);
            (plain(target) && plain(harness)).then_some((target, list))
        })
        .collect();
    let Some((target, _)) = listings
        .iter()
        .find(|(_, list)| list.split(", ").any(is_case))
    else {
        let none = format!("no planted case for {property} runs on a fuzz target");
        return if listings.is_empty() && !log.contains(&none) {
            refused(None, "the --targets run listed no targets")
        } else {
            check(None, None)
        };
    };
    match case_verdict(log, property, name, killed) {
        Ok(()) => check(Some(target), None),
        Err(why) => refused(Some(target), &why),
    }
}

/// The planted runner's verdict on case `name` of `property` in its output
/// `log`: `Ok` if its last line ([`verdict_line`]) says it was caught, or
/// why not. A run that was `killed` at its deadline gives no verdict: its
/// lines carry no time, so any of them may have come after the deadline.
fn case_verdict(log: &str, property: &str, name: &str, killed: bool) -> Result<(), String> {
    if killed {
        return Err("the planted run hit its deadline, so no verdict in it counts".to_owned());
    }
    match verdict_line(log, property, name) {
        Some(verdict) if verdict == "caught" || verdict.starts_with("caught;") => Ok(()),
        Some(verdict) => Err(verdict.to_owned()),
        None => Err("the planted run gave the case no verdict".to_owned()),
    }
}

/// Case `name` of `property`'s last line in the planted runner's output
/// `log`, `<property> <patch>: <verdict>`, its verdict, or `None` if the run
/// printed none for it. The patch is read as the case's full path, ending
/// at the first `: ` after `qa/planted/<property>/<name>.patch`, so a
/// `: ` elsewhere in the path doesn't cut it short. A `--targets` run
/// prints `confirmed through <target>` for it first, so its last line is
/// the verdict.
fn verdict_line<'a>(log: &'a str, property: &str, name: &str) -> Option<&'a str> {
    let suffix = format!("/qa/planted/{property}/{name}.patch: ");
    log.lines().rev().find_map(|line| {
        let rest = line.strip_prefix(property)?.strip_prefix(' ')?;
        let end = rest.find(&suffix)? + suffix.len();
        Some(&rest[end..])
    })
}

/// `value[field]` as a string.
fn text(value: &Value, field: &str) -> Result<String, BoxError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("no {field} in {value}").into())
}

/// Runs one test by its exact name in `dir_root`, the checkout to run it
/// in, whose source digest is `source`, with its target directory under
/// `targets`, and writes `test.json`: whether the child exited 0, since the
/// reader counts an `ok` only if it did, and whether the test binary embeds
/// `source`.
fn run_test(
    dir_root: &Path,
    options: &Options,
    source: Option<&str>,
    test: &Value,
    out: &Path,
    targets: &Path,
) -> Result<(), BoxError> {
    let package = text(test, "package")?;
    let file = text(test, "file")?;
    let name = text(test, "test")?;
    let log = out.join("test.log");
    // Writes a `test.json` that ran nothing, with `why` as its log, so the
    // result blocks.
    let ran_nothing = |why: &str, killed: bool| -> Result<(), BoxError> {
        stop_if_interrupted()?;
        fs::write(&log, why)?;
        fs::write(
            out.join("test.json"),
            serde_json::to_string_pretty(&json!({
                "package": package,
                "file": file,
                "tests": [],
                "killed": killed,
                "exited_ok": false,
                "log": why,
                "binary_source": null,
                "checkout_source": source,
            }))?,
        )?;
        Ok(())
    };
    // The test runs by the target whose source is the declared file, which
    // isn't always the file's stem.
    let target = match test_target(options, dir_root, &package, &file) {
        Ok(target) => target,
        Err(why) => return ran_nothing(&why, false),
    };
    // Cargo builds the target and names its executable in its JSON
    // messages, so the binary run, and whose source is checked, is the
    // target's own, never one a log line names.
    let build_log = out.join("build.log");
    let mut build = cargo(dir_root, targets);
    build.args([
        "test",
        "--locked",
        "--profile",
        planted::PROFILE,
        "-p",
        &package,
    ]);
    match target.kind {
        TargetKind::Integration => build.args(["--test", &target.name]),
        TargetKind::Lib => build.arg("--lib"),
        TargetKind::Bin => build.args(["--bin", &target.name]),
    };
    build.args(["--no-run", "--message-format=json"]);
    let built = child(&mut build, &build_log, options.deadline);
    let messages = fs::read_to_string(&build_log).unwrap_or_default();
    if let Err(end) = built.exited_ok() {
        return ran_nothing(
            &format!("the test's build {end}\n{messages}"),
            built.killed(),
        );
    }
    let executable = match test_executable(&messages, &target) {
        Ok(executable) => executable,
        Err(why) => return ran_nothing(&why, false),
    };
    // Run as `cargo test` runs it: from the package's directory, with
    // cargo's environment.
    let package_dir = target
        .manifest
        .parent()
        .expect("a manifest path names a file in its package's directory");
    let mut run = Command::new(&executable);
    run.args(["--exact", &name])
        .current_dir(package_dir)
        .env("CARGO_MANIFEST_DIR", package_dir)
        .env(
            "CARGO",
            env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo")),
        )
        .env("RUSTFLAGS", SIM_RUSTFLAGS)
        .env("CARGO_TARGET_DIR", targets)
        .env("CARGO_TERM_COLOR", "never");
    let ended = child(&mut run, &log, options.deadline);
    stop_if_interrupted()?;
    let text_log = fs::read_to_string(&log).unwrap_or_default();
    let tests = libtest(&text_log);
    let binary_source = match (fs::read(&executable), source) {
        (Ok(bytes), Some(source)) if planted::names_source(&bytes, source).is_ok() => {
            source.to_owned()
        }
        _ => "not-embedded".to_owned(),
    };
    fs::write(
        out.join("test.json"),
        serde_json::to_string_pretty(&json!({
            "package": package,
            "file": file,
            "tests": tests,
            "killed": ended.killed(),
            "exited_ok": ended.exited(),
            "log": text_log,
            "binary_source": binary_source,
            "checkout_source": source,
        }))?,
    )?;
    Ok(())
}

/// An integration-test target, as `cargo metadata` gives it.
#[derive(Debug, PartialEq, Eq)]
struct TestTarget {
    /// The target's name.
    name: String,
    /// Its package's manifest.
    manifest: PathBuf,
    /// Its root source file.
    source: PathBuf,
    /// Which kind of target it is, so how cargo builds its tests.
    kind: TargetKind,
}

/// The kind of target a test is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TargetKind {
    /// An integration test, `--test <name>`.
    Integration,
    /// The library's unit tests, `--lib`.
    Lib,
    /// A binary's unit tests, `--bin <name>`.
    Bin,
}

impl TargetKind {
    /// The `kind` cargo gives the target.
    fn cargo_kind(self) -> &'static str {
        match self {
            TargetKind::Integration => "test",
            TargetKind::Lib => "lib",
            TargetKind::Bin => "bin",
        }
    }
}

/// The integration-test target of `package` whose source is `file`
/// (relative to the package), from `cargo metadata` run in `dir_root`
/// under the deadline ([`target_named`]).
fn test_target(
    options: &Options,
    dir_root: &Path,
    package: &str,
    file: &str,
) -> Result<TestTarget, String> {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let log = options.out.join("logs").join(format!(
        "metadata-{}.log",
        CALLS.fetch_add(1, Ordering::Relaxed)
    ));
    let mut metadata = cargo(dir_root, &options.out.join("tests"));
    metadata.args(["metadata", "--locked", "--format-version", "1", "--no-deps"]);
    child(&mut metadata, &log, options.deadline)
        .exited_ok()
        .map_err(|end| format!("`cargo metadata` {end}"))?;
    let output = fs::read_to_string(&log).map_err(|error| error.to_string())?;
    target_named(&output, package, file)
}

/// The `test` target of `package` whose source path is `file` under the
/// package's directory, in `cargo metadata`'s output `output` (its JSON
/// line). Cargo names a target and its file apart, as a `[[test]]` entry
/// can point a name at another file, so a target is found by its source,
/// never assumed from the file's stem.
fn target_named(output: &str, package: &str, file: &str) -> Result<TestTarget, String> {
    let metadata: Value = output
        .lines()
        .find(|line| line.starts_with('{'))
        .and_then(|line| serde_json::from_str(line).ok())
        .ok_or("`cargo metadata` printed no JSON")?;
    let packages = metadata
        .get("packages")
        .and_then(Value::as_array)
        .ok_or("the metadata has no packages")?;
    let found = packages
        .iter()
        .find(|p| p.get("name").and_then(Value::as_str) == Some(package))
        .ok_or_else(|| format!("no package {package}"))?;
    let manifest = found
        .get("manifest_path")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{package} has no manifest path"))?;
    let source = manifest
        .parent()
        .ok_or_else(|| format!("{package}'s manifest has no directory"))?
        .join(file);
    let targets: Vec<&Value> = found
        .get("targets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect();
    let of_kind = |kind: &str| -> Vec<(&str, PathBuf)> {
        targets
            .iter()
            .filter(|target| {
                target
                    .get("kind")
                    .and_then(Value::as_array)
                    .is_some_and(|kinds| kinds.iter().any(|k| k == kind))
            })
            .filter_map(|target| {
                Some((
                    target.get("name")?.as_str()?,
                    PathBuf::from(target.get("src_path")?.as_str()?),
                ))
            })
            .collect()
    };
    let integration: Vec<(&str, PathBuf)> = of_kind("test")
        .into_iter()
        .filter(|(_, src)| *src == source)
        .collect();
    let target = |(name, src): (&str, PathBuf), kind| TestTarget {
        name: name.to_owned(),
        manifest: manifest.clone(),
        source: src,
        kind,
    };
    match integration.as_slice() {
        [one] => return Ok(target(one.clone(), TargetKind::Integration)),
        [] => {}
        _ => {
            return Err(format!(
                "{package} has {} test targets with source {file}",
                integration.len()
            ))
        }
    }
    // A unit test in a source file belongs to the target that owns the
    // file by Cargo's layout, as the manifest counts its module path from:
    // `src/bin/<x>.rs` and `src/bin/<x>/` are binary `<x>`'s, `src/main.rs`
    // the default binary's, and any other file under `src/` the library's,
    // or with no library the default binary's. A package with both a
    // library and a default binary leaves such a file's owner ambiguous,
    // and it's refused.
    let Some(inside) = file.strip_prefix("src/") else {
        return Err(format!("{package} has no test target with source {file}"));
    };
    let root = manifest
        .parent()
        .ok_or_else(|| format!("{package}'s manifest has no directory"))?
        .join("src");
    let libs = of_kind("lib");
    let bins = of_kind("bin");
    let bin_at = |paths: &[PathBuf]| -> Vec<(&str, PathBuf)> {
        bins.iter()
            .filter(|(_, src)| paths.contains(src))
            .cloned()
            .collect()
    };
    let main_bin = bin_at(&[root.join("main.rs")]);
    let owner = if let Some(binary) = inside.strip_prefix("bin/") {
        let name = binary.split('/').next().unwrap_or(binary);
        let name = name.strip_suffix(".rs").unwrap_or(name);
        bin_at(&[
            root.join("bin").join(format!("{name}.rs")),
            root.join("bin").join(name).join("main.rs"),
        ])
        .into_iter()
        .map(|bin| (bin, TargetKind::Bin))
        .collect::<Vec<_>>()
    } else if inside == "main.rs" {
        main_bin
            .into_iter()
            .map(|bin| (bin, TargetKind::Bin))
            .collect()
    } else if inside == "lib.rs" {
        libs.into_iter().map(|lib| (lib, TargetKind::Lib)).collect()
    } else {
        match (libs.as_slice(), main_bin.as_slice()) {
            ([lib], []) => vec![(lib.clone(), TargetKind::Lib)],
            ([], [bin]) => vec![(bin.clone(), TargetKind::Bin)],
            _ => Vec::new(),
        }
    };
    match owner.as_slice() {
        [(one, kind)] => Ok(target(one.clone(), *kind)),
        _ => Err(format!(
            "{package} has {} targets that could own {file}'s unit tests",
            owner.len()
        )),
    }
}

/// The executable cargo built for `target`, from the JSON messages of its
/// `cargo test --no-run --message-format=json` in `messages`: the one
/// `compiler-artifact` whose manifest and source are the target's and whose
/// kind is `test`. Any other artifact, of another package or target, is
/// never taken for it, and none or two is refused.
fn test_executable(messages: &str, target: &TestTarget) -> Result<PathBuf, String> {
    let found: Vec<PathBuf> = messages
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|message| {
            let built = &message["target"];
            message["reason"] == "compiler-artifact"
                && message["manifest_path"].as_str().map(Path::new) == Some(&target.manifest)
                && built["src_path"].as_str().map(Path::new) == Some(&target.source)
                && built["kind"]
                    .as_array()
                    .is_some_and(|kinds| kinds.iter().any(|kind| kind == target.kind.cargo_kind()))
                && message["profile"]["test"] == true
        })
        .filter_map(|message| message["executable"].as_str().map(PathBuf::from))
        .collect();
    match found.as_slice() {
        [executable] => Ok(executable.clone()),
        _ => Err(format!(
            "cargo built {} executables for {}",
            found.len(),
            target.source.display()
        )),
    }
}

/// The tests libtest reported in `log`, as `(name, ok|FAILED|ignored)`.
fn libtest(log: &str) -> Vec<(String, String)> {
    let mut tests = Vec::new();
    for line in log.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("test ") {
            if let Some((name, result)) = rest.rsplit_once(" ... ") {
                // An ignored test may carry its reason: `ignored, slow`.
                let result = result
                    .split(|c: char| c.is_whitespace() || c == ',')
                    .next()
                    .unwrap_or_default();
                if ["ok", "FAILED", "ignored"].contains(&result) {
                    tests.push((name.to_owned(), result.to_owned()));
                }
            }
        }
    }
    tests
}

/// Copies the planted runner's baseline and patched runs of its case
/// `slot` ([`planted::case_index`]) into `dir`, each whole trace directory
/// ([`copy_tree`]). A run the invocation didn't write is left without its
/// files, so it reads as missing.
fn copy_planted_runs(repo_root: &Path, slot: usize, dir: &Path) -> Result<(), BoxError> {
    let scratch = repo_root
        .join("target/planted")
        .join(format!("case-{slot}"));
    for run in ["baseline", "patched"] {
        let to = dir.join(run);
        fs::create_dir_all(&to)?;
        copy_tree(&scratch.join(run), &to)?;
    }
    Ok(())
}

/// Copies every file under `from` to the same path under `to`: a trace
/// directory's `trace.jsonl`, `witness.json`, `input.bin` and any blob its
/// meta names, so the copy reads as the original does. A missing `from`
/// copies nothing.
fn copy_tree(from: &Path, to: &Path) -> Result<(), BoxError> {
    if !from.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(from)? {
        let path = entry?.path();
        let target = to.join(path.file_name().unwrap_or_default());
        if path.is_dir() {
            fs::create_dir_all(&target)?;
            copy_tree(&path, &target)?;
        } else {
            fs::copy(&path, &target)?;
        }
    }
    Ok(())
}

/// Runs planted case `case`'s rebuild ([`rebuild_case`]) in a target
/// directory of its own, `cases/<task index>` under the run directory, then
/// removes that directory whatever the rebuild did, a missing one counting
/// as removed. Nothing reads the binary after its provenance check, so each
/// case's build (gigabytes) lasts only through it. hdr-14.7b, which records
/// the `Draft` run from that binary, moves the removal after the recording.
/// The outer error is the run's, a target it couldn't remove. The inner
/// result is the rebuild's, a setup failure of this task.
fn case_task(
    options: &Options,
    subject: &Subject,
    case: &Value,
    dir: &Path,
    worktree: &Path,
) -> Result<Result<(), BoxError>, BoxError> {
    let target = options
        .out
        .join("cases")
        .join(dir.file_name().unwrap_or_default());
    let rebuilt = rebuild_case(options, subject, case, dir, worktree, &target);
    match fs::remove_dir_all(&target) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(
                format!("case target {} can't be removed: {error}", target.display()).into(),
            )
        }
    }
    stop_if_interrupted()?;
    Ok(rebuilt)
}

/// Rebuilds case `case`'s patch in `target`, for its `Draft` run, and
/// checks the binary embeds the patched checkout's source digest (C5),
/// writing the check to `dir`'s `provenance.json`. A patch that doesn't
/// apply or build is a refused check, not an error. The `Draft` recording
/// itself needs C9's override in `record`, so it waits for hdr-14.7b.
fn rebuild_case(
    options: &Options,
    subject: &Subject,
    case: &Value,
    dir: &Path,
    worktree: &Path,
    target: &Path,
) -> Result<(), BoxError> {
    reset(options, worktree, &subject.commit)?;
    let patch = worktree.join(text(case, "patch")?);
    let applied = git(options, worktree, &["apply".as_ref(), patch.as_os_str()]);
    let refused = match applied {
        Err(why) => Some(format!("the patch doesn't apply: {why}")),
        Ok(()) => {
            let mut build = planted::cargo_sim(worktree, target);
            let ended = child(&mut build, &dir.join("build.log"), options.deadline);
            match ended.exited_ok() {
                Err(why) => Some(format!("the patched build {why}")),
                Ok(()) => match checkout_digest(options, worktree) {
                    Err(why) => Some(why.to_string()),
                    Ok(read) => {
                        let binary = target.join(planted::PROFILE).join("zakura-sim");
                        match fs::read(&binary) {
                            Ok(bytes) => planted::names_source(&bytes, &read.source).err(),
                            Err(error) => Some(format!("{}: {error}", binary.display())),
                        }
                    }
                },
            }
        }
    };
    let source = checkout_digest(options, worktree)
        .map_or_else(|_| provenance::UNKNOWN.to_owned(), |read| read.source);
    stop_if_interrupted()?;
    // The patch the rebuild applied, which the reader compares with the
    // case's committed patch at S.
    let patch_sha256: String = {
        use sha2::{Digest, Sha256};
        fs::read(&patch)
            .map(|bytes| {
                Sha256::digest(bytes)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect()
            })
            .unwrap_or_else(|_| provenance::UNKNOWN.to_owned())
    };
    fs::write(
        dir.join("provenance.json"),
        serde_json::to_string_pretty(
            &json!({ "source": source, "patch_sha256": patch_sha256, "refused": refused }),
        )?,
    )?;
    reset(options, worktree, &subject.commit)
}

/// Runs integrity mutation `integrity`'s history test unpatched, patched
/// and restored in `worktree`, writing each run's `test.json`. A patch
/// that doesn't apply or revert is this task's failure, not the run's: it
/// writes its reason to `setup.log` and leaves that run and the rest
/// without artifacts, so the result blocks.
fn run_integrity(
    options: &Options,
    subject: &Subject,
    integrity: &Value,
    dir: &Path,
    worktree: &Path,
) -> Result<(), BoxError> {
    reset(options, worktree, &subject.commit)?;
    let test = json!({
        "package": text(integrity, "package")?,
        "file": text(integrity, "file")?,
        "test": text(integrity, "test")?,
    });
    let targets = options.out.join("native");
    let patch = worktree.join(text(integrity, "patch_file")?);
    for run in ["unpatched", "patched", "restored"] {
        let setup = match run {
            "patched" => git(options, worktree, &["apply".as_ref(), patch.as_os_str()]),
            "restored" => git(
                options,
                worktree,
                &["apply".as_ref(), "-R".as_ref(), patch.as_os_str()],
            ),
            _ => Ok(()),
        };
        if let Err(why) = setup {
            stop_if_interrupted()?;
            fs::write(dir.join("setup.log"), format!("{run}: {why}\n"))?;
            break;
        }
        // Each run's binary must embed the checkout it ran in: S's, or the
        // patched one's.
        let source = checkout_digest(options, worktree)
            .ok()
            .map(|read| read.source)
            .filter(|source| source != provenance::UNKNOWN);
        let out = dir.join(run);
        fs::create_dir_all(&out)?;
        run_test(worktree, options, source.as_deref(), &test, &out, &targets)?;
    }
    reset(options, worktree, &subject.commit)
}

/// Adds a worktree at `worktree`, detached at `commit`.
fn add_worktree(
    repo_root: &Path,
    options: &Options,
    worktree: &Path,
    commit: &str,
) -> Result<(), BoxError> {
    if worktree.exists() {
        return Err(format!(
            "{} already exists in a fresh run directory",
            worktree.display()
        )
        .into());
    }
    git(
        options,
        repo_root,
        &[
            "worktree".as_ref(),
            "add".as_ref(),
            "--detach".as_ref(),
            worktree.as_os_str(),
            commit.as_ref(),
        ],
    )?;
    Ok(())
}

/// Removes every worktree of `repo_root`'s repository registered at or
/// under `under`, a run directory, deepest first, so one a killed child
/// left inside another (the planted runner's, inside S's checkout) is
/// unregistered before the checkout holding it is deleted. A registration
/// whose directory is already gone is removed too. A run directory is the
/// run's own, created fresh, so everything registered in it is the run's.
fn remove_worktrees_under(
    repo_root: &Path,
    options: &Options,
    under: &Path,
) -> Result<(), BoxError> {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let log = options.out.join("logs").join(format!(
        "worktrees-{}.log",
        CALLS.fetch_add(1, Ordering::Relaxed)
    ));
    let mut list = planted::git(repo_root);
    list.args(["worktree", "list", "--porcelain"]);
    child(&mut list, &log, options.deadline)
        .exited_ok()
        .map_err(|end| format!("`git worktree list` {end}"))?;
    // Git prints real paths, so `under` is compared as given and resolved.
    let roots = [resolved(under), under.to_owned()];
    let mut found: Vec<PathBuf> = fs::read_to_string(&log)?
        .lines()
        .filter_map(|line| line.strip_prefix("worktree "))
        .map(PathBuf::from)
        .filter(|path| roots.iter().any(|root| path.starts_with(root)))
        .collect();
    found.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    found.dedup();
    for path in &found {
        remove_worktree(repo_root, options, path)?;
    }
    Ok(())
}

/// `path` with its deepest existing ancestor resolved to its real path,
/// and the rest, which doesn't exist yet, kept as given.
fn resolved(path: &Path) -> PathBuf {
    let mut existing = path;
    let mut rest = Vec::new();
    while !existing.exists() {
        let (Some(parent), Some(name)) = (existing.parent(), existing.file_name()) else {
            return path.to_owned();
        };
        rest.push(name.to_owned());
        existing = parent;
    }
    let mut real = fs::canonicalize(existing).unwrap_or_else(|_| existing.to_owned());
    real.extend(rest.iter().rev());
    real
}

/// Refuses an `--out` inside the checkout that git doesn't ignore: the
/// run's own output would then make the next run's checkout of S look
/// changed, and its subject check fail. An ignored one, such as the default
/// under `target/`, or one outside the checkout, is fine.
fn check_out_dir(repo_root: &Path, options: &Options) -> Result<(), BoxError> {
    let out = resolved(&options.out);
    if !out.starts_with(resolved(repo_root)) {
        return Ok(());
    }
    let logs = tempfile::tempdir()?;
    // A directory pattern matches a path that doesn't exist yet only
    // through a path inside it, so this asks about a name nothing else
    // would ignore: the directory itself must be.
    let mut ignored = planted::git(repo_root);
    ignored
        .args(["check-ignore", "-q"])
        .arg(out.join(".header-acceptance-probe"));
    if child(
        &mut ignored,
        &logs.path().join("check-ignore.log"),
        options.deadline,
    )
    .exited()
    {
        return Ok(());
    }
    Err(format!(
        "`--out` {} is inside the checkout and not ignored by git, so the run's output \
         would make S look changed; pick an ignored directory or one outside it",
        options.out.display()
    )
    .into())
}

/// Removes the worktree at `worktree`.
fn remove_worktree(repo_root: &Path, options: &Options, worktree: &Path) -> Result<(), BoxError> {
    git(
        options,
        repo_root,
        &[
            "worktree".as_ref(),
            "remove".as_ref(),
            "--force".as_ref(),
            worktree.as_os_str(),
        ],
    )?;
    Ok(())
}

/// Resets `worktree` to `commit`, dropping every change and untracked file.
fn reset(options: &Options, worktree: &Path, commit: &str) -> Result<(), BoxError> {
    git(
        options,
        worktree,
        &[
            "reset".as_ref(),
            "--hard".as_ref(),
            "--quiet".as_ref(),
            commit.as_ref(),
        ],
    )?;
    git(options, worktree, &["clean".as_ref(), "-fdq".as_ref()])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// libtest's report gives each test's name and result. A `Running`
    /// line, which a nested `cargo test` could print too, says nothing.
    #[test]
    fn libtest_output_is_read_by_name() {
        let log = "     Running tests/other.rs (target/deps/other-0123)\n\nrunning 1 test\n\
                   test every_referenced_test_exists ... ok\n\ntest result: ok. 1 passed;";
        assert_eq!(
            libtest(log),
            vec![("every_referenced_test_exists".to_owned(), "ok".to_owned())]
        );
        assert_eq!(
            libtest("test a ... FAILED\ntest b ... ignored, slow\ntest c ... other"),
            vec![
                ("a".to_owned(), "FAILED".to_owned()),
                ("b".to_owned(), "ignored".to_owned())
            ]
        );
    }

    /// The executable run is the one cargo built for the target, by its
    /// manifest, source and kind: never another package's target with the
    /// same file, another target of the package, or a non-test artifact,
    /// whichever comes last.
    #[test]
    fn a_test_s_executable_is_its_own_target_s() {
        let target = TestTarget {
            name: "expected".into(),
            manifest: "/repo/crates/zakura-sim/Cargo.toml".into(),
            source: "/repo/crates/zakura-sim/tests/expected.rs".into(),
            kind: TargetKind::Integration,
        };
        let built = |manifest: &str, source: &str, kind: &str, test: bool, executable: &str| {
            json!({
                "reason": "compiler-artifact",
                "manifest_path": manifest,
                "target": {"kind": [kind], "src_path": source},
                "profile": {"test": test},
                "executable": executable,
            })
            .to_string()
        };
        let artifact = |manifest: &str, source: &str, kind: &str, executable: &str| {
            built(manifest, source, kind, true, executable)
        };
        let own = artifact(
            "/repo/crates/zakura-sim/Cargo.toml",
            "/repo/crates/zakura-sim/tests/expected.rs",
            "test",
            "/t/deps/expected-1",
        );
        let others = [
            artifact(
                "/repo/crates/xtask/Cargo.toml",
                "/repo/crates/zakura-sim/tests/expected.rs",
                "test",
                "/t/deps/expected-2",
            ),
            artifact(
                "/repo/crates/zakura-sim/Cargo.toml",
                "/repo/crates/zakura-sim/tests/other.rs",
                "test",
                "/t/deps/other-3",
            ),
            artifact(
                "/repo/crates/zakura-sim/Cargo.toml",
                "/repo/crates/zakura-sim/tests/expected.rs",
                "bin",
                "/t/deps/expected-4",
            ),
        ];
        let messages = format!("   Compiling zakura-sim\n{own}\n{}\n", others.join("\n"));
        assert_eq!(
            test_executable(&messages, &target),
            Ok(PathBuf::from("/t/deps/expected-1"))
        );
        assert!(test_executable(&others.join("\n"), &target).is_err());
        assert!(test_executable(&format!("{own}\n{own}"), &target).is_err());
        // A binary's unit tests: its test-profile artifact, never the binary
        // itself.
        let xtask = TestTarget {
            name: "xtask".into(),
            manifest: "/repo/crates/xtask/Cargo.toml".into(),
            source: "/repo/crates/xtask/src/main.rs".into(),
            kind: TargetKind::Bin,
        };
        let main = |test: bool, executable: &str| {
            built(
                "/repo/crates/xtask/Cargo.toml",
                "/repo/crates/xtask/src/main.rs",
                "bin",
                test,
                executable,
            )
        };
        let messages = format!(
            "{}\n{}\n",
            main(false, "/t/xtask"),
            main(true, "/t/deps/xtask-5")
        );
        assert_eq!(
            test_executable(&messages, &xtask),
            Ok(PathBuf::from("/t/deps/xtask-5"))
        );
        assert!(test_executable(&main(false, "/t/xtask"), &xtask).is_err());
    }

    /// A clean checkout has a subject, and every child goes through the
    /// deadline wrapper: one past its deadline ends killed.
    #[test]
    fn a_child_past_its_deadline_is_killed() {
        let dir = tempfile::tempdir().unwrap();
        let mut sleep = Command::new("sleep");
        sleep.arg("30");
        let ended = child(
            &mut sleep,
            &dir.path().join("sleep.log"),
            Duration::from_millis(200),
        );
        assert!(ended.killed(), "{}", ended.end);
        assert!(ended.exited_ok().is_err());
        let mut done = Command::new("true");
        assert!(child(
            &mut done,
            &dir.path().join("true.log"),
            Duration::from_secs(30)
        )
        .exited_ok()
        .is_ok());
        // An exit seen only at or past the deadline is a timeout, even a
        // successful one: with no time at all, every exit is.
        let mut late = Command::new("true");
        let ended = child(&mut late, &dir.path().join("late.log"), Duration::ZERO);
        assert_eq!(ended.end, ChildEnd::TimedOut);
    }

    /// A relative `--out` is made absolute against this process's
    /// directory, and an absolute one is kept.
    #[test]
    fn a_relative_out_is_made_absolute() {
        let repo = Path::new("/repo");
        let read = |args: &[&str]| {
            options(repo, args.iter().map(|arg| (*arg).to_owned()))
                .expect("the arguments parse")
                .out
        };
        assert_eq!(
            read(&["--out", "run"]),
            env::current_dir().unwrap().join("run")
        );
        assert_eq!(read(&["--out", "/abs/run"]), PathBuf::from("/abs/run"));
        assert_eq!(read(&[]), repo.join("target/header-acceptance"));
    }

    /// A case's numbering is the planted runner's own: its full patch path
    /// order, so `a-b.patch` comes before `a.patch` though `a` sorts before
    /// `a-b`, and a case of another property doesn't count.
    #[test]
    fn a_case_is_numbered_as_the_planted_runner_numbers_it() {
        let repo = tempfile::tempdir().unwrap();
        for (property, name) in [("p", "a"), ("p", "a-b"), ("q", "0")] {
            let dir = repo.path().join("qa/planted").join(property);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join(format!("{name}.patch")), "").unwrap();
            fs::write(dir.join(format!("{name}.toml")), "input = \"x.bin\"\n").unwrap();
        }
        assert_eq!(planted::case_index(repo.path(), "p", "a-b").unwrap(), 0);
        assert_eq!(planted::case_index(repo.path(), "p", "a").unwrap(), 1);
        assert_eq!(planted::case_index(repo.path(), "q", "0").unwrap(), 0);
        assert!(planted::case_index(repo.path(), "p", "c").is_err());
    }

    /// Old case directories are cleared before a planted run, so a run
    /// that writes nothing leaves a case's runs without their files, never
    /// an earlier invocation's.
    #[test]
    fn an_earlier_invocation_s_traces_are_never_copied() {
        let repo = tempfile::tempdir().unwrap();
        let work = repo.path().join("target/planted");
        let old = work.join("case-0/baseline");
        fs::create_dir_all(&old).unwrap();
        fs::write(old.join("trace.jsonl"), "old").unwrap();
        fs::create_dir_all(work.join("confirm-builds")).unwrap();
        clear_case_dirs(&work).unwrap();
        assert!(!work.join("case-0").exists());
        assert!(work.join("confirm-builds").exists());
        let dir = repo.path().join("task");
        copy_planted_runs(repo.path(), 0, &dir).unwrap();
        assert!(!dir.join("baseline/trace.jsonl").exists());
        assert!(dir.join("baseline").is_dir());
    }

    /// A case's target confirmation is read from its own listing and
    /// verdict lines, by its full patch name, and names its case: a catch
    /// confirms it, any other verdict or none refuses it, a case the run
    /// didn't list has no target, and a killed run or one that listed
    /// nothing confirms nothing. The runner prints `confirmed through` for a
    /// case before its verdict, which is its last line.
    #[test]
    fn a_target_confirmation_is_read_for_its_own_case() {
        let patch = |name: &str| format!("p /repo/qa/planted/p/{name}.patch");
        let log = |a_verdict: &str| {
            format!(
                "fuzz_a (header-sync/storage): {}, {}\n\
                 fuzz_b (header-sync/wire): no retained production bypass\n\
                 {}: confirmed through fuzz_a; replayed Exact\n\
                 {}: {a_verdict}\n{}: caught\n",
                patch("a"),
                patch("a-b"),
                patch("a"),
                patch("a"),
                patch("a-b"),
            )
        };
        let read = |log: &str, killed: bool, name: &str| {
            let check = target_check(log, killed, "p", name);
            assert_eq!(check["property"], "p");
            assert_eq!(check["name"], name);
            (check["target"].clone(), check["refused"].clone())
        };
        assert_eq!(
            read(
                &log("caught; the campaign caught it on 3 of 32 inputs"),
                false,
                "a"
            ),
            (json!("fuzz_a"), Value::Null)
        );
        // The prefix-named case keeps its own verdict.
        let failed = log("the patched run failed: it held");
        assert_eq!(
            read(&failed, false, "a"),
            (json!("fuzz_a"), json!("the patched run failed: it held"))
        );
        assert_eq!(read(&failed, false, "a-b"), (json!("fuzz_a"), Value::Null));
        // Listed and confirmed through the target, but with no verdict
        // after, isn't a catch.
        let unjudged = format!(
            "fuzz_a (header-sync/storage): {}\n{}: confirmed through fuzz_a\n",
            patch("a"),
            patch("a")
        );
        assert_eq!(
            read(&unjudged, false, "a").1,
            json!("confirmed through fuzz_a")
        );
        let silent = format!("fuzz_a (header-sync/storage): {}\n", patch("a"));
        assert_eq!(
            read(&silent, false, "a").1,
            json!("the planted run gave the case no verdict")
        );
        // Not listed under any target: none to confirm.
        assert_eq!(read(&log("caught"), false, "c"), (Value::Null, Value::Null));
        assert_eq!(
            read("no planted case for p runs on a fuzz target", false, "a"),
            (Value::Null, Value::Null)
        );
        // Another property's patch of the same name isn't this case.
        let other = "fuzz_a (header-sync/storage): q /repo/qa/planted/q/a.patch\n\
                     q /repo/qa/planted/q/a.patch: caught\n";
        assert_eq!(read(other, false, "a"), (Value::Null, Value::Null));
        // A killed run confirms nothing, even a case whose catch it printed,
        // since that line may have come after the deadline, or that a case
        // has no target, since its listings may be cut short.
        let killed = json!("the --targets run was killed, so it confirms nothing");
        assert_eq!(
            read(&log("caught"), true, "a"),
            (Value::Null, killed.clone())
        );
        let unrelated = "fuzz_b (header-sync/storage): p /repo/qa/planted/p/other.patch\n";
        assert_eq!(read(unrelated, true, "a"), (Value::Null, killed));
        assert_eq!(read(unrelated, false, "a"), (Value::Null, Value::Null));
        assert!(read("", true, "a").1.is_string());
        assert!(read("", false, "a").1.is_string());
    }

    /// A child past its deadline is killed with every descendant, even one
    /// that moved into a process group of its own, and the call returns
    /// only once that descendant is gone.
    #[cfg(unix)]
    #[test]
    fn a_deadline_kills_nested_process_groups() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("nested.pid");
        let script = format!(
            "perl -e 'setpgrp(0, 0); open(my $f, \">\", \"{}\"); print $f $$; close($f); \
             exec(\"sleep\", \"30\")' & wait",
            pid_file.display()
        );
        let mut shell = Command::new("sh");
        shell.args(["-c", &script]);
        // Let the nested group start before the deadline.
        let started = Instant::now();
        let ended = child(
            &mut shell,
            &dir.path().join("sh.log"),
            Duration::from_secs(2),
        );
        assert!(ended.killed(), "{}", ended.end);
        assert!(started.elapsed() < Duration::from_secs(20));
        let pid: i32 = fs::read_to_string(&pid_file)
            .expect("the nested process wrote its pid")
            .trim()
            .parse()
            .expect("a pid");
        let alive = processes()
            .expect("ps lists the processes")
            .iter()
            .any(|(p, _, _)| *p == pid);
        assert!(!alive, "the nested process {pid} outlived the deadline");
    }

    /// The planted runner's verdict on a case is its own line, by full
    /// patch name: a catch, with or without a campaign, passes, and any
    /// other verdict, another case's line, or none refuses it.
    #[test]
    fn a_case_verdict_is_read_from_its_own_line() {
        let log = "p /repo/qa/planted/p/a-b.patch: confirmed through fuzz_a\n\
                   p /repo/qa/planted/p/a-b.patch: caught\n\
                   p /repo/qa/planted/p/a.patch: the patched run failed: it held\n\
                   p /repo/qa/planted/p/c.patch: caught; the campaign caught it on 3 of 32 inputs\n\
                   q /repo/qa/planted/q/d.patch: caught\n";
        assert_eq!(case_verdict(log, "p", "a-b", false), Ok(()));
        assert_eq!(case_verdict(log, "p", "c", false), Ok(()));
        assert_eq!(
            case_verdict(log, "p", "a", false),
            Err("the patched run failed: it held".to_owned())
        );
        assert!(case_verdict(log, "p", "d", false).is_err());
        assert!(case_verdict(log, "q", "a", false).is_err());
        // An output directory whose path holds `: `.
        let odd = "p /runs/acceptance: one/subject/qa/planted/p/a.patch: caught\n";
        assert_eq!(case_verdict(odd, "p", "a", false), Ok(()));
        assert!(case_verdict(odd, "p", "b", false).is_err());
        assert!(case_verdict("", "p", "a", false).is_err());
        // A killed run gives no verdict, even for a case whose catch it
        // printed: the line may have come after the deadline.
        for name in ["a-b", "e"] {
            assert_eq!(
                case_verdict(log, "p", name, true),
                Err("the planted run hit its deadline, so no verdict in it counts".to_owned())
            );
        }
    }

    /// A test target is found by its source path, not its file's stem: a
    /// `[[test]]` named `expected` whose source is `tests/other.rs` isn't
    /// `tests/expected.rs`'s target. Another package's same file isn't
    /// either.
    #[test]
    fn a_test_target_is_found_by_its_source() {
        let target = |name: &str, path: &str, kind: &str| json!({"name": name, "kind": [kind], "src_path": path});
        let output = json!({"packages": [
            {
                "name": "zakura-sim",
                "manifest_path": "/repo/crates/zakura-sim/Cargo.toml",
                "targets": [
                    target("expected", "/repo/crates/zakura-sim/tests/other.rs", "test"),
                    target("renamed", "/repo/crates/zakura-sim/tests/expected.rs", "test"),
                    target("zakura_sim", "/repo/crates/zakura-sim/src/lib.rs", "lib"),
                ],
            },
            {
                "name": "xtask",
                "manifest_path": "/repo/crates/xtask/Cargo.toml",
                "targets": [target("expected", "/repo/crates/xtask/tests/expected.rs", "test")],
            },
        ]});
        let output = format!("warning: a note on stderr\n{output}\n");
        let name = |file: &str| target_named(&output, "zakura-sim", file).map(|target| target.name);
        assert_eq!(name("tests/expected.rs").as_deref(), Ok("renamed"));
        assert_eq!(name("tests/other.rs").as_deref(), Ok("expected"));
        assert_eq!(
            target_named(&output, "zakura-sim", "tests/expected.rs"),
            Ok(TestTarget {
                name: "renamed".into(),
                manifest: "/repo/crates/zakura-sim/Cargo.toml".into(),
                source: "/repo/crates/zakura-sim/tests/expected.rs".into(),
                kind: TargetKind::Integration,
            })
        );
        // A unit test in a source file is the library's, and with no
        // library, the one binary's.
        assert_eq!(
            target_named(&output, "zakura-sim", "src/harness/a.rs").map(|t| (t.name, t.kind)),
            Ok(("zakura_sim".to_owned(), TargetKind::Lib))
        );
        let bin_only = json!({"packages": [{
            "name": "xtask",
            "manifest_path": "/repo/crates/xtask/Cargo.toml",
            "targets": [target("xtask", "/repo/crates/xtask/src/main.rs", "bin")],
        }]})
        .to_string();
        assert_eq!(
            target_named(&bin_only, "xtask", "src/planted.rs"),
            Ok(TestTarget {
                name: "xtask".into(),
                manifest: "/repo/crates/xtask/Cargo.toml".into(),
                source: "/repo/crates/xtask/src/main.rs".into(),
                kind: TargetKind::Bin,
            })
        );
        let two_bins = json!({"packages": [{
            "name": "xtask",
            "manifest_path": "/repo/crates/xtask/Cargo.toml",
            "targets": [
                target("a", "/repo/crates/xtask/src/bin/a.rs", "bin"),
                target("b", "/repo/crates/xtask/src/bin/b.rs", "bin"),
            ],
        }]})
        .to_string();
        assert!(target_named(&two_bins, "xtask", "src/planted.rs").is_err());
        // A package with a library, a default binary and one under
        // `src/bin/`: each binary's file is its own, a file both could own
        // is refused, and the library's root is the library's.
        let mixed = json!({"packages": [{
            "name": "p",
            "manifest_path": "/repo/crates/p/Cargo.toml",
            "targets": [
                target("p", "/repo/crates/p/src/lib.rs", "lib"),
                target("p", "/repo/crates/p/src/main.rs", "bin"),
                target("worker", "/repo/crates/p/src/bin/worker.rs", "bin"),
                target("tool", "/repo/crates/p/src/bin/tool/main.rs", "bin"),
            ],
        }]})
        .to_string();
        let owner =
            |file: &str| target_named(&mixed, "p", file).map(|t| (t.name, t.kind, t.source));
        assert_eq!(
            owner("src/bin/worker.rs"),
            Ok((
                "worker".to_owned(),
                TargetKind::Bin,
                PathBuf::from("/repo/crates/p/src/bin/worker.rs")
            ))
        );
        assert_eq!(
            owner("src/bin/tool/util.rs").map(|(name, kind, _)| (name, kind)),
            Ok(("tool".to_owned(), TargetKind::Bin))
        );
        assert_eq!(
            owner("src/main.rs").map(|(_, kind, source)| (kind, source)),
            Ok((TargetKind::Bin, PathBuf::from("/repo/crates/p/src/main.rs")))
        );
        assert_eq!(
            owner("src/lib.rs").map(|(_, kind, _)| kind),
            Ok(TargetKind::Lib)
        );
        assert!(owner("src/shared.rs").is_err());
        assert!(owner("src/bin/missing.rs").is_err());
        assert!(target_named(&output, "zakura-sim", "tests/missing.rs").is_err());
        assert!(target_named(&output, "no-such-package", "tests/expected.rs").is_err());
        assert!(target_named("", "zakura-sim", "tests/expected.rs").is_err());
    }

    /// A planted run's whole trace directory is copied, nested files too,
    /// so the copy keeps the `input.bin` its trace names.
    #[test]
    fn a_planted_run_is_copied_whole() {
        let repo = tempfile::tempdir().unwrap();
        let run = repo.path().join("target/planted/case-0/baseline");
        fs::create_dir_all(run.join("blobs")).unwrap();
        for (file, text) in [
            ("trace.jsonl", "trace"),
            ("witness.json", "{}"),
            ("input.bin", "input"),
            ("blobs/fixture", "blob"),
        ] {
            fs::write(run.join(file), text).unwrap();
        }
        let dir = repo.path().join("task");
        copy_planted_runs(repo.path(), 0, &dir).unwrap();
        for (file, text) in [
            ("baseline/trace.jsonl", "trace"),
            ("baseline/input.bin", "input"),
            ("baseline/blobs/fixture", "blob"),
        ] {
            assert_eq!(fs::read_to_string(dir.join(file)).unwrap(), text, "{file}");
        }
        assert!(dir.join("patched").is_dir());
    }

    /// A child's descendant in a process group of its own is killed however
    /// the child ends: when it exits on its own, and when a signal kills
    /// it, as well as at the deadline.
    #[cfg(unix)]
    #[test]
    fn every_exit_kills_nested_process_groups() {
        for (ending, kill_self) in [("exits", "exit 0"), ("is killed", "kill -9 $$")] {
            let dir = tempfile::tempdir().unwrap();
            let pid_file = dir.path().join("nested.pid");
            let script = format!(
                "perl -e 'setpgrp(0, 0); open(my $f, \">\", \"{}\"); print $f $$; close($f); \
                 exec(\"sleep\", \"30\")' & sleep 2; {kill_self}",
                pid_file.display()
            );
            let mut shell = Command::new("sh");
            shell.args(["-c", &script]);
            let ended = child(
                &mut shell,
                &dir.path().join("sh.log"),
                Duration::from_secs(20),
            );
            assert!(
                !matches!(ended.end, ChildEnd::TimedOut),
                "{ending}: {}",
                ended.end
            );
            let pid: i32 = fs::read_to_string(&pid_file)
                .expect("the nested process wrote its pid")
                .trim()
                .parse()
                .expect("a pid");
            let alive = processes()
                .expect("ps lists the processes")
                .iter()
                .any(|(p, _, _)| *p == pid);
            assert!(
                !alive,
                "the nested process {pid} outlived a child that {ending}"
            );
        }
    }

    /// Helpers run this process's own binary: on Linux through its
    /// `/proc/<ID>/exe`, elsewhere its path.
    #[test]
    fn helpers_run_this_process_s_binary() {
        let exe = own_exe().unwrap();
        #[cfg(target_os = "linux")]
        assert_eq!(
            exe,
            PathBuf::from(format!("/proc/{}/exe", std::process::id()))
        );
        #[cfg(not(target_os = "linux"))]
        assert_eq!(exe, env::current_exe().unwrap());
        assert!(exe.exists());
    }

    /// The variable that has [`exe_runner_helper`] run, naming its
    /// directory.
    #[cfg(target_os = "linux")]
    const EXE_RUNNER: &str = "XTASK_EXE_RUNNER";

    /// The variable that has [`exe_marker_helper`] write its marker there.
    #[cfg(target_os = "linux")]
    const EXE_MARKER: &str = "XTASK_EXE_MARKER";

    /// A helper still runs the runner's own code after the runner's file is
    /// replaced and then removed: a copy of this test binary stands for the
    /// runner ([`exe_runner_helper`]), renames `true` over its own path,
    /// removes it, and then runs [`exe_marker_helper`] through [`own_exe`]
    /// and the `sh` gate. Through the file on disk that would run `true`,
    /// or nothing, and write no marker.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_helper_runs_the_runner_s_code_after_its_file_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let runner = dir.path().join("runner");
        fs::copy(env::current_exe().unwrap(), &runner).unwrap();
        // Another test's fork can briefly hold the copy's write descriptor
        // open, and exec refuses a file open for writing (`ETXTBSY`) until
        // that child execs, so the spawn is retried while it's busy.
        let started = Instant::now();
        let status = loop {
            match Command::new(&runner)
                .args([
                    "--exact",
                    "header_acceptance::tests::exe_runner_helper",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(EXE_RUNNER, dir.path())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
            {
                Err(error)
                    if error.raw_os_error() == Some(libc::ETXTBSY)
                        && started.elapsed() < Duration::from_secs(10) =>
                {
                    thread::sleep(Duration::from_millis(20));
                }
                spawned => break spawned.unwrap(),
            }
        };
        assert!(status.success());
        assert!(!runner.exists(), "the runner's file was removed");
        assert_eq!(
            fs::read_to_string(dir.path().join("end")).unwrap(),
            "exited with code 0"
        );
        assert!(
            dir.path().join("marker").exists(),
            "the helper ran the runner's code"
        );
    }

    /// The runner, run only by
    /// [`a_helper_runs_the_runner_s_code_after_its_file_is_replaced`].
    #[cfg(target_os = "linux")]
    #[test]
    fn exe_runner_helper() {
        let Some(dir) = env::var_os(EXE_RUNNER).map(PathBuf::from) else {
            return;
        };
        let _sessions = track_sessions(&dir);
        let runner = dir.join("runner");
        let replacement = dir.join("replacement");
        fs::copy("/bin/true", &replacement).unwrap();
        fs::rename(&replacement, &runner).unwrap();
        fs::remove_file(&runner).unwrap();
        let mut helper = Command::new(own_exe().unwrap());
        helper
            .args([
                "--exact",
                "header_acceptance::tests::exe_marker_helper",
                "--nocapture",
            ])
            .env(EXE_MARKER, dir.join("marker"));
        let ended = child(
            &mut helper,
            &dir.join("helper.log"),
            Duration::from_secs(60),
        );
        fs::write(dir.join("end"), ended.end.to_string()).unwrap();
    }

    /// Writes the marker [`EXE_MARKER`] names, run only by
    /// [`exe_runner_helper`].
    #[cfg(target_os = "linux")]
    #[test]
    fn exe_marker_helper() {
        if let Some(marker) = env::var_os(EXE_MARKER) {
            fs::write(marker, "ran").unwrap();
        }
    }

    /// A repository in a temp dir with one commit, for the worktree and
    /// subject controls.
    fn scratch_repo() -> tempfile::TempDir {
        let repo = tempfile::tempdir().unwrap();
        for args in [
            &["init", "-q"][..],
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "s",
            ],
        ] {
            assert!(planted::git(repo.path())
                .args(args)
                .status()
                .unwrap()
                .success());
        }
        repo
    }

    /// A case task removes its target directory whatever its rebuild did:
    /// after a refused patch, with or without a target built, and after a
    /// setup failure. A target it can't remove fails the run, not the task.
    #[test]
    fn a_case_task_drops_its_target_whatever_it_did() {
        let repo = scratch_repo();
        let head = planted::git(repo.path())
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        let commit = String::from_utf8(head.stdout).unwrap().trim().to_owned();
        let out = tempfile::tempdir().unwrap();
        let options = Options {
            out: out.path().to_owned(),
            deadline: Duration::from_secs(60),
        };
        let at = |commit: &str| Subject {
            commit: commit.to_owned(),
            source: "source".to_owned(),
        };
        let case = json!({ "patch": "missing.patch" });
        let dir = out.path().join("artifacts/0");
        let target = out.path().join("cases/0");
        let task = |subject: &Subject| {
            fs::create_dir_all(&dir).unwrap();
            case_task(&options, subject, &case, &dir, repo.path())
        };
        let refused = || {
            let check: Value =
                serde_json::from_str(&fs::read_to_string(dir.join("provenance.json")).unwrap())
                    .unwrap();
            check["refused"].as_str().unwrap_or_default().to_owned()
        };
        // No target was ever built: the patch doesn't apply.
        assert!(matches!(task(&at(&commit)), Ok(Ok(()))));
        assert!(refused().contains("doesn't apply"), "{}", refused());
        assert!(!target.exists());
        // A target left from building is removed too.
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("marker"), "built").unwrap();
        fs::remove_file(dir.join("provenance.json")).unwrap();
        assert!(matches!(task(&at(&commit)), Ok(Ok(()))));
        assert!(refused().contains("doesn't apply"));
        assert!(!target.exists());
        // A setup failure: the subject's commit doesn't exist.
        fs::create_dir_all(&target).unwrap();
        assert!(matches!(
            task(&at("0000000000000000000000000000000000000000")),
            Ok(Err(_))
        ));
        assert!(!target.exists());
        // A target that can't be removed, even as root: a file, not a
        // directory.
        fs::write(&target, "not a directory").unwrap();
        assert!(task(&at(&commit)).is_err());
    }

    /// The experiments run only as the xtask built in the runner's own
    /// checkout of S under `--out`: one built anywhere else, such as the
    /// caller's checkout, is refused before it does anything.
    #[test]
    fn only_the_xtask_built_at_s_runs_the_experiments() {
        let repo = scratch_repo();
        let out = tempfile::tempdir().unwrap();
        let run_dir = out.path().join(RUNS).join("run-1");
        fs::create_dir_all(&run_dir).unwrap();
        let args = ["--out".to_owned(), run_dir.display().to_string()];
        let Err(refused) = run_in_subject(repo.path(), "s", args.into_iter()) else {
            panic!("an xtask built outside the runner's checkout ran the experiments");
        };
        assert!(refused.to_string().contains("runs only as"), "{refused}");
        assert!(!run_dir.join("artifacts").exists());
    }

    /// An `--out` inside the checkout must be ignored by git, or the run's
    /// own output would fail the next run's subject check. One outside the
    /// checkout is fine.
    #[test]
    fn an_output_inside_the_checkout_must_be_ignored() {
        let repo = scratch_repo();
        let at = |out: PathBuf| Options {
            out,
            deadline: Duration::from_secs(60),
        };
        assert!(check_out_dir(repo.path(), &at(repo.path().join("acceptance"))).is_err());
        fs::write(repo.path().join(".gitignore"), "/acceptance/\n").unwrap();
        assert!(check_out_dir(repo.path(), &at(repo.path().join("acceptance"))).is_ok());
        let elsewhere = tempfile::tempdir().unwrap();
        assert!(check_out_dir(repo.path(), &at(elsewhere.path().join("run"))).is_ok());
    }

    /// The record invariant at each point a run can be killed: before it
    /// publishes, and between staging the copy and renaming it, there's no
    /// record, and the next start removes the staged copy. The rename
    /// publishes the record, its artifacts path made relative to `--out`,
    /// and the next start removes it before anything else.
    #[test]
    fn a_record_exists_only_once_its_run_published_it() {
        let out = tempfile::tempdir().unwrap();
        let run_dir = out.path().join(RUNS).join("run-1");
        fs::create_dir_all(&run_dir).unwrap();
        fs::write(
            run_dir.join(RECORD),
            json!({"subject": "s", "artifacts": "artifacts"}).to_string(),
        )
        .unwrap();
        fs::write(run_dir.join("results.json"), "{}").unwrap();
        // Killed before publishing.
        assert!(!out.path().join(RECORD).exists());
        assert_eq!(published_run(&out.path().join(RECORD), out.path()), None);
        // Killed between staging and the rename.
        let staged = out.path().join(".record.run-1.tmp");
        fs::write(&staged, "{}").unwrap();
        assert!(!out.path().join(RECORD).exists());
        remove_record(out.path()).unwrap();
        assert!(!staged.exists());
        // Published.
        publish_record(out.path(), &run_dir).unwrap();
        let record: Value =
            serde_json::from_str(&fs::read_to_string(out.path().join(RECORD)).unwrap()).unwrap();
        assert_eq!(record["artifacts"], json!("runs/run-1/artifacts"));
        assert_eq!(record["subject"], json!("s"));
        assert_eq!(
            published_run(&out.path().join(RECORD), out.path()),
            Some(run_dir.clone())
        );
        assert!(fs::read_dir(out.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")));
        // The next start retires it as its first change to `--out`, and
        // keeps the run directory it names.
        let (_lock, kept) = lock_out(out.path()).unwrap();
        assert_eq!(kept, Some(run_dir.clone()));
        assert!(!out.path().join(RECORD).exists());
        assert!(!out.path().join(RETIRED_RECORD).exists());
        assert!(run_dir.join("results.json").exists());
    }

    /// Only one run holds `--out` at a time. (That the lock is free once
    /// every holder is gone is shown in a process of its own, by
    /// [`a_killed_runner_keeps_the_lock_until_its_children_end`]: here, other
    /// tests' children inherit it.)
    #[cfg(unix)]
    #[test]
    fn one_run_holds_the_output_at_a_time() {
        let out = tempfile::tempdir().unwrap();
        let _held = lock_out(out.path()).unwrap();
        let refused = lock_out(out.path()).unwrap_err();
        assert!(refused.to_string().contains("holds"), "{refused}");
    }

    /// A new run removes every earlier run directory but the published
    /// record's, with the worktrees registered in it, a nested one first,
    /// and leaves every other worktree alone.
    #[test]
    fn a_new_run_clears_earlier_runs_but_the_published_one() {
        let repo = scratch_repo();
        let out = tempfile::tempdir().unwrap();
        let options = Options {
            out: out.path().to_owned(),
            deadline: Duration::from_secs(60),
        };
        let add = |dir: &Path, at: &Path| {
            assert!(planted::git(dir)
                .args(["worktree", "add", "-q", "--detach"])
                .arg(at)
                .status()
                .unwrap()
                .success());
        };
        let listed = || {
            let list = planted::git(repo.path())
                .args(["worktree", "list", "--porcelain"])
                .output()
                .unwrap();
            String::from_utf8(list.stdout)
                .unwrap()
                .matches("worktree ")
                .count()
        };
        let stale = out.path().join(RUNS).join("stale");
        let kept = out.path().join(RUNS).join("published");
        fs::create_dir_all(&stale).unwrap();
        fs::create_dir_all(&kept).unwrap();
        let other = out.path().join("other");
        add(repo.path(), &other);
        fs::write(other.join("work"), "uncommitted").unwrap();
        let subject = stale.join(SUBJECT_DIR);
        add(repo.path(), &subject);
        add(&subject, &subject.join("target/planted/worktree"));
        add(&subject, &stale.join("worktree"));
        assert_eq!(listed(), 5);
        clear_stale_runs(repo.path(), &options, Some(&kept)).unwrap();
        assert_eq!(listed(), 2);
        assert!(!stale.exists() && kept.exists());
        assert!(other.join("work").exists());
    }

    /// A run drops `--out`'s session record when it ends only once it has
    /// confirmed every earlier session ended; a run that stops before that,
    /// refused or failed, keeps it for the next.
    #[test]
    fn the_session_record_outlives_a_run_that_confirmed_nothing() {
        let out = tempfile::tempdir().unwrap();
        let file = out.path().join(SESSIONS_FILE);
        fs::write(&file, "boot 1 2\n").unwrap();
        drop(SessionsGuard::new(out.path()));
        assert!(file.exists());
        let mut guard = SessionsGuard::new(out.path());
        guard.cleared();
        drop(guard);
        assert!(!file.exists());
    }

    /// A gated command runs only once released, as the same process (so in
    /// the gate's session), and never if the gate's input closes first.
    #[cfg(unix)]
    #[test]
    fn a_gated_command_runs_only_once_released() {
        let dir = tempfile::tempdir().unwrap();
        let ran = dir.path().join("ran");
        let mut command = Command::new("sh");
        command
            .args(["-c", "echo $$ > ran"])
            .current_dir(dir.path());
        let spawn = || {
            gated(&command)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        };
        let mut gate = spawn();
        thread::sleep(Duration::from_millis(300));
        assert!(!ran.exists(), "the command ran before its release");
        {
            use std::io::Write as _;
            gate.stdin.take().unwrap().write_all(b"go\n").unwrap();
        }
        assert!(gate.wait().unwrap().success());
        let pid: u32 = fs::read_to_string(&ran).unwrap().trim().parse().unwrap();
        assert_eq!(pid, gate.id());
        fs::remove_file(&ran).unwrap();
        let mut gate = spawn();
        drop(gate.stdin.take());
        assert!(!gate.wait().unwrap().success());
        assert!(!ran.exists(), "the command ran without its release");
    }

    /// Two runs get different IDs.
    #[test]
    fn each_run_gets_its_own_id() {
        let first = run_id();
        std::thread::sleep(Duration::from_millis(1));
        assert_ne!(first, run_id());
    }

    /// A `/proc/<pid>/stat` line's parent is read after the command name,
    /// whatever the name holds.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_stat_line_s_parent_is_read_after_its_command_name() {
        assert_eq!(parent_in_stat(b"42 (sleep) S 7 42 42 0"), Some(7));
        assert_eq!(parent_in_stat(b"42 (a) b) Z 7 42 42 0"), Some(7));
        // A name cut off mid-character isn't UTF-8.
        assert_eq!(parent_in_stat(b"42 (abcdefghijklmn\xc3) S 7 42"), Some(7));
        assert_eq!(parent_in_stat(b"42 (sleep"), None);
        assert_eq!(parent_in_stat(b"42 (sleep) S"), None);
    }

    /// The variable that has [`killed_runner_helper`] run, naming its
    /// `--out`.
    #[cfg(target_os = "linux")]
    const KILLED_RUNNER: &str = "XTASK_KILLED_RUNNER";

    /// A runner killed mid-run leaves nothing a later run would take for
    /// gone: [`killed_runner_helper`], as the runner does, takes the lock,
    /// marks its descendants and starts its child in a recorded session of
    /// its own: a shell that starts a long `sleep`, a Python launcher that
    /// starts another with every inherited descriptor closed, and one that
    /// starts a non-dumpable Python sleeper the same way, and is then killed.
    /// Its direct child, the shell, dies with it (`PR_SET_PDEATHSIG`). The
    /// first `sleep` holds the lock, so no other run starts. The other two,
    /// without the lock, are still found ([`survivors`]): by the mark and
    /// the session, and, for the non-dumpable one whose environment only
    /// root can read, by the session. So a later run refuses to clear the
    /// earlier runs until they end.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_killed_runner_keeps_the_lock_until_its_children_end() {
        let out = tempfile::tempdir().unwrap();
        // The helper stands for the runner.
        let mut command = Command::new(env::current_exe().expect("the test binary has a path"));
        command
            .args([
                "--exact",
                "header_acceptance::tests::killed_runner_helper",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(KILLED_RUNNER, out.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut helper = command.spawn().expect("the test binary runs");
        let read = |name: &str| -> Option<i32> {
            fs::read_to_string(out.path().join(name))
                .ok()?
                .trim()
                .parse()
                .ok()
        };
        let started = Instant::now();

        let (shell, sleeper, closed, hidden) = loop {
            if let (Some(shell), Some(sleeper), Some(closed), Some(hidden)) = (
                read("shell.pid"),
                read("sleep.pid"),
                read("closed.pid"),
                read("hidden.pid"),
            ) {
                break (shell, sleeper, closed, hidden);
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "the helper started its shell"
            );
            thread::sleep(Duration::from_millis(20));
        };
        helper.kill().unwrap();
        helper.wait().unwrap();
        let gone = |pid: i32| {
            let started = Instant::now();
            while started.elapsed() < Duration::from_secs(5) {
                let state = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
                if state.is_empty()
                    || parent_in_stat(state.as_bytes()).is_some() && state.contains(") Z ")
                {
                    return true;
                }
                thread::sleep(Duration::from_millis(20));
            }
            false
        };
        assert!(gone(shell), "the runner's direct child died with it");
        assert!(
            Path::new(&format!("/proc/{sleeper}")).exists(),
            "the sleep lives on"
        );
        let refused = lock_out(out.path()).unwrap_err();
        assert!(refused.to_string().contains("holds"), "{refused}");
        // SAFETY: `kill` has no memory-safety preconditions, and `sleeper`
        // is this test's own `sleep`, still running.
        unsafe {
            libc::kill(sleeper, libc::SIGKILL);
        }
        assert!(gone(sleeper));
        let started = Instant::now();
        while lock_out(out.path()).is_err() {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the lock is freed"
            );
            thread::sleep(Duration::from_millis(20));
        }
        // The launcher's `sleep` closed the lock, but carries the mark, and
        // it and the non-dumpable one, whose environment only root can read,
        // are in the shell's recorded session.
        let mut found = survivors(out.path()).unwrap();
        found.sort_unstable();
        let mut expected = vec![closed, hidden];
        expected.sort_unstable();
        assert_eq!(found, expected);
        let options = Options {
            out: out.path().to_owned(),
            deadline: Duration::from_secs(60),
        };
        // Two retries in a row: the first is refused and, having confirmed
        // nothing, keeps the session record, so the second is refused too.
        for _ in 0..2 {
            let guard = SessionsGuard::new(out.path());
            let refused = clear_stale_runs(out.path(), &options, None).unwrap_err();
            assert!(refused.to_string().contains("still alive"), "{refused}");
            drop(guard);
            assert!(out.path().join(SESSIONS_FILE).exists());
        }
        for pid in [closed, hidden] {
            // SAFETY: as above, for this test's own processes.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
            assert!(gone(pid));
        }
        assert!(survivors(out.path()).unwrap().is_empty());
    }

    /// The killed runner, run only by
    /// [`a_killed_runner_keeps_the_lock_until_its_children_end`].
    #[cfg(target_os = "linux")]
    #[test]
    fn killed_runner_helper() {
        let Some(out) = env::var_os(KILLED_RUNNER).map(PathBuf::from) else {
            return;
        };
        let _lock = lock_out(&out).unwrap();
        mark_descendants(&out);
        let _sessions = track_sessions(&out);
        let script = format!(
            "echo $$ > {dir}/shell.pid; sleep 60 & echo $! > {dir}/sleep.pid; \
             python3 -c 'import subprocess, sys; \
             p = subprocess.Popen([\"sleep\", \"60\"], close_fds=True); \
             open(sys.argv[1], \"w\").write(str(p.pid))' {dir}/closed.pid; \
             python3 -c 'import subprocess, sys; \
             p = subprocess.Popen([sys.executable, \"-c\", \
             \"import ctypes, time; ctypes.CDLL(None).prctl(4, 0, 0, 0, 0); time.sleep(60)\"], \
             close_fds=True); open(sys.argv[1], \"w\").write(str(p.pid))' {dir}/hidden.pid; \
             wait",
            dir = out.display()
        );
        let mut shell = Command::new("sh");
        shell.args(["-c", &script]);
        child(&mut shell, &out.join("shell.log"), Duration::from_secs(60));
    }

    /// The variable that has [`interrupted_runner_helper`] run as the outer
    /// runner, naming its `--out`.
    #[cfg(target_os = "linux")]
    const INTERRUPTED_RUNNER: &str = "XTASK_INTERRUPTED_RUNNER";

    /// The variable that has [`interrupted_runner_helper`] run as the inner
    /// runner, the xtask built at S, naming the outer's `--out`.
    #[cfg(target_os = "linux")]
    const INTERRUPTED_INNER: &str = "XTASK_INTERRUPTED_INNER";

    /// An interrupted runner leaves nothing running: the outer runner
    /// ([`interrupted_runner_helper`]) takes the lock and runs the inner one
    /// through [`child`], which runs a shell that starts a `sleep` and a
    /// Python launcher's `sleep` with every inherited descriptor closed.
    /// SIGINT to the outer ends its child as its deadline would: every one of
    /// those processes is gone, the outer's child ended `Interrupted`, and
    /// the lock is free with no survivor. Without the handler SIGINT kills
    /// the outer, and the `sleep`s live on holding the lock.
    #[cfg(target_os = "linux")]
    #[test]
    fn an_interrupted_runner_ends_everything_it_started() {
        let out = tempfile::tempdir().unwrap();
        let mut helper = Command::new(env::current_exe().expect("the test binary has a path"))
            .args([
                "--exact",
                "header_acceptance::tests::interrupted_runner_helper",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(INTERRUPTED_RUNNER, out.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the test binary runs");
        let read = |name: &str| -> Option<i32> {
            fs::read_to_string(out.path().join(name))
                .ok()?
                .trim()
                .parse()
                .ok()
        };
        let started = Instant::now();
        let (shell, sleeper, closed) = loop {
            if let (Some(shell), Some(sleeper), Some(closed)) =
                (read("shell.pid"), read("sleep.pid"), read("closed.pid"))
            {
                break (shell, sleeper, closed);
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "the inner runner started its shell"
            );
            thread::sleep(Duration::from_millis(20));
        };
        let outer = i32::try_from(helper.id()).unwrap();
        // SAFETY: `kill` has no memory-safety preconditions, and `outer` is
        // this test's own unreaped child.
        unsafe {
            libc::kill(outer, libc::SIGINT);
        }
        let started = Instant::now();
        while helper.try_wait().unwrap().is_none() {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the interrupted runner exits"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            fs::read_to_string(out.path().join("end")).unwrap_or_default(),
            ChildEnd::Interrupted.to_string(),
            "the outer runner's child ended interrupted"
        );
        for pid in [shell, sleeper, closed] {
            let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
            assert!(
                stat.is_empty() || stat.contains(") Z "),
                "{pid} outlived the interrupted run"
            );
        }
        lock_out(out.path()).expect("the lock is free");
        assert!(survivors(out.path()).unwrap().is_empty());
    }

    /// The outer and inner runners, run only by
    /// [`an_interrupted_runner_ends_everything_it_started`]. Each installs
    /// the handler and the subreaper as the runner does. The outer writes its
    /// child's end to `end`.
    #[cfg(target_os = "linux")]
    #[test]
    fn interrupted_runner_helper() {
        if let Some(out) = env::var_os(INTERRUPTED_INNER).map(PathBuf::from) {
            sim_minimize::install_interrupt_handler();
            assert!(contain());
            let script = format!(
                "echo $$ > {dir}/shell.pid; sleep 60 & echo $! > {dir}/sleep.pid; \
                 python3 -c 'import subprocess, sys; \
                 p = subprocess.Popen([\"sleep\", \"60\"], close_fds=True); \
                 open(sys.argv[1], \"w\").write(str(p.pid))' {dir}/closed.pid; \
                 wait",
                dir = out.display()
            );
            let mut shell = Command::new("sh");
            shell.args(["-c", &script]);
            child(&mut shell, &out.join("shell.log"), Duration::from_secs(60));
            return;
        }
        let Some(out) = env::var_os(INTERRUPTED_RUNNER).map(PathBuf::from) else {
            return;
        };
        sim_minimize::install_interrupt_handler();
        assert!(contain());
        let _lock = lock_out(&out).unwrap();
        mark_descendants(&out);
        let _sessions = track_sessions(&out);
        let mut inner = Command::new(own_exe().unwrap());
        inner
            .args([
                "--exact",
                "header_acceptance::tests::interrupted_runner_helper",
                "--nocapture",
                "--test-threads=1",
            ])
            .env_remove(INTERRUPTED_RUNNER)
            .env(INTERRUPTED_INNER, &out);
        let ended = child(&mut inner, &out.join("inner.log"), Duration::from_secs(60));
        fs::write(out.join("end"), ended.end.to_string()).unwrap();
    }

    /// The variable that has [`clean_env_helper`] run, naming its log.
    #[cfg(unix)]
    const CLEAN_ENV: &str = "XTASK_CLEAN_ENV";

    /// A child of the runner inherits none of the build settings an
    /// operator's environment could carry: [`clean_env_helper`] starts with
    /// `CARGO_ENCODED_RUSTFLAGS`, `RUSTFLAGS`, a `CARGO_PROFILE_DEV_*` and a
    /// `CARGO_PROFILE_PLANTED_*` set, calls [`run`], whose startup cleans
    /// the environment, and then runs `env`, whose output holds none of
    /// them.
    #[cfg(unix)]
    #[test]
    fn the_runner_s_children_inherit_no_build_overrides() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("env.log");
        let status = Command::new(env::current_exe().expect("the test binary has a path"))
            .args([
                "--exact",
                "header_acceptance::tests::clean_env_helper",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CLEAN_ENV, &log)
            .env("CARGO_ENCODED_RUSTFLAGS", "-Cdebug-assertions=no")
            .env("RUSTFLAGS", "-Cdebug-assertions=no")
            .env("CARGO_PROFILE_DEV_PANIC", "abort")
            .env("CARGO_PROFILE_PLANTED_OVERFLOW_CHECKS", "false")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        let seen = fs::read_to_string(&log).unwrap();
        assert!(
            seen.contains(&format!("{CLEAN_ENV}=")),
            "the env ran: {seen}"
        );
        for name in [
            "CARGO_ENCODED_RUSTFLAGS=",
            "RUSTFLAGS=",
            "CARGO_PROFILE_DEV_PANIC=",
            "CARGO_PROFILE_PLANTED_OVERFLOW_CHECKS=",
        ] {
            assert!(
                !seen.lines().any(|line| line.starts_with(name)),
                "{name} reached the child"
            );
        }
    }

    /// Starts [`run`] and runs `env` into the log [`CLEAN_ENV`] names, run
    /// only by [`the_runner_s_children_inherit_no_build_overrides`]. The
    /// run is the xtask-built-at-S mode, outside the runner's checkout, so
    /// after its startup it refuses before starting any child.
    #[cfg(unix)]
    #[test]
    fn clean_env_helper() {
        let Some(log) = env::var_os(CLEAN_ENV).map(PathBuf::from) else {
            return;
        };
        let dir = log.parent().expect("the log is in a directory");
        let args = [
            IN_SUBJECT.to_owned(),
            "s".to_owned(),
            "--out".to_owned(),
            dir.join("out").display().to_string(),
        ];
        assert!(run(dir, args.into_iter()).is_err());
        let ended = child(&mut Command::new("env"), &log, Duration::from_secs(60));
        assert!(ended.exited());
    }

    /// The variable that has [`interrupted_flag_helper`] run, naming its
    /// directory.
    #[cfg(unix)]
    const INTERRUPTED_FLAG: &str = "XTASK_INTERRUPTED_FLAG";

    /// Once the run is interrupted, [`child`] starts nothing and reports
    /// `Interrupted`, and [`publish_record`], the run's last decision,
    /// publishes nothing and leaves no staged copy. The flag is set in a
    /// process of its own ([`interrupted_flag_helper`]), since every `child`
    /// in this test binary would see it.
    #[cfg(unix)]
    #[test]
    fn an_interrupted_run_starts_and_publishes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let status = Command::new(env::current_exe().expect("the test binary has a path"))
            .args([
                "--exact",
                "header_acceptance::tests::interrupted_flag_helper",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(INTERRUPTED_FLAG, dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        let report: Value =
            serde_json::from_str(&fs::read_to_string(dir.path().join("report.json")).unwrap())
                .unwrap();
        assert_eq!(report["end"], ChildEnd::Interrupted.to_string());
        assert_eq!(report["ran"], false, "the child didn't start");
        assert_eq!(report["published"], false);
        assert_eq!(report["staged"], 0, "no staged record is left");
        assert!(report["error"].as_str().unwrap().contains("interrupted"));
    }

    /// Sets the interrupt flag, then tries a child and a publication, run
    /// only by [`an_interrupted_run_starts_and_publishes_nothing`].
    #[cfg(unix)]
    #[test]
    fn interrupted_flag_helper() {
        let Some(dir) = env::var_os(INTERRUPTED_FLAG).map(PathBuf::from) else {
            return;
        };
        sim_minimize::INTERRUPTED.store(true, Ordering::SeqCst);
        let marker = dir.join("ran");
        let mut touch = Command::new("touch");
        touch.arg(&marker);
        let ended = child(&mut touch, &dir.join("touch.log"), Duration::from_secs(60));
        let out = dir.join("out");
        let run_dir = out.join(RUNS).join("run");
        fs::create_dir_all(&run_dir).unwrap();
        fs::write(run_dir.join(RECORD), r#"{"artifacts": "artifacts"}"#).unwrap();
        let error = publish_record(&out, &run_dir).unwrap_err().to_string();
        let staged = fs::read_dir(&out)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".record."))
            .count();
        fs::write(
            dir.join("report.json"),
            json!({
                "end": ended.end.to_string(),
                "ran": marker.exists(),
                "published": out.join(RECORD).exists(),
                "staged": staged,
                "error": error,
            })
            .to_string(),
        )
        .unwrap();
    }

    /// A recorded session counts only this boot, and only while its number
    /// is still that session's: a leader alive now with another start time
    /// means the session ended and its number was reused, so its members
    /// are no run's.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_reused_or_earlier_boot_session_holds_no_survivor() {
        let out = tempfile::tempdir().unwrap();
        let mut sleep = Command::new("sleep");
        sleep.arg("30");
        own_session(&mut sleep);
        let mut sleep = sleep.spawn().unwrap();
        let pid = i32::try_from(sleep.id()).unwrap();
        let start = start_ticks(pid).unwrap();
        let boot = boot_id().unwrap();
        let found = |line: String| {
            fs::write(out.path().join(SESSIONS_FILE), line).unwrap();
            survivors(out.path()).unwrap().contains(&pid)
        };
        assert!(found(format!("{boot} {pid} {start}\n")));
        assert!(!found(format!("{boot} {pid} {}\n", start + 1)));
        assert!(!found(format!("another-boot {pid} {start}\n")));
        // A record it can't read in full fails the check, never passes it.
        fs::write(out.path().join(SESSIONS_FILE), format!("{boot} {pid}\n")).unwrap();
        assert!(survivors(out.path()).is_err());
        fs::remove_file(out.path().join(SESSIONS_FILE)).unwrap();
        fs::create_dir(out.path().join(SESSIONS_FILE)).unwrap();
        assert!(survivors(out.path()).is_err());
        sleep.kill().unwrap();
        sleep.wait().unwrap();
    }

    /// The variable that has [`subreaper_cases`] run.
    #[cfg(target_os = "linux")]
    const SUBREAPER_CASES: &str = "XTASK_SUBREAPER_CASES";

    /// Runs [`subreaper_cases`] in a test process of its own: the subreaper
    /// and its sweeps are process-wide, so in this one they'd take children
    /// other tests start outside [`child`].
    #[cfg(target_os = "linux")]
    #[test]
    fn the_subreaper_leaves_nothing_a_child_started() {
        let output = Command::new(env::current_exe().expect("the test binary has a path"))
            .args([
                "--exact",
                "header_acceptance::tests::subreaper_cases",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(SUBREAPER_CASES, "1")
            .output()
            .expect("the test binary runs");
        let text = String::from_utf8_lossy(&output.stdout).into_owned()
            + &String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success() && text.contains("1 passed"),
            "{text}"
        );
    }

    /// The subreaper's cases, run only by
    /// [`the_subreaper_leaves_nothing_a_child_started`]. In each, a child
    /// leaves a process behind, and it's gone from `/proc` when [`child`]
    /// returns: round 6's orphan (a grandchild in its own group whose parent
    /// exits as soon as it's started), a zombie leader (a process whose main
    /// thread exits while another thread runs), and a grandchild of a child
    /// killed at its deadline.
    #[cfg(target_os = "linux")]
    #[test]
    fn subreaper_cases() {
        if env::var_os(SUBREAPER_CASES).is_none() {
            return;
        }
        assert!(contain(), "PR_SET_CHILD_SUBREAPER is set");
        let dir = tempfile::tempdir().unwrap();
        let case = |name: &str, script: &str, deadline: Duration, end: ChildEnd| {
            let pid_file = dir.path().join(format!("{name}.pid"));
            let script = script.replace("PIDFILE", &pid_file.display().to_string());
            let mut shell = Command::new("sh");
            shell.args(["-c", &script]);
            let ended = child(
                &mut shell,
                &dir.path().join(format!("{name}.log")),
                deadline,
            );
            assert_eq!(ended.end, end, "{name}");
            let pid: i32 = fs::read_to_string(&pid_file)
                .ok()
                .and_then(|text| text.trim().parse().ok())
                .unwrap_or_else(|| panic!("{name}: the survivor wrote its pid"));
            assert!(
                !Path::new(&format!("/proc/{pid}")).exists(),
                "{name}: {pid} outlived its child call, or wasn't reaped"
            );
            assert!(
                !ESCAPED.load(Ordering::Relaxed),
                "{name}: cleanup confirmed"
            );
        };
        let wait = "while [ ! -s PIDFILE ]; do sleep 0.01; done";
        let orphan = "perl -e 'setpgrp(0, 0); open(my $f, \">\", \"PIDFILE\"); print $f $$; \
                      close($f); exec(\"sleep\", \"30\")' &";
        case(
            "orphan",
            &format!("{orphan} {wait}; exit 0"),
            Duration::from_secs(20),
            ChildEnd::Exited(0),
        );
        // A name cut off mid-character, so its `stat` isn't UTF-8.
        let cut_name = "python3 -c 'import ctypes, os, time; \
                        ctypes.CDLL(None).prctl(15, b\"abcdefghijklmn\\xc3\\xa9\", 0, 0, 0); \
                        open(\"PIDFILE\", \"w\").write(str(os.getpid())); time.sleep(30)' &";
        case(
            "cut-name",
            &format!("{cut_name} {wait}; exit 0"),
            Duration::from_secs(20),
            ChildEnd::Exited(0),
        );
        // The leader ends only its own thread (`SYS_exit`, not
        // `exit_group`), so it's a zombie while its worker sleeps on.
        let zombie_leader = "python3 -c 'import ctypes, os, threading, time; \
                             threading.Thread(target=time.sleep, args=(30,)).start(); \
                             open(\"PIDFILE\", \"w\").write(str(os.getpid())); \
                             ctypes.CDLL(None).syscall({\"x86_64\": 60, \"aarch64\": 93}\
                             [os.uname().machine], 0)' &";
        case(
            "zombie-leader",
            &format!("{zombie_leader} {wait}; sleep 0.2; exit 0"),
            Duration::from_secs(20),
            ChildEnd::Exited(0),
        );
        case(
            "deadline",
            &format!("{orphan} {wait}; sleep 30"),
            Duration::from_secs(2),
            ChildEnd::TimedOut,
        );
    }
}
