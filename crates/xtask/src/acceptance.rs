//! The runner an acceptance tool shares (sim-7.3): `--out`'s lock and
//! published record, each run's own directory, every child under a deadline
//! and contained, the session record that finds what outlived a killed
//! runner, and a checkout's provenance and worktrees. Each tool names itself
//! through [`Tool`]. `header_acceptance`'s module doc states the record
//! invariant these keep.

#[cfg(target_os = "linux")]
use std::collections::BTreeMap;
use std::{
    collections::BTreeSet,
    env,
    ffi::OsStr,
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
    planted,
    sim_minimize::{self, ChildEnd},
    BoxError,
};

/// What tells one acceptance tool's runs and helpers from another's.
pub(crate) struct Tool {
    /// The tool's xtask subcommand, which names it in messages and runs this
    /// binary's own helpers ([`source_digest`]).
    pub(crate) command: &'static str,
    /// The variable every process a run starts carries, naming the run's
    /// `--out` ([`mark_descendants`], [`survivors`]).
    pub(crate) run_mark: &'static str,
}

/// Set once this process adopts its orphaned descendants ([`contain`]).
pub(crate) static SUBREAPER: AtomicBool = AtomicBool::new(false);

/// The direct children [`child`] is running now. A sweep under the
/// subreaper leaves every other call's child and its subtree alone.
pub(crate) static LIVE: std::sync::Mutex<BTreeSet<i32>> = std::sync::Mutex::new(BTreeSet::new());

/// Held while this process spawns a child and records it in [`LIVE`], while
/// it runs `ps` ([`table`]), and while a sweep reads the table and signals.
/// So a sweep never sees a child of this process that isn't yet in
/// [`LIVE`], or another thread's `ps`, and takes it for an orphan.
pub(crate) static SPAWN: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Locks [`SPAWN`], ignoring poisoning: it guards no data.
pub(crate) fn spawn_lock() -> std::sync::MutexGuard<'static, ()> {
    SPAWN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A copy of [`LIVE`].
#[cfg(target_os = "linux")]
pub(crate) fn live_children() -> BTreeSet<i32> {
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
pub(crate) fn contain() -> bool {
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
pub(crate) static ESCAPED: AtomicBool = AtomicBool::new(false);

/// The hidden first argument that has this command print a checkout's
/// source digest, so [`source_digest`] can read it in a child under the
/// deadline.
pub(crate) const SOURCE_DIGEST: &str = "--source-digest";

/// What a run reads from its arguments.
pub(crate) struct Options {
    /// Where everything goes.
    pub(crate) out: PathBuf,
    /// Each child's deadline.
    pub(crate) deadline: Duration,
}

/// The published record, directly under `--out`.
pub(crate) const RECORD: &str = "record.json";

/// What a starting run renames the published record to ([`lock_out`]).
pub(crate) const RETIRED_RECORD: &str = ".record.retired";

/// The directory under `--out` holding each run's own directory.
pub(crate) const RUNS: &str = "runs";

/// S's checkout inside a run directory.
pub(crate) const SUBJECT_DIR: &str = "subject";

/// The lock file under `--out` ([`lock_out`]).
pub(crate) const LOCK_FILE: &str = ".lock";

/// Takes `--out`'s lock ([`LOCK_FILE`]) for the whole run, or fails if
/// another run holds it, then starts the run: its first change to `--out`
/// is one rename that retires the published record, so from then on there
/// is none. Returns the lock and the run directory the retired record named
/// ([`published_run`]). The lock is a `flock`, which the kernel drops when
/// every holder of the open file is gone, killed included, so it's never
/// left stale. Every child inherits it. Elsewhere than Unix there is no
/// lock.
pub(crate) fn lock_out(tool: &Tool, out: &Path) -> Result<(fs::File, Option<PathBuf>), BoxError> {
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
            let found = match survivors(tool, out) {
                Ok(pids) if pids.is_empty() => String::new(),
                Ok(pids) => format!(
                    "; detected processes of a run on this output: {pids:?} (a live run, or survivors of one)"
                ),
                Err(why) => format!("; its processes can't be listed: {why}"),
            };
            return Err(format!(
                "another {} run holds {}{found}",
                tool.command,
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

/// Sets `tool`'s [`Tool::run_mark`] for this process, so every child
/// inherits it. It runs once, at the run's start, before any thread or child.
pub(crate) fn mark_descendants(tool: &Tool, out: &Path) {
    env::set_var(tool.run_mark, resolved(out));
}

/// The file under `--out` where the runner records the session of each
/// child it starts ([`track_sessions`]).
pub(crate) const SESSIONS_FILE: &str = "sessions";

/// Set by the runner to its [`SESSIONS_FILE`]: then every child [`child`]
/// starts leads a session of its own, recorded there, which everything it
/// starts shares unless it leaves on purpose. Unset, as in the xtask built
/// at S, whose children are process groups inside its own session.
pub(crate) static SESSIONS: OnceLock<PathBuf> = OnceLock::new();

/// Has every child this process starts lead a recorded session ([`SESSIONS`]),
/// and returns a guard that drops `out`'s [`SESSIONS_FILE`] when the run
/// ends with every child's descendants confirmed gone.
pub(crate) fn track_sessions(out: &Path) -> SessionsGuard {
    let guard = SessionsGuard::new(out);
    let _ = SESSIONS.set(guard.file.clone());
    guard
}

/// Drops `--out`'s [`SESSIONS_FILE`] once the run ends, but only if the run
/// confirmed every session recorded before it had ended
/// ([`SessionsGuard::cleared`], after [`clear_stale_runs`]) and none of its
/// own children's descendants escaped ([`ESCAPED`]). Otherwise a later run
/// still needs the file to find them.
pub(crate) struct SessionsGuard {
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
    pub(crate) fn cleared(&mut self) {
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
pub(crate) fn record_session(sessions: &Path, pid: i32) -> Result<(), BoxError> {
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
pub(crate) fn boot_id() -> Option<String> {
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
pub(crate) fn start_ticks(pid: i32) -> Option<u64> {
    stat_field(&fs::read(format!("/proc/{pid}/stat")).ok()?, 22)
}

/// Field `n` (counting from 1, the PID) of a `/proc/<pid>/stat` line, read
/// after the command name's last `)`, which may hold anything.
#[cfg(target_os = "linux")]
pub(crate) fn stat_field<T: std::str::FromStr>(stat: &[u8], n: usize) -> Option<T> {
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
/// non-dumpable. Or it carries `out`'s [`Tool::run_mark`] where its environment
/// can be read. Only a process that leaves its session on purpose and hides
/// its environment escapes both. It fails, rather than finding none, when
/// the session record or the process table can't be read in full: the
/// record unreadable or malformed, no boot ID, `/proc` unlisted, or a
/// process's `stat` unreadable or unparsable while it still exists.
/// Elsewhere than Linux, none.
pub(crate) fn survivors(tool: &Tool, out: &Path) -> Result<Vec<i32>, String> {
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
        let mark = format!("{}={}", tool.run_mark, resolved(out).display()).into_bytes();
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
        let _ = (tool, out);
        Ok(Vec::new())
    }
}

/// The run directory the record at `record` names its artifacts in, under
/// `out`'s run directories, if it names one.
pub(crate) fn published_run(record: &Path, out: &Path) -> Option<PathBuf> {
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
pub(crate) fn remove_record(out: &Path) -> Result<(), BoxError> {
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
pub(crate) fn clear_stale_runs(
    tool: &Tool,
    repo_root: &Path,
    options: &Options,
    kept: Option<&Path>,
) -> Result<(), BoxError> {
    // A process an earlier run started may outlive its runner without the
    // lock, so the earlier runs' directories are only cleared once none is
    // left.
    let runs = options.out.join(RUNS);
    let survivors = survivors(tool, &options.out)?;
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
pub(crate) fn publish_record(out: &Path, run_dir: &Path) -> Result<(), BoxError> {
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

/// A checkout's provenance, as `zakura-sim`'s build script reads it.
pub(crate) struct SourceRead {
    /// `HEAD`'s commit, or [`planted::provenance::UNKNOWN`].
    pub(crate) git: String,
    /// Whether a source path has a change.
    pub(crate) dirty: bool,
    /// The source digest, or [`planted::provenance::UNKNOWN`].
    pub(crate) source: String,
}

/// How the run contained its children: `subreaper` under [`contain`], and
/// `best-effort` without it.
pub(crate) fn containment(contained: bool) -> &'static str {
    if contained {
        "subreaper"
    } else {
        "best-effort"
    }
}

/// A fresh ID for this run: the time it started, to the nanosecond, and
/// this process's ID, so two attempts never share one.
pub(crate) fn run_id() -> String {
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
pub(crate) fn own_exe() -> Result<PathBuf, BoxError> {
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
pub(crate) fn source_digest(
    tool: &Tool,
    dir: &Path,
    deadline: Duration,
    log: &Path,
) -> Result<SourceRead, BoxError> {
    let mut read = Command::new(own_exe()?);
    read.arg(tool.command).arg(SOURCE_DIGEST).arg(dir);
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
pub(crate) fn checkout_digest(
    tool: &Tool,
    options: &Options,
    dir: &Path,
) -> Result<SourceRead, BoxError> {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let log = options.out.join("logs").join(format!(
        "digest-{}.log",
        CALLS.fetch_add(1, Ordering::Relaxed)
    ));
    source_digest(tool, dir, options.deadline, &log)
}

/// Runs `git <args>` in `dir` through [`child`], under the run's deadline,
/// with its output in its own log, and says how it failed if it didn't
/// exit 0.
pub(crate) fn git(options: &Options, dir: &Path, args: &[&OsStr]) -> Result<(), String> {
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

/// The commit `dir`'s checkout was made from, its `HEAD`, read by `git
/// rev-parse` under the run's deadline, or [`planted::provenance::UNKNOWN`] if git
/// can't say.
pub(crate) fn head_of(options: &Options, dir: &Path) -> String {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let log = options.out.join("logs").join(format!(
        "head-{}.log",
        CALLS.fetch_add(1, Ordering::Relaxed)
    ));
    let mut command = planted::git(dir);
    command.args(["rev-parse", "--verify", "HEAD"]);
    if !child(&mut command, &log, options.deadline).exited() {
        return planted::provenance::UNKNOWN.to_owned();
    }
    fs::read_to_string(&log)
        .ok()
        .and_then(|text| text.lines().last().map(str::trim).map(str::to_owned))
        .filter(|commit| commit.len() == 40 && commit.bytes().all(|b| b.is_ascii_hexdigit()))
        .unwrap_or_else(|| planted::provenance::UNKNOWN.to_owned())
}

/// How a child ended, with its log.
pub(crate) struct Ended {
    /// The end.
    pub(crate) end: ChildEnd,
    /// Where its output went.
    pub(crate) log: PathBuf,
}

impl Ended {
    /// `Ok` if it exited 0, or the end and its log in words.
    pub(crate) fn exited_ok(&self) -> Result<(), String> {
        match self.end {
            ChildEnd::Exited(0) => Ok(()),
            ref end => Err(format!("{end}; see {}", self.log.display())),
        }
    }

    /// Whether it exited 0.
    pub(crate) fn exited(&self) -> bool {
        matches!(self.end, ChildEnd::Exited(0))
    }

    /// Whether it ran past its deadline, a signal killed it, or the run was
    /// interrupted.
    pub(crate) fn killed(&self) -> bool {
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
pub(crate) fn interrupted() -> bool {
    sim_minimize::INTERRUPTED.load(Ordering::SeqCst)
}

/// Fails once the run was [`interrupted`], so a task stops before it writes
/// an artifact from an interrupted child's outcome, and the run before it
/// publishes.
pub(crate) fn stop_if_interrupted() -> Result<(), BoxError> {
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
pub(crate) fn child(command: &mut Command, log: &Path, deadline: Duration) -> Ended {
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
pub(crate) struct Live(Option<i32>);

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
pub(crate) fn end_unreaped(process: &mut std::process::Child, tree: &mut Tree, live: &mut Live) {
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
pub(crate) const REAP_DEADLINE: Duration = Duration::from_secs(10);

/// Waits up to `deadline` for the killed `process` to end and reaps it.
/// One that doesn't, such as one stuck in uninterruptible I/O, sets
/// [`ESCAPED`], which fails the run, rather than hanging it.
pub(crate) fn reap_within(process: &mut std::process::Child, deadline: Duration) -> bool {
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
pub(crate) const TRACK_EVERY: Duration = Duration::from_millis(500);

/// A child's process tree as last read: its leader, every descendant seen
/// and every process group one of them was in.
pub(crate) struct Tree {
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
    #[cfg(unix)]
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
pub(crate) fn own_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt as _;
    command.process_group(0);
}

/// Without process groups, only the child itself can be killed.
#[cfg(not(unix))]
pub(crate) fn own_process_group(_: &mut Command) {}

/// `command`, behind a gate: a shell, in the same directory with the same
/// environment, that waits for a line on its standard input and then
/// `exec`s the command, with no input, in its own process, so the command
/// keeps the shell's session. [`child`] writes the line only once the
/// session is recorded, so nothing the command starts can exist before
/// then. If the runner dies first, the shell reads end of input and exits
/// without running it.
pub(crate) fn gated(command: &Command) -> Command {
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
pub(crate) fn own_session(command: &mut Command) {
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
pub(crate) fn own_session(command: &mut Command) {
    own_process_group(command);
}

/// Has `command`'s child killed when this process dies
/// (`PR_SET_PDEATHSIG`), so a killed runner doesn't leave its direct
/// children running. What they started still holds `--out`'s lock
/// ([`lock_out`]) until it ends.
#[cfg(target_os = "linux")]
pub(crate) fn die_with_parent(command: &mut Command) {
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
pub(crate) fn die_with_parent(_: &mut Command) {}

/// The signal that ended a child.
#[cfg(unix)]
pub(crate) fn signal_of(status: std::process::ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt as _;
    status.signal().unwrap_or_default()
}

/// No signals outside Unix.
#[cfg(not(unix))]
pub(crate) fn signal_of(_: std::process::ExitStatus) -> i32 {
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
pub(crate) fn sweep(tree: &mut Tree) {
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
pub(crate) fn sweep(_: &mut Tree) {}

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
pub(crate) fn sweep_owned() {
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
pub(crate) fn children_of(me: i32) -> Option<Vec<i32>> {
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
pub(crate) fn no_children() -> bool {
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
pub(crate) fn parent_in_stat(stat: &[u8]) -> Option<i32> {
    let close = stat.iter().rposition(|&byte| byte == b')')?;
    let fields = std::str::from_utf8(&stat[close + 1..]).ok()?;
    fields.split_whitespace().nth(1)?.parse().ok()
}

/// Every live process, as `(pid, parent, group)`, from [`table`]. A zombie
/// waiting for its parent is dead, so it's left out.
#[cfg(unix)]
pub(crate) fn processes() -> Option<Vec<(i32, i32, i32)>> {
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
pub(crate) fn table() -> Option<Vec<(i32, i32, i32, bool)>> {
    let _spawn = spawn_lock();
    table_locked()
}

/// [`table`], for a caller already holding [`SPAWN`].
#[cfg(unix)]
pub(crate) fn table_locked() -> Option<Vec<(i32, i32, i32, bool)>> {
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
pub(crate) fn processes() -> Option<Vec<(i32, i32, i32)>> {
    None
}

/// How long [`processes`] waits for `ps`.
#[cfg(unix)]
pub(crate) const PS_DEADLINE: Duration = Duration::from_secs(5);

/// `value[field]` as a string.
pub(crate) fn text(value: &Value, field: &str) -> Result<String, BoxError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("no {field} in {value}").into())
}

/// Adds a worktree at `worktree`, detached at `commit`.
pub(crate) fn add_worktree(
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
pub(crate) fn remove_worktrees_under(
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
pub(crate) fn resolved(path: &Path) -> PathBuf {
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

/// Removes the worktree at `worktree`.
pub(crate) fn remove_worktree(
    repo_root: &Path,
    options: &Options,
    worktree: &Path,
) -> Result<(), BoxError> {
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
pub(crate) fn reset(options: &Options, worktree: &Path, commit: &str) -> Result<(), BoxError> {
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
pub(crate) mod tests {
    use super::*;

    /// The tool these tests run as, which is no real tool.
    const TOOL: Tool = Tool {
        command: "test-acceptance",
        run_mark: "XTASK_TEST_ACCEPTANCE_OUT",
    };

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
                    "acceptance::tests::exe_runner_helper",
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
                "acceptance::tests::exe_marker_helper",
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
    pub(crate) fn scratch_repo() -> tempfile::TempDir {
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
        let (_lock, kept) = lock_out(&TOOL, out.path()).unwrap();
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
        let _held = lock_out(&TOOL, out.path()).unwrap();
        let refused = lock_out(&TOOL, out.path()).unwrap_err();
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
        clear_stale_runs(&TOOL, repo.path(), &options, Some(&kept)).unwrap();
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
                "acceptance::tests::killed_runner_helper",
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
        let refused = lock_out(&TOOL, out.path()).unwrap_err();
        assert!(refused.to_string().contains("holds"), "{refused}");
        // SAFETY: `kill` has no memory-safety preconditions, and `sleeper`
        // is this test's own `sleep`, still running.
        unsafe {
            libc::kill(sleeper, libc::SIGKILL);
        }
        assert!(gone(sleeper));
        let started = Instant::now();
        while lock_out(&TOOL, out.path()).is_err() {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the lock is freed"
            );
            thread::sleep(Duration::from_millis(20));
        }
        // The launcher's `sleep` closed the lock, but carries the mark, and
        // it and the non-dumpable one, whose environment only root can read,
        // are in the shell's recorded session.
        let mut found = survivors(&TOOL, out.path()).unwrap();
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
            let refused = clear_stale_runs(&TOOL, out.path(), &options, None).unwrap_err();
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
        assert!(survivors(&TOOL, out.path()).unwrap().is_empty());
    }

    /// The killed runner, run only by
    /// [`a_killed_runner_keeps_the_lock_until_its_children_end`].
    #[cfg(target_os = "linux")]
    #[test]
    fn killed_runner_helper() {
        let Some(out) = env::var_os(KILLED_RUNNER).map(PathBuf::from) else {
            return;
        };
        let _lock = lock_out(&TOOL, &out).unwrap();
        mark_descendants(&TOOL, &out);
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
                "acceptance::tests::interrupted_runner_helper",
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
        lock_out(&TOOL, out.path()).expect("the lock is free");
        assert!(survivors(&TOOL, out.path()).unwrap().is_empty());
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
        let _lock = lock_out(&TOOL, &out).unwrap();
        mark_descendants(&TOOL, &out);
        let _sessions = track_sessions(&out);
        let mut inner = Command::new(own_exe().unwrap());
        inner
            .args([
                "--exact",
                "acceptance::tests::interrupted_runner_helper",
                "--nocapture",
                "--test-threads=1",
            ])
            .env_remove(INTERRUPTED_RUNNER)
            .env(INTERRUPTED_INNER, &out);
        let ended = child(&mut inner, &out.join("inner.log"), Duration::from_secs(60));
        fs::write(out.join("end"), ended.end.to_string()).unwrap();
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
                "acceptance::tests::interrupted_flag_helper",
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
            survivors(&TOOL, out.path()).unwrap().contains(&pid)
        };
        assert!(found(format!("{boot} {pid} {start}\n")));
        assert!(!found(format!("{boot} {pid} {}\n", start + 1)));
        assert!(!found(format!("another-boot {pid} {start}\n")));
        // A record it can't read in full fails the check, never passes it.
        fs::write(out.path().join(SESSIONS_FILE), format!("{boot} {pid}\n")).unwrap();
        assert!(survivors(&TOOL, out.path()).is_err());
        fs::remove_file(out.path().join(SESSIONS_FILE)).unwrap();
        fs::create_dir(out.path().join(SESSIONS_FILE)).unwrap();
        assert!(survivors(&TOOL, out.path()).is_err());
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
                "acceptance::tests::subreaper_cases",
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
