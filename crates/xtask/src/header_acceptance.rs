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
//! child the way its deadline would and fails the run
//! ([`crate::acceptance::interrupted`]), whose last decision is a check just
//! before the record's rename. Each build uses only the flags the runner
//! sets and the committed profiles ([`clean_build_env`]). The record names
//! its subject, which a reader compares, since runs from different checkouts
//! may share `--out`.
//! `--out` must be on a local filesystem, where `flock` and `rename` hold.

use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use serde_json::{json, Value};

use crate::{
    acceptance::{
        add_worktree, checkout_digest, child, clear_stale_runs, contain, containment, git,
        lock_out, mark_descendants, publish_record, remove_worktree, remove_worktrees_under, reset,
        resolved, run_id, source_digest, stop_if_interrupted, text, track_sessions, Ended, Options,
        Tool, ESCAPED, RECORD, RUNS, SOURCE_DIGEST, SUBJECT_DIR,
    },
    planted::{self, provenance},
    sim::SIM_RUSTFLAGS,
    sim_minimize, BoxError, UsageError,
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

/// The default deadline for each child, in seconds.
const DEFAULT_DEADLINE_SECS: u64 = 3600;

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
pub(crate) fn clean_build_env() {
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
    let (_lock, kept) = lock_out(&TOOL, &out)?;
    mark_descendants(&TOOL, &out);
    let mut sessions = track_sessions(&out);
    contain();
    check_out_dir(repo_root, &options)?;
    let subject = subject_of(repo_root, &options)?;
    clear_stale_runs(&TOOL, repo_root, &options, kept.as_deref())?;
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

/// A run directory's target directories, dropped once the run succeeds.
const RUN_TARGETS: [&str; 5] = ["xtask-target", "build", "tests", "native", "cases"];

/// The variable every process a run starts carries, naming the run's
/// `--out`, so a later run can find one that outlived its runner even if it
/// closed the inherited lock ([`crate::acceptance::survivors`]).
const RUN_MARK: &str = "ZAKURA_HEADER_ACCEPTANCE_OUT";

/// header-acceptance's names, for the shared runner.
const TOOL: Tool = Tool {
    command: "header-acceptance",
    run_mark: RUN_MARK,
};

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
    let read = source_digest(
        &TOOL,
        repo_root,
        options.deadline,
        &logs.path().join("digest.log"),
    )?;
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
pub(crate) fn cargo(dir: &Path, target: &Path) -> Command {
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

/// Runs one test by its exact name in `dir_root`, the checkout to run it
/// in, whose source digest is `source`, with its target directory under
/// `targets`, and writes `test.json`: whether the child exited 0, since the
/// reader counts an `ok` only if it did, and whether the test binary embeds
/// `source`.
pub(crate) fn run_test(
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
pub(crate) fn libtest(log: &str) -> Vec<(String, String)> {
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
                Ok(()) => match checkout_digest(&TOOL, options, worktree) {
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
    let source = checkout_digest(&TOOL, options, worktree)
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
        let source = checkout_digest(&TOOL, options, worktree)
            .ok()
            .map(|read| read.source)
            .filter(|source| source != provenance::UNKNOWN);
        let out = dir.join(run);
        fs::create_dir_all(&out)?;
        run_test(worktree, options, source.as_deref(), &test, &out, &targets)?;
    }
    reset(options, worktree, &subject.commit)
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

#[cfg(test)]
mod tests {
    use std::process::Stdio;

    use super::*;
    use crate::acceptance::tests::scratch_repo;

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
}
