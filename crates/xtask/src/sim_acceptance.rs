//! `cargo xtask sim-acceptance --subject <S> --header-out <dir> --out <dir>
//! [--deadline-secs <n>] [--only <text>]...`:
//! sim-7's acceptance run (the sim-7 plan, "sim-7.4's tool").
//!
//! It keeps the shared runner's rules ([`crate::acceptance`]): one run at a
//! time per `--out`, a fresh run directory, a clean checkout of S inside it,
//! a deadline on every child, containment, and a record that exists only if
//! the run fully succeeded. Every binary it runs is its own build from that
//! checkout, in a target directory of its own (Rule 5), the runner itself
//! included: the caller's xtask builds xtask in S's checkout and hands the
//! run to it, as header-acceptance's runner does.
//!
//! The simulator side decides everything: `sim-acceptance plan` lists the
//! tasks, this runner runs them and keeps their raw output under
//! `artifacts/<index>/`, and `sim-acceptance evaluate` judges those files and
//! hdr-14's published record, writing `record.json`.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::Ordering,
    time::Duration,
};

use serde_json::{json, Value};

use crate::{
    acceptance::{
        add_worktree, child, clear_stale_runs, contain, lock_out, mark_descendants, publish_record,
        remove_worktrees_under, resolved, run_id, source_digest, stop_if_interrupted,
        track_sessions, Options, Tool, ESCAPED, RECORD, RUNS, SOURCE_DIGEST, SUBJECT_DIR,
    },
    header_acceptance::{cargo, clean_build_env, libtest, run_test},
    planted::provenance,
    sim_minimize, BoxError, UsageError,
};

/// sim-acceptance's names, for the shared runner.
const TOOL: Tool = Tool {
    command: "sim-acceptance",
    run_mark: "ZAKURA_SIM_ACCEPTANCE_OUT",
};

/// Each child's deadline when `--deadline-secs` isn't given: two hours, for
/// the release and ASan lanes' builds.
const DEFAULT_DEADLINE_SECS: u64 = 7200;

/// A run directory's build directories, dropped once the run succeeds.
const RUN_TARGETS: [&str; 8] = [
    "xtask-target",
    "fuzz-target",
    "build",
    "build-release",
    "tests",
    "fuzz",
    "commands",
    "planted",
];

/// What the run reads from its arguments.
struct Args {
    /// Run only tasks whose expected result contains one of these, for a
    /// smoke run. Every skipped task's result blocks, so such a run is never
    /// accepted.
    only: Vec<String>,
    subject: String,
    header_out: PathBuf,
    out: PathBuf,
    deadline: Duration,
}

/// `--subject <S> --header-out <dir> --out <dir> [--deadline-secs <n>]`.
fn args(mut args: impl Iterator<Item = String>) -> Result<Args, BoxError> {
    let mut flags: BTreeMap<String, String> = BTreeMap::new();
    let mut only = Vec::new();
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or(UsageError("every sim-acceptance flag takes a value"))?;
        if flag == "--only" {
            only.push(value);
            continue;
        }
        if !["--subject", "--header-out", "--out", "--deadline-secs"].contains(&flag.as_str()) {
            return Err(UsageError(
                "expected --subject, --header-out, --out, --deadline-secs or --only",
            )
            .into());
        }
        if flags.insert(flag, value).is_some() {
            return Err(UsageError("a sim-acceptance flag is given twice").into());
        }
    }
    let take = |flags: &mut BTreeMap<String, String>, flag: &str| {
        flags.remove(flag).ok_or(UsageError(
            "sim-acceptance needs --subject, --header-out and --out",
        ))
    };
    let deadline = match flags.remove("--deadline-secs") {
        Some(secs) => secs
            .parse()
            .map_err(|_| UsageError("--deadline-secs is a number"))?,
        None => DEFAULT_DEADLINE_SECS,
    };
    Ok(Args {
        only,
        subject: take(&mut flags, "--subject")?,
        // Absolute, since children run in the subject checkout.
        header_out: absolute(PathBuf::from(take(&mut flags, "--header-out")?))?,
        out: absolute(PathBuf::from(take(&mut flags, "--out")?))?,
        deadline: Duration::from_secs(deadline),
    })
}

/// `path` made absolute against this process's working directory.
fn absolute(path: PathBuf) -> Result<PathBuf, BoxError> {
    Ok(if path.is_absolute() {
        path
    } else {
        env::current_dir()?.join(path)
    })
}

/// Runs sim-7's acceptance run for the checkout at `repo_root`, or answers
/// the shared runner's source-digest query.
pub(super) fn run(repo_root: &Path, args_in: impl Iterator<Item = String>) -> Result<(), BoxError> {
    let mut args_in = args_in.peekable();
    if args_in.peek().map(String::as_str) == Some(SOURCE_DIGEST) {
        args_in.next();
        let dir = args_in.next().ok_or(UsageError("expected a directory"))?;
        let read = provenance::Provenance::read(Path::new(&dir), provenance::SOURCE_PATHS);
        println!(
            "{}",
            json!({ "git": read.git, "dirty": read.dirty, "source": read.source })
        );
        return Ok(());
    }
    // A signal ends every child the way its deadline would.
    sim_minimize::install_interrupt_handler();
    clean_build_env();
    if args_in.peek().map(String::as_str) == Some(IN_SUBJECT) {
        args_in.next();
        return run_in_subject(repo_root, args(args_in)?);
    }
    let args = args(args_in)?;
    // The header run's record is an input: retiring it, or writing over
    // it, would lose it. `--out` is created first, so a path through a
    // directory that doesn't exist yet, or a `..`, resolves to where it
    // really points.
    fs::create_dir_all(&args.out)?;
    let real = |path: &Path| fs::canonicalize(path).unwrap_or_else(|_| resolved(path));
    let (out, header) = (real(&args.out), real(&args.header_out));
    if out.starts_with(&header) || header.starts_with(&out) {
        return Err(UsageError("--out and --header-out must not overlap").into());
    }
    let options = Options {
        out: args.out.clone(),
        deadline: args.deadline,
    };
    let (_lock, kept) = lock_out(&TOOL, &args.out)?;
    mark_descendants(&TOOL, &args.out);
    let mut sessions = track_sessions(&args.out);
    contain();
    // The caller's checkout must already be clean and at S, so a mistaken
    // subject fails before anything is built.
    subject_source(repo_root, &options, &args.subject)?;
    clear_stale_runs(&TOOL, repo_root, &options, kept.as_deref())?;
    sessions.cleared();
    let run_dir = args.out.join(RUNS).join(run_id());
    fs::create_dir_all(args.out.join(RUNS))?;
    fs::create_dir(&run_dir)?;
    let run = Options {
        out: run_dir.clone(),
        deadline: args.deadline,
    };
    let root = run_dir.join(SUBJECT_DIR);
    add_worktree(repo_root, &run, &root, &args.subject)?;
    // Everything after runs as the xtask built from S ([`hand_off`]).
    let result = hand_off(&root, &run, &args);
    let removed = remove_worktrees_under(repo_root, &run, &run_dir);
    let summary = result?;
    removed?;
    stop_if_interrupted()?;
    escaped()?;
    for target in RUN_TARGETS {
        let target = run_dir.join(target);
        if target.exists() {
            fs::remove_dir_all(target)?;
        }
    }
    write_checksums(&run_dir)?;
    publish_record(&args.out, &run_dir)?;
    // Nothing after the record is published can fail the run.
    {
        use std::io::Write as _;
        let mut stdout = std::io::stdout().lock();
        let _ = write!(stdout, "{summary}");
        let _ = writeln!(stdout, "record: {}", args.out.join(RECORD).display());
    }
    Ok(())
}

/// The hidden first argument that has this command run the experiments as
/// the xtask built from S's own checkout ([`hand_off`]).
const IN_SUBJECT: &str = "--in-subject";

/// How long the xtask built from S gets for the whole run: each of its own
/// children has the run's per-child deadline too.
const IN_SUBJECT_DEADLINE: Duration = Duration::from_secs(7 * 24 * 3600);

/// Builds xtask in `root`, S's own checkout, and runs the experiments with
/// it ([`run_in_subject`]), so the code that runs every task and writes its
/// artifacts' bindings is S's, not the caller's, as header-acceptance's
/// runner does. Returns the run's summary.
fn hand_off(root: &Path, run: &Options, args: &Args) -> Result<String, BoxError> {
    let logs = run.out.join("logs");
    fs::create_dir_all(&logs)?;
    let target = run.out.join("xtask-target");
    let cargo_path = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut build = Command::new(&cargo_path);
    build
        .args(["build", "--locked", "-p", "xtask", "--target-dir"])
        .arg(&target)
        .env("CARGO_TERM_COLOR", "never")
        .env_remove("RUSTFLAGS")
        .current_dir(root);
    child(&mut build, &logs.join("xtask-build.log"), run.deadline)
        .exited_ok()
        .map_err(|end| format!("building xtask at S {end}"))?;
    let mut inner = Command::new(target.join("debug").join("xtask"));
    inner
        .args(["sim-acceptance", IN_SUBJECT, "--subject", &args.subject])
        .arg("--header-out")
        .arg(&args.header_out)
        .arg("--out")
        .arg(&run.out)
        .arg("--deadline-secs")
        .arg(args.deadline.as_secs().to_string())
        .env("CARGO", &cargo_path)
        .current_dir(root);
    for only in &args.only {
        inner.args(["--only", only]);
    }
    let log = logs.join("in-subject.log");
    child(&mut inner, &log, IN_SUBJECT_DEADLINE)
        .exited_ok()
        .map_err(|end| {
            format!(
                "the run at S {end}:\n{}",
                fs::read_to_string(&log).unwrap_or_default()
            )
        })?;
    Ok(fs::read_to_string(logs.join("evaluate.log"))?)
}

/// The experiments, as the xtask built from S runs them ([`hand_off`]).
/// Its `--out` is the run's own directory, and `repo_root`, where it was
/// built, must be the checkout of S inside it, clean and at `--subject`.
/// The caller set that checkout up, removes it, and publishes the record
/// only if this exits 0.
fn run_in_subject(repo_root: &Path, args: Args) -> Result<(), BoxError> {
    let root = args.out.join(SUBJECT_DIR);
    if resolved(repo_root) != resolved(&root) {
        return Err(format!(
            "{IN_SUBJECT} runs only as the xtask built in {}",
            root.display()
        )
        .into());
    }
    let run = Options {
        out: args.out.clone(),
        deadline: args.deadline,
    };
    let contained = contain();
    let source = subject_source(repo_root, &run, &args.subject)?;
    run_at(repo_root, &run, &args, &source, contained)?;
    stop_if_interrupted()?;
    // This process's own children: a survivor fails the run here, so the
    // caller never publishes it.
    escaped()?;
    Ok(())
}

/// Fails if any child's descendants couldn't be confirmed gone, since a
/// survivor could have changed what later tasks read.
fn escaped() -> Result<(), BoxError> {
    if ESCAPED.load(Ordering::Relaxed) {
        return Err(
            "a child's descendants couldn't be confirmed gone, so the record can't be trusted"
                .into(),
        );
    }
    Ok(())
}

/// S's source digest, from a clean checkout at `repo_root` whose `HEAD` is
/// `subject`.
fn subject_source(repo_root: &Path, options: &Options, subject: &str) -> Result<String, BoxError> {
    let logs = tempfile::tempdir()?;
    let status = logs.path().join("status.log");
    let mut git = Command::new("git");
    git.args(["status", "--porcelain", "--untracked-files=all"])
        .current_dir(repo_root);
    child(&mut git, &status, options.deadline)
        .exited_ok()
        .map_err(|end| format!("`git status` {end}"))?;
    let changed = fs::read_to_string(&status)?;
    if !changed.trim().is_empty() {
        return Err(
            format!("sim-acceptance needs a clean checkout, and it has:\n{changed}").into(),
        );
    }
    let read = source_digest(
        &TOOL,
        repo_root,
        options.deadline,
        &logs.path().join("digest.log"),
    )?;
    if read.dirty || read.git == provenance::UNKNOWN || read.source == provenance::UNKNOWN {
        return Err("sim-acceptance needs a checkout whose source digest git can compute".into());
    }
    if read.git != subject {
        return Err(format!("the checkout is at {}, not the subject {subject}", read.git).into());
    }
    Ok(read.source)
}

/// Builds the run's binaries from `root`, plans, runs every task, and
/// evaluates, returning `evaluate`'s summary.
fn run_at(
    root: &Path,
    run: &Options,
    args: &Args,
    source: &str,
    contained: bool,
) -> Result<String, BoxError> {
    let logs = run.out.join("logs");
    let artifacts = run.out.join("artifacts");
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&artifacts)?;
    let build = |target: &str, release: bool, log: &str| -> Result<(), BoxError> {
        let mut cargo = cargo(root, &run.out.join(target));
        cargo.args(["build", "--locked", "-p", "zakura-sim"]);
        if release {
            cargo.args(["--release", "--bin", "zakura-sim"]);
        } else {
            cargo.args([
                "--bin",
                "zakura-sim",
                "--bin",
                "sim-acceptance",
                "--bin",
                "header-acceptance",
            ]);
        }
        child(&mut cargo, &logs.join(log), run.deadline)
            .exited_ok()
            .map_err(|end| format!("building the simulator {end}").into())
    };
    build("build", false, "build.log")?;
    build("build-release", true, "build-release.log")?;
    let tool = run.out.join("build/debug/sim-acceptance");
    let release = run.out.join("build-release/release/zakura-sim");

    let plan_file = run.out.join("plan.json");
    let mut plan = Command::new(&tool);
    plan.arg("plan")
        .arg("--repo")
        .arg(root)
        .arg("--out")
        .arg(&plan_file);
    child(&mut plan, &logs.join("plan.log"), run.deadline)
        .exited_ok()
        .map_err(|end| format!("`sim-acceptance plan` {end}"))?;
    let plan: Value = serde_json::from_str(&fs::read_to_string(&plan_file)?)?;

    // The committed seeds, then sched-4's saved race schedules, each in one
    // report, then each harness's full inputs written to one corpus by their
    // SHA-256 (`--out` holds one harness's seeds). Each report and its
    // child's outcome are artifacts: the evaluator counts a seed or schedule
    // only if its release inventory exited 0 and admitted it.
    let corpus = run.out.join("corpus");
    let run_name = run
        .out
        .file_name()
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    let release_sha256 = sha256_file(&release)?;
    for (name, fixtures) in [
        ("seeds", None),
        (
            "race-schedules",
            Some("crates/zakura-sim/tests/fixtures/race-schedules"),
        ),
    ] {
        let inventory = artifacts.join(name);
        fs::create_dir_all(&inventory)?;
        let mut seeds = Command::new(&release);
        seeds.arg("seeds").current_dir(root);
        if let Some(fixtures) = fixtures {
            seeds.args(["--fixtures", fixtures]);
        }
        seeds.arg("--report").arg(inventory.join("report.json"));
        let ended = child(&mut seeds, &inventory.join("inventory.log"), run.deadline);
        stop_if_interrupted()?;
        // Bound to this run, the executable and the report it wrote.
        fs::write(
            inventory.join("inventory.json"),
            serde_json::to_string_pretty(&json!({
                "exited_ok": ended.exited(),
                "killed": ended.killed(),
                "binding": {
                    "run": run_name,
                    "inventory": name,
                    "executable_sha256": release_sha256,
                    "report_sha256": sha256_file(&inventory.join("report.json")).ok(),
                },
            }))?,
        )?;
        let report: Value = serde_json::from_str(
            &fs::read_to_string(inventory.join("report.json")).unwrap_or_else(|_| "{}".to_owned()),
        )?;
        let harnesses: BTreeSet<&str> = report
            .get("seeds")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|row| row.pointer("/sidecar/harness")?.as_str())
            .collect();
        for harness in harnesses {
            let mut fill = Command::new(&release);
            fill.arg("seeds").current_dir(root);
            if let Some(fixtures) = fixtures {
                fill.args(["--fixtures", fixtures]);
            }
            fill.args(["--sim", harness, "--out"]).arg(&corpus);
            let log = logs.join(format!("{name}-{}.log", harness.replace('/', "-")));
            let _ = child(&mut fill, &log, run.deadline);
        }
    }

    let tasks = plan
        .get("tasks")
        .and_then(Value::as_array)
        .ok_or("the plan has no tasks")?;
    for (index, task) in tasks.iter().enumerate() {
        stop_if_interrupted()?;
        let expected = task
            .as_object()
            .and_then(|object| object.values().next())
            .and_then(|fields| fields.get("expected"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !args.only.is_empty()
            && !args
                .only
                .iter()
                .any(|only| expected.contains(only.as_str()))
        {
            continue;
        }
        let dir = artifacts.join(index.to_string());
        fs::create_dir_all(&dir)?;
        let task_json = serde_json::to_string_pretty(task)?;
        fs::write(dir.join("task.json"), &task_json)?;
        let (kind, fields) = task
            .as_object()
            .and_then(|object| object.iter().next())
            .ok_or("a task names its kind")?;
        let ctx = Ctx {
            root,
            run,
            dir: &dir,
            release: &release,
            corpus: &corpus,
            source,
            release_sha256: &release_sha256,
            binding: json!({
                "run": run_name,
                "task": index,
                "task_sha256": sha256_hex(task_json.as_bytes()),
            }),
        };
        match kind.as_str() {
            "test" => {
                run_test(
                    root,
                    run,
                    Some(source),
                    fields,
                    &dir,
                    &run.out.join("tests"),
                )?;
                // hdr-14's runner writes the build's messages beside its
                // `test.json`, so the binary it ran is the one test binary
                // they name.
                let built = test_executables(
                    &fs::read_to_string(dir.join("build.log")).unwrap_or_default(),
                );
                if let [(_, executable, _)] = &built[..] {
                    fs::write(
                        dir.join("binary.json"),
                        serde_json::to_string_pretty(&ctx.bound(
                            json!({
                                "sha256": sha256_file(executable)?,
                                "test_json_sha256": sha256_file(&dir.join("test.json"))?,
                            }),
                            json!({}),
                        ))?,
                    )?;
                }
            }
            "fuzz-test" => ctx.fuzz_test(fields)?,
            "lane" => ctx.lane(fields)?,
            "seed" => ctx.seed(fields)?,
            // A saved race schedule runs as a seed witness does.
            "seed-witness" | "race-schedule" => ctx.seed_witness(fields)?,
            "agreement" => ctx.agreement(fields)?,
            "planted" => ctx.planted(fields)?,
            "target" => ctx.target(fields)?,
            "minimize" => ctx.minimize(fields)?,
            "command" => ctx.command(fields)?,
            "measure" => ctx.measure(fields)?,
            other => return Err(format!("the plan has an unknown task kind `{other}`").into()),
        }
        ctx.check_checkout()?;
        escaped()?;
    }

    // Before the evaluator, which binds each trace to the kept release build.
    keep_executables(&run.out)?;
    let mut evaluate = Command::new(&tool);
    evaluate
        .arg("evaluate")
        .arg("--repo")
        .arg(root)
        .arg("--plan")
        .arg(&plan_file)
        .arg("--artifacts")
        .arg(&artifacts)
        .arg("--header-out")
        .arg(&args.header_out)
        .arg("--subject")
        .arg(&args.subject)
        .arg("--subject-source")
        .arg(source)
        .arg("--containment")
        .arg(crate::acceptance::containment(contained))
        .arg("--out")
        .arg(&run.out);
    let log = logs.join("evaluate.log");
    child(&mut evaluate, &log, run.deadline)
        .exited_ok()
        .map_err(|end| format!("`sim-acceptance evaluate` {end}"))?;
    Ok(fs::read_to_string(log)?)
}

/// What one task's runner reads.
struct Ctx<'a> {
    root: &'a Path,
    run: &'a Options,
    dir: &'a Path,
    release: &'a Path,
    corpus: &'a Path,
    /// S's source digest, which the checkout's own digest gave.
    source: &'a str,
    /// The release `zakura-sim`'s SHA-256.
    release_sha256: &'a str,
    /// The run's ID, the task's index and its `task.json`'s SHA-256, which
    /// every file the runner writes for the task carries as its `binding`.
    binding: Value,
}

/// `field` of a task, as text.
fn field<'v>(task: &'v Value, field: &str) -> Result<&'v str, BoxError> {
    task.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("the task has no `{field}`").into())
}

impl Ctx<'_> {
    /// `fields` with the task's binding, extended by `extra`, under
    /// `binding`, so the evaluator can refuse a file written for another
    /// task or about another trace.
    fn bound(&self, mut fields: Value, extra: Value) -> Value {
        let mut binding = self.binding.clone();
        if let (Some(binding), Some(extra)) = (binding.as_object_mut(), extra.as_object()) {
            binding.extend(extra.clone());
        }
        fields["binding"] = binding;
        fields
    }

    /// Runs `command` under the run's deadline with its output in
    /// `<dir>/<name>.log`, and writes `<dir>/<name>.json` with whether it
    /// exited 0, whether it was killed, its output, and its binding with
    /// `extra` added.
    fn record(&self, command: &mut Command, name: &str, extra: Value) -> Result<String, BoxError> {
        // Every simulation run a `zakura-sim` child makes writes its begin
        // and end records under `runs/`, in the phase `name` (Rule 11),
        // unless the command names its own records directory.
        let names_runs = command
            .get_envs()
            .any(|(key, _)| key == "ZAKURA_SIM_ACCEPTANCE_RUNS");
        if !names_runs
            && Path::new(command.get_program()).file_name()
                == Some(std::ffi::OsStr::new("zakura-sim"))
        {
            let runs = self.dir.join("runs");
            fs::create_dir_all(&runs)?;
            command
                .env("ZAKURA_SIM_ACCEPTANCE_RUNS", &runs)
                .env("ZAKURA_SIM_ACCEPTANCE_PHASE", name);
        }
        // Every record a child writes, at any depth, repeats the task's
        // binding.
        command.env("ZAKURA_SIM_ACCEPTANCE_BINDING", self.binding.to_string());
        let log = self.dir.join(format!("{name}.log"));
        let ended = child(command, &log, self.run.deadline);
        stop_if_interrupted()?;
        let text = fs::read_to_string(&log).unwrap_or_default();
        fs::write(
            self.dir.join(format!("{name}.json")),
            serde_json::to_string_pretty(&self.bound(
                json!({
                    "exited_ok": ended.exited(),
                    "killed": ended.killed(),
                    "log": text,
                }),
                extra,
            ))?,
        )?;
        Ok(text)
    }

    /// Replays the task's `trace/` fresh with the release build into
    /// `replay.json`, bound to the trace's SHA-256 when the replay starts and
    /// to the executable that replays it. A recording that left no trace
    /// gets no replay, so its task blocks on the missing `replay.json`
    /// rather than ending the run.
    fn replay(&self) -> Result<(), BoxError> {
        self.replay_at("trace", "replay", false)
    }

    /// [`Ctx::replay`] of the trace in `<dir>/<trace>`, into `<name>.json`,
    /// with `--observe` if `observe`, which writes the full observation log
    /// beside the trace.
    fn replay_at(&self, trace: &str, name: &str, observe: bool) -> Result<(), BoxError> {
        self.replay_with((self.release, self.release_sha256), trace, name, observe)
    }

    /// [`Ctx::replay_at`] by `executable`, with its SHA-256, which must be
    /// the build that recorded the trace.
    fn replay_with(
        &self,
        (executable, executable_sha256): (&Path, &str),
        trace: &str,
        name: &str,
        observe: bool,
    ) -> Result<(), BoxError> {
        let Ok(trace_sha256) = sha256_file(&self.dir.join(trace).join("trace.jsonl")) else {
            return Ok(());
        };
        let mut replay = Command::new(executable);
        replay
            .arg("replay")
            .arg(self.dir.join(trace))
            .current_dir(self.root);
        if observe {
            replay.arg("--observe");
        }
        self.record(
            &mut replay,
            name,
            json!({
                "trace_sha256": trace_sha256,
                "executable_sha256": executable_sha256,
            }),
        )?;
        Ok(())
    }

    /// One test of the `qa/fuzz/sim` workspace's library, by its exact
    /// name, without the ASan-only default feature. `test.json` has what
    /// libtest reported.
    fn fuzz_test(&self, task: &Value) -> Result<(), BoxError> {
        let test = field(task, "test")?;
        let mut cargo = cargo(self.root, &self.run.out.join("fuzz"));
        cargo.args([
            "test",
            "--locked",
            "--manifest-path",
            "qa/fuzz/sim/Cargo.toml",
            "--no-default-features",
            "--lib",
            "--",
            "--exact",
            test,
        ]);
        let log = self.dir.join("test.log");
        let ended = child(&mut cargo, &log, self.run.deadline);
        stop_if_interrupted()?;
        let text = fs::read_to_string(&log).unwrap_or_default();
        let binary = text
            .lines()
            .filter_map(running_binary)
            .map(|path| sha256_file(Path::new(path)))
            .collect::<Result<Vec<_>, _>>()?;
        fs::write(
            self.dir.join("test.json"),
            serde_json::to_string_pretty(&self.bound(
                json!({
                    "checkout_source": self.source,
                    "binary": binary.first(),
                    "tests": libtest(&text),
                    "killed": ended.killed(),
                    "exited_ok": ended.exited(),
                    "log": text,
                }),
                json!({}),
            ))?,
        )?;
        Ok(())
    }

    /// One lane's command, through `sh -c` as `qa/suite.toml` spells it.
    /// First the same command with `--no-run --message-format=json` builds
    /// its test binaries and names each one's package and target, and each
    /// binary is listed with `--list` before anything runs. Then the lane
    /// runs, each binary's results are matched to its target by path, and
    /// each `run = true` ignored test runs alone by its exact name.
    /// `lane.json` holds the outcome, each target's results, listed tests
    /// and binary digest, and the targets whose inventory failed (Rule 11).
    fn lane(&self, task: &Value) -> Result<(), BoxError> {
        let original = field(task, "command")?;
        // Cargo runs each test binary from its package's directory, so a
        // sanitizer file the command names relative to the checkout's root
        // is made absolute.
        let rooted = original.replace(
            "suppressions=.",
            &format!("suppressions={}/.", self.root.display()),
        );
        let command = rooted.as_str();
        let mut build = Command::new("sh");
        build
            .args(["-c", &format!("{command} --no-run --message-format=json")])
            .current_dir(self.root);
        let build_log = self.dir.join("inventory.log");
        let built = child(&mut build, &build_log, self.run.deadline);
        stop_if_interrupted()?;
        let mut inventory: BTreeMap<String, (PathBuf, PathBuf)> = BTreeMap::new();
        let mut list_failures = Vec::new();
        for (target, executable, package_dir) in
            test_executables(&fs::read_to_string(&build_log).unwrap_or_default())
        {
            if inventory
                .insert(target.clone(), (executable, package_dir))
                .is_some()
            {
                list_failures.push(format!("{target} names two test binaries"));
            }
        }
        let mut listed: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut binaries: BTreeMap<String, String> = BTreeMap::new();
        let mut binary_sources: BTreeMap<String, &str> = BTreeMap::new();
        for (target, (binary, _)) in &inventory {
            let bytes = fs::read(binary)?;
            binaries.insert(target.clone(), sha256_hex(&bytes));
            let embeds = crate::planted::names_source(&bytes, self.source).is_ok();
            binary_sources.insert(
                target.clone(),
                if embeds { self.source } else { "not-embedded" },
            );
            match (
                list_tests(binary, false, self),
                list_tests(binary, true, self),
            ) {
                (Some(all), Some(ignored)) => {
                    listed.insert(target.clone(), all.difference(&ignored).cloned().collect());
                }
                _ => list_failures.push(format!("{target}'s binary didn't list its tests")),
            }
        }
        let mut sh = Command::new("sh");
        sh.args(["-c", command]).current_dir(self.root);
        let log = self.dir.join("lane.log");
        let ended = child(&mut sh, &log, self.run.deadline);
        stop_if_interrupted()?;
        let text = fs::read_to_string(&log).unwrap_or_default();
        let by_file: BTreeMap<std::ffi::OsString, String> = inventory
            .iter()
            .filter_map(|(target, (binary, _))| {
                Some((binary.file_name()?.to_owned(), target.clone()))
            })
            .collect();
        let mut targets: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        let mut current: Option<String> = None;
        let mut section = String::new();
        let mut flush = |current: &Option<String>, section: &str| {
            if let Some(target) = current {
                targets
                    .entry(target.clone())
                    .or_default()
                    .extend(libtest(section));
            }
        };
        for line in text.lines() {
            if let Some(path) = running_binary(line) {
                flush(&current, &section);
                section.clear();
                current = Path::new(path)
                    .file_name()
                    .and_then(|file| by_file.get(file).cloned());
            } else {
                section.push_str(line);
                section.push('\n');
            }
        }
        flush(&current, &section);
        // Each `run = true` ignored test, alone, as `cargo test` runs a
        // binary: from its package's directory.
        let ignored_run: Vec<String> =
            serde_json::from_value(task.get("ignored_run").cloned().unwrap_or_default())?;
        for (index, test) in ignored_run.iter().enumerate() {
            let Some((target, name)) = test.split_once("::") else {
                continue;
            };
            let Some((binary, package_dir)) = inventory.get(target) else {
                continue;
            };
            let mut run = Command::new(binary);
            run.args(["--ignored", "--exact", name])
                .current_dir(package_dir)
                .env("CARGO_MANIFEST_DIR", package_dir);
            let log = self.dir.join(format!("ignored-{index}.log"));
            let ended = child(&mut run, &log, self.run.deadline);
            stop_if_interrupted()?;
            let results = libtest(&fs::read_to_string(&log).unwrap_or_default());
            let results = if ended.exited() {
                results
            } else {
                vec![(name.to_owned(), "FAILED".to_owned())]
            };
            targets
                .entry(target.to_owned())
                .or_default()
                .extend(results);
        }
        fs::write(
            self.dir.join("lane.json"),
            serde_json::to_string_pretty(&self.bound(
                json!({
                    "exited_ok": ended.exited() && built.exited(),
                    "killed": ended.killed() || built.killed(),
                    "targets": targets,
                    "listed": listed,
                    "binaries": binaries,
                    "binary_sources": binary_sources,
                    "checkout_source": self.source,
                    "list_failures": list_failures,
                    "log": text,
                }),
                json!({ "command": original }),
            ))?,
        )?;
        Ok(())
    }

    /// One committed seed's full input recorded with the release build,
    /// then replayed fresh by the same build: `trace/` and `replay.json`.
    fn seed(&self, task: &Value) -> Result<(), BoxError> {
        let mut record = self.zakura_sim(task, &[])?;
        self.record(&mut record, "record", json!({}))?;
        self.replay()
    }

    /// One seed witness, recorded with `--witness`: `trace/` with its
    /// `witness.json`.
    fn seed_witness(&self, task: &Value) -> Result<(), BoxError> {
        let property = field(task, "property")?;
        let expect = field(task, "expect")?;
        let mut record = self.zakura_sim(task, &["--witness", property, "--expect", expect])?;
        self.record(&mut record, "record", json!({}))?;
        self.replay()
    }

    /// One fuzz target's readiness: the release `zakura-sim targets` on it,
    /// building the fuzz binary under `<run>/fuzz-target/`, never the shared
    /// `target/sim-fuzz`, with the report in `targets/` and every
    /// fuzz-target run's begin and end records in `runs/`, which the
    /// adapter writes only when the acceptance tool names that directory.
    fn target(&self, task: &Value) -> Result<(), BoxError> {
        let runs = self.dir.join("runs");
        fs::create_dir_all(&runs)?;
        let mut targets = Command::new(self.release);
        targets
            .args(["targets", "--target", field(task, "name")?, "--out"])
            .arg(self.dir.join("targets"))
            .arg("--target-dir")
            .arg(self.run.out.join("fuzz-target"))
            .env("ZAKURA_SIM_ACCEPTANCE_RUNS", &runs)
            .current_dir(self.root);
        self.record(&mut targets, "targets", json!({}))?;
        // The fuzz binary the run built, by its `[[bin]]` name.
        let name = field(task, "name")?;
        let mut pending = vec![self.run.out.join("fuzz-target")];
        let mut built = Vec::new();
        while let Some(dir) = pending.pop() {
            for entry in fs::read_dir(&dir).into_iter().flatten().flatten() {
                let kind = entry.file_type()?;
                if kind.is_dir() {
                    pending.push(entry.path());
                } else if kind.is_file()
                    && entry.file_name() == name
                    && entry.path().parent().and_then(Path::file_name)
                        == Some(std::ffi::OsStr::new("release"))
                {
                    built.push(entry.path());
                }
            }
        }
        if let [binary] = &built[..] {
            fs::write(
                self.dir.join("fuzz-binary.json"),
                serde_json::to_string_pretty(
                    &self.bound(json!({ "sha256": sha256_file(binary)? }), json!({})),
                )?,
            )?;
        }
        Ok(())
    }

    /// The discovery planted case: `cargo xtask planted <property>` for its
    /// verdict, then S's checkout with the case's patch applied, in a
    /// worktree under `<run>/planted/<task>/` with its own target
    /// directory, whose release `zakura-sim` is kept as
    /// `<run>/bin/<sha256>/zakura-sim` and named in `patched.json`. The
    /// case's committed input is recorded by S's release build as a
    /// `holds` witness into `baseline/` and by the patched build as a
    /// `violated` witness into `patched/`, and each is replayed fresh with
    /// `--observe` by the build that recorded it. A step that fails leaves
    /// its files missing, so the task blocks.
    fn planted(&self, task: &Value) -> Result<(), BoxError> {
        let property = field(task, "property")?;
        let harness = field(task, "harness")?;
        let input = self.root.join(field(task, "input")?);
        let cargo_path = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        // Every run it makes, at any depth, writes its records here.
        let planted_runs = self.dir.join("planted-runs");
        fs::create_dir_all(&planted_runs)?;
        let mut verdict = Command::new(&cargo_path);
        verdict
            .args(["xtask", "planted", property])
            .env_remove("RUSTFLAGS")
            .env("ZAKURA_SIM_ACCEPTANCE_RUNS", &planted_runs)
            .current_dir(self.root);
        self.record(&mut verdict, "planted", json!({}))?;
        let Some((work, src)) = self.patched_worktree(field(task, "patch")?)? else {
            return Ok(());
        };
        let mut build = cargo(&src, &work.join("target"));
        build.args([
            "build",
            "--locked",
            "--release",
            "-p",
            "zakura-sim",
            "--bin",
            "zakura-sim",
        ]);
        self.record(&mut build, "build-patched", json!({}))?;
        // A build that didn't exit 0 in time keeps nothing, even if it left
        // an executable.
        let built = fs::read_to_string(self.dir.join("build-patched.json"))
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|build| build.get("exited_ok")?.as_bool())
            == Some(true);
        let Some(bytes) = built
            .then(|| fs::read(work.join("target/release/zakura-sim")).ok())
            .flatten()
        else {
            return Ok(());
        };
        let patched_sha256 = sha256_hex(&bytes);
        let kept_dir = self.run.out.join("bin").join(&patched_sha256);
        fs::create_dir_all(&kept_dir)?;
        let kept = kept_dir.join("zakura-sim");
        fs::write(&kept, &bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&kept, fs::Permissions::from_mode(0o755))?;
        }
        fs::write(
            self.dir.join("patched.json"),
            serde_json::to_string_pretty(
                &self.bound(json!({ "sha256": patched_sha256 }), json!({})),
            )?,
        )?;
        for (side, executable, executable_sha256, expect) in [
            ("baseline", self.release, self.release_sha256, "holds"),
            (
                "patched",
                kept.as_path(),
                patched_sha256.as_str(),
                "violated",
            ),
        ] {
            let mut record = Command::new(executable);
            record
                .args(["record", "--sim", harness, "--input"])
                .arg(&input)
                .arg("--out")
                .arg(self.dir.join(side))
                .args(["--witness", property, "--expect", expect])
                .current_dir(self.root);
            self.record(&mut record, &format!("record-{side}"), json!({}))?;
            self.replay_with(
                (executable, executable_sha256),
                side,
                &format!("replay-{side}"),
                true,
            )?;
        }
        Ok(())
    }

    /// A worktree of S under `<run>/planted/<task>/src` with `patch`
    /// applied, recorded in `apply.json`, and the task's work directory, or
    /// `None` if the patch didn't apply.
    fn patched_worktree(&self, patch: &str) -> Result<Option<(PathBuf, PathBuf)>, BoxError> {
        let index = self
            .binding
            .get("task")
            .and_then(Value::as_u64)
            .ok_or("a task's binding names its index")?;
        let work = self.run.out.join("planted").join(index.to_string());
        let src = work.join("src");
        add_worktree(self.root, self.run, &src, "HEAD")?;
        let mut apply = Command::new("git");
        apply
            .arg("apply")
            .arg(self.root.join(patch))
            .current_dir(&src);
        self.record(&mut apply, "apply", json!({}))?;
        let applied = fs::read_to_string(self.dir.join("apply.json"))
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|apply| apply.get("exited_ok")?.as_bool())
            == Some(true);
        Ok(applied.then_some((work, src)))
    }

    /// The header minimization: `cargo xtask sim-minimize` on the planted
    /// case's committed input through its fuzz target, in a worktree of S
    /// with the case's patch, with every run it makes, at any depth,
    /// writing its records under `runs/` and its report under `minimize/`.
    fn minimize(&self, task: &Value) -> Result<(), BoxError> {
        let Some((_, src)) = self.patched_worktree(field(task, "patch")?)? else {
            return Ok(());
        };
        let runs = self.dir.join("runs");
        fs::create_dir_all(&runs)?;
        let cargo_path = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let mut minimize = Command::new(&cargo_path);
        minimize
            .args(["xtask", "sim-minimize", field(task, "target")?])
            .arg(self.root.join(field(task, "input")?))
            .args(["--property", field(task, "property")?, "--out"])
            .arg(self.dir.join("minimize"))
            .args([
                "--runs",
                "2000",
                "--wall-secs",
                "1800",
                "--child-secs",
                "120",
            ])
            .env_remove("RUSTFLAGS")
            .env("ZAKURA_SIM_ACCEPTANCE_RUNS", &runs)
            .current_dir(&src);
        self.record(&mut minimize, "minimize", json!({}))?;
        Ok(())
    }

    /// One race fixture's agreement pair: its saved schedule recorded as its
    /// violation's witness into `base/`, the same Plan bytes on the fixed
    /// scenario as a `holds` witness into `fixed/`, and each replayed fresh
    /// with `--observe`.
    fn agreement(&self, task: &Value) -> Result<(), BoxError> {
        let harness = field(task, "harness")?;
        let mut base = Command::new(self.release);
        base.args(["record", "--sim", harness, "--input"])
            .arg(self.corpus.join(field(task, "base_input")?))
            .arg("--out")
            .arg(self.dir.join("base"))
            .args(["--witness", harness, "--expect", "violated"])
            .current_dir(self.root);
        self.record(&mut base, "record-base", json!({}))?;
        self.replay_at("base", "replay-base", true)?;
        let plan = field(task, "plan")?;
        let bytes = (0..plan.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(plan.get(i..i + 2).unwrap_or("zz"), 16))
            .collect::<Result<Vec<u8>, _>>()
            .map_err(|_| "the task's plan isn't hex")?;
        let plan_file = self.dir.join("plan.bin");
        fs::write(&plan_file, bytes)?;
        let mut fixed = Command::new(self.release);
        fixed
            .args(["record", "--sim", harness, "--scenario"])
            .arg(field(task, "fixed_scenario")?)
            .arg("--plan")
            .arg(&plan_file)
            .arg("--out")
            .arg(self.dir.join("fixed"))
            .args(["--witness", harness, "--expect", "holds"])
            .current_dir(self.root);
        self.record(&mut fixed, "record-fixed", json!({}))?;
        self.replay_at("fixed", "replay-fixed", true)?;
        Ok(())
    }

    /// `zakura-sim record` of the task's harness on its seed's full input,
    /// into `<dir>/trace`, with `extra` arguments.
    fn zakura_sim(&self, task: &Value, extra: &[&str]) -> Result<Command, BoxError> {
        let mut record = Command::new(self.release);
        record
            .args(["record", "--sim", field(task, "harness")?, "--input"])
            .arg(self.corpus.join(field(task, "input")?))
            .arg("--out")
            .arg(self.dir.join("trace"))
            .args(extra)
            .current_dir(self.root);
        Ok(record)
    }

    /// One S4.6 command. The sim-package check runs in the simulator build,
    /// the others in the normal one.
    fn command(&self, task: &Value) -> Result<(), BoxError> {
        let args: Vec<String> =
            serde_json::from_value(task.get("args").cloned().unwrap_or_default())?;
        let sim = task.get("sim").and_then(Value::as_bool).unwrap_or(false);
        let mut cargo = cargo(self.root, &self.run.out.join("commands"));
        if !sim {
            cargo.env_remove("RUSTFLAGS");
        }
        cargo.args(&args);
        self.record(&mut cargo, "command", json!({}))?;
        // What ran, so the evaluator can tell one command's result from
        // another's.
        let file = self.dir.join("command.json");
        let mut result: Value = serde_json::from_str(&fs::read_to_string(&file)?)?;
        result["args"] = json!(args);
        result["sim"] = json!(sim);
        result["checkout_source"] = json!(self.source);
        fs::write(&file, serde_json::to_string_pretty(&result)?)?;
        Ok(())
    }

    /// Writes `<dir>/checkout.json`: whether the subject checkout is still
    /// clean after the task, tracked and untracked files alike, since every
    /// later task builds from it. The evaluator blocks a task that left it
    /// dirty and every task after it.
    fn check_checkout(&self) -> Result<(), BoxError> {
        let mut status = Command::new("git");
        status
            .args(["status", "--porcelain", "--ignored=no"])
            .current_dir(self.root);
        let log = self.dir.join("checkout.log");
        let ended = child(&mut status, &log, self.run.deadline);
        stop_if_interrupted()?;
        let text = fs::read_to_string(&log).unwrap_or_default();
        fs::write(
            self.dir.join("checkout.json"),
            serde_json::to_string_pretty(&self.bound(
                json!({
                    "clean": ended.exited() && text.trim().is_empty(),
                    "status": text,
                }),
                json!({}),
            ))?,
        )?;
        Ok(())
    }

    /// One measured recording of `harness`, on the input the plan chose
    /// from the corpus, or on empty input when it chose none: `trace/` and
    /// `measure.json`.
    fn measure(&self, task: &Value) -> Result<(), BoxError> {
        let harness = field(task, "harness")?;
        let input = match task.get("input").and_then(Value::as_str) {
            Some(input) => self.corpus.join(input),
            None => {
                let empty = self.dir.join("empty.bin");
                fs::write(&empty, [])?;
                empty
            }
        };
        let mut record = Command::new(self.release);
        record
            .args(["record", "--sim", harness, "--input"])
            .arg(input)
            .arg("--out")
            .arg(self.dir.join("trace"))
            .arg("--measure")
            .arg(self.dir.join("measure.json"))
            .current_dir(self.root);
        // A race fixture's run on its saved schedule fails by design, so it
        // records as that violation's witness, which exits 0 only if it
        // happened.
        if let Some(property) = task.get("violates").and_then(Value::as_str) {
            record.args(["--witness", property, "--expect", "violated"]);
        }
        self.record(&mut record, "record", json!({}))?;
        Ok(())
    }
}

/// The path in a `cargo test` line `Running <target> (<path>)`.
fn running_binary(line: &str) -> Option<&str> {
    let rest = line.trim().strip_prefix("Running ")?;
    let (_, path) = rest.rsplit_once('(')?;
    Some(path.trim_end_matches(')'))
}

/// Each test binary a `cargo test --no-run --message-format=json` build
/// names in `messages`, as `<package>/<target>` with its executable and its
/// package's directory. The target is `lib` for a library's unit tests, the
/// name for an integration test, and `<kind>:<name>` for any other kind.
/// Only test-profile artifacts count, so a normal binary an integration
/// test needs isn't taken for one.
fn test_executables(messages: &str) -> Vec<(String, PathBuf, PathBuf)> {
    let mut built = Vec::new();
    for line in messages.lines() {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if message.get("reason").and_then(Value::as_str) != Some("compiler-artifact")
            || message.pointer("/profile/test").and_then(Value::as_bool) != Some(true)
        {
            continue;
        }
        let (Some(executable), Some(manifest)) = (
            message.get("executable").and_then(Value::as_str),
            message.get("manifest_path").and_then(Value::as_str),
        ) else {
            continue;
        };
        let package_dir = Path::new(manifest)
            .parent()
            .map_or_else(PathBuf::new, Path::to_path_buf);
        let package = package_dir
            .file_name()
            .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
        let kind = message
            .pointer("/target/kind/0")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let name = message
            .pointer("/target/name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let target = match kind {
            "test" => name.to_owned(),
            "lib" | "rlib" | "dylib" | "cdylib" | "staticlib" | "proc-macro" => "lib".to_owned(),
            other => format!("{other}:{name}"),
        };
        built.push((
            format!("{package}/{target}"),
            PathBuf::from(executable),
            package_dir,
        ));
    }
    built
}

/// `bytes`' SHA-256 in lowercase hex.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A file's SHA-256 in lowercase hex.
fn sha256_file(path: &Path) -> Result<String, BoxError> {
    let bytes = fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(sha256_hex(&bytes))
}

/// Writes `<run>/checksums.json`: every file left in the run directory,
/// by its path relative to it, with its SHA-256, so an archive of the run
/// can be checked file by file. Links aren't followed.
fn write_checksums(run_dir: &Path) -> Result<(), BoxError> {
    let mut sums = BTreeMap::new();
    let mut pending = vec![run_dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            let path = entry.path();
            if kind.is_dir() {
                pending.push(path);
            } else if kind.is_file() {
                let relative = path
                    .strip_prefix(run_dir)
                    .expect("every walked path is under the run directory")
                    .display()
                    .to_string();
                sums.insert(relative, sha256_file(&path)?);
            }
        }
    }
    fs::write(
        run_dir.join("checksums.json"),
        serde_json::to_string_pretty(&sums)?,
    )?;
    Ok(())
}

/// Copies the run's recording executables, the release and debug
/// `zakura-sim`, to `<run>/bin/<sha256>/zakura-sim`, with `bin/index.json`
/// naming which is which, before their build directories go: every trace
/// sim-7 records keeps the executable that recorded it (Rule 9).
fn keep_executables(run_dir: &Path) -> Result<(), BoxError> {
    use sha2::{Digest, Sha256};
    let mut index = serde_json::Map::new();
    for (name, path) in [
        ("release", "build-release/release/zakura-sim"),
        ("debug", "build/debug/zakura-sim"),
    ] {
        let bytes = fs::read(run_dir.join(path))?;
        let digest: String = Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let dir = run_dir.join("bin").join(&digest);
        fs::create_dir_all(&dir)?;
        let kept = dir.join("zakura-sim");
        fs::write(&kept, &bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&kept, fs::Permissions::from_mode(0o755))?;
        }
        index.insert(name.to_owned(), json!(digest));
    }
    fs::write(
        run_dir.join("bin").join("index.json"),
        serde_json::to_string_pretty(&Value::Object(index))?,
    )?;
    Ok(())
}

/// The tests `binary --list` names, or with `ignored` only the ignored
/// ones, or `None` if the listing didn't exit 0.
fn list_tests(binary: &Path, ignored: bool, ctx: &Ctx<'_>) -> Option<BTreeSet<String>> {
    let mut list = Command::new(binary);
    list.arg("--list").current_dir(ctx.root);
    if ignored {
        list.arg("--ignored");
    }
    let log = ctx.dir.join(format!(
        "list-{}.log",
        if ignored { "ignored" } else { "all" }
    ));
    child(&mut list, &log, ctx.run.deadline).exited_ok().ok()?;
    Some(
        fs::read_to_string(&log)
            .ok()?
            .lines()
            .filter_map(|line| line.strip_suffix(": test"))
            .map(str::to_owned)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::test_executables;

    /// A normal binary built for an integration test isn't taken for the
    /// library's test binary, and each target keeps its kind and name.
    #[test]
    fn the_inventory_takes_only_test_binaries() {
        let artifact = |kind: &str, name: &str, test: bool, exe: &str| {
            json!({
                "reason": "compiler-artifact",
                "manifest_path": "/repo/crates/zakura-sim/Cargo.toml",
                "target": { "kind": [kind], "name": name },
                "profile": { "test": test },
                "executable": exe,
            })
            .to_string()
        };
        let messages = [
            artifact("lib", "zakura_sim", true, "/t/deps/zakura_sim-1"),
            artifact("bin", "zakura-sim", false, "/t/zakura-sim"),
            artifact("test", "draws", true, "/t/deps/draws-2"),
            artifact("bin", "zakura-sim", true, "/t/deps/zakura_sim-3"),
        ]
        .join("\n");
        let targets: Vec<(String, String)> = test_executables(&messages)
            .into_iter()
            .map(|(target, exe, _)| (target, exe.display().to_string()))
            .collect();
        assert_eq!(
            targets,
            [
                ("zakura-sim/lib", "/t/deps/zakura_sim-1"),
                ("zakura-sim/draws", "/t/deps/draws-2"),
                ("zakura-sim/bin:zakura-sim", "/t/deps/zakura_sim-3"),
            ]
            .map(|(target, exe)| (target.to_owned(), exe.to_owned()))
        );
    }
}
